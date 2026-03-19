"""
Stats Differ — per-column statistical drift using pure DuckDB SQL.

All stats are computed server-side in DuckDB in a single pass per column group.
No data is ever pulled into Python memory for stats computation.
Works on datasets of any size — DuckDB spills to disk automatically.

Supports all formats: CSV, Parquet, JSON, JSONL, Delta, Avro, Iceberg, S3, GCS, Azure.
"""

from __future__ import annotations

import math

from difflake.connection import DuckDBConnection
from difflake.models import ColumnStatsDiff, StatsDiff

_NUMERIC_TYPES = {
    "TINYINT","SMALLINT","INTEGER","BIGINT","HUGEINT",
    "UTINYINT","USMALLINT","UINTEGER","UBIGINT",
    "FLOAT","DOUBLE","DECIMAL","REAL",
    "INT1","INT2","INT4","INT8","INT16",
    "NUMERIC",
}
_DATETIME_TYPES = {"DATE","TIMESTAMP","TIMESTAMP WITH TIME ZONE","TIMESTAMPTZ","TIMESTAMP_S","TIMESTAMP_MS","TIMESTAMP_NS","TIME","INTERVAL"}
_CATEGORICAL_TYPES = {"VARCHAR","TEXT","CHAR","BOOLEAN","ENUM","STRING"}

_MAX_CARDINALITY_ENUM = 500
_KL_SAMPLE = 100_000


def _base_type(dtype: str) -> str:
    """Normalize DuckDB type string — strip precision/scale."""
    return dtype.upper().split("(")[0].strip()


def _dtype_category(dtype: str) -> str:
    bt = _base_type(dtype)
    if bt in _NUMERIC_TYPES:
        return "numeric"
    if bt in _DATETIME_TYPES:
        return "datetime"
    if bt in _CATEGORICAL_TYPES:
        return "categorical"
    return "other"


def _looks_like_dates(con, view: str, col: str, sample: int = 5) -> bool:
    """
    Heuristic: check if a VARCHAR column contains ISO date strings.
    DuckDB stores date-looking strings as VARCHAR when inserted via Python.
    We sample a few rows and try to parse them as dates.
    """
    import re
    try:
        _, rows = con.fetchdf(f'''
            SELECT CAST("{col}" AS VARCHAR) AS v
            FROM {view}
            WHERE "{col}" IS NOT NULL
            LIMIT {sample}
        ''')
        if not rows:
            return False
        date_re = re.compile(r"^\d{{4}}-\d{{2}}-\d{{2}}")
        return all(date_re.match(str(r[0])) for r in rows if r[0] is not None)
    except Exception:
        return False


def _pct_change(before: float | None, after: float | None) -> float | None:
    if before is None or after is None:
        return None
    if before == 0:
        return None if after == 0 else float("inf")
    return round(((after - before) / abs(before)) * 100, 4)


def _kl_divergence_sql(con: DuckDBConnection, old_view: str, new_view: str,
                        col: str, bins: int = 50) -> float | None:
    """
    Compute KL divergence D(P||Q) entirely in DuckDB SQL using histogram binning.
    Samples up to _KL_SAMPLE rows from each dataset for performance.
    Returns None on failure (e.g. all-null column).
    """
    try:
        # Get combined min/max first
        row = con.fetchone(f"""
            SELECT
                MIN(v) AS combined_min,
                MAX(v) AS combined_max
            FROM (
                SELECT CAST("{col}" AS DOUBLE) AS v
                FROM {old_view} WHERE "{col}" IS NOT NULL
                USING SAMPLE {_KL_SAMPLE} ROWS
                UNION ALL
                SELECT CAST("{col}" AS DOUBLE) AS v
                FROM {new_view} WHERE "{col}" IS NOT NULL
                USING SAMPLE {_KL_SAMPLE} ROWS
            ) t
        """)
        if not row or row[0] is None or row[0] == row[1]:
            return 0.0

        cmin, cmax = float(row[0]), float(row[1])
        bin_width = (cmax - cmin) / bins

        # Build histogram and compute KL divergence in SQL
        kl = con.scalar(f"""
            WITH
            old_sample AS (
                SELECT CAST("{col}" AS DOUBLE) AS v
                FROM {old_view} WHERE "{col}" IS NOT NULL
                USING SAMPLE {_KL_SAMPLE} ROWS
            ),
            new_sample AS (
                SELECT CAST("{col}" AS DOUBLE) AS v
                FROM {new_view} WHERE "{col}" IS NOT NULL
                USING SAMPLE {_KL_SAMPLE} ROWS
            ),
            bins AS (
                SELECT generate_series AS b FROM generate_series(0, {bins - 1})
            ),
            old_hist AS (
                SELECT
                    LEAST(FLOOR((v - {cmin}) / {bin_width}), {bins - 1}) AS bucket,
                    COUNT(*) AS cnt
                FROM old_sample GROUP BY 1
            ),
            new_hist AS (
                SELECT
                    LEAST(FLOOR((v - {cmin}) / {bin_width}), {bins - 1}) AS bucket,
                    COUNT(*) AS cnt
                FROM new_sample GROUP BY 1
            ),
            old_total AS (SELECT SUM(cnt) AS n FROM old_hist),
            new_total AS (SELECT SUM(cnt) AS n FROM new_hist),
            joined AS (
                SELECT
                    b.b AS bucket,
                    COALESCE(o.cnt, 0) + 1e-10 AS p_raw,
                    COALESCE(n.cnt, 0) + 1e-10 AS q_raw
                FROM bins b
                LEFT JOIN old_hist o ON o.bucket = b.b
                LEFT JOIN new_hist n ON n.bucket = b.b
            ),
            totals AS (
                SELECT SUM(p_raw) AS p_sum, SUM(q_raw) AS q_sum FROM joined
            ),
            normalized AS (
                SELECT
                    p_raw / p_sum AS p,
                    q_raw / q_sum AS q
                FROM joined CROSS JOIN totals
            )
            SELECT SUM(p * LN(p / q)) FROM normalized
        """)
        return round(float(kl), 6) if kl is not None else None
    except Exception:
        return None


class StatsDiffer:
    """
    Computes per-column statistical drift entirely in DuckDB SQL.
    No row data is pulled into Python — all aggregations run server-side.
    Works on any size dataset; DuckDB handles spilling to disk automatically.
    """

    def __init__(
        self,
        con: DuckDBConnection,
        old_view: str,
        new_view: str,
        drift_threshold: float = 0.15,
        columns: list[str] | None = None,
    ):
        self.con             = con
        self.old_view        = old_view
        self.new_view        = new_view
        self.drift_threshold = drift_threshold

        # Resolve columns to diff — intersection of both schemas
        old_schema = dict(con.columns(old_view))
        new_schema = dict(con.columns(new_view))
        common = [c for c in old_schema if c in new_schema]
        self.columns       = [c for c in columns if c in dict.fromkeys(common)] if columns else common
        self.old_schema    = old_schema
        self.new_schema    = new_schema

    def run(self) -> StatsDiff:
        column_diffs: list[ColumnStatsDiff] = []

        for col in self.columns:
            dtype    = self.old_schema.get(col, "VARCHAR")
            cat = _dtype_category(dtype)

            # VARCHAR columns that contain ISO date strings should be
            # treated as datetime for drift detection purposes
            if cat == "categorical" and _looks_like_dates(self.con, self.old_view, col):
                cat = "datetime"

            if cat == "numeric":
                diff = self._numeric_diff(col)
            elif cat == "categorical":
                diff = self._categorical_diff(col)
            elif cat == "datetime":
                diff = self._datetime_diff(col)
            else:
                diff = self._base_diff(col, cat)

            self._check_drift(diff)
            column_diffs.append(diff)

        drifted = [d.column for d in column_diffs if d.is_drifted]
        return StatsDiff(column_diffs=column_diffs, drifted_columns=drifted)

    # ── Private helpers ────────────────────────────────────────────────────

    def _null_rates(self, col: str) -> tuple[float, float]:
        """Compute null rate for col in both views via a single SQL query."""
        row = self.con.fetchone(f"""
            SELECT
                (SELECT COUNT(*) * 100.0 / NULLIF(COUNT(*), 0)
                 FROM {self.old_view} WHERE "{col}" IS NULL)  AS old_null_pct,
                (SELECT COUNT(*) * 100.0 / NULLIF(COUNT(*), 0)
                 FROM {self.new_view} WHERE "{col}" IS NULL)  AS new_null_pct
        """)
        return (
            round(float(row[0] or 0), 4),
            round(float(row[1] or 0), 4),
        )

    def _cardinality(self, col: str) -> tuple[int, int]:
        row = self.con.fetchone(f"""
            SELECT
                (SELECT COUNT(DISTINCT "{col}") FROM {self.old_view}),
                (SELECT COUNT(DISTINCT "{col}") FROM {self.new_view})
        """)
        return int(row[0] or 0), int(row[1] or 0)

    def _base_diff(self, col: str, cat: str) -> ColumnStatsDiff:
        null_before, null_after  = self._null_rates(col)
        card_before, card_after  = self._cardinality(col)
        return ColumnStatsDiff(
            column=col,
            dtype_category=cat,
            null_rate_before=null_before,
            null_rate_after=null_after,
            cardinality_before=card_before,
            cardinality_after=card_after,
        )

    def _numeric_diff(self, col: str) -> ColumnStatsDiff:
        null_before, null_after = self._null_rates(col)
        card_before, card_after = self._cardinality(col)

        def safe(v) -> float | None:
            return round(float(v), 6) if v is not None else None

        # Single-pass aggregation for each view
        def agg(view: str):
            row = self.con.fetchone(f"""
                SELECT
                    AVG(CAST("{col}" AS DOUBLE)),
                    MEDIAN(CAST("{col}" AS DOUBLE)),
                    STDDEV(CAST("{col}" AS DOUBLE)),
                    MIN(CAST("{col}" AS DOUBLE)),
                    MAX(CAST("{col}" AS DOUBLE))
                FROM {view}
                WHERE "{col}" IS NOT NULL
            """)
            return row if row else (None,)*5

        o = agg(self.old_view)
        n = agg(self.new_view)

        mean_before = safe(o[0])
        mean_after = safe(n[0])
        mean_drift   = _pct_change(mean_before, mean_after)

        kl = _kl_divergence_sql(self.con, self.old_view, self.new_view, col)

        return ColumnStatsDiff(
            column=col,
            dtype_category="numeric",
            null_rate_before=null_before,
            null_rate_after=null_after,
            cardinality_before=card_before,
            cardinality_after=card_after,
            mean_before=mean_before,
            mean_after=mean_after,
            mean_drift_pct=mean_drift,
            median_before=safe(o[1]),
            median_after=safe(n[1]),
            std_before=safe(o[2]),
            std_after=safe(n[2]),
            min_before=safe(o[3]),
            min_after=safe(n[3]),
            max_before=safe(o[4]),
            max_after=safe(n[4]),
            kl_divergence=kl,
        )

    def _categorical_diff(self, col: str) -> ColumnStatsDiff:
        null_before, null_after = self._null_rates(col)
        card_before, card_after = self._cardinality(col)

        new_cats: list[str]     = []
        dropped_cats: list[str] = []

        # Only enumerate categories when cardinality is manageable
        if card_before <= _MAX_CARDINALITY_ENUM and card_after <= _MAX_CARDINALITY_ENUM:
            try:
                _, rows = self.con.fetchdf(f"""
                    SELECT DISTINCT 'new' AS side, CAST("{col}" AS VARCHAR) AS val
                        FROM {self.new_view}
                        WHERE "{col}" IS NOT NULL
                          AND CAST("{col}" AS VARCHAR) NOT IN (
                              SELECT DISTINCT CAST("{col}" AS VARCHAR)
                              FROM {self.old_view} WHERE "{col}" IS NOT NULL
                          )
                    UNION ALL
                    SELECT DISTINCT 'dropped' AS side, CAST("{col}" AS VARCHAR) AS val
                        FROM {self.old_view}
                        WHERE "{col}" IS NOT NULL
                          AND CAST("{col}" AS VARCHAR) NOT IN (
                              SELECT DISTINCT CAST("{col}" AS VARCHAR)
                              FROM {self.new_view} WHERE "{col}" IS NOT NULL
                          )
                    ORDER BY 1, 2
                """)
                for side, val in rows:
                    if side == "new":
                        new_cats.append(str(val))
                    else:
                        dropped_cats.append(str(val))
            except Exception:
                pass

        return ColumnStatsDiff(
            column=col,
            dtype_category="categorical",
            null_rate_before=null_before,
            null_rate_after=null_after,
            cardinality_before=card_before,
            cardinality_after=card_after,
            new_categories=new_cats[:20],
            dropped_categories=dropped_cats[:20],
        )

    def _datetime_diff(self, col: str) -> ColumnStatsDiff:
        """
        Datetime columns: null rate, cardinality, min/max date shift.
        Alerts when the min or max date shifts between source and target.
        Uses VARCHAR cast so it works for both DATE and TIMESTAMP column types.
        """
        null_before, null_after = self._null_rates(col)
        card_before, card_after = self._cardinality(col)

        def _dt_bounds(view):
            try:
                row = self.con.fetchone(
                    f'SELECT CAST(MIN("{col}") AS VARCHAR), ' 
                    f'CAST(MAX("{col}") AS VARCHAR) ' 
                    f'FROM {view} WHERE "{col}" IS NOT NULL'
                )
                if row and row[0] is not None:
                    return str(row[0])[:10], str(row[1])[:10]
                return None, None
            except Exception:
                pass
            try:
                row = self.con.fetchone(
                    f'SELECT MIN("{col}"), MAX("{col}") ' 
                    f'FROM {view} WHERE "{col}" IS NOT NULL'
                )
                if row and row[0] is not None:
                    return str(row[0])[:10], str(row[1])[:10]
                return None, None
            except Exception:
                return None, None

        old_min, old_max = _dt_bounds(self.old_view)
        new_min, new_max = _dt_bounds(self.new_view)

        diff = ColumnStatsDiff(
            column=col,
            dtype_category="datetime",
            null_rate_before=null_before,
            null_rate_after=null_after,
            cardinality_before=card_before,
            cardinality_after=card_after,
            min_before=None, min_after=None,
            max_before=None, max_after=None,
        )
        reasons = []
        if old_min and new_min and old_min != new_min:
            reasons.append(f"Min date shifted: {old_min} -> {new_min}")
        if old_max and new_max and old_max != new_max:
            reasons.append(f"Max date shifted: {old_max} -> {new_max}")
        if reasons:
            diff.drift_reasons = reasons
            diff.is_drifted = True
        return diff

    def _check_drift(self, diff: ColumnStatsDiff) -> None:
        reasons: list[str] = []
        thr_pct = self.drift_threshold * 100

        # Null rate drift
        if diff.null_rate_before is not None and diff.null_rate_after is not None:
            delta = abs(diff.null_rate_after - diff.null_rate_before)
            if delta > thr_pct:
                reasons.append(
                    f"Null rate changed {delta:.1f}pp "
                    f"({diff.null_rate_before:.1f}% → {diff.null_rate_after:.1f}%)"
                )

        # Mean drift
        if (diff.mean_drift_pct is not None
                and not math.isinf(diff.mean_drift_pct)
                and abs(diff.mean_drift_pct) > thr_pct):
                reasons.append(
                    f"Mean drifted {diff.mean_drift_pct:+.1f}% "
                    f"({diff.mean_before} → {diff.mean_after})"
                )

        # Categorical changes
        if diff.dtype_category == "categorical":
            if diff.new_categories:
                reasons.append(f"{len(diff.new_categories)} new categories appeared")
            if diff.dropped_categories:
                reasons.append(f"{len(diff.dropped_categories)} categories disappeared")

        # KL divergence
        if diff.kl_divergence is not None and diff.kl_divergence > 0.1:
            reasons.append(f"KL divergence = {diff.kl_divergence:.4f} (distribution shifted)")

        # Merge with any reasons already set (e.g. by _datetime_diff)
        all_reasons = list(diff.drift_reasons) + reasons
        diff.drift_reasons = all_reasons
        diff.is_drifted    = bool(all_reasons)

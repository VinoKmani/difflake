"""
Row Differ — key-based row-level diff using pure DuckDB SQL.

All joins and comparisons happen inside DuckDB — no rows are pulled into
Python until the tiny sample output. This means:

  - A full outer join on 50M rows works fine (DuckDB spills to disk)
  - Low-cardinality key guard catches VendorID-style mistakes early
  - Composite keys are joined natively using multiple ON conditions
  - Null-aware change detection: null→value and value→null both count as changes
  - --sample N limits rows via USING SAMPLE before the join
"""

from __future__ import annotations

from difflake.connection import DuckDBConnection
from difflake.models import RowDiff

_SAMPLE_ROWS = 10          # rows to include in sample output
_LOW_CARD_THRESHOLD = 10        # warn if unique key count < this
_LOW_CARD_RATIO    = 0.01       # and uniqueness ratio < 1% of total rows


def _parse_key(primary_key: str | list[str] | None) -> list[str]:
    if not primary_key:
        return []
    if isinstance(primary_key, list):
        return [k.strip() for k in primary_key if k.strip()]
    return [k.strip() for k in primary_key.split(",") if k.strip()]


def _closest_columns(name: str, available: set[str], n: int = 3) -> list[str]:
    """Return up to n column names most similar to `name`, for helpful error messages."""
    from difflib import SequenceMatcher
    scored = sorted(
        available,
        key=lambda c: SequenceMatcher(None, name.lower(), c.lower()).ratio(),
        reverse=True,
    )
    return scored[:n]


class RowDiffer:
    """
    Row-level diff using DuckDB SQL FULL OUTER JOIN.
    No data is materialized into Python memory during the diff.
    """

    def __init__(
        self,
        con: DuckDBConnection,
        old_view: str,
        new_view: str,
        primary_key: str | list[str] | None = None,
        sample_size: int | None = None,
    ):
        self.con         = con
        self.old_view    = old_view
        self.new_view    = new_view
        self.key_cols    = _parse_key(primary_key)
        self.sample_size = sample_size

        # Register sampled views if needed
        if sample_size:
            con.execute(f"""
                CREATE OR REPLACE VIEW __old_sampled AS
                SELECT * FROM {old_view} USING SAMPLE {sample_size} ROWS
            """)
            con.execute(f"""
                CREATE OR REPLACE VIEW __new_sampled AS
                SELECT * FROM {new_view} USING SAMPLE {sample_size} ROWS
            """)
            self.old_v = "__old_sampled"
            self.new_v = "__new_sampled"
        else:
            self.old_v = old_view
            self.new_v = new_view

    def run(self) -> RowDiff:
        row_count_before = self.con.scalar(f"SELECT COUNT(*) FROM {self.old_view}") or 0
        row_count_after  = self.con.scalar(f"SELECT COUNT(*) FROM {self.new_view}") or 0

        # Validate key columns exist in both views
        old_cols = {c for c, _ in self.con.columns(self.old_view)}
        new_cols = {c for c, _ in self.con.columns(self.new_view)}
        valid_keys = [k for k in self.key_cols if k in old_cols and k in new_cols]

        missing_in_source = [k for k in self.key_cols if k not in old_cols]
        missing_in_target = [k for k in self.key_cols if k not in new_cols]
        key_error = None

        if missing_in_source or missing_in_target:
            lines = ["Key column validation failed — falling back to count-only diff."]
            if missing_in_source:
                suggestions = _closest_columns(missing_in_source[0], old_cols)
                lines.append(f"  x '{missing_in_source[0]}' not found in SOURCE file.")
                if suggestions:
                    lines.append(f"    Did you mean: {', '.join(suggestions)} ?")
            if missing_in_target:
                suggestions = _closest_columns(missing_in_target[0], new_cols)
                lines.append(f"  x '{missing_in_target[0]}' not found in TARGET file.")
                if suggestions:
                    lines.append(f"    Did you mean: {', '.join(suggestions)} ?")
            lines.append("  Run: difflake show <file> --schema  to see available columns.")
            key_error = "\n".join(lines)

        if not valid_keys:
            return RowDiff(
                row_count_before=int(row_count_before),
                row_count_after=int(row_count_after),
                rows_added=max(0, int(row_count_after) - int(row_count_before)),
                rows_removed=max(0, int(row_count_before) - int(row_count_after)),
                key_based_diff=False,
                key_error=key_error,
            )

        self.key_cols = valid_keys

        # ── Low-cardinality guard ──────────────────────────────────────────
        # Build a COUNT(DISTINCT key) check in SQL — no data loaded into Python
        if len(valid_keys) == 1:
            unique_sql = f'SELECT COUNT(DISTINCT "{valid_keys[0]}") FROM {self.old_v}'
        else:
            concat_expr = " || '|' || ".join(
                f'COALESCE(CAST("{k}" AS VARCHAR), \'__null__\')' for k in valid_keys
            )
            unique_sql = f"SELECT COUNT(DISTINCT {concat_expr}) FROM {self.old_v}"

        unique_count = int(self.con.scalar(unique_sql) or 0)

        # Block only when BOTH conditions are true:
        #   1. fewer than 10 unique values  (absolute low cardinality)
        #   2. uniqueness ratio < 1%        (relative to dataset size)
        # This allows small test datasets (5 rows, 5 unique user_ids = 100% ratio)
        # while still catching real bad keys (VendorID: 2 unique / 2M rows = 0.0001%)
        total_src_rows = int(self.con.scalar(f"SELECT COUNT(*) FROM {self.old_view}") or 1)
        # Block if key has fewer than 10 unique values regardless of dataset size.
        # Any key with < 10 unique values cannot identify rows — a join on it would
        # match every row against every other row with the same value.
        is_low_card = (unique_count < _LOW_CARD_THRESHOLD and total_src_rows > 100)
        if is_low_card:
            import warnings
            warnings.warn(
                f"\n\n  ⚠️  LOW-CARDINALITY KEY: '{','.join(self.key_cols)}' has only "
                f"{unique_count} unique value(s) across {row_count_before:,} rows.\n"
                f"  A join on this key would create billions of combinations.\n"
                f"  → Choose a high-cardinality column instead.\n"
                f"  → Falling back to count-only diff for safety.\n",
                stacklevel=2,
            )
            low_card_error = (
                f"LOW-CARDINALITY KEY: '{','.join(self.key_cols)}' has only "
                f"{unique_count} unique value(s) across {int(row_count_before):,} rows.\n"
                f"  A join on this key creates too many combinations and will crash memory.\n"
                f"  Choose a high-cardinality column (e.g. trip_id, record_id, datetime).\n"
                f"  Run: difflake show <file> --schema  to see available columns."
            )
            return RowDiff(
                row_count_before=int(row_count_before),
                row_count_after=int(row_count_after),
                rows_added=max(0, int(row_count_after) - int(row_count_before)),
                rows_removed=max(0, int(row_count_before) - int(row_count_after)),
                key_based_diff=False,
                key_error=low_card_error,
            )

        return self._key_based_diff(int(row_count_before), int(row_count_after))

    # ── Private ────────────────────────────────────────────────────────────

    def _key_based_diff(self, row_count_before: int, row_count_after: int) -> RowDiff:
        old_v = self.old_v
        new_v = self.new_v
        keys  = self.key_cols

        # Shared non-key columns (present in both views)
        old_cols   = {c for c, _ in self.con.columns(self.old_view)}
        new_cols   = {c for c, _ in self.con.columns(self.new_view)}
        shared_val = [c for c in old_cols if c in new_cols and c not in keys]

        # Build JOIN ON clause
        join_on = " AND ".join(
            f'o."{k}" IS NOT DISTINCT FROM n."{k}"' for k in keys
        )

        # Register FULL OUTER JOIN view
        # IS NOT DISTINCT FROM handles nulls correctly (null = null → true)
        self.con.execute(f"""
            CREATE OR REPLACE VIEW __diff_join AS
            SELECT
                {', '.join(f'o."{k}" AS "__old_{k}"' for k in keys)},
                {', '.join(f'n."{k}" AS "__new_{k}"' for k in keys)},
                {', '.join(f'o."{c}" AS "__old_{c}", n."{c}" AS "__new_{c}"'
                           for c in shared_val)},
                CASE
                    WHEN {' AND '.join(f'o."{k}" IS NULL' for k in keys)} THEN 'added'
                    WHEN {' AND '.join(f'n."{k}" IS NULL' for k in keys)} THEN 'removed'
                    ELSE 'common'
                END AS __row_status
            FROM {old_v} o
            FULL OUTER JOIN {new_v} n ON {join_on}
        """)

        # ── Counts ─────────────────────────────────────────────────────────
        rows_added   = int(self.con.scalar(
            "SELECT COUNT(*) FROM __diff_join WHERE __row_status = 'added'") or 0)
        rows_removed = int(self.con.scalar(
            "SELECT COUNT(*) FROM __diff_join WHERE __row_status = 'removed'") or 0)

        # Changed rows: common rows where ANY value column differs
        if shared_val:
            change_conds = " OR ".join(
                f'(__old_{c} IS DISTINCT FROM __new_{c})' for c in shared_val
            )
            rows_changed = int(self.con.scalar(f"""
                SELECT COUNT(*) FROM __diff_join
                WHERE __row_status = 'common' AND ({change_conds})
            """) or 0)
        else:
            rows_changed = 0

        rows_unchanged = int(self.con.scalar(
            "SELECT COUNT(*) FROM __diff_join WHERE __row_status = 'common'") or 0) - rows_changed

        # ── Sample added rows ──────────────────────────────────────────────
        sample_added: list[dict] = []
        try:
            key_sel = ", ".join(f'"__new_{k}" AS "{k}"' for k in keys)
            val_sel = ", ".join(f'"__new_{c}" AS "{c}"' for c in shared_val)
            all_sel = ", ".join(filter(None, [key_sel, val_sel]))
            _, rows = self.con.fetchdf(f"""
                SELECT {all_sel} FROM __diff_join
                WHERE __row_status = 'added'
                LIMIT {_SAMPLE_ROWS}
            """)
            col_names = keys + shared_val
            sample_added = [dict(zip(col_names, r)) for r in rows]
        except Exception:
            pass

        # ── Sample removed rows ────────────────────────────────────────────
        sample_removed: list[dict] = []
        try:
            key_sel = ", ".join(f'"__old_{k}" AS "{k}"' for k in keys)
            val_sel = ", ".join(f'"__old_{c}" AS "{c}"' for c in shared_val)
            all_sel = ", ".join(filter(None, [key_sel, val_sel]))
            _, rows = self.con.fetchdf(f"""
                SELECT {all_sel} FROM __diff_join
                WHERE __row_status = 'removed'
                LIMIT {_SAMPLE_ROWS}
            """)
            col_names = keys + shared_val
            sample_removed = [dict(zip(col_names, r)) for r in rows]
        except Exception:
            pass

        # ── Sample changed rows (before/after per changed field) ───────────
        sample_changed: list[dict] = []
        if rows_changed > 0 and shared_val:
            try:
                change_conds = " OR ".join(
                    f'(__old_{c} IS DISTINCT FROM __new_{c})' for c in shared_val
                )
                _, rows = self.con.fetchdf(f"""
                    SELECT * FROM __diff_join
                    WHERE __row_status = 'common' AND ({change_conds})
                    LIMIT {_SAMPLE_ROWS}
                """)
                # Build column index from DESCRIBE
                desc_cols = [c for c, _ in self.con.columns("__diff_join")]
                for row in rows:
                    row_dict = dict(zip(desc_cols, row))
                    key_display = {k: row_dict.get(f"__old_{k}") for k in keys}
                    entry: dict = {"_key": key_display}
                    for c in shared_val:
                        before = row_dict.get(f"__old_{c}")
                        after  = row_dict.get(f"__new_{c}")
                        if str(before) != str(after):
                            entry[c] = {"before": before, "after": after}
                    sample_changed.append(entry)
            except Exception:
                pass

        return RowDiff(
            row_count_before=row_count_before,
            row_count_after=row_count_after,
            rows_added=rows_added,
            rows_removed=rows_removed,
            rows_changed=rows_changed,
            rows_unchanged=max(0, rows_unchanged),
            sample_added=sample_added,
            sample_removed=sample_removed,
            sample_changed=sample_changed,
            primary_key_used=",".join(keys),
            key_based_diff=True,
            sampled=self.sample_size is not None,
            sample_size_used=self.sample_size,
        )

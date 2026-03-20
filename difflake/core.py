"""
LakeDiff — main orchestrator.

Wires together: DuckDB connection → view registration → schema/stats/row differs → report.

All data stays in DuckDB. Python only touches tiny sample outputs.
Supports every format DuckDB supports: CSV, Parquet, JSON, JSONL, Delta,
Avro, Iceberg, S3, GCS, Azure, HTTP, PostgreSQL, MySQL.
"""

from __future__ import annotations

import time
from pathlib import Path
from typing import Literal

from difflake.connection import DuckDBConnection, _detect_format, _read_sql
from difflake.differ.row_differ import RowDiffer
from difflake.differ.schema_differ import SchemaDiffer
from difflake.differ.stats_differ import StatsDiffer
from difflake.logging_setup import get_logger
from difflake.models import DiffResult

log = get_logger(__name__)

DiffMode = Literal["full", "schema", "stats", "rows"]


class LakeDiff:
    """
    Primary entry point for the LakeDiff library.

    Usage::

        from difflake import LakeDiff

        result = LakeDiff(
            source="data/v1/users.parquet",
            target="data/v2/users.parquet",
            primary_key="user_id",
        ).run()

        result.to_html("report.html")

    Cloud / remote paths work identically::

        LakeDiff(
            source="s3://my-bucket/data/v1/users.parquet",
            target="s3://my-bucket/data/v2/users.parquet",
            primary_key="user_id",
        ).run()

    Delta Lake, Iceberg, Avro::

        LakeDiff(source="path/to/delta_table/", target="path/to/delta_table_v2/").run()
        LakeDiff(source="path/to/iceberg_table", target="...", source_format="iceberg").run()
        LakeDiff(source="file.avro", target="file2.avro").run()
    """

    def __init__(
        self,
        source: str | Path,
        target: str | Path,
        primary_key: str | list[str] | None = None,
        source_format: str | None = None,
        target_format: str | None = None,
        mode: DiffMode = "full",
        drift_threshold: float = 0.15,
        columns: list[str] | None = None,
        sample_size: int | None = None,
        limit: int | None = None,
        where: str | None = None,
        ignore_columns: list[str] | None = None,
    ):
        """
        Args:
            source:          Path to the "before" dataset. Any format or URI.
            target:          Path to the "after" dataset.
            primary_key:     Single column ("user_id"), comma-separated composite
                             ("tenant_id,order_id,event_date"), or list.
            source_format:   Explicit format override: csv / parquet / json / jsonl /
                             delta / avro / iceberg. Auto-detected by default.
            target_format:   Explicit format override for target.
            mode:            "full" | "schema" | "stats" | "rows"
            drift_threshold: Relative change threshold (0–1) for drift alerts (default 0.15).
            columns:         Subset of columns to diff. None = all shared columns.
            sample_size:     Max rows per dataset for stats and row diff.
                             Schema diff always runs on full metadata.
                             Sampled randomly (USING SAMPLE N ROWS in DuckDB).
            where:           SQL WHERE filter applied before diff.
                             e.g. where="payment_type = 1 AND fare_amount > 0"
        """
        self.source        = str(source)
        self.target        = str(target)
        self.primary_key   = primary_key
        self.source_format = source_format
        self.target_format = target_format
        self.mode          = mode
        self.drift_threshold = drift_threshold
        self.columns       = columns
        self.sample_size    = sample_size
        self.limit          = limit
        self.where          = where
        self.ignore_columns = ignore_columns

    def run(self) -> DiffResult:
        """Execute the full diff and return a DiffResult."""
        t0 = time.perf_counter()

        # ── Detect formats ─────────────────────────────────────────────────
        src_fmt = self.source_format or _detect_format(self.source)
        tgt_fmt = self.target_format or _detect_format(self.target)

        log.info(
            "diff started",
            extra={
                "source": self.source, "target": self.target,
                "mode": self.mode, "src_fmt": src_fmt, "tgt_fmt": tgt_fmt,
            },
        )
        log.debug(
            "diff parameters",
            extra={
                "primary_key": self.primary_key,
                "drift_threshold": self.drift_threshold,
                "sample_size": self.sample_size,
                "limit": self.limit,
                "where": self.where,
                "ignore_columns": self.ignore_columns,
            },
        )

        # ── Open DuckDB connection ─────────────────────────────────────────
        con = DuckDBConnection()

        def _is_remote(path: str) -> bool:
            return any(str(path).startswith(s) for s in
                       ("s3://","gs://","gcs://","az://","abfs://",
                        "abfss://","http://","https://"))

        remote_src = _is_remote(str(self.source))
        remote_tgt = _is_remote(str(self.target))

        try:
            # ── Schema views ───────────────────────────────────────────────
            # For remote paths (HTTP/S3/GCS/Azure), DuckDB re-opens the HTTP
            # connection for every query against a view. To avoid multiple
            # round-trips and HTTP 0 errors, we materialise the full file
            # into a DuckDB table once, then run all queries against that.
            # For local files, views are fine — DuckDB reads the Parquet footer
            # in-process and schema queries are instant.
            src_schema_sql = _read_sql(self.source, src_fmt)
            tgt_schema_sql = _read_sql(self.target, tgt_fmt)

            if remote_src or remote_tgt:
                # Ensure extensions and credentials before materialising
                con.ensure_extensions(src_fmt, str(self.source))
                con.ensure_extensions(tgt_fmt, str(self.target))
                if remote_src:
                    if str(self.source).startswith("s3://"):
                        con.configure_s3()
                    elif str(self.source).startswith(("gs://", "gcs://")):
                        con.configure_gcs()
                    elif str(self.source).startswith(("az://", "abfs")):
                        con.configure_azure()
                if remote_tgt:
                    if str(self.target).startswith("s3://"):
                        con.configure_s3()
                    elif str(self.target).startswith(("gs://", "gcs://")):
                        con.configure_gcs()
                    elif str(self.target).startswith(("az://", "abfs")):
                        con.configure_azure()
                # Materialise the full remote file once — all subsequent queries
                # run against the in-memory table, never touching the network again
                con.execute(f"CREATE OR REPLACE TABLE __src_schema AS {src_schema_sql}")
                con.execute(f"CREATE OR REPLACE TABLE __tgt_schema AS {tgt_schema_sql}")
            else:
                con.register_view("__src_schema", src_schema_sql, src_fmt, self.source)
                con.register_view("__tgt_schema", tgt_schema_sql, tgt_fmt, self.target)

            result = DiffResult(
                source_path=self.source,
                target_path=self.target,
                source_format=src_fmt.upper(),
                target_format=tgt_fmt.upper(),
                diff_mode=self.mode,
                threshold_used=self.drift_threshold,
            )

            # ── Schema diff ────────────────────────────────────────────────
            if self.mode in ("full", "schema"):
                log.debug("running schema diff")
                result.schema_diff = SchemaDiffer(
                    con, "__src_schema", "__tgt_schema"
                ).run()
                log.info(
                    "schema diff complete",
                    extra={
                        "added": len(result.schema_diff.added_columns),
                        "removed": len(result.schema_diff.removed_columns),
                        "type_changed": len(result.schema_diff.type_changed_columns),
                    },
                )

            # ── Register working views (with sample + where filter) ────────
            run_stats = self.mode in ("full", "stats")
            run_rows  = self.mode in ("full", "rows")

            if run_stats or run_rows:
                src_work_sql = _read_sql(
                    self.source, src_fmt,
                    where=self.where,
                    columns=self.columns,
                    sample_size=self.sample_size,
                    limit=self.limit,
                )
                tgt_work_sql = _read_sql(
                    self.target, tgt_fmt,
                    where=self.where,
                    columns=self.columns,
                    sample_size=self.sample_size,
                    limit=self.limit,
                )
                # Materialisation strategy:
                #
                # Always materialise when:
                #   (a) --sample or --limit is set — avoids re-scanning Parquet
                #       19+ times (once per stats column). 14-28x speedup.
                #   (b) source or target is an HTTP/S3/GCS/Azure path — DuckDB
                #       httpfs can't reliably re-use HTTP connections across
                #       multiple queries on the same URL (HTTP 0 errors).
                #       Materialising fetches the remote file once into memory.
                #   (c) Same file used as both source and target — materialise
                #       once and reuse.
                #
                # Without sampling on a remote path, materialise the full file.
                # This is safe because DuckDB spills to disk if memory is tight.

                should_materialise = (
                    self.sample_size or
                    self.limit or
                    remote_src or
                    remote_tgt
                )

                if should_materialise:
                    # Extensions and credentials already configured above for remote paths.
                    # For local paths with sample/limit, register_view first so
                    # ensure_extensions runs, then materialise.
                    if not (remote_src or remote_tgt):
                        con.register_view("__src_tmp", src_work_sql, src_fmt, self.source)
                        con.register_view("__tgt_tmp", tgt_work_sql, tgt_fmt, self.target)
                        con.execute("CREATE OR REPLACE TABLE __src AS SELECT * FROM __src_tmp")
                        con.execute("CREATE OR REPLACE TABLE __tgt AS SELECT * FROM __tgt_tmp")
                    else:
                        con.execute(f"CREATE OR REPLACE TABLE __src AS {src_work_sql}")
                        con.execute(f"CREATE OR REPLACE TABLE __tgt AS {tgt_work_sql}")
                else:
                    con.register_view("__src", src_work_sql, src_fmt, self.source)
                    con.register_view("__tgt", tgt_work_sql, tgt_fmt, self.target)

            # ── Row diff ───────────────────────────────────────────────────
            if run_rows:
                log.debug("running row diff", extra={"primary_key": self.primary_key})
                result.row_diff = RowDiffer(
                    con, "__src", "__tgt",
                    primary_key=self.primary_key,
                    # sample already applied via _read_sql above
                ).run()
                log.info(
                    "row diff complete",
                    extra={
                        "rows_added": result.row_diff.rows_added,
                        "rows_removed": result.row_diff.rows_removed,
                        "rows_changed": result.row_diff.rows_changed,
                    },
                )
                # Patch row counts to reflect actual file sizes (not sample)
                if self.sample_size:
                    result.row_diff.row_count_before = int(
                        con.scalar("SELECT COUNT(*) FROM __src_schema") or 0)
                    result.row_diff.row_count_after = int(
                        con.scalar("SELECT COUNT(*) FROM __tgt_schema") or 0)
            else:
                # Populate row counts even in schema/stats-only mode
                result.row_diff.row_count_before = int(
                    con.scalar("SELECT COUNT(*) FROM __src_schema") or 0)
                result.row_diff.row_count_after = int(
                    con.scalar("SELECT COUNT(*) FROM __tgt_schema") or 0)

            # ── Stats diff ─────────────────────────────────────────────────
            if run_stats:
                log.debug("running stats diff")
                # Build effective column list — respect both --columns and --ignore-columns
                diff_cols = self.columns
                if self.ignore_columns:
                    all_cols = [c for c, _ in con.columns("__src")]
                    diff_cols = [c for c in (diff_cols or all_cols)
                                 if c not in self.ignore_columns]

                result.stats_diff = StatsDiffer(
                    con, "__src", "__tgt",
                    drift_threshold=self.drift_threshold,
                    columns=diff_cols,
                ).run()

            # ── Drift alerts ───────────────────────────────────────────────
            result.drift_alerts    = self._build_alerts(result)
            result.elapsed_seconds = round(time.perf_counter() - t0, 3)

            elapsed = result.elapsed_seconds
            log.info(
                "diff complete",
                extra={
                    "elapsed_s": elapsed,
                    "drift_alerts": len(result.drift_alerts),
                    "drifted_columns": len(result.stats_diff.drifted_columns),
                },
            )
            if result.drift_alerts:
                log.warning(
                    "drift alerts detected",
                    extra={"alerts": result.drift_alerts},
                )

        finally:
            con.close()

        return result

    def _build_alerts(self, result: DiffResult) -> list[str]:
        alerts: list[str] = []

        for col_diff in result.stats_diff.column_diffs:
            for reason in col_diff.drift_reasons:
                alerts.append(f"[{col_diff.column}] {reason}")

        for col in result.schema_diff.removed_columns:
            alerts.append(f"Schema: column '{col.name}' was removed")

        for col in result.schema_diff.type_changed_columns:
            alerts.append(
                f"Schema: column '{col.name}' type changed "
                f"({col.old_dtype} → {col.new_dtype})"
            )

        rd = result.row_diff
        if rd.row_count_before > 0:
            pct = abs(rd.row_count_delta) / rd.row_count_before * 100
            if pct > 20:
                direction = "increased" if rd.row_count_delta > 0 else "decreased"
                alerts.append(
                    f"Row count {direction} by {pct:.1f}% "
                    f"({rd.row_count_before:,} → {rd.row_count_after:,})"
                )

        return alerts

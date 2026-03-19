# Changelog

All notable changes to difflake are documented here.
Format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
Versions follow [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [1.0.0] — 2025-03-13

First stable release. Complete rewrite on DuckDB backend.

### Added
- **DuckDB backend** — all diff operations run as SQL inside DuckDB. No data ever enters Python memory during schema or stats diff. Row diff uses a native FULL OUTER JOIN.
- **Any-scale row diff** — DuckDB spills to disk automatically. Files that previously caused OOM on the Polars backend now run cleanly.
- **Cloud storage** — S3, GCS, and Azure Blob Storage supported natively. Credentials read from environment variables. No extra code needed.
- **Delta Lake** — directory-based Delta tables supported via DuckDB delta extension, auto-installed on first use.
- **Avro** — `.avro` files supported via DuckDB avro extension.
- **Iceberg** — Iceberg tables supported via DuckDB iceberg extension.
- **HTTP / HTTPS Parquet** — any public Parquet URL works as a source or target path.
- **Cross-cloud diff** — source and target can be in different clouds or mixed local/cloud.
- **`--where` SQL filter** — applied as a DuckDB predicate before any diff or preview. Works on all formats.
- **`difflake show`** — file preview command with `--schema`, `--count`, `--stats`, `--where`, `--columns`, `--tail`, `--rows` flags.
- **Rich spinner** — visible progress indicator while DuckDB works on large files or remote paths.
- **Low-cardinality key guard** — detects keys with fewer than 10 unique values and blocks the join with a clear explanation and column suggestions.
- **Key error messages** — when a key column is missing, difflake shows which file it is missing from and suggests the closest matching column names.
- **Actionable cloud error messages** — access denied and credential errors are caught and converted into human-readable messages with the exact environment variables to set.
- **`difflake formats`** — command listing all supported formats, extensions, and cloud URI schemes.
- **Sample materialisation** — when `--sample` or `--limit` is used, the sampled rows are now materialised as a real in-memory DuckDB table before stats diff runs. Previously each column aggregation re-scanned the full Parquet file through the sample subquery (O(columns × file_size)). Now it's one scan to materialise then fast in-memory queries (O(file_size + columns × sample_size)). Speedup: 14–28x on real data. Stats with `--sample 500k` on 35M rows: 312s → 11s.
- **`difflake diff`** — alias for `difflake compare`. Works identically, exists because everyone types it first.
- **`--ignore-columns`** — exclude audit/ETL columns that always differ and would pollute diff results.
- **`--limit N`** — use first N rows in file order (deterministic). Complements `--sample` which draws randomly.
- **`--order-by col [DESC]`** — sort rows in `difflake show` without knowing the threshold.
- **`--freq col`** — top-10 value frequencies for a column with count, percentage, and bar chart.
- **Auto output filename** — `--output html` without `--out` auto-generates `difflake_src_vs_tgt_timestamp.html`.
- **Pre-flight info** — `compare` and `diff` show source/target row counts and file sizes before running.
- **Summary line** — single-line diff summary printed after every run, easy to copy into Slack or PRs.
- **Overview mode** — `difflake show` with no flags now shows schema + count + first 5 rows in one output.
- **Datetime stats** — timestamp columns now show min/max dates in `--stats` and drift detection for date shifts.
- **`difflake.yaml` config file** — project-level config, auto-loaded from working directory. CLI flags always override.
- **CI exit codes** — exit 0 (no drift), exit 1 (error), exit 2 (drift alerts fired).
- **`--sample N`** — random row sampling via `USING SAMPLE N ROWS` in DuckDB, applied before stats and row diff.
- **Composite primary keys** — comma-separated or list form: `--key tenant_id,order_id,event_date`.
- **Rename detection** — uses Jaro-Winkler string similarity to detect likely column renames.
- **KL divergence** — distribution shift metric computed entirely in DuckDB SQL without pulling data into Python.
- **HTML report** — self-contained with Chart.js doughnut chart, dark theme, no external server needed.
- **Markdown report** — GitHub PR / dbt-ready, post directly as a PR comment.
- **JSON report** — full machine-readable diff result, suitable for downstream automation.
- **Full test suite** — 75+ tests covering all formats, all modes, edge cases, key errors, WHERE filters, CLI commands, and report output.

### Removed
- Polars dependency — removed entirely. No Polars, PyArrow, NumPy, or SciPy in core dependencies.
- Per-format loaders (`CsvLoader`, `ParquetLoader`, etc.) — replaced by `connection.py` which uses DuckDB SQL for all formats.

---

## [0.1.0] — 2024-01-01

Initial release. Polars-based backend.

### Added
- CSV, TSV, Parquet, JSON, JSONL, Delta Lake loaders via Polars
- Schema diff, stats diff, row diff
- CLI, HTML, JSON, Markdown output
- Drift threshold alerting

[1.0.0]: https://github.com/VinoKmani/difflake/releases/tag/v1.0.0
[0.1.0]: https://github.com/VinoKmani/difflake/releases/tag/v0.1.0

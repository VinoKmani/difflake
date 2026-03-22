# difflake

**git diff, but for your data lake.**

[![CI](https://github.com/VinoKmani/difflake/actions/workflows/ci.yml/badge.svg)](https://github.com/VinoKmani/difflake/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/difflake.svg)](https://pypi.org/project/difflake/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)
[![GitHub stars](https://img.shields.io/github/stars/VinoKmani/difflake?style=social)](https://github.com/VinoKmani/difflake)

difflake tells you exactly what changed between two datasets — schema, statistics, and row-level differences — across any file format, at any scale, from any storage location. Powered by DuckDB. No server required.

```bash
pip install difflake

difflake compare jan.parquet feb.parquet
difflake compare jan.parquet feb.parquet --mode schema
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05
difflake compare jan.parquet feb.parquet --key trip_id --output html --out report.html
difflake show jan.parquet --where "fare_amount > 500" --stats
difflake validate jan.parquet --min-rows 1000 --not-null vendor_id --unique trip_id
difflake query jan.parquet "SELECT payment_type, COUNT(*) FROM t GROUP BY 1"
```

---

## Table of contents

- [Why difflake](#why-difflake)
- [Install](#install)
- [Get sample data](#get-sample-data)
- [Quick start](#quick-start)
- [Commands](#commands)
  - [compare — diff two datasets](#compare--diff-two-datasets)
  - [show — inspect a file](#show--inspect-a-file)
  - [validate — assert data quality](#validate--assert-data-quality)
  - [query — run SQL against any file](#query--run-sql-against-any-file)
- [Diff modes](#diff-modes)
  - [Schema diff](#schema-diff)
  - [Statistics diff](#statistics-diff)
  - [Row-level diff](#row-level-diff)
- [Output formats](#output-formats)
- [Filters and sampling](#filters-and-sampling)
- [Cloud storage](#cloud-storage)
- [Config file](#config-file)
- [CI integration](#ci-integration)
- [Python API](#python-api)
- [Performance](#performance)
- [How it works](#how-it-works)
- [Supported formats](#supported-formats)
- [Contributing](#contributing)
- [License](#license)

---

## Screenshots

**`difflake show` — inspect any file instantly**

![difflake show overview](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_show.png)

**`difflake compare --mode schema` — schema diff and drift alerts**

![difflake compare schema](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_compare_schema.png)

**`difflake compare --mode stats` — statistical diff**

![difflake compare stats](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_compare_stats.png)

**`difflake show --stats --where` — column profiling with a SQL filter**

![difflake show stats](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_show_stats.png)

**`difflake show --freq` — value frequency distribution**

![difflake show freq](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_freq.png)

---

## Why difflake

Data engineers spend hours answering questions that should take seconds:

- Did the schema change between yesterday and today?
- Which columns drifted after the pipeline ran?
- How many rows were added, removed, or changed?
- Why are my downstream metrics off?

difflake answers all of these in one command. Unlike data quality tools such as Great Expectations or Soda — which validate data against rules you define in advance — difflake is a **differ**. It discovers what changed between two snapshots with no pre-configuration.

It is the only actively maintained open-source tool that does schema, statistical, and row-level diff across files in a single command.

---

## Install

```bash
pip install difflake
```

Verify:

```bash
difflake --version
difflake formats
```

Cloud storage extras (optional):

```bash
pip install difflake[s3]      # adds boto3 for AWS S3
pip install difflake[gcs]     # adds google-cloud-storage
pip install difflake[azure]   # adds azure-storage-blob
pip install difflake[cloud]   # all three
```

DuckDB extensions for Delta Lake, Avro, and Iceberg are installed automatically on first use. No manual setup needed.

**Requirements:** Python 3.9+. No database, no server, no external services.

---

## Get sample data

All examples in this README use two files — `jan.parquet` and `feb.parquet` — from the NYC Yellow Taxi dataset.

Download them once before following along:

```bash
curl -L -o jan.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

curl -L -o feb.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet
```

> **Heads up:** This data is hosted on CloudFront, which rate-limits repeated programmatic requests. Download once with `curl` and run all examples against your local copies.

After downloading, confirm things work:

```bash
difflake show jan.parquet
# Shows: schema, row count, and first 5 rows
```

---

## Quick start

```bash
# What changed between January and February?
difflake compare jan.parquet feb.parquet

# Schema only — column additions, removals, type changes
difflake compare jan.parquet feb.parquet --mode schema

# Stats only — which columns drifted?
difflake compare jan.parquet feb.parquet --mode stats

# Row-level diff — which rows were added, removed, or changed?
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id

# Full diff + HTML report
difflake compare jan.parquet feb.parquet --output html --out report.html
open report.html
```

---

## Commands

### compare — diff two datasets

```bash
difflake compare SOURCE TARGET [options]
difflake diff SOURCE TARGET [options]   # alias, works identically
```

| Flag | Description |
|---|---|
| `--mode` | `full` (default), `schema`, `stats`, `rows` |
| `--key COL` | Primary key for row-level diff. Comma-separate for composite keys |
| `--output` | `cli` (default), `html`, `json`, `markdown` |
| `--out FILE` | Write report to a file |
| `--threshold N` | Drift alert threshold, default `0.15` (15%) |
| `--where "SQL"` | Filter rows before diffing |
| `--sample N` | Random sample of N rows |
| `--limit N` | First N rows in file order |
| `--columns a,b` | Diff only these columns |
| `--ignore-columns a,b` | Exclude these columns |
| `--verbose` | Show sample changed rows in output |
| `--config FILE` | Load settings from a YAML file |

Examples:

```bash
# Compare with a 5% drift threshold
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05

# Compare only specific columns
difflake compare jan.parquet feb.parquet --mode stats --columns fare_amount,trip_distance

# Composite primary key
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id,pickup_datetime

# Ignore audit columns that always differ
difflake compare jan.parquet feb.parquet --ignore-columns updated_at,_loaded_at

# Save a JSON report for CI pipelines
difflake compare jan.parquet feb.parquet --output json --out report.json
```

---

### show — inspect a file

```bash
difflake show FILE [options]
```

With no flags, `show` runs in **overview mode** — schema, row count, and first 5 rows in one output. This is the fastest way to understand an unfamiliar file.

```bash
# Overview — schema + count + first 5 rows (default)
difflake show jan.parquet

# Column names and types only
difflake show jan.parquet --schema

# Row count, column count, file size
difflake show jan.parquet --count

# Per-column stats: nulls, cardinality, min, max, mean
difflake show jan.parquet --stats

# First 20 rows
difflake show jan.parquet --rows 20

# Last 10 rows
difflake show jan.parquet --tail --rows 10

# Top-10 most common values for a column
difflake show jan.parquet --freq payment_type

# Sorted view — top 5 highest fares
difflake show jan.parquet --order-by fare_amount DESC --rows 5

# Profile only rows matching a condition
difflake show jan.parquet --where "fare_amount > 100" --stats

# Specific columns only
difflake show jan.parquet --columns vendor_id,fare_amount,trip_distance
```

---

### validate — assert data quality

`difflake validate` checks a file against assertions and exits with code 1 if any check fails. Plug it directly into CI.

```bash
difflake validate FILE [checks] [options]
```

```bash
# Basic quality checks
difflake validate jan.parquet \
  --min-rows 1000 \
  --not-null vendor_id \
  --unique trip_id \
  --min-val fare_amount:0 \
  --max-val passenger_count:9 \
  --column-exists pickup_datetime

# Check a filtered subset
difflake validate jan.parquet --where "payment_type = 1" --min-rows 500

# Stop on the first failure
difflake validate jan.parquet --min-rows 1000 --not-null vendor_id --fail-fast

# Load checks from a config file
difflake validate jan.parquet --config difflake.yaml
```

**Available checks:**

| Check | Example | Description |
|---|---|---|
| `--min-rows N` | `--min-rows 1000` | At least N rows |
| `--max-rows N` | `--max-rows 1000000` | At most N rows |
| `--not-null COL` | `--not-null vendor_id` | Column has no nulls |
| `--unique COL` | `--unique trip_id` | Column values are unique |
| `--min-val COL:N` | `--min-val fare_amount:0` | All values >= N |
| `--max-val COL:N` | `--max-val age:120` | All values <= N |
| `--column-exists COL` | `--column-exists created_at` | Column exists in schema |
| `--where-count "EXPR":N` | `--where-count "status='bad'":0` | Rows matching expr == N |

YAML config format:

```yaml
validate:
  checks:
    - kind: min_rows
      value: 1000
    - kind: not_null
      column: vendor_id
    - kind: unique
      column: trip_id
    - kind: where_count
      expr: "fare_amount < 0"
      value: 0
```

---

### query — run SQL against any file

`difflake query` registers the file as a view named `t` and runs any SQL against it. No database setup needed.

```bash
difflake query FILE "SQL" [options]
```

```bash
# Explore the data
difflake query jan.parquet "SELECT * FROM t LIMIT 10"

# Aggregate
difflake query jan.parquet "SELECT payment_type, COUNT(*) AS n, ROUND(AVG(fare_amount),2) AS avg_fare FROM t GROUP BY 1 ORDER BY 2 DESC"

# Filter and count
difflake query jan.parquet "SELECT COUNT(*) FROM t WHERE fare_amount > 100"

# Export to CSV
difflake query jan.parquet "SELECT vendor_id, fare_amount FROM t" --output csv --out fares.csv

# Export to JSON
difflake query jan.parquet "SELECT * FROM t WHERE passenger_count = 0" --output json

# Limit output rows
difflake query jan.parquet "SELECT * FROM t ORDER BY fare_amount DESC" --limit 20

# Works on any format difflake supports
difflake query s3://bucket/data.parquet "SELECT COUNT(*) FROM t"
difflake query events.jsonl "SELECT event_type, COUNT(*) FROM t GROUP BY 1"
```

---

## Diff modes

### Schema diff

Detects every structural change between two datasets. Reads only file metadata — never loads row data. Safe and instant on files with billions of rows.

```bash
difflake compare jan.parquet feb.parquet --mode schema
```

Detects:

- **Added columns** — present in target but not in source
- **Removed columns** — present in source but not in target
- **Type changes** — e.g. `INTEGER` → `DOUBLE`
- **Renamed columns** — detected via Jaro-Winkler string similarity (`user_id` → `userId` is flagged as a rename, not a drop + add)
- **Column order changes**

Example output:

```
SCHEMA DIFF
  + Added   : subscription_tier (VARCHAR)
  ~ Changed : age  INTEGER → DOUBLE
  ↔ Renamed : user_id → userId  (92% similar)
```

---

### Statistics diff

Computes per-column drift metrics and flags columns that changed beyond a threshold. All computation stays inside DuckDB — no data enters Python memory.

```bash
difflake compare jan.parquet feb.parquet --mode stats
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05
```

**Numeric columns:** mean, median, standard deviation, min, max, null rate, cardinality, KL divergence.

**Categorical columns:** null rate, cardinality, new/disappeared categories.

The drift threshold (default `0.15`) controls when a column is flagged. `--threshold 0.05` makes it more sensitive.

KL divergence measures distribution shift. A value above 0.1 means the underlying distribution changed — not just the mean — which catches cases where data has become bimodal or skewed while the average holds steady.

---

### Row-level diff

Identifies which specific rows were added, removed, or changed. Requires a primary key column.

```bash
# Count-only diff (no key required)
difflake compare jan.parquet feb.parquet --mode rows

# Key-based diff — shows exactly which rows changed and what changed in them
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id

# Composite key
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id,pickup_datetime

# Show sample changed rows in the output
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id --verbose
```

The diff uses a SQL `FULL OUTER JOIN` inside DuckDB. DuckDB spills to disk automatically so this works on files of any size.

**Low-cardinality key guard** — if a key column has fewer than 10 unique values, difflake blocks the join and explains why:

```
✗ LOW-CARDINALITY KEY: 'vendor_id' has only 2 unique values across 2.4M rows.
  A join on this key would create billions of row combinations.
  Try a higher-cardinality column. Run: difflake show <file> --schema
```

**Key validation** — if the key column is missing, difflake shows which file it is missing from and suggests the closest column names:

```
✗ 'pickup_datetime' not found in TARGET file.
  Did you mean: lpep_pickup_datetime, tpep_pickup_datetime ?
```

---

## Output formats

| Format | Flag | Description |
|---|---|---|
| CLI | `--output cli` (default) | Colorized Rich terminal output with tables and drift alerts |
| HTML | `--output html` | Self-contained browser report with charts, dark theme, offline-ready |
| JSON | `--output json` | Machine-readable full diff result for CI or downstream automation |
| Markdown | `--output markdown` | GitHub PR-ready report, post as a PR comment |

```bash
# HTML report — open in browser
difflake compare jan.parquet feb.parquet --output html --out report.html
open report.html

# JSON — full diff result for scripts/CI
difflake compare jan.parquet feb.parquet --output json --out report.json

# Markdown — paste into a PR comment
difflake compare jan.parquet feb.parquet --output markdown --out diff.md
cat diff.md | gh pr comment --body-file -

# Auto-generated filename (includes timestamp)
difflake compare jan.parquet feb.parquet --output html
```

The HTML report supports **offline mode** (default on). Chart.js is embedded inline so the report works without an internet connection. Use `--no-offline` to load Chart.js from CDN instead (smaller file size):

```bash
difflake compare jan.parquet feb.parquet --output html --out report.html --no-offline
```

---

## Filters and sampling

### SQL WHERE filter

`--where` applies a SQL predicate before any diff or preview. For Parquet, it is pushed down to DuckDB — only matching row groups are read.

```bash
# Diff only active users
difflake compare jan.parquet feb.parquet --where "payment_type = 1"

# Profile high-fare trips only
difflake show jan.parquet --where "fare_amount > 100" --stats

# Compound conditions
difflake show jan.parquet --where "trip_distance > 10 AND fare_amount < 5"

# Date range
difflake compare jan.parquet feb.parquet --where "pickup_datetime >= '2023-01-15'"
```

### Sampling

For very large files, use `--sample N` to limit rows used for stats and row diff. Schema diff always runs on the full dataset.

```bash
# Random sample of 500k rows
difflake compare jan.parquet feb.parquet --sample 500000

# First 1M rows in file order (deterministic)
difflake compare jan.parquet feb.parquet --limit 1000000
```

`--sample` draws randomly using DuckDB's `USING SAMPLE N ROWS`. `--limit` takes the first N rows in file order — same rows every run.

### Sort and limit for show

```bash
# Top 10 highest fares
difflake show jan.parquet --order-by fare_amount DESC --rows 10

# Earliest 5 trips
difflake show jan.parquet --order-by pickup_datetime ASC --rows 5
```

---

## Cloud storage

Pass cloud URIs directly as paths. Authentication is via environment variables — the same pattern used by the AWS CLI, gcloud, and az CLI.

### AWS S3

```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_DEFAULT_REGION="us-east-1"

difflake compare s3://bucket/v1/users.parquet s3://bucket/v2/users.parquet
difflake show s3://bucket/data/trips.parquet --schema
```

MinIO / S3-compatible:

```bash
export AWS_ENDPOINT_URL="https://minio.mycompany.com"
```

### Google Cloud Storage

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

difflake compare gs://bucket/v1/users.parquet gs://bucket/v2/users.parquet
```

### Azure Blob Storage

```bash
export AZURE_STORAGE_ACCOUNT="mystorageaccount"
export AZURE_STORAGE_KEY="your-key=="

difflake compare az://container/v1/users.parquet az://container/v2/users.parquet

# ADLS Gen2
difflake compare \
  "abfss://bronze@myaccount.dfs.core.windows.net/tables/v1/" \
  "abfss://bronze@myaccount.dfs.core.windows.net/tables/v2/"
```

### Cross-cloud diff

Source and target can be in different clouds or mixed local/cloud:

```bash
# Local vs production — check if local copy matches prod
difflake compare ./users_local.parquet s3://prod-bucket/users.parquet --mode schema

# S3 → GCS migration validation
difflake compare s3://old-bucket/users.parquet gs://new-bucket/users.parquet \
  --mode full --sample 500000
```

---

## Config file

Place a `difflake.yaml` in your project root and difflake reads it automatically. Useful for committing repeatable diff configurations to version control.

```yaml
# difflake.yaml
source: data/v1/users.parquet
target: data/v2/users.parquet
key: user_id
mode: full
output: html
out: reports/diff.html
threshold: 0.10
verbose: true
where: "status = 'active'"
sample: 500000
```

Run with no arguments:

```bash
difflake compare
```

CLI flags always override config values:

```bash
difflake compare --output cli          # override the html output
difflake compare --config other.yaml   # use a different config file
```

A full annotated example is in `difflake.yaml.example`.

---

## CI integration

difflake exits with code 2 when drift alerts fire — easy to fail a pipeline when data changes beyond the threshold.

### GitHub Actions

```yaml
- name: Diff datasets
  run: |
    difflake compare \
      s3://bucket/prod/users.parquet \
      s3://bucket/staging/users.parquet \
      --mode full \
      --threshold 0.10 \
      --output json \
      --out drift_report.json
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}

- name: Upload drift report
  if: always()
  uses: actions/upload-artifact@v4
  with:
    name: drift-report
    path: drift_report.json
```

### Data validation in CI

```yaml
- name: Validate data quality
  run: |
    difflake validate s3://bucket/today/users.parquet \
      --min-rows 10000 \
      --not-null user_id \
      --unique order_id
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
```

### Exit codes

| Code | Meaning |
|---|---|
| 0 | Success — diff ran, no drift alerts |
| 1 | Error — bad path, bad credentials, unsupported format |
| 2 | Success — diff ran, drift alerts fired |

Code 2 is not an error — use it to fail CI when drift is detected.

---

## Structured logging

Global logging flags are available on every command:

```bash
# Show progress during a diff
difflake --log-level INFO compare jan.parquet feb.parquet

# Structured JSON logs for log aggregation
difflake --log-level DEBUG --log-format json compare jan.parquet feb.parquet

# Write logs to a file
difflake --log-level DEBUG --log-file diff.log compare jan.parquet feb.parquet
```

Environment variables: `DIFFLAKE_LOG_LEVEL`, `DIFFLAKE_LOG_FORMAT`, `DIFFLAKE_LOG_FILE`.

---

## Python API

The CLI is a thin wrapper around the `DiffLake` class. All CLI features are available programmatically.

```python
from difflake import DiffLake

result = DiffLake(
    source="jan.parquet",
    target="feb.parquet",
    primary_key="vendor_id",
    mode="full",                   # "full" | "schema" | "stats" | "rows"
    drift_threshold=0.15,
    where="payment_type = 1",      # SQL filter applied before diff
    sample_size=500_000,
    ignore_columns=["updated_at"],
).run()

# Schema diff
print(result.schema_diff.has_changes)
print(result.schema_diff.added_columns)
print(result.schema_diff.removed_columns)
print(result.schema_diff.type_changed_columns)
print(result.schema_diff.renamed_columns)
print(result.schema_diff.summary())   # "1 added, 1 type changed"

# Stats diff
print(result.stats_diff.has_drift)
print(result.stats_diff.drifted_columns)
for col in result.stats_diff.column_diffs:
    print(col.column, col.mean_before, col.mean_after, col.kl_divergence)

# Row diff
print(result.row_diff.row_count_before)
print(result.row_diff.row_count_after)
print(result.row_diff.rows_added)
print(result.row_diff.rows_removed)
print(result.row_diff.rows_changed)
print(result.row_diff.sample_changed)  # list of dicts with before/after values

# Drift alerts
print(result.drift_alerts)
print(result.elapsed_seconds)

# Export reports
result.to_html("report.html")
result.to_json("report.json")
result.to_markdown("diff.md")

# Return as string instead of writing to file
html = result.to_html()
md = result.to_markdown()
```

**Cloud paths, Delta Lake, and cross-format work identically:**

```python
# S3 vs S3
result = DiffLake(
    source="s3://bucket/prod/users.parquet",
    target="s3://bucket/staging/users.parquet",
).run()

# Delta Lake
result = DiffLake(
    source="path/to/delta_table_v1/",
    target="path/to/delta_table_v2/",
    primary_key="user_id",
).run()

# CSV vs Parquet
result = DiffLake(
    source="old_data.csv",
    target="new_data.parquet",
    primary_key="id",
).run()
```

---

## Performance

Benchmarked against locally downloaded NYC Yellow Taxi Parquet files (see [Get sample data](#get-sample-data)) on a MacBook Air (M-series). **0 MB Python memory overhead** at every scale — data never leaves DuckDB.

| Scenario | Rows | File size | Time |
|---|---|---|---|
| Schema diff | any | any | < 0.3s |
| Row count diff | 35.2M | 527 MB | 0.2s |
| Frequency (`--freq`) | 35.2M | 282 MB | 0.2s |
| Stats (`--sample 100k`) | 6M | 91 MB | 1.3s |
| Stats (`--sample 500k`) | 6M | 91 MB | 6.1s |
| Stats (`--sample 500k`) | 16.2M | 486 MB | 8.0s |
| Stats (`--sample 500k`) | **35.2M** | **527 MB** | **11.2s** |
| Stats (`--sample 1000k`) | 35.2M | 527 MB | 23.1s |
| Stats full scan | 6M | 91 MB | 12.7s |
| Full diff + HTML (`--sample 500k`) | 35.2M | 527 MB | 11.2s |

**Why it's fast:** Schema diff reads only file metadata. Stats diff runs as a single-pass SQL aggregation — only the result numbers (mean, min, max) touch Python. Row diff is a native `FULL OUTER JOIN` with automatic disk spill. No data ever enters Python memory.

**Sampling scales correctly:** `--sample 500k` on 35M rows (11.2s) is only 1.8x slower than on 6M rows (6.1s). The extra time is one additional Parquet file scan, not proportional to row count.

---

## How it works

difflake is built on DuckDB, an embedded analytical database that runs entirely inside your Python process. No server, no external service, no data size limit.

When you run a diff, difflake:

1. **Registers** source and target as DuckDB SQL views. For Parquet, the file is not read yet — only metadata.
2. **Schema diff** — runs `DESCRIBE` on both views. No row data is loaded.
3. **Stats diff** — runs a single SQL aggregation per column group. Only the tiny result numbers (mean, min, max, etc.) come back to Python.
4. **Row diff** — runs a `FULL OUTER JOIN` inside DuckDB on the primary key, counts added/removed/changed rows, and fetches a small sample for display.
5. **Renders** the report from the tiny outputs.

This means schema diff is always instant, stats diff on a 10 GB file uses no more memory than on a 10 MB file, and row diff on 50M rows works because DuckDB handles the join on disk.

---

## Supported formats

| Format | Extension | Directory | Cloud | Notes |
|---|---|---|---|---|
| Parquet | `.parquet` `.pq` | Hive partitions | S3, GCS, Azure, HTTPS | Native columnar, fastest |
| CSV / TSV | `.csv` `.tsv` | glob concat | Yes | Type inference, multi-file merge |
| JSON | `.json` | Yes | Yes | Array of records |
| JSONL / NDJSON | `.jsonl` `.ndjson` | Yes | Yes | Streaming scan |
| Delta Lake | directory | Yes | Yes | Auto-installed delta extension |
| Avro | `.avro` | — | Yes | Auto-installed avro extension |
| Iceberg | directory / URI | Yes | Yes | Auto-installed iceberg extension |
| Parquet over HTTP | `https://...` | — | — | Any public URL |

Format is detected automatically from the file extension or directory structure. Use `--format` to override when the extension is ambiguous.

Cross-format comparison is supported — compare a CSV source against a Parquet target, or a local file against a cloud path.

---

## Contributing

```bash
git clone https://github.com/VinoKmani/difflake
cd difflake
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -v
```

Run checks before submitting a PR:

```bash
ruff check difflake/
mypy difflake/
pytest tests/ --cov=difflake --cov-report=term-missing
```

Pull requests welcome.

---

## License

MIT — see [LICENSE](LICENSE).

difflake is free to use, modify, and distribute. If you build something on top of difflake, a link back to this repository is appreciated.

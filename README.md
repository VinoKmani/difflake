# difflake

**git diff, but for your data lake.**

[![CI](https://github.com/VinoKmani/difflake/actions/workflows/ci.yml/badge.svg)](https://github.com/VinoKmani/difflake/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/difflake.svg)](https://pypi.org/project/difflake/)
[![Coverage](https://img.shields.io/badge/coverage-93%25-brightgreen.svg)](https://github.com/VinoKmani/difflake)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

You changed a pipeline. Something downstream broke. You need to know exactly what changed in the data — which columns shifted, which rows were added, which values drifted.

difflake answers that in one command:

```bash
pip install difflake
difflake compare old.parquet new.parquet
```

No config files. No database. No server. Works on Parquet, CSV, JSON, Delta Lake, and more.

---

## What it does

```bash
# Schema diff — what columns were added, removed, or changed type?
difflake compare old.parquet new.parquet --mode schema

# Stats diff — which columns drifted statistically?
difflake compare old.parquet new.parquet --mode stats

# Row diff — which rows were added, removed, or changed?
difflake compare old.parquet new.parquet --mode rows --key user_id

# Inspect any file without running a diff
difflake show users.parquet --stats
difflake show users.parquet --where "country = 'US'" --count

# Assert data quality in CI — exits 1 if any check fails
difflake validate users.parquet --min-rows 10000 --not-null user_id --unique email

# Run ad-hoc SQL against any file
difflake query users.parquet "SELECT country, COUNT(*) FROM t GROUP BY 1 ORDER BY 2 DESC"
```

---

## Table of contents

- [Install](#install)
- [Get sample data](#get-sample-data)
- [Commands](#commands)
  - [compare](#compare)
  - [show](#show)
  - [validate](#validate)
  - [query](#query)
- [Diff modes](#diff-modes)
- [Output formats](#output-formats)
- [Filters and sampling](#filters-and-sampling)
- [Cloud storage](#cloud-storage)
- [Config file](#config-file)
- [CI integration](#ci-integration)
- [Structured logging](#structured-logging)
- [Python API](#python-api)
- [Performance](#performance)
- [How it works](#how-it-works)
- [Supported formats](#supported-formats)
- [Common gotchas](#common-gotchas)
- [Contributing](#contributing)

---

## Install

```bash
pip install difflake
```

DuckDB extensions for Delta Lake, Avro, Iceberg, and cloud storage are installed automatically on first use. Nothing else to configure.

Cloud credential helpers (optional):

```bash
pip install difflake[s3]      # boto3 for AWS S3
pip install difflake[gcs]     # google-cloud-storage
pip install difflake[azure]   # azure-storage-blob
pip install difflake[cloud]   # all three
```

**Requirements:** Python 3.9+. No database. No server.

---

## Get sample data

All examples in this README use two files — `jan.parquet` and `feb.parquet` — from the NYC Yellow Taxi dataset. Download them once before following along:

```bash
curl -L -o jan.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

curl -L -o feb.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet
```

> **Note:** CloudFront rate-limits repeated programmatic requests. Download once with `curl` — every example below runs against your local copies.

Confirm everything is working:

```bash
difflake show jan.parquet --schema
```

A few things worth knowing about this dataset before running examples:

- **Column names are PascalCase** — `VendorID`, `RatecodeID`, `PULocationID`, not `vendor_id`. Commands are case-sensitive.
- **VendorID has only 2 unique values** — it is not a good primary key on its own. Use a composite key like `--key VendorID,tpep_pickup_datetime` for row-level diff.
- **Schema changes between months** — January and February have type differences (`BIGINT` → `INTEGER` on several columns) and a column rename (`airport_fee` → `Airport_fee`). This makes it a realistic test dataset, but don't be surprised when drift alerts fire.

---

## Commands

### compare

Compare two datasets. This is the core command.

```bash
difflake compare SOURCE TARGET [options]
difflake diff SOURCE TARGET [options]   # alias — works identically
```

| Flag | Default | Description |
|---|---|---|
| `--mode` | `full` | `full`, `schema`, `stats`, or `rows` |
| `--key COL` | — | Primary key for row diff. Comma-separate for composite keys |
| `--output` | `cli` | `cli`, `html`, `json`, or `markdown` |
| `--out FILE` | — | Write report to a file |
| `--threshold N` | `0.15` | Drift alert threshold (15%) |
| `--where "SQL"` | — | Filter rows before diffing |
| `--sample N` | — | Random sample of N rows |
| `--limit N` | — | First N rows in file order (deterministic) |
| `--columns a,b` | — | Diff only these columns |
| `--ignore-columns a,b` | — | Exclude these columns |
| `--verbose` | off | Show sample changed rows in output |
| `--config FILE` | — | Load settings from a YAML file |

```bash
# Schema only — instant, reads no row data
difflake compare jan.parquet feb.parquet --mode schema

# Stats diff — which columns drifted?
difflake compare jan.parquet feb.parquet --mode stats

# Tighter threshold — flag anything that drifted more than 5%
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05

# Row-level diff — VendorID alone is low-cardinality, use a composite key
difflake compare jan.parquet feb.parquet --key VendorID,tpep_pickup_datetime

# Exclude columns you don't want to diff
difflake compare jan.parquet feb.parquet --ignore-columns airport_fee,congestion_surcharge

# Save an HTML report
difflake compare jan.parquet feb.parquet --output html --out report.html

# Save a JSON report for downstream automation
difflake compare jan.parquet feb.parquet --output json --out report.json
```

---

### show

Inspect any file without running a diff. With no flags, prints schema + row count + first 5 rows in a single output — the fastest way to get oriented on an unfamiliar dataset.

```bash
difflake show FILE [options]
```

```bash
# Overview — schema + count + first 5 rows (default)
difflake show jan.parquet

# Schema only
difflake show jan.parquet --schema

# Row count, column count, file size
difflake show jan.parquet --count

# Per-column stats: nulls, cardinality, min, max, mean
difflake show jan.parquet --stats

# First 20 rows
difflake show jan.parquet --rows 20

# Last 10 rows
difflake show jan.parquet --tail --rows 10

# Top 10 most common values for a column
difflake show jan.parquet --freq payment_type

# Top 5 highest fares
difflake show jan.parquet --order-by "fare_amount DESC" --rows 5

# Profile only rows that match a condition
difflake show jan.parquet --where "fare_amount > 100" --stats

# Specific columns only
difflake show jan.parquet --columns VendorID,fare_amount,trip_distance
```

---

### validate

Assert data quality against a set of checks. Exits with code 1 if any check fails — plug it directly into a CI pipeline.

```bash
difflake validate FILE [checks] [options]
```

Using `users_v1.jsonl` from the [Get sample data](#get-sample-data) section:

```bash
# Multiple checks in one command — all pass on users_v1.jsonl
difflake validate users_v1.jsonl \
  --min-rows 100 \
  --not-null user_id \
  --unique user_id \
  --min-val score:0 \
  --max-val score:100 \
  --column-exists status

# Validate a filtered subset — e.g. only active users
difflake validate users_v1.jsonl --where "status = 'active'" --min-rows 100

# Stop on the first failure
difflake validate users_v1.jsonl --min-rows 100 --fail-fast

# Load checks from a config file
difflake validate users_v1.jsonl --config difflake.yaml
```

**Available checks:**

| Check | Example | What it asserts |
|---|---|---|
| `--min-rows N` | `--min-rows 1000` | At least N rows |
| `--max-rows N` | `--max-rows 1000000` | At most N rows |
| `--not-null COL` | `--not-null user_id` | No nulls in column |
| `--unique COL` | `--unique order_id` | All values are unique |
| `--min-val COL:N` | `--min-val score:0` | All values >= N |
| `--max-val COL:N` | `--max-val age:120` | All values <= N |
| `--column-exists COL` | `--column-exists created_at` | Column exists in schema |
| `--where-count "EXPR":N` | `--where-count "score<0":0` | Rows matching expr == N |

YAML config format:

```yaml
validate:
  checks:
    - kind: min_rows
      value: 1000
    - kind: not_null
      column: user_id
    - kind: unique
      column: order_id
    - kind: where_count
      expr: "score < 0"
      value: 0
```

---

### query

Run any SQL against any file. The file is registered as a DuckDB view named `t`. No database setup needed.

```bash
difflake query FILE "SQL" [options]
```

```bash
# Quick look at the data
difflake query jan.parquet "SELECT * FROM t LIMIT 10"

# Aggregate by category
difflake query jan.parquet \
  "SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
   FROM t GROUP BY 1 ORDER BY 2 DESC"

# Find duplicate IDs — should return 0 rows if user_id is unique
difflake query users_v1.jsonl \
  "SELECT user_id, COUNT(*) AS n FROM t GROUP BY 1 HAVING n > 1"

# Check for nulls across every column
difflake query jan.parquet \
  "SELECT COUNT(*) - COUNT(VendorID) AS null_vendor,
          COUNT(*) - COUNT(fare_amount) AS null_fare
   FROM t"

# Sanity check after a pipeline run — any negative fares?
difflake query jan.parquet "SELECT COUNT(*) FROM t WHERE fare_amount < 0"

# Export results to CSV
difflake query jan.parquet \
  "SELECT VendorID, fare_amount, trip_distance FROM t WHERE fare_amount > 100" \
  --output csv --out high_fares.csv

# Works on all supported formats — CSV, JSON, Delta Lake, S3, and more
difflake query events.jsonl "SELECT event_type, COUNT(*) FROM t GROUP BY 1"
difflake query s3://bucket/data.parquet "SELECT COUNT(*) FROM t"
```

---

## Diff modes

### Schema diff

Reads only file metadata — never loads row data. Runs in under a second regardless of file size.

```bash
difflake compare jan.parquet feb.parquet --mode schema
```

Detects:

- **Added columns** — present in target, not in source
- **Removed columns** — present in source, not in target
- **Type changes** — e.g. `INTEGER` → `DOUBLE`
- **Renamed columns** — detected using Jaro-Winkler string similarity. `user_id` → `userId` is flagged as a rename, not an independent drop and add
- **Column order changes**

---

### Statistics diff

Computes per-column drift metrics entirely inside DuckDB. No row data enters Python memory.

```bash
difflake compare jan.parquet feb.parquet --mode stats
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05
```

**Numeric columns:** mean, median, std dev, min, max, null rate, cardinality, and KL divergence (distribution shift score).

**Categorical columns:** null rate, cardinality, new values that appeared, values that disappeared.

The drift threshold (default `0.15`) controls when a column is flagged as drifted. KL divergence is especially useful — it catches cases where the mean is stable but the distribution shape has changed (bimodal, long-tailed, etc.) that a simple mean comparison would miss.

---

### Row-level diff

Identifies exactly which rows were added, removed, or changed between two datasets. Requires a primary key column.

```bash
# Count-only diff — no key needed
difflake compare jan.parquet feb.parquet --mode rows

# Key-based — composite key because VendorID alone has only 2 unique values
difflake compare jan.parquet feb.parquet --mode rows --key VendorID,tpep_pickup_datetime

# Show sample changed rows inline
difflake compare jan.parquet feb.parquet --mode rows --key VendorID,tpep_pickup_datetime --verbose
```

Uses a SQL `FULL OUTER JOIN` inside DuckDB with automatic disk spill — works on files of any size without loading data into memory.

---

## Output formats

| Format | Flag | Best for |
|---|---|---|
| CLI | `--output cli` (default) | Daily use — colorized terminal output with tables and drift alerts |
| HTML | `--output html` | Sharing — self-contained browser report with charts, works offline |
| JSON | `--output json` | Automation — full machine-readable result for CI or downstream scripts |
| Markdown | `--output markdown` | PRs — paste directly into a GitHub PR comment |

```bash
# HTML report — open in browser, no internet required
difflake compare jan.parquet feb.parquet --output html --out report.html
open report.html

# JSON — parse in scripts or store as a pipeline artifact
difflake compare jan.parquet feb.parquet --output json --out report.json

# Markdown — post as a PR comment
difflake compare jan.parquet feb.parquet --output markdown --out diff.md
cat diff.md | gh pr comment --body-file -
```

The HTML report embeds Chart.js inline by default so it works without an internet connection. Use `--no-offline` to load Chart.js from CDN instead (smaller file size):

```bash
difflake compare jan.parquet feb.parquet --output html --out report.html --no-offline
```

---

## Filters and sampling

### SQL WHERE filter

Applies a SQL predicate before any diff or preview. For Parquet files, the predicate is pushed down to DuckDB — only matching row groups are read from disk, not the entire file.

```bash
# Diff only a filtered subset
difflake compare jan.parquet feb.parquet --where "payment_type = 1"

# Profile only high-fare trips
difflake show jan.parquet --where "fare_amount > 100" --stats

# Compound conditions
difflake show jan.parquet --where "trip_distance > 10 AND fare_amount < 5"

# Date range
difflake compare jan.parquet feb.parquet --where "tpep_pickup_datetime >= '2023-01-15'"
```

### Sampling

Use `--sample N` for stats and row diff on very large files. Schema diff always runs on the full dataset regardless.

```bash
# Random sample — representative cross-section, good for drift analysis
difflake compare jan.parquet feb.parquet --mode stats --sample 500000

# First N rows in file order — deterministic, same rows every run
difflake compare jan.parquet feb.parquet --limit 1000000
```

---

## Cloud storage

### AWS S3 ✅ verified

Tested against the [Ookla Open Data](https://github.com/teamookla/ookla-open-data) public S3 bucket — 3.7M row Parquet file, full schema diff, stats diff with sampling, row count diff, and exit codes all confirmed working.

Public S3 buckets require no credentials — DuckDB uses anonymous access automatically. For private buckets:

```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_DEFAULT_REGION="us-east-1"

difflake compare s3://bucket/v1/users.parquet s3://bucket/v2/users.parquet
difflake compare s3://bucket/v1/users.parquet s3://bucket/v2/users.parquet \
  --mode stats --sample 500000
```

MinIO and other S3-compatible stores:

```bash
export AWS_ENDPOINT_URL="https://minio.mycompany.com"
```

### Google Cloud Storage ⚠️ not yet verified

The implementation follows the same DuckDB `httpfs` pattern as S3. The code is in place and should work, but we have not tested it against a real GCS bucket.

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

difflake compare gs://bucket/v1/users.parquet gs://bucket/v2/users.parquet
```

If you try this and run into any issues, please [open an issue](https://github.com/VinoKmani/difflake/issues).

### Azure Blob Storage ⚠️ not yet verified

Same situation — the code is in place but not tested against a real Azure endpoint.

```bash
export AZURE_STORAGE_ACCOUNT="mystorageaccount"
export AZURE_STORAGE_KEY="your-key=="

difflake compare az://container/v1/users.parquet az://container/v2/users.parquet
```

ADLS Gen2:

```bash
difflake compare \
  "abfss://bronze@myaccount.dfs.core.windows.net/tables/v1/" \
  "abfss://bronze@myaccount.dfs.core.windows.net/tables/v2/"
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
ignore_columns: [updated_at, _loaded_at]
where: "status = 'active'"
sample: 500000
```

```bash
difflake compare                       # reads difflake.yaml automatically
difflake compare --output cli          # CLI flags override config values
difflake compare --config other.yaml   # use a different config file
```

A fully annotated example is in `difflake.yaml.example`.

---

## CI integration

difflake exits with code 2 when drift alerts fire, making it straightforward to fail a pipeline when data changes beyond a threshold.

### Drift detection

```yaml
- name: Check for data drift
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

### Data validation

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

Code 2 is not an error. Use it to fail CI when drift is detected.

---

## Structured logging

```bash
# Show progress during a diff
difflake --log-level INFO compare jan.parquet feb.parquet

# JSON structured logs for Datadog, Splunk, or any log aggregator
difflake --log-level DEBUG --log-format json compare jan.parquet feb.parquet

# Write logs to a file
difflake --log-level DEBUG --log-file diff.log compare jan.parquet feb.parquet
```

Environment variables: `DIFFLAKE_LOG_LEVEL`, `DIFFLAKE_LOG_FORMAT`, `DIFFLAKE_LOG_FILE`.

---

## Python API

The CLI is a thin wrapper around the `DiffLake` class. Everything available in the CLI is available programmatically.

```python
from difflake import DiffLake

result = DiffLake(
    source="jan.parquet",
    target="feb.parquet",
    primary_key=["VendorID", "tpep_pickup_datetime"],
    mode="full",
    drift_threshold=0.15,
    where="payment_type = 1",
    sample_size=500_000,
    ignore_columns=["updated_at"],
).run()

# What changed?
print(result.schema_diff.summary())        # "1 added, 1 type changed"
print(result.stats_diff.drifted_columns)   # ["fare_amount", "trip_distance"]
print(result.row_diff.rows_added)          # 14_823
print(result.row_diff.rows_changed)        # 302
print(result.drift_alerts)                 # list of alert strings

# Export
result.to_html("report.html")
result.to_json("report.json")
result.to_markdown("diff.md")
```

<details>
<summary>Full result reference</summary>

```python
result.schema_diff.has_changes
result.schema_diff.added_columns        # list of ColumnSchemaDiff
result.schema_diff.removed_columns
result.schema_diff.type_changed_columns
result.schema_diff.renamed_columns

result.stats_diff.has_drift
result.stats_diff.drifted_columns
for col in result.stats_diff.column_diffs:
    print(col.column, col.mean_before, col.mean_after, col.kl_divergence)

result.row_diff.row_count_before
result.row_diff.row_count_after
result.row_diff.rows_added
result.row_diff.rows_removed
result.row_diff.rows_changed
result.row_diff.sample_changed          # list of dicts with before/after values
result.elapsed_seconds
```

</details>

S3, Delta Lake, and cross-format comparison work the same way:

```python
# S3
DiffLake(
    source="s3://bucket/v1/users.parquet",
    target="s3://bucket/v2/users.parquet",
).run()

# Delta Lake
DiffLake(source="delta_v1/", target="delta_v2/", primary_key="user_id").run()

# CSV vs Parquet — cross-format is supported
DiffLake(source="old.csv", target="new.parquet", primary_key="id").run()
```

---

## Performance

Benchmarked on locally downloaded NYC Yellow Taxi Parquet files (MacBook Air, M-series). All queries run inside DuckDB — **0 MB Python memory overhead** at every scale.

| Scenario | Rows | File size | Time |
|---|---|---|---|
| Schema diff | any | any | < 0.3s |
| Row count diff | 35.2M | 527 MB | 0.2s |
| Frequency (`--freq`) | 35.2M | 282 MB | 0.2s |
| Stats (`--sample 100k`) | 6M | 91 MB | 1.3s |
| Stats (`--sample 500k`) | 6M | 91 MB | 6.1s |
| Stats (`--sample 500k`) | **35.2M** | **527 MB** | **11.2s** |
| Stats full scan | 6M | 91 MB | 12.7s |
| Full diff + HTML report | 35.2M | 527 MB | 11.2s |

For comparison, loading a 527 MB Parquet file into pandas takes roughly 8–12s and consumes 3–5 GB of memory — before any computation begins. difflake runs the full stats diff in the same time with no memory cost.

**Sampling scales well:** `--sample 500k` on 35M rows (11.2s) is only 1.8x slower than on 6M rows (6.1s). The extra time is one additional file scan, not proportional to row count.

---

## How it works

difflake uses DuckDB, an embedded analytical database that runs entirely inside your Python process — no server, no external service, no data size limit.

1. Source and target are registered as DuckDB SQL views. For Parquet, the file is not read yet — only the metadata.
2. `DESCRIBE` runs on both views for schema diff. No row data is loaded.
3. A single SQL aggregation query computes all stats (mean, std, min, max, null rate) per column group in one pass. Only those result numbers come back to Python.
4. A `FULL OUTER JOIN` runs inside DuckDB for row diff. DuckDB spills to disk automatically when needed.
5. The report is rendered from those small outputs.

Schema diff is always instant. Stats diff on a 10 GB file uses no more Python memory than on a 10 MB file. Row diff on 50M rows works because DuckDB owns the join — Python never sees the data.

---

## Supported formats

| Format | Extension | Notes |
|---|---|---|
| Parquet | `.parquet` `.pq` | Native columnar, fastest. Hive partitions and directories supported |
| CSV / TSV | `.csv` `.tsv` | Type inference, multi-file glob concat |
| JSON | `.json` | Array of records |
| JSONL / NDJSON | `.jsonl` `.ndjson` | Streaming scan |
| Delta Lake | directory | Auto-installed delta extension |
| Avro | `.avro` | Auto-installed avro extension |
| Iceberg | directory / URI | Auto-installed iceberg extension |
| Parquet over HTTPS | `https://...` | Any publicly accessible URL |

Format is detected automatically from the file extension or directory structure. Use `--format` to override when the extension is ambiguous.

Cross-format comparison is supported — you can compare a CSV source against a Parquet target, or a local file against a cloud path.

---

## Common gotchas

**DuckDB extension install fails behind a corporate proxy**

DuckDB downloads extensions on first use. If you're behind a proxy, set `HTTP_PROXY` / `HTTPS_PROXY` before running difflake, or pre-install the extensions manually:

```python
import duckdb
conn = duckdb.connect()
conn.execute("INSTALL delta; INSTALL iceberg; INSTALL avro;")
```

**Low-cardinality key error**

```
✗ LOW-CARDINALITY KEY: 'VendorID' has only 2 unique values across 3M rows.
```

The key column needs to identify rows uniquely, or close to it. A column with 2 unique values across millions of rows would produce a cartesian join explosion. Use a composite key instead — for the NYC taxi sample data, `--key VendorID,tpep_pickup_datetime` works well.

**Key column not found**

```
✗ 'user_id' not found in TARGET file.
  Did you mean: userId, user_uuid ?
```

difflake suggests the closest column names. If the column was renamed between files, run `--mode schema` first to confirm, then pass the correct key name.

**Noisy drift alerts on partition columns**

If you're comparing quarterly files, the `quarter` or `year` column will always show 100% drift — that's expected. Use `--ignore-columns quarter,year` to suppress it.

**Stats diff is slow on a very large file**

Use `--sample N`. `--sample 500000` on a 35M-row file completes in ~11s and gives representative results for most drift analysis.

---

## Contributing

```bash
git clone https://github.com/VinoKmani/difflake
cd difflake
python3 -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"
pytest tests/ -v
```

Before submitting a PR:

```bash
ruff check difflake/
mypy difflake/
pytest tests/ --cov=difflake --cov-report=term-missing
```

Pull requests welcome.

---

## License

MIT — see [LICENSE](LICENSE).

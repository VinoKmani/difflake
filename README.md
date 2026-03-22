# difflake

**git diff, but for your data lake.**

[![CI](https://github.com/VinoKmani/difflake/actions/workflows/ci.yml/badge.svg)](https://github.com/VinoKmani/difflake/actions/workflows/ci.yml)
[![PyPI version](https://badge.fury.io/py/difflake.svg)](https://pypi.org/project/difflake/)
[![Python 3.9+](https://img.shields.io/badge/python-3.9+-blue.svg)](https://www.python.org/downloads/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

You changed a pipeline. Something downstream broke. You need to know **what changed in the data** — which columns shifted, which rows were added, which values drifted.

difflake answers that in one command:

```bash
pip install difflake
difflake compare old.parquet new.parquet
```

That's it. No config files, no database, no server. Works on Parquet, CSV, JSON, Delta Lake, S3, GCS, Azure, and more.

---

## What it does

```bash
# Schema diff — what columns were added, removed, or changed type?
difflake compare old.parquet new.parquet --mode schema

# Stats diff — which columns drifted statistically?
difflake compare old.parquet new.parquet --mode stats

# Row diff — which rows were added, removed, or changed?
difflake compare old.parquet new.parquet --mode rows --key user_id

# Inspect any file — no diff needed
difflake show users.parquet --stats
difflake show users.parquet --where "country = 'US'" --count

# Assert data quality in CI
difflake validate users.parquet --min-rows 10000 --not-null user_id --unique email

# Run ad-hoc SQL against any file
difflake query users.parquet "SELECT country, COUNT(*) FROM t GROUP BY 1 ORDER BY 2 DESC"
```

---

## How is it different from X?

| Tool | What it does | How difflake differs |
|---|---|---|
| Great Expectations / Soda | Validate data against rules you write | difflake **discovers** what changed — no rules to write |
| dbt tests | Assert invariants after a model runs | difflake diffs two snapshots, no dbt project needed |
| pandas / polars | Pull data into memory and compare | difflake never loads data into Python — runs inside DuckDB, works at any scale |
| custom SQL scripts | Ad-hoc diffing with hand-rolled queries | difflake is a one-liner that handles schema, stats, and rows together |

---

## Install

```bash
pip install difflake
```

DuckDB extensions for Delta Lake, Avro, Iceberg, and cloud storage install automatically on first use. Nothing else to configure.

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

All examples below use two files — `jan.parquet` and `feb.parquet` — from the NYC Yellow Taxi dataset. Download them once:

```bash
curl -L -o jan.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet

curl -L -o feb.parquet \
  https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet
```

> **Note:** CloudFront rate-limits repeated programmatic requests. Download once with `curl` — all examples run locally after that.

Confirm it works:

```bash
difflake show jan.parquet
```

---

## Table of contents

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

## Screenshots

![difflake compare schema](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_compare_schema.png)

*Schema diff: 5 column type changes and 1 rename detected in 4.4 seconds.*

![difflake compare stats](https://raw.githubusercontent.com/VinoKmani/difflake/main/docs/screenshot_compare_stats.png)

*Stats diff: mean drift, cardinality changes, KL divergence, and datetime shift alerts per column. 10.2s on 6M rows, 0 MB memory.*

---

## Commands

### compare

Compare two datasets. The core command.

```bash
difflake compare SOURCE TARGET [options]
difflake diff SOURCE TARGET [options]   # same thing — alias
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

Examples:

```bash
# Full diff (schema + stats + rows) with row-level key
difflake compare jan.parquet feb.parquet --key vendor_id

# Stricter drift threshold — flag anything that changed more than 5%
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05

# Ignore audit columns that always differ
difflake compare jan.parquet feb.parquet --ignore-columns updated_at,_loaded_at

# Save an HTML report
difflake compare jan.parquet feb.parquet --output html --out report.html

# Save a JSON report for downstream scripts
difflake compare jan.parquet feb.parquet --output json --out report.json

# Composite primary key
difflake compare jan.parquet feb.parquet --key vendor_id,pickup_datetime
```

---

### show

Inspect any file — no diff needed.

With no flags, `show` prints schema + row count + first 5 rows in one output. Best starting point for an unfamiliar file.

```bash
difflake show FILE [options]
```

```bash
# Overview (default) — schema + count + first 5 rows
difflake show jan.parquet

# Schema only
difflake show jan.parquet --schema

# Row and column counts, file size
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
difflake show jan.parquet --order-by fare_amount DESC --rows 5

# Profile only matching rows
difflake show jan.parquet --where "fare_amount > 100" --stats

# Specific columns only
difflake show jan.parquet --columns vendor_id,fare_amount,trip_distance
```

---

### validate

Assert data quality. Exits with code 1 if any check fails — plug directly into CI.

```bash
difflake validate FILE [checks] [options]
```

```bash
# Multiple checks in one command
difflake validate jan.parquet \
  --min-rows 1000 \
  --not-null vendor_id \
  --unique trip_id \
  --min-val fare_amount:0 \
  --max-val passenger_count:9 \
  --column-exists pickup_datetime

# Check a filtered subset only
difflake validate jan.parquet --where "payment_type = 1" --min-rows 500

# Stop on first failure
difflake validate jan.parquet --min-rows 1000 --fail-fast

# Load checks from config
difflake validate jan.parquet --config difflake.yaml
```

**Available checks:**

| Check | Example | What it asserts |
|---|---|---|
| `--min-rows N` | `--min-rows 1000` | At least N rows |
| `--max-rows N` | `--max-rows 1000000` | At most N rows |
| `--not-null COL` | `--not-null user_id` | No nulls in column |
| `--unique COL` | `--unique order_id` | All values are unique |
| `--min-val COL:N` | `--min-val fare_amount:0` | All values >= N |
| `--max-val COL:N` | `--max-val age:120` | All values <= N |
| `--column-exists COL` | `--column-exists created_at` | Column exists in schema |
| `--where-count "EXPR":N` | `--where-count "fare_amount<0":0` | Rows matching expr == N |

YAML config:

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
      expr: "fare_amount < 0"
      value: 0
```

---

### query

Run any SQL against any file. The file is registered as a view named `t`.

```bash
difflake query FILE "SQL" [options]
```

```bash
# Explore the data
difflake query jan.parquet "SELECT * FROM t LIMIT 10"

# Group by with aggregation
difflake query jan.parquet \
  "SELECT payment_type, COUNT(*) AS trips, ROUND(AVG(fare_amount), 2) AS avg_fare
   FROM t GROUP BY 1 ORDER BY 2 DESC"

# Find unexpected values
difflake query jan.parquet "SELECT * FROM t WHERE passenger_count = 0 LIMIT 20"

# Export to CSV
difflake query jan.parquet "SELECT vendor_id, fare_amount FROM t" \
  --output csv --out fares.csv

# Limit output rows
difflake query jan.parquet "SELECT * FROM t ORDER BY fare_amount DESC" --limit 20

# Works on all supported formats
difflake query events.jsonl "SELECT event_type, COUNT(*) FROM t GROUP BY 1"
difflake query s3://bucket/data.parquet "SELECT COUNT(*) FROM t"
```

---

## Diff modes

### Schema diff

Detects every structural change. Reads only file metadata — never loads row data. Instant even on files with billions of rows.

```bash
difflake compare jan.parquet feb.parquet --mode schema
```

Detects:

- **Added columns** — present in target, not in source
- **Removed columns** — present in source, not in target
- **Type changes** — e.g. `INTEGER` → `DOUBLE`
- **Renamed columns** — detected via Jaro-Winkler similarity. `user_id` → `userId` is flagged as a rename, not a drop + add
- **Column order changes**

---

### Statistics diff

Per-column drift metrics. All computation stays inside DuckDB — no data enters Python memory.

```bash
difflake compare jan.parquet feb.parquet --mode stats
difflake compare jan.parquet feb.parquet --mode stats --threshold 0.05
```

**Numeric columns:** mean, median, std dev, min, max, null rate, cardinality, KL divergence.

**Categorical columns:** null rate, cardinality, new/disappeared category values.

The drift threshold (default `0.15`) controls when a column is flagged. KL divergence catches cases where the mean is stable but the distribution shape has changed (bimodal, skewed, etc.).

---

### Row-level diff

Identifies exactly which rows were added, removed, or changed. Requires a primary key.

```bash
# Count diff — no key needed
difflake compare jan.parquet feb.parquet --mode rows

# Key-based diff — which rows changed and what changed in them
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id

# Composite key
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id,pickup_datetime

# Show sample changed rows
difflake compare jan.parquet feb.parquet --mode rows --key vendor_id --verbose
```

Uses a SQL `FULL OUTER JOIN` inside DuckDB with automatic disk spill — works on files of any size.

---

## Output formats

```bash
# CLI — colorized terminal output (default)
difflake compare jan.parquet feb.parquet

# HTML — self-contained browser report with charts
difflake compare jan.parquet feb.parquet --output html --out report.html
open report.html

# JSON — full machine-readable result for CI/scripts
difflake compare jan.parquet feb.parquet --output json --out report.json

# Markdown — paste into a GitHub PR comment
difflake compare jan.parquet feb.parquet --output markdown --out diff.md
cat diff.md | gh pr comment --body-file -
```

The HTML report embeds Chart.js inline by default so it works offline. Use `--no-offline` to load Chart.js from CDN (smaller file):

```bash
difflake compare jan.parquet feb.parquet --output html --out report.html --no-offline
```

---

## Filters and sampling

### SQL WHERE filter

Applies a SQL predicate before any diff or preview. For Parquet, pushed down to DuckDB — only matching row groups are read.

```bash
# Diff only a subset of rows
difflake compare jan.parquet feb.parquet --where "payment_type = 1"

# Profile matching rows only
difflake show jan.parquet --where "fare_amount > 100" --stats

# Compound conditions
difflake show jan.parquet --where "trip_distance > 10 AND fare_amount < 5"

# Date range
difflake compare jan.parquet feb.parquet --where "pickup_datetime >= '2023-01-15'"
```

### Sampling

```bash
# Random sample — good for stats diff on huge files
difflake compare jan.parquet feb.parquet --sample 500000

# First N rows in file order — deterministic, same rows every run
difflake compare jan.parquet feb.parquet --limit 1000000
```

---

## Cloud storage

Pass cloud URIs directly as paths. Auth is via environment variables.

**AWS S3**

```bash
export AWS_ACCESS_KEY_ID="AKIA..."
export AWS_SECRET_ACCESS_KEY="your-secret"
export AWS_DEFAULT_REGION="us-east-1"

difflake compare s3://bucket/v1/users.parquet s3://bucket/v2/users.parquet
```

**Google Cloud Storage**

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/service-account.json"

difflake compare gs://bucket/v1/users.parquet gs://bucket/v2/users.parquet
```

**Azure Blob Storage**

```bash
export AZURE_STORAGE_ACCOUNT="mystorageaccount"
export AZURE_STORAGE_KEY="your-key=="

difflake compare az://container/v1/users.parquet az://container/v2/users.parquet
```

**Cross-cloud diff**

```bash
# Local vs S3 — is your local copy in sync with prod?
difflake compare ./users.parquet s3://prod-bucket/users.parquet --mode schema

# S3 → GCS migration check
difflake compare s3://old-bucket/users.parquet gs://new-bucket/users.parquet \
  --mode full --sample 500000
```

---

## Config file

Place a `difflake.yaml` in your project root and difflake reads it automatically.

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
difflake compare                      # uses difflake.yaml
difflake compare --output cli         # CLI flag overrides config
difflake compare --config other.yaml  # use a different config file
```

A full annotated example is in `difflake.yaml.example`.

---

## CI integration

### GitHub Actions — drift detection

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

### GitHub Actions — data validation

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
| 0 | Success, no drift alerts |
| 1 | Error (bad path, bad credentials, unsupported format) |
| 2 | Success, drift alerts fired |

Code 2 is not an error. Use it to fail CI when drift is detected.

---

## Structured logging

```bash
# Show progress during a diff
difflake --log-level INFO compare jan.parquet feb.parquet

# JSON structured logs for log aggregators (Datadog, Splunk, etc.)
difflake --log-level DEBUG --log-format json compare jan.parquet feb.parquet

# Write logs to a file
difflake --log-level DEBUG --log-file diff.log compare jan.parquet feb.parquet
```

Environment variables: `DIFFLAKE_LOG_LEVEL`, `DIFFLAKE_LOG_FORMAT`, `DIFFLAKE_LOG_FILE`.

---

## Python API

```python
from difflake import DiffLake

result = DiffLake(
    source="jan.parquet",
    target="feb.parquet",
    primary_key="vendor_id",
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

Full result fields:

```python
result.schema_diff.has_changes
result.schema_diff.added_columns       # list of ColumnSchemaDiff
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
result.row_diff.sample_changed         # list of dicts with before/after values
result.elapsed_seconds
```

Cloud, Delta Lake, and cross-format work identically:

```python
# S3
DiffLake(source="s3://bucket/v1/users.parquet", target="s3://bucket/v2/users.parquet").run()

# Delta Lake
DiffLake(source="delta_v1/", target="delta_v2/", primary_key="user_id").run()

# CSV vs Parquet
DiffLake(source="old.csv", target="new.parquet", primary_key="id").run()
```

---

## Performance

Benchmarked on locally downloaded NYC Yellow Taxi Parquet files (MacBook Air, M-series). **0 MB Python memory** at every scale.

| Scenario | Rows | File size | Time |
|---|---|---|---|
| Schema diff | any | any | < 0.3s |
| Row count diff | 35.2M | 527 MB | 0.2s |
| Frequency (`--freq`) | 35.2M | 282 MB | 0.2s |
| Stats (`--sample 100k`) | 6M | 91 MB | 1.3s |
| Stats (`--sample 500k`) | 6M | 91 MB | 6.1s |
| Stats (`--sample 500k`) | **35.2M** | **527 MB** | **11.2s** |
| Stats full scan | 6M | 91 MB | 12.7s |
| Full diff + HTML (`--sample 500k`) | 35.2M | 527 MB | 11.2s |

**Why it's fast:** Schema diff reads only file metadata. Stats diff is a single SQL aggregation pass — only the result numbers (mean, min, max) touch Python. Row diff is a native `FULL OUTER JOIN` with automatic disk spill. No data ever enters Python memory.

---

## How it works

difflake is built on DuckDB, an embedded analytical database that runs inside your Python process.

1. Source and target are registered as DuckDB SQL views — the file is not read yet.
2. `DESCRIBE` is run on both views for schema diff. No row data loaded.
3. A single SQL aggregation query per column group runs for stats diff. Only the tiny result numbers come back to Python.
4. A `FULL OUTER JOIN` inside DuckDB runs for row diff. DuckDB spills to disk automatically.
5. The report is rendered from those small outputs.

Result: schema diff is always instant, stats diff on a 10 GB file uses no more Python memory than on a 10 MB file, and row diff on 50M rows works because DuckDB owns the join.

---

## Supported formats

| Format | Extension | Notes |
|---|---|---|
| Parquet | `.parquet` `.pq` | Native columnar, fastest. Hive partitions supported |
| CSV / TSV | `.csv` `.tsv` | Type inference, multi-file glob |
| JSON | `.json` | Array of records |
| JSONL / NDJSON | `.jsonl` `.ndjson` | Streaming scan |
| Delta Lake | directory | Auto-installed delta extension |
| Avro | `.avro` | Auto-installed avro extension |
| Iceberg | directory / URI | Auto-installed iceberg extension |
| Parquet over HTTP | `https://...` | Any public URL |

All formats work with all commands. Cross-format comparison supported (e.g. CSV vs Parquet).

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
✗ LOW-CARDINALITY KEY: 'vendor_id' has only 2 unique values across 2.4M rows.
```

The key column must uniquely (or near-uniquely) identify rows. A column with 2 unique values across millions of rows will produce a cartesian join explosion. Pick a higher-cardinality column or use a composite key.

**Key column not found**

```
✗ 'user_id' not found in TARGET file.
  Did you mean: userId, user_uuid ?
```

difflake suggests the closest column names. Check for renames between the two files — if the column was renamed, use `--mode schema` first to confirm, then pass the correct key name.

**Cloud access denied**

```
✗ S3 access denied for s3://my-bucket/data.parquet
  Set: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_DEFAULT_REGION
```

difflake converts credential errors into a message listing the exact environment variables to set.

**Stats diff is slow on a huge file**

Use `--sample N` to limit rows. `--sample 500000` on a 35M-row file takes ~11s and gives representative results for most drift analysis.

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

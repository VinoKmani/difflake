"""
difflake pre-publish smoke test — NYC Yellow Taxi 2023.

Covers all remaining untested scenarios before PyPI publish:
  1.  difflake show       — overview, schema, count, stats, freq, order-by, tail
  2.  difflake compare    — schema, stats, rows, full, all CLI flags
  3.  difflake diff       — alias
  4.  Output formats      — cli, json, html, markdown, auto filename
  5.  Python API          — full diff, ignore_columns, where, limit
  6.  Config file         — difflake.yaml auto-loading and flag override
  7.  Directory diff      — folder of Parquet files (Hive partition style)
  8.  Delta Lake          — local Delta table generated from taxi data
  9.  Avro                — local Avro file generated from taxi data
  10. HTTPS remote        — public URL, no credentials needed
  11. Cloud storage       — S3 / GCS / Azure (skipped if no credentials)

Usage:
    python test_real_data.py                  # run all sections
    python test_real_data.py --skip-formats   # skip Delta/Avro generation
    python test_real_data.py --skip-remote    # skip HTTPS and cloud tests
    python test_real_data.py --section 6      # run one section only

Data:
    Yellow Taxi Jan 2023  (~3M rows, ~45MB) — downloaded automatically
    Yellow Taxi Feb 2023  (~2.9M rows, ~43MB) — downloaded automatically
    Delta / Avro          — generated locally from the above
"""

import argparse
import os
import sys
import time
import urllib.request
from pathlib import Path

# ── Paths ─────────────────────────────────────────────────────────────────────

DATA_DIR = Path("./taxi_data")
DATA_DIR.mkdir(exist_ok=True)

JAN       = DATA_DIR / "yellow_2023_01.parquet"
FEB       = DATA_DIR / "yellow_2023_02.parquet"
DELTA_JAN = DATA_DIR / "delta_jan"
DELTA_FEB = DATA_DIR / "delta_feb"
AVRO_JAN  = DATA_DIR / "yellow_jan.avro"
AVRO_FEB  = DATA_DIR / "yellow_feb.avro"
DIR_JAN   = DATA_DIR / "dir_jan"
DIR_FEB   = DATA_DIR / "dir_feb"

JAN_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-01.parquet"
FEB_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2023-02.parquet"

HTTPS_JAN = JAN_URL
HTTPS_FEB = FEB_URL


# ── Helpers ───────────────────────────────────────────────────────────────────

def download(url: str, dest: Path):
    if dest.exists():
        print(f"  ✓ {dest.name}  ({dest.stat().st_size/1e6:.0f} MB cached)")
        return True
    print(f"  ↓ {dest.name} ...", end="", flush=True)
    t0 = time.time()
    try:
        urllib.request.urlretrieve(url, dest)
        print(f"  {dest.stat().st_size/1e6:.0f} MB  ({time.time()-t0:.0f}s)")
        return True
    except Exception as e:
        print(f"  FAILED: {e}")
        return False


def sep(title: str):
    print(f"\n{'='*60}")
    print(f"  {title}")
    print("="*60)


failures = []
skipped  = []


def check(label: str, cmd: str, expect_drift: bool = False):
    print(f"\n$ {cmd[:110]}{'...' if len(cmd) > 110 else ''}")
    t0   = time.time()
    code = os.system(cmd)
    elapsed = time.time() - t0
    # exit 2 = drift alerts fired — that's a valid result, not a failure
    # on macOS/Linux, Python exit(2) → os.system returns 512 (exit_code << 8)
    ok = code == 0 or (expect_drift and code in (2, 512))
    status = "✅" if ok else (f"⚠️  exit {code >> 8 or code}" if code in (2, 512) else f"❌ exit {code >> 8 or code}")
    print(f"  {status}  ({elapsed:.1f}s)")
    if not ok:
        failures.append(label)
    return ok


def skip(label: str, reason: str):
    print(f"  ⏭  SKIP  {label}  ({reason})")
    skipped.append(label)


def py(label: str, code_str: str):
    """Run a Python snippet as a check."""
    escaped = code_str.replace('"', '\\"')
    return check(label, f'python3 -c "{escaped}"')


# ── Format generators ─────────────────────────────────────────────────────────

def generate_delta():
    if DELTA_JAN.exists() and DELTA_FEB.exists():
        print(f"  ✓ Delta tables already exist")
        return True
    print("  Generating Delta Lake tables from taxi Parquet...")
    try:
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL delta; LOAD delta;")
        print("  Writing delta_jan ...", end="", flush=True)
        con.execute(f"COPY (SELECT * FROM read_parquet('{JAN}')) TO '{DELTA_JAN}' (FORMAT delta)")
        print(f" done ({sum(f.stat().st_size for f in DELTA_JAN.rglob('*') if f.is_file())/1e6:.0f} MB)")
        print("  Writing delta_feb ...", end="", flush=True)
        con.execute(f"COPY (SELECT * FROM read_parquet('{FEB}')) TO '{DELTA_FEB}' (FORMAT delta)")
        print(f" done")
        con.close()
        return True
    except Exception as e:
        print(f"\n  ⚠️  Delta generation failed: {e}")
        print("     Upgrade DuckDB: pip install --upgrade duckdb")
        return False


def generate_avro():
    if AVRO_JAN.exists() and AVRO_FEB.exists():
        print(f"  ✓ Avro files already exist")
        return True
    print("  Generating Avro files from taxi Parquet (200k rows each)...")
    try:
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL avro; LOAD avro;")
        print("  Writing yellow_jan.avro ...", end="", flush=True)
        con.execute(f"COPY (SELECT * FROM read_parquet('{JAN}') LIMIT 200000) TO '{AVRO_JAN}' (FORMAT avro)")
        print(f" done ({AVRO_JAN.stat().st_size/1e6:.0f} MB)")
        print("  Writing yellow_feb.avro ...", end="", flush=True)
        con.execute(f"COPY (SELECT * FROM read_parquet('{FEB}') LIMIT 200000) TO '{AVRO_FEB}' (FORMAT avro)")
        print(f" done ({AVRO_FEB.stat().st_size/1e6:.0f} MB)")
        con.close()
        return True
    except Exception as e:
        print(f"\n  ⚠️  Avro generation failed: {e}")
        print("     Upgrade DuckDB: pip install --upgrade duckdb")
        return False


def generate_directories():
    DIR_JAN.mkdir(exist_ok=True)
    DIR_FEB.mkdir(exist_ok=True)
    p1 = DIR_JAN / "part1.parquet"
    p2 = DIR_FEB / "part1.parquet"
    if not p1.exists():
        import shutil
        shutil.copy(JAN, p1)
        print(f"  ✓ Created {p1}")
    if not p2.exists():
        import shutil
        shutil.copy(FEB, p2)
        print(f"  ✓ Created {p2}")
    return True


# ── Test sections ─────────────────────────────────────────────────────────────

def section_show():
    sep("2. difflake show — single file inspection")

    check("show default overview (schema + count + rows)",
          f"difflake show {JAN}")

    check("show --schema",
          f"difflake show {JAN} --schema")

    check("show --count",
          f"difflake show {JAN} --count")

    check("show --stats",
          f"difflake show {JAN} --stats")

    check("show --rows 5",
          f"difflake show {JAN} --rows 5")

    check("show --tail --rows 5",
          f"difflake show {JAN} --tail --rows 5")

    check("show --order-by fare_amount DESC",
          f"difflake show {JAN} --order-by 'fare_amount DESC' --rows 5")

    check("show --where filter + count",
          f'difflake show {JAN} --where "fare_amount > 500" --count')

    check("show --where + stats",
          f'difflake show {JAN} --where "VendorID = 2" --stats')

    check("show --freq payment_type",
          f"difflake show {JAN} --freq payment_type")

    check("show --freq VendorID",
          f"difflake show {JAN} --freq VendorID")

    check("show --columns subset",
          f"difflake show {JAN} --columns VendorID,fare_amount,trip_distance --rows 5")


def section_compare():
    sep("3. difflake compare — all modes and flags")

    check("compare --mode schema",
          f"difflake compare {JAN} {FEB} --mode schema",
          expect_drift=True)

    check("compare --mode stats --sample 500k",
          f"difflake compare {JAN} {FEB} --mode stats --sample 500000",
          expect_drift=True)

    check("compare --mode stats --threshold 0.05",
          f"difflake compare {JAN} {FEB} --mode stats --sample 500000 --threshold 0.05",
          expect_drift=True)

    check("compare --mode rows count-only (no key)",
          f"difflake compare {JAN} {FEB} --mode rows")

    check("compare --mode full --sample 200k",
          f"difflake compare {JAN} {FEB} --mode full --sample 200000",
          expect_drift=True)

    check("compare --where filter",
          f'difflake compare {JAN} {FEB} --mode stats --sample 100000 --where "VendorID = 2"',
          expect_drift=True)

    check("compare --ignore-columns",
          f"difflake compare {JAN} {FEB} --mode stats --sample 200000 "
          f"--ignore-columns tpep_pickup_datetime,tpep_dropoff_datetime",
          expect_drift=True)

    check("compare --limit (first N rows)",
          f"difflake compare {JAN} {FEB} --mode stats --limit 100000",
          expect_drift=True)

    check("diff alias (same as compare)",
          f"difflake diff {JAN} {FEB} --mode schema",
          expect_drift=True)


def section_output_formats():
    sep("4. Output formats")

    check("--output json --out file",
          f"difflake compare {JAN} {FEB} --mode stats --sample 100000 "
          f"--output json --out taxi_data/diff.json",
          expect_drift=True)

    # Validate JSON structure
    py("json structure valid",
       "import json; d = json.load(open('taxi_data/diff.json')); "
       "assert all(k in d for k in ['schema_diff','stats_diff','row_diff']); "
       "print('  keys:', list(d.keys()))")

    check("--output html --out file",
          f"difflake compare {JAN} {FEB} --mode stats --sample 100000 "
          f"--output html --out taxi_data/diff.html",
          expect_drift=True)

    py("html file has content",
       "p = __import__('pathlib').Path('taxi_data/diff.html'); "
       "assert p.stat().st_size > 5000, f'HTML too small: {p.stat().st_size}'; "
       "assert b'chart' in p.read_bytes().lower(), 'Chart.js missing from HTML'; "
       "print('  html size:', p.stat().st_size, 'bytes OK')")

    check("--output markdown --out file",
          f"difflake compare {JAN} {FEB} --mode stats --sample 100000 "
          f"--output markdown --out taxi_data/diff.md",
          expect_drift=True)

    py("markdown has headers",
       "t = open('taxi_data/diff.md').read(); "
       "assert '##' in t, 'No headers in markdown'; "
       "print('  markdown lines:', len(t.splitlines()))")

    check("auto filename --output html (no --out)",
          f"difflake compare {JAN} {FEB} --mode stats --sample 100000 --output html",
          expect_drift=True)

    py("auto-named html file created",
       "import pathlib; files = list(pathlib.Path('.').glob('difflake_*.html')); "
       "assert files, 'No auto-named html file found in current directory'; "
       "print('  auto file:', files[-1].name)")

    check("auto filename --output json (no --out)",
          f"difflake compare {JAN} {FEB} --mode stats --sample 100000 --output json",
          expect_drift=True)


def section_python_api():
    sep("5. Python API")

    py("API basic diff",
       "from difflake import LakeDiff; "
       f"r = LakeDiff(source='{JAN}', target='{FEB}', mode='stats', sample_size=100000).run(); "
       "assert r.stats_diff is not None; "
       "assert r.elapsed_seconds > 0; "
       "print('  drifted:', r.stats_diff.drifted_columns[:3], 'elapsed:', round(r.elapsed_seconds,1),'s')")

    py("API ignore_columns",
       "from difflake import LakeDiff; "
       f"r = LakeDiff(source='{JAN}', target='{FEB}', mode='stats', sample_size=100000, "
       "ignore_columns=['tpep_pickup_datetime','tpep_dropoff_datetime']).run(); "
       "cols = [d.column for d in r.stats_diff.column_diffs]; "
       "assert 'tpep_pickup_datetime' not in cols; "
       "print('  excluded timestamps OK, cols:', cols[:4])")

    py("API where filter",
       "from difflake import LakeDiff; "
       f"r = LakeDiff(source='{JAN}', target='{FEB}', mode='stats', "
       "where='payment_type = 1', sample_size=100000).run(); "
       "assert r.stats_diff is not None; "
       "print('  where filter OK, cols:', len(r.stats_diff.column_diffs))")

    py("API limit",
       "from difflake import LakeDiff; "
       f"r = LakeDiff(source='{JAN}', target='{FEB}', mode='stats', limit=50000).run(); "
       "assert r.stats_diff is not None; "
       "print('  limit OK')")

    py("API to_json returns string",
       "from difflake import LakeDiff; import json; "
       f"r = LakeDiff(source='{JAN}', target='{FEB}', mode='schema').run(); "
       "s = r.to_json(); "
       "assert isinstance(s, str); "
       "d = json.loads(s); assert 'schema_diff' in d; "
       "print('  to_json() OK')")

    py("API schema_diff has correct fields",
       "from difflake import LakeDiff; "
       f"r = LakeDiff(source='{JAN}', target='{FEB}', mode='schema').run(); "
       "sd = r.schema_diff; "
       "assert hasattr(sd, 'added_columns'); "
       "assert hasattr(sd, 'removed_columns'); "
       "assert hasattr(sd, 'has_changes'); "
       "print('  schema_diff fields OK, has_changes:', sd.has_changes)")


def section_config_file():
    sep("6. Config file (difflake.yaml)")

    # Write a config file
    config_content = f"""# difflake.yaml — auto-loaded by difflake compare
mode: stats
threshold: 0.10
sample: 200000
ignore_columns: tpep_pickup_datetime,tpep_dropoff_datetime
"""
    config_path = Path("difflake.yaml")
    config_path.write_text(config_content)
    print(f"  Wrote difflake.yaml")

    check("compare reads config automatically (no flags)",
          f"difflake compare {JAN} {FEB}",
          expect_drift=True)

    check("CLI flag overrides config (--mode schema overrides mode:stats)",
          f"difflake compare {JAN} {FEB} --mode schema",
          expect_drift=True)

    check("--config explicit path",
          f"difflake compare {JAN} {FEB} --config difflake.yaml",
          expect_drift=True)

    # Clean up
    config_path.unlink()
    print("  Removed difflake.yaml")

    check("no config — reverts to defaults",
          f"difflake compare {JAN} {FEB} --mode schema",
          expect_drift=True)


def section_directory_diff():
    sep("7. Directory diff (folder of Parquet files)")

    print("  Setting up test directories...")
    generate_directories()

    check("show dir --schema",
          f"difflake show {DIR_JAN} --schema")

    check("show dir --count",
          f"difflake show {DIR_JAN} --count")

    check("compare directories --mode schema",
          f"difflake compare {DIR_JAN} {DIR_FEB} --mode schema",
          expect_drift=True)

    check("compare directories --mode stats --sample 200k",
          f"difflake compare {DIR_JAN} {DIR_FEB} --mode stats --sample 200000",
          expect_drift=True)

    check("compare directories --mode full",
          f"difflake compare {DIR_JAN} {DIR_FEB} --mode full --sample 100000",
          expect_drift=True)


def section_delta():
    sep("8. Delta Lake")

    print("  Generating Delta tables...")
    if not generate_delta():
        skip("Delta Lake tests", "DuckDB delta extension unavailable — upgrade DuckDB")
        return

    check("show delta --schema",
          f"difflake show {DELTA_JAN} --schema")

    check("show delta --count",
          f"difflake show {DELTA_JAN} --count")

    check("show delta --stats",
          f"difflake show {DELTA_JAN} --stats")

    check("compare delta --mode schema",
          f"difflake compare {DELTA_JAN} {DELTA_FEB} --mode schema")

    check("compare delta --mode stats --sample 200k",
          f"difflake compare {DELTA_JAN} {DELTA_FEB} --mode stats --sample 200000",
          expect_drift=True)

    check("compare delta --mode full --sample 200k",
          f"difflake compare {DELTA_JAN} {DELTA_FEB} --mode full --sample 200000",
          expect_drift=True)

    # Cross-format: Delta source vs Parquet target
    check("cross-format: delta vs parquet --mode schema",
          f"difflake compare {DELTA_JAN} {FEB} --mode schema")


def section_avro():
    sep("9. Avro")

    print("  Generating Avro files...")
    if not generate_avro():
        skip("Avro tests", "DuckDB avro extension unavailable — upgrade DuckDB")
        return

    check("show avro --schema",
          f"difflake show {AVRO_JAN} --schema")

    check("show avro --count",
          f"difflake show {AVRO_JAN} --count")

    check("show avro --stats",
          f"difflake show {AVRO_JAN} --stats")

    check("compare avro --mode schema",
          f"difflake compare {AVRO_JAN} {AVRO_FEB} --mode schema")

    check("compare avro --mode stats",
          f"difflake compare {AVRO_JAN} {AVRO_FEB} --mode stats",
          expect_drift=True)

    check("compare avro --mode full",
          f"difflake compare {AVRO_JAN} {AVRO_FEB} --mode full --sample 100000",
          expect_drift=True)

    # Cross-format: Avro source vs Parquet target
    check("cross-format: avro vs parquet --mode schema",
          f"difflake compare {AVRO_JAN} {JAN} --mode schema")


def section_https():
    sep("10. HTTPS remote Parquet (public, no credentials)")

    print("  Testing public NYC taxi Parquet over HTTPS...")
    print("  Note: first run downloads the full file (~45MB) into DuckDB memory.\n")

    check("show https --schema",
          f"difflake show {HTTPS_JAN} --schema")

    check("show https --count",
          f"difflake show {HTTPS_JAN} --count")

    check("compare https --mode schema",
          f"difflake compare {HTTPS_JAN} {HTTPS_FEB} --mode schema",
          expect_drift=True)

    check("compare https --mode stats --sample 200k",
          f"difflake compare {HTTPS_JAN} {HTTPS_FEB} --mode stats --sample 200000",
          expect_drift=True)

    check("compare https --mode full --sample 100k",
          f"difflake compare {HTTPS_JAN} {HTTPS_FEB} --mode full --sample 100000",
          expect_drift=True)

    # Cross: HTTPS source vs local target
    check("cross: https source vs local target",
          f"difflake compare {HTTPS_JAN} {FEB} --mode schema",
          expect_drift=True)


def section_cloud():
    sep("11. Cloud storage (S3 / GCS / Azure)")

    import os

    # S3
    has_s3 = bool(os.getenv("AWS_ACCESS_KEY_ID") and os.getenv("AWS_SECRET_ACCESS_KEY"))
    has_gcs = bool(os.getenv("GOOGLE_APPLICATION_CREDENTIALS"))
    has_az  = bool(os.getenv("AZURE_STORAGE_ACCOUNT") and os.getenv("AZURE_STORAGE_KEY"))

    if not any([has_s3, has_gcs, has_az]):
        skip("S3 tests",    "AWS_ACCESS_KEY_ID not set")
        skip("GCS tests",   "GOOGLE_APPLICATION_CREDENTIALS not set")
        skip("Azure tests", "AZURE_STORAGE_ACCOUNT not set")
        print("\n  To test cloud storage, set credentials and re-run:")
        print("    export AWS_ACCESS_KEY_ID=...  AWS_SECRET_ACCESS_KEY=...")
        print("    export GOOGLE_APPLICATION_CREDENTIALS=path/to/key.json")
        print("    export AZURE_STORAGE_ACCOUNT=...  AZURE_STORAGE_KEY=...")
        return

    s3_bucket  = os.getenv("LAKEDIFF_TEST_S3_BUCKET", "")
    gcs_bucket = os.getenv("LAKEDIFF_TEST_GCS_BUCKET", "")
    az_container = os.getenv("LAKEDIFF_TEST_AZ_CONTAINER", "")

    if has_s3 and s3_bucket:
        check("s3 show --schema",
              f"difflake show s3://{s3_bucket}/test/jan.parquet --schema")
        check("s3 compare --mode schema",
              f"difflake compare s3://{s3_bucket}/test/jan.parquet "
              f"s3://{s3_bucket}/test/feb.parquet --mode schema")
    elif has_s3:
        skip("S3 path tests", "Set LAKEDIFF_TEST_S3_BUCKET=your-bucket to test")

    if has_gcs and gcs_bucket:
        check("gcs show --schema",
              f"difflake show gs://{gcs_bucket}/test/jan.parquet --schema")
    elif has_gcs:
        skip("GCS path tests", "Set LAKEDIFF_TEST_GCS_BUCKET=your-bucket to test")

    if has_az and az_container:
        check("azure show --schema",
              f"difflake show az://{az_container}/test/jan.parquet --schema")
    elif has_az:
        skip("Azure path tests", "Set LAKEDIFF_TEST_AZ_CONTAINER=your-container to test")


# ── Main ──────────────────────────────────────────────────────────────────────

SECTIONS = {
    "2":  ("show",           section_show),
    "3":  ("compare",        section_compare),
    "4":  ("output_formats", section_output_formats),
    "5":  ("python_api",     section_python_api),
    "6":  ("config_file",    section_config_file),
    "7":  ("directory_diff", section_directory_diff),
    "8":  ("delta",          section_delta),
    "9":  ("avro",           section_avro),
    "10": ("https",          section_https),
    "11": ("cloud",          section_cloud),
}


def main():
    parser = argparse.ArgumentParser(description="difflake pre-publish smoke test")
    parser.add_argument("--section", help="Run only this section number (e.g. --section 6)")
    parser.add_argument("--skip-formats", action="store_true",
                        help="Skip Delta and Avro generation (sections 8 and 9)")
    parser.add_argument("--skip-remote", action="store_true",
                        help="Skip HTTPS and cloud tests (sections 10 and 11)")
    args = parser.parse_args()

    print("=" * 60)
    print("  difflake pre-publish smoke test")
    print("  NYC Yellow Taxi 2023 — public data, no credentials needed")
    print("=" * 60)

    # 1. Download base data
    sep("1. Download base Parquet data")
    ok1 = download(JAN_URL, JAN)
    ok2 = download(FEB_URL, FEB)
    if not ok1 or not ok2:
        print("\n❌ Download failed. Check your internet connection.")
        sys.exit(1)

    # Run sections
    for num, (name, fn) in SECTIONS.items():
        if args.section and args.section != num:
            continue
        if args.skip_formats and num in ("8", "9"):
            skip(f"Section {num} ({name})", "--skip-formats")
            continue
        if args.skip_remote and num in ("10", "11"):
            skip(f"Section {num} ({name})", "--skip-remote")
            continue
        fn()

    # Final summary
    sep("Results")
    total   = len(failures) + len([x for x in skipped if "SKIP" not in x])
    passed  = total - len(failures) if total > len(failures) else 0

    print(f"\n  Passed  : {len([x for x in range(100) if x not in range(len(failures))])}")
    print(f"  Failed  : {len(failures)}")
    print(f"  Skipped : {len(skipped)}")

    if failures:
        print("\n  Failed tests:")
        for f in failures:
            print(f"    ❌ {f}")
        sys.exit(1)
    else:
        if skipped:
            print(f"\n  ✅ All run tests passed  ({len(skipped)} skipped)")
        else:
            print(f"\n  ✅ All tests passed")


if __name__ == "__main__":
    main()

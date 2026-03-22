"""
difflake load test — NYC Yellow Taxi multi-year data.

Downloads multiple months/years of NYC yellow taxi Parquet files,
concatenates them into progressively larger datasets, and benchmarks
difflake at each scale tier.

Scale tiers:
    Small  :   ~3M rows  (1 month)   — baseline
    Medium :  ~15M rows  (5 months)  — typical production table
    Large  :  ~30M rows  (10 months) — large fact table
    XLarge :  ~60M rows  (2 years)   — stress test

Usage:
    python load_test.py              # run all tiers
    python load_test.py --tier small # run one tier only
    python load_test.py --download-only

Requirements:
    pip install difflake duckdb requests
"""

import argparse
import os
import sys
import time
import tracemalloc
import urllib.request
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

# ── Data catalogue ────────────────────────────────────────────────────────────

DATA_DIR = Path("./taxi_data")
DATA_DIR.mkdir(exist_ok=True)

# Public Parquet files from NYC TLC — no auth required
BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"

MONTHS = [
    ("2023", "01"),
    ("2023", "02"),
    ("2023", "03"),
    ("2023", "04"),
    ("2023", "05"),
    ("2022", "01"),
    ("2022", "02"),
    ("2022", "03"),
    ("2022", "04"),
    ("2022", "05"),
    ("2021", "01"),
    ("2021", "02"),
]

TIERS = {
    "small":   MONTHS[:2],    # Jan + Feb 2023  ~6M rows
    "medium":  MONTHS[:5],    # 5 months 2023   ~15M rows
    "large":   MONTHS[:10],   # 10 months       ~30M rows
    "xlarge":  MONTHS[:12],   # 12 months 2 yrs ~36M rows
}


def parquet_path(year: str, month: str) -> Path:
    return DATA_DIR / f"yellow_{year}_{month}.parquet"


def parquet_url(year: str, month: str) -> str:
    return f"{BASE_URL}/yellow_tripdata_{year}-{month}.parquet"


# ── Download ──────────────────────────────────────────────────────────────────

def download_file(year: str, month: str) -> Path:
    dest = parquet_path(year, month)
    if dest.exists():
        size_mb = dest.stat().st_size / 1e6
        print(f"  ✓ {dest.name}  ({size_mb:.0f} MB cached)")
        return dest
    url = parquet_url(year, month)
    print(f"  ↓ {dest.name} ...", end="", flush=True)
    t0 = time.time()
    try:
        urllib.request.urlretrieve(url, dest)
        size_mb = dest.stat().st_size / 1e6
        print(f" {size_mb:.0f} MB  ({time.time()-t0:.0f}s)")
    except Exception as e:
        print(f" FAILED: {e}")
        if dest.exists():
            dest.unlink()
        return None
    return dest


def download_tier(tier: str) -> list[Path]:
    months = TIERS[tier]
    print(f"\nDownloading {len(months)} files for tier '{tier}'...")
    paths = []
    for year, month in months:
        p = download_file(year, month)
        if p:
            paths.append(p)
    return paths


# ── Build combined datasets using DuckDB ─────────────────────────────────────

def build_combined(paths: list[Path], out: Path) -> Optional[int]:
    """Concatenate multiple Parquet files into one using DuckDB."""
    if out.exists():
        import duckdb
        n = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
        size_mb = out.stat().st_size / 1e6
        print(f"  ✓ {out.name}  ({n:,} rows, {size_mb:.0f} MB cached)")
        return n

    import duckdb
    glob = str(DATA_DIR / "yellow_*.parquet")
    file_list = "', '".join(str(p) for p in paths)
    print(f"  Building {out.name} from {len(paths)} files...", end="", flush=True)
    t0 = time.time()
    duckdb.sql(f"""
        COPY (SELECT * FROM read_parquet(['{file_list}']))
        TO '{out}' (FORMAT PARQUET, COMPRESSION ZSTD)
    """)
    n = duckdb.sql(f"SELECT COUNT(*) FROM read_parquet('{out}')").fetchone()[0]
    size_mb = out.stat().st_size / 1e6
    print(f" {n:,} rows, {size_mb:.0f} MB  ({time.time()-t0:.0f}s)")
    return n


# ── Benchmark runner ──────────────────────────────────────────────────────────

@dataclass
class BenchResult:
    label: str
    rows: int
    elapsed: float
    peak_mb: float
    exit_code: int
    error: str = ""

    @property
    def ok(self):
        return self.exit_code in (0, 512)  # 512 = shell exit code 2

    def row_str(self):
        status = "✅" if self.ok else "❌"
        return (f"  {status}  {self.label:<45} "
                f"{self.rows/1e6:>6.1f}M rows  "
                f"{self.elapsed:>7.1f}s  "
                f"{self.peak_mb:>7.0f} MB peak")


def bench(label: str, cmd: str, rows: int) -> BenchResult:
    print(f"\n  $ {cmd[:100]}{'...' if len(cmd)>100 else ''}")
    tracemalloc.start()
    t0 = time.time()
    code = os.system(cmd + " 2>/dev/null")
    elapsed = time.time() - t0
    _, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()
    peak_mb = peak / 1e6
    result = BenchResult(label, rows, elapsed, peak_mb, code)
    status = "✅" if result.ok else "❌"
    print(f"  {status}  {elapsed:.1f}s  peak {peak_mb:.0f} MB")
    return result


# ── Load test suite ───────────────────────────────────────────────────────────

def run_tier(tier: str, src: Path, tgt: Path, rows: int) -> list[BenchResult]:
    """Run all benchmark scenarios for a given data tier."""
    results = []

    def b(label, cmd):
        results.append(bench(label, cmd, rows))

    print(f"\n{'─'*60}")
    print(f"  Tier: {tier.upper()}  —  {rows/1e6:.1f}M rows")
    print(f"  Source: {src.name}  ({src.stat().st_size/1e6:.0f} MB)")
    print(f"  Target: {tgt.name}  ({tgt.stat().st_size/1e6:.0f} MB)")
    print(f"{'─'*60}")

    # Schema diff — should always be instant regardless of scale
    b(f"[{tier}] schema diff",
      f"difflake compare {src} {tgt} --mode schema")

    # Row count diff — fast, no key
    b(f"[{tier}] row count diff",
      f"difflake compare {src} {tgt} --mode rows")

    # Stats with sampling — the main usability benchmark
    for n in [100_000, 500_000, 1_000_000]:
        if n <= rows:
            b(f"[{tier}] stats --sample {n//1000}k",
              f"difflake compare {src} {tgt} --mode stats --sample {n}")

    # Stats full scan (only up to medium tier — large would take too long)
    if tier in ("small", "medium"):
        b(f"[{tier}] stats full scan (no sample)",
          f"difflake compare {src} {tgt} --mode stats")

    # show --stats with sample
    b(f"[{tier}] show --stats --where filter",
      f'difflake show {src} --stats --where "fare_amount > 0 AND trip_distance > 0"')

    # show --freq
    b(f"[{tier}] show --freq payment_type",
      f"difflake show {src} --freq payment_type")

    # Full diff with sample + HTML output
    b(f"[{tier}] full diff --sample 500k --output html",
      f"difflake compare {src} {tgt} --mode full --sample 500000 "
      f"--output html --out taxi_data/load_{tier}.html")

    return results


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="difflake load test")
    parser.add_argument("--tier", choices=list(TIERS.keys()),
                        help="Run only this tier")
    parser.add_argument("--download-only", action="store_true",
                        help="Only download data, don't run benchmarks")
    args = parser.parse_args()

    tiers_to_run = [args.tier] if args.tier else list(TIERS.keys())

    print("=" * 60)
    print("  difflake load test — NYC Yellow Taxi")
    print(f"  Tiers: {', '.join(tiers_to_run)}")
    print("=" * 60)

    # Download all needed files
    all_months_needed = []
    for tier in tiers_to_run:
        all_months_needed.extend(TIERS[tier])
    all_months_needed = list(dict.fromkeys(all_months_needed))  # dedupe

    print(f"\nDownloading {len(all_months_needed)} Parquet files...")
    downloaded = {}
    for year, month in all_months_needed:
        p = download_file(year, month)
        if p:
            downloaded[(year, month)] = p

    if args.download_only:
        print("\n✅ Download complete.")
        return

    # Build combined datasets and run benchmarks
    all_results = []

    for tier in tiers_to_run:
        months = TIERS[tier]
        available = [downloaded[m] for m in months if m in downloaded]
        if len(available) < 2:
            print(f"\n⚠️  Skipping tier '{tier}' — not enough files downloaded")
            continue

        # Use first half as source, second half as target
        # This simulates comparing two time periods
        mid = max(1, len(available) // 2)
        src_files = available[:mid]
        tgt_files = available[mid:] if len(available) > mid else available[:mid]

        # Build combined files
        print(f"\nBuilding combined files for tier '{tier}'...")
        src_combined = DATA_DIR / f"combined_{tier}_src.parquet"
        tgt_combined = DATA_DIR / f"combined_{tier}_tgt.parquet"

        src_rows = build_combined(src_files, src_combined)
        tgt_rows = build_combined(tgt_files, tgt_combined)

        if not src_rows:
            continue

        total_rows = (src_rows or 0) + (tgt_rows or 0)
        results = run_tier(tier, src_combined, tgt_combined, total_rows)
        all_results.extend(results)

    # Print summary table
    print(f"\n{'='*60}")
    print("  RESULTS SUMMARY")
    print(f"{'='*60}")
    print(f"  {'Label':<45} {'Rows':>8}  {'Time':>8}  {'Mem':>8}")
    print(f"  {'-'*45} {'-'*8}  {'-'*8}  {'-'*8}")

    failed = []
    for r in all_results:
        print(r.row_str())
        if not r.ok:
            failed.append(r)

    print(f"\n  {len(all_results) - len(failed)}/{len(all_results)} passed")

    # Performance analysis
    if all_results:
        print(f"\n{'─'*60}")
        print("  PERFORMANCE NOTES")
        print(f"{'─'*60}")

        # Schema diff timing
        schema_results = [r for r in all_results if "schema diff" in r.label]
        if schema_results:
            times = [r.elapsed for r in schema_results]
            print(f"  Schema diff   : {min(times):.1f}s – {max(times):.1f}s  "
                  f"(should be <2s at any scale)")

        # Stats with 500k sample
        sample_results = [r for r in all_results if "500k" in r.label]
        if sample_results:
            times = [r.elapsed for r in sample_results]
            rows  = [r.rows for r in sample_results]
            print(f"  Stats 500k    : {min(times):.1f}s – {max(times):.1f}s  "
                  f"(target: <15s with materialisation)")
            # Check if time grows with rows (it shouldn't with sampling)
            if len(sample_results) > 1:
                ratio = max(times) / min(times)
                if ratio < 2.0:
                    print(f"  ✅ Sampling scales well — {ratio:.1f}x time for "
                          f"{max(rows)/min(rows):.0f}x more rows")
                else:
                    print(f"  ⚠️  Sampling may not be working — {ratio:.1f}x time for "
                          f"{max(rows)/min(rows):.0f}x more rows")

        # Memory
        mem_results = [r for r in all_results if r.peak_mb > 0]
        if mem_results:
            peak = max(r.peak_mb for r in mem_results)
            print(f"  Peak memory   : {peak:.0f} MB  "
                  f"({'✅ OK' if peak < 500 else '⚠️  High'})")

    if failed:
        print(f"\n  ❌ {len(failed)} failed:")
        for r in failed:
            print(f"     {r.label}")
        sys.exit(1)
    else:
        print("\n  ✅ All load tests passed")


if __name__ == "__main__":
    main()

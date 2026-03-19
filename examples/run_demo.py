"""
examples/run_demo.py — Generates two versions of a realistic user dataset
and runs a full diff, outputting CLI, JSON, HTML, and Markdown reports.

Run from the repo root:
    python examples/run_demo.py
"""

from __future__ import annotations

import random
from pathlib import Path
import polars as pl

# ── Generate realistic synthetic datasets ──────────────────────────────────

random.seed(42)

COUNTRIES = ["US", "UK", "CA", "DE", "AU", "FR"]
STATUSES_V1 = ["active", "inactive", "churned"]
STATUSES_V2 = ["active", "inactive", "churned", "suspended"]   # 'suspended' is new
TIERS = ["free", "pro", "enterprise"]

N_V1 = 50_000
N_V2 = 52_500   # 2,500 new users added


def make_v1(n: int) -> pl.DataFrame:
    rng = random.Random(1)
    return pl.DataFrame({
        "user_id": list(range(1, n + 1)),
        "name": [f"User_{i}" for i in range(1, n + 1)],
        "age": [rng.randint(18, 70) for _ in range(n)],
        "revenue": [round(rng.uniform(10, 5000), 2) for _ in range(n)],
        "country": [rng.choice(COUNTRIES) for _ in range(n)],
        "status": [rng.choice(STATUSES_V1) for _ in range(n)],
        "signup_year": [rng.randint(2018, 2023) for _ in range(n)],
    })


def make_v2(v1: pl.DataFrame, extra: int) -> pl.DataFrame:
    rng = random.Random(2)

    # Mutate ~5% of existing rows (revenue changes)
    n = len(v1)
    revenues = v1["revenue"].to_list()
    ages_float = [float(a) for a in v1["age"].to_list()]   # int → float (type drift)
    statuses = v1["status"].to_list()

    mutated_revenues = [
        round(rev * rng.uniform(0.9, 1.4), 2) if rng.random() < 0.05 else rev
        for rev in revenues
    ]
    mutated_statuses = [
        rng.choice(STATUSES_V2) if rng.random() < 0.02 else s
        for s in statuses
    ]

    existing = pl.DataFrame({
        "user_id": v1["user_id"].to_list(),
        "name": v1["name"].to_list(),
        "age": ages_float,                    # ← type changed int→float
        "revenue": mutated_revenues,
        "country": v1["country"].to_list(),
        "status": mutated_statuses,
        "signup_year": v1["signup_year"].to_list(),
        "subscription_tier": [rng.choice(TIERS) for _ in range(n)],  # ← new column
    })

    # Add new rows
    base_id = n + 1
    new_rows = pl.DataFrame({
        "user_id": list(range(base_id, base_id + extra)),
        "name": [f"User_{i}" for i in range(base_id, base_id + extra)],
        "age": [float(rng.randint(18, 70)) for _ in range(extra)],
        "revenue": [round(rng.uniform(10, 5000), 2) for _ in range(extra)],
        "country": [rng.choice(COUNTRIES + ["AU", "JP"]) for _ in range(extra)],  # AU & JP new
        "status": [rng.choice(STATUSES_V2) for _ in range(extra)],
        "signup_year": [rng.randint(2023, 2025) for _ in range(extra)],
        "subscription_tier": [rng.choice(TIERS) for _ in range(extra)],
    })

    return pl.concat([existing, new_rows])


def main():
    out_dir = Path("examples/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("🔧 Generating synthetic datasets...")
    v1 = make_v1(N_V1)
    v2 = make_v2(v1, extra=N_V2 - N_V1)

    src = out_dir / "users_v1.parquet"
    tgt = out_dir / "users_v2.parquet"
    v1.write_parquet(src)
    v2.write_parquet(tgt)
    print(f"   ✅ v1: {len(v1):,} rows → {src}")
    print(f"   ✅ v2: {len(v2):,} rows → {tgt}")

    # ── Run diff ────────────────────────────────────────────────────────────
    print("\n🔍 Running diff...")
    from difflake import LakeDiff

    result = LakeDiff(
        source=src,
        target=tgt,
        primary_key="user_id",
        drift_threshold=0.10,
    ).run()

    # ── CLI output ──────────────────────────────────────────────────────────
    from difflake.reporters.cli_reporter import CliReporter
    CliReporter(result, verbose=True).render()

    # ── JSON report ─────────────────────────────────────────────────────────
    json_path = out_dir / "diff_report.json"
    result.to_json(str(json_path))
    print(f"📄 JSON report: {json_path}")

    # ── HTML report ─────────────────────────────────────────────────────────
    html_path = out_dir / "diff_report.html"
    result.to_html(str(html_path))
    print(f"🌐 HTML report: {html_path}")

    # ── Markdown report ─────────────────────────────────────────────────────
    md_path = out_dir / "diff_report.md"
    result.to_markdown(str(md_path))
    print(f"📝 Markdown report: {md_path}")


if __name__ == "__main__":
    main()

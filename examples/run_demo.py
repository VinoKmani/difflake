"""
examples/run_demo.py — Generates two versions of a realistic user dataset
using pure DuckDB (no Polars or pandas required) and runs a full diff,
outputting CLI, JSON, HTML, and Markdown reports.

Run from the repo root:
    python examples/run_demo.py

Requirements:
    pip install difflake
"""

from __future__ import annotations

from pathlib import Path

import duckdb


def generate_datasets(out_dir: Path) -> tuple[Path, Path]:
    """Generate v1 and v2 user Parquet files using DuckDB SQL."""
    src = out_dir / "users_v1.parquet"
    tgt = out_dir / "users_v2.parquet"

    con = duckdb.connect()

    # ── v1: 50,000 users ─────────────────────────────────────────────────────
    con.execute("SELECT setseed(0.42)")
    con.execute("""
        CREATE TABLE users_v1 AS
        SELECT
            i                                                      AS user_id,
            'User_' || i                                           AS name,
            (18 + (random() * 52)::INTEGER)                        AS age,
            round((10 + random() * 4990)::DECIMAL, 2)              AS revenue,
            CASE (random() * 6)::INTEGER
                WHEN 0 THEN 'US' WHEN 1 THEN 'UK' WHEN 2 THEN 'CA'
                WHEN 3 THEN 'DE' WHEN 4 THEN 'AU' ELSE 'FR'
            END                                                    AS country,
            CASE (random() * 3)::INTEGER
                WHEN 0 THEN 'active' WHEN 1 THEN 'inactive' ELSE 'churned'
            END                                                    AS status,
            (2018 + (random() * 5)::INTEGER)                       AS signup_year
        FROM range(1, 50001) t(i)
    """)
    con.execute(f"COPY users_v1 TO '{src}' (FORMAT PARQUET)")
    n_v1 = con.execute("SELECT COUNT(*) FROM users_v1").fetchone()[0]
    print(f"   ✅ v1: {n_v1:,} rows → {src}")

    # ── v2: mutate existing rows + add 2,500 new users ───────────────────────
    # Changes vs v1:
    #   - age column: INTEGER → DOUBLE  (type drift)
    #   - ~5% of revenues changed
    #   - ~2% of statuses changed; 'suspended' is a new category
    #   - new column: subscription_tier
    #   - 2,500 new rows added (some with new country 'JP')

    con.execute("SELECT setseed(0.84)")
    con.execute("""
        CREATE TABLE users_v2 AS

        -- Mutated existing rows
        SELECT
            user_id,
            name,
            age::DOUBLE                                             AS age,
            CASE WHEN random() < 0.05
                THEN round(revenue * (0.9 + random() * 0.5), 2)
                ELSE revenue
            END                                                     AS revenue,
            country,
            CASE WHEN random() < 0.02
                THEN CASE (random() * 4)::INTEGER
                    WHEN 0 THEN 'active' WHEN 1 THEN 'inactive'
                    WHEN 2 THEN 'churned' ELSE 'suspended'
                END
                ELSE status
            END                                                     AS status,
            signup_year,
            CASE (random() * 3)::INTEGER
                WHEN 0 THEN 'free' WHEN 1 THEN 'pro' ELSE 'enterprise'
            END                                                     AS subscription_tier
        FROM users_v1

        UNION ALL

        -- 2,500 new users
        SELECT
            50000 + i                                               AS user_id,
            'User_' || (50000 + i)                                  AS name,
            (18 + (random() * 52)::DOUBLE)                          AS age,
            round((10 + random() * 4990)::DECIMAL, 2)               AS revenue,
            CASE (random() * 7)::INTEGER
                WHEN 0 THEN 'US' WHEN 1 THEN 'UK' WHEN 2 THEN 'CA'
                WHEN 3 THEN 'DE' WHEN 4 THEN 'AU' WHEN 5 THEN 'FR'
                ELSE 'JP'
            END                                                     AS country,
            CASE (random() * 4)::INTEGER
                WHEN 0 THEN 'active' WHEN 1 THEN 'inactive'
                WHEN 2 THEN 'churned' ELSE 'suspended'
            END                                                     AS status,
            (2023 + (random() * 2)::INTEGER)                        AS signup_year,
            CASE (random() * 3)::INTEGER
                WHEN 0 THEN 'free' WHEN 1 THEN 'pro' ELSE 'enterprise'
            END                                                     AS subscription_tier
        FROM range(1, 2501) t(i)
    """)
    con.execute(f"COPY users_v2 TO '{tgt}' (FORMAT PARQUET)")
    n_v2 = con.execute("SELECT COUNT(*) FROM users_v2").fetchone()[0]
    print(f"   ✅ v2: {n_v2:,} rows → {tgt}")

    con.close()
    return src, tgt


def main():
    out_dir = Path("examples/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    print("🔧 Generating synthetic datasets...")
    src, tgt = generate_datasets(out_dir)

    # ── Run diff ─────────────────────────────────────────────────────────────
    print("\n🔍 Running diff...")
    from difflake import DiffLake
    from difflake.reporters.cli_reporter import CliReporter

    result = DiffLake(
        source=src,
        target=tgt,
        primary_key="user_id",
        drift_threshold=0.10,
    ).run()

    # ── CLI output ────────────────────────────────────────────────────────────
    CliReporter(result, verbose=True).render()

    # ── JSON report ───────────────────────────────────────────────────────────
    json_path = out_dir / "diff_report.json"
    result.to_json(str(json_path))
    print(f"📄 JSON report: {json_path}")

    # ── HTML report ───────────────────────────────────────────────────────────
    html_path = out_dir / "diff_report.html"
    result.to_html(str(html_path))
    print(f"🌐 HTML report: {html_path}")

    # ── Markdown report ───────────────────────────────────────────────────────
    md_path = out_dir / "diff_report.md"
    result.to_markdown(str(md_path))
    print(f"📝 Markdown report: {md_path}")


if __name__ == "__main__":
    main()

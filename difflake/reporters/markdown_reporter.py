"""
Markdown Reporter — produces GitHub-flavored Markdown diff reports.
Useful for dbt PR comments, data pipeline documentation, and CI output.
"""

from __future__ import annotations

from pathlib import Path

from difflake.models import ColumnStatsDiff, DiffResult


def _pct(val: float | None, sign: bool = False) -> str:
    if val is None:
        return "—"
    prefix = "+" if sign and val > 0 else ""
    return f"{prefix}{val:.2f}%"


def _num(val: float | None) -> str:
    if val is None:
        return "—"
    return f"{val:,.4g}"


class MarkdownReporter:
    def __init__(self, result: DiffResult):
        self.r = result

    def render(self, path: str | Path | None = None) -> str:
        lines: list[str] = []
        r = self.r

        lines += [
            "## 🔍 DiffLake Report",
            "",
            "| | Source | Target |",
            "|---|---|---|",
            f"| **Path** | `{r.source_path}` | `{r.target_path}` |",
            f"| **Format** | {r.source_format} | {r.target_format} |",
            f"| **Rows** | {r.row_diff.row_count_before:,} | {r.row_diff.row_count_after:,} |",
            "",
        ]

        # Schema diff
        lines += ["### 📋 Schema Diff", ""]
        sd = r.schema_diff
        if not sd.has_changes:
            lines.append("✅ No schema changes detected.")
        else:
            if sd.added_columns:
                lines.append("**Added columns:**")
                for col in sd.added_columns:
                    lines.append(f"- ➕ `{col.name}` ({col.new_dtype})")
            if sd.removed_columns:
                lines.append("\n**Removed columns:**")
                for col in sd.removed_columns:
                    lines.append(f"- ➖ `{col.name}` ({col.old_dtype})")
            if sd.type_changed_columns:
                lines.append("\n**Type changes:**")
                for col in sd.type_changed_columns:
                    lines.append(f"- 🔁 `{col.name}`: `{col.old_dtype}` → `{col.new_dtype}`")
            if sd.renamed_columns:
                lines.append("\n**Likely renames:**")
                for col in sd.renamed_columns:
                    lines.append(
                        f"- ↔ `{col.rename_from}` → `{col.name}` "
                        f"(similarity: {col.rename_similarity:.0%})"
                    )
        lines.append("")

        # Row diff
        lines += ["### 🔢 Row Diff", ""]
        rd = r.row_diff
        delta = rd.row_count_delta
        sign = "+" if delta >= 0 else ""
        lines += [
            "| Metric | Value |",
            "|---|---|",
            f"| Row count change | {sign}{delta:,} ({_pct(rd.row_count_delta_pct, sign=True)}) |",
        ]
        if rd.key_based_diff:
            lines += [
                f"| Rows added | {rd.rows_added:,} |",
                f"| Rows removed | {rd.rows_removed:,} |",
                f"| Rows changed | {rd.rows_changed:,} |",
                f"| Rows unchanged | {rd.rows_unchanged:,} |",
                f"| Primary key | `{rd.primary_key_used}` |",
            ]
        lines.append("")

        # Stats diff
        lines += ["### 📊 Statistical Diff", ""]
        stats = r.stats_diff.column_diffs
        if stats:
            lines += [
                "| Column | Type | Null % (before → after) | Mean Drift | Cardinality | KL Div | Drifted |",
                "|---|---|---|---|---|---|---|",
            ]
            scol: ColumnStatsDiff
            for scol in stats:
                null_str = (
                    f"{scol.null_rate_before:.1f}% → {scol.null_rate_after:.1f}%"
                    if scol.null_rate_before is not None else "—"
                )
                card_delta = ""
                if scol.cardinality_before is not None and scol.cardinality_after is not None:
                    d = scol.cardinality_after - scol.cardinality_before
                    sign = "+" if d >= 0 else ""
                    card_delta = f"{scol.cardinality_before} → {scol.cardinality_after} ({sign}{d})"

                drifted = "🔴" if scol.is_drifted else "✅"
                lines.append(
                    f"| `{scol.column}` | {scol.dtype_category} | {null_str} "
                    f"| {_pct(scol.mean_drift_pct, sign=True)} "
                    f"| {card_delta or '—'} "
                    f"| {f'{scol.kl_divergence:.3f}' if scol.kl_divergence is not None else '—'} "
                    f"| {drifted} |"
                )
        else:
            lines.append("_No stats computed._")
        lines.append("")

        # Drift alerts
        if r.drift_alerts:
            lines += ["### ⚠️ Drift Alerts", ""]
            for alert in r.drift_alerts:
                lines.append(f"- 🔴 {alert}")
            if r.threshold_used is not None:
                lines.append(f"\n_Threshold: {r.threshold_used:.0%}_")
            lines.append("")

        output = "\n".join(lines)
        if path:
            Path(path).write_text(output)
            return ""
        return output

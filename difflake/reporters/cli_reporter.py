"""
CLI Reporter — produces colorized, human-readable terminal output using Rich.
"""

from __future__ import annotations

from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from difflake.models import ColumnStatsDiff, DiffResult

console = Console()


def _fmt_pct(val: float | None, show_sign: bool = False) -> str:
    if val is None:
        return "—"
    sign = "+" if show_sign and val > 0 else ""
    return f"{sign}{val:.2f}%"


def _fmt_num(val: float | None) -> str:
    if val is None:
        return "—"
    return f"{val:,.4g}"


class CliReporter:
    """Renders a DiffResult to the terminal using Rich."""

    def __init__(self, result: DiffResult, verbose: bool = False):
        self.r = result
        self.verbose = verbose

    def render(self) -> None:
        r = self.r
        console.print()
        console.print(Panel(
            Text("🔍 DiffLake Report", justify="center", style="bold cyan"),
            border_style="cyan",
        ))

        # ── File summary ───────────────────────────────────────────────────
        console.print(f"\n[bold]📁 Source:[/bold] {r.source_path}  "
                      f"([cyan]{r.row_diff.row_count_before:,}[/cyan] rows)")
        console.print(f"[bold]📁 Target:[/bold] {r.target_path}  "
                      f"([cyan]{r.row_diff.row_count_after:,}[/cyan] rows)")
        if r.elapsed_seconds:
            console.print(f"[dim]⏱  Completed in {r.elapsed_seconds:.2f}s[/dim]")

        # ── Schema diff ────────────────────────────────────────────────────
        console.rule("\n[bold yellow]📋 SCHEMA DIFF[/bold yellow]")
        sd = r.schema_diff
        if not sd.has_changes:
            console.print("  [green]✅ No schema changes detected[/green]")
        else:
            for col in sd.added_columns:
                console.print(f"  [green]➕ Added   :[/green] [bold]{col.name}[/bold] "
                               f"([dim]{col.new_dtype}[/dim])")
            for col in sd.removed_columns:
                console.print(f"  [red]➖ Removed :[/red] [bold]{col.name}[/bold] "
                               f"([dim]{col.old_dtype}[/dim])")
            for col in sd.type_changed_columns:
                console.print(f"  [yellow]🔁 Changed :[/yellow] [bold]{col.name}[/bold] "
                               f"[dim]{col.old_dtype}[/dim] → [dim]{col.new_dtype}[/dim]")
            for col in sd.renamed_columns:
                console.print(f"  [blue]↔  Renamed :[/blue] [bold]{col.rename_from}[/bold] → "
                               f"[bold]{col.name}[/bold] "
                               f"[dim](similarity: {col.rename_similarity:.0%})[/dim]")
            if sd.order_changed:
                console.print("  [dim]📋 Column order changed[/dim]")

        # ── Row diff ───────────────────────────────────────────────────────
        console.rule("\n[bold yellow]🔢 ROW DIFF[/bold yellow]")
        rd = r.row_diff
        delta = rd.row_count_delta
        delta_str = f"[green]+{delta:,}[/green]" if delta >= 0 else f"[red]{delta:,}[/red]"
        console.print(f"  Row count: {rd.row_count_before:,} → {rd.row_count_after:,}  ({delta_str})")

        if rd.key_based_diff:
            console.print(f"  [green]➕ Added   :[/green] {rd.rows_added:,} rows")
            console.print(f"  [red]➖ Removed :[/red] {rd.rows_removed:,} rows")
            console.print(f"  [yellow]🔄 Changed :[/yellow] {rd.rows_changed:,} rows "
                           f"(key: [bold]{rd.primary_key_used}[/bold])")
            console.print(f"  [dim]✓  Unchanged: {rd.rows_unchanged:,} rows[/dim]")

            if self.verbose and rd.sample_changed:
                console.print("\n  [dim]Sample changed rows:[/dim]")
                for row in rd.sample_changed[:5]:
                    key_info = row.get("_key", {})
                    key_str = ", ".join(f"{k}={v}" for k, v in key_info.items()) if key_info else str(row.get(rd.primary_key_used or "", "?"))
                    console.print(f"    [bold]{key_str}[/bold]")
                    for field, chg in row.items():
                        if field == "_key":
                            continue
                        if isinstance(chg, dict) and "before" in chg:
                            console.print(f"      [dim]{field}:[/dim] [red]{chg['before']}[/red] → [green]{chg['after']}[/green]")
        else:
            if rd.key_error:
                # Key was specified but validation failed — show the real reason
                for line in rd.key_error.split("\n"):
                    if line.startswith("  x "):
                        console.print(f"  [red]✗{line[3:]}[/red]")
                    elif line.startswith("    Did you mean"):
                        console.print(f"  [yellow]{line.strip()}[/yellow]")
                    elif line.startswith("  Run:"):
                        console.print(f"  [dim]{line.strip()}[/dim]")
                    elif line:
                        console.print(f"  [bold red]{line}[/bold red]")
            else:
                console.print(
                    "  [dim]ℹ️  No primary key specified — showing count diff only. "
                    "Use --key to enable row-level diff.[/dim]"
                )

        # ── Stats diff ─────────────────────────────────────────────────────
        console.rule("\n[bold yellow]📊 STATISTICAL DIFF[/bold yellow]")
        stats = r.stats_diff.column_diffs
        if not stats:
            console.print("  [dim]No stats computed[/dim]")
        else:
            self._render_stats_table(stats)

        # ── Drift alerts ───────────────────────────────────────────────────
        if r.drift_alerts:
            console.rule("\n[bold red]⚠️  DRIFT ALERTS[/bold red]")
            for alert in r.drift_alerts:
                console.print(f"  [red]• {alert}[/red]")
            if r.threshold_used is not None:
                console.print(
                    f"\n  [dim]Threshold: {r.threshold_used:.0%}[/dim]"
                )
        console.print()

    def _render_stats_table(self, stats: list[ColumnStatsDiff]) -> None:
        table = Table(
            box=box.SIMPLE_HEAVY,
            show_header=True,
            header_style="bold",
            padding=(0, 1),
        )
        table.add_column("Column", style="bold", min_width=16)
        table.add_column("Type", style="dim", min_width=10)
        table.add_column("Null %", min_width=14)
        table.add_column("Mean Drift", min_width=10)
        table.add_column("Cardinality", min_width=14)
        table.add_column("KL Div", min_width=8)
        table.add_column("⚠️", min_width=3)

        for col in stats:
            null_str = (
                f"{col.null_rate_before:.1f}% → {col.null_rate_after:.1f}%"
                if col.null_rate_before is not None else "—"
            )

            drift_str = _fmt_pct(col.mean_drift_pct, show_sign=True)
            drift_style = ""
            if col.mean_drift_pct and abs(col.mean_drift_pct) > 15:
                drift_style = "red"
            elif col.mean_drift_pct and abs(col.mean_drift_pct) > 5:
                drift_style = "yellow"

            card_str = "—"
            if col.cardinality_before is not None:
                delta = (col.cardinality_after or 0) - col.cardinality_before
                sign = "+" if delta > 0 else ""
                card_str = f"{col.cardinality_before} → {col.cardinality_after} ({sign}{delta})"

            kl_str = f"{col.kl_divergence:.3f}" if col.kl_divergence is not None else "—"
            alert = "🔴" if col.is_drifted else ""

            table.add_row(
                col.column,
                col.dtype_category,
                null_str,
                Text(drift_str, style=drift_style),
                card_str,
                kl_str,
                alert,
            )

        console.print(table)

        # Print new/dropped categories inline
        for col in stats:
            if col.new_categories:
                console.print(
                    f"  [dim]{col.column}:[/dim] new categories: "
                    f"[green]{', '.join(repr(c) for c in col.new_categories[:10])}[/green]"
                )
            if col.dropped_categories:
                console.print(
                    f"  [dim]{col.column}:[/dim] dropped categories: "
                    f"[red]{', '.join(repr(c) for c in col.dropped_categories[:10])}[/red]"
                )

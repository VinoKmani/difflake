"""
CLI entrypoint — difflake compare / diff / show / formats
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import click
from rich import box
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
from rich.text import Text

from difflake.core import LakeDiff

console     = Console()
err_console = Console(stderr=True, style="red")

_ALL_FORMATS = ["csv", "parquet", "json", "jsonl", "delta", "avro", "iceberg"]


def _load_config(config_path):
    import yaml
    search_paths = []
    if config_path:
        search_paths.append(Path(config_path))
    else:
        search_paths += [Path("difflake.yaml"), Path(".difflake.yaml")]
    for p in search_paths:
        if p.exists():
            try:
                return yaml.safe_load(p.read_text()) or {}
            except Exception as e:
                console.print(f"[yellow]Warning: Could not parse {p}: {e}[/yellow]")
    return {}


def _auto_out(source, target, fmt):
    ext_map = {"json": "json", "html": "html", "markdown": "md"}
    src_stem = Path(source).stem[:20]
    tgt_stem = Path(target).stem[:20]
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    return f"difflake_{src_stem}_vs_{tgt_stem}_{ts}.{ext_map[fmt]}"


def _preflight(source, target):
    from difflake.connection import DuckDBConnection, _detect_format, _read_sql

    def _info(path):
        is_remote = str(path).startswith(("s3://","gs://","gcs://","az://",
                                          "abfs://","http://","https://"))
        size_str = "remote" if is_remote else "—"
        rows_str = "—"
        local = Path(path)
        if not is_remote:
            if local.is_file():
                size = local.stat().st_size
                size_str = (f"{size/1e6:.1f} MB" if size > 1e6
                            else f"{size/1e3:.1f} KB" if size > 1e3
                            else f"{size} bytes")
            elif local.is_dir():
                total = sum(f.stat().st_size for f in local.rglob("*") if f.is_file())
                size_str = f"{total/1e6:.1f} MB" if total > 1e6 else f"{total/1e3:.1f} KB"
            try:
                con = DuckDBConnection()
                fmt = _detect_format(path)
                sql = _read_sql(path, fmt)
                con.register_view("__pf", sql, fmt, path)
                n = con.scalar("SELECT COUNT(*) FROM __pf")
                rows_str = f"{int(n):,}" if n else "—"
                con.close()
            except Exception:
                pass
        return size_str, rows_str

    src_size, src_rows = _info(source)
    tgt_size, tgt_rows = _info(target)
    console.print()
    console.print(f"  [bold]Source[/bold] : [cyan]{source}[/cyan]  [dim]{src_rows} rows · {src_size}[/dim]")
    console.print(f"  [bold]Target[/bold] : [cyan]{target}[/cyan]  [dim]{tgt_rows} rows · {tgt_size}[/dim]")
    console.print()


def _summary_line(result):
    parts = []
    sd = result.schema_diff
    if sd.added_columns:
        parts.append(f"[green]+{len(sd.added_columns)} col{'s' if len(sd.added_columns)>1 else ''} added[/green]")
    if sd.removed_columns:
        parts.append(f"[red]-{len(sd.removed_columns)} col{'s' if len(sd.removed_columns)>1 else ''} removed[/red]")
    if sd.type_changed_columns:
        names = ", ".join(c.name for c in sd.type_changed_columns[:3])
        parts.append(f"[yellow]{names} type changed[/yellow]")
    if sd.renamed_columns:
        parts.append(f"[blue]{len(sd.renamed_columns)} renamed[/blue]")
    rd = result.row_diff
    if rd.rows_added:
        parts.append(f"[green]+{rd.rows_added:,} rows added[/green]")
    if rd.rows_removed:
        parts.append(f"[red]-{rd.rows_removed:,} rows removed[/red]")
    if rd.rows_changed:
        parts.append(f"[yellow]{rd.rows_changed:,} rows changed[/yellow]")
    if not sd.has_changes and not rd.rows_added and not rd.rows_removed and not rd.rows_changed:
        parts.append("[green]no changes[/green]")
    if result.drift_alerts:
        parts.append(f"[red]⚠️  {len(result.drift_alerts)} drift alert{'s' if len(result.drift_alerts)>1 else ''}[/red]")
    elapsed = f"  [dim]({result.elapsed_seconds:.1f}s)[/dim]" if result.elapsed_seconds else ""
    return "  " + " · ".join(parts) + elapsed


def _render_error(e):
    for line in str(e).split("\n"):
        s = line.strip()
        if not s:
            continue
        if any(s.startswith(w) for w in ("Check ", "Set ", "Verify ", "For ", "Original ")):
            err_console.print(f"  [dim]{s}[/dim]")
        else:
            err_console.print(f"[bold red]Error:[/bold red] {s}")


@click.group()
@click.version_option(package_name="difflake", prog_name="difflake")
def main():
    """🔍 difflake — git diff, but for your data lake."""
    pass


def _compare_command(source, target, key, mode, output, out, threshold,
                     source_format, target_format, where, sample, limit,
                     ignore_columns, verbose, config):
    cfg = _load_config(config)
    if key is None and "key" in cfg:
        key = cfg["key"]
    if mode == "full" and "mode" in cfg:
        mode = cfg["mode"]
    if output == "cli" and "output" in cfg:
        output = cfg["output"]
    if out is None and "out" in cfg:
        out = cfg["out"]
    if threshold == 0.15 and "threshold" in cfg:
        threshold = float(cfg["threshold"])
    if not verbose and cfg.get("verbose"):
        verbose = True
    if where is None and "where" in cfg:
        where = cfg["where"]
    if sample is None and "sample" in cfg:
        sample = int(cfg["sample"])
    if limit is None and "limit" in cfg:
        limit = int(cfg["limit"])
    if not ignore_columns and "ignore_columns" in cfg:
        ignore_columns = cfg["ignore_columns"]

    output = output.lower()

    if output in ("json", "html", "markdown") and not out:
        out = _auto_out(source, target, output)
        console.print(f"[dim]Output file: {out}[/dim]")

    ignore_list = [c.strip() for c in ignore_columns.split(",")] if ignore_columns else None

    _preflight(source, target)

    if sample:
        console.print(f"[dim]ℹ️  Sampling {sample:,} rows for stats/row diff. Schema diff uses full data.[/dim]")
    if limit:
        console.print(f"[dim]ℹ️  Limiting to first {limit:,} rows (file order).[/dim]")
    if where:
        console.print(f"[dim]🔍 Filter: WHERE {where}[/dim]")
    if ignore_list:
        console.print(f"[dim]⏭  Ignoring columns: {', '.join(ignore_list)}[/dim]")

    try:
        from rich.status import Status
        msg = f"Diffing [cyan]{Path(source).name}[/cyan] vs [cyan]{Path(target).name}[/cyan]..."
        with Status(msg, console=console, spinner="dots"):
            result = LakeDiff(
                source=source,
                target=target,
                primary_key=key,
                source_format=source_format,
                target_format=target_format,
                mode=mode.lower(),
                drift_threshold=threshold,
                sample_size=sample,
                limit=limit,
                where=where,
                ignore_columns=ignore_list,
            ).run()

    except FileNotFoundError as e:
        err_console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)
    except RuntimeError as e:
        _render_error(e)
        sys.exit(1)
    except Exception as e:
        err_console.print(f"[bold red]Unexpected error:[/bold red] {e}")
        if verbose:
            import traceback
            traceback.print_exc()
        sys.exit(1)

    console.print(_summary_line(result))
    console.print()

    if output == "cli":
        from difflake.reporters.cli_reporter import CliReporter
        CliReporter(result, verbose=verbose).render()
        if result.drift_alerts:
            sys.exit(2)
    elif output == "json":
        from difflake.reporters.json_reporter import JsonReporter
        JsonReporter(result).render(path=out)
        console.print(f"[green]✅ JSON report -> {out}[/green]")
        if result.drift_alerts:
            sys.exit(2)
    elif output == "html":
        result.to_html(out)
        console.print(f"[green]✅ HTML report -> {out}[/green]")
        if result.drift_alerts:
            sys.exit(2)
    elif output == "markdown":
        result.to_markdown(out)
        console.print(f"[green]✅ Markdown report -> {out}[/green]")
        if result.drift_alerts:
            sys.exit(2)


_COMPARE_OPTIONS = [
    click.argument("source", type=click.Path()),
    click.argument("target", type=click.Path()),
    click.option("--key", "-k", default=None,
        help="Primary key. Single (--key id) or composite (--key a,b,c)."),
    click.option("--mode", "-m",
        type=click.Choice(["full","schema","stats","rows"], case_sensitive=False),
        default="full", show_default=True),
    click.option("--output", "-o",
        type=click.Choice(["cli","json","html","markdown"], case_sensitive=False),
        default="cli", show_default=True),
    click.option("--out", "-O", default=None, type=click.Path(),
        help="Output file. Auto-named if omitted for json/html/markdown."),
    click.option("--threshold", "-t", default=0.15, show_default=True, type=float,
        help="Drift alert threshold (0-1). Default 0.15 = 15%."),
    click.option("--source-format", default=None,
        type=click.Choice(_ALL_FORMATS, case_sensitive=False)),
    click.option("--target-format", default=None,
        type=click.Choice(_ALL_FORMATS, case_sensitive=False)),
    click.option("--where", "-w", default=None,
        help='SQL WHERE filter. e.g. --where "status = \'active\'"'),
    click.option("--sample", default=None, type=int,
        help="Random sample N rows for stats/row diff."),
    click.option("--limit", default=None, type=int,
        help="Use first N rows in file order (deterministic, unlike --sample)."),
    click.option("--ignore-columns", default=None,
        help="Comma-separated columns to exclude. e.g. --ignore-columns updated_at,_etl_ts"),
    click.option("--verbose", "-v", is_flag=True,
        help="Show sample added/changed/removed rows."),
    click.option("--config", "-c", default=None, type=click.Path(),
        help="Path to difflake.yaml config file."),
]


def _add_options(options):
    def decorator(f):
        for opt in reversed(options):
            f = opt(f)
        return f
    return decorator


@main.command("compare")
@_add_options(_COMPARE_OPTIONS)
def compare(source, target, key, mode, output, out, threshold,
            source_format, target_format, where, sample, limit,
            ignore_columns, verbose, config):
    """
    Compare two datasets — schema, stats, and row-level diff.

    \b
    Examples:
      difflake compare old.parquet new.parquet
      difflake compare old.csv new.csv --key user_id
      difflake compare old.parquet new.parquet --ignore-columns updated_at,_etl_ts
      difflake compare old.parquet new.parquet --where "status = 'active'"
      difflake compare old.parquet new.parquet --sample 500000 --key trip_id
      difflake compare old.parquet new.parquet --limit 1000000 --key id
      difflake compare old.parquet new.parquet --output html
      difflake compare s3://bucket/v1/ s3://bucket/v2/ --mode stats
    """
    _compare_command(source, target, key, mode, output, out, threshold,
                     source_format, target_format, where, sample, limit,
                     ignore_columns, verbose, config)


@main.command("diff")
@_add_options(_COMPARE_OPTIONS)
def diff(source, target, key, mode, output, out, threshold,
         source_format, target_format, where, sample, limit,
         ignore_columns, verbose, config):
    """
    Alias for compare. Same flags, same output.

    \b
    Examples:
      difflake diff old.parquet new.parquet
      difflake diff old.parquet new.parquet --key user_id --output html
    """
    _compare_command(source, target, key, mode, output, out, threshold,
                     source_format, target_format, where, sample, limit,
                     ignore_columns, verbose, config)


@main.command("show")
@click.argument("path")
@click.option("--rows", "-r", default=None, type=int,
    help="Rows to display (default 5 in overview, 10 in row-only mode).")
@click.option("--schema", "show_schema", is_flag=True,
    help="Column names and types only. Never loads row data.")
@click.option("--count", "show_count", is_flag=True,
    help="Row count, column count, file size, format.")
@click.option("--tail", is_flag=True, help="Show last N rows instead of first N.")
@click.option("--stats", "show_stats", is_flag=True,
    help="Per-column stats: nulls, unique, min, max, mean. Includes datetime min/max.")
@click.option("--columns", "-c", default=None,
    help="Comma-separated columns to show.")
@click.option("--where", "-w", default=None,
    help='SQL WHERE filter. e.g. --where "fare_amount > 500"')
@click.option("--order-by", "order_by", default=None,
    help='Sort by column. e.g. --order-by fare_amount DESC')
@click.option("--freq", default=None,
    help="Top-10 value frequencies for a column. e.g. --freq payment_type")
@click.option("--format", "fmt", default=None,
    type=click.Choice(_ALL_FORMATS, case_sensitive=False),
    help="Explicit format override (auto-detected by default).")
def show(path, rows, show_schema, show_count, tail, show_stats,
         columns, where, order_by, freq, fmt):
    """
    Preview any dataset — schema, rows, stats, or value frequencies.

    \b
    Default (no flags): schema + row count + first 5 rows in one view.

    Examples:
      difflake show data.parquet
      difflake show data.parquet --schema
      difflake show data.parquet --count
      difflake show data.parquet --stats
      difflake show data.parquet --rows 20
      difflake show data.parquet --tail
      difflake show data.parquet --order-by fare_amount DESC
      difflake show data.parquet --where "fare_amount > 500"
      difflake show data.parquet --where "passenger_count is null" --count
      difflake show data.parquet --freq payment_type
      difflake show data.parquet --columns VendorID,fare_amount,trip_distance
      difflake show s3://bucket/data.parquet --schema
    """
    from rich.status import Status

    from difflake.connection import DuckDBConnection, _detect_format, _read_sql

    explicit_mode = any([show_schema, show_count, show_stats,
                         freq is not None, tail, order_by is not None,
                         rows is not None])

    try:
        con = DuckDBConnection()
        detected_fmt = fmt or _detect_format(path)
        col_list = [c.strip() for c in columns.split(",")] if columns else None

        full_sql = _read_sql(path, detected_fmt)
        con.register_view("__show_full", full_sql, detected_fmt, path)

        if show_schema:
            schema_cols = con.columns("__show_full")
            if col_list:
                schema_cols = [(n, t) for n, t in schema_cols if n in col_list]
            console.print()
            console.print(Panel(Text(f"📋 Schema — {Path(path).name}", style="bold cyan"),
                                border_style="cyan"))
            tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold")
            tbl.add_column("#",      style="dim",  width=4)
            tbl.add_column("Column", style="bold", min_width=20)
            tbl.add_column("Type",   style="cyan", min_width=16)
            for i, (col_name, dtype) in enumerate(schema_cols, 1):
                tbl.add_row(str(i), col_name, dtype)
            console.print(tbl)
            console.print(f"  [dim]{len(schema_cols)} columns[/dim]\n")
            con.close()
            return

        work_sql = _read_sql(path, detected_fmt, where=where, columns=col_list)
        with Status(f"Reading [cyan]{Path(path).name}[/cyan]...",
                    console=console, spinner="dots"):
            con.register_view("__show", work_sql, detected_fmt, path)
            total_rows = int(con.scalar("SELECT COUNT(*) FROM __show") or 0)
        schema_info = con.columns("__show")
        total_cols  = len(schema_info)
        where_tag   = f"  · filter: {where}" if where else ""

        if show_count:
            console.print()
            console.print(Panel(Text(f"🔢 {Path(path).name}{where_tag}", style="bold cyan"),
                                border_style="cyan"))
            console.print(f"  [bold]Rows   :[/bold] [cyan]{total_rows:,}[/cyan]")
            console.print(f"  [bold]Columns:[/bold] [cyan]{total_cols:,}[/cyan]")
            local = Path(path)
            if local.is_file():
                size = local.stat().st_size
                size_str = (f"{size/1e6:.1f} MB" if size > 1e6
                            else f"{size/1e3:.1f} KB" if size > 1e3
                            else f"{size} bytes")
                console.print(f"  [bold]Size   :[/bold] [cyan]{size_str}[/cyan]")
            console.print(f"  [bold]Format :[/bold] [cyan]{detected_fmt.upper()}[/cyan]\n")
            con.close()
            return

        if freq:
            _show_freq(con, freq, total_rows, path, where_tag)
            con.close()
            return

        if show_stats:
            _show_stats(con, schema_info, total_rows, total_cols,
                        path, detected_fmt, where_tag)
            con.close()
            return

        if not explicit_mode:
            _show_overview(con, path, detected_fmt, schema_info,
                           total_rows, total_cols, where_tag,
                           n_rows=rows or 5, order_by=order_by, tail=tail)
        else:
            _show_rows(con, schema_info, total_rows, total_cols,
                       path, detected_fmt, where_tag,
                       n_rows=rows or 10, tail=tail, order_by=order_by)

        con.close()

    except FileNotFoundError as e:
        err_console.print(f"[bold]Error:[/bold] {e}")
        sys.exit(1)
    except Exception as e:
        err_console.print(f"[bold]Error:[/bold] {e}")
        sys.exit(1)


def _fmt_val(v, max_len=22):
    if v is None:
        return "[dim]null[/dim]"
    s = str(v)
    return s[:max_len] + "..." if len(s) > max_len else s


def _build_order(order_by, tail):
    if order_by:
        parts = order_by.strip().split()
        col  = f'"{parts[0]}"'
        dir_ = parts[1].upper() if len(parts) > 1 and parts[1].upper() in ("ASC","DESC") else "ASC"
        return f"ORDER BY {col} {dir_}"
    if tail:
        return "ORDER BY __rn DESC"
    return "ORDER BY __rn ASC"


def _render_rows(out_console, col_names, sample_rows, total_cols, offset=0):
    if total_cols <= 10:
        tbl = Table(box=box.SIMPLE_HEAVY, show_header=True,
                    header_style="bold", padding=(0, 1))
        tbl.add_column("#", style="dim", width=4)
        for col_name in col_names:
            tbl.add_column(col_name, min_width=10, max_width=30, overflow="fold")
        for i, row in enumerate(sample_rows):
            tbl.add_row(str(offset + i + 1), *[_fmt_val(v) for v in row])
        out_console.print(tbl)

    elif total_cols <= 20:
        wide = Console(soft_wrap=True)
        tbl  = Table(box=box.SIMPLE_HEAVY, show_header=True,
                     header_style="bold", padding=(0, 0))
        tbl.add_column("#", style="dim", width=4)
        for col_name in col_names:
            label = col_name[:13] + "~" if len(col_name) > 13 else col_name
            tbl.add_column(label, min_width=6, max_width=13, overflow="fold")
        for i, row in enumerate(sample_rows):
            tbl.add_row(str(offset + i + 1), *[_fmt_val(v, 12) for v in row])
        wide.print(tbl)
        wide.print("  [dim]Tip: use --columns to select specific columns[/dim]")

    else:
        for i, row in enumerate(sample_rows):
            lines = []
            for col_name, val in zip(col_names, row):
                label = f"[dim]{col_name:<28}[/dim]"
                value = "[dim]null[/dim]" if val is None else f"[white]{str(val)[:60]}[/white]"
                lines.append(f"  {label} {value}")
            out_console.print(Panel(
                "\n".join(lines),
                title=f"[bold cyan]Row {offset + i + 1}[/bold cyan]",
                border_style="dim", padding=(0, 1),
            ))


def _show_overview(con, path, detected_fmt, schema_info, total_rows, total_cols,
                   where_tag, n_rows=5, order_by=None, tail=False):
    console.print()
    console.print(Panel(
        Text(f"🔍 {Path(path).name}  · {total_rows:,} rows  · "
             f"{total_cols} columns  · {detected_fmt.upper()}{where_tag}",
             style="bold cyan"),
        border_style="cyan",
    ))
    console.print("  [bold dim]SCHEMA[/bold dim]")
    stbl = Table(box=box.SIMPLE, show_header=False, padding=(0, 1), show_edge=False)
    stbl.add_column("num",  style="dim",  width=4)
    stbl.add_column("col",  style="bold", min_width=20)
    stbl.add_column("type", style="cyan", min_width=14)
    for i, (col_name, dtype) in enumerate(schema_info, 1):
        stbl.add_row(str(i), col_name, dtype)
    console.print(stbl)
    console.print(f"  [bold dim]FIRST {n_rows} ROWS[/bold dim]")
    col_names_sql = ", ".join(f'"{c}"' for c, _ in schema_info)
    order_clause  = _build_order(order_by, tail)
    _, sample_rows = con.fetchdf(f"""
        SELECT {col_names_sql}
        FROM (SELECT {col_names_sql}, ROW_NUMBER() OVER () AS __rn FROM __show) t
        {order_clause}
        LIMIT {n_rows}
    """)
    col_names = [c for c, _ in schema_info]
    _render_rows(console, col_names, sample_rows, total_cols, offset=0)
    if total_rows > n_rows:
        console.print(
            f"  [dim]... {total_rows - n_rows:,} more rows. "
            f"Use --rows N or --where to filter.[/dim]\n"
        )


def _show_rows(con, schema_info, total_rows, total_cols, path, detected_fmt,
               where_tag, n_rows=10, tail=False, order_by=None):
    col_names_sql = ", ".join(f'"{c}"' for c, _ in schema_info)
    order_clause  = _build_order(order_by, tail)
    if order_by:
        position = f"sorted by {order_by}"
    elif tail:
        position = "last"
    else:
        position = "first"
    _, sample_rows = con.fetchdf(f"""
        SELECT {col_names_sql}
        FROM (SELECT {col_names_sql}, ROW_NUMBER() OVER () AS __rn FROM __show) t
        {order_clause}
        LIMIT {n_rows}
    """)
    if tail and not order_by:
        sample_rows = list(reversed(sample_rows))
    col_names = [c for c, _ in schema_info]
    offset = (total_rows - n_rows) if (tail and not order_by) else 0
    console.print()
    console.print(Panel(
        Text(f"🔍 {Path(path).name}  - {position} {len(sample_rows):,} of "
             f"{total_rows:,} rows  · {total_cols} cols  · {detected_fmt.upper()}{where_tag}",
             style="bold cyan"),
        border_style="cyan",
    ))
    _render_rows(console, col_names, sample_rows, total_cols, offset=offset)
    if total_rows > n_rows:
        console.print(f"  [dim]... {total_rows - n_rows:,} more rows. Use --rows {n_rows*10} to see more.[/dim]\n")


def _show_stats(con, schema_info, total_rows, total_cols, path, detected_fmt, where_tag):
    console.print()
    console.print(Panel(
        Text(f"📊 Stats — {Path(path).name}  ({total_rows:,} rows){where_tag}",
             style="bold cyan"),
        border_style="cyan",
    ))
    tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold", padding=(0, 1))
    tbl.add_column("Column",  style="bold", min_width=18)
    tbl.add_column("Type",    style="dim",  min_width=12)
    tbl.add_column("Nulls",   min_width=8)
    tbl.add_column("Null %",  min_width=7)
    tbl.add_column("Unique",  min_width=8)
    tbl.add_column("Min",     min_width=14)
    tbl.add_column("Max",     min_width=14)
    tbl.add_column("Mean",    min_width=12)

    _NUM  = {"TINYINT","SMALLINT","INTEGER","BIGINT","HUGEINT","FLOAT","DOUBLE",
             "DECIMAL","NUMERIC","REAL","UTINYINT","USMALLINT","UINTEGER","UBIGINT"}
    _DATE = {"DATE","TIMESTAMP","TIMESTAMP WITH TIME ZONE","TIME"}

    for col_name, dtype in schema_info:
        bt = dtype.upper().split("(")[0]
        null_count   = int(con.scalar(f'SELECT COUNT(*) FROM __show WHERE "{col_name}" IS NULL') or 0)
        null_pct     = null_count / max(total_rows, 1) * 100
        unique_count = int(con.scalar(f'SELECT COUNT(DISTINCT "{col_name}") FROM __show') or 0)
        null_style   = "red" if null_pct > 20 else ("yellow" if null_pct > 5 else "green")

        if bt in _NUM:
            row = con.fetchone(f"""
                SELECT MIN(CAST("{col_name}" AS DOUBLE)),
                       MAX(CAST("{col_name}" AS DOUBLE)),
                       AVG(CAST("{col_name}" AS DOUBLE))
                FROM __show WHERE "{col_name}" IS NOT NULL
            """)
            min_v = f"{row[0]:,.4g}" if row and row[0] is not None else "—"
            max_v = f"{row[1]:,.4g}" if row and row[1] is not None else "—"
            avg_v = f"{row[2]:,.4g}" if row and row[2] is not None else "—"
        elif bt in _DATE:
            row = con.fetchone(f"""
                SELECT MIN("{col_name}"), MAX("{col_name}")
                FROM __show WHERE "{col_name}" IS NOT NULL
            """)
            min_v = str(row[0])[:19] if row and row[0] is not None else "—"
            max_v = str(row[1])[:19] if row and row[1] is not None else "—"
            avg_v = "—"
        else:
            min_v = max_v = avg_v = "—"

        tbl.add_row(
            col_name, dtype,
            f"{null_count:,}",
            f"[{null_style}]{null_pct:.1f}%[/{null_style}]",
            f"{unique_count:,}",
            min_v, max_v, avg_v,
        )
    console.print(tbl)
    console.print(f"  [dim]{total_cols} columns · {total_rows:,} rows · {detected_fmt.upper()}[/dim]\n")


def _show_freq(con, col, total_rows, path, where_tag):
    console.print()
    console.print(Panel(
        Text(f"📊 Frequencies — {col}  ({Path(path).name}){where_tag}", style="bold cyan"),
        border_style="cyan",
    ))
    _, rows = con.fetchdf(f"""
        SELECT
            CAST("{col}" AS VARCHAR) AS value,
            COUNT(*) AS count,
            ROUND(COUNT(*) * 100.0 / {max(total_rows, 1)}, 2) AS pct
        FROM __show
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 10
    """)
    tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold")
    tbl.add_column("Value", style="bold", min_width=20)
    tbl.add_column("Count", min_width=10)
    tbl.add_column("%",     min_width=8)
    tbl.add_column("Bar",   min_width=20)
    max_count = rows[0][1] if rows else 1
    for value, count, pct in rows:
        bar_len = int((count / max_count) * 20)
        bar = "[cyan]" + "#" * bar_len + "[/cyan]" + "-" * (20 - bar_len)
        display_val = str(value)[:40] if value is not None else "[dim]null[/dim]"
        tbl.add_row(display_val, f"{int(count):,}", f"{pct:.1f}%", bar)
    console.print(tbl)
    console.print(f"  [dim]Top 10 of {total_rows:,} rows[/dim]\n")


def _run_validate(path, fmt, where, checks):
    """
    Execute validation checks against a dataset.
    Returns list of (check_name, passed, detail) tuples.
    """
    from difflake.connection import DuckDBConnection, _detect_format, _read_sql

    con = DuckDBConnection()
    detected_fmt = fmt or _detect_format(path)
    sql = _read_sql(path, detected_fmt, where=where)
    con.register_view("__val", sql, detected_fmt, path)

    results = []
    for check in checks:
        kind = check["kind"]
        try:
            if kind == "min_rows":
                n = int(con.scalar("SELECT COUNT(*) FROM __val") or 0)
                passed = n >= check["value"]
                results.append((f"row count >= {check['value']:,}", passed,
                                f"{n:,} rows"))
            elif kind == "max_rows":
                n = int(con.scalar("SELECT COUNT(*) FROM __val") or 0)
                passed = n <= check["value"]
                results.append((f"row count <= {check['value']:,}", passed,
                                f"{n:,} rows"))
            elif kind == "not_null":
                col = check["column"]
                n = int(con.scalar(
                    f'SELECT COUNT(*) FROM __val WHERE "{col}" IS NULL') or 0)
                passed = n == 0
                results.append((f'"{col}" not null', passed,
                                f"{n:,} nulls found" if n else "no nulls"))
            elif kind == "unique":
                col = check["column"]
                total = int(con.scalar(
                    f'SELECT COUNT(*) FROM __val WHERE "{col}" IS NOT NULL') or 0)
                distinct = int(con.scalar(
                    f'SELECT COUNT(DISTINCT "{col}") FROM __val') or 0)
                passed = total == distinct
                dups = total - distinct
                results.append((f'"{col}" unique', passed,
                                f"{dups:,} duplicates" if dups else "all unique"))
            elif kind == "min_val":
                col = check["column"]
                val = check["value"]
                row = con.fetchone(
                    f'SELECT MIN(CAST("{col}" AS DOUBLE)) FROM __val '
                    f'WHERE "{col}" IS NOT NULL')
                actual = float(row[0]) if row and row[0] is not None else None
                passed = actual is not None and actual >= val
                results.append((f'"{col}" min >= {val}', passed,
                                f"actual min = {actual}" if actual is not None else "no data"))
            elif kind == "max_val":
                col = check["column"]
                val = check["value"]
                row = con.fetchone(
                    f'SELECT MAX(CAST("{col}" AS DOUBLE)) FROM __val '
                    f'WHERE "{col}" IS NOT NULL')
                actual = float(row[0]) if row and row[0] is not None else None
                passed = actual is not None and actual <= val
                results.append((f'"{col}" max <= {val}', passed,
                                f"actual max = {actual}" if actual is not None else "no data"))
            elif kind == "column_exists":
                col = check["column"]
                schema = dict(con.columns("__val"))
                passed = col in schema
                results.append((f'column "{col}" exists', passed,
                                "found" if passed else "missing"))
            elif kind == "where_count":
                expr = check["expr"]
                expected = check["value"]
                n = int(con.scalar(
                    f'SELECT COUNT(*) FROM __val WHERE {expr}') or 0)
                passed = n == expected
                results.append((f"WHERE {expr} count == {expected:,}", passed,
                                f"{n:,} rows match"))
        except Exception as exc:
            results.append((f"{kind} check", False, f"error: {exc}"))

    con.close()
    return results


def _parse_val_checks(min_rows, max_rows, not_null, unique,
                      min_val, max_val, column_exists, where_count,
                      config_checks):
    """Merge CLI flags + YAML config into a unified check list."""
    checks = list(config_checks)  # YAML checks first
    if min_rows is not None:
        checks.append({"kind": "min_rows",  "value": min_rows})
    if max_rows is not None:
        checks.append({"kind": "max_rows",  "value": max_rows})
    for col in not_null:
        checks.append({"kind": "not_null",  "column": col})
    for col in unique:
        checks.append({"kind": "unique",    "column": col})
    for spec in min_val:
        col, _, val = spec.rpartition(":")
        if col:
            checks.append({"kind": "min_val", "column": col, "value": float(val)})
    for spec in max_val:
        col, _, val = spec.rpartition(":")
        if col:
            checks.append({"kind": "max_val", "column": col, "value": float(val)})
    for col in column_exists:
        checks.append({"kind": "column_exists", "column": col})
    for spec in where_count:
        # format: "expr==N"  e.g.  "status='active'==100"
        if "==" in spec:
            expr, _, cnt = spec.rpartition("==")
            checks.append({"kind": "where_count", "expr": expr, "value": int(cnt)})
    return checks


@main.command("validate")
@click.argument("path", type=click.Path())
@click.option("--min-rows", default=None, type=int,
    help="Assert dataset has at least N rows.")
@click.option("--max-rows", default=None, type=int,
    help="Assert dataset has at most N rows.")
@click.option("--not-null", multiple=True, metavar="COLUMN",
    help="Assert column has no nulls. Repeatable.")
@click.option("--unique", multiple=True, metavar="COLUMN",
    help="Assert column has no duplicates. Repeatable.")
@click.option("--min-val", multiple=True, metavar="COL:VALUE",
    help="Assert numeric column min >= VALUE. e.g. --min-val fare_amount:0")
@click.option("--max-val", multiple=True, metavar="COL:VALUE",
    help="Assert numeric column max <= VALUE. e.g. --max-val age:120")
@click.option("--column-exists", multiple=True, metavar="COLUMN",
    help="Assert column is present in schema. Repeatable.")
@click.option("--where-count", multiple=True, metavar="EXPR==N",
    help="Assert COUNT(*) WHERE expr equals N. e.g. --where-count \"status='X'==0\"")
@click.option("--where", "-w", default=None,
    help="Pre-filter dataset before running checks.")
@click.option("--format", "fmt", default=None,
    type=click.Choice(_ALL_FORMATS, case_sensitive=False),
    help="Explicit format override.")
@click.option("--config", "-c", default=None, type=click.Path(),
    help="Path to difflake.yaml. Loads validate: section.")
@click.option("--fail-fast", is_flag=True,
    help="Stop after first failure.")
def validate(path, min_rows, max_rows, not_null, unique,
             min_val, max_val, column_exists, where_count,
             where, fmt, config, fail_fast):
    """
    Validate a dataset against assertions. Exits 1 if any check fails.

    \b
    Examples:
      difflake validate data.parquet --min-rows 1000
      difflake validate data.parquet --not-null user_id --not-null email
      difflake validate data.parquet --unique order_id
      difflake validate data.parquet --min-val fare_amount:0 --max-val age:120
      difflake validate data.parquet --column-exists id --column-exists name
      difflake validate data.parquet --config difflake.yaml
      difflake validate s3://bucket/data.parquet --min-rows 500
    """
    cfg = _load_config(config)
    config_checks = cfg.get("validate", {}).get("checks", [])

    checks = _parse_val_checks(min_rows, max_rows, not_null, unique,
                               min_val, max_val, column_exists, where_count,
                               config_checks)

    if not checks:
        console.print("[yellow]No checks specified. Use --min-rows, --not-null, etc.[/yellow]")
        return

    console.print()
    console.print(Panel(
        Text(f"🔍 Validating — {Path(path).name}", style="bold cyan"),
        border_style="cyan",
    ))
    if where:
        console.print(f"  [dim]Filter: WHERE {where}[/dim]")
    console.print(f"  [dim]Running {len(checks)} check{'s' if len(checks) != 1 else ''}…[/dim]\n")

    try:
        results = _run_validate(path, fmt, where, checks)
    except FileNotFoundError as e:
        err_console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)
    except Exception as e:
        err_console.print(f"[bold red]Error:[/bold red] {e}")
        sys.exit(1)

    tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold")
    tbl.add_column("Status", width=6)
    tbl.add_column("Check",  style="bold", min_width=30)
    tbl.add_column("Detail", style="dim",  min_width=20)

    failed = 0
    for name, passed, detail in results:
        icon   = "[green]✅[/green]" if passed else "[red]❌[/red]"
        detail_style = "dim" if passed else "red"
        tbl.add_row(icon, name, f"[{detail_style}]{detail}[/{detail_style}]")
        if not passed:
            failed += 1
            if fail_fast:
                break

    console.print(tbl)

    total = len(results)
    if failed == 0:
        console.print(f"  [green]✅ All {total} check{'s' if total != 1 else ''} passed[/green]\n")
    else:
        console.print(
            f"  [red]❌ {failed} of {total} check{'s' if total != 1 else ''} failed[/red]\n"
        )
        sys.exit(1)


@main.command("formats")
def formats():
    """List all supported input formats, extensions, and cloud URIs."""
    tbl = Table(box=box.SIMPLE_HEAVY, show_header=True, header_style="bold cyan",
                title="Supported Formats", title_style="bold")
    tbl.add_column("Format",        style="bold", min_width=16)
    tbl.add_column("Extension",     style="cyan", min_width=18)
    tbl.add_column("Status",        min_width=10)
    tbl.add_column("Extra install", style="dim",  min_width=24)
    tbl.add_column("Cloud / Remote",              min_width=20)
    rows = [
        ("CSV / TSV",    ".csv .tsv",       "Built-in", "—",                         "s3:// gs:// az://"),
        ("Parquet",      ".parquet .pq",    "Built-in", "—",                         "s3:// gs:// az:// https://"),
        ("JSON",         ".json",           "Built-in", "—",                         "s3:// gs:// az://"),
        ("JSONL/NDJSON", ".jsonl .ndjson",  "Built-in", "—",                         "s3:// gs:// az://"),
        ("Delta Lake",   "directory",       "Built-in", "pip install duckdb[delta]", "s3:// gs:// az://"),
        ("Avro",         ".avro",           "Built-in", "INSTALL avro (auto)",       "s3:// gs:// az://"),
        ("Iceberg",      "directory/URI",   "Built-in", "INSTALL iceberg (auto)",    "s3:// gs:// az://"),
        ("Parquet/HTTP", "https://...",     "Built-in", "INSTALL httpfs (auto)",     "Any HTTPS URL"),
        ("PostgreSQL",   "postgres://...",  "Built-in", "INSTALL postgres (auto)",   "Any PG connection"),
        ("MySQL",        "mysql://...",     "Built-in", "INSTALL mysql (auto)",      "Any MySQL connection"),
    ]
    for fmt, ext, status, install, cloud in rows:
        tbl.add_row(fmt, ext, f"[green]{status}[/green]", install, cloud)
    console.print()
    console.print(tbl)
    console.print(
        "\n  [dim]Extensions install automatically on first use.\n"
        "  Cloud auth: AWS_ACCESS_KEY_ID/SECRET, GOOGLE_APPLICATION_CREDENTIALS, "
        "AZURE_STORAGE_ACCOUNT/KEY[/dim]\n"
    )

"""
Async logic for the ``tsbm compare`` command.

Loads summary rows from SQLite for the requested run IDs and renders them
in the requested format (table / csv / json / markdown).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.table import Table

from tsbm.results.export import compare_runs, export_csv, export_json, export_markdown

console = Console()


async def async_compare(
    run_ids: list[str],
    operation: str | None,
    metric: str,
    fmt: str,
    output: Path | None,
    config_path: Path | None,
) -> None:
    """Load summaries for *run_ids* and render the comparison."""
    from tsbm.config.settings import get_settings, load_settings_from_file
    from tsbm.results.storage import ResultStorage

    settings = load_settings_from_file(config_path) if config_path else get_settings()
    storage = ResultStorage(
        sqlite_path=settings.results.sqlite_path,
        parquet_dir=settings.results.parquet_dir,
    )
    storage.initialize()

    rows = storage.load_summaries(
        run_ids=run_ids,
        operation=operation,
    )

    if not rows:
        console.print("[yellow]No results found for the given run IDs.[/yellow]")
        console.print("[dim]Available runs:[/dim]")
        for r in storage.list_runs():
            console.print(f"  {r['run_id']}  {r['benchmark_name']}  {r['database_name']}  {r['started_at']}")
        return

    fmt = fmt.lower().strip()

    if fmt == "table":
        _print_rich_table(rows, metric)
        if len(run_ids) == 2:
            rows_a = [r for r in rows if r["run_id"] == run_ids[0]]
            rows_b = [r for r in rows if r["run_id"] == run_ids[1]]
            text = compare_runs(rows_a, rows_b, run_ids[0], run_ids[1], metric)
            console.print("\n[bold]Delta comparison:[/bold]")
            console.print(text)
        return

    if fmt == "csv":
        text = export_csv(rows, output)
    elif fmt == "json":
        text = export_json(rows, output)
    elif fmt == "markdown":
        text = export_markdown(rows, metric=metric, output=output)
    else:
        console.print(f"[red]Unknown format {fmt!r}. Choose: table | csv | json | markdown[/red]")
        raise SystemExit(1)

    if output:
        console.print(f"[green]Written:[/green] {output}")
    else:
        sys.stdout.write(text)


# ---------------------------------------------------------------------------
# Rich table renderer
# ---------------------------------------------------------------------------


def _print_rich_table(rows: list[dict[str, Any]], metric: str) -> None:
    """Render the summary rows as a Rich table grouped by operation."""
    # Discover unique databases (columns)
    databases = sorted({r["database_name"] for r in rows})
    ops = sorted({(r["operation"], r.get("batch_size", 0), r.get("workers", 1)) for r in rows})

    # Build lookup
    lookup: dict[tuple, dict[str, Any]] = {}
    for r in rows:
        key = (r["operation"], r.get("batch_size", 0), r.get("workers", 1), r["database_name"])
        lookup[key] = r

    table = Table(
        title=f"Comparison — [bold]{metric}[/bold]",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Operation", style="white")
    table.add_column("Batch", justify="right")
    table.add_column("Workers", justify="right")
    for db in databases:
        table.add_column(db, justify="right", style="cyan")

    for op, batch, workers in ops:
        cells: list[str] = [op, f"{batch:,}", str(workers)]
        for db in databases:
            val = lookup.get((op, batch, workers, db), {}).get(metric)
            cells.append(_fmt(val))
        table.add_row(*cells)

    console.print(table)

    # Secondary table: run IDs per row
    run_id_table = Table(show_header=True, header_style="dim")
    run_id_table.add_column("Operation", style="dim")
    run_id_table.add_column("Batch", justify="right", style="dim")
    run_id_table.add_column("Workers", justify="right", style="dim")
    for db in databases:
        run_id_table.add_column(f"{db} run_id", style="dim")
    for op, batch, workers in ops:
        cells = [op, f"{batch:,}", str(workers)]
        for db in databases:
            rid = lookup.get((op, batch, workers, db), {}).get("run_id", "—")
            cells.append(str(rid))
        run_id_table.add_row(*cells)
    console.print(run_id_table)


def _fmt(val: Any) -> str:
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)

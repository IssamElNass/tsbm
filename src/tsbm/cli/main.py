"""
tsbm — Time-Series Database Benchmark Suite CLI.

Entry point: ``tsbm`` (defined in pyproject.toml).

Commands
--------
  run        Run a benchmark workload against one or more databases.
  generate   Generate a synthetic IoT dataset and write it to Parquet/CSV.
  load       Ingest a dataset into a database without benchmarking.
  seed       Seed QuestDB with data (no benchmarks, WAL throttle, rows/sec).
  compare    Compare results from two or more runs.
  dashboard  Launch the Streamlit results dashboard.
"""
from __future__ import annotations

import asyncio
import logging
import sys
from pathlib import Path
from typing import List, Optional

import typer
from rich.console import Console
from typing_extensions import Annotated

from tsbm.cli.run import async_run, async_generate, async_load, async_seed
from tsbm.cli.compare import async_compare
from tsbm.cli.dashboard import launch_dashboard
from tsbm.cli.report import async_report

app = typer.Typer(
    name="tsbm",
    help="Time-Series Database Benchmark Suite",
    add_completion=False,
    rich_markup_mode="rich",
)
console = Console()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


@app.command("run")
def cmd_run(
    db: Annotated[
        Optional[List[str]],
        typer.Option("--db", "-d", help="Database(s) to target. Repeat for multiple. Default: all enabled."),
    ] = None,
    benchmark: Annotated[
        str,
        typer.Option(
            "--benchmark", "-b",
            help=(
                "Workload to run. 'default' runs: ingestion, time_range, aggregation, "
                "last_point, mixed, downsampling. 'all' runs every registered workload. "
                "Or specify a single benchmark name."
            ),
        ),
    ] = "default",
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to benchmark.toml. Defaults to ./benchmark.toml."),
    ] = None,
    dataset: Annotated[
        Optional[List[str]],
        typer.Option("--dataset", help="Override dataset path(s). Repeat for multiple files. Supports globs and az:// URLs."),
    ] = None,
    timestamp_col: Annotated[
        Optional[str],
        typer.Option(
            "--timestamp-col",
            help=(
                "Name of the timestamp column in the dataset. "
                "Overrides auto-detection. Use this when your column is not named "
                "one of: time, ts, timestamp, datetime, date, created_at, event_time."
            ),
        ),
    ] = None,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable debug logging."),
    ] = False,
    skip_seed: Annotated[
        bool,
        typer.Option(
            "--skip-seed/--seed",
            help="Skip data seeding; assume tables are already populated from a previous run.",
        ),
    ] = False,
) -> None:
    """Run a benchmark workload against one or more databases."""
    _setup_logging(verbose)
    # Single dataset path for backward compat; multiple datasets go via config
    dataset_path = Path(dataset[0]) if dataset and len(dataset) == 1 else None
    dataset_list = list(dataset) if dataset and len(dataset) > 1 else None
    asyncio.run(async_run(
        db_names=list(db) if db else None,
        benchmark_name=benchmark,
        config_path=config,
        dataset_path=dataset_path,
        timestamp_col=timestamp_col,
        dataset_list=dataset_list,
        verbose=verbose,
        skip_seed=skip_seed,
    ))


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------


@app.command("generate")
def cmd_generate(
    devices: Annotated[int, typer.Option("--devices", "-n", help="Number of unique devices.")] = 100,
    readings: Annotated[int, typer.Option("--readings", "-r", help="Readings per device.")] = 1000,
    seed: Annotated[int, typer.Option("--seed", help="PRNG seed for reproducibility.")] = 42,
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output file path (.parquet or .csv)."),
    ] = Path("datasets/generated.parquet"),
    parts: Annotated[
        int,
        typer.Option("--parts", "-p", help="Split output into N Parquet part-files for parallel reads. Default: 1 (single file)."),
    ] = 1,
    chunk_rows: Annotated[
        int,
        typer.Option("--chunk-rows", help="Rows per generation chunk. Increase for billion-row datasets (e.g. 500000). Default: 100000."),
    ] = 0,
) -> None:
    """Generate a synthetic IoT dataset and write it to a file."""
    asyncio.run(async_generate(
        n_devices=devices,
        n_readings=readings,
        seed=seed,
        output_path=output,
        n_parts=parts,
        chunk_rows=chunk_rows or None,
    ))


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


@app.command("load")
def cmd_load(
    db: Annotated[
        Optional[List[str]],
        typer.Option("--db", "-d", help="Database(s) to load into. Repeat for multiple."),
    ] = None,
    dataset: Annotated[
        Optional[Path],
        typer.Option("--dataset", help="Dataset file to load (.csv, .parquet, .json)."),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to benchmark.toml."),
    ] = None,
    drop: Annotated[
        bool,
        typer.Option("--drop/--no-drop", help="Drop existing table before loading."),
    ] = True,
) -> None:
    """Ingest a dataset into a database without benchmarking."""
    asyncio.run(async_load(
        db_names=list(db) if db else None,
        dataset_path=dataset,
        config_path=config,
        drop_first=drop,
    ))


# ---------------------------------------------------------------------------
# seed (QuestDB only)
# ---------------------------------------------------------------------------


@app.command("seed")
def cmd_seed(
    db: Annotated[
        Optional[List[str]],
        typer.Option("--db", "-d", help="Database(s) to target. Repeat for multiple. Default: all enabled."),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to benchmark.toml."),
    ] = None,
    dataset: Annotated[
        Optional[List[str]],
        typer.Option("--dataset", help="Override dataset path(s). Repeat for multiple files. Supports globs and az:// URLs."),
    ] = None,
    timestamp_col: Annotated[
        Optional[str],
        typer.Option("--timestamp-col", help="Name of the timestamp column (overrides auto-detection)."),
    ] = None,
    drop: Annotated[
        bool,
        typer.Option("--drop/--no-drop", help="Drop existing table before seeding."),
    ] = True,
    workers: Annotated[
        int,
        typer.Option("--workers", "-w", help="Number of parallel ingest workers. 0 = use config value."),
    ] = 0,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable debug logging."),
    ] = False,
) -> None:
    """Seed one or more databases with the full dataset — no benchmarks.

    Uses parallel workers and WAL-lag throttling. Shows live rows/sec.
    """
    _setup_logging(verbose)
    dataset_path = Path(dataset[0]) if dataset and len(dataset) == 1 else None
    dataset_list = list(dataset) if dataset and len(dataset) > 1 else None
    asyncio.run(async_seed(
        db_names=list(db) if db else None,
        config_path=config,
        dataset_path=dataset_path,
        dataset_list=dataset_list,
        timestamp_col=timestamp_col,
        drop_first=drop,
        seed_workers=workers,
        verbose=verbose,
    ))


# ---------------------------------------------------------------------------
# compare
# ---------------------------------------------------------------------------


@app.command("compare")
def cmd_compare(
    run_ids: Annotated[
        List[str],
        typer.Argument(help="Run IDs to compare (space-separated)."),
    ],
    operation: Annotated[
        Optional[str],
        typer.Option("--operation", "-o", help="Filter to a specific operation."),
    ] = None,
    metric: Annotated[
        str,
        typer.Option("--metric", "-m", help="Metric column to highlight."),
    ] = "latency_p99_ms",
    format: Annotated[
        str,
        typer.Option("--format", "-f", help="Output format: table | csv | json | markdown."),
    ] = "table",
    output: Annotated[
        Optional[Path],
        typer.Option("--output", help="Write results to file instead of stdout."),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to benchmark.toml (for results DB path)."),
    ] = None,
) -> None:
    """Compare results from one or more benchmark runs."""
    asyncio.run(async_compare(
        run_ids=run_ids,
        operation=operation,
        metric=metric,
        fmt=format,
        output=output,
        config_path=config,
    ))


# ---------------------------------------------------------------------------
# report
# ---------------------------------------------------------------------------


@app.command("report")
def cmd_report(
    run_ids: Annotated[
        Optional[List[str]],
        typer.Argument(help="Run IDs to include. Omit to auto-discover latest completed runs."),
    ] = None,
    benchmark: Annotated[
        Optional[str],
        typer.Option("--benchmark", "-b", help="Limit report to a specific benchmark."),
    ] = None,
    output: Annotated[
        Path,
        typer.Option("--output", "-o", help="Output Markdown file path."),
    ] = Path("results/report.md"),
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to benchmark.toml."),
    ] = None,
    latest: Annotated[
        bool,
        typer.Option(
            "--latest/--no-latest",
            help="Auto-select most recent completed run per (benchmark, database). Default: on.",
        ),
    ] = True,
    summary: Annotated[
        bool,
        typer.Option(
            "--summary/--full",
            help="Generate concise summary (default) or full detailed report.",
        ),
    ] = True,
    verbose: Annotated[
        bool,
        typer.Option("--verbose", "-v", help="Enable debug logging."),
    ] = False,
) -> None:
    """Generate a Markdown benchmark report. Use --summary (default) for a concise conclusion or --full for all metrics and SQL queries."""
    _setup_logging(verbose)
    asyncio.run(async_report(
        run_ids=list(run_ids) if run_ids else None,
        benchmark_filter=benchmark,
        output=output,
        config_path=config,
        latest=latest,
        summary=summary,
    ))


# ---------------------------------------------------------------------------
# dashboard
# ---------------------------------------------------------------------------


@app.command("dashboard")
def cmd_dashboard(
    port: Annotated[int, typer.Option("--port", "-p", help="Streamlit server port.")] = 8501,
    results_db: Annotated[
        Optional[Path],
        typer.Option("--results-db", help="Path to results SQLite database."),
    ] = None,
    config: Annotated[
        Optional[Path],
        typer.Option("--config", "-c", help="Path to benchmark.toml."),
    ] = None,
) -> None:
    """Launch the Streamlit results dashboard."""
    launch_dashboard(port=port, results_db=results_db, config_path=config)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _setup_logging(verbose: bool) -> None:
    level = logging.DEBUG if verbose else logging.WARNING
    logging.basicConfig(
        format="%(asctime)s %(levelname)-8s %(name)s — %(message)s",
        level=level,
    )

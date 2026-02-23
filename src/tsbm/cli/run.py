"""
Async orchestration for the run, generate, and load commands.

run orchestration
-----------------
1. Load config (from explicit path or default benchmark.toml)
2. Capture environment snapshot
3. Load / prepare dataset
4. For each enabled adapter:
   a. Connect, get version, create table
   b. Start ResourceMonitor (if enabled)
   c. Run the selected benchmark workload
   d. Stop monitor; attach ResourceSummary to each BenchmarkSummary
   e. Save RunConfig, OperationResults, BenchmarkSummaries to storage
   f. Disconnect
5. Print Rich summary table
"""
from __future__ import annotations

import hashlib
import json
import logging
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.progress import (
    BarColumn,
    MofNCompleteColumn,
    Progress,
    SpinnerColumn,
    TaskID,
    TextColumn,
    TimeElapsedColumn,
)
from rich.table import Table

from tsbm.adapters.registry import get_enabled_adapters
from tsbm.benchmarks.registry import get_workload, list_workloads
from tsbm.config.settings import get_settings, load_settings_from_file
from tsbm.datasets.loader import apply_unit_conversions, load_dataset
from tsbm.environment.capture import capture_environment, enrich_db_versions
from tsbm.exceptions import ConfigError
from tsbm.metrics.monitor import ResourceMonitor
from tsbm.results.models import RunConfig
from tsbm.results.storage import ResultStorage

logger = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


async def async_run(
    db_names: list[str] | None,
    benchmark_name: str,
    config_path: Path | None,
    dataset_path: Path | None,
) -> None:
    """Full orchestration for the ``tsbm run`` command."""

    # ----------------------------------------------------------------
    # 1. Config
    # ----------------------------------------------------------------
    if config_path is not None:
        settings = load_settings_from_file(config_path)
    else:
        settings = get_settings()

    if benchmark_name not in list_workloads():
        raise ConfigError(
            f"Unknown benchmark {benchmark_name!r}. "
            f"Available: {', '.join(list_workloads())}"
        )

    workload = get_workload(benchmark_name)
    effective_dataset = dataset_path or settings.workload.dataset

    # ----------------------------------------------------------------
    # 2. Environment snapshot
    # ----------------------------------------------------------------
    env = capture_environment()
    config_hash = hashlib.md5(
        json.dumps(settings.model_dump(), sort_keys=True, default=str).encode()
    ).hexdigest()[:8]

    # ----------------------------------------------------------------
    # 3. Dataset
    # ----------------------------------------------------------------
    console.print(f"[cyan]Loading dataset:[/cyan] {effective_dataset}")
    dataset = load_dataset(Path(effective_dataset))

    # Apply unit conversions if configured
    if settings.workload.unit_conversions and dataset.table is not None:
        dataset.table = apply_unit_conversions(dataset.table, dict(settings.workload.unit_conversions))
        console.print(f"  Unit conversions applied: {dict(settings.workload.unit_conversions)}")
    elif settings.workload.unit_conversions and dataset.streaming:
        # Streaming mode: attach conversions for per-batch application
        dataset._unit_conversions = dict(settings.workload.unit_conversions)
        console.print(f"  Unit conversions (streaming mode): {dict(settings.workload.unit_conversions)}")

    console.print(
        f"  Schema: [bold]{dataset.schema.name}[/bold] "
        f"({dataset.schema.row_count:,} rows, "
        f"tags={dataset.schema.tag_cols}, "
        f"metrics={dataset.schema.metric_cols})"
    )

    # ----------------------------------------------------------------
    # 4. Storage
    # ----------------------------------------------------------------
    storage = ResultStorage(
        sqlite_path=settings.results.sqlite_path,
        parquet_dir=settings.results.parquet_dir,
    )
    storage.initialize()

    # ----------------------------------------------------------------
    # 5. Adapters
    # ----------------------------------------------------------------
    adapters = get_enabled_adapters(db_names)
    if not adapters:
        console.print("[red]No adapters available. Check --db and your config.[/red]")
        return

    console.print(
        f"\n[bold]Benchmark:[/bold] {benchmark_name}  "
        f"[bold]Databases:[/bold] {', '.join(a.name for a in adapters)}\n"
    )

    all_run_ids: list[str] = []

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        outer = progress.add_task(
            f"[bold white]{benchmark_name}", total=len(adapters)
        )

        for adapter in adapters:
            task_desc = f"[cyan]{adapter.name}[/cyan]"
            db_task: TaskID = progress.add_task(task_desc, total=None)

            run = RunConfig(
                benchmark_name=benchmark_name,
                database_name=adapter.name,
                dataset_name=dataset.schema.name,
                config_hash=config_hash,
                config_snapshot=env,
            )
            all_run_ids.append(run.run_id)
            storage.save_run(run)

            try:
                progress.update(db_task, description=f"{task_desc} [dim]connecting…[/dim]")
                await adapter.connect()

                version = await adapter.get_version()
                enrich_db_versions(env, adapter.name, version)
                storage.save_run(run)  # refresh with db version in snapshot

                progress.update(db_task, description=f"{task_desc} [dim]creating table…[/dim]")
                await adapter.drop_table(dataset.schema.name)
                await adapter.create_table(dataset.schema)

                # Start resource monitor
                monitor: ResourceMonitor | None = None
                if settings.monitor.enabled:
                    monitor = ResourceMonitor(interval_ms=settings.monitor.interval_ms)
                    monitor.start()

                progress.update(db_task, description=f"{task_desc} [dim]benchmarking…[/dim]")
                bench_result = await workload.run(
                    adapter, dataset, settings.workload, run.run_id
                )

                # Stop monitor and attach resource summary
                if monitor is not None:
                    samples = monitor.stop()
                    resource_summary = ResourceMonitor.get_summary(samples)
                    for summary in bench_result.summaries:
                        summary.resource = resource_summary

                # Save results
                if bench_result.operation_results:
                    storage.save_operation_results(bench_result.operation_results)
                for summary in bench_result.summaries:
                    storage.save_summary(summary)
                storage.complete_run(run.run_id)

                if bench_result.errors:
                    for err in bench_result.errors:
                        logger.warning("[%s] %s", adapter.name, err)

                progress.update(
                    db_task,
                    description=f"{task_desc} [green]done ✓[/green]",
                    completed=True,
                )

            except Exception as exc:
                logger.exception("Error benchmarking %s", adapter.name)
                console.print(f"[red][{adapter.name}] Error: {exc}[/red]")
                progress.update(
                    db_task,
                    description=f"{task_desc} [red]failed ✗[/red]",
                    completed=True,
                )
            finally:
                try:
                    await adapter.disconnect()
                except Exception:
                    pass

            progress.advance(outer)

    # ----------------------------------------------------------------
    # 6. Summary table
    # ----------------------------------------------------------------
    console.print()
    summaries = storage.load_summaries(run_ids=all_run_ids)
    if summaries:
        _print_summary_table(summaries, benchmark_name)
    console.print(f"\n[dim]Run IDs: {', '.join(all_run_ids)}[/dim]")


# ---------------------------------------------------------------------------
# generate
# ---------------------------------------------------------------------------


async def async_generate(
    n_devices: int,
    n_readings: int,
    seed: int,
    output_path: Path,
) -> None:
    """Generate synthetic IoT data and write to *output_path*."""
    from tsbm.datasets.generator import generate_iot_dataset
    import pyarrow.parquet as pq
    import pyarrow.csv as pa_csv

    console.print(
        f"Generating [bold]{n_devices * n_readings:,}[/bold] rows "
        f"({n_devices} devices × {n_readings} readings/device, seed={seed})…"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    bds = generate_iot_dataset(
        n_devices=n_devices,
        n_readings_per_device=n_readings,
        seed=seed,
        name=output_path.stem,
        lazy=False,
    )

    tbl = bds.load()
    suffix = output_path.suffix.lower()

    if suffix in (".parquet", ".parq"):
        pq.write_table(tbl, output_path, compression="snappy")
    elif suffix == ".csv":
        from tsbm.datasets.generator import generate_to_csv
        generate_to_csv(
            output_path=output_path,
            n_devices=n_devices,
            n_readings_per_device=n_readings,
            seed=seed,
        )
    else:
        # Default to Parquet
        pq.write_table(tbl, output_path.with_suffix(".parquet"), compression="snappy")
        output_path = output_path.with_suffix(".parquet")

    console.print(f"[green]Written:[/green] {output_path}  ({output_path.stat().st_size // 1024:,} KB)")


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


async def async_load(
    db_names: list[str] | None,
    dataset_path: Path | None,
    config_path: Path | None,
    drop_first: bool,
) -> None:
    """Ingest a dataset into one or more databases (no benchmark timing)."""
    if config_path is not None:
        settings = load_settings_from_file(config_path)
    else:
        settings = get_settings()

    effective_path = dataset_path or settings.workload.dataset
    console.print(f"[cyan]Loading dataset:[/cyan] {effective_path}")
    dataset = load_dataset(Path(effective_path))
    tbl = dataset.load()
    table_name = dataset.schema.name
    n_rows = len(tbl)

    adapters = get_enabled_adapters(db_names)
    if not adapters:
        console.print("[red]No adapters available.[/red]")
        return

    for adapter in adapters:
        console.print(f"\n[bold]{adapter.name}[/bold]")
        try:
            await adapter.connect()
            if drop_first:
                await adapter.drop_table(table_name)
            await adapter.create_table(dataset.schema)

            with console.status(f"Ingesting {n_rows:,} rows…"):
                timing = await adapter.ingest_batch(tbl, table_name)
                await adapter.flush()

            actual = await adapter.get_row_count(table_name)
            console.print(
                f"  Ingested [green]{actual:,}[/green] rows in "
                f"[bold]{timing.elapsed_ms:.0f}ms[/bold] "
                f"({timing.rows_per_second:,.0f} rows/sec)"
            )
        except Exception as exc:
            console.print(f"  [red]Error: {exc}[/red]")
        finally:
            try:
                await adapter.disconnect()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Rich summary table
# ---------------------------------------------------------------------------


def _print_summary_table(summaries: list[dict[str, Any]], benchmark_name: str) -> None:
    table = Table(
        title=f"Benchmark Results — [bold]{benchmark_name}[/bold]",
        show_header=True,
        header_style="bold magenta",
    )
    table.add_column("Database",     style="cyan",    no_wrap=True)
    table.add_column("Operation",    style="white")
    table.add_column("Batch",        justify="right")
    table.add_column("Workers",      justify="right")
    table.add_column("p50 ms",       justify="right", style="green")
    table.add_column("p99 ms",       justify="right", style="yellow")
    table.add_column("Rows/sec",     justify="right", style="bold white")
    table.add_column("Samples",      justify="right", style="dim")

    for row in summaries:
        if row.get("latency_p50_ms") is None:
            continue
        rps = row.get("rows_per_second_mean", 0)
        table.add_row(
            str(row.get("database_name", "")),
            str(row.get("operation", "")),
            f"{row.get('batch_size', 0):,}",
            str(row.get("workers", 1)),
            f"{row.get('latency_p50_ms', 0):.2f}",
            f"{row.get('latency_p99_ms', 0):.2f}",
            f"{rps:,.0f}" if rps else "—",
            str(row.get("sample_count", 0)),
        )

    console.print(table)

"""
Async orchestration for the run, generate, and load commands.

run orchestration
-----------------
1. Load config (from explicit path or default benchmark.toml)
2. Capture environment snapshot
3. Load / prepare dataset
4. For each benchmark (one or all):
   a. For each enabled adapter:
      i.  Connect, get version, create table
      ii. Pre-populate data for query-only benchmarks
      iii.Start ResourceMonitor (if enabled)
      iv. Run the selected benchmark workload
      v.  Stop monitor; attach ResourceSummary to each BenchmarkSummary
      vi. Save RunConfig, OperationResults, BenchmarkSummaries to storage
      vii.Disconnect
   b. Print Rich summary table with description and verdict for that benchmark
5. Print all run IDs
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
from itertools import groupby
from pathlib import Path
from typing import Any

from rich.console import Console
from rich.panel import Panel
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
from tsbm.benchmarks.registry import get_workload, list_default_workloads, list_workloads, list_workloads_ordered
from tsbm.config.settings import get_settings, load_settings_from_file
from tsbm.datasets.loader import (
    apply_unit_conversions,
    estimate_row_count,
    infer_schema_from_azure,
    infer_schema_from_sample,
    load_dataset,
    load_dataset_streaming,
    load_multi_dataset_streaming,
    resolve_dataset_sources,
)
from tsbm.environment.capture import capture_environment, enrich_db_versions
from tsbm.exceptions import ConfigError
from tsbm.metrics.monitor import ResourceMonitor
from tsbm.results.export import BENCHMARK_DESCRIPTIONS
from tsbm.results.models import RunConfig
from tsbm.results.storage import ResultStorage

logger = logging.getLogger(__name__)
console = Console()


# ---------------------------------------------------------------------------
# Benchmark metadata
# ---------------------------------------------------------------------------

# Benchmark descriptions are defined in results/export.py and re-exported here
_BENCHMARK_DESCRIPTIONS = BENCHMARK_DESCRIPTIONS

# Benchmarks where higher Rows/sec is the primary success metric
_THROUGHPUT_BENCHMARKS: frozenset[str] = frozenset({
    "ingestion",
    "ingestion_out_of_order",
    "mixed",
})

# Benchmarks that need the full dataset pre-loaded into the database before running.
# Pure query benchmarks share the seeded table (inherit seed across consecutive runs).
# mixed, materialized_view, and late_arrival also benefit from pre-existing data:
#   * mixed — readers are only meaningful when rows already exist
#   * materialized_view / late_arrival — create views over a populated table
_NEEDS_SEED_BENCHMARKS: frozenset[str] = frozenset({
    "time_range",
    "aggregation",
    "last_point",
    "high_cardinality",
    "downsampling",
    "mixed",
    "materialized_view",
    "late_arrival",
})

# Benchmarks that must drop + recreate the table on every run even if already seeded.
# Ingestion manages its own warmup/measurement data internally.
# MV benchmarks are NOT included here: they create/drop their own views and can
# safely reuse a pre-seeded table (late_arrival inserts negligible extra rows).
_ALWAYS_RESET_BENCHMARKS: frozenset[str] = frozenset({
    "ingestion",
    "ingestion_out_of_order",
})

# Keep the old name as an alias so the summary table renderer can still use it.
_QUERY_ONLY_BENCHMARKS: frozenset[str] = frozenset({
    "time_range",
    "aggregation",
    "last_point",
    "high_cardinality",
    "downsampling",
})

# ---------------------------------------------------------------------------
# IoT-scale latency thresholds
# ---------------------------------------------------------------------------
# Thresholds are (excellent_ms, good_ms, acceptable_ms) for the p99 metric.
#
# The critical distinction is RESULT SET SIZE:
#
#   Aggregation queries (SAMPLE BY / time_bucket / DATE_BIN / GROUP BY)
#   ─────────────────────────────────────────────────────────────────────
#   Return a SMALL, FIXED-SIZE result regardless of window width: e.g.
#   24 rows for hourly buckets over a day, or 168 rows for hourly over a
#   week.  The DB computes in C++ / SIMD and sends back kilobytes.
#   → Tight thresholds: 10-100 ms even on billions of rows is realistic.
#   → Exceeding "Good" here means the engine lacks vectorisation or the
#     chunk/partition strategy is wrong.
#
#   Raw-scan queries (SELECT * — time_range benchmark)
#   ─────────────────────────────────────────────────────────────────────
#   Return EVERY RAW ROW in the window.  A 1-week slice of 40 M rows
#   can be ~750 K rows.  Transferring and deserialising that over a socket
#   into Python dicts takes hundreds of milliseconds by physics alone —
#   independent of how fast the database engine is.
#   → Relaxed thresholds: the bottleneck is wire + deserialization, not DB.
#   → No real-time production IoT system should issue raw SELECT * over
#     multi-day windows; use aggregation instead.
#
# The "Good" tier maps to "fits on a real-time dashboard at p99"; the
# "Acceptable" tier maps to "tolerable for batch reports or ad-hoc queries".
_LATENCY_THRESHOLDS: dict[str, tuple[float, float, float]] = {
    # ── Aggregation (SAMPLE BY / time_bucket / DATE_BIN) ──────────────────
    # Returns N fixed-size buckets — tiny result, DB does the heavy lifting.
    # A vectorised TSDB engine should be in the 5-100 ms range here even at
    # billions of rows, because chunk pruning eliminates irrelevant data.
    "aggregation_1min":       (  5,   30,    300),   # ~1 bucket returned
    "aggregation_1h":         ( 20,  100,  1_000),   # ~60 buckets
    "aggregation_1day":       ( 50,  500,  5_000),   # ~1 440 buckets
    "aggregation_1week":      (100, 1_000, 10_000),  # ~10 080 buckets

    # ── High-cardinality GROUP BY (COUNT + AVG per tag) ────────────────────
    # Returns one row per unique device/tag — small result like aggregation,
    # but needs a hash GROUP BY step on top of the time filter.
    "high_cardinality_1min":  ( 10,   50,    500),
    "high_cardinality_1h":    ( 50,  300,  3_000),
    "high_cardinality_1day":  (200, 1_500, 15_000),
    "high_cardinality_1week": (500, 5_000, 30_000),

    # ── Time-range raw scan (SELECT * — result scales with window) ─────────
    # The DB may be fast but the bottleneck is moving rows over the wire and
    # deserialising them in Python.  Thresholds are deliberately looser.
    "time_range_1min":        ( 10,   50,    500),   # few hundred rows
    "time_range_1h":          ( 50,  300,  3_000),   # thousands of rows
    "time_range_1day":        (200, 1_500, 15_000),  # tens of thousands
    "time_range_1week":       (500, 5_000, 30_000),  # hundreds of thousands

    # ── Last-point (most recent value per device, full-table) ─────────────
    # QuestDB LATEST ON uses a native reversed-partition index — often < 20 ms.
    # TimescaleDB DISTINCT ON is O(n_chunks).  CrateDB ROW_NUMBER() is slower.
    # Thresholds sit at the TimescaleDB/CrateDB level; QuestDB will typically
    # land in Excellent.
    "last_point":             (100,  1_000, 10_000),

    # ── Downsampling (full dataset, multi-resolution, no time filter) ──────
    # Aggregates everything at 1-min / 1-hour / 1-day granularity — the
    # heaviest query in the suite.  Even vectorised engines need seconds on
    # 40 M+ rows without a pre-built continuous aggregate / MV.
    "downsampling":           (500, 5_000, 30_000),

    # ── Mixed benchmark ───────────────────────────────────────────────────
    "mixed_read":             (100,  1_000,  5_000),
    "mixed_write":            ( 10,    100,  1_000),
}
# Fallback for any operation name not in the table above.
_DEFAULT_LATENCY_THRESHOLD: tuple[float, float, float] = (50, 500, 5_000)


def _get_latency_thresholds(operation: str) -> tuple[float, float, float]:
    """
    Return ``(excellent_ms, good_ms, acceptable_ms)`` for *operation*.

    Tries an exact match first, then a prefix match, then falls back to
    ``_DEFAULT_LATENCY_THRESHOLD``.
    """
    if operation in _LATENCY_THRESHOLDS:
        return _LATENCY_THRESHOLDS[operation]
    for key, thresholds in _LATENCY_THRESHOLDS.items():
        if operation.startswith(key):
            return thresholds
    return _DEFAULT_LATENCY_THRESHOLD


# ---------------------------------------------------------------------------
# run
# ---------------------------------------------------------------------------


async def async_run(
    db_names: list[str] | None,
    benchmark_name: str,
    config_path: Path | None,
    dataset_path: Path | None,
    timestamp_col: str | None = None,
    dataset_list: list[str] | None = None,
) -> None:
    """Full orchestration for the ``tsbm run`` command."""

    # ----------------------------------------------------------------
    # 1. Config
    # ----------------------------------------------------------------
    if config_path is not None:
        settings = load_settings_from_file(config_path)
    else:
        settings = get_settings()

    known = list_workloads()
    if benchmark_name == "all":
        workload_names = list_workloads_ordered()
    elif benchmark_name == "default":
        workload_names = list_default_workloads()
    else:
        if benchmark_name not in known:
            raise ConfigError(
                f"Unknown benchmark {benchmark_name!r}. "
                f"Available: {', '.join(known)}, default, all"
            )
        workload_names = [benchmark_name]

    # CLI --dataset (multiple) overrides config datasets list
    if dataset_list:
        settings.workload.datasets = dataset_list

    effective_dataset = dataset_path or settings.workload.dataset

    # ----------------------------------------------------------------
    # 2. Environment snapshot
    # ----------------------------------------------------------------
    env = capture_environment()
    config_hash = hashlib.md5(
        json.dumps(settings.model_dump(), sort_keys=True, default=str).encode()
    ).hexdigest()[:8]

    # ----------------------------------------------------------------
    # 3. Dataset — multi-source or single file, auto-stream for large files
    # ----------------------------------------------------------------
    use_multi = bool(settings.workload.datasets) and not dataset_path
    azure_conn = settings.workload.azure_storage_connection_string
    azure_sas = settings.workload.azure_storage_sas_token

    if use_multi:
        # Multi-source mode: multiple local files, globs, and/or Azure URLs
        sources = resolve_dataset_sources(
            settings.workload.datasets,
            azure_connection_string=azure_conn,
            azure_sas_token=azure_sas,
        )
        console.print(f"[cyan]Loading {len(sources)} dataset source(s):[/cyan]")
        for src in sources:
            console.print(f"  [dim]{src}[/dim]")

        # Infer schema from first available source
        from tsbm.datasets.loader import _is_azure_url
        first_local = next((s for s in sources if isinstance(s, Path)), None)
        if first_local:
            schema_hint = infer_schema_from_sample(
                first_local,
                tag_cardinality_threshold=settings.workload.tag_cardinality_threshold,
            )
        else:
            # All sources are Azure — infer from first URL
            schema_hint = infer_schema_from_azure(
                str(sources[0]),
                tag_cardinality_threshold=settings.workload.tag_cardinality_threshold,
                connection_string=azure_conn,
                sas_token=azure_sas,
            )

        dataset = load_multi_dataset_streaming(
            sources,
            schema_hint=schema_hint,
            chunk_size=settings.workload.chunk_size,
            azure_connection_string=azure_conn,
            azure_sas_token=azure_sas,
        )
    else:
        # Single-file mode (original behavior)
        effective_dataset_path = Path(effective_dataset)
        console.print(f"[cyan]Loading dataset:[/cyan] {effective_dataset_path}")

        n_rows_est = estimate_row_count(effective_dataset_path)
        streaming_threshold = settings.workload.streaming_threshold_rows

        if n_rows_est > streaming_threshold:
            console.print(
                f"  [dim]~{n_rows_est:,} rows > streaming threshold {streaming_threshold:,} "
                f"— using streaming mode (chunk_size={settings.workload.chunk_size:,})[/dim]"
            )
            schema_hint = infer_schema_from_sample(
                effective_dataset_path,
                sample_rows=min(50_000, n_rows_est),
                tag_cardinality_threshold=settings.workload.tag_cardinality_threshold,
            )
            dataset = load_dataset_streaming(
                effective_dataset_path,
                schema_hint=schema_hint,
                chunk_size=settings.workload.chunk_size,
            )
        else:
            dataset = load_dataset(effective_dataset_path)

    # Apply unit conversions if configured
    if settings.workload.unit_conversions and dataset.table is not None:
        dataset.table = apply_unit_conversions(dataset.table, dict(settings.workload.unit_conversions))
        console.print(f"  Unit conversions applied: {dict(settings.workload.unit_conversions)}")
    elif settings.workload.unit_conversions and dataset.streaming:
        # Streaming mode: attach conversions for per-batch application
        dataset._unit_conversions = dict(settings.workload.unit_conversions)
        console.print(f"  Unit conversions (streaming mode): {dict(settings.workload.unit_conversions)}")

    # Apply timestamp column override: CLI flag takes precedence over toml setting.
    effective_ts_col = timestamp_col or settings.workload.timestamp_col or ""
    if effective_ts_col:
        from tsbm.datasets.loader import override_timestamp_col
        dataset = override_timestamp_col(dataset, effective_ts_col)
        console.print(f"  Timestamp column overridden: [bold]{effective_ts_col}[/bold]")

    console.print(
        f"  Schema: [bold]{dataset.schema.name}[/bold] "
        f"({dataset.schema.row_count:,} rows, "
        f"ts=[bold]{dataset.schema.timestamp_col}[/bold], "
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

    if len(workload_names) > 1:
        console.print(
            f"\n[bold]Running {len(workload_names)} benchmarks:[/bold] "
            f"{', '.join(workload_names)}  "
            f"[bold]Databases:[/bold] {', '.join(a.name for a in adapters)}\n"
        )
    else:
        console.print(
            f"\n[bold]Benchmark:[/bold] {workload_names[0]}  "
            f"[bold]Databases:[/bold] {', '.join(a.name for a in adapters)}\n"
        )

    # ----------------------------------------------------------------
    # 6. Pre-seed adapters in parallel (if query benchmarks will run)
    # ----------------------------------------------------------------
    seeded_adapters: set[str] = set()

    # Check whether any of the upcoming benchmarks need seeded data.
    # If so, seed adapters *once* upfront so every query/MV benchmark
    # reuses the table — regardless of how many adapters are enabled.
    first_needs_seed = any(
        wl in _NEEDS_SEED_BENCHMARKS and wl not in _ALWAYS_RESET_BENCHMARKS
        for wl in workload_names
    )
    if first_needs_seed:
        n = len(adapters)
        label = "database" if n == 1 else f"{n} databases in parallel"
        console.print(f"\n[bold]Pre-seeding {label}…[/bold]")
        await _parallel_seed_adapters(adapters, dataset, seeded_adapters)
        console.print("[green]Seeding complete.[/green]")

    # ----------------------------------------------------------------
    # 7. Run each benchmark
    # ----------------------------------------------------------------
    all_run_ids: list[str] = []

    for bench_idx, wl_name in enumerate(workload_names, 1):
        if len(workload_names) > 1:
            console.print(
                f"\n[bold cyan]({bench_idx}/{len(workload_names)}) {wl_name}[/bold cyan]"
            )

        workload = get_workload(wl_name)
        run_ids = await _run_adapters_for_workload(
            workload=workload,
            workload_name=wl_name,
            adapters=adapters,
            dataset=dataset,
            settings=settings,
            storage=storage,
            env=env,
            config_hash=config_hash,
            seeded_adapters=seeded_adapters,
        )
        all_run_ids.extend(run_ids)

        # Per-benchmark summary
        summaries = storage.load_summaries(run_ids=run_ids)
        if summaries:
            console.print()
            _print_summary_table(summaries, wl_name)
        console.print(f"[dim]Run IDs ({wl_name}): {', '.join(run_ids)}[/dim]")

    if len(workload_names) > 1:
        console.print(f"\n[dim]All run IDs: {', '.join(all_run_ids)}[/dim]")

    # Print the metric glossary once at the very end so the user has a
    # reference for every column they just saw in the results tables.
    console.print()
    _print_metric_glossary()


async def _seed_one_adapter(
    adapter: Any,
    dataset: Any,
) -> None:
    """Connect to *adapter*, drop/create table, and ingest the full dataset."""
    await adapter.connect()
    try:
        await adapter.drop_table(dataset.schema.name)
        await adapter.create_table(dataset.schema)
        if dataset.streaming:
            for chunk in dataset.iter_batches():
                await adapter.ingest_batch(chunk, dataset.schema.name)
        else:
            tbl = dataset.load()
            await adapter.ingest_batch(tbl, dataset.schema.name)
        await adapter.flush()
    finally:
        await adapter.disconnect()


async def _parallel_seed_adapters(
    adapters: list[Any],
    dataset: Any,
    seeded_adapters: set[str],
) -> None:
    """Seed all *adapters* with the full dataset in parallel."""
    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        outer = progress.add_task("[bold white]seeding", total=len(adapters))
        tasks_map: dict[str, TaskID] = {}
        for adapter in adapters:
            tid = progress.add_task(f"[cyan]{adapter.name}[/cyan] [dim]loading…[/dim]", total=None)
            tasks_map[adapter.name] = tid

        async def _seed_with_progress(adapter: Any) -> None:
            try:
                await _seed_one_adapter(adapter, dataset)
                seeded_adapters.add(adapter.name)
                progress.update(
                    tasks_map[adapter.name],
                    description=f"[cyan]{adapter.name}[/cyan] [green]seeded[/green]",
                    completed=True,
                )
            except Exception as exc:
                logger.exception("Failed to seed %s", adapter.name)
                console.print(f"[red][{adapter.name}] Seed error: {exc}[/red]")
                progress.update(
                    tasks_map[adapter.name],
                    description=f"[cyan]{adapter.name}[/cyan] [red]failed[/red]",
                    completed=True,
                )
            progress.advance(outer)

        await asyncio.gather(*[_seed_with_progress(a) for a in adapters])


async def _run_adapters_for_workload(
    workload: Any,
    workload_name: str,
    adapters: list[Any],
    dataset: Any,
    settings: Any,
    storage: ResultStorage,
    env: dict[str, Any],
    config_hash: str,
    seeded_adapters: set[str] | None = None,
) -> list[str]:
    """Run *workload* against every adapter; return the list of run_ids created."""
    if seeded_adapters is None:
        seeded_adapters = set()
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
            f"[bold white]{workload_name}", total=len(adapters)
        )

        for adapter in adapters:
            task_desc = f"[cyan]{adapter.name}[/cyan]"
            db_task: TaskID = progress.add_task(task_desc, total=None)

            snapshot = dict(env)
            snapshot["dataset_schema"] = dataset.schema.to_dict()
            run = RunConfig(
                benchmark_name=workload_name,
                database_name=adapter.name,
                dataset_name=dataset.schema.name,
                config_hash=config_hash,
                config_snapshot=snapshot,
            )
            all_run_ids.append(run.run_id)
            storage.save_run(run)

            try:
                progress.update(db_task, description=f"{task_desc} [dim]connecting…[/dim]")
                await adapter.connect()

                version = await adapter.get_version()
                enrich_db_versions(env, adapter.name, version)
                storage.save_run(run)  # refresh with db version in snapshot

                needs_seed = workload_name in _NEEDS_SEED_BENCHMARKS
                always_reset = workload_name in _ALWAYS_RESET_BENCHMARKS
                already_seeded = adapter.name in seeded_adapters

                if needs_seed and already_seeded and not always_reset:
                    # Reuse the data seeded by a previous benchmark — skip the
                    # drop/create/ingest cycle to avoid redundant I/O.
                    # Applies to consecutive query benchmarks and to mixed when
                    # query benchmarks have already populated the table.
                    progress.update(
                        db_task,
                        description=f"{task_desc} [dim]reusing seeded table…[/dim]",
                    )
                else:
                    # Drop, recreate, and optionally seed the table.
                    # Ingestion and OOO-ingestion do not pre-seed here — they
                    # manage their own warmup/measurement data internally.
                    # MV benchmarks always reset so stale views don't interfere.
                    seeded_adapters.discard(adapter.name)

                    progress.update(db_task, description=f"{task_desc} [dim]creating table…[/dim]")
                    await adapter.drop_table(dataset.schema.name)
                    await adapter.create_table(dataset.schema)

                    if needs_seed:
                        # Pre-load the full dataset before the benchmark runs.
                        # In streaming mode, chunks are fed one at a time to
                        # avoid loading the whole dataset into RAM at once.
                        progress.update(db_task, description=f"{task_desc} [dim]loading data…[/dim]")
                        if dataset.streaming:
                            for _chunk in dataset.iter_batches():
                                await adapter.ingest_batch(_chunk, dataset.schema.name)
                        else:
                            _tbl = dataset.load()
                            await adapter.ingest_batch(_tbl, dataset.schema.name)
                        await adapter.flush()
                        seeded_adapters.add(adapter.name)

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

    return all_run_ids


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
# Rich summary table helpers
# ---------------------------------------------------------------------------


def _assign_verdicts(summaries: list[dict[str, Any]], benchmark_name: str) -> dict[int, str]:
    """
    Return a mapping of row-index → Rich-formatted verdict string.

    When multiple databases appear for the same (operation, batch_size, workers)
    combination the verdict is relative (Best / Mid / Worst).  For a single
    database an absolute threshold is used instead.
    """
    is_throughput = benchmark_name in _THROUGHPUT_BENCHMARKS
    verdicts: dict[int, str] = {}

    def _group_key(item: tuple[int, dict]) -> tuple:
        r = item[1]
        return (r.get("operation", ""), r.get("batch_size", 0), r.get("workers", 1))

    indexed = list(enumerate(summaries))
    sorted_items = sorted(indexed, key=_group_key)

    for _, group_iter in groupby(sorted_items, key=_group_key):
        group = list(group_iter)

        if len(group) == 1:
            idx, row = group[0]
            if is_throughput:
                rps = row.get("rows_per_second_mean", 0) or 0
                if rps >= 500_000:
                    verdicts[idx] = "[green]Excellent[/green]"
                elif rps >= 100_000:
                    verdicts[idx] = "[green]Good[/green]"
                elif rps >= 20_000:
                    verdicts[idx] = "[yellow]Acceptable[/yellow]"
                else:
                    verdicts[idx] = "[red]Poor[/red]"
            else:
                p99 = row.get("latency_p99_ms", 0) or 0
                op = row.get("operation", "")
                excellent_ms, good_ms, acceptable_ms = _get_latency_thresholds(op)
                if p99 <= excellent_ms:
                    verdicts[idx] = "[green]Excellent[/green]"
                elif p99 <= good_ms:
                    verdicts[idx] = "[green]Good[/green]"
                elif p99 <= acceptable_ms:
                    verdicts[idx] = "[yellow]Acceptable[/yellow]"
                else:
                    verdicts[idx] = "[red]Poor[/red]"
        else:
            # Relative ranking within this group
            if is_throughput:
                ranked = sorted(
                    group,
                    key=lambda x: x[1].get("rows_per_second_mean", 0) or 0,
                    reverse=True,
                )
            else:
                ranked = sorted(
                    group,
                    key=lambda x: x[1].get("latency_p99_ms", float("inf")) or float("inf"),
                )
            for rank, (idx, _) in enumerate(ranked):
                if rank == 0:
                    verdicts[idx] = "[green]Best[/green]"
                elif rank == len(ranked) - 1:
                    verdicts[idx] = "[red]Worst[/red]"
                else:
                    verdicts[idx] = "[yellow]Mid[/yellow]"

    return verdicts


def _print_summary_table(summaries: list[dict[str, Any]], benchmark_name: str) -> None:
    # Description panel
    desc = _BENCHMARK_DESCRIPTIONS.get(benchmark_name)
    if desc:
        console.print(
            Panel(
                f"[bold]{benchmark_name}[/bold]\n[dim]{desc}[/dim]",
                border_style="blue",
                padding=(0, 1),
            )
        )

    verdicts = _assign_verdicts(summaries, benchmark_name)

    # --- Table 1: Speed ---
    speed_table = Table(
        title=f"Speed — [bold]{benchmark_name}[/bold]",
        show_header=True,
        header_style="bold magenta",
    )
    speed_table.add_column("Database",  style="cyan",    no_wrap=True)
    speed_table.add_column("Operation", style="white")
    speed_table.add_column("Batch",     justify="right")
    speed_table.add_column("Workers",   justify="right")
    speed_table.add_column("p50 ms",    justify="right", style="green")
    speed_table.add_column("Mean ms",   justify="right", style="green")
    speed_table.add_column("p95 ms",    justify="right", style="yellow")
    speed_table.add_column("p99 ms",    justify="right", style="yellow")
    speed_table.add_column("p99.9 ms",  justify="right", style="red")
    speed_table.add_column("Rows/sec",  justify="right", style="bold white")
    speed_table.add_column("MB/sec",    justify="right", style="bold white")
    speed_table.add_column("Samples",   justify="right", style="dim")
    speed_table.add_column("Verdict",   justify="center")

    for i, row in enumerate(summaries):
        if row.get("latency_p50_ms") is None:
            continue
        rps = row.get("rows_per_second_mean", 0)
        mbs = row.get("mb_per_second_mean", 0)
        speed_table.add_row(
            str(row.get("database_name", "")),
            str(row.get("operation", "")),
            f"{row.get('batch_size', 0):,}",
            str(row.get("workers", 1)),
            f"{row.get('latency_p50_ms', 0):.2f}",
            f"{row.get('latency_mean_ms', 0):.2f}",
            f"{row.get('latency_p95_ms', 0):.2f}",
            f"{row.get('latency_p99_ms', 0):.2f}",
            f"{row.get('latency_p999_ms', 0):.2f}",
            f"{rps:,.0f}" if rps else "—",
            f"{mbs:.1f}" if mbs else "—",
            str(row.get("sample_count", 0)),
            verdicts.get(i, ""),
        )

    console.print(speed_table)

    # --- Table 2: Consistency ---
    consistency_table = Table(
        title=f"Consistency — [bold]{benchmark_name}[/bold]",
        show_header=True,
        header_style="bold blue",
    )
    consistency_table.add_column("Database",   style="cyan",  no_wrap=True)
    consistency_table.add_column("Operation",  style="white")
    consistency_table.add_column("Stddev ms",  justify="right", style="yellow")
    consistency_table.add_column("CV%",        justify="right", style="yellow")
    consistency_table.add_column("IQR ms",     justify="right", style="white")
    consistency_table.add_column("Min ms",     justify="right", style="green")
    consistency_table.add_column("Max ms",     justify="right", style="red")
    consistency_table.add_column("Outliers",   justify="right", style="dim")

    for row in summaries:
        if row.get("latency_p50_ms") is None:
            continue
        stddev = row.get("latency_stddev_ms", 0) or 0
        mean = row.get("latency_mean_ms", 0) or 0
        cv = f"{stddev / mean * 100:.1f}%" if mean > 0 else "—"
        consistency_table.add_row(
            str(row.get("database_name", "")),
            str(row.get("operation", "")),
            f"{stddev:.2f}",
            cv,
            f"{row.get('latency_iqr_ms', 0):.2f}",
            f"{row.get('latency_min_ms', 0):.2f}",
            f"{row.get('latency_max_ms', 0):.2f}",
            str(row.get("latency_outlier_count", 0)),
        )

    console.print(consistency_table)
    _print_interpretation(summaries, benchmark_name)


def _print_metric_glossary() -> None:
    """
    Print a plain-English panel explaining every column in the Speed and
    Consistency tables, plus the Verdict thresholds.

    Called once at the end of a benchmark run so the user can reference the
    glossary after reading the results.
    """
    speed_lines = [
        "[bold]Speed table — what each column means:[/bold]",
        "",
        "  [green]p50 ms[/green]"
        "      Median latency. Half of all queries finished faster than this number.",
        "              This is the most reliable indicator of 'typical' speed.",
        "  [green]Mean ms[/green]"
        "     Mathematical average. A few very slow queries can pull this above p50,",
        "              so p50 is usually a better indicator of day-to-day performance.",
        "  [yellow]p95 ms[/yellow]"
        "      95th percentile. 95 out of every 100 queries finished faster than this.",
        "              Use this to judge performance on a 'slow day'.",
        "  [yellow]p99 ms[/yellow]"
        "      99th percentile. Only 1 in 100 queries was slower. This is the industry",
        "              standard for Service Level Agreements (SLAs) and dashboard targets.",
        "              [bold]This value drives the Verdict.[/bold]",
        "  [red]p99.9 ms[/red]"
        "    Worst 1 in 1,000 queries (extreme tail). Always shown in red to flag it",
        "              as an outlier metric — the red colour is cosmetic, not a verdict.",
        "  Rows/sec    How many data rows the database processed per second. Higher is better.",
        "              For ingestion this is write speed; for queries it is scan throughput.",
        "  MB/sec      Same as Rows/sec but measured in megabytes of data.",
        "  Samples     Number of individual query measurements taken. More samples =",
        "              more reliable statistics (p99 with 5 samples is a rough estimate).",
    ]

    consistency_lines = [
        "",
        "[bold]Consistency table — how stable (predictable) the database is:[/bold]",
        "",
        "  Stddev ms   Standard deviation of latency. Low = responses arrive at a steady",
        "              pace. High = unpredictable spikes (bad for real-time dashboards).",
        "  CV%         Coefficient of Variation = Stddev ÷ Mean × 100.",
        "              Under 20% → very consistent.  20–50% → moderate jitter.",
        "              Over 50% → erratic; investigate GC pauses, compaction, or I/O.",
        "  IQR ms      Interquartile range — the spread of the middle 50% of queries.",
        "              Unlike Stddev, IQR ignores extreme outliers so it shows 'normal'",
        "              variability without being distorted by one-off slow queries.",
        "  [green]Min ms[/green]"
        "      The single fastest query observed across all measurement rounds.",
        "  [red]Max ms[/red]"
        "      The single slowest query observed. Always red to highlight it as the",
        "              worst case — not a verdict, just an eye-catcher for investigation.",
        "  Outliers    Count of queries that were statistically much slower than the rest",
        "              (outside 1.5 × IQR above Q3). High counts suggest background jobs,",
        "              compaction, or garbage collection interfering with query latency.",
    ]

    verdict_lines = [
        "",
        "[bold]Verdict — how the rating is assigned:[/bold]",
        "",
        "  The key to understanding these results is knowing what the database is actually",
        "  being asked to do. There are two fundamentally different query types here:",
        "",
        "  ┌─ [bold][green]Aggregation queries[/green][/bold] ─────────────────────────────────────────────────────┐",
        "  │  Benchmarks: aggregation, high_cardinality, downsampling                      │",
        "  │                                                                               │",
        "  │  The database crunches millions of rows internally and sends back a           │",
        "  │  tiny summary — for example, 24 hourly averages over a 1-day window,          │",
        "  │  or one row per device for last_point. The answer is always small.            │",
        "  │                                                                               │",
        "  │  → A well-configured TSDB should answer in milliseconds even on billions      │",
        "  │    of rows, because vectorised engines + chunk pruning do the heavy           │",
        "  │    lifting without shipping raw data over the wire.                           │",
        "  │                                                                               │",
        "  │  → If aggregation is slow, the database is not using its engine properly:     │",
        "  │    check chunk/partition size, continuous aggregates, or compression.         │",
        "  └───────────────────────────────────────────────────────────────────────────────┘",
        "",
        "  ┌─ [bold][yellow]Raw-scan queries[/yellow][/bold] ──────────────────────────────────────────────────────┐",
        "  │  Benchmark: time_range                                                        │",
        "  │                                                                               │",
        "  │  SELECT * ships every raw row in the time window back to the client.          │",
        "  │  A 1-week window over a 40 M-row dataset can return ~750 000 rows.            │",
        "  │  Moving and deserialising that much data takes time regardless of             │",
        "  │  how fast the database engine is — the bottleneck is the wire and             │",
        "  │  the Python client, not the DB.                                               │",
        "  │                                                                               │",
        "  │  → Wide-window time_range results will naturally be slower. This is           │",
        "  │    expected, not a sign of a poorly configured database.                      │",
        "  │                                                                               │",
        "  │  → In a real IoT production system you would never run SELECT * over          │",
        "  │    a week of data — you would aggregate first. time_range benchmarks          │",
        "  │    stress the raw I/O path, which is a useful but different test.             │",
        "  └───────────────────────────────────────────────────────────────────────────────┘",
        "",
        "  [green]Excellent[/green]   Optimal — DB uses vectorised engines, chunk pruning, or native indexes.",
        "  [green]Good[/green]        Fast enough for real-time IoT dashboards and live monitoring.",
        "  [yellow]Acceptable[/yellow]  Fine for batch reports or scheduled jobs; too slow for live queries.",
        "  [red]Poor[/red]        Investigate: chunk/partition size, missing indexes, RAM, or query design.",
        "",
        "  When benchmarking [bold]multiple databases[/bold] simultaneously the Verdict is relative:",
        "  [green]Best[/green] / [yellow]Mid[/yellow] / [red]Worst[/red] within that group — no absolute thresholds apply.",
        "",
        "  [dim]Threshold guide (based on p99 latency):[/dim]",
        "  [dim]                      Excellent    Good       Acceptable[/dim]",
        "  [dim]  aggregation_1min    ≤  5 ms      ≤  30 ms   ≤  300 ms[/dim]",
        "  [dim]  aggregation_1h      ≤ 20 ms      ≤ 100 ms   ≤    1 s [/dim]",
        "  [dim]  aggregation_1day    ≤ 50 ms      ≤ 500 ms   ≤    5 s [/dim]",
        "  [dim]  aggregation_1week   ≤ 100 ms     ≤   1 s    ≤   10 s [/dim]",
        "  [dim]  time_range_1min     ≤  10 ms     ≤  50 ms   ≤  500 ms[/dim]",
        "  [dim]  time_range_1h       ≤  50 ms     ≤ 300 ms   ≤    3 s [/dim]",
        "  [dim]  time_range_1day     ≤ 200 ms     ≤ 1.5 s    ≤   15 s [/dim]",
        "  [dim]  time_range_1week    ≤ 500 ms     ≤   5 s    ≤   30 s [/dim]",
        "  [dim]  last_point          ≤ 100 ms     ≤   1 s    ≤   10 s [/dim]",
        "  [dim]  downsampling        ≤ 500 ms     ≤   5 s    ≤   30 s [/dim]",
    ]

    console.print(
        Panel(
            "\n".join(speed_lines + consistency_lines + verdict_lines),
            title="[bold dim]Metric Glossary — how to read these results[/bold dim]",
            border_style="dim",
            padding=(0, 1),
        )
    )


def _print_interpretation(summaries: list[dict[str, Any]], benchmark_name: str) -> None:
    """Print a human-readable interpretation panel below the results table."""
    is_throughput = benchmark_name in _THROUGHPUT_BENCHMARKS
    lines: list[str] = []

    # Group rows by operation (there may be several for multi-param benchmarks)
    ops: dict[str, list[dict]] = {}
    for row in summaries:
        op = row.get("operation", benchmark_name)
        ops.setdefault(op, []).append(row)

    for rows in ops.values():
        valid = [r for r in rows if r.get("latency_p50_ms") is not None]
        if not valid:
            continue

        if is_throughput:
            best = max(valid, key=lambda r: r.get("rows_per_second_mean", 0) or 0)
            worst = min(valid, key=lambda r: r.get("rows_per_second_mean", 0) or 0)
            best_rps = best.get("rows_per_second_mean", 0) or 0
            worst_rps = worst.get("rows_per_second_mean", 0) or 0

            if len(valid) > 1:
                ratio = f" ({best_rps / worst_rps:.1f}× faster)" if worst_rps > 0 else ""
                lines.append(
                    f"[green]↑[/green] [bold]{best['database_name']}[/bold] leads at "
                    f"[bold]{best_rps:,.0f}[/bold] rows/sec; "
                    f"[bold]{worst['database_name']}[/bold] trails at "
                    f"[bold]{worst_rps:,.0f}[/bold] rows/sec{ratio}."
                )
            else:
                icon = "[green]↑[/green]" if best_rps >= 100_000 else "[yellow]~[/yellow]" if best_rps >= 20_000 else "[red]↓[/red]"
                label = "Excellent" if best_rps >= 500_000 else "Good" if best_rps >= 100_000 else "Acceptable" if best_rps >= 20_000 else "Poor"
                lines.append(f"{icon} Throughput: [bold]{best_rps:,.0f}[/bold] rows/sec — {label}.")
        else:
            best = min(valid, key=lambda r: r.get("latency_p99_ms", float("inf")) or float("inf"))
            worst = max(valid, key=lambda r: r.get("latency_p99_ms", 0) or 0)
            best_p99 = best.get("latency_p99_ms", 0) or 0
            worst_p99 = worst.get("latency_p99_ms", 0) or 0
            op = rows[0].get("operation", "")
            excellent_ms, good_ms, acceptable_ms = _get_latency_thresholds(op)

            if len(valid) > 1:
                ratio = f" ({worst_p99 / best_p99:.1f}× slower)" if best_p99 > 0 else ""
                lines.append(
                    f"[green]↓[/green] Best p99: [bold]{best['database_name']}[/bold] at "
                    f"[bold]{best_p99:.1f} ms[/bold]; "
                    f"[bold]{worst['database_name']}[/bold] at "
                    f"[bold]{worst_p99:.1f} ms[/bold]{ratio}."
                )
            else:
                icon = (
                    "[green]↓[/green]" if best_p99 <= good_ms
                    else "[yellow]~[/yellow]" if best_p99 <= acceptable_ms
                    else "[red]↑[/red]"
                )
                label = (
                    "Excellent" if best_p99 <= excellent_ms
                    else "Good" if best_p99 <= good_ms
                    else "Acceptable" if best_p99 <= acceptable_ms
                    else "Poor"
                )
                lines.append(f"{icon} p99 latency: [bold]{best_p99:.1f} ms[/bold] — {label}.")

    if lines:
        console.print(
            Panel(
                "\n".join(lines),
                title="[bold]Interpretation[/bold]",
                border_style="dim",
                padding=(0, 1),
            )
        )

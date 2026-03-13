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
    "ingestion",
    "ingestion_out_of_order",
})

# Benchmarks that must drop + recreate the table on every run even if already seeded.
# Empty: all benchmarks now reuse the pre-seeded table.  Ingestion benchmarks
# insert additional data on top of the existing rows (more realistic).
_ALWAYS_RESET_BENCHMARKS: frozenset[str] = frozenset()

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
# Thresholds are (great_ms, solid_ms, fair_ms) for the p99 metric.
#
# These include headroom for Docker-bridge / loopback network overhead and
# Python client-side deserialization lag (~5-20 ms baseline jitter depending
# on result size).  Don't read too much into small differences — the goal is
# to surface obviously-good vs obviously-slow behaviour, not to split hairs.
#
# The critical distinction is RESULT SET SIZE:
#
#   Aggregation queries (SAMPLE BY / time_bucket / DATE_BIN / GROUP BY)
#   ─────────────────────────────────────────────────────────────────────
#   Return a SMALL, FIXED-SIZE result regardless of window width: e.g.
#   24 rows for hourly buckets over a day, or 168 rows for hourly over a
#   week.  The DB computes in C++ / SIMD and sends back kilobytes.
#
#   Raw-scan queries (SELECT * — time_range benchmark)
#   ─────────────────────────────────────────────────────────────────────
#   Return EVERY RAW ROW in the window.  A 1-week slice of 40 M rows
#   can be ~750 K rows.  Transferring and deserialising that over a socket
#   into Python dicts takes hundreds of milliseconds by physics alone —
#   independent of how fast the database engine is.
#
# The "Solid" tier maps to "fits on a real-time dashboard at p99"; the
# "Fair" tier maps to "tolerable for batch reports or ad-hoc queries".
_LATENCY_THRESHOLDS: dict[str, tuple[float, float, float]] = {
    # ── Aggregation (SAMPLE BY / time_bucket / DATE_BIN) ──────────────────
    # Returns N fixed-size buckets — tiny result, DB does the heavy lifting.
    # +10-20 ms headroom for network round-trip and client overhead.
    "aggregation_1min":       ( 15,   50,    400),   # ~1 bucket returned
    "aggregation_1h":         ( 35,  150,  1_500),   # ~60 buckets
    "aggregation_1day":       ( 75,  650,  6_500),   # ~1 440 buckets
    "aggregation_1week":      (150, 1_300, 13_000),  # ~10 080 buckets

    # ── High-cardinality GROUP BY (COUNT + AVG per tag) ────────────────────
    # Returns one row per unique device/tag — small result like aggregation,
    # but needs a hash GROUP BY step on top of the time filter.
    "high_cardinality_1min":  ( 20,   75,    650),
    "high_cardinality_1h":    ( 75,  400,  4_000),
    "high_cardinality_1day":  (250, 2_000, 18_000),
    "high_cardinality_1week": (650, 6_500, 38_000),

    # ── Time-range raw scan (SELECT * — result scales with window) ─────────
    # The DB may be fast but the bottleneck is moving rows over the wire and
    # deserialising them in Python.  Thresholds are deliberately loose.
    "time_range_1min":        ( 20,   75,    650),   # few hundred rows
    "time_range_1h":          ( 75,  400,  4_000),   # thousands of rows
    "time_range_1day":        (250, 2_000, 18_000),  # tens of thousands
    "time_range_1week":       (650, 6_500, 38_000),  # hundreds of thousands

    # ── Last-point (most recent value per device, full-table) ─────────────
    # QuestDB LATEST ON is typically < 20 ms.  TimescaleDB / CrateDB are
    # slower.  Extra headroom for network + client overhead.
    "last_point":             (150,  1_300, 13_000),

    # ── Downsampling (full dataset, multi-resolution, no time filter) ──────
    # Heaviest query in the suite — even vectorised engines need seconds on
    # 40 M+ rows.  Network overhead is small relative to total time here.
    "downsampling":           (650, 6_500, 38_000),

    # ── Mixed benchmark ───────────────────────────────────────────────────
    "mixed_read":             (150,  1_300,  6_500),
    "mixed_write":            ( 20,    150,  1_500),
}
# Fallback for any operation name not in the table above.
_DEFAULT_LATENCY_THRESHOLD: tuple[float, float, float] = (75, 650, 6_500)


def _get_latency_thresholds(operation: str) -> tuple[float, float, float]:
    """
    Return ``(great_ms, solid_ms, fair_ms)`` for *operation*.

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
    verbose: bool = False,
    skip_seed: bool = False,
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
    azure_acct = settings.workload.azure_storage_account_name

    if use_multi:
        # Multi-source mode: multiple local files, globs, and/or Azure URLs
        sources = resolve_dataset_sources(
            settings.workload.datasets,
            azure_connection_string=azure_conn,
            azure_sas_token=azure_sas,
            azure_account_name=azure_acct,
        )
        if verbose:
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
                account_name=azure_acct,
            )

        dataset = load_multi_dataset_streaming(
            sources,
            schema_hint=schema_hint,
            chunk_size=settings.workload.chunk_size,
            azure_connection_string=azure_conn,
            azure_sas_token=azure_sas,
            azure_account_name=azure_acct,
        )
    else:
        # Single-file mode (original behavior)
        effective_dataset_path = Path(effective_dataset)

        n_rows_est = estimate_row_count(effective_dataset_path)
        streaming_threshold = settings.workload.streaming_threshold_rows

        if n_rows_est > streaming_threshold:
            if verbose:
                console.print(
                    f"[cyan]Loading dataset:[/cyan] {effective_dataset_path} "
                    f"[dim](~{n_rows_est:,} rows, streaming, chunk_size={settings.workload.chunk_size:,})[/dim]"
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
        if verbose:
            console.print(f"  Unit conversions applied: {dict(settings.workload.unit_conversions)}")
    elif settings.workload.unit_conversions and dataset.streaming:
        # Streaming mode: attach conversions for per-batch application
        dataset._unit_conversions = dict(settings.workload.unit_conversions)
        if verbose:
            console.print(f"  Unit conversions (streaming mode): {dict(settings.workload.unit_conversions)}")

    # Apply timestamp column override: CLI flag takes precedence over toml setting.
    effective_ts_col = timestamp_col or settings.workload.timestamp_col or ""
    if effective_ts_col:
        from tsbm.datasets.loader import override_timestamp_col
        dataset = override_timestamp_col(dataset, effective_ts_col)

    console.print(
        f"[cyan]Dataset:[/cyan] [bold]{dataset.schema.name}[/bold] — "
        f"{dataset.schema.row_count:,} rows"
        + (f" [dim](streaming)[/dim]" if getattr(dataset, 'streaming', False) else "")
    )
    if verbose:
        console.print(
            f"  ts=[bold]{dataset.schema.timestamp_col}[/bold], "
            f"tags={dataset.schema.tag_cols}, "
            f"metrics={dataset.schema.metric_cols}"
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

    db_list = ", ".join(a.name for a in adapters)
    if len(workload_names) > 1:
        console.print(
            f"[cyan]Benchmarks:[/cyan] {', '.join(workload_names)}  "
            f"[cyan]Databases:[/cyan] {db_list}"
        )
    else:
        console.print(
            f"[cyan]Benchmark:[/cyan] {workload_names[0]}  "
            f"[cyan]Databases:[/cyan] {db_list}"
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
    if skip_seed:
        console.print("[yellow]--skip-seed: skipping data seeding — assuming tables are populated[/yellow]")
        seeded_adapters.update(a.name for a in adapters)
    elif first_needs_seed:
        await _parallel_seed_adapters(
            adapters, dataset, seeded_adapters,
            seed_workers=settings.workload.seed_workers,
        )

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
            _print_summary_table(summaries, wl_name, verbose=verbose)
        console.print(f"[dim]Run IDs ({wl_name}): {', '.join(run_ids)}[/dim]")

    if verbose and len(workload_names) > 1:
        console.print(f"\n[dim]All run IDs: {', '.join(all_run_ids)}[/dim]")

    # Print the metric glossary only in verbose mode — it's 100+ lines.
    if verbose:
        console.print()
        _print_metric_glossary()


_CHECKPOINT_PATH = Path(".tsbm_seed_checkpoint.json")


def _load_checkpoint() -> dict[str, Any]:
    """Load the seed checkpoint file, or return an empty dict."""
    if _CHECKPOINT_PATH.exists():
        try:
            return json.loads(_CHECKPOINT_PATH.read_text(encoding="utf-8"))
        except Exception:
            return {}
    return {}


def _save_checkpoint(data: dict[str, Any]) -> None:
    """Atomically write the seed checkpoint file."""
    tmp = _CHECKPOINT_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, indent=2, default=str), encoding="utf-8")
    tmp.replace(_CHECKPOINT_PATH)


def _format_rate(rows_per_sec: float) -> str:
    """Format a rows/sec value into a human-readable string."""
    if rows_per_sec >= 1_000_000:
        return f"{rows_per_sec / 1_000_000:.2f}M rows/s"
    if rows_per_sec >= 1_000:
        return f"{rows_per_sec / 1_000:.1f}K rows/s"
    return f"{rows_per_sec:.0f} rows/s"


async def _seed_one_adapter(
    adapter: Any,
    dataset: Any,
    on_progress: Any | None = None,
    seed_workers: int = 1,
) -> None:
    """Connect to *adapter*, drop/create table, and ingest the full dataset.

    When *seed_workers* > 1, multiple adapter clones ingest batches concurrently
    via an ``asyncio.Queue``.  Each clone has its own DB connection / ILP sender.

    When the dataset has multiple source files (``source_paths``), a checkpoint
    file tracks which sources have been completed.  On restart, already-completed
    sources are skipped and the table is not dropped/recreated.

    *on_progress* is an optional callback ``(rows_ingested, rows_per_sec, source_idx, total_sources, filename)``
    called after each batch to report live throughput.
    """
    import time as _time  # noqa: PLC0415
    import pyarrow as pa  # noqa: PLC0415
    from tsbm.adapters.registry import get_adapter  # noqa: PLC0415
    from tsbm.datasets.loader import _iter_dataset_batches, _iter_azure_batches  # noqa: PLC0415

    await adapter.connect()
    try:
        table_name = dataset.schema.name
        has_sources = bool(getattr(dataset, "source_paths", None))

        # --- checkpoint resume logic ---
        checkpoint = _load_checkpoint()
        adapter_ckpt = checkpoint.get(adapter.name, {})
        resuming_sources = (
            has_sources
            and adapter_ckpt.get("table_name") == table_name
            and adapter_ckpt.get("completed_sources")
        )
        resuming_streaming = (
            not has_sources
            and dataset.streaming
            and adapter_ckpt.get("table_name") == table_name
            and adapter_ckpt.get("chunks_completed", 0) > 0
        )
        resuming = resuming_sources or resuming_streaming

        if resuming_sources:
            completed_set = set(adapter_ckpt["completed_sources"])
            rows_ingested = adapter_ckpt.get("rows_ingested", 0)
            logger.info(
                "Resuming %s seed: %d sources done, %d rows so far",
                adapter.name, len(completed_set), rows_ingested,
            )
        elif resuming_streaming:
            completed_set: set[str] = set()
            rows_ingested = adapter_ckpt.get("rows_ingested", 0)
            _resume_chunks = adapter_ckpt.get("chunks_completed", 0)
            logger.info(
                "Resuming %s seed: %d chunks done, %d rows so far",
                adapter.name, _resume_chunks, rows_ingested,
            )
        else:
            completed_set: set[str] = set()
            rows_ingested = 0
            _resume_chunks = 0
            await adapter.drop_table(table_name)
            await adapter.create_table(dataset.schema)
            checkpoint[adapter.name] = {
                "table_name": table_name,
                "completed_sources": [],
                "rows_ingested": 0,
                "chunks_completed": 0,
            }
            _save_checkpoint(checkpoint)

        seed_start = _time.monotonic()
        n_workers = max(1, seed_workers)

        _current_file = [""]

        def _report() -> None:
            if on_progress is not None:
                elapsed = _time.monotonic() - seed_start
                rate = rows_ingested / elapsed if elapsed > 0 else 0
                on_progress(rows_ingested, rate, _current_src[0], _total_src[0], _current_file[0])

        _current_src = [0]
        _total_src = [1]

        # --- WAL backpressure (QuestDB only) ---
        _WAL_LAG_THRESHOLD = 200  # pause when WAL lag exceeds this many txns
        _WAL_LAG_POLL = 0.5  # seconds between WAL lag checks while paused
        _has_wal_check = hasattr(adapter, "get_wal_lag")

        async def _wait_for_wal() -> None:
            """Block until QuestDB WAL lag drops below threshold."""
            if not _has_wal_check:
                return
            while True:
                lag = await adapter.get_wal_lag(table_name)
                if lag < _WAL_LAG_THRESHOLD:
                    return
                logger.info(
                    "QuestDB: WAL lag %d txns — pausing seeding until lag < %d",
                    lag, _WAL_LAG_THRESHOLD,
                )
                await asyncio.sleep(_WAL_LAG_POLL)

        # --- parallel worker infrastructure ---
        _SENTINEL = None  # signals workers to stop

        # Per-file batch tracking for parallel file reading
        _file_pending: dict[str, int] = {}          # source_key -> batches not yet consumed
        _file_done_producing: dict[str, bool] = {}   # source_key -> producer finished reading
        _file_events: dict[str, asyncio.Event] = {}  # source_key -> set when all batches consumed
        _tracking_lock = asyncio.Lock()

        async def _worker(worker_adapter: Any) -> None:
            """Pull batches from the queue and ingest them."""
            nonlocal rows_ingested
            while True:
                item = await queue.get()
                if item is _SENTINEL:
                    queue.task_done()
                    break
                source_key, tbl = item
                try:
                    result = await worker_adapter.ingest_batch(tbl, table_name)
                    rows_ingested += result.rows
                    _report()
                except Exception:
                    # Re-queue failed batch once, then let it propagate
                    logger.warning("%s: worker ingest failed, retrying batch", adapter.name)
                    try:
                        result = await worker_adapter.ingest_batch(tbl, table_name)
                        rows_ingested += result.rows
                        _report()
                    except Exception as exc2:
                        logger.error("%s: worker ingest retry failed: %s", adapter.name, exc2)
                        raise
                finally:
                    queue.task_done()
                    # Track per-file completion (only active for multi-source mode)
                    if source_key in _file_pending:
                        async with _tracking_lock:
                            _file_pending[source_key] -= 1
                            if _file_pending[source_key] == 0 and _file_done_producing.get(source_key):
                                _file_events[source_key].set()

        # Create worker adapter clones (each gets its own connection + sender)
        worker_adapters: list[Any] = []
        if n_workers > 1:
            for _ in range(n_workers):
                clone = get_adapter(adapter.name)
                await clone.connect()
                # Copy the schema so ingest_batch works
                if hasattr(adapter, "_schema"):
                    clone._schema = adapter._schema
                if hasattr(adapter, "_current_table"):
                    clone._current_table = adapter._current_table
                worker_adapters.append(clone)
        else:
            worker_adapters.append(adapter)

        queue: asyncio.Queue[tuple[str, pa.Table] | None] = asyncio.Queue(maxsize=n_workers * 4)

        # Start worker tasks
        worker_tasks = [asyncio.create_task(_worker(wa)) for wa in worker_adapters]

        try:
            if has_sources:
                _total_src[0] = len(dataset.source_paths)
                pending_sources = [
                    (src_idx, source)
                    for src_idx, source in enumerate(dataset.source_paths)
                    if str(source) not in completed_set
                ]
                _file_read_sem = asyncio.Semaphore(min(4, max(1, len(pending_sources))))

                # Pre-initialize tracking for all pending files so the checkpoint
                # coroutine can safely await events immediately.
                for _si, _src in pending_sources:
                    _sk = str(_src)
                    _file_pending[_sk] = 0
                    _file_done_producing[_sk] = False
                    _file_events[_sk] = asyncio.Event()

                async def _file_producer(src_idx: int, source: Any) -> None:
                    """Read batches from one source file and push them to the queue."""
                    source_key = str(source)

                    async with _file_read_sem:
                        if isinstance(source, Path):
                            batches = _iter_dataset_batches(source, dataset.chunk_size)
                        else:
                            batches = _iter_azure_batches(
                                source, dataset.chunk_size,
                                getattr(dataset, "_azure_connection_string", ""),
                                getattr(dataset, "_azure_sas_token", ""),
                                getattr(dataset, "_azure_account_name", ""),
                            )
                        for batch in batches:
                            tbl = pa.Table.from_batches([batch]) if not isinstance(batch, pa.Table) else batch
                            await _wait_for_wal()
                            async with _tracking_lock:
                                _file_pending[source_key] += 1
                            await queue.put((source_key, tbl))

                    async with _tracking_lock:
                        _file_done_producing[source_key] = True
                        if _file_pending[source_key] == 0:
                            _file_events[source_key].set()

                async def _checkpoint_files() -> None:
                    """Wait for each file's batches to be consumed, then checkpoint."""
                    import pyarrow.parquet as _pq  # noqa: PLC0415
                    _prev_rows = rows_ingested
                    for src_idx, source in pending_sources:
                        source_key = str(source)
                        _current_src[0] = src_idx + 1
                        _current_file[0] = Path(source_key).name if isinstance(source, Path) else source_key.rsplit("/", 1)[-1]
                        await _file_events[source_key].wait()

                        # Calculate rows contributed by this file
                        file_rows = rows_ingested - _prev_rows
                        _prev_rows = rows_ingested

                        # Try to get parquet metadata row count for display
                        pq_rows_str = ""
                        if isinstance(source, Path) and source.suffix in (".parquet", ".parq"):
                            try:
                                pf = _pq.ParquetFile(source)
                                pq_rows_str = f" [dim]({pf.metadata.num_rows:,} rows in file)[/dim]"
                            except Exception:
                                pass

                        checkpoint = _load_checkpoint()
                        ckpt = checkpoint.setdefault(adapter.name, {
                            "table_name": table_name,
                            "completed_sources": [],
                            "rows_ingested": 0,
                            "chunks_completed": 0,
                        })
                        ckpt["completed_sources"].append(source_key)
                        ckpt["rows_ingested"] = rows_ingested
                        _save_checkpoint(checkpoint)
                        _display_src = _current_file[0] if isinstance(source, Path) else source_key
                        console.print(
                            f"  [green]done[/green] [cyan]{adapter.name}[/cyan] "
                            f"file {src_idx + 1}/{_total_src[0]}: [bold]{_display_src}[/bold] "
                            f"— {file_rows:,} rows{pq_rows_str}"
                        )

                # Launch file producers concurrently + checkpoint coroutine
                producer_tasks = [
                    asyncio.create_task(_file_producer(src_idx, source))
                    for src_idx, source in pending_sources
                ]
                checkpoint_task = asyncio.create_task(_checkpoint_files())
                await asyncio.gather(*producer_tasks)
                await checkpoint_task
            elif dataset.streaming:
                _chunk_idx = 0
                _skip_chunks = _resume_chunks if resuming_streaming else 0
                _stream_key = "__streaming__"
                for chunk in dataset.iter_batches():
                    _chunk_idx += 1
                    if _chunk_idx <= _skip_chunks:
                        continue
                    await _wait_for_wal()
                    await queue.put((_stream_key, chunk))

                    # Checkpoint after each chunk is fully queued and processed
                    await queue.join()
                    checkpoint = _load_checkpoint()
                    ckpt = checkpoint.setdefault(adapter.name, {
                        "table_name": table_name,
                        "completed_sources": [],
                        "rows_ingested": 0,
                        "chunks_completed": 0,
                    })
                    ckpt["chunks_completed"] = _chunk_idx
                    ckpt["rows_ingested"] = rows_ingested
                    _save_checkpoint(checkpoint)
                    console.print(
                        f"  [green]checkpoint[/green] [cyan]{adapter.name}[/cyan] "
                        f"chunk {_chunk_idx}: ({rows_ingested:,} rows total)"
                    )
            else:
                tbl = dataset.load()
                await queue.put(("__eager__", tbl))

            # Wait for remaining batches
            await queue.join()
        finally:
            # Signal all workers to stop
            for _ in worker_tasks:
                await queue.put(_SENTINEL)
            await asyncio.gather(*worker_tasks, return_exceptions=True)

            # Disconnect worker clones (not the main adapter)
            if n_workers > 1:
                for wa in worker_adapters:
                    try:
                        await wa.disconnect()
                    except Exception:
                        pass

        await adapter.flush(expected_rows=rows_ingested)
    finally:
        await adapter.disconnect()


async def _parallel_seed_adapters(
    adapters: list[Any],
    dataset: Any,
    seeded_adapters: set[str],
    seed_workers: int = 1,
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
            def _on_progress(rows: int, rate: float, src_idx: int, total_src: int, filename: str = "") -> None:
                rate_str = _format_rate(rate)
                if total_src > 1:
                    progress.update(
                        tasks_map[adapter.name],
                        description=(
                            f"[cyan]{adapter.name}[/cyan] "
                            f"[dim]{rows:,} rows — {rate_str} — {filename} ({src_idx}/{total_src})[/dim]"
                        ),
                    )
                else:
                    progress.update(
                        tasks_map[adapter.name],
                        description=(
                            f"[cyan]{adapter.name}[/cyan] "
                            f"[dim]{rows:,} rows — {rate_str}[/dim]"
                        ),
                    )

            try:
                # Per-adapter seed_workers override (e.g. QuestDB benefits from fewer workers)
                from tsbm.config.settings import get_settings as _get_settings  # noqa: PLC0415
                _db_cfg = getattr(_get_settings().databases, adapter.name, None)
                _adapter_workers = getattr(_db_cfg, "seed_workers", 0) if _db_cfg else 0
                _effective_workers = _adapter_workers if _adapter_workers > 0 else seed_workers

                await _seed_one_adapter(adapter, dataset, on_progress=_on_progress, seed_workers=_effective_workers)
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
                        import time as _time  # noqa: PLC0415
                        progress.update(db_task, description=f"{task_desc} [dim]loading data…[/dim]")
                        rows_ingested = 0
                        _seed_t0 = _time.monotonic()
                        if dataset.streaming:
                            for _chunk in dataset.iter_batches():
                                await adapter.ingest_batch(_chunk, dataset.schema.name)
                                rows_ingested += len(_chunk)
                                _elapsed = _time.monotonic() - _seed_t0
                                _rate = rows_ingested / _elapsed if _elapsed > 0 else 0
                                progress.update(
                                    db_task,
                                    description=f"{task_desc} [dim]{rows_ingested:,} rows — {_format_rate(_rate)}[/dim]",
                                )
                        else:
                            _tbl = dataset.load()
                            await adapter.ingest_batch(_tbl, dataset.schema.name)
                            rows_ingested = len(_tbl)
                        await adapter.flush(expected_rows=rows_ingested)
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
    n_parts: int = 1,
    chunk_rows: int | None = None,
) -> None:
    """Generate synthetic IoT data and write to *output_path*."""
    from tsbm.datasets.generator import generate_iot_dataset, _DEFAULT_CHUNK_ROWS
    import pyarrow.parquet as pq

    effective_chunk = chunk_rows or _DEFAULT_CHUNK_ROWS
    n_total = n_devices * n_readings
    parts_label = f", {n_parts} part-files" if n_parts > 1 else ""
    console.print(
        f"Generating [bold]{n_total:,}[/bold] rows "
        f"({n_devices} devices × {n_readings} readings/device, seed={seed}{parts_label})…"
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    suffix = output_path.suffix.lower()

    if suffix == ".csv":
        from tsbm.datasets.generator import generate_to_csv
        generate_to_csv(
            path=output_path,
            n_devices=n_devices,
            n_readings_per_device=n_readings,
            seed=seed,
        )
        console.print(f"[green]Written:[/green] {output_path}  ({output_path.stat().st_size // 1024:,} KB)")
        return

    # For large datasets (> chunk threshold), use lazy/streaming generation to
    # avoid loading all rows into memory.  This is critical for billion-row
    # datasets where eager mode would exhaust RAM.
    use_lazy = n_total > effective_chunk
    bds = generate_iot_dataset(
        n_devices=n_devices,
        n_readings_per_device=n_readings,
        seed=seed,
        name=output_path.stem,
        lazy=use_lazy,
        output_path=output_path if use_lazy else None,
        n_parts=n_parts,
        chunk_rows=chunk_rows,
    )

    if not use_lazy:
        # Small dataset: write the in-memory table to disk
        tbl = bds.load()
        if suffix in (".parquet", ".parq"):
            pq.write_table(tbl, output_path, compression="snappy")
        else:
            pq.write_table(tbl, output_path.with_suffix(".parquet"), compression="snappy")
            output_path = output_path.with_suffix(".parquet")

    if n_parts > 1:
        parts_dir = output_path.with_suffix("")
        console.print(f"[green]Written:[/green] {parts_dir}/  ({n_parts} part-files, {n_total:,} rows)")
    else:
        console.print(f"[green]Written:[/green] {output_path}  ({output_path.stat().st_size // 1024:,} KB)")


# ---------------------------------------------------------------------------
# load
# ---------------------------------------------------------------------------


async def async_seed(
    db_names: list[str] | None = None,
    config_path: Path | None = None,
    dataset_path: Path | None = None,
    dataset_list: list[str] | None = None,
    timestamp_col: str | None = None,
    drop_first: bool = True,
    seed_workers: int = 0,
    verbose: bool = False,
) -> None:
    """Seed one or more databases with the full dataset — no benchmarks.

    Uses the same parallel-worker + WAL-backpressure infrastructure as
    ``tsbm run`` but skips all benchmark workloads.
    Displays live rows/sec throughput.
    """
    import time as _time  # noqa: PLC0415

    # ── config ──────────────────────────────────────────────────────────
    if config_path is not None:
        settings = load_settings_from_file(config_path)
    else:
        settings = get_settings()

    if seed_workers <= 0:
        seed_workers = getattr(settings.workload, "seed_workers", 1)

    # CLI --dataset (multiple) overrides config datasets list
    if dataset_list:
        settings.workload.datasets = dataset_list

    effective_dataset = dataset_path or settings.workload.dataset

    # ── dataset ─────────────────────────────────────────────────────────
    azure_conn = settings.workload.azure_storage_connection_string
    azure_sas = settings.workload.azure_storage_sas_token
    azure_acct = settings.workload.azure_storage_account_name
    use_multi = bool(settings.workload.datasets) and not dataset_path

    if use_multi:
        sources = resolve_dataset_sources(
            settings.workload.datasets,
            azure_connection_string=azure_conn,
            azure_sas_token=azure_sas,
            azure_account_name=azure_acct,
        )
        if verbose:
            console.print(f"[cyan]Loading {len(sources)} dataset source(s):[/cyan]")
            for src in sources:
                console.print(f"  [dim]{src}[/dim]")

        from tsbm.datasets.loader import _is_azure_url  # noqa: PLC0415
        first_local = next((s for s in sources if isinstance(s, Path)), None)
        if first_local:
            schema_hint = infer_schema_from_sample(
                first_local,
                tag_cardinality_threshold=settings.workload.tag_cardinality_threshold,
            )
        else:
            schema_hint = infer_schema_from_azure(
                str(sources[0]),
                tag_cardinality_threshold=settings.workload.tag_cardinality_threshold,
                connection_string=azure_conn,
                sas_token=azure_sas,
                account_name=azure_acct,
            )
        dataset = load_multi_dataset_streaming(
            sources,
            schema_hint=schema_hint,
            chunk_size=settings.workload.chunk_size,
            azure_connection_string=azure_conn,
            azure_sas_token=azure_sas,
            azure_account_name=azure_acct,
        )
    else:
        effective_dataset_path = Path(effective_dataset)
        n_rows_est = estimate_row_count(effective_dataset_path)
        streaming_threshold = settings.workload.streaming_threshold_rows

        if n_rows_est > streaming_threshold:
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

    # Apply unit conversions
    if settings.workload.unit_conversions and dataset.table is not None:
        dataset.table = apply_unit_conversions(dataset.table, dict(settings.workload.unit_conversions))
    elif settings.workload.unit_conversions and dataset.streaming:
        dataset._unit_conversions = dict(settings.workload.unit_conversions)

    # Timestamp column override
    effective_ts_col = timestamp_col or settings.workload.timestamp_col or ""
    if effective_ts_col:
        from tsbm.datasets.loader import override_timestamp_col  # noqa: PLC0415
        dataset = override_timestamp_col(dataset, effective_ts_col)

    console.print(
        f"[cyan]Dataset:[/cyan] [bold]{dataset.schema.name}[/bold] — "
        f"{dataset.schema.row_count:,} rows"
        + (f" [dim](streaming)[/dim]" if getattr(dataset, 'streaming', False) else "")
    )

    # ── adapters ────────────────────────────────────────────────────────
    adapters = get_enabled_adapters(db_names)
    if not adapters:
        console.print("[red]No adapters available. Check your config and --db flag.[/red]")
        return

    adapter_names = ", ".join(a.name for a in adapters)
    console.print(
        f"[cyan]Seeding:[/cyan] [bold]{adapter_names}[/bold] "
        f"(workers={seed_workers}, WAL throttle enabled)"
    )

    # ── per-file row counts (for parquet sources) ─────────────────────
    import pyarrow.parquet as _pq  # noqa: PLC0415
    file_row_counts: list[tuple[str, int]] = []
    has_sources = bool(getattr(dataset, "source_paths", None))
    if has_sources:
        for src in dataset.source_paths:
            if isinstance(src, Path) and src.suffix in (".parquet", ".parq"):
                try:
                    pf = _pq.ParquetFile(src)
                    file_row_counts.append((src.name, pf.metadata.num_rows))
                except Exception:
                    file_row_counts.append((src.name, -1))
            elif isinstance(src, Path):
                file_row_counts.append((src.name, -1))
    elif not getattr(dataset, "streaming", False):
        # Single file — try to get row count from parquet metadata
        eff_path = Path(effective_dataset) if effective_dataset else None
        if eff_path and eff_path.suffix in (".parquet", ".parq"):
            try:
                pf = _pq.ParquetFile(eff_path)
                file_row_counts.append((eff_path.name, pf.metadata.num_rows))
            except Exception:
                pass

    for adapter in adapters:
        seed_start = _time.monotonic()
        seeded: set[str] = set()

        console.print(f"\n[cyan]{'─' * 40}[/cyan]")
        console.print(f"[cyan]Database:[/cyan] [bold]{adapter.name}[/bold]")

        with Progress(
            SpinnerColumn(),
            TextColumn("[progress.description]{task.description}"),
            BarColumn(),
            MofNCompleteColumn(),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            tid = progress.add_task(f"[cyan]{adapter.name}[/cyan] [dim]starting…[/dim]", total=None)

            def _on_progress(rows: int, rate: float, src_idx: int, total_src: int, filename: str = "", _adapter=adapter, _tid=tid) -> None:
                rate_str = _format_rate(rate)
                if total_src > 1:
                    progress.update(
                        _tid,
                        description=(
                            f"[cyan]{_adapter.name}[/cyan] "
                            f"[dim]{rows:,} rows — {rate_str} — {filename} ({src_idx}/{total_src})[/dim]"
                        ),
                    )
                else:
                    progress.update(
                        _tid,
                        description=(
                            f"[cyan]{_adapter.name}[/cyan] "
                            f"[dim]{rows:,} rows — {rate_str}[/dim]"
                        ),
                    )

            # Track per-callback rates for min/max/avg calculation
            _rates: list[float] = []

            def _on_progress_tracking(rows: int, rate: float, src_idx: int, total_src: int, filename: str = "") -> None:
                if rate > 0:
                    _rates.append(rate)
                _on_progress(rows, rate, src_idx, total_src, filename)

            try:
                await _seed_one_adapter(
                    adapter, dataset,
                    on_progress=_on_progress_tracking,
                    seed_workers=seed_workers,
                )
                seeded.add(adapter.name)
                elapsed = _time.monotonic() - seed_start
                final_count = await _get_final_count(adapter, dataset.schema.name)
                overall_rate = final_count / elapsed if elapsed > 0 else 0
                progress.update(
                    tid,
                    description=(
                        f"[cyan]{adapter.name}[/cyan] [green]seeded[/green] "
                        f"[dim]{final_count:,} rows in {elapsed:.1f}s — {_format_rate(overall_rate)}[/dim]"
                    ),
                    completed=True,
                )
            except Exception as exc:
                elapsed = _time.monotonic() - seed_start
                final_count = 0
                overall_rate = 0.0
                logger.exception("Failed to seed %s", adapter.name)
                console.print(f"[red][{adapter.name}] Seed error: {exc}[/red]")
                progress.update(
                    tid,
                    description=f"[cyan]{adapter.name}[/cyan] [red]failed[/red]",
                    completed=True,
                )

        if seeded:
            # ── summary table ───────────────────────────────────────────
            min_rate = min(_rates) if _rates else 0.0
            max_rate = max(_rates) if _rates else 0.0
            avg_rate = sum(_rates) / len(_rates) if _rates else 0.0

            # Format elapsed into human-readable duration
            mins, secs = divmod(elapsed, 60)
            hrs, mins = divmod(int(mins), 60)
            if hrs:
                duration_str = f"{hrs}h {mins}m {secs:.1f}s"
            elif mins:
                duration_str = f"{int(mins)}m {secs:.1f}s"
            else:
                duration_str = f"{secs:.1f}s"

            summary = Table(
                title=f"seed summary — {adapter.name}",
                title_style="bold green",
                border_style="green",
                show_header=False,
                padding=(0, 2),
            )
            summary.add_column("Metric", style="bold")
            summary.add_column("Value", justify="right")

            summary.add_row("Table", f"[bold]{dataset.schema.name}[/bold]")
            summary.add_row("Total rows", f"[bold]{final_count:,}[/bold]")
            summary.add_row("Duration", duration_str)
            summary.add_row("Workers", str(seed_workers))
            summary.add_row("", "")
            summary.add_row("Avg rows/sec", f"[bold]{_format_rate(avg_rate)}[/bold]")
            summary.add_row("Min rows/sec", _format_rate(min_rate))
            summary.add_row("Max rows/sec", _format_rate(max_rate))
            summary.add_row("Overall rows/sec", f"[bold green]{_format_rate(overall_rate)}[/bold green]")

            console.print()
            console.print(summary)

    # ── per-file row counts (printed once after all adapters) ─────────
    if file_row_counts:
        file_table = Table(
            title="files",
            title_style="bold cyan",
            border_style="cyan",
            padding=(0, 2),
        )
        file_table.add_column("File", style="bold")
        file_table.add_column("Rows", justify="right")
        for fname, rc in file_row_counts:
            rc_str = f"{rc:,}" if rc >= 0 else "[dim]unknown[/dim]"
            file_table.add_row(fname, rc_str)
        console.print()
        console.print(file_table)


async def _get_final_count(adapter: Any, table_name: str) -> int:
    """Reconnect (if needed) and return the row count for *table_name*."""
    try:
        await adapter.connect()
        return await adapter.get_row_count(table_name)
    except Exception:
        return 0
    finally:
        try:
            await adapter.disconnect()
        except Exception:
            pass


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
                    verdicts[idx] = "[green]Great[/green]"
                elif rps >= 100_000:
                    verdicts[idx] = "[green]Solid[/green]"
                elif rps >= 20_000:
                    verdicts[idx] = "[yellow]Fair[/yellow]"
                else:
                    verdicts[idx] = "[red]Slow[/red]"
            else:
                p99 = row.get("latency_p99_ms", 0) or 0
                op = row.get("operation", "")
                great_ms, solid_ms, fair_ms = _get_latency_thresholds(op)
                if p99 <= great_ms:
                    verdicts[idx] = "[green]Great[/green]"
                elif p99 <= solid_ms:
                    verdicts[idx] = "[green]Solid[/green]"
                elif p99 <= fair_ms:
                    verdicts[idx] = "[yellow]Fair[/yellow]"
                else:
                    verdicts[idx] = "[red]Slow[/red]"
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
                    verdicts[idx] = "[green]Fastest[/green]"
                elif rank == len(ranked) - 1:
                    verdicts[idx] = "[red]Slowest[/red]"
                else:
                    verdicts[idx] = "[yellow]Middle[/yellow]"

    return verdicts


def _print_summary_table(summaries: list[dict[str, Any]], benchmark_name: str, *, verbose: bool = False) -> None:
    # Description panel — only in verbose mode (benchmark name is in the table title)
    if verbose:
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

    if verbose:
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
                label = "Great" if best_rps >= 500_000 else "Solid" if best_rps >= 100_000 else "Fair" if best_rps >= 20_000 else "Slow"
                lines.append(f"{icon} Throughput: [bold]{best_rps:,.0f}[/bold] rows/sec — {label}.")
        else:
            best = min(valid, key=lambda r: r.get("latency_p99_ms", float("inf")) or float("inf"))
            worst = max(valid, key=lambda r: r.get("latency_p99_ms", 0) or 0)
            best_p99 = best.get("latency_p99_ms", 0) or 0
            worst_p99 = worst.get("latency_p99_ms", 0) or 0
            op = rows[0].get("operation", "")
            great_ms, solid_ms, fair_ms = _get_latency_thresholds(op)

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
                    "[green]↓[/green]" if best_p99 <= solid_ms
                    else "[yellow]~[/yellow]" if best_p99 <= fair_ms
                    else "[red]↑[/red]"
                )
                label = (
                    "Great" if best_p99 <= great_ms
                    else "Solid" if best_p99 <= solid_ms
                    else "Fair" if best_p99 <= fair_ms
                    else "Slow"
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

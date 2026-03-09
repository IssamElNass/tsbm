"""
Ingestion benchmarks.

IngestionBenchmark
    Loops over ``config.batch_sizes × config.workers``.
    Warmup iterations reset the table each time and results are recorded
    but excluded from BenchmarkSummary.  Measurement rounds optionally reset
    between rounds (``config.reset_between_rounds``).  Multi-worker ingestion
    splits the batch into equal sub-batches and runs them with asyncio.gather().

OutOfOrderIngestionBenchmark
    Same as IngestionBenchmark but shuffles each batch's timestamps before
    inserting, simulating out-of-order time-series writes.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Any

import numpy as np
import pyarrow as pa

from tsbm.benchmarks.base import BenchmarkResult
from tsbm.datasets.schema import DatasetSchema
from tsbm.metrics.stats import compute_stats, compute_throughput_stats
from tsbm.metrics.timer import TimingResult, estimate_table_bytes, timed_operation
from tsbm.results.models import BenchmarkSummary, OperationResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# IngestionBenchmark
# ---------------------------------------------------------------------------


class IngestionBenchmark:
    """
    Measures raw write throughput at multiple batch sizes and worker counts.
    """

    name = "ingestion"
    description = "Measures ingestion throughput (rows/sec) at configurable batch sizes and worker counts."

    async def run(
        self,
        adapter: Any,
        dataset: Any,
        config: Any,
        run_id: str,
    ) -> BenchmarkResult:
        result = BenchmarkResult(
            run_id=run_id,
            database_name=adapter.name,
            benchmark_name=self.name,
        )

        table_name = dataset.schema.name

        if dataset.streaming:
            await self._run_streaming(adapter, dataset, config, run_id, result)
        else:
            table = dataset.load()
            await self._run_eager(adapter, table, dataset.schema, table_name, config, run_id, result)

        return result

    async def _run_eager(
        self,
        adapter: Any,
        table: pa.Table,
        schema: Any,
        table_name: str,
        config: Any,
        run_id: str,
        result: BenchmarkResult,
    ) -> None:
        """Original eager path: full table is in memory."""
        for batch_size in config.batch_sizes:
            for workers in config.workers:
                # ---- Warmup ----
                # Table is already pre-seeded by the orchestrator; warmup
                # inserts additional data on top (no drop/create).
                for i in range(config.warmup_iterations):
                    try:
                        timing = await self._run_batch(
                            adapter, table, table_name, batch_size, workers
                        )
                        result.operation_results.append(
                            OperationResult.from_timing(
                                run_id=run_id,
                                database_name=adapter.name,
                                operation=self.name,
                                phase="warmup",
                                iteration=i,
                                timing=timing,
                                batch_size=batch_size,
                                workers=workers,
                            )
                        )
                    except Exception as exc:
                        logger.warning(
                            "Warmup error (db=%s batch=%d workers=%d iter=%d): %s",
                            adapter.name, batch_size, workers, i, exc,
                        )

                # ---- Measurement ----
                measurement_timings: list[TimingResult] = []
                for i in range(config.measurement_rounds):
                    try:
                        if config.reset_between_rounds:
                            await adapter.drop_table(table_name)
                            await adapter.create_table(schema)
                        timing = await self._run_batch(
                            adapter, table, table_name, batch_size, workers
                        )
                        measurement_timings.append(timing)
                        result.operation_results.append(
                            OperationResult.from_timing(
                                run_id=run_id,
                                database_name=adapter.name,
                                operation=self.name,
                                phase="measurement",
                                iteration=i,
                                timing=timing,
                                batch_size=batch_size,
                                workers=workers,
                            )
                        )
                    except Exception as exc:
                        msg = (
                            f"Measurement error (db={adapter.name} batch={batch_size} "
                            f"workers={workers} iter={i}): {exc}"
                        )
                        logger.error(msg)
                        result.errors.append(msg)

                if measurement_timings:
                    summary = _build_summary(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=self.name,
                        batch_size=batch_size,
                        workers=workers,
                        timings=measurement_timings,
                    )
                    result.summaries.append(summary)

    async def _run_streaming(
        self,
        adapter: Any,
        dataset: Any,
        config: Any,
        run_id: str,
        result: BenchmarkResult,
    ) -> None:
        """
        Streaming path for 1M+ row datasets.

        Iterates over disk-based chunks rather than loading all data into memory.
        Each full pass over the dataset counts as one benchmark iteration.
        Supports multi-worker parallelism: each chunk is split across workers
        just like the eager path.
        """
        table_name = dataset.schema.name
        workers_list = config.workers if config.workers else [1]

        for workers in workers_list:
            # Warmup: one full pass per warmup iteration.
            # Table is already pre-seeded by the orchestrator; warmup
            # inserts additional data on top (no drop/create).
            for i in range(config.warmup_iterations):
                try:
                    pass_timings: list[TimingResult] = []
                    for chunk in dataset.iter_batches():
                        t = await self._run_batch(adapter, chunk, table_name, len(chunk), workers)
                        pass_timings.append(t)
                    if pass_timings:
                        agg = _aggregate_timings(pass_timings)
                        result.operation_results.append(
                            OperationResult.from_timing(
                                run_id=run_id,
                                database_name=adapter.name,
                                operation=self.name,
                                phase="warmup",
                                iteration=i,
                                timing=agg,
                                batch_size=dataset.chunk_size,
                                workers=workers,
                            )
                        )
                except Exception as exc:
                    logger.warning("Streaming warmup error (workers=%d iter=%d): %s", workers, i, exc)

            # Measurement: one full pass per measurement round
            measurement_timings: list[TimingResult] = []
            for i in range(config.measurement_rounds):
                try:
                    if config.reset_between_rounds:
                        await adapter.drop_table(table_name)
                        await adapter.create_table(dataset.schema)
                    pass_timings = []
                    for chunk in dataset.iter_batches():
                        t = await self._run_batch(adapter, chunk, table_name, len(chunk), workers)
                        pass_timings.append(t)
                    if pass_timings:
                        agg = _aggregate_timings(pass_timings)
                        measurement_timings.append(agg)
                        result.operation_results.append(
                            OperationResult.from_timing(
                                run_id=run_id,
                                database_name=adapter.name,
                                operation=self.name,
                                phase="measurement",
                                iteration=i,
                                timing=agg,
                                batch_size=dataset.chunk_size,
                                workers=workers,
                            )
                        )
                except Exception as exc:
                    msg = f"Streaming measurement error (workers={workers} iter={i}): {exc}"
                    logger.error(msg)
                    result.errors.append(msg)

            if measurement_timings:
                result.summaries.append(
                    _build_summary(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=self.name,
                        batch_size=dataset.chunk_size,
                        workers=workers,
                        timings=measurement_timings,
                    )
                )

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        return []

    async def _run_batch(
        self,
        adapter: Any,
        table: pa.Table,
        table_name: str,
        batch_size: int,
        workers: int,
    ) -> TimingResult:
        """
        Ingest one batch of *batch_size* rows using *workers* concurrent tasks.

        If workers > 1 the batch is split into equal sub-batches and run with
        asyncio.gather(); the outer timed_operation wraps the entire gather
        call so end-to-end latency is captured correctly.

        Timestamp columns are pre-cast to microsecond precision once on the
        full batch so sub-batches inherit the cast and adapters skip redundant
        per-element conversion.
        """
        batch = table.slice(0, min(batch_size, len(table)))
        batch = _precast_timestamps(batch, adapter)
        byte_count = estimate_table_bytes(batch)

        if workers <= 1:
            with timed_operation(rows=len(batch), bytes_count=byte_count, disable_gc=True) as t:
                await adapter.ingest_batch(batch, table_name)
            return t

        # Split into sub-batches for concurrent workers
        sub_size = max(1, len(batch) // workers)
        sub_batches = [
            batch.slice(i * sub_size, min(sub_size, len(batch) - i * sub_size))
            for i in range(workers)
            if i * sub_size < len(batch)
        ]

        with timed_operation(
            rows=len(batch), bytes_count=byte_count, disable_gc=True
        ) as t:
            await asyncio.gather(
                *[adapter.ingest_batch(sub, table_name) for sub in sub_batches]
            )
        return t


# ---------------------------------------------------------------------------
# OutOfOrderIngestionBenchmark
# ---------------------------------------------------------------------------


class OutOfOrderIngestionBenchmark(IngestionBenchmark):
    """
    Like IngestionBenchmark but shuffles each batch's timestamp column before
    inserting, simulating late-arriving or out-of-order time-series data.
    """

    name = "ingestion_out_of_order"
    description = (
        "Measures ingestion throughput with randomly shuffled timestamps, "
        "simulating out-of-order writes."
    )

    async def _run_batch(
        self,
        adapter: Any,
        table: pa.Table,
        table_name: str,
        batch_size: int,
        workers: int,
    ) -> TimingResult:
        batch = table.slice(0, min(batch_size, len(table)))
        batch = _shuffle_timestamps(batch, adapter)
        # Delegate to parent with the shuffled table
        # Temporarily patch table reference for the super call
        byte_count = estimate_table_bytes(batch)

        if workers <= 1:
            with timed_operation(rows=len(batch), bytes_count=byte_count, disable_gc=True) as t:
                await adapter.ingest_batch(batch, table_name)
            return t

        sub_size = max(1, len(batch) // workers)
        sub_batches = [
            batch.slice(i * sub_size, min(sub_size, len(batch) - i * sub_size))
            for i in range(workers)
            if i * sub_size < len(batch)
        ]
        with timed_operation(rows=len(batch), bytes_count=byte_count, disable_gc=True) as t:
            await asyncio.gather(
                *[adapter.ingest_batch(sub, table_name) for sub in sub_batches]
            )
        return t


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _aggregate_timings(timings: list[TimingResult]) -> TimingResult:
    """Combine per-chunk timings into a single aggregate timing for a full pass."""
    return TimingResult(
        elapsed_ns=sum(t.elapsed_ns for t in timings),
        rows_processed=sum(t.rows_processed for t in timings),
        bytes_processed=sum(t.bytes_processed for t in timings),
    )


def _precast_timestamps(table: pa.Table, adapter: Any) -> pa.Table:
    """
    Pre-cast timestamp columns to microsecond precision (``us``) so that
    downstream adapter conversion is a no-op.  This is done once on the
    full batch before splitting into sub-batches for concurrent workers.
    """
    schema = getattr(adapter, "_schema", None)
    if schema is None:
        return table
    ts_col = schema.timestamp_col
    if ts_col not in table.schema.names:
        return table

    col = table.column(ts_col)
    target = pa.timestamp("us", tz="UTC")
    # Only cast if not already in the target type
    if col.type != target:
        cast_col = col.cast(target)
        col_idx = table.schema.get_field_index(ts_col)
        return table.set_column(col_idx, pa.field(ts_col, target), cast_col)
    return table


def _shuffle_timestamps(table: pa.Table, adapter: Any) -> pa.Table:
    """
    Return a copy of *table* with the timestamp column randomly permuted.

    Uses a deterministic seed derived from the table's row count to keep
    shuffles reproducible within a single run.
    """
    # Locate timestamp column name from stored schema (set during create_table)
    schema = getattr(adapter, "_schema", None)
    if schema is None:
        return table
    ts_col = schema.timestamp_col
    if ts_col not in table.schema.names:
        return table

    rng = np.random.default_rng(seed=len(table))
    idx = rng.permutation(len(table))
    shuffled_ts = table.column(ts_col).take(idx)
    col_idx = table.schema.get_field_index(ts_col)
    return table.set_column(col_idx, ts_col, shuffled_ts)


def _build_summary(
    run_id: str,
    database_name: str,
    operation: str,
    batch_size: int,
    workers: int,
    timings: list[TimingResult],
) -> BenchmarkSummary:
    latency_ns = [t.elapsed_ns for t in timings]
    latency_stats = compute_stats(latency_ns)
    throughput = compute_throughput_stats(timings)
    total_rows = sum(t.rows_processed for t in timings)

    return BenchmarkSummary(
        run_id=run_id,
        database_name=database_name,
        operation=operation,
        batch_size=batch_size,
        workers=workers,
        total_rows=total_rows,
        sample_count=len(timings),
        latency=latency_stats,
        rows_per_second_mean=throughput["rows_per_second_mean"],
        rows_per_second_p95=throughput["rows_per_second_p95"],
        rows_per_second_p99=throughput["rows_per_second_p99"],
        mb_per_second_mean=throughput["mb_per_second_mean"],
    )

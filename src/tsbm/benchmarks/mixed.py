"""
MixedBenchmark — concurrent read + write workload.

Architecture
------------
A single writer coroutine and *N* reader coroutines run simultaneously for
``config.mixed_duration_seconds`` seconds.

  * Writer  — continuously generates small batches and ingests them via the
              adapter.  Notifies the queue when fresh data is available.
  * Readers — dequeue work items and run a random query from the query pool.

An ``asyncio.Queue(maxsize=10)`` is used as a backpressure mechanism.

GC note
-------
``disable_gc=False`` in all timed_operation calls inside this benchmark
because the long-running nature of the workload makes GC pauses unavoidable
and disabling GC would cause unbounded memory growth.
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import numpy as np

from tsbm.benchmarks.base import BenchmarkResult
from tsbm.benchmarks.ingestion import _build_summary
from tsbm.benchmarks.queries import (
    AggregationBenchmark,
    LastPointBenchmark,
    TimeRangeBenchmark,
)
from tsbm.datasets.schema import DatasetSchema
from tsbm.metrics.timer import TimingResult, estimate_table_bytes, timed_operation
from tsbm.results.models import OperationResult

logger = logging.getLogger(__name__)

# Sentinel object to signal readers to stop
_STOP = object()


class MixedBenchmark:
    """
    Concurrent read + write benchmark.

    Writer ingests small batches continuously; N reader coroutines run
    queries concurrently.  Both run for ``config.mixed_duration_seconds``.
    """

    name = "mixed"
    description = (
        "Concurrent read/write workload: writer ingests continuously while "
        "reader coroutines run queries for a fixed duration."
    )

    # Number of concurrent reader coroutines
    _NUM_READERS = 4
    # Batch size for writer (small to keep latency low)
    _WRITE_BATCH_SIZE = 1000
    # Queue depth limit (backpressure)
    _QUEUE_MAXSIZE = 10

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

        table = dataset.load()
        schema = dataset.schema
        table_name = schema.name
        duration_s = float(config.mixed_duration_seconds)

        # Pre-build query templates for this adapter
        query_pool = self._build_query_pool(schema, adapter.name, table, config)
        if not query_pool:
            result.errors.append(
                f"No queries available for adapter '{adapter.name}'; skipping mixed benchmark."
            )
            return result

        # Shared queue and stop event
        queue: asyncio.Queue = asyncio.Queue(maxsize=self._QUEUE_MAXSIZE)
        stop_event = asyncio.Event()

        write_timings: list[TimingResult] = []
        read_timings: list[TimingResult] = []
        errors: list[str] = []

        async def _writer() -> None:
            row_offset = 0
            table_len = len(table)
            while not stop_event.is_set():
                batch_end = min(row_offset + self._WRITE_BATCH_SIZE, table_len)
                batch = table.slice(row_offset, batch_end - row_offset)
                row_offset = batch_end % table_len  # wrap around

                try:
                    with timed_operation(
                        rows=len(batch),
                        bytes_count=estimate_table_bytes(batch),
                        disable_gc=False,
                    ) as t:
                        await adapter.ingest_batch(batch, table_name)
                    write_timings.append(t)
                    # Notify readers that new data is available
                    try:
                        queue.put_nowait("new_data")
                    except asyncio.QueueFull:
                        pass  # readers are busy; drop notification
                except Exception as exc:
                    errors.append(f"Writer error: {exc}")
                    await asyncio.sleep(0.1)

        async def _reader(reader_id: int) -> None:
            rng = np.random.default_rng(config.prng_seed + reader_id)
            while not stop_event.is_set():
                # Wait for a notification (or timeout to avoid busy-spin)
                try:
                    await asyncio.wait_for(queue.get(), timeout=0.5)
                    queue.task_done()
                except asyncio.TimeoutError:
                    if stop_event.is_set():
                        break
                    continue

                # Pick a random query
                sql, params = query_pool[int(rng.integers(len(query_pool)))]
                try:
                    with timed_operation(rows=0, disable_gc=False) as t:
                        _, _ = await adapter.execute_query(sql, params)
                    read_timings.append(t)
                except Exception as exc:
                    errors.append(f"Reader {reader_id} error: {exc}")

        # Run writer + readers concurrently for duration_s
        async def _stopper() -> None:
            await asyncio.sleep(duration_s)
            stop_event.set()

        tasks = [
            asyncio.create_task(_writer()),
            asyncio.create_task(_stopper()),
            *[asyncio.create_task(_reader(i)) for i in range(self._NUM_READERS)],
        ]

        await asyncio.gather(*tasks, return_exceptions=True)

        # Record all timings as OperationResult
        for i, t in enumerate(write_timings):
            result.operation_results.append(
                OperationResult.from_timing(
                    run_id=run_id,
                    database_name=adapter.name,
                    operation="mixed_write",
                    phase="measurement",
                    iteration=i,
                    timing=t,
                )
            )
        for i, t in enumerate(read_timings):
            result.operation_results.append(
                OperationResult.from_timing(
                    run_id=run_id,
                    database_name=adapter.name,
                    operation="mixed_read",
                    phase="measurement",
                    iteration=i,
                    timing=t,
                )
            )

        # Build summaries
        if write_timings:
            result.summaries.append(
                _build_summary(
                    run_id=run_id,
                    database_name=adapter.name,
                    operation="mixed_write",
                    batch_size=self._WRITE_BATCH_SIZE,
                    workers=1,
                    timings=write_timings,
                )
            )
        if read_timings:
            result.summaries.append(
                _build_summary(
                    run_id=run_id,
                    database_name=adapter.name,
                    operation="mixed_read",
                    batch_size=0,
                    workers=self._NUM_READERS,
                    timings=read_timings,
                )
            )

        result.errors.extend(errors)
        return result

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        return [
            (f"mixed_write", f"INSERT INTO {schema.name} ..."),
            *TimeRangeBenchmark().get_queries(schema, adapter_name),
            *AggregationBenchmark().get_queries(schema, adapter_name),
            *LastPointBenchmark().get_queries(schema, adapter_name),
        ]

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _build_query_pool(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        table: Any,
        config: Any,
    ) -> list[tuple[str, tuple]]:
        """
        Pre-generate a pool of (sql, params) pairs for the readers to pick from.

        Uses a mix of time-range, aggregation, and last-point queries so that
        the reader workload represents typical TSDB query patterns.
        """
        from tsbm.benchmarks.queries import _random_windows, _window_from_config

        n_windows = max(20, config.measurement_rounds)
        windows = _random_windows(
            n=n_windows,
            table=table,
            ts_col=schema.timestamp_col,
            window_duration=_window_from_config(
                config.time_windows[0] if config.time_windows else "1h"
            ),
            seed=config.prng_seed,
        )
        if not windows:
            return []

        pool: list[tuple[str, tuple]] = []
        pool.extend(
            TimeRangeBenchmark()._make_queries(schema, adapter_name, windows, config)
        )
        pool.extend(
            AggregationBenchmark()._make_queries(schema, adapter_name, windows, config)
        )
        pool.extend(
            LastPointBenchmark()._make_queries(schema, adapter_name, windows, config)
        )
        return pool

"""
Precise timing primitives for benchmark measurements.

Uses ``time.perf_counter_ns()`` for nanosecond-resolution wall-clock timing.
The garbage collector is optionally disabled during measurement to eliminate
GC pauses from latency samples.

Usage
-----
    with timed_operation(rows=50_000) as result:
        await adapter.ingest_batch(table, "sensors")
    print(result.elapsed_ms, result.rows_per_second)
"""
from __future__ import annotations

import gc
import time
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Generator


# ---------------------------------------------------------------------------
# Result container
# ---------------------------------------------------------------------------


@dataclass
class TimingResult:
    """
    Timing outcome for a single benchmark operation.

    All timing is in nanoseconds internally; convenience properties expose
    milliseconds and throughput metrics.
    """

    elapsed_ns: int = 0
    rows_processed: int = 0
    bytes_processed: int = 0

    # ------------------------------------------------------------------
    # Derived metrics (read-only properties)
    # ------------------------------------------------------------------

    @property
    def elapsed_ms(self) -> float:
        return self.elapsed_ns / 1_000_000.0

    @property
    def elapsed_s(self) -> float:
        return self.elapsed_ns / 1_000_000_000.0

    @property
    def rows_per_second(self) -> float:
        if self.elapsed_ns <= 0 or self.rows_processed <= 0:
            return 0.0
        return self.rows_processed / self.elapsed_s

    @property
    def mb_per_second(self) -> float:
        if self.elapsed_ns <= 0 or self.bytes_processed <= 0:
            return 0.0
        return (self.bytes_processed / 1_048_576.0) / self.elapsed_s

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def as_dict(self) -> dict[str, float | int]:
        return {
            "elapsed_ns":      self.elapsed_ns,
            "elapsed_ms":      self.elapsed_ms,
            "rows_processed":  self.rows_processed,
            "bytes_processed": self.bytes_processed,
            "rows_per_second": self.rows_per_second,
            "mb_per_second":   self.mb_per_second,
        }

    def __repr__(self) -> str:
        return (
            f"TimingResult("
            f"elapsed={self.elapsed_ms:.2f}ms, "
            f"rows={self.rows_processed:,}, "
            f"throughput={self.rows_per_second:,.0f} rows/s)"
        )


# ---------------------------------------------------------------------------
# Context managers
# ---------------------------------------------------------------------------


@contextmanager
def timed_operation(
    rows: int = 0,
    bytes_count: int = 0,
    disable_gc: bool = True,
) -> Generator[TimingResult, None, None]:
    """
    Context manager that measures wall-clock time for a block of code.

    Parameters
    ----------
    rows:
        Expected number of rows to be processed (used for throughput).
        Can be updated on the result object inside the ``with`` block.
    bytes_count:
        Expected bytes to be transferred.  Can be updated inside the block.
    disable_gc:
        If True (default), run ``gc.collect()`` and disable the garbage
        collector before measurement, then re-enable it afterward.
        Set to False for long-running or mixed read/write benchmarks where
        GC should run normally.

    Yields
    ------
    TimingResult
        A mutable result object.  ``elapsed_ns`` is populated on block exit.
        You may update ``rows_processed`` and ``bytes_processed`` inside the
        block if the actual counts differ from the initial estimates.

    Example
    -------
    ::

        with timed_operation(rows=len(batch)) as result:
            await adapter.ingest_batch(batch, table_name)
        print(result)
    """
    result = TimingResult(
        elapsed_ns=0,
        rows_processed=rows,
        bytes_processed=bytes_count,
    )

    if disable_gc:
        gc.collect()
        gc.disable()

    try:
        start = time.perf_counter_ns()
        yield result
    finally:
        result.elapsed_ns = time.perf_counter_ns() - start
        if disable_gc:
            gc.enable()


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------


def estimate_table_bytes(table: object) -> int:
    """
    Estimate the in-memory size of a PyArrow Table in bytes.

    Falls back to 0 if the table doesn't expose ``nbytes``.
    """
    return getattr(table, "nbytes", 0)

"""
Result data models for benchmark runs.

All models are Pydantic v2 BaseModels so they serialise directly via
``.model_dump()`` for SQLite summary rows and Parquet per-operation files.
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from tsbm.metrics.stats import LatencyStats
from tsbm.metrics.monitor import ResourceSummary


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


def _new_run_id() -> str:
    return uuid.uuid4().hex[:12]


# ---------------------------------------------------------------------------
# RunConfig — metadata for a single benchmark run
# ---------------------------------------------------------------------------


class RunConfig(BaseModel):
    """Identifies and describes a single benchmark invocation."""

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str = Field(default_factory=_new_run_id)
    benchmark_name: str
    database_name: str
    dataset_name: str
    config_hash: str = ""
    started_at: datetime = Field(default_factory=_utcnow)
    completed_at: datetime | None = None
    # Snapshot of the full Settings dict at run time
    config_snapshot: dict[str, Any] = Field(default_factory=dict)

    def mark_complete(self) -> None:
        """Set ``completed_at`` to now (mutates in place)."""
        # Pydantic v2: bypass frozen if not frozen; RunConfig is not frozen
        object.__setattr__(self, "completed_at", _utcnow())


# ---------------------------------------------------------------------------
# OperationResult — one timing sample for a single operation iteration
# ---------------------------------------------------------------------------


class OperationResult(BaseModel):
    """
    Timing data for a single benchmark operation iteration.

    *phase* is ``"warmup"`` or ``"measurement"``; warmup results are recorded
    but excluded from ``BenchmarkSummary`` aggregation.
    """

    run_id: str
    database_name: str
    operation: str       # e.g. "ingestion", "time_range", "aggregation"
    phase: str           # "warmup" | "measurement"
    iteration: int
    batch_size: int = 0
    workers: int = 1
    elapsed_ns: int
    rows_processed: int
    bytes_processed: int
    rows_per_second: float
    mb_per_second: float

    @classmethod
    def from_timing(
        cls,
        run_id: str,
        database_name: str,
        operation: str,
        phase: str,
        iteration: int,
        timing: Any,       # TimingResult — avoid circular import at type check
        batch_size: int = 0,
        workers: int = 1,
    ) -> OperationResult:
        """Construct from a :class:`~tsbm.metrics.timer.TimingResult`."""
        return cls(
            run_id=run_id,
            database_name=database_name,
            operation=operation,
            phase=phase,
            iteration=iteration,
            batch_size=batch_size,
            workers=workers,
            elapsed_ns=timing.elapsed_ns,
            rows_processed=timing.rows_processed,
            bytes_processed=timing.bytes_processed,
            rows_per_second=timing.rows_per_second,
            mb_per_second=timing.mb_per_second,
        )


# ---------------------------------------------------------------------------
# BenchmarkSummary — aggregated stats for one (db, operation, batch, workers)
# ---------------------------------------------------------------------------


class BenchmarkSummary(BaseModel):
    """
    Aggregated latency and throughput statistics for a completed benchmark
    combination.

    *latency* is derived from the nanosecond ``elapsed_ns`` values of all
    *measurement* phase ``OperationResult`` records via
    :func:`~tsbm.metrics.stats.compute_stats`.
    """

    model_config = ConfigDict(arbitrary_types_allowed=True)

    run_id: str
    database_name: str
    operation: str
    batch_size: int = 0
    workers: int = 1
    total_rows: int = 0
    sample_count: int = 0

    # Latency percentiles (all in milliseconds)
    latency: LatencyStats

    # Throughput aggregates
    rows_per_second_mean: float = 0.0
    rows_per_second_p95: float = 0.0
    rows_per_second_p99: float = 0.0
    mb_per_second_mean: float = 0.0

    # Resource usage (optional — populated when ResourceMonitor is enabled)
    resource: ResourceSummary | None = None

    def flat_dict(self) -> dict[str, Any]:
        """
        Flatten into a single dict suitable for a SQLite summary row or CSV.
        ``latency`` fields are inlined with ``latency_`` prefix.
        ``resource`` fields are inlined with ``resource_`` prefix (None when
        resource monitoring was disabled).
        """
        d: dict[str, Any] = {
            "run_id": self.run_id,
            "database_name": self.database_name,
            "operation": self.operation,
            "batch_size": self.batch_size,
            "workers": self.workers,
            "total_rows": self.total_rows,
            "sample_count": self.sample_count,
            "rows_per_second_mean": self.rows_per_second_mean,
            "rows_per_second_p95": self.rows_per_second_p95,
            "rows_per_second_p99": self.rows_per_second_p99,
            "mb_per_second_mean": self.mb_per_second_mean,
        }
        for k, v in self.latency.model_dump(by_alias=False).items():
            d[f"latency_{k}"] = v
        # Always emit resource keys so SQL named-parameter binding never fails
        resource_keys = [
            "cpu_percent_mean", "cpu_percent_max",
            "rss_mb_mean", "rss_mb_max",
            "vm_used_mb_mean", "vm_used_mb_max",
            "disk_read_mb_total", "disk_write_mb_total",
            "net_sent_mb_total", "net_recv_mb_total",
            "sample_count", "duration_s",
        ]
        if self.resource is not None:
            resource_data = self.resource.model_dump()
            for k in resource_keys:
                d[f"resource_{k}"] = resource_data.get(k)
        else:
            for k in resource_keys:
                d[f"resource_{k}"] = None
        return d

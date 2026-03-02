"""
Benchmark workload registry.

Nine workloads are registered:

  ingestion              — raw write throughput at various batch sizes and workers
  ingestion_out_of_order — like ingestion but with shuffled timestamps
  time_range             — point-in-time window scan
  aggregation            — hourly average / min / max aggregation
  last_point             — latest value per tag (device)
  high_cardinality       — GROUP BY on tag within a time window
  downsampling           — multi-resolution aggregation (1min, 1h, 1day)
  mixed                  — concurrent read + write for a fixed duration
  materialized_view      — raw aggregation vs. continuous aggregate / MV query
  late_arrival           — MV recompute cost when late-arriving data is inserted

Usage::

    workload = get_workload("ingestion")
    result = await workload.run(adapter, dataset, config, run_id)
"""
from __future__ import annotations

from tsbm.benchmarks.base import BenchmarkWorkload
from tsbm.benchmarks.ingestion import (
    IngestionBenchmark,
    OutOfOrderIngestionBenchmark,
)
from tsbm.benchmarks.materialized_views import (
    LateArrivalBenchmark,
    MaterializedViewBenchmark,
)
from tsbm.benchmarks.mixed import MixedBenchmark
from tsbm.benchmarks.queries import (
    AggregationBenchmark,
    DownsamplingBenchmark,
    HighCardinalityBenchmark,
    LastPointBenchmark,
    TimeRangeBenchmark,
)

# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

_WORKLOADS: dict[str, BenchmarkWorkload] = {
    "ingestion":              IngestionBenchmark(),
    "ingestion_out_of_order": OutOfOrderIngestionBenchmark(),
    "time_range":             TimeRangeBenchmark(),
    "aggregation":            AggregationBenchmark(),
    "last_point":             LastPointBenchmark(),
    "high_cardinality":       HighCardinalityBenchmark(),
    "downsampling":           DownsamplingBenchmark(),
    "mixed":                  MixedBenchmark(),
    "materialized_view":      MaterializedViewBenchmark(),
    "late_arrival":           LateArrivalBenchmark(),
}


def get_workload(name: str) -> BenchmarkWorkload:
    """
    Return the workload registered under *name*.

    Raises
    ------
    KeyError
        If *name* is not a registered workload.
    """
    try:
        return _WORKLOADS[name]
    except KeyError:
        available = ", ".join(sorted(_WORKLOADS))
        raise KeyError(
            f"Unknown benchmark workload {name!r}. "
            f"Available workloads: {available}"
        ) from None


def list_workloads() -> list[str]:
    """Return sorted list of all registered workload names."""
    return sorted(_WORKLOADS)


# Curated set of core benchmarks that cover the key dimensions:
# write speed, scan, aggregation, latest value, concurrent read+write, downsampling.
_DEFAULT_WORKLOADS: list[str] = [
    "ingestion",
    "time_range",
    "aggregation",
    "last_point",
    "mixed",
    "downsampling",
]


def list_default_workloads() -> list[str]:
    """Return the default (curated) benchmark set."""
    return list(_DEFAULT_WORKLOADS)


def all_workloads() -> dict[str, BenchmarkWorkload]:
    """Return a shallow copy of the full registry dict."""
    return dict(_WORKLOADS)

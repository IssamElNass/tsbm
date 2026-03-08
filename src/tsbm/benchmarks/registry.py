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


# Optimal execution order: query-only benchmarks first (they share seeded
# data and avoid redundant drop/re-seed cycles), then mixed (can reuse the
# seed), then MV benchmarks (reuse seed, manage own views), then ingestion last.
_BENCHMARK_ORDER: list[str] = [
    # Query-only — seed once, reuse across all
    "time_range",
    "aggregation",
    "last_point",
    "high_cardinality",
    "downsampling",
    # Mixed — needs seed, can reuse from query benchmarks above
    "mixed",
    # MV benchmarks — reuse seeded table, create/drop their own views
    "materialized_view",
    "late_arrival",
    # Ingestion — always reset, manage own data (no pre-seed needed)
    "ingestion",
    "ingestion_out_of_order",
]


def list_workloads_ordered() -> list[str]:
    """Return all workloads in optimal execution order.

    Places query-only benchmarks first so they share a single seeded table,
    then mixed, then always-reset benchmarks (MV, ingestion) last.  This
    minimises the number of expensive drop/re-seed cycles.
    """
    # Include any newly registered workloads not yet in the explicit order.
    ordered = [w for w in _BENCHMARK_ORDER if w in _WORKLOADS]
    extras = sorted(set(_WORKLOADS) - set(_BENCHMARK_ORDER))
    return ordered + extras


# Curated set of core benchmarks that cover the key dimensions:
# write speed, scan, aggregation, latest value, concurrent read+write, downsampling.
_DEFAULT_WORKLOADS: list[str] = [
    "time_range",
    "aggregation",
    "last_point",
    "downsampling",
    "mixed",
    "ingestion",
]


def list_default_workloads() -> list[str]:
    """Return the default (curated) benchmark set."""
    return list(_DEFAULT_WORKLOADS)


def all_workloads() -> dict[str, BenchmarkWorkload]:
    """Return a shallow copy of the full registry dict."""
    return dict(_WORKLOADS)

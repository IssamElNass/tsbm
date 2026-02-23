"""
BenchmarkWorkload Protocol and BenchmarkResult container.

Every benchmark workload (ingestion, queries, mixed) implements
``BenchmarkWorkload`` via structural subtyping — no inheritance needed.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

if TYPE_CHECKING:
    from tsbm.adapters.base import DatabaseAdapter
    from tsbm.config.settings import WorkloadConfig
    from tsbm.datasets.schema import DatasetSchema
    from tsbm.metrics.monitor import ResourceSample
    from tsbm.results.models import BenchmarkSummary, OperationResult


# ---------------------------------------------------------------------------
# BenchmarkResult — returned by every BenchmarkWorkload.run()
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkResult:
    """
    All output produced by a single benchmark run for one (adapter, workload).

    The caller (CLI runner) is responsible for persisting these to storage and
    aggregating resource samples from the surrounding ResourceMonitor.
    """

    run_id: str
    database_name: str
    benchmark_name: str

    # Raw per-iteration timing records (warmup + measurement phases)
    operation_results: list[OperationResult] = field(default_factory=list)

    # Aggregated summaries (measurement phase only, computed by the workload)
    summaries: list[BenchmarkSummary] = field(default_factory=list)

    # Non-fatal errors encountered during the run (continue on error policy)
    errors: list[str] = field(default_factory=list)

    # ------------------------------------------------------------------
    # Convenience
    # ------------------------------------------------------------------

    def measurement_results(self) -> list[OperationResult]:
        """Filter to measurement-phase results only."""
        return [r for r in self.operation_results if r.phase == "measurement"]

    def has_errors(self) -> bool:
        return bool(self.errors)


# ---------------------------------------------------------------------------
# BenchmarkWorkload Protocol
# ---------------------------------------------------------------------------


@runtime_checkable
class BenchmarkWorkload(Protocol):
    """
    Structural protocol satisfied by all benchmark workload classes.

    Implementations must set ``name`` and ``description`` as class or
    instance attributes.

    The ``run`` coroutine executes the full benchmark lifecycle
    (warmup → measurement) and returns a :class:`BenchmarkResult`.
    """

    name: str
    description: str

    async def run(
        self,
        adapter: DatabaseAdapter,
        dataset: Any,        # BenchmarkDataset — avoids heavy circular import
        config: WorkloadConfig,
        run_id: str,
    ) -> BenchmarkResult:
        """
        Execute the benchmark and return aggregated results.

        The caller must have already called ``adapter.connect()`` and
        ``adapter.create_table(dataset.schema)``.
        """
        ...

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        """
        Return a list of ``(query_name, sql_string)`` pairs for this workload.

        Used by the CLI for preview/dry-run mode.  Query benchmarks return
        their full SQL template set; ingestion benchmarks return ``[]``.
        """
        ...

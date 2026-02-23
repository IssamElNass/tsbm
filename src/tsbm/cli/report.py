"""
Async logic for the ``tsbm report`` command.

Generates a comprehensive Markdown report from stored benchmark results,
including per-metric comparison tables (databases as columns) and the
exact SQL queries executed per benchmark and database.

Auto-discovery (``latest=True``) picks the most recently completed run
per (benchmark_name, database_name) pair so the user does not need to
know or supply run IDs.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from rich.console import Console

console = Console()
logger = logging.getLogger(__name__)


async def async_report(
    run_ids: list[str] | None,
    benchmark_filter: str | None,
    output: Path,
    config_path: Path | None,
    latest: bool,
) -> None:
    """Generate a Markdown benchmark report and write it to *output*."""
    from tsbm.benchmarks.registry import all_workloads
    from tsbm.config.settings import get_settings, load_settings_from_file
    from tsbm.datasets.schema import DatasetSchema
    from tsbm.results.export import export_full_report
    from tsbm.results.storage import ResultStorage

    settings = load_settings_from_file(config_path) if config_path else get_settings()
    storage = ResultStorage(
        sqlite_path=settings.results.sqlite_path,
        parquet_dir=settings.results.parquet_dir,
    )
    storage.initialize()

    all_runs = storage.list_runs()
    if not all_runs:
        console.print("[yellow]No benchmark runs found in the results database.[/yellow]")
        return

    # ------------------------------------------------------------------
    # 1. Resolve which run IDs to include
    # ------------------------------------------------------------------
    if run_ids:
        resolved_ids = list(run_ids)
    elif latest:
        resolved_ids = _pick_latest_run_ids(all_runs, benchmark_filter)
    else:
        resolved_ids = [r["run_id"] for r in all_runs]

    if not resolved_ids:
        console.print("[yellow]No completed runs found to include in the report.[/yellow]")
        return

    # ------------------------------------------------------------------
    # 2. Load metadata and summaries for resolved runs
    # ------------------------------------------------------------------
    run_metadata = [r for r in all_runs if r["run_id"] in set(resolved_ids)]

    summaries = storage.load_summaries(
        run_ids=resolved_ids,
        operation=benchmark_filter,
    )

    if not summaries:
        console.print(
            "[yellow]No summary data found for the selected runs. "
            "Have the benchmarks completed successfully?[/yellow]"
        )
        return

    # ------------------------------------------------------------------
    # 3. Build SQL sections from workload.get_queries()
    # ------------------------------------------------------------------
    workloads = all_workloads()
    adapter_names = sorted({r["database_name"] for r in run_metadata})
    benchmark_names = sorted({r["benchmark_name"] for r in run_metadata})

    sql_sections: dict[str, dict[str, list[tuple[str, str]]]] = {}

    for bench_name in benchmark_names:
        workload = workloads.get(bench_name)
        if workload is None:
            continue

        schema = _reconstruct_schema(run_metadata, bench_name)
        if schema is None:
            # Old run without schema in snapshot — skip SQL section
            sql_sections[bench_name] = {}
            logger.debug(
                "No dataset_schema in config_snapshot for benchmark %r — SQL section skipped",
                bench_name,
            )
            continue

        sql_sections[bench_name] = {}
        for adapter_name in adapter_names:
            try:
                queries = workload.get_queries(schema, adapter_name)
            except Exception as exc:
                logger.warning(
                    "get_queries failed for %s / %s: %s", bench_name, adapter_name, exc
                )
                queries = []
            sql_sections[bench_name][adapter_name] = queries

    # ------------------------------------------------------------------
    # 4. Render and write the report
    # ------------------------------------------------------------------
    report_text = export_full_report(
        summaries=summaries,
        run_metadata=run_metadata,
        sql_sections=sql_sections,
        output=output,
    )

    n_benchmarks = len({r["benchmark_name"] for r in run_metadata})
    n_dbs = len(adapter_names)
    n_runs = len(run_metadata)
    console.print(
        f"[green]Report written:[/green] {output}  "
        f"[dim]({n_runs} runs, {n_benchmarks} benchmarks, {n_dbs} databases, "
        f"{len(report_text):,} chars)[/dim]"
    )


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pick_latest_run_ids(
    all_runs: list[dict],
    benchmark_filter: str | None,
) -> list[str]:
    """
    For each (benchmark_name, database_name) pair, pick the most recently
    *completed* run.  Returns a deduplicated list of run IDs.
    """
    # Filter to completed runs only
    completed = [r for r in all_runs if r.get("completed_at")]
    if benchmark_filter:
        completed = [r for r in completed if r["benchmark_name"] == benchmark_filter]

    # Group by (benchmark_name, database_name), keeping latest started_at
    best: dict[tuple[str, str], dict] = {}
    for r in completed:
        key = (r["benchmark_name"], r["database_name"])
        if key not in best or r["started_at"] > best[key]["started_at"]:
            best[key] = r

    return [r["run_id"] for r in best.values()]


def _reconstruct_schema(
    run_metadata: list[dict],
    bench_name: str,
) -> "DatasetSchema | None":
    """
    Reconstruct a DatasetSchema from the ``dataset_schema`` key stored in
    ``config_snapshot`` by the runner.  Returns ``None`` if unavailable.
    """
    from tsbm.datasets.schema import DatasetSchema

    for r in run_metadata:
        if r.get("benchmark_name") != bench_name:
            continue
        raw_snapshot = r.get("config_snapshot")
        if not raw_snapshot:
            continue
        try:
            snapshot = (
                json.loads(raw_snapshot)
                if isinstance(raw_snapshot, str)
                else raw_snapshot
            )
            schema_dict = snapshot.get("dataset_schema")
            if schema_dict:
                return DatasetSchema.from_dict(schema_dict)
        except Exception as exc:
            logger.debug("Schema reconstruction failed for run %s: %s", r.get("run_id"), exc)
    return None

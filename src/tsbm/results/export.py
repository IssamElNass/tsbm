"""
Result export utilities.

Supports three output formats from the summaries table:
  * CSV   — plain comma-separated, suitable for spreadsheets
  * JSON  — pretty-printed array of objects
  * Markdown — comparison table with databases as columns
  * Full report — comprehensive Markdown document with results + SQL queries
"""
from __future__ import annotations

import csv
import io
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


# ---------------------------------------------------------------------------
# Benchmark descriptions (shared with cli/run.py and report generation)
# ---------------------------------------------------------------------------

BENCHMARK_DESCRIPTIONS: dict[str, str] = {
    "ingestion": (
        "Measures raw write throughput at varying batch sizes and worker counts. "
        "Higher Rows/sec = better write performance. Critical for high-velocity IoT ingestion."
    ),
    "ingestion_out_of_order": (
        "Same as ingestion but timestamps are shuffled before insert. "
        "Reveals overhead for out-of-order writes and whether the database reorders efficiently."
    ),
    "time_range": (
        "Scans a random time window using a WHERE clause. "
        "Lower p99 latency = more predictable response times for dashboards and alerting."
    ),
    "aggregation": (
        "GROUP BY time-bucket aggregations (avg/min/max per hour). "
        "The most common time-series pattern — lower p99 indicates efficient aggregate pushdown."
    ),
    "last_point": (
        "Fetches the most recent value per device/tag. "
        "Tests LATEST BY / DISTINCT ON optimisation — critical for IoT status dashboards."
    ),
    "high_cardinality": (
        "GROUP BY on a tag column with 10,000+ unique values. "
        "Exposes cardinality bottlenecks that severely degrade performance in many systems."
    ),
    "downsampling": (
        "Multi-resolution aggregation across 1-min, 1-hour, and 1-day windows. "
        "Simulates reducing raw samples for long-term storage and trend analysis."
    ),
    "mixed": (
        "Concurrent read and write workload running simultaneously. "
        "Reflects real production load — high latency or low throughput here signals contention."
    ),
    "materialized_view": (
        "Compares raw aggregation speed versus a pre-computed materialized view / continuous aggregate. "
        "A large speedup ratio shows effective MV support; a small gain means overhead dominates."
    ),
    "late_arrival": (
        "Measures cost of inserting out-of-order data when materialized views are active. "
        "Low recompute overhead = efficient incremental MV refresh."
    ),
}


DATABASE_DESCRIPTIONS: dict[str, str] = {
    "questdb": (
        "**Ingestion:** InfluxDB Line Protocol (ILP) over HTTP — bypasses SQL parsing "
        "for the highest possible write throughput.\n"
        "**Consistency:** immediate; all rows are visible as soon as the ILP batch is committed.\n"
        "**Time-bucketing:** native `SAMPLE BY` clause (e.g. `SAMPLE BY 1h`) — no GROUP BY required.\n"
        "**Last-point:** `LATEST ON ts PARTITION BY tag` — O(1) per partition via the timestamp index.\n"
        "**Partitioning:** table is range-partitioned by time (`PARTITION BY DAY` by default); "
        "time-range queries skip irrelevant partitions entirely, so short-window scans are very fast."
    ),
    "cratedb": (
        "**Ingestion:** `INSERT … SELECT * FROM UNNEST($1, $2, …)` bulk insert over the "
        "PostgreSQL wire protocol. Type casts (`::TYPE[]`) are omitted because CrateDB's ANTLR "
        "parser misreads multi-word type names (`TIMESTAMP WITH TIME ZONE`, `DOUBLE PRECISION`); "
        "types are inferred from the INSERT target column declarations instead.\n"
        "**Consistency:** eventually consistent — a `REFRESH TABLE` is issued before every "
        "query benchmark to force shard refresh (default refresh interval ~1 s).\n"
        "**Time-bucketing:** `DATE_BIN('1 hour', ts, TIMESTAMP '1970-01-01')`.\n"
        "**Last-point:** `ROW_NUMBER() OVER (PARTITION BY tag ORDER BY ts DESC)` window function "
        "(CrateDB does not support PostgreSQL's `DISTINCT ON`).\n"
        "**Materialized views:** simulated via a plain summary table; full re-population on refresh "
        "(no incremental update)."
    ),
    "timescaledb": (
        "**Ingestion:** PostgreSQL `COPY` protocol via asyncpg — the fastest ingest path "
        "for standard PostgreSQL. The table is converted to a hypertable with "
        "`SELECT create_hypertable(table, ts_col)`.\n"
        "**Consistency:** immediate (standard PostgreSQL MVCC).\n"
        "**Time-bucketing:** `time_bucket('1 hour', ts)` — a TimescaleDB extension function.\n"
        "**Last-point:** `SELECT DISTINCT ON (tag) … ORDER BY tag, ts DESC`.\n"
        "**Chunk interval:** the hypertable is partitioned into time chunks "
        "(`chunk_time_interval`, default 7 days); queries touching fewer chunks run faster — "
        "tune this to match your data frequency.\n"
        "**Continuous aggregates:** native materialized views with incremental refresh "
        "(only recomputes changed chunks)."
    ),
}


# ---------------------------------------------------------------------------
# CSV
# ---------------------------------------------------------------------------


def export_csv(rows: list[dict[str, Any]], output: Path | None = None) -> str:
    """
    Serialise *rows* (list of flat dicts) to CSV.

    Parameters
    ----------
    rows:
        Flat dicts — typically from :meth:`~ResultStorage.load_summaries`.
    output:
        Optional file path to write to.  When given the function also writes
        the file; the CSV string is always returned.
    """
    if not rows:
        return ""

    buf = io.StringIO()
    fieldnames = list(rows[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames, lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    text = buf.getvalue()

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")

    return text


# ---------------------------------------------------------------------------
# JSON
# ---------------------------------------------------------------------------


def export_json(rows: list[dict[str, Any]], output: Path | None = None) -> str:
    """
    Serialise *rows* to a pretty-printed JSON array.

    Datetime objects are converted to ISO-8601 strings automatically.
    """
    text = json.dumps(rows, indent=2, default=_json_default)

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")

    return text


def _json_default(obj: Any) -> Any:
    if hasattr(obj, "isoformat"):
        return obj.isoformat()
    raise TypeError(f"Object of type {type(obj).__name__} is not JSON serialisable")


# ---------------------------------------------------------------------------
# Markdown comparison table
# ---------------------------------------------------------------------------


def export_markdown(
    rows: list[dict[str, Any]],
    metric: str = "latency_p99_ms",
    output: Path | None = None,
) -> str:
    """
    Render *rows* as a Markdown comparison table.

    The table rows are ``(operation, batch_size, workers)`` combinations;
    the columns are the distinct database names; cells contain the requested
    *metric* value.

    Parameters
    ----------
    rows:
        Flat summary dicts (from :meth:`~ResultStorage.load_summaries`).
    metric:
        Column to show in cells.  Defaults to ``"latency_p99_ms"``.
    output:
        Optional file path to write to.
    """
    if not rows:
        return "_No results_\n"

    # Discover distinct databases and operation keys
    databases = sorted({r["database_name"] for r in rows})
    op_keys = sorted(
        {(r["operation"], r.get("batch_size", 0), r.get("workers", 1)) for r in rows}
    )

    # Build a lookup: (operation, batch_size, workers, database) → metric value
    lookup: dict[tuple, Any] = {}
    for r in rows:
        key = (r["operation"], r.get("batch_size", 0), r.get("workers", 1), r["database_name"])
        lookup[key] = r.get(metric)

    # Header
    header_cols = ["Operation", "Batch", "Workers"] + databases
    lines: list[str] = []
    lines.append("| " + " | ".join(header_cols) + " |")
    lines.append("| " + " | ".join("---" for _ in header_cols) + " |")

    for op, batch, workers in op_keys:
        cells: list[str] = [op, str(batch), str(workers)]
        for db in databases:
            val = lookup.get((op, batch, workers, db))
            cells.append(_fmt_cell(val))
        lines.append("| " + " | ".join(cells) + " |")

    table_text = "\n".join(lines) + "\n"

    # Add a legend line
    text = f"**Metric: `{metric}`**\n\n{table_text}"

    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")

    return text


# ---------------------------------------------------------------------------
# Multi-run comparison helpers
# ---------------------------------------------------------------------------


def compare_runs(
    run_a_rows: list[dict[str, Any]],
    run_b_rows: list[dict[str, Any]],
    run_a_label: str = "run_a",
    run_b_label: str = "run_b",
    metric: str = "latency_p99_ms",
) -> str:
    """
    Produce a Markdown diff table comparing two runs side-by-side.

    Cells show ``value_a → value_b (±delta%)``.
    """
    if not run_a_rows and not run_b_rows:
        return "_No results_\n"

    # Merge by (operation, batch_size, workers, database_name)
    def _index(rows: list[dict[str, Any]]) -> dict[tuple, Any]:
        return {
            (r["operation"], r.get("batch_size", 0), r.get("workers", 1), r["database_name"]): r
            for r in rows
        }

    idx_a = _index(run_a_rows)
    idx_b = _index(run_b_rows)
    all_keys = sorted(idx_a.keys() | idx_b.keys())

    header = ["Operation", "Batch", "Workers", "Database", run_a_label, run_b_label, "Δ%"]
    lines: list[str] = []
    lines.append("| " + " | ".join(header) + " |")
    lines.append("| " + " | ".join("---" for _ in header) + " |")

    for op, batch, workers, db in all_keys:
        ra = idx_a.get((op, batch, workers, db), {})
        rb = idx_b.get((op, batch, workers, db), {})
        va = ra.get(metric)
        vb = rb.get(metric)
        delta = _pct_delta(va, vb)
        cells = [op, str(batch), str(workers), db, _fmt_cell(va), _fmt_cell(vb), delta]
        lines.append("| " + " | ".join(cells) + " |")

    return f"**Metric: `{metric}`**\n\n" + "\n".join(lines) + "\n"


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _fmt_cell(val: Any) -> str:
    if val is None:
        return "—"
    if isinstance(val, float):
        return f"{val:.2f}"
    return str(val)


def _pct_delta(a: Any, b: Any) -> str:
    try:
        fa, fb = float(a), float(b)
        if fa == 0:
            return "—"
        delta = (fb - fa) / fa * 100.0
        sign = "+" if delta >= 0 else ""
        return f"{sign}{delta:.1f}%"
    except (TypeError, ValueError):
        return "—"


# ---------------------------------------------------------------------------
# Full Markdown report
# ---------------------------------------------------------------------------

_THROUGHPUT_BENCHMARKS: frozenset[str] = frozenset({
    "ingestion",
    "ingestion_out_of_order",
    "mixed",
})

_ALL_METRICS: list[tuple[str, str]] = [
    # Throughput
    ("rows_per_second_mean", "Rows/sec (mean)"),
    ("rows_per_second_p95",  "Rows/sec (p95)"),
    ("rows_per_second_p99",  "Rows/sec (p99)"),
    ("mb_per_second_mean",   "MB/sec (mean)"),
    # Latency — central tendency
    ("latency_mean_ms",      "Latency mean (ms)"),
    ("latency_p50_ms",       "Latency p50 (ms)"),
    ("latency_p95_ms",       "Latency p95 (ms)"),
    ("latency_p99_ms",       "Latency p99 (ms)"),
    ("latency_p999_ms",      "Latency p99.9 (ms)"),
    # Latency — spread / consistency
    ("latency_stddev_ms",    "Latency stddev (ms)"),
    ("latency_iqr_ms",       "Latency IQR (ms)"),
    ("latency_min_ms",       "Latency min (ms)"),
    ("latency_max_ms",       "Latency max (ms)"),
    ("latency_outlier_count", "Outlier count"),
    # Resource usage (blank cells when monitor is disabled)
    ("resource_cpu_percent_mean",    "CPU % (mean)"),
    ("resource_cpu_percent_max",     "CPU % (max)"),
    ("resource_rss_mb_mean",         "RSS MB (mean)"),
    ("resource_disk_write_mb_total", "Disk write MB"),
]


def export_full_report(
    summaries: list[dict[str, Any]],
    run_metadata: list[dict[str, Any]],
    sql_sections: dict[str, dict[str, list[tuple[str, str]]]],
    output: Path | None = None,
) -> str:
    """
    Generate a comprehensive Markdown benchmark report.

    Parameters
    ----------
    summaries:
        Flat summary dicts from ``ResultStorage.load_summaries()``.
    run_metadata:
        Run dicts from ``ResultStorage.list_runs()`` for the selected run IDs.
    sql_sections:
        ``sql_sections[benchmark_name][adapter_name] = [(query_name, sql), ...]``
        Built by the report command by calling ``workload.get_queries(schema, adapter)``.
    output:
        Optional path to write the Markdown file.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    databases = sorted({r["database_name"] for r in run_metadata})
    dataset_names = sorted({r["dataset_name"] for r in run_metadata})
    benchmark_names = _ordered_benchmarks(run_metadata)

    # Build a run_id → benchmark_name lookup
    _run_to_bench: dict[str, str] = {r["run_id"]: r["benchmark_name"] for r in run_metadata}

    # Group summaries by benchmark_name
    bench_summaries: dict[str, list[dict[str, Any]]] = {}
    for s in summaries:
        bname = _run_to_bench.get(s.get("run_id", ""), "unknown")
        bench_summaries.setdefault(bname, []).append(s)

    lines: list[str] = []

    # ---- Header ----
    lines.append("# tsbm Benchmark Report\n")
    lines.append(f"**Generated:** {now}  ")
    lines.append(f"**Databases:** {', '.join(databases)}  ")
    lines.append(f"**Dataset(s):** {', '.join(dataset_names)}  ")
    lines.append("\n---\n")

    # ---- Run Metadata ----
    lines.append("## Run Metadata\n")
    lines.append("| Benchmark | Database | Dataset | Started At | Completed At | Run ID |")
    lines.append("|---|---|---|---|---|---|")
    for r in sorted(run_metadata, key=lambda x: (x["benchmark_name"], x["database_name"])):
        completed = r.get("completed_at") or "—"
        lines.append(
            f"| {r['benchmark_name']} | {r['database_name']} | {r['dataset_name']} "
            f"| {r['started_at']} | {completed} | `{r['run_id']}` |"
        )
    lines.append("\n---\n")

    # ---- Database Methodology ----
    lines.append("## Database Methodology\n")
    lines.append(
        "> How each database ingests data, maintains consistency, and executes time-series queries.\n"
    )
    for db in databases:
        desc = DATABASE_DESCRIPTIONS.get(db)
        if desc:
            lines.append(f"### {db}\n")
            lines.append(f"{desc}\n")
    lines.append("\n---\n")

    # ---- Results ----
    lines.append("## Results\n")
    for bench_name in benchmark_names:
        bench_rows = bench_summaries.get(bench_name, [])
        lines.append(f"### {bench_name}\n")
        desc = BENCHMARK_DESCRIPTIONS.get(bench_name, "")
        if desc:
            lines.append(f"> {desc}\n")
        if bench_rows:
            for metric, metric_label in _ALL_METRICS:
                lines.append(f"\n#### {metric_label}\n")
                lines.append(export_markdown(bench_rows, metric=metric))
        else:
            lines.append("_No results available._\n")

    lines.append("\n---\n")

    # ---- SQL Queries ----
    lines.append("## SQL Queries\n")
    lines.append(
        "> The queries below show the exact SQL templates executed per database. "
        "Placeholders `<start>` and `<end>` represent the random time window bounds "
        "generated for each measurement round.\n"
    )
    for bench_name in benchmark_names:
        lines.append(f"### {bench_name}\n")
        db_queries = sql_sections.get(bench_name, {})
        has_queries = any(q for q in db_queries.values())
        if not db_queries or not has_queries:
            lines.append("_No SQL queries (write-only or not applicable)._\n")
            continue
        for adapter_name in sorted(db_queries):
            queries = db_queries[adapter_name]
            if not queries:
                continue
            lines.append(f"#### {adapter_name}\n")
            for qname, sql in queries:
                lines.append(f"**{qname}**\n")
                lines.append("```sql")
                lines.append(sql)
                lines.append("```\n")

    text = "\n".join(lines)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    return text


# ---------------------------------------------------------------------------
# Concise summary report
# ---------------------------------------------------------------------------


def export_summary_report(
    summaries: list[dict[str, Any]],
    run_metadata: list[dict[str, Any]],
    output: Path | None = None,
) -> str:
    """
    Generate a concise, conclusion-focused Markdown benchmark summary.

    Unlike ``export_full_report``, this produces a short document (~50-80 lines)
    highlighting key findings: throughput winners, query latency comparison,
    concurrency scaling, and error/reliability status.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    databases = sorted({r["database_name"] for r in run_metadata})
    dataset_names = sorted({r["dataset_name"] for r in run_metadata})
    benchmark_names = _ordered_benchmarks(run_metadata)

    # Build run_id → benchmark_name lookup
    _run_to_bench: dict[str, str] = {r["run_id"]: r["benchmark_name"] for r in run_metadata}

    # Group summaries by benchmark
    bench_summaries: dict[str, list[dict[str, Any]]] = {}
    for s in summaries:
        bname = _run_to_bench.get(s.get("run_id", ""), "unknown")
        bench_summaries.setdefault(bname, []).append(s)

    lines: list[str] = []

    # ---- Header ----
    lines.append("# TSBM Benchmark Summary\n")
    lines.append(
        f"**Databases:** {', '.join(databases)}  "
        f"**Dataset:** {', '.join(dataset_names)}  "
        f"**Date:** {now}\n"
    )
    lines.append("---\n")

    # ---- Ingestion Throughput ----
    ingestion_rows = bench_summaries.get("ingestion", []) + bench_summaries.get("ingestion_out_of_order", [])
    if ingestion_rows:
        lines.append("## Ingestion Throughput\n")
        # For each database, pick the best rows/sec across all batch/worker combos
        db_best: dict[str, dict[str, Any]] = {}
        for row in ingestion_rows:
            db = row.get("database_name", "")
            rps = row.get("rows_per_second_mean", 0) or 0
            if db not in db_best or rps > (db_best[db].get("rows_per_second_mean", 0) or 0):
                db_best[db] = row

        ranked = sorted(db_best.items(), key=lambda x: x[1].get("rows_per_second_mean", 0) or 0, reverse=True)
        lines.append("| Database | Rows/sec | MB/sec | p99 (ms) | Rank |")
        lines.append("|---|---|---|---|---|")
        for rank, (db, row) in enumerate(ranked, 1):
            rps = row.get("rows_per_second_mean", 0) or 0
            mbs = row.get("mb_per_second_mean", 0) or 0
            p99 = row.get("latency_p99_ms", 0) or 0
            label = f"**#{rank}**" if rank == 1 else f"#{rank}"
            lines.append(f"| {db} | {rps:,.0f} | {mbs:.1f} | {p99:.1f} | {label} |")
        lines.append("")

    # ---- Query Latency (p99) ----
    query_benchmarks = [b for b in benchmark_names if b not in _THROUGHPUT_BENCHMARKS]
    if query_benchmarks and databases:
        lines.append("## Query Latency (p99 ms)\n")
        header = "| Benchmark | " + " | ".join(databases) + " | Fastest |"
        sep = "| --- | " + " | ".join("---" for _ in databases) + " | --- |"
        lines.append(header)
        lines.append(sep)

        for bench_name in query_benchmarks:
            bench_rows = bench_summaries.get(bench_name, [])
            if not bench_rows:
                continue

            # Group by operation, pick the representative one (best workers=1 or smallest window)
            # For simplicity, aggregate: best p99 per database across all operations in this benchmark
            db_p99: dict[str, float] = {}
            for row in bench_rows:
                db = row.get("database_name", "")
                p99 = row.get("latency_p99_ms")
                if p99 is not None:
                    if db not in db_p99 or p99 < db_p99[db]:
                        db_p99[db] = p99

            if not db_p99:
                continue

            fastest_db = min(db_p99, key=db_p99.get)  # type: ignore[arg-type]
            cells = [bench_name]
            for db in databases:
                val = db_p99.get(db)
                cells.append(f"{val:.1f}" if val is not None else "—")
            cells.append(f"**{fastest_db}**")
            lines.append("| " + " | ".join(cells) + " |")
        lines.append("")

    # ---- Concurrency Scaling ----
    # Find a benchmark with multiple worker levels and show degradation
    _scaling_bench = None
    for candidate in ["aggregation", "time_range", "last_point"]:
        rows = bench_summaries.get(candidate, [])
        worker_vals = {r.get("workers", 1) for r in rows}
        if len(worker_vals) >= 2:
            _scaling_bench = candidate
            break

    if _scaling_bench:
        lines.append("## Concurrency Scaling\n")
        scale_rows = bench_summaries.get(_scaling_bench, [])
        worker_levels = sorted({r.get("workers", 1) for r in scale_rows})

        header_cols = ["Database"] + [f"{w}w (ms)" for w in worker_levels] + ["Degradation"]
        lines.append("| " + " | ".join(header_cols) + " |")
        lines.append("| " + " | ".join("---" for _ in header_cols) + " |")

        for db in databases:
            db_rows = [r for r in scale_rows if r.get("database_name") == db]
            if not db_rows:
                continue

            # Best p99 per worker level for this db
            worker_p99: dict[int, float] = {}
            for row in db_rows:
                w = row.get("workers", 1)
                p99 = row.get("latency_p99_ms")
                if p99 is not None:
                    if w not in worker_p99 or p99 < worker_p99[w]:
                        worker_p99[w] = p99

            cells = [db]
            for w in worker_levels:
                val = worker_p99.get(w)
                cells.append(f"{val:.1f}" if val is not None else "—")

            # Degradation: max_workers / min_workers
            min_w = min(worker_levels)
            max_w = max(worker_levels)
            if min_w in worker_p99 and max_w in worker_p99 and worker_p99[min_w] > 0:
                degrad = worker_p99[max_w] / worker_p99[min_w]
                cells.append(f"{degrad:.1f}x")
            else:
                cells.append("—")
            lines.append("| " + " | ".join(cells) + " |")

        lines.append(f"\n> Scaling benchmark: `{_scaling_bench}`\n")

    # ---- Errors & Reliability ----
    lines.append("## Errors & Reliability\n")
    lines.append("| Database | Completed Runs | Failed Runs | Status |")
    lines.append("|---|---|---|---|")
    for db in databases:
        db_runs = [r for r in run_metadata if r["database_name"] == db]
        completed = sum(1 for r in db_runs if r.get("completed_at"))
        failed = len(db_runs) - completed
        status = "Clean" if failed == 0 else f"{failed} failure(s)"
        lines.append(f"| {db} | {completed} | {failed} | {status} |")
    lines.append("")

    # ---- Key Findings ----
    lines.append("## Key Findings\n")
    findings: list[str] = []

    # Best ingestion
    if ingestion_rows:
        best_ing = max(ingestion_rows, key=lambda r: r.get("rows_per_second_mean", 0) or 0)
        rps = best_ing.get("rows_per_second_mean", 0) or 0
        findings.append(
            f"**Fastest ingestion:** {best_ing['database_name']} "
            f"({rps:,.0f} rows/sec)"
        )

    # Best query latency (lowest p99 across all query benchmarks)
    all_query_rows = []
    for b in query_benchmarks:
        all_query_rows.extend(bench_summaries.get(b, []))
    if all_query_rows:
        valid = [r for r in all_query_rows if r.get("latency_p99_ms") is not None]
        if valid:
            best_q = min(valid, key=lambda r: r["latency_p99_ms"])
            findings.append(
                f"**Lowest query latency:** {best_q['database_name']} "
                f"(p99 {best_q['latency_p99_ms']:.1f} ms on {best_q.get('operation', '?')})"
            )

    # Best concurrency scaling
    if _scaling_bench:
        scale_rows = bench_summaries.get(_scaling_bench, [])
        min_w = min(worker_levels)
        max_w = max(worker_levels)
        best_degrad = None
        best_degrad_db = None
        for db in databases:
            db_rows = [r for r in scale_rows if r.get("database_name") == db]
            w_p99: dict[int, float] = {}
            for row in db_rows:
                w = row.get("workers", 1)
                p99 = row.get("latency_p99_ms")
                if p99 is not None:
                    if w not in w_p99 or p99 < w_p99[w]:
                        w_p99[w] = p99
            if min_w in w_p99 and max_w in w_p99 and w_p99[min_w] > 0:
                d = w_p99[max_w] / w_p99[min_w]
                if best_degrad is None or d < best_degrad:
                    best_degrad = d
                    best_degrad_db = db
        if best_degrad_db and best_degrad:
            findings.append(
                f"**Best concurrency scaling:** {best_degrad_db} "
                f"({best_degrad:.1f}x degradation from {min_w} to {max_w} workers)"
            )

    # Mixed benchmark summary
    mixed_rows = bench_summaries.get("mixed", [])
    if mixed_rows:
        valid_mixed = [r for r in mixed_rows if r.get("rows_per_second_mean")]
        if valid_mixed:
            best_mixed = max(valid_mixed, key=lambda r: r.get("rows_per_second_mean", 0) or 0)
            findings.append(
                f"**Best mixed read/write:** {best_mixed['database_name']} "
                f"({best_mixed.get('rows_per_second_mean', 0):,.0f} rows/sec under concurrent load)"
            )

    for i, finding in enumerate(findings, 1):
        lines.append(f"{i}. {finding}")

    lines.append("")

    text = "\n".join(lines)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    return text


def _ordered_benchmarks(run_metadata: list[dict[str, Any]]) -> list[str]:
    """Return benchmark names in a logical order (throughput first, then query benchmarks)."""
    seen = {r["benchmark_name"] for r in run_metadata}
    ordered = [
        "ingestion",
        "ingestion_out_of_order",
        "time_range",
        "aggregation",
        "last_point",
        "high_cardinality",
        "downsampling",
        "mixed",
        "materialized_view",
        "late_arrival",
    ]
    result = [b for b in ordered if b in seen]
    # Append any unknown benchmark names at the end
    result += sorted(seen - set(ordered))
    return result

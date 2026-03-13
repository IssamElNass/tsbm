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
    Generate a concise, single-database stakeholder report.

    Produces a short, scannable document (~20-30 lines) with one results
    table showing the best result per benchmark, a verdict per row, and
    a 3-line bottom-line summary.
    """
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    databases = sorted({r["database_name"] for r in run_metadata})
    db_name = databases[0] if databases else "Unknown"
    dataset_names = sorted({r["dataset_name"] for r in run_metadata})
    benchmark_names = _ordered_benchmarks(run_metadata)

    # Build run_id → benchmark_name lookup
    _run_to_bench: dict[str, str] = {r["run_id"]: r["benchmark_name"] for r in run_metadata}

    # Group summaries by benchmark
    bench_summaries: dict[str, list[dict[str, Any]]] = {}
    for s in summaries:
        bname = _run_to_bench.get(s.get("run_id", ""), "unknown")
        bench_summaries.setdefault(bname, []).append(s)

    # Estimate total rows from summaries
    total_rows = 0
    for rows in bench_summaries.values():
        for r in rows:
            tr = r.get("total_rows", 0) or 0
            if tr > total_rows:
                total_rows = tr

    lines: list[str] = []

    # ---- Header ----
    lines.append(f"# {db_name} Benchmark Report\n")
    row_label = f"{total_rows:,}" if total_rows else "N/A"
    lines.append(
        f"**Date:** {now} | "
        f"**Dataset:** {', '.join(dataset_names)} | "
        f"**Rows:** {row_label}\n"
    )
    lines.append("---\n")

    # ---- Results Table ----
    lines.append("## Results\n")
    lines.append("| Benchmark | Throughput | p99 Latency | Verdict |")
    lines.append("|---|---|---|---|")

    passed = 0
    total_benchmarks = 0
    peak_rps = 0.0
    query_p99_values: list[float] = []

    for bench_name in benchmark_names:
        bench_rows = bench_summaries.get(bench_name, [])
        if not bench_rows:
            continue

        total_benchmarks += 1
        is_throughput = bench_name in _THROUGHPUT_BENCHMARKS

        # Pick best result across all batch_size/workers combos
        if is_throughput:
            best = max(bench_rows, key=lambda r: r.get("rows_per_second_mean", 0) or 0)
            rps = best.get("rows_per_second_mean", 0) or 0
            p99 = best.get("latency_p99_ms", 0) or 0
            throughput_str = f"{rps:,.0f} r/s"
            latency_str = f"{p99:.1f} ms"
            if rps > peak_rps:
                peak_rps = rps
            verdict = _throughput_verdict(rps)
        else:
            best = min(
                [r for r in bench_rows if r.get("latency_p99_ms") is not None],
                key=lambda r: r["latency_p99_ms"],
                default=None,
            )
            if best is None:
                continue
            p99 = best.get("latency_p99_ms", 0) or 0
            throughput_str = "\u2014"
            latency_str = f"{p99:.1f} ms"
            query_p99_values.append(p99)
            op = best.get("operation", bench_name)
            verdict = _latency_verdict(p99, op)

        if verdict == "OK":
            passed += 1

        lines.append(f"| {bench_name} | {throughput_str} | {latency_str} | {verdict} |")

    lines.append("")

    # ---- Bottom Line ----
    lines.append("## Quick Take\n")
    lines.append("_Thresholds include headroom for Docker network lag and client overhead._\n")
    if peak_rps > 0:
        lines.append(f"- **Peak ingestion:** {peak_rps:,.0f} rows/sec")
    if query_p99_values:
        avg_p99 = sum(query_p99_values) / len(query_p99_values)
        lines.append(f"- **Avg query p99:** {avg_p99:.1f} ms")
    lines.append(f"- **{passed}/{total_benchmarks}** benchmarks looking good")

    # Errors
    completed_runs = sum(1 for r in run_metadata if r.get("completed_at"))
    failed_runs = len(run_metadata) - completed_runs
    if failed_runs > 0:
        lines.append(f"- **{failed_runs} run(s) didn't finish** — worth a look")

    lines.append("")

    text = "\n".join(lines)
    if output is not None:
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(text, encoding="utf-8")
    return text


# ---------------------------------------------------------------------------
# Verdict helpers for stakeholder report
# ---------------------------------------------------------------------------

# Thresholds match cli/run.py — (great, solid, fair)
# Include headroom for Docker-bridge network lag and client deserialization.
_VERDICT_LATENCY_THRESHOLDS: dict[str, tuple[float, float, float]] = {
    "aggregation_1min":       ( 15,   50,    400),
    "aggregation_1h":         ( 35,  150,  1_500),
    "aggregation_1day":       ( 75,  650,  6_500),
    "aggregation_1week":      (150, 1_300, 13_000),
    "high_cardinality_1min":  ( 20,   75,    650),
    "high_cardinality_1h":    ( 75,  400,  4_000),
    "high_cardinality_1day":  (250, 2_000, 18_000),
    "high_cardinality_1week": (650, 6_500, 38_000),
    "time_range_1min":        ( 20,   75,    650),
    "time_range_1h":          ( 75,  400,  4_000),
    "time_range_1day":        (250, 2_000, 18_000),
    "time_range_1week":       (650, 6_500, 38_000),
    "last_point":             (150,  1_300, 13_000),
    "downsampling":           (650, 6_500, 38_000),
    "mixed_read":             (150,  1_300,  6_500),
    "mixed_write":            ( 20,    150,  1_500),
}
_DEFAULT_VERDICT_THRESHOLD: tuple[float, float, float] = (75, 650, 6_500)


def _get_verdict_thresholds(operation: str) -> tuple[float, float, float]:
    if operation in _VERDICT_LATENCY_THRESHOLDS:
        return _VERDICT_LATENCY_THRESHOLDS[operation]
    for key, thresholds in _VERDICT_LATENCY_THRESHOLDS.items():
        if operation.startswith(key):
            return thresholds
    return _DEFAULT_VERDICT_THRESHOLD


def _latency_verdict(p99_ms: float, operation: str) -> str:
    great, solid, fair = _get_verdict_thresholds(operation)
    if p99_ms <= solid:
        return "OK"
    if p99_ms <= fair:
        return "Fair"
    return "Slow"


def _throughput_verdict(rps: float) -> str:
    if rps >= 100_000:
        return "OK"
    if rps >= 20_000:
        return "Fair"
    return "Slow"


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

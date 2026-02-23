"""
TSBench Streamlit Dashboard.

Launch via CLI:
    tsbm dashboard --port 8501

Or directly:
    python -m streamlit run src/tsbm/dashboard/app.py -- results/runs.db

The results DB path is read from sys.argv[1] (after Streamlit's argument
parsing strips its own flags at the ``--`` boundary).
"""
from __future__ import annotations

import sys
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

from tsbm.dashboard.charts import (
    make_comparison_bar,
    make_latency_box,
    make_resource_heatmap,
    make_throughput_line,
)
from tsbm.results.storage import ResultStorage

# ---------------------------------------------------------------------------
# Page config (must be the very first Streamlit call)
# ---------------------------------------------------------------------------

st.set_page_config(
    page_title="TSBench Dashboard",
    page_icon="⏱",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ---------------------------------------------------------------------------
# Metric metadata — labels and one-line descriptions shown in the sidebar
# ---------------------------------------------------------------------------

METRIC_META: dict[str, tuple[str, str]] = {
    "latency_p99_ms":        ("P99 Latency (ms)",     "99% of operations completed faster than this — the practical worst-case tail."),
    "latency_p95_ms":        ("P95 Latency (ms)",     "95th-percentile response time — a good measure of consistent performance."),
    "latency_p50_ms":        ("P50 Latency (ms)",     "Median response time — half of all operations are faster than this."),
    "latency_mean_ms":       ("Mean Latency (ms)",    "Average response time across all operations."),
    "rows_per_second_mean":  ("Rows / sec",           "Average ingestion throughput — higher is better."),
    "mb_per_second_mean":    ("MB / sec",             "Average data transfer rate — higher is better."),
}

# ---------------------------------------------------------------------------
# Resolve results DB path
# ---------------------------------------------------------------------------


def _resolve_db_path() -> Path:
    """Return the results DB path from sys.argv or the default."""
    for arg in sys.argv[1:]:
        p = Path(arg)
        if p.suffix == ".db" or "runs" in arg:
            return p
    return Path("results/runs.db")


# ---------------------------------------------------------------------------
# Data loading (cached)
# ---------------------------------------------------------------------------


@st.cache_resource
def _get_storage(db_path: str) -> ResultStorage:
    storage = ResultStorage(
        sqlite_path=Path(db_path),
        parquet_dir=Path(db_path).parent / "parquet",
    )
    storage.initialize()
    return storage


@st.cache_data(ttl=30)
def _load_runs(db_path: str) -> list[dict[str, Any]]:
    return _get_storage(db_path).list_runs()


@st.cache_data(ttl=30)
def _load_summaries(db_path: str, run_ids_key: str) -> pd.DataFrame:
    run_ids = run_ids_key.split("|") if run_ids_key else None
    rows = _get_storage(db_path).load_summaries(run_ids=run_ids)
    return pd.DataFrame(rows) if rows else pd.DataFrame()


# ---------------------------------------------------------------------------
# Main app
# ---------------------------------------------------------------------------


def main() -> None:
    db_path = str(_resolve_db_path())

    # ---- Header ----
    st.markdown(
        "## ⏱ TSBench — Time-Series Database Benchmark Results",
        help="Comparing QuestDB, CrateDB, and TimescaleDB across ingestion and query workloads.",
    )
    st.caption(f"Results database: `{db_path}`")
    st.divider()

    # ---- Load runs ----
    runs = _load_runs(db_path)
    if not runs:
        st.warning(
            "No benchmark results found. "
            "Run `tsbm run` first to generate results."
        )
        st.stop()

    # ---- Sidebar ----
    with st.sidebar:
        st.header("Filters")

        run_options = {
            f"{r['run_id'][:8]}… — {r['benchmark_name']} / {r['database_name']} "
            f"({r.get('started_at', '')[:10]})": r["run_id"]
            for r in runs
        }
        selected_labels = st.multiselect(
            "Select runs",
            options=list(run_options.keys()),
            default=list(run_options.keys())[:3],
            help="Choose one or more benchmark runs to include in the charts.",
        )
        selected_ids = [run_options[lbl] for lbl in selected_labels]

        st.divider()

        metric_keys = list(METRIC_META.keys())
        metric_display = {k: METRIC_META[k][0] for k in metric_keys}
        primary_metric = st.selectbox(
            "Primary metric",
            options=metric_keys,
            format_func=lambda k: metric_display[k],
            index=0,
            help="The metric shown in the first Performance Comparison chart.",
        )
        st.caption(METRIC_META[primary_metric][1])

        st.divider()
        with st.expander("About TSBench"):
            st.markdown(
                "**TSBench** benchmarks time-series databases "
                "(QuestDB, CrateDB, TimescaleDB) on realistic ingestion "
                "and query workloads.\n\n"
                "Run `tsbm run` to execute benchmarks, then reload this "
                "dashboard to see fresh results."
            )

    if not selected_ids:
        st.info("Select at least one run in the sidebar to see results.")
        st.stop()

    # ---- Load summaries ----
    run_ids_key = "|".join(sorted(selected_ids))
    df = _load_summaries(db_path, run_ids_key)

    if df.empty:
        st.warning("No summary data found for the selected runs.")
        st.stop()

    # ---- Quick stats ----
    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric(
        "Runs selected",
        len(selected_ids),
        help="Number of benchmark runs included in the charts below.",
    )
    col_b.metric(
        "Operations",
        df["operation"].nunique() if "operation" in df.columns else 0,
        help="Distinct operation types measured (e.g. bulk_insert, query_range).",
    )
    col_c.metric(
        "Databases",
        df["database_name"].nunique() if "database_name" in df.columns else 0,
        help="Distinct databases measured across the selected runs.",
    )
    col_d.metric(
        "Total rows ingested",
        f"{df.get('total_rows', pd.Series(dtype=int)).sum():,}",
        help="Sum of all rows written across every run and operation.",
    )

    st.divider()

    # ---- Row 1: Performance comparison bars ----
    st.subheader("Performance Comparison")
    st.caption(
        "Each group of bars represents one operation type. "
        "Bars are coloured by database — shorter bars are better for latency, "
        "taller bars are better for throughput."
    )
    col1, col2 = st.columns(2)
    with col1:
        primary_label = METRIC_META.get(primary_metric, (primary_metric, ""))[0]
        st.plotly_chart(
            make_comparison_bar(df, metric=primary_metric, title=primary_label),
            use_container_width=True,
        )
    with col2:
        throughput_metric = "rows_per_second_mean"
        if throughput_metric in df.columns:
            st.plotly_chart(
                make_comparison_bar(
                    df,
                    metric=throughput_metric,
                    title="Throughput (rows / sec)",
                ),
                use_container_width=True,
            )
        else:
            st.info("No throughput data available for the selected runs.")

    st.divider()

    # ---- Row 2: Latency percentiles ----
    st.subheader("Latency by Percentile")
    st.caption(
        "Compares P50 (median), P95, P99, and P99.9 latencies side by side. "
        "A large gap between P50 and P99 indicates high tail latency — "
        "occasional slow operations that can hurt user-facing performance."
    )
    lat_cols = ["latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "latency_p999_ms"]
    if any(c in df.columns for c in lat_cols):
        st.plotly_chart(make_latency_box(df), use_container_width=True)
    else:
        st.info("No latency percentile data available for the selected runs.")

    st.divider()

    # ---- Row 3: Throughput scaling ----
    if "batch_size" in df.columns and "rows_per_second_mean" in df.columns:
        st.subheader("Throughput Scaling by Batch Size")
        st.caption(
            "Shows how each database's ingestion rate changes as the batch size grows. "
            "A steeper upward slope means the database benefits more from larger batches."
        )
        st.plotly_chart(make_throughput_line(df), use_container_width=True)
        st.divider()

    # ---- Resource monitoring ----
    _render_resource_section(db_path, selected_ids)

    # ---- Raw data table ----
    with st.expander("Raw summary data", expanded=False):
        st.caption("Full summary table for the selected runs. Use the column headers to sort.")
        st.dataframe(df, use_container_width=True)

    # ---- Download buttons ----
    st.divider()
    st.subheader("Export Results")
    st.caption("Download the current selection as a file for offline analysis or sharing.")
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        from tsbm.results.export import export_csv
        csv_text = export_csv(df.to_dict("records"))
        st.download_button(
            "Download CSV",
            data=csv_text,
            file_name="tsbm_results.csv",
            mime="text/csv",
            use_container_width=True,
        )
    with col_dl2:
        from tsbm.results.export import export_markdown
        md_text = export_markdown(df.to_dict("records"), metric=primary_metric)
        st.download_button(
            "Download Markdown",
            data=md_text,
            file_name="tsbm_results.md",
            mime="text/markdown",
            use_container_width=True,
        )


def _render_resource_section(db_path: str, selected_ids: list[str]) -> None:
    """Render the resource monitoring section if data is available."""

    @st.cache_data(ttl=30)
    def _load_resource_df(db_path: str, run_ids_key: str) -> pd.DataFrame:
        rows = _get_storage(db_path).load_summaries(
            run_ids=run_ids_key.split("|") if run_ids_key else None
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        if "resource_cpu_percent_mean" in df.columns:
            return df[df["resource_cpu_percent_mean"].notna()]
        return pd.DataFrame()

    resource_df = _load_resource_df(db_path, "|".join(sorted(selected_ids)))
    if resource_df.empty:
        return

    st.subheader("Resource Usage")
    st.caption(
        "Average CPU and memory consumption per database and operation. "
        "High RSS values may indicate memory pressure; high CPU values "
        "point to compute-heavy workloads."
    )

    if "database_name" in resource_df.columns:
        res_col1, res_col2 = st.columns(2)
        charts = [
            ("resource_cpu_percent_mean", "Avg CPU % per Operation", res_col1),
            ("resource_rss_mb_mean",      "Avg RSS Memory (MB) per Operation", res_col2),
        ]
        for col, chart_title, container in charts:
            if col in resource_df.columns:
                chart_df = resource_df[["database_name", "operation", col]].dropna()
                if not chart_df.empty:
                    with container:
                        st.plotly_chart(
                            make_comparison_bar(
                                chart_df,
                                metric=col,
                                title=chart_title,
                                color_col="database_name",
                            ),
                            use_container_width=True,
                        )
    st.divider()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

main()

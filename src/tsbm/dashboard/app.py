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
# Resolve results DB path
# ---------------------------------------------------------------------------


def _resolve_db_path() -> Path:
    """Return the results DB path from sys.argv or the default."""
    # When launched by ``tsbm dashboard``, sys.argv[1] is the DB path.
    # Streamlit strips its own arguments before running the script, so
    # argv[1] here is the first argument after ``--``.
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
    st.title("⏱ TSBench — Time-Series DB Benchmark Results")
    st.caption(f"Results database: `{db_path}`")

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
            f"{r['run_id']} — {r['benchmark_name']} / {r['database_name']} "
            f"({r.get('started_at', '')[:10]})": r["run_id"]
            for r in runs
        }
        selected_labels = st.multiselect(
            "Select runs",
            options=list(run_options.keys()),
            default=list(run_options.keys())[:3],
        )
        selected_ids = [run_options[lbl] for lbl in selected_labels]

        st.divider()
        metric_options = [
            "latency_p99_ms",
            "latency_p95_ms",
            "latency_p50_ms",
            "latency_mean_ms",
            "rows_per_second_mean",
            "mb_per_second_mean",
        ]
        primary_metric = st.selectbox("Primary metric", metric_options, index=0)

        st.divider()
        st.markdown("**About**")
        st.caption("TSBench benchmarks QuestDB, CrateDB, and TimescaleDB.")

    if not selected_ids:
        st.info("Select at least one run in the sidebar.")
        st.stop()

    # ---- Load summaries ----
    run_ids_key = "|".join(sorted(selected_ids))
    df = _load_summaries(db_path, run_ids_key)

    if df.empty:
        st.warning("No summary data found for the selected runs.")
        st.stop()

    # ---- Quick stats ----
    col_a, col_b, col_c, col_d = st.columns(4)
    col_a.metric("Runs selected", len(selected_ids))
    col_b.metric("Operations", df["operation"].nunique() if "operation" in df.columns else 0)
    col_c.metric("Databases", df["database_name"].nunique() if "database_name" in df.columns else 0)
    col_d.metric("Total rows ingested", f"{df.get('total_rows', pd.Series(dtype=int)).sum():,}")

    st.divider()

    # ---- Row 1: comparison bars ----
    st.subheader("Performance Comparison")
    col1, col2 = st.columns(2)
    with col1:
        st.plotly_chart(
            make_comparison_bar(df, metric=primary_metric),
            use_container_width=True,
        )
    with col2:
        throughput_metric = "rows_per_second_mean"
        if throughput_metric in df.columns:
            st.plotly_chart(
                make_comparison_bar(df, metric=throughput_metric, title="Throughput (rows/sec)"),
                use_container_width=True,
            )
        else:
            st.info("No throughput data available.")

    # ---- Row 2: latency box ----
    st.subheader("Latency Distribution")
    lat_cols = ["latency_p50_ms", "latency_p95_ms", "latency_p99_ms", "latency_p999_ms"]
    if any(c in df.columns for c in lat_cols):
        st.plotly_chart(make_latency_box(df), use_container_width=True)
    else:
        st.info("No latency percentile data available.")

    # ---- Row 3: throughput scaling ----
    if "batch_size" in df.columns and "rows_per_second_mean" in df.columns:
        st.subheader("Throughput Scaling by Batch Size")
        st.plotly_chart(make_throughput_line(df), use_container_width=True)

    # ---- Resource monitoring ----
    _render_resource_section(db_path, selected_ids)

    # ---- Raw data table ----
    with st.expander("Raw summary data", expanded=False):
        st.dataframe(df, use_container_width=True)

    # ---- Download buttons ----
    st.divider()
    st.subheader("Export")
    col_dl1, col_dl2 = st.columns(2)
    with col_dl1:
        from tsbm.results.export import export_csv
        csv_text = export_csv(df.to_dict("records"))
        st.download_button(
            "Download CSV",
            data=csv_text,
            file_name="tsbm_results.csv",
            mime="text/csv",
        )
    with col_dl2:
        from tsbm.results.export import export_markdown
        md_text = export_markdown(df.to_dict("records"), metric=primary_metric)
        st.download_button(
            "Download Markdown",
            data=md_text,
            file_name="tsbm_results.md",
            mime="text/markdown",
        )


def _render_resource_section(db_path: str, selected_ids: list[str]) -> None:
    """Render the resource monitoring section if data is available."""
    resource_cols = [
        "resource_cpu_percent_mean",
        "resource_rss_mb_mean",
        "resource_disk_write_mb_total",
        "resource_net_sent_mb_total",
    ]

    @st.cache_data(ttl=30)
    def _load_resource_df(db_path: str, run_ids_key: str) -> pd.DataFrame:
        rows = _get_storage(db_path).load_summaries(
            run_ids=run_ids_key.split("|") if run_ids_key else None
        )
        df = pd.DataFrame(rows)
        if df.empty:
            return df
        # Keep only rows that have resource data
        if "resource_cpu_percent_mean" in df.columns:
            return df[df["resource_cpu_percent_mean"].notna()]
        return pd.DataFrame()

    resource_df = _load_resource_df(db_path, "|".join(sorted(selected_ids)))
    if resource_df.empty:
        return

    st.subheader("Resource Usage Summary")
    # Show mean CPU and RSS per database
    if "database_name" in resource_df.columns:
        for col in ["resource_cpu_percent_mean", "resource_rss_mb_mean"]:
            if col in resource_df.columns:
                chart_df = resource_df[["database_name", "operation", col]].dropna()
                if not chart_df.empty:
                    st.plotly_chart(
                        make_comparison_bar(
                            chart_df,
                            metric=col,
                            color_col="database_name",
                        ),
                        use_container_width=True,
                    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

main()

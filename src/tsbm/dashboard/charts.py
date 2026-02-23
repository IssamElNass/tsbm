"""
Pure Plotly chart builders for the TSBench dashboard.

No Streamlit imports here — these functions accept pandas DataFrames and
return Plotly Figure objects so they can be tested independently or
embedded in other frontends.

Functions
---------
make_comparison_bar   — grouped bar: databases as colours, operations as x-axis
make_latency_box      — box plot of p50/p95/p99 per database
make_throughput_line  — line chart of rows/sec over iteration index
make_resource_heatmap — CPU% and RSS over sample time
"""
from __future__ import annotations

from typing import Any

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.graph_objects import Figure


# Consistent colour sequence across all charts
_COLOUR_SEQ = px.colors.qualitative.Safe


# ---------------------------------------------------------------------------
# Comparison bar
# ---------------------------------------------------------------------------


def make_comparison_bar(
    df: pd.DataFrame,
    metric: str = "latency_p99_ms",
    title: str | None = None,
    x_col: str = "operation",
    color_col: str = "database_name",
) -> Figure:
    """
    Grouped bar chart comparing *metric* across databases per operation.

    Parameters
    ----------
    df:
        Summary rows DataFrame (from ``ResultStorage.load_summaries()``).
    metric:
        Column to use as bar height.
    title:
        Chart title.  Defaults to the metric name.
    x_col:
        Column for the x-axis categories (default ``"operation"``).
    color_col:
        Column for colour grouping (default ``"database_name"``).
    """
    if df.empty or metric not in df.columns:
        return _empty_figure(f"No data for metric: {metric}")

    agg = (
        df.groupby([x_col, color_col], as_index=False)[metric]
        .mean()
    )
    agg[metric] = agg[metric].round(3)

    fig = px.bar(
        agg,
        x=x_col,
        y=metric,
        color=color_col,
        barmode="group",
        color_discrete_sequence=_COLOUR_SEQ,
        title=title or _pretty(metric),
        labels={metric: _pretty(metric), x_col: "Operation", color_col: "Database"},
        text_auto=".2f",
    )
    fig.update_layout(
        legend_title_text="Database",
        xaxis_tickangle=-30,
        plot_bgcolor="white",
        paper_bgcolor="white",
        margin=dict(t=50, b=80),
    )
    fig.update_yaxes(gridcolor="#e5e5e5")
    return fig


# ---------------------------------------------------------------------------
# Latency box
# ---------------------------------------------------------------------------


def make_latency_box(
    df: pd.DataFrame,
    color_col: str = "database_name",
    facet_col: str | None = "operation",
) -> Figure:
    """
    Box plot of p50 / p95 / p99 / p99.9 latencies per database.

    Plots each percentile as a separate trace so all four can be compared
    side-by-side.
    """
    latency_cols = [
        ("p50_ms",  "latency_p50_ms"),
        ("p95_ms",  "latency_p95_ms"),
        ("p99_ms",  "latency_p99_ms"),
        ("p99.9_ms", "latency_p999_ms"),
    ]

    available = [
        (label, col)
        for label, col in latency_cols
        if col in df.columns
    ]
    if df.empty or not available:
        return _empty_figure("No latency data available")

    # Melt to long form
    melt_df = df.melt(
        id_vars=[color_col, "operation"] if "operation" in df.columns else [color_col],
        value_vars=[col for _, col in available],
        var_name="percentile",
        value_name="latency_ms",
    )
    # Rename variable labels
    label_map = {col: label for label, col in available}
    melt_df["percentile"] = melt_df["percentile"].map(label_map)

    facet_kw: dict[str, Any] = {}
    if facet_col and facet_col in df.columns:
        facet_kw = {"facet_col": facet_col, "facet_col_wrap": 3}

    fig = px.box(
        melt_df,
        x="percentile",
        y="latency_ms",
        color=color_col,
        color_discrete_sequence=_COLOUR_SEQ,
        title="Latency Distribution by Percentile",
        labels={"latency_ms": "Latency (ms)", "percentile": "Percentile", color_col: "Database"},
        **facet_kw,
    )
    fig.update_layout(
        legend_title_text="Database",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_yaxes(gridcolor="#e5e5e5")
    return fig


# ---------------------------------------------------------------------------
# Throughput line
# ---------------------------------------------------------------------------


def make_throughput_line(
    df: pd.DataFrame,
    y_col: str = "rows_per_second_mean",
    color_col: str = "database_name",
    facet_col: str | None = "operation",
) -> Figure:
    """
    Line chart of throughput (rows/sec) ordered by batch_size.

    Uses *batch_size* on the x-axis so you can see how each database scales.
    """
    if df.empty or y_col not in df.columns:
        return _empty_figure(f"No throughput data for column: {y_col}")

    x_col = "batch_size" if "batch_size" in df.columns else df.columns[0]

    facet_kw: dict[str, Any] = {}
    if facet_col and facet_col in df.columns:
        facet_kw = {"facet_col": facet_col, "facet_col_wrap": 3}

    fig = px.line(
        df.sort_values(x_col),
        x=x_col,
        y=y_col,
        color=color_col,
        markers=True,
        color_discrete_sequence=_COLOUR_SEQ,
        title="Throughput vs Batch Size",
        labels={
            y_col: "Rows / sec",
            x_col: "Batch size",
            color_col: "Database",
        },
        **facet_kw,
    )
    fig.update_layout(
        legend_title_text="Database",
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    fig.update_yaxes(gridcolor="#e5e5e5")
    fig.update_xaxes(gridcolor="#e5e5e5")
    return fig


# ---------------------------------------------------------------------------
# Resource heatmap
# ---------------------------------------------------------------------------


def make_resource_heatmap(
    df: pd.DataFrame,
    time_col: str = "timestamp_ns",
) -> Figure:
    """
    Dual-axis chart of system CPU% and RSS (MB) over benchmark time.

    Expects a DataFrame of ``ResourceSample`` rows (or a subset of the
    columns ``timestamp_ns``, ``cpu_percent``, ``rss_mb``).
    """
    required = {time_col, "cpu_percent", "rss_mb"}
    if df.empty or not required.issubset(df.columns):
        return _empty_figure(
            "No resource data available\n"
            "(requires ResourceMonitor to be enabled)"
        )

    # Convert ns to seconds for readability
    df = df.copy()
    t0 = df[time_col].min()
    df["time_s"] = (df[time_col] - t0) / 1e9

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["time_s"],
        y=df["cpu_percent"],
        name="CPU %",
        mode="lines",
        line=dict(color=_COLOUR_SEQ[0], width=2),
        yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        x=df["time_s"],
        y=df["rss_mb"],
        name="RSS MB",
        mode="lines",
        line=dict(color=_COLOUR_SEQ[1], width=2, dash="dot"),
        yaxis="y2",
    ))

    fig.update_layout(
        title="System Resources During Benchmark",
        xaxis_title="Time (s)",
        yaxis=dict(title="CPU %", gridcolor="#e5e5e5"),
        yaxis2=dict(
            title="RSS (MB)",
            overlaying="y",
            side="right",
            gridcolor="#e5e5e5",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _empty_figure(msg: str) -> Figure:
    fig = go.Figure()
    fig.add_annotation(
        text=msg,
        xref="paper",
        yref="paper",
        x=0.5,
        y=0.5,
        showarrow=False,
        font=dict(size=16, color="#888"),
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="white",
        paper_bgcolor="white",
    )
    return fig


def _pretty(col: str) -> str:
    """Convert snake_case column name to a human-readable label."""
    return col.replace("_", " ").replace("ms", "(ms)").replace("mean", "avg").title()

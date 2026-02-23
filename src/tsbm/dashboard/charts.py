"""
Pure Plotly chart builders for the TSBench dashboard.

No Streamlit imports here — these functions accept pandas DataFrames and
return Plotly Figure objects so they can be tested independently or
embedded in other frontends.

Functions
---------
make_comparison_bar   — grouped bar: databases as colours, operations as x-axis
make_latency_box      — grouped bar of p50/p95/p99 per database per percentile
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

# Human-readable label map
_PRETTY_MAP: dict[str, str] = {
    "latency_p50_ms":               "P50 Latency (ms)",
    "latency_p95_ms":               "P95 Latency (ms)",
    "latency_p99_ms":               "P99 Latency (ms)",
    "latency_p999_ms":              "P99.9 Latency (ms)",
    "latency_mean_ms":              "Mean Latency (ms)",
    "rows_per_second_mean":         "Rows / sec",
    "mb_per_second_mean":           "MB / sec",
    "resource_cpu_percent_mean":    "Avg CPU %",
    "resource_rss_mb_mean":         "Avg RSS (MB)",
    "resource_disk_write_mb_total": "Disk Write (MB)",
    "resource_net_sent_mb_total":   "Net Sent (MB)",
    "batch_size":                   "Batch Size",
    "operation":                    "Operation",
    "database_name":                "Database",
    "p50_ms":                       "P50",
    "p95_ms":                       "P95",
    "p99_ms":                       "P99",
    "p99.9_ms":                     "P99.9",
}

# Base layout applied to every figure
_BASE_LAYOUT: dict[str, Any] = dict(
    template="plotly_white",
    font=dict(family="Inter, system-ui, -apple-system, sans-serif", size=13),
    title_font=dict(size=15, color="#1f2937"),
    legend=dict(
        bgcolor="rgba(255,255,255,0.9)",
        bordercolor="#e5e7eb",
        borderwidth=1,
    ),
    hoverlabel=dict(
        bgcolor="white",
        font_size=13,
        font_family="Inter, system-ui, sans-serif",
        bordercolor="#e5e7eb",
    ),
    margin=dict(t=65, b=70, l=65, r=25),
)


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

    y_label = _pretty(metric)
    chart_title = title or y_label

    fig = px.bar(
        agg,
        x=x_col,
        y=metric,
        color=color_col,
        barmode="group",
        color_discrete_sequence=_COLOUR_SEQ,
        title=chart_title,
        labels={
            metric: y_label,
            x_col: _pretty(x_col),
            color_col: _pretty(color_col),
        },
        text_auto=".2f",
    )
    fig.update_traces(
        textposition="outside",
        textfont_size=11,
        cliponaxis=False,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "%{fullData.name}: <b>%{y:.3g}</b><br>"
            "<extra></extra>"
        ),
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        legend_title_text=_pretty(color_col),
        xaxis_tickangle=-25,
        height=420,
        bargap=0.25,
        bargroupgap=0.06,
        yaxis_title=y_label,
        xaxis_title="",
    )
    fig.update_yaxes(gridcolor="#f0f0f0", zeroline=True, zerolinecolor="#d1d5db")
    return fig


# ---------------------------------------------------------------------------
# Latency percentile comparison bar
# ---------------------------------------------------------------------------


def make_latency_box(
    df: pd.DataFrame,
    color_col: str = "database_name",
    facet_col: str | None = "operation",
) -> Figure:
    """
    Grouped bar chart comparing latency percentiles (p50/p95/p99/p99.9) per database.

    Since the dashboard works with pre-aggregated summary statistics, this renders
    each percentile as a separate bar group so you can compare both percentile
    levels and databases at a glance.

    Parameters
    ----------
    df:
        Summary rows DataFrame with latency_p50_ms … latency_p999_ms columns.
    color_col:
        Column for colour grouping (default ``"database_name"``).
    facet_col:
        Column used to split into facet panels (default ``"operation"``).
        Pass ``None`` to disable faceting.
    """
    latency_cols = [
        ("P50",   "latency_p50_ms"),
        ("P95",   "latency_p95_ms"),
        ("P99",   "latency_p99_ms"),
        ("P99.9", "latency_p999_ms"),
    ]

    available = [
        (label, col)
        for label, col in latency_cols
        if col in df.columns
    ]
    if df.empty or not available:
        return _empty_figure("No latency data available")

    id_vars = [color_col]
    if "operation" in df.columns:
        id_vars.append("operation")

    melt_df = df.melt(
        id_vars=id_vars,
        value_vars=[col for _, col in available],
        var_name="percentile_col",
        value_name="latency_ms",
    )
    label_map = {col: label for label, col in available}
    melt_df["percentile"] = melt_df["percentile_col"].map(label_map)
    # Preserve percentile order
    melt_df["percentile"] = pd.Categorical(
        melt_df["percentile"], categories=[lbl for lbl, _ in available], ordered=True
    )
    melt_df = melt_df.dropna(subset=["latency_ms"])

    if facet_col and facet_col in df.columns:
        agg = melt_df.groupby([facet_col, "percentile", color_col], as_index=False, observed=True)["latency_ms"].mean()
        agg["latency_ms"] = agg["latency_ms"].round(3)
        facet_kw: dict[str, Any] = {"facet_col": facet_col, "facet_col_wrap": 3}
    else:
        agg = melt_df.groupby(["percentile", color_col], as_index=False, observed=True)["latency_ms"].mean()
        agg["latency_ms"] = agg["latency_ms"].round(3)
        facet_kw = {}

    fig = px.bar(
        agg,
        x="percentile",
        y="latency_ms",
        color=color_col,
        barmode="group",
        color_discrete_sequence=_COLOUR_SEQ,
        title="Latency by Percentile",
        labels={
            "latency_ms": "Latency (ms)",
            "percentile": "Percentile",
            color_col: _pretty(color_col),
        },
        text_auto=".2f",
        **facet_kw,
    )
    fig.update_traces(
        textposition="outside",
        textfont_size=10,
        cliponaxis=False,
        hovertemplate=(
            "<b>%{x}</b><br>"
            "%{fullData.name}: <b>%{y:.3g} ms</b><br>"
            "<extra></extra>"
        ),
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        legend_title_text=_pretty(color_col),
        height=440,
        bargap=0.2,
        bargroupgap=0.06,
        yaxis_title="Latency (ms)",
        xaxis_title="",
    )
    fig.update_yaxes(gridcolor="#f0f0f0", zeroline=True, zerolinecolor="#d1d5db")
    # Clean up facet labels
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
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
    y_label = _pretty(y_col)

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
        title="Throughput Scaling by Batch Size",
        labels={
            y_col: y_label,
            x_col: _pretty(x_col),
            color_col: _pretty(color_col),
        },
        **facet_kw,
    )
    fig.update_traces(
        line_width=2.5,
        marker_size=8,
        hovertemplate=(
            "<b>%{fullData.name}</b><br>"
            "Batch size: %{x:,}<br>"
            f"{y_label}: <b>%{{y:,.0f}}</b><br>"
            "<extra></extra>"
        ),
    )
    fig.update_layout(
        **_BASE_LAYOUT,
        legend_title_text=_pretty(color_col),
        height=420,
        yaxis_title=y_label,
        xaxis_title=_pretty(x_col),
    )
    fig.update_yaxes(gridcolor="#f0f0f0", zeroline=False)
    fig.update_xaxes(gridcolor="#f0f0f0")
    fig.for_each_annotation(lambda a: a.update(text=a.text.split("=")[-1]))
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
            "No resource timeline data available\n"
            "(requires ResourceMonitor to be enabled)"
        )

    df = df.copy()
    t0 = df[time_col].min()
    df["time_s"] = (df[time_col] - t0) / 1e9

    cpu_color = _COLOUR_SEQ[0]
    rss_color = _COLOUR_SEQ[2]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=df["time_s"],
        y=df["cpu_percent"],
        name="CPU %",
        mode="lines",
        line=dict(color=cpu_color, width=2.5),
        hovertemplate="Time: %{x:.1f}s<br>CPU: <b>%{y:.1f}%</b><extra></extra>",
        yaxis="y1",
    ))
    fig.add_trace(go.Scatter(
        x=df["time_s"],
        y=df["rss_mb"],
        name="RSS Memory (MB)",
        mode="lines",
        line=dict(color=rss_color, width=2.5, dash="dot"),
        hovertemplate="Time: %{x:.1f}s<br>RSS: <b>%{y:.0f} MB</b><extra></extra>",
        yaxis="y2",
    ))

    fig.update_layout(
        **_BASE_LAYOUT,
        title="System Resources During Benchmark",
        xaxis=dict(title="Time (seconds)", gridcolor="#f0f0f0"),
        yaxis=dict(
            title="CPU %",
            titlefont=dict(color=cpu_color),
            tickfont=dict(color=cpu_color),
            gridcolor="#f0f0f0",
        ),
        yaxis2=dict(
            title="RSS Memory (MB)",
            titlefont=dict(color=rss_color),
            tickfont=dict(color=rss_color),
            overlaying="y",
            side="right",
            gridcolor="rgba(0,0,0,0)",
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        height=380,
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
        font=dict(size=15, color="#9ca3af"),
        align="center",
    )
    fig.update_layout(
        xaxis=dict(visible=False),
        yaxis=dict(visible=False),
        plot_bgcolor="white",
        paper_bgcolor="#fafafa",
        height=280,
        margin=dict(t=20, b=20, l=20, r=20),
    )
    return fig


def _pretty(col: str) -> str:
    """Convert a column name to a human-readable label."""
    if col in _PRETTY_MAP:
        return _PRETTY_MAP[col]
    return col.replace("_", " ").title()

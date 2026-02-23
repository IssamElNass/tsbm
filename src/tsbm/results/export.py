"""
Result export utilities.

Supports three output formats from the summaries table:
  * CSV   — plain comma-separated, suitable for spreadsheets
  * JSON  — pretty-printed array of objects
  * Markdown — comparison table with databases as columns
"""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path
from typing import Any


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

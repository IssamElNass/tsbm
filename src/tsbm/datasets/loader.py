"""
Dataset loading: CSV, Parquet, and JSON → BenchmarkDataset.

Auto-inference rules
--------------------
* **Timestamp** — column whose PyArrow type is already temporal, *or* whose
  name matches a common timestamp alias (``time``, ``ts``, ``timestamp``,
  ``datetime``, ``date``), *or* a string/integer column that can be parsed
  as a timestamp.
* **TAG** — string column whose unique-value ratio is below
  ``tag_cardinality_threshold`` (default 0.05 from settings).
* **METRIC** — numeric (int / float) column.
* **OTHER** — everything else (high-cardinality strings, booleans, etc.).

The first timestamp column found becomes the primary ``timestamp_col``.
If ``schema_hint`` is provided, its column roles override inference.
"""
from __future__ import annotations

import json
import logging
import warnings
from pathlib import Path
from typing import Iterator, Sequence

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.csv as pa_csv
import pyarrow.json as pa_json
import pyarrow.parquet as pq

from tsbm.datasets.schema import (
    BenchmarkDataset,
    ColumnRole,
    ColumnSpec,
    DatasetSchema,
    _apply_unit_conversions_table,
)
from tsbm.datasets.timestamps import normalize_timestamp_column
from tsbm.exceptions import DatasetError, SchemaError

logger = logging.getLogger(__name__)

SUPPORTED_SUFFIXES: frozenset[str] = frozenset(
    {".csv", ".parquet", ".parq", ".json", ".jsonl", ".ndjson"}
)

# Column names that strongly suggest a timestamp role
_TS_NAME_HINTS: frozenset[str] = frozenset(
    {"time", "ts", "timestamp", "datetime", "date", "created_at", "event_time"}
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_dataset(
    path: Path,
    schema_hint: DatasetSchema | None = None,
    tag_cardinality_threshold: float = 0.05,
    unit_conversions: dict[str, tuple[str, str]] | None = None,
) -> BenchmarkDataset:
    """
    Load a dataset file into a :class:`BenchmarkDataset`.

    Parameters
    ----------
    path:
        Path to a CSV, Parquet, or JSON/JSONL file.
    schema_hint:
        If provided, its ``timestamp_col``, ``tag_cols``, and ``metric_cols``
        are used directly (column roles are not re-inferred).
    tag_cardinality_threshold:
        Fraction of total rows below which a string column is classed as TAG.
        Ignored when ``schema_hint`` is provided.
    unit_conversions:
        Optional mapping of column name to (from_unit, to_unit).
        Applied after timestamp normalisation.
        Example: ``{"temperature": ("celsius", "fahrenheit")}``

    Returns
    -------
    BenchmarkDataset
        With a fully normalised Arrow table (timestamp column in ns/UTC).
    """
    path = Path(path)
    if not path.exists():
        raise DatasetError(f"Dataset file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise DatasetError(
            f"Unsupported file format {suffix!r}. "
            f"Supported: {sorted(SUPPORTED_SUFFIXES)}"
        )

    table = _read_file(path, suffix)

    if schema_hint is not None:
        schema = _apply_hint(table, schema_hint, path)
    else:
        schema = _infer_schema(table, path.stem, tag_cardinality_threshold)

    # Normalise the timestamp column to timestamp[ns, UTC]
    table = normalize_timestamp_column(table, schema.timestamp_col)

    if unit_conversions:
        table = _apply_unit_conversions_table(table, unit_conversions)

    schema.row_count = len(table)

    return BenchmarkDataset(schema=schema, table=table, source_path=path)


def load_dataset_streaming(
    path: Path,
    schema_hint: DatasetSchema,
    chunk_size: int = 100_000,
) -> BenchmarkDataset:
    """
    Return a :class:`BenchmarkDataset` in streaming mode for large files.

    The file is **not** read into memory — only schema metadata is loaded.
    Data is accessed batch-by-batch via :meth:`BenchmarkDataset.iter_batches`.

    Parameters
    ----------
    path:
        Path to a CSV or Parquet file.  JSONL is supported but not recommended
        for files > 1M rows (no native streaming; loaded line-by-line).
    schema_hint:
        **Required.**  Streaming mode cannot perform cardinality-based TAG
        inference without a full column scan.  Provide a ``DatasetSchema``
        with ``timestamp_col``, ``tag_cols``, and ``metric_cols`` set.
    chunk_size:
        Number of rows per batch yielded by ``iter_batches()``.

    Returns
    -------
    BenchmarkDataset
        With ``streaming=True`` and ``table=None``.
    """
    path = Path(path)
    if not path.exists():
        raise DatasetError(f"Dataset file not found: {path}")

    suffix = path.suffix.lower()
    if suffix not in SUPPORTED_SUFFIXES:
        raise DatasetError(
            f"Unsupported file format {suffix!r}. "
            f"Supported: {sorted(SUPPORTED_SUFFIXES)}"
        )

    # Infer row count cheaply where possible (Parquet metadata)
    row_count = 0
    if suffix in {".parquet", ".parq"}:
        try:
            pf = pq.ParquetFile(path)
            row_count = pf.metadata.num_rows
        except Exception:
            row_count = 0

    schema = DatasetSchema(
        name=schema_hint.name,
        timestamp_col=schema_hint.timestamp_col,
        tag_cols=list(schema_hint.tag_cols),
        metric_cols=list(schema_hint.metric_cols),
        columns=list(schema_hint.columns),
        row_count=row_count,
    )

    logger.info(
        "Streaming dataset %r (%s rows, chunk_size=%d)",
        path.name,
        row_count or "unknown",
        chunk_size,
    )

    return BenchmarkDataset(
        schema=schema,
        table=None,
        source_path=path,
        streaming=True,
        chunk_size=chunk_size,
    )


def apply_unit_conversions(
    table: pa.Table,
    conversions: dict[str, tuple[str, str]],
) -> pa.Table:
    """
    Apply unit conversions to an Arrow table and return the result.

    Parameters
    ----------
    table:
        Arrow table to transform.
    conversions:
        Mapping of column name → (from_unit, to_unit).
        Example: ``{"temperature": ("celsius", "fahrenheit"), "pressure": ("pa", "bar")}``

    Returns
    -------
    pa.Table
        New table with converted columns (original is unchanged).
    """
    return _apply_unit_conversions_table(table, conversions)


# ---------------------------------------------------------------------------
# Streaming helpers (used by BenchmarkDataset.iter_batches)
# ---------------------------------------------------------------------------


def _iter_dataset_batches(
    path: Path,
    chunk_size: int = 100_000,
) -> Iterator[pa.RecordBatch]:
    """
    Yield :class:`pa.RecordBatch` objects from *path* without loading the full
    file into memory.

    Timestamp normalisation is applied per-batch.  The caller is responsible
    for schema validation.

    Supported formats
    -----------------
    * ``.parquet`` / ``.parq`` — uses :func:`pq.ParquetFile.iter_batches`
    * ``.csv`` — uses :func:`pa_csv.open_csv` (streaming reader)
    * ``.json`` / ``.jsonl`` / ``.ndjson`` — line-by-line buffered read
      (warns that Parquet is preferred for large files)
    """
    suffix = path.suffix.lower()

    if suffix in {".parquet", ".parq"}:
        yield from _iter_parquet_batches(path, chunk_size)
    elif suffix == ".csv":
        yield from _iter_csv_batches(path, chunk_size)
    else:
        warnings.warn(
            f"Streaming JSON files ({path.name}) is slow for large datasets. "
            "Consider converting to Parquet for better performance.",
            stacklevel=3,
        )
        yield from _iter_json_batches(path, chunk_size)


def _iter_parquet_batches(
    path: Path,
    batch_size: int,
) -> Iterator[pa.RecordBatch]:
    """Memory-efficient Parquet reader using per-batch iteration."""
    pf = pq.ParquetFile(path)
    for batch in pf.iter_batches(batch_size=batch_size):
        yield batch


def _iter_csv_batches(
    path: Path,
    chunk_size: int,
) -> Iterator[pa.RecordBatch]:
    """
    Streaming CSV reader using :func:`pa_csv.open_csv`.

    PyArrow's CSV streaming reader yields one RecordBatch per internal
    block; we accumulate rows and yield when we reach *chunk_size*.
    """
    opts = pa_csv.ConvertOptions(
        timestamp_parsers=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
    )
    reader = pa_csv.open_csv(path, convert_options=opts)
    buffer: list[pa.RecordBatch] = []
    buffered_rows = 0

    while True:
        try:
            batch = reader.read_next_batch()
        except StopIteration:
            break
        buffer.append(batch)
        buffered_rows += len(batch)

        while buffered_rows >= chunk_size:
            combined = pa.Table.from_batches(buffer).to_batches(max_chunksize=chunk_size)
            for out_batch in combined:
                if len(out_batch) >= chunk_size:
                    yield out_batch
                    buffered_rows -= len(out_batch)
                # Keep remainder in buffer for next iteration
            # Reset buffer to whatever is left
            remaining_rows = buffered_rows
            if remaining_rows > 0:
                # Rebuild buffer from the tail of combined
                tail_batches = [b for b in combined if len(b) < chunk_size]
                buffer = tail_batches
                buffered_rows = sum(len(b) for b in buffer)
            else:
                buffer = []
                buffered_rows = 0
            break

    if buffer:
        yield from pa.Table.from_batches(buffer).to_batches(max_chunksize=chunk_size)


def _iter_json_batches(
    path: Path,
    chunk_size: int,
) -> Iterator[pa.RecordBatch]:
    """
    Line-by-line JSONL reader that buffers *chunk_size* records before yielding.
    Falls back to full-file load for JSON array format.
    """
    raw = path.read_bytes().lstrip()
    if raw and raw[0:1] == b"[":
        # JSON array — no choice but to load fully
        records: list[dict] = json.loads(raw)
        table = pa.Table.from_pylist(records)
        yield from table.to_batches(max_chunksize=chunk_size)
        return

    # JSONL: read line-by-line
    buffer: list[dict] = []
    with path.open("rb") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            buffer.append(json.loads(line))
            if len(buffer) >= chunk_size:
                yield pa.Table.from_pylist(buffer).to_batches(max_chunksize=chunk_size)[0]
                buffer = []
    if buffer:
        yield pa.Table.from_pylist(buffer).to_batches(max_chunksize=chunk_size)[0]


# ---------------------------------------------------------------------------
# File readers
# ---------------------------------------------------------------------------


def _read_file(path: Path, suffix: str) -> pa.Table:
    try:
        if suffix == ".csv":
            return _read_csv(path)
        if suffix in {".parquet", ".parq"}:
            return pq.read_table(path)
        # JSON variants
        return _read_json(path)
    except Exception as exc:
        raise DatasetError(f"Failed to read {path}: {exc}") from exc


def _read_csv(path: Path) -> pa.Table:
    opts = pa_csv.ConvertOptions(
        timestamp_parsers=[
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ]
    )
    return pa_csv.read_csv(path, convert_options=opts)


def _read_json(path: Path) -> pa.Table:
    """Read newline-delimited JSON or a single JSON array."""
    raw = path.read_bytes()
    # Detect array-style JSON: first non-whitespace byte is '['
    stripped = raw.lstrip()
    if stripped and stripped[0:1] == b"[":
        # Single JSON array → parse via stdlib json, convert to Arrow
        records: list[dict] = json.loads(raw)
        return pa.Table.from_pylist(records)
    # Newline-delimited JSON (JSONL / NDJSON)
    return pa_json.read_json(path)


# ---------------------------------------------------------------------------
# Schema inference
# ---------------------------------------------------------------------------


def _infer_schema(
    table: pa.Table,
    dataset_name: str,
    tag_threshold: float,
) -> DatasetSchema:
    """
    Classify every column in *table* and return a :class:`DatasetSchema`.
    """
    n_rows = len(table)
    columns: list[ColumnSpec] = []
    timestamp_col: str | None = None
    tag_cols: list[str] = []
    metric_cols: list[str] = []

    for field in table.schema:
        name = field.name
        arrow_type = field.type
        role = _classify_column(
            name=name,
            arrow_type=arrow_type,
            col=table.column(name),
            n_rows=n_rows,
            tag_threshold=tag_threshold,
            has_timestamp_already=timestamp_col is not None,
        )

        columns.append(ColumnSpec(name=name, role=role, arrow_type=arrow_type))

        if role == ColumnRole.TIMESTAMP and timestamp_col is None:
            timestamp_col = name
        elif role == ColumnRole.TAG:
            tag_cols.append(name)
        elif role == ColumnRole.METRIC:
            metric_cols.append(name)

    if timestamp_col is None:
        raise SchemaError(
            f"No timestamp column found in dataset {dataset_name!r}. "
            "Rename a column to 'timestamp', 'ts', 'time', or 'datetime', "
            "or provide a schema_hint."
        )

    return DatasetSchema(
        name=dataset_name,
        timestamp_col=timestamp_col,
        tag_cols=tag_cols,
        metric_cols=metric_cols,
        columns=columns,
        row_count=n_rows,
    )


def _classify_column(
    name: str,
    arrow_type: pa.DataType,
    col: pa.ChunkedArray,
    n_rows: int,
    tag_threshold: float,
    has_timestamp_already: bool,
) -> ColumnRole:
    """Return the ColumnRole for a single column."""

    # --- Timestamp detection ---
    if not has_timestamp_already:
        if pa.types.is_timestamp(arrow_type) or pa.types.is_date(arrow_type):
            return ColumnRole.TIMESTAMP
        if name.lower() in _TS_NAME_HINTS:
            return ColumnRole.TIMESTAMP

    # --- Numeric → METRIC ---
    if pa.types.is_integer(arrow_type) or pa.types.is_floating(arrow_type):
        return ColumnRole.METRIC

    # --- Boolean → OTHER (not a metric, not a tag in the TS sense) ---
    if pa.types.is_boolean(arrow_type):
        return ColumnRole.OTHER

    # --- String → TAG or OTHER based on cardinality ---
    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        if n_rows == 0:
            return ColumnRole.TAG
        n_unique = pc.count_distinct(col).as_py()
        ratio = n_unique / n_rows
        return ColumnRole.TAG if ratio < tag_threshold else ColumnRole.OTHER

    return ColumnRole.OTHER


# ---------------------------------------------------------------------------
# Apply a schema hint
# ---------------------------------------------------------------------------


def _apply_hint(
    table: pa.Table,
    hint: DatasetSchema,
    path: Path,
) -> DatasetSchema:
    """
    Build a DatasetSchema from *hint*, validating that the referenced
    columns actually exist in *table*.
    """
    missing = [
        c for c in [hint.timestamp_col] + hint.tag_cols + hint.metric_cols
        if c not in table.schema.names
    ]
    if missing:
        raise SchemaError(
            f"Schema hint references columns not present in {path.name}: {missing}"
        )

    columns: list[ColumnSpec] = []
    for field in table.schema:
        name = field.name
        if name == hint.timestamp_col:
            role = ColumnRole.TIMESTAMP
        elif name in hint.tag_cols:
            role = ColumnRole.TAG
        elif name in hint.metric_cols:
            role = ColumnRole.METRIC
        else:
            role = ColumnRole.OTHER
        columns.append(ColumnSpec(name=name, role=role, arrow_type=field.type))

    return DatasetSchema(
        name=hint.name,
        timestamp_col=hint.timestamp_col,
        tag_cols=hint.tag_cols,
        metric_cols=hint.metric_cols,
        columns=columns,
        row_count=len(table),
    )

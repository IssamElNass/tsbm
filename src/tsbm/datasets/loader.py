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

import glob as globmod
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


def override_timestamp_col(
    dataset: BenchmarkDataset,
    col_name: str,
) -> BenchmarkDataset:
    """
    Override the auto-detected timestamp column in a :class:`BenchmarkDataset`.

    Use this when the dataset's timestamp column has a non-standard name that
    was not picked up by auto-detection (name hints or PyArrow type).

    The previously detected timestamp column is demoted to role ``OTHER``.
    The new column is promoted to ``TIMESTAMP`` and normalised to
    ``timestamp[ns, tz=UTC]`` if the table is already in memory.

    Parameters
    ----------
    dataset:
        The loaded (or streaming) dataset to modify in-place.
    col_name:
        Name of the column to use as the primary timestamp.

    Raises
    ------
    SchemaError
        If *col_name* is not present in the dataset schema.
    """
    col_names = [c.name for c in dataset.schema.columns]
    if col_name not in col_names:
        raise SchemaError(
            f"Timestamp column override {col_name!r} not found in dataset. "
            f"Available columns: {col_names}"
        )
    if dataset.schema.timestamp_col == col_name:
        return dataset  # already correct — nothing to do

    old_ts_name = dataset.schema.timestamp_col

    # Re-classify columns: demote old timestamp, promote new one
    new_columns = []
    for spec in dataset.schema.columns:
        if spec.name == old_ts_name:
            new_columns.append(ColumnSpec(spec.name, ColumnRole.OTHER, spec.arrow_type))
        elif spec.name == col_name:
            new_columns.append(ColumnSpec(spec.name, ColumnRole.TIMESTAMP, spec.arrow_type))
        else:
            new_columns.append(spec)

    dataset.schema.timestamp_col = col_name
    dataset.schema.columns = new_columns

    # Normalise the new timestamp column if the table is already in memory.
    # Streaming datasets have table=None; they are normalised chunk-by-chunk during
    # ingestion/iteration so no action is needed here.
    if dataset.table is not None:
        dataset.table = normalize_timestamp_column(dataset.table, col_name)

    logger.info("Timestamp column overridden: %r → %r", old_ts_name, col_name)
    return dataset


def estimate_row_count(path: Path) -> int:
    """
    Cheaply estimate the number of rows in *path* without reading the full file.

    * **Parquet** — exact count from file metadata (near-instant).
    * **CSV / other** — rough estimate based on file size assuming ~80 bytes
      per row on disk.  Good enough to decide whether streaming is warranted.
    """
    path = Path(path)
    suffix = path.suffix.lower()
    if suffix in {".parquet", ".parq"}:
        try:
            return pq.ParquetFile(path).metadata.num_rows
        except Exception:
            pass
    try:
        return path.stat().st_size // 80
    except Exception:
        return 0


def infer_schema_from_sample(
    path: Path,
    sample_rows: int = 50_000,
    tag_cardinality_threshold: float = 0.05,
) -> DatasetSchema:
    """
    Read the first *sample_rows* rows of *path* and infer a
    :class:`DatasetSchema` — without loading the whole file.

    Used by the run orchestrator to get a ``schema_hint`` before switching to
    streaming mode for large datasets.

    Parameters
    ----------
    path:
        CSV or Parquet dataset file.
    sample_rows:
        Maximum rows to read for schema and cardinality inference.
    tag_cardinality_threshold:
        Fraction of sample rows below which a string column is classed as TAG.
    """
    path = Path(path)
    suffix = path.suffix.lower()

    if suffix in {".parquet", ".parq"}:
        batch = next(pq.ParquetFile(path).iter_batches(batch_size=sample_rows), None)
        if batch is None:
            raise DatasetError(f"Parquet file {path} appears to be empty")
        sample: pa.Table = pa.Table.from_batches([batch])
    elif suffix == ".csv":
        opts = pa_csv.ConvertOptions(
            timestamp_parsers=[
                "%Y-%m-%dT%H:%M:%S%z",
                "%Y-%m-%dT%H:%M:%SZ",
                "%Y-%m-%d %H:%M:%S",
                "%Y-%m-%d",
            ]
        )
        reader = pa_csv.open_csv(path, convert_options=opts)
        batches: list[pa.RecordBatch] = []
        n_read = 0
        while n_read < sample_rows:
            try:
                b = reader.read_next_batch()
                batches.append(b)
                n_read += len(b)
            except StopIteration:
                break
        if not batches:
            raise DatasetError(f"CSV file {path} appears to be empty")
        sample = pa.Table.from_batches(batches).slice(0, min(sample_rows, n_read))
    else:
        # JSON / other formats are usually small — load fully
        sample = _read_file(path, suffix)

    schema = _infer_schema(sample, path.stem, tag_cardinality_threshold)
    # Normalise timestamp so downstream adapters see consistent types
    sample = normalize_timestamp_column(sample, schema.timestamp_col)
    # Re-infer after normalisation to capture updated types
    return _infer_schema(sample, path.stem, tag_cardinality_threshold)


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
# Multi-source & Azure helpers
# ---------------------------------------------------------------------------


_AZURE_URL_PREFIXES = ("az://", "abfs://", "abfss://")
_AZURE_BLOB_HOST = ".blob.core.windows.net/"


def _is_azure_url(source: str) -> bool:
    """Return True if *source* looks like an Azure Blob Storage URL."""
    s = source.lower()
    return s.startswith(_AZURE_URL_PREFIXES) or _AZURE_BLOB_HOST in s


def resolve_dataset_sources(
    sources: list[str],
    azure_connection_string: str = "",
    azure_sas_token: str = "",
) -> list[Path | str]:
    """
    Resolve a list of dataset source strings into concrete paths or URLs.

    - Local paths are validated for existence.
    - Glob patterns (containing ``*`` or ``?``) are expanded.
    - Azure URLs are returned as-is (validated at read time).

    Returns a list where each entry is a ``Path`` (local) or ``str`` (Azure URL).

    Raises
    ------
    DatasetError
        If a local path does not exist and is not a valid glob.
    """
    resolved: list[Path | str] = []
    for src in sources:
        if _is_azure_url(src):
            resolved.append(src)
        elif "*" in src or "?" in src:
            # Glob pattern
            matches = sorted(globmod.glob(src, recursive=True))
            if not matches:
                raise DatasetError(f"Glob pattern matched no files: {src!r}")
            for m in matches:
                p = Path(m)
                if p.suffix.lower() in SUPPORTED_SUFFIXES:
                    resolved.append(p)
        else:
            p = Path(src)
            if not p.exists():
                raise DatasetError(f"Dataset file not found: {p}")
            resolved.append(p)

    if not resolved:
        raise DatasetError("No dataset sources resolved from the provided list")

    return resolved


def _iter_azure_batches(
    url: str,
    chunk_size: int,
    connection_string: str = "",
    sas_token: str = "",
) -> Iterator[pa.RecordBatch]:
    """
    Stream Parquet batches from Azure Blob Storage.

    Uses ``fsspec`` + ``adlfs`` to open the file as a seekable stream,
    then PyArrow's ``ParquetFile`` reads batches without full download.

    Authentication priority:
      1. ``connection_string`` (explicit)
      2. ``sas_token`` (explicit)
      3. ``DefaultAzureCredential`` (auto-detected via ``adlfs``)
    """
    try:
        import fsspec  # noqa: PLC0415
    except ImportError:
        raise DatasetError(
            "Azure dataset support requires the 'azure' extra. "
            "Install with: pip install tsbm[azure]"
        ) from None

    storage_options: dict[str, str] = {}
    if connection_string:
        storage_options["connection_string"] = connection_string
    elif sas_token:
        storage_options["sas_token"] = sas_token
    # else: DefaultAzureCredential via adlfs (auto-detected)

    try:
        with fsspec.open(url, mode="rb", **storage_options) as f:
            pf = pq.ParquetFile(f)
            for batch in pf.iter_batches(batch_size=chunk_size):
                yield batch
    except Exception as exc:
        raise DatasetError(
            f"Failed to read Azure dataset {url!r}: {exc}"
        ) from exc


def _iter_multi_source_batches(
    sources: list[Path | str],
    chunk_size: int = 100_000,
    azure_connection_string: str = "",
    azure_sas_token: str = "",
) -> Iterator[pa.RecordBatch]:
    """
    Yield ``RecordBatch`` objects from multiple sources sequentially.

    Local files use the existing ``_iter_dataset_batches``.
    Azure URLs use ``_iter_azure_batches`` for streaming without full download.
    """
    for source in sources:
        if isinstance(source, Path):
            yield from _iter_dataset_batches(source, chunk_size)
        else:
            # Azure URL
            yield from _iter_azure_batches(
                source, chunk_size, azure_connection_string, azure_sas_token
            )


def load_multi_dataset_streaming(
    sources: list[Path | str],
    schema_hint: DatasetSchema,
    chunk_size: int = 100_000,
    azure_connection_string: str = "",
    azure_sas_token: str = "",
) -> BenchmarkDataset:
    """
    Return a :class:`BenchmarkDataset` that streams from multiple sources.

    The sources are iterated sequentially during ``iter_batches()``.
    No data is read into memory until iteration begins.
    """
    # Estimate total row count from local sources
    total_rows = 0
    first_local = None
    for src in sources:
        if isinstance(src, Path):
            if first_local is None:
                first_local = src
            total_rows += estimate_row_count(src)
        # Azure sources: row count unknown until read

    schema = DatasetSchema(
        name=schema_hint.name,
        timestamp_col=schema_hint.timestamp_col,
        tag_cols=list(schema_hint.tag_cols),
        metric_cols=list(schema_hint.metric_cols),
        columns=list(schema_hint.columns),
        row_count=total_rows,
    )

    logger.info(
        "Multi-source streaming dataset: %d source(s), ~%s rows, chunk_size=%d",
        len(sources),
        total_rows or "unknown",
        chunk_size,
    )

    return BenchmarkDataset(
        schema=schema,
        table=None,
        source_path=first_local,  # for timestamp column reading
        streaming=True,
        chunk_size=chunk_size,
        source_paths=list(sources),
        _azure_connection_string=azure_connection_string,
        _azure_sas_token=azure_sas_token,
    )


def infer_schema_from_azure(
    url: str,
    sample_rows: int = 50_000,
    tag_cardinality_threshold: float = 0.05,
    connection_string: str = "",
    sas_token: str = "",
) -> DatasetSchema:
    """
    Infer a ``DatasetSchema`` from the first rows of an Azure Parquet file.
    """
    batches = []
    n_read = 0
    for batch in _iter_azure_batches(url, sample_rows, connection_string, sas_token):
        batches.append(batch)
        n_read += len(batch)
        if n_read >= sample_rows:
            break

    if not batches:
        raise DatasetError(f"Azure dataset appears empty: {url!r}")

    sample = pa.Table.from_batches(batches).slice(0, min(sample_rows, n_read))
    from tsbm.datasets.timestamps import normalize_timestamp_column

    schema = _infer_schema(sample, "multi_dataset", tag_cardinality_threshold)
    sample = normalize_timestamp_column(sample, schema.timestamp_col)
    return _infer_schema(sample, "multi_dataset", tag_cardinality_threshold)


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

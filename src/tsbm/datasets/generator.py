"""
Deterministic synthetic IoT dataset generator.

Uses NumPy's PCG64 PRNG (``numpy.random.default_rng``), which is
platform-reproducible: the same seed always produces the same output
regardless of OS or CPU architecture.

Default schema
--------------
timestamp  : timestamp[ns, tz=UTC]  — primary time column
device_id  : string (TAG)           — "device_000000" … "device_NNNNNN"
temperature: float64 (METRIC)       — Normal(20, 5) °C, clipped [−40, 125]
humidity   : float64 (METRIC)       — Beta(2, 5) × 100 %RH, range (0, 100)
voltage    : float64 (METRIC)       — Normal(3.3, 0.1) V
pressure   : float64 (METRIC)       — Normal(101325, 200) Pa
"""
from __future__ import annotations

import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterator

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from tsbm.datasets.schema import (
    BenchmarkDataset,
    ColumnRole,
    ColumnSpec,
    DatasetSchema,
)
from tsbm.datasets.timestamps import TIMESTAMP_TYPE

# Default column order — matches small_iot.csv header
DEFAULT_COLUMNS: list[ColumnSpec] = [
    ColumnSpec("timestamp",   ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
    ColumnSpec("device_id",   ColumnRole.TAG,       pa.string()),
    ColumnSpec("temperature", ColumnRole.METRIC,    pa.float64()),
    ColumnSpec("humidity",    ColumnRole.METRIC,    pa.float64()),
    ColumnSpec("voltage",     ColumnRole.METRIC,    pa.float64()),
    ColumnSpec("pressure",    ColumnRole.METRIC,    pa.float64()),
]

_CHUNK_ROWS = 100_000  # rows per chunk for lazy / large generation


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def generate_iot_dataset(
    n_devices: int = 100,
    n_readings_per_device: int = 100,
    start_ts: datetime | None = None,
    interval_seconds: float | None = None,
    seed: int = 42,
    name: str = "iot",
    lazy: bool = False,
    output_path: Path | None = None,
) -> BenchmarkDataset:
    """
    Generate a deterministic synthetic IoT dataset.

    Parameters
    ----------
    n_devices:
        Number of unique devices (tag cardinality).
    n_readings_per_device:
        Number of time-series readings per device.
    start_ts:
        Start timestamp (UTC).  Defaults to 2024-01-01T00:00:00Z.
    interval_seconds:
        Time between consecutive readings for a single device.
        Defaults to 86400 / max(n_readings_per_device - 1, 1) so the
        series spans exactly 24 hours.
    seed:
        PRNG seed — same seed always produces the same data.
    name:
        Logical dataset / table name.
    lazy:
        If True, write the dataset to *output_path* (or a temp Parquet file)
        and return a :class:`BenchmarkDataset` with ``table=None``.
    output_path:
        Where to write the Parquet file in lazy mode.  A temp file is created
        if ``None``.  Ignored when ``lazy=False``.

    Returns
    -------
    BenchmarkDataset
    """
    if start_ts is None:
        start_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    if start_ts.tzinfo is None:
        start_ts = start_ts.replace(tzinfo=timezone.utc)

    n_readings = max(n_readings_per_device, 1)
    if interval_seconds is None:
        interval_seconds = 86400.0 / max(n_readings - 1, 1)

    n_total = n_devices * n_readings
    schema = _build_schema(name, n_total)

    if lazy or n_total > _CHUNK_ROWS:
        return _generate_lazy(
            n_devices=n_devices,
            n_readings=n_readings,
            start_ts=start_ts,
            interval_seconds=interval_seconds,
            seed=seed,
            schema=schema,
            output_path=output_path,
        )

    table = _generate_table(
        n_devices=n_devices,
        n_readings=n_readings,
        start_ts=start_ts,
        interval_seconds=interval_seconds,
        seed=seed,
    )
    schema.row_count = len(table)
    return BenchmarkDataset(schema=schema, table=table)


def generate_to_csv(
    path: Path,
    n_devices: int = 100,
    n_readings_per_device: int = 100,
    start_ts: datetime | None = None,
    interval_seconds: float | None = None,
    seed: int = 42,
    name: str = "iot",
) -> None:
    """
    Write a generated dataset directly to a CSV file.

    Uses PyArrow's CSV writer for correct timestamp formatting.
    """
    import pyarrow.csv as pa_csv

    ds = generate_iot_dataset(
        n_devices=n_devices,
        n_readings_per_device=n_readings_per_device,
        start_ts=start_ts,
        interval_seconds=interval_seconds,
        seed=seed,
        name=name,
        lazy=False,
    )
    assert ds.table is not None

    # Cast timestamp to string for human-readable ISO 8601 CSV output
    tbl = _cast_timestamps_to_string(ds.table)
    path.parent.mkdir(parents=True, exist_ok=True)
    pa_csv.write_csv(tbl, str(path))


# ---------------------------------------------------------------------------
# Core generation logic
# ---------------------------------------------------------------------------


def _generate_table(
    n_devices: int,
    n_readings: int,
    start_ts: datetime,
    interval_seconds: float,
    seed: int,
) -> pa.Table:
    """Generate a single in-memory Arrow table (all rows at once)."""
    rng = np.random.default_rng(seed)

    n_total = n_devices * n_readings

    # --- Timestamps ---
    # For each device, create a regular series; interleave all devices.
    # Layout: device 0 readings 0..N-1, then device 1, etc.
    start_ns = int(start_ts.timestamp() * 1e9)
    interval_ns = int(interval_seconds * 1e9)

    offsets = np.arange(n_readings, dtype=np.int64) * interval_ns  # shape (n_readings,)
    # Tile across all devices: shape (n_devices, n_readings) → flatten
    ts_ns = np.tile(offsets, (n_devices, 1)).flatten() + start_ns
    timestamps = pa.array(ts_ns, type=TIMESTAMP_TYPE)

    # --- device_id ---
    # Repeat each device ID n_readings times
    device_ids = pa.array(
        np.repeat(
            [f"device_{i:06d}" for i in range(n_devices)],
            n_readings,
        )
    )

    # --- Numeric metrics (all generated with one rng, order matters for reproducibility) ---
    temperature = pa.array(
        np.clip(rng.normal(loc=20.0, scale=5.0, size=n_total), -40.0, 125.0)
    )
    humidity = pa.array(
        np.clip(rng.beta(a=2.0, b=5.0, size=n_total) * 100.0, 0.0, 100.0)
    )
    voltage = pa.array(
        rng.normal(loc=3.3, scale=0.1, size=n_total)
    )
    pressure = pa.array(
        rng.normal(loc=101325.0, scale=200.0, size=n_total)
    )

    return pa.table(
        {
            "timestamp":   timestamps,
            "device_id":   device_ids,
            "temperature": temperature,
            "humidity":    humidity,
            "voltage":     voltage,
            "pressure":    pressure,
        }
    )


def _generate_chunks(
    n_devices: int,
    n_readings: int,
    start_ts: datetime,
    interval_seconds: float,
    seed: int,
) -> Iterator[pa.Table]:
    """
    Yield Arrow tables in chunks of ``_CHUNK_ROWS`` rows.

    Generates devices in batches to stay within the chunk budget.  The PRNG
    advances deterministically regardless of chunk size so the final result
    is identical to a single-shot generation.
    """
    rng = np.random.default_rng(seed)
    start_ns = int(start_ts.timestamp() * 1e9)
    interval_ns = int(interval_seconds * 1e9)
    offsets = np.arange(n_readings, dtype=np.int64) * interval_ns

    devices_per_chunk = max(1, _CHUNK_ROWS // n_readings)

    for chunk_start in range(0, n_devices, devices_per_chunk):
        chunk_end = min(chunk_start + devices_per_chunk, n_devices)
        chunk_devices = chunk_end - chunk_start
        n_chunk = chunk_devices * n_readings

        ts_ns = (
            np.tile(offsets, (chunk_devices, 1)).flatten() + start_ns
        )
        timestamps = pa.array(ts_ns, type=TIMESTAMP_TYPE)

        device_ids = pa.array(
            np.repeat(
                [f"device_{i:06d}" for i in range(chunk_start, chunk_end)],
                n_readings,
            )
        )

        temperature = pa.array(
            np.clip(rng.normal(20.0, 5.0, n_chunk), -40.0, 125.0)
        )
        humidity = pa.array(
            np.clip(rng.beta(2.0, 5.0, n_chunk) * 100.0, 0.0, 100.0)
        )
        voltage  = pa.array(rng.normal(3.3, 0.1, n_chunk))
        pressure = pa.array(rng.normal(101325.0, 200.0, n_chunk))

        yield pa.table(
            {
                "timestamp":   timestamps,
                "device_id":   device_ids,
                "temperature": temperature,
                "humidity":    humidity,
                "voltage":     voltage,
                "pressure":    pressure,
            }
        )


def _generate_lazy(
    n_devices: int,
    n_readings: int,
    start_ts: datetime,
    interval_seconds: float,
    seed: int,
    schema: DatasetSchema,
    output_path: Path | None,
) -> BenchmarkDataset:
    """Write chunks to Parquet and return a lazy BenchmarkDataset."""
    if output_path is None:
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        output_path = Path(tmp.name)
        tmp.close()

    output_path.parent.mkdir(parents=True, exist_ok=True)

    writer: pq.ParquetWriter | None = None
    total_rows = 0
    try:
        for chunk in _generate_chunks(
            n_devices, n_readings, start_ts, interval_seconds, seed
        ):
            if writer is None:
                writer = pq.ParquetWriter(
                    str(output_path),
                    chunk.schema,
                    compression="snappy",
                )
            writer.write_table(chunk)
            total_rows += len(chunk)
    finally:
        if writer is not None:
            writer.close()

    schema.row_count = total_rows
    return BenchmarkDataset(
        schema=schema,
        table=None,
        source_path=output_path,
        streaming=True,
        chunk_size=_CHUNK_ROWS,
    )


# ---------------------------------------------------------------------------
# Schema helper
# ---------------------------------------------------------------------------


def _build_schema(name: str, row_count: int = 0) -> DatasetSchema:
    return DatasetSchema(
        name=name,
        timestamp_col="timestamp",
        tag_cols=["device_id"],
        metric_cols=["temperature", "humidity", "voltage", "pressure"],
        columns=list(DEFAULT_COLUMNS),
        row_count=row_count,
    )


def _cast_timestamps_to_string(table: pa.Table) -> pa.Table:
    """Replace timestamp[ns, UTC] column with ISO 8601 string for CSV export.

    Strips timezone before casting to avoid the PyArrow Windows tzdata
    database requirement (the underlying int64 ns values are UTC regardless).
    """
    import pandas as pd

    idx = table.schema.get_field_index("timestamp")
    if idx < 0:
        return table
    # Use pandas for robust ISO 8601 formatting without PyArrow tzdata requirement
    ts_col = table.column("timestamp")
    pd_ts = ts_col.to_pandas()  # pandas Timestamp Series (UTC-aware)
    str_values = pd_ts.dt.strftime("%Y-%m-%dT%H:%M:%S.%fZ").tolist()
    str_col = pa.array(str_values, type=pa.string())
    return table.set_column(idx, "timestamp", str_col)

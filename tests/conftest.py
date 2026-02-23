"""
Shared pytest fixtures for the tsbm test suite.

Unit tests use the in-memory / on-disk fixtures here and never require
running Docker containers.

Integration tests use the adapter fixtures in tests/integration/conftest.py
and are gated by ``@pytest.mark.integration``.
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from tsbm.datasets.schema import (
    BenchmarkDataset,
    ColumnRole,
    ColumnSpec,
    DatasetSchema,
)
from tsbm.datasets.timestamps import TIMESTAMP_TYPE

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

DATASETS_DIR = Path(__file__).parent.parent / "datasets"
SMALL_IOT_CSV = DATASETS_DIR / "small_iot.csv"


@pytest.fixture(scope="session")
def small_iot_csv() -> Path:
    """Path to the bundled 10 000-row synthetic IoT CSV."""
    assert SMALL_IOT_CSV.exists(), f"Missing: {SMALL_IOT_CSV}"
    return SMALL_IOT_CSV


# ---------------------------------------------------------------------------
# Schema fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_schema() -> DatasetSchema:
    """Minimal three-column schema: ts (TIMESTAMP), device (TAG), value (METRIC)."""
    return DatasetSchema(
        name="simple",
        timestamp_col="ts",
        tag_cols=["device"],
        metric_cols=["value"],
        columns=[
            ColumnSpec("ts",     ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            ColumnSpec("device", ColumnRole.TAG,       pa.string()),
            ColumnSpec("value",  ColumnRole.METRIC,    pa.float64()),
        ],
    )


@pytest.fixture
def iot_schema() -> DatasetSchema:
    """Schema matching the small_iot.csv file."""
    return DatasetSchema(
        name="small_iot",
        timestamp_col="timestamp",
        tag_cols=["device_id"],
        metric_cols=["temperature", "humidity", "voltage", "pressure"],
        columns=[
            ColumnSpec("timestamp",   ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            ColumnSpec("device_id",   ColumnRole.TAG,       pa.string()),
            ColumnSpec("temperature", ColumnRole.METRIC,    pa.float64()),
            ColumnSpec("humidity",    ColumnRole.METRIC,    pa.float64()),
            ColumnSpec("voltage",     ColumnRole.METRIC,    pa.float64()),
            ColumnSpec("pressure",    ColumnRole.METRIC,    pa.float64()),
        ],
        row_count=10_000,
    )


# ---------------------------------------------------------------------------
# Arrow table fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def simple_arrow_table() -> pa.Table:
    """Small in-memory Arrow table matching *simple_schema*."""
    import numpy as np

    n = 100
    rng = np.random.default_rng(0)
    base_ns = 1_704_067_200_000_000_000  # 2024-01-01T00:00:00Z in ns
    return pa.table(
        {
            "ts":     pa.array(
                [base_ns + i * 60_000_000_000 for i in range(n)],
                type=TIMESTAMP_TYPE,
            ),
            "device": pa.array([f"d{i % 5:02d}" for i in range(n)]),
            "value":  pa.array(rng.normal(20, 5, n)),
        }
    )


@pytest.fixture(scope="session")
def iot_dataset(small_iot_csv: Path) -> BenchmarkDataset:
    """Loaded BenchmarkDataset from the bundled small_iot.csv."""
    from tsbm.datasets.loader import load_dataset

    return load_dataset(small_iot_csv)

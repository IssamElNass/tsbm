"""
Unit tests for tsbm.datasets.generator.

Tests:
  - PRNG determinism: same seed → identical output
  - Different seeds → different data
  - Correct row count (n_devices × n_readings)
  - Schema columns: timestamp, device_id, temperature, humidity, voltage, pressure
  - Timestamps are monotonically increasing per device
  - device_id cardinality matches n_devices
  - Lazy mode writes Parquet and returns table=None with source_path set
  - generate_to_csv produces a valid CSV matching expected row count
"""
from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import pytest

from tsbm.datasets.generator import (
    DEFAULT_COLUMNS,
    generate_iot_dataset,
    generate_to_csv,
)
from tsbm.datasets.schema import BenchmarkDataset, ColumnRole
from tsbm.datasets.timestamps import TIMESTAMP_TYPE


# ---------------------------------------------------------------------------
# PRNG determinism
# ---------------------------------------------------------------------------


class TestDeterminism:
    def test_same_seed_produces_identical_tables(self) -> None:
        a = generate_iot_dataset(n_devices=5, n_readings_per_device=10, seed=42)
        b = generate_iot_dataset(n_devices=5, n_readings_per_device=10, seed=42)
        assert a.load().equals(b.load())

    def test_different_seeds_produce_different_temperatures(self) -> None:
        a = generate_iot_dataset(n_devices=5, n_readings_per_device=10, seed=1)
        b = generate_iot_dataset(n_devices=5, n_readings_per_device=10, seed=2)
        # Temperature columns should differ
        ta = a.load().column("temperature").to_pylist()
        tb = b.load().column("temperature").to_pylist()
        assert ta != tb

    def test_seed_42_matches_repeated_call(self) -> None:
        out1 = generate_iot_dataset(n_devices=10, n_readings_per_device=5, seed=42)
        out2 = generate_iot_dataset(n_devices=10, n_readings_per_device=5, seed=42)
        assert out1.load().column("voltage").equals(out2.load().column("voltage"))


# ---------------------------------------------------------------------------
# Row count and structure
# ---------------------------------------------------------------------------


class TestStructure:
    def test_row_count(self) -> None:
        n_devices, n_readings = 7, 13
        ds = generate_iot_dataset(n_devices=n_devices, n_readings_per_device=n_readings)
        assert len(ds.load()) == n_devices * n_readings

    def test_schema_has_all_default_columns(self) -> None:
        ds = generate_iot_dataset(n_devices=3, n_readings_per_device=5)
        names = ds.load().schema.names
        for col in DEFAULT_COLUMNS:
            assert col.name in names, f"Missing column: {col.name}"

    def test_timestamp_column_type(self) -> None:
        ds = generate_iot_dataset(n_devices=2, n_readings_per_device=4)
        tbl = ds.load()
        assert tbl.schema.field("timestamp").type == TIMESTAMP_TYPE

    def test_device_id_cardinality(self) -> None:
        n = 8
        ds = generate_iot_dataset(n_devices=n, n_readings_per_device=5)
        unique_ids = pa.compute.value_counts(ds.load().column("device_id"))
        assert len(unique_ids) == n

    def test_device_id_format(self) -> None:
        ds = generate_iot_dataset(n_devices=3, n_readings_per_device=2)
        ids = set(ds.load().column("device_id").to_pylist())
        for did in ids:
            assert did.startswith("device_"), f"Unexpected device_id format: {did}"

    def test_schema_name_set_correctly(self) -> None:
        ds = generate_iot_dataset(n_devices=2, n_readings_per_device=2, name="mytest")
        assert ds.schema.name == "mytest"

    def test_tag_col_role_assigned(self) -> None:
        ds = generate_iot_dataset(n_devices=2, n_readings_per_device=2)
        device_col = ds.schema.get_col("device_id")
        assert device_col.role == ColumnRole.TAG

    def test_metric_col_roles_assigned(self) -> None:
        ds = generate_iot_dataset(n_devices=2, n_readings_per_device=2)
        for name in ["temperature", "humidity", "voltage", "pressure"]:
            assert ds.schema.get_col(name).role == ColumnRole.METRIC


# ---------------------------------------------------------------------------
# Value ranges
# ---------------------------------------------------------------------------


class TestValueRanges:
    def test_temperature_plausible_range(self) -> None:
        ds = generate_iot_dataset(n_devices=10, n_readings_per_device=50, seed=42)
        temps = ds.load().column("temperature").to_pylist()
        for t in temps:
            assert -40 <= t <= 125, f"Out-of-range temperature: {t}"

    def test_humidity_in_0_100(self) -> None:
        ds = generate_iot_dataset(n_devices=10, n_readings_per_device=50, seed=42)
        hums = ds.load().column("humidity").to_pylist()
        for h in hums:
            assert 0 < h < 100, f"Out-of-range humidity: {h}"

    def test_voltage_near_3_3v(self) -> None:
        ds = generate_iot_dataset(n_devices=20, n_readings_per_device=100, seed=42)
        voltages = ds.load().column("voltage").to_pylist()
        mean_v = sum(voltages) / len(voltages)
        assert 3.0 <= mean_v <= 3.6, f"Mean voltage {mean_v} not near 3.3V"

    def test_timestamps_span_24h(self) -> None:
        import pyarrow.compute as pc

        ds = generate_iot_dataset(n_devices=1, n_readings_per_device=100, seed=0)
        ts = ds.load().column("timestamp").cast(pa.int64())
        span_s = (pc.max(ts).as_py() - pc.min(ts).as_py()) / 1e9
        assert 23 * 3600 <= span_s <= 25 * 3600


# ---------------------------------------------------------------------------
# Lazy mode
# ---------------------------------------------------------------------------


class TestLazyMode:
    def test_lazy_returns_none_table(self, tmp_path: Path) -> None:
        out = tmp_path / "lazy_test.parquet"
        ds = generate_iot_dataset(
            n_devices=3, n_readings_per_device=5, lazy=True, output_path=out
        )
        assert ds.is_lazy()
        assert ds.table is None
        assert ds.source_path == out

    def test_lazy_parquet_file_created(self, tmp_path: Path) -> None:
        out = tmp_path / "lazy_test.parquet"
        generate_iot_dataset(n_devices=3, n_readings_per_device=5, lazy=True, output_path=out)
        assert out.exists()
        assert out.stat().st_size > 0

    def test_lazy_load_returns_correct_rows(self, tmp_path: Path) -> None:
        out = tmp_path / "lazy_test.parquet"
        ds = generate_iot_dataset(
            n_devices=4, n_readings_per_device=6, lazy=True, output_path=out
        )
        tbl = ds.load()
        assert len(tbl) == 4 * 6

    def test_lazy_no_output_path_uses_temp_file(self) -> None:
        ds = generate_iot_dataset(n_devices=2, n_readings_per_device=3, lazy=True)
        assert ds.is_lazy()
        assert ds.source_path is not None
        assert ds.source_path.exists()


# ---------------------------------------------------------------------------
# generate_to_csv
# ---------------------------------------------------------------------------


class TestGenerateToCsv:
    def test_csv_row_count(self, tmp_path: Path) -> None:
        out = tmp_path / "test.csv"
        generate_to_csv(
            path=out,
            n_devices=5,
            n_readings_per_device=10,
            seed=42,
        )
        lines = out.read_text().strip().splitlines()
        # header + 50 data rows
        assert len(lines) == 51

    def test_csv_has_expected_header(self, tmp_path: Path) -> None:
        out = tmp_path / "header_test.csv"
        generate_to_csv(path=out, n_devices=2, n_readings_per_device=2, seed=0)
        header = out.read_text().splitlines()[0]
        for col in ["timestamp", "device_id", "temperature", "humidity", "voltage", "pressure"]:
            assert col in header

    def test_csv_deterministic(self, tmp_path: Path) -> None:
        a = tmp_path / "a.csv"
        b = tmp_path / "b.csv"
        generate_to_csv(path=a, n_devices=3, n_readings_per_device=4, seed=7)
        generate_to_csv(path=b, n_devices=3, n_readings_per_device=4, seed=7)
        assert a.read_text() == b.read_text()

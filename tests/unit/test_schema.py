"""
Unit tests for tsbm.datasets.schema and tsbm.datasets.loader.

Tests:
  - DatasetSchema serialisation roundtrip (to_dict / from_dict)
  - DatasetSchema.arrow_schema() produces correct pa.Schema
  - DatasetSchema.cols_by_role() filters correctly
  - DatasetSchema.get_col() raises KeyError for unknown column
  - BenchmarkDataset.load() → loads table from source_path
  - BenchmarkDataset.slice() → returns sub-table
  - Auto-inference from loader: timestamp / TAG / METRIC detection
  - load_dataset on small_iot.csv produces correct schema
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
    _parse_arrow_type,
)
from tsbm.datasets.timestamps import TIMESTAMP_TYPE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_schema(name: str = "test") -> DatasetSchema:
    return DatasetSchema(
        name=name,
        timestamp_col="ts",
        tag_cols=["device"],
        metric_cols=["temp", "hum"],
        columns=[
            ColumnSpec("ts",     ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            ColumnSpec("device", ColumnRole.TAG,       pa.string()),
            ColumnSpec("temp",   ColumnRole.METRIC,    pa.float64()),
            ColumnSpec("hum",    ColumnRole.METRIC,    pa.float64()),
        ],
        row_count=100,
    )


# ---------------------------------------------------------------------------
# DatasetSchema serialisation
# ---------------------------------------------------------------------------


class TestDatasetSchemaSerde:
    def test_to_dict_contains_all_fields(self) -> None:
        schema = _make_schema()
        d = schema.to_dict()
        assert d["name"] == "test"
        assert d["timestamp_col"] == "ts"
        assert d["tag_cols"] == ["device"]
        assert d["metric_cols"] == ["temp", "hum"]
        assert len(d["columns"]) == 4
        assert d["row_count"] == 100

    def test_roundtrip_name(self) -> None:
        schema = _make_schema("myds")
        assert DatasetSchema.from_dict(schema.to_dict()).name == "myds"

    def test_roundtrip_timestamp_col(self) -> None:
        schema = _make_schema()
        rebuilt = DatasetSchema.from_dict(schema.to_dict())
        assert rebuilt.timestamp_col == "ts"

    def test_roundtrip_tag_cols(self) -> None:
        schema = _make_schema()
        rebuilt = DatasetSchema.from_dict(schema.to_dict())
        assert rebuilt.tag_cols == ["device"]

    def test_roundtrip_metric_cols(self) -> None:
        schema = _make_schema()
        rebuilt = DatasetSchema.from_dict(schema.to_dict())
        assert rebuilt.metric_cols == ["temp", "hum"]

    def test_roundtrip_column_roles(self) -> None:
        schema = _make_schema()
        rebuilt = DatasetSchema.from_dict(schema.to_dict())
        assert rebuilt.get_col("ts").role == ColumnRole.TIMESTAMP
        assert rebuilt.get_col("device").role == ColumnRole.TAG
        assert rebuilt.get_col("temp").role == ColumnRole.METRIC

    def test_roundtrip_row_count(self) -> None:
        schema = _make_schema()
        rebuilt = DatasetSchema.from_dict(schema.to_dict())
        assert rebuilt.row_count == 100

    def test_from_dict_missing_columns_defaults_empty(self) -> None:
        minimal = {"name": "x", "timestamp_col": "ts", "tag_cols": [], "metric_cols": []}
        schema = DatasetSchema.from_dict(minimal)
        assert schema.columns == []


# ---------------------------------------------------------------------------
# DatasetSchema.arrow_schema()
# ---------------------------------------------------------------------------


class TestArrowSchema:
    def test_returns_pa_schema(self) -> None:
        schema = _make_schema()
        arrow_schema = schema.arrow_schema()
        assert isinstance(arrow_schema, pa.Schema)

    def test_field_count_matches_columns(self) -> None:
        schema = _make_schema()
        assert len(schema.arrow_schema()) == len(schema.columns)

    def test_timestamp_field_type(self) -> None:
        schema = _make_schema()
        arrow_schema = schema.arrow_schema()
        ts_field = arrow_schema.field("ts")
        assert ts_field.type == TIMESTAMP_TYPE

    def test_metric_field_type(self) -> None:
        schema = _make_schema()
        arrow_schema = schema.arrow_schema()
        assert arrow_schema.field("temp").type == pa.float64()


# ---------------------------------------------------------------------------
# DatasetSchema.cols_by_role()
# ---------------------------------------------------------------------------


class TestColsByRole:
    def test_returns_only_tags(self) -> None:
        schema = _make_schema()
        tags = schema.cols_by_role(ColumnRole.TAG)
        assert len(tags) == 1
        assert tags[0].name == "device"

    def test_returns_all_metrics(self) -> None:
        schema = _make_schema()
        metrics = schema.cols_by_role(ColumnRole.METRIC)
        names = {c.name for c in metrics}
        assert names == {"temp", "hum"}

    def test_returns_timestamp(self) -> None:
        schema = _make_schema()
        ts_cols = schema.cols_by_role(ColumnRole.TIMESTAMP)
        assert len(ts_cols) == 1
        assert ts_cols[0].name == "ts"

    def test_empty_role_returns_empty_list(self) -> None:
        schema = _make_schema()
        other = schema.cols_by_role(ColumnRole.OTHER)
        assert other == []


# ---------------------------------------------------------------------------
# DatasetSchema.get_col()
# ---------------------------------------------------------------------------


class TestGetCol:
    def test_found_by_name(self) -> None:
        schema = _make_schema()
        col = schema.get_col("device")
        assert col.name == "device"
        assert col.role == ColumnRole.TAG

    def test_missing_raises_key_error(self) -> None:
        schema = _make_schema()
        with pytest.raises(KeyError, match="not found"):
            schema.get_col("nonexistent")


# ---------------------------------------------------------------------------
# BenchmarkDataset
# ---------------------------------------------------------------------------


class TestBenchmarkDataset:
    def test_is_not_lazy_when_table_provided(self) -> None:
        tbl = pa.table({"a": [1, 2, 3]})
        bds = BenchmarkDataset(schema=_make_schema(), table=tbl)
        assert not bds.is_lazy()

    def test_is_lazy_when_table_is_none(self) -> None:
        bds = BenchmarkDataset(schema=_make_schema(), table=None, source_path=Path("x.csv"))
        assert bds.is_lazy()

    def test_load_returns_table_when_in_memory(self) -> None:
        tbl = pa.table({"a": [1]})
        bds = BenchmarkDataset(schema=_make_schema(), table=tbl)
        assert bds.load() is tbl

    def test_load_raises_when_no_table_and_no_path(self) -> None:
        bds = BenchmarkDataset(schema=_make_schema(), table=None, source_path=None)
        with pytest.raises(ValueError, match="neither"):
            bds.load()

    def test_load_from_parquet(self, tmp_path: Path) -> None:
        import pyarrow.parquet as pq

        # Use proper Unix-second timestamps (> 1e9) so the timestamp cascade works
        base_s = 1_704_067_200  # 2024-01-01T00:00:00Z
        tbl = pa.table({
            "ts":  pa.array([base_s, base_s + 60, base_s + 120], type=pa.int64()),
            "val": pa.array([1.0, 2.0, 3.0]),
        })
        out = tmp_path / "test.parquet"
        pq.write_table(tbl, out)

        from tsbm.datasets.schema import ColumnSpec, DatasetSchema

        schema = DatasetSchema(
            name=out.stem,
            timestamp_col="ts",
            tag_cols=[],
            metric_cols=["val"],
            columns=[
                ColumnSpec("ts",  ColumnRole.TIMESTAMP, pa.int64()),
                ColumnSpec("val", ColumnRole.METRIC,    pa.float64()),
            ],
        )
        bds = BenchmarkDataset(schema=schema, table=None, source_path=out)
        loaded = bds.load()
        assert len(loaded) == 3

    def test_slice_returns_sub_table(self) -> None:
        tbl = pa.table({"a": list(range(10))})
        bds = BenchmarkDataset(schema=_make_schema(), table=tbl)
        sliced = bds.slice(2, 3)
        assert len(sliced) == 3
        assert sliced.column("a").to_pylist() == [2, 3, 4]


# ---------------------------------------------------------------------------
# _parse_arrow_type helper
# ---------------------------------------------------------------------------


class TestParseArrowType:
    @pytest.mark.parametrize("type_str,expected", [
        ("int64",              pa.int64()),
        ("float64",            pa.float64()),
        ("string",             pa.string()),
        ("bool",               pa.bool_()),
        ("boolean",            pa.bool_()),
        ("timestamp[ns]",      pa.timestamp("ns", tz="UTC")),
        ("timestamp[ns, UTC]", pa.timestamp("ns", tz="UTC")),
    ])
    def test_known_types(self, type_str: str, expected: pa.DataType) -> None:
        assert _parse_arrow_type(type_str) == expected

    def test_unknown_falls_back_to_large_string(self) -> None:
        result = _parse_arrow_type("totally_unknown_type")
        assert result == pa.large_string()


# ---------------------------------------------------------------------------
# Integration: load_dataset on small_iot.csv
# ---------------------------------------------------------------------------


class TestLoadSmallIotCsv:
    def test_loads_without_error(self, small_iot_csv: Path) -> None:
        from tsbm.datasets.loader import load_dataset

        bds = load_dataset(small_iot_csv)
        assert bds is not None

    def test_row_count(self, small_iot_csv: Path) -> None:
        from tsbm.datasets.loader import load_dataset

        bds = load_dataset(small_iot_csv)
        assert bds.schema.row_count == 10_000

    def test_timestamp_col_detected(self, small_iot_csv: Path) -> None:
        from tsbm.datasets.loader import load_dataset

        bds = load_dataset(small_iot_csv)
        assert bds.schema.timestamp_col == "timestamp"

    def test_timestamp_column_type(self, small_iot_csv: Path) -> None:
        from tsbm.datasets.loader import load_dataset

        bds = load_dataset(small_iot_csv)
        tbl = bds.load()
        assert tbl.schema.field("timestamp").type == TIMESTAMP_TYPE

    def test_device_id_is_tag(self, small_iot_csv: Path) -> None:
        from tsbm.datasets.loader import load_dataset

        bds = load_dataset(small_iot_csv)
        assert "device_id" in bds.schema.tag_cols

    def test_numeric_columns_are_metrics(self, small_iot_csv: Path) -> None:
        from tsbm.datasets.loader import load_dataset

        bds = load_dataset(small_iot_csv)
        for col in ["temperature", "humidity", "voltage", "pressure"]:
            assert col in bds.schema.metric_cols

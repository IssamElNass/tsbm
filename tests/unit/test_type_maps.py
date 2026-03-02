"""
Unit tests for tsbm.adapters.type_maps.

Tests:
  - get_db_type for every PyArrow type × 3 databases
  - SYMBOL vs STRING distinction for QuestDB TAG vs METRIC columns
  - arrow_table_to_ddl produces correct DDL for all three databases
  - QuestDB DDL includes TIMESTAMP(col) PARTITION BY DAY
  - CrateDB DDL is plain CREATE TABLE
  - TimescaleDB DDL is plain CREATE TABLE (hypertable added separately)
  - cratedb_array_type appends [] to every type
  - _quote protects reserved words in PostgreSQL-style DBs
"""
from __future__ import annotations

import pyarrow as pa
import pytest

from tsbm.adapters.type_maps import (
    DB_CRATEDB,
    DB_QUESTDB,
    DB_TIMESCALEDB,
    arrow_table_to_ddl,
    cratedb_array_type,
    get_db_type,
)
from tsbm.datasets.schema import ColumnRole, ColumnSpec, DatasetSchema
from tsbm.datasets.timestamps import TIMESTAMP_TYPE


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _spec(name: str, role: ColumnRole, arrow_type: pa.DataType) -> ColumnSpec:
    return ColumnSpec(name=name, role=role, arrow_type=arrow_type)


def _schema(*cols: ColumnSpec, ts_col: str = "ts") -> DatasetSchema:
    tag_cols = [c.name for c in cols if c.role == ColumnRole.TAG]
    metric_cols = [c.name for c in cols if c.role == ColumnRole.METRIC]
    return DatasetSchema(
        name="test",
        timestamp_col=ts_col,
        tag_cols=tag_cols,
        metric_cols=metric_cols,
        columns=list(cols),
    )


# ---------------------------------------------------------------------------
# get_db_type — integer types
# ---------------------------------------------------------------------------


class TestIntegerTypes:
    @pytest.mark.parametrize("arrow_type,db,expected", [
        (pa.int64(),  DB_QUESTDB,     "LONG"),
        (pa.int64(),  DB_CRATEDB,     "BIGINT"),
        (pa.int64(),  DB_TIMESCALEDB, "BIGINT"),
        (pa.uint64(), DB_QUESTDB,     "LONG"),
        (pa.uint64(), DB_CRATEDB,     "BIGINT"),
        (pa.int32(),  DB_QUESTDB,     "INT"),
        (pa.int32(),  DB_CRATEDB,     "INTEGER"),
        (pa.int32(),  DB_TIMESCALEDB, "INTEGER"),
        (pa.int16(),  DB_CRATEDB,     "INTEGER"),
        (pa.int8(),   DB_TIMESCALEDB, "INTEGER"),
        (pa.uint32(), DB_QUESTDB,     "INT"),
    ])
    def test_integer_mapping(self, arrow_type: pa.DataType, db: str, expected: str) -> None:
        result = get_db_type(arrow_type, ColumnRole.METRIC, db)
        assert result == expected


# ---------------------------------------------------------------------------
# get_db_type — float types
# ---------------------------------------------------------------------------


class TestFloatTypes:
    @pytest.mark.parametrize("arrow_type,db,expected", [
        (pa.float64(), DB_QUESTDB,     "DOUBLE"),
        (pa.float64(), DB_CRATEDB,     "DOUBLE PRECISION"),
        (pa.float64(), DB_TIMESCALEDB, "DOUBLE PRECISION"),
        (pa.float32(), DB_QUESTDB,     "FLOAT"),
        (pa.float32(), DB_CRATEDB,     "REAL"),
        (pa.float32(), DB_TIMESCALEDB, "REAL"),
    ])
    def test_float_mapping(self, arrow_type: pa.DataType, db: str, expected: str) -> None:
        result = get_db_type(arrow_type, ColumnRole.METRIC, db)
        assert result == expected


# ---------------------------------------------------------------------------
# get_db_type — string types (role-dependent for QuestDB)
# ---------------------------------------------------------------------------


class TestStringTypes:
    def test_questdb_tag_is_symbol(self) -> None:
        assert get_db_type(pa.string(), ColumnRole.TAG, DB_QUESTDB) == "SYMBOL"

    def test_questdb_metric_string_is_string(self) -> None:
        assert get_db_type(pa.string(), ColumnRole.METRIC, DB_QUESTDB) == "STRING"

    def test_questdb_other_string_is_string(self) -> None:
        assert get_db_type(pa.string(), ColumnRole.OTHER, DB_QUESTDB) == "STRING"

    def test_cratedb_all_strings_are_text(self) -> None:
        for role in ColumnRole:
            assert get_db_type(pa.string(), role, DB_CRATEDB) == "TEXT"

    def test_timescaledb_all_strings_are_text(self) -> None:
        for role in ColumnRole:
            assert get_db_type(pa.string(), role, DB_TIMESCALEDB) == "TEXT"

    def test_large_string_same_as_string(self) -> None:
        assert get_db_type(pa.large_string(), ColumnRole.TAG, DB_QUESTDB) == "SYMBOL"
        assert get_db_type(pa.large_string(), ColumnRole.TAG, DB_CRATEDB) == "TEXT"


# ---------------------------------------------------------------------------
# get_db_type — timestamp and boolean
# ---------------------------------------------------------------------------


class TestTimestampAndBoolTypes:
    @pytest.mark.parametrize("db,expected", [
        (DB_QUESTDB,     "TIMESTAMP"),
        (DB_CRATEDB,     "TIMESTAMP WITH TIME ZONE"),
        (DB_TIMESCALEDB, "TIMESTAMPTZ"),
    ])
    def test_timestamp_ns_utc(self, db: str, expected: str) -> None:
        assert get_db_type(TIMESTAMP_TYPE, ColumnRole.TIMESTAMP, db) == expected

    def test_date32_maps_to_timestamp(self) -> None:
        for db in (DB_QUESTDB, DB_CRATEDB, DB_TIMESCALEDB):
            result = get_db_type(pa.date32(), ColumnRole.TIMESTAMP, db)
            assert "TIMESTAMP" in result or "TIMESTAMPTZ" in result

    @pytest.mark.parametrize("db", [DB_QUESTDB, DB_CRATEDB, DB_TIMESCALEDB])
    def test_boolean(self, db: str) -> None:
        assert get_db_type(pa.bool_(), ColumnRole.METRIC, db) == "BOOLEAN"


# ---------------------------------------------------------------------------
# arrow_table_to_ddl — QuestDB
# ---------------------------------------------------------------------------


class TestQuestDBDDL:
    def _schema(self) -> DatasetSchema:
        return _schema(
            _spec("ts",    ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            _spec("dev",   ColumnRole.TAG,       pa.string()),
            _spec("temp",  ColumnRole.METRIC,    pa.float64()),
            ts_col="ts",
        )

    def test_contains_create_table(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_QUESTDB, "sensors")
        assert "CREATE TABLE" in ddl
        assert "sensors" in ddl

    def test_contains_timestamp_partition(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_QUESTDB, "sensors")
        assert 'TIMESTAMP("ts")' in ddl
        assert "PARTITION BY DAY" in ddl

    def test_symbol_for_tag_column(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_QUESTDB, "sensors")
        assert "SYMBOL" in ddl

    def test_double_for_float64_column(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_QUESTDB, "sensors")
        assert "DOUBLE" in ddl


# ---------------------------------------------------------------------------
# arrow_table_to_ddl — CrateDB
# ---------------------------------------------------------------------------


class TestCrateDBDDL:
    def _schema(self) -> DatasetSchema:
        return _schema(
            _spec("ts",   ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            _spec("tag",  ColumnRole.TAG,       pa.string()),
            _spec("val",  ColumnRole.METRIC,    pa.float64()),
            ts_col="ts",
        )

    def test_contains_create_table(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_CRATEDB, "t")
        assert "CREATE TABLE" in ddl

    def test_no_partition_clause(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_CRATEDB, "t")
        assert "PARTITION BY DAY" not in ddl
        assert "TIMESTAMP(ts)" not in ddl

    def test_timestamp_with_time_zone(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_CRATEDB, "t")
        assert "TIMESTAMP WITH TIME ZONE" in ddl

    def test_reserved_word_quoted(self) -> None:
        schema = _schema(
            _spec("timestamp", ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            _spec("value",     ColumnRole.METRIC,    pa.float64()),
            ts_col="timestamp",
        )
        ddl = arrow_table_to_ddl(schema, DB_CRATEDB, "t")
        assert '"timestamp"' in ddl
        assert '"value"' in ddl


# ---------------------------------------------------------------------------
# arrow_table_to_ddl — TimescaleDB
# ---------------------------------------------------------------------------


class TestTimescaleDBDDL:
    def _schema(self) -> DatasetSchema:
        return _schema(
            _spec("ts",  ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
            _spec("dev", ColumnRole.TAG,       pa.string()),
            _spec("val", ColumnRole.METRIC,    pa.float64()),
            ts_col="ts",
        )

    def test_contains_create_table(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_TIMESCALEDB, "t")
        assert "CREATE TABLE" in ddl

    def test_no_hypertable_call_in_ddl(self) -> None:
        # Hypertable is created separately by the adapter
        ddl = arrow_table_to_ddl(self._schema(), DB_TIMESCALEDB, "t")
        assert "create_hypertable" not in ddl.lower()

    def test_timestamptz_type(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_TIMESCALEDB, "t")
        assert "TIMESTAMPTZ" in ddl

    def test_timestamp_col_not_null(self) -> None:
        ddl = arrow_table_to_ddl(self._schema(), DB_TIMESCALEDB, "t")
        # Should have NOT NULL on the timestamp column
        assert "NOT NULL" in ddl


# ---------------------------------------------------------------------------
# Unknown database raises ValueError
# ---------------------------------------------------------------------------


def test_unknown_db_raises() -> None:
    # arrow_table_to_ddl raises ValueError at the end of the function for unknown
    # databases; however, _column_definitions → get_db_type → _ts_type may raise
    # KeyError first (dict lookup on an unknown key).  Accept either.
    schema = _schema(
        _spec("ts", ColumnRole.TIMESTAMP, TIMESTAMP_TYPE),
        ts_col="ts",
    )
    with pytest.raises((ValueError, KeyError)):
        arrow_table_to_ddl(schema, "baddb", "t")


# ---------------------------------------------------------------------------
# cratedb_array_type
# ---------------------------------------------------------------------------


class TestCrateDBArrayType:
    @pytest.mark.parametrize("arrow_type,role,expected_suffix", [
        (pa.float64(),  ColumnRole.METRIC,    "DOUBLE PRECISION[]"),
        (pa.int64(),    ColumnRole.METRIC,    "BIGINT[]"),
        (pa.string(),   ColumnRole.TAG,       "TEXT[]"),
        (TIMESTAMP_TYPE, ColumnRole.TIMESTAMP, "TIMESTAMPTZ[]"),
        (pa.bool_(),    ColumnRole.METRIC,    "BOOLEAN[]"),
    ])
    def test_array_suffix_added(
        self,
        arrow_type: pa.DataType,
        role: ColumnRole,
        expected_suffix: str,
    ) -> None:
        col = _spec("col", role, arrow_type)
        result = cratedb_array_type(col)
        assert result == expected_suffix

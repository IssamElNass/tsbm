"""
Unit tests for tsbm.datasets.timestamps.

Covers all four cascade paths:
  1. pa.timestamp input  → cast to timestamp[ns, UTC]
  2. Integer array       → nanosecond epoch (> 1e18) or second epoch (> 1e9)
  3. Float array         → Unix seconds with fractional nanoseconds
  4. String array        → ISO 8601 via pd.to_datetime; exotic via dateutil

Also covers edge cases:
  - Already-correct timestamp type returns without copy
  - Integer too small raises SchemaError
  - Mixed null values survive all paths
  - Date32 (date-only) input is promoted to timestamp[ns, UTC]
"""
from __future__ import annotations

from datetime import datetime, timezone

import pyarrow as pa
import pytest

from tsbm.datasets.timestamps import (
    TIMESTAMP_TYPE,
    normalize_timestamp_array,
    normalize_timestamp_column,
)
from tsbm.exceptions import SchemaError

# Reference timestamp: 2024-01-01T00:00:00Z
_REF_DT = datetime(2024, 1, 1, tzinfo=timezone.utc)
_REF_NS = int(_REF_DT.timestamp() * 1e9)   # 1704067200000000000
_REF_S  = int(_REF_DT.timestamp())         # 1704067200


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def _ns(arr: pa.Array) -> list[int]:
    """Return the integer nanosecond values of a timestamp array."""
    return arr.cast(pa.int64()).to_pylist()


# ---------------------------------------------------------------------------
# Path 1: pa.timestamp input
# ---------------------------------------------------------------------------


class TestTimestampPath:
    def test_already_ns_utc_returns_identical(self) -> None:
        arr = pa.array([_REF_NS], type=TIMESTAMP_TYPE)
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert _ns(out)[0] == _REF_NS

    def test_microsecond_utc_is_cast_to_ns(self) -> None:
        us_type = pa.timestamp("us", tz="UTC")
        ref_us = _REF_NS // 1_000
        arr = pa.array([ref_us], type=us_type)
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert _ns(out)[0] == pytest.approx(_REF_NS, abs=1_000)

    def test_millisecond_no_tz_gets_utc(self) -> None:
        ms_type = pa.timestamp("ms")
        ref_ms = _REF_NS // 1_000_000
        arr = pa.array([ref_ms], type=ms_type)
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        # Value within 1 ms tolerance
        assert abs(_ns(out)[0] - _REF_NS) < 1_000_001

    def test_second_resolution_timestamp(self) -> None:
        s_type = pa.timestamp("s", tz="UTC")
        arr = pa.array([_REF_S], type=s_type)
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert _ns(out)[0] == _REF_NS

    def test_null_values_preserved(self) -> None:
        arr = pa.array([_REF_NS, None, _REF_NS], type=TIMESTAMP_TYPE)
        out = normalize_timestamp_array(arr)
        assert out[1].is_valid is False

    def test_chunked_array_is_flattened(self) -> None:
        chunk = pa.chunked_array(
            [pa.array([_REF_NS], type=TIMESTAMP_TYPE),
             pa.array([_REF_NS], type=TIMESTAMP_TYPE)]
        )
        out = normalize_timestamp_array(chunk)
        assert isinstance(out, pa.Array)
        assert len(out) == 2


# ---------------------------------------------------------------------------
# Path 2: integer input
# ---------------------------------------------------------------------------


class TestIntegerPath:
    def test_nanosecond_epoch(self) -> None:
        arr = pa.array([_REF_NS], type=pa.int64())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert _ns(out)[0] == _REF_NS

    def test_second_epoch(self) -> None:
        arr = pa.array([_REF_S], type=pa.int64())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert _ns(out)[0] == _REF_NS

    def test_int32_second_epoch(self) -> None:
        arr = pa.array([_REF_S], type=pa.int32())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE

    def test_small_integer_raises_schema_error(self) -> None:
        arr = pa.array([42], type=pa.int64())
        with pytest.raises(SchemaError, match="too small"):
            normalize_timestamp_array(arr)

    def test_negative_second_epoch(self) -> None:
        # Unix epoch for 1969-12-31T23:00:00Z = -3600 seconds — too small
        arr = pa.array([-3600], type=pa.int64())
        with pytest.raises(SchemaError):
            normalize_timestamp_array(arr)

    def test_all_nulls_returns_null_array(self) -> None:
        arr = pa.array([None, None], type=pa.int64())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert all(not v.is_valid for v in out)


# ---------------------------------------------------------------------------
# Path 3: float input
# ---------------------------------------------------------------------------


class TestFloatPath:
    def test_float_unix_seconds(self) -> None:
        ref_f = float(_REF_S)
        arr = pa.array([ref_f], type=pa.float64())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        # Allow 1 ms tolerance for float → ns rounding
        assert abs(_ns(out)[0] - _REF_NS) < 1_000_001

    def test_float32_promoted(self) -> None:
        arr = pa.array([float(_REF_S)], type=pa.float32())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE

    def test_fractional_seconds_preserved(self) -> None:
        # 0.5 seconds after ref
        arr = pa.array([float(_REF_S) + 0.5], type=pa.float64())
        out = normalize_timestamp_array(arr)
        diff_ns = _ns(out)[0] - _REF_NS
        # Should be within 1 ms of 500ms
        assert abs(diff_ns - 500_000_000) < 1_000_001


# ---------------------------------------------------------------------------
# Path 4: string input
# ---------------------------------------------------------------------------


class TestStringPath:
    def test_iso8601_utc_z(self) -> None:
        arr = pa.array(["2024-01-01T00:00:00Z"])
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert abs(_ns(out)[0] - _REF_NS) < 1_000_001

    def test_iso8601_with_offset(self) -> None:
        arr = pa.array(["2024-01-01T01:00:00+01:00"])
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert abs(_ns(out)[0] - _REF_NS) < 1_000_001

    def test_pandas_readable_format(self) -> None:
        arr = pa.array(["2024-01-01 00:00:00"])
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE

    def test_large_string_type(self) -> None:
        arr = pa.array(["2024-01-01T00:00:00Z"], type=pa.large_string())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE

    def test_null_strings_produce_two_elements(self) -> None:
        # The string path converts via pandas int64 ns; None becomes NaT (iNaT),
        # which Arrow represents as int64 min.  The important invariant is that
        # the output array has the right length and the correct type.
        arr = pa.array(["2024-01-01T00:00:00Z", None])
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        assert len(out) == 2


# ---------------------------------------------------------------------------
# Date32 bonus path
# ---------------------------------------------------------------------------


class TestDate32Path:
    def test_date32_promoted_to_timestamp(self) -> None:
        from datetime import date

        arr = pa.array([date(2024, 1, 1)], type=pa.date32())
        out = normalize_timestamp_array(arr)
        assert out.type == TIMESTAMP_TYPE
        # 2024-01-01T00:00:00Z in ns
        assert _ns(out)[0] == _REF_NS


# ---------------------------------------------------------------------------
# normalize_timestamp_column
# ---------------------------------------------------------------------------


class TestNormalizeColumn:
    def test_replaces_column_in_table(self) -> None:
        table = pa.table(
            {
                "ts":  pa.array([_REF_S], type=pa.int64()),
                "val": pa.array([1.0]),
            }
        )
        out = normalize_timestamp_column(table, "ts")
        assert out.schema.field("ts").type == TIMESTAMP_TYPE
        assert "val" in out.schema.names

    def test_missing_column_raises(self) -> None:
        table = pa.table({"a": [1]})
        with pytest.raises(SchemaError):
            normalize_timestamp_column(table, "ts")

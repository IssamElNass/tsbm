"""
Timestamp normalisation utilities.

All public functions return a ``pa.Array`` of type ``timestamp[ns, tz=UTC]``
regardless of the input representation.

Cascade order
-------------
1. Already a PyArrow timestamp type  → cast to timestamp[ns, UTC]
2. Integer array                     → nanoseconds if value > 1e18, else seconds
3. Float array                       → Unix seconds (fractional), convert to ns
4. String / large_string array       → pd.to_datetime(utc=True), fallback dateutil
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Union

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from tsbm.exceptions import SchemaError

# The canonical output type used throughout the suite
TIMESTAMP_TYPE = pa.timestamp("ns", tz="UTC")

# Heuristic thresholds for integer epoch detection
_NS_THRESHOLD = int(1e18)   # values above this → nanosecond epoch
_S_THRESHOLD  = int(1e9)    # values above this (but ≤ 1e18) → second epoch

ArrayLike = Union[pa.Array, pa.ChunkedArray]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def normalize_timestamp_array(arr: ArrayLike) -> pa.Array:
    """
    Normalise *arr* to ``timestamp[ns, tz=UTC]``.

    Parameters
    ----------
    arr:
        A PyArrow Array or ChunkedArray of any temporal, integer, float, or
        string type.

    Returns
    -------
    pa.Array
        Flat (non-chunked) array of type ``timestamp[ns, tz=UTC]``.

    Raises
    ------
    SchemaError
        If the array cannot be interpreted as timestamps.
    """
    flat = _flatten(arr)
    t = flat.type

    if pa.types.is_timestamp(t):
        return _from_timestamp(flat)

    if pa.types.is_integer(t):
        return _from_integer(flat)

    if pa.types.is_floating(t):
        return _from_float(flat)

    if pa.types.is_string(t) or pa.types.is_large_string(t):
        return _from_string(flat)

    if pa.types.is_date(t):
        # Cast date32/date64 → timestamp[ms] → timestamp[ns, UTC]
        ms_arr = flat.cast(pa.timestamp("ms"))
        return _from_timestamp(ms_arr)

    raise SchemaError(
        f"Cannot interpret column of type {t} as timestamps. "
        "Expected timestamp, integer, float, or string."
    )


def normalize_timestamp_column(
    table: pa.Table, col_name: str
) -> pa.Table:
    """
    Return a copy of *table* with *col_name* replaced by a normalised
    ``timestamp[ns, tz=UTC]`` array.
    """
    idx = table.schema.get_field_index(col_name)
    if idx < 0:
        raise SchemaError(f"Column {col_name!r} not found in table")

    normalised = normalize_timestamp_array(table.column(col_name))
    return table.set_column(idx, col_name, normalised)


# ---------------------------------------------------------------------------
# Private helpers — one function per cascade step
# ---------------------------------------------------------------------------


def _flatten(arr: ArrayLike) -> pa.Array:
    if isinstance(arr, pa.ChunkedArray):
        return arr.combine_chunks()
    return arr


def _from_timestamp(arr: pa.Array) -> pa.Array:
    """Cast any pa.timestamp type to timestamp[ns, UTC]."""
    t = arr.type
    # If already the right type, short-circuit
    if t == TIMESTAMP_TYPE:
        return arr
    # Attach UTC if no timezone info present
    if pa.types.is_timestamp(t) and t.tz is None:
        arr = arr.cast(pa.timestamp(t.unit, tz="UTC"))
    # Cast to ns
    return arr.cast(TIMESTAMP_TYPE)


def _from_integer(arr: pa.Array) -> pa.Array:
    """
    Interpret integer values as Unix epoch timestamps.

    Heuristic:
      value > 1e18  → nanoseconds
      value > 1e9   → seconds
      otherwise     → raise (likely not a timestamp)
    """
    # Use the first non-null value for the heuristic
    sample = _first_non_null_int(arr)
    if sample is None:
        # All nulls — return null timestamp array
        return pa.nulls(len(arr), type=TIMESTAMP_TYPE)

    abs_sample = abs(sample)
    if abs_sample > _NS_THRESHOLD:
        # Already nanoseconds
        return arr.cast(TIMESTAMP_TYPE)
    if abs_sample > _S_THRESHOLD:
        # Seconds → multiply by 1e9 to get nanoseconds
        ns_arr = pc.multiply(arr.cast(pa.int64()), pa.scalar(1_000_000_000, pa.int64()))
        return ns_arr.cast(TIMESTAMP_TYPE)

    raise SchemaError(
        f"Integer column sample value {sample} is too small to be a Unix epoch "
        "(expected > 1e9 for seconds or > 1e18 for nanoseconds)."
    )


def _from_float(arr: pa.Array) -> pa.Array:
    """
    Interpret float values as fractional Unix seconds.
    Converts to integer nanoseconds.
    """
    # float seconds → int64 nanoseconds
    ns_float = pc.multiply(arr.cast(pa.float64()), pa.scalar(1e9))
    ns_int = pc.cast(ns_float, pa.int64())
    return ns_int.cast(TIMESTAMP_TYPE)


def _from_string(arr: pa.Array) -> pa.Array:
    """
    Parse string timestamps.

    Strategy:
      1. Try pd.to_datetime with utc=True (vectorised, handles ISO 8601 and many
         common formats via pandas' inference).
      2. Fall back to dateutil.parser.parse per-element for exotic formats.
    """
    import pandas as pd

    py_list: list[str | None] = arr.to_pylist()

    # Step 1: pandas fast path
    try:
        parsed = pd.to_datetime(py_list, utc=True)
        # Convert to int64 nanoseconds then to Arrow
        ns_values = parsed.view("int64")  # already ns
        return pa.array(ns_values, type=pa.int64()).cast(TIMESTAMP_TYPE)
    except Exception:
        pass

    # Step 2: dateutil per-element fallback
    try:
        from dateutil import parser as dateutil_parser

        datetimes: list[datetime | None] = []
        for s in py_list:
            if s is None:
                datetimes.append(None)
            else:
                dt = dateutil_parser.parse(s)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                datetimes.append(dt)

        # Convert list of aware datetimes → Arrow timestamp[ns, UTC]
        return pa.array(
            [
                None if dt is None else int(dt.timestamp() * 1e9)
                for dt in datetimes
            ],
            type=pa.int64(),
        ).cast(TIMESTAMP_TYPE)
    except Exception as exc:
        raise SchemaError(
            f"Failed to parse string column as timestamps: {exc}"
        ) from exc


def _first_non_null_int(arr: pa.Array) -> int | None:
    """Return the first non-null integer value, or None if all null."""
    for val in arr:
        if val.is_valid:
            return val.as_py()
    return None

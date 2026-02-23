from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, ClassVar, Iterator

import pyarrow as pa
import pyarrow.compute as pc


# ---------------------------------------------------------------------------
# Column classification
# ---------------------------------------------------------------------------


class ColumnRole(Enum):
    """Semantic role of a column within a time-series dataset."""

    TIMESTAMP = "timestamp"
    TAG = "tag"        # low-cardinality string dimension (e.g. device_id, region)
    METRIC = "metric"  # numeric measurement (e.g. temperature, voltage)
    OTHER = "other"    # high-cardinality strings or unclassified columns


# ---------------------------------------------------------------------------
# Column specification
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class ColumnSpec:
    """Immutable description of a single dataset column."""

    name: str
    role: ColumnRole
    arrow_type: pa.DataType
    nullable: bool = False

    def __repr__(self) -> str:
        return (
            f"ColumnSpec(name={self.name!r}, role={self.role.value}, "
            f"type={self.arrow_type}, nullable={self.nullable})"
        )


# ---------------------------------------------------------------------------
# Dataset schema
# ---------------------------------------------------------------------------


@dataclass
class DatasetSchema:
    """
    Describes the structure of a benchmark dataset.

    Attributes:
        name:          Logical table/dataset name used for DDL and result labelling.
        timestamp_col: Name of the primary time column.
        tag_cols:      Ordered list of TAG column names (low-cardinality dimensions).
        metric_cols:   Ordered list of METRIC column names (numeric values).
        columns:       Full ordered list of ColumnSpec objects (all columns).
        row_count:     Number of rows in the dataset (0 if unknown).
    """

    name: str
    timestamp_col: str
    tag_cols: list[str] = field(default_factory=list)
    metric_cols: list[str] = field(default_factory=list)
    columns: list[ColumnSpec] = field(default_factory=list)
    row_count: int = 0

    # ------------------------------------------------------------------
    # Lookups
    # ------------------------------------------------------------------

    def get_col(self, name: str) -> ColumnSpec:
        for col in self.columns:
            if col.name == name:
                return col
        raise KeyError(f"Column {name!r} not found in schema {self.name!r}")

    def cols_by_role(self, role: ColumnRole) -> list[ColumnSpec]:
        return [c for c in self.columns if c.role == role]

    # ------------------------------------------------------------------
    # PyArrow schema
    # ------------------------------------------------------------------

    def arrow_schema(self) -> pa.Schema:
        return pa.schema(
            [pa.field(c.name, c.arrow_type, nullable=c.nullable) for c in self.columns]
        )

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "timestamp_col": self.timestamp_col,
            "tag_cols": self.tag_cols,
            "metric_cols": self.metric_cols,
            "columns": [
                {
                    "name": c.name,
                    "role": c.role.value,
                    "arrow_type": str(c.arrow_type),
                    "nullable": c.nullable,
                }
                for c in self.columns
            ],
            "row_count": self.row_count,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> DatasetSchema:
        columns = [
            ColumnSpec(
                name=c["name"],
                role=ColumnRole(c["role"]),
                arrow_type=pa.field("_", pa.large_string()).type  # placeholder
                if c["arrow_type"] == "large_string"
                else _parse_arrow_type(c["arrow_type"]),
                nullable=c.get("nullable", False),
            )
            for c in data.get("columns", [])
        ]
        return cls(
            name=data["name"],
            timestamp_col=data["timestamp_col"],
            tag_cols=data.get("tag_cols", []),
            metric_cols=data.get("metric_cols", []),
            columns=columns,
            row_count=data.get("row_count", 0),
        )

    def __repr__(self) -> str:
        return (
            f"DatasetSchema(name={self.name!r}, "
            f"ts={self.timestamp_col!r}, "
            f"tags={self.tag_cols}, "
            f"metrics={self.metric_cols}, "
            f"rows={self.row_count})"
        )


# ---------------------------------------------------------------------------
# Dataset container
# ---------------------------------------------------------------------------


@dataclass
class BenchmarkDataset:
    """
    A loaded (or lazily referenced) dataset ready for benchmarking.

    When `table` is None the dataset is in *lazy* mode — only `source_path`
    is populated. Adapters and benchmarks must call `load()` before accessing
    the Arrow table in that case.
    """

    schema: DatasetSchema
    table: pa.Table | None = None        # None for large / lazy datasets
    source_path: Path | None = None      # Path to CSV/Parquet on disk
    streaming: bool = False              # True for 1M+ row datasets (disk-based iteration)
    chunk_size: int = 100_000            # rows per batch when streaming=True
    _unit_conversions: dict[str, tuple[str, str]] = field(
        default_factory=dict, repr=False, compare=False
    )

    def is_lazy(self) -> bool:
        return self.table is None

    def load(self) -> pa.Table:
        """
        Load the Arrow table from `source_path` if in lazy mode.
        Returns the in-memory table (caches it on the instance).
        """
        if self.table is not None:
            return self.table
        if self.source_path is None:
            raise ValueError("BenchmarkDataset has neither table nor source_path")
        # Import here to avoid circular imports at module load time
        from tsbm.datasets.loader import load_dataset

        loaded = load_dataset(self.source_path, schema_hint=self.schema)
        self.table = loaded.table
        return self.table  # type: ignore[return-value]

    def iter_batches(self) -> Iterator[pa.Table]:
        """
        Yield Arrow tables in ``chunk_size`` row batches.

        * **Streaming mode** (``streaming=True``): reads from disk chunk-by-chunk
          without loading the entire file into memory.  Requires ``source_path``
          to be set.
        * **Eager mode** (``streaming=False``): slices the in-memory table.
          Calls ``load()`` first if the table is not yet materialised.
        """
        if self.streaming and self.source_path is not None:
            from tsbm.datasets.loader import _iter_dataset_batches
            for batch in _iter_dataset_batches(self.source_path, self.chunk_size):
                tbl = pa.Table.from_batches([batch]) if isinstance(batch, pa.RecordBatch) else batch
                if self._unit_conversions:
                    tbl = _apply_unit_conversions_table(tbl, self._unit_conversions)
                yield tbl
        else:
            tbl = self.load()
            offset = 0
            while offset < len(tbl):
                chunk = tbl.slice(offset, self.chunk_size)
                if self._unit_conversions:
                    chunk = _apply_unit_conversions_table(chunk, self._unit_conversions)
                yield chunk
                offset += self.chunk_size

    def slice(self, offset: int, length: int) -> pa.Table:
        """Return a slice of the Arrow table, loading it first if necessary."""
        tbl = self.load()
        return tbl.slice(offset, length)

    def __repr__(self) -> str:
        mode = "streaming" if self.streaming else ("lazy" if self.is_lazy() else f"{len(self.table)} rows")  # type: ignore[arg-type]
        return f"BenchmarkDataset(schema={self.schema.name!r}, {mode})"


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_ARROW_TYPE_MAP: dict[str, pa.DataType] = {
    "int8": pa.int8(),
    "int16": pa.int16(),
    "int32": pa.int32(),
    "int64": pa.int64(),
    "uint8": pa.uint8(),
    "uint16": pa.uint16(),
    "uint32": pa.uint32(),
    "uint64": pa.uint64(),
    "float": pa.float32(),
    "float32": pa.float32(),
    "double": pa.float64(),
    "float64": pa.float64(),
    "string": pa.string(),
    "large_string": pa.large_string(),
    "bool": pa.bool_(),
    "boolean": pa.bool_(),
    "timestamp[ns]": pa.timestamp("ns", tz="UTC"),
    "timestamp[us]": pa.timestamp("us", tz="UTC"),
    "timestamp[ms]": pa.timestamp("ms", tz="UTC"),
    "date32": pa.date32(),
}


# ---------------------------------------------------------------------------
# Unit conversion
# ---------------------------------------------------------------------------


def _apply_unit_conversions_table(
    table: pa.Table,
    conversions: dict[str, tuple[str, str]],
) -> pa.Table:
    """Apply unit conversions to a table in-place (returns new table)."""
    for col_name, (from_unit, to_unit) in conversions.items():
        if col_name not in table.schema.names:
            continue
        key = (from_unit.lower(), to_unit.lower())
        scale_offset = UnitConverter._CONVERSIONS.get(key)
        if scale_offset is None:
            raise ValueError(
                f"Unsupported unit conversion: {from_unit!r} → {to_unit!r}. "
                f"Supported: {sorted(UnitConverter._CONVERSIONS)}"
            )
        scale, offset = scale_offset
        col = table.column(col_name).cast(pa.float64())
        converted = pc.add(pc.multiply(col, scale), offset)
        idx = table.schema.get_field_index(col_name)
        table = table.set_column(idx, col_name, converted)
    return table


@dataclass
class UnitConverter:
    """
    Vectorized unit converter for Arrow metric columns.

    All conversions are linear transforms: ``result = value * scale + offset``.

    Supported pairs
    ---------------
    celsius    ↔ fahrenheit
    pa         ↔ bar
    pa         ↔ kpa
    m          ↔ km
    m_per_s    ↔ km_per_h

    Parameters
    ----------
    conversions:
        Mapping of column name → (from_unit, to_unit).
        Example: ``{"temperature": ("celsius", "fahrenheit")}``
    """

    conversions: dict[str, tuple[str, str]] = field(default_factory=dict)

    _CONVERSIONS: ClassVar[dict[tuple[str, str], tuple[float, float]]] = {
        ("celsius",    "fahrenheit"): (9 / 5,     32.0),
        ("fahrenheit", "celsius"):    (5 / 9,     -32.0 * 5 / 9),
        ("pa",         "bar"):        (1e-5,       0.0),
        ("bar",        "pa"):         (1e5,        0.0),
        ("pa",         "kpa"):        (1e-3,       0.0),
        ("kpa",        "pa"):         (1e3,        0.0),
        ("m",          "km"):         (1e-3,       0.0),
        ("km",         "m"):          (1e3,        0.0),
        ("m_per_s",    "km_per_h"):   (3.6,        0.0),
        ("km_per_h",   "m_per_s"):    (1 / 3.6,   0.0),
    }

    def apply(self, table: pa.Table) -> pa.Table:
        """Apply all configured conversions, returning a new Arrow table."""
        return _apply_unit_conversions_table(table, self.conversions)


# ---------------------------------------------------------------------------
# Arrow type parsing
# ---------------------------------------------------------------------------


def _parse_arrow_type(type_str: str) -> pa.DataType:
    """Best-effort parse of a PyArrow type string produced by str(pa.DataType)."""
    t = _ARROW_TYPE_MAP.get(type_str.lower())
    if t is not None:
        return t
    # Handle timestamp[ns, UTC] style strings
    if type_str.startswith("timestamp["):
        # e.g. "timestamp[ns, UTC]" or "timestamp[us, tz=UTC]"
        inner = type_str[len("timestamp["):].rstrip("]")
        parts = [p.strip().lstrip("tz=") for p in inner.split(",")]
        unit = parts[0]
        tz = parts[1] if len(parts) > 1 else None
        return pa.timestamp(unit, tz=tz)
    # Fallback: large_string
    return pa.large_string()

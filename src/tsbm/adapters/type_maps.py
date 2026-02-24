"""
Cross-database type mapping and DDL generation.

This module is pure data + transformation logic — no I/O, no imports of
database drivers.  Every adapter imports from here; nothing here imports
from any adapter.

Type mapping table
------------------
PyArrow type          QuestDB          CrateDB                   TimescaleDB
─────────────────────────────────────────────────────────────────────────────
int8 / int16 / int32  INT              INTEGER                   INTEGER
int64                 LONG             BIGINT                    BIGINT
uint8/16/32           INT              INTEGER                   INTEGER
uint64                LONG             BIGINT                    BIGINT
float32               FLOAT            REAL                      REAL
float64               DOUBLE           DOUBLE PRECISION          DOUBLE PRECISION
string (TAG role)     SYMBOL           TEXT                      TEXT
string (other)        STRING           TEXT                      TEXT
large_string          STRING           TEXT                      TEXT
bool                  BOOLEAN          BOOLEAN                   BOOLEAN
timestamp[ns/us/ms]   TIMESTAMP        TIMESTAMP WITH TIME ZONE  TIMESTAMPTZ
date32 / date64       TIMESTAMP        TIMESTAMP WITH TIME ZONE  TIMESTAMPTZ
"""
from __future__ import annotations

import pyarrow as pa

from tsbm.datasets.schema import ColumnRole, ColumnSpec, DatasetSchema

# ---------------------------------------------------------------------------
# Type resolution
# ---------------------------------------------------------------------------

# Supported DB identifiers
DB_QUESTDB     = "questdb"
DB_CRATEDB     = "cratedb"
DB_TIMESCALEDB = "timescaledb"


def get_db_type(
    arrow_type: pa.DataType,
    role: ColumnRole,
    db: str,
) -> str:
    """
    Return the SQL type string for *arrow_type* in *db*.

    Parameters
    ----------
    arrow_type:
        The PyArrow data type of the column.
    role:
        Semantic role of the column (TAG / METRIC / TIMESTAMP / OTHER).
    db:
        One of ``"questdb"``, ``"cratedb"``, or ``"timescaledb"``.
    """
    if pa.types.is_timestamp(arrow_type) or pa.types.is_date(arrow_type):
        return _ts_type(db)

    if pa.types.is_boolean(arrow_type):
        return "BOOLEAN"

    if pa.types.is_integer(arrow_type):
        return _int_type(arrow_type, db)

    if pa.types.is_floating(arrow_type):
        return _float_type(arrow_type, db)

    if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
        return _string_type(role, db)

    # Fallback: store as text
    return "TEXT" if db != DB_QUESTDB else "STRING"


def _ts_type(db: str) -> str:
    return {
        DB_QUESTDB:     "TIMESTAMP",
        DB_CRATEDB:     "TIMESTAMP WITH TIME ZONE",
        DB_TIMESCALEDB: "TIMESTAMPTZ",
    }[db]


def _int_type(arrow_type: pa.DataType, db: str) -> str:
    # int8, int16, int32, uint8, uint16, uint32 → 32-bit integer
    # int64, uint64 → 64-bit integer
    is_64 = arrow_type in (pa.int64(), pa.uint64())
    if db == DB_QUESTDB:
        return "LONG" if is_64 else "INT"
    return "BIGINT" if is_64 else "INTEGER"


def _float_type(arrow_type: pa.DataType, db: str) -> str:
    is_32 = arrow_type == pa.float32()
    if db == DB_QUESTDB:
        return "FLOAT" if is_32 else "DOUBLE"
    return "REAL" if is_32 else "DOUBLE PRECISION"


def _string_type(role: ColumnRole, db: str) -> str:
    if db == DB_QUESTDB:
        # QuestDB: SYMBOL is dictionary-encoded and optimised for tag queries
        return "SYMBOL" if role == ColumnRole.TAG else "STRING"
    return "TEXT"


# ---------------------------------------------------------------------------
# DDL generation
# ---------------------------------------------------------------------------


_QUESTDB_PARTITION_VALUES = frozenset({"HOUR", "DAY", "WEEK", "MONTH", "YEAR"})


def arrow_table_to_ddl(
    schema: DatasetSchema,
    db: str,
    table_name: str,
    partition_by: str = "DAY",
) -> str:
    """
    Generate a ``CREATE TABLE`` statement for *db* from *schema*.

    Notes
    -----
    * QuestDB    — appends ``TIMESTAMP(col) PARTITION BY <partition_by>``.
                   Valid values: HOUR, DAY, WEEK, MONTH, YEAR.
    * CrateDB    — plain CREATE TABLE (no partitioning; CrateDB's native
                   partitioning requires a generated column and is handled
                   separately if needed)
    * TimescaleDB — plain CREATE TABLE; ``create_hypertable`` is called
                    separately by the adapter after this DDL

    The statement uses ``CREATE TABLE IF NOT EXISTS`` for idempotency.
    """
    col_defs = _column_definitions(schema, db)

    if db == DB_QUESTDB:
        pb = partition_by.upper()
        if pb not in _QUESTDB_PARTITION_VALUES:
            raise ValueError(
                f"Invalid QuestDB partition_by {partition_by!r}. "
                f"Valid values: {sorted(_QUESTDB_PARTITION_VALUES)}"
            )
        return _questdb_ddl(table_name, col_defs, schema.timestamp_col, pb)
    if db == DB_CRATEDB:
        return _cratedb_ddl(table_name, col_defs)
    if db == DB_TIMESCALEDB:
        return _timescaledb_ddl(table_name, col_defs)

    raise ValueError(f"Unknown database: {db!r}")


def _column_definitions(schema: DatasetSchema, db: str) -> list[str]:
    """Return a list of ``"col_name TYPE [NOT NULL]"`` strings."""
    defs = []
    for col in schema.columns:
        sql_type = get_db_type(col.arrow_type, col.role, db)
        # QuestDB CREATE TABLE does not support NOT NULL constraints
        if db == DB_QUESTDB:
            null_clause = ""
        else:
            null_clause = "" if col.nullable else " NOT NULL"
            # TimescaleDB requires the primary timestamp to be NOT NULL
            if col.name == schema.timestamp_col and db == DB_TIMESCALEDB:
                null_clause = " NOT NULL"
        defs.append(f"    {_quote(col.name, db)} {sql_type}{null_clause}")
    return defs


def _questdb_ddl(table_name: str, col_defs: list[str], ts_col: str, partition_by: str = "DAY") -> str:
    cols = ",\n".join(col_defs)
    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        f"{cols}\n"
        f') TIMESTAMP("{ts_col}") PARTITION BY {partition_by};'
    )


def _cratedb_ddl(table_name: str, col_defs: list[str]) -> str:
    cols = ",\n".join(col_defs)
    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        f"{cols}\n"
        f");"
    )


def _timescaledb_ddl(table_name: str, col_defs: list[str]) -> str:
    cols = ",\n".join(col_defs)
    return (
        f'CREATE TABLE IF NOT EXISTS "{table_name}" (\n'
        f"{cols}\n"
        f");"
    )


def _quote(name: str, db: str) -> str:
    """
    Quote a column name with SQL double-quotes.

    CrateDB and TimescaleDB share PostgreSQL's large reserved-word set
    (OFFSET, LIMIT, ORDER, KEY, INDEX, GROUP, …).  Always quoting is safer
    than maintaining a whitelist and matches the quoting used everywhere else
    in the adapters (UNNEST inserts, query templates, etc.).
    QuestDB identifiers are left unquoted for compatibility with its parser.
    """
    if db == DB_QUESTDB:
        return name
    return f'"{name}"'


# ---------------------------------------------------------------------------
# Array type strings for UNNEST (CrateDB only)
# ---------------------------------------------------------------------------


def cratedb_array_type(col: ColumnSpec) -> str:
    """
    Return the CrateDB SQL array cast type for use in UNNEST parameters.

    Uses ``TIMESTAMPTZ[]`` instead of ``TIMESTAMP WITH TIME ZONE[]`` for
    timestamp columns: the ``WITH`` keyword inside a ``::`` cast confuses
    CrateDB's ANTLR parser, which tries to interpret it as a CTE clause
    (``WITH [RECURSIVE] …``).  ``TIMESTAMPTZ`` is a supported single-word
    alias in CrateDB 5.x (``crate:latest``).

    E.g. ``TIMESTAMPTZ[]``, ``TEXT[]``, ``DOUBLE PRECISION[]``.
    """
    if pa.types.is_timestamp(col.arrow_type) or pa.types.is_date(col.arrow_type):
        return "TIMESTAMPTZ[]"
    base = get_db_type(col.arrow_type, col.role, DB_CRATEDB)
    return f"{base}[]"

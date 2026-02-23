"""
DatabaseAdapter Protocol — the single interface all database adapters must satisfy.

Uses ``@runtime_checkable`` structural subtyping (PEP 544) so that adapter
classes do not need to inherit from this Protocol.  Adding a fourth database
is a single-file change with no modifications to the benchmark engine.

Lifecycle contract
------------------
1. ``__init__(config)``       — store config, do not connect
2. ``connect()``              — open connection(s), must be called first
3. ``create_table(schema)``   — run DDL; stores the schema for later use
4. ``ingest_batch(table, …)`` — write data; returns timing
5. ``flush()``                — ensure data is visible for reads (CrateDB: REFRESH TABLE)
6. ``execute_query(sql, …)``  — run a SELECT; returns rows + timing
7. ``disconnect()``           — close connections cleanly
"""
from __future__ import annotations

from datetime import datetime
from typing import Protocol, runtime_checkable

import pyarrow as pa

from tsbm.datasets.schema import DatasetSchema
from tsbm.metrics.timer import TimingResult


@runtime_checkable
class DatabaseAdapter(Protocol):
    """
    Structural protocol that every database adapter must satisfy.

    Attributes
    ----------
    name:
        Short identifier used in result labelling, e.g. ``"questdb"``.
    """

    name: str

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open all required connections (PostgreSQL wire + native client)."""
        ...

    async def disconnect(self) -> None:
        """Close all connections and release resources."""
        ...

    async def health_check(self) -> bool:
        """
        Return True if the database is reachable and accepting queries.
        Must not raise; return False on any connection error.
        """
        ...

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def create_table(self, schema: DatasetSchema) -> None:
        """
        Create the benchmark table and any required auxiliary objects
        (e.g. TimescaleDB hypertable).

        Implementations must store *schema* so ``ingest_batch`` and
        ``flush`` can access column roles and the designated timestamp.
        """
        ...

    async def drop_table(self, table_name: str) -> None:
        """Drop the table if it exists (idempotent)."""
        ...

    async def table_exists(self, table_name: str) -> bool:
        """Return True if *table_name* exists in the database."""
        ...

    # ------------------------------------------------------------------
    # Data operations
    # ------------------------------------------------------------------

    async def ingest_batch(
        self,
        table: pa.Table,
        table_name: str,
    ) -> TimingResult:
        """
        Write *table* into *table_name* using the database's native fast path.

        * QuestDB    — ILP over HTTP via the ``questdb`` sender
        * CrateDB    — UNNEST bulk INSERT via asyncpg
        * TimescaleDB — COPY via ``asyncpg.copy_records_to_table``

        ``create_table`` must have been called with a compatible schema first.
        """
        ...

    async def flush(self) -> None:
        """
        Ensure that all previously ingested rows are visible for reads.

        * CrateDB    — ``REFRESH TABLE <table>`` (eventual consistency)
        * QuestDB    — no-op (ILP HTTP is synchronous; data visible on ack)
        * TimescaleDB — no-op (COPY commits immediately)
        """
        ...

    async def execute_query(
        self,
        sql: str,
        params: tuple = (),
    ) -> tuple[list[dict], TimingResult]:
        """
        Execute *sql* and return ``(rows, timing)``.

        *rows* is a list of dicts (column name → value).
        *timing* covers only the network round-trip + server execution time.
        """
        ...

    async def get_row_count(self, table_name: str) -> int:
        """Return the current row count of *table_name*."""
        ...

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def get_version(self) -> str:
        """Return a human-readable database version string."""
        ...


@runtime_checkable
class MVCapableAdapter(Protocol):
    """
    Optional extension protocol for adapters that support materialized views
    or continuous aggregates.

    Usage
    -----
    Check support before use::

        if isinstance(adapter, MVCapableAdapter):
            await adapter.create_materialized_view(...)

    Note: :class:`QuestDBAdapter` implements this protocol structurally but
    all methods are no-ops — ``view_exists()`` always returns ``False`` which
    signals to benchmarks that MV measurements should be skipped.
    """

    async def create_materialized_view(
        self,
        view_name: str,
        source_table: str,
        schema: DatasetSchema,
        granularity: str = "1 hour",
    ) -> None:
        """
        Create a materialized view or continuous aggregate over *source_table*.

        * **TimescaleDB** — ``CREATE MATERIALIZED VIEW ... WITH (timescaledb.continuous)``
          followed by ``CALL refresh_continuous_aggregate(view, NULL, NULL)``
        * **CrateDB** — creates a plain summary table and fills it with
          ``INSERT INTO ... SELECT DATE_BIN(...) ... GROUP BY ...``
        * **QuestDB** — no-op; emits a warning
        """
        ...

    async def refresh_materialized_view(
        self,
        view_name: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> TimingResult:
        """
        Refresh the materialized view for the given time window.

        Returns a :class:`TimingResult` covering the refresh duration.

        * **TimescaleDB** — ``CALL refresh_continuous_aggregate(view, start, end)``
        * **CrateDB** — ``DELETE`` affected rows then ``INSERT ... SELECT`` + ``REFRESH TABLE``
        * **QuestDB** — returns an empty ``TimingResult``
        """
        ...

    async def drop_materialized_view(self, view_name: str) -> None:
        """Drop the materialized view / summary table (idempotent)."""
        ...

    async def view_exists(self, view_name: str) -> bool:
        """
        Return True if the named view / summary table exists.

        Used by benchmarks to detect QuestDB no-ops (always False for QuestDB).
        """
        ...


# ---------------------------------------------------------------------------
# Retry helpers
# ---------------------------------------------------------------------------

import asyncpg.exceptions as _apg_exc  # noqa: E402

RETRYABLE_CONNECTION_EXCEPTIONS: tuple[type[Exception], ...] = (
    _apg_exc.ConnectionDoesNotExistError,
    _apg_exc.ConnectionFailureError,
    _apg_exc.InterfaceError,        # "connection is closed" local errors
    _apg_exc.InternalClientError,
    _apg_exc.CannotConnectNowError,
    _apg_exc.TooManyConnectionsError,
)

"""
QuestDB adapter.

Ingestion path  — ILP over HTTP via the official ``questdb`` Python package
                  (v1.x, ``questdb.ingress.Sender``).  The sender is
                  synchronous and blocking so it is wrapped in
                  ``asyncio.to_thread`` to avoid stalling the event loop.

Query path      — asyncpg on the PGWire port (default 8812).
                  ``statement_cache_size=0`` is required because QuestDB's
                  PGWire support does not fully implement prepared-statement
                  caching.

Materialized    — QuestDB supports native materialized views via
views             ``CREATE MATERIALIZED VIEW ... WITH BASE ... REFRESH IMMEDIATE
                  AS (SELECT ... SAMPLE BY <interval>)``.  The initial refresh
                  is asynchronous; ``_wait_for_mv_refresh`` polls
                  ``materialized_views()`` until ``refresh_base_table_txn >=
                  base_table_txn``.

Install         — ``pip install tsbm[questdb]``  (``questdb>=1.0``)
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

import asyncpg
import pandas as pd
import pyarrow as pa

from tsbm.adapters.base import RETRYABLE_CONNECTION_EXCEPTIONS
from tsbm.adapters.type_maps import (
    DB_QUESTDB,
    arrow_table_to_ddl,
)
from tsbm.config.settings import QuestDBConfig
from tsbm.datasets.schema import ColumnRole, DatasetSchema
from tsbm.exceptions import ConnectionError, IngestionError, QueryError
from tsbm.metrics.timer import TimingResult, estimate_table_bytes, timed_operation

logger = logging.getLogger(__name__)


class QuestDBAdapter:
    """
    Adapter for QuestDB.

    Parameters
    ----------
    config:
        ``QuestDBConfig`` from ``get_settings().databases.questdb``.
    """

    name = "questdb"

    def __init__(self, config: QuestDBConfig) -> None:
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._schema: DatasetSchema | None = None
        self._current_table: str | None = None
        self._mv_granularity: str = "1 hour"
        self._mv_ts_col: str | None = None
        self._sender: Any = None  # Reusable questdb.ingress.Sender instance

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Open a connection pool on the PGWire port."""
        try:
            self._pool = await asyncpg.create_pool(
                host=self._config.host,
                port=self._config.pg_port,
                database="qdb",
                user="admin",
                password="quest",
                statement_cache_size=0,  # QuestDB PGWire has partial PS support
                command_timeout=60,
                min_size=1,
                max_size=32,
            )
            logger.debug("QuestDB: connected on port %d", self._config.pg_port)
        except Exception as exc:
            raise ConnectionError(f"QuestDB connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._sender is not None:
            try:
                self._sender.__exit__(None, None, None)
            except Exception as exc:
                logger.debug("QuestDB: sender close error: %s", exc)
            self._sender = None
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.debug("QuestDB: disconnected")

    async def _ensure_connected(self) -> None:
        """Reconnect the pool if it was never created or was closed."""
        if self._pool is None:
            logger.warning("QuestDB: pool not initialised — reconnecting")
            await self.connect()

    async def health_check(self) -> bool:
        try:
            await self._pool.fetchval("SELECT 1")  # type: ignore[union-attr]
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def create_table(self, schema: DatasetSchema) -> None:
        """Create the QuestDB table with TIMESTAMP designation and DAY partitioning."""
        self._schema = schema
        self._current_table = schema.name

        # QuestDB's CREATE TABLE doesn't support IF NOT EXISTS — use WAL mode
        # Drop first to guarantee a clean state
        await self.drop_table(schema.name)
        ddl = arrow_table_to_ddl(schema, DB_QUESTDB, schema.name, self._config.partition_by)
        logger.debug("QuestDB DDL:\n%s", ddl)
        try:
            await self._pool.execute(ddl)  # type: ignore[union-attr]
        except Exception as exc:
            raise QueryError(f"QuestDB create_table failed: {exc}") from exc

    async def drop_table(self, table_name: str) -> None:
        try:
            await self._pool.execute(  # type: ignore[union-attr]
                f'DROP TABLE IF EXISTS "{table_name}"'
            )
        except Exception as exc:
            logger.warning("QuestDB drop_table %s: %s", table_name, exc)

    async def table_exists(self, table_name: str) -> bool:
        try:
            row = await self._pool.fetchrow(  # type: ignore[union-attr]
                "SELECT table_name FROM tables() WHERE table_name = $1",
                table_name,
            )
            return row is not None
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Ingestion
    # ------------------------------------------------------------------

    async def ingest_batch(
        self,
        table: pa.Table,
        table_name: str,
    ) -> TimingResult:
        """
        Write *table* via ILP over HTTP using a reusable questdb Sender.

        The Sender is created lazily on first call and reused across batches
        to avoid HTTP connection setup overhead.  ``sender.flush()`` is called
        per batch to ensure data is committed.

        The Sender is synchronous; we run it in a thread pool executor
        so the asyncio event loop is not blocked (important for the mixed
        read/write benchmark).
        """
        if self._schema is None:
            raise IngestionError("call create_table() before ingest_batch()")

        schema = self._schema
        conf_str = f"http::addr={self._config.host}:{self._config.ilp_port};"
        symbols = [c.name for c in schema.columns if c.role == ColumnRole.TAG]
        ts_col = schema.timestamp_col
        n_rows = len(table)
        n_bytes = estimate_table_bytes(table)

        # Build a pandas DataFrame that the QuestDB sender understands.
        df = _table_to_pandas_safe(table, schema)

        def _sync_send() -> TimingResult:
            from questdb.ingress import Sender  # noqa: PLC0415

            # Reuse sender across batches — avoids HTTP connection setup per call
            if self._sender is None:
                self._sender = Sender.from_conf(conf_str)
                self._sender.__enter__()

            with timed_operation(rows=n_rows, bytes_count=n_bytes, disable_gc=False) as result:
                self._sender.dataframe(
                    df,
                    table_name=table_name,
                    symbols=symbols,
                    at=ts_col,
                )
                self._sender.flush()
            return result

        await self._ensure_connected()
        try:
            return await asyncio.to_thread(_sync_send)
        except Exception as exc:
            # On error, close the sender so next call creates a fresh one
            if self._sender is not None:
                try:
                    self._sender.__exit__(None, None, None)
                except Exception:
                    pass
                self._sender = None
            raise IngestionError(f"QuestDB ingest_batch failed: {exc}") from exc

    async def flush(self) -> None:
        """No-op — ILP HTTP acknowledges after commit."""
        pass

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    async def execute_query(
        self,
        sql: str,
        params: tuple = (),
    ) -> tuple[list[dict], TimingResult]:
        await self._ensure_connected()
        last_exc: Exception | None = None
        for attempt in range(3):
            if attempt > 0:
                await asyncio.sleep(0.5 * attempt)
            try:
                with timed_operation() as result:
                    rows = await self._pool.fetch(sql, *params)  # type: ignore[union-attr]
                return [dict(r) for r in rows], result
            except RETRYABLE_CONNECTION_EXCEPTIONS as exc:
                last_exc = exc
                logger.warning(
                    "QuestDB: connection error on attempt %d/3: %s", attempt + 1, exc
                )
                continue
            except Exception as exc:
                raise QueryError(
                    f"QuestDB query failed [{type(exc).__name__}: {exc}] SQL: {sql}"
                ) from exc
        raise QueryError(
            f"QuestDB query failed after 3 attempts [{type(last_exc).__name__}: {last_exc}] SQL: {sql}"
        ) from last_exc

    async def get_row_count(self, table_name: str) -> int:
        row = await self._pool.fetchrow(  # type: ignore[union-attr]
            f'SELECT count() FROM "{table_name}"'
        )
        return int(row[0]) if row else 0

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def get_version(self) -> str:
        try:
            row = await self._pool.fetchrow(  # type: ignore[union-attr]
                "SELECT build FROM build()"
            )
            return str(row[0]) if row else "unknown"
        except Exception:
            try:
                row = await self._pool.fetchrow("SELECT version()")  # type: ignore[union-attr]
                return str(row[0]) if row else "unknown"
            except Exception:
                return "unknown"

    # ------------------------------------------------------------------
    # MVCapableAdapter — native QuestDB materialized views (SAMPLE BY)
    # ------------------------------------------------------------------

    async def create_materialized_view(
        self,
        view_name: str,
        source_table: str,
        schema: Any,
        granularity: str = "1 hour",
    ) -> None:
        """
        Create a QuestDB native materialized view using SAMPLE BY.

        Syntax::

            CREATE MATERIALIZED VIEW <view_name>
            WITH BASE <source_table> REFRESH IMMEDIATE AS (
                SELECT <ts>, [tags,] avg(<m>) AS avg_<m>, ...
                FROM <source_table>
                SAMPLE BY <interval>
            )

        The initial refresh is asynchronous; this method polls
        ``materialized_views()`` until the refresh is complete.

        The MV keeps the original timestamp column name (not ``ts_bucket``).
        """
        self._mv_granularity = granularity
        self._mv_ts_col = schema.timestamp_col

        ts = schema.timestamp_col
        tag_cols = schema.tag_cols
        metric_cols = schema.metric_cols
        sample_interval = _granularity_to_questdb(granularity)

        tag_select = (", ".join(f'"{t}"' for t in tag_cols) + ", ") if tag_cols else ""
        metric_select = ", ".join(
            f'avg("{m}") AS "avg_{m}", min("{m}") AS "min_{m}", max("{m}") AS "max_{m}"'
            for m in metric_cols
        )

        drop_sql = f'DROP MATERIALIZED VIEW IF EXISTS "{view_name}"'
        create_sql = (
            f'CREATE MATERIALIZED VIEW "{view_name}" '
            f'WITH BASE "{source_table}" REFRESH IMMEDIATE AS ('
            f'SELECT "{ts}", {tag_select}{metric_select} '
            f'FROM "{source_table}" '
            f"SAMPLE BY {sample_interval}"
            f")"
        )

        try:
            await self._pool.execute(drop_sql)  # type: ignore[union-attr]
            await self._pool.execute(create_sql)  # type: ignore[union-attr]
            await _wait_for_mv_refresh(self._pool, view_name)  # type: ignore[union-attr]
            logger.debug("QuestDB: created materialized view %r", view_name)
        except Exception as exc:
            raise QueryError(
                f"QuestDB create_materialized_view failed: {exc}"
            ) from exc

    async def refresh_materialized_view(
        self,
        view_name: str,
        start: Any = None,
        end: Any = None,
    ) -> TimingResult:
        """
        Refresh a QuestDB materialized view.

        * When ``start``/``end`` are provided (late-arrival case) a ``FULL``
          refresh is issued so that already-processed time buckets are
          recomputed with the newly inserted rows.
        * Otherwise an incremental refresh picks up new WAL transactions.

        The call blocks until ``materialized_views()`` confirms the refresh
        is complete.
        """
        full = start is not None and end is not None
        refresh_sql = f'REFRESH MATERIALIZED VIEW "{view_name}"' + (
            " FULL" if full else ""
        )
        try:
            with timed_operation() as result:
                await self._pool.execute(refresh_sql)  # type: ignore[union-attr]
                await _wait_for_mv_refresh(self._pool, view_name)  # type: ignore[union-attr]
            return result
        except Exception as exc:
            raise QueryError(
                f"QuestDB refresh_materialized_view failed: {exc}"
            ) from exc

    async def drop_materialized_view(self, view_name: str) -> None:
        """Drop the QuestDB materialized view (idempotent)."""
        try:
            await self._pool.execute(  # type: ignore[union-attr]
                f'DROP MATERIALIZED VIEW IF EXISTS "{view_name}"'
            )
        except Exception as exc:
            logger.warning("QuestDB drop_materialized_view %r: %s", view_name, exc)

    async def view_exists(self, view_name: str) -> bool:
        """Return True if the QuestDB materialized view exists."""
        try:
            rows = await self._pool.fetch(  # type: ignore[union-attr]
                "SELECT view_name FROM materialized_views() WHERE view_name = $1",
                view_name,
            )
            return len(rows) > 0
        except Exception:
            return False

    @property
    def mv_ts_col(self) -> str | None:
        """Timestamp column name used in the most recently created MV."""
        return self._mv_ts_col


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _granularity_to_questdb(granularity: str) -> str:
    """Convert e.g. ``'1 hour'`` to QuestDB SAMPLE BY notation ``'1h'``."""
    _MAP = {
        "1 minute": "1m",
        "5 minutes": "5m",
        "15 minutes": "15m",
        "30 minutes": "30m",
        "1 hour": "1h",
        "6 hours": "6h",
        "12 hours": "12h",
        "1 day": "1d",
    }
    return _MAP.get(granularity.lower(), "1h")


async def _wait_for_mv_refresh(
    conn: asyncpg.Pool,
    view_name: str,
    timeout_seconds: float = 120.0,
    poll_interval: float = 0.5,
) -> None:
    """
    Poll ``materialized_views()`` until the view's refresh is complete.

    QuestDB signals completion when
    ``refresh_base_table_txn >= base_table_txn``.  If the poll times out
    a warning is logged but no exception is raised (the caller can still
    proceed — data may simply not be fully visible yet).
    """
    deadline = time.monotonic() + timeout_seconds
    while time.monotonic() < deadline:
        try:
            rows = await conn.fetch(
                "SELECT refresh_base_table_txn, base_table_txn "
                "FROM materialized_views() "
                "WHERE view_name = $1",
                view_name,
            )
            if rows:
                row = rows[0]
                refresh_txn = row["refresh_base_table_txn"]
                base_txn = row["base_table_txn"]
                if (
                    refresh_txn is not None
                    and base_txn is not None
                    and refresh_txn >= base_txn
                ):
                    return
        except Exception as exc:
            logger.debug("QuestDB MV poll error for %r: %s", view_name, exc)
        await asyncio.sleep(poll_interval)
    logger.warning(
        "QuestDB MV %r: refresh did not complete within %.0fs; "
        "benchmark results may be incomplete.",
        view_name,
        timeout_seconds,
    )


def _table_to_pandas_safe(table: pa.Table, schema: DatasetSchema) -> pd.DataFrame:
    """
    Convert an Arrow table to pandas without triggering the PyArrow Windows
    timezone-database requirement.

    Timestamp columns are converted via int64 nanoseconds → numpy array →
    ``pd.to_datetime`` to avoid ``pc.cast(timestamp_with_tz, …)`` which
    needs tzdata on Windows.  Using a numpy array instead of a Python list
    as the intermediate avoids O(n) Python object creation overhead.
    """
    cols: dict[str, object] = {}
    for col_spec in schema.columns:
        raw = table.column(col_spec.name)
        if col_spec.role == ColumnRole.TIMESTAMP:
            # int64 ns → numpy array (zero-copy) → pd.DatetimeIndex
            ns_array = raw.cast(pa.int64()).to_numpy()
            cols[col_spec.name] = pd.to_datetime(ns_array, unit="ns", utc=True)
        else:
            cols[col_spec.name] = raw.to_pylist()
    return pd.DataFrame(cols)

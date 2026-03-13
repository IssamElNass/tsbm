"""
TimescaleDB adapter.

Ingestion path  — ``asyncpg.copy_records_to_table()`` (PostgreSQL COPY
                  protocol), the fastest programmatic ingestion path for
                  Postgres-compatible databases.  No extra package needed
                  beyond asyncpg.

Hypertable      — ``SELECT create_hypertable(table, ts_col, if_not_exists=>TRUE)``
                  is called after ``CREATE TABLE`` in ``create_table()``.

Port            — 5433 by default (Docker host maps 5433→5432 to avoid the
                  CrateDB port conflict on 5432).

Query path      — asyncpg (no statement cache size restriction needed).
"""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import asyncpg
import pyarrow as pa

from tsbm.adapters.base import RETRYABLE_CONNECTION_EXCEPTIONS
from tsbm.adapters.type_maps import DB_TIMESCALEDB, arrow_table_to_ddl
from tsbm.config.settings import TimescaleDBConfig
from tsbm.datasets.schema import ColumnRole, DatasetSchema
from tsbm.exceptions import ConnectionError, IngestionError, QueryError
from tsbm.metrics.timer import TimingResult, estimate_table_bytes, timed_operation

logger = logging.getLogger(__name__)

class TimescaleDBAdapter:
    """
    Adapter for TimescaleDB (PostgreSQL + TimescaleDB extension).

    Parameters
    ----------
    config:
        ``TimescaleDBConfig`` from ``get_settings().databases.timescaledb``.
    """

    name = "timescaledb"

    def __init__(self, config: TimescaleDBConfig) -> None:
        self._config = config
        self._pool: asyncpg.Pool | None = None
        self._schema: DatasetSchema | None = None
        self._current_table: str | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        try:
            self._pool = await asyncpg.create_pool(
                host=self._config.host,
                port=self._config.pg_port,
                database=self._config.dbname,
                user=self._config.user,
                password=self._config.password,
                command_timeout=300,
                min_size=4,
                max_size=32,
            )
            logger.debug(
                "TimescaleDB: connected on port %d db=%s",
                self._config.pg_port,
                self._config.dbname,
            )
        except Exception as exc:
            raise ConnectionError(f"TimescaleDB connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.debug("TimescaleDB: disconnected")

    async def _ensure_connected(self) -> None:
        """Reconnect the pool if it was never created or was closed."""
        if self._pool is None:
            logger.warning("TimescaleDB: pool not initialised — reconnecting")
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
        """
        Create a regular PostgreSQL table then convert it to a hypertable.

        Two-step process:
          1. ``CREATE TABLE IF NOT EXISTS …``
          2. ``SELECT create_hypertable(table, ts_col, if_not_exists => TRUE)``
        """
        self._schema = schema
        self._current_table = schema.name

        ddl = arrow_table_to_ddl(schema, DB_TIMESCALEDB, schema.name)
        logger.debug("TimescaleDB DDL:\n%s", ddl)
        try:
            await self._pool.execute(ddl)  # type: ignore[union-attr]
            await self._pool.execute(  # type: ignore[union-attr]
                "SELECT create_hypertable($1, $2, if_not_exists => TRUE, "
                "chunk_time_interval => $3::INTERVAL)",
                schema.name,
                schema.timestamp_col,
                self._config.chunk_time_interval,
            )
        except Exception as exc:
            raise QueryError(f"TimescaleDB create_table failed: {exc}") from exc

    async def drop_table(self, table_name: str) -> None:
        try:
            await self._pool.execute(  # type: ignore[union-attr]
                f'DROP TABLE IF EXISTS "{table_name}" CASCADE'
            )
        except Exception as exc:
            logger.warning("TimescaleDB drop_table %s: %s", table_name, exc)

    async def table_exists(self, table_name: str) -> bool:
        try:
            row = await self._pool.fetchrow(  # type: ignore[union-attr]
                "SELECT tablename FROM pg_tables WHERE tablename = $1",
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
        Bulk insert via PostgreSQL COPY protocol.

        ``asyncpg.copy_records_to_table`` streams rows over the binary COPY
        wire format — the fastest programmatic path for TimescaleDB.

        Timestamp values are converted from nanosecond int64 to UTC-aware
        ``datetime`` objects (microsecond precision, which matches
        TIMESTAMPTZ's maximum resolution in PostgreSQL).
        """
        if self._schema is None:
            raise IngestionError("call create_table() before ingest_batch()")

        schema = self._schema
        n_rows = len(table)
        n_bytes = estimate_table_bytes(table)

        columns = [c.name for c in schema.columns]
        records = _build_records(table, schema)

        await self._ensure_connected()
        try:
            with timed_operation(rows=n_rows, bytes_count=n_bytes) as result:
                async with self._pool.acquire() as conn:  # type: ignore[union-attr]
                    await conn.copy_records_to_table(
                        table_name,
                        records=records,
                        columns=columns,
                    )
            return result
        except Exception as exc:
            raise IngestionError(
                f"TimescaleDB ingest_batch failed: {exc}"
            ) from exc

    async def flush(self, expected_rows: int | None = None) -> None:
        """No-op — COPY commits immediately under autocommit."""
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
                    "TimescaleDB: connection error on attempt %d/3: %s", attempt + 1, exc
                )
                continue
            except Exception as exc:
                raise QueryError(
                    f"TimescaleDB query failed: {exc}\nSQL: {sql}"
                ) from exc
        raise QueryError(
            f"TimescaleDB query failed after 3 attempts: {last_exc}\nSQL: {sql}"
        ) from last_exc

    async def get_row_count(self, table_name: str) -> int:
        row = await self._pool.fetchrow(  # type: ignore[union-attr]
            f'SELECT count(*) FROM "{table_name}"'
        )
        return int(row[0]) if row else 0

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def get_version(self) -> str:
        try:
            pg_ver = await self._pool.fetchval("SELECT version()")  # type: ignore[union-attr]
            ts_ver = await self._pool.fetchval(  # type: ignore[union-attr]
                "SELECT extversion FROM pg_extension WHERE extname = 'timescaledb'"
            )
            ts_str = f" / TimescaleDB {ts_ver}" if ts_ver else ""
            return f"{pg_ver}{ts_str}"
        except Exception:
            return "unknown"

    # ------------------------------------------------------------------
    # MVCapableAdapter — continuous aggregates
    # ------------------------------------------------------------------

    async def create_materialized_view(
        self,
        view_name: str,
        source_table: str,
        schema: DatasetSchema,
        granularity: str = "1 hour",
    ) -> None:
        """
        Create a TimescaleDB continuous aggregate.

        Creates the view ``WITH NO DATA`` then immediately triggers a full
        historical refresh via ``CALL refresh_continuous_aggregate``.
        The source table must already be a hypertable (guaranteed by
        ``create_table()``).

        Parameters
        ----------
        granularity:
            Time-bucket width understood by TimescaleDB, e.g. ``"1 hour"``,
            ``"30 minutes"``, ``"1 day"``.
        """
        self._mv_granularity = granularity

        ts = schema.timestamp_col
        tag_exprs = ", ".join(f'"{t}"' for t in schema.tag_cols)
        metric_exprs = ", ".join(
            f'avg("{m}") AS "avg_{m}", min("{m}") AS "min_{m}", max("{m}") AS "max_{m}"'
            for m in schema.metric_cols
        )
        group_by_idx = ", ".join(
            str(i) for i in range(1, 2 + len(schema.tag_cols))
        )

        drop_sql = f'DROP MATERIALIZED VIEW IF EXISTS "{view_name}" CASCADE'
        create_sql = (
            f'CREATE MATERIALIZED VIEW "{view_name}" '
            f"WITH (timescaledb.continuous) AS "
            f'SELECT time_bucket(\'{granularity}\', "{ts}") AS ts_bucket, '
            f"{tag_exprs + ', ' if tag_exprs else ''}"
            f"{metric_exprs} "
            f'FROM "{source_table}" '
            f"GROUP BY {group_by_idx} "
            f"WITH NO DATA"
        )
        refresh_sql = (
            f"CALL refresh_continuous_aggregate('{view_name}', NULL, NULL)"
        )

        try:
            await self._pool.execute(drop_sql)  # type: ignore[union-attr]
            await self._pool.execute(create_sql)  # type: ignore[union-attr]
            # Full historical refresh — may be slow for large tables
            await self._pool.execute(refresh_sql)  # type: ignore[union-attr]
            logger.debug("TimescaleDB: created continuous aggregate %r", view_name)
        except Exception as exc:
            raise QueryError(
                f"TimescaleDB create_materialized_view failed: {exc}"
            ) from exc

    async def refresh_materialized_view(
        self,
        view_name: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> TimingResult:
        """
        Incrementally refresh a continuous aggregate for the given window.

        TimescaleDB only re-aggregates the buckets within ``[start, end)``,
        making late-arrival refreshes very fast.
        """
        try:
            with timed_operation() as result:
                await self._pool.execute(  # type: ignore[union-attr]
                    f"CALL refresh_continuous_aggregate($1, $2::timestamptz, $3::timestamptz)",
                    view_name,
                    start,
                    end,
                )
            return result
        except Exception as exc:
            raise QueryError(
                f"TimescaleDB refresh_materialized_view failed: {exc}"
            ) from exc

    async def drop_materialized_view(self, view_name: str) -> None:
        try:
            await self._pool.execute(  # type: ignore[union-attr]
                f'DROP MATERIALIZED VIEW IF EXISTS "{view_name}" CASCADE'
            )
        except Exception as exc:
            logger.warning("TimescaleDB drop_materialized_view %r: %s", view_name, exc)

    async def view_exists(self, view_name: str) -> bool:
        try:
            row = await self._pool.fetchrow(  # type: ignore[union-attr]
                "SELECT view_name FROM timescaledb_information.continuous_aggregates "
                "WHERE view_name = $1",
                view_name,
            )
            return row is not None
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_records(
    table: pa.Table,
    schema: DatasetSchema,
) -> list[tuple]:
    """
    Convert an Arrow table to a list of tuples for ``copy_records_to_table``.

    Timestamp columns are cast to microsecond-precision UTC datetimes via
    PyArrow's vectorized C++ cast.  Uses a single ``to_pydict()`` call
    instead of per-column ``to_pylist()`` for better allocation performance.
    """
    # Pre-cast timestamp columns in-place on the Arrow table (C++ level)
    for col_spec in schema.columns:
        if col_spec.role == ColumnRole.TIMESTAMP:
            idx = table.schema.get_field_index(col_spec.name)
            table = table.set_column(
                idx, col_spec.name,
                table.column(col_spec.name).cast(pa.timestamp("us", tz="UTC")),
            )

    # Single C++ call to convert all columns at once — avoids repeated
    # to_pylist() overhead and produces fewer intermediate Python objects.
    d = table.to_pydict()
    columns_ordered = [col_spec.name for col_spec in schema.columns]
    return list(zip(*(d[name] for name in columns_ordered)))

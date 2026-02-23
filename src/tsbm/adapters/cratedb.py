"""
CrateDB adapter.

Ingestion path  — UNNEST bulk INSERT via asyncpg on the PostgreSQL wire
                  protocol port (default 5432).  ``statement_cache_size=0``
                  is required because CrateDB's PG wire support does not
                  implement prepared-statement caching.

                  INSERT INTO t (c1, c2, …)
                  SELECT * FROM UNNEST($1::TYPE[], $2::TYPE[], …)

Consistency     — CrateDB is *eventually consistent*: newly inserted data is
                  not searchable until the next shard refresh (default ~1 s).
                  ``flush()`` issues ``REFRESH TABLE`` to force visibility
                  before query benchmarks run.

Query path      — asyncpg on the same PG wire port.

Default DB      — CrateDB's default schema/database is ``"doc"``.

Install         — ``pip install tsbm[cratedb]``  (``crate>=1.0``)
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

import asyncpg
import pyarrow as pa

from tsbm.adapters.type_maps import (
    DB_CRATEDB,
    arrow_table_to_ddl,
    cratedb_array_type,
)
from tsbm.config.settings import CrateDBConfig
from tsbm.datasets.schema import ColumnRole, DatasetSchema
from tsbm.exceptions import ConnectionError, IngestionError, QueryError
from tsbm.metrics.timer import TimingResult, estimate_table_bytes, timed_operation

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


class CrateDBAdapter:
    """
    Adapter for CrateDB.

    Parameters
    ----------
    config:
        ``CrateDBConfig`` from ``get_settings().databases.cratedb``.
    """

    name = "cratedb"

    def __init__(self, config: CrateDBConfig) -> None:
        self._config = config
        self._pg_conn: asyncpg.Connection | None = None
        self._schema: DatasetSchema | None = None
        self._current_table: str | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        try:
            self._pg_conn = await asyncpg.connect(
                host=self._config.host,
                port=self._config.pg_port,
                database="doc",   # CrateDB default schema
                user="crate",
                password="",
                statement_cache_size=0,  # CrateDB PG wire: no PS caching
                command_timeout=120,
            )
            logger.debug("CrateDB: connected on port %d", self._config.pg_port)
        except Exception as exc:
            raise ConnectionError(f"CrateDB connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._pg_conn is not None:
            await self._pg_conn.close()
            self._pg_conn = None
            logger.debug("CrateDB: disconnected")

    async def health_check(self) -> bool:
        try:
            await self._pg_conn.fetchval("SELECT 1")  # type: ignore[union-attr]
            return True
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def create_table(self, schema: DatasetSchema) -> None:
        self._schema = schema
        self._current_table = schema.name
        ddl = arrow_table_to_ddl(schema, DB_CRATEDB, schema.name)
        logger.debug("CrateDB DDL:\n%s", ddl)
        try:
            await self._pg_conn.execute(ddl)  # type: ignore[union-attr]
        except Exception as exc:
            raise QueryError(f"CrateDB create_table failed: {exc}") from exc

    async def drop_table(self, table_name: str) -> None:
        try:
            await self._pg_conn.execute(  # type: ignore[union-attr]
                f'DROP TABLE IF EXISTS "{table_name}"'
            )
        except Exception as exc:
            logger.warning("CrateDB drop_table %s: %s", table_name, exc)

    async def table_exists(self, table_name: str) -> bool:
        try:
            row = await self._pg_conn.fetchrow(  # type: ignore[union-attr]
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = $1",
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
        Bulk insert via CrateDB's UNNEST pattern.

        All columns are passed as typed arrays using asyncpg's native
        PostgreSQL parameter binding.  Type casts (``$1::TYPE[]``) are
        required because CrateDB does not infer types from untyped arrays.
        """
        if self._schema is None:
            raise IngestionError("call create_table() before ingest_batch()")

        schema = self._schema
        n_rows = len(table)
        n_bytes = estimate_table_bytes(table)

        # Build column lists and parameter arrays
        cols = [c for c in schema.columns]
        col_names = ", ".join(f'"{c.name}"' for c in cols)
        unnest_parts = ", ".join(
            f"${i + 1}::{cratedb_array_type(c)}" for i, c in enumerate(cols)
        )
        sql = (
            f'INSERT INTO "{table_name}" ({col_names}) '
            f"SELECT * FROM UNNEST({unnest_parts})"
        )

        params = [_col_to_list(table.column(c.name), c) for c in cols]

        try:
            with timed_operation(rows=n_rows, bytes_count=n_bytes) as result:
                await self._pg_conn.execute(sql, *params)  # type: ignore[union-attr]
            return result
        except Exception as exc:
            raise IngestionError(f"CrateDB ingest_batch failed: {exc}") from exc

    async def flush(self) -> None:
        """
        Issue ``REFRESH TABLE`` to make recently inserted rows visible.

        CrateDB is eventually consistent — without REFRESH, query benchmarks
        run immediately after ingestion may return stale (empty) results.
        """
        if self._current_table and self._pg_conn is not None:
            try:
                await self._pg_conn.execute(
                    f'REFRESH TABLE "{self._current_table}"'
                )
            except Exception as exc:
                logger.warning("CrateDB REFRESH TABLE failed: %s", exc)

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    async def execute_query(
        self,
        sql: str,
        params: tuple = (),
    ) -> tuple[list[dict], TimingResult]:
        try:
            with timed_operation() as result:
                rows = await self._pg_conn.fetch(sql, *params)  # type: ignore[union-attr]
            return [dict(r) for r in rows], result
        except Exception as exc:
            raise QueryError(f"CrateDB query failed: {exc}\nSQL: {sql}") from exc

    async def get_row_count(self, table_name: str) -> int:
        row = await self._pg_conn.fetchrow(  # type: ignore[union-attr]
            f'SELECT count(*) FROM "{table_name}"'
        )
        return int(row[0]) if row else 0

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------

    async def get_version(self) -> str:
        try:
            row = await self._pg_conn.fetchrow("SELECT version()")  # type: ignore[union-attr]
            return str(row[0]) if row else "unknown"
        except Exception:
            return "unknown"

    # ------------------------------------------------------------------
    # MVCapableAdapter — simulated materialized views via summary tables
    # ------------------------------------------------------------------

    async def create_materialized_view(
        self,
        view_name: str,
        source_table: str,
        schema: DatasetSchema,
        granularity: str = "1 hour",
    ) -> None:
        """
        Simulate a materialized view using a plain CrateDB summary table.

        Steps:
          1. Drop existing summary table (if any).
          2. Create the summary table with ts_bucket + tag + avg/min/max columns.
          3. Populate it with a GROUP BY aggregation from the source table.
          4. REFRESH TABLE to make the data visible.

        Parameters
        ----------
        granularity:
            Interval string for DATE_BIN, e.g. ``"1 hour"``, ``"30 minutes"``.
        """
        self._mv_granularity = granularity
        self._mv_source_table = source_table

        ts = schema.timestamp_col
        tag_cols = schema.tag_cols
        metric_cols = schema.metric_cols

        # Build DDL for the summary table
        col_defs = ['"ts_bucket" TIMESTAMP WITH TIME ZONE NOT NULL']
        for tag in tag_cols:
            col_defs.append(f'"{tag}" TEXT')
        for m in metric_cols:
            col_defs.append(f'"avg_{m}" DOUBLE PRECISION')
            col_defs.append(f'"min_{m}" DOUBLE PRECISION')
            col_defs.append(f'"max_{m}" DOUBLE PRECISION')

        tag_select = (", ".join(f'"{t}"' for t in tag_cols) + ", ") if tag_cols else ""
        metric_select = ", ".join(
            f'avg("{m}") AS "avg_{m}", min("{m}") AS "min_{m}", max("{m}") AS "max_{m}"'
            for m in metric_cols
        )
        n_group = 1 + len(tag_cols)
        group_by = ", ".join(str(i) for i in range(1, n_group + 1))

        drop_sql = f'DROP TABLE IF EXISTS "{view_name}"'
        create_sql = f'CREATE TABLE IF NOT EXISTS "{view_name}" ({", ".join(col_defs)})'
        insert_sql = (
            f'INSERT INTO "{view_name}" '
            f"SELECT DATE_BIN('{granularity}', \"{ts}\", TIMESTAMP '1970-01-01') AS ts_bucket, "
            f"{tag_select}{metric_select} "
            f'FROM "{source_table}" '
            f"GROUP BY {group_by}"
        )

        try:
            await self._pg_conn.execute(drop_sql)  # type: ignore[union-attr]
            await self._pg_conn.execute(create_sql)  # type: ignore[union-attr]
            await self._pg_conn.execute(insert_sql)  # type: ignore[union-attr]
            await self._pg_conn.execute(f'REFRESH TABLE "{view_name}"')  # type: ignore[union-attr]
            logger.debug("CrateDB: created MV summary table %r", view_name)
        except Exception as exc:
            raise QueryError(
                f"CrateDB create_materialized_view failed: {exc}"
            ) from exc

    async def refresh_materialized_view(
        self,
        view_name: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> TimingResult:
        """
        Refresh the simulated MV for a given time window.

        Deletes the affected rows from the summary table, then re-inserts
        the aggregated data from the source.  Both operations are timed
        together as "refresh latency".
        """
        source_table = self._mv_source_table
        schema = self._schema
        granularity = getattr(self, "_mv_granularity", "1 hour")

        if source_table is None or schema is None:
            raise QueryError(
                "create_table() and create_materialized_view() must be called first"
            )

        ts = schema.timestamp_col
        tag_cols = schema.tag_cols
        metric_cols = schema.metric_cols
        tag_select = (", ".join(f'"{t}"' for t in tag_cols) + ", ") if tag_cols else ""
        metric_select = ", ".join(
            f'avg("{m}") AS "avg_{m}", min("{m}") AS "min_{m}", max("{m}") AS "max_{m}"'
            for m in metric_cols
        )
        n_group = 1 + len(tag_cols)
        group_by = ", ".join(str(i) for i in range(1, n_group + 1))

        try:
            with timed_operation() as result:
                if start is not None and end is not None:
                    await self._pg_conn.execute(  # type: ignore[union-attr]
                        f'DELETE FROM "{view_name}" '
                        f"WHERE ts_bucket >= $1 AND ts_bucket < $2",
                        start,
                        end,
                    )
                    await self._pg_conn.execute(  # type: ignore[union-attr]
                        f'INSERT INTO "{view_name}" '
                        f"SELECT DATE_BIN('{granularity}', \"{ts}\", "
                        f"TIMESTAMP '1970-01-01') AS ts_bucket, "
                        f"{tag_select}{metric_select} "
                        f'FROM "{source_table}" '
                        f'WHERE "{ts}" >= $1 AND "{ts}" < $2 '
                        f"GROUP BY {group_by}",
                        start,
                        end,
                    )
                else:
                    # Full refresh
                    await self._pg_conn.execute(f'DELETE FROM "{view_name}"')  # type: ignore[union-attr]
                    await self._pg_conn.execute(  # type: ignore[union-attr]
                        f'INSERT INTO "{view_name}" '
                        f"SELECT DATE_BIN('{granularity}', \"{ts}\", "
                        f"TIMESTAMP '1970-01-01') AS ts_bucket, "
                        f"{tag_select}{metric_select} "
                        f'FROM "{source_table}" '
                        f"GROUP BY {group_by}"
                    )
                await self._pg_conn.execute(f'REFRESH TABLE "{view_name}"')  # type: ignore[union-attr]
            return result
        except Exception as exc:
            raise QueryError(
                f"CrateDB refresh_materialized_view failed: {exc}"
            ) from exc

    async def drop_materialized_view(self, view_name: str) -> None:
        try:
            await self._pg_conn.execute(f'DROP TABLE IF EXISTS "{view_name}"')  # type: ignore[union-attr]
        except Exception as exc:
            logger.warning("CrateDB drop_materialized_view %r: %s", view_name, exc)

    async def view_exists(self, view_name: str) -> bool:
        try:
            row = await self._pg_conn.fetchrow(  # type: ignore[union-attr]
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_name = $1",
                view_name,
            )
            return row is not None
        except Exception:
            return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _col_to_list(col: pa.ChunkedArray | pa.Array, spec) -> list:  # type: ignore[type-arg]
    """
    Convert an Arrow column to a Python list suitable for asyncpg array params.

    Timestamp columns are converted to UTC-aware ``datetime`` objects
    (microsecond precision — TIMESTAMPTZ max precision in CrateDB/PostgreSQL).
    """
    if spec.role == ColumnRole.TIMESTAMP:
        ns_vals = col.cast(pa.int64()).to_pylist()
        return [_ns_to_datetime(ns) for ns in ns_vals]
    return col.to_pylist()


def _ns_to_datetime(ns: int | None) -> datetime | None:
    """Convert nanosecond Unix epoch to a UTC-aware datetime (μs precision)."""
    if ns is None:
        return None
    # timedelta arithmetic preserves microsecond precision without fp error
    return _EPOCH + timedelta(microseconds=ns // 1_000)

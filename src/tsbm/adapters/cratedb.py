"""
CrateDB adapter.

Ingestion path  — HTTP bulk API via ``/_sql`` on the HTTP port (default 4200).
                  Uses CrateDB's bulk_args parameter for high-throughput writes:

                  POST /_sql
                  {"stmt": "INSERT INTO t (c1, c2) VALUES (?, ?)",
                   "bulk_args": [[v1, v2], [v3, v4], ...]}

                  This bypasses asyncpg/PG-wire UNNEST issues entirely and is
                  CrateDB's recommended bulk ingestion path.

Consistency     — CrateDB is *eventually consistent*: newly inserted data is
                  not searchable until the next shard refresh (default ~1 s).
                  ``flush()`` issues ``REFRESH TABLE`` to force visibility
                  before query benchmarks run.

Query path      — asyncpg on the PG wire port (default 5432).

Default DB      — CrateDB's default schema/database is ``"doc"``.

Install         — ``pip install tsbm[cratedb]``  (``crate>=1.0``)
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any

import aiohttp
import asyncpg
import pyarrow as pa

from tsbm.adapters.base import RETRYABLE_CONNECTION_EXCEPTIONS
from tsbm.adapters.type_maps import (
    DB_CRATEDB,
    arrow_table_to_ddl,
)
from tsbm.config.settings import CrateDBConfig
from tsbm.datasets.schema import ColumnRole, DatasetSchema
from tsbm.exceptions import ConnectionError, IngestionError, QueryError
from tsbm.metrics.timer import TimingResult, estimate_table_bytes, timed_operation

logger = logging.getLogger(__name__)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_HTTP_BULK_CHUNK = 100_000  # max rows per HTTP POST to /_sql


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
        self._pool: asyncpg.Pool | None = None
        self._schema: DatasetSchema | None = None
        self._current_table: str | None = None
        self._http_session: aiohttp.ClientSession | None = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        try:
            self._pool = await asyncpg.create_pool(
                host=self._config.host,
                port=self._config.pg_port,
                database="doc",   # CrateDB default schema
                user="crate",
                password="",
                statement_cache_size=0,  # CrateDB PG wire: no PS caching
                command_timeout=120,
                min_size=1,
                max_size=32,
            )
            self._http_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=300),
            )
            logger.debug(
                "CrateDB: connected on PG port %d, HTTP port %d",
                self._config.pg_port,
                self._config.http_port,
            )
        except Exception as exc:
            raise ConnectionError(f"CrateDB connection failed: {exc}") from exc

    async def disconnect(self) -> None:
        if self._http_session is not None:
            await self._http_session.close()
            self._http_session = None
        if self._pool is not None:
            await self._pool.close()
            self._pool = None
            logger.debug("CrateDB: disconnected")

    async def _ensure_connected(self) -> None:
        """Reconnect the pool if it was never created or was closed."""
        if self._pool is None or self._http_session is None:
            logger.warning("CrateDB: pool not initialised — reconnecting")
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
        self._schema = schema
        self._current_table = schema.name
        ddl = arrow_table_to_ddl(schema, DB_CRATEDB, schema.name)
        logger.debug("CrateDB DDL:\n%s", ddl)
        try:
            await self._pool.execute(ddl)  # type: ignore[union-attr]
        except Exception as exc:
            raise QueryError(f"CrateDB create_table failed: {exc}") from exc

    async def drop_table(self, table_name: str) -> None:
        try:
            await self._pool.execute(  # type: ignore[union-attr]
                f'DROP TABLE IF EXISTS "{table_name}"'
            )
        except Exception as exc:
            logger.warning("CrateDB drop_table %s: %s", table_name, exc)

    async def table_exists(self, table_name: str) -> bool:
        try:
            row = await self._pool.fetchrow(  # type: ignore[union-attr]
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'doc' AND table_name = $1",
                table_name,
            )
            return row is not None
        except Exception:
            return False

    # ------------------------------------------------------------------
    # Ingestion — HTTP bulk API
    # ------------------------------------------------------------------

    async def ingest_batch(
        self,
        table: pa.Table,
        table_name: str,
    ) -> TimingResult:
        """
        Bulk insert via CrateDB's HTTP ``/_sql`` bulk API.

        Rows are sent in sub-batches of ``_HTTP_BULK_CHUNK`` to avoid
        serialising millions of rows into a single JSON payload (which
        causes memory spikes on billion-row workloads).
        """
        if self._schema is None:
            raise IngestionError("call create_table() before ingest_batch()")

        schema = self._schema
        n_rows = len(table)
        n_bytes = estimate_table_bytes(table)

        # Build the INSERT statement with positional placeholders
        cols = list(schema.columns)
        col_names = ", ".join(f'"{c.name}"' for c in cols)
        placeholders = ", ".join("?" for _ in cols)
        stmt = f'INSERT INTO "{table_name}" ({col_names}) VALUES ({placeholders})'

        # Convert columns to Python lists (vectorized timestamp conversion)
        col_lists = [_col_to_list(table.column(c.name), c) for c in cols]
        bulk_args = list(zip(*col_lists))

        await self._ensure_connected()
        url = f"http://{self._config.host}:{self._config.http_port}/_sql"

        max_retries = 3
        last_exc: Exception | None = None
        for attempt in range(max_retries):
            try:
                with timed_operation(rows=n_rows, bytes_count=n_bytes) as result:
                    for chunk_start in range(0, len(bulk_args), _HTTP_BULK_CHUNK):
                        chunk_args = bulk_args[chunk_start:chunk_start + _HTTP_BULK_CHUNK]
                        payload: dict[str, Any] = {"stmt": stmt, "bulk_args": chunk_args}
                        async with self._http_session.post(  # type: ignore[union-attr]
                            url,
                            json=payload,
                        ) as resp:
                            if resp.status != 200:
                                body = await resp.text()
                                raise IngestionError(
                                    f"CrateDB HTTP bulk insert failed (HTTP {resp.status}): {body}"
                                )
                            resp_json = await resp.json()
                            # Check for per-row errors in the results array
                            results = resp_json.get("results", [])
                            errors = [r for r in results if r.get("rowcount", 0) < 0]
                            if errors:
                                raise IngestionError(
                                    f"CrateDB bulk insert had {len(errors)} row errors "
                                    f"out of {len(results)} batches"
                                )
                return result
            except IngestionError:
                raise
            except Exception as exc:
                last_exc = exc
                if attempt < max_retries - 1:
                    logger.warning(
                        "CrateDB ingest_batch attempt %d/%d failed: %s — retrying",
                        attempt + 1, max_retries, exc,
                    )
                    await asyncio.sleep(1.0 * (attempt + 1))
        raise IngestionError(
            f"CrateDB ingest_batch failed after {max_retries} attempts: {last_exc}"
        ) from last_exc

    async def flush(self, expected_rows: int | None = None) -> None:
        """
        Issue ``REFRESH TABLE`` to make recently inserted rows visible.

        CrateDB is eventually consistent — without REFRESH, query benchmarks
        run immediately after ingestion may return stale (empty) results.

        When *expected_rows* is provided, polls ``get_row_count()`` after
        the REFRESH until the count reaches the expected value — this
        eliminates intermittent 0-row query results caused by shard
        refresh lag.
        """
        if self._current_table and self._pool is not None:
            try:
                await self._pool.execute(
                    f'REFRESH TABLE "{self._current_table}"'
                )
            except Exception as exc:
                logger.warning("CrateDB REFRESH TABLE failed: %s", exc)

            if expected_rows is not None:
                await self.wait_for_visibility(
                    self._current_table, expected_rows
                )

    async def wait_for_visibility(
        self,
        table_name: str,
        expected_rows: int,
        timeout_seconds: float = 60.0,
        poll_interval: float = 0.5,
    ) -> bool:
        """Poll until row count reaches *expected_rows* after REFRESH TABLE."""
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            try:
                await self._pool.execute(f'REFRESH TABLE "{table_name}"')  # type: ignore[union-attr]
                count = await self.get_row_count(table_name)
                if count >= expected_rows:
                    return True
            except Exception as exc:
                logger.debug("CrateDB visibility poll error: %s", exc)
            await asyncio.sleep(poll_interval)
        logger.warning(
            "CrateDB: data visibility timeout — expected %d rows in %r "
            "but did not reach that count within %.0fs",
            expected_rows, table_name, timeout_seconds,
        )
        return False

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
                    "CrateDB: connection error on attempt %d/3: %s", attempt + 1, exc
                )
                continue
            except Exception as exc:
                raise QueryError(f"CrateDB query failed: {exc}\nSQL: {sql}") from exc
        raise QueryError(
            f"CrateDB query failed after 3 attempts: {last_exc}\nSQL: {sql}"
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
            row = await self._pool.fetchrow("SELECT version()")  # type: ignore[union-attr]
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
            await self._pool.execute(drop_sql)  # type: ignore[union-attr]
            await self._pool.execute(create_sql)  # type: ignore[union-attr]
            await self._pool.execute(insert_sql)  # type: ignore[union-attr]
            await self._pool.execute(f'REFRESH TABLE "{view_name}"')  # type: ignore[union-attr]
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
                    await self._pool.execute(  # type: ignore[union-attr]
                        f'DELETE FROM "{view_name}" '
                        f"WHERE ts_bucket >= $1 AND ts_bucket < $2",
                        start,
                        end,
                    )
                    await self._pool.execute(  # type: ignore[union-attr]
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
                    await self._pool.execute(f'DELETE FROM "{view_name}"')  # type: ignore[union-attr]
                    await self._pool.execute(  # type: ignore[union-attr]
                        f'INSERT INTO "{view_name}" '
                        f"SELECT DATE_BIN('{granularity}', \"{ts}\", "
                        f"TIMESTAMP '1970-01-01') AS ts_bucket, "
                        f"{tag_select}{metric_select} "
                        f'FROM "{source_table}" '
                        f"GROUP BY {group_by}"
                    )
                await self._pool.execute(f'REFRESH TABLE "{view_name}"')  # type: ignore[union-attr]
            return result
        except Exception as exc:
            raise QueryError(
                f"CrateDB refresh_materialized_view failed: {exc}"
            ) from exc

    async def drop_materialized_view(self, view_name: str) -> None:
        try:
            await self._pool.execute(f'DROP TABLE IF EXISTS "{view_name}"')  # type: ignore[union-attr]
        except Exception as exc:
            logger.warning("CrateDB drop_materialized_view %r: %s", view_name, exc)

    async def view_exists(self, view_name: str) -> bool:
        try:
            row = await self._pool.fetchrow(  # type: ignore[union-attr]
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = 'doc' AND table_name = $1",
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
    Convert an Arrow column to a Python list suitable for HTTP bulk API args.

    Timestamp columns are converted to millisecond epoch integers using
    vectorized NumPy integer division (no per-element Python loop).
    """
    if spec.role == ColumnRole.TIMESTAMP:
        # CrateDB HTTP API expects timestamps as millisecond epoch integers.
        # Vectorized: Arrow int64 (nanoseconds) → NumPy → floor-divide → list
        import numpy as np
        ns_array = col.cast(pa.int64()).to_numpy()
        ms_array = (ns_array // 1_000_000).tolist()
        return ms_array
    return col.to_pylist()

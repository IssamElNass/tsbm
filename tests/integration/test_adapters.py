"""
Integration tests for all three database adapters.

These tests require running Docker containers (see docker/docker-compose.test.yml).
They are gated by the ``integration`` marker so they are skipped during
normal ``pytest tests/unit/`` runs.

Each adapter is tested through the full lifecycle:
  connect → health_check → create_table → ingest_batch → flush
  → get_row_count → execute_query → drop_table → disconnect
"""
from __future__ import annotations

import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# QuestDB
# ---------------------------------------------------------------------------


class TestQuestDBAdapter:
    async def test_health_check(self, questdb) -> None:
        assert await questdb.health_check() is True

    async def test_get_version(self, questdb) -> None:
        version = await questdb.get_version()
        assert isinstance(version, str)
        assert len(version) > 0

    async def test_create_and_drop_table(self, questdb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await questdb.drop_table(table_name)
        await questdb.create_table(small_dataset.schema)
        assert await questdb.table_exists(table_name)
        await questdb.drop_table(table_name)
        assert not await questdb.table_exists(table_name)

    async def test_ingest_batch(self, questdb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await questdb.drop_table(table_name)
        await questdb.create_table(small_dataset.schema)
        tbl = small_dataset.load()
        timing = await questdb.ingest_batch(tbl, table_name)
        assert timing.rows_processed == len(tbl)
        assert timing.elapsed_ns > 0
        await questdb.drop_table(table_name)

    async def test_flush_noop(self, questdb, small_dataset) -> None:
        # flush() is a no-op for QuestDB; should not raise
        await questdb.drop_table(small_dataset.schema.name)
        await questdb.create_table(small_dataset.schema)
        await questdb.ingest_batch(small_dataset.load(), small_dataset.schema.name)
        await questdb.flush()
        await questdb.drop_table(small_dataset.schema.name)

    async def test_row_count_after_ingest(self, questdb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await questdb.drop_table(table_name)
        await questdb.create_table(small_dataset.schema)
        await questdb.ingest_batch(small_dataset.load(), table_name)
        await questdb.flush()
        count = await questdb.get_row_count(table_name)
        assert count == len(small_dataset.load())
        await questdb.drop_table(table_name)

    async def test_execute_query_returns_rows(self, questdb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await questdb.drop_table(table_name)
        await questdb.create_table(small_dataset.schema)
        await questdb.ingest_batch(small_dataset.load(), table_name)
        await questdb.flush()
        rows, timing = await questdb.execute_query(f"SELECT count() FROM {table_name}")
        assert len(rows) > 0
        assert timing.elapsed_ns > 0
        await questdb.drop_table(table_name)


# ---------------------------------------------------------------------------
# CrateDB
# ---------------------------------------------------------------------------


class TestCrateDBAdapter:
    async def test_health_check(self, cratedb) -> None:
        assert await cratedb.health_check() is True

    async def test_get_version(self, cratedb) -> None:
        version = await cratedb.get_version()
        assert isinstance(version, str)
        assert len(version) > 0

    async def test_create_and_drop_table(self, cratedb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await cratedb.drop_table(table_name)
        await cratedb.create_table(small_dataset.schema)
        assert await cratedb.table_exists(table_name)
        await cratedb.drop_table(table_name)
        assert not await cratedb.table_exists(table_name)

    async def test_ingest_and_flush(self, cratedb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await cratedb.drop_table(table_name)
        await cratedb.create_table(small_dataset.schema)
        tbl = small_dataset.load()
        timing = await cratedb.ingest_batch(tbl, table_name)
        await cratedb.flush()  # REFRESH TABLE
        assert timing.rows_processed == len(tbl)
        count = await cratedb.get_row_count(table_name)
        assert count == len(tbl)
        await cratedb.drop_table(table_name)

    async def test_execute_query(self, cratedb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await cratedb.drop_table(table_name)
        await cratedb.create_table(small_dataset.schema)
        await cratedb.ingest_batch(small_dataset.load(), table_name)
        await cratedb.flush()
        rows, timing = await cratedb.execute_query(f"SELECT COUNT(*) FROM {table_name}")
        assert len(rows) > 0
        await cratedb.drop_table(table_name)


# ---------------------------------------------------------------------------
# TimescaleDB
# ---------------------------------------------------------------------------


class TestTimescaleDBAdapter:
    async def test_health_check(self, timescaledb) -> None:
        assert await timescaledb.health_check() is True

    async def test_get_version(self, timescaledb) -> None:
        version = await timescaledb.get_version()
        assert isinstance(version, str)
        assert len(version) > 0

    async def test_create_and_drop_table(self, timescaledb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await timescaledb.drop_table(table_name)
        await timescaledb.create_table(small_dataset.schema)
        assert await timescaledb.table_exists(table_name)
        await timescaledb.drop_table(table_name)
        assert not await timescaledb.table_exists(table_name)

    async def test_ingest_batch_via_copy(self, timescaledb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await timescaledb.drop_table(table_name)
        await timescaledb.create_table(small_dataset.schema)
        tbl = small_dataset.load()
        timing = await timescaledb.ingest_batch(tbl, table_name)
        assert timing.rows_processed == len(tbl)
        count = await timescaledb.get_row_count(table_name)
        assert count == len(tbl)
        await timescaledb.drop_table(table_name)

    async def test_execute_query(self, timescaledb, small_dataset) -> None:
        table_name = small_dataset.schema.name
        await timescaledb.drop_table(table_name)
        await timescaledb.create_table(small_dataset.schema)
        await timescaledb.ingest_batch(small_dataset.load(), table_name)
        rows, timing = await timescaledb.execute_query(
            f"SELECT COUNT(*) FROM {table_name}"
        )
        assert len(rows) > 0
        await timescaledb.drop_table(table_name)

    async def test_hypertable_created(self, timescaledb, small_dataset) -> None:
        """Verify the hypertable is registered in TimescaleDB's catalog."""
        table_name = small_dataset.schema.name
        await timescaledb.drop_table(table_name)
        await timescaledb.create_table(small_dataset.schema)
        rows, _ = await timescaledb.execute_query(
            "SELECT hypertable_name FROM timescaledb_information.hypertables "
            "WHERE hypertable_name = $1",
            (table_name,),
        )
        assert len(rows) == 1
        await timescaledb.drop_table(table_name)

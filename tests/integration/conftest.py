"""
Fixtures for integration tests.

All fixtures attempt a health-check before yielding the adapter.
If the database is not reachable the test is automatically skipped —
this ensures ``pytest tests/unit/`` works without Docker while
``pytest tests/integration/ -m integration`` drives the full stack.

Usage:
    docker compose -f docker/docker-compose.test.yml up -d
    pytest tests/integration/ -m integration -v
"""
from __future__ import annotations

import asyncio

import pytest

from tsbm.adapters.registry import get_adapter
from tsbm.datasets.generator import generate_iot_dataset
from tsbm.datasets.schema import BenchmarkDataset


# ---------------------------------------------------------------------------
# Adapter fixtures
# ---------------------------------------------------------------------------


async def _connect_or_skip(name: str):
    """Return a connected adapter or skip the test if unreachable."""
    try:
        adapter = get_adapter(name)
        await adapter.connect()
    except Exception as exc:
        pytest.skip(f"{name} not reachable: {exc}")
    healthy = await adapter.health_check()
    if not healthy:
        await adapter.disconnect()
        pytest.skip(f"{name} health_check returned False")
    return adapter


@pytest.fixture
async def questdb():
    adapter = await _connect_or_skip("questdb")
    yield adapter
    await adapter.disconnect()


@pytest.fixture
async def cratedb():
    adapter = await _connect_or_skip("cratedb")
    yield adapter
    await adapter.disconnect()


@pytest.fixture
async def timescaledb():
    adapter = await _connect_or_skip("timescaledb")
    yield adapter
    await adapter.disconnect()


# ---------------------------------------------------------------------------
# Small dataset fixture (session-scoped for speed)
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def small_dataset() -> BenchmarkDataset:
    """A tiny in-memory dataset for integration tests (50 rows)."""
    return generate_iot_dataset(
        n_devices=5,
        n_readings_per_device=10,
        seed=42,
        name="integ_test",
    )

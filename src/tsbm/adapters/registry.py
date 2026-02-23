"""
Adapter registry and factory.

Uses lazy imports so that missing optional dependencies (e.g. ``questdb``
not installed) only affect the specific adapter that needs them, not the
whole suite.  Each adapter is only imported when its database is enabled
in the config.
"""
from __future__ import annotations

import logging

from tsbm.adapters.base import DatabaseAdapter
from tsbm.config.settings import get_settings

logger = logging.getLogger(__name__)

# All known adapter names — used for validation
KNOWN_ADAPTERS: frozenset[str] = frozenset({"questdb", "cratedb", "timescaledb"})


def get_adapter(name: str) -> DatabaseAdapter:
    """
    Instantiate and return a single adapter by name.

    Parameters
    ----------
    name:
        One of ``"questdb"``, ``"cratedb"``, ``"timescaledb"``.

    Raises
    ------
    ValueError
        If *name* is not a recognised adapter.
    ImportError
        If the required optional dependency is not installed.
    """
    if name not in KNOWN_ADAPTERS:
        raise ValueError(
            f"Unknown adapter {name!r}. Known: {sorted(KNOWN_ADAPTERS)}"
        )

    settings = get_settings()

    if name == "questdb":
        from tsbm.adapters.questdb import QuestDBAdapter  # noqa: PLC0415

        return QuestDBAdapter(settings.databases.questdb)  # type: ignore[return-value]

    if name == "cratedb":
        from tsbm.adapters.cratedb import CrateDBAdapter  # noqa: PLC0415

        return CrateDBAdapter(settings.databases.cratedb)  # type: ignore[return-value]

    # timescaledb
    from tsbm.adapters.timescaledb import TimescaleDBAdapter  # noqa: PLC0415

    return TimescaleDBAdapter(settings.databases.timescaledb)  # type: ignore[return-value]


def get_enabled_adapters(
    requested: list[str] | None = None,
) -> list[DatabaseAdapter]:
    """
    Return adapters for all enabled databases.

    Parameters
    ----------
    requested:
        Optional explicit list of adapter names to load.  If ``None``,
        all databases that have ``enabled = true`` in the config are used.

    Returns
    -------
    list[DatabaseAdapter]
        Ready-to-use adapter instances (not yet connected).

    Notes
    -----
    Missing optional packages are logged as warnings and skipped rather
    than raising, so the suite can still run against the available databases.
    """
    settings = get_settings()
    adapters: list[DatabaseAdapter] = []

    candidates: list[str] = requested or _enabled_names(settings)

    for name in candidates:
        if name not in KNOWN_ADAPTERS:
            logger.warning("Unknown adapter %r — skipping", name)
            continue
        try:
            adapter = get_adapter(name)
            adapters.append(adapter)
            logger.debug("Loaded adapter: %s", name)
        except ImportError as exc:
            logger.warning(
                "Skipping %r — required package not installed: %s. "
                "Run: pip install tsbm[%s]",
                name, exc, name,
            )
        except Exception as exc:
            logger.warning("Failed to load adapter %r: %s", name, exc)

    return adapters


def _enabled_names(settings) -> list[str]:  # type: ignore[no-untyped-def]
    """Return the names of databases marked enabled in the config."""
    result = []
    if settings.databases.questdb.enabled:
        result.append("questdb")
    if settings.databases.cratedb.enabled:
        result.append("cratedb")
    if settings.databases.timescaledb.enabled:
        result.append("timescaledb")
    return result

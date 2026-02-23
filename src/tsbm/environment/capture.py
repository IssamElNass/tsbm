"""
Environment snapshot for benchmark reproducibility.

Captures the host configuration at run time so results can be compared
across machines and over time.  All fields are plain Python types so the
dict can be stored as JSON in the SQLite ``runs.config_snapshot`` column.
"""
from __future__ import annotations

import importlib.metadata
import platform
import subprocess
import sys
from typing import Any

import psutil


# ---------------------------------------------------------------------------
# Packages whose versions are always recorded
# ---------------------------------------------------------------------------

_TRACKED_PACKAGES = [
    "pyarrow",
    "pandas",
    "numpy",
    "pydantic",
    "pydantic-settings",
    "asyncpg",
    "hdrh",
    "psutil",
    "typer",
    "rich",
    "aiohttp",
    "orjson",
    "questdb",
    "crate",
]


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def capture_environment() -> dict[str, Any]:
    """
    Return a flat dict describing the current execution environment.

    Keys
    ----
    python_version      : e.g. "3.10.5"
    platform            : e.g. "Windows-10-10.0.19045-SP0"
    platform_system     : e.g. "Windows"
    cpu_count_logical   : number of logical CPU cores
    cpu_count_physical  : number of physical CPU cores (may be None)
    cpu_freq_mhz        : current CPU frequency in MHz (None if unavailable)
    total_ram_gb        : total system RAM in GiB
    git_commit          : short SHA of HEAD, or "unknown"
    packages            : dict of {package_name: version_string}
    db_versions         : dict of {adapter_name: version_string} — populated
                          externally after adapter.connect() is called
    """
    return {
        "python_version":    _python_version(),
        "platform":          platform.platform(),
        "platform_system":   platform.system(),
        "cpu_count_logical":  _cpu_count_logical(),
        "cpu_count_physical": _cpu_count_physical(),
        "cpu_freq_mhz":       _cpu_freq_mhz(),
        "total_ram_gb":       _total_ram_gb(),
        "git_commit":         _git_commit(),
        "packages":           _package_versions(),
        "db_versions":        {},
    }


def enrich_db_versions(
    env: dict[str, Any],
    adapter_name: str,
    version: str,
) -> None:
    """
    Add a database version entry to an existing environment snapshot.

    Mutates *env* in place.  Called after each adapter connects successfully.
    """
    env.setdefault("db_versions", {})[adapter_name] = version


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _python_version() -> str:
    return sys.version.split()[0]


def _cpu_count_logical() -> int:
    return psutil.cpu_count(logical=True) or 1


def _cpu_count_physical() -> int | None:
    return psutil.cpu_count(logical=False)


def _cpu_freq_mhz() -> float | None:
    try:
        freq = psutil.cpu_freq()
        return round(freq.current, 1) if freq else None
    except Exception:
        return None


def _total_ram_gb() -> float:
    return round(psutil.virtual_memory().total / (1024 ** 3), 2)


def _git_commit() -> str:
    try:
        result = subprocess.run(
            ["git", "rev-parse", "--short", "HEAD"],
            capture_output=True,
            text=True,
            timeout=5,
        )
        if result.returncode == 0:
            return result.stdout.strip()
    except Exception:
        pass
    return "unknown"


def _package_versions() -> dict[str, str]:
    versions: dict[str, str] = {}
    for pkg in _TRACKED_PACKAGES:
        try:
            versions[pkg] = importlib.metadata.version(pkg)
        except importlib.metadata.PackageNotFoundError:
            versions[pkg] = "not_installed"
    return versions

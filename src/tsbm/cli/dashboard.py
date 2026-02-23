"""
Launch the Streamlit dashboard as a subprocess.

The dashboard app is located at ``tsbm/dashboard/app.py``.
The results DB path is passed as a positional argument after ``--``:

    python -m streamlit run .../app.py --server.port 8501 -- results/runs.db
"""
from __future__ import annotations

import subprocess
import sys
from importlib.util import find_spec
from pathlib import Path

from rich.console import Console

console = Console()


def launch_dashboard(
    port: int = 8501,
    results_db: Path | None = None,
    config_path: Path | None = None,
) -> None:
    """
    Launch the Streamlit dashboard in a subprocess.

    Parameters
    ----------
    port:
        Port for the Streamlit server (default 8501).
    results_db:
        Explicit path to the SQLite results database.  Falls back to the
        value in ``benchmark.toml`` / defaults.
    config_path:
        Path to ``benchmark.toml`` (used to resolve *results_db* default).
    """
    # Resolve the results DB path
    if results_db is None:
        try:
            from tsbm.config.settings import (
                get_settings,
                load_settings_from_file,
            )

            settings = (
                load_settings_from_file(config_path)
                if config_path
                else get_settings()
            )
            results_db = settings.results.sqlite_path
        except Exception:
            results_db = Path("results/runs.db")

    # Locate the dashboard app module
    app_path = _find_app_path()
    if app_path is None:
        console.print(
            "[red]Cannot locate tsbm/dashboard/app.py.[/red]\n"
            "Ensure the package is installed with: [bold]pip install -e .[dashboard][/bold]"
        )
        raise SystemExit(1)

    # Check streamlit is available
    if find_spec("streamlit") is None:
        console.print(
            "[red]streamlit is not installed.[/red]\n"
            "Install it with: [bold]pip install tsbm[dashboard][/bold]"
        )
        raise SystemExit(1)

    cmd = [
        sys.executable,
        "-m",
        "streamlit",
        "run",
        str(app_path),
        "--server.port",
        str(port),
        "--server.headless",
        "true",
        "--",              # separator: everything after goes to app.py as sys.argv
        str(results_db),
    ]

    console.print(
        f"[cyan]Launching dashboard on[/cyan] [bold]http://localhost:{port}[/bold]"
    )
    console.print(f"[dim]Results DB: {results_db}[/dim]")
    console.print("[dim]Press Ctrl+C to stop.[/dim]\n")

    try:
        subprocess.run(cmd, check=False)
    except KeyboardInterrupt:
        console.print("\n[yellow]Dashboard stopped.[/yellow]")


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _find_app_path() -> Path | None:
    """Locate dashboard/app.py relative to this package."""
    # When installed as a package: navigate from this file up to the
    # tsbm package root, then into dashboard/
    this_file = Path(__file__).resolve()
    # cli/dashboard.py → cli/ → tsbm/ → dashboard/app.py
    candidate = this_file.parent.parent / "dashboard" / "app.py"
    if candidate.exists():
        return candidate
    return None

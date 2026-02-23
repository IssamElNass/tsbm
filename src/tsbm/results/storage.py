"""
Dual-backend result storage: SQLite for run metadata/summaries, Parquet for
per-operation timing rows.

Layout
------
SQLite (results/runs.db):
  runs       — one row per RunConfig
  summaries  — one row per BenchmarkSummary (flattened latency/resource)

Parquet (results/parquet/{run_id}/{database}/{operation}.parquet):
  One file per (run_id, database, operation) with snappy compression and
  row_group_size=50_000.  Retains every OperationResult for offline analysis.
"""
from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from tsbm.results.models import BenchmarkSummary, OperationResult, RunConfig


# ---------------------------------------------------------------------------
# DDL
# ---------------------------------------------------------------------------

_RUNS_DDL = """
CREATE TABLE IF NOT EXISTS runs (
    run_id          TEXT PRIMARY KEY,
    benchmark_name  TEXT NOT NULL,
    database_name   TEXT NOT NULL,
    dataset_name    TEXT NOT NULL,
    config_hash     TEXT NOT NULL DEFAULT '',
    started_at      TEXT NOT NULL,
    completed_at    TEXT,
    config_snapshot TEXT NOT NULL DEFAULT '{}'
)
"""

_SUMMARIES_DDL = """
CREATE TABLE IF NOT EXISTS summaries (
    id                    INTEGER PRIMARY KEY AUTOINCREMENT,
    run_id                TEXT NOT NULL,
    database_name         TEXT NOT NULL,
    operation             TEXT NOT NULL,
    batch_size            INTEGER NOT NULL DEFAULT 0,
    workers               INTEGER NOT NULL DEFAULT 1,
    total_rows            INTEGER NOT NULL DEFAULT 0,
    sample_count          INTEGER NOT NULL DEFAULT 0,
    rows_per_second_mean  REAL    NOT NULL DEFAULT 0,
    rows_per_second_p95   REAL    NOT NULL DEFAULT 0,
    rows_per_second_p99   REAL    NOT NULL DEFAULT 0,
    mb_per_second_mean    REAL    NOT NULL DEFAULT 0,
    latency_p50_ms        REAL    NOT NULL DEFAULT 0,
    latency_p95_ms        REAL    NOT NULL DEFAULT 0,
    latency_p99_ms        REAL    NOT NULL DEFAULT 0,
    latency_p999_ms       REAL    NOT NULL DEFAULT 0,
    latency_mean_ms       REAL    NOT NULL DEFAULT 0,
    latency_min_ms        REAL    NOT NULL DEFAULT 0,
    latency_max_ms        REAL    NOT NULL DEFAULT 0,
    latency_iqr_ms        REAL    NOT NULL DEFAULT 0,
    latency_stddev_ms     REAL    NOT NULL DEFAULT 0,
    latency_outlier_count INTEGER NOT NULL DEFAULT 0,
    resource_cpu_percent_mean  REAL,
    resource_cpu_percent_max   REAL,
    resource_rss_mb_mean       REAL,
    resource_rss_mb_max        REAL,
    resource_disk_write_mb_total REAL,
    resource_net_sent_mb_total   REAL,
    resource_net_recv_mb_total   REAL,
    FOREIGN KEY (run_id) REFERENCES runs (run_id)
)
"""

_SUMMARIES_INDEX_DDL = """
CREATE INDEX IF NOT EXISTS idx_summaries_run_id
    ON summaries (run_id)
"""

# PyArrow schema for the per-operation Parquet files
_OPERATION_SCHEMA = pa.schema([
    pa.field("run_id",          pa.string()),
    pa.field("database_name",   pa.string()),
    pa.field("operation",       pa.string()),
    pa.field("phase",           pa.string()),
    pa.field("iteration",       pa.int32()),
    pa.field("batch_size",      pa.int32()),
    pa.field("workers",         pa.int32()),
    pa.field("elapsed_ns",      pa.int64()),
    pa.field("rows_processed",  pa.int64()),
    pa.field("bytes_processed", pa.int64()),
    pa.field("rows_per_second", pa.float64()),
    pa.field("mb_per_second",   pa.float64()),
])


# ---------------------------------------------------------------------------
# ResultStorage
# ---------------------------------------------------------------------------


class ResultStorage:
    """
    Manages all benchmark result persistence.

    Call :meth:`initialize` once before any other method to create the
    SQLite schema and directory structure.
    """

    def __init__(self, sqlite_path: Path, parquet_dir: Path) -> None:
        self.sqlite_path = sqlite_path
        self.parquet_dir = parquet_dir

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def initialize(self) -> None:
        """Create directories and run DDL (idempotent)."""
        self.sqlite_path.parent.mkdir(parents=True, exist_ok=True)
        self.parquet_dir.mkdir(parents=True, exist_ok=True)
        with self._connect() as conn:
            conn.execute(_RUNS_DDL)
            conn.execute(_SUMMARIES_DDL)
            conn.execute(_SUMMARIES_INDEX_DDL)

    # ------------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------------

    def save_run(self, run: RunConfig) -> None:
        """Insert a new run row (called at run start)."""
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO runs
                    (run_id, benchmark_name, database_name, dataset_name,
                     config_hash, started_at, completed_at, config_snapshot)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    run.run_id,
                    run.benchmark_name,
                    run.database_name,
                    run.dataset_name,
                    run.config_hash,
                    run.started_at.isoformat(),
                    run.completed_at.isoformat() if run.completed_at else None,
                    json.dumps(run.config_snapshot),
                ),
            )

    def complete_run(self, run_id: str) -> None:
        """Stamp ``completed_at`` on an existing run row."""
        now = datetime.now(timezone.utc).isoformat()
        with self._connect() as conn:
            conn.execute(
                "UPDATE runs SET completed_at = ? WHERE run_id = ?",
                (now, run_id),
            )

    def list_runs(self) -> list[dict[str, Any]]:
        """Return all run rows as plain dicts, ordered by start time."""
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT * FROM runs ORDER BY started_at DESC"
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Summaries
    # ------------------------------------------------------------------

    def save_summary(self, summary: BenchmarkSummary) -> None:
        """Insert a flattened BenchmarkSummary row."""
        d = summary.flat_dict()
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO summaries (
                    run_id, database_name, operation,
                    batch_size, workers, total_rows, sample_count,
                    rows_per_second_mean, rows_per_second_p95, rows_per_second_p99,
                    mb_per_second_mean,
                    latency_p50_ms, latency_p95_ms, latency_p99_ms, latency_p999_ms,
                    latency_mean_ms, latency_min_ms, latency_max_ms,
                    latency_iqr_ms, latency_stddev_ms, latency_outlier_count,
                    resource_cpu_percent_mean, resource_cpu_percent_max,
                    resource_rss_mb_mean, resource_rss_mb_max,
                    resource_disk_write_mb_total,
                    resource_net_sent_mb_total, resource_net_recv_mb_total
                ) VALUES (
                    :run_id, :database_name, :operation,
                    :batch_size, :workers, :total_rows, :sample_count,
                    :rows_per_second_mean, :rows_per_second_p95, :rows_per_second_p99,
                    :mb_per_second_mean,
                    :latency_p50_ms, :latency_p95_ms, :latency_p99_ms, :latency_p999_ms,
                    :latency_mean_ms, :latency_min_ms, :latency_max_ms,
                    :latency_iqr_ms, :latency_stddev_ms, :latency_outlier_count,
                    :resource_cpu_percent_mean, :resource_cpu_percent_max,
                    :resource_rss_mb_mean, :resource_rss_mb_max,
                    :resource_disk_write_mb_total,
                    :resource_net_sent_mb_total, :resource_net_recv_mb_total
                )
                """,
                d,
            )

    def load_summaries(
        self,
        run_ids: list[str] | None = None,
        database_name: str | None = None,
        operation: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Load summary rows, with optional filters.

        Returns plain dicts ready for export or display.
        """
        conditions: list[str] = []
        params: list[Any] = []
        if run_ids is not None:
            placeholders = ", ".join("?" * len(run_ids))
            conditions.append(f"run_id IN ({placeholders})")
            params.extend(run_ids)
        if database_name is not None:
            conditions.append("database_name = ?")
            params.append(database_name)
        if operation is not None:
            conditions.append("operation = ?")
            params.append(operation)

        where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        with self._connect() as conn:
            rows = conn.execute(
                f"SELECT * FROM summaries {where} ORDER BY run_id, database_name, operation",
                params,
            ).fetchall()
        return [dict(r) for r in rows]

    # ------------------------------------------------------------------
    # Per-operation Parquet files
    # ------------------------------------------------------------------

    def save_operation_results(
        self,
        results: list[OperationResult],
    ) -> Path | None:
        """
        Append *results* to the appropriate Parquet file.

        File path: ``{parquet_dir}/{run_id}/{database_name}/{operation}.parquet``

        Returns the path written to, or None if *results* is empty.
        """
        if not results:
            return None

        first = results[0]
        out_dir = (
            self.parquet_dir
            / first.run_id
            / first.database_name
        )
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / f"{first.operation}.parquet"

        table = _operation_results_to_arrow(results)

        if out_path.exists():
            existing = pq.read_table(out_path)
            table = pa.concat_tables([existing, table])

        pq.write_table(
            table,
            out_path,
            compression="snappy",
            row_group_size=50_000,
        )
        return out_path

    def load_operation_results(
        self,
        run_id: str,
        database_name: str,
        operation: str,
    ) -> list[dict[str, Any]]:
        """
        Read back per-operation Parquet rows as plain dicts.
        Returns an empty list if the file does not exist.
        """
        path = self.parquet_dir / run_id / database_name / f"{operation}.parquet"
        if not path.exists():
            return []
        tbl = pq.read_table(path)
        return tbl.to_pydict()  # type: ignore[return-value]

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.sqlite_path)
        conn.row_factory = sqlite3.Row
        # Enable WAL mode for better concurrent read performance
        conn.execute("PRAGMA journal_mode=WAL")
        return conn


# ---------------------------------------------------------------------------
# Module-level factory
# ---------------------------------------------------------------------------


def get_storage() -> ResultStorage:
    """
    Return a :class:`ResultStorage` instance using the current settings.

    Calls :meth:`~ResultStorage.initialize` on first use.
    """
    from tsbm.config.settings import get_settings

    cfg = get_settings()
    storage = ResultStorage(
        sqlite_path=cfg.results.sqlite_path,
        parquet_dir=cfg.results.parquet_dir,
    )
    storage.initialize()
    return storage


# ---------------------------------------------------------------------------
# Private helper
# ---------------------------------------------------------------------------


def _operation_results_to_arrow(results: list[OperationResult]) -> pa.Table:
    """Convert a list of OperationResult objects to a PyArrow Table."""
    return pa.table(
        {
            "run_id":          [r.run_id          for r in results],
            "database_name":   [r.database_name   for r in results],
            "operation":       [r.operation       for r in results],
            "phase":           [r.phase           for r in results],
            "iteration":       [r.iteration       for r in results],
            "batch_size":      [r.batch_size      for r in results],
            "workers":         [r.workers         for r in results],
            "elapsed_ns":      [r.elapsed_ns      for r in results],
            "rows_processed":  [r.rows_processed  for r in results],
            "bytes_processed": [r.bytes_processed for r in results],
            "rows_per_second": [r.rows_per_second for r in results],
            "mb_per_second":   [r.mb_per_second   for r in results],
        },
        schema=_OPERATION_SCHEMA,
    )

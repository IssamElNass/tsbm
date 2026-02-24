"""
Query benchmarks with database-specific SQL templates.

Five query workloads are implemented:

  TimeRangeBenchmark      — point-in-time window scan (WHERE ts >= x AND ts < y)
  AggregationBenchmark    — hourly aggregation (SAMPLE BY / DATE_BIN / time_bucket)
  LastPointBenchmark      — latest value per tag (LATEST ON / DISTINCT ON)
  HighCardinalityBenchmark— GROUP BY on tag with many distinct values
  DownsamplingBenchmark   — multi-resolution aggregation (1-min, 1-hour, 1-day)

SQL template strategy
---------------------
* QuestDB:      f-strings with sanitised/trusted values (no $1 for some query types)
* CrateDB:      asyncpg $1/$2 parameterised
* TimescaleDB:  asyncpg $1/$2 parameterised

Random time windows use ``numpy.random.default_rng(config.prng_seed)`` so
each run is deterministic and reproducible.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pyarrow.compute as pc

from tsbm.benchmarks.base import BenchmarkResult
from tsbm.benchmarks.ingestion import _build_summary
from tsbm.datasets.schema import DatasetSchema
from tsbm.metrics.timer import TimingResult, timed_operation
from tsbm.results.models import OperationResult

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Column-selection helpers
# ---------------------------------------------------------------------------


def _pick_metric(schema: "DatasetSchema", config: Any) -> str:
    """
    Return the metric column to use for aggregations.

    Checks ``config.agg_metric_col`` first; falls back to the first entry in
    ``schema.metric_cols``.  This lets callers override the default via
    ``benchmark.toml`` or an environment variable instead of always picking
    column index 0 (which is often an ID-like column).
    """
    override = getattr(config, "agg_metric_col", None)
    if override:
        return override
    return schema.metric_cols[0] if schema.metric_cols else "value"


def _pick_tag(schema: "DatasetSchema", config: Any) -> str:
    """
    Return the tag column to use for GROUP BY / PARTITION BY queries.

    Checks ``config.group_by_tag_col`` first; falls back to the first entry
    in ``schema.tag_cols``.
    """
    override = getattr(config, "group_by_tag_col", None)
    if override:
        return override
    return schema.tag_cols[0] if schema.tag_cols else "device_id"


# Supported adapter names
_DB_QUESTDB = "questdb"
_DB_CRATEDB = "cratedb"
_DB_TIMESCALEDB = "timescaledb"


# ---------------------------------------------------------------------------
# Time-window helpers
# ---------------------------------------------------------------------------


def _random_windows(
    n: int,
    table: Any,
    ts_col: str,
    window_duration: timedelta,
    seed: int,
) -> list[tuple[datetime, datetime]]:
    """
    Generate *n* random non-overlapping time windows within the dataset range.
    """
    # Ensure tz-aware datetime objects
    def _to_aware(dt: Any) -> datetime:
        if isinstance(dt, datetime):
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        return datetime.fromtimestamp(dt / 1e9, tz=timezone.utc)

    # Use vectorized Arrow compute to find min/max — O(n) in C++, near-zero extra memory.
    # Avoid .to_pylist() + sorted() which would materialise all N timestamps to Python
    # objects and sort them in O(n log n), costing minutes on million-row datasets.
    col = table.column(ts_col)
    t_min_raw = pc.min(col).as_py()
    t_max_raw = pc.max(col).as_py()
    if t_min_raw is None or t_max_raw is None:
        return []

    t_min = _to_aware(t_min_raw)
    t_max = _to_aware(t_max_raw)
    total_range = (t_max - t_min).total_seconds()

    if total_range <= window_duration.total_seconds():
        return [(t_min, t_max)]

    rng = np.random.default_rng(seed)
    max_offset = total_range - window_duration.total_seconds()
    offsets = rng.uniform(0, max_offset, size=n)
    windows = []
    for offset in offsets:
        start = t_min + timedelta(seconds=float(offset))
        end = start + window_duration
        windows.append((start, end))
    return windows


def _window_from_config(cfg_str: str) -> timedelta:
    """Parse a window string like '1min', '1h', '1day', '1week' into timedelta."""
    mapping = {
        "1min": timedelta(minutes=1),
        "5min": timedelta(minutes=5),
        "1h":   timedelta(hours=1),
        "1hour": timedelta(hours=1),
        "1day": timedelta(days=1),
        "1week": timedelta(weeks=1),
    }
    return mapping.get(cfg_str.lower(), timedelta(hours=1))


# ---------------------------------------------------------------------------
# Base class for query benchmarks
# ---------------------------------------------------------------------------


class _QueryBenchmarkBase:
    """
    Shared run() logic for all query benchmarks.

    Subclasses override ``_build_sql()`` to return the database-specific SQL
    and optionally ``_params()`` to return asyncpg parameter tuples.
    """

    name: str = "query"
    description: str = "Generic query benchmark"

    async def run(
        self,
        adapter: Any,
        dataset: Any,
        config: Any,
        run_id: str,
    ) -> BenchmarkResult:
        result = BenchmarkResult(
            run_id=run_id,
            database_name=adapter.name,
            benchmark_name=self.name,
        )

        table = dataset.load()
        schema = dataset.schema
        windows = _random_windows(
            n=config.measurement_rounds + config.warmup_iterations,
            table=table,
            ts_col=schema.timestamp_col,
            window_duration=_window_from_config(
                config.time_windows[0] if config.time_windows else "1h"
            ),
            seed=config.prng_seed,
        )
        if not windows:
            result.errors.append("No valid time windows; dataset may be empty.")
            return result

        queries = self._make_queries(schema, adapter.name, windows, config)

        # ---- Warmup ----
        warmup_q = queries[: config.warmup_iterations]
        for i, (sql, params) in enumerate(warmup_q):
            try:
                _, timing = await adapter.execute_query(sql, params)
                result.operation_results.append(
                    OperationResult.from_timing(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=self.name,
                        phase="warmup",
                        iteration=i,
                        timing=timing,
                    )
                )
            except Exception as exc:
                logger.warning("Warmup error (db=%s op=%s iter=%d): %s", adapter.name, self.name, i, exc)

        # ---- Measurement ----
        measurement_q = queries[config.warmup_iterations:]
        measurement_timings: list[TimingResult] = []
        for i, (sql, params) in enumerate(measurement_q[: config.measurement_rounds]):
            try:
                _, timing = await adapter.execute_query(sql, params)
                measurement_timings.append(timing)
                result.operation_results.append(
                    OperationResult.from_timing(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=self.name,
                        phase="measurement",
                        iteration=i,
                        timing=timing,
                    )
                )
            except Exception as exc:
                msg = f"Query error (db={adapter.name} op={self.name} iter={i}): {exc}"
                logger.error(msg)
                result.errors.append(msg)

        if measurement_timings:
            result.summaries.append(
                _build_summary(
                    run_id=run_id,
                    database_name=adapter.name,
                    operation=self.name,
                    batch_size=0,
                    workers=1,
                    timings=measurement_timings,
                )
            )

        return result

    def _make_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        windows: list[tuple[datetime, datetime]],
        config: Any,
    ) -> list[tuple[str, tuple]]:
        """Return a list of (sql, params) tuples for the full query set."""
        raise NotImplementedError

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        """Return representative (name, sql) pairs for dry-run preview."""
        return []


# ---------------------------------------------------------------------------
# TimeRangeBenchmark
# ---------------------------------------------------------------------------


class TimeRangeBenchmark(_QueryBenchmarkBase):
    """Scan all rows within a random time window."""

    name = "time_range"
    description = "Scans all rows within a random time window (WHERE ts >= start AND ts < end)."

    def _make_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        windows: list[tuple[datetime, datetime]],
        config: Any,
    ) -> list[tuple[str, tuple]]:
        tbl = schema.name
        ts = schema.timestamp_col
        queries = []
        for start, end in windows:
            if adapter_name == _DB_QUESTDB:
                # QuestDB: use f-string with ISO timestamps (no $1 parameterisation)
                s = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                e = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                sql = (
                    f'SELECT * FROM "{tbl}" '
                    f'WHERE "{ts}" >= \'{s}\' AND "{ts}" < \'{e}\''
                )
                queries.append((sql, ()))
            else:
                sql = f'SELECT * FROM "{tbl}" WHERE "{ts}" >= $1 AND "{ts}" < $2'
                queries.append((sql, (start, end)))
        return queries

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        tbl = schema.name
        ts = schema.timestamp_col
        if adapter_name == _DB_QUESTDB:
            return [(self.name, f'SELECT * FROM "{tbl}" WHERE "{ts}" >= \'<start>\' AND "{ts}" < \'<end>\'')]
        return [(self.name, f'SELECT * FROM "{tbl}" WHERE "{ts}" >= $1 AND "{ts}" < $2')]


# ---------------------------------------------------------------------------
# AggregationBenchmark
# ---------------------------------------------------------------------------


class AggregationBenchmark(_QueryBenchmarkBase):
    """Hourly aggregation of the first metric column."""

    name = "aggregation"
    description = "Hourly average/min/max aggregation within a time window."

    def _make_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        windows: list[tuple[datetime, datetime]],
        config: Any,
    ) -> list[tuple[str, tuple]]:
        tbl = schema.name
        ts = schema.timestamp_col
        metric = _pick_metric(schema, config)
        queries = []

        for start, end in windows:
            if adapter_name == _DB_QUESTDB:
                s = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                e = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                sql = (
                    f'SELECT "{ts}", avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= \'{s}\' AND "{ts}" < \'{e}\' '
                    f"SAMPLE BY 1h"
                )
                queries.append((sql, ()))
            elif adapter_name == _DB_CRATEDB:
                sql = (
                    f"SELECT DATE_BIN('1 hour', \"{ts}\", TIMESTAMP '1970-01-01') AS bucket, "
                    f'avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 '
                    f"GROUP BY bucket ORDER BY bucket"
                )
                queries.append((sql, (start, end)))
            else:  # timescaledb
                sql = (
                    f"SELECT time_bucket('1 hour', \"{ts}\") AS bucket, "
                    f'avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 '
                    f"GROUP BY bucket ORDER BY bucket"
                )
                queries.append((sql, (start, end)))
        return queries

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        tbl = schema.name
        ts = schema.timestamp_col
        metric = schema.metric_cols[0] if schema.metric_cols else "value"
        if adapter_name == _DB_QUESTDB:
            return [(self.name,
                f'SELECT "{ts}", avg("{metric}"), min("{metric}"), max("{metric}") '
                f'FROM "{tbl}" WHERE "{ts}" >= \'<start>\' AND "{ts}" < \'<end>\' SAMPLE BY 1h')]
        if adapter_name == _DB_CRATEDB:
            return [(self.name,
                f"SELECT DATE_BIN('1 hour', \"{ts}\", TIMESTAMP '1970-01-01') AS bucket, "
                f'avg("{metric}"), min("{metric}"), max("{metric}") FROM "{tbl}" '
                f'WHERE "{ts}" >= $1 AND "{ts}" < $2 GROUP BY bucket ORDER BY bucket')]
        return [(self.name,
            f"SELECT time_bucket('1 hour', \"{ts}\") AS bucket, "
            f'avg("{metric}"), min("{metric}"), max("{metric}") FROM "{tbl}" '
            f'WHERE "{ts}" >= $1 AND "{ts}" < $2 GROUP BY bucket ORDER BY bucket')]


# ---------------------------------------------------------------------------
# LastPointBenchmark
# ---------------------------------------------------------------------------


class LastPointBenchmark(_QueryBenchmarkBase):
    """Latest value per device (tag)."""

    name = "last_point"
    description = "Retrieves the most recent metric value for each unique tag value."

    def _make_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        windows: list[tuple[datetime, datetime]],
        config: Any,
    ) -> list[tuple[str, tuple]]:
        tbl = schema.name
        ts = schema.timestamp_col
        tag = _pick_tag(schema, config)
        metric = _pick_metric(schema, config)
        queries = []

        for _ in windows:
            if adapter_name == _DB_QUESTDB:
                sql = (
                    f'SELECT "{tag}", "{metric}", "{ts}" '
                    f'FROM "{tbl}" '
                    f'LATEST ON "{ts}" PARTITION BY "{tag}"'
                )
                queries.append((sql, ()))
            elif adapter_name == _DB_CRATEDB:
                # CrateDB does not support DISTINCT ON (PostgreSQL-specific).
                # Use a ROW_NUMBER() window function subquery instead.
                sql = (
                    f'SELECT "{tag}", "{metric}", "{ts}" '
                    f'FROM ('
                    f'SELECT "{tag}", "{metric}", "{ts}", '
                    f'ROW_NUMBER() OVER (PARTITION BY "{tag}" ORDER BY "{ts}" DESC) AS _rn '
                    f'FROM "{tbl}"'
                    f') _sub WHERE _rn = 1 ORDER BY "{tag}"'
                )
                queries.append((sql, ()))
            else:  # timescaledb
                sql = (
                    f'SELECT DISTINCT ON ("{tag}") "{tag}", "{metric}", "{ts}" '
                    f'FROM "{tbl}" '
                    f'ORDER BY "{tag}", "{ts}" DESC'
                )
                queries.append((sql, ()))
        return queries

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        tbl = schema.name
        ts = schema.timestamp_col
        tag = schema.tag_cols[0] if schema.tag_cols else "device_id"
        metric = schema.metric_cols[0] if schema.metric_cols else "value"
        if adapter_name == _DB_QUESTDB:
            return [(self.name,
                f'SELECT "{tag}", "{metric}", "{ts}" FROM "{tbl}" LATEST ON "{ts}" PARTITION BY "{tag}"')]
        return [(self.name,
            f'SELECT "{tag}", "{metric}", "{ts}" FROM ('
            f'SELECT "{tag}", "{metric}", "{ts}", '
            f'ROW_NUMBER() OVER (PARTITION BY "{tag}" ORDER BY "{ts}" DESC) AS _rn '
            f'FROM "{tbl}") _sub WHERE _rn = 1 ORDER BY "{tag}"')]


# ---------------------------------------------------------------------------
# HighCardinalityBenchmark
# ---------------------------------------------------------------------------


class HighCardinalityBenchmark(_QueryBenchmarkBase):
    """
    GROUP BY on the tag column to stress cardinality handling.

    Counts rows and computes average metric per tag within a time window.
    """

    name = "high_cardinality"
    description = "GROUP BY on tag column within a time window — tests cardinality performance."

    def _make_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        windows: list[tuple[datetime, datetime]],
        config: Any,
    ) -> list[tuple[str, tuple]]:
        tbl = schema.name
        ts = schema.timestamp_col
        tag = _pick_tag(schema, config)
        metric = _pick_metric(schema, config)
        queries = []

        for start, end in windows:
            if adapter_name == _DB_QUESTDB:
                s = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                e = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                sql = (
                    f'SELECT "{tag}", count(), avg("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= \'{s}\' AND "{ts}" < \'{e}\' '
                    f'ORDER BY "{tag}"'
                )
                queries.append((sql, ()))
            else:
                sql = (
                    f'SELECT "{tag}", COUNT(*) as cnt, avg("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 '
                    f'GROUP BY "{tag}" ORDER BY "{tag}"'
                )
                queries.append((sql, (start, end)))
        return queries

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        tbl = schema.name
        ts = schema.timestamp_col
        tag = schema.tag_cols[0] if schema.tag_cols else "device_id"
        metric = schema.metric_cols[0] if schema.metric_cols else "value"
        if adapter_name == _DB_QUESTDB:
            return [(self.name,
                f'SELECT "{tag}", count(), avg("{metric}") FROM "{tbl}" '
                f'WHERE "{ts}" >= \'<start>\' AND "{ts}" < \'<end>\' ORDER BY "{tag}"')]
        return [(self.name,
            f'SELECT "{tag}", COUNT(*) as cnt, avg("{metric}") FROM "{tbl}" '
            f'WHERE "{ts}" >= $1 AND "{ts}" < $2 GROUP BY "{tag}" ORDER BY "{tag}"')]


# ---------------------------------------------------------------------------
# DownsamplingBenchmark
# ---------------------------------------------------------------------------


class DownsamplingBenchmark(_QueryBenchmarkBase):
    """
    Multi-resolution downsampling over the full dataset range.

    Cycles through granularities: 1 minute, 1 hour, 1 day.
    Each measurement round uses the next granularity in the cycle.
    """

    name = "downsampling"
    description = "Multi-resolution time-series downsampling (1min / 1h / 1day)."

    _GRANULARITIES_QUESTDB = ["1m", "1h", "1d"]
    _GRANULARITIES_DATE_BIN = ["1 minute", "1 hour", "1 day"]
    _GRANULARITIES_TIME_BUCKET = ["1 minute", "1 hour", "1 day"]

    def _make_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
        windows: list[tuple[datetime, datetime]],
        config: Any,
    ) -> list[tuple[str, tuple]]:
        tbl = schema.name
        ts = schema.timestamp_col
        metric = _pick_metric(schema, config)
        queries = []

        for i, (start, end) in enumerate(windows):
            if adapter_name == _DB_QUESTDB:
                gran = self._GRANULARITIES_QUESTDB[i % len(self._GRANULARITIES_QUESTDB)]
                s = start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                e = end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
                sql = (
                    f'SELECT "{ts}", avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= \'{s}\' AND "{ts}" < \'{e}\' '
                    f"SAMPLE BY {gran}"
                )
                queries.append((sql, ()))
            elif adapter_name == _DB_CRATEDB:
                gran = self._GRANULARITIES_DATE_BIN[i % len(self._GRANULARITIES_DATE_BIN)]
                sql = (
                    f"SELECT DATE_BIN('{gran}', \"{ts}\", TIMESTAMP '1970-01-01') AS bucket, "
                    f'avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 '
                    f"GROUP BY bucket ORDER BY bucket"
                )
                queries.append((sql, (start, end)))
            else:  # timescaledb
                gran = self._GRANULARITIES_TIME_BUCKET[i % len(self._GRANULARITIES_TIME_BUCKET)]
                sql = (
                    f"SELECT time_bucket('{gran}', \"{ts}\") AS bucket, "
                    f'avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 '
                    f"GROUP BY bucket ORDER BY bucket"
                )
                queries.append((sql, (start, end)))
        return queries

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        tbl = schema.name
        ts = schema.timestamp_col
        metric = schema.metric_cols[0] if schema.metric_cols else "value"
        result = []
        for gran_q, gran_db, gran_ts in zip(
            self._GRANULARITIES_QUESTDB,
            self._GRANULARITIES_DATE_BIN,
            self._GRANULARITIES_TIME_BUCKET,
        ):
            if adapter_name == _DB_QUESTDB:
                result.append((f"{self.name}_{gran_q}",
                    f'SELECT "{ts}", avg("{metric}"), min("{metric}"), max("{metric}") '
                    f'FROM "{tbl}" WHERE "{ts}" >= \'<start>\' AND "{ts}" < \'<end>\' SAMPLE BY {gran_q}'))
            elif adapter_name == _DB_CRATEDB:
                result.append((f"{self.name}_{gran_db}",
                    f"SELECT DATE_BIN('{gran_db}', \"{ts}\", TIMESTAMP '1970-01-01') AS bucket, "
                    f'avg("{metric}"), min("{metric}"), max("{metric}") FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 GROUP BY bucket ORDER BY bucket'))
            else:
                result.append((f"{self.name}_{gran_ts}",
                    f"SELECT time_bucket('{gran_ts}', \"{ts}\") AS bucket, "
                    f'avg("{metric}"), min("{metric}"), max("{metric}") FROM "{tbl}" '
                    f'WHERE "{ts}" >= $1 AND "{ts}" < $2 GROUP BY bucket ORDER BY bucket'))
        return result

"""
Materialized view benchmarks.

MaterializedViewBenchmark
    Compares raw aggregation query latency vs. equivalent query against a
    materialized view / continuous aggregate.
    Supported: TimescaleDB (continuous aggregates), CrateDB (summary tables),
    QuestDB (native MVs via SAMPLE BY / REFRESH IMMEDIATE).

LateArrivalBenchmark
    Measures MV recompute cost when late-arriving data is inserted.
    Each round: insert late data → refresh MV for affected window → query MV.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from tsbm.adapters.base import MVCapableAdapter
from tsbm.benchmarks.base import BenchmarkResult
from tsbm.benchmarks.ingestion import _build_summary
from tsbm.datasets.schema import DatasetSchema
from tsbm.metrics.timer import TimingResult, timed_operation
from tsbm.results.models import OperationResult

logger = logging.getLogger(__name__)

_VIEW_SUFFIX = "_mv_bench"
_LA_VIEW_SUFFIX = "_la_bench"


# ---------------------------------------------------------------------------
# MaterializedViewBenchmark
# ---------------------------------------------------------------------------


class MaterializedViewBenchmark:
    """
    Compare raw aggregation latency vs. query-against-MV latency.

    Results are stored under two operation names:
    * ``materialized_view_raw`` — raw query against the source table
    * ``materialized_view_mv``  — equivalent query against the MV/summary table

    Speedup ratio (raw_mean / mv_mean) is logged at INFO level.
    """

    name = "materialized_view"
    description = (
        "Compares raw aggregation query latency vs. the equivalent query "
        "against a materialized view / continuous aggregate."
    )

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

        if not isinstance(adapter, MVCapableAdapter):
            result.errors.append(
                f"{adapter.name} does not implement MVCapableAdapter; skipping."
            )
            return result

        table = dataset.load()
        schema = dataset.schema
        table_name = schema.name
        view_name = f"{table_name}{_VIEW_SUFFIX}"
        granularity = getattr(config, "mv_granularity", "1 hour")

        # Setup
        try:
            await adapter.create_materialized_view(
                view_name=view_name,
                source_table=table_name,
                schema=schema,
                granularity=granularity,
            )
        except Exception as exc:
            result.errors.append(f"create_materialized_view failed: {exc}")
            return result

        if not await adapter.view_exists(view_name):
            result.errors.append(
                f"View {view_name!r} not found after creation "
                "(adapter may not support materialized views — skipping measurements)."
            )
            return result

        # Generate time windows
        windows = _random_windows_from_table(
            table=table,
            ts_col=schema.timestamp_col,
            n=config.warmup_iterations + config.measurement_rounds,
            seed=config.prng_seed,
            window_duration=timedelta(hours=2),
        )
        if not windows:
            result.errors.append("No valid time windows (dataset may be empty).")
            await _safe_drop_mv(adapter, view_name)
            return result

        metric = schema.metric_cols[0] if schema.metric_cols else None
        if metric is None:
            result.errors.append("No metric columns found; skipping.")
            await _safe_drop_mv(adapter, view_name)
            return result

        raw_timings: list[TimingResult] = []
        mv_timings: list[TimingResult] = []

        for i, (start, end) in enumerate(windows):
            phase = "warmup" if i < config.warmup_iterations else "measurement"

            # Round A: raw query
            raw_sql, raw_params = _build_agg_query(
                table_name, schema.timestamp_col, metric, schema.tag_cols,
                adapter.name, start, end, granularity,
            )
            try:
                _, raw_t = await adapter.execute_query(raw_sql, raw_params)
                result.operation_results.append(
                    OperationResult.from_timing(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=f"{self.name}_raw",
                        phase=phase,
                        iteration=i,
                        timing=raw_t,
                    )
                )
                if phase == "measurement":
                    raw_timings.append(raw_t)
            except Exception as exc:
                result.errors.append(f"Raw query error iter={i}: {exc}")
                logger.warning("MV benchmark raw query error iter=%d: %s", i, exc)

            # Round B: MV query
            mv_sql, mv_params = _build_mv_query(
                view_name, schema.tag_cols, metric,
                adapter.name, start, end,
                ts_col=schema.timestamp_col,
            )
            try:
                _, mv_t = await adapter.execute_query(mv_sql, mv_params)
                result.operation_results.append(
                    OperationResult.from_timing(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=f"{self.name}_mv",
                        phase=phase,
                        iteration=i,
                        timing=mv_t,
                    )
                )
                if phase == "measurement":
                    mv_timings.append(mv_t)
            except Exception as exc:
                result.errors.append(f"MV query error iter={i}: {exc}")
                logger.warning("MV benchmark mv query error iter=%d: %s", i, exc)

        # Build summaries
        if raw_timings:
            result.summaries.append(
                _build_summary(run_id, adapter.name, f"{self.name}_raw", 0, 1, raw_timings)
            )
        if mv_timings:
            result.summaries.append(
                _build_summary(run_id, adapter.name, f"{self.name}_mv", 0, 1, mv_timings)
            )

        # Log speedup ratio
        if raw_timings and mv_timings:
            raw_mean = sum(t.elapsed_ms for t in raw_timings) / len(raw_timings)
            mv_mean = sum(t.elapsed_ms for t in mv_timings) / len(mv_timings)
            speedup = raw_mean / mv_mean if mv_mean > 0 else 0.0
            logger.info(
                "%s materialized_view: raw=%.2fms mv=%.2fms speedup=%.2fx",
                adapter.name, raw_mean, mv_mean, speedup,
            )

        # Teardown
        await _safe_drop_mv(adapter, view_name)
        return result

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        return [
            (
                f"{self.name}_raw",
                f"SELECT time_bucket/DATE_BIN/SAMPLE BY ... FROM {schema.name} GROUP BY bucket",
            ),
            (
                f"{self.name}_mv",
                f"SELECT * FROM {schema.name}{_VIEW_SUFFIX} WHERE ts_bucket >= ... AND ts_bucket < ...",
            ),
        ]


# ---------------------------------------------------------------------------
# LateArrivalBenchmark
# ---------------------------------------------------------------------------


class LateArrivalBenchmark:
    """
    Measure MV recompute cost when late-arriving data is inserted.

    Each measurement round:
      1. Pick a random time window 30 min–48 h before the dataset's end.
      2. Insert a small batch of rows with timestamps in that past window.
      3. Refresh the MV for the affected window only (incremental).
      4. Query the MV to confirm the data appears.

    Three summaries are produced:
    * ``late_arrival_insert``  — insert latency for the late batch
    * ``late_arrival_refresh`` — MV refresh latency
    * ``late_arrival_query``   — MV query latency after refresh
    """

    name = "late_arrival"
    description = (
        "Measures MV recompute cost when out-of-order / late-arriving data "
        "is inserted: insert → incremental refresh → verify query."
    )

    _MIN_LATENESS_MINUTES = 30
    _MAX_LATENESS_HOURS = 48

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

        if not isinstance(adapter, MVCapableAdapter):
            result.errors.append(
                f"{adapter.name} does not implement MVCapableAdapter; skipping."
            )
            return result

        table = dataset.load()
        schema = dataset.schema
        table_name = schema.name
        view_name = f"{table_name}{_LA_VIEW_SUFFIX}"
        granularity = getattr(config, "mv_granularity", "1 hour")
        late_batch_size = getattr(config, "late_arrival_batch_size", 100)

        # Setup
        try:
            await adapter.create_materialized_view(
                view_name=view_name,
                source_table=table_name,
                schema=schema,
                granularity=granularity,
            )
        except Exception as exc:
            result.errors.append(f"Setup failed: {exc}")
            return result

        if not await adapter.view_exists(view_name):
            result.errors.append(
                f"View {view_name!r} not found after creation; skipping measurements."
            )
            return result

        # Determine dataset time range for "late" window generation
        ts_col_arr = table.column(schema.timestamp_col)
        ts_ns_max = pc.max(ts_col_arr.cast(pa.int64())).as_py()
        if ts_ns_max is None:
            result.errors.append("Cannot determine dataset time range; skipping.")
            await _safe_drop_mv(adapter, view_name)
            return result

        ts_max = datetime.fromtimestamp(ts_ns_max / 1e9, tz=timezone.utc)
        rng = np.random.default_rng(config.prng_seed)
        metric = schema.metric_cols[0] if schema.metric_cols else None

        insert_timings: list[TimingResult] = []
        refresh_timings: list[TimingResult] = []
        query_timings: list[TimingResult] = []

        total_rounds = config.warmup_iterations + config.measurement_rounds

        for i in range(total_rounds):
            phase = "warmup" if i < config.warmup_iterations else "measurement"

            # 1. Pick random past window
            lateness_minutes = int(
                rng.integers(
                    self._MIN_LATENESS_MINUTES,
                    self._MAX_LATENESS_HOURS * 60,
                )
            )
            late_start = ts_max - timedelta(minutes=lateness_minutes)
            late_end = late_start + timedelta(hours=1)

            # 2. Build late batch
            late_batch = _select_or_generate_late_rows(
                table=table,
                schema=schema,
                start=late_start,
                end=late_end,
                batch_size=late_batch_size,
                rng=rng,
            )
            if len(late_batch) == 0:
                logger.warning("LateArrival: empty late batch for window %s–%s; skipping round %d", late_start, late_end, i)
                continue

            # 3. Insert late data
            try:
                insert_t = await adapter.ingest_batch(late_batch, table_name)
                await adapter.flush()
                result.operation_results.append(
                    OperationResult.from_timing(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=f"{self.name}_insert",
                        phase=phase,
                        iteration=i,
                        timing=insert_t,
                    )
                )
                if phase == "measurement":
                    insert_timings.append(insert_t)
            except Exception as exc:
                result.errors.append(f"Insert error iter={i}: {exc}")
                logger.warning("LateArrival insert error iter=%d: %s", i, exc)
                continue

            # 4. Refresh MV for the affected window
            try:
                refresh_t = await adapter.refresh_materialized_view(
                    view_name, late_start, late_end
                )
                result.operation_results.append(
                    OperationResult.from_timing(
                        run_id=run_id,
                        database_name=adapter.name,
                        operation=f"{self.name}_refresh",
                        phase=phase,
                        iteration=i,
                        timing=refresh_t,
                    )
                )
                if phase == "measurement":
                    refresh_timings.append(refresh_t)
            except Exception as exc:
                result.errors.append(f"Refresh error iter={i}: {exc}")
                logger.warning("LateArrival refresh error iter=%d: %s", i, exc)

            # 5. Query MV to verify data appears
            if metric is not None:
                try:
                    verify_sql, verify_params = _build_mv_query(
                        view_name, schema.tag_cols, metric,
                        adapter.name, late_start, late_end,
                        ts_col=schema.timestamp_col,
                    )
                    _, query_t = await adapter.execute_query(verify_sql, verify_params)
                    result.operation_results.append(
                        OperationResult.from_timing(
                            run_id=run_id,
                            database_name=adapter.name,
                            operation=f"{self.name}_query",
                            phase=phase,
                            iteration=i,
                            timing=query_t,
                        )
                    )
                    if phase == "measurement":
                        query_timings.append(query_t)
                except Exception as exc:
                    result.errors.append(f"Verify query error iter={i}: {exc}")
                    logger.warning("LateArrival verify error iter=%d: %s", i, exc)

        # Build summaries
        for op_name, timings in [
            (f"{self.name}_insert",  insert_timings),
            (f"{self.name}_refresh", refresh_timings),
            (f"{self.name}_query",   query_timings),
        ]:
            if timings:
                result.summaries.append(
                    _build_summary(run_id, adapter.name, op_name, 0, 1, timings)
                )

        # Teardown
        await _safe_drop_mv(adapter, view_name)
        return result

    def get_queries(
        self,
        schema: DatasetSchema,
        adapter_name: str,
    ) -> list[tuple[str, str]]:
        return [
            (f"{self.name}_insert",  f"INSERT INTO {schema.name} ... (late-arriving batch)"),
            (f"{self.name}_refresh", "CALL refresh_continuous_aggregate / DELETE+INSERT summary"),
            (f"{self.name}_query",   f"SELECT FROM {schema.name}{_LA_VIEW_SUFFIX} WHERE ts_bucket ..."),
        ]


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _random_windows_from_table(
    table: Any,
    ts_col: str,
    n: int,
    seed: int,
    window_duration: timedelta = timedelta(hours=1),
) -> list[tuple[datetime, datetime]]:
    """Generate *n* random time windows covering the dataset's time range."""
    ts_arr = table.column(ts_col)
    ts_ns_min = pc.min(ts_arr.cast(pa.int64())).as_py()
    ts_ns_max = pc.max(ts_arr.cast(pa.int64())).as_py()

    if ts_ns_min is None or ts_ns_max is None:
        return []

    t_min = datetime.fromtimestamp(ts_ns_min / 1e9, tz=timezone.utc)
    t_max = datetime.fromtimestamp(ts_ns_max / 1e9, tz=timezone.utc)
    total_sec = (t_max - t_min).total_seconds()

    if total_sec <= window_duration.total_seconds():
        return [(t_min, t_max)] * n

    rng = np.random.default_rng(seed)
    max_offset = total_sec - window_duration.total_seconds()
    offsets = rng.uniform(0, max_offset, size=n)
    return [
        (t_min + timedelta(seconds=float(off)), t_min + timedelta(seconds=float(off)) + window_duration)
        for off in offsets
    ]


def _build_agg_query(
    table_name: str,
    ts_col: str,
    metric: str,
    tag_cols: list[str],
    adapter_name: str,
    start: datetime,
    end: datetime,
    granularity: str = "1 hour",
) -> tuple[str, tuple]:
    """
    Build a time-bucket aggregation query against the source table.

    SQL templates mirror those in queries.py AggregationBenchmark.
    """
    tag_select = (", ".join(f'"{t}"' for t in tag_cols) + ", ") if tag_cols else ""
    tag_group = (", " + ", ".join(f'"{t}"' for t in tag_cols)) if tag_cols else ""

    if adapter_name == "questdb":
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        end_iso = end.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        sql = (
            f'SELECT {tag_select}avg("{metric}") FROM "{table_name}" '
            f"WHERE \"{ts_col}\" >= '{start_iso}' AND \"{ts_col}\" < '{end_iso}' "
            f"SAMPLE BY {_granularity_to_questdb(granularity)}"
        )
        return sql, ()
    elif adapter_name == "cratedb":
        sql = (
            f"SELECT DATE_BIN('{granularity}', \"{ts_col}\", TIMESTAMP '1970-01-01') AS ts_bucket, "
            f"{tag_select}avg(\"{metric}\") "
            f'FROM "{table_name}" '
            f'WHERE "{ts_col}" >= $1 AND "{ts_col}" < $2 '
            f"GROUP BY ts_bucket{tag_group} "
            f"ORDER BY ts_bucket"
        )
        return sql, (start, end)
    else:  # timescaledb
        sql = (
            f"SELECT time_bucket('{granularity}', \"{ts_col}\") AS ts_bucket, "
            f"{tag_select}avg(\"{metric}\") "
            f'FROM "{table_name}" '
            f'WHERE "{ts_col}" >= $1 AND "{ts_col}" < $2 '
            f"GROUP BY ts_bucket{tag_group} "
            f"ORDER BY ts_bucket"
        )
        return sql, (start, end)


def _build_mv_query(
    view_name: str,
    tag_cols: list[str],
    metric: str,
    adapter_name: str,
    start: datetime,
    end: datetime,
    ts_col: str | None = None,
) -> tuple[str, tuple]:
    """
    Build a query against the materialized view / summary table.

    * TimescaleDB / CrateDB: MV has a ``ts_bucket`` column (time-bucketed ts).
    * QuestDB: MV keeps the original timestamp column name (passed via
      ``ts_col``); timestamps are embedded as ISO-8601 literals because
      QuestDB's PG-wire parameterised binding for timestamps can be limited.

    Columns: <ts_col or ts_bucket>, [tag_cols], avg_<metric>,
             min_<metric>, max_<metric>
    """
    tag_select = (", ".join(f'"{t}"' for t in tag_cols) + ", ") if tag_cols else ""

    if adapter_name == "questdb":
        # QuestDB MVs keep the original designated timestamp column name.
        bucket_col = ts_col if ts_col else "ts"
        start_iso = start.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        end_iso = end.strftime("%Y-%m-%dT%H:%M:%S.000000Z")
        sql = (
            f'SELECT "{bucket_col}", {tag_select}"avg_{metric}" '
            f'FROM "{view_name}" '
            f"WHERE \"{bucket_col}\" >= '{start_iso}' AND \"{bucket_col}\" < '{end_iso}' "
            f'ORDER BY "{bucket_col}"'
        )
        return sql, ()
    else:
        sql = (
            f'SELECT ts_bucket, {tag_select}"avg_{metric}" '
            f'FROM "{view_name}" '
            f"WHERE ts_bucket >= $1 AND ts_bucket < $2 "
            f"ORDER BY ts_bucket"
        )
        return sql, (start, end)


def _select_or_generate_late_rows(
    table: Any,
    schema: DatasetSchema,
    start: datetime,
    end: datetime,
    batch_size: int,
    rng: np.random.Generator,
) -> pa.Table:
    """
    Return *batch_size* rows for the given time window.

    Strategy:
    1. Filter the existing table for rows where ts >= start and ts < end.
    2. If enough rows exist: return a random sample of batch_size rows.
    3. If too few rows: generate synthetic rows by taking existing rows
       and shifting their timestamps uniformly into [start, end).
    """
    ts_col = schema.timestamp_col
    start_ns = int(start.timestamp() * 1e9)
    end_ns = int(end.timestamp() * 1e9)

    ts_int = table.column(ts_col).cast(pa.int64())
    mask = pc.and_(
        pc.greater_equal(ts_int, start_ns),
        pc.less(ts_int, end_ns),
    )
    filtered = table.filter(mask)

    if len(filtered) >= batch_size:
        # Random sample
        idx = rng.choice(len(filtered), size=batch_size, replace=False)
        return filtered.take(idx)

    # Not enough rows in window — generate synthetic rows by shifting timestamps
    # Sample from full table and replace timestamps
    n_sample = min(batch_size, len(table))
    if n_sample == 0:
        return table.slice(0, 0)

    idx = rng.choice(len(table), size=n_sample, replace=n_sample > len(table))
    base_rows = table.take(idx)

    # Generate random timestamps in [start_ns, end_ns)
    range_ns = end_ns - start_ns
    if range_ns <= 0:
        return base_rows.slice(0, 0)

    new_ts_ns = start_ns + rng.integers(0, range_ns, size=n_sample)
    new_ts = pa.array(new_ts_ns, type=pa.timestamp("ns", tz="UTC"))
    ts_idx = base_rows.schema.get_field_index(ts_col)
    return base_rows.set_column(ts_idx, ts_col, new_ts)


def _granularity_to_questdb(granularity: str) -> str:
    """Convert a granularity string like '1 hour' to QuestDB SAMPLE BY syntax."""
    mapping = {
        "1 minute": "1m",
        "30 minutes": "30m",
        "1 hour": "1h",
        "6 hours": "6h",
        "1 day": "1d",
    }
    return mapping.get(granularity.lower(), "1h")


async def _safe_drop_mv(adapter: Any, view_name: str) -> None:
    """Drop a materialized view / summary table, suppressing errors."""
    if isinstance(adapter, MVCapableAdapter):
        try:
            await adapter.drop_materialized_view(view_name)
        except Exception as exc:
            logger.warning("drop_materialized_view %r failed: %s", view_name, exc)

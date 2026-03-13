from __future__ import annotations

from pathlib import Path
from typing import Any

try:
    import tomllib
except ImportError:
    import tomli as tomllib  # type: ignore[no-redef]

from pydantic import BaseModel
from pydantic_settings import (
    BaseSettings,
    PydanticBaseSettingsSource,
    SettingsConfigDict,
    TomlConfigSettingsSource,
)


# ---------------------------------------------------------------------------
# Per-database config models
# ---------------------------------------------------------------------------


class QuestDBConfig(BaseModel):
    enabled: bool = True
    host: str = "localhost"
    ilp_port: int = 9000
    pg_port: int = 8812
    # Table partitioning granularity. Valid values: HOUR, DAY, WEEK, MONTH, YEAR.
    # Finer partitions improve query speed on short time ranges but create more
    # file segments.  For 30 M+ rows spanning many days use HOUR; for sparse
    # multi-year datasets use MONTH or YEAR.
    partition_by: str = "DAY"
    # Automatically add INDEX on SYMBOL (TAG) columns in CREATE TABLE DDL.
    # Indexes speed up WHERE filters on tag columns but double write I/O.
    index_symbols: bool = True


class CrateDBConfig(BaseModel):
    enabled: bool = True
    host: str = "localhost"
    http_port: int = 4200
    pg_port: int = 5432


class TimescaleDBConfig(BaseModel):
    enabled: bool = True
    host: str = "localhost"
    pg_port: int = 5433  # Docker host port (maps 5433→5432, avoids CrateDB conflict)
    user: str = "postgres"
    password: str = "postgres"
    dbname: str = "tsbm"
    # Hypertable chunk interval passed to create_hypertable().
    # Smaller intervals = faster range queries on short windows but more chunks
    # and higher metadata overhead.  Use "1 day" for high-frequency data
    # (millions of rows/day) or "7 days" for sparser datasets.
    chunk_time_interval: str = "7 days"


class DatabasesConfig(BaseModel):
    questdb: QuestDBConfig = QuestDBConfig()
    cratedb: CrateDBConfig = CrateDBConfig()
    timescaledb: TimescaleDBConfig = TimescaleDBConfig()


# ---------------------------------------------------------------------------
# Workload config
# ---------------------------------------------------------------------------


class WorkloadConfig(BaseModel):
    dataset: Path = Path("datasets/small_iot.csv")
    batch_sizes: list[int] = [1000, 5000, 10000, 50000]
    workers: list[int] = [1, 4, 8, 16]
    warmup_iterations: int = 5
    measurement_rounds: int = 30
    time_windows: list[str] = ["1min", "1h", "1day", "1week"]
    query_row_limits: list[int] = [10_000, 100_000, 1_000_000]
    prng_seed: int = 42
    mixed_duration_seconds: int = 60
    reset_between_rounds: bool = False
    tag_cardinality_threshold: float = 0.05  # fraction of rows; below → TAG column
    streaming_threshold_rows: int = 500_000  # auto-enable streaming above this row count
    chunk_size: int = 100_000               # rows per batch in streaming mode
    unit_conversions: dict[str, tuple[str, str]] = {}  # {"col": ("from_unit", "to_unit")}
    # Column overrides for query benchmarks.
    # By default every aggregation/GROUP BY uses the first metric/tag column in the
    # dataset schema.  Set these to target a specific column instead — useful when
    # the dataset has an ID-like column in position 0 that should not be aggregated.
    agg_metric_col: str | None = None       # metric column for AVG/MIN/MAX; None → metric_cols[0]
    group_by_tag_col: str | None = None     # tag column for GROUP BY / PARTITION BY; None → tag_cols[0]
    mv_granularity: str = "1 hour"          # time bucket granularity for MV benchmarks
    late_arrival_rounds: int = 10           # iterations for LateArrivalBenchmark
    late_arrival_batch_size: int = 100      # rows per late-arrival insertion
    # Rows to pre-load into the table after warmup but before measurement rounds.
    # 0 = disabled (default).  Only effective when reset_between_rounds = false;
    # if reset_between_rounds = true the seed is wiped at the start of each round.
    ingestion_seed_rows: int = 0
    # Number of concurrent ingest workers used during data seeding.
    # Each worker gets its own adapter/connection and pulls batches from a shared queue.
    # Higher values increase throughput but use more memory and connections.
    seed_workers: int = 4
    # Explicit timestamp column name. Leave empty ("") for auto-detection.
    # Set this when the dataset's timestamp column has a non-standard name
    # (i.e. not one of: time, ts, timestamp, datetime, date, created_at, event_time).
    # Can also be overridden at runtime with --timestamp-col on the CLI.
    timestamp_col: str = ""

    # Multiple dataset sources: list of local paths, directories, globs, or Azure Blob URLs.
    # When set, these are concatenated in order during the seed phase.
    # Each entry can be:
    #   - A local file path: "datasets/part1.parquet"
    #   - A local directory: "datasets/parts/" (all .parquet/.parq files discovered recursively)
    #   - A glob pattern: "datasets/parts/*.parquet"
    #   - An Azure Blob file URL: "az://container/path/file.parquet"
    #   - An Azure container/folder URL: "az://container/" or "az://container/subfolder"
    #     (all Parquet files in the container/folder are discovered automatically)
    #   - HTTPS Azure URL: "https://account.blob.core.windows.net/container/file.parquet"
    # When empty, falls back to the single `dataset` field.
    datasets: list[str] = []

    # Azure storage authentication (used when any dataset entry is an Azure URL).
    # Priority: connection_string > sas_token > DefaultAzureCredential (auto).
    azure_storage_connection_string: str = ""
    azure_storage_sas_token: str = ""
    # Required when using sas_token with az:// URLs (auto-detected from https:// URLs).
    azure_storage_account_name: str = ""


# ---------------------------------------------------------------------------
# Results config
# ---------------------------------------------------------------------------


class ResultsConfig(BaseModel):
    sqlite_path: Path = Path("results/runs.db")
    parquet_dir: Path = Path("results/parquet")


# ---------------------------------------------------------------------------
# Monitor config
# ---------------------------------------------------------------------------


class MonitorConfig(BaseModel):
    enabled: bool = True
    interval_ms: int = 500


# ---------------------------------------------------------------------------
# Top-level settings
# ---------------------------------------------------------------------------


_TOML_CONFIG_PATH = Path("benchmark.toml")


class Settings(BaseSettings):
    """
    Reads configuration from (in priority order, highest first):
      1. Environment variables prefixed with TSBM_ (e.g. TSBM_DATABASES__QUESTDB__HOST)
      2. benchmark.toml in the current working directory
      3. Hard-coded defaults above
    """

    model_config = SettingsConfigDict(
        env_prefix="TSBM_",
        env_nested_delimiter="__",
        # Do not raise on extra fields from TOML that aren't in the model
        extra="ignore",
    )

    databases: DatabasesConfig = DatabasesConfig()
    workload: WorkloadConfig = WorkloadConfig()
    results: ResultsConfig = ResultsConfig()
    monitor: MonitorConfig = MonitorConfig()

    @classmethod
    def settings_customise_sources(
        cls,
        settings_cls: type[BaseSettings],
        init_settings: PydanticBaseSettingsSource,
        env_settings: PydanticBaseSettingsSource,
        dotenv_settings: PydanticBaseSettingsSource,
        file_secret_settings: PydanticBaseSettingsSource,
    ) -> tuple[PydanticBaseSettingsSource, ...]:
        sources: list[PydanticBaseSettingsSource] = [
            init_settings,
            env_settings,
        ]
        if _TOML_CONFIG_PATH.exists():
            sources.append(TomlConfigSettingsSource(settings_cls, toml_file=_TOML_CONFIG_PATH))
        return tuple(sources)


# ---------------------------------------------------------------------------
# Module-level singleton
# ---------------------------------------------------------------------------

_settings: Settings | None = None


def get_settings() -> Settings:
    """Return the cached Settings singleton, creating it on first call."""
    global _settings
    if _settings is None:
        _settings = Settings()
    return _settings


def reset_settings() -> None:
    """Clear the cached singleton (useful in tests that need fresh config)."""
    global _settings
    _settings = None


def load_settings_from_file(path: Path) -> Settings:
    """Load settings from an explicit TOML path (bypasses CWD lookup)."""
    global _TOML_CONFIG_PATH
    original = _TOML_CONFIG_PATH
    _TOML_CONFIG_PATH = path
    reset_settings()
    try:
        return get_settings()
    finally:
        _TOML_CONFIG_PATH = original
        reset_settings()

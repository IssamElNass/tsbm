Plan: Production-Grade Time-Series DB Benchmarking Suite (tsbench)
Context
Building a custom Python benchmarking suite targeting QuestDB, CrateDB, and TimescaleDB from scratch. The primary motivation over existing tools (TSBS, TSM-Bench) is real-dataset support as a first-class feature alongside synthetic data. The suite implements a strict three-phase decoupled pipeline (generate → load → query) that eliminates measurement overhead and ensures reproducibility.

Starting point: c:/Users/issam/Desktop/tsbm/ contains only an empty main.py.

Environment Constraints
Python 3.10.5 — tomllib not in stdlib; use tomli backport (tomli>=2.0; python_version < "3.11")
Pydantic v1 installed — code targets Pydantic v2 API; pyproject.toml pins pydantic>=2.0,<3
QuestDB driver — use questdb v1.x (current stable) with Sender.from_conf() API, not the non-existent v4.1.0 from the spec
hdrh — PyPI package is hdrh, not hdrhistogram; import is from hdrh.histogram import HdrHistogram
pgcopy — drop from deps (psycopg2-based, incompatible with asyncpg); TimescaleDB COPY handled via asyncpg.copy_records_to_table()
Port conflict — CrateDB and TimescaleDB both want 5432; map TimescaleDB to host port 5433:5432 in Docker Compose
asyncpg on Windows — install via pre-built wheel; document in README
Target Project Structure

tsbm/
├── pyproject.toml
├── benchmark.toml
├── src/
│ └── tsbench/
│ ├── **init**.py
│ ├── **main**.py
│ ├── exceptions.py
│ ├── cli/
│ │ ├── main.py
│ │ ├── run.py
│ │ ├── compare.py
│ │ └── dashboard.py
│ ├── adapters/
│ │ ├── base.py # DatabaseAdapter Protocol
│ │ ├── registry.py
│ │ ├── questdb.py # ILP ingestion (questdb v1.x) + asyncpg queries
│ │ ├── cratedb.py # UNNEST ingestion + asyncpg queries
│ │ ├── timescaledb.py # COPY ingestion + asyncpg queries
│ │ └── type_maps.py
│ ├── benchmarks/
│ │ ├── base.py # BenchmarkWorkload Protocol
│ │ ├── ingestion.py
│ │ ├── queries.py # time_range, aggregation, last_point, high_cardinality, downsampling
│ │ ├── mixed.py # concurrent read+write
│ │ └── registry.py
│ ├── datasets/
│ │ ├── loader.py
│ │ ├── schema.py
│ │ ├── timestamps.py
│ │ └── generator.py
│ ├── metrics/
│ │ ├── timer.py
│ │ ├── stats.py
│ │ └── monitor.py
│ ├── results/
│ │ ├── storage.py
│ │ ├── models.py
│ │ └── export.py
│ ├── config/
│ │ └── settings.py
│ ├── dashboard/
│ │ ├── app.py
│ │ └── charts.py
│ └── environment/
│ └── capture.py
├── datasets/
│ ├── small_iot.csv
│ └── dataset_manifest.toml
├── docker/
│ ├── docker-compose.yml
│ └── docker-compose.test.yml
└── tests/
├── conftest.py
├── unit/
└── integration/
Implementation Phases (Build Order)
Phase 0 — Project Scaffold
Creates: pyproject.toml, all **init**.py files, benchmark.toml, src/tsbench/**main**.py, src/tsbench/exceptions.py

pyproject.toml key decisions:

[project]
requires-python = ">=3.10"
dependencies = [
"typer[all]>=0.9",
"rich>=13.0",
"pydantic>=2.0,<3",
"pydantic-settings>=2.3", # 2.3+ has TomlConfigSettingsSource built-in
"tomli>=2.0; python_version < '3.11'",
"pyarrow>=12.0",
"pandas>=2.0",
"numpy>=1.23",
"hdrh>=0.10.0", # NOT "hdrhistogram" — actual PyPI name
"psutil>=5.9",
"asyncpg>=0.28",
"aiohttp>=3.8",
"python-dateutil>=2.8",
"orjson>=3.8",
]

[project.optional-dependencies]
questdb = ["questdb>=1.0"] # v1.x current stable, NOT v4.1.0
cratedb = ["crate>=1.0"]
timescaledb = [] # pure asyncpg, no pgcopy needed
dashboard = ["streamlit>=1.28", "plotly>=5.0"]
all = ["tsbench[questdb,cratedb,timescaledb]"]
dev = ["pytest>=7.0", "pytest-asyncio>=0.21", "pytest-cov>=4.0"]
src/tsbench/exceptions.py:

class TsBenchError(Exception): ...
class ConnectionError(TsBenchError): ...
class IngestionError(TsBenchError): ...
class QueryError(TsBenchError): ...
class ConfigError(TsBenchError): ...
class SchemaError(TsBenchError): ...
Phase 1 — Config + Schema Layer
Creates: src/tsbench/config/settings.py, src/tsbench/datasets/schema.py
Unblocks: Everything

settings.py — Pydantic v2 BaseSettings with TOML source:

Use pydantic*settings.TomlConfigSettingsSource (available in pydantic-settings >= 2.3)
Env prefix TSBENCH* with env_nested_delimiter="**" (e.g. TSBENCH_DATABASES**QUESTDB\_\_HOST)
Module-level get_settings() singleton
Nested models: QuestDBConfig, CrateDBConfig, TimescaleDBConfig, WorkloadConfig, ResultsConfig, MonitorConfig
TimescaleDBConfig.pg_port defaults to 5433 (Docker host port, avoids CrateDB conflict)
WorkloadConfig adds mixed_duration_seconds: int = 60 and prng_seed: int = 42
schema.py — universal data contracts:

class ColumnRole(Enum): TIMESTAMP | TAG | METRIC

@dataclass(frozen=True)
class ColumnSpec:
name: str; role: ColumnRole; arrow_type: pa.DataType; nullable: bool = False

@dataclass
class DatasetSchema:
name: str; timestamp_col: str; tag_cols: list[str]
metric_cols: list[str]; columns: list[ColumnSpec]; row_count: int = 0

@dataclass
class BenchmarkDataset:
schema: DatasetSchema
table: pa.Table | None # None for lazy/large datasets
source_path: Path | None = None
Phase 2 — Data Layer
Creates: datasets/timestamps.py, datasets/loader.py, datasets/generator.py, datasets/small_iot.csv, datasets/dataset_manifest.toml

timestamps.py — normalization cascade (all return pa.timestamp[ns, UTC]):

Already pa.timestamp type → cast to timestamp[ns, UTC]
Integer: > 1e18 → nanoseconds; > 1e9 → seconds; else raise
Float → interpret as Unix seconds, convert to ns
String → pd.to_datetime(utc=True) → fallback to dateutil.parser.parse()
loader.py — accepts .csv, .parquet/.parq, .json/.jsonl/.ndjson:

Use PyArrow native readers (pa_csv.read_csv, pq.read_table, pa_json.read_json)
Auto-infer schema via \_infer_schema(): strings with cardinality < 5% of rows → TAG; numeric → METRIC; timestamp → TIMESTAMP
Support schema_hint: DatasetSchema | None parameter
generator.py — deterministic IoT data:

numpy.random.default*rng(seed) (PCG64, platform-reproducible)
Generates: device_id (f"device*{i:06d}"), timestamp (regular interval), temperature (Normal), humidity (Beta×100), voltage (Normal), pressure (Normal)
lazy=True parameter writes to temp Parquet, returns BenchmarkDataset(table=None, source_path=...)
small_iot.csv — 10,000 rows (100 devices × 100 readings), 2024-01-01T00:00:00Z to 2024-01-02T00:00:00Z, seed=42

Phase 3 — Metrics Layer
Creates: metrics/timer.py, metrics/stats.py, metrics/monitor.py

timer.py:

@contextmanager
def timed_operation(rows=0, bytes_count=0, disable_gc=True): # gc.collect() → gc.disable() → perf_counter_ns() start → yield → stop → gc.enable() # disable_gc=False for mixed/long-running benchmarks
stats.py — make LatencyStats a Pydantic BaseModel (not dataclass) for direct SQLite/Parquet serialization:

HdrHistogram(1, 60_000_000_000, 3) — 1ns to 60s, 3 significant figures
Record in nanoseconds; report in milliseconds (÷ 1,000,000)
IQR computed from raw values via numpy.percentile([25, 75]) (not from histogram)
Outlier detection: 1.5×IQR fence method
monitor.py — ResourceMonitor daemon thread:

System-level (not per-process — DBs run in Docker, psutil can't see container internals)
psutil.cpu_percent(interval=None) + virtual_memory().rss + disk/net IO deltas
deque(maxlen=10_000) — bounded buffer, threading.Lock around reads
start() / stop() → list[ResourceSample]
Phase 4 — Adapters Layer
Creates: adapters/base.py, adapters/type_maps.py, adapters/questdb.py, adapters/cratedb.py, adapters/timescaledb.py, adapters/registry.py

base.py — @runtime_checkable Protocol (structural typing, no inheritance required):

class DatabaseAdapter(Protocol):
name: str
async def connect(self) -> None
async def disconnect(self) -> None
async def create_table(self, schema: DatasetSchema) -> None
async def drop_table(self, table_name: str) -> None
async def table_exists(self, table_name: str) -> bool
async def ingest_batch(self, table: pa.Table, table_name: str) -> TimingResult
async def execute_query(self, sql: str, params: tuple = ()) -> tuple[list[dict], TimingResult]
async def get_row_count(self, table_name: str) -> int
async def flush(self) -> None # ensure data visible (CrateDB REFRESH, QuestDB no-op)
async def health_check(self) -> bool
async def get_version(self) -> str
type_maps.py — pure data, no logic:

PyArrow QuestDB CrateDB TimescaleDB
int64 LONG BIGINT BIGINT
float64 DOUBLE DOUBLE PRECISION DOUBLE PRECISION
string (TAG role) SYMBOL TEXT TEXT
string (METRIC role) STRING TEXT TEXT
timestamp[ns] TIMESTAMP TIMESTAMP WITH TIME ZONE TIMESTAMPTZ
bool BOOLEAN BOOLEAN BOOLEAN
Also exports arrow_table_to_ddl(schema, db, table_name) -> str

questdb.py — QuestDB adapter:

Ingest: questdb.ingress.Sender.from_conf("http::addr=host:9000;") with sender.dataframe(df, table_name=..., symbols=[tag_cols], at=ts_col) then sender.flush() — timing wraps both calls
Query: asyncpg.connect(host=..., port=8812, database="qdb", user="admin", password="quest")
Note: asyncpg prepared statement caching may fail; test with statement_cache_size=0 if needed
flush(): no-op (ILP sender flush is synchronous inside context manager)
create_table(): DDL via asyncpg PGWire (no hypertable step needed)
cratedb.py — CrateDB adapter:

Ingest: UNNEST pattern via asyncpg (PostgreSQL wire, port 5432, user="crate", db="doc"):

INSERT INTO t (ts, device_id, temp)
SELECT \* FROM UNNEST($1::TIMESTAMP WITH TIME ZONE[], $2::TEXT[], $3::DOUBLE PRECISION[])
Pass Python lists as asyncpg params. Required: asyncpg.connect(..., statement_cache_size=0) for CrateDB
flush(): REFRESH TABLE table_name — CrateDB is eventually consistent (1s default refresh)
create_table(): Add PARTITIONED BY (timestamp_col) clause for time-series optimization
timescaledb.py — TimescaleDB adapter:

Ingest: asyncpg.copy_records_to_table(table_name, records=list_of_tuples, columns=[...]) — timing wraps this call; datetime values must have tzinfo=timezone.utc
Query: standard asyncpg on port 5433 (Docker host mapping)
create_table(): DDL first, then SELECT create_hypertable($1, $2, if_not_exists => TRUE)
flush(): no-op (COPY committed immediately in autocommit)
registry.py — lazy import with try/except for optional deps:

def get_enabled_adapters() -> list[DatabaseAdapter]: # For each enabled DB in settings, try import; warn and skip on ImportError
Phase 5 — Results Layer
Creates: results/models.py, results/storage.py, results/export.py, environment/capture.py

models.py — Pydantic v2 BaseModels: RunConfig, OperationResult, BenchmarkSummary

LatencyStats imported from metrics/stats.py (already a BaseModel)
model_config = ConfigDict(arbitrary_types_allowed=True) where needed
storage.py — dual storage:

SQLite: runs table + summaries table with p50/p95/p99/p999/mean/min/max/IQR/throughput columns
Parquet: per-operation timings at parquet_dir/{run_id}/{database}/{operation}.parquet, snappy compression, row_group_size=50_000
ResultStorage.initialize() creates dirs and runs DDL
export.py — CSV, JSON, Markdown comparison table export

environment/capture.py — captures: Python version, platform, CPU count, total RAM, git commit (subprocess), installed package versions, DB versions (populated per-adapter after connect)

Phase 6 — Benchmarks Layer
Creates: benchmarks/base.py, benchmarks/ingestion.py, benchmarks/queries.py, benchmarks/mixed.py, benchmarks/registry.py

base.py — BenchmarkWorkload Protocol:

class BenchmarkWorkload(Protocol):
name: str; description: str
async def run(self, adapter, dataset, config, run_id) -> BenchmarkResult
def get_queries(self, schema: DatasetSchema, adapter_name: str) -> list[tuple[str, str]]
ingestion.py — IngestionBenchmark:

Loops over config.batch_sizes × config.workers
Multi-worker: asyncio.gather() with sub-batches; outer timing wraps gather() call
Phase tracking: "warmup" (not saved to summaries) vs "measurement"
reset_between_rounds: bool config (default False = append mode; warmup always resets)
Also implements OutOfOrderIngestionBenchmark (shuffle timestamps before insert)
queries.py — DB-specific SQL templates:

Benchmark QuestDB CrateDB TimescaleDB
Aggregation SAMPLE BY 1h DATE_BIN('1 hour', ts, ...) time_bucket('1 hour', ts)
Last-point LATEST ON ts PARTITION BY device_id DISTINCT ON(device_id) ... ORDER BY device_id, ts DESC DISTINCT ON(device_id) ... ORDER BY device_id, ts DESC
Time-range WHERE ts >= $1 AND ts < $2 (same all DBs) same same
Random start times for time-range queries use default_rng(config.prng_seed) for determinism.

mixed.py — MixedBenchmark:

asyncio.Queue(maxsize=10) producer/consumer architecture
Writer coroutine + N reader coroutines run for config.mixed_duration_seconds
GC must NOT be disabled (disable_gc=False in timed_operation)
Seven workloads registered: ingestion, time_range_scan, aggregation, last_point, high_cardinality, downsampling, mixed

Phase 7 — CLI Layer
Creates: cli/main.py, cli/run.py, cli/compare.py, cli/dashboard.py

Commands:

tsbench run --db questdb --benchmark ingestion --config benchmark.toml
tsbench generate --devices 100 --readings 1000 --seed 42 --output datasets/out.parquet
tsbench load --db timescaledb --dataset datasets/small_iot.csv
tsbench compare <run_id1> <run_id2> --operation ingestion --format table
tsbench dashboard --port 8501 --results-db results/runs.db
run.py orchestration:

Load config → capture environment → load dataset
Initialize adapters (await adapter.connect())
Per DB × benchmark: start ResourceMonitor → run benchmark → stop monitor → save results
Wrap each DB in try/except; log failures and continue
Print Rich summary table (p50 / p99 / rows/sec per DB per operation)
Progress display: Rich Progress with SpinnerColumn, BarColumn, TextColumn, TimeRemainingColumn

dashboard.py: launches Streamlit as subprocess: python -m streamlit run dashboard/app.py -- results/runs.db

Phase 8 — Dashboard
Creates: dashboard/charts.py, dashboard/app.py

charts.py — pure Plotly, no Streamlit imports:

make_comparison_bar(df, metric) — px.bar(barmode='group')
make_latency_box(df) — px.box() with percentile add_vline annotations
make_throughput_line(df) — px.line() rows/sec over iterations
make_resource_heatmap(df) — CPU% and RSS over time
app.py — Streamlit:

sys.argv[1] → results DB path (passed by CLI launcher)
Sidebar: st.multiselect for run IDs
Two-column layout with bar charts; full-width latency box + resource plots
Phase 9 — Tests
Creates: tests/conftest.py, tests/unit/, tests/integration/

Unit test priorities:

test_timestamps.py — all 4 cascade paths
test_stats.py — HDR percentiles on known data; IQR outlier detection
test_generator.py — PRNG determinism (same seed → same output)
test_type_maps.py — all type conversions and DDL generation
test_schema.py — auto-detection on synthetic data
Integration tests: marked with @pytest.mark.integration, require running Docker containers

Phase 10 — Docker Infrastructure
Creates: docker/docker-compose.yml, docker/docker-compose.test.yml

Port mappings:

QuestDB: 9000:9000 (ILP HTTP + Web Console), 8812:8812 (PGWire)
CrateDB: 4200:4200 (HTTP), 5432:5432 (PGWire)
TimescaleDB: 5433:5432 (host 5433 to avoid CrateDB conflict)
Critical Implementation Notes
LatencyStats must be a Pydantic BaseModel (not dataclass) — it's serialized to both SQLite summaries and Parquet, so Pydantic .model_dump() is needed throughout.

Async throughout — all adapter methods are async; CLI uses asyncio.run(\_async_main(...)). Never call asyncio.run() inside an already-running event loop (use await in tests with pytest-asyncio).

CrateDB asyncpg — always connect with statement_cache_size=0; always call REFRESH TABLE after inserts; default database is "doc" not "postgres".

QuestDB SQL limitations — avoid $1 parameterized queries for some QuestDB query types; for predefined benchmark queries use f-strings with sanitized/trusted values only.

Table naming — use {dataset_name} (e.g. small_iot) with full DROP/CREATE at the start of each run to avoid collisions.

GC handling — disable_gc=True only for single-operation micro-benchmarks; False for MixedBenchmark and ingestion of large datasets.

Verification / Testing
Unit tests: pip install -e ".[dev]" → pytest tests/unit/ (no Docker needed)
Integration tests: docker compose -f docker/docker-compose.test.yml up -d → pytest tests/integration/ -m integration
End-to-end smoke test:

tsbench generate --devices 10 --readings 100 --seed 42 --output datasets/smoke.parquet
tsbench run --db questdb --benchmark ingestion --config benchmark.toml
tsbench compare <run_id> --format table
tsbench dashboard
Reproducibility check: run same config twice with same seed → verify p50/p99 latencies are within 5% variance
Dashboard validation: open http://localhost:8501 → verify charts render for all three databases
File Dependency Summary
Phase Critical Files Depends On
0 pyproject.toml, exceptions.py —
1 config/settings.py, datasets/schema.py Phase 0
2 datasets/loader.py, datasets/generator.py Phase 1
3 metrics/timer.py, metrics/stats.py, metrics/monitor.py Phase 1
4 adapters/base.py, adapters/_.py Phases 1–3
5 results/models.py, results/storage.py, environment/capture.py Phases 1, 3
6 benchmarks/_.py Phases 1–5
7 cli/_.py Phases 1–6
8 dashboard/_.py Phases 1, 5
9 tests/ Phases 1–8
10 docker/ —

# tsbm — Time-Series Database Benchmark

Benchmark **QuestDB**, **CrateDB**, and **TimescaleDB** on real-world time-series workloads: ingestion throughput, range queries, aggregations, materialized view performance vs. raw queries, and late-arrival MV recompute cost.

Built for datasets from thousands to **tens of millions of rows** — large files stream in configurable chunks to avoid OOM issues.

---

## Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start — 11M+ Row Parquet File on QuestDB & CrateDB](#quick-start--11m-row-parquet-file-on-questdb--cratedb)
- [Available Benchmarks](#available-benchmarks)
- [Configuration Reference](#configuration-reference)
- [CLI Reference](#cli-reference)
- [Results & Comparison](#results--comparison)
- [Development](#development)

---

## Prerequisites

| Requirement             | Version                  |
| ----------------------- | ------------------------ |
| Python                  | 3.10+                    |
| Docker + Docker Compose | any recent version       |
| Disk space              | ~3 GB for Docker volumes |

---

## Installation

```bash
# Clone the repo
git clone https://github.com/IssamElNass/tsbm
cd tsbm

# Install with all database drivers
pip install -e ".[questdb,cratedb,timescaledb]"

# Optional: dashboard (Streamlit + Plotly)
pip install -e ".[questdb,cratedb,timescaledb,dashboard]"

# Verify
tsbm --help
```

---

## Quick Start — 11M+ Row Parquet File on QuestDB & CrateDB

### 1. Start the databases

```bash
docker compose -f docker/docker-compose.yml up -d questdb cratedb
```

Wait ~20 s for health checks to pass:

```bash
docker compose -f docker/docker-compose.yml ps
```

Port summary:

| Database | UI / Console          | PGWire |
| -------- | --------------------- | ------ |
| QuestDB  | http://localhost:9000 | 8812   |
| CrateDB  | http://localhost:4200 | 5432   |

### 2. Configure your Parquet file

Edit `benchmark.toml` — point `dataset` at your file and tune chunk size for your RAM:

```toml
[workload]
dataset   = "path/to/your_data.parquet"   # ← your 11M+ row file
chunk_size = 200000                        # rows per ingestion batch (default 100k)
streaming_threshold_rows = 500000          # auto-enable streaming above this count

# MV benchmark settings
mv_granularity          = "1 hour"
late_arrival_rounds     = 10
late_arrival_batch_size = 200
```

> **Streaming is automatic.** When the row count exceeds `streaming_threshold_rows`
> (default 500 000), tsbm reads and ingests the file in `chunk_size`-row batches
> — no full load into RAM.

### 3. Run benchmarks

**Ingestion throughput** (both databases, streams 11M rows in chunks):

```bash
tsbm run --db questdb --db cratedb --benchmark ingestion
```

**Aggregation queries** (avg/min/max over time windows):

```bash
tsbm run --db questdb --db cratedb --benchmark aggregation
```

**Materialized view vs. raw query** — QuestDB uses native `SAMPLE BY` MVs,
CrateDB simulates with a summary table:

```bash
tsbm run --db questdb --db cratedb --benchmark materialized_view
```

**Late-arrival MV recompute** — insert out-of-order data, measure refresh cost:

```bash
tsbm run --db questdb --db cratedb --benchmark late_arrival
```

**Compare two runs side-by-side:**

```bash
# The run IDs are printed at the end of each `tsbm run` call
tsbm compare <run_id_questdb> <run_id_cratedb> --format table
```

### 4. (Optional) Dashboard

```bash
tsbm dashboard           # opens http://localhost:8501
```

---

## Available Benchmarks

| Name                     | Description                                                                |
| ------------------------ | -------------------------------------------------------------------------- |
| `ingestion`              | Bulk write throughput across batch sizes and worker counts                 |
| `ingestion_out_of_order` | Write throughput with timestamps intentionally out of order                |
| `time_range`             | Point-in-time range SELECT latency at 1 min / 1 h / 1 day / 1 week windows |
| `aggregation`            | avg/min/max GROUP BY time-bucket across varying granularities              |
| `last_point`             | Latest value per device/tag (common IoT query pattern)                     |
| `high_cardinality`       | Query performance with 10 000+ unique tag values                           |
| `downsampling`           | Query a coarser-grain view derived from the raw table                      |
| `mixed`                  | Concurrent read + write workload for a configurable duration               |
| `materialized_view`      | Raw aggregation latency vs. query against a pre-built MV                   |
| `late_arrival`           | Insert late-arriving rows → incremental MV refresh → query MV              |

### Materialized view support per database

| Database    | MV type                   | Create                                                     | Refresh                                                          |
| ----------- | ------------------------- | ---------------------------------------------------------- | ---------------------------------------------------------------- |
| QuestDB     | Native MV (`SAMPLE BY`)   | `CREATE MATERIALIZED VIEW … WITH BASE … REFRESH IMMEDIATE` | `REFRESH MATERIALIZED VIEW` / `REFRESH MATERIALIZED VIEW … FULL` |
| CrateDB     | Simulated (summary table) | `CREATE TABLE` + `INSERT … SELECT DATE_BIN(…) GROUP BY`    | `DELETE` + `INSERT` + `REFRESH TABLE`                            |
| TimescaleDB | Continuous aggregate      | `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)` | `CALL refresh_continuous_aggregate(…)`                           |

---

## Configuration Reference

`benchmark.toml` is loaded from the current working directory. Every key can be
overridden via an environment variable prefixed with `TSBM_` and using `__` as
the nested separator (e.g. `TSBM_DATABASES__QUESTDB__HOST=my-host`).

```toml
[databases.questdb]
enabled  = true
host     = "localhost"
ilp_port = 9000        # ILP over HTTP (ingestion)
pg_port  = 8812        # PGWire (queries)

[databases.cratedb]
enabled   = true
host      = "localhost"
http_port = 4200
pg_port   = 5432

[databases.timescaledb]
enabled  = true
host     = "localhost"
pg_port  = 5433        # Docker maps host:5433 → container:5432
user     = "postgres"
password = "postgres"
dbname   = "tsbm"

[workload]
dataset               = "datasets/small_iot.csv"   # path to Parquet / CSV / JSON
batch_sizes           = [1000, 5000, 10000, 50000]  # rows per INSERT batch
workers               = [1, 4, 8, 16]               # concurrent writer threads
warmup_iterations     = 5
measurement_rounds    = 30
time_windows          = ["1min", "1h", "1day", "1week"]
prng_seed             = 42
mixed_duration_seconds = 60
streaming_threshold_rows = 500000   # enable streaming above this row count
chunk_size               = 100000   # rows per streaming batch
mv_granularity           = "1 hour" # time-bucket width for MV benchmarks
late_arrival_rounds      = 10
late_arrival_batch_size  = 100

# Optional: convert metric columns before ingestion
# unit_conversions = { "temperature" = ["celsius", "fahrenheit"] }

[results]
sqlite_path = "results/runs.db"
parquet_dir = "results/parquet"

[monitor]
enabled     = true
interval_ms = 500   # CPU/RSS sampling interval
```

### Supported input formats

| Extension          | Notes                                                     |
| ------------------ | --------------------------------------------------------- |
| `.parquet`         | Preferred; fastest schema inference and streaming         |
| `.csv`             | Auto-detected delimiter; schema inferred from first batch |
| `.json` / `.jsonl` | Line-delimited JSON; slow for large files                 |

---

## CLI Reference

```
tsbm run       --db <name> [--db <name>]  # one or more of: questdb cratedb timescaledb
               --benchmark <name>          # default: ingestion
               --dataset <path>            # override dataset from config
               --config <path>             # override config file (default: ./benchmark.toml)
               --verbose

tsbm generate  --devices <n>    # unique devices/tags  (default 100)
               --readings <n>   # readings per device  (default 1000)
               --seed <n>
               --output <path>  # .parquet or .csv

tsbm load      --db <name>      # load dataset into DB without benchmarking
               --dataset <path>
               --drop / --no-drop

tsbm compare   <run_id> [<run_id> ...]
               --metric latency_p99_ms     # column to highlight
               --format table | csv | json | markdown
               --output <path>

tsbm dashboard --port 8501
               --results-db <path>
```

---

## Results & Comparison

Results are stored in a local SQLite database (`results/runs.db`) and also
written as Parquet files to `results/parquet/`. Each `tsbm run` prints the
run ID at the end — use it with `tsbm compare`.

```bash
# Example: compare ingestion runs for questdb vs cratedb
tsbm compare abc123 def456 --format markdown

# Export to CSV for further analysis
tsbm compare abc123 def456 --format csv --output comparison.csv
```

---

## Development

### Run unit tests (no Docker required)

```bash
pip install -e ".[questdb,cratedb,timescaledb,dev]"
pytest tests/unit/ -v
```

### Run integration tests (Docker required)

```bash
docker compose -f docker/docker-compose.yml up -d
pytest tests/integration/ -m integration -v
```

### Generate a synthetic dataset

```bash
# 10 000 devices × 1 100 readings = ~11M rows
tsbm generate --devices 10000 --readings 1100 --seed 42 \
              --output datasets/large.parquet

tsbm run --db questdb --db cratedb --benchmark ingestion \
         --dataset datasets/large.parquet
```

---

## Architecture

```
src/tsbm/
├── adapters/          # QuestDB, CrateDB, TimescaleDB drivers
│   ├── base.py        # DatabaseAdapter + MVCapableAdapter protocols
│   ├── questdb.py     # ILP HTTP ingestion + PGWire queries + native MVs
│   ├── cratedb.py     # UNNEST bulk INSERT + simulated MV (summary table)
│   └── timescaledb.py # COPY ingestion + continuous aggregates
├── benchmarks/        # Workload implementations
│   ├── ingestion.py
│   ├── queries.py
│   ├── materialized_views.py   # MaterializedViewBenchmark + LateArrivalBenchmark
│   └── registry.py
├── datasets/
│   ├── schema.py      # DatasetSchema, BenchmarkDataset, UnitConverter
│   ├── loader.py      # load_dataset(), load_dataset_streaming()
│   └── generator.py   # Synthetic IoT data generator
├── config/
│   └── settings.py    # Pydantic-settings (TOML + env vars, prefix TSBM_)
├── metrics/
│   ├── timer.py       # TimingResult, timed_operation(), HDR histograms
│   └── monitor.py     # Background CPU/RSS sampling
├── results/
│   ├── models.py      # RunConfig, OperationResult, BenchmarkSummary
│   └── storage.py     # SQLite + Parquet persistence
└── cli/               # Typer CLI (run, generate, load, compare, dashboard)
```

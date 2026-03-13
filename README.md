# tsbm — Time-Series Database Benchmark

Benchmark **QuestDB**, **CrateDB**, and **TimescaleDB** on real-world time-series workloads: ingestion throughput, range queries, aggregations, materialized view performance, and late-arrival recompute cost.

Handles datasets from **100 K to 100 M+ rows**. Large files are read and ingested in streaming chunks — no OOM crashes.

---

## Contents

- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Dataset Guide](#dataset-guide)
  - [Generating a Synthetic Dataset](#generating-a-synthetic-dataset)
  - [Bring Your Own File](#bring-your-own-file)
  - [Dataset Schema / Manifest](#dataset-schema--manifest)
- [Available Benchmarks](#available-benchmarks)
  - [Benchmark Descriptions](#benchmark-descriptions)
  - [Materialized View Support](#materialized-view-support-per-database)
- [Configuration Reference](#configuration-reference)
  - [\[databases.questdb\]](#databasesquestdb)
  - [\[databases.cratedb\]](#databasescratedb)
  - [\[databases.timescaledb\]](#databasestimescaledb)
  - [\[workload\]](#workload)
  - [\[results\]](#results)
  - [\[monitor\]](#monitor)
  - [Environment Variable Overrides](#environment-variable-overrides)
- [CLI Reference](#cli-reference)
  - [tsbm seed](#tsbm-seed)
- [Results & Comparison](#results--comparison)
- [Architecture](#architecture)
- [Development](#development)

---

## Prerequisites

| Requirement             | Version            |
| ----------------------- | ------------------ |
| Python                  | 3.10+              |
| Docker + Docker Compose | any recent version |
| Disk space              | ~3 GB for volumes  |

---

## Installation

```bash
# Clone
git clone https://github.com/IssamElNass/tsbm
cd tsbm

# Install with all database drivers
pip install -e ".[questdb,cratedb,timescaledb]"

# Optional: interactive dashboard (Streamlit + Plotly)
pip install -e ".[questdb,cratedb,timescaledb,dashboard]"

# Verify
tsbm --help
```

---

## Quick Start

### 1. Start the databases

```bash
docker compose -f docker/docker-compose.yml up -d
```

Wait ~20 s, then verify:

```bash
docker compose -f docker/docker-compose.yml ps
```

Port summary:

| Database    | UI / Console          | PGWire |
| ----------- | --------------------- | ------ |
| QuestDB     | http://localhost:9000 | 8812   |
| CrateDB     | http://localhost:4200 | 5432   |
| TimescaleDB | —                     | 5433   |

### 2. Pick a dataset size

| Scale  | Devices | Readings/device | Total rows | Generate command                                                                |
| ------ | ------- | --------------- | ---------- | ------------------------------------------------------------------------------- |
| Small  | 100     | 1 000           | ~100 K     | `tsbm generate --devices 100 --readings 1000 --output datasets/small.parquet`   |
| Medium | 5 000   | 2 200           | ~11 M      | `tsbm generate --devices 5000 --readings 2200 --output datasets/11m.parquet`    |
| Large  | 10 000  | 10 000          | ~100 M     | `tsbm generate --devices 10000 --readings 10000 --output datasets/100m.parquet` |

Update `benchmark.toml` to point at your file:

```toml
[workload]
dataset = "datasets/11m.parquet"
```

### 3. Run benchmarks

**Single benchmark:**

```bash
tsbm run --benchmark ingestion
tsbm run --benchmark aggregation --db questdb --db timescaledb
```

**All benchmarks in one command:**

```bash
tsbm run --benchmark all
tsbm run --benchmark all --dataset datasets/11m.parquet
```

Each benchmark prints its own results table with a verdict column and an interpretation summary as it completes.

### 4. Generate a report

Generate a single Markdown file with all results and the exact SQL queries run against each database. Useful for sharing or comparing against another database outside of tsbm:

```bash
tsbm report                            # auto-discovers latest runs, writes results/report.md
tsbm report --output comparison.md     # custom output path
tsbm report --benchmark aggregation    # single benchmark only
tsbm report abc123 def456              # specific run IDs
```

The report contains:
- Side-by-side database comparison tables for every metric (Rows/sec, p50/p95/p99 latency)
- The exact SQL executed per benchmark per database (with actual column names)

### 5. Compare specific runs

```bash
# Run IDs are printed at the end of every tsbm run call
tsbm compare <run_id_1> <run_id_2> --format table
tsbm compare <run_id_1> <run_id_2> --format markdown --output diff.md
```

### 6. (Optional) Dashboard

```bash
tsbm dashboard   # opens http://localhost:8501
```

---

## Dataset Guide

### Generating a Synthetic Dataset

`tsbm generate` creates a synthetic IoT dataset of N devices × M readings, writing to Parquet or CSV.

```bash
tsbm generate \
  --devices  5000 \       # unique device IDs / tag values
  --readings 2200 \       # readings per device
  --seed     42   \       # PRNG seed for reproducibility
  --output   datasets/11m.parquet
```

The output schema is:

| Column        | Type      | Role      | Notes                          |
| ------------- | --------- | --------- | ------------------------------ |
| `ts`          | timestamp | timestamp | Microsecond-precision, UTC     |
| `device_id`   | string    | tag       | `"device_0001"` … `"device_N"` |
| `temperature` | float64   | metric    | °C, normally distributed       |
| `humidity`    | float64   | metric    | %, clamped 0–100               |
| `pressure`    | float64   | metric    | hPa, realistic range           |
| `battery`     | float64   | metric    | Voltage, 3.0–4.2 V             |

### Bring Your Own File

tsbm accepts any file that has at least one timestamp column and one numeric column. Supported formats:

| Extension          | Notes                                                         |
| ------------------ | ------------------------------------------------------------- |
| `.parquet`         | Preferred — fastest schema inference and streaming            |
| `.csv`             | Delimiter auto-detected; schema inferred from the first batch |
| `.json` / `.jsonl` | Line-delimited JSON; slow for large files                     |

### Azure Blob Storage

tsbm can read Parquet files directly from Azure Blob Storage — no need to download them first. Install the Azure extra:

```bash
pip install tsbm[azure]
```

Point `datasets` at one or more Azure URLs:

```toml
[workload]
datasets = [
    "az://myblobcontainer/",
    "az://myblobcontainer/data/sensors/",
    "https://myaccount.blob.core.windows.net/data/sensors_2025.parquet",
]
```

Supported URL schemes: `az://`, `abfs://`, `abfss://`, and `https://<account>.blob.core.windows.net/...`.
Container/folder URLs are auto-expanded — all `.parquet` files under the prefix are discovered.

**Authentication** (pick one — listed by priority):

| Setting                            | When to use                                    |
| ---------------------------------- | ---------------------------------------------- |
| `azure_storage_connection_string`  | Full connection string from the Azure portal   |
| `azure_storage_sas_token`          | Shared Access Signature token                  |
| _(none)_                           | Falls back to `DefaultAzureCredential` (auto)  |

When using a SAS token, `account_name` is required by the underlying library. It is **auto-detected** from `https://<account>.blob.core.windows.net/` URLs. For `az://` scheme URLs, set it explicitly:

```toml
[workload]
azure_storage_sas_token    = "sv=2022-11-02&ss=b&srt=co&sp=rl..."
azure_storage_account_name = "myaccount"   # required with az:// URLs + SAS token
```

All three settings can also be set via environment variables: `TSBM_WORKLOAD__AZURE_STORAGE_CONNECTION_STRING`, `TSBM_WORKLOAD__AZURE_STORAGE_SAS_TOKEN`, `TSBM_WORKLOAD__AZURE_STORAGE_ACCOUNT_NAME`.

### Dataset Schema / Manifest

When tsbm loads a file it builds a `DatasetSchema` — an internal manifest that drives all SQL generation and metric reporting. You never write this by hand; it is inferred automatically.

| Field           | Type             | Description                                                                              |
| --------------- | ---------------- | ---------------------------------------------------------------------------------------- |
| `name`          | `str`            | Table name used in the database, derived from the filename stem                          |
| `timestamp_col` | `str`            | The single timestamp column (earliest `TIMESTAMP`/`DATETIME` column found)               |
| `tag_cols`      | `list[str]`      | Low-cardinality string columns (fraction of unique values ≤ `tag_cardinality_threshold`) |
| `metric_cols`   | `list[str]`      | Numeric columns that are not the timestamp                                               |
| `row_count`     | `int`            | Total row count as reported by the file's metadata                                       |
| `column_types`  | `dict[str, str]` | Arrow dtype per column, used to generate CREATE TABLE DDL for each database              |

The `tag_cardinality_threshold` workload setting (default `0.05`) controls the boundary: a string column whose number of unique values is ≤ 5 % of total rows is treated as a tag; above that threshold it is treated as a regular `TEXT` metric column.

**Unit conversions** — you can ask tsbm to convert metric columns before ingestion:

```toml
[workload]
# Converts the "temperature" column from Celsius to Fahrenheit before inserting.
unit_conversions = { "temperature" = ["celsius", "fahrenheit"] }
```

---

## Available Benchmarks

Run `tsbm run --benchmark <name>` for any individual benchmark, or `--benchmark all` to run every one in sequence.

### Benchmark Descriptions

| Name                     | Measures                                                     | Key metric     | Good result (11M rows)              |
| ------------------------ | ------------------------------------------------------------ | -------------- | ----------------------------------- |
| `ingestion`              | Raw write throughput across batch sizes and worker counts    | Rows/sec       | > 500 K rows/sec                    |
| `ingestion_out_of_order` | Write throughput with shuffled (out-of-order) timestamps     | Rows/sec       | Close to `ingestion` baseline       |
| `time_range`             | Single-device window scan with `WHERE ts >= x AND ts < y`    | p99 latency    | < 10 ms for a 1-hour window         |
| `aggregation`            | `avg/min/max` grouped by time-bucket (hourly)                | p99 latency    | < 50 ms                             |
| `last_point`             | Most recent row per device/tag (`LATEST ON` / `DISTINCT ON`) | p99 latency    | < 20 ms                             |
| `high_cardinality`       | `GROUP BY` on a tag with 10 K+ unique values                 | p99 latency    | < 200 ms                            |
| `downsampling`           | Multi-resolution aggregation (1 min, 1 h, 1 day, 1 week)     | p99 latency    | < 100 ms per window                 |
| `mixed`                  | Concurrent reads + writes for `mixed_duration_seconds`       | Rows/sec + p99 | No more than 2× degradation vs solo |
| `materialized_view`      | Raw aggregation latency vs. pre-computed MV query latency    | p99 latency    | MV ≥ 5× faster than raw             |
| `late_arrival`           | MV recompute cost after inserting out-of-order rows          | p99 latency    | < 500 ms per refresh                |

#### `ingestion`

Loops over every combination of `batch_sizes × workers`. For each combo:

1. **Warmup** — drops and recreates the table `warmup_iterations` times (results discarded).
2. _(Optional)_ **Pre-seed** — if `ingestion_seed_rows > 0`, inserts that many rows so measurement rounds append into a pre-populated table.
3. **Measurement** — runs `measurement_rounds` timed insertions. If `reset_between_rounds = true` the table is recreated before each round (clean-slate isolation); otherwise rows accumulate.

Reports `p50 ms`, `p99 ms`, and `Rows/sec` per (batch_size, workers) combination.

#### `ingestion_out_of_order`

Identical to `ingestion` but each batch's timestamp column is randomly permuted before insertion. The delta vs. `ingestion` shows how much the database's out-of-order handling costs (sorting, re-partitioning, WAL reorder).

#### `time_range`

Generates `warmup_iterations + measurement_rounds` random non-overlapping time windows from the dataset's own timestamp range. Runs `SELECT … WHERE ts >= $1 AND ts < $2` for each window. The window duration is taken from `time_windows[0]`.

> **Seed-once behaviour** — When running `--benchmark all`, the dataset is loaded into the database once, and reused by all benchmarks that need pre-existing data. The rules are:
> - **Query benchmarks** (`aggregation`, `downsampling`, `high_cardinality`, `last_point`, `time_range`): seed once, reuse across all consecutive runs.
> - **`mixed`**: inherits the seeded table if one already exists (readers see realistic data), or seeds on first run.
> - **`materialized_view` / `late_arrival`**: always drop + recreate + reseed to avoid stale view state from a previous run.
> - **`ingestion` / `ingestion_out_of_order`**: always start fresh — they manage their own warmup/measurement data internally. Use `ingestion_seed_rows` to pre-populate before measurement rounds.

#### `aggregation`

Same window generation strategy, but runs a time-bucket `GROUP BY` query computing `avg`, `min`, and `max` of all metric columns per bucket.

- QuestDB uses `SAMPLE BY`.
- CrateDB and TimescaleDB use `DATE_BIN` / `time_bucket`.

#### `last_point`

Fetches the single most recent row per unique tag value.

- QuestDB: `LATEST ON ts PARTITION BY device_id`
- CrateDB / TimescaleDB: `DISTINCT ON (device_id) ORDER BY ts DESC`

#### `high_cardinality`

A `GROUP BY device_id` aggregation over a random time window, testing scalability as the number of unique tags grows.

#### `downsampling`

Runs aggregation queries at all window sizes in `time_windows` (e.g. 1 min, 1 h, 1 day, 1 week). Simulates a data historian that reads at progressively coarser granularities.

#### `mixed`

Spawns concurrent read and write tasks for `mixed_duration_seconds` seconds. The write side uses the first entry in `batch_sizes` and `workers`. The read side runs random `time_range` queries. Measures steady-state throughput and latency under mixed pressure.

The table is pre-seeded with the full dataset before the benchmark starts, so read queries operate on realistic data volumes from the first second. When run as part of `--benchmark all`, the existing seeded table is reused if query benchmarks already ran.

#### `materialized_view`

1. Builds the MV / continuous aggregate (`CREATE MATERIALIZED VIEW`).
2. Runs the same aggregation query twice: once against the raw table, once against the MV.
3. Reports latency for both and the speedup ratio.

#### `late_arrival`

1. Builds the MV, verifies it is populated.
2. Inserts `late_arrival_rounds × late_arrival_batch_size` rows with timestamps set in the past (out of the MV's current range).
3. Measures the time for each MV refresh triggered by the late rows.

### Materialized View Support Per Database

| Database    | MV type                   | Create statement                                                        | Refresh                                           |
| ----------- | ------------------------- | ----------------------------------------------------------------------- | ------------------------------------------------- |
| QuestDB     | Native MV (`SAMPLE BY`)   | `CREATE MATERIALIZED VIEW … WITH BASE … REFRESH IMMEDIATE SAMPLE BY 1h` | Polled via `materialized_views()` system function |
| CrateDB     | Simulated (summary table) | `CREATE TABLE summary` + `INSERT … SELECT DATE_BIN(…) GROUP BY`         | `DELETE` + `INSERT` + `REFRESH TABLE`             |
| TimescaleDB | Continuous aggregate      | `CREATE MATERIALIZED VIEW … WITH (timescaledb.continuous)`              | `CALL refresh_continuous_aggregate(…)`            |

---

## Configuration Reference

`benchmark.toml` is read from the current working directory. All keys can be overridden via environment variables (see [Environment Variable Overrides](#environment-variable-overrides)).

---

### `[databases.questdb]`

```toml
[databases.questdb]
enabled  = true
host     = "localhost"
ilp_port = 9000
pg_port  = 8812
```

| Key        | Type   | Default       | Description                                                                               |
| ---------- | ------ | ------------- | ----------------------------------------------------------------------------------------- |
| `enabled`  | bool   | `true`        | Include QuestDB when `--db` is not specified. Set to `false` to skip it globally.         |
| `host`     | string | `"localhost"` | Hostname or IP of the QuestDB server.                                                     |
| `ilp_port` | int    | `9000`        | InfluxDB Line Protocol (ILP) over HTTP — used exclusively for ingestion.                  |
| `pg_port`  | int    | `8812`        | PGWire port — used for all query benchmarks and DDL (`CREATE TABLE`, `DROP TABLE`, etc.). |

---

### `[databases.cratedb]`

```toml
[databases.cratedb]
enabled   = true
host      = "localhost"
http_port = 4200
pg_port   = 5432
```

| Key         | Type   | Default       | Description                                                             |
| ----------- | ------ | ------------- | ----------------------------------------------------------------------- |
| `enabled`   | bool   | `true`        | Include CrateDB when `--db` is not specified.                           |
| `host`      | string | `"localhost"` | Hostname or IP.                                                         |
| `http_port` | int    | `4200`        | CrateDB HTTP API — used for bulk `UNNEST` inserts and admin operations. |
| `pg_port`   | int    | `5432`        | PostgreSQL wire protocol — used for queries and DDL.                    |

---

### `[databases.timescaledb]`

```toml
[databases.timescaledb]
enabled  = true
host     = "localhost"
pg_port  = 5433
user     = "postgres"
password = "postgres"
dbname   = "tsbm"
```

| Key        | Type   | Default       | Description                                                                                         |
| ---------- | ------ | ------------- | --------------------------------------------------------------------------------------------------- |
| `enabled`  | bool   | `true`        | Include TimescaleDB when `--db` is not specified.                                                   |
| `host`     | string | `"localhost"` | Hostname or IP.                                                                                     |
| `pg_port`  | int    | `5433`        | Docker maps host port `5433` → container port `5432`. Change if you run TimescaleDB outside Docker. |
| `user`     | string | `"postgres"`  | PostgreSQL username.                                                                                |
| `password` | string | `"postgres"`  | PostgreSQL password.                                                                                |
| `dbname`   | string | `"tsbm"`      | Database name. Created automatically by the Docker image's init script.                             |

---

### `[workload]`

Controls the dataset and all benchmark parameters.

```toml
[workload]
dataset                  = "datasets/small_iot.csv"
batch_sizes              = [10_000, 100_000, 500_000, 1_000_000]
workers                  = [1, 4, 8, 16]
warmup_iterations        = 3
measurement_rounds       = 10
time_windows             = ["1min", "1h", "1day", "1week"]
prng_seed                = 42
mixed_duration_seconds   = 60
reset_between_rounds     = false
tag_cardinality_threshold = 0.05
streaming_threshold_rows = 1_000_000
chunk_size               = 500_000
unit_conversions         = {}
mv_granularity           = "1 hour"
late_arrival_rounds      = 10
late_arrival_batch_size  = 10_000
ingestion_seed_rows      = 0
```

#### Dataset

| Key       | Type | Default                    | Description                                                                                                  |
| --------- | ---- | -------------------------- | ------------------------------------------------------------------------------------------------------------ |
| `dataset` | path | `"datasets/small_iot.csv"` | Path to the benchmark dataset (`.parquet`, `.csv`, `.json`, or `.jsonl`). Relative to the working directory. |

#### Ingestion parameters

| Key                    | Type    | Default                                 | Description                                                                                                                                                                                                                                                                                                                                                       |
| ---------------------- | ------- | --------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `batch_sizes`          | `[int]` | `[10_000, 100_000, 500_000, 1_000_000]` | Row counts tested per `INSERT` call. Each value runs as an independent combination with every worker count. Larger batches amortise per-call overhead; very small batches stress commit and WAL paths. Batches larger than the dataset are capped to the dataset size automatically.                                                                              |
| `workers`              | `[int]` | `[1, 4, 8, 16]`                         | Number of concurrent async writer tasks per batch. `1` = serial, `>1` = the batch is split equally and submitted with `asyncio.gather`. Exercises the database's write concurrency and connection-pool scaling.                                                                                                                                                   |
| `warmup_iterations`    | int     | `3`                                     | Iterations discarded before measurement begins. Warms JIT compilers, OS page caches, and connection pools. Reduce to `2` for 100 M-row datasets.                                                                                                                                                                                                                  |
| `measurement_rounds`   | int     | `10`                                    | Timed iterations that feed the summary statistics. More rounds → tighter percentile estimates. Reduce to `5` for very large datasets or slow machines.                                                                                                                                                                                                            |
| `reset_between_rounds` | bool    | `false`                                 | `false` — rows accumulate across rounds, testing append-into-growing-table performance. `true` — the table is dropped and recreated before each round, giving a reproducible clean-slate measurement.                                                                                                                                                             |
| `ingestion_seed_rows`  | int     | `0`                                     | Pre-populate the table with this many rows **after** warmup and **before** measurement rounds. Simulates writing into an existing production table where indexes, partitions, and WAL segments are already warm. `0` = disabled (clean-table baseline). Has no effect when `reset_between_rounds = true` because the table is dropped at the start of each round. |

#### Query parameters

| Key                         | Type    | Default                        | Description                                                                                                                                                                                  |
| --------------------------- | ------- | ------------------------------ | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `time_windows`              | `[str]` | `["1min","1h","1day","1week"]` | Window durations used by `time_range`, `aggregation`, `last_point`, `high_cardinality`, and `downsampling`. Supported units: `1min`, `5min`, `1h`, `1hour`, `1day`, `1week`.                 |
| `prng_seed`                 | int     | `42`                           | Seed for `numpy.random.default_rng` when generating random time windows. Using the same seed across runs makes query windows reproducible and comparable.                                    |
| `tag_cardinality_threshold` | float   | `0.05`                         | A string column is classified as a _tag_ (low-cardinality, indexed) if the ratio of unique values to total rows is ≤ this value. Above the threshold it is treated as a plain `TEXT` metric. |

#### Streaming

| Key                        | Type | Default     | Description                                                                                                                                                               |
| -------------------------- | ---- | ----------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `streaming_threshold_rows` | int  | `1_000_000` | Files whose row count exceeds this value are read and ingested in chunks instead of being loaded fully into RAM. For 11 M rows set to ≤ 1 M; for 100 M rows set to ≤ 5 M. |
| `chunk_size`               | int  | `500_000`   | Rows per streaming chunk. Larger chunks reduce I/O round-trips but increase peak RAM usage. `250_000`–`1_000_000` is a good range for 16 GB+ systems.                     |

#### Mixed benchmark

| Key                      | Type | Default | Description                                                                                                                |
| ------------------------ | ---- | ------- | -------------------------------------------------------------------------------------------------------------------------- |
| `mixed_duration_seconds` | int  | `60`    | How long the concurrent read + write benchmark runs. Longer durations expose steady-state behaviour after caches saturate. |

#### Materialized view benchmarks

| Key                       | Type   | Default    | Description                                                                                                                                                     |
| ------------------------- | ------ | ---------- | --------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `mv_granularity`          | string | `"1 hour"` | Time-bucket width passed to `CREATE MATERIALIZED VIEW`. Must match the format expected by the database (e.g. `"1 hour"` for TimescaleDB, `"1h"` also accepted). |
| `late_arrival_rounds`     | int    | `10`       | Number of times a batch of late-arriving rows is inserted and the MV refresh measured.                                                                          |
| `late_arrival_batch_size` | int    | `10_000`   | Rows per late-arrival batch. Set to ~0.1 % of your total dataset size so the refresh cost is measurable but not catastrophic.                                   |

#### Unit conversions

```toml
# Optional: convert metric columns before ingestion.
# Format: { "column_name" = ["from_unit", "to_unit"] }
unit_conversions = { "temperature" = ["celsius", "fahrenheit"] }
```

Supported conversions are applied in-memory before any data is sent to the database.

---

### `[results]`

```toml
[results]
sqlite_path = "results/runs.db"
parquet_dir = "results/parquet"
```

| Key           | Type | Default             | Description                                                                                                                                                                                            |
| ------------- | ---- | ------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| `sqlite_path` | path | `"results/runs.db"` | SQLite database holding run metadata (`runs` table) and aggregated summaries (`summaries` table). Used by `tsbm compare`.                                                                              |
| `parquet_dir` | path | `"results/parquet"` | Root directory for per-operation Parquet files. Layout: `{parquet_dir}/{run_id}/{database}/{operation}.parquet`. Each file stores every `OperationResult` (warmup + measurement) for offline analysis. |

---

### `[monitor]`

```toml
[monitor]
enabled     = true
interval_ms = 500
```

| Key           | Type | Default | Description                                                                                                                                                       |
| ------------- | ---- | ------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`     | bool | `true`  | Sample CPU % and RSS memory in a background thread while the benchmark runs. Results are attached to each `BenchmarkSummary` and stored in the `summaries` table. |
| `interval_ms` | int  | `500`   | Sampling interval in milliseconds. Lower values give finer-grained resource data at the cost of slightly higher overhead. `200`–`1000` ms is the practical range. |

---

### Environment Variable Overrides

Every key in `benchmark.toml` can be overridden without editing the file by setting an environment variable prefixed with `TSBM_` and using `__` as the hierarchy separator.

```bash
# Connect to a remote QuestDB host
export TSBM_DATABASES__QUESTDB__HOST=x.x.x.x

# Disable CrateDB for this run only
export TSBM_DATABASES__CRATEDB__ENABLED=false

# Use a different dataset
export TSBM_WORKLOAD__DATASET=datasets/11m.parquet

# Reduce measurement rounds without touching the file
export TSBM_WORKLOAD__MEASUREMENT_ROUNDS=5
```

Priority order (highest wins):

1. Environment variables (`TSBM_…`)
2. `benchmark.toml`
3. Built-in defaults

---

## CLI Reference

### `tsbm run`

```
tsbm run [OPTIONS]

Options:
  --db / -d       TEXT   Database(s) to benchmark. Repeat for multiple.
                         Default: all databases with enabled = true.
                         Values: questdb | cratedb | timescaledb

  --benchmark / -b TEXT  Workload to run, or 'all' to run every workload
                         sequentially. Default: ingestion.
                         Values: ingestion | ingestion_out_of_order |
                                 time_range | aggregation | last_point |
                                 high_cardinality | downsampling | mixed |
                                 materialized_view | late_arrival | all

  --dataset        PATH  Override the dataset path from benchmark.toml.

  --config / -c    PATH  Path to a benchmark.toml file.
                         Default: ./benchmark.toml in the working directory.

  --verbose / -v         Enable DEBUG-level logging.
```

**Examples:**

```bash
# All benchmarks, all enabled databases
tsbm run --benchmark all

# Single benchmark on two databases
tsbm run --benchmark aggregation --db questdb --db timescaledb

# Large dataset, fewer rounds (override without editing the file)
TSBM_WORKLOAD__MEASUREMENT_ROUNDS=5 tsbm run \
    --benchmark all --dataset datasets/100m.parquet
```

---

### `tsbm generate`

```
tsbm generate [OPTIONS]

Options:
  --devices / -n   INT   Number of unique devices (tag values). Default: 100.
  --readings / -r  INT   Readings per device. Default: 1000.
  --seed           INT   PRNG seed for reproducibility. Default: 42.
  --output / -o    PATH  Output file (.parquet or .csv). Default: datasets/generated.parquet.
```

**Scale guide:**

```bash
tsbm generate --devices 100   --readings 1000  --output datasets/100k.parquet   #  ~100 K rows
tsbm generate --devices 5000  --readings 2200  --output datasets/11m.parquet    #  ~11 M rows
tsbm generate --devices 10000 --readings 10000 --output datasets/100m.parquet   # ~100 M rows
```

---

### `tsbm load`

Ingests a dataset into the database without running any benchmark timing. Useful for pre-populating a table before running query-only benchmarks manually.

```
tsbm load [OPTIONS]

Options:
  --db / -d       TEXT   Target database(s).
  --dataset        PATH  Dataset file to load.
  --config / -c    PATH  benchmark.toml path.
  --drop / --no-drop     Drop the existing table before loading. Default: --drop.
```

---

### `tsbm seed`

Seeds QuestDB with the full dataset without running any benchmarks. Uses parallel ingest workers and automatically throttles when QuestDB's WAL lag gets too high. Displays live rows/sec throughput.

```
tsbm seed [OPTIONS]

Options:
  --config / -c    PATH  benchmark.toml path.
  --dataset        TEXT  Override dataset path(s). Repeat for multiple files.
                         Supports globs and az:// URLs.
  --timestamp-col  TEXT  Name of the timestamp column (overrides auto-detection).
  --drop / --no-drop     Drop existing table before seeding. Default: --drop.
  --workers / -w   INT   Number of parallel ingest workers. 0 = use config value.
                         Default: 0.
  --verbose / -v         Enable DEBUG-level logging.
```

**Examples:**

```bash
# Seed using the dataset from benchmark.toml
tsbm seed

# Seed with a specific dataset and 8 workers
tsbm seed --dataset datasets/11m.parquet --workers 8

# Append into an existing table (no drop)
tsbm seed --no-drop --dataset datasets/extra.parquet
```

---

### `tsbm compare`

```
tsbm compare <run_id> [<run_id> ...] [OPTIONS]

Arguments:
  run_ids   One or more run IDs printed at the end of tsbm run.

Options:
  --operation / -o  TEXT  Filter to a specific operation name.
  --metric / -m     TEXT  Column to highlight. Default: latency_p99_ms.
                          Other useful values: latency_p50_ms, rows_per_second_mean,
                          mb_per_second_mean, resource_cpu_percent_mean.
  --format / -f     TEXT  Output format: table | csv | json | markdown.
                          Default: table.
  --output          PATH  Write output to a file instead of stdout.
  --config / -c     PATH  benchmark.toml path (for results DB location).
```

**Examples:**

```bash
# Side-by-side table in the terminal
tsbm compare abc123 def456

# Markdown report for a wiki or PR comment
tsbm compare abc123 def456 --format markdown --output comparison.md

# Filter to ingestion only, export CSV for spreadsheet analysis
tsbm compare abc123 def456 --operation ingestion --format csv --output ingestion.csv
```

---

### `tsbm report`

Generates a self-contained Markdown file with all benchmark results and the SQL queries that were executed — useful for sharing, archiving, or comparing against a database not covered by tsbm.

```
tsbm report [RUN_IDS...] [OPTIONS]

Arguments:
  run_ids   Zero or more run IDs to include. Omit to auto-discover the latest
            completed run per (benchmark, database) pair.

Options:
  --benchmark / -b  TEXT  Limit the report to one benchmark.
  --output / -o     PATH  Output Markdown file. Default: results/report.md.
  --latest          bool  Auto-select latest runs (default: on). Use --no-latest
                          to disable when supplying explicit run IDs.
  --config / -c     PATH  benchmark.toml path.
  --verbose / -v          Enable DEBUG-level logging.
```

**Report contents:**

| Section | Description |
|---|---|
| Run Metadata | Table of included runs — benchmark name, database, dataset, timestamps, run ID |
| Results | Per-benchmark comparison tables with databases as columns — Rows/sec, p50/p95/p99 latency |
| SQL Queries | Exact SQL templates per benchmark × database with real column names from the dataset |

**Examples:**

```bash
# Auto-discover latest runs, write to results/report.md
tsbm report

# Custom output path
tsbm report --output reports/2026-02-24.md

# Only the aggregation benchmark
tsbm report --benchmark aggregation

# Specific run IDs (e.g. to compare two different dataset sizes)
tsbm report abc123 def456 ghi789
```

---

### `tsbm dashboard`

```
tsbm dashboard [OPTIONS]

Options:
  --port / -p     INT   Streamlit server port. Default: 8501.
  --results-db    PATH  Path to results SQLite DB. Default: value from benchmark.toml.
  --config / -c   PATH  benchmark.toml path.
```

---

## Results & Comparison

### Generating a report

`tsbm report` produces a single Markdown file containing all results and SQL queries — the easiest way to compare databases or share results. It auto-discovers the most recently completed run per benchmark and database:

```bash
tsbm report                         # → results/report.md
tsbm report --output quarterly.md   # custom path
```

For quick terminal comparisons between two specific runs, use `tsbm compare`:

```bash
tsbm compare <run_id_a> <run_id_b> --format table
```

### Storage layout

```
results/
├── runs.db                           # SQLite — run metadata + aggregated summaries
└── parquet/
    └── {run_id}/
        └── {database}/
            └── {operation}.parquet   # every OperationResult (warmup + measurement)
```

### SQLite tables

**`runs`** — one row per `tsbm run` invocation per database:

| Column            | Description                                    |
| ----------------- | ---------------------------------------------- |
| `run_id`          | 12-char hex ID printed at the end of every run |
| `benchmark_name`  | Workload name                                  |
| `database_name`   | `questdb` / `cratedb` / `timescaledb`          |
| `dataset_name`    | Filename stem of the dataset                   |
| `config_hash`     | MD5 of the full config (for change detection)  |
| `started_at`      | UTC timestamp                                  |
| `completed_at`    | UTC timestamp (null if the run failed)         |
| `config_snapshot` | Full settings dict + database version as JSON  |

**`summaries`** — one row per (run, operation, batch_size, workers) combination:

| Column group | Columns                                                                                                                                                                                        |
| ------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| Identity     | `run_id`, `database_name`, `operation`, `batch_size`, `workers`                                                                                                                                |
| Counts       | `total_rows`, `sample_count`                                                                                                                                                                   |
| Latency (ms) | `latency_p50_ms`, `latency_p95_ms`, `latency_p99_ms`, `latency_p999_ms`, `latency_mean_ms`, `latency_min_ms`, `latency_max_ms`, `latency_iqr_ms`, `latency_stddev_ms`, `latency_outlier_count` |
| Throughput   | `rows_per_second_mean`, `rows_per_second_p95`, `rows_per_second_p99`, `mb_per_second_mean`                                                                                                     |
| Resources    | `resource_cpu_percent_mean`, `resource_cpu_percent_max`, `resource_rss_mb_mean`, `resource_rss_mb_max`, `resource_disk_write_mb_total`, `resource_net_sent_mb_total`                           |

---

## Architecture

```
src/tsbm/
├── adapters/               # Database drivers
│   ├── base.py             # DatabaseAdapter + MVCapableAdapter protocols
│   ├── questdb.py          # ILP HTTP ingestion + PGWire queries + native MVs
│   ├── cratedb.py          # UNNEST bulk INSERT + simulated MV (summary table)
│   ├── timescaledb.py      # COPY ingestion + continuous aggregates
│   ├── registry.py         # Lazy-import adapter factory
│   └── type_maps.py        # Arrow dtype → CREATE TABLE DDL per database
├── benchmarks/             # Workload implementations
│   ├── base.py             # BenchmarkWorkload protocol + BenchmarkResult
│   ├── ingestion.py        # IngestionBenchmark + OutOfOrderIngestionBenchmark
│   ├── queries.py          # TimeRange, Aggregation, LastPoint, HighCardinality, Downsampling
│   ├── mixed.py            # MixedBenchmark (concurrent read + write)
│   ├── materialized_views.py # MaterializedViewBenchmark + LateArrivalBenchmark
│   └── registry.py         # Dict-based workload registry
├── config/
│   └── settings.py         # Pydantic-settings (TOML + env vars, prefix TSBM_)
├── datasets/
│   ├── schema.py           # DatasetSchema, BenchmarkDataset
│   ├── loader.py           # load_dataset(), streaming support
│   └── generator.py        # Synthetic IoT data generator
├── metrics/
│   ├── timer.py            # TimingResult, timed_operation() context manager
│   ├── stats.py            # Percentile / outlier aggregation (LatencyStats)
│   └── monitor.py          # Background CPU / RSS sampler (ResourceMonitor)
├── results/
│   ├── models.py           # RunConfig, OperationResult, BenchmarkSummary
│   ├── storage.py          # SQLite + Parquet persistence
│   └── export.py           # CSV / JSON / Markdown / full report export
├── environment/
│   └── capture.py          # Python version, OS, CPU info snapshot
├── dashboard/
│   └── app.py              # Streamlit interactive results viewer
└── cli/
    ├── main.py             # Typer app + command definitions
    ├── run.py              # Benchmark orchestration (run, generate, load, seed)
    ├── compare.py          # tsbm compare command
    ├── report.py           # tsbm report command (Markdown report with SQL queries)
    └── dashboard.py        # Streamlit subprocess launcher
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

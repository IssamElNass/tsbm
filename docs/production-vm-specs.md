# Production VM Specifications

Recommended VM sizing for running tsbm benchmarks and production time-series workloads.

## Quick Reference

| Tier | CPU | RAM | Disk | Dataset Size | Docker Compose |
|------|-----|-----|------|-------------|----------------|
| **Dev/Smoke** | 4 cores | 8 GB | 50 GB SSD | up to 1M rows | `docker-compose.yml` |
| **Standard** | 8 cores | 32 GB | 200 GB SSD | up to 50M rows | `docker-compose.yml` |
| **Production** | 16 cores | 64 GB | 500 GB NVMe | up to 500M rows | `docker-compose.performance.yml` |
| **High-Scale** | 32 cores | 128 GB | 1 TB NVMe | 500M–1B+ rows | `docker-compose.performance.yml` (scaled) |

## Azure VM Recommendations

| Tier | VM SKU | vCPUs | RAM | Temp Disk | Monthly (est.) |
|------|--------|-------|-----|-----------|----------------|
| Dev/Smoke | Standard_D4s_v5 | 4 | 16 GB | 150 GB | ~$140 |
| Standard | Standard_D8s_v5 | 8 | 32 GB | 300 GB | ~$280 |
| Production | Standard_E16s_v5 | 16 | 64 GB (mem-optimised) | 600 GB | ~$730 |
| High-Scale | Standard_E32s_v5 | 32 | 128 GB (mem-optimised) | 1.2 TB | ~$1,460 |

**Disk:** Attach a Premium SSD P30 (1 TB, 5000 IOPS) or P40 (2 TB, 7500 IOPS) as the data volume.
For High-Scale, use Premium SSD v2 or Ultra Disk for consistent sub-ms latency.

## AWS EC2 Recommendations

| Tier | Instance Type | vCPUs | RAM | Monthly (est.) |
|------|---------------|-------|-----|----------------|
| Dev/Smoke | r6i.xlarge | 4 | 32 GB | ~$180 |
| Standard | r6i.2xlarge | 8 | 64 GB | ~$360 |
| Production | r6i.4xlarge | 16 | 128 GB | ~$720 |
| High-Scale | r6i.8xlarge | 32 | 256 GB | ~$1,440 |

**Disk:** gp3 with 500 GB, 6000 IOPS, 250 MB/s throughput baseline.
For High-Scale, use io2 Block Express for guaranteed IOPS.

## Disk Recommendations

| Workload | Type | Min Size | Min IOPS | Notes |
|----------|------|----------|----------|-------|
| Dev/Smoke | Standard SSD | 50 GB | 500 | Fine for quick tests |
| Standard | Premium SSD | 200 GB | 3000 | Covers 3 databases + dataset |
| Production | NVMe / Premium SSD v2 | 500 GB | 5000+ | Critical for ingestion throughput |
| High-Scale | NVMe / Ultra Disk | 1 TB+ | 10,000+ | Required for 100M+ row ingestion |

All three databases benefit significantly from NVMe storage for write-heavy workloads.
QuestDB and TimescaleDB use WAL (Write-Ahead Logging) which is IOPS-bound.
CrateDB uses Lucene segment merging which is throughput-bound.

## Database-Specific Tuning Per Tier

### QuestDB

| Setting | Standard | Production | High-Scale |
|---------|----------|------------|------------|
| `QDB_SHARED_WORKER_COUNT` | 8 | 16 | 32 |
| `QDB_CAIRO_SQL_PARALLEL_FILTER_WORKER_COUNT` | 8 | 16 | 32 |
| `QDB_CAIRO_SQL_PARALLEL_GROUPBY_ENABLED` | true | true | true |
| `QDB_CAIRO_MAX_UNCOMMITTED_ROWS` | 2,000,000 | 4,000,000 | 8,000,000 |
| `QDB_LINE_HTTP_RECV_BUFFER_SIZE` | 16 MB | 32 MB | 64 MB |
| `QDB_HTTP_WORKER_COUNT` | 4 | 8 | 16 |
| `partition_by` (benchmark.toml) | DAY | HOUR | HOUR |

### CrateDB

| Setting | Standard | Production | High-Scale |
|---------|----------|------------|------------|
| `CRATE_HEAP_SIZE` | 16g | 32g | 64g |
| `index.number_of_shards` | 8 | 16 | 32 |
| `indices.memory.index_buffer_size` | 4096mb | 8192mb | 16384mb |
| `threadpool.search.size` | 8 | 16 | 32 |
| `threadpool.write.size` | 8 | 16 | 32 |

### TimescaleDB

| Setting | Standard | Production | High-Scale |
|---------|----------|------------|------------|
| `shared_buffers` | 8GB | 16GB | 32GB |
| `effective_cache_size` | 24GB | 48GB | 96GB |
| `work_mem` | 256MB | 512MB | 1GB |
| `maintenance_work_mem` | 2GB | 4GB | 8GB |
| `max_parallel_workers_per_gather` | 4 | 8 | 12 |
| `max_parallel_workers` | 8 | 16 | 32 |
| `chunk_time_interval` (benchmark.toml) | 7 days | 1 day | 1 day |
| `shm_size` (Docker) | 10gb | 18gb | 36gb |

## Workload Tuning Per Tier

Match your `benchmark.toml` settings to your VM tier:

```toml
# ── Standard (8 cores / 32 GB) ──
batch_sizes = [10_000, 100_000, 500_000]
workers = [1, 4, 8]
warmup_iterations = 3
measurement_rounds = 10
streaming_threshold_rows = 500_000
chunk_size = 100_000

# ── Production (16 cores / 64 GB) ──
batch_sizes = [50_000, 500_000, 2_000_000]
workers = [1, 4, 8, 16]
warmup_iterations = 2
measurement_rounds = 5
streaming_threshold_rows = 2_000_000
chunk_size = 500_000

# ── High-Scale (32 cores / 128 GB) ──
batch_sizes = [100_000, 1_000_000, 5_000_000]
workers = [1, 2, 4, 8]
warmup_iterations = 1
measurement_rounds = 3
streaming_threshold_rows = 5_000_000
chunk_size = 1_000_000
```

## Network Considerations

- All three databases should run on the **same VM** as the benchmark tool to eliminate network latency from results.
- If running databases on separate VMs, use a **low-latency network** (Azure Accelerated Networking / AWS Enhanced Networking) and place all VMs in the same availability zone.
- QuestDB ILP ingestion is sensitive to network latency — each batch is a single HTTP POST that must complete before the next batch starts.

## OS Tuning

For production benchmark runs, apply these Linux sysctl settings:

```bash
# Increase max open files (all 3 databases need many file descriptors)
ulimit -n 131072

# Disable transparent hugepages (causes latency spikes during compaction)
echo never > /sys/kernel/mm/transparent_hugepage/enabled

# Increase VM dirty ratio for write-heavy workloads
sysctl -w vm.dirty_ratio=40
sysctl -w vm.dirty_background_ratio=10

# Increase max memory map areas (CrateDB and QuestDB use mmap)
sysctl -w vm.max_map_count=262144
```

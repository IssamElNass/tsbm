# TSBM Benchmark Summary

**Databases:** cratedb, questdb, timescaledb  **Dataset:** 11m  **Date:** 2026-03-02

---

## Ingestion Throughput

| Database | Rows/sec | MB/sec | p99 (ms) | Rank |
|---|---|---|---|---|
| questdb | 1,842,500 | 128.4 | 312.5 | **#1** |
| timescaledb | 485,200 | 33.8 | 1,240.3 | #2 |
| cratedb | 312,600 | 21.8 | 2,105.7 | #3 |

## Query Latency (p99 ms)

| Benchmark | cratedb | questdb | timescaledb | Fastest |
| --- | --- | --- | --- | --- |
| time_range | 185.3 | 12.4 | 45.8 | **questdb** |
| aggregation | 342.1 | 8.7 | 28.4 | **questdb** |
| last_point | 1,250.8 | 5.2 | 312.5 | **questdb** |
| high_cardinality | 2,840.3 | 95.6 | 485.2 | **questdb** |
| downsampling | 4,520.1 | 245.3 | 1,105.8 | **questdb** |

## Concurrency Scaling

| Database | 1w (ms) | 4w (ms) | 8w (ms) | 16w (ms) | Degradation |
| --- | --- | --- | --- | --- | --- |
| cratedb | 125.4 | 198.7 | 412.3 | 845.6 | 6.7x |
| questdb | 4.2 | 6.8 | 12.5 | 28.4 | 6.8x |
| timescaledb | 18.5 | 32.1 | 68.4 | 142.5 | 7.7x |

> Scaling benchmark: `aggregation`

## Errors & Reliability

| Database | Completed Runs | Failed Runs | Status |
|---|---|---|---|
| cratedb | 6 | 0 | Clean |
| questdb | 6 | 0 | Clean |
| timescaledb | 6 | 0 | Clean |

## Key Findings

1. **Fastest ingestion:** questdb (1,842,500 rows/sec)
2. **Lowest query latency:** questdb (p99 5.2 ms on last_point)
3. **Best concurrency scaling:** cratedb (6.7x degradation from 1 to 16 workers)
4. **Best mixed read/write:** questdb (925,400 rows/sec under concurrent load)

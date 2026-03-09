#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# tsbm quickstart — generate data, start databases, run benchmarks, report.
#
# Usage:
#   bash scripts/quickstart.sh                  # default: 11M rows, 6 core benchmarks
#   bash scripts/quickstart.sh --small          # 100K rows, quick smoke-test
#   bash scripts/quickstart.sh --large          # 100M rows, stress test
#   bash scripts/quickstart.sh --all            # run all 10 benchmarks
#   bash scripts/quickstart.sh --perf           # use performance-tuned Docker Compose
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

# ── Defaults ──────────────────────────────────────────────────────────────────
DEVICES=5000
READINGS=2200
DATASET="datasets/11m.parquet"
COMPOSE_FILE="docker/docker-compose.yml"
BENCHMARK="default"

# ── Parse arguments ───────────────────────────────────────────────────────────
for arg in "$@"; do
    case "$arg" in
        --small)
            DEVICES=100
            READINGS=1000
            DATASET="datasets/100k.parquet"
            ;;
        --large)
            DEVICES=10000
            READINGS=10000
            DATASET="datasets/100m.parquet"
            ;;
        --perf)
            COMPOSE_FILE="docker/docker-compose.performance.yml"
            ;;
        --all)
            BENCHMARK="all"
            ;;
        *)
            echo "Unknown argument: $arg"
            echo "Usage: bash scripts/quickstart.sh [--small|--large] [--all] [--perf]"
            exit 1
            ;;
    esac
done

TOTAL_ROWS=$((DEVICES * READINGS))
echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  tsbm quickstart                                               ║"
echo "║  Dataset: ${TOTAL_ROWS} rows (${DEVICES} devices × ${READINGS} readings)  "
echo "║  Compose: ${COMPOSE_FILE}                                      "
echo "║  Benchmark: ${BENCHMARK}                                       "
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# ── Step 1: Generate dataset ─────────────────────────────────────────────────
if [ -f "$DATASET" ]; then
    echo "→ Dataset already exists: $DATASET (skipping generation)"
else
    echo "→ Step 1/5: Generating dataset..."
    tsbm generate --devices "$DEVICES" --readings "$READINGS" --output "$DATASET"
fi
echo ""

# ── Step 2: Start databases ──────────────────────────────────────────────────
echo "→ Step 2/5: Starting databases..."
docker compose -f "$COMPOSE_FILE" up -d
echo ""

# ── Step 3: Wait for health checks ──────────────────────────────────────────
echo "→ Step 3/5: Waiting for databases to become healthy..."

wait_for_service() {
    local service=$1
    local max_wait=120
    local elapsed=0
    while [ $elapsed -lt $max_wait ]; do
        status=$(docker compose -f "$COMPOSE_FILE" ps --format json "$service" 2>/dev/null | \
                 python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('Health',''))" 2>/dev/null || echo "")
        if [ "$status" = "healthy" ]; then
            echo "  ✓ $service is healthy"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    echo "  ✗ $service did not become healthy within ${max_wait}s"
    return 1
}

wait_for_service "questdb"
wait_for_service "cratedb"
wait_for_service "timescaledb"
echo ""

# ── Step 4: Run benchmarks ───────────────────────────────────────────────────
echo "→ Step 4/5: Running benchmarks (this may take a while)..."
tsbm run --benchmark "$BENCHMARK" --dataset "$DATASET"
echo ""

# ── Step 5: Generate report ──────────────────────────────────────────────────
echo "→ Step 5/5: Generating summary report..."
tsbm report --summary -o results/report.md
echo ""

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Done!                                                         ║"
echo "║                                                                ║"
echo "║  Report:    results/report.md                                  ║"
echo "║  Results:   results/runs.db (SQLite)                           ║"
echo "║  Raw data:  results/parquet/                                   ║"
echo "║                                                                ║"
echo "║  Next steps:                                                   ║"
echo "║    tsbm report --full -o results/full_report.md                ║"
echo "║    tsbm dashboard                                              ║"
echo "║    tsbm compare <run_id_1> <run_id_2>                         ║"
echo "╚══════════════════════════════════════════════════════════════════╝"

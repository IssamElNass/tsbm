#!/usr/bin/env bash
# ──────────────────────────────────────────────────────────────────────────────
# setup-perf-vm.sh — Prepare a VM for performance benchmarks.
#
# What it does:
#   1. Creates /data/{questdb,cratedb,timescaledb} directories
#   2. Sets ownership to match each container's expected UID
#   3. Tunes kernel settings (vm.max_map_count, open files) for databases
#
# Usage:
#   sudo bash scripts/setup-perf-vm.sh          # default: /data
#   sudo bash scripts/setup-perf-vm.sh /mnt/ssd # custom mount point
#
# Idempotent — safe to run multiple times.
# ──────────────────────────────────────────────────────────────────────────────
set -euo pipefail

DATA_ROOT="${1:-/data}"

# ── Must run as root ────────────────────────────────────────────────────────
if [ "$(id -u)" -ne 0 ]; then
    echo "Error: this script must be run as root (sudo)."
    exit 1
fi

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  tsbm VM setup — performance profile                          ║"
echo "║  Data root: ${DATA_ROOT}                                       "
echo "╚══════════════════════════════════════════════════════════════════╝"
echo ""

# ── 1. Create data directories ─────────────────────────────────────────────
echo "→ Creating data directories under ${DATA_ROOT} ..."
mkdir -p "${DATA_ROOT}/questdb"
mkdir -p "${DATA_ROOT}/cratedb"
mkdir -p "${DATA_ROOT}/timescaledb"

# QuestDB runs as UID 10001 inside the container
chown -R 10001:10001 "${DATA_ROOT}/questdb"

# CrateDB runs as UID 1000 inside the container
chown -R 1000:1000 "${DATA_ROOT}/cratedb"

# TimescaleDB (PostgreSQL) runs as UID 999 inside the container
chown -R 999:999 "${DATA_ROOT}/timescaledb"

echo "  ✓ Directories created and ownership set"
echo ""

# ── 2. Kernel tuning ───────────────────────────────────────────────────────
echo "→ Applying kernel tuning ..."

# CrateDB and QuestDB need high vm.max_map_count for mmap.
# 1048576 supports 8B+ rows with HOUR partitioning (~8760 partitions × 6 columns).
REQUIRED_MAP_COUNT=1048576
CURRENT_MAP_COUNT=$(sysctl -n vm.max_map_count 2>/dev/null || echo "0")
if [ "$CURRENT_MAP_COUNT" -lt "$REQUIRED_MAP_COUNT" ]; then
    sysctl -w vm.max_map_count=$REQUIRED_MAP_COUNT
    # Persist across reboots
    if grep -q "vm.max_map_count" /etc/sysctl.conf 2>/dev/null; then
        sed -i "s/vm.max_map_count=.*/vm.max_map_count=$REQUIRED_MAP_COUNT/" /etc/sysctl.conf
    else
        echo "vm.max_map_count=$REQUIRED_MAP_COUNT" >> /etc/sysctl.conf
    fi
    echo "  ✓ vm.max_map_count set to $REQUIRED_MAP_COUNT (was ${CURRENT_MAP_COUNT})"
else
    echo "  ✓ vm.max_map_count already ${CURRENT_MAP_COUNT} (>= $REQUIRED_MAP_COUNT)"
fi

# Raise open file limits for the current session
ulimit -n 131072 2>/dev/null || true

# Persist open file limits
LIMITS_FILE="/etc/security/limits.d/tsbench.conf"
if [ ! -f "$LIMITS_FILE" ]; then
    cat > "$LIMITS_FILE" <<'EOF'
# tsbm benchmark — raise open file limits for database containers
*    soft    nofile    131072
*    hard    nofile    131072
root soft    nofile    131072
root hard    nofile    131072
EOF
    echo "  ✓ Open file limits persisted to ${LIMITS_FILE}"
else
    echo "  ✓ Open file limits already configured"
fi

# Disable transparent huge pages (reduces latency jitter for databases)
if [ -f /sys/kernel/mm/transparent_hugepage/enabled ]; then
    echo never > /sys/kernel/mm/transparent_hugepage/enabled 2>/dev/null || true
    echo never > /sys/kernel/mm/transparent_hugepage/defrag 2>/dev/null || true
    echo "  ✓ Transparent huge pages disabled"
fi

echo ""

# ── 3. Verify disk ─────────────────────────────────────────────────────────
echo "→ Disk info for ${DATA_ROOT}:"
df -h "${DATA_ROOT}" 2>/dev/null || echo "  (could not determine disk info)"
echo ""

echo "╔══════════════════════════════════════════════════════════════════╗"
echo "║  Setup complete!                                               ║"
echo "║                                                                ║"
echo "║  Next:                                                         ║"
echo "║    bash scripts/quickstart.sh --perf                           ║"
echo "╚══════════════════════════════════════════════════════════════════╝"

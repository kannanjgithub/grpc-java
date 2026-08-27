#!/bin/bash
# Drives BurstRepro: proxy + backend + resolver-burst iterations hunting the stale-picker wedge.
set -u

WORK=pf-stuck-repro
OUT=$WORK/out-burst
MODE_FILE=/tmp/proxy-mode
PROXY_PORT=15001
BACKEND_PORT=15002

mkdir -p "$OUT"
exec > >(tee "$OUT/driver.log") 2>&1

ts() { date +%H:%M:%S.%3N; }
say() { echo "$(ts) [driver] $*"; }

echo pass > "$MODE_FILE"
python3 "$WORK/proxy.py" $PROXY_PORT $BACKEND_PORT "$MODE_FILE" > "$OUT/proxy.log" 2>&1 &
PROXY_PID=$!
say "proxy started pid=$PROXY_PID"


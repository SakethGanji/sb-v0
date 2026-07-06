#!/usr/bin/env bash
# B6b Stage 2 FINAL leg (post-T7-migration): 2025 + 2026 + build-regimes.
# Output now lands on the T7 via the data/ symlink, so the disk guard checks
# the output dir's filesystem (df data/outputs), not the repo root.
set -u
cd /home/saketh/Projects/playground/saketh-atlas/atlas-sb
BIN=./target/release/write-forward-outcomes
OUT=data/outputs
CUR=data/_engine_state.sqlite
LOGD=data/_b6b_logs
MIN_FREE_GB=60
mkdir -p "$LOGD"

chunks=(
  "2025-01-01 2025-12-31"
  "2026-01-01 2026-06-05"
)

for ch in "${chunks[@]}"; do
  set -- $ch; from=$1; to=$2
  free_gb=$(df -BG --output=avail "$OUT" | tail -1 | tr -dc '0-9')
  echo "=== chunk $from..$to | out-fs free=${free_gb}G | $(date '+%m-%d %H:%M') ==="
  if [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
    echo "!!! ABORT: out-fs free ${free_gb}G < ${MIN_FREE_GB}G before $from."
    echo "=== B6B FINAL HALTED (disk guard) at $from ==="; exit 3
  fi
  t0=$(date +%s)
  $BIN --from "$from" --to "$to" --out "$OUT" --cursor "$CUR" --force \
      >"${LOGD}/fwd_${from}.log" 2>&1
  rc=$?; t1=$(date +%s)
  if [ $rc -ne 0 ]; then
    echo "!!! chunk $from FAILED rc=$rc"; tail -4 "${LOGD}/fwd_${from}.log" | grep -vE "future-dated|WARN"
    echo "=== B6B FINAL FAILED at $from ==="; exit $rc
  fi
  grep -E "^done:" "${LOGD}/fwd_${from}.log" | tail -1
  echo "    chunk done in $(( (t1-t0)/60 ))m"
done

echo "=== build-regimes (full corpus) ==="
./target/release/build-regimes --out "$OUT" >"${LOGD}/regimes.log" 2>&1 && tail -2 "${LOGD}/regimes.log"
df -h "$OUT" | tail -1
echo "=== B6B FORWARD SWEEP COMPLETE ==="

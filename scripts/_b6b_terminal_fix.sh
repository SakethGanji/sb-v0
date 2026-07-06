#!/usr/bin/env bash
# Terminal-event rename-fix re-sweep: forward_outcomes ONLY (--no-path), yearly
# chunks matching the original B6b boundaries so matrix_end stays consistent
# corpus-wide. forward_path_short is unaffected and left in place.
set -u
cd /home/saketh/Projects/playground/saketh-atlas/atlas-sb
BIN=./target/release/write-forward-outcomes
OUT=data/outputs
CUR=data/_engine_state.sqlite
LOGD=data/_b6b_logs
mkdir -p "$LOGD"

chunks=(
  "2016-06-08 2016-12-31" "2017-01-01 2017-12-31" "2018-01-01 2018-12-31"
  "2019-01-01 2019-12-31" "2020-01-01 2020-12-31" "2021-01-01 2021-12-31"
  "2022-01-01 2022-12-31" "2023-01-01 2023-12-31" "2024-01-01 2024-12-31"
  "2025-01-01 2025-12-31" "2026-01-01 2026-06-05"
)
for ch in "${chunks[@]}"; do
  set -- $ch; from=$1; to=$2
  free_gb=$(df -BG --output=avail "$OUT" | tail -1 | tr -dc '0-9')
  echo "=== fo-only chunk $from..$to | free=${free_gb}G | $(date '+%m-%d %H:%M') ==="
  if [ "$free_gb" -lt 40 ]; then echo "!!! ABORT low disk ${free_gb}G"; exit 3; fi
  t0=$(date +%s)
  $BIN --from "$from" --to "$to" --out "$OUT" --cursor "$CUR" --force --no-path \
      >"${LOGD}/foonly_${from}.log" 2>&1
  rc=$?; t1=$(date +%s)
  if [ $rc -ne 0 ]; then echo "!!! chunk $from FAILED rc=$rc"; tail -4 "${LOGD}/foonly_${from}.log" | grep -vE "future-dated|WARN"; exit $rc; fi
  grep -E "^done:" "${LOGD}/foonly_${from}.log" | tail -1
  echo "    chunk done in $(( (t1-t0)/60 ))m"
done
echo "=== B6B TERMINAL-FIX RE-SWEEP COMPLETE ($(date '+%H:%M')) ==="

#!/usr/bin/env bash
# B6b Stage 2 RESUME after the 2026-06-14 crash (died mid-2017 at 2017-08-17).
# 2016 + 2017<=08-16 already landed; re-do 2017 fully (idempotent) then 2018+.
# Logs go under data/ so they survive a reboot; disk guard stops cleanly before
# any chunk that won't fit (migrate to the 2TB, then re-run this to continue).
set -u
cd /home/saketh/Projects/playground/saketh-atlas/atlas-sb
BIN=./target/release/write-forward-outcomes
OUT=data/outputs
CUR=data/_engine_state.sqlite
LOGD=data/_b6b_logs
MIN_FREE_GB=75

chunks=(
  "2017-01-01 2017-12-31"
  "2018-01-01 2018-12-31"
  "2019-01-01 2019-12-31"
  "2020-01-01 2020-12-31"
  "2021-01-01 2021-12-31"
  "2022-01-01 2022-12-31"
  "2023-01-01 2023-12-31"
  "2024-01-01 2024-12-31"
  "2025-01-01 2025-12-31"
  "2026-01-01 2026-06-05"
)

for ch in "${chunks[@]}"; do
  set -- $ch; from=$1; to=$2
  # skip a chunk already fully on disk (its Dec file present), except let the
  # resume re-do the crash year if asked; here all listed chunks need running.
  free_gb=$(df -BG --output=avail . | tail -1 | tr -dc '0-9')
  echo "=== chunk $from..$to | free=${free_gb}G | $(date +%H:%M) ==="
  if [ "$free_gb" -lt "$MIN_FREE_GB" ]; then
    echo "!!! ABORT: free ${free_gb}G < ${MIN_FREE_GB}G. Migrate data/ to the 2TB, then re-run this from $from."
    echo "=== B6B RESUME HALTED (disk guard) at $from ==="
    exit 3
  fi
  t0=$(date +%s)
  $BIN --from "$from" --to "$to" --out "$OUT" --cursor "$CUR" --force \
      >"${LOGD}/fwd_${from}.log" 2>&1
  rc=$?; t1=$(date +%s)
  if [ $rc -ne 0 ]; then
    echo "!!! chunk $from FAILED rc=$rc"; tail -4 "${LOGD}/fwd_${from}.log" | grep -vE "future-dated|WARN"
    echo "=== B6B RESUME FAILED at $from ==="; exit $rc
  fi
  grep -E "^done:" "${LOGD}/fwd_${from}.log" | tail -1
  echo "    chunk done in $(( (t1-t0)/60 ))m"
done
echo "=== build-regimes (full corpus) ==="
./target/release/build-regimes --out "$OUT" >"${LOGD}/regimes.log" 2>&1 && tail -1 "${LOGD}/regimes.log"
df -h . | tail -1
echo "=== B6B FORWARD SWEEP COMPLETE ==="

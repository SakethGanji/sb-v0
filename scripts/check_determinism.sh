#!/usr/bin/env bash
# Determinism + resume-equality checks (implementation-plan.md §4.5 item 5).
#
# A. Same day written twice (fresh cursors) must be byte-identical.
# B. Clearing one day from the real cursor and re-sweeping WITHOUT --force
#    (rolling state rebuilt by reading prior days) must reproduce the
#    original file byte-for-byte.
#
# Usage: scripts/check_determinism.sh [DAY=2016-12-30] [FROM=2016-06-08]
set -euo pipefail
DAY="${1:-2016-12-30}"; FROM="${2:-2016-06-08}"
TABLES="daily_observation market_context_daily sector_aggregates_daily security_classification_daily"
BIN=./target/release/write-phase0

rm -rf /tmp/det1 /tmp/det2 /tmp/det1.sqlite /tmp/det2.sqlite
$BIN --day "$DAY" --force --out /tmp/det1 --cursor /tmp/det1.sqlite >/dev/null 2>&1
$BIN --day "$DAY" --force --out /tmp/det2 --cursor /tmp/det2.sqlite >/dev/null 2>&1
fail=0
for t in $TABLES; do
  if cmp -s "/tmp/det1/$t/$DAY.parquet" "/tmp/det2/$t/$DAY.parquet"; then
    echo "DETERMINISM PASS $t"
  else
    echo "DETERMINISM FAIL $t"; fail=1
  fi
done

for t in $TABLES; do
  sha256sum "data/outputs/$t/$DAY.parquet" | cut -d' ' -f1 > "/tmp/before_$t.sha"
done
python3 - "$DAY" << 'PY'
import sqlite3, sys
c = sqlite3.connect('data/_engine_state.sqlite')
c.execute("DELETE FROM day_done WHERE day=?", (sys.argv[1],))
c.commit()
PY
$BIN --from "$FROM" --to "$DAY" >/dev/null 2>&1
for t in $TABLES; do
  h=$(sha256sum "data/outputs/$t/$DAY.parquet" | cut -d' ' -f1)
  if [ "$h" = "$(cat /tmp/before_$t.sha)" ]; then
    echo "RESUME-EQUALITY PASS $t"
  else
    echo "RESUME-EQUALITY FAIL $t"; fail=1
  fi
done
exit $fail

#!/usr/bin/env bash
# The keeper loop (Round H1; same design as the NHL model's keeper).
# Sleeps until each slot in next_slot.py, starts that workflow, and before GitHub's 6-hour job
# limit starts a fresh keeper. A disabled workflow is skipped; when all six seasonal workflows are
# disabled (offseason) the keeper stops without handing off, so pausing/unpausing those six is all
# it takes. (scrape_results.yml stays on all year and has its own schedule.)
# Test knobs: DRY_RUN=1 (print, start nothing), MAX_SLOTS=n (stop after n), FAKE_WAIT=s (don't sleep long).
set -u
START=$(date +%s); BUDGET=${KEEPER_BUDGET:-19800}   # 5.5 h
REPO=${GITHUB_REPOSITORY:?}; SUMMARY=${RUN_SUMMARY:-/dev/null}
DIR=$(cd "$(dirname "$0")" && pwd)

note() { echo "$1 $(TZ=America/New_York date '+%-I:%M %p'): $2" | tee -a "$SUMMARY"; }
retry() { for i in $(seq 1 20); do "$@" && return 0; sleep 30; done; return 1; }   # keep trying ~10 min
active() { [ "$(gh api "repos/$REPO/actions/workflows/$1" --jq .state 2>/dev/null)" = "active" ]; }
start() {
  if [ -n "${DRY_RUN:-}" ]; then echo "DRY RUN: would start $1"; return 0; fi
  retry gh workflow run "$1" -R "$REPO"
}

any=""
for wf in $(python3 "$DIR/next_slot.py" --seasonal); do active "$wf" && { any=1; break; }; done   # scores job stays on all year: not counted
if [ -z "$any" ]; then
  note "•" "every daily job is paused (offseason): nothing to start, not handing off"
  exit 0
fi

n=0
while true; do
  read -r WAIT WF WHEN <<< "$(python3 "$DIR/next_slot.py")"
  [ -n "${FAKE_WAIT:-}" ] && WAIT=$FAKE_WAIT
  ELAPSED=$(( $(date +%s) - START ))
  if [ $((ELAPSED + WAIT)) -lt "$BUDGET" ]; then
    echo "next: $WHEN -> $WF (in $((WAIT / 60)) min)"; sleep "$WAIT"
    if ! active "$WF"; then note "•" "$WHEN: $WF is paused - skipped"
    elif start "$WF"; then note "✓" "${DRY_RUN:+(dry run) would have }started $WF ($WHEN)"
    else echo "::error::could not start $WF"; note "✗" "couldn't start $WF ($WHEN); its backup schedule will run it"; fi
    n=$((n + 1))
    if [ -n "${MAX_SLOTS:-}" ] && [ "$n" -ge "$MAX_SLOTS" ]; then echo "test: stopping after $n slot(s)"; exit 0; fi
    sleep "${AFTER_START:-90}"
  else
    REST=$(( BUDGET - ELAPSED ))
    [ "$REST" -gt 0 ] && { echo "next slot ($WHEN) is past this run's limit; handing off in $((REST / 60)) min"; sleep "$REST"; }
    if start keeper.yml; then note "✓" "handed off to a fresh keeper (its first slot: $WHEN)"
    else echo "::error::could not start the next keeper"; note "✗" "couldn't start the next keeper (the 6-hourly schedule restarts it)"; fi
    exit 0
  fi
done

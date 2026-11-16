#!/usr/bin/env python3
"""
schedule_report.py — are the daily jobs starting on time? (read-only, Round H1)

For each job the keeper starts (.github/scripts/next_slot.py), compares every run's start with
its keeper time and prints the delay: median, 90th percentile, worst, and who started it
(keeper / backup schedule / by hand). Uses the GitHub CLI (`gh`); changes nothing.

  python3 scripts/schedule_report.py            # last 14 days
  python3 scripts/schedule_report.py --days 30
"""
import argparse
import json
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", ".github", "scripts"))
import next_slot as ns   # noqa: E402

ET = ZoneInfo("America/New_York")
REPO = "Prezbo8/MLB-BATTING-STATS-2026"


def slot_times(wf):
    return [hm for hm, w, _ in ns.SLOTS if w == wf]


def delay_minutes(start, slots):
    """Minutes from the latest keeper slot at or before `start` (ET wall clock) to `start`."""
    st = start.astimezone(ET)
    best = None
    for d in (0, -1):
        day = (st + timedelta(days=d)).date()
        for h, m in slots:
            t = datetime(day.year, day.month, day.day, h, m, tzinfo=ET)
            if t <= st and (best is None or t > best):
                best = t
    return (st.astimezone(timezone.utc) - best.astimezone(timezone.utc)).total_seconds() / 60


def who(run):
    if run["event"] == "schedule":
        return "backup schedule"
    if run["event"] == "workflow_dispatch":
        return "keeper" if (run.get("triggering_actor") or {}).get("login") == "github-actions[bot]" else "by hand"
    return run["event"]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=14)
    a = ap.parse_args()
    since = datetime.now(timezone.utc) - timedelta(days=a.days)
    print(f"Daily job start times vs keeper slots, last {a.days} days\n")
    print(f"{'job':28s} {'runs':>4s}  {'median':>7s} {'p90':>7s} {'worst':>7s}  started by")
    for wf in sorted({w for _, w, _ in ns.SLOTS}):
        out = subprocess.run(["gh", "api", f"repos/{REPO}/actions/workflows/{wf}/runs?per_page=100&created=>={since:%Y-%m-%d}"],
                             capture_output=True, text=True)
        runs = json.loads(out.stdout or "{}").get("workflow_runs", [])
        runs = [r for r in runs if r.get("conclusion") != "skipped"]
        if not runs:
            print(f"{wf:28s} {0:4d}  (no runs)")
            continue
        d = sorted(delay_minutes(datetime.fromisoformat(r["run_started_at"].replace("Z", "+00:00")), slot_times(wf)) for r in runs)
        q = lambda p: d[min(len(d) - 1, int(p * len(d)))]
        by = {}
        for r in runs:
            by[who(r)] = by.get(who(r), 0) + 1
        print(f"{wf:28s} {len(d):4d}  {q(.5):5.0f} m {q(.9):5.0f} m {d[-1]:5.0f} m  "
              + ", ".join(f"{k} {v}" for k, v in sorted(by.items())))


if __name__ == "__main__":
    main()

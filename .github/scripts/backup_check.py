"""Should this run do its work? Used by the first job ("check") of every daily workflow (Round H1).

The keeper starts each job on time; GitHub's own schedule stays as a backup. A backup
(schedule-triggered) run skips when this workflow already succeeded recently:
  --since today   since midnight Eastern (the once-a-day jobs)
  --since 50m     in the last 50 minutes (hourly lineups)
Runs started by the keeper or by hand always do their work.
Writes run=true|false to $GITHUB_OUTPUT.
"""
import argparse
import json
import os
import subprocess
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")


def cutoff(since, now):
    if since == "today":
        return now.astimezone(ET).replace(hour=0, minute=0, second=0, microsecond=0).astimezone(timezone.utc)
    return now - timedelta(minutes=int(since.rstrip("m")))


def decide(event, runs, now, since, this_run=None):
    """(do the work?, why). runs: [{databaseId, conclusion, createdAt}, ...] of this workflow."""
    if event != "schedule":
        return True, f"started by {event}: always runs"
    since_t = cutoff(since, now)
    done = [r for r in runs if r.get("conclusion") == "success" and str(r.get("databaseId")) != str(this_run)
            and datetime.fromisoformat(r["createdAt"].replace("Z", "+00:00")) >= since_t]
    if done:
        return False, f"backup schedule: already done (run {done[0]['databaseId']} succeeded) - skipping"
    return True, "backup schedule: not done yet - running"


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--workflow", required=True)
    ap.add_argument("--since", default="today")
    a = ap.parse_args()
    event = os.environ.get("GITHUB_EVENT_NAME", "")
    runs = []
    if event == "schedule":
        out = subprocess.run(["gh", "run", "list", "-R", os.environ["GITHUB_REPOSITORY"], "--workflow", a.workflow,
                              "-L", "40", "--json", "databaseId,conclusion,createdAt"],
                             capture_output=True, text=True, check=True).stdout
        runs = json.loads(out)
    run, why = decide(event, runs, datetime.now(timezone.utc), a.since, os.environ.get("GITHUB_RUN_ID"))
    print(why)
    with open(os.environ.get("GITHUB_OUTPUT", "/dev/null"), "a") as f:
        f.write(f"run={'true' if run else 'false'}\n")


if __name__ == "__main__":
    main()

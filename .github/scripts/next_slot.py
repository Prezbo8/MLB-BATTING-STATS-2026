"""The keeper's timetable (Round H1). Eastern time, so daylight saving needs no edits.

    python3 .github/scripts/next_slot.py              -> "<seconds> <workflow file> <label>"
    python3 .github/scripts/next_slot.py --workflows  -> every workflow the keeper starts
    python3 .github/scripts/next_slot.py --seasonal   -> the six paused in the offseason (README)

GitHub's own scheduler started these jobs a median 2-3 h late (4-5 h in September 2026),
so the keeper sleeps until each time below and starts the job itself.
"""
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

ET = ZoneInfo("America/New_York")

DAILY = [  # (hour, minute) ET, workflow, what it does
    ((5, 30), "scrape_results.yml", "final scores"),
    ((6, 0), "mlb_pitcher_pipeline.yml", "pitcher pipeline"),
    ((6, 30), "scrape_team_splits.yml", "team splits"),
    ((7, 0), "scrape_player_splits.yml", "player splits"),
    ((7, 30), "scrape_rpg.yml", "runs per game"),
    ((11, 0), "report_pdf.yml", "Telegram report"),
]
LINEUPS = [((h, 17), "scrape_lineups.yml", "lineups") for h in range(11, 22)]   # 11:17 AM ... 9:17 PM
SLOTS = DAILY + LINEUPS
# Disabled in the offseason; scrape_results.yml stays on all year, so it doesn't count when the
# keeper decides whether it is the season (all six disabled = offseason = keeper stops)
SEASONAL = sorted({wf for _, wf, _ in SLOTS} - {"scrape_results.yml"})
MIN_AHEAD = timedelta(seconds=30)   # a slot less than 30 s away counts as passed


def next_slot(now):
    """(seconds until it, workflow, label) for the first slot more than 30 s after `now`."""
    now_utc = now.astimezone(timezone.utc)
    best = None
    for d in (0, 1):
        day = (now.astimezone(ET) + timedelta(days=d)).date()
        for (h, m), wf, label in SLOTS:
            t = datetime(day.year, day.month, day.day, h, m, tzinfo=ET).astimezone(timezone.utc)
            if t > now_utc + MIN_AHEAD and (best is None or t < best[0]):
                best = (t, wf, label)
    t, wf, label = best
    # subtract in UTC: two aware datetimes with the same zoneinfo subtract as wall-clock times,
    # which is an hour off across a daylight-saving change
    return int((t - now_utc).total_seconds()), wf, t.astimezone(ET).strftime("%a %-I:%M %p %Z") + " " + label


if __name__ == "__main__":
    if "--workflows" in sys.argv:
        print(" ".join(sorted({wf for _, wf, _ in SLOTS})))
    elif "--seasonal" in sys.argv:
        print(" ".join(SEASONAL))
    else:
        secs, wf, label = next_slot(datetime.now(timezone.utc))
        print(secs, wf, label)

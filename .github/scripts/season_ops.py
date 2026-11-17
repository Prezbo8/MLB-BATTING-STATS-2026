"""Season start automation: what season_start.yml should do today (pure date logic + CLI).

  python3 .github/scripts/season_ops.py plan --started <0|1> --checked <0|1> [--stage auto|start|week]
      -> prints "stage=<start|week|none>", "year=<YYYY>", "opening=<YYYY-MM-DD>" lines

  stage start: the day after Opening Day (2027: Mar 26) or later, not done yet this year
               -> re-enable the six seasonal workflows, start the keeper, open the reminder issue
  stage week : a week after that (Opening Day + 8, 2027: Apr 2) or later, start done, check not
               done -> schedule report + re-record the browser-test data

Opening Day comes from MLB's API via scripts/season.py (fallback Mar 20), so every season works
without editing a date. "Done" is recorded as a GitHub issue per stage per year.
"""
import argparse
import os
import sys
from datetime import date, timedelta

START_AFTER = timedelta(days=1)    # the README's rule: unpause on the day after Opening Day
WEEK_AFTER = timedelta(days=8)     # one week of live runs later
WINDOW = timedelta(days=30)        # only act within 30 days of Opening Day: never unpause in the fall


def stage(today, opening, started, checked):
    """'start' | 'week' | None for today (pure; tested)."""
    if not (opening <= today <= opening + WINDOW):
        return None
    if not started and today >= opening + START_AFTER:
        return "start"
    if started and not checked and today >= opening + WEEK_AFTER:
        return "week"
    return None


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("cmd", choices=["plan"])
    ap.add_argument("--started", type=int, default=0)
    ap.add_argument("--checked", type=int, default=0)
    ap.add_argument("--stage", default="auto", choices=["auto", "start", "week"])
    a = ap.parse_args()
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "..", "scripts"))
    import season                                     # Opening Day from MLB's API, fallback Mar 20
    today = date.fromisoformat(os.environ["SEASON_TODAY"]) if os.environ.get("SEASON_TODAY") else date.today()
    opening = season.opening_day(today.year)
    st = a.stage if a.stage != "auto" else (stage(today, opening, bool(a.started), bool(a.checked)) or "none")
    print(f"stage={st}")
    print(f"year={today.year}")
    print(f"opening={opening.isoformat()}")


if __name__ == "__main__":
    main()

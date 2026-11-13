"""
The current MLB season, worked out fresh every run, for every year to come.

Rule: from the day AFTER this year's Opening Day, the season is this year;
until then it is still last year (the offseason keeps showing the finished
season, and the 1 AM ET run on Opening Day itself doesn't hit an empty season).
Opening Day comes from MLB's own schedule (it includes international openers,
e.g. Tokyo 2025-03-18). If MLB's API can't be reached (or hasn't published the
year yet), the season does NOT switch until April 10 — switching too early
would scrape an empty season — while date windows still start on March 20.

MLB_TODAY=YYYY-MM-DD in the environment pretends today is that date (testing).
"""

import os
from datetime import date

import requests

FALLBACK_MONTH_DAY = (3, 20)          # data windows start here if MLB's API is down
FALLBACK_SWITCH    = (4, 10)          # ...but the season only switches after this


def today():
    override = os.environ.get("MLB_TODAY")
    return date.fromisoformat(override) if override else date.today()


def _opening_day_from_mlb(year):
    """MLB's regularSeasonStartDate for `year`, or None if it can't be read."""
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/seasons/{year}",
                         params={"sportId": 1}, timeout=20)
        r.raise_for_status()
        return date.fromisoformat(r.json()["seasons"][0]["regularSeasonStartDate"])
    except Exception as e:
        print(f"   ⚠️  Opening Day lookup for {year} failed ({e})")
        return None


def opening_day(year):
    """First regular-season game date of `year` (falls back to Mar 20)."""
    return _opening_day_from_mlb(year) or date(year, *FALLBACK_MONTH_DAY)


def regular_season_end(year):
    """Last regular-season game date of `year` (falls back to Oct 5)."""
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/seasons/{year}",
                         params={"sportId": 1}, timeout=20)
        r.raise_for_status()
        return date.fromisoformat(r.json()["seasons"][0]["regularSeasonEndDate"])
    except Exception as e:
        print(f"   ⚠️  Season-end lookup for {year} failed ({e}) — using Oct 5")
        return date(year, 10, 5)


def current_season(on=None):
    on = on or today()
    switch = _opening_day_from_mlb(on.year) or date(on.year, *FALLBACK_SWITCH)
    return on.year if on > switch else on.year - 1


SEASON       = current_season()
SEASON_START = opening_day(SEASON)          # date the current season began

"""
The current MLB season, worked out fresh every run, for every year to come.

Rule: from the day AFTER this year's Opening Day, the season is this year;
until then it is still last year (the offseason keeps showing the finished
season, and the 1 AM ET run on Opening Day itself doesn't hit an empty season).
Opening Day comes from MLB's own schedule (it includes international openers,
e.g. Tokyo 2025-03-18). If MLB's API can't be reached, Opening Day is taken as
March 20 of the same year.

MLB_TODAY=YYYY-MM-DD in the environment pretends today is that date (testing).
"""

import os
from datetime import date

import requests

FALLBACK_MONTH_DAY = (3, 20)


def today():
    override = os.environ.get("MLB_TODAY")
    return date.fromisoformat(override) if override else date.today()


def opening_day(year):
    """First regular-season game date of `year` (falls back to Mar 20)."""
    try:
        r = requests.get(f"https://statsapi.mlb.com/api/v1/seasons/{year}",
                         params={"sportId": 1}, timeout=20)
        r.raise_for_status()
        return date.fromisoformat(r.json()["seasons"][0]["regularSeasonStartDate"])
    except Exception as e:
        print(f"   ⚠️  Opening Day lookup for {year} failed ({e}) — using Mar 20")
        return date(year, *FALLBACK_MONTH_DAY)


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
    return on.year if on > opening_day(on.year) else on.year - 1


SEASON       = current_season()
SEASON_START = opening_day(SEASON)          # date the current season began

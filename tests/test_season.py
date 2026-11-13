"""Season rule (scripts/season.py) and the month windows built from it."""

from datetime import date

import pytest

import season


@pytest.mark.parametrize("today, expected", [
    ("2027-02-01", 2026),   # offseason keeps last season
    ("2027-03-24", 2026),
    ("2027-03-25", 2026),   # Opening Day itself: no games scraped yet at 1 AM
    ("2027-03-26", 2027),   # day after Opening Day: new season
    ("2027-07-04", 2027),
    ("2027-12-31", 2027),
])
def test_season_switches_the_day_after_opening_day(net, today, expected):
    net.season_dates[2027] = ("2027-03-25", "2027-09-26")
    assert season.current_season(date.fromisoformat(today)) == expected


def test_international_opener_counts_as_opening_day(net):
    net.season_dates[2025] = ("2025-03-18", "2025-09-28")      # Tokyo Series
    assert season.current_season(date(2025, 3, 18)) == 2024
    assert season.current_season(date(2025, 3, 19)) == 2025


def test_mlb_api_down_falls_back_to_march_20(net):
    net.mlb_down = True
    assert season.opening_day(2029) == date(2029, 3, 20)
    assert season.current_season(date(2029, 3, 20)) == 2028
    assert season.current_season(date(2029, 3, 21)) == 2029
    assert season.regular_season_end(2029) == date(2029, 10, 5)


def test_regular_season_end_comes_from_mlb(net):
    net.season_dates[2026] = ("2026-03-25", "2026-09-27")
    assert season.regular_season_end(2026) == date(2026, 9, 27)


@pytest.mark.parametrize("module", ["scrape_team_splits_history", "scrape_player_splits_history"])
@pytest.mark.parametrize("today, last", [
    ("2026-11-13", ("2026-09", "2026-9-1", "2026-9-27")),   # offseason: no October
    ("2026-09-24", ("2026-09", "2026-9-1", "2026-9-24")),   # mid-September: partial month
    ("2026-04-15", ("2026-04", "2026-4-1", "2026-4-15")),
])
def test_history_months_stop_at_the_last_regular_season_day(net, monkeypatch, module, today, last):
    mod = __import__(module)
    monkeypatch.setenv("MLB_TODAY", today)
    windows = mod.get_month_windows()
    assert windows[0][0] == "2026-03"
    assert windows[-1] == last
    assert not any(w[0] == "2026-10" for w in windows)

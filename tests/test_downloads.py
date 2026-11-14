"""Round G5: the pages download what they show, not whole tables."""

import os
import re

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
html = lambda p: open(os.path.join(REPO, p)).read()


def test_pitcher_splits_load_one_season_and_split_at_a_time():
    d, m = html("index.html"), html("mobile.html")
    # up front, only which seasons exist (and how many rated rows each has)
    assert "sbFetchAll(PS_TABLE, 'select=period&season_score=not.is.null')" in d
    assert "sbFetchSel(PSPL_TABLE, 'select=period&season_score=not.is.null')" in m
    # the full columns only ever with a season and split filter (or one pitcher for Compare)
    assert re.search(r"\$\{PS_SELECT\}&period=eq\.\$\{encodeURIComponent\(_period\)\}&split=eq\.", d)
    assert re.search(r"\$\{PSPL_SELECT\}&period=eq\.\$\{encodeURIComponent\(period\)\}&split=eq\.", m)
    assert "sbFetchAll(PS_TABLE, PS_SELECT)" not in d and "sbFetchSel(PSPL_TABLE, PSPL_SELECT)" not in m


def test_split_scores_are_matched_by_player_id():
    d = html("index.html")
    assert "prevSznMap[r.playerid || r.name]" in d and "prevCareerMap[r.playerid || r.name]" in d


def test_home_page_loads_only_the_core_batter_lines():
    for page in ("index.html", "mobile.html"):
        s = html(page)
        assert "date_range=eq.season&split=in.(no_split,vs_lhp,vs_rhp)" in s
        assert "renderTodayEdges" in s and re.search(r"Promise\.all\(\[ensureLineups\(\), ensurePlayerCore\(\)", s)


def test_batter_downloads_are_sorted():
    """Paging through an unsorted table can repeat a row and skip another."""
    m = html("mobile.html")
    assert "const PLAYER_ORDER = '&order=name.asc,tm.asc,split.asc,date_range.asc';" in m
    assert "+ PLAYER_ORDER" in m


def test_no_repeat_schedule_or_lineup_downloads():
    d, m = html("index.html"), html("mobile.html")
    for s in (d, m):
        assert "(_slateP = _slateP || loadSlateOnce())" in s
    assert "lineupsStale()" in d and "Date.now() - lineupLastFetched > 5 * 60e3" in d

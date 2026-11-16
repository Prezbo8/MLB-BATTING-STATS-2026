"""Round G5 download rules. The behaviour (home page size, one pitcher-split slice at a time,
core batter lines on the home page, sorted batter downloads, one schedule download) is checked
in a real browser by tests/test_browser.py (Round H4). What stays here can't be seen from the
network: which key the split-score columns use."""

import os

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_split_scores_are_matched_by_player_id():
    """Two pitchers can share a name (two Jose Rodriguez): the previous-season and career
    columns of the pitcher Splits table must look scores up by player id."""
    d = open(os.path.join(REPO, "index.html")).read()
    assert "prevSznMap[r.playerid || r.name]" in d and "prevCareerMap[r.playerid || r.name]" in d

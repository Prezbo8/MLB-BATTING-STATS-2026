"""The edge math in scripts/backtest_edges.py, and that both pages use the same numbers."""

import json
import os
import re

import pytest

import backtest_edges as be

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def test_last_season_line_leans_on_100():
    assert be.batter_prior(120, 500) == pytest.approx((120 * 500 + 100 * 100) / 600)
    assert be.batter_prior(None, 0) == 100


def test_batter_vs_hand_blends_split_overall_and_last_season():
    prior = (120 * 500 + 100 * 100) / 600                       # 116.67
    overall = (130 * 200 + prior * 60) / 260                     # 126.92
    want = (150 * 80 + overall * 100) / 180                      # 137.18
    assert be.batter_vs_hand(130, 200, 150, 80, 120, 500) == pytest.approx(want)


def test_rookie_without_split_leans_on_100_and_unknown_is_none():
    assert be.batter_vs_hand(90, 40, 0, 0, None, 0) == pytest.approx((90 * 40 + 100 * 60) / 100)
    assert be.batter_vs_hand(0, 0, 0, 0, None, 0) is None
    assert be.batter_vs_hand(0, 0, 0, 0, 110, 300) == pytest.approx(be.batter_prior(110, 300))


def test_lineup_is_weighted_by_batting_slot_and_skips_unknowns():
    assert be.lineup_wrc([(1, 100), (2, None), (9, 120)]) == round((4.6 * 100 + 3.8 * 120) / 8.4)
    assert be.lineup_wrc([(1, None)]) is None


def test_edge_and_labels():
    z, lp, sp = be.edge_z(112, 47, 72.2, 26.4)
    assert lp == pytest.approx(1.0) and sp == pytest.approx(25.2 / 26.4) and z == pytest.approx(lp + sp)
    assert be.edge_z(100, None, 72.2, 26.4)[0] == 0
    assert [be.edge_label(v) for v in (1.2, 0.5, 0.0, -0.5, -1.2)] == \
        ["Strong: bats", "Lean: bats", "Even", "Lean: starter", "Strong: starter"]


def test_starter_scale_needs_twenty_starters():
    assert be.starter_scale([70.0] * 19) == (50.0, 25.0)
    mid, sd = be.starter_scale([60.0] * 10 + [80.0] * 10)
    assert mid == 80.0 and sd == pytest.approx(((10 * 20 ** 2) / 20) ** 0.5)


def test_site_labels_come_from_the_starter_score():
    assert [be.site_label(v) for v in (97.0, 90.0, 89.9, 73.5, 60.0, None, float("nan"))] == \
        ["Tough matchup", "Tough matchup", "Slight edge: starter", "Slight edge: starter",
         "No clear edge", "No clear edge", "No clear edge"]


def test_site_labels_separate_runs_in_every_season():
    """Round H5: graded on 2023-2026 with the scores the site shows (in-season), the site's
    cut-offs must split offenses in order -- Tough < Slight < the rest -- every season, and the
    results file must describe the cut-offs the site uses."""
    r = json.load(open(os.path.join(REPO, "edges_results.json")))
    assert r["site_cutoffs"] == {"tough": be.STARTER_TOUGH, "slight": be.STARTER_SLIGHT}
    assert len(r["site_bands_by_season"]) >= 4
    for season, bands in [("all", r["site_bands"])] + list(r["site_bands_by_season"].items()):
        b = {x["band"]: x for x in bands}
        assert b["tough"]["runs"] < b["slight"]["runs"] < b["none"]["runs"], season
    tough = {x["band"]: x for x in r["site_bands"]}["tough"]
    assert tough["ci"][1] < {x["band"]: x for x in r["site_bands"]}["none"]["ci"][0]   # clearly apart


@pytest.mark.parametrize("page", ["index.html", "mobile.html"])
def test_pages_use_the_same_numbers_as_the_backtest(page):
    # the matchup math lives in assets/js/shared.js (Round G3); PA_BY_SLOT stays in each page
    html = open(os.path.join(REPO, page)).read() + open(os.path.join(REPO, "assets", "js", "shared.js")).read()
    num = lambda pat: float(re.search(pat, html).group(1))
    assert num(r"const SPLIT_PRIOR_PA = (\d+)") == be.SPLIT_PRIOR_PA
    assert num(r"PRIOR_PA = (\d+), LAST_SEASON_SHRINK_PA") == be.PRIOR_PA
    assert num(r"LAST_SEASON_SHRINK_PA = (\d+)") == be.LAST_SEASON_SHRINK_PA
    slots = re.search(r"const PA_BY_SLOT = \[([^\]]+)\]", html).group(1)
    assert [float(x) for x in slots.split(",")] == be.PA_BY_SLOT
    assert num(r"const STARTER_TOUGH = (\d+(?:\.\d+)?)") == be.STARTER_TOUGH
    assert num(r"STARTER_SLIGHT = (\d+(?:\.\d+)?);") == be.STARTER_SLIGHT
    assert "Tough matchup" in html and "(lv.wrc - 100) / 12" not in html   # label = starter score only
    assert "starterScale" not in html                                     # no separate site yardstick



# ── Round H5: the multi-season pieces ───────────────────────────────────────

def test_month_cutoffs_never_look_ahead(monkeypatch):
    from datetime import date
    monkeypatch.setattr(be, "season_dates", lambda y: (date(2025, 3, 27), date(2025, 9, 28)))
    c = be.cutoffs(2025, [3, 4, 5, 9])
    assert c[3] is None and c[4] == date(2025, 3, 31) and c[5] == date(2025, 4, 30) and c[9] == date(2025, 8, 31)


def _games():
    import pandas as pd
    rows = []
    for pk, home, away, hr, ar, hl, al in [(1, "COL", "SDP", 9, 7, 100, 110), (2, "SDP", "COL", 3, 2, 110, 100),
                                         (3, "COL", "LAD", 8, 6, 100, 120), (4, "SDP", "LAD", 2, 4, 110, 120)]:
        rows.append({"season": 2025, "game_pk": pk, "home": home, "offense": away, "runs": ar, "lineup": al, "sp_in": 60.0, "month": 5})
        rows.append({"season": 2025, "game_pk": pk, "home": home, "offense": home, "runs": hr, "lineup": hl, "sp_in": 80.0, "month": 5})
    return pd.DataFrame(rows)


def test_park_adjustment_divides_by_the_parks_run_rate():
    df = _games()
    adj = be.park_adjusted(df)
    # COL games average 15 runs, SDP 5.5, league 10.25 -> COL factor 1.46, SDP 0.54
    col = df["home"] == "COL"
    assert adj[col].mean() < df.loc[col, "runs"].mean() and adj[~col].mean() > df.loc[~col, "runs"].mean()


def test_same_game_pairs_away_minus_home():
    sg = be.same_game(_games()).set_index("game_pk")
    assert sg.loc[1, "d_runs"] == 7 - 9 and sg.loc[1, "d_lineup"] == 110 - 100 and sg.loc[1, "d_sp"] == 60 - 80
    assert len(sg) == 4

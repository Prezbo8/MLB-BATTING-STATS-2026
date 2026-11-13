"""The edge math in scripts/backtest_edges.py, and that both pages use the same numbers."""

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


def test_site_labels_come_from_the_starter_part():
    assert [be.site_label(v) for v in (-1.2, -0.5, 0.0, 1.5)] == \
        ["Tough matchup", "Slight edge: starter", "No clear edge", "No clear edge"]


@pytest.mark.parametrize("page", ["index.html", "mobile.html"])
def test_pages_use_the_same_numbers_as_the_backtest(page):
    html = open(os.path.join(REPO, page)).read()
    num = lambda pat: float(re.search(pat, html).group(1))
    assert num(r"const SPLIT_PRIOR_PA = (\d+)") == be.SPLIT_PRIOR_PA
    assert num(r"PRIOR_PA = (\d+), LAST_SEASON_SHRINK_PA") == be.PRIOR_PA
    assert num(r"LAST_SEASON_SHRINK_PA = (\d+)") == be.LAST_SEASON_SHRINK_PA
    slots = re.search(r"const PA_BY_SLOT = \[([^\]]+)\]", html).group(1)
    assert [float(x) for x in slots.split(",")] == be.PA_BY_SLOT
    assert num(r"const STARTER_STRONG = (\d+(?:\.\d+)?)") == be.STARTER_STRONG
    assert num(r"STARTER_LEAN = (\d+(?:\.\d+)?);") == be.STARTER_LEAN
    assert "Tough matchup" in html and "(lv.wrc - 100) / 12" not in html   # label = starter part only

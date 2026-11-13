"""Building blocks of scripts/backtest.py (the full run needs downloads; not run here)."""

import numpy as np
import pandas as pd
import pytest

import backtest as bt


@pytest.mark.parametrize("ip, expected", [
    ("162.2", 162 + 2 / 3), ("6.1", 6 + 1 / 3), (7.0, 7.0), ("0.2", 2 / 3), (None, np.nan),
])
def test_innings_notation(ip, expected):
    got = bt.ip_to_float(ip)
    assert (np.isnan(got) and np.isnan(expected)) or got == pytest.approx(expected)


@pytest.mark.parametrize("season", bt.SEASONS)
def test_preseason_score_never_sees_the_season_it_predicts(season):
    used = bt.seasons_used(season)
    assert used and max(used) == season - 1 and min(used) == bt.FIRST


def test_spearman_orientation():
    good = pd.Series([1, 2, 3, 4, 5] * 3, dtype=float)
    runs = pd.Series([5, 4, 3, 2, 1] * 3, dtype=float)       # better pitcher, fewer runs
    assert bt.spearman(good, -runs) == pytest.approx(1.0)
    assert bt.spearman(good, runs) == pytest.approx(-1.0)


def test_bootstrap_keeps_a_pitchers_starts_together():
    df = pd.DataFrame({"season": [2025] * 40,
                       "player_key": np.repeat(list("abcdefgh"), 5),
                       "composite": np.repeat(np.arange(8.0), 5),
                       "prior_ERA": np.repeat(np.arange(8.0)[::-1], 5),
                       "prior_SIERA": np.repeat(np.arange(8.0)[::-1], 5),
                       "team_ra": np.repeat(np.arange(8.0)[::-1], 5) + np.tile(np.arange(5.0), 8) * 0.01})
    old = bt.BOOT
    bt.BOOT = 50
    try:
        ci, diff = bt.bootstrap(df, "team_ra", [p for p in bt.PREDICTORS if p[0] in
                                                ("composite", "prior_ERA", "prior_SIERA")], unit="player_key")
    finally:
        bt.BOOT = old
    lo, hi = ci["composite"]
    assert 0.8 < lo <= hi <= 1.0

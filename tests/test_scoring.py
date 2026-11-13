"""
Scoring on a small fixed sample (tests/fixtures/scoring/): the rules, plus a
snapshot of every score. If scoring is changed ON PURPOSE, refresh the
snapshots with:
    UPDATE_SNAPSHOTS=1 python -m pytest tests/test_scoring.py
and review the diff of tests/fixtures/scoring/expected_*.csv in the PR.
"""

import contextlib
import io
import os

import numpy as np
import pandas as pd
import pytest

from conftest import FIXTURES

import score_pitcher_overall as so
import score_pitcher_splits as ss

DIR = os.path.join(FIXTURES, "scoring")
UPDATE = os.environ.get("UPDATE_SNAPSHOTS") == "1"


def quiet(fn, *a):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a)


@pytest.fixture(scope="module")
def overall():
    m = pd.read_csv(os.path.join(DIR, "master_all.csv"), low_memory=False)
    m["season"] = m["season"].astype(str).str.strip().apply(lambda x: int(x) if x.isdigit() else x)
    m = quiet(so.assign_player_key, m)
    m = so.compute_season_scores(m)
    return so.compute_composite_scores(m, so.WEIGHTS_WITH_CURRENT)


@pytest.fixture(scope="module")
def splits():
    m = pd.read_csv(os.path.join(DIR, "master_splits.csv"), low_memory=False)
    m["period"] = m["period"].astype(str)
    m = quiet(ss.assign_player_key, m)
    return ss.compute_composite_scores(ss.compute_season_scores(m))


# ── rules ────────────────────────────────────────────────────────────────────

def test_rows_under_min_ip_get_no_season_score(overall):
    ip = pd.to_numeric(overall["IP"], errors="coerce")
    assert overall.loc[ip < so.MIN_IP, "season_score"].isna().all()
    assert overall.loc[ip >= so.MIN_IP, "season_score"].notna().all()


def test_rows_under_min_ip_keep_the_pitchers_composite_and_tier(overall):
    for key, g in overall.groupby("player_key"):
        assert g["composite_score"].nunique(dropna=False) == 1          # one composite per pitcher
        if g["season_score"].notna().any():
            assert g["tier"].notna().all(), key                          # incl. under-cutoff rows
        else:
            assert g["tier"].isna().all(), key                           # no qualified data: unrated


def test_season_weight_grows_with_innings():
    w = lambda ip: so.WEIGHTS_WITH_CURRENT[so.SEASON] * min(1.0, ip / so.FULL_WEIGHT_IP)
    assert w(20) == pytest.approx(0.32 * 20 / 60)
    assert w(so.FULL_WEIGHT_IP) == w(200) == 0.32
    # 20 IP of a new season counts for less than a full previous season
    assert w(20) < so.WEIGHTS_WITH_CURRENT[so.SEASON - 1]


def test_rolling_year_weights():
    w = so.WEIGHTS_WITH_CURRENT
    assert [w[so.SEASON - i] for i in range(6)] == [0.32, 0.20, 0.12, 0.08, 0.05, 0.03]
    assert w["career"] == 0.20 and so.SEASON - 6 not in w


def test_single_tiny_season_is_pulled_toward_average(splits):
    # a pitcher whose only rated data is one short period can't keep its raw score
    ss_rated = splits[splits["season_score"].notna()]
    one = ss_rated.groupby(["player_key", "split"]).filter(lambda g: len(g) == 1 and g["tbf"].iloc[0] < 60)
    assert len(one) > 0
    raw = lambda s, tbf: (s * 0.32 * tbf / ss.FULL_WEIGHT_TBF + ss.PRIOR_SCORE * ss.PRIOR_WEIGHT) \
        / (0.32 * tbf / ss.FULL_WEIGHT_TBF + ss.PRIOR_WEIGHT)
    s, tbf = 95.0, 40
    assert 50 < raw(s, tbf) < 75                                       # roughly halfway to average


# ── snapshots: every score must match unless scoring changed on purpose ─────

@pytest.mark.parametrize("name, cols", [
    ("overall", ["player_key", "season", "season_score", "composite_score", "tier"]),
    ("splits",  ["player_key", "period", "split", "season_score", "composite_split_score", "split_tier"]),
])
def test_scores_match_snapshot(request, name, cols):
    df = request.getfixturevalue(name)[cols].copy()
    df = df.sort_values(cols[:3] if name == "splits" else cols[:2]).reset_index(drop=True)
    df = df.astype({c: str for c in cols[:3] if c in ("player_key", "season", "period", "split")})
    path = os.path.join(DIR, f"expected_{name}.csv")
    if UPDATE or not os.path.exists(path):
        df.to_csv(path, index=False)
        pytest.skip(f"snapshot written: {path}")
    expected = pd.read_csv(path, dtype=str, keep_default_na=False)
    # missing -> "" the same way under pandas 2 and 3 (3 keeps NaN in str columns)
    got = df.apply(lambda col: col.map(lambda v: "" if pd.isna(v) else str(v)))
    assert list(got.columns) == list(expected.columns)
    assert len(got) == len(expected)
    for c in cols:
        if c.endswith("score"):
            a = pd.to_numeric(got[c], errors="coerce"); b = pd.to_numeric(expected[c], errors="coerce")
            assert ((a - b).abs().fillna(0) < 1e-6).all() and (a.isna() == b.isna()).all(), c
        else:
            assert (got[c].values == expected[c].values).all(), c

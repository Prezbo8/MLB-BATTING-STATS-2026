"""Round H3: every historical pitcher file carries FanGraphs player ids, so same-name pitchers
(two Logan Allens) can't be split into a separate 'ghost' by score_pitcher_overall.py."""

import glob
import os

import pandas as pd
import pytest

import add_historical_ids as ahi
import score_pitcher_overall as spo

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
HIST = sorted(glob.glob(os.path.join(REPO, "data", "pitcher", "historical", "*.csv")))


def test_there_are_45_historical_files():
    assert len(HIST) == 45      # 9 stat tabs x 2021-2025


@pytest.mark.parametrize("path", HIST, ids=os.path.basename)
def test_every_row_has_a_player_id_right_after_team(path):
    df = pd.read_csv(path)
    cols = list(df.columns)
    assert cols.index("playerid") == cols.index("Team") + 1
    assert df["playerid"].notna().all()


def test_ids_agree_with_the_2026_and_career_files():
    known = ahi.known_ids()
    for path in HIST:
        df = pd.read_csv(path, usecols=["Name", "playerid"])
        for name, pid in zip(df["Name"], df["playerid"].map(ahi.norm_id)):
            k = known.get(name)
            if k and len(k) == 1:
                assert pid in k, (os.path.basename(path), name, pid, k)


def test_same_name_pitchers_stay_apart():
    """The scorer keys rows by id; with ids everywhere no row falls back to its name."""
    rows = []
    for path in glob.glob(os.path.join(REPO, "data", "pitcher", "historical", "Stats_Standard*.csv")) + \
            glob.glob(os.path.join(REPO, "data", "pitcher", "Stats_Standard*.csv")):
        d = pd.read_csv(path, usecols=["Name", "Team", "playerid"]).rename(columns={"Name": "name"})
        rows.append(d)
    keyed = spo.assign_player_key(pd.concat(rows, ignore_index=True))
    allen = keyed[keyed["name"] == "Logan Allen"]
    assert len(allen) and allen["player_key"].astype(str).str.fullmatch(r"\d+").all()
    assert set(allen["player_key"].astype(str)) <= {"27589", "18555"}


def test_suffix_names_still_match():
    assert ahi.bare("Lou Trivino III") == "Lou Trivino" and ahi.bare("Bobby Witt Jr.") == "Bobby Witt"

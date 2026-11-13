"""
Dry runs of every scraper's main() against saved responses (tests/fixtures/),
plus the safety guards. Nothing leaves the machine: Supabase swaps and GitHub
pushes are recorded by the `net` fixture (conftest.py).
"""

import glob
import os
import shutil
import sys

import pandas as pd
import pytest

from conftest import REPO, fixture

MAX_SWAP_ROWS = 3000      # a 9.5k-row swap hit Supabase's statement timeout


def run_main(module, entry="main", argv=None):
    mod = __import__(module)
    old = sys.argv
    sys.argv = [module] + (argv or [])
    try:
        getattr(mod, entry)()
    finally:
        sys.argv = old
    return mod


def swaps_for(net, table):
    return [s for s in net.swaps if s["p_table"] == table]


def check_swap_sizes(swaps):
    for s in swaps:
        assert 0 < len(s["p_rows"]) <= MAX_SWAP_ROWS, (s["p_table"], s["p_where_val"], len(s["p_rows"]))


# ── one-season guard (audit C1) ──────────────────────────────────────────────

def test_team_splits_refuses_a_range_spanning_two_seasons(net, monkeypatch):
    import scrape_team_splits as t
    one = t.fetch_split([], "2026-3-25", "2026-11-13")
    two = one + [dict(r, Season=2025) for r in one]              # 60 rows, 2 per team
    monkeypatch.setattr(t, "fetch_split", lambda *a: two)
    with pytest.raises(ValueError, match="one row per Tm"):
        t.scrape_table([], "2025-3-18", "2026-11-13", "no_split__season")


def test_player_splits_refuses_duplicate_players(net, monkeypatch):
    import scrape_player_splits as p
    rows = p.fetch_split([], "2026-3-25", "2026-11-13")
    monkeypatch.setattr(p, "fetch_split", lambda *a: rows + rows[:3])
    with pytest.raises(ValueError, match="one row per playerid"):
        p.scrape_table([], "2026-3-25", "2026-11-13", "no_split__season")


# ── batting scrapers ─────────────────────────────────────────────────────────

def test_team_splits_main(net, workdir):
    run_main("scrape_team_splits")
    (swap,) = swaps_for(net, "fangraphs_splits")
    assert len(swap["p_rows"]) == 36 * 30                         # 9 splits × 4 ranges × 30 teams
    assert {r["season"] for r in swap["p_rows"]} == {2026}


def test_team_splits_goes_red_when_supabase_fails(net, workdir):
    net.fail["rpc/replace_rows"] = 500
    with pytest.raises(SystemExit) as e:
        run_main("scrape_team_splits")
    assert e.value.code == 1


def test_player_splits_main(net, workdir):
    run_main("scrape_player_splits")
    (swap,) = swaps_for(net, "fangraphs_player_splits")
    assert len(swap["p_rows"]) == 20 * 60                         # 5 splits × 4 ranges × 60 players


def test_runs_per_game_main(net, workdir):
    run_main("scrape_rpg")
    (swap,) = swaps_for(net, "mlb_rpg")
    assert len(swap["p_rows"]) == 30
    assert {"szn_current", "szn_prev"} <= set(swap["p_rows"][0])


@pytest.mark.parametrize("module, table", [
    ("scrape_team_splits_history", "fangraphs_splits_history"),
    ("scrape_player_splits_history", "fangraphs_player_splits_history"),
])
def test_monthly_history_main_swaps_one_month_at_a_time(net, workdir, module, table):
    run_main(module)
    swaps = swaps_for(net, table)
    assert [s["p_where_val"] for s in swaps] == [f"2026-{m:02d}" for m in range(3, 10)]
    check_swap_sizes(swaps)


def test_multi_year_history_main_swaps_one_season_at_a_time(net, workdir):
    run_main("scrape_player_history_multi")
    swaps = swaps_for(net, "fangraphs_player_history")
    assert [s["p_where_val"] for s in swaps] == [str(y) for y in range(2021, 2027)]
    check_swap_sizes(swaps)


def test_results_main(net, workdir):
    run_main("scrape_results")
    df = pd.read_csv("data/results/results_2026.csv")
    assert len(df) > 0 and not df["game_pk"].duplicated().any()
    assert set(df["status"]) <= {"Final", "Completed Early", "Game Over"}


# ── pitcher pipeline, end to end (mlb_pitcher_pipeline.yml) ──────────────────

def copy_repo_data(paths):
    for p in paths:
        src = os.path.join(REPO, p)
        dst = os.path.join(os.getcwd(), p)
        os.makedirs(os.path.dirname(dst), exist_ok=True)
        if os.path.isdir(src):
            shutil.copytree(src, dst, dirs_exist_ok=True)
        else:
            shutil.copy(src, dst)


def test_pitcher_overall_chain(net, workdir):
    copy_repo_data(["data/pitcher"])
    run_main("scrape_pitcher_career")
    run_main("scrape_pitcher_season")
    saved = glob.glob("data/pitcher/*2026.csv") + glob.glob("data/pitcher/career/*_career.csv")
    assert len(saved) == 18
    # workflow step: copy every earlier season next to today's scrape
    for f in glob.glob("data/pitcher/historical/*.csv"):
        shutil.copy(f, "pitcher_data/")
    for f in glob.glob("data/pitcher/*.csv"):
        if not os.path.exists(os.path.join("pitcher_data", os.path.basename(f))):
            shutil.copy(f, "pitcher_data/")
    run_main("merge_pitcher_overall", "run")
    run_main("score_pitcher_overall", "run")
    swaps = swaps_for(net, "pitcher_scores")
    assert [s["p_where_val"] for s in swaps] == [str(y) for y in range(2021, 2027)] + ["career"]
    check_swap_sizes(swaps)
    scored = pd.read_csv("pitcher_data/scored_all.csv", low_memory=False)
    assert sum(len(s["p_rows"]) for s in swaps) == len(scored)


def test_pitcher_splits_chain(net, workdir):
    copy_repo_data([f"data/pitcher_splits_data/splits_{y}_raw.csv" for y in range(2021, 2026)])
    run_main("scrape_pitcher_splits_season", "run")
    run_main("scrape_pitcher_splits_career", "run")
    assert os.path.exists("data/pitcher_splits_data/splits_2026_raw.csv")
    run_main("merge_pitcher_splits", "run")
    run_main("score_pitcher_splits", "run")
    swaps = swaps_for(net, "pitcher_split_scores")
    pieces = [(s["p_where_val"], s["p_where_val2"]) for s in swaps]
    assert len(pieces) == len(set(pieces)) == 7 * 4             # each (period, split) once
    check_swap_sizes(swaps)
    scored = pd.read_csv("data/pitcher_splits_data/scored_splits.csv", low_memory=False)
    assert sum(len(s["p_rows"]) for s in swaps) == len(scored)


def test_splits_season_freezes_a_missing_previous_season(net, workdir, monkeypatch):
    copy_repo_data([f"data/pitcher_splits_data/splits_{y}_raw.csv" for y in range(2021, 2025)])
    import scrape_pitcher_splits_season as s
    seen = []
    real = s.scrape_season
    monkeypatch.setattr(s, "scrape_season", lambda y: (seen.append(y), real(y))[1])
    s.run()
    assert seen == [2026, 2025]
    assert os.path.exists("data/pitcher_splits_data/splits_2025_raw.csv")
    seen.clear()
    s.run()
    assert seen == [2026]                                       # frozen once only


def test_merge_goes_red_when_a_season_is_missing(net, workdir):
    copy_repo_data([f"data/pitcher_splits_data/splits_{y}_raw.csv" for y in (2021, 2022, 2024, 2025, 2026)])
    with pytest.raises(SystemExit, match="splits_2023_raw.csv"):
        run_main("merge_pitcher_splits", "run")

"""
MLB final scores -> data/results/results_<season>.csv

Replaces the old local ~/2025_MLB_RESULTS.py (plaintextsports text scraping,
which missed ~14% of games). Uses MLB's official schedule API: one request
returns every game of the season with its final score.

- Season from scripts/season.py (switches the day after Opening Day).
- Regular season + postseason only (no spring training / All-Star Game).
- Only finished games (Final / Completed Early); postponed games show up
  later on the day they are actually played.
- The whole season file is rebuilt each run, so corrections and suspended
  games resumed later are picked up. Writes the file only if it changed.

Usage: python scripts/scrape_results.py [season]   (default: current season)
"""

import csv
import os
import sys

import requests

from season import SEASON

OUT_DIR    = "data/results"
GAME_TYPES = {"R": "regular", "F": "wild card", "D": "division series",
              "L": "league championship", "W": "world series"}
FINISHED   = {"Final", "Completed Early", "Game Over"}
COLUMNS    = ["date", "game_pk", "game_type", "game_number",
              "away", "away_score", "home", "home_score", "status"]


def fetch_games(season):
    r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
                     params={"sportId": 1, "season": season,
                             "gameType": ",".join(GAME_TYPES), "hydrate": "team"},
                     timeout=60)
    r.raise_for_status()
    return [g for d in r.json().get("dates", []) for g in d.get("games", [])]


def to_rows(games):
    rows = {}
    for g in games:
        status = g["status"]["detailedState"]
        away, home = g["teams"]["away"], g["teams"]["home"]
        if status not in FINISHED or away.get("score") is None or home.get("score") is None:
            continue
        rows[g["gamePk"]] = {          # a game can be listed twice (e.g. suspended)
            "date":        g["officialDate"],
            "game_pk":     g["gamePk"],
            "game_type":   GAME_TYPES[g["gameType"]],
            "game_number": g.get("gameNumber", 1),
            "away":        away["team"]["abbreviation"],
            "away_score":  away["score"],
            "home":        home["team"]["abbreviation"],
            "home_score":  home["score"],
            "status":      status,
        }
    return sorted(rows.values(), key=lambda r: (r["date"], r["game_pk"]))


def write_if_changed(path, rows):
    tmp = path + ".tmp"
    with open(tmp, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(rows)
    if os.path.exists(path) and open(path, "rb").read() == open(tmp, "rb").read():
        os.remove(tmp)
        return False
    os.replace(tmp, path)
    return True


def main():
    season = int(sys.argv[1]) if len(sys.argv) > 1 else SEASON
    rows = to_rows(fetch_games(season))
    if not rows:
        print(f"No finished games for {season} yet — nothing written")
        return
    os.makedirs(OUT_DIR, exist_ok=True)
    path = os.path.join(OUT_DIR, f"results_{season}.csv")
    changed = write_if_changed(path, rows)
    regular = sum(r["game_type"] == "regular" for r in rows)
    print(f"{season}: {len(rows)} finished games ({regular} regular season) -> "
          f"{path} ({'updated' if changed else 'unchanged'})")


if __name__ == "__main__":
    main()

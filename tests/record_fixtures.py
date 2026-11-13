"""
Record the small saved responses the tests replay (tests/fixtures/).

Run by hand from a machine FanGraphs doesn't block (not needed in CI):
    python tests/record_fixtures.py
Each response is trimmed to a few dozen rows so the files stay small. Re-run
only when a site changes its response format.
"""

import json
import os
import sys
import time

import requests

HERE = os.path.dirname(os.path.abspath(__file__))
OUT = os.path.join(HERE, "fixtures")
sys.path.insert(0, os.path.join(HERE, "..", "scripts"))

FG_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/149.0.0.0 Safari/537.36"),
    "Referer": "https://www.fangraphs.com/leaders/splits-leaderboards",
    "Origin": "https://www.fangraphs.com",
}
SEASON, START, END = 2026, "2026-03-25", "2026-12-31"
TYPES = [8, 0, 1, 2, 23, 24, 5, 10, 36]
N = 40


def save(name, obj):
    path = os.path.join(OUT, name)
    with open(path, "w") as f:
        if isinstance(obj, str):
            f.write(obj)
        else:
            json.dump(obj, f, separators=(",", ":"))
    print(f"  {name}: {os.path.getsize(path) // 1024} KB")


def leaders(kind, t):
    if kind == "season":
        q = (f"pos=all&lg=all&type={t}&season={SEASON}&month=1000&season1={SEASON}"
             f"&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=1"
             f"&pagenum=1&pageitems=2000&startdate={START}&enddate={END}")
    else:
        q = (f"pos=all&lg=all&type={t}&month=0&ind=0&rost=0&age=0&filter=&players=0"
             f"&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate="
             f"&season2={SEASON}&season1=2017&season={SEASON}")
    r = requests.get(f"https://www.fangraphs.com/api/leaders/major-league/data?{q}",
                     headers=FG_HEADERS, timeout=120)
    r.raise_for_status()
    p = r.json()
    rows = p.get("data", p) if isinstance(p, dict) else p
    return {"data": rows[:N]}


def splits(stat_type, position, sg, group="season", start=START, end="2026-11-01"):
    payload = {
        "strPlayerId": "all", "strSplitArr": [], "strSplitArrPitch": [],
        "strGroup": group, "strPosition": position, "strType": sg,
        "strStartDate": start, "strEndDate": end, "strSplitTeams": False,
        "dctFilters": [], "strStatType": stat_type, "strAutoPt": "false",
        "arrPlayerId": [], "arrWxTemperature": None, "arrWxPressure": None,
        "arrWxAirDensity": None, "arrWxElevation": None, "arrWxWindSpeed": None,
    }
    r = requests.post("https://www.fangraphs.com/api/leaders/splits/splits-leaders",
                      json=payload, headers=FG_HEADERS, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])


def main():
    os.makedirs(OUT, exist_ok=True)
    print("FanGraphs leaders (pitcher season + career)")
    # FanGraphs returns the same ~540-field rows for every stat type; keep one
    # file per kind, trimmed to the fields the scrapers' column maps read
    import scrape_pitcher_season, scrape_pitcher_career
    keys = {"playerid", "PlayerName", "TeamNameAbb", "Name", "Team"}
    for d in scrape_pitcher_season.get_scrapers() + scrape_pitcher_career.get_stat_types():
        for k in d["column_map"]:
            keys.update(k if isinstance(k, tuple) else (k,))
    for kind in ("season", "career"):
        rows = leaders(kind, TYPES[0])["data"]
        save(f"fg_leaders_{kind}.json", {"data": [{k: v for k, v in r.items() if k in keys} for r in rows]})
        time.sleep(2)

    print("FanGraphs splits — pitchers (3 stat groups, same 40 pitchers)")
    sg = {g: splits("player", "P", g) for g in (1, 2, 3)}
    ids = [r["playerId"] for r in sg[1][:N]]
    for g, rows in sg.items():
        save(f"fg_splits_pitcher_sg{g}.json", {"data": [r for r in rows if r["playerId"] in ids]})
        time.sleep(2)

    print("FanGraphs splits — batting")
    save("fg_splits_team.json", {"data": splits("team", "B", 2)})          # all 30 teams
    time.sleep(2)
    save("fg_splits_player.json", {"data": splits("player", "B", 2)[:60]})

    print("TeamRankings runs per game")
    import scrape_rpg
    r = requests.get(scrape_rpg.URL, headers=scrape_rpg.HEADERS, timeout=60)
    r.raise_for_status()
    start = r.text.index("<table"); end = r.text.index("</table>", start) + 8
    save("teamrankings_rpg.html", "<html><body>" + r.text[start:end] + "</body></html>")

    print("MLB schedule (results): first 20 finished regular-season games + 3 postseason")
    r = requests.get("https://statsapi.mlb.com/api/v1/schedule",
                     params={"sportId": 1, "season": SEASON, "gameType": "R,F,D,L,W",
                             "hydrate": "team"}, timeout=60)
    r.raise_for_status()
    dates = r.json()["dates"]
    keep = dates[:6] + dates[-2:]
    save("mlb_schedule_season.json", {"dates": keep})


if __name__ == "__main__":
    main()

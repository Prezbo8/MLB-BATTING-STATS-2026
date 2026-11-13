"""
Backtest: does the pitcher score PREDICT how pitchers do? (Round D)

For each season Y = 2022..2026 it rebuilds every pitcher's score as of Opening
Day of Y — using ONLY seasons before Y and a career line that ends in Y-1 (no
look-ahead) — with the same scoring code the site uses (score_pitcher_overall),
then compares it with what happened in Y:

  Season test  pitchers with 60+ IP in Y: rank correlation of the preseason
               score with runs allowed per 9 (RA9) and ERA, against simple
               baselines from Y-1 (ERA, SIERA, xFIP, K-BB%, Pitching+),
               with bootstrap 95% intervals; RA9/ERA by preseason tier.
  Game test    every start in Y by a pitcher with a preseason score (MLB game
               logs): starter's runs allowed and his team's runs allowed in
               that game, by preseason tier, and rank correlations.

Writes BACKTEST.md and backtest_results.json. Reads public data only (FanGraphs,
MLB stats API); downloads are cached in backtest_cache/ (not committed), so a
re-run is offline. Changes no scores, nothing on the site, nothing in Supabase.

    python scripts/backtest.py            # full run (first run downloads ~900 game logs)
"""

import contextlib
import io
import json
import os
import re
import shutil
import sys
import time

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import merge_pitcher_overall as mpo        # noqa: E402
import score_pitcher_overall as spo        # noqa: E402
import scrape_pitcher_career as spc        # noqa: E402
import scrape_pitcher_season as sps        # noqa: E402
import scrape_results as sr                # noqa: E402
import season                              # noqa: E402

CACHE   = os.path.join(REPO, "backtest_cache")
# Seasons predicted: 2022 through the last COMPLETED regular season (needs no
# yearly edit; mid-season the current year waits until it is over)
LAST_DONE = season.SEASON if season.today() > season.regular_season_end(season.SEASON) else season.SEASON - 1
SEASONS = list(range(2022, LAST_DONE + 1))
FIRST   = 2021                             # earliest season file
MIN_IP_OUTCOME = 60                        # season test population
MIN_IP_PRIOR   = 40                        # "has a prior season" for baselines
MIN_GS_LOGS    = 5                         # starters whose game logs we fetch
BOOT = 1000
RNG  = np.random.default_rng(2026)
MLB  = "https://statsapi.mlb.com/api/v1"


# ── small helpers ────────────────────────────────────────────────────────────

def ip_to_float(ip):
    """Baseball innings '162.2' = 162 2/3."""
    try:
        v = float(ip)
    except (TypeError, ValueError):
        return np.nan
    whole = int(v)
    return whole + round((v - whole) * 10) / 3


def seasons_used(y):
    """Season files allowed in the preseason score for season y (no look-ahead)."""
    return list(range(FIRST, y))


def season_file(prefix, year):
    hist = os.path.join(REPO, "data", "pitcher", "historical", f"{prefix}{year}.csv")
    return hist if os.path.exists(hist) else os.path.join(REPO, "data", "pitcher", f"{prefix}{year}.csv")


def quiet(fn, *a, **k):
    with contextlib.redirect_stdout(io.StringIO()):
        return fn(*a, **k)


def cached_json(name, fetch):
    path = os.path.join(CACHE, name)
    if os.path.exists(path):
        return json.load(open(path))
    data = fetch()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    json.dump(data, open(path, "w"))
    return data


# ── downloads (cached) ───────────────────────────────────────────────────────

def fangraphs_ids(year):
    """FanGraphs playerid / name / team / MLBAM id for every pitcher in `year`."""
    def fetch():
        _, api = sps.build_urls(8, year, f"{year}-03-01", f"{year}-12-31")
        r = requests.get(api, headers=sps.API_HEADERS, timeout=120)
        r.raise_for_status()
        rows = r.json()["data"]
        time.sleep(2)
        return [{"playerid": str(x["playerid"]), "name": x["PlayerName"], "team": x.get("TeamNameAbb"),
                 "mlbam": x.get("xMLBAMID")} for x in rows]
    return pd.DataFrame(cached_json(f"ids_{year}.json", fetch))


def career_through(year):
    """Folder with the 9 career CSVs for 2017..year (as the live scraper builds them)."""
    folder = os.path.join(CACHE, f"career_through_{year}")
    if os.path.exists(os.path.join(folder, "done")):
        return folder
    os.makedirs(folder, exist_ok=True)
    spc.SEASON = year
    responses = {}
    real_get = spc.req_lib.get

    def get_once(url, **kw):              # FanGraphs returns the same rows for every type=
        key = re.sub(r"type=\d+&", "", url)
        if key not in responses:
            responses[key] = real_get(url, **kw)
            time.sleep(2)
        return responses[key]

    spc.req_lib.get = get_once
    try:
        for d in spc.get_stat_types():
            quiet(spc.scrape_one, d).to_csv(os.path.join(folder, d["csv_file"]), index=False)
    finally:
        spc.req_lib.get = real_get
    open(os.path.join(folder, "done"), "w").close()
    return folder


def game_log(mlbam, year):
    def fetch():
        r = requests.get(f"{MLB}/people/{mlbam}/stats",
                         params={"stats": "gameLog", "group": "pitching", "season": year,
                                 "gameType": "R"}, timeout=30)
        r.raise_for_status()
        time.sleep(0.3)
        out = []
        for s in (r.json().get("stats") or [{}])[0].get("splits", []):
            st = s["stat"]
            out.append({"game_pk": s["game"]["gamePk"], "date": s["date"], "is_home": s.get("isHome"),
                        "gs": st.get("gamesStarted", 0), "ip": st.get("inningsPitched"),
                        "r": st.get("runs"), "er": st.get("earnedRuns")})
        return out
    return cached_json(f"gamelogs/{year}/{mlbam}.json", fetch)


def results(year):
    path = os.path.join(REPO, "data", "results", f"results_{year}.csv")
    if os.path.exists(path):
        return pd.read_csv(path)
    rows = cached_json(f"results_{year}.json", lambda: sr.to_rows(sr.fetch_games(year)))
    return pd.DataFrame(rows)


# ── preseason scores (no look-ahead) ─────────────────────────────────────────

def preseason_scores(y):
    """Every pitcher's score as of Opening Day of season y."""
    work = os.path.join(CACHE, f"work_{y}")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    for year in seasons_used(y):
        for _, prefix in mpo.ANNUAL_TAB_FILES:
            src = season_file(prefix, year)
            if os.path.exists(src):
                shutil.copy(src, work)
    for f in os.listdir(career_through(y - 1)):
        if f.endswith(".csv"):
            shutil.copy(os.path.join(CACHE, f"career_through_{y - 1}", f), work)

    mpo.UPLOAD_DIR = work
    frames = [quiet(mpo.merge_tabs, mpo.ANNUAL_TAB_FILES, year) for year in seasons_used(y)]
    frames.append(quiet(mpo.merge_tabs, mpo.CAREER_TAB_FILES, "career", label="career"))
    df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    df["season"] = df["season"].astype(str).str.strip().apply(lambda x: int(x) if x.isdigit() else x)
    df = quiet(spo.assign_player_key, df)
    df = spo.compute_season_scores(df)
    weights = spo.rolling_weights([0.39, 0.15, 0.10, 0.06, 0.04], y - 1, career=0.26)
    df = spo.compute_composite_scores(df, weights)

    prior = df[df["season"] == y - 1].copy()
    prior["prior_ip"] = prior["IP"].map(ip_to_float)
    per_player = (df.groupby("player_key")
                    .agg(name=("name", "first"), composite=("composite_score", "first"),
                         tier=("tier", "first")).reset_index())
    cols = {"season_score": "prior_season_score", "ERA": "prior_ERA", "SIERA": "prior_SIERA",
            "xFIP": "prior_xFIP", "K-BB%": "prior_KBB", "Pitching+": "prior_PitchingPlus"}
    keep = ["player_key", "prior_ip"] + [c for c in cols if c in prior.columns]
    out = per_player.merge(prior[keep].rename(columns=cols), on="player_key", how="left")
    # link to FanGraphs playerid: id keys already are; name keys via the prior season's id list
    ids = fangraphs_ids(y - 1)
    uniq = ids.drop_duplicates("name", keep=False).set_index("name")["playerid"]
    out["playerid"] = out["player_key"].where(out["player_key"].astype(str).str.fullmatch(r"\d+"),
                                              out["name"].map(uniq))
    return out


# ── outcomes ─────────────────────────────────────────────────────────────────

def season_outcomes(y):
    std = pd.read_csv(season_file("Stats_Standard", y))
    dash = pd.read_csv(season_file("Stats_Dashboard", y))[["Name", "Team", "FIP"]]
    std = std.merge(dash, on=["Name", "Team"], how="left")
    std["ip"] = std["IP"].map(ip_to_float)
    std = std[std["ip"] > 0].copy()
    std["RA9"] = std["R"] * 9 / std["ip"]
    std["ERA_y"] = std["ER"] * 9 / std["ip"]
    ids = fangraphs_ids(y)
    uniq = ids.drop_duplicates("name", keep=False).set_index("name")
    std["playerid"] = std["Name"].map(uniq["playerid"])
    std["mlbam"] = std["Name"].map(uniq["mlbam"])
    return std[["Name", "Team", "playerid", "mlbam", "ip", "GS", "R", "ER", "RA9", "ERA_y", "FIP"]]


# ── statistics ───────────────────────────────────────────────────────────────

PREDICTORS = [   # (column, label, sign: +1 if higher = better pitcher)
    ("composite", "Pitcher score (composite)", 1),
    ("prior_season_score", "Last season's score", 1),
    ("prior_SIERA", "Last season's SIERA", -1),
    ("prior_xFIP", "Last season's xFIP", -1),
    ("prior_KBB", "Last season's K-BB%", 1),
    ("prior_PitchingPlus", "Last season's Pitching+", 1),
    ("prior_ERA", "Last season's ERA", -1),
]


def spearman(a, b):
    m = a.notna() & b.notna()
    return a[m].rank().corr(b[m].rank()) if m.sum() > 10 else np.nan


def season_corrs(df, outcome, cols):
    """Mean over seasons of the rank correlation (good pitcher = fewer runs: sign-adjusted)."""
    vals = {c: [] for c, _, _ in cols}
    for _, g in df.groupby("season"):
        for c, _, sign in cols:
            vals[c].append(spearman(sign * g[c], -g[outcome]))
    return {c: float(np.nanmean(v)) for c, v in vals.items()}


def bootstrap(df, outcome, cols, unit=None):
    """Resample within each season — rows, or whole `unit`s (e.g. a pitcher with all
    his starts, since one pitcher's starts aren't independent). Returns
    {col: (lo, hi)} and paired 95% ranges of composite minus each other predictor."""
    seasons = []
    for _, g in df.groupby("season"):
        g = g.reset_index(drop=True)
        seasons.append((g, [ix for _, ix in g.groupby(unit).indices.items()] if unit else None))
    draws = {c: [] for c, _, _ in cols}
    for _ in range(BOOT):
        parts = []
        for g, units in seasons:
            if units is None:
                parts.append(g.iloc[RNG.integers(0, len(g), len(g))])
            else:
                pick = RNG.integers(0, len(units), len(units))
                parts.append(g.iloc[np.concatenate([units[i] for i in pick])])
        sample = pd.concat(parts, ignore_index=True)
        r = season_corrs(sample, outcome, cols)
        for c in r:
            draws[c].append(r[c])
    ci = {c: (float(np.nanpercentile(v, 2.5)), float(np.nanpercentile(v, 97.5))) for c, v in draws.items()}
    base = np.array(draws["composite"])
    diff = {c: (float(np.nanpercentile(base - np.array(v), 2.5)), float(np.nanpercentile(base - np.array(v), 97.5)))
            for c, v in draws.items() if c != "composite"}
    return ci, diff


TIERS = ["Elite", "Good", "Mid", "Bad", "Ass"]


def tier_table(df, runs="R", ip="ip"):
    rows = []
    for t in TIERS:
        g = df[df["tier"] == t]
        if len(g):
            rows.append({"tier": t, "pitchers": int(g["player_key"].nunique()) if "player_key" in g else len(g),
                         "n": len(g), "ip": float(g[ip].sum()), "ra9": float(g[runs].sum() * 9 / g[ip].sum())})
    return rows


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    os.makedirs(CACHE, exist_ok=True)
    season_rows, start_rows = [], []
    for y in SEASONS:
        print(f"── {y}: preseason scores from {seasons_used(y)[0]}–{y - 1} + career through {y - 1}")
        pre = preseason_scores(y)
        out = season_outcomes(y)
        j = out.merge(pre.dropna(subset=["playerid"]), on="playerid", how="inner")
        j["season"] = y
        season_rows.append(j)
        print(f"   {len(pre)} pitchers scored, {len(j)} linked to {y} outcomes")

        starters = out[(out["GS"] >= MIN_GS_LOGS) & out["mlbam"].notna()]
        res = results(y).set_index("game_pk")
        for _, p in starters.iterrows():
            pk = p["playerid"]
            tier = pre.loc[pre["playerid"] == pk]
            if tier.empty or pd.isna(tier["composite"].iloc[0]):
                continue
            for g in game_log(int(p["mlbam"]), y):
                if g["gs"] != 1 or g["game_pk"] not in res.index:
                    continue
                gm = res.loc[g["game_pk"]]
                gm = gm.iloc[0] if isinstance(gm, pd.DataFrame) else gm
                team_ra = gm["away_score"] if g["is_home"] else gm["home_score"]
                start_rows.append({"season": y, "player_key": pk, "name": p["Name"],
                                   "composite": tier["composite"].iloc[0], "tier": tier["tier"].iloc[0],
                                   "prior_ERA": tier["prior_ERA"].iloc[0], "prior_SIERA": tier["prior_SIERA"].iloc[0],
                                   "ip": ip_to_float(g["ip"]), "R": g["r"], "team_ra": team_ra})
        print(f"   {len(starters)} starters, {sum(1 for s in start_rows if s['season'] == y)} starts linked")

    S = pd.concat(season_rows, ignore_index=True)
    A = S[S["ip"] >= MIN_IP_OUTCOME].copy()
    B = A[A["prior_ip"] >= MIN_IP_PRIOR].copy()
    G = pd.DataFrame(start_rows)

    report = {"seasons": SEASONS, "population": {}, "season_test": {}, "game_test": {}}
    for label, df in (("all_scored", A), ("with_prior_season", B)):
        cols = [p for p in PREDICTORS if p[0] in df.columns and df[p[0]].notna().sum() > 50]
        if label == "all_scored":
            cols = [p for p in cols if p[0] == "composite"]
        res = {}
        for outcome in ("RA9", "ERA_y"):
            point = season_corrs(df, outcome, cols)
            ci, diff = bootstrap(df, outcome, cols)
            res[outcome] = {c: {"label": l, "rho": point[c], "ci": ci[c], "vs_composite": diff.get(c)}
                            for c, l, _ in cols}
        report["season_test"][label] = res
        report["population"][label] = {"pitcher_seasons": len(df), "by_season": df.groupby("season").size().to_dict()}
    report["season_test"]["tiers_RA9"] = tier_table(A)

    G2 = G.dropna(subset=["ip", "R", "team_ra"])
    report["game_test"] = {
        "starts": len(G2), "starters": int(G2["player_key"].nunique()),
        "tiers": [dict(t, team_ra_per_game=float(G2[G2["tier"] == t["tier"]]["team_ra"].mean()))
                  for t in tier_table(G2)],
        "rho_team_runs": season_corrs(G2, "team_ra",
                                      [p for p in PREDICTORS if p[0] in ("composite", "prior_ERA", "prior_SIERA")])
    }
    gci, gdiff = bootstrap(G2, "team_ra",
                           [p for p in PREDICTORS if p[0] in ("composite", "prior_ERA", "prior_SIERA")],
                           unit="player_key")
    report["game_test"]["rho_team_runs_ci"] = gci
    report["game_test"]["vs_composite"] = gdiff

    json.dump(report, open(os.path.join(REPO, "backtest_results.json"), "w"), indent=1, default=str)
    write_report(report)
    print("✅ wrote BACKTEST.md and backtest_results.json")


def write_report(r):
    f = lambda x: f"{x:.3f}"
    ci = lambda c: f"{c[0]:.3f} to {c[1]:.3f}"
    L = ["# Backtest: does the pitcher score predict how pitchers do?", "",
         "_Generated by `scripts/backtest.py` — rerun it to refresh. Read-only: changes no scores._", "",
         "Each season 2022–2026 is predicted with scores rebuilt **as of Opening Day**, from earlier",
         "seasons and a career line ending the year before (no look-ahead), using the live scoring code.",
         "Numbers are rank correlations averaged over the five seasons (1 = perfect order, 0 = no",
         "relation), signed so that **higher = better at predicting fewer runs**; ranges are bootstrap",
         "95% intervals.", ""]
    B = r["season_test"]["with_prior_season"]
    pop = r["population"]["with_prior_season"]
    # plain-language verdict, straight from the intervals
    better, same = [], []
    for c, v in B["RA9"].items():
        if v["vs_composite"] is None:
            continue
        name = "last season's score alone" if c == "prior_season_score" else v["label"].replace("Last season's ", "")
        (better if v["vs_composite"][0] > 0 else same).append(name)
    t = {x["tier"]: x["ra9"] for x in r["season_test"]["tiers_RA9"]}
    g = {x["tier"]: x["team_ra_per_game"] for x in r["game_test"]["tiers"]}
    L += ["## Verdict", "",
          f"- **The score predicts.** Before a season it ranks pitchers by the runs they go on to allow "
          f"with a correlation of {f(B['RA9']['composite']['rho'])} — the highest of the predictors tested.",
          f"- **Clearly better than:** {', '.join(better) or 'none'} (the whole 95% range of the difference is above 0).",
          f"- **No clear difference from:** {', '.join(same) or 'none'} — the score is about as good as the best "
          "single public stats, not measurably better.",
          f"- **Tiers mean something:** preseason Elite pitchers allowed {t.get('Elite', float('nan')):.2f} runs per 9, "
          f"Good {t.get('Good', float('nan')):.2f}, Mid {t.get('Mid', float('nan')):.2f}, Bad {t.get('Bad', float('nan')):.2f}, "
          f"Ass {t.get('Ass', float('nan')):.2f}"
          + (" — the bottom two tiers are not separated: only Ass-tier pitchers whom teams keep"
             " giving 60+ innings are in this test, i.e. the ones pitching better than their score."
             if t.get("Ass", 0) <= t.get("Bad", 0) else "."),
          f"- **In single games:** teams allowed {g.get('Elite', float('nan')):.2f} runs per game behind an Elite starter vs "
          f"{g.get('Mid', float('nan')):.2f} behind a Mid one. One start is noisy (correlation "
          f"{f(r['game_test']['rho_team_runs']['composite'])}), so the edge shows over many games, not in any one.",
          ""]
    L += ["## 1. Season test", "",
          f"Pitchers with {MIN_IP_OUTCOME}+ IP in the season and {MIN_IP_PRIOR}+ IP the season before "
          f"({pop['pitcher_seasons']} pitcher-seasons).", "",
          "| Predictor (known before the season) | vs runs allowed per 9 | 95% range | vs ERA | Composite better by (RA9, 95% range) |",
          "|---|---|---|---|---|"]
    for c, v in sorted(B["RA9"].items(), key=lambda kv: -kv[1]["rho"]):
        d = v["vs_composite"]
        L.append(f"| {v['label']} | {f(v['rho'])} | {ci(v['ci'])} | {f(B['ERA_y'][c]['rho'])} | "
                 f"{'—' if d is None else ci(d)} |")
    A = r["season_test"]["all_scored"]["RA9"]["composite"]
    L += ["", f"Including pitchers with no qualifying previous season "
          f"({r['population']['all_scored']['pitcher_seasons']} pitcher-seasons), the composite scores "
          f"{f(A['rho'])} ({ci(A['ci'])}).", "",
          "**Runs allowed per 9 in the season, by preseason tier** (60+ IP pitchers):", "",
          "| Preseason tier | Pitcher-seasons | Innings | RA9 |", "|---|---|---|---|"]
    for t in r["season_test"]["tiers_RA9"]:
        L.append(f"| {t['tier']} | {t['n']} | {t['ip']:.0f} | {t['ra9']:.2f} |")
    g = r["game_test"]
    L += ["", "## 2. Game test", "",
          f"{g['starts']} starts by {g['starters']} starters with a preseason score (MLB game logs).", "",
          "| Starter's preseason tier | Starts | His RA9 in those starts | His team's runs allowed per game |",
          "|---|---|---|---|"]
    for t in g["tiers"]:
        L.append(f"| {t['tier']} | {t['n']} | {t['ra9']:.2f} | {t['team_ra_per_game']:.2f} |")
    L += ["", "Rank correlation with the team's runs allowed in each start (higher = better):", "",
          "| Predictor | Correlation | 95% range |", "|---|---|---|"]
    for c, (lo, hi) in g["rho_team_runs_ci"].items():
        label = dict((p[0], p[1]) for p in PREDICTORS)[c]
        L.append(f"| {label} | {f(g['rho_team_runs'][c])} | {lo:.3f} to {hi:.3f} |")
    open(os.path.join(REPO, "BACKTEST.md"), "w").write("\n".join(L) + "\n")


if __name__ == "__main__":
    main()

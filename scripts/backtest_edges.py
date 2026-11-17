#!/usr/bin/env python3
"""
backtest_edges.py — do the site's "Today's edges" predict runs? (read-only)

Rebuilds the edge for both offenses of every regular-season game of 2023 through
the last finished season from what was known before that game, with the same math
as the site (assets/js/shared.js), and compares it with the runs each offense
actually scored (Round H5: four seasons, in-season starter scores, ballparks
taken out).

  Lineups / starters  MLB box scores (who actually batted where, who started)
  Batter numbers      each batter's exact season-to-date FanGraphs lines up to the
                      end of the previous month (a June game sees through May 31),
                      leaning on his previous-season line exactly like the site
  Starter score       as of Opening Day (backtest.py) and as the live site would
                      have shown it that month (rebuilt from his stats so far)
  Outcome             runs the offense scored in that game

Writes EDGES.md and edges_results.json; changes no data. Downloads (FanGraphs,
MLB's API) are cached in backtest_cache/ (not committed); the first full run
takes a while, reruns are fast.

  python3 scripts/backtest_edges.py                 # full run
  python3 scripts/backtest_edges.py --report-only   # rewrite EDGES.md from edges_results.json
"""

import json
import os
import re
import sys
import time

import numpy as np
import pandas as pd
import requests

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

import backtest as bt                      # noqa: E402

SEASON = bt.LAST_DONE
MLB = bt.MLB
CACHE = bt.CACHE
BOOT = 1000

# ── the site's edge math (keep in step with index.html / mobile.html) ────────
PA_BY_SLOT = [4.6, 4.5, 4.4, 4.3, 4.2, 4.1, 4.0, 3.9, 3.8]
SPLIT_PRIOR_PA = 100          # a vs-hand split leans on the batter's overall line
PRIOR_PA = 60                 # his current-season line leans on last season's
LAST_SEASON_SHRINK_PA = 100   # last season's line leans on 100
LINEUP_DIV = 12               # wRC+ points per unit of edge
STRONG, LEAN = 1.0, 0.4       # cut-offs on |z| (the pre-G2 combined edge, graded below)
# The site's labels (Round H2): the starter's plain score. 90 = Elite tier = this backtest's
# "1 spread above the middle starter" (62.5 + 27.4); 73.5 = 0.4 spread above it.
STARTER_TOUGH, STARTER_SLIGHT = 90.0, 73.5


def batter_prior(last_wrc, last_pa):
    """Last season's line pulled toward 100; rookies (no line) start at 100."""
    if last_wrc is None or not last_pa:
        return 100.0
    return (last_wrc * last_pa + 100 * LAST_SEASON_SHRINK_PA) / (last_pa + LAST_SEASON_SHRINK_PA)


def batter_vs_hand(cur_wrc, cur_pa, split_wrc, split_pa, last_wrc, last_pa):
    """One batter's wRC+ against the starter's hand, or None (no data at all)."""
    prior = batter_prior(last_wrc, last_pa)
    cur_pa = cur_pa or 0
    overall = ((cur_wrc * cur_pa if cur_pa else 0) + prior * PRIOR_PA) / (cur_pa + PRIOR_PA)
    if split_pa:
        return (split_wrc * split_pa + overall * SPLIT_PRIOR_PA) / (split_pa + SPLIT_PRIOR_PA)
    if cur_pa or last_pa:
        return overall
    return None


def lineup_wrc(values_by_slot):
    """PA-weighted lineup wRC+ from [(slot 1-9, wrc or None), ...]."""
    w = acc = 0.0
    for slot, v in values_by_slot:
        if v is None:
            continue
        wt = PA_BY_SLOT[slot - 1] if 1 <= slot <= 9 else 4.2
        w += wt
        acc += wt * v
    return round(acc / w) if w else None


def edge_z(lineup, sp_score, scale_mid, scale_sd):
    lineup_part = (lineup - 100) / LINEUP_DIV
    starter_part = (scale_mid - sp_score) / scale_sd if sp_score is not None else 0.0
    return lineup_part + starter_part, lineup_part, starter_part


def edge_label(z):
    if z >= STRONG:
        return "Strong: bats"
    if z >= LEAN:
        return "Lean: bats"
    if z <= -STRONG:
        return "Strong: starter"
    if z <= -LEAN:
        return "Lean: starter"
    return "Even"


def site_label(score):
    """The site's label (Round H2): from the starter's score only (see EDGES.md)."""
    if score is None or (isinstance(score, float) and np.isnan(score)):
        return "No clear edge"
    if score >= STARTER_TOUGH:
        return "Tough matchup"
    if score >= STARTER_SLIGHT:
        return "Slight edge: starter"
    return "No clear edge"


SITE_BANDS = [("tough", "Tough matchup (starter 90+)"), ("slight", "Slight edge (73.5–90)"),
              ("none", "No clear edge (under 73.5)"), ("unscored", "No starter score")]


def site_band(score):
    if score is None or (isinstance(score, float) and np.isnan(score)):
        return "unscored"
    return "tough" if score >= STARTER_TOUGH else "slight" if score >= STARTER_SLIGHT else "none"


def starter_scale(scores):
    """Median and spread of established starters' scores (site: starterScale)."""
    v = sorted(float(s) for s in scores if s is not None and not np.isnan(s))
    if len(v) < 20:
        return 50.0, 25.0
    mid = v[len(v) // 2]
    sd = float(np.sqrt(np.mean([(x - mid) ** 2 for x in v]))) or 25.0
    return mid, sd


# ── inputs (all cached in backtest_cache/edges/; FanGraphs + MLB only) ─────────

SEASONS = list(range(2023, SEASON + 1))   # 2022 is the first prior season the batter lines need
FG_DELAY = 3                              # seconds between FanGraphs requests (polite)
SPLIT_ARR = {"no_split": [], "vs_lhp": [1], "vs_rhp": [2]}


def norm(name):
    import unicodedata
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[.'’]", "", s)
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def fgdate(d):
    return f"{d.year}-{d.month}-{d.day}"     # FanGraphs' own format (scrape_player_splits)


def retry(fn, tries=6):
    """Network calls retry with growing waits (MLB's API timed out once mid-run)."""
    for i in range(tries):
        try:
            return fn()
        except (requests.RequestException, ValueError) as e:
            if i == tries - 1:
                raise
            wait = min(300, 10 * 2 ** i)
            print(f"   ⚠️  {type(e).__name__}; retrying in {wait}s")
            time.sleep(wait)


def season_dates(y):
    """(Opening Day, last regular-season day) from MLB's API, cached."""
    import season as sn
    def fetch():
        return [sn.opening_day(y).isoformat(), sn.regular_season_end(y).isoformat()]
    a, b = bt.cached_json(f"edges/dates_{y}.json", fetch)
    from datetime import date
    return date.fromisoformat(a), date.fromisoformat(b)


def cutoffs(y, months):
    """Each game month -> the last day whose stats a game that month may use (None: no
    current-season stats yet, i.e. the opening month)."""
    from datetime import date, timedelta
    start, _ = season_dates(y)
    out = {}
    for m in months:
        if m <= start.month:
            out[m] = None
        else:
            out[m] = date(y, m, 1) - timedelta(days=1)
    return out


def fg_batters(y, split, start, end):
    """One FanGraphs splits-leaderboard call: {playerid: (name, team, PA, wRC+)}, cached."""
    import scrape_player_splits as spl

    def fetch():
        time.sleep(FG_DELAY)
        rows = retry(lambda: spl.fetch_split(SPLIT_ARR[split], fgdate(start), fgdate(end)))
        return [{"pid": str(r.get("playerId")), "name": r.get("playerName"), "tm": r.get("TeamNameAbb"),
                 "pa": r.get("PA"), "wrc": r.get("wRC+")} for r in rows if r.get("playerId") is not None]
    rows = bt.cached_json(f"edges/bat/{y}/{split}_{start}_{end}.json", fetch)
    return {r["pid"]: r for r in rows if r.get("wrc") is not None and r.get("pa")}


class BatterBook:
    """A season's batter lines as known before each month: exact season-to-date FanGraphs
    lines (no month-by-month approximation), last season's full line, and hindsight."""

    def __init__(self, y, months):
        start, end = season_dates(y)
        p_start, p_end = season_dates(y - 1)
        self.prior = fg_batters(y - 1, "no_split", p_start, p_end)
        self.cut = cutoffs(y, months)
        self.lines = {}
        for m, c in self.cut.items():
            if c is not None and c >= start:
                self.lines[m] = {s: fg_batters(y, s, start, c) for s in SPLIT_ARR}
        self.lines[13] = {s: fg_batters(y, s, start, end) for s in SPLIT_ARR}       # hindsight: the whole season
        names = {}
        for tables in self.lines.values():
            for table in tables.values():
                for pid, r in table.items():
                    names.setdefault(norm(r["name"]), set()).add(pid)
        for pid, r in self.prior.items():
            names.setdefault(norm(r["name"]), set()).add(pid)
        self.ids_by_name = {k: v for k, v in names.items()}

    def find(self, name):
        ids = self.ids_by_name.get(norm(name), set())
        return next(iter(ids)) if len(ids) == 1 else None      # same name twice: no guess

    def vs_hand(self, name, hand, month):
        pid = self.find(name)
        if pid is None:
            return None
        p = self.prior.get(pid)
        last_wrc, last_pa = (float(p["wrc"]), float(p["pa"])) if p else (None, 0)
        tables = self.lines.get(month)
        get = lambda s: tables[s].get(pid) if tables else None
        cur = get("no_split")
        split = {"L": "vs_lhp", "R": "vs_rhp"}.get(hand)
        spl_row = get(split) if split else None
        return batter_vs_hand(float(cur["wrc"]) if cur else 0, float(cur["pa"]) if cur else 0,
                              float(spl_row["wrc"]) if spl_row else 0, float(spl_row["pa"]) if spl_row else 0,
                              last_wrc, last_pa)


def inseason_scores(y, end):
    """Every pitcher's score as the live site would have shown it after `end`: season files
    before y + y's stats through `end` + career through y-1, scored with the live weights
    (current season counts). {FanGraphs playerid: composite}, cached."""
    import merge_pitcher_overall as mpo
    import score_pitcher_overall as spo
    import scrape_pitcher_season as sps
    import shutil
    path = f"edges/inseason/{y}_{end}.json"
    if os.path.exists(os.path.join(CACHE, path)):
        return bt.cached_json(path, dict)
    start, _ = season_dates(y)
    part = os.path.join(CACHE, "edges", "inseason_raw", f"{y}_{end}")
    os.makedirs(part, exist_ok=True)
    sps.SEASON, sps.START_DATE, sps.END_DATE = y, start.isoformat(), end.isoformat()
    responses, real_get = {}, sps.req_lib.get

    def get_once(url, **kw):              # FanGraphs returns the same rows for every type=
        key = re.sub(r"type=\d+&", "", url)
        if key not in responses:
            time.sleep(FG_DELAY)
            responses[key] = real_get(url, **kw)
        return responses[key]
    sps.req_lib.get = get_once
    try:
        for scraper in sps.get_scrapers():
            out = os.path.join(part, scraper["file"])
            if not os.path.exists(out):
                bt.quiet(sps.scrape_one, scraper).to_csv(out, index=False)
    finally:
        sps.req_lib.get = real_get
    work = os.path.join(CACHE, "edges", "inseason_work")
    shutil.rmtree(work, ignore_errors=True)
    os.makedirs(work)
    for year in bt.seasons_used(y):
        for _, prefix in mpo.ANNUAL_TAB_FILES:
            src = bt.season_file(prefix, year)
            if os.path.exists(src):
                shutil.copy(src, work)
    for f in os.listdir(part):
        shutil.copy(os.path.join(part, f), work)
    for f in os.listdir(bt.career_through(y - 1)):
        if f.endswith(".csv"):
            shutil.copy(os.path.join(CACHE, f"career_through_{y - 1}", f), work)
    mpo.UPLOAD_DIR = work
    frames = [bt.quiet(mpo.merge_tabs, mpo.ANNUAL_TAB_FILES, year) for year in bt.seasons_used(y) + [y]]
    frames.append(bt.quiet(mpo.merge_tabs, mpo.CAREER_TAB_FILES, "career", label="career"))
    df = pd.concat([f for f in frames if not f.empty], ignore_index=True)
    df["season"] = df["season"].astype(str).str.strip().apply(lambda x: int(x) if x.isdigit() else x)
    df = bt.quiet(spo.assign_player_key, df)
    df = spo.compute_season_scores(df)
    df = spo.compute_composite_scores(df, spo.rolling_weights([0.32, 0.20, 0.12, 0.08, 0.05, 0.03], y, career=0.20))
    per = df.groupby("player_key")["composite_score"].first().dropna()
    scores = {re.sub(r"\.0$", "", str(k)): float(v) for k, v in per.items() if re.fullmatch(r"\d+(\.0)?", str(k))}
    os.makedirs(os.path.dirname(os.path.join(CACHE, path)), exist_ok=True)
    json.dump(scores, open(os.path.join(CACHE, path), "w"))
    return scores


def boxscore(y, pk):
    def fetch():
        def get():
            resp = requests.get(f"{MLB}/game/{pk}/boxscore", timeout=30)
            resp.raise_for_status()
            return resp
        r = retry(get)
        time.sleep(0.15)
        b = r.json()
        side = {}
        for s in ("away", "home"):
            t = b["teams"][s]
            players = t["players"]
            bats = [{"id": pid, "name": players.get(f"ID{pid}", {}).get("person", {}).get("fullName", "")}
                    for pid in t.get("battingOrder", [])[:9]]
            sp = t["pitchers"][0] if t.get("pitchers") else None
            sp_name = players.get(f"ID{sp}", {}).get("person", {}).get("fullName", "") if sp else ""
            side[s] = {"bats": bats, "sp": sp, "sp_name": sp_name,
                       "runs": t.get("teamStats", {}).get("batting", {}).get("runs")}
        return side
    return bt.cached_json(f"edges/box/{y}/{pk}.json", fetch)


def pitch_hands(y, ids):
    path = f"edges/hands_{y}.json"
    have = bt.cached_json(path, lambda: {})
    missing = [i for i in ids if str(i) not in have]
    for k in range(0, len(missing), 150):
        def get():
            resp = requests.get(f"{MLB}/people", params={"personIds": ",".join(str(i) for i in missing[k:k + 150])}, timeout=30)
            resp.raise_for_status()
            return resp
        r = retry(get)
        for p in r.json().get("people", []):
            have[str(p["id"])] = (p.get("pitchHand") or {}).get("code")
        time.sleep(0.3)
    if missing:
        json.dump(have, open(os.path.join(CACHE, path), "w"))
    return have


# ── build every edge ─────────────────────────────────────────────────────────

def build(y):
    res = bt.results(y)
    games = res[(res["game_type"] == "regular") & res["status"].isin(["Final", "Completed Early"])]
    print(f"── {y}: {len(games)} regular-season games")
    months = sorted({int(str(d)[5:7]) for d in games["date"]})

    pre = bt.preseason_scores(y)
    ids = pd.concat([bt.fangraphs_ids(y - 1), bt.fangraphs_ids(y)]).dropna(subset=["mlbam"])
    mlbam_to_fg = {str(int(float(m))): str(p) for m, p in zip(ids["mlbam"], ids["playerid"])}
    pre_by_fg = {re.sub(r"\.0$", "", str(k)): float(v) for k, v in zip(pre["playerid"], pre["composite"]) if pd.notna(v) and pd.notna(k)}
    mid, sd = starter_scale(pre.loc[pre["prior_ip"] >= 100, "composite"].tolist())
    print(f"   preseason yardstick: median {mid:.1f}, spread {sd:.1f}")

    cut = cutoffs(y, months)
    inseason = {m: (inseason_scores(y, c) if c is not None else None) for m, c in cut.items()}
    print(f"   in-season pitcher scores rebuilt for {sum(v is not None for v in inseason.values())} month starts")
    book = BatterBook(y, months)

    rows = []
    for i, g in enumerate(games.itertuples(), 1):
        if i % 500 == 0:
            print(f"   box scores {i}/{len(games)}")
        rows.append((g, boxscore(y, int(g.game_pk))))
    hands = pitch_hands(y, sorted({b[s]["sp"] for _, b in rows for s in ("away", "home") if b[s]["sp"]}))

    out = []
    for g, box in rows:
        month = int(str(g.date)[5:7])
        for off, dfn in (("away", "home"), ("home", "away")):
            o, d = box[off], box[dfn]
            hand = hands.get(str(d["sp"]))
            vals = [(k + 1, book.vs_hand(p["name"], hand, month)) for k, p in enumerate(o["bats"])]
            lw = lineup_wrc(vals)
            if lw is None or o["runs"] is None:
                continue
            hind = lineup_wrc([(k + 1, book.vs_hand(p["name"], hand, 13)) for k, p in enumerate(o["bats"])])
            fg = mlbam_to_fg.get(str(d["sp"]))
            sp_pre = pre_by_fg.get(fg) if fg else None
            ins = inseason.get(month)
            sp_in = (ins.get(fg) if (ins and fg) else None)
            if sp_in is None:
                sp_in = sp_pre               # before any current-season stats the site shows the preseason score
            z, lp, spp = edge_z(lw, sp_pre, mid, sd)
            out.append({"season": y, "date": str(g.date), "game_pk": int(g.game_pk), "month": month,
                        "offense": getattr(g, off), "defense": getattr(g, dfn), "home": g.home,
                        "lineup": lw, "lineup_hindsight": hind, "matched": sum(v is not None for _, v in vals),
                        "sp": d["sp_name"], "hand": hand, "sp_score": sp_pre, "sp_in": sp_in,
                        "lineup_part": lp, "starter_part": spp, "z": z, "label": edge_label(z),
                        "starter_label": edge_label(spp), "runs": int(o["runs"])})
    df = pd.DataFrame(out)
    print(f"   {len(df)} team-games ({df['sp_in'].notna().mean():.0%} with a starter score)")
    return df, (mid, sd)


# ── grading ──────────────────────────────────────────────────────────────────

LABELS = ["Strong: bats", "Lean: bats", "Even", "Lean: starter", "Strong: starter"]


def spearman(a, b):
    return float(pd.Series(a).rank().corr(pd.Series(b).rank()))


def cluster_bootstrap(df, stat, keys=("season", "offense", "month"), boot=BOOT, seed=7):
    """95% range of stat(df), resampling whole clusters (default team-months: a lineup's
    number barely changes within a month, so its games are not independent)."""
    rng = np.random.default_rng(seed)
    groups = [g for _, g in df.groupby(list(keys))]
    vals = []
    for _ in range(boot):
        pick = rng.integers(0, len(groups), len(groups))
        vals.append(stat(pd.concat([groups[k] for k in pick], ignore_index=True)))
    return float(np.nanpercentile(vals, 2.5)), float(np.nanpercentile(vals, 97.5))


def bands(df, score_col):
    out = []
    for key, name in SITE_BANDS:
        sub = df[df[score_col].map(site_band) == key]
        lo, hi = cluster_bootstrap(df, lambda d, k=key: d.loc[d[score_col].map(site_band) == k, "runs"].mean())
        out.append({"band": key, "label": name, "n": len(sub), "share": len(sub) / len(df),
                    "runs": float(sub["runs"].mean()), "ci": [lo, hi]})
    return out


def park_adjusted(df):
    """Runs divided by the park's run factor (runs per game there vs the season average)."""
    per_game = df.groupby(["season", "game_pk", "home"])["runs"].sum().reset_index()
    pf = per_game.groupby(["season", "home"])["runs"].mean() / per_game.groupby("season")["runs"].mean()
    f = df.set_index(["season", "home"]).index.map(pf.to_dict())
    return df["runs"] / np.asarray(f, dtype=float)


def same_game(df):
    """One row per game: away minus home for runs, lineup and the starter each faced."""
    a = df[df["offense"] != df["home"]].set_index("game_pk")
    h = df[df["offense"] == df["home"]].set_index("game_pk")
    j = a.join(h, lsuffix="_a", rsuffix="_h", how="inner")
    g = pd.DataFrame({"season": j["season_a"], "home": j["home_a"], "month": j["month_a"],
                      "d_runs": j["runs_a"] - j["runs_h"], "d_lineup": j["lineup_a"] - j["lineup_h"],
                      "d_sp": j["sp_in_a"] - j["sp_in_h"]})
    return g.reset_index()


def grade(df):
    r = {"team_games": len(df), "games": int(df["game_pk"].nunique()), "league_runs": float(df["runs"].mean()),
         "team_months": int(df.groupby(["season", "offense", "month"]).ngroups)}
    h = df.dropna(subset=["lineup_hindsight"])
    r["rho_lineup_hindsight"] = spearman(h["lineup_hindsight"], h["runs"])
    for col in ("z", "lineup_part", "starter_part"):
        r[f"rho_{col}"] = spearman(df[col], df["runs"])
        r[f"rho_{col}_ci"] = cluster_bootstrap(df, lambda d, c=col: spearman(d[c], d["runs"]))
    s_in = df.dropna(subset=["sp_in"])
    r["rho_sp_in"] = spearman(-s_in["sp_in"], s_in["runs"])
    r["rho_sp_in_ci"] = cluster_bootstrap(s_in, lambda d: spearman(-d["sp_in"], d["runs"]))
    s_pre = df.dropna(subset=["sp_score"])
    r["rho_sp_pre"] = spearman(-s_pre["sp_score"], s_pre["runs"])
    r["rho_sp_pre_ci"] = cluster_bootstrap(s_pre, lambda d: spearman(-d["sp_score"], d["runs"]))
    X = np.column_stack([np.ones(len(df)), df["lineup_part"], df["starter_part"]])
    coef = np.linalg.lstsq(X, df["runs"].to_numpy(float), rcond=None)[0]
    r["runs_per_lineup_unit"], r["runs_per_starter_unit"] = float(coef[1]), float(coef[2])
    # A. lineups, with the ballpark taken out two ways
    adj = df.assign(adj=park_adjusted(df))
    r["rho_lineup_parkadj"] = spearman(adj["lineup"], adj["adj"])
    r["rho_lineup_parkadj_ci"] = cluster_bootstrap(adj, lambda d: spearman(d["lineup"], d["adj"]))
    sg = same_game(df)
    r["same_game_games"] = len(sg)
    r["rho_same_game_lineup"] = spearman(sg["d_lineup"], sg["d_runs"])
    r["rho_same_game_lineup_ci"] = cluster_bootstrap(sg, lambda d: spearman(d["d_lineup"], d["d_runs"]), keys=("season", "home", "month"))
    s = sg.dropna(subset=["d_sp"])
    r["rho_same_game_sp"] = spearman(-s["d_sp"], s["d_runs"])
    r["rho_same_game_sp_ci"] = cluster_bootstrap(s, lambda d: spearman(-d["d_sp"], d["d_runs"]), keys=("season", "home", "month"))
    X = np.column_stack([np.ones(len(s)), s["d_lineup"] / LINEUP_DIV, -s["d_sp"] / 25.0])
    coef = np.linalg.lstsq(X, s["d_runs"].to_numpy(float), rcond=None)[0]
    r["same_game_runs_per_12_wrc"], r["same_game_runs_per_25_score"] = float(coef[1]), float(coef[2])
    # site labels, graded with the scores the site shows (in-season) and with Opening Day scores
    r["site_bands"] = bands(df, "sp_in")
    r["site_bands_preseason"] = bands(df, "sp_score")
    r["site_bands_by_season"] = {str(y): bands(g, "sp_in") for y, g in df.groupby("season")}
    r["by_season"] = [{"season": int(y), "team_games": len(g), "rho_lineup": spearman(g["lineup"], g["runs"]),
                       "rho_sp_in": spearman(-g["sp_in"].fillna(g["sp_in"].median()), g["runs"])}
                      for y, g in df.groupby("season")]
    for key, col in (("labels", "label"), ("starter_labels", "starter_label")):
        out = []
        for lab in LABELS:
            sub = df[df[col] == lab]
            if len(sub) < 20:
                continue
            lo, hi = cluster_bootstrap(df, lambda d, L=lab, c=col: d.loc[d[c] == L, "runs"].mean())
            out.append({"label": lab, "n": len(sub), "share": len(sub) / len(df), "runs": float(sub["runs"].mean()), "ci": [lo, hi]})
        r[key] = out
    r["by_month"] = [{"month": int(m), "n": len(g), "rho_z": spearman(g["z"], g["runs"])}
                     for m, g in df.groupby("month") if len(g) >= 100]
    return r


def _ci(r, k):
    a, b = r[k + "_ci"]
    return f"{r[k]:.3f} ({a:.3f} to {b:.3f})"


def write_report(r, seasons, scales):
    span = f"{seasons[0]}–{seasons[-1]}"
    b = {x["band"]: x for x in r["site_bands"]}
    sc = "; ".join(f"{y}: {m:.1f} + {s:.1f} = {m + s:.1f}" for y, (m, s) in scales.items())
    L = ["# Backtest: do \"Today's edges\" predict runs?", "",
         "_Generated by `scripts/backtest_edges.py` — rerun it to refresh. Read-only: changes no data._", "",
         f"Every regular-season game of {span} ({r['games']} games, {r['team_games']} team-games) is rebuilt from what",
         "was known **before** it: the real batting orders and starters (MLB box scores), each batter's exact",
         "season-to-date FanGraphs lines up to the end of the previous month plus his previous-season line (the",
         "site's Round G1 math), and each starter's score — both as of Opening Day and as the live site would have",
         "shown it that month (rebuilt from his stats so far with the live weights). The edge is then compared",
         "with the runs that offense scored.", "",
         "## Verdict", "",
         f"**What the site does with this:** the edges card labels each game from the starter's score only: "
         f"\"Tough matchup\" for {STARTER_TOUGH:g}+ (the Elite tier), \"Slight edge\" for {STARTER_SLIGHT:g}–{STARTER_TOUGH:g}; "
         f"lineup wRC+ is shown as context. Plain scores mean the same in every season (a percentile among "
         f"active pitchers); the season's \"middle starter\" does not ({sc}), which is why the site "
         f"uses plain scores rather than a yardstick.", "",
         f"- **The starter matchup predicts, every season.** Offenses facing a Tough-matchup starter scored "
         f"**{b['tough']['runs']:.2f}** runs ({b['tough']['ci'][0]:.2f} to {b['tough']['ci'][1]:.2f}), Slight "
         f"{b['slight']['runs']:.2f}, others {b['none']['runs']:.2f} (league {r['league_runs']:.2f}). "
         f"Starter score vs runs: in-season {_ci(r, 'rho_sp_in')}, Opening Day only {_ci(r, 'rho_sp_pre')}.",
         f"- **The lineup matters a little, far less than the starter.** Across all games it shows almost nothing: "
         f"raw runs {_ci(r, 'rho_lineup_part')}, park-adjusted {_ci(r, 'rho_lineup_parkadj')}. Comparing the two "
         f"offenses within the same game (same park, weather and day) shows a small real effect, "
         f"{_ci(r, 'rho_same_game_lineup')}, about half the starter's in those games, {_ci(r, 'rho_same_game_sp')}. "
         f"Even with hindsight (full-season lines) a lineup predicts one game's runs at only {r['rho_lineup_hindsight']:.3f}, "
         "so the site keeps it as context, not in the label.",
         "", "## Runs by the site's labels (in-season scores, as the site shows them)", "",
         "| Site label | Team-games | Share | Runs scored | 95% range |", "|---|---|---|---|---|"]
    for x in r["site_bands"]:
        L.append(f"| {x['label']} | {x['n']} | {x['share']:.0%} | {x['runs']:.2f} | {x['ci'][0]:.2f} to {x['ci'][1]:.2f} |")
    L += ["", "### By season", "", "| Season | Tough | Slight | No clear edge | No score |", "|---|---|---|---|---|"]
    for y, bs in r["site_bands_by_season"].items():
        d = {x["band"]: x for x in bs}
        L.append(f"| {y} | " + " | ".join(f"{d[k]['runs']:.2f} ({d[k]['share']:.0%})" for k in ("tough", "slight", "none", "unscored")) + " |")
    L += ["", "### Same labels with Opening Day scores (frozen all season)", "",
          "| Site label | Team-games | Share | Runs scored | 95% range |", "|---|---|---|---|---|"]
    for x in r["site_bands_preseason"]:
        L.append(f"| {x['label']} | {x['n']} | {x['share']:.0%} | {x['runs']:.2f} | {x['ci'][0]:.2f} to {x['ci'][1]:.2f} |")
    L += ["", "## The lineup question, with ballparks taken out", "",
          "| Test | Rank correlation | 95% range |", "|---|---|---|",
          f"| Lineup vs raw runs | {r['rho_lineup_part']:.3f} | {r['rho_lineup_part_ci'][0]:.3f} to {r['rho_lineup_part_ci'][1]:.3f} |",
          f"| Lineup vs park-adjusted runs | {r['rho_lineup_parkadj']:.3f} | {r['rho_lineup_parkadj_ci'][0]:.3f} to {r['rho_lineup_parkadj_ci'][1]:.3f} |",
          f"| Same game: lineup difference vs run difference | {r['rho_same_game_lineup']:.3f} | {r['rho_same_game_lineup_ci'][0]:.3f} to {r['rho_same_game_lineup_ci'][1]:.3f} |",
          f"| Same game: starter difference vs run difference | {r['rho_same_game_sp']:.3f} | {r['rho_same_game_sp_ci'][0]:.3f} to {r['rho_same_game_sp_ci'][1]:.3f} |",
          f"| Lineup with hindsight (full-season lines) | {r['rho_lineup_hindsight']:.3f} | |", "",
          f"In the same game, 12 points of lineup wRC+ are worth {r['same_game_runs_per_12_wrc']:+.2f} runs and 25 points "
          f"of starter score {r['same_game_runs_per_25_score']:+.2f} runs ({r['same_game_games']} games).", "",
          "### By season", "", "| Season | Team-games | Lineup vs runs | Starter (in-season) vs runs |", "|---|---|---|---|"]
    for x in r["by_season"]:
        L.append(f"| {x['season']} | {x['team_games']} | {x['rho_lineup']:.3f} | {x['rho_sp_in']:.3f} |")
    L += ["", "## The combined edge as first built (Round F2)", "",
          "| Label | Team-games | Share | Runs scored | 95% range |", "|---|---|---|---|---|"]
    for x in r["labels"]:
        L.append(f"| {x['label']} | {x['n']} | {x['share']:.0%} | {x['runs']:.2f} | {x['ci'][0]:.2f} to {x['ci'][1]:.2f} |")
    L += ["", "| Predictor | Rank correlation | 95% range |", "|---|---|---|"]
    for col, name in (("z", "Edge (both parts)"), ("lineup_part", "Lineup part only"), ("starter_part", "Starter part only (Opening Day)")):
        a, bb = r[f"rho_{col}_ci"]
        L.append(f"| {name} | {r[f'rho_{col}']:.3f} | {a:.3f} to {bb:.3f} |")
    L += ["", "## Limits", "",
          "- The lineup also faces the bullpen, which the edge ignores.",
          "- Park factors come from the same seasons' runs (a standard shortcut; the same-game test needs none).",
          "- In-season starter scores use stats through the end of the previous month, so they lag up to a month.",
          f"- 95% ranges resample whole team-months ({r['team_months']}), or for same-game tests whole park-months.", ""]
    open(os.path.join(REPO, "EDGES.md"), "w").write("\n".join(L))


def main():
    if "--report-only" in sys.argv:          # rewrite EDGES.md from the saved results (no regrading)
        r = json.load(open(os.path.join(REPO, "edges_results.json")))
        scales = {int(y): (v["mid"], v["sd"]) for y, v in r["scales"].items()}
        write_report(r, r["seasons"], scales)
        print("✅ rewrote EDGES.md from edges_results.json")
        return
    os.makedirs(os.path.join(CACHE, "edges"), exist_ok=True)
    frames, scales = [], {}
    for y in SEASONS:
        df, scale = build(y)
        frames.append(df)
        scales[y] = scale
    df = pd.concat(frames, ignore_index=True)
    r = grade(df)
    mid = float(np.mean([m for m, _ in scales.values()])); sd = float(np.mean([s for _, s in scales.values()]))
    r.update({"season": f"{SEASONS[0]}–{SEASONS[-1]}", "seasons": SEASONS, "scale": {"mid": mid, "sd": sd},
              "scales": {str(y): {"mid": m, "sd": s} for y, (m, s) in scales.items()},
              "cutoffs": {"strong": STRONG, "lean": LEAN, "lineup_div": LINEUP_DIV},
              "site_cutoffs": {"tough": STARTER_TOUGH, "slight": STARTER_SLIGHT}})
    json.dump(r, open(os.path.join(REPO, "edges_results.json"), "w"), indent=1)
    df.to_csv(os.path.join(CACHE, "edges", "edges_all.csv"), index=False)
    write_report(r, SEASONS, scales)
    print("✅ wrote EDGES.md and edges_results.json")


if __name__ == "__main__":
    main()

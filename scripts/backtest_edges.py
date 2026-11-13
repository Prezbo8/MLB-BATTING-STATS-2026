#!/usr/bin/env python3
"""
backtest_edges.py — do the site's "Today's edges" predict runs? (read-only)

Rebuilds the edge for both offenses of every regular-season game of SEASON
from what was known before that game, with the same math as the site
(index.html / mobile.html, MATCHUPS + Round G1), and compares it with the runs
each offense actually scored.

  Lineups / starters  MLB box scores (who actually batted where, who started)
  Batter numbers      monthly FanGraphs lines from months that had already
                      ended (a June game sees March-May only), leaning on the
                      batter's previous-season line exactly like the site
  Starter score       each pitcher's score as of Opening Day (backtest.py)
  Outcome             runs the offense scored in that game

Writes EDGES.md and edges_results.json; changes no data. Downloads are cached
in backtest_cache/ (not committed). Batter lines are read from the site's own
Supabase tables with the public read-only key the website uses.

  python3 scripts/backtest_edges.py
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
STARTER_STRONG, STARTER_LEAN = 1.0, 0.4   # the site's labels since G2: starter part only


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


def site_label(starter_part):
    """The site's label since Round G2 (starter matchup only; see EDGES.md)."""
    if starter_part <= -STARTER_STRONG:
        return "Tough matchup"
    if starter_part <= -STARTER_LEAN:
        return "Slight edge: starter"
    return "No clear edge"


def starter_scale(scores):
    """Median and spread of established starters' scores (site: starterScale)."""
    v = sorted(float(s) for s in scores if s is not None and not np.isnan(s))
    if len(v) < 20:
        return 50.0, 25.0
    mid = v[len(v) // 2]
    sd = float(np.sqrt(np.mean([(x - mid) ** 2 for x in v]))) or 25.0
    return mid, sd


# ── inputs ───────────────────────────────────────────────────────────────────

def norm(name):
    import unicodedata
    s = unicodedata.normalize("NFKD", name or "").encode("ascii", "ignore").decode().lower()
    s = re.sub(r"[.'’]", "", s)
    s = re.sub(r"\b(jr|sr|ii|iii|iv)\b", "", s)
    return re.sub(r"\s+", " ", s).strip()


def _supabase():
    """The site's public read-only (anon) key, unless SUPABASE_URL/KEY are set."""
    url, key = os.environ.get("SUPABASE_URL"), os.environ.get("SUPABASE_KEY")
    if url and key:
        return url, key
    html = open(os.path.join(REPO, "index.html")).read()
    url = re.search(r"SUPABASE_URL\s*=\s*'([^']+)'", html).group(1)
    key = re.search(r"SUPABASE_KEY\s*=\s*'([^']+)'", html).group(1)
    return url, key


def sb_rows(table, query):
    url, key = _supabase()
    out, step = [], 1000
    while True:
        r = requests.get(f"{url}/rest/v1/{table}?{query}", timeout=60,
                         headers={"apikey": key, "Authorization": f"Bearer {key}",
                                  "Range": f"{len(out)}-{len(out) + step - 1}"})
        r.raise_for_status()
        page = r.json()
        out += page
        if len(page) < step:
            return out


def monthly_batters(season):
    rows = bt.cached_json(f"edges/player_months_{season}.json", lambda: sb_rows(
        "fangraphs_player_splits_history",
        f"select=playerid,name,tm,split,date_range,pa,wrcplus&season=eq.{season}&split=in.(no_split,vs_lhp,vs_rhp)"))
    df = pd.DataFrame(rows)
    df["pa"] = pd.to_numeric(df["pa"], errors="coerce").fillna(0)
    df["wrcplus"] = pd.to_numeric(df["wrcplus"], errors="coerce")
    df = df[(df["pa"] > 0) & df["wrcplus"].notna()]
    df["month"] = df["date_range"].str.slice(5, 7).astype(int)
    df["playerid"] = df["playerid"].astype(str)
    return df


def last_season_lines(season):
    rows = bt.cached_json(f"edges/player_season_{season - 1}.json", lambda: sb_rows(
        "fangraphs_player_history",
        f"select=playerid,name,pa,wrcplus&season=eq.{season - 1}&split=eq.no_split&period=eq.season"))
    out = {}
    for x in rows:
        if x.get("wrcplus") is None or not x.get("pa"):
            continue
        out[str(x["playerid"])] = (float(x["wrcplus"]), float(x["pa"]))
    return out


def boxscore(pk):
    def fetch():
        r = requests.get(f"{MLB}/game/{pk}/boxscore", timeout=30)
        r.raise_for_status()
        time.sleep(0.15)
        b = r.json()
        side = {}
        for s in ("away", "home"):
            t = b["teams"][s]
            players = t["players"]
            bats = []
            for pid in t.get("battingOrder", [])[:9]:
                p = players.get(f"ID{pid}", {})
                bats.append({"id": pid, "name": p.get("person", {}).get("fullName", "")})
            sp = t["pitchers"][0] if t.get("pitchers") else None
            sp_name = players.get(f"ID{sp}", {}).get("person", {}).get("fullName", "") if sp else ""
            side[s] = {"bats": bats, "sp": sp, "sp_name": sp_name,
                       "runs": t.get("teamStats", {}).get("batting", {}).get("runs")}
        return side
    return bt.cached_json(f"edges/box/{SEASON}/{pk}.json", fetch)


def pitch_hands(ids):
    have = bt.cached_json(f"edges/hands_{SEASON}.json", lambda: {})
    missing = [i for i in ids if str(i) not in have]
    for k in range(0, len(missing), 150):
        chunk = ",".join(str(i) for i in missing[k:k + 150])
        r = requests.get(f"{MLB}/people", params={"personIds": chunk}, timeout=30)
        r.raise_for_status()
        for p in r.json().get("people", []):
            have[str(p["id"])] = (p.get("pitchHand") or {}).get("code")
        time.sleep(0.3)
    if missing:
        json.dump(have, open(os.path.join(CACHE, f"edges/hands_{SEASON}.json"), "w"))
    return have


# ── build every edge ─────────────────────────────────────────────────────────

class BatterBook:
    """A batter's numbers as known before a given month."""

    def __init__(self, months, last):
        self.last = last
        self.by_id = {}
        for (pid, split), g in months.groupby(["playerid", "split"]):
            self.by_id[(pid, split)] = g.sort_values("month")[["month", "pa", "wrcplus"]].to_numpy()
        names = months.drop_duplicates("playerid")[["playerid", "name", "tm"]]
        names = names.assign(key=names["name"].map(norm))
        self.ids_by_name = names.groupby("key")["playerid"].apply(list).to_dict()

    def find(self, name):
        ids = self.ids_by_name.get(norm(name), [])
        return ids[0] if len(ids) == 1 else None          # same name twice: no guess

    def before(self, pid, split, month):
        a = self.by_id.get((pid, split))
        if a is None:
            return None, 0
        a = a[a[:, 0] < month]
        pa = a[:, 1].sum()
        return ((a[:, 1] * a[:, 2]).sum() / pa if pa else None), pa

    def vs_hand(self, name, hand, month):
        pid = self.find(name)
        last_wrc, last_pa = self.last.get(pid, (None, 0)) if pid else (None, 0)
        if pid is None:
            return None
        cur_wrc, cur_pa = self.before(pid, "no_split", month)
        split = {"L": "vs_lhp", "R": "vs_rhp"}.get(hand)
        s_wrc, s_pa = self.before(pid, split, month) if split else (None, 0)
        return batter_vs_hand(cur_wrc or 0, cur_pa, s_wrc or 0, s_pa, last_wrc, last_pa)


def build(season):
    res = bt.results(season)
    games = res[(res["game_type"] == "regular") & res["status"].isin(["Final", "Completed Early"])]
    print(f"── {season}: {len(games)} regular-season games")

    pre = bt.preseason_scores(season)
    ids = pd.concat([bt.fangraphs_ids(season - 1), bt.fangraphs_ids(season)]).dropna(subset=["mlbam"])
    mlbam_to_fg = {str(int(float(m))): str(p) for m, p in zip(ids["mlbam"], ids["playerid"])}
    score_by_fg = dict(zip(pre["playerid"].astype(str), pre["composite"]))
    uniq_names = pre.assign(k=pre["name"].map(norm)).drop_duplicates("k", keep=False)
    score_by_name = dict(zip(uniq_names["k"], uniq_names["composite"]))
    # yardstick = last season's 100+ IP starters (the site's early-season scale)
    mid, sd = starter_scale(pre.loc[pre["prior_ip"] >= 100, "composite"].tolist())
    print(f"   starter yardstick: median {mid:.1f}, spread {sd:.1f}")

    book = BatterBook(monthly_batters(season), last_season_lines(season))
    rows, n = [], len(games)
    for i, g in enumerate(games.itertuples(), 1):
        if i % 250 == 0:
            print(f"   box scores {i}/{n}")
        box = boxscore(int(g.game_pk))
        rows.append((g, box))
    hands = pitch_hands(sorted({b[s]["sp"] for _, b in rows for s in ("away", "home") if b[s]["sp"]}))

    out = []
    for g, box in rows:
        month = int(str(g.date)[5:7])
        for off, dfn in (("away", "home"), ("home", "away")):
            o, d = box[off], box[dfn]
            hand = hands.get(str(d["sp"]))
            vals = [(k + 1, book.vs_hand(p["name"], hand, month)) for k, p in enumerate(o["bats"])]
            lw = lineup_wrc(vals)
            hind = lineup_wrc([(k + 1, book.vs_hand(p["name"], hand, 13)) for k, p in enumerate(o["bats"])])
            if lw is None or o["runs"] is None:
                continue
            fg = mlbam_to_fg.get(str(d["sp"]))
            sp_score = score_by_fg.get(fg) if fg else None
            if sp_score is None or (isinstance(sp_score, float) and np.isnan(sp_score)):
                sp_score = score_by_name.get(norm(d["sp_name"]))
            sp_score = None if sp_score is None or pd.isna(sp_score) else float(sp_score)
            z, lp, spp = edge_z(lw, sp_score, mid, sd)
            out.append({"date": str(g.date), "game_pk": int(g.game_pk), "month": month,
                        "offense": getattr(g, off), "defense": getattr(g, dfn),
                        "lineup": lw, "lineup_hindsight": hind, "matched": sum(v is not None for _, v in vals),
                        "sp": d["sp_name"], "hand": hand, "sp_score": sp_score,
                        "lineup_part": lp, "starter_part": spp, "z": z, "label": edge_label(z),
                        "starter_label": edge_label(spp),
                        "runs": int(o["runs"])})
    df = pd.DataFrame(out)
    print(f"   {len(df)} team-games with an edge ({df['sp_score'].notna().mean():.0%} with a starter score)")
    return df, (mid, sd)


# ── grading ──────────────────────────────────────────────────────────────────

LABELS = ["Strong: bats", "Lean: bats", "Even", "Lean: starter", "Strong: starter"]


def spearman(a, b):
    return float(pd.Series(a).rank().corr(pd.Series(b).rank()))


def cluster_bootstrap(df, stat, boot=BOOT, seed=7):
    """95% range of stat(df), resampling whole team-months. A lineup's number barely
    changes within a month, so its games are not independent; resampling single
    games made the ranges far too narrow."""
    rng = np.random.default_rng(seed)
    groups = [g for _, g in df.groupby(["offense", "month"])]
    vals = []
    for _ in range(boot):
        pick = rng.integers(0, len(groups), len(groups))
        vals.append(stat(pd.concat([groups[k] for k in pick], ignore_index=True)))
    return float(np.nanpercentile(vals, 2.5)), float(np.nanpercentile(vals, 97.5))


def grade(df):
    r = {"team_games": len(df), "games": int(df["game_pk"].nunique()),
         "team_months": int(df.groupby(["offense", "month"]).ngroups), "league_runs": float(df["runs"].mean())}
    h = df.dropna(subset=["lineup_hindsight"])
    r["rho_lineup_hindsight"] = spearman(h["lineup_hindsight"], h["runs"])
    for col in ("z", "lineup_part", "starter_part"):
        r[f"rho_{col}"] = spearman(df[col], df["runs"])
        r[f"rho_{col}_ci"] = cluster_bootstrap(df, lambda d, c=col: spearman(d[c], d["runs"]))
    # runs per unit of each part (least squares), to see how the two parts should be weighted
    X = np.column_stack([np.ones(len(df)), df["lineup_part"], df["starter_part"]])
    coef = np.linalg.lstsq(X, df["runs"].to_numpy(float), rcond=None)[0]
    r["runs_per_lineup_unit"], r["runs_per_starter_unit"] = float(coef[1]), float(coef[2])
    for key, col in (("labels", "label"), ("starter_labels", "starter_label")):
        out = []
        for lab in LABELS:
            sub = df[df[col] == lab]
            if len(sub) < 20:
                continue
            lo, hi = cluster_bootstrap(df, lambda d, L=lab, c=col: d.loc[d[c] == L, "runs"].mean())
            out.append({"label": lab, "n": len(sub), "share": len(sub) / len(df),
                        "runs": float(sub["runs"].mean()), "ci": [lo, hi]})
        r[key] = out
    # by month, so April (mostly last season's lines) can be judged on its own
    r["by_month"] = [{"month": int(m), "n": len(g), "rho_z": spearman(g["z"], g["runs"])}
                     for m, g in df.groupby("month") if len(g) >= 100]
    # deciles of z
    df = df.assign(dec=pd.qcut(df["z"], 10, labels=False, duplicates="drop"))
    r["deciles"] = [{"decile": int(k) + 1, "z_lo": float(g["z"].min()), "z_hi": float(g["z"].max()),
                     "runs": float(g["runs"].mean()), "n": len(g)} for k, g in df.groupby("dec")]
    return r


def write_report(r, season, scale):
    L = r["league_runs"]
    rho, (lo, hi) = r["rho_z"], r["rho_z_ci"]
    lab = {x["label"]: x for x in r["labels"]}
    sb, ss, ev = lab.get("Strong: bats"), lab.get("Strong: starter"), lab.get("Even")
    lines = [
        "# Backtest: do \"Today's edges\" predict runs?", "",
        "_Generated by `scripts/backtest_edges.py` — rerun it to refresh. Read-only: changes no data._", "",
        f"Every {season} regular-season game is rebuilt from what was known **before** it: the real",
        "batting orders and starters (MLB box scores), each batter's lines from months that had",
        "already ended plus his previous-season line (the site's Round G1 math), and each starter's",
        "score as of Opening Day. The edge is then compared with the runs that offense scored.",
        f"Starter yardstick: last season's 100+ IP starters (median {scale[0]:.1f}, spread {scale[1]:.1f}).", "",
        "## Verdict", "",
        "**What the site does with this:** since Round G2 the edges card labels each game from the "
        "starter matchup only (the part with evidence) — \"Tough matchup\" at 1.0 starter spreads, "
        "\"Slight edge\" at 0.4 — and shows lineup wRC+ as context.", "",
    ]
    if lo > 0:
        lines.append(f"- **The edge predicts runs, weakly.** Rank correlation with runs scored: {rho:.3f} "
                     f"(95% range {lo:.3f} to {hi:.3f}) over {r['team_games']} team-games. One game is noisy; "
                     "the difference shows over many games.")
    else:
        lines.append(f"- **No clear signal.** Rank correlation with runs scored: {rho:.3f} "
                     f"(95% range {lo:.3f} to {hi:.3f}).")
    if sb and ss and ev:
        lines.append(f"- Offenses labelled **Strong (bats)** scored {sb['runs']:.2f} runs; **Even** {ev['runs']:.2f}; "
                     f"**Strong (starter)** {ss['runs']:.2f} — league average {L:.2f}.")
    a, b = r["rho_starter_part_ci"]
    lines.append(f"- **The starter part carries the signal:** {r['rho_starter_part']:.3f} ({a:.3f} to {b:.3f}), in line with "
                 "BACKTEST.md's game test. Offenses facing a much better starter score fewer runs.")
    a, b = r["rho_lineup_part_ci"]
    lines.append(f"- **The lineup part does not:** {r['rho_lineup_part']:.3f} ({a:.3f} to {b:.3f}). Even with hindsight "
                 f"(each batter's full-season line) a lineup's wRC+ predicts one game's runs at only "
                 f"{r['rho_lineup_hindsight']:.3f}: wRC+ is adjusted for ballparks and the run environment, raw runs are not, "
                 "and a lineup also faces the bullpen.")
    lines += ["", "## Runs by label", "",
              "| Label | Team-games | Share | Runs scored | 95% range |", "|---|---|---|---|---|"]
    for x in r["labels"]:
        lines.append(f"| {x['label']} | {x['n']} | {x['share']:.0%} | {x['runs']:.2f} | {x['ci'][0]:.2f} to {x['ci'][1]:.2f} |")
    lines += [f"| _All_ | {r['team_games']} | 100% | {L:.2f} | |", "",
              "## Runs by label, starter part only", "",
              "The same cut-offs applied to the starter part alone.", "",
              "| Label | Team-games | Share | Runs scored | 95% range |", "|---|---|---|---|---|"]
    for x in r["starter_labels"]:
        lines.append(f"| {x['label'].replace('bats', 'weak starter').replace('starter', 'starter') if x['label'].endswith('bats') else x['label']} | {x['n']} | {x['share']:.0%} | {x['runs']:.2f} | {x['ci'][0]:.2f} to {x['ci'][1]:.2f} |")
    lines += ["",
              "## Correlation with runs scored", "",
              "| Predictor | Rank correlation | 95% range |", "|---|---|---|"]
    for col, name in (("z", "Edge (both parts)"), ("lineup_part", "Lineup part only"), ("starter_part", "Starter part only")):
        a, b = r[f"rho_{col}_ci"]
        lines.append(f"| {name} | {r[f'rho_{col}']:.3f} | {a:.3f} to {b:.3f} |")
    lines += ["", "## Runs by edge decile", "", "| Decile | Edge from | to | Runs scored |", "|---|---|---|---|"]
    for x in r["deciles"]:
        lines.append(f"| {x['decile']} | {x['z_lo']:+.2f} | {x['z_hi']:+.2f} | {x['runs']:.2f} |")
    lines += ["", "## By month", "", "| Month | Team-games | Correlation |", "|---|---|---|"]
    for x in r["by_month"]:
        lines.append(f"| {x['month']} | {x['n']} | {x['rho_z']:.3f} |")
    lines += ["", "## Limits", "",
              "- Batter lines are monthly, so a game sees only months that had ended (April games lean mostly on last season).",
              "- Monthly wRC+ is combined by plate appearances — a close approximation, not FanGraphs' exact figure.",
              "- Starter scores are frozen at Opening Day, so the starter part is slightly undersold.",
              "- The lineup also faces the bullpen, which the edge ignores.",
              f"- 95% ranges resample whole team-months ({r['team_months']} of them), since a lineup's number barely changes within a month.", ""]
    open(os.path.join(REPO, "EDGES.md"), "w").write("\n".join(lines))


def main():
    os.makedirs(os.path.join(CACHE, "edges"), exist_ok=True)
    df, scale = build(SEASON)
    r = grade(df)
    r.update({"season": SEASON, "scale": {"mid": scale[0], "sd": scale[1]},
              "cutoffs": {"strong": STRONG, "lean": LEAN, "lineup_div": LINEUP_DIV}})
    json.dump(r, open(os.path.join(REPO, "edges_results.json"), "w"), indent=1)
    df.to_csv(os.path.join(CACHE, "edges", f"edges_{SEASON}.csv"), index=False)
    write_report(r, SEASON, scale)
    print("✅ wrote EDGES.md and edges_results.json")


if __name__ == "__main__":
    main()

"""
MLB Pitcher Model — Scorer (with Career)
==========================================
Scoring flow:
1. Load master_all.csv (annual 2021-current season + career rows)
2. Per-season percentile ranks within each season group, among pitchers
   with at least MIN_IP innings (rows below it get no season score, but
   keep the pitcher's composite + tier from his other seasons)
3. Per-pitcher weighted avg across seasons using YEAR_WEIGHTS, each season's
   weight scaled by its innings (full weight at FULL_WEIGHT_IP)
4. Normalize weighted avg to 0-100 composite score
5. Assign tier label
6. Upsert to Supabase: every season is replaced daily (one transaction
   per season), because each pitcher's composite (shared by all his
   rows) moves whenever the current season or career rows change
"""

import os
import gc
import math
import numpy as np
import pandas as pd
from datetime import date
from sb_replace import replace_rows
from season import SEASON

# ─────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────
OUTPUT_DIR = "pitcher_data"
MASTER_CSV = os.path.join(OUTPUT_DIR, "master_all.csv")
SCORED_CSV = os.path.join(OUTPUT_DIR, "scored_all.csv")

# ─────────────────────────────────────────
# SUPABASE (from environment)
# ─────────────────────────────────────────
SUPABASE_URL   = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY", "")
SUPABASE_TABLE = "pitcher_scores"

# ─────────────────────────────────────────
# YEAR WEIGHTS — rolling, relative to the current season (scripts/season.py)
# Newest first: current season, last season, 2 seasons ago, ... Seasons older
# than the window get weight 0 (still shown, not counted).
# season='career' treated as its own period.
# Auto-detects whether the current season has rows yet.
# ─────────────────────────────────────────
def rolling_weights(newest_first, newest_year, career):
    w = {newest_year - i: wt for i, wt in enumerate(newest_first)}
    w["career"] = career
    return w

WEIGHTS_WITH_CURRENT = rolling_weights(
    [0.32, 0.20, 0.12, 0.08, 0.05, 0.03], SEASON, career=0.20)

WEIGHTS_WITHOUT_CURRENT = rolling_weights(
    [0.39, 0.15, 0.10, 0.06, 0.04], SEASON - 1, career=0.26)

# ─────────────────────────────────────────
# STAT WEIGHTS
# (col_name, weight, direction)
# direction: 1=higher better, -1=lower better
# Weights are relative — they sum to 1.02, not 1.00, and that's fine:
# scoring divides by the sum of weights actually present per pitcher.
# ─────────────────────────────────────────
STAT_WEIGHTS = [
    ("Pitching+",  0.11,  1),
    ("SIERA",      0.08, -1),
    ("K-BB%",      0.08,  1),
    ("Stuff+",     0.07,  1),
    ("xERA",       0.06, -1),
    ("CSW%",       0.06,  1),
    ("SwStr%",     0.05,  1),
    ("Location+",  0.05,  1),
    ("K%",         0.05,  1),
    ("Barrel%",    0.04, -1),
    ("HardHit%",   0.04, -1),
    ("FIP-",       0.04, -1),
    ("vFA_pi",     0.04,  1),
    ("BB%",        0.03, -1),
    ("xFIP-",      0.03, -1),
    ("GB%",        0.02,  1),
    ("HR/FB",      0.02, -1),
    ("F-Strike%",  0.02,  1),
    ("IP_per_G",   0.02,  1),
    ("ERA-",       0.02, -1),
    ("HR/9",       0.015,-1),
    ("WHIP",       0.015,-1),
    ("K/BB",       0.01,  1),
    ("K/9",        0.01,  1),
    ("BB/9",       0.01, -1),
    ("LA",         0.01, -1),
    ("BABIP",      0.005,-1),
    ("LOB%",       0.005, 1),
    ("Soft%",      0.005, 1),
    ("AVG",        0.005,-1),
]

# Minimum innings for a season (or career) row to be ranked. Below it a
# 0.00 ERA in 1 IP lands at the top of every percentile and pushes real
# pitchers down, so those rows are left out of the pool entirely.
MIN_IP = 20

# A season's composite weight grows with its innings: full weight at this many
# IP, proportionally less below it. Without this, 20 IP of a new season (weight
# .32) outweighed a whole previous season (.20).
FULL_WEIGHT_IP = 60

# Every composite is blended with an "average pitcher" (score 50) worth this
# much weight (about half a season). Established pitchers barely move; a
# pitcher whose only data is one tiny season is pulled toward average instead
# of ranking like a full season (regression to the mean).
PRIOR_WEIGHT = 0.10
PRIOR_SCORE  = 50.0

# ─────────────────────────────────────────
# COLUMN SANITIZER
# ─────────────────────────────────────────
_EXPLICIT = {
    "ERA-":"era_minus","FIP-":"fip_minus","xFIP-":"xfip_minus",
    "K/9":"k_9","BB/9":"bb_9","K/BB":"k_bb","HR/9":"hr_9",
    "K/9+":"k_9_plus","BB/9+":"bb_9_plus","K/BB+":"k_bb_plus","HR/9+":"hr_9_plus",
    "K%":"k_pct","BB%":"bb_pct","K-BB%":"k_bb_pct","LOB%":"lob_pct",
    "GB%":"gb_pct","LD%":"ld_pct","FB%":"fb_pct","IFFB%":"iffb_pct",
    "HR/FB":"hr_fb","GB/FB":"gb_fb","RS/9":"rs_9",
    "Pull%":"pull_pct","Cent%":"cent_pct","Oppo%":"oppo_pct",
    "Soft%":"soft_pct","Med%":"med_pct","Hard%":"hard_pct",
    "Barrel%":"barrel_pct","HardHit%":"hardhit_pct",
    "O-Swing%":"o_swing_pct","Z-Swing%":"z_swing_pct","Swing%":"swing_pct",
    "O-Contact%":"o_contact_pct","Z-Contact%":"z_contact_pct",
    "Contact%":"contact_pct","Zone%":"zone_pct","F-Strike%":"f_strike_pct",
    "SwStr%":"swstr_pct","CStr%":"cstr_pct","CSW%":"csw_pct",
    "K%+":"k_pct_plus","BB%+":"bb_pct_plus","AVG+":"avg_plus",
    "WHIP+":"whip_plus","BABIP+":"babip_plus","LOB%+":"lob_pct_plus",
    "LD%+":"ld_pct_plus","GB%+":"gb_pct_plus","FB%+":"fb_pct_plus",
    "Stf+ FA":"stf_plus_fa","Stf+ SI":"stf_plus_si","Stf+ FC":"stf_plus_fc",
    "Stf+ FS":"stf_plus_fs","Stf+ SL":"stf_plus_sl","Stf+ CU":"stf_plus_cu",
    "Stf+ CH":"stf_plus_ch","Stf+ KC":"stf_plus_kc","Stf+ FO":"stf_plus_fo",
    "Stuff+":"stuff_plus","Location+":"location_plus","Pitching+":"pitching_plus",
    "vFA (pi)":"vfa_pi","vFA_pi":"vfa_pi","E-FxFIP":"e_fxfip","IP_per_G":"ip_per_g",
}

def sanitize_col(col):
    if col in _EXPLICIT: return _EXPLICIT[col]
    return (col.lower().replace("/","_").replace("%","_pct")
               .replace("-","_").replace("+","_plus")
               .replace("(","").replace(")","").replace(" ","_"))


# ─────────────────────────────────────────
# TIER
# ─────────────────────────────────────────
def get_tier(score):
    if pd.isna(score): return None
    if score >= 90:    return "Elite"
    if score >= 75:    return "Good"
    if score >= 50:    return "Mid"
    if score >= 25:    return "Bad"
    return "Ass"


# ─────────────────────────────────────────
# PLAYER KEY
# Composite scores must group the same human, and names collide (two
# different Logan Allens are active). Freshly scraped rows carry a
# FanGraphs playerid; the static historical CSVs don't.
# ─────────────────────────────────────────
def _norm_pid(v):
    """playerid may arrive as int, float (12345.0) or string."""
    if pd.isna(v):
        return None
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    return s or None


def assign_player_key(df):
    """
    player_key = playerid when the row has one; otherwise the playerid that
    row's name maps to when the name is unambiguous (exactly one id across
    all id-bearing rows); otherwise the name itself.
    """
    df = df.copy()
    if "playerid" in df.columns:
        pids = df["playerid"].map(_norm_pid)
    else:
        pids = pd.Series([None] * len(df), index=df.index, dtype=object)

    with_id = pd.DataFrame({"name": df["name"], "pid": pids}).dropna(subset=["pid"])
    ids_per_name = with_id.groupby("name")["pid"].nunique()
    unique_names = ids_per_name[ids_per_name == 1].index
    name_to_pid  = (with_id[with_id["name"].isin(unique_names)]
                    .drop_duplicates("name").set_index("name")["pid"])

    df["player_key"] = pids.fillna(df["name"].map(name_to_pid)).fillna(df["name"])
    n_ids = pids.notna().sum()
    print(f"   player_key: {n_ids} rows with scraped playerid, "
          f"{len(df) - n_ids} matched by name")
    return df


# ─────────────────────────────────────────
# STEP 1 — PERCENTILE RANKS WITHIN SEASON
# ─────────────────────────────────────────
def compute_season_scores(df):
    """Rank each stat within its season group (pitchers with MIN_IP+ only),
    combine into season_score. Rows below MIN_IP get no season_score."""
    result_frames = []

    for season, grp in df.groupby("season"):
        grp        = grp.copy()
        score_acc  = pd.Series(0.0, index=grp.index)
        weight_acc = pd.Series(0.0, index=grp.index)
        qualified  = pd.to_numeric(grp["IP"], errors="coerce") >= MIN_IP

        for raw_col, weight, direction in STAT_WEIGHTS:
            col = raw_col if raw_col in grp.columns else sanitize_col(raw_col)
            if col not in grp.columns:
                continue
            numeric = pd.to_numeric(grp[col], errors="coerce").where(qualified)
            ranks   = numeric.rank(pct=True, na_option="keep") * 100
            if direction == -1:
                ranks = 100 - ranks
            valid = ranks.notna()
            score_acc[valid]  += ranks[valid] * weight
            weight_acc[valid] += weight

        grp["season_score"] = (
            score_acc / weight_acc.replace(0, np.nan)
        ).round(2)

        result_frames.append(grp)

    return pd.concat(result_frames, ignore_index=True)


# ─────────────────────────────────────────
# STEP 2 — WEIGHTED AVG ACROSS SEASONS
# ─────────────────────────────────────────
def compute_composite_scores(df, year_weights):
    """
    For each pitcher (player_key), compute a weighted avg of their
    season_scores using year_weights, then convert to a percentile rank
    (0-100) so tier cutoffs mean "top X%" and aren't hostage to outliers.
    All of a pitcher's rows share the same composite.
    """
    df = df.copy()
    composite_map = {}

    for key, grp in df.groupby("player_key"):
        num   = 0.0
        denom = 0.0
        for _, row in grp.iterrows():
            season = row["season"]
            score  = row["season_score"]
            if pd.isna(score):
                continue
            # season can be int year or string 'career'
            try:
                season_key = int(season)
            except (ValueError, TypeError):
                season_key = season
            ip     = pd.to_numeric(row["IP"], errors="coerce")
            yw     = year_weights.get(season_key, 0.0) * min(1.0, (ip if ip > 0 else 0) / FULL_WEIGHT_IP)
            num   += score * yw
            denom += yw
        composite_map[key] = (round((num + PRIOR_SCORE * PRIOR_WEIGHT) / (denom + PRIOR_WEIGHT), 2)
                              if denom > 0 else np.nan)

    # Percentile 0-100 against ACTIVE pitchers: a row in the newest season of
    # the data or the one before (live: 2026/2025; backtest: before season Y)
    raw_scores = pd.Series(composite_map).dropna()
    if len(raw_scores) > 0:
        years  = [s for s in df["season"].unique() if isinstance(s, (int, np.integer))]
        newest = max(years) if years else None
        active = set(df.loc[df["season"].isin([newest, newest - 1]), "player_key"]) if newest else set()
        composite_map = active_percentiles(raw_scores, active).to_dict()

    # Every row of a pitcher carries his composite — including a season below
    # MIN_IP (no season score), so early-season rows still show a tier
    df["composite_score"] = df["player_key"].map(composite_map)
    df["tier"]            = df["composite_score"].apply(get_tier)
    return df


# ─────────────────────────────────────────
def active_percentiles(raw, active):
    """0-100 percentile of each raw composite against ACTIVE pitchers only.

    Tier cutoffs ("Elite = top 10%") should describe today's pitchers, not a
    pool full of retired/minor-league arms. Active pitchers are ranked among
    themselves; everyone else is placed on that same scale (share of active
    pitchers at or below his raw composite), so nobody goes blank.
    """
    raw = raw.dropna()
    act = raw[raw.index.isin(active)]
    if act.empty:
        return (raw.rank(pct=True) * 100).round(2)
    out = (act.rank(pct=True) * 100)
    rest = raw[~raw.index.isin(act.index)]
    if len(rest):
        srt = np.sort(act.values)
        out = pd.concat([out, pd.Series(np.searchsorted(srt, rest.values, side="right")
                                        / len(srt) * 100, index=rest.index)])
    return out.round(2)


# ─────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────
def clean_records(df):
    today   = date.today().isoformat()
    records = []
    for _, row in df.iterrows():
        r = {}
        for k, v in row.to_dict().items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)):
                r[k] = None
            elif pd.isna(v) if not isinstance(v, (list, dict)) else False:
                r[k] = None
            else:
                r[k] = v
        r["updated_at"] = today
        records.append(r)
    return records


def sanitize_df_cols(df):
    df = df.copy()
    df.columns = [sanitize_col(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def upsert_to_supabase(df):
    """Replace each season's rows in one transaction (sql/replace_rows.sql).
    A season whose swap fails keeps yesterday's rows; the run goes red."""
    df_san = sanitize_df_cols(df)
    # Push the FanGraphs playerid (unique (playerid, season) lets two
    # same-named pitchers coexist); player_key is internal-only and the
    # table has no such column, so drop it before push.
    df_san = df_san.drop(columns=["player_key"], errors="ignore")

    if not SUPABASE_KEY:
        print("   ⏭️  Supabase skipped (no SUPABASE_KEY)")
        return True

    all_ok = True
    for season_val in list(range(2021, SEASON + 1)) + ["career"]:
        df_yr = df_san[df_san["season"] == season_val]
        if df_yr.empty:
            print(f"   ⏭️  season={season_val} — no data, skipping")
            continue
        ok = replace_rows(SUPABASE_URL, SUPABASE_KEY, SUPABASE_TABLE,
                          clean_records(df_yr), where_col="season",
                          where_val=season_val, label=f"season={season_val}")
        all_ok = all_ok and ok
    return all_ok


# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def run():
    print("=" * 60)
    print("MLB PITCHER MODEL — SCORER (with Career)")
    print("=" * 60)

    # 1. Load
    print(f"\n── Step 1: Loading data ──")
    df = pd.read_csv(MASTER_CSV, low_memory=False)
    # Normalize season: int years become strings for consistent lookup
    df["season"] = df["season"].astype(str).str.strip()
    df["season"] = df["season"].apply(lambda x: int(x) if x.isdigit() else x)
    has_current = (df["season"].astype(str) == str(SEASON)).any()
    year_weights = WEIGHTS_WITH_CURRENT if has_current else WEIGHTS_WITHOUT_CURRENT
    df = assign_player_key(df)
    print(f"   {len(df)} rows × {len(df.columns)} cols")
    print(f"   Mode: {'WITH' if has_current else 'WITHOUT'} {SEASON}")
    print(f"   Year weights: {year_weights}")

    # 2. Season scores
    print(f"\n── Step 2: Season scores (percentile ranks) ──")
    df = compute_season_scores(df)
    for season, grp in df.groupby("season"):
        avg = grp["season_score"].mean()
        print(f"   {season}: {len(grp)} pitchers | avg: {avg:.1f}")

    # 3. Composite scores
    print(f"\n── Step 3: Composite scores (weighted avg → normalized) ──")
    df = compute_composite_scores(df, year_weights)
    print(f"   Range: {df['composite_score'].min():.1f} – {df['composite_score'].max():.1f}")

    # 4. Tier distribution (current season only)
    print(f"\n── Step 4: {SEASON} Tier distribution ──")
    df_cur = df[df["season"] == SEASON]
    for tier in ["Elite","Good","Mid","Bad","Ass"]:
        count = (df_cur["tier"] == tier).sum()
        print(f"   {tier:8s}: {count}")

    # 5. Save
    print(f"\n── Step 5: Saving ──")
    df.to_csv(SCORED_CSV, index=False)
    print(f"   💾 {SCORED_CSV} ({len(df)} rows)")

    # 6. Upsert
    print(f"\n── Step 6: Upserting to Supabase ──")
    ok = upsert_to_supabase(df)

    print(f"\n{'=' * 60}")
    print(f"✅ SCORING COMPLETE — {len(df)} rows")
    print(f"{'=' * 60}")
    if not ok:
        raise SystemExit(1)
    return df


if __name__ == "__main__":
    run()

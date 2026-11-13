"""
Pitcher Splits — Scorer
Reads master_splits.csv, scores all rows, uploads to Supabase.
- season_score: percentile rank within (split + period), among pitchers
  with at least MIN_TBF batters faced (rows below it get no season score
  but keep the composite + tier from the pitcher's other periods)
- composite_split_score: weighted avg across periods per (name, split),
  each period's weight scaled by batters faced (full at FULL_WEIGHT_TBF)
- split_tier: Elite/Good/Mid/Bad/Ass

Daily mode:
  - Replaces every period (one transaction per period), because each
    pitcher's composite (shared by all his rows for a split) moves
    whenever the current season/career change
"""

import os, math
import numpy as np
import pandas as pd
from datetime import date
from sb_replace import replace_rows
from season import SEASON

# ─────────────────────────────────────────
# PATHS
# ─────────────────────────────────────────
MASTER_CSV  = "data/pitcher_splits_data/master_splits.csv"
SCORED_CSV  = "data/pitcher_splits_data/scored_splits.csv"

# ─────────────────────────────────────────
# SUPABASE (from environment)
# ─────────────────────────────────────────
SUPABASE_URL   = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY   = os.environ.get("SUPABASE_KEY", "")
SUPABASE_TABLE = "pitcher_split_scores"

# ─────────────────────────────────────────
# STAT WEIGHTS (splits only stats)
# Weights are relative — they sum to 1.10, not 1.00, and that's fine:
# scoring divides by the sum of weights actually present per pitcher.
# ─────────────────────────────────────────
SPLIT_STAT_WEIGHTS = [
    ("k_bb_pct",  0.16,  1),
    ("k_pct",     0.14,  1),
    ("era",       0.10, -1),
    ("fip",       0.10, -1),
    ("xfip",      0.08, -1),
    ("bb_pct",    0.08, -1),
    ("woba",      0.08, -1),
    ("whip",      0.06, -1),
    ("hard_pct",  0.05, -1),
    ("gb_pct",    0.04,  1),
    ("hr_fb",     0.04, -1),
    ("lob_pct",   0.03,  1),
    ("k_9",       0.03,  1),
    ("bb_9",      0.03, -1),
    ("hr_9",      0.02, -1),
    ("babip",     0.02, -1),
    ("ld_pct",    0.02, -1),
    ("obp",       0.01, -1),
    ("slg",       0.01, -1),
]

# Minimum batters faced for a split row to be ranked. Below it a catcher
# who faced 2 lefties can top the vs-LHH list and pushes real pitchers
# down, so those rows are left out of the pool entirely.
MIN_TBF = 30

# A period's composite weight grows with batters faced: full weight at this
# many, proportionally less below it (a few weeks of a new season shouldn't
# outweigh a whole previous season)
FULL_WEIGHT_TBF = 150

# Every composite is blended with an "average pitcher" (score 50) worth this
# much weight (about half a season). Established pitchers barely move; a
# pitcher whose only data is one tiny season is pulled toward average instead
# of ranking like a full season (regression to the mean).
PRIOR_WEIGHT = 0.10
PRIOR_SCORE  = 50.0

# Rolling, relative to the current season (scripts/season.py), newest first.
# Seasons older than the window get weight 0 (still shown, not counted).
YEAR_WEIGHTS = {str(SEASON - i): w
                for i, w in enumerate([0.32, 0.20, 0.12, 0.08, 0.05, 0.03])}
YEAR_WEIGHTS["career"] = 0.20

def get_tier(score):
    if score is None or (isinstance(score, float) and math.isnan(score)):
        return None
    if score >= 90: return "Elite"
    if score >= 75: return "Good"
    if score >= 50: return "Mid"
    if score >= 25: return "Bad"
    return "Ass"

# ─────────────────────────────────────────
# PLAYER KEY
# Composites must group the same human, and names collide (two different
# Logan Allens are active). Raws from 2026 on carry a FanGraphs
# playerid; the static 2021-2025 raws don't.
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
# SCORING
# ─────────────────────────────────────────
def compute_season_scores(df):
    """Percentile rank within (split + period) group (MIN_TBF+ only).
    Rows below MIN_TBF get no season_score."""
    result_frames = []
    for (split, period), grp in df.groupby(["split","period"]):
        grp = grp.copy()
        score_acc  = pd.Series(0.0, index=grp.index)
        weight_acc = pd.Series(0.0, index=grp.index)
        qualified  = pd.to_numeric(grp["tbf"], errors="coerce") >= MIN_TBF
        for col, weight, direction in SPLIT_STAT_WEIGHTS:
            if col not in grp.columns: continue
            numeric = pd.to_numeric(grp[col], errors="coerce").where(qualified)
            # Normalize 0-1 pct to 0-100
            if numeric.dropna().max() <= 1.5 and col.endswith("_pct"):
                numeric = numeric * 100
            ranks = numeric.rank(pct=True, na_option="keep") * 100
            if direction == -1:
                ranks = 100 - ranks
            valid = ranks.notna()
            score_acc[valid]  += ranks[valid] * weight
            weight_acc[valid] += weight
        grp["season_score"] = (score_acc / weight_acc.replace(0, np.nan)).round(2)
        result_frames.append(grp)
    return pd.concat(result_frames, ignore_index=True) if result_frames else df


def compute_composite_scores(df):
    """Weighted avg of season_scores across periods per (player_key, split),
    then percentile-ranked 0-100 within each split so tier cutoffs mean
    "top X%" and aren't hostage to outliers."""
    df = df.copy()
    composite_map = {}

    for (key, split), grp in df.groupby(["player_key","split"]):
        num, denom = 0.0, 0.0
        for _, row in grp.iterrows():
            period = str(row["period"])
            score  = row.get("season_score")
            if score is None or (isinstance(score, float) and math.isnan(score)): continue
            tbf = pd.to_numeric(row["tbf"], errors="coerce")
            yw = YEAR_WEIGHTS.get(period, 0.0) * min(1.0, (tbf if tbf > 0 else 0) / FULL_WEIGHT_TBF)
            num += float(score) * yw
            denom += yw
        composite_map[(key, split)] = (round((num + PRIOR_SCORE * PRIOR_WEIGHT) / (denom + PRIOR_WEIGHT), 2)
                                       if denom > 0 else np.nan)

    # Percentile rank per split group 0-100
    for split_name in df["split"].unique():
        vals = pd.Series({k: v for k, v in composite_map.items()
                          if k[1] == split_name}).dropna()
        if vals.empty: continue
        composite_map.update((vals.rank(pct=True) * 100).round(2).to_dict())

    df["composite_split_score"] = df.apply(
        lambda r: composite_map.get((r["player_key"], r["split"]), np.nan), axis=1
    )
    # Every row carries the pitcher's composite for that split — including a
    # period below MIN_TBF (no season score), so early-season rows keep a tier
    df["split_tier"] = df["composite_split_score"].apply(get_tier)
    return df


# ─────────────────────────────────────────
# SUPABASE HELPERS
# ─────────────────────────────────────────
def clean_records(df):
    today = date.today().isoformat()
    records = []
    for _, row in df.iterrows():
        r = {}
        for k, v in row.to_dict().items():
            if isinstance(v, float) and (math.isnan(v) or math.isinf(v)): r[k] = None
            elif pd.isna(v) if not isinstance(v, (list, dict)) else False: r[k] = None
            else: r[k] = v
        r["updated_at"] = today
        records.append(r)
    return records

def upsert_to_supabase(df):
    """Replace each (period, split)'s rows in one transaction (sql/replace_rows.sql).
    A period whose swap fails keeps yesterday's rows; the run goes red."""
    if not SUPABASE_KEY:
        print("   ⏭️  Supabase skipped (no SUPABASE_KEY)")
        return True

    # Push the FanGraphs playerid (unique (playerid, period, split) lets
    # two same-named pitchers coexist); player_key is internal-only and the
    # table has no such column, so drop it before push.
    df = df.drop(columns=["player_key"], errors="ignore")

    all_ok = True
    for period in [str(y) for y in range(2021, SEASON + 1)] + ["career"]:
        df_p = df[df["period"] == period].copy()
        # Drop rows where name is null or empty
        df_p = df_p[df_p["name"].notna() & (df_p["name"].str.strip() != "")]
        if df_p.empty:
            print(f"   ⏭️  period={period} — no data")
            continue
        # One swap per (period, split): career with every pitcher is ~9.5k
        # rows, and a single swap that size hit the statement timeout
        for split in sorted(df_p["split"].unique()):
            ok = replace_rows(SUPABASE_URL, SUPABASE_KEY, SUPABASE_TABLE,
                              clean_records(df_p[df_p["split"] == split]),
                              where_col="period", where_val=period,
                              where_col2="split", where_val2=split,
                              label=f"splits_{period}_{split}")
            all_ok = all_ok and ok
    return all_ok

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def run():
    print("=" * 60)
    print("PITCHER SPLITS — SCORER")
    print("=" * 60)

    # 1. Load
    print(f"\n── Step 1: Loading {MASTER_CSV} ──")
    if not os.path.exists(MASTER_CSV):
        print(f"❌ {MASTER_CSV} not found. Run merge_pitcher_splits.py first.")
        raise SystemExit(1)
    df = pd.read_csv(MASTER_CSV, low_memory=False)
    df = assign_player_key(df)
    print(f"   {len(df)} rows × {len(df.columns)} cols")
    print(f"   Periods: {sorted(df['period'].unique().tolist())}")
    print(f"   Splits:  {sorted(df['split'].unique().tolist())}")

    # 2. Season scores
    print(f"\n── Step 2: Season scores (within split+period) ──")
    df = compute_season_scores(df)
    for (split, period), grp in df.groupby(["split","period"]):
        avg = grp["season_score"].mean()
        print(f"   {split:8} {period:7}: {len(grp)} pitchers | avg: {avg:.1f}")

    # 3. Composite scores
    print(f"\n── Step 3: Composite split scores ──")
    df = compute_composite_scores(df)
    for split, grp in df.groupby("split"):
        # Show current-season tier dist
        df_cur = grp[grp["period"]==str(SEASON)]
        if not df_cur.empty:
            dist = df_cur["split_tier"].value_counts()
            print(f"   {split}: " + " | ".join(f"{t}:{dist.get(t,0)}" for t in ["Elite","Good","Mid","Bad","Ass"]))

    # 4. Save
    print(f"\n── Step 4: Saving {SCORED_CSV} ──")
    df.to_csv(SCORED_CSV, index=False)
    print(f"   💾 {len(df)} rows saved")

    # 5. Upload
    print(f"\n── Step 5: Upserting to Supabase ──")
    ok = upsert_to_supabase(df)

    print(f"\n{'=' * 60}")
    print(f"✅ SPLITS SCORING COMPLETE — {len(df)} rows")
    print(f"{'=' * 60}")
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    run()

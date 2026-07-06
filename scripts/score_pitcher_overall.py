"""
MLB Pitcher Model — Scorer (with Career)
==========================================
Scoring flow:
1. Load master_all.csv (annual 2021-2026 + career rows)
2. Per-season percentile ranks within each season group
3. Per-pitcher weighted avg across seasons using YEAR_WEIGHTS
4. Normalize weighted avg to 0-100 composite score
5. Assign tier label
6. Upsert to Supabase:
   - Delete 2026 + career rows daily (re-scraped daily)
   - 2021-2025 inserted once, never deleted
"""

import os
import gc
import math
import numpy as np
import pandas as pd
import requests
from datetime import date

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
BATCH_SIZE     = 500

SUPABASE_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

# ─────────────────────────────────────────
# YEAR WEIGHTS
# season='career' treated as its own period
# Auto-detects whether 2026 is active
# ─────────────────────────────────────────
WEIGHTS_WITH_2026 = {
    2021:     0.03,
    2022:     0.05,
    2023:     0.08,
    2024:     0.12,
    2025:     0.20,
    2026:     0.32,
    "career": 0.20,
}

WEIGHTS_WITHOUT_2026 = {
    2021:     0.04,
    2022:     0.06,
    2023:     0.10,
    2024:     0.15,
    2025:     0.39,
    "career": 0.26,
}

# ─────────────────────────────────────────
# STAT WEIGHTS
# (col_name, weight, direction)
# direction: 1=higher better, -1=lower better
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
    if pd.isna(score): return "Unknown"
    if score >= 90:    return "Elite"
    if score >= 75:    return "Good"
    if score >= 50:    return "Mid"
    if score >= 25:    return "Bad"
    return "Ass"


# ─────────────────────────────────────────
# STEP 1 — PERCENTILE RANKS WITHIN SEASON
# ─────────────────────────────────────────
def compute_season_scores(df):
    """Rank each stat within its season group, combine into season_score."""
    result_frames = []

    for season, grp in df.groupby("season"):
        grp        = grp.copy()
        score_acc  = pd.Series(0.0, index=grp.index)
        weight_acc = pd.Series(0.0, index=grp.index)

        for raw_col, weight, direction in STAT_WEIGHTS:
            col = raw_col if raw_col in grp.columns else sanitize_col(raw_col)
            if col not in grp.columns:
                continue
            numeric = pd.to_numeric(grp[col], errors="coerce")
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
    For each pitcher (name), compute a weighted avg of their season_scores
    using year_weights. Normalize to 0-100. All rows share the same composite.
    """
    df = df.copy()
    composite_map = {}

    for name, grp in df.groupby("name"):
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
            yw     = year_weights.get(season_key, 0.0)
            num   += score * yw
            denom += yw
        composite_map[name] = round(num / denom, 2) if denom > 0 else np.nan

    # Normalize 0-100 across all pitchers
    raw_scores = pd.Series(composite_map)
    raw_scores = raw_scores.dropna()
    if len(raw_scores) > 0:
        min_s = raw_scores.min()
        max_s = raw_scores.max()
        if max_s > min_s:
            normalized = ((raw_scores - min_s) / (max_s - min_s) * 100).round(2)
        else:
            normalized = raw_scores
        composite_map = normalized.to_dict()

    df["composite_score"] = df["name"].map(composite_map)
    df["tier"]            = df["composite_score"].apply(get_tier)
    return df


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


def delete_season(season_val):
    """Delete rows where season = season_val (works for int years and 'career')."""
    print(f"   🗑️  Deleting season={season_val}...")
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?season=eq.{season_val}",
        headers=SUPABASE_HEADERS,
        timeout=30,
    )
    if r.status_code in (200, 204):
        print(f"   ✅ Cleared")
    else:
        print(f"   ❌ Failed [{r.status_code}]: {r.text[:200]}")


def season_exists(season_val):
    r = requests.get(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?season=eq.{season_val}&limit=1",
        headers=SUPABASE_HEADERS,
        timeout=15,
    )
    return r.status_code == 200 and len(r.json()) > 0


def insert_batches(records, label="", upsert=False):
    pushed = 0
    headers = SUPABASE_HEADERS.copy()
    if upsert:
        headers["Prefer"] = "resolution=merge-duplicates"
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i : i + BATCH_SIZE]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
            headers=headers,
            json=batch,
            timeout=30,
        )
        if r.status_code in (200, 201):
            pushed += len(batch)
            print(f"   ✅ {label} batch {i//BATCH_SIZE+1}: {len(batch)} rows")
        else:
            print(f"   ❌ {label} batch {i//BATCH_SIZE+1} failed [{r.status_code}]: {r.text[:300]}")
    return pushed


def sanitize_df_cols(df):
    df = df.copy()
    df.columns = [sanitize_col(c) for c in df.columns]
    df = df.loc[:, ~df.columns.duplicated()]
    return df


def upsert_to_supabase(df, force_reseed=False):
    """
    force_reseed=True : wipe entire table, reinsert all years (first run)
    force_reseed=False: only delete+reinsert 2026 and career (daily)
    """
    df_san = sanitize_df_cols(df)

    if not SUPABASE_KEY:
        print("   ⏭️  Supabase skipped (no SUPABASE_KEY)")
        return True

    if force_reseed:
        print("   🗑️  Full reseed — clearing all rows...")
        r = requests.delete(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?updated_at=gte.2000-01-01T00:00:00Z",
            headers=SUPABASE_HEADERS,
            timeout=30,
        )
        print(f"   {'✅ Cleared' if r.status_code in (200,204) else f'❌ Failed [{r.status_code}]'}")
        seasons_to_insert = [2021, 2022, 2023, 2024, 2025, 2026, "career"]
    else:
        # Daily: only refresh 2026 and career
        for season_val in [2026, "career"]:
            delete_season(season_val)
        seasons_to_insert = []
        for year in [2021, 2022, 2023, 2024, 2025]:
            if season_exists(year):
                print(f"   ⏭️  season={year} already exists — skipping")
            else:
                seasons_to_insert.append(year)
        seasons_to_insert += [2026, "career"]

    all_ok = True
    for season_val in seasons_to_insert:
        df_yr = df_san[df_san["season"] == season_val]
        if df_yr.empty:
            print(f"   ⏭️  season={season_val} — no data, skipping")
            continue
        records = clean_records(df_yr)
        pushed  = insert_batches(records, label=f"season={season_val}", upsert=True)
        print(f"   📊 season={season_val}: {pushed}/{len(records)} rows pushed")
        if pushed != len(records):
            all_ok = False
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
    has_2026 = (df["season"].astype(str) == '2026').any()
    year_weights = WEIGHTS_WITH_2026 if has_2026 else WEIGHTS_WITHOUT_2026
    print(f"   {len(df)} rows × {len(df.columns)} cols")
    print(f"   Mode: {'WITH 2026' if has_2026 else 'WITHOUT 2026'}")
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

    # 4. Tier distribution (2026 only)
    print(f"\n── Step 4: 2026 Tier distribution ──")
    df_2026 = df[df["season"] == 2026]
    for tier in ["Elite","Good","Mid","Bad","Ass"]:
        count = (df_2026["tier"] == tier).sum()
        print(f"   {tier:8s}: {count}")

    # 5. Save
    print(f"\n── Step 5: Saving ──")
    df.to_csv(SCORED_CSV, index=False)
    print(f"   💾 {SCORED_CSV} ({len(df)} rows)")

    # 6. Upsert
    print(f"\n── Step 6: Upserting to Supabase ──")
    ok = upsert_to_supabase(df, force_reseed=False)

    print(f"\n{'=' * 60}")
    print(f"✅ SCORING COMPLETE — {len(df)} rows")
    print(f"{'=' * 60}")
    if not ok:
        raise SystemExit(1)
    return df


if __name__ == "__main__":
    run()

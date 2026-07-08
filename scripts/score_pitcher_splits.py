"""
Pitcher Splits — Scorer
Reads master_splits.csv, scores all rows, uploads to Supabase.
- season_score: percentile rank within (split + period)
- composite_split_score: weighted avg across periods per (name, split)
- split_tier: Elite/Good/Mid/Bad/Ass

Daily mode:
  - Deletes 2026 + career rows then reinserts fresh
  - Historical rows (2021-2025) inserted once, never touched again
"""

import os, math, time
import numpy as np
import pandas as pd
import requests
from datetime import date

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
BATCH_SIZE     = 500

SB_HEADERS = {
    "apikey":        SUPABASE_KEY,
    "Authorization": f"Bearer {SUPABASE_KEY}",
    "Content-Type":  "application/json",
    "Prefer":        "return=minimal",
}

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

YEAR_WEIGHTS = {
    "2021":   0.03,
    "2022":   0.05,
    "2023":   0.08,
    "2024":   0.12,
    "2025":   0.20,
    "2026":   0.32,
    "career": 0.20,
}

def get_tier(score):
    if score is None or (isinstance(score, float) and math.isnan(score)):
        return "Unknown"
    if score >= 90: return "Elite"
    if score >= 75: return "Good"
    if score >= 50: return "Mid"
    if score >= 25: return "Bad"
    return "Ass"

# ─────────────────────────────────────────
# PLAYER KEY
# Composites must group the same human, and names collide (two different
# Logan Allens are active). The 2026/career raws carry a FanGraphs
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
    """Percentile rank within (split + period) group."""
    result_frames = []
    for (split, period), grp in df.groupby(["split","period"]):
        grp = grp.copy()
        score_acc  = pd.Series(0.0, index=grp.index)
        weight_acc = pd.Series(0.0, index=grp.index)
        for col, weight, direction in SPLIT_STAT_WEIGHTS:
            if col not in grp.columns: continue
            numeric = pd.to_numeric(grp[col], errors="coerce")
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
            yw = YEAR_WEIGHTS.get(period, 0.0)
            num += float(score) * yw
            denom += yw
        composite_map[(key, split)] = round(num / denom, 2) if denom > 0 else np.nan

    # Percentile rank per split group 0-100
    for split_name in df["split"].unique():
        vals = pd.Series({k: v for k, v in composite_map.items()
                          if k[1] == split_name}).dropna()
        if vals.empty: continue
        composite_map.update((vals.rank(pct=True) * 100).round(2).to_dict())

    df["composite_split_score"] = df.apply(
        lambda r: composite_map.get((r["player_key"], r["split"]), np.nan), axis=1
    )
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

def sb_request(method, url, json=None, timeout=30):
    """Supabase call with up to 3 attempts (5s/15s backoff) on network
    errors and 5xx. Returns the last Response, or None if all attempts
    raised."""
    last = None
    for attempt in range(3):
        if attempt:
            wait = (5, 15)[attempt - 1]
            print(f"   🔄 retry {attempt+1}/3 in {wait}s...")
            time.sleep(wait)
        try:
            last = requests.request(method, url, headers=SB_HEADERS,
                                    json=json, timeout=timeout)
            if last.status_code < 500:
                return last
            print(f"   ⚠️  [{last.status_code}] {last.text[:150]}")
        except requests.RequestException as e:
            print(f"   ⚠️  {e}")
    return last

def delete_period(period):
    """Returns True on success — callers must NOT insert the period's fresh
    rows if this failed, or the table gets duplicates."""
    print(f"   🗑️  Deleting period={period}...")
    r = sb_request("DELETE", f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?period=eq.{period}")
    if r is not None and r.status_code in (200, 204):
        print("   ✅ Cleared")
        return True
    print(f"   ❌ [{r.status_code if r is not None else 'no response'}]")
    return False

def period_exists(period):
    """On any failure, assume the period exists — skipping a reinsert is
    harmless, reinserting on top of existing rows duplicates them."""
    r = sb_request("GET", f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?period=eq.{period}&limit=1",
                   timeout=15)
    if r is None or r.status_code != 200:
        print(f"   ⚠️  Could not check period={period} — assuming it exists")
        return True
    try:
        return len(r.json()) > 0
    except ValueError:
        return True

def insert_batches(records, label=""):
    pushed = 0
    for i in range(0, len(records), BATCH_SIZE):
        batch = records[i:i+BATCH_SIZE]
        r = sb_request("POST", f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}", json=batch)
        if r is not None and r.status_code in (200, 201):
            pushed += len(batch)
            print(f"   ✅ {label} batch {i//BATCH_SIZE+1}: {len(batch)} rows")
        else:
            code = r.status_code if r is not None else "no response"
            body = r.text[:200] if r is not None else ""
            print(f"   ❌ {label} batch {i//BATCH_SIZE+1} [{code}]: {body}")
    return pushed

def upsert_to_supabase(df, force_reseed=False):
    """
    force_reseed=True : wipe entire table, reinsert all periods
    force_reseed=False: delete 2026+career, skip historical if exists
    """
    if not SUPABASE_KEY:
        print("   ⏭️  Supabase skipped (no SUPABASE_KEY)")
        return True

    # pitcher_split_scores has no playerid/player_key columns (yet) —
    # PostgREST rejects whole batches containing unknown columns.
    df = df.drop(columns=["playerid", "player_key"], errors="ignore")

    all_ok = True
    if force_reseed:
        print("   🗑️  Full reseed — clearing all rows...")
        r = sb_request(
            "DELETE",
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?updated_at=gte.2000-01-01T00:00:00Z",
        )
        if r is None or r.status_code not in (200, 204):
            print("   ❌ Clear failed — aborting push, old data left in place")
            return False
        print("   ✅ Cleared")
        periods_to_insert = ["2021","2022","2023","2024","2025","2026","career"]
    else:
        periods_to_insert = []
        for p in ["2021","2022","2023","2024","2025"]:
            if period_exists(p):
                print(f"   ⏭️  period={p} already exists — skipping")
            else:
                periods_to_insert.append(p)
        # Daily: refresh 2026 and career, but only where the delete worked
        for p in ["2026","career"]:
            if delete_period(p):
                periods_to_insert.append(p)
            else:
                print(f"   🚫 period={p}: delete failed — old rows kept, reinsert skipped")
                all_ok = False

    for period in periods_to_insert:
        df_p = df[df["period"] == period].copy()
        # Drop rows where name is null or empty
        df_p = df_p[df_p["name"].notna() & (df_p["name"].str.strip() != "")]
        if df_p.empty:
            print(f"   ⏭️  period={period} — no data")
            continue
        records = clean_records(df_p)
        pushed  = insert_batches(records, label=f"splits_{period}")
        print(f"   📊 {period}: {pushed}/{len(records)} rows pushed")
        if pushed != len(records):
            all_ok = False
    return all_ok

# ─────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────
def run(force_reseed=False):
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
        # Show 2026 tier dist
        df_26 = grp[grp["period"]=="2026"]
        if not df_26.empty:
            dist = df_26["split_tier"].value_counts()
            print(f"   {split}: " + " | ".join(f"{t}:{dist.get(t,0)}" for t in ["Elite","Good","Mid","Bad","Ass"]))

    # 4. Save
    print(f"\n── Step 4: Saving {SCORED_CSV} ──")
    df.to_csv(SCORED_CSV, index=False)
    print(f"   💾 {len(df)} rows saved")

    # 5. Upload
    print(f"\n── Step 5: Upserting to Supabase ──")
    ok = upsert_to_supabase(df, force_reseed=force_reseed)

    print(f"\n{'=' * 60}")
    print(f"✅ SPLITS SCORING COMPLETE — {len(df)} rows")
    print(f"{'=' * 60}")
    if not ok:
        raise SystemExit(1)

if __name__ == "__main__":
    import sys
    force = "--reseed" in sys.argv
    run(force_reseed=force)

"""
MLB Pitcher Model — Merger (Local, with Career)
Combines all annual CSVs (9 tabs x 6 years) + 9 career CSVs into:
  - master_YYYY.csv per year
  - master_career.csv
  - master_all.csv (all years + career, season='career')
"""

import os
import gc
import pandas as pd
import numpy as np

# Scrapers write fresh 2026/career CSVs here; the workflow copies the static
# 2021-2025 CSVs in from pitcher/historical/ before this runs.
UPLOAD_DIR = "pitcher_data"
OUTPUT_DIR = "pitcher_data"

YEARS = [2021, 2022, 2023, 2024, 2025, 2026]

ANNUAL_TAB_FILES = [
    ("Dashboard",      "Stats_Dashboard"),
    ("Standard",       "Stats_Standard"),
    ("Advanced",       "Stats_Advanced"),
    ("BattedBall",     "Stats_BattedBall"),
    ("Statcast",       "Stats_Statcast"),
    ("AdvancedPlus",   "Stats_+Stats"),
    ("PlateDiscipline","PlateDiscipline"),
    ("PitchVelocity",  "PitchVelocity"),
    ("StuffPlus",      "Pitch_StuffPlus"),
]

CAREER_TAB_FILES = [
    ("Dashboard",      "Stats_Dashboard_career"),
    ("Standard",       "Stats_Standard_career"),
    ("Advanced",       "Stats_Advanced_career"),
    ("BattedBall",     "Stats_BattedBall_career"),
    ("Statcast",       "Stats_Statcast_career"),
    ("AdvancedPlus",   "Stats_+Stats_career"),
    ("PlateDiscipline","PlateDiscipline_career"),
    ("PitchVelocity",  "PitchVelocity_career"),
    ("StuffPlus",      "Pitch_StuffPlus_career"),
]

RENAME_MAP = {
    "Name":     "name",
    "Team":     "team",
    "E-F":      "E-FxFIP",
    "vFA (pi)": "vFA_pi",
}

def load_tab(filepath, year):
    df = pd.read_csv(filepath, low_memory=False)
    df = df.rename(columns=RENAME_MAP)
    df.columns = [str(c).strip() for c in df.columns]
    df = df.drop(columns=["rank"], errors="ignore")
    df["name"]   = df["name"].astype(str).str.strip()
    df["team"]   = df["team"].astype(str).str.strip()
    df["season"] = year
    for col in df.columns:
        if col not in ("name", "team", "season"):
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def merge_tabs(tab_files, year, label=""):
    print(f"\n── Merging {label or year} ──")
    all_cols = {"name", "team", "season"}
    tab_dfs  = []

    for tab_name, prefix in tab_files:
        fpath = os.path.join(UPLOAD_DIR, f"{prefix}.csv") if year == "career" \
                else os.path.join(UPLOAD_DIR, f"{prefix}{year}.csv")
        if not os.path.exists(fpath):
            print(f"   ⚠️  Missing: {os.path.basename(fpath)} — skipping")
            continue
        df   = load_tab(fpath, year)
        keep = ["name","team","season"] + [c for c in df.columns if c not in all_cols]
        df   = df[keep].copy()
        all_cols.update(df.columns)
        tab_dfs.append(df)
        print(f"   ✅ {tab_name}: {len(df)} rows, {len(df.columns)-3} new cols")
        gc.collect()

    if not tab_dfs:
        return pd.DataFrame()

    merged = tab_dfs[0]
    for df in tab_dfs[1:]:
        new_cols = [c for c in df.columns if c not in ("name","team","season")]
        merged = pd.merge(
            merged,
            df[["name","team","season"] + new_cols],
            on=["name","team","season"],
            how="left"
        )
        del df
        gc.collect()

    del tab_dfs
    gc.collect()

    if "IP" in merged.columns and "G" in merged.columns:
        merged["IP_per_G"] = (
            pd.to_numeric(merged["IP"], errors="coerce") /
            pd.to_numeric(merged["G"],  errors="coerce").replace(0, np.nan)
        ).round(3)

    print(f"   📊 {len(merged)} pitchers × {len(merged.columns)} columns")
    return merged


def run():
    print("=" * 60)
    print("MLB PITCHER MODEL — MERGER (with Career)")
    print("=" * 60)

    master_path = os.path.join(OUTPUT_DIR, "master_all.csv")
    first_write = True

    # ── Annual years ──────────────────────────────────────────
    for year in YEARS:
        df = merge_tabs(ANNUAL_TAB_FILES, year)
        if df.empty:
            continue
        year_path = os.path.join(OUTPUT_DIR, f"master_{year}.csv")
        df.to_csv(year_path, index=False)
        print(f"   💾 master_{year}.csv saved")
        df.to_csv(master_path, index=False,
                  mode="w" if first_write else "a",
                  header=first_write)
        first_write = False
        del df
        gc.collect()
        print(f"   🧹 Memory freed")

    # ── Career ────────────────────────────────────────────────
    df_career = merge_tabs(CAREER_TAB_FILES, "career", label="Career (2017-2026)")
    if not df_career.empty:
        career_path = os.path.join(OUTPUT_DIR, "master_career.csv")
        df_career.to_csv(career_path, index=False)
        print(f"   💾 master_career.csv saved")
        df_career.to_csv(master_path, index=False,
                         mode="a", header=False)
        del df_career
        gc.collect()
        print(f"   🧹 Memory freed")

    print(f"\n{'=' * 60}")
    print(f"✅ MERGE COMPLETE → {master_path}")
    print(f"{'=' * 60}")


if __name__ == "__main__":
    run()

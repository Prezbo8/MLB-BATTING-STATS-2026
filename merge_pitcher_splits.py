"""
Pitcher Splits — Merger
Combines all raw split CSVs into master_splits.csv
Run after all 3 scrapers have finished.
"""

import os
import gc
import pandas as pd

# Static 2021-2025 raw CSVs are committed in this repo dir;
# the two splits scrapers write fresh 2026/career raws into it each run.
INPUT_DIR   = "pitcher_splits_data"
OUTPUT_PATH = "pitcher_splits_data/master_splits.csv"

PERIODS = ["2021","2022","2023","2024","2025","2026","career"]

def run():
    print("=" * 60)
    print("PITCHER SPLITS — MERGER")
    print("=" * 60)

    frames = []
    for period in PERIODS:
        csv_path = os.path.join(INPUT_DIR, f"splits_{period}_raw.csv")
        if not os.path.exists(csv_path):
            print(f"   ⚠️  Missing: splits_{period}_raw.csv — skipping")
            continue
        df = pd.read_csv(csv_path, low_memory=False)
        frames.append(df)
        print(f"   ✅ {period}: {len(df)} rows, {len(df.columns)} cols")
        gc.collect()

    if not frames:
        print("❌ No CSV files found in", INPUT_DIR)
        return

    master = pd.concat(frames, ignore_index=True)
    # Drop rows where name is null/empty (merger artifact from outer join)
    before = len(master)
    master = master[master["name"].notna() & (master["name"].astype(str).str.strip() != "")]
    dropped = before - len(master)
    if dropped > 0:
        print(f"   🧹 Dropped {dropped} null-name rows")
    master.to_csv(OUTPUT_PATH, index=False)

    print(f"\n✅ MERGE COMPLETE")
    print(f"   Total rows : {len(master)}")
    print(f"   Periods    : {master['period'].unique().tolist()}")
    print(f"   Splits     : {master['split'].unique().tolist()}")
    print(f"   Output     : {OUTPUT_PATH}")

if __name__ == "__main__":
    run()

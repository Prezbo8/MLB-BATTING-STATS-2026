"""
Pitcher Splits — 2026 Season Scraper (API version)
Fully self-contained. Fetches 4 splits x 3 statgroups from the FanGraphs
splits JSON API (no browser), merges on playerId, saves CSV.

Output matches the old Selenium scraper's splits_2026_raw.csv format:
percent stats on the 0-100 scale, grid-style rounding.

All-or-nothing: if any table still fails after retries, exits 1 and
writes nothing, so the downstream merger/scorer never see partial data.
"""

import os, time, traceback
from datetime import date
import pandas as pd
import requests

# ── CONFIG ─────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

OUTPUT_DIR   = "data/pitcher_splits_data"
OUTPUT_CSV   = os.path.join(OUTPUT_DIR, "splits_2026_raw.csv")
PERIOD       = "2026"
GROUP_BY     = "season"
AUTO_PT      = "false"     # 2026: include ALL pitchers regardless of TBF
SPLIT_PREFIX = []          # career adds the [42] qualifier split; 2026 doesn't
START_DATE   = "2026-03-01"
END_DATE     = "2026-11-01"

DELAY        = 3
MAX_RETRIES  = 5
RETRY_WAITS  = [10, 30, 60, 120, 300]

SPLITS = {"vsLHH": 5, "vsRHH": 6, "Home": 9, "Away": 10}

API_URL = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
    "Referer": "https://www.fangraphs.com/leaders/splits-leaderboards",
    "Origin": "https://www.fangraphs.com",
}

# Output columns: (csv_col, statgroup, api_key, kind)
# kind: raw | count | rate1 | rate2 | rate3 | pct1 (x100, 1 decimal)
COLUMN_SPEC = [
    ("season_label", 1, "Season",      "raw"),
    ("name",         1, "playerName",  "raw"),
    ("team",         1, "TeamNameAbb", "raw"),
    ("g",            1, "G",           "count"),
    ("tbf",          1, "TBF",         "count"),
    ("era",          1, "ERA",         "rate2"),
    ("h",            1, "H",           "count"),
    ("doubles",      1, "2B",          "count"),
    ("triples",      1, "3B",          "count"),
    ("r",            1, "R",           "count"),
    ("er",           1, "ER",          "count"),
    ("hr",           1, "HR",          "count"),
    ("bb",           1, "BB",          "count"),
    ("ibb",          1, "IBB",         "count"),
    ("hbp",          1, "HBP",         "count"),
    ("so",           1, "SO",          "count"),
    ("avg",          1, "AVG",         "rate3"),
    ("obp",          1, "OBP",         "rate3"),
    ("slg",          1, "SLG",         "rate3"),
    ("woba",         1, "wOBA",        "rate3"),
    ("ip",           2, "IP",          "rate1"),
    ("k_9",          2, "K/9",         "rate2"),
    ("bb_9",         2, "BB/9",        "rate2"),
    ("k_bb",         2, "K/BB",        "rate2"),
    ("hr_9",         2, "HR/9",        "rate2"),
    ("k_pct",        2, "K%",          "pct1"),
    ("bb_pct",       2, "BB%",         "pct1"),
    ("k_bb_pct",     2, "K-BB%",       "pct1"),
    ("whip",         2, "WHIP",        "rate2"),
    ("babip",        2, "BABIP",       "rate3"),
    ("lob_pct",      2, "LOB%",        "pct1"),
    ("fip",          2, "FIP",         "rate2"),
    ("xfip",         2, "xFIP",        "rate2"),
    ("gb_fb",        3, "GB/FB",       "rate2"),
    ("ld_pct",       3, "LD%",         "pct1"),
    ("gb_pct",       3, "GB%",         "pct1"),
    ("fb_pct",       3, "FB%",         "pct1"),
    ("iffb_pct",     3, "IFFB%",       "pct1"),
    ("hr_fb",        3, "HR/FB",       "pct1"),
    ("ifh_pct",      3, "IFH%",        "pct1"),
    ("buh_pct",      3, "BUH%",        "pct1"),
    ("pull_pct",     3, "Pull%",       "pct1"),
    ("cent_pct",     3, "Cent%",       "pct1"),
    ("oppo_pct",     3, "Oppo%",       "pct1"),
    ("soft_pct",     3, "Soft%",       "pct1"),
    ("med_pct",      3, "Med%",        "pct1"),
    ("hard_pct",     3, "Hard%",       "pct1"),
]

def tg(msg):
    if not TELEGRAM_TOKEN:
        return
    try:
        requests.post(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                      data={"chat_id": TELEGRAM_CHAT_ID, "text": msg, "parse_mode": "HTML"}, timeout=10)
    except Exception:
        pass

def fetch_sg(split_arr, sg):
    payload = {
        "strPlayerId": "all",
        "strSplitArr": SPLIT_PREFIX + [split_arr],
        "strSplitArrPitch": [],
        "strGroup": GROUP_BY,
        "strPosition": "P",
        "strType": sg,
        "strStartDate": START_DATE,
        "strEndDate": END_DATE,
        "strSplitTeams": False,
        "dctFilters": [],       # empty -> server applies auto PT filter when strAutoPt is true
        "strStatType": "player",
        "strAutoPt": AUTO_PT,
        "arrPlayerId": [],
        "arrWxTemperature": None,
        "arrWxPressure": None,
        "arrWxAirDensity": None,
        "arrWxElevation": None,
        "arrWxWindSpeed": None,
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=60)
    r.raise_for_status()
    rows = r.json().get("data", [])
    if not rows:
        raise ValueError("API returned no rows")
    return {row["playerId"]: row for row in rows}

def fetch_sg_with_retries(split_name, split_arr, sg):
    label = f"{PERIOD}_{split_name}_sg{sg}"
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"   🌐 {label} (attempt {attempt}/{MAX_RETRIES})")
            return fetch_sg(split_arr, sg)
        except Exception as e:
            print(f"   ⚠️  {label}: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_WAITS[min(attempt - 1, len(RETRY_WAITS) - 1)]
                print(f"   🔄 Retrying in {wait}s")
                time.sleep(wait)
    raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {label}")

def fmt(value, kind):
    if value is None or kind == "raw":
        return value
    try:
        v = float(value)
    except (TypeError, ValueError):
        return None
    if kind == "count":
        return v
    if kind == "rate1":
        return round(v, 1)
    if kind == "rate2":
        return round(v, 2)
    if kind == "rate3":
        return round(v, 3)
    if kind == "pct1":
        return round(v * 100, 1)
    return v

def build_split_frame(split_name, split_arr):
    sg_rows = {}
    for sg in (1, 2, 3):
        sg_rows[sg] = fetch_sg_with_retries(split_name, split_arr, sg)
        time.sleep(DELAY)

    records = []
    for pid, base in sg_rows[1].items():
        rec = {}
        for csv_col, sg, api_key, kind in COLUMN_SPEC:
            row = sg_rows[sg].get(pid) if sg != 1 else base
            rec[csv_col] = fmt(row.get(api_key), kind) if row else None
        rec["playerid"] = pid
        rec["split"]  = split_name
        rec["period"] = PERIOD
        records.append(rec)

    df = pd.DataFrame(records)
    df["name"] = df["name"].astype(str).str.strip()
    df["team"] = df["team"].astype(str).str.strip()
    df = df[df["name"].notna() & (df["name"].str.strip() != "")]
    return df

def run():
    today = date.today().strftime("%Y-%m-%d")
    print(f"\n🚀 Pitcher Splits — {PERIOD} (API) — {today}")
    tg(f"⚾ <b>Pitcher Splits {PERIOD} started</b>\n📅 {today}")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    all_frames = []

    try:
        for split_name, split_arr in SPLITS.items():
            merged = build_split_frame(split_name, split_arr)
            all_frames.append(merged)
            print(f"   ✅ {split_name}: {len(merged)} rows, {len(merged.columns)} cols")
    except Exception as e:
        print(f"\n💥 Crash:\n{traceback.format_exc()}")
        tg(f"💥 <b>Splits {PERIOD} CRASHED</b>\n🚫 Nothing written\n❌ {str(e)[:200]}")
        raise SystemExit(1)

    final = pd.concat(all_frames, ignore_index=True)
    final = final.drop_duplicates(subset=["playerid", "split", "period"], keep="first")
    final.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 Saved: {OUTPUT_CSV} ({len(final)} rows, {len(final.columns)} cols)")
    tg(f"✅ <b>Splits {PERIOD} Done</b>\n📊 {len(final)} rows\n📅 {today}")

if __name__ == "__main__":
    run()

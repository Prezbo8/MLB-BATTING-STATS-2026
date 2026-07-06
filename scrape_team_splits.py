"""
FanGraphs Team Splits Leaderboard Scraper (API version)
========================================================
Fetches 20 tables from the FanGraphs splits JSON API (no browser needed),
saves to CSV, pushes to GitHub, upserts to Supabase.

Secrets come from environment variables (set as GitHub Actions secrets):
    GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
Steps whose env vars are missing are skipped (handy for local dry runs).

Usage:
    pip install pandas requests
    python scrape_team_splits.py
"""

import os
import time
import base64
import traceback
import requests
import pandas as pd
from datetime import date, timedelta

# ── Secrets (from environment) ────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
GITHUB_TOKEN     = os.environ.get("GITHUB_TOKEN", "")
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")

# ── GitHub ────────────────────────────────────────────────────────────────────
GITHUB_USERNAME = "Prezbo8"
GITHUB_REPO     = "MLB-BATTING-STATS-2026"
GITHUB_BRANCH   = "main"
GITHUB_CSV_PATH = "_ALL_SPLITS_COMBINED.csv"   # path inside the repo

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_TABLE = "fangraphs_splits"

# ── Settings ──────────────────────────────────────────────────────────────────
OUTPUT_DIR          = "fangraphs_splits"
COMBINED_CSV        = os.path.join(OUTPUT_DIR, "_ALL_SPLITS_COMBINED.csv")
DELAY_BETWEEN_CALLS = 3
RETRY_DELAY         = 15
MAX_RETRY_ROUNDS    = 3
SEASON_START        = "2026-3-18"

API_URL = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
    "Referer": "https://www.fangraphs.com/leaders/splits-leaderboards",
    "Origin": "https://www.fangraphs.com",
}

# Output columns, in the same order the old Selenium scraper produced them
STAT_COLS = ["#", "Season", "Tm", "PA", "BB%", "K%", "BB/K", "AVG", "OBP",
             "SLG", "OPS", "ISO", "BABIP", "wRC", "wRAA", "wOBA", "wRC+"]

# ── Splits & date ranges ──────────────────────────────────────────────────────
SPLITS = [
    ("no_split", []),
    ("vs_lhp",   [1]),
    ("vs_rhp",   [2]),
    ("home",     [7]),
    ("away",     [8]),
]

def get_date_ranges():
    today = date.today()
    fmt = lambda d: f"{d.year}-{d.month}-{d.day}"
    return [
        ("season",  SEASON_START,                    fmt(today)),
        ("last_30", fmt(today - timedelta(days=30)), fmt(today)),
        ("last_14", fmt(today - timedelta(days=14)), fmt(today)),
        ("last_7",  fmt(today - timedelta(days=7)),  fmt(today)),
    ]

# ── Telegram ──────────────────────────────────────────────────────────────────
def send_telegram(message):
    if not TELEGRAM_TOKEN:
        print("   ⏭️  Telegram skipped (no TELEGRAM_TOKEN)")
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"},
            timeout=10
        )
    except Exception as e:
        print(f"   ⚠️  Telegram failed: {e}")

# ── FanGraphs API ─────────────────────────────────────────────────────────────
def fetch_split(split_arr, start_date, end_date):
    payload = {
        "strPlayerId": "all",
        "strSplitArr": split_arr,
        "strSplitArrPitch": [],
        "strGroup": "season",
        "strPosition": "B",
        "strType": 2,
        "strStartDate": start_date,
        "strEndDate": end_date,
        "strSplitTeams": False,
        "dctFilters": [{"stat": "PA", "comp": "gt", "low": 0, "high": -99, "auto": False}],
        "strStatType": "team",
        "strAutoPt": "false",
        "arrPlayerId": [],
        "arrWxTemperature": None,
        "arrWxPressure": None,
        "arrWxAirDensity": None,
        "arrWxElevation": None,
        "arrWxWindSpeed": None,
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])

def build_table(rows):
    """Turn API rows into a DataFrame matching the old scraped-grid format."""
    df = pd.DataFrame(rows)
    df = df.sort_values("wRC+", ascending=False).reset_index(drop=True)
    out = pd.DataFrame()
    out["#"]      = range(1, len(df) + 1)
    out["Season"] = df["Season"].astype(float)
    out["Tm"]     = df["TeamNameAbb"]
    out["PA"]     = df["PA"].astype(float)
    out["BB%"]    = (df["BB%"] * 100).map(lambda v: f"{v:.1f}%")
    out["K%"]     = (df["K%"] * 100).map(lambda v: f"{v:.1f}%")
    out["BB/K"]   = df["BB/K"].round(1)
    for col in ["AVG", "OBP", "SLG", "OPS", "ISO", "BABIP"]:
        out[col] = df[col].round(3)
    out["wRC"]    = df["wRC"].round(0)
    out["wRAA"]   = df["wRAA"].round(1)
    out["wOBA"]   = df["wOBA"].round(3)
    out["wRC+"]   = df["wRC+"].round(0)
    return out

def scrape_table(split_arr, start_date, end_date, name):
    rows = fetch_split(split_arr, start_date, end_date)
    if not rows:
        print(f"   ⚠️  {name}: API returned no rows")
        return None
    return build_table(rows)

# ── GitHub push ───────────────────────────────────────────────────────────────
def push_to_github(csv_path):
    print("\n📤 Pushing CSV to GitHub...")
    if not GITHUB_TOKEN:
        print("   ⏭️  Skipped (no GITHUB_TOKEN)")
        return False
    api = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{GITHUB_CSV_PATH}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json"
    }

    with open(csv_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    # Check if file already exists in repo (need its SHA to update)
    sha = None
    r = requests.get(api, headers=headers)
    if r.status_code == 200:
        sha = r.json().get("sha")

    payload = {
        "message": f"Update splits data {date.today().isoformat()}",
        "content": content,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = requests.put(api, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201):
        print("   ✅ CSV pushed to GitHub successfully")
        return True
    else:
        print(f"   ❌ GitHub push failed: {r.status_code} {r.text[:200]}")
        return False

# ── Supabase: delete-all then insert fresh ────────────────────────────────────
def upsert_to_supabase(csv_path):
    print("\n📤 Pushing fresh data to Supabase (delete → insert)...")
    if not SUPABASE_KEY:
        print("   ⏭️  Skipped (no SUPABASE_KEY)")
        return False
    import math

    df = pd.read_csv(csv_path)
    df = df.drop(columns=["#"], errors="ignore")
    df["updated_at"] = date.today().isoformat()

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "Season": "season", "Tm": "tm", "PA": "pa",
        "BB%": "bb_pct", "K%": "k_pct", "BB/K": "bb_per_k",
        "AVG": "avg", "OBP": "obp", "SLG": "slg", "OPS": "ops",
        "ISO": "iso", "BABIP": "babip", "wRC": "wrc",
        "wRAA": "wraa", "wOBA": "woba", "wRC+": "wrcplus",
    })
    valid_cols = [
        "split", "date_range", "start_date", "end_date", "season", "tm", "pa",
        "bb_pct", "k_pct", "bb_per_k", "avg", "obp", "slg", "ops", "iso",
        "babip", "wrc", "wraa", "woba", "wrcplus", "updated_at"
    ]
    df = df[[c for c in valid_cols if c in df.columns]]

    # Clean NaN → None for JSON safety
    df = df.where(pd.notnull(df), other=None)
    records = df.to_dict(orient="records")
    records = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]

    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    # ── Step 1: Delete ALL existing rows ─────────────────────────────────────
    print("   🗑️  Deleting all existing rows...")
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?updated_at=gte.2000-01-01T00:00:00Z",
        headers=headers,
        timeout=30
    )
    if r.status_code in (200, 204):
        print("   ✅ Table cleared")
    else:
        print(f"   ❌ Delete failed: {r.status_code} {r.text[:200]}")
        return False

    # ── Step 2: Insert today's fresh rows in batches ──────────────────────────
    batch_size = 500
    total      = len(records)
    pushed     = 0

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        r = requests.post(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
            headers=headers,
            json=batch,
            timeout=30
        )
        if r.status_code in (200, 201):
            pushed += len(batch)
            print(f"   ✅ Batch {i // batch_size + 1}: {len(batch)} rows inserted")
        else:
            print(f"   ❌ Batch {i // batch_size + 1} failed: {r.status_code} {r.text[:200]}")

    print(f"   📊 Total inserted: {pushed}/{total} rows")
    return pushed == total

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    today_str  = date.today().strftime("%Y-%m-%d")

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    date_ranges = get_date_ranges()
    all_tables = [
        (sl, sa, rl, sd, ed)
        for sl, sa in SPLITS
        for rl, sd, ed in date_ranges
    ]
    total = len(all_tables)

    print(f"\n🚀 FanGraphs Splits Scraper (API) — {today_str}")
    print(f"   {total} tables | Season start: {SEASON_START}")
    print(f"   Output -> ./{OUTPUT_DIR}/\n")

    send_telegram(
        f"⚾ <b>FanGraphs Scraper Started</b>\n"
        f"📅 {today_str}\n"
        f"📊 Scraping {total} tables..."
    )

    saved_paths = []

    try:
        failed = []
        pending = list(all_tables)
        for round_no in range(MAX_RETRY_ROUNDS + 1):
            if not pending:
                break
            if round_no > 0:
                print(f"\n── Retry round {round_no}/{MAX_RETRY_ROUNDS}: {len(pending)} table(s) ──")
                print(f"   Waiting {RETRY_DELAY}s...")
                time.sleep(RETRY_DELAY)
            failed = []
            for n, table in enumerate(pending, 1):
                sl, sa, rl, sd, ed = table
                name = f"{sl}__{rl}"
                print(f"[{n:02d}/{len(pending)}] {name}  ({sd} -> {ed})")
                try:
                    df = scrape_table(sa, sd, ed, name)
                except Exception as e:
                    print(f"       ⚠️  {e}")
                    df = None
                if df is not None and not df.empty:
                    df.insert(0, "split",      sl)
                    df.insert(1, "date_range", rl)
                    df.insert(2, "start_date", sd)
                    df.insert(3, "end_date",   ed)
                    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
                    df.to_csv(path, index=False)
                    print(f"       ✅ {len(df)} teams -> {path}")
                    saved_paths.append(path)
                else:
                    print(f"       ❌ Failed — will retry")
                    failed.append(table)
                time.sleep(DELAY_BETWEEN_CALLS)
            pending = failed

        ok = total - len(failed)
        failed_names = [f"{t[0]}__{t[2]}" for t in failed]

    except Exception as e:
        err = traceback.format_exc()
        print(f"\n💥 Crash:\n{err}")
        send_telegram(
            f"💥 <b>FanGraphs Scraper CRASHED</b>\n"
            f"📅 {today_str}\n"
            f"❌ {str(e)[:200]}"
        )
        raise

    # ── Build combined CSV ────────────────────────────────────────────────────
    combined_rows = 0
    if saved_paths:
        combined = pd.concat([pd.read_csv(p) for p in saved_paths], ignore_index=True)
        combined["updated_at"] = today_str
        combined.to_csv(COMBINED_CSV, index=False)
        combined_rows = len(combined)
        print(f"\n📊 Combined CSV: {combined_rows} rows -> {COMBINED_CSV}")

    # ── Push to GitHub ────────────────────────────────────────────────────────
    github_ok = False
    if os.path.exists(COMBINED_CSV):
        github_ok = push_to_github(COMBINED_CSV)

    # ── Upsert to Supabase ────────────────────────────────────────────────────
    supabase_ok = False
    if os.path.exists(COMBINED_CSV):
        supabase_ok = upsert_to_supabase(COMBINED_CSV)

    # ── Final summary ─────────────────────────────────────────────────────────
    elapsed = round((time.time() - start_time) / 60, 1)
    status  = "✅" if not failed else "⚠️"
    failed_msg  = f"\n❌ Still failed: {', '.join(failed_names)}" if failed_names else ""
    github_msg  = "✅ GitHub pushed" if github_ok  else "❌ GitHub failed"
    supabase_msg= "✅ Supabase updated" if supabase_ok else "❌ Supabase failed"

    print(f"\n{'─'*55}")
    print(f"Saved: {ok}/{total} | Rows: {combined_rows} | Time: {elapsed}min")
    print(github_msg)
    print(supabase_msg)

    send_telegram(
        f"{status} <b>FanGraphs Scraper Done</b>\n"
        f"📅 {today_str}\n"
        f"📊 {ok}/{total} tables saved\n"
        f"📁 {combined_rows} total rows\n"
        f"⏱️ Took {elapsed} min\n"
        f"{github_msg}\n"
        f"{supabase_msg}"
        f"{failed_msg}"
    )

    if failed or not (github_ok or supabase_ok):
        raise SystemExit(1)

if __name__ == "__main__":
    main()

"""
TeamRankings MLB Runs Per Game Scraper (API-free, no browser)
==============================================================
Fetches https://www.teamrankings.com/mlb/stat/runs-per-game daily.
Columns: Rank, Team, 2026, Last 3, Last 1, Home, Away, 2025

Saves to CSV, pushes to GitHub, upserts to Supabase.
Sends Telegram notifications.

Secrets from env: GITHUB_TOKEN, SUPABASE_URL, SUPABASE_KEY,
                  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
Steps whose env vars are missing are skipped (handy for local dry runs).

Usage:
    pip install pandas requests lxml
    python scrape_rpg.py
"""

import io
import os
import time
import math
import base64
import traceback
import requests
import pandas as pd
from datetime import date

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
GITHUB_CSV_PATH = "data/_RPG.csv"

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_TABLE = "mlb_rpg"

# ── Settings ──────────────────────────────────────────────────────────────────
OUTPUT_DIR  = "rpg_data"
OUTPUT_CSV  = os.path.join(OUTPUT_DIR, "_RPG.csv")
URL         = "https://www.teamrankings.com/mlb/stat/runs-per-game"
MAX_RETRIES = 5
RETRY_WAITS = [10, 30, 60, 120, 300]

HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
}

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

# ── GitHub push ───────────────────────────────────────────────────────────────
def push_to_github(csv_path):
    print("\n📤 Pushing to GitHub...")
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
    sha = None
    r = requests.get(api, headers=headers)
    if r.status_code == 200:
        sha = r.json().get("sha")
    payload = {
        "message": f"Update RPG data {date.today().isoformat()}",
        "content": content,
        "branch": GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha
    r = requests.put(api, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201):
        print("   ✅ Pushed to GitHub")
        return True
    print(f"   ❌ GitHub failed: {r.status_code} {r.text[:200]}")
    return False

# ── Supabase: delete all rows then insert fresh data ─────────────────────────
def upsert_to_supabase(df):
    print("\n📤 Pushing to Supabase (delete → insert)...")
    if not SUPABASE_KEY:
        print("   ⏭️  Skipped (no SUPABASE_KEY)")
        return False
    headers = {
        "apikey": SUPABASE_KEY,
        "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json",
        "Prefer": "return=minimal"
    }

    # Step 1: delete ALL existing rows (wipe the table clean)
    del_r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?scraped_date=gte.2000-01-01",
        headers=headers,
        timeout=30
    )
    if del_r.status_code in (200, 204):
        print("   🗑️  Old rows deleted")
    else:
        print(f"   ⚠️  Delete returned {del_r.status_code}: {del_r.text[:200]}")

    # Step 2: insert today's fresh records
    df = df.where(pd.notnull(df), other=None)
    records = df.to_dict(orient="records")
    records = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]
    ins_r = requests.post(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
        headers={**headers, "Prefer": "return=minimal"},
        json=records,
        timeout=30
    )
    if ins_r.status_code in (200, 201):
        print(f"   ✅ {len(records)} rows inserted to Supabase")
        return True
    print(f"   ❌ Supabase insert failed: {ins_r.status_code} {ins_r.text[:300]}")
    return False

# ── Scrape the table ──────────────────────────────────────────────────────────
def scrape_rpg():
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"📥 Fetching {URL} (attempt {attempt}/{MAX_RETRIES})...")
            r = requests.get(URL, headers=HEADERS, timeout=60)
            r.raise_for_status()
            tables = pd.read_html(io.StringIO(r.text))
            if not tables or tables[0].empty:
                raise ValueError("No table found in page")
            df = tables[0]
            print(f"   Headers: {list(df.columns)}")
            return df
        except Exception as e:
            print(f"   ⚠️  Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_WAITS[min(attempt - 1, len(RETRY_WAITS) - 1)]
                print(f"   🔄 Retrying in {wait}s")
                time.sleep(wait)
    return None

# ── Clean and format the DataFrame ───────────────────────────────────────────
def clean_df(df, today_str):
    # Normalize column names
    col_map = {}
    for col in df.columns:
        cl = str(col).lower().strip()
        if cl == "rank":           col_map[col] = "rank"
        elif cl == "team":         col_map[col] = "team"
        elif "2026" in cl:         col_map[col] = "szn_2026"
        elif "last 3" in cl:       col_map[col] = "last_3"
        elif "last 1" in cl:       col_map[col] = "last_1"
        elif cl == "home":         col_map[col] = "home"
        elif cl == "away":         col_map[col] = "away"
        elif "2025" in cl:         col_map[col] = "szn_2025"
        else:                      col_map[col] = cl.replace(" ", "_")

    df = df.rename(columns=col_map)

    # Clean team names — strip trailing record/rank suffixes if present
    if "team" in df.columns:
        df["team"] = df["team"].astype(str).str.replace(r"\s*\(.*\)", "", regex=True).str.strip()

    # Add scraped date
    df["scraped_date"] = today_str

    # Keep only known columns that exist
    valid_cols = ["rank", "team", "szn_2026", "last_3", "last_1",
                  "home", "away", "szn_2025", "scraped_date"]
    df = df[[c for c in valid_cols if c in df.columns]]

    return df

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    today_str  = date.today().isoformat()

    os.makedirs(OUTPUT_DIR, exist_ok=True)

    print(f"\n🚀 RPG Scraper — {today_str}")

    send_telegram(
        f"⚾ <b>RPG Scraper Started</b>\n"
        f"📅 {today_str}\n"
        f"🌐 teamrankings.com/mlb/stat/runs-per-game"
    )

    try:
        df = scrape_rpg()
    except Exception as e:
        print(f"\n💥 Crash:\n{traceback.format_exc()}")
        send_telegram(f"💥 <b>RPG Scraper CRASHED</b>\n📅 {today_str}\n❌ {str(e)[:200]}")
        raise SystemExit(1)

    if df is None or df.empty:
        send_telegram(f"❌ <b>RPG Scraper Failed</b>\n📅 {today_str}\nNo data extracted — nothing pushed")
        raise SystemExit(1)

    print(f"   ✅ {len(df)} teams scraped")
    print(df.head(5).to_string(index=False))

    # Clean
    df = clean_df(df, today_str)

    # Save CSV
    df.to_csv(OUTPUT_CSV, index=False)
    print(f"\n💾 Saved -> {OUTPUT_CSV}")

    # Push to GitHub
    github_ok = push_to_github(OUTPUT_CSV)

    # Upsert to Supabase
    supabase_ok = upsert_to_supabase(df)

    elapsed = round((time.time() - start_time) / 60, 2)
    status  = "✅" if github_ok and supabase_ok else "⚠️"
    github_msg   = "✅ GitHub pushed"    if github_ok   else "❌ GitHub failed"
    supabase_msg = "✅ Supabase updated" if supabase_ok else "❌ Supabase failed"

    print(f"\n{'─'*50}")
    print(f"{github_msg} | {supabase_msg} | ⏱️ {elapsed}min")

    send_telegram(
        f"{status} <b>RPG Scraper Done</b>\n"
        f"📅 {today_str}\n"
        f"📊 {len(df)} teams\n"
        f"⏱️ {elapsed} min\n"
        f"{github_msg}\n"
        f"{supabase_msg}"
    )

    if (GITHUB_TOKEN and not github_ok) or (SUPABASE_KEY and not supabase_ok):
        raise SystemExit(1)

if __name__ == "__main__":
    main()

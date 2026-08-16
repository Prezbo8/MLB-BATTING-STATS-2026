"""
FanGraphs Player Splits — HISTORICAL MONTHLY DUMP
=================================================
One-off / re-runnable backfill that captures PLAYER batting stats one calendar
month at a time (March→October) and stores them in a SEPARATE Supabase table
(`fangraphs_player_splits_history`) so live `fangraphs_player_splits` is untouched.

Difference from the daily scraper (scrape_player_splits.py):
  • Date windows are calendar MONTHS, not the relative season/last_30/14/7.
  • `date_range` holds a month key ("2026-03"); start_date/end_date = the window.
  • Months that haven't started yet are skipped; the current month is clipped to
    today (a partial capture that a later re-run overwrites once it closes).
  • Supabase push is delete-just-this-run's-months → insert (idempotent, and it
    never wipes other months already in the archive). No GitHub push.

Secrets come from environment variables (GitHub Actions secrets):
    SUPABASE_URL, SUPABASE_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
Steps whose env vars are missing are skipped (handy for local dry runs).
"""

import os
import time
import math
import calendar
import traceback
import requests
import pandas as pd
from datetime import date

# ── Secrets (from environment) ────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")

# ── Supabase ──────────────────────────────────────────────────────────────────
SUPABASE_TABLE = "fangraphs_player_splits_history"

# ── Settings ──────────────────────────────────────────────────────────────────
OUTPUT_DIR          = "fangraphs_player_splits_history"
COMBINED_CSV        = os.path.join(OUTPUT_DIR, "_ALL_PLAYER_SPLITS_HISTORY.csv")
DELAY_BETWEEN_CALLS = 3
RETRY_BACKOFFS      = [15, 30, 60, 120, 300, 600]  # last value repeats
RETRY_BUDGET_SECONDS = 45 * 60   # give up (and push nothing) after this long
SEASON_YEAR         = 2026
SEASON_MONTHS       = range(3, 11)   # March (3) → October (10)

API_URL = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
    "Referer": "https://www.fangraphs.com/leaders/splits-leaderboards",
    "Origin": "https://www.fangraphs.com",
}

# ── Splits & month windows ────────────────────────────────────────────────────
SPLITS = [
    ("no_split", []),
    ("vs_lhp",   [1]),
    ("vs_rhp",   [2]),
    ("home",     [7]),
    ("away",     [8]),
]

def get_month_windows():
    """(month_key, start_date, end_date) per calendar month that has started.
    The current month is clipped to today (partial); future months are skipped."""
    today = date.today()
    fmt = lambda d: f"{d.year}-{d.month}-{d.day}"
    windows = []
    for m in SEASON_MONTHS:
        start = date(SEASON_YEAR, m, 1)
        if start > today:
            break  # month hasn't started yet
        last_day = calendar.monthrange(SEASON_YEAR, m)[1]
        end = date(SEASON_YEAR, m, last_day)
        if end > today:
            end = today  # partial current month
        windows.append((f"{SEASON_YEAR}-{m:02d}", fmt(start), fmt(end)))
    return windows

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
        "dctFilters": [{"stat": "PA", "comp": "gt", "low": 1, "high": -99, "auto": False}],
        "strStatType": "player",
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
    """Turn API rows into a DataFrame matching the daily scraper's format."""
    df = pd.DataFrame(rows)
    df = df.sort_values("wRC+", ascending=False).reset_index(drop=True)
    out = pd.DataFrame()
    out["#"]      = range(1, len(df) + 1)
    out["Season"] = df["Season"].astype(float)
    out["Name"]   = df["playerName"]
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

# ── Supabase: delete just this run's months, then insert ──────────────────────
def push_to_supabase(csv_path, month_keys):
    print("\n📤 Pushing to Supabase history (delete this run's months → insert)...")
    if not SUPABASE_KEY:
        print("   ⏭️  Skipped (no SUPABASE_KEY)")
        return False

    df = pd.read_csv(csv_path)
    df = df.drop(columns=["#"], errors="ignore")
    df["updated_at"] = date.today().isoformat()

    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "Season": "season", "Name": "name", "Tm": "tm", "PA": "pa",
        "BB%": "bb_pct", "K%": "k_pct", "BB/K": "bb_per_k",
        "AVG": "avg", "OBP": "obp", "SLG": "slg", "OPS": "ops",
        "ISO": "iso", "BABIP": "babip", "wRC": "wrc",
        "wRAA": "wraa", "wOBA": "woba", "wRC+": "wrcplus",
    })
    valid_cols = [
        "split", "date_range", "start_date", "end_date",
        "season", "name", "tm", "pa",
        "bb_pct", "k_pct", "bb_per_k", "avg", "obp", "slg", "ops", "iso",
        "babip", "wrc", "wraa", "woba", "wrcplus", "updated_at"
    ]
    df = df[[c for c in valid_cols if c in df.columns]]

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

    # ── Step 1: Delete only the months this run is refreshing ─────────────────
    months_list = ",".join(month_keys)
    print(f"   🗑️  Deleting existing rows for months: {months_list}")
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?date_range=in.({months_list})",
        headers=headers,
        timeout=30
    )
    if r.status_code in (200, 204):
        print("   ✅ Old rows for these months cleared")
    else:
        print(f"   ❌ Delete failed: {r.status_code} {r.text[:200]}")
        return False

    # ── Step 2: Insert fresh rows in batches ──────────────────────────────────
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
    windows = get_month_windows()
    month_keys = [w[0] for w in windows]
    all_tables = [
        (sl, sa, mk, sd, ed)
        for mk, sd, ed in windows
        for sl, sa in SPLITS
    ]
    total = len(all_tables)

    print(f"\n🚀 FanGraphs PLAYER Splits — Historical Monthly Dump — {today_str}")
    print(f"   Months: {', '.join(month_keys)}")
    print(f"   {total} tables ({len(windows)} months × {len(SPLITS)} splits)")
    print(f"   Output -> ./{OUTPUT_DIR}/  |  Supabase table: {SUPABASE_TABLE}\n")

    send_telegram(
        f"⚾ <b>Player Splits History Dump Started</b>\n"
        f"📅 {today_str}\n"
        f"🗓️ Months: {', '.join(month_keys)}\n"
        f"📊 {total} tables..."
    )

    saved_paths = []
    try:
        failed = []
        pending = list(all_tables)
        round_no = 0
        while pending:
            if round_no > 0:
                delay = RETRY_BACKOFFS[min(round_no - 1, len(RETRY_BACKOFFS) - 1)]
                if time.time() - start_time + delay > RETRY_BUDGET_SECONDS:
                    print(f"\n⏰ Retry budget ({RETRY_BUDGET_SECONDS // 60} min) exhausted "
                          f"with {len(pending)} table(s) still failing.")
                    break
                print(f"\n── Retry round {round_no}: {len(pending)} table(s) ──")
                print(f"   Waiting {delay}s...")
                time.sleep(delay)
            failed = []
            for n, table in enumerate(pending, 1):
                sl, sa, mk, sd, ed = table
                name = f"{mk}__{sl}"
                print(f"[{n:02d}/{len(pending)}] {name}  ({sd} -> {ed})")
                try:
                    df = scrape_table(sa, sd, ed, name)
                except Exception as e:
                    print(f"       ⚠️  {e}")
                    df = None
                if df is not None and not df.empty:
                    df.insert(0, "split",      sl)
                    df.insert(1, "date_range", mk)
                    df.insert(2, "start_date", sd)
                    df.insert(3, "end_date",   ed)
                    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
                    df.to_csv(path, index=False)
                    print(f"       ✅ {len(df)} players -> {path}")
                    saved_paths.append(path)
                else:
                    print(f"       ❌ Failed — will retry")
                    failed.append(table)
                time.sleep(DELAY_BETWEEN_CALLS)
            pending = failed
            round_no += 1

        ok = total - len(failed)
        failed_names = [f"{t[2]}__{t[0]}" for t in failed]

    except Exception as e:
        err = traceback.format_exc()
        print(f"\n💥 Crash:\n{err}")
        send_telegram(
            f"💥 <b>Player Splits History Dump CRASHED</b>\n📅 {today_str}\n❌ {str(e)[:200]}"
        )
        raise

    # ── All-or-nothing: never push partial data ───────────────────────────────
    if failed:
        elapsed = round((time.time() - start_time) / 60, 1)
        print(f"\n❌ Only {ok}/{total} tables succeeded after {elapsed} min of retries.")
        print("   Nothing pushed — existing Supabase data left untouched.")
        send_telegram(
            f"❌ <b>Player Splits History Dump GAVE UP</b>\n"
            f"📅 {today_str}\n📊 Only {ok}/{total} tables after {elapsed} min\n"
            f"🚫 Nothing pushed\n❌ Failed: {', '.join(failed_names)}"
        )
        raise SystemExit(1)

    # ── Build combined CSV ────────────────────────────────────────────────────
    combined_rows = 0
    if saved_paths:
        combined = pd.concat([pd.read_csv(p) for p in saved_paths], ignore_index=True)
        combined["updated_at"] = today_str
        combined.to_csv(COMBINED_CSV, index=False)
        combined_rows = len(combined)
        print(f"\n📊 Combined CSV: {combined_rows} rows -> {COMBINED_CSV}")

    # ── Push to Supabase ──────────────────────────────────────────────────────
    supabase_ok = False
    if os.path.exists(COMBINED_CSV):
        supabase_ok = push_to_supabase(COMBINED_CSV, month_keys)

    elapsed = round((time.time() - start_time) / 60, 1)
    supabase_msg = "✅ Supabase updated" if supabase_ok else "❌ Supabase failed"
    print(f"\n{'─'*55}")
    print(f"Saved: {ok}/{total} | Rows: {combined_rows} | Time: {elapsed}min")
    print(supabase_msg)

    send_telegram(
        f"✅ <b>Player Splits History Dump Done</b>\n"
        f"📅 {today_str}\n🗓️ {', '.join(month_keys)}\n"
        f"📊 {ok}/{total} tables · {combined_rows} rows\n"
        f"⏱️ {elapsed} min\n{supabase_msg}"
    )

    if not supabase_ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

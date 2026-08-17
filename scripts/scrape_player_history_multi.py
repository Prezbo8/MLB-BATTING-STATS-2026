"""
FanGraphs Player MULTI-YEAR History Scraper (2021–2026)
=======================================================
Backfills per-YEAR player batting stats so the site can chart a player's
season-over-season trajectory (regular 2021→2026) or the same month across
years (e.g. June 2021 → June 2026).

Uses FanGraphs' month-of-season split codes (so the buckets match the site
exactly, including the combined Mar/Apr and Sep/Oct groupings):
    84 = Mar/Apr   85 = May   86 = Jun   87 = Jul   88 = Aug   89 = Sep/Oct
crossed with handedness (1 = vs LHP, 2 = vs RHP; none = overall). Each split is
run once PER YEAR with that year's date range, which returns that year's value
(a full-range date span would instead aggregate all six years into one row).

Writes to a SEPARATE table `fangraphs_player_history`, keyed by the stable
FanGraphs `playerid` so seasons join cleanly across name/team changes.

Secrets from env (GitHub Actions): SUPABASE_URL, SUPABASE_KEY, TELEGRAM_TOKEN,
TELEGRAM_CHAT_ID. Missing ones are skipped (handy for local dry runs).
"""

import os
import time
import math
import calendar
import traceback
import requests
import pandas as pd
from datetime import date

# ── Secrets ───────────────────────────────────────────────────────────────────
TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
SUPABASE_URL     = os.environ.get("SUPABASE_URL", "")
SUPABASE_KEY     = os.environ.get("SUPABASE_KEY", "")

SUPABASE_TABLE = "fangraphs_player_history"

# ── Settings ──────────────────────────────────────────────────────────────────
OUTPUT_DIR          = "fangraphs_player_history"
COMBINED_CSV        = os.path.join(OUTPUT_DIR, "_ALL_PLAYER_HISTORY.csv")
DELAY_BETWEEN_CALLS = 3
RETRY_BACKOFFS      = [15, 30, 60, 120, 300, 600]
RETRY_BUDGET_SECONDS = 60 * 60
YEARS               = range(2021, 2027)   # 2021 … 2026

API_URL = "https://www.fangraphs.com/api/leaders/splits/splits-leaders"
HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
    "Referer": "https://www.fangraphs.com/leaders/splits-leaderboards",
    "Origin": "https://www.fangraphs.com",
}

# Handedness (split value, FanGraphs codes) and month-of-season periods.
HANDS = [
    ("no_split", []),
    ("vs_lhp",   [1]),
    ("vs_rhp",   [2]),
]
PERIODS = [
    ("season",  None),   # full year (no month code)
    ("mar_apr", 84),
    ("may",     85),
    ("jun",     86),
    ("jul",     87),
    ("aug",     88),
    ("sep_oct", 89),
]

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
        "arrWxTemperature": None, "arrWxPressure": None, "arrWxAirDensity": None,
        "arrWxElevation": None, "arrWxWindSpeed": None,
    }
    r = requests.post(API_URL, json=payload, headers=HEADERS, timeout=60)
    r.raise_for_status()
    return r.json().get("data", [])

def build_table(rows):
    df = pd.DataFrame(rows)
    df = df.sort_values("wRC+", ascending=False).reset_index(drop=True)
    out = pd.DataFrame()
    out["playerid"] = df["playerId"]
    out["Name"]     = df["playerName"]
    out["Tm"]       = df["TeamNameAbb"]
    out["PA"]       = df["PA"].astype(float)
    out["BB%"]      = (df["BB%"] * 100).map(lambda v: f"{v:.1f}%")
    out["K%"]       = (df["K%"] * 100).map(lambda v: f"{v:.1f}%")
    out["BB/K"]     = df["BB/K"].round(1)
    for col in ["AVG", "OBP", "SLG", "OPS", "ISO", "BABIP"]:
        out[col] = df[col].round(3)
    out["wRC"]  = df["wRC"].round(0)
    out["wRAA"] = df["wRAA"].round(1)
    out["wOBA"] = df["wOBA"].round(3)
    out["wRC+"] = df["wRC+"].round(0)
    return out

def scrape_table(split_arr, start_date, end_date, name):
    rows = fetch_split(split_arr, start_date, end_date)
    if not rows:
        print(f"   ⚠️  {name}: API returned no rows")
        return None
    return build_table(rows)

# ── Windows: (split, period, year, split_arr, start, end) per started year ─────
def get_windows():
    today = date.today()
    fmt = lambda d: f"{d.year}-{d.month}-{d.day}"
    windows = []
    for y in YEARS:
        start = date(y, 3, 1)
        if start > today:
            continue
        end = date(y, 11, 1)
        if end > today:
            end = today
        for hand, hcodes in HANDS:
            for period, pcode in PERIODS:
                arr = list(hcodes) + ([pcode] if pcode is not None else [])
                windows.append((hand, period, y, arr, fmt(start), fmt(end)))
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
            timeout=10)
    except Exception as e:
        print(f"   ⚠️  Telegram failed: {e}")

# ── League-average rows (playerid=0) so the site's LG line survives re-runs ────
def league_avg_rows(df):
    """PA-weighted league average per (split, period, season), as playerid=0 rows.
    Regenerated on every run so a delete-all backfill never drops the LG line."""
    def wavg(g, col):
        v = pd.to_numeric(g[col], errors="coerce")
        w = pd.to_numeric(g["pa"], errors="coerce")
        m = v.notna() & w.notna()
        return (v[m] * w[m]).sum() / w[m].sum() if w[m].sum() > 0 else None
    def wpct(g, col):
        v = pd.to_numeric(g[col].astype(str).str.replace("%", "", regex=False), errors="coerce")
        w = pd.to_numeric(g["pa"], errors="coerce")
        m = v.notna() & w.notna()
        return f"{(v[m] * w[m]).sum() / w[m].sum():.1f}%" if w[m].sum() > 0 else None
    rows = []
    for (sp, pe, se), g in df.groupby(["split", "period", "season"]):
        wrc = wavg(g, "wrcplus")
        rows.append({
            "playerid": 0, "split": sp, "period": pe, "season": se,
            "name": "LG AVG", "tm": "LG",
            "pa": pd.to_numeric(g["pa"], errors="coerce").sum(),
            "bb_pct": wpct(g, "bb_pct"), "k_pct": wpct(g, "k_pct"),
            "avg": wavg(g, "avg"), "obp": wavg(g, "obp"), "slg": wavg(g, "slg"),
            "ops": wavg(g, "ops"), "iso": wavg(g, "iso"), "babip": wavg(g, "babip"),
            "woba": wavg(g, "woba"), "wrcplus": round(wrc) if wrc is not None else None,
            "updated_at": g["updated_at"].iloc[0] if "updated_at" in g else None,
        })
    return pd.DataFrame(rows)

# ── Supabase: delete-all then insert ──────────────────────────────────────────
def push_to_supabase(csv_path):
    print("\n📤 Pushing player history to Supabase (delete all → insert)...")
    if not SUPABASE_KEY:
        print("   ⏭️  Skipped (no SUPABASE_KEY)")
        return False

    df = pd.read_csv(csv_path)
    df.columns = [c.strip() for c in df.columns]
    df = df.rename(columns={
        "Name": "name", "Tm": "tm", "PA": "pa",
        "BB%": "bb_pct", "K%": "k_pct", "BB/K": "bb_per_k",
        "AVG": "avg", "OBP": "obp", "SLG": "slg", "OPS": "ops",
        "ISO": "iso", "BABIP": "babip", "wRC": "wrc",
        "wRAA": "wraa", "wOBA": "woba", "wRC+": "wrcplus",
    })
    valid_cols = [
        "playerid", "split", "period", "season", "start_date", "end_date",
        "name", "tm", "pa", "bb_pct", "k_pct", "bb_per_k", "avg", "obp",
        "slg", "ops", "iso", "babip", "wrc", "wraa", "woba", "wrcplus",
        "updated_at",
    ]
    df = df[[c for c in valid_cols if c in df.columns]]
    df = pd.concat([df, league_avg_rows(df)], ignore_index=True)   # append playerid=0 LG rows
    df = df.where(pd.notnull(df), other=None)
    records = df.to_dict(orient="records")
    records = [
        {k: (None if isinstance(v, float) and math.isnan(v) else v) for k, v in row.items()}
        for row in records
    ]

    headers = {
        "apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}",
        "Content-Type": "application/json", "Prefer": "return=minimal",
    }
    print("   🗑️  Deleting all existing rows...")
    r = requests.delete(
        f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}?updated_at=gte.2000-01-01",
        headers=headers, timeout=60)
    if r.status_code not in (200, 204):
        print(f"   ❌ Delete failed: {r.status_code} {r.text[:200]}")
        return False
    print("   ✅ Table cleared")

    batch_size, total, pushed = 500, len(records), 0
    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]
        r = requests.post(f"{SUPABASE_URL}/rest/v1/{SUPABASE_TABLE}",
                          headers=headers, json=batch, timeout=45)
        if r.status_code in (200, 201):
            pushed += len(batch)
        else:
            print(f"   ❌ Batch {i // batch_size + 1} failed: {r.status_code} {r.text[:200]}")
    print(f"   📊 Total inserted: {pushed}/{total} rows")
    return pushed == total

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    start_time = time.time()
    today_str  = date.today().strftime("%Y-%m-%d")
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    windows = get_windows()
    total = len(windows)
    print(f"\n🚀 Player Multi-Year History — {today_str}")
    print(f"   {total} pulls ({len(HANDS)} hands × {len(PERIODS)} periods × started years)\n")
    send_telegram(f"⚾ <b>Player Multi-Year History Started</b>\n📅 {today_str}\n📊 {total} pulls...")

    saved = []
    try:
        pending, round_no, failed = list(windows), 0, []
        while pending:
            if round_no > 0:
                delay = RETRY_BACKOFFS[min(round_no - 1, len(RETRY_BACKOFFS) - 1)]
                if time.time() - start_time + delay > RETRY_BUDGET_SECONDS:
                    print(f"\n⏰ Retry budget exhausted with {len(pending)} left."); break
                print(f"\n── Retry round {round_no}: {len(pending)} pull(s), waiting {delay}s ──")
                time.sleep(delay)
            failed = []
            for n, w in enumerate(pending, 1):
                hand, period, yr, arr, sd, ed = w
                name = f"{yr}_{hand}_{period}"
                print(f"[{n:03d}/{len(pending)}] {name}  arr={arr} ({sd}->{ed})")
                errored = False
                try:
                    df = scrape_table(arr, sd, ed, name)
                except Exception as e:
                    print(f"       ⚠️  {e}"); df = None; errored = True
                if df is not None and not df.empty:
                    df.insert(0, "split",  hand)
                    df.insert(1, "period", period)
                    df.insert(2, "season", yr)
                    df.insert(3, "start_date", sd)
                    df.insert(4, "end_date",   ed)
                    path = os.path.join(OUTPUT_DIR, f"{name}.csv")
                    df.to_csv(path, index=False)
                    print(f"       ✅ {len(df)} players -> {path}")
                    saved.append(path)
                elif errored:
                    failed.append(w)                       # real failure — retry
                else:
                    print("       ⏭️  no data for this window (future/empty) — skipped")
                time.sleep(DELAY_BETWEEN_CALLS)
            pending = failed
            round_no += 1
        ok = total - len(failed)
    except Exception as e:
        print(f"\n💥 Crash:\n{traceback.format_exc()}")
        send_telegram(f"💥 <b>Player Multi-Year History CRASHED</b>\n❌ {str(e)[:200]}")
        raise

    if failed:
        elapsed = round((time.time() - start_time) / 60, 1)
        print(f"\n❌ Only {ok}/{total} pulls after {elapsed} min. Nothing pushed.")
        send_telegram(f"❌ <b>Player Multi-Year History GAVE UP</b>\n📊 {ok}/{total} after {elapsed} min")
        raise SystemExit(1)

    combined = pd.concat([pd.read_csv(p) for p in saved], ignore_index=True)
    combined["updated_at"] = today_str
    combined.to_csv(COMBINED_CSV, index=False)
    print(f"\n📊 Combined: {len(combined)} rows -> {COMBINED_CSV}")

    supabase_ok = push_to_supabase(COMBINED_CSV)
    elapsed = round((time.time() - start_time) / 60, 1)
    msg = "✅ Supabase updated" if supabase_ok else "❌ Supabase failed"
    print(f"\n{'─'*55}\nSaved {ok}/{total} | Rows {len(combined)} | {elapsed}min\n{msg}")
    send_telegram(f"✅ <b>Player Multi-Year History Done</b>\n📅 {today_str}\n"
                  f"📊 {ok}/{total} pulls · {len(combined)} rows\n⏱️ {elapsed} min\n{msg}")
    if not supabase_ok:
        raise SystemExit(1)

if __name__ == "__main__":
    main()

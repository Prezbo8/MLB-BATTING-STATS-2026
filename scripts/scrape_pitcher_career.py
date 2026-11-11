"""
FanGraphs Career Pitcher Stats Scraper (2017 to current season) — API version
Fetches all 9 stat categories from the FanGraphs leaders JSON API (no browser):
  - Scrapes ALL types first; aborts (pushing nothing) if any type fails
  - Saves each as a CSV, pushes each to GitHub
  - Sends a Telegram notification when complete

Secrets from env: GITHUB_TOKEN,
                  TELEGRAM_TOKEN, TELEGRAM_CHAT_ID
Steps whose env vars are missing are skipped (handy for local dry runs).

Setup (one-time):
    pip install pandas requests
"""

import os
import time
import base64
from datetime import datetime, timezone

import pandas as pd
import requests as req_lib

from season import SEASON

# ══════════════════════════════════════════════════════════════════════════════
#  CREDENTIALS (from environment)
# ══════════════════════════════════════════════════════════════════════════════

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

GITHUB_TOKEN    = os.environ.get("GITHUB_TOKEN", "")
GITHUB_USERNAME = "Prezbo8"
GITHUB_REPO     = "MLB-BATTING-STATS-2026"
GITHUB_BRANCH   = "main"

OUTPUT_DIR  = "pitcher_data"
MAX_RETRIES = 5
RETRY_WAITS = [10, 30, 60, 120, 300]

API_HEADERS = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) "
                   "Chrome/149.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Referer": "https://www.fangraphs.com/leaders/major-league",
}

# ══════════════════════════════════════════════════════════════════════════════
#  STAT TYPE DEFINITIONS  (9 types, hardcoded career URLs)
# ══════════════════════════════════════════════════════════════════════════════

def get_stat_types():
    return [

        # ── Dashboard (type=8) ────────────────────────────────────────────────
        {
            "name":       "Dashboard",
            "csv_file":   "Stats_Dashboard_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=8&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","W","L","SV","G","GS","IP",
                           "K/9","BB/9","HR/9","BABIP","LOB%","GB%","HR/FB",
                           "vFA (pi)","ERA","xERA","FIP","xFIP","WAR"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid",
                "W":"W","L":"L","SV":"SV","G":"G","GS":"GS","IP":"IP",
                "K/9":"K/9","BB/9":"BB/9","HR/9":"HR/9",
                "BABIP":"BABIP","LOB%":"LOB%","GB%":"GB%","HR/FB":"HR/FB",
                "pivFA":"vFA (pi)",
                "ERA":"ERA","xERA":"xERA","FIP":"FIP","xFIP":"xFIP","WAR":"WAR",
            },
            "numeric":    ["W","L","SV","G","GS","IP","K/9","BB/9","HR/9",
                           "BABIP","LOB%","GB%","HR/FB","vFA (pi)",
                           "ERA","xERA","FIP","xFIP","WAR"],
        },

        # ── Standard (type=0) ─────────────────────────────────────────────────
        {
            "name":       "Standard",
            "csv_file":   "Stats_Standard_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=0&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","W","L","ERA","G","GS","QS",
                           "CG","ShO","SV","HLD","BS","IP","TBF","H","R",
                           "ER","HR","BB","IBB","HBP","WP","BK","SO"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid",
                "W":"W","L":"L","ERA":"ERA","G":"G","GS":"GS","QS":"QS",
                "CG":"CG","ShO":"ShO","SV":"SV","HLD":"HLD","BS":"BS",
                "IP":"IP","TBF":"TBF","H":"H","R":"R","ER":"ER","HR":"HR",
                "BB":"BB","IBB":"IBB","HBP":"HBP","WP":"WP","BK":"BK","SO":"SO",
            },
            "numeric":    ["W","L","ERA","G","GS","QS","CG","ShO","SV",
                           "HLD","BS","IP","TBF","H","R","ER","HR","BB",
                           "IBB","HBP","WP","BK","SO"],
        },

        # ── Advanced (type=1) ─────────────────────────────────────────────────
        {
            "name":       "Advanced",
            "csv_file":   "Stats_Advanced_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=1&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","K/9","BB/9","K/BB","HR/9",
                           "K%","BB%","K-BB%","AVG","WHIP","BABIP","LOB%",
                           "ERA-","FIP-","xFIP-","ERA","FIP","E-F","xFIP","SIERA"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid",
                "K/9":"K/9","BB/9":"BB/9","K/BB":"K/BB","HR/9":"HR/9",
                "K%":"K%","BB%":"BB%","K-BB%":"K-BB%",
                "AVG":"AVG","WHIP":"WHIP","BABIP":"BABIP","LOB%":"LOB%",
                ("ERA-","ERA_minus","EraMinus"): "ERA-",
                ("FIP-","FIP_minus","FipMinus"): "FIP-",
                ("xFIP-","xFIP_minus","xFipMinus"): "xFIP-",
                "ERA":"ERA","FIP":"FIP","E-F":"E-F","xFIP":"xFIP","SIERA":"SIERA",
            },
            "numeric":    ["K/9","BB/9","K/BB","HR/9","K%","BB%","K-BB%",
                           "AVG","WHIP","BABIP","LOB%","ERA-","FIP-","xFIP-",
                           "ERA","FIP","E-F","xFIP","SIERA"],
        },

        # ── Batted Ball (type=2) ──────────────────────────────────────────────
        {
            "name":       "BattedBall",
            "csv_file":   "Stats_BattedBall_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=2&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","BABIP","GB/FB","LD%","GB%","FB%",
                           "IFFB%","HR/FB","RS","RS/9","Balls","Strikes","Pitches",
                           "Pull%","Cent%","Oppo%","Soft%","Med%","Hard%"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid",
                "BABIP":"BABIP",
                ("GB/FB","GBFB"): "GB/FB",
                "LD%":"LD%","GB%":"GB%","FB%":"FB%","IFFB%":"IFFB%","HR/FB":"HR/FB",
                "RS":"RS",
                ("RS/9","RS9"): "RS/9",
                "Balls":"Balls","Strikes":"Strikes","Pitches":"Pitches",
                "Pull%":"Pull%","Cent%":"Cent%","Oppo%":"Oppo%",
                "Soft%":"Soft%","Med%":"Med%","Hard%":"Hard%",
            },
            "numeric":    ["BABIP","GB/FB","LD%","GB%","FB%","IFFB%","HR/FB",
                           "RS","RS/9","Balls","Strikes","Pitches",
                           "Pull%","Cent%","Oppo%","Soft%","Med%","Hard%"],
        },

        # ── +Stats (type=23) ──────────────────────────────────────────────────
        {
            "name":       "+Stats",
            "csv_file":   "Stats_+Stats_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=23&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","IP",
                           "K/9+","BB/9+","K/BB+","HR/9+","K%+","BB%+",
                           "AVG+","WHIP+","BABIP+","LOB%+",
                           "ERA-","FIP-","xFIP-","LD%+","GB%+","FB%+"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid","IP":"IP",
                ("K/9+","K9+","K_9+","SO9+"):                  "K/9+",
                ("BB/9+","BB9+","BB_9+"):                      "BB/9+",
                ("K/BB+","KBB+","K_BB+"):                      "K/BB+",
                ("HR/9+","HR9+","HR_9+"):                      "HR/9+",
                ("K%+","K_pct+","Kpct+","K+","SO%+"):          "K%+",
                ("BB%+","BB_pct+","BBpct+","BB+"):             "BB%+",
                ("AVG+","AVGplus"):                            "AVG+",
                ("WHIP+","WHIPplus"):                          "WHIP+",
                ("BABIP+","BABIPplus"):                        "BABIP+",
                ("LOB%+","LOB+","LOB_pct+","LOBpct+"):         "LOB%+",
                ("ERA-","ERA_minus","EraMinus","ERA_"):         "ERA-",
                ("FIP-","FIP_minus","FipMinus","FIP_"):         "FIP-",
                ("xFIP-","xFIP_minus","xFipMinus","xFIP_"):    "xFIP-",
                ("LD%+","LD+","LD_pct+","LDpct+"):             "LD%+",
                ("GB%+","GB+","GB_pct+","GBpct+"):             "GB%+",
                ("FB%+","FB+","FB_pct+","FBpct+"):             "FB%+",
            },
            "numeric":    ["IP","K/9+","BB/9+","K/BB+","HR/9+","K%+","BB%+",
                           "AVG+","WHIP+","BABIP+","LOB%+",
                           "ERA-","FIP-","xFIP-","LD%+","GB%+","FB%+"],
        },

        # ── Statcast (type=24) ────────────────────────────────────────────────
        {
            "name":       "Statcast",
            "csv_file":   "Stats_Statcast_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=24&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","IP","Events","EV","EV90","maxEV",
                           "LA","Barrels","Barrel%","HardHit","HardHit%","ERA","xERA"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid","IP":"IP","Events":"Events",
                ("EV","AvgEV","avg_EV"):                               "EV",
                ("EV90","EV90th"):                                     "EV90",
                ("maxEV","MaxEV","max_EV"):                            "maxEV",
                ("LA","AvgLA","avg_LA","LaunchAngle"):                 "LA",
                ("Barrels","barrels"):                                 "Barrels",
                ("Barrel%","Barrel_pct","BarrelPct","Barrel%1"):       "Barrel%",
                ("HardHit","HardHits","hard_hit"):                     "HardHit",
                ("HardHit%","HardHit_pct","HardHitPct","HardHit%1"):   "HardHit%",
                "ERA":"ERA",
                ("xERA","xera"):                                       "xERA",
            },
            "numeric":    ["IP","Events","EV","EV90","maxEV","LA",
                           "Barrels","Barrel%","HardHit","HardHit%","ERA","xERA"],
        },

        # ── Plate Discipline (type=5) ─────────────────────────────────────────
        {
            "name":       "PlateDiscipline",
            "csv_file":   "PlateDiscipline_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=5&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid",
                           "O-Swing%","Z-Swing%","Swing%",
                           "O-Contact%","Z-Contact%","Contact%",
                           "Zone%","F-Strike%","SwStr%","CStr%","CSW%"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid",
                ("O-Swing%","OSwing%","O_Swing%","OSwingPct"):           "O-Swing%",
                ("Z-Swing%","ZSwing%","Z_Swing%","ZSwingPct"):           "Z-Swing%",
                ("Swing%","SwingPct","Swing_pct"):                       "Swing%",
                ("O-Contact%","OContact%","O_Contact%","OContactPct"):   "O-Contact%",
                ("Z-Contact%","ZContact%","Z_Contact%","ZContactPct"):   "Z-Contact%",
                ("Contact%","ContactPct","Contact_pct"):                 "Contact%",
                ("Zone%","ZonePct","Zone_pct"):                          "Zone%",
                ("F-Strike%","FStrike%","F_Strike%","FStrikePct"):       "F-Strike%",
                ("SwStr%","SwStrPct","SwStr_pct","SwStr","SwingStrike%"): "SwStr%",
                ("CStr%","CStrPct","CStr_pct","CStr","CalledStrike%"):   "CStr%",
                ("CSW%","CSWPct","CSW_pct","CSW","C+SwStr%","CSwStr%"):  "CSW%",
            },
            "numeric":    ["O-Swing%","Z-Swing%","Swing%",
                           "O-Contact%","Z-Contact%","Contact%",
                           "Zone%","F-Strike%","SwStr%","CStr%","CSW%"],
        },

        # ── Pitch Velocity (type=10) ──────────────────────────────────────────
        {
            "name":       "PitchVelocity",
            "csv_file":   "PitchVelocity_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=10&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","IP",
                           "vFA","vFC","vFS","vFO","vSI",
                           "vSL","vCU","vKC","vEP","vCH","vSC","vKN"],
            "column_map": {
                "PlayerName":"Name","TeamNameAbb":"Team","playerid":"playerid","IP":"IP",
                ("pfxvFA","vFA","vFA (pfx)","FA_velo","vFF"):            "vFA",
                ("pfxvFT","vFT","vFT (pfx)","FT_velo"):                 "vFT",
                ("pfxvFC","vFC","vFC (pfx)","FC_velo"):                 "vFC",
                ("pfxvFS","vFS","vFS (pfx)","FS_velo"):                 "vFS",
                ("pfxvFO","vFO","vFO (pfx)","FO_velo"):                 "vFO",
                ("pfxvSI","vSI","vSI (pfx)","SI_velo"):                 "vSI",
                ("pfxvSL","vSL","vSL (pfx)","SL_velo"):                 "vSL",
                ("pfxvCU","vCU","vCU (pfx)","CU_velo","vCB","pfxvCB"):  "vCU",
                ("pfxvKC","vKC","vKC (pfx)","KC_velo"):                 "vKC",
                ("pfxvEP","vEP","vEP (pfx)","EP_velo"):                 "vEP",
                ("pfxvCH","vCH","vCH (pfx)","CH_velo"):                 "vCH",
                ("pfxvSC","vSC","vSC (pfx)","SC_velo"):                 "vSC",
                ("pfxvKN","vKN","vKN (pfx)","KN_velo"):                 "vKN",
            },
            "numeric":    ["IP","vFA","vFC","vFS","vFO","vSI",
                           "vSL","vCU","vKC","vEP","vCH","vSC","vKN"],
        },

        # ── Stuff+ / Location+ / Pitching+ (type=36) ─────────────────────────
        {
            "name":       "StuffPlus",
            "csv_file":   "Pitch_StuffPlus_career.csv",
            "page_url":   f"https://www.fangraphs.com/leaders/major-league?pos=all&lg=all&type=36&month=0&ind=0&rost=0&age=0&filter=&players=0&team=0&stats=sta&qual=10&pageitems=2000000000&startdate=&enddate=&season2={SEASON}&season1=2017&season={SEASON}",
            "columns":    ["rank","Name","Team","playerid","IP",
                           "Stf+ FA","Stf+ SI","Stf+ FC","Stf+ FS","Stf+ SL",
                           "Stf+ CU","Stf+ CH","Stf+ KC","Stf+ FO",
                           "Stuff+","Location+","Pitching+"],
            "column_map": {
                "PlayerName": "Name",
                "TeamNameAbb": "Team",
                "playerid": "playerid",
                "IP": "IP",
                ("sp_s_FF","sp_s_FA","spFA","stf_FA","Stf+ FA"):    "Stf+ FA",
                ("sp_s_SI","spSI","stf_SI","Stf+ SI"):              "Stf+ SI",
                ("sp_s_FC","spFC","stf_FC","Stf+ FC"):              "Stf+ FC",
                ("sp_s_FS","spFS","stf_FS","Stf+ FS"):              "Stf+ FS",
                ("sp_s_SL","spSL","stf_SL","Stf+ SL"):              "Stf+ SL",
                ("sp_s_CU","spCU","stf_CU","Stf+ CU"):              "Stf+ CU",
                ("sp_s_CH","spCH","stf_CH","Stf+ CH"):              "Stf+ CH",
                ("sp_s_KC","spKC","stf_KC","Stf+ KC"):              "Stf+ KC",
                ("sp_s_FO","spFO","stf_FO","Stf+ FO"):              "Stf+ FO",
                ("sp_stuff","spStuff","stuff_plus","Stuff+"):        "Stuff+",
                ("sp_location","spLocation","location_plus","Location+"): "Location+",
                ("sp_pitching","spPitching","pitching_plus","Pitching+"): "Pitching+",
            },
            "numeric":    ["IP",
                           "Stf+ FA","Stf+ SI","Stf+ FC","Stf+ FS","Stf+ SL",
                           "Stf+ CU","Stf+ CH","Stf+ KC","Stf+ FO",
                           "Stuff+","Location+","Pitching+"],
        },
    ]


# ══════════════════════════════════════════════════════════════════════════════
#  HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def utcnow():
    return datetime.now(timezone.utc)


def get_value(row, key_spec):
    if isinstance(key_spec, tuple):
        for k in key_spec:
            if k in row:
                return row[k]
        return ""
    return row.get(key_spec, "")


def check_for_nans(df, rows, label):
    all_nan = [c for c in df.columns if c not in ("rank","Name","Team") and df[c].isna().all()]
    if all_nan:
        print(f"  ⚠️  All-NaN columns in {label}: {all_nan}")
        print(f"  Available API keys: {list(rows[0].keys())}")


# ══════════════════════════════════════════════════════════════════════════════
#  SCRAPE ONE STAT TYPE (CAREER)
# ══════════════════════════════════════════════════════════════════════════════

def scrape_one(stat_type_def):
    page_url   = stat_type_def["page_url"]
    api_url    = page_url.replace(
                     "https://www.fangraphs.com/leaders/major-league?",
                     "https://www.fangraphs.com/api/leaders/major-league/data?"
                 )
    columns    = stat_type_def["columns"]
    column_map = stat_type_def["column_map"]
    numeric    = stat_type_def["numeric"]
    label      = stat_type_def["name"]

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  [{label}] Fetching API (attempt {attempt}/{MAX_RETRIES})...")
            r = req_lib.get(api_url, headers=API_HEADERS, timeout=120)
            r.raise_for_status()

            payload = r.json()
            rows = payload.get("data", payload) if isinstance(payload, dict) else payload
            print(f"  [{label}] Got {len(rows)} rows.")

            if not rows:
                raise ValueError(f"No rows for {label}.")

            data_rows = []
            for i, row in enumerate(rows, start=1):
                record = {"rank": i}
                for key_spec, col_name in column_map.items():
                    record[col_name] = get_value(row, key_spec)
                data_rows.append(record)

            df = pd.DataFrame(data_rows, columns=columns)
            for col in numeric:
                df[col] = pd.to_numeric(df[col], errors="coerce")

            check_for_nans(df, rows, label)
            return df

        except Exception as e:
            print(f"  [{label}] ⚠️  Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                wait = RETRY_WAITS[min(attempt - 1, len(RETRY_WAITS) - 1)]
                print(f"  [{label}] Retrying in {wait}s...")
                time.sleep(wait)
            else:
                raise RuntimeError(f"All {MAX_RETRIES} attempts failed for {label}.") from e


# ══════════════════════════════════════════════════════════════════════════════
#  GITHUB
# ══════════════════════════════════════════════════════════════════════════════

def push_to_github(csv_path, repo_path):
    if not GITHUB_TOKEN:
        print(f"  ⏭️  GitHub skipped for {repo_path} (no GITHUB_TOKEN)")
        return False
    url = f"https://api.github.com/repos/{GITHUB_USERNAME}/{GITHUB_REPO}/contents/{repo_path}"
    headers = {
        "Authorization": f"token {GITHUB_TOKEN}",
        "Accept": "application/vnd.github.v3+json",
    }
    with open(csv_path, "rb") as f:
        content = base64.b64encode(f.read()).decode("utf-8")

    sha = None
    r = req_lib.get(url, headers=headers, timeout=30)
    if r.status_code == 200:
        sha = r.json().get("sha")

    payload = {
        "message": f"Career load {utcnow().strftime('%Y-%m-%d %H:%M UTC')}",
        "content": content,
        "branch":  GITHUB_BRANCH,
    }
    if sha:
        payload["sha"] = sha

    r = req_lib.put(url, headers=headers, json=payload, timeout=30)
    if r.status_code in (200, 201):
        print(f"  ✅ GitHub: {repo_path}")
        return True
    else:
        print(f"  ❌ GitHub failed for {repo_path}: {r.status_code} {r.text[:150]}")
        return False


# ══════════════════════════════════════════════════════════════════════════════
#  TELEGRAM
# ══════════════════════════════════════════════════════════════════════════════

def send_telegram(message):
    if not TELEGRAM_TOKEN:
        print("  ⏭️  Telegram skipped (no TELEGRAM_TOKEN)")
        return
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    try:
        r = req_lib.post(url, json={"chat_id": TELEGRAM_CHAT_ID, "text": message, "parse_mode": "HTML"}, timeout=10)
        if r.status_code == 200:
            print("  ✅ Telegram notification sent.")
        else:
            print(f"  ⚠️  Telegram failed: {r.status_code} {r.text[:100]}")
    except Exception as e:
        print(f"  ⚠️  Telegram error: {e}")


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

def main():
    run_start  = utcnow()
    stat_types = get_stat_types()
    total_jobs = len(stat_types)

    print(f"🚀 FanGraphs Career Pitcher Stats Scraper (2017-{SEASON}) — API")
    print(f"   {total_jobs} stat types")
    print("=" * 60)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    results = []

    # ── Phase 1: scrape everything (abort if anything fails) ──────────────────
    for st in stat_types:
        name     = st["name"]
        csv_file = st["csv_file"]

        print(f"\n  {'─' * 55}")
        print(f"  📊 {name}")

        result = {"name": name, "file": csv_file, "df": None,
                  "rows": 0, "scrape_ok": False,
                  "github_ok": False}

        try:
            df = scrape_one(st)
            csv_path = os.path.join(OUTPUT_DIR, csv_file)
            df.to_csv(csv_path, index=False)
            result["df"]        = df
            result["rows"]      = len(df)
            result["scrape_ok"] = True
            print(f"  ✅ CSV saved: {csv_path} ({len(df)} rows)")
        except Exception as e:
            print(f"  ❌ Scrape failed: {e}")

        results.append(result)
        time.sleep(2)

    failed = [r["name"] for r in results if not r["scrape_ok"]]
    if failed:
        print(f"\n❌ {len(failed)} stat type(s) failed — nothing pushed, old data left in place.")
        send_telegram(
            f"❌ <b>Career Pitcher Scraper GAVE UP</b>\n"
            f"🚫 Nothing pushed — old data left in place\n"
            f"❌ Failed: {', '.join(failed)}"
        )
        raise SystemExit(1)

    # ── Phase 2: push all to GitHub ────────────────────────────────────────────
    for r in results:
        repo_path = f"data/pitcher/career/{r['file']}"
        r["github_ok"]   = push_to_github(os.path.join(OUTPUT_DIR, r["file"]), repo_path)

    run_end     = utcnow()
    elapsed     = int((run_end - run_start).total_seconds())
    scraped_ok  = sum(1 for r in results if r["scrape_ok"])
    github_ok   = sum(1 for r in results if r["github_ok"])

    print(f"\n{'=' * 60}")
    print(f"✅ Done in {elapsed}s — {scraped_ok}/{total_jobs} scraped, "
          f"{github_ok}/{total_jobs} → GitHub")

    lines = [
        f"<b>⚾ FanGraphs Career Load Complete (2017-{SEASON})</b>",
        f"🕐 {run_end.strftime('%Y-%m-%d %H:%M UTC')}  ({elapsed}s)",
        f"📦 {scraped_ok}/{total_jobs} scraped | GH {github_ok}/{total_jobs}",
    ]

    send_telegram("\n".join(lines))

    # The CSVs feed the merge + scorer steps (which write pitcher_scores /
    # pitcher_split_scores); a failed GitHub CSV push fails the run.
    if GITHUB_TOKEN and github_ok < total_jobs:
        raise SystemExit(1)


if __name__ == "__main__":
    main()

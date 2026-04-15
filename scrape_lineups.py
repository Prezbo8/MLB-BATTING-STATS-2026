"""
scrape_lineups.py  —  Single scraper for all MLB lineups (projected + confirmed)
Source: https://www.rotowire.com/baseball/daily-lineups.php

RotoWire DOM structure (confirmed from live inspection):
  .lineup                           ← one per game
    .lineup__meta .lineup__time     ← "12:35 PM ET"
    .lineup__box
      .lineup__top .lineup__abbr    ← team abbrs (first=away, second=home)
      .lineup__main
        ul.lineup__list.is-visit    ← away side
        ul.lineup__list.is-home     ← home side
          li.lineup__player-highlight
            .lineup__player-highlight-name  ← pitcher name (includes hand inline)
            .lineup__throws                 ← pitcher hand "L" or "R"
          li.lineup__status[.is-confirmed]  ← status
          li.lineup__player  (×9)
            .lineup__pos    ← position
            a               ← batter name
            .lineup__bats   ← bat side L/R/S

Replaces: scrape_projected_lineups.py + scrape_confirmed_lineups.py
Schedule: every 30 min, 11 AM–9 PM ET (same window as before)
Telegram: fires once per team when status flips to Confirmed
"""

import os
import re
import sys
import json
import logging
import pytz
import requests
from datetime import datetime
from supabase import create_client, Client
from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

supabase: Client = create_client(
    os.environ["SUPABASE_URL"],
    os.environ["SUPABASE_KEY"]
)

TELEGRAM_TOKEN   = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

_tz = pytz.timezone('America/New_York')

ROTOWIRE_URL = "https://www.rotowire.com/baseball/daily-lineups.php"

# ── ABBREVIATION MAP ──────────────────────────────────────────────────────
# RotoWire uses standard MLB abbrs already, just a few edge cases
RW_ABBR_MAP = {
    "WAS": "WSN",
    "WSH": "WSN",
    "KC":  "KCR",
    "SD":  "SDP",
    "SF":  "SFG",
    "TB":  "TBR",
    "CWS": "CHW",
    "OAK": "ATH",
    "SAC": "ATH",
}

def normalize_abbr(abbr):
    if not abbr:
        return abbr
    abbr = abbr.strip().upper()
    return RW_ABBR_MAP.get(abbr, abbr)


# ── NAME DISPLAY MAP ──────────────────────────────────────────────────────
# RotoWire uses abbreviated first names (e.g. "C. Carroll") for batters
# and sometimes omits accents. Map known players to canonical names.
DISPLAY_NAME_MAP = {
    "Ronald Acuna Jr.":       "Ronald Acuña Jr.",
    "Ronald Acuna":           "Ronald Acuña Jr.",
    "Fernando Tatis Jr.":     "Fernando Tatis Jr.",
    "Fernando Tatis":         "Fernando Tatis Jr.",
    "Bobby Witt Jr.":         "Bobby Witt Jr.",
    "Bobby Witt":             "Bobby Witt Jr.",
    "Vladimir Guerrero Jr.":  "Vladimir Guerrero Jr.",
    "Vladimir Guerrero":      "Vladimir Guerrero Jr.",
    "Jazz Chisholm Jr.":      "Jazz Chisholm Jr.",
    "Jazz Chisholm":          "Jazz Chisholm Jr.",
    "Luis Robert Jr.":        "Luis Robert Jr.",
    "Luis Robert":            "Luis Robert Jr.",
    "Luis Garcia Jr.":        "Luis García Jr.",
    "Luis Garcia":            "Luis García Jr.",
    "Jose Abreu":             "José Abreu",
    "Jose Ramirez":           "José Ramírez",
    "Yordan Alvarez":         "Yordan Álvarez",
    "Julio Rodriguez":        "Julio Rodríguez",
    "Eloy Jimenez":           "Eloy Jiménez",
}

def normalize_name(name):
    if not name:
        return name
    cleaned = ' '.join(name.strip().split())
    return DISPLAY_NAME_MAP.get(cleaned, cleaned)


# ── TELEGRAM ──────────────────────────────────────────────────────────────
def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        logging.info("Telegram not configured — skipping")
        return
    try:
        r = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data={"chat_id": TELEGRAM_CHAT_ID, "text": msg},
            timeout=10
        )
        r.raise_for_status()
    except Exception as e:
        logging.warning(f"Telegram send failed: {e}")


# ── SCRAPER ───────────────────────────────────────────────────────────────
def scrape_rotowire():
    """Scrape RotoWire lineups page. Returns list of team records."""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/124.0.0.0 Safari/537.36"
        ))
        logging.info(f"Loading {ROTOWIRE_URL}")
        page.goto(ROTOWIRE_URL, wait_until="networkidle", timeout=60000)
        try:
            page.wait_for_selector(".lineup", timeout=20000)
            logging.info("Page ready — .lineup found")
        except Exception:
            logging.warning("Timed out waiting for .lineup — proceeding anyway")
        html = page.content()
        browser.close()

    # Save debug artifact
    try:
        with open("rotowire_debug.html", "w", encoding="utf-8") as f:
            f.write(html)
        logging.info(f"Debug HTML saved ({len(html):,} chars)")
    except Exception as e:
        logging.warning(f"Could not save debug HTML: {e}")

    soup = BeautifulSoup(html, "lxml")
    cards = soup.select(".lineup")
    logging.info(f"Found {len(cards)} lineup cards")

    results = []
    for card in cards:
        try:
            records = parse_lineup_card(card)
            results.extend(records)
        except Exception as e:
            logging.warning(f"Card parse error: {e}", exc_info=True)

    return results


def parse_lineup_card(card):
    """Parse one .lineup card into away + home records."""
    now_et = datetime.now(_tz)
    today  = now_et.strftime('%Y-%m-%d')
    ts     = now_et.strftime("%Y-%m-%d %H:%M:%S %Z")

    # Game time
    time_el   = card.select_one(".lineup__time")
    game_time = time_el.get_text(strip=True) if time_el else ""

    # Team abbrs — first is away, second is home
    team_els = card.select(".lineup__abbr")
    if len(team_els) < 2:
        logging.warning("Could not find 2 team abbrs — skipping")
        return []
    away_abbr = normalize_abbr(team_els[0].get_text(strip=True))
    home_abbr = normalize_abbr(team_els[1].get_text(strip=True))

    # The two lineup lists: is-visit = away, is-home = home
    away_list = card.select_one(".lineup__list.is-visit")
    home_list = card.select_one(".lineup__list.is-home")

    if not away_list or not home_list:
        logging.warning(f"{away_abbr} @ {home_abbr} — missing lineup lists, skipping")
        return []

    def parse_side(ul, side_label):
        """Parse one ul.lineup__list into a record dict."""
        # Pitcher
        ph = ul.select_one(".lineup__player-highlight")
        pitcher_name = None
        pitcher_hand = None
        if ph:
            name_el = ph.select_one(".lineup__player-highlight-name")
            raw_name = name_el.get_text(strip=True) if name_el else ""
            # Name may include hand inline e.g. "E. Rodriguez L" — strip it
            pitcher_name = normalize_name(re.sub(r'\s+[LRS]$', '', raw_name).strip())
            hand_el = ph.select_one(".lineup__throws")
            if hand_el:
                h = hand_el.get_text(strip=True).upper()
                if h in ("L", "R"):
                    pitcher_hand = h

        # Status — is-confirmed class means confirmed
        status_el = ul.select_one(".lineup__status")
        if status_el and "is-confirmed" in status_el.get("class", []):
            status = "Confirmed"
        else:
            status = "Projected"

        # Batting order
        order = []
        for i, player_el in enumerate(ul.select(".lineup__player")):
            pos_el  = player_el.select_one(".lineup__pos")
            name_el = player_el.select_one("a")
            bats_el = player_el.select_one(".lineup__bats")
            name    = normalize_name(name_el.get_text(strip=True)) if name_el else ""
            if not name:
                continue
            order.append({
                "order":    i + 1,
                "name":     name,
                "position": pos_el.get_text(strip=True) if pos_el else "",
                "bat_side": bats_el.get_text(strip=True) if bats_el else "",
            })

        logging.info(f"  {side_label} | {status} | pitcher={pitcher_name} ({pitcher_hand}) | batters={len(order)}")
        return {
            "lineup_status": status,
            "pitcher_name":  pitcher_name,
            "pitcher_hand":  pitcher_hand,
            "batting_order": json.dumps(order),
        }

    logging.info(f"{away_abbr} @ {home_abbr} | {game_time}")
    away_data = parse_side(away_list, away_abbr)
    home_data = parse_side(home_list, home_abbr)

    return [
        {
            "team":          away_abbr,
            "side":          "Away",
            "game_date":     today,
            "game_time":     game_time,
            "lineup_status": away_data["lineup_status"],
            "pitcher_name":  away_data["pitcher_name"],   # own pitcher
            "pitcher_hand":  away_data["pitcher_hand"],
            "batting_order": away_data["batting_order"],
            "scrape_date":   ts,
        },
        {
            "team":          home_abbr,
            "side":          "Home",
            "game_date":     today,
            "game_time":     game_time,
            "lineup_status": home_data["lineup_status"],
            "pitcher_name":  home_data["pitcher_name"],   # own pitcher
            "pitcher_hand":  home_data["pitcher_hand"],
            "batting_order": home_data["batting_order"],
            "scrape_date":   ts,
        },
    ]


# ── SUPABASE ──────────────────────────────────────────────────────────────
def get_already_confirmed(today):
    """Return set of team abbrs already Confirmed in Supabase today."""
    try:
        res = supabase.table("projected_lineups") \
            .select("team") \
            .eq("game_date", today) \
            .eq("lineup_status", "Confirmed") \
            .execute()
        return {row["team"] for row in (res.data or [])}
    except Exception as e:
        logging.warning(f"Could not fetch confirmed teams: {e}")
        return set()


def write_to_supabase(records, today):
    if not records:
        logging.info("No records to write")
        return

    # 1. Delete rows older than today
    supabase.table("projected_lineups") \
        .delete().lt("game_date", today).execute()
    logging.info("Cleared rows older than today")

    # 2. Find which teams are already confirmed in Supabase
    #    so we know who newly flipped this run (for Telegram)
    already_confirmed = get_already_confirmed(today)

    # 3. Separate records into confirmed and projected
    confirmed_records  = [r for r in records if r["lineup_status"] == "Confirmed"]
    projected_records  = [r for r in records if r["lineup_status"] == "Projected"]

    # 4. Upsert ALL confirmed records (always overwrite — official data)
    newly_confirmed = []
    if confirmed_records:
        # Deduplicate
        seen = {}
        for r in confirmed_records:
            seen[(r["team"], r["game_date"])] = r
        conf_deduped = list(seen.values())

        supabase.table("projected_lineups") \
            .upsert(conf_deduped, on_conflict="team,game_date") \
            .execute()
        logging.info(f"Upserted {len(conf_deduped)} confirmed records")

        # Track newly confirmed for Telegram
        for r in conf_deduped:
            if r["team"] not in already_confirmed:
                newly_confirmed.append(r)

    # 5. Upsert projected records only for teams NOT already confirmed
    confirmed_teams = {r["team"] for r in confirmed_records} | already_confirmed
    proj_to_insert  = [r for r in projected_records if r["team"] not in confirmed_teams]

    if proj_to_insert:
        # Delete existing projected rows for these teams first (clean refresh)
        supabase.table("projected_lineups") \
            .delete() \
            .eq("game_date", today) \
            .eq("lineup_status", "Projected") \
            .execute()
        logging.info(f"Cleared projected rows for today")

        # Deduplicate
        seen = {}
        for r in proj_to_insert:
            seen[(r["team"], r["game_date"])] = r
        proj_deduped = list(seen.values())

        supabase.table("projected_lineups") \
            .upsert(proj_deduped, on_conflict="team,game_date") \
            .execute()
        logging.info(f"Upserted {len(proj_deduped)} projected records")

    # 6. Send Telegram for newly confirmed teams
    for r in newly_confirmed:
        msg = (
            f"✅ Lineup CONFIRMED\n"
            f"{r['team']} — {r['side']}\n"
            f"🕐 {r['game_time']}"
        )
        send_telegram(msg)
        logging.info(f"Telegram sent for {r['team']}")

    if not newly_confirmed:
        logging.info("No new confirmations this run")


# ── MAIN ──────────────────────────────────────────────────────────────────
def main():
    now_et = datetime.now(_tz)
    today  = now_et.strftime('%Y-%m-%d')
    force  = "--force" in sys.argv

    if not force:
        window_start = now_et.replace(hour=11, minute=0, second=0, microsecond=0)
        window_end   = now_et.replace(hour=21, minute=0, second=0, microsecond=0)
        if not (window_start <= now_et <= window_end):
            logging.info(
                f"Outside window (11 AM–9 PM ET). "
                f"Now: {now_et.strftime('%I:%M %p %Z')} — exiting. "
                f"Pass --force to override."
            )
            return

    logging.info(
        f"Scraping RotoWire lineups for {today} "
        f"(ET: {now_et.strftime('%I:%M %p %Z')})"
        + (" [FORCED]" if force else "")
    )
    records = scrape_rotowire()
    logging.info(f"Parsed {len(records)} team records")

    confirmed = sum(1 for r in records if r["lineup_status"] == "Confirmed")
    projected = sum(1 for r in records if r["lineup_status"] == "Projected")
    logging.info(f"  Confirmed: {confirmed} | Projected: {projected}")

    write_to_supabase(records, today)
    logging.info("Done")


if __name__ == "__main__":
    main()

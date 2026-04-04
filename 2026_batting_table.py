from pybaseball import batting_stats
import pandas as pd
from supabase import create_client, Client
import os
import requests

print("🚀 Starting 2026 batting stats update (5 tables)...")

# Connect to Supabase
try:
    supabase: Client = create_client(
        os.environ["SUPABASE_URL"],
        os.environ["SUPABASE_SERVICE_ROLE_KEY"]
    )
    print("✅ Connected to Supabase successfully")
except Exception as e:
    print(f"❌ Connection error: {e}")
    raise

# Column mapping (same for every table)
cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

rename_map = {
    'IDfg': 'idfg', 'Season': 'season', 'Name': 'name', 'Team': 'tm',
    'PA': 'pa', 'BB%': 'bb_percent', 'K%': 'k_percent', 'BB/K': 'bb_k',
    'AVG': 'avg', 'OBP': 'obp', 'SLG': 'slg', 'OPS': 'ops',
    'ISO': 'iso', 'BABIP': 'babip',
    'wRC': 'wrc', 'wRAA': 'wraa', 'wOBA': 'woba', 'wRC+': 'wrc_plus'
}

def update_table(table_name, data):
    if len(data) == 0:
        print(f"   ⚠️ No data returned for {table_name}")
        return
    df = data[cols].copy()
    df = df.rename(columns=rename_map)
    print(f"   → {len(df)} rows prepared for {table_name}")
    
    # Clear old data + insert fresh data
    supabase.table(table_name).delete().neq('idfg', -1).execute()
    supabase.table(table_name).insert(df.to_dict(orient='records')).execute()
    print(f"   ✅ {table_name} updated successfully!")

# ==================== FETCH ALL 5 DATASETS ====================
print("Fetching data from FanGraphs...")

# 1. Overall
data_overall = batting_stats(2026, qual=10)
update_table('batting_stats_2026', data_overall)

# 2. vs LHP
data_lhp = batting_stats(2026, qual=10, month=13)
update_table('batting_stats_2026_vs_lhp', data_lhp)

# 3. vs RHP
data_rhp = batting_stats(2026, qual=10, month=14)
update_table('batting_stats_2026_vs_rhp', data_rhp)

# 4. Home
data_home = batting_stats(2026, qual=10, month=15)
update_table('batting_stats_2026_home', data_home)

# 5. Away
data_away = batting_stats(2026, qual=10, month=16)
update_table('batting_stats_2026_away', data_away)

print("🎉 All 5 tables updated successfully!")

# ============== SEND TELEGRAM NOTIFICATION ==============
print("Sending Telegram notification...")
try:
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    
    message = f"""✅ **2026 MLB Batting Stats Updated!**

• Overall: {len(data_overall)} players
• vs LHP: {len(data_lhp)} players
• vs RHP: {len(data_rhp)} players
• Home: {len(data_home)} players
• Away: {len(data_away)} players

All tables refreshed (min 10 PA)"""

    response = requests.post(
        f"https://api.telegram.org/bot{token}/sendMessage",
        json={
            "chat_id": chat_id,
            "text": message,
            "parse_mode": "Markdown"
        }
    )
    
    if response.status_code == 200:
        print("✅ Telegram message sent successfully!")
    else:
        print(f"⚠️ Telegram failed: {response.text}")
except Exception as e:
    print(f"⚠️ Could not send Telegram message: {e}")

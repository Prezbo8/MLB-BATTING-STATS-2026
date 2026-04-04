from pybaseball import batting_stats
import pandas as pd
from supabase import create_client, Client
import os
import requests

print("Starting 2026 batting stats update...")

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

# Pull 2026 stats (min 10 PA)
print("Fetching data from FanGraphs...")
data = batting_stats(2026, qual=10)

cols = ['IDfg', 'Season', 'Name', 'Team', 'PA', 'BB%', 'K%', 'BB/K',
        'AVG', 'OBP', 'SLG', 'OPS', 'ISO', 'BABIP',
        'wRC', 'wRAA', 'wOBA', 'wRC+']

df = data[cols].copy()

# Rename columns to match Supabase table
df = df.rename(columns={
    'IDfg': 'idfg',
    'Season': 'season',
    'Name': 'name',
    'Team': 'tm',
    'PA': 'pa',
    'BB%': 'bb_percent',
    'K%': 'k_percent',
    'BB/K': 'bb_k',
    'AVG': 'avg',
    'OBP': 'obp',
    'SLG': 'slg',
    'OPS': 'ops',
    'ISO': 'iso',
    'BABIP': 'babip',
    'wRC': 'wrc',
    'wRAA': 'wraa',
    'wOBA': 'woba',
    'wRC+': 'wrc_plus'
})

print(f"✅ Fetched and prepared {len(df)} players")

# Clear old data and insert fresh data
print("Clearing old data from Supabase...")
supabase.table('batting_stats_2026').delete().neq('idfg', -1).execute()

print("Inserting new data...")
result = supabase.table('batting_stats_2026').insert(df.to_dict(orient='records')).execute()

print(f"🎉 Successfully loaded {len(df)} rows into Supabase!")

# ============== SEND FREE TELEGRAM MESSAGE ==============
print("Sending Telegram notification...")
try:
    token = os.environ["TELEGRAM_TOKEN"]
    chat_id = os.environ["TELEGRAM_CHAT_ID"]
    
    message = f"✅ 2026 MLB Batting Stats Updated!\n\n{len(df)} players loaded into Supabase (min 10 PA)"

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

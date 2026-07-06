# MLB Batting Stats 2026

Live dashboard (GitHub Pages) + automated FanGraphs/TeamRankings/RotoWire data pipeline.
The site reads from Supabase; the CSVs in `data/` are the pipeline's archived outputs.

## Layout

```
index.html, mobile.html, ipad.html   the dashboard (GitHub Pages serves from root)
favicon*, manifest.json, *.png       site assets
scripts/                             all scrapers, mergers, scorers (run by Actions)
data/                                pipeline outputs and static inputs
  _ALL_SPLITS_COMBINED.csv           team batting splits (daily)
  _ALL_PLAYER_SPLITS_COMBINED.csv    player batting splits (daily)
  _RPG.csv                           runs per game (daily)
  pitcher/                           pitcher stat CSVs: 2026 (daily) + career/ + historical/ (static 2021-25)
  pitcher_splits_data/               static 2021-25 pitcher splits raws (merger inputs)
.github/workflows/                   schedules below
```

## Daily schedule (UTC / ET during daylight time)

| UTC | ET | Workflow |
|---|---|---|
| 05:00 | 1 AM | `mlb_pitcher_pipeline.yml` — pitcher overalls chain, then pitcher splits chain |
| 08:00 | 4 AM | `scrape_team_splits.yml` — team batting splits |
| 11:00 | 7 AM | `scrape_player_splits.yml` — player batting splits |
| 14:00 | 10 AM | `scrape_rpg.yml` — runs per game |
| 15:00–02:00, every 30 min | 11 AM–10 PM | `scrape_lineups.yml` — RotoWire lineups |

All scrapers hit JSON APIs / plain HTML with `requests` (no browser), read secrets
from repo Actions secrets, retry with escalating waits, and push nothing on failure
(old data stays in place). Each run sends a Telegram notification.

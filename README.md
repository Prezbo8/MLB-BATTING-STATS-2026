# MLB Batting Stats 2026

Live dashboard (GitHub Pages) + automated FanGraphs/TeamRankings/RotoWire data pipeline.
The site reads from Supabase; the CSVs in `data/` are the pipeline's archived outputs.

## Layout

```
index.html, mobile.html              the dashboard (GitHub Pages serves from root)
assets/                              favicons and app icons
assets/js/shared.js                  code both pages load: matchups/edges, drawer, trend lines,
                                     tier cards (tests/test_shared_js.py checks it against the Python;
                                     bump the ?v= in both pages' script tag — the test tells you the value)
manifest.json, site.webmanifest      PWA manifests
scripts/                             all scrapers, mergers, scorers (run by Actions)
data/                                pipeline outputs and static inputs
  _ALL_SPLITS_COMBINED.csv           team batting splits (daily)
  _ALL_PLAYER_SPLITS_COMBINED.csv    player batting splits (daily)
  _RPG.csv                           runs per game (daily)
  pitcher/                           pitcher stat CSVs: one set per season (current season rewritten daily) + career/ + historical/ (2021-25)
  pitcher_splits_data/               pitcher splits raws, one file per season (merger inputs)
  results/                           MLB final scores, results_<season>.csv
.github/workflows/                   schedules below
```

## Daily schedule (UTC / ET during daylight time)

| UTC | ET | Workflow |
|---|---|---|
| 05:00 (retry 09:00) | 1 AM (5 AM) | `mlb_pitcher_pipeline.yml` — pitcher overalls chain, then pitcher splits chain; one commit per run |
| 08:00 (retry 12:00) | 4 AM (8 AM) | `scrape_team_splits.yml` — team batting splits |
| 10:00 | 6 AM | `scrape_results.yml` — MLB final scores → `data/results/results_<season>.csv` |
| 11:00 (retry 15:00) | 7 AM (11 AM) | `scrape_player_splits.yml` — player batting splits |
| 14:00 | 10 AM | `scrape_rpg.yml` — runs per game |
| 15:17–01:17, hourly | 11 AM–9 PM | `scrape_lineups.yml` — RotoWire lineups |

The retry runs only do work when that day's first run failed (FanGraphs sometimes
blocks GitHub's servers for a few hours).

All scrapers hit JSON APIs / plain HTML with `requests` (no browser), read secrets
from repo Actions secrets, retry with escalating waits, and push nothing on failure
(old data stays in place). Each run sends a Telegram notification.

## Seasons, offseason pause and unpause

**Season rule** (`scripts/season.py`): the current season is this year from the
day *after* MLB's Opening Day (read from statsapi.mlb.com, fallback Mar 20),
otherwise last year. Scrapers, merges and scorers use it; the site and the
Telegram bot show the newest season found in the data. No year needs editing.
Test any date with `MLB_TODAY=YYYY-MM-DD python scripts/...`.

**Pause** (after the last regular-season game has been scraped): disable the
scheduled workflows — `mlb_pitcher_pipeline`, `scrape_team_splits`,
`scrape_player_splits`, `scrape_rpg`, `scrape_lineups`, `report_pdf`
(`gh workflow disable <file>`). `scrape_results` can stay on; it commits nothing
until scores change.

**Unpause** on or after the day after Opening Day (2027: **Mar 26**) — earlier,
the last-30/14/7-day windows have no games and the splits runs go red:
`gh workflow enable <file>` for the six above, and re-enable `scrape_results`
too if GitHub disabled it for inactivity (it does that after 60 days without
repo activity).

**One-time database setup** (already applied; re-run only on a new project):
`sql/replace_rows.sql` (one-transaction table swaps used by every writer),
`sql/public_read_only.sql` (public key read-only), `sql/rpg_rename_season_columns.sql`.
Writers use the service-role key (`SUPABASE_KEY` secret); the site and bot only
read with the public key.

## Tests

`tests/` is an offline test suite (`python -m pytest -q tests`, ~6 s): the
season rule, history month windows, the one-season guard, scoring on a small
fixed sample (with saved expected scores), a dry run of every scraper's
`main()` and of both pitcher chains, Supabase swap sizes (≤ 3,000 rows each),
page JavaScript syntax and workflow files. Every HTTP call is answered from
`tests/fixtures/` (trimmed real responses) — no secrets, no network, no live
data. `tests.yml` runs it on every push and PR; merging into `main` requires it
to pass.

- Changed scoring on purpose? `UPDATE_SNAPSHOTS=1 python -m pytest tests/test_scoring.py`
  and review the `tests/fixtures/scoring/expected_*.csv` diff.
- A site changed its response format? Re-record with `python tests/record_fixtures.py`
  (from a machine FanGraphs doesn't block).

## Backtest

`BACKTEST.md` answers "does the pitcher score predict anything?": each season
2022–2026 is scored as of Opening Day from earlier data only (no look-ahead),
then compared with runs allowed that season and in every start, against SIERA,
xFIP, K-BB%, Pitching+ and ERA, with 95% ranges. Rerun with
`python scripts/backtest.py` (downloads cached in `backtest_cache/`, not committed;
read-only — changes no scores).

`EDGES.md` answers "do Today's edges predict runs?": every regular-season game of the
last season is rebuilt from what was known before it (MLB box-score lineups, batters'
earlier months + previous season, Opening-Day starter scores) and compared with runs
scored. It found the starter matchup predicts and the lineup part doesn't, so the site
labels games from the starter matchup only. Rerun with `python scripts/backtest_edges.py`
(needs `backtest.py`'s cache; read-only).


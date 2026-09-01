-- projected_lineups stores one row per team. Without the opponent, the site had
-- to guess which two teams formed a game by zipping same-start-time rows in
-- team-abbr order, which silently produced wrong matchups on any slate with
-- simultaneous games. scrape_lineups.py already parses the opponent off the
-- RotoWire card, so store it and let the site read the matchup instead.
alter table projected_lineups add column if not exists opponent text;

-- Backfill is intentionally omitted: rows older than today are deleted on every
-- scrape run, so the column fills in on the next run.

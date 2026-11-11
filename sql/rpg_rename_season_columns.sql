-- Runs per game: name the season columns by position instead of by year, so
-- the table keeps the same shape every season (scrape_rpg.py fills
-- szn_current with the current season and szn_prev with the one before).
-- Run once in the Supabase SQL editor, right after PR 3b is merged.
alter table public.mlb_rpg rename column szn_2026 to szn_current;
alter table public.mlb_rpg rename column szn_2025 to szn_prev;

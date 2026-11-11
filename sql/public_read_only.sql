-- Make every MLB table READ-ONLY for the public (anon) key.
--
-- Why: the public key is in index.html / mobile.html, so anyone can use it.
-- Checked Nov 11 2026 (read-only catalog query):
--   * 6 tables have a policy "Allow service role delete" that is granted TO
--     public, so the public key can delete every row of them;
--   * projected_lineups also has "Service role write" (ALL, to public), so the
--     public key can insert/update/delete lineups;
--   * the 3 history tables have row-level security OFF, so the public key can
--     do anything to them.
-- None of this is needed: the scrapers use the service-role key, which skips
-- row-level security entirely, and the site + Telegram bot only read.
--
-- Run once in the Supabase SQL editor. Everything is in one transaction.

begin;

-- 1. Remove the write/delete policies that were granted to everyone
drop policy if exists "Allow service role delete" on public.fangraphs_splits;
drop policy if exists "Allow service role delete" on public.fangraphs_player_splits;
drop policy if exists "Allow service role delete" on public.mlb_rpg;
drop policy if exists "Allow service role delete" on public.pitcher_scores;
drop policy if exists "Allow service role delete" on public.pitcher_split_scores;
drop policy if exists "Allow service role delete" on public.projected_lineups;
drop policy if exists "Service role write"        on public.projected_lineups;

-- 2. Turn row-level security on for the history tables, with public read only
alter table public.fangraphs_splits_history        enable row level security;
alter table public.fangraphs_player_splits_history enable row level security;
alter table public.fangraphs_player_history        enable row level security;

create policy "Public read" on public.fangraphs_splits_history        for select to public using (true);
create policy "Public read" on public.fangraphs_player_splits_history for select to public using (true);
create policy "Public read" on public.fangraphs_player_history        for select to public using (true);

commit;

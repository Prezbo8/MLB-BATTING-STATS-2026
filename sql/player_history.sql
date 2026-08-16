-- Player history groundwork. Run once in the Supabase SQL editor.

-- 1) Stable FanGraphs playerid on the existing batter tables (backfilled on the
--    next scrape run). Pitcher tables already have this.
alter table fangraphs_player_splits          add column if not exists playerid bigint;
alter table fangraphs_player_splits_history   add column if not exists playerid bigint;

-- 2) Multi-year (2021-2026) per-YEAR player history, keyed by playerid.
--    split  = no_split | vs_lhp | vs_rhp
--    period = season | mar_apr | may | jun | jul | aug | sep_oct  (FanGraphs month buckets)
--    season = the year (2021..2026)
create table if not exists fangraphs_player_history (
  id          bigint generated always as identity primary key,
  playerid    bigint,
  split       text,
  period      text,
  season      integer,
  start_date  text,
  end_date    text,
  name        text,
  tm          text,
  pa          double precision,
  bb_pct      text,
  k_pct       text,
  bb_per_k    double precision,
  avg         double precision,
  obp         double precision,
  slg         double precision,
  ops         double precision,
  iso         double precision,
  babip       double precision,
  wrc         double precision,
  wraa        double precision,
  woba        double precision,
  wrcplus     double precision,
  updated_at  date
);
create index if not exists idx_phist_playerid on fangraphs_player_history (playerid);
create index if not exists idx_phist_lookup   on fangraphs_player_history (split, period, season);

-- Historical monthly batting-stats archive tables.
-- Separate from the live fangraphs_splits / fangraphs_player_splits tables the
-- site reads — the historical dump scripts write here only.
-- date_range holds a month key ("2026-03"); start_date/end_date = the window.
-- Run once in the Supabase SQL editor before triggering the "Historical Monthly
-- Splits Dump" GitHub Actions workflow.

create table if not exists fangraphs_splits_history (
  id          bigint generated always as identity primary key,
  split       text,
  date_range  text,
  start_date  text,
  end_date    text,
  season      double precision,
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
create index if not exists idx_splits_history_month on fangraphs_splits_history (date_range);

create table if not exists fangraphs_player_splits_history (
  id          bigint generated always as identity primary key,
  split       text,
  date_range  text,
  start_date  text,
  end_date    text,
  season      double precision,
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
create index if not exists idx_player_splits_history_month on fangraphs_player_splits_history (date_range);

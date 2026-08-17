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

-- 3) League-average rows (playerid = 0) for the Lineup History / Player card
--    y-axis LG line. PA-weighted from the table itself. RE-RUN THIS after any
--    scrape_player_history_multi backfill (delete-all wipes playerid=0 rows).
delete from fangraphs_player_history where playerid = 0;
insert into fangraphs_player_history
  (playerid, split, period, season, name, tm, pa, bb_pct, k_pct, avg, obp, slg, ops, iso, babip, woba, wrcplus, updated_at)
select 0, split, period, season, 'LG AVG', 'LG',
  sum(pa),
  round((sum(nullif(replace(bb_pct,'%',''),'')::numeric*pa) filter (where bb_pct is not null)/nullif(sum(pa) filter (where bb_pct is not null),0))::numeric,1)::text||'%',
  round((sum(nullif(replace(k_pct,'%',''),'')::numeric*pa) filter (where k_pct is not null)/nullif(sum(pa) filter (where k_pct is not null),0))::numeric,1)::text||'%',
  sum(avg*pa)   filter (where avg   is not null)/nullif(sum(pa) filter (where avg   is not null),0),
  sum(obp*pa)   filter (where obp   is not null)/nullif(sum(pa) filter (where obp   is not null),0),
  sum(slg*pa)   filter (where slg   is not null)/nullif(sum(pa) filter (where slg   is not null),0),
  sum(ops*pa)   filter (where ops   is not null)/nullif(sum(pa) filter (where ops   is not null),0),
  sum(iso*pa)   filter (where iso   is not null)/nullif(sum(pa) filter (where iso   is not null),0),
  sum(babip*pa) filter (where babip is not null)/nullif(sum(pa) filter (where babip is not null),0),
  sum(woba*pa)  filter (where woba  is not null)/nullif(sum(pa) filter (where woba  is not null),0),
  round((sum(wrcplus*pa) filter (where wrcplus is not null)/nullif(sum(pa) filter (where wrcplus is not null),0))::numeric)::double precision,
  current_date
from fangraphs_player_history
where playerid is distinct from 0
group by split, period, season;

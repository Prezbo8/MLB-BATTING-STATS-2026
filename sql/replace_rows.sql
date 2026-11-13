-- replace_rows: swap a table's rows (or one season/period of them) in ONE
-- transaction. Either the delete and the insert both happen, or neither does,
-- so the site never sees an empty or half-filled table.
--
-- Called from Python via POST /rest/v1/rpc/replace_rows (scripts/sb_replace.py).
--   p_table     one of the allowed tables below
--   p_where_col column to filter the delete on (null = replace the whole table)
--   p_where_val value of that column, compared as text ('2026', 'career')
--   p_rows      JSON array of rows; columns = keys of the first row that exist
--               in the table (id and other defaults are left to the table)
-- Returns the number of rows inserted.
--
-- SECURITY INVOKER: runs with the caller's own rights, so it cannot do
-- anything the caller's key could not already do with plain delete/insert.
--   p_where_col2 / p_where_val2  optional second filter (AND), e.g. period +
--               split, to swap a big period in smaller pieces (a 9.5k-row
--               single swap hit the statement timeout)
-- Run once in the Supabase SQL editor (drops the older 4-argument version).

begin;
drop function if exists public.replace_rows(text, text, text, jsonb);

create or replace function public.replace_rows(
    p_table      text,
    p_where_col  text,
    p_where_val  text,
    p_rows       jsonb,
    p_where_col2 text default null,
    p_where_val2 text default null
) returns integer
language plpgsql
security invoker
set search_path = public
as $$
declare
    allowed text[] := array[
        'fangraphs_splits', 'fangraphs_player_splits', 'mlb_rpg',
        'pitcher_scores', 'pitcher_split_scores',
        'fangraphs_splits_history', 'fangraphs_player_splits_history',
        'fangraphs_player_history'
    ];
    cols text;
    n    integer;
begin
    if not (p_table = any(allowed)) then
        raise exception 'replace_rows: table % is not allowed', p_table;
    end if;
    if jsonb_typeof(p_rows) <> 'array' or jsonb_array_length(p_rows) = 0 then
        raise exception 'replace_rows: no rows for %', p_table;
    end if;

    select string_agg(quote_ident(c.column_name), ', ' order by c.ordinal_position)
      into cols
      from information_schema.columns c
     where c.table_schema = 'public'
       and c.table_name   = p_table
       and (p_rows -> 0) ? c.column_name;

    if p_where_col is null then
        execute format('delete from public.%I where true', p_table);
    elsif p_where_col2 is null then
        execute format('delete from public.%I where %I::text = $1',
                       p_table, p_where_col) using p_where_val;
    else
        execute format('delete from public.%I where %I::text = $1 and %I::text = $2',
                       p_table, p_where_col, p_where_col2) using p_where_val, p_where_val2;
    end if;

    execute format(
        'insert into public.%I (%s) select %s from jsonb_populate_recordset(null::public.%I, $1)',
        p_table, cols, cols, p_table) using p_rows;
    get diagnostics n = row_count;
    return n;
end;
$$;
commit;

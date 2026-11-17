"""
Atomic Supabase table refresh, shared by the scrapers and scorers.

replace_rows() calls the Postgres function public.replace_rows
(sql/replace_rows.sql), which deletes the old rows and inserts the new ones
in ONE transaction: if anything fails, nothing changes and yesterday's data
stays on the site. Retrying is safe — replacing twice with the same rows
gives the same table.
"""

import time
import requests


def replace_rows(url, key, table, rows, where_col=None, where_val=None,
                 where_col2=None, where_val2=None, label="", timeout=120):
    """Replace `table` (or the rows where where_col = where_val [and
    where_col2 = where_val2]) with `rows`.
    Up to 3 attempts (5s/15s backoff). Returns True when the swap committed."""
    label = label or table
    if not rows:
        print(f"   ❌ {label}: no rows to write — old rows left in place")
        return False
    headers = {
        "apikey":        key,
        "Authorization": f"Bearer {key}",
        "Content-Type":  "application/json",
    }
    body = {
        "p_table":     table,
        "p_where_col": where_col,
        "p_where_val": None if where_val is None else str(where_val),
        "p_rows":      rows,
    }
    if where_col2 is not None:
        body["p_where_col2"] = where_col2
        body["p_where_val2"] = str(where_val2)
    for attempt in range(3):
        if attempt:
            wait = (5, 15)[attempt - 1]
            print(f"   🔄 {label}: retry {attempt + 1}/3 in {wait}s...")
            time.sleep(wait)
        try:
            r = requests.post(f"{url}/rest/v1/rpc/replace_rows",
                              headers=headers, json=body, timeout=timeout)
        except requests.RequestException as e:
            print(f"   ⚠️  {label}: {e}")
            continue
        if r.status_code == 200:
            print(f"   ✅ {label}: {r.text} rows replaced")
            return True
        print(f"   ⚠️  {label} [{r.status_code}] {r.text[:300]}")
        if r.status_code < 500 and r.status_code != 408:
            return False          # bad request / bad data: retrying won't help
    print(f"   ❌ {label}: failed after 3 attempts — old rows left in place")
    return False


def drop_old_seasons(url, key, table, col, first_season, timeout=60):
    """Delete `table` rows whose `col` is a season before `first_season`, so the
    season that leaves the 6-season window (e.g. 2021 once 2027 starts) is gone
    from the site too. 'career' is kept ('career' > '2022' as text).
    Returns True when the delete succeeded (deleting nothing counts)."""
    headers = {"apikey": key, "Authorization": f"Bearer {key}"}
    try:
        r = requests.delete(f"{url}/rest/v1/{table}",
                            params={col: f"lt.{first_season}"},
                            headers=headers, timeout=timeout)
    except requests.RequestException as e:
        print(f"   ⚠️  {table}: dropping seasons before {first_season} failed ({e})")
        return False
    if r.status_code in (200, 204):
        print(f"   🧹 {table}: seasons before {first_season} removed")
        return True
    print(f"   ⚠️  {table}: dropping seasons before {first_season} [{r.status_code}] {r.text[:300]}")
    return False

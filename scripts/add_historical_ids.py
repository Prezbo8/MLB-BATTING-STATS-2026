#!/usr/bin/env python3
"""
add_historical_ids.py — give the 2021-2025 pitcher files their FanGraphs player ids (Round H3)

The 45 files in data/pitcher/historical/ were saved before the scrapers kept a `playerid`
column. Without it, score_pitcher_overall.py's player_key falls back to the name, and when
two pitchers share a name (two Logan Allens) the older seasons of either can't be assigned
and become a separate "ghost" pitcher with its own score.

This adds a `playerid` column (after Team, like the 2026 files), matched on name + team +
season against FanGraphs' own list of that season's pitchers (backtest.fangraphs_ids: cached
in backtest_cache/, downloaded once if missing). Safety:
  * a (name, team, season) with zero or several FanGraphs ids is left blank, never guessed;
  * every match must agree with the ids the 2026 / career files already use for that name
    (when the name has exactly one) — any disagreement stops the run and writes nothing.
Rerunnable: files that already have the column are re-checked and only blanks are filled.

  python3 scripts/add_historical_ids.py           # check, then write
  python3 scripts/add_historical_ids.py --check   # report only
"""
import argparse
import csv
import glob
import os
import re
import sys

import pandas as pd

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
sys.path.insert(0, HERE)

HIST = os.path.join(REPO, "data", "pitcher", "historical")
CURRENT = [os.path.join(REPO, "data", "pitcher"), os.path.join(REPO, "data", "pitcher", "career")]


def norm_id(v):
    if v is None or (isinstance(v, float) and pd.isna(v)) or str(v).strip() in ("", "nan"):
        return None
    return re.sub(r"\.0$", "", str(v).strip())


def known_ids():
    """name -> set of ids used by the files that already carry ids (2026 + career)."""
    out = {}
    for d in CURRENT:
        for f in glob.glob(os.path.join(d, "*.csv")):
            df = pd.read_csv(f, usecols=lambda c: c in ("Name", "playerid"))
            if "playerid" not in df.columns:
                continue
            for name, pid in zip(df["Name"], df["playerid"].map(norm_id)):
                if pid:
                    out.setdefault(name, set()).add(pid)
    return out


SUFFIX = re.compile(r"\s+(jr\.?|sr\.?|ii|iii|iv)$", re.I)


def bare(name):
    """'Lou Trivino III' -> 'Lou Trivino' (FanGraphs' id list sometimes carries a suffix the stat files don't)."""
    return SUFFIX.sub("", str(name).strip())


def season_lookup(year):
    """(name, team) -> FanGraphs id for that season, only where exactly one id exists; exact
    names first, then names without a Jr./Sr./II/III/IV suffix."""
    import backtest as bt
    ids = bt.fangraphs_ids(year)
    ids = ids.assign(pid=ids["playerid"].map(norm_id), bare=ids["name"].map(bare))
    one = lambda keys: {k: v[0] for k, v in ids.groupby(keys)["pid"].agg(lambda s: sorted(set(x for x in s if x))).items()
                        if len(v) == 1}
    return one(["name", "team"]), one(["bare", "team"])


def ids_for(df, lookup):
    exact, suffixless = lookup
    return [exact.get((n, t)) or suffixless.get((bare(n), t)) for n, t in zip(df["Name"], df["Team"])]


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="report only, write nothing")
    a = ap.parse_args()

    known = known_ids()
    files = sorted(glob.glob(os.path.join(HIST, "*.csv")))
    lookups, plans, conflicts, blanks = {}, [], [], []
    for f in files:
        year = int(re.search(r"(\d{4})", os.path.basename(f)).group(1))
        lookup = lookups.setdefault(year, season_lookup(year))
        df = pd.read_csv(f)
        new = ids_for(df, lookup)
        if "playerid" in df.columns:          # rerun: keep existing ids, fill blanks only
            old = df["playerid"].map(norm_id).tolist()
            new = [o or n for o, n in zip(old, new)]
        for name, team, pid in zip(df["Name"], df["Team"], new):
            if pid is None:
                blanks.append((os.path.basename(f), name, team))
                continue
            k = known.get(name)
            if k and len(k) == 1 and pid not in k:
                conflicts.append((os.path.basename(f), name, team, pid, sorted(k)))
        plans.append((f, df, new))

    n_rows = sum(len(p[1]) for p in plans)
    print(f"{len(files)} files, {n_rows} rows: {n_rows - len(blanks)} matched, {len(blanks)} left blank, "
          f"{len(conflicts)} conflicts")
    for b in sorted(set(blanks)):
        print(f"   blank (no single FanGraphs id for this name/team/season): {b}")
    for c in conflicts:
        print(f"   CONFLICT: {c}")
    if conflicts:
        sys.exit("stopped: a match disagrees with the ids the 2026/career files use — nothing written")
    if a.check:
        return
    for f, df, new in plans:
        write_ids(f, new)
    print(f"✅ wrote playerid into {len(plans)} files")


def write_ids(path, ids):
    """Insert/refresh the playerid column right after Team, leaving every other cell's text
    exactly as it was (csv module, not a pandas round trip that could reformat numbers)."""
    with open(path, newline="") as fh:
        rows = list(csv.reader(fh))
    head = rows[0]
    if "playerid" in head:
        col = head.index("playerid")
        for r, pid in zip(rows[1:], ids):
            r[col] = pid or ""
    else:
        col = head.index("Team") + 1
        head.insert(col, "playerid")
        for r, pid in zip(rows[1:], ids):
            r.insert(col, pid or "")
    with open(path, "w", newline="") as fh:
        csv.writer(fh, lineterminator="\n").writerows(rows)


if __name__ == "__main__":
    main()

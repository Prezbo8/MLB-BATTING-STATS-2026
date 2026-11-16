"""Round H1: the keeper's timetable, the backup "skip if done" check and the workflow wiring."""

import glob
import os
import subprocess
import sys
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, ".github", "scripts"))
sys.path.insert(0, os.path.join(REPO, "scripts"))

import backup_check as bc        # noqa: E402
import next_slot as ns           # noqa: E402
import schedule_report as sr     # noqa: E402

ET = ZoneInfo("America/New_York")
et = lambda *a: datetime(*a, tzinfo=ET)
DAILY_WORKFLOWS = ["mlb_pitcher_pipeline.yml", "scrape_team_splits.yml", "scrape_player_splits.yml",
                   "scrape_results.yml", "scrape_rpg.yml", "report_pdf.yml", "scrape_lineups.yml"]


# ── timetable ────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("now, wf, secs", [
    (et(2027, 4, 10, 5, 0), "scrape_results.yml", 30 * 60),
    (et(2027, 4, 10, 6, 10), "scrape_team_splits.yml", 20 * 60),
    (et(2027, 4, 10, 10, 59), "report_pdf.yml", 60),
    (et(2027, 4, 10, 11, 5), "scrape_lineups.yml", 12 * 60),
    (et(2027, 4, 10, 21, 20), "scrape_results.yml", (8 * 60 + 10) * 60),   # after the last lineups run: tomorrow 5:30
])
def test_next_slot(now, wf, secs):
    got = ns.next_slot(now)
    assert got[1] == wf and got[0] == secs


def test_a_slot_under_30_seconds_away_counts_as_passed():
    secs, wf, _ = ns.next_slot(et(2027, 4, 10, 6, 59, 45))
    assert wf == "scrape_rpg.yml" and secs == 30 * 60 + 15        # 7:00 is 15 s away -> next is 7:30


def test_spring_forward():
    # Sat Mar 13 2027 10 PM EST -> Sun 5:30 AM EDT is 6.5 real hours (wall clock says 7.5)
    secs, wf, label = ns.next_slot(et(2027, 3, 13, 22, 0))
    assert wf == "scrape_results.yml" and secs == int(6.5 * 3600) and "EDT" in label


def test_fall_back():
    # Sat Nov 6 2027 10 PM EDT -> Sun 5:30 AM EST is 8.5 real hours (wall clock says 7.5)
    secs, wf, label = ns.next_slot(et(2027, 11, 6, 22, 0))
    assert wf == "scrape_results.yml" and secs == int(8.5 * 3600) and "EST" in label


def test_offseason_is_judged_by_the_six_seasonal_workflows():
    """README: these six are disabled in the offseason; scrape_results stays on all year."""
    out = subprocess.run([sys.executable, os.path.join(REPO, ".github", "scripts", "next_slot.py"), "--seasonal"],
                         capture_output=True, text=True, check=True).stdout.split()
    assert sorted(out) == sorted(w for w in DAILY_WORKFLOWS if w != "scrape_results.yml")
    readme = open(os.path.join(REPO, "README.md")).read()
    pause = readme[readme.index("**Pause**"):readme.index("**Unpause**")]
    assert all(w.replace(".yml", "") in pause for w in out)


def test_keeper_starts_every_daily_workflow():
    out = subprocess.run([sys.executable, os.path.join(REPO, ".github", "scripts", "next_slot.py"), "--workflows"],
                         capture_output=True, text=True, check=True).stdout.split()
    assert sorted(out) == sorted(DAILY_WORKFLOWS)


# ── backup check ─────────────────────────────────────────────────────────────

NOW = datetime(2027, 4, 10, 16, 0, tzinfo=timezone.utc)   # noon EDT
iso = lambda dt: dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def test_keeper_and_manual_runs_always_run():
    assert bc.decide("workflow_dispatch", [{"databaseId": 1, "conclusion": "success", "createdAt": iso(NOW)}], NOW, "today")[0]


@pytest.mark.parametrize("runs, since, want", [
    ([{"databaseId": 1, "conclusion": "success", "createdAt": iso(et(2027, 4, 10, 6, 1))}], "today", False),
    ([{"databaseId": 1, "conclusion": "success", "createdAt": iso(et(2027, 4, 9, 23, 30))}], "today", True),   # yesterday ET
    ([{"databaseId": 1, "conclusion": "failure", "createdAt": iso(et(2027, 4, 10, 6, 1))}], "today", True),
    ([{"databaseId": 1, "conclusion": "success", "createdAt": iso(NOW - timedelta(minutes=30))}], "50m", False),
    ([{"databaseId": 1, "conclusion": "success", "createdAt": iso(NOW - timedelta(minutes=70))}], "50m", True),
    ([], "today", True),
])
def test_backup_runs_skip_only_when_already_done(runs, since, want):
    assert bc.decide("schedule", runs, NOW, since)[0] is want


def test_backup_ignores_its_own_run():
    runs = [{"databaseId": 7, "conclusion": "success", "createdAt": iso(NOW)}]
    assert bc.decide("schedule", runs, NOW, "today", this_run="7")[0] is True


# ── workflow wiring ──────────────────────────────────────────────────────────

def wf(name):
    return yaml.safe_load(open(os.path.join(REPO, ".github", "workflows", name)))


@pytest.mark.parametrize("name", DAILY_WORKFLOWS)
def test_daily_workflow_has_backup_check(name):
    w = wf(name)
    on = w[True]
    assert "workflow_dispatch" in on, "the keeper starts it with workflow_dispatch"
    for c in on["schedule"]:
        assert not c["cron"].startswith("0 "), "GitHub delays top-of-the-hour schedules the most"
    steps = w["jobs"]["check"]["steps"]
    assert any(f"backup_check.py --workflow {name}" in (s.get("run") or "") for s in steps)
    assert w["jobs"]["check"]["permissions"]["actions"] == "read"
    others = [j for j in w["jobs"] if j != "check"]
    assert others and all(w["jobs"][j].get("needs") == "check" and "needs.check.outputs.run == 'true'" in w["jobs"][j].get("if", "")
                          for j in others)


def test_keeper_workflow():
    w = wf("keeper.yml")
    assert w["permissions"]["actions"] == "write" and w["concurrency"]["group"] == "keeper"
    assert w["jobs"]["keep"]["timeout-minutes"] < 360
    r = subprocess.run(["bash", "-n", os.path.join(REPO, ".github", "scripts", "keeper.sh")], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


def test_schedule_report_delay():
    slots = sr.slot_times("mlb_pitcher_pipeline.yml")
    assert sr.delay_minutes(et(2027, 4, 10, 6, 5), slots) == 5
    assert sr.delay_minutes(et(2027, 4, 11, 1, 0), slots) == 19 * 60       # before today's 6:00 -> yesterday's

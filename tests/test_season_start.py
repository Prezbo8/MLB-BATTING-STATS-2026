"""March automation: season_start.yml (unpause + first-week check) and keepalive.yml."""

import os
import subprocess
import sys
from datetime import date, datetime, timedelta, timezone

import pytest
import yaml

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, ".github", "scripts"))

import keepalive_check as kc   # noqa: E402
import season_ops as so        # noqa: E402

OD = date(2027, 3, 25)


@pytest.mark.parametrize("today, started, checked, want", [
    (date(2026, 11, 17), False, False, None),        # the fall: never unpause
    (date(2027, 3, 24), False, False, None),
    (OD, False, False, None),                        # Opening Day itself: not yet
    (date(2027, 3, 26), False, False, "start"),      # the day after Opening Day
    (date(2027, 3, 27), True, False, None),          # started; the week isn't up
    (date(2027, 4, 2), True, False, "week"),         # Opening Day + 8
    (date(2027, 4, 2), False, False, "start"),       # a missed start catches up first
    (date(2027, 4, 3), True, True, None),            # both done
    (date(2027, 4, 30), False, False, None),         # past the 30-day window
])
def test_what_each_day_calls_for(today, started, checked, want):
    assert so.stage(today, OD, started, checked) == want


def test_keepalive_commits_only_after_45_quiet_days():
    now = datetime(2027, 1, 10, tzinfo=timezone.utc)
    assert kc.needs_commit(now - timedelta(days=44), now) is False
    assert kc.needs_commit(now - timedelta(days=45), now) is True


def wf(name):
    return yaml.safe_load(open(os.path.join(REPO, ".github", "workflows", name)))


def test_season_start_enables_exactly_the_keepers_seasonal_workflows():
    seasonal = subprocess.run([sys.executable, os.path.join(REPO, ".github", "scripts", "next_slot.py"), "--seasonal"],
                              capture_output=True, text=True, check=True).stdout.split()
    w = wf("season_start.yml")
    assert sorted(w["env"]["SEASONAL"].split()) == sorted(seasonal)
    assert w["permissions"] == {"actions": "write", "contents": "write", "issues": "write"}


def test_season_start_runs_daily_in_march_and_april_and_dispatch_defaults_to_dry_run():
    w = wf("season_start.yml")
    on = w[True]
    assert [c["cron"].split()[3] for c in on["schedule"]] == ["3,4"]
    assert on["workflow_dispatch"]["inputs"]["dry_run"]["default"] is True


def test_keepalive_leaves_paused_workflows_alone():
    text = open(os.path.join(REPO, ".github", "workflows", "keepalive.yml")).read()
    assert "disabled_manually) echo" in text and "/enable" in text
    w = wf("keepalive.yml")
    assert w["permissions"] == {"actions": "write", "contents": "write"}

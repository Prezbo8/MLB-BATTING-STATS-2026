"""The keeper loop itself (.github/scripts/keeper.sh), run for real against a fake `gh`.

tests/test_keeper.py covers the timetable and the backup check; this runs the bash loop the
keeper workflow runs, with a stand-in GitHub CLI that reports which workflows are enabled and
logs every start request instead of making it. Fable's re-grade after Round H: the loop's real
job (start the slot's workflow, skip paused ones, hand off before the time limit) had only been
dry-run by hand.
"""

import os
import shutil
import stat
import subprocess
import sys

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SCRIPTS = os.path.join(REPO, ".github", "scripts")
KEEPER = os.path.join(SCRIPTS, "keeper.sh")
SEASONAL = subprocess.run([sys.executable, os.path.join(SCRIPTS, "next_slot.py"), "--seasonal"],
                          capture_output=True, text=True, check=True).stdout.split()

FAKE_GH = r"""#!/usr/bin/env bash
# Stand-in GitHub CLI for the keeper loop:
#   gh api repos/<repo>/actions/workflows/<file> --jq .state -> active / disabled_manually
#   gh workflow run <file> -R <repo>                      -> logs the start (or fails)
if [ "$1" = "api" ]; then
  wf="${2##*/}"
  for d in $FAKE_DISABLED; do [ "$d" = "$wf" ] && { echo disabled_manually; exit 0; }; done
  echo active; exit 0
fi
if [ "$1 $2" = "workflow run" ]; then
  echo "start $3" >> "$FAKE_LOG"
  [ -n "$FAKE_RUN_FAIL" ] && exit 1
  exit 0
fi
echo "unexpected gh call: $*" >> "$FAKE_LOG"; exit 2
"""


@pytest.fixture
def keeper(tmp_path):
    if shutil.which("bash") is None:
        pytest.skip("no bash")
    bindir = tmp_path / "bin"
    bindir.mkdir()
    gh = bindir / "gh"
    gh.write_text(FAKE_GH)
    gh.chmod(gh.stat().st_mode | stat.S_IEXEC)

    def run(disabled=(), run_fail=False, **knobs):
        log, summary = tmp_path / "started.log", tmp_path / "summary.txt"
        for f in (log, summary):
            f.write_text("")
        env = dict(os.environ, PATH=f"{bindir}{os.pathsep}{os.environ['PATH']}",
                   GITHUB_REPOSITORY="Prezbo8/MLB-BATTING-STATS-2026", RUN_SUMMARY=str(summary),
                   FAKE_LOG=str(log), FAKE_DISABLED=" ".join(disabled), FAKE_RUN_FAIL="1" if run_fail else "",
                   FAKE_WAIT="0", AFTER_START="0", RETRY_TRIES="3", RETRY_WAIT="0")
        env.update({k: str(v) for k, v in knobs.items()})             # a test's own knobs win
        p = subprocess.run(["bash", KEEPER], env=env, capture_output=True, text=True, timeout=60)
        starts = [line.split(" ", 1)[1] for line in log.read_text().splitlines() if line.startswith("start ")]
        odd = [line for line in log.read_text().splitlines() if not line.startswith("start ")]
        return p, starts, summary.read_text(), odd
    return run


def next_workflow():
    out = subprocess.run([sys.executable, os.path.join(SCRIPTS, "next_slot.py")],
                         capture_output=True, text=True, check=True).stdout.split()
    return out[1]


def test_offseason_stops_without_starting_or_handing_off(keeper):
    """The real winter: the six seasonal workflows disabled, scrape_results.yml still on."""
    p, starts, summary, odd = keeper(disabled=SEASONAL)
    assert p.returncode == 0, p.stderr
    assert starts == [] and not odd
    assert "paused (offseason)" in summary


def test_season_starts_the_slots_workflow(keeper):
    want = next_workflow()
    p, starts, summary, odd = keeper(MAX_SLOTS=1)
    assert p.returncode == 0, p.stderr
    assert starts == [want] and not odd
    assert f"started {want}" in summary


def test_a_paused_workflow_is_skipped_not_started(keeper):
    want = next_workflow()
    others_on = [w for w in SEASONAL if w != want]
    assert others_on, "need another seasonal workflow enabled so it is still the season"
    p, starts, summary, _ = keeper(disabled=[want], MAX_SLOTS=1)
    assert p.returncode == 0, p.stderr
    assert starts == [] and f"{want} is paused - skipped" in summary


def test_hands_off_to_a_fresh_keeper_before_the_time_limit(keeper):
    # the real wait to the next slot (no FAKE_WAIT) is far past a 1-second budget
    p, starts, summary, _ = keeper(KEEPER_BUDGET=1, FAKE_WAIT="")
    assert p.returncode == 0, p.stderr
    assert starts == ["keeper.yml"] and "handed off to a fresh keeper" in summary


def test_a_refused_start_is_retried_then_reported_and_the_loop_survives(keeper):
    want = next_workflow()
    p, starts, summary, _ = keeper(run_fail=True, MAX_SLOTS=1)
    assert p.returncode == 0, p.stderr                         # one bad slot doesn't kill the keeper
    assert starts == [want] * 3                                # RETRY_TRIES
    assert f"couldn't start {want}" in summary and "backup schedule will run it" in summary


def test_dry_run_starts_no_data_job_but_hands_off_for_real(keeper):
    """A dry run reports the slot, starts no data job, then starts a fresh keeper for real
    (proving on GitHub that the keeper may start workflows)."""
    want = next_workflow()
    p, starts, summary, _ = keeper(DRY_RUN=1, MAX_SLOTS=1)
    assert p.returncode == 0, p.stderr
    assert starts == ["keeper.yml"] and f"would have started {want}" in summary
    assert "started a fresh keeper for real" in summary


def test_dry_run_in_the_offseason_still_checks_the_slot_and_hand_off(keeper):
    p, starts, summary, _ = keeper(disabled=SEASONAL, DRY_RUN=1, MAX_SLOTS=1)
    assert p.returncode == 0, p.stderr
    assert "checking the next slot and the hand-off anyway" in summary
    assert starts == ["keeper.yml"]                      # no data job, just the real hand-off


def test_dry_run_fails_loudly_if_the_keeper_cannot_start_workflows(keeper):
    p, starts, summary, _ = keeper(run_fail=True, DRY_RUN=1, MAX_SLOTS=1)
    assert p.returncode == 1 and "couldn't start a fresh keeper" in summary

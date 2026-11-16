"""Both pages in a real Chromium, on recorded data (Round H4).

Walks every desktop tab and phone panel (tests/browser_harness.py) with outside requests
answered from tests/fixtures/browser/responses.json.gz and the clock frozen at the recording
time. Re-record with: python3 tests/record_browser_fixtures.py
Skips when Playwright or a browser isn't available (CI installs both).
"""

import os
import re
import sys
from urllib.parse import unquote

import pytest

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import browser_harness as bh   # noqa: E402

pw = pytest.importorskip("playwright.sync_api")
HOME_BUDGET_KB = 2048          # the home page used to download 4.6 MB; 1.4 MB since Round G5


@pytest.fixture(scope="module")
def run():
    if not os.path.exists(bh.FIXTURE):
        pytest.skip("no recorded fixtures (python3 tests/record_browser_fixtures.py)")
    fx = bh.load_fixture()
    with bh.Server() as srv, pw.sync_playwright() as p:
        try:
            browser = bh.launch(p)
        except Exception as e:                       # no Chromium / Chrome on this machine
            pytest.skip(f"no browser available: {e}")
        net = bh.Network("replay", srv.base, fx["responses"])
        out = bh.tour(browser, srv.base, net, fx["frozen_at"])
        browser.close()
    out["net"] = net
    return out


def views(run):
    for bucket in ("desktop", "phone390", "phone375"):
        for name, v in run[bucket].items():
            yield f"{bucket}/{name}", v


def test_every_request_was_recorded(run):
    assert not run["net"].unrecorded, ("the pages asked for data the recording doesn't have; re-record "
                                       f"(tests/record_browser_fixtures.py): {run['net'].unrecorded[:5]}")


def test_no_javascript_errors(run):
    assert not run["errors"], run["errors"][:5]


def test_no_tab_shows_an_error_message(run):
    """A tab whose loader fails shows 'Error: ...' in the page instead of crashing."""
    bad = {n: v["errorText"] for n, v in views(run) if v["errorText"]}
    assert not bad, bad


def test_tour_covered_every_tab_and_panel(run):
    assert len(run["desktop"]) >= len(bh.DESKTOP_TABS) + 1
    assert len(run["phone390"]) >= len(bh.PHONE_PANELS) + 1


def test_no_sideways_scroll_on_phones(run):
    bad = {n: v["spill"] or v["overflow"] for n, v in views(run) if n.startswith("phone") and v["overflow"] > 0}
    assert not bad, bad


def test_every_clickable_thing_is_keyboard_reachable(run):
    bad = {n: v["unreachable"] for n, v in views(run) if v["unreachableCount"]}
    assert not bad, bad


def test_every_control_has_a_name(run):
    bad = {n: v["unnamed"] for n, v in views(run) if v["unnamed"]}
    assert not bad, bad


def test_todays_edges_render_with_labels(run):
    for text in (run["desktop_edges"], run["phone_edges"]):
        up = text.upper()
        assert "TOUGH MATCHUP" in up or "SLIGHT EDGE" in up or "NO CLEAR EDGE" in up, text[:200]
        assert re.search(r"scored \d\.\d\d runs a game, vs \d\.\d\d", text) or "No lineups" in text, text[-300:]   # numbers from edges_results.json


def test_pitcher_drawer_opens(run):
    for text in (run["desktop_drawer"], run["phone_drawer"]):
        assert "HOW THE SCORE IS BUILT" in text.upper(), text[:200]


@pytest.mark.parametrize("side", ["desktop", "phone"])
def test_tier_cards_explain_the_tier(run, side):
    text = run[f"{side}_tiertip"]
    how = ("hover" if run.get("desktop_hover") else "click") if side == "desktop" else "tap"
    assert "Backtest" in text and "runs per 9" in text, f"{side} ({how}): {text[:200]!r}"


def test_ticker_pause_button_works(run):
    assert run["desktop_ticker_paused"] == "true"


def test_pitcher_compare_and_splits_render(run):
    assert "Chris Sale" in run["desktop_compare"] or "Skubal" in run["desktop_compare"]
    assert run["desktop_splits_rows"] > 50


# ── downloads (Round G5): behaviour, not source wording ──────────────────────

def test_home_page_stays_light(run):
    for phase in ("desktop-home", "phone-home"):
        kb = sum(n for ph, _, n in run["net"].log if ph == phase) / 1024
        assert kb < HOME_BUDGET_KB, f"{phase}: {kb:.0f} KB"


def test_home_page_loads_only_core_batter_lines(run):
    urls = [unquote(u) for ph, u, _ in run["net"].log if ph in ("desktop-home", "phone-home") and "fangraphs_player_splits?" in u]
    assert urls and all("date_range=eq.season" in u and "split=in.(no_split,vs_lhp,vs_rhp)" in u for u in urls), urls[:3]


def test_pitcher_splits_download_one_slice_at_a_time(run):
    urls = [unquote(u) for _, u, _ in run["net"].log if "pitcher_split_scores?" in u]
    # every request is narrowed: one season (period=eq), one split's score columns (split=eq), the
    # season list (select=period), or one pitcher (name / playerid); never the whole table
    whole = [u for u in urls if not any(k in u for k in ("period=eq.", "split=eq.", "select=period&", "name=eq.", "playerid=eq."))]
    assert urls and not whole, whole[:3]


def test_batter_downloads_are_sorted(run):
    urls = [unquote(u) for _, u, _ in run["net"].log if "fangraphs_player_splits?" in u]
    assert urls and all("order=" in u for u in urls), [u for u in urls if "order=" not in u][:3]


def test_schedule_downloaded_once_per_page_load(run):
    for phase in ("desktop-home", "phone-home"):
        sched = [u for ph, u, _ in run["net"].log if ph == phase and "/api/v1/schedule?" in u]
        assert len(sched) == len(set(sched)), f"{phase}: repeated schedule downloads {sched}"

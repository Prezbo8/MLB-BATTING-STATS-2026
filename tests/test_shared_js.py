"""assets/js/shared.js (the site's matchup / drawer math) agrees with the Python.

Runs the shared file's pure functions in Node and compares them with
scripts/score_pitcher_overall.py and scripts/backtest_edges.py, so changing a
weight on one side fails CI until the other matches. Skips if Node is missing.
"""

import hashlib
import json
import os
import re
import shutil
import subprocess

import pandas as pd
import pytest

import backtest_edges as be
import score_pitcher_overall as spo

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SHARED = os.path.join(REPO, "assets", "js", "shared.js")
NODE = shutil.which("node")
needs_node = pytest.mark.skipif(NODE is None, reason="Node.js not installed")

# Load shared.js in a sandbox (it only touches the page inside functions), call
# one function per input and print the results as JSON.
RUNNER = r"""
const fs = require('fs'), vm = require('vm');
const ctx = vm.createContext({ console, Math, JSON, Number, parseInt, parseFloat, isNaN,
    document: { addEventListener() {} }, matchMedia: () => ({ matches: false }) });
const [file, fn, calls] = [process.argv[1], process.argv[2], JSON.parse(process.argv[3])];
vm.runInContext(fs.readFileSync(file, 'utf8') + `\n;globalThis.__fn = ${fn};`, ctx);
console.log(JSON.stringify(calls.map(args => ctx.__fn(...args))));
"""


def js(fn, calls):
    out = subprocess.run([NODE, "-e", RUNNER, SHARED, fn, json.dumps(calls)],
                         capture_output=True, text=True, timeout=30)
    assert out.returncode == 0, out.stderr
    return json.loads(out.stdout)


def test_shared_js_parses():
    if NODE is None:
        pytest.skip("Node.js not installed")
    r = subprocess.run([NODE, "--check", SHARED], capture_output=True, text=True)
    assert r.returncode == 0, r.stderr


@pytest.mark.parametrize("page", ["index.html", "mobile.html"])
def test_pages_load_the_current_shared_js(page):
    """The ?v= on the script tag is the file's hash, so browsers fetch a changed file."""
    want = hashlib.sha1(open(SHARED, "rb").read()).hexdigest()[:8]
    html = open(os.path.join(REPO, page)).read()
    got = re.findall(r'<script src="assets/js/shared\.js\?v=([0-9a-f]+)"></script>', html)
    assert got == [want], f"{page}: update the script tag to shared.js?v={want}"
    assert html.index("shared.js?v=") < html.index("const SUPABASE_URL"), "load shared.js before the main script"


@needs_node
def test_batter_blend_matches_backtest():
    cases = [(130, 200, 150, 80, 120, 500), (90, 40, 0, 0, None, 0), (0, 0, 0, 0, 110, 300),
             (0, 0, 0, 0, None, 0), (75, 12, 200, 3, 140, 610), (101, 550, 99, 180, 95, 20)]
    calls = [[cw, cp, sw, sp, pw, bool(lp)] for cw, cp, sw, sp, lw, lp in cases
             for pw in [js("priorFromLast", [[lw, lp]])[0]]]
    got = js("blendBatter", calls)
    for (cw, cp, sw, sp, lw, lp), g in zip(cases, got):
        want = be.batter_vs_hand(cw, cp, sw, sp, lw, lp)
        assert (g is None) == (want is None)
        if want is not None:
            assert g["wrc"] == pytest.approx(want)


@needs_node
def test_prior_matches_backtest():
    cases = [[120, 500], [None, 0], [80, 40], [150, 0]]
    for (w, pa), g in zip(cases, js("priorFromLast", cases)):
        assert g == pytest.approx(be.batter_prior(w, pa))


@needs_node
def test_starter_yardstick_matches_backtest():
    pools = [[70.0] * 19, [60.0] * 10 + [80.0] * 10, [float(x) for x in range(30, 100, 3)]]
    for pool, g in zip(pools, js("scaleFrom", [[p] for p in pools])):
        mid, sd = be.starter_scale(pool)
        assert g["mid"] == pytest.approx(mid) and g["sd"] == pytest.approx(sd)


@needs_node
def test_drawer_blend_matches_the_scoring_script(monkeypatch):
    """The drawer's 'weighted blend' = the composite before it is ranked."""
    newest = spo.SEASON
    pitchers = {
        "veteran": [(newest, 174.2, 93.8), (newest - 1, 63, 76.8), ("career", 238, 92.6)],
        "thin":    [(newest, 25, 88.0), ("career", 25, 70.0)],
        "old":     [(newest - 2, 180, 60.0), (newest - 5, 150, 55.0), (newest - 7, 160, 40.0), ("career", 900, 58.0)],
        "under20": [(newest, 12, None), (newest - 1, 140, 81.0), ("career", 400, 79.0)],
    }
    rows = [{"player_key": k, "season": s, "IP": ip, "season_score": sc}
            for k, v in pitchers.items() for s, ip, sc in v]
    monkeypatch.setattr(spo, "active_percentiles", lambda raw, active: raw)   # keep the raw blend
    df = spo.compute_composite_scores(pd.DataFrame(rows), spo.WEIGHTS_WITH_CURRENT)
    want = df.groupby("player_key")["composite_score"].first().to_dict()

    calls = [[[{"season": str(s), "ip": ip, "season_score": sc} for s, ip, sc in v], newest] for v in pitchers.values()]
    for name, g in zip(pitchers, js("drawerBlend", calls)):
        assert g["raw"] == pytest.approx(want[name], abs=0.01), name


@needs_node
def test_drawer_constants_match_the_scoring_script():
    got = js("() => ({w: PD_SEASON_WEIGHTS, c: PD_CAREER_WEIGHT, ip: PD_FULL_IP, p: PD_PRIOR, m: PD_MIN_IP})", [[]])[0]
    newest = spo.SEASON
    assert got["w"] == [spo.WEIGHTS_WITH_CURRENT.get(newest - i, 0) for i in range(len(got["w"]))]
    assert got["c"] == spo.WEIGHTS_WITH_CURRENT["career"]
    assert (got["ip"], got["p"], got["m"]) == (spo.FULL_WEIGHT_IP, spo.PRIOR_WEIGHT, spo.MIN_IP)
    assert spo.PRIOR_SCORE == 50

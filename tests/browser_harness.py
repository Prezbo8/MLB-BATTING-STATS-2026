"""Shared by tests/test_browser.py and tests/record_browser_fixtures.py (Round H4).

Opens index.html (desktop) and mobile.html (phone) in a real Chromium, served from a local
server, and walks every tab / panel. Every outside request (Supabase, MLB's API) is either
recorded live (recorder) or answered from tests/fixtures/browser/responses.json.gz (test),
with the browser clock frozen at the recording time so "today's games" never changes.
External images and fonts are blocked: logos don't affect any check.
"""

import base64
import functools
import gzip
import http.server
import json
import os
import threading
import time
from datetime import datetime

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
FIXTURE = os.path.join(REPO, "tests", "fixtures", "browser", "responses.json.gz")

DESKTOP_TABS = [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13]
PHONE_PANELS = ["rankings", "players", "pitchers", "compare", "hotcold", "lineups", "rpg", "report", "today"]

# In-page checks, run on every view
AUDIT_JS = r"""() => {
  const NATIVE = ['A','BUTTON','INPUT','SELECT','TEXTAREA','OPTION','LABEL','SUMMARY'];
  const focusable = el => el.tabIndex >= 0 || NATIVE.includes(el.tagName);
  const vis = el => { const r = el.getBoundingClientRect(); return r.width > 0 && r.height > 0 && getComputedStyle(el).visibility !== 'hidden'; };
  // clickable = has a click handler attribute, a delegated-click marker, or a pointer cursor
  // (sortable headers use the 'help' cursor for their tooltips, so the cursor alone misses them)
  const clickable = el => el.hasAttribute('onclick') || el.matches('[data-pid]:not([data-pid=""]),[data-pitcher],.pr-suggest-item')
      || getComputedStyle(el).cursor === 'pointer';
  const unreachable = [...document.querySelectorAll('body *')].filter(el => vis(el) && clickable(el)
      && !focusable(el) && !el.closest('a,button,label,select,input,summary,[tabindex]:not([tabindex="-1"])')   // inside something focusable; tabindex=-1 (the skip-link target, <main>) doesn't count
      && !el.matches('.tier-pill,.tier-pill *,img,svg,svg *,option'));
  const unnamed = [...document.querySelectorAll('select,input:not([type=hidden])')].filter(el => vis(el)
      && !(el.labels && el.labels.length) && !el.getAttribute('aria-label') && !el.getAttribute('aria-labelledby'));
  // content past the right edge of the screen. The phone page never scrolls the document
  // sideways (overflow hidden), so too-wide content is silently CUT OFF: measure elements,
  // except inside intentional sideways scrollers (overflow-x auto/scroll) and fixed overlays
  const W = window.innerWidth;
  const scroller = el => { for (let a = el.parentElement; a && a !== document.body; a = a.parentElement) {
      const s = getComputedStyle(a); if (s.overflowX === 'auto' || s.overflowX === 'scroll' || s.position === 'fixed') return true; } return false; };
  const spill = [...document.querySelectorAll('body *')].filter(el => vis(el) && el.getBoundingClientRect().right > W + 1
      && getComputedStyle(el).position !== 'fixed' && !scroller(el));
  // visible error messages (a failed tab shows "Error: ..." instead of crashing)
  const errText = [...document.querySelectorAll('.empty-state-text,.m-empty-text,.side-empty,.pd-empty,.edge-note,.m-sb-empty')]
      .filter(vis).map(e => e.innerText).filter(t => /error|failed|couldn.t load|unavailable/i.test(t));
  return { errorText: errText.slice(0, 3),
           overflow: Math.max(document.documentElement.scrollWidth - W, ...spill.map(e => Math.round(e.getBoundingClientRect().right - W)), 0),
           spill: spill.slice(0, 4).map(e => e.tagName + '.' + (e.className || '') + ' +' + Math.round(e.getBoundingClientRect().right - W) + 'px'),
           unreachable: unreachable.slice(0, 5).map(e => e.tagName + '.' + e.className + ' ' + (e.innerText || '').slice(0, 30)),
           unreachableCount: unreachable.length,
           unnamed: unnamed.map(e => e.id || e.outerHTML.slice(0, 60)) };
}"""


class Server:
    """The repo served on 127.0.0.1 (any free port), quietly, in a thread."""

    def __enter__(self):
        handler = functools.partial(_Quiet, directory=REPO)
        self.httpd = http.server.ThreadingHTTPServer(("127.0.0.1", 0), handler)
        threading.Thread(target=self.httpd.serve_forever, daemon=True).start()
        self.base = f"http://127.0.0.1:{self.httpd.server_port}"
        return self

    def __exit__(self, *exc):
        self.httpd.shutdown()


class _Quiet(http.server.SimpleHTTPRequestHandler):
    def log_message(self, *a):
        pass


class Network:
    """Routes every request: local files pass through; outside images/fonts are blocked;
    other outside requests are recorded (mode 'record') or replayed (mode 'replay')."""

    def __init__(self, mode, base, store=None):
        self.mode, self.base = mode, base
        self.store = store if store is not None else {}
        self.unrecorded, self.log, self.inflight = [], [], 0
        self.phase = "load"

    def handle(self, route):
        req = route.request
        url = req.url
        if url.startswith(self.base):
            return route.continue_()
        if req.resource_type in ("image", "font", "media") or url.startswith("data:"):
            return route.abort()
        key = f"{req.method} {url}"
        self.inflight += 1
        try:
            if req.method == "OPTIONS":
                return route.fulfill(status=204, headers=_cors({}))
            if self.mode == "record":
                resp = route.fetch()
                body = resp.body()
                self.store[key] = {"status": resp.status, "ctype": resp.headers.get("content-type", ""), **_pack(body)}
                self.log.append((self.phase, url, len(body)))
                return route.fulfill(response=resp, headers=_cors(resp.headers))
            hit = self.store.get(key)
            if hit is None:
                self.unrecorded.append(key)
                return route.abort()
            body = _unpack(hit)
            self.log.append((self.phase, url, len(body)))
            return route.fulfill(status=hit["status"], body=body, headers=_cors({"content-type": hit["ctype"]}))
        finally:
            self.inflight -= 1

    def settle(self, page, quiet_ms=500, timeout_s=20):
        """Wait until no outside request has been in flight for quiet_ms."""
        end, quiet_since = time.time() + timeout_s, None
        while time.time() < end:
            page.wait_for_timeout(100)
            if self.inflight == 0:
                quiet_since = quiet_since or time.time()
                if (time.time() - quiet_since) * 1000 >= quiet_ms:
                    return
            else:
                quiet_since = None


def _pack(body):
    """Text stays text (compresses ~4x better than base64); anything else is base64."""
    try:
        return {"text": body.decode("utf-8")}
    except UnicodeDecodeError:
        return {"b64": base64.b64encode(body).decode()}


def _unpack(hit):
    return hit["text"].encode("utf-8") if "text" in hit else base64.b64decode(hit["b64"])


def _cors(h):
    h = dict(h)
    h["access-control-allow-origin"] = "*"
    h["access-control-allow-headers"] = "*"
    return h


def load_fixture():
    with gzip.open(FIXTURE, "rt") as f:
        return json.load(f)


def save_fixture(frozen_at, store):
    os.makedirs(os.path.dirname(FIXTURE), exist_ok=True)
    with gzip.open(FIXTURE, "wt", compresslevel=9) as f:
        json.dump({"frozen_at": frozen_at, "responses": dict(sorted(store.items()))}, f)


def launch(p):
    """CI installs Playwright's Chromium; locally fall back to the installed Google Chrome."""
    try:
        return p.chromium.launch()
    except Exception:
        return p.chromium.launch(channel="chrome")


def _wait_for_note(page):
    """The edges note gets its numbers once edges_results.json has loaded: wait for them."""
    try:
        page.wait_for_function("() => /runs a game/.test((document.querySelector('.edge-note-text') || {}).textContent || '')", timeout=5000)
    except Exception:
        pass                       # the test then reports the missing numbers


def _wait_for_tip(page):
    """The tier card appears once backtest_results.json has loaded: wait for it, not a fixed
    time (a fixed 0.5 s was flaky on GitHub's slower runners)."""
    try:
        page.wait_for_selector("#tier-tip.show", timeout=5000)
    except Exception:
        pass                       # the test then reports the empty card


def tour(browser, base, net, frozen_at):
    """Walk both pages; returns per-view results. Recorder and test run exactly this."""
    out = {"desktop": {}, "phone390": {}, "phone375": {}, "errors": []}
    frozen = datetime.fromisoformat(frozen_at)

    def new_page(width, height, mobile=False):
        ctx = browser.new_context(viewport={"width": width, "height": height}, is_mobile=mobile, has_touch=mobile)
        page = ctx.new_page()
        page.clock.set_fixed_time(frozen)
        page.route("**/*", net.handle)
        page.on("pageerror", lambda e: out["errors"].append(f"{width}px pageerror: {e}"))
        page.on("console", lambda m: m.type == "error" and "Failed to load resource" not in m.text
                and out["errors"].append(f"{width}px console: {m.text[:200]}"))
        return ctx, page

    def view(page, bucket, name):
        net.settle(page)
        page.wait_for_timeout(300)
        out[bucket][name] = page.evaluate(AUDIT_JS)

    # ── desktop ──
    ctx, page = new_page(1440, 900)
    net.phase = "desktop-home"
    page.goto(f"{base}/index.html")
    view(page, "desktop", "home")
    _wait_for_note(page)
    out["desktop_edges"] = page.inner_text("#today-edges")
    for n in DESKTOP_TABS:
        net.phase = f"desktop-tab{n}"
        page.evaluate(f"switchTab({n})")
        view(page, "desktop", f"tab{n}")
        if n == 10:                                       # pitcher splits: default view + one more season
            page.evaluate("ptsToggle('splits')")
            view(page, "desktop", "tab10-splits")
            page.evaluate("""() => { const s = document.getElementById('ps-period');
                if (s.options.length > 1) { s.value = s.options[1].value; s.dispatchEvent(new Event('change')); } }""")
            view(page, "desktop", "tab10-splits-2")
            out["desktop_splits_rows"] = page.evaluate("document.querySelectorAll('#ps-body tr').length")
        if n == 11:                                       # compare two pitchers
            page.evaluate("pcSelect('a', 'Chris Sale'); pcSelect('b', 'Tarik Skubal')")
            view(page, "desktop", "tab11-compare")
            out["desktop_compare"] = page.inner_text("#pc-container")[:400]
    net.phase = "desktop-drawer"
    page.evaluate("switchTab(13)")
    view(page, "desktop", "home-again")
    page.locator("#today-pitchers [data-pitcher]").first.click()
    view(page, "desktop", "drawer")
    out["desktop_drawer"] = page.inner_text("#pd-body")[:600]
    page.keyboard.press("Escape")
    pill = page.locator("#rankings-body .tier-pill, #today-pitchers .tier-pill").first
    out["desktop_hover"] = not page.evaluate("matchMedia('(hover: none)').matches")
    # the site shows tier cards on hover where the device can hover, on tap where it can't;
    # a headless Linux browser may report no hover, so do what a user there would do
    # scroll first and let it settle: the site closes tier cards on any scroll, and the scroll
    # Playwright does inside hover() can land just after the card opened (seen on CI runners)
    pill.scroll_into_view_if_needed()
    page.wait_for_timeout(400)
    (pill.hover if out["desktop_hover"] else pill.click)()
    _wait_for_tip(page)
    out["desktop_tiertip"] = page.evaluate("(document.getElementById('tier-tip') || {}).innerText || ''")
    page.locator("#ticker-pause").click()
    out["desktop_ticker_paused"] = page.get_attribute("#ticker-pause", "aria-pressed")
    ctx.close()

    # ── phone, 390 px: every panel ──
    ctx, page = new_page(390, 844, mobile=True)
    net.phase = "phone-home"
    page.goto(f"{base}/mobile.html")
    view(page, "phone390", "today")
    _wait_for_note(page)
    out["phone_edges"] = page.inner_text("#today-edges")
    for panel in PHONE_PANELS:
        net.phase = f"phone-{panel}"
        page.evaluate(f"switchPanel('{panel}')")
        view(page, "phone390", panel)
    net.phase = "phone-drawer"
    page.locator("#today-pitchers [data-pitcher]").first.click()
    view(page, "phone390", "drawer")
    out["phone_drawer"] = page.inner_text("#pd-body")[:600]
    page.keyboard.press("Escape")
    tpill = page.locator("#today-pitchers .tier-pill").first
    tpill.scroll_into_view_if_needed()
    page.wait_for_timeout(400)
    tpill.tap()
    _wait_for_tip(page)
    out["phone_tiertip"] = page.evaluate("(document.getElementById('tier-tip') || {}).innerText || ''")
    ctx.close()

    # ── phone, 375 px: the narrowest common width ──
    ctx, page = new_page(375, 812, mobile=True)
    net.phase = "phone375"
    page.goto(f"{base}/mobile.html")
    view(page, "phone375", "today")
    for panel in ("rankings", "lineups", "pitchers"):
        page.evaluate(f"switchPanel('{panel}')")
        view(page, "phone375", panel)
    ctx.close()
    return out

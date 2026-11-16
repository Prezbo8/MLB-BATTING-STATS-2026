#!/usr/bin/env python3
"""Re-record the data tests/test_browser.py replays (Round H4).

Opens both pages against the LIVE site data (read-only: the pages only read, with the public
key), walks the same tour as the test, and saves every outside response to
tests/fixtures/browser/responses.json.gz together with the moment it was recorded; the test
freezes the browser clock at that moment so "today" matches the recording.

Re-record when the data's shape changes (new columns, new tables) or the test reports
"unrecorded requests":   python3 tests/record_browser_fixtures.py
"""
import os
import sys
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
import browser_harness as bh                          # noqa: E402
from playwright.sync_api import sync_playwright       # noqa: E402


def main():
    frozen_at = datetime.now(timezone.utc).replace(microsecond=0).isoformat()
    with bh.Server() as srv, sync_playwright() as p:
        browser = bh.launch(p)
        net = bh.Network("record", srv.base)
        out = bh.tour(browser, srv.base, net, frozen_at)
        browser.close()
    bh.save_fixture(frozen_at, net.store)
    size = os.path.getsize(bh.FIXTURE) / 1024
    print(f"recorded {len(net.store)} responses at {frozen_at} -> {os.path.relpath(bh.FIXTURE, bh.REPO)} ({size:.0f} KB)")
    if out["errors"]:
        print("page errors while recording (fix before trusting the fixtures):")
        for e in out["errors"]:
            print("  ", e)


if __name__ == "__main__":
    main()

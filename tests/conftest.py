"""
Shared test setup. Runs before any test module imports a script, because
several scripts look up the season (MLB's API) the moment they're imported.

- Pretend today is 2026-11-13 (MLB_TODAY), so results don't change with the
  real date.
- No real network: every HTTP call goes through `requests.Session.request`,
  which is replaced by a router that answers from tests/fixtures/ and records
  what would have been sent to Supabase / GitHub / Telegram. Anything else
  raises, so a test can never touch live data.
- time.sleep is a no-op (the scrapers wait between calls and retries).
"""

import json
import os
import re
import sys
import time

import pytest
import requests

HERE     = os.path.dirname(os.path.abspath(__file__))
REPO     = os.path.dirname(HERE)
FIXTURES = os.path.join(HERE, "fixtures")
sys.path.insert(0, os.path.join(REPO, "scripts"))

os.environ["MLB_TODAY"] = "2026-11-13"
for var in ("GITHUB_TOKEN", "TELEGRAM_TOKEN", "TELEGRAM_CHAT_ID"):
    os.environ.pop(var, None)
os.environ["SUPABASE_URL"] = "https://supabase.test"
os.environ["SUPABASE_KEY"] = "test-key"

time.sleep = lambda *_a, **_k: None


def fixture(name):
    with open(os.path.join(FIXTURES, name), "rb") as f:
        return f.read()


def response(status=200, body=b"", content_type="application/json"):
    r = requests.models.Response()
    r.status_code = status
    r._content = body if isinstance(body, bytes) else json.dumps(body).encode()
    r.headers["Content-Type"] = content_type
    r.encoding = "utf-8"
    return r


class Network:
    """Answers every request from fixtures; records writes."""

    def __init__(self):
        self.swaps   = []      # replace_rows bodies sent to Supabase
        self.github  = []      # paths PUT to the GitHub contents API
        self.calls   = []      # (method, url) of everything
        self.season_dates = {}             # year -> (start, end), override per test
        self.mlb_down = False
        self.fail = {}                     # url substring -> status to return

    def seasons(self, year):
        start, end = self.season_dates.get(year, (f"{year}-03-25", f"{year}-09-27"))
        return {"seasons": [{"regularSeasonStartDate": start, "regularSeasonEndDate": end}]}

    def __call__(self, session, method, url, params=None, json=None, data=None, **kw):
        method = method.upper()
        self.calls.append((method, url))
        for part, status in self.fail.items():
            if part in url:
                return response(status, {"message": "forced failure"})

        if "statsapi.mlb.com" in url:
            if self.mlb_down:
                raise requests.ConnectionError("MLB API down (test)")
            m = re.search(r"/api/v1/seasons/(\d{4})", url)
            if m:
                return response(200, self.seasons(int(m.group(1))))
            if "/schedule" in url:
                return response(200, fixture("mlb_schedule_season.json"))

        if "fangraphs.com/api/leaders/major-league/data" in url:
            kind = "career" if "season2=" in url else "season"
            return response(200, fixture(f"fg_leaders_{kind}.json"))

        if "fangraphs.com/api/leaders/splits/splits-leaders" in url:
            if json["strPosition"] == "P":
                return response(200, fixture(f"fg_splits_pitcher_sg{json['strType']}.json"))
            name = "team" if json["strStatType"] == "team" else "player"
            return response(200, fixture(f"fg_splits_{name}.json"))

        if "teamrankings.com" in url:
            return response(200, fixture("teamrankings_rpg.html"), "text/html")

        if url.startswith("https://supabase.test/rest/v1/rpc/replace_rows"):
            self.swaps.append(json)
            return response(200, len(json["p_rows"]))

        if "api.github.com" in url:
            if method == "GET":
                return response(404, {"message": "Not Found"})
            self.github.append(url.split("/contents/", 1)[1])
            return response(201, {"content": {}})

        if "api.telegram.org" in url:
            return response(200, {"ok": True})

        raise RuntimeError(f"Unexpected network call in a test: {method} {url}")


NET = Network()
requests.Session.request = lambda self, method, url, **kw: NET(self, method, url, **kw)


@pytest.fixture
def net():
    """Fresh recorder for one test."""
    NET.__init__()
    return NET


@pytest.fixture
def workdir(tmp_path, monkeypatch):
    """Run a script from an empty folder (they write relative paths)."""
    monkeypatch.chdir(tmp_path)
    return tmp_path

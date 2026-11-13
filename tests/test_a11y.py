"""Accessibility basics on both pages (Round G4) — static checks of the HTML."""

import os
import re

import pytest

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
PAGES = ["index.html", "mobile.html"]


def html(page):
    return open(os.path.join(REPO, page)).read()


def lum(hx):
    r, g, b = (int(hx.lstrip("#")[i:i + 2], 16) / 255 for i in (0, 2, 4))
    f = lambda c: c / 12.92 if c <= 0.03928 else ((c + 0.055) / 1.055) ** 2.4
    return 0.2126 * f(r) + 0.7152 * f(g) + 0.0722 * f(b)


def contrast(a, b):
    hi, lo = sorted((lum(a), lum(b)), reverse=True)
    return (hi + 0.05) / (lo + 0.05)


@pytest.mark.parametrize("page", PAGES)
def test_every_form_control_has_a_name(page):
    s = html(page)
    labelled = set(re.findall(r'<label[^>]*\bfor="([^"]+)"', s))
    missing = []
    for m in re.finditer(r"<(select|input)\b([^>]*)>", s):
        attrs = m.group(2)
        line = s[s.rfind("\n", 0, m.start()) + 1:m.start()]
        if line.lstrip().startswith("//"):          # a code comment that mentions <select>
            continue
        if "aria-label" in attrs or 'type="hidden"' in attrs:
            continue
        mid = re.search(r'\bid="([^"]+)"', attrs)
        if not mid or mid.group(1) not in labelled:
            missing.append(m.group(0)[:70])
    assert not missing, missing


@pytest.mark.parametrize("page", PAGES)
def test_every_image_has_alt_text(page):
    assert not re.findall(r"<img\b(?![^>]*\balt=)[^>]*>", html(page))


@pytest.mark.parametrize("page", PAGES)
def test_skip_link_and_focus_ring(page):
    s = html(page)
    assert '<a class="skip-link" href="#main-content">' in s and 'id="main-content"' in s
    assert ":focus-visible { outline: 2px solid var(--focus)" in s


@pytest.mark.parametrize("page", PAGES)
def test_text_colors_meet_4_5_to_1(page):
    """Body text, secondary text and caption gray on every panel background, both themes."""
    s = html(page)
    for block in (re.search(r":root\s*\{([^}]*)\}", s).group(1),
                  re.search(r'\[data-theme="light"\]\s*\{([^}]*)\}', s).group(1)):
        v = dict(re.findall(r"--([\w-]+):\s*(#[0-9a-fA-F]{6})", block))
        for fg in ("text", "text2", "muted"):
            for bg in ("bg", "surface", "surface2", "surface3"):
                assert contrast(v[fg], v[bg]) >= 4.5, (page, fg, bg, round(contrast(v[fg], v[bg]), 2))


def test_shared_js_makes_clickables_keyboard_reachable():
    js = open(os.path.join(REPO, "assets", "js", "shared.js")).read()
    assert "function a11yEnhance" in js and "MutationObserver" in js and "aria-sort" in js


def test_ticker_has_a_pause_button():
    s = html("index.html")
    assert 'id="ticker-pause"' in s and "_tickerUserPaused" in s

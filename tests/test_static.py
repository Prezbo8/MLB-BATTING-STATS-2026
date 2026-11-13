"""Cheap checks that catch a typo before it blanks a page or breaks a workflow."""

import glob
import os
import re
import shutil
import subprocess

import pytest
import yaml

from conftest import REPO

NODE = shutil.which("node")


@pytest.mark.skipif(not NODE, reason="node not installed")
@pytest.mark.parametrize("page", ["index.html", "mobile.html"])
def test_page_scripts_parse(page, tmp_path):
    html = open(os.path.join(REPO, page), encoding="utf-8").read()
    blocks = re.findall(r"<script(?![^>]*\bsrc=)[^>]*>(.*?)</script>", html, re.S | re.I)
    assert blocks, page
    for i, js in enumerate(blocks):
        f = tmp_path / f"{page}.{i}.js"
        f.write_text(js, encoding="utf-8")
        r = subprocess.run([NODE, "--check", str(f)], capture_output=True, text=True)
        assert r.returncode == 0, f"{page} <script> #{i}: {r.stderr[:500]}"


@pytest.mark.skipif(not NODE, reason="node not installed")
def test_report_script_parses():
    r = subprocess.run([NODE, "--check", os.path.join(REPO, "scripts", "report_summary.js")],
                       capture_output=True, text=True)
    assert r.returncode == 0, r.stderr[:500]


@pytest.mark.parametrize("path", sorted(glob.glob(os.path.join(REPO, ".github", "workflows", "*.yml"))))
def test_workflow_files_parse(path):
    wf = yaml.safe_load(open(path))
    assert "jobs" in wf and wf["jobs"], path


def test_scripts_named_in_workflows_exist():
    for path in glob.glob(os.path.join(REPO, ".github", "workflows", "*.yml")):
        for script in re.findall(r"(?:python|node) (scripts/[\w./-]+)", open(path).read()):
            assert os.path.exists(os.path.join(REPO, script)), f"{os.path.basename(path)}: {script}"

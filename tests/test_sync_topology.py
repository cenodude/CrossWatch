# tests/test_sync_topology.py
# CrossWatch - Sync topology integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from pathlib import Path
import shutil
import subprocess

import pytest

ROOT = Path(__file__).resolve().parents[1]


def test_topology_analysis_and_layout():
    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js is needed for the local frontend topology tests")
    result = subprocess.run(
        [node, "--test", "tests/topology.test.mjs"], cwd=ROOT,
        capture_output=True, text=True, encoding="utf-8", timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


def test_topology_summary_extends_sync_and_stays_out_of_managed_shell():
    from ui_frontend import get_index_html

    html = get_index_html()
    assert html.index('id="providers_list"') < html.index('id="sync-topology-health"') < html.index('id="pairs_list"')
    assert '/assets/helpers/feature-meta.js' in html
    assert '/assets/js/topology/advisor.js' in html
    assert 'id="sync-topology-health"' not in get_index_html(include_admin=False)

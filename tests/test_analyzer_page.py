# tests/test_analyzer_page.py
# CrossWatch - Analyzer Page Access Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from ui_frontend import get_index_html


@pytest.mark.parametrize("admin,write,visible", [(True, False, True), (False, True, True), (False, False, False)])
def test_analyzer_page_respects_existing_write_access(admin, write, visible):
    html = get_index_html(include_admin=admin, user={"permissions": {"write": write, "dashboard": True}})
    assert ('id="page-analyzer"' in html) is visible
    assert 'id="tab-analyzer"' not in html
    if visible:
        assert 'html[data-cw-initial-tab="analyzer"] #page-analyzer' in html
        assert 'analyzer: "Analyzer"' in html

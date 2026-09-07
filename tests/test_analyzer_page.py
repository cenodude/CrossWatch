# tests/test_analyzer_page.py
# CrossWatch - Analyzer Page Access Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from ui_frontend import get_index_html


@pytest.mark.parametrize("admin,write,visible", [(True, False, True), (False, True, True), (False, False, False)])
@pytest.mark.parametrize("page,label", [("analyzer", "Analyzer"), ("events", "Events"), ("logs", "Logs")])
def test_analyzer_page_respects_existing_write_access(admin, write, visible, page, label):
    html = get_index_html(include_admin=admin, user={"permissions": {"write": write, "dashboard": True}})
    assert (f'id="page-{page}"' in html) is visible
    assert f'id="tab-{page}"' not in html
    if visible:
        assert f'html[data-cw-initial-tab="{page}"] #page-{page}' in html
        assert f'{page}: "{label}"' in html

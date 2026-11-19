# tests/test_copy_fields.py
# CrossWatch - Copy buttons and copyable URL field tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from pathlib import Path

ENDPOINT_SOURCES = (
    "assets/js/modals/scrobbler-webhook/index.js",
    "assets/js/modals/scrobbler-route/index.js",
    "assets/js/scrobbler.js",
)


def test_clipboard_helper_loads_on_main_and_hub_pages() -> None:
    import ui_frontend

    pages = (
        ui_frontend.get_index_html(include_admin=True, user={"is_admin": True, "username": "admin"}),
        ui_frontend.get_index_html(include_admin=False, user={"is_admin": False, "username": "viewer", "permissions": {"write": False}}),
        ui_frontend.get_profile_html(user={"is_admin": True, "username": "admin"}),
    )
    for html in pages:
        assert html.count("/assets/helpers/clipboard.js") == 1


def test_endpoint_urls_are_selectable_read_only_fields() -> None:
    for path in ENDPOINT_SOURCES:
        js = Path(path).read_text("utf-8")
        assert re.search(r'<input type="text" readonly value="\$\{esc\([^)]+\)\}"[^>]*data-copy-field>', js), path
        assert "window.cwCopyText?.(" in js, path
        assert "window.cwSelectCopyField?.(" in js, path


def test_only_the_clipboard_helper_calls_the_clipboard_api() -> None:
    offenders = []
    for path in Path("assets").rglob("*.js"):
        if path.as_posix() == "assets/helpers/clipboard.js":
            continue
        for number, line in enumerate(path.read_text("utf-8").splitlines(), 1):
            if path.as_posix() == "assets/auth/auth.shared.js" and line.lstrip().startswith("'"):
                continue
            if "navigator.clipboard" in line or re.search(r"execCommand\(['\"]copy", line):
                offenders.append(f"{path.as_posix()}:{number}")
    assert offenders == []

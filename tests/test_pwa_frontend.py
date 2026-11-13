# tests/test_pwa_frontend.py
# CrossWatch - Shared installation entry points for Main and My Media Hub
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from html.parser import HTMLParser
import json
from pathlib import Path
from urllib.parse import urlsplit

import pytest

import ui_frontend


class AssetLinks(HTMLParser):
    def __init__(self, markup):
        super().__init__()
        self.scripts = []
        self.manifests = []
        self.feed(markup)

    def handle_starttag(self, tag, attrs):
        values = dict(attrs)
        if tag == "script" and values.get("src"):
            self.scripts.append(urlsplit(values["src"]).path)
        if tag == "link" and values.get("rel") == "manifest":
            self.manifests.append(values.get("href"))


@pytest.mark.parametrize("admin", [False, True])
@pytest.mark.parametrize("page", ["main", "profile"])
def test_installation_is_available_on_each_page(monkeypatch, admin, page):
    monkeypatch.setattr(ui_frontend, "_managed_tracker_allowed", lambda user: False)
    user = {"is_admin": admin, "username": "test", "profile_id": "default"}
    markup = (
        ui_frontend.get_index_html(include_admin=admin, user=user)
        if page == "main"
        else ui_frontend.get_profile_html(user=user)
    )
    links = AssetLinks(markup)
    assert 'aria-label="Compact mode"' not in markup
    assert links.manifests == ["/manifest.webmanifest"]
    assert links.scripts.count("/assets/helpers/pwa-install.js") == 1
    assert links.scripts.index("/assets/helpers/pwa-install.js") < links.scripts.index("/assets/helpers/account-menu.js")
    if page == "profile":
        assert "/assets/crosswatch.js" not in links.scripts


def test_installed_app_opens_the_responsive_ui_with_the_same_identity():
    for manifest in [json.loads(ui_frontend.DEFAULT_MANIFEST), json.loads(Path("assets/manifest.webmanifest").read_text("utf-8"))]:
        assert manifest["id"] == "/"
        assert manifest["start_url"] == "/"
        assert manifest["display"] == "standalone"

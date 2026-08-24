from __future__ import annotations

from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def _read(rel: str) -> str:
    return (ROOT / rel).read_text(encoding="utf-8")


def test_media_server_whitelist_ui_exposes_collection_category() -> None:
    css = _read("assets/css/pages.css")
    table_js = _read("assets/helpers/whitelist_table.js")
    providers_ui = _read("assets/helpers/providers-ui.js")
    assert "--wl-feature-count" in css
    assert "repeat(var(--wl-feature-count),var(--wl-col))" in css
    assert "--wl-coll:#ff7a1a" in css
    assert "cw-wl-coll" in css
    assert 'coll: "inventory_2"' in table_js
    assert "__cwWlClickHandler" in table_js
    assert 'coll: "collection"' in providers_ui
    assert '{ key: "coll", label: "Collections" }' in providers_ui
    assert "collections and scrobbling" in providers_ui

    providers = {
        "plex": ("providers/auth/_auth_PLEX.py", "assets/auth/auth.plex.js", "plex_lib_collection"),
        "emby": ("providers/auth/_auth_EMBY.py", "assets/auth/auth.emby.js", "emby_lib_collection"),
        "jellyfin": ("providers/auth/_auth_JELLYFIN.py", "assets/auth/auth.jellyfin.js", "jfy_lib_collection"),
        "kodi": ("providers/auth/_auth_KODI.py", "assets/auth/auth.kodi.js", "kodi_lib_collection"),
    }

    for provider, (html_path, js_path, select_id) in providers.items():
        html = _read(html_path)
        js = _read(js_path)
        if provider in {"plex", "emby", "jellyfin"}:
            assert "max-width:980px" not in html
            assert "width:100%;max-width:none" in html
        assert f'id="{select_id}"' in html
        assert select_id in js
        assert 'label: "Collections"' in js
        assert "collection?.libraries" in js
        assert ".collection" in js


def test_media_server_whitelist_defaults_include_collection(monkeypatch) -> None:
    from providers.sync.emby import _utils as emby_utils
    from providers.sync.jellyfin import _utils as jellyfin_utils
    from providers.sync.plex import _utils as plex_utils

    saved: list[dict] = []
    monkeypatch.setattr(emby_utils, "save_config", lambda cfg: saved.append(cfg))
    monkeypatch.setattr(jellyfin_utils, "save_config", lambda cfg: saved.append(cfg))

    plex_cfg: dict = {"plex": {}}
    assert plex_utils.ensure_whitelist_defaults(plex_cfg, persist=False) is True
    assert plex_cfg["plex"]["collection"]["libraries"] == []

    emby_cfg: dict = {"emby": {}}
    emby_utils.ensure_whitelist_defaults(emby_cfg)
    assert emby_cfg["emby"]["collection"]["libraries"] == []

    jellyfin_cfg: dict = {"jellyfin": {}}
    jellyfin_utils.ensure_whitelist_defaults(jellyfin_cfg)
    assert jellyfin_cfg["jellyfin"]["collection"]["libraries"] == []

# tests/test_provider_app_version.py
# CrossWatch - Installed app version in provider request metadata
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from importlib import import_module

import pytest

from cw_platform import app_version as version_source
from cw_platform.anime_mapping import simkl_catalog
from providers.scrobble.simkl import sink
from providers.sync.simkl import _common
from providers.sync.mdblist import _progress

pytestmark = pytest.mark.usefixtures("config_base")


@pytest.mark.parametrize("stamped,environment,expected", [
    ("v0.12.4", "v0.11.3-dev", "v0.12.4"),
    ("v0.12.4-dev", "v0.11.3-dev", "v0.12.4-dev"),
    (None, "v0.12.4", "v0.12.4"),
    (None, "v0.0.0", None),
    (None, "", None),
])
def test_provider_metadata_uses_the_installed_version(tmp_path, monkeypatch, stamped, environment, expected):
    version_file = tmp_path / "VERSION"
    if stamped is not None:
        version_file.write_text(stamped, encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", version_file)
    monkeypatch.setenv("APP_VERSION", environment)
    expected = expected or version_source.FALLBACK_VERSION

    assert _common.simkl_api_params("test-client")["app-version"] == expected
    assert simkl_catalog._app_version() == expected
    cfg = {"runtime": {"version": "v0.11.3-dev", "build_date": "2026-09-21"}}
    for module in (
        sink,
        import_module("providers.scrobble.trakt.sink"),
        import_module("providers.scrobble.mdblist.sink"),
        import_module("providers.webhooks.plex"),
        import_module("providers.webhooks.jellyfin"),
        import_module("providers.webhooks.emby"),
    ):
        assert module._app_meta(cfg) == {"app_version": expected, "app_date": "2026-09-21"}, module.__name__
        assert module._app_meta({}) == {"app_version": expected}, module.__name__
    body, reason = _progress._movie_body({"ids": {"imdb": "tt0137523"}}, 20.0)
    assert reason is None
    assert body is not None and body["app_version"] == expected
    body, reason = _progress._episode_body({"show_ids": {"imdb": "tt0944947"}, "season": 2, "episode": 1}, 20.0)
    assert reason is None
    assert body is not None and body["app_version"] == expected


def test_catalog_redirect_sends_installed_version(tmp_path, monkeypatch):
    from cw_platform import simkl_http

    version_file = tmp_path / "VERSION"
    version_file.write_text("v0.12.4", encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", version_file)
    monkeypatch.setenv("APP_VERSION", "v0.11.3-dev")
    monkeypatch.setenv("CW_SIMKL_UA", "SimklClient/3")
    monkeypatch.setattr(simkl_catalog, "client_id", lambda *_args: "test-client")
    calls = []

    def request(sender, method, url, **kwargs):
        calls.append((method, url, kwargs))
        raise RuntimeError("stop before network access")

    monkeypatch.setattr(simkl_http, "paced_request", request)
    with pytest.raises(simkl_catalog.SimklCatalogError):
        simkl_catalog._request("/redirect", to="simkl", tvdb="361753")
    assert len(calls) == 1
    assert calls[0][2]["params"]["app-version"] == "v0.12.4"
    assert calls[0][2]["headers"]["User-Agent"] == "SimklClient/3"

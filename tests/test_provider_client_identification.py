# /tests/test_provider_client_identification.py
# CrossWatch - Provider client identification and User-Agent overrides
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from pathlib import Path
import subprocess
import sys

import pytest

from cw_platform import app_version as version_source
from cw_platform.app_version import app_version, user_agent

pytestmark = pytest.mark.usefixtures("config_base")


def test_provider_user_agent_override_precedence(tmp_path, monkeypatch):
    stamp = tmp_path / "VERSION"
    stamp.write_text("v0.12.4", encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", stamp)
    monkeypatch.setenv("APP_VERSION", "v0.11.3-dev")
    monkeypatch.delenv("CW_UA", raising=False)
    monkeypatch.delenv("CW_PLEX_UA", raising=False)

    assert user_agent("Plex", override_env="CW_PLEX_UA") == "CrossWatch/v0.12.4 (Plex)"
    monkeypatch.setenv("CW_UA", "CustomClient/2")
    assert user_agent("Plex", override_env="CW_PLEX_UA") == "CustomClient/2"
    monkeypatch.setenv("CW_PLEX_UA", "CustomPlex/3")
    assert user_agent("Plex", override_env="CW_PLEX_UA") == "CustomPlex/3"
    assert app_version() == "v0.12.4"


@pytest.mark.parametrize("custom_agent", ["", "CustomClient/2", "ProviderClient/3"])
def test_provider_identification_after_restart(tmp_path, custom_agent):
    stamp = tmp_path / "VERSION"
    stamp.write_text("v0.12.4", encoding="utf-8")
    env = {
        key: value for key, value in os.environ.items()
        if not (key.startswith("CW_") and (key.endswith("_UA") or key.endswith("_VERSION")))
    }
    env.update(APP_VERSION="v0.11.3-dev", CONFIG_BASE=str(tmp_path / "config"))
    if custom_agent:
        env["CW_UA"] = custom_agent
    if custom_agent == "ProviderClient/3":
        env["CW_UA"] = "GlobalClient/2"
        for provider in (
            "ANILIST", "BINGEBASE", "EMBY", "FLICKLIST", "FLOPPY", "KODI", "MDBLIST",
            "NUVIO", "PLEX", "PUBLICMETADB", "PUNCHPLAY", "SCROB", "SIMKL", "STREMIO",
            "TAUTULLI", "TRACEARR", "TRAKT", "TMDB", "JELLYFIN",
        ):
            env[f"CW_{provider}_UA"] = custom_agent
    script = r'''
import importlib
from pathlib import Path
import sys
import requests
from cw_platform import app_version as version_source

version_source.VERSION_FILE = Path(sys.argv[1])
expected_agent = sys.argv[2]

def no_network(*args, **kwargs):
    raise AssertionError("Unexpected network access")

requests.sessions.Session.request = no_network
targets = [
    ("providers.auth._auth_" + provider, "UA")
    for provider in (
        "ANILIST", "BINGEBASE", "EMBY", "FLICKLIST", "FLOPPY", "KODI", "MDBLIST",
        "NUVIO", "PLEX", "PUBLICMETADB", "PUNCHPLAY", "SCROB", "SIMKL", "STREMIO",
        "TAUTULLI", "TRACEARR",
    )
]
targets += [
    ("providers.scrobble." + provider + ".sink", "APP_AGENT")
    for provider in ("bingebase", "flicklist", "mdblist", "punchplay", "scrob", "simkl", "trakt")
]
targets += [
    ("providers.sync._mod_ANILIST", "UA"),
    ("providers.sync._mod_EMBY", "_DEF_UA"),
    ("providers.sync._mod_JELLYFIN", "_DEF_UA"),
    ("providers.sync.mdblist._auth", "UA"),
    ("providers.sync.emby._utils", "UA"),
    ("providers.sync.jellyfin._auth_http", "UA"),
]
for module_name, attribute in targets:
    value = getattr(importlib.import_module(module_name), attribute)
    if expected_agent:
        assert value == expected_agent, (module_name, value)
    else:
        assert value.startswith("CrossWatch/v0.12.4"), (module_name, value)

from cw_platform.anime_mapping.updater import UA as updater_agent
assert updater_agent == ("GlobalClient/2" if expected_agent == "ProviderClient/3" else expected_agent or "CrossWatch/v0.12.4 (AnimeMapping)")

from providers.metadata._meta_TMDB import TmdbProvider
assert TmdbProvider.UA == (expected_agent or "CrossWatch/v0.12.4")
from providers.sync.plex._common import plex_headers
from providers.sync.jellyfin._auth_http import auth_headers
from providers.auth._auth_EMBY import _headers
from providers.scrobble.jellyfin.watch import _hdr as jellyfin_headers
from providers.scrobble.emby.watch import _hdr as emby_headers
assert plex_headers("test-token", client_id="test-device")["X-Plex-Version"] == "v0.12.4"
for headers in (auth_headers("test-token", "test-device"), _headers("test-token", "test-device"), jellyfin_headers("test-token", {}), emby_headers("test-token", {})):
    assert 'Version="v0.12.4"' in headers["Authorization"]

from providers.sync.simkl._common import simkl_user_agent
from providers.scrobble.trakt.sink import _hdr
assert simkl_user_agent() == (expected_agent or "CrossWatch/v0.12.4 (SIMKL)")
assert _hdr({"trakt": {"client_id": "test-client"}})["trakt-api-version"] == "2"
from providers.sync._mod_JELLYFIN import __VERSION__
assert __VERSION__ == "2.7"
from providers.sync._mod_common import build_session
for provider in ("PLEX", "SIMKL", "TMDB", "MDBLIST", "NUVIO", "STREMIO", "SCROB", "TRACEARR", "TAUTULLI"):
    session = build_session(provider, None)
    agent = session.headers["User-Agent"]
    if expected_agent:
        assert agent == expected_agent, (provider, agent)
    else:
        assert agent.startswith("CrossWatch/v0.12.4 ("), (provider, agent)
    session.close()
from providers.scrobble.flicklist.sink import FlickListSink
sink = FlickListSink(cfg_provider=lambda: {})
assert sink.session.headers["User-Agent"] == (expected_agent or "CrossWatch/v0.12.4 (FlickListWatcher)")
sink.session.close()
from api import probesAPI
assert probesAPI._provider_headers("emby")["User-Agent"] == (expected_agent or "CrossWatch/v0.12.4 (Emby)")
assert probesAPI._provider_headers("simkl")["User-Agent"] == (expected_agent or "CrossWatch/v0.12.4")
from services.authPlex import _headers as plex_login_headers
assert plex_login_headers("test-device")["X-Plex-Version"] == "v0.12.4"
assert plex_login_headers("test-device")["User-Agent"] == (expected_agent or "CrossWatch/v0.12.4 (Plex)")
'''
    result = subprocess.run(
        [sys.executable, "-c", script, str(stamp), custom_agent],
        cwd=Path(__file__).resolve().parents[1], env=env,
        capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 0, result.stdout + result.stderr


@pytest.mark.parametrize("present", [True, False])
def test_version_stamp_is_read_once(tmp_path, monkeypatch, present):
    stamp = tmp_path / "VERSION"
    if present:
        stamp.write_text("v0.12.4", encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", stamp)
    monkeypatch.setenv("APP_VERSION", "v0.11.3-dev")
    original = Path.read_text
    reads = []

    def read(path, *args, **kwargs):
        reads.append(path)
        return original(path, *args, **kwargs)

    monkeypatch.setattr(Path, "read_text", read)
    for _ in range(100):
        assert app_version() == ("v0.12.4" if present else "v0.11.3-dev")
    assert reads == [stamp]


@pytest.mark.parametrize("global_agent,provider_agent,expected", [
    ("", "", "CrossWatch/v0.12.4 (TraktAuth)"),
    ("GlobalClient/2", "", "GlobalClient/2"),
    ("GlobalClient/2", "TraktClient/3", "TraktClient/3"),
])
def test_trakt_device_auth_identification(tmp_path, monkeypatch, global_agent, provider_agent, expected):
    from unittest.mock import Mock
    from providers.auth import _auth_TRAKT as auth

    stamp = tmp_path / "VERSION"
    stamp.write_text("v0.12.4", encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", stamp)
    monkeypatch.setenv("APP_VERSION", "v0.11.3-dev")
    monkeypatch.setenv("CW_UA", global_agent)
    monkeypatch.setenv("CW_TRAKT_UA", provider_agent)
    response = Mock(status_code=200)
    response.json.return_value = {"user_code": "test-code", "device_code": "test-device"}
    post = Mock(return_value=response)
    monkeypatch.setattr(auth.requests, "post", post)
    monkeypatch.setattr(auth, "_save_config", Mock())

    assert auth.PROVIDER.start({"trakt": {"client_id": "test-client"}})["ok"]
    post.assert_called_once()
    assert post.call_args.args[0] == auth.OAUTH_DEVICE_CODE
    assert post.call_args.kwargs["headers"]["User-Agent"] == expected
    assert post.call_args.kwargs["headers"]["trakt-api-version"] == "2"


@pytest.mark.parametrize("provider", ["jellyfin", "emby"])
@pytest.mark.parametrize("custom_agent", ["", "MediaClient/3"])
def test_media_users_api_identification(tmp_path, monkeypatch, provider, custom_agent):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import authenticationAPI as auth
    from unittest.mock import Mock

    stamp = tmp_path / "VERSION"
    stamp.write_text("v0.12.4", encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", stamp)
    monkeypatch.delenv("CW_UA", raising=False)
    monkeypatch.setenv(f"CW_{provider.upper()}_UA", custom_agent)
    monkeypatch.setattr(auth, "load_config", lambda: {
        provider: {"server": "https://media.example", "access_token": "test-token"},
    })
    response = Mock(ok=True, status_code=200)
    response.json.return_value = [{"Id": "1", "Name": "One"}, {"Id": "2", "Name": "Two"}]
    get = Mock(return_value=response)
    monkeypatch.setattr(auth.requests, "get", get)
    app = FastAPI()
    auth.register_auth(app)
    result = TestClient(app).get(f"/api/{provider}/users")
    assert result.status_code == 200, result.text
    assert get.call_count >= 1
    for call in get.call_args_list:
        headers = call.kwargs["headers"]
        assert headers["User-Agent"] == (custom_agent or "CrossWatch/v0.12.4")
        assert 'Version="v0.12.4"' in headers["Authorization"]


def test_plex_probe_identification(tmp_path, monkeypatch):
    from api import probesAPI as probes

    stamp = tmp_path / "VERSION"
    stamp.write_text("v0.12.4", encoding="utf-8")
    monkeypatch.setattr(version_source, "VERSION_FILE", stamp)
    monkeypatch.setenv("CW_PLEX_UA", "PlexClient/3")
    calls = []

    def get(url, headers):
        calls.append(headers)
        return 200, b"{}"

    monkeypatch.setattr(probes, "_http_get", get)
    monkeypatch.setattr(probes, "_consume_bust", lambda *_: 0)
    monkeypatch.setattr(probes, "_account_fetch", lambda provider, cfg, fetch: fetch())
    assert probes._probe_plex_detail.__wrapped__({"plex": {"account_token": "test-token"}}, max_age_sec=0)[0]
    assert calls[0]["User-Agent"] == "PlexClient/3"
    assert calls[0]["X-Plex-Version"] == "v0.12.4"

# CrossWatch test scripts
from __future__ import annotations

import copy
import pathlib
from typing import Any

from fastapi import FastAPI
from fastapi.testclient import TestClient

import api.probesAPI as probes
import providers.auth._auth_SCROB as scrob_auth


PROFILE = "SCROB-P01"


def _cfg(instance: dict[str, Any]) -> dict[str, Any]:
    return {
        "scrob": {
            "server_url": "",
            "api_key": "",
            "username": "",
            "password": "",
            "access_token": "",
            "expires_at": 0,
            "instances": {PROFILE: dict(instance)},
        },
        "pairs": [],
    }


CONNECTED = {
    "server_url": "http://host:7330",
    "api_key": "KEY",
    "username": "frank",
    "password": "pw",
    "api_prefix": "/api/proxy",
    "access_token": "TOKEN",
    "expires_at": 4102444800,
    "label": "SC-Frank",
}


class _Ok:
    status_code = 200

    def json(self) -> dict[str, Any]:
        return {"id": 42, "username": "frank"}


def _client(monkeypatch, state: dict[str, Any]) -> TestClient:
    monkeypatch.setattr(scrob_auth.ScrobClient, "request", lambda self, method, path, **kw: _Ok())
    probes.STATUS_CACHE["data"] = None
    probes.STATUS_CACHE["ts"] = 0
    probes.PROBE_DETAIL_CACHE.clear()
    app = FastAPI()
    probes.register_probes(app, lambda: copy.deepcopy(state["cfg"]))
    return TestClient(app)


def _connected(client: TestClient, url: str) -> Any:
    providers = client.get(url).json().get("providers") or {}
    return (providers.get("SCROB") or {}).get("connected")


def test_fresh_status_sees_a_profile_connected_after_the_cache_was_warmed_empty(monkeypatch) -> None:
    state = {"cfg": _cfg({})}
    client = _client(monkeypatch, state)

    assert _connected(client, "/api/status") is False

    state["cfg"] = _cfg(CONNECTED)

    assert _connected(client, "/api/status") is False
    assert _connected(client, "/api/status?fresh=1") is True


def test_status_cache_still_serves_repeat_calls_without_the_fresh_flag(monkeypatch) -> None:
    state = {"cfg": _cfg(CONNECTED)}
    client = _client(monkeypatch, state)

    assert _connected(client, "/api/status") is True

    state["cfg"] = _cfg({})

    assert _connected(client, "/api/status") is True
    assert _connected(client, "/api/status?fresh=1") is False


def test_a_fresh_refresh_does_not_reprobe_providers_that_did_not_change(monkeypatch) -> None:
    state = {"cfg": _cfg(CONNECTED)}
    state["cfg"]["plex"] = {"account_token": "PT"}
    state["cfg"]["trakt"] = {"client_id": "CID", "access_token": "TT", "expires_at": 4102444800}
    client = _client(monkeypatch, state)
    probes._USERINFO_CACHE.clear()

    calls: list[str] = []
    monkeypatch.setattr(
        probes,
        "_http_get",
        lambda url, headers=None, timeout=None, **kw: (calls.append(url), (200, '{"user_id":1,"username":"u","ids":{"slug":"u"}}'))[1],
    )
    monkeypatch.setattr(scrob_auth.ScrobClient, "request", lambda self, method, path, **kw: (calls.append(self.url_for(path, prefix=kw.get("prefix"))), _Ok())[1])

    client.get("/api/status")
    calls.clear()

    client.get("/api/status?fresh=1")
    assert calls == []

    changed = dict(CONNECTED)
    changed["api_key"] = "ROTATED"
    state["cfg"] = _cfg(changed)
    state["cfg"]["plex"] = {"account_token": "PT"}
    state["cfg"]["trakt"] = {"client_id": "CID", "access_token": "TT", "expires_at": 4102444800}

    assert _connected(client, "/api/status?fresh=1") is True
    assert calls
    assert all("7330" in url for url in calls)


def test_client_sends_the_fresh_flag_only_on_a_forced_status_refresh() -> None:
    api_js = pathlib.Path("assets/helpers/api.js").read_text(encoding="utf-8")
    lines = api_js.split("\n")
    start = next(i for i, line in enumerate(lines) if "const qs = [];" in line)
    body = "\n".join(line.strip() for line in lines[start - 1:start + 4])

    assert "if (force) qs.push('fresh=1');" in body

    core_js = pathlib.Path("assets/helpers/core.js").read_text(encoding="utf-8")
    assert 'requestJSON(force ? "/api/status?fresh=1" : "/api/status", {}, 15000)' in core_js


def test_provider_cards_prefer_live_status_over_persisted_status() -> None:
    providers_ui = pathlib.Path("assets/helpers/providers-ui.js").read_text(encoding="utf-8")
    assert "window.__CW_PROVIDER_STATUS__?.providers || window._statusCache?.providers || window.loadStatusCache?.()?.providers" in providers_ui

    core_js = pathlib.Path("assets/helpers/core.js").read_text(encoding="utf-8")
    assert "window._statusCache = payload;" in core_js

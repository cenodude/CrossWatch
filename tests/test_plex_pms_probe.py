from __future__ import annotations

from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

SHARED_SERVER = "https://192-0-2-10.example.plex.direct:32400"
OLD_SERVER = "https://198-51-100-20.example.plex.direct:32400"


class _Resp:
    def __init__(self, status_code: int) -> None:
        self.status_code = status_code
        self.ok = 200 <= status_code < 300


def _client(monkeypatch, cfg: dict[str, Any], responses: dict[str, int], seen: list[tuple[str, str]]):
    import api.authenticationAPI as auth
    from providers.sync.plex import _utils as plex_utils

    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda _cfg: None)
    monkeypatch.setattr(plex_utils, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(plex_utils, "_build_session", lambda token, verify: {"token": token, "verify": verify})

    def _try_get(session, base, path, timeout=5.0):
        token = session["token"]
        seen.append((token, path))
        status = responses.get(token)
        return None if status is None else _Resp(status)

    monkeypatch.setattr(plex_utils, "_try_get", _try_get)

    app = FastAPI()
    auth.register_auth(app)
    return TestClient(app)


def _plex_cfg(**overrides: Any) -> dict[str, Any]:
    block = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS",
        "server_url": SHARED_SERVER,
        "machine_id": "mid-1",
    }
    block.update(overrides)
    return {"plex": block}


def test_probe_succeeds_on_shared_server_when_only_pms_token_works(monkeypatch) -> None:
    seen: list[tuple[str, str]] = []
    cfg = _plex_cfg()
    client = _client(monkeypatch, cfg, {"ACCOUNT": 401, "PMS": 200}, seen)

    body = client.get("/api/plex/pms/probe").json()

    assert body["reachable"] is True
    assert body["status"] == 200
    assert body["token_source"] == "pms_token"
    assert body["server_url"] == SHARED_SERVER
    assert seen[0][0] == "PMS"


def test_probe_falls_back_to_account_token(monkeypatch) -> None:
    seen: list[tuple[str, str]] = []
    cfg = _plex_cfg()
    client = _client(monkeypatch, cfg, {"ACCOUNT": 200, "PMS": 401}, seen)

    body = client.get("/api/plex/pms/probe").json()

    assert body["reachable"] is True
    assert body["token_source"] == "account_token"
    assert {token for token, _ in seen} == {"PMS", "ACCOUNT"}


def test_probe_reports_unreachable_when_both_tokens_fail(monkeypatch) -> None:
    seen: list[tuple[str, str]] = []
    cfg = _plex_cfg()
    client = _client(monkeypatch, cfg, {"ACCOUNT": 401, "PMS": 401}, seen)

    body = client.get("/api/plex/pms/probe").json()

    assert body["reachable"] is False
    assert body["status"] == 401
    assert "token_source" not in body


def test_probe_works_without_pms_token(monkeypatch) -> None:
    seen: list[tuple[str, str]] = []
    cfg = _plex_cfg(pms_token="")
    client = _client(monkeypatch, cfg, {"ACCOUNT": 200}, seen)

    body = client.get("/api/plex/pms/probe").json()

    assert body["reachable"] is True
    assert body["token_source"] == "account_token"
    assert [token for token, _ in seen] == ["ACCOUNT"]


@pytest.mark.parametrize("bound", [OLD_SERVER, "", None])
def test_stale_pms_token_is_invalidated_when_server_url_changed(monkeypatch, bound) -> None:
    from providers.sync.plex import _utils as plex_utils

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "OLD-PMS",
        "machine_id": "old-mid",
        "server_url": SHARED_SERVER,
        "client_id": "cid",
    }
    if bound is not None:
        block["pms_token_server"] = bound
    cfg = {"plex": block}

    monkeypatch.setattr(plex_utils, "load_config", lambda: cfg)
    monkeypatch.setattr(plex_utils, "save_config", lambda _cfg: None)
    monkeypatch.setattr(plex_utils, "fetch_cloud_user_info", lambda _t: None)
    monkeypatch.setattr(plex_utils, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(
        plex_utils, "_resource_token_for_connection", lambda *a, **k: ("new-mid", "NEW-PMS")
    )
    monkeypatch.setattr(plex_utils, "_try_get", lambda *a, **k: None)

    plex_utils.inspect_and_persist(cfg)

    assert block["pms_token"] == "NEW-PMS"
    assert block["machine_id"] == "new-mid"
    assert block["pms_token_server"] == SHARED_SERVER


@pytest.mark.parametrize("outcome", ["raises", "returns_nothing"])
def test_stale_token_is_kept_when_rediscovery_fails(monkeypatch, outcome) -> None:
    from providers.sync.plex import _utils as plex_utils

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "OLD-PMS",
        "machine_id": "old-mid",
        "server_url": SHARED_SERVER,
        "client_id": "cid",
    }
    cfg = {"plex": block}

    def _fail(*a: Any, **k: Any):
        if outcome == "raises":
            raise RuntimeError("plex.tv unreachable")
        return (None, None)

    monkeypatch.setattr(plex_utils, "load_config", lambda: cfg)
    monkeypatch.setattr(plex_utils, "save_config", lambda _cfg: None)
    monkeypatch.setattr(plex_utils, "fetch_cloud_user_info", lambda _t: None)
    monkeypatch.setattr(plex_utils, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(plex_utils, "_resource_token_for_connection", _fail)
    monkeypatch.setattr(plex_utils, "_try_get", lambda *a, **k: None)

    plex_utils.inspect_and_persist(cfg)

    assert block["pms_token"] == "OLD-PMS"
    assert block["machine_id"] == "old-mid"
    assert "pms_token_server" not in block


def test_pms_token_kept_when_bound_to_current_server(monkeypatch) -> None:
    from providers.sync.plex import _utils as plex_utils

    block: dict[str, Any] = {
        "account_token": "ACCOUNT",
        "pms_token": "PMS",
        "machine_id": "mid-1",
        "server_url": SHARED_SERVER,
        "pms_token_server": SHARED_SERVER,
        "client_id": "cid",
    }
    cfg = {"plex": block}

    calls: list[int] = []

    def _never(*a: Any, **k: Any):
        calls.append(1)
        return ("other-mid", "OTHER-PMS")

    monkeypatch.setattr(plex_utils, "load_config", lambda: cfg)
    monkeypatch.setattr(plex_utils, "save_config", lambda _cfg: None)
    monkeypatch.setattr(plex_utils, "fetch_cloud_user_info", lambda _t: None)
    monkeypatch.setattr(plex_utils, "_resolve_verify_from_cfg", lambda *a, **k: True)
    monkeypatch.setattr(plex_utils, "_resource_token_for_connection", _never)
    monkeypatch.setattr(plex_utils, "_try_get", lambda *a, **k: None)

    plex_utils.inspect_and_persist(cfg)

    assert block["pms_token"] == "PMS"
    assert block["machine_id"] == "mid-1"
    assert not calls

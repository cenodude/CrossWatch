# tests/test_movieparadise_auth.py
# CrossWatch - Movie Paradise device code login, rotating refresh and connection status
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from dataclasses import dataclass, field
from typing import Any

import pytest
import requests
from urllib3.exceptions import MaxRetryError, NewConnectionError


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    text: str = "{}"
    headers: dict[str, str] = field(default_factory=dict)

    def json(self) -> Any:
        if self.payload is None:
            raise ValueError("no json")
        return self.payload


class FakePost:
    def __init__(self, responses: list[Any]) -> None:
        self.responses = list(responses)
        self.calls: list[dict[str, Any]] = []

    def __call__(self, url: str, **kwargs: Any) -> ResponseStub:
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError(f"unexpected post {url}")
        nxt = self.responses.pop(0)
        if isinstance(nxt, BaseException):
            raise nxt
        return nxt


@pytest.fixture()
def mp(monkeypatch: pytest.MonkeyPatch):
    import providers.auth._auth_MOVIEPARADISE as mod

    store: dict[str, Any] = {"cfg": {"movieparadise": {}}}
    monkeypatch.setattr(mod, "_load_full_cfg", lambda: store["cfg"])
    monkeypatch.setattr(mod, "_save_full_cfg", lambda cfg: store.__setitem__("cfg", cfg))
    monkeypatch.setattr(mod, "_store_identity", lambda block, token: None)
    monkeypatch.delenv(mod.CLIENT_ID_ENV, raising=False)
    mod._LAST_POLL.clear()
    yield mod, store
    mod._LAST_POLL.clear()


def _pending(store: dict[str, Any], **extra: Any) -> None:
    store["cfg"]["movieparadise"]["_pending_device"] = {
        "device_code": "raw-device-code",
        "user_code": "N4NW-KCMV",
        "interval": 5,
        "expires_at": int(time.time()) + 600,
        **extra,
    }


def _connected(store: dict[str, Any], *, expires_in: int = 900) -> None:
    store["cfg"]["movieparadise"].update(
        {"access_token": "mp_at_old", "refresh_token": "mp_rt_old", "expires_at": int(time.time()) + expires_in}
    )


def test_manifest_is_device_code_without_credential_fields() -> None:
    from providers.auth._auth_MOVIEPARADISE import MovieParadiseAuth

    manifest = MovieParadiseAuth().manifest()

    assert manifest.name == "MOVIEPARADISE"
    assert manifest.label == "Movie Paradise"
    assert manifest.flow == "device_code"
    assert manifest.fields == []
    assert manifest.verify_url == "https://movieparadise.app/oauth/device"


def test_client_id_env_override(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, _ = mp
    assert mod.app_client_id() == mod.DEFAULT_CLIENT_ID
    monkeypatch.setenv(mod.CLIENT_ID_ENV, "mpc_custom")
    assert mod.app_client_id() == "mpc_custom"


def test_device_start_posts_form_with_resource_and_keeps_device_code_server_side(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    post = FakePost([
        ResponseStub(200, {
            "device_code": "raw-device-code",
            "user_code": "N4NW-KCMV",
            "verification_uri": "https://movieparadise.app/oauth/device",
            "expires_in": 600,
            "interval": 5,
        })
    ])
    monkeypatch.setattr(mod.requests, "post", post)

    res = mod.start_device_code(store["cfg"], instance_id="default")

    assert res["ok"] is True
    assert res["user_code"] == "N4NW-KCMV"
    assert "device_code" not in res
    call = post.calls[0]
    assert call["url"] == mod.DEVICE_CODE_URL
    assert "json" not in call
    assert call["data"]["client_id"] == mod.DEFAULT_CLIENT_ID
    assert call["data"]["resource"] == "https://platform.movieparadise.app"
    assert set(call["data"]["scope"].split()) == {"library:read", "library:write", "scrobble:write", "offline_access"}
    pending = store["cfg"]["movieparadise"]["_pending_device"]
    assert pending["device_code"] == "raw-device-code"
    assert pending["interval"] == 5


def test_device_start_rate_limited_reports_retry_after(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    monkeypatch.setattr(mod.requests, "post", FakePost([
        ResponseStub(429, {"error": "temporarily_unavailable"}, headers={"Retry-After": "17"})
    ]))

    res = mod.start_device_code(store["cfg"], instance_id="default")

    assert res == {"ok": False, "error": "rate_limited", "retry_after": 17, "rate_limit_source": "provider", "instance": "default"}
    assert "_pending_device" not in store["cfg"]["movieparadise"]


def test_poll_pending_keeps_pending(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _pending(store)
    post = FakePost([ResponseStub(400, {"error": "authorization_pending"})])
    monkeypatch.setattr(mod.requests, "post", post)

    res = mod.poll_device_code(store["cfg"], instance_id="default")

    assert res["status"] == "authorization_pending"
    assert post.calls[0]["data"] == {
        "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
        "device_code": "raw-device-code",
        "client_id": mod.DEFAULT_CLIENT_ID,
    }
    assert store["cfg"]["movieparadise"]["_pending_device"]["device_code"] == "raw-device-code"


def test_poll_slow_down_adds_five_seconds_for_good(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _pending(store)
    monkeypatch.setattr(mod.requests, "post", FakePost([ResponseStub(400, {"error": "slow_down"})]))

    res = mod.poll_device_code(store["cfg"], instance_id="default")

    assert res["status"] == "slow_down"
    assert res["interval"] == 10
    assert store["cfg"]["movieparadise"]["_pending_device"]["interval"] == 10


def test_poll_inside_interval_does_not_reach_provider(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _pending(store)
    post = FakePost([ResponseStub(400, {"error": "authorization_pending"})])
    monkeypatch.setattr(mod.requests, "post", post)

    first = mod.poll_device_code(store["cfg"], instance_id="default")
    second = mod.poll_device_code(store["cfg"], instance_id="default")

    assert first["status"] == second["status"] == "authorization_pending"
    assert len(post.calls) == 1


@pytest.mark.parametrize("error", ["access_denied", "expired_token"])
def test_poll_terminal_errors_clear_pending(mp, monkeypatch: pytest.MonkeyPatch, error: str) -> None:
    mod, store = mp
    _pending(store)
    monkeypatch.setattr(mod.requests, "post", FakePost([ResponseStub(400, {"error": error})]))

    res = mod.poll_device_code(store["cfg"], instance_id="default")

    assert res["status"] == error
    assert "_pending_device" not in store["cfg"]["movieparadise"]


def test_poll_server_error_is_a_lost_answer(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _pending(store)
    monkeypatch.setattr(mod.requests, "post", FakePost([ResponseStub(503, {"error": "temporarily_unavailable"})]))

    res = mod.poll_device_code(store["cfg"], instance_id="default")

    assert res["status"] == "answer_lost"
    assert "_pending_device" not in store["cfg"]["movieparadise"]


def test_poll_success_stores_tokens_and_clears_pending(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _pending(store)
    monkeypatch.setattr(mod.requests, "post", FakePost([
        ResponseStub(200, {
            "access_token": "mp_at_1",
            "refresh_token": "mp_rt_1",
            "expires_in": 899,
            "scope": "library:read library:write scrobble:write",
            "token_type": "Bearer",
        })
    ]))

    res = mod.poll_device_code(store["cfg"], instance_id="default")

    block = store["cfg"]["movieparadise"]
    assert res["ok"] is True and res["status"] == "authorized"
    assert block["access_token"] == "mp_at_1"
    assert block["refresh_token"] == "mp_rt_1"
    assert block["expires_at"] >= int(time.time()) + 890
    assert "_pending_device" not in block
    assert mod.is_configured(block)


def test_fresh_token_is_not_refreshed(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store)
    monkeypatch.setattr(mod.requests, "post", FakePost([]))

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["status"] == "fresh"


def test_refresh_rotates_both_tokens_with_resource(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    post = FakePost([ResponseStub(200, {"access_token": "mp_at_new", "refresh_token": "mp_rt_new", "expires_in": 899})])
    monkeypatch.setattr(mod.requests, "post", post)

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["ok"] is True
    assert post.calls[0]["data"] == {
        "grant_type": "refresh_token",
        "refresh_token": "mp_rt_old",
        "client_id": mod.DEFAULT_CLIENT_ID,
        "resource": "https://platform.movieparadise.app",
    }
    block = store["cfg"]["movieparadise"]
    assert (block["access_token"], block["refresh_token"]) == ("mp_at_new", "mp_rt_new")


def test_refresh_invalid_grant_flags_reconnect(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    monkeypatch.setattr(mod.requests, "post", FakePost([ResponseStub(400, {"error": "invalid_grant"})]))

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["reconnect_required"] is True
    assert mod.needs_reconnect(store["cfg"]["movieparadise"])
    assert mod.status_for_block(store["cfg"]["movieparadise"])["reconnect_required"] is True
    monkeypatch.setattr(mod.requests, "post", FakePost([]))
    assert mod.refresh_token(store["cfg"], instance_id="default")["status"] == "reconnect_required"


def test_refresh_server_error_never_resends_the_refresh_token(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    post = FakePost([ResponseStub(503, {"error": "temporarily_unavailable"})])
    monkeypatch.setattr(mod.requests, "post", post)

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["status"] == "reconnect_required"
    assert len(post.calls) == 1
    assert mod.needs_reconnect(store["cfg"]["movieparadise"])


def test_refresh_rate_limit_keeps_connection(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    monkeypatch.setattr(mod.requests, "post", FakePost([ResponseStub(429, {"error": "temporarily_unavailable"}, headers={"Retry-After": "9"})]))

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["status"] == "rate_limited"
    assert res["retry_after"] == 9
    assert not mod.needs_reconnect(store["cfg"]["movieparadise"])


def test_refresh_read_timeout_is_a_lost_answer(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    monkeypatch.setattr(mod.requests, "post", FakePost([requests.ReadTimeout("read timed out")]))

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["status"] == "reconnect_required"
    assert mod.needs_reconnect(store["cfg"]["movieparadise"])


def test_refresh_unreachable_host_keeps_connection(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    refused = requests.ConnectionError(MaxRetryError(None, "/oauth/token", NewConnectionError(None, "refused")))
    monkeypatch.setattr(mod.requests, "post", FakePost([refused]))

    res = mod.refresh_token(store["cfg"], instance_id="default")

    assert res["status"] == "network_error"
    assert not mod.needs_reconnect(store["cfg"]["movieparadise"])


def test_refresh_is_single_flight(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store, expires_in=30)
    gate = threading.Event()
    calls: list[dict[str, Any]] = []

    def slow_post(url: str, **kwargs: Any) -> ResponseStub:
        calls.append(kwargs)
        gate.wait(2)
        return ResponseStub(200, {"access_token": "mp_at_new", "refresh_token": "mp_rt_new", "expires_in": 899})

    monkeypatch.setattr(mod.requests, "post", slow_post)
    results: list[dict[str, Any]] = []
    threads = [threading.Thread(target=lambda: results.append(mod.refresh_token(store["cfg"], instance_id="default"))) for _ in range(3)]
    for t in threads:
        t.start()
    time.sleep(0.1)
    gate.set()
    for t in threads:
        t.join(5)

    assert len(calls) == 1
    assert all(r["ok"] for r in results)


def test_status_reports_pending_only_with_a_user_code(mp) -> None:
    mod, _ = mp
    assert "pending" not in mod.status_for_block({"_pending_device": {"user_code": ""}})
    status = mod.status_for_block({"_pending_device": {"user_code": "N4NW-KCMV", "device_code": "secret", "expires_at": 1}})
    assert status["pending"]["user_code"] == "N4NW-KCMV"
    assert "device_code" not in status["pending"]


def test_disconnect_clears_tokens_and_revokes(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    _connected(store)
    revoked: list[Any] = []
    monkeypatch.setattr(mod, "cleanup_remote", lambda fn, remote: revoked.append(remote))

    mod.PROVIDER.disconnect(store["cfg"], instance_id="default")

    block = store["cfg"]["movieparadise"]
    assert block["access_token"] == "" and block["refresh_token"] == ""
    assert revoked and revoked[0]["movieparadise"]["refresh_token"] == "mp_rt_old"


def test_revoke_posts_refresh_token_form(mp, monkeypatch: pytest.MonkeyPatch) -> None:
    mod, store = mp
    post = FakePost([ResponseStub(200, {})])
    monkeypatch.setattr(mod.requests, "post", post)

    res = mod.revoke_token({"movieparadise": {"refresh_token": "mp_rt_old"}})

    assert res["ok"] is True
    assert post.calls[0]["url"] == mod.REVOKE_URL
    assert post.calls[0]["data"] == {"token": "mp_rt_old", "client_id": mod.DEFAULT_CLIENT_ID}


def test_default_config_and_redaction() -> None:
    from cw_platform.config_base import DEFAULT_CFG, redact_config

    block = DEFAULT_CFG["movieparadise"]
    assert "_pending_device" not in block
    assert block["scope"] == ""
    redacted = redact_config({"movieparadise": {
        "access_token": "mp_at_x",
        "refresh_token": "mp_rt_x",
        "username": "pazzie",
        "_pending_device": {"device_code": "d"},
    }})["movieparadise"]
    assert redacted["access_token"] != "mp_at_x"
    assert redacted["refresh_token"] != "mp_rt_x"
    assert redacted["_pending_device"] != {"device_code": "d"}
    assert redacted["username"] == "pazzie"


def test_runtime_backend_and_registry() -> None:
    from cw_platform.modules_registry import MODULES, provider_display_label
    from providers.auth import runtime

    assert MODULES["AUTH"]["_auth_MOVIEPARADISE"] == "providers.auth._auth_MOVIEPARADISE"
    assert runtime.is_configured("movieparadise", {"access_token": "x"}) is True
    assert provider_display_label("MOVIEPARADISE") == "Movie Paradise"


def test_movie_paradise_stays_hidden_until_sync_is_ready() -> None:
    import re
    from pathlib import Path

    from providers.auth.registry import auth_providers_html

    html = auth_providers_html()
    meta = Path("assets/helpers/provider-meta.js").read_text("utf-8")

    assert 'id="sec-movieparadise"' not in html
    assert 'id="sec-trakt"' in html
    assert re.search(r"MOVIEPARADISE: \{[^\n]*hidden: true \}", meta)
    assert "info && !info.hidden ? info.authSectionId" in meta

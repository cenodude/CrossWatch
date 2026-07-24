# CrossWatch test scripts
from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

import pytest


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    text: str = "{}"

    def json(self) -> Any:
        return self.payload if self.payload is not None else {}


class FakeSession:
    def __init__(self, *, posts: list[ResponseStub] | None = None, requests: list[ResponseStub] | None = None) -> None:
        self.posts = list(posts or [])
        self.requests = list(requests or [])
        self.post_calls: list[dict[str, Any]] = []
        self.request_calls: list[dict[str, Any]] = []

    def post(self, url: str, **kwargs: Any) -> ResponseStub:
        self.post_calls.append({"url": url, **kwargs})
        if not self.posts:
            raise AssertionError(f"unexpected post {url}")
        return self.posts.pop(0)

    def request(self, method: str, url: str, **kwargs: Any) -> ResponseStub:
        self.request_calls.append({"method": method, "url": url, **kwargs})
        if not self.requests:
            raise AssertionError(f"unexpected request {method} {url}")
        return self.requests.pop(0)


def _cfg() -> dict[str, Any]:
    return {
        "nuvio": {
            "base_url": "https://api.nuvio.tv",
            "access_token": "",
            "refresh_token": "",
            "expires_at": 0,
            "profile_id": "",
            "profile_name": "",
        }
    }


def test_nuvio_manifest_has_tv_login_without_password_fields() -> None:
    from providers.auth._auth_NUVIO import NuvioAuth

    manifest = NuvioAuth().manifest()

    assert manifest.name == "NUVIO"
    assert manifest.flow == "tv_login"
    assert manifest.actions == {"start": True, "finish": True, "refresh": True, "disconnect": True}
    assert manifest.fields == []


def test_device_login_start_and_poll_parse_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    session = FakeSession(
        posts=[
            ResponseStub(payload={"access_token": "anon-access", "refresh_token": "anon-refresh", "expires_in": 600}),
            ResponseStub(payload=[{"code": "ABCD", "qr_content": "https://nuvio.tv/login/ABCD", "expires_at_millis": 1_900_000_000_000, "poll_interval_seconds": 2}]),
            ResponseStub(payload=[{"status": "approved", "poll_interval_seconds": 2}]),
        ]
    )
    client = common.NuvioClient(cfg, session=session)

    started = client.start_tv_login_session(cfg)
    polled = client.poll_tv_login_session(cfg)

    assert started["ok"] is True
    assert started["code"] == "ABCD"
    assert started["login_url"] == "https://nuvio.tv/login/ABCD"
    assert "access_token" not in started
    assert "refresh_token" not in started
    assert cfg["nuvio"]["_pending_tv_login"]["code"] == "ABCD"
    assert cfg["nuvio"]["_pending_tv_login"]["caller_access_token"] == "anon-access"
    assert polled["ok"] is True
    assert polled["status"] == "approved"


def test_device_login_start_parses_current_nuvio_web_contract(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    session = FakeSession(
        posts=[
            ResponseStub(payload={"access_token": "anon-access", "refresh_token": "anon-refresh", "expires_in": 600}),
            ResponseStub(payload=[{"code": "WXYZ", "web_url": "https://nuvio.tv/tv-login?code=WXYZ", "expires_at": "2030-03-17T17:46:40Z", "poll_interval_seconds": 3}]),
        ]
    )

    started = common.NuvioClient(cfg, session=session).start_tv_login_session(cfg)

    assert started["ok"] is True
    assert started["code"] == "WXYZ"
    assert started["login_url"] == "https://nuvio.tv/tv-login?code=WXYZ"
    assert started["expires_at"] == 1_900_000_000
    assert session.post_calls[1]["json"]["p_device_name"] == "CrossWatch"


def test_device_login_uses_shared_public_key(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    session = FakeSession(
        posts=[
            ResponseStub(payload={"access_token": "anon-access", "refresh_token": "anon-refresh", "expires_in": 600}),
            ResponseStub(payload=[{"code": "ABCD", "qr_content": "https://nuvio.tv/login/ABCD", "expires_at_millis": 1_900_000_000_000, "poll_interval_seconds": 3}]),
        ]
    )

    started = common.NuvioClient(cfg, session=session).start_tv_login_session(cfg)

    assert started["ok"] is True
    assert session.post_calls[0]["headers"]["apikey"] == common.SHARED_PUBLIC_CLIENT_KEY
    assert session.post_calls[0]["headers"]["Authorization"] == f"Bearer {common.SHARED_PUBLIC_CLIENT_KEY}"
    assert session.post_calls[1]["headers"]["apikey"] == common.SHARED_PUBLIC_CLIENT_KEY


def test_device_login_rejects_numeric_second_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    session = FakeSession(
        posts=[
            ResponseStub(payload={"access_token": "anon-access", "refresh_token": "anon-refresh", "expires_in": 600}),
            ResponseStub(payload=[{"code": "ABCD", "qr_content": "https://nuvio.tv/login/ABCD", "expires_at_millis": common.now() + 300, "poll_interval_seconds": 3}]),
        ]
    )

    with pytest.raises(common.NuvioInvalidResponse, match="invalid_expiry"):
        common.NuvioClient(cfg, session=session).start_tv_login_session(cfg)


def test_device_login_exchange_persists_tokens_and_clears_temporary_state(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    cfg["nuvio"]["_pending_tv_login"] = {
        "code": "ABCD",
        "device_nonce": "nonce",
        "expires_at": 1_900_000_000,
        "poll_interval_seconds": 2,
        "caller_access_token": "anon-access",
        "caller_refresh_token": "anon-refresh",
    }
    cfg["nuvio"]["_pending_tv_caller"] = {"access_token": "anon-access"}
    session = FakeSession(
        posts=[ResponseStub(payload={"access_token": "real-access", "refresh_token": "real-refresh", "expires_in": 3600})],
        requests=[ResponseStub(payload=[{"profile_index": 1, "name": "Main", "uses_primary_addons": True, "uses_primary_plugins": False}])],
    )

    result = common.NuvioClient(cfg, session=session).exchange_tv_login_session(cfg)

    assert result["ok"] is True
    assert result["profiles"][0]["profile_id"] == 1
    assert cfg["nuvio"]["profile_id"] == 1
    assert cfg["nuvio"]["profile_name"] == "Main"
    assert cfg["nuvio"]["access_token"] == "real-access"
    assert cfg["nuvio"]["refresh_token"] == "real-refresh"
    assert "_pending_tv_login" not in cfg["nuvio"]
    assert "_pending_tv_caller" not in cfg["nuvio"]
    assert "real-access" not in json.dumps(result)
    assert "real-refresh" not in json.dumps(result)


def test_device_login_exchange_uses_nuvio_default_profile_when_no_synced_profiles(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    cfg["nuvio"]["_pending_tv_login"] = {
        "code": "ABCD",
        "device_nonce": "nonce",
        "expires_at": 1_900_000_000,
        "poll_interval_seconds": 2,
        "caller_access_token": "anon-access",
    }
    session = FakeSession(
        posts=[ResponseStub(payload={"access_token": "real-access", "refresh_token": "real-refresh", "expires_in": 3600})],
        requests=[ResponseStub(payload=[])],
    )

    result = common.NuvioClient(cfg, session=session).exchange_tv_login_session(cfg)

    assert result["profiles"] == [common.DEFAULT_PROFILE]
    assert cfg["nuvio"]["profile_id"] == 1
    assert cfg["nuvio"]["profile_name"] == "Profile 1"


def test_refresh_token_rotates_credentials_and_failed_refresh_is_clear(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    cfg = _cfg()
    cfg["nuvio"]["refresh_token"] = "old-refresh"
    monkeypatch.setattr(common, "load_config", lambda: cfg)
    monkeypatch.setattr(common, "save_config", lambda _cfg: None)

    ok_session = FakeSession(posts=[ResponseStub(payload={"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600})])
    ok = common.NuvioClient(cfg, session=ok_session).refresh_token(cfg)

    assert ok["ok"] is True
    assert cfg["nuvio"]["access_token"] == "new-access"
    assert cfg["nuvio"]["refresh_token"] == "new-refresh"

    cfg["nuvio"]["refresh_token"] = "bad-refresh"
    bad_session = FakeSession(posts=[ResponseStub(status_code=401, payload={"error": "invalid"})])
    bad = common.NuvioClient(cfg, session=bad_session).refresh_token(cfg)

    assert bad == {"ok": False, "status": "invalid_refresh", "instance": "default"}


def test_request_retries_once_after_auth_failure_refresh(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    cfg = _cfg()
    cfg["nuvio"].update({"access_token": "old-access", "refresh_token": "old-refresh", "expires_at": common.now() + 3600, "profile_id": 1})
    monkeypatch.setattr(common, "load_config", lambda: cfg)
    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    session = FakeSession(
        posts=[ResponseStub(payload={"access_token": "new-access", "refresh_token": "new-refresh", "expires_in": 3600})],
        requests=[ResponseStub(status_code=401, payload={"message": "jwt expired"}), ResponseStub(payload=[{"profile_index": 1, "name": "Main"}])],
    )

    data = common.NuvioClient(cfg, session=session).pull_profiles(cfg)

    assert data[0]["profile_id"] == 1
    assert len(session.request_calls) == 2
    assert len(session.post_calls) == 1
    assert cfg["nuvio"]["access_token"] == "new-access"


def test_profiles_select_and_disconnect_are_instance_specific(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    cfg["nuvio"].update({"access_token": "access", "refresh_token": "refresh", "profile_id": 1, "profile_name": "Main", "instances": {"kid": {"profile_id": 2, "profile_name": "Kid", "_pending_tv_caller": {"access_token": "anon"}}}})

    client = common.NuvioClient(cfg, instance_id="kid")
    client.pull_profiles = lambda *_args, **_kwargs: [{"profile_id": 1, "name": "Main"}, {"profile_id": 2, "name": "Kid"}]  # type: ignore[method-assign]
    selected = client.select_profile(cfg, 2)

    assert selected["name"] == "Kid"
    assert cfg["nuvio"]["profile_id"] == 1
    assert cfg["nuvio"]["instances"]["kid"]["profile_id"] == 2
    assert common.normalize_profiles([{"profile_index": "3", "name": "Guest", "usesPrimaryAddons": True, "usesPrimaryPlugins": True}])[0]["profile_id"] == 3
    assert common.normalize_profiles([{"id": "4", "name": "Only"}])[0]["profile_id"] == 4
    with pytest.raises(common.NuvioInvalidResponse):
        common.normalize_profiles([{"profileId": "5", "name": "Unsupported"}])
    with pytest.raises(common.NuvioInvalidResponse):
        common.normalize_profiles({"profile_index": 1})

    client.disconnect(cfg)
    kid = cfg["nuvio"]["instances"]["kid"]
    assert kid["access_token"] == ""
    assert kid["refresh_token"] == ""
    assert kid["expires_at"] == 0
    assert kid["profile_id"] == ""
    assert kid["profile_name"] == ""
    assert "_pending_tv_caller" not in kid


def test_config_redaction_masks_nuvio_secrets() -> None:
    from cw_platform.config_base import redact_config

    redacted = redact_config(
        {
            "nuvio": {
                "access_token": "access-secret",
                "refresh_token": "refresh-secret",
                "_pending_tv_login": {"code": "ABCD", "device_nonce": "nonce"},
                "profile_id": 1,
                "profile_name": "Main",
            }
        }
    )

    mask = "\u2022" * 8
    assert redacted["nuvio"]["access_token"] == mask
    assert redacted["nuvio"]["refresh_token"] == mask
    assert redacted["nuvio"]["_pending_tv_login"] == mask
    assert redacted["nuvio"]["profile_name"] == "Main"


def test_config_normalization_removes_nuvio_public_client_key() -> None:
    from cw_platform.config_base import _normalize_nuvio

    cfg = {"nuvio": {"base_url": "https://api.nuvio.tv", "public_client_key": "user-key", "instances": {"kid": {"public_client_key": "kid-key"}}}}

    _normalize_nuvio(cfg)

    assert "public_client_key" not in cfg["nuvio"]
    assert "public_client_key" not in cfg["nuvio"]["instances"]["kid"]

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


def test_nuvio_profile_dropdown_custom_select_has_room() -> None:
    from providers.auth import _auth_NUVIO as common

    markup = common.html()

    assert "#sec-nuvio .nuvio-profile-row>div{width:min(320px,100%)}" in markup
    assert "#sec-nuvio .nuvio-profile-row .cw-icon-select{width:320px;min-width:280px;max-width:100%}" in markup


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


class DiscoveryResponse:
    def __init__(self, payload: Any, *, status_code: int = 200, url: str = "https://nuvio.example.com/.well-known/nuvio") -> None:
        self.status_code = status_code
        self.url = url
        self._body = payload if isinstance(payload, bytes) else json.dumps(payload).encode("utf-8")

    def iter_content(self, size: int):
        for i in range(0, len(self._body), size):
            yield self._body[i:i + size]

    def close(self) -> None:
        pass


class DiscoverySession(FakeSession):
    def __init__(self, response: DiscoveryResponse, **kwargs: Any) -> None:
        super().__init__(**kwargs)
        self.response = response
        self.get_calls: list[str] = []

    def get(self, url: str, **kwargs: Any) -> DiscoveryResponse:
        self.get_calls.append(url)
        return self.response


def _discovery_doc(**overrides: Any) -> dict[str, Any]:
    doc: dict[str, Any] = {
        "version": 1,
        "service": "nuvio",
        "self_hosted": True,
        "backend_url": "https://nuvio.example.com",
        "publishable_key": "self-hosted-key",
        "capabilities": {"email_password_auth": True, "tv_login": True},
    }
    doc.update(overrides)
    return doc


def test_discovery_url_normalization() -> None:
    from providers.auth import _auth_NUVIO as common

    assert common.discovery_url("nuvio.example.com") == "https://nuvio.example.com/.well-known/nuvio"
    assert common.discovery_url("http://host:8080/") == "http://host:8080/.well-known/nuvio"
    assert common.discovery_url("https://h/base/.well-known/nuvio?x=1") == "https://h/base/.well-known/nuvio"
    for bad in ("", "ftp://host", "https://user:pw@host"):
        with pytest.raises(common.NuvioDiscoveryError, match="invalid_url"):
            common.discovery_url(bad)


@pytest.mark.parametrize(
    "overrides,reason",
    [
        ({"version": 2}, "unsupported_version"),
        ({"service": "other"}, "wrong_service"),
        ({"self_hosted": False}, "not_self_hosted"),
        ({"publishable_key": " "}, "missing_configuration"),
        ({"backend_url": "https://h/?q=1"}, "missing_configuration"),
        ({"capabilities": {"email_password_auth": True, "tv_login": False}}, "tv_login_unsupported"),
    ],
)
def test_discovery_document_validation(overrides: dict[str, Any], reason: str) -> None:
    from providers.auth import _auth_NUVIO as common

    with pytest.raises(common.NuvioDiscoveryError, match=reason):
        common.parse_discovery(_discovery_doc(**overrides))


def test_discovery_rejects_official_server_and_bad_responses() -> None:
    from providers.auth import _auth_NUVIO as common

    with pytest.raises(common.NuvioDiscoveryError, match="official_server"):
        common.discover_server("https://api.nuvio.tv", session=DiscoverySession(DiscoveryResponse(_discovery_doc())))
    big = DiscoveryResponse(b"x" * (common.DISCOVERY_MAX_BYTES + 10))
    with pytest.raises(common.NuvioDiscoveryError, match="response_too_large"):
        common.discover_server("nuvio.example.com", session=DiscoverySession(big))
    with pytest.raises(common.NuvioDiscoveryError, match="http_error"):
        common.discover_server("nuvio.example.com", session=DiscoverySession(DiscoveryResponse({}, status_code=404)))
    downgraded = DiscoveryResponse(_discovery_doc(), url="http://nuvio.example.com/.well-known/nuvio")
    with pytest.raises(common.NuvioDiscoveryError, match="connection_failed"):
        common.discover_server("nuvio.example.com", session=DiscoverySession(downgraded))


def test_self_hosted_login_uses_discovered_backend_key_and_tv_login_page(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    discovery = DiscoverySession(DiscoveryResponse(_discovery_doc()))

    res = common.configure_server(cfg, server_mode="self_hosted", base_url="nuvio.example.com", session=discovery)

    assert res["ok"] is True
    assert discovery.get_calls == ["https://nuvio.example.com/.well-known/nuvio"]
    assert cfg["nuvio"]["server_mode"] == "self_hosted"
    assert cfg["nuvio"]["base_url"] == "https://nuvio.example.com"
    assert cfg["nuvio"]["publishable_key"] == "self-hosted-key"
    assert common.status_for_block(cfg["nuvio"])["base_url"] == "https://nuvio.example.com"

    session = FakeSession(
        posts=[
            ResponseStub(payload={"access_token": "anon-access", "refresh_token": "anon-refresh", "expires_in": 600}),
            ResponseStub(payload=[{"code": "WXYZ", "web_url": "https://nuvio.example.com/tv-login?code=WXYZ", "expires_at": "2030-03-17T17:46:40Z", "poll_interval_seconds": 3}]),
        ]
    )
    started = common.NuvioClient(cfg, session=session).start_tv_login_session(cfg, redirect_base_url="https://nuvio.tv/tv-login")

    assert started["login_url"] == "https://nuvio.example.com/tv-login?code=WXYZ"
    assert session.post_calls[0]["url"] == "https://nuvio.example.com/auth/v1/signup"
    assert session.post_calls[0]["headers"]["apikey"] == "self-hosted-key"
    assert session.post_calls[1]["url"] == "https://nuvio.example.com/rest/v1/rpc/start_tv_login_session"
    assert session.post_calls[1]["json"]["p_redirect_base_url"] == "https://nuvio.example.com/tv-login"
    assert session.post_calls[1]["headers"]["apikey"] == "self-hosted-key"


def test_cloud_login_keeps_official_tv_login_page(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    session = FakeSession(
        posts=[
            ResponseStub(payload={"access_token": "anon-access", "refresh_token": "anon-refresh", "expires_in": 600}),
            ResponseStub(payload=[{"code": "WXYZ", "web_url": "https://nuvio.tv/tv-login?code=WXYZ", "expires_at": "2030-03-17T17:46:40Z", "poll_interval_seconds": 3}]),
        ]
    )

    common.NuvioClient(cfg, session=session).start_tv_login_session(cfg)

    assert session.post_calls[1]["json"]["p_redirect_base_url"] == common.TV_LOGIN_WEB_BASE_URL
    assert session.post_calls[1]["headers"]["apikey"] == common.SHARED_PUBLIC_CLIENT_KEY


def test_server_change_requires_disconnect_and_switch_back_to_cloud(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: None)
    cfg = _cfg()
    cfg["nuvio"].update({"access_token": "a", "refresh_token": "r", "profile_id": 1})

    with pytest.raises(common.NuvioDiscoveryError, match="disconnect_first"):
        common.configure_server(cfg, server_mode="self_hosted", base_url="nuvio.example.com", session=DiscoverySession(DiscoveryResponse(_discovery_doc())))
    assert common.configure_server(cfg, server_mode="cloud")["changed"] is False
    assert cfg["nuvio"]["access_token"] == "a"

    common.clear_oauth(cfg["nuvio"])
    common.configure_server(cfg, server_mode="self_hosted", base_url="nuvio.example.com", session=DiscoverySession(DiscoveryResponse(_discovery_doc())))
    res = common.configure_server(cfg, server_mode="cloud")

    assert res["changed"] is True
    assert cfg["nuvio"]["server_mode"] == "cloud"
    assert cfg["nuvio"]["base_url"] == common.API_BASE
    assert "publishable_key" not in cfg["nuvio"]
    assert common.app_public_client_key(cfg["nuvio"]) == common.SHARED_PUBLIC_CLIENT_KEY


def test_self_hosted_without_key_is_not_configured() -> None:
    from providers.auth import _auth_NUVIO as common

    block = {"server_mode": "self_hosted", "base_url": "https://nuvio.example.com", "access_token": "a", "refresh_token": "r", "profile_id": 1}

    assert common.is_configured(block) is False
    assert common.is_configured({**block, "publishable_key": "k"}) is True


def test_config_normalization_keeps_publishable_key_only_for_self_hosted() -> None:
    from cw_platform.config_base import _normalize_nuvio

    cfg = {
        "nuvio": {
            "publishable_key": "stray",
            "instances": {"kid": {"server_mode": "self_hosted", "base_url": "https://nuvio.example.com/", "publishable_key": " k "}},
        }
    }

    _normalize_nuvio(cfg)

    assert cfg["nuvio"]["server_mode"] == "cloud"
    assert "publishable_key" not in cfg["nuvio"]
    kid = cfg["nuvio"]["instances"]["kid"]
    assert kid["server_mode"] == "self_hosted"
    assert kid["base_url"] == "https://nuvio.example.com"
    assert kid["publishable_key"] == "k"

# CrossWatch test scripts
from __future__ import annotations

from typing import Any
from unittest.mock import Mock

import pytest

from providers.auth import _auth_TRAKT as auth


def _response(status: int, body: dict[str, Any] | None = None) -> Mock:
    response = Mock(status_code=status, text="", headers={})
    response.json.return_value = body or {}
    return response


@pytest.fixture
def post(monkeypatch):
    mock = Mock()
    monkeypatch.setattr(auth.requests, "post", mock)
    monkeypatch.setattr(auth, "_save_config", Mock(return_value=True))
    monkeypatch.delenv(auth.CLIENT_ID_ENV, raising=False)
    return mock


@pytest.mark.parametrize("block,expected", [
    ({}, "pin"),
    ({"client_id": "own"}, "pin"),
    ({"client_id": "own", "client_secret": "secret"}, "app"),
    ({"client_id": "own", "access_token": "token"}, "app"),
    ({"auth_method": "pin", "client_secret": "secret", "access_token": "token"}, "pin"),
    ({"auth_method": "app"}, "app"),
])
def test_normalize_auth_method(block, expected):
    assert auth.normalize_auth_method(None, block) == expected


def test_pin_flow_uses_built_in_client_without_secret(post):
    cfg: dict[str, Any] = {"trakt": {}}
    post.return_value = _response(200, {"user_code": "CODE", "device_code": "device", "expires_in": 600, "interval": 6})

    assert auth.PROVIDER.start(cfg, method="pin")["ok"]
    assert post.call_args.kwargs["json"] == {"client_id": auth.DEFAULT_CLIENT_ID}
    assert cfg["trakt"]["_pending_device"]["auth_method"] == "pin"

    post.return_value = _response(200, {"access_token": "access", "refresh_token": "refresh", "expires_in": 604800})
    assert auth.PROVIDER.finish(cfg)["ok"]

    sent = post.call_args.kwargs
    assert sent["json"] == {"code": "device", "client_id": auth.DEFAULT_CLIENT_ID}
    assert sent["headers"]["trakt-api-key"] == auth.DEFAULT_CLIENT_ID
    block = cfg["trakt"]
    assert block["client_id"] == auth.DEFAULT_CLIENT_ID
    assert block["auth_method"] == "pin"
    assert block["client_secret"] == ""
    assert block["access_token"] == "access"
    assert "_pending_device" not in block


def test_pin_flow_replaces_own_app_credentials(post):
    cfg: dict[str, Any] = {"trakt": {"client_id": "own", "client_secret": "secret", "access_token": "old"}}
    post.return_value = _response(200, {"user_code": "CODE", "device_code": "device"})
    assert auth.PROVIDER.start(cfg, method="pin")["ok"]
    assert post.call_args.kwargs["json"] == {"client_id": auth.DEFAULT_CLIENT_ID}

    post.return_value = _response(200, {"access_token": "access", "refresh_token": "refresh", "expires_in": 604800})
    assert auth.PROVIDER.finish(cfg)["ok"]
    assert "client_secret" not in post.call_args.kwargs["json"]
    assert cfg["trakt"]["client_id"] == auth.DEFAULT_CLIENT_ID
    assert cfg["trakt"]["client_secret"] == ""


def test_own_app_flow_keeps_client_secret(post):
    cfg: dict[str, Any] = {"trakt": {"client_id": "own", "client_secret": "secret"}}
    post.return_value = _response(200, {"user_code": "CODE", "device_code": "device"})
    assert auth.PROVIDER.start(cfg)["ok"]
    assert post.call_args.kwargs["json"] == {"client_id": "own"}

    post.return_value = _response(200, {"access_token": "access", "refresh_token": "refresh", "expires_in": 604800})
    assert auth.PROVIDER.finish(cfg)["ok"]
    assert post.call_args.kwargs["json"] == {"code": "device", "client_id": "own", "client_secret": "secret"}
    assert cfg["trakt"]["auth_method"] == "app"
    assert cfg["trakt"]["client_secret"] == "secret"


def test_instances_keep_their_own_method(post):
    cfg: dict[str, Any] = {"trakt": {
        "client_id": "own", "client_secret": "secret", "access_token": "old", "refresh_token": "own-refresh", "expires_at": 1,
        "instances": {"second": {}},
    }}
    post.return_value = _response(200, {"user_code": "CODE", "device_code": "device"})
    assert auth.PROVIDER.start(cfg, instance_id="second")["ok"]
    assert post.call_args.kwargs["json"] == {"client_id": auth.DEFAULT_CLIENT_ID}

    post.return_value = _response(200, {"access_token": "pin-access", "refresh_token": "pin-refresh", "expires_in": 604800})
    assert auth.PROVIDER.finish(cfg, instance_id="second")["ok"]
    second = cfg["trakt"]["instances"]["second"]
    assert second["auth_method"] == "pin"
    assert second["client_id"] == auth.DEFAULT_CLIENT_ID
    assert cfg["trakt"]["client_id"] == "own"
    assert cfg["trakt"]["client_secret"] == "secret"
    assert "auth_method" not in cfg["trakt"]

    post.return_value = _response(200, {"access_token": "new", "refresh_token": "new-refresh", "expires_in": 604800})
    assert auth.PROVIDER.refresh(cfg)["ok"]
    assert post.call_args.kwargs["json"] == {
        "refresh_token": "own-refresh", "client_id": "own", "grant_type": "refresh_token", "client_secret": "secret",
    }

    second["expires_at"] = 1
    assert auth.PROVIDER.refresh(cfg, instance_id="second")["ok"]
    assert post.call_args.kwargs["json"] == {
        "refresh_token": "pin-refresh", "client_id": auth.DEFAULT_CLIENT_ID, "grant_type": "refresh_token",
    }


def test_own_app_flow_requires_client_id(post):
    assert auth.PROVIDER.start({"trakt": {}}, method="app") == {"ok": False, "error": "missing_client_id"}
    post.assert_not_called()


def test_pending_device_poll_stays_pending(post):
    cfg: dict[str, Any] = {"trakt": {"_pending_device": {"device_code": "device", "expires_at": auth._now() + 600, "auth_method": "pin"}}}
    response = _response(400)
    response.json.side_effect = ValueError("empty")
    post.return_value = response
    assert auth.PROVIDER.finish(cfg) == {"ok": False, "status": "authorization_pending"}


def test_pin_refresh_omits_client_secret_and_rotates_token(post):
    cfg: dict[str, Any] = {"trakt": {
        "client_id": auth.DEFAULT_CLIENT_ID, "auth_method": "pin", "access_token": "old",
        "refresh_token": "old-refresh", "expires_at": 1, "auth_error": "reconnect_required",
    }}
    post.return_value = _response(200, {"access_token": "new", "refresh_token": "new-refresh", "expires_in": 604800})

    assert auth.PROVIDER.refresh(cfg)["ok"]
    assert post.call_args.kwargs["json"] == {
        "refresh_token": "old-refresh", "client_id": auth.DEFAULT_CLIENT_ID, "grant_type": "refresh_token",
    }
    assert cfg["trakt"]["refresh_token"] == "new-refresh"
    assert "auth_error" not in cfg["trakt"]


def test_own_app_refresh_sends_client_secret(post):
    cfg: dict[str, Any] = {"trakt": {
        "client_id": "own", "client_secret": "secret", "access_token": "old", "refresh_token": "old-refresh", "expires_at": 1,
    }}
    post.return_value = _response(200, {"access_token": "new", "refresh_token": "new-refresh", "expires_in": 604800})
    assert auth.PROVIDER.refresh(cfg)["ok"]
    assert post.call_args.kwargs["json"]["client_secret"] == "secret"


def test_invalid_grant_marks_reconnect_required(post):
    cfg: dict[str, Any] = {"trakt": {
        "client_id": auth.DEFAULT_CLIENT_ID, "auth_method": "pin", "access_token": "old", "refresh_token": "dead", "expires_at": 1,
    }}
    post.return_value = _response(400, {"error": "invalid_grant", "error_description": "session not found"})
    result = auth.PROVIDER.refresh(cfg)
    assert result["ok"] is False
    assert result["reconnect_required"] is True
    assert cfg["trakt"]["auth_error"] == "reconnect_required"


def test_other_refresh_errors_do_not_force_reconnect(post):
    cfg: dict[str, Any] = {"trakt": {
        "client_id": auth.DEFAULT_CLIENT_ID, "auth_method": "pin", "access_token": "old", "refresh_token": "refresh", "expires_at": 1,
    }}
    post.return_value = _response(503, {})
    result = auth.PROVIDER.refresh(cfg)
    assert result["ok"] is False
    assert "reconnect_required" not in result
    assert "auth_error" not in cfg["trakt"]


def test_refresh_all_only_refreshes_profiles_close_to_expiry(post, monkeypatch):
    now = auth._now()
    cfg: dict[str, Any] = {"trakt": {
        "client_id": "own", "client_secret": "secret", "access_token": "a", "refresh_token": "soon", "expires_at": now + 3600,
        "instances": {
            "later": {"client_id": auth.DEFAULT_CLIENT_ID, "auth_method": "pin", "access_token": "b", "refresh_token": "later", "expires_at": now + 5 * 86400},
            "dead": {"client_id": auth.DEFAULT_CLIENT_ID, "auth_method": "pin", "access_token": "c", "refresh_token": "dead", "expires_at": now + 60, "auth_error": "reconnect_required"},
            "legacy": {"client_id": "own", "client_secret": "secret", "access_token": "d", "refresh_token": "legacy", "expires_at": 0},
            "empty": {},
        },
    }}
    monkeypatch.setattr(auth, "_load_config", lambda: cfg)
    post.return_value = _response(200, {"access_token": "new", "refresh_token": "new-refresh", "expires_in": 604800})

    assert auth.refresh_all() == {"default": "ok"}
    post.assert_called_once()
    assert post.call_args.kwargs["json"]["refresh_token"] == "soon"
    assert cfg["trakt"]["access_token"] == "new"
    assert cfg["trakt"]["instances"]["later"]["access_token"] == "b"


def test_refresh_keeps_the_short_margin_for_on_demand_callers(post):
    cfg: dict[str, Any] = {"trakt": {
        "client_id": "own", "client_secret": "secret", "access_token": "a", "refresh_token": "r", "expires_at": auth._now() + 3600,
    }}
    assert auth.PROVIDER.refresh(cfg)["status"] == "fresh"
    post.assert_not_called()

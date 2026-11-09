# /tests/test_auth_reset_env.py
# CrossWatch - Startup administrator authentication recovery tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest

from cw_platform import config_base as cb


@pytest.fixture
def startup(config_base, monkeypatch):
    monkeypatch.delenv("CW_RESET_AUTH_ONCE", raising=False)
    import crosswatch

    monkeypatch.setattr(crosswatch, "CONFIG_DIR", config_base)
    monkeypatch.setattr(crosswatch, "_AUTH_RESET_ENV_APPLIED", False)
    cb.invalidate_config_cache()
    yield crosswatch
    cb.invalidate_config_cache()


def _seed_config(config_base):
    cfg = {
        "app_auth": {
            "enabled": True,
            "username": "synthetic-admin",
            "reset_required": False,
            "password": {
                "scheme": "pbkdf2_sha256",
                "iterations": 260000,
                "salt": "synthetic-salt",
                "hash": "synthetic-password-hash",
            },
            "session": {"token_hash": "synthetic-session-hash", "expires_at": 4102444800},
            "sessions": [{"token_hash": "synthetic-session-hash", "expires_at": 4102444800}],
            "api_tokens": [{"token_hash": "synthetic-api-token-hash"}],
            "last_login_at": 1700000000,
        },
        "trakt": {"client_id": "synthetic-client", "access_token": "synthetic-provider-token"},
    }
    (config_base / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    return cb.load_config()


def test_auth_reset_persists_and_preserves_other_configuration(startup, config_base, monkeypatch, capsys):
    from api.appAuthAPI import credentials_configured, setup_lock_required

    before = _seed_config(config_base)
    assert credentials_configured(before)
    assert before["app_auth"]["sessions"]
    assert before["app_auth"]["api_tokens"]
    monkeypatch.setenv("CW_RESET_AUTH_ONCE", "1")

    startup._apply_auth_reset_env_once()

    monkeypatch.delenv("CW_RESET_AUTH_ONCE")
    cb.invalidate_config_cache()
    after = cb.load_config()
    auth = after["app_auth"]
    assert not credentials_configured(after)
    assert setup_lock_required(after)
    assert auth["username"] == ""
    assert auth["password"]["salt"] == ""
    assert auth["password"]["hash"] == ""
    assert auth["reset_required"] is True
    assert auth["session"] == {"token_hash": "", "expires_at": 0}
    assert auth["sessions"] == []
    assert auth["api_tokens"] == []
    assert auth["last_login_at"] == 0
    assert {k: v for k, v in after.items() if k != "app_auth"} == {
        k: v for k, v in before.items() if k != "app_auth"
    }
    assert "app authentication was reset" in capsys.readouterr().out


@pytest.mark.parametrize("value", [None, "0", "false"])
def test_auth_reset_requires_enabled_environment_flag(startup, config_base, monkeypatch, value):
    _seed_config(config_base)
    path = config_base / "config.json"
    before = path.read_bytes()
    if value is not None:
        monkeypatch.setenv("CW_RESET_AUTH_ONCE", value)

    startup._apply_auth_reset_env_once()

    assert path.read_bytes() == before


def test_auth_reset_skips_missing_configuration(startup, config_base, monkeypatch, capsys):
    monkeypatch.setenv("CW_RESET_AUTH_ONCE", "1")
    path = config_base / "config.json"
    path.unlink(missing_ok=True)

    startup._apply_auth_reset_env_once()

    assert not path.exists()
    assert "no existing config.json found" in capsys.readouterr().out


def test_auth_reset_only_runs_once(startup, config_base, monkeypatch):
    _seed_config(config_base)
    monkeypatch.setenv("CW_RESET_AUTH_ONCE", "1")
    startup._apply_auth_reset_env_once()
    _seed_config(config_base)
    path = config_base / "config.json"
    before = path.read_bytes()

    startup._apply_auth_reset_env_once()

    assert path.read_bytes() == before

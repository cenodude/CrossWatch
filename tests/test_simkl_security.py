# tests/test_simkl_security.py
# CrossWatch - SIMKL cache encryption, fingerprints and safe OAuth errors
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import subprocess
import sys
import time

import pytest

from cw_platform import config_base as cb
from cw_platform import simkl_http as http
from providers.auth import _auth_SIMKL as auth
from providers.sync.simkl import _common


@pytest.fixture(autouse=True)
def isolated(config_base, monkeypatch):
    monkeypatch.delenv("CW_CONFIG_KEY", raising=False)
    monkeypatch.delenv("CROSSWATCH_CONFIG_KEY", raising=False)
    monkeypatch.setattr(_common, "STATE_DIR", config_base / "state")
    monkeypatch.setattr(_common, "_QUOTA_BLOCKS", {})
    monkeypatch.setattr(http, "_USER_IDENTITIES", {})
    monkeypatch.setattr(http, "_OUTCOMES", {})
    return config_base


def test_token_keys_are_shared_across_request_cache_and_quota_paths():
    key = http.token_key("simkl_at_test")
    assert len(key) == 64
    assert key == http.token_key(" BeArEr simkl_at_test ")
    assert key == _common.account_cache_key("simkl_at_test")
    assert key == _common.quota_account_key({"access_token": "simkl_at_test"})
    assert key == _common.quota_account_key({"refresh_token": "simkl_at_test"})
    assert key != http.token_key("different-token")


def test_token_fingerprint_uses_installation_key(monkeypatch):
    monkeypatch.setenv("CW_CONFIG_KEY", "test-installation-a")
    first = http.token_key("same-token")
    monkeypatch.setenv("CW_CONFIG_KEY", "test-installation-b")
    assert http.token_key("same-token") != first


def test_empty_token_does_not_create_config_key(isolated):
    assert http.token_key("") == ""
    assert http.token_key("   ") == ""
    assert not (isolated / ".cw_master_key").exists()


def test_missing_key_does_not_fall_back_to_an_unkeyed_fingerprint(monkeypatch):
    monkeypatch.setattr(cb, "_load_config_key", lambda **kwargs: None)
    with pytest.raises(RuntimeError, match="config encryption key"):
        http.token_key("test-token")


def test_cached_account_survives_a_new_process():
    key = http.token_key("test-token")
    data = {"account": {"id": 42, "type": "pro"}, "user": {"name": "test-user"}}
    _common.account_settings_store(key, data)
    script = (
        "import json; "
        "from cw_platform import config_base as cb; "
        "from cw_platform.simkl_http import token_key; "
        "from providers.sync.simkl import _common; "
        "_common.STATE_DIR = cb.CONFIG / 'state'; "
        "key = token_key('test-token'); "
        "print(json.dumps([key, _common.account_settings_cached(key, 3600)]))"
    )
    output = subprocess.check_output([sys.executable, "-c", script], text=True)
    assert json.loads(output) == [key, data]


def test_cached_account_payload_is_encrypted_on_disk():
    data = {
        "account": {"id": 42, "type": "vip"},
        "user": {"name": "private-user", "email": "private@example.invalid"},
        "access_token": "private-access-token",
        "nested": {"password": "private-password"},
    }
    key = http.token_key("private-access-token")
    _common.account_settings_store(key, data)
    raw = _common._account_cache_path(key).read_text("utf-8")
    for value in ("private-user", "private@example.invalid", "private-access-token", "private-password"):
        assert value not in raw
    assert _common.account_settings_cached(key, 3600) == data


@pytest.mark.parametrize("payload", [{"account": {"type": "pro"}}, "invalid-ciphertext"])
def test_plaintext_and_corrupt_cached_payloads_are_rejected(payload):
    key = http.token_key("test-token")
    path = _common._account_cache_path(key)
    path.parent.mkdir(parents=True)
    path.write_text(json.dumps({"ts": time.time(), "data": payload}), "utf-8")
    assert _common.account_settings_cached(key, 3600) is None


def test_cache_is_unreadable_with_a_different_key(monkeypatch):
    key = http.token_key("test-token")
    _common.account_settings_store(key, {"account": {"type": "pro"}})
    monkeypatch.setenv("CW_CONFIG_KEY", "wrong-config-key")
    assert _common.account_settings_cached(key, 3600) is None


def test_expired_encrypted_cache_is_not_reused(monkeypatch):
    key = http.token_key("test-token")
    now = time.time()
    _common.account_settings_store(key, {"account": {"type": "pro"}})
    monkeypatch.setattr(_common.time, "time", lambda: now + 7200)
    assert _common.account_settings_cached(key, 3600) is None
    assert _common.account_settings_cached(key, float("inf")) == {"account": {"type": "pro"}}


@pytest.mark.parametrize("failure", ["missing", "exception"])
def test_encryption_failure_never_writes_plaintext(monkeypatch, failure):
    key = http.token_key("test-token")

    def unavailable(**kwargs):
        if failure == "exception":
            raise RuntimeError("test encryption failure")
        return None

    monkeypatch.setattr(cb, "_get_cipher", unavailable)
    _common.account_settings_store(key, {"access_token": "private-access-token"})
    assert not _common._account_cache_path(key).exists()


def test_token_rotation_preserves_encrypted_settings_and_quota():
    old_key = http.token_key("old-token")
    new_key = http.token_key("new-token")
    data = {"account": {"id": 42, "type": "pro"}}
    _common.account_settings_store(old_key, data)
    until = _common.block_quota(old_key, 300)
    http.transfer_token_state("old-token", "new-token")
    assert _common.account_settings_cached(new_key, 3600) == data
    assert _common.quota_blocked_until(new_key) == pytest.approx(until)
    with pytest.raises(_common.SIMKLQuotaError):
        http._check_quota("new-token")


@pytest.mark.parametrize("body", [
    {"error": "private-access-token"},
    {"error_description": "private-access-token"},
    {"error": "invalid_grant", "error_description": "private-access-token"},
])
def test_oauth_exchange_errors_do_not_log_or_raise_response_secrets(monkeypatch, capsys, body):
    monkeypatch.setattr(auth, "_post_form", lambda *args: (400, body))
    monkeypatch.setattr(auth, "_real_log", None)
    cfg = {"simkl": {"client_id": "test-client", "client_secret": "test-secret"}}
    with pytest.raises(RuntimeError) as exc:
        auth.PROVIDER.finish(cfg, code="test-code", code_verifier="test-verifier")
    assert "private-access-token" not in str(exc.value)
    assert "private-access-token" not in capsys.readouterr().out


@pytest.mark.parametrize("error", ["invalid_grant", "private-refresh-token"])
def test_refresh_errors_are_safe_and_preserve_reconnect_handling(monkeypatch, capsys, error):
    cfg = {"simkl": {
        "auth_version": 2, "client_id": "test-client", "access_token": "test-access",
        "refresh_token": "private-refresh-token",
    }}
    saved = []
    monkeypatch.setattr(auth, "load_config", lambda: cfg)
    monkeypatch.setattr(auth, "save_config", lambda data: saved.append(data))
    monkeypatch.setattr(auth, "_post_form", lambda *args: (400, {
        "error": error, "error_description": "private-refresh-token",
    }))
    monkeypatch.setattr(auth, "_real_log", None)
    result = auth.PROVIDER.refresh()
    assert result["ok"] is False
    assert result["error"] == ("invalid_grant" if error == "invalid_grant" else "oauth_error")
    assert "private-refresh-token" not in capsys.readouterr().out
    assert bool(saved) == (error == "invalid_grant")
    if saved:
        assert cfg["simkl"]["auth_error"] == "reconnect_required"


def test_device_start_error_does_not_return_response_secrets(monkeypatch):
    monkeypatch.setattr(auth, "_post_form", lambda *args: (400, {
        "error": "private-device-code", "error_description": "private-device-code",
    }))
    result = auth.PROVIDER._device_start({"simkl": {}})
    assert result == {"ok": False, "error": "http_error", "status": 400, "body": "oauth_error"}

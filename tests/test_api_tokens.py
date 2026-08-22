# tests/test_api_tokens.py
# CrossWatch - API token tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path

import pytest
from starlette.requests import Request


def _request(path: str, *, method: str = "GET", headers: dict[str, str] | None = None) -> Request:
    raw_headers = [(b"host", b"testserver")]
    for key, value in (headers or {}).items():
        raw_headers.append((str(key).lower().encode("latin-1"), str(value).encode("latin-1")))
    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": method,
        "scheme": "http",
        "path": path,
        "raw_path": path.encode("latin-1"),
        "query_string": b"",
        "headers": raw_headers,
        "client": ("127.0.0.1", 12345),
        "server": ("testserver", 80),
    }
    return Request(scope)


@pytest.fixture()
def tokens_env(config_base: Path, monkeypatch: pytest.MonkeyPatch):
    import importlib

    from cw_platform import config_base as cfg_base

    importlib.reload(cfg_base)
    from api import apiTokensAPI

    importlib.reload(apiTokensAPI)

    from api import appAuthAPI as auth

    salt = b"0123456789abcdef"
    cfg = {
        "app_auth": {
            "enabled": True,
            "username": "admin",
            "password": {
                "scheme": "pbkdf2_sha256",
                "iterations": 260_000,
                "salt": auth._b64e(salt),
                "hash": auth._b64e(auth._pbkdf2_hash("secrett1", salt, iterations=260_000)),
            },
            "sessions": [],
            "api_tokens": [],
        }
    }
    (config_base / "config.json").write_text(json.dumps(cfg), encoding="utf-8")
    apiTokensAPI._TOUCH_CACHE.clear()
    return apiTokensAPI


def test_issue_and_resolve_round_trip(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    raw, entry = mod.issue_api_token(load_config(), name="cli", expires_days=0)

    assert raw.startswith(mod.TOKEN_PREFIX)
    assert entry["name"] == "cli"
    assert entry["expires_at"] == 0
    assert raw not in json.dumps(load_config())
    stored = load_config()["app_auth"]["api_tokens"][0]["token_hash"]
    assert isinstance(stored, dict)
    assert stored["scheme"] == "pbkdf2_sha256"

    identity = mod.resolve_api_token(load_config(), raw)
    assert identity is not None
    assert identity["is_admin"] is True
    assert identity["auth_kind"] == "api_token"
    assert identity["api_token_id"] == entry["id"]


def test_unknown_and_malformed_tokens_are_rejected(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    mod.issue_api_token(load_config(), name="cli")
    cfg = load_config()

    assert mod.resolve_api_token(cfg, "") is None
    assert mod.resolve_api_token(cfg, "not-a-token") is None
    assert mod.resolve_api_token(cfg, mod.TOKEN_PREFIX + "wrongwrongwrong") is None


def test_expired_token_is_rejected(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config, update_config

    raw, entry = mod.issue_api_token(load_config(), name="cli", expires_days=1)
    assert mod.resolve_api_token(load_config(), raw) is not None

    def _expire(cfg: dict) -> None:
        for item in cfg["app_auth"]["api_tokens"]:
            if item["id"] == entry["id"]:
                item["expires_at"] = 1

    update_config(_expire)

    assert mod.resolve_api_token(load_config(), raw) is None
    assert mod.list_api_tokens(load_config()) == []


def test_plain_config_save_cannot_clobber_tokens(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config, save_config

    raw, _entry = mod.issue_api_token(load_config(), name="cli")

    cfg = load_config()
    cfg["app_auth"]["api_tokens"] = []
    cfg["scheduling"] = {"enabled": True}
    save_config(cfg)

    assert mod.resolve_api_token(load_config(), raw) is not None


def test_revoke_removes_the_token(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    raw, entry = mod.issue_api_token(load_config(), name="cli")
    assert mod.revoke_api_token(entry["id"]) is True
    assert mod.resolve_api_token(load_config(), raw) is None
    assert mod.revoke_api_token(entry["id"]) is False


def test_listing_never_exposes_the_secret(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config

    raw, _entry = mod.issue_api_token(load_config(), name="cli")
    listed = mod.list_api_tokens(load_config())

    assert len(listed) == 1
    blob = json.dumps(listed)
    assert raw not in blob
    assert "token_hash" not in blob


def test_extract_reads_header_and_bearer() -> None:
    from api import apiTokensAPI as mod

    assert mod.extract_api_token(_request("/api/status", headers={"X-CW-Token": "cwt_abc"})) == "cwt_abc"
    assert mod.extract_api_token(_request("/api/status", headers={"Authorization": "Bearer cwt_xyz"})) == "cwt_xyz"
    assert mod.extract_api_token(_request("/api/status")) == ""


def test_redaction_masks_token_hashes(tokens_env) -> None:
    mod = tokens_env
    from cw_platform.config_base import load_config, redact_config

    mod.issue_api_token(load_config(), name="cli")
    redacted = redact_config(load_config())

    for entry in redacted["app_auth"]["api_tokens"]:
        assert entry["token_hash"] == "•" * 8

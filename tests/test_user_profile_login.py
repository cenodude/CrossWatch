# tests/test_user_profile_login.py
# CrossWatch - User profile save with optional login tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

import pytest

import api.appAuthAPI as auth
import api.providerInstancesAPI as provider_api
from tests.test_app_auth_api import _auth_cfg, _json_body, _request

BOB_PROFILE_ID = "22222222222242228222222222222222"
ALICE_PROFILE_ID = "11111111111141118111111111111111"


@pytest.fixture
def env(monkeypatch) -> dict[str, Any]:
    store: dict[str, Any] = _auth_cfg()
    store["user_profiles"] = {ALICE_PROFILE_ID: {"label": "Alice", "instances": {}}}
    token, _exp = auth._issue_session(store, _request("/api/app-auth/login"))
    state: dict[str, Any] = {"store": store, "saves": 0, "audits": []}

    def fake_update(mutator):
        cfg = json.loads(json.dumps(state["store"]))
        result = mutator(cfg)
        state["store"] = cfg
        state["saves"] += 1
        return cfg, result

    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(state["store"])))
    monkeypatch.setattr(provider_api, "update_config", fake_update)
    monkeypatch.setattr(provider_api, "generate_user_profile_id", lambda _cfg: BOB_PROFILE_ID)
    monkeypatch.setattr(auth, "load_config", lambda: state["store"])
    monkeypatch.setattr(auth, "save_config", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(auth, "_audit", lambda _request, action, **kwargs: state["audits"].append((action, kwargs.get("target_id"))))
    auth._LOGIN_FAILS.clear()
    state["req"] = lambda: _request("/api/user-profiles", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    return state


def _users(state: dict[str, Any]) -> dict[str, Any]:
    return state["store"]["app_auth"].get("users") or {}


def test_create_profile_with_login_saves_once_and_user_can_sign_in(env) -> None:
    created = provider_api.api_user_profiles_create(
        {"label": "Bob", "login": {"username": "bob", "password": "secrett2", "permissions": {"write": False}}},
        request=env["req"](),
    )

    assert created["ok"] is True
    assert created["profile"]["id"] == BOB_PROFILE_ID
    assert created["login"]["username"] == "bob"
    assert created["login"]["profile_id"] == BOB_PROFILE_ID
    assert created["login"]["permissions"]["write"] is False
    assert env["saves"] == 1
    assert env["audits"] == [("user_created", created["login"]["id"])]

    login = auth.api_login(_request("/api/app-auth/login"), {"username": "bob", "password": "secrett2"})
    assert login.status_code == 200
    assert _json_body(login)["user"]["profile_id"] == BOB_PROFILE_ID


def test_create_profile_without_login_creates_no_user(env) -> None:
    created = provider_api.api_user_profiles_create({"label": "Bob"}, request=env["req"]())

    assert created["ok"] is True
    assert "login" not in created
    assert _users(env) == {}


@pytest.mark.parametrize(
    ("login", "status", "error"),
    [
        ({"username": "bob", "password": "short"}, 400, "Password must be at least 8 characters"),
        ({"username": "bob"}, 400, "Password is required"),
        ({"password": "secrett2"}, 400, "Username is required"),
        ({"username": "ADMIN", "password": "secrett2"}, 409, "Username already exists"),
    ],
)
def test_create_profile_with_bad_login_saves_nothing(env, login, status, error) -> None:
    before = json.loads(json.dumps(env["store"]))

    resp = provider_api.api_user_profiles_create({"label": "Bob", "login": login}, request=env["req"]())

    assert resp.status_code == status
    assert _json_body(resp) == {"ok": False, "error": error}
    assert env["store"] == before
    assert env["audits"] == []


def test_create_profile_with_login_requires_configured_auth(monkeypatch) -> None:
    store: dict[str, Any] = {"user_profiles": {}}
    monkeypatch.setattr(provider_api, "load_config", lambda: json.loads(json.dumps(store)))
    monkeypatch.setattr(provider_api, "update_config", lambda mutator: (store, mutator(json.loads(json.dumps(store)))))
    monkeypatch.setattr(provider_api, "generate_user_profile_id", lambda _cfg: BOB_PROFILE_ID)

    resp = provider_api.api_user_profiles_create(
        {"label": "Bob", "login": {"username": "bob", "password": "secrett2"}},
        request=_request("/api/user-profiles"),
    )

    assert resp.status_code == 403
    assert store == {"user_profiles": {}}


def test_update_adds_login_then_changes_the_linked_one(env) -> None:
    added = provider_api.api_user_profiles_update(
        ALICE_PROFILE_ID,
        {"label": "Alice", "login": {"username": "alice", "password": "secrett2"}},
        request=env["req"](),
    )
    user_id = added["login"]["id"]

    changed = provider_api.api_user_profiles_update(
        ALICE_PROFILE_ID,
        {"label": "Alice", "login": {"password": "newsecret", "enabled": False}},
        request=env["req"](),
    )

    assert added["ok"] is True
    assert changed["login"]["id"] == user_id
    assert changed["login"]["enabled"] is False
    assert list(_users(env)) == [user_id]
    assert auth._password_matches(_users(env)[user_id]["password"], "newsecret")
    assert env["audits"] == [("user_created", user_id), ("user_updated", user_id)]


def test_update_with_several_logins_needs_user_id(env) -> None:
    a = env["store"]["app_auth"]
    target = auth._new_managed_user(a, "alice", "secrett2", ALICE_PROFILE_ID, None)[0]
    auth._new_managed_user(a, "alice2", "secrett2", ALICE_PROFILE_ID, None)
    body = {"label": "Alice"}

    ambiguous = provider_api.api_user_profiles_update(ALICE_PROFILE_ID, {**body, "login": {"enabled": False}}, request=env["req"]())
    picked = provider_api.api_user_profiles_update(ALICE_PROFILE_ID, {**body, "login": {"user_id": target, "enabled": False}}, request=env["req"]())
    missing = provider_api.api_user_profiles_update(ALICE_PROFILE_ID, {**body, "login": {"user_id": "f" * 32}}, request=env["req"]())

    assert ambiguous.status_code == 409
    assert picked["login"]["id"] == target
    assert _users(env)[target]["enabled"] is False
    assert missing.status_code == 404

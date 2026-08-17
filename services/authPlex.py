# services/authPlex.py
# CrossWatch - Plex SSO authentication flow management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import hashlib
import secrets
import time
from urllib.parse import urlencode

import requests

PLEX_PIN_URL = "https://plex.tv/api/v2/pins"
PLEX_USER_URL = "https://plex.tv/api/v2/user"
PLEX_AUTH_URL = "https://app.plex.tv/auth#?"
PENDING_TTL_SEC = 10 * 60

_PENDING_FLOWS: dict[str, dict[str, Any]] = {}


def _now() -> int:
    return int(time.time())


def _sha256_hex(value: str) -> str:
    return hashlib.sha256((value or "").encode("utf-8")).hexdigest()


def _plex_sso(cfg: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    app_auth = cfg.get("app_auth")
    if not isinstance(app_auth, dict):
        if not create:
            return {}
        app_auth = {}
        cfg["app_auth"] = app_auth

    plex_sso = app_auth.get("plex_sso")
    if not isinstance(plex_sso, dict):
        if not create:
            return {}
        plex_sso = {}
        app_auth["plex_sso"] = plex_sso
    return plex_sso


def _headers(client_id: str, token: str | None = None) -> dict[str, str]:
    out = {
        "Accept": "application/json",
        "X-Plex-Client-Identifier": str(client_id or "").strip(),
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": "1.0",
        "X-Plex-Platform": "Web",
    }
    if token:
        out["X-Plex-Token"] = str(token).strip()
    return out


def _ensure_client_id(cfg: dict[str, Any]) -> str:
    plex_sso = _plex_sso(cfg, create=True)
    client_id = str(plex_sso.get("client_id") or "").strip()
    if client_id:
        return client_id
    client_id = f"crosswatch-{secrets.token_hex(10)}"
    plex_sso["client_id"] = client_id
    return client_id


def _prune_pending() -> None:
    now = _now()
    dead = [k for k, v in _PENDING_FLOWS.items() if int(v.get("expires_at") or 0) <= now]
    for key in dead:
        _PENDING_FLOWS.pop(key, None)


def _admin_link(cfg: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    return _plex_sso(cfg, create=create)


def _managed_link(raw_user: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    link = raw_user.get("plex_sso")
    if isinstance(link, dict):
        return link
    if create:
        raw_user["plex_sso"] = {}
        return raw_user["plex_sso"]
    return {}


def _linked_account_id(link: dict[str, Any]) -> str:
    return str(link.get("account_id") or link.get("linked_plex_account_id") or "").strip()


def _link_status(link: dict[str, Any], *, master_enabled: bool) -> dict[str, Any]:
    linked_id = _linked_account_id(link)
    linked = bool(linked_id)
    enabled = bool(master_enabled) and linked
    return {
        "enabled": enabled,
        "linked": linked,
        "client_id": str(link.get("client_id") or "").strip(),
        "linked_plex_account_id": linked_id,
        "linked_username": str(link.get("username") or link.get("linked_username") or "").strip(),
        "linked_email": str(link.get("email") or link.get("linked_email") or "").strip(),
        "linked_thumb": str(link.get("thumb") or link.get("linked_thumb") or "").strip(),
        "linked_at": int(link.get("linked_at") or 0),
    }


def get_status(cfg: dict[str, Any], raw_user: dict[str, Any] | None = None) -> dict[str, Any]:
    master = _plex_sso(cfg)
    master_enabled = bool(master.get("enabled"))
    link = _managed_link(raw_user) if isinstance(raw_user, dict) else master
    st = _link_status(link, master_enabled=master_enabled)
    st["client_id"] = str(master.get("client_id") or "").strip()
    return st


def login_available(cfg: dict[str, Any]) -> bool:
    master = _plex_sso(cfg)
    if not bool(master.get("enabled")):
        return False
    if _linked_account_id(master):
        return True
    app_auth = cfg.get("app_auth")
    users = app_auth.get("users") if isinstance(app_auth, dict) else None
    if isinstance(users, dict):
        for raw in users.values():
            if isinstance(raw, dict) and bool(raw.get("enabled", True)) and _linked_account_id(_managed_link(raw)):
                return True
    return False


def link_identity(cfg: dict[str, Any], identity: dict[str, Any], raw_user: dict[str, Any] | None = None) -> dict[str, Any]:
    master = _plex_sso(cfg, create=True)
    master["enabled"] = True
    if isinstance(raw_user, dict):
        link = _managed_link(raw_user, create=True)
        link["account_id"] = str(identity.get("id") or "").strip()
        link["username"] = str(identity.get("username") or "").strip()
        link["email"] = str(identity.get("email") or "").strip()
        link["thumb"] = str(identity.get("thumb") or "").strip()
        link["linked_at"] = _now()
        return get_status(cfg, raw_user)
    master["linked_plex_account_id"] = str(identity.get("id") or "").strip()
    master["linked_username"] = str(identity.get("username") or "").strip()
    master["linked_email"] = str(identity.get("email") or "").strip()
    master["linked_thumb"] = str(identity.get("thumb") or "").strip()
    master["linked_at"] = _now()
    return get_status(cfg)


def unlink_identity(cfg: dict[str, Any], raw_user: dict[str, Any] | None = None) -> dict[str, Any]:
    if isinstance(raw_user, dict):
        raw_user.pop("plex_sso", None)
        return get_status(cfg, raw_user)
    plex_sso = _plex_sso(cfg, create=True)
    plex_sso["linked_plex_account_id"] = ""
    plex_sso["linked_username"] = ""
    plex_sso["linked_email"] = ""
    plex_sso["linked_thumb"] = ""
    plex_sso["linked_at"] = 0
    return get_status(cfg)


def identity_matches_link(link: dict[str, Any] | None, identity: dict[str, Any]) -> bool:
    if not isinstance(link, dict):
        return False
    want = _linked_account_id(link)
    got = str(identity.get("id") or "").strip()
    return bool(want and got and want == got)


def identity_matches(cfg: dict[str, Any], identity: dict[str, Any]) -> bool:
    return identity_matches_link(_admin_link(cfg), identity)


def start_flow(
    cfg: dict[str, Any],
    *,
    intent: str,
    callback_url: str,
    flow_nonce_hash: str,
    remember_me: bool = False,
    target_user_id: str = "",
) -> dict[str, Any]:
    _prune_pending()

    client_id = _ensure_client_id(cfg)
    resp = requests.post(
        PLEX_PIN_URL,
        headers={**_headers(client_id), "Content-Type": "application/x-www-form-urlencoded"},
        data={"strong": "true"},
        timeout=20,
    )
    resp.raise_for_status()
    data = resp.json() or {}

    pin_id = str(data.get("id") or "").strip()
    code = str(data.get("code") or "").strip()
    if not pin_id or not code:
        raise RuntimeError("Plex PIN could not be issued")

    expires_at = _now() + PENDING_TTL_SEC
    state = secrets.token_urlsafe(18)
    _PENDING_FLOWS[state] = {
        "intent": str(intent or "").strip(),
        "client_id": client_id,
        "pin_id": pin_id,
        "flow_nonce_hash": str(flow_nonce_hash or "").strip(),
        "remember_me": bool(remember_me),
        "target_user_id": str(target_user_id or "").strip(),
        "expires_at": expires_at,
    }

    params = {
        "clientID": client_id,
        "code": code,
        "context[device][product]": "CrossWatch",
        "forwardUrl": str(callback_url or "").strip(),
    }

    return {
        "ok": True,
        "state": state,
        "pin_id": pin_id,
        "auth_url": f"{PLEX_AUTH_URL}{urlencode(params)}",
        "expires_at": expires_at,
    }


def check_flow(cfg: dict[str, Any], *, state: str, intent: str) -> dict[str, Any]:
    _prune_pending()
    rec = _PENDING_FLOWS.get(str(state or "").strip())
    if not isinstance(rec, dict):
        return {"ok": False, "error": "Plex sign-in expired. Start again.", "status_code": 400}

    if str(rec.get("intent") or "") != str(intent or ""):
        return {"ok": False, "error": "Plex sign-in expired. Start again.", "status_code": 400}

    client_id = str(rec.get("client_id") or _ensure_client_id(cfg)).strip()
    pin_id = str(rec.get("pin_id") or "").strip()
    if not pin_id:
        _PENDING_FLOWS.pop(str(state or "").strip(), None)
        return {"ok": False, "error": "Plex sign-in expired. Start again.", "status_code": 400}

    pin_resp = requests.get(f"{PLEX_PIN_URL}/{pin_id}", headers=_headers(client_id), timeout=20)
    pin_resp.raise_for_status()
    pin = pin_resp.json() or {}
    token = str(pin.get("authToken") or "").strip()
    if not token:
        return {"ok": True, "pending": True}

    user_resp = requests.get(PLEX_USER_URL, headers=_headers(client_id, token), timeout=20)
    user_resp.raise_for_status()
    user = user_resp.json() or {}

    _PENDING_FLOWS.pop(str(state or "").strip(), None)
    return {
        "ok": True,
        "pending": False,
        "remember_me": bool(rec.get("remember_me")),
        "flow_nonce_hash": str(rec.get("flow_nonce_hash") or "").strip(),
        "target_user_id": str(rec.get("target_user_id") or "").strip(),
        "identity": {
            "id": str(user.get("id") or "").strip(),
            "username": str(user.get("username") or "").strip(),
            "email": str(user.get("email") or "").strip(),
            "thumb": str(user.get("thumb") or "").strip(),
        },
    }

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


def get_status(cfg: dict[str, Any]) -> dict[str, Any]:
    plex_sso = _plex_sso(cfg)
    linked_id = str(plex_sso.get("linked_plex_account_id") or "").strip()
    linked = bool(linked_id)
    enabled = bool(plex_sso.get("enabled")) and linked
    return {
        "enabled": enabled,
        "linked": linked,
        "client_id": str(plex_sso.get("client_id") or "").strip(),
        "linked_plex_account_id": linked_id,
        "linked_username": str(plex_sso.get("linked_username") or "").strip(),
        "linked_email": str(plex_sso.get("linked_email") or "").strip(),
        "linked_thumb": str(plex_sso.get("linked_thumb") or "").strip(),
        "linked_at": int(plex_sso.get("linked_at") or 0),
    }


def login_available(cfg: dict[str, Any]) -> bool:
    st = get_status(cfg)
    return bool(st["enabled"] and st["linked_plex_account_id"])


def link_identity(cfg: dict[str, Any], identity: dict[str, Any]) -> dict[str, Any]:
    plex_sso = _plex_sso(cfg, create=True)
    plex_sso["enabled"] = True
    plex_sso["linked_plex_account_id"] = str(identity.get("id") or "").strip()
    plex_sso["linked_username"] = str(identity.get("username") or "").strip()
    plex_sso["linked_email"] = str(identity.get("email") or "").strip()
    plex_sso["linked_thumb"] = str(identity.get("thumb") or "").strip()
    plex_sso["linked_at"] = _now()
    return get_status(cfg)


def unlink_identity(cfg: dict[str, Any]) -> dict[str, Any]:
    plex_sso = _plex_sso(cfg, create=True)
    plex_sso["enabled"] = False
    plex_sso["linked_plex_account_id"] = ""
    plex_sso["linked_username"] = ""
    plex_sso["linked_email"] = ""
    plex_sso["linked_thumb"] = ""
    plex_sso["linked_at"] = 0
    return get_status(cfg)


def identity_matches(cfg: dict[str, Any], identity: dict[str, Any]) -> bool:
    want = str(get_status(cfg).get("linked_plex_account_id") or "").strip()
    got = str(identity.get("id") or "").strip()
    return bool(want and got and want == got)


def start_flow(
    cfg: dict[str, Any],
    *,
    intent: str,
    callback_url: str,
    flow_nonce_hash: str,
    remember_me: bool = False,
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
        "identity": {
            "id": str(user.get("id") or "").strip(),
            "username": str(user.get("username") or "").strip(),
            "email": str(user.get("email") or "").strip(),
            "thumb": str(user.get("thumb") or "").strip(),
        },
    }

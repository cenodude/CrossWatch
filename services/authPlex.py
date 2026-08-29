# services/authPlex.py
# CrossWatch - Plex SSO authentication flow management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import hashlib
import secrets
import threading
import time
from urllib.parse import urlencode

import requests

from providers.sync.plex._common import stable_client_id

PLEX_PIN_URL = "https://plex.tv/api/v2/pins"
PLEX_USER_URL = "https://plex.tv/api/v2/user"
PLEX_AUTH_URL = "https://app.plex.tv/auth#?"
PENDING_TTL_SEC = 10 * 60
START_RATE_WINDOW_SEC = 60
START_RATE_LIMIT = 8
MAX_PENDING_FLOWS = 64

_PENDING_FLOWS: dict[str, dict[str, Any]] = {}
_START_EVENTS: dict[str, list[int]] = {}
_PENDING_STARTS_IN_FLIGHT = 0
_PENDING_LOCK = threading.RLock()


class PlexStartRateLimited(RuntimeError):
    def __init__(self, retry_after: int) -> None:
        super().__init__("Plex sign-in start rate limited")
        self.retry_after = max(1, int(retry_after or 1))


class PlexPendingCapacityError(RuntimeError):
    def __init__(self, retry_after: int = 30) -> None:
        super().__init__("Too many pending Plex sign-in flows")
        self.retry_after = max(1, int(retry_after or 1))


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


def _plex_provider(cfg: dict[str, Any], *, create: bool = False) -> dict[str, Any]:
    plex = cfg.get("plex")
    if isinstance(plex, dict):
        return plex
    if not create:
        return {}
    plex = {}
    cfg["plex"] = plex
    return plex


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
    plex = _plex_provider(cfg, create=True)
    client_id = str(plex.get("client_id") or plex_sso.get("client_id") or "").strip()
    if not client_id:
        client_id = stable_client_id()
    plex_sso["client_id"] = client_id
    plex["client_id"] = client_id
    return client_id


def ensure_client_id(cfg: dict[str, Any]) -> str:
    return _ensure_client_id(cfg)


def _prune_pending_locked(now: int | None = None) -> None:
    ts = _now() if now is None else int(now)
    dead = [k for k, v in _PENDING_FLOWS.items() if int(v.get("expires_at") or 0) <= ts]
    for key in dead:
        _PENDING_FLOWS.pop(key, None)


def _prune_pending() -> None:
    with _PENDING_LOCK:
        _prune_pending_locked()


def _prune_start_events_locked(now: int) -> None:
    cutoff = int(now) - START_RATE_WINDOW_SEC
    for key in list(_START_EVENTS.keys()):
        kept = [ts for ts in _START_EVENTS.get(key, []) if int(ts) > cutoff]
        if kept:
            _START_EVENTS[key] = kept
        else:
            _START_EVENTS.pop(key, None)


def _capacity_retry_after_locked(now: int) -> int:
    expiries = [int(v.get("expires_at") or 0) for v in _PENDING_FLOWS.values() if int(v.get("expires_at") or 0) > now]
    if expiries:
        return max(1, min(60, min(expiries) - now))
    return 30


def _reserve_start_slot(client_key: str | None = None) -> None:
    global _PENDING_STARTS_IN_FLIGHT
    now = _now()
    key = str(client_key or "").strip()
    with _PENDING_LOCK:
        _prune_pending_locked(now)
        _prune_start_events_locked(now)
        if key:
            events = list(_START_EVENTS.get(key) or [])
            if len(events) >= START_RATE_LIMIT:
                retry_after = START_RATE_WINDOW_SEC - max(0, now - min(events))
                raise PlexStartRateLimited(retry_after)
        if len(_PENDING_FLOWS) + _PENDING_STARTS_IN_FLIGHT >= MAX_PENDING_FLOWS:
            raise PlexPendingCapacityError(_capacity_retry_after_locked(now))
        if key:
            _START_EVENTS[key] = [*events, now]
        _PENDING_STARTS_IN_FLIGHT += 1


def _release_start_slot() -> None:
    global _PENDING_STARTS_IN_FLIGHT
    with _PENDING_LOCK:
        _PENDING_STARTS_IN_FLIGHT = max(0, _PENDING_STARTS_IN_FLIGHT - 1)


def _issue_pin(client_id: str) -> tuple[str, str]:
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
    return pin_id, code


def _store_pending_flow(
    *,
    intent: str,
    client_id: str,
    pin_id: str,
    code: str,
    callback_url: str,
    flow_nonce_hash: str,
    remember_me: bool,
    target_user_id: str,
) -> dict[str, Any]:
    global _PENDING_STARTS_IN_FLIGHT
    expires_at = _now() + PENDING_TTL_SEC
    params = {
        "clientID": client_id,
        "code": code,
        "context[device][product]": "CrossWatch",
        "forwardUrl": str(callback_url or "").strip(),
    }
    with _PENDING_LOCK:
        _PENDING_STARTS_IN_FLIGHT = max(0, _PENDING_STARTS_IN_FLIGHT - 1)
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

    return {
        "ok": True,
        "state": state,
        "pin_id": pin_id,
        "auth_url": f"{PLEX_AUTH_URL}{urlencode(params)}",
        "expires_at": expires_at,
    }


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
    client_id: str = "",
    client_key: str | None = None,
) -> dict[str, Any]:
    client_id = str(client_id or "").strip() or _ensure_client_id(cfg)
    _reserve_start_slot(client_key)
    reserved = True

    try:
        pin_id, code = _issue_pin(client_id)
        out = _store_pending_flow(
            intent=intent,
            client_id=client_id,
            pin_id=pin_id,
            code=code,
            callback_url=callback_url,
            flow_nonce_hash=flow_nonce_hash,
            remember_me=remember_me,
            target_user_id=target_user_id,
        )
        reserved = False
        return out
    finally:
        if reserved:
            _release_start_slot()


def check_flow(cfg: dict[str, Any], *, state: str, intent: str) -> dict[str, Any]:
    state_key = str(state or "").strip()
    with _PENDING_LOCK:
        _prune_pending_locked()
        rec = _PENDING_FLOWS.get(state_key)
        rec = dict(rec) if isinstance(rec, dict) else None
    if not isinstance(rec, dict):
        return {"ok": False, "error": "Plex sign-in expired. Start again.", "status_code": 400}

    if str(rec.get("intent") or "") != str(intent or ""):
        return {"ok": False, "error": "Plex sign-in expired. Start again.", "status_code": 400}

    client_id = str(rec.get("client_id") or _ensure_client_id(cfg)).strip()
    pin_id = str(rec.get("pin_id") or "").strip()
    if not pin_id:
        with _PENDING_LOCK:
            _PENDING_FLOWS.pop(state_key, None)
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

    with _PENDING_LOCK:
        _PENDING_FLOWS.pop(state_key, None)
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

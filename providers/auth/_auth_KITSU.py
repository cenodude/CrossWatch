# providers/auth/_auth_KITSU.py
# CrossWatch - Kitsu authentication and token refresh
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections.abc import Mapping
from typing import Any

import requests

from cw_platform.app_version import user_agent
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block
from ._auth_base import AuthManifest, AuthStatus

__VERSION__ = "0.1"
API_URL = "https://kitsu.io/api/edge"
TOKEN_URL = "https://kitsu.io/api/oauth/token"
IDENTITY_URL = API_URL + "/users?filter[self]=true"
_LOCKS: dict[str, threading.RLock] = {}
_GUARD = threading.Lock()


class KitsuAuthError(RuntimeError):
    pass


def instance_lock(instance_id: Any) -> Any:
    with _GUARD:
        return _LOCKS.setdefault(normalize_instance_id(instance_id), threading.RLock())


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool((block or {}).get("access_token") and not (block or {}).get("reauth_required"))


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    block = block or {}
    return {"connected": is_configured(block), "label": "Kitsu", "user": block.get("user") or {},
            "expires_at": block.get("expires_at"), "reauth_required": bool(block.get("reauth_required"))}


def _exchange(payload: dict[str, str], session: Any = None) -> dict[str, Any]:
    try:
        response = (session or requests).post(TOKEN_URL, data=payload, timeout=15,
            headers={"Accept": "application/json", "User-Agent": user_agent("KitsuAuth")}, allow_redirects=False)
        data = response.json()
    except (requests.RequestException, ValueError):
        raise KitsuAuthError("Kitsu token request failed") from None
    finally:
        payload.clear()
    if not isinstance(data, dict):
        raise KitsuAuthError("Kitsu returned an invalid token response")
    if response.status_code >= 300:
        reason = "invalid_grant" if data.get("error") == "invalid_grant" else "token_request_failed"
        raise KitsuAuthError(reason)
    if not data.get("access_token") or not data.get("refresh_token"):
        raise KitsuAuthError("Kitsu returned an incomplete token response")
    try:
        expires_at = int(data.get("created_at") or time.time()) + int(data["expires_in"])
    except (TypeError, ValueError, KeyError):
        raise KitsuAuthError("Kitsu returned an invalid token expiry") from None
    return {"access_token": str(data["access_token"]), "refresh_token": str(data["refresh_token"]),
            "expires_at": expires_at, "scope": str(data.get("scope") or ""), "reauth_required": False}


def identity(response: requests.Response) -> dict[str, str]:
    if response.status_code == 401:
        raise KitsuAuthError("Kitsu reconnect required")
    if response.status_code != 200:
        raise KitsuAuthError("Kitsu identity request failed")
    try:
        rows = response.json().get("data")
        if not isinstance(rows, list) or len(rows) != 1 or not rows[0].get("id") or rows[0].get("type") != "users":
            raise ValueError()
        row = rows[0]
        return {"id": str(row["id"]), "name": str(row.get("attributes", {}).get("name") or "")}
    except (ValueError, TypeError, AttributeError):
        raise KitsuAuthError("Kitsu returned an invalid identity") from None


def login(username: str, password: str) -> dict[str, Any]:
    if not username.strip() or not password:
        raise KitsuAuthError("Enter your Kitsu email or username and password")
    try:
        tokens = _exchange({"grant_type": "password", "username": username.strip(), "password": password})
    finally:
        password = ""
    with requests.Session() as session:
        response = request_with_auth(session, "GET", IDENTITY_URL, cfg={"kitsu": tokens})
        tokens["user"] = identity(response)
    return tokens


def clear_oauth(block: dict[str, Any]) -> None:
    for key in ("access_token", "refresh_token", "scope"):
        block[key] = ""
    block["expires_at"] = 0
    block["user"] = {}
    block.pop("password", None)


def refresh_token(cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None,
                  rejected_token: str | None = None) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    with instance_lock(inst):
        full = load_config()
        current = resolve_provider_block(full, "kitsu", inst)
        supplied = resolve_provider_block(cfg or {}, "kitsu", inst)
        if not is_configured(current):
            raise KitsuAuthError("Kitsu reconnect required")
        if (supplied.get("user") or {}).get("id") and (supplied.get("user") or {}).get("id") != (current.get("user") or {}).get("id"):
            raise KitsuAuthError("Kitsu account changed; retry the operation")
        if rejected_token and current.get("access_token") != rejected_token:
            return current
        if not rejected_token and int(current.get("expires_at") or 0) > time.time() + 60:
            return current
        try:
            tokens = _exchange({"grant_type": "refresh_token", "refresh_token": str(current.get("refresh_token") or "")})
        except KitsuAuthError as exc:
            if str(exc) == "invalid_grant":
                full = load_config()
                block = ensure_instance_block(full, "kitsu", inst)
                clear_oauth(block)
                block["reauth_required"] = True
                save_config(full)
            raise
        full = load_config()
        block = ensure_instance_block(full, "kitsu", inst)
        block.update(tokens)
        block.pop("password", None)
        save_config(full)
        return dict(block)


def request_with_auth(session: requests.Session, method: str, url: str, *, cfg: Mapping[str, Any] | None,
                      instance_id: Any = None, **kwargs: Any) -> requests.Response:
    from providers.sync._mod_common import request_with_retries

    if not (url == API_URL or url.startswith(API_URL + "/")):
        raise KitsuAuthError("Invalid Kitsu API URL")
    block = resolve_provider_block(cfg or {}, "kitsu", instance_id)
    if not is_configured(block):
        raise KitsuAuthError("Kitsu reconnect required")
    if int(block.get("expires_at") or 0) and int(block["expires_at"]) <= time.time() + 60:
        block = refresh_token(cfg, instance_id=instance_id)
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update({"Accept": "application/vnd.api+json", "Content-Type": "application/vnd.api+json",
                    "User-Agent": user_agent("Kitsu")})
    kwargs.setdefault("timeout", 15)
    kwargs.setdefault("max_retries", 3 if method.upper() == "GET" else 1)
    kwargs["allow_redirects"] = False
    for attempt in range(2):
        token = str(block["access_token"])
        headers["Authorization"] = "Bearer " + token
        response = request_with_retries(session, method, url, headers=headers, **kwargs)
        if response.status_code != 401 or attempt:
            return response
        block = refresh_token(cfg, instance_id=instance_id, rejected_token=token)
    return response


class KitsuAuth:
    name = "KITSU"

    def manifest(self) -> AuthManifest:
        return AuthManifest(name=self.name, label="Kitsu", flow="password",
            actions={"start": False, "finish": True, "refresh": True, "disconnect": True},
            notes="Sign in once with your Kitsu account. Only tokens are retained.")

    def capabilities(self) -> dict[str, Any]:
        return {"features": {name: {"read": True, "write": True} for name in ("watchlist", "ratings", "history")}}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        block = resolve_provider_block(cfg, "kitsu", instance_id)
        return AuthStatus(connected=is_configured(block), label="Kitsu", user=(block.get("user") or {}).get("name"),
                          extra={"reauth_required": bool(block.get("reauth_required"))})

    def disconnect(self, cfg: dict[str, Any], *, instance_id: Any = None) -> AuthStatus:
        clear_oauth(ensure_instance_block(cfg, "kitsu", instance_id))
        return self.get_status(cfg, instance_id=instance_id)

    def finish(self, cfg: dict[str, Any], *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        try:
            connection = login(str(payload.get("username") or ""), str(payload.pop("password", "")))
        finally:
            payload.clear()
        block = ensure_instance_block(cfg, "kitsu", instance_id)
        block.update(connection)
        block.pop("password", None)
        return self.get_status(cfg, instance_id=instance_id)

    def refresh(self, cfg: dict[str, Any], *, instance_id: Any = None) -> AuthStatus:
        refresh_token(cfg, instance_id=instance_id)
        return self.get_status(load_config(), instance_id=instance_id)


PROVIDER = KitsuAuth()


def html() -> str:
    return '''<div class="section" id="sec-kitsu">
  <div class="head" data-toggle-section="sec-kitsu"><span class="chev"></span><strong>Kitsu</strong></div>
  <div class="body"><div class="cw-panel"><div class="cw-meta-provider-panel active" data-provider="kitsu">
    <div class="cw-panel-head"><div class="cw-panel-title">Kitsu</div></div>
    <div class="cw-subtiles"><button type="button" class="cw-subtile active" data-sub="auth">Authentication</button></div>
    <div class="cw-subpanels"><div class="cw-subpanel active" data-sub="auth">
      <div class="grid2">
        <div><label for="kitsu_username">Email or username</label><input id="kitsu_username" autocomplete="username" /></div>
        <div><label for="kitsu_password">Password</label><input id="kitsu_password" type="password" autocomplete="current-password" /></div>
      </div>
      <div class="inline"><button class="btn" id="kitsu_connect" type="button">Connect Kitsu</button>
        <button class="btn danger" id="kitsu_disconnect" type="button">Disconnect</button>
        <span class="msg" id="kitsu_msg" role="status" aria-live="polite"></span></div>
      <div class="anime-mapping-rec hidden" id="kitsu_mapping_recommendation">
        <div class="anime-mapping-rec-copy">
          <div class="anime-mapping-rec-kicker">Recommended for Kitsu</div>
          <strong>Use Anime ID Mapping</strong>
          <div class="muted">Match anime across providers and translate episode numbering for history and scrobbling.</div>
          <div class="anime-mapping-rec-state hidden" id="kitsu_mapping_recommendation_state" role="status" aria-live="polite"></div>
        </div>
        <div class="anime-mapping-rec-actions">
          <button class="btn primary" type="button" id="kitsu_enable_mapping">Enable Anime ID Mapping</button>
          <button class="btn" type="button" id="kitsu_dismiss_mapping">Not now</button>
        </div>
      </div>
    </div></div>
  </div></div></div>
</div>'''

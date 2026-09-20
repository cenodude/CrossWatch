# providers/auth/_auth_SIMKL.py
# CrossWatch - SIMKL Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import base64
import hashlib
import os
import secrets
import threading
import time
from collections.abc import Mapping, MutableMapping
from typing import Any
from urllib.parse import urlencode

import requests
from cw_platform.simkl_http import paced_request, record_outcome, transfer_token_state

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, ensure_provider_block, normalize_instance_id
from providers.sync.simkl._common import simkl_api_params, simkl_user_agent

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

def log(msg: str, level: str = "INFO", module: str = "AUTH", **_: Any) -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module=module, **_)
        else:
            print(f"[{module}] {level}: {msg}")
    except Exception:
        pass

SIMKL_PIN = "https://api.simkl.com/oauth/pin"
PIN_VERIFY_URL = "https://simkl.com/pin"
OAUTH2_AUTHORIZE = "https://simkl.com/oauth2/authorize"
OAUTH2_DEVICE = "https://api.simkl.com/oauth2/device"
OAUTH2_TOKEN = "https://api.simkl.com/oauth2/token"
OAUTH2_REVOKE = "https://api.simkl.com/oauth2/revoke"
OAUTH2_ISSUER = "https://simkl.com"
OAUTH2_SCOPE = "media:read media:write"
DEVICE_GRANT = "urn:ietf:params:oauth:grant-type:device_code"
# Baked CrossWatch app id used for the PIN flow (public identifier; env-overridable),
# mirroring how MDBList bakes its device-code client id in code.
DEFAULT_PIN_CLIENT_ID = "d9b210c448f28757294ce491a834a7591aabdd7b01f678031a07574fe6a4fb47"
PIN_CLIENT_ID_ENV = "CROSSWATCH_SIMKL_CLIENT_ID"
DEFAULT_DEVICE_CLIENT_ID = "a984232a78f1a5a690b12afdd94de142d08f7bf87550b84860748a8e86396c2d"
DEVICE_CLIENT_ID_ENV = "CROSSWATCH_SIMKL_DEVICE_CLIENT_ID"
V1_SUNSET = "2027-03-31"
REFRESH_MARGIN_S = 24 * 3600
USE_REFRESH_MARGIN_S = 48 * 3600
REFRESH_WORKER_INTERVAL_S = 3600
TOKEN_KEYS = (
    "access_token",
    "refresh_token",
    "token_expires_at",
    "scopes",
    "account",
    "_pending_pin",
    "auth_method",
    "auth_version",
    "auth_error",
)
UA = "CrossWatch/1.0"
HTTP_TIMEOUT = 15
__VERSION__ = "3.0.0"

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()
_WORKER: threading.Thread | None = None
_WORKER_GUARD = threading.Lock()


def app_pin_client_id() -> str:
    return str(os.environ.get(PIN_CLIENT_ID_ENV) or DEFAULT_PIN_CLIENT_ID).strip()


def app_device_client_id() -> str:
    return str(os.environ.get(DEVICE_CLIENT_ID_ENV) or DEFAULT_DEVICE_CLIENT_ID).strip()


def baked_client_ids() -> set[str]:
    return {cid for cid in (app_pin_client_id(), app_device_client_id()) if cid}


def auth_version(block: Mapping[str, Any] | None) -> int:
    blk = block if isinstance(block, Mapping) else {}
    token = str(blk.get("access_token") or "").strip()
    if not token:
        return 0
    try:
        declared = int(blk.get("auth_version") or 0)
    except (TypeError, ValueError):
        declared = 0
    if declared >= 2 or token.startswith("simkl_at_"):
        return 2
    return 1


def token_expires_at(block: Mapping[str, Any] | None) -> int:
    try:
        return int((block or {}).get("token_expires_at") or 0)
    except (TypeError, ValueError):
        return 0


def needs_refresh(block: Mapping[str, Any] | None, margin_s: int = REFRESH_MARGIN_S) -> bool:
    if auth_version(block) != 2:
        return False
    if not str((block or {}).get("refresh_token") or "").strip():
        return False
    exp = token_expires_at(block)
    return not exp or (exp - int(time.time())) <= max(0, int(margin_s))


def _refresh_lock(instance_id: Any) -> threading.Lock:
    inst = normalize_instance_id(instance_id)
    with _REFRESH_LOCKS_GUARD:
        lock = _REFRESH_LOCKS.get(inst)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[inst] = lock
        return lock


def _pkce_pair() -> tuple[str, str]:
    verifier = secrets.token_urlsafe(64)[:96]
    digest = hashlib.sha256(verifier.encode("ascii")).digest()
    challenge = base64.urlsafe_b64encode(digest).decode("ascii").rstrip("=")
    return verifier, challenge


def _form_headers() -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/x-www-form-urlencoded",
        "User-Agent": simkl_user_agent(),
    }


def _post_form(url: str, data: Mapping[str, Any]) -> tuple[int, dict[str, Any], str]:
    r = paced_request(requests.post, "POST", url, data={k: v for k, v in data.items() if v not in (None, "")}, headers=_form_headers(), timeout=HTTP_TIMEOUT)
    try:
        body = r.json()
    except ValueError:
        body = None
    return int(r.status_code), body if isinstance(body, dict) else {}, (r.text or "")[:400]


def _store_tokens(blk: MutableMapping[str, Any], tok: Mapping[str, Any], client_id: str, method: str) -> None:
    blk["access_token"] = str(tok.get("access_token") or "").strip()
    refresh = str(tok.get("refresh_token") or "").strip()
    if refresh:
        blk["refresh_token"] = refresh
    try:
        expires_in = int(tok.get("expires_in") or 0)
    except (TypeError, ValueError):
        expires_in = 0
    blk["token_expires_at"] = int(time.time()) + expires_in if expires_in > 0 else 0
    if tok.get("scope"):
        blk["scopes"] = str(tok.get("scope"))
    blk["client_id"] = client_id
    blk["api_key"] = client_id
    blk["auth_method"] = method
    blk["auth_version"] = 2
    record_outcome(str(blk["access_token"]), 200)


def _instance_ids(cfg: Mapping[str, Any]) -> list[str]:
    base = cfg.get("simkl") if isinstance(cfg, Mapping) else None
    if not isinstance(base, Mapping):
        return []
    out = ["default"]
    insts = base.get("instances")
    if isinstance(insts, Mapping):
        out.extend(normalize_instance_id(k) for k in insts.keys() if normalize_instance_id(k) != "default")
    return out


def _lookup_block(cfg: Mapping[str, Any], instance_id: Any) -> Mapping[str, Any]:
    base = cfg.get("simkl") if isinstance(cfg, Mapping) else None
    if not isinstance(base, Mapping):
        return {}
    inst = normalize_instance_id(instance_id)
    if inst == "default":
        return base
    insts = base.get("instances")
    sub = insts.get(inst) if isinstance(insts, Mapping) else None
    return sub if isinstance(sub, Mapping) else {}


def instance_for_block(cfg: Mapping[str, Any], block: Mapping[str, Any] | None) -> str | None:
    blk = block if isinstance(block, Mapping) else {}
    for field_name in ("refresh_token", "access_token"):
        value = str(blk.get(field_name) or "").strip()
        if not value:
            continue
        for inst in _instance_ids(cfg):
            if str(_lookup_block(cfg, inst).get(field_name) or "").strip() == value:
                return inst
    return None


def fresh_access_token(block: Mapping[str, Any] | None, *, margin_s: int = USE_REFRESH_MARGIN_S) -> str:
    blk = block if isinstance(block, Mapping) else {}
    current = str(blk.get("access_token") or "").strip()
    if not needs_refresh(blk, margin_s):
        return current
    try:
        inst = instance_for_block(load_config() or {}, blk)
    except Exception:
        inst = None
    if inst is None:
        return current
    return ensure_fresh(inst, margin_s=margin_s) or current


def ensure_fresh(instance_id: Any = None, *, margin_s: int = REFRESH_MARGIN_S) -> str:
    inst = normalize_instance_id(instance_id)
    try:
        blk = _lookup_block(load_config() or {}, inst)
    except Exception:
        return ""
    if needs_refresh(blk, margin_s):
        PROVIDER.refresh(None, instance_id=inst, margin_s=margin_s)
        try:
            blk = _lookup_block(load_config() or {}, inst)
        except Exception:
            return ""
    return str(blk.get("access_token") or "").strip()


def recover_access_token(block: Mapping[str, Any] | None, current: str) -> str:
    blk = block if isinstance(block, Mapping) else {}
    if auth_version(blk) != 2:
        return ""
    try:
        cfg = load_config() or {}
    except Exception:
        return ""
    inst = instance_for_block(cfg, blk)
    if inst is None:
        return ""
    stored = str(_lookup_block(cfg, inst).get("access_token") or "").strip()
    if stored and stored != str(current or "").strip():
        return stored
    res = PROVIDER.refresh(None, instance_id=inst)
    if not res.get("ok"):
        return ""
    try:
        return str(_lookup_block(load_config() or {}, inst).get("access_token") or "").strip()
    except Exception:
        return ""


def refresh_all(margin_s: int = REFRESH_MARGIN_S) -> dict[str, str]:
    out: dict[str, str] = {}
    try:
        cfg = load_config() or {}
    except Exception:
        return out
    for inst in _instance_ids(cfg):
        if needs_refresh(_lookup_block(cfg, inst), margin_s):
            res = PROVIDER.refresh(None, instance_id=inst, margin_s=margin_s)
            out[inst] = str(res.get("status") or "")
    return out


def _refresh_worker_loop() -> None:
    while True:
        try:
            refresh_all()
        except Exception as e:
            log(f"SIMKL: refresh worker error: {type(e).__name__}", level="ERROR", module="AUTH")
        time.sleep(REFRESH_WORKER_INTERVAL_S)


def start_refresh_worker() -> bool:
    global _WORKER
    with _WORKER_GUARD:
        if _WORKER is not None and _WORKER.is_alive():
            return False
        _WORKER = threading.Thread(target=_refresh_worker_loop, name="simkl-token-refresh", daemon=True)
        _WORKER.start()
        return True


def revoke_block(block: Mapping[str, Any] | None) -> bool:
    blk = block if isinstance(block, Mapping) else {}
    if auth_version(blk) != 2:
        return False
    token = str(blk.get("refresh_token") or blk.get("access_token") or "").strip()
    client_id = str(blk.get("client_id") or "").strip()
    if not token or not client_id:
        return False
    try:
        status, _, _ = _post_form(
            OAUTH2_REVOKE,
            {"token": token, "client_id": client_id, "client_secret": str(blk.get("client_secret") or "").strip()},
        )
    except requests.RequestException:
        return False
    return status < 400

class SimklAuth(AuthProvider):
    name = "SIMKL"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="SIMKL",
            label="SIMKL",
            flow="oauth",
            fields=[
                {
                    "key": "simkl.client_id",
                    "label": "Client ID",
                    "type": "text",
                    "required": True,
                },
                {
                    "key": "simkl.client_secret",
                    "label": "Client Secret",
                    "type": "password",
                    "required": True,
                },
            ],
            actions={"start": True, "finish": False, "refresh": True, "disconnect": True},
            notes="Authorize with SIMKL; you'll be redirected back to the app.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "collections": {"read": False, "write": False},
                "ratings": {"read": True, "write": True, "scale": "1-10"},
                "watched": {"read": True, "write": True},
                "liked_lists": {"read": True, "write": False},
            },
            "entity_types": ["movie", "show"],
        }

    def get_status(self, cfg: Mapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        base: Mapping[str, Any] = {}
        blk: Mapping[str, Any] = {}

        s0 = cfg.get("simkl") if isinstance(cfg, Mapping) else None
        if isinstance(s0, Mapping):
            base = s0
            blk = s0
            if inst != "default":
                insts = s0.get("instances")
                sub = insts.get(inst) if isinstance(insts, Mapping) else None
                if isinstance(sub, Mapping):
                    blk = sub

        ok = bool((blk.get("access_token") or "").strip())
        return AuthStatus(
            connected=ok,
            label="SIMKL",
            user=blk.get("account") or None,
            scopes=blk.get("scopes") or None,
        )

    def _resolve_creds(self, cfg: MutableMapping[str, Any], instance_id: str | None) -> tuple[str, str, dict[str, Any]]:
        inst = normalize_instance_id(instance_id)
        if isinstance(cfg, dict):
            base = ensure_provider_block(cfg, "simkl")
            view_like = inst != "default" and "instances" not in base and bool(str(base.get("access_token") or "").strip())
            if view_like:
                blk = base
            else:
                blk = ensure_instance_block(cfg, "simkl", inst)
                if base.get("client_id") and not blk.get("client_id"):
                    blk["client_id"] = base.get("client_id")
                if base.get("client_secret") and not blk.get("client_secret"):
                    blk["client_secret"] = base.get("client_secret")
                if base.get("client_id") and not blk.get("api_key"):
                    blk["api_key"] = base.get("client_id")

            client_id = str((blk.get("client_id") or "")).strip() or str((base.get("client_id") or "")).strip()
            client_secret = str((blk.get("client_secret") or "")).strip() or str((base.get("client_secret") or "")).strip()
            return client_id, client_secret, blk

        s0 = cfg.get("simkl") if isinstance(cfg, dict) else (cfg.get("simkl") if hasattr(cfg, "get") else None)
        base = dict(s0 or {}) if isinstance(s0, Mapping) else {}
        client_id = str(base.get("client_id") or "").strip()
        client_secret = str(base.get("client_secret") or "").strip()
        return client_id, client_secret, base

    def start(
        self,
        cfg: MutableMapping[str, Any],
        redirect_uri: str,
        instance_id: str | None = None,
        state: str = "",
    ) -> dict[str, Any]:
        client_id, _, _ = self._resolve_creds(cfg, instance_id)
        verifier, challenge = _pkce_pair()
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "scope": OAUTH2_SCOPE,
            "code_challenge": challenge,
            "code_challenge_method": "S256",
        }
        if state:
            params["state"] = state
        url = f"{OAUTH2_AUTHORIZE}?{urlencode(params)}"
        inst = normalize_instance_id(instance_id)
        log("SIMKL: start OAuth", level="INFO", module="AUTH", extra={"instance": inst, "redirect_uri": redirect_uri})
        return {"url": url, "code_verifier": verifier}

    def finish(self, cfg: MutableMapping[str, Any], instance_id: str | None = None, **payload: Any) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        client_id, client_secret, target = self._resolve_creds(cfg, inst)

        data = {
            "grant_type": "authorization_code",
            "client_id": client_id,
            "client_secret": client_secret,
            "redirect_uri": payload.get("redirect_uri", ""),
            "code": payload.get("code", ""),
            "code_verifier": payload.get("code_verifier", ""),
        }
        log("SIMKL: exchange code", level="INFO", module="AUTH", extra={"instance": inst})
        status, tok, text = _post_form(OAUTH2_TOKEN, data)
        if status >= 400 or not str(tok.get("access_token") or "").strip():
            err = str(tok.get("error") or tok.get("error_description") or text or status)
            log(f"SIMKL: code exchange failed {status}: {err}", level="ERROR", module="AUTH", extra={"instance": inst})
            raise RuntimeError(f"SIMKL code exchange failed: {err}")

        _store_tokens(target, tok, client_id, "oauth")
        log("SIMKL: tokens stored", level="SUCCESS", module="AUTH", extra={"instance": inst})
        return self.get_status(cfg, inst)

    def refresh(
        self,
        cfg: MutableMapping[str, Any] | None = None,
        instance_id: str | None = None,
        *,
        margin_s: int = 0,
    ) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        with _refresh_lock(inst):
            cfgd: dict[str, Any] = load_config() or {}
            blk = ensure_instance_block(cfgd, "simkl", inst)
            version = auth_version(blk)
            if version == 1:
                return {"ok": True, "status": "legacy", "instance": inst}
            if version == 0:
                return {"ok": False, "status": "not_connected", "instance": inst}
            if margin_s and not needs_refresh(blk, margin_s):
                return {"ok": True, "status": "fresh", "expires_at": token_expires_at(blk), "instance": inst}

            refresh_token = str(blk.get("refresh_token") or "").strip()
            client_id = str(blk.get("client_id") or "").strip()
            if not (refresh_token and client_id):
                log("SIMKL: missing client_id/refresh_token for refresh", level="ERROR", module="AUTH", extra={"instance": inst})
                return {"ok": False, "status": "missing_refresh", "instance": inst}

            try:
                status, tok, text = _post_form(
                    OAUTH2_TOKEN,
                    {
                        "grant_type": "refresh_token",
                        "refresh_token": refresh_token,
                        "client_id": client_id,
                        "client_secret": str(blk.get("client_secret") or "").strip(),
                    },
                )
            except requests.RequestException as e:
                log(f"SIMKL: token refresh network error: {type(e).__name__}", level="ERROR", module="AUTH", extra={"instance": inst})
                return {"ok": False, "status": "network_error", "instance": inst}

            if status >= 400 or not str(tok.get("access_token") or "").strip():
                err = str(tok.get("error") or tok.get("error_description") or text or status)
                log(f"SIMKL: token refresh failed {status}: {err}", level="ERROR", module="AUTH", extra={"instance": inst})
                if err == "invalid_grant":
                    blk["auth_error"] = "reconnect_required"
                    save_config(cfgd)
                return {"ok": False, "status": f"refresh_failed:{status}", "error": err, "instance": inst}

            old_token = str(blk.get("access_token") or "")
            _store_tokens(blk, tok, client_id, str(blk.get("auth_method") or "pin"))
            transfer_token_state(old_token, str(blk["access_token"]))
            blk.pop("auth_error", None)
            save_config(cfgd)
            log("SIMKL: refresh ok", level="SUCCESS", module="AUTH", extra={"instance": inst})
            return {"ok": True, "status": "ok", "expires_at": token_expires_at(blk), "instance": inst}

    # PIN flow (https://api.simkl.org/api-reference/pin)
    def pin_start(self, cfg: MutableMapping[str, Any], *, instance_id: str | None = None) -> dict[str, Any]:
        if app_device_client_id():
            return self._device_start(cfg, instance_id=instance_id)
        inst = normalize_instance_id(instance_id)
        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        blk = ensure_instance_block(cfgd, "simkl", inst)
        cid = app_pin_client_id()
        if not cid:
            return {"ok": False, "error": "missing_client_id"}

        try:
            r = paced_request(requests.get, "GET",
                SIMKL_PIN,
                params=simkl_api_params(cid),
                headers={"Accept": "application/json", "User-Agent": simkl_user_agent()},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            return {"ok": False, "error": "network_error", "detail": str(e)}
        if r.status_code >= 400:
            return {"ok": False, "error": "http_error", "status": int(r.status_code), "body": (r.text or "")[:400]}
        try:
            data: dict[str, Any] = r.json() or {}
        except ValueError:
            return {"ok": False, "error": "invalid_json", "body": (r.text or "")[:400]}
        if str(data.get("result") or "").upper() != "OK":
            return {"ok": False, "error": "pin_request_failed", "body": str(data)[:200]}

        user_code = str(data.get("user_code") or "").strip()
        if not user_code:
            return {"ok": False, "error": "invalid_response"}
        verification_url = str(data.get("verification_url") or data.get("verification_uri") or PIN_VERIFY_URL).strip() or PIN_VERIFY_URL
        interval = int(data.get("interval") or 5)
        expires_in = int(data.get("expires_in") or 900)

        blk["_pending_pin"] = {
            "user_code": user_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_at": int(time.time()) + expires_in,
            "created_at": int(time.time()),
        }
        save_config(cfgd)
        log("SIMKL: PIN issued", level="INFO", module="AUTH", extra={"instance": inst})
        return {
            "ok": True,
            "user_code": user_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_in": expires_in,
        }

    def pin_poll(self, cfg: MutableMapping[str, Any], *, instance_id: str | None = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        blk = ensure_instance_block(cfgd, "simkl", inst)
        cid = app_pin_client_id()
        pend_raw = blk.get("_pending_pin")
        pend: dict[str, Any] = dict(pend_raw) if isinstance(pend_raw, Mapping) else {}
        if str(pend.get("device_code") or "").strip():
            return self._device_poll(cfgd, blk, pend, inst)
        user_code = str((pend or {}).get("user_code") or "").strip()
        if not user_code:
            return {"ok": False, "status": "no_pin"}
        if int((pend or {}).get("expires_at") or 0) and time.time() >= int((pend or {}).get("expires_at") or 0):
            blk.pop("_pending_pin", None)
            save_config(cfgd)
            return {"ok": False, "status": "expired"}

        try:
            r = paced_request(requests.get, "GET",
                f"{SIMKL_PIN}/{user_code}",
                params=simkl_api_params(cid),
                headers={"Accept": "application/json", "User-Agent": simkl_user_agent()},
                timeout=HTTP_TIMEOUT,
            )
        except requests.RequestException as e:
            return {"ok": False, "status": "network_error", "error": str(e)}
        if r.status_code >= 400:
            return {"ok": False, "status": f"http:{r.status_code}"}
        try:
            data: dict[str, Any] = r.json() or {}
        except ValueError:
            return {"ok": False, "status": "bad_json"}

        if str(data.get("result") or "").upper() != "OK":
            return {"ok": True, "status": "pending"}
        token = str(data.get("access_token") or "").strip()
        if not token:
            return {"ok": True, "status": "pending"}

        blk["access_token"] = token
        blk["client_id"] = cid
        blk["api_key"] = cid
        blk["auth_method"] = "pin"
        blk["auth_version"] = 1
        for k in ("refresh_token", "token_expires_at", "auth_error"):
            blk.pop(k, None)
        blk.pop("_pending_pin", None)
        save_config(cfgd)
        log("SIMKL: PIN token stored", level="SUCCESS", module="AUTH", extra={"instance": inst})
        return {"ok": True, "status": "authorized", "auth_method": "pin"}

    def _device_start(self, cfg: MutableMapping[str, Any], *, instance_id: str | None = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        blk = ensure_instance_block(cfgd, "simkl", inst)
        cid = app_device_client_id()
        try:
            status, data, text = _post_form(OAUTH2_DEVICE, {"client_id": cid, "scope": OAUTH2_SCOPE})
        except requests.RequestException as e:
            return {"ok": False, "error": "network_error", "detail": str(e)}
        if status >= 400:
            return {"ok": False, "error": "http_error", "status": status, "body": text}

        device_code = str(data.get("device_code") or "").strip()
        user_code = str(data.get("user_code") or "").strip()
        if not device_code or not user_code:
            return {"ok": False, "error": "invalid_response"}
        verification_url = (
            str(data.get("verification_uri_complete") or data.get("verification_uri") or PIN_VERIFY_URL).strip()
            or PIN_VERIFY_URL
        )
        interval = int(data.get("interval") or 5)
        expires_in = int(data.get("expires_in") or 900)
        now = int(time.time())

        blk["_pending_pin"] = {
            "device_code": device_code,
            "user_code": user_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_at": now + expires_in,
            "created_at": now,
        }
        save_config(cfgd)
        log("SIMKL: device code issued", level="INFO", module="AUTH", extra={"instance": inst})
        return {
            "ok": True,
            "user_code": user_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_in": expires_in,
        }

    def _device_poll(
        self,
        cfgd: dict[str, Any],
        blk: MutableMapping[str, Any],
        pend: dict[str, Any],
        inst: str,
    ) -> dict[str, Any]:
        if int(pend.get("expires_at") or 0) and time.time() >= int(pend.get("expires_at") or 0):
            blk.pop("_pending_pin", None)
            save_config(cfgd)
            return {"ok": False, "status": "expired"}

        cid = app_device_client_id()
        try:
            status, data, _ = _post_form(
                OAUTH2_TOKEN,
                {"grant_type": DEVICE_GRANT, "client_id": cid, "device_code": str(pend.get("device_code") or "")},
            )
        except requests.RequestException as e:
            return {"ok": False, "status": "network_error", "error": str(e)}

        err = str(data.get("error") or "").strip()
        if err == "authorization_pending":
            return {"ok": True, "status": "pending"}
        if err == "slow_down":
            pend["interval"] = int(pend.get("interval") or 5) + 5
            blk["_pending_pin"] = pend
            save_config(cfgd)
            return {"ok": True, "status": "slow_down", "interval": pend["interval"]}
        if err in ("expired_token", "access_denied", "invalid_grant"):
            blk.pop("_pending_pin", None)
            save_config(cfgd)
            return {"ok": False, "status": "expired" if err != "access_denied" else "denied"}
        if err == "invalid_client":
            blk.pop("_pending_pin", None)
            save_config(cfgd)
            log("SIMKL: device flow rejected client_id", level="ERROR", module="AUTH", extra={"instance": inst})
            return {"ok": False, "status": "invalid_client"}
        if status >= 400 or not str(data.get("access_token") or "").strip():
            return {"ok": False, "status": f"http:{status}"}

        _store_tokens(blk, data, cid, "pin")
        blk.pop("auth_error", None)
        blk.pop("_pending_pin", None)
        save_config(cfgd)
        log("SIMKL: device token stored", level="SUCCESS", module="AUTH", extra={"instance": inst})
        return {"ok": True, "status": "authorized", "auth_method": "pin", "auth_version": 2}

    def pin_cancel(self, cfg: MutableMapping[str, Any], *, instance_id: str | None = None) -> dict[str, Any]:
        inst = normalize_instance_id(instance_id)
        cfgd: dict[str, Any] = cfg if isinstance(cfg, dict) else dict(cfg)
        blk = ensure_instance_block(cfgd, "simkl", inst)
        existed = blk.pop("_pending_pin", None) is not None
        if existed:
            save_config(cfgd)
        return {"ok": True, "cancelled": existed}

    def disconnect(self, cfg: MutableMapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        if isinstance(cfg, dict):
            base = ensure_provider_block(cfg, "simkl")
            view_like = inst != "default" and "instances" not in base and bool(str(base.get("access_token") or "").strip())
            target = base if view_like else ensure_instance_block(cfg, "simkl", inst)
        else:
            target = cfg.setdefault("simkl", {})  # type: ignore[assignment]

        # If this profile was connected via PIN, clear the baked app id too so the
        # OAuth pane doesn't show it; leave user-supplied OAuth creds untouched.
        revoke_block(target)
        try:
            if str(target.get("client_id") or "").strip() in baked_client_ids():
                target.pop("client_id", None)
                target.pop("api_key", None)
        except Exception:
            pass

        for k in TOKEN_KEYS:
            try:
                target.pop(k, None)
            except Exception:
                pass

        try:
            if isinstance(cfg, dict):
                save_config(dict(cfg))
        except Exception:
            pass

        log("SIMKL: disconnected", level="INFO", module="AUTH", extra={"instance": inst})
        return self.get_status(cfg, inst)


PROVIDER = SimklAuth()
__all__ = ["PROVIDER", "SimklAuth", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-simkl">
  <style>
    #sec-simkl .inline{display:flex;gap:8px;align-items:center}
    #sec-simkl .inline .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-simkl .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-simkl .inline .msg.hidden{display:none}
    #sec-simkl .btn.danger{background:#a8182e;border-color:rgba(255,107,107,.4)}
    #sec-simkl .btn.danger:hover{filter:brightness(1.08)}

    /* Connect SIMKL */
    #sec-simkl #btn-connect-simkl,
    #sec-simkl #btn-connect-simkl-pin,
    #sec-simkl .btn.smk-connect{
      background: linear-gradient(135deg,#00e084,#2ea859) !important;
      border-color: rgba(0,224,132,.45) !important;
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff !important;
    }
    #sec-simkl #btn-connect-simkl:hover,
    #sec-simkl #btn-connect-simkl-pin:hover,
    #sec-simkl .btn.smk-connect:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }

    #sec-simkl .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}

    /* Method selector */
    #sec-simkl .hidden{display:none !important}
    #sec-simkl .smk-method-row{display:grid;grid-template-columns:minmax(0,1fr) auto;gap:12px;align-items:center;margin-top:14px}
    #sec-simkl .smk-methods{display:flex;gap:8px;min-width:0}
    #sec-simkl .smk-actions{display:flex;align-items:center;gap:8px;justify-content:flex-end;flex-wrap:wrap}
    #sec-simkl .smk-method{
      appearance:none;cursor:pointer;flex:1 1 0;padding:10px 12px;border-radius:10px;
      border:1px solid rgba(255,255,255,.14);background:rgba(255,255,255,.03);
      color:inherit;font:inherit;display:flex;align-items:center;justify-content:center;gap:8px;
      transition:border-color .15s ease, background .15s ease;
    }
    #sec-simkl .smk-method:hover{border-color:rgba(0,224,132,.5)}
    #sec-simkl .smk-method.active{
      border-color:rgba(0,224,132,.7);
      background:linear-gradient(135deg,rgba(0,224,132,.16),rgba(46,168,89,.10));
      box-shadow:0 0 12px rgba(0,224,132,.20);
    }
    #sec-simkl .smk-method .badge{
      display:inline-flex;align-items:center;line-height:1;
      font-size:.72em;text-transform:uppercase;letter-spacing:.05em;padding:3px 7px;border-radius:999px;
      background:rgba(0,224,132,.22);border:1px solid rgba(0,224,132,.45);color:#8ff0c2;
    }
    #sec-simkl .smk-pane{margin-top:12px}

    /* Quick Connect-style link-code card */
    #sec-simkl .smk-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.06)}
    #sec-simkl .smk-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-simkl .smk-qc-code{
      font-size:2em;font-weight:700;letter-spacing:.24em;padding:6px 0 6px .24em;color:#8ff0c2;
      text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums;
    }
    #sec-simkl .smk-qc-copy{
      appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;
      width:34px;height:34px;border-radius:9px;flex:0 0 auto;
      border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.08);color:#8ff0c2;
      transition:background .15s ease, border-color .15s ease, color .15s ease, transform .12s ease;
    }
    #sec-simkl .smk-qc-copy:hover{background:rgba(0,224,132,.16);border-color:rgba(0,224,132,.6)}
    #sec-simkl .smk-qc-copy:active{transform:scale(.94)}
    #sec-simkl .smk-qc-copy.copied{background:rgba(0,224,132,.24);border-color:rgba(0,224,132,.75)}
    #sec-simkl .smk-qc-copy svg{width:16px;height:16px;display:block}
    #sec-simkl .smk-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-simkl .smk-oauth-status{margin-top:10px;color:var(--muted)}
    @media(max-width:900px){#sec-simkl .smk-method-row{grid-template-columns:1fr}#sec-simkl .smk-actions{justify-content:flex-start}}
  </style>

  <div class="head" data-toggle-section="sec-simkl">
    <span class="chev"></span><strong>SIMKL</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="simkl">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">SIMKL</div>
            <div class="muted">Connect your SIMKL account for watchlist/ratings sync.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:15,182,222;--cw-auth-c2:15,182,222;--cw-auth-logo:url('/assets/img/SIMKL.svg')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to SIMKL</div>
                <div class="cw-auth-journey-copy">Connect with a PIN code (recommended) &mdash; CrossWatch shows a short code you enter at simkl.com/pin, no keys needed. OAuth with your own SIMKL app credentials remains available.</div>
              </div>
            </div>

            <div class="smk-method-row">
              <div class="smk-methods" role="tablist" aria-label="Authentication method">
                <button type="button" class="smk-method active" data-method="pin" role="tab" aria-selected="true">
                  PIN Flow <span class="badge">Recommended</span>
                </button>
                <button type="button" class="smk-method" data-method="oauth" role="tab" aria-selected="false">
                  OAuth (Client ID)
                </button>
              </div>
              <div class="smk-actions smk-actions-pin" data-method-actions="pin">
                <button id="btn-connect-simkl-pin" class="btn smk-connect" type="button">Connect SIMKL</button>
                <button id="btn-simkl-pin-cancel" class="btn danger hidden" type="button">Cancel</button>
                <button id="btn-simkl-pin-restart" class="btn hidden" type="button">Restart</button>
                <button id="btn-delete-simkl" class="btn danger" type="button">Delete</button>
              </div>
              <div class="smk-actions smk-actions-oauth hidden" data-method-actions="oauth">
                <button id="btn-connect-simkl" class="btn smk-connect" type="button">Connect SIMKL</button>
                <button id="btn-delete-simkl-oauth" class="btn danger" type="button">Delete</button>
                <span id="simkl-countdown" style="min-width:60px;"></span>
              </div>
            </div>
            <input id="simkl_auth_method" name="simkl_auth_method" type="hidden" value="pin">

            <div id="simkl_pin_panel" class="smk-pane" data-method="pin">
              <input id="simkl_pin_code" type="hidden">
              <div id="simkl_qc_state" class="smk-qc hidden">
                <div class="smk-qc-codewrap">
                  <div class="smk-qc-code" id="simkl_qc_code">------</div>
                  <button type="button" id="simkl_qc_copy" class="smk-qc-copy" title="Copy code" aria-label="Copy code">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                  </button>
                </div>
                <div class="sub" id="simkl_qc_help">Opening simkl.com/pin &mdash; enter this code there and approve CrossWatch.</div>
                <div class="smk-qc-meta">
                  <span class="sub" id="simkl_qc_status">Waiting for authorization&hellip;</span>
                  <span class="sub" id="simkl_qc_timer"></span>
                </div>
              </div>
            </div>

            <div id="simkl_oauth_panel" class="smk-pane" data-method="oauth" style="display:none">
              <div class="grid2">
                <div>
                  <label for="simkl_client_id">Client ID</label>
                  <input id="simkl_client_id" name="simkl_client_id" placeholder="Your SIMKL client id" autocomplete="off" spellcheck="false" autocapitalize="off">
                </div>
                <div>
                  <label for="simkl_client_secret">Client Secret</label>
                  <input id="simkl_client_secret" name="simkl_client_secret" placeholder="Your SIMKL client secret" type="password" autocomplete="off" spellcheck="false" autocapitalize="off" data-lpignore="true" data-1p-ignore="true" data-bwignore="true">
                </div>
              </div>

              <div id="simkl_hint" class="msg warn hidden" style="margin-top:8px">
                You need a SIMKL AUTH V2 app of type "Server apps &amp; services". Create one at
                <a href="https://simkl.com/settings/developer/" target="_blank" rel="noopener">SIMKL Developer</a>.
                Old V1 keys will not work.
                Set the Redirect URL to <code id="redirect_uri_preview"></code>.
                <button id="btn-copy-simkl-redirect" class="btn" type="button" style="margin-left:8px">Copy Redirect URL</button>
              </div>

              <div id="simkl-status" class="text-sm smk-oauth-status">Opens SIMKL authorize; callback returns here</div>
            </div>

            <div class="inline" style="margin-top:10px;justify-content:flex-end">
              <div id="simkl_msg" class="msg ok hidden">Connected</div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
'''

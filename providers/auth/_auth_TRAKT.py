# providers/auth/_auth_TRAKT.py
# CrossWatch - Trakt Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, ensure_provider_block, normalize_instance_id

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None

def _public_log_message(msg: Any) -> str:
    text = str(msg or "")
    if text == "TRAKT: request device code":
        return "TRAKT: request device code"
    if text == "TRAKT: device code received":
        return "TRAKT: device code received"
    if text == "TRAKT: exchange device code":
        return "TRAKT: exchange device code"
    if text == "TRAKT: tokens stored":
        return "TRAKT: tokens stored"
    if text == "TRAKT: missing client_id/client_secret/refresh_token for refresh":
        return "TRAKT: missing refresh credentials"
    if text == "TRAKT: refresh token":
        return "TRAKT: refresh token"
    if text == "TRAKT: refresh ok":
        return "TRAKT: refresh ok"
    if text.startswith("TRAKT: token refresh network error"):
        return "TRAKT: token refresh network error"
    if text.startswith("TRAKT: token refresh failed"):
        return "TRAKT: token refresh failed"
    if text.startswith("TRAKT: token refresh invalid JSON"):
        return "TRAKT: token refresh invalid JSON"
    if text == "TRAKT: token refresh succeeded but no access_token in response":
        return "TRAKT: token refresh succeeded without access token"
    if text.startswith("TRAKT[") and text.endswith("]: disconnected"):
        return "TRAKT: disconnected"
    return "TRAKT: auth event"


def log(msg: str, level: str = "INFO", module: str = "AUTH", **_: Any) -> None:
    public_msg = _public_log_message(msg)
    try:
        if _real_log is not None:
            _real_log(public_msg, level=level, module=module, **_)
        else:
            print(f"[{module}] {level}: {public_msg}")
    except Exception:
        pass


API = "https://api.trakt.tv"
OAUTH_DEVICE_CODE = f"{API}/oauth/device/code"
OAUTH_DEVICE_TOKEN = f"{API}/oauth/device/token"
OAUTH_TOKEN = f"{API}/oauth/token"
VERIFY_URL = "https://trakt.tv/activate"

__VERSION__ = "2.2.0"

_H: dict[str, str] = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "trakt-api-version": "2",
}

_REFRESH_LOCKS: dict[str, threading.Lock] = {}
_REFRESH_LOCKS_GUARD = threading.Lock()


def _now() -> int:
    return int(time.time())


def _load_config() -> dict[str, Any]:
    try:
        cfg = load_config()
        if isinstance(cfg, dict):
            return cfg
        return dict(cfg)
    except Exception:
        return {}


def _refresh_lock(instance_id: Any) -> threading.Lock:
    inst = normalize_instance_id(instance_id)
    with _REFRESH_LOCKS_GUARD:
        lock = _REFRESH_LOCKS.get(inst)
        if lock is None:
            lock = threading.Lock()
            _REFRESH_LOCKS[inst] = lock
        return lock


def _save_config(cfg: dict[str, Any]) -> None:
    try:
        save_config(cfg)
    except Exception:
        pass


def _blocks(cfg: dict[str, Any], instance_id: Any) -> tuple[str, dict[str, Any], dict[str, Any]]:
    inst = normalize_instance_id(instance_id)
    base = ensure_provider_block(cfg, "trakt")
    tr = ensure_instance_block(cfg, "trakt", inst)
    return inst, base, tr


def _client(cfg: dict[str, Any], instance_id: Any) -> dict[str, str]:
    inst, base, tr = _blocks(cfg, instance_id)
    src = base if inst == "default" else tr
    return {
        "client_id": str(src.get("client_id") or "").strip(),
        "client_secret": str(src.get("client_secret") or "").strip(),
    }


def _headers(token: str | None = None) -> dict[str, str]:
    h: dict[str, str] = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "trakt-api-version": "2",
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


class _TraktProvider:
    name = "TRAKT"
    label = "Trakt"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name=self.name,
            label=self.label,
            flow="device_pin",
            fields=[
                {"key": "trakt.client_id", "label": "Client ID", "type": "text", "required": True},
                {"key": "trakt.client_secret", "label": "Client Secret", "type": "password", "required": True},
            ],
            actions={"start": True, "finish": True, "refresh": True, "disconnect": True},
            verify_url=VERIFY_URL,
            notes="Open Trakt, enter the code, then return here. Client ID/Secret are required.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {
            "watchlist": True,
            "ratings": True,
            "history": True,
            "device_code": True,
            "refresh": True,
        }

    def get_status(self, cfg: dict[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfg = cfg or _load_config()
        inst, _, tr = _blocks(cfg, instance_id)
        token = str(tr.get("access_token") or "").strip()
        label = "Trakt" if inst == "default" else f"Trakt ({inst})"
        return AuthStatus(
            connected=bool(token),
            label=label,
            user=str(tr.get("username") or "") or None,
            expires_at=int(tr.get("expires_at") or 0) or None,
            scopes=[str(tr.get("scope") or "public")],
        )

    def html(self, cfg: dict[str, Any] | None = None) -> str:
        # HTML is static; multi-profile UI is injected by auth.trakt.js
        return r'''<div class="section" id="sec-trakt">
  <style>
    #sec-trakt .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-trakt .inline{display:flex;gap:8px;align-items:center}
    #sec-trakt .sub{opacity:.7;font-size:.92em}
    #sec-trakt .inline .msg{margin-left:auto}

    /* Connect TRAKT */
    #sec-trakt #btn-connect-trakt{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
    }
    #sec-trakt #btn-connect-trakt:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }

    /* Quick Connect-style link-code card */
    #sec-trakt .hidden{display:none !important}
    #sec-trakt .trk-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.06)}
    #sec-trakt .trk-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-trakt .trk-qc-code{
      font-size:2em;font-weight:700;letter-spacing:.24em;padding:6px 0 6px .24em;color:#8ff0c2;
      text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums;
    }
    #sec-trakt .trk-qc-copy{
      appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;
      width:34px;height:34px;border-radius:9px;flex:0 0 auto;
      border:1px solid rgba(0,224,132,.35);background:rgba(0,224,132,.08);color:#8ff0c2;
      transition:background .15s ease, border-color .15s ease, color .15s ease, transform .12s ease;
    }
    #sec-trakt .trk-qc-copy:hover{background:rgba(0,224,132,.16);border-color:rgba(0,224,132,.6)}
    #sec-trakt .trk-qc-copy:active{transform:scale(.94)}
    #sec-trakt .trk-qc-copy.copied{background:rgba(0,224,132,.24);border-color:rgba(0,224,132,.75)}
    #sec-trakt .trk-qc-copy svg{width:16px;height:16px;display:block}
    #sec-trakt .trk-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
  </style>

  <div class="head" data-toggle-section="sec-trakt">
    <span class="chev"></span><strong>Trakt</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="trakt">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">Trakt</div>
            <div class="muted">Connect your account and set API keys.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:225,20,60;--cw-auth-c2:159,66,198;--cw-auth-logo:url('/assets/img/TRAKT.svg')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to Trakt</div>
                <div class="cw-auth-journey-copy">Add your Trakt Client ID and Secret, then click Connect TRAKT and open trakt.tv/activate to enter the link code shown here. Once approved, CrossWatch can sync your Trakt watchlist, history and ratings.</div>
              </div>
            </div>

            <div class="grid2">
              <div>
                <label for="trakt_client_id">Client ID</label>
                <input id="trakt_client_id" name="trakt_client_id" placeholder="Enter your Trakt Client ID">
              </div>
              <div>
                <label for="trakt_client_secret">Client Secret</label>
                <input id="trakt_client_secret" name="trakt_client_secret" type="password" placeholder="Enter your Trakt Client Secret">
              </div>
            </div>

            <div id="trakt_hint" class="msg warn hidden" style="margin-top:8px">
              You need a Trakt API application. Create one at <a href="https://trakt.tv/oauth/applications" target="_blank" rel="noopener">Trakt Applications</a>
              Set the Redirect URL to <code id="trakt_redirect_uri_preview">urn:ietf:wg:oauth:2.0:oob</code>.
              <button id="btn-copy-trakt-redirect" class="btn" type="button" style="margin-left:8px">Copy Redirect URL</button>
            </div>

            <div class="sep"></div>

            <input id="trakt_pin" type="hidden">
            <div class="inline" style="margin-top:10px">
              <button id="btn-connect-trakt" class="btn" type="button">Connect TRAKT</button>
              <button id="btn-trakt-cancel" class="btn danger hidden" type="button">Cancel</button>
              <button id="btn-trakt-restart" class="btn hidden" type="button">Restart</button>
              <button id="btn-delete-trakt" class="btn danger" type="button">Delete</button>
              <div id="trakt_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>

            <div id="trakt_qc_state" class="trk-qc hidden">
              <div class="trk-qc-codewrap">
                <div class="trk-qc-code" id="trakt_qc_code">------</div>
                <button type="button" id="trakt_qc_copy" class="trk-qc-copy" title="Copy code" aria-label="Copy code">
                  <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                </button>
              </div>
              <div class="sub" id="trakt_qc_help">Opening trakt.tv/activate &mdash; enter this code there and approve CrossWatch.</div>
              <div class="trk-qc-meta">
                <span class="sub" id="trakt_qc_status">Waiting for authorization&hellip;</span>
                <span class="sub" id="trakt_qc_timer"></span>
              </div>
            </div>
          </div>
        </div>

      </div>
    </div>
  </div>
</div>
    '''

    def start(self, cfg: dict[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        cfg = cfg or _load_config()
        inst, _, tr = _blocks(cfg, instance_id)
        c = _client(cfg, inst)

        cid = (c.get("client_id") or "").strip()
        if not cid:
            return {"ok": False, "error": "missing_client_id"}

        log("TRAKT: request device code", level="INFO", module="AUTH")

        headers_primary = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "trakt-api-version": "2",
            "User-Agent": "CrossWatch/TraktAuth",
            "trakt-api-key": cid,
        }

        try:
            r = requests.post(OAUTH_DEVICE_CODE, json={"client_id": cid}, headers=headers_primary, timeout=20)
        except requests.RequestException as e:
            return {"ok": False, "error": "network_error", "detail": str(e)}

        if r.status_code != 200:
            return {"ok": False, "error": "http_error", "status": int(r.status_code), "body": (r.text or "")[:400]}

        try:
            data: dict[str, Any] = r.json() or {}
        except ValueError:
            return {"ok": False, "error": "invalid_json", "body": (r.text or "")[:400]}

        user_code = str(data.get("user_code") or "")
        device_code = str(data.get("device_code") or "")
        verification_url = str(data.get("verification_url") or VERIFY_URL)
        interval = int(data.get("interval", 5) or 5)
        expires_at = _now() + int(data.get("expires_in", 600) or 600)

        if not user_code or not device_code:
            return {"ok": False, "error": "invalid_response", "body": (r.text or "")[:400]}

        tr["_pending_device"] = {
            "user_code": user_code,
            "device_code": device_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_at": expires_at,
            "created_at": _now(),
        }
        _save_config(cfg)

        log("TRAKT: device code received", level="INFO", module="AUTH")
        return {
            "ok": True,
            "user_code": user_code,
            "device_code": device_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_at": expires_at,
        }

    def finish(self, cfg: dict[str, Any] | None = None, *, device_code: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        cfg = cfg or _load_config()
        inst, _, tr = _blocks(cfg, instance_id)
        c = _client(cfg, inst)
        if not c["client_id"] or not c["client_secret"]:
            return {"ok": False, "status": "missing_client"}

        pend = tr.get("_pending_device") or {}
        dc = (device_code or pend.get("device_code") or "").strip()
        if not dc:
            return {"ok": False, "status": "no_device_code"}
        if _now() >= int(pend.get("expires_at") or 0):
            return {"ok": False, "status": "expired_token"}

        log("TRAKT: exchange device code", level="INFO", module="AUTH")

        r = requests.post(
            OAUTH_DEVICE_TOKEN,
            json={"code": dc, "client_id": c["client_id"], "client_secret": c["client_secret"]},
            headers=_headers(),
            timeout=30,
        )

        if r.status_code == 429:
            try:
                retry_after = int(float(r.headers.get("Retry-After") or 0))
            except Exception:
                retry_after = 0
            return {"ok": False, "status": "slow_down", "retry_after": retry_after}

        if r.status_code == 410:
            return {"ok": False, "status": "expired_token"}
        if r.status_code == 404:
            return {"ok": False, "status": "not_found"}
        if r.status_code == 409:
            return {"ok": False, "status": "already_used"}
        if r.status_code == 418:
            return {"ok": False, "status": "access_denied"}

        if r.status_code >= 500:
            return {"ok": False, "status": "server_error", "status_code": int(r.status_code)}

        if r.status_code in (400, 401, 403):
            try:
                err = str((r.json() or {}).get("error") or "authorization_pending")
            except Exception:
                err = "authorization_pending"
            return {"ok": False, "status": err}

        r.raise_for_status()
        tok: dict[str, Any] = r.json() or {}

        tr.update(
            {
                "access_token": tok.get("access_token") or "",
                "refresh_token": tok.get("refresh_token") or "",
                "scope": tok.get("scope") or "public",
                "token_type": tok.get("token_type") or "bearer",
                "expires_at": _now() + int(tok.get("expires_in", 0) or 0),
            }
        )

        try:
            tr.pop("_pending_device", None)
        except Exception:
            pass

        _save_config(cfg)
        log("TRAKT: tokens stored", level="SUCCESS", module="AUTH")
        return {"ok": True, "status": "ok"}

    def refresh(self, cfg: dict[str, Any] | None = None, *, instance_id: Any = None) -> dict[str, Any]:
        inst_key = normalize_instance_id(instance_id)
        with _refresh_lock(inst_key):
            cfg = cfg or _load_config()
            inst, _, tr = _blocks(cfg, inst_key)
            c = _client(cfg, inst)

            try:
                exp = int(tr.get("expires_at") or 0)
            except Exception:
                exp = 0
            if str(tr.get("access_token") or "").strip() and exp and exp > (_now() + 120):
                return {"ok": True, "status": "fresh", "expires_at": exp}

            rt = str(tr.get("refresh_token") or "").strip()
            cid = str(c.get("client_id") or "").strip()
            secr = str(c.get("client_secret") or "").strip()

            if not (cid and secr and rt):
                log("TRAKT: missing client_id/client_secret/refresh_token for refresh", "ERROR")
                return {"ok": False, "status": "missing_refresh"}

            log("TRAKT: refresh token", level="INFO", module="AUTH")

            payload: dict[str, Any] = {
                "refresh_token": rt,
                "client_id": cid,
                "client_secret": secr,
                "grant_type": "refresh_token",
            }

            try:
                r = requests.post(OAUTH_TOKEN, json=payload, headers=_headers(), timeout=30)
            except Exception as e:
                log(f"TRAKT: token refresh network error: {e}", "ERROR")
                return {"ok": False, "status": "network_error", "error": str(e)}

            if r.status_code >= 400:
                body: dict[str, Any] = {}
                try:
                    body = r.json() or {}
                except Exception:
                    body = {}
                err = str(body.get("error") or "") or str(body.get("error_description") or "") or (r.text or "")[:400]
                log(f"TRAKT: token refresh failed {r.status_code}: {err}", "ERROR")
                return {"ok": False, "status": f"refresh_failed:{r.status_code}", "error": err}

            try:
                tok: dict[str, Any] = r.json() or {}
            except Exception as e:
                log(f"TRAKT: token refresh invalid JSON: {e}", "ERROR")
                return {"ok": False, "status": "bad_json"}

            acc = str(tok.get("access_token") or "").strip()
            if not acc:
                log("TRAKT: token refresh succeeded but no access_token in response", "ERROR")
                return {"ok": False, "status": "no_access_token"}

            new_rt = str(tok.get("refresh_token") or rt or "").strip()
            exp_in = int(tok.get("expires_in") or 0)
            expires_at = _now() + exp_in if exp_in > 0 else 0

            tr.update(
                {
                    "access_token": acc,
                    "refresh_token": new_rt,
                    "scope": tok.get("scope") or tr.get("scope") or "public",
                    "token_type": tok.get("token_type") or tr.get("token_type") or "bearer",
                    "expires_at": expires_at,
                }
            )
            _save_config(cfg)
            log("TRAKT: refresh ok", level="SUCCESS", module="AUTH")
            return {"ok": True, "status": "ok"}

    def disconnect(self, cfg: dict[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfg = cfg or _load_config()
        inst, _, tr = _blocks(cfg, instance_id)
        for key in ("access_token", "refresh_token", "scope", "token_type", "expires_at", "_pending_device"):
            tr.pop(key, None)
        _save_config(cfg)
        log(f"TRAKT[{inst}]: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg, instance_id=inst)


PROVIDER = _TraktProvider()
__all__ = ["PROVIDER", "_TraktProvider", "html", "__VERSION__"]


def html() -> str:
    try:
        return PROVIDER.html({})
    except Exception:
        return PROVIDER.html(None)

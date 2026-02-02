# providers/auth/_auth_TRAKT.py
# CrossWatch - Trakt Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from typing import Any

import requests

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, ensure_provider_block, normalize_instance_id

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


API = "https://api.trakt.tv"
OAUTH_DEVICE_CODE = f"{API}/oauth/device/code"
OAUTH_DEVICE_TOKEN = f"{API}/oauth/device/token"
OAUTH_TOKEN = f"{API}/oauth/token"
VERIFY_URL = "https://trakt.tv/activate"

__VERSION__ = "1.0.0"

_H: dict[str, str] = {
    "Accept": "application/json",
    "Content-Type": "application/json",
    "trakt-api-version": "2",
}


def _now() -> int:
    return int(time.time())


def _load_config() -> dict[str, Any]:
    try:
        cfg = load_config()
        if isinstance(cfg, dict):
            return cfg
        return dict(cfg)
    except Exception:
        try:
            with open("config.json", "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}


def _save_config(cfg: dict[str, Any]) -> None:
    try:
        save_config(cfg)
    except Exception:
        try:
            with open("config.json", "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2, ensure_ascii=False)
        except Exception:
            pass


def _blocks(cfg: dict[str, Any], instance_id: Any) -> tuple[str, dict[str, Any], dict[str, Any]]:
    inst = normalize_instance_id(instance_id)
    base = ensure_provider_block(cfg, "trakt")
    tr = ensure_instance_block(cfg, "trakt", inst)
    # Ensure the instance dict is self-contained for pair config views.
    if base.get("client_id") and not tr.get("client_id"):
        tr["client_id"] = base.get("client_id")
    if base.get("client_secret") and not tr.get("client_secret"):
        tr["client_secret"] = base.get("client_secret")
    return inst, base, tr


def _client(cfg: dict[str, Any], instance_id: Any) -> dict[str, str]:
    _, base, tr = _blocks(cfg, instance_id)
    return {
        "client_id": str(tr.get("client_id") or base.get("client_id") or "").strip(),
        "client_secret": str(tr.get("client_secret") or base.get("client_secret") or "").strip(),
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

    def manifest(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "label": self.label,
            "flow": "device_pin",
            "fields": [
                {"key": "trakt.client_id", "label": "Client ID", "type": "text", "required": True},
                {"key": "trakt.client_secret", "label": "Client Secret", "type": "password", "required": True},
            ],
            "actions": {"start": True, "finish": True, "refresh": True, "disconnect": True},
            "verify_url": VERIFY_URL,
            "notes": "Open Trakt, enter the code, then return here. Client ID/Secret are required.",
        }

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
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-trakt')">
    <span class="chev">▶</span><strong>Trakt</strong>
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
            <div class="grid2">
              <div>
                <label>Client ID</label>
                <input id="trakt_client_id" placeholder="Enter your Trakt Client ID"
                  oninput="updateTraktHint()"
                  onchange="try{saveSetting('trakt.client_id', this.value); updateTraktHint();}catch(_){}">
              </div>
              <div>
                <label>Client Secret</label>
                <input id="trakt_client_secret" type="password" placeholder="Enter your Trakt Client Secret"
                  oninput="updateTraktHint()"
                  onchange="try{saveSetting('trakt.client_secret', this.value); updateTraktHint();}catch(_){}">
              </div>
            </div>

            <div id="trakt_hint" class="msg warn hidden" style="margin-top:8px">
              You need a Trakt API application. Create one at
              <a href="https://trakt.tv/oauth/applications" target="_blank" rel="noopener">Trakt Applications</a>.
              Set the Redirect URL to <code id="trakt_redirect_uri_preview">urn:ietf:wg:oauth:2.0:oob</code>.
              <button class="btn" style="margin-left:8px" onclick="copyTraktRedirect()">Copy Redirect URL</button>
            </div>

            <div class="sep"></div>

            <div class="grid2">
              <div>
                <label>Current token</label>
                <div style="display:flex;gap:8px">
                  <input id="trakt_token" placeholder="empty = not set" readonly>
                  <button id="btn-copy-trakt-token" class="btn copy" onclick="copyInputValue('trakt_token', this)">Copy</button>
                </div>
              </div>
              <div>
                <label>Link code (PIN)</label>
                <div style="display:flex;gap:8px">
                  <input id="trakt_pin" placeholder="" readonly>
                  <button id="btn-copy-trakt-pin" class="btn copy" onclick="copyInputValue('trakt_pin', this)">Copy</button>
                </div>
              </div>
            </div>

            <div class="inline" style="margin-top:10px">
              <button id="btn-connect-trakt" class="btn" onclick="requestTraktPin()">Connect TRAKT</button>
              <button class="btn danger" onclick="try{ traktDeleteToken && traktDeleteToken(); }catch(_){;}">Delete</button>
              <div class="sub">Open <a href="https://trakt.tv/activate" target="_blank" rel="noopener">trakt.tv/activate</a> and enter your code.</div>
              <div id="trakt_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
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
        cfg = cfg or _load_config()
        inst, _, tr = _blocks(cfg, instance_id)
        c = _client(cfg, inst)

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


PROVIDER = _TraktProvider()
__all__ = ["PROVIDER", "_TraktProvider", "html", "__VERSION__"]


def html() -> str:
    try:
        return PROVIDER.html({})
    except Exception:
        return PROVIDER.html(None)

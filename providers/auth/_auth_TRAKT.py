# providers/auth/_auth_TRAKT.py
# CrossWatch - Trakt Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from typing import Any

import requests

from cw_platform.config_base import load_config, save_config

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

def _post(url: str, json_payload: dict[str, Any], client_id: str, timeout: int = 20) -> dict[str, Any]:
    h = dict(_H)
    h["trakt-api-key"] = client_id
    try:
        r = requests.post(url, headers=h, json=json_payload, timeout=timeout)
    except Exception as e:
        return {"ok": False, "error": "network_error", "detail": str(e)}
    if not r.ok:
        text = ""
        try:
            text = r.text[:500]
        except Exception:
            pass
        return {"ok": False, "error": "http_error", "status": r.status_code, "body": text}
    try:
        return {"ok": True, "json": r.json()}
    except Exception:
        return {"ok": False, "error": "bad_json", "status": r.status_code, "body": r.text[:500]}


def _now() -> int:
    return int(time.time())


def _load_config() -> dict[str, Any]:
    # Keep legacy helper but delegate to central config handling
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
    # Keep legacy helper but delegate to central config handling
    try:
        save_config(cfg)
    except Exception:
        try:
            with open("config.json", "w", encoding="utf-8") as f:
                json.dump(cfg, f, indent=2, ensure_ascii=False)
        except Exception:
            pass


def _client(cfg: dict[str, Any]) -> dict[str, str]:
    tr = cfg.get("trakt") or {}
    return {
        "client_id": (tr.get("client_id") or "").strip(),
        "client_secret": (tr.get("client_secret") or "").strip(),
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
                {
                    "key": "trakt.client_id",
                    "label": "Client ID",
                    "type": "text",
                    "required": True,
                },
                {
                    "key": "trakt.client_secret",
                    "label": "Client Secret",
                    "type": "password",
                    "required": True,
                },
            ],
            "actions": {"start": True, "finish": True, "refresh": True, "disconnect": True},
            "verify_url": VERIFY_URL,
            "notes": "Open Trakt, enter the code, then return here. Client ID/Secret are required.",
        }

    def html(self, cfg: dict[str, Any] | None = None) -> str:
        return r'''<div class="section" id="sec-trakt">
    <style>
      #sec-trakt .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
      #sec-trakt .inline{display:flex;gap:8px;align-items:center}
      #sec-trakt .muted{opacity:.7;font-size:.92em}
      #sec-trakt .inline .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
      #sec-trakt .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
      #sec-trakt .inline .msg.hidden{display:none}
      #sec-trakt .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
      #sec-trakt .btn.danger:hover{ filter:brightness(1.08) }
    </style>
    <div class="head" onclick="toggleSection('sec-trakt')">
        <span class="chev"></span><strong>Trakt</strong>
    </div>
    <div class="body">

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

        <div id="trakt_hint" class="msg warn hidden" style="margin-top:6px">
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

        <div class="inline" style="margin-top:8px">
          <button class="btn" onclick="requestTraktPin()">Connect TRAKT</button>
          <button class="btn danger" onclick="try{ traktDeleteToken && traktDeleteToken(); }catch(_){;}">Delete</button>
          <div class="muted">Open <a href="https://trakt.tv/activate" target="_blank" rel="noopener">trakt.tv/activate</a> and enter your code.</div>
          <div id="trakt_msg" class="msg ok hidden" aria-live="polite" style="display:none"></div>
        </div>

        <script>
 
          (function(){
            const msg=document.getElementById('trakt_msg');
            const pin=document.getElementById('trakt_pin');
            const tok=document.getElementById('trakt_token');
            function show(text){ if(!msg) return; msg.textContent=text||''; msg.classList.remove('hidden'); msg.style.display=''; }
            function hide(){ if(!msg) return; msg.classList.add('hidden'); msg.style.display='none'; }
            window.traktMsg={show,hide};
            function tick(){
              const hasPin=!!(pin&&pin.value&&pin.value.trim().length);
              const hasTok=!!(tok&&tok.value&&tok.value.trim().length);
              if(hasPin && !hasTok){ show('Code generated — finish linking on Trakt'); }
              else { hide(); }
            }
            tick();
            setInterval(tick, 600);
          })();
        </script>

        <div class="sep"></div>
    </div>
    </div>
    '''

    def start(self, cfg: dict[str, Any] | None = None, *, redirect_uri: str | None = None) -> dict[str, Any]:
        cfg = cfg or _load_config()
        c = _client(cfg)

        cid = (c.get("client_id") or "").strip()
        if not cid:
            return {"ok": False, "error": "missing_client_id"}

        headers_primary = {
            "Accept": "application/json",
            "Content-Type": "application/json",
            "trakt-api-version": "2",
            "User-Agent": "CrossWatch/TraktAuth",
        }

        def _call(headers: dict[str, str]) -> tuple[requests.Response | None, int, str, dict[str, str]]:
            try:
                r = requests.post(
                    OAUTH_DEVICE_CODE,
                    json={"client_id": cid},
                    headers=headers,
                    timeout=20,
                )
                return r, r.status_code, (r.text or ""), dict(r.headers or {})
            except requests.RequestException as e:
                return None, 0, str(e), {}

        r, status, text, hdrs = _call(headers_primary)

        if status != 200 or not r:
            body = text if isinstance(text, str) else str(text)
            return {
                "ok": False,
                "error": "http_error",
                "status": int(status),
                "body": body[:400],
                "cf_ray": hdrs.get("CF-RAY"),
                "content_type": hdrs.get("Content-Type"),
            }

        try:
            data: dict[str, Any] = r.json() or {}
        except ValueError:
            return {"ok": False, "error": "invalid_json", "body": (text[:400] if text else "")}

        user_code = data.get("user_code") or ""
        device_code = data.get("device_code") or ""
        verification_url = data.get("verification_url") or VERIFY_URL
        interval = int(data.get("interval", 5) or 5)
        expires_at = _now() + int(data.get("expires_in", 600) or 600)

        if not user_code or not device_code:
            body = text if text else str(data)
            return {"ok": False, "error": "invalid_response", "body": body[:400]}

        pend = {
            "user_code": user_code,
            "device_code": device_code,
            "verification_url": verification_url,
            "interval": interval,
            "expires_at": expires_at,
            "created_at": _now(),
        }
        cfg.setdefault("trakt", {})["_pending_device"] = pend
        _save_config(cfg)

        out = dict(pend)
        out["ok"] = True
        return out

    def finish(self, cfg: dict[str, Any] | None = None, *, device_code: str | None = None) -> dict[str, Any]:
        cfg = cfg or _load_config()
        c = _client(cfg)
        if not c["client_id"] or not c["client_secret"]:
            return {"ok": False, "status": "missing_client"}

        pend = (cfg.get("trakt") or {}).get("_pending_device") or {}
        dc = device_code or pend.get("device_code")
        if not dc:
            return {"ok": False, "status": "no_device_code"}
        if _now() >= int(pend.get("expires_at") or 0):
            return {"ok": False, "status": "expired_token"}

        r = requests.post(
            OAUTH_DEVICE_TOKEN,
            json={"code": dc, "client_id": c["client_id"], "client_secret": c["client_secret"]},
            headers=_headers(),
            timeout=30,
        )
        if r.status_code in (400, 401, 403):
            try:
                err = r.json().get("error") or "authorization_pending"
            except Exception:
                err = "authorization_pending"
            return {"ok": False, "status": err}

        r.raise_for_status()
        tok: dict[str, Any] = r.json() or {}
        tr = cfg.setdefault("trakt", {})
        tr.update(
            {
                "access_token": tok.get("access_token"),
                "refresh_token": tok.get("refresh_token"),
                "scope": tok.get("scope") or "public",
                "token_type": tok.get("token_type") or "bearer",
                "expires_at": _now() + int(tok.get("expires_in", 0)),
            }
        )
        try:
            tr.pop("_pending_device", None)
        except Exception:
            pass
        try:
            ((cfg.get("auth") or {}).get("trakt") or {}).clear()
        except Exception:
            pass

        _save_config(cfg)
        return {"ok": True, "status": "ok"}

    def refresh(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        cfg = cfg or _load_config()
        c = _client(cfg)
        tr = (cfg.get("trakt") or {}) or {}

        rt = (tr.get("refresh_token") or "").strip()
        cid = (c.get("client_id") or "").strip()
        secr = (c.get("client_secret") or "").strip()

        if not (cid and secr and rt):
            log("TRAKT: missing client_id/client_secret/refresh_token for refresh", "ERROR")
            return {"ok": False, "status": "missing_refresh"}

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
            err = (
                str(body.get("error") or "")
                or str(body.get("error_description") or "")
                or (r.text or "")[:400]
            )
            log(f"TRAKT: token refresh failed {r.status_code}: {err}", "ERROR")
            return {"ok": False, "status": f"refresh_failed:{r.status_code}", "error": err}

        try:
            tok: dict[str, Any] = r.json() or {}
        except Exception as e:
            log(f"TRAKT: token refresh invalid JSON: {e}", "ERROR")
            return {"ok": False, "status": "bad_json"}

        acc = (tok.get("access_token") or "").strip()
        if not acc:
            log("TRAKT: token refresh succeeded but no access_token in response", "ERROR")
            return {"ok": False, "status": "no_access_token"}

        new_rt = (tok.get("refresh_token") or rt or "").strip()
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
        cfg["trakt"] = tr
        _save_config(cfg)
        log("TRAKT: token refreshed and persisted", "DEBUG")
        return {"ok": True, "status": "ok"}

PROVIDER = _TraktProvider()
__all__ = ["PROVIDER", "_TraktProvider", "html", "__VERSION__"]


def html() -> str:
    try:
        return PROVIDER.html({})
    except Exception:
        return PROVIDER.html(None)
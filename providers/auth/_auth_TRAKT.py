# providers/auth/_auth_TRAKT.py
from __future__ import annotations
import time, json
from typing import Any, Dict, Optional
import requests

API = "https://api.trakt.tv"
OAUTH_DEVICE_CODE = f"{API}/oauth/device/code"
OAUTH_DEVICE_TOKEN = f"{API}/oauth/device/token"
OAUTH_TOKEN = f"{API}/oauth/token"
VERIFY_URL = "https://trakt.tv/activate"

def _now() -> int:
    return int(time.time())

def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)

def _save_config(cfg: Dict[str, Any]) -> None:
    try:
        from crosswatch import save_config as _save
        _save(cfg)
    except Exception:
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2, ensure_ascii=False)

def _client(cfg: Dict[str, Any]) -> Dict[str, str]:
    tr = (cfg.get("trakt") or {})
    return {
        "client_id": (tr.get("client_id") or "").strip(),
        "client_secret": (tr.get("client_secret") or "").strip(),
    }

def _headers(token: Optional[str] = None) -> Dict[str, str]:
    h = {
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

    def manifest(self) -> Dict[str, Any]:
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

    # HTML for settings
    
    def html(self, cfg: Optional[Dict[str, Any]] = None) -> str:
        return r'''<div class="section" id="sec-trakt">
    <div class="head" onclick="toggleSection('sec-trakt')">
        <span class="chev"></span><strong>Trakt</strong>
    </div>
    <div class="body">

        <!-- App keys -->
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

        <!-- Hint block (only shown when missing ID/Secret) -->
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

        <div style="display:flex;gap:8px;margin-top:8px;align-items:center">
        <button class="btn" onclick="requestTraktPin()">Get Code</button>
        <div style="align-self:center;color:var(--muted)">
            Opens <a href="https://trakt.tv/activate" target="_blank" rel="noopener">trakt.tv/activate</a> and shows your code
        </div>
        </div>

        <div id="trakt_msg" class="msg ok hidden">Code generated — finish linking on Trakt</div>
        <div class="sep"></div>
    </div>
    </div>
    '''


    # ---------- Flow ----------
    def start(self, cfg: Optional[Dict[str, Any]] = None, *, redirect_uri: Optional[str] = None) -> Dict[str, Any]:
        cfg = cfg or _load_config()
        c = _client(cfg)
        if not c["client_id"]:
            raise RuntimeError("Trakt client_id is missing. Set it in Settings first.")
        r = requests.post(OAUTH_DEVICE_CODE, json={"client_id": c["client_id"]}, headers=_headers(), timeout=30)
        r.raise_for_status()
        data = r.json() or {}
        pend = {
            "user_code": data.get("user_code"),
            "device_code": data.get("device_code"),
            "verification_url": data.get("verification_url") or VERIFY_URL,
            "interval": int(data.get("interval", 5)),
            "expires_at": _now() + int(data.get("expires_in", 600)),
            "created_at": _now(),
        }
        cfg.setdefault("trakt", {})["_pending_device"] = pend
        _save_config(cfg)
        return dict(pend)

    def finish(self, cfg: Optional[Dict[str, Any]] = None, *, device_code: Optional[str] = None) -> Dict[str, Any]:
        cfg = cfg or _load_config()
        c = _client(cfg)
        if not c["client_id"] or not c["client_secret"]:
            return {"ok": False, "status": "missing_client"}
        pend = ((cfg.get("trakt") or {}).get("_pending_device") or {})
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
        tok = r.json() or {}
        auth = cfg.setdefault("auth", {}).setdefault("trakt", {})
        auth.update({
            "access_token": tok.get("access_token"),
            "refresh_token": tok.get("refresh_token"),
            "scope": tok.get("scope") or "public",
            "token_type": tok.get("token_type") or "bearer",
            "expires_at": _now() + int(tok.get("expires_in", 0)),
        })
        try:
            cfg["trakt"].pop("_pending_device", None)
        except Exception:
            pass
        _save_config(cfg)
        return {"ok": True, "status": "ok"}

    def refresh(self, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        cfg = cfg or _load_config()
        c = _client(cfg)
        auth = ((cfg.get("auth") or {}).get("trakt") or {})
        rt = auth.get("refresh_token")
        if not (c["client_id"] and c["client_secret"] and rt):
            return {"ok": False, "status": "missing_refresh"}
        r = requests.post(
            OAUTH_TOKEN,
            json={"refresh_token": rt, "client_id": c["client_id"], "client_secret": c["client_secret"], "grant_type": "refresh_token"},
            headers=_headers(),
            timeout=30,
        )
        if r.status_code >= 400:
            return {"ok": False, "status": f"refresh_failed:{r.status_code}"}
        tok = r.json() or {}
        auth.update({
            "access_token": tok.get("access_token"),
            "refresh_token": tok("refresh_token") if callable(tok) else tok.get("refresh_token") or rt,
            "scope": tok.get("scope") or "public",
            "token_type": tok.get("token_type") or "bearer",
            "expires_at": _now() + int(tok.get("expires_in", 0)),
        })
        _save_config(cfg)
        return {"ok": True, "status": "ok"}

    def disconnect(self, cfg: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        cfg = cfg or _load_config()
        try:
            ((cfg.get("auth") or {}).get("trakt") or {}).clear()
        except Exception:
            pass
        _save_config(cfg)
        return {"ok": True}

PROVIDER = _TraktProvider()

def html() -> str:
    try:
        return PROVIDER.html({})
    except Exception:
        return PROVIDER.html(None)

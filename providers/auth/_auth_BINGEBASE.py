# providers/auth/_auth_BINGEBASE.py
# CrossWatch - BingeBase Authentication Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id, resolve_provider_block

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


API_BASE = "https://bingebase.com"
DEVICE_CODE_URL = f"{API_BASE}/api/v1/kodi/device/code"
DEVICE_TOKEN_URL = f"{API_BASE}/api/v1/kodi/device/token"
VERIFY_URL = f"{API_BASE}/activate"
KODI_WEBHOOK_PREFIX = f"{API_BASE}/webhooks/kodi/"
POLL_INTERVAL_SEC = 5
HTTP_TIMEOUT = 20
UA = "CrossWatch/BingeBaseAuth"
__VERSION__ = "0.1"

_TOKEN_KEYS = ("access_token", "username", "user_id")


class BingeBaseAuthError(RuntimeError):
    pass


def now() -> int:
    return int(time.time())


def _load_full_cfg() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
        return cfg if isinstance(cfg, dict) else dict(cfg)
    except Exception:
        return {}


def _save_full_cfg(cfg: dict[str, Any]) -> None:
    save_config(cfg)


def _headers(token: str | None = None) -> dict[str, str]:
    h = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": UA}
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _error_of(r: requests.Response) -> str:
    try:
        body = r.json() or {}
    except Exception:
        return ""
    if not isinstance(body, Mapping):
        return ""
    return str(body.get("error") or body.get("message") or "").strip()


def provider_block(cfg: Mapping[str, Any] | None, instance_id: Any = None) -> dict[str, Any]:
    try:
        block = resolve_provider_block(cfg or {}, "bingebase", instance_id) or {}
        return dict(block)
    except Exception:
        base = (cfg or {}).get("bingebase") if isinstance(cfg, Mapping) else None
        return dict(base or {}) if isinstance(base, Mapping) else {}


def writable_block(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return ensure_instance_block(cfg, "bingebase", instance_id)


def normalize_auth_method(value: Any = None, block: Mapping[str, Any] | None = None) -> str:
    return "device_code"


def active_method(block: Mapping[str, Any] | None = None) -> str:
    return "device_code"


def set_active_method(block: MutableMapping[str, Any], method: str = "device_code") -> str:
    block["auth_method"] = "device_code"
    return "device_code"


def clear_oauth(block: MutableMapping[str, Any]) -> None:
    for key in _TOKEN_KEYS:
        if key in block:
            block[key] = ""
    block.pop("_pending_device", None)
    block["auth_method"] = "device_code"


def is_configured(block: Mapping[str, Any] | None) -> bool:
    return bool(str((block or {}).get("access_token") or "").strip())


def _looks_masked_secret(value: Any) -> bool:
    text = str(value or "").strip()
    if not text:
        return False
    if text in {"********", "**********", "\u2022" * 8}:
        return True
    return len(text) >= 3 and all(ch in {"*", "\u2022"} for ch in text)


def sink_configured(block: Mapping[str, Any] | None) -> bool:
    webhook_url = str((block or {}).get("webhook_url") or "").strip()
    return bool(webhook_url) and not _looks_masked_secret(webhook_url)


def kodi_webhook_url(access_token: Any) -> str:
    token = str(access_token or "").strip()
    return f"{KODI_WEBHOOK_PREFIX}{token}" if token else ""


def _should_replace_webhook(value: Any) -> bool:
    text = str(value or "").strip()
    return not text or _looks_masked_secret(text) or text.startswith(KODI_WEBHOOK_PREFIX)


def ensure_default_webhook(block: MutableMapping[str, Any] | None) -> bool:
    if not isinstance(block, MutableMapping):
        return False
    access_token = str(block.get("access_token") or "").strip()
    if not access_token or not _should_replace_webhook(block.get("webhook_url")):
        return False
    webhook_url = kodi_webhook_url(access_token)
    if not webhook_url or str(block.get("webhook_url") or "").strip() == webhook_url:
        return False
    block["webhook_url"] = webhook_url
    return True


def status_for_block(block: Mapping[str, Any] | None) -> dict[str, Any]:
    b = block or {}
    out: dict[str, Any] = {
        "auth_method": "device_code",
        "connected": is_configured(b),
        "webhook_configured": sink_configured(b),
        "api_key_configured": bool(str(b.get("api_key") or "").strip()),
        "username": str(b.get("username") or ""),
        "user_id": str(b.get("user_id") or ""),
    }
    pend = b.get("_pending_device")
    if isinstance(pend, Mapping) and str(pend.get("user_code") or "").strip():
        out["pending"] = {
            "user_code": str(pend.get("user_code") or ""),
            "verification_uri": str(pend.get("verification_uri") or VERIFY_URL),
            "verification_uri_complete": str(pend.get("verification_uri_complete") or ""),
            "expires_at": int(pend.get("expires_at") or 0),
            "interval": int(pend.get("interval") or POLL_INTERVAL_SEC),
        }
    return out


def _apply_token_response(block: MutableMapping[str, Any], tok: Mapping[str, Any]) -> None:
    block["access_token"] = str(tok.get("access_token") or "").strip()
    block["auth_method"] = "device_code"
    ensure_default_webhook(block)
    username = str(tok.get("username") or tok.get("name") or "").strip()
    if username:
        block["username"] = username
    user_id = str(tok.get("user_id") or tok.get("id") or "").strip()
    if user_id:
        block["user_id"] = user_id


def start_device_code(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    timeout: float = HTTP_TIMEOUT,
    **_: Any,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    set_active_method(block)

    try:
        r = requests.post(DEVICE_CODE_URL, data=b"", headers=_headers(), timeout=timeout)
    except requests.RequestException as e:
        return {"ok": False, "error": "network_error", "detail": str(e), "instance": inst}

    if r.status_code >= 400:
        return {"ok": False, "error": _error_of(r) or "http_error", "status": int(r.status_code), "instance": inst}

    try:
        data: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "error": "invalid_json", "instance": inst}

    device_code = str(data.get("device_code") or "").strip()
    user_code = str(data.get("user_code") or "").strip()
    if not device_code or not user_code:
        return {"ok": False, "error": "invalid_response", "instance": inst}

    try:
        expires_in = int(data.get("expires_in") or 600)
    except Exception:
        expires_in = 600
    try:
        interval = int(data.get("interval") or POLL_INTERVAL_SEC)
    except Exception:
        interval = POLL_INTERVAL_SEC

    verification_uri = str(data.get("verification_uri") or data.get("verification_url") or VERIFY_URL).strip() or VERIFY_URL
    verification_uri_complete = str(data.get("verification_uri_complete") or "").strip()
    expires_at = now() + max(1, expires_in)

    block["_pending_device"] = {
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "verification_uri_complete": verification_uri_complete,
        "interval": max(1, interval),
        "expires_at": expires_at,
        "created_at": now(),
    }
    _save_full_cfg(cfgd)
    log(f"BINGEBASE: device code issued (instance={inst})", level="INFO", module="AUTH")
    return {
        "ok": True,
        "instance": inst,
        "device_code": device_code,
        "user_code": user_code,
        "verification_uri": verification_uri,
        "verification_uri_complete": verification_uri_complete,
        "interval": max(1, interval),
        "expires_in": expires_in,
        "expires_at": expires_at,
    }


def poll_device_code(
    cfg: dict[str, Any] | None = None,
    *,
    instance_id: Any = None,
    device_code: str | None = None,
    timeout: float = HTTP_TIMEOUT,
) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    pend = block.get("_pending_device") if isinstance(block.get("_pending_device"), Mapping) else {}
    dc = str(device_code or (pend or {}).get("device_code") or "").strip()
    if not dc:
        return {"ok": False, "status": "no_device_code", "instance": inst}
    pend_expires = int((pend or {}).get("expires_at") or 0)
    if pend_expires and now() >= pend_expires:
        block.pop("_pending_device", None)
        _save_full_cfg(cfgd)
        return {"ok": False, "status": "expired", "instance": inst}

    try:
        r = requests.post(DEVICE_TOKEN_URL, json={"device_code": dc}, headers=_headers(), timeout=timeout)
    except requests.RequestException as e:
        return {"ok": False, "status": "network_error", "error": str(e), "instance": inst}

    if r.status_code >= 500:
        return {"ok": False, "status": "server_error", "instance": inst}
    if r.status_code >= 400:
        err = _error_of(r) or "authorization_pending"
        if err in {"expired", "expired_token"}:
            block.pop("_pending_device", None)
            _save_full_cfg(cfgd)
        return {"ok": False, "status": err, "instance": inst}

    try:
        tok: dict[str, Any] = r.json() or {}
    except ValueError:
        return {"ok": False, "status": "bad_json", "instance": inst}

    access_token = str(tok.get("access_token") or "").strip()
    if not access_token:
        return {"ok": False, "status": "no_access_token", "instance": inst}

    _apply_token_response(block, tok)
    block.pop("_pending_device", None)
    _save_full_cfg(cfgd)
    log(f"BINGEBASE: access token stored (instance={inst})", level="SUCCESS", module="AUTH")
    webhook_url = str(block.get("webhook_url") or "").strip()
    result = {"ok": True, "status": "authorized", "instance": inst, "username": str(block.get("username") or "")}
    if webhook_url.startswith(KODI_WEBHOOK_PREFIX):
        result["generated_webhook_url"] = webhook_url
    return result


def cancel_device_code(cfg: dict[str, Any] | None = None, *, instance_id: Any = None) -> dict[str, Any]:
    cfgd = cfg if isinstance(cfg, dict) else _load_full_cfg()
    inst = normalize_instance_id(instance_id)
    block = writable_block(cfgd, inst)
    existed = block.pop("_pending_device", None) is not None
    if existed:
        _save_full_cfg(cfgd)
    return {"ok": True, "cancelled": existed, "instance": inst}


def refresh_token(cfg: dict[str, Any] | None = None, *, instance_id: Any = None, **_: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    block = provider_block(cfg if isinstance(cfg, Mapping) else _load_full_cfg(), inst)
    return {"ok": is_configured(block), "status": "unsupported" if not is_configured(block) else "fresh", "instance": inst}


def request_with_auth(
    session: requests.Session,
    method: str,
    url: str,
    *,
    cfg: Mapping[str, Any] | None,
    instance_id: Any = None,
    timeout: float = HTTP_TIMEOUT,
    **kwargs: Any,
) -> requests.Response:
    token = str(provider_block(cfg, instance_id).get("access_token") or "").strip()
    if not token:
        raise BingeBaseAuthError("missing_access_token")
    headers = dict(kwargs.pop("headers", {}) or {})
    headers.update(_headers(token))
    return session.request(method, url, headers=headers, timeout=timeout, **kwargs)


class BingeBaseAuth:
    name = "BINGEBASE"
    label = "BingeBase"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name=self.name,
            label=self.label,
            flow="device_code",
            fields=[],
            actions={"start": True, "finish": True, "disconnect": True},
            verify_url=VERIFY_URL,
            notes="Device code authentication plus an auto-filled Kodi realtime webhook URL with optional personal webhook override.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"device_code": True, "webhook_scrobble": True}

    def get_status(self, cfg: Mapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = cfg if cfg is not None else _load_full_cfg()
        inst = normalize_instance_id(instance_id)
        block = provider_block(cfgd, inst)
        label = "BingeBase" if inst == "default" else f"BingeBase ({inst})"
        return AuthStatus(connected=is_configured(block), label=label, user=str(block.get("username") or "") or None)

    def start(
        self,
        cfg: MutableMapping[str, Any] | None = None,
        redirect_uri: str | None = None,
        *,
        instance_id: Any = None,
    ) -> dict[str, Any]:
        cfgd = dict(cfg or _load_full_cfg())
        return start_device_code(cfgd, instance_id=instance_id)

    def finish(
        self,
        cfg: MutableMapping[str, Any] | None = None,
        *,
        instance_id: Any = None,
        **payload: Any,
    ) -> AuthStatus:
        cfgd = dict(cfg or _load_full_cfg())
        poll_device_code(cfgd, instance_id=instance_id, device_code=str(payload.get("device_code") or "").strip() or None)
        return self.get_status(_load_full_cfg(), instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg or _load_full_cfg(), instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_full_cfg())
        inst = normalize_instance_id(instance_id)
        block = writable_block(cfgd, inst)
        clear_oauth(block)
        block["webhook_url"] = ""
        block["api_key"] = ""
        _save_full_cfg(cfgd)
        log(f"BINGEBASE: disconnected (instance={inst})", level="INFO", module="AUTH")
        return self.get_status(cfgd, instance_id=inst)

    def html(self, cfg: Mapping[str, Any] | None = None) -> str:
        return html()


PROVIDER = BingeBaseAuth()
__all__ = ["PROVIDER", "BingeBaseAuth", "BingeBaseAuthError", "html", "__VERSION__"]


def html() -> str:
    return r'''<div class="section" id="sec-bingebase">
  <style>
    #sec-bingebase .sub{opacity:.7;font-size:.92em}
    #sec-bingebase .hidden{display:none !important}
    #sec-bingebase .bb-actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap;margin-top:18px}
    #sec-bingebase #bingebase_device_start{
      background: linear-gradient(135deg,#ff8533,#f8f1e8);
      border-color: rgba(255,133,51,.45);
      box-shadow: 0 0 14px rgba(255,133,51,.28);
      color: #1e1510;
    }
    #sec-bingebase #bingebase_device_start:hover{filter:brightness(1.04);box-shadow:0 0 18px rgba(255,133,51,.42)}
    #sec-bingebase .bb-qc{margin-top:12px;padding:14px;border-radius:12px;border:1px solid rgba(255,133,51,.35);background:rgba(255,133,51,.06)}
    #sec-bingebase .bb-qc-codewrap{display:flex;align-items:center;justify-content:center;gap:12px}
    #sec-bingebase .bb-qc-code{font-size:2em;font-weight:700;letter-spacing:.18em;padding:6px 0 6px .18em;color:#ffb978;text-align:center;text-transform:uppercase;font-variant-numeric:tabular-nums}
    #sec-bingebase .bb-qc-copy{appearance:none;cursor:pointer;display:inline-flex;align-items:center;justify-content:center;width:34px;height:34px;border-radius:9px;flex:0 0 auto;border:1px solid rgba(255,133,51,.35);background:rgba(255,133,51,.08);color:#ffb978;transition:background .15s ease,border-color .15s ease,color .15s ease,transform .12s ease}
    #sec-bingebase .bb-qc-copy:hover{background:rgba(255,133,51,.16);border-color:rgba(255,133,51,.6)}
    #sec-bingebase .bb-qc-copy:active{transform:scale(.94)}
    #sec-bingebase .bb-qc-copy.copied{background:rgba(255,133,51,.24);border-color:rgba(255,133,51,.75)}
    #sec-bingebase .bb-qc-copy svg{width:16px;height:16px;display:block}
    #sec-bingebase .bb-qc-meta{display:flex;justify-content:space-between;gap:12px;margin-top:6px}
    #sec-bingebase .bb-realtime-panel{display:grid;gap:10px;margin-top:-8px}
    #sec-bingebase .bb-realtime-grid{display:grid;grid-template-columns:repeat(2,minmax(0,1fr));gap:16px;align-items:start}
    #sec-bingebase .bb-secret-field{display:grid;gap:8px;min-width:0}
    #sec-bingebase .bb-secret-field input{width:100%;min-width:0}
    @media(max-width:760px){#sec-bingebase .bb-realtime-grid{grid-template-columns:1fr}}
  </style>

  <div class="head" data-toggle-section="sec-bingebase">
    <span class="chev"></span><strong>BingeBase</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="bingebase">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">BingeBase</div>
            <div class="muted">Connect BingeBase for authentication and realtime scrobbling.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="cw-auth-journey" style="--cw-auth-c1:255,133,51;--cw-auth-c2:248,241,232;--cw-auth-logo:url('/assets/img/BINGEBASE.png')">
              <div class="cw-auth-journey-text">
                <div class="cw-auth-journey-title">Connect to BingeBase</div>
                <div class="cw-auth-journey-copy">Click Connect BingeBase and approve the code at bingebase.com/activate.<br>BingeBase only provides scrobble.</div>
              </div>
            </div>

            <div id="bingebase_device_panel">
              <input id="bingebase_device_code" type="hidden">
              <div id="bingebase_qc_state" class="bb-qc hidden">
                <div class="bb-qc-codewrap">
                  <div class="bb-qc-code" id="bingebase_qc_code">----&ndash;----</div>
                  <button type="button" id="bingebase_qc_copy" class="bb-qc-copy" title="Copy code" aria-label="Copy code">
                    <svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><rect x="9" y="9" width="13" height="13" rx="2" ry="2"></rect><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"></path></svg>
                  </button>
                </div>
                <div class="sub" id="bingebase_qc_help">Opening bingebase.com/activate &mdash; enter this code there and approve CrossWatch.</div>
                <div class="bb-qc-meta">
                  <span class="sub" id="bingebase_qc_status">Waiting for approval&hellip;</span>
                  <span class="sub" id="bingebase_qc_timer"></span>
                </div>
              </div>
            </div>

            <div id="bingebase_realtime_panel" class="bb-realtime-panel">
              <div class="bb-realtime-grid">
                <div class="bb-secret-field">
                  <label for="bingebase_webhook_url">BingeBase webhook URL (optional override)</label>
                  <input id="bingebase_webhook_url" type="password" autocomplete="off" spellcheck="false" placeholder="Auto-filled from Kodi device auth">
                  <div id="bingebase_webhook_hint" class="sub">Device auth fills automatically the realtime webhook.</div>
                </div>

                <div class="bb-secret-field">
                  <label for="bingebase_api_key">Webhook API key / bearer token (optional)</label>
                  <input id="bingebase_api_key" type="password" autocomplete="off" spellcheck="false" placeholder="Bearer token, if required">
                  <div id="bingebase_api_key_hint" class="sub">Optional Authorization bearer token</div>
                </div>
              </div>
            </div>

            <div class="bb-actions">
              <button id="bingebase_device_start" class="btn" type="button">Connect BingeBase</button>
              <button id="bingebase_device_cancel" class="btn danger hidden" type="button">Cancel</button>
              <button id="bingebase_device_restart" class="btn hidden" type="button">Restart</button>
              <button id="bingebase_disconnect" class="btn danger" type="button">Delete</button>
              <div id="bingebase_msg" class="msg ok hidden" role="status" aria-live="polite"></div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
'''

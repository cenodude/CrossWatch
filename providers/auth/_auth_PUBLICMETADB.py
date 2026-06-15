# providers/auth/_auth_PUBLICMETADB.py
# CrossWatch - PublicMetaDB Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

import requests

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import get_provider_block, ensure_instance_block, normalize_instance_id

try:
    from _logging import log as _real_log
except ImportError:
    _real_log = None


API_BASE = "https://publicmetadb.com"
UA = "CrossWatch/1.0"
HTTP_TIMEOUT = 10
__VERSION__ = "0.0.1"


def log(msg: str, level: str = "INFO", module: str = "AUTH", **_: Any) -> None:
    try:
        if _real_log is not None:
            _real_log(msg, level=level, module=module, **_)
        else:
            print(f"[{module}] {level}: {msg}")
    except Exception:
        pass


def _load_config() -> dict[str, Any]:
    try:
        return dict(load_config() or {})
    except Exception:
        return {}


def _block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    return get_provider_block(cfg or {}, "publicmetadb", instance_id)


def validate_api_key(api_key: str, *, base_url: str = API_BASE, timeout: float = HTTP_TIMEOUT) -> tuple[bool, str]:
    key = str(api_key or "").strip()
    if not key:
        return False, "api_key_required"
    base = str(base_url or API_BASE).strip().rstrip("/") or API_BASE
    try:
        r = requests.get(
            f"{base}/api/external/lists",
            params={"page": 1, "perPage": 1},
            headers={"Accept": "application/json", "Authorization": f"Bearer {key}", "User-Agent": UA},
            timeout=timeout,
        )
    except requests.Timeout:
        return False, "validation_timeout"
    except requests.RequestException:
        return False, "validation_failed"
    if r.status_code in (401, 403):
        return False, "invalid_api_key"
    if r.status_code >= 400:
        return False, f"validation_http_{int(r.status_code)}"
    try:
        data = r.json() or {}
    except ValueError:
        return False, "validation_bad_response"
    if not isinstance(data, dict) or not isinstance(data.get("items"), list):
        return False, "validation_bad_response"
    return True, ""


def _status_from_key(cfg: Mapping[str, Any], instance_id: Any = None) -> AuthStatus:
    inst = normalize_instance_id(instance_id)
    b = _block(cfg, inst)
    has = bool(str(b.get("api_key") or "").strip())
    label = "PublicMetaDB" if inst == "default" else f"PublicMetaDB ({inst})"
    return AuthStatus(connected=has, label=label, user=None)


class PublicMetaDBAuth(AuthProvider):
    name = "PUBLICMETADB"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="PUBLICMETADB",
            label="PublicMetaDB",
            flow="api_keys",
            fields=[
                {
                    "key": "publicmetadb.api_key",
                    "label": "API Key",
                    "type": "password",
                    "required": True,
                    "placeholder": "pm-...",
                }
            ],
            actions={"start": False, "finish": True, "refresh": False, "disconnect": True},
            notes="Create an API key in PublicMetaDB Settings > API.",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True, "history": True}

    def get_status(self, cfg: Mapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return _status_from_key(cfg, instance_id)

    def start(self, cfg: MutableMapping[str, Any] | None = None, *, redirect_uri: str | None = None, instance_id: Any = None) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None, **payload: Any) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "publicmetadb", instance_id)
        key = str(payload.get("api_key") or payload.get("publicmetadb.api_key") or "").strip()
        if key:
            ok, reason = validate_api_key(key, base_url=str(b.get("base_url") or API_BASE).strip().rstrip("/"))
            if not ok:
                raise ValueError(reason)
            b["api_key"] = key
        save_config(cfgd)
        log(f"PublicMetaDB API key saved (instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)

    def refresh(self, cfg: MutableMapping[str, Any], *, instance_id: Any = None) -> AuthStatus:
        return self.get_status(cfg, instance_id=instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any] | None = None, *, instance_id: Any = None) -> AuthStatus:
        cfgd = dict(cfg or _load_config() or {})
        b = ensure_instance_block(cfgd, "publicmetadb", instance_id)
        b["api_key"] = ""
        save_config(cfgd)
        log(f"PublicMetaDB disconnected (instance={normalize_instance_id(instance_id)}).", module="AUTH")
        return self.get_status(cfgd, instance_id=instance_id)


def html() -> str:
    return r"""<div class="section" id="sec-publicmetadb">
  <style>
    #sec-publicmetadb .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-publicmetadb .inline{display:flex;gap:8px;align-items:center}
    #sec-publicmetadb .muted{opacity:.7;font-size:.92em}
    #sec-publicmetadb .inline .msg{margin-left:auto;padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-publicmetadb .inline .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-publicmetadb .inline .msg.hidden{display:none}
    #sec-publicmetadb .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-publicmetadb .btn.danger:hover{ filter:brightness(1.08) }
    #sec-publicmetadb #publicmetadb_save{
      background: linear-gradient(135deg,#16a34a,#22c55e);
      border-color: rgba(34,197,94,.45);
      box-shadow: 0 0 14px rgba(34,197,94,.32);
      color: #fff;
    }
  </style>

  <div class="head" data-toggle-section="sec-publicmetadb">
    <span class="chev"></span><strong>PublicMetaDB</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="publicmetadb">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">PublicMetaDB</div>
            <div class="muted">Connect and verify your PublicMetaDB API key.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="grid2">
              <div>
                <label for="publicmetadb_key">API Key</label>
                <div style="display:flex;gap:8px">
                  <input id="publicmetadb_key" name="publicmetadb_key" type="password" placeholder="pm-..." />
                  <button id="publicmetadb_save" class="btn">Connect</button>
                </div>
                <div id="publicmetadb_hint" class="msg warn" style="margin-top:8px">
                  Create an API key in
                  <a href="https://publicmetadb.com" target="_blank" rel="noopener">PublicMetaDB Settings &gt; API</a>.
                </div>
              </div>

              <div>
                <div class="field-label">Status</div>
                <div class="inline">
                  <button id="publicmetadb_disconnect" class="btn danger">Disconnect</button>
                  <div id="publicmetadb_msg" class="msg ok hidden" aria-live="polite"></div>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


PROVIDER = PublicMetaDBAuth()
__all__ = ["PROVIDER", "PublicMetaDBAuth", "html", "__VERSION__"]

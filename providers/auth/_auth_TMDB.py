# providers/auth/_auth_TMDB.py
# CrossWatch - TMDb Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import save_config
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


__VERSION__ = "2.0.0"


class TMDbAuth(AuthProvider):
    name = "TMDB"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="TMDB",
            label="TMDb",
            flow="api_keys",
            fields=[
                {
                    "key": "tmdb_sync.api_key",
                    "label": "API Key (v3)",
                    "type": "password",
                    "required": True,
                    "placeholder": "••••••••",
                },
                {
                    "key": "tmdb_sync.session_id",
                    "label": "Session ID (v3)",
                    "type": "password",
                    "required": True,
                    "placeholder": "session_id",
                },
            ],
            actions={"start": False, "finish": False, "refresh": True, "disconnect": True},
            notes="TMDb sync adapter auth (separate from Metadata TMDb).",
        )

    def capabilities(self) -> dict[str, Any]:
        return {"watchlist": True, "ratings": True}

    def get_status(self, cfg: Mapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        tm: Mapping[str, Any] = {}
        base = cfg.get("tmdb_sync") if isinstance(cfg, Mapping) else None
        if isinstance(base, Mapping):
            tm = base
            if inst != "default":
                insts = base.get("instances")
                blk = insts.get(inst) if isinstance(insts, Mapping) else None
                if isinstance(blk, Mapping):
                    tm = blk

        has_key = bool(str(tm.get("api_key") or "").strip())
        has_sess = bool(str(tm.get("session_id") or "").strip())
        return AuthStatus(connected=bool(has_key and has_sess), label="TMDb", user=None)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any], instance_id: str | None = None, **payload: Any) -> AuthStatus:
        key = (payload.get("api_key") or payload.get("tmdb_sync.api_key") or "").strip()
        sess = (payload.get("session_id") or payload.get("tmdb_sync.session_id") or "").strip()

        inst = normalize_instance_id(instance_id)
        if isinstance(cfg, dict):
            ensure_provider_block(cfg, "tmdb_sync")
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            tm["api_key"] = key
            tm["session_id"] = sess
            save_config(dict(cfg))
        else:
            tm = cfg.setdefault("tmdb_sync", {})
            if isinstance(tm, dict):
                tm["api_key"] = key
                tm["session_id"] = sess

        log(f"TMDb sync credentials saved ({inst}).", module="AUTH")
        return self.get_status(cfg, inst)

    def refresh(self, cfg: MutableMapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        return self.get_status(cfg, instance_id)

    def disconnect(self, cfg: MutableMapping[str, Any], instance_id: str | None = None) -> AuthStatus:
        inst = normalize_instance_id(instance_id)
        if isinstance(cfg, dict):
            ensure_provider_block(cfg, "tmdb_sync")
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            tm["api_key"] = ""
            tm["session_id"] = ""
            tm.pop("account_id", None)
            tm.pop("username", None)
            save_config(dict(cfg))
        else:
            tm = cfg.setdefault("tmdb_sync", {})
            if isinstance(tm, dict):
                tm["api_key"] = ""
                tm["session_id"] = ""
                tm.pop("account_id", None)
                tm.pop("username", None)

        log(f"TMDb disconnected ({inst}).", module="AUTH")
        return self.get_status(cfg, inst)

    def html(self) -> str:
        return _tmdb_sync_html()

PROVIDER = TMDbAuth()


def _tmdb_sync_html() -> str:
    return r"""<div class="section" id="sec-tmdb-sync">
  <style>
    #sec-tmdb-sync .grid2{display:grid;grid-template-columns:1fr 1fr;gap:12px}
    #sec-tmdb-sync .inline{display:flex;gap:8px;align-items:center}
    #sec-tmdb-sync .muted{opacity:.7;font-size:.92em}
    #sec-tmdb-sync .msg{padding:8px 12px;border-radius:12px;border:1px solid rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7;font-weight:600}
    #sec-tmdb-sync .msg.warn{border-color:rgba(255,210,0,.18);background:rgba(255,210,0,.08);color:#ffe9a6}
    #sec-tmdb-sync .msg.ok{border-color:rgba(0,255,170,.18);background:rgba(0,255,170,.08);color:#b9ffd7}
    #sec-tmdb-sync .msg.hidden{display:none}
    #sec-tmdb-sync .btn.danger{ background:#a8182e; border-color:rgba(255,107,107,.4) }
    #sec-tmdb-sync .btn.danger:hover{ filter:brightness(1.08) }
    #sec-tmdb-sync #tmdb_sync_connect{
      background: linear-gradient(135deg,#00e084,#2ea859);
      border-color: rgba(0,224,132,.45);
      box-shadow: 0 0 14px rgba(0,224,132,.35);
      color: #fff;
    }
    #sec-tmdb-sync #tmdb_sync_connect:hover{
      filter: brightness(1.06);
      box-shadow: 0 0 18px rgba(0,224,132,.5);
    }
  </style>

  <div class="head" onclick="toggleSection && toggleSection('sec-tmdb-sync')">
    <span class="chev">▶</span><strong>TMDb (Sync)</strong>
  </div>

  <div class="body">
    <div class="cw-panel">
      <div class="cw-meta-provider-panel active" data-provider="tmdb">
        <div class="cw-panel-head">
          <div>
            <div class="cw-panel-title">TMDb (Sync)</div>
            <div class="muted">Sync watchlist/ratings via TMDb v3 session.</div>
          </div>
        </div>

        <div class="cw-subtiles" style="margin-top:2px">
          <button type="button" class="cw-subtile active" data-sub="auth">Authentication</button>
        </div>

        <div class="cw-subpanels">
          <div class="cw-subpanel active" data-sub="auth">
            <div class="sub">Sync (watchlist/ratings) via TMDb v3 session. Metadata TMDb is configured separately.</div>
            
                <div class="grid2">
                  <div>
                    <label>API Key (v3)</label>
                    <div style="display:flex;gap:8px">
                      <input id="tmdb_sync_api_key" type="password" autocomplete="off" placeholder="••••••••" />
                      <button id="tmdb_sync_connect" class="btn">Connect</button>
                    </div>
                    <div id="tmdb_sync_hint" class="msg warn" style="margin-top:8px">
                      You need an TMDb API key. Create one at
                      <a href="https://www.themoviedb.org/settings/api" target="_blank" rel="noopener">TMDb Preferences</a>
                      and use the url: https://www.themoviedb.org/settings/api
                    </div>
                  </div>
            
                  <div>
                    <label>Session ID (v3)</label>
                    <input id="tmdb_sync_session_id" type="password" autocomplete="off" placeholder="Auto-filled after approval" />
                    <div class="muted">Required for account watchlists/ratings.</div>
                  </div>
                </div>
            
                <div style="margin-top:10px">
                  <label>Status</label>
                  <div class="inline">
                    <button id="tmdb_sync_verify" class="btn">Verify</button>
                    <button id="tmdb_sync_disconnect" class="btn danger">Disconnect</button>
                    <div id="tmdb_sync_msg" class="msg ok hidden" aria-live="polite" style="margin-left:auto"></div>
                  </div>
                </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</div>
"""


def html() -> str:
    return _tmdb_sync_html()

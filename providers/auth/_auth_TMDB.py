# providers/auth/_auth_TMDB.py
# CrossWatch - TMDb Auth Provider
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping, MutableMapping
from typing import Any

from ._auth_base import AuthManifest, AuthProvider, AuthStatus
from cw_platform.config_base import save_config

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


__VERSION__ = "1.0.0"


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

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        tm = cfg.get("tmdb_sync") or {}
        has_key = bool((tm.get("api_key") or "").strip()) if isinstance(tm, Mapping) else False
        has_sess = bool((tm.get("session_id") or "").strip()) if isinstance(tm, Mapping) else False
        return AuthStatus(connected=bool(has_key and has_sess), label="TMDb", user=None)

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, Any]:
        return {}

    def finish(self, cfg: MutableMapping[str, Any], **payload: Any) -> AuthStatus:
        key = (payload.get("api_key") or payload.get("tmdb_sync.api_key") or "").strip()
        sess = (payload.get("session_id") or payload.get("tmdb_sync.session_id") or "").strip()
        cfg.setdefault("tmdb_sync", {})["api_key"] = key
        cfg.setdefault("tmdb_sync", {})["session_id"] = sess
        save_config(dict(cfg))
        log("TMDb sync credentials saved.", module="AUTH")
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        tm = cfg.setdefault("tmdb_sync", {})
        tm["api_key"] = ""
        tm["session_id"] = ""
        tm.pop("account_id", None)
        tm.pop("username", None)
        save_config(dict(cfg))
        log("TMDb disconnected.", module="AUTH")
        return self.get_status(cfg)



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

    /* TMDb Connect (match MDBList look) */
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

  <div class="head" onclick="toggleSection('sec-tmdb-sync')">
    <span class="chev">▶</span><strong>TMDb (Sync)</strong>
  </div>

  <div class="body">
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
</div>"""


def html() -> str:
    return _tmdb_sync_html()

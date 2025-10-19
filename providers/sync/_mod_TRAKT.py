# /providers/sync/_mod_TRAKT.py
# Trakt adapter: manifest + client + OPS bridge.

from __future__ import annotations
__VERSION__ = "2.0.0"
__all__ = ["get_manifest", "TRAKTModule", "OPS"]

# stdlib
import os, time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, List, Callable

# shared trakt helpers + features
from .trakt._common import build_headers, normalize as trakt_normalize, key_of as trakt_key_of

try:
    from ..auth._auth_TRAKT import PROVIDER as AUTH_TRAKT  # token refresh hook
except Exception:
    from providers.auth._auth_TRAKT import PROVIDER as AUTH_TRAKT
    
from .trakt import _watchlist as feat_watchlist
try:
    from .trakt import _history as feat_history
except Exception:
    feat_history = None
try:
    from .trakt import _ratings as feat_ratings
except Exception:
    feat_ratings = None
try:
    from .trakt import _playlists as feat_playlists
except Exception:
    feat_playlists = None

# ---- common instrumentation (shared) ----------------------------------------
from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,
    label_trakt,
    make_snapshot_progress,
)
# orchestrator ctx (fallback if not injected)
try:  # type: ignore[name-defined]
    ctx  # type: ignore
except Exception:
    ctx = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# debug

def _log(msg: str):
    #Enable with CW_DEBUG=1 or CW_TRAKT_DEBUG=1
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_TRAKT_DEBUG"):
        print(f"[TRAKT] {msg}")

# ──────────────────────────────────────────────────────────────────────────────
# feature registry (register only what exists)
_FEATURES: Dict[str, Any] = {}
if feat_watchlist: _FEATURES["watchlist"] = feat_watchlist
if feat_history:   _FEATURES["history"]   = feat_history
if feat_ratings:   _FEATURES["ratings"]   = feat_ratings
if feat_playlists: _FEATURES["playlists"] = feat_playlists

def _features_flags() -> Dict[str, bool]:
    # Truth comes from actual modules present
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings":   "ratings"   in _FEATURES,
        "history":   "history"   in _FEATURES,
        "playlists": "playlists" in _FEATURES,
    }

# ──────────────────────────────────────────────────────────────────────────────
# manifest

def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "TRAKT",
        "label": "Trakt",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": _features_flags(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True, "unrate": True, "from_date": False
            },
        },
    }

# ──────────────────────────────────────────────────────────────────────────────
# config + client

@dataclass
class TRAKTConfig:
    client_id: str
    access_token: str
    timeout: float = 15.0
    max_retries: int = 3
    history_number_fallback: bool = False
    history_collection: bool = False

class TRAKTClient:
    BASE = "https://api.trakt.tv"

    def __init__(self, cfg: "TRAKTConfig", raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("TRAKT", ctx, feature_label=label_trakt)
        self._apply_headers(cfg.access_token)

    # --- internals ------------------------------------------------------------
    def _trakt_dict(self) -> Dict[str, Any]:
        try:
            return dict(self.raw_cfg.get("trakt") or {})
        except Exception:
            return {}

    def _apply_headers(self, access_token: Optional[str]):
        self.session.headers.update(build_headers({"trakt": {
            "client_id": self.cfg.client_id,
            "access_token": (access_token or "")
        }}))

    def _reload_token_from_cfg(self) -> str:
        tok = str(self._trakt_dict().get("access_token") or "").strip()
        if tok and tok != (self.cfg.access_token or ""):
            self.cfg.access_token = tok
            self._apply_headers(tok)
            _log("TRAKT: applied refreshed token")
        return tok

    def _about_to_expire(self, threshold: int = 120) -> bool:
        try:
            exp = int(self._trakt_dict().get("expires_at") or 0)
            now = int(__import__("time").time())
            return bool(exp and (exp - now) <= max(0, threshold))
        except Exception:
            return False

    def _try_refresh(self) -> bool:
        # Calls existing OAuth refresh in providers/auth/_auth_TRAKT.py
        try:
            res = AUTH_TRAKT.refresh(self.raw_cfg)
            ok = bool(isinstance(res, dict) and res.get("ok"))
            if ok:
                self._reload_token_from_cfg()
            else:
                _log(f"TRAKT: token refresh failed ({res!r})")
            return ok
        except Exception as e:
            _log(f"TRAKT: token refresh error: {e}")
            return False

    def _preflight(self):
        if self._about_to_expire():
            self._try_refresh()

    def _do(self, method: str, url: str, **kw):
        self._preflight()
        r = request_with_retries(self.session, method, url,
                                 timeout=self.cfg.timeout,
                                 max_retries=self.cfg.max_retries, **kw)
        if r.status_code in (401, 403):
            if self._try_refresh():
                r = request_with_retries(self.session, method, url,
                                         timeout=self.cfg.timeout,
                                         max_retries=self.cfg.max_retries, **kw)
        return r

    # --- public ---------------------------------------------------------------
    def connect(self) -> "TRAKTClient":
        try:
            r = self._do("GET", f"{self.BASE}/sync/last_activities")
            if r.status_code in (401, 403):
                raise TRAKTAuthError("Trakt auth failed")
            _log("Connected to Trakt API")
        except Exception as e:
            raise TRAKTError(f"Trakt connect failed: {e}") from e
        return self

    def get(self, url: str, **kw):
        return self._do("GET", url, **kw)

    def post(self, url: str, json: Mapping[str, Any], **kw):
        return self._do("POST", url, json=json, **kw)

    def delete(self, url: str, json: Optional[Mapping[str, Any]] = None, **kw):
        return self._do("DELETE", url, json=json, **kw)

# ──────────────────────────────────────────────────────────────────────────────
# errors

class TRAKTError(RuntimeError): pass
class TRAKTAuthError(TRAKTError): pass

# ──────────────────────────────────────────────────────────────────────────────
# adapter wrapper

class TRAKTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        t = dict((cfg.get("trakt") or {}))
        self.cfg = TRAKTConfig(
            client_id=str(t.get("client_id") or "").strip(),
            access_token=str(t.get("access_token") or "").strip(),
            timeout=float((t.get("timeout") or cfg.get("timeout") or 15.0)),
            max_retries=int((t.get("max_retries") or cfg.get("max_retries") or 3)),
            history_number_fallback=bool(t.get("history_number_fallback")),
            history_collection=bool(t.get("history_collection")),
        )
        if not self.cfg.client_id or not self.cfg.access_token:
            raise TRAKTAuthError("Missing Trakt client_id/access_token")

        if t.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_TRAKT_DEBUG", "1")
        self.client = TRAKTClient(self.cfg, cfg).connect()
        self.raw_cfg = cfg

        def _mk_prog(feature: str):
            try: return make_snapshot_progress(ctx, dst="TRAKT", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *a, **k): pass
                    def done(self, *a, **k): pass
                return _Noop()
        self.progress_factory: Callable[[str], Any] = _mk_prog

    # ---- feature toggles ------------------
    @staticmethod
    def supported_features() -> Dict[str, bool]:
        toggles = {
            "watchlist": True,
            "ratings":   True,
            "history":   True,
            "playlists": False,
        }
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    # shared delegates
    @staticmethod
    def normalize(obj) -> Dict[str, Any]: return trakt_normalize(obj)
    @staticmethod
    def key_of(obj) -> str: return trakt_key_of(obj)

    # health probe (standardized for orchestrator; probes only enabled features)
    def health(self) -> Mapping[str, Any]:
        """
        Uses instrumented session. Activities + watchlist probe if enabled.
        """
        enabled = self.supported_features()
        need_core = any(enabled.values())
        need_wl = bool(enabled.get("watchlist"))

        tmo = max(3.0, min(self.cfg.timeout, 15.0))
        base = self.client.BASE
        sess = self.client.session

        start = time.perf_counter()

        # ---- Core probe: /sync/last_activities
        core_ok = False
        core_reason: Optional[str] = None
        core_code: Optional[int] = None
        retry_after: Optional[int] = None
        rate = {"limit": None, "remaining": None, "reset": None}

        if need_core:
            try:
                r = request_with_retries(
                    sess, "GET", f"{base}/sync/last_activities",
                    timeout=tmo, max_retries=self.cfg.max_retries,
                )
                core_code = r.status_code
                if r.status_code in (401, 403):
                    core_reason = "unauthorized"
                elif 200 <= r.status_code < 300:
                    core_ok = True
                else:
                    core_reason = f"http:{r.status_code}"
                ra = r.headers.get("Retry-After")
                if ra:
                    try: retry_after = int(ra)
                    except Exception: pass
                rate = parse_rate_limit(r.headers)
            except Exception as e:
                core_reason = f"exception:{e.__class__.__name__}"

        # ---- Watchlist read probe (only if enabled AND core_ok)
        wl_ok = False
        wl_reason: Optional[str] = None
        wl_code: Optional[int] = None
        if need_wl and core_ok:
            try:
                r2 = request_with_retries(
                    sess, "GET", f"{base}/sync/watchlist",
                    params={"limit": 1, "page": 1},
                    timeout=tmo, max_retries=self.cfg.max_retries,
                )
                wl_code = r2.status_code
                if 200 <= r2.status_code < 300:
                    wl_ok = True
                elif r2.status_code in (401, 403):
                    wl_reason = "unauthorized"
                elif r2.status_code == 429:
                    wl_reason = "rate_limited"
                    ra2 = r2.headers.get("Retry-After")
                    if ra2:
                        try: retry_after = int(ra2)
                        except Exception: pass
                else:
                    wl_reason = f"http:{r2.status_code}"
            except Exception as e:
                wl_reason = f"exception:{e.__class__.__name__}"

        latency_ms = int((time.perf_counter() - start) * 1000)

        # ---- Feature readiness (reflect only enabled features)
        features = {
            "watchlist": (core_ok and wl_ok) if (need_wl and "watchlist" in _FEATURES) else False,
            "ratings":   (core_ok)           if (enabled.get("ratings")   and "ratings"   in _FEATURES) else False,
            "history":   (core_ok)           if (enabled.get("history")   and "history"   in _FEATURES) else False,
            "playlists": (core_ok)           if (enabled.get("playlists") and "playlists" in _FEATURES) else False,
        }

        checks: List[bool] = []
        if need_core: checks.append(core_ok)
        if need_wl:   checks.append(wl_ok)

        core_auth_failed = need_core and (core_code in (401, 403) or core_reason == "unauthorized")
        wl_auth_failed   = need_wl and (wl_code in (401, 403) or wl_reason == "unauthorized")

        if not checks:
            status = "ok"  # nothing enabled → OK
        elif all(checks):
            status = "ok"
        elif any(checks):
            status = "degraded"
        else:
            status = "auth_failed" if (core_auth_failed or wl_auth_failed) else "down"

        ok = status in ("ok", "degraded")

        # Reasons / details
        details: Dict[str, Any] = {}
        disabled = [k for k, v in enabled.items() if not v]
        if disabled:
            details["disabled"] = disabled

        reasons = []
        if need_core and not core_ok:
            reasons.append(f"core:{core_reason or 'down'}")
        if need_wl and not wl_ok:
            reasons.append(f"watchlist:{wl_reason or 'down'}")
        if reasons:
            details["reason"] = "; ".join(reasons)
        if retry_after is not None:
            details["retry_after_s"] = retry_after

        api = {
            "last_activities": {
                "status": (core_code if need_core else None),
                "retry_after": (retry_after if need_core else None),
                "rate": rate if need_core else {"limit": None, "remaining": None, "reset": None},
            },
            "watchlist": {
                "status": (wl_code if need_wl else None),
                "retry_after": (retry_after if need_wl else None),
            },
        }

        _log(f"health status={status} ok={ok} latency_ms={latency_ms} reasons={details.get('reason')}")
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": api,
        }

    # dispatch
    def feature_names(self) -> Tuple[str, ...]:
        return tuple(k for k, v in self.supported_features().items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs) -> Dict[str, Dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"build_index skipped: feature disabled or missing: {feature}")
            return {}
        mod = _FEATURES.get(feature)
        return mod.build_index(self, **kwargs) if mod else {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool=False) -> Dict[str, Any]:
        lst = list(items)
        if not lst: return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"add skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run: return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        try:
            cnt, unresolved = mod.add(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            return {"ok": False, "error": str(e)}

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool=False) -> Dict[str, Any]:
        lst = list(items)
        if not lst: return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"remove skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run: return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        try:
            cnt, unresolved = mod.remove(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            return {"ok": False, "error": str(e)}

# ──────────────────────────────────────────────────────────────────────────────
# orchestrator bridge

class _TraktOPS:
    def name(self) -> str: return "TRAKT"
    def label(self) -> str: return "Trakt"
    def features(self) -> Mapping[str, bool]:
        return TRAKTModule.supported_features()
        
    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True, "unrate": True, "from_date": False
            },
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        """No I/O; Trakt is configured iff we have an access_token."""
        c  = cfg or {}
        tr = c.get("trakt") or {}
        au = (c.get("auth") or {}).get("trakt") or {}

        token = (
            tr.get("access_token")
            or tr.get("token")
            or (tr.get("oauth") or {}).get("access_token")
            or au.get("access_token")
            or au.get("token")
            or (au.get("oauth") or {}).get("access_token")
            or ""
        )
        return bool(str(token).strip())
    
    def _adapter(self, cfg: Mapping[str, Any]) -> TRAKTModule:
        return TRAKTModule(cfg)

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _TraktOPS()

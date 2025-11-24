# /providers/sync/_mod_MDBLIST.py
from __future__ import annotations
__VERSION__ = "1.0.1"
__all__ = ["get_manifest", "MDBLISTModule", "OPS"]

import os, time
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, List, Callable

try:
    from .mdblist import _watchlist as feat_watchlist
except Exception:
    feat_watchlist = None
try:
    from .mdblist import _ratings as feat_ratings
except Exception:
    feat_ratings = None

from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,
    make_snapshot_progress,
)

try:
    ctx  # type: ignore[name-defined]
except Exception:
    ctx = None  # type: ignore

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST] {msg}")

_FEATURES: Dict[str, Any] = {}
if feat_watchlist: _FEATURES["watchlist"] = feat_watchlist
if feat_ratings:   _FEATURES["ratings"]   = feat_ratings

def _features_flags() -> Dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings":   "ratings"   in _FEATURES,
        "history":   False,
        "playlists": False,
    }

def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "MDBLIST",
        "label": "MDBList",
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

@dataclass
class MDBLISTConfig:
    api_key: str
    timeout: float = 15.0
    max_retries: int = 3

class MDBLISTClient:
    BASE = "https://api.mdblist.com"

    def __init__(self, cfg: "MDBLISTConfig", raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("MDBLIST", ctx, feature_label="MDBLIST")

    def connect(self) -> "MDBLISTClient":
        if not self.cfg.api_key:
            raise MDBLISTAuthError("Missing MDBList api_key")
        _log("MDBList client initialized and ready")
        return self

    def get(self, url: str, **kw):
        return request_with_retries(
            self.session, "GET", url,
            timeout=self.cfg.timeout, max_retries=self.cfg.max_retries, **kw
        )

    def post(self, url: str, **kw):
        return request_with_retries(
            self.session, "POST", url,
            timeout=self.cfg.timeout, max_retries=self.cfg.max_retries, **kw
        )

class MDBLISTError(RuntimeError): pass
class MDBLISTAuthError(MDBLISTError): pass
class MDBLISTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        m = dict((cfg.get("mdblist") or {}))
        self.cfg = MDBLISTConfig(
            api_key=str(m.get("api_key") or "").strip(),
            timeout=float((m.get("timeout") or cfg.get("timeout") or 15.0)),
            max_retries=int((m.get("max_retries") or cfg.get("max_retries") or 3)),
        )
        if not self.cfg.api_key:
            raise MDBLISTAuthError("Missing MDBList api_key")
        if m.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_MDBLIST_DEBUG", "1")
        self.client = MDBLISTClient(self.cfg, cfg).connect()
        self.raw_cfg = cfg
        self.config = cfg

        def _mk_prog(feature: str):
            try: return make_snapshot_progress(ctx, dst="MDBLIST", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *a, **k): pass
                    def done(self, *a, **k): pass
                return _Noop()
        self.progress_factory: Callable[[str], Any] = _mk_prog

    @staticmethod
    def supported_features() -> Dict[str, bool]:
        toggles = {"watchlist": True, "ratings": True, "history": False, "playlists": False}
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))
    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()
    def health(self) -> Mapping[str, Any]:
        enabled = self.supported_features()
        need_any = any(enabled.values())
        if not need_any:
            return {"ok": True, "status": "ok", "latency_ms": 0, "features": {}, "details": {"disabled": ["watchlist","ratings"]}, "api": {}}
        tmo = max(3.0, min(self.cfg.timeout, 15.0))
        base = self.client.BASE
        sess = self.client.session
        start = time.perf_counter()

        wl_ok = False; wl_code = None; retry_after = None
        rate = {"limit": None, "remaining": None, "reset": None}
        try:
            r = request_with_retries(
                sess, "GET", f"{base}/watchlist/items",
                params={"apikey": self.cfg.api_key, "limit": 1, "offset": 0, "unified": 1},
                timeout=tmo, max_retries=self.cfg.max_retries,
            )
            wl_code = r.status_code
            if 200 <= r.status_code < 300:
                wl_ok = True
            elif r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    try: retry_after = int(ra)
                    except Exception: pass
            rate = parse_rate_limit(r.headers)
        except Exception:
            wl_ok = False
        latency_ms = int((time.perf_counter() - start) * 1000)
        features = {
            "watchlist": wl_ok if enabled.get("watchlist") else False,
            "ratings":   wl_ok if enabled.get("ratings") else False,
            "history":   False,
            "playlists": False,
        }
        checks = [features[k] for k in ("watchlist","ratings") if enabled.get(k)]
        if not checks:
            status = "ok"
        elif all(checks):
            status = "ok"
        elif any(checks):
            status = "degraded"
        else:
            status = "down"

        ok = status in ("ok","degraded")
        details: Dict[str, Any] = {}
        disabled = [k for k, v in enabled.items() if not v]
        if disabled: details["disabled"] = disabled
        if retry_after is not None:
            details["retry_after_s"] = retry_after
        api = {
            "watchlist": {"status": wl_code, "retry_after": retry_after, "rate": rate},
        }
        _log(f"health status={status} ok={ok} latency_ms={latency_ms}")
        return {
            "ok": ok, "status": status, "latency_ms": latency_ms,
            "features": features, "details": details or None, "api": api,
        }

    def feature_names(self) -> Tuple[str, ...]:
        return tuple(k for k, v in self.supported_features().items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs) -> Dict[str, Dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"build_index skipped (disabled/missing): {feature}")
            return {}
        mod = _FEATURES.get(feature)
        return mod.build_index(self, **kwargs) if mod else {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool=False) -> Dict[str, Any]:
        lst = list(items)
        if not lst: return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"add skipped (disabled/missing): {feature}")
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
            _log(f"remove skipped (disabled/missing): {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run: return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        try:
            cnt, unresolved = mod.remove(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            return {"ok": False, "error": str(e)}

class _MDBLISTOps:
    def name(self) -> str: return "MDBLIST"
    def label(self) -> str: return "MDBList"
    def features(self) -> Mapping[str, bool]:
        return MDBLISTModule.supported_features()
    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True, "unrate": True, "from_date": False
            },
        }
    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}; m = c.get("mdblist") or {}
        key = m.get("api_key") or m.get("apikey") or ""
        return bool(str(key).strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> MDBLISTModule:
        return MDBLISTModule(cfg)
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)
    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()
OPS = _MDBLISTOps()

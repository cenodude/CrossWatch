# /providers/sync/_mod_SIMKL.py
# CrossWatch SIMKL module
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations
__VERSION__ = "1.0.0"
__all__ = ["get_manifest", "SIMKLModule", "OPS"]

# stdlib
import os, time, json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, List
import requests

# shared instrumentation + helpers
from ._mod_common import (
    build_session,
    label_simkl,
    request_with_retries,
    parse_rate_limit,
    make_snapshot_progress,
)

# strict relative imports
from .simkl._common import build_headers, normalize as simkl_normalize, key_of as simkl_key_of

try:
    from .simkl import _watchlist as feat_watchlist
except Exception as e:
    feat_watchlist = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_SIMKL_DEBUG"):
        print(f"[SIMKL] failed to import watchlist: {e}")

try:
    from .simkl import _history as feat_history
except Exception as e:
    feat_history = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_SIMKL_DEBUG"):
        print(f"[SIMKL] failed to import history: {e}")

try:
    from .simkl import _ratings as feat_ratings
except Exception as e:
    feat_ratings = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_SIMKL_DEBUG"):
        print(f"[SIMKL] failed to import ratings: {e}")
        
# orchestrator ctx (fallback)
try:  # type: ignore[name-defined]
    ctx  # noqa: F401
except NameError:
    class _NullCtx:
        def emit(self, *args, **kwargs) -> None: pass
    ctx = _NullCtx()  # type: ignore[assignment]

# ──────────────────────────────────────────────────────────────────────────────
# errors

class SIMKLError(RuntimeError): ...
class SIMKLAuthError(SIMKLError): ...

# ──────────────────────────────────────────────────────────────────────────────
# debug / logging

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_SIMKL_DEBUG"):
        print(f"[SIMKL] {msg}")

STATE_DIR = "/config/.cw_state"
ACTIVITIES_SHADOW = f"{STATE_DIR}/simkl.activities.shadow.json"

def _json_load(path: str) -> Dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _json_save(path: str, data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass

# ──────────────────────────────────────────────────────────────────────────────
# feature registry

_FEATURES: Dict[str, Any] = {}
if feat_watchlist: _FEATURES["watchlist"] = feat_watchlist
if feat_history:   _FEATURES["history"]   = feat_history
if feat_ratings:   _FEATURES["ratings"]   = feat_ratings

def _features_flags() -> Dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings":   "ratings"   in _FEATURES,
        "history":   "history"   in _FEATURES,
        "playlists": False,
    }

def supported_features() -> Dict[str, bool]:
    # Toggle mask (then gated by presence)
    toggles = {
        "watchlist": True,
        "ratings":   True,
        "history":   True,
        "playlists": False,
    }
    present = _features_flags()
    return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

# ──────────────────────────────────────────────────────────────────────────────
# manifest

def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "SIMKL",
        "label": "SIMKL",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": supported_features(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "delta",      # present vs delta
            "observed_deletes": False,       # do not rely on observed deletions
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True, "unrate": True, "from_date": True,
            },
        },
    }

# ──────────────────────────────────────────────────────────────────────────────
# config + client

@dataclass
class SIMKLConfig:
    api_key: str
    access_token: str
    date_from: str = ""          # kept for backward-compat; features use watermarks now
    timeout: float = 15.0
    max_retries: int = 3

class SIMKLClient:
    BASE = "https://api.simkl.com"

    def __init__(self, cfg: SIMKLConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session: requests.Session = build_session("SIMKL", ctx, feature_label=label_simkl)
        self.session.headers.update(build_headers({
            "simkl": {"api_key": cfg.api_key, "access_token": cfg.access_token}
        }))

    def _request(self, method: str, url: str, **kw) -> requests.Response:
        return request_with_retries(
            self.session,
            method,
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )

    def connect(self) -> "SIMKLClient":
        return self

    def activities(self) -> Dict[str, Any]:
        try:
            r = self._request("POST", f"{self.BASE}/sync/activities")
            if r.ok:
                return r.json() if r.text else {}
            return {"status": r.status_code}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def normalize(obj) -> Dict[str, Any]:
        return simkl_normalize(obj)

    @staticmethod
    def key_of(obj) -> str:
        return simkl_key_of(obj)

# ──────────────────────────────────────────────────────────────────────────────
# module wrapper

class SIMKLModule:
    """Adapter used by the orchestrator and feature modules."""
    def __init__(self, cfg: Mapping[str, Any]):
        simkl_cfg = dict(cfg.get("simkl") or {})
        api_key = str(simkl_cfg.get("api_key") or simkl_cfg.get("client_id") or "").strip()
        access_token = str(simkl_cfg.get("access_token") or "").strip()
        date_from = str(simkl_cfg.get("date_from") or "").strip()

        self.cfg = SIMKLConfig(
            api_key=api_key,
            access_token=access_token,
            date_from=date_from,
            timeout=float(simkl_cfg.get("timeout", cfg.get("timeout", 15.0))),
            max_retries=int(simkl_cfg.get("max_retries", cfg.get("max_retries", 3))),
        )
        if not self.cfg.api_key or not self.cfg.access_token:
            raise SIMKLError("SIMKL requires both api_key (or client_id) and access_token")

        if simkl_cfg.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_SIMKL_DEBUG", "1")

        self.client = SIMKLClient(self.cfg, simkl_cfg).connect()
        self.raw_cfg = cfg
        
        # progress factory to features (adapter.progress_factory(...))
        self.progress_factory = lambda feature, total=None, throttle_ms=300: make_snapshot_progress(
            ctx, dst="SIMKL", feature=str(feature), total=total, throttle_ms=throttle_ms
        )

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        """Single-probe health: rely on /sync/activities for auth + reachability."""
        enabled = supported_features()
        need_core = any(enabled.values())

        base = self.client.BASE
        sess = self.client.session
        tmo = max(3.0, min(self.cfg.timeout, 15.0))
        start = time.perf_counter()

        core_ok = False
        core_reason: Optional[str] = None
        core_code: Optional[int] = None
        retry_after: Optional[int] = None
        rate = {"limit": None, "remaining": None, "reset": None}

        if need_core:
            try:
                r = sess.post(f"{base}/sync/activities", timeout=tmo)
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

        latency_ms = int((time.perf_counter() - start) * 1000)

        # Feature health
        features = {
            "watchlist": bool(enabled.get("watchlist") and "watchlist" in _FEATURES and core_ok),
            "ratings":   bool(enabled.get("ratings")   and "ratings"   in _FEATURES and core_ok),
            "history":   bool(enabled.get("history")   and "history"   in _FEATURES and core_ok),
            "playlists": False,
        }

        # Overall status derived only from the core probe.
        if not need_core:
            status = "ok"
        elif core_ok:
            status = "ok"
        else:
            status = "auth_failed" if (core_code in (401, 403) or core_reason == "unauthorized") else "down"

        ok = status in ("ok", "degraded")  # keep shape consistent with orchestrator

        details: Dict[str, Any] = {}
        if need_core and not core_ok:
            details["reason"] = f"core:{core_reason or 'down'}"
        if retry_after is not None:
            details["retry_after_s"] = retry_after

        api = {
            "activities": {
                "status": (core_code if need_core else None),
                "retry_after": (retry_after if need_core else None),
                "rate": rate if need_core else {"limit": None, "remaining": None, "reset": None},
            },
        }

        try:
            _json_save(ACTIVITIES_SHADOW, {"ts": int(time.time()), "data": {"status": core_code}})
        except Exception:
            pass

        _log(f"health status={status} ok={ok} latency_ms={latency_ms} reason={details.get('reason')}")
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": api,
        }

    def get_date_from(self) -> str:
        return self.cfg.date_from  # legacy access; features prefer watermarks via simkl._common

    @staticmethod
    def normalize(obj) -> Dict[str, Any]:
        return simkl_normalize(obj)

    @staticmethod
    def key_of(obj) -> str:
        return simkl_key_of(obj)

    def feature_names(self) -> Tuple[str, ...]:
        feats = supported_features()
        return tuple(k for k, v in feats.items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs) -> Dict[str, Dict[str, Any]]:
        feats = supported_features()
        if not feats.get(feature) or feature not in _FEATURES:
            _log(f"build_index skipped: feature disabled or missing: {feature}")
            return {}
        mod = _FEATURES.get(feature)
        return mod.build_index(self, **kwargs) if mod else {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> Dict[str, Any]:
        feats = supported_features()
        if not feats.get(feature) or feature not in _FEATURES:
            _log(f"add skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        items = list(items or [])
        if not items:
            return {"ok": True, "count": 0}
        if dry_run:
            return {"ok": True, "count": len(items), "dry_run": True}
        count, unresolved = _FEATURES[feature].add(self, items)
        return {"ok": True, "count": int(count), "unresolved": unresolved}

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> Dict[str, Any]:
        feats = supported_features()
        if not feats.get(feature) or feature not in _FEATURES:
            _log(f"remove skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        items = list(items or [])
        if not items:
            return {"ok": True, "count": 0}
        if dry_run:
            return {"ok": True, "count": len(items), "dry_run": True}
        count, unresolved = _FEATURES[feature].remove(self, items)
        return {"ok": True, "count": int(count), "unresolved": unresolved}

# ──────────────────────────────────────────────────────────────────────────────
# OPS bridge (orchestrator-facing)

class _SIMKLOPS:
    def name(self) -> str:
        return "SIMKL"

    def label(self) -> str:
        return "SIMKL"

    def features(self) -> Mapping[str, bool]:
        return supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "delta",
            "observed_deletes": False,
        }
        
    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        """No I/O; SIMKL is configured iff we have an access_token."""
        c  = cfg or {}
        sm = c.get("simkl") or {}
        au = (c.get("auth") or {}).get("simkl") or {}

        token = (
            sm.get("access_token")
            or sm.get("token")
            or (sm.get("oauth") or {}).get("access_token")
            or au.get("access_token")
            or au.get("token")
            or (au.get("oauth") or {}).get("access_token")
            or ""
        )
        return bool(str(token).strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> SIMKLModule:
        return SIMKLModule(cfg)

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _SIMKLOPS()

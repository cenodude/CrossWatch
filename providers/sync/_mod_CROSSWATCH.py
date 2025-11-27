# /providers/sync/_mod_CROSSWATCH.py
# Internal CrossWatch adapter: manifest + local snapshot bridge.

from __future__ import annotations
__VERSION__ = "0.1.0"
__all__ = ["get_manifest", "CROSSWATCHModule", "OPS"]

import os, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, Mapping, Optional, List, Callable

try:
    from .crosswatch import _watchlist as feat_watchlist
except Exception:
    feat_watchlist = None
try:
    from .crosswatch import _history as feat_history
except Exception:
    feat_history = None
try:
    from .crosswatch import _ratings as feat_ratings
except Exception:
    feat_ratings = None

# Progress helper (no HTTP here, just reuse the emitter)
try:
    from ._mod_common import make_snapshot_progress
except Exception:
    make_snapshot_progress = None  # type: ignore

# Orchestrator ctx (injected at runtime; safe fallback for direct import)
try:  # type: ignore[name-defined]
    ctx  # type: ignore
except Exception:
    ctx = None  # type: ignore

# ---------------------------------------------------------------------------
# Debug / logging

def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_CROSSWATCH_DEBUG"):
        print(f"[CROSSWATCH] {msg}")

# ---------------------------------------------------------------------------
# Feature registry

_FEATURES: Dict[str, Any] = {}
if feat_watchlist: _FEATURES["watchlist"] = feat_watchlist
if feat_history:   _FEATURES["history"]   = feat_history
if feat_ratings:   _FEATURES["ratings"]   = feat_ratings

def _features_flags() -> Dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "history":   "history"   in _FEATURES,
        "ratings":   "ratings"   in _FEATURES,
        "playlists": False,
    }

# ---------------------------------------------------------------------------
# Manifest

def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "CROSSWATCH",
        "label": "CrossWatch (local)",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": _features_flags(),
        "requires": [],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "observed_deletes": True,
            # Ratings are stored as-is; provider does not enforce any scale.
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
            "snapshots": {
                "root_dir_default": "/config/.cw_provider",
                "managed_by": "CrossWatch",
            },
        },
    }

# ---------------------------------------------------------------------------
# Config

@dataclass
class CROSSWATCHConfig:
    root_dir: str = "/config/.cw_provider"
    retention_days: int = 30
    auto_snapshot: bool = True
    max_snapshots: int = 64
    restore_watchlist: Optional[str] = "latest"
    restore_history: Optional[str] = "latest"
    restore_ratings: Optional[str] = "latest"
    @property
    def base_path(self) -> Path:
        return Path(self.root_dir)

# ---------------------------------------------------------------------------
# Module implementation

class CROSSWATCHModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.raw_cfg = cfg
        cw_cfg = dict((cfg.get("CrossWatch") or cfg.get("crosswatch") or {}) or {})

        def _bool(key: str, default: bool) -> bool:
            v = cw_cfg.get(key, default)
            if isinstance(v, bool):
                return v
            s = str(v).strip().lower()
            if s in ("1", "true", "yes", "on"):
                return True
            if s in ("0", "false", "no", "off"):
                return False
            return default

        def _int(key: str, default: int) -> int:
            try:
                return int(cw_cfg.get(key, default))
            except Exception:
                return int(default)

        root_dir = str(cw_cfg.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
        self.cfg = CROSSWATCHConfig(
            root_dir=root_dir,
            retention_days=_int("retention_days", 30),
            auto_snapshot=_bool("auto_snapshot", True),
            max_snapshots=_int("max_snapshots", 64),
            restore_watchlist=(cw_cfg.get("restore_watchlist") or "latest"),
            restore_history=(cw_cfg.get("restore_history") or "latest"),
            restore_ratings=(cw_cfg.get("restore_ratings") or "latest"),
        )

        # Ensure the base directory exists early.
        try:
            self.cfg.base_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            _log(f"failed to ensure provider dir: {e}")

        self.config = cfg

        def _mk_prog(feature: str):
            if make_snapshot_progress is None or ctx is None:
                class _Noop:
                    def tick(self, *a, **k): pass
                    def done(self, *a, **k): pass
                return _Noop()
            try:
                return make_snapshot_progress(ctx, dst="CROSSWATCH", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *a, **k): pass
                    def done(self, *a, **k): pass
                return _Noop()

        self.progress_factory: Callable[[str], Any] = _mk_prog

    # Shared utils for feature modules
    @staticmethod
    def supported_features() -> Dict[str, bool]:
        toggles = {
            "watchlist": True,
            "history":   True,
            "ratings":   True,
            "playlists": False,
        }
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    # Index / apply API used by orchestrator

    def build_index(self, feature: str, **kwargs) -> Dict[str, Dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"build_index skipped: feature disabled or missing: {feature}")
            return {}
        mod = _FEATURES.get(feature)
        return mod.build_index(self, **kwargs) if mod else {}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> Dict[str, Any]:
        lst: List[Mapping[str, Any]] = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"add skipped (disabled/missing): {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        try:
            cnt, unresolved = mod.add(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            _log(f"add error for {feature}: {e}")
            return {"ok": False, "error": str(e)}

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> Dict[str, Any]:
        lst: List[Mapping[str, Any]] = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"remove skipped (disabled/missing): {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        try:
            cnt, unresolved = mod.remove(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            _log(f"remove error for {feature}: {e}")
            return {"ok": False, "error": str(e)}

    # Simple health check: local filesystem only
    def health(self) -> Mapping[str, Any]:
        started = time.time()
        ok = True
        detail: Dict[str, Any] = {}
        try:
            base = self.cfg.base_path
            base.mkdir(parents=True, exist_ok=True)
            test = base / ".health.touch"
            test.write_text("ok", encoding="utf-8")
            try:
                test.unlink()
            except Exception:
                pass
        except Exception as e:
            ok = False
            detail["error"] = str(e)
        latency_ms = int((time.time() - started) * 1000)
        return {
            "ok": ok,
            "status": "ok" if ok else "error",
            "latency_ms": latency_ms,
            "features": self.supported_features(),
            "details": detail,
            "api": {},
        }

# ---------------------------------------------------------------------------
# OPS bridge

class _CrossWatchOPS:
    def name(self) -> str:
        return "CROSSWATCH"

    def label(self) -> str:
        return "CrossWatch"

    def features(self) -> Mapping[str, bool]:
        return CROSSWATCHModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "observed_deletes": True,
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        root = (cfg or {}).get("CrossWatch") or (cfg or {}).get("crosswatch") or {}
        if not isinstance(root, Mapping):
            return True
        v = root.get("enabled")
        if v is None:
            return True
        if isinstance(v, bool):
            return v
        s = str(v).strip().lower()
        return s not in ("0", "false", "no", "off", "disabled")

    def _adapter(self, cfg: Mapping[str, Any]) -> CROSSWATCHModule:
        return CROSSWATCHModule(cfg)

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> Dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _CrossWatchOPS()

# /providers/sync/_mod_CROSSWATCH.py
# CrossWatch CROSSWATCH tracker module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

try:
    from ._log import log as cw_log
except Exception:  # pragma: no cover
    def cw_log(provider: str, feature: str, level: str, msg: str, **fields: Any) -> None:  # type: ignore[no-redef]
        pass

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

try:
    from .crosswatch import _watchlist as feat_watchlist
except Exception as e:
    feat_watchlist = None
    cw_log("CROSSWATCH", "module", "warn", "feature_import_failed", import_feature="watchlist", error=str(e))

try:
    from .crosswatch import _history as feat_history
except Exception as e:
    feat_history = None
    cw_log("CROSSWATCH", "module", "warn", "feature_import_failed", import_feature="history", error=str(e))

try:
    from .crosswatch import _ratings as feat_ratings
except Exception as e:
    feat_ratings = None
    cw_log("CROSSWATCH", "module", "warn", "feature_import_failed", import_feature="ratings", error=str(e))

try:
    from ._mod_common import make_snapshot_progress
except Exception:
    make_snapshot_progress = None  # type: ignore[assignment]

__VERSION__ = "1.0.0"
__all__ = ["get_manifest", "CROSSWATCHModule", "OPS"]

_FEATURES: dict[str, Any] = {}
if feat_watchlist:
    _FEATURES["watchlist"] = feat_watchlist
if feat_history:
    _FEATURES["history"] = feat_history
if feat_ratings:
    _FEATURES["ratings"] = feat_ratings


def _dbg(feature: str, msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", feature, "debug", msg, **fields)


def _info(feature: str, msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", feature, "info", msg, **fields)


def _warn(feature: str, msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", feature, "warn", msg, **fields)


def _error(feature: str, msg: str, **fields: Any) -> None:
    cw_log("CROSSWATCH", feature, "error", msg, **fields)


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "history": "history" in _FEATURES,
        "ratings": "ratings" in _FEATURES,
        "playlists": False,
    }


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


@dataclass
class CROSSWATCHConfig:
    root_dir: str = "/config/.cw_provider"
    retention_days: int = 30
    auto_snapshot: bool = True
    max_snapshots: int = 64
    
    restore_watchlist: str | None = None
    restore_history: str | None = None
    restore_ratings: str | None = None

    @property
    def base_path(self) -> Path:
        return Path(self.root_dir)


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
            
        def _restore_id(key: str) -> str | None:
            v = cw_cfg.get(key)
            if v is None:
                return None
            s = str(v).strip()
            if not s or s.lower() == "latest":
                return None
            return s

        root_dir = str(cw_cfg.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
        self.cfg = CROSSWATCHConfig(
            root_dir=root_dir,
            retention_days=_int("retention_days", 30),
            auto_snapshot=_bool("auto_snapshot", True),
            max_snapshots=_int("max_snapshots", 64),
            restore_watchlist=_restore_id("restore_watchlist"),
            restore_history=_restore_id("restore_history"),
            restore_ratings=_restore_id("restore_ratings"),
        )

        try:
            self.cfg.base_path.mkdir(parents=True, exist_ok=True)
        except Exception as e:
            _warn("module", "provider_dir_create_failed", path=str(self.cfg.base_path), error=str(e))

        self.config = cfg

        class _Noop:
            def tick(self, *args: Any, **kwargs: Any) -> None:
                pass
            def done(self, *args: Any, **kwargs: Any) -> None:
                pass

        def _mk_prog(feature: str):
            if make_snapshot_progress is not None and ctx is not None:
                try:
                    return make_snapshot_progress(ctx, dst="CROSSWATCH", feature=feature)
                except Exception:
                    pass
            return _Noop()

        self.progress_factory: Callable[[str], Any] = _mk_prog

    @staticmethod
    def supported_features() -> dict[str, bool]:
        toggles = {
            "watchlist": True,
            "history": True,
            "ratings": True,
            "playlists": False,
        }
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _info(feature, "index_skipped", reason="disabled_or_missing")
            return {}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn(feature, "index_skipped", reason="module_missing")
            return {}
        started = time.perf_counter()
        out = mod.build_index(self, **kwargs)
        _info(feature, "index_done", count=len(out), dur_ms=int((time.perf_counter() - started) * 1000))
        return out

    def add(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _info(feature, "write_skipped", op="add", reason="disabled_or_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn(feature, "write_skipped", op="add", reason="module_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            started = time.perf_counter()
            cnt, unresolved = mod.add(self, lst)
            _info(feature, "write_done", op="add", count=int(cnt), unresolved=len(unresolved), dur_ms=int((time.perf_counter() - started) * 1000))
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            _error(feature, "write_failed", op="add", error=str(e))
            return {"ok": False, "error": str(e)}

    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _info(feature, "write_skipped", op="remove", reason="disabled_or_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _warn(feature, "write_skipped", op="remove", reason="module_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            started = time.perf_counter()
            cnt, unresolved = mod.remove(self, lst)
            _info(feature, "write_done", op="remove", count=int(cnt), unresolved=len(unresolved), dur_ms=int((time.perf_counter() - started) * 1000))
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            _error(feature, "write_failed", op="remove", error=str(e))
            return {"ok": False, "error": str(e)}

    def health(self) -> Mapping[str, Any]:
        started = time.perf_counter()
        enabled = self.supported_features()
        ok = True
        error: str | None = None

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
            error = str(e)

        latency_ms = int((time.perf_counter() - started) * 1000)

        if ok:
            features = dict(enabled)
            status = "ok"
        else:
            features = {k: False for k in enabled.keys()}
            status = "down"

        details: dict[str, Any] = {}
        if error:
            details["error"] = error
        disabled = [k for k, v in enabled.items() if not v]
        if disabled:
            details["disabled"] = disabled

        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": {},
        }

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

    def build_index(
        self,
        cfg: Mapping[str, Any],
        *,
        feature: str,
    ) -> Mapping[str, dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _CrossWatchOPS()
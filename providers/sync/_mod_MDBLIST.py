# /providers/sync/_mod_MDBLIST.py
# CrossWatch MDBLIST module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,
    make_snapshot_progress,
)

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "2.0.0"
__all__ = ["get_manifest", "MDBLISTModule", "OPS"]


def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST] {msg}")


def _label_mdblist(*_args: Any, **_kwargs: Any) -> str:
    return "MDBLIST"


try:
    from .mdblist import _watchlist as feat_watchlist
except Exception:
    feat_watchlist = None

try:
    from .mdblist import _ratings as feat_ratings
except Exception:
    feat_ratings = None


_FEATURES: dict[str, Any] = {}
if feat_watchlist:
    _FEATURES["watchlist"] = feat_watchlist
if feat_ratings:
    _FEATURES["ratings"] = feat_ratings


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings": "ratings" in _FEATURES,
        "history": False,
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
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
        },
    }


@dataclass
class MDBLISTConfig:
    api_key: str
    timeout: float = 15.0
    max_retries: int = 3


class MDBLISTError(RuntimeError):
    pass


class MDBLISTAuthError(MDBLISTError):
    pass


class MDBLISTClient:
    BASE = "https://api.mdblist.com"

    def __init__(self, cfg: MDBLISTConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("MDBLIST", ctx, feature_label=_label_mdblist)

    def connect(self) -> MDBLISTClient:
        if not self.cfg.api_key:
            raise MDBLISTAuthError("Missing MDBList api_key")
        _log("MDBList client initialized and ready")
        return self

    def get(self, url: str, **kw: Any):
        return request_with_retries(
            self.session,
            "GET",
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )

    def post(self, url: str, **kw: Any):
        return request_with_retries(
            self.session,
            "POST",
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )


class MDBLISTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        m = dict(cfg.get("mdblist") or {})
        self.cfg = MDBLISTConfig(
            api_key=str(m.get("api_key") or "").strip(),
            timeout=float(m.get("timeout", cfg.get("timeout", 15.0))),
            max_retries=int(m.get("max_retries", cfg.get("max_retries", 3))),
        )
        if not self.cfg.api_key:
            raise MDBLISTAuthError("Missing MDBList api_key")

        if m.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_MDBLIST_DEBUG", "1")

        self.client = MDBLISTClient(self.cfg, cfg).connect()
        self.raw_cfg = cfg
        self.config = cfg

        def _mk_prog(feature: str):
            try:
                return make_snapshot_progress(ctx, dst="MDBLIST", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *args: Any, **kwargs: Any) -> None:
                        pass

                    def done(self, *args: Any, **kwargs: Any) -> None:
                        pass

                return _Noop()

        self.progress_factory: Callable[[str], Any] = _mk_prog

    @staticmethod
    def supported_features() -> dict[str, bool]:
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
            return {
                "ok": True,
                "status": "ok",
                "latency_ms": 0,
                "features": {},
                "details": {"disabled": ["watchlist", "ratings"]},
                "api": {},
            }

        tmo = max(3.0, min(self.cfg.timeout, 15.0))
        base = self.client.BASE
        sess = self.client.session
        start = time.perf_counter()

        wl_ok = False
        wl_code: int | None = None
        retry_after: int | None = None
        rate: dict[str, int | None] = {"limit": None, "remaining": None, "reset": None}

        try:
            r = request_with_retries(
                sess,
                "GET",
                f"{base}/watchlist/items",
                params={"apikey": self.cfg.api_key, "limit": 1, "offset": 0, "unified": 1},
                timeout=tmo,
                max_retries=self.cfg.max_retries,
            )
            wl_code = r.status_code
            if 200 <= r.status_code < 300:
                wl_ok = True
            elif r.status_code == 429:
                ra = r.headers.get("Retry-After")
                if ra:
                    try:
                        retry_after = int(ra)
                    except Exception:
                        pass
            rate = parse_rate_limit(r.headers)
        except Exception:
            wl_ok = False

        latency_ms = int((time.perf_counter() - start) * 1000)

        features = {
            "watchlist": wl_ok if enabled.get("watchlist") else False,
            "ratings": wl_ok if enabled.get("ratings") else False,
            "history": False,
            "playlists": False,
        }

        checks = [features[k] for k in ("watchlist", "ratings") if enabled.get(k)]
        if not checks:
            status = "ok"
        elif all(checks):
            status = "ok"
        elif any(checks):
            status = "degraded"
        else:
            status = "down"

        ok = status in ("ok", "degraded")

        details: dict[str, Any] = {}
        disabled = [k for k, v in enabled.items() if not v]
        if disabled:
            details["disabled"] = disabled
        if retry_after is not None:
            details["retry_after_s"] = retry_after

        api = {
            "watchlist": {
                "status": wl_code,
                "retry_after": retry_after,
                "rate": rate,
            },
        }

        _log(f"health status={status} ok={ok} latency_ms={latency_ms}")
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": api,
        }

    def feature_names(self) -> tuple[str, ...]:
        return tuple(k for k, v in self.supported_features().items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        if not self._is_enabled(feature) or feature not in _FEATURES:
            _log(f"build_index skipped (disabled/missing): {feature}")
            return {}
        mod = _FEATURES.get(feature)
        return mod.build_index(self, **kwargs) if mod else {}

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
            _log(f"add skipped (disabled/missing): {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _log(f"add skipped: feature module missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.add(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
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
            _log(f"remove skipped (disabled/missing): {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _log(f"remove skipped: feature module missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.remove(self, lst)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved}
        except Exception as e:
            return {"ok": False, "error": str(e)}


class _MDBLISTOPS:
    def name(self) -> str:
        return "MDBLIST"

    def label(self) -> str:
        return "MDBList"

    def features(self) -> Mapping[str, bool]:
        return MDBLISTModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "index_semantics": "present",
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}
        m = c.get("mdblist") or {}
        key = m.get("api_key") or m.get("apikey") or ""
        return bool(str(key).strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> MDBLISTModule:
        return MDBLISTModule(cfg)

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

OPS = _MDBLISTOPS()
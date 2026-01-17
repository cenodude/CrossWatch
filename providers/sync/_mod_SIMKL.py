# /providers/sync/_mod_SIMKL.py
# CrossWatch SIMKL module
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

import requests

from ._log import log as cw_log
from ._mod_common import (
    build_session,
    label_simkl,
    make_snapshot_progress,
    parse_rate_limit,
    request_with_retries,
)
from .simkl._common import _pair_scope as simkl_pair_scope, build_headers, normalize as simkl_normalize, key_of as simkl_key_of, state_file

__VERSION__ = "3.0.0"
__all__ = ["get_manifest", "SIMKLModule", "OPS"]


def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("SIMKL", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)


def _log(msg: str, *, level: str = "debug", feature: str = "module", **fields: Any) -> None:
    cw_log("SIMKL", feature, level, msg, **fields)

if "ctx" not in globals():
    class _NullCtx:
        def emit(self, *args: Any, **kwargs: Any) -> None:
            pass

    ctx = _NullCtx()  # type: ignore[assignment]

try:
    from .simkl import _watchlist as feat_watchlist
except Exception as e:
    feat_watchlist = None
    _log("failed to import watchlist", level="warn", error=str(e))

try:
    from .simkl import _history as feat_history
except Exception as e:
    feat_history = None
    _log("failed to import history", level="warn", error=str(e))

try:
    from .simkl import _ratings as feat_ratings
except Exception as e:
    feat_ratings = None
    _log("failed to import ratings", level="warn", error=str(e))


class SIMKLError(RuntimeError):
    pass


class SIMKLAuthError(SIMKLError):
    pass


def _json_load(path: str) -> dict[str, Any]:
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


_FEATURES: dict[str, Any] = {}
if feat_watchlist:
    _FEATURES["watchlist"] = feat_watchlist
if feat_history:
    _FEATURES["history"] = feat_history
if feat_ratings:
    _FEATURES["ratings"] = feat_ratings


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings": "ratings" in _FEATURES,
        "history": "history" in _FEATURES,
        "playlists": False,
    }


def supported_features() -> dict[str, bool]:
    toggles = {
        "watchlist": True,
        "ratings": True,
        "history": True,
        "playlists": False,
    }
    present = _features_flags()
    return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}


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
            "index_semantics": "delta",
            "observed_deletes": False,
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "unrate": True,
                "from_date": True,
            },
        },
    }


@dataclass
class SIMKLConfig:
    api_key: str
    access_token: str
    date_from: str = ""
    timeout: float = 15.0
    max_retries: int = 3


class SIMKLClient:
    BASE = "https://api.simkl.com"

    def __init__(self, cfg: SIMKLConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session: requests.Session = build_session("SIMKL", ctx, feature_label=label_simkl)
        self.session.headers.update(
            build_headers({"simkl": {"api_key": cfg.api_key, "access_token": cfg.access_token}})
        )

    def _request(self, method: str, url: str, **kw: Any) -> requests.Response:
        return request_with_retries(
            self.session,
            method,
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )

    def connect(self) -> SIMKLClient:
        return self

    def activities(self) -> dict[str, Any]:
        try:
            r = self._request("POST", f"{self.BASE}/sync/activities")
            if r.ok:
                return r.json() if r.text else {}
            return {"status": r.status_code}
        except Exception as e:
            return {"error": str(e)}

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return simkl_normalize(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return simkl_key_of(obj)


class SIMKLModule:
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
        self.progress_factory = (
            lambda feature, total=None, throttle_ms=300: make_snapshot_progress(
                ctx,
                dst="SIMKL",
                feature=str(feature),
                total=total,
                throttle_ms=int(throttle_ms),
            )
        )

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        enabled = supported_features()
        need_core = any(enabled.values())

        base = self.client.BASE
        sess = self.client.session
        tmo = max(3.0, min(self.cfg.timeout, 15.0))
        start = time.perf_counter()

        core_ok = False
        core_reason: str | None = None
        core_code: int | None = None
        retry_after: int | None = None
        rate: dict[str, int | None] = {"limit": None, "remaining": None, "reset": None}

        if need_core:
            try:
                r = request_with_retries(
                    sess,
                    "POST",
                    f"{base}/sync/activities",
                    timeout=tmo,
                    max_retries=self.cfg.max_retries,
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
                    try:
                        retry_after = int(ra)
                    except Exception:
                        pass
                rate = parse_rate_limit(r.headers)
            except Exception as e:
                core_reason = f"exception:{e.__class__.__name__}"

        latency_ms = int((time.perf_counter() - start) * 1000)

        features = {
            "watchlist": bool(enabled.get("watchlist") and "watchlist" in _FEATURES and core_ok),
            "ratings": bool(enabled.get("ratings") and "ratings" in _FEATURES and core_ok),
            "history": bool(enabled.get("history") and "history" in _FEATURES and core_ok),
            "playlists": False,
        }

        if not need_core:
            status = "ok"
        elif core_ok:
            status = "ok"
        else:
            status = (
                "auth_failed"
                if (core_code in (401, 403) or core_reason == "unauthorized")
                else "down"
            )

        ok = status in ("ok", "degraded")

        details: dict[str, Any] = {}
        if need_core and not core_ok:
            details["reason"] = f"core:{core_reason or 'down'}"
        if retry_after is not None:
            details["retry_after_s"] = retry_after

        api = {
            "activities": {
                "status": core_code if need_core else None,
                "retry_after": retry_after if need_core else None,
                "rate": rate if need_core else {"limit": None, "remaining": None, "reset": None},
            },
        }

        if simkl_pair_scope():
            try:
                _json_save(
                    str(state_file("simkl.activities.shadow.json")),
                    {"ts": int(time.time()), "data": {"status": core_code}},
                )
            except Exception:
                pass

        _health(status, ok, latency_ms)
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details or None,
            "api": api,
        }

    def get_date_from(self) -> str:
        return self.cfg.date_from

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return simkl_normalize(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return simkl_key_of(obj)

    def feature_names(self) -> tuple[str, ...]:
        feats = supported_features()
        return tuple(k for k, v in feats.items() if v and k in _FEATURES)

    def build_index(self, feature: str, **kwargs: Any) -> dict[str, dict[str, Any]]:
        feats = supported_features()
        if not feats.get(feature) or feature not in _FEATURES:
            _log(f"build_index skipped: feature disabled or missing: {feature}")
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
        feats = supported_features()
        if not feats.get(feature) or feature not in _FEATURES:
            _log(f"add skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        lst = list(items or [])
        if not lst:
            return {"ok": True, "count": 0}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _log(f"add skipped: feature module missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        count, unresolved = mod.add(self, lst)
        return {"ok": True, "count": int(count), "unresolved": unresolved}

    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        feats = supported_features()
        if not feats.get(feature) or feature not in _FEATURES:
            _log(f"remove skipped: feature disabled or missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        lst = list(items or [])
        if not lst:
            return {"ok": True, "count": 0}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _log(f"remove skipped: feature module missing: {feature}")
            return {"ok": True, "count": 0, "unresolved": []}
        count, unresolved = mod.remove(self, lst)
        return {"ok": True, "count": int(count), "unresolved": unresolved}


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
        c = cfg or {}
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
        api_key = (
            sm.get("api_key")
            or sm.get("client_id")
            or (au.get("api_key") if isinstance(au, dict) else None)
            or (au.get("client_id") if isinstance(au, dict) else None)
            or ""
        )
        return bool(str(token).strip() and str(api_key).strip())

    def _adapter(self, cfg: Mapping[str, Any]) -> SIMKLModule:
        return SIMKLModule(cfg)

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

OPS = _SIMKLOPS()
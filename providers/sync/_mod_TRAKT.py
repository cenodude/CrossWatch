# /providers/sync/_mod_TRAKT.py
# CrossWatch SIMKL module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

from .trakt._common import build_headers, normalize as trakt_normalize, key_of as trakt_key_of
from ._log import log as cw_log


def _confirmed_keys(key_of, items: Iterable[Mapping[str, Any]], unresolved: Any) -> list[str]:
    attempted: list[str] = []
    for it in items or []:
        try:
            k = str(key_of(it) or "").strip()
        except Exception:
            k = ""
        if k:
            attempted.append(k)

    unresolved_keys: set[str] = set()
    if unresolved:
        for u in unresolved:
            obj: Any = u
            if isinstance(u, Mapping):
                if isinstance(u.get("key"), str) and u.get("key"):
                    unresolved_keys.add(str(u.get("key")))
                    continue
                if "item" in u:
                    obj = u.get("item")
            if isinstance(obj, str) and obj:
                unresolved_keys.add(obj)
                continue
            if isinstance(obj, Mapping):
                try:
                    k = str(key_of(obj) or "").strip()
                except Exception:
                    k = ""
                if k:
                    unresolved_keys.add(k)

    out: list[str] = []
    seen: set[str] = set()
    for k in attempted:
        if k in unresolved_keys or k in seen:
            continue
        out.append(k)
        seen.add(k)
    return out

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

feat_playlists = None

from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,
    label_trakt,
    make_snapshot_progress,
)

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "3.2.0"
__all__ = ["get_manifest", "TRAKTModule", "OPS"]


class TRAKTError(RuntimeError):
    pass


class TRAKTAuthError(TRAKTError):
    pass


_PROVIDER = "TRAKT"


def _dbg(feature: str, event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, str(feature or "module"), "debug", event, **fields)

def _info(feature: str, event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, str(feature or "module"), "info", event, **fields)

def _warn(feature: str, event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, str(feature or "module"), "warn", event, **fields)

def _error(feature: str, event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, str(feature or "module"), "error", event, **fields)



_FEATURES: dict[str, Any] = {}
if feat_watchlist:
    _FEATURES["watchlist"] = feat_watchlist
if feat_history:
    _FEATURES["history"] = feat_history
if feat_ratings:
    _FEATURES["ratings"] = feat_ratings
if feat_playlists:
    _FEATURES["playlists"] = feat_playlists


def _features_flags() -> dict[str, bool]:
    return {
        "watchlist": "watchlist" in _FEATURES,
        "ratings": "ratings" in _FEATURES,
        "history": "history" in _FEATURES,
        "playlists": "playlists" in _FEATURES,
    }


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "TRAKT",
        "label": "Trakt",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": TRAKTModule.supported_features(),
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
class TRAKTConfig:
    client_id: str
    access_token: str
    timeout: float = 15.0
    max_retries: int = 3
    history_number_fallback: bool = False
    history_collection: bool = False


class TRAKTClient:
    BASE = "https://api.trakt.tv"

    def __init__(self, cfg: TRAKTConfig, raw_cfg: Mapping[str, Any]):
        self.cfg = cfg
        self.raw_cfg = raw_cfg
        self.session = build_session("TRAKT", ctx, feature_label=label_trakt)
        self._apply_headers(cfg.access_token)

    def _trakt_dict(self) -> dict[str, Any]:
        try:
            return dict(self.raw_cfg.get("trakt") or {})
        except Exception:
            return {}

    def _apply_headers(self, access_token: str | None) -> None:
        self.session.headers.update(
            build_headers(
                {
                    "trakt": {
                        "client_id": self.cfg.client_id,
                        "access_token": access_token or "",
                    }
                }
            )
        )

    def _reload_token_from_cfg(self) -> str:
        tok = str(self._trakt_dict().get("access_token") or "").strip()
        if tok and tok != (self.cfg.access_token or ""):
            self.cfg.access_token = tok
            self._apply_headers(tok)
            _info("auth", "token_refreshed")
        return tok

    def _about_to_expire(self, threshold: int = 120) -> bool:
        try:
            exp = int(self._trakt_dict().get("expires_at") or 0)
            now = int(__import__("time").time())
            return bool(exp and (exp - now) <= max(0, threshold))
        except Exception:
            return False

    def _try_refresh(self) -> bool:
        try:
            src_p = str(os.getenv("CW_PAIR_SRC") or "").upper().strip()
            dst_p = str(os.getenv("CW_PAIR_DST") or "").upper().strip()
            inst = "default"
            if src_p == "TRAKT":
                inst = str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default").strip() or "default"
            elif dst_p == "TRAKT":
                inst = str(os.getenv("CW_PAIR_DST_INSTANCE") or "default").strip() or "default"

            res = AUTH_TRAKT.refresh(None, instance_id=inst)
            ok = bool(isinstance(res, dict) and res.get("ok"))
            if ok:
                try:
                    from cw_platform.config_base import load_config
                    from cw_platform.provider_instances import get_provider_block, normalize_instance_id

                    cfg = load_config() or {}
                    blk = get_provider_block(cfg, "trakt", normalize_instance_id(inst))
                    tok = str((blk or {}).get("access_token") or "").strip()
                    if tok and tok != (self.cfg.access_token or ""):
                        self.cfg.access_token = tok
                        self._apply_headers(tok)
                        _info("auth", "token_refreshed")
                except Exception:
                    pass
            else:
                _warn("auth", "token_refresh_failed", result=repr(res))
            return ok
        except Exception as e:
            _warn("auth", "token_refresh_error", error=str(e))
            return False

    def _preflight(self) -> None:
        if self._about_to_expire():
            self._try_refresh()

    def _do(self, method: str, url: str, **kw: Any):
        self._preflight()
        r = request_with_retries(
            self.session,
            method,
            url,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
            **kw,
        )
        if r.status_code in (401, 403):
            if self._try_refresh():
                r = request_with_retries(
                    self.session,
                    method,
                    url,
                    timeout=self.cfg.timeout,
                    max_retries=self.cfg.max_retries,
                    **kw,
                )
        return r

    def connect(self) -> TRAKTClient:
        try:
            r = self._do("GET", f"{self.BASE}/sync/last_activities")
            if r.status_code in (401, 403):
                raise TRAKTAuthError("Trakt auth failed")
            
        except Exception as e:
            raise TRAKTError(f"Trakt connect failed: {e}") from e
        return self

    def get(self, url: str, **kw: Any):
        return self._do("GET", url, **kw)

    def post(self, url: str, json: Mapping[str, Any], **kw: Any):
        return self._do("POST", url, json=json, **kw)

    def delete(self, url: str, json: Mapping[str, Any] | None = None, **kw: Any):
        return self._do("DELETE", url, json=json, **kw)


class TRAKTModule:
    def __init__(self, cfg: Mapping[str, Any]):
        t = dict(cfg.get("trakt") or {})
        self.cfg = TRAKTConfig(
            client_id=str(t.get("client_id") or "").strip(),
            access_token=str(t.get("access_token") or "").strip(),
            timeout=float(t.get("timeout", cfg.get("timeout", 15.0))),
            max_retries=int(t.get("max_retries", cfg.get("max_retries", 3))),
            history_number_fallback=bool(t.get("history_number_fallback")),
            history_collection=bool(t.get("history_collection")),
        )
        if not self.cfg.client_id or not self.cfg.access_token:
            raise TRAKTAuthError("Missing Trakt client_id/access_token")

        if t.get("debug") in (True, "1", 1):
            os.environ.setdefault("CW_TRAKT_DEBUG", "1")

        self.client = TRAKTClient(self.cfg, cfg).connect()
        self.raw_cfg = cfg
        self.progress_factory = (
            lambda feature, total=None, throttle_ms=300: make_snapshot_progress(
                ctx,
                dst="TRAKT",
                feature=str(feature),
                total=total,
                throttle_ms=int(throttle_ms),
            )
        )

    @staticmethod
    def supported_features() -> dict[str, bool]:
        toggles = {
            "watchlist": True,
            "ratings": True,
            "history": True,
            "playlists": False,
        }
        present = _features_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return trakt_normalize(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return trakt_key_of(obj)

    def health(self) -> Mapping[str, Any]:
        enabled = self.supported_features()
        need_core = any(enabled.values())
        need_wl = bool(enabled.get("watchlist"))

        tmo = max(3.0, min(self.cfg.timeout, 15.0))
        base = self.client.BASE
        sess = self.client.session

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
                    "GET",
                    f"{base}/sync/last_activities",
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

        wl_ok = False
        wl_reason: str | None = None
        wl_code: int | None = None
        if need_wl and core_ok:
            try:
                r2 = request_with_retries(
                    sess,
                    "GET",
                    f"{base}/sync/watchlist",
                    params={"limit": 1, "page": 1},
                    timeout=tmo,
                    max_retries=self.cfg.max_retries,
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
                        try:
                            retry_after = int(ra2)
                        except Exception:
                            pass
                else:
                    wl_reason = f"http:{r2.status_code}"
            except Exception as e:
                wl_reason = f"exception:{e.__class__.__name__}"

        latency_ms = int((time.perf_counter() - start) * 1000)

        features = {
            "watchlist": (core_ok and wl_ok) if (need_wl and "watchlist" in _FEATURES) else False,
            "ratings": (core_ok if (enabled.get("ratings") and "ratings" in _FEATURES) else False),
            "history": (core_ok if (enabled.get("history") and "history" in _FEATURES) else False),
            "playlists": (core_ok if (enabled.get("playlists") and "playlists" in _FEATURES) else False),
        }

        checks: list[bool] = []
        if need_core:
            checks.append(core_ok)
        if need_wl:
            checks.append(wl_ok)

        core_auth_failed = need_core and (core_code in (401, 403) or core_reason == "unauthorized")
        wl_auth_failed = need_wl and (wl_code in (401, 403) or wl_reason == "unauthorized")

        if not checks:
            status = "ok"
        elif all(checks):
            status = "ok"
        elif any(checks):
            status = "degraded"
        else:
            status = "auth_failed" if (core_auth_failed or wl_auth_failed) else "down"

        ok = status in ("ok", "degraded")

        details: dict[str, Any] = {}
        disabled = [k for k, v in enabled.items() if not v]
        if disabled:
            details["disabled"] = disabled

        reasons: list[str] = []
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
                "status": core_code if need_core else None,
                "retry_after": retry_after if need_core else None,
                "rate": rate if need_core else {"limit": None, "remaining": None, "reset": None},
            },
            "watchlist": {
                "status": wl_code if need_wl else None,
                "retry_after": retry_after if need_wl else None,
            },
        }

        _info("health", "health", status=status, ok=ok, latency_ms=latency_ms)
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
            _dbg(feature, "feature_disabled_or_missing")
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
            _dbg(feature, "feature_disabled_or_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _dbg(feature, "feature_module_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.add(self, lst)
            confirmed_keys = _confirmed_keys(self.key_of, lst, unresolved)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
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
            _dbg(feature, "feature_disabled_or_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True}
        mod = _FEATURES.get(feature)
        if not mod:
            _dbg(feature, "feature_module_missing")
            return {"ok": True, "count": 0, "unresolved": []}
        try:
            cnt, unresolved = mod.remove(self, lst)
            confirmed_keys = _confirmed_keys(self.key_of, lst, unresolved)
            return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
        except Exception as e:
            return {"ok": False, "error": str(e)}


class _TraktOPS:
    def name(self) -> str:
        return "TRAKT"

    def label(self) -> str:
        return "Trakt"

    def features(self) -> Mapping[str, bool]:
        return TRAKTModule.supported_features()

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

OPS = _TraktOPS()
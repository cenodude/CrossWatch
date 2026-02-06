# /providers/sync/_mod_EMBY.py
# CrossWatch EMBY module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

import requests

from cw_platform.provider_instances import normalize_instance_id

from ._log import log as cw_log

from .emby._common import normalize as emby_normalize, key_of as emby_key_of
from .emby._common import _pair_scope as _emby_pair_scope, state_file as _emby_state_file
from .emby import _watchlist as feat_watchlist
from .emby import _history as feat_history
from .emby import _ratings as feat_ratings

from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,  # parity
    label_emby,
    make_snapshot_progress,
)


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

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "3.0.0"
__all__ = ["get_manifest", "EMBYModule", "OPS"]

_DEF_UA = os.environ.get("CW_UA", f"CrossWatch/{__VERSION__} (Emby)")


def _pick_instance_id(provider: str) -> str:
    prov = str(provider or "").upper().strip()
    for k in ("CW_SNAPSHOT_INSTANCE", "CW_INSTANCE_ID", "CW_PROFILE", "CW_PROVIDER_INSTANCE", "CW_INSTANCE"):
        v = (os.environ.get(k) or "").strip()
        if v:
            return normalize_instance_id(v)
    if (os.environ.get("CW_PAIR_SRC") or "").upper().strip() == prov:
        v = (os.environ.get("CW_PAIR_SRC_INSTANCE") or os.environ.get("CW_SRC_INSTANCE") or "").strip()
        if v:
            return normalize_instance_id(v)
    if (os.environ.get("CW_PAIR_DST") or "").upper().strip() == prov:
        v = (os.environ.get("CW_PAIR_DST_INSTANCE") or os.environ.get("CW_DST_INSTANCE") or "").strip()
        if v:
            return normalize_instance_id(v)
    v = (os.environ.get("CW_PAIR_INSTANCE") or "").strip()
    return normalize_instance_id(v)

def _merge_instance_block(raw: Any, inst: str) -> dict[str, Any]:
    base = dict(raw or {}) if isinstance(raw, Mapping) else {}
    if inst == "default":
        base.pop("instances", None)
        return base
    insts = base.get("instances")
    if isinstance(insts, Mapping) and isinstance(insts.get(inst), Mapping):
        merged = dict(base)
        merged.update(dict(insts.get(inst) or {}))
        merged.pop("instances", None)
        return merged
    base.pop("instances", None)
    return base



def _dbg(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "core", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "core", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "core", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "core", "error", msg, **fields)
    
def _health(status: str, ok: bool, latency_ms: int) -> None:
    cw_log("EMBY", "health", "info", "health", latency_ms=latency_ms, ok=ok, status=status)


_FEATURES: dict[str, Any] = {
    "watchlist": feat_watchlist,
    "history": feat_history,
    "ratings": feat_ratings,
}

_HEALTH_SHADOW_NAME = "emby.health.shadow.json"


def _present_flags() -> dict[str, bool]:
    return {k: bool(v) for k, v in _FEATURES.items()}


def _save_health_shadow(payload: Mapping[str, Any]) -> None:
    if _emby_pair_scope() is None:
        return
    try:
        path = _emby_state_file(_HEALTH_SHADOW_NAME)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "EMBY",
        "label": "Emby",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": {
            "watchlist": True,
            "history": True,
            "ratings": False,
            "playlists": False,
        },
        "requires": ["requests"],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": False,
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
class EMBYConfig:
    server: str
    access_token: str
    user_id: str
    device_id: str = "crosswatch"
    verify_ssl: bool = True
    timeout: float = 15.0
    max_retries: int = 3
    watchlist_mode: str = "favorites"
    watchlist_playlist_name: str = "Watchlist"
    watchlist_query_limit: int = 25
    watchlist_write_delay_ms: int = 0
    watchlist_guid_priority: list[str] | None = None
    history_query_limit: int = 25
    history_write_delay_ms: int = 0
    history_guid_priority: list[str] | None = None
    history_force_overwrite: bool = False
    history_backdate: bool = False
    history_backdate_tolerance_s: int = 300
    history_libraries: list[str] | None = None
    ratings_libraries: list[str] | None = None


class EMBYClient:
    BASE_PATH_PING = "/System/Ping"
    BASE_PATH_INFO = "/System/Info"
    BASE_PATH_USER = "/Users/{user_id}"

    def __init__(self, cfg: EMBYConfig):
        if not cfg.server or not cfg.access_token or not cfg.user_id:
            raise RuntimeError("Emby config requires server, access_token, user_id")
        self.cfg = cfg
        self.base = cfg.server.rstrip("/")
        self.session = build_session("EMBY", ctx, feature_label=label_emby)
        self.session.verify = bool(cfg.verify_ssl)
        auth_val = (
            f'MediaBrowser Client="CrossWatch", Device="CrossWatch", '
            f'DeviceId="{cfg.device_id}", Version="{__VERSION__}", Token="{cfg.access_token}"'
        )
        self.session.headers.update(
            {
                "Accept": "application/json",
                "User-Agent": _DEF_UA,
                "Authorization": auth_val,
                "X-Emby-Authorization": auth_val,
                "X-MediaBrowser-Token": cfg.access_token,
            }
        )

    def _url(self, path: str) -> str:
        return self.base + (path if path.startswith("/") else ("/" + path))

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> requests.Response:
        return request_with_retries(
            self.session,
            method,
            self._url(path),
            params=params or {},
            json=json,
            timeout=self.cfg.timeout,
            max_retries=self.cfg.max_retries,
        )

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> requests.Response:
        return self._request("GET", path, params=params)

    def post(
        self,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json: Any = None,
    ) -> requests.Response:
        return self._request("POST", path, params=params, json=json)

    def delete(self, path: str, *, params: dict[str, Any] | None = None) -> requests.Response:
        return self._request("DELETE", path, params=params)

    def ping(self) -> requests.Response:
        return self.get(self.BASE_PATH_PING)

    def system_info(self) -> requests.Response:
        return self.get(self.BASE_PATH_INFO)

    def user_probe(self) -> requests.Response:
        path = self.BASE_PATH_USER.format(user_id=self.cfg.user_id)
        return self.get(path)


class EMBYModule:
    def __init__(self, cfg: Mapping[str, Any]):
        inst = _pick_instance_id("EMBY")
        em = _merge_instance_block((cfg or {}).get("emby") or {}, inst)
        auth = _merge_instance_block(dict((cfg or {}).get("auth") or {}).get("emby") or {}, inst)
        em.setdefault("server", auth.get("server"))
        em.setdefault("access_token", auth.get("access_token"))
        em.setdefault("user_id", auth.get("user_id"))

        wl = dict(em.get("watchlist") or {})
        wl_mode = str(wl.get("mode") or "favorites").strip().lower()
        wl_pname = (wl.get("playlist_name") or "Watchlist").strip() or "Watchlist"
        wl_qlim = int(wl.get("watchlist_query_limit", 25) or 25)
        wl_wdel = int(wl.get("watchlist_write_delay_ms", 0) or 0)
        wl_gprio = wl.get("watchlist_guid_priority") or [
            "tmdb",
            "imdb",
            "tvdb",
            "agent:themoviedb:en",
            "agent:themoviedb",
            "agent:imdb",
        ]

        hi = dict(em.get("history") or {})
        hi_qlim = int(hi.get("history_query_limit", 25) or 25)
        hi_wdel = int(hi.get("history_write_delay_ms", 0) or 0)
        hi_gprio = hi.get("history_guid_priority") or wl_gprio
        ra = dict(em.get("ratings") or {})

        def _list_str(v: Any) -> list[str] | None:
            if not v:
                return None
            rows = [str(x).strip() for x in v if str(x).strip()]
            return rows or None

        def _b(v: Any) -> bool:
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v != 0
            if isinstance(v, str):
                return v.strip().lower() in ("1", "true", "yes", "y", "on")
            return False

        def _i(v: Any, d: int) -> int:
            try:
                return int(v)
            except Exception:
                return int(d)

        force_overwrite = _b(em.get("history_force_overwrite", False) or hi.get("force_overwrite", False))
        backdate = _b(em.get("history_backdate", False) or hi.get("backdate", False))
        bd_tolerance = _i(
            hi.get("backdate_tolerance_s", em.get("history_backdate_tolerance_s", 300)),
            300,
        )

        self.cfg = EMBYConfig(
            server=str(em.get("server") or "").strip(),
            access_token=str(em.get("access_token") or "").strip(),
            user_id=str(em.get("user_id") or "").strip(),
            device_id=str(em.get("device_id") or "crosswatch"),
            verify_ssl=bool(em.get("verify_ssl", True)),
            timeout=float((cfg or {}).get("timeout", em.get("timeout", 15.0))),
            max_retries=int((cfg or {}).get("max_retries", em.get("max_retries", 3))),
            watchlist_mode=wl_mode,
            watchlist_playlist_name=wl_pname,
            watchlist_query_limit=wl_qlim,
            watchlist_write_delay_ms=wl_wdel,
            watchlist_guid_priority=list(wl_gprio),
            history_query_limit=hi_qlim,
            history_write_delay_ms=hi_wdel,
            history_guid_priority=list(hi_gprio),
            history_libraries=_list_str(hi.get("libraries")),
            ratings_libraries=_list_str(ra.get("libraries")),
            history_force_overwrite=force_overwrite,
            history_backdate=backdate,
            history_backdate_tolerance_s=bd_tolerance,
        )
        self.client = EMBYClient(self.cfg)

        def _mk_prog(feature: str):
            try:
                return make_snapshot_progress(ctx, dst="EMBY", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *args: Any, **kwargs: Any) -> None:
                        pass

                    def done(self, *args: Any, **kwargs: Any) -> None:
                        pass

                return _Noop()

        self.progress_factory: Callable[[str], Any] = _mk_prog

    @staticmethod
    def normalize(obj: Any) -> dict[str, Any]:
        return emby_normalize(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return emby_key_of(obj)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    @staticmethod
    def supported_features() -> dict[str, bool]:
        toggles = {"watchlist": True, "history": True, "ratings": False, "playlists": False}
        present = _present_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    def health(self) -> Mapping[str, Any]:
        enabled = self.supported_features()
        need_any = any(enabled.values())
        start = time.perf_counter()

        if not need_any:
            latency_ms = int((time.perf_counter() - start) * 1000)
            details: dict[str, Any] = {
                "server_ok": False,
                "auth_ok": False,
                "server": {"product": None, "version": None},
                "disabled": [k for k, v in enabled.items() if not v],
            }
            features = {k: False for k in ("watchlist", "history", "ratings", "playlists")}
            api = {
                "ping": {"status": None},
                "info": {"status": None},
                "user": {"status": None},
            }
            return {
                "ok": True,
                "status": "ok",
                "latency_ms": latency_ms,
                "features": features,
                "details": details,
                "api": api,
            }

        retry_after: int | None = None
        rate: dict[str, int | None] = {"limit": None, "remaining": None, "reset": None}

        try:
            ru = self.client.user_probe()
            user_code = ru.status_code
            user_ok = bool(ru.ok)
            ra = ru.headers.get("Retry-After")
            if ra:
                try:
                    retry_after = int(ra)
                except Exception:
                    pass
            rate = parse_rate_limit(ru.headers)
        except Exception:
            ru = None
            user_code = None
            user_ok = False

        latency_ms = int((time.perf_counter() - start) * 1000)

        server_ok = bool(user_ok and user_code is not None and user_code < 500)
        auth_ok = bool(user_ok and user_code == 200)
        product = "Emby Server"
        version = None

        base_ready = bool(server_ok and auth_ok)
        features = {
            "watchlist": base_ready if enabled.get("watchlist") else False,
            "history": base_ready if enabled.get("history") else False,
            "ratings": base_ready if enabled.get("ratings") else False,
            "playlists": base_ready if enabled.get("playlists") else False,
        }

        checks: list[bool] = [features[k] for k, on in enabled.items() if on]
        if checks and all(checks):
            status = "ok"
        elif checks and any(checks):
            status = "degraded"
        else:
            status = "auth_failed" if (user_code in (401, 403)) else "down"

        ok = status in ("ok", "degraded")

        reasons: list[str] = []
        if not server_ok:
            if user_code and user_code >= 500:
                reasons.append(f"user:http:{user_code}")
            else:
                reasons.append("server_unreachable")
        if not auth_ok:
            if user_code in (401, 403):
                reasons.append("user:unauthorized")
            elif user_code and user_code < 500:
                reasons.append(f"user:http:{user_code}")
            else:
                reasons.append("user:unreachable")

        details: dict[str, Any] = {
            "server_ok": server_ok,
            "auth_ok": auth_ok,
            "server": {"product": product, "version": version},
        }
        disabled = [k for k, v in enabled.items() if not v]
        if disabled:
            details["disabled"] = disabled
        if reasons:
            details["reason"] = "; ".join(reasons)

        api = {
            "user": {
                "status": user_code,
                "retry_after": retry_after,
                "rate": rate,
            }
        }

        try:
            _save_health_shadow(
                {
                    "ts": int(time.time()),
                    "status": status,
                    "api": api,
                    "server_ok": server_ok,
                    "auth_ok": auth_ok,
                    "disabled": disabled,
                }
            )
        except Exception:
            pass

        _health(status, ok, latency_ms)


        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details if details else None,
            "api": api,
        }

    def feature_names(self) -> tuple[str, ...]:
        enabled = self.supported_features()
        return tuple(k for k, v in enabled.items() if v)

    def build_index(self, feature: str, **kwargs: Any) -> Mapping[str, dict[str, Any]]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _dbg("feature_disabled", op="build_index", feature=f)
            return {}
        mod = _FEATURES.get(f)
        if not mod:
            return {}
        return mod.build_index(self, **kwargs)

    def _dry_result(self, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        lst = list(items)
        return {"ok": True, "count": len(lst), "dry_run": True}

    def add(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> Mapping[str, Any]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _dbg("feature_disabled", op="add", feature=f)
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return self._dry_result(items)
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        mod = _FEATURES.get(f)
        if not mod:
            return {
                "ok": False,
                "count": 0,
                "unresolved": [],
                "error": f"unknown_feature:{feature}",
            }
        cnt, unresolved = mod.add(self, lst)
        confirmed_keys = _confirmed_keys(self.key_of, lst, unresolved)
        return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> Mapping[str, Any]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _dbg("feature_disabled", op="remove", feature=f)
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run:
            return self._dry_result(items)
        lst = list(items)
        if not lst:
            return {"ok": True, "count": 0}
        mod = _FEATURES.get(f)
        if not mod:
            return {
                "ok": False,
                "count": 0,
                "unresolved": [],
                "error": f"unknown_feature:{feature}",
            }
        cnt, unresolved = mod.remove(self, lst)
        confirmed_keys = _confirmed_keys(self.key_of, lst, unresolved)
        return {"ok": True, "count": int(cnt), "unresolved": unresolved, "confirmed_keys": confirmed_keys}
class _EmbyOPS:
    def name(self) -> str:
        return "EMBY"

    def label(self) -> str:
        return "Emby"

    def features(self) -> Mapping[str, bool]:
        return EMBYModule.supported_features()

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": False,
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
        em = c.get("emby") or {}
        au = (c.get("auth") or {}).get("emby") or {}
        server = (em.get("server") or au.get("server") or "").strip()
        token = (em.get("access_token") or au.get("access_token") or "").strip()
        user_id = (em.get("user_id") or au.get("user_id") or "").strip()
        return bool(server and token and user_id)

    def _adapter(self, cfg: Mapping[str, Any]) -> EMBYModule:
        return EMBYModule(cfg)

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
    ) -> Mapping[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(
        self,
        cfg: Mapping[str, Any],
        items: Iterable[Mapping[str, Any]],
        *,
        feature: str,
        dry_run: bool = False,
    ) -> Mapping[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _EmbyOPS()
# /providers/sync/_mod_JELLYFIN.py
# CrossWatch JELLYFIN module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Callable, Iterable, Mapping

import requests

from cw_platform.provider_instances import normalize_instance_id
from cw_platform.value_coercion import coerce_bool

from ._log import log as cw_log

from .jellyfin._common import normalize as jelly_normalize, key_of as jelly_key_of, _pair_scope as _jf_pair_scope, state_file as _jf_state_file, _is_capture_mode as _jf_capture_mode
from .jellyfin import _watchlist as feat_watchlist
from .jellyfin import _history as feat_history
from .jellyfin import _ratings as feat_ratings
from .jellyfin import _progress as feat_progress
try:
    from .jellyfin import _playlists as feat_playlists
except Exception as e:
    feat_playlists = None
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_JELLYFIN_DEBUG"):
        cw_log("JELLYFIN", "playlists", "warn", "feature_import_failed", error=str(e))
from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,  # parity
    label_jellyfin,
    make_snapshot_progress,
    unresolved_keys as _unresolved_keys,
    build_op_result,
)


def _finalize_result(adapter: Any, key_of, feature: str, items, cnt: int, unresolved: Any) -> dict[str, Any]:
    meta = getattr(adapter, "_history_write_meta", None) if feature == "history" else None
    if isinstance(meta, Mapping):
        rc = meta.get("reason_counts")
        return build_op_result(
            ok=True,
            count=int(cnt),
            confirmed_keys=meta.get("confirmed_keys") or [],
            unresolved_keys=meta.get("unresolved_keys") or _unresolved_keys(unresolved, key_of),
            unresolved=unresolved,
            results=meta.get("results") or [],
            reason_counts=(dict(rc) if isinstance(rc, Mapping) else None),
        )
    results = list(getattr(adapter, "_progress_write_results", [])) if feature == "progress" else []
    return build_op_result(
        ok=True,
        count=int(cnt),
        confirmed_keys=_confirmed_keys(key_of, items, unresolved),
        unresolved_keys=_unresolved_keys(unresolved, key_of),
        unresolved=unresolved,
        results=results,
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

__VERSION__ = "2.0"
_MIN_PROGRESS_WRITE_VERSION = (10, 9)
_JF_VERSION_CACHE: dict[str, tuple[float, str | None]] = {}


def _version_tuple(value: Any) -> tuple[int, ...] | None:
    try:
        parts = str(value or "").strip().split(".")
        if len(parts) < 2 or not all(part.isdigit() for part in parts):
            return None
        return tuple(int(part) for part in parts)
    except Exception:
        return None


def _progress_version_supported(value: Any) -> bool:
    parsed = _version_tuple(value)
    return bool(parsed and parsed[:2] >= _MIN_PROGRESS_WRITE_VERSION)
os.environ.setdefault("CW_JELLYFIN_VERSION", __VERSION__)
os.environ.setdefault("CW_JELLYFIN_UA", f"CrossWatch/{__VERSION__} (Jellyfin)")
__all__ = ["get_manifest", "JELLYFINModule", "OPS"]

_DEF_UA = os.environ.get("CW_JELLYFIN_UA") or os.environ.get("CW_UA") or f"CrossWatch/{__VERSION__} (Jellyfin)"


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


_FEATURES: dict[str, Any] = {
    "watchlist": feat_watchlist,
    "history": feat_history,
    "ratings": feat_ratings,
    "progress": feat_progress,
}

_PLAYLIST_CAPABILITIES: dict[str, Any] = {
    "read": True,
    "create": True,
    "add": True,
    "remove": True,
    "reorder": False,
    "smart": False,
    "smart_writable": False,
    "media_types": ["movie", "show"],
    "endpoint_types": ["playlist", "collection"],
    "unordered_endpoint_types": ["collection"],
}

_HEALTH_SHADOW_NAME = "jellyfin.health.shadow.json"



def _dbg(feature: str, msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", feature, "debug", msg, **fields)


def _info(feature: str, msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", feature, "info", msg, **fields)


def _warn(feature: str, msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", feature, "warn", msg, **fields)


def _error(feature: str, msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", feature, "error", msg, **fields)


def _save_health_shadow(payload: Mapping[str, Any]) -> None:
    if _jf_pair_scope() is None or _jf_capture_mode():
        return
    try:
        path = str(_jf_state_file(_HEALTH_SHADOW_NAME))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, path)
    except Exception:
        pass


def _present_flags() -> dict[str, bool]:
    return {k: bool(v) for k, v in _FEATURES.items()}


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "JELLYFIN",
        "label": "Jellyfin",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": {
            "watchlist": True,
            "history": True,
            "ratings": False,
            "playlists": feat_playlists is not None,
            "progress": True,
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
            "playlists": _PLAYLIST_CAPABILITIES,
        },
    }


@dataclass
class JFConfig:
    server: str
    access_token: str
    user_id: str
    device_id: str = "crosswatch"
    verify_ssl: bool = True
    timeout: float = 15.0
    max_retries: int = 3
    strict_id_matching: bool = False
    watchlist_mode: str = "favorites"
    watchlist_playlist_name: str = "Watchlist"
    watchlist_query_limit: int = 25
    watchlist_write_delay_ms: int = 0
    watchlist_guid_priority: list[str] | None = None
    history_query_limit: int = 25
    history_write_delay_ms: int = 0
    history_guid_priority: list[str] | None = None
    history_libraries: list[str] | None = None
    progress_libraries: list[str] | None = None
    ratings_libraries: list[str] | None = None
    progress_replay_enabled: bool = False
    progress_timestamp_tolerance_seconds: int = 30


class JFClient:
    BASE_PATH_PING = "/System/Ping"
    BASE_PATH_INFO = "/System/Info"
    BASE_PATH_USER = "/Users/{user_id}"

    def __init__(self, cfg: JFConfig):
        if not cfg.server or not cfg.access_token or not cfg.user_id:
            raise RuntimeError("Jellyfin config requires server, access_token, user_id")
        self.cfg = cfg
        self.base = cfg.server.rstrip("/")
        self.session = build_session("JELLYFIN", ctx, feature_label=label_jellyfin)
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

    def server_version(self, *, ttl_seconds: int = 300) -> str | None:
        now = time.time()
        cached = _JF_VERSION_CACHE.get(self.base)
        if cached and now - cached[0] < max(1, int(ttl_seconds)):
            return cached[1]
        version: str | None = None
        try:
            response = self.system_info()
            if getattr(response, "status_code", 0) == 200:
                body = response.json() or {}
                raw = body.get("Version") or body.get("ServerVersion")
                version = str(raw).strip() if raw is not None and str(raw).strip() else None
        except Exception:
            version = None
        _JF_VERSION_CACHE[self.base] = (now, version)
        return version

    def progress_write_supported(self) -> tuple[bool, str | None]:
        version = self.server_version()
        return _progress_version_supported(version), version

    def user_probe(self) -> requests.Response:
        path = self.BASE_PATH_USER.format(user_id=self.cfg.user_id)
        return self.get(path)


class JELLYFINModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.instance_id = "default"
        inst = _pick_instance_id("JELLYFIN")
        jf = _merge_instance_block((cfg or {}).get("jellyfin") or {}, inst)
        auth = _merge_instance_block(dict((cfg or {}).get("auth") or {}).get("jellyfin") or {}, inst)
        jf.setdefault("server", auth.get("server"))
        jf.setdefault("access_token", auth.get("access_token"))
        jf.setdefault("user_id", auth.get("user_id"))

        wl = dict(jf.get("watchlist") or {})
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

        hi = dict(jf.get("history") or {})
        hi_qlim = int(hi.get("history_query_limit", 25) or 25)
        hi_wdel = int(hi.get("history_write_delay_ms", 0) or 0)
        hi_gprio = hi.get("history_guid_priority") or wl_gprio
        pr = dict(jf.get("progress") or {})
        ra = dict(jf.get("ratings") or {})

        def _list_str(v: Any) -> list[str] | None:
            if not v:
                return None
            rows = [str(x).strip() for x in v if str(x).strip()]
            return rows or None

        def _int_value(value: Any, default: int) -> int:
            try:
                return int(value)
            except Exception:
                return int(default)

        self.cfg = JFConfig(
            server=str(jf.get("server") or "").strip(),
            access_token=str(jf.get("access_token") or "").strip(),
            user_id=str(jf.get("user_id") or "").strip(),
            device_id=str(jf.get("device_id") or "crosswatch"),
            verify_ssl=coerce_bool(jf.get("verify_ssl", True), True),
            timeout=float((cfg or {}).get("timeout", jf.get("timeout", 15.0))),
            max_retries=int((cfg or {}).get("max_retries", jf.get("max_retries", 3))),
            strict_id_matching=coerce_bool(jf.get("strict_id_matching", False)),
            watchlist_mode=wl_mode,
            watchlist_playlist_name=wl_pname,
            watchlist_query_limit=wl_qlim,
            watchlist_write_delay_ms=wl_wdel,
            watchlist_guid_priority=list(wl_gprio),
            history_query_limit=hi_qlim,
            history_write_delay_ms=hi_wdel,
            history_guid_priority=list(hi_gprio),
            history_libraries=_list_str(hi.get("libraries")),
            progress_libraries=_list_str(pr.get("libraries")),
            ratings_libraries=_list_str(ra.get("libraries")),
            progress_replay_enabled=coerce_bool(pr.get("replay_enabled", jf.get("progress_replay_enabled", False))),
            progress_timestamp_tolerance_seconds=_int_value(pr.get("timestamp_tolerance_seconds", jf.get("progress_clock_drift_seconds", 30)), 30),
        )
        self.client = JFClient(self.cfg)

        def _mk_prog(feature: str):
            try:
                return make_snapshot_progress(ctx, dst="JELLYFIN", feature=feature)
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
        return jelly_normalize(obj)

    @staticmethod
    def key_of(obj: Any) -> str:
        return jelly_key_of(obj)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    @staticmethod
    def supported_features() -> dict[str, bool]:
        toggles = {"watchlist": True, "history": True, "ratings": False, "playlists": feat_playlists is not None, "progress": True}
        present = _present_flags()
        out = {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}
        out["playlists"] = bool(feat_playlists is not None)
        return out

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
            features = {k: False for k in ("watchlist", "history", "ratings", "playlists", "progress")}
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
        product = "Jellyfin Server"
        version = None
        progress_supported = False
        if auth_ok:
            try:
                progress_supported, version = self.client.progress_write_supported()
            except Exception:
                progress_supported, version = False, None

        base_ready = bool(server_ok and auth_ok)
        features = {
            "watchlist": base_ready if enabled.get("watchlist") else False,
            "history": base_ready if enabled.get("history") else False,
            "ratings": base_ready if enabled.get("ratings") else False,
            "playlists": base_ready if enabled.get("playlists") else False,
            "progress": bool(base_ready and progress_supported) if enabled.get("progress") else False,
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
        if enabled.get("progress") and auth_ok and not progress_supported:
            reasons.append(f"progress:unsupported_server_version:{version or 'unknown'}")

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

        _info("health", "health", status=status, ok=ok, latency_ms=latency_ms)

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
            _info(f, "index_skipped", reason="feature_disabled")
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
        if f == "progress":
            supported, version = self.client.progress_write_supported()
            if not supported:
                _info(f, "write_skipped", op="add", reason="unsupported_server_version", server_version=version)
                return {"ok": False, "count": 0, "unresolved": [], "confirmed_keys": [], "unresolved_keys": [], "results": [], "error": "unsupported_server_version", "server_version": version}
        if not self._is_enabled(f):
            _info(f, "write_skipped", op="add", reason="feature_disabled")
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
        try:
            setattr(self, "_history_write_meta", None)
        except Exception:
            pass
        cnt, unres = mod.add(self, lst)
        return _finalize_result(self, self.key_of, f, lst, cnt, unres)
    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> Mapping[str, Any]:
        f = (feature or "watchlist").lower()
        if f == "progress":
            supported, version = self.client.progress_write_supported()
            if not supported:
                _info(f, "write_skipped", op="remove", reason="unsupported_server_version", server_version=version)
                return {"ok": False, "count": 0, "unresolved": [], "confirmed_keys": [], "unresolved_keys": [], "results": [], "error": "unsupported_server_version", "server_version": version}
        if not self._is_enabled(f):
            _info(f, "write_skipped", op="remove", reason="feature_disabled")
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
        try:
            setattr(self, "_history_write_meta", None)
        except Exception:
            pass
        cnt, unres = mod.remove(self, lst)
        return _finalize_result(self, self.key_of, f, lst, cnt, unres)
class _JellyfinOPS:
    def name(self) -> str:
        return "JELLYFIN"

    def label(self) -> str:
        return "Jellyfin"

    def features(self) -> Mapping[str, bool]:
        return JELLYFINModule.supported_features()

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
            "playlists": _PLAYLIST_CAPABILITIES,
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}
        jf = c.get("jellyfin") or {}
        au = (c.get("auth") or {}).get("jellyfin") or {}

        server = (jf.get("server") or au.get("server") or "").strip()
        token = (jf.get("access_token") or au.get("access_token") or "").strip()
        user_id = (jf.get("user_id") or au.get("user_id") or "").strip()

        return bool(server and token and user_id)

    def progress_write_capability(self, cfg: Mapping[str, Any]) -> tuple[bool, str, str | None]:
        try:
            supported, version = self._adapter(cfg).client.progress_write_supported()
        except Exception:
            supported, version = False, None
        return supported, "" if supported else "unsupported_server_version", version

    def _adapter(self, cfg: Mapping[str, Any]) -> JELLYFINModule:
        return JELLYFINModule(cfg)

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

    def _pl(self) -> Any:
        if feat_playlists is None:
            raise RuntimeError("Jellyfin playlists feature is unavailable")
        return feat_playlists

    def _playlist_adapter(self, cfg: Mapping[str, Any], instance: str | None):
        ad = self._adapter(cfg)
        try:
            ad.instance_id = normalize_instance_id(instance)
        except Exception:
            ad.instance_id = "default"
        return ad

    def list_playlist_resources(self, cfg: Mapping[str, Any], *, instance: str | None = None):
        if feat_playlists is None:
            return []
        return list(self._pl().list_resources(self._playlist_adapter(cfg, instance)))

    def get_playlist_snapshot(self, cfg: Mapping[str, Any], playlist_id: str, *, instance: str | None = None):
        return self._pl().get_snapshot(self._playlist_adapter(cfg, instance), playlist_id)

    def create_playlist(
        self,
        cfg: Mapping[str, Any],
        name: str,
        *,
        media_type: str | None = None,
        items: Iterable[Mapping[str, Any]] | None = None,
        instance: str | None = None,
        dry_run: bool = False,
    ):
        return self._pl().create(
            self._playlist_adapter(cfg, instance),
            name,
            media_type=media_type,
            items=list(items or []),
            dry_run=dry_run,
        )

    def add_playlist_items(
        self,
        cfg: Mapping[str, Any],
        playlist_id: str,
        items: Iterable[Mapping[str, Any]],
        *,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items or [])
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True, "unresolved": [], "confirmed_keys": []}
        return self._pl().add(self._playlist_adapter(cfg, instance), playlist_id, lst)

    def remove_playlist_items(
        self,
        cfg: Mapping[str, Any],
        playlist_id: str,
        items: Iterable[Mapping[str, Any]],
        *,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        lst = list(items or [])
        if dry_run:
            return {"ok": True, "count": len(lst), "dry_run": True, "unresolved": [], "confirmed_keys": []}
        return self._pl().remove(self._playlist_adapter(cfg, instance), playlist_id, lst)

    def reorder_playlist_items(
        self,
        cfg: Mapping[str, Any],
        playlist_id: str,
        ordered_keys: Iterable[str],
        *,
        instance: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        keys = list(ordered_keys or [])
        if dry_run:
            return {"ok": True, "count": 0, "dry_run": True}
        return self._pl().reorder(self._playlist_adapter(cfg, instance), playlist_id, keys)

OPS = _JellyfinOPS()

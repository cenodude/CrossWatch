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

from ._log import log as cw_log

from .jellyfin._common import normalize as jelly_normalize, key_of as jelly_key_of, _pair_scope as _jf_pair_scope, state_file as _jf_state_file
from .jellyfin import _watchlist as feat_watchlist
from .jellyfin import _history as feat_history
from .jellyfin import _ratings as feat_ratings
from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,  # parity
    label_jellyfin,
    make_snapshot_progress,
)

try:  # type: ignore[name-defined]
    ctx  # type: ignore[misc]
except Exception:
    ctx = None  # type: ignore[assignment]

__VERSION__ = "2.1.0"
__all__ = ["get_manifest", "JELLYFINModule", "OPS"]

_DEF_UA = os.environ.get("CW_UA", f"CrossWatch/{__VERSION__} (Jellyfin)")

_FEATURES: dict[str, Any] = {
    "watchlist": feat_watchlist,
    "history": feat_history,
    "ratings": feat_ratings,
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
    if _jf_pair_scope() is None:
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
class JFConfig:
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
    history_libraries: list[str] | None = None
    ratings_libraries: list[str] | None = None


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

    def user_probe(self) -> requests.Response:
        path = self.BASE_PATH_USER.format(user_id=self.cfg.user_id)
        return self.get(path)


class JELLYFINModule:
    def __init__(self, cfg: Mapping[str, Any]):
        jf = dict((cfg or {}).get("jellyfin") or {})
        auth = dict((cfg or {}).get("auth") or {}).get("jellyfin") or {}
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
        ra = dict(jf.get("ratings") or {})

        def _list_str(v: Any) -> list[str] | None:
            if not v:
                return None
            rows = [str(x).strip() for x in v if str(x).strip()]
            return rows or None

        self.cfg = JFConfig(
            server=str(jf.get("server") or "").strip(),
            access_token=str(jf.get("access_token") or "").strip(),
            user_id=str(jf.get("user_id") or "").strip(),
            device_id=str(jf.get("device_id") or "crosswatch"),
            verify_ssl=bool(jf.get("verify_ssl", True)),
            timeout=float((cfg or {}).get("timeout", jf.get("timeout", 15.0))),
            max_retries=int((cfg or {}).get("max_retries", jf.get("max_retries", 3))),
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
        toggles = {"watchlist": True, "history": True, "ratings": True, "playlists": False}
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
        product = "Jellyfin Server"
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
            _dbg(f, "build index skipped", reason="feature disabled")
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
            _dbg(f, "add skipped", reason="feature disabled")
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
        cnt, unres = mod.add(self, lst)
        return {"ok": True, "count": int(cnt), "unresolved": unres}

    def remove(
        self,
        feature: str,
        items: Iterable[Mapping[str, Any]],
        *,
        dry_run: bool = False,
    ) -> Mapping[str, Any]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _dbg(f, "remove skipped", reason="feature disabled")
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
        cnt, unres = mod.remove(self, lst)
        return {"ok": True, "count": int(cnt), "unresolved": unres}


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
        }

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c = cfg or {}
        jf = c.get("jellyfin") or {}
        au = (c.get("auth") or {}).get("jellyfin") or {}

        server = (jf.get("server") or au.get("server") or "").strip()
        token = (jf.get("access_token") or au.get("access_token") or "").strip()
        user_id = (jf.get("user_id") or au.get("user_id") or "").strip()

        return bool(server and token and user_id)

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

OPS = _JellyfinOPS()
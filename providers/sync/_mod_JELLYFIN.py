# /providers/sync/_mod_JELLYFIN.py
# Jellyfin adapter: manifest + client + feature registry + OPS wrapper.

from __future__ import annotations
__VERSION__ = "1.0.0"
__all__ = ["get_manifest", "JELLYFINModule", "OPS"]

import os, time, json, requests
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Mapping, Optional, Tuple, List, Callable

# Debug gate
_DEF_UA = os.environ.get("CW_UA", f"CrossWatch/{__VERSION__} (Jellyfin)")

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_JELLYFIN_DEBUG"):
        print(f"[JELLYFIN] {msg}")

from .jellyfin._common import normalize as jelly_normalize, key_of as jelly_key_of
from .jellyfin import _watchlist as feat_watchlist
from .jellyfin import _history   as feat_history
from .jellyfin import _ratings   as feat_ratings
from .jellyfin import _playlists as feat_playlists

# Instrumentation (session + progress)
from ._mod_common import (
    build_session,
    request_with_retries,
    parse_rate_limit,   # kept for parity
    label_jellyfin,
    make_snapshot_progress,
)
try:  # type: ignore[name-defined]
    ctx  # type: ignore
except Exception:
    ctx = None  # type: ignore

# ──────────────────────────────────────────────────────────────────────────────
# manifest

def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "JELLYFIN",
        "label": "Jellyfin",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": True,
        "features": {
            "watchlist": True,
            "history":   True,
            "ratings":   True,
            "playlists": False,
        },
        "requires": ["requests"],
        "capabilities": {
            "bidirectional": True,
            "provides_ids": False,
            "index_semantics": "present",
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True, "unrate": True, "from_date": False
            },
        },
    }

# ──────────────────────────────────────────────────────────────────────────────
# config + client

@dataclass
class JFConfig:
    server: str
    access_token: str
    user_id: str
    device_id: str = "crosswatch"
    verify_ssl: bool = True
    timeout: float = 15.0
    max_retries: int = 3
    # Watchlist
    watchlist_mode: str = "favorites"            # or "playlist"
    watchlist_playlist_name: str = "Watchlist"
    watchlist_query_limit: int = 25
    watchlist_write_delay_ms: int = 0
    watchlist_guid_priority: Optional[List[str]] = None
    # History
    history_query_limit: int = 25
    history_write_delay_ms: int = 0
    history_guid_priority: Optional[List[str]] = None
    # Library scoping (None = all)
    history_libraries: Optional[List[str]] = None
    ratings_libraries: Optional[List[str]] = None

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
        auth_val = (f'MediaBrowser Client="CrossWatch", Device="CrossWatch", '
                    f'DeviceId="{cfg.device_id}", Version="{__VERSION__}", Token="{cfg.access_token}"')
        self.session.headers.update({
            "Accept": "application/json",
            "User-Agent": _DEF_UA,
            "Authorization": auth_val,
            "X-Emby-Authorization": auth_val,
            "X-MediaBrowser-Token": cfg.access_token,
        })

    def _url(self, path: str) -> str:
        return self.base + (path if path.startswith("/") else ("/" + path))

    # Central request path (retries centralized)
    def _request(self, method: str, path: str, *, params: Optional[dict] = None, json: Any = None) -> requests.Response:
        return request_with_retries(
            self.session, method, self._url(path),
            params=(params or {}), json=json,
            timeout=self.cfg.timeout, max_retries=self.cfg.max_retries,
        )

    # Thin wrappers for features
    def get(self, path: str, *, params: Optional[dict] = None) -> requests.Response:
        return self._request("GET", path, params=params)

    def post(self, path: str, *, params: Optional[dict] = None, json: Any = None) -> requests.Response:
        return self._request("POST", path, params=params, json=json)

    def delete(self, path: str, *, params: Optional[dict] = None) -> requests.Response:
        return self._request("DELETE", path, params=params)

    # Health primitives
    def ping(self) -> requests.Response: return self.get(self.BASE_PATH_PING)
    def system_info(self) -> requests.Response: return self.get(self.BASE_PATH_INFO)
    def user_probe(self) -> requests.Response:
        p = self.BASE_PATH_USER.format(user_id=self.cfg.user_id)
        return self.get(p)

# ──────────────────────────────────────────────────────────────────────────────
# module wrapper

_HEALTH_SHADOW = "/config/.cw_state/jellyfin.health.shadow.json"

def _save_health_shadow(payload: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(_HEALTH_SHADOW), exist_ok=True)
        tmp = f"{_HEALTH_SHADOW}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2, sort_keys=True)
        os.replace(tmp, _HEALTH_SHADOW)
    except Exception:
        pass

def _present_flags() -> Dict[str, bool]:
    return {
        "watchlist": bool(feat_watchlist),
        "history":   bool(feat_history),
        "ratings":   bool(feat_ratings),
        "playlists": bool(feat_playlists),
    }

class JELLYFINModule:
    def __init__(self, cfg: Mapping[str, Any]):
        # Pull provider cfg
        jf = dict((cfg or {}).get("jellyfin") or {})
        # Legacy nesting support
        auth = dict((cfg or {}).get("auth") or {}).get("jellyfin") or {}
        jf.setdefault("server", auth.get("server"))
        jf.setdefault("access_token", auth.get("access_token"))
        jf.setdefault("user_id", auth.get("user_id"))

        # Watchlist sub-config
        wl = dict(jf.get("watchlist") or {})
        wl_mode = str(wl.get("mode") or "favorites").strip().lower()
        wl_pname = (wl.get("playlist_name") or "Watchlist").strip() or "Watchlist"
        wl_qlim = int(wl.get("watchlist_query_limit", 25) or 25)
        wl_wdel = int(wl.get("watchlist_write_delay_ms", 0) or 0)
        wl_gprio = wl.get("watchlist_guid_priority") or [
            "tmdb","imdb","tvdb","agent:themoviedb:en","agent:themoviedb","agent:imdb"
        ]

        # History sub-config
        hi = dict(jf.get("history") or {})
        hi_qlim = int(hi.get("history_query_limit", 25) or 25)
        hi_wdel = int(hi.get("history_write_delay_ms", 0) or 0)
        hi_gprio = hi.get("history_guid_priority") or wl_gprio

        # Ratings sub-config
        ra = dict(jf.get("ratings") or {})

        def _list_str(v):
            return [str(x).strip() for x in (v or []) if str(x).strip()]

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

        # Progress helper used by features
        def _mk_prog(feature: str):
            try: return make_snapshot_progress(ctx, dst="JELLYFIN", feature=feature)
            except Exception:
                class _Noop:
                    def tick(self, *a, **k): pass
                    def done(self, *a, **k): pass
                return _Noop()
        self.progress_factory: Callable[[str], Any] = _mk_prog

    # Shared utils (exposed to features)
    @staticmethod
    def normalize(obj) -> Dict[str, Any]: return jelly_normalize(obj)
    @staticmethod
    def key_of(obj) -> str:    return jelly_key_of(obj)

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    # Feature toggles (masked by presence)
    @staticmethod
    def supported_features() -> Dict[str, bool]:
        toggles = {"watchlist": True, "history": True, "ratings": True, "playlists": False}
        present = _present_flags()
        return {k: bool(toggles.get(k, False) and present.get(k, False)) for k in toggles.keys()}

    def _is_enabled(self, feature: str) -> bool:
        return bool(self.supported_features().get(feature, False))

    # Health for orchestrator
    def health(self) -> Mapping[str, Any]:
        enabled = self.supported_features()
        need_any = any(enabled.values())
        start = time.perf_counter()

        if not need_any:
            latency_ms = int((time.perf_counter() - start) * 1000)
            details: Dict[str, Any] = {
                "server_ok": False, "auth_ok": False,
                "server": {"product": None, "version": None},
                "disabled": [k for k, v in enabled.items() if not v],
            }
            features = {k: False for k in ("watchlist", "history", "ratings", "playlists")}
            api = {"ping": {"status": None}, "info": {"status": None}, "user": {"status": None}}
            return {"ok": True, "status": "ok", "latency_ms": latency_ms, "features": features, "details": details, "api": api}
        
        # Single-call health: user probe
        try:
            ru = self.client.user_probe()
        except Exception:
            ru = None

        latency_ms = int((time.perf_counter() - start) * 1000)

        user_code = getattr(ru, "status_code", None) if ru is not None else None
        user_ok = bool(getattr(ru, "ok", False)) if ru is not None else False
        server_ok = bool(user_ok and user_code is not None and user_code < 500)
        auth_ok = bool(user_ok and user_code == 200)
        product = "Jellyfin Server"
        version = None

        base_ready = bool(server_ok and auth_ok)
        features = {
            "watchlist": base_ready if enabled.get("watchlist") else False,
            "history":   base_ready if enabled.get("history")   else False,
            "ratings":   base_ready if enabled.get("ratings")   else False,
            "playlists": base_ready if enabled.get("playlists") else False,
        }

        checks: List[bool] = [features[k] for k, on in enabled.items() if on]
        if checks and all(checks):
            status = "ok"
        elif checks and any(checks):
            status = "degraded"
        else:
            status = "auth_failed" if (user_code in (401, 403)) else "down"

        ok = status in ("ok", "degraded")

        reasons: List[str] = []
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

        details: Dict[str, Any] = {
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
            "user": {"status": user_code},
        }

        try:
            _save_health_shadow({
                "ts": int(time.time()),
                "status": status,
                "api": api,
                "server_ok": server_ok,
                "auth_ok": auth_ok,
                "disabled": disabled,
            })
        except Exception:
            pass

        _log(f"health status={status} ok={ok} latency_ms={latency_ms} reason={details.get('reason') if 'reason' in details else None}")

        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": features,
            "details": details if details else None,
            "api": api,
        }

    # Feature dispatch (present-state)
    def feature_names(self) -> Tuple[str, ...]:
        enabled = self.supported_features()
        return tuple(k for k, v in enabled.items() if v)

    def build_index(self, feature: str, **kwargs) -> Mapping[str, Dict[str, Any]]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _log(f"build_index skipped: feature disabled: {f}")
            return {}
        if f == "watchlist": return feat_watchlist.build_index(self, **kwargs)
        if f == "history":   return feat_history.build_index(self, **kwargs)
        if f == "ratings":   return feat_ratings.build_index(self, **kwargs)
        if f == "playlists": return feat_playlists.build_index(self, **kwargs)
        return {}

    def _dry_result(self, items: Iterable[Mapping[str, Any]]) -> Mapping[str, Any]:
        lst = list(items)
        return {"ok": True, "count": len(lst), "dry_run": True}

    def add(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool=False) -> Mapping[str, Any]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _log(f"add skipped: feature disabled: {f}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run: return self._dry_result(items)
        lst = list(items)
        if not lst: return {"ok": True, "count": 0}
        if f == "watchlist": cnt, unres = feat_watchlist.add(self, lst)
        elif f == "history": cnt, unres = feat_history.add(self, lst)
        elif f == "ratings": cnt, unres = feat_ratings.add(self, lst)
        elif f == "playlists": cnt, unres = feat_playlists.add(self, lst)
        else: return {"ok": False, "count": 0, "unresolved": [], "error": f"unknown_feature:{feature}"}
        return {"ok": True, "count": int(cnt), "unresolved": unres}

    def remove(self, feature: str, items: Iterable[Mapping[str, Any]], *, dry_run: bool=False) -> Mapping[str, Any]:
        f = (feature or "watchlist").lower()
        if not self._is_enabled(f):
            _log(f"remove skipped: feature disabled: {f}")
            return {"ok": True, "count": 0, "unresolved": []}
        if dry_run: return self._dry_result(items)
        lst = list(items)
        if not lst: return {"ok": True, "count": 0}
        if f == "watchlist": cnt, unres = feat_watchlist.remove(self, lst)
        elif f == "history": cnt, unres = feat_history.remove(self, lst)
        elif f == "ratings": cnt, unres = feat_ratings.remove(self, lst)
        elif f == "playlists": cnt, unres = feat_playlists.remove(self, lst)
        else: return {"ok": False, "count": 0, "unresolved": [], "error": f"unknown_feature:{feature}"}
        return {"ok": True, "count": int(cnt), "unresolved": unres}


# ──────────────────────────────────────────────────────────────────────────────
# OPS bridge (orchestrator contract)

class _JellyfinOPS:
    def name(self) -> str: return "JELLYFIN"
    def label(self) -> str: return "Jellyfin"
    def features(self) -> Mapping[str, bool]:
        return JELLYFINModule.supported_features()
    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": False,
            "index_semantics": "present",
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": True, "episodes": True},
                "upsert": True, "unrate": True, "from_date": False
            },
        }
        
    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        c  = cfg or {}
        jf = c.get("jellyfin") or {}
        au = (c.get("auth") or {}).get("jellyfin") or {}

        server  = (jf.get("server")       or au.get("server")       or "").strip()
        token   = (jf.get("access_token") or au.get("access_token") or "").strip()
        user_id = (jf.get("user_id")      or au.get("user_id")      or "").strip()

        return bool(server and token and user_id)

    def _adapter(self, cfg: Mapping[str, Any]) -> JELLYFINModule:
        return JELLYFINModule(cfg)

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        return self._adapter(cfg).build_index(feature)

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Mapping[str, Any]:
        return self._adapter(cfg).add(feature, items, dry_run=dry_run)

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Mapping[str, Any]:
        return self._adapter(cfg).remove(feature, items, dry_run=dry_run)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

OPS = _JellyfinOPS()
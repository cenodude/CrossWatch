from __future__ import annotations

__VERSION__ = "1.3.0"
__all__ = ["OPS", "SIMKLModule", "get_manifest"]

import json
import time
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

import requests

# Track SIMKL API call counts for debugging (process-global)
_CALLS = getattr(globals(), "_CALLS", {"GET": 0, "POST": 0})
globals()["_CALLS"] = _CALLS

# In-run GET memoization: collapses repeated identical GETs within the same process/run.
_RUN_GET_CACHE: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Tuple[float, Optional[requests.Response]]] = {}

def _norm_params(p: Optional[dict]) -> Tuple[Tuple[str, str], ...]:
    if not p:
        return tuple()
    out = []
    for k, v in p.items():
        if isinstance(v, (list, tuple)):
            out.append((str(k), ",".join(map(str, v))))
        else:
            out.append((str(k), str(v)))
    out.sort()
    return tuple(out)


# Core SIMKL endpoints and constants
UA = "CrossWatch/Module"
SIMKL_BASE = "https://api.simkl.com"

SIMKL_SYNC_ACTIVITIES = f"{SIMKL_BASE}/sync/activities"

# Plan-to-watch (PTW)
SIMKL_ALL_ITEMS_MOVIES_PTW = f"{SIMKL_BASE}/sync/all-items/movies/plantowatch"
SIMKL_ALL_ITEMS_SHOWS_PTW  = f"{SIMKL_BASE}/sync/all-items/shows/plantowatch"
SIMKL_ALL_ITEMS_ANIME_PTW  = f"{SIMKL_BASE}/sync/all-items/anime/plantowatch"
SIMKL_ADD_TO_LIST          = f"{SIMKL_BASE}/sync/add-to-list"
SIMKL_HISTORY_REMOVE       = f"{SIMKL_BASE}/sync/history/remove"  # PTW removal via history API

# History
SIMKL_HISTORY_ADD          = f"{SIMKL_BASE}/sync/history"          # GET supports date_from

# Ratings
SIMKL_RATINGS_GET          = f"{SIMKL_BASE}/sync/ratings"
SIMKL_RATINGS_REMOVE       = f"{SIMKL_BASE}/sync/ratings/remove"
SIMKL_RATINGS_SET          = f"{SIMKL_BASE}/sync/ratings"

_ID_KEYS = ("simkl", "imdb", "tmdb", "tvdb", "slug")

# Optional host integrations
try:
    from _logging import log as host_log
except Exception:  # pragma: no cover
    def host_log(*a, **k):  # type: ignore
        pass

try:
    from _statistics import Stats  # type: ignore
    _stats = Stats()
except Exception:
    _stats = None

# Try to get config location for state co-location
try:
    from cw_platform.config_base import CONFIG  # type: ignore
except Exception:
    CONFIG = None  # type: ignore

# ---------------------------------------------------------------------------
# Small helpers
# ---------------------------------------------------------------------------

def _cfg_get(cfg_root: Mapping[str, Any], path: str, default: Any = None) -> Any:
    """Dot-path fetch from nested config."""
    parts = path.split(".")
    cur: Any = cfg_root
    try:
        for p in parts:
            if not isinstance(cur, Mapping):
                return default
            cur = cur.get(p)
            if cur is None:
                return default
        return cur
    except Exception:
        return default

def _emit_rating_event(*, action: str, node: Mapping[str, Any], prev: Optional[int], value: Optional[int]) -> None:
    """Emit compact rating event for UI spotlight/summary."""
    try:
        payload = {
            "feature": "ratings",
            "action": action,                 # "rate" | "unrate" | "update"
            "title": node.get("title"),
            "type": node.get("type"),
            "ids": dict(node.get("ids") or {}),
            "value": value,
            "prev": prev,
            "provider": "SIMKL",
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()))),
        }
        if _stats and hasattr(_stats, "record_event"):
            try:
                _stats.record_event(payload)
            except Exception:
                pass
        try:
            host_log("event", payload)
        except Exception:
            pass
    except Exception:
        pass  # never break writes due to telemetry

# ---------------------------------------------------------------------------
# Global Tombstones (optional)
# ---------------------------------------------------------------------------

_GMT_ENABLED_DEFAULT = True

try:
    from providers.gmt_hooks import suppress_check as _gmt_suppress, record_negative as _gmt_record  # type: ignore
    from cw_platform.gmt_store import GlobalTombstoneStore  # type: ignore
    _HAS_GMT = True
except Exception:  # pragma: no cover
    _HAS_GMT = False
    GlobalTombstoneStore = None  # type: ignore
    def _gmt_suppress(**_kwargs) -> bool:  # type: ignore
        return False
    def _gmt_record(**_kwargs) -> None:  # type: ignore
        return

def _gmt_is_enabled(cfg: Mapping[str, Any]) -> bool:
    sync = dict(cfg.get("sync") or {})
    val = sync.get("gmt_enable")
    if val is None:
        return _GMT_ENABLED_DEFAULT
    return bool(val)

def _gmt_ops_for_feature(feature: str) -> Tuple[str, str]:
    f = (feature or "").lower()
    if f == "ratings":
        return "rate", "unrate"
    if f == "history":
        return "scrobble", "unscrobble"
    return "add", "remove"  # watchlist

def _gmt_store_from_cfg(cfg: Mapping[str, Any]) -> Optional[GlobalTombstoneStore]:
    if not _HAS_GMT or not _gmt_is_enabled(cfg):
        return None
    try:
        ttl_days = int(((cfg.get("sync") or {}).get("gmt_quarantine_days") or (cfg.get("sync") or {}).get("tombstone_ttl_days") or 7))
        return GlobalTombstoneStore(ttl_sec=max(1, ttl_days) * 24 * 3600)
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def iso_to_ts(s: str) -> int:
    """Parse ISO-8601 string into epoch seconds. Returns 0 on failure."""
    if not s:
        return 0
    try:
        import datetime as dt
        return int(dt.datetime.strptime(s.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z").timestamp())
    except Exception:
        try:
            import datetime as dt
            return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0


def ts_to_iso(ts: int) -> str:
    """Format epoch seconds into UTC ISO-8601."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


# ---------------------------------------------------------------------------
# Disk HTTP cache (GET) under /config/.cw_state
# ---------------------------------------------------------------------------

_HTTP_CACHE_FILE_NAME = "simkl_http_cache.json"
_HTTP_CACHE_TTL_SEC   = 60
_HTTP_CACHE_MAX_ENTRIES = 512

def _resolve_state_dir(root_cfg: Mapping[str, Any]) -> Path:
    """
    Choose where durable state lives:
    1) cfg.runtime.state_dir
    2) env: CW_STATE_DIR
    3) next to config.json (CONFIG.path)
    4) /config (Docker volume convention)
    5) cwd (last resort)
    """
    try:
        p = (root_cfg.get("runtime") or {}).get("state_dir")
        if p:
            return Path(p)
    except Exception:
        pass

    p = os.environ.get("CW_STATE_DIR")
    if p:
        return Path(p)

    try:
        if CONFIG and getattr(CONFIG, "path", None):
            return Path(CONFIG.path).parent  # type: ignore[attr-defined]
    except Exception:
        pass

    if Path("/config").exists():
        return Path("/config")

    return Path.cwd()


def _http_cache_path(root_cfg: Optional[Mapping[str, Any]] = None) -> Path:
    base = _resolve_state_dir(root_cfg or {})
    p = base / ".cw_state"
    try:
        p.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return p / _HTTP_CACHE_FILE_NAME


def _load_http_cache(root_cfg: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    path = _http_cache_path(root_cfg)
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return {"map": {}, "order": []}


def _save_http_cache(cache: Mapping[str, Any], root_cfg: Optional[Mapping[str, Any]] = None) -> None:
    path = _http_cache_path(root_cfg)
    tmp = path.with_suffix(path.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(cache, ensure_ascii=False), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def _http_cache_key(url: str, params: Optional[dict]) -> str:
    return f"{url}::{json.dumps(dict(params or {}), sort_keys=True)}"


def _http_cache_get(root_cfg: Mapping[str, Any], url: str, params: Optional[dict]) -> Optional[Dict[str, Any]]:
    cache = _load_http_cache(root_cfg)
    key = _http_cache_key(url, params)
    ent = (cache.get("map") or {}).get(key)
    if not ent:
        return None
    try:
        if (time.time() - float(ent.get("ts", 0))) > _HTTP_CACHE_TTL_SEC:
            return None
    except Exception:
        return None
    return ent


def _http_cache_peek_ts(root_cfg: Mapping[str, Any], url: str, params: Optional[dict]) -> float:
    """Return cache entry epoch timestamp or 0 when absent/expired."""
    ent = _http_cache_get(root_cfg, url, params)
    try:
        return float(ent.get("ts", 0)) if ent else 0.0
    except Exception:
        return 0.0


def _http_cache_put(root_cfg: Mapping[str, Any], url: str, params: Optional[dict], response: requests.Response) -> None:
    try:
        body = response.content or b""
        etag = None
        try:
            etag = response.headers.get("ETag")
        except Exception:
            etag = None
        ent = {
            "ts": time.time(),
            "status": int(getattr(response, "status_code", 0) or 0),
            "etag": etag,
            "headers": dict(getattr(response, "headers", {}) or {}),
            "body": body.decode("utf-8", errors="ignore"),
        }
        cache = _load_http_cache(root_cfg)
        key = _http_cache_key(url, params)
        cache_map = cache.get("map") or {}
        cache_order = cache.get("order") or []
        cache_map[key] = ent
        try:
            cache_order.remove(key)
        except Exception:
            pass
        cache_order.append(key)
        while len(cache_order) > _HTTP_CACHE_MAX_ENTRIES:
            victim = cache_order.pop(0)
            cache_map.pop(victim, None)
        cache["map"], cache["order"] = cache_map, cache_order
        _save_http_cache(cache, root_cfg)
    except Exception:
        pass


class _CachedResponse:
    """Response-like wrapper to serve JSON from disk cache uniformly."""
    def __init__(self, ent: Mapping[str, Any]):
        self._ent = dict(ent)
        self.status_code = int(ent.get("status", 200))
        self.ok = 200 <= self.status_code < 300
        self.headers = dict(ent.get("headers") or {})
        self.content = (ent.get("body") or "").encode("utf-8")
        self.elapsed = 0

    def json(self) -> Any:
        try:
            return json.loads(self._ent.get("body") or "null")
        except Exception:
            return None


# ---------------------------------------------------------------------------
# Provider protocol
# ---------------------------------------------------------------------------

class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]: ...


# ---------------------------------------------------------------------------
# Cursor, shadows, and cache bust
# ---------------------------------------------------------------------------

class _CursorStore:
    """
    Stores per-feature cursors and a watchlist shadow in a durable, atomic way.
    State lives in `<state_dir>/.cw_state/` (defaults to /config/.cw_state in Docker).
    """

    def __init__(self, root_cfg: Mapping[str, Any]):
        base = _resolve_state_dir(root_cfg)
        base_dir = (base / ".cw_state")

        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            base_dir = Path.cwd() / ".cw_state"
            base_dir.mkdir(parents=True, exist_ok=True)

        try:
            old_dir = Path("/app/.cw_state")
            if old_dir.exists() and old_dir != base_dir:
                for name in ("simkl_cursors.json", "simkl_watchlist.shadow.json"):
                    src = old_dir / name
                    dst = base_dir / name
                    if src.exists() and not dst.exists():
                        dst.write_text(src.read_text("utf-8"), "utf-8")
        except Exception:
            pass

        self.file = base_dir / "simkl_cursors.json"
        self.shadow_file = base_dir / "simkl_watchlist.shadow.json"

    def _read(self) -> Dict[str, Any]:
        try:
            return json.loads(self.file.read_text("utf-8"))
        except Exception:
            return {}

    def _atomic_write(self, path: Path, data: Mapping[str, Any]) -> None:
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, path)

    def get(self, feature: str) -> Optional[str]:
        return (self._read() or {}).get(feature)

    def set(self, feature: str, iso_ts: str) -> None:
        data = self._read() or {}
        data[feature] = iso_ts
        self._atomic_write(self.file, data)

    def load_shadow(self) -> Dict[str, Any]:
        try:
            return json.loads(self.shadow_file.read_text("utf-8"))
        except Exception:
            return {"items": {}, "last_sync_ts": 0}

    def save_shadow(self, items: Mapping[str, Any], last_sync_ts: int) -> None:
        data = {"items": dict(items or {}), "last_sync_ts": int(last_sync_ts)}
        self._atomic_write(self.shadow_file, data)

def _ratings_shadow_path(cfg_root: Mapping[str, Any]) -> Path:
    base = _resolve_state_dir(cfg_root) / ".cw_state"
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base / "simkl_ratings.shadow.json"

def _ratings_shadow_load(cfg_root: Mapping[str, Any]) -> Dict[str, Any]:
    p = _ratings_shadow_path(cfg_root)
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {"items": {}, "ts": 0}

def _ratings_shadow_save(cfg_root: Mapping[str, Any], items: Mapping[str, Any]) -> None:
    p = _ratings_shadow_path(cfg_root)
    try:
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps({"items": dict(items), "ts": int(time.time())}, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, p)
    except Exception:
        pass

def _clear_run_cache() -> None:
    """Clear in-process GET memoization. Safe to call every sync start."""
    try:
        _RUN_GET_CACHE.clear()
    except Exception:
        pass

def _bust_ptw_cache(cfg_root: Mapping[str, Any]) -> None:
    try:
        cache = _load_http_cache(cfg_root)
        m = cache.get("map") or {}
        order = cache.get("order") or []
        def drop(url: str, params: dict):
            key = _http_cache_key(url, params)
            m.pop(key, None)
            try: order.remove(key)
            except Exception: pass
        # present-snapshot keys we use in _ptw_fetch_present
        base_params = {"extended": "full", "episode_watched_at": "yes", "memos": "yes", "date_from": "1970-01-01T00:00:00Z"}
        drop(SIMKL_ALL_ITEMS_MOVIES_PTW, base_params)
        drop(SIMKL_ALL_ITEMS_SHOWS_PTW,  base_params)
        cache["map"], cache["order"] = m, order
        _save_http_cache(cache, cfg_root)
    except Exception:
        pass

def _bust_ratings_cache(cfg_root: Mapping[str, Any]) -> None:
    """Remove all cached entries for the ratings endpoint."""
    try:
        cache = _load_http_cache(cfg_root)
        m = cache.get("map") or {}
        order = cache.get("order") or []
        prefix = f"{SIMKL_RATINGS_GET}::"
        victims = [k for k in list(m.keys()) if k.startswith(prefix)]
        for k in victims:
            m.pop(k, None)
            try:
                order.remove(k)
            except Exception:
                pass
        cache["map"], cache["order"] = m, order
        _save_http_cache(cache, cfg_root)
    except Exception:
        pass

# ---------------------------------------------------------------------------
# Activities (cached per run)
# ---------------------------------------------------------------------------

_RUN_ACT = {"ts": 0.0, "data": None}
_ACT_TTL_SEC = 60  # Short TTL to surface interactive changes quickly

def _activities(cfg_root: Mapping[str, Any]) -> Dict[str, Any]:
    """Fetch SIMKL activities once per TTL; flatten a few nested keys."""
    now = time.time()
    if (now - float(_RUN_ACT.get("ts") or 0)) < _ACT_TTL_SEC and (_RUN_ACT.get("data") is not None):
        return dict(_RUN_ACT["data"])
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    try:
        r = _simkl_get(SIMKL_SYNC_ACTIVITIES, headers=simkl_headers(simkl_cfg), timeout=30, cfg_root=cfg_root)
        data = _json_or_empty_dict(r)
    except Exception:
        data = {}
    out: Dict[str, Any] = {}
    for k in ("watchlist", "ratings", "history", "completed", "lists", "episodes", "movies", "shows"):
        if isinstance(data.get(k), (str, int)):
            out[k] = data[k]
        elif isinstance(data.get(k), dict):
            for sk, sv in data[k].items():
                if isinstance(sv, (str, int)):
                    out[f"{k}.{sk}"] = sv
    _RUN_ACT.update(ts=now, data=out)
    return out

# ---------------------------------------------------------------------------
# HTTP recording & backoff wrappers
# ---------------------------------------------------------------------------

def _record_http_simkl(r: Optional[requests.Response], *, endpoint: str, method: str, payload: Any = None, count: bool = True) -> None:
    """
    Record HTTP telemetry if host statistics are available.
    Also increments global GET/POST counters for debug analysis (unless count=False for cache hits).
    """
    if count:
        try:
            if method == "GET":
                _CALLS["GET"] = _CALLS.get("GET", 0) + 1
            elif method == "POST":
                _CALLS["POST"] = _CALLS.get("POST", 0) + 1
        except Exception:
            pass

    if not _stats:
        return

    try:
        status = int(getattr(r, "status_code", 0) or 0)
        ok = bool(getattr(r, "ok", False))
        bytes_in = len(getattr(r, "content", b"") or b"") if r is not None else 0

        if isinstance(payload, (bytes, bytearray)):
            bytes_out = len(payload)
        elif payload is None:
            bytes_out = 0
        else:
            try:
                bytes_out = len(json.dumps(payload))
            except Exception:
                bytes_out = 0

        ms = int(getattr(r, "elapsed", 0).total_seconds() * 1000) if (r is not None and getattr(r, "elapsed", None)) else 0

        rate_remaining = None
        rate_reset_iso = None
        try:
            if r is not None:
                rem = r.headers.get("X-RateLimit-Remaining")
                rst = r.headers.get("X-RateLimit-Reset")
                if rem is not None:
                    rate_remaining = int(rem)
                if rst:
                    try:
                        rst_i = int(rst)
                        rate_reset_iso = ts_to_iso(rst_i) if rst_i > 0 else None
                    except Exception:
                        rate_reset_iso = None
        except Exception:
            pass

        _stats.record_http(
            provider="SIMKL",
            endpoint=endpoint,
            method=method,
            status=status,
            ok=ok,
            bytes_in=bytes_in,
            bytes_out=bytes_out,
            ms=ms,
            rate_remaining=rate_remaining,
            rate_reset_iso=rate_reset_iso,
        )
    except Exception:
        pass


def _with_backoff(req_fn, *a, **kw) -> Optional[requests.Response]:
    """
    Defensive retries with exponential backoff.
    Retries on network errors, 5xx, and 429. Pauses briefly on low remaining quota.
    """
    delay = 1.0
    last: Optional[requests.Response] = None
    for _ in range(5):
        try:
            r: requests.Response = req_fn(*a, **kw)
            last = r
        except Exception:
            r = None  # type: ignore

        if r is not None:
            try:
                rem = r.headers.get("X-RateLimit-Remaining")
                if rem is not None and int(rem) <= 0:
                    rst = r.headers.get("X-RateLimit-Reset")
                    if rst:
                        try:
                            wait = max(0, int(rst) - int(time.time()))
                            time.sleep(min(wait, 10))
                        except Exception:
                            time.sleep(delay)
                    else:
                        time.sleep(delay)
                if r.status_code == 429 or (500 <= r.status_code < 600):
                    time.sleep(delay)
                    delay = min(delay * 2, 10)
                    continue
                return r
            except Exception:
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue
        else:
            time.sleep(delay)
            delay = min(delay * 2, 10)
    return last

# ---------------------------------------------------------------------------
# GET/POST with memo + disk cache + ETag (configurable bypass)
# ---------------------------------------------------------------------------

def _simkl_get(
    url: str,
    *,
    headers: Mapping[str, str],
    params: Optional[dict] = None,
    timeout: int = 45,
    cfg_root: Optional[Mapping[str, Any]] = None,
    force_refresh: bool = False
) -> Optional[requests.Response]:
    """
    GET with layers:
    1) small in-process memo with TTL (skipped when force_refresh=True),
    2) disk cache TTL (skipped when force_refresh=True),
    3) If-None-Match using cached ETag (304 -> synthesize from cache),
    4) store fresh body+ETag on 200.
    """
    key = (url, _norm_params(params))
    now = time.time()

    # 1) in-run memo (short TTL)
    if not force_refresh:
        ent = _RUN_GET_CACHE.get(key)
        if ent:
            ts, r_cached = ent
            if (now - ts) < _HTTP_CACHE_TTL_SEC:
                _record_http_simkl(r_cached, endpoint=url.replace(SIMKL_BASE, ""), method="GET", count=False)
                return r_cached
            else:
                _RUN_GET_CACHE.pop(key, None)

    # 2) disk TTL cache (no network)
    if not force_refresh:
        ent = _load_http_cache(cfg_root or {}).get("map", {}).get(_http_cache_key(url, params))
        if ent and (now - float(ent.get("ts", 0))) <= _HTTP_CACHE_TTL_SEC:
            r = _CachedResponse(ent)
            _RUN_GET_CACHE[key] = (now, r)
            return r

    # 3) network with optional If-None-Match
    hdrs = dict(headers or {})
    try:
        prev = _load_http_cache(cfg_root or {}).get("map", {}).get(_http_cache_key(url, params))
        if (not force_refresh) and prev and prev.get("etag"):
            hdrs["If-None-Match"] = prev.get("etag")
    except Exception:
        pass

    try:
        r = _with_backoff(requests.get, url, headers=hdrs, params=(params or {}), timeout=timeout)
    except Exception:
        _record_http_simkl(None, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
        if not force_refresh:
            _RUN_GET_CACHE[key] = (now, None)
        return None

    # 304 -> synthesize from cache body
    try:
        if r is not None and getattr(r, "status_code", 0) == 304:
            prev = _load_http_cache(cfg_root or {}).get("map", {}).get(_http_cache_key(url, params))
            if prev:
                r2 = _CachedResponse(prev)
                if not force_refresh:
                    _RUN_GET_CACHE[key] = (now, r2)
                _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
                return r2
    except Exception:
        pass

    if r is not None and getattr(r, "ok", False):
        _http_cache_put(cfg_root or {}, url, params, r)

    if not force_refresh:
        _RUN_GET_CACHE[key] = (now, r)
    _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
    return r

def _simkl_post(url: str, *, headers: Mapping[str, str], json_payload: Mapping[str, Any], timeout: int = 45) -> Optional[requests.Response]:
    try:
        r = _with_backoff(
            requests.post, url, headers=headers, json=json_payload, timeout=timeout
        )
    except Exception:
        _record_http_simkl(None, endpoint=url.replace(SIMKL_BASE, ""), method="POST", payload=json_payload)
        return None
    _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="POST", payload=json_payload)
    return r


def _json_or_empty_list(r: Optional[requests.Response], list_key: Optional[str] = None) -> List[Any]:
    """
    Parse a SIMKL response that should be a list (or dict with a list inside).
    Treat shapes like {"type":"null","body":null} or None as empty list.
    """
    if not r or not getattr(r, "ok", False):
        return []
    try:
        data = r.json()
    except Exception:
        return []
    if data is None:
        return []
    if isinstance(data, dict) and data.get("type") == "null" and data.get("body") is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and list_key:
        arr = data.get(list_key)
        return arr if isinstance(arr, list) else []
    return []


def _json_or_empty_dict(r: Optional[requests.Response]) -> Dict[str, Any]:
    """
    Parse a SIMKL response that should be a dict.
    Treat shapes like {"type":"null","body":null} or None as empty dict.
    """
    if not r or not getattr(r, "ok", False):
        return {}
    try:
        data = r.json()
    except Exception:
        return {}
    if data is None:
        return {}
    if isinstance(data, dict) and data.get("type") == "null" and data.get("body") is None:
        return {}
    return data if isinstance(data, dict) else {}


# ---------------------------------------------------------------------------
# PTW: delta-only fetch + local shadow => full snapshot
# ---------------------------------------------------------------------------

def simkl_headers(simkl_cfg: Mapping[str, Any]) -> Dict[str, str]:
    """Build standard SIMKL headers."""
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {simkl_cfg.get('access_token','')}",
        "simkl-api-key": simkl_cfg.get("client_id",""),
    }


def canonical_key(item: Mapping[str, Any]) -> str:
    """Produce a deterministic key based on IDs, falling back to title/year."""
    ids = item.get("ids") or {}
    for k in _ID_KEYS:
        v = ids.get(k)
        if v:
            return f"{k}:{v}".lower()
    t = (item.get("title") or "").strip().lower()
    y = item.get("year") or ""
    typ = (item.get("type") or "").lower()
    return f"{typ}|title:{t}|year:{y}"


def minimal(item: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a compact item payload safe to cache and persist."""
    return {
        "ids": {k: item.get("ids", {}).get(k) for k in _ID_KEYS if item.get("ids", {}).get(k)},
        "title": item.get("title"),
        "year": item.get("year"),
        "type": (item.get("type") or "").lower() or None,
    }


def _read_as_list(j: Any, key: str) -> List[dict]:
    """Extract a list from a response payload tolerant to null-shapes."""
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        if j.get("type") == "null" and j.get("body") is None:
            return []
        arr = j.get(key)
        if isinstance(arr, list):
            return arr
    return []


def _ids_from_simkl_node(node: Mapping[str, Any]) -> Dict[str, Any]:
    """Extract known IDs from a SIMKL node into string form."""
    ids = dict(node.get("ids") or {})
    out: Dict[str, Any] = {}
    for k in _ID_KEYS:
        v = ids.get(k)
        if v is not None:
            out[k] = str(v)
    return out


def _ptw_fetch_delta(hdr: Mapping[str, str], date_from_iso: str, *, cfg_root: Optional[Mapping[str, Any]] = None) -> Dict[str, List[dict]]:
    params = {"extended": "full", "episode_watched_at": "yes", "memos": "yes", "date_from": date_from_iso}

    def _get(url: str, key: str) -> List[dict]:
        r = _simkl_get(url, headers=hdr, params=params, timeout=45, cfg_root=cfg_root, force_refresh=False)
        return _json_or_empty_list(r, key)

    movies = _get(SIMKL_ALL_ITEMS_MOVIES_PTW, "movies")
    shows  = _get(SIMKL_ALL_ITEMS_SHOWS_PTW,  "shows")
    return {"movies": movies, "shows": shows}


def _ptw_flatten_item(kind: str, it: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize PTW item into a compact shape. Treat anime as show for cross-provider compatibility."""
    node = it.get("movie") or it.get("show") or it.get("anime") or it.get(kind) or {}
    target_kind = "movie" if kind == "movie" else "show"
    return {"type": target_kind, "title": node.get("title"), "year": node.get("year"), "ids": _ids_from_simkl_node(node)}

def _ptw_key_for(kind: str, node: Mapping[str, Any]) -> str:
    """Build a stable key for PTW entries by ID, falling back to title/year."""
    ids = node.get("ids") or {}
    for k in _ID_KEYS:
        v = ids.get(k)
        if v:
            return f"{k}:{v}".lower()
    t = (node.get("title") or "").strip().lower()
    y = node.get("year") or ""
    return f"{kind}|title:{t}|year:{y}"


def _looks_removed(it: Mapping[str, Any]) -> bool:
    """SIMKL deltas may encode removals via different flags; normalize here."""
    action = str(it.get("action") or "").strip().lower()
    status = str(it.get("status") or "").strip().lower()
    removed_flag = bool(it.get("removed"))
    return removed_flag or action in {"remove", "removed", "delete", "deleted", "unwatch", "unlisted", "unsave", "un-save"} or status in {"removed", "deleted", "unlisted"}


def _ptw_apply_delta(shadow: Dict[str, Any], delta: Dict[str, List[dict]]) -> Dict[str, Any]:
    """Apply PTW delta onto the local shadow map."""
    items = dict(shadow)
    for kind, arr in (delta or {}).items():
        if not arr:
            continue
        # movies -> movie, shows/anime -> show
        k = "movie" if kind == "movies" else "show"
        for it in arr:
            node = _ptw_flatten_item(k, it)
            key = _ptw_key_for(k, node)
            if _looks_removed(it):
                items.pop(key, None)
            else:
                items[key] = node
    return items

def _ptw_bootstrap_using_windows(hdr: Mapping[str, str], start_iso: str, *, cfg_root: Optional[Mapping[str, Any]] = None, window_days: int = 365, max_windows: int = 8) -> Dict[str, Any]:
    """
    Conservative cold-start: walk backwards in larger windows, capped at 8.
    This bounds API calls even if activities/cursors are missing.
    """
    cursor_ts = iso_to_ts(start_iso) if start_iso else int(time.time())
    items: Dict[str, Any] = {}
    empty_streak = 0
    for _ in range(max_windows):
        since_ts = cursor_ts - window_days * 86400
        delta = _ptw_fetch_delta(hdr, ts_to_iso(since_ts), cfg_root=cfg_root)
        size = len(delta.get("movies") or []) + len(delta.get("shows") or [])
        if size == 0:
            empty_streak += 1
            if empty_streak >= 2:
                break
        else:
            empty_streak = 0
            items = _ptw_apply_delta(items, delta)
        cursor_ts = since_ts
    return items


def _ptw_fetch_present(
    hdr: Mapping[str, str],
    *,
    cfg_root: Optional[Mapping[str, Any]] = None,
    force_refresh: bool = False,
) -> Tuple[Dict[str, Any], bool]:
    """
    Fetch the present Plant-To-Watch snapshot for movies and shows.

    - Always include `date_from` (SIMKL requires it); 1970 ensures a full present view.
    - When `force_refresh=True`, _simkl_get will bypass ETag so we get a fresh 200 body.
    """
    params = {
        "extended": "full",
        "episode_watched_at": "yes",
        "memos": "yes",
        "date_from": "1970-01-01T00:00:00Z",
    }

    # Propagate `force_refresh` to control ETag usage and cache bypass.
    r_m = _simkl_get(
        SIMKL_ALL_ITEMS_MOVIES_PTW,
        headers=hdr,
        params=params,
        timeout=45,
        cfg_root=cfg_root,
        force_refresh=force_refresh,
    )
    r_s = _simkl_get(
        SIMKL_ALL_ITEMS_SHOWS_PTW,
        headers=hdr,
        params=params,
        timeout=45,
        cfg_root=cfg_root,
        force_refresh=force_refresh,
    )

    movies = _json_or_empty_list(r_m, "movies")
    shows  = _json_or_empty_list(r_s, "shows")

    ok = bool(
        (r_m is not None and getattr(r_m, "ok", False)) or
        (r_s is not None and getattr(r_s, "ok", False))
    )

    items: Dict[str, Any] = {}
    if movies:
        for it in movies:
            if _looks_removed(it):
                continue
            node = _ptw_flatten_item("movie", it)
            key  = _ptw_key_for("movie", node)
            items[key] = node

    if shows:
        for it in shows:
            if _looks_removed(it):
                continue
            node = _ptw_flatten_item("show", it)
            key  = _ptw_key_for("show", node)
            items[key] = node

    return items, ok

# ---------------------------------------------------------------------------
# Connectivity / liveness
# ---------------------------------------------------------------------------

def _simkl_alive(hdr: Mapping[str, str], *, cfg_root: Optional[Mapping[str, Any]] = None) -> bool:
    """Quick liveness check using /sync/activities; avoids expensive sweeps when offline."""
    try:
        r = _simkl_get(SIMKL_SYNC_ACTIVITIES, headers=hdr, timeout=15, cfg_root=cfg_root)
        return bool(r and r.ok)
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Watchlist (PTW)
# ---------------------------------------------------------------------------

def _watchlist_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build a full PTW snapshot with minimal API cost and correct freshness:
    - Zero-call fast path when activities ≤ shadow timestamp.
    - Prefer present snapshot (authoritative).
    - If present fails and shadow is empty, bounded bootstrap.
    - Use delta only when present failed.
    - Clear per-run GET memo to avoid cross-run staleness.
    """
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)

    # Reset in-run GET memoization so prior runs can't leak stale responses.
    try:
        _RUN_GET_CACHE.clear()
    except Exception:
        pass

    # Offline → serve last shadow to keep orchestrator stable.
    if not _simkl_alive(hdr, cfg_root=cfg_root):
        shadow = store.load_shadow()
        out = {}
        for k, node in (shadow.get("items") or {}).items():
            out[k] = {
                "type": node.get("type"),
                "title": node.get("title"),
                "year": node.get("year"),
                "ids": node.get("ids") or {},
            }
        try:
            if (cfg_root.get("runtime") or {}).get("debug"):
                host_log("SIMKL.calls", {"feature": "watchlist", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
        except Exception:
            pass
        return out

    acts = _activities(cfg_root)

    # Cursor precedence: persisted → config hints → activities → now
    cursor = store.get("watchlist") or simkl_cfg.get("last_date") or simkl_cfg.get("date_from")
    date_from = cursor or acts.get("watchlist") or acts.get("lists") or ts_to_iso(int(time.time()))

    # Load shadow as temporary base
    shadow = store.load_shadow()
    items = dict(shadow.get("items") or {})

    # Activity-aware fast path: if nothing moved since shadow, skip network entirely.
    try:
        shadow_ts = int(shadow.get("last_sync_ts") or 0)
    except Exception:
        shadow_ts = 0
    try:
        acts_ts = iso_to_ts(str(acts.get("watchlist") or acts.get("lists") or "")) or 0
    except Exception:
        acts_ts = 0

    if shadow_ts and acts_ts and acts_ts <= shadow_ts:
        out: Dict[str, Dict[str, Any]] = {}
        for k, node in items.items():
            out[k] = {
                "type": node.get("type"),
                "title": node.get("title"),
                "year": node.get("year"),
                "ids": node.get("ids") or {},
            }
        try:
            if (cfg_root.get("runtime") or {}).get("debug"):
                host_log("SIMKL.calls", {"feature": "watchlist", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
        except Exception:
            pass
        return out

    # 1) Present snapshot first (authoritative). Force refresh only if activities moved.
    force_present = bool(acts_ts and (acts_ts > shadow_ts))
    present_items, ok_present = _ptw_fetch_present(hdr, cfg_root=cfg_root, force_refresh=force_present)
    if ok_present:
        items = dict(present_items)  # authoritative, including empty
        # Persist immediately to prevent stale shadow re-infecting later runs
        try:
            now_ts = int(time.time())
            store.save_shadow(items, now_ts)
            store.set("watchlist", ts_to_iso(now_ts))
        except Exception:
            pass
        did_present = True
    else:
        did_present = False
        # If present failed and no shadow exists, do bounded bootstrap
        if not items:
            items = _ptw_bootstrap_using_windows(hdr, date_from, cfg_root=cfg_root)

    # 2) Delta only when present failed
    if not did_present:
        # Backshift cursor slightly to avoid boundary misses
        try:
            base_ts = iso_to_ts(str(date_from)) or int(time.time())
            date_from_safe = ts_to_iso(max(0, base_ts - 5))
        except Exception:
            date_from_safe = date_from

        delta = _ptw_fetch_delta(hdr, date_from_safe, cfg_root=cfg_root)
        items = _ptw_apply_delta(items, delta)

        # Safety net: activities moved but delta looked empty → tiny sweep
        try:
            act_ts = iso_to_ts(str(acts.get("watchlist") or acts.get("lists") or "")) or 0
            cur_ts = iso_to_ts(str(date_from_safe)) or 0
            delta_size = (
                len(delta.get("movies", [])) +
                len(delta.get("shows", [])) +
                (len(delta.get("anime", [])) if isinstance(delta.get("anime", []), list) else 0)
            )
            if delta_size == 0 and act_ts and cur_ts and act_ts > cur_ts:
                tiny_since = ts_to_iso(int(time.time()) - 7 * 86400)
                tiny = _ptw_fetch_delta(hdr, tiny_since, cfg_root=cfg_root)
                items = _ptw_apply_delta(items, tiny)
        except Exception:
            pass

    # 3) Persist shadow and advance cursor from activities (or now as fallback)
    try:
        now_ts = int(time.time())
        store.save_shadow(items, now_ts)
        acts2 = _activities(cfg_root)
        new_cursor = acts2.get("watchlist") or acts2.get("lists") or ts_to_iso(now_ts)
        store.set("watchlist", new_cursor)
    except Exception:
        pass

    # 4) Normalize snapshot for orchestrator
    out: Dict[str, Dict[str, Any]] = {}
    for k, node in items.items():
        out[k] = {
            "type": node.get("type"),
            "title": node.get("title"),
            "year": node.get("year"),
            "ids": node.get("ids") or {},
        }

    # Debug counters (optional)
    try:
        if (cfg_root.get("runtime") or {}).get("debug"):
            host_log("SIMKL.calls", {"feature": "watchlist", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
    except Exception:
        pass

    return out


def _watchlist_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Add items to SIMKL watchlist (PTW). Immediately advance cursor and shadow on success.
    GMT is consulted to avoid re-adding items under quarantine.
    """
    items_list = list(items)
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and items_list:
        op_add, _ = _gmt_ops_for_feature("watchlist")
        items_list = [it for it in items_list
                      if not _gmt_suppress(store=store_gmt, item=it, feature="watchlist", write_op=op_add)]

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        typ = (it.get("type") or "movie").lower()
        if typ == "anime":
            typ = "show"
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}, "to": "plantowatch"}
        if entry["ids"]:
            (payload["movies"] if typ == "movie" else payload["shows"]).append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    r = _simkl_post(SIMKL_ADD_TO_LIST, headers=simkl_headers(simkl_cfg), json_payload=payload, timeout=45)
    if r and r.ok:
        # Bust caches so _watchlist_index sees the new state right away
        _bust_ptw_cache(cfg_root)
        _clear_run_cache()

        now_ts = int(time.time())
        now_iso = ts_to_iso(now_ts)
        store = _CursorStore(cfg_root)
        store.set("watchlist", now_iso)

        # Update shadow append-only
        shadow = store.load_shadow()
        m = dict(shadow.get("items") or {})
        for it in items_list:
            typ = (it.get("type") or "movie").lower()
            if typ == "anime":
                typ = "show"
            node = {"type": typ, "title": it.get("title"), "year": it.get("year"), "ids": dict(it.get("ids") or {})}
            k = canonical_key(node)
            m[k] = node
        store.save_shadow(m, now_ts)

        return sum(len(v) for v in payload.values())
    return 0


def _watchlist_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Remove items from SIMKL PTW using the official history/remove endpoint.
    On success, advance both history and watchlist cursors, update shadow, and record GMT.
    """
    items_list = list(items)

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        typ = (it.get("type") or "movie").lower()
        if typ == "anime":
            typ = "show"
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if entry["ids"]:
            (payload["movies"] if typ == "movie" else payload["shows"]).append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    r = _simkl_post(SIMKL_HISTORY_REMOVE, headers=simkl_headers(simkl_cfg), json_payload=payload, timeout=45)
    if r and r.ok:
        # Bust caches + run memo so the next index is fresh
        _bust_ptw_cache(cfg_root)
        _clear_run_cache()

        now_ts = int(time.time())
        now_iso = ts_to_iso(now_ts)
        store = _CursorStore(cfg_root)
        store.set("history", now_iso)
        store.set("watchlist", now_iso)

        # Shadow: remove keys
        shadow = store.load_shadow()
        m = dict(shadow.get("items") or {})
        for it in items_list:
            typ = (it.get("type") or "movie").lower()
            if typ == "anime":
                typ = "show"
            node = {"type": typ, "title": it.get("title"), "year": it.get("year"), "ids": dict(it.get("ids") or {})}
            k = canonical_key(node)
            m.pop(k, None)
        store.save_shadow(m, now_ts)

        # GMT negatives start quarantine
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="watchlist", op="remove", origin="SIMKL")

        return sum(len(v) for v in payload.values())
    return 0


# ---------------------------------------------------------------------------
# Ratings (with shadow + TTL)
# ---------------------------------------------------------------------------

def _ratings_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Ratings snapshot with optional TTL cache:
    - When TTL (ratings.cache.ttl_minutes) > 0 and shadow is fresh → return shadow.
    - Otherwise fetch from SIMKL. If date_from cursor is present, merge delta into shadow.
      If no cursor, replace shadow with full snapshot.
    """
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)

    # Shadow TTL gate
    ttl_min = int(_cfg_get(cfg_root, "ratings.cache.ttl_minutes", 0) or 0)
    ttl_sec = max(0, ttl_min * 60)
    sh = _ratings_shadow_load(cfg_root)
    sh_items = dict(sh.get("items") or {})
    sh_ts = int(sh.get("ts") or 0)
    now = int(time.time())
    if ttl_sec > 0 and sh_ts > 0 and (now - sh_ts) < ttl_sec and sh_items:
        try:
            host_log("SIMKL.ratings", {"cache": "hit", "age_sec": now - sh_ts, "count": len(sh_items)})
        except Exception:
            pass
        return sh_items

    if not _simkl_alive(hdr, cfg_root=cfg_root):
        # Offline → shadow only (even if stale)
        return sh_items

    params: Dict[str, Any] = {"extended": "full"}
    df = store.get("ratings") or simkl_cfg.get("date_from")
    if df:
        params["date_from"] = df

    r = _simkl_get(SIMKL_RATINGS_GET, headers=hdr, params=params, timeout=45, cfg_root=cfg_root)
    data = _json_or_empty_dict(r)

    idx: Dict[str, Dict[str, Any]] = {}

    def handle(arr: List[dict], kind: str, base: Dict[str, Dict[str, Any]]):
        for it in arr or []:
            node = it.get(kind) or {}
            ids = _ids_from_simkl_node(node)
            if not ids and (node.get("title") is None):
                continue
            key = None
            for k in _ID_KEYS:
                v = ids.get(k)
                if v:
                    key = f"{k}:{v}".lower()
                    break
            if not key:
                t = str(node.get("title") or "").strip().lower()
                y = node.get("year") or ""
                key = f"{kind}|title:{t}|year:{y}"
            base[key] = {
                "type":  kind,
                "title": node.get("title"),
                "year":  node.get("year"),
                "ids":   ids,
                "rating": int(it.get("rating")) if isinstance(it.get("rating"), (int, float)) else None,
            }

    # Build fresh snapshot (for full) or delta map (for merge)
    handle(_read_as_list(data, "movies"), "movie", idx)
    handle(_read_as_list(data, "shows"),  "show",  idx)

    # Merge vs replace
    if df:
        # Merge delta onto shadow
        merged = dict(sh_items)
        for k, v in idx.items():
            merged[k] = v
        _ratings_shadow_save(cfg_root, merged)
        out = merged
    else:
        # Replace entire snapshot
        _ratings_shadow_save(cfg_root, idx)
        out = idx

    # Advance cursor to now (server Date header is optional/inconsistent)
    try:
        store.set("ratings", ts_to_iso(int(time.time())))
    except Exception:
        pass

    try:
        if (cfg_root.get("runtime") or {}).get("debug"):
            host_log("SIMKL.calls", {"feature": "ratings", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
    except Exception:
        pass

    return out

def _ratings_set(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    items_list = list(items)
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and items_list:
        op_add, _op_neg = _gmt_ops_for_feature("ratings")
        items_list = [it for it in items_list if not _gmt_suppress(store=store_gmt, item=it, feature="ratings", write_op=op_add)]

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)

    if not items_list:
        return 0

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        rating = it.get("rating")
        if rating is None:
            continue
        entry = {"rating": int(rating), "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        (movies if (it.get("type") or "movie") == "movie" else shows).append(entry)

    def _chunks(lst: List[dict], n: int):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    total = 0
    for chunk in _chunks(movies, 200):
        payload = {"movies": chunk}
        r = _simkl_post(SIMKL_RATINGS_SET, headers=hdr, json_payload=payload, timeout=45)
        if r and r.ok:
            total += len(chunk)
    for chunk in _chunks(shows, 200):
        payload = {"shows": chunk}
        r = _simkl_post(SIMKL_RATINGS_SET, headers=hdr, json_payload=payload, timeout=45)
        if r and r.ok:
            total += len(chunk)

    if total:
        # Bust caches to reflect changes immediately
        _bust_ratings_cache(cfg_root)
        _clear_run_cache()

        # Update shadow + emit events
        sh = _ratings_shadow_load(cfg_root)
        smap: Dict[str, Any] = dict(sh.get("items") or {})
        now_iso = ts_to_iso(int(time.time()))
        for it in items_list:
            if it.get("rating") is None:
                continue
            node = {
                "type": (it.get("type") or "movie"),
                "title": it.get("title"),
                "year":  it.get("year"),
                "ids":   {k: (it.get("ids") or {}).get(k) for k in _ID_KEYS if (it.get("ids") or {}).get(k)},
            }
            key = canonical_key(node)
            prev_val = None
            if key in smap and isinstance(smap[key], dict):
                pv = smap[key].get("rating")
                prev_val = int(pv) if isinstance(pv, int) else None

            new_val = int(it.get("rating"))
            smap[key] = {
                "type": node["type"],
                "title": node["title"],
                "year":  node["year"],
                "ids":   node["ids"],
                "rating": new_val,
                "rated_at": now_iso,
            }

            if prev_val is None:
                _emit_rating_event(action="rate", node=smap[key], prev=None, value=new_val)
            elif prev_val != new_val:
                _emit_rating_event(action="update", node=smap[key], prev=prev_val, value=new_val)
            else:
                _emit_rating_event(action="rate", node=smap[key], prev=prev_val, value=new_val)

        _ratings_shadow_save(cfg_root, smap)
        # Move cursor forward so next index can be delta
        try:
            _CursorStore(cfg_root).set("ratings", now_iso)
        except Exception:
            pass

    return total

def _ratings_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Remove ratings from SIMKL. Updates shadow, emits events, and records GMT negatives on success."""
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)

    items_list = list(items)
    if not items_list:
        return 0

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        (movies if (it.get("type") or "movie") == "movie" else shows).append(entry)

    def _chunks(lst: List[dict], n: int):
        for i in range(0, len(lst), n):
            yield lst[i:i+n]

    total = 0
    for chunk in _chunks(movies, 200):
        payload = {"movies": chunk}
        r = _simkl_post(SIMKL_RATINGS_REMOVE, headers=hdr, json_payload=payload, timeout=45)
        if r and r.ok:
            total += len(chunk)
    for chunk in _chunks(shows, 200):
        payload = {"shows": chunk}
        r = _simkl_post(SIMKL_RATINGS_REMOVE, headers=hdr, json_payload=payload, timeout=45)
        if r and r.ok:
            total += len(chunk)

    if total:
        # Bust caches + memo
        _bust_ratings_cache(cfg_root)
        _clear_run_cache()

        # Update shadow + emit events
        sh = _ratings_shadow_load(cfg_root)
        smap: Dict[str, Any] = dict(sh.get("items") or {})
        for it in items_list:
            node = {
                "type": (it.get("type") or "movie"),
                "title": it.get("title"),
                "year":  it.get("year"),
                "ids":   {k: (it.get("ids") or {}).get(k) for k in _ID_KEYS if (it.get("ids") or {}).get(k)},
            }
            key = canonical_key(node)
            prev_val = None
            if key in smap and isinstance(smap[key], dict):
                pv = smap[key].get("rating")
                prev_val = int(pv) if isinstance(pv, int) else None
                smap[key].pop("rating", None)  # keep node, drop rating only
            _emit_rating_event(action="unrate", node=node, prev=prev_val, value=None)

        _ratings_shadow_save(cfg_root, smap)

        # GMT: record negatives (unrate)
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="ratings", op="unrate", origin="SIMKL")

        # Move cursor forward
        try:
            _CursorStore(cfg_root).set("ratings", ts_to_iso(int(time.time())))
        except Exception:
            pass

    return total


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

def _history_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return a snapshot of SIMKL user history since the last cursor."""
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr   = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)
    acts  = _activities(cfg_root)

    if not _simkl_alive(hdr, cfg_root=cfg_root):
        try:
            if (cfg_root.get("runtime") or {}).get("debug"):
                host_log("SIMKL.calls", {"feature": "history", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
        except Exception:
            pass
        return {}

    params: Dict[str, Any] = {"extended": "full"}
    cursor    = store.get("history") or simkl_cfg.get("last_date") or simkl_cfg.get("date_from")
    date_from = cursor or acts.get("history") or acts.get("completed")
    if date_from:
        params["date_from"] = date_from

    r = _simkl_get(SIMKL_HISTORY_ADD, headers=hdr, params=params, timeout=45, cfg_root=cfg_root)
    data = _json_or_empty_dict(r)

    idx: Dict[str, Dict[str, Any]] = {}

    def handle(arr: List[dict], kind: str):
        for it in arr or []:
            node = it.get(kind) or {}
            ids = _ids_from_simkl_node(node)
            if not ids and (node.get("title") is None):
                continue
            key = None
            for k in _ID_KEYS:
                if ids.get(k):
                    key = f"{k}:{ids[k]}".lower()
                    break
            if not key:
                t = str(node.get("title") or "").strip().lower(); y = node.get("year") or ""
                key = f"{kind}|title:{t}|year:{y}"
            idx[key] = {"type": kind, "title": node.get("title"), "year": node.get("year"), "ids": ids, "watched": True}

    handle(_read_as_list(data, "movies"), "movie")
    handle(_read_as_list(data, "shows"),  "show")

    try:
        if (cfg_root.get("runtime") or {}).get("debug"):
            host_log("SIMKL.calls", {"feature": "history", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
    except Exception:
        pass

    return idx


def _history_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Add history (scrobbles). Immediately advances the history cursor on success.
    GMT is consulted to avoid re-scrobbles during quarantine.
    """
    items_list = list(items)
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and items_list:
        op_add, _op_neg = _gmt_ops_for_feature("history")
        items_list = [it for it in items_list if not _gmt_suppress(store=store_gmt, item=it, feature="history", write_op=op_add)]

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    now_iso = ts_to_iso(int(time.time()))
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        entry = {"watched_at": it.get("watched_at") or now_iso, "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        (movies if (it.get("type") or "movie") == "movie" else shows).append(entry)
    payload = {k: v for k, v in {"movies": movies, "shows": shows}.items() if v}
    if not payload:
        return 0

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    r = _simkl_post(SIMKL_HISTORY_ADD, headers=simkl_headers(simkl_cfg), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("history", now_iso)
        return sum(len(v) for v in payload.values())
    return 0


def _history_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Remove history entries. Records GMT negatives on success."""
    items_list = list(items)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        (movies if (it.get("type") or "movie") == "movie" else shows).append(entry)
    payload = {k: v for k, v in {"movies": movies, "shows": shows}.items() if v}
    if not payload:
        return 0

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    r = _simkl_post(SIMKL_HISTORY_REMOVE, headers=simkl_headers(simkl_cfg), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("history", ts_to_iso(int(time.time())))
        # GMT: record negatives (unscrobble) after successful removes
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="history", op="unscrobble", origin="SIMKL")
        return sum(len(v) for v in payload.values())
    return 0


# ---------------------------------------------------------------------------
# OPS adapter
# ---------------------------------------------------------------------------

class _SimklOPS:
    def name(self) -> str: return "SIMKL"
    def label(self) -> str: return "Simkl"
    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": False}
    def capabilities(self) -> Mapping[str, Any]:
        # This provider returns stable IDs across snapshots.
        return {"bidirectional": True, "provides_ids": True}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        if feature == "watchlist":
            return _watchlist_index(cfg)
        if feature == "ratings":
            return _ratings_index(cfg)
        if feature == "history":
            return _history_index(cfg)
        if feature == "playlists":
            return {}
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]:
        items_list = list(items)
        if dry_run:
            return {"ok": True, "count": len(items_list), "dry_run": True}
        if feature == "watchlist":
            cnt = _watchlist_add(cfg, items_list)
            return {"ok": True, "count": cnt}
        if feature == "ratings":
            cnt = _ratings_set(cfg, items_list)
            return {"ok": True, "count": cnt}
        if feature == "history":
            cnt = _history_add(cfg, items_list)
            return {"ok": True, "count": cnt}
        if feature == "playlists":
            return {"ok": False, "count": 0, "error": "SIMKL playlists API not supported"}
        return {"ok": True, "count": 0}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]:
        items_list = list(items)
        if dry_run:
            return {"ok": True, "count": len(items_list), "dry_run": True}
        if feature == "watchlist":
            cnt = _watchlist_remove(cfg, items_list)
            return {"ok": True, "count": cnt}
        if feature == "ratings":
            cnt = _ratings_remove(cfg, items_list)
            return {"ok": True, "count": cnt}
        if feature == "history":
            cnt = _history_remove(cfg, items_list)
            return {"ok": True, "count": cnt}
        if feature == "playlists":
            return {"ok": False, "count": 0, "error": "SIMKL playlists API not supported"}
        return {"ok": True, "count": 0}


OPS: InventoryOps = _SimklOPS()


# ---------------------------------------------------------------------------
# Module manifest
# ---------------------------------------------------------------------------

try:
    from providers.sync._base import SyncModule, ModuleInfo, ModuleCapabilities  # type: ignore
except Exception:  # pragma: no cover
    class SyncModule: ...
    @dataclass
    class ModuleCapabilities:
        supports_dry_run: bool = True
        supports_cancel: bool = True
        supports_timeout: bool = True
        status_stream: bool = True
        bidirectional: bool = True
        config_schema: dict | None = None
    @dataclass
    class ModuleInfo:
        name: str
        version: str
        description: str
        vendor: str
        capabilities: ModuleCapabilities


class SIMKLModule(SyncModule):
    info = ModuleInfo(
        name="SIMKL",
        version=__VERSION__,
        description="Reads and writes SIMKL watchlist (PTW), ratings (with TTL cache + events), and history with resilient cursors, ETag/TTL caching, and low-call indexing.",
        vendor="community",
        capabilities=ModuleCapabilities(
            supports_dry_run=True,
            supports_cancel=True,
            supports_timeout=True,
            status_stream=True,
            bidirectional=True,
            config_schema={
                "type": "object",
                "properties": {
                    "simkl": {
                        "type": "object",
                        "properties": {
                            "client_id": {"type": "string", "minLength": 1},
                            "access_token": {"type": "string", "minLength": 1},
                            "date_from": {"type": "string"},
                            "last_date": {"type": "string"},
                        },
                        "required": ["client_id", "access_token"],
                    },
                    "ratings": {
                        "type": "object",
                        "properties": {
                            "cache": {
                                "type": "object",
                                "properties": {
                                    "ttl_minutes": {"type": "integer", "minimum": 0}
                                }
                            }
                        }
                    },
                    "sync": {
                        "type": "object",
                        "properties": {
                            "gmt_enable": {"type": "boolean"},
                            "gmt_quarantine_days": {"type": "integer", "minimum": 1},
                        },
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}, "state_dir": {"type": "string"}},
                    },
                },
                "required": ["simkl"],
            },
        ),
    )

    @staticmethod
    def supported_features() -> dict:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": False}


def get_manifest() -> dict:
    return {
        "name": SIMKLModule.info.name,
        "label": "Simkl",
        "features": SIMKLModule.supported_features(),
        "capabilities": {"bidirectional": True},
        "version": SIMKLModule.info.version,
        "vendor": SIMKLModule.info.vendor,
        "description": SIMKLModule.info.description,
    }

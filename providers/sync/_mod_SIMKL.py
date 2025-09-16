from __future__ import annotations

__VERSION__ = "1.2.1"
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
_RUN_GET_CACHE: Dict[Tuple[str, Tuple[Tuple[str, str], ...]], Optional[requests.Response]] = {}

def _norm_params(p: Optional[dict]) -> Tuple[Tuple[str, str], ...]:
    if not p:
        return tuple()
    # stringify and sort keys to normalize
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
SIMKL_ADD_TO_LIST          = f"{SIMKL_BASE}/sync/add-to-list"
SIMKL_HISTORY_REMOVE       = f"{SIMKL_BASE}/sync/history/remove"  # PTW removal via history API

# History
SIMKL_HISTORY_ADD          = f"{SIMKL_BASE}/sync/history"          # GET supports date_from

# Ratings
SIMKL_RATINGS_GET          = f"{SIMKL_BASE}/sync/ratings"
SIMKL_RATINGS_SET          = f"{SIMKL_BASE}/sync/ratings"
SIMKL_RATINGS_REMOVE       = f"{SIMKL_BASE}/sync/ratings/remove"

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
    # watchlist behaves like add/remove
    return "add", "remove"

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
    try:
        import datetime as dt
        return int(dt.datetime.strptime(s.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z").timestamp())
    except Exception:
        return 0


def ts_to_iso(ts: int) -> str:
    """Format epoch seconds into UTC ISO-8601."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


# ---------------------------------------------------------------------------
# Disk HTTP cache (GET) under /config/.cw_state
# ---------------------------------------------------------------------------

_HTTP_CACHE_FILE_NAME = "simkl_http_cache.json"
_HTTP_CACHE_TTL_SEC   = 600  # 10 minutes default TTL for GETs
_HTTP_CACHE_MAX_ENTRIES = 512

def _resolve_state_dir(root_cfg: Mapping[str, Any]) -> Path:
    """
    Decide where to keep durable state:
    1) cfg.runtime.state_dir
    2) env: CW_STATE_DIR
    3) next to config.json (CONFIG.path)
    4) /config (Docker volume convention)
    5) cwd (last resort)
    """
    # 1) explicit override in config
    try:
        p = (root_cfg.get("runtime") or {}).get("state_dir")
        if p:
            return Path(p)
    except Exception:
        pass

    # 2) env override
    p = os.environ.get("CW_STATE_DIR")
    if p:
        return Path(p)

    # 3) next to config.json
    try:
        if CONFIG and getattr(CONFIG, "path", None):
            return Path(CONFIG.path).parent  # type: ignore[attr-defined]
    except Exception:
        pass

    # 4) Docker default
    if Path("/config").exists():
        return Path("/config")

    # 5) fallback
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
        return {"map": {}, "order": []}  # order: LRU-ish list of keys


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
    # TTL check
    try:
        if (time.time() - float(ent.get("ts", 0))) > _HTTP_CACHE_TTL_SEC:
            return None
    except Exception:
        return None
    return ent


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
        # LRU cap
        while len(cache_order) > _HTTP_CACHE_MAX_ENTRIES:
            victim = cache_order.pop(0)
            cache_map.pop(victim, None)
        cache["map"], cache["order"] = cache_map, cache_order
        _save_http_cache(cache, root_cfg)
    except Exception:
        pass


class _CachedResponse:
    """Tiny Response-like wrapper to serve JSON from disk cache uniformly."""
    def __init__(self, ent: Mapping[str, Any]):
        self._ent = dict(ent)
        self.status_code = int(ent.get("status", 200))
        self.ok = 200 <= self.status_code < 300
        self.headers = dict(ent.get("headers") or {})
        self.content = (ent.get("body") or "").encode("utf-8")
        self.elapsed = 0  # pretend instant

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
# Cursor and shadow storage
# ---------------------------------------------------------------------------

class _CursorStore:
    """
    Stores per-feature cursors and a watchlist shadow in a durable, atomic way.
    State lives in `<state_dir>/.cw_state/` (defaults to /config/.cw_state in Docker).
    """

    def __init__(self, root_cfg: Mapping[str, Any]):
        base = _resolve_state_dir(root_cfg)
        base_dir = (base / ".cw_state")

        # Ensure directory exists; fallback to cwd if needed
        try:
            base_dir.mkdir(parents=True, exist_ok=True)
        except Exception:
            base_dir = Path.cwd() / ".cw_state"
            base_dir.mkdir(parents=True, exist_ok=True)

        # Optional one-time migration from /app/.cw_state -> new base
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
        os.replace(tmp, path)  # atomic on POSIX/Windows

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

# ---------------------------------------------------------------------------
# Helpers for IDs and shapes
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
    """Return a compact item payload safe to cache."""
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


# ---------------------------------------------------------------------------
# Activities (cached per run)
# ---------------------------------------------------------------------------

_RUN_ACT = {"ts": 0.0, "data": None}
_ACT_TTL_SEC = 60  # Cache activities for 1 minutes to avoid chattiness

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
                if isinstance(sv, (str, int, str)):
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
    # Increment basic counters regardless of _stats availability (unless disabled)
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

        # Estimate outgoing payload size
        if isinstance(payload, (bytes, bytearray)):
            bytes_out = len(payload)
        elif payload is None:
            bytes_out = 0
        else:
            try:
                bytes_out = len(json.dumps(payload))
            except Exception:
                bytes_out = 0

        # Timing
        ms = int(getattr(r, "elapsed", 0).total_seconds() * 1000) if (r is not None and getattr(r, "elapsed", None)) else 0

        # Rate headers (best-effort)
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
    Apply defensive retries with exponential backoff.
    - Retries on network errors, 5xx, and 429.
    - If headers expose low remaining quota, pause briefly.
    """
    delay = 1.0
    last: Optional[requests.Response] = None
    for _ in range(5):
        try:
            r: requests.Response = req_fn(*a, **kw)
            last = r
        except Exception:
            r = None  # type: ignore

        # If we have a response, inspect status and rate headers
        if r is not None:
            try:
                rem = r.headers.get("X-RateLimit-Remaining")
                if rem is not None and int(rem) <= 0:
                    # If reset header exists, sleep up to 10s max (keep runs responsive)
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
                # For other statuses, return directly (including 4xx which we don't retry)
                return r
            except Exception:
                # If inspection fails, treat as transient
                time.sleep(delay)
                delay = min(delay * 2, 10)
                continue
        else:
            # Network error, retry
            time.sleep(delay)
            delay = min(delay * 2, 10)
    return last


# ---------------------------------------------------------------------------
# GET/POST with memo + disk cache + ETag
# ---------------------------------------------------------------------------

def _simkl_get(url: str, *, headers: Mapping[str, str], params: Optional[dict] = None, timeout: int = 45, cfg_root: Optional[Mapping[str, Any]] = None) -> Optional[requests.Response]:
    """
    GET with layers:
    1) per-run memo (identical url+params),
    2) disk cache TTL hit (no network, no call count),
    3) If-None-Match using cached ETag (network, counts; 304 -> synthesize from cache),
    4) store fresh body+ETag on 200.
    """
    key = (url, _norm_params(params))
    # 1) in-run memo
    if key in _RUN_GET_CACHE:
        r = _RUN_GET_CACHE[key]
        # count as a logical GET for stats, but don't double log bandwidth
        _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
        return r

    # 2) disk TTL cache (no network)
    ent = _http_cache_get(cfg_root or {}, url, params)
    if ent:
        try:
            r = _CachedResponse(ent)
            _RUN_GET_CACHE[key] = r
            # do NOT count a call here (no HTTP made)
            return r
        except Exception:
            pass  # fall through to network

    # 3) network with optional If-None-Match
    hdrs = dict(headers or {})
    try:
        # if we have a cached ETag (even if TTL expired), send it
        cache_all = _load_http_cache(cfg_root or {})
        ck = _http_cache_key(url, params)
        prev = (cache_all.get("map") or {}).get(ck)
        if prev and prev.get("etag"):
            hdrs["If-None-Match"] = prev.get("etag")
    except Exception:
        pass

    try:
        r = _with_backoff(
            requests.get, url, headers=hdrs, params=(params or {}), timeout=timeout
        )
    except Exception:
        _record_http_simkl(None, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
        _RUN_GET_CACHE[key] = None
        return None

    # Handle 304 from server using cached body
    try:
        if r is not None and getattr(r, "status_code", 0) == 304:
            cache_all = _load_http_cache(cfg_root or {})
            ck = _http_cache_key(url, params)
            prev = (cache_all.get("map") or {}).get(ck)
            if prev:
                r2 = _CachedResponse(prev)
                _RUN_GET_CACHE[key] = r2
                _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="GET")  # counts the network 304
                return r2
    except Exception:
        pass

    # Normal success path: write cache on 200
    if r is not None and getattr(r, "ok", False):
        _http_cache_put(cfg_root or {}, url, params, r)

    _RUN_GET_CACHE[key] = r
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

def _ptw_fetch_delta(hdr: Mapping[str, str], date_from_iso: str, *, cfg_root: Optional[Mapping[str, Any]] = None) -> Dict[str, List[dict]]:
    """Fetch PTW delta since a given date."""
    params = {"extended": "full", "episode_watched_at": "yes", "memos": "yes", "date_from": date_from_iso}

    def _get(url: str, key: str) -> List[dict]:
        r = _simkl_get(url, headers=hdr, params=params, timeout=45, cfg_root=cfg_root)
        return _json_or_empty_list(r, key)

    movies = _get(SIMKL_ALL_ITEMS_MOVIES_PTW, "movies")
    shows  = _get(SIMKL_ALL_ITEMS_SHOWS_PTW,  "shows")
    return {"movies": movies, "shows": shows}


def _ptw_flatten_item(kind: str, it: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize PTW item into a compact shape."""
    node = (it.get("movie") if kind == "movie" else it.get("show")) or {}
    return {"type": kind, "title": node.get("title"), "year": node.get("year"), "ids": _ids_from_simkl_node(node)}


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
        k = "movie" if kind == "movies" else "show"
        for it in arr or []:
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


def _ptw_fetch_present(hdr: Mapping[str, str], *, cfg_root: Optional[Mapping[str, Any]] = None) -> Tuple[Dict[str, Any], bool]:
    """
    Present-time PTW snapshot by sweeping from epoch (SIMKL requires date_from).
    Returns (items_map, ok_flag). ok_flag indicates a meaningful response.
    """
    params = {"extended": "full", "episode_watched_at": "yes", "memos": "yes", "date_from": "1970-01-01T00:00:00Z"}

    r_m = _simkl_get(SIMKL_ALL_ITEMS_MOVIES_PTW, headers=hdr, params=params, timeout=45, cfg_root=cfg_root)
    r_s = _simkl_get(SIMKL_ALL_ITEMS_SHOWS_PTW,  headers=hdr, params=params, timeout=45, cfg_root=cfg_root)

    movies = _json_or_empty_list(r_m, "movies")
    shows  = _json_or_empty_list(r_s, "shows")

    ok = (r_m is not None and getattr(r_m, "ok", False)) or (r_s is not None and getattr(r_s, "ok", False))
    items: Dict[str, Any] = {}
    for it in movies or []:
        node = _ptw_flatten_item("movie", it)
        key = _ptw_key_for("movie", node)
        if not _looks_removed(it):
            items[key] = node
    for it in shows or []:
        node = _ptw_flatten_item("show", it)
        key = _ptw_key_for("show", node)
        if not _looks_removed(it):
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
    Build a full watchlist (PTW) snapshot with minimal API cost:
    - Prefer present-time snapshot (2 calls).
    - If present works, skip delta unless activities moved past our cursor.
    - If present fails and shadow is empty, use a bounded bootstrap (<= 8 windows).
    - Advance the cursor using activities (server view) when possible.
    """
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)

    # If offline, return last known shadow. Orchestrator can surface transient state.
    if not _simkl_alive(hdr, cfg_root=cfg_root):
        shadow = store.load_shadow()
        out = {}
        for k, node in (shadow.get("items") or {}).items():
            out[k] = {"type": node.get("type"), "title": node.get("title"), "year": node.get("year"), "ids": node.get("ids") or {}}
        # Log call counters in debug
        try:
            if (cfg_root.get("runtime") or {}).get("debug"):
                host_log("SIMKL.calls", {"feature": "watchlist", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
        except Exception:
            pass
        return out

    acts = _activities(cfg_root)

    # Cursor precedence: persisted -> simkl config hints -> activities -> now
    cursor = store.get("watchlist") or simkl_cfg.get("last_date") or simkl_cfg.get("date_from")
    date_from = cursor or acts.get("watchlist") or acts.get("lists") or ts_to_iso(int(time.time()))

    # Load shadow first; cheap local baseline
    shadow = store.load_shadow()
    items = dict(shadow.get("items") or {})

    # 1) Try present-time snapshot first (authoritative, cheap)
    present_items, ok_present = _ptw_fetch_present(hdr, cfg_root=cfg_root)
    did_present = bool(ok_present)
    if did_present:
        items = dict(present_items)
    else:
        # 1b) If present failed and we have no shadow, do a bounded bootstrap
        if not items:
            items = _ptw_bootstrap_using_windows(hdr, date_from, cfg_root=cfg_root)

    # 2) Decide if a delta fetch is actually needed
    need_delta = not did_present
    if not need_delta:
        # Only do delta if activities indicate movement beyond our cursor
        try:
            act_ts = iso_to_ts(str(acts.get("watchlist") or acts.get("lists") or ""))
            cur_ts = iso_to_ts(str(date_from))
            need_delta = bool(act_ts and cur_ts and act_ts > cur_ts)
        except Exception:
            need_delta = False

    if need_delta:
        delta = _ptw_fetch_delta(hdr, date_from, cfg_root=cfg_root)
        items = _ptw_apply_delta(items, delta)

        # Safety net: if activities moved but delta looked empty, sweep a tiny window
        try:
            act_ts = iso_to_ts(str(acts.get("watchlist") or acts.get("lists") or ""))
            cur_ts = iso_to_ts(str(date_from))
            delta_size = (len(delta.get("movies", [])) + len(delta.get("shows", [])))
            if delta_size == 0 and act_ts and cur_ts and act_ts > cur_ts:
                tiny_since = ts_to_iso(int(time.time()) - 7 * 86400)
                tiny = _ptw_fetch_delta(hdr, tiny_since, cfg_root=cfg_root)
                items = _ptw_apply_delta(items, tiny)
        except Exception:
            pass

    # 3) Advance cursor from activities if available; otherwise use "now"
    try:
        acts2 = _activities(cfg_root)
    except Exception:
        acts2 = {}
    new_cursor = acts2.get("watchlist") or acts2.get("lists") or ts_to_iso(int(time.time()))
    try:
        store.set("watchlist", new_cursor)
    except Exception:
        pass

    # Persist shadow for next run
    store.save_shadow(items, int(time.time()))

    # 4) Normalize snapshot for orchestrator
    out: Dict[str, Dict[str, Any]] = {}
    for k, node in items.items():
        out[k] = {
            "type": node.get("type"),
            "title": node.get("title"),
            "year": node.get("year"),
            "ids": node.get("ids") or {}
        }

    # Log call counters in debug
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
        op_add, _op_neg = _gmt_ops_for_feature("watchlist")
        items_list = [it for it in items_list if not _gmt_suppress(store=store_gmt, item=it, feature="watchlist", write_op=op_add)]

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        typ = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}, "to": "plantowatch"}
        if entry["ids"]:
            (payload["movies"] if typ == "movie" else payload["shows"]).append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    r = _simkl_post(SIMKL_ADD_TO_LIST, headers=simkl_headers(simkl_cfg), json_payload=payload, timeout=45)
    if r and r.ok:
        # Move cursor forward and update shadow immediately
        now_iso = ts_to_iso(int(time.time()))
        store = _CursorStore(cfg_root)
        store.set("watchlist", now_iso)
        shadow = store.load_shadow()
        m = dict(shadow.get("items") or {})
        for it in items_list:
            k = canonical_key({"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year"), "type": (it.get("type") or "movie").lower()})
            m[k] = {"type": (it.get("type") or "movie").lower(), "title": it.get("title"), "year": it.get("year"), "ids": it.get("ids") or {}}
        store.save_shadow(m, int(time.time()))
        return sum(len(v) for v in payload.values())
    return 0


def _watchlist_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Remove items from PTW using the official history/remove endpoint.
    On success, advance both history and watchlist cursors and update shadow.
    """
    items_list = list(items)

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        typ = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if entry["ids"]:
            (payload["movies"] if typ == "movie" else payload["shows"]).append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0

    simkl_cfg = dict(cfg_root.get("simkl") or {})
    r = _simkl_post(SIMKL_HISTORY_REMOVE, headers=simkl_headers(simkl_cfg), json_payload=payload, timeout=45)
    if r and r.ok:
        now_iso = ts_to_iso(int(time.time()))
        store = _CursorStore(cfg_root)
        store.set("history", now_iso)
        store.set("watchlist", now_iso)
        shadow = store.load_shadow()
        m = dict(shadow.get("items") or {})
        for it in items_list:
            k = canonical_key({"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year"), "type": (it.get("type") or "movie").lower()})
            m.pop(k, None)
        store.save_shadow(m, int(time.time()))
        # GMT: record negatives for successful removes
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="watchlist", op="remove", origin="SIMKL")
        return sum(len(v) for v in payload.values())
    return 0


# ---------------------------------------------------------------------------
# Ratings
# ---------------------------------------------------------------------------

def _ratings_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Return a snapshot of SIMKL ratings, advancing the ratings cursor."""
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)

    if not _simkl_alive(hdr, cfg_root=cfg_root):
        # Debug call counters
        try:
            if (cfg_root.get("runtime") or {}).get("debug"):
                host_log("SIMKL.calls", {"feature": "ratings", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
        except Exception:
            pass
        return {}

    params: Dict[str, Any] = {"extended": "full"}
    df = store.get("ratings") or simkl_cfg.get("date_from")
    if df:
        params["date_from"] = df

    r = _simkl_get(SIMKL_RATINGS_GET, headers=hdr, params=params, timeout=45, cfg_root=cfg_root)
    data = _json_or_empty_dict(r)

    idx: Dict[str, Dict[str, Any]] = {}

    def handle(arr: List[dict], kind: str):
        for it in arr or []:
            node = it.get(kind) or {}
            ids = _ids_from_simkl_node(node)
            rating = it.get("rating")
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
            idx[key] = {"type": kind, "title": node.get("title"), "year": node.get("year"), "ids": ids, "rating": rating}

    handle(_read_as_list(data, "movies"), "movie")
    handle(_read_as_list(data, "shows"), "show")

    # Advance cursor using server Date header if present
    try:
        srv_now = r.headers.get("Date") if r else None
    except Exception:
        srv_now = None
    new_cursor = srv_now or ts_to_iso(int(time.time()))
    try:
        store.set("ratings", new_cursor)
    except Exception:
        pass

    # Debug call counters
    try:
        if (cfg_root.get("runtime") or {}).get("debug"):
            host_log("SIMKL.calls", {"feature": "ratings", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
    except Exception:
        pass

    return idx


def _ratings_set(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Set ratings in SIMKL for movies/shows. Sends in chunks, returns count of successful writes.
    GMT is consulted to avoid re-rating during quarantine.
    """
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

    return total


def _ratings_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Remove ratings from SIMKL. Records GMT negatives on success."""
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

    # GMT: record negatives (unrate) after successful removals
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and total:
        for it in items_list:
            _gmt_record(store=store_gmt, item=it, feature="ratings", op="unrate", origin="SIMKL")

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
        return {"bidirectional": True}

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
        description="Reads and writes SIMKL watchlist (PTW), ratings, and history with resilient cursors, ETag/TTL caching, and low-call indexing.",
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

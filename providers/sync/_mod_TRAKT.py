from __future__ import annotations

__VERSION__ = "1.1.0"
__all__ = ["OPS", "TRAKTModule", "get_manifest"]

import json
import time
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

import requests

# -----------------------------------------------------------------------------
# Trakt constants
# -----------------------------------------------------------------------------

UA = "CrossWatch/TraktModule"
TRAKT_BASE = "https://api.trakt.tv"

# Activities (server-side clocks to decide if anything moved)
TRAKT_LAST_ACTIVITIES = f"{TRAKT_BASE}/sync/last_activities"

# Watchlist
TRAKT_WATCHLIST_MOVIES = f"{TRAKT_BASE}/sync/watchlist/movies"
TRAKT_WATCHLIST_SHOWS  = f"{TRAKT_BASE}/sync/watchlist/shows"

# Ratings (GET user ratings by type) + mutations
TRAKT_USER_RATINGS_MOVIES = f"{TRAKT_BASE}/users/me/ratings/movies/all"
TRAKT_USER_RATINGS_SHOWS  = f"{TRAKT_BASE}/users/me/ratings/shows/all"
TRAKT_RATINGS_POST        = f"{TRAKT_BASE}/sync/ratings"
TRAKT_RATINGS_REMOVE      = f"{TRAKT_BASE}/sync/ratings/remove"

# History
TRAKT_HISTORY_BASE     = f"{TRAKT_BASE}/sync/history"           # POST add
TRAKT_HISTORY_GET_MOV  = f"{TRAKT_BASE}/sync/history/movies"
TRAKT_HISTORY_GET_EP   = f"{TRAKT_BASE}/sync/history/episodes"
TRAKT_HISTORY_REMOVE   = f"{TRAKT_BASE}/sync/history/remove"

_ID_KEYS = ("trakt", "imdb", "tmdb", "tvdb", "slug")
_TRAKT_ETAGS: Dict[str, str] = {}
_TRAKT_BODY_CACHE: Dict[str, Dict[str, Any]] = {}  # key -> {"status":int, "headers":dict, "body":str, "ts":float}

def _bust_trakt_watchlist_cache() -> None:
    # Invalidate the exact keys used by GET watchlist calls
    for k in ("watchlist_movies", "watchlist_shows"):
        _TRAKT_ETAGS.pop(k, None)
        _TRAKT_BODY_CACHE.pop(k, None)

def _bust_trakt_ratings_cache() -> None:
    # Invalidate the exact keys used by GET ratings calls
    for k in ("rt.movies", "rt.shows"):
        _TRAKT_ETAGS.pop(k, None)
        _TRAKT_BODY_CACHE.pop(k, None)

def _bust_run_memo_for(use_keys: Iterable[str]) -> None:
    """Drop any per-run memoized GETs whose use_etag_key matches one of use_keys."""
    try:
        bad = [k for k in list(_RUN_GET_CACHE.keys()) if len(k) >= 3 and (k[2] in set(use_keys))]
        for k in bad:
            _RUN_GET_CACHE.pop(k, None)
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Optional host integrations
# -----------------------------------------------------------------------------

try:
    from _logging import log as host_log
except Exception:  # pragma: no cover
    def host_log(*_a, **_k):  # type: ignore
        pass

try:
    from _statistics import Stats  # type: ignore
    _stats = Stats()
except Exception:
    _stats = None

def _emit_rating_event(*, action: str, node: Mapping[str, Any], prev: Optional[int], value: Optional[int]) -> None:
    """Emit a compact rating event for UI (watchlist-style spotlight & summary lanes)."""
    try:
        payload = {
            "feature": "ratings",
            "action": action,                   # "rate" | "unrate" | "update"
            "title": node.get("title"),
            "type": node.get("type") or _plural(node.get("type") or "movie"),
            "ids": dict(node.get("ids") or {}),
            "value": value,
            "prev": prev,
            "provider": "TRAKT",
            "ts": ts_to_iso(int(time.time())),
        }
        if _stats and hasattr(_stats, "record_event"):
            try:
                _stats.record_event(payload)   # preferred if available
            except Exception:
                pass
        # Fallback: structured host log entry (safe no-op if host_log is stub)
        try:
            host_log("event", payload)
        except Exception:
            pass
    except Exception:
        # Never let event logging break mutations
        pass

# -----------------------------------------------------------------------------
# Global call counters (debug visibility)
# -----------------------------------------------------------------------------

_CALLS = getattr(globals(), "_CALLS", {"GET": 0, "POST": 0})
globals()["_CALLS"] = _CALLS

# -----------------------------------------------------------------------------
# Storage roots: ALWAYS write to /config as requested
# -----------------------------------------------------------------------------

def _state_root() -> Path:
    """
    State lives under /config. This is the only writable folder in the user's setup.
    """
    base = Path("/config")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Last resort: still try to use /config even if mkdir fails (container may mount it)
        pass
    return base / ".cw_state"

# Files we keep:
# - trakt_activities.cache.json       (TTL 5 min)
# - trakt_etags.json                  (ETag per endpoint)
# - trakt_watchlist.shadow.json       (last full normalized snapshot)
# - trakt_ratings.shadow.json         (last full normalized snapshot)
# - trakt_history.cursor.json         (per-feature cursors/timestamps)

# -----------------------------------------------------------------------------
# Utilities
# -----------------------------------------------------------------------------

def iso_to_ts(s: str) -> int:
    """Parse ISO-8601 string into epoch seconds. Returns 0 on failure."""
    if not s:
        return 0
    try:
        import datetime as dt
        # Trakt returns e.g. "2024-09-01T10:20:30.000Z" or without millis
        s2 = s.replace("Z", "+0000").split(".")[0]
        return int(dt.datetime.strptime(s2, "%Y-%m-%dT%H:%M:%S%z").timestamp())
    except Exception:
        try:
            import datetime as dt
            return int(dt.datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return 0


def ts_to_iso(ts: int) -> str:
    """Format epoch seconds into UTC ISO-8601."""
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _record_http_trakt(r: Optional[requests.Response], *, endpoint: str, method: str, payload: Any = None) -> None:
    """
    Record HTTP telemetry if host statistics are available.
    Also increments global GET/POST counters for debug analysis.
    """
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

        ms = int(getattr(r, "elapsed", 0).total_seconds() * 1000) if (r is not None and getattr(r, "elapsed", None)) else 0

        # Rate headers (Trakt sends X-RateLimit fields)
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
            provider="TRAKT",
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


class _CachedRespTR:
    """Lightweight Response-like wrapper for cached Trakt bodies."""
    def __init__(self, status: int, headers: Mapping[str, Any], body: str):
        self.status_code = int(status)
        self.ok = 200 <= self.status_code < 300
        self.headers = dict(headers or {})
        self.content = body.encode("utf-8")
        self.elapsed = 0
    def json(self) -> Any:
        try:
            return json.loads(self.content.decode("utf-8") or "null")
        except Exception:
            return None

def _norm_params_trakt(p: Optional[dict]) -> Tuple[Tuple[str, str], ...]:
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

def _trakt_cache_key(use_etag_key: Optional[str], url: str, params: Optional[dict]) -> str:
    if use_etag_key:
        return use_etag_key
    return f"{url}::{json.dumps(dict(params or {}), sort_keys=True)}"

# -----------------------------------------------------------------------------
# HTTP helpers with backoff, memoization, and ETag support
# -----------------------------------------------------------------------------

# In-run GET memo to collapse identical calls
_RUN_GET_CACHE: Dict[Tuple[str, Tuple[Tuple[str, str], ...], str], Optional[requests.Response]] = {}

def _norm_params(p: Optional[dict]) -> Tuple[Tuple[str, str], ...]:
    if not p:
        return tuple()
    items = []
    for k, v in p.items():
        if isinstance(v, (list, tuple)):
            items.append((str(k), ",".join(map(str, v))))
        else:
            items.append((str(k), str(v)))
    items.sort()
    return tuple(items)


def trakt_headers(trakt_cfg: Mapping[str, Any]) -> Dict[str, str]:
    """Build standard Trakt headers."""
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {trakt_cfg.get('access_token','')}",
        "trakt-api-key": trakt_cfg.get("client_id",""),
        "trakt-api-version": "2",
    }


def _with_backoff(req_fn, *a, **kw) -> Optional[requests.Response]:
    """
    Defensive retries with exponential backoff.
    Retries on network errors, 429, and 5xx. Respects X-RateLimit-Remaining/Reset if provided.
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
                if r.status_code in (429,) or (500 <= r.status_code < 600):
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


def _etag_store_path() -> Path:
    return _state_root() / "trakt_etags.json"


def _load_etags() -> Dict[str, str]:
    try:
        return json.loads(_etag_store_path().read_text("utf-8"))
    except Exception:
        return {}


def _save_etags(d: Mapping[str, str]) -> None:
    try:
        root = _state_root()
        root.mkdir(parents=True, exist_ok=True)
        p = _etag_store_path()
        tmp = p.with_suffix(p.suffix + ".tmp")
        tmp.write_text(json.dumps(dict(d), ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, p)
    except Exception:
        pass

def _trakt_get(
    url: str,
    *,
    headers: Mapping[str, str],
    params: Optional[dict] = None,
    timeout: int = 45,
    cfg_root: Optional[Mapping[str, Any]] = None,   # accepted for parity; not used here
    force_refresh: bool = False,
    use_etag_key: Optional[str] = None
) -> Optional[requests.Response]:
    """
    Trakt GET with per-run memo + ETag revalidation.
    - If force_refresh=False and we already memoized this exact call, return memo.
    - Sends If-None-Match when we have an ETag; on 304 returns cached body wrapper.
    - On 200 stores body+ETag; on errors returns None.
    """
    key = (url, _norm_params_trakt(params), use_etag_key)
    # 1) per-run memo
    if not force_refresh and key in _RUN_GET_CACHE:
        r = _RUN_GET_CACHE[key]
        _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
        return r  # type: ignore[return-value]

    # 2) network with ETag
    hdrs = dict(headers or {})
    ck = _trakt_cache_key(use_etag_key, url, params)
    et = _TRAKT_ETAGS.get(ck)
    if et:
        hdrs["If-None-Match"] = et

    try:
        r = _with_backoff(requests.get, url, headers=hdrs, params=(params or {}), timeout=timeout)
    except Exception:
        _RUN_GET_CACHE[key] = None
        _record_http_trakt(None, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
        return None

    # 3) 304 -> synthesize Response from cached body (if available)
    if r is not None and getattr(r, "status_code", 0) == 304:
        prev = _TRAKT_BODY_CACHE.get(ck)
        if prev:
            r2 = _CachedRespTR(prev.get("status", 200), prev.get("headers", {}), prev.get("body", ""))
            _RUN_GET_CACHE[key] = r2  # type: ignore[assignment]
            _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="GET")  # count the 304
            return r2
        # No cached body; treat as empty 200 so callers don't crash
        r2 = _CachedRespTR(200, {}, "[]")
        _RUN_GET_CACHE[key] = r2  # type: ignore[assignment]
        _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="GET")  # count the 304
        return r2

    # 4) 2xx -> store ETag + body
    if r is not None and getattr(r, "ok", False):
        try:
            et_new = r.headers.get("ETag")
            if et_new:
                _TRAKT_ETAGS[ck] = et_new
            _TRAKT_BODY_CACHE[ck] = {
                "status": int(getattr(r, "status_code", 200) or 200),
                "headers": dict(getattr(r, "headers", {}) or {}),
                "body": (r.content or b"").decode("utf-8", errors="ignore"),
                "ts": time.time(),
            }
        except Exception:
            pass

    _RUN_GET_CACHE[key] = r  # type: ignore[assignment]
    _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
    return r


def _trakt_post(url: str, *, headers: Mapping[str, str], json_payload: Mapping[str, Any], timeout: int = 45) -> Optional[requests.Response]:
    try:
        r = _with_backoff(
            requests.post, url, headers=headers, json=json_payload, timeout=timeout
        )
    except Exception:
        _record_http_trakt(None, endpoint=url.replace(TRAKT_BASE, ""), method="POST", payload=json_payload)
        return None
    _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="POST", payload=json_payload)
    return r


def _json_or_empty_list(r: Optional[requests.Response]) -> List[Any]:
    """Parse a response that should be a list. 304 or bad → empty list."""
    if not r:
        return []
    if r.status_code == 304:
        # Not modified; caller should use shadow
        return []
    if not getattr(r, "ok", False):
        return []
    try:
        data = r.json()
    except Exception:
        return []
    return data if isinstance(data, list) else []


def _json_or_empty_dict(r: Optional[requests.Response]) -> Dict[str, Any]:
    """Parse a response that should be a dict. 304 or bad → empty dict."""
    if not r:
        return {}
    if r.status_code == 304:
        return {}
    if not getattr(r, "ok", False):
        return {}
    try:
        data = r.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


# -----------------------------------------------------------------------------
# Config helpers
# -----------------------------------------------------------------------------

def _cfg_get(cfg_root: Mapping[str, Any], path: str, default: Any) -> Any:
    """
    Lightweight nested config getter using dot path.
    Searches common roots: root, root['sync'], root['runtime'].
    """
    def _dig(d: Mapping[str, Any], keys: List[str]) -> Any:
        cur: Any = d
        for k in keys:
            if not isinstance(cur, Mapping):
                return default
            cur = cur.get(k)
            if cur is None:
                return default
        return cur
    keys = path.split(".")
    for root_key in ("", "sync", "runtime"):
        base = cfg_root if root_key == "" else (cfg_root.get(root_key) or {})
        val = _dig(base, keys)
        if val is not None and val != default:
            return val
    return default

def _iter_chunks(seq: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    """Yield list chunks of max `size` items."""
    if size <= 0:
        yield seq
        return
    for i in range(0, len(seq), size):
        yield seq[i:i+size]

# -----------------------------------------------------------------------------
# Shadows + cursors (persisted snapshots & timestamps under /config/.cw_state)
# -----------------------------------------------------------------------------

def _shadow_path(name: str) -> Path:
    return _state_root() / name

def _read_json(path: Path, fallback: Any) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return fallback

def _write_json(path: Path, data: Any) -> None:
    try:
        root = path.parent
        root.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass


def _watchlist_shadow_load() -> Dict[str, Any]:
    return _read_json(_shadow_path("trakt_watchlist.shadow.json"), {"items": {}, "ts": 0})

def _watchlist_shadow_save(items: Mapping[str, Any]) -> None:
    _write_json(_shadow_path("trakt_watchlist.shadow.json"), {"items": dict(items), "ts": int(time.time())})

def _ratings_shadow_load() -> Dict[str, Any]:
    return _read_json(_shadow_path("trakt_ratings.shadow.json"), {"items": {}, "ts": 0})

def _ratings_shadow_save(items: Mapping[str, Any]) -> None:
    _write_json(_shadow_path("trakt_ratings.shadow.json"), {"items": dict(items), "ts": int(time.time())})

def _cursor_store_load() -> Dict[str, Any]:
    return _read_json(_shadow_path("trakt_history.cursor.json"), {})

def _cursor_store_save(d: Mapping[str, Any]) -> None:
    _write_json(_shadow_path("trakt_history.cursor.json"), dict(d))


# -----------------------------------------------------------------------------
# Activities (file-cached; process-agnostic)
# -----------------------------------------------------------------------------

_RUN_ACT: Dict[str, Any] = {"ts": 0.0, "data": None}
_ACT_TTL_SEC = 60

def _activities(cfg_root: Mapping[str, Any]) -> Dict[str, Any]:
    """
    Fetch Trakt activities with a file-backed cache.
    Many features can piggyback on these timestamps to skip heavy calls.
    """
    now = time.time()
    # 1) in-run cache
    if (now - float(_RUN_ACT.get("ts") or 0)) < _ACT_TTL_SEC and (_RUN_ACT.get("data") is not None):
        return dict(_RUN_ACT["data"])

    # 2) file cache
    cache_file = _shadow_path("trakt_activities.cache.json")
    try:
        j = json.loads(cache_file.read_text("utf-8"))
        ts = float(j.get("ts") or 0)
        if (now - ts) < _ACT_TTL_SEC and isinstance(j.get("data"), dict):
            _RUN_ACT.update(ts=ts, data=j["data"])
            return dict(j["data"])
    except Exception:
        pass

    # 3) real call
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or cfg_root.get("Trakt") or {})
    hdr = trakt_headers(trakt_cfg)
    r = _trakt_get(TRAKT_LAST_ACTIVITIES, headers=hdr, params=None, use_etag_key="last_activities", timeout=30)
    data = _json_or_empty_dict(r)

    flat: Dict[str, Any] = {}
    # Known keys: all, movies, episodes, shows, seasons, lists, comments, watchlist, recommendations, account, lists, ratings, history
    def _copy_ts(d: Dict[str, Any], root: str) -> None:
        sub = data.get(root) or {}
        if isinstance(sub, dict):
            for k, v in sub.items():
                if isinstance(v, str):
                    flat[f"{root}.{k}"] = v
        # Also store top-level updated_at if exists
        if isinstance(data.get(root), str):
            flat[root] = data[root]

    for key in ("watchlist", "ratings", "history", "movies", "shows", "episodes", "lists"):
        _copy_ts(flat, key)

    # Persist both caches
    _RUN_ACT.update(ts=now, data=flat)
    try:
        cache_file.parent.mkdir(parents=True, exist_ok=True)
        cache_file.write_text(json.dumps({"ts": now, "data": flat}, ensure_ascii=False), "utf-8")
    except Exception:
        pass

    return flat

# -----------------------------------------------------------------------------
# Shapes & helpers
# -----------------------------------------------------------------------------

def _plural(kind: str) -> str:
    """Map Trakt object kind to orchestrator's plural type names."""
    k = (kind or "").lower()
    if k == "movie": return "movies"
    if k == "show": return "shows"
    if k == "season": return "seasons"
    if k == "episode": return "episodes"
    return k or "movies"

def canonical_key(kind: str, node: Mapping[str, Any]) -> str:
    """
    Produce a deterministic key based on IDs, falling back to type|title|year.
    Use plural type names to stay consistent with orchestrator filters.
    """
    ids = dict(node.get("ids") or {})
    for k in _ID_KEYS:
        v = ids.get(k)
        if v:
            return f"{k}:{v}".lower()
    t = (node.get("title") or "").strip().lower()
    y = node.get("year") or ""
    return f"{_plural(kind)}|title:{t}|year:{y}"

def minimal_node(kind: str, node: Mapping[str, Any]) -> Dict[str, Any]:
    """Return a minimal canonical node (pluralized type)."""
    ids = dict(node.get("ids") or {})
    return {
        "type": _plural(kind),
        "title": node.get("title"),
        "year": node.get("year"),
        "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}
    }

def _flatten_watchlist(arr: List[dict], kind: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for it in arr or []:
        node = it.get(kind) or {}
        key = canonical_key(kind, node)
        out[key] = minimal_node(kind, node)
    return out

def _flatten_ratings(arr: List[dict], kind: str) -> Dict[str, Any]:
    """
    Trakt ratings rows have `rating` (1..10) and `rated_at`.
    Keep both to enable recency-based conflict resolution upstream.
    """
    out: Dict[str, Any] = {}
    for it in arr or []:
        node = it.get(kind) or {}
        key = canonical_key(kind, node)
        rating = None
        if isinstance(it.get("rating"), (int, float)):
            try:
                rating = int(it.get("rating"))
            except Exception:
                rating = None
        row = minimal_node(kind, node)
        if rating is not None:
            row["rating"] = rating
        ra = it.get("rated_at")
        if isinstance(ra, str) and ra:
            row["rated_at"] = ra
        out[key] = row
    return out

def _flatten_history(arr: List[dict], kind: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for it in arr or []:
        node = it.get(kind) or {}
        key = canonical_key(kind, node)
        row = minimal_node(kind, node)
        row["watched"] = True
        # Keep watched_at if present; helps downstream merges
        if isinstance(it.get("watched_at"), str):
            row["watched_at"] = it.get("watched_at")
        out[key] = row
    return out

# -----------------------------------------------------------------------------
# Provider protocol
# -----------------------------------------------------------------------------

class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> Dict[str, Any]: ...

# -----------------------------------------------------------------------------
# Indices (watchlist, ratings, history)
# -----------------------------------------------------------------------------

def _watchlist_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Fresh Trakt watchlist snapshot with low API cost.
    - Conditional GETs (ETag) on movies & shows; 304 keeps shadow.
    - Never throw; on failures return last shadow.items.
    - Advance cursor to activities timestamp if present, else to now when updated.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    # Isolate this run from stale in-run memo
    try:
        _RUN_GET_CACHE.clear()
    except Exception:
        pass

    # Load activities (for cursor progression) and shadow (as safe fallback)
    acts = _activities(cfg_root)
    if not isinstance(acts, dict):
        acts = {}
    shadow = _watchlist_shadow_load()
    items: Dict[str, Dict[str, Any]] = dict((shadow.get("items") or {})) if isinstance(shadow, dict) else {}

    params = {"extended": "full"}

    # Conditional GETs; 304 is cheap
    r_mov = _trakt_get(TRAKT_WATCHLIST_MOVIES, headers=hdr, params=params,
                       timeout=45, force_refresh=False, use_etag_key="watchlist_movies")
    r_sho = _trakt_get(TRAKT_WATCHLIST_SHOWS,  headers=hdr, params=params,
                       timeout=45, force_refresh=False, use_etag_key="watchlist_shows")

    mov = _json_or_empty_list(r_mov)
    sho = _json_or_empty_list(r_sho)

    updated = False
    if (r_mov is not None and getattr(r_mov, "status_code", 0) == 200) or \
       (r_sho is not None and getattr(r_sho, "status_code", 0) == 200):
        out: Dict[str, Dict[str, Any]] = {}
        if mov:
            out.update(_flatten_watchlist(mov, "movie"))
        if sho:
            out.update(_flatten_watchlist(sho, "show"))
        items = out
        _watchlist_shadow_save(items)  # save pure items, not a wrapper
        updated = True

    # Cursor: prefer server activities; else move when we updated
    act_updated = str(acts.get("watchlist.updated_at") or acts.get("watchlist") or acts.get("lists") or "")
    cursors = _cursor_store_load()
    if not isinstance(cursors, dict):
        cursors = {}
    if act_updated:
        cursors["watchlist"] = act_updated
        _cursor_store_save(cursors)
    elif updated:
        cursors["watchlist"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

    return items

def _ratings_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build a Trakt ratings snapshot (movies + shows).
    Guard with activities + ETag + shadow + optional TTL.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    # Optional TTL to avoid first-run full pulls when nothing changed
    ttl_min = int(_cfg_get(cfg_root, "ratings.cache.ttl_minutes", 0) or 0)
    ttl_sec = max(0, ttl_min * 60)

    acts = _activities(cfg_root)
    cursors = _cursor_store_load()
    last_cur = str(cursors.get("ratings") or "")
    act_updated = str(acts.get("ratings.updated_at") or acts.get("ratings") or "")

    shadow = _ratings_shadow_load()
    items = dict(shadow.get("items") or {})
    shadow_ts = int(shadow.get("ts") or 0)

    # Activities gate: if nothing moved since last cursor → keep shadow
    if last_cur and act_updated and iso_to_ts(act_updated) <= iso_to_ts(last_cur):
        return items

    # TTL gate: if present and fresh → keep shadow
    now = int(time.time())
    if ttl_sec > 0 and shadow_ts > 0 and (now - shadow_ts) < ttl_sec and items:
        return items

    # Use /users/me/ratings/*/all because it returns rated_at per row
    m = _trakt_get(TRAKT_USER_RATINGS_MOVIES, headers=hdr, params={"extended": "full"}, use_etag_key="rt.movies", timeout=45)
    s = _trakt_get(TRAKT_USER_RATINGS_SHOWS,  headers=hdr, params={"extended": "full"}, use_etag_key="rt.shows",  timeout=45)

    mov = _json_or_empty_list(m)
    sho = _json_or_empty_list(s)

    if (m is not None and m.status_code == 304) and (s is not None and s.status_code == 304) and items:
        # Both unchanged; rely on shadow
        pass
    else:
        got_any = False
        out: Dict[str, Any] = {}
        if mov:
            out.update(_flatten_ratings(mov, "movie")); got_any = True
        if sho:
            out.update(_flatten_ratings(sho, "show"));  got_any = True
        if got_any:
            items = out
            _ratings_shadow_save(items)

    # Move cursor to server view if available (best for consistency)
    if act_updated:
        cursors["ratings"] = act_updated
        _cursor_store_save(cursors)

    return items

def _history_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build a Trakt history snapshot since last cursor (start_at).
    This one benefits less from ETag, but we still guard with activities.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    acts = _activities(cfg_root)
    cursors = _cursor_store_load()
    last_cur = str(cursors.get("history") or "")
    act_updated = str(acts.get("history.updated_at") or acts.get("history") or "")

    params: Dict[str, Any] = {}
    if last_cur:
        params["start_at"] = last_cur

    r_mov = _trakt_get(TRAKT_HISTORY_GET_MOV, headers=hdr, params=params or None, use_etag_key=None, timeout=45)
    r_ep  = _trakt_get(TRAKT_HISTORY_GET_EP,  headers=hdr, params=params or None, use_etag_key=None, timeout=45)

    mov = _json_or_empty_list(r_mov)
    eps = _json_or_empty_list(r_ep)

    out: Dict[str, Dict[str, Any]] = {}
    if mov:
        out.update(_flatten_history(mov, "movie"))
    if eps:
        out.update(_flatten_history(eps, "episode"))

    # Advance cursor to activities time (server view) if available; else "now"
    new_cursor = act_updated or ts_to_iso(int(time.time()))
    if new_cursor:
        cursors["history"] = new_cursor
        _cursor_store_save(cursors)

    return out

# -----------------------------------------------------------------------------
# Mutations (watchlist, ratings, history)
# -----------------------------------------------------------------------------

def _watchlist_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Add items to Trakt watchlist (mixed types supported)."""
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        typ = _plural(it.get("type") or "movie")
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if typ == "movies":
            movies.append(entry)
        elif typ == "shows":
            shows.append(entry)

    payload: Dict[str, Any] = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    if not payload:
        return 0

    url = f"{TRAKT_BASE}/sync/watchlist"
    r = _trakt_post(url, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        # Bust caches + memo, advance cursor, refresh shadow
        try:
            _bust_trakt_watchlist_cache()
            _bust_run_memo_for(["watchlist_movies", "watchlist_shows"])
        except Exception:
            pass

        cursors = _cursor_store_load()
        cursors["watchlist"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        sh = _watchlist_shadow_load()
        m = dict(sh.get("items") or {})
        for it in items or []:
            typ = _plural(it.get("type") or "movie")
            node = {"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year")}
            key = canonical_key(typ.rstrip("s"), node)  # canonical_key expects singular hint
            m[key] = {
                "type": typ,
                "title": node["title"],
                "year": node["year"],
                "ids": {k: node["ids"].get(k) for k in _ID_KEYS if node["ids"].get(k)}
            }
        _watchlist_shadow_save(m)
        return sum(len(v) for v in payload.values())
    return 0


def _watchlist_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Remove items from Trakt watchlist."""
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        typ = _plural(it.get("type") or "movie")
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if typ == "movies":
            movies.append(entry)
        elif typ == "shows":
            shows.append(entry)

    payload: Dict[str, Any] = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    if not payload:
        return 0

    url = f"{TRAKT_BASE}/sync/watchlist/remove"
    r = _trakt_post(url, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        try:
            _bust_trakt_watchlist_cache()
            _bust_run_memo_for(["watchlist_movies", "watchlist_shows"])
        except Exception:
            pass

        cursors = _cursor_store_load()
        cursors["watchlist"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        sh = _watchlist_shadow_load()
        m = dict(sh.get("items") or {})
        for it in items or []:
            typ = _plural(it.get("type") or "movie")
            node = {"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year")}
            key = canonical_key(typ.rstrip("s"), node)
            m.pop(key, None)
        _watchlist_shadow_save(m)
        return sum(len(v) for v in payload.values())
    return 0


def _ratings_set(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Set (upsert) ratings at Trakt (/sync/ratings) with batch support + events."""
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    # Batch size is configurable; default safe value
    batch_size = int(_cfg_get(cfg_root, "ratings.write.batch_size", 200) or 200)

    # Prepare per-type payloads and collect event context
    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    ctx:    List[Dict[str, Any]] = []  # keep full incoming nodes for event emission

    for it in items or []:
        ids = dict(it.get("ids") or {})
        rating = it.get("rating")
        if rating is None:
            continue
        entry: Dict[str, Any] = {"rating": int(rating), "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        ra = it.get("rated_at")
        if isinstance(ra, str) and ra:
            entry["rated_at"] = ra  # let server honor it if valid
        typ = _plural(it.get("type") or "movie")
        # Keep original node metadata for event emission
        node = {
            "type": typ,
            "title": it.get("title"),
            "year": it.get("year"),
            "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)},
            "rating": int(rating),
            "rated_at": ra if isinstance(ra, str) else None,
        }
        ctx.append(node)
        if typ == "movies":
            movies.append(entry)
        elif typ == "shows":
            shows.append(entry)

    total = 0
    if not movies and not shows:
        return total

    # Chunk and post; we keep events and shadow in sync after success
    def _post_payload(payload: Dict[str, Any]) -> bool:
        r = _trakt_post(TRAKT_RATINGS_POST, headers=hdr, json_payload=payload, timeout=45)
        return bool(r and r.ok)

    # Shadow for event delta classification (rate vs update)
    sh = _ratings_shadow_load()
    shadow_map: Dict[str, Any] = dict(sh.get("items") or {})

    # Build chunks
    mov_chunks = list(_iter_chunks(movies, batch_size)) if movies else []
    sho_chunks = list(_iter_chunks(shows,  batch_size)) if shows  else []

    # Send movie chunks
    for chunk in mov_chunks:
        if _post_payload({"movies": chunk}):
            total += len(chunk)
        else:
            continue  # skip shadow/events on failed chunk

        # Update shadow + emit events for each item in this chunk
        for entry in chunk:
            node = next((n for n in ctx if n["ids"] == entry["ids"]), None)
            if not node:
                continue
            key = None
            try:
                key = canonical_key("movie", {"ids": node["ids"], "title": node.get("title"), "year": node.get("year")})
            except Exception:
                pass
            # Compute prev and new for event
            prev_val = None
            if key and key in shadow_map and isinstance(shadow_map[key], dict):
                p = shadow_map[key].get("rating")
                if isinstance(p, int):
                    prev_val = p
            new_val = int(node.get("rating")) if isinstance(node.get("rating"), int) else None
            # Update shadow
            shadow_map[key] = {
                "type": "movies",
                "title": node.get("title"),
                "year": node.get("year"),
                "ids": node.get("ids"),
                "rating": new_val,
                "rated_at": node.get("rated_at") or ts_to_iso(int(time.time())),
            }
            # Emit event
            if prev_val is None and new_val is not None:
                _emit_rating_event(action="rate", node=shadow_map[key], prev=None, value=new_val)
            elif prev_val is not None and new_val is not None and prev_val != new_val:
                _emit_rating_event(action="update", node=shadow_map[key], prev=prev_val, value=new_val)
            else:
                # Same value; emit a lightweight rate event for visibility
                _emit_rating_event(action="rate", node=shadow_map[key], prev=prev_val, value=new_val)

    # Send show chunks
    for chunk in sho_chunks:
        if _post_payload({"shows": chunk}):
            total += len(chunk)
        else:
            continue

        for entry in chunk:
            node = next((n for n in ctx if n["ids"] == entry["ids"]), None)
            if not node:
                continue
            key = None
            try:
                key = canonical_key("show", {"ids": node["ids"], "title": node.get("title"), "year": node.get("year")})
            except Exception:
                pass
            prev_val = None
            if key and key in shadow_map and isinstance(shadow_map[key], dict):
                p = shadow_map[key].get("rating")
                if isinstance(p, int):
                    prev_val = p
            new_val = int(node.get("rating")) if isinstance(node.get("rating"), int) else None
            shadow_map[key] = {
                "type": "shows",
                "title": node.get("title"),
                "year": node.get("year"),
                "ids": node.get("ids"),
                "rating": new_val,
                "rated_at": node.get("rated_at") or ts_to_iso(int(time.time())),
            }
            if prev_val is None and new_val is not None:
                _emit_rating_event(action="rate", node=shadow_map[key], prev=None, value=new_val)
            elif prev_val is not None and new_val is not None and prev_val != new_val:
                _emit_rating_event(action="update", node=shadow_map[key], prev=prev_val, value=new_val)
            else:
                _emit_rating_event(action="rate", node=shadow_map[key], prev=prev_val, value=new_val)

    if total > 0:
        # Bust caches + memo
        try:
            _bust_trakt_ratings_cache()
            _bust_run_memo_for(["rt.movies", "rt.shows"])
        except Exception:
            pass

        # Move cursor to now (activities will catch up soon)
        cursors = _cursor_store_load()
        cursors["ratings"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        # Persist shadow
        _ratings_shadow_save(shadow_map)

    return total


def _ratings_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Unset ratings at Trakt (/sync/ratings/remove) with batch support + events."""
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    batch_size = int(_cfg_get(cfg_root, "ratings.write.batch_size", 200) or 200)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    ctx:    List[Dict[str, Any]] = []  # carry metadata for events

    for it in items or []:
        ids = dict(it.get("ids") or {})
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        typ = _plural(it.get("type") or "movie")
        node = {
            "type": typ,
            "title": it.get("title"),
            "year": it.get("year"),
            "ids": entry["ids"],
        }
        ctx.append(node)
        if typ == "movies":
            movies.append(entry)
        elif typ == "shows":
            shows.append(entry)

    total = 0
    if not movies and not shows:
        return total

    def _post_payload(payload: Dict[str, Any]) -> bool:
        r = _trakt_post(TRAKT_RATINGS_REMOVE, headers=hdr, json_payload=payload, timeout=45)
        return bool(r and r.ok)

    # Current shadow for prev values
    sh = _ratings_shadow_load()
    shadow_map: Dict[str, Any] = dict(sh.get("items") or {})

    for chunk in list(_iter_chunks(movies, batch_size)) if movies else []:
        if _post_payload({"movies": chunk}):
            total += len(chunk)
        else:
            continue
        for entry in chunk:
            node = next((n for n in ctx if n["ids"] == entry["ids"]), None)
            if not node:
                continue
            key = None
            try:
                key = canonical_key("movie", {"ids": node["ids"], "title": node.get("title"), "year": node.get("year")})
            except Exception:
                pass
            prev_val = None
            if key and key in shadow_map and isinstance(shadow_map[key], dict):
                p = shadow_map[key].get("rating")
                if isinstance(p, int):
                    prev_val = p
                # Remove rating in shadow but keep node
                shadow_map[key].pop("rating", None)
            _emit_rating_event(action="unrate", node={"type": "movies", "title": node.get("title"), "ids": node.get("ids")}, prev=prev_val, value=None)

    for chunk in list(_iter_chunks(shows, batch_size)) if shows else []:
        if _post_payload({"shows": chunk}):
            total += len(chunk)
        else:
            continue
        for entry in chunk:
            node = next((n for n in ctx if n["ids"] == entry["ids"]), None)
            if not node:
                continue
            key = None
            try:
                key = canonical_key("show", {"ids": node["ids"], "title": node.get("title"), "year": node.get("year")})
            except Exception:
                pass
            prev_val = None
            if key and key in shadow_map and isinstance(shadow_map[key], dict):
                p = shadow_map[key].get("rating")
                if isinstance(p, int):
                    prev_val = p
                shadow_map[key].pop("rating", None)
            _emit_rating_event(action="unrate", node={"type": "shows", "title": node.get("title"), "ids": node.get("ids")}, prev=prev_val, value=None)

    if total > 0:
        try:
            _bust_trakt_ratings_cache()
            _bust_run_memo_for(["rt.movies", "rt.shows"])
        except Exception:
            pass

        cursors = _cursor_store_load()
        cursors["ratings"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        _ratings_shadow_save(shadow_map)

    return total


def _history_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Add history entries (scrobbles) to Trakt."""
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    episodes: List[Dict[str, Any]] = []
    now_iso = ts_to_iso(int(time.time()))
    for it in items or []:
        ids = dict(it.get("ids") or {})
        entry = {"watched_at": it.get("watched_at") or now_iso, "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        typ = _plural(it.get("type") or "movie")
        if typ == "movies":
            movies.append(entry)
        else:
            episodes.append(entry)

    payload: Dict[str, Any] = {}
    if movies:   payload["movies"]   = movies
    if episodes: payload["episodes"] = episodes
    if not payload:
        return 0

    r = _trakt_post(TRAKT_HISTORY_BASE, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        cursors = _cursor_store_load()
        cursors["history"] = now_iso
        _cursor_store_save(cursors)
        return sum(len(v) for v in payload.values())
    return 0


def _history_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """Remove history entries from Trakt."""
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    episodes: List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        typ = _plural(it.get("type") or "movie")
        if typ == "movies":
            movies.append(entry)
        else:
            episodes.append(entry)

    payload: Dict[str, Any] = {}
    if movies:   payload["movies"]   = movies
    if episodes: payload["episodes"] = episodes
    if not payload:
        return 0

    r = _trakt_post(TRAKT_HISTORY_REMOVE, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        cursors = _cursor_store_load()
        cursors["history"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)
        return sum(len(v) for v in payload.values())
    return 0


# -----------------------------------------------------------------------------
# OPS adapter
# -----------------------------------------------------------------------------

class _TraktOPS:
    def name(self) -> str:
        return "TRAKT"

    def label(self) -> str:
        return "Trakt"

    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": False}

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": False,
            "ratings": {
                "types": {"movies": True, "shows": True, "seasons": False, "episodes": False},
                "upsert": True,
                "unrate": True,
                "from_date": False,
            },
        }

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        if feature == "watchlist":
            res = _watchlist_index(cfg)
            try:
                if (cfg.get("runtime") or {}).get("debug"):
                    host_log("TRAKT.calls", {"feature": "watchlist", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
            except Exception:
                pass
            return res
        if feature == "ratings":
            res = _ratings_index(cfg)
            try:
                if (cfg.get("runtime") or {}).get("debug"):
                    host_log("TRAKT.calls", {"feature": "ratings", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
            except Exception:
                pass
            return res
        if feature == "history":
            res = _history_index(cfg)
            try:
                if (cfg.get("runtime") or {}).get("debug"):
                    host_log("TRAKT.calls", {"feature": "history", "GET": _CALLS.get("GET", 0), "POST": _CALLS.get("POST", 0)})
            except Exception:
                pass
            return res
        if feature == "playlists":
            return {}
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        if feature == "watchlist":
            if dry_run:
                # Count how many will be posted
                movies = shows = 0
                for it in items or []:
                    typ = _plural(it.get("type") or "movie")
                    if typ == "movies": movies += 1
                    elif typ == "shows": shows += 1
                return {"ok": True, "count": movies + shows, "dry_run": True}
            cnt = _watchlist_add(cfg, items)
            return {"ok": True, "count": int(cnt)}

        if feature == "ratings":
            if dry_run:
                payload = 0
                for it in items or []:
                    if it.get("rating") is not None:
                        typ = _plural(it.get("type") or "movie")
                        if typ in ("movies", "shows"):
                            payload += 1
                return {"ok": True, "count": payload, "dry_run": True}
            cnt = _ratings_set(cfg, items)
            return {"ok": True, "count": int(cnt)}

        if feature == "history":
            if dry_run:
                movies = episodes = 0
                for it in items or []:
                    typ = _plural(it.get("type") or "movie")
                    if typ == "movies": movies += 1
                    else: episodes += 1
                return {"ok": True, "count": movies + episodes, "dry_run": True}
            cnt = _history_add(cfg, items)
            return {"ok": True, "count": int(cnt)}

        return {"ok": True, "count": 0}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        if feature == "watchlist":
            if dry_run:
                movies = shows = 0
                for it in items or []:
                    typ = _plural(it.get("type") or "movie")
                    if typ == "movies": movies += 1
                    elif typ == "shows": shows += 1
                return {"ok": True, "count": movies + shows, "dry_run": True}
            cnt = _watchlist_remove(cfg, items)
            return {"ok": True, "count": int(cnt)}

        if feature == "ratings":
            if dry_run:
                movies = shows = 0
                for it in items or []:
                    typ = _plural(it.get("type") or "movie")
                    if typ == "movies": movies += 1
                    elif typ == "shows": shows += 1
                return {"ok": True, "count": movies + shows, "dry_run": True}
            cnt = _ratings_remove(cfg, items)
            return {"ok": True, "count": int(cnt)}

        if feature == "history":
            if dry_run:
                movies = episodes = 0
                for it in items or []:
                    typ = _plural(it.get("type") or "movie")
                    if typ == "movies": movies += 1
                    else: episodes += 1
                return {"ok": True, "count": movies + episodes, "dry_run": True}
            cnt = _history_remove(cfg, items)
            return {"ok": True, "count": int(cnt)}

        return {"ok": True, "count": 0}


OPS: InventoryOps = _TraktOPS()

# -----------------------------------------------------------------------------
# Module manifest
# -----------------------------------------------------------------------------

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


class TRAKTModule(SyncModule):
    info = ModuleInfo(
        name="TRAKT",
        version=__VERSION__,
        description="Trakt connector with activities/ETag/TTL guards, batch writes, and rating events under /config.",
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
                    "trakt": {
                        "type": "object",
                        "properties": {
                            "client_id": {"type": "string", "minLength": 1},
                            "access_token": {"type": "string", "minLength": 1},
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
                            },
                            "write": {
                                "type": "object",
                                "properties": {
                                    "batch_size": {"type": "integer", "minimum": 1}
                                }
                            }
                        }
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}},
                    },
                },
                "required": ["trakt"],
            },
        ),
    )

    @staticmethod
    def supported_features() -> dict:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": False}


def get_manifest() -> dict:
    return {
        "name": TRAKTModule.info.name,
        "label": "Trakt",
        "features": TRAKTModule.supported_features(),
        "capabilities": {"bidirectional": True},
        "version": TRAKTModule.info.version,
        "vendor": TRAKTModule.info.vendor,
        "description": TRAKTModule.info.description,
    }

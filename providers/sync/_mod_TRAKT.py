"""Trakt sync provider module.

Implements read/write for watchlist, ratings, and history with guarded API
usage (activities gates, ETag memo), and durable shadows/cursors under
`/config/.cw_state`. This is a documentation-only cleanup; no code or
identifiers were changed.
"""

from __future__ import annotations

__VERSION__ = "1.0.0"
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

# Ratings
TRAKT_RATINGS_MOVIES   = f"{TRAKT_BASE}/sync/ratings/movies"
TRAKT_RATINGS_SHOWS    = f"{TRAKT_BASE}/sync/ratings/shows"
TRAKT_RATINGS_POST     = f"{TRAKT_BASE}/sync/ratings"
TRAKT_RATINGS_REMOVE   = f"{TRAKT_BASE}/sync/ratings/remove"

# History
TRAKT_HISTORY_BASE     = f"{TRAKT_BASE}/sync/history"           # POST add
TRAKT_HISTORY_GET_MOV  = f"{TRAKT_BASE}/sync/history/movies"
TRAKT_HISTORY_GET_EP   = f"{TRAKT_BASE}/sync/history/episodes"
TRAKT_HISTORY_REMOVE   = f"{TRAKT_BASE}/sync/history/remove"

_ID_KEYS = ("trakt", "imdb", "tmdb", "tvdb", "slug")

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

        el = getattr(r, "elapsed", None) if r is not None else None
        ms = int((el.total_seconds() if hasattr(el, "total_seconds") else 0) * 1000) if el is not None else 0

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


def _trakt_get(url: str, *, headers: Mapping[str, str], params: Optional[dict] = None, use_etag_key: Optional[str] = None, timeout: int = 45) -> Optional[requests.Response]:
    """
    GET with in-run memoization and optional ETag.
    If use_etag_key is set, we will send If-None-Match and store the ETag when 200.
    """
    params_norm = _norm_params(params)
    memo_key = (url, params_norm, use_etag_key or "")
    if memo_key in _RUN_GET_CACHE:
        r = _RUN_GET_CACHE[memo_key]
        _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
        return r

    hdr = dict(headers)
    if use_etag_key:
        etags = _load_etags()
        etag = etags.get(use_etag_key)
        if etag:
            hdr["If-None-Match"] = etag

    try:
        r = _with_backoff(
            requests.get, url, headers=hdr, params=(params or {}), timeout=timeout
        )
    except Exception:
        _record_http_trakt(None, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
        _RUN_GET_CACHE[memo_key] = None
        return None

    # Save new ETag if present and 200
    if use_etag_key and r is not None and r.status_code == 200:
        new_etag = r.headers.get("ETag")
        if new_etag:
            etags = _load_etags()
            if etags.get(use_etag_key) != new_etag:
                etags[use_etag_key] = new_etag
                _save_etags(etags)

    _RUN_GET_CACHE[memo_key] = r
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
_ACT_TTL_SEC = 300  # 5 minutes

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

def canonical_key(kind: str, node: Mapping[str, Any]) -> str:
    """
    Produce a deterministic key based on IDs, falling back to title/year/ids.
    """
    ids = dict(node.get("ids") or {})
    for k in _ID_KEYS:
        v = ids.get(k)
        if v:
            return f"{k}:{v}".lower()
    t = (node.get("title") or "").strip().lower()
    y = node.get("year") or ""
    return f"{kind}|title:{t}|year:{y}"


def minimal_node(kind: str, node: Mapping[str, Any]) -> Dict[str, Any]:
    ids = dict(node.get("ids") or {})
    return {
        "type": kind,
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
    out: Dict[str, Any] = {}
    for it in arr or []:
        node = it.get(kind) or {}
        key = canonical_key(kind, node)
        rating = None
        # Trakt rating can be under 'rating'
        if isinstance(it.get("rating"), (int, float)):
            rating = int(it.get("rating") or 0)
        row = minimal_node(kind, node)
        row["rating"] = rating
        out[key] = row
    return out


def _flatten_history(arr: List[dict], kind: str) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for it in arr or []:
        node = it.get(kind) or {}
        key = canonical_key(kind, node)
        row = minimal_node(kind, node)
        row["watched"] = True
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
# Indexers with guards + ETag + shadows
# -----------------------------------------------------------------------------

def _watchlist_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build a Trakt watchlist snapshot with minimal API cost.
    - If activities show no movement since last cursor, return last shadow.
    - Otherwise GET with ETag; if 304 → use shadow; if 200 → refresh shadow.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    acts = _activities(cfg_root)
    # Cursor: last seen watchlist.updated_at
    cursors = _cursor_store_load()
    last_cur = str(cursors.get("watchlist") or "")
    act_updated = str(acts.get("watchlist.updated_at") or acts.get("watchlist") or "")

    shadow = _watchlist_shadow_load()
    items = dict(shadow.get("items") or {})

    # Guard: if activities did not move, keep shadow
    if last_cur and act_updated and iso_to_ts(act_updated) <= iso_to_ts(last_cur):
        return items

    # Fetch with ETag per endpoint and combine
    m = _trakt_get(TRAKT_WATCHLIST_MOVIES, headers=hdr, params={"extended": "full"}, use_etag_key="wl.movies", timeout=45)
    s = _trakt_get(TRAKT_WATCHLIST_SHOWS,  headers=hdr, params={"extended": "full"}, use_etag_key="wl.shows",  timeout=45)

    mov = _json_or_empty_list(m)
    sho = _json_or_empty_list(s)

    # If both were 304 or empty but we have a shadow, keep it
    if (m is not None and m.status_code == 304) and (s is not None and s.status_code == 304) and items:
        # no change
        pass
    else:
        # Rebuild from fresh pulls (if any returned 200)
        got_any = False
        out: Dict[str, Any] = {}
        if mov:
            out.update(_flatten_watchlist(mov, "movie"))
            got_any = True
        if sho:
            out.update(_flatten_watchlist(sho, "show"))
            got_any = True
        if got_any:
            items = out
            _watchlist_shadow_save(items)

    # Advance cursor to activities point (server-side truth) if present
    if act_updated:
        cursors["watchlist"] = act_updated
        _cursor_store_save(cursors)

    return items


def _ratings_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """
    Build a Trakt ratings snapshot.
    Use activities guard + ETag + shadow.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    acts = _activities(cfg_root)
    cursors = _cursor_store_load()
    last_cur = str(cursors.get("ratings") or "")
    act_updated = str(acts.get("ratings.updated_at") or acts.get("ratings") or "")

    shadow = _ratings_shadow_load()
    items = dict(shadow.get("items") or {})

    # Guard: if nothing moved since last cursor, keep shadow
    if last_cur and act_updated and iso_to_ts(act_updated) <= iso_to_ts(last_cur):
        return items

    m = _trakt_get(TRAKT_RATINGS_MOVIES, headers=hdr, params={"extended": "full"}, use_etag_key="rt.movies", timeout=45)
    s = _trakt_get(TRAKT_RATINGS_SHOWS,  headers=hdr, params={"extended": "full"}, use_etag_key="rt.shows",  timeout=45)

    mov = _json_or_empty_list(m)
    sho = _json_or_empty_list(s)

    if (m is not None and m.status_code == 304) and (s is not None and s.status_code == 304) and items:
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
# Mutations (add/remove ratings/history, add/remove watchlist)
# -----------------------------------------------------------------------------

def _watchlist_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Trakt watchlist add goes through /sync/watchlist (POST by type).
    We accept mixed items and split into movies/shows.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        typ = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if typ == "movie":
            movies.append(entry)
        else:
            shows.append(entry)

    payload = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    if not payload:
        return 0

    # Trakt watchlist add/remove uses /sync/watchlist and /sync/watchlist/remove, but the
    # v2 combined endpoint is typically /sync/watchlist.
    url = f"{TRAKT_BASE}/sync/watchlist"
    r = _trakt_post(url, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        # Bust ETags for watchlist and advance cursor to "now" (activities will shortly reflect)
        et = _load_etags()
        changed = False
        for k in ("wl.movies", "wl.shows"):
            if k in et:
                et.pop(k, None)
                changed = True
        if changed:
            _save_etags(et)

        cursors = _cursor_store_load()
        cursors["watchlist"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        # Refresh local shadow best-effort (append-only)
        sh = _watchlist_shadow_load()
        m = dict(sh.get("items") or {})
        for it in items or []:
            typ = (it.get("type") or "movie").lower()
            node = {"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year")}
            key = canonical_key(typ, node)
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
    """
    Remove items from Trakt watchlist.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        typ = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if typ == "movie":
            movies.append(entry)
        else:
            shows.append(entry)

    payload = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    if not payload:
        return 0

    url = f"{TRAKT_BASE}/sync/watchlist/remove"
    r = _trakt_post(url, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        # Bust ETags for watchlist and advance cursor
        et = _load_etags()
        changed = False
        for k in ("wl.movies", "wl.shows"):
            if k in et:
                et.pop(k, None); changed = True
        if changed:
            _save_etags(et)

        cursors = _cursor_store_load()
        cursors["watchlist"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        # Update shadow: remove keys
        sh = _watchlist_shadow_load()
        m = dict(sh.get("items") or {})
        for it in items or []:
            typ = (it.get("type") or "movie").lower()
            node = {"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year")}
            key = canonical_key(typ, node)
            m.pop(key, None)
        _watchlist_shadow_save(m)
        return sum(len(v) for v in payload.values())
    return 0


def _ratings_set(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Set ratings on Trakt (/sync/ratings).
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []

    for it in items or []:
        ids = dict(it.get("ids") or {})
        rating = it.get("rating")
        if rating is None:
            continue
        entry = {"rating": int(rating), "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if (it.get("type") or "movie") == "movie":
            movies.append(entry)
        else:
            shows.append(entry)

    payload = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    if not payload:
        return 0

    r = _trakt_post(TRAKT_RATINGS_POST, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        # Bust ETags (ratings lists) and advance cursor
        et = _load_etags()
        changed = False
        for k in ("rt.movies", "rt.shows"):
            if k in et:
                et.pop(k, None); changed = True
        if changed:
            _save_etags(et)

        cursors = _cursor_store_load()
        cursors["ratings"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        # Update shadow (best-effort)
        sh = _ratings_shadow_load()
        m = dict(sh.get("items") or {})
        for it in items or []:
            typ = (it.get("type") or "movie").lower()
            node = {"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year")}
            key = canonical_key(typ, node)
            row = m.get(key) or {"type": typ, "title": it.get("title"), "year": it.get("year"), "ids": {k: node["ids"].get(k) for k in _ID_KEYS if node["ids"].get(k)}}
            if it.get("rating") is not None:
                row["rating"] = int(it.get("rating") or 0)
            m[key] = row
        _ratings_shadow_save(m)

        return sum(len(v) for v in payload.values())
    return 0


def _ratings_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Remove ratings on Trakt.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if (it.get("type") or "movie") == "movie":
            movies.append(entry)
        else:
            shows.append(entry)

    payload = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    if not payload:
        return 0

    r = _trakt_post(TRAKT_RATINGS_REMOVE, headers=hdr, json_payload=payload, timeout=45)
    if r and r.ok:
        et = _load_etags()
        changed = False
        for k in ("rt.movies", "rt.shows"):
            if k in et:
                et.pop(k, None); changed = True
        if changed:
            _save_etags(et)

        cursors = _cursor_store_load()
        cursors["ratings"] = ts_to_iso(int(time.time()))
        _cursor_store_save(cursors)

        # Update shadow (best-effort)
        sh = _ratings_shadow_load()
        m = dict(sh.get("items") or {})
        for it in items or []:
            typ = (it.get("type") or "movie").lower()
            node = {"ids": it.get("ids") or {}, "title": it.get("title"), "year": it.get("year")}
            key = canonical_key(typ, node)
            row = m.get(key)
            if row:
                row.pop("rating", None)
                m[key] = row
        _ratings_shadow_save(m)

        return sum(len(v) for v in payload.values())
    return 0


def _history_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    """
    Add history entries (scrobbles) to Trakt.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    now_iso = ts_to_iso(int(time.time()))
    for it in items or []:
        ids = dict(it.get("ids") or {})
        entry = {"watched_at": it.get("watched_at") or now_iso, "ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if (it.get("type") or "movie") == "movie":
            movies.append(entry)
        else:
            shows.append(entry)

    payload = {}
    if movies: payload["movies"] = movies
    if shows:  payload["episodes"] = shows   # Trakt expects episodes under "episodes"
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
    """
    Remove history entries from Trakt.
    """
    trakt_cfg = dict(cfg_root.get("trakt") or cfg_root.get("TRAKT") or {})
    hdr = trakt_headers(trakt_cfg)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items or []:
        ids = dict(it.get("ids") or {})
        entry = {"ids": {k: ids.get(k) for k in _ID_KEYS if ids.get(k)}}
        if not entry["ids"]:
            continue
        if (it.get("type") or "movie") == "movie":
            movies.append(entry)
        else:
            shows.append(entry)

    payload = {}
    if movies: payload["movies"] = movies
    if shows:  payload["episodes"] = shows
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
    def name(self) -> str: return "TRAKT"
    def label(self) -> str: return "Trakt"
    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": False}
    def capabilities(self) -> Mapping[str, Any]:
        return {"bidirectional": True}

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
            return {"ok": False, "count": 0, "error": "Trakt playlists API not supported"}
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
            return {"ok": False, "count": 0, "error": "Trakt playlists API not supported"}
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


class TRAKTModule(SyncModule):  # type: ignore[misc]
    info = ModuleInfo(
        name="TRAKT",
        version=__VERSION__,
        description="Trakt connector with activities guards, ETag caching under /config, and low-call indexing.",
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

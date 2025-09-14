from __future__ import annotations

__VERSION__ = "1.1.1"
__all__ = ["OPS", "SIMKLModule", "get_manifest"]

import json
import time
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol

import requests

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


# ---------------------------------------------------------------------------
# Utilities
# ---------------------------------------------------------------------------

def iso_to_ts(s: str) -> int:
    try:
        import datetime as dt
        return int(dt.datetime.strptime(s.replace("Z", "+0000"), "%Y-%m-%dT%H:%M:%S%z").timestamp())
    except Exception:
        return 0


def ts_to_iso(ts: int) -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(ts))


def _record_http_simkl(r: Optional[requests.Response], *, endpoint: str, method: str, payload: Any = None) -> None:
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
        _stats.record_http(
            provider="SIMKL",
            endpoint=endpoint,
            method=method,
            status=status,
            ok=ok,
            bytes_in=bytes_in,
            bytes_out=bytes_out,
            ms=ms,
            rate_remaining=None,
            rate_reset_iso=None,
        )
    except Exception:
        pass


def _simkl_get(url: str, *, headers: Mapping[str, str], params: Optional[dict] = None, timeout: int = 45) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
    except Exception:
        _record_http_simkl(None, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
        return None
    _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="GET")
    return r


def _simkl_post(url: str, *, headers: Mapping[str, str], json_payload: Mapping[str, Any], timeout: int = 45) -> Optional[requests.Response]:
    try:
        r = requests.post(url, headers=headers, json=json_payload, timeout=timeout)
    except Exception:
        _record_http_simkl(None, endpoint=url.replace(SIMKL_BASE, ""), method="POST", payload=json_payload)
        return None
    _record_http_simkl(r, endpoint=url.replace(SIMKL_BASE, ""), method="POST", payload=json_payload)
    return r


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
    def __init__(self, root_cfg: Mapping[str, Any]):
        runtime = dict(root_cfg.get("runtime") or {})
        base_dir = runtime.get("state_dir") or os.environ.get("CROSSWATCH_STATE_DIR") or str(Path.home() / ".crosswatch")
        try:
            p = Path(base_dir); p.mkdir(parents=True, exist_ok=True)
        except Exception:
            p = Path.cwd()
        self.file = p / "simkl_cursors.json"
        self.shadow_file = p / "simkl_watchlist.shadow.json"

    def _read(self) -> Dict[str, Any]:
        try:
            return json.loads(self.file.read_text("utf-8"))
        except Exception:
            return {}

    def _write(self, data: Mapping[str, Any]) -> None:
        tmp = self.file.with_suffix(self.file.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        tmp.replace(self.file)

    def get(self, feature: str) -> Optional[str]:
        return self._read().get(feature)

    def set(self, feature: str, iso_ts: str) -> None:
        data = self._read()
        data[feature] = iso_ts
        self._write(data)

    def load_shadow(self) -> Dict[str, Any]:
        try:
            return json.loads(self.shadow_file.read_text("utf-8"))
        except Exception:
            return {"items": {}, "last_sync_ts": 0}

    def save_shadow(self, items: Mapping[str, Any], last_sync_ts: int) -> None:
        data = {"items": items, "last_sync_ts": int(last_sync_ts)}
        tmp = self.shadow_file.with_suffix(self.shadow_file.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False), "utf-8")
        tmp.replace(self.shadow_file)


# ---------------------------------------------------------------------------
# Helpers for IDs and shapes
# ---------------------------------------------------------------------------

def simkl_headers(simkl_cfg: Mapping[str, Any]) -> Dict[str, str]:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {simkl_cfg.get('access_token','')}",
        "simkl-api-key": simkl_cfg.get("client_id",""),
    }


def canonical_key(item: Mapping[str, Any]) -> str:
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
    return {
        "ids": {k: item.get("ids", {}).get(k) for k in _ID_KEYS if item.get("ids", {}).get(k)},
        "title": item.get("title"),
        "year": item.get("year"),
        "type": (item.get("type") or "").lower() or None,
    }


def _read_as_list(j: Any, key: str) -> List[dict]:
    if isinstance(j, list):
        return j
    if isinstance(j, dict):
        arr = j.get(key)
        if isinstance(arr, list):
            return arr
    return []


def _ids_from_simkl_node(node: Mapping[str, Any]) -> Dict[str, Any]:
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

def _activities(cfg_root: Mapping[str, Any]) -> Dict[str, Any]:
    now = time.time()
    if (now - float(_RUN_ACT.get("ts") or 0)) < 60 and (_RUN_ACT.get("data") is not None):
        return dict(_RUN_ACT["data"])
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    try:
        r = _simkl_get(SIMKL_SYNC_ACTIVITIES, headers=simkl_headers(simkl_cfg), timeout=30)
        if not r or not r.ok:
            return {}
        data = r.json() or {}
    except Exception:
        return {}
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
# PTW: delta-only fetch + local shadow => full snapshot
# ---------------------------------------------------------------------------

def _ptw_fetch_delta(hdr: Mapping[str, str], date_from_iso: str) -> Dict[str, List[dict]]:
    params = {"extended": "full", "episode_watched_at": "yes", "memos": "yes", "date_from": date_from_iso}

    def _get(url: str, key: str) -> List[dict]:
        try:
            r = _simkl_get(url, headers=hdr, params=params, timeout=45)
            if not r or not r.ok:
                return []
            j = r.json() or {}
            if isinstance(j, list):
                return j
            if isinstance(j, dict):
                arr = j.get(key)
                return arr if isinstance(arr, list) else []
            return []
        except Exception:
            return []

    movies = _get(SIMKL_ALL_ITEMS_MOVIES_PTW, "movies")
    shows  = _get(SIMKL_ALL_ITEMS_SHOWS_PTW,  "shows")
    return {"movies": movies, "shows": shows}


def _ptw_flatten_item(kind: str, it: Mapping[str, Any]) -> Dict[str, Any]:
    node = (it.get("movie") if kind == "movie" else it.get("show")) or {}
    return {"type": kind, "title": node.get("title"), "year": node.get("year"), "ids": _ids_from_simkl_node(node)}


def _ptw_key_for(kind: str, node: Mapping[str, Any]) -> str:
    ids = node.get("ids") or {}
    for k in _ID_KEYS:
        v = ids.get(k)
        if v:
            return f"{k}:{v}".lower()
    t = (node.get("title") or "").strip().lower()
    y = node.get("year") or ""
    return f"{kind}|title:{t}|year:{y}"


def _ptw_apply_delta(shadow: Dict[str, Any], delta: Dict[str, List[dict]]) -> Dict[str, Any]:
    items = dict(shadow)
    for kind, arr in (delta or {}).items():
        k = "movie" if kind == "movies" else "show"
        for it in arr or []:
            node = _ptw_flatten_item(k, it)
            key = _ptw_key_for(k, node)
            removed = bool(it.get("removed") or it.get("action") in ("remove", "deleted", "unlisted"))
            if removed:
                items.pop(key, None)
            else:
                items[key] = node
    return items


def _ptw_bootstrap_using_windows(hdr: Mapping[str, str], start_iso: str, window_days: int = 120, max_windows: int = 40) -> Dict[str, Any]:
    cursor_ts = iso_to_ts(start_iso) if start_iso else int(time.time())
    items: Dict[str, Any] = {}
    empty_streak = 0
    for _ in range(max_windows):
        since_ts = cursor_ts - window_days * 86400
        delta = _ptw_fetch_delta(hdr, ts_to_iso(since_ts))
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


# ---------------------------------------------------------------------------
# Watchlist (PTW)
# ---------------------------------------------------------------------------

def _watchlist_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)
    acts = _activities(cfg_root)

    # Required cursor for SIMKL; prefer persisted or activities
    cursor = store.get("watchlist") or simkl_cfg.get("last_date") or simkl_cfg.get("date_from")
    date_from = cursor or acts.get("watchlist") or acts.get("lists") or ts_to_iso(int(time.time()))

    # Load shadow; build baseline if empty (using date_from windows only)
    shadow = store.load_shadow()
    items = dict(shadow.get("items") or {})
    if not items:
        items = _ptw_bootstrap_using_windows(hdr, date_from)
        store.save_shadow(items, int(time.time()))

    # Apply this run's delta
    delta = _ptw_fetch_delta(hdr, date_from)
    items = _ptw_apply_delta(items, delta)

    # Tiny fallback: if activities advanced but delta was empty, sweep a small window
    try:
        act_ts = iso_to_ts(str(acts.get("watchlist") or acts.get("lists") or ""))
        cur_ts = iso_to_ts(str(date_from))
        delta_size = (len(delta.get("movies", [])) + len(delta.get("shows", [])))
        if delta_size == 0 and act_ts and cur_ts and act_ts > cur_ts:
            tiny_since = ts_to_iso(int(time.time()) - 7 * 86400)
            tiny = _ptw_fetch_delta(hdr, tiny_since)
            items = _ptw_apply_delta(items, tiny)
    except Exception:
        pass

    # Advance cursor using server perspective if available
    try:
        acts2 = _activities(cfg_root)
    except Exception:
        acts2 = {}
    new_cursor = acts2.get("watchlist") or acts2.get("lists") or ts_to_iso(int(time.time()))
    try:
        store.set("watchlist", new_cursor)
    except Exception:
        pass
    store.save_shadow(items, int(time.time()))

    # Full snapshot for orchestrator
    out: Dict[str, Dict[str, Any]] = {}
    for k, node in items.items():
        out[k] = {"type": node.get("type"), "title": node.get("title"), "year": node.get("year"), "ids": node.get("ids") or {}}
    return out


def _watchlist_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    items_list = list(items)
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
    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    items_list = list(items)
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
        return sum(len(v) for v in payload.values())
    return 0


# ---------------------------------------------------------------------------
# Ratings
# ---------------------------------------------------------------------------

def _ratings_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)

    params: Dict[str, Any] = {"extended": "full"}
    df = store.get("ratings") or simkl_cfg.get("date_from")
    if df:
        params["date_from"] = df

    try:
        r = _simkl_get(SIMKL_RATINGS_GET, headers=hdr, params=params, timeout=45)
        if not r or not r.ok:
            return {}
        data = r.json() or {}
    except Exception:
        return {}

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

    try:
        srv_now = r.headers.get("Date")
    except Exception:
        srv_now = None
    new_cursor = srv_now or ts_to_iso(int(time.time()))
    try:
        store.set("ratings", new_cursor)
    except Exception:
        pass

    return idx


def _ratings_set(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr = simkl_headers(simkl_cfg)

    items_list = list(items)
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

    return total


# ---------------------------------------------------------------------------
# History
# ---------------------------------------------------------------------------

def _history_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    simkl_cfg = dict(cfg_root.get("simkl") or {})
    hdr   = simkl_headers(simkl_cfg)
    store = _CursorStore(cfg_root)
    acts  = _activities(cfg_root)

    params: Dict[str, Any] = {"extended": "full"}
    cursor    = store.get("history") or simkl_cfg.get("last_date") or simkl_cfg.get("date_from")
    date_from = cursor or acts.get("history") or acts.get("completed")
    if date_from:
        params["date_from"] = date_from

    try:
        r = _simkl_get(SIMKL_HISTORY_ADD, headers=hdr, params=params, timeout=45)
        if not r or not r.ok:
            return {}
        data = r.json() or {}
    except Exception:
        return {}

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
    return idx


def _history_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    now_iso = ts_to_iso(int(time.time()))
    for it in items:
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
    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items:
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
        description="Reads and writes SIMKL watchlist (PTW), ratings, and history.",
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
                        },
                        "required": ["client_id", "access_token"],
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

from __future__ import annotations
# providers/sync/_mod_TRAKT.py
# Unified OPS provider for Trakt: watchlist, ratings, history, playlists (custom lists)

__VERSION__ = "1.1.2"

import json
import time
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Tuple

import requests

# ---- Public provider OPS protocol (for clarity) -----------------------------
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...

# ---- Trakt constants ---------------------------------------------------------
UA = "CrossWatch/Module"
TRAKT_BASE = "https://api.trakt.tv"
API_VERSION = "2"

# Activities
TRAKT_LAST_ACTIVITIES = f"{TRAKT_BASE}/sync/last_activities"

# Watchlist
TRAKT_SYNC_WATCHLIST        = f"{TRAKT_BASE}/sync/watchlist"
TRAKT_SYNC_WATCHLIST_REMOVE = f"{TRAKT_BASE}/sync/watchlist/remove"
TRAKT_USERS_WATCHLIST_MOV   = f"{TRAKT_BASE}/users/me/watchlist/movies"
TRAKT_USERS_WATCHLIST_SHOW  = f"{TRAKT_BASE}/users/me/watchlist/shows"

# Ratings
TRAKT_SYNC_RATINGS          = f"{TRAKT_BASE}/sync/ratings"
TRAKT_SYNC_RATINGS_REMOVE   = f"{TRAKT_BASE}/sync/ratings/remove"

# History
TRAKT_SYNC_HISTORY          = f"{TRAKT_BASE}/sync/history"
TRAKT_SYNC_HISTORY_GET_MOV  = f"{TRAKT_BASE}/sync/history/movies"
TRAKT_SYNC_HISTORY_GET_EP   = f"{TRAKT_BASE}/sync/history/episodes"

# Lists (playlists)
TRAKT_USERS_LISTS           = f"{TRAKT_BASE}/users/me/lists"
TRAKT_USERS_LIST_ITEMS      = lambda slug: f"{TRAKT_BASE}/users/me/lists/{slug}/items"
TRAKT_USERS_LIST_ITEMS_ADD  = lambda slug: f"{TRAKT_BASE}/users/me/lists/{slug}/items"
TRAKT_USERS_LIST_ITEMS_RM   = lambda slug: f"{TRAKT_BASE}/users/me/lists/{slug}/items/remove"

# ---- Optional statistics hook ------------------------------------------------
try:
    from _statistics import Stats  # type: ignore
    _stats = Stats()
except Exception:
    _stats = None

def _record_http_trakt(r: Optional[requests.Response], *, endpoint: str, method: str, payload: Any = None) -> None:
    """Best-effort telemetry to _statistics; safe if Stats is absent."""
    if not _stats:
        return
    try:
        status = int(getattr(r, "status_code", 0) or 0)
        ok     = bool(getattr(r, "ok", False))
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

        # Rate-limit headers (Trakt v2)
        rem: Optional[int] = _int_header(r, "X-RateLimit-Remaining")
        reset_hdr = r.headers.get("X-RateLimit-Reset") if r is not None else None
        reset_iso: Optional[str] = str(reset_hdr) if reset_hdr is not None else None
        if r is not None:
            try:
                rem = int(r.headers.get("X-RateLimit-Remaining")) if r.headers.get("X-RateLimit-Remaining") else None
            except Exception:
                rem = None
            reset_hdr = r.headers.get("X-RateLimit-Reset")
            reset_iso = str(reset_hdr) if reset_hdr else None

        _stats.record_http(
            provider="TRAKT",
            endpoint=endpoint,
            method=method,
            status=status,
            ok=ok,
            bytes_in=bytes_in,
            bytes_out=bytes_out,
            ms=ms,
            rate_remaining=rem,
            rate_reset_iso=reset_iso,
        )
    except Exception:
        pass
    
def _int_header(r: Optional[requests.Response], name: str) -> Optional[int]:
    if r is None:
        return None
    try:
        raw: Any = r.headers.get(name)
        if raw is None:
            return None
        if isinstance(raw, int):
            return raw
        s = str(raw).strip()
        # strikt numeriek; Trakt zet soms epoch in string
        return int(s) if s.isdigit() else None
    except Exception:
        return None


def _trakt_get(url: str, *, headers: Mapping[str, str], params: Optional[dict] = None, timeout: int = 45) -> Optional[requests.Response]:
    try:
        r = requests.get(url, headers=headers, params=params or {}, timeout=timeout)
    except Exception:
        _record_http_trakt(None, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
        return None
    _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="GET")
    return r

def _trakt_post(url: str, *, headers: Mapping[str, str], json_payload: Mapping[str, Any], timeout: int = 45) -> Optional[requests.Response]:
    try:
        r = requests.post(url, headers=headers, json=json_payload, timeout=timeout)
    except Exception:
        _record_http_trakt(None, endpoint=url.replace(TRAKT_BASE, ""), method="POST", payload=json_payload)
        return None
    _record_http_trakt(r, endpoint=url.replace(TRAKT_BASE, ""), method="POST", payload=json_payload)
    return r

# ---- Global Tombstones (optional) -------------------------------------------
# --------------- Optional Global Tombstones integration: feature-scoped suppression + negative records ---------------
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

# ---- Helpers -----------------------------------------------------------------
_ID_KEYS = ("trakt", "imdb", "tmdb", "tvdb", "slug")

def _headers(root_cfg: Mapping[str, Any]) -> Dict[str, str]:
    trakt_cfg = dict(root_cfg.get("trakt") or {})
    auth_cfg = dict(root_cfg.get("auth", {})).get("trakt", {})
    token = auth_cfg.get("access_token") or trakt_cfg.get("access_token")
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": UA,
        "trakt-api-version": API_VERSION,
        "trakt-api-key": trakt_cfg.get("client_id", ""),
        "Authorization": f"Bearer {token}" if token else "",
    }

class _CursorStore:
    """Lightweight cursor storage for date_from/start_at per feature (state_dir or ~/.crosswatch)."""
    def __init__(self, root_cfg: Mapping[str, Any]):
        runtime = dict(root_cfg.get("runtime") or {})
        base_dir = runtime.get("state_dir") or os.environ.get("CROSSWATCH_STATE_DIR") or str(Path.home() / ".crosswatch")
        try:
            p = Path(base_dir); p.mkdir(parents=True, exist_ok=True)
        except Exception:
            p = Path.cwd()
        self.file = p / "trakt_cursors.json"

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
        data = self._read(); data[feature] = iso_ts; self._write(data)

def _ids_from_trakt_node(node: Mapping[str, Any]) -> Dict[str, Any]:
    ids = dict((node.get("ids") or {}))
    out: Dict[str, Any] = {}
    for k in _ID_KEYS:
        v = ids.get(k)
        if v is not None:
            out[k] = v
    return out

# ---- Activities (per-run cache ~60s) ----------------------------------------
_RUN_ACT: Dict[str, Any] = {"ts": 0.0, "data": None}

def _activities(root_cfg: Mapping[str, Any]) -> Dict[str, Any]:
    now = time.time()
    cached_ts = float(_RUN_ACT.get("ts") or 0.0)
    cached = _RUN_ACT.get("data")
    if (now - cached_ts) < 60 and isinstance(cached, dict):
        return dict(cached)

    try:
        r = _trakt_get(TRAKT_LAST_ACTIVITIES, headers=_headers(root_cfg), timeout=30)
        if not r or not r.ok:
            return {}
        data = r.json() or {}
    except Exception:
        return {}

    out: Dict[str, Any] = {}
    if isinstance(data.get("watchlist"), dict) and data["watchlist"].get("updated_at"):
        out["watchlist"] = data["watchlist"]["updated_at"]
    if isinstance(data.get("lists"), dict) and data["lists"].get("updated_at"):
        out["lists"] = data["lists"]["updated_at"]
    if isinstance(data.get("ratings"), dict) and data["ratings"].get("updated_at"):
        out["ratings"] = data["ratings"]["updated_at"]
    # Optional hints for history
    for k in ("movies", "episodes", "shows"):
        sec = data.get(k)
        if isinstance(sec, dict) and sec.get("watched_at"):
            out[f"history.{k}"] = sec["watched_at"]

    _RUN_ACT["ts"] = now
    _RUN_ACT["data"] = dict(out)
    return out

# ---- Watchlist ---------------------------------------------------------------
def _watchlist_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    hdr = _headers(cfg_root)
    params = {"extended": "full"}

    def _get(url: str) -> List[dict]:
        try:
            r = _trakt_get(url, headers=hdr, params=params, timeout=45)
            if not r or not r.ok:
                return []
            data = r.json() or []
            return data if isinstance(data, list) else []
        except Exception:
            return []

    movies = _get(TRAKT_USERS_WATCHLIST_MOV)
    shows  = _get(TRAKT_USERS_WATCHLIST_SHOW)

    idx: Dict[str, Dict[str, Any]] = {}
    for kind, arr in (("movie", movies), ("show", shows)):
        for it in arr:
            node = it.get(kind) or {}
            ids = _ids_from_trakt_node(node)
            key = None
            for k in ("trakt", "imdb", "tmdb", "tvdb"):
                if ids.get(k):
                    key = f"{k}:{ids[k]}".lower(); break
            if not key:
                t = str(node.get("title") or "").strip().lower(); y = node.get("year") or ""
                key = f"{kind}|title:{t}|year:{y}"
            idx[key] = {"type": kind, "title": node.get("title"), "year": node.get("year"), "ids": ids}
    return idx

def _watchlist_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    # --------------- GMT suppression for watchlist adds ---------------
    items_list = list(items)
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and items_list:
        op_add, _op_neg = _gmt_ops_for_feature("watchlist")
        items_list = [it for it in items_list if not _gmt_suppress(store=store_gmt, item=it, feature="watchlist", write_op=op_add)]

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k)}}
        if entry["ids"]:
            payload["movies" if kind == "movie" else "shows"].append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_SYNC_WATCHLIST, headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("watchlist", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        return sum(len(v) for v in payload.values())
    return 0

def _watchlist_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    items_list = list(items)

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k)}}
        if entry["ids"]:
            payload["movies" if kind == "movie" else "shows"].append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_SYNC_WATCHLIST_REMOVE, headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("watchlist", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        # --------------- GMT negative record for removals ---------------
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="watchlist", op="remove", origin="TRAKT")
        return sum(len(v) for v in payload.values())
    return 0

# ---- Ratings -----------------------------------------------------------------
def _ratings_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    hdr = _headers(cfg_root)
    params = {"extended": "full"}

    def _get(url: str) -> List[dict]:
        try:
            r = _trakt_get(url, headers=hdr, params=params, timeout=45)
            if not r or not r.ok:
                return []
            data = r.json() or []
            return data if isinstance(data, list) else []
        except Exception:
            return []

    movies = _get(f"{TRAKT_SYNC_RATINGS}/movies")
    shows  = _get(f"{TRAKT_SYNC_RATINGS}/shows")

    idx: Dict[str, Dict[str, Any]] = {}
    for kind, arr in (("movie", movies), ("show", shows)):
        for it in arr:
            node = it.get(kind) or {}
            ids = _ids_from_trakt_node(node)
            rating = it.get("rating")
            key = None
            for k in ("trakt", "imdb", "tmdb", "tvdb"):
                if ids.get(k):
                    key = f"{k}:{ids[k]}".lower(); break
            if not key:
                t = str(node.get("title") or "").strip().lower(); y = node.get("year") or ""
                key = f"{kind}|title:{t}|year:{y}"
            idx[key] = {"type": kind, "title": node.get("title"), "year": node.get("year"), "ids": ids, "rating": rating}
    return idx

def _ratings_set(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    # --------------- GMT suppression for rating sets ---------------
    items_list = list(items)
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and items_list:
        op_add, _op_neg = _gmt_ops_for_feature("ratings")
        items_list = [it for it in items_list if not _gmt_suppress(store=store_gmt, item=it, feature="ratings", write_op=op_add)]

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    def _coerce(v):
        try:
            n = int(round(float(v)))
            return min(10, max(1, n))
        except Exception:
            return None

    for it in items_list:
        ids = dict((it.get("ids") or {}))
        rating = _coerce(it.get("rating"))
        if rating is None:
            continue
        entry_ids = {k: ids.get(k) for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k)}
        if not entry_ids:
            continue

        kind = (it.get("type") or "movie").lower()
        entry = {
            "rating": rating,
            "rated_at": it.get("rated_at") or now_iso,
            "ids": entry_ids,
        }
        (payload["movies"] if kind == "movie" else payload["shows"]).append(entry)

    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0

    r = _trakt_post(TRAKT_SYNC_RATINGS, headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("ratings", now_iso)
        return sum(len(v) for v in payload.values())
    return 0


def _ratings_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    items_list = list(items)

    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}}
        if entry["ids"]:
            payload["movies" if kind == "movie" else "shows"].append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_SYNC_RATINGS_REMOVE, headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("ratings", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        # --------------- GMT negative record for rating removals ---------------
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="ratings", op="unrate", origin="TRAKT")
        return sum(len(v) for v in payload.values())
    return 0

# ---- History (watched / unwatch) --------------------------------------------
def _history_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    hdr = _headers(cfg_root)
    store = _CursorStore(cfg_root)
    acts = _activities(cfg_root)
    start_at = store.get("history") or acts.get("history") or acts.get("history.movies") or acts.get("history.episodes")

    params = {"extended": "full"}
    if start_at:
        params["start_at"] = start_at

    def _get(url: str) -> List[dict]:
        try:
            r = _trakt_get(url, headers=hdr, params=params, timeout=45)
            if not r or not r.ok:
                return []
            data = r.json() or []
            return data if isinstance(data, list) else []
        except Exception:
            return []

    movies   = _get(TRAKT_SYNC_HISTORY_GET_MOV)
    episodes = _get(TRAKT_SYNC_HISTORY_GET_EP)

    idx: Dict[str, Dict[str, Any]] = {}
    for it in movies:
        node = it.get("movie") or {}
        ids = _ids_from_trakt_node(node)
        key = None
        for k in ("trakt","imdb","tmdb","tvdb"):
            if ids.get(k):
                key = f"{k}:{ids[k]}".lower(); break
        if not key:
            t = str(node.get("title") or "").strip().lower(); y = node.get("year") or ""
            key = f"movie|title:{t}|year:{y}"
        idx[key] = {"type": "movie", "title": node.get("title"), "year": node.get("year"), "ids": ids, "watched": True}

    # Roll up episode history to show level (best-effort)
    for it in episodes:
        show = (it.get("show") or {})
        sids = _ids_from_trakt_node(show)
        skey = None
        for k in ("trakt","imdb","tmdb","tvdb"):
            if sids.get(k):
                skey = f"{k}:{sids[k]}".lower(); break
        if skey and skey not in idx:
            idx[skey] = {"type": "show", "title": show.get("title"), "year": show.get("year"), "ids": sids, "watched": True}

    return idx

def _history_add(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    # --------------- GMT suppression for scrobbles ---------------
    items_list = list(items)
    store_gmt = _gmt_store_from_cfg(cfg_root)
    if store_gmt and items_list:
        op_add, _op_neg = _gmt_ops_for_feature("history")
        items_list = [it for it in items_list if not _gmt_suppress(store=store_gmt, item=it, feature="history", write_op=op_add)]

    now_iso = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or "movie").lower()
        if kind == "movie":
            movies.append({"watched_at": it.get("watched_at") or now_iso,
                           "ids": {k: ids.get(k) for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}})
        else:
            shows.append({"watched_at": it.get("watched_at") or now_iso,
                          "ids": {k: ids.get(k) for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}})
    payload = {k: v for k, v in {"movies": movies, "shows": shows}.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_SYNC_HISTORY, headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("history", now_iso)
        return sum(len(v) for v in payload.values())
    return 0

def _history_remove(cfg_root: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> int:
    items_list = list(items)

    movies: List[Dict[str, Any]] = []
    shows:  List[Dict[str, Any]] = []
    for it in items_list:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}}
        if not entry["ids"]:
            continue
        (movies if kind == "movie" else shows).append(entry)
    payload = {k: v for k, v in {"movies": movies, "shows": shows}.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_SYNC_HISTORY + "/remove", headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("history", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        # --------------- GMT negative record for unscrobbles ---------------
        store_gmt = _gmt_store_from_cfg(cfg_root)
        if store_gmt:
            for it in items_list:
                _gmt_record(store=store_gmt, item=it, feature="history", op="unscrobble", origin="TRAKT")
        return sum(len(v) for v in payload.values())
    return 0

# ---- Playlists (custom lists) ------------------------------------------------
def _lists_index(cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    hdr = _headers(cfg_root)
    def _get(url: str, params: Optional[Dict[str, Any]] = None) -> Optional[Any]:
        try:
            r = _trakt_get(url, headers=hdr, params=params or {}, timeout=45)
            if not r or not r.ok:
                return None
            return r.json()
        except Exception:
            return None

    lists = _get(TRAKT_USERS_LISTS) or []
    idx: Dict[str, Dict[str, Any]] = {}
    for lst in lists:
        slug = (lst.get("ids") or {}).get("slug") or lst.get("slug")
        if not slug:
            continue
        title = lst.get("name") or lst.get("title")
        key = f"playlist:{slug}".lower()
        idx[key] = {"type": "playlist", "title": title, "ids": {"slug": slug}}
    return idx

def _list_add_items(cfg_root: Mapping[str, Any], slug: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None) -> int:
    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or mtype_hint or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}}
        if entry["ids"]:
            payload["movies" if kind == "movie" else "shows"].append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_USERS_LIST_ITEMS_ADD(slug), headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("playlists", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        return sum(len(v) for v in payload.values())
    return 0

def _list_remove_items(cfg_root: Mapping[str, Any], slug: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None) -> int:
    payload: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items:
        ids = dict((it.get("ids") or {}))
        kind = (it.get("type") or mtype_hint or "movie").lower()
        entry = {"ids": {k: ids.get(k) for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}}
        if entry["ids"]:
            payload["movies" if kind == "movie" else "shows"].append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        return 0
    r = _trakt_post(TRAKT_USERS_LIST_ITEMS_RM(slug), headers=_headers(cfg_root), json_payload=payload, timeout=45)
    if r and r.ok:
        _CursorStore(cfg_root).set("playlists", time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()))
        return sum(len(v) for v in payload.values())
    return 0

# ---- OPS implementation ------------------------------------------------------
class _TraktOPS:
    def name(self) -> str: return "TRAKT"
    def label(self) -> str: return "Trakt"
    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}
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
            return _lists_index(cfg)
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
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
            applied = 0
            for pl in items_list:
                slug = (pl.get("playlist") or pl.get("slug") or "").strip()
                if not slug:
                    continue
                applied += _list_add_items(cfg, slug, pl.get("items") or [], mtype_hint=pl.get("type"))
            return {"ok": True, "count": applied}
        return {"ok": True, "count": 0}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
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
            removed = 0
            for pl in items_list:
                slug = (pl.get("playlist") or pl.get("slug") or "").strip()
                if not slug:
                    continue
                removed += _list_remove_items(cfg, slug, pl.get("items") or [], mtype_hint=pl.get("type"))
            return {"ok": True, "count": removed}
        return {"ok": True, "count": 0}

OPS: InventoryOps = _TraktOPS()

# ---- Module manifest for /api/sync/providers --------------------------------
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
        config_schema: dict = None  # type: ignore
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
        description="Reads/writes Trakt watchlist, ratings, history, and custom lists via Trakt API v2.",
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
                            "client_secret": {"type": "string", "minLength": 1}
                        },
                        "required": ["client_id"]
                    },
                    "auth": {
                        "type": "object",
                        "properties": {
                            "trakt": {
                                "type": "object",
                                "properties": {
                                    "access_token": {"type": "string", "minLength": 1},
                                    "refresh_token": {"type": "string"},
                                    "expires_at": {"type": "integer"},
                                    "scope": {"type": "string"},
                                    "token_type": {"type": "string"}
                                },
                                "required": ["access_token"]
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
                        "properties": {
                            "state_dir": {"type": "string"}
                        }
                    }
                },
                "required": ["trakt", "auth"]
            },
        ),
    )

    @staticmethod
    def supported_features() -> dict:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

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

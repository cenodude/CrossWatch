from __future__ import annotations
# providers/sync/_mod_PLEX.py
# Unified OPS provider for Plex: watchlist, ratings, history, playlists

__VERSION__ = "2.1.0"

import re
import os
import json
import time
from pathlib import Path
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Protocol, Sequence, Tuple, cast

import requests

# ----- Optional root logger shim ------------------------------------------------
try:
    from _logging import log as host_log
except Exception:  # pragma: no cover
    def host_log(*a, **k):  # type: ignore
        pass

# ----- Statistics hook ---------------------------------------------------------
try:
    from _statistics import Stats  # type: ignore
    _stats = Stats()
except Exception:
    _stats = None

def _emit_rating_event(*, action: str, node: Mapping[str, Any], prev: Optional[int], value: Optional[int]) -> None:
    """Emit compact rating event for UI spotlight/summary."""
    try:
        payload = {
            "feature": "ratings",
            "action": action,
            "title": node.get("title"),
            "type": node.get("type"),
            "ids": dict(node.get("ids") or {}),
            "value": value,
            "prev": prev,
            "provider": "PLEX",
            "ts": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()))),
        }
        if _stats and hasattr(_stats, "record_event"):
            try: _stats.record_event(payload)
            except Exception: pass
        try: host_log("event", payload)
        except Exception: pass
    except Exception:
        pass  # never break writes due to telemetry

# ----- Global Tombstones (optional) --------------------------------------------
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
    if val is None: return _GMT_ENABLED_DEFAULT
    return bool(val)

def _gmt_ops_for_feature(feature: str) -> Tuple[str, str]:
    f = (feature or "").lower()
    if f == "ratings": return "rate", "unrate"
    if f == "history": return "scrobble", "unscrobble"
    return "add", "remove"

def _gmt_store_from_cfg(cfg: Mapping[str, Any]) -> Optional[GlobalTombstoneStore]:
    if not _HAS_GMT or not _gmt_is_enabled(cfg): return None
    try:
        ttl_days = int(((cfg.get("sync") or {}).get("gmt_quarantine_days") or (cfg.get("sync") or {}).get("tombstone_ttl_days") or 7))
        return GlobalTombstoneStore(ttl_sec=max(1, ttl_days) * 24 * 3600)
    except Exception:
        return None

# ----- Small state helpers (cache under /config/.cw_state) ---------------------
def _state_root() -> Path:
    base = Path("/config")
    try: base.mkdir(parents=True, exist_ok=True)
    except Exception: pass
    return base / ".cw_state"

def _shadow_path(name: str) -> Path: return _state_root() / name

def _read_json(path: Path, fallback: Any) -> Any:
    try: return json.loads(path.read_text("utf-8"))
    except Exception: return fallback

def _write_json(path: Path, data: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
        os.replace(tmp, path)
    except Exception:
        pass

def _ratings_shadow_load() -> Dict[str, Any]:
    return _read_json(_shadow_path("plex_ratings.shadow.json"), {"items": {}, "ts": 0})

def _ratings_shadow_save(items: Mapping[str, Any]) -> None:
    _write_json(_shadow_path("plex_ratings.shadow.json"), {"items": dict(items), "ts": int(time.time())})

def _history_shadow_load() -> Dict[str, Any]:
    return _read_json(_shadow_path("plex_history.shadow.json"), {"items": {}, "ts": 0})

def _history_shadow_save(items: Mapping[str, Any]) -> None:
    _write_json(_shadow_path("plex_history.shadow.json"), {"items": dict(items), "ts": int(time.time())})

def _cfg_get(cfg_root: Mapping[str, Any], path: str, default: Any) -> Any:
    """Simple dot-path config getter across root/sync/runtime."""
    def _dig(d: Mapping[str, Any], keys: List[str]) -> Any:
        cur: Any = d
        for k in keys:
            if not isinstance(cur, Mapping): return default
            cur = cur.get(k)
            if cur is None: return default
        return cur
    keys = path.split(".")
    for root_key in ("", "sync", "runtime"):
        base = cfg_root if root_key == "" else (cfg_root.get(root_key) or {})
        val = _dig(base, keys)
        if val is not None and val != default: return val
    return default

# ----- PlexAPI dependency ------------------------------------------------------
try:
    import plexapi  # type: ignore
    from plexapi.myplex import MyPlexAccount  # type: ignore
    HAS_PLEXAPI = True
except Exception:  # pragma: no cover
    HAS_PLEXAPI = False
    MyPlexAccount = object  # type: ignore

# ----- Provider protocol (for orchestrator discovery) --------------------------
class InventoryOps(Protocol):
    def name(self) -> str: ...
    def label(self) -> str: ...
    def features(self) -> Mapping[str, bool]: ...
    def capabilities(self) -> Mapping[str, Any]: ...
    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]: ...
    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...
    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]: ...

# ----- Constants / endpoints ---------------------------------------------------
UA = "CrossWatch/Module"
DISCOVER_HOST = "https://discover.provider.plex.tv"
PLEX_WATCHLIST_PATH = "/library/sections/watchlist/all"
PLEX_METADATA_PATH = "/library/metadata"

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|tvdb)://(\d+)", re.I)

# Canonical id preference (match JELLYFIN): imdb -> tmdb -> tvdb -> trakt -> plex -> guid
_ID_ORDER = ("imdb", "tmdb", "tvdb", "trakt", "plex", "guid")

# ----- Normalization helpers ---------------------------------------------------
def _norm_type(t: Any) -> str:
    x = (str(t or "")).strip().lower()
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series"): return "show"
    if x in ("seasons", "season"): return "season"
    if x in ("episodes", "episode"): return "episode"
    return x or "movie"

def _clamp_ts(ts: Any) -> Any:
    if not isinstance(ts, str) or not ts: return ts
    s = ts.strip()
    for sep in ("+", "-"):
        if sep in s[19:]:
            s = s[:s.find(sep)]
            break
    s = s[:-1] if s.endswith("Z") else s
    s = s.split(".", 1)[0]
    if len(s) >= 19: s = s[:19]
    return s + "Z"

def _norm_ids(ids: Any) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(ids, Mapping):
        for k in ("imdb", "tmdb", "tvdb", "trakt"):  # keep cross-provider ids only
            v = ids.get(k)
            if v is not None and str(v).strip():
                out[k] = v if k != "tmdb" and k != "tvdb" else int(v)
    return out

def _norm_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    r = dict(row or {})
    r["type"] = _norm_type(r.get("type"))
    r["ids"] = _norm_ids(r.get("ids") or {})
    if "watched_at" in r: r["watched_at"] = _clamp_ts(r.get("watched_at"))
    if "rated_at" in r:   r["rated_at"]   = _clamp_ts(r.get("rated_at"))
    return r

def _normalize_shadow_items(items: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for _, v in (items or {}).items():
        nr = _norm_row(v)
        out[canonical_key(nr)] = nr
    return out

# ----- Canonical/minimal helpers ----------------------------------------------
def _plural(t: str) -> str:
    x = (t or "").lower()
    if x.endswith("s"): return x
    if x == "movie": return "movies"
    if x == "show": return "shows"
    if x == "season": return "seasons"
    if x == "episode": return "episodes"
    return x or "movies"

def _libtype_from_item(item: Mapping[str, Any]) -> str:
    t = (item.get("type") or "").lower()
    if t in ("movie", "movies"): return "movie"
    if t in ("show", "shows"): return "show"
    if t in ("season", "seasons"): return "season"
    if t in ("episode", "episodes"): return "episode"
    return "movie"

def _singular(t: str) -> str:
    x = (t or "").lower()
    if x.startswith("movie"): return "movie"
    if x.startswith("show"): return "show"
    if x.startswith("season"): return "season"
    if x.startswith("episode"): return "episode"
    return "movie"

def canonical_key(item: Mapping[str, Any]) -> str:
    ids = item.get("ids") or {}
    for k in _ID_ORDER:
        v = ids.get(k)
        if v is not None and str(v) != "":
            return f"{k}:{v}".lower()
    title = (item.get("title") or "").strip().lower()
    year  = item.get("year") or ""
    typ   = _singular(item.get("type") or "")
    return f"{typ}|title:{title}|year:{year}"

def minimal(item: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "ids": {k: item.get("ids", {}).get(k) for k in _ID_ORDER if item.get("ids", {}).get(k)},
        "title": item.get("title"),
        "year": item.get("year"),
        "type": _singular(item.get("type") or "") or None,
    }

def _extract_ids_from_guid_strings(guid_values: List[str]) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    imdb = tmdb = tvdb = None
    for s in guid_values or []:
        s = str(s)
        m = _PAT_IMDB.search(s)
        if m and not imdb: imdb = m.group(1)
        m = _PAT_TMDB.search(s)
        if m and not tmdb:
            try: tmdb = int(m.group(1))
            except Exception: pass
        m = _PAT_TVDB.search(s)
        if m and not tvdb:
            try: tvdb = int(m.group(1))
            except Exception: pass
    return imdb, tmdb, tvdb

# ----- Discover helpers --------------------------------------------------------
def _plex_headers(token: str) -> dict:
    return {
        "X-Plex-Token": token,
        "Accept": "application/json",
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": __VERSION__,
        "X-Plex-Client-Identifier": "crosswatch",
        "X-Plex-Device": "python",
        "X-Plex-Device-Name": "crosswatch",
        "X-Plex-Platform": "python",
        "User-Agent": UA,
    }

def _discover_get(path: str, token: str, params: dict, timeout: int = 20) -> Optional[dict]:
    """GET wrapper for plex.tv discover with telemetry."""
    url = f"{DISCOVER_HOST}{path}"
    try:
        r = requests.get(url, headers=_plex_headers(token), params=params, timeout=timeout)
        if _stats:
            try:
                _stats.record_http(
                    provider="PLEX",
                    endpoint=path,
                    method="GET",
                    status=int(getattr(r, "status_code", 0) or 0),
                    ok=bool(getattr(r, "ok", False)),
                    bytes_in=len(getattr(r, "content", b"") or b""),
                    bytes_out=0,
                    ms=int(getattr(r, "elapsed", 0).total_seconds() * 1000) if getattr(r, "elapsed", None) else 0,
                )
            except Exception: pass
        if r.ok: return r.json()
    except Exception:
        if _stats:
            try:
                _stats.record_http(provider="PLEX", endpoint=path, method="GET", status=0, ok=False, bytes_in=0, bytes_out=0, ms=0)
            except Exception: pass
    return None

def _discover_metadata_by_ratingkey(token: str, rating_key: str) -> Optional[dict]:
    """Resolve extra GUIDs via plex.tv discover."""
    params = {"includeExternalMedia": "1"}
    data = _discover_get(f"{PLEX_METADATA_PATH}/{rating_key}", token, params, timeout=12)
    if not data: return None
    md = (data.get("MediaContainer", {}).get("Metadata") or [])
    if md: return md[0]
    items = data.get("items") or []
    return items[0] if items else None

def _watchlist_fetch_via_discover(token: str, page_size: int = 100) -> List[Dict[str, Any]]:
    """Page watchlist using plex.tv discover; returns normalized items."""
    params_base = {"includeCollections": "1", "includeExternalMedia": "1"}
    start = 0
    items: List[dict] = []
    while True:
        params = dict(params_base)
        params["X-Plex-Container-Start"] = str(start)
        params["X-Plex-Container-Size"] = str(page_size)
        data = _discover_get(PLEX_WATCHLIST_PATH, token, params, timeout=20)
        if not data: break
        mc = (data.get("MediaContainer", {}) or {})
        md = mc.get("Metadata") or data.get("items") or []
        if not isinstance(md, list): md = []
        fetched = 0
        for it in md:
            fetched += 1
            title = it.get("title") or it.get("name")
            rating_key = str(it.get("ratingKey") or "") or ""
            mtype_raw = it.get("type") or it.get("metadataType")
            mtype = "show" if (isinstance(mtype_raw, str) and mtype_raw.startswith("show")) or mtype_raw == 2 else "movie"
            guid_values: List[str] = []
            if isinstance(it.get("guid"), str): guid_values.append(it["guid"])
            if isinstance(it.get("Guid"), list):
                for gg in it["Guid"]:
                    if isinstance(gg, dict) and "id" in gg: guid_values.append(gg["id"])
            imdb, tmdb, tvdb = _extract_ids_from_guid_strings(guid_values)
            if not any([imdb, tmdb, tvdb]) and rating_key:
                enriched = _discover_metadata_by_ratingkey(token, rating_key)
                if enriched:
                    e_guids: List[str] = []
                    if isinstance(enriched.get("Guid"), list):
                        for gg in enriched["Guid"]:
                            if isinstance(gg, dict) and "id" in gg: e_guids.append(gg["id"])
                    if isinstance(enriched.get("guid"), str): e_guids.append(enriched["guid"])
                    imdb, tmdb, tvdb = _extract_ids_from_guid_strings(e_guids)
            ids: Dict[str, Any] = {}
            if imdb: ids["imdb"] = imdb
            if tmdb is not None: ids["tmdb"] = tmdb
            if tvdb is not None: ids["tvdb"] = tvdb
            items.append({"type": mtype, "title": title, "ids": ids})
        if fetched < page_size: break
        start += fetched
    return items

# ----- PlexAPI environment (used for writes/servers) --------------------------
@dataclass
class PlexEnv:
    account: Optional[MyPlexAccount]
    servers: List[Any]  # plexapi.server.PlexServer

def _ensure_account(plex_cfg: Mapping[str, Any]) -> MyPlexAccount:
    """Require plexapi and a valid account token."""
    if not HAS_PLEXAPI: raise RuntimeError("plexapi is required")
    token = (plex_cfg.get("account_token") or "").strip()
    if not token: raise ValueError("plex.account_token is required")
    return MyPlexAccount(token=token)  # type: ignore

def _connect_servers(acct: MyPlexAccount, plex_cfg: Mapping[str, Any]) -> List[Any]:
    """Connect to configured servers (machine_ids allowlist), fallback to first reachable."""
    wanted_ids: List[str] = list(plex_cfg.get("servers", {}).get("machine_ids") or [])
    servers: List[Any] = []
    for res in acct.resources():
        if "server" not in (res.provides or ""): continue
        if wanted_ids and res.clientIdentifier not in wanted_ids: continue
        try: servers.append(res.connect(timeout=6))
        except Exception: continue
    if not servers:
        for res in acct.resources():
            if "server" in (res.provides or ""):
                try: servers.append(res.connect(timeout=6)); break
                except Exception: continue
    return servers

def _env_from_config(cfg: Mapping[str, Any]) -> PlexEnv:
    plex_cfg = dict(cfg.get("plex") or {})
    acct = _ensure_account(plex_cfg)
    servers = _connect_servers(acct, plex_cfg)
    return PlexEnv(account=acct, servers=servers)

# ----- Server/object helpers ---------------------------------------------------
def _ids_from_plexobj(obj: Any) -> Dict[str, Any]:
    """Extract imdb/tmdb/tvdb ids from Plex object GUIDs."""
    ids: Dict[str, Any] = {}
    try:
        for g in (getattr(obj, "guids", []) or []):
            gid = getattr(g, "id", None)
            if isinstance(gid, str):
                if "imdb" in gid and "imdb" not in ids:
                    m = _PAT_IMDB.search(gid);  ids["imdb"] = m.group(1) if m else ids.get("imdb")
                if "tmdb" in gid and "tmdb" not in ids:
                    m = _PAT_TMDB.search(gid);  ids["tmdb"] = int(m.group(1)) if m else ids.get("tmdb")
                if "thetvdb" in gid and "tvdb" not in ids:
                    m = _PAT_TVDB.search(gid);  ids["tvdb"] = int(m.group(1)) if m else ids.get("tvdb")
    except Exception: pass
    try:
        gsingle = getattr(obj, "guid", None)
        if isinstance(gsingle, str):
            if "imdb" in gsingle and "imdb" not in ids:
                m = _PAT_IMDB.search(gsingle);  ids["imdb"] = m.group(1) if m else ids.get("imdb")
            if "tmdb" in gsingle and "tmdb" not in ids:
                m = _PAT_TMDB.search(gsingle);  ids["tmdb"] = int(m.group(1)) if m else ids.get("tmdb")
            if "thetvdb" in gsingle and "tvdb" not in ids:
                m = _PAT_TVDB.search(gsingle);  ids["tvdb"] = int(m.group(1)) if m else ids.get("tvdb")
    except Exception: pass
    return {k: v for k, v in ids.items() if v is not None}

def _server_find_item(server: Any, q: Mapping[str, Any], libtype: str) -> Optional[Any]:
    """Try guid-first then title/year search."""
    ids = (q.get("ids") or q) or {}
    for key in ("imdb", "tmdb", "tvdb"):
        val = ids.get(key)
        if not val: continue
        variants = [f"{key}://{val}"]
        if key == "imdb": variants.append(f"com.plexapp.agents.imdb://{val}")
        elif key == "tmdb": variants.append(f"com.plexapp.agents.tmdb://{val}")
        elif key == "tvdb": variants.append(f"com.plexapp.agents.thetvdb://{val}")
        for g in variants:
            try:
                hits = server.search(guid=g, libtype=libtype) or []
                if hits: return hits[0]
            except Exception: pass
    title = q.get("title"); year = q.get("year")
    try:
        hits = server.search(title=title, year=year, libtype=libtype) or []
        if hits: return hits[0]
    except Exception: pass
    return None

def _resolve_on_servers(env: PlexEnv, q: Mapping[str, Any], mtype: str) -> Optional[Any]:
    """Search across connected servers."""
    for s in env.servers:
        it = _server_find_item(s, q, mtype)
        if it: return it
    return None

# ----- Section filtering -------------------------------------------------------
def _section_filters(plex_cfg: Mapping[str, Any]) -> Tuple[set, set]:
    """Build include/exclude match sets (title/key/uuid)."""
    sec_cfg = dict(plex_cfg.get("servers", {}).get("sections") or {})
    def _norm(v: Any) -> str: return str(v).strip().lower()
    include = {_norm(v) for v in (sec_cfg.get("include") or []) if str(v).strip()}
    exclude = {_norm(v) for v in (sec_cfg.get("exclude") or []) if str(v).strip()}
    return include, exclude

def _match_section(sec: Any, include: set, exclude: set) -> bool:
    """Match by title, key, uuid (case-insensitive)."""
    t = getattr(sec, "title", "") or ""
    k = getattr(sec, "key", "") or ""
    u = getattr(sec, "uuid", "") or ""
    keys = {str(t).strip().lower(), str(k).strip().lower(), str(u).strip().lower()}
    if include and not (keys & include): return False
    if exclude and (keys & exclude): return False
    return True

# ----- Watchlist (plex.tv; read via discover, write via account) --------------
def _resolve_discover_item(acct: MyPlexAccount, ids: dict, libtype: str) -> Optional[Any]:
    """Resolve a discover item by ids/title/year."""
    queries: List[str] = []
    if ids.get("imdb"): queries.append(ids["imdb"])
    if ids.get("tmdb"): queries.append(str(ids["tmdb"]))
    if ids.get("tvdb"): queries.append(str(ids["tvdb"]))
    if ids.get("title"): queries.append(ids["title"])
    queries = list(dict.fromkeys(queries))
    for q in queries:
        try: hits: Sequence[Any] = acct.searchDiscover(q, libtype=libtype) or []
        except Exception: hits = []
        for md in hits:
            md_ids = _ids_from_plexobj(md)
            if ids.get("imdb") and md_ids.get("imdb") == ids.get("imdb"): return md
            if ids.get("tmdb") and md_ids.get("tmdb") == ids.get("tmdb"): return md
            if ids.get("tvdb") and md_ids.get("tvdb") == ids.get("tvdb"): return md
            if ids.get("title") and ids.get("year"):
                try:
                    same_t = str(md.title).strip().lower() == str(ids["title"]).strip().lower()
                    same_y = int(getattr(md, "year", 0) or 0) == int(ids["year"])
                    if same_t and same_y: return md
                except Exception: pass
    return None

def _watchlist_add(acct: MyPlexAccount, items: Iterable[Mapping[str, Any]]) -> int:
    """Add items to Watchlist via discover."""
    added = 0
    for it in items:
        ids = dict(it.get("ids") or {})
        if "title" not in ids and it.get("title"): ids["title"] = it["title"]
        if "year"  not in ids and it.get("year"):  ids["year"]  = it["year"]
        libtype = "movie" if (it.get("type") or "movie") in ("movie", "movies") else "show"
        md = _resolve_discover_item(acct, ids, libtype)
        if not md: continue
        try:
            cast(Any, md).addToWatchlist(account=acct); added += 1
        except Exception as e:
            msg = str(e).lower()
            if "already on the watchlist" in msg or "409" in msg: added += 1
    return added

def _watchlist_remove(acct: MyPlexAccount, items: Iterable[Mapping[str, Any]]) -> int:
    """Remove items from Watchlist via discover."""
    removed = 0
    for it in items:
        ids = dict(it.get("ids") or {})
        if "title" not in ids and it.get("title"): ids["title"] = it["title"]
        if "year"  not in ids and it.get("year"):  ids["year"]  = it["year"]
        libtype = "movie" if (it.get("type") or "movie") in ("movie", "movies") else "show"
        md = _resolve_discover_item(acct, ids, libtype)
        if not md: continue
        try:
            cast(Any, md).removeFromWatchlist(account=acct); removed += 1
        except Exception as e:
            msg = str(e).lower()
            if "not on the watchlist" in msg or "404" in msg: removed += 1
    return removed

def _watchlist_index(token: str) -> Dict[str, Dict[str, Any]]:
    """Read Watchlist via discover using account token."""
    items = _watchlist_fetch_via_discover(token, page_size=100)
    idx: Dict[str, Dict[str, Any]] = {}
    for it in items:
        ids = it.get("ids") or {}
        for k in ("imdb", "tmdb", "tvdb"):
            if ids.get(k):
                row = {"type": "movie" if (it.get("type") or "movie") == "movie" else "show",
                       "title": it.get("title"),
                       "ids": {kk: ids.get(kk) for kk in ("imdb", "tmdb", "tvdb") if ids.get(kk)}}
                nr = _norm_row(row)
                idx[canonical_key(nr)] = nr
                break
    return idx

# ----- Ratings (server-side) ---------------------------------------------------
def _rating_row(obj: Any, kind: str) -> Optional[Dict[str, Any]]:
    """Normalize a Plex user rating (kind must be singular: movie/show/season/episode)."""
    try: ur = getattr(obj, "userRating", None)
    except Exception: ur = None
    if ur is None: return None
    ids = _ids_from_plexobj(obj)
    if not ids: return None
    row: Dict[str, Any] = {
        "type": _singular(kind),
        "title": getattr(obj, "title", None),
        "year": getattr(obj, "year", None),
        "ids": ids,
        "rating": int(round(float(ur))) if isinstance(ur, (int, float)) else None,
    }
    try:
        ra = getattr(obj, "lastRatedAt", None)
        if ra:
            try: row["rated_at"] = ra.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception: pass
    except Exception: pass
    return _norm_row(row)

def _ratings_index_full(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Build ratings index across filtered sections (singular types)."""
    plex_cfg = dict(cfg_root.get("plex") or {})
    include, exclude = _section_filters(plex_cfg)
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            sections = [sec for sec in s.library.sections() if getattr(sec, "type", "") in ("movie", "show")]
            sections = [sec for sec in sections if _match_section(sec, include, exclude)]
            for section in sections:
                if section.type == "movie":
                    try:
                        for mv in section.all():
                            row = _rating_row(mv, "movie")
                            if not row or row.get("rating") is None: continue
                            key = None
                            for k in ("imdb", "tmdb", "tvdb"):
                                if row["ids"].get(k): key = f"{k}:{row['ids'][k]}".lower(); break
                            if not key: key = canonical_key(row)
                            idx[key] = row
                    except Exception: continue
                if section.type == "show":
                    try:
                        for sh in section.all():
                            r_show = _rating_row(sh, "show")
                            if r_show and r_show.get("rating") is not None:
                                key = None
                                for k in ("imdb", "tmdb", "tvdb"):
                                    if r_show["ids"].get(k): key = f"{k}:{r_show['ids'][k]}".lower(); break
                                if not key: key = canonical_key(r_show)
                                idx[key] = r_show
                            try:
                                for sn in sh.seasons():
                                    r_season = _rating_row(sn, "season")
                                    if not r_season or r_season.get("rating") is None: continue
                                    key = None
                                    for k in ("imdb", "tmdb", "tvdb"):
                                        if r_season["ids"].get(k): key = f"{k}:{r_season['ids'][k]}".lower(); break
                                    if not key:
                                        sid = None
                                        for k in ("imdb", "tmdb", "tvdb"):
                                            if r_show and (r_show.get("ids") or {}).get(k): sid = f"{k}:{r_show['ids'][k]}"; break
                                        snum = getattr(sn, "index", None)
                                        key = f"{(sid or canonical_key(r_show or r_season))}#season:{snum}"
                                    idx[key] = r_season
                            except Exception: pass
                            try:
                                for ep in sh.episodes():
                                    r_ep = _rating_row(ep, "episode")
                                    if not r_ep or r_ep.get("rating") is None: continue
                                    key = None
                                    for k in ("imdb", "tmdb", "tvdb"):
                                        if r_ep["ids"].get(k): key = f"{k}:{r_ep['ids'][k]}".lower(); break
                                    if not key:
                                        sid = None
                                        for k in ("imdb", "tmdb", "tvdb"):
                                            if r_show and (r_show.get("ids") or {}).get(k): sid = f"{k}:{r_show['ids'][k]}"; break
                                        snum = getattr(ep, "seasonNumber", getattr(ep, "seasonNumberLocal", None))
                                        enum = getattr(ep, "index", None)
                                        key = f"{(sid or canonical_key(r_show or r_ep))}#s{str(snum).zfill(2)}e{str(enum).zfill(2)}"
                                    idx[key] = r_ep
                            except Exception: pass
                    except Exception: continue
        except Exception: continue
    return idx

def _ratings_index(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Ratings snapshot with TTL cache (normalized on read/write)."""
    ttl_min = int(_cfg_get(cfg_root, "ratings.cache.ttl_minutes", 0) or 0)
    ttl_sec = max(0, ttl_min * 60)
    shadow = _ratings_shadow_load()
    items = dict(shadow.get("items") or {}); ts = int(shadow.get("ts") or 0)
    now = int(time.time())
    if ttl_sec > 0 and ts > 0 and (now - ts) < ttl_sec and items:
        norm = _normalize_shadow_items(items)
        try: host_log("PLEX.ratings", {"cache": "hit", "age_sec": now - ts, "count": len(norm)})
        except Exception: pass
        return norm
    idx = _ratings_index_full(env, cfg_root)
    if idx:
        norm = _normalize_shadow_items(idx)
        _ratings_shadow_save(norm)
        try: host_log("PLEX.ratings", {"cache": "refresh", "count": len(norm)})
        except Exception: pass
        return norm
    norm = _normalize_shadow_items(items)
    try: host_log("PLEX.ratings", {"cache": "fallback_shadow", "count": len(norm)})
    except Exception: pass
    return norm

def _ratings_apply(env: PlexEnv, items: Iterable[Mapping[str, Any]]) -> int:
    """Upsert user ratings (normalized rows into shadow)."""
    updated = 0
    sh = _ratings_shadow_load()
    shadow_map: Dict[str, Any] = _normalize_shadow_items(dict(sh.get("items") or {}))
    for it in items:
        tr = it.get("rating")
        if tr is None: continue
        libtype = _libtype_from_item(it)
        obj = _resolve_on_servers(env, it, libtype)
        if not obj: continue
        try:
            obj.rate(float(int(tr))); updated += 1
        except Exception: continue
        node = _norm_row({
            "type": _singular(it.get("type") or "movie"),
            "title": it.get("title"),
            "year": it.get("year"),
            "ids": {k: (it.get("ids") or {}).get(k) for k in ("imdb", "tmdb", "tvdb") if (it.get("ids") or {}).get(k)},
        })
        key = None
        ids = node["ids"]
        for k in ("imdb", "tmdb", "tvdb"):
            if ids.get(k): key = f"{k}:{ids[k]}".lower(); break
        if not key: key = canonical_key(node)
        prev_val = None
        if key in shadow_map and isinstance(shadow_map[key], dict):
            pv = shadow_map[key].get("rating"); prev_val = int(pv) if isinstance(pv, int) else None
        new_val = int(tr)
        shadow_map[key] = {
            "type": node["type"], "title": node["title"], "year": node["year"], "ids": node["ids"],
            "rating": new_val, "rated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(int(time.time()))),
        }
        if prev_val is None: _emit_rating_event(action="rate", node=shadow_map[key], prev=None, value=new_val)
        elif prev_val != new_val: _emit_rating_event(action="update", node=shadow_map[key], prev=prev_val, value=new_val)
        else: _emit_rating_event(action="rate", node=shadow_map[key], prev=prev_val, value=new_val)
    if updated: _ratings_shadow_save(shadow_map)
    return updated

def _ratings_remove(env: PlexEnv, items: Iterable[Mapping[str, Any]]) -> int:
    """Clear user ratings (normalized rows into shadow)."""
    cleared = 0
    sh = _ratings_shadow_load()
    shadow_map: Dict[str, Any] = _normalize_shadow_items(dict(sh.get("items") or {}))
    for it in items:
        libtype = _libtype_from_item(it)
        obj = _resolve_on_servers(env, it, libtype)
        if not obj: continue
        try: obj.rate(None); cleared += 1
        except Exception: continue
        node = _norm_row({
            "type": _singular(it.get("type") or "movie"),
            "title": it.get("title"),
            "year": it.get("year"),
            "ids": {k: (it.get("ids") or {}).get(k) for k in ("imdb", "tmdb", "tvdb") if (it.get("ids") or {}).get(k)},
        })
        key = None
        ids = node["ids"]
        for k in ("imdb", "tmdb", "tvdb"):
            if ids.get(k): key = f"{k}:{ids[k]}".lower(); break
        if not key: key = canonical_key(node)
        prev_val = None
        if key in shadow_map and isinstance(shadow_map[key], dict):
            pv = shadow_map[key].get("rating"); prev_val = int(pv) if isinstance(pv, int) else None
            shadow_map[key].pop("rating", None)
        _emit_rating_event(action="unrate", node={"type": node["type"], "title": node["title"], "ids": node["ids"]}, prev=prev_val, value=None)
    if cleared: _ratings_shadow_save(shadow_map)
    return cleared

# ----- History ----------------------------------------------------------------
def _history_row(obj: Any, kind: str) -> Optional[Dict[str, Any]]:
    """Normalize a Plex watched row (kind must be singular)."""
    try: vc = getattr(obj, "viewCount", 0) or 0
    except Exception: vc = 0
    if vc <= 0: return None
    ids = _ids_from_plexobj(obj)
    if not ids: return None
    row: Dict[str, Any] = {
        "type": _singular(kind), "title": getattr(obj, "title", None), "year": getattr(obj, "year", None),
        "ids": ids, "watched": True,
    }
    try:
        wa = getattr(obj, "lastViewedAt", None)
        if wa:
            try: row["watched_at"] = wa.strftime("%Y-%m-%dT%H:%M:%SZ")
            except Exception: pass
    except Exception: pass
    return _norm_row(row)

def _history_index_full(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """Build watched index across filtered sections (singular types)."""
    plex_cfg = dict(cfg_root.get("plex") or {})
    include, exclude = _section_filters(plex_cfg)
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            sections = [sec for sec in s.library.sections() if getattr(sec, "type", "") in ("movie", "show")]
            sections = [sec for sec in sections if _match_section(sec, include, exclude)]
            for section in sections:
                if section.type == "movie":
                    try:
                        for mv in section.all():
                            row = _history_row(mv, "movie")
                            if not row: continue
                            key = None
                            for k in ("imdb", "tmdb", "tvdb"):
                                if row["ids"].get(k): key = f"{k}:{row['ids'][k]}".lower(); break
                            if not key: key = canonical_key(row)
                            idx[key] = row
                    except Exception: continue
                if section.type == "show":
                    try:
                        for sh in section.all():
                            try:
                                for ep in sh.episodes():
                                    row = _history_row(ep, "episode")
                                    if not row: continue
                                    key = None
                                    for k in ("imdb", "tmdb", "tvdb"):
                                        if row["ids"].get(k): key = f"{k}:{row['ids'][k]}".lower(); break
                                    if not key: key = canonical_key(row)
                                    idx[key] = row
                            except Exception: pass
                    except Exception: continue
        except Exception: continue
    return idx

def _history_index(env: PlexEnv, cfg_root: Mapping[str, Any]) -> Dict[str, Dict[str, Any]]:
    """History snapshot with optional TTL cache (normalized on read/write)."""
    ttl_min = int(_cfg_get(cfg_root, "history.cache.ttl_minutes", 0) or 0)
    ttl_sec = max(0, ttl_min * 60)
    shadow = _history_shadow_load()
    items = dict(shadow.get("items") or {}); ts = int(shadow.get("ts") or 0)
    now = int(time.time())
    if ttl_sec > 0 and ts > 0 and (now - ts) < ttl_sec and items:
        norm = _normalize_shadow_items(items)
        try: host_log("PLEX.history", {"cache": "hit", "age_sec": now - ts, "count": len(norm)})
        except Exception: pass
        return norm
    idx = _history_index_full(env, cfg_root)
    if idx:
        norm = _normalize_shadow_items(idx)
        _history_shadow_save(norm)
        try: host_log("PLEX.history", {"cache": "refresh", "count": len(norm)})
        except Exception: pass
        return norm
    norm = _normalize_shadow_items(items)
    try: host_log("PLEX.history", {"cache": "fallback_shadow", "count": len(norm)})
    except Exception: pass
    return norm

def _history_apply(env: PlexEnv, items: Iterable[Mapping[str, Any]], watched: bool) -> int:
    """Mark items played/unplayed across servers."""
    changed = 0
    for it in items:
        libtype = _libtype_from_item(it)
        obj = _resolve_on_servers(env, it, libtype)
        if not obj: continue
        try:
            obj.markPlayed() if watched else obj.markUnplayed()
            changed += 1
        except Exception: continue
    return changed

# ----- Playlists ---------------------------------------------------------------
def _playlists_index(env: PlexEnv) -> Dict[str, Dict[str, Any]]:
    """List video playlists."""
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            for pl in s.playlists():
                if getattr(pl, "playlistType", "") not in ("video", "movie", "show"): continue
                key = f"playlist:{pl.ratingKey}".lower()
                row = {"type": "playlist", "title": getattr(pl, "title", None), "ids": {"plex": pl.ratingKey}}
                idx[key] = row
        except Exception: continue
    return idx

def _playlist_add_items(env: PlexEnv, playlist_title: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None) -> bool:
    """Create or extend a playlist by title."""
    if not env.servers: return False
    s = env.servers[0]
    libtype = "movie" if (mtype_hint or "movie") in ("movie", "movies") else "show"
    plex_items: List[Any] = []
    for it in items:
        obj = _resolve_on_servers(env, it, libtype)
        if obj: plex_items.append(obj)
    if not plex_items: return False
    try:
        for pl in s.playlists():
            if getattr(pl, "title", "") == playlist_title:
                pl.addItems(plex_items); return True
        s.createPlaylist(playlist_title, plex_items); return True
    except Exception: return False

def _playlist_remove_items(env: PlexEnv, playlist_title: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None) -> bool:
    """Remove items from a playlist by title."""
    if not env.servers: return False
    s = env.servers[0]
    libtype = "movie" if (mtype_hint or "movie") in ("movie", "movies") else "show"
    plex_items: List[Any] = []
    for it in items:
        obj = _resolve_on_servers(env, it, libtype)
        if obj: plex_items.append(obj)
    if not plex_items: return False
    try:
        for pl in s.playlists():
            if getattr(pl, "title", "") == playlist_title:
                pl.removeItems(plex_items); return True
    except Exception: return False
    return False

# ----- OPS implementation ------------------------------------------------------
class _PlexOPS:
    def name(self) -> str: return "PLEX"
    def label(self) -> str: return "Plex"

    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {
            "bidirectional": True,
            "provides_ids": True,
            "ratings": {"types": {"movies": True, "shows": True, "seasons": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False},
        }

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        plex_cfg = dict(cfg.get("plex") or {})
        token = (plex_cfg.get("account_token") or "").strip()
        if not token: raise ValueError("plex.account_token is required")
        if feature == "watchlist": return _watchlist_index(token)
        env = _env_from_config(cfg)
        if feature == "ratings":  return _ratings_index(env, cfg)
        if feature == "history":  return _history_index(env, cfg)
        if feature == "playlists": return _playlists_index(env)
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        plex_cfg = dict(cfg.get("plex") or {})
        acct = _ensure_account(plex_cfg)
        env  = _env_from_config(cfg)
        items_list = list(items)
        if feature in ("watchlist", "ratings", "history"):
            store = _gmt_store_from_cfg(cfg)
            if store:
                op_add, _ = _gmt_ops_for_feature(feature)
                items_list = [it for it in items_list if not _gmt_suppress(store=store, item=it, feature=feature, write_op=op_add)]
        if feature == "watchlist":
            if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _watchlist_add(acct, items_list); return {"ok": True, "count": cnt}
        if feature == "ratings":
            if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _ratings_apply(env, items_list); return {"ok": True, "count": cnt}
        if feature == "history":
            if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _history_apply(env, items_list, watched=True); return {"ok": True, "count": cnt}
        if feature == "playlists":
            if dry_run: return {"ok": True, "count": sum(len(it.get("items", [])) for it in items_list), "dry_run": True}
            added = 0
            for pl in items_list:
                title = pl.get("playlist") or pl.get("title")
                if not title: continue
                mtyp = pl.get("type") or "movie"
                if _playlist_add_items(env, str(title), pl.get("items") or [], mtype_hint=mtyp):
                    added += len(pl.get("items") or [])
            return {"ok": True, "count": added}
        return {"ok": True, "count": 0}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        plex_cfg = dict(cfg.get("plex") or {})
        acct = _ensure_account(plex_cfg)
        env  = _env_from_config(cfg)
        items_list = list(items)
        if feature == "watchlist":
            if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _watchlist_remove(acct, items_list)
            store = _gmt_store_from_cfg(cfg)
            if store and cnt:
                for it in items_list: _gmt_record(store=store, item=it, feature="watchlist", op="remove", origin="PLEX")
            return {"ok": True, "count": cnt}
        if feature == "ratings":
            if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _ratings_remove(env, items_list)
            store = _gmt_store_from_cfg(cfg)
            if store and cnt:
                for it in items_list: _gmt_record(store=store, item=it, feature="ratings", op="unrate", origin="PLEX")
            return {"ok": True, "count": cnt}
        if feature == "history":
            if dry_run: return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _history_apply(env, items_list, watched=False)
            store = _gmt_store_from_cfg(cfg)
            if store and cnt:
                for it in items_list: _gmt_record(store=store, item=it, feature="history", op="unscrobble", origin="PLEX")
            return {"ok": True, "count": cnt}
        if feature == "playlists":
            if dry_run: return {"ok": True, "count": sum(len(it.get("items", [])) for it in items_list), "dry_run": True}
            removed = 0
            for pl in items_list:
                title = pl.get("playlist") or pl.get("title")
                if not title: continue
                mtyp = pl.get("type") or "movie"
                ok = _playlist_remove_items(env, str(title), pl.get("items") or [], mtype_hint=mtyp)
                if ok: removed += len(pl.get("items") or [])
            return {"ok": True, "count": removed}
        return {"ok": True, "count": 0}

# Exported adapter for orchestrator discovery
OPS: InventoryOps = _PlexOPS()

# ----- Module manifest (for /api/sync/providers) ------------------------------
try:
    from providers.sync._base import SyncModule, ModuleInfo, ModuleCapabilities  # type: ignore
except Exception:  # pragma: no cover
    from dataclasses import dataclass
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

class PLEXModule(SyncModule):
    info = ModuleInfo(
        name="PLEX",
        version=__VERSION__,
        description="Reads/writes Plex watchlist (plex.tv), ratings (movies/shows/seasons/episodes) with TTL cache + events, history, and playlists.",
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
                    "plex": {
                        "type": "object",
                        "properties": {
                            "account_token": {"type": "string", "minLength": 1},
                            "servers": {
                                "type": "object",
                                "properties": {
                                    "machine_ids": {"type": "array", "items": {"type": "string"}},
                                    "sections": {
                                        "type": "object",
                                        "properties": {
                                            "include": {"type": "array", "items": {"type": "string"}},
                                            "exclude": {"type": "array", "items": {"type": "string"}},
                                        }
                                    },
                                }
                            },
                        },
                        "required": ["account_token"],
                    },
                    "ratings": {
                        "type": "object",
                        "properties": {
                            "cache": {"type": "object", "properties": {"ttl_minutes": {"type": "integer", "minimum": 0}}}
                        }
                    },
                    "history": {
                        "type": "object",
                        "properties": {
                            "cache": {"type": "object", "properties": {"ttl_minutes": {"type": "integer", "minimum": 0}}}
                        }
                    },
                    "sync": {
                        "type": "object",
                        "properties": {
                            "gmt_enable": {"type": "boolean"},
                            "gmt_quarantine_days": {"type": "integer", "minimum": 1},
                        },
                    },
                    "runtime": {"type": "object", "properties": {"debug": {"type": "boolean"}}},
                },
                "required": ["plex"],
            },
        ),
    )

    @staticmethod
    def supported_features() -> dict:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

def get_manifest() -> dict:
    return {
        "name": PLEXModule.info.name,
        "label": "Plex",
        "features": PLEXModule.supported_features(),
        "capabilities": {
            "bidirectional": True,
            "ratings": {"types": {"movies": True, "shows": True, "seasons": True, "episodes": True}, "upsert": True, "unrate": True, "from_date": False},
        },
        "version": PLEXModule.info.version,
        "vendor": PLEXModule.info.vendor,
        "description": PLEXModule.info.description,
    }

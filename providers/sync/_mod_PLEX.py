from __future__ import annotations
# providers/sync/_mod_PLEX.py
# Unified OPS provider for Plex: watchlist, ratings, history, playlists

__VERSION__ = "1.0.2"

import re
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

_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "plex", "guid")

# ----- HTTP helpers for plex.tv discover --------------------------------------
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
            except Exception:
                pass
        if r.ok:
            return r.json()
    except Exception:
        if _stats:
            try:
                _stats.record_http(
                    provider="PLEX",
                    endpoint=path,
                    method="GET",
                    status=0,
                    ok=False,
                    bytes_in=0,
                    bytes_out=0,
                    ms=0,
                )
            except Exception:
                pass
    return None

# ----- Canonical/minimal helpers ----------------------------------------------
def canonical_key(item: Mapping[str, Any]) -> str:
    ids = item.get("ids") or {}
    for k in ("tmdb", "imdb", "tvdb", "guid"):
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
def _discover_metadata_by_ratingkey(token: str, rating_key: str) -> Optional[dict]:
    params = {"includeExternalMedia": "1"}
    data = _discover_get(f"{PLEX_METADATA_PATH}/{rating_key}", token, params, timeout=12)
    if not data:
        return None
    md = (data.get("MediaContainer", {}).get("Metadata") or [])
    if md:
        return md[0]
    items = data.get("items") or []
    return items[0] if items else None

def _watchlist_fetch_via_discover(token: str, page_size: int = 100) -> List[Dict[str, Any]]:
    params_base = {"includeCollections": "1", "includeExternalMedia": "1"}
    start = 0
    items: List[dict] = []
    while True:
        params = dict(params_base)
        params["X-Plex-Container-Start"] = str(start)
        params["X-Plex-Container-Size"] = str(page_size)
        data = _discover_get(PLEX_WATCHLIST_PATH, token, params, timeout=20)
        if not data:
            break
        mc = (data.get("MediaContainer", {}) or {})
        md = mc.get("Metadata") or data.get("items") or []
        if not isinstance(md, list):
            md = []
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
                    if isinstance(enriched.get("guid"), str):
                        e_guids.append(enriched["guid"])
                    imdb, tmdb, tvdb = _extract_ids_from_guid_strings(e_guids)

            ids: Dict[str, Any] = {}
            if imdb: ids["imdb"] = imdb
            if tmdb is not None: ids["tmdb"] = tmdb
            if tvdb is not None: ids["tvdb"] = tvdb
            items.append({"type": mtype, "title": title, "ids": ids})
        if fetched < page_size:
            break
        start += fetched
    return items

# ----- PlexAPI environment (used for writes/servers) --------------------------
@dataclass
class PlexEnv:
    account: Optional[MyPlexAccount]
    servers: List[Any]  # plexapi.server.PlexServer

def _ensure_account(plex_cfg: Mapping[str, Any]) -> MyPlexAccount:
    if not HAS_PLEXAPI:
        raise RuntimeError("plexapi is required")
    token = (plex_cfg.get("account_token") or "").strip()
    if not token:
        raise ValueError("plex.account_token is required")
    return MyPlexAccount(token=token)  # type: ignore

def _connect_servers(acct: MyPlexAccount, plex_cfg: Mapping[str, Any]) -> List[Any]:
    wanted_ids: List[str] = list(plex_cfg.get("servers", {}).get("machine_ids") or [])
    servers: List[Any] = []
    for res in acct.resources():
        if "server" not in (res.provides or ""):
            continue
        if wanted_ids and res.clientIdentifier not in wanted_ids:
            continue
        try:
            servers.append(res.connect(timeout=6))
        except Exception:
            continue
    if not servers:
        for res in acct.resources():
            if "server" in (res.provides or ""):
                try:
                    servers.append(res.connect(timeout=6))
                    break
                except Exception:
                    continue
    return servers

def _env_from_config(cfg: Mapping[str, Any]) -> PlexEnv:
    plex_cfg = dict(cfg.get("plex") or {})
    acct = _ensure_account(plex_cfg)
    servers = _connect_servers(acct, plex_cfg)
    return PlexEnv(account=acct, servers=servers)

# ----- Server/object helpers ---------------------------------------------------
def _ids_from_plexobj(obj: Any) -> Dict[str, Any]:
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
    except Exception:
        pass
    try:
        gsingle = getattr(obj, "guid", None)
        if isinstance(gsingle, str):
            if "imdb" in gsingle and "imdb" not in ids:
                m = _PAT_IMDB.search(gsingle);  ids["imdb"] = m.group(1) if m else ids.get("imdb")
            if "tmdb" in gsingle and "tmdb" not in ids:
                m = _PAT_TMDB.search(gsingle);  ids["tmdb"] = int(m.group(1)) if m else ids.get("tmdb")
            if "thetvdb" in gsingle and "tvdb" not in ids:
                m = _PAT_TVDB.search(gsingle);  ids["tvdb"] = int(m.group(1)) if m else ids.get("tvdb")
    except Exception:
        pass
    return {k: v for k, v in ids.items() if v is not None}

def _server_find_item(server: Any, q: Mapping[str, Any], mtype: str) -> Optional[Any]:
    ids = (q.get("ids") or q) or {}
    for key in ("imdb", "tmdb", "tvdb"):
        val = ids.get(key)
        if not val:
            continue
        variants = [f"{key}://{val}"]
        if key == "imdb":
            variants.append(f"com.plexapp.agents.imdb://{val}")
        elif key == "tmdb":
            variants.append(f"com.plexapp.agents.tmdb://{val}")
        elif key == "tvdb":
            variants.append(f"com.plexapp.agents.thetvdb://{val}")
        for g in variants:
            try:
                hits = server.search(guid=g, libtype=mtype) or []
                if hits:
                    return hits[0]
            except Exception:
                pass
    title = q.get("title")
    year = q.get("year")
    try:
        hits = server.search(title=title, year=year, libtype=mtype) or []
        if hits:
            return hits[0]
    except Exception:
        pass
    return None


def _resolve_on_servers(env: PlexEnv, q: Mapping[str, Any], mtype: str) -> Optional[Any]:
    for s in env.servers:
        it = _server_find_item(s, q, "movie" if mtype == "movie" else "show")
        if it: return it
    return None

# ----- Watchlist (plex.tv; read via discover, write via account) --------------
def _resolve_discover_item(acct: MyPlexAccount, ids: dict, libtype: str) -> Optional[Any]:
    queries: List[str] = []
    if ids.get("imdb"): queries.append(ids["imdb"])
    if ids.get("tmdb"): queries.append(str(ids["tmdb"]))
    if ids.get("tvdb"): queries.append(str(ids["tvdb"]))
    if ids.get("title"): queries.append(ids["title"])
    queries = list(dict.fromkeys(queries))
    for q in queries:
        try:
            hits: Sequence[Any] = acct.searchDiscover(q, libtype=libtype) or []
        except Exception:
            hits = []
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
                except Exception:
                    pass
    return None

def _watchlist_add(acct: MyPlexAccount, items: Iterable[Mapping[str, Any]]) -> int:
    added = 0
    for it in items:
        ids = dict(it.get("ids") or {})
        if "title" not in ids and it.get("title"): ids["title"] = it["title"]
        if "year"  not in ids and it.get("year"):  ids["year"]  = it["year"]
        libtype = "movie" if (it.get("type") or "movie") == "movie" else "show"
        md = _resolve_discover_item(acct, ids, libtype)
        if not md: continue
        try:
            cast(Any, md).addToWatchlist(account=acct)
            added += 1
        except Exception as e:
            msg = str(e).lower()
            if "already on the watchlist" in msg or "409" in msg:
                added += 1
    return added

def _watchlist_remove(acct: MyPlexAccount, items: Iterable[Mapping[str, Any]]) -> int:
    removed = 0
    for it in items:
        ids = dict(it.get("ids") or {})
        if "title" not in ids and it.get("title"): ids["title"] = it["title"]
        if "year"  not in ids and it.get("year"):  ids["year"]  = it["year"]
        libtype = "movie" if (it.get("type") or "movie") == "movie" else "show"
        md = _resolve_discover_item(acct, ids, libtype)
        if not md: continue
        try:
            cast(Any, md).removeFromWatchlist(account=acct)
            removed += 1
        except Exception as e:
            msg = str(e).lower()
            if "not on the watchlist" in msg or "404" in msg:
                removed += 1
    return removed

def _watchlist_index(token: str) -> Dict[str, Dict[str, Any]]:
    """Read Plex Watchlist via plex.tv discover using only the account token."""
    items = _watchlist_fetch_via_discover(token, page_size=100)
    idx: Dict[str, Dict[str, Any]] = {}
    for it in items:
        ids = it.get("ids") or {}
        for k in ("imdb", "tmdb", "tvdb"):
            if ids.get(k):
                idx[f"{k}:{ids[k]}".lower()] = {
                    "type": it.get("type") or "movie",
                    "title": it.get("title"),
                    "ids": {kk: ids.get(kk) for kk in ("imdb", "tmdb", "tvdb") if ids.get(kk)},
                }
                break
    return idx

# ----- Ratings (server-side) ---------------------------------------------------
def _ratings_index(env: PlexEnv) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            for section in s.library.sections():
                if section.type not in ("movie", "show"): 
                    continue
                try:
                    items = section.all()
                except Exception:
                    items = []
                for it in items:
                    ur = getattr(it, "userRating", None)
                    if ur is None: 
                        continue
                    ids = _ids_from_plexobj(it)
                    if not ids: 
                        continue
                    key = None
                    for k in ("imdb", "tmdb", "tvdb"):
                        if ids.get(k):
                            key = f"{k}:{ids[k]}".lower(); break
                    if not key: 
                        continue
                    idx[key] = {"type": section.type, "title": getattr(it, "title", None), "year": getattr(it, "year", None), "ids": ids, "rating": ur}
        except Exception:
            continue
    return idx

def _ratings_apply(env: PlexEnv, items: Iterable[Mapping[str, Any]]) -> int:
    updated = 0
    for it in items:
        target_rating = it.get("rating")
        if target_rating is None: 
            continue
        mtype = "movie" if (it.get("type") or "movie") == "movie" else "show"
        obj = _resolve_on_servers(env, it, mtype)
        if not obj: 
            continue
        try:
            obj.rate(float(target_rating))
            updated += 1
        except Exception:
            continue
    return updated

def _ratings_remove(env: PlexEnv, items: Iterable[Mapping[str, Any]]) -> int:
    cleared = 0
    for it in items:
        mtype = "movie" if (it.get("type") or "movie") == "movie" else "show"
        obj = _resolve_on_servers(env, it, mtype)
        if not obj: 
            continue
        try:
            obj.rate(None)  # clear rating
            cleared += 1
        except Exception:
            continue
    return cleared

# ----- History (played/unplayed) ----------------------------------------------
def _history_index(env: PlexEnv) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            for section in s.library.sections():
                if section.type not in ("movie", "show"): 
                    continue
                try:
                    items = section.all()
                except Exception:
                    items = []
                for it in items:
                    vc = getattr(it, "viewCount", 0) or 0
                    if vc <= 0: 
                        continue
                    ids = _ids_from_plexobj(it)
                    if not ids: 
                        continue
                    key = None
                    for k in ("imdb", "tmdb", "tvdb"):
                        if ids.get(k):
                            key = f"{k}:{ids[k]}".lower(); break
                    if not key: 
                        continue
                    idx[key] = {"type": section.type, "title": getattr(it, "title", None), "year": getattr(it, "year", None), "ids": ids, "watched": True}
        except Exception:
            continue
    return idx

def _history_apply(env: PlexEnv, items: Iterable[Mapping[str, Any]], watched: bool) -> int:
    changed = 0
    for it in items:
        mtype = "movie" if (it.get("type") or "movie") == "movie" else "show"
        obj = _resolve_on_servers(env, it, mtype)
        if not obj: 
            continue
        try:
            if watched:
                obj.markPlayed()
            else:
                obj.markUnplayed()
            changed += 1
        except Exception:
            continue
    return changed

# ----- Playlists ---------------------------------------------------------------
def _playlists_index(env: PlexEnv) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for s in env.servers:
        try:
            for pl in s.playlists():
                if getattr(pl, "playlistType", "") not in ("video", "movie", "show"): 
                    continue
                key = f"playlist:{pl.ratingKey}".lower()
                idx[key] = {"type": "playlist", "title": getattr(pl, "title", None), "ids": {"plex": pl.ratingKey}}
        except Exception:
            continue
    return idx

def _playlist_add_items(env: PlexEnv, playlist_title: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None) -> bool:
    if not env.servers: 
        return False
    s = env.servers[0]
    libtype = "movie" if (mtype_hint or "movie") == "movie" else "show"
    plex_items: List[Any] = []
    for it in items:
        obj = _resolve_on_servers(env, it, libtype)
        if obj: 
            plex_items.append(obj)
    if not plex_items: 
        return False
    try:
        for pl in s.playlists():
            if getattr(pl, "title", "") == playlist_title:
                pl.addItems(plex_items)
                return True
        s.createPlaylist(playlist_title, plex_items)
        return True
    except Exception:
        return False

def _playlist_remove_items(env: PlexEnv, playlist_title: str, items: Iterable[Mapping[str, Any]], mtype_hint: Optional[str]=None) -> bool:
    if not env.servers: 
        return False
    s = env.servers[0]
    libtype = "movie" if (mtype_hint or "movie") == "movie" else "show"
    plex_items: List[Any] = []
    for it in items:
        obj = _resolve_on_servers(env, it, libtype)
        if obj: 
            plex_items.append(obj)
    if not plex_items: 
        return False
    try:
        for pl in s.playlists():
            if getattr(pl, "title", "") == playlist_title:
                pl.removeItems(plex_items)
                return True
    except Exception:
        return False
    return False

# ----- OPS implementation ------------------------------------------------------
class _PlexOPS:
    def name(self) -> str: return "PLEX"
    def label(self) -> str: return "Plex"
    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True, "ratings": True, "history": True, "playlists": True}
    def capabilities(self) -> Mapping[str, Any]:
        # provides_ids=True means orchestrator can skip enrichment for Plex-sourced items
        return {"bidirectional": True, "provides_ids": True}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Dict[str, Any]]:
        plex_cfg = dict(cfg.get("plex") or {})
        token = plex_cfg.get("account_token", "").strip()
        if not token:
            raise ValueError("plex.account_token is required")

        if feature == "watchlist":
            # Read-only via discover; no plexapi required
            return _watchlist_index(token)

        # The features below require server access via plexapi
        env = _env_from_config(cfg)
        if feature == "ratings":
            return _ratings_index(env)
        if feature == "history":
            return _history_index(env)
        if feature == "playlists":
            return _playlists_index(env)
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        plex_cfg = dict(cfg.get("plex") or {})
        acct = _ensure_account(plex_cfg)
        env = _env_from_config(cfg)
        items_list = list(items)

        if feature == "watchlist":
            if dry_run:
                return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _watchlist_add(acct, items_list)
            return {"ok": True, "count": cnt}

        if feature == "ratings":
            if dry_run:
                return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _ratings_apply(env, items_list)
            return {"ok": True, "count": cnt}

        if feature == "history":
            if dry_run:
                return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _history_apply(env, items_list, watched=True)
            return {"ok": True, "count": cnt}

        if feature == "playlists":
            if dry_run:
                return {"ok": True, "count": sum(len(it.get("items", [])) for it in items_list), "dry_run": True}
            removed = 0
            for pl in items_list:
                title = pl.get("playlist") or pl.get("title")
                if not title:
                    continue
                mtyp = pl.get("type") or "movie"
                if _playlist_remove_items(env, str(title), pl.get("items") or [], mtype_hint=mtyp):
                    removed += len(pl.get("items") or [])
            return {"ok": True, "count": removed}

        return {"ok": True, "count": 0}


    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool=False) -> Dict[str, Any]:
        plex_cfg = dict(cfg.get("plex") or {})
        acct = _ensure_account(plex_cfg)
        env = _env_from_config(cfg)
        items_list = list(items)

        if feature == "watchlist":
            if dry_run:
                return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _watchlist_remove(acct, items_list)
            return {"ok": True, "count": cnt}

        if feature == "ratings":
            if dry_run:
                return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _ratings_remove(env, items_list)
            return {"ok": True, "count": cnt}

        if feature == "history":
            if dry_run:
                return {"ok": True, "count": len(items_list), "dry_run": True}
            cnt = _history_apply(env, items_list, watched=False)
            return {"ok": True, "count": cnt}

        if feature == "playlists":
            if dry_run:
                return {"ok": True, "count": sum(len(it.get("items", [])) for it in items_list), "dry_run": True}
            removed = 0
            for pl in items_list:
                title = pl.get("playlist") or pl.get("title")
                if not title:
                    continue
                mtyp = pl.get("type") or "movie"
                if _playlist_remove_items(env, str(title), pl.get("items") or [], mtype_hint=mtyp):
                    removed += len(pl.get("items") or [])
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
        description="Reads/writes Plex watchlist (plex.tv), ratings, history, and playlists.",
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
                                }
                            },
                        },
                        "required": ["account_token"],
                    },
                    "runtime": {
                        "type": "object",
                        "properties": {"debug": {"type": "boolean"}},
                    },
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
        "capabilities": {"bidirectional": True},
        "version": PLEXModule.info.version,
        "vendor": PLEXModule.info.vendor,
        "description": PLEXModule.info.description,
    }

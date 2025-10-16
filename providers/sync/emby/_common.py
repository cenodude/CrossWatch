# /providers/sync/emby/_common.py
from __future__ import annotations
from typing import Any, Dict, Mapping, Optional, Iterable, List, Tuple, Union
from datetime import datetime
import os, re, time, json

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

_IMDB_PAT  = re.compile(r"(?:tt)?(\d{5,9})$")
_NUM_PAT   = re.compile(r"(\d{1,10})$")
_BAD_NUM   = re.compile(r"^\d{13,}$")

CfgLike = Union[Mapping[str, Any], object]

# Adapter-scoped provider-index cache
_PROVIDER_INDEX_CACHE: dict[int, tuple[float, dict[str, list[dict[str, Any]]]]] = {}

# --- logging (quiet by default)
def _debug_level() -> str:
    env = (os.environ.get("CW_EMBY_DEBUG_LEVEL") or "").strip().lower()
    if env in ("2", "v", "verbose"): return "verbose"
    if env in ("1", "s", "summary", "true", "on"): return "summary"
    if os.environ.get("CW_EMBY_DEBUG") or os.environ.get("CW_DEBUG"): return "summary"
    return "off"

def _log_summary(msg: str) -> None:
    if _debug_level() in ("summary", "verbose"):
        print(f"[EMBY:common] {msg}")

def _log_detail(msg: str) -> None:
    if _debug_level() == "verbose":
        print(f"[EMBY:common] {msg}")

def _log(msg: str) -> None:
    _log_summary(msg)

# --- cfg helpers
def _as_list_str(v: Any) -> List[str]:
    if v is None: return []
    it = v if isinstance(v, (list, tuple, set)) else [v]
    out: List[str] = []; seen = set()
    for x in it:
        s = str(x).strip()
        if s and s not in seen:
            seen.add(s); out.append(s)
    return out

def _pluck(cfg: CfgLike, *path: str) -> Any:
    cur: Any = cfg
    for key in path:
        if isinstance(cur, Mapping) and key in cur:
            cur = cur[key]
        else:
            cur = getattr(cur, key, None)
        if cur is None:
            return None
    return cur

def _ts(v):
    try:
        s = str(v).strip()
        if not s: return None
        if s.isdigit(): return int(s)
        s = s.replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        try:
            return int(datetime.strptime(s[:19], "%Y-%m-%dT%H:%M:%S").timestamp())
        except Exception:
            try:
                return int(datetime.strptime(s[:10], "%Y-%m-%d").timestamp())
            except Exception:
                return None

def _emby_scope_from_list(libs: List[str]) -> Dict[str, Any]:
    if not libs: return {}
    if len(libs) == 1:
        return {"ParentId": libs[0], "Recursive": True}
    return {"AncestorIds": libs, "Recursive": True}

def emby_library_scope(cfg: CfgLike, feature: str) -> Dict[str, Any]:
    em = _pluck(cfg, "emby") or cfg
    libs = _as_list_str(_pluck(em, feature, "libraries"))
    if not libs:
        libs = _as_list_str(getattr(cfg, f"{feature}_libraries", None))
        if not libs:
            sub = getattr(cfg, feature, None)
            if sub is not None:
                libs = _as_list_str(getattr(sub, "libraries", None))
    return _emby_scope_from_list(libs)

def with_emby_scope(params: Mapping[str, Any], cfg: CfgLike, feature: str) -> Dict[str, Any]:
    out = dict(params or {})
    out.update(emby_library_scope(cfg, feature))
    return out

def emby_scope_history(cfg: CfgLike) -> Dict[str, Any]:
    return emby_library_scope(cfg, "history")

def emby_scope_ratings(cfg: CfgLike) -> Dict[str, Any]:
    return emby_library_scope(cfg, "ratings")

def emby_scope_any(cfg: CfgLike) -> Dict[str, Any]:
    em = _pluck(cfg, "emby") or cfg
    libs_h = _as_list_str(_pluck(em, "history", "libraries"))
    libs_r = _as_list_str(_pluck(em, "ratings", "libraries"))
    libs: List[str] = []
    seen = set()
    for x in (libs_h + libs_r):
        if x and x not in seen:
            seen.add(x); libs.append(x)
    if not libs and not isinstance(cfg, Mapping):
        libs_h2 = _as_list_str(getattr(cfg, "history_libraries", None))
        libs_r2 = _as_list_str(getattr(cfg, "ratings_libraries", None))
        for x in (libs_h2 + libs_r2):
            if x and x not in seen:
                seen.add(x); libs.append(x)
        for sub in (getattr(cfg, "history", None), getattr(cfg, "ratings", None)):
            if hasattr(sub, "libraries"):
                for x in _as_list_str(getattr(sub, "libraries", None)):
                    if x and x not in seen:
                        seen.add(x); libs.append(x)
    return _emby_scope_from_list(libs)

# --- type & id helpers
def _norm_type(t: Any) -> str:
    x = (str(t or "").strip().lower())
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series", "tv"): return "show"
    if x in ("episode", "episodes"): return "episode"
    return "movie"

def looks_like_bad_id(iid: Any) -> bool:
    return bool(_BAD_NUM.match(str(iid or "")))

def _ids_from_provider_ids(pids: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(pids, Mapping): return out
    low = {str(k).lower(): (v if v is not None else "") for k, v in pids.items()}
    v = low.get("imdb")
    if v is not None:
        m = _IMDB_PAT.search(str(v).strip());  out["imdb"] = f"tt{m.group(1)}" if m else out.get("imdb")
    v = low.get("tmdb")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip());   out["tmdb"] = m.group(1) if m else out.get("tmdb")
    v = low.get("tvdb")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip());   out["tvdb"] = m.group(1) if m else out.get("tvdb")
    em = low.get("emby")
    if em: out["emby"] = str(em)
    return out

def normalize(obj: Mapping[str, Any]) -> Dict[str, Any]:
    if isinstance(obj, Mapping) and "ids" in obj and "type" in obj:
        return id_minimal(obj)
    t = _norm_type(obj.get("Type") or obj.get("BaseItemKind") or obj.get("type"))
    title = (obj.get("Name") or obj.get("title") or "").strip() or None
    year = obj.get("ProductionYear") if isinstance(obj.get("ProductionYear"), int) else obj.get("year")
    pids = obj.get("ProviderIds") if isinstance(obj.get("ProviderIds"), Mapping) else (obj.get("ids") or {})
    ids = {k: v for k, v in _ids_from_provider_ids(pids).items() if v}
    em_id = obj.get("Id") or (pids.get("emby") if isinstance(pids, Mapping) else None)
    if em_id: ids["emby"] = str(em_id)
    row: Dict[str, Any] = {"type": t, "title": title, "year": year, "ids": ids}
    if t == "episode":
        series_title = (obj.get("SeriesName") or obj.get("Series") or obj.get("SeriesTitle") or obj.get("series_title") or "").strip() or None
        if series_title: row["series_title"] = series_title
        s = (obj.get("ParentIndexNumber") or obj.get("SeasonIndexNumber") or obj.get("season") or obj.get("season_number"))
        e = (obj.get("IndexNumber") or obj.get("EpisodeIndexNumber") or obj.get("episode") or obj.get("episode_number"))
        try:
            if s is not None: row["season"] = int(s)
        except Exception: pass
        try:
            if e is not None: row["episode"] = int(e)
        except Exception: pass
    return id_minimal(row)

def key_of(item: Mapping[str, Any]) -> str:
    return canonical_key(normalize(item))

def map_provider_key(k: str) -> Optional[str]:
    if not k: return None
    kl = str(k).strip().lower()
    if kl.startswith("agent:themoviedb"): return "tmdb"
    if kl.startswith("agent:imdb"): return "imdb"
    if kl.startswith("agent:tvdb"): return "tvdb"
    if kl in ("tmdb", "imdb", "tvdb"): return kl
    return None

def format_provider_pair(k: str, v: Any) -> Optional[str]:
    kk = map_provider_key(k);  sv = str(v or "").strip()
    if not kk or not sv: return None
    if kk == "imdb":
        m = _IMDB_PAT.search(sv);  sv = f"tt{m.group(1)}" if m else None
    else:
        m = _NUM_PAT.search(sv);    sv = str(int(m.group(1))) if m else None
    return f"{kk}.{sv}" if sv else None

def guid_priority_from_cfg(cfg_list: Optional[Iterable[str]]) -> List[str]:
    default = ["tmdb", "imdb", "tvdb", "agent:themoviedb:en", "agent:themoviedb", "agent:imdb"]
    if not cfg_list: return default
    seen, out = set(), []
    for k in cfg_list:
        k = str(k).strip()
        if k and k not in seen:
            out.append(k); seen.add(k)
    for k in default:
        if k not in seen: out.append(k)
    return out

def _merged_guid_priority(adapter) -> List[str]:
    hist = _as_list_str(_pluck(getattr(adapter, "cfg", None) or {}, "history_guid_priority"))
    wlst = _as_list_str(_pluck(getattr(adapter, "cfg", None) or {}, "watchlist_guid_priority"))
    return guid_priority_from_cfg(hist + wlst)

def pick_external_id(ids: Mapping[str, Any], priority: Iterable[str]) -> Optional[Tuple[str, str]]:
    for k in priority:
        v = ids.get(k)
        if v: return k, str(v)
    return None

def all_ext_pairs(it_ids: Mapping[str, Any], priority: Iterable[str]) -> List[str]:
    out: List[str] = []; seen = set()
    ext = pick_external_id(dict(it_ids or {}), list(priority))
    if ext:
        p = format_provider_pair(ext[0], ext[1])
        if p and p not in seen: out.append(p); seen.add(p)
    for k in ("tmdb", "imdb", "tvdb"):
        v = (it_ids or {}).get(k)
        p = format_provider_pair(k, v) if v else None
        if p and p not in seen: out.append(p); seen.add(p)
    return out

# --- provider index
def build_provider_index(adapter) -> Dict[str, List[Dict[str, Any]]]:
    http, uid = adapter.client, adapter.cfg.user_id
    out: Dict[str, List[Dict[str, Any]]] = {}
    start, limit, total = 0, 500, None
    while True:
        params = {
            "IncludeItemTypes": "Movie,Series",
            "Recursive": True,
            "Fields": "ProviderIds,ProductionYear,Type",
            "StartIndex": start,
            "Limit": limit,
            "EnableTotalRecordCount": True,
        }
        params.update(emby_scope_any(adapter.cfg))
        r = http.get(f"/Users/{uid}/Items", params=params)
        body = r.json() or {}
        items = body.get("Items") or []
        if total is None:
            total = int(body.get("TotalRecordCount") or 0)
            _log_summary(f"provider-index scan total={total}")
        for row in items:
            pids = row.get("ProviderIds") or {}
            if not pids: continue
            low = {str(k).lower(): str(v).strip() for k, v in pids.items() if v}
            if "imdb" in low:
                m = _IMDB_PAT.search(low["imdb"])
                if m: out.setdefault(f"imdb.tt{m.group(1)}", []).append(row)
            if "tmdb" in low and _NUM_PAT.search(low["tmdb"]):
                out.setdefault(f"tmdb.{int(_NUM_PAT.search(low['tmdb']).group(1))}", []).append(row)
            if "tvdb" in low and _NUM_PAT.search(low["tvdb"]):
                out.setdefault(f"tvdb.{int(_NUM_PAT.search(low['tvdb']).group(1))}", []).append(row)
        start += len(items)
        if not items or (total is not None and start >= total):
            break
    for k, rows in out.items():
        rows.sort(key=lambda r: str(r.get("Id") or ""))
    _log_summary(f"provider-index built keys={len(out)}")
    return out

def provider_index(adapter, *, ttl_sec: int = 300, force_refresh: bool = False) -> Dict[str, List[Dict[str, Any]]]:
    import time as _time
    key = id(adapter); now = _time.time()
    if not force_refresh:
        hit = _PROVIDER_INDEX_CACHE.get(key)
        if hit and (now - hit[0]) < max(1, int(ttl_sec)):
            return hit[1]
    idx = build_provider_index(adapter)
    _PROVIDER_INDEX_CACHE[key] = (now, idx)
    return idx

def find_series_in_index(adapter, pairs: Iterable[str]) -> Optional[Dict[str, Any]]:
    idx = provider_index(adapter)
    for pref in pairs or []:
        rows = idx.get(pref) or []
        for row in rows:
            if (row.get("Type") or "").strip() == "Series":
                return row
    return None

# --- series/episodes
def get_series_episodes(http, user_id: str, series_id: str, start: int = 0, limit: int = 500) -> Dict[str, Any]:
    q = {
        "UserId": user_id,
        "StartIndex": max(0, int(start)),
        "Limit": max(1, int(limit)),
        "Fields": "IndexNumber,ParentIndexNumber,SeasonId,SeriesId,ProviderIds,ProductionYear,Type",
        "EnableUserData": False,
    }
    r = http.get(f"/Shows/{series_id}/Episodes", params=q)
    if getattr(r, "status_code", 0) != 200:
        return {"Items": [], "TotalRecordCount": 0}
    data = r.json() or {}
    data.setdefault("Items", [])
    data.setdefault("TotalRecordCount", len(data["Items"]))
    return data

def _is_future_episode(it):
    now = int(time.time())
    keys = ("air_date","first_aired","premiere_date","premiered","originally_available_at","ReleaseDate","PremiereDate")
    if not isinstance(it, dict): return False
    for k in keys:
        v = it.get(k) or it.get(k.lower())
        ts = _ts(v) if v else None
        if ts and ts > now:
            return True
    return False

def _series_minimal_from_episode(http, uid: str, ep: Mapping[str, Any], _cache: Dict[str, Optional[Dict[str, Any]]]) -> Optional[Dict[str, Any]]:
    sid = ep.get("SeriesId") or ep.get("seriesid")
    if not sid: return None
    if sid in _cache: return _cache[sid]
    r = http.get(f"/Users/{uid}/Items", params={"Ids": sid, "Fields": "ProviderIds,ProductionYear,Type,Name"})
    if getattr(r, "status_code", 0) == 200:
        arr = (r.json() or {}).get("Items") or []
        if arr:
            m = normalize(arr[0])
            _cache[sid] = m
            return m
    _cache[sid] = None
    return None

def playlist_as_watchlist_index(http, user_id: str, playlist_id: str, *, limit: int = 1000, progress=None) -> Dict[str, Dict[str, Any]]:
    body = get_playlist_items(http, playlist_id, start=0, limit=max(1, int(limit)))
    rows: List[Mapping[str, Any]] = body.get("Items") or []
    total = int(body.get("TotalRecordCount") or len(rows) or 0)
    if progress:
        try: progress.tick(0, total=total, force=True)
        except Exception: pass
    out: Dict[str, Dict[str, Any]] = {}
    cache: Dict[str, Optional[Dict[str, Any]]] = {}
    done = 0
    for row in rows:
        t = (row.get("Type") or row.get("type") or "").strip().lower()
        if t == "movie":
            try:
                m = normalize(row); out[canonical_key(m)] = m
            except Exception: pass
        elif t == "episode":
            try:
                m = _series_minimal_from_episode(http, user_id, row, cache)
                if m: out[canonical_key(m)] = m
            except Exception: pass
        done += 1
        if progress:
            try: progress.tick(done, total=total)
            except Exception: pass
    return out

# --- playlists
def find_playlist_id_by_name(http, user_id: str, name: str) -> Optional[str]:
    q = {"UserId": user_id, "IncludeItemTypes": "Playlist", "Recursive": True, "SearchTerm": name}
    r = http.get(f"/Users/{user_id}/Items", params=q)
    if getattr(r, "status_code", 0) != 200: return None
    items = (r.json() or {}).get("Items") or []
    name_l = (name or "").strip().lower()
    for it in items:
        if (it.get("Name") or "").strip().lower() == name_l:
            return it.get("Id")
    if not items:
        q2 = {"UserId": user_id, "IncludeItemTypes": "Playlist", "Recursive": True}
        r2 = http.get(f"/Users/{user_id}/Items", params=q2)
        if getattr(r2, "status_code", 0) != 200: return None
        for it in (r2.json() or {}).get("Items") or []:
            if (it.get("Name") or "").strip().lower() == name_l:
                return it.get("Id")
    return None

def create_playlist(http, user_id: str, name: str, is_public: bool = False) -> Optional[str]:
    norm = (name or "").strip()
    if not norm:
        return None
    try:
        r = http.post("/Playlists", params={"Name": norm})
        if getattr(r, "status_code", 0) in (200, 201, 204):
            try:
                data = r.json() or {}
            except Exception:
                data = {}
            pid = data.get("Id") or data.get("PlaylistId") or data.get("id")
            if pid: return str(pid)
    except Exception:
        pass
    pid = find_playlist_id_by_name(http, user_id, norm)
    return str(pid) if pid else None

def get_playlist_items(http, playlist_id: str, start: int = 0, limit: int = 100) -> Dict[str, Any]:
    q = {"StartIndex": max(0, int(start)), "Limit": max(1, int(limit)), "Fields": "ProviderIds,ProductionYear,Type", "EnableUserData": False}
    r = http.get(f"/Playlists/{playlist_id}/Items", params=q)
    if getattr(r, "status_code", 0) != 200: return {"Items": [], "TotalRecordCount": 0}
    data = r.json() or {}
    data.setdefault("Items", [])
    data.setdefault("TotalRecordCount", len(data["Items"]))
    return data

def playlist_add_items(http, playlist_id: str, user_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids: return True
    r = http.post(f"/Playlists/{playlist_id}/Items", params={"Ids": ids, "UserId": user_id})
    return getattr(r, "status_code", 0) in (200, 204)

def playlist_remove_entries(http, playlist_id: str, entry_ids: Iterable[str]) -> bool:
    eids = ",".join(str(x) for x in entry_ids if x)
    if not eids: return True
    r = http.delete(f"/Playlists/{playlist_id}/Items", params={"EntryIds": eids})
    return getattr(r, "status_code", 0) in (200, 204)

# --- collections (BoxSets)
def find_seed_item_id(http, user_id: str) -> Optional[str]:
    for t in ("Movie", "Series"):
        r = http.get(f"/Users/{user_id}/Items", params={"IncludeItemTypes": t, "Recursive": True, "Limit": 1})
        if getattr(r, "status_code", 0) == 200:
            arr = (r.json() or {}).get("Items") or []
            if arr:
                iid = arr[0].get("Id")
                if iid: return str(iid)
    return None

def _collections_parent_ids(http, user_id: str) -> List[str]:
    out: List[str] = []
    try:
        r = http.get(f"/Users/{user_id}/Views")
        if getattr(r, "status_code", 0) == 200:
            for it in (r.json() or {}).get("Items", []):
                t = (it.get("Type") or "").strip()
                ct = (it.get("CollectionType") or "").strip().lower()
                if t == "CollectionFolder" or ct == "boxsets":
                    vid = it.get("Id")
                    if vid: out.append(str(vid))
    except Exception:
        pass
    return out

def _match_name_eq(items: Iterable[Mapping[str, Any]], name: str) -> Optional[str]:
    want = (name or "").strip().lower()
    for it in items or []:
        nm = (it.get("Name") or "").strip().lower()
        if nm == want:
            iid = it.get("Id")
            if iid: return str(iid)
    return None

def find_collection_id_by_name(http, user_id: str, name: str) -> Optional[str]:
    norm = (name or "").strip()
    if not norm: return None
    q1 = {"IncludeItemTypes": "BoxSet", "Recursive": True, "SearchTerm": norm, "Limit": 50}
    r1 = http.get(f"/Users/{user_id}/Items", params=q1)
    if getattr(r1, "status_code", 0) == 200:
        hit = _match_name_eq((r1.json() or {}).get("Items", []), norm)
        if hit: return hit
    for pid in _collections_parent_ids(http, user_id):
        q2 = {"IncludeItemTypes": "BoxSet", "Recursive": True, "ParentId": pid, "Limit": 200}
        r2 = http.get(f"/Users/{user_id}/Items", params=q2)
        if getattr(r2, "status_code", 0) == 200:
            hit = _match_name_eq((r2.json() or {}).get("Items", []), norm)
            if hit: return hit
    try:
        q3 = {"IncludeItemTypes": "BoxSet", "Recursive": True, "SearchTerm": norm, "Limit": 50}
        r3 = http.get("/Items", params=q3)
        if getattr(r3, "status_code", 0) == 200:
            hit = _match_name_eq((r3.json() or {}).get("Items", []), norm)
            if hit: return hit
    except Exception:
        pass
    return None

def create_collection(http, name: str, initial_ids: Optional[Iterable[str]] = None) -> Optional[str]:
    norm = (name or "").strip()
    if not norm:
        return None
    ids = ",".join(str(x) for x in (initial_ids or []) if x)
    try:
        params = {"Name": norm}
        if ids: params["Ids"] = ids
        r = http.post("/Collections", params=params)
        if getattr(r, "status_code", 0) in (200, 201, 204):
            try:
                data = r.json() or {}
            except Exception:
                data = {}
            cid = data.get("Id") or data.get("id")
            if cid:
                return str(cid)
    except Exception:
        pass
    try:
        r2 = http.get("/Items", params={"IncludeItemTypes": "BoxSet", "Recursive": True, "SearchTerm": norm, "Limit": 25})
        if getattr(r2, "status_code", 0) == 200:
            for it in (r2.json() or {}).get("Items", []):
                if (it.get("Name") or "").strip().lower() == norm.lower():
                    iid = it.get("Id")
                    if iid:
                        return str(iid)
    except Exception:
        pass
    return None

def get_collection_items(http, user_id: str, collection_id: str) -> Dict[str, Any]:
    q = {
        "IncludeItemTypes": "Movie,Series",
        "ParentId": collection_id,
        "Recursive": False,
        "Fields": "ProviderIds,ProductionYear,Type",
        "EnableTotalRecordCount": True,
        "Limit": 10000,
    }
    r = http.get(f"/Users/{user_id}/Items", params=q)
    if getattr(r, "status_code", 0) != 200:
        return {"Items": [], "TotalRecordCount": 0}
    data = r.json() or {}
    data.setdefault("Items", [])
    data.setdefault("TotalRecordCount", len(data["Items"]))
    return data

def collection_add_items(http, collection_id: str, item_ids: Iterable[str]) -> bool:
    ids = [str(x) for x in item_ids if x]
    if not ids: return True
    try:
        r = http.post(f"/Collections/{collection_id}/Items", params={"Ids": ",".join(ids)})
        if getattr(r, "status_code", 0) in (200, 204): return True
    except Exception:
        pass
    try:
        r2 = http.post(f"/Collections/{collection_id}/Items", json={"Ids": ids})
        if getattr(r2, "status_code", 0) in (200, 204): return True
    except Exception:
        pass
    return False

def collection_remove_items(http, collection_id: str, item_ids: Iterable[str]) -> bool:
    ids = [str(x) for x in item_ids if x]
    if not ids: return True
    try:
        r = http.delete(f"/Collections/{collection_id}/Items", params={"Ids": ",".join(ids)})
        if getattr(r, "status_code", 0) in (200, 204): return True
    except Exception:
        pass
    try:
        r2 = http.post(f"/Collections/{collection_id}/Items/Delete", params={"Ids": ",".join(ids)})
        if getattr(r2, "status_code", 0) in (200, 204): return True
    except Exception:
        pass
    try:
        r3 = http.post(f"/Collections/{collection_id}/Items/Delete", json={"Ids": ids})
        if getattr(r3, "status_code", 0) in (200, 204): return True
    except Exception:
        pass
    return False

# --- misc writes
def mark_favorite(http, user_id: str, item_id: str, flag: bool) -> bool:
    path = f"/Users/{user_id}/FavoriteItems/{item_id}"
    r = http.post(path) if flag else http.delete(path)
    ok = getattr(r, "status_code", 0) in (200, 204)
    if not ok:
        body_snip = "no-body"
        try:
            bj = r.json(); s = json.dumps(bj, ensure_ascii=False); body_snip = (s[:200] + "…") if len(s) > 200 else s
        except Exception:
            try:
                t = r.text() if callable(getattr(r, "text", None)) else getattr(r, "text", "")
                s = str(t or ""); body_snip = (s[:200] + "…") if len(s) > 200 else s
            except Exception:
                body_snip = "no-body"
        _log_summary(f"favorite write failed user={user_id} item={item_id} status={getattr(r,'status_code',None)} body={body_snip}")
    return ok

# ---- FLEX update_userdata: supports BOTH old and new callsites ----------------
def update_userdata(*args, **kwargs) -> bool:
    """
    Compatible wrapper:
    1) update_userdata(adapter, item_id_or_minimal, **fields)
    2) update_userdata(adapter, item_id_or_minimal, payload=<Mapping>)
    3) update_userdata(http, user_id, item_id, payload=<Mapping>)
    4) update_userdata(http, user_id, item_id, **fields)
    """
    try:
        # Case 1/2: first arg is adapter
        if args and hasattr(args[0], "client") and hasattr(getattr(args[0], "cfg", None), "user_id"):
            adapter = args[0]
            http, uid = adapter.client, adapter.cfg.user_id
            target = args[1] if len(args) > 1 else None
            if target is None: return False
            iid = str(target) if isinstance(target, str) else resolve_item_id(adapter, target)
            if not iid: return False
            # payload can be provided via kwargs or as explicit 'payload' kw
            payload = dict(kwargs.pop("payload", {}) or {})
            payload.update({k: v for k, v in kwargs.items() if v is not None})
            r = http.post(f"/Users/{uid}/Items/{iid}/UserData", json=payload)
            return getattr(r, "status_code", 0) in (200, 204)

        # Case 3/4: (http, user_id, item_id, payload|**fields)
        if len(args) >= 3:
            http, uid, iid = args[0], str(args[1]), str(args[2])
            payload = {}
            if len(args) >= 4 and isinstance(args[3], Mapping):
                payload = dict(args[3])
            payload.update({k: v for k, v in kwargs.items() if v is not None})
            r = http.post(f"/Users/{uid}/Items/{iid}/UserData", json=payload)
            return getattr(r, "status_code", 0) in (200, 204)

    except Exception:
        return False
    return False

# Backwards alias if something imports the alt name
def update_user_data(*args, **kwargs) -> bool:
    return update_userdata(*args, **kwargs)

# --- resolver (movie/show/episode) with direct AnyProviderIdEquals first ------

def _pick_from_candidates(cands: List[Dict[str, Any]], *, want_type: Optional[str], want_year: Optional[int]) -> Optional[str]:
    def score_val(row: Dict[str, Any]) -> Tuple[int, int, str]:
        t = (row.get("Type") or "").strip()
        y = row.get("ProductionYear")
        s = 0
        if want_type:
            if want_type == "movie" and t == "Movie": s += 3
            if want_type in ("show", "series") and t == "Series": s += 3
            if want_type == "episode" and t == "Episode": s += 3
        if isinstance(want_year, int) and isinstance(y, int) and abs(y - want_year) <= 1: s += 1
        if (row.get("ProviderIds") or {}): s += 1
        iid = str(row.get("Id") or "")
        return (-s, len(iid), iid)
    if not cands:
        return None
    best = min(cands, key=score_val)
    iid = best.get("Id")
    return str(iid) if iid and not looks_like_bad_id(iid) else None

def _direct_query_by_pairs(http, uid: str, pairs: List[str], include_types: str, scope: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    if not pairs: return []
    q = {
        "AnyProviderIdEquals": ",".join(pairs),
        "IncludeItemTypes": include_types,
        "Recursive": True,
        "Fields": "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId,ParentId,Name",
        "Limit": 50,
        "UserId": uid,
    }
    q.update(scope or {})
    try:
        r = http.get(f"/Users/{uid}/Items", params=q)
        if getattr(r, "status_code", 0) != 200:
            return []
        body = r.json() or {}
        return body.get("Items") or []
    except Exception:
        return []

def resolve_item_id(adapter, it: Mapping[str, Any]) -> Optional[str]:
    http, uid = adapter.client, adapter.cfg.user_id
    ids = dict((it.get("ids") or {}))

    # Per-run memo (cheap)
    try:
        memo: Dict[str, Optional[str]] = getattr(adapter, "_emby_resolve_cache")
    except Exception:
        memo = {}
        try: setattr(adapter, "_emby_resolve_cache", memo)
        except Exception: pass

    try:
        import json as _json
        mk = _json.dumps({"ids": ids, "t": it.get("type"), "ti": it.get("title"),
                          "y": it.get("year"), "s": it.get("season"),
                          "e": it.get("episode"), "st": it.get("series_title"),
                          "sid": it.get("show_ids")}, sort_keys=True)
    except Exception:
        mk = f"{it.get('type')}|{it.get('title')}|{it.get('year')}|{it.get('season')}|{it.get('episode')}|{it.get('series_title')}|{tuple(sorted((ids or {}).items()))}|{tuple(sorted(((it.get('show_ids') or {}) or {}).items()))}"

    if mk in memo and memo[mk]:
        return memo[mk]

    em = ids.get("emby")
    if em and not looks_like_bad_id(em):
        _log_detail(f"resolve direct emby id -> {em}")
        memo[mk] = str(em)
        return str(em)

    t = _norm_type(it.get("type"))
    title = (it.get("title") or "").strip()
    year = it.get("year")
    season = it.get("season")
    episode = it.get("episode")
    series_title = (it.get("series_title") or "").strip()
    series_ids = dict(it.get("show_ids") or {})  # prefer series IDs for episodes

    prio = _merged_guid_priority(adapter)
    ep_pairs = all_ext_pairs(ids, prio)
    series_pairs = all_ext_pairs(series_ids, prio) if series_ids else []
    scope = emby_scope_any(adapter.cfg)

    # ---- 1) Direct hit via AnyProviderIdEquals (Emby-native) -----------------
    if t == "movie":
        rows = _direct_query_by_pairs(http, uid, ep_pairs, "Movie", scope)
        if rows:
            iid = _pick_from_candidates([r for r in rows if (r.get("Type") or "") == "Movie"], want_type="movie", want_year=year)
            if iid:
                _log_detail(f"resolve (movie) direct AnyProviderIdEquals -> {iid}")
                memo[mk] = iid
                return iid

    elif t in ("show", "series"):
        rows = _direct_query_by_pairs(http, uid, ep_pairs or series_pairs, "Series", scope)
        if rows:
            iid = _pick_from_candidates([r for r in rows if (r.get("Type") or "") == "Series"], want_type="show", want_year=year)
            if iid:
                _log_detail(f"resolve (series) direct AnyProviderIdEquals -> {iid}")
                memo[mk] = iid
                return iid

    elif t == "episode":
        # Try episodes directly first
        rows = _direct_query_by_pairs(http, uid, ep_pairs, "Episode,Series", scope)
        # exact episode?
        ep_row = next((r for r in rows if (r.get("Type") or "") == "Episode" and
                       int(r.get("ParentIndexNumber") or -1) == int(season or -999) and
                       int(r.get("IndexNumber") or -1) == int(episode or -999)), None)
        if ep_row and ep_row.get("Id"):
            iid = str(ep_row["Id"])
            memo[mk] = iid
            _log_detail(f"resolve (episode) direct AnyProviderIdEquals -> {iid}")
            return iid
        # got Series? fetch episodes
        ser_row = next((r for r in rows if (r.get("Type") or "") == "Series"), None)
        if not ser_row and series_pairs:
            rows2 = _direct_query_by_pairs(http, uid, series_pairs, "Series", scope)
            ser_row = next((r for r in rows2 if (r.get("Type") or "") == "Series"), None)
        if ser_row and season is not None and episode is not None:
            sid = ser_row.get("Id")
            if sid:
                eps = get_series_episodes(http, uid, sid, start=0, limit=500).get("Items") or []
                for ep in eps:
                    if int(ep.get("ParentIndexNumber") or -1) == int(season) and int(ep.get("IndexNumber") or -1) == int(episode):
                        iid = str(ep.get("Id") or "")
                        if iid:
                            memo[mk] = iid
                            _log_detail(f"resolve (episode) via series->S/E -> {iid}")
                            return iid

    # ---- 2) provider_index fallback ------------------------------------------
    idx = provider_index(adapter)

    if t == "movie":
        for pref in ep_pairs:
            cands = idx.get(pref) or []
            iid = _pick_from_candidates(cands, want_type="movie", want_year=year)
            if iid:
                _log_detail(f"resolve hit (movie index) pref={pref} -> item_id={iid}")
                memo[mk] = iid
                return iid

    if t in ("show", "series"):
        for pref in ep_pairs:
            cands = [row for row in (idx.get(pref) or []) if (row.get("Type") or "").strip() == "Series"]
            iid = _pick_from_candidates(cands, want_type="show", want_year=year)
            if iid:
                _log_detail(f"resolve series (index) pref={pref} -> {iid}")
                memo[mk] = iid
                return iid

    if t == "episode":
        series_row: Optional[Dict[str, Any]] = None
        if series_pairs:
            series_row = find_series_in_index(adapter, series_pairs)
            if series_row: _log_detail("series hit via show_ids in provider index")
        if not series_row and ep_pairs:
            maybe = find_series_in_index(adapter, ep_pairs)
            if maybe:
                series_row = maybe
                _log_detail("series hit via episode ids (weak)")
        if series_row and season is not None and episode is not None:
            sid = series_row.get("Id")
            if sid:
                eps = get_series_episodes(http, uid, sid, start=0, limit=10000)
                for row in eps.get("Items") or []:
                    s = row.get("ParentIndexNumber"); e = row.get("IndexNumber")
                    if isinstance(s, int) and isinstance(e, int) and s == int(season) and e == int(episode):
                        iid = row.get("Id")
                        if iid and not looks_like_bad_id(iid):
                            _log_detail(f"resolve episode S{int(season):02d}E{int(episode):02d} -> {iid}")
                            memo[mk] = str(iid)
                            return str(iid)

    # ---- 3) text search fallback ---------------------------------------------
    def _items(resp) -> List[Mapping[str, Any]]:
        try:
            body = resp.json() or {}
            return body.get("Items") or []
        except Exception:
            _log_detail("safe-json: treating response as empty")
            return []

    if t == "movie" and title:
        try:
            q = {"UserId": uid, "Recursive": True, "IncludeItemTypes": "Movie",
                 "SearchTerm": title, "Fields": "ProviderIds,ProductionYear,Type", "Limit": 50}
            q.update(emby_scope_any(adapter.cfg))
            r = http.get("/Items", params=q)
            t_l = title.lower()
            cand: List[Mapping[str, Any]] = []
            for row in _items(r):
                if (row.get("Type") or "") != "Movie":
                    continue
                nm = (row.get("Name") or "").strip().lower()
                yr = row.get("ProductionYear")
                if nm == t_l and ((year is None) or (isinstance(yr, int) and abs(yr - year) <= 1)):
                    cand.append(row)
            cand.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
            for row in cand:
                iid = row.get("Id")
                if iid and not looks_like_bad_id(iid):
                    _log_summary(f"resolve movie '{title}' ({year}) -> {iid}")
                    memo[mk] = str(iid)
                    return str(iid)
        except Exception:
            pass

    if t in ("show", "series") and title:
        try:
            q = {"UserId": uid, "Recursive": True, "IncludeItemTypes": "Series",
                 "SearchTerm": title, "Fields": "ProviderIds,ProductionYear,Type", "Limit": 50}
            q.update(emby_scope_any(adapter.cfg))
            r = http.get("/Items", params=q)
            title_lc = title.lower()
            cand: List[Mapping[str, Any]] = []
            for row in _items(r):
                if (row.get("Type") or "") != "Series":
                    continue
                nm = (row.get("Name") or "").strip().lower()
                yr = row.get("ProductionYear")
                if nm == title_lc and ((year is None) or (isinstance(yr, int) and abs(yr - year) <= 1)):
                    cand.append(row)
            cand.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
            for row in cand:
                iid = row.get("Id")
                if iid and not looks_like_bad_id(iid):
                    _log_summary(f"resolve series '{title}' ({year}) -> {iid}")
                    memo[mk] = str(iid)
                    return str(iid)
        except Exception:
            pass

    if t == "episode" and title:
        try:
            q = {"UserId": uid, "Recursive": True, "IncludeItemTypes": "Episode",
                 "SearchTerm": title, "Fields": "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId",
                 "Limit": 50}
            q.update(emby_scope_any(adapter.cfg))
            r = http.get("/Items", params=q)
            t_l = title.lower()
            for row in _items(r):
                if (row.get("Type") or "") != "Episode": continue
                nm = (row.get("Name") or "").strip().lower()
                s = row.get("ParentIndexNumber"); e = row.get("IndexNumber")
                if nm == t_l and ((season is None) or s == season) and ((episode is None) or e == episode):
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _log_detail(f"resolve episode '{title}' S{season}E{episode} -> {iid}")
                        memo[mk] = str(iid)
                        return str(iid)
        except Exception:
            pass

    _log_detail(f"resolve miss: type={t} title='{title}' year={year} S{season}E{episode} series='{series_title}'")
    return None

# --- utils
def chunked(it: Iterable[Any], n: int) -> Iterable[List[Any]]:
    n = max(1, int(n)); buf: List[Any] = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf; buf = []
    if buf: yield buf

def sleep_ms(ms: int) -> None:
    m = int(ms or 0)
    if m > 0: time.sleep(m / 1000.0)

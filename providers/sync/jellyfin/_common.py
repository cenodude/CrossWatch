# /providers/sync/jellyfin/_common.py
from __future__ import annotations
from typing import Any, Dict, Mapping, Optional, Iterable, List, Tuple, Union
import os, re, time, json

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

_DEF_TYPES = {"movie", "show", "episode"}
_IMDB_PAT  = re.compile(r"(?:tt)?(\d{5,9})$")
_NUM_PAT   = re.compile(r"(\d{1,10})$")
_BAD_NUM   = re.compile(r"^\d{13,}$")

CfgLike = Union[Mapping[str, Any], object]

# --- logging (quiet by default) ----------------------------------------------
def _debug_level() -> str:
    env = (os.environ.get("CW_JELLYFIN_DEBUG_LEVEL") or "").strip().lower()
    if env in ("2", "v", "verbose"): return "verbose"
    if env in ("1", "s", "summary", "true", "on"): return "summary"
    if os.environ.get("CW_JELLYFIN_DEBUG") or os.environ.get("CW_DEBUG"): return "summary"
    return "off"

def _log_summary(msg: str) -> None:
    if _debug_level() in ("summary", "verbose"):
        print(f"[JELLYFIN:common] {msg}")

def _log_detail(msg: str) -> None:
    if _debug_level() == "verbose":
        print(f"[JELLYFIN:common] {msg}")

# legacy alias
def _log(msg: str) -> None:
    _log_summary(msg)
    
## --- cfg helpers ----Library selection------------------------------------------
def _as_list_str(v: Any) -> List[str]:
    if v is None: return []
    if isinstance(v, (list, tuple, set)): it = v
    else: it = [v]
    out: List[str] = []
    seen = set()
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

def jf_library_scope(cfg: CfgLike, feature: str) -> Dict[str, Any]:
    """
    Build Jellyfin item-query scope from config.
    """
    jf = _pluck(cfg, "jellyfin") or cfg  # allow passing full cfg or jf-subdict
    libs = _as_list_str(_pluck(jf, feature, "libraries"))
    if not libs:
        # Fallbacks for dataclass-style configs (e.g. cfg.history_libraries / cfg.ratings_libraries)
        libs_attr = _as_list_str(getattr(cfg, f"{feature}_libraries", None))
        if libs_attr:
            libs = libs_attr
        else:
            sub = getattr(cfg, feature, None)
            if sub is not None:
                libs = _as_list_str(getattr(sub, "libraries", None))
    if not libs:
        return {}
    if len(libs) == 1:
        return {"parentId": libs[0], "recursive": True}
    return {"ancestorIds": libs, "recursive": True}

def with_jf_scope(params: Mapping[str, Any], cfg: CfgLike, feature: str) -> Dict[str, Any]:
    out = dict(params or {})
    out.update(jf_library_scope(cfg, feature))
    return out

def jf_scope_history(cfg: CfgLike) -> Dict[str, Any]:
    return jf_library_scope(cfg, "history")

def jf_scope_ratings(cfg: CfgLike) -> Dict[str, Any]:
    return jf_library_scope(cfg, "ratings")

def jf_scope_any(cfg: CfgLike) -> Dict[str, Any]:
    """Union of history + ratings libraries. Falls back gracefully."""
    # Try mapping style: cfg['jellyfin'][feature]['libraries']
    jf_map = _pluck(cfg, "jellyfin") or cfg
    libs_h = _as_list_str(_pluck(jf_map, "history", "libraries"))
    libs_r = _as_list_str(_pluck(jf_map, "ratings", "libraries"))
    libs: List[str] = []
    seen = set()
    for x in (libs_h + libs_r):
        if x and x not in seen:
            seen.add(x); libs.append(x)
    # Try dataclass/object attributes too (e.g., cfg.history_libraries / cfg.ratings_libraries)
    if not libs and not isinstance(cfg, Mapping):
        libs_h2 = _as_list_str(getattr(cfg, "history_libraries", None))
        libs_r2 = _as_list_str(getattr(cfg, "ratings_libraries", None))
        for x in (libs_h2 + libs_r2):
            if x and x not in seen:
                seen.add(x); libs.append(x)
        # Or nested objects: cfg.history.libraries / cfg.ratings.libraries
        hist_obj = getattr(cfg, "history", None)
        rate_obj = getattr(cfg, "ratings", None)
        if hasattr(hist_obj, "libraries"):
            for x in _as_list_str(getattr(hist_obj, "libraries", None)):
                if x and x not in seen:
                    seen.add(x); libs.append(x)
        if hasattr(rate_obj, "libraries"):
            for x in _as_list_str(getattr(rate_obj, "libraries", None)):
                if x and x not in seen:
                    seen.add(x); libs.append(x)
    if not libs:
        return {}
    if len(libs) == 1:
        return {"parentId": libs[0], "recursive": True}
    return {"ancestorIds": libs, "recursive": True}


# --- type & id helpers --------------------------------------------------------
def _norm_type(t: Any) -> str:
    x = (str(t or "").strip().lower())
    if x in ("movies", "movie"): return "movie"
    if x in ("shows", "show", "series", "tv"): return "show"
    if x in ("episode", "episodes"): return "episode"
    return "movie"

def looks_like_bad_id(iid: Any) -> bool:
    s = str(iid or "")
    return bool(_BAD_NUM.match(s))

def _ids_from_provider_ids(pids: Optional[Mapping[str, Any]]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(pids, Mapping): return out
    low = {str(k).lower():(v if v is not None else "") for k, v in pids.items()}
    v = low.get("imdb")
    if v is not None:
        m = _IMDB_PAT.search(str(v).strip());  out["imdb"] = f"tt{m.group(1)}" if m else out.get("imdb")
    v = low.get("tmdb")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip());   out["tmdb"] = m.group(1) if m else out.get("tmdb")
    v = low.get("tvdb")
    if v is not None:
        m = _NUM_PAT.search(str(v).strip());   out["tvdb"] = m.group(1) if m else out.get("tvdb")
    jf = low.get("jellyfin")
    if jf: out["jellyfin"] = str(jf)
    return out

def normalize(obj: Mapping[str, Any]) -> Dict[str, Any]:
    if isinstance(obj, Mapping) and "ids" in obj and "type" in obj:
        return id_minimal(obj)
    t = _norm_type(obj.get("Type") or obj.get("BaseItemKind") or obj.get("type"))
    title = (obj.get("Name") or obj.get("title") or "").strip() or None
    year = obj.get("ProductionYear") if isinstance(obj.get("ProductionYear"), int) else obj.get("year")
    pids = obj.get("ProviderIds") if isinstance(obj.get("ProviderIds"), Mapping) else (obj.get("ids") or {})
    ids = {k: v for k, v in _ids_from_provider_ids(pids).items() if v}
    jf_id = obj.get("Id") or (pids.get("jellyfin") if isinstance(pids, Mapping) else None)
    if jf_id: ids["jellyfin"] = str(jf_id)
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

# --- provider index -----------------------------------------------------------
def build_provider_index(adapter) -> Dict[str, List[Dict[str, Any]]]:
    cache = getattr(adapter, "_provider_index_cache", None)
    if isinstance(cache, dict) and cache:
        return cache

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
        params.update(jf_scope_any(adapter.cfg))
        r = http.get(
            f"/Users/{uid}/Items",
            params=params,
        )
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
    setattr(adapter, "_provider_index_cache", out)
    return out

def find_series_in_index(adapter, pairs: Iterable[str]) -> Optional[Dict[str, Any]]:
    idx = build_provider_index(adapter)
    for pref in pairs or []:
        rows = idx.get(pref) or []
        for row in rows:
            if (row.get("Type") or "").strip() == "Series":
                return row
    return None

# --- series/episodes listing --------------------------------------------------
def get_series_episodes(http, user_id: str, series_id: str, start: int = 0, limit: int = 500) -> Dict[str, Any]:
    q = {
        "userId": user_id,
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

# --- playlists / collections --------------------------------------------------
def find_playlist_id_by_name(http, user_id: str, name: str) -> Optional[str]:
    q = {"userId": user_id, "includeItemTypes": "Playlist", "recursive": True, "SearchTerm": name}
    r = http.get(f"/Users/{user_id}/Items", params=q)
    if getattr(r, "status_code", 0) != 200: return None
    items = (r.json() or {}).get("Items") or []
    name_l = (name or "").strip().lower()
    for it in items:
        if (it.get("Name") or "").strip().lower() == name_l:
            return it.get("Id")
    if not items:
        q = {"userId": user_id, "includeItemTypes": "Playlist", "recursive": True}
        r = http.get(f"/Users/{user_id}/Items", params=q)
        if getattr(r, "status_code", 0) != 200: return None
        for it in (r.json() or {}).get("Items") or []:
            if (it.get("Name") or "").strip().lower() == name_l:
                return it.get("Id")
    return None

def create_playlist(http, user_id: str, name: str, is_public: bool = False) -> Optional[str]:
    body = {"Name": name, "UserId": user_id, "IsPublic": bool(is_public)}
    r = http.post("/Playlists", json=body)
    if getattr(r, "status_code", 0) not in (200, 204): return None
    data = r.json() or {}
    return data.get("Id") or data.get("PlaylistId") or data.get("id")

def get_playlist_items(http, playlist_id: str, start: int = 0, limit: int = 100) -> Dict[str, Any]:
    q = {"startIndex": max(0, int(start)), "limit": max(1, int(limit)), "Fields": "ProviderIds,ProductionYear,Type", "EnableUserData": False}
    r = http.get(f"/Playlists/{playlist_id}/Items", params=q)
    if getattr(r, "status_code", 0) != 200: return {"Items": [], "TotalRecordCount": 0}
    data = r.json() or {}
    data.setdefault("Items", [])
    data.setdefault("TotalRecordCount", len(data["Items"]))
    return data

def playlist_add_items(http, playlist_id: str, user_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids: return True
    r = http.post(f"/Playlists/{playlist_id}/Items", params={"ids": ids, "userId": user_id})
    return getattr(r, "status_code", 0) in (200, 204)

def playlist_remove_entries(http, playlist_id: str, entry_ids: Iterable[str]) -> bool:
    eids = ",".join(str(x) for x in entry_ids if x)
    if not eids: return True
    r = http.delete(f"/Playlists/{playlist_id}/Items", params={"entryIds": eids})
    return getattr(r, "status_code", 0) in (200, 204)

def find_collection_id_by_name(http, user_id: str, name: str) -> Optional[str]:
    try:
        r = http.get(f"/Users/{user_id}/Items", params={
            "IncludeItemTypes": "BoxSet", "Recursive": True, "SearchTerm": name, "Limit": 50
        })
        if getattr(r, "status_code", 0) != 200: return None
        items = (r.json() or {}).get("Items") or []
        name_lc = (name or "").strip().lower()
        for row in items:
            if (row.get("Type") == "BoxSet") and (str(row.get("Name") or "").strip().lower() == name_lc):
                return str(row.get("Id"))
        r2 = http.get(f"/Users/{user_id}/Items", params={"IncludeItemTypes": "BoxSet", "Recursive": True})
        if getattr(r2, "status_code", 0) != 200: return None
        for row in (r2.json() or {}).get("Items") or []:
            if (row.get("Type") == "BoxSet") and (str(row.get("Name") or "").strip().lower() == name_lc):
                return str(row.get("Id"))
    except Exception:
        pass
    return None

def create_collection(http, name: str) -> Optional[str]:
    try:
        r = http.post("/Collections", params={"Name": name})
        if getattr(r, "status_code", 0) in (200, 201, 204):
            body = r.json() or {}
            cid = body.get("Id") or body.get("id")
            return str(cid) if cid else None
    except Exception:
        pass
    return None

def get_collection_items(http, user_id: str, collection_id: str) -> Dict[str, Any]:
    try:
        r = http.get(f"/Users/{user_id}/Items", params={
            "ParentId": collection_id, "Recursive": True, "IncludeItemTypes": "Movie,Series",
            "Fields": "ProviderIds,ProductionYear,Type", "EnableTotalRecordCount": True, "Limit": 10000
        })
        if getattr(r, "status_code", 0) != 200: return {"Items": [], "TotalRecordCount": 0}
        data = r.json() or {}
        data.setdefault("Items", [])
        data.setdefault("TotalRecordCount", len(data["Items"]))
        return data
    except Exception:
        return {"Items": [], "TotalRecordCount": 0}

def collection_add_items(http, collection_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids: return True
    try:
        r = http.post(f"/Collections/{collection_id}/Items", params={"Ids": ids})
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False

def collection_remove_items(http, collection_id: str, item_ids: Iterable[str]) -> bool:
    ids = ",".join(str(x) for x in item_ids if x)
    if not ids: return True
    try:
        r = http.delete(f"/Collections/{collection_id}/Items", params={"Ids": ids})
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False

# --- misc writes --------------------------------------------------------------
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

def update_userdata(http, user_id: str, item_id: str, payload: Mapping[str, Any]) -> bool:
    try:
        r = http.post(f"/Items/{item_id}/UserData", params={"userId": user_id}, json=dict(payload))
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False

# --- resolver (movie/show/episode) -------------------------------------------
def _pick_from_candidates(cands: List[Dict[str, Any]], *, want_type: Optional[str], want_year: Optional[int]) -> Optional[str]:
    # Deterministic winner: highest score, then shortest Id, then lexicographically smallest Id.
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
        return (-s, len(iid), iid)  # min() picks highest s via -s, then stable Id
    if not cands:
        return None
    best = min(cands, key=score_val)
    iid = best.get("Id")
    return str(iid) if iid and not looks_like_bad_id(iid) else None

def resolve_item_id(adapter, it: Mapping[str, Any]) -> Optional[str]:
    http, uid = adapter.client, adapter.cfg.user_id
    ids = dict((it.get("ids") or {}))

    jf = ids.get("jellyfin")
    if jf and not looks_like_bad_id(jf):
        _log_detail(f"resolve direct jellyfin id -> {jf}")
        return str(jf)

    t = _norm_type(it.get("type"))
    title = (it.get("title") or "").strip()
    year = it.get("year")
    season = it.get("season")
    episode = it.get("episode")
    series_title = (it.get("series_title") or "").strip()

    prio = guid_priority_from_cfg(getattr(getattr(adapter, "cfg", None), "watchlist_guid_priority", None))
    pairs = all_ext_pairs(ids, prio)
    idx = build_provider_index(adapter)

    # Movies
    if t == "movie":
        for pref in pairs:
            cands = idx.get(pref) or []
            iid = _pick_from_candidates(cands, want_type="movie", want_year=year)
            if iid:
                _log_detail(f"resolve hit (movie index) pref={pref} -> item_id={iid}")
                return iid
        if title:
            try:
                q = { "userId": uid, "recursive": True, "includeItemTypes": "Movie", "SearchTerm": title, "Fields": "ProviderIds,ProductionYear,Type", "Limit": 50 }
                q.update(jf_scope_any(adapter.cfg))
                r = http.get("/Items", params=q)
                t_l = title.lower()
                cand: List[Mapping[str, Any]] = []
                for row in (r.json() or {}).get("Items") or []:
                    if (row.get("Type") or "") != "Movie": continue
                    nm = (row.get("Name") or "").strip().lower()
                    yr = row.get("ProductionYear")
                    if nm == t_l and ((year is None) or (isinstance(yr, int) and abs(yr - year) <= 1)):
                        cand.append(row)
                cand.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
                for row in cand:
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _log_summary(f"resolve movie '{title}' ({year}) -> {iid}")
                        return str(iid)
            except Exception:
                pass
        _log_detail(f"resolve miss (movie): '{title}' ({year})")
        return None

    # Shows (Series)
    if t in ("show", "series"):
        for pref in pairs:
            cands = [row for row in (idx.get(pref) or []) if (row.get("Type") or "").strip() == "Series"]
            iid = _pick_from_candidates(cands, want_type="show", want_year=year)
            if iid:
                _log_summary(f"resolve series '{title or pref}' ({year}) -> {iid}")
                return iid
        if title:
            try:
                q = { "userId": uid, "recursive": True, "includeItemTypes": "Series", "SearchTerm": title, "Fields": "ProviderIds,ProductionYear,Type", "Limit": 50 }
                q.update(jf_scope_any(adapter.cfg))
                r = http.get("/Items", params=q)
                title_lc = title.lower()
                cand: List[Mapping[str, Any]] = []
                for row in (r.json() or {}).get("Items") or []:
                    if (row.get("Type") or "") != "Series": continue
                    nm = (row.get("Name") or "").strip().lower()
                    yr = row.get("ProductionYear")
                    if nm == title_lc and ((year is None) or (isinstance(yr, int) and abs(yr - year) <= 1)):
                        cand.append(row)
                cand.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
                for row in cand:
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _log_summary(f"resolve series '{title}' ({year}) -> {iid}")
                        return str(iid)
            except Exception:
                pass
        _log_detail(f"resolve miss (series): '{title}' ({year})")
        return None

    # Episodes
    series_row: Optional[Dict[str, Any]] = None
    if pairs:
        series_row = find_series_in_index(adapter, pairs)
        if series_row:
            _log_detail("series hit via provider index")
    if not series_row and series_title:
        try:
            q = { "userId": uid, "recursive": True, "includeItemTypes": "Series", "SearchTerm": series_title, "Fields": "ProviderIds,ProductionYear,Type", "Limit": 50 }
            q.update(jf_scope_any(adapter.cfg))
            r = http.get("/Items", params=q)
            t_l = series_title.lower()
            cands = []
            for row in (r.json() or {}).get("Items") or []:
                if (row.get("Type") or "") != "Series": continue
                nm = (row.get("Name") or "").strip().lower()
                if nm == t_l:
                    cands.append(row)
            cands.sort(key=lambda x: 0 if (x.get("ProviderIds") or {}) else 1)
            if cands:
                series_row = cands[0]
                _log_detail(f"series hit via title '{series_title}'")
        except Exception:
            pass

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
                        return str(iid)

    if title:
        try:
            q = { "userId": uid, "recursive": True, "includeItemTypes": "Episode", "SearchTerm": title, "Fields": "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId", "Limit": 50 }
            q.update(jf_scope_any(adapter.cfg))
            r = http.get("/Items", params=q)
            t_l = title.lower()
            for row in (r.json() or {}).get("Items") or []:
                if (row.get("Type") or "") != "Episode": continue
                nm = (row.get("Name") or "").strip().lower()
                s = row.get("ParentIndexNumber"); e = row.get("IndexNumber")
                if nm == t_l and ((season is None) or s == season) and ((episode is None) or e == episode):
                    iid = row.get("Id")
                    if iid and not looks_like_bad_id(iid):
                        _log_detail(f"resolve episode '{title}' S{season}E{episode} -> {iid}")
                        return str(iid)
        except Exception:
            pass

    _log_detail(f"resolve miss (episode): title='{title}' series='{series_title}' S{season}E{episode}")
    return None

# --- utilities ----------------------------------------------------------------
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

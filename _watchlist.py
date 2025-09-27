# _watchlist.py
# Helpers for building merged watchlist view and performing deletes
# Refactoring project: watchlist.py (v0.1)

from __future__ import annotations

from typing import Any, Dict, Set
from pathlib import Path
import json

from cw_platform.config_base import CONFIG

# ---------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------
def _state_path() -> Path:
    return CONFIG / "watchlist_state.json"

HIDE_PATH: Path = CONFIG / "watchlist_hide.json"
# ---------------------------------------------------------------------
# Hide overlay (server-side; UI uses localStorage separately)
# ---------------------------------------------------------------------
def _load_hide_set() -> Set[str]:
    try:
        if HIDE_PATH.exists():
            data = json.loads(HIDE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {str(x) for x in data}
    except Exception:
        pass
    return set()

def _save_hide_set(hide: Set[str]) -> None:
    try:
        HIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        HIDE_PATH.write_text(json.dumps(sorted(hide)), encoding="utf-8")
    except Exception:
        pass

# ---------------------------------------------------------------------
# Generic helpers
# ---------------------------------------------------------------------
def _norm_type(x: str | None) -> str:
    t = (x or "").strip().lower()
    if t in {"tv", "show", "shows", "series", "season", "episode"}:
        return "tv"
    if t in {"movie", "movies", "film", "films"}:
        return "movie"
    return ""

def _rich_ids_score(item: dict | None) -> int:
    if not isinstance(item, dict):
        return 0
    ids = (item.get("ids") or {}) | {k: item.get(k) for k in ("tmdb","imdb","tvdb","trakt","slug")}
    return sum(1 for k in ("tmdb","imdb","tvdb","trakt","slug") if ids.get(k))

def _load_state_dict(path: Path) -> Dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_state_dict(path: Path, state: Dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _prov(state: Dict[str, Any], provider: str) -> Dict[str, Any]:
    return ((state.get("providers") or {}).get(provider.upper()) or {})

def _get_provider_items(state: Dict[str, Any], provider: str) -> Dict[str, Any]:
    prov = _prov(state, provider)
    wl = (((prov.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
    return wl or (prov.get("items") or {})

def _del_key_from_provider_items(state: Dict[str, Any], provider: str, key: str) -> bool:
    changed = False
    prov = _prov(state, provider)
    wl = (((prov.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
    if isinstance(wl, dict) and key in wl:
        wl.pop(key, None); changed = True
    items = prov.get("items") or {}
    if isinstance(items, dict) and key in items:
        items.pop(key, None); changed = True
    return changed

def _find_item_in_state(state: Dict[str, Any], key: str) -> Dict[str, Any]:
    for prov in ("PLEX", "SIMKL", "TRAKT", "JELLYFIN"):
        it = _get_provider_items(state, prov).get(key)
        if it:
            return dict(it)
    return {}

def _find_item_in_state_for_provider(state: Dict[str, Any], key: str, provider: str) -> Dict[str, Any]:
    it = _get_provider_items(state, provider).get(key)
    return dict(it) if it else {}

def _ids_from_key_or_item(key: str, item: Dict[str, Any]) -> Dict[str, Any]:
    ids = dict(item.get("ids") or {})
    pref, _, val = (key or "").partition(":")
    pref = pref.lower().strip()
    if pref in {"imdb","tmdb","tvdb","trakt","slug","jellyfin"} and val:
        ids.setdefault(pref, val)
    if "thetvdb" in ids and "tvdb" not in ids:
        ids["tvdb"] = ids.get("thetvdb")
    return {k: str(ids[k]) for k in ("simkl","imdb","tmdb","tvdb","trakt","slug","jellyfin") if ids.get(k)}

def _type_from_item_or_guess(item: Dict[str, Any], key: str) -> str:
    typ = (item.get("type") or "").lower()
    if typ in {"movie","tv","show","series"}:
        return "movie" if typ == "movie" else "tv"
    ids = item.get("ids") or {}
    if ids.get("tvdb") or ids.get("thetvdb"):
        return "tv"
    pref = (key or "").split(":", 1)[0].lower().strip()
    return "tv" if pref in {"tvdb","thetvdb"} else "movie"

_SIMKL_ID_KEYS = ("simkl","imdb","tmdb","tvdb","slug")

def _simkl_filter_ids(ids: Dict[str, Any]) -> Dict[str, Any]:
    return {k: str(v) for k, v in ids.items() if k in _SIMKL_ID_KEYS and v}

# ---------------------------------------------------------------------
# Plex GUID helpers
# ---------------------------------------------------------------------
def _pick_added(d: Dict[str, Any]) -> str | None:
    if not isinstance(d, dict):
        return None
    for k in ("added","added_at","addedAt","date_added","created_at","createdAt"):
        if d.get(k):
            return str(d[k])
    nested = d.get("dates") or d.get("meta") or d.get("attributes") or {}
    if isinstance(nested, dict):
        for k in ("added","added_at","created","created_at"):
            if nested.get(k):
                return str(nested[k])
    return None

def _iso_to_epoch(iso: str | None) -> int:
    if not iso:
        return 0
    try:
        s = str(iso).strip().replace("Z","+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0

def _norm_guid(g: str) -> tuple[str,str]:
    s = (g or "").strip()
    if not s:
        return "",""
    s = s.split("?",1)[0]
    if s.startswith("com.plexapp.agents."):
        try:
            rest = s.split("com.plexapp.agents.",1)[1]
            prov, ident = rest.split("://",1)
            return prov.lower().replace("thetvdb","tvdb"), ident.strip()
        except Exception:
            return "",""
    try:
        prov, ident = s.split("://",1)
        return prov.lower().replace("thetvdb","tvdb"), ident.strip()
    except Exception:
        return "",""

def _guid_variants_from_key_or_item(key: str, item: dict | None=None) -> list[str]:
    prov, _, ident = (key or "").partition(":")
    prov, ident = prov.lower().strip(), ident.strip()
    if not (prov and ident):
        ids = (item or {}).get("ids") or {}
        if ids.get("imdb"):   prov, ident = "imdb", str(ids["imdb"])
        elif ids.get("tmdb"): prov, ident = "tmdb", str(ids["tmdb"])
        elif ids.get("tvdb") or ids.get("thetvdb"):
            prov, ident = "tvdb", str(ids.get("tvdb") or ids.get("thetvdb"))
    if not (prov and ident):
        return []
    prov = "tvdb" if prov in {"thetvdb","tvdb"} else prov
    base = f"{prov}://{ident}"
    return [base, f"com.plexapp.agents.{prov}://{ident}", f"com.plexapp.agents.{prov}://{ident}?lang=en"]

def _extract_plex_identifiers(item: dict) -> tuple[str|None,str|None]:
    if not isinstance(item, dict):
        return None,None
    ids = item.get("ids") or {}
    guid = item.get("guid") or ids.get("guid") or (item.get("plex") or {}).get("guid")
    rating = item.get("ratingKey") or item.get("id") or ids.get("ratingKey") or (item.get("plex") or {}).get("ratingKey") or (item.get("plex") or {}).get("id")
    return (str(guid) if guid else None, str(rating) if rating else None)

# ---------------------------------------------------------------------
# Jellyfin helpers
# ---------------------------------------------------------------------
def _jf_base(cfg: dict[str,Any]) -> str:
    base = (cfg.get("server") or "").strip()
    if not base:
        raise RuntimeError("Jellyfin: missing 'server' in config")
    return base if base.endswith("/") else base+"/"

def _jf_headers(cfg: dict[str,Any]) -> dict[str,str]:
    token = (cfg.get("access_token") or cfg.get("api_key") or "").strip()
    dev = (cfg.get("device_id") or "CrossWatch").strip() or "CrossWatch"
    h = {
        "Accept":"application/json",
        "Content-Type":"application/json",
        "X-Emby-Authorization":f'MediaBrowser Client="CrossWatch", Device="WebUI", DeviceId="{dev}", Version="1.0.0"',
    }
    if token: h["X-Emby-Token"] = token
    return h

def _jf_require_user(cfg: dict[str,Any]) -> str:
    uid = (cfg.get("user_id") or "").strip()
    if not uid:
        raise RuntimeError("Jellyfin: missing 'user_id' in config")
    return uid

def _extract_jf_id(item: dict, key: str) -> str | None:
    ids = (item or {}).get("ids") or {}
    cand = ids.get("jellyfin") or item.get("jellyfinId") or item.get("jf_id") or item.get("Id") or item.get("id")
    if cand: return str(cand)
    pref, _, val = (key or "").partition(":")
    return val.strip() if pref.lower().strip()=="jellyfin" and val.strip() else None

def _jf_get(base: str, path: str, headers: dict[str,str], params: dict[str,Any]|None=None) -> dict[str,Any]:
    url = base + path.lstrip("/")
    if params:
        url += ("&" if "?" in url else "?") + urlencode({k:v for k,v in params.items() if v is not None})
    r = requests.get(url, headers=headers, timeout=45)
    if not r.ok:
        raise RuntimeError(f"Jellyfin GET {path} -> {r.status_code}: {getattr(r,'text','')}")
    try: j = r.json()
    except Exception: j = {}
    return j if isinstance(j,dict) else {}

def _jf_delete(base: str, path: str, headers: dict[str,str], params: dict[str,Any]|None=None) -> None:
    url = base + path.lstrip("/")
    if params:
        url += ("&" if "?" in url else "?") + urlencode({k:v for k,v in params.items() if v is not None})
    r = requests.delete(url, headers=headers, timeout=45)
    st = int(getattr(r,"status_code",0) or 0)
    if st not in (200,202,204,404) and not r.ok:
        raise RuntimeError(f"Jellyfin DELETE {path} -> {st}: {getattr(r,'text','')}")

def _jf_provider_tokens(ids: dict[str,Any]) -> list[str]:
    out = []
    for k in ("tmdb","imdb","tvdb","trakt"):
        v = ids.get(k)
        if not v: continue
        out += [f"{k.capitalize()}:{v}", f"{k}:{v}", str(v)]
    seen, uniq = set(), []
    for t in out:
        if t not in seen: seen.add(t); uniq.append(t)
    return uniq

def _jf_find_playlist_id(cfg: dict[str,Any], headers: dict[str,str], name: str) -> str | None:
    j = _jf_get(_jf_base(cfg), f"Users/{_jf_require_user(cfg)}/Items", headers, {
        "IncludeItemTypes":"Playlist","Recursive":"true","SortBy":"SortName","Fields":"ItemCounts","Limit":1000
    })
    name_lc = (name or "").strip().lower()
    for it in (j.get("Items") or []):
        if str(it.get("Name","")).strip().lower() == name_lc:
            return str(it.get("Id") or "")
    return None

def _jf_playlist_items(cfg: dict[str,Any], headers: dict[str,str], pl_id: str) -> list[dict[str,Any]]:
    j = _jf_get(_jf_base(cfg), f"Playlists/{pl_id}/Items", headers, {"UserId":_jf_require_user(cfg),"Fields":"ProviderIds","Limit":5000})
    return (j.get("Items") or []) if isinstance(j,dict) else []

def _jf_favorite_items(cfg: dict[str,Any], headers: dict[str,str]) -> list[dict[str,Any]]:
    j = _jf_get(_jf_base(cfg), f"Users/{_jf_require_user(cfg)}/Items", headers, {
        "Recursive":"true","Filters":"IsFavorite","IncludeItemTypes":"Movie,Series","Fields":"ProviderIds","Limit":5000
    })
    return (j.get("Items") or []) if isinstance(j,dict) else []

def _jf_index_watchlist(cfg: dict[str,Any], headers: dict[str,str], mode: str, playlist: str) -> dict[str,Any]:
    index, entry_by_item, items = {}, {}, []
    if mode=="playlist":
        pl_id = _jf_find_playlist_id(cfg,headers,playlist)
        if not pl_id: return {"by_token":index,"entry_by_item":entry_by_item}
        items = _jf_playlist_items(cfg,headers,pl_id)
    else:
        items = _jf_favorite_items(cfg,headers)
    for it in items:
        try:
            iid = str(it.get("Id") or "")
            if not iid: continue
            prov = it.get("ProviderIds") or {}
            tok = []
            for k in ("Tmdb","Imdb","Tvdb","Trakt"):
                v = prov.get(k)
                if not v: continue
                if k=="Imdb" and isinstance(v,list): v=v[0]
                tok += [f"{k}:{v}",f"{k.lower()}:{v}",str(v)]
            for t in tok:
                if t not in index: index[t]=iid
            if it.get("PlaylistItemId"): entry_by_item[iid]=str(it["PlaylistItemId"])
        except Exception:
            continue
    return {"by_token":index,"entry_by_item":entry_by_item}

def _jf_lookup_by_provider_ids(cfg: dict[str,Any], headers: dict[str,str], tokens: list[str]) -> str|None:
    if not tokens: return None
    for tok in tokens:
        try:
            j = _jf_get(_jf_base(cfg), f"Users/{_jf_require_user(cfg)}/Items", headers, {
                "Recursive":"true","IncludeItemTypes":"Movie,Series","AnyProviderIdEquals":tok,"Limit":1,"Fields":"ProviderIds"
            })
            items = (j.get("Items") or []) if isinstance(j,dict) else []
            if items and items[0].get("Id"): return str(items[0]["Id"])
        except Exception:
            continue
    return None

# ---------------------------------------------------------------------
# Build merged watchlist (for UI)
# ---------------------------------------------------------------------
def _get_items(state: dict[str,Any], prov: str) -> dict[str,Any]:
    return _get_provider_items(state, prov)

def build_watchlist(state: dict[str,Any], tmdb_ok: bool) -> list[dict[str,Any]]:
    plex, simkl, trakt, jelly = (_get_items(state,p) for p in ("PLEX","SIMKL","TRAKT","JELLYFIN"))
    hidden, out = _load_hide_set(), []
    for key in set(plex)|set(simkl)|set(trakt)|set(jelly):
        if key in hidden: continue
        p,s,t,j = plex.get(key) or {}, simkl.get(key) or {}, trakt.get(key) or {}, jelly.get(key) or {}
        candidates = [("plex",p),("simkl",s),("trakt",t),("jellyfin",j)]
        info = max(candidates, key=lambda kv:_rich_ids_score(kv[1]))[1] or (p or s or t or j)
        if not info: continue
        declared = {_norm_type(it.get("type")) for _,it in candidates if it}; declared.discard("")
        if "tv" in declared: typ="tv"
        elif "movie" in declared: typ="movie"
        else:
            ids = (info.get("ids") or {}) | {k:info.get(k) for k in ("tmdb","imdb","tvdb","trakt","slug")}
            typ = "tv" if ids.get("tvdb") else "movie"
        title = info.get("title") or info.get("name") or ""
        year, tmdb_id = info.get("year") or info.get("release_year"), (info.get("ids") or {}).get("tmdb") or info.get("tmdb")
        epochs = {src:_iso_to_epoch(_pick_added(it)) for src,it in candidates}
        added_src, added_epoch = max(epochs.items(), key=lambda kv: kv[1])
        out.append({
            "key":key, "type":typ, "title":title, "year":year,
            "tmdb":int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id,
            "status": {1:{"plex":"plex_only","simkl":"simkl_only","trakt":"trakt_only","jellyfin":"jellyfin_only"}[added_src], 2:"both",3:"both",4:"both"}.get(len([n for n,it in candidates if it]),"both"),
            "sources":[n for n,it in candidates if it],
            "added_epoch":added_epoch,"added_when":_pick_added(locals()[added_src[0]]),"added_src":added_src,
            "categories":[], "ids":_ids_from_key_or_item(key,info)
        })
    out.sort(key=lambda x:(x.get("added_epoch") or 0, x.get("year") or 0), reverse=True)
    return out

# ---------------------------------------------------------------------
# Provider deletes
# ---------------------------------------------------------------------
def _delete_on_plex_single(key: str, state: dict[str,Any], cfg: dict[str,Any]) -> None:
    token = (cfg.get("plex") or {}).get("account_token","").strip()
    if not token: raise RuntimeError("missing plex token")
    account = MyPlexAccount(token=token)
    item = (_get_items(state,"PLEX").get(key) or _get_items(state,"SIMKL").get(key) 
           or _get_items(state,"TRAKT").get(key) or _get_items(state,"JELLYFIN").get(key) or {})
    guid,rk = _extract_plex_identifiers(item); variants=_guid_variants_from_key_or_item(key,item)
    if guid: variants=list(dict.fromkeys(variants+[guid]))
    targets={_norm_guid(v) for v in variants if v}; rk=str(rk or "").strip()
    wl = account.watchlist(maxresults=100000)
    def matches(m) -> bool:
        cand={(getattr(m,"guid","") or "").split("?",1)[0]}
        try:
            for g in getattr(m,"guids",[]) or []: cand.add(str(getattr(g,"id",g) or "").split("?",1)[0])
        except: pass
        if any(_norm_guid(c) in targets for c in cand): return True
        return rk and str(getattr(m,"ratingKey","") or getattr(m,"id","")).strip()==rk
    found = next((m for m in wl if matches(m)),None)
    if not found: raise RuntimeError("item not found in Plex watchlist")
    removed=False
    try:
        rm=getattr(found,"removeFromWatchlist",None)
        if callable(rm): rm(); removed=True
    except: pass
    if not removed: account.removeFromWatchlist([found])
    if any(matches(m) for m in account.watchlist(maxresults=100000)):
        raise RuntimeError("PlexAPI reported removal but item still present")

_SIMKL_HIST, _SIMKL_WL = "https://api.simkl.com/sync/history/remove","https://api.simkl.com/sync/watchlist/remove"

def _simkl_headers(cfg: dict[str,Any]) -> dict[str,str]:
    return {
        "User-Agent":"CrossWatch/WebUI","Accept":"application/json","Content-Type":"application/json",
        "Authorization":f"Bearer {cfg.get('access_token','')}", "simkl-api-key":cfg.get("client_id","")
    }

def _post_simkl_delete(url: str, hdr: dict[str,str], payload: dict[str,Any]) -> dict[str,Any]:
    r=requests.post(url,headers=hdr,json=payload,timeout=45)
    if not r.ok: raise RuntimeError(f"SIMKL delete {r.status_code} {getattr(r,'text','')}")
    try: return r.json() if isinstance(r.json(),dict) else {}
    except: return {}

def _simkl_deleted_count(resp: dict[str,Any]) -> int:
    d=resp.get("deleted") or {}; return sum(int(d.get(k,0) or 0) for k in ("movies","shows","episodes","seasons")) if isinstance(d,dict) else 0

def _delete_on_simkl_batch(items: list[dict[str,Any]], cfg: dict[str,Any]) -> None:
    token,client_id=(cfg.get("access_token","").strip(), cfg.get("client_id","").strip())
    if not (token and client_id): raise RuntimeError("SIMKL not configured")
    payload={"movies":[],"shows":[]}
    for it in items:
        ids=_simkl_filter_ids(_ids_from_key_or_item(it["key"],it["item"]))
        if ids: (payload["movies"] if it["type"]=="movie" else payload["shows"]).append({"ids":ids})
    payload={k:v for k,v in payload.items() if v}
    if not payload: raise RuntimeError("SIMKL delete: no resolvable IDs")
    hdr=_simkl_headers(cfg)
    if _simkl_deleted_count(_post_simkl_delete(_SIMKL_WL,hdr,payload))>0: return
    if _simkl_deleted_count(_post_simkl_delete(_SIMKL_HIST,hdr,payload))>0: return
    raise RuntimeError(f"SIMKL delete matched 0 items. Payload={payload}")

# ---------------------------------------------------------------------
# TRAKT (batch)
# ---------------------------------------------------------------------
_TRAKT_REMOVE = "https://api.trakt.tv/sync/watchlist/remove"

def _trakt_headers(cfg: dict[str,Any]) -> dict[str,str]:
    tok = (cfg.get("access_token") or cfg.get("token") or "").strip()
    return {
        "Content-Type":"application/json","Accept":"application/json","User-Agent":"CrossWatch/WebUI",
        "trakt-api-version":"2","trakt-api-key":(cfg.get("client_id") or "").strip(),
        "Authorization":f"Bearer {tok}" if tok else ""
    }

def _delete_on_trakt_batch(items: list[dict[str,Any]], cfg: dict[str,Any]) -> None:
    hdr=_trakt_headers(cfg)
    if not (hdr.get("Authorization") and hdr.get("trakt-api-key")): raise RuntimeError("TRAKT not configured")
    payload={"movies":[],"shows":[]}
    for it in items:
        ids=_ids_from_key_or_item(it["key"],it["item"])
        entry={k:ids[k] for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k)}
        if entry: (payload["movies"] if it["type"]=="movie" else payload["shows"]).append({"ids":entry})
    payload={k:v for k,v in payload.items() if v}
    if not payload: raise RuntimeError("TRAKT delete: no resolvable IDs")
    r=requests.post(_TRAKT_REMOVE,headers=hdr,json=payload,timeout=45)
    if not r.ok: raise RuntimeError(f"TRAKT delete failed: {getattr(r,'text','no response')}")

# ---------------------------------------------------------------------
# JELLYFIN (batch)
# ---------------------------------------------------------------------
def _delete_on_jellyfin_batch(items: list[dict[str,Any]], cfg: dict[str,Any]) -> None:
    hdr,base,user=_jf_headers(cfg),_jf_base(cfg),_jf_require_user(cfg)
    mode=(cfg.get("watchlist",{}).get("mode") or "favorites").strip().lower()
    pl_name=(cfg.get("watchlist",{}).get("playlist_name") or "Watchlist").strip()
    idx=_jf_index_watchlist(cfg,hdr,mode,pl_name); by_tok=idx.get("by_token") or {}; entry_by=idx.get("entry_by_item") or {}
    jf_ids=[]
    for it in items:
        k,itm=it.get("key",""),it.get("item") or {}
        ids=_ids_from_key_or_item(k,itm); jf_id=_extract_jf_id(itm,k)
        if not jf_id:
            for tok in _jf_provider_tokens(ids):
                jf_id=by_tok.get(tok); 
                if jf_id: break
        if not jf_id: jf_id=_jf_lookup_by_provider_ids(cfg,hdr,_jf_provider_tokens(ids))
        if jf_id: jf_ids.append(jf_id)
    if not jf_ids: raise RuntimeError("Jellyfin delete: no resolvable ItemIds")
    if mode=="favorites":
        ok,last=0,None
        for iid in jf_ids:
            try: _jf_delete(base,f"Users/{user}/FavoriteItems/{iid}",hdr); ok+=1
            except Exception as e: last=e
        if ok==0: raise last or RuntimeError("Jellyfin favorites delete failed")
    elif mode=="playlist":
        pl_id=_jf_find_playlist_id(cfg,hdr,pl_name)
        if not pl_id: raise RuntimeError(f"Jellyfin: playlist '{pl_name}' not found")
        entries=[entry_by.get(iid) for iid in jf_ids if entry_by.get(iid)]
        if entries: _jf_delete(base,f"Playlists/{pl_id}/Items",hdr,params={"EntryIds":",".join(entries)})
        else: _jf_delete(base,f"Playlists/{pl_id}/Items",hdr,params={"Ids":",".join(jf_ids)})
    else: raise RuntimeError(f"Jellyfin: unknown mode '{mode}'")

# ---------------------------------------------------------------------
# Public facade
# ---------------------------------------------------------------------
def delete_watchlist_batch(keys: list[str], prov: str, state: dict[str,Any], cfg: dict[str,Any]) -> dict[str,Any]:
    prov=(prov or "").upper().strip(); keys=[k for k in keys or [] if isinstance(k,str) and k.strip()]
    if not keys: return {"deleted":0,"provider":prov,"note":"no-keys"}
    if prov=="SIMKL":
        items=[{"key":k,"item":_find_item_in_state_for_provider(state,k,"SIMKL") or _find_item_in_state(state,k),"type":_type_from_item_or_guess(_find_item_in_state(state,k),k)} for k in keys]
        _delete_on_simkl_batch(items,cfg.get("simkl",{}) or {})
    elif prov=="TRAKT":
        items=[{"key":k,"item":_find_item_in_state_for_provider(state,k,"TRAKT") or _find_item_in_state(state,k),"type":_type_from_item_or_guess(_find_item_in_state(state,k),k)} for k in keys]
        _delete_on_trakt_batch(items,cfg.get("trakt",{}) or {})
    elif prov=="PLEX":
        for k in keys: _delete_on_plex_single(k,state,cfg)
    elif prov=="JELLYFIN":
        items=[{"key":k,"item":_find_item_in_state_for_provider(state,k,"JELLYFIN") or _find_item_in_state(state,k),"type":_type_from_item_or_guess(_find_item_in_state(state,k),k)} for k in keys]
        _delete_on_jellyfin_batch(items,cfg.get("jellyfin",{}) or {})
    else: raise RuntimeError(f"unknown provider: {prov}")
    if any(_del_key_from_provider_items(state,prov,k) for k in keys): _save_state_dict(_state_path(),state)
    return {"deleted":len(keys),"provider":prov,"status":"ok"}

# ---------------------------------------------------------------------
# Public: single delete
# ---------------------------------------------------------------------
def delete_watchlist_item(key: str, state_path: Path, cfg: dict[str,Any], log=None, provider: str|None=None) -> dict[str,Any]:
    prov=(provider or "PLEX").upper(); state=_load_state_dict(state_path)

    def _log(level,msg): 
        try: log and log(level,msg)
        except: pass

    def _present(): return any(_get_provider_items(state,p).get(key) for p in ("PLEX","SIMKL","TRAKT","JELLYFIN"))

    def _delete_and_drop(p, fn):
        it=_find_item_in_state(state,key) or {}
        fn([{"key":key,"item":it,"type":_type_from_item_or_guess(it,key)}], cfg.get(p.lower(),{}) or {})
        _del_key_from_provider_items(state,p,key)

    try:
        if prov=="PLEX":
            _delete_on_plex_single(key,state,cfg); _del_key_from_provider_items(state,"PLEX",key)
        elif prov=="SIMKL": _delete_and_drop("SIMKL",_delete_on_simkl_batch)
        elif prov=="TRAKT": _delete_and_drop("TRAKT",_delete_on_trakt_batch)
        elif prov=="JELLYFIN": _delete_and_drop("JELLYFIN",_delete_on_jellyfin_batch)
        elif prov=="ALL":
            details={}
            for p,fn in {"PLEX":lambda *_:_delete_on_plex_single(key,state,cfg),
                         "SIMKL":lambda *_:_delete_and_drop("SIMKL",_delete_on_simkl_batch),
                         "TRAKT":lambda *_:_delete_and_drop("TRAKT",_delete_on_trakt_batch),
                         "JELLYFIN":lambda *_:_delete_and_drop("JELLYFIN",_delete_on_jellyfin_batch)}.items():
                try: fn(); details[p]={"ok":True}
                except Exception as e: _log("TRBL",f"[WATCHLIST] {p} delete failed: {e}"); details[p]={"ok":False,"error":str(e)}
            if not _present(): hide=_load_hide_set(); hide.add(key); _save_hide_set(hide)
            _save_state_dict(state_path,state); return {"ok":any(v["ok"] for v in details.values()),"deleted":key,"provider":"ALL","details":details}
        else: return {"ok":False,"error":f"unknown provider '{prov}'"}

        if not _present(): hide=_load_hide_set(); hide.add(key); _save_hide_set(hide)
        _save_state_dict(state_path,state); return {"ok":True,"deleted":key,"provider":prov}

    except Exception as e:
        _log("TRBL",f"[WATCHLIST] {prov} delete failed: {e}")
        return {"ok":False,"error":str(e),"provider":prov}

# _watchlist.py
#  Helpers for building the merged watchlist view and performing deletes
from __future__ import annotations

from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
import json
import requests
from time import time
from urllib.parse import urlencode

from plexapi.myplex import MyPlexAccount
from cw_platform.config_base import CONFIG

# -----------------------------------------------------------------------------
# Paths
# -----------------------------------------------------------------------------

def _state_path() -> Path:
    return CONFIG / "watchlist_state.json"

# Local overlay for server-side hiding (UI also uses localStorage)
HIDE_PATH: Path = CONFIG / "watchlist_hide.json"

# -----------------------------------------------------------------------------
# Local "hide" overlay helpers (server-side; UI uses localStorage)
# -----------------------------------------------------------------------------

def _load_hide_set() -> Set[str]:
    try:
        if HIDE_PATH.exists():
            data = json.loads(HIDE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return set(str(x) for x in data)
    except Exception:
        pass
    return set()

def _save_hide_set(hide: Set[str]) -> None:
    try:
        HIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        HIDE_PATH.write_text(json.dumps(sorted(hide)), encoding="utf-8")
    except Exception:
        pass

# -----------------------------------------------------------------------------
# Generic helpers (ids / type / state)
# -----------------------------------------------------------------------------

def _load_state_dict(state_path: Path) -> Dict[str, Any]:
    try:
        if state_path and state_path.exists():
            return json.loads(state_path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}

def _save_state_dict(state_path: Path, state: Dict[str, Any]) -> None:
    try:
        state_path.write_text(json.dumps(state, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass

def _get_provider_items(state: Dict[str, Any], provider: str) -> Dict[str, Any]:
    P = (state.get("providers") or {}).get(provider.upper(), {}) or {}
    wl = (((P.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
    if wl:
        return wl
    return (P.get("items") or {}) or {}

def _del_key_from_provider_items(state: Dict[str, Any], provider: str, key: str) -> bool:
    changed = False
    prov = (state.get("providers") or {}).get(provider.upper()) or {}
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
    if pref in ("imdb", "tmdb", "tvdb", "trakt", "slug", "jellyfin") and val:
        ids.setdefault(pref, val)
    if "thetvdb" in ids and "tvdb" not in ids:
        ids["tvdb"] = ids.get("thetvdb")
    return {k: ids[k] for k in ("simkl", "imdb", "tmdb", "tvdb", "trakt", "slug", "jellyfin") if ids.get(k)}

def _type_from_item_or_guess(item: Dict[str, Any], key: str) -> str:
    typ = (item.get("type") or "").lower()
    if typ in ("movie", "show", "tv", "series"):
        return "movie" if typ == "movie" else "show"
    ids = (item.get("ids") or {})
    if ids.get("tvdb") or ids.get("thetvdb"):
        return "show"
    pref = (key or "").split(":", 1)[0].lower().strip()
    if pref in ("tvdb", "thetvdb"):
        return "show"
    return "movie"

_SIMKL_ID_KEYS = ("simkl", "imdb", "tmdb", "tvdb", "slug")

def _simkl_filter_ids(ids: Dict[str, Any]) -> Dict[str, Any]:
    return {k: str(v) for k, v in ids.items() if k in _SIMKL_ID_KEYS and v}

# -----------------------------------------------------------------------------
# Plex GUID helpers (for PlexAPI matching)
# -----------------------------------------------------------------------------

def _pick_added(d: Dict[str, Any]) -> Optional[str]:
    if not isinstance(d, dict):
        return None
    for k in ("added", "added_at", "addedAt", "date_added", "created_at", "createdAt"):
        v = d.get(k)
        if v:
            return str(v)
    nested = d.get("dates") or d.get("meta") or d.get("attributes") or {}
    if isinstance(nested, dict):
        for k in ("added", "added_at", "created", "created_at"):
            v = nested.get(k)
            if v:
                return str(v)
    return None

def _iso_to_epoch(iso: Optional[str]) -> int:
    if not iso:
        return 0
    try:
        s = str(iso).strip().replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0

def _norm_guid(g: str) -> Tuple[str, str]:
    s = (g or "").strip()
    if not s:
        return "", ""
    s = s.split("?", 1)[0]
    if s.startswith("com.plexapp.agents."):
        try:
            rest = s.split("com.plexapp.agents.", 1)[1]
            prov, ident = rest.split("://", 1)
            prov = prov.lower().strip().replace("thetvdb", "tvdb")
            return prov, ident.strip()
        except Exception:
            return "", ""
    try:
        prov, ident = s.split("://", 1)
        prov = prov.lower().strip().replace("thetvdb", "tvdb")
        return prov, ident.strip()
    except Exception:
        return "", ""

def _guid_variants_from_key_or_item(key: str, item: Optional[Dict[str, Any]] = None) -> List[str]:
    prov, _, ident = (key or "").partition(":")
    prov = (prov or "").lower().strip()
    ident = (ident or "").strip()
    if not prov or not ident:
        ids = (item or {}).get("ids") or {}
        if ids.get("imdb"):
            prov, ident = "imdb", str(ids["imdb"])
        elif ids.get("tmdb"):
            prov, ident = "tmdb", str(ids["tmdb"])
        elif ids.get("tvdb") or ids.get("thetvdb"):
            prov, ident = "tvdb", str(ids.get("tvdb") or ids.get("thetvdb"))
    if not prov or not ident:
        return []
    prov = "tvdb" if prov in ("thetvdb", "tvdb") else prov
    base = f"{prov}://{ident}"
    return [
        base,
        f"com.plexapp.agents.{prov}://{ident}",
        f"com.plexapp.agents.{prov}://{ident}?lang=en",
    ]

def _extract_plex_identifiers(item: Dict[str, Any]) -> Tuple[Optional[str], Optional[str]]:
    if not isinstance(item, dict):
        return None, None
    guid = item.get("guid") or (item.get("ids", {}) or {}).get("guid")
    ratingKey = item.get("ratingKey") or item.get("id") or (item.get("ids", {}) or {}).get("ratingKey")
    p = item.get("plex") or {}
    if not guid:
        guid = p.get("guid")
    if not ratingKey:
        ratingKey = p.get("ratingKey") or p.get("id")
    return (str(guid) if guid else None, str(ratingKey) if ratingKey else None)

# -----------------------------------------------------------------------------
# Jellyfin helpers (API calls, id resolution, index)
# -----------------------------------------------------------------------------

def _jf_base(cfg: Dict[str, Any]) -> str:
    base = (cfg.get("server") or "").strip()
    if not base:
        raise RuntimeError("Jellyfin: missing 'server' in config")
    if not base.endswith("/"):
        base += "/"
    return base

def _jf_headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    token = (cfg.get("access_token") or cfg.get("api_key") or "").strip()
    device_id = (cfg.get("device_id") or "CrossWatch").strip() or "CrossWatch"
    hdr = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Emby-Authorization": f'MediaBrowser Client="CrossWatch", Device="WebUI", DeviceId="{device_id}", Version="1.0.0"',
    }
    if token:
        hdr["X-Emby-Token"] = token
    return hdr

def _jf_require_user(cfg: Dict[str, Any]) -> str:
    user_id = (cfg.get("user_id") or "").strip()
    if not user_id:
        raise RuntimeError("Jellyfin: missing 'user_id' in config")
    return user_id

def _extract_jf_id(item: Dict[str, Any], key: str) -> Optional[str]:
    if not isinstance(item, dict):
        item = {}
    ids = (item.get("ids") or {})
    cand = (
        ids.get("jellyfin")
        or item.get("jellyfinId")
        or item.get("jf_id")
        or item.get("Id")
        or item.get("id")
    )
    if cand:
        return str(cand)
    pref, _, val = (key or "").partition(":")
    if pref.lower().strip() == "jellyfin" and val.strip():
        return val.strip()
    return None

def _jf_get(base: str, path: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    url = base + path.lstrip("/")
    if params:
        url += ("&" if "?" in url else "?") + urlencode({k: v for k, v in params.items() if v is not None})
    r = requests.get(url, headers=headers, timeout=45)
    if not r.ok:
        raise RuntimeError(f"Jellyfin GET {path} -> {r.status_code}: {getattr(r, 'text', '')}")
    try:
        j = r.json()
    except Exception:
        j = {}
    return j if isinstance(j, dict) else {}

def _jf_delete(base: str, path: str, headers: Dict[str, str], params: Optional[Dict[str, Any]] = None) -> None:
    url = base + path.lstrip("/")
    if params:
        url += ("&" if "?" in url else "?") + urlencode({k: v for k, v in params.items() if v is not None})

    r = requests.delete(url, headers=headers, timeout=45)
    st = int(getattr(r, "status_code", 0) or 0)

    if st in (200, 202, 204, 404) or r.ok:
        return

    raise RuntimeError(f"Jellyfin DELETE {path} -> {st}: {getattr(r, 'text', '')}")


def _jf_provider_tokens(ids: Dict[str, Any]) -> List[str]:
    """
    Build a list of ProviderId tokens Jellyfin understands for AnyProviderIdEquals.
    We'll try multiple casings just in case.
    """
    out: List[str] = []
    tmdb = ids.get("tmdb"); imdb = ids.get("imdb"); tvdb = ids.get("tvdb"); trakt = ids.get("trakt")
    if tmdb:
        out += [f"Tmdb:{tmdb}", f"tmdb:{tmdb}", str(tmdb)]
    if imdb:
        out += [f"Imdb:{imdb}", f"imdb:{imdb}", str(imdb)]
    if tvdb:
        out += [f"Tvdb:{tvdb}", f"tvdb:{tvdb}", str(tvdb)]
    if trakt:
        out += [f"Trakt:{trakt}", f"trakt:{trakt}", str(trakt)]
    # dedupe keep order
    seen = set(); uniq = []
    for t in out:
        if t not in seen:
            seen.add(t); uniq.append(t)
    return uniq

def _jf_find_playlist_id(cfg: Dict[str, Any], headers: Dict[str, str], name: str) -> Optional[str]:
    base = _jf_base(cfg)
    user_id = _jf_require_user(cfg)
    j = _jf_get(base, f"Users/{user_id}/Items", headers, params={
        "IncludeItemTypes": "Playlist",
        "Recursive": "true",
        "SortBy": "SortName",
        "Fields": "ItemCounts",
        "Limit": 1000,
    })
    items = (j.get("Items") or []) if isinstance(j, dict) else []
    name_lc = (name or "").strip().lower()
    for it in items:
        if isinstance(it, dict) and str(it.get("Name", "")).strip().lower() == name_lc:
            return str(it.get("Id") or "")
    return None

def _jf_playlist_items(cfg: Dict[str, Any], headers: Dict[str, str], playlist_id: str) -> List[Dict[str, Any]]:
    base = _jf_base(cfg)
    user_id = _jf_require_user(cfg)
    j = _jf_get(base, f"Playlists/{playlist_id}/Items", headers, params={"UserId": user_id, "Fields": "ProviderIds", "Limit": 5000})
    return (j.get("Items") or []) if isinstance(j, dict) else []

def _jf_favorite_items(cfg: Dict[str, Any], headers: Dict[str, str]) -> List[Dict[str, Any]]:
    base = _jf_base(cfg)
    user_id = _jf_require_user(cfg)
    j = _jf_get(base, f"Users/{user_id}/Items", headers, params={
        "Recursive": "true",
        "Filters": "IsFavorite",
        "IncludeItemTypes": "Movie,Series",
        "Fields": "ProviderIds",
        "Limit": 5000,
    })
    return (j.get("Items") or []) if isinstance(j, dict) else []

def _jf_index_watchlist(cfg: Dict[str, Any], headers: Dict[str, str], mode: str, playlist_name: str) -> Dict[str, Any]:
    """
    Build a reverse index of the user's Favorites/Playlist:
      - map provider ids (tmdb/imdb/tvdb/trakt) -> Jellyfin ItemId
      - for playlist mode also collect PlaylistItemId entries for quick delete
    """
    index: Dict[str, str] = {}          # token -> itemId
    entry_by_item: Dict[str, str] = {}  # itemId -> playlistEntryId (playlist mode)
    items: List[Dict[str, Any]] = []

    if mode == "playlist":
        pl_id = _jf_find_playlist_id(cfg, headers, playlist_name)
        if not pl_id:
            return {"by_token": index, "entry_by_item": entry_by_item}
        items = _jf_playlist_items(cfg, headers, pl_id)
    else:
        items = _jf_favorite_items(cfg, headers)

    for it in items:
        try:
            iid = str(it.get("Id") or "")
            if not iid:
                continue
            prov = (it.get("ProviderIds") or {}) if isinstance(it.get("ProviderIds"), dict) else {}
            tok = []
            for k in ("Tmdb", "Imdb", "Tvdb", "Trakt"):
                v = prov.get(k)
                if not v:
                    continue
                if k == "Imdb" and isinstance(v, list):
                    v = v[0]
                tok += [f"{k}:{v}", f"{k.lower()}:{v}", str(v)]
            for t in tok:
                if t not in index:
                    index[t] = iid
            if "PlaylistItemId" in it:
                entry_by_item[iid] = str(it.get("PlaylistItemId"))
        except Exception:
            continue

    return {"by_token": index, "entry_by_item": entry_by_item}

def _jf_lookup_by_provider_ids(cfg: Dict[str, Any], headers: Dict[str, str], tokens: List[str]) -> Optional[str]:
    """
    Try querying Jellyfin by AnyProviderIdEquals until we find an item Id.
    """
    if not tokens:
        return None
    base = _jf_base(cfg)
    user_id = _jf_require_user(cfg)

    for tok in tokens:
        try:
            j = _jf_get(base, f"Users/{user_id}/Items", headers, params={
                "Recursive": "true",
                "IncludeItemTypes": "Movie,Series",
                "AnyProviderIdEquals": tok,
                "Limit": 1,
                "Fields": "ProviderIds",
            })
            items = (j.get("Items") or []) if isinstance(j, dict) else []
            if items:
                iid = str(items[0].get("Id") or "")
                if iid:
                    return iid
        except Exception:
            continue
    return None
# -----------------------------------------------------------------------------
# Build merged watchlist view (for UI)
# -----------------------------------------------------------------------------

def _get_items(state: Dict[str, Any], prov: str) -> Dict[str, Any]:
    return _get_provider_items(state, prov)

def build_watchlist(state: Dict[str, Any], tmdb_api_key_present: bool) -> List[Dict[str, Any]]:
    plex_items   = _get_items(state, "PLEX")
    simkl_items  = _get_items(state, "SIMKL")
    trakt_items  = _get_items(state, "TRAKT")
    jelly_items  = _get_items(state, "JELLYFIN")

    hidden = _load_hide_set()
    out: List[Dict[str, Any]] = []
    all_keys = set(plex_items) | set(simkl_items) | set(trakt_items) | set(jelly_items)

    for key in all_keys:
        if key in hidden:
            continue

        p = plex_items.get(key) or {}
        s = simkl_items.get(key) or {}
        t = trakt_items.get(key) or {}
        j = jelly_items.get(key) or {}
        info = p or s or t or j
        if not info:
            continue

        typ_raw = (info.get("type") or "").lower()
        typ = "tv" if typ_raw in ("tv", "show", "series") else "movie"
        title = info.get("title") or info.get("name") or ""
        year = info.get("year") or info.get("release_year")
        tmdb_id = (info.get("ids", {}) or {}).get("tmdb") or info.get("tmdb")

        p_ep = _iso_to_epoch(_pick_added(p))
        s_ep = _iso_to_epoch(_pick_added(s))
        t_ep = _iso_to_epoch(_pick_added(t))
        j_ep = _iso_to_epoch(_pick_added(j))
        added_epoch = max(p_ep, s_ep, t_ep, j_ep)
        if added_epoch == p_ep:
            added_when, added_src = _pick_added(p), "plex"
        elif added_epoch == s_ep:
            added_when, added_src = _pick_added(s), "simkl"
        elif added_epoch == t_ep:
            added_when, added_src = _pick_added(t), "trakt"
        else:
            added_when, added_src = _pick_added(j), "jellyfin"

        sources = [name for name, it in (("plex", p), ("simkl", s), ("trakt", t), ("jellyfin", j)) if it]
        status = {
            1: {"plex": "plex_only", "simkl": "simkl_only", "trakt": "trakt_only", "jellyfin": "jellyfin_only"}[sources[0]],
            2: "both",
            3: "both",
            4: "both",
        }[len(sources) if len(sources) in (1, 2, 3, 4) else 1]

        ids_for_ui = _ids_from_key_or_item(key, info)

        out.append({
            "key": key,
            "type": typ,
            "title": title,
            "year": year,
            "tmdb": int(tmdb_id) if str(tmdb_id).isdigit() else tmdb_id,
            "status": status,
            "sources": sources,
            "added_epoch": added_epoch,
            "added_when": added_when,
            "added_src": added_src,
            "categories": [],
            "ids": ids_for_ui,
        })

    out.sort(key=lambda x: (x.get("added_epoch") or 0, x.get("year") or 0), reverse=True)
    return out

# -----------------------------------------------------------------------------
# Provider-specific deletes (single + batch)
# -----------------------------------------------------------------------------

# ---- Plex (single only) -----------------------------------------------

def _delete_on_plex_single(key: str, state: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    token = ((cfg.get("plex", {}) or {}).get("account_token") or "").strip()
    if not token:
        raise RuntimeError("missing plex token")
    account = MyPlexAccount(token=token)

    plex_items = _get_provider_items(state, "PLEX")
    simkl_items = _get_provider_items(state, "SIMKL")
    trakt_items = _get_provider_items(state, "TRAKT")
    jelly_items = _get_provider_items(state, "JELLYFIN")
    item = plex_items.get(key) or simkl_items.get(key) or trakt_items.get(key) or jelly_items.get(key) or {}

    guid, ratingKey = _extract_plex_identifiers(item)
    variants = _guid_variants_from_key_or_item(key, item)
    if guid:
        variants = list(dict.fromkeys(variants + [guid]))
    targets = {_norm_guid(v) for v in variants if v}
    rk = str(ratingKey or "").strip()

    watchlist = account.watchlist(maxresults=100000)

    def matches(media) -> bool:
        cand = set()
        primary = (getattr(media, "guid", "") or "").split("?", 1)[0]
        if primary:
            cand.add(primary)
        try:
            for gg in getattr(media, "guids", []) or []:
                gid = str(getattr(gg, "id", gg) or "")
                if gid:
                    cand.add(gid.split("?", 1)[0])
        except Exception:
            pass
        if any(_norm_guid(cg) in targets for cg in cand):
            return True
        m_rk = str(getattr(media, "ratingKey", "") or getattr(media, "id", "") or "").strip()
        return rk and m_rk and (rk == m_rk)

    found = next((m for m in watchlist if matches(m)), None)
    if not found:
        raise RuntimeError("item not found in Plex online watchlist")

    removed = False
    try:
        rm = getattr(found, "removeFromWatchlist", None)
        if callable(rm):
            rm()
            removed = True
    except Exception:
        pass
    if not removed:
        account.removeFromWatchlist([found])

    wl2 = account.watchlist(maxresults=100000)
    if any(matches(m) for m in wl2):
        raise RuntimeError("PlexAPI reported removal but item is still present")

# ---- SIMKL (batch) ----------------------------------------------------

_SIMKL_HISTORY_REMOVE = "https://api.simkl.com/sync/history/remove"
_SIMKL_WATCHLIST_REMOVE = "https://api.simkl.com/sync/watchlist/remove"

def _simkl_headers(simkl_cfg: Dict[str, Any]) -> Dict[str, str]:
    return {
        "User-Agent": "CrossWatch/WebUI",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {simkl_cfg.get('access_token','')}",
        "simkl-api-key": simkl_cfg.get("client_id",""),
    }

def _post_simkl_delete(url: str, hdrs: Dict[str, str], payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(url, headers=hdrs, json=payload, timeout=45)
    if not r.ok:
        raise RuntimeError(f"SIMKL delete failed: {r.status_code} {getattr(r,'text','')}")
    try:
        data = r.json()
    except Exception:
        data = {}
    return data if isinstance(data, dict) else {}

def _simkl_deleted_count(resp: Dict[str, Any]) -> int:
    d = resp.get("deleted") or {}
    if not isinstance(d, dict):
        return 0
    return sum(int(d.get(k, 0) or 0) for k in ("movies", "shows", "episodes", "seasons"))

def _delete_on_simkl_batch(items: List[Dict[str, Any]], simkl_cfg: Dict[str, Any]) -> None:
    token = (simkl_cfg.get("access_token") or "").strip()
    client_id = (simkl_cfg.get("client_id") or "").strip()
    if not token or not client_id:
        raise RuntimeError("SIMKL not configured")

    payload = {"movies": [], "shows": []}
    for it in items:
        ids = _ids_from_key_or_item(it["key"], it["item"])
        entry_ids = _simkl_filter_ids(ids)
        if not entry_ids:
            continue
        (payload["movies"] if it["type"] == "movie" else payload["shows"]).append({"ids": entry_ids})

    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        raise RuntimeError("SIMKL delete: no resolvable IDs for requested items")

    hdr = _simkl_headers(simkl_cfg)

    resp_wl = _post_simkl_delete(_SIMKL_WATCHLIST_REMOVE, hdr, payload)
    if _simkl_deleted_count(resp_wl) > 0:
        return

    resp_hist = _post_simkl_delete(_SIMKL_HISTORY_REMOVE, hdr, payload)
    if _simkl_deleted_count(resp_hist) > 0:
        return

    raise RuntimeError(f"SIMKL delete matched 0 items. Payload={payload} WL={resp_wl} HIST={resp_hist}")

# ---- TRAKT (batch) ----------------------------------------------------

_TRAKT_REMOVE = "https://api.trakt.tv/sync/watchlist/remove"

def _trakt_headers(trakt_cfg: Dict[str, Any]) -> Dict[str, str]:
    token = (trakt_cfg.get("access_token") or "").strip() or (trakt_cfg.get("token") or "").strip()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "CrossWatch/WebUI",
        "trakt-api-version": "2",
        "trakt-api-key": (trakt_cfg.get("client_id") or "").strip(),
        "Authorization": f"Bearer {token}" if token else "",
    }

def _delete_on_trakt_batch(items: List[Dict[str, Any]], trakt_cfg: Dict[str, Any]) -> None:
    hdr = _trakt_headers(trakt_cfg)
    if not hdr.get("Authorization") or not hdr.get("trakt-api-key"):
        raise RuntimeError("TRAKT not configured")

    payload = {"movies": [], "shows": []}
    for it in items:
        ids = _ids_from_key_or_item(it["key"], it["item"])
        if not ids:
            continue
        entry_ids = {k: ids[k] for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k)}
        if not entry_ids:
            continue
        (payload["movies"] if it["type"] == "movie" else payload["shows"]).append({"ids": entry_ids})
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        raise RuntimeError("TRAKT delete: no resolvable IDs for requested items")

    r = requests.post(_TRAKT_REMOVE, headers=hdr, json=payload, timeout=45)
    if not r or not r.ok:
        raise RuntimeError(f"TRAKT delete failed: {getattr(r,'text','no response')}")

# ---- JELLYFIN (batch) -------------------------------------------------

def _delete_on_jellyfin_batch(items: List[Dict[str, Any]], jf_cfg: Dict[str, Any]) -> None:
    """
    Delete items from Jellyfin watchlist:
      - mode=favorites -> remove from Favorites
      - mode=playlist  -> remove entries from named Playlist
    Resolves Jellyfin ItemIds from external ids when needed.
    """
    headers = _jf_headers(jf_cfg)
    base = _jf_base(jf_cfg)
    user_id = _jf_require_user(jf_cfg)

    mode = (((jf_cfg.get("watchlist") or {}).get("mode") or "favorites").strip().lower())
    playlist_name = ((jf_cfg.get("watchlist") or {}).get("playlist_name") or "Watchlist").strip()

    # Build reverse index for extra robustness
    idx = _jf_index_watchlist(jf_cfg, headers, mode, playlist_name)
    by_tok = idx.get("by_token") or {}
    entry_by_item = idx.get("entry_by_item") or {}

    jf_ids: List[str] = []
    for it in items:
        key = it.get("key") or ""
        src_item = it.get("item") or {}
        ids = _ids_from_key_or_item(key, src_item)
        # 1) direct jellyfin id
        jf_id = _extract_jf_id(src_item, key)
        if not jf_id:
            # 2) try index by provider tokens
            tokens = _jf_provider_tokens(ids)
            for tok in tokens:
                jf_id = by_tok.get(tok)
                if jf_id:
                    break
        if not jf_id:
            # 3) query Jellyfin by AnyProviderIdEquals tokens
            jf_id = _jf_lookup_by_provider_ids(jf_cfg, headers, _jf_provider_tokens(ids))
        if jf_id:
            jf_ids.append(jf_id)

    if not jf_ids:
        raise RuntimeError("Jellyfin delete: no resolvable ItemIds (check ProviderIds or playlist/favorites content)")

    if mode == "favorites":
        # DELETE /Users/{userId}/FavoriteItems/{itemId}
        last_err = None
        ok = 0
        for iid in jf_ids:
            try:
                _jf_delete(base, f"Users/{user_id}/FavoriteItems/{iid}", headers)
                ok += 1
            except Exception as e:
                last_err = e
        if ok == 0:
            raise last_err or RuntimeError("Jellyfin favorites delete failed")

    elif mode == "playlist":
        pl_id = _jf_find_playlist_id(jf_cfg, headers, playlist_name)
        if not pl_id:
            raise RuntimeError(f"Jellyfin: playlist '{playlist_name}' not found")

        # Prefer PlaylistEntryIds when available, else we can delete by ItemIds too
        entry_ids = [entry_by_item.get(iid) for iid in jf_ids if entry_by_item.get(iid)]
        if entry_ids:
            _jf_delete(base, f"Playlists/{pl_id}/Items", headers, params={"EntryIds": ",".join(entry_ids)})
        else:
            # Fallback: Jellyfin also accepts Ids= (item ids) for playlist delete
            _jf_delete(base, f"Playlists/{pl_id}/Items", headers, params={"Ids": ",".join(jf_ids)})
    else:
        raise RuntimeError(f"Jellyfin: unknown watchlist mode '{mode}'")

# -----------------------------------------------------------------------------
# Public: batch facade used by the API
# -----------------------------------------------------------------------------

def delete_watchlist_batch(keys: List[str], provider: str, state: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    prov = (provider or "").upper().strip()
    keys = [k for k in (keys or []) if isinstance(k, str) and k.strip()]
    if not keys:
        return {"deleted": 0, "provider": prov, "note": "no-keys"}

    if prov == "SIMKL":
        items: List[Dict[str, Any]] = []
        for k in keys:
            it = _find_item_in_state_for_provider(state, k, "SIMKL") or _find_item_in_state(state, k)
            items.append({"key": k, "item": it or {}, "type": _type_from_item_or_guess(it or {}, k)})
        _delete_on_simkl_batch(items, cfg.get("simkl", {}) or {})

    elif prov == "TRAKT":
        items = []
        for k in keys:
            it = _find_item_in_state_for_provider(state, k, "TRAKT") or _find_item_in_state(state, k)
            items.append({"key": k, "item": it or {}, "type": _type_from_item_or_guess(it or {}, k)})
        _delete_on_trakt_batch(items, cfg.get("trakt", {}) or {})

    elif prov == "PLEX":
        for k in keys:
            _delete_on_plex_single(k, state, cfg)

    elif prov == "JELLYFIN":
        items = []
        for k in keys:
            it = _find_item_in_state_for_provider(state, k, "JELLYFIN") or _find_item_in_state(state, k)
            items.append({"key": k, "item": it or {}, "type": _type_from_item_or_guess(it or {}, k)})
        _delete_on_jellyfin_batch(items, cfg.get("jellyfin", {}) or {})

    else:
        raise RuntimeError(f"unknown provider: {prov}")

    changed = False
    for k in keys:
        changed |= _del_key_from_provider_items(state, prov, k)
    if changed:
        _save_state_dict(_state_path(), state)

    return {"deleted": len(keys), "provider": prov, "status": "ok"}

# -----------------------------------------------------------------------------
# Public: single delete
# -----------------------------------------------------------------------------

def delete_watchlist_item(
    key: str,
    state_path: Path,
    cfg: Dict[str, Any],
    log=None,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    provider = (provider or "PLEX").upper()
    state = _load_state_dict(state_path)

    def _log(level: str, msg: str):
        if log:
            try:
                log(level, msg)
            except Exception:
                pass

    def _present_any_prov() -> bool:
        return any(_get_provider_items(state, p).get(key) for p in ("PLEX", "SIMKL", "TRAKT", "JELLYFIN"))

    def _delete_simkl_any():
        item = _find_item_in_state(state, key) or {}
        _delete_on_simkl_batch(
            [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
            (cfg.get("simkl") or {}),
        )
        _del_key_from_provider_items(state, "SIMKL", key)

    def _delete_trakt_any():
        item = _find_item_in_state(state, key) or {}
        _delete_on_trakt_batch(
            [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
            (cfg.get("trakt") or {}),
        )
        _del_key_from_provider_items(state, "TRAKT", key)

    def _delete_jf_any():
        item = _find_item_in_state(state, key) or {}
        _delete_on_jellyfin_batch(
            [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
            (cfg.get("jellyfin") or {}),
        )
        _del_key_from_provider_items(state, "JELLYFIN", key)

    try:
        if provider == "PLEX":
            _delete_on_plex_single(key=key, state=state, cfg=cfg)
            _del_key_from_provider_items(state, "PLEX", key)
            if not _present_any_prov():
                hide = _load_hide_set(); hide.add(key); _save_hide_set(hide)
            _save_state_dict(state_path, state)
            return {"ok": True, "deleted": key, "provider": provider}

        elif provider == "SIMKL":
            _delete_simkl_any()
            if not _present_any_prov():
                hide = _load_hide_set(); hide.add(key); _save_hide_set(hide)
            _save_state_dict(state_path, state)
            return {"ok": True, "deleted": key, "provider": provider}

        elif provider == "TRAKT":
            _delete_trakt_any()
            if not _present_any_prov():
                hide = _load_hide_set(); hide.add(key); _save_hide_set(hide)
            _save_state_dict(state_path, state)
            return {"ok": True, "deleted": key, "provider": provider}

        elif provider == "JELLYFIN":
            _delete_jf_any()
            if not _present_any_prov():
                hide = _load_hide_set(); hide.add(key); _save_hide_set(hide)
            _save_state_dict(state_path, state)
            return {"ok": True, "deleted": key, "provider": provider}

        elif provider == "ALL":
            details: Dict[str, Dict[str, Any]] = {}

            try:
                _delete_on_plex_single(key=key, state=state, cfg=cfg)
                _del_key_from_provider_items(state, "PLEX", key)
                details["PLEX"] = {"ok": True}
            except Exception as e:
                _log("TRBL", f"[WATCHLIST] PLEX delete failed: {e}")
                details["PLEX"] = {"ok": False, "error": str(e)}

            try:
                _delete_simkl_any()
                details["SIMKL"] = {"ok": True}
            except Exception as e:
                _log("TRBL", f"[WATCHLIST] SIMKL delete failed: {e}")
                details["SIMKL"] = {"ok": False, "error": str(e)}

            try:
                _delete_trakt_any()
                details["TRAKT"] = {"ok": True}
            except Exception as e:
                _log("TRBL", f"[WATCHLIST] TRAKT delete failed: {e}")
                details["TRAKT"] = {"ok": False, "error": str(e)}

            try:
                _delete_jf_any()
                details["JELLYFIN"] = {"ok": True}
            except Exception as e:
                _log("TRBL", f"[WATCHLIST] JELLYFIN delete failed: {e}")
                details["JELLYFIN"] = {"ok": False, "error": str(e)}

            if not _present_any_prov():
                hide = _load_hide_set(); hide.add(key); _save_hide_set(hide)

            _save_state_dict(state_path, state)
            any_ok = any(v.get("ok") for v in details.values())
            return {"ok": any_ok, "deleted": key, "provider": "ALL", "details": details}

        else:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

    except Exception as e:
        _log("TRBL", f"[WATCHLIST] {provider} delete failed: {e}")
        return {"ok": False, "error": str(e), "provider": provider}

# services/watchlist.py
# CrossWatch - Watchlist management helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Any, cast

import requests
from urllib.parse import urlencode

from cw_platform.config_base import CONFIG
from cw_platform.modules_registry import MODULES as MR_MODULES, load_sync_ops
from cw_platform.provider_instances import build_config_view, list_instance_ids, normalize_instance_id

try:
    from plexapi.myplex import MyPlexAccount

    _HAVE_PLEXAPI = True
except Exception:
    MyPlexAccount = None  # type: ignore[assignment]
    _HAVE_PLEXAPI = False


# path helpers
def _state_path() -> Path:
    return CONFIG / "state.json"


HIDE_PATH: Path = CONFIG / "watchlist_hide.json"


def _load_hide_set() -> set[str]:
    try:
        if HIDE_PATH.exists():
            data = json.loads(HIDE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return {str(x) for x in data}
    except Exception:
        pass
    return set()


def _save_hide_set(hide: set[str]) -> None:
    try:
        HIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        HIDE_PATH.write_text(json.dumps(sorted(hide)), encoding="utf-8")
    except Exception:
        pass


def _load_state_dict(path: Path) -> dict[str, Any]:
    try:
        if path.exists():
            return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        pass
    return {}


def _save_state_dict(path: Path, state: dict[str, Any]) -> None:
    try:
        path.write_text(json.dumps(state, ensure_ascii=False), encoding="utf-8")
    except Exception:
        pass


# Registry and provider helpers
def _registry_sync_providers() -> list[str]:
    return [
        k.replace("_mod_", "").upper()
        for k in (MR_MODULES.get("SYNC") or {}).keys()
    ]


def _normalize_label(pid: str) -> str:
    mapping = {
        "PLEX": "Plex",
        "SIMKL": "SIMKL",
        "TRAKT": "Trakt",
        "ANILIST": "AniList",
        "JELLYFIN": "Jellyfin",
        "EMBY": "Emby",
        "MDBLIST": "MDBList",
        "TMDB": "TMDb",
        "CROSSWATCH": "CrossWatch",
    }
    return mapping.get(pid.upper(), pid.title())


def _feat_enabled(fmap: dict[str, Any] | None, name: str) -> bool:
    v = (fmap or {}).get(name)
    if v is True:
        return True
    if isinstance(v, dict):
        return bool(v.get("enable", True))
    return False


def _configured_via_registry(pid: str, cfg: dict[str, Any]) -> bool:
    try:
        ops = load_sync_ops(pid)
        if not ops:
            return False
        feats = ops.features() or {}
        if feats and not _feat_enabled(feats, "watchlist"):
            return False
        if ops.is_configured(cfg):
            return True
        for inst_id in list_instance_ids(cfg, pid):
            if inst_id == "default":
                continue
            try:
                view = build_config_view(cfg, {pid: inst_id})
                if ops.is_configured(view):
                    return True
            except Exception:
                continue
        return False
    except Exception:
        return False

_DEFAULT_INSTANCE = "default"


def _prov(state: dict[str, Any], provider: str) -> dict[str, Any]:
    return (state.get("providers") or {}).get((provider or "").upper().strip()) or {}


def _iter_provider_instance_blocks(
    state: dict[str, Any],
    provider: str,
) -> list[tuple[str, dict[str, Any]]]:
    p = _prov(state, provider)
    out: list[tuple[str, dict[str, Any]]] = []
    if isinstance(p, dict) and p:
        out.append((_DEFAULT_INSTANCE, p))
        insts = p.get("instances") or {}
        if isinstance(insts, dict):
            for inst_id, blk in insts.items():
                if isinstance(blk, dict) and blk:
                    out.append((str(inst_id), blk))
    return out


def _items_from_block(block: dict[str, Any]) -> dict[str, Any]:
    wl = (((block.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
    if isinstance(wl, dict) and wl:
        return wl
    legacy = block.get("items") or {}
    return legacy if isinstance(legacy, dict) else {}


def _pick_best_item(a: dict[str, Any], b: dict[str, Any]) -> dict[str, Any]:
    if not a:
        return dict(b or {})
    if not b:
        return dict(a or {})
    sa, sb = _rich_ids_score(a), _rich_ids_score(b)
    best = a if sa >= sb else b
    other = b if best is a else a
    out = dict(best)
    for k, v in (other or {}).items():
        if k not in out or out[k] in (None, "", [], {}):
            out[k] = v
    if isinstance(best.get("ids"), dict) or isinstance(other.get("ids"), dict):
        ids = dict(other.get("ids") or {})
        ids.update(dict(best.get("ids") or {}))
        out["ids"] = ids
    return out


def _get_provider_item_refs(
    state: dict[str, Any],
    provider: str,
    instance_id: str | None = None,
) -> dict[str, list[dict[str, Any]]]:
    want = (instance_id or "").strip()
    want_lc = want.lower()
    out: dict[str, list[dict[str, Any]]] = {}
    for inst_id, blk in _iter_provider_instance_blocks(state, provider):
        if want and want_lc not in {"all", "any", "*"} and inst_id != want:
            continue
        items = _items_from_block(blk)
        if not isinstance(items, dict) or not items:
            continue
        for k, it in items.items():
            if not it:
                continue
            d = dict(it)
            if inst_id != _DEFAULT_INSTANCE:
                d.setdefault("_cw_instance", inst_id)
            out.setdefault(str(k), []).append(d)
    return out


def _get_provider_items(
    state: dict[str, Any],
    provider: str,
    instance_id: str | None = None,
) -> dict[str, Any]:
    refs = _get_provider_item_refs(state, provider, instance_id=instance_id)
    out: dict[str, Any] = {}
    for k, arr in refs.items():
        best: dict[str, Any] = {}
        for it in arr:
            best = _pick_best_item(best, it)
        if best:
            best.pop("_cw_instance", None)
            out[k] = best
    return out

def _find_item_in_state(state: dict[str, Any], key: str) -> dict[str, Any]:
    for prov in _registry_sync_providers():
        it = _get_provider_items(state, prov).get(key)
        if it:
            return dict(it)
    return {}

def _find_item_in_state_for_provider(
    state: dict[str, Any],
    key: str,
    provider: str,
    instance_id: str | None = None,
) -> dict[str, Any]:
    it = _get_provider_items(state, provider, instance_id=instance_id).get(key)
    return dict(it) if it else {}


# ID and type helpers
def _norm_type(x: str | None) -> str:
    t = (x or "").strip().lower()
    if t in {"tv", "show", "shows", "series", "season", "episode"}:
        return "tv"
    if t in {"anime"}:
        return "anime"
    if t in {"movie", "movies", "film", "films"}:
        return "movie"
    return ""


def _rich_ids_score(item: dict[str, Any] | None) -> int:
    if not isinstance(item, dict):
        return 0
    ids = (item.get("ids") or {}) | {
        k: item.get(k) for k in ("tmdb", "imdb", "tvdb", "trakt", "slug")
    }
    return sum(1 for k in ("tmdb", "imdb", "tvdb", "trakt", "slug") if ids.get(k))


def _ids_from_key_or_item(key: str, item: dict[str, Any]) -> dict[str, Any]:
    ids = dict((item or {}).get("ids") or {})
    parts = [t for t in str(key or "").split(":") if t]
    if len(parts) >= 2:
        k = parts[-2].lower().strip()
        v = parts[-1].strip()
        if k in {"imdb", "tmdb", "tvdb", "trakt", "slug", "jellyfin", "emby", "anilist", "mal"} and v:
            ids.setdefault(k, v)
    if "thetvdb" in ids and "tvdb" not in ids:
        ids["tvdb"] = ids.get("thetvdb")
    imdb = str(ids.get("imdb") or "").strip()
    if imdb and imdb.isdigit():
        ids["imdb"] = f"tt{imdb}"
    out: dict[str, Any] = {}
    for k in (
        "simkl",
        "imdb",
        "tmdb",
        "tvdb",
        "trakt",
        "slug",
        "jellyfin",
        "emby",
        "anilist",
        "mal",
    ):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out[k] = s
    return out


def _type_from_item_or_guess(item: dict[str, Any], key: str) -> str:
    typ = (item.get("type") or "").lower().strip()
    if typ == "movie":
        return "movie"
    if typ in {"tv", "show", "series", "anime"}:
        return "tv"
    ids = item.get("ids") or {}
    if ids.get("tvdb") or ids.get("thetvdb") or ids.get("anilist") or ids.get("mal"):
        return "tv"
    pref = (key or "").split(":", 1)[0].lower().strip()
    return "tv" if pref in {"tvdb", "thetvdb", "anilist", "mal"} else "movie"


_SIMKL_ID_KEYS = ("simkl", "imdb", "tmdb", "tvdb", "slug")


def _simkl_filter_ids(ids: dict[str, Any]) -> dict[str, Any]:
    return {k: str(v) for k, v in ids.items() if k in _SIMKL_ID_KEYS and v}


# PLEX GUID and added date helpers
def _pick_added(d: dict[str, Any]) -> str | None:
    if not isinstance(d, dict):
        return None
    for k in ("added", "added_at", "addedAt", "date_added", "created_at", "createdAt"):
        if d.get(k):
            return str(d[k])
    nested = d.get("dates") or d.get("meta") or d.get("attributes") or {}
    if isinstance(nested, dict):
        for k in ("added", "added_at", "created", "created_at"):
            if nested.get(k):
                return str(nested[k])
    return None


def _iso_to_epoch(iso: str | None) -> int:
    if not iso:
        return 0
    try:
        s = str(iso).strip().replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0


def _norm_guid(g: str) -> tuple[str, str]:
    s = (g or "").strip()
    if not s:
        return "", ""
    s = s.split("?", 1)[0]
    if s.startswith("com.plexapp.agents."):
        try:
            rest = s.split("com.plexapp.agents.", 1)[1]
            prov, ident = rest.split("://", 1)
            return prov.lower().replace("thetvdb", "tvdb"), ident.strip()
        except Exception:
            return "", ""
    try:
        prov, ident = s.split("://", 1)
        return prov.lower().replace("thetvdb", "tvdb"), ident.strip()
    except Exception:
        return "", ""


def _guid_variants_from_key_or_item(
    key: str,
    item: dict[str, Any] | None = None,
) -> list[str]:
    prov, _, ident = (key or "").partition(":")
    prov, ident = prov.lower().strip(), ident.strip()
    if not (prov and ident):
        ids = (item or {}).get("ids") or {}
        if ids.get("imdb"):
            prov, ident = "imdb", str(ids["imdb"])
        elif ids.get("tmdb"):
            prov, ident = "tmdb", str(ids["tmdb"])
        elif ids.get("tvdb") or ids.get("thetvdb"):
            prov, ident = "tvdb", str(ids.get("tvdb") or ids.get("thetvdb"))
    if not (prov and ident):
        return []
    prov = "tvdb" if prov in {"thetvdb", "tvdb"} else prov
    base = f"{prov}://{ident}"
    return [
        base,
        f"com.plexapp.agents.{prov}://{ident}",
        f"com.plexapp.agents.{prov}://{ident}?lang=en",
    ]


def _extract_plex_identifiers(
    item: dict[str, Any],
) -> tuple[str | None, str | None]:
    if not isinstance(item, dict):
        return None, None
    ids = item.get("ids") or {}
    guid = (
        item.get("guid")
        or ids.get("guid")
        or (item.get("plex") or {}).get("guid")
    )
    rating = (
        item.get("ratingKey")
        or item.get("id")
        or ids.get("ratingKey")
        or (item.get("plex") or {}).get("ratingKey")
        or (item.get("plex") or {}).get("id")
    )
    return (str(guid) if guid else None, str(rating) if rating else None)


# Jellyfin/Emby API helpers
def _jf_base(cfg: dict[str, Any]) -> str:
    base = (cfg.get("server") or "").strip()
    if not base:
        raise RuntimeError("Jellyfin/Emby: missing 'server'")
    return base if base.endswith("/") else base + "/"


def _jf_headers(cfg: dict[str, Any]) -> dict[str, str]:
    token = (
        cfg.get("access_token") or cfg.get("api_key") or cfg.get("token") or ""
    ).strip()
    dev = (cfg.get("device_id") or "CrossWatch").strip() or "CrossWatch"
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "X-Emby-Authorization": (
            'MediaBrowser Client="CrossWatch", '
            f'Device="WebUI", DeviceId="{dev}", Version="1.0.0"'
        ),
    }
    if token:
        headers["X-Emby-Token"] = token
    return headers


def _jf_require_user(cfg: dict[str, Any]) -> str:
    uid = (cfg.get("user_id") or "").strip()
    if not uid:
        raise RuntimeError("Jellyfin/Emby: missing 'user_id'")
    return uid


def _extract_jf_id(item: dict[str, Any], key: str) -> str | None:
    ids = (item or {}).get("ids") or {}
    cand = (
        ids.get("jellyfin")
        or ids.get("emby")
        or item.get("jellyfinId")
        or item.get("embyId")
        or item.get("jf_id")
        or item.get("Id")
        or item.get("id")
    )
    if cand:
        return str(cand)
    pref, _, val = (key or "").partition(":")
    if pref.lower().strip() in {"jellyfin", "emby"} and val.strip():
        return val.strip()
    return None


def _jf_get(
    base: str,
    path: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
) -> dict[str, Any]:
    url = base + path.lstrip("/")
    if params:
        url += ("&" if "?" in url else "?") + urlencode(
            {k: v for k, v in params.items() if v is not None}
        )
    r = requests.get(url, headers=headers, timeout=45)
    if not r.ok:
        raise RuntimeError(
            f"Jellyfin/Emby GET {path} -> {r.status_code}: {getattr(r, 'text', '')}"
        )
    try:
        j = r.json()
    except Exception:
        j = {}
    return j if isinstance(j, dict) else {}

def _delete_on_crosswatch_batch(items: list[dict[str, Any]], cfg: dict[str, Any]) -> None:
    ops = load_sync_ops("CROSSWATCH")
    if not ops:
        raise RuntimeError("CROSSWATCH module not available")
    payload: list[dict[str, Any]] = []
    for it in items or []:
        obj = it.get("item") if isinstance(it.get("item"), dict) else {}
        d = dict(obj or {})
        ids = _ids_from_key_or_item(str(it.get("key") or ""), d)
        if ids:
            existing = d.get("ids")
            d["ids"] = (dict(existing) | dict(ids)) if isinstance(existing, dict) else dict(ids)
        t = it.get("type")
        if t and "type" not in d:
            d["type"] = t
        payload.append(d)

    r = ops.remove(cfg, payload, feature="watchlist", dry_run=False) or {}
    if not isinstance(r, dict) or not r.get("ok"):
        raise RuntimeError(f"CROSSWATCH delete failed: {r.get('error')}")
    if int(r.get("count", 0) or 0) < 1:
        raise RuntimeError("CROSSWATCH delete matched 0 items")


def _delete_on_anilist_batch(items: list[dict[str, Any]], cfg: dict[str, Any]) -> None:
    ops = load_sync_ops("ANILIST")
    if not ops:
        raise RuntimeError("ANILIST module not available")

    payload: list[dict[str, Any]] = []
    for it in items or []:
        obj = it.get("item") if isinstance(it.get("item"), dict) else {}
        d = dict(obj or {})
        ids = _ids_from_key_or_item(str(it.get("key") or ""), d)
        if ids:
            existing = d.get("ids")
            d["ids"] = (dict(existing) | dict(ids)) if isinstance(existing, dict) else dict(ids)
        if "type" not in d and (ids.get("anilist") or ids.get("mal")):
            d["type"] = "anime"
        payload.append(d)

    r = ops.remove(cfg, payload, feature="watchlist", dry_run=False) or {}
    if not isinstance(r, dict) or not r.get("ok"):
        raise RuntimeError(f"ANILIST delete failed: {r.get('error')}")
    if int(r.get("count", 0) or 0) < 1:
        raise RuntimeError("ANILIST delete matched 0 items")


def _delete_on_tmdb_batch(items: list[dict[str, Any]], cfg: dict[str, Any]) -> None:
    ops = load_sync_ops("TMDB")
    if not ops:
        raise RuntimeError("TMDB module not available")

    payload: list[dict[str, Any]] = []
    for it in items or []:
        obj = it.get("item") if isinstance(it.get("item"), dict) else {}
        d = dict(obj or {})
        ids = _ids_from_key_or_item(str(it.get("key") or ""), d)
        if ids:
            existing = d.get("ids")
            d["ids"] = (dict(existing) | dict(ids)) if isinstance(existing, dict) else dict(ids)
        t = it.get("type")
        if t and "type" not in d:
            d["type"] = t
        payload.append(d)

    r = ops.remove(cfg, payload, feature="watchlist", dry_run=False) or {}
    if not isinstance(r, dict) or not r.get("ok"):
        raise RuntimeError(f"TMDB delete failed: {r.get('error')}")
    if int(r.get("count", 0) or 0) < 1:
        raise RuntimeError("TMDB delete matched 0 items")


def _jf_delete(
    base: str,
    path: str,
    headers: dict[str, str],
    params: dict[str, Any] | None = None,
) -> None:
    url = base + path.lstrip("/")
    if params:
        url += ("&" if "?" in url else "?") + urlencode(
            {k: v for k, v in params.items() if v is not None}
        )
    r = requests.delete(url, headers=headers, timeout=45)
    st = int(getattr(r, "status_code", 0) or 0)
    if st not in (200, 202, 204, 404) and not r.ok:
        raise RuntimeError(
            f"Jellyfin/Emby DELETE {path} -> {st}: {getattr(r, 'text', '')}"
        )


def _jf_provider_tokens(ids: dict[str, Any]) -> list[str]:
    out: list[str] = []
    for k in ("tmdb", "imdb", "tvdb", "trakt"):
        v = ids.get(k)
        if not v:
            continue
        out += [f"{k.capitalize()}:{v}", f"{k}:{v}", str(v)]
    uniq: list[str] = []
    seen: set[str] = set()
    for t in out:
        if t not in seen:
            seen.add(t)
            uniq.append(t)
    return uniq


def _jf_find_playlist_id(
    cfg: dict[str, Any],
    headers: dict[str, str],
    name: str,
) -> str | None:
    j = _jf_get(
        _jf_base(cfg),
        f"Users/{_jf_require_user(cfg)}/Items",
        headers,
        {
            "IncludeItemTypes": "Playlist",
            "Recursive": "true",
            "SortBy": "SortName",
            "Fields": "ItemCounts",
            "Limit": 1000,
        },
    )
    name_lc = (name or "").strip().lower()
    for it in (j.get("Items") or []):
        if str(it.get("Name", "")).strip().lower() == name_lc:
            return str(it.get("Id") or "")
    return None


def _jf_find_collection_id(
    cfg: dict[str, Any],
    headers: dict[str, str],
    name: str,
) -> str | None:
    j = _jf_get(
        _jf_base(cfg),
        f"Users/{_jf_require_user(cfg)}/Items",
        headers,
        {
            "IncludeItemTypes": "BoxSet",
            "Recursive": "true",
            "SearchTerm": name,
            "Limit": 1000,
        },
    )
    name_lc = (name or "").strip().lower()
    for it in (j.get("Items") or []):
        if str(it.get("Name", "")).strip().lower() == name_lc:
            return str(it.get("Id") or "")
    j2 = _jf_get(
        _jf_base(cfg),
        f"Users/{_jf_require_user(cfg)}/Items",
        headers,
        {
            "IncludeItemTypes": "BoxSet",
            "Recursive": "true",
            "Limit": 1000,
        },
    )
    for it in (j2.get("Items") or []):
        if str(it.get("Name", "")).strip().lower() == name_lc:
            return str(it.get("Id") or "")
    return None


def _jf_collection_items(
    cfg: dict[str, Any],
    headers: dict[str, str],
    coll_id: str,
) -> list[dict[str, Any]]:
    j = _jf_get(
        _jf_base(cfg),
        f"Users/{_jf_require_user(cfg)}/Items",
        headers,
        {
            "ParentId": coll_id,
            "Recursive": "true",
            "IncludeItemTypes": "Movie,Series",
            "Fields": "ProviderIds",
            "Limit": 5000,
        },
    )
    return (j.get("Items") or []) if isinstance(j, dict) else []


def _jf_playlist_items(
    cfg: dict[str, Any],
    headers: dict[str, str],
    pl_id: str,
) -> list[dict[str, Any]]:
    j = _jf_get(
        _jf_base(cfg),
        f"Playlists/{pl_id}/Items",
        headers,
        {
            "UserId": _jf_require_user(cfg),
            "Fields": "ProviderIds",
            "Limit": 5000,
        },
    )
    return (j.get("Items") or []) if isinstance(j, dict) else []


def _jf_favorite_items(
    cfg: dict[str, Any],
    headers: dict[str, str],
) -> list[dict[str, Any]]:
    j = _jf_get(
        _jf_base(cfg),
        f"Users/{_jf_require_user(cfg)}/Items",
        headers,
        {
            "Recursive": "true",
            "Filters": "IsFavorite",
            "IncludeItemTypes": "Movie,Series",
            "Fields": "ProviderIds",
            "Limit": 5000,
        },
    )
    return (j.get("Items") or []) if isinstance(j, dict) else []


def _jf_index_watchlist(
    cfg: dict[str, Any],
    headers: dict[str, str],
    mode: str,
    playlist: str,
) -> dict[str, Any]:
    index: dict[str, str] = {}
    entry_by_item: dict[str, str] = {}
    if mode == "playlist":
        pl_id = _jf_find_playlist_id(cfg, headers, playlist)
        if not pl_id:
            return {"by_token": index, "entry_by_item": entry_by_item}
        items = _jf_playlist_items(cfg, headers, pl_id)
    elif mode == "collection":
        coll_id = _jf_find_collection_id(cfg, headers, playlist)
        if not coll_id:
            return {"by_token": index, "entry_by_item": entry_by_item}
        items = _jf_collection_items(cfg, headers, coll_id)
    else:
        items = _jf_favorite_items(cfg, headers)
    for it in items or []:
        try:
            iid = str(it.get("Id") or "")
            if not iid:
                continue
            prov = it.get("ProviderIds") or {}
            tokens: list[str] = []
            for k in ("Tmdb", "Imdb", "Tvdb", "Trakt"):
                v = prov.get(k)
                if not v:
                    continue
                if k == "Imdb" and isinstance(v, list):
                    v = v[0]
                tokens += [f"{k}:{v}", f"{k.lower()}:{v}", str(v)]
            for t in tokens:
                if t not in index:
                    index[t] = iid
            if it.get("PlaylistItemId"):
                entry_by_item[iid] = str(it["PlaylistItemId"])
        except Exception:
            continue
    return {"by_token": index, "entry_by_item": entry_by_item}


def _jf_lookup_by_provider_ids(
    cfg: dict[str, Any],
    headers: dict[str, str],
    tokens: list[str],
) -> str | None:
    if not tokens:
        return None
    for tok in tokens:
        try:
            j = _jf_get(
                _jf_base(cfg),
                f"Users/{_jf_require_user(cfg)}/Items",
                headers,
                {
                    "Recursive": "true",
                    "IncludeItemTypes": "Movie,Series",
                    "AnyProviderIdEquals": tok,
                    "Limit": 1,
                    "Fields": "ProviderIds",
                },
            )
            items = (j.get("Items") or []) if isinstance(j, dict) else []
            if items and items[0].get("Id"):
                return str(items[0]["Id"])
        except Exception:
            continue
    return None


# Merge and build watchlist helpers
def _get_items(state: dict[str, Any], prov: str) -> dict[str, Any]:
    return _get_provider_items(state, prov)


def build_watchlist(state: dict[str, Any], tmdb_ok: bool) -> list[dict[str, Any]]:
    providers = _registry_sync_providers()
    hidden = _load_hide_set()
    by_key: dict[str, list[tuple[str, str, dict[str, Any]]]] = {}

    for p in providers:
        refs = _get_provider_item_refs(state, p, instance_id="all")
        for k, arr in refs.items():
            if str(k) in hidden:
                continue
            for it in arr or []:
                inst = str((it or {}).get("_cw_instance") or _DEFAULT_INSTANCE)
                by_key.setdefault(str(k), []).append((p.lower(), inst, it))

    out: list[dict[str, Any]] = []
    for key, candidates in by_key.items():
        if not candidates:
            continue

        sources = sorted({n for n, _, _ in candidates})
        if not sources:
            continue

        sources_by_provider: dict[str, list[str]] = {}
        info_best: dict[str, Any] = {}
        declared = {_norm_type((it or {}).get("type")) for _, _, it in candidates if it}
        declared.discard("")

        added_src = sources[0]
        added_instance = _DEFAULT_INSTANCE
        added_epoch = 0
        added_when: str | None = None

        for n, inst, it in candidates:
            sources_by_provider.setdefault(n, [])
            if inst not in sources_by_provider[n]:
                sources_by_provider[n].append(inst)

            info_best = _pick_best_item(info_best, it or {})

            ep = _iso_to_epoch(_pick_added(it or {}))
            if ep and ep > added_epoch:
                added_src, added_instance, added_epoch = n, inst, ep
                added_when = _pick_added(it or {})

        for n in sources_by_provider:
            insts = sources_by_provider[n]
            sources_by_provider[n] = sorted(insts, key=lambda x: (x != _DEFAULT_INSTANCE, x))

        info = dict(info_best or {})
        info.pop("_cw_instance", None)

        if "anime" in declared:
            typ = "anime"
        elif "tv" in declared:
            typ = "tv"
        elif "movie" in declared:
            typ = "movie"
        else:
            ids_ = (info.get("ids") or {}) | {k: info.get(k) for k in ("tmdb", "imdb", "tvdb", "trakt", "slug", "anilist", "mal")}
            pref = (key or "").split(":", 1)[0].lower().strip()
            if ids_.get("anilist") or ids_.get("mal") or pref in {"anilist", "mal"}:
                typ = "anime"
            else:
                typ = "tv" if ids_.get("tvdb") else "movie"

        title = info.get("title") or info.get("name") or ""
        year = info.get("year") or info.get("release_year")
        tmdb_id = (info.get("ids") or {}).get("tmdb") or info.get("tmdb")

        if not added_epoch:
            added_when = _pick_added(info)
            added_epoch = _iso_to_epoch(added_when)

        status = f"{sources[0]}_only" if len(sources) == 1 else "both"

        tmdb_str = str(tmdb_id)
        tmdb_value = int(tmdb_str) if tmdb_str.isdigit() else tmdb_id

        out.append(
            {
                "key": key,
                "type": typ,
                "title": title,
                "year": year,
                "tmdb": tmdb_value,
                "status": status,
                "sources": sources,
                "sources_by_provider": sources_by_provider,
                "added_epoch": added_epoch,
                "added_when": added_when,
                "added_src": added_src,
                "added_instance": added_instance,
                "categories": [],
                "ids": _ids_from_key_or_item(key, info),
            }
        )

    out.sort(
        key=lambda x: (x.get("added_epoch") or 0, x.get("year") or 0),
        reverse=True,
    )
    return out

def _del_key_from_provider_items(
    state: dict[str, Any],
    provider: str,
    key: str,
    instance_id: str | None = None,
) -> bool:
    prov = (provider or "").upper().strip()
    providers = state.get("providers") or {}
    if prov not in providers:
        return False

    want = (instance_id or "").strip()
    want_lc = want.lower()
    changed = False

    def _del_from(block: dict[str, Any]) -> bool:
        hit = False
        wl = (((block.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
        if isinstance(wl, dict) and key in wl:
            wl.pop(key, None)
            hit = True
        legacy = block.get("items") or {}
        if isinstance(legacy, dict) and key in legacy:
            legacy.pop(key, None)
            hit = True
        return hit

    p = providers[prov] or {}
    if not want or want_lc in {"all", "any", "*"} or want == _DEFAULT_INSTANCE:
        changed |= _del_from(p)

    insts = (p.get("instances") or {})
    if isinstance(insts, dict):
        for inst_id, blk in insts.items():
            if not isinstance(blk, dict):
                continue
            if want and want_lc not in {"all", "any", "*"} and str(inst_id) != want:
                continue
            changed |= _del_from(blk)

    return changed

def _delete_on_plex_single(
    key: str,
    state: dict[str, Any],
    cfg: dict[str, Any],
) -> None:
    if not _HAVE_PLEXAPI:
        raise RuntimeError("plexapi is not available")

    def _id_tokens_from_key(k: str) -> set[str]:
        parts = [t.strip() for t in str(k or "").split(":") if t]
        if len(parts) == 2:
            t, v = parts[-2].lower(), parts[-1]
            if t == "imdb" and v.isdigit():
                v = f"tt{v}"
            return {f"{t}:{v}"}
        return set()

    def _id_tokens_from_item(it: dict[str, Any]) -> set[str]:
        ids = dict((it or {}).get("ids") or {})
        out_tokens: set[str] = set()
        for t in ("tmdb", "imdb", "tvdb", "trakt"):
            v = ids.get(t)
            if v is None:
                continue
            s = str(v).strip()
            if not s:
                continue
            if t == "imdb" and s.isdigit():
                s = f"tt{s}"
            out_tokens.add(f"{t}:{s}")
        return out_tokens

    def _tokens_from_plex_obj(m: Any) -> set[str]:
        toks: set[str] = set()

        def _one(val: str) -> None:
            val = (val or "").split("?", 1)[0]
            if "://" in val:
                scheme, ident = val.split("://", 1)
                if scheme and ident:
                    toks.add(f"{scheme.lower()}:{ident}")

        _one(getattr(m, "guid", "") or "")
        try:
            for g in getattr(m, "guids", []) or []:
                raw = getattr(g, "id", g)
                _one(str(raw or ""))
        except Exception:
            pass
        rk = str(getattr(m, "ratingKey", "") or getattr(m, "id", "")).strip()
        if rk:
            toks.add(f"rk:{rk}")
        return toks

    token = (cfg.get("plex") or {}).get("account_token", "").strip()
    if not token:
        raise RuntimeError("missing plex token")
    account = cast(Any, MyPlexAccount)(token=token)

    item = _find_item_in_state(state, key) or {}
    guid, rk = _extract_plex_identifiers(item)
    variants = _guid_variants_from_key_or_item(key, item)
    if guid:
        variants = list(dict.fromkeys(variants + [guid]))
    targets_guid = {_norm_guid(v) for v in variants if v}

    target_tokens = set()
    target_tokens |= _id_tokens_from_key(key)
    target_tokens |= _id_tokens_from_item(item)
    if str(rk or "").strip():
        target_tokens.add(f"rk:{str(rk).strip()}")

    wl = account.watchlist(maxresults=100000)

    def matches(m: Any) -> bool:
        cand_tokens = _tokens_from_plex_obj(m)
        if targets_guid:
            cand_guids = {
                _norm_guid((getattr(m, "guid", "") or "").split("?", 1)[0])
            }
            try:
                for g in getattr(m, "guids", []) or []:
                    cand_guids.add(
                        _norm_guid(
                            str(getattr(g, "id", g) or "").split("?", 1)[0]
                        )
                    )
            except Exception:
                pass
            if any(c in targets_guid for c in cand_guids):
                return True
        return bool(target_tokens & cand_tokens)

    found = next((m for m in wl if matches(m)), None)
    if not found:
        raise RuntimeError("item not found in Plex watchlist")

    removed = False
    try:
        rm: Any = getattr(found, "removeFromWatchlist", None)
        if callable(rm):
            rm()
            removed = True
    except Exception:
        pass
    if not removed:
        account.removeFromWatchlist([found])

    if any(matches(m) for m in account.watchlist(maxresults=100000)):
        raise RuntimeError("PlexAPI reported removal but item still present")


_SIMKL_HIST = "https://api.simkl.com/sync/history/remove"
_SIMKL_WL = "https://api.simkl.com/sync/watchlist/remove"


def _simkl_headers(cfg: dict[str, Any]) -> dict[str, str]:
    return {
        "User-Agent": "CrossWatch/WebUI",
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {cfg.get('access_token', '')}",
        "simkl-api-key": cfg.get("client_id", ""),
    }


def _post_simkl_delete(
    url: str,
    hdr: dict[str, str],
    payload: dict[str, Any],
) -> dict[str, Any]:
    r = requests.post(url, headers=hdr, json=payload, timeout=45)
    if not r.ok:
        raise RuntimeError(
            f"SIMKL delete {r.status_code} {getattr(r, 'text', '')}"
        )
    try:
        data = r.json()
    except Exception:
        return {}
    return data if isinstance(data, dict) else {}


def _simkl_deleted_count(resp: dict[str, Any]) -> int:
    deleted = resp.get("deleted") or {}
    if not isinstance(deleted, dict):
        return 0
    return sum(
        int(deleted.get(k, 0) or 0)
        for k in ("movies", "shows", "episodes", "seasons")
    )


def _delete_on_simkl_batch(
    items: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> None:
    token = (cfg.get("access_token", "")).strip()
    client_id = (cfg.get("client_id", "")).strip()
    if not (token and client_id):
        raise RuntimeError("SIMKL not configured")
    payload: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items:
        ids = _simkl_filter_ids(_ids_from_key_or_item(it["key"], it["item"]))
        if not ids:
            continue
        if it["type"] == "movie":
            payload["movies"].append({"ids": ids})
        else:
            payload["shows"].append({"ids": ids})
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        raise RuntimeError("SIMKL delete: no resolvable IDs")
    hdr = _simkl_headers(cfg)
    if _simkl_deleted_count(_post_simkl_delete(_SIMKL_WL, hdr, payload)) > 0:
        return
    if _simkl_deleted_count(_post_simkl_delete(_SIMKL_HIST, hdr, payload)) > 0:
        return
    raise RuntimeError(f"SIMKL delete matched 0 items. Payload={payload}")


_TRAKT_REMOVE = "https://api.trakt.tv/sync/watchlist/remove"


def _trakt_headers(cfg: dict[str, Any]) -> dict[str, str]:
    tok = (cfg.get("access_token") or cfg.get("token") or "").strip()
    return {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "User-Agent": "CrossWatch/WebUI",
        "trakt-api-version": "2",
        "trakt-api-key": (cfg.get("client_id") or "").strip(),
        "Authorization": f"Bearer {tok}" if tok else "",
    }


def _delete_on_trakt_batch(
    items: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> None:
    hdr = _trakt_headers(cfg)
    if not (hdr.get("Authorization") and hdr.get("trakt-api-key")):
        raise RuntimeError("TRAKT not configured")
    payload: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items:
        ids = _ids_from_key_or_item(it["key"], it["item"])
        entry = {
            k: ids[k] for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k)
        }
        if not entry:
            continue
        if it["type"] == "movie":
            payload["movies"].append({"ids": entry})
        else:
            payload["shows"].append({"ids": entry})
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        raise RuntimeError("TRAKT delete: no resolvable IDs")
    r = requests.post(_TRAKT_REMOVE, headers=hdr, json=payload, timeout=45)
    if not r.ok:
        raise RuntimeError(
            f"TRAKT delete failed: {getattr(r, 'text', 'no response')}"
        )


def _delete_on_jellyfin_batch(
    items: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> None:
    hdr = _jf_headers(cfg)
    base = _jf_base(cfg)
    user = _jf_require_user(cfg)
    mode = (cfg.get("watchlist", {}).get("mode") or "favorites").strip().lower()
    wl_name = (
        cfg.get("watchlist", {}).get("playlist_name") or "Watchlist"
    ).strip()

    idx = _jf_index_watchlist(cfg, hdr, mode, wl_name)
    by_token = idx.get("by_token") or {}
    entry_by = idx.get("entry_by_item") or {}

    jf_ids: list[str] = []
    for it in items or []:
        key = it.get("key", "")
        itm = it.get("item") or {}
        ids = _ids_from_key_or_item(key, itm)
        jf_id = (
            _extract_jf_id(itm, key)
            or next(
                (by_token.get(t) for t in _jf_provider_tokens(ids) if by_token.get(t)),
                None,
            )
            or _jf_lookup_by_provider_ids(
                cfg,
                hdr,
                _jf_provider_tokens(ids),
            )
        )
        if jf_id:
            jf_ids.append(str(jf_id))
    jf_ids = list(dict.fromkeys(jf_ids))
    if not jf_ids:
        raise RuntimeError("Jellyfin delete: no resolvable ItemIds")

    if mode == "favorites":
        ok = 0
        last: Exception | None = None
        for iid in jf_ids:
            try:
                _jf_delete(base, f"Users/{user}/FavoriteItems/{iid}", hdr)
                ok += 1
            except Exception as e:
                last = e
        if ok == 0:
            raise last or RuntimeError("Jellyfin favorites delete failed")
    elif mode == "playlist":
        pl_id = _jf_find_playlist_id(cfg, hdr, wl_name)
        if not pl_id:
            raise RuntimeError(f"Jellyfin: playlist '{wl_name}' not found")
        entries: list[str] = [
            str(entry_by[iid]) for iid in jf_ids if entry_by.get(iid)
        ]
        params = (
            {"EntryIds": ",".join(entries)}
            if entries
            else {"Ids": ",".join(jf_ids)}
        )
        _jf_delete(base, f"Playlists/{pl_id}/Items", hdr, params=params)
    elif mode == "collection":
        coll_id = _jf_find_collection_id(cfg, hdr, wl_name)
        if not coll_id:
            raise RuntimeError(f"Jellyfin: collection '{wl_name}' not found")
        _jf_delete(
            base,
            f"Collections/{coll_id}/Items",
            hdr,
            params={"Ids": ",".join(jf_ids)},
        )
    else:
        raise RuntimeError(f"Jellyfin: unknown mode '{mode}'")


def _delete_on_emby_batch(
    items: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> None:
    _delete_on_jellyfin_batch(items, cfg)


def _delete_on_plex_batch(
    items: list[dict[str, Any]],
    state: dict[str, Any],
    cfg: dict[str, Any],
) -> None:
    for it in items or []:
        _delete_on_plex_single(it["key"], state, cfg)


_MDBLIST_REMOVE = "https://api.mdblist.com/watchlist/items/remove"


def _delete_on_mdblist_batch(
    items: list[dict[str, Any]],
    cfg: dict[str, Any],
) -> None:
    api_key = (cfg.get("api_key") or cfg.get("apikey") or "").strip()
    if not api_key:
        raise RuntimeError("MDBLIST not configured")
    payload: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": []}
    for it in items or []:
        ids = _ids_from_key_or_item(it["key"], it["item"])
        entry: dict[str, Any] = {}
        tmdb = ids.get("tmdb")
        imdb = ids.get("imdb")
        if tmdb and str(tmdb).isdigit():
            entry["tmdb"] = int(str(tmdb))
        elif tmdb:
            entry["tmdb"] = tmdb
        if imdb:
            entry["imdb"] = imdb
        if not entry:
            continue
        if it.get("type") == "movie":
            payload["movies"].append(entry)
        else:
            payload["shows"].append(entry)
    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        raise RuntimeError("MDBLIST delete: no resolvable IDs")
    url = f"{_MDBLIST_REMOVE}?{urlencode({'apikey': api_key})}"
    r = requests.post(url, json=payload, timeout=45)
    if not r.ok:
        raise RuntimeError(
            f"MDBLIST delete failed: {getattr(r, 'text', 'no response')}"
        )


# Delete watchlist items
def delete_watchlist_batch(
    keys: list[str],
    prov: str,
    state: dict[str, Any],
    cfg: dict[str, Any],
    provider_instance: str | None = None,
    state_path: Path | None = None,
) -> dict[str, Any]:
    prov = (prov or "").upper().strip()
    keys = [k for k in (keys or []) if isinstance(k, str) and k.strip()]
    if not keys:
        return {"ok": False, "deleted": 0, "provider": prov, "status": "noop", "note": "no-keys"}

    state_path = state_path or _state_path()
    inst_raw = str(provider_instance or "").strip()
    inst_lc = inst_raw.lower()
    inst_sel = None if not inst_raw or inst_lc in {"all", "any", "*", "auto"} else normalize_instance_id(inst_raw)
    if prov == "ALL":
        inst_sel = None

    def _instance_targets(p: str) -> list[str]:
        hits: set[str] = set()
        for inst_id, blk in _iter_provider_instance_blocks(state, p):
            if inst_sel and inst_id != inst_sel:
                continue
            items = _items_from_block(blk)
            if not isinstance(items, dict) or not items:
                continue
            for k in keys:
                if k in items:
                    hits.add(inst_id)
        if not hits and inst_sel:
            hits.add(inst_sel)
        if not hits:
            hits.add(_DEFAULT_INSTANCE)
        return sorted(hits, key=lambda x: (x != _DEFAULT_INSTANCE, x))

    def _build_items(for_provider: str, inst_id: str) -> list[dict[str, Any]]:
        arr: list[dict[str, Any]] = []
        for k in keys:
            it = _find_item_in_state_for_provider(state, k, for_provider, instance_id=inst_id) or {}
            if not it and inst_id != _DEFAULT_INSTANCE:
                it = _find_item_in_state_for_provider(state, k, for_provider) or {}
            arr.append({"key": k, "item": it, "type": _type_from_item_or_guess(it, k)})
        return arr

    def _call_handler(p: str, items: list[dict[str, Any]], cfg_view: dict[str, Any]) -> None:
        if p == "PLEX":
            _delete_on_plex_batch(items, state, cfg_view)
            return
        if p == "SIMKL":
            _delete_on_simkl_batch(items, cfg_view.get("simkl", {}) or {})
            return
        if p == "TRAKT":
            _delete_on_trakt_batch(items, cfg_view.get("trakt", {}) or {})
            return
        if p == "TMDB":
            _delete_on_tmdb_batch(items, cfg_view)
            return
        if p == "JELLYFIN":
            _delete_on_jellyfin_batch(items, cfg_view.get("jellyfin", {}) or {})
            return
        if p == "EMBY":
            _delete_on_emby_batch(items, cfg_view.get("emby", {}) or {})
            return
        if p == "MDBLIST":
            _delete_on_mdblist_batch(items, cfg_view.get("mdblist", {}) or {})
            return
        if p == "ANILIST":
            _delete_on_anilist_batch(items, cfg_view)
            return
        if p == "CROSSWATCH":
            _delete_on_crosswatch_batch(items, cfg_view)
            return
        raise RuntimeError(f"delete not supported: {p}")

    def _delete_for_provider(p: str) -> dict[str, Any]:
        removed: set[str] = set()
        per_instance: dict[str, Any] = {}
        for inst in _instance_targets(p):
            cfg_view = build_config_view(cfg, {p: inst})
            try:
                _call_handler(p, _build_items(p, inst), cfg_view)
                for k in keys:
                    if _del_key_from_provider_items(state, p, k, instance_id=inst):
                        removed.add(k)
                per_instance[inst] = {"ok": True, "removed": len(removed)}
            except Exception as e:
                per_instance[inst] = {"ok": False, "error": str(e)}
        return {"ok": any(v.get("ok") for v in per_instance.values()), "per_instance": per_instance, "removed": len(removed)}

    supported = {"CROSSWATCH", "ANILIST", "PLEX", "SIMKL", "TRAKT", "TMDB", "JELLYFIN", "EMBY", "MDBLIST"}

    if prov == "ALL":
        details: dict[str, Any] = {}
        ok_any = False
        deleted_sum = 0

        for p in _registry_sync_providers():
            if p not in supported:
                details[p] = {"ok": False, "error": "delete not supported"}
                continue
            res = _delete_for_provider(p)
            details[p] = res
            ok_any |= bool(res.get("ok"))
            deleted_sum += int(res.get("removed") or 0)

        if deleted_sum:
            _save_state_dict(state_path, state)

        return {"ok": ok_any, "deleted": deleted_sum, "provider": "ALL", "details": details, "status": "ok" if ok_any else "error"}

    if prov not in supported:
        raise RuntimeError(f"unknown provider: {prov}")

    res = _delete_for_provider(prov)
    if int(res.get("removed") or 0) > 0:
        _save_state_dict(state_path, state)

    return {"ok": bool(res.get("ok")), "deleted": int(res.get("removed") or 0), "provider": prov, "details": res.get("per_instance"), "status": "ok" if res.get("ok") else "error"}

def delete_watchlist_item(
    key: str,
    state_path: Path,
    cfg: dict[str, Any],
    log: Any = None,
    provider: str | None = None,
    provider_instance: str | None = None,
) -> dict[str, Any]:
    prov = (provider or "ALL").upper().strip()
    try:
        state = _load_state_dict(state_path)
    except Exception as e:
        return {"ok": False, "status": "error", "provider": prov, "key": key, "error": str(e)}

    def _log(level: str, msg: str) -> None:
        try:
            if callable(log):
                log(level, msg)
        except Exception:
            pass

    try:
        res = delete_watchlist_batch(
            keys=[key],
            prov=prov,
            state=state,
            cfg=cfg,
            provider_instance=provider_instance,
            state_path=state_path,
        ) or {}
        _log("SYNC", f"[WL] delete {key} on {prov}: {'OK' if res.get('ok') else 'NOOP'}")
        res.setdefault("key", key)
        return res
    except Exception as e:
        _log("SYNC", f"[WL] delete {key} on {prov} failed: {e}")
        return {"ok": False, "status": "error", "provider": prov, "key": key, "error": str(e)}

def detect_available_watchlist_providers(
    cfg: dict[str, Any],
) -> list[dict[str, Any]]:
    providers = _registry_sync_providers()
    counts: dict[str, int] = {pid: 0 for pid in providers}

    try:
        from api.syncAPI import _load_state

        st = _load_state() or {}
        P = st.get("providers") or {}
        for pid in providers:
            pblk = P.get(pid) or {}
            keys: set[str] = set()
            base = (((pblk.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
            if isinstance(base, dict):
                keys |= set(base.keys())
            insts = pblk.get("instances") or {}
            if isinstance(insts, dict):
                for blk in insts.values():
                    if not isinstance(blk, dict):
                        continue
                    items = (((blk.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
                    if isinstance(items, dict):
                        keys |= set(items.keys())
            counts[pid] = len(keys)
    except Exception:
        pass

    if providers and not any(counts.values()):
        try:
            from cw_platform.orchestrator import Orchestrator

            snaps = Orchestrator(config=cfg).build_snapshots(feature="watchlist") or {}
            for pid in providers:
                counts[pid] = len(snaps.get(pid) or {})
        except Exception:
            pass

    arr: list[dict[str, Any]] = []
    for pid in providers:
        conf = _configured_via_registry(pid, cfg)
        arr.append(
            {
                "id": pid,
                "label": _normalize_label(pid),
                "configured": conf,
                "supports_delete": True,
                "supports_batch": True,
                "count": counts.get(pid, 0) if conf else 0,
            }
        )

    arr.insert(
        0,
        {
            "id": "ALL",
            "label": "All Providers",
            "configured": True,
            "supports_delete": True,
            "supports_batch": True,
            "count": sum(counts.values()),
        },
    )
    return arr


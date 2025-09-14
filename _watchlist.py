# _watchlist.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
import json
import requests

from plexapi.myplex import MyPlexAccount
from cw_platform.config_base import CONFIG

def _state_path() -> Path:
    return CONFIG / "watchlist_state.json"

# ======================================================================
# Local "hide" overlay helpers (server-side; UI gebruikt localStorage)
# ======================================================================

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

# ======================================================================
# Generic helpers (ids / type / state)
# ======================================================================

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
    return (P.get("items") or {})

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
    for prov in ("PLEX", "SIMKL", "TRAKT"):
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
    if pref in ("imdb", "tmdb", "tvdb", "trakt", "slug") and val:
        ids.setdefault(pref, val)
    if "thetvdb" in ids and "tvdb" not in ids:
        ids["tvdb"] = ids.get("thetvdb")
    return {k: ids[k] for k in ("simkl", "imdb", "tmdb", "tvdb", "trakt", "slug") if ids.get(k)}

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

# ======================================================================
# Plex GUID helpers (for PlexAPI matching)
# ======================================================================

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

# ======================================================================
# Build merged watchlist view (for UI)
# ======================================================================

def _get_items(state: Dict[str, Any], prov: str) -> Dict[str, Any]:
    return _get_provider_items(state, prov)

def build_watchlist(state: Dict[str, Any], tmdb_api_key_present: bool) -> List[Dict[str, Any]]:
    plex_items  = _get_items(state, "PLEX")
    simkl_items = _get_items(state, "SIMKL")
    trakt_items = _get_items(state, "TRAKT")

    hidden = _load_hide_set()
    out: List[Dict[str, Any]] = []
    all_keys = set(plex_items) | set(simkl_items) | set(trakt_items)

    for key in all_keys:
        if key in hidden:
            continue

        p = plex_items.get(key) or {}
        s = simkl_items.get(key) or {}
        t = trakt_items.get(key) or {}
        info = p or s or t
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
        added_epoch = max(p_ep, s_ep, t_ep)
        if added_epoch == p_ep:
            added_when, added_src = _pick_added(p), "plex"
        elif added_epoch == s_ep:
            added_when, added_src = _pick_added(s), "simkl"
        else:
            added_when, added_src = _pick_added(t), "trakt"

        sources = [name for name, it in (("plex", p), ("simkl", s), ("trakt", t)) if it]
        status = {
            1: {"plex": "plex_only", "simkl": "simkl_only", "trakt": "trakt_only"}[sources[0]],
            2: "both",
            3: "both",
        }[len(sources) if len(sources) in (1, 2, 3) else 1]

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

# ======================================================================
# Provider-specific deletes (single + batch)
# ======================================================================

# ---- Plex (single only) -----------------------------------------------
def _delete_on_plex_single(key: str, state: Dict[str, Any], cfg: Dict[str, Any]) -> None:
    token = ((cfg.get("plex", {}) or {}).get("account_token") or "").strip()
    if not token:
        raise RuntimeError("missing plex token")
    account = MyPlexAccount(token=token)

    plex_items = _get_provider_items(state, "PLEX")
    simkl_items = _get_provider_items(state, "SIMKL")
    item = plex_items.get(key) or simkl_items.get(key) or {}

    guid, ratingKey = _extract_plex_identifiers(item)
    variants = _guid_variants_from_key_or_item(key, item)
    if guid:
        variants = list(dict.fromkeys(variants + [guid]))
    targets = {_norm_guid(v) for v in variants if v}
    rk = str(ratingKey or "").strip()

    # 1) laad volledige lijst
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
        # GUID-match
        if any(_norm_guid(cg) in targets for cg in cand):
            return True
        # fallback: ratingKey als Plex ‘watchlist’ die expose’t
        m_rk = str(getattr(media, "ratingKey", "") or getattr(media, "id", "") or "").strip()
        return rk and m_rk and (rk == m_rk)

    found = next((m for m in watchlist if matches(m)), None)
    if not found:
        raise RuntimeError("item not found in Plex online watchlist")

    # 2) verwijder via item-methode als die bestaat, anders via account
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

    # 3) verificatie (volle lijst opnieuw)
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
        target = payload["movies"] if it["type"] == "movie" else payload["shows"]
        target.append({"ids": entry_ids})

    payload = {k: v for k, v in payload.items() if v}
    if not payload:
        raise RuntimeError("SIMKL delete: no resolvable IDs for requested items")

    hdr = _simkl_headers(simkl_cfg)

    resp = _post_simkl_delete(_SIMKL_HISTORY_REMOVE, hdr, payload)
    if _simkl_deleted_count(resp) == 0:
        resp2 = _post_simkl_delete(_SIMKL_WATCHLIST_REMOVE, hdr, payload)
        if _simkl_deleted_count(resp2) == 0:
            reb = {}
            if payload.get("movies"):
                reb["shows"] = payload["movies"]
            if payload.get("shows"):
                reb["movies"] = payload["shows"]
            if reb:
                resp3 = _post_simkl_delete(_SIMKL_HISTORY_REMOVE, hdr, reb)
                if _simkl_deleted_count(resp3) == 0:
                    resp4 = _post_simkl_delete(_SIMKL_WATCHLIST_REMOVE, hdr, reb)
                    if _simkl_deleted_count(resp4) == 0:
                        raise RuntimeError(
                            f"SIMKL delete matched 0 items. Payload={payload} "
                            f"Resp1={resp} Resp2={resp2} Reb={reb} Resp3={resp3} Resp4={resp4}"
                        )
            else:
                raise RuntimeError(
                    f"SIMKL delete matched 0 items (history+watchlist). Payload={payload} Resp1={resp} Resp2={resp2}"
                )

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

# ======================================================================
# Public: single delete
# ======================================================================

def delete_watchlist_item(
    key: str,
    state_path: Path,
    cfg: Dict[str, Any],
    log=None,
    provider: Optional[str] = None,
) -> Dict[str, Any]:
    provider = (provider or "PLEX").upper()
    state = _load_state_dict(state_path)

    try:
        if provider == "PLEX":
            _delete_on_plex_single(key=key, state=state, cfg=cfg)
            _del_key_from_provider_items(state, "PLEX", key)
        elif provider == "SIMKL":
            item = _find_item_in_state_for_provider(state, key, "SIMKL")
            if not item:
                raise RuntimeError("SIMKL delete: item not found in SIMKL state")
            _delete_on_simkl_batch(
                [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
                (cfg.get("simkl") or {})
            )
            _del_key_from_provider_items(state, "SIMKL", key)
        elif provider == "TRAKT":
            item = _find_item_in_state_for_provider(state, key, "TRAKT")
            if not item:
                raise RuntimeError("TRAKT delete: item not found in TRAKT state")
            _delete_on_trakt_batch(
                [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
                (cfg.get("trakt") or {})
            )
            _del_key_from_provider_items(state, "TRAKT", key)
        else:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        # Server-side hide (UI gebruikt eigen localStorage; dit is optioneel)
        hide = _load_hide_set()
        if key not in hide:
            hide.add(key)
            _save_hide_set(hide)

        _save_state_dict(state_path, state)
        return {"ok": True, "deleted": key, "provider": provider}

    except Exception as e:
        if log:
            try:
                log("TRBL", f"[WATCHLIST] ERROR [{provider}]: {e}")
            except Exception:
                pass
        return {"ok": False, "error": str(e), "provider": provider}

# ======================================================================
# Public: batch delete
# ======================================================================

def delete_watchlist_items(
    keys: List[str],
    provider: str,
    state_path: Path,
    cfg: Dict[str, Any],
) -> Dict[str, Any]:
    state = _load_state_dict(state_path)
    provider = (provider or "").upper()
    if not provider:
        return {"ok": False, "error": "missing provider"}

    items: List[Dict[str, Any]] = []
    missing: List[str] = []

    try:
        if provider == "PLEX":
            for k in keys:
                it = _find_item_in_state(state, k)
                if it:
                    items.append({"key": k, "item": it, "type": _type_from_item_or_guess(it, k)})
                else:
                    missing.append(k)
            for it in items:
                _delete_on_plex_single(it["key"], state, cfg)
                _del_key_from_provider_items(state, "PLEX", it["key"])

        elif provider == "SIMKL":
            for k in keys:
                it = _find_item_in_state_for_provider(state, k, "SIMKL")
                if it:
                    items.append({"key": k, "item": it, "type": _type_from_item_or_guess(it, k)})
                else:
                    missing.append(k)
            _delete_on_simkl_batch(items, (cfg.get("simkl") or {}))
            for it in items:
                _del_key_from_provider_items(state, "SIMKL", it["key"])

        elif provider == "TRAKT":
            for k in keys:
                it = _find_item_in_state_for_provider(state, k, "TRAKT")
                if it:
                    items.append({"key": k, "item": it, "type": _type_from_item_or_guess(it, k)})
                else:
                    missing.append(k)
            _delete_on_trakt_batch(items, (cfg.get("trakt") or {}))
            for it in items:
                _del_key_from_provider_items(state, "TRAKT", it["key"])

        else:
            return {"ok": False, "error": f"unknown provider '{provider}'"}

        hide = _load_hide_set()
        hide |= {it["key"] for it in items}
        _save_hide_set(hide)
        _save_state_dict(state_path, state)

        return {"ok": True, "deleted": [it["key"] for it in items], "missing": missing, "provider": provider}

    except Exception as e:
        return {"ok": False, "error": str(e), "provider": provider}

# _watchlist.py
from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
import json
import requests

from plexapi.myplex import MyPlexAccount
from cw_platform.config_base import CONFIG

# Path for server-side "hidden items" overlay persisted on disk.
HIDE_PATH: Path = CONFIG / "watchlist_hide.json"

def _state_path() -> Path:
    return CONFIG / "watchlist_state.json"

# ======================================================================
# Local "hide" overlay helpers (server-side). The UI uses localStorage.
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
# Generic helpers (IDs, types, and state access)
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
# Plex GUID helpers (assist with PlexAPI matching)
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
# Build merged watchlist view (for the UI)
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
            "tmdb": int(str(tmdb_id)) if str(tmdb_id).isdigit() else tmdb_id,
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
# Provider-specific deletes (single and batch)
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

    # 1) Load the full Plex online watchlist
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
        # Match on GUID (primary or alternates)
        if any(_norm_guid(cg) in targets for cg in cand):
            return True
        # Fallback: match by ratingKey if Plex exposes it for watchlist
        m_rk = str(getattr(media, "ratingKey", "") or getattr(media, "id", "") or "").strip()
        return bool(rk and m_rk and (rk == m_rk))

    found = next((m for m in watchlist if matches(m)), None)
    if not found:
        raise RuntimeError("item not found in Plex online watchlist")

    # 2) Remove via the item method if available; otherwise via the account
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

    # 3) Verify removal by reloading the full list
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

    # Build compact payload once
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

    # Try watchlist first (most common); if nothing was removed, try history once (no re-balance)
    resp_wl = _post_simkl_delete(_SIMKL_WATCHLIST_REMOVE, hdr, payload)
    if _simkl_deleted_count(resp_wl) > 0:
        return

    resp_hist = _post_simkl_delete(_SIMKL_HISTORY_REMOVE, hdr, payload)
    if _simkl_deleted_count(resp_hist) > 0:
        return

    # If both reported 0, fail clearly
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
    
# === Batch facade used by the API ===
def delete_watchlist_batch(keys: list[str], provider: str, state: Dict[str, Any], cfg: Dict[str, Any]) -> Dict[str, Any]:
    """
    Delete multiple items from a single provider in one go.
    Returns a small result dict for logging/UI.
    """
    prov = (provider or "").upper().strip()
    keys = [k for k in (keys or []) if isinstance(k, str) and k.strip()]
    if not keys:
        return {"deleted": 0, "provider": prov, "note": "no-keys"}

    if prov == "SIMKL":
        items = [_find_item_in_state_for_provider(state, k, "SIMKL") for k in keys]
        _delete_on_simkl_batch(items, cfg.get("simkl", {}) or {})
    elif prov == "TRAKT":
        items = [_find_item_in_state_for_provider(state, k, "TRAKT") for k in keys]
        _delete_on_trakt_batch(items, cfg.get("trakt", {}) or {})
    elif prov == "PLEX":
        for k in keys:
            _delete_on_plex_single(k, state, cfg)
    else:
        raise RuntimeError(f"unknown provider: {prov}")

    # prune local state for this provider so the UI updates instantly
    changed = False
    for k in keys:
        changed |= _del_key_from_provider_items(state, prov, k)

    if changed:
        _save_state_dict(_state_path(), state)

    return {"deleted": len(keys), "provider": prov, "status": "ok"}


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
    """Delete a single watchlist item by key from one provider or ALL."""
    provider = (provider or "PLEX").upper()
    state = _load_state_dict(state_path)

    def _log(level: str, msg: str):
        if log:
            try:
                log(level, msg)
            except Exception:
                pass

    def _present_any_prov() -> bool:
        return any(_get_provider_items(state, p).get(key) for p in ("PLEX", "SIMKL", "TRAKT"))

    # SIMKL delete without relying on a pre-populated SIMKL provider-state
    def _delete_simkl_any():
        item = _find_item_in_state(state, key) or {}
        _delete_on_simkl_batch(
            [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
            (cfg.get("simkl") or {}),
        )
        _del_key_from_provider_items(state, "SIMKL", key)

    # TRAKT delete without relying on a pre-populated TRAKT provider-state
    def _delete_trakt_any():
        item = _find_item_in_state(state, key) or {}
        _delete_on_trakt_batch(
            [{"key": key, "item": item, "type": _type_from_item_or_guess(item, key)}],
            (cfg.get("trakt") or {}),
        )
        _del_key_from_provider_items(state, "TRAKT", key)

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

        elif provider == "ALL":
            details = {}

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


# ======================================================================
# Public: batch delete
# ======================================================================

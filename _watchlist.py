# _watchlist.py
# Implements watchlist logic (PlexAPI-only, hide-overlay)

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
import json

from plexapi.myplex import MyPlexAccount

from cw_platform.config_base import CONFIG
HIDE_PATH  = CONFIG / "watchlist_hide.json"

# -------- Helper functions --------
def _load_hide_set() -> Set[str]:
    # Load the hide-overlay set from disk.
    try:
        if HIDE_PATH.exists():
            data = json.loads(HIDE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return set(str(x) for x in data)
    except Exception as e:
        print(f"Error loading hide set: {e}")
    return set()

def _save_hide_set(hide: Set[str]) -> None:
    # Persist the hide-overlay set to disk.
    try:
        HIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        HIDE_PATH.write_text(json.dumps(sorted(hide)), encoding="utf-8")
    except Exception as e:
        print(f"Error saving hide set: {e}")

def _pick_added(d: Dict[str, Any]) -> Optional[str]:
    # Return a plausible 'added at' timestamp from various input object formats.
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
    # Convert an ISO-8601-like timestamp to epoch seconds (best effort).
    if not iso:
        return 0
    try:
        s = str(iso).strip().replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return 0


# -------- GUID normalization --------
def _norm_guid(g: str) -> Tuple[str, str]:
        """
        Normalize a GUID to (provider, identifier), for example:
            "com.plexapp.agents.imdb://tt123?lang=en" -> ("imdb", "tt123")
            "imdb://tt123"                            -> ("imdb", "tt123")
            "thetvdb://123"                           -> ("tvdb", "123")
        Returns ("", "") for unknown or invalid input.
        """
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
    """
    Build plausible GUID variants for matching PlexAPI watchlist items.
    Example: imdb:tt123 → ["imdb://tt123", "com.plexapp.agents.imdb://tt123", "com.plexapp.agents.imdb://tt123?lang=en"]
    """
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
    """
    Extract GUID and ratingKey from a state item, if present.
    Only the GUID is used for PlexAPI matching; ratingKey is ignored.
    """
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


# -------- Public: build watchlist (grid) --------

def _get_items(state: Dict[str, Any], prov: str) -> Dict[str, Any]:
    P = (state.get("providers") or {}).get(prov.upper(), {}) or {}
    wl = (((P.get("watchlist") or {}).get("baseline") or {}).get("items") or {})
    if wl:
        return wl
    # legacy fallback if someone still writes old structure
    return (P.get("items") or {})

def _iso_to_epoch(s: str | None) -> int:
    from datetime import datetime
    if not s:
        return 0
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return 0

def _pick_added(rec: Dict[str, Any]) -> str | None:
    # Prefer explicit added_when; fallback to created_at or updated_at if present.
    return rec.get("added_when") or rec.get("created_at") or rec.get("updated_at")

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
            2: "both",   # keep old UI styles working
            3: "both",
        }[len(sources) if len(sources) in (1, 2, 3) else 1]

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
        })

    out.sort(key=lambda x: (x.get("added_epoch") or 0, x.get("year") or 0), reverse=True)
    return out


# -------- Public: delete one item (PlexAPI only) --------
def delete_watchlist_item(key: str, state_path: Path, cfg: Dict[str, Any], log=None) -> Dict[str, Any]:
    """
    Remove the item from the user's online Plex watchlist using PlexAPI.
    On success, add the key to the local hide-overlay to maintain UI consistency across refreshes.
    This updates the local hidden items set, but does not modify state.json.
    The next synchronization will reconcile state.
    """
    try:
        token = ((cfg.get("plex", {}) or {}).get("account_token") or "").strip()
        if not token:
            return {"ok": False, "error": "missing plex token"}

        # Build GUID candidates for matching
        state = {}
        try:
            if state_path and state_path.exists():
                state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            state = {}

        def _g(st, prov):
            P = (st.get("providers") or {}).get(prov, {}) or {}
            return (((P.get("watchlist") or {}).get("baseline") or {}).get("items") or {})

        plex_items  = _g(state, "PLEX")
        simkl_items = _g(state, "SIMKL")
        item = plex_items.get(key) or simkl_items.get(key) or {}

        guid, _ = _extract_plex_identifiers(item)
        variants = _guid_variants_from_key_or_item(key, item)
        if guid:
            variants = list(dict.fromkeys(variants + [guid]))  # Remove duplicates

        targets = {_norm_guid(v) for v in variants if v}
        if not targets:
            return {"ok": False, "error": "cannot derive a valid GUID for this key"}

        # Match against Plex online watchlist
        account = MyPlexAccount(token=token)
        watchlist = account.watchlist()

        found = None
        for media in watchlist:
            cand_guids = set()
            primary = (getattr(media, "guid", "") or "").split("?", 1)[0]
            if primary:
                cand_guids.add(primary)
            try:
                for gg in getattr(media, "guids", []) or []:
                    gid = str(getattr(gg, "id", gg) or "")
                    if gid:
                        cand_guids.add(gid.split("?", 1)[0])
            except Exception:
                pass

            if any(_norm_guid(cg) in targets for cg in cand_guids):
                found = media
                break

        if not found:
            return {"ok": False, "error": "item not found in Plex online watchlist"}

        # Delete on Plex (will raise on failure)
        account.removeFromWatchlist([found])

        # Only on success: add to hide-overlay (local hidden set)
        hide = _load_hide_set()  # Load current hidden keys
        if key not in hide:
            hide.add(key)  # Mark this key as deleted (hidden)
            _save_hide_set(hide)  # Save the updated hidden keys list

        if log:
            log("WATCHLIST", f"[WATCHLIST] deleted {key} via PlexAPI")

        return {"ok": True, "deleted": key}

    except Exception as e:
        if log:
            log("TRBL", f"[WATCHLIST] ERROR: {e}")
        return {"ok": False, "error": str(e)}

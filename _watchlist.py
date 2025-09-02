# _watchlist.py
# Watchlist logic for Plex ⇄ SIMKL Web UI (PlexAPI-only, hide-overlay)

from __future__ import annotations
from typing import Any, Dict, List, Optional, Tuple, Set
from datetime import datetime
from pathlib import Path
import json

# Requires: pip install PlexAPI
from plexapi.myplex import MyPlexAccount


# -------- Paths (Docker-aware) --------
ROOT = Path(__file__).resolve().parent
CONFIG_BASE = Path("/config") if str(ROOT).startswith("/app") else ROOT
HIDE_PATH = CONFIG_BASE / "watchlist_hide.json"  # overlay: keys to hide in UI


# -------- Small helpers --------
def _load_hide_set() -> Set[str]:
    """Load the hide-overlay set from disk."""
    try:
        if HIDE_PATH.exists():
            data = json.loads(HIDE_PATH.read_text(encoding="utf-8"))
            if isinstance(data, list):
                return set(str(x) for x in data)
    except Exception as e:
        print(f"Error loading hide set: {e}")
    return set()

def _save_hide_set(hide: Set[str]) -> None:
    """Persist the hide-overlay set."""
    try:
        HIDE_PATH.parent.mkdir(parents=True, exist_ok=True)
        HIDE_PATH.write_text(json.dumps(sorted(hide)), encoding="utf-8")
    except Exception as e:
        print(f"Error saving hide set: {e}")

def _pick_added(d: Dict[str, Any]) -> Optional[str]:
    """Return a plausible 'added at' timestamp from various shapes of input objects."""
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
    """Convert an ISO-8601-like timestamp to epoch seconds (best-effort)."""
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
    Normalize a GUID to (provider, ident), e.g.:
      "com.plexapp.agents.imdb://tt123?lang=en" -> ("imdb", "tt123")
      "imdb://tt123"                             -> ("imdb", "tt123")
      "thetvdb://123"                            -> ("tvdb", "123")
    Unknown/invalid -> ("", "")
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
    Build plausible GUID variants for matching against PlexAPI watchlist items.
    Example (imdb:tt123) → ["imdb://tt123", "com.plexapp.agents.imdb://tt123", "com.plexapp.agents.imdb://tt123?lang=en"]
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
    Extract GUID/ratingKey from a state item (if present).
    Only the GUID is used for matching with PlexAPI; ratingKey is ignored here.
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
def build_watchlist(state: Dict[str, Any], tmdb_api_key_present: bool) -> List[Dict[str, Any]]:
    """
    Build a merged watchlist view from state.json, newest-first.
    Filters out items present in the local hide-overlay.
    """
    plex_items = (state.get("plex", {}) or {}).get("items", {}) or {}
    simkl_items = (state.get("simkl", {}) or {}).get("items", {}) or {}
    hidden = _load_hide_set()

    out: List[Dict[str, Any]] = []
    all_keys = set(plex_items.keys()) | set(simkl_items.keys())

    for key in all_keys:
        if key in hidden:
            continue

        p = plex_items.get(key) or {}
        s = simkl_items.get(key) or {}
        info = p or s
        if not info:
            continue

        typ_raw = (info.get("type") or "").lower()
        typ = "tv" if typ_raw in ("tv", "show") else "movie"
        title = info.get("title") or info.get("name") or ""
        year = info.get("year") or info.get("release_year")
        tmdb_id = (info.get("ids", {}) or {}).get("tmdb") or info.get("tmdb")

        p_when = _pick_added(p)
        s_when = _pick_added(s)
        p_ep = _iso_to_epoch(p_when)
        s_ep = _iso_to_epoch(s_when)

        if p_ep >= s_ep:
            added_when = p_when
            added_epoch = p_ep
            added_src = "plex" if p else ("simkl" if s else "")
        else:
            added_when = s_when
            added_epoch = s_ep
            added_src = "simkl" if s else ("plex" if p else "")

        status = (
            "both"
            if key in plex_items and key in simkl_items
            else ("plex_only" if key in plex_items else "simkl_only")
        )

        out.append(
            {
                "key": key,
                "type": typ,
                "title": title,
                "year": year,
                "tmdb": tmdb_id,
                "status": status,
                "added_epoch": added_epoch,
                "added_when": added_when,
                "added_src": added_src,
                "categories": [],
            }
        )

    out.sort(key=lambda x: (x.get("added_epoch") or 0, x.get("year") or 0), reverse=True)
    return out

# -------- Public: delete one item (PlexAPI only) --------
def delete_watchlist_item(key: str, state_path: Path, cfg: Dict[str, Any], log=None) -> Dict[str, Any]:
    """
    Remove the item from the user's *online* Plex watchlist using PlexAPI.
    On success, add the key to the local hide-overlay (so the UI stays consistent across refreshes).
    This will update the local hidden items set, but state.json is not modified.
    The next sync will reconcile state.
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

        plex_items = (state.get("plex", {}) or {}).get("items", {}) or {}
        simkl_items = (state.get("simkl", {}) or {}).get("items", {}) or {}
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

#!/usr/bin/env python3

"""
Plex ⇄ SIMKL Watchlist Sync

Keep your Plex Watchlist and SIMKL “Plan to Watch” list in sync.

Sync modes
----------
- Two-way (default):
  • First run: safe seeding (adds only, no deletions).
  • Subsequent runs: full delta (adds + deletions) in both directions.
- Mirror:
  Make one side exactly match the other (choose Plex or SIMKL as the source of truth).

SIMKL OAuth setup
-----------------
1. Put your SIMKL client_id and client_secret into config.json.
2. Run the helper:
     ./plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787
3. Add the shown callback URL to your SIMKL app Redirect URIs.
4. Open the printed authorization URL and complete login.
   Tokens will be stored in config.json.

Requirements
------------
- Python 3.10+
- Packages: requests, plexapi (4.17.1+)
- config.json and state.json stored next to the script (or in /config when containerized).

Disclaimer
----------
This is a community project, not affiliated with Plex or SIMKL.
Use at your own risk. Keep backups of your lists.

GitHub: https://github.com/cenodude/Plex-SIMKL-Watchlist-Sync
"""

import argparse
import json
import re
import time
import requests
import sys
import urllib.parse
import webbrowser
import secrets
import datetime, builtins
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Sequence, Tuple, List, Dict, Set, Optional, NoReturn, cast

__VERSION__ = "0.3.7"

# --- timestamped & colored print ---
ANSI_DIM    = "\033[90m"  # grey
ANSI_BLUE   = "\033[94m"  # blue
ANSI_YELLOW = "\033[33m"  # yellow/orange
ANSI_RESET  = "\033[0m"   # reset (local to logger)

def log_print(*args, **kwargs):
    ts = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    prefix = f"{ANSI_DIM}[{ts}]{ANSI_RESET}"
    new_args = []
    for a in args:
        if isinstance(a, str):
            a = (a.replace("[i]",     f"{ANSI_BLUE}[i]{ANSI_RESET}")
                   .replace("[debug]", f"{ANSI_YELLOW}[debug]{ANSI_RESET}")
                   .replace("[✓]",    f"\033[92m[✓]{ANSI_RESET}")
                   .replace("[!]",    f"\033[91m[!]{ANSI_RESET}"))
        new_args.append(a)
    builtins.print(prefix, *new_args, flush=True, **kwargs)

print = log_print
# -----------------------------------

# Paths / constants
HERE = Path(__file__).resolve().parent
CONFIG_PATH = HERE / "config.json"
STATE_PATH  = HERE / "state.json"

UA = f"Plex-SIMKL-Watchlist-Sync/{__VERSION__}"

ANSI_G = "\033[92m"
ANSI_R = "\033[91m"
ANSI_X = "\033[0m"

DISCOVER_HOST = "https://discover.provider.plex.tv"
PLEX_WATCHLIST_PATH = "/library/sections/watchlist/all"
PLEX_METADATA_PATH = "/library/metadata"

# Default config
DEFAULT_CONFIG = {
    "plex": {
        "account_token": ""
    },
    "simkl": {
        "client_id": "",
        "client_secret": "",
        "access_token": "",
        "refresh_token": "",
        "token_expires_at": 0
    },
    "sync": {
        "enable_add": True,
        "enable_remove": True,
        "bidirectional": {
            "enabled": True,
            "mode": "two-way",          # "two-way" or "mirror"
            "source_of_truth": "plex"   # used only when mode="mirror": "simkl" or "plex"
        },
        "activity": {
            "use_activity": True,
            "types": ["watchlist"]
        }
    },
    "runtime": {
        "debug": False
    }
}

# --------------------------- Configuration -----------------------------------
def _read_text(p: Path) -> str:
    with open(p, "r", encoding="utf-8") as f:
        return f.read()

def _write_text(p: Path, s: str) -> None:
    with open(p, "w", encoding="utf-8") as f:
        f.write(s)

def load_config_file(path: Path) -> dict:
    if not path.exists():
        sys.exit(f"[!] Missing config.json at {path}.")
    try:
        cfg = json.loads(_read_text(path))
    except Exception as e:
        sys.exit(f"[!] Could not parse {path} as JSON: {e}")
    # ensure sections and defaults
    for k, v in DEFAULT_CONFIG.items():
        if k not in cfg:
            cfg[k] = v
        elif isinstance(v, dict):
            for kk, vv in v.items():
                cfg[k].setdefault(kk, vv)
    return cfg

def dump_config_file(path: Path, cfg: dict) -> None:
    _write_text(path, json.dumps(cfg, indent=2))

# --------------------------- State -------------------------------------------
def load_state(path: Path) -> Optional[dict]:
    if not path.exists():
        return None
    try:
        return json.loads(_read_text(path))
    except Exception:
        return None

def save_state(path: Path, data: dict) -> None:
    data = dict(data)
    data["version"] = 2
    data["last_sync_epoch"] = int(time.time())
    _write_text(path, json.dumps(data, indent=2))

def clear_state(path: Path) -> None:
    try:
        if path.exists():
            path.unlink()
    except Exception:
        pass

def print_banner() -> None:
    # Use builtins.print to avoid the timestamp wrapper
    builtins.print("")
    builtins.print(
        f"{ANSI_G}Plex{ANSI_X} ⇄ {ANSI_R}SIMKL{ANSI_X} Watchlist Sync "
        f"{ANSI_G}Version {__VERSION__}{ANSI_X}"
    )

# --------------------------- SIMKL API ---------------------------------------
SIMKL_BASE = "https://api.simkl.com"
SIMKL_OAUTH_TOKEN   = f"{SIMKL_BASE}/oauth/token"
SIMKL_ALL_ITEMS     = f"{SIMKL_BASE}/sync/all-items"
SIMKL_ADD_TO_LIST   = f"{SIMKL_BASE}/sync/add-to-list"
SIMKL_HISTORY_REMOVE= f"{SIMKL_BASE}/sync/history/remove"
SIMKL_ACTIVITIES    = f"{SIMKL_BASE}/sync/activities"
SIMKL_AUTH_URL      = "https://simkl.com/oauth/authorize"

def simkl_headers(simkl_cfg: dict) -> dict:
    return {
        "User-Agent": UA,
        "Accept": "application/json",
        "Content-Type": "application/json",
        "Authorization": f"Bearer {simkl_cfg.get('access_token','')}",
        "simkl-api-key": simkl_cfg.get("client_id",""),
    }

def token_expired(simkl_cfg: dict) -> bool:
    try:
        exp = float(simkl_cfg.get("token_expires_at", 0.0))
    except Exception:
        exp = 0.0
    return time.time() >= (exp - 60)

def simkl_refresh(cfg: dict, cfg_path: Path, debug: bool=False) -> dict:
    s = cfg.get("simkl") or {}
    if not s.get("refresh_token"):
        if debug:
            print("[debug] No refresh_token; using current access_token as-is.")
        return cfg
    payload = {
        "grant_type": "refresh_token",
        "refresh_token": s["refresh_token"],
        "client_id": s.get("client_id",""),
        "client_secret": s.get("client_secret",""),
    }
    r = requests.post(SIMKL_OAUTH_TOKEN, json=payload, headers={"User-Agent": UA}, timeout=30)
    if not r.ok:
        raise SystemExit(f"[!] SIMKL refresh failed: HTTP {r.status_code} {r.text}")
    tok = r.json()
    s["access_token"] = tok["access_token"]
    s["refresh_token"] = tok.get("refresh_token", s.get("refresh_token",""))
    s["token_expires_at"] = time.time() + int(tok.get("expires_in", 3600))
    cfg["simkl"] = s
    dump_config_file(cfg_path, cfg)
    if debug:
        print(f"[debug] SIMKL token refreshed and saved to {cfg_path}")
    return cfg

def _cb() -> str:
    return str(int(time.time() * 1000))

def _http_get_json(url: str, headers: dict, params: Optional[dict]=None, debug: bool=False):
    params = dict(params or {})
    params["_cb"] = _cb()
    if debug:
        qs = "&".join(f"{k}={v}" for k,v in params.items())
        print(f"[debug] SIMKL GET: {url}?{qs}")
    r = requests.get(url, headers=headers, params=params, timeout=45)
    if not r.ok:
        raise SystemExit(f"[!] SIMKL GET {url} failed: HTTP {r.status_code} {r.text}")
    try:
        return r.json()
    except Exception:
        return None

def simkl_get_ptw_full(simkl_cfg: dict, debug: bool=False) -> Tuple[List[dict], List[dict]]:
    """One-time full PTW pull (movies, shows). Returns (shows, movies)."""
    hdrs = simkl_headers(simkl_cfg)
    shows_js  = _http_get_json(f"{SIMKL_ALL_ITEMS}/shows/plantowatch", hdrs, debug=debug)
    movies_js = _http_get_json(f"{SIMKL_ALL_ITEMS}/movies/plantowatch", hdrs, debug=debug)
    shows_items  = (shows_js  or {}).get("shows",  [])
    movies_items = (movies_js or {}).get("movies", [])
    return shows_items, movies_items

def ids_from_simkl_item(it: dict) -> dict:
    """Extract ids/title/year from a SIMKL 'movie'/'show' wrapper or 'ids' node."""
    node = None
    for k in ("movie", "show", "anime", "ids"):
        if isinstance(it.get(k), dict):
            node = it[k]
            break
    if node is None:
        return {}
    ids_block = node.get("ids", node) if ("ids" in node or node is it) else node
    ids = {}
    for k in ("simkl", "imdb", "tmdb", "tvdb", "slug"):
        v = ids_block.get(k)
        if v is not None:
            ids[k] = v
    if "title" in node:
        ids["title"] = node.get("title")
    if "year" in node:
        try:
            ids["year"] = int(node.get("year"))
        except Exception:
            ids["year"] = node.get("year")
    for k in ("tmdb", "tvdb", "year"):
        if k in ids and ids[k] is not None:
            try:
                ids[k] = int(ids[k])
            except Exception:
                pass
    return ids

def combine_ids(ids: dict) -> dict:
    """Keep only the identifiers SIMKL accepts in payloads (+title/year for Plex resolving)."""
    out = {}
    for k in ("imdb", "tmdb", "tvdb", "slug", "title", "year"):
        if k in ids and ids[k] is not None:
            out[k] = ids[k]
    return out

def canonical_identity(ids: dict) -> Optional[Tuple[str, str]]:
    for k in ("imdb", "tmdb", "tvdb", "slug"):
        v = ids.get(k)
        if v is not None:
            return (k, str(v))
    return None

def identity_key(pair: Tuple[str, str]) -> str:
    return f"{pair[0]}:{pair[1]}"

# --------------------------- Activities / Deltas ------------------------------
def simkl_get_activities(simkl_cfg: dict, debug: bool=False) -> dict:
    """
    Normalize SIMKL /sync/activities into a stable shape.
    Handles 'all' as string or object, and 'tv_shows' vs 'shows'.
    """
    hdrs = simkl_headers(simkl_cfg)
    js = _http_get_json(SIMKL_ACTIVITIES, hdrs, debug=debug) or {}

    # top-level "all" may be a string or {"all": "..."}
    all_raw = js.get("all")
    all_val = all_raw.get("all") if isinstance(all_raw, dict) else all_raw

    def _pick_section(j, *names):
        for n in names:
            sec = j.get(n)
            if isinstance(sec, dict):
                return sec
        return {}

    movies_sec = _pick_section(js, "movies")
    shows_sec  = _pick_section(js, "tv_shows", "shows")
    anime_sec  = _pick_section(js, "anime")

    def _norm(sec: dict) -> dict:
        if not isinstance(sec, dict):
            return {"all": None, "rated_at": None, "plantowatch": None,
                    "completed": None, "dropped": None, "watching": None}
        return {
            "all": sec.get("all"),
            "rated_at": sec.get("rated_at"),
            "plantowatch": sec.get("plantowatch"),
            "completed": sec.get("completed"),
            "dropped": sec.get("dropped"),
            "watching": sec.get("watching"),
        }

    return {
        "all": all_val,
        "movies": _norm(movies_sec),
        "tv_shows": _norm(shows_sec),
        "anime": _norm(anime_sec),
    }

def needs_fetch(curr_ts: Optional[str], prev_ts: Optional[str]) -> bool:
    """True if curr_ts is newer than prev_ts. Missing values -> no fetch."""
    if not curr_ts:
        return False
    return iso_to_epoch(curr_ts) > iso_to_epoch(prev_ts)

def iso_to_epoch(s: Optional[str]) -> int:
    if not s:
        return 0
    try:
        from datetime import datetime, timezone
        try:
            dt = datetime.fromisoformat(s.replace("Z","+00:00"))
        except Exception:
            from email.utils import parsedate_to_datetime
            dt = parsedate_to_datetime(s)
        return int(dt.replace(tzinfo=timezone.utc).timestamp())
    except Exception:
        return 0

def allitems_delta(simkl_cfg: dict, typ: str, status: str, since_iso: str, debug: bool=False) -> List[dict]:
    """
    Fetch delta for given type/status since since_iso.
    type: "movies" | "shows"
    status: "plantowatch" | "completed" | "dropped" | "watching"
    Returns raw list of items (each has nested movie/show + ids).
    """
    hdrs = simkl_headers(simkl_cfg)
    base = f"{SIMKL_ALL_ITEMS}/{'movies' if typ=='movies' else 'shows'}/{status}"
    params = {"date_from": since_iso}
    js = _http_get_json(base, hdrs, params=params, debug=debug) or {}
    key = "movies" if typ == "movies" else "shows"
    return js.get(key, []) or []

def build_index(rows_movies: List[dict], rows_shows: List[dict]) -> Dict[str, dict]:
    """Build a flat index keyed by canonical id (e.g., imdb:tt123)."""
    idx: Dict[str, dict] = {}
    for r in rows_movies:
        ids = combine_ids(r["ids"])
        pair = canonical_identity(ids)
        if not pair:
            continue
        idx[identity_key(pair)] = {
            "type": "movie",
            "ids": ids,
            "title": r.get("title"),
            "year": r.get("year"),
        }
    for r in rows_shows:
        ids = combine_ids(r["ids"])
        pair = canonical_identity(ids)
        if not pair:
            continue
        idx[identity_key(pair)] = {
            "type": "show",
            "ids": ids,
            "title": r.get("title"),
            "year": r.get("year"),
        }
    return idx

def build_index_from_simkl(simkl_movies: List[dict], simkl_shows: List[dict]) -> Dict[str, dict]:
    """Build index from SIMKL API items (movies list, shows list)."""
    idx: Dict[str, dict] = {}
    for m in simkl_movies:
        ids = combine_ids(ids_from_simkl_item(m))
        pair = canonical_identity(ids)
        if not pair:
            continue
        node = (m.get("movie") or m.get("show") or {})
        idx[identity_key(pair)] = {"type": "movie", "ids": ids, "title": node.get("title"), "year": ids.get("year")}
    for s in simkl_shows:
        ids = combine_ids(ids_from_simkl_item(s))
        pair = canonical_identity(ids)
        if not pair:
            continue
        node = (s.get("show") or s.get("movie") or {})
        idx[identity_key(pair)] = {"type": "show", "ids": ids, "title": node.get("title"), "year": ids.get("year")}
    return idx

def apply_simkl_deltas(prev_idx: Dict[str, dict],
                       simkl_cfg: dict,
                       prev_acts: Optional[dict],
                       curr_acts: dict,
                       debug: bool=False) -> Dict[str, dict]:
    """
    Update previous PTW index using SIMKL /sync/activities.
    - No previous state: full PTW fetch.
    - plantowatch changed: full refresh for that type (covers pure deletes + additions).
    - completed/dropped/watching changed: remove those from the PTW index.
    """
    idx = dict(prev_idx or {})

    # Initial seed
    if not prev_acts or not prev_idx:
        if debug:
            print("[debug] No previous state; doing full PTW fetch.")
        shows_list, movies_list = simkl_get_ptw_full(simkl_cfg, debug=debug)  # (shows, movies)
        idx = build_index_from_simkl(movies_list, shows_list)
        return idx

    # Full refresh helper
    def _refresh_type(typ: str) -> None:
        hdrs = simkl_headers(simkl_cfg)
        path_type = "movies" if typ == "movies" else "shows"
        full_js = _http_get_json(f"{SIMKL_ALL_ITEMS}/{path_type}/plantowatch", hdrs, debug=debug) or {}
        full_list = full_js.get("movies" if typ == "movies" else "shows", []) or []

        fresh: Dict[str, dict] = {}
        for it in full_list:
            ids2 = combine_ids(ids_from_simkl_item(it))
            pair2 = canonical_identity(ids2)
            if not pair2:
                continue
            key = identity_key(pair2)
            node = (it.get("movie") or it.get("show") or {})
            fresh[key] = {
                "type": "movie" if typ == "movies" else "show",
                "ids": ids2,
                "title": node.get("title"),
                "year": ids2.get("year")
            }

        # Replace existing items of this type
        to_delete = [k for k, v in idx.items() if v.get("type") == ("movie" if typ == "movies" else "show")]
        for k in to_delete:
            idx.pop(k, None)
        idx.update(fresh)

        if debug:
            print(f"[debug] SIMKL {typ}.plantowatch full refresh: {len(fresh)} items (replaced {len(to_delete)})")

    # Process sections
    for typ, section in (("movies", "movies"), ("shows", "tv_shows")):
        prev = (prev_acts.get(section) or {})
        curr = (curr_acts.get(section) or {})

        if needs_fetch(curr.get("plantowatch"), prev.get("plantowatch")):
            _refresh_type(typ)

        for st in ("completed", "dropped", "watching"):
            if needs_fetch(curr.get(st), prev.get(st)):
                since = prev.get(st) or "1970-01-01T00:00:00Z"
                rows = allitems_delta(simkl_cfg, typ=("movies" if typ == "movies" else "shows"),
                                      status=st, since_iso=since, debug=debug)
                if debug:
                    print(f"[debug] SIMKL delta {typ}.{st} items: {len(rows)} (prune from PTW)")
                for it in rows:
                    ids = combine_ids(ids_from_simkl_item(it))
                    pair = canonical_identity(ids)
                    if pair:
                        idx.pop(identity_key(pair), None)

    return idx

# --------------------------- Plex (plexapi + read fallback) ------------------
try:
    import plexapi
    from plexapi.myplex import MyPlexAccount  # type: ignore
except Exception:
    py = Path(sys.executable)
    pip = py.with_name("pip")
    print(ANSI_R + "[!] plexapi is not installed in this Python environment." + ANSI_X)
    print("    Install it, then rerun:")
    print(f"      {pip} install -U plexapi")
    sys.exit(1)

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|tvdb)://(\d+)", re.I)

def _extract_ids_from_guid_strings(guid_values: List[str]) -> Tuple[Optional[str], Optional[int], Optional[int]]:
    imdb = tmdb = tvdb = None
    for s in guid_values or []:
        s = str(s)
        m = _PAT_IMDB.search(s)
        if m and not imdb:
            imdb = m.group(1)
        m = _PAT_TMDB.search(s)
        if m and not tmdb:
            try:
                tmdb = int(m.group(1))
            except Exception:
                pass
        m = _PAT_TVDB.search(s)
        if m and not tvdb:
            try:
                tvdb = int(m.group(1))
            except Exception:
                pass
    return imdb, tmdb, tvdb

def _plexapi_upgrade_hint(where: str, exc: Optional[Exception], debug: bool) -> NoReturn:
    v = getattr(plexapi, "__version__", "?")
    pip_bin = Path(sys.executable).with_name("pip")
    print("")
    print(ANSI_R + "[!] Plex API call failed" + ANSI_X)
    print(f"    While: {where} (plexapi {v})")
    print("    Upgrade plexapi in the SAME environment:")
    print(f"      {pip_bin} install -U plexapi")
    if debug and exc is not None:
        print(f"    [debug] Error: {repr(exc)}")
    sys.exit(1)

def plex_fetch_watchlist_items_via_plexapi(acct: MyPlexAccount, debug: bool=False) -> Optional[List[object]]:
    try:
        movies = acct.watchlist(libtype="movie")
        shows = acct.watchlist(libtype="show")
        items = (movies or []) + (shows or [])
        if debug:
            print(f"[debug] plexapi watchlist fetched: {len(items)} items")
        return items
    except Exception as e:
        if debug:
            print(f"[debug] plexapi watchlist fetch failed: {e}")
        return None

# Read fallback via Discover HTTP (read-only)
def _plex_headers(token: str) -> dict:
    return {
        "X-Plex-Token": token,
        "Accept": "application/json",
        "X-Plex-Product": "PlexWatchlistSync",
        "X-Plex-Version": __VERSION__,
        "X-Plex-Client-Identifier": "plex-simkl-bridge",
        "X-Plex-Device": "Python",
        "X-Plex-Device-Name": "plex-simkl-bridge",
        "X-Plex-Platform": "Python",
        "User-Agent": UA,
    }

def _discover_get(path: str, token: str, params: dict, timeout: int=20) -> Optional[dict]:
    url = f"{DISCOVER_HOST}{path}"
    try:
        r = requests.get(url, headers=_plex_headers(token), params=params, timeout=timeout)
        if r.ok:
            return r.json()
    except Exception:
        pass
    return None

def _discover_metadata_by_ratingkey(token: str, rating_key: str, debug: bool=False) -> Optional[dict]:
    params = {"includeExternalMedia": "1"}
    data = _discover_get(f"{PLEX_METADATA_PATH}/{rating_key}", token, params, timeout=12)
    if not data:
        return None
    md = (data.get("MediaContainer", {}).get("Metadata") or [])
    return md[0] if md else None

def plex_fetch_watchlist_items_via_discover(token: str, page_size: int=100, debug: bool=False) -> List[dict[str, Any]]:
    params_base = {"includeCollections": "1", "includeExternalMedia": "1"}
    start = 0
    items: List[dict[str, Any]] = []
    while True:
        params = dict(params_base)
        params["X-Plex-Container-Start"] = str(start)     # ensure string
        params["X-Plex-Container-Size"]  = str(page_size) # ensure string
        data = _discover_get(PLEX_WATCHLIST_PATH, token, params, timeout=20)
        if not data:
            if debug:
                print("[debug] discover watchlist: no data")
            break
        mc = data.get("MediaContainer", {}) or {}
        md = mc.get("Metadata", []) or []
        if debug:
            total = mc.get("totalSize")
            try:
                total = int(total) if total is not None else None
            except Exception:
                total = None
            print(f"[debug] discover page start={start} got={len(md)} total={total}")
        for it in md:
            title = it.get("title") or it.get("name")
            rating_key = str(it.get("ratingKey") or "") or ""
            mtype = it.get("type") or it.get("metadataType")
            mtype = "show" if (isinstance(mtype, str) and mtype.startswith("show")) or mtype == 2 else "movie"

            guid_values: List[str] = []
            if isinstance(it.get("guid"), str):
                guid_values.append(it["guid"])
            if isinstance(it.get("Guid"), list):
                for gg in it["Guid"]:
                    if isinstance(gg, dict) and "id" in gg:
                        guid_values.append(gg["id"])

            imdb, tmdb, tvdb = _extract_ids_from_guid_strings(guid_values)
            if not any([imdb, tmdb, tvdb]) and rating_key:
                enriched = _discover_metadata_by_ratingkey(token, rating_key, debug=debug)
                if enriched:
                    e_guids: List[str] = []
                    if isinstance(enriched.get("Guid"), list):
                        for gg in enriched["Guid"]:
                            if isinstance(gg, dict) and "id" in gg:
                                e_guids.append(gg["id"])
                    if isinstance(enriched.get("guid"), str):
                        e_guids.append(enriched["guid"])
                    imdb, tmdb, tvdb = _extract_ids_from_guid_strings(e_guids)
                    if debug:
                        allg = list(dict.fromkeys(guid_values + e_guids))
                        print(f"[debug] Enriched '{title}' (rk={rating_key}) GUIDs: {allg}")

            ids: Dict[str, Any] = {}
            if imdb:
                ids["imdb"] = imdb
            if tmdb is not None:
                ids["tmdb"] = tmdb
            if tvdb is not None:
                ids["tvdb"] = tvdb

            items.append({"type": mtype, "title": title, "ids": ids})

        size = int(mc.get("size", len(md)))
        if not md or size < page_size:
            break
        start += size if size else page_size
    return items

# Mixed fetch
def plex_fetch_watchlist_items(
    acct: MyPlexAccount, plex_token: str, debug: bool=False
) -> Sequence[object | dict[str, Any]]:
    items = plex_fetch_watchlist_items_via_plexapi(acct, debug=debug)
    if items is not None:
        return items
    if debug:
        print("[debug] Falling back to Discover HTTP for watchlist read")
    return plex_fetch_watchlist_items_via_discover(plex_token, page_size=100, debug=debug)

def plex_item_to_ids(item: Any) -> Dict[str, Any]:
    """Extract imdb/tmdb/tvdb + title/year from a plexapi item or fallback dict row."""
    if isinstance(item, dict):
        ids = item.get("ids") or {}
        title = item.get("title")
        year = item.get("year")
        out: Dict[str, Any] = {"title": title, "year": year}
        out.update({k: v for k, v in ids.items() if v is not None})
        return {k: v for k, v in out.items() if v is not None}

    title = getattr(item, "title", None)
    year = getattr(item, "year", None)
    guid_values: List[str] = []
    try:
        for g in (getattr(item, "guids", []) or []):
            gid = getattr(g, "id", None)
            if isinstance(gid, str):
                guid_values.append(gid)
    except Exception:
        pass
    gsingle = getattr(item, "guid", None)
    if isinstance(gsingle, str):
        guid_values.append(gsingle)
    imdb, tmdb, tvdb = _extract_ids_from_guid_strings(guid_values)
    out: Dict[str, Any] = {"title": title, "year": year, "imdb": imdb, "tmdb": tmdb, "tvdb": tvdb}
    return {k: v for k, v in out.items() if v}

def item_libtype(item: Any) -> str:
    if isinstance(item, dict):
        return "show" if item.get("type") == "show" else "movie"
    t = getattr(item, "type", "movie")
    return "show" if t == "show" else "movie"

def resolve_discover_item(acct: MyPlexAccount, ids: dict, libtype: str, debug: bool = False) -> Optional[Any]:
    """Resolve a Plex Discover metadata item by imdb/tmdb/tvdb or title+year."""
    queries: List[str] = []
    if ids.get("imdb"):
        queries.append(ids["imdb"])
    if ids.get("tmdb"):
        queries.append(str(ids["tmdb"]))
    if ids.get("tvdb"):
        queries.append(str(ids["tvdb"]))
    if ids.get("title"):
        queries.append(ids["title"])

    # Optionally de-duplicate while preserving order
    queries = list(dict.fromkeys(queries))

    for q in queries:
        hits: Sequence[Any] = []  # ensure it's always defined for type checker
        try:
            hits = acct.searchDiscover(q, libtype=libtype) or []
        except Exception as e:
            _plexapi_upgrade_hint("MyPlexAccount.searchDiscover(libtype=...)", e, debug)
            hits = []  # unreachable (above exits), but keeps Pylance happy

        for md in hits:
            md_ids = plex_item_to_ids(md)
            if ids.get("imdb") and md_ids.get("imdb") == ids.get("imdb"):
                return md
            if ids.get("tmdb") and md_ids.get("tmdb") == ids.get("tmdb"):
                return md
            if ids.get("tvdb") and md_ids.get("tvdb") == ids.get("tvdb"):
                return md
            if ids.get("title") and ids.get("year"):
                try:
                    same_title = str(md_ids.get("title", "")).strip().lower() == str(ids["title"]).strip().lower()
                    same_year = int(md_ids.get("year", 0)) == int(ids["year"])
                    if same_title and same_year:
                        return md
                except Exception:
                    pass
    return None

def plex_add_by_ids(acct: MyPlexAccount, ids: dict, libtype: str, debug: bool=False) -> bool:
    it = resolve_discover_item(acct, ids, libtype, debug=debug)
    if not it:
        if debug:
            print(f"[debug] plexapi add: could not resolve {ids}")
        return False
    try:
        cast(Any, it).addToWatchlist(account=acct)  # satisfy type checker
        if debug:
            print(f"[debug] plexapi add OK: {getattr(it, 'title', ids)}")
        return True
    except Exception as e:
        if debug:
            print(f"[debug] plexapi add failed: {e}")
        msg = str(e).lower()
        if ("already on the watchlist" in msg or "already on watchlist" in msg or "409" in msg):
            if debug:
                print("[debug] treat as success: item already present on Plex")
            return True
        return False

def plex_remove_by_ids(acct: MyPlexAccount, ids: dict, libtype: str, debug: bool=False) -> bool:
    it = resolve_discover_item(acct, ids, libtype, debug=debug)
    if not it:
        if debug:
            print(f"[debug] plexapi remove: could not resolve {ids}")
        return False
    try:
        cast(Any, it).removeFromWatchlist(account=acct)  # satisfy type checker
        if debug:
            print(f"[debug] plexapi remove OK: {getattr(it, 'title', ids)}")
        return True
    except Exception as e:
        if debug:
            print(f"[debug] plexapi remove failed: {e}")
        msg = str(e).lower()
        if "not on the watchlist" in msg or "404" in msg or "not found" in msg:
            if debug:
                print("[debug] treat as success: item already absent on Plex")
            return True
        return False

# --------------------------- Sync helpers ------------------------------------
def _current_counts(acct, plex_token: str, simkl_cfg: dict, debug: bool=False) -> Tuple[int,int]:
    plex_after = plex_fetch_watchlist_items(acct, plex_token, debug=debug)
    try:
        shows, movies = simkl_get_ptw_full(simkl_cfg, debug=debug)
        simkl_n = len(shows) + len(movies)
    except Exception:
        # Fallback to 0 if SIMKL fetch fails temporarily
        simkl_n = 0
    return len(plex_after), simkl_n

def wait_for_eventual_consistency(acct, plex_token: str, simkl_cfg: dict,
                                  tries: int = 3, delay: float = 2.0, debug: bool=False) -> Tuple[bool,int,int]:
    """
    Poll a few times to let SIMKL/Plex catch up after writes.
    Returns (equal, plex_count, simkl_count).
    """
    last_p = last_s = 0
    for i in range(tries):
        p, s = _current_counts(acct, plex_token, simkl_cfg, debug=debug)
        last_p, last_s = p, s
        if p == s:
            return True, p, s
        if debug:
            print(f"[debug] counts not equal yet (plex={p} simkl={s}); retry {i+1}/{tries} in {delay}s...")
        time.sleep(delay)
    return False, last_p, last_s

def neutral_precheck_msg(plex_total: int, simkl_total: int) -> None:
    if plex_total == simkl_total:
        print(f"[i] Pre-sync counts: Plex={plex_total} vs SIMKL={simkl_total} (equal)")
    else:
        print(f"[i] Pre-sync counts: Plex={plex_total} vs SIMKL={simkl_total} (differences)")

def colored_postcheck(plex_total: int, simkl_total: int) -> None:
    ok = plex_total == simkl_total
    msg = "EQUAL" if ok else "NOT EQUAL"
    color = ANSI_G if ok else ANSI_R
    print(f"[i] Post-sync: Plex={plex_total} vs SIMKL={simkl_total} → {color}{msg}{ANSI_X}")

def gather_plex_rows(items: Sequence[object | dict[str, Any]]) -> List[dict[str, Any]]:
    """Normalize Plex items into rows with type/title/year/ids."""
    rows: List[dict[str, Any]] = []
    for it in items:
        libtype = item_libtype(it)
        ids_full = plex_item_to_ids(it)
        ids = {k: v for k, v in ids_full.items() if k in ("imdb", "tmdb", "tvdb", "slug") and v}
        rows.append({"type": libtype, "title": ids_full.get("title"), "year": ids_full.get("year"), "ids": ids})
    return rows

def snapshot_for_state(plex_idx: Dict[str, dict], simkl_idx: Dict[str, dict], last_activities: dict) -> dict:
    return {"plex": {"items": plex_idx}, "simkl": {"items": simkl_idx, "last_activities": last_activities}}

# --------------------------- CLI / Main --------------------------------------
def build_parser(include_examples: bool = False) -> argparse.ArgumentParser:
    epilog_examples = """Examples

  Initialize SIMKL tokens (headless):
    ./plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787

  Initialize SIMKL tokens and open a browser:
    ./plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787 --open

  Run a sync (two-way by default):
    ./plex_simkl_watchlist_sync.py --sync

  Run with debug logging:
    ./plex_simkl_watchlist_sync.py --sync --debug
"""
    epilog = epilog_examples if include_examples else None

    ap = argparse.ArgumentParser(
        prog="plex_simkl_watchlist_sync.py",
        description="Sync Plex Watchlist with SIMKL.",
        formatter_class=argparse.RawTextHelpFormatter,
        epilog=epilog,
    )
    ap.add_argument("--sync", action="store_true", help="Run synchronization")
    ap.add_argument("--init-simkl", choices=["redirect"], help="Initialize SIMKL tokens via local redirect helper")
    ap.add_argument("--bind", default="0.0.0.0:8787", help="Bind host:port for redirect helper (default 0.0.0.0:8787)")
    ap.add_argument("--open", action="store_true", help="With --init-simkl redirect, also open the auth URL")
    ap.add_argument("--plex-account-token", help="Override Plex token for this run")
    ap.add_argument("--debug", action="store_true", help="Enable verbose logging")
    ap.add_argument("--version", action="store_true", help="Print version info and exit")
    ap.add_argument("--reset-state", action="store_true", help="Delete state.json (next run will re-seed)")
    return ap

# ----- OAuth helper  ----------------------
def detect_local_ip(fallback: str="localhost") -> str:
    import socket
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
        s.close()
        if ip and not ip.startswith("127."):
            return ip
    except Exception:
        pass
    return fallback

def build_simkl_authorize_url(client_id: str, redirect_uri: str, state: str = "", scope: Optional[str] = None) -> str:
    params = {"response_type": "code", "client_id": client_id, "redirect_uri": redirect_uri}
    if state:
        params["state"] = state
    if scope:
        params["scope"] = scope
    return f"{SIMKL_AUTH_URL}?{urllib.parse.urlencode(params)}"

def simkl_exchange_code_for_tokens(code: str, redirect_uri: str, simkl_cfg: dict, cfg_path: Path, debug: bool=False):
    payload = {
        "grant_type": "authorization_code",
        "code": code.strip(),
        "redirect_uri": redirect_uri,
        "client_id": simkl_cfg.get("client_id", ""),
        "client_secret": simkl_cfg.get("client_secret", ""),
    }
    r = requests.post(SIMKL_OAUTH_TOKEN, json=payload, headers={"User-Agent": UA}, timeout=30)
    if not r.ok:
        raise SystemExit(f"[!] SIMKL token exchange failed: HTTP {r.status_code} {r.text}")
    tok = r.json()
    simkl_cfg["access_token"] = tok["access_token"]
    simkl_cfg["refresh_token"] = tok.get("refresh_token", simkl_cfg.get("refresh_token", ""))
    simkl_cfg["token_expires_at"] = time.time() + int(tok.get("expires_in", 3600))
    full_cfg = load_config_file(cfg_path)
    full_cfg["simkl"] = simkl_cfg
    dump_config_file(cfg_path, full_cfg)
    if debug:
        print("[debug] SIMKL token exchange success; tokens saved to", cfg_path)
    return tok

class _RedirectHandler(BaseHTTPRequestHandler):
    def _html(self, body: str, status: int=200) -> None:
        self.send_response(status)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.end_headers()
        self.wfile.write(body.encode("utf-8"))

    def log_message(self, fmt, *args) -> None:  # type: ignore[override]
        if getattr(self.server, "debug", False):
            super().log_message(fmt, *args)

    def do_GET(self) -> None:  # type: ignore[override]
        parsed = urllib.parse.urlparse(self.path)
        if parsed.path != "/callback":
            return self._html("<h3>Not Found</h3>", 404)
        qs = urllib.parse.parse_qs(parsed.query or "")
        code = (qs.get("code") or [""])[0].strip()
        state = (qs.get("state") or [""])[0].strip()
        if not code:
            return self._html("<h3>Missing ?code</h3>", 400)
        if self.server.expected_state and state and state != self.server.expected_state:  # type: ignore[attr-defined]
            return self._html("<h3>State mismatch</h3>", 400)
        try:
            simkl_exchange_code_for_tokens(
                code, self.server.redirect_uri, self.server.simkl_cfg, self.server.cfg_path, debug=self.server.debug  # type: ignore[attr-defined]
            )
            return self._html("<h3>Success!</h3><p>Tokens saved. You can close this tab.</p>")
        except SystemExit as e:
            return self._html(f"<h3>Exchange failed</h3><pre>{e}</pre>", 500)
        except Exception as e:
            return self._html(f"<h3>Unexpected error</h3><pre>{e}</pre>", 500)

def simkl_oauth_redirect(cfg_path: Path, bind_host: str="0.0.0.0", bind_port: int=8787, open_browser: bool=False, debug: bool=False) -> None:
    import os, socket

    cfg = load_config_file(cfg_path)
    s = cfg.get("simkl") or {}
    if not s.get("client_id") or not s.get("client_secret"):
        raise SystemExit("[!] Please set simkl.client_id and simkl.client_secret in config.json first.")

    # shown_host = friendly IP/host for printing (non-0.0.0.0 if we can detect)
    shown_host = detect_local_ip() if bind_host in ("0.0.0.0", "::") else bind_host
    redirect_uri = f"http://{shown_host}:{bind_port}/callback"

    state = secrets.token_urlsafe(16)
    auth_url = build_simkl_authorize_url(s["client_id"], redirect_uri, state=state)

    # HTTP server that receives the SIMKL redirect
    srv = HTTPServer((bind_host, bind_port), _RedirectHandler)
    srv.simkl_cfg = s            # type: ignore[attr-defined]
    srv.cfg_path = cfg_path      # type: ignore[attr-defined]
    srv.redirect_uri = redirect_uri  # type: ignore[attr-defined]
    srv.expected_state = state   # type: ignore[attr-defined]
    srv.debug = debug            # type: ignore[attr-defined]

    print("[i] Redirect helper is running")
    print(f"    Bind: {bind_host}:{bind_port}")

    # Detect if we’re inside a container (best-effort)
    running_in_container = os.path.exists("/.dockerenv")

    if running_in_container:
        # Container-internal callback URL + host hint
        try:
            container_ip = socket.gethostbyname(socket.gethostname())
        except Exception:
            container_ip = shown_host
        print("[i] Callback URL (container internal):")
        print(f"    http://{container_ip}:{bind_port}/callback")
        print(f"[i] If you published the port with -p {bind_port}:{bind_port}, open on your machine:")
        print(f"    http://<docker-host-ip>:{bind_port}/callback")
        print("[i] Add the exact URL you will use above to your SIMKL app Redirect URIs.")
    else:
        # Bare metal / no container
        print(f"[i] Callback URL:")
        print(f"    http://{shown_host}:{bind_port}/callback")
        print("[i] Add this exact redirect URL in your SIMKL app settings.")

    print("[i] Open this SIMKL authorization URL:")
    print(f"    {auth_url}")

    if open_browser:
        try:
            webbrowser.open(auth_url)
        except Exception:
            pass

    print("[i] Waiting for SIMKL to redirect back with ?code=...")
    try:
        srv.handle_request()  # one request, then stop
        print("[✓] Code handled; tokens saved if exchange succeeded.")
    except KeyboardInterrupt:
        print("\n[!] Redirect helper stopped.")

# --------------------------- Main --------------------------------------------
def main() -> None:
    # If user requested help, show help WITH examples
    if any(h in sys.argv[1:] for h in ("-h", "--help")):
        ap = build_parser(include_examples=True)
        ap.print_help()
        sys.exit(0)

    # Normal parser (no examples in help)
    ap = build_parser(include_examples=False)

    # No args → show minimal help (no examples)
    if not sys.argv[1:]:
        ap.print_help()
        return

    args = ap.parse_args()

    if args.version:
        print(f"Plex_SIMKL_Watchlist_Sync version: {__VERSION__}")
        print(f"plexapi version: {getattr(plexapi, '__version__', '?')}")
        return

    if args.reset_state:
        clear_state(STATE_PATH)
        print("[✓] Cleared state.json (next --sync will re-seed).")
        return

    if args.init_simkl:
        host, port = "0.0.0.0", 8787
        if ":" in args.bind:
            host, p = args.bind.rsplit(":", 1)
            try:
                port = int(p)
            except Exception:
                pass
        simkl_oauth_redirect(CONFIG_PATH, bind_host=host, bind_port=port, open_browser=bool(args.open), debug=True)
        return

    if not args.sync:
        ap.print_help()
        return

    print_banner()

    cfg = load_config_file(CONFIG_PATH)
    plex_cfg = cfg.get("plex") or {}
    simkl_cfg = cfg.get("simkl") or {}
    sync_cfg  = cfg.get("sync") or {}
    run_cfg   = cfg.get("runtime") or {}
    bidi_cfg  = (sync_cfg.get("bidirectional") or {})
    act_cfg   = (sync_cfg.get("activity") or {})

    debug = bool(args.debug or run_cfg.get("debug", False))

    plex_token = args.plex_account_token or plex_cfg.get("account_token", "")
    if not plex_token:
        print("[!] Missing Plex token. Set 'plex.account_token' in config.json")
        return

    if not simkl_cfg.get("client_id") or not simkl_cfg.get("client_secret"):
        print("[!] Missing SIMKL client credentials in config.json")
        print("    Then run: ./plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787")
        return

    if not simkl_cfg.get("access_token"):
        if simkl_cfg.get("refresh_token"):
            cfg = simkl_refresh(cfg, CONFIG_PATH, debug=debug)
            simkl_cfg = cfg.get("simkl") or {}
        if not simkl_cfg.get("access_token"):
            print("[!] No SIMKL access_token. Initialize tokens first.")
            print("    Then run: ./plex_simkl_watchlist_sync.py --init-simkl redirect --bind 0.0.0.0:8787")
            return

    if token_expired(simkl_cfg):
        cfg = simkl_refresh(cfg, CONFIG_PATH, debug=debug)
        simkl_cfg = cfg.get("simkl") or {}

    # Plex account
    try:
        acct = MyPlexAccount(token=plex_token)
    except Exception as e:
        print(ANSI_R + "[!] Could not authenticate to Plex with provided token." + ANSI_X)
        print(f"    {e}")
        return

    # Load prev state
    prev_state = load_state(STATE_PATH) or {}
    prev_plex_idx  = ((prev_state.get("plex") or {}).get("items") or {})
    prev_simkl_idx = ((prev_state.get("simkl") or {}).get("items") or {})
    prev_acts      = ((prev_state.get("simkl") or {}).get("last_activities") or {})

    first_run = (not prev_state) or (not prev_simkl_idx) or (not prev_acts)

    # 1) Pull Plex
    plex_items = plex_fetch_watchlist_items(acct, plex_token, debug=debug)
    print(f"[i] Plex items: {len(plex_items)}")
    plex_rows = gather_plex_rows(plex_items)
    plex_movies_rows = [r for r in plex_rows if r["type"] == "movie"]
    plex_shows_rows  = [r for r in plex_rows if r["type"] == "show"]
    plex_idx = build_index(plex_movies_rows, plex_shows_rows)

    # 2) SIMKL activity-first
    simkl_idx = dict(prev_simkl_idx)
    curr_acts: dict = {}
    if bool(act_cfg.get("use_activity", True)):
        curr_acts = simkl_get_activities(simkl_cfg, debug=debug)
        simkl_idx = apply_simkl_deltas(prev_simkl_idx, simkl_cfg, prev_acts, curr_acts, debug=debug)
    else:
        # Fallback: full PTW each time (not ideal)
        shows, movies = simkl_get_ptw_full(simkl_cfg, debug=debug)
        simkl_idx = build_index_from_simkl(movies, shows)

    plex_total = len(plex_idx)
    simkl_total = len(simkl_idx)
    neutral_precheck_msg(plex_total, simkl_total)

    # 3) Plan differences for two-way / mirror
    def keyset(idx: Dict[str, dict], typ: str) -> Set[str]:
        return {k for k, v in idx.items() if v.get("type") == typ}

    plex_movies_keys = keyset(plex_idx, "movie")
    plex_shows_keys  = keyset(plex_idx, "show")
    simkl_movies_keys= keyset(simkl_idx, "movie")
    simkl_shows_keys = keyset(simkl_idx, "show")

    plex_only_movies_keys  = plex_movies_keys - simkl_movies_keys
    plex_only_shows_keys   = plex_shows_keys  - simkl_shows_keys
    simkl_only_movies_keys = simkl_movies_keys- plex_movies_keys
    simkl_only_shows_keys  = simkl_shows_keys - plex_shows_keys

    enable_add    = bool(sync_cfg.get("enable_add", True))
    enable_remove = bool(sync_cfg.get("enable_remove", True))
    bidi_enabled  = bool(bidi_cfg.get("enabled", True))
    mode          = str(bidi_cfg.get("mode", "two-way")).lower()
    source_of_truth = str(bidi_cfg.get("source_of_truth", "plex")).lower()

    if debug:
        print(f"[debug] plan: PLEX-only movies={len(plex_only_movies_keys)} shows={len(plex_only_shows_keys)}")
        print(f"[debug] plan: SIMKL-only movies={len(simkl_only_movies_keys)} shows={len(simkl_only_shows_keys)}")
        print(f"[debug] mode={mode} source={source_of_truth} add={enable_add} remove={enable_remove}")

    any_failure = False
    added_simkl = removed_simkl = added_plex = removed_plex = 0
    hdrs_simkl = simkl_headers(simkl_cfg)

    def ids_by_key(idx: Dict[str, dict], k: str) -> dict:
        return (idx.get(k) or {}).get("ids") or {}

    # ---- two-way logic with deltas on SIMKL side ----
    if bidi_enabled and mode == "two-way":
        if first_run:
            # First run: safe seeding (adds only both ways)
            if enable_add:
                payload: Dict[str, List[dict]] = {}
                if plex_only_movies_keys:
                    payload["movies"] = [{"to": "plantowatch", "ids": combine_ids(ids_by_key(plex_idx, k))} for k in plex_only_movies_keys]
                if plex_only_shows_keys:
                    payload["shows"] = [{"to": "plantowatch", "ids": combine_ids(ids_by_key(plex_idx, k))} for k in plex_only_shows_keys]
                if payload:
                    if debug:
                        print(f"[debug] SIMKL add payload (seed): {json.dumps(payload, indent=2)}")
                    r = requests.post(SIMKL_ADD_TO_LIST, headers=hdrs_simkl, json=payload, timeout=45)
                    if not r.ok:
                        print(ANSI_R + f"[!] SIMKL add-to-list failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                        any_failure = True
                    else:
                        added_simkl += sum(len(v) for v in payload.values())
                        if added_simkl:
                            print(f"[✓] Added Plex→SIMKL items: {added_simkl}")
                if enable_add:
                    for k in simkl_only_movies_keys:
                        if plex_add_by_ids(acct, ids_by_key(simkl_idx, k), "movie", debug=debug):
                            added_plex += 1
                        else:
                            any_failure = True
                    for k in simkl_only_shows_keys:
                        if plex_add_by_ids(acct, ids_by_key(simkl_idx, k), "show", debug=debug):
                            added_plex += 1
                        else:
                            any_failure = True
                    if added_plex:
                        print(f"[✓] Added SIMKL→Plex items: {added_plex}")
        else:
            # True deltas vs previous snapshot
            old_plex_keys  = set(prev_plex_idx.keys())
            new_plex_keys  = set(plex_idx.keys())
            old_simkl_keys = set(prev_simkl_idx.keys())
            new_simkl_keys = set(simkl_idx.keys())

            plex_added_keys   = new_plex_keys  - old_plex_keys
            plex_removed_keys = old_plex_keys  - new_plex_keys
            simkl_added_keys  = new_simkl_keys - old_simkl_keys
            simkl_removed_keys= old_simkl_keys - new_simkl_keys

            if debug:
                print(f"[debug] deltas: plex +{len(plex_added_keys)} / -{len(plex_removed_keys)} | simkl +{len(simkl_added_keys)} / -{len(simkl_removed_keys)}")

            # Plex → SIMKL (adds)
            if enable_add and plex_added_keys:
                payload: Dict[str, List[dict]] = {"movies": [], "shows": []}
                for k in plex_added_keys:
                    rec = plex_idx.get(k)
                    if not rec:
                        continue
                    to = "movies" if rec["type"] == "movie" else "shows"
                    payload[to].append({"to": "plantowatch", "ids": combine_ids(rec["ids"])})
                payload = {k: v for k, v in payload.items() if v}
                if payload:
                    if debug:
                        print(f"[debug] SIMKL add payload (plex→simkl): {json.dumps(payload, indent=2)}")
                    r = requests.post(SIMKL_ADD_TO_LIST, headers=hdrs_simkl, json=payload, timeout=45)
                    if not r.ok:
                        print(ANSI_R + f"[!] SIMKL add-to-list failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                        any_failure = True
                    else:
                        added_simkl += sum(len(v) for v in payload.values())

            # Plex → SIMKL (removes)
            if enable_remove and plex_removed_keys:
                payload: Dict[str, List[dict]] = {"movies": [], "shows": []}
                for k in plex_removed_keys:
                    rec = (prev_plex_idx.get(k) or {})
                    if not rec:
                        continue
                    to = "movies" if rec.get("type") == "movie" else "shows"
                    payload[to].append({"ids": combine_ids(rec.get("ids") or {})})
                payload = {k: v for k, v in payload.items() if v}
                if payload:
                    if debug:
                        print(f"[debug] SIMKL remove payload (plex→simkl): {json.dumps(payload, indent=2)}")
                    r = requests.post(SIMKL_HISTORY_REMOVE, headers=hdrs_simkl, json=payload, timeout=45)
                    if not r.ok:
                        print(ANSI_R + f"[!] SIMKL history/remove failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                        any_failure = True
                    else:
                        removed_simkl += sum(len(v) for v in payload.values())

            # SIMKL → Plex (adds)
            if enable_add and simkl_added_keys:
                for k in simkl_added_keys:
                    rec = simkl_idx.get(k)
                    if not rec:
                        continue
                    if plex_add_by_ids(acct, rec["ids"], rec["type"], debug=debug):
                        added_plex += 1
                    else:
                        any_failure = True

            # SIMKL → Plex (removes)
            if enable_remove and simkl_removed_keys:
                for k in simkl_removed_keys:
                    rec = (prev_simkl_idx.get(k) or {})
                    if not rec:
                        continue
                    if plex_remove_by_ids(acct, rec.get("ids") or {}, rec.get("type") or "movie", debug=debug):
                        removed_plex += 1
                    else:
                        any_failure = True

    elif bidi_enabled and mode == "mirror":
        if source_of_truth == "plex":
            # Make SIMKL match Plex
            simkl_add_payload: Dict[str, List[dict]] = {}
            if enable_add and plex_only_movies_keys:
                simkl_add_payload["movies"] = [{"to": "plantowatch", "ids": combine_ids(ids_by_key(plex_idx, k))} for k in plex_only_movies_keys]
            if enable_add and plex_only_shows_keys:
                simkl_add_payload["shows"]  = [{"to": "plantowatch", "ids": combine_ids(ids_by_key(plex_idx, k))} for k in plex_only_shows_keys]
            if simkl_add_payload:
                if debug: print(f"[debug] SIMKL add payload (mirror/plex): {json.dumps(simkl_add_payload, indent=2)}")
                r = requests.post(SIMKL_ADD_TO_LIST, headers=hdrs_simkl, json=simkl_add_payload, timeout=45)
                if not r.ok:
                    print(ANSI_R + f"[!] SIMKL add-to-list failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                    any_failure = True
                else:
                    added = len(simkl_add_payload.get("movies", [])) + len(simkl_add_payload.get("shows", []))
                    if added:
                        print(f"[✓] MIRROR(plex): added {added} to SIMKL")

            rm_payload: Dict[str, List[dict]] = {}
            if enable_remove and simkl_only_movies_keys:
                rm_payload["movies"] = [{"ids": combine_ids(ids_by_key(simkl_idx, k))} for k in simkl_only_movies_keys]
            if enable_remove and simkl_only_shows_keys:
                rm_payload["shows"]  = [{"ids": combine_ids(ids_by_key(simkl_idx, k))} for k in simkl_only_shows_keys]
            if rm_payload:
                if debug: print(f"[debug] SIMKL remove payload (mirror/plex): {json.dumps(rm_payload, indent=2)}")
                r = requests.post(SIMKL_HISTORY_REMOVE, headers=hdrs_simkl, json=rm_payload, timeout=45)
                if not r.ok:
                    print(ANSI_R + f"[!] SIMKL history/remove failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                    any_failure = True
                else:
                    removed = len(rm_payload.get("movies", [])) + len(rm_payload.get("shows", []))
                    if removed:
                        print(f"[✓] MIRROR(plex): removed {removed} from SIMKL")
        else:
            # Make Plex match SIMKL
            added = removed = 0
            if enable_add:
                for k in simkl_only_movies_keys:
                    if plex_add_by_ids(acct, ids_by_key(simkl_idx, k), "movie", debug=debug): added += 1
                    else: any_failure = True
                for k in simkl_only_shows_keys:
                    if plex_add_by_ids(acct, ids_by_key(simkl_idx, k), "show", debug=debug): added += 1
                    else: any_failure = True
            if enable_remove:
                for k in plex_only_movies_keys:
                    if plex_remove_by_ids(acct, ids_by_key(plex_idx, k), "movie", debug=debug): removed += 1
                    else: any_failure = True
                for k in plex_only_shows_keys:
                    if plex_remove_by_ids(acct, ids_by_key(plex_idx, k), "show", debug=debug): removed += 1
                    else: any_failure = True
            if added or removed:
                print(f"[✓] MIRROR(simkl): +{added} / -{removed} on Plex")

    else:
        # One-way: Plex -> SIMKL
        payload: Dict[str, List[dict]] = {}
        if enable_add and (plex_only_movies_keys or plex_only_shows_keys):
            if plex_only_movies_keys:
                payload["movies"] = [{"to": "plantowatch", "ids": combine_ids(ids_by_key(plex_idx, k))} for k in plex_only_movies_keys]
            if plex_only_shows_keys:
                payload["shows"]  = [{"to": "plantowatch", "ids": combine_ids(ids_by_key(plex_idx, k))} for k in plex_only_shows_keys]
            if debug: print(f"[debug] SIMKL add payload (one-way): {json.dumps(payload, indent=2)}")
            r = requests.post(SIMKL_ADD_TO_LIST, headers=hdrs_simkl, json=payload, timeout=45)
            if not r.ok:
                print(ANSI_R + f"[!] SIMKL add-to-list failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                any_failure = True

        if enable_remove and (simkl_only_movies_keys or simkl_only_shows_keys):
            rm_payload: Dict[str, List[dict]] = {}
            if simkl_only_movies_keys:
                rm_payload["movies"] = [{"ids": combine_ids(ids_by_key(simkl_idx, k))} for k in simkl_only_movies_keys]
            if simkl_only_shows_keys:
                rm_payload["shows"]  = [{"ids": combine_ids(ids_by_key(simkl_idx, k))} for k in simkl_only_shows_keys]
            if debug: print(f"[debug] SIMKL remove payload (one-way): {json.dumps(rm_payload, indent=2)}")
            r = requests.post(SIMKL_HISTORY_REMOVE, headers=hdrs_simkl, json=rm_payload, timeout=45)
            if not r.ok:
                print(ANSI_R + f"[!] SIMKL history/remove failed: HTTP {r.status_code} {r.text}" + ANSI_X)
                any_failure = True

    # Post-check with short eventual-consistency window
    equal_now, p_after, s_after = wait_for_eventual_consistency(
        acct, plex_token, simkl_cfg, tries=3, delay=2.0, debug=debug
    )
    colored_postcheck(p_after, s_after)

    # Save snapshot only if all actions succeeded AND counts match (after wait)
    if any_failure:
        print(ANSI_R + "[!] Some actions failed; NOT saving state." + ANSI_X)
    elif equal_now:
        save_state(STATE_PATH, snapshot_for_state(plex_idx, simkl_idx, curr_acts or prev_acts or {}))
        if debug:
            print("[debug] State updated.")
    else:
        print("[i] Counts still differ after a short wait; likely eventual consistency. "
            "Not saving state; will re-check next run.")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n[!] Aborted")

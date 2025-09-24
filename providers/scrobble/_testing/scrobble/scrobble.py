from __future__ import annotations
# Scrobble core: config, events, dispatcher, helpers
# 24-09-2025 Back-to-Basics Editions...and pray i guess..

import json, os, time, re
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional, Protocol, List

# --- config ---------------------------------------------------------------

def _find_config_path() -> Path:
    p = os.environ.get("CROSSWATCH_CONFIG")
    if p and Path(p).is_file():
        return Path(p)
    if Path("./config.json").is_file():
        return Path("./config.json")
    return Path("/config/config.json")

def load_config() -> Dict[str, Any]:
    try:
        with _find_config_path().open("r", encoding="utf-8") as f:
            cfg = json.load(f)
    except Exception:
        cfg = {}

    # Defaults
    cfg.setdefault("runtime", {}).setdefault("debug", False)
    cfg.setdefault("app", {}).setdefault("version", os.getenv("APP_VERSION"))
    cfg.setdefault("app", {}).setdefault("date", os.getenv("APP_DATE"))

    # Canonical provider tree
    providers = cfg.setdefault("providers", {})
    px = providers.setdefault("plex", {})
    tk = providers.setdefault("trakt", {})

    # --- Legacy Plex → canonical
    legacy_plex = cfg.get("plex", {}) or {}
    px.setdefault("url", (os.getenv("PLEX_URL") or legacy_plex.get("server_url", "")).rstrip("/"))
    px.setdefault("token", os.getenv("PLEX_TOKEN") or legacy_plex.get("account_token", ""))
    px.setdefault("verify", os.getenv("PLEX_VERIFY", "1") not in ("0","false","False","no","NO"))
    px.setdefault("user_id", os.getenv("PLEX_USER_ID"))
    px.setdefault("username", legacy_plex.get("username"))

    # --- Legacy Trakt → canonical
    legacy_trakt = cfg.get("trakt", {}) or {}
    tk.setdefault("client_id",
        os.getenv("TRAKT_CLIENT_ID", "") or legacy_trakt.get("client_id", "")
    )
    tk.setdefault("token",
        os.getenv("TRAKT_TOKEN", "") or
        legacy_trakt.get("access_token", "") or
        legacy_trakt.get("token", "")
    )
    # Optional extras (unused by sink but keep for completeness)
    if "refresh_token" in legacy_trakt:
        tk.setdefault("refresh_token", legacy_trakt.get("refresh_token"))
    if "token_type" in legacy_trakt:
        tk.setdefault("token_type", legacy_trakt.get("token_type"))
    if "expires_at" in legacy_trakt:
        tk.setdefault("expires_at", legacy_trakt.get("expires_at"))

    # --- Scrobble settings
    sc = cfg.setdefault("scrobble", {})
    sc.setdefault("enabled", True)
    sc.setdefault("mode", "watch")
    sc.setdefault("poll_secs", int(os.getenv("POLL_SECS", "5")))
    sc.setdefault("heartbeat_secs", 30)
    sc.setdefault("pause_debounce_secs", 4)
    sc.setdefault("stop_demote_threshold", None)
    sc.setdefault("autostart", True)
    sc.setdefault("filters", {})

    # Back-compat: scrobble.watch.*
    w = sc.get("watch") or {}
    if isinstance(w.get("autostart"), bool):
        sc["autostart"] = w["autostart"]
    wf = w.get("filters") or {}
    if isinstance(wf.get("username_whitelist"), list):
        sc["filters"].setdefault("plex_usernames", wf["username_whitelist"])
    if "server_uuid" in wf:
        v = wf["server_uuid"]
        if isinstance(v, list):
            sc["filters"].setdefault("server_uuids", v)
        elif isinstance(v, str) and v.strip():
            sc["filters"].setdefault("server_uuids", [v])

    # Keep older filters too
    sc["filters"].setdefault("plex_user_ids", [])
    sc["filters"].setdefault("servers", [])

    return cfg


CFG: Dict[str, Any] = load_config()

def cfg(path: str, default: Any = None) -> Any:
    cur: Any = CFG
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return default
        cur = cur[part]
    return cur

# --- logging --------------------------------------------------------------

def log(msg: str, level: str = "INFO") -> None:
    ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    print(f"{ts} | {level:<5} | {msg}")

def debug(msg: str) -> None:
    if cfg("runtime.debug", False):
        log(msg, "DEBUG")

# --- event + sink ---------------------------------------------------------

@dataclass
class ScrobbleEvent:
    action: str                    # "start" | "pause" | "stop"
    kind: str                      # "movie" | "episode"
    progress: float                # 0..100
    payload: Dict[str, Any]        # Trakt 'movie' or {'show','episode'}
    session_id: str
    plex_user_id: Optional[str] = None
    plex_username: Optional[str] = None
    plex_server: Optional[str] = None
    plex_server_uuid: Optional[str] = None

class ScrobbleSink(Protocol):
    def send(self, ev: ScrobbleEvent) -> None: ...

class Dispatcher:
    """Tiny filter + simple pause-debounce. Nothing clever."""
    def __init__(self, sinks: List[ScrobbleSink]):
        self._sinks = sinks
        self._last_start: Dict[str, float] = {}

    def _passes(self, ev: ScrobbleEvent) -> bool:
        f = cfg("scrobble.filters", {}) or {}
        # Legacy
        id_whitelist = set(f.get("plex_user_ids", []))
        server_titles = set(f.get("servers", []))
        # New
        usernames = set(f.get("plex_usernames", []))
        server_uuids = set(f.get("server_uuids", []))

        if id_whitelist and ev.plex_user_id and ev.plex_user_id not in id_whitelist:
            debug(f"drop by user-id filter: {ev.plex_user_id}"); return False
        if usernames and ev.plex_username and ev.plex_username not in usernames:
            debug(f"drop by username filter: {ev.plex_username}"); return False
        if server_uuids and ev.plex_server_uuid and ev.plex_server_uuid not in server_uuids:
            debug(f"drop by server-uuid filter: {ev.plex_server_uuid}"); return False
        if server_titles and ev.plex_server and ev.plex_server not in server_titles:
            debug(f"drop by server title filter: {ev.plex_server}"); return False
        return True

    def dispatch(self, ev: ScrobbleEvent) -> None:
        if not self._passes(ev):
            return
        if ev.action == "start":
            self._last_start[ev.session_id] = time.time()
        if ev.action == "pause":
            last = self._last_start.get(ev.session_id, 0)
            if time.time() - last < float(cfg("scrobble.pause_debounce_secs", 4)):
                debug("debounce pause"); return
        for s in self._sinks:
            s.send(ev)

# --- GUID → Trakt helpers -------------------------------------------------

_GUIDS = [
    (re.compile(r"^imdb://(tt\d+)", re.I), ("imdb", lambda m: m.group(1))),
    (re.compile(r"^tmdb://(\d+)", re.I), ("tmdb", lambda m: int(m.group(1)))),
    (re.compile(r"^tvdb://(\d+)(?:/(\d+))?(?:/(\d+))?", re.I),
        ("tvdb", lambda m: (int(m.group(1)), m.group(2), m.group(3)))),
    (re.compile(r"^com\.plexapp\.agents\.imdb://(tt\d+)", re.I), ("imdb", lambda m: m.group(1))),
    (re.compile(r"^com\.plexapp\.agents\.themoviedb://(\d+)", re.I), ("tmdb", lambda m: int(m.group(1)))),
    (re.compile(r"^com\.plexapp\.agents\.thetvdb://(\d+)(?:/(\d+))?(?:/(\d+))?", re.I),
        ("tvdb", lambda m: (int(m.group(1)), m.group(2), m.group(3)))),
]

def _ids_from_guid_str(g: str) -> Dict[str, Any]:
    """Return {'imdb':..., 'tmdb':..., 'tvdb':...} best-effort from one guid string."""
    g = (g or "").split("?")[0]
    for rx, (idt, fn) in _GUIDS:
        m = rx.match(g)
        if not m: continue
        val = fn(m)
        if idt == "imdb": return {"imdb": val}
        if idt == "tmdb": return {"tmdb": val}
        if idt == "tvdb":
            # tvdb path may include s/e; caller decides how to use
            sid = val[0] if isinstance(val, tuple) else int(val)
            out: Dict[str, Any] = {"tvdb": sid}
            if isinstance(val, tuple):
                s, e = val[1], val[2]
                if s and str(s).isdigit(): out["season"] = int(s)
                if e and str(e).isdigit(): out["number"] = int(e)
            return out
    return {}

def build_trakt_payload_from_plex(
    kind: str,
    guid: Optional[str],
    grandparent_guid: Optional[str] = None,
    season_hint: Optional[int] = None,
    number_hint: Optional[int] = None,
) -> Optional[Dict[str, Any]]:
    """Map a single Plex GUID pair → Trakt payload."""
    if not guid:
        return None
    g = guid.split("?")[0]

    if kind == "movie":
        ids = _ids_from_guid_str(g)
        return {"movie": {"ids": ids}} if ids else None

    # episode
    base = (grandparent_guid or guid).split("?")[0]
    show_ids = {k: v for k, v in _ids_from_guid_str(base).items() if k in ("tvdb","tmdb","imdb")}
    se: Dict[str, Any] = {}

    # S/E from item guid if tvdb path present
    ids_item = _ids_from_guid_str(g)
    if "season" in ids_item: se["season"] = ids_item["season"]
    if "number" in ids_item: se["number"] = ids_item["number"]

    # Hints win if provided
    if season_hint is not None: se["season"] = season_hint
    if number_hint is not None: se["number"] = number_hint

    if not show_ids:
        return None
    if "season" not in se or "number" not in se:
        # Trakt accepts start/pause/stop without S/E for some agents
        pass
    return {"show": {"ids": show_ids}, "episode": se}

# webhook compat (noop)
def from_plex_webhook(event_json: dict):
    debug("from_plex_webhook() called; ignored.")
    return []

__all__ = [
    "CFG", "cfg", "load_config",
    "log", "debug",
    "ScrobbleEvent", "ScrobbleSink", "Dispatcher",
    "build_trakt_payload_from_plex",
    "from_plex_webhook",
]

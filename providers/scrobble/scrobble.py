from __future__ import annotations

"""Scrobble core (compact)

- refactored: 21-09-2025
- Normalized ScrobbleEvent + sink protocol
- Plex PSN + flat parsers
- Dispatcher with username/server filters and pause debounce
"""

import json, re, time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Protocol, Set, Tuple

# Optional project logger
try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

# --- utils ---------------------------------------------------------------------

def _log(msg: str, lvl: str = "INFO") -> None:
    if BASE_LOG:
        try:
            BASE_LOG(str(msg), level=lvl, module="SCROBBLE"); return
        except Exception:
            pass
    print(f"{lvl} [SCROBBLE] {msg}")

def _load_config() -> Dict[str, Any]:
    for p in (Path("/app/config/config.json"), Path("./config.json"), Path("/mnt/data/config.json")):
        try:
            if p.exists(): return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _i(x: Any) -> Optional[int]:
    try: return int(x)
    except Exception: return None

# id patterns
_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)

def _grab(s: str, pat: re.Pattern) -> Optional[str]:
    m = pat.search(s or ""); return m.group(1) if m else None

def _ids_from_meta(meta: Dict[str, Any]) -> Dict[str, Any]:
    guid = str(meta.get("guid") or "")
    ids = {"imdb": _grab(guid,_PAT_IMDB), "tmdb": _grab(guid,_PAT_TMDB), "tvdb": _grab(guid,_PAT_TVDB)}
    gpg  = str(meta.get("grandparentGuid") or "")
    if gpg:
        ids["imdb_show"] = _grab(gpg,_PAT_IMDB)
        ids["tmdb_show"] = _grab(gpg,_PAT_TMDB)
        ids["tvdb_show"] = _grab(gpg,_PAT_TVDB)
    return {k:v for k,v in ids.items() if v}

def _progress(state: str, view_offset: int, duration: int) -> Tuple[int, "ScrobbleAction"]:
    d = max(1, _i(duration) or 0)
    vo = max(0, min(_i(view_offset) or 0, d))
    pct = max(0, min(100, int(round((vo / float(d)) * 100))))
    s = (state or "").lower()
    if   s == "playing": act: ScrobbleAction = "start"
    elif s == "paused":  act = "pause"
    elif s in ("stopped","bufferingstopped"): act = "stop"
    else: act = "start"
    return pct, act

def _event_from_meta(meta: Dict[str, Any], raw: Dict[str, Any]) -> "ScrobbleEvent":
    ids = _ids_from_meta(meta)
    pct, act = _progress(meta.get("state",""), meta.get("viewOffset",0) or 0, meta.get("duration",0) or 0)
    mtype: MediaType = "episode" if (meta.get("type") or "").lower() == "episode" else "movie"
    title  = meta.get("grandparentTitle") if mtype == "episode" else meta.get("title")
    season = meta.get("grandparentIndex") if mtype == "episode" else None
    number = meta.get("index") if mtype == "episode" else None
    return ScrobbleEvent(
        action=act, media_type=mtype, ids=ids, title=title, year=meta.get("year"),
        season=season, number=number, progress=pct,
        account=(meta.get("account") and str(meta["account"])) or None,
        server_uuid=(meta.get("machineIdentifier") and str(meta["machineIdentifier"])) or None,
        session_key=(meta.get("sessionKey") and str(meta["sessionKey"])) or None,
        raw=raw,
    )

# --- types ---------------------------------------------------------------------

ScrobbleAction = Literal["start", "pause", "stop"]
MediaType      = Literal["movie", "episode"]

@dataclass(frozen=True)
class ScrobbleEvent:
    action: ScrobbleAction
    media_type: MediaType
    ids: Dict[str, Any]
    title: Optional[str]
    year: Optional[int]
    season: Optional[int]
    number: Optional[int]
    progress: int
    account: Optional[str]
    server_uuid: Optional[str]
    session_key: Optional[str]
    raw: Dict[str, Any]

class ScrobbleSink(Protocol):
    def send(self, event: ScrobbleEvent) -> None: ...

# --- webhook (compat) ----------------------------------------------------------

def from_plex_webhook(payload: Any, defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    defaults = defaults or {}
    try:
        if isinstance(payload, dict) and "payload" in payload:
            obj = json.loads(payload["payload"])
        elif isinstance(payload, (str, bytes, bytearray)):
            obj = json.loads(payload if isinstance(payload,str) else payload.decode("utf-8"))
        elif isinstance(payload, dict):
            obj = payload
        else:
            return None
    except Exception:
        return None
    if isinstance(obj.get("PlaySessionStateNotification"), list):
        return from_plex_pssn(obj, defaults)
    return None

# --- parsers -------------------------------------------------------------------

def from_plex_pssn(payload: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    defaults = defaults or {}
    arr = payload.get("PlaySessionStateNotification")
    if not (isinstance(arr, list) and arr): return None
    n = dict(arr[0])
    meta = {
        "guid": n.get("guid"), "grandparentGuid": n.get("grandparentGuid"),
        "title": n.get("title"), "grandparentTitle": n.get("grandparentTitle"),
        "year": _i(n.get("year")), "index": _i(n.get("index")), "grandparentIndex": _i(n.get("grandparentIndex")),
        "duration": _i(n.get("duration") or 0) or 0, "viewOffset": _i(n.get("viewOffset") or 0) or 0,
        "type": n.get("type") or "", "state": n.get("state") or "",
        "sessionKey": n.get("sessionKey"), "account": n.get("account") or n.get("accountID"),
        "machineIdentifier": n.get("machineIdentifier") or defaults.get("server_uuid"),
    }
    return _event_from_meta(meta, payload)

def from_plex_flat_playing(payload: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    defaults = defaults or {}
    if int(payload.get("size") or 0) < 1: return None
    if (payload.get("_type") or payload.get("type") or "").lower() != "playing": return None
    first = next((v for v in payload.values() if isinstance(v, dict) and "guid" in v), None)
    if not first: return None
    meta = {
        "guid": first.get("guid"), "grandparentGuid": first.get("grandparentGuid"),
        "title": first.get("title"), "grandparentTitle": first.get("grandparentTitle"),
        "year": _i(first.get("year")), "index": _i(first.get("index")), "grandparentIndex": _i(first.get("grandparentIndex")),
        "duration": _i(first.get("duration") or 0) or 0, "viewOffset": _i(first.get("viewOffset") or 0) or 0,
        "type": first.get("type") or "", "state": first.get("state") or "",
        "sessionKey": first.get("sessionKey"), "account": first.get("account"),
        "machineIdentifier": first.get("machineIdentifier") or defaults.get("server_uuid"),
    }
    return _event_from_meta(meta, payload)

# --- dispatcher ----------------------------------------------------------------

class Dispatcher:
    """Filters + forwards events to sinks (username/server filters, pause debounce)."""

    def __init__(self, sinks: Iterable[ScrobbleSink], cfg_provider=None) -> None:
        self._sinks = list(sinks or [])
        self._cfg_provider = cfg_provider or _load_config
        self._session_ok: Set[str] = set()
        self._debounce: Dict[str, float] = {}
        self._last_action: Dict[str, str] = {}
        self._last_progress: Dict[str, int] = {}

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        if ev.session_key and str(ev.session_key) in self._session_ok: return True
        cfg  = self._cfg_provider() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl   = filt.get("username_whitelist")
        want_server = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))
        if want_server and ev.server_uuid and str(ev.server_uuid) != str(want_server): return False

        def _allow():
            if ev.session_key: self._session_ok.add(str(ev.session_key))
            return True

        if not wl: return _allow()

        def norm(s: str) -> str: return re.sub(r"[^a-z0-9]+","",(s or "").lower())
        wl_list = wl if isinstance(wl, list) else [wl]

        # Plain username match
        if any(not str(x).lower().startswith(("id:","uuid:")) and norm(str(x)) == norm(ev.account or "") for x in wl_list):
            return _allow()

        # Extract id/uuid from raw PSN if present
        def find_psn(o):
            if isinstance(o, dict):
                for k,v in o.items():
                    if isinstance(k,str) and k.lower()=="playsessionstatenotification":
                        return v if isinstance(v,list) else [v]
                for v in o.values():
                    r = find_psn(v)
                    if r: return r
            elif isinstance(o, list):
                for v in o:
                    r = find_psn(v)
                    if r: return r
            return None

        n = (find_psn(ev.raw or {}) or [None])[0] or {}
        acc_id   = str(n.get("accountID") or "")
        acc_uuid = str(n.get("accountUUID") or "").lower()

        for e in wl_list:
            s = str(e).strip().lower()
            if s.startswith("id:")   and acc_id   and s.split(":",1)[1].strip()==acc_id:   return _allow()
            if s.startswith("uuid:") and acc_uuid and s.split(":",1)[1].strip()==acc_uuid: return _allow()
        return False

    def _should_send(self, ev: ScrobbleEvent) -> bool:
        sk = ev.session_key or "?"
        last_a, last_p = self._last_action.get(sk), self._last_progress.get(sk, -1)
        changed = (ev.action != last_a) or (abs(ev.progress - last_p) >= 1)

        if ev.action == "pause":
            now, k = time.time(), f"{sk}|pause"
            if now - self._debounce.get(k, 0) < 5.0 and ev.action == last_a:
                return False
            self._debounce[k] = now

        if changed:
            self._last_action[sk] = ev.action
            self._last_progress[sk] = ev.progress
            return True
        return False

    def dispatch(self, ev: ScrobbleEvent) -> None:
        if not self._passes_filters(ev): return
        if not self._should_send(ev):    return
        for s in self._sinks:
            try: s.send(ev)
            except Exception as e: _log(f"Sink error: {e}", "ERROR")

# --- exports -------------------------------------------------------------------

__all__ = ["ScrobbleEvent","ScrobbleSink","Dispatcher","from_plex_webhook","from_plex_pssn","from_plex_flat_playing"]

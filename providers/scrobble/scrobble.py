from __future__ import annotations

import json
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Iterable, List, Literal, Optional, Protocol, Set, Tuple

try:
    from modules._logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


def _log(msg: str, lvl: str = "INFO") -> None:
    if BASE_LOG:
        BASE_LOG("SCROBBLE", lvl, msg)
    else:
        print(f"{lvl} [SCROBBLE] {msg}")


def _load_config() -> Dict[str, Any]:
    """
    Load configuration from common locations. If crosswatch.load_config is
    available elsewhere in your project and you prefer that, feel free to
    swap this loader for a direct import.
    """
    for p in (
        Path("/app/config/config.json"),
        Path("./config.json"),
        Path("/mnt/data/config.json"),
    ):
        try:
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


# ---- types & patterns ----------------------------------------------------------

ScrobbleAction = Literal["start", "pause", "stop"]
MediaType = Literal["movie", "episode"]

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)


# ---- domain model --------------------------------------------------------------

@dataclass(frozen=True)
class ScrobbleEvent:
    """
    Normalized playback event forwarded to sinks (e.g., Trakt).
    - progress: integer 0..100
    - raw: original payload for diagnostics / extra filter context
    """
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


# ---- webhook (compat) ----------------------------------------------------------

def from_plex_webhook(payload: Any, defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    """
    Back-compat parser for Plex webhooks.

    Accepts:
      - dict with key "payload" (JSON string)
      - raw JSON string/bytes
      - already-parsed dict

    If a PlaySessionStateNotification list is present, defer to PSN parsing.
    Otherwise we cannot reliably normalize a full event here.
    """
    defaults = defaults or {}
    try:
        if isinstance(payload, dict) and "payload" in payload:
            obj = json.loads(payload["payload"])
        elif isinstance(payload, (str, bytes, bytearray)):
            obj = json.loads(payload if isinstance(payload, str) else payload.decode("utf-8"))
        elif isinstance(payload, dict):
            obj = payload
        else:
            return None
    except Exception:
        return None

    psn = obj.get("PlaySessionStateNotification")
    if isinstance(psn, list) and psn:
        return from_plex_pssn(obj, defaults=defaults)
    return None


# ---- id & progress helpers -----------------------------------------------------

def _ids_from_metadata(meta: Dict[str, Any]) -> Dict[str, Any]:
    """Extract IMDB/TMDB/TVDB IDs from Plex GUIDs, plus show-level IDs for episodes."""
    def _grab(value: str, pat: re.Pattern):
        m = pat.search(value or "")
        return m.group(1) if m else None

    guid = str(meta.get("guid") or "")
    ids: Dict[str, Any] = {
        "imdb": _grab(guid, _PAT_IMDB),
        "tmdb": _grab(guid, _PAT_TMDB),
        "tvdb": _grab(guid, _PAT_TVDB),
    }
    gpg = str(meta.get("grandparentGuid") or "")
    if gpg:
        ids["imdb_show"] = _grab(gpg, _PAT_IMDB)
        ids["tmdb_show"] = _grab(gpg, _PAT_TMDB)
        ids["tvdb_show"] = _grab(gpg, _PAT_TVDB)
    return {k: v for k, v in ids.items() if v}


def _progress_from_psn(state: str, view_offset: int, duration: int) -> Tuple[int, ScrobbleAction]:
    try:
        duration_i = int(duration or 0)
        view_i = int(view_offset or 0)
    except Exception:
        duration_i, view_i = 0, 0

    if duration_i <= 0:
        pct = 0
    else:
        pct = int(round(100.0 * max(0, min(view_i, duration_i)) / float(duration_i)))
        pct = max(0, min(100, pct))

    st = (state or "").lower()
    if st in ("playing", "buffering"):
        act: ScrobbleAction = "start"
    elif st == "paused":
        act = "pause"
    elif st in ("stopped", "bufferingstopped"):
        act = "stop"
    else:
        # Unknown/odd states: be conservative; never mark watched
        act = "pause"

    return (pct, act)


def _safe_int(x: Any) -> Optional[int]:
    try:
        return int(x)
    except Exception:
        return None


# ---- PSN / flat parsers --------------------------------------------------------

def from_plex_pssn(payload: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    """
    Parse PlaySessionStateNotification payloads into a ScrobbleEvent.
    Accept a list of PSN entries and use the first one (Plex may batch).
    """
    defaults = defaults or {}
    arr = payload.get("PlaySessionStateNotification")
    if not isinstance(arr, list) or not arr:
        return None
    n = dict(arr[0])

    sess = str(n.get("sessionKey") or "")
    acc = str(n.get("account") or n.get("accountID") or "")
    srv = str(n.get("machineIdentifier") or defaults.get("server_uuid") or "")

    meta = {
        "guid": n.get("guid"),
        "grandparentGuid": n.get("grandparentGuid"),
        "title": n.get("title"),
        "grandparentTitle": n.get("grandparentTitle"),
        "year": _safe_int(n.get("year")),
        "index": _safe_int(n.get("index")),
        "grandparentIndex": _safe_int(n.get("grandparentIndex")),
        "duration": _safe_int(n.get("duration") or 0) or 0,
        "viewOffset": _safe_int(n.get("viewOffset") or 0) or 0,
        "type": n.get("type") or "",
        "state": n.get("state") or "",
    }
    ids = _ids_from_metadata(meta)
    p, act = _progress_from_psn(meta["state"], meta["viewOffset"], meta["duration"])
    mtype: MediaType = "episode" if (meta.get("type") or "").lower() == "episode" else "movie"
    title = meta.get("grandparentTitle") if mtype == "episode" else meta.get("title")
    season = meta.get("grandparentIndex") if mtype == "episode" else None
    number = meta.get("index") if mtype == "episode" else None

    return ScrobbleEvent(
        action=act,
        media_type=mtype,
        ids=ids,
        title=title,
        year=meta.get("year"),
        season=season,
        number=number,
        progress=p,
        account=acc or None,
        server_uuid=srv or None,
        session_key=sess or None,
        raw=payload,
    )


def from_plex_flat_playing(payload: Dict[str, Any], defaults: Optional[Dict[str, Any]] = None) -> Optional[ScrobbleEvent]:
    """
    Parse a flattened "playing" container (XML→dict style). Less rich than PSN, but usable.
    """
    defaults = defaults or {}
    size = int(payload.get("size") or 0)
    if size < 1:
        return None
    typ = (payload.get("_type") or payload.get("type") or "").lower()
    if typ != "playing":
        return None

    # Flatten the first child element resembling a media item
    first = None
    for k, v in payload.items():
        if isinstance(v, dict) and "guid" in v:
            first = v
            break
    if not first:
        return None

    meta = {
        "guid": first.get("guid"),
        "grandparentGuid": first.get("grandparentGuid"),
        "title": first.get("title"),
        "grandparentTitle": first.get("grandparentTitle"),
        "year": _safe_int(first.get("year")),
        "index": _safe_int(first.get("index")),
        "grandparentIndex": _safe_int(first.get("grandparentIndex")),
        "duration": _safe_int(first.get("duration") or 0) or 0,
        "viewOffset": _safe_int(first.get("viewOffset") or 0) or 0,
        "type": first.get("type") or "",
        "state": first.get("state") or "",
        "sessionKey": first.get("sessionKey"),
        "account": first.get("account"),
        "machineIdentifier": first.get("machineIdentifier") or defaults.get("server_uuid"),
    }
    ids = _ids_from_metadata(meta)
    p, act = _progress_from_psn(meta["state"], meta["viewOffset"], meta["duration"])
    mtype: MediaType = "episode" if (meta.get("type") or "").lower() == "episode" else "movie"
    title = meta.get("grandparentTitle") if mtype == "episode" else meta.get("title")
    season = meta.get("grandparentIndex") if mtype == "episode" else None
    number = meta.get("index") if mtype == "episode" else None

    return ScrobbleEvent(
        action=act,
        media_type=mtype,
        ids=ids,
        title=title,
        year=meta.get("year"),
        season=season,
        number=number,
        progress=p,
        account=str(meta.get("account") or "") or None,
        server_uuid=str(meta.get("machineIdentifier") or "") or None,
        session_key=str(meta.get("sessionKey") or "") or None,
        raw=payload,
    )


# ---- dispatcher ---------------------------------------------------------------

class Dispatcher:
    """
    Filters and forwards events to sinks.
    - Username whitelist supports plain names plus "id:<accountID>" and "uuid:<accountUUID>".
    - Once a session passes filters, it’s allowlisted so later pause/stop won’t be dropped.
    - Debounces 'pause' for 5 seconds per session; suppresses no-op duplicates.
    """

    def __init__(self, sinks: Iterable[ScrobbleSink], cfg_provider=None) -> None:
        self._sinks = list(sinks or [])
        self._cfg_provider = cfg_provider or _load_config
        self._session_ok: Set[str] = set()
        self._debounce: Dict[str, float] = {}
        self._last_action: Dict[str, str] = {}
        self._last_progress: Dict[str, int] = {}

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        if ev.session_key and str(ev.session_key) in self._session_ok:
            return True

        cfg = self._cfg_provider() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want_server = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))

        if want_server and ev.server_uuid and str(ev.server_uuid) != str(want_server):
            return False

        if not wl:
            if ev.session_key:
                self._session_ok.add(str(ev.session_key))
            return True

        import re as _re
        def norm(s: str) -> str: return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())

        # Plain username match
        for e in (wl if isinstance(wl, list) else [wl]):
            s = str(e).strip()
            if not s.lower().startswith(("id:", "uuid:")) and norm(s) == norm(ev.account or ""):
                if ev.session_key:
                    self._session_ok.add(str(ev.session_key))
                return True

        # id:/uuid: extracted from raw PSN when available
        raw = ev.raw or {}

        def find_psn(o):
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(k, str) and k.lower() == "playsessionstatenotification":
                        return v if isinstance(v, list) else [v]
                for v in o.values():
                    r = find_psn(v)
                    if r: return r
            elif isinstance(o, list):
                for v in o:
                    r = find_psn(v)
                    if r: return r
            return None

        n = (find_psn(raw) or [None])[0] or {}
        acc_id = str(n.get("accountID") or "")
        acc_uuid = str(n.get("accountUUID") or "").lower()

        for e in (wl if isinstance(wl, list) else [wl]):
            s = str(e).strip().lower()
            if s.startswith("id:") and acc_id and s.split(":", 1)[1].strip() == acc_id:
                if ev.session_key:
                    self._session_ok.add(str(ev.session_key))
                return True
            if s.startswith("uuid:") and acc_uuid and s.split(":", 1)[1].strip() == acc_uuid:
                if ev.session_key:
                    self._session_ok.add(str(ev.session_key))
                return True

        return False

    def _should_send(self, ev: ScrobbleEvent) -> bool:
        """
        Forward only when the action changed or progress moved by >=1%.
        Debounce 'pause' for 5 seconds per session to avoid hammering sinks.
        """
        sk = ev.session_key or "?"
        last_a = self._last_action.get(sk)
        last_p = self._last_progress.get(sk, -1)

        action_changed = ev.action != last_a
        progress_changed = abs(ev.progress - last_p) >= 1

        if ev.action == "pause":
            now = time.time()
            k = f"{sk}|pause"
            if now - self._debounce.get(k, 0) < 5.0 and not action_changed:
                return False
            self._debounce[k] = now

        if action_changed or progress_changed:
            self._last_action[sk] = ev.action
            self._last_progress[sk] = ev.progress
            return True
        return False

    def dispatch(self, ev: ScrobbleEvent) -> None:
        if not self._passes_filters(ev):
            return
        if not self._should_send(ev):
            return
        for s in self._sinks:
            try:
                s.send(ev)
            except Exception as e:
                _log(f"Sink error: {e}", "ERROR")


# ---- exports -------------------------------------------------------------------

__all__ = [
    "ScrobbleEvent", "ScrobbleSink", "Dispatcher",
    "from_plex_webhook", "from_plex_pssn", "from_plex_flat_playing",
]

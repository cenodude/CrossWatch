# /providers/scrobble/plex/watch.py
from __future__ import annotations
# 24-09-2025 Back-to-Basics Editions...and pray i guess..

import time, threading
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional, List
import requests

from ..scrobble import (
    CFG, cfg, log, debug, Dispatcher, ScrobbleEvent,
    build_trakt_payload_from_plex, _ids_from_guid_str,  # direct import allowed
)

# ------------------------- Plex helpers ------------------------------------

def _plex_headers() -> Dict[str, str]:
    return {"X-Plex-Token": cfg("providers.plex.token", ""), "Accept": "application/xml"}

def _i(v: Optional[str]) -> Optional[int]:
    try:
        return int(v) if v is not None and str(v).isdigit() else None
    except Exception:
        return None

def _pct(offset_ms: int, dur_ms: int) -> float:
    if not dur_ms or dur_ms <= 0: return 0.0
    p = max(0.0, min(99.99, 100.0 * (float(offset_ms)/float(dur_ms))))
    return round(p, 2)

_item_guid_cache: Dict[str, List[str]] = {}
_show_guid_cache: Dict[str, List[str]] = {}

def _fetch_metadata_guids(rating_key: Optional[str]) -> List[str]:
    """Fetch external GUIDs from /library/metadata/{ratingKey} (cached)."""
    if not rating_key:
        return []
    if rating_key in _item_guid_cache:
        return _item_guid_cache[rating_key]

    base = cfg("providers.plex.url", "") or (cfg("plex.server_url") or "").rstrip("/")
    verify = bool(cfg("providers.plex.verify", True))
    url = f"{base}/library/metadata/{rating_key}"
    guids: List[str] = []
    try:
        r = requests.get(url, headers=_plex_headers(), verify=verify, timeout=10)
        r.raise_for_status()
        root = ET.fromstring(r.text)
        guids = [g.attrib.get("id") for g in root.findall(".//Guid") if g.attrib.get("id")]
    except Exception as e:
        debug(f"metadata fetch failed for rk={rating_key}: {e}")
    _item_guid_cache[rating_key] = guids
    return guids

def _plex_sessions() -> Dict[str, Dict[str, Any]]:
    base = cfg("providers.plex.url", "") or (cfg("plex.server_url") or "").rstrip("/")
    if not base:
        raise RuntimeError("providers.plex.url is not set")
    verify = bool(cfg("providers.plex.verify", True))
    url = f"{base}/status/sessions"

    r = requests.get(url, headers=_plex_headers(), verify=verify, timeout=10)
    r.raise_for_status()
    root = ET.fromstring(r.text)

    out: Dict[str, Dict[str, Any]] = {}
    for v in root.findall("Video"):
        t = v.attrib.get("type")
        if t not in ("movie", "episode"):
            continue

        user = v.find("User")
        plex_user_id = user.attrib.get("id") if user is not None else None
        plex_username = user.attrib.get("title") if user is not None else None

        must_user = cfg("providers.plex.user_id")
        if must_user and plex_user_id and str(must_user) != str(plex_user_id):
            continue

        player = v.find("Player")
        state = player.attrib.get("state", "stopped") if player is not None else "stopped"

        guid = v.attrib.get("guid")                   # may be plex://...
        gpguid = v.attrib.get("grandparentGuid")      # may be plex://...
        rating_key = v.attrib.get("ratingKey")
        gp_rating_key = v.attrib.get("grandparentRatingKey")

        # Child <Guid> nodes present in /status/sessions
        guid_list = [g.attrib.get("id") for g in v.findall("Guid") if g.attrib.get("id")]

        season_hint = _i(v.attrib.get("parentIndex"))
        number_hint = _i(v.attrib.get("index"))
        view_offset = _i(v.attrib.get("viewOffset")) or 0
        duration = _i(v.attrib.get("duration")) or 0
        progress = _pct(view_offset, duration)

        session_id = v.attrib.get("ratingKey") or v.attrib.get("sessionKey") or f"rk:{rating_key}"

        server = v.find("Server")
        plex_server = server.attrib.get("title") if server is not None else None
        plex_server_uuid = server.attrib.get("machineIdentifier") if server is not None else None

        out[session_id] = dict(
            kind=t, state=state,
            guid=guid, gpguid=gpguid,
            rating_key=rating_key, gp_rating_key=gp_rating_key,
            guid_list=guid_list,
            season_hint=season_hint, number_hint=number_hint,
            progress=progress,
            plex_user_id=plex_user_id, plex_username=plex_username,
            plex_server=plex_server, plex_server_uuid=plex_server_uuid,
        )
    return out

def _payload_from_candidates(kind: str,
                             item_guids: List[str],
                             show_guids: List[str],
                             season_hint: Optional[int],
                             number_hint: Optional[int]) -> Optional[Dict[str, Any]]:
    """Build Trakt payload using multiple GUID candidates."""
    if kind == "movie":
        for g in item_guids:
            ids = _ids_from_guid_str(g)
            if ids:
                return {"movie": {"ids": {k: v for k, v in ids.items() if k in ("imdb","tmdb","tvdb")}}}
        return None

    # Episodes prefer show GUID + item GUID (for S/E)
    for sg in show_guids or []:
        for ig in item_guids or []:
            p = build_trakt_payload_from_plex("episode", ig, sg, season_hint, number_hint)
            if p: return p
    # Item-only: tvdb path may include show id + s/e
    for ig in item_guids or []:
        p = build_trakt_payload_from_plex("episode", ig, ig, season_hint, number_hint)
        if p: return p
    return None

# ----------------------- Watch loop ----------------------------------------

class WatchLoop:
    """Tiny state machine. No drama."""
    def __init__(self, dispatcher: Dispatcher):
        self.d = dispatcher
        self.live: Dict[str, Dict[str, Any]] = {}

    # Resolve Trakt payload robustly (handles plex:// GUIDs)
    def _resolve_payload(self, s: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        # 1) Try raw attrs (fast path)
        p = build_trakt_payload_from_plex(
            s["kind"], s["guid"], s["gpguid"], s["season_hint"], s["number_hint"]
        )
        if p:
            return p

        # 2) Try child <Guid> candidates; fetch show if needed
        item_candidates = list(s.get("guid_list") or [])
        show_candidates: List[str] = []

        if s.get("gpguid") and not str(s["gpguid"]).startswith("plex://"):
            show_candidates.append(s["gpguid"])

        if not item_candidates:
            item_candidates = _fetch_metadata_guids(s.get("rating_key"))
        if s["kind"] == "episode":
            gp_rk = s.get("gp_rating_key")
            if gp_rk:
                if gp_rk in _show_guid_cache:
                    show_candidates = _show_guid_cache[gp_rk]
                else:
                    show_candidates = _fetch_metadata_guids(gp_rk)
                    _show_guid_cache[gp_rk] = show_candidates

        return _payload_from_candidates(
            s["kind"], item_candidates, show_candidates, s["season_hint"], s["number_hint"]
        )

    def tick(self) -> None:
        try:
            sessions = _plex_sessions()
        except Exception as e:
            log(f"Plex sessions failed: {e}", "WARN")
            sessions = {}

        now_ids = set(sessions.keys())

        for sid, s in sessions.items():
            payload = self._resolve_payload(s)
            if not payload:
                debug(f"skip unmapped GUID: {s.get('guid')} / {s.get('gpguid')}")
                continue

            state = s["state"]; progress = s["progress"]
            ls = self.live.get(sid)

            if ls is None:
                self._emit("start", sid, s, payload, progress)
                continue

            if state == "playing":
                hb = (time.time() - ls["last_beat"]) >= float(cfg("scrobble.heartbeat_secs", 30))
                moved = abs(progress - ls["progress"]) >= 1.0
                if ls["phase"] != "start" or moved or hb:
                    self._emit("start", sid, s, payload, progress)
            elif state == "paused":
                if ls["phase"] != "pause":
                    self._emit("pause", sid, s, payload, progress)

        # Stopped sessions
        for sid in list(self.live.keys()):
            if sid not in now_ids:
                ls = self.live.pop(sid, None)
                if ls:
                    self._emit("stop", sid, ls["last_s"], ls["payload"], ls["progress"])

    def _emit(self, action: str, sid: str, s: Dict[str, Any], payload: Dict[str, Any], progress: float) -> None:
        ev = ScrobbleEvent(
            action=action, kind=s["kind"], progress=progress, payload=payload,
            session_id=sid,
            plex_user_id=s.get("plex_user_id"), plex_username=s.get("plex_username"),
            plex_server=s.get("plex_server"), plex_server_uuid=s.get("plex_server_uuid"),
        )
        debug(f"{action:<5} {s['kind']:<7} p={progress:.2f} sid={sid}")
        self.d.dispatch(ev)
        self.live[sid] = {
            "phase": action, "progress": progress, "last_beat": time.time(),
            "payload": payload, "last_s": s,
        }

# ----------------------- Service wrapper -----------------------------------

class WatchService:

    def __init__(self, sinks: Optional[List[Any]] = None):
        from ..trakt.sink import TraktSink
        sinks = sinks or [TraktSink()]
        self.dispatcher = Dispatcher(sinks)
        self._wl = WatchLoop(self.dispatcher)
        self._poll = float(cfg("scrobble.poll_secs", 5))
        self._stop = threading.Event()
        self._th: Optional[threading.Thread] = None

    # -- public API expected by crosswatch.py / scrobbler.js --

    def set_filters(self, filters: Dict[str, Any]) -> None:
        """Accept legacy UI shape under scrobble.watch.filters and canonicalize."""
        wf = dict(filters or {})
        # Map username_whitelist → scrobble.filters.plex_usernames
        usernames = wf.get("username_whitelist")
        if isinstance(usernames, list):
            CFG.setdefault("scrobble", {}).setdefault("filters", {})["plex_usernames"] = usernames
        # Map server_uuid → scrobble.filters.server_uuids
        srv = wf.get("server_uuid")
        if isinstance(srv, list):
            CFG.setdefault("scrobble", {}).setdefault("filters", {})["server_uuids"] = [str(x) for x in srv if str(x).strip()]
        elif isinstance(srv, str) and srv.strip():
            CFG.setdefault("scrobble", {}).setdefault("filters", {})["server_uuids"] = [srv.strip()]

    def start(self) -> None:
        if self._th and self._th.is_alive():
            return
        def _run():
            log("WatchService started")
            while not self._stop.is_set():
                try:
                    self._wl.tick()
                except Exception as e:
                    log(f"watch tick error: {e}", "WARN")
                self._stop.wait(self._poll)
            log("WatchService stopped")
        self._stop.clear()
        self._th = threading.Thread(target=_run, name="WatchService", daemon=True)
        self._th.start()

    def start_async(self) -> None:
        """Alias to start() for UI code that prefers async naming."""
        self.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop.set()
        if self._th:
            self._th.join(timeout=timeout)

    def is_alive(self) -> bool:
        return bool(self._th and self._th.is_alive())

    def is_stopping(self) -> bool:
        return self._stop.is_set()

    # Back-compat for older callers
    def is_running(self) -> bool:
        return self.is_alive()

# ----------------------- Public factory & shims -----------------------------

def start_watch(sinks: Optional[List[Any]] = None) -> WatchLoop:
    """Convenience for direct runners (not used by UI)."""
    ws = WatchService(sinks=sinks)
    ws.start()
    return ws._wl

def run_forever() -> None:
    ws = WatchService()
    ws.start()
    try:
        while True:
            time.sleep(float(cfg("scrobble.poll_secs", 5)))
    except KeyboardInterrupt:
        ws.stop()

def make_default_watch(sinks: Optional[List[Any]] = None) -> WatchService:
    """Factory used by crosswatch fallback; returns a non-started service."""
    return WatchService(sinks=sinks)

def autostart_from_config() -> Optional[WatchService]:
    """
     settings UI:
      - scrobble.enabled == True
      - scrobble.mode == "watch"
      - scrobble.watch.autostart == True
    """
    sc = cfg("scrobble", {}) or {}
    enabled = bool(sc.get("enabled", True))
    mode = str(sc.get("mode", "watch")).lower()
    watch_autostart = bool((sc.get("watch") or {}).get("autostart", sc.get("autostart", True)))
    if not (enabled and mode == "watch" and watch_autostart):
        log("watch autostart: skipped by config")
        return None
    svc = WatchService()
    svc.start()
    return svc

__all__ = [
    "WatchLoop", "WatchService",
    "start_watch", "run_forever",
    "make_default_watch", "autostart_from_config",
]

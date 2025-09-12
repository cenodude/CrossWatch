from __future__ import annotations
import json, gzip, threading, time, ssl
import xml.etree.ElementTree as ET
from typing import Any, Dict, Optional, Iterable, Callable, List
from urllib.parse import urlparse, urlencode

import websocket  # pip install websocket-client
import requests

# central logger (optional)
try:
    from modules._logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import (
    Dispatcher, from_plex_pssn, from_plex_flat_playing, ScrobbleSink, ScrobbleEvent
)

# Map NotificationContainer.type -> inner key
EVENT_MAP = {
    "account": "AccountUpdateNotification",
    "activity": "ActivityNotification",
    "backgroundProcessingQueue": "BackgroundProcessingQueueEventNotification",
    "playing": "PlaySessionStateNotification",
    "preference": "Setting",
    "progress": "ProgressNotification",
    "reachability": "ReachabilityNotification",
    "status": "StatusNotification",
    "timeline": "TimelineEntry",
    "transcodeSession.end": "TranscodeSession",
    "transcodeSession.start": "TranscodeSession",
    "transcodeSession.update": "TranscodeSession",
}

# ---- config ----
def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        import json as _json
        with open("config.json", "r", encoding="utf-8") as f:
            return _json.load(f)

def _plex_token(cfg: Dict[str, Any]) -> str:
    plex = cfg.get("plex") or {}
    return plex.get("account_token") or plex.get("token") or ""

def _plex_base(cfg: Dict[str, Any]) -> str:
    plex = cfg.get("plex") or {}
    base = (plex.get("server_url") or plex.get("base_url") or "http://127.0.0.1:32400").strip().rstrip("/")
    if "://" not in base:
        base = f"http://{base}"
    return base

# ---- watch service ----
class WatchService:
    """Plex WS listener. INFO-only logging. PSN > transcode, debounced stop."""

    def __init__(self, sinks: Optional[Iterable[ScrobbleSink]] = None,
                 dispatcher: Optional[Dispatcher] = None,
                 logger: Optional[Callable[..., None]] = None):
        self._ws: Optional[websocket.WebSocketApp] = None
        self._thread: Optional[threading.Thread] = None
        self._thr: Optional[threading.Thread] = None  # legacy alias
        self._stop = threading.Event()
        self._logger = logger or (BASE_LOG.child("WATCH") if BASE_LOG else None)
        try:
            self._logger.set_level("INFO")
        except Exception:
            pass
        self._dumped_sample = False  # kept but unused at INFO
        self._psn_sessions: set[str] = set()  # sessions with PSN seen
        self._last_seen: Dict[str, float] = {}  # sessionKey -> ts (for stop debounce)

        if dispatcher is not None:
            self._d = dispatcher
        elif sinks is not None:
            self._d = Dispatcher(list(sinks), _load_config)
        else:
            self._d = Dispatcher([], _load_config)

    # ---- logging adapter ----
    def _log(self, msg: str, level: str = "INFO"):
        if self._logger:
            try:
                self._logger(str(msg), level=level.lower()); return
            except Exception:
                pass
        if level in ("INFO", "WARN", "ERROR"):
            print(f"{level} [WATCH] {msg}")

    # ---- ws url ----
    def _plex_ws_url(self, cfg: Dict[str, Any]) -> str:
        plex = cfg.get("plex") or {}
        if plex.get("ws_url") or plex.get("websocket_url"):
            return plex.get("ws_url") or plex.get("websocket_url")
        base = _plex_base(cfg)
        p = urlparse(base)
        scheme = "wss" if p.scheme == "https" else "ws"
        host = p.hostname or "127.0.0.1"
        port = p.port or (32400 if p.scheme in ("http", "ws", "") else 32443)
        qs = urlencode({
            "X-Plex-Token": _plex_token(cfg),
            "X-Plex-Client-Identifier": "CrossWatch",
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
            "X-Plex-Device": "CrossWatch",
            "X-Plex-Device-Name": "CrossWatch",
            "X-Plex-Platform": "Linux",
        })
        return f"{scheme}://{host}:{port}/:/websockets/notifications?{qs}"

    # ---- whitelist (pre-dispatch) ----
    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        cfg = _load_config() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want_server = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))
        if want_server and ev.server_uuid and str(ev.server_uuid) != str(want_server):
            return False
        if not wl:
            return True
        import re as _re
        def norm(s: str) -> str: return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())
        # title
        for e in (wl if isinstance(wl, list) else [wl]):
            s = str(e).strip()
            if not s.lower().startswith(("id:", "uuid:")) and norm(s) == norm(ev.account or ""):
                return True
        # id/uuid from PSN
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
            s = str(e).strip()
            if s.lower().startswith("id:") and acc_id and s.split(":", 1)[1].strip() == acc_id:
                return True
            if s.lower().startswith("uuid:") and acc_uuid and s.split(":", 1)[1].strip().lower() == acc_uuid:
                return True
        return False

    # ---- sessions (fallback for transcode) ----
    def _fetch_sessions(self) -> Optional[ET.Element]:
        cfg = _load_config()
        url = f"{_plex_base(cfg)}/status/sessions"
        headers = {"X-Plex-Token": _plex_token(cfg)} if _plex_token(cfg) else {}
        try:
            r = requests.get(url, headers=headers, timeout=6)
            r.raise_for_status()
            return ET.fromstring(r.text)
        except Exception as e:
            self._log(f"sessions fetch failed: {e}", "ERROR")
            return None

    def _pick_video_for_transcode(self) -> Optional[ET.Element]:
        el = self._fetch_sessions()
        if el is None:
            return None
        for v in el.findall(".//Video"):
            if v.get("viewOffset") is not None or v.find(".//TranscodeSession") is not None:
                return v
        return el.find(".//Video")

    def _video_to_event(self, v: ET.Element, action: str) -> Optional[ScrobbleEvent]:
        try:
            md: Dict[str, Any] = {
                "type": (v.get("type") or "").lower(),
                "title": v.get("title"),
                "year": int(v.get("year")) if v.get("year") else None,
                "duration": int(v.get("duration")) if v.get("duration") else None,
                "guid": v.get("guid"),
                "grandparentGuid": v.get("grandparentGuid"),
                "parentGuid": v.get("parentGuid"),
                "grandparentTitle": v.get("grandparentTitle"),
                "parentIndex": int(v.get("parentIndex")) if v.get("parentIndex") else None,
                "index": int(v.get("index")) if v.get("index") else None,
                "ratingKey": v.get("ratingKey"),
                "sessionKey": v.get("sessionKey"),
                "viewOffset": int(v.get("viewOffset")) if v.get("viewOffset") else 0,
            }
            psn = {"PlaySessionStateNotification": [{
                "state": {"start": "playing", "pause": "paused", "stop": "stopped"}[action],
                "type": md["type"], "title": md["title"], "year": md["year"], "duration": md["duration"],
                "guid": md["guid"], "grandparentGuid": md["grandparentGuid"], "parentGuid": md["parentGuid"],
                "grandparentTitle": md["grandparentTitle"], "parentIndex": md["parentIndex"], "index": md["index"],
                "ratingKey": md["ratingKey"], "sessionKey": md["sessionKey"], "viewOffset": md["viewOffset"],
                "account": None, "machineIdentifier": v.get("machineIdentifier") or None,
            }]}
            cfg = _load_config()
            defaults = {"username": ((cfg.get("plex") or {}).get("username") or ""),
                        "server_uuid": ((cfg.get("plex") or {}).get("server_uuid") or "")}
            return from_plex_pssn(psn, defaults=defaults)
        except Exception as e:
            self._log(f"video->event failed: {e}", "ERROR")
            return None

    # ---- lifecycle ----
    def start(self):
        if self._thread and self._thread.is_alive():
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run, name="PlexWatch", daemon=True)
        self._thr = self._thread
        self._thread.start()
        self._log("watch started", "INFO")

    def stop(self):
        self._stop.set()
        if self._ws:
            try: self._ws.close()
            except Exception: pass
        if self._thread:
            self._thread.join(timeout=3)
        self._log("watch stopped", "INFO")

    # ---- ws callbacks ----
    def _on_open(self, _ws): self._log("connected", "INFO")
    def _on_close(self, _ws, *a, **k): self._log("disconnected", "INFO")
    def _on_error(self, _ws, e): self._log(f"error: {e}", "ERROR")
    def _on_pong(self, _ws, data): pass  # keep quiet at INFO

    def _on_message(self, _ws, message: str):
        try:
            obj = json.loads(message)
        except Exception:
            return

        container = obj.get("NotificationContainer") if isinstance(obj, dict) else None
        if not isinstance(container, dict):
            return

        ctype = container.get("type")
        size = container.get("size")
        key = EVENT_MAP.get(ctype) if isinstance(ctype, str) else None

        cfg = _load_config()
        defaults = {"username": ((cfg.get("plex") or {}).get("username") or ""),
                    "server_uuid": ((cfg.get("plex") or {}).get("server_uuid") or "")}

        ev: Optional[ScrobbleEvent] = None

        # prefer PSN
        if key and size == 1 and key in container and isinstance(container[key], list):
            if ctype == "playing":
                mini = {"PlaySessionStateNotification": container[key]}
                ev = from_plex_pssn(mini, defaults=defaults)
                if ev and ev.session_key:
                    self._psn_sessions.add(str(ev.session_key))

            elif ctype in ("transcodeSession.start", "transcodeSession.end"):
                # ignore transcode if we already have PSN for this session
                # otherwise synthesize with debounce for 'end'
                action = "start" if ctype.endswith(".start") else "stop"
                if action == "start":
                    if not ev:
                        v = self._pick_video_for_transcode()
                        if v is not None:
                            tmp = self._video_to_event(v, "start")
                            if tmp and tmp.session_key and str(tmp.session_key) not in self._psn_sessions:
                                ev = tmp
                else:
                    # stop: confirm after short delay to avoid flap
                    time.sleep(1.5)
                    v2 = self._pick_video_for_transcode()
                    if v2 is None and not ev:
                        # still gone; synthesize stop if no PSN for this session
                        # (we don't know the sessionKey reliably here; best-effort)
                        pass  # let flat/psn handle real stop

        if not ev:
            # some servers send flat "type=playing"
            ev = from_plex_flat_playing(container, defaults=defaults)
            if ev and ev.session_key:
                self._psn_sessions.add(str(ev.session_key))

        if not ev:
            return

        if not self._passes_filters(ev):
            return

        # stop debounce: require 2s since last seen for same session
        if ev.session_key and ev.action == "stop":
            sk = str(ev.session_key)
            last = self._last_seen.get(sk, 0.0)
            now = time.time()
            if now - last < 2.0:
                return

        if ev.session_key:
            self._last_seen[str(ev.session_key)] = time.time()

        self._log(f"event {ev.action} {ev.media_type} user={ev.account} p={ev.progress} sess={ev.session_key}", "INFO")
        try:
            self._d.dispatch(ev)
        except Exception as e:
            self._log(f"dispatch failed: {e}", "ERROR")

    def _on_data(self, _ws, data, opcode, fin):
        if isinstance(data, (bytes, bytearray)):
            try:
                text = gzip.decompress(data).decode("utf-8", "ignore")
            except Exception:
                try:
                    text = data.decode("utf-8", "ignore")
                except Exception:
                    return
            self._on_message(None, text)

    # ---- loop ----
    def _run(self):
        while not self._stop.is_set():
            try:
                cfg = _load_config()
                url = self._plex_ws_url(cfg)
                # log safe endpoint (no token)
                try:
                    p = urlparse(url)
                    safe = f"{p.scheme}://{p.hostname}:{p.port}/:/websockets/notifications"
                    self._log(f"connect {safe}", "INFO")
                except Exception:
                    self._log("connect ws", "INFO")

                self._ws = websocket.WebSocketApp(
                    url,
                    on_open=self._on_open,
                    on_close=self._on_close,
                    on_error=self._on_error,
                    on_pong=self._on_pong,
                    on_message=self._on_message,
                    on_data=self._on_data,
                )

                sslopt = {"cert_reqs": ssl.CERT_NONE} if url.startswith("wss://") else {}
                t = threading.Thread(
                    target=self._ws.run_forever,
                    kwargs={"sslopt": sslopt, "ping_interval": 30, "ping_timeout": 10},
                    daemon=True,
                )
                t.start()

                while not self._stop.is_set() and t.is_alive():
                    time.sleep(0.5)

                try:
                    self._ws.close()
                except Exception:
                    pass
                self._ws = None

            except Exception as e:
                self._log(f"loop error: {e}", "ERROR")

            if self._stop.is_set():
                break
            time.sleep(3)

# convenience
def make_default_watch(sinks: Iterable[ScrobbleSink]) -> WatchService:
    return WatchService(sinks=sinks)

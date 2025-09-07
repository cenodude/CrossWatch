# providers/scrobble/plex/watch.py
from __future__ import annotations
import json
import threading
import time
import ssl
from typing import Any, Dict, Optional
from urllib.parse import urlparse, urlencode

import websocket  # pip install websocket-client

from providers.scrobble.scrobble import Dispatcher, from_plex_pssn


def _load_config() -> Dict[str, Any]:
    """Load config from app or local file."""
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        import json as _json
        with open("config.json", "r", encoding="utf-8") as f:
            return _json.load(f)


class WatchService:
    """Plex WS watcher → dispatch play/pause/stop with state de-dup."""

    def __init__(self, dispatcher: Optional[Dispatcher], logger=None):
        self._d = dispatcher
        self._logger = logger
        self._thr: Optional[threading.Thread] = None
        self._stop = threading.Event()
        # per-session de-dupe state
        self._state: Dict[str, Dict[str, Any]] = {}

    # ---- internals ---------------------------------------------------------

    def _log(self, msg: str, level: str = "INFO"):
        try:
            if self._logger:
                self._logger(msg, level=level, module="WATCH")
        except Exception:
            pass

    def _ws_url(self, cfg: Dict[str, Any]) -> str:
        plex = cfg.get("plex") or {}
        base = (plex.get("server_url") or plex.get("base_url") or "http://127.0.0.1:32400").strip()
        tok = (plex.get("account_token") or "").strip()

        u = urlparse(base if "://" in base else f"http://{base}")
        scheme = "wss" if u.scheme == "https" else "ws"
        host = u.hostname or "127.0.0.1"
        port = u.port or (443 if scheme == "wss" else 32400)

        qs = urlencode({
            "X-Plex-Token": tok,
            "X-Plex-Client-Identifier": "CrossWatch",
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
            "X-Plex-Device": "CrossWatch",
            "X-Plex-Device-Name": "CrossWatch",
            "X-Plex-Platform": "Linux",
        })
        return f"{scheme}://{host}:{port}/:/websockets/notifications?{qs}"

    def _ensure_dispatcher(self):
        if self._d is not None:
            return
        try:
            from providers.scrobble.scrobble import Dispatcher
            from providers.scrobble.trakt.sink import TraktSink
            self._d = Dispatcher([TraktSink(logger=self._logger)])
            self._log("dispatcher initialized (auto)", "WARN")
        except Exception as e:
            self._log(f"dispatcher init failed: {e}", "ERROR")

    # ---- ws handlers -------------------------------------------------------

    def _on_message(self, message: str):
        try:
            obj = json.loads(message)
        except Exception:
            # non-JSON noise
            return

        cfg = _load_config()
        defaults = {
            "username": (cfg.get("plex") or {}).get("username") or "",
            "server_uuid": (cfg.get("plex") or {}).get("server_uuid") or "",
        }

        ev = from_plex_pssn(obj, defaults=defaults)
        if not ev:
            # ignore non-playstate frames (transcode/update/etc.)
            return

        # --- de-dupe: gate repeated states/progress bursts ---
        sess = ev.session_key or "?"
        st = self._state.get(sess) or {}
        last = st.get("action")
        last_ts = float(st.get("ts") or 0)
        last_prog = float(st.get("progress") or -1)
        now = time.time()

        # hard gate: only one START until PAUSE or STOP
        if last == "start" and ev.action == "start":
            self._log(f"dedup start sess={sess}", "DEBUG")
            return

        # light debounce for repeated pause/stop
        if last == ev.action:
            if ev.action == "pause" and abs((ev.progress or 0) - last_prog) < 1.0 and (now - last_ts) < 5.0:
                self._log(f"dedup pause sess={sess}", "DEBUG")
                return
            if ev.action == "stop" and (now - last_ts) < 5.0:
                self._log(f"dedup stop sess={sess}", "DEBUG")
                return

        self._state[sess] = {"action": ev.action, "ts": now, "progress": ev.progress}

        self._ensure_dispatcher()
        if not self._d:
            self._log("no dispatcher; dropping event", "ERROR")
            return

        res = self._d.send(ev)
        self._log(f"dispatch {ev.action} {ev.media_type} -> {res}", "DEBUG")

    def _run(self):
        backoff = 2
        while not self._stop.is_set():
            cfg = _load_config()
            url = self._ws_url(cfg)

            plex = cfg.get("plex") or {}
            if not (plex.get("account_token")):
                self._log("missing plex.account_token", "ERROR")
                time.sleep(5)
                continue

            self._log(f"connect {url}", "INFO")
            ws = None
            try:
                ws = websocket.WebSocketApp(
                    url,
                    on_open=lambda w: self._log("connected", "INFO"),
                    on_close=lambda w, c, m: self._log(f"closed code={c} msg={m}", "WARN"),
                    on_error=lambda w, e: self._log(f"error {e}", "ERROR"),
                    on_message=lambda w, m: self._on_message(m),
                )

                sslopt = None
                if url.startswith("wss://"):
                    # allow self-signed on LAN
                    sslopt = {"cert_reqs": ssl.CERT_NONE}

                ws.run_forever(ping_interval=30, ping_timeout=20, sslopt=sslopt)

            except Exception as e:
                self._log(f"ws fatal {e}", "ERROR")
            finally:
                try:
                    if ws:
                        ws.close()
                except Exception:
                    pass

            if self._stop.is_set():
                break

            self._log(f"reconnect in {backoff}s", "WARN")
            time.sleep(backoff)
            backoff = min(backoff * 2, 60)

    # ---- public API --------------------------------------------------------

    def start(self):
        if self._thr and self._thr.is_alive():
            return
        self._stop.clear()
        self._thr = threading.Thread(target=self._run, name="plex-watch", daemon=True)
        self._thr.start()
        self._log("watch started")

    def stop(self):
        self._stop.set()
        if self._thr:
            self._thr.join(timeout=3)
            self._log("watch stopped")

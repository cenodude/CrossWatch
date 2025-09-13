# providers/scrobble/plex/watch.py
from __future__ import annotations

import time
import threading
from typing import Any, Dict, Iterable, Optional, Set

from plexapi.server import PlexServer
from plexapi.alert import AlertListener

try:
    from modules._logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import (
    Dispatcher,
    ScrobbleSink,
    ScrobbleEvent,
    from_plex_pssn,
    from_plex_flat_playing,
)

# ------------------------------------------------------------------------------
# Config helpers
# ------------------------------------------------------------------------------

def _load_config() -> Dict[str, Any]:
    """Prefer crosswatch.load_config(); fallback to config.json."""
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        import json, pathlib
        p = pathlib.Path("config.json")
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}

def _plex_base_and_token(cfg: Dict[str, Any]) -> tuple[str, str]:
    plex = cfg.get("plex") or {}
    base = (plex.get("server_url") or plex.get("base_url") or "http://127.0.0.1:32400").strip().rstrip("/")
    if "://" not in base:
        base = f"http://{base}"
    token = plex.get("account_token") or plex.get("token") or ""
    return base, token

# ------------------------------------------------------------------------------
# Watch service (plexapi-based)
# ------------------------------------------------------------------------------

class WatchService:
    """
    Plex AlertListener-based watcher:
      - Prefer PSN (PlaySessionStateNotification); fallback to flat "playing".
      - Enrich events via plexapi.fetchItem(ratingKey) -> title/year/ids.
      - Resolve account (username/title) via /status/sessions for whitelist match.
      - Session allow-list before dispatch; debounce & dedupe 'stop'.
      - Exponential backoff reconnects.
    """

    def __init__(
        self,
        sinks: Optional[Iterable[ScrobbleSink]] = None,
        dispatcher: Optional[Dispatcher] = None,
    ) -> None:
        self._dispatch = dispatcher or Dispatcher(list(sinks or []), _load_config)
        self._plex: Optional[PlexServer] = None
        self._listener: Optional[AlertListener] = None

        # lifecycle state for async run
        self._stop = threading.Event()
        self._bg: Optional[threading.Thread] = None

        # state/filters
        self._psn_sessions: Set[str] = set()
        self._allowed_sessions: Set[str] = set()
        self._last_seen: Dict[str, float] = {}
        self._last_emit: Dict[str, tuple[str, int]] = {}  # session_key -> (last_action, last_progress)
        self._attempt: int = 0

        # logging
        self._logger = getattr(BASE_LOG, "child", lambda *_: None)("WATCH") if BASE_LOG else None
        try:
            if self._logger and hasattr(self._logger, "set_level"):
                self._logger.set_level("INFO")
        except Exception:
            pass

    # ------------------------- logging -----------------------------------------

    def _log(self, msg: str, level: str = "INFO") -> None:
        if BASE_LOG:
            try:
                BASE_LOG("WATCH", level.upper(), str(msg))
                return
            except Exception:
                pass
        print(f"{level} [WATCH] {msg}")


    # ------------------------- filtering (pre-dispatch) -------------------------

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        """Session allow-list + username/server whitelist (fast pre-filter)."""
        if ev.session_key and ev.session_key in self._allowed_sessions:
            return True

        cfg = _load_config() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want_server = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))

        if want_server and ev.server_uuid and str(ev.server_uuid) != str(want_server):
            return False

        if not wl:
            if ev.session_key:
                self._allowed_sessions.add(ev.session_key)
            return True

        import re as _re
        def norm(s: str) -> str: return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())
        wl_list = wl if isinstance(wl, list) else [wl]

        # plain username/title
        for e in wl_list:
            s = str(e).strip()
            if not s.lower().startswith(("id:", "uuid:")) and norm(s) == norm(ev.account or ""):
                if ev.session_key:
                    self._allowed_sessions.add(ev.session_key)
                return True

        # id:/uuid: from PSN inside raw
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
        for e in wl_list:
            s = str(e).strip().lower()
            if s.startswith("id:") and acc_id and s.split(":", 1)[1].strip() == acc_id:
                if ev.session_key: self._allowed_sessions.add(ev.session_key)
                return True
            if s.startswith("uuid:") and acc_uuid and s.split(":", 1)[1].strip() == acc_uuid:
                if ev.session_key: self._allowed_sessions.add(ev.session_key)
                return True
        return False

    # ------------------------- enrichment (plexapi) -----------------------------

    def _find_rating_key(self, raw: Dict[str, Any]) -> Optional[int]:
        """Pull ratingKey out of PSN payload."""
        if not isinstance(raw, dict):
            return None
        psn = raw.get("PlaySessionStateNotification")
        if isinstance(psn, list) and psn:
            rk = psn[0].get("ratingKey") or psn[0].get("ratingkey")
            try:
                return int(rk) if rk is not None else None
            except Exception:
                return None
        # flat fallback: search shallow keys
        for v in raw.values():
            if isinstance(v, dict) and ("ratingKey" in v or "ratingkey" in v):
                try:
                    return int(v.get("ratingKey") or v.get("ratingkey"))
                except Exception:
                    pass
        return None

    def _ids_from_guids(self, guids: Any) -> Dict[str, Any]:
        """Map plexapi item.guids to Trakt-style ids."""
        out: Dict[str, Any] = {}
        try:
            for g in (guids or []):
                gid = getattr(g, "id", None) or ""
                gid = str(gid)
                low = gid.lower()
                if "imdb://" in low:
                    out.setdefault("imdb", gid.split("imdb://", 1)[1])
                elif "tmdb://" in low:
                    val = gid.split("tmdb://", 1)[1]
                    if val.isdigit():
                        out.setdefault("tmdb", int(val))
                elif "thetvdb://" in low or "tvdb://" in low:
                    val = gid.split("://", 1)[1]
                    if val.isdigit():
                        out.setdefault("tvdb", int(val))
        except Exception:
            pass
        return out

    def _resolve_account_from_session(self, session_key: Optional[str]) -> Optional[str]:
        """
        Use /status/sessions to find the User title for the given sessionKey.
        Returns a 'title' (friendly display name), which we normalize for whitelist match.
        """
        if not (self._plex and session_key):
            return None
        try:
            # raw XML through plexapi
            el = self._plex.query("/status/sessions")
            # plexapi returns an ElementTree.Element
            for v in el.iter("Video"):
                if v.get("sessionKey") == str(session_key):
                    u = v.find("User")
                    if u is not None:
                        return u.get("title") or None
            return None
        except Exception:
            return None

    def _enrich_event_with_plex(self, ev: ScrobbleEvent) -> ScrobbleEvent:
        """
        Use plexapi to fetch full metadata by ratingKey.
        Fill title/year/season/number/external IDs and resolve account if missing.
        """
        try:
            if not self._plex:
                return ev
            rk = self._find_rating_key(ev.raw or {})
            if not rk:
                # still try to resolve account if we can
                if not ev.account:
                    acc = self._resolve_account_from_session(ev.session_key)
                    if acc:
                        return ScrobbleEvent(**{**ev.__dict__, "account": acc})
                return ev
            it = self._plex.fetchItem(int(rk))

            # base fields
            media_type = getattr(it, "type", "") or ev.media_type
            title = getattr(it, "title", None)
            year = getattr(it, "year", None)
            ids = dict(ev.ids or {})
            ids.update(self._ids_from_guids(getattr(it, "guids", [])))

            season = ev.season
            number = ev.number

            if media_type == "episode":
                try:
                    show = it.show()
                except Exception:
                    show = None
                show_title = getattr(show, "title", None) if show else getattr(it, "grandparentTitle", None)
                title = show_title or title
                season = getattr(it, "seasonNumber", None) if hasattr(it, "seasonNumber") else season
                number = getattr(it, "index", None) if hasattr(it, "index") else number
                if show:
                    ids_show = self._ids_from_guids(getattr(show, "guids", []))
                    for k, v in ids_show.items():
                        ids[f"{k}_show"] = v

            # resolve account if missing
            account = ev.account or self._resolve_account_from_session(ev.session_key)

            return ScrobbleEvent(
                action=ev.action,
                media_type="episode" if media_type == "episode" else "movie",
                ids=ids,
                title=title,
                year=year,
                season=season,
                number=number,
                progress=ev.progress,
                account=account,
                server_uuid=ev.server_uuid,
                session_key=ev.session_key,
                raw=ev.raw,
            )
        except Exception:
            return ev

    # ------------------------- alert handling -----------------------------------

    def _handle_alert(self, alert: Dict[str, Any]) -> None:
        # DEBUG: show every alert
        try:
            self._log(f"alert type={alert.get('type')} keys={list(alert.keys())[:6]}")
        except Exception:
            pass

        try:
            t = (alert.get("type") or "").lower()
            if t not in ("playing", "transcodesession.start", "transcodesession.end"):
                return

            # Defaults / context
            server_uuid = None
            try:
                server_uuid = self._plex.machineIdentifier if self._plex else None
            except Exception:
                pass
            cfg = _load_config()
            defaults = {
                "username": (cfg.get("plex") or {}).get("username") or "",
                "server_uuid": server_uuid or (cfg.get("plex") or {}).get("server_uuid") or "",
            }

            ev: Optional[ScrobbleEvent] = None

            if t == "playing":
                # Preferred path: PSN present (>=1 entries)
                psn = alert.get("PlaySessionStateNotification")
                if isinstance(psn, list) and psn:
                    ev = from_plex_pssn({"PlaySessionStateNotification": psn}, defaults=defaults)
                    if ev and ev.session_key:
                        self._psn_sessions.add(str(ev.session_key))

                # Fallback: some servers send a flat "playing" container
                if not ev:
                    flat = dict(alert)
                    flat["_type"] = "playing"
                    ev = from_plex_flat_playing(flat, defaults=defaults)
                    if ev and ev.session_key:
                        self._psn_sessions.add(str(ev.session_key))

            elif t in ("transcodesession.start", "transcodesession.end"):
                # Ignore transcode events; PSN/playing carries state better.
                return

            if not ev:
                self._log("alert parsed but no event produced (unknown shape)", "WARN")
                return

            # Enrich with plexapi metadata (title/year/ids) and resolve account
            ev = self._enrich_event_with_plex(ev)

            if not self._passes_filters(ev):
                self._log(f"event filtered: user={ev.account} server={ev.server_uuid}", "INFO")
                return

            # Stop debounce: require 2s since last event for same session
            if ev.session_key and ev.action == "stop":
                skd = str(ev.session_key)
                last = self._last_seen.get(skd, 0.0)
                now = time.time()
                if now - last < 2.0:
                    self._log(f"drop stop due to debounce sess={skd}", "INFO")
                    return

            if ev.session_key:
                self._last_seen[str(ev.session_key)] = time.time()

            # --- suppress duplicate STOP spam per session (same progress) ---
            sk = str(ev.session_key) if ev.session_key else None
            if sk:
                prev = self._last_emit.get(sk)
                if prev:
                    last_action, last_progress = prev
                    if ev.action == "stop" and last_action == "stop" and abs(ev.progress - last_progress) <= 1:
                        self._log(f"suppress duplicate stop sess={sk} p={ev.progress}", "INFO")
                        return
                self._last_emit[sk] = (ev.action, ev.progress)

            self._log(f"event {ev.action} {ev.media_type} user={ev.account} p={ev.progress} sess={ev.session_key}")
            self._dispatch.dispatch(ev)

        except Exception as e:
            self._log(f"_handle_alert failure: {e}", "ERROR")

    # ------------------------- lifecycle ----------------------------------------

    def start(self) -> None:
        """Connect AlertListener and run until stop() is called, with backoff."""
        self._stop.clear()
        self._log("Ensuring AlertListener is running (plexapi)")
        while not self._stop.is_set():
            try:
                base, token = _plex_base_and_token(_load_config())
                self._plex = PlexServer(base, token)

                # Start via PlexServer.startAlertListener(): it spawns the thread for us.
                self._listener = self._plex.startAlertListener(
                    callback=self._handle_alert,
                    callbackError=lambda e: self._log(f"listener error: {e}", "ERROR"),
                )
                self._attempt = 0
                self._log("AlertListener connected")

                # Wait until stop() is requested or listener thread dies
                while not self._stop.is_set() and self._listener and self._listener.is_alive():
                    time.sleep(0.5)

            except Exception as e:
                self._log(f"alert loop error: {e}", "ERROR")

            if self._stop.is_set():
                break

            # Reconnect with exponential backoff (cap ~30s)
            self._attempt += 1
            delay = min(30, (2 ** min(self._attempt, 5))) + (time.time() % 1.5)
            self._log(f"reconnecting after {delay:.1f}s")
            time.sleep(delay)

    def stop(self) -> None:
        """Stop the AlertListener."""
        self._stop.set()
        try:
            if self._listener:
                self._listener.stop()
        except Exception:
            pass
        self._log("Watch service stopping")

    # --- compatibility helpers used by your API/debug endpoints ---
    def start_async(self) -> None:
        """Start watcher in a background thread."""
        if self._bg and self._bg.is_alive():
            return
        self._bg = threading.Thread(target=self.start, name="PlexWatch", daemon=True)
        self._bg.start()

    def is_alive(self) -> bool:
        """True if background thread is running."""
        return bool(self._bg and self._bg.is_alive())

    def is_stopping(self) -> bool:
        """True if stop signal was set."""
        return bool(self._stop and self._stop.is_set())

# Convenience factory
def make_default_watch(sinks: Iterable[ScrobbleSink]) -> WatchService:
    return WatchService(sinks=sinks)

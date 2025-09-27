# providers/scrobble/watch.py
# Refactoring project: watch.py (1.0)
from __future__ import annotations

import json, os, time, threading
from pathlib import Path
from typing import Any, Iterable

from plexapi.server import PlexServer
from plexapi.alert import AlertListener

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import (
    Dispatcher, ScrobbleSink, ScrobbleEvent,
    from_plex_pssn, from_plex_flat_playing,
)

# --- config / utils ------------------------------------------------------------
def _cfg() -> dict[str, Any]:
    p = Path("/config/config.json")
    try: return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    except Exception: return {}

def _is_debug() -> bool:
    try: return bool((( _cfg().get("runtime") or {}).get("debug")))
    except Exception: return False

def _plex_btok(cfg: dict[str, Any]) -> tuple[str, str]:
    px = cfg.get("plex") or {}
    base = (px.get("server_url") or px.get("base_url") or "http://127.0.0.1:32400").strip().rstrip("/")
    if "://" not in base: base = f"http://{base}"
    return base, (px.get("account_token") or px.get("token") or "")

def _safe_int(x: Any) -> int | None:
    try: return int(x)
    except Exception: return None

# --- service -------------------------------------------------------------------
class WatchService:
    def __init__(self, sinks: Iterable[ScrobbleSink] | None = None, dispatcher: Dispatcher | None = None) -> None:
        self._dispatch = dispatcher or Dispatcher(list(sinks or []), _cfg)
        self._plex: PlexServer | None = None
        self._listener: AlertListener | None = None
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None

        self._psn_sessions: set[str] = set()
        self._allowed_sessions: set[str] = set()
        self._last_seen: dict[str, float] = {}
        self._last_emit: dict[str, tuple[str, int]] = {}
        self._attempt = 0

    # logging
    def _log(self, msg: str, level: str = "INFO") -> None:
        if BASE_LOG:
            try: BASE_LOG(str(msg), level=level.upper(), module="WATCH"); return
            except Exception: pass
        print(f"{level} [WATCH] {msg}")

    def _dbg(self, msg: str) -> None:
        if _is_debug(): print(f"DEBUG [WATCH] {msg}")

    def sinks_count(self) -> int:
        try: return len(getattr(self._dispatch, "_sinks", []) or [])
        except Exception: return 0

    # filtering helpers
    def _find_psn(self, o: Any):
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(k, str) and k.lower() == "playsessionstatenotification":
                    return v if isinstance(v, list) else [v]
            for v in o.values():
                r = self._find_psn(v)
                if r: return r
        elif isinstance(o, list):
            for v in o:
                r = self._find_psn(v)
                if r: return r
        return None

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        if ev.session_key and ev.session_key in self._allowed_sessions: return True
        cfg  = _cfg() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl   = filt.get("username_whitelist")
        want = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))
        if want and ev.server_uuid and str(ev.server_uuid) != str(want): return False

        def _allow() -> bool:
            if ev.session_key: self._allowed_sessions.add(str(ev.session_key))
            return True

        if not wl: return _allow()

        import re as _re
        def norm(s: str) -> str: return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())
        wl_list = wl if isinstance(wl, list) else [wl]

        # username match
        if any(not str(x).lower().startswith(("id:", "uuid:")) and norm(str(x)) == norm(ev.account or "") for x in wl_list):
            return _allow()

        # id/uuid match from raw PSN
        n = (self._find_psn(ev.raw or {}) or [None])[0] or {}
        acc_id, acc_uuid = str(n.get("accountID") or ""), str(n.get("accountUUID") or "").lower()
        for e in wl_list:
            s = str(e).strip().lower()
            if s.startswith("id:")   and acc_id   and s.split(":",1)[1].strip()==acc_id:   return _allow()
            if s.startswith("uuid:") and acc_uuid and s.split(":",1)[1].strip()==acc_uuid: return _allow()
        return False

    # enrichment
    def _find_rating_key(self, raw: dict[str, Any]) -> int | None:
        if not isinstance(raw, dict): return None
        psn = raw.get("PlaySessionStateNotification")
        if isinstance(psn, list) and psn:
            rk = psn[0].get("ratingKey") or psn[0].get("ratingkey")
            return _safe_int(rk) if rk is not None else None
        for v in raw.values():
            if isinstance(v, dict) and ("ratingKey" in v or "ratingkey" in v):
                return _safe_int(v.get("ratingKey") or v.get("ratingkey"))
        return None

    def _ids_from_guids(self, guids: Any) -> dict[str, Any]:
        out: dict[str, Any] = {}
        try:
            for g in (guids or []):
                gid = str(getattr(g, "id", "")).lower()
                if "imdb://" in gid: out.setdefault("imdb", gid.split("imdb://",1)[1])
                elif "tmdb://" in gid:
                    v = gid.split("tmdb://",1)[1]
                    if v.isdigit(): out.setdefault("tmdb", int(v))
                elif "thetvdb://" in gid or "tvdb://" in gid:
                    v = gid.split("://",1)[1]
                    if v.isdigit(): out.setdefault("tvdb", int(v))
        except Exception:
            pass
        return out

    def _resolve_account_from_session(self, session_key: str | None) -> str | None:
        if not (self._plex and session_key): return None
        try:
            el = self._plex.query("/status/sessions")
            if not hasattr(el, "iter"): return None
            for v in el.iter("Video"):  # type: ignore[attr-defined]
                if v.get("sessionKey") == str(session_key):
                    u = v.find("User"); return (u.get("title") if u is not None else None)
        except Exception:
            pass
        return None

    def _enrich_event_with_plex(self, ev: ScrobbleEvent) -> ScrobbleEvent:
        try:
            if not self._plex: return ev
            rk = self._find_rating_key(ev.raw or {})
            if not rk:
                if not ev.account:
                    acc = self._resolve_account_from_session(ev.session_key)
                    if acc: return ScrobbleEvent(**{**ev.__dict__, "account": acc})
                return ev

            it = self._plex.fetchItem(int(rk))
            if not it: return ev

            media_type = getattr(it, "type", "") or ev.media_type
            title = getattr(it, "title", None)
            year  = getattr(it, "year", None)
            ids = dict(ev.ids or {}, plex=int(rk))
            ids.update(self._ids_from_guids(getattr(it, "guids", [])))

            season, number = ev.season, ev.number
            if media_type == "episode":
                try: show = it.show()
                except Exception: show = None
                title  = (getattr(show,"title",None) if show else getattr(it,"grandparentTitle",None)) or title
                season = getattr(it,"seasonNumber",None) if hasattr(it,"seasonNumber") else season
                number = getattr(it,"index",None) if hasattr(it,"index") else number
                if show:
                    for k,v in self._ids_from_guids(getattr(show,"guids",[])).items():
                        ids[f"{k}_show"] = v

            account = ev.account or self._resolve_account_from_session(ev.session_key)
            return ScrobbleEvent(
                action=ev.action, media_type=("episode" if media_type=="episode" else "movie"),
                ids=ids, title=title, year=year, season=season, number=number,
                progress=ev.progress, account=account, server_uuid=ev.server_uuid,
                session_key=ev.session_key, raw=ev.raw,
            )
        except Exception:
            return ev

    # probe
    def _probe_session_progress(self, ev: ScrobbleEvent) -> int | None:
        try:
            if not self._plex: return None
            sessions = self._plex.sessions()
        except Exception:
            return None
        sid = str(ev.session_key or "")
        rk  = str((ev.ids or {}).get("plex") or "")
        tgt = None
        for v in sessions:
            try:
                if (sid and str(getattr(v,"sessionKey","")) == sid) or (rk and str(getattr(v,"ratingKey","")) == rk):
                    tgt = v; break
            except Exception:
                pass
        if not tgt: return None
        d, vo = getattr(tgt,"duration",None), getattr(tgt,"viewOffset",None)
        try:
            d = int(d) if d is not None else None
            vo = int(vo) if vo is not None else None
        except Exception:
            return None
        if not d or vo is None: return None
        return int(round(100 * max(0, min(vo, d)) / float(d)))

    # alert handling
    def _handle_alert(self, alert: dict[str, Any]) -> None:
        try:
            t = (alert.get("type") or "").lower()
            if t != "playing": return
        except Exception:
            return
        try:
            try: server_uuid = self._plex.machineIdentifier if self._plex else None
            except Exception: server_uuid = None
            cfg = _cfg()
            defaults = {"username": (cfg.get("plex") or {}).get("username") or "",
                        "server_uuid": server_uuid or (cfg.get("plex") or {}).get("server_uuid") or ""}

            ev: ScrobbleEvent | None = None
            psn = alert.get("PlaySessionStateNotification")
            if isinstance(psn, list) and psn:
                ev = from_plex_pssn({"PlaySessionStateNotification": psn}, defaults=defaults)
                if ev and ev.session_key: self._psn_sessions.add(str(ev.session_key))
            if not ev:
                flat = dict(alert); flat["_type"] = "playing"
                ev = from_plex_flat_playing(flat, defaults=defaults)
                if ev and ev.session_key: self._psn_sessions.add(str(ev.session_key))
            if not ev:
                self._dbg("alert parsed but no event produced (unknown shape)"); return

            ev = self._enrich_event_with_plex(ev)
            if not self._passes_filters(ev):
                self._dbg(f"event filtered: user={ev.account} server={ev.server_uuid}"); return

            # probe correction (debug only)
            want = ev.progress; best = want
            for _ in range(3):
                real = self._probe_session_progress(ev)
                if real is not None and abs(real - best) >= 5: best = real
                if 5 <= best <= 95: break
                time.sleep(0.25)
            if best != want:
                self._dbg(f"probe correction: {want}% → {best}%")
                ev = ScrobbleEvent(**{**ev.__dict__, "progress": best})

            # floor tiny start to 1%
            if ev.action == "start" and ev.progress < 1:
                ev = ScrobbleEvent(**{**ev.__dict__, "progress": 1})

            # debounce stop (allow only if force_stop_at or idle > 2s)
            if ev.session_key and ev.action == "stop":
                skd, now = str(ev.session_key), time.time()
                force_at = int((((_cfg().get("scrobble") or {}).get("trakt") or {}).get("force_stop_at", 95)))
                elapsed = now - self._last_seen.get(skd, 0.0)
                if ev.progress < force_at and elapsed < 2.0:
                    self._dbg(f"drop stop due to debounce sess={skd} p={ev.progress} thr={force_at} dt={elapsed:.2f}s"); return

            if ev.session_key: self._last_seen[str(ev.session_key)] = time.time()

            # suppress duplicate stop
            sk = str(ev.session_key) if ev.session_key else None
            if sk:
                last = self._last_emit.get(sk)
                if last and ev.action == "stop" and last[0] == "stop" and abs(ev.progress - last[1]) <= 1:
                    self._dbg(f"suppress duplicate stop sess={sk} p={ev.progress}"); return
                self._last_emit[sk] = (ev.action, ev.progress)

            self._log(f"event {ev.action} {ev.media_type} user={ev.account} p={ev.progress} sess={ev.session_key}")
            self._dispatch.dispatch(ev)

        except Exception as e:
            self._log(f"_handle_alert failure: {e}", "ERROR")

    # lifecycle
    def start(self) -> None:
        self._stop.clear()
        self._log(f"Ensuring Watcher is running; wired sinks: {self.sinks_count()}")
        while not self._stop.is_set():
            try:
                base, token = _plex_btok(_cfg())
                self._plex = PlexServer(base, token)
                self._listener = self._plex.startAlertListener(
                    callback=self._handle_alert,
                    callbackError=lambda e: self._log(f"Watcher error: {e}", "ERROR"),
                )
                self._attempt = 0
                self._log("Watcher connected")
                while not self._stop.is_set() and self._listener and self._listener.is_alive():
                    time.sleep(0.5)
            except Exception as e:
                self._log(f"alert loop error: {e}", "ERROR")

            if self._stop.is_set(): break
            self._attempt += 1
            delay = min(30, (2 ** min(self._attempt, 5))) + (time.time() % 1.5)
            self._log(f"reconnecting after {delay:.1f}s"); time.sleep(delay)

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._listener: self._listener.stop()
        except Exception:
            pass
        self._log("Watch service stopping")

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive(): return
        self._bg = threading.Thread(target=self.start, name="PlexWatch", daemon=True)
        self._bg.start()

    def is_alive(self) -> bool:    return bool(self._bg and self._bg.is_alive())
    def is_stopping(self) -> bool: return bool(self._stop and self._stop.is_set())

def make_default_watch(sinks: Iterable[ScrobbleSink]) -> WatchService:
    return WatchService(sinks=sinks)

# --- autostart -----------------------------------------------------------------
_AUTO_WATCH: WatchService | None = None

def autostart_from_config() -> WatchService | None:
    global _AUTO_WATCH
    cfg = _cfg() or {}
    sc  = (cfg.get("scrobble") or {})
    if not (sc.get("enabled") and str(sc.get("mode") or "").lower() == "watch"): return None
    if not ((sc.get("watch") or {}).get("autostart")): return None
    if _AUTO_WATCH and _AUTO_WATCH.is_alive(): return _AUTO_WATCH

    try:
        from providers.scrobble.trakt.sink import TraktSink
        sinks: list[ScrobbleSink] = [TraktSink()]
    except Exception:
        return None  # no sink available

    _AUTO_WATCH = WatchService(sinks=sinks); _AUTO_WATCH.start_async()
    return _AUTO_WATCH

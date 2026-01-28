# providers/scrobble/plex/watch.py
# CrossWatch - Plex Watch Service
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time, threading, re, base64, hmac, hashlib
from typing import Any, Iterable, Mapping, Callable

from plexapi.server import PlexServer
from plexapi.alert import AlertListener

import requests

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.config_base import load_config
from providers.scrobble.scrobble import (
    Dispatcher,
    ScrobbleSink,
    ScrobbleEvent,
    from_plex_pssn,
    from_plex_flat_playing,
)
from providers.scrobble.currently_watching import update_from_event as _cw_update


_CFG_CACHE: dict[str, Any] = {"ts": 0.0, "cfg": {}}
_CFG_TTL_SEC = 2.0


def _cfg(ttl: float = _CFG_TTL_SEC) -> dict[str, Any]:
    now = time.time()
    try:
        ts = float(_CFG_CACHE.get("ts") or 0.0)
        cfg = _CFG_CACHE.get("cfg") or {}
        if isinstance(cfg, dict) and cfg and (now - ts) < float(ttl):
            return cfg
    except Exception:
        pass
    try:
        cfg2 = load_config() or {}
        if not isinstance(cfg2, dict):
            cfg2 = {}
    except Exception:
        cfg2 = {}
    _CFG_CACHE.update({"ts": now, "cfg": cfg2})
    return cfg2


def _is_debug() -> bool:
    try:
        v = ((_cfg().get("runtime") or {}).get("debug"))
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on", "y", "t")
        return False
    except Exception:
        return False


def _plex_btok(cfg: dict[str, Any]) -> tuple[str, str]:
    px = cfg.get("plex") or {}
    base = (px.get("server_url") or px.get("base_url") or "http://127.0.0.1:32400").strip().rstrip("/")
    if "://" not in base:
        base = f"http://{base}"
    return base, (px.get("account_token") or px.get("token") or "")


def _safe_int(x: Any) -> int | None:
    try:
        return int(x)
    except Exception:
        return None


def _as_set_str(v: Any) -> set[str]:
    if v is None:
        return set()
    it = v if isinstance(v, (list, tuple, set)) else [v]
    out: set[str] = set()
    for x in it:
        s = str(x).strip()
        if s:
            out.add(s)
    return out


def _ids_desc(ids: dict[str, Any] | None) -> str:
    d = ids or {}
    for k in ("trakt", "imdb", "tmdb", "tvdb"):
        if d.get(k):
            return f"{k}:{d[k]}"
    for k in ("trakt_show", "imdb_show", "tmdb_show", "tvdb_show"):
        if d.get(k):
            return f"{k.replace('_show', '')}:{d[k]}"
    return "none"


def _media_name(ev: ScrobbleEvent) -> str:
    if (ev.media_type or "").lower() == "episode":
        s = ev.season if isinstance(ev.season, int) else None
        n = ev.number if isinstance(ev.number, int) else None
        base = ev.title or "?"
        if s is not None and n is not None:
            return f"{base} S{s:02}E{n:02}"
        return base
    return ev.title or "?"


def _watch_pause_debounce_seconds(cfg: dict[str, Any]) -> int:
    try:
        return int((((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5)))
    except Exception:
        return 5


def _watch_suppress_start_at(cfg: dict[str, Any]) -> int:
    try:
        return int((((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99)))
    except Exception:
        return 99


def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try:
        return int((((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 80)))
    except Exception:
        return 80


def _force_stop_at(cfg: dict[str, Any]) -> int:
    try:
        return int((((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at", 95)))
    except Exception:
        return 95


def _normalize_ids(ids: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (ids or {}).items():
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out[str(k)] = s
    return out


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
        self._wl_removed: set[str] = set()
        self._attempt = 0
        self._max_seen: dict[str, int] = {}
        self._first_seen: dict[str, float] = {}
        self._last_pause_ts: dict[str, float] = {}
        self._filtered_ts: dict[str, float] = {}
        self._last_probe: dict[str, float] = {}

    def _log(self, msg: str, level: str = "INFO") -> None:
        lvl = (str(level) or "INFO").upper()
        if lvl == "DEBUG" and not _is_debug():
            return
        if BASE_LOG is not None:
            try:
                BASE_LOG(msg, level=lvl, module="PLEX ")
                return
            except Exception:
                pass
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"[{ts}] [PLEX ] {lvl} {msg}")

    def _dbg(self, msg: str) -> None:
        self._log(msg, "DEBUG")

    def sinks_count(self) -> int:
        try:
            return len(getattr(self._dispatch, "_sinks", []) or [])
        except Exception:
            return 0

    def _find_psn(self, o: Any):
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(k, str) and k.lower() == "playsessionstatenotification":
                    return v if isinstance(v, list) else [v]
            for v in o.values():
                r = self._find_psn(v)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = self._find_psn(v)
                if r:
                    return r
        return None

    def _plex_section_id(self, ev: ScrobbleEvent) -> str | None:
        raw = ev.raw or {}
        psn = self._find_psn(raw)
        if isinstance(psn, list) and psn:
            n = psn[0] or {}
            if isinstance(n, dict):
                for k in ("librarySectionID", "librarySectionId", "sectionID", "sectionId"):
                    v = n.get(k)
                    if v is not None:
                        s = str(v).strip()
                        if s:
                            return s
        if isinstance(raw, dict):
            for k in ("librarySectionID", "librarySectionId", "sectionID", "sectionId"):
                v = raw.get(k)
                if v is not None:
                    s = str(v).strip()
                    if s:
                        return s

        rk = self._find_rating_key(raw if isinstance(raw, dict) else {})
        if rk is None or not self._plex:
            return None
        try:
            obj = self._plex.fetchItem(int(rk))
        except Exception:
            obj = None
        if not obj:
            return None

        for attr in ("librarySectionID", "sectionID", "librarySectionId", "sectionId"):
            try:
                v = getattr(obj, attr, None)
                if v is not None:
                    s = str(v).strip()
                    if s:
                        return s
            except Exception:
                pass

        try:
            sec = getattr(obj, "section", None)
            if callable(sec):
                s2 = sec()
                key = getattr(s2, "key", None)
                if key is not None:
                    s = str(key).strip()
                    if s:
                        return s
        except Exception:
            pass
        return None

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        sk = str(ev.session_key) if ev.session_key is not None else None
        if sk and sk in self._allowed_sessions:
            return True
        cfg = _cfg() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))
        if want and ev.server_uuid and str(ev.server_uuid) != str(want):
            return False
        libs = _as_set_str((((cfg.get("plex") or {}).get("scrobble") or {}).get("libraries")))
        if libs:
            sid = self._plex_section_id(ev)
            if not sid:
                return False
            if sid not in libs:
                return False

        def _allow() -> bool:
            if sk:
                self._allowed_sessions.add(sk)
            return True

        if not wl:
            return _allow()

        import re as _re

        def norm(s: str) -> str:
            return _re.sub(r"[^a-z0-9]+", "", (s or "").lower())

        wl_list = wl if isinstance(wl, list) else [wl]
        if any(not str(x).lower().startswith(("id:", "uuid:")) and norm(str(x)) == norm(ev.account or "") for x in wl_list):
            return _allow()

        n = (self._find_psn(ev.raw or {}) or [None])[0] or {}
        acc_id, acc_uuid = str(n.get("accountID") or ""), str(n.get("accountUUID") or "").lower()
        for e in wl_list:
            s = str(e).strip().lower()
            if s.startswith("id:") and acc_id and s.split(":", 1)[1].strip() == acc_id:
                return _allow()
            if s.startswith("uuid:") and acc_uuid and s.split(":", 1)[1].strip() == acc_uuid:
                return _allow()
        return False

    def _find_rating_key(self, raw: dict[str, Any]) -> int | None:
        if not isinstance(raw, dict):
            return None
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
                if "imdb://" in gid:
                    out.setdefault("imdb", gid.split("imdb://", 1)[1])
                elif "tmdb://" in gid:
                    v = gid.split("tmdb://", 1)[1]
                    if v.isdigit():
                        out.setdefault("tmdb", int(v))
                elif "thetvdb://" in gid or "tvdb://" in gid:
                    v = gid.split("://", 1)[1]
                    if v.isdigit():
                        out.setdefault("tvdb", int(v))
        except Exception:
            pass
        return out

    def _resolve_account_from_session(self, session_key: str | None) -> str | None:
        if not (self._plex and session_key):
            return None
        try:
            el: Any = self._plex.query("/status/sessions")
            if el is None or not hasattr(el, "iter"):
                return None
            for v in el.iter("Video"):
                if v.get("sessionKey") == str(session_key):
                    u = v.find("User")
                    return (u.get("title") if u is not None else None)
        except Exception:
            pass
        return None

    def _enrich_event_with_plex(self, ev: ScrobbleEvent) -> ScrobbleEvent:
        try:
            if not self._plex:
                return ev
            rk = self._find_rating_key(ev.raw or {})
            if not rk:
                if not ev.account:
                    acc = self._resolve_account_from_session(ev.session_key)
                    if acc:
                        return ScrobbleEvent(**{**ev.__dict__, "account": acc})
                return ev

            it = self._plex.fetchItem(int(rk))
            if not it:
                return ev

            media_type = getattr(it, "type", "") or ev.media_type
            title = getattr(it, "title", None)
            year = getattr(it, "year", None)
            ids_raw: dict[str, Any] = dict(ev.ids or {}, plex=int(rk))
            ids_raw.update(self._ids_from_guids(getattr(it, "guids", [])))
            ids = _normalize_ids(ids_raw)
            season, number = ev.season, ev.number
            if media_type == "episode":
                try:
                    show = it.show()
                except Exception:
                    show = None
                title = (getattr(show, "title", None) if show else getattr(it, "grandparentTitle", None)) or title
                season = getattr(it, "seasonNumber", None) if hasattr(it, "seasonNumber") else season
                number = getattr(it, "index", None) if hasattr(it, "index") else number
                if show:
                    show_ids_raw = self._ids_from_guids(getattr(show, "guids", []))
                    for k, v in show_ids_raw.items():
                        ids.setdefault(f"{k}_show", str(v))

            account = ev.account or self._resolve_account_from_session(ev.session_key)
            return ScrobbleEvent(
                action=ev.action,
                media_type=("episode" if media_type == "episode" else "movie"),
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

    def _probe_session_progress(self, ev: ScrobbleEvent) -> int | None:
        try:
            if not self._plex:
                return None
            sessions = self._plex.sessions()
        except Exception:
            return None

        sid = str(ev.session_key or "")
        rk = str((ev.ids or {}).get("plex") or "")
        tgt = None

        # Prefer sessionKey
        if sid:
            for v in sessions:
                try:
                    if str(getattr(v, "sessionKey", "")) == sid:
                        tgt = v
                        break
                except Exception:
                    pass

        # Fallback to ratingKey
        if not tgt and rk:
            matches = []
            for v in sessions:
                try:
                    if str(getattr(v, "ratingKey", "")) == rk:
                        matches.append(v)
                except Exception:
                    pass
            if len(matches) == 1:
                tgt = matches[0]
            else:
                return None

        if not tgt:
            return None

        d = getattr(tgt, "duration", None)
        vo = getattr(tgt, "viewOffset", None)
        try:
            d = int(d) if d is not None else None
            vo = int(vo) if vo is not None else None
        except Exception:
            return None
        if not d or vo is None:
            return None
        return int(round(100 * max(0, min(vo, d)) / float(d)))

    def _throttled_filtered_log(self, ev: ScrobbleEvent) -> None:
        key = f"{ev.account}|{ev.server_uuid}|{ev.session_key or self._find_rating_key(ev.raw or {}) or '?'}"
        now = time.time()
        last = self._filtered_ts.get(key, 0.0)
        if now - last >= 30.0:
            self._dbg(f"event filtered: user={ev.account} server={ev.server_uuid}")
            self._filtered_ts[key] = now

    def _handle_alert(self, alert: dict[str, Any]) -> None:
        try:
            t = (alert.get("type") or "").lower()
            if t != "playing":
                return
        except Exception:
            return
        try:
            try:
                server_uuid = self._plex.machineIdentifier if self._plex else None
            except Exception:
                server_uuid = None
            cfg = _cfg()
            sc = (cfg.get("scrobble") or {})
            if not bool(sc.get("enabled")) or str(sc.get("mode") or "").lower() != "watch":
                return
            defaults = {
                "username": (cfg.get("plex") or {}).get("username") or "",
                "server_uuid": server_uuid or (cfg.get("plex") or {}).get("server_uuid") or "",
            }
            ev: ScrobbleEvent | None = None
            psn = alert.get("PlaySessionStateNotification")
            if isinstance(psn, list) and psn:
                ev = from_plex_pssn({"PlaySessionStateNotification": psn}, defaults=defaults)
                if ev and ev.session_key:
                    self._psn_sessions.add(str(ev.session_key))
            if not ev:
                flat = dict(alert)
                flat["_type"] = "playing"
                ev = from_plex_flat_playing(flat, defaults=defaults)
                if ev and ev.session_key:
                    self._psn_sessions.add(str(ev.session_key))
            if not ev:
                self._dbg("alert parsed but no event produced (unknown shape)")
                return
            ev = self._enrich_event_with_plex(ev)
            if not self._passes_filters(ev):
                self._throttled_filtered_log(ev)
                return
            self._log(
                f"incoming 'playing' user='{ev.account}' server='{ev.server_uuid}' media='{_media_name(ev)}'",
                "DEBUG",
            )
            self._log(f"ids resolved: {_media_name(ev)} -> {_ids_desc(ev.ids)}", "DEBUG")
            sk = str(ev.session_key) if ev.session_key else None
            if sk and sk not in self._first_seen:
                self._first_seen[sk] = time.time()
            want = ev.progress
            best = want

            probe_key = sk
            if not probe_key:
                px_id = (ev.ids or {}).get("plex")
                if px_id:
                    probe_key = f"rk:{px_id}"

            need_probe = (ev.action == "stop") or (want < 5) or (want > 95)
            if need_probe and probe_key:
                now = time.time()
                lastp = self._last_probe.get(probe_key, 0.0)
                if now - lastp >= 2.0:
                    self._last_probe[probe_key] = now
                    real = self._probe_session_progress(ev)
                    if real is not None and abs(real - best) >= 5:
                        best = real

            if ev.action == "stop" and best == want:
                prev = self._last_emit.get(sk or "", (None, None))[1] if sk else None
                if isinstance(prev, int):
                    best = prev

            if best != want:
                self._dbg(f"progress normalized: {best}%")
                ev = ScrobbleEvent(**{**ev.__dict__, "progress": best})
            if ev.action == "start" and ev.progress < 1:
                ev = ScrobbleEvent(**{**ev.__dict__, "progress": 1})
            if sk:
                self._max_seen[sk] = max(ev.progress, self._max_seen.get(sk, 0))
            sup_at = _watch_suppress_start_at(cfg)
            if ev.action == "start" and ev.progress >= sup_at:
                self._dbg(f"suppress start at {ev.progress}% (>= {float(sup_at):.1f}%)")
                return
            if ev.action == "pause" and sk:
                now = time.time()
                lastp = self._last_pause_ts.get(sk, 0.0)
                if now - lastp < max(0, _watch_pause_debounce_seconds(cfg)):
                    self._dbg(f"drop pause due to debounce sess={sk} dt={now - lastp:.2f}s")
                    return
                self._last_pause_ts[sk] = now
            if ev.session_key and ev.action == "stop":
                skd, now = str(ev.session_key), time.time()
                thr = _stop_pause_threshold(cfg)
                maxp = self._max_seen.get(skd, ev.progress)
                if ev.progress >= 98 and maxp < thr:
                    ev = ScrobbleEvent(**{**ev.__dict__, "action": "pause", "progress": maxp})
                    self._dbg(f"demote stop/pause sess={skd} p={ev.progress} max_seen={maxp} thr={thr}")
                else:
                    first = self._first_seen.get(skd)
                    age = (now - first) if first else 999.0
                    fstop = _force_stop_at(cfg)
                    if ev.progress < fstop and age < 2.0:
                        self._dbg(f"drop stop due to debounce sess={skd} p={ev.progress} thr={fstop} age={age:.2f}s")
                        return

            if ev.session_key:
                self._last_seen[str(ev.session_key)] = time.time()
            if sk:
                last = self._last_emit.get(sk)
                if last:
                    last_action, last_prog = last
                    if ev.action == "stop" and last_action == "stop" and abs(ev.progress - last_prog) <= 1:
                        self._dbg(f"suppress duplicate stop sess={sk} p={ev.progress}")
                        return
                    if ev.action == last_action and ev.progress == last_prog:
                        self._dbg(f"suppress duplicate {ev.action} sess={sk} p={ev.progress}")
                        return
                self._last_emit[sk] = (ev.action, ev.progress)
            try:
                _cw_update("plex", ev)
            except Exception:
                pass
            self._log(f"event {ev.action} {ev.media_type} user={ev.account} p={ev.progress} sess={ev.session_key}")
            self._dispatch.dispatch(ev)
        except Exception as e:
            self._log(f"_handle_alert failure: {e}", "ERROR")

    def start(self) -> None:
        self._stop.clear()
        cfg = _cfg() or {}
        sc = (cfg.get("scrobble") or {})
        if not bool(sc.get("enabled")) or str(sc.get("mode") or "").lower() != "watch":
            self._log("Watcher disabled by config; not starting", "INFO")
            return
        self._log(f"Ensuring Watcher is running; wired sinks: {self.sinks_count()}", "INFO")
        while not self._stop.is_set():
            try:
                base, token = _plex_btok(_cfg())
                if not token:
                    self._log("Missing plex.account_token or plex.token in config.json", "ERROR")
                    return
                self._plex = PlexServer(base, token)
                self._listener = self._plex.startAlertListener(
                    callback=self._handle_alert,
                    callbackError=lambda e: self._log(f"Watcher error: {e}", "ERROR"),
                )
                self._attempt = 0
                self._log("Watcher connected", "INFO")
                while not self._stop.is_set() and self._listener and self._listener.is_alive():
                    time.sleep(0.5)
            except Exception as e:
                self._log(f"alert loop error: {e}", "ERROR")
            if self._stop.is_set():
                break
            self._attempt += 1
            delay = min(30, (2 ** min(self._attempt, 5))) + (time.time() % 1.5)
            self._log(f"reconnecting after {delay:.1f}s", "DEBUG")
            time.sleep(delay)

    def stop(self) -> None:
        self._stop.set()
        try:
            if self._listener:
                self._listener.stop()
        except Exception:
            pass
        self._log("Watch service stopping", "INFO")

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._bg = threading.Thread(target=self.start, name="PlexWatch", daemon=True)
        self._bg.start()

    def is_alive(self) -> bool:
        return bool(self._bg and self._bg.is_alive())

    def is_stopping(self) -> bool:
        return bool(self._stop and self._stop.is_set())


def make_default_watch(sinks: Iterable[ScrobbleSink]) -> WatchService:
    return WatchService(sinks=sinks)


_AUTO_WATCH: WatchService | None = None


def autostart_from_config() -> WatchService | None:
    global _AUTO_WATCH
    cfg = _cfg() or {}
    sc = (cfg.get("scrobble") or {})
    if not (sc.get("enabled") and str(sc.get("mode") or "").lower() == "watch"):
        return None
    if not ((sc.get("watch") or {}).get("autostart")):
        return None
    if _AUTO_WATCH and _AUTO_WATCH.is_alive():
        return _AUTO_WATCH
    try:
        from providers.scrobble.trakt.sink import TraktSink
        sinks: list[ScrobbleSink] = [TraktSink()]
    except Exception:
        return None
    _AUTO_WATCH = WatchService(sinks=sinks)
    _AUTO_WATCH.start_async()
    return _AUTO_WATCH



_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)

_LAST_RATING_BY_ACC: dict[tuple[str, str, str], dict[str, Any]] = {}


def _emit(logger: Callable[..., None] | None, msg: str, level: str = "INFO") -> None:
    if logger is None:
        return
    try:
        logger(msg, level)  # type: ignore[misc]
    except Exception:
        try:
            logger(msg, level=level)  # type: ignore[misc]
        except Exception:
            pass


def _verify_signature(raw: bytes | None, headers: Mapping[str, str], secret: str) -> bool:
    if not secret:
        return True
    raw2 = raw or b""
    sig = (headers.get("X-Plex-Signature") or headers.get("x-plex-signature") or "").strip()
    if not sig:
        return False
    digest = hmac.new(secret.encode("utf-8"), raw2, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("ascii")
    try:
        return hmac.compare_digest(sig.strip(), expected.strip())
    except Exception:
        return sig.strip() == expected.strip()


def _norm_user(s: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", (s or "").lower())


def _account_key(payload: dict[str, Any]) -> str:
    acc = payload.get("Account") or {}
    if isinstance(acc, dict):
        u = str(acc.get("uuid") or "").strip().lower()
        if u:
            return f"uuid:{u}"
        i = str(acc.get("id") or "").strip()
        if i:
            return f"id:{i}"
        t = str(acc.get("title") or "").strip()
        if t:
            return f"title:{_norm_user(t)}"
    return "unknown"


def _account_allowed(allow: Any, payload: dict[str, Any]) -> bool:
    if not allow:
        return True
    allow_list = allow if isinstance(allow, list) else [allow]
    acc = payload.get("Account") or {}
    title = str((acc.get("title") if isinstance(acc, dict) else "") or "")
    acc_id = str((acc.get("id") if isinstance(acc, dict) else "") or "")
    acc_uuid = str((acc.get("uuid") if isinstance(acc, dict) else "") or "").lower()

    for e in allow_list:
        s = str(e).strip()
        if not s:
            continue
        sl = s.lower()
        if sl.startswith("id:") and acc_id and sl.split(":", 1)[1].strip() == acc_id:
            return True
        if sl.startswith("uuid:") and acc_uuid and sl.split(":", 1)[1].strip() == acc_uuid:
            return True
        if not sl.startswith(("id:", "uuid:")) and _norm_user(s) == _norm_user(title):
            return True
    return False


def _server_allowed(want_uuid: str, payload: dict[str, Any]) -> bool:
    if not want_uuid:
        return True
    srv = payload.get("Server") or {}
    got = str((srv.get("uuid") if isinstance(srv, dict) else "") or "").strip()
    return (not got) or got == want_uuid


def _gather_guid_candidates(md: dict[str, Any]) -> list[str]:
    cand: list[str] = []
    for k in ("guid", "grandparentGuid", "parentGuid"):
        v = md.get(k)
        if v:
            cand.append(str(v))
    gi = md.get("Guid") or []
    for g in gi:
        if isinstance(g, dict):
            v = g.get("id")
            if v:
                cand.append(str(v))
        elif isinstance(g, str):
            cand.append(g)
    out: list[str] = []
    seen: set[str] = set()
    for v in cand:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out


def _all_ids_from_metadata(md: dict[str, Any]) -> dict[str, Any]:
    ids: dict[str, Any] = {}
    for s in _gather_guid_candidates(md):
        if not s:
            continue
        m = _PAT_IMDB.search(s)
        if m:
            ids.setdefault("imdb", m.group(1))
        m = _PAT_TMDB.search(s)
        if m:
            ids.setdefault("tmdb", int(m.group(1)))
        m = _PAT_TVDB.search(s)
        if m:
            ids.setdefault("tvdb", int(m.group(1)))
    return ids


def _sanitize_ids(ids: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    imdb = ids.get("imdb")
    if imdb:
        s = str(imdb).strip()
        if s:
            out["imdb"] = s
    for k in ("tmdb", "tvdb"):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit():
            out[k] = int(s)
    return out


def _plex_rating_to_10(v: Any) -> int | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        f = float(str(v).strip())
    except Exception:
        return None
    if f <= 0:
        return 0

    n = int(round(f))
    if n <= 0:
        n = 1
    return max(0, min(10, n))


def _ids_from_guids_simple(guids: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    try:
        for g in (guids or []):
            gid = str(getattr(g, "id", "") or "").lower()
            if not gid:
                continue
            if "imdb://" in gid:
                out.setdefault("imdb", gid.split("imdb://", 1)[1])
            elif "tmdb://" in gid:
                v = gid.split("tmdb://", 1)[1]
                if v.isdigit():
                    out.setdefault("tmdb", int(v))
            elif "thetvdb://" in gid or "tvdb://" in gid:
                v = gid.split("://", 1)[1]
                if v.isdigit():
                    out.setdefault("tvdb", int(v))
    except Exception:
        pass
    return out


def _library_allowed(cfg: dict[str, Any], payload: dict[str, Any]) -> bool:
    libs = _as_set_str((((cfg.get("plex") or {}).get("scrobble") or {}).get("libraries")))
    if not libs:
        return True
    md = payload.get("Metadata") or {}
    lib_id = None
    if isinstance(md, dict):
        lib_id = md.get("librarySectionID") or md.get("librarySectionId") or md.get("librarySectionKey")
    if lib_id is None:
        lib_id = payload.get("librarySectionID") or payload.get("LibrarySectionID")
    if lib_id is None:
        return False
    return str(lib_id).strip() in libs


def _trakt_post_with_refresh(path: str, body: dict[str, Any], cfg: dict[str, Any]) -> Any:
    from providers.scrobble.trakt import sink as trakt_sink
    r = trakt_sink._post(path, body, cfg)
    if r.status_code == 401 and trakt_sink._tok_refresh(cfg):
        r = trakt_sink._post(path, body, cfg)
    return r


def _trakt_send_rating(media_type: str, ids: dict[str, Any], rating: int, cfg: dict[str, Any], logger: Callable[..., None] | None) -> dict[str, Any]:
    bucket = "movies" if media_type == "movie" else ("shows" if media_type == "show" else "episodes")
    ids2 = _sanitize_ids(ids)
    if not ids2:
        return {"ok": False, "error": "no_ids"}
    if rating == 0:
        body = {bucket: [{"ids": ids2}]}
        r = _trakt_post_with_refresh("/sync/ratings/remove", body, cfg)
    else:
        body = {bucket: [{"ids": ids2, "rating": int(rating)}]}
        r = _trakt_post_with_refresh("/sync/ratings", body, cfg)
    try:
        j = r.json()
    except Exception:
        j = {"raw": (getattr(r, "text", "") or "")[:200]}
    if r.status_code >= 400:
        _emit(logger, f"trakt rating failed {r.status_code} {str(j)[:180]}", "ERROR")
        return {"ok": False, "status": r.status_code, "trakt": j}
    return {"ok": True, "status": r.status_code, "trakt": j}


def _simkl_send_rating(media_type: str, ids: dict[str, Any], rating: int, cfg: dict[str, Any], logger: Callable[..., None] | None) -> dict[str, Any]:
    from providers.scrobble.simkl import sink as simkl_sink
    bucket = "movies" if media_type == "movie" else "shows"
    ids2 = _sanitize_ids(ids)
    if not ids2:
        return {"ok": False, "error": "no_ids"}
    if rating == 0:
        body = {bucket: [{"ids": ids2}]}
        r = simkl_sink._post("/sync/ratings/remove", body, cfg)
    else:
        body = {bucket: [{"ids": ids2, "rating": int(rating)}]}
        r = simkl_sink._post("/sync/ratings", body, cfg)
    try:
        j = r.json()
    except Exception:
        j = {"raw": (getattr(r, "text", "") or "")[:200]}
    if r.status_code >= 400:
        _emit(logger, f"simkl rating failed {r.status_code} {str(j)[:180]}", "ERROR")
        return {"ok": False, "status": r.status_code, "simkl": j}
    return {"ok": True, "status": r.status_code, "simkl": j}



def _mdblist_send_rating(media_type: str, ids: dict[str, Any], rating: int, cfg: dict[str, Any], logger: Callable[..., None] | None) -> dict[str, Any]:
    md_cfg = (cfg.get("mdblist") or {}) if isinstance(cfg, dict) else {}
    apikey = str(md_cfg.get("api_key") or "").strip()
    if not apikey:
        return {"ok": False, "error": "missing_api_key"}
    if media_type not in ("movie", "show"):
        return {"ok": True, "ignored": True}
    bucket = "movies" if media_type == "movie" else "shows"
    ids2 = _sanitize_ids(ids) or {}
    ids3: dict[str, Any] = {}
    for k in ("imdb", "tmdb", "trakt", "tvdb", "kitsu"):
        if ids2.get(k):
            ids3[k] = ids2[k]
    if not ids3:
        return {"ok": False, "error": "no_ids"}

    base = "https://api.mdblist.com"
    is_remove = int(rating or 0) <= 0
    url = f"{base}/sync/ratings/remove" if is_remove else f"{base}/sync/ratings"

    item: dict[str, Any] = {"ids": ids3}
    if not is_remove:
        item["rating"] = int(rating)
    body: dict[str, Any] = {bucket: [item]}

    tmo = float(md_cfg.get("timeout") or 10)
    max_retries = int(md_cfg.get("max_retries") or 3)
    max_backoff = int(md_cfg.get("ratings_max_backoff_ms") or 8000) / 1000.0
    backoff = 0.5

    r: requests.Response | None = None
    for attempt in range(max_retries):
        try:
            r = requests.post(url, params={"apikey": apikey}, json=body, timeout=tmo)
        except Exception as e:
            if attempt >= max_retries - 1:
                _emit(logger, f"mdblist rating request failed: {type(e).__name__}: {e}", "ERROR")
                return {"ok": False, "error": "request_failed"}
            time.sleep(backoff)
            backoff = min(backoff * 2.0, max_backoff)
            continue

        if r.status_code in (429, 500, 502, 503, 504) and attempt < max_retries - 1:
            ra = r.headers.get("Retry-After")
            if ra:
                try:
                    time.sleep(float(ra))
                except Exception:
                    time.sleep(backoff)
            else:
                time.sleep(backoff)
            backoff = min(backoff * 2.0, max_backoff)
            continue
        break

    if r is None:
        return {"ok": False, "error": "request_failed"}

    try:
        j = r.json() if getattr(r, "content", b"") else {}
    except Exception:
        j = {"raw": (getattr(r, "text", "") or "")[:200]}

    if r.status_code >= 400:
        _emit(logger, f"mdblist rating failed {r.status_code} {str(j)[:180]}", "ERROR")
        return {"ok": False, "status": r.status_code, "resp": j}
    return {"ok": True, "status": r.status_code, "resp": j}


def process_rating_webhook(
    payload: dict[str, Any],
    headers: Mapping[str, str],
    raw: bytes | None = None,
    logger: Callable[..., None] | None = None,
) -> dict[str, Any]:
    cfg = _cfg() or {}
    sc = (cfg.get("scrobble") or {})
    if not bool(sc.get("enabled")) or str(sc.get("mode") or "").lower() != "watch":
        return {"ok": True, "ignored": True}

    watch_cfg = (sc.get("watch") or {})
    if str(watch_cfg.get("provider", "plex")).lower().strip() != "plex":
        return {"ok": True, "ignored": True}

    enable_trakt = bool(watch_cfg.get("plex_trakt_ratings"))
    enable_simkl = bool(watch_cfg.get("plex_simkl_ratings"))
    enable_mdblist = bool(watch_cfg.get("plex_mdblist_ratings"))
    if not (enable_trakt or enable_simkl or enable_mdblist):
        return {"ok": True, "ignored": True}

    secret = str(((cfg.get("plex") or {}).get("webhook_secret") or "")).strip()
    if secret and not _verify_signature(raw, dict(headers), secret):
        _emit(logger, "invalid X-Plex-Signature", "WARN")
        return {"ok": True, "ignored": True, "invalid_signature": True}

    if not payload:
        return {"ok": True, "ignored": True}

    if str(payload.get("event") or "") != "media.rate":
        return {"ok": True, "ignored": True}

    filt = (watch_cfg.get("filters") or {})
    wl = filt.get("username_whitelist")
    want_uuid = str((filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid") or "")).strip()

    if want_uuid and not _server_allowed(want_uuid, payload):
        return {"ok": True, "ignored": True}

    if not _account_allowed(wl, payload):
        return {"ok": True, "ignored": True}

    if not _library_allowed(cfg, payload):
        return {"ok": True, "ignored": True}

    md = payload.get("Metadata") or {}
    if not isinstance(md, dict):
        return {"ok": True, "ignored": True}

    media_type = str(md.get("type") or "").lower().strip()
    if media_type not in ("movie", "show", "episode"):
        return {"ok": True, "ignored": True}

    rating_raw = md.get("userRating") if "userRating" in md else None
    if rating_raw is None:
        rating_raw = md.get("user_rating") or payload.get("userRating") or payload.get("user_rating")
    rating_val = _plex_rating_to_10(rating_raw) if rating_raw is not None else 0
    if rating_val is None:
        rating_val = 0


    if media_type == "episode" and not enable_trakt:
        return {"ok": True, "ignored": True}

    acc_key = _account_key(payload)
    rk = str(md.get("ratingKey") or md.get("ratingkey") or "").strip()
    dedup_key = (acc_key, rk or "?", media_type)
    prev = _LAST_RATING_BY_ACC.get(dedup_key) or {}
    if prev and prev.get("rating") == rating_val and (time.time() - float(prev.get("ts", 0))) < 10:
        return {"ok": True, "dedup": True}
    _LAST_RATING_BY_ACC[dedup_key] = {"rating": rating_val, "ts": time.time()}

    ids = _all_ids_from_metadata(md)

    if (not ids) and rk:
        try:
            base, token = _plex_btok(cfg)
            if token:
                px = PlexServer(base, token)
                it = px.fetchItem(int(rk))
                if it is not None:
                    ids.update(_ids_from_guids_simple(getattr(it, "guids", [])))
                    if media_type == "episode":
                        try:
                            show = it.show()
                            ids.update(_ids_from_guids_simple(getattr(show, "guids", [])))
                        except Exception:
                            pass
        except Exception:
            pass

    if not _sanitize_ids(ids):
        _emit(logger, "rating event without usable external IDs; ignored", "DEBUG")
        return {"ok": True, "ignored": True}

    results: dict[str, Any] = {"ok": True, "action": "rating", "media_type": media_type, "rating": rating_val}
    if enable_trakt:
        results["trakt"] = _trakt_send_rating(media_type, ids, rating_val, cfg, logger)
    if enable_simkl and media_type in ("movie", "show"):
        results["simkl"] = _simkl_send_rating(media_type, ids, rating_val, cfg, logger)
    if enable_mdblist and media_type in ("movie", "show"):
        results["mdblist"] = _mdblist_send_rating(media_type, ids, rating_val, cfg, logger)
    return results

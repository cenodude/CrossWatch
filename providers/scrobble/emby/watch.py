# providers/scrobble/emby/watch.py
from __future__ import annotations

import json, time, threading, requests, re
from pathlib import Path
from typing import Any, Iterable, Dict, Tuple, Set

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import Dispatcher, ScrobbleSink, ScrobbleEvent

def _cfg() -> dict[str, Any]:
    p = Path("/config/config.json")
    try:
        return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
    except Exception:
        return {}

def _is_debug() -> bool:
    try:
        return bool(((_cfg().get("runtime") or {}).get("debug")))
    except Exception:
        return False

def _emby_bt(cfg: dict[str, Any]) -> tuple[str, str]:
    e = cfg.get("emby") or {}
    base = str(e.get("server", "")).strip().rstrip("/")
    tok = str(e.get("access_token", "")).strip()
    if not base or not tok:
        raise ValueError("Missing emby.server or emby.access_token in config.json")
    if "://" not in base:
        base = "http://" + base
    return base, tok

def _hdr(tok: str, cfg: dict[str, Any]) -> dict[str, str]:
    e = cfg.get("emby") or {}
    did = str(e.get("device_id") or "crosswatch")
    return {
        "Accept": "application/json",
        "X-Emby-Token": tok,
        "X-MediaBrowser-Token": tok,
        "Authorization": f'Emby Client="CrossWatch", Device="CrossWatch", DeviceId="{did}", Version="1.0.0"',
    }

def _get_json(base: str, tok: str, path: str) -> Any:
    cfg = _cfg()
    e = cfg.get("emby") or {}
    r = requests.get(
        f"{base}{path}",
        headers=_hdr(tok, cfg),
        timeout=float(e.get("timeout", 6)),
        verify=bool(e.get("verify_ssl", True)),
    )
    r.raise_for_status()
    return r.json()

def _ticks_to_pct(pos_ticks: Any, dur_ticks: Any) -> int:
    try:
        p = max(0, int(pos_ticks or 0))
        d = max(1, int(dur_ticks or 0))
        return max(0, min(100, int(round((p / float(d)) * 100))))
    except Exception:
        return 0

def _map_provider_ids(item: dict[str, Any]) -> dict[str, Any]:
    ids = {}
    prov = item.get("ProviderIds") or {}
    def put(k, v):
        if v:
            ids[k] = str(v)
    put("imdb", prov.get("Imdb"))
    put("tmdb", prov.get("Tmdb") or prov.get("TmdbId"))
    put("tvdb", prov.get("Tvdb") or prov.get("TvdbId"))
    sprov = item.get("SeriesProviderIds") or {}
    if sprov:
        if sprov.get("Imdb"):
            ids["imdb_show"] = str(sprov.get("Imdb"))
        if sprov.get("Tmdb") or sprov.get("TmdbId"):
            ids["tmdb_show"] = str(sprov.get("Tmdb") or sprov.get("TmdbId"))
        if sprov.get("Tvdb") or sprov.get("TvdbId"):
            ids["tvdb_show"] = str(sprov.get("Tvdb") or sprov.get("TvdbId"))
    return ids

def _media_from_session(sess: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    item = (sess.get("NowPlayingItem") or {})
    ps = (sess.get("PlayState") or {})
    return (item if item else None), ps

def _server_id(base: str, tok: str) -> str | None:
    try:
        info = _get_json(base, tok, "/System/Info/Public")
        return str(info.get("Id") or "") or None
    except Exception:
        return None

def _cfg_for_dispatch(server_id: str | None) -> dict[str, Any]:
    cfg = _cfg()
    if server_id:
        px = dict((cfg.get("plex") or {}))
        px["server_uuid"] = server_id
        cfg = dict(cfg)
        cfg["plex"] = px
    return cfg

class EmbyWatchService:
    def __init__(self, sinks: Iterable[ScrobbleSink] | None = None, poll_secs: float = 0.7) -> None:
        self._base, self._tok = _emby_bt(_cfg())
        self._server_id = _server_id(self._base, self._tok)
        self._dispatch = Dispatcher(list(sinks or []), cfg_provider=lambda: _cfg_for_dispatch(self._server_id))
        self._poll = max(0.3, float(poll_secs))
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None
        self._last: Dict[str, Dict[str, Any]] = {}
        self._last_emit: Dict[str, Tuple[str, str]] = {}
        self._allowed_sessions: Set[str] = set()
        self._dbg_last_total = None
        self._dbg_last_playing = None
        self._dbg_last_ts = 0.0

    def _log(self, msg: str, level: str = "INFO") -> None:
        if BASE_LOG:
            try:
                BASE_LOG(str(msg), level=level.upper(), module="EMBYWATCH")
                return
            except Exception:
                pass
        print(f"{level} [EMBYWATCH] {msg}")

    def _dbg(self, msg: str) -> None:
        return

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        if ev.session_key and ev.session_key in self._allowed_sessions:
            return True
        cfg = _cfg() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want = (filt.get("server_uuid") or self._server_id or None)
        if want and ev.server_uuid and str(ev.server_uuid) != str(want):
            return False
        def _allow() -> bool:
            if ev.session_key:
                self._allowed_sessions.add(str(ev.session_key))
            return True
        if not wl:
            return _allow()
        def norm(s: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", (s or "").lower())
        wl_list = wl if isinstance(wl, list) else [wl]
        if any(not str(x).lower().startswith(("id:", "uuid:")) and norm(str(x)) == norm(ev.account or "") for x in wl_list):
            return _allow()
        raw = ev.raw or {}
        uid = str(raw.get("UserId") or "").strip().lower()
        for e in wl_list:
            s = str(e).strip().lower()
            if s.startswith("id:") and uid and s.split(":", 1)[1].strip().lower() == uid:
                return _allow()
            if s.startswith("uuid:") and uid and s.split(":", 1)[1].strip().lower() == uid:
                return _allow()
        return False

    def _build_event(self, sess: dict[str, Any], action: str, progress: int) -> ScrobbleEvent | None:
        item, _ps = _media_from_session(sess)
        if not item:
            return None
        mtype = "episode" if (item.get("Type") or "").lower() == "episode" else "movie"
        ids = _map_provider_ids(item)
        title = item.get("SeriesName") if mtype == "episode" else item.get("Name") or item.get("OriginalTitle")
        year = item.get("ProductionYear")
        season = item.get("ParentIndexNumber") if mtype == "episode" else None
        number = item.get("IndexNumber") if mtype == "episode" else None
        return ScrobbleEvent(
            action=("start" if action == "playing" else "pause" if action == "paused" else "stop"),
            media_type=("episode" if mtype == "episode" else "movie"),
            ids=ids,
            title=title,
            year=year,
            season=season,
            number=number,
            progress=progress,
            account=(sess.get("UserName") or sess.get("UserId") or None),
            server_uuid=self._server_id,
            session_key=str(sess.get("Id") or ""),
            raw=sess,
        )

    def _current_sessions(self) -> list[dict[str, Any]]:
        try:
            e = (_cfg().get("emby") or {})
            uid = str(e.get("user_id") or "").strip().lower()
            q = "/Sessions?ActiveWithinSeconds=15"
            all_sessions = _get_json(self._base, self._tok, q) or []
            playing = []
            for s in all_sessions:
                if not (s.get("NowPlayingItem") or {}):
                    continue
                if uid and str(s.get("UserId") or "").strip().lower() != uid:
                    continue
                playing.append(s)
            return playing
        except Exception as ex:
            self._log(f"session poll failed: {ex}", "ERROR")
            return []

    def _meta_from_event(self, ev: ScrobbleEvent) -> Dict[str, Any]:
        return {
            "media_type": ev.media_type,
            "ids": dict(ev.ids or {}),
            "title": ev.title,
            "year": ev.year,
            "season": ev.season,
            "number": ev.number,
            "account": ev.account,
        }

    def _emit(self, ev: ScrobbleEvent) -> None:
        if not self._passes_filters(ev):
            return
        sk = str(ev.session_key or "")
        self._log(f"event {ev.action} {ev.media_type} user={ev.account} p={ev.progress} sess={sk}")
        self._dispatch.dispatch(ev)
        if sk:
            last = self._last.get(sk) or {}
            last["meta"] = self._meta_from_event(ev)
            self._last[sk] = last
            self._last_emit[sk] = (ev.action, str(last.get("key") or ""))

    def _tick(self) -> None:
        now = time.time()
        cur = self._current_sessions()
        seen: Set[str] = set()
        try:
            debounce = float((((_cfg().get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds") or 0))
        except Exception:
            debounce = 0.0
        try:
            suppress_start_at = int((((_cfg().get("scrobble") or {}).get("watch") or {}).get("suppress_start_at") or 0))
        except Exception:
            suppress_start_at = 0
        try:
            force_at = int((((_cfg().get("scrobble") or {}).get("trakt") or {}).get("force_stop_at") or 95))
        except Exception:
            force_at = 95

        for s in cur:
            sid = str(s.get("Id") or "")
            if not sid:
                continue
            item, ps = _media_from_session(s)
            if not item:
                continue
            dur = item.get("RunTimeTicks") or 0
            pos = ps.get("PositionTicks") if isinstance(ps, dict) else None
            state = "paused" if (ps.get("IsPaused") if isinstance(ps, dict) else False) else "playing"
            p = _ticks_to_pct(pos or 0, dur or 0)
            key = f"{item.get('Id') or item.get('InternalId') or item.get('Name')}-{mhash(dur)}"
            last = self._last.get(sid) or {}
            last_key = last.get("key")
            last_state = last.get("state")
            state_ts = float(last.get("state_ts") or 0.0)
            emit_action: str | None = None

            if key != last_key:
                emit_action = "start"
            elif last_state != state:
                if state == "playing":
                    emit_action = "start"
                else:
                    if (now - (state_ts or now)) >= debounce:
                        emit_action = "pause"

            if emit_action == "start" and p < 1:
                p = 1
            if emit_action == "start" and last_key != key and suppress_start_at and p >= suppress_start_at:
                emit_action = None

            if emit_action:
                ev = self._build_event(s, "playing" if emit_action == "start" else "paused", p)
                if ev:
                    last_em = self._last_emit.get(sid)
                    if not (last_em and last_em[0] == ev.action and last_em[1] == key):
                        self._emit(ev)
                        state_ts = now

            self._last[sid] = {
                "key": key,
                "state": state,
                "p": p,
                "ts": now,
                "state_ts": state_ts or now,
                "meta": (self._last.get(sid) or {}).get("meta"),
            }
            seen.add(sid)

        for sid, memo in list(self._last.items()):
            if sid in seen:
                continue
            last_p = int(memo.get("p") or 0)
            dt = now - float(memo.get("ts", 0))
            if last_p < 1:
                last_p = 1

            meta = memo.get("meta") or {}
            if not meta:
                del self._last[sid]
                continue  # no metadata → skip stop

            if last_p >= force_at or dt >= 2.0:
                fake = {"Id": sid, "UserName": meta.get("account"), "NowPlayingItem": {}, "PlayState": {}}
                ev = ScrobbleEvent(
                    action="stop",
                    media_type=str(meta.get("media_type") or "movie"),
                    ids=dict(meta.get("ids") or {}),
                    title=meta.get("title"),
                    year=meta.get("year"),
                    season=meta.get("season"),
                    number=meta.get("number"),
                    progress=last_p,
                    account=meta.get("account"),
                    server_uuid=self._server_id,
                    session_key=sid,
                    raw=fake,
                )
                last_em = self._last_emit.get(sid)
                if not (last_em and last_em[0] == "stop"):
                    self._emit(ev)
                del self._last[sid]

    def start(self) -> None:
        self._stop.clear()
        self._log(f"Emby watcher starting → {self._base}")
        while not self._stop.is_set():
            self._tick()
            time.sleep(self._poll)

    def stop(self) -> None:
        self._stop.set()
        self._log("Emby watcher stopping")

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._bg = threading.Thread(target=self.start, name="EmbyWatch", daemon=True)
        self._bg.start()

    def is_alive(self) -> bool:
        return bool(self._bg and self._bg.is_alive())

def mhash(x: Any) -> int:
    try:
        return abs(hash(int(x)))
    except Exception:
        return abs(hash(str(x)))

def make_default_watch(sinks: Iterable[ScrobbleSink]) -> EmbyWatchService:
    return EmbyWatchService(sinks=sinks)

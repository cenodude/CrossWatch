# providers/scrobble/scrobble.py
# CrossWatch - Generic scrobbling module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import inspect
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Protocol

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


def _log(msg: str, lvl: str = "INFO") -> None:
    if BASE_LOG:
        try:
            BASE_LOG(str(msg), level=lvl, module="SCROBBLE")
            return
        except Exception:
            pass
    print(f"[SCROBBLE:{(lvl or 'INFO').upper()}] {msg}")


def _load_config() -> dict[str, Any]:
    try:
        from cw_platform.config_base import load_config as _load_cfg
        return _load_cfg()
    except Exception:
        p = Path("/config/config.json")
        try:
            return json.loads(p.read_text(encoding="utf-8")) if p.exists() else {}
        except Exception:
            return {}


def _i(x: Any) -> int | None:
    try:
        return int(x)
    except Exception:
        return None


_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)


def _grab(s: str, pat: re.Pattern[str]) -> str | None:
    m = pat.search(s or "")
    return m.group(1) if m else None


def _ids_from_meta(meta: dict[str, Any]) -> dict[str, str]:
    guid = str(meta.get("guid") or "")
    ids: dict[str, str] = {}
    for k, pat in (("imdb", _PAT_IMDB), ("tmdb", _PAT_TMDB), ("tvdb", _PAT_TVDB)):
        v = _grab(guid, pat)
        if v:
            ids[k] = v
    gpg = str(meta.get("grandparentGuid") or "")
    if gpg:
        for k, pat in (("imdb_show", _PAT_IMDB), ("tmdb_show", _PAT_TMDB), ("tvdb_show", _PAT_TVDB)):
            v = _grab(gpg, pat)
            if v:
                ids[k] = v
    return ids


def _normalize_units(offset: int, duration: int) -> tuple[int, int]:
    # Plex notifications sometimes mix seconds/milliseconds depending on the shape.
    o = int(offset or 0)
    d = int(duration or 0)
    if d <= 0:
        return o, d
    if d < 10_000 and o > 10_000:
        return o, d * 1000
    if d > 10_000 and 0 < o < 10_000:
        return o * 1000, d
    return o, d


def _progress(state: str, view_offset: int, duration: int) -> tuple[int, ScrobbleAction]:
    s = (state or "").lower()
    if s == "playing":
        act: ScrobbleAction = "start"
    elif s == "paused":
        act = "pause"
    elif s in ("stopped", "bufferingstopped"):
        act = "stop"
    else:
        act = "start"
    d0 = _i(duration) or 0
    o0 = _i(view_offset) or 0
    if d0 <= 0:
        return 0, act
    o, d = _normalize_units(o0, d0)
    vo = max(0, min(o, d))
    pct = max(0, min(100, int(round((vo / float(d)) * 100))))
    return pct, act


def _event_from_meta(meta: dict[str, Any], raw: dict[str, Any]) -> ScrobbleEvent:
    ids = _ids_from_meta(meta)
    pct, act = _progress(meta.get("state", ""), meta.get("viewOffset", 0) or 0, meta.get("duration", 0) or 0)
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
        progress=pct,
        account=(meta.get("account") and str(meta["account"])) or None,
        server_uuid=(meta.get("machineIdentifier") and str(meta["machineIdentifier"])) or None,
        session_key=(meta.get("sessionKey") and str(meta["sessionKey"])) or None,
        raw=raw,
    )


ScrobbleAction = Literal["start", "pause", "stop"]
MediaType = Literal["movie", "episode"]


@dataclass(frozen=True)
class ScrobbleEvent:
    action: ScrobbleAction
    media_type: MediaType
    ids: dict[str, str]
    title: str | None
    year: int | None
    season: int | None
    number: int | None
    progress: int
    account: str | None
    server_uuid: str | None
    session_key: str | None
    raw: dict[str, Any]


class ScrobbleSink(Protocol):
    def send(self, event: ScrobbleEvent) -> None: ...


def from_plex_webhook(payload: Any, defaults: dict[str, Any] | None = None) -> ScrobbleEvent | None:
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
    if isinstance(obj.get("PlaySessionStateNotification"), (list, dict)):
        return from_plex_pssn(obj, defaults)
    return None


def from_plex_pssn(payload: dict[str, Any], defaults: dict[str, Any] | None = None) -> ScrobbleEvent | None:
    defaults = defaults or {}
    raw = payload.get("PlaySessionStateNotification")
    if isinstance(raw, dict):
        items = [raw]
    elif isinstance(raw, list):
        items = [x for x in raw if isinstance(x, dict)]
    else:
        return None
    if not items:
        return None

    def score(d: dict[str, Any]) -> int:
        s = 0
        if d.get("sessionKey") is not None:
            s += 5
        if d.get("ratingKey") is not None:
            s += 4
        if d.get("guid"):
            s += 3
        if d.get("state"):
            s += 2
        if d.get("viewOffset") is not None or d.get("view_offset") is not None:
            s += 2
        if d.get("duration") is not None:
            s += 1
        return s

    n = max(items, key=score)
    meta = {
        "guid": n.get("guid"),
        "grandparentGuid": n.get("grandparentGuid"),
        "title": n.get("title"),
        "grandparentTitle": n.get("grandparentTitle"),
        "year": _i(n.get("year")),
        "index": _i(n.get("index")),
        "grandparentIndex": _i(n.get("grandparentIndex")),
        "duration": _i(n.get("duration") or 0) or 0,
        "viewOffset": _i(n.get("viewOffset") or n.get("view_offset") or 0) or 0,
        "type": n.get("type") or "",
        "state": n.get("state") or "",
        "sessionKey": n.get("sessionKey"),
        "account": n.get("account") or n.get("accountID") or defaults.get("username"),
        "machineIdentifier": n.get("machineIdentifier") or defaults.get("server_uuid"),
    }
    return _event_from_meta(meta, payload)


def from_plex_flat_playing(payload: dict[str, Any], defaults: dict[str, Any] | None = None) -> ScrobbleEvent | None:
    defaults = defaults or {}
    if int(payload.get("size") or 0) < 1:
        return None
    if (payload.get("_type") or payload.get("type") or "").lower() != "playing":
        return None

    def _find_timeline(o: Any) -> dict[str, Any] | None:
        if isinstance(o, dict):
            for k, v in o.items():
                if isinstance(k, str) and k.lower() == "timelineentry":
                    if isinstance(v, dict):
                        return v
                    if isinstance(v, list):
                        return next((x for x in v if isinstance(x, dict)), None)
                r = _find_timeline(v)
                if r:
                    return r
        elif isinstance(o, list):
            for v in o:
                r = _find_timeline(v)
                if r:
                    return r
        return None

    def _best_meta_dict(o: Any) -> dict[str, Any] | None:
        best: tuple[int, dict[str, Any]] | None = None

        def score(d: dict[str, Any]) -> int:
            s = 0
            if d.get("guid"):
                s += 5
            if d.get("ratingKey"):
                s += 3
            if d.get("title"):
                s += 2
            if d.get("grandparentTitle"):
                s += 2
            if d.get("type"):
                s += 1
            if d.get("duration") or d.get("viewOffset") or d.get("time"):
                s += 1
            return s

        def walk(x: Any) -> None:
            nonlocal best
            if isinstance(x, dict):
                if "guid" in x or "ratingKey" in x or "title" in x:
                    sc = score(x)
                    if sc > 0 and (best is None or sc > best[0]):
                        best = (sc, x)
                for v in x.values():
                    walk(v)
            elif isinstance(x, list):
                for v in x:
                    walk(v)

        walk(o)
        return best[1] if best else None

    tl = _find_timeline(payload)
    meta_src = _best_meta_dict(payload)
    if not tl and not meta_src:
        return None

    first = dict(meta_src or tl or {})
    prog_src = dict(tl or first)
    vo = prog_src.get("viewOffset")
    if vo is None:
        vo = prog_src.get("time")
    dur = prog_src.get("duration")
    meta = {
        "guid": first.get("guid") or prog_src.get("guid"),
        "grandparentGuid": first.get("grandparentGuid") or prog_src.get("grandparentGuid"),
        "title": first.get("title") or prog_src.get("title"),
        "grandparentTitle": first.get("grandparentTitle") or prog_src.get("grandparentTitle"),
        "year": _i(first.get("year")),
        "index": _i(first.get("index")),
        "grandparentIndex": _i(first.get("grandparentIndex")),
        "duration": _i(dur or first.get("duration") or 0) or 0,
        "viewOffset": _i(vo or first.get("viewOffset") or 0) or 0,
        "type": first.get("type") or prog_src.get("type") or "",
        "state": prog_src.get("state") or first.get("state") or "",
        "sessionKey": first.get("sessionKey") or prog_src.get("sessionKey"),
        "account": first.get("account") or prog_src.get("account") or defaults.get("username"),
        "machineIdentifier": first.get("machineIdentifier") or prog_src.get("machineIdentifier") or defaults.get("server_uuid"),
    }
    return _event_from_meta(meta, payload)


class Dispatcher:
    def __init__(self, sinks: Iterable[ScrobbleSink], cfg_provider=None) -> None:
        self._sinks = list(sinks or [])
        self._cfg_provider = cfg_provider or _load_config
        self._session_ok: set[str] = set()
        self._debounce: dict[str, float] = {}
        self._last_action: dict[str, str] = {}
        self._last_progress: dict[str, int] = {}
        self._sink_accepts_cfg: dict[int, bool] = {}

    def _send_sink(self, sink: Any, ev: ScrobbleEvent, cfg: dict[str, Any]) -> None:
        sid = id(sink)
        ok = self._sink_accepts_cfg.get(sid)
        if ok is None:
            ok = False
            try:
                sig = inspect.signature(getattr(sink, "send"))
                params = list(sig.parameters.values())
                ok = any(p.kind == p.VAR_KEYWORD for p in params) or ("cfg" in sig.parameters)
            except Exception:
                ok = False
            self._sink_accepts_cfg[sid] = ok
        if not ok:
            sink.send(ev)
            return
        try:
            sink.send(ev, cfg=cfg)
        except TypeError:
            sink.send(ev, cfg)

    def _passes_filters(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> bool:
        if ev.session_key and str(ev.session_key) in self._session_ok:
            return True
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        want_server = (filt.get("server_uuid") or (cfg.get("plex") or {}).get("server_uuid"))
        if want_server and ev.server_uuid and str(ev.server_uuid) != str(want_server):
            return False

        def _allow() -> bool:
            if ev.session_key:
                self._session_ok.add(str(ev.session_key))
            return True

        if not wl:
            return _allow()

        def norm(s: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

        wl_list = wl if isinstance(wl, list) else [wl]

        if any(
            not str(x).lower().startswith(("id:", "uuid:"))
            and norm(str(x)) == norm(ev.account or "")
            for x in wl_list
        ):
            return _allow()

        def find_user_id(o: Any) -> str:
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(k, str) and k.lower() in ("userid", "user_id"):
                        return str(v or "").strip().lower()
                for v in o.values():
                    uid = find_user_id(v)
                    if uid:
                        return uid
            elif isinstance(o, list):
                for v in o:
                    uid = find_user_id(v)
                    if uid:
                        return uid
            return ""

        def find_psn(o: Any) -> list[dict[str, Any]] | None:
            if isinstance(o, dict):
                for k, v in o.items():
                    if isinstance(k, str) and k.lower() == "playsessionstatenotification":
                        return v if isinstance(v, list) else [v]
                for v in o.values():
                    r = find_psn(v)
                    if r:
                        return r
            elif isinstance(o, list):
                for v in o:
                    r = find_psn(v)
                    if r:
                        return r
            return None

        n = (find_psn(ev.raw or {}) or [None])[0] or {}
        acc_id = str(n.get("accountID") or "")
        acc_uuid = str(n.get("accountUUID") or "").lower()
        user_id = find_user_id(ev.raw or {})

        for e in wl_list:
            s = str(e).strip().lower()
            if s.startswith("id:") and acc_id and s.split(":", 1)[1].strip() == acc_id:
                return _allow()
            if s.startswith("uuid:") and acc_uuid and s.split(":", 1)[1].strip() == acc_uuid:
                return _allow()
            if s.startswith("id:") and user_id and s.split(":", 1)[1].strip().lower() == user_id:
                return _allow()
            if s.startswith("uuid:") and user_id and s.split(":", 1)[1].strip().lower() == user_id:
                return _allow()
        return False

    def _should_send(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> bool:
        sk = ev.session_key or "?"
        last_a = self._last_action.get(sk)
        last_p = self._last_progress.get(sk, -1)
        try:
            sup = int(((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99))
            pause_db = float(((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5))
        except Exception:
            sup, pause_db = 99, 5.0

        if ev.action == "start" and last_p is not None and last_p >= sup and ev.progress >= sup:
            return False

        changed = (ev.action != last_a) or (abs(ev.progress - (last_p or -1)) >= 1)

        if ev.action == "pause":
            now = time.time()
            k = f"{sk}|pause"
            if now - self._debounce.get(k, 0.0) < pause_db and ev.action == last_a:
                return False
            self._debounce[k] = now

        if changed:
            self._last_action[sk] = ev.action
            self._last_progress[sk] = ev.progress
            return True
        return False

    def dispatch(self, ev: ScrobbleEvent) -> None:
        cfg = self._cfg_provider() or {}
        if not self._passes_filters(ev, cfg):
            return
        if not self._should_send(ev, cfg):
            return
        for s in self._sinks:
            try:
                self._send_sink(s, ev, cfg)
            except Exception as e:
                _log(f"Sink error: {e}", "ERROR")


__all__ = (
    "ScrobbleEvent",
    "ScrobbleSink",
    "Dispatcher",
    "from_plex_webhook",
    "from_plex_pssn",
    "from_plex_flat_playing",
)

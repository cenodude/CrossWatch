# providers/scrobble/scrobble.py
# CrossWatch - Generic scrobbling module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import inspect
import re
import time
import threading
import hashlib
from collections import OrderedDict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Literal, Protocol

from cw_platform.account_match import media_account_allowed, normalize_media_account_name
from providers.scrobble.anime_mapping import maybe_enrich_event_for_sink, sink_name_for_mapping
from providers.scrobble.media_filters import event_ignore_reason, log_media_filter_drop

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


def _norm_user(s: str) -> str:
    return normalize_media_account_name(s)


def _as_filter_set(value: Any) -> set[str]:
    if value is None:
        return set()
    items = list(value) if isinstance(value, (list, tuple, set)) else [value]
    out: set[str] = set()
    for item in items:
        if item is None:
            continue
        for part in re.split(r"[\s,]+", str(item)):
            clean = part.strip()
            if clean:
                out.add(clean)
    return out


def _server_uuid_allowed(filt: dict[str, Any], server_uuid: str | None) -> bool:
    got = str(server_uuid or "").strip()
    allow = _as_filter_set(filt.get("server_uuid_whitelist"))
    legacy = str(filt.get("server_uuid") or "").strip()
    if legacy:
        allow.add(legacy)
    block = _as_filter_set(filt.get("server_uuid_blacklist"))
    if got and got in block:
        return False
    if allow and (not got or got not in allow):
        return False
    return True


def mask_account(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return "unknown"
    if len(s) <= 2:
        return s[0] + "*"
    return s[:2] + "***"


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


def _progress(state: str, view_offset: int, duration: int) -> tuple[float, ScrobbleAction]:
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
    pct = max(0.0, min(100.0, (vo / float(d)) * 100.0))
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
    progress: float
    account: str | None
    server_uuid: str | None
    session_key: str | None
    raw: dict[str, Any]
    position_ms: int | None = None
    duration_ms: int | None = None


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
        "account": n.get("account"),
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
        "account": first.get("account") or prog_src.get("account"),
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
        self._last_progress: dict[str, float] = {}
        self._sink_accepts_cfg: dict[int, bool] = {}
        self._route_log_ts: dict[str, float] = {}
        self._retry_after: dict[str, tuple[float, int]] = {}
        self._dispatch_lock = threading.RLock()
        self._pending: OrderedDict[str, tuple[float, ScrobbleEvent]] = OrderedDict()
        self._config_identity = ""
        self._inflight: set[int] = set()
        self._failed_ids: OrderedDict[str, dict[str, str]] = OrderedDict()

    def _send_sink(self, sink: Any, ev: ScrobbleEvent, cfg: dict[str, Any]) -> Any:
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
            return sink.send(ev)
        try:
            return sink.send(ev, cfg=cfg)
        except TypeError:
            return sink.send(ev, cfg)

    def _route_label(self, cfg: dict[str, Any]) -> str:
        try:
            w = ((cfg.get("scrobble") or {}).get("watch") or {})
            rid = str(w.get("route_id") or "").strip()

            def _side(name_key: str, inst_key: str) -> str:
                name = str(w.get(name_key) or "").strip()
                inst = str(w.get(inst_key) or "").strip()
                if not name or not inst or inst.lower() == "default":
                    return name
                raw = cfg.get(name)
                block = raw if isinstance(raw, dict) else {}
                shown = str(block.get("label") or "").strip() or inst
                return f"{name}[{shown}]"

            prov = _side("route_provider", "route_provider_instance")
            sink = _side("route_sink", "route_sink_instance")
            pair = f"{prov}->{sink}" if prov and sink else (prov or sink)
            if rid and pair:
                return f"{rid} {pair}"
            return rid or pair or "?"
        except Exception:
            return "?"

    def _throttled_route_log(self, key: str, msg: str, lvl: str = "DEBUG") -> None:
        now = time.time()
        if now - self._route_log_ts.get(key, 0.0) >= 30.0:
            self._route_log_ts[key] = now
            _log(msg, lvl)

    def _fallback_account(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> str:
        if str(ev.account or "").strip():
            return ""
        try:
            watch_cfg = ((cfg.get("scrobble") or {}).get("watch") or {})
            route_options = watch_cfg.get("route_options") or {}
            watch_options = route_options.get("watch") if isinstance(route_options, dict) else {}
            if not (isinstance(watch_options, dict) and bool(watch_options.get("unresolved_user_fallback"))):
                return ""
        except Exception:
            return ""
        raw = ev.raw if isinstance(ev.raw, dict) else {}
        if not raw.get("_cw_sessions_access_unavailable"):
            return ""
        fallback = raw.get("_cw_unresolved_user_fallback")
        if isinstance(fallback, dict):
            fallback_account = str(fallback.get("account") or "").strip()
        else:
            fallback_account = str(fallback or "").strip()
        if not fallback_account:
            plex_cfg = cfg.get("plex") if isinstance(cfg, dict) else {}
            if isinstance(plex_cfg, dict):
                fallback_account = str(plex_cfg.get("username") or "").strip()
        return fallback_account

    def _route_event(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> ScrobbleEvent:
        fallback_account = self._fallback_account(ev, cfg)
        if not fallback_account:
            return ev
        raw2 = dict(ev.raw if isinstance(ev.raw, dict) else {})
        raw2["_cw_unresolved_user_fallback_used"] = True
        self._throttled_route_log(
            f"fallback|{fallback_account}|{ev.session_key}",
            f"route {self._route_label(cfg)}: unresolved user fallback used "
            f"user={mask_account(fallback_account)} sess={ev.session_key}",
        )
        return ScrobbleEvent(**{**ev.__dict__, "account": fallback_account, "raw": raw2})

    def needs_user_resolution(self) -> bool:
        try:
            cfg = self._cfg_provider() or {}
            watch_cfg = ((cfg.get("scrobble") or {}).get("watch") or {})
            filt = watch_cfg.get("filters") or {}
            if not isinstance(filt, dict):
                filt = {}
            scoped = bool(str(watch_cfg.get("route_profile_id") or "").strip())
            if filt.get("username_whitelist") or str(filt.get("user_id") or "").strip() or scoped:
                return True
            route_options = watch_cfg.get("route_options") or {}
            watch_options = route_options.get("watch") if isinstance(route_options, dict) else {}
            return bool(isinstance(watch_options, dict) and watch_options.get("unresolved_user_fallback"))
        except Exception:
            return False

    def _passes_filters(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> bool:
        cache_key: str | None = None
        if ev.session_key:
            cache_key = f"{ev.session_key}|{_norm_user(ev.account or '')}|{str(ev.server_uuid or '').strip().lower()}"

        ignore_reason = event_ignore_reason(ev, cfg)
        if ignore_reason:
            log_media_filter_drop(ev, ignore_reason)
            return False

        if cache_key:
            if cache_key in self._session_ok:
                return True

        if not self._identity_allowed(ev, cfg):
            return False
        if cache_key:
            self._session_ok.add(cache_key)
        return True

    def accepts_user(self, ev: ScrobbleEvent) -> bool:
        try:
            cfg = self._cfg_provider() or {}
            acct = self._fallback_account(ev, cfg)
            if acct:
                ev = ScrobbleEvent(**{**ev.__dict__, "account": acct})
            return self._identity_allowed(ev, cfg)
        except Exception:
            return True

    def _identity_allowed(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> bool:
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        if not isinstance(filt, dict):
            filt = {}

        wl = filt.get("username_whitelist")
        want_user = str(filt.get("user_id") or "").strip().lower()
        if not _server_uuid_allowed(filt, ev.server_uuid):
            return False

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

        raw = ev.raw or {}
        ident = raw.get("_cw_session_identity") if isinstance(raw, dict) else None
        if not isinstance(ident, dict):
            ident = {}

        n = (find_psn(raw) or [None])[0] or {}
        acc_id = str(ident.get("account_id") or n.get("accountID") or "")
        acc_uuid = str(ident.get("account_uuid") or n.get("accountUUID") or "").lower()
        user_id = str(ident.get("user_id") or "").strip().lower() or find_user_id(raw)
        account = (
            str(
                ev.account
                or ident.get("name")
                or ident.get("user_name")
                or ident.get("account_name")
                or ""
            ).strip()
        )

        resolved = bool(str(account or "").strip() or str(user_id or "").strip())

        if want_user and want_user != user_id:
            if resolved:
                self._throttled_route_log(
                    f"user_id|{user_id}|{ev.session_key}",
                    f"route {self._route_label(cfg)}: filtered user={mask_account(account)} "
                    f"sess={ev.session_key} reason=user_id",
                )
            return False

        scoped = bool(str(((cfg.get("scrobble") or {}).get("watch") or {}).get("route_profile_id") or "").strip())
        if not wl and scoped:
            _log(
                f"route {self._route_label(cfg)}: filtered user={mask_account(account)} "
                f"sess={ev.session_key} reason=profile_scoped_without_whitelist",
                "WARNING",
            )
            return False

        if not media_account_allowed(wl, account, account_id=acc_id, account_uuid=acc_uuid, user_id=user_id, default_allow=not scoped):
            if resolved:
                self._throttled_route_log(
                    f"username|{account}|{ev.session_key}",
                    f"route {self._route_label(cfg)}: filtered user={mask_account(account)} "
                    f"sess={ev.session_key} reason=username_whitelist",
                )
            return False
        return True

    def _should_send(self, ev: ScrobbleEvent, cfg: dict[str, Any], sk: str) -> bool:
        last_a = self._last_action.get(sk)
        last_p = self._last_progress.get(sk, -1)
        try:
            sup = int(((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99))
            pause_db = float(((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5))
        except Exception:
            sup, pause_db = 99, 5.0

        if ev.action == "start" and last_p is not None and last_p >= sup and ev.progress >= sup:
            return False

        changed = (ev.action != last_a) or (abs(ev.progress - last_p) >= 1)

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

    def accepts(self, ev: ScrobbleEvent) -> bool:
        cfg = self._cfg_provider() or {}
        ev = self._route_event(ev, cfg)
        return self._passes_filters(ev, cfg)

    def dispatch(self, ev: ScrobbleEvent) -> bool:
        return self._dispatch(ev)

    def retry_pending(self, cancelled: threading.Event | None = None) -> None:
        due: list[tuple[str, ScrobbleEvent]] = []
        with self._dispatch_lock:
            now = time.monotonic()
            config_identity = self._config_identity
            for sk, (created, ev) in list(self._pending.items()):
                if cancelled is not None and cancelled.is_set():
                    return
                if now - created >= 3600:
                    self._pending.pop(sk, None)
                    self._retry_after.pop(sk, None)
                    continue
                if now >= self._retry_after.get(sk, (0.0, 0))[0]:
                    due.append((sk, ev))
        if not due or (cancelled is not None and cancelled.is_set()):
            return
        cfg = self._cfg_provider() or {}
        started = time.monotonic()
        for sk, ev in due[:4]:
            if cancelled is not None and cancelled.is_set():
                return
            self._dispatch(ev, retry_key=sk, cfg=cfg, config_identity=config_identity)
            if time.monotonic() - started >= 1.0:
                break

    def _queue_pending(self, sk: str, ev: ScrobbleEvent) -> None:
        now = time.monotonic()
        self._pending[sk] = (self._pending.get(sk, (now, ev))[0], ev)
        self._pending.move_to_end(sk)
        if len(self._pending) > 1024:
            expired, _ = self._pending.popitem(last=False)
            self._retry_after.pop(expired, None)

    def _failed_sink(self, sk: str, ev: ScrobbleEvent) -> None:
        retry = self._retry_after.get(sk)
        attempts = min(5, (retry[1] if retry else 0) + 1)
        now = time.monotonic()
        self._retry_after[sk] = (now + min(300, 30 * 2 ** (attempts - 1)), attempts)
        self._last_action.pop(sk, None)
        self._last_progress.pop(sk, None)
        self._queue_pending(sk, ev)

    def _delivery_identity(self, cfg: dict[str, Any]) -> str:
        from providers.scrobble.routes import scrobble_sink_config

        scrobble = cfg.get("scrobble") or {}
        watch = scrobble.get("watch") or {}
        sink = str(watch.get("route_sink") or "").strip().lower()
        block = scrobble_sink_config(cfg, sink, watch.get("route_sink_instance"))[sink] if sink else {}
        relevant = {"watch": {k: v for k, v in watch.items() if k != "routes"},
                    "sink": block, "policy": scrobble.get("trakt"), "anime_mapping": cfg.get("anime_mapping")}
        return hashlib.sha256(json.dumps(relevant, sort_keys=True, default=str).encode()).hexdigest()

    def _session_key(self, sink: Any, ev: ScrobbleEvent) -> str:
        identity: Any = ev.session_key
        raw = ev.raw if isinstance(ev.raw, dict) else {}
        playing = raw.get("NowPlayingItem")
        item_id = playing.get("Id") if isinstance(playing, dict) else None
        if not identity:
            identity = [ev.title, ev.year, ev.ids]
        return json.dumps([id(sink), ev.server_uuid, ev.account, identity, item_id, ev.media_type, ev.season, ev.number], sort_keys=True)

    def _dispatch(self, ev: ScrobbleEvent, *, retry_key: str | None = None, cfg: dict[str, Any] | None = None,
                  config_identity: str | None = None) -> bool:
        from providers.scrobble.routes import same_scrobble_endpoint
        with self._dispatch_lock:
            if config_identity is not None and config_identity != self._config_identity:
                return False
            cfg = cfg if cfg is not None else (self._cfg_provider() or {})
            identity = self._delivery_identity(cfg)
            watch = ((cfg.get("scrobble") or {}).get("watch") or {})
            if identity != self._config_identity:
                if self._pending:
                    _log(f"route {self._route_label(cfg)}: cancelled {len(self._pending)} queued deliveries after destination or route configuration changed", "WARNING")
                self._pending.clear()
                self._retry_after.clear()
                self._session_ok.clear()
                self._last_action.clear()
                self._last_progress.clear()
                self._debounce.clear()
                self._failed_ids.clear()
                self._config_identity = identity
            if retry_key is not None:
                if retry_key not in self._pending:
                    return False
                ev = self._pending[retry_key][1]
            if same_scrobble_endpoint(watch.get("route_provider"), watch.get("route_provider_instance"), watch.get("route_sink"), watch.get("route_sink_instance")):
                return False
            ev = self._route_event(ev, cfg)
            if not self._passes_filters(ev, cfg):
                return False
        sent = False
        failed = False
        queued = False
        for s in self._sinks:
            sk = self._session_key(s, ev)
            with self._dispatch_lock:
                if identity != self._config_identity:
                    return False
                if retry_key is not None and (sk != retry_key or sk not in self._pending):
                    continue
                if id(s) in self._inflight:
                    if retry_key is None:
                        self._queue_pending(sk, ev)
                        self._retry_after.setdefault(sk, (time.monotonic(), 0))
                    queued = True
                    continue
                retry = self._retry_after.get(sk)
                if retry and time.monotonic() < retry[0]:
                    if sk in self._pending and retry_key is None:
                        self._queue_pending(sk, ev)
                    failed = True
                    continue
                if retry_key is None and sk in self._pending:
                    self._queue_pending(sk, ev)
                if sk in self._failed_ids and self._failed_ids[sk] != ev.ids:
                    self._last_action.pop(sk, None)
                    self._last_progress.pop(sk, None)
                if not self._should_send(ev, cfg, sk):
                    if retry_key is not None:
                        self._pending.pop(sk, None)
                        self._retry_after.pop(sk, None)
                    continue
                self._inflight.add(id(s))
            delivered = False
            try:
                log_delivery = bool(getattr(s, "log_delivery", True))
                try:
                    ev_for_sink = maybe_enrich_event_for_sink(ev, sink_name_for_mapping(s), cfg)
                    result = self._send_sink(s, ev_for_sink, cfg)
                except Exception as e:
                    result = {"ok": False, "error": str(e), "retryable": True}
                delivered = True
                with self._dispatch_lock:
                    if identity != self._config_identity:
                        continue
                    pending = self._pending.get(sk)
                    newer = pending is not None and pending[1] is not ev
                    if isinstance(result, dict) and result.get("ok") is False:
                        failed = True
                        self._failed_ids[sk] = dict(ev.ids)
                        self._failed_ids.move_to_end(sk)
                        if len(self._failed_ids) > 1024:
                            expired, _ = self._failed_ids.popitem(last=False)
                            self._last_action.pop(expired, None)
                            self._last_progress.pop(expired, None)
                            self._debounce.pop(f"{expired}|pause", None)
                        if result.get("retryable", not result.get("skipped")):
                            self._failed_sink(sk, pending[1] if newer and pending else ev)
                        elif newer:
                            self._last_action.pop(sk, None)
                            self._last_progress.pop(sk, None)
                            self._retry_after[sk] = (time.monotonic(), 0)
                        else:
                            self._retry_after.pop(sk, None)
                            self._pending.pop(sk, None)
                        if log_delivery:
                            _log(f"route {self._route_label(cfg)}: failed {ev.action} "
                                 f"user={mask_account(ev.account)} sess={ev.session_key} "
                                 f"reason={result.get('error') or 'unknown'}", "ERROR")
                        continue
                    self._failed_ids.pop(sk, None)
                    if newer:
                        self._retry_after[sk] = (time.monotonic(), 0)
                    else:
                        self._retry_after.pop(sk, None)
                        self._pending.pop(sk, None)
                    sent = True
                    if log_delivery:
                        skipped = isinstance(result, dict) and bool(result.get("skipped"))
                        status = "skipped" if skipped else "accepted"
                        reason = str(result.get("reason") or "unknown") if skipped else ""
                        self._throttled_route_log(
                            f"{id(s)}|{status}|{reason}|{ev.action}|{ev.account}|{ev.session_key}",
                            f"route {self._route_label(cfg)}: {status} {ev.action} "
                            f"user={mask_account(ev.account)} p={ev.progress} sess={ev.session_key}"
                            + (f" reason={reason}" if reason else ""),
                        )
            finally:
                with self._dispatch_lock:
                    self._inflight.discard(id(s))
                    if not delivered and identity == self._config_identity:
                        self._last_action.pop(sk, None)
                        self._last_progress.pop(sk, None)
                        self._debounce.pop(f"{sk}|pause", None)

        return not failed and (sent or queued or not self._sinks)


__all__ = (
    "ScrobbleEvent",
    "ScrobbleSink",
    "Dispatcher",
    "from_plex_webhook",
    "from_plex_pssn",
    "from_plex_flat_playing",
)

# providers/scrobble/scrob/watch.py
# CrossWatch - Scrob playback session watcher
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import requests

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.config_base import load_config
from cw_platform.provider_instances import get_provider_block, normalize_instance_id
from providers.scrobble.currently_watching import update_from_event as _cw_update
from providers.scrobble.currently_watching import update_from_payload as _cw_update_payload
from providers.scrobble.scrob.sink import is_crosswatch_session
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent, ScrobbleSink, mask_account
from providers.scrobble.sources import source_enabled
from providers.auth._auth_SCROB import normalize_server_url
from providers.sync.scrob._common import (
    PATH_NOW_PLAYING,
    as_int,
    ok_status,
    positive_int,
    safe_json,
    scrob_request,
)

BASE_POLL_SECONDS = 4.0
MIN_POLL_SECONDS = 2.0
MAX_BASE_POLL_SECONDS = 15.0
MAX_IDLE_POLL_SECONDS = 20.0
OFFLINE_INITIAL_RETRY_SECONDS = 30.0
OFFLINE_MAX_RETRY_SECONDS = 300.0
SEEK_JUMP_PERCENT = 10.0
MISSING_POLLS_BEFORE_STOP = 1


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="SCROB-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[SCROB-WATCH:{lvl}] {msg}")
    except Exception:
        pass


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _clamp_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        num = float(value)
    except Exception:
        num = default
    return max(low, min(high, num))


def _stable_server_uuid(server: str) -> str:
    base = normalize_server_url(server)
    return "scrob:" + hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:16] if base else "scrob"


def _ids_from_media(media: Mapping[str, Any], media_type: str) -> dict[str, str]:
    ids: dict[str, str] = {}
    tmdb = positive_int(media.get("tmdb_id"))
    if media_type == "episode":
        if tmdb:
            ids["tmdb_episode"] = str(tmdb)
        show_tmdb = positive_int(media.get("show_tmdb_id"))
        if show_tmdb:
            ids["tmdb_show"] = str(show_tmdb)
        show_tvdb = positive_int(media.get("show_tvdb_id"))
        if show_tvdb:
            ids["tvdb_show"] = str(show_tvdb)
        return ids
    if tmdb:
        ids["tmdb"] = str(tmdb)
    return ids


def _metadata_from_session(row: Mapping[str, Any]) -> dict[str, Any] | None:
    media = _dict(row.get("media"))
    raw_type = str(media.get("type") or "").strip().lower()
    if raw_type not in ("movie", "episode"):
        return None

    ids = _ids_from_media(media, raw_type)
    if not ids:
        return None

    episode_title = str(media.get("title") or "").strip()
    show_title = str(media.get("show_title") or "").strip()
    title = (show_title or episode_title) if raw_type == "episode" else episode_title

    runtime = as_int(media.get("runtime"))
    duration_ms = runtime * 60_000 if runtime and runtime > 0 else None

    return {
        "media_type": raw_type,
        "ids": ids,
        "title": title or None,
        "episode_title": episode_title or None,
        "year": None,
        "season": as_int(media.get("season_number")) if raw_type == "episode" else None,
        "number": as_int(media.get("episode_number")) if raw_type == "episode" else None,
        "duration_ms": duration_ms,
    }


def _progress_of(row: Mapping[str, Any]) -> tuple[float, int | None]:
    try:
        fraction = float(row.get("progress_percent") or 0.0)
    except Exception:
        fraction = 0.0
    percent = max(0.0, min(100.0, fraction * 100.0))
    seconds = as_int(row.get("progress_seconds"))
    position_ms = seconds * 1000 if seconds and seconds > 0 else None
    return percent, position_ms


class ScrobWatchService:
    def __init__(
        self,
        sinks: list[ScrobbleSink] | None = None,
        *,
        dispatcher: Any | None = None,
        cfg_provider: Callable[[], dict[str, Any]] | None = None,
        instance_id: Any = "default",
        poll_secs: float = BASE_POLL_SECONDS,
        quiet_startup: bool = False,
    ) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._dispatch = dispatcher or Dispatcher(list(sinks or []), cfg_provider=self._active_cfg)
        self._base_poll = _clamp_float(poll_secs, BASE_POLL_SECONDS, MIN_POLL_SECONDS, MAX_BASE_POLL_SECONDS)
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None
        self._sessions: dict[str, dict[str, Any]] = {}
        self._missing: dict[str, int] = {}
        self._idle_poll = self._base_poll
        self._last_error: tuple[str, float] = ("", 0.0)
        self._quiet_startup = bool(quiet_startup)
        self._offline = False
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
        self.session = requests.Session()
        try:
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers.setdefault("User-Agent", "CrossWatch/Watcher/1.0")
        except Exception:
            pass

    @property
    def config(self) -> dict[str, Any]:
        return self._active_cfg()

    @property
    def instance_id(self) -> str:
        return self._instance_id

    def _active_cfg(self) -> dict[str, Any]:
        if self._cfg_provider:
            try:
                cfg = self._cfg_provider() or {}
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                return {}
        try:
            cfg = load_config() or {}
            return cfg if isinstance(cfg, dict) else {}
        except Exception:
            return {}

    def _block(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return get_provider_block(cfg, "scrob", self._instance_id) or {}
        except Exception:
            return _dict(cfg.get("scrob"))

    def _configured(self, cfg: Mapping[str, Any]) -> bool:
        block = self._block(cfg)
        return bool(str(block.get("server_url") or "").strip() and str(block.get("api_key") or "").strip())

    def _log_error_limited(self, key: str, message: str) -> None:
        now = time.time()
        last_key, last_ts = self._last_error
        if key != last_key or (now - last_ts) >= 30.0:
            _log(message, "WARNING")
            self._last_error = (key, now)

    def _mark_offline(self, exc: Exception) -> None:
        if not self._offline:
            self._offline = True
            self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
            _log(f"Scrob watcher offline: {exc.__class__.__name__}; retrying with backoff", "WARNING")
            return
        self._offline_retry = min(OFFLINE_MAX_RETRY_SECONDS, max(OFFLINE_INITIAL_RETRY_SECONDS, self._offline_retry * 2.0))

    def _mark_online(self) -> None:
        if self._offline:
            _log("Scrob watcher reconnected", "INFO")
        self._offline = False
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
        self._last_error = ("", 0.0)

    def _progress_step(self) -> float:
        cfg = self._active_cfg()
        try:
            return max(1.0, float((((cfg.get("scrobble") or {}).get("trakt") or {}).get("progress_step") or 25)))
        except Exception:
            return 25.0

    def _force_stop_at(self) -> float:
        cfg = self._active_cfg()
        try:
            return max(0.0, min(100.0, float((((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at") or 95))))
        except Exception:
            return 95.0

    def _event(self, session: Mapping[str, Any], action: str, progress: float) -> ScrobbleEvent:
        meta = _dict(session.get("meta"))
        duration_ms = as_int(session.get("duration_ms"))
        position_ms = as_int(session.get("position_ms"))
        if position_ms is None and duration_ms and duration_ms > 0:
            position_ms = int(round((float(progress or 0.0) / 100.0) * float(duration_ms)))
        raw = {
            "provider": "scrob",
            "provider_instance": self._instance_id,
            "session_key": session.get("source_session_key"),
            "source": session.get("source"),
            "media": session.get("media"),
            "_cw_seek": bool(session.get("seek_pending")),
            "_cw_preserve_stop": bool(action == "stop" and progress >= self._force_stop_at()),
        }
        return ScrobbleEvent(
            action=action,  # type: ignore[arg-type]
            media_type=str(meta.get("media_type") or "movie"),  # type: ignore[arg-type]
            ids=dict(meta.get("ids") or {}),
            title=meta.get("title"),
            year=meta.get("year"),
            season=meta.get("season"),
            number=meta.get("number"),
            progress=max(0.0, min(100.0, float(progress or 0.0))),
            account=session.get("account"),
            server_uuid=session.get("server_uuid"),
            session_key=str(session.get("session_key") or ""),
            raw=raw,
            position_ms=position_ms,
            duration_ms=duration_ms if duration_ms and duration_ms > 0 else None,
        )

    def _dispatch_event(self, ev: ScrobbleEvent, duration_ms: int | None = None) -> bool:
        accepted = bool(self._dispatch.dispatch(ev))
        if accepted:
            try:
                _cw_update("scrob", ev, duration_ms=duration_ms, provider_instance=self._instance_id)
            except Exception:
                pass
            _log(
                f"event {ev.action} {ev.media_type} user={mask_account(ev.account)} p={ev.progress:.1f} sess={ev.session_key}",
                "DEBUG",
            )
        elif ev.action == "stop":
            try:
                _cw_update_payload(
                    "scrob",
                    ev.media_type,
                    ev.title or "",
                    ev.year,
                    ev.season,
                    ev.number,
                    ev.progress,
                    True,
                    state="stopped",
                    clear_on_stop=True,
                    ids=ev.ids,
                    session_key=ev.session_key,
                    provider_instance=self._instance_id,
                )
            except Exception:
                pass
        return accepted

    def _create_session(self, row: Mapping[str, Any], source_key: str) -> dict[str, Any] | None:
        meta = _metadata_from_session(row)
        if not meta:
            return None
        cfg = self._active_cfg()
        server = str(self._block(cfg).get("server_url") or "")
        started = str(row.get("started_at") or "")
        session_key = f"scrob:{self._instance_id}:{source_key}:{started}"
        percent, position_ms = _progress_of(row)
        session = {
            "session_key": session_key,
            "source_session_key": source_key,
            "source": str(row.get("source") or ""),
            "meta": meta,
            "media": _dict(row.get("media")),
            "account": str(row.get("source") or "").strip() or None,
            "server_uuid": _stable_server_uuid(server),
            "state": "",
            "last_action": "",
            "last_progress": percent,
            "last_emitted_progress": None,
            "position_ms": position_ms,
            "duration_ms": meta.get("duration_ms"),
            "seek_pending": False,
        }
        self._sessions[source_key] = session
        return session

    def _maybe_dispatch_progress(self, session: dict[str, Any], row: Mapping[str, Any]) -> None:
        percent, position_ms = _progress_of(row)
        state = "paused" if str(row.get("state") or "").strip().lower() == "paused" else "playing"
        session["last_progress"] = percent
        if position_ms is not None:
            session["position_ms"] = position_ms
            if percent > 0 and not session.get("duration_ms"):
                session["duration_ms"] = int(round(position_ms / (percent / 100.0)))

        prev_state = str(session.get("state") or "")
        prev_action = str(session.get("last_action") or "")
        prev_progress = session.get("last_emitted_progress")
        emit_action: str | None = None

        if not prev_action:
            emit_action = "start"
        elif state == "paused" and prev_state != "paused":
            emit_action = "pause"
        elif state == "playing" and prev_state == "paused":
            emit_action = "start"
        elif state == "playing" and prev_action == "start":
            try:
                last_emit = float(prev_progress if prev_progress is not None else percent)
            except Exception:
                last_emit = percent
            jump = abs(percent - last_emit)
            if jump >= self._progress_step():
                emit_action = "start"
                session["seek_pending"] = bool(jump >= SEEK_JUMP_PERCENT)

        session["state"] = state
        if not emit_action:
            return

        emit_progress = max(1.0, percent) if emit_action == "start" else percent
        ev = self._event(session, emit_action, emit_progress)
        if self._dispatch_event(ev, session.get("duration_ms")):
            session["last_action"] = emit_action
            session["last_emitted_progress"] = emit_progress
        session["seek_pending"] = False

    def _stop_session(self, source_key: str) -> None:
        session = self._sessions.pop(source_key, None)
        self._missing.pop(source_key, None)
        if not session:
            return
        percent = float(session.get("last_progress") or 0.0)
        self._dispatch_event(self._event(session, "stop", percent), session.get("duration_ms"))

    def _fetch_sessions(self) -> list[dict[str, Any]]:
        resp = scrob_request(self, "GET", PATH_NOW_PLAYING)
        if not ok_status(resp):
            raise RuntimeError(f"http:{int(resp.status_code)}")
        data = safe_json(resp)
        rows = data.get("now_playing") if isinstance(data, Mapping) else None
        return [dict(r) for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []

    def _tick(self) -> bool:
        cfg = self._active_cfg()
        if not self._configured(cfg):
            return False

        try:
            rows = self._fetch_sessions()
        except Exception as exc:
            self._mark_offline(exc)
            return bool(self._sessions)

        self._mark_online()

        seen: set[str] = set()
        active = False
        for row in rows:
            source_key = str(row.get("session_key") or "").strip()
            if not source_key:
                continue
            if is_crosswatch_session(source_key):
                continue
            seen.add(source_key)
            session = self._sessions.get(source_key)
            try:
                if session is None:
                    session = self._create_session(row, source_key)
                    if session is None:
                        continue
                active = True
                self._missing.pop(source_key, None)
                self._maybe_dispatch_progress(session, row)
            except Exception as exc:
                self._log_error_limited(f"{type(exc).__name__}:{source_key}", f"Scrob session {source_key} poll failed: {exc}")
                continue

        for source_key in list(self._sessions.keys()):
            if source_key in seen:
                continue
            misses = self._missing.get(source_key, 0) + 1
            self._missing[source_key] = misses
            if misses >= MISSING_POLLS_BEFORE_STOP:
                self._stop_session(source_key)

        return active

    def start(self) -> None:
        cfg = self._active_cfg()
        if not source_enabled(cfg, "watcher"):
            if not self._quiet_startup:
                _log("Watcher source is disabled; Scrob watcher not started.", "INFO")
            return
        if not self._configured(cfg):
            if not self._quiet_startup:
                _log("Scrob is not connected; watcher not started.", "WARNING")
            return
        self._stop.clear()
        while not self._stop.is_set():
            active = self._tick()
            if self._offline:
                self._stop.wait(self._offline_retry)
                continue
            if active:
                self._idle_poll = self._base_poll
            else:
                self._idle_poll = min(MAX_IDLE_POLL_SECONDS, max(self._base_poll, self._idle_poll + 1.5))
            self._stop.wait(self._idle_poll)

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._stop.clear()
        self._bg = threading.Thread(target=self.start, name=f"ScrobWatch:{self._instance_id}", daemon=True)
        self._bg.start()

    def stop(self) -> None:
        self._stop.set()
        bg = self._bg
        if bg and bg.is_alive() and bg is not threading.current_thread():
            bg.join(timeout=6.0)

    def is_alive(self) -> bool:
        bg = self._bg
        return bool(bg and bg.is_alive() and not self._stop.is_set())


def make_default_watch(
    dispatcher: Any | None = None,
    cfg_provider: Callable[[], dict[str, Any]] | None = None,
    instance_id: Any = "default",
    sinks: list[ScrobbleSink] | None = None,
) -> ScrobWatchService:
    return ScrobWatchService(sinks=sinks, dispatcher=dispatcher, cfg_provider=cfg_provider, instance_id=instance_id)


WatchService = ScrobWatchService

__all__ = ["ScrobWatchService", "WatchService", "make_default_watch"]

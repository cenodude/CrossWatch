# providers/scrobble/silo/watch.py
# CrossWatch - Silo playback session watcher
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

from cw_platform.app_version import user_agent as http_user_agent
from cw_platform.config_base import load_config
from cw_platform.provider_instances import get_instance_block, list_sync_user_ids, normalize_instance_id
from providers.auth._auth_SILO import is_configured, normalize_server_url, provider_block
from providers.scrobble.currently_watching import update_from_event as _cw_update
from providers.scrobble.currently_watching import update_from_payload as _cw_update_payload
from providers.scrobble.routes import watch_poll_seconds
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent, ScrobbleSink, mask_account
from providers.scrobble.sources import source_enabled
from providers.sync.silo._common import as_int, ids_from_content_id, item_detail, item_ids, safe_json, series_id_of, silo_request

BASE_POLL_SECONDS = 10.0
MIN_POLL_SECONDS = 2.0
MAX_BASE_POLL_SECONDS = 60.0
MAX_IDLE_POLL_SECONDS = 30.0
OFFLINE_INITIAL_RETRY_SECONDS = 30.0
OFFLINE_MAX_RETRY_SECONDS = 300.0
SEEK_JUMP_PERCENT = 10.0
MISSING_POLLS_BEFORE_STOP = 1
SESSIONS_PATH = "/profiles/household/sessions"


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="SILO-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[SILO-WATCH:{lvl}] {msg}")
    except Exception:
        pass


def _clamp_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        num = float(value)
    except Exception:
        num = default
    return max(low, min(high, num))


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def server_uuid_for(server: str) -> str:
    base = normalize_server_url(server)
    return "silo:" + hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:16] if base else "silo"


def progress_of(row: Mapping[str, Any]) -> tuple[float, int | None, int | None]:
    pos = _float(row.get("position_seconds"))
    dur = as_int(row.get("file_duration"))
    position_ms = int(round(pos * 1000.0)) if pos is not None and pos >= 0 else None
    duration_ms = dur * 1000 if dur and dur > 0 else None
    percent = 0.0
    if position_ms is not None and duration_ms:
        percent = max(0.0, min(100.0, position_ms / duration_ms * 100.0))
    return percent, position_ms, duration_ms


class SiloWatchService:
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
        self._default_poll = _clamp_float(poll_secs, BASE_POLL_SECONDS, MIN_POLL_SECONDS, MAX_BASE_POLL_SECONDS)
        self._base_poll = self._default_poll
        self._max_idle_poll = MAX_IDLE_POLL_SECONDS
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
            self.session.headers["User-Agent"] = http_user_agent("Watcher", override_env="CW_SILO_UA")
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
        return provider_block(cfg, self._instance_id)

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
            _log(f"Silo watcher offline: {exc}; retrying with backoff", "WARNING")
            return
        self._offline_retry = min(OFFLINE_MAX_RETRY_SECONDS, max(OFFLINE_INITIAL_RETRY_SECONDS, self._offline_retry * 2.0))

    def _mark_online(self) -> None:
        if self._offline:
            _log("Silo watcher reconnected", "INFO")
        self._offline = False
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
        self._last_error = ("", 0.0)

    def _scrobble_setting(self, key: str, default: float) -> float:
        cfg = self._active_cfg()
        try:
            return float((((cfg.get("scrobble") or {}).get("trakt") or {}).get(key) or default))
        except Exception:
            return default

    def _metadata(self, row: Mapping[str, Any]) -> dict[str, Any] | None:
        cid = str(row.get("content_id") or "").strip()
        media_type = str(row.get("media_type") or "").strip().lower()
        if not cid or media_type not in ("movie", "episode"):
            return None
        if media_type == "movie":
            _kind, ids = ids_from_content_id(cid)
            ids = {**ids, **item_ids(self, cid, feature="watch")}
            if not ids:
                return None
            return {"media_type": "movie", "ids": ids, "title": str(row.get("media_title") or "").strip() or None, "year": None, "season": None, "number": None}
        detail = item_detail(self, cid, feature="watch")
        series = str(detail.get("series_id") or "") or series_id_of(cid)
        if not series:
            return None
        _kind, show_ids = ids_from_content_id(series)
        show_ids = {**show_ids, **item_ids(self, series, feature="watch")}
        if not show_ids:
            return None
        ids = {f"{k}_show": v for k, v in show_ids.items()}
        for k in ("tmdb", "tvdb", "imdb"):
            v = str(detail.get(f"{k}_id") or "").strip()
            if v and v != show_ids.get(k):
                ids[f"{k}_episode"] = v
        season = as_int(row.get("season_number"))
        number = as_int(row.get("episode_number"))
        if season is None or number is None:
            season, number = as_int(detail.get("season_number")), as_int(detail.get("episode_number"))
        if season is None or number is None:
            return None
        return {
            "media_type": "episode",
            "ids": ids,
            "title": str(row.get("series_name") or "").strip() or None,
            "episode_title": str(row.get("episode_name") or "").strip() or None,
            "year": None,
            "season": season,
            "number": number,
        }

    def _event(self, session: Mapping[str, Any], action: str, progress: float) -> ScrobbleEvent:
        meta = dict(session.get("meta") or {})
        duration_ms = as_int(session.get("duration_ms"))
        position_ms = as_int(session.get("position_ms"))
        raw = {
            "provider": "silo",
            "provider_instance": self._instance_id,
            "session_key": session.get("source_session_key"),
            "content_id": session.get("content_id"),
            "profile_id": session.get("profile_id"),
            "client": session.get("client"),
            "_cw_session_identity": {"user_id": session.get("profile_id"), "name": session.get("account")},
            "_cw_seek": bool(session.get("seek_pending")),
            "_cw_preserve_stop": bool(action == "stop" and progress >= self._scrobble_setting("force_stop_at", 95.0)),
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
                _cw_update("silo", ev, duration_ms=duration_ms, provider_instance=self._instance_id)
            except Exception:
                pass
            _log(f"event {ev.action} {ev.media_type} user={mask_account(ev.account)} p={ev.progress:.1f} sess={ev.session_key}", "DEBUG")
        elif ev.action == "stop":
            try:
                _cw_update_payload(
                    "silo", ev.media_type, ev.title or "", ev.year, ev.season, ev.number, ev.progress, True,
                    state="stopped", clear_on_stop=True, ids=ev.ids, session_key=ev.session_key, provider_instance=self._instance_id,
                )
            except Exception:
                pass
        return accepted

    def _create_session(self, row: Mapping[str, Any], source_key: str) -> dict[str, Any] | None:
        meta = self._metadata(row)
        if not meta:
            return None
        server = str(self._block(self._active_cfg()).get("server_url") or "")
        percent, position_ms, duration_ms = progress_of(row)
        session = {
            "session_key": f"silo:{self._instance_id}:{source_key}:{row.get('started_at') or ''}",
            "source_session_key": source_key,
            "content_id": str(row.get("content_id") or ""),
            "profile_id": str(row.get("profile_id") or ""),
            "client": str(row.get("client_label") or row.get("client_name") or ""),
            "meta": meta,
            "account": str(row.get("profile_name") or "").strip() or None,
            "server_uuid": server_uuid_for(server),
            "state": "",
            "last_action": "",
            "last_progress": percent,
            "last_emitted_progress": None,
            "position_ms": position_ms,
            "duration_ms": duration_ms,
            "seek_pending": False,
        }
        self._sessions[source_key] = session
        return session

    def _maybe_dispatch_progress(self, session: dict[str, Any], row: Mapping[str, Any]) -> None:
        percent, position_ms, duration_ms = progress_of(row)
        state = "paused" if row.get("is_paused") is True else "playing"
        session["last_progress"] = percent
        if position_ms is not None:
            session["position_ms"] = position_ms
        if duration_ms:
            session["duration_ms"] = duration_ms
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
            if jump >= max(1.0, self._scrobble_setting("progress_step", 25.0)):
                emit_action = "start"
                session["seek_pending"] = bool(jump >= SEEK_JUMP_PERCENT)
        session["state"] = state
        if not emit_action:
            return
        emit_progress = max(1.0, percent) if emit_action == "start" else percent
        if self._dispatch_event(self._event(session, emit_action, emit_progress), session.get("duration_ms")):
            session["last_action"] = emit_action
            session["last_emitted_progress"] = emit_progress
        session["seek_pending"] = False

    def _stop_session(self, source_key: str) -> None:
        session = self._sessions.pop(source_key, None)
        self._missing.pop(source_key, None)
        if not session:
            return
        self._dispatch_event(self._event(session, "stop", float(session.get("last_progress") or 0.0)), session.get("duration_ms"))

    def _fetch_sessions(self) -> list[dict[str, Any]]:
        resp = silo_request(self, "GET", SESSIONS_PATH)
        if resp.status_code != 200:
            raise RuntimeError(f"http:{int(resp.status_code)}")
        data = safe_json(resp)
        rows = data.get("items") if isinstance(data, Mapping) else None
        if not isinstance(rows, list):
            raise RuntimeError("invalid_response")
        return [dict(r) for r in rows if isinstance(r, Mapping)]

    def _profile_ids(self, cfg: Mapping[str, Any], block: Mapping[str, Any]) -> set[str]:
        out = {str(block.get("profile_id") or "")}
        needs = getattr(self._dispatch, "needs_user_resolution", None)
        try:
            wants_users = bool(needs()) if callable(needs) else False
        except Exception:
            wants_users = False
        if wants_users:
            for user_inst in list_sync_user_ids(cfg, "silo", self._instance_id):
                pid = str(get_instance_block(cfg, "silo", user_inst).get("profile_id") or "")
                if pid:
                    out.add(pid)
        out.discard("")
        return out

    def _tick(self) -> bool:
        cfg = self._active_cfg()
        self._base_poll = watch_poll_seconds(cfg, self._default_poll)
        self._max_idle_poll = watch_poll_seconds(cfg, MAX_IDLE_POLL_SECONDS, "idle_poll_seconds")
        block = self._block(cfg)
        if not is_configured(block):
            return False
        try:
            rows = self._fetch_sessions()
        except Exception as exc:
            self._mark_offline(exc)
            return bool(self._sessions)
        self._mark_online()
        profiles = self._profile_ids(cfg, block)
        seen: set[str] = set()
        active = False
        for row in rows:
            source_key = str(row.get("id") or "").strip()
            if not source_key or str(row.get("profile_id") or "") not in profiles:
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
                self._log_error_limited(f"{type(exc).__name__}:{source_key}", f"Silo session poll failed: {exc}")
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
                _log("Watcher source is disabled; Silo watcher not started.", "INFO")
            return
        if not is_configured(self._block(cfg)):
            if not self._quiet_startup:
                _log("Silo is not connected; watcher not started.", "WARNING")
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
                self._idle_poll = min(max(self._max_idle_poll, self._base_poll), max(self._base_poll, self._idle_poll * 1.5))
            self._stop.wait(self._idle_poll)

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._stop.clear()
        self._bg = threading.Thread(target=self.start, name=f"SiloWatch:{self._instance_id}", daemon=True)
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
) -> SiloWatchService:
    return SiloWatchService(sinks=sinks, dispatcher=dispatcher, cfg_provider=cfg_provider, instance_id=instance_id)


WatchService = SiloWatchService

__all__ = ["SiloWatchService", "WatchService", "make_default_watch"]

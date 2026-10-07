# providers/scrobble/stremio/watch.py
# CrossWatch - Stremio add-on playback watcher
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
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
from cw_platform.provider_instances import normalize_instance_id
from providers.scrobble.currently_watching import update_from_event as _cw_update
from providers.scrobble.currently_watching import update_from_payload as _cw_update_payload
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent, ScrobbleSink
from providers.scrobble.sources import source_enabled
from providers.scrobble.stremio.addon import PlayerEvent
from providers.sync.stremio._common import metahub_poster_url

CINEMETA_BASE = "https://v3-cinemeta.strem.io"
META_TIMEOUT_SECONDS = 4.0
META_CACHE_LIMIT = 512
TICK_SECONDS = 30.0
CARD_REFRESH_SECONDS = 120.0
STALE_GRACE_SECONDS = 900.0
PAUSED_TIMEOUT_SECONDS = 1800.0
UNKNOWN_RUNTIME_SECONDS = 4 * 3600.0
EARLY_START_MS = 120_000
ESTIMATE_STOP_CAP = 50.0

_META_CACHE: dict[tuple[str, str], dict[str, Any]] = {}
_META_LOCK = threading.Lock()


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="STREMIO-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[STREMIO-WATCH:{lvl}] {msg}")
    except Exception:
        pass


def _fetch_meta(kind: str, imdb: str) -> dict[str, Any]:
    resp = requests.get(
        f"{CINEMETA_BASE}/meta/{kind}/{imdb}.json",
        headers={"Accept": "application/json", "User-Agent": http_user_agent("Watcher", override_env="CW_STREMIO_UA")},
        timeout=META_TIMEOUT_SECONDS,
    )
    resp.raise_for_status()
    data = resp.json()
    meta = data.get("meta") if isinstance(data, Mapping) else None
    if not isinstance(meta, Mapping):
        return {}
    year = re.search(r"\d{4}", str(meta.get("year") or meta.get("releaseInfo") or ""))
    runtime = re.search(r"\d+", str(meta.get("runtime") or ""))
    minutes = int(runtime.group(0)) if runtime else 0
    return {
        "title": str(meta.get("name") or "").strip() or None,
        "year": int(year.group(0)) if year else None,
        "runtime_ms": minutes * 60_000 if 0 < minutes < 1000 else None,
    }


def lookup_meta(media_type: str, ids: Mapping[str, Any]) -> dict[str, Any]:
    kind = "movie" if media_type == "movie" else "series"
    imdb = str(ids.get("imdb") or ids.get("imdb_show") or "").strip()
    if not imdb:
        return {}
    key = (kind, imdb)
    with _META_LOCK:
        if key in _META_CACHE:
            return dict(_META_CACHE[key])
    try:
        found = _fetch_meta(kind, imdb)
    except Exception as exc:
        _log(f"metadata lookup failed for {imdb}: {type(exc).__name__}", "DEBUG")
        found = {}
    with _META_LOCK:
        if len(_META_CACHE) >= META_CACHE_LIMIT:
            _META_CACHE.clear()
        _META_CACHE[key] = dict(found)
    return found


def _percent(position_ms: int | None, duration_ms: int | None) -> float | None:
    if position_ms is None or not duration_ms:
        return None
    return max(0.0, min(100.0, (position_ms / float(duration_ms)) * 100.0))


class StremioWatchService:
    def __init__(
        self,
        sinks: list[ScrobbleSink] | None = None,
        *,
        dispatcher: Any | None = None,
        cfg_provider: Callable[[], dict[str, Any]] | None = None,
        instance_id: Any = "default",
        quiet_startup: bool = False,
    ) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._dispatch = dispatcher or Dispatcher(list(sinks or []), cfg_provider=self._active_cfg)
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None
        self._lock = threading.RLock()
        self._sessions: dict[str, dict[str, Any]] = {}
        self._quiet_startup = bool(quiet_startup)

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

    def _force_stop_at(self) -> float:
        cfg = self._active_cfg()
        try:
            return max(0.0, min(100.0, float((((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at") or 95))))
        except Exception:
            return 95.0

    def _event(self, session: Mapping[str, Any], action: str, progress: float) -> ScrobbleEvent:
        media_type = str(session.get("media_type") or "movie")
        title = session.get("title")
        raw = {
            "provider": "stremio",
            "provider_instance": self._instance_id,
            "item": {"type": media_type, "showtitle": title if media_type == "episode" else None, "title": None if media_type == "episode" else title},
            "video_id": session.get("video_id"),
            "poster": session.get("cover"),
            "duration_ms": session.get("duration_ms"),
            "_cw_preserve_stop": bool(action == "stop" and progress >= self._force_stop_at()),
        }
        return ScrobbleEvent(
            action=action,  # type: ignore[arg-type]
            media_type=media_type,  # type: ignore[arg-type]
            ids=dict(session.get("ids") or {}),
            title=title,
            year=session.get("year"),
            season=session.get("season"),
            number=session.get("number"),
            progress=max(0.0, min(100.0, float(progress or 0.0))),
            account=None,
            server_uuid=f"stremio:{self._instance_id}",
            session_key=str(session.get("session_key") or ""),
            raw=raw,
            position_ms=session.get("position_ms"),
            duration_ms=session.get("duration_ms"),
        )

    def _card(self, session: Mapping[str, Any], progress: float, state: str) -> None:
        try:
            _cw_update_payload(
                "stremio",
                str(session.get("media_type") or "movie"),
                str(session.get("title") or ""),
                session.get("year"),
                session.get("season"),
                session.get("number"),
                int(progress),
                state == "stopped",
                duration_ms=session.get("duration_ms"),
                cover=session.get("cover"),
                state=state,
                clear_on_stop=True,
                ids=dict(session.get("ids") or {}),
                session_key=str(session.get("session_key") or ""),
                provider_instance=self._instance_id,
            )
        except Exception:
            pass

    def _send(self, session: dict[str, Any], action: str, progress: float) -> bool:
        event = self._event(session, action, progress)
        accepted = bool(self._dispatch.dispatch(event))
        session["matched"] = accepted
        if accepted:
            try:
                _cw_update("stremio", event, duration_ms=event.duration_ms, cover=session.get("cover"), provider_instance=self._instance_id)
            except Exception:
                pass
            _log(f"event {action} {event.media_type} p={event.progress:.1f} sess={event.session_key}", "DEBUG")
        elif action == "stop":
            self._card(session, progress, "stopped")
        return accepted

    def _new_session(self, item: PlayerEvent) -> dict[str, Any]:
        meta = lookup_meta(item.media_type, item.ids)
        return {
            "session_key": item.session_key,
            "video_id": item.video_id,
            "media_type": item.media_type,
            "ids": dict(item.ids),
            "season": item.season,
            "number": item.number,
            "title": meta.get("title"),
            "year": meta.get("year"),
            "runtime_ms": meta.get("runtime_ms"),
            "percent_est": None,
            "cover": metahub_poster_url(item.ids.get("imdb") or item.ids.get("imdb_show")) or None,
            "state": "",
            "position_ms": None,
            "duration_ms": None,
            "percent": None,
            "matched": False,
            "updated": 0.0,
            "card_at": 0.0,
        }

    def ingest(self, item: PlayerEvent, *, now: float | None = None) -> tuple[bool, str]:
        current = float(now if now is not None else time.time())
        with self._lock:
            known = item.session_key in self._sessions
        fresh = None if known else self._new_session(item)
        with self._lock:
            session = self._sessions.get(item.session_key)
            if session is None:
                session = fresh or self._new_session(item)
                if item.action != "stop":
                    self._sessions[item.session_key] = session
            if item.duration_ms:
                session["duration_ms"] = item.duration_ms
            percent: float | None = None
            if item.position_ms is not None:
                session["position_ms"] = item.position_ms
                percent = _percent(item.position_ms, session.get("duration_ms"))
                if percent is not None:
                    session["percent"] = percent
                elif item.action != "stop":
                    percent = _percent(item.position_ms, session.get("runtime_ms"))
                    if percent is None and item.position_ms <= EARLY_START_MS:
                        percent = 0.0
                    if percent is not None:
                        session["percent_est"] = percent
            if percent is None and item.action == "stop":
                percent = session.get("percent")
                if percent is None and session.get("percent_est") is not None:
                    percent = min(float(session["percent_est"]), ESTIMATE_STOP_CAP)
            session["updated"] = current
            session["card_at"] = current

            if item.action == "stop":
                self._sessions.pop(item.session_key, None)
                if percent is None:
                    self._card(session, 0, "stopped")
                    return False, "progress_unknown"
                return (True, "") if self._send(session, "stop", percent) else (False, "no_matching_route")

            started = bool(session.get("state"))
            session["state"] = "paused" if item.action == "pause" else "playing"
            if percent is None:
                return False, "progress_unknown"
            if item.action == "pause" and not started and session.get("percent") is None:
                return False, "not_started"
            progress = max(1.0, percent) if item.action == "start" else percent
            last = session.get("last_sent")
            if last and last[0] == item.action and abs(float(last[1]) - progress) < 1.0:
                return False, "duplicate"
            if not self._send(session, item.action, progress):
                return False, "no_matching_route"
            session["last_sent"] = (item.action, progress)
            return True, ""

    def _estimate(self, session: Mapping[str, Any], now: float) -> float:
        percent = float(session.get("percent") or session.get("percent_est") or 0.0)
        duration = session.get("duration_ms") or session.get("runtime_ms")
        if session.get("state") != "playing" or not duration:
            return percent
        elapsed_ms = max(0.0, now - float(session.get("updated") or now)) * 1000.0
        return max(percent, min(99.0, percent + (elapsed_ms / float(duration)) * 100.0))

    def _expired(self, session: Mapping[str, Any], now: float) -> bool:
        idle = now - float(session.get("updated") or now)
        if session.get("state") != "playing":
            return idle > PAUSED_TIMEOUT_SECONDS
        duration = session.get("duration_ms") or session.get("runtime_ms")
        position = session.get("position_ms")
        remaining = max(0.0, (duration - position) / 1000.0) if duration and position is not None else UNKNOWN_RUNTIME_SECONDS
        return idle > remaining + STALE_GRACE_SECONDS

    def sweep(self, *, now: float | None = None) -> None:
        current = float(now if now is not None else time.time())
        with self._lock:
            for key, session in list(self._sessions.items()):
                if self._expired(session, current):
                    self._sessions.pop(key, None)
                    percent = session.get("percent")
                    if percent is None and session.get("percent_est") is not None:
                        percent = min(float(session["percent_est"]), ESTIMATE_STOP_CAP)
                    _log(f"session timed out without a stop sess={key}", "DEBUG")
                    if percent is None:
                        self._card(session, 0, "stopped")
                    else:
                        self._send(session, "stop", float(percent))
                    continue
                if session.get("matched") and current - float(session.get("card_at") or 0.0) >= CARD_REFRESH_SECONDS:
                    session["card_at"] = current
                    self._card(session, self._estimate(session, current), str(session.get("state") or "playing"))

    def start(self) -> None:
        if not source_enabled(self._active_cfg(), "watcher"):
            if not self._quiet_startup:
                _log("Watcher source is disabled; Stremio watcher not started.", "INFO")
            return
        self._stop.clear()
        if not self._quiet_startup:
            _log(f"Watcher ready, waiting for Stremio add-on events; inst={self._instance_id}", "INFO")
        while not self._stop.wait(TICK_SECONDS):
            try:
                self.sweep()
            except Exception as exc:
                _log(f"session sweep failed: {type(exc).__name__}: {exc}", "WARNING")

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._stop.clear()
        self._bg = threading.Thread(target=self.start, name=f"StremioWatch:{self._instance_id}", daemon=True)
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
) -> StremioWatchService:
    return StremioWatchService(sinks=sinks, dispatcher=dispatcher, cfg_provider=cfg_provider, instance_id=instance_id)


WatchService = StremioWatchService

__all__ = ["StremioWatchService", "WatchService", "lookup_meta", "make_default_watch"]

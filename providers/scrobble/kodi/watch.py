# providers/scrobble/kodi/watch.py
# CrossWatch - Kodi HTTP JSON-RPC Watcher Service
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.config_base import load_config
from providers.auth._auth_KODI import KodiAuthError, clean_base, jsonrpc_call
from providers.scrobble.currently_watching import update_from_event as _cw_update
from providers.scrobble.currently_watching import update_from_payload as _cw_update_payload
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent, ScrobbleSink, mask_account
from providers.scrobble.sources import source_enabled
from providers.sync.kodi._common import path_scope_status

BASE_POLL_SECONDS = 1.75
MIN_POLL_SECONDS = 1.5
MAX_BASE_POLL_SECONDS = 2.0
MAX_IDLE_POLL_SECONDS = 6.0
OFFLINE_INITIAL_RETRY_SECONDS = 30.0
OFFLINE_MAX_RETRY_SECONDS = 300.0
OFFLINE_TIMEOUT_SECONDS = 2.0
PROFILE_CACHE_SECONDS = 60.0
SEEK_JUMP_PERCENT = 10.0


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="KODI-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[KODI-WATCH:{lvl}] {msg}")
    except Exception:
        pass


def _dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def _clamp_float(value: Any, default: float, low: float, high: float) -> float:
    try:
        num = float(value)
    except Exception:
        num = default
    return max(low, min(high, num))


def _time_parts_to_seconds(value: Any) -> float | None:
    if not isinstance(value, Mapping):
        return None
    try:
        return (
            int(value.get("hours") or 0) * 3600
            + int(value.get("minutes") or 0) * 60
            + int(value.get("seconds") or 0)
            + int(value.get("milliseconds") or 0) / 1000.0
        )
    except Exception:
        return None


def _progress_from_props(props: Mapping[str, Any]) -> tuple[float, int | None]:
    pct_raw = props.get("percentage")
    try:
        if pct_raw is not None:
            pct = max(0.0, min(100.0, float(pct_raw)))
            total = _time_parts_to_seconds(props.get("totaltime"))
            return pct, int(total * 1000) if total and total > 0 else None
    except Exception:
        pass

    pos = _time_parts_to_seconds(props.get("time"))
    total = _time_parts_to_seconds(props.get("totaltime"))
    if pos is None or not total or total <= 0:
        return 0.0, int(total * 1000) if total and total > 0 else None
    return max(0.0, min(100.0, (pos / total) * 100.0)), int(total * 1000)


def _file_hash(value: Any) -> str:
    text = str(value or "").strip()
    digest = hashlib.sha1(text.encode("utf-8", "ignore")).hexdigest()[:16]
    return f"file:{digest}"


def _stable_server_uuid(server: str) -> str:
    base = clean_base(server)
    return "kodi:" + hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:16] if base else "kodi"


def _norm_unique_key(key: Any) -> str:
    return "".join(ch for ch in str(key or "").strip().lower() if ch.isalnum())


def _normalize_uniqueids(uniqueid: Any, media_type: str) -> dict[str, str]:
    ids: dict[str, str] = {}
    raw = uniqueid if isinstance(uniqueid, Mapping) else {}

    def put(name: str, value: Any) -> None:
        text = str(value or "").strip()
        if text and name not in ids:
            ids[name] = text

    for key, value in raw.items():
        text = str(value or "").strip()
        if not text:
            continue
        nk = _norm_unique_key(key)
        is_show = media_type == "episode" and any(part in nk for part in ("show", "tvshow", "series"))
        target = None
        if "imdb" in nk or text.lower().startswith("tt"):
            target = "imdb"
        elif "tmdb" in nk or "themoviedb" in nk:
            target = "tmdb"
        elif "tvdb" in nk or "thetvdb" in nk:
            target = "tvdb"
        if target:
            if media_type == "episode":
                put(f"{target}_show" if is_show else f"{target}_episode", text)
            else:
                put(target, text)
    return ids


def _has_show_ids(ids: Mapping[str, Any]) -> bool:
    return any(str(ids.get(key) or "").strip() for key in ("imdb_show", "tmdb_show", "tvdb_show"))


def _show_ids_from_uniqueids(uniqueid: Any) -> dict[str, str]:
    base = _normalize_uniqueids(uniqueid, "movie")
    return {f"{key}_show": value for key, value in base.items() if key in {"imdb", "tmdb", "tvdb"} and value}


def _item_identity(item: Mapping[str, Any], media_type: str) -> str:
    item_id = item.get("id")
    if item_id not in (None, "", -1):
        return f"id:{item_id}"
    return _file_hash(item.get("file") or item.get("title") or media_type)


def _metadata_from_item(item: Mapping[str, Any], props: Mapping[str, Any]) -> dict[str, Any] | None:
    media_type = str(item.get("type") or "").strip().lower()
    if media_type not in {"movie", "episode"}:
        return None
    if bool(props.get("live")):
        return None

    episode_title = str(item.get("title") or "").strip()
    show_title = str(item.get("showtitle") or "").strip()
    title = (show_title or episode_title) if media_type == "episode" else episode_title
    ids = _normalize_uniqueids(item.get("uniqueid"), media_type)
    duration = _to_int(item.get("duration"))
    return {
        "media_type": media_type,
        "ids": ids,
        "title": title or None,
        "episode_title": episode_title or None,
        "year": _to_int(item.get("year")),
        "season": _to_int(item.get("season")) if media_type == "episode" else None,
        "number": _to_int(item.get("episode")) if media_type == "episode" else None,
        "item_identity": _item_identity(item, media_type),
        "duration_ms": duration * 1000 if duration and duration > 0 else None,
    }


class KodiWatchService:
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
        self._instance_id = str(instance_id or "default").strip() or "default"
        self._dispatch = dispatcher or Dispatcher(list(sinks or []), cfg_provider=self._active_cfg)
        self._base_poll = _clamp_float(poll_secs, BASE_POLL_SECONDS, MIN_POLL_SECONDS, MAX_BASE_POLL_SECONDS)
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None
        self._sessions: dict[str, dict[str, Any]] = {}
        self._player_session: dict[int, str] = {}
        self._empty_success_polls = 0
        self._idle_poll = self._base_poll
        self._profile: tuple[float, str | None] = (0.0, None)
        self._last_error: tuple[str, float] = ("", 0.0)
        self._last_filter: dict[str, float] = {}
        self._quiet_startup = bool(quiet_startup)
        self._show_ids_cache: dict[int, dict[str, str] | None] = {}
        self._offline = False
        self._offline_failures = 0
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS

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

    def _kodi_cfg(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        return _dict(_dict(cfg).get("kodi"))

    def _configured(self, cfg: Mapping[str, Any]) -> bool:
        kodi = self._kodi_cfg(cfg)
        return bool(str(kodi.get("server") or "").strip() and kodi.get("connection_verified") is True)

    def _rpc(self, method: str, params: Mapping[str, Any] | None = None) -> Any:
        cfg = self._active_cfg()
        kodi = self._kodi_cfg(cfg)
        timeout = float(kodi.get("timeout", 6.0) or 6.0)
        if self._offline or self._offline_failures:
            timeout = min(timeout, OFFLINE_TIMEOUT_SECONDS)
        return jsonrpc_call(
            str(kodi.get("server") or ""),
            method,
            params=params,
            username=str(kodi.get("username") or "").strip(),
            password=str(kodi.get("password") or ""),
            verify_ssl=bool(kodi.get("verify_ssl", False)),
            timeout=timeout,
        )

    def _log_error_limited(self, key: str, message: str) -> None:
        now = time.time()
        last_key, last_ts = self._last_error
        if key != last_key or (now - last_ts) >= 30.0:
            _log(message, "WARNING")
            self._last_error = (key, now)

    def _mark_offline(self, exc: Exception) -> None:
        self._offline_failures += 1
        if not self._offline:
            self._offline = True
            self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
            _log(f"Kodi watcher offline: {exc}; retrying with backoff", "WARNING")
            return
        self._offline_retry = min(OFFLINE_MAX_RETRY_SECONDS, max(OFFLINE_INITIAL_RETRY_SECONDS, self._offline_retry * 2.0))

    def _mark_online(self) -> None:
        if self._offline:
            _log("Kodi watcher reconnected", "INFO")
        self._offline = False
        self._offline_failures = 0
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
        self._last_error = ("", 0.0)

    def _log_filter_limited(self, key: str, message: str) -> None:
        now = time.time()
        last_ts = float(self._last_filter.get(key) or 0.0)
        if (now - last_ts) >= 30.0:
            _log(message, "DEBUG")
            self._last_filter[key] = now

    def _current_profile(self) -> str | None:
        now = time.time()
        ts, cached = self._profile
        if cached and (now - ts) < PROFILE_CACHE_SECONDS:
            return cached
        try:
            result = self._rpc("Profiles.GetCurrentProfile")
            profile = str(_dict(result).get("label") or _dict(result).get("profile") or "").strip()
            self._profile = (now, profile or None)
        except Exception:
            if not cached:
                self._profile = (now, None)
        return self._profile[1]

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

    def _event(self, session: Mapping[str, Any], action: str, progress: float, props: Mapping[str, Any]) -> ScrobbleEvent:
        meta = _dict(session.get("meta"))
        duration_ms = _to_int(session.get("duration_ms"))
        position_seconds = _time_parts_to_seconds(_dict(props).get("time"))
        position_ms = int(position_seconds * 1000) if position_seconds is not None else None
        if position_ms is None and duration_ms and duration_ms > 0:
            position_ms = round((float(progress or 0.0) / 100.0) * float(duration_ms))
        raw = {
            "provider": "kodi",
            "provider_instance": self._instance_id,
            "playerid": session.get("playerid"),
            "item": session.get("item"),
            "properties": dict(props or {}),
            "profile": session.get("account"),
            "duration_ms": session.get("duration_ms"),
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

    def _tvshow_details(self, tvshow_id: int) -> dict[str, Any] | None:
        if tvshow_id <= 0:
            return None
        cached = self._show_ids_cache.get(tvshow_id)
        if cached is not None:
            return {"uniqueid": cached}
        if tvshow_id in self._show_ids_cache:
            return None
        try:
            result = self._rpc("VideoLibrary.GetTVShowDetails", {"tvshowid": tvshow_id, "properties": ["uniqueid", "title", "year"]})
            details = _dict(_dict(result).get("tvshowdetails"))
            ids = _show_ids_from_uniqueids(details.get("uniqueid"))
            self._show_ids_cache[tvshow_id] = ids or None
            return details if ids else None
        except Exception:
            self._show_ids_cache[tvshow_id] = None
            return None

    def _episode_tvshow_id(self, item: Mapping[str, Any]) -> int | None:
        tvshow_id = _to_int(item.get("tvshowid"))
        if tvshow_id:
            return tvshow_id
        episode_id = _to_int(item.get("id"))
        if not episode_id:
            return None
        try:
            result = self._rpc("VideoLibrary.GetEpisodeDetails", {"episodeid": episode_id, "properties": ["tvshowid"]})
            return _to_int(_dict(_dict(result).get("episodedetails")).get("tvshowid"))
        except Exception:
            return None

    def _enrich_episode_show_ids(self, item: Mapping[str, Any], meta: dict[str, Any]) -> None:
        if meta.get("media_type") != "episode":
            return
        ids = meta.get("ids")
        if not isinstance(ids, dict) or _has_show_ids(ids):
            return
        tvshow_id = self._episode_tvshow_id(item)
        if not tvshow_id:
            return
        details = self._tvshow_details(tvshow_id)
        show_ids = _show_ids_from_uniqueids(_dict(details).get("uniqueid")) if details else {}
        ids.update(show_ids)
        if details:
            if not meta.get("title"):
                meta["title"] = str(details.get("title") or "").strip() or meta.get("title")
            if meta.get("year") is None:
                meta["year"] = _to_int(details.get("year"))

    def _dispatch_event(self, ev: ScrobbleEvent, duration_ms: int | None = None) -> bool:
        accepted = bool(self._dispatch.dispatch(ev))
        if accepted:
            try:
                _cw_update("kodi", ev, duration_ms=duration_ms, provider_instance=self._instance_id)
            except Exception:
                pass
            _log(f"event {ev.action} {ev.media_type} user={mask_account(ev.account)} p={ev.progress:.1f} sess={ev.session_key}", "DEBUG")
        elif ev.action == "stop":
            try:
                _cw_update_payload(
                    "kodi",
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

    def _create_session(self, player: Mapping[str, Any], item: Mapping[str, Any], props: Mapping[str, Any]) -> dict[str, Any] | None:
        meta = _metadata_from_item(item, props)
        if not meta:
            return None
        self._enrich_episode_show_ids(item, meta)
        allowed, reason, allowed_paths, blocked_paths = path_scope_status(self._active_cfg(), "scrobble", item.get("file"), self._instance_id)
        if not allowed:
            item_name = str(meta.get("title") or item.get("title") or "?")
            file_path = str(item.get("file") or "none")
            scope_kind = "blacklist" if reason == "blocked_library_scope" else "whitelist"
            scope_values = blocked_paths if scope_kind == "blacklist" else allowed_paths
            scope_label = "blocked" if scope_kind == "blacklist" else "allowed"
            self._log_filter_limited(
                f"{reason}:{meta.get('item_identity')}",
                f"event filtered by scrobble {scope_kind}: view={file_path} {scope_label}={scope_values!r} item={item_name}",
            )
            return None
        player_id = int(player.get("playerid") or 0)
        start_ts = time.time()
        server = str(self._kodi_cfg(self._active_cfg()).get("server") or "")
        session_key = f"kodi:{self._instance_id}:{player_id}:{meta['media_type']}:{meta['item_identity']}:{int(start_ts * 1000)}"
        pct, duration_ms = _progress_from_props(props)
        if duration_ms is None:
            duration_ms = meta.get("duration_ms")
        session = {
            "playerid": player_id,
            "session_key": session_key,
            "started_at": start_ts,
            "item_identity": meta["item_identity"],
            "meta": meta,
            "item": dict(item),
            "account": self._current_profile(),
            "server_uuid": _stable_server_uuid(server),
            "last_action": "",
            "state": "",
            "last_progress": pct,
            "last_emitted_progress": None,
            "last_successful_poll": time.time(),
            "duration_ms": duration_ms,
            "seek_pending": False,
        }
        self._sessions[session_key] = session
        self._player_session[player_id] = session_key
        return session

    def _active_players(self) -> list[dict[str, Any]]:
        result = self._rpc("Player.GetActivePlayers")
        if not isinstance(result, list):
            raise KodiAuthError("Kodi returned an invalid active player response", reason="invalid_response")
        return [dict(x) for x in result if isinstance(x, Mapping)]

    def _player_item(self, player_id: int) -> dict[str, Any]:
        result = self._rpc(
            "Player.GetItem",
            {"playerid": player_id, "properties": ["title", "showtitle", "season", "episode", "year", "uniqueid", "file", "duration"]},
        )
        item = _dict(result).get("item")
        if not isinstance(item, Mapping):
            raise KodiAuthError("Kodi returned an invalid player item response", reason="invalid_response")
        return dict(item)

    def _player_props(self, player_id: int) -> dict[str, Any]:
        result = self._rpc(
            "Player.GetProperties",
            {"playerid": player_id, "properties": ["speed", "time", "totaltime", "percentage", "live"]},
        )
        if not isinstance(result, Mapping):
            raise KodiAuthError("Kodi returned an invalid player properties response", reason="invalid_response")
        return dict(result)

    def _maybe_dispatch_progress(self, session: dict[str, Any], props: Mapping[str, Any]) -> None:
        pct, duration_ms = _progress_from_props(props)
        speed = _to_int(props.get("speed")) or 0
        state = "playing" if speed > 0 else "paused"
        session["last_successful_poll"] = time.time()
        session["last_progress"] = pct
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
                last_emit = float(prev_progress if prev_progress is not None else pct)
            except Exception:
                last_emit = pct
            jump = abs(pct - last_emit)
            if jump >= self._progress_step():
                emit_action = "start"
                session["seek_pending"] = bool(jump >= SEEK_JUMP_PERCENT)

        session["state"] = state
        if emit_action:
            emit_progress = max(1.0, pct) if emit_action == "start" else pct
            ev = self._event(session, emit_action, pct, props)
            if emit_progress != pct:
                ev = ScrobbleEvent(**{**ev.__dict__, "progress": emit_progress})
            if self._dispatch_event(ev, session.get("duration_ms")):
                session["last_action"] = emit_action
                session["last_emitted_progress"] = emit_progress
            session["seek_pending"] = False

    def _stop_missing_sessions(self) -> None:
        for sk, session in list(self._sessions.items()):
            pct = float(session.get("last_progress") or 0.0)
            ev = self._event(session, "stop", pct, _dict(session.get("last_properties")))
            self._dispatch_event(ev, session.get("duration_ms"))
            self._sessions.pop(sk, None)
        self._player_session.clear()

    def _tick(self) -> bool:
        cfg = self._active_cfg()
        if not self._configured(cfg):
            return False

        try:
            players = self._active_players()
        except Exception as exc:
            self._mark_offline(exc)
            return bool(self._sessions)

        self._mark_online()
        video_players = [p for p in players if str(p.get("type") or "").lower() == "video" and p.get("playerid") is not None]
        if not video_players:
            self._empty_success_polls += 1
            if self._empty_success_polls >= 2 and self._sessions:
                self._stop_missing_sessions()
            return False

        self._empty_success_polls = 0
        active_supported = False
        seen_player_ids: set[int] = set()

        for player in video_players:
            player_id = int(player.get("playerid") or 0)
            seen_player_ids.add(player_id)
            sk = self._player_session.get(player_id)
            session = self._sessions.get(sk or "")
            try:
                if session is None:
                    item = self._player_item(player_id)
                    props = self._player_props(player_id)
                    session = self._create_session(player, item, props)
                    if session is None:
                        continue
                    active_supported = True
                    self._maybe_dispatch_progress(session, props)
                    session["last_properties"] = props
                else:
                    props = self._player_props(player_id)
                    if bool(props.get("live")):
                        continue
                    active_supported = True
                    self._maybe_dispatch_progress(session, props)
                    session["last_properties"] = props
            except Exception as exc:
                self._log_error_limited(f"{type(exc).__name__}:{player_id}", f"Kodi player {player_id} poll failed: {exc}")
                continue

        for player_id, sk in list(self._player_session.items()):
            if player_id not in seen_player_ids:
                self._player_session.pop(player_id, None)
        return active_supported

    def start(self) -> None:
        cfg = self._active_cfg()
        if not source_enabled(cfg, "watcher"):
            if not self._quiet_startup:
                _log("Watcher source is disabled; Kodi watcher not started.", "INFO")
            return
        if not self._configured(cfg):
            if not self._quiet_startup:
                _log("Kodi is not connected; watcher not started.", "WARNING")
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
                self._idle_poll = min(MAX_IDLE_POLL_SECONDS, max(self._base_poll, self._idle_poll + 0.75))
            self._stop.wait(self._idle_poll)

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._stop.clear()
        self._bg = threading.Thread(target=self.start, name=f"KodiWatch:{self._instance_id}", daemon=True)
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
) -> KodiWatchService:
    return KodiWatchService(sinks=sinks, dispatcher=dispatcher, cfg_provider=cfg_provider, instance_id=instance_id)


WatchService = KodiWatchService

__all__ = ["KodiWatchService", "WatchService", "make_default_watch"]

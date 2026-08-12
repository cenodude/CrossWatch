# providers/scrobble/scrob/sink.py
# CrossWatch - Scrobble Scrob Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Callable, Mapping
from typing import Any

import requests

from cw_platform.config_base import load_config
from cw_platform.event_archive import record_watch
from cw_platform.local_db.ttl_dedupe import once_per_ttl
from cw_platform.provider_instances import get_provider_block, normalize_instance_id
from services.activity import record_scrobble_event

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
from providers.scrobble._watched_gate import resolve_stop_action

try:
    from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink, mask_account  # type: ignore
except ImportError:
    class ScrobbleSink:  # pragma: no cover
        def send(self, event: Any) -> None: ...

    class ScrobbleEvent:  # pragma: no cover
        ...

    def mask_account(value: Any) -> str:  # pragma: no cover
        s = str(value or "").strip()
        if not s:
            return "unknown"
        return s[0] + "*" if len(s) <= 2 else s[:2] + "***"

from providers.sync.scrob._common import (
    PATH_WEBHOOK_KODI,
    PATH_WEBHOOK_JELLYFIN,
    as_int,
    ok_status,
    safe_json,
    webhook_post,
)

APP_AGENT = "CrossWatch/Watcher/1.0"
DEFAULT_WATCHED_AT = 90.0
SESSION_TTL_SEC = 6 * 3600
SESSION_PREFIX = "crosswatch"
_AR_TTL = 60

_STATE_LOCK = threading.Lock()
_SESSIONS: dict[str, dict[str, Any]] = {}


def is_crosswatch_session(session_key: Any) -> bool:
    return f":{SESSION_PREFIX}-" in str(session_key or "")


def _route_source(cfg: Mapping[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, Mapping) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


def _ar_seen(key: str) -> bool:
    try:
        return not once_per_ttl(None, "auto_remove_seen", key, ttl_seconds=_AR_TTL)
    except Exception:
        return False


def _norm_media_type(value: str) -> str:
    text = (value or "").strip().lower()
    if text.endswith("s"):
        text = text[:-1]
    return "show" if text in ("serie", "series") else text


def _auto_remove_enabled(cfg: Mapping[str, Any], media_type: str) -> bool:
    scrobble = (cfg.get("scrobble") or {}) if isinstance(cfg, Mapping) else {}
    watch = scrobble.get("watch") or {}
    route_opts = watch.get("route_options") if isinstance(watch.get("route_options"), Mapping) else {}
    mode = str((route_opts or {}).get("auto_remove_watchlist") or "inherit").strip().lower()
    if mode == "off":
        return False
    if not scrobble.get("delete_plex") and mode != "on":
        return False
    types = scrobble.get("delete_plex_types") or []
    mt = _norm_media_type(media_type)
    if isinstance(types, str):
        return _norm_media_type(types) == mt
    try:
        return mt in {_norm_media_type(str(x)) for x in types if str(x).strip()}
    except Exception:
        return False


def _auto_remove_across(event: Any, cfg: Mapping[str, Any], scope: str = "") -> None:
    media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
    if not _auto_remove_enabled(cfg, media_type):
        return
    ids = {k: str(v) for k, v in (getattr(event, "ids", None) or {}).items() if v}
    if not ids:
        return
    key = f"{scope}|{media_type}|" + ",".join(f"{k}={v}" for k, v in sorted(ids.items()))
    if _ar_seen(key):
        return
    try:
        _rm_across(ids, media_type, scope=scope)
    except Exception:
        pass


def _cfg() -> dict[str, Any]:
    try:
        return load_config() or {}
    except Exception:
        return {}


def _is_debug() -> bool:
    try:
        return bool((_cfg().get("runtime") or {}).get("debug"))
    except Exception:
        return False


def _log(msg: str, lvl: str = "INFO") -> None:
    level = (str(lvl) or "INFO").upper()
    if level == "DEBUG" and not _is_debug():
        return
    if BASE_LOG is not None:
        try:
            BASE_LOG(msg, level=level, module="SCROBBLE")
            return
        except Exception:
            pass
    print(f"[SCROBBLE] {level}: {msg}")


def _prune_sessions(now: float) -> None:
    stale = [k for k, v in _SESSIONS.items() if now - float(v.get("seen", 0.0)) > SESSION_TTL_SEC]
    for k in stale:
        _SESSIONS.pop(k, None)


def _session_scope(event: Any, instance: str) -> str:
    parts = [
        str(getattr(event, "server_uuid", "") or ""),
        str(getattr(event, "session_key", "") or ""),
    ]
    scope = ":".join(p for p in parts if p)
    if not scope:
        ids = getattr(event, "ids", None) or {}
        seed = "|".join(f"{k}={v}" for k, v in sorted(ids.items()) if v)
        seed = seed or str(getattr(event, "title", "") or "")
        scope = hashlib.sha256(seed.encode("utf-8")).hexdigest()[:24]
    return f"{instance}:{scope}"


def _session_id(scope: str) -> str:
    digest = hashlib.sha256(scope.encode("utf-8")).hexdigest()[:20]
    return f"{SESSION_PREFIX}-{digest}"


class _Adapter:
    def __init__(self, cfg: dict[str, Any], instance_id: str, session: requests.Session) -> None:
        self.config = cfg
        self.instance_id = instance_id
        self.session = session


class ScrobSink(ScrobbleSink):
    name = "scrob"

    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: Any = None) -> None:
        self._cfg_provider = cfg_provider
        self.instance_id = normalize_instance_id(instance_id)
        self.session = requests.Session()
        try:
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers.setdefault("User-Agent", APP_AGENT)
        except Exception:
            pass

    @property
    def config(self) -> dict[str, Any]:
        if self._cfg_provider is not None:
            try:
                cfg = self._cfg_provider()
                if isinstance(cfg, Mapping):
                    return dict(cfg)
            except Exception:
                pass
        return _cfg()

    def _block(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        try:
            return get_provider_block(cfg, "scrob", self.instance_id) or {}
        except Exception:
            block = cfg.get("scrob") if isinstance(cfg, Mapping) else None
            return dict(block) if isinstance(block, Mapping) else {}

    def _watched_at(self, cfg: Mapping[str, Any]) -> float:
        try:
            sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
            value = ((sc or {}).get("scrob") or {}).get("watched_at")
            if value is None:
                value = ((sc or {}).get("trakt") or {}).get("watched_at", DEFAULT_WATCHED_AT)
            return max(0.0, min(100.0, float(value)))
        except Exception:
            return DEFAULT_WATCHED_AT

    def _kodi_item(self, event: Any, media_type: str) -> dict[str, Any] | None:
        ids = {str(k): str(v) for k, v in (getattr(event, "ids", None) or {}).items() if v}

        def pick(*names: str) -> dict[str, str]:
            out: dict[str, str] = {}
            for name in names:
                value = ids.get(name)
                if value:
                    out[name.split("_", 1)[0]] = value
            return out

        if media_type == "episode":
            season = as_int(getattr(event, "season", None))
            number = as_int(getattr(event, "number", None))
            if season is None or number is None:
                return None
            unique = pick("tmdb_show", "tvdb_show", "imdb_show")
            if not unique:
                unique = pick("tmdb", "tvdb", "imdb")
            if not unique:
                return None
            item: dict[str, Any] = {
                "type": "episode",
                "season": season,
                "episode": number,
                "uniqueid": unique,
            }
            title = str(getattr(event, "title", "") or "").strip()
            if title:
                item["showtitle"] = title
                item["title"] = title
            return item

        unique = pick("tmdb", "imdb", "tvdb")
        if not unique:
            return None
        item = {"type": "movie", "uniqueid": unique}
        title = str(getattr(event, "title", "") or "").strip()
        if title:
            item["title"] = title
        year = as_int(getattr(event, "year", None))
        if year:
            item["year"] = year
        return item

    def _resolve_action(self, scope: str, action: str, now: float) -> tuple[str, str, dict[str, Any] | None]:
        with _STATE_LOCK:
            _prune_sessions(now)
            state = _SESSIONS.get(scope)
            before = dict(state) if state is not None else None
            if state is None:
                state = {"paused": False, "started": False, "seen": now}
                _SESSIONS[scope] = state
            state["seen"] = now

            if action == "stop":
                _SESSIONS.pop(scope, None)
                return "stop", _session_id(scope), before
            if action == "pause":
                if not state.get("started"):
                    _SESSIONS.pop(scope, None)
                    return "stop", _session_id(scope), before
                state["paused"] = True
                return "pause", _session_id(scope), before
            if state.get("paused"):
                state["paused"] = False
                return "resume", _session_id(scope), before
            if state.get("started"):
                return "progress", _session_id(scope), before
            state["started"] = True
            return "start", _session_id(scope), before

    def _rollback(self, scope: str, before: dict[str, Any] | None) -> None:
        with _STATE_LOCK:
            if before is None:
                _SESSIONS.pop(scope, None)
            else:
                _SESSIONS[scope] = before

    def send(self, event: Any) -> None:
        cfg = self.config
        block = self._block(cfg)
        if not (str(block.get("server_url") or "").strip() and str(block.get("api_key") or "").strip()):
            _log("SCROB: skip scrobble, not connected", "DEBUG")
            return

        raw_action = str(getattr(event, "action", "") or "").strip().lower()
        if raw_action not in ("start", "pause", "stop"):
            return

        media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
        item = self._kodi_item(event, media_type)
        if item is None:
            _log("SCROB: skip scrobble, no supported ids", "DEBUG")
            return

        try:
            progress_pct = max(0.0, min(100.0, float(getattr(event, "progress", 0.0) or 0.0)))
        except Exception:
            progress_pct = 0.0

        watched_at = self._watched_at(cfg)
        watched = raw_action == "stop" and resolve_stop_action(progress_pct, watched_at) == "stop"

        now = time.time()
        scope = _session_scope(event, self.instance_id)
        action, session_id, before = self._resolve_action(scope, raw_action, now)

        duration_ms = as_int(getattr(event, "duration_ms", None))
        position_ms = as_int(getattr(event, "position_ms", None))
        total_seconds = int(round(duration_ms / 1000.0)) if duration_ms and duration_ms > 0 else 0
        if position_ms is not None and position_ms >= 0:
            position_seconds = int(round(position_ms / 1000.0))
        elif total_seconds:
            position_seconds = int(round(total_seconds * (progress_pct / 100.0)))
        else:
            position_seconds = 0
        if not total_seconds and position_seconds and progress_pct > 0:
            total_seconds = int(round(position_seconds / (progress_pct / 100.0)))

        payload: dict[str, Any] = {
            "item": item,
            "position_seconds": position_seconds,
            "total_seconds": total_seconds,
            "session_id": session_id,
        }

        if action == "stop":
            payload["method"] = "Player.OnStop"
            payload["params"] = {"data": {"end": bool(watched)}}
        else:
            payload["event"] = {
                "start": "playback_started",
                "resume": "playback_resumed",
                "pause": "playback_paused",
                "progress": "playback_seeked",
            }[action]

        self._post(cfg, event, action, payload, progress_pct, watched=watched, scope=scope, before=before)

    def _post(
        self,
        cfg: Mapping[str, Any],
        event: Any,
        action: str,
        payload: Mapping[str, Any],
        progress_pct: float,
        *,
        watched: bool = False,
        scope: str = "",
        before: dict[str, Any] | None = None,
    ) -> None:
        adapter = _Adapter(dict(cfg), self.instance_id, self.session)
        src, src_inst = _route_source(cfg)
        archived = action in ("start", "stop")
        title = str(getattr(event, "title", "") or "")

        try:
            resp = webhook_post(adapter, PATH_WEBHOOK_KODI, payload)
        except Exception as exc:
            reason = exc.__class__.__name__
            _log(f"SCROB: scrobble {action} failed ({reason})", "WARN")
            if scope:
                self._rollback(scope, before)
            if archived:
                self._archive(event, action, src, src_inst, progress_pct, status="fail", reason=reason)
            return

        body = safe_json(resp)
        status = str((body or {}).get("status") or "").lower() if isinstance(body, Mapping) else ""
        if not ok_status(resp) or status not in ("ok", ""):
            reason = str((body or {}).get("reason") or "") if isinstance(body, Mapping) else ""
            reason = reason or f"http:{int(getattr(resp, 'status_code', 0) or 0)}"
            _log(f"SCROB: scrobble {action} rejected status={int(getattr(resp, 'status_code', 0) or 0)} reason={reason}", "WARN")
            if scope:
                self._rollback(scope, before)
            if archived:
                self._archive(event, action, src, src_inst, progress_pct, status="fail", reason=reason)
            return

        if archived:
            self._archive(event, action, src, src_inst, progress_pct)

        if action == "stop" and watched:
            try:
                record_scrobble_event(
                    event,
                    source=src,
                    source_instance=src_inst,
                    target="scrob",
                    target_instance=self.instance_id,
                    progress=progress_pct,
                )
            except Exception:
                pass
            _auto_remove_across(event, cfg, scope=f"scrob:{self.instance_id}")

        _log(
            f"SCROB: scrobble {action} user='{mask_account(getattr(event, 'account', None))}' "
            f"p={progress_pct:.1f}% media={title!r}",
            "DEBUG" if action == "progress" else "INFO",
        )

    def send_watched_state(self, event: Any, *, watched: bool) -> bool:
        cfg = self.config
        block = self._block(cfg)
        if not (str(block.get("server_url") or "").strip() and str(block.get("api_key") or "").strip()):
            return False

        media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
        ids = {str(k): str(v) for k, v in (getattr(event, "ids", None) or {}).items() if v}
        tmdb = ids.get("tmdb")
        payload: dict[str, Any] = {
            "NotificationType": "UserDataSaved",
            "SaveReason": "TogglePlayed",
            "Played": bool(watched),
            "ItemType": "Episode" if media_type == "episode" else "Movie",
            "Name": str(getattr(event, "title", "") or ""),
        }
        if tmdb:
            payload["Provider_tmdb"] = str(tmdb)
        year = as_int(getattr(event, "year", None))
        if year:
            payload["Year"] = year
        if media_type == "episode":
            season = as_int(getattr(event, "season", None))
            number = as_int(getattr(event, "number", None))
            if season is None or number is None:
                return False
            payload["SeasonNumber"] = season
            payload["EpisodeNumber"] = number
            payload["SeriesName"] = str(getattr(event, "title", "") or "")
        elif not tmdb:
            return False

        adapter = _Adapter(dict(cfg), self.instance_id, self.session)
        try:
            resp = webhook_post(adapter, PATH_WEBHOOK_JELLYFIN, payload)
        except Exception as exc:
            _log(f"SCROB: watched state delivery failed ({exc.__class__.__name__})", "WARN")
            return False

        body = safe_json(resp)
        status = str((body or {}).get("status") or "").lower() if isinstance(body, Mapping) else ""
        if ok_status(resp) and status == "ok":
            _log(f"SCROB: marked {'watched' if watched else 'unwatched'} media={str(getattr(event, 'title', '') or '')!r}", "INFO")
            return True
        _log(f"SCROB: watched state rejected status={int(getattr(resp, 'status_code', 0) or 0)}", "WARN")
        return False

    def _archive(
        self,
        event: Any,
        action: str,
        src: str,
        src_inst: str,
        progress_pct: float,
        *,
        status: str = "",
        reason: str = "",
    ) -> None:
        try:
            extra: dict[str, Any] = {}
            if status:
                extra["status"] = status
            if reason:
                extra["reason"] = reason
            record_watch(
                event,
                action=action,
                source_provider=src,
                source_instance=src_inst,
                destination_provider="scrob",
                destination_instance=self.instance_id,
                progress=progress_pct,
                **extra,
            )
        except Exception:
            pass


__all__ = ["ScrobSink", "is_crosswatch_session"]

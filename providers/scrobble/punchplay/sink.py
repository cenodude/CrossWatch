# providers/scrobble/punchplay/sink.py
# CrossWatch - Scrobble PunchPlay Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import threading
import time
import uuid
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

from providers.sync.punchplay._common import (
    URL_PLAYBACK,
    error_of,
    punchplay_request,
    request_id_of,
)

APP_AGENT = "CrossWatch/Watcher/1.0"
DEFAULT_WATCHED_AT = 90.0
SESSION_TTL_SEC = 6 * 3600
_AR_TTL = 60

_STATE_LOCK = threading.Lock()
_SESSIONS: dict[str, dict[str, Any]] = {}


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
    return "show" if text == "serie" or text == "series" else text


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


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(str(value).strip())
    except Exception:
        return None


def _imdb(value: Any) -> str | None:
    text = str(value or "").strip().lower()
    if not text:
        return None
    if not text.startswith("tt"):
        text = f"tt{text.lstrip('t')}"
    rest = text[2:]
    return text if rest.isdigit() and rest else None


def _prune_sessions(now: float) -> None:
    stale = [k for k, v in _SESSIONS.items() if now - float(v.get("seen", 0.0)) > SESSION_TTL_SEC]
    for k in stale:
        _SESSIONS.pop(k, None)


def _media_fingerprint(event: Any) -> str:
    ids = getattr(event, "ids", None) or {}
    seed = "|".join(f"{k}={v}" for k, v in sorted(ids.items()) if v)
    seed = seed or str(getattr(event, "title", "") or "")
    if str(getattr(event, "media_type", "") or "").lower() == "episode":
        seed = f"{seed}|s{getattr(event, 'season', None)}e{getattr(event, 'number', None)}"
    return hashlib.sha256(seed.encode("utf-8")).hexdigest()[:16]


def _session_scope(event: Any, instance: str) -> str:
    parts = [
        str(getattr(event, "server_uuid", "") or ""),
        str(getattr(event, "session_key", "") or ""),
    ]
    scope = ":".join(p for p in parts if p)
    fingerprint = _media_fingerprint(event)
    return f"{instance}:{scope}:{fingerprint}" if scope else f"{instance}:{fingerprint}"


class PunchPlaySink(ScrobbleSink):
    name = "punchplay"

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
            return get_provider_block(cfg, "punchplay", self.instance_id) or {}
        except Exception:
            block = cfg.get("punchplay") if isinstance(cfg, Mapping) else None
            return dict(block) if isinstance(block, Mapping) else {}

    def _watched_at(self, cfg: Mapping[str, Any]) -> float:
        try:
            sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
            value = ((sc or {}).get("punchplay") or {}).get("watched_at")
            if value is None:
                value = ((sc or {}).get("trakt") or {}).get("watched_at", DEFAULT_WATCHED_AT)
            return max(0.0, min(100.0, float(value)))
        except Exception:
            return DEFAULT_WATCHED_AT

    def _ids_payload(self, event: Any, media_type: str) -> dict[str, Any]:
        ids = getattr(event, "ids", None) or {}
        if media_type == "episode":
            ids = {k[: -len("_show")]: v for k, v in ids.items() if k.endswith("_show") and v}
        out: dict[str, Any] = {}
        tmdb = _as_int(ids.get("tmdb"))
        if tmdb and tmdb > 0:
            out["tmdb_id"] = tmdb
        imdb = _imdb(ids.get("imdb"))
        if imdb:
            out["imdb_id"] = imdb
        tvdb = _as_int(ids.get("tvdb"))
        if tvdb and tvdb > 0:
            out["tvdb_id"] = tvdb
        return out

    def _resolve_action(self, scope: str, action: str, now: float) -> tuple[str, str, dict[str, Any] | None]:
        with _STATE_LOCK:
            _prune_sessions(now)
            state = _SESSIONS.get(scope)
            before = dict(state) if state is not None else None
            if state is None:
                state = {"session_id": str(uuid.uuid4()), "paused": False, "started": False, "seen": now}
                _SESSIONS[scope] = state
            state["seen"] = now
            session_id = str(state["session_id"])

            if action == "stop" or (action == "pause" and not state.get("started")):
                _SESSIONS.pop(scope, None)
                return "stop", session_id, before
            if action == "pause":
                state["paused"] = True
                return "pause", session_id, before
            if state.get("paused"):
                state["paused"] = False
                return "resume", session_id, before
            if state.get("started"):
                return "progress", session_id, before
            state["started"] = True
            return "start", session_id, before

    def _rollback(self, scope: str, before: dict[str, Any] | None) -> None:
        with _STATE_LOCK:
            if before is None:
                _SESSIONS.pop(scope, None)
            else:
                _SESSIONS[scope] = before

    def send(self, event: Any) -> None:
        cfg = self.config
        block = self._block(cfg)
        if not str(block.get("access_token") or "").strip():
            _log("PUNCHPLAY: skip scrobble, not connected", "DEBUG")
            return

        raw_action = str(getattr(event, "action", "") or "").strip().lower()
        if raw_action not in ("start", "pause", "stop"):
            return

        media_type = "episode" if str(getattr(event, "media_type", "") or "").lower() == "episode" else "movie"
        ids_payload = self._ids_payload(event, media_type)
        if not ids_payload:
            _log("PUNCHPLAY: skip scrobble, no supported ids", "DEBUG")
            return

        title = str(getattr(event, "title", "") or "").strip()
        if not title:
            _log("PUNCHPLAY: skip scrobble, missing title", "DEBUG")
            return

        season = number = None
        if media_type == "episode":
            season = _as_int(getattr(event, "season", None))
            number = _as_int(getattr(event, "number", None))
            if season is None or number is None:
                _log("PUNCHPLAY: skip scrobble, episode missing season/episode", "DEBUG")
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

        payload: dict[str, Any] = dict(ids_payload)
        payload["media_type"] = media_type
        payload["event_id"] = str(uuid.uuid4())
        payload["playback_session_id"] = session_id
        payload["event_created_at"] = int(now * 1000)
        payload["client_version"] = APP_AGENT
        payload["title"] = title

        year = _as_int(getattr(event, "year", None))
        if year:
            payload["year"] = year

        if media_type == "episode":
            payload["season"] = season
            payload["episode"] = number

        position_ms = _as_int(getattr(event, "position_ms", None))
        duration_ms = _as_int(getattr(event, "duration_ms", None))
        if duration_ms and duration_ms > 0:
            payload["duration_seconds"] = int(round(duration_ms / 1000.0))
        if position_ms is not None and position_ms >= 0:
            payload["position_seconds"] = int(round(position_ms / 1000.0))
        elif duration_ms and duration_ms > 0:
            payload["position_seconds"] = int(round(duration_ms * (progress_pct / 100.0) / 1000.0))

        payload["progress"] = round(progress_pct / 100.0, 4)

        device_id = str(block.get("device_id") or "").strip()
        if device_id:
            payload["device_id"] = device_id

        if action == "stop":
            threshold = round(watched_at / 100.0, 4)
            if threshold > 0.0:
                payload["watched_threshold"] = threshold
            payload["watched"] = watched

        if "position_seconds" not in payload and (
            action in ("pause", "progress") or (action == "stop" and not watched)
        ):
            _log(f"PUNCHPLAY: skip scrobble {action}, no position or duration", "DEBUG")
            self._rollback(scope, before)
            return

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
        title = str(payload.get("title") or "")

        try:
            resp = punchplay_request(adapter, "POST", URL_PLAYBACK.format(action=action), json=dict(payload))
        except Exception as exc:
            reason = exc.__class__.__name__
            _log(f"PUNCHPLAY: scrobble {action} failed ({reason})", "WARN")
            if scope:
                self._rollback(scope, before)
            if archived:
                self._archive(event, action, src, src_inst, progress_pct, status="fail", reason=reason)
            return

        code = int(getattr(resp, "status_code", 0) or 0)
        if not (200 <= code < 300):
            reason = error_of(resp) or f"http:{code}"
            _log(
                f"PUNCHPLAY: scrobble {action} rejected status={code} error={reason} request_id={request_id_of(resp)}",
                "WARN",
            )
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
                    target="punchplay",
                    target_instance=self.instance_id,
                    progress=progress_pct,
                )
            except Exception:
                pass
            _auto_remove_across(event, cfg, scope=f"punchplay:{self.instance_id}")

        _log(
            f"PUNCHPLAY: scrobble {action} user='{mask_account(getattr(event, 'account', None))}' "
            f"p={progress_pct:.1f}% media={title!r}",
            "DEBUG" if action == "progress" else "INFO",
        )

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
                destination_provider="punchplay",
                destination_instance=self.instance_id,
                progress=progress_pct,
                **extra,
            )
        except Exception:
            pass


class _Adapter:
    def __init__(self, cfg: dict[str, Any], instance_id: str, session: requests.Session) -> None:
        self.config = cfg
        self.instance_id = instance_id
        self.session = session


__all__ = ["PunchPlaySink"]

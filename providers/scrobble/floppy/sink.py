# providers/scrobble/floppy/sink.py
# CrossWatch - Floppy watcher sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from typing import Any

from cw_platform.event_archive import record_watch
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from providers.auth._auth_FLOPPY import FloppyAuthError, FloppyClient, is_configured
from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink, mask_account
from providers.sync.floppy._common import api_post
from services.activity import record_scrobble_event
from services.playback_progress.models import utc_now_iso

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


class _Adapter:
    def __init__(self, client: FloppyClient) -> None:
        self.client = client


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="FLOPPY-SCROBBLE")
            return
        except Exception:
            pass
    print(f"[FLOPPY-SCROBBLE:{lvl}] {msg}")


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _clamp(value: Any) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = 0.0
    return max(0.0, min(100.0, raw))


def _watched_at(cfg: Mapping[str, Any]) -> float:
    try:
        sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
        value = ((sc or {}).get("floppy") or {}).get("watched_at")
        if value is None:
            value = ((sc or {}).get("trakt") or {}).get("watched_at", 90.0)
        return max(0.0, min(100.0, float(value)))
    except Exception:
        return 90.0


def _route_source(cfg: Mapping[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, Mapping) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


def _find_int(raw: Any, keys: set[str]) -> int | None:
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            if str(key or "").strip().lower() in keys:
                found = _int(value)
                if found is not None and found >= 0:
                    return found
        for value in raw.values():
            found = _find_int(value, keys)
            if found is not None:
                return found
    if isinstance(raw, list):
        for value in raw:
            found = _find_int(value, keys)
            if found is not None:
                return found
    return None


def _duration_ms(raw: Any) -> int | None:
    value = _find_int(raw, {"duration", "duration_ms", "durationms", "runtime_ms", "totaltime"})
    if value is None or value <= 0:
        return None
    return value * 1000 if value < 10_000 else value


def _position_ms(raw: Any, progress: float, duration_ms: int | None) -> int | None:
    value = _find_int(raw, {"viewoffset", "view_offset", "position", "position_ms", "positionms", "time", "timeoffset"})
    if value is None and duration_ms:
        value = round((progress / 100.0) * float(duration_ms))
    if value is None:
        return None
    if duration_ms and duration_ms > 10_000 and 0 < value < 10_000:
        value *= 1000
    return max(0, min(value, duration_ms)) if duration_ms else max(0, value)


def _ids(ev: ScrobbleEvent) -> dict[str, str]:
    source = ev.ids or {}
    out: dict[str, str] = {}
    for key in ("tmdb", "imdb", "tvdb"):
        lookup = f"{key}_show" if ev.media_type == "episode" else key
        value = str(source.get(lookup) or "").strip()
        if value:
            out[key] = value
    if ev.media_type == "episode" and not out:
        for key in ("tmdb", "imdb", "tvdb"):
            value = str(source.get(key) or "").strip()
            if value:
                out[key] = value
    return out


def _payload(ev: ScrobbleEvent, cfg: Mapping[str, Any]) -> dict[str, Any] | None:
    progress = _clamp(ev.progress)
    ids = _ids(ev)
    if not ids:
        return None
    body: dict[str, Any] = {
        "action": str(ev.action or "").strip().lower(),
        "media_type": "episode" if ev.media_type == "episode" else "movie",
        "ids": ids,
        "title": ev.title,
    }
    if body["media_type"] == "episode":
        season = _int(ev.season)
        episode = _int(ev.number)
        if season is None or episode is None:
            return None
        body["series_title"] = ev.title
        body["season_number"] = season
        body["episode_number"] = episode
    duration = _duration_ms(ev.raw)
    position = _position_ms(ev.raw, progress, duration)
    if duration:
        body["duration_seconds"] = max(1, round(duration / 1000.0))
    if position is not None:
        body["position_seconds"] = max(0, round(position / 1000.0))
    if body["action"] == "stop":
        body["completed"] = progress >= _watched_at(cfg)
        body["played_at"] = utc_now_iso()
    return {k: v for k, v in body.items() if v not in (None, "", {}, [])}


class FloppySink(ScrobbleSink):
    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._completed: set[str] = set()

    def _cfg(self, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        if isinstance(cfg, dict):
            return cfg
        if self._cfg_provider:
            try:
                got = self._cfg_provider()
                if isinstance(got, dict):
                    return got
            except Exception:
                pass
        return {}

    def _view(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        built = build_provider_config_view(dict(cfg or {}), "floppy", self._instance_id)
        block = built.get("floppy") if isinstance(built, Mapping) else None
        return dict(block or {}) if isinstance(block, Mapping) else {}

    def _client(self, view: Mapping[str, Any]) -> FloppyClient:
        return FloppyClient(
            str(view.get("server_url") or ""),
            str(view.get("api_token") or ""),
            verify_ssl=bool(view.get("verify_ssl", False)),
            timeout=float(view.get("timeout", 12.0) or 12.0),
        )

    def _key(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        base = "|".join(f"{k}:{ids[k]}" for k in sorted(ids) if ids.get(k)) or str(ev.title or "")
        if ev.media_type == "episode":
            base = f"{base}|s{_int(ev.season) or 0}e{_int(ev.number) or 0}"
        return f"{ev.session_key or '?'}:{base}"

    def send(self, ev: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
        cfgd = self._cfg(cfg)
        view = self._view(cfgd)
        if not is_configured(view):
            _log(f"Floppy disabled for sink profile {self._instance_id}; skipping", "WARNING")
            return
        body = _payload(ev, cfgd)
        if not body:
            _log("Floppy sink skipped event without enough identity", "DEBUG")
            return
        complete_key = self._key(ev)
        if body.get("action") == "stop" and body.get("completed") is True and complete_key in self._completed:
            return
        src, src_inst = _route_source(cfgd)
        try:
            api_post(_Adapter(self._client(view)), "scrobble", json=body)
        except FloppyAuthError as exc:
            reason = str(getattr(exc, "reason", "") or "request_failed")
            _log(f"scrobble failed status={getattr(exc, 'status_code', None)} reason={reason}", "ERROR")
            if body.get("action") in {"start", "stop"}:
                record_watch(ev, action=str(body.get("action")), source_provider=src, source_instance=src_inst, destination_provider="floppy", destination_instance=self._instance_id, status="fail", progress=_clamp(ev.progress), reason=reason)
            return
        except Exception as exc:
            _log(f"scrobble failed err={exc}", "ERROR")
            return
        progress = _clamp(ev.progress)
        action = str(body.get("action") or ev.action)
        record_watch(ev, action=action, source_provider=src, source_instance=src_inst, destination_provider="floppy", destination_instance=self._instance_id, progress=progress)
        if action == "stop" and body.get("completed") is True:
            self._completed.add(complete_key)
            try:
                record_scrobble_event(ev, source=src, source_instance=src_inst, target="floppy", target_instance=self._instance_id, progress=progress)
            except Exception:
                pass
        _log(f"scrobble {action} user='{mask_account(ev.account)}' p={progress:.1f}% media='{ev.title or '?'}'", "INFO")


__all__ = ["FloppySink"]

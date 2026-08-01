# providers/scrobble/crosswatch/sink.py
# CrossWatch - Local tracker scrobble sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import time
from collections.abc import Callable, Mapping
from typing import Any

from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink, mask_account
from providers.sync._mod_CROSSWATCH import OPS as CROSSWATCH_OPS
from services.activity import record_scrobble_event
from cw_platform.event_archive import record_watch
from services.playback_progress.models import utc_now_iso

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


def _cfg() -> dict[str, Any]:
    try:
        from cw_platform.config_base import load_config

        return load_config()
    except Exception:
        return {}


def _is_debug() -> bool:
    try:
        return bool((_cfg().get("runtime") or {}).get("debug"))
    except Exception:
        return False


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if lvl == "DEBUG" and not _is_debug():
        return
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="CW-SCROBBLE")
            return
        except Exception:
            pass
    print(f"[CW-SCROBBLE:{lvl}] {msg}")


def _clamp(value: Any) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = 0.0
    return max(0.0, min(100.0, raw))


def _int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(value))
    except Exception:
        return None


def _watched_at(cfg: Mapping[str, Any]) -> float:
    try:
        sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
        source = (sc or {}).get("crosswatch") or {}
        value = source.get("watched_at")
        if value is None:
            value = ((sc or {}).get("trakt") or {}).get("watched_at", 90.0)
        return max(0.0, min(100.0, float(value)))
    except Exception:
        return 90.0


def _progress_step(cfg: Mapping[str, Any]) -> float:
    try:
        sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
        source = (sc or {}).get("crosswatch") or {}
        value = source.get("progress_step")
        if value is None:
            value = ((sc or {}).get("trakt") or {}).get("progress_step", 5)
        return max(1.0, min(25.0, float(value)))
    except Exception:
        return 5.0


def _slug(value: Any) -> str:
    text = str(value or "").strip().lower()
    text = re.sub(r"[^a-z0-9]+", "-", text).strip("-")
    return text[:80] or "unknown"


def _route_source(cfg: Mapping[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, Mapping) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


def _best_duration_ms(raw: Any) -> int | None:
    keys = {"duration", "duration_ms", "durationms", "runtime_ms", "totaltime"}

    def walk(value: Any) -> int | None:
        if isinstance(value, Mapping):
            if {"hours", "minutes", "seconds"} & set(str(k).lower() for k in value.keys()):
                h = _int(value.get("hours")) or 0
                m = _int(value.get("minutes")) or 0
                s = _int(value.get("seconds")) or 0
                ms = _int(value.get("milliseconds")) or 0
                total = (((h * 60) + m) * 60 + s) * 1000 + ms
                if total > 0:
                    return total
            for key, item in value.items():
                if str(key or "").strip().lower() in keys:
                    number = _int(item)
                    if number and number > 0:
                        return number * 1000 if number < 10_000 else number
            for item in value.values():
                found = walk(item)
                if found:
                    return found
        if isinstance(value, list):
            for item in value:
                found = walk(item)
                if found:
                    return found
        return None

    return walk(raw)


def _ids_for_movie(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}
    return {key: ids[key] for key in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mdblist", "plex", "jellyfin", "emby", "slug") if ids.get(key)}


def _show_ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}
    out: dict[str, Any] = {}
    for key in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mdblist", "plex", "jellyfin", "emby", "slug"):
        show_key = f"{key}_show"
        if ids.get(show_key):
            out[key] = ids[show_key]
    return out


def _tmdb_provider(cfg: Mapping[str, Any]) -> Any | None:
    try:
        from providers.metadata._meta_TMDB import TmdbProvider

        view = dict(cfg or {})
        return TmdbProvider(lambda: view, lambda _cfg: None)
    except Exception:
        return None


def _tmdb_enrich(cfg: Mapping[str, Any], *, media_type: str, ids: Mapping[str, Any], title: str, year: Any = None) -> dict[str, Any]:
    provider = _tmdb_provider(cfg)
    if provider is None:
        return {}
    lookup = {str(k): str(v) for k, v in dict(ids or {}).items() if v not in (None, "")}
    if title:
        lookup.setdefault("title", str(title))
    if year:
        lookup.setdefault("year", str(year))
    if not any(lookup.get(key) for key in ("tmdb", "imdb", "tvdb", "title")):
        return {}
    try:
        detail = provider.fetch(entity="movie" if media_type == "movie" else "tv", ids=lookup, need={"poster": False, "backdrop": False, "ids": True})
    except Exception:
        return {}
    return detail if isinstance(detail, dict) else {}


def _item_from_event(ev: ScrobbleEvent, cfg: Mapping[str, Any], progress: float) -> dict[str, Any] | None:
    now = utc_now_iso()
    media_type = "episode" if ev.media_type == "episode" else "movie"
    duration_ms = _int(getattr(ev, "duration_ms", None)) or _best_duration_ms(ev.raw)
    position_ms = _int(getattr(ev, "position_ms", None))

    if media_type == "movie":
        ids = _ids_for_movie(ev)
        detail = _tmdb_enrich(cfg, media_type="movie", ids=ids, title=ev.title or "", year=ev.year)
        detail_ids = detail.get("ids") if isinstance(detail.get("ids"), Mapping) else {}
        item: dict[str, Any] = {
            "type": "movie",
            "title": detail.get("title") or ev.title,
            "year": detail.get("year") or ev.year,
            "ids": {**ids, **dict(detail_ids or {})},
            "progress_percent": round(progress, 3),
            "progress_at": now,
        }
    else:
        season = _int(ev.season)
        episode = _int(ev.number)
        if season is None or episode is None:
            return None
        show_ids = _show_ids(ev)
        detail = _tmdb_enrich(cfg, media_type="episode", ids=show_ids, title=ev.title or "", year=ev.year)
        detail_ids = detail.get("ids") if isinstance(detail.get("ids"), Mapping) else {}
        show_ids = {**show_ids, **dict(detail_ids or {})}
        series_title = str(detail.get("title") or ev.title or "").strip()
        if not show_ids and series_title:
            show_ids = {"slug": _slug(series_title)}
        item = {
            "type": "episode",
            "title": f"S{season:02d}E{episode:02d}",
            "series_title": series_title or ev.title,
            "year": detail.get("year") or ev.year,
            "season": season,
            "episode": episode,
            "ids": {},
            "show_ids": show_ids,
            "progress_percent": round(progress, 3),
            "progress_at": now,
        }

    if duration_ms and duration_ms > 0:
        item["duration_ms"] = duration_ms
        item["progress_ms"] = max(0, min(duration_ms, position_ms if position_ms is not None else round((progress / 100.0) * float(duration_ms))))
    return {k: v for k, v in item.items() if v not in (None, "", {}, [])}


class CrossWatchSink(ScrobbleSink):
    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._p_sess: dict[tuple[str, str], float] = {}
        self._completed: dict[str, float] = {}

    def _config_view(self, cfg: Mapping[str, Any]) -> dict[str, Any]:
        return build_provider_config_view(dict(cfg or {}), "crosswatch", self._instance_id)

    def _media_key(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        parts: list[str] = []
        for key in ("tmdb", "imdb", "tvdb", "trakt", "simkl", "plex", "jellyfin", "emby"):
            if ids.get(key):
                parts.append(f"{key}:{ids[key]}")
        if ev.media_type == "episode":
            for key in ("tmdb_show", "imdb_show", "tvdb_show", "trakt_show", "simkl_show", "plex_show", "jellyfin_show", "emby_show"):
                if ids.get(key):
                    parts.append(f"{key}:{ids[key]}")
            parts.append(f"S{int(ev.season or 0):02d}E{int(ev.number or 0):02d}")
        if not parts:
            parts.append(f"{ev.media_type}:{ev.title or ''}:{ev.year or ''}:{ev.season or ''}:{ev.number or ''}")
        return "|".join(parts)

    def _should_write_progress(self, ev: ScrobbleEvent, cfg: Mapping[str, Any], progress: float) -> bool:
        key = (str(ev.session_key or "?"), self._media_key(ev))
        previous = self._p_sess.get(key)
        self._p_sess[key] = progress
        if previous is None:
            return progress > 0
        if progress < previous:
            return True
        return (progress - previous) >= _progress_step(cfg)

    def send(self, ev: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
        cfg = cfg or (self._cfg_provider() if self._cfg_provider else None) or _cfg()
        if not isinstance(cfg, dict):
            cfg = {}
        view = self._config_view(cfg)
        try:
            if not CROSSWATCH_OPS.is_configured(view):
                _log(f"CrossWatch tracker disabled for sink profile {self._instance_id}; skipping", "WARNING")
                return
        except Exception:
            return

        action = str(ev.action or "").lower().strip()
        progress = _clamp(ev.progress)
        watched_at = _watched_at(cfg)
        item = _item_from_event(ev, view, progress)
        if not item:
            _log("CrossWatch tracker sink skipped event without enough identity", "DEBUG")
            return

        src, src_inst = _route_source(cfg)
        if action == "start":
            record_watch(ev, action="start", source_provider=src, source_instance=src_inst, destination_provider="crosswatch", destination_instance=self._instance_id, progress=progress)

        media_key = f"{str(ev.session_key or '?')}:{self._media_key(ev)}"
        complete = action == "stop" and progress >= watched_at
        if complete:
            if self._completed.get(media_key, -1.0) >= watched_at:
                return
            item["watched_at"] = utc_now_iso()
            history = CROSSWATCH_OPS.add(view, [item], feature="history", dry_run=False) or {}
            cleanup = CROSSWATCH_OPS.remove(view, [item], feature="progress", dry_run=False) or {}
            ok = bool(history.get("ok")) and bool(cleanup.get("ok"))
            if ok:
                self._completed[media_key] = progress
                record_watch(ev, action="stop", source_provider=src, source_instance=src_inst, destination_provider="crosswatch", destination_instance=self._instance_id, progress=progress)
                try:
                    record_scrobble_event(ev, source=src, source_instance=src_inst, target="crosswatch", target_instance=self._instance_id, progress=progress)
                except Exception:
                    pass
                _log(f"scrobble stop user='{mask_account(ev.account)}' p={progress:.1f}% media='{ev.title or '?'}'", "INFO")
            else:
                record_watch(ev, action="stop", source_provider=src, source_instance=src_inst, destination_provider="crosswatch", destination_instance=self._instance_id, status="fail", progress=progress, reason="crosswatch_history_failed")
            return

        if action in {"start", "pause", "stop"} and self._should_write_progress(ev, cfg, progress):
            result = CROSSWATCH_OPS.add(view, [item], feature="progress", dry_run=False) or {}
            if not result.get("ok"):
                record_watch(ev, action="start", source_provider=src, source_instance=src_inst, destination_provider="crosswatch", destination_instance=self._instance_id, status="fail", progress=progress, reason="crosswatch_progress_failed")
                return
            _log(f"progress {action or 'update'} user='{mask_account(ev.account)}' p={progress:.1f}% media='{ev.title or '?'}'", "DEBUG")


__all__ = ["CrossWatchSink"]

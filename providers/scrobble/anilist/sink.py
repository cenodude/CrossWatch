# providers/scrobble/anilist/sink.py
# CrossWatch - AniList one-way progress sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.event_archive import record_watch
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from providers.scrobble._watched_gate import resolve_stop_action
from providers.scrobble.anime_mapping import _event_item
from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink, mask_account
from providers.sync._mod_ANILIST import OPS, ANILISTAuthError, ANILISTModule
from providers.sync.anilist._progress import apply_progress, resolve_target
from services.activity import record_scrobble_event

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


DEFAULT_WATCHED_AT = 90.0
_DONE_TTL = 6 * 3600
_DONE_MAX = 512
_WRITE_LOCK = threading.Lock()
_DONE: OrderedDict[tuple[str, int, int], float] = OrderedDict()
_WARNED: set[str] = set()


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="ANILIST-SINK")
            return
        except Exception:
            pass
    print(f"[ANILIST-SINK:{lvl}] {msg}")


def _warn_once(key: str, msg: str) -> None:
    if key in _WARNED:
        return
    _WARNED.add(key)
    _log(msg, "WARNING")


def _clamp(value: Any) -> float:
    try:
        raw = float(value)
    except Exception:
        raw = 0.0
    return max(0.0, min(100.0, raw))


def _watched_at(cfg: Mapping[str, Any]) -> float:
    try:
        sc = cfg.get("scrobble") if isinstance(cfg, Mapping) else {}
        value = ((sc or {}).get("anilist") or {}).get("watched_at")
        if value is None:
            value = ((sc or {}).get("trakt") or {}).get("watched_at", DEFAULT_WATCHED_AT)
        return max(0.0, min(100.0, float(value)))
    except Exception:
        return DEFAULT_WATCHED_AT


def _route_source(cfg: Mapping[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, Mapping) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


def _media_name(ev: ScrobbleEvent) -> str:
    title = ev.title or "?"
    if ev.media_type == "episode":
        return f"{title} S{int(ev.season or 0):02d}E{int(ev.number or 0):02d}"
    return f"{title} ({ev.year})" if ev.year else title


def _done_seen(key: tuple[str, int, int]) -> bool:
    now = time.monotonic()
    stamp = _DONE.get(key)
    if stamp is None:
        return False
    if now - stamp > _DONE_TTL:
        _DONE.pop(key, None)
        return False
    return True


def _done_mark(key: tuple[str, int, int]) -> None:
    _DONE[key] = time.monotonic()
    _DONE.move_to_end(key)
    while len(_DONE) > _DONE_MAX:
        _DONE.popitem(last=False)


class AniListSink(ScrobbleSink):
    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)

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

    def send(self, ev: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> dict[str, Any] | None:
        cfgd = self._cfg(cfg)
        view = build_provider_config_view(dict(cfgd or {}), "anilist", self._instance_id)
        if not OPS.is_configured(view):
            _warn_once(f"cfg:{self._instance_id}", f"AniList not connected for sink profile {self._instance_id}; skipping")
            return {"ok": True, "skipped": True, "reason": "not_configured"}
        progress = _clamp(ev.progress)
        if str(ev.action or "").lower() != "stop" or resolve_stop_action(progress, _watched_at(cfgd)) != "stop":
            return {"ok": True, "skipped": True, "reason": "not_watched"}
        mapping_block = cfgd.get("anime_mapping") if isinstance(cfgd.get("anime_mapping"), Mapping) else {}
        if not (mapping_block or {}).get("enabled") or not AnimeMappingService(cfgd).ready():
            _warn_once("mapping", "AniList scrobbling needs Anime ID mapping enabled and installed; skipping")
            return {"ok": True, "skipped": True, "reason": "anime_mapping_unavailable"}
        item = _event_item(ev)
        if item is None:
            return {"ok": True, "skipped": True, "reason": "missing_media_identity"}
        target = resolve_target(cfgd, item)
        if target is None:
            _log(f"no AniList entry for '{_media_name(ev)}'; not anime or unmapped", "DEBUG")
            return {"ok": True, "skipped": True, "reason": "not_mapped"}
        anilist_id, episode = target
        key = (self._instance_id, anilist_id, episode)
        src, src_inst = _route_source(cfgd)
        with _WRITE_LOCK:
            if _done_seen(key):
                return {"ok": True, "skipped": True, "reason": "session_completed"}
            try:
                result = apply_progress(ANILISTModule(view).client, anilist_id, episode)
            except ANILISTAuthError:
                _log("AniList rejected the token; reconnect AniList", "ERROR")
                record_watch(ev, action="stop", source_provider=src, source_instance=src_inst, destination_provider="anilist",
                             destination_instance=self._instance_id, status="fail", progress=progress, reason="unauthorized")
                return {"ok": False, "error": "unauthorized", "retryable": False}
            except Exception as exc:
                _log(f"progress update failed anilist={anilist_id} ep={episode} err={exc}", "ERROR")
                record_watch(ev, action="stop", source_provider=src, source_instance=src_inst, destination_provider="anilist",
                             destination_instance=self._instance_id, status="fail", progress=progress, reason="request_failed")
                return {"ok": False, "error": str(exc), "retryable": True}
            if not result.get("ok"):
                return {"ok": True, "skipped": True, "reason": str(result.get("reason") or "media_not_found")}
            _done_mark(key)
        if result.get("skipped"):
            _log(f"'{_media_name(ev)}' -> anilist:{anilist_id} ep {episode} skipped "
                 f"reason={result.get('reason')} status={result.get('previous_status')} progress={result.get('previous_progress')}", "DEBUG")
            return {"ok": True, "skipped": True, "reason": str(result.get("reason"))}
        record_watch(ev, action="stop", source_provider=src, source_instance=src_inst, destination_provider="anilist",
                     destination_instance=self._instance_id, progress=progress)
        try:
            record_scrobble_event(ev, source=src, source_instance=src_inst, target="anilist", target_instance=self._instance_id, progress=progress)
        except Exception:
            pass
        _log(f"progress user='{mask_account(ev.account)}' media='{_media_name(ev)}' -> anilist:{anilist_id} "
             f"'{result.get('title') or '?'}' {result.get('previous_progress')}->{result.get('progress')}"
             f"/{result.get('total') or '?'} status={result.get('status')}", "INFO")
        return {"ok": True}


__all__ = ["AniListSink"]

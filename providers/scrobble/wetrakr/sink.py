# providers/scrobble/wetrakr/sink.py
# CrossWatch - WeTrakr playback scrobbles and completion handling
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping
from typing import Any
from urllib.parse import quote

from cw_platform.app_version import app_version
from cw_platform.config_base import load_config
from cw_platform.event_archive import record_watch
from cw_platform.provider_instances import normalize_instance_id, resolve_provider_block
from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids
from providers.scrobble._watched_gate import resolve_stop_action
from providers.scrobble.scrobble import ScrobbleEvent, mask_account
from providers.sync._mod_WETRAKR import WETRAKRModule
from providers.sync.wetrakr._common import WeTrakrSyncError, account_key, body_of, identity_tokens, int_value, request, resolve_child, write_lock
from providers.sync.wetrakr._progress import ignored_reason, number, scrobble_ids
from services.activity import record_scrobble_event

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None


def _log(msg: str, lvl: str = "INFO") -> None:
    level = str(lvl or "INFO").upper()
    if level == "DEBUG":
        try:
            if not (load_config().get("runtime") or {}).get("debug"):
                return
        except Exception:
            return
    if BASE_LOG is not None:
        try:
            BASE_LOG(msg, level=level, module="WETRAKR-SINK")
            return
        except Exception:
            pass
    print(f"[WETRAKR-SINK:{level}] {msg}")


def _media_name(event: ScrobbleEvent) -> str:
    title = event.title or "?"
    if event.media_type == "episode":
        return f"{title} S{int(event.season or 0):02d}E{int(event.number or 0):02d}"
    return f"{title} ({event.year})" if event.year else title


def _setting(cfg: Mapping[str, Any], key: str, default: float, *, watch: bool = False) -> float:
    sc = cfg.get("scrobble") or {}
    value = (sc.get("watch" if watch else "trakt") or {}).get(key, default)
    parsed = number(value)
    return parsed if parsed is not None else default


def _item(event: ScrobbleEvent) -> dict[str, Any]:
    item: dict[str, Any] = {"type": event.media_type, "title": event.title, "ids": dict(event.ids)}
    if event.media_type == "episode":
        item.update(show_ids={k[:-5]: v for k, v in event.ids.items() if k.endswith("_show")},
                    season=event.season, episode=event.number)
    elif event.media_type != "movie":
        raise WeTrakrSyncError("unsupported_media_type")
    return item


def _body(item: Mapping[str, Any], progress: float) -> dict[str, Any]:
    body: dict[str, Any] = {"progress": progress, "app_version": app_version()}
    if item["type"] == "episode":
        season, episode = int_value(item.get("season")), int_value(item.get("episode"))
        if season < 0 or episode < 1:
            raise WeTrakrSyncError("invalid_episode_coordinates")
        body.update(show={"ids": scrobble_ids(item.get("show_ids"))}, episode={"season": season, "number": episode})
    else:
        body["movie"] = {"ids": scrobble_ids(item.get("ids"))}
    return body


class WeTrakrSink:
    name = "wetrakr"

    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: Any = None) -> None:
        self._cfg_provider = cfg_provider or load_config
        self.instance_id = normalize_instance_id(instance_id)
        self._lock = threading.RLock()
        self._sessions: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._resolved: OrderedDict[str, float] = OrderedDict()

    def _resolve(self, adapter: WETRAKRModule, item: Mapping[str, Any], body: Mapping[str, Any]) -> None:
        key = json.dumps({k: v for k, v in body.items() if k not in ("progress", "app_version")}, sort_keys=True)
        now = time.time()
        if now - self._resolved.get(key, 0) < 3600:
            return
        if item["type"] == "episode":
            resolve_child(adapter, {**item, "ids": {}})
        else:
            source, value = next(iter(body["movie"]["ids"].items()))
            row = body_of(request(adapter, "GET", f"/media/external/{source}/{quote(str(value), safe='')}", params={"type": "movie"}))
            if not isinstance(row, Mapping) or row.get("type") != "movie" or int_value(row.get("id")) < 1:
                raise WeTrakrSyncError("movie_not_resolved")
        self._resolved[key] = now
        self._resolved.move_to_end(key)
        while len(self._resolved) > 512:
            self._resolved.popitem(last=False)

    def send(self, event: ScrobbleEvent, cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
        config = dict(cfg if cfg is not None else self._cfg_provider() or {})
        block = resolve_provider_block(config, "wetrakr", self.instance_id)
        if not block.get("access_token"):
            return {"ok": False, "error": "not_configured", "retryable": False}
        if event.action not in ("start", "pause", "stop"):
            return {"ok": False, "error": "invalid_action", "retryable": False}
        progress = number(event.progress)
        if progress is None or not 0 <= progress <= 100:
            return {"ok": False, "error": "invalid_progress", "retryable": False}
        try:
            item = _item(event)
            body = _body(item, max(0.01, progress))
        except WeTrakrSyncError as exc:
            return {"ok": False, "error": exc.reason, "retryable": False}
        adapter = WETRAKRModule(config, self.instance_id)
        watch = (config.get("scrobble") or {}).get("watch") or {}
        source = str(watch.get("route_provider") or "watcher")
        source_instance = str(watch.get("route_provider_instance") or "default")
        media = {k: v for k, v in body.items() if k not in ("progress", "app_version")}
        scope = json.dumps([account_key(adapter), source, source_instance, event.server_uuid, event.account, event.session_key])
        tokens = identity_tokens(item)
        key = scope + json.dumps(media, sort_keys=True)
        with self._lock, write_lock(adapter):
            now = time.time()
            for stale in [k for k, v in self._sessions.items() if now - v["seen"] > 21600]:
                self._sessions.pop(stale)
            key = next((k for k, v in self._sessions.items() if v.get("scope") == scope and v.get("tokens", set()) & tokens), key)
            state = self._sessions.setdefault(key, {"seen": now})
            state["seen"] = now
            self._sessions.move_to_end(key)
            while len(self._sessions) > 512:
                self._sessions.popitem(last=False)
            if event.action == "start" and progress <= 2 and state.get("completed"):
                state.clear()
                state["seen"] = now
            state.update(scope=scope, tokens=state.get("tokens", set()) | tokens)
            if state.get("uncertain"):
                return {"ok": False, "error": "completion_unconfirmed", "retryable": False}
            if state.get("completed"):
                return {"ok": True, "skipped": True, "reason": "session_completed"}
            action = event.action
            if action == "stop":
                action = resolve_stop_action(progress, max(80, _setting(config, "watched_at", 90)))
                previous = state.get("progress")
                if (not event.raw.get("_cw_preserve_stop") and not event.raw.get("_cw_seek")
                        and progress >= 98 and previous is not None and previous < _setting(config, "stop_pause_threshold", 85)
                        and progress - previous >= 30):
                    action = "pause"
                    body["progress"] = max(0.01, previous)
            if action == "start":
                if progress >= _setting(config, "suppress_start_at", 99, watch=True):
                    return {"ok": True, "skipped": True, "reason": "start_suppressed"}
                body["progress"] = max(2, progress)
                if (state.get("action") == "start" and not event.raw.get("_cw_seek")
                        and abs(progress - state.get("progress", 0)) < max(1, min(25, _setting(config, "progress_step", 25)))):
                    return {"ok": True, "skipped": True, "reason": "progress_step"}
            if (action == "pause" and state.get("action") == action and state.get("progress") == body["progress"]
                    and now - state.get("sent_at", 0) < _setting(config, "pause_debounce_seconds", 5, watch=True)):
                return {"ok": True, "skipped": True, "reason": "debounced"}
            posted = False
            path = f"/scrobble/{action}"
            name = _media_name(event)
            user = mask_account(event.account)
            try:
                self._resolve(adapter, item, body)
                ids = body["show" if event.media_type == "episode" else "movie"]["ids"]
                description = ",".join(f"{source}:{value}" for source, value in ids.items())
                _log(f"intent path={path} ids={description} p={body['progress']}", "DEBUG")
                posted = True
                http_response = request(adapter, "POST", path, json=body)
                response = body_of(http_response)
                returned_action = response.get("action", action) if isinstance(response, Mapping) else action
                _log(f"send path={path} status={http_response.status_code} action={returned_action}", "DEBUG")
                reason = ignored_reason(response)
                if reason is not None:
                    for field in ("action", "progress", "sent_at", "completed", "uncertain"):
                        state.pop(field, None)
                    _log(f"scrobble ignored action={action} user='{user}' p={body['progress']:.1f}% media='{name}' reason='{reason}'")
                    return {"ok": True, "skipped": True, "ignored": True, "reason": reason}
                expected = "scrobble" if action == "stop" else action
                if not isinstance(response, Mapping) or response.get("action") != expected or response.get("success") is False:
                    raise WeTrakrSyncError("scrobble_not_confirmed")
            except WeTrakrSyncError as exc:
                uncertain = posted and action == "stop" and (not exc.status_code or exc.status_code >= 500)
                state["uncertain"] = uncertain
                self._archive(event, config, progress, "fail", exc.reason)
                _log(f"{path} failed for {name}: status={exc.status_code} reason={exc.reason}", "WARN")
                return {"ok": False, "error": exc.reason, "retryable": not uncertain and (exc.status_code in (408, 429) or exc.status_code >= 500 or exc.reason == "request_failed"),
                        "retry_after": exc.retry_after}
            state.update(action=action, progress=body["progress"], sent_at=now, completed=action == "stop")
            self._archive(event, config, progress, "ok")
            if action == "stop":
                try:
                    record_scrobble_event(event, source=source, source_instance=source_instance, target="wetrakr",
                                          target_instance=self.instance_id, progress=progress)
                except Exception:
                    pass
                self._auto_remove(event, config)
            _log(f"scrobble {response['action']} user='{user}' p={body['progress']:.1f}% media='{name}'")
            return {"ok": True, "action": response["action"]}

    def _archive(self, event: ScrobbleEvent, cfg: Mapping[str, Any], progress: float, status: str, reason: str = "") -> None:
        if event.action not in ("start", "stop"):
            return
        watch = (cfg.get("scrobble") or {}).get("watch") or {}
        try:
            record_watch(event, action=event.action, source_provider=str(watch.get("route_provider") or "watcher"),
                         source_instance=str(watch.get("route_provider_instance") or "default"), destination_provider="wetrakr",
                         destination_instance=self.instance_id, progress=progress, status=status, reason=reason)
        except Exception:
            pass

    def _auto_remove(self, event: ScrobbleEvent, cfg: Mapping[str, Any]) -> None:
        sc = cfg.get("scrobble") or {}
        options = (sc.get("watch") or {}).get("route_options") or {}
        mode = options.get("auto_remove_watchlist", "inherit")
        types = sc.get("delete_plex_types") or []
        if isinstance(types, str):
            types = [types]
        if mode == "off" or (mode != "on" and not sc.get("delete_plex")) or event.media_type not in {str(t).rstrip("s") for t in types}:
            return
        try:
            remove_across_providers_by_ids(event.ids, event.media_type, scope=f"wetrakr:{self.instance_id}")
        except Exception:
            pass

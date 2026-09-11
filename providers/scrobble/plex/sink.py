# providers/scrobble/plex/sink.py
# CrossWatch - Plex Scrobble Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any
from urllib.parse import urlencode

from providers.scrobble._media_server import DeliveryError, MediaServerSink, PUBLIC_IDS as _PUBLIC_IDS, ids_match as _id_match
from providers.scrobble.scrobble import ScrobbleEvent
from providers.sync.plex._common import configure_plex_context, home_scope_enter, home_scope_exit, ids_from_obj, isolated_plex_context, plex_feature_library_ids
from providers.sync.plex._progress import _currently_playing, _timeline_progress


def _object_ids(obj: Any) -> dict[str, str]:
    result = ids_from_obj(obj)
    return {key: str(result[key]) for key in _PUBLIC_IDS if result.get(key)}


def _matches(server: Any, obj: Any, item: Mapping[str, Any], allowed: set[str]) -> bool:
    if str(getattr(obj, "type", "")) != item["type"]:
        return False
    if allowed and str(getattr(obj, "librarySectionID", "")) not in allowed:
        return False
    own = _object_ids(obj)
    wanted = item.get("ids") or {}
    if item["type"] == "movie":
        return _id_match(wanted, own)
    if _id_match(wanted, own):
        return True
    show_ids = item.get("show_ids") or {}
    if not show_ids or item.get("season") is None or item.get("episode") is None:
        return False
    if getattr(obj, "parentIndex", None) != item["season"] or getattr(obj, "index", None) != item["episode"]:
        return False
    parent = getattr(obj, "grandparentRatingKey", None)
    if not parent:
        return False
    show = server.fetchItem(int(parent))
    return getattr(show, "type", "") == "show" and _id_match(show_ids, _object_ids(show))


def _resolve(adapter: Any, item: Mapping[str, Any], allowed: set[str]) -> Any:
    server = adapter.client.server
    candidates: dict[str, Any] = {}
    groups = [(item.get("ids") or {}, 4 if item["type"] == "episode" else 1)]
    if item["type"] == "episode":
        groups.append((item.get("show_ids") or {}, 2))
    for ids, plex_type in groups:
        for key, value in ids.items():
            query = urlencode({"guid": f"{key}://{value}", "type": plex_type,
                               "X-Plex-Container-Start": 0, "X-Plex-Container-Size": 100})
            root = server.query(f"/library/all?{query}")
            if root is not None and int(root.attrib.get("totalSize") or root.attrib.get("size") or 0) > 100:
                raise DeliveryError("ambiguous_ids")
            for row in root.iter() if root is not None else []:
                rk = row.attrib.get("ratingKey")
                if rk:
                    candidates[str(rk)] = server.fetchItem(int(rk))
    if not candidates:
        title = item.get("series_title") or item.get("title")
        if title:
            for obj in server.search(title, mediatype="show" if item["type"] == "episode" else "movie") or []:
                candidates[str(obj.ratingKey)] = obj
    matches: dict[str, Any] = {}
    for obj in candidates.values():
        if str(getattr(obj, "type", "")) == "show":
            if (item.get("season") is None or item.get("episode") is None
                    or not _id_match(item.get("show_ids") or {}, _object_ids(obj))):
                continue
            for episode in obj.episodes(parentIndex=item["season"], index=item["episode"]) or []:
                episode = server.fetchItem(int(episode.ratingKey))
                if _matches(server, episode, item, allowed):
                    matches[str(episode.ratingKey)] = episode
            continue
        if _matches(server, obj, item, allowed):
            matches[str(obj.ratingKey)] = obj
    if len(matches) != 1:
        raise DeliveryError("ambiguous_ids" if matches else "unmatched_in_plex")
    return next(iter(matches.values()))


class PlexSink(MediaServerSink):
    name = "plex"

    def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> dict[str, Any]:
        with isolated_plex_context():
            return super().send(event, cfg)

    def _connect(self, cfg: dict[str, Any]) -> Any:
        adapter = super()._connect(cfg)
        cli = adapter.client
        server = cli.server
        configure_plex_context(
            baseurl=getattr(server, "_baseurl", None) or getattr(adapter.cfg, "baseurl", None),
            token=getattr(server, "_token", None) or getattr(adapter.cfg, "pms_token", None) or getattr(adapter.cfg, "token", None),
            account_token=getattr(cli, "cloud_token", None) or getattr(adapter.cfg, "token", None) or "",
        )
        return adapter

    def _new_adapter(self, cfg: dict[str, Any]) -> Any:
        from providers.sync._mod_PLEX import PLEXModule
        with isolated_plex_context():
            return PLEXModule(cfg)

    def _deliver(self, adapter: Any, item: dict[str, Any], complete: bool, progress: float) -> dict[str, Any]:
        switched = False
        try:
            server = adapter.client.server
            if server is None:
                raise DeliveryError("plex_server_unavailable")
            needed, switched, account_id, username = home_scope_enter(adapter)
            if needed and not switched:
                raise DeliveryError("home_scope_not_applied")
            if account_id is None and not username:
                account_id = getattr(adapter.client, "user_account_id", None) or getattr(adapter.client, "token_account_id", None)
                username = getattr(adapter.client, "user_username", None) or getattr(adapter.client, "token_username", None)
            allowed = plex_feature_library_ids(adapter, "history" if complete else "progress")
            obj = self._cached_target(
                item, sorted(allowed), fetch=lambda rk: server.fetchItem(int(rk)),
                matches=lambda row: _matches(server, row, item, allowed),
                resolve=lambda: _resolve(adapter, item, allowed), key_of=lambda row: row.ratingKey,
            )
            rk = str(obj.ratingKey)
            if _currently_playing(server, rk, account_id=account_id, username=username, fail_on_error=True):
                return {"ok": False, "retryable": True, "error": "active_destination_session"}
            watched = bool(getattr(obj, "viewCount", 0))
            if complete:
                if not watched:
                    server.query("/:/scrobble", method=server._session.put,
                                 params={"key": rk, "identifier": "com.plexapp.plugins.library"})
                if int(getattr(obj, "viewOffset", 0) or 0) > 0:
                    _timeline_progress(adapter, server, rk, 0, int(getattr(obj, "duration", 0) or 0))
            else:
                if watched:
                    return {"ok": True, "skipped": True, "reason": "destination_already_watched"}
                duration = int(getattr(obj, "duration", 0) or 0)
                if duration <= 0:
                    raise DeliveryError("missing_destination_duration")
                position = round(duration * progress / 100.0)
                if position <= 0:
                    return {"ok": True, "skipped": True, "reason": "no_progress"}
                if abs(position - int(getattr(obj, "viewOffset", 0) or 0)) >= 1000:
                    _timeline_progress(adapter, server, rk, position, duration)
            return {"ok": True}
        finally:
            home_scope_exit(adapter, switched)

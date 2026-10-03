# services/published_lists.py
# CrossWatch - Published CrossWatch lists for Kometa, Radarr and Sonarr
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hmac
import json
import os
import secrets
import threading
import time
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG as CONFIG_DIR, _decrypt_secret, _encrypt_secret
from cw_platform.playlists import supports_playlists
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id

PROVIDER = "CROSSWATCH"
FORMATS: dict[str, dict[str, str]] = {
    "kometa-movies.json": {"tool": "Kometa", "label": "Kometa movies", "media": "movie"},
    "kometa-shows.json": {"tool": "Kometa", "label": "Kometa shows", "media": "show"},
    "radarr.json": {"tool": "Radarr", "label": "Radarr", "media": "movie"},
    "sonarr.json": {"tool": "Sonarr", "label": "Sonarr", "media": "show"},
}
_SHOW_TYPES = {"show", "anime", "season", "episode"}
_LOCK = threading.Lock()


def _path() -> Path:
    return Path(CONFIG_DIR) / "published_lists.json"


def _read() -> dict[str, dict[str, Any]]:
    try:
        raw = json.loads(_path().read_text("utf-8"))
    except Exception:
        return {}
    feeds = raw.get("feeds") if isinstance(raw, Mapping) else None
    out = {str(k): dict(v) for k, v in feeds.items() if isinstance(v, Mapping)} if isinstance(feeds, Mapping) else {}
    for feed in out.values():
        try:
            feed["key"] = str(_decrypt_secret(feed.get("key")) or "")
        except Exception:
            feed["key"] = ""
    return out


def _write(feeds: Mapping[str, Mapping[str, Any]]) -> None:
    path = _path()
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(".json.tmp")
    stored = {feed_id: {**feed, "key": _encrypt_secret(str(feed.get("key") or ""))} for feed_id, feed in feeds.items()}
    tmp.write_text(json.dumps({"version": 1, "feeds": stored}, ensure_ascii=False, sort_keys=True), "utf-8")
    os.replace(tmp, path)


def _ops() -> Any:
    from services.playlists import _providers

    ops = _providers().get(PROVIDER)
    return ops if ops and supports_playlists(ops) else None


def local_lists(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    ops = _ops()
    if not ops:
        return []
    out: list[dict[str, Any]] = []
    for raw_instance in list_instance_ids(cfg, PROVIDER) or ["default"]:
        instance = normalize_instance_id(raw_instance)
        try:
            resources = ops.list_playlist_resources(build_provider_config_view(cfg, PROVIDER, instance), instance=instance) or []
        except Exception:
            continue
        for resource in resources:
            extra = resource.extra if isinstance(resource.extra, Mapping) else {}
            out.append({"instance": instance, "list_id": resource.id, "name": resource.name,
                        "item_count": extra.get("item_count")})
    return out


def feeds() -> dict[str, dict[str, Any]]:
    with _LOCK:
        return _read()


def publish(instance: Any, list_id: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance)
    lid = str(list_id or "").strip()
    with _LOCK:
        current = _read()
        for feed in current.values():
            if feed.get("instance") == inst and feed.get("list_id") == lid:
                return feed
        feed_id = secrets.token_hex(6)
        while feed_id in current:
            feed_id = secrets.token_hex(6)
        current[feed_id] = {"id": feed_id, "instance": inst, "list_id": lid, "key": secrets.token_urlsafe(24),
                            "created_at": int(time.time()), "last_fetch_at": 0, "last_fetch_format": "", "fetch_count": 0}
        _write(current)
        return current[feed_id]


def rotate(feed_id: str) -> dict[str, Any] | None:
    with _LOCK:
        current = _read()
        feed = current.get(str(feed_id))
        if not feed:
            return None
        feed["key"] = secrets.token_urlsafe(24)
        _write(current)
        return feed


def unpublish(feed_id: str) -> bool:
    with _LOCK:
        current = _read()
        if current.pop(str(feed_id), None) is None:
            return False
        _write(current)
        return True


def authorized(feed_id: str, key: Any) -> dict[str, Any] | None:
    feed = feeds().get(str(feed_id))
    if not feed or not key:
        return None
    return feed if hmac.compare_digest(str(feed.get("key") or ""), str(key)) else None


def touch(feed_id: str, name: str) -> None:
    with _LOCK:
        current = _read()
        feed = current.get(str(feed_id))
        if not feed:
            return
        feed["last_fetch_at"] = int(time.time())
        feed["last_fetch_format"] = name
        feed["fetch_count"] = int(feed.get("fetch_count") or 0) + 1
        _write(current)


def _number(value: Any) -> int | None:
    try:
        number = int(str(value).strip())
    except Exception:
        return None
    return number if number > 0 else None


def _imdb(value: Any) -> str:
    text = str(value or "").strip()
    return text if text.startswith("tt") else ""


def _entries(items: Sequence[Mapping[str, Any]], media: str) -> list[tuple[str, Mapping[str, Any]]]:
    out: list[tuple[str, Mapping[str, Any]]] = []
    for item in items:
        typ = str(item.get("type") or "").strip().lower()
        if media == "movie":
            if typ == "movie" and isinstance(item.get("ids"), Mapping):
                out.append((str(item.get("title") or ""), item["ids"]))
            continue
        if typ not in _SHOW_TYPES:
            continue
        nested = typ in {"season", "episode"}
        ids = item.get("show_ids") if nested else item.get("ids")
        if isinstance(ids, Mapping):
            out.append((str((item.get("series_title") if nested else "") or item.get("title") or ""), ids))
    return out


def format_items(items: Sequence[Mapping[str, Any]], name: str) -> tuple[list[dict[str, Any]], int]:
    spec = FORMATS[name]
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    skipped = 0
    for title, ids in _entries(items, spec["media"]):
        imdb, tmdb, tvdb = _imdb(ids.get("imdb")), _number(ids.get("tmdb")), _number(ids.get("tvdb"))
        row: dict[str, Any] | None = None
        if name == "kometa-movies.json":
            row = {"imdb_id": imdb} if imdb else {"tmdb_id": tmdb} if tmdb else None
        elif name == "kometa-shows.json":
            row = {"tvdb_id": tvdb} if tvdb else {"imdb_id": imdb} if imdb else None
        elif name == "radarr.json":
            row = {"title": title, "imdb_id": imdb} if imdb else None
        elif name == "sonarr.json":
            row = {"title": title, "tvdbId": tvdb} if tvdb else None
        if row is None:
            skipped += 1
            continue
        mark = json.dumps({k: v for k, v in row.items() if k != "title"}, sort_keys=True)
        if mark in seen:
            continue
        seen.add(mark)
        rows.append(row)
    return rows, skipped


def list_items(cfg: Mapping[str, Any], feed: Mapping[str, Any]) -> list[dict[str, Any]] | None:
    ops = _ops()
    if not ops:
        return None
    instance = normalize_instance_id(feed.get("instance"))
    try:
        snap = ops.get_playlist_snapshot(build_provider_config_view(cfg, PROVIDER, instance), str(feed.get("list_id") or ""),
                                         instance=instance)
    except Exception:
        return None
    return [dict(entry.item or {}) for entry in snap.items]

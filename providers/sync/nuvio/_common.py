# providers/sync/nuvio/_common.py
# CrossWatch Nuvio sync helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from cw_platform.id_map import canonical_key, ids_from, merge_ids, minimal as id_minimal

from providers.auth._auth_NUVIO import (
    NuvioAuthError,
    NuvioClient,
    NuvioError,
    NuvioInvalidResponse,
    NuvioProfileUnavailable,
    NuvioServiceUnavailable,
    NuvioTokenRefreshError,
    is_configured as auth_is_configured,
    profile_id_value,
    provider_block,
)

__all__ = [
    "EpisodeMapResult",
    "NuvioAuthError",
    "NuvioClient",
    "NuvioError",
    "NuvioInvalidResponse",
    "NuvioProfileUnavailable",
    "NuvioServiceUnavailable",
    "NuvioTokenRefreshError",
    "canonical_item_key",
    "configured_block",
    "content_id_for_item",
    "content_id_key",
    "epoch_ms",
    "ids_for_content_id",
    "is_configured",
    "library_lock",
    "make_item",
    "metadata_title_for_content_id",
    "payload_item_key",
    "positive_int",
    "progress_key",
    "pull_library_rows",
    "pull_watch_progress_rows",
    "pull_watched_rows",
    "resolve_content_id_for_item",
    "resolve_episode",
    "rpc",
    "selected_profile_id",
    "to_int",
]

_LIBRARY_LOCKS: dict[str, threading.Lock] = {}
_LIBRARY_LOCKS_GUARD = threading.Lock()


@dataclass(frozen=True)
class EpisodeMapResult:
    ok: bool
    reason: str
    match_basis: str | None = None
    content_id: str | None = None
    video_id: str | None = None
    source_season: int | None = None
    source_episode: int | None = None
    destination_season: int | None = None
    destination_episode: int | None = None


def configured_block(cfg: Mapping[str, Any] | None, instance_id: Any = "default") -> dict[str, Any]:
    return provider_block(cfg or {}, instance_id)


def is_configured(cfg: Mapping[str, Any] | None, instance_id: Any = "default") -> bool:
    return auth_is_configured(configured_block(cfg, instance_id))


def selected_profile_id(adapter: Any) -> int:
    block = configured_block(getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}, getattr(adapter, "instance_id", "default"))
    pid = profile_id_value(block)
    if pid is None:
        raise NuvioProfileUnavailable("nuvio_profile_unavailable")
    return int(pid)


def rpc(adapter: Any, name: str, payload: Mapping[str, Any] | None = None) -> Any:
    client = getattr(adapter, "client", None)
    if client is None:
        raise NuvioServiceUnavailable("service_unavailable")
    return client.request_json("POST", f"/rest/v1/rpc/{str(name).strip()}", payload=dict(payload or {}), refresh=True, retry=True)


def to_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(float(str(value).strip()))
    except Exception:
        return None


def positive_int(value: Any) -> int | None:
    number = to_int(value)
    return number if number is not None and number > 0 else None


def epoch_ms(value: Any) -> int | None:
    number = to_int(value)
    if number is not None:
        return number * 1000 if 0 < number < 10_000_000_000 else number
    text = str(value or "").strip()
    if not text:
        return None
    try:
        from datetime import datetime

        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        return int(parsed.timestamp() * 1000)
    except Exception:
        return None


def ids_for_content_id(content_id: Any) -> dict[str, str] | None:
    raw = str(content_id or "").strip()
    low = raw.lower()
    if raw.startswith("tt") and raw[2:].isdigit():
        return {"imdb": raw}
    if low.startswith("tmdb:"):
        value = raw.split(":", 1)[1].strip()
        if value.isdigit():
            return {"tmdb": value}
    return None


def content_id_for_item(item: Mapping[str, Any]) -> str | None:
    raw = str(item.get("_nuvio_content_id") or item.get("content_id") or "").strip()
    if raw and ids_for_content_id(raw):
        return raw
    item_type = str(item.get("type") or "").strip().lower()
    show_ids_obj = item.get("show_ids")
    if item_type in {"episode", "episodes", "season", "seasons"}:
        if isinstance(show_ids_obj, Mapping):
            raw_ids = merge_ids({str(k): v for k, v in show_ids_obj.items()}, {})
        else:
            return None
    else:
        raw_ids = merge_ids(ids_from(item), ids_from(id_minimal(item)))
        if isinstance(show_ids_obj, Mapping):
            raw_ids = merge_ids(raw_ids, {str(k): v for k, v in show_ids_obj.items()})
    tmdb = str(raw_ids.get("tmdb") or "").strip()
    if tmdb.isdigit():
        return f"tmdb:{tmdb}"
    imdb = str(raw_ids.get("imdb") or "").strip()
    if imdb.startswith("tt") and imdb[2:].isdigit():
        return imdb
    return None


def _tmdb_metadata_provider(adapter: Any) -> Any | None:
    cfg = getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}
    if not isinstance(cfg, Mapping):
        return None
    tmdb_obj = cfg.get("tmdb")
    tmdb: Mapping[str, Any] = tmdb_obj if isinstance(tmdb_obj, Mapping) else {}
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    if not str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip():
        return None
    try:
        from providers.metadata._meta_TMDB import TmdbProvider

        return TmdbProvider(lambda: dict(cfg), lambda _cfg: None)
    except Exception:
        return None


def _metadata_lookup_ids(item: Mapping[str, Any], item_type: str) -> dict[str, str]:
    source: Mapping[str, Any] | None = None
    show_ids = item.get("show_ids")
    if item_type in {"episode", "episodes", "season", "seasons"} and isinstance(show_ids, Mapping):
        source = show_ids
    if source is None:
        source = merge_ids(ids_from(item), ids_from(id_minimal(item)))
        if isinstance(show_ids, Mapping):
            source = merge_ids(source, {str(k): v for k, v in show_ids.items()})
        content_ids = ids_for_content_id(item.get("content_id"))
        if content_ids:
            source = merge_ids(source, content_ids)
    return {
        key: str(source.get(key) or "").strip()
        for key in ("tmdb", "imdb", "tvdb")
        if str(source.get(key) or "").strip()
    }


def resolve_content_id_for_item(adapter: Any, item: Mapping[str, Any]) -> str | None:
    remote_content_id = str(item.get("_nuvio_content_id") or "").strip()
    if remote_content_id and ids_for_content_id(remote_content_id):
        return remote_content_id
    direct = content_id_for_item(item)
    if direct and direct.lower().startswith("tmdb:"):
        return direct
    item_type = str(item.get("type") or id_minimal(item).get("type") or "").strip().lower()
    lookup_ids = _metadata_lookup_ids(item, item_type)
    if not lookup_ids:
        return None
    provider = _tmdb_metadata_provider(adapter)
    if provider is None:
        return None
    try:
        detail = provider.fetch(
            entity="tv" if item_type in {"episode", "episodes", "season", "seasons", "show", "series", "tv"} else "movie",
            ids=lookup_ids,
            need={"poster": False, "backdrop": False, "ids": True},
        )
    except Exception:
        return None
    if not isinstance(detail, Mapping):
        return None
    ids = detail.get("ids")
    if not isinstance(ids, Mapping):
        return None
    tmdb = str(ids.get("tmdb") or "").strip()
    return f"tmdb:{tmdb}" if tmdb.isdigit() else None


def metadata_title_for_content_id(adapter: Any, content_id: Any, entity: str) -> str:
    ids = ids_for_content_id(content_id) or {}
    if not ids:
        return ""
    provider = _tmdb_metadata_provider(adapter)
    if provider is None:
        return ""
    try:
        detail = provider.fetch(entity=entity, ids=ids, need={"poster": False, "backdrop": False, "ids": False})
    except Exception:
        return ""
    return str(detail.get("title") or "").strip() if isinstance(detail, Mapping) else ""


def make_item(
    *,
    content_id: Any,
    content_type: Any,
    season: Any = None,
    episode: Any = None,
    title: Any = None,
    year: Any = None,
) -> dict[str, Any] | None:
    ids = ids_for_content_id(content_id)
    if not ids:
        return None
    ctype = str(content_type or "").strip().lower()
    season_n = positive_int(season)
    episode_n = positive_int(episode)
    if ctype in {"series", "show", "tv"} and not (season_n and episode_n):
        item = {"type": "show", "ids": dict(ids)}
        if title:
            item["title"] = str(title)
        if year is not None:
            item["year"] = year
        return item

    if ctype in {"episode"} or (season_n and episode_n):
        if season_n is None or episode_n is None:
            return None
        item: dict[str, Any] = {
            "type": "episode",
            "show_ids": dict(ids),
            "ids": dict(ids),
            "season": season_n,
            "episode": episode_n,
        }
        if title:
            item["series_title"] = str(title)
    else:
        item = {"type": "movie", "ids": dict(ids)}
        if title:
            item["title"] = str(title)
        if year is not None:
            item["year"] = year
    return item


def canonical_item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(id_minimal(item))


def payload_item_key(payload: Mapping[str, Any]) -> str:
    return canonical_item_key(
        make_item(
            content_id=payload.get("content_id"),
            content_type=payload.get("content_type"),
            season=payload.get("season"),
            episode=payload.get("episode"),
            title=payload.get("name"),
            year=payload.get("release_info"),
        )
        or {}
    )


def content_id_key(item: Mapping[str, Any]) -> str:
    content_id = content_id_for_item(item)
    season = positive_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    if content_id and season and episode:
        return f"{content_id}:{season}:{episode}"
    return str(content_id or "")


def progress_key(item: Mapping[str, Any]) -> str | None:
    direct = str(item.get("_nuvio_progress_key") or item.get("progress_key") or "").strip()
    if direct:
        return direct
    content_id = content_id_for_item(item)
    if not content_id:
        return None
    season = positive_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    if season and episode:
        return f"{content_id}_s{season}e{episode}"
    return content_id


def resolve_episode(adapter: Any, item: Mapping[str, Any], *, current_rows: Any = None) -> EpisodeMapResult:
    content_id = resolve_content_id_for_item(adapter, item)
    season = positive_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    if not content_id:
        return EpisodeMapResult(False, "nuvio_id_missing")
    if not season or not episode:
        return EpisodeMapResult(False, "nuvio_episode_not_found", content_id=content_id)

    video_id = str(item.get("_nuvio_video_id") or item.get("video_id") or "").strip()
    if video_id:
        return EpisodeMapResult(True, "ok", "existing_video_id", content_id, video_id, season, episode, season, episode)

    for row in current_rows or []:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("content_id") or "").strip() != content_id:
            continue
        if positive_int(row.get("season")) != season or positive_int(row.get("episode")) != episode:
            continue
        row_video = str(row.get("video_id") or "").strip()
        if row_video:
            return EpisodeMapResult(True, "ok", "existing_remote_identity", content_id, row_video, season, episode, season, episode)

    return EpisodeMapResult(True, "ok", "canonical_episode_identifier", content_id, f"{content_id}:{season}:{episode}", season, episode, season, episode)


def _rows(data: Any, reason: str) -> list[Mapping[str, Any]]:
    if not isinstance(data, list):
        raise NuvioInvalidResponse(reason)
    return [row for row in data if isinstance(row, Mapping)]


def pull_watch_progress_rows(adapter: Any, *, limit: int = 1000, max_pages: int = 1000) -> list[Mapping[str, Any]]:
    pid = selected_profile_id(adapter)
    per_page = max(1, min(int(limit or 1000), 1000))
    pages = max(1, int(max_pages or 1000))
    cursor = 0
    out: list[Mapping[str, Any]] = []

    for _ in range(pages):
        data = rpc(adapter, "sync_pull_watch_progress", {"p_profile_id": pid, "p_since_last_watched": cursor, "p_limit": per_page})
        rows = _rows(data, "nuvio_progress_invalid")
        out.extend(rows)
        if len(rows) < per_page:
            break
        next_cursor = max((epoch_ms(row.get("last_watched")) or cursor for row in rows), default=cursor)
        if next_cursor <= cursor:
            raise NuvioInvalidResponse("nuvio_progress_invalid")
        cursor = next_cursor
    return out


def pull_watched_rows(adapter: Any, *, page_size: int = 900, max_pages: int = 1000) -> list[Mapping[str, Any]]:
    pid = selected_profile_id(adapter)
    size = max(1, min(int(page_size or 900), 1000))
    out: list[Mapping[str, Any]] = []
    for page in range(1, max(1, int(max_pages or 1000)) + 1):
        data = rpc(adapter, "sync_pull_watched_items", {"p_profile_id": pid, "p_page": page, "p_page_size": size})
        rows = _rows(data, "nuvio_history_invalid")
        out.extend(rows)
        if len(rows) < size:
            break
    return out


def pull_library_rows(adapter: Any, *, limit: int = 500, max_pages: int = 1000) -> list[Mapping[str, Any]]:
    pid = selected_profile_id(adapter)
    size = max(1, min(int(limit or 500), 500))
    out: list[Mapping[str, Any]] = []
    for page in range(max(1, int(max_pages or 1000))):
        offset = page * size
        data = rpc(adapter, "sync_pull_library", {"p_profile_id": pid, "p_limit": size, "p_offset": offset})
        rows = _rows(data, "nuvio_library_read_failed")
        out.extend(rows)
        if len(rows) < size:
            break
    return out


def library_lock(adapter: Any) -> threading.Lock:
    key = f"{getattr(adapter, 'instance_id', 'default')}:{selected_profile_id(adapter)}"
    with _LIBRARY_LOCKS_GUARD:
        lock = _LIBRARY_LOCKS.get(key)
        if lock is None:
            lock = threading.Lock()
            _LIBRARY_LOCKS[key] = lock
        return lock

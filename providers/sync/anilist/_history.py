# /providers/sync/anilist/_history.py
# AniList one-way history writer
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.anime_mapping.episodes import resolve_axis_coordinate_map
from cw_platform.anime_mapping.storage import normalize_release_tag
from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._progress import GQL_SAVE_PROGRESS, plan_progress, resolve_target

GQL_HISTORY_LIST = """
query ($userId: Int!, $type: MediaType!) {
  MediaListCollection(userId: $userId, type: $type) {
    lists {
      entries {
        id
        mediaId
        status
        progress
        repeat
        updatedAt
        createdAt
        startedAt { year month day }
        completedAt { year month day }
        media {
          id
          idMal
          format
          episodes
          status
          seasonYear
          startDate { year }
          title { romaji english native }
        }
      }
    }
  }
}
""".strip()

GQL_HISTORY_MEDIA = """
query ($ids: [Int], $page: Int) {
  Page(page: $page, perPage: 50) {
    media(id_in: $ids, type: ANIME) {
      id
      episodes
      status
      title { romaji english native }
    }
  }
}
""".strip()

_MEDIA_PAGE = 50
REMOVE_UNSUPPORTED = "anilist_history_remove_unsupported"
SPECIALS_UNSUPPORTED = "anilist_history_specials_unsupported"
NOT_MAPPED = "not_anime_or_no_match"


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "history", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "history", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("ANILIST", "history", "warn", msg, **fields)


def _to_int(value: Any) -> int | None:
    try:
        if value is None or isinstance(value, bool) or value == "":
            return None
        return int(str(value).strip())
    except Exception:
        return None


def _release_tag(cfg: Any) -> str:
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    return normalize_release_tag((block if isinstance(block, Mapping) else {}).get("release_tag") or "v3")


def _title(media: Mapping[str, Any]) -> str:
    block = media.get("title") if isinstance(media.get("title"), Mapping) else {}
    return str((block or {}).get("english") or (block or {}).get("romaji") or (block or {}).get("native") or "").strip()


def _year(media: Mapping[str, Any]) -> int | None:
    year = _to_int(media.get("seasonYear"))
    if year is None and isinstance(media.get("startDate"), Mapping):
        year = _to_int(media["startDate"].get("year"))
    return year


def _fuzzy_iso(value: Any) -> str | None:
    if not isinstance(value, Mapping):
        return None
    year, month, day = _to_int(value.get("year")), _to_int(value.get("month")), _to_int(value.get("day"))
    if not year:
        return None
    try:
        return date(year, month or 1, day or 1).strftime("%Y-%m-%dT00:00:00Z")
    except ValueError:
        return None


def _epoch_iso(value: Any) -> str | None:
    stamp = _to_int(value)
    if not stamp or stamp <= 0:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stamp))


def _watched_at(entry: Mapping[str, Any]) -> str | None:
    return _fuzzy_iso(entry.get("completedAt")) or _epoch_iso(entry.get("updatedAt")) or _epoch_iso(entry.get("createdAt"))


def _watched_day(item: Mapping[str, Any]) -> date | None:
    text = str(item.get("watched_at") or item.get("last_watched_at") or "").strip()
    if not text:
        return None
    if text.isdigit():
        stamp = int(text)
        return datetime.fromtimestamp(stamp // 1000 if len(text) >= 13 else stamp, tz=timezone.utc).date()
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).astimezone(timezone.utc).date()
    except ValueError:
        return None


def _fuzzy(day: date) -> dict[str, int]:
    return {"year": day.year, "month": day.month, "day": day.day}


def _mapping_service(adapter: Any) -> AnimeMappingService | None:
    cfg = getattr(adapter, "raw_cfg", None)
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else None
    if not bool((block if isinstance(block, Mapping) else {}).get("enabled", False)):
        return None
    try:
        svc = AnimeMappingService(cfg)
        return svc if svc.ready() else None
    except Exception:
        return None


def _tick(prog: Any, value: int, total: int | None = None, *, force: bool = False) -> None:
    if prog is None:
        return
    try:
        if total is not None:
            prog.tick(value, total=total, force=force)
        else:
            prog.tick(value)
    except Exception:
        pass


def _entries(adapter: Any) -> dict[int, dict[str, Any]]:
    viewer = adapter.client.viewer()
    user_id = viewer.get("id") if isinstance(viewer, dict) else None
    if not user_id:
        return {}
    data = adapter.client.gql(GQL_HISTORY_LIST, {"userId": int(user_id), "type": "ANIME"}, feature="history:index")
    collection = (data or {}).get("MediaListCollection")
    lists = collection.get("lists") if isinstance(collection, Mapping) else None
    out: dict[int, dict[str, Any]] = {}
    for bucket in lists if isinstance(lists, list) else []:
        for entry in (bucket.get("entries") if isinstance(bucket, Mapping) else None) or []:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("media"), Mapping):
                continue
            media_id = _to_int(entry["media"].get("id") or entry.get("mediaId"))
            if media_id and media_id not in out:
                out[media_id] = dict(entry)
    return out


def _watched_count(entry: Mapping[str, Any]) -> int:
    media = entry.get("media") if isinstance(entry.get("media"), Mapping) else {}
    progress = _to_int(entry.get("progress")) or 0
    total = _to_int((media or {}).get("episodes")) or 0
    if str(entry.get("status") or "").strip().upper() == "COMPLETED" and total > progress:
        return total
    return progress


def _movie_item(svc: AnimeMappingService, entry: Mapping[str, Any], watched_at: str) -> dict[str, Any]:
    media = entry["media"]
    seed = {"anilist": str(media.get("id"))}
    if media.get("idMal"):
        seed["mal"] = str(media.get("idMal"))
    try:
        found = svc.enrich_ids(seed, media_type="movie").get("ids") or {}
    except Exception:
        found = {}
    ids = {**seed, **{str(k): str(v) for k, v in dict(found).items() if str(v or "").strip()}}
    return {"type": "movie", "title": _title(media), "year": _year(media) or 0, "ids": ids, "watched": True, "watched_at": watched_at}


def _episode_items(tag: str, entry: Mapping[str, Any], watched: int, watched_at: str) -> list[dict[str, Any]]:
    media = entry["media"]
    title = _title(media)
    ids = {"anilist": str(media.get("id"))}
    out: list[dict[str, Any]] = []
    for number in range(1, watched + 1):
        try:
            axes = resolve_axis_coordinate_map(ids, number, release_tag=tag)
        except Exception:
            axes = {}
        for (provider, ident), (season, episode) in axes.items():
            out.append({"type": "episode", "title": title, "series_title": title, "season": int(season), "episode": int(episode),
                        "ids": {}, "show_ids": {str(provider): str(ident)}, "watched": True, "watched_at": watched_at})
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None
    svc = _mapping_service(adapter)
    if svc is None:
        _warn("anime_mapping_unavailable", op="index")
        return {}
    tag = _release_tag(getattr(adapter, "raw_cfg", None))
    out: dict[str, dict[str, Any]] = {}
    unmapped = 0
    for done, entry in enumerate(_entries(adapter).values(), start=1):
        watched = _watched_count(entry)
        watched_at = _watched_at(entry)
        if watched <= 0 or not watched_at:
            continue
        media = entry["media"]
        items: list[dict[str, Any]] = []
        if str(media.get("format") or "").strip().upper() == "MOVIE" or _to_int(media.get("episodes")) == 1:
            items.append(_movie_item(svc, entry, watched_at))
        if str(media.get("format") or "").strip().upper() != "MOVIE":
            items.extend(_episode_items(tag, entry, watched, watched_at))
        if not items:
            unmapped += 1
        for item in items:
            mini = id_minimal(item)
            mini["watched"] = True
            mini["watched_at"] = watched_at
            key = canonical_key(mini)
            if key and key not in out:
                out[key] = mini
        _tick(prog, done)
    _info("index_done", count=len(out), unmapped_entries=unmapped, source="live")
    return out


def _media_states(adapter: Any, wanted: Iterable[int]) -> dict[int, dict[str, Any]]:
    states: dict[int, dict[str, Any]] = {}
    for media_id, entry in _entries(adapter).items():
        media = entry["media"]
        states[media_id] = {
            "id": media_id,
            "episodes": media.get("episodes"),
            "status": media.get("status"),
            "title": media.get("title"),
            "mediaListEntry": {"id": entry.get("id"), "status": entry.get("status"), "progress": entry.get("progress"),
                               "repeat": entry.get("repeat"), "startedAt": entry.get("startedAt")},
        }
    missing = [media_id for media_id in dict.fromkeys(wanted) if media_id not in states]
    for start in range(0, len(missing), _MEDIA_PAGE):
        chunk = missing[start:start + _MEDIA_PAGE]
        data = adapter.client.gql(GQL_HISTORY_MEDIA, {"ids": chunk, "page": 1}, feature="history:lookup", tolerate_errors=True)
        page = (data or {}).get("Page")
        for media in (page.get("media") if isinstance(page, Mapping) else None) or []:
            media_id = _to_int(media.get("id")) if isinstance(media, Mapping) else None
            if media_id:
                states[media_id] = {**dict(media), "mediaListEntry": None}
    return states


def _unresolved(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    out = dict(id_minimal(item))
    out["reason"] = reason
    return out


def add_detailed(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(item) for item in items or [] if isinstance(item, Mapping)]
    empty = {"ok": True, "count": 0, "confirmed": 0, "confirmed_keys": [], "skipped": 0, "skipped_keys": [], "unresolved": []}
    if not rows:
        return empty
    if _mapping_service(adapter) is None:
        _warn("anime_mapping_unavailable", op="add")
        return {**empty, "unresolved": [_unresolved(item, "anime_mapping_unavailable") for item in rows]}

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history", total=len(rows)) if callable(prog_mk) else None
    cfg = getattr(adapter, "raw_cfg", None)
    groups: dict[int, list[tuple[int, str, date | None, Mapping[str, Any]]]] = {}
    skipped_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []

    for item in rows:
        key = str(adapter.key_of(item) or "")
        specials = str(item.get("type") or "").strip().lower() == "episode" and _to_int(item.get("season")) == 0
        target = None if specials else resolve_target(cfg, item)
        if target is None:
            _dbg("write_item_skipped", op="add", title=str(item.get("series_title") or item.get("title") or ""),
                 reason=SPECIALS_UNSUPPORTED if specials else NOT_MAPPED)
            if key and key not in skipped_keys:
                skipped_keys.append(key)
            continue
        groups.setdefault(int(target[0]), []).append((int(target[1]), key, _watched_day(item), item))

    confirmed_keys: list[str] = []
    done = len(rows) - sum(len(group) for group in groups.values())
    states = _media_states(adapter, groups) if groups else {}
    for anilist_id, group in groups.items():
        done += len(group)
        media = states.get(anilist_id)
        if not isinstance(media, Mapping):
            unresolved.extend(_unresolved(item, "media_not_found") for _ep, _key, _day, item in group)
            _tick(prog, done, total=len(rows))
            continue
        days = [day for _ep, _key, day, _item in group if day is not None]
        plan = plan_progress(media, max(ep for ep, _key, _day, _item in group), today=max(days) if days else None)
        if not plan.get("skip"):
            variables = dict(plan["variables"])
            if "startedAt" in variables and days:
                variables["startedAt"] = _fuzzy(min(days))
            try:
                adapter.client.gql(GQL_SAVE_PROGRESS, variables, feature="history:add")
            except Exception as exc:
                _warn("write_failed", op="add", anilist_id=anilist_id, error_type=exc.__class__.__name__)
                unresolved.extend(_unresolved(item, f"write_failed:{exc.__class__.__name__}") for _ep, _key, _day, item in group)
                _tick(prog, done, total=len(rows))
                continue
            _dbg("write_item_done", op="add", anilist_id=anilist_id, progress=variables.get("progress"),
                 status=variables.get("status"), episodes=len(group))
        else:
            _dbg("write_item_skipped", op="add", anilist_id=anilist_id, reason=str(plan.get("skip")), episodes=len(group))
        for _ep, key, _day, _item in group:
            if key and key not in confirmed_keys:
                confirmed_keys.append(key)
        _tick(prog, done, total=len(rows))

    _info("write_done", op="add", ok=not unresolved, applied=len(confirmed_keys), skipped=len(skipped_keys),
          unresolved=len(unresolved), entries=len(groups))
    return {"ok": True, "count": len(confirmed_keys), "confirmed": len(confirmed_keys), "confirmed_keys": confirmed_keys,
            "skipped": len(skipped_keys), "skipped_keys": skipped_keys, "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    res = add_detailed(adapter, items)
    return int(res.get("confirmed") or 0), list(res.get("unresolved") or [])


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return 0, [_unresolved(item, REMOVE_UNSUPPORTED) for item in items or [] if isinstance(item, Mapping)]


__all__ = ["build_index", "add_detailed", "add", "remove"]

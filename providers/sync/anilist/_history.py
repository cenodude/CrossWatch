# /providers/sync/anilist/_history.py
# AniList watched status and episode history
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from datetime import date, datetime, timezone
from typing import Any

from cw_platform.anime_mapping import AnimeMappingService
from cw_platform.anime_mapping.coordinates import translate
from cw_platform.anime_mapping.episodes import resolve_axis_coordinate_map
from cw_platform.anime_mapping.overrides import find_episode_override, find_source_overrides
from cw_platform.anime_mapping.service import PAIR_FEATURE_OPTIONS_KEY, runtime_pair_feature_options
from cw_platform.anime_mapping.storage import normalize_release_tag, query_edges
from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._progress import GQL_SAVE_PROGRESS, native_to_anilist, plan_progress, resolve_target

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
_HELD_STATUS = {"dropped": "DROPPED", "on_hold": "PAUSED"}
GQL_REMOVE_HISTORY = """
mutation ($id: Int!, $status: MediaListStatus!, $progress: Int!, $completedAt: FuzzyDateInput) {
  SaveMediaListEntry(id: $id, status: $status, progress: $progress, completedAt: $completedAt) {
    id
    status
    progress
    completedAt { year month day }
  }
}
""".strip()
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


def _entries(adapter: Any, *, strict: bool = False) -> dict[int, dict[str, Any]]:
    viewer = adapter.client.viewer()
    user_id = viewer.get("id") if isinstance(viewer, dict) else None
    if not user_id:
        if strict:
            raise ValueError("missing_viewer")
        return {}
    data = adapter.client.gql(GQL_HISTORY_LIST, {"userId": int(user_id), "type": "ANIME"}, feature="history:index")
    collection = (data or {}).get("MediaListCollection")
    lists = collection.get("lists") if isinstance(collection, Mapping) else None
    if strict and not isinstance(lists, list):
        raise ValueError("invalid_history_list")
    out: dict[int, dict[str, Any]] = {}
    for bucket in lists if isinstance(lists, list) else []:
        if strict and (not isinstance(bucket, Mapping) or not isinstance(bucket.get("entries"), list)):
            raise ValueError("invalid_history_list")
        for entry in (bucket.get("entries") if isinstance(bucket, Mapping) else None) or []:
            if not isinstance(entry, Mapping) or not isinstance(entry.get("media"), Mapping):
                if strict:
                    raise ValueError("invalid_history_entry")
                continue
            media_id = _to_int(entry["media"].get("id") or entry.get("mediaId"))
            if strict and (not media_id or not _to_int(entry.get("id")) or (_to_int(entry.get("progress")) or 0) < 0
                           or _to_int(entry.get("progress")) is None or not entry["media"].get("format")
                           or entry.get("status") not in {"CURRENT", "PLANNING", "COMPLETED", "DROPPED", "PAUSED", "REPEATING"}):
                raise ValueError("invalid_history_entry")
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


def _ruled_elsewhere(tag: str, media_id: int | None, watched: int, provider: str, ident: str, season: int, episode: int) -> bool:
    try:
        ruled = find_episode_override({str(provider): str(ident)}, season, episode)
    except Exception:
        return False
    if ruled is None:
        return False
    hit = native_to_anilist(tag, ruled.namespace, ruled.target_id, ruled.absolute)
    if hit is None:
        return False
    return hit[0] != media_id or hit[1] > watched


def _native_edges(tag: str, anilist_id: str) -> list[dict[str, Any]]:
    try:
        rows = query_edges(tag, "anilist", anilist_id)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for row in rows:
        namespace = str(row.get("target_provider") or "").strip().lower()
        if namespace not in ("mal", "anidb") or not str(row.get("target_id") or "").strip():
            continue
        if namespace == "anidb" and str(row.get("target_scope") or "").strip().upper() != "R":
            continue
        out.append(row)
    return out


def _native_numbers(anilist_id: str, edges: list[dict[str, Any]], number: int) -> list[tuple[str, str, int]]:
    out: list[tuple[str, str, int]] = [("anilist", anilist_id, number)]
    for row in edges:
        mapped = translate(row.get("source_range"), row.get("target_range"), number)
        if not mapped or int(mapped) <= 0:
            continue
        native = (str(row.get("target_provider") or "").strip().lower(), str(row.get("target_id") or "").strip(), int(mapped))
        if native not in out:
            out.append(native)
    return out


def _episode_items(tag: str, entry: Mapping[str, Any], watched: int, watched_at: str) -> list[dict[str, Any]]:
    media = entry["media"]
    title = _title(media)
    ids = {"anilist": str(media.get("id"))}
    media_id = _to_int(media.get("id"))
    edges = _native_edges(tag, ids["anilist"])
    out: list[dict[str, Any]] = []
    for number in range(1, watched + 1):
        coords: list[tuple[str, str, int, int]] = []
        try:
            for namespace, native_id, absolute in _native_numbers(ids["anilist"], edges, number):
                for hit in find_source_overrides(namespace, native_id, absolute):
                    coord = (hit.provider, hit.ident, hit.season, hit.episode)
                    if coord not in coords and not _ruled_elsewhere(tag, media_id, watched, *coord):
                        coords.append(coord)
        except Exception:
            coords = []
        if not coords:
            try:
                axes = resolve_axis_coordinate_map(ids, number, release_tag=tag)
            except Exception:
                axes = {}
            coords = [(provider, ident, season, episode) for (provider, ident), (season, episode) in axes.items()
                      if not _ruled_elsewhere(tag, media_id, watched, provider, ident, season, episode)]
        for provider, ident, season, episode in coords:
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


def _source_status_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "raw_cfg", None)
    if not isinstance(cfg, Mapping) or PAIR_FEATURE_OPTIONS_KEY not in cfg:
        return False
    return bool(runtime_pair_feature_options(cfg, "history").get("use_source_status"))


def _status_override(items: Iterable[Mapping[str, Any]], media: Mapping[str, Any], planned: Any) -> str | None:
    if str(planned or "").upper() != "CURRENT":
        return None
    statuses = {str(item.get("watch_status") or "").strip().lower() for item in items}
    statuses.discard("")
    if len(statuses) == 1:
        held = _HELD_STATUS.get(next(iter(statuses)))
        if held:
            return held
    if statuses:
        return None
    entry = media.get("mediaListEntry") if isinstance(media.get("mediaListEntry"), Mapping) else {}
    current = str((entry or {}).get("status") or "").strip().upper()
    return current if current in _HELD_STATUS.values() else None


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
    use_source_status = _source_status_enabled(adapter)
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
            if use_source_status:
                override = _status_override([item for _ep, _key, _day, item in group], media, variables.get("status"))
                if override:
                    variables["status"] = override
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


def order_removals(cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    def position(item: Mapping[str, Any]) -> int:
        try:
            target = resolve_target(cfg, item)
            return target[1] if target else 0
        except Exception:
            return 0

    return sorted(items, key=position, reverse=True)


def remove_detailed(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = [dict(item) for item in items or [] if isinstance(item, Mapping)]
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    groups: dict[int, list[tuple[Mapping[str, Any], int]]] = {}
    cfg = getattr(adapter, "raw_cfg", None)
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history", total=len(rows)) if callable(prog_mk) else None

    def failed(item: Mapping[str, Any], exc: Exception) -> None:
        reason = str(exc) if isinstance(exc, ValueError) else f"request_failed:{exc.__class__.__name__}"
        unresolved.append(_unresolved(item, reason))

    def confirm(item: Mapping[str, Any]) -> None:
        key = str(adapter.key_of(item) or "")
        if key and key not in confirmed:
            confirmed.append(key)

    ready = _mapping_service(adapter) is not None
    for item in rows:
        try:
            if not ready:
                raise ValueError("anime_mapping_unavailable")
            kind = str(item.get("type") or "").lower()
            if kind not in ("movie", "episode"):
                raise ValueError("unsupported_media_type")
            if kind == "episode" and _to_int(item.get("season")) == 0:
                raise ValueError(SPECIALS_UNSUPPORTED)
            target = resolve_target(cfg, item)
            if not target or target[1] <= 0:
                raise ValueError(NOT_MAPPED)
            groups.setdefault(target[0], []).append((item, target[1]))
        except Exception as exc:
            failed(item, exc)

    try:
        entries = _entries(adapter, strict=True) if groups else {}
    except Exception as exc:
        for group in groups.values():
            for item, _ in group:
                failed(item, exc)
        groups = {}
        entries = {}

    done = len(rows) - sum(len(group) for group in groups.values())
    for media_id, group in groups.items():
        entry = entries.get(media_id)
        if not entry:
            for item, _ in group:
                confirm(item)
        else:
            media = entry["media"]
            total = _to_int(media.get("episodes")) or 0
            movie = str(media.get("format") or "").upper() == "MOVIE"
            watched = max(0, _watched_count(entry))
            if movie and entry.get("status") == "COMPLETED":
                watched = max(1, watched)
            progress = watched
            pending = []
            for item, episode in sorted(group, key=lambda row: row[1], reverse=True):
                is_movie = str(item.get("type") or "").lower() == "movie"
                if (is_movie and not (movie or total == 1)) or (not is_movie and movie):
                    failed(item, ValueError("anime_media_type_mismatch"))
                elif entry.get("status") == "COMPLETED" and not movie and not total:
                    failed(item, ValueError("anilist_history_unknown_completed_total"))
                elif not watched or episode > watched:
                    confirm(item)
                elif not is_movie and episode < progress:
                    failed(item, ValueError("anilist_history_remove_would_erase_later_episodes"))
                else:
                    progress = 0 if is_movie else min(progress, episode - 1)
                    pending.append(item)
            if pending:
                status = "CURRENT" if progress else "PAUSED"
                variables = {"id": int(entry["id"]), "progress": progress, "status": status,
                             "completedAt": {"year": None, "month": None, "day": None}}
                try:
                    data = adapter.client.gql(GQL_REMOVE_HISTORY, variables, feature="history:remove")
                    saved = (data or {}).get("SaveMediaListEntry")
                    if not isinstance(saved, Mapping) or _to_int(saved.get("id")) != variables["id"] or _to_int(saved.get("progress")) != progress or saved.get("status") != status:
                        raise RuntimeError("AniList did not confirm the history update")
                    completed = saved.get("completedAt")
                    if "completedAt" not in saved or (completed is not None and (not isinstance(completed, Mapping) or any(k not in completed or completed[k] is not None for k in ("year", "month", "day")))):
                        raise RuntimeError("AniList did not clear the completion date")
                except Exception as exc:
                    for item in pending:
                        failed(item, exc)
                else:
                    for item in pending:
                        confirm(item)
        done += len(group)
        _tick(prog, done, total=len(rows))

    _tick(prog, len(rows), total=len(rows), force=True)
    _info("write_done", op="remove", applied=len(confirmed), unresolved=len(unresolved), entries=len(groups))
    return {"ok": True, "count": len(confirmed), "confirmed": len(confirmed), "confirmed_keys": confirmed,
            "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    result = remove_detailed(adapter, items)
    return result["count"], result["unresolved"]


__all__ = ["build_index", "add_detailed", "add", "remove_detailed", "remove", "order_removals"]

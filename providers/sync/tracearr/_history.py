# providers/sync/tracearr/_history.py
# CrossWatch - Tracearr history (read-only)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from datetime import datetime, timezone
from typing import Any

from cw_platform.history_events import history_sync_key, minimal_history_item
from cw_platform.id_map import canonical_key

from ._common import make_logger


_EXT_ID_KEYS = ("tmdb", "imdb", "tvdb")
_LOCAL_ID_KEYS = {"plex": "plex", "jellyfin": "jellyfin", "emby": "emby"}
_DEFAULT_MIN_PERCENT = 85.0
_DEFERRED_FIELDS = ("id", "server_id", "server_type", "media_title", "show_title", "season_number", "episode_number", "year", "grandparent_rating_key")
_log = make_logger("history")


def _cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    cfg = getattr(adapter, "cfg", None) or {}
    cur: Any = cfg
    for part in str(key).split("."):
        if not isinstance(cur, Mapping):
            return default
        cur = cur.get(part)
        if cur is None:
            return default
    return cur


def _rewatches_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "cfg", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_history_rewatches"))


def _to_float(v: Any) -> float | None:
    if v is None or isinstance(v, bool):
        return None
    try:
        return float(str(v).strip())
    except (TypeError, ValueError):
        return None


def _to_int(v: Any) -> int | None:
    if v is None or isinstance(v, bool):
        return None
    if isinstance(v, int):
        return v
    if isinstance(v, float):
        return int(v) if v.is_integer() else None
    s = str(v).strip()
    if not s or not s.isdigit():
        return None
    return int(s)


def _min_percent(adapter: Any, media_type: str) -> float:
    key = "min_percent_episode" if media_type == "episode" else "min_percent_movie"
    value = _to_float(_cfg_get(adapter, f"tracearr.history.{key}", _DEFAULT_MIN_PERCENT))
    return _DEFAULT_MIN_PERCENT if value is None else value


def _row_is_watched(row: Mapping[str, Any], min_percent: float) -> bool:
    watched = row.get("watched")
    if isinstance(watched, bool):
        return watched
    pc = _to_float(row.get("percent_complete"))
    if pc is not None:
        return pc >= min_percent
    return False


def _iso_z(v: Any) -> str | None:
    s = str(v or "").strip()
    if not s:
        return None
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if not dt.tzinfo:
        dt = dt.replace(tzinfo=timezone.utc)
    if dt.timestamp() <= 0:
        return None
    return dt.astimezone(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _watched_at(row: Mapping[str, Any]) -> str | None:
    for field in ("stopped_at", "started_at"):
        value = _iso_z(row.get(field))
        if value:
            return value
    return None


def _ext_ids(obj: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    imdb = str(obj.get("imdb_id") or "").strip()
    if imdb.lower().startswith("tt"):
        out["imdb"] = imdb
    for key in ("tmdb", "tvdb"):
        n = _to_int(obj.get(f"{key}_id"))
        if n and n > 0:
            out[key] = str(n)
    return out


def _has_ext_ids(ids: Mapping[str, str]) -> bool:
    return any(ids.get(k) for k in _EXT_ID_KEYS)


def _local_key(row: Mapping[str, Any]) -> str | None:
    return _LOCAL_ID_KEYS.get(str(row.get("server_type") or "").strip().lower())


def _first_text(*vals: Any) -> str | None:
    for v in vals:
        if v is None:
            continue
        s = str(v).strip()
        if s and s.lower() not in ("none", "null"):
            return s
    return None


def _movie_item(row: Mapping[str, Any], watched_at: str) -> dict[str, Any] | None:
    ids = _ext_ids(row)
    local = _local_key(row)
    rk = _first_text(row.get("rating_key"))
    if local and rk:
        ids[local] = rk
    if not ids:
        return None
    return {
        "type": "movie",
        "title": _first_text(row.get("media_title")),
        "year": _to_int(row.get("year")),
        "ids": ids,
        "watched_at": watched_at,
    }


def _episode_item(row: Mapping[str, Any], watched_at: str, show_ids: Mapping[str, str]) -> dict[str, Any] | None:
    season = _to_int(row.get("season_number"))
    episode = _to_int(row.get("episode_number"))
    if season is None or episode is None:
        return None
    ids = dict(show_ids)
    local = _local_key(row)
    grk = _first_text(row.get("grandparent_rating_key"))
    if local and grk and not ids.get(local):
        ids[local] = grk
    if not ids:
        return None
    item: dict[str, Any] = {
        "type": "episode",
        "ids": ids,
        "title": _first_text(row.get("media_title")),
        "year": _to_int(row.get("year")),
        "season": season,
        "episode": episode,
        "watched_at": watched_at,
    }
    series = _first_text(row.get("show_title"))
    if series:
        item["series_title"] = series
    if _has_ext_ids(ids):
        item["show_ids"] = {k: v for k, v in ids.items() if k in _EXT_ID_KEYS}
    return item


def _title_key(row: Mapping[str, Any]) -> tuple[str, str] | None:
    title = _first_text(row.get("show_title"))
    if not title:
        return None
    return str(row.get("server_id") or "").strip(), title.casefold()


def _learn_show(titles: dict[tuple[str, str], set[tuple[tuple[str, str], ...]]], row: Mapping[str, Any], show_ids: Mapping[str, str]) -> None:
    tk = _title_key(row)
    ext = tuple(sorted((k, v) for k, v in show_ids.items() if k in _EXT_ID_KEYS and v))
    if tk and ext:
        titles.setdefault(tk, set()).add(ext)


def _title_show_ids(titles: Mapping[tuple[str, str], set[tuple[tuple[str, str], ...]]], row: Mapping[str, Any]) -> dict[str, str]:
    tk = _title_key(row)
    found = titles.get(tk) if tk else None
    if not found or len(found) != 1:
        return {}
    return dict(next(iter(found)))


def build_index(adapter: Any, *, per_page: int = 100, max_pages: int = 5000) -> dict[str, dict[str, Any]]:
    client = getattr(adapter, "client", None)
    if not client:
        return {}

    user_id = str(_cfg_get(adapter, "tracearr.history.user_id", "") or "").strip()
    server_id = str(_cfg_get(adapter, "tracearr.history.server_id", "") or "").strip()
    cfg_per_page = max(1, min(100, int(_cfg_get(adapter, "tracearr.history.per_page", per_page) or per_page)))
    cfg_max_pages = int(_cfg_get(adapter, "tracearr.history.max_pages", max_pages) or max_pages)
    if cfg_max_pages <= 0:
        cfg_max_pages = max_pages
    watched_only = bool(_cfg_get(adapter, "tracearr.history.watched_only", True))

    _log(
        "index_fetch_counts",
        per_page=cfg_per_page,
        max_pages=cfg_max_pages,
        has_user_id=bool(user_id),
        has_server_id=bool(server_id),
        watched_only=watched_only,
    )

    out: dict[str, dict[str, Any]] = {}
    cursor: str | None = None
    seen_cursors: set[str] = set()
    pages = 0

    rows_seen = 0
    rows_kept = 0
    skipped_type = 0
    skipped_partial = 0
    skipped_no_time = 0
    skipped_no_ids = 0
    title_matched = 0
    event_mode = _rewatches_enabled(adapter)
    titles: dict[tuple[str, str], set[tuple[tuple[str, str], ...]]] = {}
    deferred: list[tuple[dict[str, Any], str]] = []

    def _emit(item: dict[str, Any], row: Mapping[str, Any], watched_at: str) -> None:
        nonlocal rows_kept
        if event_mode:
            key = history_sync_key(item, event_mode=True)
            if key and key in out:
                event_id = str(row.get("id") or rows_seen).strip()
                key = f"{key}~tr{event_id or rows_seen}"
            if key:
                out[key] = minimal_history_item(item, key, event_mode=True)
                rows_kept += 1
            return

        ck = canonical_key(item)
        if ck and (ck not in out or watched_at > out[ck]["watched_at"]):
            if ck not in out:
                rows_kept += 1
            out[ck] = item

    while True:
        pages += 1
        if pages > cfg_max_pages:
            _log("index_reconcile", level="warn", reason="max_pages_reached", pages=pages, max_pages=cfg_max_pages)
            break

        params: dict[str, Any] = {"pageSize": cfg_per_page}
        if cursor:
            params["cursor"] = cursor
        if user_id:
            params["user_id"] = user_id
        if server_id:
            params["server_id"] = server_id

        payload = client.get("/history", **params) or {}
        rows = payload.get("data") if isinstance(payload, Mapping) else None
        meta = payload.get("meta") if isinstance(payload, Mapping) else None
        if not isinstance(rows, list) or not rows:
            break

        _log("index_fetch_counts", page=pages, batch=len(rows))

        for row in rows:
            if not isinstance(row, Mapping):
                continue
            rows_seen += 1

            mtype = str(row.get("media_type") or "").lower()
            if mtype not in ("movie", "episode"):
                skipped_type += 1
                continue

            if watched_only and not _row_is_watched(row, _min_percent(adapter, mtype)):
                skipped_partial += 1
                continue

            watched_at = _watched_at(row)
            if not watched_at:
                skipped_no_time += 1
                continue

            if mtype == "movie":
                item = _movie_item(row, watched_at)
            else:
                show_ids = client.show_ids(row.get("show_media_id"))
                if not _has_ext_ids(show_ids):
                    deferred.append(({k: row.get(k) for k in _DEFERRED_FIELDS}, watched_at))
                    continue
                _learn_show(titles, row, show_ids)
                item = _episode_item(row, watched_at, show_ids)
            if not item:
                skipped_no_ids += 1
                continue
            _emit(item, row, watched_at)

        next_cursor = str((meta or {}).get("nextCursor") or "").strip() if isinstance(meta, Mapping) else ""
        if not next_cursor:
            break
        if next_cursor in seen_cursors:
            _log("index_reconcile", level="warn", reason="cursor_repeat", pages=pages)
            break
        seen_cursors.add(next_cursor)
        cursor = next_cursor

    for row, watched_at in deferred:
        show_ids = _title_show_ids(titles, row)
        if show_ids:
            title_matched += 1
        item = _episode_item(row, watched_at, show_ids)
        if not item:
            skipped_no_ids += 1
            continue
        _emit(item, row, watched_at)

    _log(
        "index_done",
        level="info",
        count=len(out),
        pages=pages,
        rows_seen=rows_seen,
        rows_kept=rows_kept,
        skipped_type=skipped_type,
        skipped_partial=skipped_partial,
        skipped_no_time=skipped_no_time,
        skipped_no_ids=skipped_no_ids,
        title_matched=title_matched,
    )
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    _ = (adapter, items, dry_run)
    _log("write_skipped", level="info", op="add", reason="read_only")
    return {"ok": True, "count": 0, "unresolved": [], "reason": "read_only"}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    _ = (adapter, items, dry_run)
    _log("write_skipped", level="info", op="remove", reason="read_only")
    return {"ok": True, "count": 0, "unresolved": [], "reason": "read_only"}

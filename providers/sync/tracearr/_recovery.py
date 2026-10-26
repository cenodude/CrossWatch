# providers/sync/tracearr/_recovery.py
# CrossWatch - Tracearr Module for Plex history recovery
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any, Callable, Mapping

from . import _history as history

_KNOWN = "Known identity"
_MISSING = "Missing identity — enter a match"
_MAX_PAGES = 5000


def _label(row: Mapping[str, Any], kind: str) -> str:
    title = history._first_text(row.get("media_title")) or "Untitled item"
    if kind == "episode":
        series = history._first_text(row.get("show_title")) or title
        season, episode = row.get("season_number"), row.get("episode_number")
        return f"{series} · S{season if season is not None else '?'} E{episode if episode is not None else '?'} · {title}"
    return f"{title} ({row.get('year')})" if row.get("year") else title


def _read(adapter: Any, user_id: str, progress: Callable[..., None], check_cancel: Callable[[], None]) -> list[Mapping[str, Any]]:
    server_id = str(history._cfg_get(adapter, "tracearr.history.server_id", "") or "").strip()
    rows: list[Mapping[str, Any]] = []
    cursor: str | None = None
    seen: set[str] = set()
    for _ in range(_MAX_PAGES):
        check_cancel()
        params: dict[str, Any] = {"pageSize": 100, "cursor": cursor, "user_id": user_id or None, "server_id": server_id or None}
        payload = adapter.client.get("/history", **params)
        page = payload.get("data") if isinstance(payload, Mapping) else None
        if not isinstance(page, list):
            raise RuntimeError("tracearr_history_invalid_response")
        rows.extend(row for row in page if isinstance(row, Mapping))
        progress("history", "Reading Tracearr watch history", len(rows), 0)
        meta = payload.get("meta") if isinstance(payload, Mapping) else None
        cursor = str((meta.get("nextCursor") if isinstance(meta, Mapping) else "") or "").strip() or None
        if not cursor or not page:
            return rows
        if cursor in seen:
            raise RuntimeError("tracearr_history_repeated_cursor")
        seen.add(cursor)
    raise RuntimeError(f"tracearr_history_page_cap_reached:{_MAX_PAGES}")


def _mine(row: Mapping[str, Any], user_id: str) -> bool:
    user = row.get("user")
    return str((user.get("id") if isinstance(user, Mapping) else None) or user_id) == user_id


def scan(adapter: Any, *, progress: Callable[..., None], check_cancel: Callable[[], None],
         max_rows: int = 50_000) -> list[dict[str, Any]]:
    check_cancel()
    user_id = str(history._cfg_get(adapter, "tracearr.history.user_id", "") or "").strip()
    if not user_id:
        raise ValueError("tracearr_user_unknown")
    watched_only = bool(history._cfg_get(adapter, "tracearr.history.watched_only", True))
    progress("history", "Reading Tracearr watch history", 0, 0)
    rows = _read(adapter, user_id, progress, check_cancel)
    recovered: list[dict[str, Any]] = []
    skipped = 0

    def report(checked: int, title: str = "", action: str = "", result: str = "") -> None:
        activity: dict[str, Any] = dict(checked=checked, found=len(recovered), skipped=skipped,
                                        current_item=title, current_action=action)
        if result and title:
            activity["event"] = dict(title=title, result=result)
        progress("matching", "Matching Tracearr history", checked, len(rows), activity)

    report(0)
    for index, row in enumerate(rows):
        check_cancel()
        kind = str(row.get("media_type") or "").lower()
        if (kind not in ("movie", "episode") or str(row.get("server_type") or "").lower() != "plex"
                or not _mine(row, user_id)
                or (watched_only and not history._row_is_watched(row, history._min_percent(adapter, kind)))):
            skipped += 1
            report(index + 1)
            continue
        title = _label(row, kind)
        year = history._to_int(row.get("year"))
        if kind == "movie":
            ids = history._ext_ids(row)
            original = history._first_text(row.get("media_title")) or ""
            item: dict[str, Any] = {"type": "movie", "title": original, "year": year, "ids": dict(ids)}
            identity = bool(ids)
            group = row.get("media_id") or row.get("rating_key")
        else:
            report(index, title, "Looking up the show at Tracearr")
            found = adapter.client.show_ids(row.get("show_media_id")) or {}
            ids = {key: str(found[key]) for key in history._EXT_ID_KEYS if found.get(key)}
            original = history._first_text(row.get("show_title"), row.get("media_title")) or ""
            season, episode = history._to_int(row.get("season_number")), history._to_int(row.get("episode_number"))
            code = f"S{season:02d}E{episode:02d}" if season is not None and episode is not None else original
            year = None
            item = {"type": "episode", "title": code, "series_title": original, "year": year,
                    "season": season, "episode": episode, "ids": {}, "show_ids": dict(ids)}
            identity = bool(ids) and season is not None and episode is not None
            group = row.get("show_media_id") or row.get("grandparent_rating_key")
        item["watched_at"] = history._watched_at(row)
        item["watched"] = True
        recovered.append({"feature": "history", "item": item, "source_path": "Tracearr history",
                          "recovery_match": _KNOWN if identity else _MISSING, "recovery_valid": identity,
                          "recovery_requires_review": False,
                          "original_group": json.dumps(["show" if kind == "episode" else "movie", str(group or ""), [],
                                                        original.strip().casefold(), str(year or "")], ensure_ascii=True),
                          "original_year": year, "original_title": original})
        if len(recovered) > max_rows:
            raise RuntimeError(f"More than {max_rows:,} watch events. Set a Tracearr user for this instance to narrow the history.")
        result = "Found for review"
        if not identity:
            result = "Found: needs a match"
        elif not item["watched_at"]:
            result = "Found: missing watch date"
        report(index + 1, title, result=result)
    return recovered

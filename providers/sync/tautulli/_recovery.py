# /providers/sync/tautulli/_recovery.py
# Tautulli Module for history recovery
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any, Callable, Mapping

from cw_platform.id_map import ids_from_guid

from . import _history as history

_EXTERNAL = ("tmdb", "imdb", "tvdb")
_KNOWN = "Known identity"
_GUID = "Matched by Plex GUID"
_MISSING = "Missing identity — enter a match"


def _external(ids: Mapping[str, Any] | None) -> dict[str, str]:
    return {key: str(ids[key]) for key in _EXTERNAL if ids and ids.get(key)}


def _guid_ids(*values: Any) -> dict[str, str]:
    ids: dict[str, str] = {}
    for value in values:
        for guid in value if isinstance(value, (list, tuple)) else [value]:
            text = history._clean_guid(str(guid or ""))
            if text:
                ids.update(ids_from_guid(text))
    return _external(ids)


def _label(row: Mapping[str, Any], kind: str) -> str:
    title = str(row.get("title") or row.get("full_title") or "Untitled item")
    if kind == "episode":
        series = history._series_title(row, None, None) or title
        season, episode = row.get("parent_media_index"), row.get("media_index")
        return f"{series} · S{season if season not in (None, '') else '?'} E{episode if episode not in (None, '') else '?'} · {title}"
    return f"{title} ({row.get('year')})" if row.get("year") else title


def _read(adapter: Any, progress: Callable[..., None], check_cancel: Callable[[], None]) -> list[Mapping[str, Any]]:
    user_id = str(history._cfg_get(adapter, "tautulli.history.user_id", "") or "").strip()
    per_page = max(1, min(500, int(history._cfg_get(adapter, "tautulli.history.per_page", 100) or 100)))
    rows: list[Mapping[str, Any]] = []
    previous: list[Any] | None = None
    start = 0
    while True:
        check_cancel()
        params: dict[str, Any] = dict(start=start, length=per_page, order_column="date", order_dir="desc",
                                      grouping=1, include_activity=0)
        if user_id:
            params["user_id"] = user_id
        payload = adapter.client.call("get_history", **params) or {}
        block = payload.get("data") if isinstance(payload, Mapping) else None
        if isinstance(block, Mapping):
            payload, block = block, block.get("data")
        page = list(block) if isinstance(block, list) else []
        if not page or page == previous:
            break
        previous = page
        rows.extend(row for row in page if isinstance(row, Mapping))
        total = history._to_int_total(payload.get("recordsFiltered") or payload.get("recordsTotal"))
        progress("history", "Reading Tautulli watch history", len(rows), max(total or 0, len(rows)))
        start += per_page
        if len(page) < per_page:
            break
    return rows


def scan(adapter: Any, *, resolve: Callable[[str, str], Mapping[str, Any]], progress: Callable[..., None],
         check_cancel: Callable[[], None], max_rows: int = 50_000) -> list[dict[str, Any]]:
    check_cancel()
    watched_only = bool(history._cfg_get(adapter, "tautulli.history.watched_only", True))
    min_percent = history._to_float(history._cfg_get(adapter, "tautulli.history.min_percent", 90))
    if min_percent is None:
        min_percent = 90.0
    progress("history", "Reading Tautulli watch history", 0, 0)
    rows = _read(adapter, progress, check_cancel)
    library: dict[str, dict[str, str]] = {}
    cloud: dict[tuple[str, str], dict[str, str]] = {}
    shows: dict[str, tuple[dict[str, str], str]] = {}
    pending: list[tuple[dict[str, Any], str]] = []
    recovered: list[dict[str, Any]] = []
    skipped = 0

    def report(checked: int, title: str = "", action: str = "", result: str = "") -> None:
        activity: dict[str, Any] = dict(checked=checked, found=len(recovered), skipped=skipped,
                                        current_item=title, current_action=action)
        if result and title:
            activity["event"] = dict(title=title, result=result)
        progress("matching", "Matching Tautulli history", checked, len(rows), activity)

    def lookup(rating_key: str) -> dict[str, str]:
        if rating_key and rating_key not in library:
            try:
                found = adapter.client.call("get_metadata", rating_key=rating_key)
            except Exception:
                found = None
            library[rating_key] = _guid_ids(found.get("guid"), found.get("guids")) if isinstance(found, Mapping) else {}
        return library.get(rating_key, {})

    def plex(kind: str, guid: str) -> dict[str, str]:
        if not guid.lower().startswith("plex://"):
            return {}
        if (kind, guid) not in cloud:
            cloud[(kind, guid)] = _external(resolve(kind, guid))
        return cloud[(kind, guid)]

    report(0)
    for index, row in enumerate(rows):
        check_cancel()
        kind = str(row.get("media_type") or "").lower()
        if kind not in ("movie", "episode") or (watched_only and not history._row_is_watched(row, min_percent)):
            skipped += 1
            report(index + 1)
            continue
        title = _label(row, kind)
        guid = history._clean_guid(str(row.get("guid") or ""))
        rating_key = str(row.get("rating_key") or "").strip()
        show_key = str(row.get("grandparent_rating_key") or "").strip() if kind == "episode" else ""
        year = history._to_int(row.get("year"))
        ids, method = _guid_ids(guid), _KNOWN
        if not ids and show_key in shows:
            ids, method = shows[show_key]
        if not ids:
            report(index, title, "Checking Tautulli metadata")
            ids = lookup(show_key if kind == "episode" else rating_key)
        if not ids:
            report(index, title, "Looking up identity at Plex")
            ids, method = plex(kind, guid), _GUID
        if ids and show_key:
            shows[show_key] = (ids, method)
        if kind == "movie":
            original = str(row.get("title") or row.get("full_title") or "")
            item: dict[str, Any] = {"type": "movie", "title": original, "year": year, "ids": dict(ids)}
            identity = bool(ids)
        else:
            original = history._series_title(row, None, None) or str(row.get("title") or "")
            season = history._to_int(row.get("parent_media_index"))
            episode = history._to_int(row.get("media_index"))
            code = f"S{season:02d}E{episode:02d}" if season and episode else original
            item = {"type": "episode", "title": code, "series_title": original, "year": year,
                    "season": season, "episode": episode, "ids": {}, "show_ids": dict(ids)}
            identity = bool(ids) and season is not None and episode is not None
        item["watched_at"] = history._watched_at(row)
        item["watched"] = True
        entry = {"feature": "history", "item": item, "source_path": "Tautulli history",
                 "recovery_match": method if identity else _MISSING, "recovery_valid": identity,
                 "recovery_requires_review": False,
                 "original_group": json.dumps(["show" if kind == "episode" else "movie", show_key or rating_key or guid, [],
                                               original.strip().casefold(), str(year or "")], ensure_ascii=True),
                 "original_year": year, "original_title": original}
        recovered.append(entry)
        if len(recovered) > max_rows:
            raise RuntimeError(f"More than {max_rows:,} watch events. Set a Tautulli user for this instance to narrow the history.")
        if not ids and show_key:
            pending.append((entry, show_key))
        result = "Found for review"
        if not identity:
            result = "Found: needs a match"
        elif not item["watched_at"]:
            result = "Found: missing watch date"
        report(index + 1, title, result=result)
    for entry, show_key in pending:
        if show_key not in shows:
            continue
        ids, method = shows[show_key]
        item = entry["item"]
        item["show_ids"] = dict(ids)
        if item.get("season") is not None and item.get("episode") is not None:
            entry.update(recovery_valid=True, recovery_match=method)
    return recovered

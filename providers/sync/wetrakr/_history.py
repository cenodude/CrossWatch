# providers/sync/wetrakr/_history.py
# CrossWatch - WeTrakr watched state and individual watch events
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.interactive_reads import retained_read

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.history_events import history_epoch_from_value, minimal_history_item
from providers.sync._log import log
from ._common import WeTrakrSyncError, item_key, media_item, tracking_rows, write_items


LOGGED_RECORDS = 20
SHOWN_RECORDS = 3
MESSAGE_LIMIT = 480
SUFFIX_RESERVE = 70
INVALID_REASONS = frozenset({
    "invalid_history_item", "invalid_media_type", "missing_episode_show", "invalid_episode_coordinates",
    "missing_media_identity", "missing_watch_timestamp", "missing_history_id",
})


def _scalar(value: Any, limit: int = 64) -> str:
    if isinstance(value, bool) or not isinstance(value, (str, int)):
        return ""
    return str(value).strip()[:limit]


def _ids(value: Any) -> dict[str, str]:
    if not isinstance(value, Mapping):
        return {}
    out: dict[str, str] = {}
    for key in ("tmdb", "imdb", "tvdb"):
        raw = value.get(key)
        text = _scalar(raw.get("id") if isinstance(raw, Mapping) else raw)
        if text:
            out[key] = text
    return out


def invalid_record(row: Mapping[str, Any], media: Any, kind: str, reason: str, event_mode: bool) -> dict[str, Any]:
    media = media if isinstance(media, Mapping) else {}
    show = media.get("show") if isinstance(media.get("show"), Mapping) else {}
    fields = {
        "reason": (reason, 64), "type": (kind, 16), "id": (media.get("id"), 64),
        "play_id": (row.get("id") if event_mode else None, 64), "title": (media.get("title"), 80),
        "show_title": (show.get("title"), 80), "show_id": (show.get("id"), 64),
        "season": (media.get("season_number") if kind == "episode" else None, 16),
        "episode": (media.get("number") if kind == "episode" else None, 16), "watched_at": (row.get("watched_at"), 40),
    }
    record: dict[str, Any] = {key: text for key, (value, limit) in fields.items() if (text := _scalar(value, limit))}
    for key, value in (("ids", _ids(media.get("ids"))), ("show_ids", _ids(show.get("ids")))):
        if value:
            record[key] = value
    return record


def describe_record(record: Mapping[str, Any]) -> str:
    kind = record.get("type") or "item"
    parts = [f"{kind} '{record.get('show_title') or record.get('title') or 'untitled'}'"]
    if kind == "episode":
        parts.append(f"season={record.get('season', 'missing')} episode={record.get('episode', 'missing')}")
    parts += [f"{key}={record[key]}" for key in ("id", "show_id", "play_id") if record.get(key)]
    ids = {**record.get("show_ids", {}), **record.get("ids", {})} if kind == "movie" else record.get("show_ids", {})
    if ids:
        parts.append("ids=" + ",".join(f"{key}:{value}" for key, value in ids.items()))
    return f"{' '.join(parts)} ({record.get('reason') or 'invalid'})"


def _history_item(row: Mapping[str, Any], media: Any, kind: str, event_mode: bool) -> dict[str, Any]:
    if not isinstance(media, Mapping):
        raise WeTrakrSyncError("invalid_history_item")
    item = media_item(media, kind)
    item["watched"] = True
    if row.get("watched_at_unknown"):
        item["_wetrakr_watched_at_unknown"] = True
    elif history_epoch_from_value(row.get("watched_at")) is not None:
        item["watched_at"] = row["watched_at"]
    elif event_mode:
        raise WeTrakrSyncError("missing_watch_timestamp")
    if event_mode:
        event_id = str(row.get("id") or "")
        if not event_id:
            raise WeTrakrSyncError("missing_history_id")
        item["_wetrakr_history_id"] = event_id
    return minimal_history_item(item, event_mode=event_mode)


def invalid_summary(records: list[dict[str, Any]], total: int) -> str:
    text = f"WeTrakr returned {total} unreadable history {'record' if total == 1 else 'records'}: "
    shown = 0
    for record in records[:SHOWN_RECORDS]:
        part = ("; " if shown else "") + describe_record(record)
        if shown and len(text) + len(part) > MESSAGE_LIMIT - SUFFIX_RESERVE:
            break
        text += part
        shown += 1
    remaining = total - shown
    listed = len(records) - shown
    if remaining and listed == remaining:
        text += f"; {remaining} more in the log"
    elif remaining and listed:
        text += f"; {remaining} more, the log lists {listed} of them"
    elif remaining:
        text += f"; {remaining} more not listed"
    return text


@retained_read
def build_index(adapter: Any, *, force: bool = False) -> dict[str, dict[str, Any]]:
    event_mode = bool(adapter.config.get("_cw_history_rewatches"))
    out: dict[str, dict[str, Any]] = {}
    invalid: list[dict[str, Any]] = []
    invalid_count = 0
    snapshot = tracking_rows(adapter, "history", force=force)
    for kind in ("movie", "episode"):
        for row in snapshot[kind]:
            media = row.get(kind) if event_mode else row
            try:
                item = _history_item(row, media, kind, event_mode)
            except WeTrakrSyncError as exc:
                if exc.reason not in INVALID_REASONS:
                    raise
                invalid_count += 1
                if len(invalid) < LOGGED_RECORDS:
                    record = invalid_record(row, media, kind, exc.reason, event_mode)
                    invalid.append(record)
                    log("WETRAKR", "history", "warn", "history_record_invalid", record=describe_record(record), **record)
                continue
            key = item_key(adapter, "history", item)
            if key in out:
                if not event_mode or out[key].get("_wetrakr_history_id") == item.get("_wetrakr_history_id"):
                    raise WeTrakrSyncError("duplicate_history_identity")
                key += "~" + item["_wetrakr_history_id"]
                if key in out:
                    raise WeTrakrSyncError("duplicate_history_identity")
                item["_cw_event_key"] = key
            out[key] = item
    if invalid_count:
        log("WETRAKR", "history", "error", "history_records_invalid", count=invalid_count, logged=len(invalid))
        raise WeTrakrSyncError("invalid_history_records", detail=invalid_summary(invalid, invalid_count))
    log("WETRAKR", "history", "info", "index_done", count=len(out), rewatches=event_mode)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "history", items, remove=False, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return write_items(adapter, "history", items, remove=True, dry_run=dry_run)

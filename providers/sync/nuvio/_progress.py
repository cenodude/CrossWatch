# providers/sync/nuvio/_progress.py
# CrossWatch Nuvio progress functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal

from providers.sync._log import log as cw_log
from providers.sync._progress_policy import decide_progress_write, progress_materially_equal, select_progress_record

from ._common import (
    canonical_item_key,
    content_id_for_item,
    epoch_ms,
    make_item,
    positive_int,
    progress_key,
    pull_watch_progress_rows,
    resolve_episode,
    rpc,
    selected_profile_id,
    to_int,
)


def _info(event: str, **fields: Any) -> None:
    cw_log("NUVIO", "progress", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("NUVIO", "progress", "warn", event, **fields)


def _progress_percent(position: int, duration: int) -> float | None:
    if position < 0 or duration <= 0:
        return None
    return round((float(position) / float(duration)) * 100.0, 3)


def _progress_ms(item: Mapping[str, Any]) -> int | None:
    for key in ("progress_ms", "progressMs", "position", "position_ms", "positionMs", "viewOffset", "progress"):
        number = to_int(item.get(key))
        if number is not None:
            return number
    return None


def _duration_ms(item: Mapping[str, Any]) -> int | None:
    for key in ("duration_ms", "durationMs", "duration", "runtime_ms", "runtimeMs"):
        number = positive_int(item.get(key))
        if number is not None:
            return number
    return None


def _item_from_row(row: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    base = make_item(
        content_id=row.get("content_id"),
        content_type=row.get("content_type"),
        season=row.get("season"),
        episode=row.get("episode"),
        title=row.get("title") or row.get("name"),
        year=row.get("year"),
    )
    position = to_int(row.get("position"))
    duration = positive_int(row.get("duration"))
    last_watched = epoch_ms(row.get("last_watched"))
    video_id = str(row.get("video_id") or "").strip()
    content_id = str(row.get("content_id") or "").strip()
    if not base or position is None or position < 0 or duration is None or last_watched is None or not video_id:
        return None, None, "nuvio_progress_invalid"

    item = dict(base)
    item.update(
        {
            "progress_ms": int(position),
            "duration_ms": int(duration),
            "progress_at": int(last_watched),
            "progress_percent": _progress_percent(int(position), int(duration)),
            "progress_key": str(row.get("progress_key") or progress_key(item) or "").strip(),
            "_nuvio_content_id": content_id,
            "_nuvio_video_id": video_id,
            "_nuvio_profile_id": row.get("profile_id"),
            "_nuvio_last_watched": int(last_watched),
        }
    )
    key = canonical_item_key(item)
    if not key or key == "unknown:":
        return None, None, "nuvio_id_missing"
    return key, item, None


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    rows = pull_watch_progress_rows(adapter)
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        key, item, reason = _item_from_row(row)
        if not key or not item:
            if reason:
                skipped += 1
                _warn("row_skipped", reason=reason)
            continue
        chosen, _action = select_progress_record(out.get(key), item)
        out[key] = chosen
    _info("index_done", count=len(out), rows=len(rows), skipped=skipped)
    return out


def _payload_for_item(adapter: Any, item: Mapping[str, Any], current_rows: Any = None) -> tuple[dict[str, Any] | None, str | None]:
    it = dict(item or {})
    mini = id_minimal(it)
    typ = str(mini.get("type") or it.get("type") or "").strip().lower()
    content_id = content_id_for_item(it)
    if not content_id:
        return None, "nuvio_id_missing"

    position = _progress_ms(mini) if isinstance(mini, Mapping) else None
    if position is None:
        position = _progress_ms(it)
    if position is None or position < 0:
        return None, "nuvio_progress_invalid"

    duration = _duration_ms(mini) if isinstance(mini, Mapping) else None
    if duration is None:
        duration = _duration_ms(it)
    if duration is None:
        return None, "nuvio_duration_missing"

    last_watched = epoch_ms(mini.get("progress_at") if isinstance(mini, Mapping) else None)
    if last_watched is None:
        last_watched = epoch_ms(it.get("progress_at") or it.get("last_watched") or it.get("lastViewedAt") or it.get("last_played"))
    if last_watched is None:
        return None, "nuvio_progress_invalid"

    if typ in {"episode", "episodes"}:
        resolved = resolve_episode(adapter, it, current_rows=current_rows)
        if not resolved.ok:
            return None, resolved.reason
        return {
            "content_id": resolved.content_id,
            "content_type": "series",
            "video_id": resolved.video_id,
            "season": resolved.destination_season,
            "episode": resolved.destination_episode,
            "position": int(position),
            "duration": int(duration),
            "last_watched": int(last_watched),
        }, None

    if typ not in {"movie", "movies"}:
        return None, "nuvio_id_missing"

    return {
        "content_id": content_id,
        "content_type": "movie",
        "video_id": content_id,
        "season": None,
        "episode": None,
        "position": int(position),
        "duration": int(duration),
        "last_watched": int(last_watched),
    }, None


def _unresolved(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {"status": "unresolved", "reason": reason, "item": id_minimal(item)}


def _confirmed_after_write(after: Mapping[str, Mapping[str, Any]], key: str, payload: Mapping[str, Any]) -> bool:
    row = after.get(key)
    return bool(
        isinstance(row, Mapping)
        and progress_materially_equal(payload.get("position"), payload.get("duration"), row.get("progress_ms"), row.get("duration_ms"))
        and (epoch_ms(row.get("progress_at")) or 0) >= (epoch_ms(payload.get("last_watched")) or 0)
    )


def _result(ok: bool, count: int, attempted: int, confirmed: list[str], unresolved: list[dict[str, Any]], results: list[dict[str, Any]], skipped: int, **extra: Any) -> dict[str, Any]:
    out = {
        "ok": bool(ok),
        "count": int(count),
        "attempted": int(attempted),
        "confirmed_keys": list(dict.fromkeys(k for k in confirmed if k)),
        "unresolved": unresolved,
        "unresolved_keys": list(dict.fromkeys(canonical_key((u.get("item") or {}) if isinstance(u, Mapping) else {}) for u in unresolved)),
        "results": results,
        "skipped": int(skipped),
        "errors": sum(1 for u in unresolved if str(u.get("status") or "") == "failed"),
    }
    out.update(extra)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]
    rows = pull_watch_progress_rows(adapter)
    current: dict[str, dict[str, Any]] = {}
    for row in rows:
        key, item, _reason = _item_from_row(row)
        if key and item:
            current[key] = item

    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    keys: list[str] = []
    pending_items: list[dict[str, Any]] = []
    skipped = 0

    for item in src:
        key = canonical_item_key(item)
        payload, reason = _payload_for_item(adapter, item, current_rows=rows)
        if payload is None:
            entry = _unresolved(item, reason or "nuvio_progress_invalid")
            unresolved.append(entry)
            results.append(entry)
            continue
        target = current.get(key)
        decision = decide_progress_write(
            active_session=False,
            source_timestamp=payload.get("last_watched"),
            target_timestamp=(target or {}).get("progress_at") if isinstance(target, Mapping) else None,
            source_progress_ms=payload.get("position"),
            source_duration_ms=payload.get("duration"),
            target_progress_ms=(target or {}).get("progress_ms") if isinstance(target, Mapping) else None,
            target_duration_ms=(target or {}).get("duration_ms") if isinstance(target, Mapping) else None,
            target_watched=False,
            same_origin=False,
            replay_enabled=True,
        )
        if not decision.apply:
            skipped += 1
            results.append({"status": "skipped", "reason": decision.reason, "item": id_minimal(item), "canonical_key": key})
            continue
        entries.append(payload)
        keys.append(key)
        pending_items.append(item)
        results.append({"status": "pending" if not dry_run else "dry_run", "item": id_minimal(item), "canonical_key": key})

    if dry_run:
        return _result(len(unresolved) == 0, len(entries), len(entries), [], unresolved, results, skipped, dry_run=True)

    write_failed = False
    if entries:
        try:
            rpc(adapter, "sync_push_watch_progress", {"p_profile_id": selected_profile_id(adapter), "p_entries": entries})
        except Exception:
            write_failed = True
            for item, key in zip(pending_items, keys):
                unresolved.append({"status": "failed", "reason": "nuvio_progress_write_failed", "item": id_minimal(item), "canonical_key": key})

    after = build_index(adapter) if entries and not write_failed else current
    confirmed: list[str] = []
    for key, payload in ([] if write_failed else zip(keys, entries)):
        if _confirmed_after_write(after, key, payload):
            confirmed.append(key)
        else:
            unresolved.append({"status": "failed", "reason": "nuvio_progress_verification_failed", "canonical_key": key, "item": dict(after.get(key) or {})})

    ok = len(unresolved) == 0
    _info("write_done", op="add", ok=ok, attempted=len(entries), confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(confirmed), len(entries), confirmed, unresolved, results, skipped)


def _remote_key_for_item(current: Mapping[str, Mapping[str, Any]], item: Mapping[str, Any]) -> tuple[str | None, str | None]:
    item_key = canonical_item_key(item)
    row = current.get(item_key)
    if isinstance(row, Mapping):
        remote = progress_key(row)
        if remote:
            return remote, item_key
    remote = progress_key(item)
    return remote, item_key


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    remote_keys: list[str] = []
    item_keys: list[str] = []
    pending_items: list[dict[str, Any]] = []
    skipped = 0

    for item in src:
        remote_key, item_key = _remote_key_for_item(current, item)
        if not remote_key:
            entry = _unresolved(item, "nuvio_video_id_missing")
            unresolved.append(entry)
            results.append(entry)
            continue
        if item_key and item_key not in current:
            skipped += 1
            results.append({"status": "skipped", "reason": "already_absent", "item": id_minimal(item), "canonical_key": item_key})
            continue
        remote_keys.append(remote_key)
        item_keys.append(item_key or "")
        pending_items.append(item)
        results.append({"status": "pending" if not dry_run else "dry_run", "item": id_minimal(item), "canonical_key": item_key})

    if dry_run:
        return _result(len(unresolved) == 0, len(remote_keys), len(remote_keys), [], unresolved, results, skipped, dry_run=True)

    delete_failed = False
    if remote_keys:
        try:
            rpc(adapter, "sync_delete_watch_progress", {"p_profile_id": selected_profile_id(adapter), "p_keys": remote_keys})
        except Exception:
            delete_failed = True
            for item, key in zip(pending_items, item_keys):
                unresolved.append({"status": "failed", "reason": "nuvio_progress_delete_failed", "item": id_minimal(item), "canonical_key": key})

    after = build_index(adapter) if remote_keys and not delete_failed else current
    confirmed: list[str] = []
    for item_key in ([] if delete_failed else item_keys):
        if item_key and item_key not in after:
            confirmed.append(item_key)
        elif item_key:
            unresolved.append({"status": "failed", "reason": "nuvio_progress_verification_failed", "canonical_key": item_key, "item": dict(after.get(item_key) or {})})

    ok = len(unresolved) == 0
    _info("write_done", op="remove", ok=ok, attempted=len(remote_keys), confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(confirmed), len(remote_keys), confirmed, unresolved, results, skipped)

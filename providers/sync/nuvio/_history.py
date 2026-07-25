# providers/sync/nuvio/_history.py
# CrossWatch Nuvio history functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal

from providers.sync._log import log as cw_log

from ._common import (
    canonical_item_key,
    epoch_ms,
    make_item,
    metadata_title_for_content_id,
    payload_item_key,
    positive_int,
    pull_watched_rows,
    resolve_content_id_for_item,
    resolve_episode,
    rpc,
    selected_profile_id,
)


def _info(event: str, **fields: Any) -> None:
    cw_log("NUVIO", "history", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("NUVIO", "history", "warn", event, **fields)


def _is_episode_code(value: Any) -> bool:
    return bool(re.fullmatch(r"S\d{1,3}E\d{1,4}", str(value or "").strip(), re.I))


def _series_title_from_episode_label(value: Any) -> str:
    match = re.fullmatch(r"\s*(.+?)\s+-\s+S\d{1,3}E\d{1,4}\s*", str(value or "").strip(), re.I)
    return match.group(1).strip() if match else str(value or "").strip()


def _episode_title(item: Mapping[str, Any], season: Any, episode: Any) -> str:
    code = f"S{int(season):02d}E{int(episode):02d}" if positive_int(season) and positive_int(episode) else ""
    series = str(item.get("series_title") or item.get("show_title") or "").strip()
    title = str(item.get("title") or "").strip()
    if series:
        return f"{series} - {code}" if code else series
    return title or code


def _item_from_row(adapter: Any, row: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None]:
    ctype = str(row.get("content_type") or "").strip().lower()
    title = str(row.get("title") or row.get("name") or "").strip()
    if ctype in {"series", "show"}:
        title = metadata_title_for_content_id(adapter, row.get("content_id"), "tv") if not title or _is_episode_code(title) else _series_title_from_episode_label(title)
    item = make_item(
        content_id=row.get("content_id"),
        content_type=ctype,
        season=row.get("season"),
        episode=row.get("episode"),
        title=title,
        year=row.get("year"),
    )
    watched_at = epoch_ms(row.get("watched_at"))
    if not item or watched_at is None:
        return None, None, "nuvio_history_invalid"
    item["watched"] = True
    item["watched_at"] = int(watched_at)
    item["_nuvio_content_id"] = str(row.get("content_id") or "").strip()
    item["_nuvio_profile_id"] = row.get("profile_id")
    key = canonical_item_key(item)
    if not key or key == "unknown:":
        return None, None, "nuvio_id_missing"
    return key, item, None


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    rows = pull_watched_rows(adapter)
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in rows:
        key, item, reason = _item_from_row(adapter, row)
        if not key or not item:
            if reason:
                skipped += 1
                _warn("row_skipped", reason=reason)
            continue
        existing = out.get(key)
        if not existing or (epoch_ms(item.get("watched_at")) or 0) >= (epoch_ms(existing.get("watched_at")) or 0):
            out[key] = item
    _info("index_done", count=len(out), rows=len(rows), skipped=skipped)
    return out


def _payload_for_item(adapter: Any, item: Mapping[str, Any], current_rows: Any = None) -> tuple[dict[str, Any] | None, str | None]:
    it = dict(item or {})
    mini = id_minimal(it)
    typ = str(mini.get("type") or it.get("type") or "").strip().lower()
    content_id = resolve_content_id_for_item(adapter, it)
    watched_at = epoch_ms(mini.get("watched_at") if isinstance(mini, Mapping) else None)
    if watched_at is None:
        watched_at = epoch_ms(it.get("watched_at") or it.get("last_watched_at") or it.get("lastWatchedAt"))
    if not content_id:
        return None, "nuvio_id_missing"
    if watched_at is None:
        return None, "nuvio_history_write_failed"

    if typ in {"episode", "episodes"}:
        resolved = resolve_episode(adapter, it, current_rows=current_rows)
        if not resolved.ok:
            return None, resolved.reason
        return {
            "content_id": resolved.content_id,
            "content_type": "series",
            "title": _episode_title(it, resolved.destination_season, resolved.destination_episode),
            "season": resolved.destination_season,
            "episode": resolved.destination_episode,
            "watched_at": int(watched_at),
        }, None
    if typ not in {"movie", "movies"}:
        return None, "nuvio_id_missing"
    title = str(it.get("title") or "").strip()
    return {"content_id": content_id, "content_type": "movie", "title": title, "watched_at": int(watched_at)}, None


def _delete_key_for_item(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any] | None:
    content_id = resolve_content_id_for_item(adapter, item)
    if not content_id:
        return None
    key: dict[str, Any] = {"content_id": content_id}
    season = positive_int(item.get("season"))
    episode = positive_int(item.get("episode"))
    if season and episode:
        key["season"] = season
        key["episode"] = episode
    return key


def _unresolved(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {"status": "unresolved", "reason": reason, "item": id_minimal(item)}


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
    rows = pull_watched_rows(adapter)
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    payloads: list[dict[str, Any]] = []
    keys: list[str] = []
    verify_keys: list[str] = []
    pending_items: list[dict[str, Any]] = []
    skipped = 0

    for item in src:
        key = canonical_item_key(item)
        payload, reason = _payload_for_item(adapter, item, current_rows=rows)
        if payload is None:
            entry = _unresolved(item, reason or "nuvio_history_write_failed")
            unresolved.append(entry)
            results.append(entry)
            continue
        verify_key = payload_item_key(payload) or key
        target = current.get(verify_key) or current.get(key)
        if target and (epoch_ms(target.get("watched_at")) or 0) >= int(payload.get("watched_at") or 0):
            skipped += 1
            results.append({"status": "skipped", "reason": "history_unchanged", "item": id_minimal(item), "canonical_key": key})
            continue
        payloads.append(payload)
        keys.append(key)
        verify_keys.append(verify_key)
        pending_items.append(item)
        results.append({"status": "pending" if not dry_run else "dry_run", "item": id_minimal(item), "canonical_key": key})

    if dry_run:
        return _result(len(unresolved) == 0, len(payloads), len(payloads), [], unresolved, results, skipped, dry_run=True)

    failed = False
    if payloads:
        try:
            rpc(adapter, "sync_push_watched_items", {"p_profile_id": selected_profile_id(adapter), "p_items": payloads})
        except Exception:
            failed = True
            for item, key in zip(pending_items, keys):
                unresolved.append({"status": "failed", "reason": "nuvio_history_write_failed", "item": id_minimal(item), "canonical_key": key})

    after = build_index(adapter) if payloads and not failed else current
    confirmed: list[str] = []
    for key, verify_key in ([] if failed else zip(keys, verify_keys)):
        if verify_key in after:
            confirmed.append(key)
        else:
            unresolved.append({"status": "failed", "reason": "nuvio_history_verification_failed", "canonical_key": key})

    ok = len(unresolved) == 0
    _info("write_done", op="add", ok=ok, attempted=len(payloads), confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(confirmed), len(payloads), confirmed, unresolved, results, skipped)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    src = [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]
    current = build_index(adapter)
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    keys_payload: list[dict[str, Any]] = []
    item_keys: list[str] = []
    verify_keys: list[str] = []
    pending_items: list[dict[str, Any]] = []
    skipped = 0

    for item in src:
        item_key = canonical_item_key(item)
        payload = _delete_key_for_item(adapter, current.get(item_key) or item)
        if not payload:
            entry = _unresolved(item, "nuvio_id_missing")
            unresolved.append(entry)
            results.append(entry)
            continue
        verify_key = payload_item_key(payload) or item_key
        if verify_key and verify_key not in current:
            skipped += 1
            results.append({"status": "skipped", "reason": "already_absent", "item": id_minimal(item), "canonical_key": item_key})
            continue
        keys_payload.append(payload)
        item_keys.append(item_key)
        verify_keys.append(verify_key)
        pending_items.append(item)
        results.append({"status": "pending" if not dry_run else "dry_run", "item": id_minimal(item), "canonical_key": item_key})

    if dry_run:
        return _result(len(unresolved) == 0, len(keys_payload), len(keys_payload), [], unresolved, results, skipped, dry_run=True)

    failed = False
    if keys_payload:
        try:
            rpc(adapter, "sync_delete_watched_items", {"p_profile_id": selected_profile_id(adapter), "p_keys": keys_payload})
        except Exception:
            failed = True
            for item, key in zip(pending_items, item_keys):
                unresolved.append({"status": "failed", "reason": "nuvio_history_write_failed", "item": id_minimal(item), "canonical_key": key})

    after = build_index(adapter) if keys_payload and not failed else current
    confirmed: list[str] = []
    for key, verify_key in ([] if failed else zip(item_keys, verify_keys)):
        if key and verify_key not in after:
            confirmed.append(key)
        elif key:
            unresolved.append({"status": "failed", "reason": "nuvio_history_verification_failed", "canonical_key": key})

    ok = len(unresolved) == 0
    _info("write_done", op="remove", ok=ok, attempted=len(keys_payload), confirmed=len(confirmed), skipped=skipped, unresolved=len(unresolved))
    return _result(ok, len(confirmed), len(keys_payload), confirmed, unresolved, results, skipped)

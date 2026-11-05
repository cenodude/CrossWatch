# /providers/sync/tofa/_history.py
# Tofa history sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.played_writes import note_write

from ._common import (
    TofaSyncError,
    all_files,
    cfg_section,
    chunks,
    completed_editions,
    completed_state,
    default_edition_files,
    error_of,
    failed_ids_of,
    iso_z,
    key_of,
    library,
    minimal_for,
    resolve_item,
    tofa_request,
    unresolved_entry,
    _dbg,
    _info,
    _warn,
)

FEATURE = "history"
_KINDS = ("movie", "episode")


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    state = completed_state(adapter, feature=FEATURE)
    targets: set[tuple[str, Any]] = set()
    unmatched = 0
    for fid in state:
        hit = lib.file_of.get(fid)
        if hit is None:
            unmatched += 1
            continue
        targets.add((hit[0], hit[1]))
    collected: dict[str, dict[str, Any]] = {}
    undated = 0
    for kind, target in targets:
        done = completed_editions(lib, kind, target, state)
        item = minimal_for(lib, kind, target) if done else None
        if not item:
            continue
        stamps = [s for _e, s in done if s]
        item["watched"] = True
        if stamps:
            item["watched_at"] = max(stamps)
        else:
            item["_cw_watched_state"] = True
            undated += 1
        key = key_of(item)
        if key:
            collected[key] = item
    _info(FEATURE, "index_done", count=len(collected), rows=len(state), unmatched=unmatched, undated=undated)
    return collected


def _post_batch(adapter: Any, entries: list[dict[str, Any]], *, action: str) -> tuple[bool, set[str]]:
    resp = tofa_request(adapter, "POST", "/media/watched/batch", json_body={"items": entries})
    if resp.status_code == 200:
        return True, set()
    if resp.status_code == 401:
        raise TofaSyncError("tofa_history_unauthorized")
    failed = failed_ids_of(resp) if resp.status_code == 404 else set()
    _warn(FEATURE, "write_failed", action=action, status=resp.status_code, code=error_of(resp) or None, failed_ids=len(failed) or None)
    return False, failed


def write_groups(adapter: Any, groups: dict[str, tuple[list[str], str | None]], *, watched: bool, action: str) -> tuple[set[str], dict[str, str]]:
    profile = cfg_section(adapter).get("profile_id")
    failed_keys: dict[str, str] = {}
    pending = dict(groups)
    for attempt in range(2):
        entries: list[dict[str, Any]] = []
        owner: dict[str, str] = {}
        for key, (files, stamp) in pending.items():
            for fid in files:
                entry: dict[str, Any] = {"media_file_id": fid, "watched": watched}
                if watched and stamp:
                    entry["watched_at"] = stamp
                entries.append(entry)
                owner[fid] = key
        retry: dict[str, tuple[list[str], str | None]] = {}
        for batch in chunks(entries):
            for entry in batch:
                note_write("tofa", profile, entry["media_file_id"])
            ok, missing = _post_batch(adapter, batch, action=action)
            if ok:
                continue
            batch_keys = {owner[e["media_file_id"]] for e in batch}
            bad_keys = {owner[fid] for fid in missing if fid in owner}
            for key in bad_keys:
                failed_keys[key] = "file_not_found"
            for key in batch_keys - bad_keys:
                if missing and attempt == 0:
                    retry[key] = pending[key]
                else:
                    failed_keys[key] = "write_failed"
        if not retry:
            break
        _dbg(FEATURE, "write_retry_without_failed_ids", action=action, groups=len(retry))
        pending = retry
    done = {key for key in groups if key not in failed_keys}
    return done, failed_keys


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    unresolved: list[dict[str, Any]] = []
    groups: dict[str, tuple[list[str], str | None]] = {}
    items_by_key: dict[str, Mapping[str, Any]] = {}
    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        target, kind = resolve_item(lib, item)
        if kind not in _KINDS:
            unresolved.append(unresolved_entry(key, "unsupported_type", item))
            continue
        files = default_edition_files(lib, kind, target) if target else []
        if not files:
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="not_in_library")
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        stamp = iso_z(item.get("watched_at"))
        if not stamp and not (item.get("_cw_watched_state") is True or item.get("watched") is True):
            unresolved.append(unresolved_entry(key, "missing_watched_at", item))
            continue
        groups[key] = (files, stamp)
        items_by_key[key] = item

    _dbg(FEATURE, "write_prepare", action="add", groups=len(groups), files=sum(len(f) for f, _s in groups.values()), unresolved=len(unresolved))
    done, failed = write_groups(adapter, groups, watched=True, action="add") if groups else (set(), {})
    confirmed: list[str] = []
    if done:
        state = completed_state(adapter, feature=FEATURE)
        for key in sorted(done):
            if all(fid in state for fid in groups[key][0]):
                confirmed.append(key)
            else:
                unresolved.append(unresolved_entry(key, "not_confirmed", items_by_key.get(key)))
                _dbg(FEATURE, "write_not_confirmed", key=key)
    for key, status in failed.items():
        unresolved.append(unresolved_entry(key, status, items_by_key.get(key)))
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    unresolved: list[dict[str, Any]] = []
    groups: dict[str, tuple[list[str], str | None]] = {}
    items_by_key: dict[str, Mapping[str, Any]] = {}
    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        target, kind = resolve_item(lib, item)
        files = all_files(lib, kind, target) if kind in _KINDS and target else []
        if not files:
            unresolved.append(unresolved_entry(key, "unsupported_type" if kind not in _KINDS else "not_in_library", item))
            continue
        groups[key] = (files, None)
        items_by_key[key] = item
    done, failed = write_groups(adapter, groups, watched=False, action="remove") if groups else (set(), {})
    for key, status in failed.items():
        unresolved.append(unresolved_entry(key, status, items_by_key.get(key)))
    confirmed = sorted(done)
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}

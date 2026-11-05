# /providers/sync/tofa/_progress.py
# Tofa progress sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from cw_platform.played_writes import note_write

from ._history import write_groups
from ._common import (
    MIN_POSITION_MS,
    TofaSyncError,
    all_files,
    as_int,
    cfg_section,
    completed_editions,
    completed_state,
    default_edition_files,
    durations,
    error_of,
    iso_z,
    key_of,
    library,
    minimal_for,
    resolve_item,
    safe_json,
    tofa_request,
    unresolved_entry,
    watch_state,
    _dbg,
    _info,
    _warn,
)

FEATURE = "progress"
POSITION_TOLERANCE_MS = 2000
_KINDS = ("movie", "episode")


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    state = watch_state(adapter, completed_only=False, feature=FEATURE)
    best: dict[tuple[str, Any], tuple[int, str | None, str]] = {}
    unmatched = 0
    for fid, row in state.items():
        pos = as_int(row.get("position_ms")) or 0
        if row.get("completed") or pos <= 0:
            continue
        hit = lib.file_of.get(fid)
        if hit is None:
            unmatched += 1
            continue
        target = (hit[0], hit[1])
        if completed_editions(lib, hit[0], hit[1], state):
            continue
        prev = best.get(target)
        if prev is None or pos > prev[0]:
            best[target] = (pos, iso_z(row.get("last_watched_at")), fid)
    lengths = durations(adapter, lib, best.keys(), feature=FEATURE) if best else {}
    collected: dict[str, dict[str, Any]] = {}
    for (kind, target), (pos, stamp, fid) in best.items():
        item = minimal_for(lib, kind, target)
        if not item:
            continue
        item["progress_ms"] = pos
        dur = lengths.get(fid)
        if dur and dur > 0:
            item["duration_ms"] = dur
            item["progress_percent"] = round(max(0.0, min(100.0, pos / dur * 100.0)), 3)
        if stamp:
            item["progress_at"] = stamp
        key = key_of(item)
        if key:
            collected[key] = item
    _info(FEATURE, "index_done", count=len(collected), rows=len(state), unmatched=unmatched)
    return collected


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    state = completed_state(adapter, feature=FEATURE)
    profile = cfg_section(adapter).get("profile_id")
    unresolved: list[dict[str, Any]] = []
    confirmed: list[str] = []
    skipped: list[str] = []
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
        pos = as_int(item.get("progress_ms"))
        if not pos or pos <= 0:
            unresolved.append(unresolved_entry(key, "missing_position", item))
            continue
        if completed_editions(lib, kind, target, state) or any(fid in state for fid in files):
            skipped.append(key)
            _dbg(FEATURE, "write_skipped", key=key, reason="completed_on_server")
            continue
        if pos < MIN_POSITION_MS:
            skipped.append(key)
            _dbg(FEATURE, "write_skipped", key=key, reason="below_server_minimum")
            continue
        note_write("tofa", profile, files[0])
        resp = tofa_request(adapter, "PUT", f"/media/{quote(files[0], safe='')}/progress", json_body={"position_ms": pos})
        if resp.status_code == 401:
            raise TofaSyncError("tofa_progress_unauthorized")
        if resp.status_code != 200:
            _warn(FEATURE, "write_failed", action="add", status=resp.status_code, code=error_of(resp) or None)
            unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))
            continue
        data = safe_json(resp)
        got = as_int(data.get("position_ms")) if isinstance(data, Mapping) else None
        if got is not None and abs(got - pos) <= POSITION_TOLERANCE_MS:
            confirmed.append(key)
        else:
            skipped.append(key)
            _dbg(FEATURE, "write_ignored", key=key, reason="server_kept_position")
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), skipped=len(skipped), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "skipped_keys": skipped, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    state = completed_state(adapter, feature=FEATURE)
    unresolved: list[dict[str, Any]] = []
    skipped: list[str] = []
    entries: list[tuple[str, list[str], Mapping[str, Any]]] = []
    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        target, kind = resolve_item(lib, item)
        files = all_files(lib, kind, target) if kind in _KINDS and target else []
        if not files:
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        open_files = [fid for fid in files if fid not in state]
        if completed_editions(lib, kind, target, state) or not open_files:
            skipped.append(key)
            continue
        entries.append((key, open_files, item))
    groups: dict[str, tuple[list[str], str | None]] = {key: (files, None) for key, files, _item in entries}
    done, failed = write_groups(adapter, groups, watched=False, action="remove") if groups else (set(), {})
    items_by_key = {key: item for key, _files, item in entries}
    confirmed = sorted(done)
    unresolved.extend(unresolved_entry(key, status, items_by_key.get(key)) for key, status in failed.items())
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), skipped=len(skipped), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "skipped_keys": skipped, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}

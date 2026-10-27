# /providers/sync/silo/_history.py
# Silo history sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from ._common import (
    SiloSyncError,
    as_int,
    chunks,
    iso_z,
    key_of,
    library,
    minimal_for,
    pages,
    problem_of,
    resolve_item,
    runtime_seconds,
    safe_json,
    silo_request,
    unresolved_entry,
    _dbg,
    _info,
    _warn,
)

FEATURE = "history"


def completed_rows(adapter: Any) -> dict[str, dict[str, Any]]:
    rows = pages(adapter, "/progress", feature=FEATURE, params={"status": "completed"}) or []
    return {str(r.get("media_item_id") or ""): dict(r) for r in rows if r.get("completed") and r.get("media_item_id")}


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter)
    rows = completed_rows(adapter)
    collected: dict[str, dict[str, Any]] = {}
    unmatched = 0
    for cid, row in rows.items():
        item = minimal_for(lib, cid)
        watched = iso_z(row.get("updated_at"))
        if not item or item.get("type") not in ("movie", "episode") or not watched:
            unmatched += 1
            continue
        item["watched"] = True
        item["watched_at"] = watched
        key = key_of(item)
        if key:
            collected[key] = item
    _info(FEATURE, "index_done", count=len(collected), rows=len(rows), unmatched=unmatched)
    return collected


def _duration_seconds(lib: Any, cid: str, item: Mapping[str, Any]) -> int:
    ms = as_int(item.get("duration_ms"))
    if ms and ms > 0:
        return int(ms / 1000)
    return runtime_seconds(lib, cid)


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter)
    unresolved: list[dict[str, Any]] = []
    dated: list[dict[str, Any]] = []
    undated: list[tuple[str, str]] = []
    keys_by_cid: dict[str, str] = {}

    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        cid, kind = resolve_item(adapter, lib, item, feature=FEATURE)
        if kind not in ("movie", "episode"):
            unresolved.append(unresolved_entry(key, "unsupported_type", item))
            continue
        if not cid:
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="not_in_library")
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        watched = iso_z(item.get("watched_at"))
        if not watched:
            if item.get("_cw_watched_state") is True or item.get("watched") is True:
                undated.append((cid, key))
                keys_by_cid[cid] = key
            else:
                unresolved.append(unresolved_entry(key, "missing_watched_at", item))
            continue
        duration = _duration_seconds(lib, cid, item)
        if duration <= 0:
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="missing_duration")
            unresolved.append(unresolved_entry(key, "missing_duration", item))
            continue
        dated.append({"client_ref": key[:120], "media_item_id": cid, "position_ms": duration * 1000, "duration_ms": duration * 1000, "updated_at": watched})
        keys_by_cid[cid] = key

    _dbg(FEATURE, "write_prepare", action="add", dated=len(dated), undated=len(undated), unresolved=len(unresolved))
    failed: set[str] = set()
    for batch in chunks(dated):
        resp = silo_request(adapter, "POST", "/sync/progress", json_body={"items": batch})
        if resp.status_code == 401:
            raise SiloSyncError("silo_history_unauthorized")
        if resp.status_code != 200:
            _warn(FEATURE, "write_failed", action="add", status=resp.status_code, problem=problem_of(resp) or None)
            failed.update(entry["media_item_id"] for entry in batch)
            continue
        data = safe_json(resp)
        results = data.get("items") if isinstance(data, Mapping) else None
        for res in results if isinstance(results, list) else []:
            if isinstance(res, Mapping) and res.get("status") != "success":
                failed.add(str(res.get("media_item_id") or ""))
    for cid, _key in undated:
        resp = silo_request(adapter, "POST", f"/watched/{quote(cid, safe='')}")
        if resp.status_code == 401:
            raise SiloSyncError("silo_history_unauthorized")
        if resp.status_code not in (200, 204):
            failed.add(cid)

    confirmed: list[str] = []
    if keys_by_cid:
        after = completed_rows(adapter)
        for cid, key in keys_by_cid.items():
            if cid in after and cid not in failed:
                confirmed.append(key)
            elif cid in failed:
                unresolved.append(unresolved_entry(key, "write_failed"))
            else:
                unresolved.append(unresolved_entry(key, "ignored_by_server"))
                _dbg(FEATURE, "write_ignored", key=key, reason="older_than_unwatch")
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    hidden: list[dict[str, str]] = []
    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        cid, kind = resolve_item(adapter, lib, item, feature=FEATURE)
        if kind not in ("movie", "episode") or not cid:
            unresolved.append(unresolved_entry(key, "unsupported_type" if kind not in ("movie", "episode") else "not_in_library", item))
            continue
        resp = silo_request(adapter, "DELETE", f"/watched/{quote(cid, safe='')}")
        if resp.status_code in (200, 204):
            confirmed.append(key)
            hidden.append({"content_id": cid, "scope": "item"})
            continue
        if resp.status_code == 401:
            raise SiloSyncError("silo_history_unauthorized")
        _warn(FEATURE, "write_failed", action="remove", status=resp.status_code, problem=problem_of(resp) or None)
        unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))
    for batch in chunks(hidden):
        resp = silo_request(adapter, "POST", "/history/remove", json_body={"targets": batch})
        if resp.status_code not in (200, 204):
            _dbg(FEATURE, "feed_hide_failed", status=resp.status_code)
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}

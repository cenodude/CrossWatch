# /providers/sync/silo/_progress.py
# Silo progress sync module
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

FEATURE = "progress"
POSITION_TOLERANCE_MS = 2000


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def in_progress_rows(adapter: Any) -> dict[str, dict[str, Any]]:
    rows = pages(adapter, "/progress", feature=FEATURE, params={"status": "in_progress"}) or []
    return {str(r.get("media_item_id") or ""): dict(r) for r in rows if r.get("media_item_id") and not r.get("completed")}


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    rows = in_progress_rows(adapter)
    collected: dict[str, dict[str, Any]] = {}
    unmatched = 0
    for cid, row in rows.items():
        item = minimal_for(lib, cid)
        pos = _float(row.get("position_seconds"))
        if not item or item.get("type") not in ("movie", "episode") or not pos or pos <= 0:
            unmatched += 1
            continue
        item["progress_ms"] = int(round(pos * 1000.0))
        dur = _float(row.get("duration_seconds"))
        if dur and dur > 0:
            item["duration_ms"] = int(round(dur * 1000.0))
            item["progress_percent"] = round(max(0.0, min(100.0, pos / dur * 100.0)), 3)
        updated = iso_z(row.get("updated_at"))
        if updated:
            item["progress_at"] = updated
        key = key_of(item)
        if key:
            collected[key] = item
    _info(FEATURE, "index_done", count=len(collected), rows=len(rows), unmatched=unmatched)
    return collected


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    unresolved: list[dict[str, Any]] = []
    entries: list[dict[str, Any]] = []
    wanted: dict[str, tuple[str, int]] = {}
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
        pos = as_int(item.get("progress_ms"))
        dur = as_int(item.get("duration_ms")) or runtime_seconds(lib, cid) * 1000
        if not pos or pos <= 0 or not dur or dur <= pos:
            unresolved.append(unresolved_entry(key, "missing_position_or_duration", item))
            continue
        entry: dict[str, Any] = {"client_ref": key[:120], "media_item_id": cid, "position_ms": pos, "duration_ms": dur}
        updated = iso_z(item.get("progress_at") or item.get("updated_at"))
        if updated:
            entry["updated_at"] = updated
        entries.append(entry)
        wanted[cid] = (key, pos)

    _dbg(FEATURE, "write_prepare", action="add", items=len(entries), unresolved=len(unresolved))
    failed: set[str] = set()
    for batch in chunks(entries):
        resp = silo_request(adapter, "POST", "/sync/progress", json_body={"items": batch})
        if resp.status_code == 401:
            raise SiloSyncError("silo_progress_unauthorized")
        if resp.status_code != 200:
            _warn(FEATURE, "write_failed", action="add", status=resp.status_code, problem=problem_of(resp) or None)
            failed.update(e["media_item_id"] for e in batch)
            continue
        data = safe_json(resp)
        results = data.get("items") if isinstance(data, Mapping) else None
        for res in results if isinstance(results, list) else []:
            if isinstance(res, Mapping) and res.get("status") != "success":
                failed.add(str(res.get("media_item_id") or ""))

    confirmed: list[str] = []
    skipped: list[str] = []
    if wanted:
        after = in_progress_rows(adapter)
        for cid, (key, pos) in wanted.items():
            row = after.get(cid)
            got = _float((row or {}).get("position_seconds"))
            if cid in failed:
                unresolved.append(unresolved_entry(key, "write_failed"))
            elif row is not None and got is not None and abs(got * 1000.0 - pos) <= POSITION_TOLERANCE_MS:
                confirmed.append(key)
            else:
                skipped.append(key)
                _dbg(FEATURE, "write_ignored", key=key, reason="server_newer")
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), skipped=len(skipped), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "skipped_keys": skipped, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        cid, kind = resolve_item(adapter, lib, item, feature=FEATURE)
        if kind not in ("movie", "episode") or not cid:
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        resp = silo_request(adapter, "DELETE", f"/progress/{quote(cid, safe='')}/position")
        if resp.status_code in (200, 204):
            confirmed.append(key)
            continue
        if resp.status_code == 401:
            raise SiloSyncError("silo_progress_unauthorized")
        _warn(FEATURE, "write_failed", action="remove", status=resp.status_code, problem=problem_of(resp) or None)
        unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}

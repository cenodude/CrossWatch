# CrossWatch - FlickList playback progress sync
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ._common import (
    URL_PLAYBACK,
    URL_PLAYBACK_ITEM,
    error_of,
    flicklist_request,
    key_of,
    ok_status,
    percent_of,
    read_minimal,
    safe_json,
    write_ident,
    _info,
    _warn,
)

FEATURE = "progress"


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, Mapping):
        for key in ("items", "data", "results", "playback"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = flicklist_request(adapter, "GET", URL_PLAYBACK)
    if not ok_status(resp):
        _warn(FEATURE, "index_failed", status=int(resp.status_code), error=error_of(resp))
        return {}
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    for row in _rows(safe_json(resp)):
        if not isinstance(row, Mapping):
            skipped += 1
            continue
        item = read_minimal(row, default_type=row.get("media_type") or "movie")
        if not item:
            skipped += 1
            continue
        pct = percent_of({"progress_percent": row.get("progress")})
        if pct is not None:
            item["progress_percent"] = pct
        updated = row.get("updated_at")
        if updated:
            item["progress_at"] = updated
            item["progress_at_source"] = "flicklist"
        rid = str(row.get("id") or "").strip()
        if rid:
            item["_flicklist_playback_id"] = rid
        key = key_of(item)
        if key:
            out[key] = item
        else:
            skipped += 1
    _info(FEATURE, "index_done", count=len(out), skipped=skipped)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True
    for raw in items or []:
        key = key_of(raw)
        payload = write_ident(raw)
        pct = percent_of(raw)
        if not key or payload is None or pct is None:
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_progress_or_supported_id"})
            continue
        payload["progress"] = pct
        if raw.get("paused") is not None:
            payload["paused"] = bool(raw.get("paused"))
        if dry_run:
            confirmed.append(key)
            continue
        resp = flicklist_request(adapter, "POST", URL_PLAYBACK, json=payload)
        if ok_status(resp):
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "write_failed", action="add", key=key, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_done", action="add", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    current = None if dry_run else build_index(adapter)
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True
    for raw in items or []:
        key = key_of(raw)
        playback_id = str(raw.get("_flicklist_playback_id") or "").strip()
        if not playback_id and current is not None and key:
            playback_id = str((current.get(key) or {}).get("_flicklist_playback_id") or "").strip()
        if not key:
            continue
        if dry_run:
            confirmed.append(key)
            continue
        if not playback_id:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_remote_playback_id"})
            continue
        resp = flicklist_request(adapter, "DELETE", URL_PLAYBACK_ITEM.format(id=playback_id))
        if ok_status(resp) or int(resp.status_code) == 404:
            confirmed.append(key)
            continue
        ok = False
        unresolved_keys.append(key)
        unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "write_failed", action="remove", key=key, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}

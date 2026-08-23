# CrossWatch - FlickList ratings sync
from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ._common import (
    URL_RATINGS,
    chunk,
    classify_write,
    error_of,
    flicklist_request,
    has_write_counters,
    half_point,
    key_of,
    not_future,
    ok_status,
    read_minimal,
    safe_json,
    write_ident,
    write_batch_size,
    _info,
    _warn,
)

FEATURE = "ratings"


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, Mapping):
        for key in ("items", "data", "results", "ratings"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    resp = flicklist_request(adapter, "GET", URL_RATINGS)
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
        rating = half_point(row.get("rating"))
        if rating is None:
            skipped += 1
            continue
        item["rating"] = rating
        rated_at = row.get("rated_at")
        if rated_at:
            item["rated_at"] = rated_at
        key = key_of(item)
        if key:
            out[key] = item
        else:
            skipped += 1
    _info(FEATURE, "index_done", count=len(out), skipped=skipped)
    return out


def _accepted(items: Iterable[Mapping[str, Any]], *, include_rating: bool) -> tuple[list[tuple[str, dict[str, Any]]], list[str], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        key = key_of(raw)
        payload = write_ident(raw)
        if not key or payload is None:
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id"})
            continue
        if include_rating:
            rating = half_point(raw.get("rating"))
            if rating is None:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "invalid_rating"})
                continue
            payload["rating"] = rating
            rated_at = not_future(str(raw.get("rated_at") or "").strip() or None)
            if rated_at:
                payload["rated_at"] = rated_at
        if key in seen:
            continue
        seen.add(key)
        accepted.append((key, payload))
    return accepted, unresolved_keys, unresolved


def _write(adapter: Any, method: str, items: Iterable[Mapping[str, Any]], *, include_rating: bool, dry_run: bool = False) -> dict[str, Any]:
    accepted, unresolved_keys, unresolved = _accepted(items, include_rating=include_rating)
    confirmed: list[str] = []
    ok = True
    if dry_run:
        return {"ok": True, "count": len(accepted), "confirmed_keys": [k for k, _ in accepted], "unresolved_keys": unresolved_keys, "unresolved": unresolved}
    batch_size = write_batch_size(adapter, FEATURE)
    for batch in chunk([{"key": k, "payload": p} for k, p in accepted], batch_size):
        sent = [(str(row["key"]), row["payload"]) for row in batch]
        resp = flicklist_request(adapter, method, URL_RATINGS, json={"items": [p for _k, p in sent]})
        if ok_status(resp):
            body = safe_json(resp)
            if not has_write_counters(body, method):
                ok = False
                for key, _payload in sent:
                    unresolved_keys.append(key)
                    unresolved.append({"key": key, "status": "invalid_response", "error": "missing FlickList write counters"})
                _warn(FEATURE, "write_invalid_response", method=method, status=int(resp.status_code), count=len(sent))
                continue
            yes, misses, miss_rows, counters = classify_write(sent, body, method=method)
            if misses:
                ok = False
                _warn(FEATURE, "write_partial", method=method, sent=len(sent), confirmed=len(yes), unresolved=len(misses), **counters)
            confirmed.extend(yes)
            unresolved_keys.extend(misses)
            unresolved.extend(miss_rows)
            continue
        ok = False
        for key, _payload in sent:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "write_failed", method=method, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_done", action=method.lower(), sent=len(accepted), confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, "POST", items, include_rating=True, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, "DELETE", items, include_rating=False, dry_run=dry_run)

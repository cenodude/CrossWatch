# /providers/sync/silo/_ratings.py
# Silo ratings sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from ._common import (
    SiloSyncError,
    as_int,
    attach_paths,
    iso_z,
    key_of,
    library,
    minimal_for,
    pages,
    problem_of,
    resolve_item,
    silo_request,
    unresolved_entry,
    _dbg,
    _info,
    _warn,
)

FEATURE = "ratings"


def silo_rating(rating: Any) -> int | None:
    try:
        value = float(rating)
    except Exception:
        return None
    if not value > 0:
        return None
    return max(1, min(10, int(value + 0.5)))


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    rows = pages(adapter, "/ratings", feature=FEATURE) or []
    collected: dict[str, dict[str, Any]] = {}
    by_cid: list[tuple[str, dict[str, Any]]] = []
    unmatched = 0
    for row in rows:
        cid = str(row.get("item_id") or "")
        value = as_int(row.get("rating"))
        item = minimal_for(lib, cid)
        if not item or not value or not 1 <= value <= 10 or item.get("type") == "episode":
            unmatched += 1
            continue
        item["rating"] = value
        rated = iso_z(row.get("rated_at"))
        if rated:
            item["rated_at"] = rated
        key = key_of(item)
        if key:
            collected[key] = item
            by_cid.append((cid, item))
    if getattr(adapter, "path_matching", False):
        attach_paths(adapter, lib, by_cid, feature=FEATURE)
    _info(FEATURE, "index_done", count=len(collected), rows=len(rows), unmatched=unmatched)
    return collected


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear: bool) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []

    for item in items or []:
        key = key_of(item)
        if not key:
            continue
        cid, kind = resolve_item(adapter, lib, item, feature=FEATURE)
        if kind not in ("movie", "show"):
            unresolved.append(unresolved_entry(key, "unsupported_type", item))
            continue
        if not cid:
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="not_in_library")
            unresolved.append(unresolved_entry(key, "not_in_library", item))
            continue
        if clear:
            resp = silo_request(adapter, "DELETE", f"/ratings/{quote(cid, safe='')}")
        else:
            value = silo_rating(item.get("rating"))
            if value is None:
                unresolved.append(unresolved_entry(key, "invalid_rating", item))
                continue
            resp = silo_request(adapter, "PUT", f"/ratings/{quote(cid, safe='')}", json_body={"rating": value})
        if resp.status_code in (200, 204):
            confirmed.append(key)
            continue
        if resp.status_code == 401:
            raise SiloSyncError("silo_ratings_unauthorized")
        _warn(FEATURE, "write_failed", action="remove" if clear else "add", status=resp.status_code, problem=problem_of(resp) or None)
        unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))

    _info(FEATURE, "write_done", action="remove" if clear else "add", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, clear=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, clear=True)

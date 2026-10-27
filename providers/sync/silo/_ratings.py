# /providers/sync/silo/_ratings.py
# Silo ratings sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from ._common import (
    SiloSyncError,
    account_scope,
    as_int,
    iso_z,
    key_of,
    library,
    minimal_for,
    pages,
    problem_of,
    read_state,
    resolve_item,
    silo_request,
    unresolved_entry,
    write_state,
    _dbg,
    _info,
    _warn,
)

FEATURE = "ratings"
STATE_NAME = "ratings_written"


def stars_for(rating: Any) -> int | None:
    try:
        value = float(rating)
    except Exception:
        return None
    if value <= 0:
        return None
    return max(1, min(5, int(value / 2.0 + 0.5)))


def rating_for(stars: int, written: Any) -> int | float:
    try:
        prior = float(written)
    except Exception:
        prior = 0.0
    if prior and stars_for(prior) == stars:
        return int(prior) if prior.is_integer() else prior
    return stars * 2


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    rows = pages(adapter, "/ratings", feature=FEATURE) or []
    written = read_state(STATE_NAME, account_scope(adapter))
    collected: dict[str, dict[str, Any]] = {}
    unmatched = 0
    for row in rows:
        cid = str(row.get("item_id") or "")
        stars = as_int(row.get("rating"))
        item = minimal_for(lib, cid)
        if not item or not stars or item.get("type") == "episode":
            unmatched += 1
            continue
        item["rating"] = rating_for(stars, written.get(cid))
        rated = iso_z(row.get("rated_at"))
        if rated:
            item["rated_at"] = rated
        key = key_of(item)
        if key:
            collected[key] = item
    _info(FEATURE, "index_done", count=len(collected), rows=len(rows), unmatched=unmatched)
    return collected


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, clear: bool) -> dict[str, Any]:
    lib = library(adapter, feature=FEATURE)
    scope = account_scope(adapter)
    written = read_state(STATE_NAME, scope)
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
            stars = stars_for(item.get("rating"))
            if stars is None:
                unresolved.append(unresolved_entry(key, "invalid_rating", item))
                continue
            resp = silo_request(adapter, "PUT", f"/ratings/{quote(cid, safe='')}", json_body={"rating": stars})
        if resp.status_code in (200, 204):
            confirmed.append(key)
            if clear:
                written.pop(cid, None)
            else:
                written[cid] = item.get("rating")
            continue
        if resp.status_code == 401:
            raise SiloSyncError("silo_ratings_unauthorized")
        _warn(FEATURE, "write_failed", action="remove" if clear else "add", status=resp.status_code, problem=problem_of(resp) or None)
        unresolved.append(unresolved_entry(key, f"http_{resp.status_code}", item))

    write_state(STATE_NAME, scope, written)
    _info(FEATURE, "write_done", action="remove" if clear else "add", confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "confirmed_keys": confirmed, "unresolved_keys": [u["key"] for u in unresolved], "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, clear=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, clear=True)

# providers/sync/stremio/_watchlist.py
# CrossWatch Stremio watchlist functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import copy
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import ids_from, merge_ids, minimal as id_minimal
from providers.sync._mod_common import build_op_result, unresolved_keys

from ._common import (
    canonical_item_key,
    datastore_get,
    datastore_put,
    default_record,
    imdb_id,
    imdb_ids_from_item,
    item_from_movie_record,
    library_records,
    now_iso,
    poster_url_from_item,
    record_id,
    stremio_id_for_item,
    tmdb_metadata_provider,
)


def _image_url(detail: Mapping[str, Any], kind: str) -> str:
    images = detail.get("images")
    image_map = images if isinstance(images, Mapping) else {}
    rows = image_map.get(kind)
    if not isinstance(rows, list):
        return ""
    for row in rows:
        if isinstance(row, Mapping):
            url = str(row.get("url") or "").strip()
            if url:
                return url
    return ""


def _item_from_series_record(record: Mapping[str, Any]) -> dict[str, Any] | None:
    rid = imdb_id(record_id(record))
    if not rid:
        return None
    item: dict[str, Any] = {"type": "show", "ids": {"imdb": rid}, "_stremio_id": rid}
    title = str(record.get("name") or "").strip()
    if title:
        item["title"] = title
    poster = str(record.get("poster") or "").strip()
    if poster:
        item["poster"] = poster
    return item


def _is_listed(record: Mapping[str, Any]) -> bool:
    return record.get("removed") is False and record.get("temp") is False


def _metadata_enriched(adapter: Any, item: Mapping[str, Any], typ: str) -> dict[str, Any]:
    out = dict(item)
    stremio_id = stremio_id_for_item(out)
    if stremio_id:
        if not str(out.get("poster") or out.get("poster_url") or "").strip():
            out["poster"] = poster_url_from_item(out, stremio_id)
        return out
    provider = tmdb_metadata_provider(adapter)
    if provider is None:
        return out
    ids = merge_ids(ids_from(out), imdb_ids_from_item(out))
    lookup = {k: str(ids.get(k) or "").strip() for k in ("tmdb", "imdb", "tvdb") if str(ids.get(k) or "").strip()}
    title = str(out.get("series_title") or out.get("show_title") or out.get("title") or "").strip()
    if title:
        lookup["title"] = title
    if out.get("year"):
        lookup["year"] = str(out.get("year"))
    if not lookup:
        return out
    try:
        detail = provider.fetch(entity="tv" if typ in {"show", "series", "tv"} else "movie", ids=lookup, need={"poster": True, "backdrop": False, "ids": True})
    except Exception:
        return out
    if not isinstance(detail, Mapping):
        return out
    detail_ids = detail.get("ids")
    ids_map: Mapping[str, Any] = detail_ids if isinstance(detail_ids, Mapping) else {}
    found = imdb_id(ids_map.get("imdb"))
    if found:
        out_ids_raw = out.get("ids")
        out_ids: Mapping[str, Any] = out_ids_raw if isinstance(out_ids_raw, Mapping) else {}
        merged = dict(out_ids)
        merged["imdb"] = found
        out["ids"] = merged
    if not str(out.get("title") or out.get("series_title") or "").strip() and str(detail.get("title") or "").strip():
        out["title"] = str(detail.get("title") or "").strip()
    if not str(out.get("poster") or out.get("poster_url") or "").strip():
        poster = poster_url_from_item(out, found) or _image_url(detail, "poster")
        if poster:
            out["poster"] = poster
    return out


def build_index(adapter: Any, **kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for record in library_records(adapter, incremental=bool(kwargs.get("incremental"))):
        if not _is_listed(record):
            continue
        typ = str(record.get("type") or "").strip().lower()
        item = item_from_movie_record(record) if typ == "movie" else _item_from_series_record(record) if typ in {"series", "show"} else None
        if item:
            out[canonical_item_key(item)] = item
    return out


def _unresolved(item: Mapping[str, Any], reason: str) -> dict[str, Any]:
    return {"status": "unresolved", "reason": reason, "item": id_minimal(item)}


def _apply_membership(record: dict[str, Any], item: Mapping[str, Any], listed: bool) -> None:
    poster = poster_url_from_item(item, record.get("_id"))
    if poster and not str(record.get("poster") or "").strip():
        record["poster"] = poster
    record["removed"] = not listed
    record["temp"] = False
    record["_mtime"] = now_iso()


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, True, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, items, False, dry_run=dry_run)


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], listed: bool, *, dry_run: bool = False) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    attempted = 0
    for raw in [dict(x or {}) for x in items or [] if isinstance(x, Mapping)]:
        typ = str(id_minimal(raw).get("type") or raw.get("type") or "").strip().lower()
        if typ in {"episode", "episodes", "season", "seasons"}:
            entry = _unresolved(raw, "stremio_watchlist_type_unsupported")
            unresolved.append(entry)
            results.append(entry)
            continue
        item = _metadata_enriched(adapter, raw, typ)
        key = canonical_item_key(item)
        stremio_id = stremio_id_for_item(item)
        if not stremio_id or typ not in {"movie", "show", "series", "tv"}:
            entry = _unresolved(item, "stremio_id_missing")
            unresolved.append(entry)
            results.append(entry)
            continue
        attempted += 1
        if dry_run:
            confirmed.append(key)
            results.append({"status": "dry_run", "item": id_minimal(item), "canonical_key": key})
            continue
        try:
            rows = datastore_get(adapter, ids=[stremio_id], all_records=False)
            if not rows and not listed:
                results.append({"status": "skipped", "reason": "already_absent", "item": id_minimal(item), "canonical_key": key})
                continue
            record = copy.deepcopy(dict(rows[0])) if rows else default_record(stremio_id, "movie" if typ == "movie" else "series", item)
            if not isinstance(record.get("state"), dict):
                record["state"] = {}
            _apply_membership(record, item, listed)
            datastore_put(adapter, [record])
        except Exception:
            entry = {"status": "failed", "reason": "stremio_watchlist_write_failed", "item": id_minimal(item), "canonical_key": key}
            unresolved.append(entry)
            results.append(entry)
            continue
        confirmed.append(key)
        results.append({"status": "applied", "item": id_minimal(item), "canonical_key": key})
    return build_op_result(ok=not unresolved, count=len(confirmed), confirmed_keys=confirmed, unresolved_keys=unresolved_keys(unresolved, canonical_item_key), unresolved=unresolved, results=results, attempted=attempted)

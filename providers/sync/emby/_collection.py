# /providers/sync/emby/_collection.py
# EMBY Module for collection inventory synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key

from ._common import (
    _ids_from_provider_ids,
    chunked,
    emby_get_library_roots,
    emby_item_library_ids,
    emby_resolve_library_id,
    emby_selected_library_ids,
    make_logger,
    normalize as emby_normalize,
)

_dbg, _info, _warn, _error = make_logger("collection")

_ITEM_FIELDS = (
    "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId,"
    "SeriesName,ParentId,CollectionFolderId,AncestorIds,LibraryId,Name,DateCreated"
)


def _progress(adapter: Any) -> Any:
    factory = getattr(adapter, "progress_factory", None)
    if callable(factory):
        try:
            return factory("collection")
        except Exception:
            pass

    class _Noop:
        def tick(self, *args: Any, **kwargs: Any) -> None:
            pass

        def done(self, *args: Any, **kwargs: Any) -> None:
            pass

    return _Noop()


def _collected_at_from_raw(raw: Mapping[str, Any]) -> str | None:
    value = raw.get("DateCreated") or raw.get("date_created") or raw.get("added_at")
    text = str(value or "").strip()
    return text or None


def _series_ids_by_item_id(
    http: Any,
    uid: str,
    rows: Iterable[tuple[Mapping[str, Any], str | None]],
) -> dict[str, dict[str, str]]:
    series_ids = sorted(
        {
            str(raw.get("SeriesId") or "").strip()
            for raw, _source_library_id in rows
            if str(raw.get("SeriesId") or "").strip()
        }
    )
    out: dict[str, dict[str, str]] = {}
    for batch in chunked(series_ids, 100):
        try:
            response = http.get(
                f"/Users/{uid}/Items",
                params={
                    "Ids": ",".join(batch),
                    "Fields": "ProviderIds,ProductionYear,Type,Name",
                },
            )
            if getattr(response, "status_code", 0) != 200:
                continue
            body = response.json() or {}
            for row in body.get("Items") or []:
                if not isinstance(row, Mapping):
                    continue
                series_id = str(row.get("Id") or "").strip()
                provider_ids = row.get("ProviderIds")
                if series_id and isinstance(provider_ids, Mapping):
                    out[series_id] = _ids_from_provider_ids(provider_ids)
        except Exception as exc:
            _warn("series_metadata_query_failed", series_ids=batch, error=str(exc))
    return out


def _fetch_rows(adapter: Any) -> tuple[list[tuple[Mapping[str, Any], str | None]], set[str]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return [], set()

    allowed = emby_selected_library_ids(adapter.cfg, "collection")
    if not allowed:
        _dbg("library_scope_not_configured")
    parents: list[str | None] = list(sorted(allowed)) or [None]
    rows: list[tuple[Mapping[str, Any], str | None]] = []
    seen_item_ids: set[str] = set()
    page_size = 500
    base_params: dict[str, Any] = {
        "Recursive": True,
        "IncludeItemTypes": "Movie,Series,Season,Episode",
        "Fields": _ITEM_FIELDS,
        "EnableUserData": False,
    }
    for parent_id in parents:
        start = 0
        seen_pages: set[tuple[str, ...]] = set()
        while True:
            params = dict(base_params)
            params.update({"StartIndex": start, "Limit": page_size, "EnableTotalRecordCount": True})
            if parent_id:
                params["ParentId"] = parent_id
            try:
                response = http.get(f"/Users/{uid}/Items", params=params)
                if getattr(response, "status_code", 0) != 200:
                    _warn("library_scope_query_failed", source_library_id=parent_id, allowed_library_ids=sorted(allowed), status=getattr(response, "status_code", None))
                    break
                body = response.json() or {}
                page = body.get("Items") or []
                if not isinstance(page, list):
                    break
            except Exception as exc:
                _warn("library_scope_query_failed", source_library_id=parent_id, allowed_library_ids=sorted(allowed), error=str(exc))
                break
            signature = tuple(str(raw.get("Id") or "") for raw in page if isinstance(raw, Mapping))
            if page and signature in seen_pages:
                _warn("pagination_repeated_page", source_library_id=parent_id, start_index=start)
                break
            seen_pages.add(signature)
            for raw in page:
                if not isinstance(raw, Mapping):
                    continue
                item_id = str(raw.get("Id") or "").strip()
                if item_id and item_id in seen_item_ids:
                    _dbg("duplicate_collection_item", provider_item_id=item_id, source_library_id=parent_id, item_title=str(raw.get("Name") or ""), media_type=str(raw.get("Type") or ""))
                    continue
                if item_id:
                    seen_item_ids.add(item_id)
                rows.append((raw, parent_id))
            start += len(page)
            total = int(body.get("TotalRecordCount") or 0)
            if not page or (total and start >= total) or len(page) < page_size:
                break
    return rows, allowed


def build_index(adapter: Any, **_kwargs: Any) -> Mapping[str, dict[str, Any]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return {}

    prog = _progress(adapter)
    rows, allowed = _fetch_rows(adapter)
    roots = emby_get_library_roots(adapter)
    scope_libs = sorted(allowed)
    series_ids_by_item_id = _series_ids_by_item_id(http, str(uid), rows)
    out: dict[str, dict[str, Any]] = {}
    total = max(1, len(rows))
    dup_keys = 0

    for idx, (raw, source_library_id) in enumerate(rows, start=1):
        raw_type = str(raw.get("Type") or "").strip()
        if raw_type not in {"Movie", "Series", "Season", "Episode"}:
            _warn("unsupported_collection_row_type", provider_item_id=str(raw.get("Id") or ""), media_type=raw_type)
            continue
        if allowed:
            memberships = emby_item_library_ids(raw)
            if memberships and not (memberships & allowed):
                _dbg("outside_library_scope", provider_item_id=str(raw.get("Id") or ""), source_library_id=source_library_id, allowed_library_ids=sorted(allowed), item_title=str(raw.get("Name") or ""), media_type=raw_type, provider_ids=dict(raw.get("ProviderIds") or {}))
                continue
        try:
            item = emby_normalize(raw)
            if raw_type == "Series":
                item["type"] = "show"
            elif raw_type == "Season":
                item["type"] = "season"
            elif raw_type == "Episode":
                item["type"] = "episode"
            elif raw_type == "Movie":
                item["type"] = "movie"
            collected_at = _collected_at_from_raw(raw)
            if collected_at:
                item["collected_at"] = collected_at
            lib_id = emby_resolve_library_id(
                raw,
                roots,
                scope_libs,
                http,
                str(uid),
                allow_deep_lookup=False,
            ) or source_library_id
            if lib_id:
                item["library_id"] = lib_id
            series_id = str(raw.get("SeriesId") or "").strip()
            show_ids = series_ids_by_item_id.get(series_id)
            if raw_type in {"Season", "Episode"} and show_ids:
                item["show_ids"] = show_ids
            if raw_type == "Season" and item.get("season") is None:
                season_no = raw.get("IndexNumber")
                try:
                    if season_no is not None:
                        item["season"] = int(season_no)
                except Exception:
                    pass
            key = canonical_key(item)
            if not key:
                _warn("collection_item_without_key", provider_item_id=str(raw.get("Id") or ""), item_title=str(raw.get("Name") or ""), media_type=raw_type, provider_ids=dict(raw.get("ProviderIds") or {}))
                continue
            if key in out:
                dup_keys += 1
                _dbg("duplicate_collection_key", key=key, provider_item_id=str(raw.get("Id") or ""), item_title=str(raw.get("Name") or ""), media_type=raw_type)
            out[key] = item
        finally:
            prog.tick(idx, total=total)

    try:
        prog.done(total=len(out))
    except Exception:
        pass
    _info("collection_index_built", rows=len(rows), indexed=len(out), duplicate_keys=dup_keys, library_ids=sorted(allowed), resolved_roots=len(roots))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(item), "hint": "emby_collection_write_unsupported"} for item in items or []]
    return 0, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(item), "hint": "emby_collection_write_unsupported"} for item in items or []]
    return 0, unresolved

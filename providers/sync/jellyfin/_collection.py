# /providers/sync/jellyfin/_collection.py
# JELLYFIN Module for collection inventory synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.collection_media import stream_media_info
from cw_platform.id_map import canonical_key

from ._common import (
    _ids_from_provider_ids,
    chunked,
    item_rows,
    fetch_item_pages,
    jf_get_library_roots,
    jf_item_library_ids,
    jf_resolve_library_id,
    jf_selected_library_ids,
    make_logger,
    normalize as jelly_normalize,
)
from ._routes import items as items_route, user_params

_dbg, _info, _warn = make_logger("collection")

_ITEM_FIELDS = (
    "ProviderIds,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId,"
    "SeriesName,ParentId,CollectionFolderId,AncestorIds,LibraryId,Name,DateCreated"
)
_MEDIA_INFO_FIELDS = ",Width,Height,Video3DFormat,MediaStreams"


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


def _media_info_enabled(adapter: Any) -> bool:
    return bool(getattr(getattr(adapter, "cfg", None), "collection_media_info", False))


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
                items_route(),
                params=user_params(uid, {
                    "Ids": ",".join(batch),
                    "Fields": "ProviderIds,ProductionYear,Type,Name",
                }),
            )
        except Exception as exc:
            _warn("series_metadata_query_failed", series_ids=batch, error=str(exc))
            raise RuntimeError(f"jellyfin_collection_series_lookup_read_failed:{exc}") from exc
        for row in item_rows(response, "jellyfin_collection_series_lookup"):
            series_id = str(row.get("Id") or "").strip()
            provider_ids = row.get("ProviderIds")
            if series_id and isinstance(provider_ids, Mapping):
                out[series_id] = _ids_from_provider_ids(provider_ids)
    return out


def _fetch_rows(adapter: Any) -> tuple[list[tuple[Mapping[str, Any], str | None]], set[str]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        raise RuntimeError("jellyfin_collection_not_configured")

    allowed = jf_selected_library_ids(adapter.cfg, "collection")
    if not allowed:
        _dbg("library_scope_not_configured")
    parents: list[str | None] = list(sorted(allowed)) or [None]
    rows: list[tuple[Mapping[str, Any], str | None]] = []
    seen_item_ids: set[str] = set()
    page_size = 500
    base_params: dict[str, Any] = {
        "Recursive": True,
        "IncludeItemTypes": "Movie,Series,Season,Episode",
        "CollapseBoxSetItems": False,
        "Fields": _ITEM_FIELDS + (_MEDIA_INFO_FIELDS if _media_info_enabled(adapter) else ""),
        "EnableUserData": False,
        "EnableTotalRecordCount": True,
    }
    for parent_id in parents:
        params = dict(base_params)
        if parent_id:
            params["ParentId"] = parent_id
        page_rows = fetch_item_pages(
            http,
            items_route(),
            user_params(str(uid), params),
            reason="jellyfin_collection",
            page_size=page_size,
            warn=_warn,
            source_library_id=parent_id,
            allowed_library_ids=sorted(allowed),
        )
        for raw in page_rows:
            item_id = str(raw.get("Id") or "").strip()
            if item_id in seen_item_ids:
                _dbg("duplicate_collection_item", provider_item_id=item_id, source_library_id=parent_id, item_title=str(raw.get("Name") or ""), media_type=str(raw.get("Type") or ""))
                continue
            seen_item_ids.add(item_id)
            rows.append((raw, parent_id))
    return rows, allowed


def build_index(adapter: Any, **_kwargs: Any) -> Mapping[str, dict[str, Any]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        raise RuntimeError("jellyfin_collection_not_configured")

    prog = _progress(adapter)
    rows, allowed = _fetch_rows(adapter)
    include_media = _media_info_enabled(adapter)
    roots = jf_get_library_roots(adapter)
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
            memberships = jf_item_library_ids(raw)
            if memberships and not (memberships & allowed):
                _dbg("outside_library_scope", provider_item_id=str(raw.get("Id") or ""), source_library_id=source_library_id, allowed_library_ids=sorted(allowed), item_title=str(raw.get("Name") or ""), media_type=raw_type, provider_ids=dict(raw.get("ProviderIds") or {}))
                continue
        try:
            item = jelly_normalize(raw)
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
            if include_media and raw_type in {"Movie", "Episode"}:
                item["format"] = "digital"
                media_info = stream_media_info(raw)
                if media_info:
                    item["media_info"] = media_info
            lib_id = source_library_id or jf_resolve_library_id(
                raw,
                roots,
                scope_libs,
                http,
                allow_deep_lookup=False,
            )
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
    _info("collection_index_built", rows=len(rows), indexed=len(out), duplicate_keys=dup_keys, library_ids=sorted(allowed), resolved_roots=len(roots), media_info=include_media)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(item), "hint": "jellyfin_collection_write_unsupported"} for item in items or []]
    return 0, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    unresolved = [{"item": dict(item), "hint": "jellyfin_collection_write_unsupported"} for item in items or []]
    return 0, unresolved

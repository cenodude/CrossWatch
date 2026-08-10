# /providers/sync/punchplay/_watchlist.py
# PunchPlay watchlist sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    LIST_PAGE_MAX,
    URL_LIST_DETAILS,
    URL_LIST_ITEMS,
    URL_LISTS,
    bulk_write,
    cfg_section,
    client_item_id,
    error_of,
    has_write_id,
    ids_for_punchplay,
    needs_client_item_id,
    punchplay_request,
    request_id_of,
    safe_json,
    snapshot_pages,
    _dbg,
    _info,
    _warn,
)

FEATURE = "watchlist"


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _pick(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        if name in row and row.get(name) is not None:
            return row.get(name)
    return None


def _nested(row: Mapping[str, Any], *names: str) -> Mapping[str, Any]:
    for name in names:
        value = row.get(name)
        if isinstance(value, Mapping):
            return value
    return {}


def _kind_of(item: Mapping[str, Any]) -> str:
    raw = str(item.get("type") or item.get("kind") or "").strip().lower()
    if raw in ("movie", "movies", "film"):
        return "movie"
    return "show"


def _row_type(row: Mapping[str, Any]) -> str:
    title = _nested(row, "title", "item")
    raw = str(_pick(row, "type", "kind", "mediaType", "media_type") or _pick(title, "type", "kind", "mediaType", "media_type") or "").strip().lower()
    if raw in ("movie", "movies", "film"):
        return "movie"
    if raw in ("show", "tv", "series", "anime"):
        return "show"
    return "show"


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any]:
    ids: dict[str, Any] = {}
    title = _nested(row, "title", "item")
    tmdb = _pick(row, "tmdbId", "tmdb_id", "resolved_tmdb_id", "titleTmdbId", "title_tmdb_id") or _pick(title, "tmdbId", "tmdb_id")
    try:
        tmdb_i = int(tmdb) if tmdb is not None else None
    except Exception:
        tmdb_i = None
    if tmdb_i and tmdb_i > 0:
        ids["tmdb"] = str(tmdb_i)

    out: dict[str, Any] = {"type": _row_type(row), "ids": ids}
    title_text = _pick(row, "title", "name")
    if isinstance(title_text, Mapping):
        title_text = _pick(title_text, "title", "name")
    if not title_text:
        title_text = _pick(title, "title", "name")
    title_s = str(title_text or "").strip()
    if title_s:
        out["title"] = title_s
    year = _pick(row, "year") or _pick(title, "year")
    try:
        if year is not None:
            out["year"] = int(year)
            return out
    except Exception:
        pass
    release = str(_pick(row, "releaseDate", "release_date") or _pick(title, "releaseDate", "release_date") or "").strip()
    if len(release) >= 4 and release[:4].isdigit():
        out["year"] = int(release[:4])
    return out


def _row_data(row: Mapping[str, Any]) -> Mapping[str, Any]:
    data = row.get("data")
    return data if isinstance(data, Mapping) else row


def _row_list_id(row: Mapping[str, Any]) -> str:
    data = _row_data(row)
    list_obj = _nested(data, "list")
    value = _pick(data, "listId", "list_id", "listID") or _pick(list_obj, "id")
    return str(value).strip() if value is not None else ""


def _row_is_watchlist(row: Mapping[str, Any]) -> bool:
    data = _row_data(row)
    list_obj = _nested(data, "list")
    return bool(_pick(data, "isWatchlist", "is_watchlist") or _pick(list_obj, "isWatchlist", "is_watchlist"))


def watchlist_ids(adapter: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    cursor: str | None = None
    guard = 0
    while True:
        guard += 1
        if guard > 200:
            _warn(FEATURE, "list_page_guard_tripped")
            break
        params: dict[str, Any] = {"limit": 100}
        if cursor:
            params["cursor"] = cursor
        resp = punchplay_request(adapter, "GET", URL_LISTS, params=params)
        if resp.status_code != 200:
            _warn(FEATURE, "lists_fetch_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            break
        data = safe_json(resp) or {}
        if not isinstance(data, Mapping):
            break
        rows = data.get("items")
        rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []
        for row in rows:
            if not row.get("isWatchlist"):
                continue
            if row.get("externalSource"):
                _info(FEATURE, "skip_external_watchlist", list_id=row.get("id"), external_source=row.get("externalSource"))
                continue
            out.append({"id": row.get("id"), "name": row.get("name"), "item_count": row.get("itemCount"), "dynamic": bool(row.get("isDynamicList"))})
        cursor = data.get("nextCursor") or None
        if not cursor:
            break
    return out


def _collect_rows(rows: Any, collected: dict[str, dict[str, Any]]) -> tuple[int, int]:
    row_list = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []
    added = 0
    for row in row_list:
        minimal = _to_minimal(row)
        if not minimal.get("ids"):
            continue
        key = _key_of(minimal)
        if key:
            collected[key] = minimal
            added += 1
    return len(row_list), added


def _index_list_detail(adapter: Any, list_id: Any, collected: dict[str, dict[str, Any]]) -> bool:
    resp = punchplay_request(adapter, "GET", URL_LIST_DETAILS.format(list_id=list_id))
    if resp.status_code != 200:
        _warn(FEATURE, "details_fetch_failed", list_id=list_id, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
        return False
    data = safe_json(resp) or {}
    if not isinstance(data, Mapping):
        return True
    _collect_rows(data.get("items"), collected)
    return True


def _index_dynamic_items(adapter: Any, list_id: Any, collected: dict[str, dict[str, Any]]) -> None:
    offset = 0
    guard = 0
    while True:
        guard += 1
        if guard > 2000:
            _warn(FEATURE, "items_page_guard_tripped", list_id=list_id)
            break
        resp = punchplay_request(
            adapter,
            "GET",
            URL_LIST_ITEMS.format(list_id=list_id),
            params={"offset": offset, "limit": LIST_PAGE_MAX},
        )
        if resp.status_code != 200:
            _warn(FEATURE, "items_fetch_failed", list_id=list_id, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            break
        data = safe_json(resp) or {}
        if not isinstance(data, Mapping):
            break
        rows = data.get("items")
        row_count, _added = _collect_rows(rows, collected)
        nxt = data.get("nextOffset")
        try:
            nxt_i = int(nxt) if nxt is not None else None
        except Exception:
            nxt_i = None
        if nxt_i is None or nxt_i <= offset or row_count <= 0:
            break
        offset = nxt_i


def _index_snapshot_items(adapter: Any, lists: Iterable[Mapping[str, Any]], collected: dict[str, dict[str, Any]]) -> None:
    list_ids = {str(entry.get("id")).strip() for entry in lists if entry.get("id") is not None}
    if not list_ids:
        return

    seen_rows = 0
    matched_rows = 0
    for page in snapshot_pages(adapter, "list_item", feature=FEATURE):
        for row in page:
            seen_rows += 1
            data = _row_data(row)
            if not isinstance(data, Mapping):
                continue
            list_id = _row_list_id(row)
            if list_id:
                if list_id not in list_ids:
                    continue
            elif not _row_is_watchlist(row):
                continue
            matched_rows += 1
            minimal = _to_minimal(data)
            if not minimal.get("ids"):
                continue
            key = _key_of(minimal)
            if key:
                collected[key] = minimal
    _dbg(FEATURE, "snapshot_scanned", rows=seen_rows, matched=matched_rows, indexed=len(collected))


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    section = cfg_section(adapter)
    if not section:
        section = {}

    lists = watchlist_ids(adapter)
    if not lists:
        _info(FEATURE, "index_done", count=0, reason="no_watchlist")
        return {}

    collected: dict[str, dict[str, Any]] = {}
    for entry in lists:
        list_id = entry.get("id")
        if list_id is None:
            continue
        if entry.get("dynamic"):
            _index_dynamic_items(adapter, list_id, collected)
        else:
            _index_list_detail(adapter, list_id, collected)
    _index_snapshot_items(adapter, lists, collected)

    _info(FEATURE, "index_done", count=len(collected), lists=len(lists))
    return collected


def _payload_for(item: Mapping[str, Any], *, remove: bool) -> tuple[dict[str, Any], str] | None:
    key = _key_of(item)
    if not key:
        return None
    payload = ids_for_punchplay(item)
    if not has_write_id(payload):
        return None

    payload["kind"] = _kind_of(item)
    title = str(item.get("title") or "").strip()
    if title:
        payload["title"] = title[:500]
    if item.get("is_anime") is True or str(item.get("type") or "").strip().lower() == "anime":
        payload["is_anime"] = True
    if remove:
        payload["remove"] = True
    if needs_client_item_id(payload):
        payload["client_item_id"] = client_item_id("cw", "watchlist", key)
    return payload, key


def _write(adapter: Any, items: Iterable[Mapping[str, Any]], *, remove: bool) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []

    for item in items or []:
        built = _payload_for(item, remove=remove)
        if built is None:
            key = _key_of(item)
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id"})
                _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue
        payload, key = built
        entries.append({"payload": payload, "key": key})

    if not entries:
        return {
            "ok": True,
            "confirmed_keys": [],
            "unresolved_keys": unresolved_keys,
            "deferred_keys": [],
            "unresolved": unresolved,
        }

    _dbg(FEATURE, "write_prepare", action="remove" if remove else "add", items=len(entries), skipped=len(unresolved_keys))
    res = bulk_write(adapter, "watchlist", entries, _key_of, feature=FEATURE)
    res["unresolved_keys"] = list(res.get("unresolved_keys") or []) + unresolved_keys
    res["unresolved"] = list(res.get("unresolved") or []) + unresolved
    _info(
        FEATURE,
        "write_done",
        action="remove" if remove else "add",
        confirmed=len(res.get("confirmed_keys") or []),
        deferred=len(res.get("deferred_keys") or []),
        unresolved=len(res.get("unresolved_keys") or []),
    )
    return res


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, remove=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, items, remove=True)

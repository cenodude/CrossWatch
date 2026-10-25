# /providers/sync/tmdb/_playlists.py
# TMDb list sync (v4 lists)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from cw_platform.id_map import minimal as id_minimal
from cw_platform.interactive_reads import retained_read
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PlaylistItem, PlaylistResource, PlaylistSnapshot

from .._log import log as cw_log

from ._common import (
    as_int,
    enrich_ids_dict,
    key_of,
    pick_media_type,
    resolve_tmdb_id,
    year_from_date,
)

FEATURE = "playlists"
MEDIA_TYPES = ("movie", "show")
BATCH_SIZE = 100
MAX_PAGES = 500


def _info(msg: str, **fields: Any) -> None:
    cw_log("TMDB", FEATURE, "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("TMDB", FEATURE, "warn", msg, **fields)


class TMDBPlaylistError(RuntimeError):
    pass


class TMDBPlaylistNotFound(TMDBPlaylistError):
    pass


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _ok(resp: Any) -> bool:
    return 200 <= int(getattr(resp, "status_code", 0) or 0) < 300


def _json(resp: Any) -> Any:
    try:
        return resp.json() if (getattr(resp, "content", b"") or b"") else {}
    except Exception:
        return {}


def _error_of(resp: Any) -> str:
    data = _json(resp)
    if isinstance(data, Mapping):
        msg = str(data.get("status_message") or data.get("message") or "").strip()
        if msg:
            return msg
    return f"http:{int(getattr(resp, 'status_code', 0) or 0)}"


def _resource(adapter: Any, row: Mapping[str, Any], fallback_id: str = "") -> PlaylistResource | None:
    raw_id = str(row.get("id") or fallback_id or "").strip()
    if not raw_id:
        return None
    return PlaylistResource(
        provider="TMDB",
        id=raw_id,
        name=str(row.get("name") or raw_id).strip(),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=False,
        media_types=MEDIA_TYPES,
        extra={
            "raw_id": raw_id,
            "description": str(row.get("description") or "").strip(),
            "item_count": as_int(row.get("number_of_items") if row.get("number_of_items") is not None else row.get("total_results")),
            "private": not bool(row.get("public")),
        },
    )


def _pages(adapter: Any, path: str) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    page = 1
    total = 1
    while page <= total:
        resp = adapter.client.v4("GET", path, params={"page": page})
        if int(resp.status_code) == 404:
            raise TMDBPlaylistNotFound(f"TMDb list not found ({path})")
        if not _ok(resp):
            raise TMDBPlaylistError(_error_of(resp))
        data = _json(resp)
        if not isinstance(data, Mapping) or not isinstance(data.get("results"), list):
            raise TMDBPlaylistError(f"TMDb list page invalid ({path})")
        out.append(data)
        total = as_int(data.get("total_pages")) or 1
        if total > MAX_PAGES:
            raise TMDBPlaylistError(f"TMDb list too large ({total} pages, limit {MAX_PAGES})")
        page += 1
    return out


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    object_id = adapter.client.account_object_id()
    resources: list[PlaylistResource] = []
    for data in _pages(adapter, f"/account/{object_id}/lists"):
        rows = data.get("results")
        for row in rows if isinstance(rows, list) else []:
            if isinstance(row, Mapping):
                res = _resource(adapter, row)
                if res is not None:
                    resources.append(res)
    _info("list_resources_done", count=len(resources))
    return resources


def _media_from_row(adapter: Any, row: Mapping[str, Any]) -> dict[str, Any] | None:
    tmdb_id = as_int(row.get("id"))
    if tmdb_id is None:
        return None
    kind = str(row.get("media_type") or "").strip().lower()
    if kind == "movie":
        item: dict[str, Any] = {
            "type": "movie",
            "title": row.get("title") or row.get("original_title"),
            "year": year_from_date(row.get("release_date")),
            "ids": {"tmdb": tmdb_id},
        }
        enrich_ids_dict(adapter, item["ids"], media_type="movie", tmdb_id=tmdb_id, feature=FEATURE)
    elif kind == "tv":
        item = {
            "type": "show",
            "title": row.get("name") or row.get("original_name"),
            "year": year_from_date(row.get("first_air_date")),
            "ids": {"tmdb": tmdb_id},
        }
        enrich_ids_dict(adapter, item["ids"], media_type="tv", tmdb_id=tmdb_id, feature=FEATURE)
    else:
        return None
    return id_minimal(item)


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise TMDBPlaylistNotFound("missing TMDb list id")
    pages = _pages(adapter, f"/list/{lid}")
    head = pages[0] if pages else {}
    resource = _resource(adapter, head, lid) or PlaylistResource(provider="TMDB", id=lid, name=lid, instance=_instance_id(adapter))
    items: list[PlaylistItem] = []
    for data in pages:
        rows = data.get("results")
        for row in rows if isinstance(rows, list) else []:
            if not isinstance(row, Mapping):
                continue
            media = _media_from_row(adapter, row)
            if not media:
                continue
            item = PlaylistItem.from_media(media, position=len(items), provider_media_id=f"{row.get('media_type')}:{row.get('id')}")
            if item.key:
                items.append(item)
    _info("snapshot_done", list_id=lid, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    if dry_run:
        return PlaylistResource(provider="TMDB", id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, media_types=MEDIA_TYPES)
    resp = adapter.client.v4("POST", "/list", json_body={"name": nm, "iso_639_1": "en", "public": False})
    data = _json(resp)
    if not _ok(resp) or not isinstance(data, Mapping) or not data.get("id"):
        raise TMDBPlaylistError(_error_of(resp) if not _ok(resp) else "TMDb create list returned no id")
    resource = _resource(adapter, {"id": data.get("id"), "name": nm, "number_of_items": 0, "public": False})
    if resource is None:
        raise TMDBPlaylistError("TMDb create list returned no id")
    if items:
        add(adapter, resource.id, items)
    _info("create_done", list_id=resource.id, name=nm)
    return resource


def _accepted(adapter: Any, items: Sequence[Mapping[str, Any]]) -> tuple[list[tuple[str, dict[str, Any]]], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        media = id_minimal(raw)
        key = key_of(media)
        kind = pick_media_type(media)
        if not key:
            unresolved.append({"item": media, "hint": "missing_key"})
            continue
        if kind not in ("movie", "tv"):
            unresolved.append({"item": media, "key": key, "hint": "unsupported_type"})
            continue
        tmdb_id = resolve_tmdb_id(adapter, media, want=kind)
        if tmdb_id is None:
            unresolved.append({"item": media, "key": key, "hint": "missing_tmdb_id"})
            continue
        token = f"{kind}:{tmdb_id}"
        if token in seen:
            continue
        seen.add(token)
        accepted.append((key, {"media_type": kind, "media_id": int(tmdb_id)}))
    return accepted, unresolved


def _result_ok(results: Any, payload: Mapping[str, Any]) -> tuple[bool | None, str]:
    if not isinstance(results, list):
        return None, ""
    for row in results:
        if not isinstance(row, Mapping):
            continue
        if str(row.get("media_type") or "") == payload["media_type"] and as_int(row.get("media_id")) == payload["media_id"]:
            err = row.get("error")
            if isinstance(err, list):
                err = "; ".join(str(e) for e in err)
            return bool(row.get("success")), str(err or "").strip()
    return None, ""


def _write_items(adapter: Any, method: str, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    lid = str(playlist_id or "").strip()
    if not lid:
        raise TMDBPlaylistNotFound("missing TMDb list id")
    accepted, unresolved = _accepted(adapter, items)
    confirmed: list[str] = []
    unresolved_keys = [str(u["key"]) for u in unresolved if u.get("key")]
    ok = True
    for start in range(0, len(accepted), BATCH_SIZE):
        batch = accepted[start:start + BATCH_SIZE]
        resp = adapter.client.v4(method, f"/list/{lid}/items", json_body={"items": [payload for _key, payload in batch]})
        if not _ok(resp):
            ok = False
            err = _error_of(resp)
            for key, _payload in batch:
                unresolved_keys.append(key)
                unresolved.append({"item": {"key": key}, "key": key, "hint": f"http:{int(resp.status_code)}", "error": err})
            _warn("write_items_failed", method=method, list_id=lid, status=int(resp.status_code), error=err)
            continue
        data = _json(resp)
        results = data.get("results") if isinstance(data, Mapping) else None
        whole_ok = bool(isinstance(data, Mapping) and data.get("success"))
        for key, payload in batch:
            hit, err = _result_ok(results, payload)
            if hit is True or (hit is None and whole_ok):
                confirmed.append(key)
                continue
            ok = False
            unresolved_keys.append(key)
            unresolved.append({"item": {"key": key}, "key": key, "hint": "rejected", "error": err})
    _info("write_items_done", method=method.lower(), list_id=lid, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write_items(adapter, "POST", playlist_id, items)


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write_items(adapter, "DELETE", playlist_id, items)


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}

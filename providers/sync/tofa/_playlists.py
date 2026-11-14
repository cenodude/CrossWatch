# /providers/sync/tofa/_playlists.py
# Tofa custom collections as playlist endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import replace
from typing import Any, Mapping, Sequence
from urllib.parse import quote

from cw_platform.id_map import canonical_key, minimal
from cw_platform.playlists import PlaylistItem, PlaylistReadOnlyError, PlaylistResource, PlaylistSnapshot, PlaylistUnavailableError
from cw_platform.run_control import raise_if_cancelled

from ._common import as_int, safe_json, tofa_request, _info

BASE = "/collections/custom"
PREFIX = "collection:"
MEDIA_TYPES = ("movie", "show")


class TofaPlaylistError(PlaylistUnavailableError):
    pass


def _path(playlist_id: Any) -> str:
    pid = str(playlist_id or "").strip()
    if not pid.startswith(PREFIX) or not pid[len(PREFIX):]:
        raise TofaPlaylistError("unknown tofa collection id")
    return f"{BASE}/{quote(pid[len(PREFIX):], safe='')}"


def _request(adapter: Any, method: str, path: str, **kwargs: Any) -> Any:
    raise_if_cancelled()
    resp = tofa_request(adapter, method, path, **kwargs)
    if resp.status_code not in (200, 201, 204):
        raise TofaPlaylistError(f"tofa_playlists_http_{resp.status_code}")
    return safe_json(resp)


def _resource(adapter: Any, row: Any) -> PlaylistResource:
    if not isinstance(row, Mapping) or not row.get("id") or not isinstance(row.get("can_edit"), bool):
        raise TofaPlaylistError("tofa_playlists_invalid_response")
    smart = row.get("smart_rules") is not None
    external = row.get("source") is not None
    writable = row["can_edit"] and not smart and not external
    return PlaylistResource(
        provider="TOFA", id=f"{PREFIX}{row['id']}", name=str(row.get("name") or row["id"]),
        instance=str(getattr(adapter, "instance_id", "") or "default"), kind="smart" if smart or external else "regular",
        can_read=True, can_add=writable, can_remove=writable, can_reorder=writable, media_types=MEDIA_TYPES,
        extra={"endpoint_type": "collection", "raw_id": str(row["id"]), "item_count": row.get("item_count"),
               "can_rename": writable, "can_delete": writable, "privacy": row.get("visibility"), "external": external},
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    data = _request(adapter, "GET", BASE, params={"include_hidden": "true", "rollups": "false"})
    if not isinstance(data, Mapping) or not isinstance(data.get("collections"), list):
        raise TofaPlaylistError("tofa_playlists_invalid_response")
    resources = [_resource(adapter, row) for row in data["collections"]]
    resources.sort(key=lambda r: (not r.writable, r.name.lower(), r.id))
    return resources


def _detail(adapter: Any, playlist_id: Any) -> tuple[PlaylistResource, Mapping[str, Any]]:
    data = _request(adapter, "GET", _path(playlist_id), params={"all_members": "true", "sort": "custom"})
    resource = _resource(adapter, data)
    if resource.id != str(playlist_id).strip():
        raise TofaPlaylistError("tofa_playlists_invalid_response")
    return resource, data


def _rows(data: Mapping[str, Any], resource: PlaylistResource) -> list[Mapping[str, Any]]:
    members = data.get("members")
    rows = data.get("items") if members is None and resource.is_smart else members
    if not isinstance(rows, list) or not all(isinstance(row, Mapping) for row in rows):
        raise TofaPlaylistError("tofa_playlists_invalid_members")
    if members is not None:
        ids = [str(row.get("member_id") or "") for row in rows]
        if not all(ids) or len(set(ids)) != len(ids) or any(as_int(row.get("position")) is None for row in rows):
            raise TofaPlaylistError("tofa_playlists_invalid_members")
        return sorted(rows, key=lambda r: int(r["position"]))
    return rows


def _media(row: Mapping[str, Any]) -> dict[str, Any] | None:
    kind = str(row.get("type") or row.get("media_type") or "").lower()
    kind = "show" if kind in ("tv", "series", "show") else kind
    tmdb = as_int(row.get("tmdb_id"))
    if kind not in MEDIA_TYPES or row.get("season_number") is not None or not tmdb or tmdb < 1:
        return None
    return {"type": kind, "ids": {"tmdb": str(tmdb)}, "title": str(row.get("title") or ""),
            "year": as_int(row.get("year")) or as_int(str(row.get("release_date") or "")[:4])}


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    resource, data = _detail(adapter, playlist_id)
    items = []
    for pos, row in enumerate(_rows(data, resource)):
        media = _media(row)
        if media:
            items.append(PlaylistItem.from_media(media, playlist_item_id=row.get("member_id"), position=pos,
                                                provider_media_id=row.get("local_media_id") or row.get("id")))
    return PlaylistSnapshot(resource=resource, items=items)


def _writable(resource: PlaylistResource) -> None:
    if not resource.writable:
        raise PlaylistReadOnlyError("The Tofa collection is read only")


def _name(name: Any) -> str:
    value = str(name or "").strip()
    if not value:
        raise ValueError("playlist name required")
    return value


def _privacy(privacy: Any) -> str:
    value = str(privacy or "private").strip().lower()
    if value not in ("private", "household"):
        raise ValueError("Tofa collection visibility must be private or household")
    return value


def create(adapter: Any, name: str, *, media_type: str | None = None, items: Sequence[Mapping[str, Any]] | None = None,
           privacy: str | None = None, dry_run: bool = False) -> PlaylistResource:
    payload = {"name": _name(name), "sort_mode": "custom", "visibility": _privacy(privacy)}
    if dry_run:
        return replace(_resource(adapter, {"id": "dry-run", "can_edit": True, **payload}), id="")
    resource = _resource(adapter, _request(adapter, "POST", BASE, json_body=payload))
    if items:
        result = add(adapter, resource.id, items)
        if not result["ok"] or result["unresolved"]:
            raise TofaPlaylistError(f"Tofa collection {resource.id} was created but some initial items could not be added")
    _info("playlists", "create_done", list_id=resource.id)
    return resource


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    name = _name(name)
    resource, _ = _detail(adapter, playlist_id)
    _writable(resource)
    if dry_run:
        return replace(resource, name=name)
    data = _request(adapter, "PATCH", _path(playlist_id), json_body={"name": name})
    changed = _detail(adapter, playlist_id)[0] if data is None else _resource(adapter, data)
    if changed.id != resource.id or changed.name != name:
        raise TofaPlaylistError("tofa_playlists_rename_not_confirmed")
    return changed


def set_privacy(adapter: Any, playlist_id: Any, privacy: str, *, dry_run: bool = False) -> PlaylistResource:
    privacy = _privacy(privacy)
    resource, _ = _detail(adapter, playlist_id)
    _writable(resource)
    if dry_run:
        return replace(resource, extra={**resource.extra, "privacy": privacy})
    data = _request(adapter, "PATCH", _path(playlist_id), json_body={"visibility": privacy})
    changed = _detail(adapter, playlist_id)[0] if data is None else _resource(adapter, data)
    if changed.id != resource.id or changed.extra.get("privacy") != privacy:
        raise TofaPlaylistError("tofa_playlists_privacy_not_confirmed")
    return changed


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    resource, _ = _detail(adapter, playlist_id)
    _writable(resource)
    if dry_run:
        return {"ok": True, "dry_run": True}
    _request(adapter, "DELETE", _path(playlist_id))
    return {"ok": True, "count": 1}


def _write(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]], *, removing: bool) -> dict[str, Any]:
    resource, data = _detail(adapter, playlist_id)
    _writable(resource)
    by_key: dict[str, list[str]] = {}
    for row in _rows(data, resource):
        media = _media(row)
        if media:
            by_key.setdefault(canonical_key(media), []).append(str(row["member_id"]))
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    ok = True
    for raw in items:
        key = canonical_key(raw)
        if key in seen:
            continue
        seen.add(key)
        kind = str(raw.get("type") or "").lower()
        raw_ids = raw.get("ids")
        ids = raw_ids if isinstance(raw_ids, Mapping) else {}
        tmdb = as_int(ids.get("tmdb"))
        hint = "unsupported_type" if kind not in MEDIA_TYPES else "missing_tmdb_id" if not tmdb or tmdb < 1 else ""
        if not hint:
            try:
                if removing:
                    for mid in by_key.get(key, []):
                        _request(adapter, "DELETE", f"{_path(playlist_id)}/members/{quote(mid, safe='')}")
                elif key not in by_key:
                    result = _request(adapter, "POST", f"{_path(playlist_id)}/members",
                                      json_body={"tmdb_id": tmdb, "media_type": "tv" if kind == "show" else "movie"})
                    if not isinstance(result, Mapping) or not result.get("member_id") or not isinstance(result.get("added"), bool):
                        raise TofaPlaylistError("tofa_playlists_write_not_verified")
                confirmed.append(key)
            except TofaPlaylistError as exc:
                ok = False
                hint = str(exc)
        if hint:
            unresolved.append({"item": minimal(raw), "key": key, "hint": hint})
    _info("playlists", "write_done", op="remove" if removing else "add", list_id=resource.id,
          confirmed=len(confirmed), unresolved=len(unresolved))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed,
            "unresolved_keys": [row["key"] for row in unresolved], "unresolved": unresolved}


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, items, removing=False)


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, items, removing=True)


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    resource, data = _detail(adapter, playlist_id)
    _writable(resource)
    rows = _rows(data, resource)
    members = [str(row["member_id"]) for row in rows]
    by_key: dict[str, list[str]] = {}
    for row in rows:
        media = _media(row)
        if media:
            by_key.setdefault(canonical_key(media), []).append(str(row["member_id"]))
    wanted = list(dict.fromkeys(mid for key in ordered_keys for mid in by_key.get(key, [])))
    selected = set(wanted)
    ordered = wanted + [mid for mid in members if mid not in selected]
    if ordered == members and data.get("sort_mode") == "custom":
        return {"ok": True, "count": 0, "reordered": 0}
    _request(adapter, "PUT", f"{_path(playlist_id)}/members/order", json_body={"member_ids": ordered})
    if data.get("sort_mode") != "custom":
        _request(adapter, "PATCH", _path(playlist_id), json_body={"sort_mode": "custom"})
    return {"ok": True, "count": len(ordered), "reordered": len(ordered)}

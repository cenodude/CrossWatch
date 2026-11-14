# /providers/sync/plex/_cloud_lists.py
# Plex account Lists with per-list privacy and ordered membership
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import replace
import re
from typing import Any, Mapping, Sequence
from urllib.parse import quote

import requests

from cw_platform.id_map import canonical_key, minimal
from cw_platform.playlists import PlaylistAuthError, PlaylistItem, PlaylistResource, PlaylistSnapshot, PlaylistUnavailableError
from cw_platform.run_control import raise_if_cancelled
from . import _common as common

COMMUNITY = "https://community.plex.tv/api"
PREFIX = "cloud:"
MEDIA_TYPES = ("movie", "show", "season", "episode")
PRIVACY = {"private": "PRIVATE", "friends": "FRIENDS", "friends_of_friends": "FRIENDS_OF_FRIENDS", "plex": "PLEX", "public": "ANYONE"}
FIELDS = "id name slug privacy ranked itemCount user { id username }"
LISTS = "query CrossWatchLists($id: ID!, $after: String) { user(id: $id) { customLists(first: 100, after: $after) { nodes { " + FIELDS + " } pageInfo { hasNextPage endCursor } } } }"
DETAIL = "query CrossWatchList($slug: String!, $username: String!, $after: String) { customListBySlug(slug: $slug, username: $username) { " + FIELDS + " metadataItems(first: 100, after: $after) { nodes { id type title year } pageInfo { hasNextPage endCursor } } } }"
CREATE = "mutation CrossWatchCreateList($input: CreateCustomListInput!) { createCustomList(input: $input) { " + FIELDS + " } }"
UPDATE = "mutation CrossWatchUpdateList($id: ID!, $input: UpdateCustomListInput!) { updateCustomList(id: $id, input: $input) { " + FIELDS + " } }"
DELETE = "mutation CrossWatchRemoveList($id: ID!) { removeCustomList(id: $id) }"
EDIT = "mutation CrossWatchEditList($id: ID!, $input: EditCustomListItemsInput!) { editCustomListItems(id: $id, input: $input) { id } }"


class PlexListError(PlaylistUnavailableError):
    pass


def available(adapter: Any) -> bool:
    client = getattr(adapter, "client", None)
    return bool(common.plex_cfg_get(adapter, "account_token") and not getattr(client, "_cloud_token_suppressed", False)
                and getattr(client, "_user_scope_kind", None) not in ("home", "shared"))


def _privacy(value: Any) -> str:
    name = str(value or "private").lower().strip()
    if name not in PRIVACY:
        raise ValueError("Unsupported Plex list privacy")
    return PRIVACY[name]


def _name(value: Any) -> str:
    name = str(value or "").strip()
    if not name:
        raise ValueError("playlist name required")
    return name


class Client:
    def __init__(self, adapter: Any):
        if not available(adapter):
            raise PlaylistAuthError("Plex cloud Lists require the selected user's Plex account token")
        self.adapter = adapter
        self.token = str(common.plex_cfg_get(adapter, "account_token"))
        self.session = adapter.client.session
        self.user: dict[str, Any] = {}
        self.cache: dict[str, Mapping[str, Any]] = {}

    def request(self, method: str, url: str, **kwargs: Any) -> Mapping[str, Any]:
        raise_if_cancelled()
        try:
            response = self.session.request(method, url, headers=common.plex_headers(self.token, accept="application/json"),
                                            timeout=30, verify=True, allow_redirects=False, **kwargs)
        except requests.RequestException:
            raise PlexListError("Plex cloud Lists could not be reached") from None
        if response.status_code in (401, 403):
            raise PlaylistAuthError("Plex cloud Lists authentication failed")
        if response.status_code == 429:
            raise PlexListError("Plex cloud Lists are rate limited; retry later")
        if response.status_code != 200:
            raise PlexListError(f"plex_lists_http_{response.status_code}")
        try:
            data = response.json()
        except ValueError:
            raise PlexListError("plex_lists_invalid_response") from None
        if not isinstance(data, Mapping):
            raise PlexListError("plex_lists_invalid_response")
        return data

    def gql(self, query: str, variables: Mapping[str, Any]) -> Mapping[str, Any]:
        data = self.request("POST", COMMUNITY, json={"query": query, "variables": dict(variables)})
        if data.get("errors") or not isinstance(data.get("data"), Mapping):
            raise PlexListError("Plex cloud Lists rejected the request")
        return data["data"]

    def account(self) -> Mapping[str, Any]:
        if not self.user:
            row = self.request("GET", "https://plex.tv/api/v2/user")
            client = self.adapter.client
            expected = getattr(client, "selected_username", None) or getattr(client, "user_username", None) or common.plex_cfg_get(self.adapter, "username")
            selected = getattr(client, "selected_account_id", None) or common.plex_cfg_get(self.adapter, "account_id")
            if row.get("restricted") or not row.get("uuid") or not row.get("username"):
                raise PlaylistAuthError("Plex cloud Lists are unavailable for this account")
            if (expected and str(expected).casefold() != str(row["username"]).casefold()) or (selected and str(selected) != "1" and not expected):
                raise PlaylistAuthError("Plex cloud Lists token does not match the selected user")
            self.user = dict(row)
        return self.user

    def resource(self, row: Any) -> PlaylistResource:
        if not isinstance(row, Mapping) or not row.get("id") or not row.get("slug") or not isinstance(row.get("user"), Mapping) or not row["user"].get("id") or not row["user"].get("username"):
            raise PlexListError("plex_lists_invalid_resource")
        owner = row["user"]
        own = str(owner.get("id") or "") == str(self.account()["uuid"])
        privacy = next((key for key, value in PRIVACY.items() if value == row.get("privacy")), "")
        if row.get("privacy") == "PUBLIC":
            privacy = "public"
        if not privacy:
            raise PlexListError("plex_lists_unknown_privacy")
        return PlaylistResource(
            provider="PLEX", id=PREFIX + str(row["id"]), name=str(row.get("name") or row["id"]),
            instance=str(getattr(self.adapter, "instance_id", "default") or "default"), can_read=True,
            can_add=own, can_remove=own, can_reorder=own, media_types=MEDIA_TYPES,
            extra={"endpoint_type": "cloud_list", "raw_id": str(row["id"]), "slug": str(row["slug"]),
                   "username": str(owner.get("username") or ""), "privacy": privacy, "ranked": bool(row.get("ranked")),
                   "can_rename": own, "can_delete": own, "item_count": row.get("itemCount")},
        )

    def page(self, data: Any, seen: set[str]) -> tuple[list[Mapping[str, Any]], str | None]:
        if not isinstance(data, Mapping) or not isinstance(data.get("nodes"), list) or not all(isinstance(r, Mapping) for r in data["nodes"]):
            raise PlexListError("plex_lists_invalid_page")
        info = data.get("pageInfo")
        if not isinstance(info, Mapping) or not isinstance(info.get("hasNextPage"), bool):
            raise PlexListError("plex_lists_invalid_page")
        cursor = str(info.get("endCursor") or "") if info["hasNextPage"] else None
        if info["hasNextPage"]:
            if not cursor or cursor in seen or not data["nodes"]:
                raise PlexListError("plex_lists_invalid_continuation")
            seen.add(cursor)
        return data["nodes"], cursor

    def resources(self) -> list[PlaylistResource]:
        out: list[PlaylistResource] = []
        after = None
        seen: set[str] = set()
        ids: set[str] = set()
        while True:
            data = self.gql(LISTS, {"id": self.account()["uuid"], "after": after})
            rows, after = self.page((data.get("user") or {}).get("customLists"), seen)
            for row in rows:
                resource = self.resource(row)
                if resource.id in ids:
                    raise PlexListError("plex_lists_repeated_resource")
                ids.add(resource.id)
                out.append(resource)
            if after is None:
                return out

    def find(self, playlist_id: Any) -> PlaylistResource:
        for resource in self.resources():
            if resource.id == str(playlist_id):
                return resource
        raise PlexListError("Plex cloud list is missing or no longer accessible")

    def members(self, resource: PlaylistResource) -> list[Mapping[str, Any]]:
        out: list[Mapping[str, Any]] = []
        after = None
        seen: set[str] = set()
        ids: set[str] = set()
        while True:
            data = self.gql(DETAIL, {"slug": resource.extra["slug"], "username": resource.extra["username"], "after": after})
            detail = data.get("customListBySlug")
            if not isinstance(detail, Mapping) or self.resource(detail).id != resource.id:
                raise PlexListError("plex_lists_invalid_resource")
            rows, after = self.page(detail.get("metadataItems"), seen)
            for row in rows:
                mid = str(row.get("id") or "")
                if not mid or mid in ids:
                    raise PlexListError("plex_lists_invalid_members")
                ids.add(mid)
                out.append(row)
            if after is None:
                if detail.get("itemCount") != len(out):
                    raise PlexListError("Plex cloud list changed or returned incomplete membership; retry")
                return out

    def metadata(self, mid: str) -> Mapping[str, Any]:
        if mid not in self.cache:
            data = self.request("GET", common.METADATA + "/library/metadata/" + quote(mid, safe=""), params={"includeGuids": 1})
            rows = (data.get("MediaContainer") or {}).get("Metadata")
            if not isinstance(rows, list) or len(rows) != 1 or str(rows[0].get("ratingKey")) != mid:
                raise PlexListError("plex_lists_metadata_unavailable")
            self.cache[mid] = rows[0]
        return self.cache[mid]

    def media(self, mid: str) -> dict[str, Any]:
        row = self.metadata(mid)
        kind = str(row.get("type") or "").lower()
        if kind not in MEDIA_TYPES:
            raise PlexListError("plex_lists_unsupported_media")
        item = {"type": kind, "ids": common.ids_from_discover_row(row), "title": row.get("title"), "year": row.get("year")}
        if kind in ("season", "episode"):
            parent = row.get("parentRatingKey") if kind == "season" else row.get("grandparentRatingKey")
            if not parent:
                raise PlexListError("plex_lists_show_metadata_unavailable")
            item["show_ids"] = common.ids_from_discover_row(self.metadata(str(parent)))
            item["season"] = common._safe_int(row.get("index") if kind == "season" else row.get("parentIndex"))
            if kind == "episode":
                item["episode"] = common._safe_int(row.get("index"))
                item["series_title"] = row.get("grandparentTitle")
            if item["season"] is None or (kind == "episode" and item["episode"] is None):
                raise PlexListError("plex_lists_episode_coordinates_unavailable")
        return item

    def match(self, kind: str, ids: Mapping[str, Any]) -> str | None:
        if not any(ids.get(key) for key in ("tmdb", "tvdb", "imdb")):
            mid = str(ids.get("plex") or "").rsplit("/", 1)[-1]
            if re.fullmatch(r"[0-9a-f]{24}", mid) and self.metadata(mid).get("type") == kind:
                return mid
            return None
        found: set[str] = set()
        for provider in ("tmdb", "tvdb", "imdb"):
            if not ids.get(provider):
                continue
            data = self.request("GET", common.METADATA + "/library/metadata/matches",
                                params={"type": 1 if kind == "movie" else 2, "guid": f"{provider}://{ids[provider]}", "includeGuids": 1})
            rows = (data.get("MediaContainer") or {}).get("Metadata")
            if not isinstance(rows, list):
                raise PlexListError("plex_lists_invalid_match_response")
            for row in rows:
                external = common.ids_from_discover_row(row)
                if row.get("type") != kind or str(external.get(provider) or "") != str(ids[provider]):
                    continue
                if any(ids.get(k) and external.get(k) and str(ids[k]) != external[k] for k in ("tmdb", "tvdb", "imdb")):
                    raise PlexListError("plex_lists_ambiguous_match")
                if row.get("ratingKey"):
                    found.add(str(row["ratingKey"]))
        if len(found) > 1:
            raise PlexListError("plex_lists_ambiguous_match")
        return next(iter(found), None)

    def child(self, parent: str, kind: str, index: int) -> str | None:
        start = 0
        seen: set[str] = set()
        matches: set[str] = set()
        while True:
            data = self.request("GET", common.METADATA + "/library/metadata/" + quote(parent, safe="") + "/children",
                                params={"includeGuids": 1, "X-Plex-Container-Start": start, "X-Plex-Container-Size": 100})
            container = data.get("MediaContainer") or {}
            rows = container.get("Metadata")
            if not isinstance(rows, list):
                raise PlexListError("plex_lists_invalid_children")
            for row in rows:
                mid = str(row.get("ratingKey") or "")
                if not mid or mid in seen:
                    raise PlexListError("plex_lists_repeated_children")
                seen.add(mid)
                if row.get("type") == kind and common._safe_int(row.get("index")) == index:
                    matches.add(mid)
            start += len(rows)
            total = common._safe_int(container.get("totalSize"))
            if total is not None and start < total and not rows:
                raise PlexListError("plex_lists_incomplete_children")
            if (total is not None and start >= total) or (total is None and len(rows) < 100):
                break
        if len(matches) > 1:
            raise PlexListError("plex_lists_ambiguous_match")
        return next(iter(matches), None)

    def resolve(self, item: Mapping[str, Any]) -> str | None:
        kind = str(item.get("type") or "").lower()
        if kind in ("movie", "show"):
            return self.match(kind, item.get("ids") or {})
        if kind not in ("season", "episode"):
            return None
        season = common._safe_int(item.get("season"))
        episode = common._safe_int(item.get("episode"))
        if season is None or season < 0 or (kind == "episode" and (episode is None or episode < 1)):
            return None
        show = self.match("show", item.get("show_ids") or (item.get("ids") if kind == "season" else {}) or {})
        parent = self.child(show, "season", season) if show else None
        if kind == "episode":
            if parent is None or episode is None:
                return None
            mid = self.child(parent, "episode", episode)
        else:
            mid = parent
        if mid and (kind == "episode" or item.get("show_ids")):
            external = common.ids_from_discover_row(self.metadata(mid))
            expected = item.get("ids") or {}
            if any(expected.get(key) and external.get(key) and str(expected[key]) != str(external[key]) for key in ("tmdb", "tvdb", "imdb")):
                raise PlexListError("plex_lists_ambiguous_match")
        return mid

    def writable(self, resource: PlaylistResource) -> str:
        if not resource.writable:
            raise PlaylistAuthError("Only your own Plex cloud Lists can be changed")
        return str(resource.extra["raw_id"])

    def edit(self, resource: PlaylistResource, changes: Sequence[Mapping[str, Any]]) -> None:
        result = self.gql(EDIT, {"id": self.writable(resource), "input": {"changes": list(changes)}})
        if str((result.get("editCustomListItems") or {}).get("id") or "") != resource.extra["raw_id"]:
            raise PlexListError("plex_lists_write_not_confirmed")


def list_resources(adapter: Any) -> list[PlaylistResource]:
    return Client(adapter).resources() if available(adapter) else []


def get_snapshot(adapter: Any, playlist_id: str) -> PlaylistSnapshot:
    client = Client(adapter)
    resource = client.find(playlist_id)
    items = [PlaylistItem.from_media(client.media(str(row["id"])), playlist_item_id=row["id"], provider_media_id=row["id"], position=i)
             for i, row in enumerate(client.members(resource))]
    return PlaylistSnapshot(resource=resource, items=items)


def create(adapter: Any, name: str, *, privacy: str | None = None, items: Sequence[Mapping[str, Any]] | None = None,
           dry_run: bool = False) -> PlaylistResource:
    name, visibility = _name(name), _privacy(privacy)
    if dry_run:
        return PlaylistResource(provider="PLEX", id="", name=name, instance=str(getattr(adapter, "instance_id", "default")),
                                can_add=True, can_remove=True, can_reorder=True, media_types=MEDIA_TYPES, extra={"endpoint_type": "cloud_list"})
    client = Client(adapter)
    client.account()
    result = client.gql(CREATE, {"input": {"name": name, "privacy": visibility, "ranked": False}})
    resource = client.resource(result.get("createCustomList"))
    if not resource.writable or resource.name != name or _privacy(resource.extra["privacy"]) != visibility:
        raise PlexListError("plex_lists_create_not_confirmed")
    if items:
        added = add(adapter, resource.id, items)
        if added["unresolved"]:
            raise PlexListError(f"Plex list {resource.id} was created, but some initial items could not be added")
    return resource


def update(adapter: Any, playlist_id: str, *, name: str | None = None, privacy: str | None = None,
           dry_run: bool = False) -> PlaylistResource:
    payload: dict[str, Any] = {}
    if name is not None:
        payload["name"] = _name(name)
    if privacy is not None:
        payload["privacy"] = _privacy(privacy)
    client = Client(adapter)
    resource = client.find(playlist_id)
    lid = client.writable(resource)
    if dry_run:
        return replace(resource, name=payload.get("name", resource.name), extra={**resource.extra, **({"privacy": privacy} if privacy else {})})
    changed = client.resource(client.gql(UPDATE, {"id": lid, "input": payload}).get("updateCustomList"))
    if changed.id != resource.id or (name is not None and changed.name != payload["name"]) or (privacy is not None and _privacy(changed.extra["privacy"]) != payload["privacy"]):
        raise PlexListError("plex_lists_update_not_confirmed")
    return changed


def delete(adapter: Any, playlist_id: str, *, dry_run: bool = False) -> dict[str, Any]:
    client = Client(adapter)
    resource = client.find(playlist_id)
    lid = client.writable(resource)
    if dry_run:
        return {"ok": True, "dry_run": True}
    client.gql(DELETE, {"id": lid})
    if any(row.id == resource.id for row in client.resources()):
        raise PlexListError("plex_lists_delete_not_confirmed")
    return {"ok": True, "count": 1}


def _write(adapter: Any, playlist_id: str, items: Sequence[Mapping[str, Any]], *, removing: bool) -> dict[str, Any]:
    client = Client(adapter)
    resource = client.find(playlist_id)
    client.writable(resource)
    members = client.members(resource)
    by_key = {canonical_key(client.media(str(row["id"]))): str(row["id"]) for row in members}
    present = {str(row["id"]) for row in members}
    candidates: dict[str, str] = {}
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in items:
        key = canonical_key(item)
        if key in seen:
            continue
        seen.add(key)
        mid = by_key.get(key) or client.resolve(item)
        if not mid:
            unresolved.append({"item": minimal(item), "key": key, "hint": "plex_cloud_title_unresolved"})
            continue
        candidates[key] = mid
    changes = [{"remove" if removing else "add": {"metadataID": mid}} for mid in dict.fromkeys(candidates.values())
               if (mid in present) == removing]
    for start in range(0, len(changes), 100):
        client.edit(resource, changes[start:start + 100])
    after = {str(row["id"]) for row in client.members(resource)} if changes else present
    confirmed = []
    for key, mid in candidates.items():
        if (mid in after) != removing:
            confirmed.append(key)
        else:
            unresolved.append({"item": {"key": key}, "key": key, "hint": "plex_lists_write_not_confirmed"})
    return {"ok": not unresolved, "count": len(confirmed), "confirmed_keys": confirmed,
            "unresolved": unresolved, "unresolved_keys": [row["key"] for row in unresolved]}


def add(adapter: Any, playlist_id: str, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, items, removing=False)


def remove(adapter: Any, playlist_id: str, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, items, removing=True)


def reorder(adapter: Any, playlist_id: str, ordered_keys: Sequence[str]) -> dict[str, Any]:
    client = Client(adapter)
    resource = client.find(playlist_id)
    client.writable(resource)
    members = client.members(resource)
    current = [str(row["id"]) for row in members]
    by_key = {canonical_key(client.media(mid)): mid for mid in current}
    desired = list(dict.fromkeys(by_key[key] for key in ordered_keys if key in by_key))
    selected = set(desired)
    desired.extend(mid for mid in current if mid not in selected)
    changes = []
    work = list(current)
    for pos, mid in enumerate(desired):
        old = work.index(mid)
        if old != pos:
            changes.append({"reorder": {"position": old, "newPosition": pos}})
            work.insert(pos, work.pop(old))
    for start in range(0, len(changes), 100):
        client.edit(resource, changes[start:start + 100])
    if changes and [str(row["id"]) for row in client.members(resource)] != desired:
        raise PlexListError("plex_lists_order_not_confirmed")
    return {"ok": True, "count": len(changes), "reordered": len(changes)}

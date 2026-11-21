# /tests/test_plex_cloud_lists.py
# Plex cloud Lists privacy, membership, account scope, and response validation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from types import SimpleNamespace

import pytest
import requests

from cw_platform.playlists import PlaylistAuthError, resolve_create_privacy
from providers.sync import _mod_PLEX
from providers.sync.plex import _cloud_lists as lists
from providers.sync.plex import _playlists as playlists


def metadata(mid, kind="movie", **extra):
    return {"ratingKey": mid, "type": kind, "title": mid, "Guid": [{"id": f"tmdb://{mid}"}], **extra}


def page(rows, after=None):
    start = int(after or 0)
    end = start + 2
    return {"nodes": deepcopy(rows[start:end]), "pageInfo": {"hasNextPage": end < len(rows), "endCursor": str(end)}}


class Session:
    def __init__(self):
        self.calls = []
        self.owner = {"uuid": "owner", "username": "Owner", "restricted": False}
        self.rows = {"a": {"id": "a", "name": "Movies", "slug": "movies", "privacy": "PRIVATE", "ranked": False,
                           "user": {"id": "owner", "username": "Owner"}}}
        self.members = {"a": ["1", "2", "3"]}
        self.media = {mid: metadata(mid) for mid in self.members["a"]}
        self.ignore_writes = False
        self.error = None
        self.refused: set[str] = set()
        self.off_discover: set[str] = set()
        self.edit_errors = None

    def resource(self, lid):
        return {**self.rows[lid], "itemCount": len(self.members[lid])}

    def request(self, method, url, **kwargs):
        self.calls.append((method, url, deepcopy(kwargs)))
        assert kwargs["verify"] is True and kwargs["allow_redirects"] is False
        assert kwargs["headers"]["X-Plex-Token"] == "account-secret"
        if self.error:
            if isinstance(self.error, Exception):
                raise self.error
            return SimpleNamespace(status_code=self.error, json=lambda: {})
        if url.startswith(lists.common.DISCOVER):
            return SimpleNamespace(status_code=404 if url.rsplit("/", 1)[-1] in self.off_discover else 200, json=lambda: {})
        if url == "https://plex.tv/api/v2/user":
            result = self.owner
        elif url == lists.COMMUNITY:
            payload = kwargs["json"]
            query, variables = payload["query"], payload["variables"]
            if query == lists.LISTS:
                result = {"user": {"customLists": page([self.resource(k) for k in self.rows], variables["after"])}}
            elif query == lists.DETAIL:
                lid = next(k for k, v in self.rows.items() if v["slug"] == variables["slug"])
                result = {"customListBySlug": {**self.resource(lid), "metadataItems": page(
                    [{"id": mid, "type": self.media[mid]["type"].upper()} for mid in self.members[lid]], variables["after"])}}
            elif query == lists.CREATE:
                self.rows["new"] = {**self.rows["a"], "id": "new", "slug": "new", **variables["input"]}
                self.members["new"] = []
                result = {"createCustomList": self.resource("new")}
            elif query == lists.UPDATE:
                lid = variables["id"]
                if not self.ignore_writes:
                    self.rows[lid].update(variables["input"])
                result = {"updateCustomList": self.resource(lid)}
            elif query == lists.DELETE:
                if not self.ignore_writes:
                    del self.rows[variables["id"]]
                result = {"removeCustomList": True}
            elif query == lists.EDIT:
                changes = variables["input"]["changes"]
                if self.edit_errors or any((c.get("add") or c.get("remove") or {}).get("metadataID") in self.refused for c in changes):
                    errors = self.edit_errors or [{"message": "An internal server error occurred."}]
                    return SimpleNamespace(status_code=200, json=lambda: {"errors": deepcopy(errors), "data": None})
                members = self.members[variables["id"]]
                if not self.ignore_writes:
                    for change in variables["input"]["changes"]:
                        if "add" in change:
                            members.append(change["add"]["metadataID"])
                        elif "remove" in change:
                            members.remove(change["remove"]["metadataID"])
                        else:
                            move = change["reorder"]
                            members.insert(move["newPosition"], members.pop(move["position"]))
                result = {"editCustomListItems": {"id": variables["id"]}}
            else:
                raise AssertionError(query)
            result = {"data": result}
        elif url.endswith("/matches"):
            guid = kwargs["params"]["guid"]
            result = {"MediaContainer": {"Metadata": [row for row in self.media.values() if {"id": guid} in row["Guid"]]}}
        elif url.endswith("/children"):
            parent = url.split("/")[-2]
            rows = [row for row in self.media.values() if row.get("parentRatingKey") == parent]
            result = {"MediaContainer": {"Metadata": rows, "totalSize": len(rows)}}
        else:
            result = {"MediaContainer": {"Metadata": [self.media[url.rsplit("/", 1)[-1]]]}}
        return SimpleNamespace(status_code=200, json=lambda: deepcopy(result))

    def writes(self):
        return [kw["json"] for method, url, kw in self.calls if method == "POST" and kw["json"]["query"].startswith("mutation")]


@pytest.fixture
def adapter():
    return SimpleNamespace(instance_id="default", config={"plex": {"account_token": "account-secret", "username": "Owner", "account_id": 1}},
                           client=SimpleNamespace(session=Session(), server=None))


def test_cloud_only_resources_and_snapshot_preserve_order(adapter):
    resource = next(r for r in playlists.list_resources(adapter) if r.id == "cloud:a")
    assert resource.can_reorder and resource.extra["privacy"] == "private"
    snapshot = playlists.get_snapshot(adapter, resource.id)
    assert snapshot.ordered_keys() == ["tmdb:1", "tmdb:2", "tmdb:3"]
    assert [item.provider_media_id for item in snapshot.items] == ["1", "2", "3"]
    assert [item.position for item in snapshot.items] == [0, 1, 2]


def test_resource_pagination(adapter):
    session = adapter.client.session
    for lid in ("b", "c", "d"):
        session.rows[lid] = {**session.rows["a"], "id": lid, "slug": lid}
        session.members[lid] = []
    assert [r.id for r in lists.list_resources(adapter)] == ["cloud:a", "cloud:b", "cloud:c", "cloud:d"]


@pytest.mark.parametrize("failure", ["selected_user", "restricted", "http", "rate_limit", "transport", "graphql", "bad_resource"])
@pytest.mark.parametrize("server_connected", [False, True])
def test_cloud_discovery_failure_depends_on_server_connection(adapter, monkeypatch, failure, server_connected):
    session = adapter.client.session
    adapter.client.server = object() if server_connected else None
    monkeypatch.setattr(playlists, "_iter_video_playlists", lambda srv: [SimpleNamespace(ratingKey=7, title="Local")])
    monkeypatch.setattr(playlists, "_iter_collections", lambda srv: [(SimpleNamespace(key=1, type="movie"), SimpleNamespace(ratingKey=8, title="Collection"))])
    if failure == "selected_user":
        adapter.client.selected_username = "Other"
    elif failure == "restricted":
        session.owner["restricted"] = True
    elif failure in ("http", "rate_limit"):
        session.error = 503 if failure == "http" else 429
    elif failure == "transport":
        session.error = requests.ConnectionError("offline")
    elif failure == "graphql":
        def reject(*args, **kwargs):
            raise lists.PlexListError("query failed")
        monkeypatch.setattr(lists.Client, "gql", reject)
    else:
        session.rows["a"]["privacy"] = "unknown"
    if server_connected:
        resources = playlists.list_resources(adapter)
        assert [r.id for r in resources] == [playlists.WATCHLIST_ID, "7", "collection:1:8"]
        assert resources[1].can_reorder and not resources[2].can_reorder
    else:
        with pytest.raises((lists.PlexListError, PlaylistAuthError)):
            playlists.list_resources(adapter)
    with pytest.raises((lists.PlexListError, PlaylistAuthError)):
        playlists.get_snapshot(adapter, "cloud:a")
    assert not session.writes()


def test_list_detail_failure_is_not_hidden(adapter, monkeypatch):
    assert any(r.id == "cloud:a" for r in playlists.list_resources(adapter))
    def reject(*args, **kwargs):
        raise lists.PlexListError("bad member metadata")
    monkeypatch.setattr(lists.Client, "metadata", reject)
    with pytest.raises(lists.PlexListError, match="bad member"):
        playlists.get_snapshot(adapter, "cloud:a")


@pytest.mark.parametrize("wire", ["ANYONE", "PUBLIC"])
def test_public_privacy_read_aliases(adapter, wire):
    adapter.client.session.rows["a"]["privacy"] = wire
    assert lists.list_resources(adapter)[0].extra["privacy"] == "public"


@pytest.mark.parametrize("privacy,wire", list(lists.PRIVACY.items()))
def test_create_and_edit_privacy(adapter, privacy, wire):
    resource = playlists.create(adapter, " New list ", media_type="cloud_list", privacy=privacy)
    assert resource.name == "New list" and resource.extra["privacy"] == privacy
    assert adapter.client.session.writes()[0]["variables"]["input"]["privacy"] == wire
    changed = lists.update(adapter, "cloud:a", privacy=privacy.upper())
    assert changed.extra["privacy"] == privacy


def test_default_privacy_and_dry_runs(adapter):
    assert playlists.create(adapter, "New", media_type="cloud_list").extra["privacy"] == "private"
    before = len(adapter.client.session.writes())
    playlists.create(adapter, "Dry", media_type="cloud_list", dry_run=True)
    assert playlists.rename(adapter, "cloud:a", "Renamed", dry_run=True).name == "Renamed"
    playlists.delete(adapter, "cloud:a", dry_run=True)
    assert len(adapter.client.session.writes()) == before
    with pytest.raises(ValueError):
        lists.create(adapter, "New", privacy="invalid")
    assert len(adapter.client.session.writes()) == before


def test_add_remove_reorder_rename_delete(adapter):
    session = adapter.client.session
    session.media["4"] = metadata("4")
    item = {"type": "movie", "ids": {"tmdb": "4"}}
    assert playlists.add(adapter, "cloud:a", [item, item])["confirmed_keys"] == ["tmdb:4"]
    assert session.members["a"] == ["1", "2", "3", "4"]
    writes = len(session.writes())
    assert playlists.add(adapter, "cloud:a", [item])["ok"]
    assert len(session.writes()) == writes
    result = playlists.reorder(adapter, "cloud:a", ["tmdb:4", "tmdb:4", "tmdb:2", "missing"])
    assert result["reordered"] == 2 and session.members["a"] == ["4", "2", "1", "3"]
    assert playlists.reorder(adapter, "cloud:a", ["tmdb:4", "tmdb:2"])["reordered"] == 0
    assert playlists.remove(adapter, "cloud:a", [item])["confirmed_keys"] == ["tmdb:4"]
    assert playlists.rename(adapter, "cloud:a", "Renamed").name == "Renamed"
    assert playlists.delete(adapter, "cloud:a")["ok"] and "a" not in session.rows


@pytest.mark.parametrize("operation", ["add", "remove", "reorder", "rename", "delete", "privacy"])
def test_mutations_require_ownership(adapter, operation):
    adapter.client.session.rows["a"]["user"]["id"] = "other"
    calls = {"add": lambda: lists.add(adapter, "cloud:a", []), "remove": lambda: lists.remove(adapter, "cloud:a", []),
             "reorder": lambda: lists.reorder(adapter, "cloud:a", []), "rename": lambda: lists.update(adapter, "cloud:a", name="X"),
             "delete": lambda: lists.delete(adapter, "cloud:a"), "privacy": lambda: lists.update(adapter, "cloud:a", privacy="public")}
    with pytest.raises(PlaylistAuthError):
        calls[operation]()
    assert not adapter.client.session.writes()


@pytest.mark.parametrize("change", ["restricted", "username", "selected_user", "home", "shared", "suppressed", "server_token"])
def test_account_scope_rejects_managed_mismatched_and_server_tokens(adapter, change):
    if change == "restricted":
        adapter.client.session.owner["restricted"] = True
    elif change == "username":
        adapter.client.session.owner["username"] = "Other"
    elif change == "selected_user":
        adapter.client.selected_username = "Other"
    elif change in ("home", "shared"):
        adapter.client._user_scope_kind = change
    elif change == "suppressed":
        adapter.client._cloud_token_suppressed = True
    else:
        adapter.config["plex"] = {"pms_token": "server-secret"}
    with pytest.raises(PlaylistAuthError):
        lists.create(adapter, "New")
    assert not adapter.client.session.writes()


@pytest.mark.parametrize("status", [401, 403, 429, 500, 302])
def test_http_errors_fail_without_writes(adapter, status):
    adapter.client.session.error = status
    with pytest.raises((lists.PlexListError, PlaylistAuthError)):
        lists.get_snapshot(adapter, "cloud:a")
    assert not adapter.client.session.writes()


def test_transport_error_redacts_credentials(adapter):
    adapter.client.session.error = requests.ConnectionError("account-secret")
    with pytest.raises(lists.PlexListError) as caught:
        lists.list_resources(adapter)
    assert "account-secret" not in str(caught.value)


def test_graphql_errors_and_incomplete_pages_fail_closed(adapter, monkeypatch):
    client = lists.Client(adapter)
    client.account()
    monkeypatch.setattr(client, "request", lambda *a, **kw: {"errors": [{"message": "secret"}], "data": {"user": None}})
    with pytest.raises(lists.PlexListError, match="rejected"):
        client.resources()
    for bad in ({}, {"nodes": [], "pageInfo": {"hasNextPage": True, "endCursor": "1"}},
                {"nodes": [{}], "pageInfo": {"hasNextPage": True, "endCursor": "seen"}}):
        with pytest.raises(lists.PlexListError):
            client.page(bad, {"seen"})


@pytest.mark.parametrize("failure", ["missing", "duplicate", "count"])
def test_membership_never_returns_partial_snapshots(adapter, monkeypatch, failure):
    session = adapter.client.session
    original = session.request
    def request(method, url, **kwargs):
        response = original(method, url, **kwargs)
        data = response.json()
        if kwargs.get("json", {}).get("query") == lists.DETAIL:
            detail = data["data"]["customListBySlug"]
            if failure == "count":
                detail["itemCount"] += 1
            elif failure == "missing":
                detail.pop("metadataItems")
            else:
                detail["metadataItems"]["nodes"] *= 2
        return SimpleNamespace(status_code=200, json=lambda: data)
    monkeypatch.setattr(session, "request", request)
    with pytest.raises(lists.PlexListError):
        lists.get_snapshot(adapter, "cloud:a")


def test_writes_must_be_confirmed_by_membership_and_metadata(adapter):
    session = adapter.client.session
    session.ignore_writes = True
    session.media["4"] = metadata("4")
    result = lists.add(adapter, "cloud:a", [{"type": "movie", "ids": {"tmdb": "4"}}])
    assert not result["ok"] and not result["confirmed_keys"]
    assert result["unresolved_keys"] == ["tmdb:4"]
    for call in (lambda: lists.update(adapter, "cloud:a", privacy="public"), lambda: lists.delete(adapter, "cloud:a"),
                 lambda: lists.reorder(adapter, "cloud:a", ["tmdb:3"])):
        with pytest.raises(lists.PlexListError, match="not_confirmed"):
            call()


def test_season_episode_matching_and_snapshot(adapter):
    session = adapter.client.session
    session.media.update({"10": metadata("10", "show"),
                          "11": metadata("11", "season", parentRatingKey="10", index=0),
                          "12": metadata("12", "episode", parentRatingKey="11", grandparentRatingKey="10", parentIndex=0, index=1)})
    client = lists.Client(adapter)
    season = {"type": "season", "show_ids": {"tmdb": "10"}, "season": 0}
    episode = {"type": "episode", "show_ids": {"tmdb": "10"}, "ids": {"tmdb": "12"}, "season": 0, "episode": 1}
    assert client.resolve(season) == "11" and client.resolve(episode) == "12"
    assert client.resolve({**episode, "episode": 99}) is None
    assert client.resolve({**episode, "episode": 0}) is None
    with pytest.raises(lists.PlexListError, match="ambiguous"):
        client.resolve({**episode, "ids": {"tmdb": "999"}})
    session.members["a"] = ["11", "12"]
    snap = lists.get_snapshot(adapter, "cloud:a")
    assert snap.ordered_keys() == ["tmdb:10#season:0", "tmdb:10#s00e01"]


def test_unresolved_and_ambiguous_titles_are_never_added(adapter):
    session = adapter.client.session
    assert lists.add(adapter, "cloud:a", [{"type": "movie", "ids": {"tmdb": "99"}}])["unresolved_keys"] == ["tmdb:99"]
    session.media["4"] = metadata("4", Guid=[{"id": "tmdb://99"}])
    session.media["5"] = metadata("5", Guid=[{"id": "tmdb://99"}])
    with pytest.raises(lists.PlexListError, match="ambiguous"):
        lists.add(adapter, "cloud:a", [{"type": "movie", "ids": {"tmdb": "99"}}])
    assert not session.writes()


def test_capabilities_scope_privacy_to_cloud_lists():
    caps = _mod_PLEX.OPS.capabilities()["playlists"]
    assert "collection" not in caps["ordered_endpoint_types"]
    assert "collection" in caps["unordered_endpoint_types"]
    assert resolve_create_privacy(_mod_PLEX.OPS, None, "cloud_list") == "private"
    assert resolve_create_privacy(_mod_PLEX.OPS, "public", "playlist") is None
    assert resolve_create_privacy(_mod_PLEX.OPS, "public", "collection") is None
    with pytest.raises(ValueError, match="only"):
        _mod_PLEX.OPS.update_playlist_privacy({}, "collection:1:3", "public")


def test_refused_items_are_skipped_and_the_rest_is_added(adapter, monkeypatch):
    logged = []
    monkeypatch.setattr(lists, "cw_log", lambda *args, **fields: logged.append((args, fields)))
    session = adapter.client.session
    for mid in ("4", "5", "6", "7"):
        session.media[mid] = metadata(mid)
    session.refused = {"5", "7"}
    session.off_discover = {"5", "6"}
    items = [{"type": "movie", "title": f"Movie {mid}", "year": 2001, "ids": {"tmdb": mid}} for mid in ("4", "5", "6", "7")]
    result = lists.add(adapter, "cloud:a", items)
    assert session.members["a"] == ["1", "2", "3", "4", "6"]
    assert sorted(result["confirmed_keys"]) == ["tmdb:4", "tmdb:6"]
    assert {row["key"]: row["hint"] for row in result["unresolved"]} == {
        "tmdb:5": "plex_cloud_not_on_discover", "tmdb:7": "plex_cloud_list_item_refused"}
    refused = [fields for args, fields in logged if args[3] == "cloud_list_item_refused"]
    assert {row["title"] for row in refused} == {"Movie 5", "Movie 7"}
    rejected = [fields for args, fields in logged if args[3] == "cloud_list_request_rejected"]
    assert rejected and rejected[0]["operation"] == "CrossWatchEditList"
    assert rejected[0]["message"] == "An internal server error occurred."


def test_a_lone_refused_item_does_not_fail_the_sync(adapter):
    session = adapter.client.session
    session.media["4"] = metadata("4")
    session.refused = {"4"}
    result = lists.add(adapter, "cloud:a", [{"type": "movie", "ids": {"tmdb": "4"}}])
    assert [(row["key"], row["hint"]) for row in result["unresolved"]] == [("tmdb:4", "plex_cloud_list_item_refused")]
    assert session.members["a"] == ["1", "2", "3"]


def test_rate_limited_edits_are_not_split(adapter):
    session = adapter.client.session
    for mid in ("4", "5"):
        session.media[mid] = metadata(mid)
    session.edit_errors = [{"message": "slow down", "extensions": {"code": "RATE_LIMITED"}}]
    with pytest.raises(lists.PlexListError, match="rate limited"):
        lists.add(adapter, "cloud:a", [{"type": "movie", "ids": {"tmdb": mid}} for mid in ("4", "5")])
    assert len(session.writes()) == 1


def test_create_keeps_the_list_when_a_seed_item_is_refused(adapter):
    session = adapter.client.session
    for mid in ("4", "5"):
        session.media[mid] = metadata(mid)
    session.refused = session.off_discover = {"5"}
    resource = lists.create(adapter, "New", items=[{"type": "movie", "ids": {"tmdb": mid}} for mid in ("4", "5")])
    assert resource.id == "cloud:new" and session.members["new"] == ["4"]
    assert resource.extra["seed_unresolved_keys"] == ["tmdb:5"]

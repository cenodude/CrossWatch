# /tests/test_tofa_playlists.py
# Tofa playlist contract and custom collection regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from typing import Any

import pytest

from cw_platform.playlists import PlaylistReadOnlyError, playlist_capabilities, supports_playlists
from providers.sync._mod_TOFA import OPS
from providers.sync.tofa._playlists import TofaPlaylistError

CFG = {"tofa": {"server_url": "http://tofa.local:33333", "access_token": "test-token", "profile_id": "p1"}}
BASE = "/collections/custom"
MOVIE = {"type": "movie", "ids": {"tmdb": "100"}}
SHOW = {"type": "show", "ids": {"tmdb": "100"}}


def _member(mid: str, tmdb: int, kind: str, position: int, **kwargs: Any) -> dict[str, Any]:
    return {"member_id": mid, "tmdb_id": tmdb, "type": kind, "position": position,
            "title": f"Title {tmdb}", "in_library": False, **kwargs}


class Response:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self.payload = payload
        self.text = "x" if payload is not None else ""

    def json(self) -> Any:
        return self.payload


class Server:
    def __init__(self):
        self.rows = {
            "c1": {"id": "c1", "name": "Mine", "can_edit": True, "sort_mode": "custom", "visibility": "private",
                   "items": [], "members": [_member("m-show", 100, "tv", 1), _member("m-movie", 100, "movie", 0)]},
            "smart": {"id": "smart", "name": "Smart", "can_edit": True, "smart_rules": {}, "members": None,
                      "items": [{"id": "local-1", "tmdb_id": 200, "media_type": "movie", "title": "Beta"}]},
            "external": {"id": "external", "name": "Imported", "can_edit": True, "source": {"sync_mode": "mirror"},
                         "members": [], "items": []},
            "shared": {"id": "shared", "name": "Shared", "can_edit": False, "members": [], "items": []},
        }
        self.calls: list[dict[str, Any]] = []
        self.overrides: dict[tuple[str, str], Response] = {}
        self.empty_patch_reply = False

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> Response:
        path = url.split("/api/v1", 1)[1]
        self.calls.append({"method": method, "path": path, **kwargs})
        if (method, path) in self.overrides:
            return self.overrides[(method, path)]
        if path == BASE:
            if method == "GET":
                return Response(200, {"collections": deepcopy(list(self.rows.values())), "containing": []})
            if method == "POST":
                row = {"id": "new", "can_edit": True, "items": [], "members": [], **kwargs["json"]}
                self.rows["new"] = row
                return Response(200, deepcopy(row))
        parts = path[len(BASE) + 1:].split("/")
        row = self.rows.get(parts[0])
        if row is None:
            return Response(404)
        if len(parts) == 1:
            if method == "GET":
                return Response(200, deepcopy(row))
            if method == "PATCH":
                row.update(kwargs["json"])
                return Response(204) if self.empty_patch_reply else Response(200, deepcopy(row))
            if method == "DELETE":
                del self.rows[parts[0]]
                return Response(204)
        if parts[1:] == ["members"] and method == "POST":
            payload = kwargs["json"]
            mid = f"added-{len(row['members'])}"
            row["members"].append(_member(mid, payload["tmdb_id"], payload["media_type"], len(row["members"])))
            return Response(200, {"member_id": mid, "added": True})
        if parts[1:] == ["members", "order"] and method == "PUT":
            for member in row["members"]:
                member["position"] = kwargs["json"]["member_ids"].index(member["member_id"])
            return Response(204)
        if len(parts) == 3 and parts[1] == "members" and method == "DELETE":
            row["members"] = [m for m in row["members"] if m["member_id"] != parts[2]]
            return Response(204)
        raise AssertionError(f"unexpected {method} {path}")

    def writes(self) -> list[dict[str, Any]]:
        return [call for call in self.calls if call["method"] != "GET"]


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch) -> Server:
    server = Server()
    monkeypatch.setattr("providers.sync.tofa._common.request_with_auth", server)
    return server


def test_resources_capabilities_and_permissions(server: Server):
    assert supports_playlists(OPS)
    assert OPS.features()["playlists"]
    caps = playlist_capabilities(OPS)
    assert caps["create_endpoint_types"] == ["collection"]
    assert caps["create_privacy"]["values"] == ["private", "household"]
    resources = {r.id: r for r in OPS.list_playlist_resources(CFG, instance="family")}
    assert resources["collection:c1"].can_reorder
    assert resources["collection:c1"].instance == "family"
    assert resources["collection:c1"].extra["can_delete"]
    assert resources["collection:smart"].is_smart
    for cid in ("smart", "external", "shared"):
        assert not resources[f"collection:{cid}"].writable
    assert server.calls[0]["params"] == {"include_hidden": "true", "rollups": "false"}
    assert server.calls[0]["instance_id"] == "family"


def test_empty_patch_reply_refetches_saved_name_and_privacy(server: Server):
    server.empty_patch_reply = True
    assert OPS.rename_playlist(CFG, "collection:c1", "Changed").name == "Changed"
    assert OPS.update_playlist_privacy(CFG, "collection:c1", "household").extra["privacy"] == "household"
    assert [call["method"] for call in server.calls] == ["GET", "PATCH", "GET", "GET", "PATCH", "GET"]


@pytest.mark.parametrize("privacy", [False, True])
def test_empty_patch_reply_does_not_confirm_unsaved_changes(server: Server, privacy: bool):
    server.overrides[("PATCH", BASE + "/c1")] = Response(204)
    with pytest.raises(TofaPlaylistError, match="not_confirmed"):
        if privacy:
            OPS.update_playlist_privacy(CFG, "collection:c1", "household")
        else:
            OPS.rename_playlist(CFG, "collection:c1", "Changed")


def test_snapshot_includes_unowned_titles_and_distinguishes_show_ids(server: Server):
    snap = OPS.get_playlist_snapshot(CFG, "collection:c1")
    assert snap.ordered_keys() == ["tmdb:100", "tmdb:100#show"]
    assert [i.playlist_item_id for i in snap.items] == ["m-movie", "m-show"]
    assert server.calls[0]["params"] == {"all_members": "true", "sort": "custom"}
    smart = OPS.get_playlist_snapshot(CFG, "collection:smart")
    assert smart.ordered_keys() == ["tmdb:200"]
    assert smart.items[0].provider_media_id == "local-1"


def test_add_deduplicates_existing_and_handles_remote_missing_titles(server: Server):
    new = {"type": "movie", "ids": {"tmdb": "200"}}
    result = OPS.add_playlist_items(CFG, "collection:c1", [MOVIE, new, new])
    assert result["confirmed_keys"] == ["tmdb:100", "tmdb:200"]
    assert len(server.writes()) == 1
    assert server.writes()[0]["json"] == {"tmdb_id": 200, "media_type": "movie"}
    assert OPS.get_playlist_snapshot(CFG, "collection:c1").ordered_keys()[-1] == "tmdb:200"


def test_add_tv_and_report_unsupported_items(server: Server):
    server.rows["c1"]["members"] = []
    result = OPS.add_playlist_items(CFG, "collection:c1", [SHOW, {"type": "episode", "ids": {"tmdb": "1"}},
                                                             {"type": "movie", "ids": {"imdb": "tt123"}}])
    assert result["confirmed_keys"] == ["tmdb:100#show"]
    assert [r["hint"] for r in result["unresolved"]] == ["unsupported_type", "missing_tmdb_id"]
    assert server.writes()[0]["json"] == {"tmdb_id": 100, "media_type": "tv"}


@pytest.mark.parametrize("response", [Response(403), Response(500), Response(200, {}), Response(200, {"added": True})])
def test_failed_add_is_not_confirmed(server: Server, response: Response):
    server.overrides[("POST", BASE + "/c1/members")] = response
    result = OPS.add_playlist_items(CFG, "collection:c1", [{"type": "movie", "ids": {"tmdb": "200"}}])
    assert not result["ok"]
    assert result["confirmed_keys"] == []
    assert result["unresolved_keys"] == ["tmdb:200"]


def test_remove_uses_membership_ids_and_is_idempotent(server: Server):
    server.rows["c1"]["members"].append(_member("duplicate", 100, "movie", 2))
    result = OPS.remove_playlist_items(CFG, "collection:c1", [MOVIE, MOVIE])
    assert result["confirmed_keys"] == ["tmdb:100"]
    assert {c["path"] for c in server.writes()} == {BASE + "/c1/members/m-movie", BASE + "/c1/members/duplicate"}
    assert OPS.get_playlist_snapshot(CFG, "collection:c1").ordered_keys() == ["tmdb:100#show"]
    assert OPS.remove_playlist_items(CFG, "collection:c1", [MOVIE])["confirmed_keys"] == ["tmdb:100"]
    assert len(server.writes()) == 2


def test_remove_failure_is_unresolved(server: Server):
    server.overrides[("DELETE", BASE + "/c1/members/m-movie")] = Response(403)
    result = OPS.remove_playlist_items(CFG, "collection:c1", [MOVIE])
    assert not result["ok"]
    assert result["unresolved_keys"] == ["tmdb:100"]


def test_reorder_preserves_unsupported_members_and_sets_custom_sort(server: Server):
    server.rows["c1"]["members"].append(_member("season", 100, "tv", 2, season_number=1))
    server.rows["c1"]["sort_mode"] = "title"
    result = OPS.reorder_playlist_items(CFG, "collection:c1", ["tmdb:100#show", "tmdb:100#show", "missing", "tmdb:100"])
    assert result["reordered"] == 3
    assert server.writes()[0]["json"] == {"member_ids": ["m-show", "m-movie", "season"]}
    assert server.writes()[1]["json"] == {"sort_mode": "custom"}
    assert OPS.reorder_playlist_items(CFG, "collection:c1", ["tmdb:100#show", "tmdb:100"])["reordered"] == 0
    assert len(server.writes()) == 2


def test_manage_collection_and_privacy(server: Server):
    created = OPS.create_playlist(CFG, "New", items=[MOVIE], privacy="household")
    assert created.id == "collection:new"
    assert server.writes()[0]["json"] == {"name": "New", "sort_mode": "custom", "visibility": "household"}
    renamed = OPS.rename_playlist(CFG, created.id, "Renamed")
    assert renamed.name == "Renamed"
    updated = OPS.update_playlist_privacy(CFG, created.id, "private")
    assert updated.extra["privacy"] == "private"
    assert OPS.delete_playlist(CFG, created.id)["ok"]
    assert "new" not in server.rows


def test_create_defaults_private_and_rejects_bad_privacy(server: Server):
    assert OPS.create_playlist(CFG, "Private").extra["privacy"] == "private"
    with pytest.raises(ValueError):
        OPS.create_playlist(CFG, "Invalid", privacy="public")
    assert len(server.writes()) == 1


@pytest.mark.parametrize("cid", ["smart", "shared", "external"])
@pytest.mark.parametrize("operation", ["add", "remove", "rename", "delete", "reorder", "privacy"])
def test_nonwritable_collections_never_mutate(server: Server, cid: str, operation: str):
    pid = f"collection:{cid}"
    actions = {
        "add": lambda: OPS.add_playlist_items(CFG, pid, [MOVIE]),
        "remove": lambda: OPS.remove_playlist_items(CFG, pid, [MOVIE]),
        "rename": lambda: OPS.rename_playlist(CFG, pid, "Other"),
        "delete": lambda: OPS.delete_playlist(CFG, pid),
        "reorder": lambda: OPS.reorder_playlist_items(CFG, pid, ["tmdb:100"]),
        "privacy": lambda: OPS.update_playlist_privacy(CFG, pid, "private"),
    }
    with pytest.raises(PlaylistReadOnlyError):
        actions[operation]()
    assert not server.writes()


def test_dry_runs_never_mutate(server: Server):
    OPS.create_playlist(CFG, "New", items=[MOVIE], dry_run=True)
    OPS.add_playlist_items(CFG, "collection:c1", [MOVIE], dry_run=True)
    OPS.remove_playlist_items(CFG, "collection:c1", [MOVIE], dry_run=True)
    OPS.rename_playlist(CFG, "collection:c1", "Other", dry_run=True)
    OPS.update_playlist_privacy(CFG, "collection:c1", "household", dry_run=True)
    OPS.reorder_playlist_items(CFG, "collection:c1", ["tmdb:100#show"], dry_run=True)
    OPS.delete_playlist(CFG, "collection:c1", dry_run=True)
    assert not server.writes()


@pytest.mark.parametrize("payload", [{}, {"collections": None}, {"collections": [{}]}])
def test_invalid_resource_lists_fail_closed(server: Server, payload: Any):
    server.overrides[("GET", BASE)] = Response(200, payload)
    with pytest.raises(TofaPlaylistError):
        OPS.list_playlist_resources(CFG)


@pytest.mark.parametrize("members", [None, {}, [{}], [_member("a", 100, "movie", 0), _member("a", 200, "movie", 1)]])
def test_invalid_members_fail_before_writes(server: Server, members: Any):
    server.rows["c1"]["members"] = members
    with pytest.raises(TofaPlaylistError):
        OPS.get_playlist_snapshot(CFG, "collection:c1")
    with pytest.raises(TofaPlaylistError):
        OPS.remove_playlist_items(CFG, "collection:c1", [MOVIE])
    assert not server.writes()


def test_service_exposes_collection_management(server: Server, monkeypatch: pytest.MonkeyPatch):
    from services import playlists

    monkeypatch.setattr(playlists, "_providers", lambda: {"TOFA": OPS})
    result = playlists.create_provider_playlist(CFG, "TOFA", "default", "New", media_type="collection", privacy="household")
    assert result["ok"]
    assert result["resource"]["can_rename"]
    assert result["resource"]["can_delete"]
    assert result["resource"]["can_edit_privacy"]
    assert result["resource"]["privacy"] == "household"

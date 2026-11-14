# tests/test_playlist_create_privacy.py
# CrossWatch - Privacy choice when CrossWatch creates a provider list
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import pytest

from cw_platform import playlists_runner as runner
from cw_platform.playlists import PlaylistResource, create_privacy_options, resolve_create_privacy
from services import playlists as svc


class _Ops:
    def __init__(self, name: str, privacy: dict[str, Any] | None = None, types: list[str] | None = None, resources: list[PlaylistResource] | None = None):
        self._name, self.privacy, self.types, self.calls = name, privacy, types, []
        self.resources = list(resources or [])

    def name(self) -> str:
        return self._name

    def capabilities(self) -> dict[str, Any]:
        caps: dict[str, Any] = {"create": True}
        if self.privacy is not None:
            caps["create_privacy"] = self.privacy
        if self.types:
            caps["create_endpoint_types"] = self.types
        return {"playlists": caps}

    def is_configured(self, cfg) -> bool:
        return True

    def list_playlist_resources(self, cfg, *, instance=None):
        return list(self.resources)

    def update_playlist_privacy(self, cfg, playlist_id, privacy, *, instance=None, dry_run=False):
        self.calls.append({"update": playlist_id, "privacy": privacy})
        res = next(r for r in self.resources if r.id == playlist_id)
        return PlaylistResource(provider=self._name, id=res.id, name=res.name, can_add=True, can_remove=True, extra={**res.extra, "privacy": privacy})

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        raise AssertionError

    def create_playlist(self, cfg, name, *, media_type=None, instance=None, dry_run=False, **kwargs):
        self.calls.append({"name": name, "media_type": media_type, **kwargs})
        return PlaylistResource(provider=self._name, id="new", name=name)

    def add_playlist_items(self, *a, **k):
        return {"ok": True}

    def remove_playlist_items(self, *a, **k):
        return {"ok": True}

    def reorder_playlist_items(self, *a, **k):
        return {"ok": True}


PRIVACY = {"values": ["public", "unlisted", "private"], "default": "public"}


@pytest.fixture
def provs(monkeypatch):
    own = PlaylistResource(provider="SIMKL", id="list:5", name="Heist", can_add=True, can_remove=True, extra={"endpoint_type": "custom_list", "privacy": "private"})
    shared = PlaylistResource(provider="SIMKL", id="list:8", name="Shared", can_add=True, can_remove=True, extra={"endpoint_type": "collaboration", "privacy": "public"})
    bucket = PlaylistResource(provider="SIMKL", id="plantowatch", name="Plan", can_add=True, can_remove=True, extra={"endpoint_type": "status_bucket", "can_edit_privacy": False})
    followed = PlaylistResource(provider="SIMKL", id="list:9", name="Star Wars", can_add=False, can_remove=False, extra={"endpoint_type": "followed_list", "public_list": True})
    out = {
        "SIMKL": _Ops("SIMKL", {**PRIVACY, "editable": True}, ["movie", "show", "anime"], [own, shared, bucket, followed]),
        "PLAIN": _Ops("PLAIN"),
        "JELLY": _Ops("JELLY", {"values": ["private", "public"], "default": "private", "endpoint_types": ["playlist"]}, ["playlist", "collection"]),
    }
    monkeypatch.setattr(svc, "_providers", lambda: out)
    monkeypatch.setattr(svc, "_save", lambda cfg: None)
    monkeypatch.setattr(svc, "build_provider_config_view", lambda cfg, name, inst: {})
    return out


def test_declared_privacy_defaults_validates_and_respects_endpoint_types(provs):
    assert create_privacy_options(provs["SIMKL"]) == {"values": ["public", "unlisted", "private"], "default": "public", "endpoint_types": [], "editable": True}
    assert create_privacy_options(provs["PLAIN"]) is None
    assert resolve_create_privacy(provs["SIMKL"], None) == "public"
    assert resolve_create_privacy(provs["SIMKL"], "Unlisted") == "unlisted"
    with pytest.raises(ValueError, match="public, unlisted, private"):
        resolve_create_privacy(provs["SIMKL"], "friends")
    assert resolve_create_privacy(provs["JELLY"], "public", "collection") is None
    assert resolve_create_privacy(provs["JELLY"], "public", "playlist") == "public"


def test_service_passes_privacy_only_to_providers_that_declare_it(provs):
    assert svc.create_provider_playlist({}, "SIMKL", "default", "Heist", "movie")["ok"] is True
    assert svc.create_provider_playlist({}, "SIMKL", "default", "Heist", "show", "private")["ok"] is True
    assert [c.get("privacy") for c in provs["SIMKL"].calls] == ["public", "private"]
    bad = svc.create_provider_playlist({}, "SIMKL", "default", "Heist", "movie", "friends")
    assert bad["ok"] is False and "privacy" in bad["error"] and len(provs["SIMKL"].calls) == 2
    assert svc.create_provider_playlist({}, "PLAIN", "default", "Heist", None, "public")["ok"] is True
    assert "privacy" not in provs["PLAIN"].calls[0]
    assert svc.create_provider_playlist({}, "JELLY", "default", "Box", "collection", "public")["ok"] is True
    assert "privacy" not in provs["JELLY"].calls[0]


def test_endpoint_create_and_pending_create_keep_the_choice(provs):
    cfg: dict[str, Any] = {"playlists": {"endpoints": [], "mappings": []}}
    res = svc.upsert_endpoint(cfg, {"name": "Heist", "provider": "SIMKL", "create": True, "create_name": "Heist", "media_type": "movie", "privacy": "unlisted"})
    assert res["ok"] is True and provs["SIMKL"].calls[-1]["privacy"] == "unlisted"
    assert svc._clean_pending_create({"name": "Heist", "media_type": "movie", "privacy": "Private"}) == {"name": "Heist", "media_type": "movie", "privacy": "private"}
    assert svc._clean_pending_create({"name": "Heist"}) == {"name": "Heist", "media_type": "playlist"}
    bad = svc.upsert_endpoint(cfg, {"name": "Later", "provider": "SIMKL", "pending_create": {"name": "Later", "media_type": "movie", "privacy": "friends"}})
    assert bad["ok"] is False and "privacy" in bad["error"]


def test_runner_pending_create_sends_stored_privacy(provs, monkeypatch):
    ops = provs["SIMKL"]
    monkeypatch.setattr(runner, "_ops_cfg", lambda providers, cfg, endpoint: (ops, {}, "default", "SIMKL", None))
    monkeypatch.setattr(runner, "_persist_endpoint_playlist_id", lambda *a, **k: True)
    monkeypatch.setattr(runner, "_apply_new_playlist_id", lambda *a, **k: None)
    item = SimpleNamespace(key="tmdb:1", item={"type": "movie", "ids": {"tmdb": "1"}})
    endpoint = {"endpoint_id": "EP-01", "pending_create": {"name": "Heist", "media_type": "movie", "privacy": "private"}}
    new_id, keys = runner._materialize_pending({}, {}, {}, endpoint, [item], materialize=True)
    assert new_id == "new" and keys == {"tmdb:1"} and ops.calls[-1]["privacy"] == "private"


class _Stop(Exception):
    pass


def _capture(sink: list[Any]):
    def fake(*args, **kwargs):
        sink.append(kwargs.get("json") if "json" in kwargs else kwargs.get("json_body"))
        raise _Stop
    return fake


def _adapter(**client):
    return SimpleNamespace(client=SimpleNamespace(session=None, **client), cfg=SimpleNamespace(timeout=5.0, max_retries=1, user_id="u"), instance_id="default", config={})


@pytest.mark.parametrize("privacy", [None, "public"])
def test_provider_create_bodies_map_privacy(monkeypatch, privacy):
    from providers.sync.flicklist import _playlists as flick
    from providers.sync.mdblist import _playlists as mdb
    from providers.sync.punchplay import _playlists as punch
    from providers.sync.scrob import _playlists as scrob
    from providers.sync.wetrakr import _playlists as wet

    seen: list[Any] = []
    monkeypatch.setattr(flick, "flicklist_request", _capture(seen))
    monkeypatch.setattr(mdb, "mdblist_request", _capture(seen))
    monkeypatch.setattr(mdb, "_apikey", lambda adapter: "k")
    monkeypatch.setattr(punch, "punchplay_request", _capture(seen))
    monkeypatch.setattr(scrob, "scrob_request", _capture(seen))
    monkeypatch.setattr(wet, "request", _capture(seen))
    for mod in (flick, mdb, punch, scrob, wet):
        with pytest.raises(_Stop):
            mod.create(_adapter(), "Heist", privacy=privacy)
    public = privacy == "public"
    assert seen[0]["privacy"] == ("public" if public else "private")
    assert seen[1]["private"] is (not public)
    assert seen[2]["isPublic"] is public
    assert seen[3]["privacy_level"] == ("public" if public else "private")
    assert seen[4]["privacy"] == ("public" if public else "private")


@pytest.mark.parametrize("privacy", [None, "public"])
def test_trakt_tmdb_publicmetadb_and_jellyfin_create_bodies(monkeypatch, privacy):
    from providers.sync.jellyfin import _playlists as jelly
    from providers.sync.publicmetadb import _playlists as pmdb
    from providers.sync.tmdb import _playlists as tmdb
    from providers.sync.trakt import _playlists as trakt

    seen: list[Any] = []
    monkeypatch.setattr(trakt, "_list_count_limit", lambda adapter: None)
    monkeypatch.setattr(trakt, "headers_for_adapter", lambda adapter: {})
    monkeypatch.setattr(trakt, "request_with_retries", _capture(seen))
    with pytest.raises(_Stop):
        trakt.create(_adapter(), "Heist", privacy=privacy)
    with pytest.raises(_Stop):
        tmdb.create(_adapter(v4=_capture(seen)), "Heist", privacy=privacy)
    with pytest.raises(_Stop):
        pmdb.create(_adapter(post_json=_capture(seen)), "Heist", privacy=privacy)
    flags: list[Any] = []

    def jelly_create(client, user_id, name, is_public=False):
        flags.append(is_public)
        raise _Stop

    monkeypatch.setattr(jelly, "create_playlist", jelly_create)
    with pytest.raises(_Stop):
        jelly.create(_adapter(), "Heist", media_type="playlist", privacy=privacy)
    public = privacy == "public"
    assert seen[0]["privacy"] == ("public" if public else "private")
    assert seen[1]["public"] == (1 if public else 0)
    assert seen[2]["is_public"] is public
    assert flags == [public]


def test_only_own_lists_offer_privacy_editing_with_the_current_value(provs):
    rows = {r["id"]: r for r in svc.list_resources({}, "SIMKL", "default")["resources"]}
    assert rows["list:5"]["can_edit_privacy"] is True and rows["list:5"]["privacy"] == "private"
    assert [rows[k]["can_edit_privacy"] for k in ("list:8", "plantowatch", "list:9")] == [False, False, False]
    assert svc._resource_privacy({"private": True}, ["private", "public"]) == "private"
    assert svc._resource_privacy({"private": False}, ["private", "public"]) == "public"
    assert svc._resource_privacy({"privacy": "friends_only"}, ["private", "friends_only", "public"]) == "friends_only"
    assert svc._resource_privacy({}, ["private", "public"]) == ""


def test_privacy_update_is_validated_and_refused_for_read_only_lists(provs):
    ok = svc.update_provider_playlist_privacy({}, "SIMKL", "default", "list:5", "Unlisted")
    assert ok["ok"] is True and ok["resource"]["privacy"] == "unlisted"
    assert provs["SIMKL"].calls[-1] == {"update": "list:5", "privacy": "unlisted"}
    bad = svc.update_provider_playlist_privacy({}, "SIMKL", "default", "list:5", "friends")
    assert bad["ok"] is False and "public, unlisted, private" in bad["error"]
    denied = svc.update_provider_playlist_privacy({}, "SIMKL", "default", "list:8", "private")
    assert denied["ok"] is False and "cannot be changed" in denied["error"]
    assert svc.update_provider_playlist_privacy({}, "PLAIN", "default", "x", "public")["ok"] is False
    assert len([c for c in provs["SIMKL"].calls if "update" in c]) == 1


def _owned_resource(provider, rid="7", **extra):
    return PlaylistResource(provider=provider, id=rid, name="Heist", can_add=True, can_remove=True, extra={"can_rename": True, **extra})


@pytest.mark.parametrize("privacy", ["private", "public"])
def test_provider_privacy_update_requests(monkeypatch, privacy):
    from providers.sync.jellyfin import _playlists as jelly
    from providers.sync.mdblist import _playlists as mdb
    from providers.sync.punchplay import _playlists as punch
    from providers.sync.scrob import _playlists as scrob
    from providers.sync.tmdb import _playlists as tmdb
    from providers.sync.trakt import _playlists as trakt
    from providers.sync.wetrakr import _playlists as wet

    seen: list[Any] = []
    public = privacy == "public"
    monkeypatch.setattr(trakt, "headers_for_adapter", lambda adapter: {})
    monkeypatch.setattr(trakt, "request_with_retries", _capture(seen))
    with pytest.raises(_Stop):
        trakt.set_privacy(_adapter(), "123", privacy)
    monkeypatch.setattr(tmdb, "_find_resource", lambda adapter, lid: _owned_resource("TMDB"))
    with pytest.raises(_Stop):
        tmdb.set_privacy(_adapter(v4=_capture(seen)), "7", privacy)
    monkeypatch.setattr(wet, "_owned", lambda adapter, pid: _owned_resource("WETRAKR"))
    monkeypatch.setattr(wet, "request", _capture(seen))
    with pytest.raises(_Stop):
        wet.set_privacy(_adapter(), "7", privacy)
    monkeypatch.setattr(scrob, "_owned", lambda adapter, pid, field: _owned_resource("SCROB"))
    monkeypatch.setattr(scrob, "scrob_request", _capture(seen))
    with pytest.raises(_Stop):
        scrob.set_privacy(_adapter(), "7", privacy)
    monkeypatch.setattr(punch, "_owned", lambda adapter, pid, op: _owned_resource("PUNCHPLAY"))
    monkeypatch.setattr(punch, "punchplay_request", _capture(seen))
    with pytest.raises(_Stop):
        punch.set_privacy(_adapter(), "7", privacy)
    jelly_res = _owned_resource("JELLYFIN", endpoint_type="playlist")
    monkeypatch.setattr(jelly, "_resolved_resource", lambda adapter, pid: ("playlist", "abc", jelly_res))
    with pytest.raises(_Stop):
        jelly.set_privacy(_adapter(post=_capture(seen)), "7", privacy)
    assert seen[0] == {"privacy": privacy}
    assert seen[1] == {"public": public}
    assert seen[2] == {"privacy": privacy}
    assert seen[3] == {"privacy_level": privacy}
    assert seen[4] == {"isPublic": public}
    assert seen[5] == {"IsPublic": public}

    meta: list[Any] = []
    mdb_res = SimpleNamespace(id="7", name="Heist", instance="default", kind="regular", can_read=True, can_add=True, can_remove=True,
                              can_reorder=False, media_types=("movie",), extra={}, is_smart=False)
    monkeypatch.setattr(mdb, "_find_owned_resource", lambda adapter, pid: mdb_res)
    monkeypatch.setattr(mdb, "update_metadata", lambda adapter, lid, **kw: meta.append(kw))
    assert mdb.set_privacy(_adapter(), "7", privacy).extra["private"] is (not public)
    assert meta == [{"private": not public}]


def test_jellyfin_collections_have_no_privacy(monkeypatch):
    from providers.sync.jellyfin import _playlists as jelly

    monkeypatch.setattr(jelly, "_resolved_resource", lambda adapter, pid: ("collection", "abc", _owned_resource("JELLYFIN")))
    with pytest.raises(jelly.JellyfinPlaylistError):
        jelly.set_privacy(_adapter(post=_capture([])), "7", "public")

from __future__ import annotations

from typing import Any

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.playlists import PlaylistItem, PlaylistResource, PlaylistSnapshot
from cw_platform import playlists_runner as R
import services.playlists as svc


def _movie(n: int) -> dict[str, Any]:
    return {"type": "movie", "title": f"M{n}", "year": 2020, "ids": {"tmdb": str(n)}}


class FakeOps:
    can_create_empty = False

    def __init__(self, name: str, playlists: dict[str, dict[str, Any]]):
        self._name = name
        self.pl = playlists
        self.created: list[dict[str, Any]] = []
        self.calls: list[Any] = []
        self._next_id = 500

    def capabilities(self) -> dict[str, Any]:
        return {"playlists": {"create": True, "create_empty": bool(self.can_create_empty)}}

    def _resource(self, pid: str) -> PlaylistResource:
        d = self.pl[pid]
        return PlaylistResource(
            provider=self._name,
            id=pid,
            name=d.get("name") or pid,
            can_add=True,
            can_remove=True,
            can_reorder=True,
        )

    def list_playlist_resources(self, cfg, *, instance=None):
        return [self._resource(pid) for pid in self.pl]

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        d = self.pl[playlist_id]
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(d["items"])]
        return PlaylistSnapshot(resource=self._resource(playlist_id), items=items)

    def create_playlist(self, cfg, name, *, media_type=None, items=None, instance=None, dry_run=False):
        seed = [dict(x) for x in (items or [])]
        if not self.can_create_empty and not seed:
            raise RuntimeError("cannot create an empty playlist")
        self._next_id += 1
        pid = str(self._next_id)
        self.pl[pid] = {"name": name, "items": seed}
        self.created.append({"id": pid, "name": name, "seed": [canonical_key(x) for x in seed]})
        return self._resource(pid)

    def add_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("add", playlist_id, [canonical_key(x) for x in items]))
        d = self.pl[playlist_id]
        existing = {canonical_key(m) for m in d["items"]}
        conf = []
        for it in items:
            k = canonical_key(it)
            if k not in existing:
                d["items"].append(dict(it))
                existing.add(k)
                conf.append(k)
        return {"ok": True, "count": len(conf), "unresolved": [], "confirmed_keys": conf}

    def remove_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("remove", playlist_id, [canonical_key(x) for x in items]))
        d = self.pl[playlist_id]
        want = {canonical_key(x) for x in items}
        conf = [canonical_key(m) for m in d["items"] if canonical_key(m) in want]
        d["items"] = [m for m in d["items"] if canonical_key(m) not in want]
        return {"ok": True, "count": len(conf), "unresolved": [], "confirmed_keys": conf}

    def reorder_playlist_items(self, cfg, playlist_id, ordered_keys, *, instance=None, dry_run=False):
        return {"ok": True, "reordered": 0, "count": 0}

    def is_configured(self, cfg) -> bool:
        return True


def _cfg(src: dict[str, Any]) -> dict[str, Any]:
    return {
        "trakt": {},
        "plex": {},
        "runtime": {},
        "playlists": {
            "endpoints": [
                {"id": "EP-01", "name": "Src", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"},
                {
                    "id": "EP-02",
                    "name": "Dst",
                    "provider": "PLEX",
                    "instance": "default",
                    "playlist_id": "",
                    "playlist_name": "Weekend",
                    "pending_create": {"name": "Weekend", "media_type": "playlist"},
                },
            ],
            "mappings": [
                {
                    "id": "MAP-01",
                    "name": "Map1",
                    "source_endpoint": "EP-01",
                    "target_endpoints": ["EP-02"],
                    "membership": "managed_only",
                    "order": "ignore",
                    "enabled": True,
                }
            ],
            "rulesets": [],
        },
    }


@pytest.fixture
def world(config_base, monkeypatch):
    from cw_platform import config_base as cb

    def _update_config(mutator):
        cfg = getattr(_update_config, "cfg", None)
        return cfg, (mutator(cfg) if isinstance(cfg, dict) else None)

    monkeypatch.setattr(cb, "update_config", _update_config)
    monkeypatch.setattr(svc, "_save", lambda _cfg: None)

    src = {"L1": {"name": "src", "items": [_movie(1), _movie(2), _movie(3)]}}
    dst: dict[str, dict[str, Any]] = {}
    provs = {"TRAKT": FakeOps("TRAKT", src), "PLEX": FakeOps("PLEX", dst)}
    cfg = _cfg(src)
    _update_config.cfg = cfg
    return cfg, src, dst, provs


def test_pending_target_is_created_on_the_first_run(world) -> None:
    cfg, src, dst, provs = world
    mapping = R.resolve_mapping_by_id(cfg, "MAP-01")

    res = R.run_mapping(cfg, mapping, providers=provs)

    assert res["ok"] and res.get("created") is True
    created = provs["PLEX"].created
    assert len(created) == 1 and created[0]["name"] == "Weekend"
    assert {canonical_key(m) for m in dst[created[0]["id"]]["items"]} == {"tmdb:1", "tmdb:2", "tmdb:3"}


def test_the_new_playlist_id_is_written_back_to_the_endpoint(world) -> None:
    cfg, src, dst, provs = world
    mapping = R.resolve_mapping_by_id(cfg, "MAP-01")

    R.run_mapping(cfg, mapping, providers=provs)

    ep = R.get_endpoint(cfg, "EP-02")
    assert ep["playlist_id"] == provs["PLEX"].created[0]["id"]
    assert "pending_create" not in ep, "a materialized endpoint must not stay pending"


def test_the_second_run_reuses_the_playlist_instead_of_creating_another(world) -> None:
    cfg, src, dst, provs = world

    R.run_mapping(cfg, R.resolve_mapping_by_id(cfg, "MAP-01"), providers=provs)
    R.run_mapping(cfg, R.resolve_mapping_by_id(cfg, "MAP-01"), providers=provs)

    assert len(provs["PLEX"].created) == 1


def test_seeded_items_are_managed_so_a_later_source_removal_propagates(world) -> None:
    cfg, src, dst, provs = world

    R.run_mapping(cfg, R.resolve_mapping_by_id(cfg, "MAP-01"), providers=provs)
    pid = provs["PLEX"].created[0]["id"]

    src["L1"]["items"] = [_movie(1)]
    res = R.run_mapping(cfg, R.resolve_mapping_by_id(cfg, "MAP-01"), providers=provs)

    assert res["removed"] == 2, "items created as the seed must land in the managed baseline"
    assert {canonical_key(m) for m in dst[pid]["items"]} == {"tmdb:1"}


def test_a_dry_run_never_creates_the_playlist(world) -> None:
    cfg, src, dst, provs = world
    mapping = R.resolve_mapping_by_id(cfg, "MAP-01")

    with pytest.raises(R.PlaylistRunError, match="first run"):
        R.run_mapping(cfg, mapping, dry_run=True, providers=provs)

    assert provs["PLEX"].created == []


def test_an_empty_source_cannot_create_the_target(world) -> None:
    cfg, src, dst, provs = world
    src["L1"]["items"] = []

    with pytest.raises(R.PlaylistRunError, match="empty source"):
        R.run_mapping(cfg, R.resolve_mapping_by_id(cfg, "MAP-01"), providers=provs)

    assert provs["PLEX"].created == []


def test_service_parks_the_endpoint_when_the_provider_cannot_create_an_empty_list(world, monkeypatch) -> None:
    cfg, _src, _dst, provs = world
    monkeypatch.setattr(svc, "_providers", lambda: provs)

    res = svc.upsert_endpoint(
        cfg,
        {"name": "Later", "provider": "PLEX", "instance": "default", "create": True, "create_name": "Later"},
    )

    assert res["ok"] is True
    assert provs["PLEX"].created == [], "creation must be deferred, not attempted with no items"
    assert res["endpoint"]["pending_create"] == {"name": "Later", "media_type": "playlist"}


def test_service_still_creates_immediately_when_the_provider_supports_it(world, monkeypatch) -> None:
    cfg, _src, _dst, provs = world
    provs["PLEX"].can_create_empty = True
    monkeypatch.setattr(svc, "_providers", lambda: provs)

    res = svc.upsert_endpoint(
        cfg,
        {"name": "Now", "provider": "PLEX", "instance": "default", "create": True, "create_name": "Now"},
    )

    assert res["ok"] is True
    assert len(provs["PLEX"].created) == 1
    assert res["endpoint"]["playlist_id"] == provs["PLEX"].created[0]["id"]
    assert "pending_create" not in res["endpoint"]


class CollectionOps(FakeOps):
    can_create_empty = True

    def __init__(self, name: str, playlists: dict[str, Any], endpoint_types: list[str]):
        super().__init__(name, playlists)
        self._endpoint_types = endpoint_types

    def capabilities(self) -> dict[str, Any]:
        return {
            "playlists": {
                "create": True,
                "create_empty": True,
                "endpoint_types": ["playlist", "collection"],
                "create_endpoint_types": self._endpoint_types,
            }
        }

    def _resource(self, pid: str) -> PlaylistResource:
        res = super()._resource(pid)
        res.extra = {"endpoint_type": self.pl[pid].get("endpoint_type") or "playlist"}
        return res

    def create_playlist(self, cfg, name, *, media_type=None, items=None, instance=None, dry_run=False):
        res = super().create_playlist(cfg, name, media_type=media_type, items=items, instance=instance, dry_run=dry_run)
        pid = self.created[-1]["id"]
        self.pl[pid]["endpoint_type"] = "collection" if media_type == "collection" else "playlist"
        self.created[-1]["media_type"] = media_type
        return self._resource(pid)


def _collection_world(monkeypatch, endpoint_types):
    provs = {"JELLYFIN": CollectionOps("JELLYFIN", {}, endpoint_types)}
    monkeypatch.setattr(svc, "_providers", lambda: provs)
    monkeypatch.setattr(svc, "_save", lambda _cfg: None)
    return provs


def test_a_requested_collection_reaches_the_provider_as_a_collection(config_base, monkeypatch) -> None:
    provs = _collection_world(monkeypatch, ["playlist", "collection"])

    res = svc.upsert_endpoint(
        {},
        {
            "name": "Marvel",
            "provider": "JELLYFIN",
            "instance": "default",
            "create": True,
            "create_name": "Marvel",
            "media_type": "collection",
        },
    )

    assert res["ok"] is True
    assert provs["JELLYFIN"].created[0]["media_type"] == "collection"
    assert res["endpoint"]["playlist_type"] == "collection"


def test_the_default_create_type_is_a_playlist_not_a_media_type(config_base, monkeypatch) -> None:
    provs = _collection_world(monkeypatch, ["playlist", "collection"])

    svc.upsert_endpoint(
        {},
        {"name": "Weekend", "provider": "JELLYFIN", "instance": "default", "create": True, "create_name": "Weekend"},
    )

    assert provs["JELLYFIN"].created[0]["media_type"] == "playlist"


def test_a_type_the_provider_cannot_create_is_refused(config_base, monkeypatch) -> None:
    provs = _collection_world(monkeypatch, ["playlist"])

    res = svc.upsert_endpoint(
        {},
        {
            "name": "Marvel",
            "provider": "JELLYFIN",
            "instance": "default",
            "create": True,
            "create_name": "Marvel",
            "media_type": "collection",
        },
    )

    assert res["ok"] is False and "can only create a playlist" in res["error"]
    assert provs["JELLYFIN"].created == []

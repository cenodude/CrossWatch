from __future__ import annotations

from typing import Any

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.playlists import PlaylistItem, PlaylistResource, PlaylistSnapshot
from cw_platform import playlists_runner as runner
from services import playlists as svc


class FakeOps:
    def __init__(self, name: str, playlists: dict[str, dict[str, Any]], reorder: bool = True):
        self._name = name
        self.pl = playlists
        self.reorder = reorder
        self.calls: list[Any] = []

    def name(self) -> str:
        return self._name

    def label(self) -> str:
        return self._name.title()

    def capabilities(self) -> dict[str, Any]:
        return {"playlists": {"reorder": self.reorder}}

    def is_configured(self, cfg) -> bool:
        return True

    def _res(self, pid: str) -> PlaylistResource:
        return PlaylistResource(provider=self._name, id=pid, name=self.pl[pid].get("name") or pid,
                                can_add=True, can_remove=True, can_reorder=self.reorder)

    def list_playlist_resources(self, cfg, *, instance=None):
        return [self._res(p) for p in self.pl]

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        d = self.pl[playlist_id]
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(d["items"])]
        return PlaylistSnapshot(resource=self._res(playlist_id), items=items)

    def create_playlist(self, cfg, name, *, media_type=None, items=None, instance=None, dry_run=False):
        return PlaylistResource(provider=self._name, id="new", name=name)

    def add_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("add", playlist_id))
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
        self.calls.append(("remove", playlist_id))
        d = self.pl[playlist_id]
        want = {canonical_key(x) for x in items}
        conf = [canonical_key(m) for m in d["items"] if canonical_key(m) in want]
        d["items"] = [m for m in d["items"] if canonical_key(m) not in want]
        return {"ok": True, "count": len(conf), "unresolved": [], "confirmed_keys": conf}

    def reorder_playlist_items(self, cfg, playlist_id, ordered_keys, *, instance=None, dry_run=False):
        self.calls.append(("reorder", playlist_id))
        return {"ok": True, "reordered": len(ordered_keys), "count": len(ordered_keys)}


def _movie(n: int) -> dict[str, Any]:
    return {"type": "movie", "title": f"M{n}", "ids": {"tmdb": str(n)}}


@pytest.fixture
def fake_providers(monkeypatch):
    src = {"L1": {"name": "src1", "items": [_movie(1), _movie(2)]}, "L2": {"name": "src2", "items": [_movie(9)]}}
    dst = {"T1": {"name": "dst1", "items": []}, "T2": {"name": "dst2", "items": []}}
    provs = {"TRAKT": FakeOps("TRAKT", src), "PLEX": FakeOps("PLEX", dst)}
    monkeypatch.setattr(svc, "_providers", lambda: provs)
    monkeypatch.setattr(runner, "_providers", lambda: provs)
    monkeypatch.setattr(svc, "_save", lambda cfg: None)
    return provs


def _cfg():
    return {
        "trakt": {"client_id": "x", "access_token": "y"},
        "plex": {"account_token": "z"},
        "runtime": {},
        "playlists": {"endpoints": [], "mappings": []},
        "pairs": [
            {"id": "p1", "source": "TRAKT", "target": "PLEX", "source_instance": "default",
             "target_instance": "default", "mode": "one-way", "enabled": True,
             "features": {"playlists": {"enable": True, "mappings": []}}},
        ],
    }


def _seed_endpoints(cfg):
    e1 = svc.upsert_endpoint(cfg, {"name": "Trakt src", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"})
    e2 = svc.upsert_endpoint(cfg, {"name": "Plex dst", "provider": "PLEX", "instance": "default", "playlist_id": "T1"})
    return e1["endpoint"]["id"], e2["endpoint"]["id"]


def test_endpoints_and_mapping_ids(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    assert e1 == "EP-01" and e2 == "EP-02"
    m = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})
    assert m["ok"] and m["created"] and m["mapping"]["id"] == "MAP-01"
    assert m["pair_id"].startswith("pair_playlist_")
    pair = next(p for p in cfg["pairs"] if p["id"] == m["pair_id"])
    assert pair["source"] == "TRAKT"
    assert pair["target"] == "PLEX"
    assert pair["features"]["playlists"]["mappings"] == ["MAP-01"]
    assert pair["features"]["playlists"]["managed_by"] == "playlists"
    maps = svc.list_mappings(cfg)
    assert len(maps) == 1
    assert maps[0]["source"]["provider"] == "TRAKT" and maps[0]["target"]["provider"] == "PLEX"
    assert maps[0]["assigned_pair"] == m["pair_id"]


def test_endpoint_name_is_required_and_short(config_base, fake_providers):
    cfg = _cfg()
    missing = svc.upsert_endpoint(cfg, {"name": "", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"})
    assert missing["ok"] is False and "required" in missing["error"]
    long = svc.upsert_endpoint(cfg, {"name": "VeryLongName", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"})
    assert long["ok"] is False and "10 characters" in long["error"]
    bad = svc.upsert_endpoint(cfg, {"name": "Bad/Name", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"})
    assert bad["ok"] is False and "unsupported" in bad["error"]
    bad_start = svc.upsert_endpoint(cfg, {"name": "$Bad", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"})
    assert bad_start["ok"] is False and "letter or number" in bad_start["error"]


def test_created_playlist_name_is_limited_and_safe(config_base, fake_providers):
    cfg = _cfg()
    long = svc.upsert_endpoint(cfg, {"name": "NewList", "provider": "TRAKT", "instance": "default", "create": True, "create_name": "This Playlist Name Is Too Long"})
    assert long["ok"] is False and "20 characters" in long["error"]
    bad = svc.upsert_endpoint(cfg, {"name": "NewList", "provider": "TRAKT", "instance": "default", "create": True, "create_name": "Bad/List"})
    assert bad["ok"] is False and "unsupported" in bad["error"]
    ok = svc.upsert_endpoint(cfg, {"name": "NewList", "provider": "TRAKT", "instance": "default", "create": True, "create_name": "Weekend Movies"})
    assert ok["ok"] is True


def test_mapping_validation(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    ok, _ = svc.validate_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})
    assert ok
    bad, why = svc.validate_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e1]})
    assert not bad and "differ" in why
    bad2, why2 = svc.validate_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": ["EP-99"]})
    assert not bad2
    bad3, why3 = svc.validate_mapping(cfg, {"name": "", "source_endpoint": e1, "target_endpoints": [e2]})
    assert not bad3 and "required" in why3
    bad4, why4 = svc.validate_mapping(cfg, {"name": "LongMapName", "source_endpoint": e1, "target_endpoints": [e2]})
    assert not bad4 and "10 characters" in why4
    bad5, why5 = svc.validate_mapping(cfg, {"name": "Bad/Map", "source_endpoint": e1, "target_endpoints": [e2]})
    assert not bad5 and "unsupported" in why5


def test_preserve_order_rejected_when_target_not_reorderable(config_base, monkeypatch):
    src = {"L1": {"name": "s", "items": [_movie(1)]}}
    dst = {"T1": {"name": "d", "items": []}}
    provs = {"TRAKT": FakeOps("TRAKT", src), "PLEX": FakeOps("PLEX", dst, reorder=False)}
    monkeypatch.setattr(svc, "_providers", lambda: provs)
    monkeypatch.setattr(runner, "_providers", lambda: provs)
    monkeypatch.setattr(svc, "_save", lambda c: None)
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    res = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2], "order": "preserve"})
    assert res["ok"] is False and "ordering" in res["error"]
    assert svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2], "order": "ignore"})["ok"]


def test_preserve_order_uses_endpoint_reorderability(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    for ep in cfg["playlists"]["endpoints"]:
        if ep["id"] == e2:
            ep["can_reorder"] = False

    res = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2], "order": "preserve"})
    assert res["ok"] is False and "ordering" in res["error"]


def test_preview_performs_no_writes(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    mid = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})["mapping"]["id"]
    prev = svc.preview_mapping(cfg, mid)
    assert prev["ok"] and prev["preview"]["planned_additions"] == 2
    assert fake_providers["PLEX"].calls == []


def test_run_only_selected_mapping(config_base, fake_providers):
    cfg = _cfg()
    e1 = svc.upsert_endpoint(cfg, {"name": "s1", "provider": "TRAKT", "instance": "default", "playlist_id": "L1"})["endpoint"]["id"]
    e2 = svc.upsert_endpoint(cfg, {"name": "d1", "provider": "PLEX", "instance": "default", "playlist_id": "T1"})["endpoint"]["id"]
    e3 = svc.upsert_endpoint(cfg, {"name": "s2", "provider": "TRAKT", "instance": "default", "playlist_id": "L2"})["endpoint"]["id"]
    e4 = svc.upsert_endpoint(cfg, {"name": "d2", "provider": "PLEX", "instance": "default", "playlist_id": "T2"})["endpoint"]["id"]
    m1 = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})["mapping"]["id"]
    svc.upsert_mapping(cfg, {"name": "Map2", "source_endpoint": e3, "target_endpoints": [e4]})

    out = svc.run_mapping(cfg, m1)
    assert out["ok"] and out["result"]["added"] == 2
    assert ("add", "T1") in fake_providers["PLEX"].calls
    assert ("add", "T2") not in fake_providers["PLEX"].calls


def test_provider_count_summary_merges_endpoint_and_mapping_counts(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    mid = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})["mapping"]["id"]
    before = svc.provider_count_summary(cfg)
    assert before["providers"]["trakt"] == 2
    assert before["providers"].get("plex", 0) == 0

    out = svc.run_mapping(cfg, mid)
    assert out["ok"] and out["result"]["added"] == 2
    after = svc.provider_count_summary(cfg)
    assert after["providers"]["trakt"] == 2
    assert after["providers"]["plex"] == 2
    assert after["providers_instances"]["plex"]["default"] == 2


def test_mappings_for_pair_compat_and_one_pair(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    r1 = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})
    m1 = r1["mapping"]["id"]
    m2 = svc.upsert_mapping(cfg, {"name": "Map2", "source_endpoint": e1, "target_endpoints": [e2]})["mapping"]["id"]

    res = svc.mappings_for_pair(cfg, "p1")
    assert {m["id"] for m in res["mappings"]} == set()
    generated = svc.mappings_for_pair(cfg, r1["pair_id"])
    assert {m["id"] for m in generated["mappings"]} == {m1}

    cfg["pairs"].append({"id": "p2", "source": "TRAKT", "target": "PLEX", "source_instance": "default",
                         "target_instance": "default", "mode": "one-way", "enabled": True,
                         "features": {"playlists": {"enable": True, "mappings": [m2]}}})
    res2 = svc.mappings_for_pair(cfg, "p1")
    assert {m["id"] for m in res2["mappings"]} == set()


def test_mapping_delete_removes_generated_pair(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    res = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})
    mid = res["mapping"]["id"]
    pair_id = res["pair_id"]
    assert any(p["id"] == pair_id for p in cfg["pairs"])

    deleted = svc.delete_mapping(cfg, mid)
    assert deleted["ok"]
    assert not any(p.get("id") == pair_id for p in cfg["pairs"])


def test_list_mappings_repairs_orphan_mapping_pair(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    cfg["playlists"]["mappings"].append({
        "id": "MAP-99",
        "name": "Legacy orphan",
        "source_endpoint": e1,
        "target_endpoints": [e2],
        "ruleset_id": "",
        "membership": "managed_only",
        "order": "ignore",
        "enabled": True,
    })

    maps = svc.list_mappings(cfg)
    repaired = next(m for m in maps if m["id"] == "MAP-99")
    assert repaired["assigned_pair"].startswith("pair_playlist_")
    pair = next(p for p in cfg["pairs"] if p["id"] == repaired["assigned_pair"])
    assert pair["features"]["playlists"]["mappings"] == ["MAP-99"]


def test_endpoint_edit_refreshes_generated_pair(config_base, fake_providers):
    cfg = _cfg()
    e1, e2 = _seed_endpoints(cfg)
    res = svc.upsert_mapping(cfg, {"name": "Map1", "source_endpoint": e1, "target_endpoints": [e2]})
    pair_id = res["pair_id"]

    upd = svc.upsert_endpoint(cfg, {"id": e2, "name": "Plex dst", "provider": "PLEX", "instance": "second", "playlist_id": "T1"})
    assert upd["ok"]
    pair = next(p for p in cfg["pairs"] if p["id"] == pair_id)
    assert pair["target_instance"] == "second"

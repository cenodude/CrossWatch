from __future__ import annotations

from typing import Any

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.playlists import PlaylistItem, PlaylistResource, PlaylistSnapshot
from cw_platform import playlists_runner as R


class FakeOps:
    def __init__(self, name: str, playlists: dict[str, dict[str, Any]]):
        self._name = name
        self.pl = playlists
        self.calls: list[Any] = []

    def _resource(self, pid: str) -> PlaylistResource:
        d = self.pl[pid]
        smart = bool(d.get("smart"))
        return PlaylistResource(
            provider=self._name,
            id=pid,
            name=d.get("name") or pid,
            kind="smart" if smart else "regular",
            can_add=not smart,
            can_remove=not smart,
            can_reorder=bool(d.get("can_reorder", True)) and not smart,
        )

    def list_playlist_resources(self, cfg, *, instance=None):
        return [self._resource(pid) for pid in self.pl]

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        d = self.pl[playlist_id]
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(d["items"])]
        return PlaylistSnapshot(resource=self._resource(playlist_id), items=items)

    def create_playlist(self, *a, **k):
        return PlaylistResource(provider=self._name, id="new", name="new")

    def add_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("add", playlist_id, [canonical_key(x) for x in items]))
        d = self.pl[playlist_id]
        existing = {canonical_key(m) for m in d["items"]}
        added, conf = 0, []
        for it in items:
            k = canonical_key(it)
            if k not in existing:
                d["items"].append(dict(it))
                existing.add(k)
                added += 1
                conf.append(k)
        return {"ok": True, "count": added, "unresolved": [], "confirmed_keys": conf}

    def remove_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("remove", playlist_id, [canonical_key(x) for x in items]))
        d = self.pl[playlist_id]
        want = {canonical_key(x) for x in items}
        before = len(d["items"])
        conf = [canonical_key(m) for m in d["items"] if canonical_key(m) in want]
        d["items"] = [m for m in d["items"] if canonical_key(m) not in want]
        return {"ok": True, "count": before - len(d["items"]), "unresolved": [], "confirmed_keys": conf}

    def reorder_playlist_items(self, cfg, playlist_id, ordered_keys, *, instance=None, dry_run=False):
        self.calls.append(("reorder", playlist_id, list(ordered_keys)))
        d = self.pl[playlist_id]
        order = {k: i for i, k in enumerate(ordered_keys)}
        d["items"].sort(key=lambda m: order.get(canonical_key(m), 10**9))
        return {"ok": True, "reordered": len(ordered_keys), "count": len(ordered_keys)}


def _movie(n: int) -> dict[str, Any]:
    return {"type": "movie", "title": f"M{n}", "year": 2020, "ids": {"tmdb": str(n)}}


def _movie_ids(n: int, ids: dict[str, Any]) -> dict[str, Any]:
    return {"type": "movie", "title": f"M{n}", "year": 2020, "ids": ids}


def _cfg():
    return {"trakt": {}, "plex": {}, "runtime": {}}


def _mapping(mid="MAP-01", src_pl="L1", dst_pl="T1", membership="managed_only", order="ignore"):
    return {
        "id": mid,
        "source": {"provider": "TRAKT", "instance": "default", "playlist_id": src_pl},
        "target": {"provider": "PLEX", "instance": "default", "playlist_id": dst_pl},
        "targets": [{"provider": "PLEX", "instance": "default", "playlist_id": dst_pl}],
        "membership": membership,
        "order": order,
        "enabled": True,
    }


def _providers(src_pl, dst_pl):
    return {"TRAKT": FakeOps("TRAKT", src_pl), "PLEX": FakeOps("PLEX", dst_pl)}


def test_managed_only_preserves_manual_items(config_base):
    src = {"L1": {"name": "src", "items": [_movie(1), _movie(2)]}}
    dst = {"T1": {"name": "dst", "items": [_movie(1), _movie(3)]}}
    provs = _providers(src, dst)

    r1 = R.run_mapping(_cfg(), _mapping(), providers=provs)
    assert r1["added"] == 1
    assert {canonical_key(m) for m in dst["T1"]["items"]} == {"tmdb:1", "tmdb:2", "tmdb:3"}

    src["L1"]["items"] = [_movie(1)]
    r2 = R.run_mapping(_cfg(), _mapping(), providers=provs)
    assert r2["removed"] == 1
    assert {canonical_key(m) for m in dst["T1"]["items"]} == {"tmdb:1", "tmdb:3"}


def test_direct_mapping_matches_items_by_any_shared_external_id(config_base):
    src = {"L1": {"name": "src", "items": [_movie_ids(1, {"tmdb": "1", "imdb": "tt0000001"})]}}
    dst = {"T1": {"name": "dst", "items": [_movie_ids(1, {"imdb": "tt0000001"})]}}
    provs = _providers(src, dst)

    res = R.run_mapping(_cfg(), _mapping(), providers=provs)

    assert res["added"] == 0
    assert provs["PLEX"].calls == []


def test_mirror_removes_extras(config_base):
    src = {"L1": {"name": "src", "items": [_movie(1), _movie(2)]}}
    dst = {"T1": {"name": "dst", "items": [_movie(1), _movie(3)]}}
    provs = _providers(src, dst)
    res = R.run_mapping(_cfg(), _mapping(membership="mirror"), providers=provs)
    assert res["added"] == 1 and res["removed"] == 1
    assert res["manual_affected"] is True
    assert {canonical_key(m) for m in dst["T1"]["items"]} == {"tmdb:1", "tmdb:2"}


def test_multiple_mappings_isolated(config_base):
    src = {"L1": {"name": "s1", "items": [_movie(1)]}, "L2": {"name": "s2", "items": [_movie(9)]}}
    dst = {"T1": {"name": "d1", "items": []}, "T2": {"name": "d2", "items": []}}
    provs = _providers(src, dst)

    R.run_mapping(_cfg(), _mapping("MAP-01", "L1", "T1"), providers=provs)
    R.run_mapping(_cfg(), _mapping("MAP-02", "L2", "T2"), providers=provs)

    assert R.load_baseline(_mapping("MAP-01", "L1", "T1")) == {"tmdb:1"}
    assert R.load_baseline(_mapping("MAP-02", "L2", "T2")) == {"tmdb:9"}


def test_dry_run_no_mutation(config_base):
    src = {"L1": {"name": "src", "items": [_movie(1), _movie(2)]}}
    dst = {"T1": {"name": "dst", "items": [_movie(1)]}}
    provs = _providers(src, dst)
    res = R.run_mapping(_cfg(), _mapping(), providers=provs, dry_run=True)
    assert res["dry_run"] is True and res["planned_additions"] == 1
    assert provs["PLEX"].calls == []
    assert len(dst["T1"]["items"]) == 1


def test_ordering_idempotent(config_base):
    src = {"L1": {"name": "src", "items": [_movie(2), _movie(1)]}}
    dst = {"T1": {"name": "dst", "items": [_movie(1), _movie(2)]}}
    provs = _providers(src, dst)
    m = _mapping(order="preserve")
    r1 = R.run_mapping(_cfg(), m, providers=provs)
    assert r1["reordered"] > 0
    assert [canonical_key(x) for x in dst["T1"]["items"]] == ["tmdb:2", "tmdb:1"]
    r2 = R.run_mapping(_cfg(), m, providers=provs)
    assert r2["reordered"] == 0


def test_smart_target_fails_before_writes(config_base):
    src = {"L1": {"name": "src", "items": [_movie(1)]}}
    dst = {"T1": {"name": "dst", "items": [], "smart": True}}
    provs = _providers(src, dst)
    with pytest.raises(R.PlaylistRunError):
        R.run_mapping(_cfg(), _mapping(), providers=provs)
    assert provs["PLEX"].calls == []


def test_missing_target_playlist_fails(config_base):
    src = {"L1": {"name": "src", "items": [_movie(1)]}}
    dst = {"T1": {"name": "dst", "items": []}}
    provs = _providers(src, dst)
    with pytest.raises(R.PlaylistRunError):
        R.run_mapping(_cfg(), _mapping(dst_pl="NOPE"), providers=provs)


def test_scope_key_isolates_by_mapping_and_endpoints():
    a = R.scope_key(_mapping("MAP-01", "L1", "T1"))
    b = R.scope_key(_mapping("MAP-01", "L1", "T2"))
    c = R.scope_key(_mapping("MAP-02", "L1", "T1"))
    assert a != b and a != c

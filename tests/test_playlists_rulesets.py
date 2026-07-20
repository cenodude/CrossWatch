from __future__ import annotations

from typing import Any

from cw_platform.id_map import canonical_key
from cw_platform.playlists import BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID, BUILTIN_RULESETS
from cw_platform.playlists import PlaylistItem, PlaylistResource, PlaylistSnapshot
from cw_platform import playlists_runner as R
from services import playlists as svc


class FakeOps:
    def __init__(self, name: str, playlists: dict[str, list[dict[str, Any]]]):
        self.name = name
        self.playlists = playlists
        self.calls: list[tuple[str, str, list[str]]] = []

    def list_playlist_resources(self, cfg, *, instance=None):
        return [
            PlaylistResource(provider=self.name, id=pid, name=pid, can_read=True, can_add=True, can_remove=True, can_reorder=False)
            for pid in self.playlists
        ]

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        items = [PlaylistItem.from_media(m, position=i) for i, m in enumerate(self.playlists[str(playlist_id)])]
        res = PlaylistResource(provider=self.name, id=str(playlist_id), name=str(playlist_id), can_read=True, can_add=True, can_remove=True)
        return PlaylistSnapshot(resource=res, items=items)

    def create_playlist(self, *a, **k):
        return PlaylistResource(provider=self.name, id="new", name="new")

    def add_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        keys = [canonical_key(x) for x in items]
        self.calls.append(("add", str(playlist_id), keys))
        have = {canonical_key(x) for x in self.playlists[str(playlist_id)]}
        confirmed: list[str] = []
        for it in items:
            k = canonical_key(it)
            if k not in have:
                self.playlists[str(playlist_id)].append(dict(it))
                have.add(k)
            confirmed.append(k)
        return {"ok": True, "count": len(confirmed), "unresolved": [], "confirmed_keys": confirmed}

    def remove_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        keys = [canonical_key(x) for x in items]
        self.calls.append(("remove", str(playlist_id), keys))
        want = set(keys)
        before = len(self.playlists[str(playlist_id)])
        self.playlists[str(playlist_id)] = [x for x in self.playlists[str(playlist_id)] if canonical_key(x) not in want]
        return {"ok": True, "count": before - len(self.playlists[str(playlist_id)]), "unresolved": [], "confirmed_keys": keys}

    def reorder_playlist_items(self, *a, **k):
        return {"ok": True, "count": 0, "reordered": 0}


def movie(n: int) -> dict[str, Any]:
    return {"type": "movie", "title": f"M{n}", "ids": {"tmdb": str(n)}}


def cfg(rule: dict[str, Any] | None = None) -> dict[str, Any]:
    root = {
        "plex": {},
        "trakt": {},
        "runtime": {},
        "playlists": {
            "endpoints": [
                {"id": "EP-S", "name": "source", "provider": "PLEX", "instance": "default", "playlist_id": "S"},
                {"id": "EP-1", "name": "target1", "provider": "TRAKT", "instance": "default", "playlist_id": "T1"},
                {"id": "EP-2", "name": "target2", "provider": "TRAKT", "instance": "default", "playlist_id": "T2"},
                {"id": "EP-3", "name": "target3", "provider": "TRAKT", "instance": "default", "playlist_id": "T3"},
            ],
            "mappings": [],
            "rulesets": [],
        },
    }
    if rule:
        root["playlists"]["rulesets"].append(rule)
    return root


def mapping(rule_id: str = BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID, targets: list[str] | None = None) -> dict[str, Any]:
    ids = targets or ["EP-1", "EP-2"]
    return {
        "id": "MAP-01",
        "source_endpoint": "EP-S",
        "target_endpoints": ids,
        "source": {"provider": "PLEX", "instance": "default", "playlist_id": "S", "endpoint_id": "EP-S"},
        "target": {"provider": "TRAKT", "instance": "default", "playlist_id": "T1", "endpoint_id": "EP-1"},
        "targets": [
            {"provider": "TRAKT", "instance": "default", "playlist_id": "T1", "endpoint_id": "EP-1"},
            {"provider": "TRAKT", "instance": "default", "playlist_id": "T2", "endpoint_id": "EP-2"},
        ][: len(ids)],
        "ruleset_id": rule_id,
        "enabled": True,
    }


def providers(src: list[dict[str, Any]], t1=None, t2=None, t3=None):
    plex = FakeOps("PLEX", {"S": list(src)})
    trakt = FakeOps("TRAKT", {"T1": list(t1 or []), "T2": list(t2 or []), "T3": list(t3 or [])})
    return {"PLEX": plex, "TRAKT": trakt}


def custom_rule(capacity: int, aggregate: int | None = None) -> dict[str, Any]:
    rule = dict(BUILTIN_RULESETS[BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID])
    rule.update({"id": "customrl", "name": "Custom", "built_in": False, "per_endpoint_capacity": capacity, "aggregate_capacity": aggregate or capacity * 5})
    return rule


def test_partition_500_source_items_into_two_250_targets(config_base):
    provs = providers([movie(i) for i in range(500)])
    plan = R.preview_mapping(cfg(), mapping(), providers=provs)
    assert plan["blocked"] is False
    assert [x["planned_additions"] for x in plan["per_target"]] == [250, 250]


def test_aggregate_target_reads_as_one_logical_collection(config_base):
    provs = providers([], [movie(i) for i in range(250)], [movie(i) for i in range(250, 500)])
    plan = R.preview_mapping(cfg(), mapping(), providers=provs)
    assert plan["logical_aggregated_target_count"] == 500
    assert plan["planned_additions"] == 0


def test_stable_assignment_does_not_rebalance_after_addition(config_base):
    rule = custom_rule(300, 600)
    conf = cfg(rule)
    provs = providers([movie(i) for i in range(500)])
    m = mapping("customrl")
    R.run_mapping(conf, m, providers=provs)
    before = R._state_entry(m)["assignments"]
    provs["PLEX"].playlists["S"].append(movie(500))
    R.run_mapping(conf, m, providers=provs)
    after = R._state_entry(m)["assignments"]
    assert {k: v["endpoint_id"] for k, v in before.items()} == {k: after[k]["endpoint_id"] for k in before}
    assert after["tmdb:500"]["endpoint_id"] == "EP-2"


def test_target_addition_propagates_back_to_source(config_base):
    conf = cfg(custom_rule(10, 20))
    provs = providers([movie(1)])
    m = mapping("customrl")
    R.run_mapping(conf, m, providers=provs)
    provs["TRAKT"].playlists["T1"].append(movie(2))
    res = R.run_mapping(conf, m, providers=provs)
    assert res["ok"] is True
    assert canonical_key(movie(2)) in {canonical_key(x) for x in provs["PLEX"].playlists["S"]}


def test_target_removal_propagates_back_to_source(config_base):
    conf = cfg(custom_rule(10, 20))
    provs = providers([movie(1), movie(2)])
    m = mapping("customrl")
    R.run_mapping(conf, m, providers=provs)
    provs["TRAKT"].playlists["T1"] = [x for x in provs["TRAKT"].playlists["T1"] if canonical_key(x) != "tmdb:1"]
    R.run_mapping(conf, m, providers=provs)
    assert "tmdb:1" not in {canonical_key(x) for x in provs["PLEX"].playlists["S"]}


def test_501_items_blocks_before_writes(config_base):
    provs = providers([movie(i) for i in range(501)])
    res = R.run_mapping(cfg(), mapping(), providers=provs)
    assert res["ok"] is False
    assert res["capacity_error"] is True
    assert provs["TRAKT"].calls == []


def test_unmanaged_target_items_count_toward_capacity(config_base):
    src = [movie(i) for i in range(250)]
    unmanaged = [movie(1000 + i) for i in range(10)]
    provs = providers(src, unmanaged, [])
    plan = R.preview_mapping(cfg(), mapping(), providers=provs)
    assert [x["planned_additions"] for x in plan["per_target"]] == [240, 10]


def test_built_in_ruleset_rejects_modification(config_base):
    conf = cfg()
    raw = dict(BUILTIN_RULESETS[BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID])
    raw["per_endpoint_capacity"] = 500
    res = svc.upsert_ruleset(conf, raw)
    assert res["ok"] is False


def test_cloned_custom_ruleset_uses_changed_capacity(config_base):
    conf = cfg()
    cloned = svc.clone_ruleset(conf, BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID, {"name": "SmallTrakt"})
    assert cloned["ok"] is True
    rs = dict(cloned["ruleset"])
    rs["per_endpoint_capacity"] = 2
    rs["aggregate_capacity"] = 4
    saved = svc.upsert_ruleset(conf, rs)
    assert saved["ok"] is True
    provs = providers([movie(i) for i in range(4)])
    plan = R.preview_mapping(conf, mapping(rs["id"]), providers=provs)
    assert [x["planned_additions"] for x in plan["per_target"]] == [2, 2]

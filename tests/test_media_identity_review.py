# tests/test_media_identity_review.py
# CrossWatch - Media identity adapter, deletion and legacy retry regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.history_events import history_sync_key, minimal_history_item
from cw_platform.orchestrator import Orchestrator, _pairs_blocklist, _unresolved
from cw_platform.orchestrator._snapshots import canonicalize_index
from cw_platform.orchestrator._tombstones import media_tombstone_tokens
from test_mdblist_pagination import response, setup_reader
from test_orchestrator_oneway_watchlist import FakeOps


WHEN = "2026-01-01T00:00:00Z"
MOVIE = {"type": "movie", "title": "Movie", "ids": {"tmdb": "121"}, "rating": 8, "rated_at": WHEN}
SHOW = {"type": "show", "title": "Show", "ids": {"tmdb": "121"}, "rating": 6, "rated_at": WHEN}


@pytest.mark.parametrize("feature", ["ratings", "history"])
@pytest.mark.parametrize("removed,remaining", [(MOVIE, SHOW), (SHOW, MOVIE)])
def test_pair_deletion_markers_block_only_matching_type(monkeypatch, feature, removed, remaining):
    monkeypatch.setattr(_pairs_blocklist, "load_unresolved_keys", lambda *a, **kw: set())
    monkeypatch.setattr(_pairs_blocklist, "load_blackbox_keys", lambda *a, **kw: set())
    monkeypatch.setattr(_pairs_blocklist, "keys_for_feature", lambda *a, **kw:
                        {key: 1800000000 for key in media_tombstone_tokens(removed)})
    rows = [{**item, "watched_at": WHEN} for item in (removed, remaining)]
    assert _pairs_blocklist.apply_blocklist(None, rows, dst="MDBLIST", feature=feature) == [rows[1]]
    field = "rated_at" if feature == "ratings" else "watched_at"
    newer = {**rows[0], field: "2030-01-01T00:00:00Z"}
    assert _pairs_blocklist.apply_blocklist(None, [newer], dst="MDBLIST", feature=feature) == [newer]


@pytest.mark.parametrize("alias", ["tv", "series", "shows", "anime"])
def test_show_aliases_coalesce_without_merging_movie(alias):
    first = {**SHOW, "type": alias, "ids": {"tmdb": "121", "imdb": "tt0056751"}}
    second = {**SHOW, "ids": {"imdb": "tt0056751"}}
    index = canonicalize_index({"legacy": first, "other": second, "movie": MOVIE}, feature="watchlist")
    assert set(index) == {"tmdb:121", "tmdb:121#show"}


def test_empty_type_follows_existing_movie_default():
    first = {**MOVIE, "type": "", "ids": {"tmdb": "121", "imdb": "tt0167261"}}
    second = {**MOVIE, "ids": {"imdb": "tt0167261"}}
    index = canonicalize_index({"legacy": first, "other": second}, feature="watchlist")
    assert set(index) == {"tmdb:121"}


@pytest.mark.parametrize("pending", [False, True])
def test_legacy_show_retry_migrates_and_clears_without_clearing_movie(tmp_path, monkeypatch, pending):
    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    path = (_unresolved._pending_path if pending else _unresolved._blocking_path)("MDBLIST", "watchlist")
    path.parent.mkdir(parents=True, exist_ok=True)
    if pending:
        data = {"keys": ["tmdb:121"], "items": {"tmdb:121": SHOW}, "hints": {"tmdb:121": {"reason": "not_found"}}}
    else:
        data = {"tmdb:121": {"item": SHOW, "reason": "not_found"}}
    path.write_text(json.dumps(data), encoding="utf-8")
    assert "tmdb:121#show" in _unresolved.load_unresolved_map("MDBLIST", "watchlist", cross_features=False)
    assert "tmdb:121" not in _unresolved.load_unresolved_keys("MDBLIST", "watchlist")
    assert _unresolved.clear_unresolved("MDBLIST", "watchlist", ["tmdb:121"])["count"] == 0
    assert _unresolved.clear_unresolved("MDBLIST", "watchlist", ["tmdb:121#show"])["count"] == 1
    assert not _unresolved.load_unresolved_map("MDBLIST", "watchlist", cross_features=False)
    path.write_text(json.dumps({"tmdb:121": {"item": MOVIE}}), encoding="utf-8")
    assert _unresolved.clear_unresolved("MDBLIST", "watchlist", ["tmdb:121#show"])["count"] == 0


def test_legacy_retry_without_item_type_is_not_guessed(tmp_path, monkeypatch):
    monkeypatch.setattr(_unresolved, "STATE_DIR", tmp_path)
    path = _unresolved._pending_path("MDBLIST", "ratings")
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps({"keys": ["tmdb:121"], "hints": {"tmdb:121": {"reason": "failed"}}}))
    assert _unresolved.clear_unresolved("MDBLIST", "ratings", ["tmdb:121#show"])["count"] == 0
    assert _unresolved.load_unresolved_pending("MDBLIST", "ratings")[0]["key"] == "tmdb:121"


@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
def test_mdblist_real_cache_loader_upgrades_show_key(tmp_path, monkeypatch, feature):
    module, _, _ = setup_reader(monkeypatch, tmp_path, feature)
    (tmp_path / "cache.json").write_text(json.dumps({"version": 2, "items": {"tmdb:121": SHOW}}))
    result = module._shadow_load()["items"] if feature == "watchlist" else module._load_cache()
    assert result == {"tmdb:121#show": SHOW}


def test_mdblist_not_found_show_does_not_freeze_or_unfreeze_movie(tmp_path, monkeypatch):
    module, adapter, _ = setup_reader(monkeypatch, tmp_path, "watchlist")
    monkeypatch.setattr(module, "_load_unresolved", lambda: {})
    monkeypatch.setattr(module, "_shadow_bust", lambda: None)
    frozen, cleared = [], []
    monkeypatch.setattr(module, "_freeze_item", lambda item, **kw: frozen.append(item))
    monkeypatch.setattr(module, "_unfreeze_keys_if_present", lambda keys: cleared.extend(keys))
    monkeypatch.setattr(module, "mdblist_request", lambda *a, **kw:
                        response({"added": {"movies": 1}, "not_found": {"shows": [{"tmdb": 121}]}}))
    count, unresolved = module.add(adapter, [MOVIE, SHOW])
    assert count == 1
    assert [item["type"] for item in frozen] == ["show"]
    assert [row["item"]["type"] for row in unresolved] == ["show"]
    assert cleared == ["tmdb:121"]


def test_mdblist_legacy_frozen_show_does_not_block_movie(monkeypatch):
    from providers.sync.mdblist import _watchlist

    monkeypatch.setattr(_watchlist, "read_json", lambda *a: {"tmdb:121": {"item": SHOW}})
    assert set(_watchlist._load_unresolved()) == {"tmdb:121#show"}
    accepted, unresolved = _watchlist._batch_payload([MOVIE, SHOW])
    assert [row["type"] for row in accepted] == ["movie"]
    assert [row["item"]["type"] for row in unresolved] == ["show"]
    assert unresolved[0]["hint"] == "frozen_unresolved"


class FeatureOps(FakeOps):
    feature = "watchlist"

    def features(self):
        return {self.feature: True}

    def capabilities(self):
        return {"features": self.features(), "observed_deletes": True, "index_semantics": "present"}

    def health(self, cfg, **kwargs):
        return {"ok": True, "status": "ok", "features": self.features()}

    def build_index(self, cfg, *, feature):
        assert feature == self.feature
        return copy.deepcopy(self.index)


class MDBListOps(FeatureOps):
    def build_index(self, cfg, *, feature):
        return self.module.build_index(self.adapter)

    def add(self, cfg, items, *, feature, dry_run=False):
        batch = list(items)
        self.add_calls.append(copy.deepcopy(batch))
        count, unresolved = self.module.add(self.adapter, batch)
        return {"ok": not unresolved, "count": count, "added": count, "unresolved": unresolved}

    def remove(self, cfg, items, *, feature, dry_run=False):
        batch = list(items)
        self.remove_calls.append(copy.deepcopy(batch))
        count, unresolved = self.module.remove(self.adapter, batch)
        return {"ok": not unresolved, "count": count, "removed": count, "unresolved": unresolved}


class HistoryOps(FeatureOps):
    feature = "history"
    event_mode = False

    def capabilities(self):
        return {**super().capabilities(), "history": {"rewatches": {"read": True, "write": True}}}

    def add(self, cfg, items, *, feature, dry_run=False):
        batch = list(items)
        self.add_calls.append(copy.deepcopy(batch))
        for row in batch:
            self.index[history_sync_key(row, event_mode=self.event_mode)] = minimal_history_item(row, event_mode=self.event_mode)
        return {"ok": True, "count": len(batch), "added": len(batch)}

    def remove(self, cfg, items, *, feature, dry_run=False):
        batch = list(items)
        self.remove_calls.append(copy.deepcopy(batch))
        for row in batch:
            self.index.pop(history_sync_key(row, event_mode=self.event_mode), None)
        return {"ok": True, "count": len(batch), "removed": len(batch)}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("event_mode", [False, True])
@pytest.mark.parametrize("removed_type", ["movie", "episode"])
def test_history_removal_preserves_other_type_and_other_play(config_base, monkeypatch, mode, event_mode, removed_type):
    movie = {**MOVIE, "watched_at": WHEN}
    episode = {"type": "episode", "title": "Episode", "ids": {"tmdb": "121"},
               "show_ids": {"tmdb": "121"}, "season": 1, "episode": 2, "watched_at": WHEN}
    rows = [movie, episode]
    if event_mode:
        rows.append({**movie, "watched_at": "2026-02-01T00:00:00Z"})
    source = HistoryOps("WETRAKR", {history_sync_key(row, event_mode=event_mode):
                                   minimal_history_item(row, event_mode=event_mode) for row in rows})
    target = HistoryOps("CROSSWATCH", {})
    source.event_mode = target.event_mode = event_mode
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"WETRAKR": source, "CROSSWATCH": target})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    cfg = {"runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"enable_add": True, "enable_remove": True, "include_observed_deletes": True, "allow_mass_delete": True},
           "pairs": [{"id": "history-collision", "enabled": True, "source": "WETRAKR", "target": "CROSSWATCH", "mode": mode,
                      "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": True, "rewatches": event_mode}}}]}
    orchestrator = Orchestrator(cfg)
    assert not orchestrator.run()["errors"]
    assert set(target.index) == set(source.index)
    removed = movie if removed_type == "movie" else episode
    source.index.pop(history_sync_key(removed, event_mode=event_mode))
    assert not orchestrator.run()["errors"]
    assert set(target.index) == set(source.index)
    assert sum(map(len, target.remove_calls)) == 1
    initial_adds = sum(map(len, target.add_calls))
    assert not orchestrator.run()["errors"]
    assert set(target.index) == set(source.index)
    assert sum(map(len, target.add_calls)) == initial_adds
    assert not source.add_calls


@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("removed", [MOVIE, SHOW])
def test_real_mdblist_adapter_collision_sync_repeat_and_remove(config_base, tmp_path, monkeypatch, feature, mode, removed):
    module, adapter, _ = setup_reader(monkeypatch, tmp_path, feature)
    adapter.config["mdblist"]["ratings_write_delay_ms"] = 0
    if feature == "watchlist":
        monkeypatch.setattr(module, "_load_unresolved", lambda: {})
        monkeypatch.setattr(module, "_shadow_bust", lambda: None)
    remote = {}
    writes = []

    def request(adapter, method, url, **kwargs):
        if method == "GET":
            if feature == "watchlist":
                return response(list(remote.values()))
            return response({kind + "s": [{kind: {"ids": row["ids"]}, "rating": row["rating"], "rated_at": WHEN}
                                          for row in remote.values() if row["type"] == kind]
                             for kind in ("movie", "show")})
        payload = kwargs["json"]
        writes.append(copy.deepcopy(payload))
        removing = url == (module.URL_UNRATE if feature == "ratings" else module.URL_MODIFY.format(action="remove"))
        counts = {}
        for bucket, rows in payload.items():
            kind = bucket[:-1]
            counts[bucket] = len(rows)
            for row in rows:
                ids = row.get("ids") or row
                key = (kind, str(ids["tmdb"]))
                if removing:
                    remote.pop(key, None)
                else:
                    remote[key] = {"type": kind, "mediatype": kind, "ids": {"tmdb": str(ids["tmdb"])},
                                   "rating": row.get("rating", 8), "rated_at": WHEN}
        return response({"removed" if removing else "added": counts})

    monkeypatch.setattr(module, "mdblist_request", request)
    source = FeatureOps("TRAKT", {canonical_key(row): copy.deepcopy(row) for row in (MOVIE, SHOW)})
    target = MDBListOps("MDBLIST", {})
    source.feature = target.feature = feature
    target.module, target.adapter = module, adapter
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"TRAKT": source, "MDBLIST": target})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    cfg = {"runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"enable_add": True, "enable_remove": True, "include_observed_deletes": True, "allow_mass_delete": True},
           "pairs": [{"id": "real-mdblist", "enabled": True, "source": "TRAKT", "target": "MDBLIST", "mode": mode,
                      "feature": feature, "features": {feature: {"enable": True, "add": True, "remove": True}}}]}
    orchestrator = Orchestrator(cfg)
    assert not orchestrator.run()["errors"]
    assert set(target.build_index(cfg, feature=feature)) == {"tmdb:121", "tmdb:121#show"}
    initial_writes = len(writes)
    assert not orchestrator.run()["errors"]
    assert len(writes) == initial_writes
    source.index.pop(canonical_key(removed))
    assert not orchestrator.run()["errors"]
    remaining = SHOW if removed == MOVIE else MOVIE
    assert set(target.build_index(cfg, feature=feature)) == {canonical_key(remaining)}
    after_remove = len(writes)
    assert not orchestrator.run()["errors"]
    assert len(writes) == after_remove
    assert set(source.index) == {canonical_key(remaining)}

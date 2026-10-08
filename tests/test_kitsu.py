# tests/test_kitsu.py
# CrossWatch - Kitsu library, mapping and scrobble integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from contextlib import nullcontext
from dataclasses import replace
import json
from types import SimpleNamespace

import pytest

from cw_platform.anime_mapping import storage
from cw_platform.anime_mapping.overrides import upsert_override
from cw_platform.id_map import canonical_key
from cw_platform.modules_registry import load_sync_ops, state_read_features, sync_provider_names
from providers.sync import _mod_KITSU as module
from providers.sync.kitsu import _common, _history, _ratings, _watchlist


@pytest.fixture()
def mapping(config_base):
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"anilist:100": {"13-22": "1-10"}},
        "tmdb_movie:20": {"anilist:200": {"1": "1"}},
    }), encoding="utf-8")
    paths["identity"].write_text("anidb\tmyanimelist\tanilist\tsimkl\tkitsu\n\t\t100\t\t1\n\t\t200\t\t2\n", encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return {"anime_mapping": {"enabled": True}, "kitsu": {"access_token": "token"}}


def episode(number=15):
    return {"type": "episode", "show_ids": {"tvdb": "10"}, "ids": {}, "season": 3, "episode": number, "title": "Anime"}


def movie():
    return {"type": "movie", "ids": {"kitsu": "2"}, "title": "Movie"}


class Library:
    def library_batch(self, identifiers):
        return nullcontext()

    def __init__(self):
        self.rows = {}
        self.writes = []
        self.media_rows = {
            "1": {"id": "1", "type": "anime", "attributes": {"canonicalTitle": "Anime", "subtype": "TV", "episodeCount": 10, "status": "finished"}},
            "2": {"id": "2", "type": "anime", "attributes": {"canonicalTitle": "Movie", "subtype": "movie", "episodeCount": 1, "status": "finished"}},
        }

    def lookup(self, ident):
        return self.rows.get(ident)

    def media(self, ident):
        return self.media_rows[ident]

    def save(self, ident, entry, attributes):
        self.writes.append((ident, dict(attributes)))
        self.rows.setdefault(ident, {"id": ident, "attributes": {"progress": 0, "updatedAt": "2026-10-07T00:00:00Z"}})["attributes"].update(attributes)

    def delete(self, entry):
        del self.rows[entry["id"]]

    def entries(self):
        return [(entry, self.media_rows[ident]) for ident, entry in self.rows.items()]


@pytest.fixture()
def adapter(mapping):
    return SimpleNamespace(raw_cfg=mapping, client=Library())


def test_registry_and_capabilities():
    assert "KITSU" in sync_provider_names(upper=True)
    ops = load_sync_ops("KITSU")
    assert set(state_read_features(ops)) == {"watchlist", "ratings", "history"}
    assert ops.features()["progress"] is False
    assert ops.capabilities()["history"]["remove"] is True
    assert module.get_manifest()["version"] == "0.1"


@pytest.mark.parametrize("verbose", [False, True])
def test_interactive_http_progress_for_reads_and_writes(config_base, monkeypatch, verbose):
    import requests
    from cw_platform.orchestrator._pairs_utils import inject_ctx_into_provider, release_ctx_from_providers
    from services.interactive_sync_progress import SyncProgress

    progress = SyncProgress()
    ctx = SimpleNamespace(interactive=SimpleNamespace(on_progress=progress.event),
                          emit=lambda event, **data: progress.event(dict(event=event, **data)))
    cfg = {"kitsu": {"access_token": "test-token"}}
    calls = []
    entry = {"id": "entry", "type": "libraryEntries", "attributes": {"status": "planned", "progress": 0},
             "relationships": {"anime": {"data": {"id": "2", "type": "anime"}}}}

    def transport(session, method, url, **kwargs):
        calls.append(method)
        if method == "PATCH":
            data = {"data": kwargs["json"]["data"]}
        elif "/users?" in url:
            data = {"data": [{"id": "7", "type": "users", "attributes": {"name": "Test"}}]}
        else:
            data = {"data": [entry], "included": [Library().media("2")]}
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(data).encode()
        return response

    monkeypatch.setenv("CW_API_HITS", "1" if verbose else "")
    monkeypatch.setattr(requests.Session, "request", transport)
    monkeypatch.delattr(module, "ctx", raising=False)
    inject_ctx_into_provider(module.OPS, ctx)
    try:
        assert module.OPS.build_index(cfg, feature="watchlist")
        assert progress.public()["requests_by_provider"] == {"KITSU": 2}
        assert module.OPS.add(cfg, [{**movie(), "rating": 8}], feature="ratings")["count"] == 1
        assert calls == ["GET", "GET", "GET", "GET", "PATCH"]
        assert progress.public()["requests"] == 5
        assert progress.public()["requests_by_provider"] == {"KITSU": 5}
    finally:
        release_ctx_from_providers(ctx)
    assert not hasattr(module, "ctx")
    assert module.OPS.build_index(cfg, feature="watchlist")
    assert progress.public()["requests"] == 5


def test_mapping_uses_identity_index_and_season_offset(mapping):
    assert _common.resolve_target(mapping, episode()) == ("1", 3)
    assert _common.resolve_target(mapping, {"type": "movie", "ids": {"tmdb": "20"}}) == ("2", 1)
    assert _common.resolve_target(mapping, {**episode(), "show_ids": {"tvdb": "999"}}) is None
    assert _common.resolve_target({}, episode()) is None


def test_custom_identity_mapping_reads_back_and_rejects_ambiguity(adapter):
    rule = {"media_type": "movie", "match_provider": "tmdb", "match_id": "999",
            "target_namespace": "kitsu", "target_id": "2"}
    upsert_override(rule)
    assert _common.media_item(adapter, adapter.client.media("2"))["ids"]["tmdb"] == "999"
    upsert_override({**rule, "match_id": "888"})
    from cw_platform.anime_mapping.overrides import find_source_identity_overrides
    assert find_source_identity_overrides({"kitsu": "2"}, media_type="movie") == {}


def test_completed_entry_still_validates_media_type(adapter):
    adapter.client.rows["1"] = {"id": "1", "attributes": {"status": "completed", "progress": 10}}
    result = _history.add(adapter, [{"type": "movie", "ids": {"kitsu": "1"}}])
    assert not result["confirmed_keys"]
    assert result["unresolved"]


def test_custom_kitsu_episode_override_wins_both_directions(mapping):
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "10", "match_season": 3,
                     "episode_from": 15, "episode_to": 16, "episode_start_at": 1, "target_namespace": "kitsu", "target_id": "9"})
    assert _common.resolve_target(mapping, episode()) == ("9", 1)
    assert [item["episode"] for item in _common.episode_items(mapping, "9", 2, "Anime")] == [15, 16]
    original = _common.episode_items(mapping, "1", 4, "Anime")
    assert {item["episode"] for item in original} == {13, 14}


def test_custom_movie_mapping_without_installed_dataset(config_base):
    upsert_override({"media_type": "movie", "match_provider": "tmdb", "match_id": "50", "target_namespace": "kitsu", "target_id": "2"})
    assert _common.resolve_target({"anime_mapping": {"enabled": True}}, {"type": "movie", "ids": {"tmdb": "50"}}) == ("2", 1)


def test_history_read_and_monotonic_writes_preserve_rating(adapter):
    adapter.client.save("1", None, {"status": "planned", "ratingTwenty": 18, "progress": 0})
    result = _history.add(adapter, [episode(), episode(14)])
    assert result["count"] == 2
    assert adapter.client.rows["1"]["attributes"]["progress"] == 3
    assert adapter.client.rows["1"]["attributes"]["ratingTwenty"] == 18
    assert len(adapter.client.writes) == 2
    result = _history.build_index(adapter)
    assert {item["episode"] for item in result.values()} == {13, 14, 15}
    assert all(item["season"] == 3 and item["watched"] for item in result.values())
    assert all(item["show_ids"] == {"tvdb": "10"} for item in result.values())


def test_completed_status_expands_known_total_and_movie(adapter):
    adapter.client.save("1", None, {"status": "completed", "progress": 0})
    adapter.client.save("2", None, {"status": "completed", "progress": 0})
    items = list(_history.build_index(adapter).values())
    assert len([item for item in items if item["type"] == "episode"]) == 10
    assert len([item for item in items if item["type"] == "movie"]) == 1


@pytest.mark.parametrize("status", ["on_hold", "dropped"])
def test_new_episode_progress_resumes_held_or_dropped_title(adapter, status):
    adapter.client.save("1", None, {"status": status, "ratingTwenty": 18, "progress": 1})
    result = _history.add(adapter, [episode()])
    assert result["count"] == 1
    attr = adapter.client.rows["1"]["attributes"]
    assert attr["status"] == "current"
    assert attr["progress"] == 3
    assert attr["ratingTwenty"] == 18


def test_history_finishes_movie_and_final_episode_but_not_airing(adapter):
    assert _history.add(adapter, [episode(22), movie()])["count"] == 2
    assert all(entry["attributes"]["status"] == "completed" for entry in adapter.client.rows.values())
    adapter.client.rows.clear()
    adapter.client.media_rows["1"]["attributes"]["status"] = "current"
    _history.add(adapter, [episode(22)])
    assert adapter.client.rows["1"]["attributes"]["status"] == "current"


def test_ratings_and_watchlist_preserve_other_features(adapter):
    _watchlist.add(adapter, [movie()])
    _ratings.add(adapter, [{**movie(), "rating": 8}])
    assert adapter.client.rows["2"]["attributes"]["status"] == "planned"
    assert next(iter(_ratings.build_index(adapter).values()))["rating"] == 8
    _watchlist.remove(adapter, [movie()])
    assert adapter.client.rows["2"]["attributes"]["status"] == "on_hold"
    assert adapter.client.rows["2"]["attributes"]["ratingTwenty"] == 16
    _history.add(adapter, [movie()])
    _ratings.remove(adapter, [movie()])
    assert adapter.client.rows["2"]["attributes"]["status"] == "completed"
    assert adapter.client.rows["2"]["attributes"]["progress"] == 1
    assert adapter.client.rows["2"]["attributes"]["ratingTwenty"] is None
    assert not _watchlist.add(adapter, [movie()])["confirmed_keys"]


def test_watchlist_delete_only_empty_planned_entry(adapter):
    _watchlist.add(adapter, [movie()])
    assert _watchlist.build_index(adapter)
    _watchlist.remove(adapter, [movie()])
    assert not adapter.client.rows


def test_unmapped_and_failed_writes_are_unresolved(adapter):
    item = {"type": "movie", "ids": {"imdb": "tt000"}}
    result = _history.add(adapter, [item])
    assert result["unresolved_keys"] == [canonical_key(item)]
    assert not result["confirmed_keys"]
    adapter.client.save = lambda *args: (_ for _ in ()).throw(RuntimeError("failure"))
    assert not _history.add(adapter, [movie()])["confirmed_keys"]
    adapter.client.rows["2"] = {"id": "2", "attributes": {"progress": 1, "status": "completed"}}
    assert _history.remove(adapter, [movie()])["unresolved"]


@pytest.mark.parametrize("existing", [False, True])
def test_history_batch_writes_one_progress_update_per_title(mapping, monkeypatch, existing):
    client = module.KitsuClient(mapping, "default")
    client._user = {"id": "7"}
    calls = []
    stored = {"id": "entry", "type": "libraryEntries", "attributes": {"progress": 0, "status": "current", "ratingTwenty": 18}} if existing else None
    media = {"id": "1", "type": "anime", "attributes": {"subtype": "TV", "episodeCount": 200, "status": "finished"}}

    def request(method, path, **kwargs):
        nonlocal stored
        calls.append((method, path, copy.deepcopy(kwargs)))
        if method == "GET" and path == "/library-entries":
            return {"data": [copy.deepcopy(stored)] if stored else []}
        if method == "GET" and path == "/anime/1":
            return {"data": copy.deepcopy(media)}
        if method in ("PATCH", "POST"):
            stored = stored or {"id": "entry", "type": "libraryEntries", "attributes": {}}
            stored["attributes"].update(kwargs["json"]["data"]["attributes"])
            return {"data": copy.deepcopy(stored)}
        raise AssertionError((method, path))

    monkeypatch.setattr(client, "request", request)
    items = [{"type": "episode", "show_ids": {"kitsu": "1"}, "season": 1, "episode": n} for n in range(1, 101)]
    adapter = SimpleNamespace(raw_cfg=mapping, client=client)
    result = _history.add(adapter, items)
    assert result["count"] == 100 and not result["unresolved"]
    assert set(result["confirmed_keys"]) == {canonical_key(item) for item in items}
    assert [call[0] for call in calls] == ["GET", "GET", "PATCH" if existing else "POST"]
    assert calls[-1][2]["json"]["data"]["attributes"] == {"progress": 100, "status": "current"}
    if existing:
        assert stored["attributes"]["ratingTwenty"] == 18
    stored["attributes"]["progress"] = 150
    calls.clear()
    assert _history.add(adapter, items)["count"] == 100
    assert [call[0] for call in calls] == ["GET", "GET"]
    assert stored["attributes"]["progress"] == 150


def test_grouped_history_validates_each_episode_before_writing(adapter):
    valid = [episode(13), episode(15)]
    bad = {**episode(30), "show_ids": {"kitsu": "1"}, "season": 1}
    wrong_kind = {"type": "movie", "ids": {"kitsu": "1"}}
    result = _history.add(adapter, [bad, *valid, wrong_kind, movie()])
    assert set(result["confirmed_keys"]) == {canonical_key(item) for item in [*valid, movie()]}
    assert {row["reason"] for row in result["unresolved"]} == {"episode_out_of_range", "anime_media_type_mismatch"}
    assert adapter.client.writes == [("1", {"progress": 3, "status": "current"}), ("2", {"progress": 1, "status": "completed"})]


@pytest.mark.parametrize("failure", ["lookup", "media", "save"])
def test_grouped_history_failure_only_affects_that_title(adapter, monkeypatch, failure):
    original = getattr(adapter.client, failure)

    def fail(ident, *args):
        if ident == "1":
            raise RuntimeError("request failed")
        return original(ident, *args)

    monkeypatch.setattr(adapter.client, failure, fail)
    episodes = [episode(13), episode(15)]
    result = _history.add(adapter, [*episodes, movie()])
    assert result["confirmed_keys"] == [canonical_key(movie())]
    assert set(result["unresolved_keys"]) == {canonical_key(item) for item in episodes}


def test_grouped_history_uses_custom_episode_offsets(adapter):
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "99", "match_season": 2,
                     "target_namespace": "kitsu", "target_id": "1", "episode_from": 5,
                     "episode_to": 10, "episode_start_at": 4})
    items = [episode(15), {**episode(7), "show_ids": {"tvdb": "99"}, "season": 2}]
    result = _history.add(adapter, items)
    assert result["count"] == 2 and not result["unresolved"]
    assert adapter.client.writes == [("1", {"progress": 6, "status": "current"})]


def test_grouped_history_preserves_source_status(status_adapter):
    items = [{**episode(n), "watch_status": "dropped"} for n in range(13, 18)]
    result = _history.add(status_adapter, items)
    assert result["count"] == 5 and not result["unresolved"]
    assert status_adapter.client.writes == [("1", {"progress": 5, "status": "current"}), ("1", {"status": "dropped"})]


@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
def test_title_batches_use_one_lookup_and_keep_cache_current(mapping, monkeypatch, feature):
    client = module.KitsuClient(mapping, "default")
    client._user = {"id": "7"}
    calls = []
    entries = {}

    def request(method, path, **kwargs):
        calls.append((method, path, copy.deepcopy(kwargs)))
        if method == "GET":
            assert kwargs["params"]["include"] == "anime"
            ids = kwargs["params"]["filter[animeId]"].split(",")
            return {"data": [copy.deepcopy(entries[i]) for i in ids if i in entries]}
        if method == "DELETE":
            del entries[path.rsplit("/", 1)[1]]
            return {}
        payload = kwargs["json"]["data"]
        ident = payload["id"] if method == "PATCH" else payload["relationships"]["anime"]["data"]["id"]
        entry = entries.setdefault(ident, {"id": ident, "type": "libraryEntries", "attributes": {},
            "relationships": {"anime": {"data": {"type": "anime", "id": ident}}}})
        entry["attributes"].update(payload["attributes"])
        return {"data": copy.deepcopy(entry)}

    monkeypatch.setattr(client, "request", request)
    adapter = SimpleNamespace(raw_cfg={}, client=client)
    writer = _watchlist if feature == "watchlist" else _ratings
    items = [{"type": "show", "ids": {"kitsu": str(i)}, "rating": 8} for i in range(1, 101)]
    items.append({**items[0], "ids": {"kitsu": "1", "imdb": "tt999"}, "rating": 9})
    result = writer.add(adapter, items)
    assert not result["unresolved"]
    assert sum(method == "GET" for method, _, _ in calls) == 1
    assert sum(method == "POST" for method, _, _ in calls) == 100
    assert sum(method == "PATCH" for method, _, _ in calls) == (1 if feature == "ratings" else 0)
    if feature == "ratings":
        assert entries["1"]["attributes"]["ratingTwenty"] == 18
    assert client._entry_cache is None
    calls.clear()
    result = writer.remove(adapter, items)
    assert not result["unresolved"]
    assert sum(method == "GET" for method, _, _ in calls) == 1
    if feature == "watchlist":
        assert not entries
        assert sum(method == "DELETE" for method, _, _ in calls) == 100
    else:
        assert all(row["attributes"]["ratingTwenty"] is None for row in entries.values())
    assert client._entry_cache is None


@pytest.mark.parametrize("bad", ["partial", "duplicate", "foreign", "empty_next"])
def test_batch_lookup_never_writes_after_incomplete_or_invalid_read(monkeypatch, bad):
    client = module.KitsuClient({}, "default")
    client._user = {"id": "7"}
    calls = []
    row = {"id": "1", "type": "libraryEntries", "attributes": {"status": "planned"},
           "relationships": {"anime": {"data": {"type": "anime", "id": "1"}}}}

    def request(method, path, **kwargs):
        calls.append(method)
        assert method == "GET"
        if kwargs["params"]["page[offset]"]:
            raise RuntimeError("later page failed")
        if bad == "foreign":
            row["relationships"]["anime"]["data"]["id"] = "3"
        return {"data": [] if bad == "empty_next" else [row, row] if bad == "duplicate" else [row],
                "links": {"next": "more"} if bad in ("partial", "empty_next") else {}}

    monkeypatch.setattr(client, "request", request)
    items = [{"type": "show", "ids": {"kitsu": str(i)}} for i in (1, 2)]
    result = _watchlist.add(SimpleNamespace(raw_cfg={}, client=client), items)
    assert not result["confirmed_keys"]
    assert len(result["unresolved_keys"]) == 2
    assert client._entry_cache is None


def test_batch_lookup_bounds_filters_and_rechecks_failed_writes(monkeypatch):
    client = module.KitsuClient({}, "default")
    client._user = {"id": "7"}
    filters = []
    row = {"id": "entry", "type": "libraryEntries", "attributes": {"progress": 4},
           "relationships": {"anime": {"data": {"type": "anime", "id": "1"}}}}

    def request(method, path, **kwargs):
        if method != "GET":
            raise RuntimeError("write failed")
        ids = kwargs["params"]["filter[animeId]"].split(",")
        filters.append(ids)
        return {"data": [copy.deepcopy(row)] if "1" in ids else []}

    monkeypatch.setattr(client, "request", request)
    with client.library_batch(map(str, range(1, 202))):
        assert [len(ids) for ids in filters] == [100, 100, 1]
        assert client.lookup("2") is None
        entry = client.lookup("1")
        with pytest.raises(RuntimeError):
            client.save("1", entry, {"progress": 6})
        row["attributes"]["progress"] = 5
        assert client.lookup("1")["attributes"]["progress"] == 5
        assert filters[-1] == ["1"]
        with pytest.raises(RuntimeError):
            client.delete(client.lookup("1"))
        assert client.lookup("1") is not None
        assert filters[-2:] == [["1"], ["1"]]
    assert client._entry_cache is None


def test_library_pagination_and_partial_response_failure(monkeypatch):
    client = module.KitsuClient({}, "default")
    client._user = {"id": "7"}
    offsets = []

    def request(method, path, **kwargs):
        offset = kwargs["params"]["page[offset]"]
        offsets.append(offset)
        assert kwargs["params"]["filter[kind]"] == "anime"
        assert kwargs["params"]["page[limit]"] == 500
        if offset:
            raise RuntimeError("page failed")
        return {"data": [{"id": "entry", "type": "libraryEntries", "attributes": {},
            "relationships": {"anime": {"data": {"id": "1"}}}}], "included": [Library().media("1")], "links": {"next": "ignored"}}

    monkeypatch.setattr(client, "request", request)
    with pytest.raises(RuntimeError, match="page failed"):
        list(client.entries())
    assert offsets == [0, 1]


def test_library_jsonapi_payloads_and_confirmation(monkeypatch):
    client = module.KitsuClient({}, "default")
    client._user = {"id": "7"}
    calls = []

    def request(method, path, **kwargs):
        calls.append((method, path, copy.deepcopy(kwargs)))
        return {"data": {"id": "entry", **kwargs["json"]["data"]}}

    monkeypatch.setattr(client, "request", request)
    client.save("1", None, {"status": "current", "progress": 3})
    assert calls[0][2]["json"]["data"]["relationships"]["anime"]["data"] == {"type": "anime", "id": "1"}
    client.save("1", {"id": "entry"}, {"ratingTwenty": None})
    assert calls[-1] == ("PATCH", "/library-entries/entry", {"json": {"data": {"type": "libraryEntries", "id": "entry", "attributes": {"ratingTwenty": None}}}})
    monkeypatch.setattr(client, "request", lambda *a, **kw: {"data": {"id": "entry", "type": "libraryEntries", "attributes": {}}})
    with pytest.raises(RuntimeError):
        client.save("1", {"id": "entry"}, {"progress": 4})


@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_scrobble_watched_gate_profile_and_write(mapping, monkeypatch, kind):
    from providers.scrobble.kitsu import sink
    from providers.scrobble.scrobble import ScrobbleEvent

    writes = []
    monkeypatch.setattr(sink, "record_watch", lambda *a, **kw: None)
    monkeypatch.setattr(sink, "record_scrobble_event", lambda *a, **kw: None)
    monkeypatch.setattr(module.KITSUModule, "add", lambda self, feature, items: writes.append((self.instance_id, feature, items)) or {"count": 1})
    cfg = {**mapping, "kitsu": {"instances": {"P01": {"access_token": "t"}}}}
    target = sink.KitsuSink(instance_id="P01")
    ev = ScrobbleEvent(action="stop", media_type=kind, ids={"kitsu": "2", "tvdb_show": "10"}, title="Anime", year=2020,
                      season=3, number=15, progress=40, account="test", server_uuid="s", session_key="1", raw={})
    assert target.send(ev, cfg)["skipped"]
    ev = replace(ev, progress=95)
    assert target.send(ev, cfg)["ok"]
    assert writes[0][0:2] == ("P01", "history")
    assert writes[0][2][0]["type"] == kind


def test_watcher_and_webhook_factories_accept_kitsu_profile():
    from providers.scrobble.kitsu.sink import KitsuSink
    from providers.scrobble.watch_manager import _make_sink
    from providers.webhooks.config import sink_configured

    cfg = {"kitsu": {"instances": {"P01": {"access_token": "token"}}}}
    assert isinstance(_make_sink("kitsu", lambda: cfg, "P01"), KitsuSink)
    assert sink_configured(cfg, "kitsu", "P01")


def test_capture_and_sync_api_discover_kitsu(mapping, monkeypatch, config_base):
    from api.syncAPI import api_sync_providers
    from services import snapshots

    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: mapping)
    providers = json.loads(api_sync_providers().body)
    provider = next(row for row in providers if row["name"] == "KITSU")
    assert provider["version"] == "0.1" and provider["configured"]
    assert provider["features"]["history"] and not provider["features"]["progress"]
    monkeypatch.setattr(snapshots, "CONFIG", config_base)
    monkeypatch.setattr(module.KITSUModule, "build_index", lambda self, feature: {canonical_key(movie()): movie()})
    result = snapshots.create_snapshot("KITSU", "watchlist", cfg=mapping)
    assert result["ok"]
    loaded = snapshots.read_snapshot(result["path"])
    assert loaded["stats"]["count"] == 1


def test_insights_include_kitsu_profiles_and_native_anime_ids(config_base, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import insightAPI

    cfg = {"kitsu": {"instances": {"P01": {"access_token": "test-token"}}}}
    assert insightAPI._settings_auth_summary(cfg) == {
        "configured": 1, "profiles": [{"provider": "kitsu", "count": 1}]
    }
    items = {f"kitsu:{ident}": {"type": "show", "ids": {"kitsu": ident}} for ident in ("1", "2")}
    state = {"providers": {"KITSU": {"instances": {"P01": {
        "watchlist": {"baseline": {"items": items}}
    }}}}}
    cw = SimpleNamespace(STATS=SimpleNamespace(data={"samples": [], "events": []}),
                         REPORT_DIR=None, CACHE_DIR=None, _load_wall_snapshot=lambda: [],
                         _append_log=lambda *args, **kwargs: None)
    monkeypatch.setattr(insightAPI, "_env", lambda: (cw, lambda: cfg, lambda value: None, lambda *a, **kw: None))
    monkeypatch.setattr(insightAPI, "_load_state_features", lambda features: state)
    monkeypatch.setattr(insightAPI, "_state_fingerprint", lambda features: None)
    app = FastAPI()
    insightAPI.register_insights(app)
    response = TestClient(app).get("/api/insights?limit_samples=0&history=0&include_events=0")
    assert response.status_code == 200
    feature = response.json()["features"]["watchlist"]
    assert feature["providers"]["kitsu"] == 2
    assert feature["providers_instances"]["kitsu"]["P01"] == 2
    assert feature["providers_mse"]["kitsu"]["anime"] == 2


def test_history_runs_through_orchestrator_and_second_run_is_idempotent(mapping, monkeypatch):
    from cw_platform.orchestrator.facade import Orchestrator
    from tests.test_anilist_history_orchestrator import FakeSource

    library = Library()
    watched = {**episode(), "watched": True, "watched_at": "2026-10-07T12:00:00Z"}
    source = FakeSource({canonical_key(watched): watched})
    monkeypatch.setattr(module, "KitsuClient", lambda *a: library)
    monkeypatch.setattr(module.OPS, "health", lambda cfg: {"ok": True, "status": "ok", "features": module.supported_features(), "api": {}})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"JELLYFIN": source, "KITSU": module.OPS})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_unresolved_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_blackbox_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *a, **kw: set())
    cfg = {**mapping, "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"dry_run": False, "enable_add": True, "enable_remove": False},
           "pairs": [{"id": "kitsu-test", "enabled": True, "source": "JELLYFIN", "target": "KITSU", "mode": "one-way",
                      "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": False}}}]}
    Orchestrator(cfg).run()
    assert library.rows["1"]["attributes"]["progress"] == 3
    writes = len(library.writes)
    Orchestrator(cfg).run()
    assert len(library.writes) == writes
    assert not source.add_calls


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "history"])
@pytest.mark.parametrize("direction", ["one-way", "two-way", "two-way-reversed"])
def test_anime_only_filter_applies_before_interactive_review(mapping, config_base, monkeypatch, feature, direction):
    from cw_platform.orchestrator.facade import Orchestrator
    from cw_platform.orchestrator._interactive import InteractivePlan
    from tests.test_orchestrator_dry_run_no_side_effects import FakeOps, _install

    library = Library()
    library.media_rows["3"] = {**library.media_rows["2"], "id": "3"}
    library.media_rows["4"] = {**library.media_rows["2"], "id": "4"}
    upsert_override({"media_type": "movie", "match_provider": "tmdb", "match_id": "999",
                     "target_namespace": "kitsu", "target_id": "4"})
    anime = [{"type": "movie", "ids": {"tmdb": "20"}, "title": "Mapped anime"},
             {"type": "movie", "ids": {"kitsu": "3"}, "title": "Native anime"},
             {"type": "movie", "ids": {"tmdb": "999"}, "title": "Custom anime"}]
    other = [{"type": "movie", "ids": {"tmdb": ident}, "title": title}
             for ident, title in [("101", "Leon"), ("425", "Ice Age"), ("550", "Fight Club")]]
    if feature == "history":
        upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "999", "match_season": 1,
                         "target_namespace": "kitsu", "target_id": "1", "episode_from": 1,
                         "episode_to": 10, "episode_start_at": 1})
        anime.extend([episode(), {**episode(2), "show_ids": {"tvdb": "999"}, "season": 1, "title": "Custom episode"}])
        other.extend([{**episode(), "show_ids": {"tvdb": "888"}, "title": "Non-anime episode"},
                      {**episode(), "season": 0, "title": "Special"}])
    rows = [{**item, "rating": 8, "watched": True, "watched_at": "2026-10-07T12:00:00Z"} for item in anime + other]
    source = FakeOps("SIMKL", {canonical_key(item): item for item in rows})
    _install(monkeypatch, source, source, config_base / ".cw_state")
    monkeypatch.setattr(source, "features", module.supported_features)
    monkeypatch.setattr(source, "capabilities", lambda: {"features": module.supported_features(), "index_semantics": "present"})
    health = lambda cfg, **kwargs: {"ok": True, "status": "ok", "features": module.supported_features(), "api": {}}
    monkeypatch.setattr(source, "health", health)
    monkeypatch.setattr(module.OPS, "health", health)
    monkeypatch.setattr(module, "KitsuClient", lambda *args: library)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SIMKL": source, "KITSU": module.OPS})
    src, dst = ("KITSU", "SIMKL") if direction == "two-way-reversed" else ("SIMKL", "KITSU")
    cfg = {**mapping, "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"dry_run": False, "enable_add": True, "enable_remove": False},
           "pairs": [{"id": "p1", "enabled": True, "source": src, "target": dst,
                      "mode": "one-way" if direction == "one-way" else "two-way", "feature": feature,
                      "features": {feature: {"enable": True, "add": True, "remove": False,
                                             "use_anime_mapping": True, "anime_only_sync": True}}}]}
    plan = InteractivePlan()
    Orchestrator(cfg, interactive=plan).run(dry_run=True, write_state_json=False)
    assert {row["item"]["title"] for row in plan.rows.values()} == {item["title"] for item in anime}
    assert all(row["provider"] == "KITSU" and row["operation"] == "add" for row in plan.rows.values())
    assert not library.writes and not source.add_calls
    Orchestrator(cfg, interactive=InteractivePlan(preview=False, selected=set(plan.rows))).run()
    assert set(library.rows) == ({"1", "2", "3", "4"} if feature == "history" else {"2", "3", "4"})
    assert not source.add_calls


def test_anime_only_filter_keeps_native_movie_without_mapping(config_base):
    from cw_platform.anime_mapping.service import anime_only_adds

    other = {"type": "movie", "ids": {"tmdb": "101"}, "title": "Leon"}
    assert anime_only_adds([movie(), other, episode()], {}, {}, "history", target="KITSU") == ([movie()], 2)


def test_shared_anime_filter_uses_destination_ids(mapping):
    from cw_platform.anime_mapping.service import anime_only_adds

    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "999", "match_season": 1,
                     "target_namespace": "kitsu", "target_id": "3", "episode_from": 1,
                     "episode_to": 10, "episode_start_at": 1})
    items = [{**movie(), "ids": {"kitsu": "3"}},
             {**episode(2), "show_ids": {"tvdb": "999"}, "season": 1}]
    assert anime_only_adds(items, mapping, {}, "history", target="KITSU") == (items, 0)
    assert anime_only_adds(items, mapping, {}, "history", target="ANILIST") == ([], 2)


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "history"])
def test_shared_anime_filter_keeps_custom_mapping_without_dataset(config_base, feature):
    from cw_platform.anime_mapping.service import anime_only_adds

    upsert_override({"media_type": "movie", "match_provider": "tmdb", "match_id": "999",
                     "target_namespace": "kitsu", "target_id": "4"})
    item = {"type": "movie", "ids": {"tmdb": "999"}, "title": "Custom anime"}
    assert anime_only_adds([item], {"anime_mapping": {"enabled": True}}, {}, feature, target="KITSU") == ([item], 0)


@pytest.mark.parametrize("status", ["current", "completed", "on_hold", "dropped"])
def test_history_removal_reduces_tail_preserving_library_fields(adapter, status):
    adapter.client.save("1", None, {"status": status, "progress": 10, "ratingTwenty": 18,
                                   "notes": "Keep", "private": True, "finishedAt": "2026-10-07T00:00:00Z"})
    result = _history.remove(adapter, [episode(20), episode(22), episode(21)])
    assert result["count"] == 3 and not result["unresolved"]
    attr = adapter.client.rows["1"]["attributes"]
    assert attr["progress"] == 7 and attr["status"] == "current"
    assert attr["finishedAt"] is None
    assert (attr["ratingTwenty"], attr["notes"], attr["private"]) == (18, "Keep", True)
    assert {item["episode"] for item in _history.build_index(adapter).values()} == set(range(13, 20))
    assert len(adapter.client.writes) == 2


@pytest.mark.parametrize("reset", [False, True])
def test_history_removal_batch_request_count(mapping, monkeypatch, reset):
    client = module.KitsuClient(mapping, "default")
    client._user = {"id": "7"}
    calls = []
    stored = {"id": "entry", "type": "libraryEntries", "attributes": {
        "progress": 100, "status": "completed", "ratingTwenty": 18, "notes": "Keep"}}

    def request(method, path, **kwargs):
        calls.append((method, path))
        if method == "GET" and path == "/library-entries":
            return {"data": [copy.deepcopy(stored)]}
        if method == "GET" and path == "/anime/1":
            return {"data": {"id": "1", "type": "anime", "attributes": {"subtype": "TV", "episodeCount": 100}}}
        assert method == "PATCH"
        stored["attributes"].update(kwargs["json"]["data"]["attributes"])
        return {"data": copy.deepcopy(stored)}

    monkeypatch.setattr(client, "request", request)
    items = [{"type": "episode", "show_ids": {"kitsu": "1"}, "season": 1, "episode": n}
             for n in range(1 if reset else 2, 101)]
    result = _history.remove(SimpleNamespace(raw_cfg=mapping, client=client), items)
    assert result["count"] == len(items) and not result["unresolved"]
    assert [method for method, _ in calls] == ["GET", "GET", "PATCH"] + (["PATCH"] if reset else [])
    assert stored["attributes"] == {"progress": 0 if reset else 1, "status": "on_hold" if reset else "current",
                                    "ratingTwenty": 18, "notes": "Keep", "finishedAt": None}


@pytest.mark.parametrize("failure", [None, "lookup", "media", "save", "reset_status"])
def test_grouped_removal_failures_duplicates_and_gaps(adapter, monkeypatch, failure):
    adapter.client.save("1", None, {"status": "current", "progress": 4})
    adapter.client.save("2", None, {"status": "completed", "progress": 1})
    if failure:
        method = "save" if failure == "reset_status" else failure
        original = getattr(adapter.client, method)

        def fail(ident, *args):
            if ident == "1" and (failure != "reset_status" or args[-1] == {"status": "on_hold"}):
                raise RuntimeError("request failed")
            return original(ident, *args)

        monkeypatch.setattr(adapter.client, method, fail)
    items = [episode(16), episode(16), episode(15), episode(13), movie()]
    if failure == "reset_status":
        items.append(episode(14))
    result = _history.remove(adapter, items)
    if failure:
        assert result["confirmed_keys"] == [canonical_key(movie())]
    else:
        assert set(result["confirmed_keys"]) == {canonical_key(item) for item in [episode(16), episode(15), movie()]}
        assert result["unresolved"][0]["reason"] == "kitsu_history_remove_would_erase_later_episodes"
        assert adapter.client.rows["1"]["attributes"]["progress"] == 2
    assert adapter.client.rows["2"]["attributes"]["progress"] == 0


def test_history_removal_rejects_gaps_and_does_not_confirm_failed_writes(adapter):
    adapter.client.save("1", None, {"status": "current", "progress": 4})
    result = _history.remove(adapter, [episode(14)])
    assert not result["confirmed_keys"]
    assert result["unresolved"][0]["reason"] == "kitsu_history_remove_would_erase_later_episodes"
    assert adapter.client.rows["1"]["attributes"]["progress"] == 4
    adapter.client.save = lambda *args: (_ for _ in ()).throw(RuntimeError("failure"))
    result = _history.remove(adapter, [episode(16), episode(15)])
    assert not result["confirmed_keys"] and len(result["unresolved"]) == 2
    assert adapter.client.rows["1"]["attributes"]["progress"] == 4


@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_history_reset_preserves_entry_and_is_idempotent(adapter, kind):
    ident = "2" if kind == "movie" else "1"
    items = [movie()] if kind == "movie" else [episode(13), episode(14), episode(15)]
    adapter.client.save(ident, None, {"status": "completed" if kind == "movie" else "current",
                                    "progress": 1 if kind == "movie" else 3, "ratingTwenty": 16, "notes": "Keep"})
    result = _history.remove(adapter, items)
    assert result["count"] == len(items)
    attr = adapter.client.rows[ident]["attributes"]
    assert attr["progress"] == 0 and attr["status"] == "on_hold"
    assert attr["ratingTwenty"] == 16 and attr["notes"] == "Keep"
    assert not _history.build_index(adapter)
    assert not _watchlist.build_index(adapter)
    writes = len(adapter.client.writes)
    assert _history.remove(adapter, items)["count"] == len(items)
    assert len(adapter.client.writes) == writes


def test_history_remove_missing_entry_unmapped_and_media_mismatch(adapter):
    assert _history.remove(adapter, [movie()])["count"] == 1
    assert not adapter.client.writes
    adapter.client.save("2", None, {"progress": 1, "status": "completed"})
    bad = {"type": "episode", "show_ids": {"kitsu": "2"}, "season": 1, "episode": 1}
    result = _history.remove(adapter, [bad, {"type": "movie", "ids": {"imdb": "tt000"}}])
    assert not result["confirmed_keys"] and len(result["unresolved"]) == 2
    assert adapter.client.rows["2"]["attributes"]["progress"] == 1


def test_history_remove_completed_total_and_unknown_total(adapter):
    adapter.client.save("1", None, {"progress": 0, "status": "completed"})
    assert _history.remove(adapter, [episode(22)])["count"] == 1
    assert adapter.client.rows["1"]["attributes"]["progress"] == 9
    adapter.client.save("1", None, {"progress": 3, "status": "completed"})
    adapter.client.media_rows["1"]["attributes"]["episodeCount"] = None
    result = _history.remove(adapter, [episode()])
    assert result["unresolved"][0]["reason"] == "kitsu_history_unknown_completed_total"
    assert adapter.client.rows["1"]["attributes"]["progress"] == 3


@pytest.mark.parametrize("dry_run", [False, True])
def test_history_remove_orders_mapped_episodes_before_orchestrator_chunks(mapping, monkeypatch, dry_run):
    from cw_platform.orchestrator._applier import apply_remove

    library = Library()
    library.save("1", None, {"status": "current", "progress": 3})
    monkeypatch.setattr(module, "KitsuClient", lambda *args: library)
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "99", "match_season": 1,
                     "episode_from": 1, "episode_to": 1, "episode_start_at": 3, "target_namespace": "kitsu", "target_id": "1"})
    custom = {"type": "episode", "show_ids": {"tvdb": "99"}, "season": 1, "episode": 1}
    result = apply_remove(dst_ops=module.OPS, cfg=mapping, dst_name="KITSU", feature="history",
                          items=[episode(13), episode(14), custom], dry_run=dry_run,
                          emit=lambda *a, **kw: None, dbg=lambda *a, **kw: None, chunk_size=1, chunk_pause_ms=0)
    assert result["count"] == (0 if dry_run else 3) and not result["unresolved"]
    assert result["attempted"] == 3
    assert library.rows["1"]["attributes"]["progress"] == (3 if dry_run else 0)
    if not dry_run:
        assert [attr["progress"] for _, attr in library.writes if "progress" in attr] == [3, 2, 1, 0]


def test_history_removal_runs_through_orchestrator(mapping, monkeypatch):
    from cw_platform.orchestrator.facade import Orchestrator
    from tests.test_anilist_history_orchestrator import FakeSource

    library = Library()
    watched = {**episode(), "watched": True, "watched_at": "2026-10-07T12:00:00Z"}
    source = FakeSource({canonical_key(episode(n)): {**watched, **episode(n)} for n in (13, 14, 15)})
    monkeypatch.setattr(module, "KitsuClient", lambda *a: library)
    monkeypatch.setattr(module.OPS, "health", lambda cfg: {"ok": True, "status": "ok", "features": module.supported_features(), "api": {}})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"JELLYFIN": source, "KITSU": module.OPS})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_unresolved_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_blackbox_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *a, **kw: set())
    cfg = {**mapping, "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0, "apply_chunk_size": 1},
           "sync": {"dry_run": False, "enable_add": True, "enable_remove": True, "allow_mass_delete": True},
           "pairs": [{"id": "kitsu-test", "enabled": True, "source": "JELLYFIN", "target": "KITSU", "mode": "one-way",
                      "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": True, "remove_mode": "mirror"}}}]}
    Orchestrator(cfg).run()
    assert library.rows["1"]["attributes"]["progress"] == 3
    source.index = {canonical_key(episode(13)): {**watched, **episode(13)}}
    Orchestrator(cfg).run()
    assert library.rows["1"]["attributes"]["progress"] == 1
    assert not source.add_calls



class StatusLibrary(Library):
    def save(self, ident, entry, attributes):
        before = copy.deepcopy(self.rows.get(ident, {}).get("attributes", {}))
        super().save(ident, entry, attributes)
        after = self.rows[ident]["attributes"]
        if before.get("progress") != after.get("progress") and before.get("status") == after.get("status"):
            after["status"] = "current"
        if any(after.get(key) != value for key, value in attributes.items()):
            raise RuntimeError("Kitsu returned different library values")


@pytest.fixture()
def status_adapter(mapping):
    from cw_platform.anime_mapping.service import config_with_pair_feature_options

    cfg = config_with_pair_feature_options(mapping, {"feature": "history", "use_source_status": True})
    return SimpleNamespace(raw_cfg=cfg, client=StatusLibrary())


@pytest.mark.parametrize("source_status", ["dropped", "on_hold"])
@pytest.mark.parametrize("existing_status", [None, "current", "dropped", "on_hold"])
def test_history_source_status_survives_progress_callback(status_adapter, source_status, existing_status):
    adapter = status_adapter
    if existing_status:
        adapter.client.save("1", None, {"progress": 1, "status": existing_status, "ratingTwenty": 18, "notes": "Keep"})
    result = _history.add(adapter, [{**episode(), "watch_status": source_status}])
    assert result["count"] == 1 and not result["unresolved"]
    attr = adapter.client.rows["1"]["attributes"]
    assert attr["progress"] == 3 and attr["status"] == source_status
    assert adapter.client.writes[-2:] == [("1", {"progress": 3, "status": "current"}), ("1", {"status": source_status})]
    if existing_status:
        assert attr["ratingTwenty"] == 18 and attr["notes"] == "Keep"
    writes = len(adapter.client.writes)
    assert _history.add(adapter, [{**episode(), "watch_status": source_status}])["count"] == 1
    assert len(adapter.client.writes) == writes


@pytest.mark.parametrize("source_status", ["watching", "planning", "completed", "unexpected"])
def test_history_source_status_does_not_guess_completion(status_adapter, source_status):
    adapter = status_adapter
    adapter.client.save("1", None, {"progress": 1, "status": "dropped"})
    assert _history.add(adapter, [{**episode(), "watch_status": source_status}])["count"] == 1
    assert adapter.client.rows["1"]["attributes"]["status"] == "current"


def test_history_source_status_missing_keeps_existing_hold_when_enabled(status_adapter):
    adapter = status_adapter
    adapter.client.save("1", None, {"progress": 1, "status": "on_hold"})
    assert _history.add(adapter, [episode()])["count"] == 1
    assert adapter.client.rows["1"]["attributes"]["status"] == "on_hold"


def test_history_source_status_is_opt_in_and_does_not_affect_scrobble(adapter):
    from cw_platform.anime_mapping.service import config_with_pair_feature_options

    item = {**episode(), "watch_status": "dropped"}
    assert _history.add(adapter, [item])["count"] == 1
    assert adapter.client.rows["1"]["attributes"]["status"] == "current"
    adapter.raw_cfg = config_with_pair_feature_options(adapter.raw_cfg, {"feature": "history", "use_source_status": False})
    assert _history.add(adapter, [{**episode(16), "watch_status": "dropped"}])["count"] == 1
    assert adapter.client.rows["1"]["attributes"]["status"] == "current"


@pytest.mark.parametrize("item", [movie(), episode(22)])
def test_history_source_status_does_not_override_completion(status_adapter, item):
    adapter = status_adapter
    assert _history.add(adapter, [{**item, "watch_status": "dropped"}])["count"] == 1
    ident = "2" if item["type"] == "movie" else "1"
    assert adapter.client.rows[ident]["attributes"]["status"] == "completed"


def test_history_source_status_does_not_change_completed_or_higher_progress(status_adapter):
    adapter = status_adapter
    for status in ("completed", "current"):
        adapter.client.save("1", None, {"progress": 10, "status": status})
        before = copy.deepcopy(adapter.client.rows)
        assert _history.add(adapter, [{**episode(), "watch_status": "on_hold"}])["count"] == 1
        assert adapter.client.rows == before


def test_history_source_status_failed_second_write_is_unresolved_and_retryable(status_adapter, monkeypatch):
    adapter = status_adapter
    save = adapter.client.save

    def fail_status(ident, entry, attributes):
        if attributes == {"status": "dropped"}:
            raise RuntimeError("status request failed")
        save(ident, entry, attributes)

    monkeypatch.setattr(adapter.client, "save", fail_status)
    item = {**episode(), "watch_status": "dropped"}
    result = _history.add(adapter, [item])
    assert not result["confirmed_keys"] and result["unresolved_keys"] == [canonical_key(item)]
    assert adapter.client.rows["1"]["attributes"]["progress"] == 3
    monkeypatch.setattr(adapter.client, "save", save)
    assert _history.add(adapter, [item])["count"] == 1
    assert adapter.client.rows["1"]["attributes"]["status"] == "dropped"


@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("status", ["dropped", "on_hold"])
def test_simkl_source_status_reaches_kitsu_through_orchestrator(mapping, monkeypatch, enabled, status):
    from cw_platform.orchestrator.facade import Orchestrator
    from tests.test_anilist_history_orchestrator import FakeSource

    library = StatusLibrary()
    watched = {**episode(), "watched": True, "watched_at": "2026-10-07T12:00:00Z"}
    source = FakeSource({canonical_key(watched): watched})
    from providers.sync.simkl import _history as simkl_history
    watched["show_ids"]["simkl"] = "100"
    monkeypatch.setattr(simkl_history, "_watch_status_by_record", lambda adapter: {"100": status})
    monkeypatch.setattr(source, "build_index", lambda cfg, **kw: simkl_history._with_watch_status(
        SimpleNamespace(raw_cfg=cfg), copy.deepcopy(source.index)))
    monkeypatch.setattr(source, "name", lambda: "SIMKL")
    monkeypatch.setattr(module, "KitsuClient", lambda *a: library)
    monkeypatch.setattr(module.OPS, "health", lambda cfg: {"ok": True, "status": "ok", "features": module.supported_features(), "api": {}})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SIMKL": source, "KITSU": module.OPS})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_unresolved_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_blackbox_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *a, **kw: set())
    cfg = {**mapping, "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"dry_run": False, "enable_add": True, "enable_remove": False},
           "pairs": [{"id": "kitsu-test", "enabled": True, "source": "SIMKL", "target": "KITSU", "mode": "one-way",
                      "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": False, "use_source_status": enabled}}}]}
    Orchestrator(cfg).run()
    assert library.rows["1"]["attributes"]["progress"] == 3
    assert library.rows["1"]["attributes"]["status"] == (status if enabled else "current")
    writes = len(library.writes)
    Orchestrator(cfg).run()
    assert len(library.writes) == writes
    assert not source.add_calls

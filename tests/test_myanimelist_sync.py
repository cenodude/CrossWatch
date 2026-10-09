# tests/test_myanimelist_sync.py
# CrossWatch - MyAnimeList sync, request budgets and application integration
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from dataclasses import replace
from types import SimpleNamespace

import pytest
import requests

from cw_platform.anime_mapping import storage
from cw_platform.id_map import canonical_key
from cw_platform.modules_registry import load_sync_ops, state_read_features, sync_provider_names
from providers.sync import _mod_MYANIMELIST as module
from providers.sync.myanimelist import _common, _history, _ratings, _watchlist


def media(ident=1, **values):
    return {"id": ident, "title": "Anime", "media_type": "tv", "num_episodes": 12,
            "status": "finished_airing", **values}


def status(**values):
    return {"status": "watching", "score": 0, "num_episodes_watched": 0, **values}


def episode(n=1):
    return {"type": "episode", "show_ids": {"mal": "1"}, "season": 1, "episode": n, "title": "Anime"}


def movie():
    return {"type": "movie", "ids": {"mal": "2"}, "title": "Movie"}


@pytest.fixture
def mapping(config_base):
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"mal:1": {"13-24": "1-12"}},
        "tmdb_movie:20": {"mal:2": {"1": "1"}},
    }), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return {"anime_mapping": {"enabled": True}, "myanimelist": {"access_token": "test", "user_id": "7"}}


@pytest.fixture
def adapter(mapping, monkeypatch):
    client = module.MyAnimeListClient(mapping, "default")
    client.rows = {"1": status(score=8, comments="keep"), "2": status(status="plan_to_watch")}
    client.calls = []

    def request(method, path, **kwargs):
        client.calls.append((method, path, copy.deepcopy(kwargs)))
        if path == "/users/@me/animelist":
            assert "list_status{" in kwargs["params"]["fields"]
            assert "comments" in kwargs["params"]["fields"]
            return {"data": [{"node": media(int(i), media_type="movie" if i == "2" else "tv"),
                               "list_status": copy.deepcopy(row)} for i, row in client.rows.items()], "paging": {}}
        ident = path.split("/")[2]
        if method == "GET":
            return media(int(ident), media_type="movie" if ident == "2" else "tv", my_list_status=copy.deepcopy(client.rows.get(ident)))
        if method == "DELETE":
            client.rows.pop(ident, None)
            return {}
        assert method == "PATCH"
        data = kwargs["data"]
        client.rows.setdefault(ident, status()).update({
            ("num_episodes_watched" if k == "num_watched_episodes" else k): v for k, v in data.items()})
        return copy.deepcopy(client.rows[ident])

    monkeypatch.setattr(client, "request", request)
    return SimpleNamespace(raw_cfg=mapping, client=client)


def test_registry():
    assert "MYANIMELIST" in sync_provider_names(upper=True)
    ops = load_sync_ops("MYANIMELIST")
    assert set(state_read_features(ops)) == {"watchlist", "ratings", "history"}
    assert ops.features()["progress"] is False
    assert ops.features()["playlists"] is False
    assert not ops.capabilities()["history"]["observed_deletes"]


def test_empty_named_profile_does_not_inherit_default_credentials():
    cfg = {"myanimelist": {"access_token": "default-token", "instances": {"P01": {}}}, "_cw_provider_instance": "P01"}
    assert not module.OPS.is_configured(cfg)


def test_mapping_offsets_and_overrides(mapping):
    from cw_platform.anime_mapping.overrides import upsert_override
    item = {**episode(15), "season": 3, "show_ids": {"tvdb": "10"}}
    assert _common.resolve_target(mapping, item) == ("1", 3)
    assert _common.resolve_target(mapping, {"type": "movie", "ids": {"tmdb": "20"}}) == ("2", 1)
    assert _common.resolve_target({}, item) is None
    upsert_override({"media_type": "show", "match_provider": "tvdb", "match_id": "10", "match_season": 3,
                     "episode_from": 15, "episode_to": 16, "episode_start_at": 1, "target_namespace": "mal", "target_id": "9"})
    assert _common.resolve_target(mapping, item) == ("9", 1)
    assert [row["episode"] for row in _common.episode_items(mapping, "9", 2, "Anime")] == [15, 16]


def test_reads_share_snapshot_and_do_not_invent_dates(adapter):
    adapter.client.rows["1"].update(num_episodes_watched=3, updated_at="2026-10-09T12:00:00Z")
    assert len(_watchlist.build_index(adapter)) == 1
    assert len(_ratings.build_index(adapter)) == 1
    history = _history.build_index(adapter)
    assert {row["episode"] for row in history.values()} == {13, 14, 15}
    assert not any("watched_at" in row for row in history.values())
    assert len(adapter.client.calls) == 1


def test_grouped_history_budget_and_preservation(adapter):
    result = _history.add(adapter, [episode(n) for n in range(1, 11)])
    assert result["count"] == 10 and not result["unresolved"]
    assert len(adapter.client.calls) == 2
    assert adapter.client.calls[-1][2]["data"] == {"num_watched_episodes": 10, "status": "watching"}
    assert adapter.client.rows["1"]["score"] == 8
    assert adapter.client.rows["1"]["comments"] == "keep"
    _history.add(adapter, [episode(5)])
    assert len(adapter.client.calls) == 2


def test_source_status_one_patch_and_completed_preservation(adapter):
    adapter.raw_cfg["_cw_pair_feature_options"] = {"feature": "history", "use_source_status": True}
    assert _history.add(adapter, [{**episode(3), "watch_status": "dropped"}])["count"] == 1
    assert adapter.client.rows["1"]["status"] == "dropped"
    assert len(adapter.client.calls) == 2
    _history.add(adapter, [episode(12)])
    assert adapter.client.rows["1"]["status"] == "completed"
    _history.add(adapter, [{**episode(3), "watch_status": "on_hold"}])
    assert adapter.client.rows["1"]["status"] == "completed"


def test_removal_tail_only_and_single_patch(adapter):
    adapter.client.rows["1"].update(num_episodes_watched=5)
    result = _history.remove(adapter, [episode(2)])
    assert result["unresolved"] and not result["count"]
    result = _history.remove(adapter, [episode(n) for n in (5, 4, 3)])
    assert result["count"] == 3
    assert adapter.client.rows["1"]["num_episodes_watched"] == 2
    assert adapter.client.rows["1"]["score"] == 8
    assert len(adapter.client.calls) == 2


def test_watchlist_preserves_other_features_and_unrate(adapter):
    adapter.client.rows["2"].update(score=9, comments="notes")
    assert _watchlist.remove(adapter, [movie()])["count"] == 1
    assert adapter.client.rows["2"]["status"] == "on_hold"
    assert adapter.client.rows["2"]["score"] == 9
    assert _watchlist.add(adapter, [movie()])["unresolved"]
    assert _ratings.remove(adapter, [movie()])["count"] == 1
    assert adapter.client.rows["2"]["score"] == 0
    assert adapter.client.rows["2"]["comments"] == "notes"


def test_watchlist_delete_empty_entry(adapter):
    assert _watchlist.remove(adapter, [movie()])["count"] == 1
    assert "2" not in adapter.client.rows


@pytest.mark.parametrize("feature", [_watchlist, _ratings, _history])
def test_non_anime_unresolved(adapter, feature):
    result = feature.add(adapter, [{"type": "movie", "ids": {"tmdb": "99999"}, "rating": 8}])
    assert result["unresolved"] and not result["count"]
    assert not adapter.client.calls


@pytest.mark.parametrize("failure", ["missing", "repeat", "empty", "http"])
def test_incomplete_snapshot_never_cached(monkeypatch, failure):
    ctx = SimpleNamespace()
    client = module.MyAnimeListClient({}, "default", ctx)
    calls = []

    def request(method, path, **kw):
        calls.append(kw["params"]["offset"])
        if len(calls) == 1:
            return {"data": [{"node": media(), "list_status": status()}], "paging": {"next": "ignored"}}
        if failure == "http":
            raise RuntimeError("failed page")
        if failure == "missing":
            return {"data": [{"node": media(2)}], "paging": {}}
        if failure == "empty":
            return {"data": [], "paging": {"next": "ignored"}}
        return {"data": [{"node": media(), "list_status": status()}], "paging": {}}

    monkeypatch.setattr(client, "request", request)
    with pytest.raises(RuntimeError):
        client.entries()
    assert calls == [0, 1]
    assert client._snapshot is None and not ctx._myanimelist_snapshots


def test_operation_snapshot_shared_not_global(adapter, monkeypatch):
    ctx = SimpleNamespace()
    first = module.MyAnimeListClient(adapter.raw_cfg, "default", ctx)
    monkeypatch.setattr(first, "request", adapter.client.request)
    first.entries()
    second = module.MyAnimeListClient(adapter.raw_cfg, "default", ctx)
    monkeypatch.setattr(second, "request", lambda *a, **kw: pytest.fail("extra read"))
    assert second.entries() == first.entries()
    assert module.MyAnimeListClient(adapter.raw_cfg, "default")._snapshot is None


def test_retry_after_and_request_count(monkeypatch, config_base):
    from services.interactive_sync_progress import SyncProgress
    progress = SyncProgress()
    ctx = SimpleNamespace(interactive=SimpleNamespace(on_progress=progress.event))
    client = module.MyAnimeListClient({}, "default", ctx)
    clock = [100.0]
    calls = []
    monkeypatch.setattr(module.time, "monotonic", lambda: clock[0])
    monkeypatch.setattr(module, "_sleep_cancellable", lambda n: clock.__setitem__(0, clock[0] + n))
    monkeypatch.setattr(module, "_NEXT_REQUEST", 0)

    def request(self, method, url, **kwargs):
        calls.append(clock[0])
        response = requests.Response()
        response.status_code = 429 if len(calls) < 3 else 200
        response.headers["Retry-After"] = "3"
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    assert module.transport(client.session, "GET", "https://api.myanimelist.net/v2/users/@me").status_code == 200
    assert calls == [100, 103, 106]
    assert progress.public()["requests_by_provider"] == {"MYANIMELIST": 3}


def test_long_cooldown_is_not_retried_early(monkeypatch):
    response = SimpleNamespace(status_code=429, headers={"Retry-After": "120"})
    calls = []
    monkeypatch.setattr(module, "_NEXT_REQUEST", 0)
    session = SimpleNamespace(request=lambda *a, **kw: calls.append(1) or response)
    assert module.transport(session, "GET", "url").status_code == 429
    with pytest.raises(RuntimeError, match="cooldown"):
        module.transport(session, "GET", "url")
    assert len(calls) == 1


def test_new_titles_batch_uses_one_read_and_skips_duplicate_writes(adapter):
    items = [{"type": "show", "ids": {"mal": str(i)}, "rating": 8} for i in range(3, 13)]
    assert _ratings.add(adapter, items + items)["count"] == 10
    assert len([c for c in adapter.client.calls if c[0] == "GET"]) == 1
    assert len([c for c in adapter.client.calls if c[0] == "PATCH"]) == 10


def test_editor_and_webhook_discovery(mapping):
    from api.editorAPI import _editor_send_targets
    from providers.webhooks.dispatch import _make_sink
    from providers.scrobble.myanimelist.sink import MyAnimeListSink

    for feature in ("watchlist", "ratings", "history"):
        assert any(row["provider"] == "MYANIMELIST" for row in _editor_send_targets(mapping, feature))
    assert not any(row["provider"] == "MYANIMELIST" for row in _editor_send_targets(mapping, "progress"))
    assert isinstance(_make_sink("myanimelist", "default", lambda: mapping), MyAnimeListSink)


def test_failed_confirmation_invalidates_snapshot(adapter, monkeypatch):
    adapter.client.entries()
    monkeypatch.setattr(adapter.client, "request", lambda *a, **kw: status(score=3))
    with pytest.raises(RuntimeError, match="confirm"):
        adapter.client.save("1", status(), {"score": 9})
    assert adapter.client._snapshot is None
    assert adapter.client._entries is None


def test_retry_after_http_date(monkeypatch):
    monkeypatch.setattr(module.time, "time", lambda: 0)
    assert module.retry_delay("Thu, 01 Jan 1970 00:00:07 GMT", 0) == 7


def test_ops_share_reads_count_writes_and_invalidate_after_write(mapping, monkeypatch):
    from cw_platform.orchestrator._pairs_utils import inject_ctx_into_provider, release_ctx_from_providers
    from services.interactive_sync_progress import SyncProgress

    mapping["myanimelist"]["expires_at"] = 9999999999
    monkeypatch.setattr(module.auth, "load_config", lambda: mapping)
    monkeypatch.setattr(module, "_NEXT_REQUEST", 0)
    monkeypatch.setattr(module, "_sleep_cancellable", lambda n: None)
    current = status(status="plan_to_watch", score=8)
    calls = []

    def request(session, method, url, **kwargs):
        calls.append(method)
        if method == "PATCH":
            current.update(kwargs["data"])
            data = dict(current)
        else:
            data = {"data": [{"node": media(2, media_type="movie"), "list_status": dict(current)}], "paging": {}}
        response = requests.Response()
        response.status_code = 200
        response._content = json.dumps(data).encode()
        return response

    monkeypatch.setattr(requests.Session, "request", request)
    progress = SyncProgress()
    ctx = SimpleNamespace(interactive=SimpleNamespace(on_progress=progress.event))
    inject_ctx_into_provider(module.OPS, ctx)
    try:
        assert module.OPS.build_index(mapping, feature="watchlist")
        assert module.OPS.build_index(mapping, feature="ratings")
        assert calls == ["GET"]
        assert module.OPS.add(mapping, [{**movie(), "rating": 9}], feature="ratings")["count"] == 1
        assert calls == ["GET", "PATCH"]
        assert module.OPS.build_index(mapping, feature="ratings")
        assert calls == ["GET", "PATCH", "GET"]
        assert progress.public()["requests_by_provider"] == {"MYANIMELIST": 3}
    finally:
        release_ctx_from_providers(ctx)

@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_scrobble_watched_gate_profile_and_write(mapping, monkeypatch, kind):
    from providers.scrobble.myanimelist import sink
    from providers.scrobble.scrobble import ScrobbleEvent

    writes = []
    monkeypatch.setattr(sink, "record_watch", lambda *a, **kw: None)
    monkeypatch.setattr(sink, "record_scrobble_event", lambda *a, **kw: None)
    monkeypatch.setattr(module.MYANIMELISTModule, "add", lambda self, feature, items: writes.append((self.instance_id, feature, items)) or {"count": 1})
    cfg = {**mapping, "myanimelist": {"instances": {"P01": {"access_token": "t"}}}}
    monkeypatch.setattr(module.auth, "load_config", lambda: cfg)
    target = sink.MyAnimeListSink(instance_id="P01")
    ev = ScrobbleEvent(action="stop", media_type=kind, ids={"mal": "2", "tvdb_show": "10"}, title="Anime", year=2020,
                      season=3, number=15, progress=40, account="test", server_uuid="s", session_key="1", raw={})
    assert target.send(ev, cfg)["skipped"]
    ev = replace(ev, progress=95)
    assert target.send(ev, cfg)["ok"]
    assert writes[0][0:2] == ("P01", "history")
    assert writes[0][2][0]["type"] == kind


def test_watcher_and_webhook_factories_accept_myanimelist_profile():
    from providers.scrobble.myanimelist.sink import MyAnimeListSink
    from providers.scrobble.watch_manager import _make_sink
    from providers.webhooks.config import sink_configured

    cfg = {"myanimelist": {"instances": {"P01": {"access_token": "token"}}}}
    assert isinstance(_make_sink("myanimelist", lambda: cfg, "P01"), MyAnimeListSink)
    assert sink_configured(cfg, "myanimelist", "P01")


def test_capture_and_sync_api_discover_myanimelist(mapping, monkeypatch, config_base):
    from api.syncAPI import api_sync_providers
    from services import snapshots

    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: mapping)
    providers = json.loads(api_sync_providers().body)
    provider = next(row for row in providers if row["name"] == "MYANIMELIST")
    assert provider["version"] == "0.1" and provider["configured"]
    assert provider["features"]["history"] and not provider["features"]["progress"]
    monkeypatch.setattr(snapshots, "CONFIG", config_base)
    monkeypatch.setattr(module.MYANIMELISTModule, "build_index", lambda self, feature: {canonical_key(movie()): movie()})
    result = snapshots.create_snapshot("MYANIMELIST", "watchlist", cfg=mapping)
    assert result["ok"]
    loaded = snapshots.read_snapshot(result["path"])
    assert loaded["stats"]["count"] == 1


def test_insights_include_myanimelist_profiles_and_native_anime_ids(config_base, monkeypatch):
    from fastapi import FastAPI
    from fastapi.testclient import TestClient
    from api import insightAPI

    cfg = {"myanimelist": {"instances": {"P01": {"access_token": "test-token"}}}}
    assert insightAPI._settings_auth_summary(cfg) == {
        "configured": 1, "profiles": [{"provider": "myanimelist", "count": 1}]
    }
    items = {f"mal:{ident}": {"type": "show", "ids": {"mal": ident}} for ident in ("1", "2")}
    state = {"providers": {"MYANIMELIST": {"instances": {"P01": {
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
    assert feature["providers"]["myanimelist"] == 2
    assert feature["providers_instances"]["myanimelist"]["P01"] == 2
    assert feature["providers_mse"]["myanimelist"]["anime"] == 2



@pytest.mark.parametrize("preexisting", [False, True])
@pytest.mark.parametrize("interactive", [False, True])
@pytest.mark.parametrize("enabled", [False, True])
@pytest.mark.parametrize("status", ["dropped", "on_hold"])
def test_simkl_source_status_reaches_myanimelist_through_orchestrator(adapter, mapping, monkeypatch, enabled, status, interactive, preexisting):
    from cw_platform.orchestrator.facade import Orchestrator
    from tests.test_anilist_history_orchestrator import FakeSource

    library = adapter.client
    if preexisting:
        library.rows["1"].update(num_episodes_watched=3, status=status if enabled else "watching")
    watched = {**episode(15), "season": 3, "show_ids": {"tvdb": "10"}, "watched": True, "watched_at": "2026-10-07T12:00:00Z"}
    source = FakeSource({canonical_key(watched): watched})
    from providers.sync.simkl import _history as simkl_history
    watched["show_ids"]["simkl"] = "100"
    monkeypatch.setattr(simkl_history, "_watch_status_by_record", lambda adapter: {"100": status})
    monkeypatch.setattr(source, "build_index", lambda cfg, **kw: simkl_history._with_watch_status(
        SimpleNamespace(raw_cfg=cfg), copy.deepcopy(source.index)))
    monkeypatch.setattr(source, "name", lambda: "SIMKL")
    monkeypatch.setattr(module, "MyAnimeListClient", lambda *a: library)
    monkeypatch.setattr(module.OPS, "health", lambda cfg: {"ok": True, "status": "ok", "features": module.supported_features(), "api": {}})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SIMKL": source, "MYANIMELIST": module.OPS})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_unresolved_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_oneway.load_blackbox_keys", lambda *a, **kw: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *a, **kw: set())
    cfg = {**mapping, "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"dry_run": False, "enable_add": True, "enable_remove": False},
           "pairs": [{"id": "myanimelist-test", "enabled": True, "source": "SIMKL", "target": "MYANIMELIST", "mode": "one-way",
                      "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": False, "use_source_status": enabled}}}]}
    if interactive:
        from cw_platform.orchestrator._interactive import InteractivePlan
        plan = InteractivePlan()
        Orchestrator(cfg, interactive=plan).run(dry_run=True, write_state_json=False)
        assert bool(plan.rows) is (not preexisting)
        assert not any(call[0] == "PATCH" for call in library.calls)
        Orchestrator(cfg, interactive=InteractivePlan(preview=False, selected=set(plan.rows))).run()
    else:
        result = Orchestrator(cfg).run()
        if preexisting:
            assert result["added"] == 0
    assert library.rows["1"]["num_episodes_watched"] == 3
    assert library.rows["1"]["status"] == (status if enabled else "watching")
    writes = len([call for call in library.calls if call[0] == "PATCH"])
    repeated = Orchestrator(cfg).run()
    assert repeated["added"] == 0
    assert len([call for call in library.calls if call[0] == "PATCH"]) == writes
    assert not source.add_calls


@pytest.mark.parametrize("source", ["anilist", "kitsu", "myanimelist"])
@pytest.mark.parametrize("target", ["anilist", "kitsu", "myanimelist", "simkl"])
@pytest.mark.parametrize("enabled", [False, True])
def test_anime_tracker_history_transfers_source_status(adapter, mapping, monkeypatch, source, target, enabled):
    from providers.sync.anilist import _history as anilist_history
    from providers.sync.kitsu import _history as kitsu_history
    from providers.sync.simkl import _history as simkl_history
    from tests.test_kitsu import Library

    paths = storage.paths("v3")
    paths["mappings"].write_text(json.dumps({
        "tvdb_show:10:s3": {"mal:1": {"13-24": "1-12"}, "anilist:100": {"13-24": "1-12"}, "kitsu:1": {"13-24": "1-12"}},
    }), encoding="utf-8")
    paths["identity"].write_text("anidb\tmyanimelist\tanilist\tsimkl\tkitsu\n\t1\t100\t\t1\n", encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    cfg = {**mapping, "_cw_pair_feature_options": {"feature": "history", "use_source_status": enabled}}
    media_row = {"id": 100, "format": "TV", "episodes": 12, "status": "FINISHED", "title": {"english": "Anime"}}
    entry = {"id": 9, "status": "PAUSED", "progress": 3, "updatedAt": 1785000000, "media": media_row}
    if source == "anilist":
        client = SimpleNamespace(viewer=lambda: {"id": 1}, gql=lambda *a, **kw: {"MediaListCollection": {"lists": [{"entries": [entry]}]}})
        index = anilist_history.build_index(SimpleNamespace(raw_cfg=cfg, client=client))
    elif source == "kitsu":
        client = Library()
        client.rows["1"] = {"id": "1", "attributes": {"status": "on_hold", "progress": 3}}
        index = kitsu_history.build_index(SimpleNamespace(raw_cfg=cfg, client=client))
    else:
        client = SimpleNamespace(entries=lambda: [(status(status="on_hold", num_episodes_watched=3), media())])
        index = _history.build_index(SimpleNamespace(raw_cfg=cfg, client=client))
    items = list(index.values())
    assert len(items) == 3 and {item["watch_status"] for item in items} == {"on_hold"}
    if target == "myanimelist":
        adapter.raw_cfg = cfg
        assert _history.add(adapter, items)["count"] == 3
        assert adapter.client.rows["1"]["status"] == ("on_hold" if enabled else "watching")
    elif target == "kitsu":
        client = Library()
        assert kitsu_history.add(SimpleNamespace(raw_cfg=cfg, client=client), items)["count"] == 3
        assert client.rows["1"]["attributes"]["status"] == ("on_hold" if enabled else "current")
    elif target == "anilist":
        writes = []
        def gql(query, variables, **kwargs):
            if "SaveMediaListEntry" in query:
                writes.append(variables)
                return {"SaveMediaListEntry": {"id": 9}}
            return {"MediaListCollection": {"lists": [{"entries": [{**entry, "status": "CURRENT", "progress": 0}]}]}}
        client = SimpleNamespace(viewer=lambda: {"id": 1}, gql=gql)
        result = anilist_history.add_detailed(SimpleNamespace(raw_cfg=cfg, client=client, key_of=canonical_key), items)
        assert result["count"] == 3 and not result["unresolved"]
        assert writes[0]["status"] == ("PAUSED" if enabled else "CURRENT")
    else:
        from unittest.mock import Mock
        from tests.test_simkl_history_batch_confirmation import _response
        session = SimpleNamespace(get=Mock(return_value=_response({})), post=Mock(return_value=_response({"added": {"episodes": 3}, "not_found": {}})))
        monkeypatch.setattr(simkl_history, "_headers", lambda *a, **kw: {})
        monkeypatch.setattr(simkl_history, "_anibridge_config", lambda *a, **kw: None)
        monkeypatch.setattr(simkl_history, "_inject_adds_into_cache", lambda *a: None)
        monkeypatch.setattr(simkl_history, "_remember_source_aliases", lambda *a: None)
        dest = SimpleNamespace(raw_cfg=cfg, config={}, cfg=SimpleNamespace(timeout=5), client=SimpleNamespace(session=session))
        count, unresolved = simkl_history.add(dest, items)
        assert count == 3 and not unresolved
        assert session.post.call_args.kwargs["json"]["shows"][0].get("status") == ("hold" if enabled else None)


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_undated_mal_history_can_be_used_as_source(adapter, mapping, monkeypatch, mode):
    from cw_platform.orchestrator.facade import Orchestrator
    from tests.test_anilist_history_orchestrator import FakeSource

    adapter.client.rows["1"].update(num_episodes_watched=3)
    target = FakeSource({})
    monkeypatch.setattr(target, "name", lambda: "SIMKL")
    monkeypatch.setattr(module, "MyAnimeListClient", lambda *a: adapter.client)
    monkeypatch.setattr(module.OPS, "health", lambda cfg: {"ok": True, "status": "ok", "features": module.supported_features(), "api": {}})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SIMKL": target, "MYANIMELIST": module.OPS})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *a: True)
    cfg = {**mapping, "runtime": {"snapshot_ttl_sec": 0, "apply_chunk_pause_ms": 0},
           "sync": {"dry_run": False, "enable_add": True, "enable_remove": False},
           "pairs": [{"id": "undated-source", "enabled": True, "source": "MYANIMELIST", "target": "SIMKL", "mode": mode,
                      "feature": "history", "features": {"history": {"enable": True, "add": True, "remove": False}}}]}
    Orchestrator(cfg).run()
    sent = [item for batch in target.add_calls for item in batch]
    assert len(sent) == 3
    assert {item["episode"] for item in sent} == {13, 14, 15}
    assert all(item["watched"] is True and not item.get("watched_at") for item in sent)


def test_history_filters_keep_explicit_undated_watches_but_not_synthetic_rows():
    from cw_platform.orchestrator._history_rewatches import filter_history_events
    from cw_platform.orchestrator._snapshots import _eventish_count

    rows = {
        "tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}, "watched": True, "_cw_watched_state": True},
        "tvdb:10#s01e01": {"type": "episode", "show_ids": {"tvdb": "10"}, "season": 1, "episode": 1, "watched": True, "_cw_watched_state": True},
        "tmdb:2": {"type": "movie", "ids": {"tmdb": "2"}},
        "tmdb:3": {"type": "movie", "ids": {"tmdb": "3"}, "watched": False},
        "tvdb:20": {"type": "show", "ids": {"tvdb": "20"}, "watched": True, "_cw_watched_state": True},
    }
    assert set(filter_history_events(rows, event_mode=False)) == {"tmdb:1", "tvdb:10#s01e01"}
    assert filter_history_events(rows, event_mode=True) == {}
    assert _eventish_count("history", rows) == 2

# tests/test_wetrakr_ratings_progress.py
# CrossWatch - WeTrakr ratings, resume progress and playback integration tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy

import pytest

from test_wetrakr_sync import EPISODE, LATER, MOVIE, SHOW, WHEN, Response, env, item
from providers.sync.wetrakr import _common as common
from services.playback_progress.adapters.wetrakr import WeTrakrPlaybackAdapter

SEASON = {"id": 135905, "type": "season", "number": 1, "title": "Season 1", "show": SHOW}


@pytest.fixture
def features(env):
    env.ratings = {}
    env.playing = {}
    env.reject_write = False
    env.server.activities["ratings"] = {"all": WHEN, "last_updated_at": WHEN, "last_removed_at": WHEN}

    def hook(method, path, kwargs):
        if path == "/media/external/tmdb/1396":
            assert kwargs["params"] == {"type": "show"}
            return Response({"id": SHOW["id"], "type": "show"})
        if path == f"/shows/{SHOW['id']}/seasons/1/episodes/1":
            return Response({**EPISODE, "media_id": SHOW["id"]})
        if path == f"/shows/{SHOW['id']}/seasons/1":
            return Response({**SEASON, "media_id": SHOW["id"]})
        ratings = path.startswith("/sync/ratings")
        playing = path.startswith("/sync/tracking/playing/")
        if method == "GET" and (ratings or playing):
            kind = path.rsplit("/", 1)[-1][:-1]
            rows = [copy.deepcopy(row) for row in (env.ratings if ratings else env.playing).values() if row["type"] == kind]
            return Response(rows, headers={"X-Pagination-Page": "1", "X-Pagination-Page-Count": "1", "X-Pagination-Item-Count": str(len(rows))})
        if method == "POST" and ratings:
            if not env.reject_write:
                for group, rows in kwargs["json"].items():
                    for row in rows:
                        media = SEASON if group == "seasons" and row.get("id") == SEASON["id"] else env.server.resolve(group, row)
                        assert media is not None
                        if path.endswith("/remove"):
                            env.ratings.pop(media["id"], None)
                        else:
                            env.ratings[media["id"]] = {**media, "interactions": {"user": {"rating": {"rating": row["rating"], "rated_at": WHEN}}}}
            return Response({"added": {"total": 1}})
        if method == "POST" and path == "/scrobble/pause":
            body = kwargs["json"]
            if "movie" in body:
                media = MOVIE
                assert body["movie"]["ids"]["tmdb"] == 155
            else:
                media = EPISODE
                assert body["show"]["ids"]["tmdb"] == 1396
                assert body["episode"] == {"season": 1, "number": 1}
            if not env.reject_write:
                if body["progress"] == 0 and media["id"] in env.playing:
                    env.playing.pop(media["id"])
                else:
                    env.playing[media["id"]] = {**media, "playback": {"status": "paused", "progress_percent": body["progress"],
                                                                   "runtime_seconds": 6000, "tracked_at": LATER}}
            return Response({"action": "pause", "progress": body["progress"]})
        return None

    env.server.hook = hook
    return env


@pytest.mark.parametrize("media", [MOVIE, SHOW, SEASON, EPISODE])
def test_ratings_read_update_repeat_and_remove(features, media):
    source = {**item(media), "rating": 8}
    assert features.adapter.add("ratings", [source])["ok"]
    index = features.adapter.build_index("ratings", force_refresh=True)
    assert next(iter(index.values()))["rating"] == 8
    assert features.adapter.add("ratings", [source])["ok"]
    assert len([call for call in features.server.calls if call[0] == "POST"]) == 1
    assert features.adapter.add("ratings", [{**source, "rating": 6}])["ok"]
    assert features.ratings[media["id"]]["interactions"]["user"]["rating"]["rating"] == 6
    assert features.adapter.remove("ratings", [item(media)])["ok"]
    assert not features.ratings
    assert not features.server.history and not features.server.planning


def test_episode_rating_resolves_show_and_episode_without_confusing_ids(features):
    source = {**item(EPISODE), "rating": 7, "ids": {"tmdb": "62085"}, "show_ids": {"tmdb": "1396"}}
    assert features.adapter.add("ratings", [source])["ok"]
    payload = next(kw["json"] for method, _, kw in features.server.calls if method == "POST")
    assert payload == {"episodes": [{"id": EPISODE["id"], "rating": 7}]}
    assert len([path for _, path, _ in features.server.calls if path.startswith("/media/external/")]) == 1


def test_season_rating_resolves_by_show_id_and_season_number(features):
    source = {"type": "season", "season": 1, "show_ids": {"tmdb": "1396"}, "rating": 8}
    assert features.adapter.add("ratings", [source])["ok"]
    payload = next(kw["json"] for method, _, kw in features.server.calls if method == "POST")
    assert payload == {"seasons": [{"id": SEASON["id"], "rating": 8}]}


@pytest.mark.parametrize("rating", [0, 11, True, None, "8.5", float("nan")])
def test_invalid_rating_does_not_call_api(features, rating):
    result = features.adapter.add("ratings", [{**item(), "rating": rating}])
    assert not result["ok"] and result["unresolved"][0]["reason"] == "invalid_rating"
    assert not features.server.calls


def test_rating_response_counters_do_not_confirm_wrong_value(features):
    assert features.adapter.add("ratings", [{**item(), "rating": 8}])["ok"]
    features.reject_write = True
    result = features.adapter.add("ratings", [{**item(), "rating": 5}])
    assert not result["ok"] and result["confirmed_keys"] == []


def test_ratings_500_items_use_one_bulk_write_and_one_verification(features, monkeypatch):
    media = [{"id": i, "type": "movie", "ids": {"tmdb": i}} for i in range(1, 501)]
    by_id = {row["id"]: row for row in media}
    monkeypatch.setattr(features.server, "resolve", lambda group, row: copy.deepcopy(by_id.get(row.get("id"))))
    reads = []
    original = features.adapter.build_index

    def read(*args, **kwargs):
        reads.append(args)
        return original(*args, **kwargs)

    monkeypatch.setattr(features.adapter, "build_index", read)
    result = features.adapter.add("ratings", [{**item(row), "rating": 7} for row in media])
    assert result["ok"] and len(result["confirmed_keys"]) == 500
    assert len(reads) == 2
    assert len([method for method, _, _ in features.server.calls if method == "POST"]) == 1


def test_wrong_episode_lookup_does_not_write(features):
    original = features.server.hook

    def hook(method, path, kwargs):
        if "/seasons/1/episodes/1" in path:
            return Response({**EPISODE, "media_id": 999, "season_number": 2})
        return original(method, path, kwargs)

    features.server.hook = hook
    source = {**item(EPISODE), "rating": 7, "ids": {}, "show_ids": {"tmdb": "1396"}}
    result = features.adapter.add("ratings", [source])
    assert not result["ok"] and result["unresolved"][0]["reason"] == "child_not_resolved"
    assert not any(method == "POST" for method, _, _ in features.server.calls)


def test_rating_activity_delta_and_removal_refresh(features):
    assert features.adapter.add("ratings", [{**item(), "rating": 8}])["ok"]
    features.server.calls.clear()
    features.adapter.build_index("ratings")
    assert [path for _, path, _ in features.server.calls] == ["/sync/last_activities"]
    features.server.activities["ratings"].update(all=LATER, last_updated_at=LATER)
    features.ratings[MOVIE["id"]]["interactions"]["user"]["rating"]["rating"] = 6
    assert next(iter(features.adapter.build_index("ratings").values()))["rating"] == 6
    assert any(kw.get("params", {}).get("from_date") for _, _, kw in features.server.calls)
    features.server.calls.clear()
    features.server.activities["ratings"].update(all="2026-09-18T21:09:53Z", last_removed_at="2026-09-18T21:09:53Z")
    features.ratings.clear()
    assert features.adapter.build_index("ratings") == {}
    assert not any(kw.get("params", {}).get("from_date") for _, _, kw in features.server.calls)


def progress_item(media=MOVIE, percent=40, when=LATER):
    return {**item(media), "progress_ms": percent * 60000, "duration_ms": 6000000, "progress_at": when}


@pytest.mark.parametrize("media", [MOVIE, EPISODE])
def test_progress_read_write_repeat_without_watched_side_effect(features, media):
    result = features.adapter.add("progress", [progress_item(media)])
    assert result["ok"] and len(result["confirmed_keys"]) == 1
    index = features.adapter.build_index("progress")
    row = next(iter(index.values()))
    assert row["progress_ms"] == 2400000 and row["duration_ms"] == 6000000
    result = features.adapter.add("progress", [progress_item(media)])
    assert result["ok"] and result["skipped"] == 1
    assert len([call for call in features.server.calls if call[0] == "POST"]) == 1
    assert not features.server.history


@pytest.mark.parametrize("state,when,reason", [("playing", WHEN, "active_session"), ("paused", "2026-09-25T21:09:53Z", "target_newer")])
def test_progress_does_not_overwrite_active_or_newer_playback(features, state, when, reason):
    features.playing[MOVIE["id"]] = {**MOVIE, "playback": {"status": state, "tracked_at": when, "progress_percent": 30, "runtime_seconds": 6000}}
    result = features.adapter.add("progress", [progress_item()])
    assert result["ok"] and result["results"][0]["reason"] == reason
    assert not any(method == "POST" for method, _, _ in features.server.calls)


def test_progress_same_profile_does_not_write(features, monkeypatch):
    monkeypatch.setenv("CW_PAIR_SRC", "WETRAKR")
    monkeypatch.setenv("CW_PAIR_DST", "WETRAKR")
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "P01")
    monkeypatch.setenv("CW_PAIR_DST_INSTANCE", "P01")
    result = features.adapter.add("progress", [progress_item()])
    assert result["results"][0]["reason"] == "same_origin"
    assert not any(method == "POST" for method, _, _ in features.server.calls)


@pytest.mark.parametrize("percent", [0, 100, -1, float("inf"), float("nan")])
def test_invalid_progress_does_not_call_api(features, percent):
    assert not features.adapter.add("progress", [progress_item(percent=percent)])["ok"]
    assert not features.server.calls


def test_progress_success_response_must_be_verified(features):
    features.reject_write = True
    result = features.adapter.add("progress", [progress_item()])
    assert not result["ok"] and not result["confirmed_keys"]


@pytest.mark.parametrize("feature", ["ratings", "progress"])
def test_new_features_report_quota_failure_without_confirmation(features, feature):
    original = features.server.hook

    def hook(method, path, kwargs):
        if method == "POST":
            return Response({}, 429, {"Retry-After": "30"})
        return original(method, path, kwargs)

    features.server.hook = hook
    result = features.adapter.add(feature, [{**progress_item(), "rating": 8}])
    assert not result["ok"] and not result["confirmed_keys"]
    assert result["unresolved_keys"] == ["tmdb:155"]


def test_progress_without_duration_preserves_percentage(features):
    features.playing[MOVIE["id"]] = {**MOVIE, "playback": {"status": "paused", "progress_percent": 40, "tracked_at": WHEN}}
    row = next(iter(features.adapter.build_index("progress").values()))
    assert row["progress_percent"] == 40 and "duration_ms" not in row and "progress_ms" not in row
    result = features.adapter.add("progress", [{**item(), "progress_percent": 55, "progress_at": LATER}])
    assert result["ok"] and len(result["confirmed_keys"]) == 1


def test_progress_malformed_playback_is_not_an_empty_snapshot(features):
    features.playing[MOVIE["id"]] = {**MOVIE, "playback": {"status": "paused", "progress_percent": "invalid", "tracked_at": WHEN}}
    with pytest.raises(common.WeTrakrSyncError, match="invalid_playback"):
        features.adapter.build_index("progress")


@pytest.mark.parametrize("feature", ["ratings", "progress"])
def test_new_features_dry_run_does_not_call_api(features, feature):
    source = {**progress_item(), "rating": 8}
    assert features.adapter.add(feature, [source], dry_run=True)["ok"]
    assert not features.server.calls


@pytest.mark.parametrize("media", [MOVIE, EPISODE])
def test_progress_removal_and_repeat_do_not_recreate_playback(features, media):
    assert features.adapter.add("progress", [progress_item(media)])["ok"]
    features.server.calls.clear()
    result = features.adapter.remove("progress", [progress_item(media)])
    assert result["ok"] and len(result["confirmed_keys"]) == 1
    assert not features.playing and not features.server.history
    assert features.adapter.remove("progress", [progress_item(media)])["ok"]
    posts = [(path, kwargs) for method, path, kwargs in features.server.calls if method == "POST"]
    assert len(posts) == 1 and posts[0][0] == "/scrobble/pause" and posts[0][1]["json"]["progress"] == 0
    assert not features.playing


def test_progress_removal_aliases_write_only_once(features):
    assert features.adapter.add("progress", [progress_item()])["ok"]
    features.server.calls.clear()
    result = features.adapter.remove("progress", [progress_item(), {**progress_item(), "ids": {"imdb": "tt0468569"}}])
    assert result["ok"] and len(result["confirmed_keys"]) == 2
    assert not features.playing
    assert sum(method == "POST" for method, _, _ in features.server.calls) == 1


@pytest.mark.parametrize("state,when,reason", [("playing", WHEN, "active_session"), ("paused", "2099-01-01T00:00:00Z", "target_newer")])
def test_progress_removal_protects_active_and_newer_entries(features, state, when, reason):
    features.playing[MOVIE["id"]] = {**MOVIE, "playback": {"status": state, "tracked_at": when, "progress_percent": 30}}
    result = features.adapter.remove("progress", [progress_item()])
    assert result["ok"] and result["results"][0]["reason"] == reason and not result["confirmed_keys"]
    assert not any(method == "POST" for method, _, _ in features.server.calls)


def test_progress_removal_same_origin_is_skipped(features, monkeypatch):
    assert features.adapter.add("progress", [progress_item()])["ok"]
    features.server.calls.clear()
    monkeypatch.setenv("CW_PAIR_SRC", "WETRAKR")
    monkeypatch.setenv("CW_PAIR_DST", "WETRAKR")
    result = features.adapter.remove("progress", [progress_item()])
    assert result["results"][0]["reason"] == "same_origin"
    assert not any(method == "POST" for method, _, _ in features.server.calls)


def test_progress_removal_dry_run_has_no_api_calls(features):
    assert features.adapter.remove("progress", [progress_item()], dry_run=True)["ok"]
    assert not features.server.calls


def test_progress_removal_rejected_write_is_not_confirmed(features):
    assert features.adapter.add("progress", [progress_item()])["ok"]
    features.reject_write = True
    result = features.adapter.remove("progress", [progress_item()])
    assert not result["ok"] and not result["confirmed_keys"] and features.playing


def test_progress_removal_zero_entry_is_not_confirmed_as_absent(features):
    assert features.adapter.add("progress", [progress_item()])["ok"]
    original = features.server.hook

    def hook(method, path, kwargs):
        if method == "POST":
            features.playing[MOVIE["id"]]["playback"]["progress_percent"] = 0
            return Response({"action": "pause", "progress": 0})
        return original(method, path, kwargs)

    features.server.hook = hook
    result = features.adapter.remove("progress", [progress_item()])
    assert not result["ok"] and not result["confirmed_keys"]


def test_progress_removal_batch_shares_preflight_and_verification(features, monkeypatch):
    sources = [progress_item(media) for media in (MOVIE, EPISODE)]
    assert features.adapter.add("progress", sources)["ok"]
    reads = []
    original = features.adapter.build_index

    def read(*args, **kwargs):
        reads.append((args, kwargs))
        return original(*args, **kwargs)

    monkeypatch.setattr(features.adapter, "build_index", read)
    result = features.adapter.remove("progress", sources)
    assert result["ok"] and len(result["confirmed_keys"]) == 2 and not features.playing
    assert len(reads) == 2 and all(kwargs.get("force_refresh") for _, kwargs in reads)


@pytest.mark.parametrize("applied", [True, False])
def test_progress_removal_uncertain_response_reads_back_without_retry(features, applied):
    import requests

    assert features.adapter.add("progress", [progress_item()])["ok"]
    original = features.server.hook

    def hook(method, path, kwargs):
        if method == "POST":
            if applied:
                original(method, path, kwargs)
            raise requests.ConnectionError("Response lost")
        return original(method, path, kwargs)

    features.server.hook = hook
    features.server.calls.clear()
    result = features.adapter.remove("progress", [progress_item()])
    assert result["ok"] is applied
    assert bool(result["confirmed_keys"]) is applied
    assert sum(method == "POST" for method, _, _ in features.server.calls) == 1


def test_progress_removal_failed_readback_can_retry_without_recreating(features):
    assert features.adapter.add("progress", [progress_item()])["ok"]
    original = features.server.hook
    wrote = False

    def hook(method, path, kwargs):
        nonlocal wrote
        if method == "POST":
            wrote = True
        elif wrote and "/playing/" in path:
            return Response({}, 503)
        return original(method, path, kwargs)

    features.server.hook = hook
    features.server.calls.clear()
    result = features.adapter.remove("progress", [progress_item()])
    assert not result["ok"] and not result["confirmed_keys"] and not features.playing
    features.server.hook = original
    assert features.adapter.remove("progress", [progress_item()])["ok"]
    assert sum(method == "POST" for method, _, _ in features.server.calls) == 1


def test_playback_profile_listing_update_and_mark_watched(features):
    assert features.adapter.add("progress", [progress_item(EPISODE)])["ok"]
    adapter = WeTrakrPlaybackAdapter()
    args = {"instance_id": "P01", "instance_label": "Test"}
    caps = adapter.capabilities(features.view, **args)
    assert caps.configured and caps.update_progress and caps.remove_progress and caps.bulk_remove_progress
    listed = adapter.list_progress(features.view, **args)
    assert listed.ok and len(listed.items) == 1
    record = listed.items[0].to_dict()
    assert record["instance_id"] == "P01" and record["season"] == 1 and record["episode"] == 1
    assert record["duration_seconds"] == 6000 and record["remaining_seconds"] == 3600
    assert adapter.update_progress(features.view, record, 60, **args).ok
    assert features.playing[EPISODE["id"]]["playback"]["progress_percent"] == 60
    assert adapter.mark_watched(features.view, record, watched_at=WHEN, **args).ok
    assert len(features.server.history) == 1
    assert adapter.remove_progress(features.view, record, **args).ok
    assert not features.playing and len(features.server.history) == 1


def test_playback_service_discovers_wetrakr():
    from services.playback_progress.service import PHASE1_PROVIDERS, PlaybackProgressService
    service = PlaybackProgressService()
    assert "wetrakr" in PHASE1_PROVIDERS
    assert isinstance(service.adapters["wetrakr"], WeTrakrPlaybackAdapter)


def test_playback_active_entry_disables_and_skips_removal(features):
    features.playing[MOVIE["id"]] = {**MOVIE, "playback": {"status": "playing", "progress_percent": 40, "tracked_at": WHEN}}
    adapter = WeTrakrPlaybackAdapter()
    args = {"instance_id": "P01", "instance_label": "Test"}
    record = adapter.list_progress(features.view, **args).items[0].to_dict()
    assert not record["can_remove_progress"]
    result = adapter.remove_progress(features.view, record, **args)
    assert result.status == "skipped" and result.reason == "active_session"
    assert not any(method == "POST" for method, _, _ in features.server.calls)


@pytest.mark.parametrize("feature", ["ratings", "progress"])
def test_interactive_removal_uses_verified_wetrakr_write(features, config_base, monkeypatch, feature):
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync import run
    from test_wetrakr_sync import interactive_setup

    cfg, source = interactive_setup(features, config_base, monkeypatch, feature, "one-way")
    cfg["sync"].update(enable_remove=True, allow_mass_delete=True)
    cfg["pairs"][0]["features"][feature].update(remove=True, remove_mode="mirror", min_seconds=0, min_percent=0, max_age_days=0)
    items = [progress_item(media, when="2099-01-01T00:00:00Z") if feature == "progress" else {**item(media), "rating": 8} for media in (MOVIE, EPISODE)]
    source.index = {common.item_key(features.adapter, feature, row): row for row in items}
    initial = InteractivePlan()
    assert not run(cfg, initial)["errors"]
    assert not run(cfg, InteractivePlan(preview=False, selected=set(initial.rows)))["errors"]
    source.index.clear()
    features.server.calls.clear()
    plan = InteractivePlan()
    assert not run(cfg, plan)["errors"]
    assert len(plan.rows) == 2 and all(row["operation"] == "remove" for row in plan.rows.values())
    assert all(method == "GET" for method, _, _ in features.server.calls)
    chosen = next(key for key, row in plan.rows.items() if row["item"]["type"] == "episode")
    from cw_platform.orchestrator import Orchestrator
    events = []
    result = Orchestrator(cfg, interactive=InteractivePlan(preview=False, selected={chosen}), on_progress=events.append).run(
        dry_run=False, pair_scope_ids=["p1"], write_state_json=True)
    assert not result["errors"], "\n".join(event for event in events if '"event":"error"' in event or ':error"' in event)
    stored = features.playing if feature == "progress" else features.ratings
    assert set(stored) == {MOVIE["id"]}


def test_playback_lists_only_configured_wetrakr_profile(features):
    from services.playback_progress.service import PlaybackProgressService
    profiles = [row for row in PlaybackProgressService().provider_instances(features.cfg) if row["provider"] == "wetrakr"]
    assert [row["instance_id"] for row in profiles] == ["P01"]


@pytest.mark.parametrize("feature", ["ratings", "progress"])
def test_new_features_capture_fresh_data_and_report_progress(features, monkeypatch, feature):
    from datetime import datetime, timezone
    from providers.sync import _mod_WETRAKR as module
    from services import snapshots

    assert features.adapter.add(feature, [{**progress_item(), "rating": 8}])["ok"]
    if feature == "ratings":
        features.ratings[MOVIE["id"]]["interactions"]["user"]["rating"]["rating"] = 5
    else:
        features.playing[MOVIE["id"]]["playback"]["progress_percent"] = 50
    events = []
    monkeypatch.setattr(module.ctx, "emit", lambda event, fields: events.append((event, fields)))
    data = snapshots._build_index_capture_mode(ops=module.OPS, cfg_view=features.view, pid="WETRAKR", instance="P01",
                                              feat=feature, ts=datetime.now(timezone.utc), progress_id="capture-new-features")
    row = next(iter(data.values()))
    assert row["ids"]["wetrakr"] == str(MOVIE["id"])
    assert row["rating" if feature == "ratings" else "progress_percent"] == (5 if feature == "ratings" else 50)
    assert any(event == "snapshot:progress" and fields.get("done") == 1 for event, fields in events)


@pytest.mark.parametrize("feature", ["ratings", "progress"])
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_interactive_new_features_preview_and_selected_apply(features, config_base, monkeypatch, feature, mode):
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync import run
    from test_wetrakr_sync import interactive_setup

    cfg, source = interactive_setup(features, config_base, monkeypatch, feature, mode)
    cfg["pairs"][0]["features"][feature].update(max_age_days=0, min_seconds=0, min_percent=0)
    sources = [progress_item(media, when="2099-01-01T00:00:00Z") if feature == "progress" else {**item(media), "rating": 8} for media in (MOVIE, EPISODE)]
    source.index = {common.item_key(features.adapter, feature, row): row for row in sources}
    preview = InteractivePlan()
    assert not run(cfg, preview)["errors"]
    assert len(preview.rows) == 2
    assert all(method == "GET" for method, _, _ in features.server.calls)
    selected = next(key for key, row in preview.rows.items() if row["item"]["type"] == "episode")
    assert not run(cfg, InteractivePlan(preview=False, selected={selected}))["errors"]
    stored = features.playing if feature == "progress" else features.ratings
    assert set(stored) == {EPISODE["id"]}

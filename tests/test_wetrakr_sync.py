# tests/test_wetrakr_sync.py
# CrossWatch - WeTrakr sync contract, reads, verified writes and rate limits
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
from types import SimpleNamespace
from urllib.parse import urlsplit

import pytest
import requests

from cw_platform.history_events import minimal_history_item
from cw_platform.id_map import canonical_key, minimal
from cw_platform.provider_instances import build_provider_config_view
from providers.auth import _auth_WETRAKR as auth
from providers.sync._mod_WETRAKR import OPS, WETRAKRModule, get_manifest
from providers.sync.wetrakr import _common as common

MOVIE = {"id": 126, "type": "movie", "title": "Test movie", "ids": {"tmdb": {"id": 155}, "imdb": {"id": "tt0468569"}}}
SHOW = {"id": 1391953, "type": "show", "title": "Test show", "ids": {"tmdb": 1396, "tvdb": 81189}}
EPISODE = {"id": 174658, "type": "episode", "title": "Pilot", "ids": {"tmdb": 62085}, "season_number": 1, "number": 1, "show": SHOW}
WHEN = "2026-09-15T21:09:53Z"
LATER = "2026-09-17T21:09:53Z"


class Response:
    def __init__(self, data, status=200, headers=None):
        self.data = data
        self.status_code = status
        self.headers = headers or {}

    def json(self):
        return copy.deepcopy(self.data)


class Server:
    def __init__(self):
        self.calls = []
        self.planning = []
        self.history = []
        self.hook = None
        self.partial = False
        self.timeout_after_write = False
        self.sequence = 0
        self.activities = {kind: {"all": WHEN} for kind in ("movies", "shows", "episodes")}

    def play(self, media, when=WHEN):
        self.sequence += 1
        self.history.append({"id": f"play-{self.sequence}", "watched_at": when, media["type"]: copy.deepcopy(media)})

    def resolve(self, group, row):
        candidates = {"movies": [MOVIE], "shows": [SHOW], "episodes": [EPISODE]}[group]
        for item in candidates:
            ids = common.media_ids(item)
            if row.get("id") == item["id"] or any(str(value) == ids.get(key) for key, value in row.get("ids", {}).items()):
                return copy.deepcopy(item)
        return None

    def request(self, session, method, url, **kwargs):
        path = urlsplit(url).path
        self.calls.append((method, path, copy.deepcopy(kwargs)))
        if self.hook:
            response = self.hook(method, path, kwargs)
            if response is not None:
                return response
        if method == "GET":
            if path == "/sync/last_activities":
                return Response(self.activities)
            if path == "/account/settings":
                return Response({"id": 42, "plan": "free"})
            kind = "episode" if path.endswith("episodes") else "show" if path.endswith("shows") else "movie"
            if "/planning/" in path:
                rows = [row for row in self.planning if row["type"] == kind]
            elif "/history/" in path:
                rows = [row for row in self.history if kind in row]
            else:
                deduped = {}
                for row in self.history:
                    if kind in row:
                        deduped[row[kind]["id"]] = {**row[kind], "watched_at": row["watched_at"]}
                rows = list(deduped.values())
            return Response(rows, headers={"X-Pagination-Page": "1", "X-Pagination-Page-Count": "1", "X-Pagination-Item-Count": str(len(rows))})
        if method == "DELETE":
            event_id = path.rsplit("/", 1)[-1]
            self.history = [row for row in self.history if row["id"] != event_id]
            return Response({"success": True, "removed": 1})
        assert method == "POST"
        for group, rows in kwargs["json"].items():
            for row in rows:
                if self.partial:
                    continue
                media = self.resolve(group, row)
                if media is None:
                    continue
                status = row.get("status")
                fields = row
                if group == "shows" and row.get("seasons"):
                    season = row["seasons"][0]
                    episode = season["episodes"][0]
                    if season["number"] != 1 or episode["number"] != 1:
                        continue
                    media, fields, status = copy.deepcopy(EPISODE), episode, episode["status"]
                if status == "planning":
                    self.planning = [entry for entry in self.planning if entry["id"] != media["id"]]
                    if not path.endswith("remove"):
                        self.planning.append(media)
                elif "/remove/all" in path:
                    kind = media["type"]
                    self.history = [entry for entry in self.history if entry.get(kind, {}).get("id") != media["id"]]
                else:
                    self.play(media, fields.get("tracked_at", WHEN))
        if self.timeout_after_write:
            raise requests.Timeout("simulated uncertain write")
        return Response({"added": {"total": 1}, "notFound": {}})


@pytest.fixture
def env(monkeypatch, config_base):
    cfg = {"wetrakr": {"instances": {"P01": {"access_token": "test-access", "user_id": "42", "plan": "free", "expires_at": 9999999999}}}}
    view = build_provider_config_view(cfg, "wetrakr", "P01")
    view["_cw_provider_instance"] = "P01"
    server = Server()
    common._STATES.clear()
    common._WRITE_LOCKS.clear()
    auth._QUOTA_SEEN.clear()
    monkeypatch.setattr(auth, "_load_full_cfg", lambda: copy.deepcopy(cfg))
    monkeypatch.setattr(common.SimpleRateLimiter, "wait", lambda *a: 0)
    monkeypatch.setattr(common, "log", lambda *a, **k: None)
    monkeypatch.setattr(requests.sessions.Session, "request", lambda session, method, url, **kw: server.request(session, method, url, **kw))
    adapter = WETRAKRModule(view)
    return SimpleNamespace(cfg=cfg, view=view, adapter=adapter, server=server)


def item(media=MOVIE, when=None):
    result = common.media_item(media, media["type"])
    if when:
        result["watched_at"] = when
    return result


def test_registry_profiles_capabilities_and_ids(env):
    from cw_platform.modules_registry import load_sync_ops, sync_provider_supports_feature
    from cw_platform.orchestrator._history_rewatches import history_rewatch_pair_enabled

    assert load_sync_ops("WETRAKR") is OPS
    assert get_manifest()["version"] == "0.1"
    assert OPS.is_configured(env.view)
    assert not OPS.is_configured(env.cfg)
    assert sync_provider_supports_feature("wetrakr", "watchlist")
    assert sync_provider_supports_feature("wetrakr", "progress")
    assert sync_provider_supports_feature("wetrakr", "ratings")
    assert OPS.capabilities()["progress"]["remove"]
    assert OPS.capabilities()["history"]["rewatches"]["account_gate"] is False
    assert not history_rewatch_pair_enabled("history", {}, "WETRAKR", OPS, "WETRAKR", OPS)
    assert history_rewatch_pair_enabled("history", {"rewatches": True}, "WETRAKR", OPS, "WETRAKR", OPS, bidirectional=True)
    assert minimal({"type": "movie", "ids": {"wetrakr": "126"}})["ids"] == {"wetrakr": "126"}
    assert canonical_key({"type": "movie", "ids": {"wetrakr": "126"}}) == "wetrakr:126"
    assert env.adapter.health()["ok"]
    assert env.server.calls[-1][2]["headers"]["Authorization"] == "Bearer test-access"


def test_api_totals_count_requests_without_debug_and_exclude_local_quota_blocks(env, monkeypatch):
    from cw_platform.orchestrator._pairs_metrics import ApiMetrics
    from providers.sync import _mod_WETRAKR as module

    monkeypatch.delenv("CW_API_HITS", raising=False)
    metrics = ApiMetrics(lambda *args, **kwargs: None)
    monkeypatch.setattr(module, "ctx", metrics)
    adapter = WETRAKRModule(env.view)
    assert adapter.health()["ok"]
    env.server.hook = lambda *args: Response({}, 429, {"Retry-After": "30"})
    with pytest.raises(common.WeTrakrSyncError):
        common.request(adapter, "GET", "/sync/last_activities")
    with pytest.raises(common.WeTrakrSyncError):
        common.request(adapter, "GET", "/sync/last_activities")
    totals = metrics.totals()
    assert totals["total"] == len(env.server.calls) == 2
    assert totals["providers"]["WETRAKR"]["by_feature"] == {"account/settings": 1, "sync/last_activities": 1}


@pytest.mark.parametrize("media", [MOVIE, EPISODE])
def test_unresolved_logs_and_archive_keep_media_details(env, monkeypatch, media):
    from cw_platform.orchestrator import _applier

    source = item(media)
    env.server.partial = True
    result = env.adapter.add("history", iter([source]))
    assert not result["ok"] and result["unresolved"][0]["item"] == source
    events, saved = [], []
    monkeypatch.setattr(_applier, "record_unresolved", lambda *args, **kwargs: saved.extend(args[2]))
    _applier._normalize(result, [source], "add", dst="WETRAKR", feature="history",
                        emit=lambda event, **fields: events.append((event, fields)))
    detail = next(fields["items"][0] for event, fields in events if event == "apply:unresolved")
    assert detail["title"] == source["title"] and detail["reason"] == "write_not_verified"
    assert saved[0]["ids"] == source["ids"]
    if media == EPISODE:
        assert detail["season"] == 1 and detail["episode"] == 1
        assert detail["series_title"] == SHOW["title"]


def test_unresolved_without_title_uses_identity_and_keeps_preflight_error(env):
    source = {"type": "movie", "ids": {"tmdb": "155"}}
    env.server.hook = lambda *args: Response({}, 503)
    result = env.adapter.add("watchlist", [source])
    row = result["unresolved"][0]
    assert row["reason"] == "request_rejected" and row["item"]["title"] == "tmdb:155"
    assert "title" not in source


@pytest.mark.parametrize("remove", [False, True])
def test_sync_write_summary_matches_other_providers(env, monkeypatch, remove):
    logs = []
    monkeypatch.setattr(common, "log", lambda *args, **fields: logs.append((args, fields)))
    env.server.planning = [MOVIE] if remove else []
    result = (env.adapter.remove if remove else env.adapter.add)("watchlist", [item()])
    assert result["ok"]
    args, fields = next(row for row in logs if row[0][-1] == "write_done")
    assert args == ("WETRAKR", "watchlist", "info", "write_done")
    assert fields == {"op": "remove" if remove else "add", "ok": True, "applied": 1, "unresolved": 0}


def test_watchlist_reads_movies_shows_and_normalizes_ids(env):
    env.server.planning = [MOVIE, SHOW]
    index = env.adapter.build_index("watchlist")
    assert set(index) == {"tmdb:155", "tmdb:1396"}
    assert index["tmdb:155"]["ids"]["imdb"] == "tt0468569"
    assert index["tmdb:1396"]["type"] == "show"


def test_watchlist_add_remove_and_repeat_are_verified(env):
    first = env.adapter.add("watchlist", [item()])
    assert first["confirmed_keys"] == ["tmdb:155"]
    repeat = env.adapter.add("watchlist", [item()])
    assert repeat["ok"]
    assert len([call for call in env.server.calls if call[0] == "POST"]) == 1
    result = env.adapter.remove("watchlist", [item()])
    assert result["confirmed_keys"] == ["tmdb:155"] and not env.server.planning


@pytest.mark.parametrize("feature", ["watchlist", "history"])
@pytest.mark.parametrize("remove", [False, True])
def test_bulk_write_uses_one_request_and_one_verification_for_500_items(env, monkeypatch, feature, remove):
    media = [{"id": i, "type": "movie", "title": f"Movie {i}", "ids": {"tmdb": i}} for i in range(1, 501)]
    by_id = {row["id"]: row for row in media}
    monkeypatch.setattr(env.server, "resolve", lambda group, row: copy.deepcopy(by_id.get(row.get("id"))))
    if remove:
        if feature == "watchlist":
            env.server.planning = copy.deepcopy(media)
        else:
            for row in media:
                env.server.play(row)
    reads = []
    build_index = env.adapter.build_index

    def observed_read(*args, **kwargs):
        reads.append(kwargs)
        return build_index(*args, **kwargs)

    monkeypatch.setattr(env.adapter, "build_index", observed_read)
    operation = env.adapter.remove if remove else env.adapter.add
    result = operation(feature, [item(row, WHEN) for row in media])
    assert result["ok"] and len(result["confirmed_keys"]) == 500
    writes = [kwargs["json"] for method, _, kwargs in env.server.calls if method == "POST"]
    assert len(writes) == 1 and len(writes[0]["movies"]) == 500
    assert reads == [{"force_refresh": True}, {"force_refresh": True}]
    remaining = env.server.planning if feature == "watchlist" else env.server.history
    assert len(remaining) == (0 if remove else 500)


def test_bulk_uncertain_write_verifies_first_500_and_defers_remaining_items(env, monkeypatch):
    media = [{"id": i, "type": "movie", "title": f"Movie {i}", "ids": {"tmdb": i}} for i in range(1, 502)]
    by_id = {row["id"]: row for row in media}
    monkeypatch.setattr(env.server, "resolve", lambda group, row: copy.deepcopy(by_id.get(row.get("id"))))
    env.server.timeout_after_write = True
    result = env.adapter.add("watchlist", [item(row) for row in media])
    assert not result["ok"] and len(result["confirmed_keys"]) == 500
    assert result["unresolved_keys"] == ["tmdb:501"]
    assert len(env.server.planning) == 500
    assert len([call for call in env.server.calls if call[0] == "POST"]) == 1


def test_state_history_does_not_add_rewatch_and_remove_clears_all_plays(env):
    env.server.play(MOVIE)
    env.server.play(MOVIE, LATER)
    assert len(env.adapter.build_index("history")) == 1
    assert env.adapter.add("history", [item(MOVIE, LATER)])["ok"]
    assert not any(method == "POST" for method, _, _ in env.server.calls)
    assert env.adapter.remove("history", [item(MOVIE)])["ok"]
    assert not env.server.history
    assert any(path == "/sync/tracking/remove/all" for _, path, _ in env.server.calls)


def test_free_account_rewatch_add_repeat_and_delete_only_target_event(env):
    env.adapter.config["_cw_history_rewatches"] = True
    env.server.play(MOVIE)
    source = item(MOVIE, LATER)
    result = env.adapter.add("history", [source])
    assert result["ok"] and len(env.server.history) == 2
    assert env.adapter.add("history", [source])["ok"]
    assert len(env.server.history) == 2
    assert len(env.adapter.build_index("history")) == 2
    source["_wetrakr_history_id"] = "foreign-account-id"
    assert env.adapter.remove("history", [source])["ok"]
    assert [row["id"] for row in env.server.history] == ["play-1"]
    assert any(path == "/sync/tracklogs/play-2" for _, path, _ in env.server.calls)


@pytest.mark.parametrize("persisted", [False, True])
def test_rewatch_removal_keeps_event_identity_when_equal_date_rows_reorder(env, persisted):
    env.adapter.config["_cw_history_rewatches"] = True
    env.server.play(MOVIE)
    env.server.play(MOVIE)
    selected = next(iter(env.adapter.build_index("history").values()))
    if persisted:
        selected["provider_event_id"] = selected.pop("_wetrakr_history_id")
    env.server.history.reverse()
    result = env.adapter.remove("history", [selected])
    assert result["ok"]
    assert [row["id"] for row in env.server.history] == ["play-2"]


def test_episode_show_ids_are_not_confused_with_episode_ids(env):
    source = item(EPISODE, WHEN)
    result = env.adapter.add("history", [source])
    assert result["ok"]
    write = next(kwargs["json"] for method, _, kwargs in env.server.calls if method == "POST")
    show = write["shows"][0]
    assert show["id"] == SHOW["id"]
    assert show["seasons"][0]["episodes"][0]["number"] == 1
    stored = next(iter(env.adapter.build_index("history").values()))
    assert stored["show_ids"]["tmdb"] == "1396"
    assert stored["ids"]["tmdb"] == "62085"


def test_missing_episode_coords_or_ids_are_unresolved_without_network(env):
    result = env.adapter.add("history", [{"type": "episode", "ids": {"tmdb": "62085"}}, {"type": "movie", "ids": {}}])
    assert not result["ok"] and len(result["unresolved"]) == 2
    assert not env.server.calls


def test_unknown_dates_are_not_fabricated_for_event_sync(env):
    env.adapter.config["_cw_history_rewatches"] = True
    result = env.adapter.add("history", [item(MOVIE)])
    assert result["unresolved"][0]["reason"] == "missing_watch_timestamp"
    assert not env.server.calls
    normalized = minimal_history_item({**item(MOVIE), "_wetrakr_history_id": "abc", "_wetrakr_watched_at_unknown": True}, event_mode=True)
    assert normalized["_cw_event_key"].endswith("@id:abc")


def test_partial_success_is_not_claimed_from_aggregate_counters(env):
    env.server.partial = True
    result = env.adapter.add("watchlist", [item()])
    assert not result["ok"]
    assert result["confirmed_keys"] == [] and result["unresolved_keys"] == ["tmdb:155"]


def test_uncertain_history_write_is_read_back_without_post_retry(env):
    env.adapter.config["_cw_history_rewatches"] = True
    env.server.timeout_after_write = True
    result = env.adapter.add("history", [item(MOVIE, WHEN)])
    assert result["ok"]
    assert len(env.server.history) == 1
    assert len([call for call in env.server.calls if call[0] == "POST"]) == 1


@pytest.mark.parametrize("data,headers,reason", [
    ({"error": "bad"}, {}, "invalid_page"),
    ([MOVIE], {}, "invalid_pagination"),
    ([], {"X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "1"}, "incomplete_snapshot"),
    ([MOVIE], {"X-Pagination-Page-Count": "0", "X-Pagination-Item-Count": "1"}, "invalid_pagination"),
    ([MOVIE], {"X-Pagination-Page-Count": "1"}, "invalid_pagination"),
])
def test_invalid_reads_raise_instead_of_returning_empty_snapshot(env, data, headers, reason):
    env.server.hook = lambda *a: Response(data, headers=headers)
    with pytest.raises(common.WeTrakrSyncError, match=reason):
        env.adapter.build_index("watchlist")
    result = env.adapter.remove("watchlist", [item()])
    assert not result["ok"] and not result["confirmed_keys"]
    assert not any(method != "GET" for method, _, _ in env.server.calls)


def test_pagination_consumes_all_pages_and_rejects_repetition(env):
    def paged(method, path, kwargs):
        page = kwargs["params"]["page"]
        row = MOVIE if page == 1 else {**MOVIE, "id": 127, "ids": {"tmdb": 156}}
        return Response([row], headers={"X-Pagination-Page": str(page), "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"})

    env.server.hook = paged
    assert len(common.pages(env.adapter, "/sync/tracking/planning/movies")) == 2
    env.server.hook = lambda *a: Response([MOVIE], headers={"X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"})
    with pytest.raises(common.WeTrakrSyncError, match="repeated_page"):
        common.pages(env.adapter, "/sync/tracking/planning/movies")


@pytest.mark.parametrize("daily", [False, True])
def test_quota_headers_and_cooldown_do_not_retry_or_share_other_accounts(env, daily):
    env.server.hook = lambda *a: Response({"error": "QUOTA_EXCEEDED" if daily else "TOO_MANY_REQUESTS"}, 429,
                                        {"Retry-After": "120", "X-Quota-Remaining": "0", "X-Quota-Limit": "1000", "X-Quota-Reset": "600"})
    with pytest.raises(common.WeTrakrSyncError) as caught:
        common.request(env.adapter, "GET", "/account/settings")
    assert caught.value.retry_after == (600 if daily else 120)
    assert auth.latest_quota(env.cfg["wetrakr"]["instances"]["P01"])["daily_remaining"] == 0
    with pytest.raises(common.WeTrakrSyncError):
        common.request(env.adapter, "GET", "/account/settings")
    assert len(env.server.calls) == 1
    other = WETRAKRModule({"wetrakr": {"access_token": "different", "user_id": "43"}}, instance_id="default")
    assert common._state(other)["blocked"] == {}


def test_dry_run_does_not_read_or_write_remote_data(env):
    assert env.adapter.add("history", [item(MOVIE, WHEN)], dry_run=True)["count"] == 1
    assert not env.server.calls


def test_unchanged_snapshot_is_reused_across_adapter_instances(env):
    env.server.planning = [MOVIE, SHOW]
    expected = env.adapter.build_index("watchlist")
    env.server.calls.clear()
    assert WETRAKRModule(env.view).build_index("watchlist") == expected
    assert [path for _, path, _ in env.server.calls] == ["/sync/last_activities"]


def test_changed_section_refresh_detects_removal_without_removed_timestamp(env):
    env.server.planning = [MOVIE, SHOW]
    env.adapter.build_index("watchlist")
    env.server.planning = [SHOW]
    env.server.activities["movies"] = {"all": LATER, "last_tracking_watched_at": LATER}
    env.server.calls.clear()
    assert set(env.adapter.build_index("watchlist")) == {"tmdb:1396"}
    reads = [path for _, path, _ in env.server.calls if "/planning/" in path]
    assert reads == ["/sync/tracking/planning/movies"]


@pytest.mark.parametrize("activities", [{}, {"movies": {"all": "invalid"}}, {"movies": {"all": "2026-09-26"}}])
def test_missing_or_invalid_activity_requires_full_read(env, activities):
    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    env.server.activities = activities
    env.server.planning = []
    env.server.calls.clear()
    assert env.adapter.build_index("watchlist") == {}
    assert len([path for _, path, _ in env.server.calls if "/planning/" in path]) == 2


def test_failed_refresh_keeps_previous_cache_and_raises(env):
    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    previous = path.read_bytes()
    env.server.activities["movies"]["all"] = LATER
    env.server.hook = lambda method, path, kw: Response({}, 503) if "/planning/" in path else None
    with pytest.raises(common.WeTrakrSyncError, match="request_rejected"):
        env.adapter.build_index("watchlist")
    assert path.read_bytes() == previous


def test_activity_change_during_read_does_not_commit_snapshot(env):
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    previous = path.read_bytes()

    def change_during_read(method, path, kwargs):
        if path.endswith("/planning/movies"):
            env.server.activities["movies"]["all"] = LATER

    env.server.hook = change_during_read
    with pytest.raises(common.WeTrakrSyncError, match="snapshot_changed"):
        env.adapter.build_index("watchlist", force_refresh=True)
    assert path.read_bytes() == previous


def test_activity_rate_limit_never_returns_stale_cache(env):
    env.adapter.build_index("watchlist")
    env.server.hook = lambda *a: Response({}, 429, {"Retry-After": "60"})
    with pytest.raises(common.WeTrakrSyncError, match="rate_limited"):
        env.adapter.build_index("watchlist")


def test_expired_snapshot_refreshes_even_when_activity_misses_change(env, monkeypatch):
    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    monkeypatch.setattr(common, "MAX_CACHE_AGE", 0)
    env.server.planning = []
    assert env.adapter.build_index("watchlist") == {}


def test_cache_separates_watch_modes_profiles_and_accounts(env):
    original = common.cache_path(env.adapter, "history")
    env.adapter.config["_cw_history_rewatches"] = True
    assert common.cache_path(env.adapter, "history") != original
    env.adapter.config["_cw_history_rewatches"] = False
    env.adapter.instance_id = "P02"
    assert common.cache_path(env.adapter, "history") != original
    other = WETRAKRModule({"wetrakr": {"access_token": "other", "user_id": "43"}}, instance_id="P01")
    assert common.cache_path(other, "history") != original


def test_corrupted_snapshot_is_rebuilt(env):
    env.adapter.build_index("watchlist")
    common.cache_path(env.adapter, "watchlist").write_text("invalid", encoding="utf-8")
    env.server.planning = [MOVIE]
    assert set(env.adapter.build_index("watchlist")) == {"tmdb:155"}


def test_invalid_media_does_not_replace_valid_snapshot(env):
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    previous = path.read_bytes()
    env.server.planning = [{**MOVIE, "id": None, "ids": {}}]
    with pytest.raises(common.WeTrakrSyncError, match="missing_media_identity"):
        env.adapter.build_index("watchlist", force_refresh=True)
    assert path.read_bytes() == previous


def test_minute_exhaustion_defers_only_affected_bucket_until_reset(env, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(common.time, "time", lambda: now[0])
    env.server.hook = lambda *a: Response({}, headers={"RateLimit-Remaining": "0", "RateLimit-Reset": "30"})
    common.request(env.adapter, "GET", "/account/settings")
    with pytest.raises(common.WeTrakrSyncError) as caught:
        common.request(env.adapter, "GET", "/account/settings")
    assert caught.value.retry_after == 30
    assert len(env.server.calls) == 1
    common.request(env.adapter, "POST", "/sync/tracking", json={})
    now[0] += 31
    common.request(env.adapter, "GET", "/account/settings")
    assert len(env.server.calls) == 3


def test_zero_daily_remaining_alone_does_not_block_beta_requests(env):
    env.server.hook = lambda *a: Response({}, headers={"X-Quota-Remaining": "0", "X-Quota-Reset": "600"})
    common.request(env.adapter, "GET", "/account/settings")
    common.request(env.adapter, "GET", "/account/settings")
    assert len(env.server.calls) == 2


def test_sync_discovery_exposes_profile_only_watchlist_history_and_rewatches(env, monkeypatch):
    from api import syncAPI
    from cw_platform import config_base

    monkeypatch.setattr(config_base, "load_config", lambda: env.cfg)
    monkeypatch.setattr(syncAPI, "sync_provider_names", lambda **kw: ["WETRAKR"])
    data = json.loads(syncAPI.api_sync_providers().body)
    assert len(data) == 1
    assert data[0]["configured"]
    assert {feature for feature, enabled in data[0]["features"].items() if enabled} == {"watchlist", "history", "ratings", "progress"}
    assert data[0]["capabilities"]["history"]["rewatches"] == {"read": True, "write": True}
    assert not env.server.calls


def test_narrowed_profile_without_environment_hint_uses_its_own_credentials(env):
    view = build_provider_config_view(env.cfg, "wetrakr", "P01")
    adapter = WETRAKRModule(view)
    assert adapter.instance_id == "P01"
    assert adapter.health()["ok"]
    assert env.server.calls[-1][2]["headers"]["Authorization"] == "Bearer test-access"


def test_stale_narrowed_view_never_borrows_default_credentials(env):
    view = build_provider_config_view(env.cfg, "wetrakr", "P01")
    view["wetrakr"]["access_token"] = "stale"
    with pytest.raises(common.WeTrakrSyncError, match="provider_credentials_changed"):
        WETRAKRModule(view)
    assert not env.server.calls


def test_capture_reads_selected_profile_with_native_ids(env, monkeypatch):
    from datetime import datetime, timezone
    from services.snapshots import _build_index_capture_mode

    env.server.planning = [MOVIE]
    view = build_provider_config_view(env.cfg, "wetrakr", "P01")
    result = _build_index_capture_mode(ops=OPS, cfg_view=view, pid="WETRAKR", instance="P01",
                                       feat="watchlist", ts=datetime.now(timezone.utc))
    assert set(result) == {"wetrakr:126"}


def tracking_activity(stamp=WHEN, removed=WHEN):
    return {"all": stamp, "last_tracking_planning_at": stamp,
            "last_tracking_watched_at": stamp, "last_tracking_removed_at": removed}


def delta_response(rows):
    return Response(rows, headers={"X-Pagination-Page": "1", "X-Pagination-Page-Count": "1",
                                   "X-Pagination-Item-Count": str(len(rows))})


@pytest.mark.parametrize("media,feature", [(MOVIE, "watchlist"), (SHOW, "watchlist"), (MOVIE, "history"), (EPISODE, "history")])
def test_incremental_state_merges_changes_without_dropping_other_items(env, media, feature):
    kind = media["type"]
    section = kind + "s"
    env.server.activities[section] = tracking_activity()
    other = {**media, "id": 900001, "ids": {"tmdb": 900001}}
    if feature == "watchlist":
        env.server.planning = [media, other]
    else:
        if kind == "episode":
            other = {**other, "number": 2}
        env.server.play(media)
        env.server.play(other)
    initial = env.adapter.build_index(feature)
    env.server.activities[section] = tracking_activity(LATER)
    changed = {**media, "title": "Updated", "watched_at": "2020-01-02T12:00:00Z"}
    env.server.hook = lambda method, path, kw: delta_response([changed]) if kw.get("params", {}).get("from_date") else None
    env.server.calls.clear()
    result = env.adapter.build_index(feature)
    assert set(result) == set(initial)
    assert any(row.get("title") == "Updated" for row in result.values())
    reads = [(path, kw) for _, path, kw in env.server.calls if "/tracking/" in path]
    assert len(reads) == 1
    assert reads[0][1]["params"]["from_date"] == "2026-09-15T21:09:51Z"


@pytest.mark.parametrize("media", [MOVIE, EPISODE])
def test_incremental_rewatch_edit_replaces_event_by_id_and_preserves_other_play(env, media):
    env.adapter.config["_cw_history_rewatches"] = True
    section = media["type"] + "s"
    env.server.activities[section] = tracking_activity()
    env.server.play(media)
    env.server.play(media, LATER)
    before = env.adapter.build_index("history")
    old_key = next(key for key, row in before.items() if row["_wetrakr_history_id"] == "play-2")
    changed = {**env.server.history[1], "watched_at": "2020-01-02T12:00:00Z"}
    env.server.activities[section] = tracking_activity(LATER)
    env.server.hook = lambda method, path, kw: delta_response([changed]) if kw.get("params", {}).get("from_date") else None
    after = env.adapter.build_index("history")
    assert len(after) == 2 and old_key not in after
    edited = next(row for row in after.values() if row["_wetrakr_history_id"] == "play-2")
    assert edited["watched_at"] == changed["watched_at"]
    assert any(row["_wetrakr_history_id"] == "play-1" for row in after.values())


def test_incremental_rewatch_add_preserves_distinct_events_at_same_timestamp(env):
    env.adapter.config["_cw_history_rewatches"] = True
    env.server.activities["movies"] = tracking_activity()
    env.server.play(MOVIE)
    env.adapter.build_index("history")
    env.server.play(MOVIE)
    env.server.activities["movies"] = tracking_activity(LATER)
    env.server.hook = lambda method, path, kw: delta_response(env.server.history) if kw.get("params", {}).get("from_date") else None
    after = env.adapter.build_index("history")
    assert len(after) == 2
    assert {row["_wetrakr_history_id"] for row in after.values()} == {"play-1", "play-2"}


@pytest.mark.parametrize("media,feature", [(MOVIE, "watchlist"), (SHOW, "watchlist"), (MOVIE, "history"), (EPISODE, "history")])
def test_removal_activity_requires_full_section_refresh(env, media, feature):
    section = media["type"] + "s"
    env.server.activities[section] = tracking_activity()
    env.adapter.config["_cw_history_rewatches"] = True
    if feature == "watchlist":
        env.server.planning = [media]
    else:
        env.server.play(media)
        env.server.play(media, LATER)
    env.adapter.build_index(feature)
    env.server.planning = []
    env.server.history = env.server.history[:1]
    env.server.activities[section] = tracking_activity(LATER, removed=LATER)
    env.server.calls.clear()
    after = env.adapter.build_index(feature)
    assert len(after) == (0 if feature == "watchlist" else 1)
    reads = [kw for _, path, kw in env.server.calls if "/tracking/" in path]
    assert len(reads) == 1 and "from_date" not in reads[0]["params"]


@pytest.mark.parametrize("mutation", ["missing_removal", "missing_watched", "bad_removal", "regressed", "same_all", "unrelated"])
def test_inconsistent_or_unexplained_activity_uses_full_refresh(env, mutation):
    env.server.activities["movies"] = tracking_activity()
    env.adapter.build_index("history")
    updated = tracking_activity(LATER)
    if mutation == "missing_removal":
        updated.pop("last_tracking_removed_at")
    elif mutation == "missing_watched":
        updated.pop("last_tracking_watched_at")
    elif mutation == "bad_removal":
        updated["last_tracking_removed_at"] = "invalid"
    elif mutation == "regressed":
        updated["last_tracking_removed_at"] = "2020-01-01T00:00:00Z"
    elif mutation == "same_all":
        updated["all"] = WHEN
    else:
        updated["last_tracking_watched_at"] = WHEN
    env.server.activities["movies"] = updated
    env.server.calls.clear()
    env.adapter.build_index("history")
    reads = [kw for _, path, kw in env.server.calls if "/tracking/" in path]
    assert len(reads) == 1 and "from_date" not in reads[0]["params"]


def test_failed_delta_keeps_checkpoint_for_retry(env):
    env.server.activities["movies"] = tracking_activity()
    env.adapter.build_index("history")
    path = common.cache_path(env.adapter, "history")
    previous = path.read_bytes()
    env.server.activities["movies"] = tracking_activity(LATER)
    env.server.hook = lambda method, path, kw: Response({}, 503) if kw.get("params", {}).get("from_date") else None
    with pytest.raises(common.WeTrakrSyncError, match="request_rejected"):
        env.adapter.build_index("history")
    assert path.read_bytes() == previous


def test_empty_delta_falls_back_to_complete_section(env):
    env.server.activities["movies"] = tracking_activity()
    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    env.server.activities["movies"] = tracking_activity(LATER)
    env.server.planning = []
    env.server.calls.clear()
    assert env.adapter.build_index("watchlist") == {}
    reads = [kw for _, path, kw in env.server.calls if path.endswith("/planning/movies")]
    assert len(reads) == 2
    assert "from_date" in reads[0]["params"] and "from_date" not in reads[1]["params"]


def test_delta_does_not_postpone_periodic_full_refresh(env, monkeypatch):
    now = [1000.0]
    monkeypatch.setattr(common.time, "time", lambda: now[0])
    env.server.activities["movies"] = tracking_activity()
    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    now[0] += 100
    env.server.activities["movies"] = tracking_activity(LATER)
    env.adapter.build_index("watchlist")
    now[0] += common.MAX_CACHE_AGE
    env.server.calls.clear()
    env.adapter.build_index("watchlist")
    reads = [kw for _, path, kw in env.server.calls if "/tracking/" in path]
    assert len(reads) == 2 and all("from_date" not in kw["params"] for kw in reads)


def test_delta_pagination_keeps_from_date_on_every_page(env):
    env.server.activities["movies"] = tracking_activity()
    env.adapter.build_index("watchlist")
    env.server.activities["movies"] = tracking_activity(LATER)

    def paged(method, path, kwargs):
        params = kwargs.get("params", {})
        if not params.get("from_date"):
            return None
        assert params["from_date"] == "2026-09-15T21:09:51Z"
        page = params["page"]
        row = MOVIE if page == 1 else {**MOVIE, "id": 127, "ids": {"tmdb": 156}}
        return Response([row], headers={"X-Pagination-Page": str(page), "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"})

    env.server.hook = paged
    assert len(env.adapter.build_index("watchlist")) == 2


def test_concurrent_change_during_delta_keeps_previous_checkpoint(env):
    env.server.activities["movies"] = tracking_activity()
    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    previous = path.read_bytes()
    env.server.activities["movies"] = tracking_activity(LATER)

    def concurrent(method, path, kwargs):
        if kwargs.get("params", {}).get("from_date"):
            env.server.activities["movies"] = tracking_activity("2026-09-18T00:00:00Z")
            return delta_response([MOVIE])

    env.server.hook = concurrent
    with pytest.raises(common.WeTrakrSyncError, match="snapshot_changed"):
        env.adapter.build_index("watchlist")
    assert path.read_bytes() == previous


@pytest.mark.parametrize("rows", [[{"type": "movie"}], [MOVIE, MOVIE]])
def test_invalid_delta_identity_does_not_commit(env, rows):
    env.server.activities["movies"] = tracking_activity()
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    previous = path.read_bytes()
    env.server.activities["movies"] = tracking_activity(LATER)
    env.server.hook = lambda method, path, kw: delta_response(rows) if kw.get("params", {}).get("from_date") else None
    with pytest.raises(common.WeTrakrSyncError, match="invalid_delta_identity"):
        env.adapter.build_index("watchlist")
    assert path.read_bytes() == previous


def test_old_cache_schema_requires_full_baseline(env):
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    old = json.loads(path.read_text())
    old["schema"] = 1
    path.write_text(json.dumps(old))
    env.server.planning = [MOVIE]
    env.server.calls.clear()
    assert len(env.adapter.build_index("watchlist")) == 1
    reads = [kw for _, path, kw in env.server.calls if "/tracking/" in path]
    assert len(reads) == 2 and all("from_date" not in kw["params"] for kw in reads)


def test_lost_activities_after_delta_does_not_commit(env):
    env.server.activities["movies"] = tracking_activity()
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    previous = path.read_bytes()
    env.server.activities["movies"] = tracking_activity(LATER)

    def lose_activities(method, path, kwargs):
        if kwargs.get("params", {}).get("from_date"):
            env.server.activities = {}
            return delta_response([MOVIE])

    env.server.hook = lose_activities
    with pytest.raises(common.WeTrakrSyncError, match="snapshot_changed"):
        env.adapter.build_index("watchlist")
    assert path.read_bytes() == previous


def progress_events(env, monkeypatch):
    from providers.sync import _mod_WETRAKR as module

    events = []
    monkeypatch.setattr(module, "ctx", SimpleNamespace(emit=lambda event, **fields: events.append({"event": event, **fields})))
    return events


def test_read_progress_reports_pages_and_cached_completion(env, monkeypatch):
    events = progress_events(env, monkeypatch)
    movies = [{**MOVIE, "id": i + 1, "ids": {"tmdb": i + 1}} for i in range(150)]

    def paged(method, path, kwargs):
        if path.endswith("/planning/movies"):
            page = kwargs["params"]["page"]
            return Response(movies[(page - 1) * 100:page * 100], headers={
                "X-Pagination-Page": str(page), "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "150"})

    env.server.hook = paged
    assert len(env.adapter.build_index("watchlist")) == 150
    updates = [event for event in events if event["event"] == "snapshot:progress"]
    assert any(event["done"] == 100 and not event.get("final") for event in updates)
    assert updates[-1] == {"event": "snapshot:progress", "dst": "WETRAKR", "feature": "watchlist", "done": 150,
                           "total": 150, "final": True, "ok": True}
    events.clear()
    env.server.calls.clear()
    assert len(env.adapter.build_index("watchlist")) == 150
    assert events[-1]["done"] == 150 and events[-1]["ok"]
    assert not any("/tracking/" in path for _, path, _ in env.server.calls)


def test_failed_read_reports_failure_and_preserves_snapshot(env, monkeypatch):
    events = progress_events(env, monkeypatch)
    env.adapter.build_index("history")
    path = common.cache_path(env.adapter, "history")
    before = path.read_bytes()
    env.server.hook = lambda method, path, kwargs: Response({}, 503) if "/tracking/" in path else None
    with pytest.raises(common.WeTrakrSyncError):
        env.adapter.build_index("history", force_refresh=True)
    assert events[-1]["final"] and events[-1]["ok"] is False
    assert path.read_bytes() == before


def test_cancelled_read_stops_paging_and_reports_failure(env, monkeypatch):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel

    events = progress_events(env, monkeypatch)
    env.adapter.build_index("watchlist")
    path = common.cache_path(env.adapter, "watchlist")
    before = path.read_bytes()

    def cancel_on_page(method, path, kwargs):
        if "/planning/movies" in path:
            request_cancel()
            return Response([MOVIE], headers={"X-Pagination-Page": "1", "X-Pagination-Page-Count": "2", "X-Pagination-Item-Count": "2"})

    env.server.hook = cancel_on_page
    env.server.calls.clear()
    try:
        with pytest.raises(SyncCancelled):
            env.adapter.build_index("watchlist", force_refresh=True)
    finally:
        clear_cancel()
    assert path.read_bytes() == before
    assert events[-1]["final"] and events[-1]["ok"] is False
    assert len([path for _, path, _ in env.server.calls if "/planning/" in path]) == 1


def test_capture_forces_live_read_instead_of_using_existing_cache(env):
    from datetime import datetime, timezone
    from services.snapshots import _build_index_capture_mode

    env.server.planning = [MOVIE]
    env.adapter.build_index("watchlist")
    env.server.planning = [SHOW]
    env.server.calls.clear()
    result = _build_index_capture_mode(ops=OPS, cfg_view=env.view, pid="WETRAKR", instance="P01",
                                       feat="watchlist", ts=datetime.now(timezone.utc))
    assert set(result) == {"wetrakr:1391953"}
    assert len([path for _, path, _ in env.server.calls if "/planning/" in path]) == 2
    assert not any(method != "GET" for method, _, _ in env.server.calls)


def interactive_setup(env, config_base, monkeypatch, feature, mode):
    from test_orchestrator_dry_run_no_side_effects import FakeOps, _cfg, _install
    from providers.sync import _mod_WETRAKR as module

    source = FakeOps("SRC", {})
    source.features = lambda: {feature: True}
    source.capabilities = OPS.capabilities
    source.health = lambda *a, **k: {"ok": True, "features": {feature: True}}
    _install(monkeypatch, source, OPS, config_base / ".cw_state")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": source, "WETRAKR": OPS})
    monkeypatch.setattr(module, "ctx", module.ctx)
    cfg = {**_cfg(False), **copy.deepcopy(env.cfg)}
    cfg["pairs"][0].update(target="WETRAKR", target_instance="P01", mode=mode, feature=feature,
                           features={feature: {"enable": True, "add": True, "remove": False}})
    return cfg, source


@pytest.mark.parametrize("feature", ["watchlist", "history"])
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_interactive_preview_and_execution_use_real_wetrakr_adapter(env, config_base, monkeypatch, feature, mode):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync import run

    cfg, source = interactive_setup(env, config_base, monkeypatch, feature, mode)
    if feature == "history":
        cfg["pairs"][0]["features"][feature]["rewatches"] = True
        items = [minimal_history_item(item(MOVIE, when), event_mode=True) for when in (WHEN, LATER)]
    else:
        items = [item(MOVIE), item(SHOW)]
    source.index = {common.item_key(env.adapter, feature, row) if feature == "watchlist" else row["_cw_event_key"]: row for row in items}
    events = []
    plan = InteractivePlan()
    result = Orchestrator(cfg, interactive=plan, on_progress=lambda line: events.append(json.loads(line)) if line.startswith("{") else None).run(
        dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    assert not result["errors"]
    assert len(plan.rows) == 2
    assert all(method == "GET" for method, _, _ in env.server.calls)
    assert any(event.get("event") == "snapshot:progress" and event.get("dst") == "WETRAKR" for event in events)
    chosen = next(rid for rid, row in plan.rows.items() if row["item"].get("watched_at") == LATER) if feature == "history" else next(
        rid for rid, row in plan.rows.items() if row["item"].get("type") == "show")
    assert not run(cfg, InteractivePlan(preview=False, selected={chosen}))["errors"]
    assert len([path for method, path, _ in env.server.calls if method == "POST"]) == 1
    if feature == "history":
        assert len(env.server.history) == 1 and env.server.history[0]["watched_at"] == LATER
    else:
        assert [row["type"] for row in env.server.planning] == ["show"]


def test_capture_page_activity_reaches_capture_progress(env, monkeypatch):
    from datetime import datetime, timezone
    from providers.sync import _log
    from services import snapshots

    events = []
    monkeypatch.setattr(common, "log", _log.log)
    monkeypatch.setattr(snapshots, "record_capture_activity", lambda progress_id, **fields: events.append((progress_id, fields)))
    env.server.planning = [MOVIE, SHOW]
    snapshots._build_index_capture_mode(ops=OPS, cfg_view=env.view, pid="WETRAKR", instance="P01",
                                        feat="watchlist", ts=datetime.now(timezone.utc), progress_id="capture-test")
    pages = [fields for progress_id, fields in events if progress_id == "capture-test" and fields["event"] == "index_page"]
    assert pages and pages[-1]["fields"]["count"] == 2


@pytest.mark.parametrize("feature", ["watchlist", "history"])
def test_interactive_removal_only_deletes_selected_remote_item(env, config_base, monkeypatch, feature):
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync import run

    cfg, source = interactive_setup(env, config_base, monkeypatch, feature, "one-way")
    cfg["sync"].update(enable_remove=True, allow_mass_delete=True)
    cfg["pairs"][0]["features"][feature].update(remove=True, remove_mode="mirror", rewatches=feature == "history")
    if feature == "history":
        env.server.play(MOVIE)
        env.server.play(MOVIE, LATER)
    else:
        env.server.planning = [MOVIE, SHOW]
        source.index = {"tmdb:155": item(MOVIE), "tmdb:1396": item(SHOW)}
        assert not run(cfg, InteractivePlan(preview=False))["errors"]
        source.index.pop("tmdb:1396")
    plan = InteractivePlan()
    assert not run(cfg, plan)["errors"]
    assert len(plan.rows) == (2 if feature == "history" else 1)
    assert all(row["operation"] == "remove" for row in plan.rows.values())
    assert all(method == "GET" for method, _, _ in env.server.calls)
    chosen = next(rid for rid, row in plan.rows.items() if row["item"].get("watched_at") == LATER) if feature == "history" else next(
        rid for rid, row in plan.rows.items() if row["item"].get("type") == "show")
    assert not run(cfg, InteractivePlan(preview=False, selected={chosen}))["errors"]
    if feature == "history":
        assert len(env.server.history) == 1 and env.server.history[0]["watched_at"] == WHEN
        assert [path for method, path, _ in env.server.calls if method == "DELETE"] == ["/sync/tracklogs/play-2"]
    else:
        assert [row["type"] for row in env.server.planning] == ["movie"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_interactive_wetrakr_source_only_sends_selected_item(env, config_base, monkeypatch, mode):
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync import run

    cfg, target = interactive_setup(env, config_base, monkeypatch, "watchlist", mode)
    cfg["pairs"][0].update(source="WETRAKR", source_instance="P01", target="SRC", target_instance="default")
    env.server.planning = [MOVIE, SHOW]
    plan = InteractivePlan()
    assert not run(cfg, plan)["errors"]
    assert len(plan.rows) == 2 and not target.add_calls
    chosen = next(rid for rid, row in plan.rows.items() if row["item"]["type"] == "show")
    assert not run(cfg, InteractivePlan(preview=False, selected={chosen}))["errors"]
    assert len(target.add_calls) == 1 and [row["type"] for row in target.add_calls[0]] == ["show"]
    assert all(method == "GET" for method, _, _ in env.server.calls)


def test_interactive_stale_selection_does_not_write_changed_item(env, config_base, monkeypatch):
    from cw_platform.orchestrator._interactive import InteractivePlan
    from test_interactive_sync import run

    cfg, source = interactive_setup(env, config_base, monkeypatch, "watchlist", "one-way")
    source.index = {"tmdb:155": item(MOVIE)}
    plan = InteractivePlan()
    assert not run(cfg, plan)["errors"] and len(plan.rows) == 1
    source.index["tmdb:155"]["title"] = "Changed after preview"
    assert not run(cfg, InteractivePlan(preview=False, selected=set(plan.rows)))["errors"]
    assert all(method == "GET" for method, _, _ in env.server.calls)

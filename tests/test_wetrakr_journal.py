# tests/test_wetrakr_journal.py
# CrossWatch - WeTrakr journal reconciliation and checkpoint safety
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import json
import time
from types import SimpleNamespace

import pytest

from providers.sync.wetrakr import _common as common
from test_wetrakr_sync import EPISODE, LATER, MOVIE, SHOW, WHEN, Response, env

MARK = "2026-09-16T21:09:53Z"
SEASON = {"id": 233725, "type": "season", "title": "Season 1", "number": 1, "show": SHOW}


def paged(rows, params):
    limit, page = params.get("limit", 100), params.get("page", 1)
    return Response(rows[(page - 1) * limit:page * limit], headers={
        "X-Pagination-Page": str(page), "X-Pagination-Page-Count": str((len(rows) + limit - 1) // limit),
        "X-Pagination-Item-Count": str(len(rows))})


def entry(kind="movie", feature="watchlist", **extra):
    return {"category": {"watchlist": "planning", "history": "watched", "ratings": "ratings"}[feature],
            "type": kind, "id": 126, "action_at": LATER, "status": "removed", **extra}


def setup_removal(env, feature="watchlist", kind="movie", size=400, *, fallback_ratings=False):
    media = {"movie": MOVIE, "show": SHOW, "season": SEASON, "episode": EPISODE}[kind]
    rows = [{**copy.deepcopy(media), "id": 126 + i, "ids": {"tmdb": 10000 + i}} for i in range(size)]
    if kind in ("season", "episode"):
        rows = [{**row, "number": i + 1} for i, row in enumerate(rows)]
    if feature == "ratings":
        rows = [{**row, "interactions": {"user": {"rating": {"rating": 7, "rated_at": WHEN}}}} for row in rows]
    if feature == "history":
        env.adapter.config["_cw_history_rewatches"] = True
        rows = [{"id": f"play-{i}", "watched_at": WHEN, kind: row} for i, row in enumerate(rows)]
    activity = {"all": MARK, "last_tracking_planning_at": WHEN, "last_tracking_watched_at": WHEN,
                "last_tracking_removed_at": WHEN, "last_updated_at": WHEN, "last_removed_at": WHEN}
    env.server.activities = {name: copy.deepcopy(activity) for name in ("movies", "shows", "seasons", "episodes", "ratings")}
    if fallback_ratings:
        env.server.activities.pop("ratings")
        env.server.activities["all"] = MARK
        for name in ("movies", "shows", "seasons", "episodes"):
            env.server.activities[name]["last_rated_at"] = WHEN
    endpoint = f"/sync/ratings/{kind}s" if feature == "ratings" else f"/sync/tracking/{'watched/history' if feature == 'history' else 'planning'}/{kind}s"
    state = SimpleNamespace(rows=rows, journal=[entry(kind, feature, **({"play_id": "play-0"} if feature == "history" else {}))],
                            journal_error=None)

    def hook(method, path, kwargs):
        if path == "/sync/last_activities":
            return Response(env.server.activities)
        if path == "/sync/journal":
            if state.journal_error:
                return state.journal_error
            response = paged(state.journal, kwargs["params"])
            response.data = {"retention_days": 30, "journal": response.data}
            return response
        if path.startswith("/sync/"):
            return paged(state.rows if path == endpoint else [], kwargs.get("params", {}))
        raise AssertionError(path)

    env.server.hook = hook
    original = env.adapter.build_index(feature)
    state.rows = state.rows[1:]
    section = "ratings" if feature == "ratings" and not fallback_ratings else f"{kind}s"
    env.server.activities[section]["all"] = LATER
    env.server.activities[section]["last_removed_at" if feature == "ratings" else "last_tracking_removed_at"] = LATER
    if fallback_ratings:
        env.server.activities["all"] = LATER
    env.server.calls.clear()
    return state, original, endpoint


@pytest.mark.parametrize("feature,kind", [("history", "movie"), ("history", "episode")])
def test_removal_reconciles_large_snapshot_and_reuses_checkpoint(env, feature, kind):
    state, original, endpoint = setup_removal(env, feature, kind)
    cache = common.cache_path(env.adapter, feature)
    before = json.loads(cache.read_text())["sections"][f"{kind}s"]
    current = env.adapter.build_index(feature)
    assert len(current) == len(original) - 1
    assert not any(row["ids"]["wetrakr"] == "126" for row in current.values())
    assert [kw["params"]["limit"] for _, path, kw in env.server.calls if path == endpoint] == [1]
    after = json.loads(cache.read_text())["sections"][f"{kind}s"]
    assert after["journal_at"] == LATER and after["checked_at"] == before["checked_at"]
    env.server.calls.clear()
    assert env.adapter.build_index(feature) == current
    assert [path for _, path, _ in env.server.calls] == ["/sync/last_activities"]


def test_ratings_without_global_section_use_account_activity(env):
    setup_removal(env, "ratings", "episode", fallback_ratings=True)
    assert len(env.adapter.build_index("ratings")) == 399
    assert not any(path == "/sync/journal" for _, path, _ in env.server.calls)
    env.server.calls.clear()
    assert len(env.adapter.build_index("ratings")) == 399
    assert [path for _, path, _ in env.server.calls] == ["/sync/last_activities"]


@pytest.mark.parametrize("reason", ["empty", "partial", "mixed", "wrong_owner", "expired", "small", "stale", "force"])
def test_unsafe_or_unhelpful_journal_falls_back_to_full_snapshot(env, reason):
    feature = "history"
    state, original, endpoint = setup_removal(env, feature, size=2 if reason == "small" else 400)
    if reason == "empty":
        state.journal = []
    elif reason == "partial":
        state.rows = state.rows[1:]
    elif reason == "mixed":
        state.journal.append(entry(feature=feature, id=127, status="added"))
    elif reason == "wrong_owner":
        state.journal[0]["id"] = 999
    elif reason == "expired":
        state.journal_error = Response({}, 400)
    elif reason == "stale":
        path = common.cache_path(env.adapter, feature)
        data = json.loads(path.read_text())
        data["sections"]["movies"]["checked_at"] = time.time() - common.MAX_CACHE_AGE - 1
        path.write_text(json.dumps(data))
    current = env.adapter.build_index(feature, force_refresh=reason == "force")
    assert len(current) == len(state.rows)
    assert any(kw["params"]["limit"] == 100 for _, path, kw in env.server.calls if path == endpoint)
    if reason in ("small", "stale", "force"):
        assert not any(path == "/sync/journal" for _, path, _ in env.server.calls)


def test_deleted_rewatch_preserves_other_play_of_same_title(env):
    state, _, endpoint = setup_removal(env, "history")
    cache = common.cache_path(env.adapter, "history")
    data = json.loads(cache.read_text())
    retained = {"id": "play-retained", "watched_at": LATER, "movie": copy.deepcopy(data["sections"]["movies"]["rows"][0]["movie"])}
    data["sections"]["movies"]["rows"].append(retained)
    cache.write_text(json.dumps(data))
    state.rows.append(retained)
    current = env.adapter.build_index("history")
    assert [row["_wetrakr_history_id"] for row in current.values() if row["ids"]["wetrakr"] == "126"] == ["play-retained"]
    assert [kw["params"]["limit"] for _, path, kw in env.server.calls if path == endpoint] == [1]


def test_removal_with_watched_activity_merges_current_play_edits(env):
    state, _, endpoint = setup_removal(env, "history")
    env.server.activities["movies"]["last_tracking_watched_at"] = LATER
    state.rows[5]["watched_at"] = LATER
    original = env.server.hook

    def hook(method, path, kwargs):
        if path == endpoint and "from_date" in kwargs["params"]:
            return paged([state.rows[5]], kwargs["params"])
        return original(method, path, kwargs)

    env.server.hook = hook
    current = env.adapter.build_index("history")
    assert len(current) == 399
    assert next(row for row in current.values() if row["_wetrakr_history_id"] == "play-6")["watched_at"] == LATER
    reads = [kw["params"] for _, path, kw in env.server.calls if path == endpoint]
    assert len(reads) == 2 and "from_date" in reads[0] and reads[1]["limit"] == 1


def test_cost_gate_includes_incremental_history_request(env):
    setup_removal(env, "history", size=300)
    env.server.activities["movies"]["last_tracking_watched_at"] = LATER
    assert len(env.adapter.build_index("history")) == 299
    assert not any(path == "/sync/journal" for _, path, _ in env.server.calls)


@pytest.mark.parametrize("status", [401, 403, 429])
def test_journal_auth_or_quota_failure_preserves_cache(env, status):
    state, _, _ = setup_removal(env, "history")
    cache = common.cache_path(env.adapter, "history")
    before = cache.read_bytes()
    state.journal_error = Response({}, status, {"Retry-After": "60"})
    with pytest.raises(common.WeTrakrSyncError):
        env.adapter.build_index("history")
    assert cache.read_bytes() == before


@pytest.mark.parametrize("feature,kind", [("watchlist", "movie"), ("watchlist", "show"),
    ("ratings", "movie"), ("ratings", "show"), ("ratings", "season"), ("ratings", "episode")])
def test_non_history_removals_keep_existing_read_path(env, feature, kind):
    _, _, endpoint = setup_removal(env, feature, kind)
    assert len(env.adapter.build_index(feature)) == 399
    assert not any(path == "/sync/journal" for _, path, _ in env.server.calls)
    assert any(kw["params"]["limit"] == 100 for _, path, kw in env.server.calls if path == endpoint)


def test_watched_state_does_not_replay_event_removals(env):
    state, _, _ = setup_removal(env, "history")
    env.adapter.config["_cw_history_rewatches"] = False
    old = {"rows": state.rows, "activity": env.server.activities["movies"], "checked_at": time.time()}
    assert common.journal_removals(env.adapter, "history", "movie", old, old["activity"], "/unused", {}) is None
    assert not env.server.calls


@pytest.mark.parametrize("fault", [None, "count", "order", "duplicate", "category", "status", "page", "shape"])
def test_journal_pagination_ties_and_invalid_feeds(env, fault):
    rows = [entry(id=126), entry(id=127)]
    if fault == "order":
        rows[1]["action_at"] = MARK
    elif fault == "duplicate":
        rows[1] = rows[0]
    elif fault in ("category", "status"):
        rows[1][fault] = "invalid"

    def hook(method, path, kwargs):
        page = kwargs["params"]["page"]
        response = paged(rows, {"page": page, "limit": 1})
        response.data = {"retention_days": 30, "journal": response.data}
        if fault == "count" and page == 2:
            response.headers["X-Pagination-Item-Count"] = "3"
        if fault == "page":
            response.headers["X-Pagination-Page"] = "9"
        if fault == "shape":
            response.data = []
        return response

    env.server.hook = hook
    if fault:
        with pytest.raises(common.WeTrakrSyncError):
            common.journal_rows(env.adapter, "watchlist", WHEN)
    else:
        result, checkpoint = common.journal_rows(env.adapter, "watchlist", WHEN)
        assert result == rows and checkpoint == LATER
        assert [kw["params"]["from_date"] for _, _, kw in env.server.calls] == [WHEN, WHEN]


def test_activity_change_during_reconciliation_does_not_commit(env):
    setup_removal(env, "history")
    cache = common.cache_path(env.adapter, "history")
    before = cache.read_bytes()
    original = env.server.hook

    def hook(method, path, kwargs):
        response = original(method, path, kwargs)
        if path == "/sync/journal":
            env.server.activities["movies"]["all"] = "2026-09-18T21:09:53Z"
        return response

    env.server.hook = hook
    with pytest.raises(common.WeTrakrSyncError, match="snapshot_changed"):
        env.adapter.build_index("history")
    assert cache.read_bytes() == before


@pytest.mark.parametrize("fault", ["missing_row", "changed_row", "bad_pages"])
def test_invalid_count_probe_requires_full_snapshot(env, fault):
    setup_removal(env, "history")
    original = env.server.hook

    def hook(method, path, kwargs):
        response = original(method, path, kwargs)
        if "/history/" in path and kwargs["params"]["limit"] == 1:
            if fault == "missing_row":
                response.data = []
            elif fault == "changed_row":
                response.data[0]["movie"]["title"] = "Changed title"
            else:
                response.headers["X-Pagination-Page-Count"] = "1"
        return response

    env.server.hook = hook
    assert len(env.adapter.build_index("history")) == 399
    assert any("/history/" in path and kw["params"]["limit"] == 100 for _, path, kw in env.server.calls)


def test_cancellation_does_not_advance_checkpoint(env, monkeypatch):
    setup_removal(env, "history")
    cache = common.cache_path(env.adapter, "history")
    before = cache.read_bytes()
    original = env.server.hook
    cancelled = False

    def hook(method, path, kwargs):
        nonlocal cancelled
        response = original(method, path, kwargs)
        if path == "/sync/journal":
            cancelled = True
        return response

    def check():
        if cancelled:
            raise RuntimeError("cancelled")

    env.server.hook = hook
    monkeypatch.setattr(common, "raise_if_cancelled", check)
    with pytest.raises(RuntimeError, match="cancelled"):
        env.adapter.build_index("history")
    assert cache.read_bytes() == before


def test_capture_reads_full_snapshot(env, monkeypatch):
    setup_removal(env, "history")
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setenv("CW_CAPTURE_PROVIDER", "WETRAKR")
    assert len(env.adapter.build_index("history")) == 399
    assert not any(path == "/sync/journal" for _, path, _ in env.server.calls)


def test_legacy_cache_without_journal_checkpoint_is_supported(env):
    setup_removal(env, "history")
    path = common.cache_path(env.adapter, "history")
    data = json.loads(path.read_text())
    data["sections"]["movies"].pop("journal_at")
    path.write_text(json.dumps(data))
    assert len(env.adapter.build_index("history")) == 399
    assert json.loads(path.read_text())["sections"]["movies"]["journal_at"] == LATER

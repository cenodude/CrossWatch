# /tests/test_plex_discover_history.py
# Plex account watched-state opt-in and recovery regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace as NS

import pytest
import requests

from providers.sync.plex import _common as common, _discover_history as cloud, _history as history

NOW = 2_000_000_000
WATCHED = 1_500_000_000
MOVIE = {"type": "movie", "title": "Example", "ids": {"tmdb": "123"}, "watched_at": common.iso_from_epoch(WATCHED)}


@pytest.fixture
def remote(tmp_path, monkeypatch):
    monkeypatch.setattr(common, "STATE_DIR", tmp_path)
    monkeypatch.setattr(common, "_is_capture_mode", lambda: False)
    monkeypatch.setattr(cloud.time, "time", lambda: NOW)
    state = NS(calls=[], watched=False, nodes=[], marks=0, edits=0, fail_date=False, fail_mark=False,
               delay=False, rate_limit=False, extra=False, child_pages=[], restricted=False, sleeps=[], on_sleep=None)
    def sleep(seconds):
        state.sleeps.append(seconds)
        if state.on_sleep:
            state.on_sleep()
    monkeypatch.setattr(cloud.time, "sleep", sleep)
    adapter = NS(config={"plex": {"account_token": "account-secret", "history": {"discover_fallback": True}}},
                 client=NS(user_username="owner", server=NS()))
    def request(method, url, **kw):
        state.calls.append((method, url, kw))
        data = {}
        status = 200
        if url.endswith("/api/v2/user"):
            data = {"uuid": "account-uuid", "username": "owner", "restricted": state.restricted}
        elif url.endswith("/matches"):
            kind = "movie" if kw["params"]["type"] == 1 else "show"
            data = {"MediaContainer": {"Metadata": [dict(type=kind, ratingKey="catalog", Guid=[{"id": "tmdb://123"}])]}}
        elif url.endswith("/children"):
            state.child_pages.append(kw["headers"])
            rows = ([dict(type="season", ratingKey="season", index=0)] if "/catalog/" in url else
                    [dict(type="episode", ratingKey="special", index=1)])
            data = {"MediaContainer": {"Metadata": rows, "totalSize": len(rows)}}
        elif url.endswith("/userState"):
            data = {"MediaContainer": {"UserState": [{"viewCount": int(state.watched)}]}}
        elif url.endswith("/actions/scrobble"):
            journal = json.loads(next(tmp_path.glob("*.json")).read_text())
            assert kw["params"]["key"] in journal["pending"]
            state.marks += 1
            if state.fail_mark:
                raise requests.Timeout()
            state.watched = True
            if not state.delay:
                state.nodes.append({"id": "new-activity", "date": common.iso_from_epoch(NOW),
                                    "metadataItem": {"id": kw["params"]["key"]}})
                if state.extra:
                    state.nodes.append(dict(state.nodes[-1], id="another-activity"))
        elif url == cloud.COMMUNITY:
            payload = kw["json"]
            if payload["query"].startswith("mutation"):
                state.edits += 1
                if state.fail_date:
                    raise requests.Timeout()
                if state.rate_limit:
                    data = {"errors": [{"extensions": {"code": "RATE_LIMITED", "retryAfter": 120}}]}
                else:
                    data = {"data": {"updateActivity": {"id": payload["variables"]["id"]}}}
            else:
                data = {"data": {"user": {"watchHistory": {"nodes": list(state.nodes), "pageInfo": {"hasNextPage": False}}}}}
        else:
            raise AssertionError(url)
        response = requests.Response()
        response.status_code = status
        response._content = json.dumps(data).encode()
        return response
    monkeypatch.setattr(requests.Session, "request", lambda self, *a, **kw: request(*a, **kw))
    state.adapter = adapter
    state.writer = cloud.Writer(adapter)
    state.path = lambda: next(tmp_path.glob("*.json"))
    yield state
    state.writer.close()


@pytest.mark.parametrize("value", [None, False, "true", 1])
def test_cloud_requires_explicit_boolean_opt_in(remote, value):
    remote.adapter.config["plex"]["history"]["discover_fallback"] = value
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_disabled"
    assert not remote.calls


def test_cloud_marks_once_then_restores_original_date(remote):
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    assert (remote.marks, remote.edits) == (1, 1)
    mutations = [kw["json"] for method, url, kw in remote.calls if url == cloud.COMMUNITY and kw["json"]["query"].startswith("mutation")]
    assert mutations[0]["variables"] == {"id": "new-activity", "input": {"date": common.iso_from_epoch(WATCHED)}}
    assert json.loads(remote.path().read_text())["pending"] == {}
    assert remote.writer.add(MOVIE, WATCHED) == "cached"
    assert (remote.marks, remote.edits) == (1, 1)
    assert "account-secret" not in remote.path().read_text()


def test_cloud_already_watched_never_changes_date(remote):
    remote.watched = True
    assert remote.writer.add(MOVIE, WATCHED) == "present"
    assert remote.marks == remote.edits == 0


def test_date_failure_is_retried_after_restart_without_scrobbling(remote):
    remote.fail_date = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_request_failed"
    assert json.loads(remote.path().read_text())["pending"]["catalog"]["activity"] == "new-activity"
    remote.fail_date = False
    writer = cloud.Writer(remote.adapter)
    try:
        assert writer.add(MOVIE, WATCHED + 100) == "present"
    finally:
        writer.close()
    assert remote.marks == 1 and remote.edits == 2
    assert remote.calls[-1][2]["json"]["variables"]["input"]["date"] == common.iso_from_epoch(WATCHED)


def test_delayed_activity_stays_pending_and_can_be_repaired(remote):
    remote.delay = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.marks == 1 and remote.edits == 0
    remote.nodes.append({"id": "later", "date": common.iso_from_epoch(NOW), "metadataItem": {"id": "catalog"}})
    writer = cloud.Writer(remote.adapter)
    try:
        assert writer.add(MOVIE, WATCHED) == "dated"
    finally:
        writer.close()
    assert remote.marks == 1


def test_slow_activity_is_dated_in_the_same_run(remote):
    remote.delay = True
    remote.on_sleep = lambda: remote.nodes.append({"id": "slow", "date": common.iso_from_epoch(NOW), "metadataItem": {"id": "catalog"}}) if remote.sleeps == [2, 4] else None
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    assert remote.sleeps == [2, 4]
    assert (remote.marks, remote.edits) == (1, 1)
    assert json.loads(remote.path().read_text())["pending"] == {}


def test_missing_activity_waits_briefly_then_stays_pending(remote):
    remote.delay = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.sleeps == list(cloud._SETTLE_DELAYS)
    writer = cloud.Writer(remote.adapter)
    try:
        assert writer.repair_pending() == {"catalog": "plex_discover_date_pending"}
    finally:
        writer.close()
    assert remote.sleeps == list(cloud._SETTLE_DELAYS)


def test_settle_retry_never_spends_the_last_history_page(remote):
    remote.delay = True
    remote.writer.history_pages = 97
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.sleeps == []
    assert not json.loads(remote.path().read_text()).get("retry_at")


def _journal(since, spent):
    path = common.STATE_DIR / ("plex_discover_history." + cloud.sha256(b"account-uuid").hexdigest() + ".json")
    path.write_text(json.dumps({"settle": {"since": since, "spent": spent}}))
    return path


def test_settle_waits_are_capped_across_writers(remote):
    remote.delay = True
    path = _journal(NOW - 10, cloud._SETTLE_BUDGET - 6)
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.sleeps == [2, 4]
    assert json.loads(path.read_text())["settle"]["spent"] == cloud._SETTLE_BUDGET


def test_settle_allowance_renews_after_the_window(remote):
    remote.delay = True
    path = _journal(NOW - cloud._SETTLE_WINDOW, cloud._SETTLE_BUDGET)
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.sleeps == list(cloud._SETTLE_DELAYS)
    assert json.loads(path.read_text())["settle"] == {"since": NOW, "spent": sum(cloud._SETTLE_DELAYS)}


def test_ambiguous_new_activities_are_not_edited(remote):
    remote.extra = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.edits == 0
    assert remote.sleeps == []


def test_failed_mark_is_not_blindly_repeated(remote, monkeypatch):
    remote.fail_mark = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_request_failed"
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.marks == 1
    remote.fail_mark = False
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 61)
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.marks == 1


def test_later_unrelated_activity_is_never_backdated(remote, monkeypatch):
    remote.delay = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    monkeypatch.setattr(cloud.time, "time", lambda: NOW + 1000)
    remote.nodes.append({"id": "unrelated", "date": common.iso_from_epoch(NOW + 999), "metadataItem": {"id": "catalog"}})
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_date_pending"
    assert remote.edits == 0


def test_rate_limit_persists_across_restart(remote):
    remote.rate_limit = True
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_rate_limited"
    journal = json.loads(remote.path().read_text())
    assert journal["retry_at"] == NOW + 120
    writer = cloud.Writer(remote.adapter)
    try:
        assert writer.add(MOVIE, WATCHED) == "plex_discover_rate_limited"
    finally:
        writer.close()
    assert remote.marks == remote.edits == 1


def test_six_date_update_budget_defers_before_another_mark(remote):
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    state = json.loads(remote.path().read_text())
    state["edits"] = [NOW] * 6
    state["completed"] = {}
    remote.path().write_text(json.dumps(state))
    remote.watched = False
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_rate_limited"
    assert remote.marks == 1


def test_specials_are_matched_by_exact_show_and_children(remote):
    item = dict(type="episode", show_ids={"tmdb": "123"}, season=0, episode=1)
    assert remote.writer.add(item, WATCHED) == "dated"
    marks = [kw for _, url, kw in remote.calls if url.endswith("/actions/scrobble")]
    assert marks[0]["params"]["key"] == "special"
    assert remote.child_pages == [{"X-Plex-Container-Start": "0", "X-Plex-Container-Size": "200"}] * 2


def test_children_are_paged_with_headers_and_ambiguous_numbers_are_rejected(remote, monkeypatch):
    pages = []
    def metadata(path, **kwargs):
        start = int(kwargs["headers"]["X-Plex-Container-Start"])
        pages.append(start)
        rows = [dict(type="episode", index=n + 1, ratingKey=str(n + 1)) for n in range(start, min(start + 200, 201))]
        return {"Metadata": rows, "totalSize": 201}
    monkeypatch.setattr(remote.writer, "_metadata", metadata)
    assert remote.writer._child("season", "episode", 201) == "201"
    assert pages == [0, 200]
    remote.writer.cache[("children", "season")].append(dict(type="episode", index=201, ratingKey="duplicate"))
    with pytest.raises(cloud.CloudError, match="ambiguous"):
        remote.writer._child("season", "episode", 201)


def test_conflicting_external_ids_are_not_written(remote, monkeypatch):
    monkeypatch.setattr(remote.writer, "_metadata", lambda *a, **kw: {"Metadata": [
        dict(type="movie", ratingKey="one", Guid=[{"id": "tmdb://123"}, {"id": "imdb://tt999"}])]})
    assert remote.writer.add(dict(MOVIE, ids={"tmdb": "123", "imdb": "tt123"}), WATCHED) == "plex_discover_ambiguous_match"
    assert remote.marks == 0


def test_corrupt_pending_journal_blocks_writes(remote):
    assert remote.writer.add(MOVIE, WATCHED) == "dated"
    remote.path().write_text("invalid")
    remote.watched = False
    assert remote.writer.add(MOVIE, WATCHED) == "plex_discover_request_failed"
    assert remote.marks == 1


@pytest.mark.parametrize("case", ["no_ids", "wrong_ids", "no_token", "wrong_account", "managed", "capture"])
def test_cloud_rejects_unsafe_or_unsupported_inputs(remote, monkeypatch, case):
    item = dict(MOVIE)
    if case == "no_ids":
        item["ids"] = {}
    elif case == "wrong_ids":
        item["ids"] = {"tmdb": "456"}
    elif case == "no_token":
        remote.writer.token = ""
    elif case == "wrong_account":
        remote.adapter.client.selected_username = "someone-else"
    elif case == "managed":
        remote.restricted = True
    else:
        monkeypatch.setattr(common, "_is_capture_mode", lambda: True)
    assert remote.writer.add(item, WATCHED) not in {"dated", "present"}
    assert remote.marks == remote.edits == 0


@pytest.mark.parametrize("opt_in,klass,home,outcome,expected", [
    (False, history.CLASS_NOT_IN_PLEX_CATALOG, False, "dated", 0),
    (True, history.CLASS_NOT_IN_PLEX_CATALOG, False, "dated", 1),
    (True, history.CLASS_NOT_IN_PLEX_CATALOG, False, "plex_discover_date_pending", 0),
    (True, history.CLASS_RESOLVE_AMBIGUOUS, False, "dated", 0),
    (True, history.CLASS_NOT_IN_PLEX_CATALOG, True, "dated", 0),
])
def test_history_writer_cloud_fallback_is_gated(monkeypatch, opt_in, klass, home, outcome, expected):
    adapter = NS(config={"plex": {"history": {"discover_fallback": opt_in}}}, client=NS(server=NS()))
    calls = []
    catalog = NS(resolve=lambda *a, **kw: (None, klass), by_rk={})
    monkeypatch.setattr(history, "home_scope_enter", lambda a: (home, home, None, None))
    monkeypatch.setattr(history, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(history, "_get_history_catalog", lambda *a, **kw: catalog)
    monkeypatch.setattr(history, "resolve_path", lambda *a: (None, None))
    monkeypatch.setattr(history, "_resolve_rating_key", lambda *a, **kw: None)
    monkeypatch.setattr(history, "_maybe_trace", lambda *a, **kw: None)
    monkeypatch.setattr(history, "_shadow_add_batch", lambda *a: None)
    monkeypatch.setattr(cloud, "Writer", lambda a: NS(add=lambda *args: calls.append(args) or outcome, close=lambda: None,
                                                    repair_pending=lambda: {}))
    count, unresolved = history.add(adapter, [MOVIE])
    assert count == expected
    assert bool(unresolved) == (expected == 0)
    assert len(calls) == int(opt_in and not home and klass != history.CLASS_RESOLVE_AMBIGUOUS)
    if outcome == "dated" and expected:
        assert adapter._plex_history_write_meta["accepted_not_seen_live_keys"]
        assert not adapter._plex_history_write_meta["date_confirmed_keys"]


def test_cloud_fallback_does_not_bypass_selected_libraries(monkeypatch):
    adapter = NS(config={"plex": {"history": {"discover_fallback": True, "libraries": [1]}}}, client=NS(server=NS()))
    missing = NS(resolve=lambda *a, **kw: (None, history.CLASS_NOT_IN_PLEX_CATALOG), by_rk={})
    present = NS(resolve=lambda *a, **kw: ("elsewhere", history.CLASS_IN_CATALOG_UNWATCHED), by_rk={})
    monkeypatch.setattr(history, "home_scope_enter", lambda a: (False, False, None, None))
    monkeypatch.setattr(history, "home_scope_exit", lambda *a: None)
    monkeypatch.setattr(history, "_get_history_catalog", lambda a, allow, **kw: missing if allow else present)
    monkeypatch.setattr(history, "_populate_catalog_episode_leaves", lambda *a, **kw: True)
    monkeypatch.setattr(history, "resolve_path", lambda *a: (None, None))
    monkeypatch.setattr(history, "_maybe_trace", lambda *a, **kw: None)
    monkeypatch.setattr(history, "_shadow_add_batch", lambda *a: None)
    calls = []
    monkeypatch.setattr(cloud, "Writer", lambda a: NS(add=lambda *args: calls.append(args) or "dated", close=lambda: None,
                                                    repair_pending=lambda: {}))
    count, unresolved = history.add(adapter, [MOVIE])
    assert count == 0 and not calls
    assert unresolved[0]["reason"] == "plex_discover_outside_library_scope"

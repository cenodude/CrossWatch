# tests/test_mdblist_history_acknowledgments.py
# CrossWatch - MDBList duplicate and merged play acknowledgments
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from cw_platform.history_events import history_sync_key
from cw_platform.orchestrator import _applier
from cw_platform.orchestrator._pairs_oneway import compute_effective_add, select_baseline_keys
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync import _mod_MDBLIST as module
from providers.sync.mdblist import _common, _history as history
from tests.test_history_baseline_native import HistoryOps, _baseline, _cfg


FIRST = "2026-11-08T00:10:32Z"
MERGED = "2026-11-08T03:10:32Z"
LATER = "2026-11-09T00:10:32Z"


def adapter(instance="default", account="test-account"):
    return SimpleNamespace(
        config={"_cw_history_rewatches": True, "mdblist": {"api_key": account, "history_write_delay_ms": 0}},
        instance_id=instance,
        cfg=SimpleNamespace(timeout=1, max_retries=0),
        client=SimpleNamespace(session=object()),
    )


def item(kind="movie", watched_at=MERGED):
    result = {"type": kind, "ids": {"tmdb": 70667}, "watched_at": watched_at}
    if kind == "episode":
        result.update(ids={"tmdb": 301}, show_ids={"tmdb": 42}, season=1, episode=2)
    result.update(_cw_rewatch_sync=True, _cw_event_key=history_sync_key(result, event_mode=True))
    return result


def play(value, added=False):
    result = {"type": value["type"], "watched_at": value["watched_at"].replace("Z", ".000Z"), "added": added}
    if value["type"] == "movie":
        result["ids"] = value["ids"]
    else:
        result["episode"] = {"ids": value["ids"], "season": value["season"], "number": value["episode"],
                             "show": {"ids": value["show_ids"]}}
    return result


def snapshot(*values):
    result = {"movies": [], "episodes": [], "pagination": {"has_more": False}}
    for number, value in enumerate(values, 1):
        row = play(value)
        row["play_id"] = number
        if value["type"] == "movie":
            row["movie"] = {"ids": row.pop("ids")}
        result[value["type"] + "s"].append(row)
    return result


@pytest.fixture
def remote(tmp_path, monkeypatch):
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    calls, responses = [], []

    def request(adapter, method, url, **kwargs):
        calls.append((method, url, kwargs))
        status, payload = responses.pop(0)
        return SimpleNamespace(status_code=status, text=json.dumps(payload), json=lambda: payload)

    monkeypatch.setattr(history, "mdblist_request", request)
    return calls, responses


@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_acknowledgment_survives_restart_without_inventing_history(remote, kind):
    calls, responses = remote
    original, merged, later = (item(kind, when) for when in (FIRST, MERGED, LATER))
    responses.extend([(200, {"plays": [play(merged)]}), (200, snapshot(original)),
                      (200, {"plays": [play(later, True)]})])

    first = history.add(adapter(), [merged])
    assert first["ok"] and first["count"] == 0
    assert first["skipped_keys"] == [merged["_cw_event_key"]]
    assert first["confirmed_keys"] == first["presence_confirmed_keys"] == first["unresolved"] == []

    fresh = adapter()
    index = history.build_index(fresh)
    assert len(index) == 1
    assert {row["watched_at"] for row in index.values()} == {FIRST}
    result = history.add(fresh, [merged, later])
    assert result["skipped_keys"] == [merged["_cw_event_key"]]
    assert result["confirmed_keys"] == result["presence_confirmed_keys"] == [later["_cw_event_key"]]
    assert result["count"] == 1 and result["attempted"] == 2 and not result["unresolved"]
    assert len(calls) == 3
    assert MERGED not in json.dumps(calls[-1][2]["json"])


@pytest.mark.parametrize("change", ["instance", "account", "pair"])
def test_acknowledgments_are_scoped_to_destination(remote, monkeypatch, change):
    calls, responses = remote
    value = item()
    responses.extend([(200, {"plays": [play(value)]})] * 2)
    history.add(adapter(), [value])
    other = adapter(instance="second") if change == "instance" else adapter(account="second") if change == "account" else adapter()
    if change == "pair":
        monkeypatch.setenv("CW_PAIR_KEY", "another-pair")
    assert history.add(other, [value])["skipped_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 2


@pytest.mark.parametrize("visible", [True, False])
def test_full_snapshot_releases_visible_or_removed_acknowledgments(remote, visible):
    calls, responses = remote
    value = item()
    responses.extend([(200, {"plays": [play(value)]}), (200, snapshot(*([value] if visible else []))),
                      (200, {"plays": [play(value, True)]})])
    history.add(adapter(), [value])
    history.build_index(adapter())
    assert history.add(adapter(), [value])["confirmed_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 3


def test_incomplete_snapshot_does_not_forget_acknowledgments(remote):
    calls, responses = remote
    value = item()
    responses.extend([(200, {"plays": [play(value)]}), (500, {})])
    history.add(adapter(), [value])
    with pytest.raises(history.MDBListFetchError):
        history.build_index(adapter())
    assert history.add(adapter(), [value])["skipped_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 2


@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_removing_nearby_play_releases_acknowledgment_even_when_later_watch_remains(remote, kind):
    calls, responses = remote
    original, merged, later = (item(kind, when) for when in (FIRST, MERGED, "2026-11-10T00:10:32Z"))
    responses.extend([(200, {"plays": [play(merged)]}), (200, snapshot(original, later)),
                      (200, snapshot(later)), (200, {"plays": [play(merged, True)]})])
    history.add(adapter(), [merged])
    history.build_index(adapter())
    assert history.add(adapter(), [merged])["skipped_keys"] == [merged["_cw_event_key"]]
    index = history.build_index(adapter())
    assert {row["watched_at"] for row in index.values()} == {later["watched_at"]}
    result = history.add(adapter(), [merged])
    assert result["confirmed_keys"] == [merged["_cw_event_key"]]
    assert result["skipped_keys"] == result["unresolved"] == []
    assert len(calls) == 4


@pytest.mark.parametrize("kind", ["movie", "episode"])
@pytest.mark.parametrize("watched_at,keep", [("2026-11-08T12:10:31Z", True),
                                           ("2026-11-08T12:10:32Z", True),
                                           ("2026-11-08T12:10:33Z", False)])
def test_acknowledgment_merge_window_includes_exactly_twelve_hours(remote, kind, watched_at, keep):
    calls, responses = remote
    original, merged = item(kind, FIRST), item(kind, watched_at)
    responses.extend([(200, {"plays": [play(merged)]}), (200, snapshot(original))])
    history.add(adapter(), [merged])
    history.build_index(adapter())
    if keep:
        assert history.add(adapter(), [merged])["skipped_keys"] == [merged["_cw_event_key"]]
        responses.append((200, snapshot(original)))
        history.build_index(adapter())
        assert history.add(adapter(), [merged])["skipped_keys"] == [merged["_cw_event_key"]]
    else:
        responses.append((200, {"plays": [play(merged, True)]}))
        assert history.add(adapter(), [merged])["confirmed_keys"] == [merged["_cw_event_key"]]
    assert sum(method == "POST" for method, _, _ in calls) == (1 if keep else 2)


@pytest.mark.parametrize("payload", [{"plays": []}, {"updated": {"movies": 1}}, {"plays": [{}]},
                                    {"plays": [{"type": "movie", "ids": {"tmdb": 70667}, "watched_at": MERGED}]},
                                    {"plays": [], "not_found": {"movies": [{"ids": {"tmdb": 70667}}]}}])
def test_unacknowledged_watches_remain_retryable(remote, payload):
    calls, responses = remote
    value = item()
    responses.extend([(200, payload), (200, {"plays": [play(value, True)]})])
    result = history.add(adapter(), [value])
    assert not result["ok"] and result["unresolved"]
    assert result["confirmed_keys"] == result["skipped_keys"] == []
    assert history.add(adapter(), [value])["confirmed_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 2


def test_mixed_results_keep_missing_and_http_failures_unresolved(remote):
    calls, responses = remote
    duplicate, added, missing, failed = (item(watched_at=when) for when in (FIRST, MERGED, LATER, "2026-11-10T00:00:00Z"))
    responses.extend([(200, {"plays": [play(duplicate), play(added, True)]}), (400, {})])
    target = adapter()
    target.config["mdblist"]["history_chunk_size"] = 3
    result = history.add(target, [duplicate, added, missing, failed])
    assert result["confirmed_keys"] == [added["_cw_event_key"]]
    assert result["skipped_keys"] == [duplicate["_cw_event_key"]]
    assert {row["hint"] for row in result["unresolved"]} == {"http:400", "mdblist_play_report_missing"}
    assert len(calls) == 2


def test_skips_clear_failure_accounting_without_creating_baseline_entries(remote):
    _, responses = remote
    value = item()
    key = value["_cw_event_key"]
    responses.append((200, {"plays": [play(value)]}))
    raw = module._write_result(history.add(adapter(), [value]), [value])
    result = _applier._normalize(raw, [value], "apply:add", dst="MDBLIST", feature="history", emit=lambda *a, **k: None)
    assert result["confirmed"] == result["unresolved"] == result["errors"] == 0
    assert result["skipped"] == 1
    decision = compute_effective_add(attempted_keys=[key], prov_confirmed=0, confirmed_keys=[],
                                     still_unresolved={key}, skipped_keys=result["skipped_keys"], have_exact_keys=True,
                                     verify_after_write=False, provider_skipped=True)
    assert decision["success_keys"] == [key]
    assert decision["failed_keys"] == []
    assert select_baseline_keys(decision["success_keys"], result) == []


def test_changed_episode_coordinates_are_not_suppressed(remote):
    calls, responses = remote
    value = item("episode")
    value["ids"] = {}
    responses.append((200, {"plays": [play(value)]}))
    history.add(adapter(), [value])
    changed = dict(value, episode=3)
    responses.append((200, {"plays": [play(changed, True)]}))
    result = history.add(adapter(), [changed])
    assert result["confirmed_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 2


def test_conflicting_not_found_report_is_not_remembered(remote):
    calls, responses = remote
    value = item()
    responses.extend([(200, {"plays": [play(value)], "not_found": {"movies": [{"ids": value["ids"]}]}}),
                      (200, {"plays": [play(value, True)]})])
    result = history.add(adapter(), [value])
    assert result["unresolved"] and not result["skipped_keys"]
    assert history.add(adapter(), [value])["confirmed_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 2


def test_acknowledged_movie_does_not_skip_show_with_same_id(remote):
    calls, responses = remote
    value = item()
    responses.extend([(200, {"plays": [play(value)]}), (200, {"updated": {"shows": 1}})])
    history.add(adapter(), [value])
    result = history.add(adapter(), [dict(value, type="show")])
    assert result["confirmed_keys"] == [value["_cw_event_key"]]
    assert len(calls) == 2


@pytest.mark.parametrize("kind", ["movie", "episode"])
def test_richer_destination_ids_do_not_invalidate_acknowledgments(remote, kind):
    calls, responses = remote
    merged = item(kind)
    original = item(kind, FIRST)
    if kind == "movie":
        merged["ids"] = {"imdb": "tt1613750"}
        original["ids"]["imdb"] = "tt1613750"
    else:
        merged["ids"] = {}
        merged["show_ids"] = {"tvdb": 123}
        original["show_ids"]["tvdb"] = 123
    responses.extend([(200, {"plays": [play(merged)]}), (200, snapshot(original))])
    history.add(adapter(), [merged])
    history.build_index(adapter())
    result = history.add(adapter(), [merged])
    assert result["skipped_keys"] == [merged["_cw_event_key"]]
    assert len(calls) == 2


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_repeated_syncs_do_not_resend_merged_plays_or_add_them_to_baselines(config_base, remote, monkeypatch, mode):
    original, merged = item(watched_at=FIRST), item()
    posts = []

    def request(target, method, url, **kwargs):
        if method == "GET":
            payload = snapshot(original)
        else:
            posts.append(kwargs["json"])
            payload = {"plays": [play(merged)]}
        return SimpleNamespace(status_code=200, text=json.dumps(payload), json=lambda: payload)

    class Destination(HistoryOps):
        def build_index(self, cfg, *, feature):
            return history.build_index(adapter())

        def add(self, cfg, items, *, feature, dry_run=False):
            assert not dry_run
            batch = list(items)
            self.add_calls.append(batch)
            return module._write_result(history.add(adapter(), batch), batch)

    source = HistoryOps("SRC", {value["_cw_event_key"]: value for value in (original, merged)}, history_rewatches=True)
    target = Destination("DST", {}, history_rewatches=True)
    monkeypatch.setattr(history, "mdblist_request", request)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": source, "DST": target})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda *_: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *_a, **_k: None)
    cfg = _cfg("history", rewatches=True)
    cfg["pairs"][0]["mode"] = mode
    cfg["sync"]["blackbox"] = {"enabled": False}
    for _ in range(3):
        orchestrator = Orchestrator(cfg)
        orchestrator.run()
        baseline = _baseline(orchestrator, "DST", "history")
        assert set(baseline) == {original["_cw_event_key"]}
    assert len(posts) == 1
    assert len(target.add_calls) == 3
    assert source.add_calls == source.remove_calls == target.remove_calls == []

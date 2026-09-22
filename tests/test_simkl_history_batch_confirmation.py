# /tests/test_simkl_history_batch_confirmation.py
# CrossWatch - SIMKL batch confirmation and rewatch accounting regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cw_platform.history_events import minimal_history_item
from cw_platform.orchestrator import _applier
from providers.sync._mod_SIMKL import OPS, SIMKLModule
from providers.sync.simkl import _history as history


def _response(payload, status=200):
    return SimpleNamespace(status_code=status, ok=200 <= status < 300,
                           text=json.dumps(payload), json=lambda: payload)


def _episode(number, day=1, *, rewatches=True):
    item = {"type": "episode", "season": 1, "episode": number,
            "show_ids": {"tmdb": "96777", "tvdb": "399959"},
            "watched_at": f"2026-09-{day:02d}T12:00:00Z"}
    return minimal_history_item(item, event_mode=rewatches)


def _live(items):
    return {"shows": [{"show": {"title": "Only Murders in the Building",
                               "ids": {"tvdb": "399959", "simkl": "12345"}},
                       "seasons": [{"number": 1, "episodes": [
                           {"number": it["episode"], "watched_at": it["watched_at"]}
                           for it in items]}]}]}


@pytest.fixture
def env(config_base, monkeypatch):
    monkeypatch.setattr(history, "state_file", lambda name: config_base / name)
    monkeypatch.setattr(history, "_headers", lambda *a, **k: {})
    monkeypatch.setattr(history, "_rewatch_account_ok", lambda adapter: True)
    monkeypatch.setattr(history, "_inject_adds_into_cache", Mock())
    monkeypatch.setattr(history, "_remember_source_aliases", Mock())
    monkeypatch.setattr(_applier, "record_unresolved", Mock())
    session = SimpleNamespace(post=Mock(return_value=_response({
        "added": {"movies": 0, "shows": 0, "episodes": 0},
        "not_found": {"movies": [], "shows": [], "episodes": []},
    })), get=Mock(return_value=_response({})))
    adapter = SimpleNamespace(client=SimpleNamespace(session=session),
                              cfg=SimpleNamespace(timeout=5),
                              config={"_cw_history_rewatches": True}, raw_cfg={})
    return adapter, session


def _normalize(adapter, items, count, unresolved):
    return _applier._normalize({
        "ok": True, "count": count, "unresolved": unresolved,
        "confirmed_keys": adapter._simkl_history_add_confirmed_keys,
        "skipped_keys": adapter._simkl_history_add_skipped_keys,
    }, items, "apply:add", dst="SIMKL", feature="history", emit=lambda *a, **k: None)


def test_zero_add_batch_verifies_499_watches_and_keeps_only_missing_one_unresolved(env):
    adapter, session = env
    items = [_episode(n) for n in range(1, 501)]
    session.get.return_value = _response(_live(items[:-1]))
    count, unresolved = history.add(adapter, items)
    out = _normalize(adapter, items, count, unresolved)
    assert count == 0
    assert len(unresolved) == 1
    assert unresolved[0]["item"]["episode"] == 500
    assert (out["confirmed"], out["skipped_exact"], out["unresolved"]) == (0, 499, 1)
    assert out["skipped_inferred"] == 0
    session.post.assert_called_once()
    session.get.assert_called_once()
    assert session.get.call_args.args[0] == history.URL_ALL_ITEMS
    assert "date_from" not in session.get.call_args.kwargs["params"]
    assert session.get.call_args.kwargs["params"]["allow_rewatch"] == "yes"


def test_positive_batch_count_does_not_confirm_missing_rewatch_dates(env):
    adapter, session = env
    items = [_episode(1), _episode(1, 5), _episode(2)]
    session.post.return_value = _response({"added": {"episodes": 1}, "not_found": {}})
    session.get.return_value = _response(_live([items[0], items[2]]))
    count, unresolved = history.add(adapter, items)
    assert count == 2
    assert [row["item"]["watched_at"] for row in unresolved] == [items[1]["watched_at"]]
    out = _normalize(adapter, items, count, unresolved)
    assert out["confirmed"] == 2
    assert out["unresolved"] == 1
    assert out["skipped"] == 0


@pytest.mark.parametrize("rewatches", [False, True])
def test_zero_add_noop_requires_matching_watch_date_only_in_rewatch_mode(env, rewatches):
    adapter, session = env
    adapter.config = {"_cw_history_rewatches": rewatches}
    item = _episode(1, 5, rewatches=rewatches)
    session.get.return_value = _response(_live([_episode(1)]))
    count, unresolved = history.add(adapter, [item])
    assert count == 0
    assert len(unresolved) == int(rewatches)
    assert len(adapter._simkl_history_add_skipped_keys) == int(not rewatches)


def test_failed_readback_does_not_confirm_or_skip_any_watch(env):
    adapter, session = env
    session.get.return_value = _response({}, status=429)
    items = [_episode(1), _episode(1, 5)]
    count, unresolved = history.add(adapter, items)
    assert count == 0
    assert len(unresolved) == 2
    assert adapter._simkl_history_add_confirmed_keys == []
    assert adapter._simkl_history_add_skipped_keys == []
    session.post.assert_called_once()


def test_500_unconfirmed_events_are_not_counted_as_383_unresolved_and_117_skips(env):
    adapter, session = env
    items = [_episode(n % 383 + 1, 1 + n // 383) for n in range(500)]
    count, unresolved = history.add(adapter, items)
    out = _normalize(adapter, items, count, unresolved)
    assert out["attempted"] == 500
    assert out["unresolved"] == 500
    assert len(out["unresolved_keys"]) == 500
    assert out["skipped"] == 0


def test_one_movie_not_found_does_not_confirm_the_rejected_movie(env):
    adapter, session = env
    adapter.config = {}
    items = [{"type": "movie", "ids": {"tmdb": str(n)},
              "watched_at": "2026-09-01T12:00:00Z"} for n in (1, 2)]
    session.post.return_value = _response({"added": {"movies": 1},
                                         "not_found": {"movies": [{"ids": {"tmdb": "2"}}]}})
    count, unresolved = history.add(adapter, items)
    assert count == 1
    assert len(unresolved) == 1
    assert unresolved[0]["item"]["ids"]["tmdb"] == "2"
    assert adapter._simkl_history_add_confirmed_keys == [history._thaw_key(items[0])]


def test_one_rejected_episode_does_not_fail_the_other_499_watches(env):
    adapter, session = env
    items = [_episode(n) for n in range(1, 501)]
    session.get.return_value = _response(_live(items[:-1]))
    session.post.return_value = _response({
        "added": {"episodes": 499}, "not_found": {"episodes": [
            {"ids": {"tvdb": "399959"}, "seasons": [{"number": 1, "episodes": [{"number": 500}]}]},
        ]},
    })
    count, unresolved = history.add(adapter, items)
    assert count == 499
    assert len(unresolved) == 1
    assert unresolved[0]["hint"] == "simkl_not_found:episodes"
    out = _normalize(adapter, items, count, unresolved)
    assert (out["confirmed"], out["unresolved"], out["skipped"]) == (499, 1, 0)


def test_not_found_processing_is_not_truncated_at_50_movies(env):
    adapter, session = env
    adapter.config = {}
    items = [{"type": "movie", "ids": {"tmdb": str(n)},
              "watched_at": "2026-09-01T12:00:00Z"} for n in range(61)]
    session.post.return_value = _response({"added": {"movies": 1}, "not_found": {
        "movies": [{"ids": item["ids"]} for item in items[1:]],
    }})
    count, unresolved = history.add(adapter, items)
    assert count == 1
    assert len(unresolved) == 60


def test_unresolved_history_without_rewatches_still_deduplicates_by_item(env):
    items = [_episode(1, day, rewatches=False) for day in (1, 5)]
    rows = [{"item": item, "hint": "simkl_write_response_unconfirmed:add"} for item in items]
    assert len(_applier._dedupe_unresolved_rows(rows, feature="history")) == 1


@pytest.mark.parametrize("dated", [False, True])
def test_rejected_episode_keeps_all_original_watch_events(env, dated):
    adapter, session = env
    items = [_episode(1), _episode(1, 5)]
    rejected = {"number": 1}
    if dated:
        rejected["watched_at"] = items[0]["watched_at"]
    session.post.return_value = _response({"added": {"episodes": int(dated)}, "not_found": {"episodes": [
        {"ids": {"tvdb": "399959"}, "seasons": [{"number": 1, "episodes": [rejected]}]},
    ]}})
    session.get.return_value = _response(_live(items[1:]))
    count, unresolved = history.add(adapter, items)
    assert count == int(dated)
    assert len(unresolved) == (1 if dated else 2)
    assert all(row["hint"] == "simkl_not_found:episodes" for row in unresolved)
    assert unresolved[0]["item"]["watched_at"] == items[0]["watched_at"]


def test_main_batch_noop_preserves_independent_native_anime_success(env, monkeypatch):
    adapter, session = env
    monkeypatch.setattr(history.time, "sleep", lambda *a: None)
    monkeypatch.setattr(history, "_native_anime_ids_for_mismatched_show",
                        lambda session, headers, timeout, item, state, **kw:
                        {"simkl": "100"} if item["episode"] == 1 else {})
    monkeypatch.setattr(history, "_anime_retry_episode_number", lambda *a, **kw: 1)
    first, second = _episode(1), _episode(2)
    monkeypatch.setattr(history, "_add_native_anime", lambda *a, **kw:
                        ({history._thaw_key(first)}, set(), set(), []))
    count, unresolved = history.add(adapter, [first, second])
    assert count == 1
    assert adapter._simkl_history_add_confirmed_keys == [history._thaw_key(first)]
    assert [row["item"]["episode"] for row in unresolved] == [2]


def _install_adapter(env, monkeypatch):
    adapter, session = env

    def factory(cfg):
        instance = SIMKLModule.__new__(SIMKLModule)
        instance.__dict__.update(adapter.__dict__)
        instance.config = cfg
        instance.raw_cfg = cfg
        return instance

    monkeypatch.setattr(OPS, "_adapter", factory)


def _apply_batches(env, monkeypatch, items, *, rewatches=True, dry_run=False):
    _install_adapter(env, monkeypatch)
    return _applier.apply_add(
        dst_ops=OPS, cfg={"_cw_history_rewatches": rewatches}, dst_name="SIMKL", feature="history",
        items=items, dry_run=dry_run, emit=lambda *a, **k: None, dbg=lambda *a, **k: None,
        chunk_size=500, chunk_pause_ms=0,
    )


def test_10k_watches_use_20_writes_and_one_read_after_all_batches(env, monkeypatch):
    _, session = env
    items = [_episode(n) for n in range(1, 10001)]
    reads_after_writes = []

    def read(*a, **kw):
        reads_after_writes.append(session.post.call_count)
        return _response(_live(items))

    session.get.side_effect = read
    out = _apply_batches(env, monkeypatch, items)
    assert session.post.call_count == 20
    assert reads_after_writes == [20]
    assert (out["attempted"], out["confirmed"], out["skipped_exact"], out["unresolved"]) == (10000, 0, 10000, 0)


def test_batched_verification_failure_keeps_every_event_unresolved(env, monkeypatch):
    _, session = env
    monkeypatch.setattr(history, "_freeze_failed_adds", Mock())
    items = [_episode(n % 383 + 1, 1 + n // 383) for n in range(1000)]
    session.get.return_value = _response({}, status=429)
    out = _apply_batches(env, monkeypatch, items)
    assert session.post.call_count == 2
    session.get.assert_called_once()
    assert (out["confirmed"], out["skipped"], out["unresolved"]) == (0, 0, 1000)


def test_cancelled_batches_verify_only_the_writes_already_sent(env, monkeypatch):
    _, session = env
    items = [_episode(n) for n in range(1, 1001)]
    monkeypatch.setattr(_applier, "cancel_requested", lambda: session.post.call_count > 0)
    session.get.return_value = _response(_live(items[:500]))
    out = _apply_batches(env, monkeypatch, items)
    session.post.assert_called_once()
    session.get.assert_called_once()
    assert out["cancelled"] is True
    assert (out["attempted"], out["skipped_exact"], out["unresolved"]) == (500, 500, 0)


def test_dry_run_never_writes_or_verifies_batches(env, monkeypatch):
    _, session = env
    out = _apply_batches(env, monkeypatch, [_episode(n) for n in range(1, 1001)], dry_run=True)
    session.post.assert_not_called()
    session.get.assert_not_called()
    assert out["dry_run"] is True


def test_non_rewatch_successful_batches_need_no_verification_read(env, monkeypatch):
    _, session = env
    session.post.return_value = _response({"added": {"episodes": 500}, "not_found": {}})
    items = [_episode(n, rewatches=False) for n in range(1, 1001)]
    out = _apply_batches(env, monkeypatch, items, rewatches=False)
    assert session.post.call_count == 2
    session.get.assert_not_called()
    assert out["confirmed"] == 1000


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("chunk_size", [1, 2, 4])
@pytest.mark.parametrize("selection", ["none", "mixed", "readback_failure"])
def test_interactive_simkl_verifies_selected_batches_once_and_reports_final_results(
    env, config_base, monkeypatch, mode, chunk_size, selection,
):
    from cw_platform.history_events import history_sync_key
    from cw_platform.orchestrator._interactive import InteractivePlan
    from services.interactive_sync import Session
    from services.interactive_sync_report import SyncReport
    from test_interactive_sync import run
    from test_interactive_sync_features import feature_setup

    _, session = env
    select_items = selection != "none"
    readback_ok = selection != "readback_failure"
    _install_adapter(env, monkeypatch)
    items = [_episode(1), _episode(1, 5), _episode(2), _episode(3), _episode(4)]
    cfg, src, _ = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    cfg["runtime"]["apply_chunk_size"] = chunk_size
    cfg["pairs"][0]["target"] = "SIMKL"
    cfg["pairs"][0]["features"]["history"].update(rewatches=True, remove=False)
    src.index = {history_sync_key(item, event_mode=True): item for item in items}
    src.capabilities = lambda: {
        "features": {"history": True}, "index_semantics": "present",
        "history": {"rewatches": {"read": True, "write": True}},
    }
    monkeypatch.setattr(OPS, "health", lambda *a, **kw: {"ok": True, "status": "ok", "features": {"history": True}})
    monkeypatch.setattr(OPS, "build_index", lambda *a, **kw: {})
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": src, "SIMKL": OPS})
    reads_after_writes = []

    def write(*a, **kw):
        episodes = [episode for show in kw["json"].get("shows", [])
                    for season in show.get("seasons", []) for episode in season.get("episodes", [])]
        return _response({"added": {"episodes": sum(episode["number"] == 3 for episode in episodes)}, "not_found": {}})

    def read(*a, **kw):
        reads_after_writes.append(session.post.call_count)
        return _response(_live(items[:-1])) if readback_ok else _response({}, status=429)

    session.post.side_effect = write
    session.get.side_effect = read
    collector = SyncReport()
    preview = InteractivePlan(on_result=collector.record)
    assert run(cfg, preview)["ok"]
    assert len(preview.rows) == len(items)
    assert collector.features == []
    session.post.assert_not_called()
    session.get.assert_not_called()
    selected = {rid for rid, row in preview.rows.items()
                if (row["item"]["episode"], row["item"]["watched_at"]) != (1, items[0]["watched_at"])} if select_items else set()
    execution = InteractivePlan(preview=False, selected=selected, on_result=collector.record)
    result = run(cfg, execution)
    report = collector.finish(Session(pair_id="p1", owner="local"), execution, result)
    assert result["ok"]
    assert result["errors"] == 0
    assert report["requested"] == report["reached_execution"] == len(selected)
    assert report["not_reached"] == 0
    assert report["totals"]["added"] == (int(select_items) * (3 if chunk_size == 4 else 1) if readback_ok else 0)
    assert report["totals"]["skipped"] == (2 * int(select_items and chunk_size < 4) if readback_ok else 0)
    assert report["totals"]["unresolved"] == int(select_items) * (1 if readback_ok else 4)
    assert report["totals"]["errors"] == 0
    assert not src.add_calls
    if select_items:
        assert session.post.call_count == 4 // chunk_size
        assert reads_after_writes == [4 // chunk_size]
        sent = [(episode["number"], episode["watched_at"])
                for call in session.post.call_args_list for show in call.kwargs["json"].get("shows", [])
                for season in show.get("seasons", []) for episode in season.get("episodes", [])]
        assert sorted(sent) == sorted((item["episode"], item["watched_at"]) for item in items[1:])
    else:
        session.post.assert_not_called()
        session.get.assert_not_called()

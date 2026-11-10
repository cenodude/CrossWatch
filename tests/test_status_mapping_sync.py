# tests/test_status_mapping_sync.py
# CrossWatch - Native status execution, persistence and interactive selection tests.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace
from types import SimpleNamespace

import pytest

from cw_platform.orchestrator import _status_mapping as runtime
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.status_mapping import Capabilities, Observation, ReadState, Status
from cw_platform.status_mapping_store import StatusStore
from providers.sync._status_mapping import NativeStatusAdapter, Record, skipped


def row(status, ident="1", *, note="unchanged"):
    return Record(ident, "shows", {"type": "show", "ids": {"tmdb": ident}, "title": "Title " + ident},
                  Observation(ReadState.PRESENT, status, status.value, note, False, False), {})


@pytest.fixture()
def sync_case(monkeypatch, tmp_path, config_base):
    rows = {"a": {"1": row(Status.DROPPED)}, "b": {"1": row(Status.WATCHING)}}
    writes, events = [], []
    class Adapter(NativeStatusAdapter):
        capabilities = {kind: Capabilities(frozenset(Status), frozenset(Status) - {Status.PLANNED}) for kind in ("movies", "shows")}

        def read(self):
            return deepcopy(rows[self.cfg["endpoint"]])

        def write(self, record, desired):
            endpoint = self.cfg["endpoint"]
            writes.append((endpoint, record.native_id, desired))
            before = rows[endpoint][record.native_id]
            rows[endpoint][record.native_id] = replace(before, observation=replace(before.observation, status=desired, native_status=desired.value))

    monkeypatch.setattr(runtime, "adapter_class", lambda name: Adapter)
    monkeypatch.setattr(runtime, "build_provider_config_view", lambda cfg, provider, instance: {"endpoint": provider.lower()})
    rules = {status.value: {"include": True, "target": status.value} for status in Status if status != Status.PLANNED}
    pair = {"id": "test", "enabled": True, "source": "A", "target": "B", "mode": "one-way",
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "a", "instance": "default"},
                               "b": {"provider": "b", "instance": "default"},
                               "directions": {"a_to_b": {"shows": deepcopy(rules)}}}}
    ctx = SimpleNamespace(dry_run=False, interactive=None, emit=lambda name, **kwargs: events.append((name, kwargs)))
    store = StatusStore(tmp_path / "statuses.sqlite3")
    return SimpleNamespace(rows=rows, writes=writes, events=events, pair=pair, ctx=ctx, store=store, adapter=Adapter, rules=rules)


def run(case, pairs=None):
    return runtime.run_status_mappings(case.ctx, pairs or [case.pair], {}, {}, store=case.store)


@pytest.mark.parametrize("native,counted", [("untracked", False), ("caught_up", False), ("waiting", False), ("", True)])
def test_unmatched_titles_without_a_status_are_not_reported(sync_case, native, counted):
    case = sync_case
    case.rows["a"]["9"] = Record("9", "shows", {"type": "show", "ids": {"tmdb": "9"}, "title": "Only A"},
                                 Observation(ReadState.UNKNOWN, native_status=native), {})
    case.rows["a"]["8"] = Record("8", "shows", {"type": "show", "ids": {"tmdb": "8"}, "title": "Absent A"}, Observation(ReadState.ABSENT), {})
    run(case)
    read = next(kwargs for name, kwargs in case.events if name == "status_mapping:read")
    reviews = [kwargs["reason"] for name, kwargs in case.events if name == "status_mapping:review" and "no unique matching" in kwargs["reason"]]
    assert read["unmatched"] == int(counted)
    assert len(reviews) == int(counted)


def test_status_writes_share_one_preflight_and_one_readback_per_side(sync_case, monkeypatch):
    case = sync_case
    for ident in ("2", "3", "4", "5"):
        case.rows["a"][ident] = row(Status.DROPPED, ident)
        case.rows["b"][ident] = row(Status.WATCHING, ident)
    reads = {"a": 0, "b": 0}
    original = case.adapter.read

    def counted(self):
        reads[self.cfg["endpoint"]] += 1
        return original(self)

    monkeypatch.setattr(case.adapter, "read", counted)
    assert run(case)["updated"] == 5
    assert len(case.writes) == 5
    assert reads == {"a": 3, "b": 3}


def test_chained_pair_does_not_forward_a_status_changed_earlier_in_the_batch(sync_case):
    case = sync_case
    case.rows["c"] = {"1": row(Status.ON_HOLD)}
    second = deepcopy(case.pair)
    second.update(id="chain", source="B", target="C")
    second["status_mapping"]["a"] = {"provider": "b", "instance": "default"}
    second["status_mapping"]["b"] = {"provider": "c", "instance": "default"}
    result = run(case, [case.pair, second])
    assert case.writes == [("b", "1", Status.DROPPED)]
    assert result["updated"] == 1 and result["unresolved"] >= 1
    assert case.rows["c"]["1"].observation.status == Status.ON_HOLD
    assert not any(event == "status_mapping:updated" and value["pair_id"] == "chain" for event, value in case.events)


def test_later_write_to_an_earlier_source_waits_for_the_next_run(sync_case):
    case = sync_case
    case.rows["c"] = {"1": row(Status.ON_HOLD)}
    second = deepcopy(case.pair)
    second.update(id="reverse", source="C", target="A")
    second["status_mapping"]["a"] = {"provider": "c", "instance": "default"}
    second["status_mapping"]["b"] = {"provider": "a", "instance": "default"}
    result = run(case, [case.pair, second])
    assert case.writes == [("b", "1", Status.DROPPED)]
    assert result["updated"] == 1 and result["errors"] == 0
    assert case.rows["a"]["1"].observation.status == Status.DROPPED
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    assert not case.store.get(item).get("pending")
    result = run(case, [case.pair, second])
    assert case.writes == [("b", "1", Status.DROPPED), ("a", "1", Status.ON_HOLD)]
    assert result["updated"] == 1


def test_failed_preflight_read_is_not_repeated_per_title(sync_case, monkeypatch):
    case = sync_case
    for ident in ("2", "3"):
        case.rows["a"][ident] = row(Status.DROPPED, ident)
        case.rows["b"][ident] = row(Status.WATCHING, ident)
    reads = {"a": 0, "b": 0}
    original = case.adapter.read

    def failing(self):
        endpoint = self.cfg["endpoint"]
        reads[endpoint] += 1
        if reads[endpoint] > 1:
            raise RuntimeError("provider unavailable")
        return original(self)

    monkeypatch.setattr(case.adapter, "read", failing)
    assert run(case)["errors"] == 3
    assert case.writes == []
    assert reads == {"a": 2, "b": 1}


def test_one_way_write_persists_and_is_idempotent(sync_case):
    case = sync_case
    assert run(case)["updated"] == 1
    assert case.writes == [("b", "1", Status.DROPPED)]
    assert run(case)["updated"] == 0
    assert len(case.writes) == 1
    assert case.store.path.exists()
    summaries = [value for event, value in case.events if event == "status_mapping:done"]
    assert [(value["updated"], value["unchanged"]) for value in summaries] == [(1, 0), (0, 1)]
    assert all(value["state"] == "complete" and value["matched"] == 1 for value in summaries)
    change = next(value for event, value in case.events if event == "status_mapping:updated")
    assert (change["title"], change["before"], change["after"]) == ("Title 1", "watching", "dropped")


@pytest.mark.parametrize("setting,reason", [("enable", "History is disabled"), ("use_source_status", "switched off"), ("mapping", "No custom status mapping")])
def test_mapping_skip_is_visible_without_warning_or_provider_reads(sync_case, monkeypatch, setting, reason):
    case = sync_case
    case.ctx.interactive = InteractivePlan()
    if setting == "mapping":
        case.pair.pop("status_mapping")
    else:
        case.pair["features"]["history"][setting] = False
    monkeypatch.setattr(case.adapter, "read", lambda self: pytest.fail("Skipped mapping must not read providers"))
    assert run(case) == {"updated": 0, "unresolved": 0, "errors": 0, "skipped": 0}
    summary = case.ctx.interactive.status_mappings[0]
    assert summary["state"] == "skipped" and reason in summary["reason"]
    assert case.events == [("status_mapping:skip", summary)]
    assert not case.ctx.interactive.notices and not case.writes


def test_mapping_preview_and_selected_apply_have_separate_counts(sync_case):
    case = sync_case
    case.ctx.interactive = InteractivePlan()
    assert run(case)["updated"] == 0
    summary = case.ctx.interactive.status_mappings[0]
    assert summary["preview"] and summary["proposed"] == 1 and summary["updated"] == 0
    assert not case.ctx.interactive.notices and not case.writes
    case.ctx.interactive = InteractivePlan(preview=False, selected=set(case.ctx.interactive.rows))
    assert run(case)["updated"] == 1
    summary = case.ctx.interactive.status_mappings[0]
    assert not summary["preview"] and summary["updated"] == 1 and summary["state"] == "complete"


def test_mapping_unverified_write_is_not_reported_as_updated(sync_case, monkeypatch):
    case = sync_case
    def uncertain(self, record, desired):
        raise TimeoutError("private provider response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    summary = case.events[-1][1]
    assert summary["state"] == "attention" and summary["errors"] == 1 and summary["updated"] == 0
    assert not any(event == "status_mapping:updated" for event, _ in case.events)
    assert "private provider response" not in str(case.events)


@pytest.mark.parametrize("baseline", [False, True])
def test_review_replay_noop_preserves_intervening_pending_write(sync_case, monkeypatch, baseline):
    from cw_platform.interactive_reads import ReviewReads, use_review_reads
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    case = sync_case
    case.rows["a"]["1"] = row(Status.WATCHING)
    if baseline:
        assert run(case)["updated"] == 0
    case.rows["a"]["2"] = row(Status.DROPPED, "2")
    case.rows["b"]["2"] = row(Status.WATCHING, "2")
    reads = ReviewReads()
    original = case.adapter.write
    try:
        case.ctx.interactive = InteractivePlan()
        with use_review_reads(reads, collecting=True):
            run(case)
        selected = set(case.ctx.interactive.rows)
        assert len(selected) == 1
        case.rows["a"]["1"] = row(Status.DROPPED)
        case.ctx.interactive = None
        def uncertain(self, record, desired):
            case.writes.append((self.cfg["endpoint"], record.native_id, desired))
            request_cancel()
            raise TimeoutError("lost response")
        monkeypatch.setattr(case.adapter, "write", uncertain)
        with pytest.raises(SyncCancelled):
            run(case)
        clear_cancel()
        item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
        pending = deepcopy(case.store.get(item)["pending"])
        monkeypatch.setattr(case.adapter, "write", original)
        case.ctx.interactive = InteractivePlan(preview=False, selected=selected)
        with use_review_reads(reads):
            result = run(case)
        assert result["updated"] == 1
        assert case.store.get(item)["pending"] == pending
        case.ctx.interactive = None
        assert run(case)["updated"] == 0
        assert [ident for _, ident, _ in case.writes] == ["1", "2"]
    finally:
        clear_cancel()
        reads.close()


@pytest.mark.parametrize("competing_first", [False, True])
@pytest.mark.parametrize("interactive", [False, True])
@pytest.mark.parametrize("excluded", [False, True])
def test_delayed_recovery_retains_shared_target_intention(sync_case, monkeypatch, competing_first, interactive, excluded):
    from cw_platform.interactive_reads import ReviewReads, use_review_reads
    case = sync_case
    def uncertain(self, record, desired):
        case.writes.append((self.cfg["endpoint"], record.native_id, desired))
        raise TimeoutError("lost response")
    original = case.adapter.write
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    monkeypatch.setattr(case.adapter, "write", original)
    case.rows["b"]["1"] = row(Status.DROPPED)
    case.rows["c"] = {"1": row(Status.WATCHING)}
    other = deepcopy(case.pair)
    other.update(id="other", source="C")
    other["status_mapping"]["a"]["provider"] = "c"
    other["status_mapping"]["conflict"] = "prefer_a"
    case.pair["status_mapping"]["conflict"] = "prefer_a"
    case.pair["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["include"] = not excluded
    pairs = [other, case.pair] if competing_first else [case.pair, other]
    reads = ReviewReads()
    try:
        if interactive:
            case.ctx.interactive = InteractivePlan()
            with use_review_reads(reads, collecting=True):
                run(case, pairs)
            case.ctx.interactive = InteractivePlan(preview=False, selected=set(case.ctx.interactive.rows))
        with use_review_reads(reads if interactive else None):
            result = run(case, pairs)
        assert result["updated"] == int(excluded and not interactive)
        if not excluded:
            assert result["unresolved"] == 1
            assert any("Other pairs" in event.get("reason", "") for _, event in case.events)
        assert len(case.writes) == 1 + int(excluded and not interactive)
        assert case.rows["b"]["1"].observation.status == (Status.WATCHING if excluded and not interactive else Status.DROPPED)
    finally:
        reads.close()
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    assert not case.store.get(item).get("pending")


@pytest.mark.parametrize("phase", ["recovery", "new_write"])
def test_settlement_does_not_clear_replacement_operation(sync_case, monkeypatch, phase):
    case = sync_case
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    original_write = case.adapter.write
    if phase == "recovery":
        def uncertain(self, record, desired):
            case.writes.append((self.cfg["endpoint"], record.native_id, desired))
            raise TimeoutError("lost response")
        monkeypatch.setattr(case.adapter, "write", uncertain)
        assert run(case)["errors"] == 1
        monkeypatch.setattr(case.adapter, "write", original_write)
        case.rows["b"]["1"] = row(Status.DROPPED)
    original_read = case.adapter.read_record
    replacement = {}
    def read(self, record):
        observed = original_read(self, record)
        state = case.store.get(item)
        if self.cfg["endpoint"] == "b" and state.get("pending"):
            replacement.update(deepcopy(state))
            replacement["pending"]["operation_id"] = "a-newer-independent-operation"
            other_store = StatusStore(case.store.path)
            other_store.purge(item.pair.pair_id)
            assert other_store.reserve(item, replacement)
        return observed
    monkeypatch.setattr(case.adapter, "read_record", read)
    result = run(case)
    assert result["updated"] == 0
    assert result["unresolved"] == 1
    assert case.store.get(item) == replacement
    assert case.store.get(item)["pending"]["operation_id"] == "a-newer-independent-operation"
    assert len(case.writes) == 1


@pytest.mark.parametrize("endpoint", ["a", "b"])
def test_cancelling_pending_recovery_preserves_operation(sync_case, monkeypatch, endpoint):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    case = sync_case
    def uncertain(self, record, desired):
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = deepcopy(case.store.get(item)["pending"])
    case.rows["b"]["1"] = row(Status.DROPPED)
    original_read = case.adapter.read_record
    def read(self, record):
        value = original_read(self, record)
        if self.cfg["endpoint"] == endpoint:
            request_cancel()
        return value
    monkeypatch.setattr(case.adapter, "read_record", read)
    try:
        with pytest.raises(SyncCancelled):
            run(case)
        assert case.store.get(item)["pending"] == pending
    finally:
        clear_cancel()


@pytest.mark.parametrize("changed", ["source", "destination", "missing", "failed"])
def test_review_recovery_requires_fresh_readback(sync_case, monkeypatch, changed):
    from cw_platform.interactive_reads import ReviewReads, use_review_reads
    case = sync_case
    original = case.adapter.write
    def uncertain(self, record, desired):
        case.writes.append((self.cfg["endpoint"], record.native_id, desired))
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    monkeypatch.setattr(case.adapter, "write", original)
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = deepcopy(case.store.get(item)["pending"])
    case.rows["b"]["1"] = row(Status.DROPPED)
    reads = ReviewReads()
    try:
        case.ctx.interactive = InteractivePlan()
        with use_review_reads(reads, collecting=True):
            run(case)
        assert case.store.get(item)["pending"] == pending
        if changed == "source":
            case.rows["a"]["1"] = row(Status.WATCHING)
        elif changed == "destination":
            case.rows["b"]["1"] = row(Status.WATCHING)
        elif changed == "missing":
            case.rows["b"].clear()
        else:
            def failed(self, record):
                raise RuntimeError("read failed")
            monkeypatch.setattr(case.adapter, "read_record", failed)
        case.ctx.interactive = InteractivePlan(preview=False)
        with use_review_reads(reads):
            result = run(case)
        assert result["updated"] == 0
        assert result["unresolved"] + result["errors"] >= 1
        assert case.store.get(item)["pending"] == pending
        assert len(case.writes) == 1
    finally:
        reads.close()


@pytest.mark.parametrize("change", ["disabled", "optout", "legacy", "excluded", "watchlist_only", "dry_run"])
def test_no_unauthorized_status_writes(sync_case, change):
    case = sync_case
    if change == "disabled":
        case.pair["features"]["history"]["enable"] = False
    elif change == "optout":
        case.pair["features"]["history"]["use_source_status"] = False
    elif change == "legacy":
        case.pair.pop("status_mapping")
    elif change == "excluded":
        case.pair["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["include"] = False
    elif change == "watchlist_only":
        case.pair["features"] = {"watchlist": {"enable": True}}
    else:
        case.ctx.dry_run = True
    assert run(case)["updated"] == 0
    assert not case.writes
    assert not case.store.path.exists()


def test_two_way_initial_conflict_and_explicit_preference(sync_case):
    case = sync_case
    case.pair["mode"] = "two-way"
    case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": case.rules}
    assert run(case)["unresolved"] == 1
    assert not case.writes
    case.pair["status_mapping"]["conflict"] = "prefer_b"
    assert run(case)["updated"] == 1
    assert case.writes == [("a", "1", Status.WATCHING)]


def test_two_way_propagates_user_change_without_echo(sync_case):
    case = sync_case
    case.pair["mode"] = "two-way"
    case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": case.rules}
    case.rows["b"]["1"] = row(Status.DROPPED)
    run(case)
    case.rows["b"]["1"] = row(Status.ON_HOLD)
    assert run(case)["updated"] == 1
    assert case.writes == [("a", "1", Status.ON_HOLD)]
    assert run(case)["updated"] == 0


def test_interactive_preview_and_selected_apply(sync_case):
    case = sync_case
    case.ctx.interactive = InteractivePlan()
    assert run(case)["updated"] == 0
    assert not case.writes and not case.store.path.exists()
    planned = list(case.ctx.interactive.rows.values())
    assert len(planned) == 1
    assert planned[0]["item"]["_cw_status_mapping"] == {"from": "watching", "to": "dropped"}
    case.ctx.interactive = InteractivePlan(preview=False, selected={planned[0]["id"]})
    assert run(case)["updated"] == 1
    assert planned[0]["id"] in case.ctx.interactive.seen


def test_interactive_unselected_does_not_write(sync_case):
    sync_case.ctx.interactive = InteractivePlan(preview=False)
    assert run(sync_case)["updated"] == 0
    assert not sync_case.writes


def test_interactive_conflict_choice_uses_shared_planner(sync_case):
    case = sync_case
    case.pair["mode"] = "two-way"
    case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": case.rules}
    case.ctx.interactive = InteractivePlan()
    run(case)
    cid, conflict = next(iter(case.ctx.interactive.conflicts.items()))
    case.ctx.interactive = InteractivePlan(choices={cid: conflict["source"]})
    run(case)
    assert len(case.ctx.interactive.rows) == 1
    assert not case.writes


def test_api_preference_can_return_to_leave_unchanged_before_apply(sync_case, monkeypatch):
    from api import interactiveSyncAPI as api
    case = sync_case
    case.pair["mode"] = "two-way"
    case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": case.rules}
    case.ctx.interactive = InteractivePlan()
    run(case)
    cid, conflict = next(iter(case.ctx.interactive.conflicts.items()))
    session = SimpleNamespace(plan=case.ctx.interactive, revision=1, status="ready", public=lambda: {"ok": True})
    monkeypatch.setattr(api, "load_config", lambda: {})
    monkeypatch.setattr(api, "get_session", lambda *args: session)
    def recalculate(current, task, cfg, choices):
        assert task is api.svc.recalculate
        case.ctx.interactive = InteractivePlan(choices=choices)
        run(case)
        current.plan = case.ctx.interactive
        current.revision += 1
    monkeypatch.setattr(api, "launch", recalculate)
    api.refresh("test", api.Refresh(revision=1, choices={cid: conflict["source"]}), None)
    selected = set(session.plan.rows)
    assert len(selected) == 1
    api.refresh("test", api.Refresh(revision=2, choices={cid: ""}), None)
    assert not session.plan.rows
    assert session.plan.conflicts[cid]["winner"] == ""
    assert cid not in session.plan.choices
    case.ctx.interactive = InteractivePlan(preview=False, selected=selected, choices=session.plan.choices)
    assert run(case)["updated"] == 0
    assert case.writes == []


def test_cancellation_after_pending_is_saved_never_mutates(sync_case, monkeypatch):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    case = sync_case
    original = case.store.reserve
    def cancel(item, state, **kwargs):
        reserved = original(item, state, **kwargs)
        request_cancel()
        return reserved
    monkeypatch.setattr(case.store, "reserve", cancel)
    clear_cancel()
    try:
        with pytest.raises(SyncCancelled):
            run(case)
        assert case.writes == []
    finally:
        clear_cancel()
    assert run(case)["unresolved"] == 1
    assert case.writes == []


def test_conflicting_pairs_block_both_writes(sync_case):
    case = sync_case
    other = deepcopy(case.pair)
    other["id"] = "other"
    other["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    assert run(case, [case.pair, other])["unresolved"] == 2
    assert not case.writes


def test_failed_write_stays_pending_and_is_not_retried_automatically(sync_case, monkeypatch):
    case = sync_case
    def failed(self, record, desired):
        case.writes.append(("failed", record.native_id, desired))
        raise RuntimeError("timeout")
    monkeypatch.setattr(case.adapter, "write", failed)
    assert run(case)["errors"] == 1
    assert run(case)["unresolved"] == 1
    assert len(case.writes) == 1


@pytest.mark.parametrize("endpoint", ["a", "b"])
def test_cancellation_during_preflight_never_writes(sync_case, monkeypatch, endpoint):
    from cw_platform.run_control import SyncCancelled, clear_cancel, request_cancel
    case = sync_case
    original = case.adapter.read_record
    def cancelled(self, record):
        value = original(self, record)
        if self.cfg["endpoint"] == endpoint:
            request_cancel()
        return value
    monkeypatch.setattr(case.adapter, "read_record", cancelled)
    clear_cancel()
    try:
        with pytest.raises(SyncCancelled):
            run(case)
        assert case.writes == []
        assert not case.store.path.exists()
    finally:
        clear_cancel()


@pytest.mark.parametrize("change", ["mode", "rules", "both"])
@pytest.mark.parametrize("reverse", [False, True])
def test_pending_write_survives_mode_and_rule_changes_and_restart(sync_case, monkeypatch, change, reverse):
    case = sync_case
    case.pair["mode"] = "two-way"
    case.pair["status_mapping"]["conflict"] = "prefer_b" if reverse else "prefer_a"
    case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": deepcopy(case.rules)}
    if reverse:
        case.rows["a"]["1"], case.rows["b"]["1"] = row(Status.WATCHING), row(Status.DROPPED)
    def failed(self, record, desired):
        case.writes.append((self.cfg["endpoint"], desired))
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", failed)
    assert run(case)["errors"] == 1
    if change in {"mode", "both"}:
        case.pair["mode"] = "one-way"
        case.pair["status_mapping"]["conflict"] = "prefer_a"
        case.pair["status_mapping"]["directions"].pop("b_to_a")
    if change in {"rules", "both"}:
        case.pair["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    case.store = StatusStore(case.store.path)
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = deepcopy(case.store.get(item)["pending"])
    for _ in range(3):
        assert run(case)["unresolved"] == 1
        assert case.store.get(item)["pending"] == pending
    assert len(case.writes) == 1
    target = "a" if reverse else "b"
    case.rows[target]["1"] = row(Status.DROPPED)
    assert run(case)["errors"] == 0
    assert "pending" not in case.store.get(item)
    assert len(case.writes) == 1


@pytest.mark.parametrize("legacy_mode", [False, True])
def test_pending_legacy_key_remains_visible_after_mode_change(sync_case, legacy_mode):
    import json
    import sqlite3
    from dataclasses import asdict
    case = sync_case
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    case.store.put(item, {})
    old_item = replace(item, pair=replace(item.pair, two_way=legacy_mode))
    pending = {"direction": "a_to_b", "desired": "dropped", "mapping_fingerprint": "old"}
    with sqlite3.connect(case.store.path) as conn:
        conn.execute("DELETE FROM status_state")
        conn.execute("INSERT INTO status_state VALUES (?,?,?)", (json.dumps(asdict(old_item), sort_keys=True, separators=(",", ":")), "test", json.dumps({"pending": pending})))
    changed = replace(item, pair=replace(item.pair, two_way=not legacy_mode))
    recovered = case.store.get(changed)
    assert recovered["pending"]["desired"] == "dropped"
    assert recovered["pending"]["item"]["pair"]["two_way"] is legacy_mode
    case.store.put(changed, recovered)
    assert case.store.get(old_item) == recovered


@pytest.mark.parametrize("timeout", [False, True])
def test_unchanged_readback_remains_pending_until_observed(sync_case, monkeypatch, timeout):
    case = sync_case
    def delayed(self, record, desired):
        case.writes.append(desired)
        if timeout:
            raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", delayed)
    run(case)
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    for _ in range(3):
        assert run(case)["unresolved"] == 1
        assert case.store.get(item).get("pending")
    assert len(case.writes) == 1
    case.rows["b"]["1"] = row(Status.DROPPED)
    assert run(case)["unresolved"] == 0
    assert "pending" not in case.store.get(item)


@pytest.mark.parametrize("excluded", [False, True])
def test_satisfied_shared_target_intention_prevents_oscillation(sync_case, excluded):
    case = sync_case
    case.rows["b"]["1"] = row(Status.DROPPED)
    case.pair["status_mapping"]["conflict"] = "prefer_a"
    other = deepcopy(case.pair)
    other["id"] = "other"
    other["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    case.pair["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["include"] = not excluded
    for _ in range(4):
        result = run(case, [case.pair, other])
        assert result["unresolved"] == (0 if excluded else 1)
        assert case.rows["b"]["1"].observation.status == (Status.ON_HOLD if excluded else Status.DROPPED)
    assert len(case.writes) == int(excluded)


def test_another_pair_cannot_repeat_an_uncertain_destination_write(sync_case, monkeypatch):
    case = sync_case
    def failed(self, record, desired):
        case.writes.append(desired)
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", failed)
    assert run(case)["errors"] == 1
    other = deepcopy(case.pair)
    other["id"] = "other"
    for _ in range(3):
        assert run(case, [other, case.pair])["unresolved"] == 2
    assert len(case.writes) == 1


@pytest.mark.parametrize("timeout", [False, True])
@pytest.mark.parametrize("interactive", [False, True])
def test_new_pending_write_blocks_other_pair_in_same_run(sync_case, monkeypatch, timeout, interactive):
    case = sync_case
    case.rows["c"] = deepcopy(case.rows["a"])
    other = deepcopy(case.pair)
    other.update(id="other", source="C")
    other["status_mapping"]["a"]["provider"] = "c"
    pairs = [case.pair, other]
    if interactive:
        case.ctx.interactive = InteractivePlan()
        run(case, pairs)
        selected = set(case.ctx.interactive.rows)
        assert len(selected) == 2
        case.ctx.interactive = InteractivePlan(preview=False, selected=selected)
    def uncertain(self, record, desired):
        case.writes.append((self.cfg["endpoint"], record.native_id, desired))
        if timeout:
            raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    result = run(case, pairs)
    assert len(case.writes) == 1
    assert result["updated"] == 0
    assert result["unresolved"] + result["errors"] == 2
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    assert case.store.get(item)["pending"]["desired"] == "dropped"
    other_item = replace(item, pair=runtime.pair_scope(other))
    assert not case.store.get(other_item).get("pending")


@pytest.mark.parametrize("owner", ["outside_run", "disabled", "unmatched", "opted_out"])
@pytest.mark.parametrize("interactive", [False, True])
def test_durable_pending_blocks_destination_without_participating_owner(sync_case, monkeypatch, owner, interactive):
    case = sync_case
    case.rows["c"] = deepcopy(case.rows["a"])
    other = deepcopy(case.pair)
    other.update(id="other", source="C")
    other["status_mapping"]["a"]["provider"] = "c"
    original = case.adapter.write
    def uncertain(self, record, desired):
        case.writes.append((self.cfg["endpoint"], record.native_id, desired))
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = case.store.get(item)["pending"]
    case.store = StatusStore(case.store.path)
    monkeypatch.setattr(case.adapter, "write", original)
    pairs = [other, case.pair]
    if owner == "outside_run":
        pairs = [other]
    elif owner == "disabled":
        case.pair["enabled"] = False
    elif owner == "unmatched":
        case.rows["a"].clear()
    else:
        case.pair["features"]["history"]["use_source_status"] = False
    if interactive:
        case.ctx.interactive = InteractivePlan()
        assert run(case, pairs)["unresolved"] >= 1
        assert not case.ctx.interactive.rows
        case.ctx.interactive = InteractivePlan(preview=False)
    assert run(case, pairs)["unresolved"] >= 1
    assert len(case.writes) == 1
    assert case.store.get(item)["pending"] == pending
    assert case.rows["b"]["1"].observation.status == Status.WATCHING


@pytest.mark.parametrize("change", ["direction", "desired", "observations"])
def test_interactive_cannot_replace_a_different_pending_operation(sync_case, monkeypatch, change):
    case = sync_case
    case.pair["mode"] = "two-way"
    case.pair["status_mapping"]["conflict"] = "prefer_b" if change == "direction" else "prefer_a"
    case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": deepcopy(case.rules)}
    original = case.adapter.write
    def uncertain(self, record, desired):
        case.writes.append((self.cfg["endpoint"], record.native_id, desired))
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = case.store.get(item)["pending"]
    case.pair["mode"] = "one-way"
    case.pair["status_mapping"]["conflict"] = "prefer_a"
    case.pair["status_mapping"]["directions"].pop("b_to_a")
    if change == "desired":
        case.pair["status_mapping"]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    elif change == "observations":
        case.rows["b"]["1"] = row(Status.WATCHING, note="changed")
    case.store = StatusStore(case.store.path)
    monkeypatch.setattr(case.adapter, "write", original)
    isolated = StatusStore(case.store.path.with_name("preview-without-pending.sqlite3"))
    case.ctx.interactive = InteractivePlan()
    runtime.run_status_mappings(case.ctx, [case.pair], {}, {}, store=isolated)
    selected = set(case.ctx.interactive.rows)
    assert len(selected) == 1
    case.ctx.interactive = InteractivePlan()
    assert run(case)["unresolved"] == 1
    assert not case.ctx.interactive.rows
    assert case.ctx.interactive.notices
    case.ctx.interactive = InteractivePlan(preview=False, selected=selected)
    assert run(case)["updated"] == 0
    assert case.ctx.interactive.notices
    assert case.store.get(item)["pending"] == pending
    assert len(case.writes) == 1


@pytest.mark.parametrize("mode_change", [False, True])
@pytest.mark.parametrize("timeout", [False, True])
def test_interactive_can_retry_original_pending_operation(sync_case, monkeypatch, mode_change, timeout):
    case = sync_case
    if mode_change:
        case.pair["mode"] = "two-way"
        case.pair["status_mapping"]["conflict"] = "prefer_a"
        case.pair["status_mapping"]["directions"]["b_to_a"] = {"shows": deepcopy(case.rules)}
    original = case.adapter.write
    def uncertain(self, record, desired):
        case.writes.append((self.cfg["endpoint"], record.native_id, desired))
        raise TimeoutError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = case.store.get(item)["pending"]
    if mode_change:
        case.pair["mode"] = "one-way"
        case.pair["status_mapping"]["directions"].pop("b_to_a")
    def retry(self, record, desired):
        assert case.store.get(item)["pending"] == pending
        if timeout:
            uncertain(self, record, desired)
        original(self, record, desired)
    monkeypatch.setattr(case.adapter, "write", retry)
    case.ctx.interactive = InteractivePlan()
    run(case)
    selected = set(case.ctx.interactive.rows)
    assert len(selected) == 1
    case.ctx.interactive = InteractivePlan(preview=False, selected=selected)
    result = run(case)
    assert result["updated"] == int(not timeout)
    assert result["errors"] == int(timeout)
    assert len(case.writes) == 2
    if timeout:
        assert case.store.get(item)["pending"] == pending
    else:
        assert not case.store.get(item).get("pending")


def test_pending_created_during_preflight_blocks_reservation(sync_case, monkeypatch):
    from dataclasses import asdict
    case = sync_case
    original = case.adapter.read_record
    other = deepcopy(case.pair)
    other["id"] = "other"
    item = runtime.ItemScope(runtime.pair_scope(other), "shows", "1", "1")
    pending = {"item": asdict(item), "direction": "a_to_b", "desired": "dropped"}
    def read(self, record):
        if self.cfg["endpoint"] == "b":
            StatusStore(case.store.path).put(item, {"pending": pending})
        return original(self, record)
    monkeypatch.setattr(case.adapter, "read_record", read)
    assert run(case)["unresolved"] == 1
    assert not case.writes
    assert case.store.get(item)["pending"] == pending
    assert not case.store.get(replace(item, pair=runtime.pair_scope(case.pair)))


@pytest.mark.parametrize("legacy", [False, True])
def test_pending_destination_identity_is_exact_and_pair_independent(sync_case, legacy):
    import json
    import sqlite3
    from dataclasses import asdict
    case = sync_case
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    pending = {"item": asdict(item), "direction": "a_to_b", "desired": "dropped"}
    case.store.put(item, {"pending": pending})
    if legacy:
        with sqlite3.connect(case.store.path) as conn:
            conn.execute("DELETE FROM status_state")
            conn.execute("INSERT INTO status_state VALUES (?,?,?)", (case.store.legacy_keys(item)[0], item.pair.pair_id,
                         json.dumps({"pending": {key: value for key, value in pending.items() if key != "item"}})))
    store = StatusStore(case.store.path)
    assert store.pending_targets() == {(item.pair.b, "shows", "1"): {store.key(item)}}
    other = replace(item, pair=replace(item.pair, pair_id="other"))
    assert not store.reserve(other, {"pending": {**pending, "item": asdict(other)}})
    for separate in (
        replace(other, b_id="2"),
        replace(other, media="movies"),
        replace(other, pair=replace(other.pair, b=replace(other.pair.b, instance="other"))),
        replace(other, pair=replace(other.pair, b=replace(other.pair.b, provider="c"))),
    ):
        assert store.reserve(separate, {"pending": {**pending, "item": asdict(separate)}})
    assert store.get(item)["pending"] == pending


def test_destination_reservation_is_atomic_across_store_connections(sync_case):
    from concurrent.futures import ThreadPoolExecutor
    from dataclasses import asdict
    from threading import Barrier
    case = sync_case
    barrier = Barrier(2)
    items = [runtime.ItemScope(replace(runtime.pair_scope(case.pair), pair_id=name), "shows", "1", "1") for name in ("first", "second")]
    def reserve(item):
        store = StatusStore(case.store.path)
        barrier.wait(timeout=5)
        return store.reserve(item, {"pending": {"item": asdict(item), "direction": "a_to_b", "desired": "dropped"}})
    with ThreadPoolExecutor(max_workers=2) as pool:
        results = list(pool.map(reserve, items))
    assert sorted(results) == [False, True]
    assert sum(bool(case.store.get(item).get("pending")) for item in items) == 1


def test_conflicting_legacy_pending_operations_cannot_be_replaced(sync_case):
    import json
    import sqlite3
    from dataclasses import asdict
    case = sync_case
    item = runtime.ItemScope(runtime.pair_scope(case.pair), "shows", "1", "1")
    case.store.put(item, {})
    with sqlite3.connect(case.store.path) as conn:
        conn.execute("DELETE FROM status_state")
        for key, direction in zip(case.store.legacy_keys(item), ("a_to_b", "b_to_a")):
            conn.execute("INSERT INTO status_state VALUES (?,?,?)", (key, item.pair.pair_id,
                         json.dumps({"pending": {"direction": direction, "desired": "dropped"}})))
    before = case.store.pending_targets()
    assert len(before) == 2
    assert case.store.get(item) == {"pending_conflict": True}
    pending = {"item": asdict(item), "direction": "a_to_b", "desired": "dropped"}
    assert not case.store.reserve(item, {"pending": pending}, expected_pending=pending)
    assert case.store.pending_targets() == before


def test_failed_read_never_writes_or_advances_baseline(sync_case, monkeypatch):
    def failed(self):
        raise RuntimeError("incomplete")
    monkeypatch.setattr(sync_case.adapter, "read", failed)
    assert run(sync_case)["errors"] == 1
    assert not sync_case.writes and not sync_case.store.path.exists()


@pytest.mark.parametrize("error,shown", [
    (RuntimeError("scrob_status_ambiguous_watchlist"), "scrob_status_ambiguous_watchlist"),
    (RuntimeError("GET https://api.example/lists?apikey=secret failed"), "RuntimeError"),
    (RuntimeError("0123456789abcdef0123456789abcdef"), "RuntimeError"),
    (RuntimeError("secret"), "RuntimeError"),
    (RuntimeError(" scrob_status_ambiguous_watchlist"), "RuntimeError"),
    (RuntimeError("scrob_status_0123456789abcdef0123456789abcdef"), "RuntimeError"),
    (RuntimeError("scrob_status_write_http_401"), "scrob_status_write_http_401"),
])
def test_failed_read_reports_a_safe_reason(sync_case, monkeypatch, error, shown):
    def failed(self):
        raise error
    monkeypatch.setattr(sync_case.adapter, "read", failed)
    assert run(sync_case)["errors"] == 1
    reasons = [value["reason"] for event, value in sync_case.events if event == "status_mapping:review"]
    assert reasons == [f"Status read or planning failed ({shown}). Refresh before retrying."]
    assert "secret" not in str(sync_case.events)


def test_destination_changed_since_preview_blocks_write(sync_case, monkeypatch):
    case = sync_case
    original = case.adapter.read
    calls = {}
    def changed(self):
        name = self.cfg["endpoint"]
        calls[name] = calls.get(name, 0) + 1
        if name == "b" and calls[name] == 2:
            case.rows["b"]["1"] = row(Status.WATCHING, note="changed")
        return original(self)
    monkeypatch.setattr(case.adapter, "read", changed)
    assert run(case)["unresolved"] == 1
    assert not case.writes


def test_ambiguous_identity_never_changes_a_title(sync_case):
    case = sync_case
    case.rows["b"]["duplicate"] = replace(row(Status.WATCHING), native_id="duplicate")
    assert run(case)["unresolved"] == 1
    assert not case.writes


def test_store_read_does_not_create_database_and_purge_is_pair_scoped(sync_case):
    case = sync_case
    scope = runtime.pair_scope(case.pair)
    item = runtime.ItemScope(scope, "shows", "1", "1")
    assert case.store.get(item) == {} and not case.store.path.exists()
    run(case)
    case.store.purge("unrelated")
    assert case.store.get(item)["baseline"]
    case.store.purge("test")
    assert case.store.get(item) == {}


@pytest.mark.parametrize("normalized", [False, True])
def test_successful_remote_write_with_lost_response_is_reconciled_without_retry(sync_case, monkeypatch, normalized):
    case = sync_case
    write = case.adapter.write
    def uncertain(self, record, desired):
        write(self, record, Status.ON_HOLD if normalized else desired)
        raise RuntimeError("lost response")
    monkeypatch.setattr(case.adapter, "write", uncertain)
    assert run(case)["errors"] == 1
    result = run(case)
    assert result["errors"] == 0
    assert result["unresolved"] == int(normalized)
    assert run(case)["unresolved"] == 0
    assert len(case.writes) == 1


def test_unexpected_readback_effect_remains_pending(sync_case, monkeypatch):
    case = sync_case
    write = case.adapter.write
    def changed(self, record, desired):
        write(self, record, desired)
        case.rows["b"]["1"] = row(desired, note="unexpected rating change")
    monkeypatch.setattr(case.adapter, "write", changed)
    assert run(case)["unresolved"] == 1
    assert run(case)["unresolved"] == 1
    assert len(case.writes) == 1


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("eligible", [True, False])
def test_actual_orchestrator_only_executes_statuses_for_successful_history_pairs(config_base, monkeypatch, mode, eligible):
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator import _pairs
    from test_interactive_sync_features import feature_setup
    cfg, src, dst = feature_setup(config_base, monkeypatch, "history", [], [], mode)
    cfg["pairs"][0]["status_mapping"] = {"custom": True}
    cfg["pairs"][0]["features"]["history"]["use_source_status"] = True
    calls, options = [], []
    monkeypatch.setattr(runtime, "validate_pair", lambda pair: None)
    monkeypatch.setattr(runtime, "run_status_mappings", lambda ctx, pairs, *args: (calls.extend(pairs) or {"updated": 0, "unresolved": 0, "errors": 0}))
    def history(*args, **kwargs):
        options.append(kwargs["fcfg"])
        return {"errors": 0 if eligible else 1}
    monkeypatch.setattr(_pairs, "run_one_way_feature", history)
    monkeypatch.setattr(_pairs, "run_two_way_feature", history)
    review = InteractivePlan(preview=False)
    Orchestrator(cfg, interactive=review).run(pair_scope_ids=["p1"])
    assert options and options[0]["use_source_status"] is False
    assert len(calls) == int(eligible)
    assert cfg["pairs"][0]["features"]["history"]["use_source_status"] is True
    if not eligible:
        assert review.status_mappings[0]["state"] == "skipped"
        assert "History did not complete successfully" in review.status_mappings[0]["reason"]


def test_native_id_matching_respects_anime_mapping_opt_out():
    a = replace(row(Status.DROPPED), item={"type": "show", "ids": {"mal": "1", "kitsu": "9"}}, raw={"native_ids": {"mal": "1"}})
    b = replace(row(Status.WATCHING), item={"type": "show", "ids": {"kitsu": "9"}})
    assert list(runtime.matched_records({"1": a}, {"1": b}, allow_mapping=True)) == [(a, b)]
    assert not list(runtime.matched_records({"1": a}, {"1": b}, allow_mapping=False))


def test_real_crosswatch_instances_sync_new_notes_without_touching_history(tmp_path, config_base):
    from cw_platform.tracker_store import TrackerStore
    source_root, target_root = tmp_path / "source", tmp_path / "target"
    left, right = TrackerStore(source_root), TrackerStore(target_root)
    left.save_note("movie:tmdb:1", {"type": "movie", "title": "A movie", "tmdb": "1", "status": "dropped", "notes": "Private source note"})
    cfg = {"crosswatch": {"root_dir": str(source_root), "instances": {"P01": {"root_dir": str(target_root)}}}}
    pair = {"id": "local", "source": "CROSSWATCH", "target": "CROSSWATCH", "target_instance": "P01", "mode": "one-way",
            "features": {"history": {"enable": True, "use_source_status": True}},
            "status_mapping": {"version": 1, "a": {"provider": "crosswatch", "instance": "default"},
                               "b": {"provider": "crosswatch", "instance": "P01"},
                               "directions": {"a_to_b": {"movies": {"dropped": {"include": True, "target": "on_hold"}}}}}}
    ctx = SimpleNamespace(dry_run=False, interactive=None, emit=lambda *args, **kwargs: None)
    store = StatusStore(tmp_path / "state.sqlite3")
    assert runtime.run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 1
    assert left.note("movie:tmdb:1")["status"] == "dropped"
    assert right.note("movie:tmdb:1")["status"] == "on_hold"
    assert right.note("movie:tmdb:1").get("notes") != "Private source note"
    assert not right.has_state("history") and not right.has_state("watchlist")
    assert runtime.run_status_mappings(ctx, [pair], cfg, {}, store=store)["updated"] == 0


def _reviews(case):
    return [kwargs["reason"] for name, kwargs in case.events if name == "status_mapping:review"]


def test_a_skipped_title_is_reported_and_the_rest_still_syncs(sync_case):
    case = sync_case
    case.rows["a"]["2"] = row(Status.DROPPED, "2")
    case.rows["b"]["2"] = skipped("2", "shows", {"type": "show", "ids": {"tmdb": "2"}, "title": "Title 2"}, "b_status_duplicate_entry")
    case.rows["a"]["3"] = skipped("3", "shows", {"type": "show", "ids": {"tmdb": "3"}, "title": "Only A"}, "a_status_invalid_progress")
    result = run(case)
    assert case.writes == [("b", "1", Status.DROPPED)]
    assert sorted(_reviews(case)) == ["Skipped (a_status_invalid_progress)", "Skipped (b_status_duplicate_entry)"]
    assert result["errors"] == 0 and result["updated"] == 1
    read = next(kwargs for name, kwargs in case.events if name == "status_mapping:read")
    assert read["unmatched"] == 0


@pytest.mark.parametrize("unidentified", [False, True])
def test_rows_without_an_id_are_reported_and_stop_missing_record_creation(sync_case, monkeypatch, unidentified):
    case = sync_case
    case.rows["a"]["3"] = row(Status.DROPPED, "3")
    created = []
    original = case.adapter.read

    def read(self):
        if unidentified and self.cfg["endpoint"] == "b":
            self.skip_unidentified("b_status_missing_identity")
        return original(self)

    monkeypatch.setattr(case.adapter, "read", read)
    monkeypatch.setattr(case.adapter, "creates_missing", True)
    monkeypatch.setattr(case.adapter, "missing_record", lambda self, source, snapshot: created.append(source.native_id))
    result = run(case)
    assert result["errors"] == 0 and case.writes == [("b", "1", Status.DROPPED)]
    notice = "1 B status entries without a usable ID were skipped (b_status_missing_identity)."
    assert (notice in _reviews(case)) is unidentified
    assert created == ([] if unidentified else ["3"])


@pytest.mark.parametrize("failures,errors", [(1, 0), (2, 1)])
def test_a_changed_snapshot_is_read_again_once(sync_case, monkeypatch, failures, errors):
    case = sync_case
    left = {"a": failures}
    original = case.adapter.read

    def read(self):
        endpoint = self.cfg["endpoint"]
        if left.get(endpoint):
            left[endpoint] -= 1
            raise RuntimeError(f"{endpoint}_status_snapshot_changed")
        return original(self)

    monkeypatch.setattr(case.adapter, "read", read)
    result = run(case)
    assert result["errors"] == errors
    assert case.writes == ([] if errors else [("b", "1", Status.DROPPED)])


def test_a_skipped_destination_never_reaches_adapter_blockers(sync_case, monkeypatch):
    case = sync_case
    case.rows["b"] = {"0": skipped("0", "shows", {"type": "show", "ids": {"tmdb": "0"}, "title": "Title 0"}, "b_status_invalid_show_state"),
                      **{key: replace(value, raw={"entry": {}}) for key, value in case.rows["b"].items()}}
    case.rows["a"] = {"0": row(Status.DROPPED, "0"), **case.rows["a"]}
    monkeypatch.setattr(case.adapter, "blockers", lambda self, record, desired: record.raw["entry"] and ())
    result = run(case)
    assert case.writes == [("b", "1", Status.DROPPED)]
    assert _reviews(case) == ["Skipped (b_status_invalid_show_state)"]
    assert result["errors"] == 0


@pytest.mark.parametrize("unidentified", [False, True])
def test_pre_write_rows_without_an_id_stop_missing_record_writes(sync_case, monkeypatch, unidentified):
    case = sync_case
    case.rows["b"] = {}
    reads = {"b": 0}
    original = case.adapter.read

    def read(self):
        if self.cfg["endpoint"] == "b":
            reads["b"] += 1
            if unidentified and reads["b"] > 1:
                self.skip_unidentified("b_status_missing_identity")
        return original(self)

    def missing(self, source, snapshot):
        return Record(source.native_id, source.media, source.item, Observation(ReadState.ABSENT, protected_token="none", watchlisted=False, rewatching=False), {})

    monkeypatch.setattr(case.adapter, "read", read)
    monkeypatch.setattr(case.adapter, "creates_missing", True)
    monkeypatch.setattr(case.adapter, "missing_record", missing)
    run(case)
    assert case.writes == ([] if unidentified else [("b", "1", Status.DROPPED)])


def test_a_title_skipped_during_pending_recovery_never_reaches_blockers(sync_case, monkeypatch):
    case = sync_case
    original_write = case.adapter.write
    monkeypatch.setattr(case.adapter, "write", lambda self, record, desired: (_ for _ in ()).throw(RuntimeError("timeout")))
    assert run(case)["errors"] == 1
    monkeypatch.setattr(case.adapter, "write", original_write)
    original_record = runtime._FreshReads.record

    def record(self, endpoint, reference):
        current = original_record(self, endpoint, reference)
        if endpoint.provider == "b" and current is not None:
            return skipped(current.native_id, current.media, current.item, "b_status_invalid_show_state")
        return current

    monkeypatch.setattr(runtime._FreshReads, "record", record)
    monkeypatch.setattr(case.adapter, "blockers", lambda self, rec, desired: rec.raw["entry"] and ())
    events_before = len(case.events)
    result = run(case)
    reviews = [kwargs["reason"] for name, kwargs in case.events[events_before:] if name == "status_mapping:review"]
    assert reviews == ["Skipped (b_status_invalid_show_state)"]
    assert result["errors"] == 0 and case.writes == []

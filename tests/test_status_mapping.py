# tests/test_status_mapping.py
# CrossWatch - Status mapping configuration and conflict planner tests.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import replace

import pytest

from cw_platform.status_mapping import (
    Capabilities,
    ConflictPolicy,
    Direction,
    Endpoint,
    ExecutionScope,
    ItemScope,
    Observation,
    PairScope,
    ReadState,
    Status,
    TransitionCheck,
    parse_mapping,
    plan_status,
    verified_baseline,
    verify_status_write,
)


@pytest.fixture()
def mapping_case():
    a = Endpoint("tracker_a", "account-a")
    b = Endpoint("tracker_b", "account-b")
    scope = PairScope("pair-1", a, b, two_way=True)
    states = frozenset(Status) - {Status.PLANNED}
    caps = {(endpoint, media): Capabilities(states, states) for endpoint in (a, b) for media in ("movies", "shows")}
    raw = {
        "version": 1,
        "a": {"provider": a.provider, "instance": a.instance},
        "b": {"provider": b.provider, "instance": b.instance},
        "directions": {
            direction.value: {
                media: {status.value: {"include": status != Status.COMPLETED, "target": status.value} for status in states}
                for media in ("movies", "shows")
            }
            for direction in Direction
        },
    }
    return scope, caps, raw


def observed(status=Status.WATCHING):
    return Observation(ReadState.PRESENT, status, native_status=status.value, protected_token="progress-notes-ratings-dates-v1", watchlisted=False, rewatching=False)


def run_plan(case, a, b, *, baseline=None, execution=None, checks=None, resolution=None, media="shows"):
    scope, caps, raw = case
    config = parse_mapping(raw, scope=scope, capabilities=caps)
    item = ItemScope(scope, media, "native-a", "native-b")
    return plan_status(
        config,
        item=item,
        a=a,
        b=b,
        capabilities=caps,
        execution=execution if execution is not None else {d: ExecutionScope(True, True, frozenset(Status)) for d in Direction},
        checks=checks if checks is not None else {
            Direction.FORWARD: TransitionCheck(item, a, b, a.status or Status.WATCHING, verified=True),
            Direction.REVERSE: TransitionCheck(item, b, a, b.status or Status.WATCHING, verified=True),
        },
        baseline=baseline,
        resolution=resolution,
    )


def baseline_for(case, a, b):
    scope, caps, raw = case
    config = parse_mapping(raw, scope=scope, capabilities=caps)
    assert config is not None
    return verified_baseline(config, ItemScope(scope, "shows", "native-a", "native-b"), a, b)


def one_way(case):
    scope, caps, raw = deepcopy(case)
    raw["directions"].pop(Direction.REVERSE)
    return replace(scope, two_way=False), caps, raw


def test_missing_mapping_preserves_legacy_behavior(mapping_case):
    scope, caps, _ = mapping_case
    assert parse_mapping(None, scope=scope, capabilities=caps) is None
    result = run_plan((scope, caps, None), observed(Status.DROPPED), observed())
    assert (result.outcome, result.reason) == ("skip", "legacy_behavior")


@pytest.mark.parametrize("side", ["source", "destination"])
@pytest.mark.parametrize("policy", list(ConflictPolicy))
def test_active_rewatch_is_not_translated_to_an_ordinary_status(mapping_case, side, policy):
    a, b = observed(Status.WATCHING), observed(Status.DROPPED)
    if side == "source":
        a = replace(a, native_status="REPEATING", rewatching=True)
    else:
        b = replace(b, rewatching=True)
    result = run_plan(mapping_case, a, b, resolution=policy)
    assert (result.outcome, result.reason) == ("skip", "rewatch_state_preserved")


def test_parser_is_pure_and_fingerprints_semantic_rules(mapping_case):
    scope, caps, raw = mapping_case
    original = deepcopy(raw)
    parsed = parse_mapping(raw, scope=scope, capabilities=caps)
    reordered = dict(reversed(list(raw.items())))
    reordered["conflict"] = "leave_unchanged"
    assert parsed == parse_mapping(reordered, scope=scope, capabilities=caps)
    assert raw == original
    assert parsed.conflict == ConflictPolicy.LEAVE
    assert parsed.rule(Direction.FORWARD, "shows", Status.COMPLETED).include is False


@pytest.mark.parametrize("change", [
    {"version": True}, {"version": 2}, {"version": "1"},
    {"conflict": "newest"}, {"conflict": []},
    {"directions": []}, {"directions": {"sideways": {}}},
    {"directions": {"a_to_b": {"episodes": {}}}},
    {"a": {"provider": "tracker_a", "instance": "another-account"}},
    {"b": {"provider": "kitsu", "instance": "account-b"}},
    {"enabled": True},
])
def test_rejects_invalid_config_and_endpoint_changes(mapping_case, change):
    scope, caps, raw = mapping_case
    raw.update(change)
    with pytest.raises(ValueError):
        parse_mapping(raw, scope=scope, capabilities=caps)


@pytest.mark.parametrize("source,row", [
    ("planned", {"include": True, "target": "watching"}),
    ("watching", {"include": True, "target": "planned"}),
    ("planning", {"include": False, "target": "dropped"}),
    ("watching", {"include": "false", "target": "dropped"}),
    ("watching", {"include": 0, "target": "dropped"}),
    ("watching", {"include": True}),
    ("watching", {"include": True, "target": "delete"}),
    ("watching", {"include": False, "target": "clear"}),
    ("watching", {"include": True, "target": "dropped", "force": True}),
])
def test_rejects_planned_clear_and_malformed_rows(mapping_case, source, row):
    scope, caps, raw = mapping_case
    raw["directions"]["a_to_b"]["shows"] = {source: row}
    with pytest.raises(ValueError):
        parse_mapping(raw, scope=scope, capabilities=caps)


def test_validates_direction_and_media_specific_capabilities(mapping_case):
    scope, caps, raw = mapping_case
    caps[scope.b, "movies"] = Capabilities(frozenset({Status.DROPPED}), frozenset({Status.DROPPED}))
    with pytest.raises(ValueError, match="Unsupported status mapping"):
        parse_mapping(raw, scope=scope, capabilities=caps)
    for direction in raw["directions"].values():
        direction.pop("movies")
    assert parse_mapping(raw, scope=scope, capabilities=caps) is not None


def test_one_way_rejects_reverse_rules_and_prefer_destination(mapping_case):
    scope, caps, raw = mapping_case
    with pytest.raises(ValueError, match="reverse"):
        parse_mapping(raw, scope=replace(scope, two_way=False), capabilities=caps)
    raw["directions"].pop("b_to_a")
    raw["conflict"] = "prefer_b"
    with pytest.raises(ValueError, match="prefer its destination"):
        parse_mapping(raw, scope=replace(scope, two_way=False), capabilities=caps)


def test_one_way_uses_source_on_first_run(mapping_case):
    result = run_plan(one_way(mapping_case), observed(Status.DROPPED), observed())
    assert (result.outcome, result.direction, result.desired) == ("write", Direction.FORWARD, Status.DROPPED)


def test_initial_two_way_disagreement_is_a_conflict(mapping_case):
    result = run_plan(mapping_case, observed(Status.DROPPED), observed())
    assert (result.outcome, result.reason, result.direction) == ("conflict", "status_conflict", None)


@pytest.mark.parametrize("policy,direction,status", [
    (ConflictPolicy.PREFER_A, Direction.FORWARD, Status.DROPPED),
    (ConflictPolicy.PREFER_B, Direction.REVERSE, Status.WATCHING),
])
def test_explicit_conflict_preferences(mapping_case, policy, direction, status):
    mapping_case[2]["conflict"] = policy.value
    result = run_plan(mapping_case, observed(Status.DROPPED), observed())
    assert (result.outcome, result.direction, result.desired) == ("write", direction, status)


def test_interactive_resolution_uses_same_gates(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    result = run_plan(mapping_case, a, b, resolution=ConflictPolicy.PREFER_A)
    assert result.outcome == "write"
    result = run_plan(mapping_case, a, b, resolution=ConflictPolicy.PREFER_A, execution={})
    assert (result.outcome, result.reason) == ("skip", "opt_in_disabled")


@pytest.mark.parametrize("gate", [ExecutionScope(), ExecutionScope(True, False), ExecutionScope(False, True)])
def test_watchlist_only_and_disabled_opt_in_cannot_write(mapping_case, gate):
    result = run_plan(mapping_case, observed(Status.DROPPED), observed(), execution={d: gate for d in Direction})
    assert (result.outcome, result.reason) == ("skip", "opt_in_disabled")


def test_existing_opt_in_scope_does_not_expand(mapping_case):
    case = one_way(mapping_case)
    gate = ExecutionScope(True, True, frozenset({Status.ON_HOLD, Status.DROPPED}))
    result = run_plan(case, observed(), observed(Status.DROPPED), execution={Direction.FORWARD: gate})
    assert (result.outcome, result.reason) == ("skip", "outside_opt_in_scope")


@pytest.mark.parametrize("include", [False, None])
def test_exclusion_has_no_reverse_fallback(mapping_case, include):
    mapping_case[2]["conflict"] = "prefer_a"
    rows = mapping_case[2]["directions"]["a_to_b"]["shows"]
    if include is None:
        rows.pop("dropped")
    else:
        rows["dropped"]["include"] = include
    result = run_plan(mapping_case, observed(Status.DROPPED), observed())
    assert (result.outcome, result.reason, result.direction) == ("skip", "excluded", Direction.FORWARD)


def test_custom_translation_is_compared_before_raw_status(mapping_case):
    mapping_case[2]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    result = run_plan(mapping_case, observed(Status.DROPPED), observed(Status.ON_HOLD))
    assert (result.outcome, result.reason) == ("noop", "already_mapped")


def test_reverse_rules_are_not_inferred_and_media_rules_are_independent(mapping_case):
    scope, caps, raw = mapping_case
    raw["directions"].pop("b_to_a")
    raw["directions"]["a_to_b"].pop("movies")
    config = parse_mapping(raw, scope=scope, capabilities=caps)
    assert config.rule(Direction.REVERSE, "shows", Status.DROPPED) is None
    assert config.rule(Direction.FORWARD, "movies", Status.DROPPED) is None
    old = observed()
    baseline = baseline_for(mapping_case, old, old)
    result = run_plan(mapping_case, old, observed(Status.DROPPED), baseline=baseline)
    assert (result.outcome, result.reason) == ("skip", "excluded")


@pytest.mark.parametrize("direction", list(Direction))
def test_only_changed_side_supplies_two_way_status(mapping_case, direction):
    old = observed()
    baseline = baseline_for(mapping_case, old, old)
    a, b = (observed(Status.DROPPED), old) if direction == Direction.FORWARD else (old, observed(Status.DROPPED))
    result = run_plan(mapping_case, a, b, baseline=baseline)
    assert (result.outcome, result.direction) == ("write", direction)


def test_concurrent_incompatible_changes_remain_conflicted(mapping_case):
    old = observed()
    baseline = baseline_for(mapping_case, old, old)
    result = run_plan(mapping_case, observed(Status.DROPPED), observed(Status.ON_HOLD), baseline=baseline)
    assert result.outcome == "conflict"


def test_one_way_destination_edit_is_not_overwritten_by_default(mapping_case):
    case = one_way(mapping_case)
    old = observed()
    baseline = baseline_for(case, old, old)
    result = run_plan(case, old, observed(Status.DROPPED), baseline=baseline)
    assert result.outcome == "conflict"
    result = run_plan(case, old, observed(Status.DROPPED), baseline=baseline, resolution=ConflictPolicy.PREFER_A)
    assert (result.outcome, result.direction) == ("write", Direction.FORWARD)
    result = run_plan(case, old, observed(Status.DROPPED), baseline=baseline, resolution=ConflictPolicy.PREFER_B)
    assert result.outcome == "conflict"


def test_verified_own_write_is_not_echoed(mapping_case):
    a, b = observed(Status.DROPPED), observed(Status.DROPPED)
    baseline = baseline_for(mapping_case, a, b)
    result = run_plan(mapping_case, a, b, baseline=baseline)
    assert (result.outcome, result.reason) == ("noop", "unchanged_since_baseline")


def test_normalized_readback_does_not_retry_forever(mapping_case):
    a, b = observed(Status.ON_HOLD), observed(Status.WATCHING)
    baseline = baseline_for(mapping_case, a, b)
    result = run_plan(mapping_case, a, b, baseline=baseline)
    assert (result.outcome, result.reason) == ("noop", "unchanged_since_baseline")


@pytest.mark.parametrize("field,value", [
    ("protected_token", "progress-notes-ratings-dates-v2"),
    ("watchlisted", True), ("rewatching", True),
])
def test_metadata_edits_do_not_trigger_a_status_write(mapping_case, field, value):
    case = one_way(mapping_case)
    a, b = observed(Status.DROPPED), observed()
    baseline = baseline_for(case, a, b)
    result = run_plan(case, a, replace(b, **{field: value}), baseline=baseline)
    assert (result.outcome, result.reason) == ("noop", "status_unchanged")


def test_metadata_change_is_not_mistaken_for_a_two_way_status_change(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    baseline = baseline_for(mapping_case, a, b)
    result = run_plan(mapping_case, a, replace(b, protected_token="edited-note"), baseline=baseline)
    assert (result.outcome, result.reason) == ("noop", "status_unchanged")


def test_source_status_change_and_destination_metadata_change_conflict(mapping_case):
    a, b = observed(), observed()
    baseline = baseline_for(mapping_case, a, b)
    result = run_plan(mapping_case, observed(Status.DROPPED), replace(b, protected_token="edited-note"), baseline=baseline)
    assert result.outcome == "conflict"


def test_native_status_changes_are_observed(mapping_case):
    case = one_way(mapping_case)
    a, b = observed(Status.DROPPED), observed()
    baseline = baseline_for(case, a, b)
    result = run_plan(case, a, replace(b, native_status="REPEATING"), baseline=baseline)
    assert result.outcome == "conflict"


@pytest.mark.parametrize("field,value", [("a_id", "other-title"), ("media", "movies")])
def test_baseline_is_bound_to_native_media_identity(mapping_case, field, value):
    old = observed()
    baseline = baseline_for(mapping_case, old, old)
    baseline = replace(baseline, item=replace(baseline.item, **{field: value}))
    result = run_plan(mapping_case, observed(Status.DROPPED), old, baseline=baseline)
    assert result.outcome == "conflict"


def test_baseline_is_bound_to_provider_instance(mapping_case):
    old = observed()
    baseline = baseline_for(mapping_case, old, old)
    other_pair = replace(baseline.item.pair, b=Endpoint("tracker_b", "other"))
    baseline = replace(baseline, item=replace(baseline.item, pair=other_pair))
    assert run_plan(mapping_case, observed(Status.DROPPED), old, baseline=baseline).outcome == "conflict"


def test_rule_changes_invalidate_old_baseline(mapping_case):
    old = observed()
    baseline = baseline_for(mapping_case, old, old)
    mapping_case[2]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    assert run_plan(mapping_case, observed(Status.DROPPED), old, baseline=baseline).outcome == "conflict"


@pytest.mark.parametrize("side", ["a", "b"])
@pytest.mark.parametrize("state,reason", [
    (ReadState.UNKNOWN, "incomplete_read"),
    (ReadState.CLEARED, "explicit_clear_not_supported"),
])
def test_unknown_and_clear_are_not_absence(mapping_case, side, state, reason):
    a, b = (Observation(state), observed()) if side == "a" else (observed(), Observation(state))
    result = run_plan(one_way(mapping_case), a, b)
    assert (result.outcome, result.reason) == ("skip", reason)


def test_absent_source_is_not_a_clear_or_drop(mapping_case):
    result = run_plan(one_way(mapping_case), Observation(ReadState.ABSENT), observed())
    assert (result.outcome, result.reason) == ("skip", "source_absent")


def test_absent_destination_requires_verified_creation(mapping_case):
    result = run_plan(one_way(mapping_case), observed(Status.DROPPED), Observation(ReadState.ABSENT), checks={})
    assert (result.outcome, result.reason) == ("skip", "transition_not_verified")


@pytest.mark.parametrize("side", ["a", "b"])
def test_planned_is_frozen_regardless_of_conflict_preference(mapping_case, side):
    a, b = (observed(Status.PLANNED), observed()) if side == "a" else (observed(), observed(Status.PLANNED))
    for policy in ConflictPolicy:
        result = run_plan(mapping_case, a, b, resolution=policy)
        assert (result.outcome, result.reason) == ("skip", "watchlist_managed")


@pytest.mark.parametrize("field,value", [("protected_token", ""), ("watchlisted", None), ("rewatching", None)])
def test_incomplete_preservation_data_cannot_authorize_a_write(mapping_case, field, value):
    result = run_plan(one_way(mapping_case), observed(Status.DROPPED), replace(observed(), **{field: value}))
    assert (result.outcome, result.reason) == ("skip", "incomplete_observation")


def test_completed_destination_is_protected(mapping_case):
    result = run_plan(one_way(mapping_case), observed(Status.DROPPED), observed(Status.COMPLETED))
    assert (result.outcome, result.reason) == ("skip", "completed_destination_preserved")


@pytest.mark.parametrize("reason", [
    "watchlist_membership_would_change", "history_would_expand", "resume_would_clear",
    "start_date_would_change", "rewatch_state_would_change", "tracking_entry_required",
    "outbound_integration_effect_unknown", "overlapping_history_write",
])
def test_provider_preferences_cannot_override_transition_blockers(mapping_case, reason):
    a, b = observed(Status.DROPPED), observed()
    item = ItemScope(mapping_case[0], "shows", "native-a", "native-b")
    checks = {Direction.FORWARD: TransitionCheck(item, a, b, Status.DROPPED, verified=True, blockers=(reason,), effects=("Provider-specific effect",))}
    result = run_plan(mapping_case, a, b, checks=checks, resolution=ConflictPolicy.PREFER_A)
    assert (result.outcome, result.reason, result.effects) == ("skip", reason, ("Provider-specific effect",))


def test_preflight_is_bound_to_exact_observations_and_destination(mapping_case):
    mapping_case = one_way(mapping_case)
    item = ItemScope(mapping_case[0], "shows", "native-a", "native-b")
    a, b = observed(Status.DROPPED), observed()
    for check in (
        TransitionCheck(item, a, b, Status.ON_HOLD, verified=True),
        TransitionCheck(item, a, replace(b, protected_token="old"), Status.DROPPED, verified=True),
        TransitionCheck(item, replace(a, rewatching=True), b, Status.DROPPED, verified=True),
        TransitionCheck(replace(item, media="movies"), a, b, Status.DROPPED, verified=True),
        TransitionCheck(item, a, b, Status.DROPPED),
    ):
        result = run_plan(mapping_case, a, b, checks={Direction.FORWARD: check})
        assert (result.outcome, result.reason) == ("skip", "transition_not_verified")


def test_current_capabilities_are_checked_again_at_planning(mapping_case):
    scope, caps, raw = one_way(mapping_case)
    config = parse_mapping(raw, scope=scope, capabilities=caps)
    caps[scope.b, "shows"] = Capabilities()
    result = plan_status(config, item=ItemScope(scope, "shows", "a", "b"), a=observed(Status.DROPPED), b=observed(), capabilities=caps,
                         execution={Direction.FORWARD: ExecutionScope(True, True, frozenset(Status))}, checks={})
    assert (result.outcome, result.reason) == ("skip", "unsupported_status")


@pytest.mark.parametrize("state", [ReadState.UNKNOWN, ReadState.CLEARED])
def test_incomplete_reads_cannot_advance_baseline(mapping_case, state):
    with pytest.raises(ValueError, match="incomplete"):
        baseline_for(mapping_case, observed(), Observation(state))


def test_observation_does_not_conflate_absence_and_status():
    with pytest.raises(ValueError):
        Observation(ReadState.ABSENT, Status.DROPPED)
    with pytest.raises(ValueError):
        Observation(ReadState.PRESENT)


def test_all_excluded_rows_skip_without_conflict(mapping_case):
    for media in mapping_case[2]["directions"].values():
        for rows in media.values():
            for row in rows.values():
                row["include"] = False
    result = run_plan(mapping_case, observed(Status.DROPPED), observed())
    assert (result.outcome, result.reason) == ("skip", "excluded")


def test_creation_needs_expected_preserved_fields(mapping_case):
    result = run_plan(one_way(mapping_case), observed(Status.DROPPED), Observation(ReadState.ABSENT))
    assert (result.outcome, result.reason) == ("skip", "creation_preservation_not_verified")


def verify(case, before_a, before_b, after_a, after_b, **kwargs):
    scope, caps, raw = case
    config = parse_mapping(raw, scope=scope, capabilities=caps)
    assert config is not None
    decision = run_plan(case, before_a, before_b, resolution=ConflictPolicy.PREFER_A)
    return verify_status_write(config, item=ItemScope(scope, "shows", "native-a", "native-b"),
                               decision=kwargs.get("decision", decision), before_a=before_a, before_b=before_b,
                               after_a=after_a, after_b=after_b)


def test_successful_readback_records_actual_state(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    after = observed(Status.DROPPED)
    result = verify(mapping_case, a, b, a, after)
    assert (result.outcome, result.reason) == ("verified", "status_verified")
    assert (result.baseline.a, result.baseline.b) == (a, after)
    assert run_plan(mapping_case, a, after, baseline=result.baseline).outcome == "noop"


def test_normalized_readback_is_reported_and_not_retried(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    after = observed(Status.ON_HOLD)
    result = verify(mapping_case, a, b, a, after)
    assert (result.outcome, result.reason) == ("review", "provider_normalized_status")
    assert result.baseline.b == after
    assert run_plan(mapping_case, a, after, baseline=result.baseline).reason == "unchanged_since_baseline"


@pytest.mark.parametrize("native", ["watching", "native-alias"])
def test_unchanged_status_cannot_establish_normalization(mapping_case, native):
    a, b = observed(Status.DROPPED), observed()
    result = verify(mapping_case, a, b, a, replace(b, native_status=native))
    assert (result.outcome, result.reason, result.baseline) == ("uncertain", "status_write_not_observed", None)


def test_normalization_to_planned_is_not_accepted(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    result = verify(mapping_case, a, b, a, observed(Status.PLANNED))
    assert (result.outcome, result.reason, result.baseline) == ("review", "unexpected_watchlist_status", None)


@pytest.mark.parametrize("field,value", [
    ("protected_token", "different-progress-rating-note-or-date"), ("watchlisted", True), ("rewatching", True),
])
def test_unexpected_effects_do_not_advance_baseline(mapping_case, field, value):
    a, b = observed(Status.DROPPED), observed()
    after = replace(observed(Status.DROPPED), **{field: value})
    result = verify(mapping_case, a, b, a, after)
    assert (result.outcome, result.reason, result.baseline) == ("review", "unexpected_status_write_effects", None)


def test_source_edit_during_write_requires_review(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    result = verify(mapping_case, a, b, observed(Status.ON_HOLD), observed(Status.DROPPED))
    assert (result.outcome, result.reason, result.baseline) == ("review", "source_changed_during_write", None)


@pytest.mark.parametrize("after", [Observation(ReadState.UNKNOWN), Observation(ReadState.CLEARED), replace(observed(), protected_token="")])
def test_failed_readback_is_uncertain_and_retains_old_baseline(mapping_case, after):
    a, b = observed(Status.DROPPED), observed()
    result = verify(mapping_case, a, b, a, after)
    assert (result.outcome, result.reason, result.baseline) == ("uncertain", "readback_incomplete", None)


def test_disappeared_destination_is_reported(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    result = verify(mapping_case, a, b, a, Observation(ReadState.ABSENT))
    assert (result.outcome, result.reason, result.baseline) == ("review", "destination_missing_after_write", None)


def test_noop_is_not_reported_as_a_verified_write(mapping_case):
    a = observed(Status.DROPPED)
    result = verify(mapping_case, a, a, a, a)
    assert (result.outcome, result.reason, result.baseline) == ("review", "no_status_write_planned", None)


def test_reverse_write_readback_checks_correct_destination(mapping_case):
    a, b = observed(), observed(Status.DROPPED)
    decision = run_plan(mapping_case, a, b, resolution=ConflictPolicy.PREFER_B)
    result = verify(mapping_case, a, b, observed(Status.DROPPED), b, decision=decision)
    assert result.outcome == "verified"
    result = verify(mapping_case, a, b, replace(observed(Status.DROPPED), watchlisted=True), b, decision=decision)
    assert (result.outcome, result.reason) == ("review", "unexpected_status_write_effects")


def test_mapping_changed_during_write_cannot_advance_baseline(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    decision = run_plan(mapping_case, a, b, resolution=ConflictPolicy.PREFER_A)
    mapping_case[2]["directions"]["a_to_b"]["shows"]["dropped"]["target"] = "on_hold"
    result = verify(mapping_case, a, b, a, observed(Status.DROPPED), decision=decision)
    assert (result.outcome, result.reason, result.baseline) == ("review", "plan_changed_before_readback", None)


def test_write_decision_cannot_be_reused_for_another_item(mapping_case):
    a, b = observed(Status.DROPPED), observed()
    decision = run_plan(mapping_case, a, b, resolution=ConflictPolicy.PREFER_A)
    decision = replace(decision, item=replace(decision.item, b_id="different-title"))
    result = verify(mapping_case, a, b, a, observed(Status.DROPPED), decision=decision)
    assert (result.outcome, result.reason, result.baseline) == ("review", "plan_changed_before_readback", None)

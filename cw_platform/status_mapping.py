# cw_platform/status_mapping.py
# CrossWatch - Status mapping validation and shared conflict planning.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping
from dataclasses import dataclass
from enum import StrEnum
from typing import Any


STATUS_MAPPING_ERRORS = {
    "invalid_structure": "Invalid status mapping configuration",
    "version": "Unsupported status mapping version",
    "endpoint": "Status mapping endpoint changed; reopen the pair and configure its mappings again",
    "conflict": "Invalid status conflict policy",
    "one_way_preference": "A one-way mapping cannot prefer its destination",
    "reverse": "A one-way mapping cannot contain reverse rules",
    "include": "Status mapping Include must be a boolean",
    "destination": "Invalid status mapping destination",
    "planned": "Planned remains managed by Watchlist",
    "unsupported": "Unsupported status mapping for these providers and title type",
    "tagging": "Use either custom status mapping or Dropped shows tagging for this pair.",
}


class StatusMappingValidationError(ValueError):
    def __init__(self, code: str, detail: str = ""):
        self.code = code
        message = STATUS_MAPPING_ERRORS[code]
        super().__init__(f"{message}: {detail}" if detail else message)


class Status(StrEnum):
    WATCHING = "watching"
    PLANNED = "planned"
    ON_HOLD = "on_hold"
    DROPPED = "dropped"
    COMPLETED = "completed"


class Direction(StrEnum):
    FORWARD = "a_to_b"
    REVERSE = "b_to_a"


class ReadState(StrEnum):
    PRESENT = "present"
    ABSENT = "absent"
    UNKNOWN = "unknown"
    CLEARED = "cleared"


class ConflictPolicy(StrEnum):
    LEAVE = "leave_unchanged"
    PREFER_A = "prefer_a"
    PREFER_B = "prefer_b"


@dataclass(frozen=True)
class Endpoint:
    provider: str
    instance: str


@dataclass(frozen=True)
class PairScope:
    pair_id: str
    a: Endpoint
    b: Endpoint
    two_way: bool = False


@dataclass(frozen=True)
class Capabilities:
    readable: frozenset[Status] = frozenset()
    writable: frozenset[Status] = frozenset()


@dataclass(frozen=True)
class Rule:
    direction: Direction
    media: str
    source: Status
    target: Status
    include: bool


@dataclass(frozen=True)
class MappingConfig:
    scope: PairScope
    conflict: ConflictPolicy
    rules: tuple[Rule, ...]
    fingerprint: str

    def rule(self, direction: Direction, media: str, source: Status) -> Rule | None:
        return next((r for r in self.rules if (r.direction, r.media, r.source) == (direction, media, source)), None)


def _object(raw: Any, keys: set[str], path: str) -> Mapping[str, Any]:
    if not isinstance(raw, Mapping) or any(key not in keys for key in raw):
        raise StatusMappingValidationError("invalid_structure", path)
    return raw


def parse_mapping(
    raw: Any,
    *,
    scope: PairScope,
    capabilities: Mapping[tuple[Endpoint, str], Capabilities],
) -> MappingConfig | None:
    if raw is None:
        return None
    data = _object(raw, {"version", "a", "b", "conflict", "directions"}, "configuration")
    if type(data.get("version")) is not int or data["version"] != 1:
        raise StatusMappingValidationError("version")
    for side, endpoint in (("a", scope.a), ("b", scope.b)):
        ref = _object(data.get(side), {"provider", "instance"}, side)
        if dict(ref) != {"provider": endpoint.provider, "instance": endpoint.instance}:
            raise StatusMappingValidationError("endpoint", side)
    try:
        conflict = ConflictPolicy(data.get("conflict", ConflictPolicy.LEAVE))
    except (TypeError, ValueError) as exc:
        raise StatusMappingValidationError("conflict") from exc
    if not scope.two_way and conflict == ConflictPolicy.PREFER_B:
        raise StatusMappingValidationError("one_way_preference")
    directions = _object(data.get("directions"), {d.value for d in Direction}, "directions")
    rules: list[Rule] = []
    for key, raw_media in directions.items():
        direction = Direction(key)
        if direction == Direction.REVERSE and not scope.two_way:
            raise StatusMappingValidationError("reverse")
        source, target = (scope.a, scope.b) if direction == Direction.FORWARD else (scope.b, scope.a)
        for media, raw_rows in _object(raw_media, {"movies", "shows"}, key).items():
            source_caps = capabilities.get((source, media), Capabilities())
            target_caps = capabilities.get((target, media), Capabilities())
            rows = _object(raw_rows, {s.value for s in Status if s != Status.PLANNED}, f"{key}.{media}")
            for source_status, raw_rule in rows.items():
                row = _object(raw_rule, {"include", "target"}, f"{key}.{media}.{source_status}")
                if type(row.get("include")) is not bool:
                    raise StatusMappingValidationError("include")
                try:
                    target_status = Status(row.get("target"))
                except (TypeError, ValueError) as exc:
                    raise StatusMappingValidationError("destination") from exc
                status = Status(source_status)
                if target_status == Status.PLANNED:
                    raise StatusMappingValidationError("planned")
                if status not in source_caps.readable or target_status not in target_caps.writable:
                    raise StatusMappingValidationError("unsupported", f"{key}.{media}.{source_status}")
                rules.append(Rule(direction, media, status, target_status, row["include"]))
    ordered = tuple(sorted(rules, key=lambda r: (r.direction, r.media, r.source)))
    canonical = [conflict.value, [(r.direction, r.media, r.source, r.target, r.include) for r in ordered]]
    fingerprint = hashlib.sha256(json.dumps(canonical, separators=(",", ":")).encode()).hexdigest()
    return MappingConfig(scope, conflict, ordered, fingerprint)


UNTRACKED_STATES = frozenset({"waiting", "caught_up_or_untracked", "caught_up", "untracked", "dropped_seasons"})


@dataclass(frozen=True)
class Observation:
    state: ReadState
    status: Status | None = None
    native_status: str = ""
    protected_token: str = ""
    watchlisted: bool | None = None
    rewatching: bool | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.state, ReadState):
            raise ValueError("Invalid status read state")
        if self.state == ReadState.PRESENT:
            if not isinstance(self.status, Status):
                raise ValueError("A present status observation needs a known status")
        elif self.status is not None:
            raise ValueError("Only a present observation can carry a status")

    @property
    def complete(self) -> bool:
        return (
            self.state == ReadState.ABSENT
            or self.state == ReadState.PRESENT
            and bool(self.protected_token)
            and type(self.watchlisted) is bool
            and type(self.rewatching) is bool
        )


@dataclass(frozen=True)
class ItemScope:
    pair: PairScope
    media: str
    a_id: str
    b_id: str


@dataclass(frozen=True)
class Baseline:
    item: ItemScope
    mapping_fingerprint: str
    a: Observation
    b: Observation


@dataclass(frozen=True)
class ExecutionScope:
    feature_enabled: bool = False
    opted_in: bool = False
    source_statuses: frozenset[Status] = frozenset()


@dataclass(frozen=True)
class TransitionCheck:
    item: ItemScope
    source: Observation
    target: Observation
    desired: Status
    verified: bool = False
    blockers: tuple[str, ...] = ()
    effects: tuple[str, ...] = ()


@dataclass(frozen=True)
class Decision:
    outcome: str
    reason: str
    direction: Direction | None = None
    desired: Status | None = None
    effects: tuple[str, ...] = ()
    item: ItemScope | None = None
    mapping_fingerprint: str = ""


def plan_status(
    config: MappingConfig | None,
    *,
    item: ItemScope,
    a: Observation,
    b: Observation,
    capabilities: Mapping[tuple[Endpoint, str], Capabilities],
    execution: Mapping[Direction, ExecutionScope],
    checks: Mapping[Direction, TransitionCheck],
    baseline: Baseline | None = None,
    resolution: ConflictPolicy | None = None,
) -> Decision:
    if config is None:
        return Decision("skip", "legacy_behavior")
    if config.scope != item.pair or item.media not in {"movies", "shows"} or not item.a_id or not item.b_id:
        return Decision("skip", "scope_mismatch")
    if a.state == ReadState.UNKNOWN or b.state == ReadState.UNKNOWN:
        unknown = [side for side in (a, b) if side.state == ReadState.UNKNOWN]
        if all(side.native_status in UNTRACKED_STATES for side in unknown):
            return Decision("skip", "caught_up")
        return Decision("skip", "incomplete_read")
    if a.state == ReadState.CLEARED or b.state == ReadState.CLEARED:
        return Decision("skip", "explicit_clear_not_supported")
    if Status.PLANNED in (a.status, b.status):
        return Decision("skip", "watchlist_managed")
    if not a.complete or not b.complete:
        return Decision("skip", "incomplete_observation")

    directions = (Direction.FORWARD, Direction.REVERSE) if item.pair.two_way else (Direction.FORWARD,)

    def authorized(direction: Direction) -> bool:
        gate = execution.get(direction, ExecutionScope())
        return gate.feature_enabled is True and gate.opted_in is True

    if not any(authorized(d) for d in directions):
        return Decision("skip", "opt_in_disabled")

    def candidate(direction: Direction) -> Decision:
        source, target = (a, b) if direction == Direction.FORWARD else (b, a)
        source_ep, target_ep = (item.pair.a, item.pair.b) if direction == Direction.FORWARD else (item.pair.b, item.pair.a)
        gate = execution.get(direction, ExecutionScope())
        if not authorized(direction):
            return Decision("skip", "opt_in_disabled", direction)
        if source.rewatching or target.rewatching:
            return Decision("skip", "rewatch_state_preserved", direction)
        if source.state == ReadState.ABSENT:
            return Decision("skip", "source_absent", direction)
        if source.status is None or source.status not in gate.source_statuses:
            return Decision("skip", "outside_opt_in_scope", direction)
        rule = config.rule(direction, item.media, source.status)
        if rule is None or not rule.include:
            return Decision("skip", "excluded", direction)
        source_caps = capabilities.get((source_ep, item.media), Capabilities())
        target_caps = capabilities.get((target_ep, item.media), Capabilities())
        if source.status not in source_caps.readable or rule.target not in target_caps.writable:
            return Decision("skip", "unsupported_status", direction)
        if rule.target == target.status:
            return Decision("noop", "already_mapped", direction, rule.target)
        if target.status == Status.COMPLETED:
            return Decision("skip", "completed_destination_preserved", direction)
        check = checks.get(direction)
        if check is None or not check.verified or (check.item, check.source, check.target, check.desired) != (item, source, target, rule.target):
            return Decision("skip", "transition_not_verified", direction, rule.target)
        if check.blockers:
            return Decision("skip", check.blockers[0], direction, rule.target, check.effects)
        if not target.protected_token or type(target.watchlisted) is not bool or type(target.rewatching) is not bool:
            return Decision("skip", "creation_preservation_not_verified", direction, rule.target)
        return Decision("write", "mapped", direction, rule.target, check.effects, item, config.fingerprint)

    forward = candidate(Direction.FORWARD)
    reverse = candidate(Direction.REVERSE) if item.pair.two_way else None
    valid_baseline = (
        baseline is not None
        and baseline.item == item
        and baseline.mapping_fingerprint == config.fingerprint
        and baseline.a.complete
        and baseline.b.complete
    )
    if valid_baseline and baseline is not None:
        a_changed = (a.state, a.status, a.native_status) != (baseline.a.state, baseline.a.status, baseline.a.native_status)
        b_changed = (b.state, b.status, b.native_status) != (baseline.b.state, baseline.b.status, baseline.b.native_status)
        if not a_changed and not b_changed:
            reason = "unchanged_since_baseline" if (a, b) == (baseline.a, baseline.b) else "status_unchanged"
            return Decision("noop", reason)
        if a_changed and b == baseline.b:
            return forward
        if b_changed and a == baseline.a and reverse is not None:
            return reverse
    elif not item.pair.two_way:
        return forward

    if forward.outcome == "noop":
        return forward
    if reverse is not None and reverse.outcome == "noop":
        return reverse
    policy = resolution if resolution is not None else config.conflict
    if policy == ConflictPolicy.PREFER_A:
        return forward
    if policy == ConflictPolicy.PREFER_B and reverse is not None:
        return reverse
    if forward.outcome == "skip" and (reverse is None or reverse.outcome == "skip"):
        return forward
    return Decision("conflict", "status_conflict")


def verified_baseline(config: MappingConfig, item: ItemScope, a: Observation, b: Observation) -> Baseline:
    if config.scope != item.pair or item.media not in {"movies", "shows"} or not item.a_id or not item.b_id:
        raise ValueError("Cannot record a baseline for a different mapping scope")
    if not a.complete or not b.complete:
        raise ValueError("Cannot record a baseline from incomplete status observations")
    return Baseline(item, config.fingerprint, a, b)


@dataclass(frozen=True)
class WriteVerification:
    outcome: str
    reason: str
    baseline: Baseline | None = None


def verify_status_write(
    config: MappingConfig,
    *,
    item: ItemScope,
    decision: Decision,
    before_a: Observation,
    before_b: Observation,
    after_a: Observation,
    after_b: Observation,
) -> WriteVerification:
    if decision.outcome != "write" or not isinstance(decision.direction, Direction) or not isinstance(decision.desired, Status):
        return WriteVerification("review", "no_status_write_planned")
    if config.scope != item.pair or item.media not in {"movies", "shows"} or not item.a_id or not item.b_id:
        return WriteVerification("review", "scope_mismatch")
    if decision.direction == Direction.REVERSE and not item.pair.two_way:
        return WriteVerification("review", "scope_mismatch")
    if decision.item != item or decision.mapping_fingerprint != config.fingerprint:
        return WriteVerification("review", "plan_changed_before_readback")
    if not after_a.complete or not after_b.complete:
        return WriteVerification("uncertain", "readback_incomplete")
    if decision.direction == Direction.FORWARD:
        source_before, source_after, target_before, target_after = before_a, after_a, before_b, after_b
    else:
        source_before, source_after, target_before, target_after = before_b, after_b, before_a, after_a
    if source_before != source_after:
        return WriteVerification("review", "source_changed_during_write")
    if target_after.state != ReadState.PRESENT:
        return WriteVerification("review", "destination_missing_after_write")
    if target_after.status == Status.PLANNED:
        return WriteVerification("review", "unexpected_watchlist_status")
    if not target_before.protected_token or any(
        getattr(target_before, field) != getattr(target_after, field)
        for field in ("protected_token", "watchlisted", "rewatching")
    ):
        return WriteVerification("review", "unexpected_status_write_effects")
    if target_after.status == target_before.status and target_after.status != decision.desired:
        return WriteVerification("uncertain", "status_write_not_observed")
    baseline = verified_baseline(config, item, after_a, after_b)
    if target_after.status != decision.desired:
        return WriteVerification("review", "provider_normalized_status", baseline)
    return WriteVerification("verified", "status_verified", baseline)

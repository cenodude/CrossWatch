# cw_platform/orchestrator/_status_mapping.py
# CrossWatch - Shared scheduled and interactive native status execution.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import asdict, replace
from uuid import uuid4

from cw_platform.interactive_reads import read_once, use_review_reads
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from cw_platform.run_control import raise_if_cancelled
from cw_platform.status_mapping import (
    ConflictPolicy, Decision, Direction, Endpoint, ExecutionScope, ItemScope, PairScope, Status, TransitionCheck,
    StatusMappingValidationError, parse_mapping, plan_status, verified_baseline, verify_status_write,
)
from cw_platform.status_mapping_store import StatusStore, decode_item, decode_observation, encode_observation
from providers.sync._status_mapping import NativeStatusAdapter, adapter_class


class _FreshReads:
    def __init__(self, adapters):
        self.adapters = adapters
        self.snapshots = {}

    def record(self, endpoint, reference):
        adapter = self.adapters[endpoint]
        with use_review_reads(None):
            if getattr(getattr(adapter, "read_record", None), "__func__", None) is not NativeStatusAdapter.read_record:
                return adapter.read_record(reference)
            if endpoint not in self.snapshots:
                try:
                    self.snapshots[endpoint] = adapter.read()
                except Exception as exc:
                    self.snapshots[endpoint] = exc
                    raise
            snapshot = self.snapshots[endpoint]
            if isinstance(snapshot, Exception):
                raise snapshot
            return snapshot.get(reference.native_id) or adapter.missing_record(reference, snapshot)


def pair_scope(pair):
    return PairScope(str(pair.get("id") or ""),
                     Endpoint(str(pair.get("source") or "").lower(), normalize_instance_id(pair.get("source_instance"))),
                     Endpoint(str(pair.get("target") or "").lower(), normalize_instance_id(pair.get("target_instance"))),
                     str(pair.get("mode") or "one-way") == "two-way")


def capabilities(scope):
    result = {}
    for endpoint in (scope.a, scope.b):
        cls = adapter_class(endpoint.provider)
        if cls:
            result.update({(endpoint, media): value for media, value in cls.capabilities.items()})
    return result


def validate_pair(pair):
    scope = pair_scope(pair)
    if pair.get("status_mapping") and any(isinstance(value, dict) and value.get("history_dropped_mode") == "tag"
                                          for value in (pair.get("providers") or {}).values()):
        raise StatusMappingValidationError("tagging")
    return parse_mapping(pair.get("status_mapping"), scope=scope, capabilities=capabilities(scope))


def skip_reason(pair):
    history = (pair.get("features") or {}).get("history") or {}
    if pair.get("enabled", True) is False:
        return "The pair is disabled."
    if not pair.get("status_mapping"):
        return "No custom status mapping is saved. Existing provider behavior applies."
    if not isinstance(history, dict) or history.get("enable") is not True:
        return "History is disabled. Enable History to run status mapping."
    if history.get("use_source_status") is not True:
        return "Status mapping is switched off. Enable Use source watch status to run it."
    return ""


def publish_status(ctx, pair, event, **values):
    summary = dict(pair_id=pair.get("id"), feature="history", src=pair.get("source"), dst=pair.get("target"),
                   src_instance=pair.get("source_instance") or "default", dst_instance=pair.get("target_instance") or "default",
                   mode=pair.get("mode") or "one-way", **values)
    review = getattr(ctx, "interactive", None)
    if review is not None:
        review.status_mappings = [row for row in review.status_mappings if row["pair_id"] != summary["pair_id"]] + [summary]
    ctx.emit("status_mapping:" + event, **summary)


def report_skipped(ctx, pair, reason):
    history = (pair.get("features") or {}).get("history") or {}
    if pair.get("status_mapping") or isinstance(history, dict) and history.get("use_source_status"):
        publish_status(ctx, pair, "skip", state="skipped", reason=reason)


def _identity_ids(record, allow_mapping):
    return (record.item.get("ids") if allow_mapping else record.raw.get("native_ids", record.item.get("ids"))) or {}


def _tokens(record, allow_mapping=True):
    ids = _identity_ids(record, allow_mapping)
    native = {k: str(v) for k, v in ids.items() if k in {"anilist", "mal", "kitsu", "anidb"} and v}
    values = native if record.media == "shows" and native else {k: str(v) for k, v in ids.items() if k in {"tmdb", "imdb", "tvdb", "simkl", "anilist", "mal", "kitsu", "anidb", "wetrakr", "trakt"} and v}
    return {(record.media, k, v) for k, v in values.items()}


def matched_records(left, right, *, allow_mapping=True):
    index = {}
    for ident, record in right.items():
        for token in _tokens(record, allow_mapping):
            index.setdefault(token, set()).add(ident)
    candidates = {}
    for ident, record in left.items():
        hits = set().union(*(index.get(token, set()) for token in _tokens(record, allow_mapping)))
        if len(hits) == 1:
            candidates[ident] = next(iter(hits))
    counts = {}
    for ident in candidates.values():
        counts[ident] = counts.get(ident, 0) + 1
    for a_id, b_id in candidates.items():
        a, b = left[a_id], right[b_id]
        a_ids, b_ids = _identity_ids(a, allow_mapping), _identity_ids(b, allow_mapping)
        shared = set(a_ids) & set(b_ids)
        if counts[b_id] == 1 and not any(str(a_ids[key]) != str(b_ids[key]) for key in shared if key in {"anilist", "mal", "kitsu", "anidb", "tmdb", "imdb", "tvdb", "simkl", "wetrakr", "trakt"}):
            yield a, b


def run_status_mappings(ctx, pairs, cfg, health_map, *, store=None, adapter_overrides=None):
    store = store or StatusStore()
    pairs = list(pairs)
    endpoints = {endpoint for pair in pairs if not skip_reason(pair)
                 for endpoint in (pair_scope(pair).a, pair_scope(pair).b) if endpoint not in (adapter_overrides or {})}
    with store.exclusive_instances(endpoints):
        return _run_status_mappings(ctx, pairs, cfg, health_map, store=store, adapter_overrides=adapter_overrides)


def _run_status_mappings(ctx, pairs, cfg, health_map, *, store, adapter_overrides=None):
    review = getattr(ctx, "interactive", None)
    preview = review is not None and review.preview
    dry_run = bool(getattr(ctx, "dry_run", False) or (cfg.get("sync") or {}).get("dry_run") or (cfg.get("sync") or {}).get("enable_add") is False)
    totals = {"updated": 0, "unresolved": 0, "errors": 0, "skipped": 0}
    adapters, snapshots, proposals, claims = {}, {}, [], {}
    summaries = {}
    planning_reads = _FreshReads(adapters)

    def report(pair, reason, *, item=None, error=False):
        totals["errors" if error else "unresolved"] += 1
        summaries[pair.get("id")]["errors" if error else "unresolved"] += 1
        ctx.emit("status_mapping:review", pair_id=pair.get("id"), feature="history", src=pair.get("source"), dst=pair.get("target"),
                 reason=reason, title=(item or {}).get("title") or "")
        if item:
            ctx.emit("apply:unresolved", pair_id=pair.get("id"), feature="history", provider=pair.get("target"),
                     destination_instance=pair.get("target_instance") or "default", op="update", items=[{"item": item, "reason": reason}])
        if review is not None:
            if error:
                review.failed_features.add("history")
                review.completed_features.discard("history")
            if not preview and review.on_result:
                review.on_result("history", pair.get("source"), pair.get("target"), pair.get("source_instance") or "default",
                                 pair.get("target_instance") or "default", "one-way", {"errors" if error else "unresolved": 1})
            notice = {"feature": "history", "provider": pair.get("target"), "reason": "Status mapping", "detail": reason + (" — " + item["title"] if item and item.get("title") else "")}
            if notice not in review.notices:
                review.notices.append(notice)

    for pair in pairs:
        raise_if_cancelled()
        reason = skip_reason(pair)
        if not reason and review is not None and review.allowed_features is not None and "history" not in review.allowed_features:
            reason = "History is outside this Interactive Sync selection."
        if reason:
            report_skipped(ctx, pair, reason)
            continue
        summary = summaries[pair.get("id")] = dict(state="checking", preview=preview, dry_run=dry_run,
            matched=0, unmatched=0, proposed=0, unchanged=0, excluded=0, recovered=0, updated=0, unresolved=0, errors=0, not_selected=0)
        publish_status(ctx, pair, "start", **summary)
        try:
            scope = pair_scope(pair)
            caps = capabilities(scope)
            config = validate_pair(pair)
            if config is None:
                continue
            for endpoint in (scope.a, scope.b):
                if endpoint not in adapters:
                    cls = adapter_class(endpoint.provider)
                    if cls is None:
                        raise ValueError("This provider does not support native status mapping.")
                    h = health_map.get(f"{endpoint.provider.upper()}#{endpoint.instance}") or health_map.get(endpoint.provider.upper()) or {}
                    if h.get("status") in {"down", "auth_failed"}:
                        raise ValueError("A status provider is unavailable.")
                    view = build_provider_config_view(cfg, endpoint.provider.upper(), endpoint.instance)
                    view["_cw_provider_instance"] = endpoint.instance
                    instances = (cfg.get(endpoint.provider) or {}).get("instances")
                    if instances:
                        view[endpoint.provider] = {**view[endpoint.provider], "instances": instances}
                    adapter = (adapter_overrides or {}).get(endpoint) or cls(view)
                    snapshots[endpoint] = read_once(["status_mapping", asdict(endpoint)], adapter.read, scoped=False)
                    adapters[endpoint] = adapter
            left, right = dict(snapshots[scope.a]), dict(snapshots[scope.b])
            allow_mapping = bool((pair.get("features") or {}).get("history", {}).get("use_anime_mapping", (cfg.get("anime_mapping") or {}).get("enabled", False)))
            for sources, destinations, endpoint in ((left, right, scope.b), (right, left, scope.a)):
                if not adapters[endpoint].creates_missing or endpoint == scope.a and not scope.two_way:
                    continue
                covered = {token for target in destinations.values() for token in _tokens(target, allow_mapping)}
                for source in list(sources.values()):
                    if _tokens(source, allow_mapping) & covered:
                        continue
                    candidate = adapters[endpoint].missing_record(replace(source, item={**source.item, "ids": _identity_ids(source, allow_mapping)}), destinations)
                    if candidate:
                        destinations[candidate.native_id] = candidate
                        covered.update(_tokens(candidate, allow_mapping))
            matched = list(matched_records(left, right, allow_mapping=allow_mapping))
            missing = len(left) - len(matched) + (len(right) - len(matched) if scope.two_way else 0)
            summary.update(source_count=len(snapshots[scope.a]), destination_count=len(snapshots[scope.b]), matched=len(matched), unmatched=missing)
            publish_status(ctx, pair, "read", **summary)
            if missing:
                report(pair, f"{missing} status entries have no unique matching title on the other side. No status was written for those entries.")
            for a, b in matched:
                item = ItemScope(scope, a.media, a.native_id, b.native_id)
                state = read_once(["status_baseline", store.key(item)], lambda item=item: store.get(item), scoped=False)
                if state.get("pending_conflict"):
                    report(pair, "Multiple previous status writes need review before this title can be changed.", item=a.item)
                    continue
                baseline = store.baseline(item, state)
                pending = state.get("pending") or {}
                if pending.get("a") and pending.get("b") and not preview and not dry_run:
                    current_a = planning_reads.record(scope.a, a)
                    raise_if_cancelled()
                    current_b = planning_reads.record(scope.b, b)
                    raise_if_cancelled()
                    if current_a is None or current_b is None:
                        report(pair, "The pending status write could not be read back. Refresh before retrying.", item=a.item)
                        continue
                    a, b = current_a, current_b
                execution = {direction: ExecutionScope(True, True, frozenset(Status) - {Status.PLANNED}) for direction in Direction}
                checks = {}
                for direction, source, target, endpoint in ((Direction.FORWARD, a, b, scope.b), (Direction.REVERSE, b, a, scope.a)):
                    rule = config.rule(direction, a.media, source.observation.status) if source.observation.status else None
                    if rule:
                        blockers = adapters[endpoint].blockers(target, rule.target)
                        checks[direction] = TransitionCheck(item, source.observation, target.observation, rule.target, verified=True, blockers=blockers)
                for direction in (Direction.FORWARD, Direction.REVERSE) if scope.two_way else (Direction.FORWARD,):
                    intention_config = replace(config, rules=tuple(rule for rule in config.rules if rule.direction == direction))
                    intention = plan_status(intention_config, item=item, a=a.observation, b=b.observation, capabilities=caps,
                        execution=execution, checks=checks, resolution=ConflictPolicy.PREFER_A if direction == Direction.FORWARD else ConflictPolicy.PREFER_B)
                    if intention.outcome in {"write", "noop"} and intention.desired is not None:
                        endpoint, ident = (scope.b, b.native_id) if direction == Direction.FORWARD else (scope.a, a.native_id)
                        claims.setdefault((endpoint, item.media, ident), set()).add(intention.desired)
                if pending.get("a") and pending.get("b"):
                    old_item = decode_item(pending.get("item") or asdict(item))
                    old_config = replace(config, scope=old_item.pair, fingerprint=pending["mapping_fingerprint"])
                    old_decision = Decision("write", "mapped", Direction(pending["direction"]), Status(pending["desired"]), item=old_item, mapping_fingerprint=old_config.fingerprint)
                    verified = verify_status_write(old_config, item=old_item, decision=old_decision, before_a=decode_observation(pending["a"]),
                                                   before_b=decode_observation(pending["b"]), after_a=a.observation, after_b=b.observation)
                    if verified.baseline:
                        if not preview and not dry_run and not store.settle(verified.baseline, expected_state=state):
                            report(pair, "Status state changed during verification. The newer operation was preserved; refresh before continuing.", item=a.item)
                        elif verified.outcome != "verified":
                            report(pair, verified.reason.replace("_", " "), item=a.item)
                        else:
                            summary["recovered"] += 1
                        continue
                    if review is None:
                        report(pair, "A previous status write needs review. Refresh Interactive sync and select this status update to retry.", item=a.item)
                        continue
                decision = plan_status(config, item=item, a=a.observation, b=b.observation, capabilities=caps, execution=execution, checks=checks, baseline=baseline)
                if decision.outcome == "conflict" and review is not None:
                    winner = review.conflict("history", "status:" + store.key(item), scope.a.provider.upper() + "#" + scope.a.instance,
                                             scope.b.provider.upper() + "#" + scope.b.instance,
                                             {"watch_status": a.observation.status, "_cw_status_mapping": True, "title": a.item.get("title")},
                                             {"watch_status": b.observation.status, "_cw_status_mapping": True, "title": b.item.get("title")}, "")
                    resolution = ConflictPolicy.PREFER_A if winner == scope.a.provider.upper() + "#" + scope.a.instance else ConflictPolicy.PREFER_B if winner else None
                    if resolution:
                        decision = plan_status(config, item=item, a=a.observation, b=b.observation, capabilities=caps, execution=execution,
                                               checks=checks, baseline=baseline, resolution=resolution)
                if decision.outcome == "noop":
                    if pending:
                        report(pair, "A previous status write changed other fields or returned an unexpected state. Review the provider before changing this mapping.", item=a.item)
                        continue
                    summary["unchanged"] += 1
                    if not preview and not dry_run and not state.get("pending"):
                        if not store.settle(verified_baseline(config, item, a.observation, b.observation), expected_state=state):
                            report(pair, "Status state changed since planning. The newer operation was preserved; refresh before continuing.", item=a.item)
                    continue
                if decision.outcome != "write":
                    if pending and decision.reason == "caught_up":
                        report(pair, "A previous status write could not be verified. Review the provider before retrying.", item=a.item)
                    elif decision.reason not in {"excluded", "source_absent", "watchlist_managed", "caught_up"}:
                        report(pair, decision.reason.replace("_", " "), item=a.item)
                    else:
                        summary["excluded"] += 1
                    continue
                proposals.append((pair, config, item, a, b, decision, state))
        except Exception as exc:
            report(pair, f"Status read or planning failed ({type(exc).__name__}). Refresh before retrying.", error=True)

    before_reads = _FreshReads(adapters)
    written = []
    touched_records = set()
    for pair, config, item, a, b, decision, state in proposals:
        raise_if_cancelled()
        source, target = (a, b) if decision.direction == Direction.FORWARD else (b, a)
        source_ep, target_ep = (item.pair.a, item.pair.b) if decision.direction == Direction.FORWARD else (item.pair.b, item.pair.a)
        pending = state.get("pending") or {}
        if pending and not (
            review is not None and pending.get("direction") == decision.direction and pending.get("desired") == decision.desired
            and store.key(decode_item(pending.get("item") or asdict(item))) == store.key(item)
            and pending.get("a") == encode_observation(a.observation) and pending.get("b") == encode_observation(b.observation)
        ):
            report(pair, "A different status operation is still pending for this title. Resolve the original operation before applying this change.", item=target.item)
            continue
        owners = store.pending_targets().get((target_ep, item.media, target.native_id), set())
        if owners and (not pending or review is None or owners != {store.key(item)}):
            report(pair, "A previous status write for this destination needs review before another pair can change it.", item=target.item)
            continue
        if len(claims[target_ep, item.media, target.native_id]) > 1:
            report(pair, "Other pairs propose a different status for this title.", item=target.item)
            continue
        summaries[pair.get("id")]["proposed"] += 1
        display = {**target.item, "watch_status": decision.desired.value,
                   "_cw_status_mapping": {"from": target.observation.status, "to": decision.desired.value}}
        selected = True
        if review is not None:
            from cw_platform.id_map import canonical_key
            selected = bool(review.filter("history", target_ep.provider.upper(), target_ep.instance, "update", [display],
                            source=source_ep.provider.upper(), source_instance=source_ep.instance,
                            before={canonical_key(display): {"watch_status": target.observation.status, "_cw_status_mapping": True}}, scope="status:" + store.key(item)))
        if preview or dry_run or not selected:
            if not preview and not selected:
                summaries[pair.get("id")]["not_selected"] += 1
            if pending and not preview and not dry_run and not selected:
                report(pair, "The previous status write is still pending. Refresh Interactive sync to review its retry.", item=target.item)
            continue
        records = {(item.pair.a, item.media, a.native_id), (item.pair.b, item.media, b.native_id)}
        if records & touched_records:
            report(pair, "Another status update for this title is still being verified in this run. It will be applied on the next run.", item=target.item)
            continue
        try:
            current_a = before_reads.record(item.pair.a, a)
            raise_if_cancelled()
            current_b = before_reads.record(item.pair.b, b)
            raise_if_cancelled()
            if current_a is None or current_b is None or (current_a.observation, current_b.observation) != (a.observation, b.observation):
                report(pair, "The title changed since planning. Refresh before applying its status.", item=target.item)
                continue
            operation = pending or {"operation_id": uuid4().hex, "item": asdict(item), "direction": decision.direction, "desired": decision.desired, "mapping_fingerprint": config.fingerprint,
                                    "a": encode_observation(a.observation), "b": encode_observation(b.observation)}
            reserved_state = {**state, "pending": operation}
            if not store.reserve(item, reserved_state, expected_pending=pending or None):
                report(pair, "A pending status operation now protects this title or destination. Refresh before applying its status.", item=target.item)
                continue
            with use_review_reads(None):
                raise_if_cancelled()
                touched_records.update(records)
                adapters[target_ep].write(target, decision.desired)
            written.append((pair, config, item, a, b, decision, source_ep, target, target_ep, operation, reserved_state))
        except Exception as exc:
            report(pair, f"Status update could not be verified ({type(exc).__name__}). Review before retrying.", item=target.item, error=True)
    after_reads = _FreshReads(adapters)
    for pair, config, item, a, b, decision, source_ep, target, target_ep, operation, reserved_state in written:
        try:
            after_a = after_reads.record(item.pair.a, a)
            after_b = after_reads.record(item.pair.b, b)
            if after_a is None or after_b is None:
                raise RuntimeError("status_readback_missing")
            write_item = decode_item(operation["item"])
            write_config = replace(config, scope=write_item.pair, fingerprint=operation["mapping_fingerprint"])
            write_decision = replace(decision, item=write_item, mapping_fingerprint=write_config.fingerprint)
            verified = verify_status_write(write_config, item=write_item, decision=write_decision, before_a=a.observation, before_b=b.observation,
                                           after_a=after_a.observation, after_b=after_b.observation)
            if verified.baseline and not store.settle(verified.baseline, expected_state=reserved_state):
                report(pair, "Status state changed during verification. The newer operation was preserved; refresh before continuing.", item=target.item)
                continue
            if verified.outcome != "verified":
                report(pair, verified.reason.replace("_", " "), item=target.item)
            else:
                totals["updated"] += 1
                summaries[pair.get("id")]["updated"] += 1
                ctx.emit("status_mapping:updated", pair_id=pair.get("id"), feature="history", provider=target_ep.provider.upper(),
                         instance=target_ep.instance, title=target.item.get("title") or "", before=target.observation.status, after=decision.desired.value)
                if review is not None and review.on_result:
                    review.on_result("history", source_ep.provider.upper(), target_ep.provider.upper(), source_ep.instance, target_ep.instance, "one-way", {"updated": 1})
                ctx.emit("apply:done", pair_id=pair.get("id"), provider=target_ep.provider.upper(), instance=target_ep.instance,
                         feature="history", op="update", updated=1, count=1, status_mapping=True)
        except Exception as exc:
            report(pair, f"Status update could not be verified ({type(exc).__name__}). Review before retrying.", item=target.item, error=True)
    for pair in pairs:
        if pair.get("id") in summaries:
            summary = summaries[pair.get("id")]
            summary["state"] = "attention" if summary["errors"] or summary["unresolved"] else "complete"
            publish_status(ctx, pair, "done", **summary)
    return totals

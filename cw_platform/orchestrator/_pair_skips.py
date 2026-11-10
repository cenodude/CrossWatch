# cw_platform/orchestrator/_pair_skips.py
# Items a pair leaves out on purpose: dropped shows in Ignore mode and watches before a rewatch reset.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Iterable, Mapping
from typing import Any

from ..local_db import pair_skips as store
from ._dropped import matches_dropped_show
from ._history_resets import ResetPlan, reset_before

Endpoint = tuple[str, str]


def skip_state(endpoints: tuple[Endpoint, Endpoint], *, dropped: Iterable[str] = (),
               resets: Mapping[str, int] | None = None) -> dict[str, Any]:
    tokens = sorted({str(token) for token in dropped if str(token or "").strip()})
    dates = {str(token): int(when) for token, when in (resets or {}).items() if token and when is not None}
    if not tokens and not dates:
        return {}
    return {
        "endpoints": [[str(provider).upper(), str(instance or "default")] for provider, instance in endpoints],
        "dropped": tokens,
        "resets": dates,
    }


def record_pair_skips(
    state_store: Any,
    feature: str,
    endpoints: tuple[Endpoint, Endpoint],
    *,
    dropped: Iterable[str] = (),
    reset_plan: ResetPlan | None = None,
    emit: Callable[..., Any],
) -> None:
    scope = getattr(state_store, "pair_scope", None)
    if not scope:
        return
    state = skip_state(endpoints, dropped=dropped, resets=reset_plan.reset_at if reset_plan else None)
    try:
        store.save(state_store.base_path, scope, str(feature or "").lower(), state)
    except Exception as exc:
        emit("debug", msg="pair_skips.save_failed", feature=feature, error=str(exc)[:200])


Rule = tuple[set[str], dict[str, int]]
Route = tuple[str, str]


def _rule(state: Mapping[str, Any] | None) -> Rule | None:
    if not isinstance(state, Mapping):
        return None
    dropped = {str(t) for t in state.get("dropped") or [] if str(t or "").strip()}
    resets: dict[str, int] = {}
    for show, when in (state.get("resets") or {}).items():
        try:
            resets[str(show)] = int(when)
        except (TypeError, ValueError):
            continue
    return (dropped, resets) if dropped or resets else None


def _rule_reason(rule: Rule | None, item: Mapping[str, Any]) -> str:
    if rule is None:
        return ""
    dropped, resets = rule
    if dropped and matches_dropped_show(item, dropped):
        return "dropped"
    if reset_before(resets, item):
        return "rewatch_reset"
    return ""


class PairSkips:
    def __init__(self, entries: Iterable[tuple[str, Iterable[Route], Mapping[str, Any] | None]]) -> None:
        self.routes: dict[tuple[str, str, str], list[Rule | None]] = {}
        for feature, routes, state in entries:
            rule = _rule(state)
            for src, dst in routes:
                self.routes.setdefault((str(feature or "").lower(), src, dst), []).append(rule)

    def __bool__(self) -> bool:
        return any(rule is not None for rules in self.routes.values() for rule in rules)

    def reason(self, feature: str, src: str, dst: str, item: Mapping[str, Any]) -> str:
        rules = self.routes.get((str(feature or "").lower(), src, dst))
        if not rules or not isinstance(item, Mapping):
            return ""
        reasons = [_rule_reason(rule, item) for rule in rules]
        return reasons[0] if all(reasons) else ""

    def skipped(self, feature: str, src: str, dst: str, item: Mapping[str, Any]) -> bool:
        return bool(self.reason(feature, src, dst, item))


def load_pair_skips(base: Any, pairs: Iterable[tuple[str, str, Iterable[Route]]]) -> PairSkips:
    wanted = [(str(scope), str(feature).lower(), list(routes)) for scope, feature, routes in pairs if scope]
    if not wanted:
        return PairSkips(())
    rows = store.load(base, {scope for scope, _, _ in wanted})
    return PairSkips((feature, routes, rows.get((scope, feature))) for scope, feature, routes in wanted)

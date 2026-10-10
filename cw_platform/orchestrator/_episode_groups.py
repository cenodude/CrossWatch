# cw_platform/orchestrator/_episode_groups.py
# CrossWatch - History episode groups in ordinary and interactive synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from ..episode_groups import check_corrections, coverage, endpoint, plan_group, tokens, validate_groups
from ..local_db import episode_groups as storage
from ..local_db.manual_policy import load_policy
from ._interactive import fingerprint
from ._specials import specials_excluded
from ._pairs_utils import filter_manual_block


class HistoryGroups:
    def __init__(self, ctx, feature, endpoints, indexes, *, reverse=False, blocked="", fcfg=None, blocks=None):
        self.ctx = ctx
        self.endpoints = endpoints
        self.groups = []
        self.kept = [{}, {}]
        self.adds = [[], []]
        self.states = {}
        self.deferred = set()
        self.tokens = [set(), set()]
        self.writable = not blocked
        self.scope = getattr(ctx.state_store, "pair_scope", None)
        pair_id = getattr(ctx.state_store, "mapping_pair_id", "")
        if feature != "history" or not pair_id or not self.scope:
            return
        policy = load_policy(ctx.state_store.base_path)
        raw = ((policy.get("pairs") or {}).get(pair_id) or {}).get("episode_groups") or []
        self.groups = validate_groups(raw)
        if not self.groups:
            return
        check_corrections(policy, pair_id, self.groups)
        for group in self.groups:
            for side in ("source", "target"):
                if endpoint(group[side]) not in endpoints:
                    raise ValueError("Episode group provider instances no longer match this sync pair")
                self.tokens[endpoints.index(endpoint(group[side]))].update(
                    token for episode in group[side]["episodes"] for token in tokens(episode))
        lookups = [{}, {}]
        for index, items in enumerate(indexes):
            for key, item in items.items():
                shared = self.tokens[index] & tokens(item)
                if shared:
                    self.kept[index][key] = item
                    for token in shared:
                        lookups[index].setdefault(token, {})[key] = item
        previous = storage.load(ctx.state_store.base_path, self.scope)
        for group in self.groups:
            sides = [endpoint(group[side]) for side in ("source", "target")]
            if set(sides) != set(endpoints):
                raise ValueError("Episode group provider instances no longer match this sync pair")
            order = [endpoints.index(side) for side in sides]
            current = {side: {key: item for episode in group[side]["episodes"] for token in tokens(episode)
                              for key, item in lookups[index].get(token, {}).items()}
                       for side, index in zip(("source", "target"), order)}
            signature = fingerprint({side: dict(endpoint=endpoint(group[side]), episodes=sorted(
                sorted(tokens(episode)) for episode in group[side]["episodes"])) for side in ("source", "target")})
            old = previous.get(group["id"]) or {}
            if old.get("mapping") != signature:
                old = {}
            group_blocked = blocked
            if specials_excluded(feature, fcfg) and any(ep["season"] == 0 for side in ("source", "target") for ep in group[side]["episodes"]):
                group_blocked = "This group includes specials excluded by the pair."
            if any(len(filter_manual_block(group[side]["episodes"], set(blocks or []))) != len(group[side]["episodes"])
                   for side in ("source", "target")):
                group_blocked = "An episode in this group is blocked for this pair."
            planned, state, reason = plan_group(group, current, old, reverse=True, blocked=group_blocked)
            self.states[group["id"]] = {**state, "mapping": signature}
            if group_blocked:
                self.deferred.add(group["id"])
                self.states[group["id"]] = old
            for side, index in zip(("source", "target"), order):
                if reverse or index == 1:
                    self.adds[index].extend(planned[side])
            if reason:
                ctx.emit("writes:skipped", feature="history", reason=f"{group['name']}: {reason}",
                         episode_group=group["id"], count=1)
            review = getattr(ctx, "interactive", None)
            if review is not None:
                counts = " / ".join(f"{group[side]['provider']}: {sum(state['coverage'][side])}/{len(state['coverage'][side])} parts watched"
                                    for side in ("source", "target"))
                review.notices.append(dict(event="history:episode_group", feature="history",
                                           reason=f"{group['name']} — {reason or counts}", provider=""))
    def contains(self, index, item):
        return bool(self.tokens[index] & tokens(item))

    def strip(self, index, items):
        return {key: item for key, item in items.items() if not self.contains(index, item)} if self.groups else items

    def ordinary(self, index, items):
        return [item for item in items if not self.contains(index, item)] if self.groups else items

    def restore(self, index, items):
        return {**self.strip(index, items), **self.kept[index]} if self.groups else items

    def commit(self, indexes):
        if not self.groups or not self.writable:
            return
        for group in self.groups:
            if group["id"] in self.deferred:
                continue
            state = self.states[group["id"]]
            states = {side: coverage(indexes[self.endpoints.index(endpoint(group[side]))], group[side])
                      for side in ("source", "target")}
            state["coverage"] = states
            if all(all(values) for values in states.values()) or not any(any(values) for values in states.values()):
                state["held"] = False
        storage.save(self.ctx.state_store.base_path, self.scope, self.states)

# services/analyzer_episode_groups.py
# CrossWatch - Read-only episode group comparisons for the Analyzer
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import importlib
from types import SimpleNamespace

from cw_platform.episode_groups import endpoint, tokens, watched
from cw_platform.mapping_policy import effective_policy
from cw_platform.modules_registry import get_sync_module_path_by_name
from cw_platform.orchestrator._episode_groups import HistoryGroups
from cw_platform.orchestrator._history_rewatches import collapse_history_latest, filter_history_events, history_rewatch_pair_enabled
from cw_platform.orchestrator._pairs_utils import manual_policy
from cw_platform.pair_scope import pair_feature_scope


def provider_ops(provider):
    path = get_sync_module_path_by_name(provider)
    return getattr(importlib.import_module(path), "OPS", None) if path else None


class AnalyzerEpisodeGroups:
    def __init__(self, analysis):
        from services import analyzer as an

        self.routes = {}
        self.members = {}
        self.problems = []
        policy = an._load_manual_state()
        for pair in analysis.cfg.get("pairs") or []:
            pair_id = an._pair_id(pair)
            if not ((policy.get("pairs") or {}).get(pair_id) or {}).get("episode_groups"):
                continue
            directions = an._pair_map({"pairs": [pair]}, analysis.state)
            routes = [(src, dst) for (src, feature), targets in directions.items() if feature == "history" for dst in targets]
            if not routes:
                continue
            src, dst = routes[0]
            ends = [an._split_prov_token(token) for token in (src, dst)]
            fcfg = (pair.get("features") or {}).get("history")
            fcfg = fcfg if isinstance(fcfg, dict) else {}
            reverse = (dst, src) in routes
            effective = effective_policy(policy, pair_id)
            blocks = manual_policy(effective, ends[0][0], "history", ends[0][1])[1]
            if reverse:
                blocks |= manual_policy(effective, ends[1][0], "history", ends[1][1])[1]
            buckets = [an._bucket(analysis.state, token, "history") for token in (src, dst)]
            reason = "A History snapshot is missing. Refresh this pair before comparing its episode groups." if any(bucket is None for bucket in buckets) else ""
            try:
                if fcfg.get("rewatches") and history_rewatch_pair_enabled(
                    "history", fcfg, ends[0][0], provider_ops(ends[0][0]), ends[1][0], provider_ops(ends[1][0]), bidirectional=reverse,
                ):
                    reason = "Grouped rewatches are not supported. Disable History rewatches for this pair."
                indexes = []
                for token, peer, bucket in zip((src, dst), (dst, src), buckets):
                    rows = {key: item for key, item in (bucket or {}).items() if isinstance(item, dict)
                            and an._passes_pair_type_filter(analysis.pair_types, token, "history", peer, item)
                            and an._passes_pair_lib_filter(analysis.pair_libs, token, "history", peer, item)}
                    indexes.append(collapse_history_latest(filter_history_events(rows, event_mode=False)))
                reasons = {}
                ctx = SimpleNamespace(state_store=SimpleNamespace(base_path=an.CONFIG_DIR,
                    pair_scope=pair_feature_scope(analysis.cfg, pair, "history"), mapping_pair_id=pair_id),
                    emit=lambda _event, **row: reasons.update({row["episode_group"]: row["reason"]}))
                groups = HistoryGroups(ctx, "history", ends, indexes, reverse=reverse, fcfg=fcfg, blocks=blocks, blocked=reason)
            except Exception as exc:
                self.problems.append(dict(type="history_episode_group_invalid", severity="error", pair_id=pair_id,
                    feature="history", source=src, target=dst,
                    message=f"Episode groups could not be evaluated: {exc}. Review Editor → Mappings & blocks → Episode groups."))
                continue
            seen = [{token for item in index.values() if watched(item) for token in tokens(item)} for index in indexes]
            for group in groups.groups:
                sides = {an._prov_token(*endpoint(group[side])): side for side in ("source", "target")}
                counts = {token: [bool(tokens(ep) & seen[ends.index(endpoint(group[side]))]) for ep in group[side]["episodes"]]
                          for token, side in sides.items()}
                summary = " / ".join(f"{token}: {sum(values)}/{len(values)} parts watched" for token, values in counts.items())
                for token, side in sides.items():
                    self.members.setdefault((token, dst if token == src else src), set()).update(
                        member for episode in group[side]["episodes"] for member in tokens(episode))
                reported = False
                for source, target in routes:
                    held = reasons.get(group["id"], "").removeprefix(f"{group['name']}: ")
                    status = "held" if held else "synced" if all(counts[target]) else "pending" if all(counts[source]) else "waiting"
                    message = f"{group['name']} — {summary}. " + (held or {
                        "synced": "The destination has every watched part represented by this group.",
                        "pending": "The source group is complete; the destination is missing grouped watches. Review or run this History pair.",
                        "waiting": "Waiting for all separate parts to be watched before adding the combined episode.",
                    }[status])
                    record = dict(id=group["id"], name=group["name"], pair_id=pair_id, source=source, target=target,
                                  status=status, coverage=counts, message=message)
                    for episode in group[sides[source]]["episodes"]:
                        for member in tokens(episode):
                            self.routes.setdefault((source, target), {})[member] = record
                    if not reported and status in {"waiting", "held"} and (any(any(values) for values in counts.values()) or held):
                        self.problems.append(dict(type=f"history_episode_group_{status}", severity="warn" if held else "info",
                            provider=source, feature="history", pair_id=pair_id, key=f"episode-group:{group['id']}",
                            title=group["name"], target=target, episode_groups=[record], message=message))
                        reported = True

    def match(self, source, target, item):
        lookup = self.routes.get((source, target), {})
        return next((lookup[token] for token in sorted(tokens(item)) if token in lookup), None)

    def contains(self, source, target, item):
        return bool(tokens(item) & self.members.get((source, target), set()))

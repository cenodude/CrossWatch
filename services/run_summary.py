# services/run_summary.py
# CrossWatch - Pair-attributed sync results and scoped run summaries
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping, Sequence
from copy import deepcopy
from typing import Any

from cw_platform.access_policy import pair_refs
from cw_platform.provider_instances import DEFAULT_OVERVIEW_SCOPE, normalize_user_profile_id


FEATURES = ("watchlist", "ratings", "history", "progress", "playlists", "collection")
TERMINAL = {"done", "skipped", "failed", "cancelled"}
TRACKED_EVENTS = {"run:pair", "run:pair:skip", "pair:skip", "run:done", "run:cancelled", "run:aborted", "run:feature", "feature:done",
                  "feature:error", "one:plan", "two:plan", "plan:selected", "ui:spotlight", "debug"} | {
    f"apply:{action}:{phase}" for action in ("add", "remove", "update") for phase in ("start", "progress", "done")
}


def pair_key(pair: Mapping[str, Any]) -> tuple[Any, ...]:
    return (str(pair.get("id") or ""), *pair_refs(pair))


def initialize(summary: dict[str, Any], pairs: Sequence[Mapping[str, Any]]) -> None:
    summary["pair_summaries"] = [
        {"pair": {**{key: pair[key] for key in ("id", "source", "target", "source_instance", "target_instance", "features") if key in pair},
                  "profile_id": normalize_user_profile_id(pair.get("profile_id"))},
         "status": "pending", "features": {}, "_phase": {}}
        for pair in pairs
    ]
    summary["pair_scope_ids"] = sorted({str(pair.get("id") or "") for pair in pairs if pair.get("id")})
    summary["pair_attribution_complete"] = True
    summary.pop("_active_pair_summary", None)


def finish(summary: dict[str, Any], *, failed: bool = False, cancelled: bool = False) -> None:
    for row in summary.get("pair_summaries") or []:
        if row.get("status") not in TERMINAL:
            row["status"] = "cancelled" if cancelled else "failed" if failed or row.get("error") else "done"
    summary.pop("_active_pair_summary", None)


def record_event(summary: dict[str, Any], event: Mapping[str, Any], *, merge: Callable, resolve: Callable) -> dict[str, Any] | None:
    rows = summary.get("pair_summaries")
    if not isinstance(rows, list):
        return None
    name = str(event.get("event") or "")
    if name not in TRACKED_EVENTS or (name == "debug" and event.get("msg") != "apply:add:corrected"):
        return None
    index = summary.get("_active_pair_summary")
    current = rows[index] if isinstance(index, int) and 0 <= index < len(rows) else None
    if name == "run:pair":
        if current is not None and current.get("status") == "running":
            current["status"] = "failed" if current.get("error") else "done"
        pair = {"id": event.get("pair_id"), "source": event.get("src"), "target": event.get("dst"),
                "source_instance": event.get("src_instance"), "target_instance": event.get("dst_instance")}
        index = next((i for i, row in enumerate(rows) if pair_key(row["pair"]) == pair_key(pair)), None)
        summary["_active_pair_summary"] = index
        if index is None:
            summary["pair_attribution_complete"] = False
            return None
        current = rows[index]
        current["status"] = "running"
    elif name in {"run:pair:skip", "pair:skip"}:
        candidates = [row for row in rows if str(row["pair"].get("id") or "") == str(event.get("pair_id") or "")]
        if event.get("reason") == "same_provider_instance":
            candidates = [row for row in candidates if row["status"] == "pending" and pair_refs(row["pair"])[0] == pair_refs(row["pair"])[1]]
        elif current is not None and current in candidates:
            candidates = [current]
        for row in candidates:
            if row["status"] not in TERMINAL:
                row["status"] = "failed" if event.get("reason") in {"auth_failed", "provider_down", "missing_provider_ops"} else "skipped"
        return None
    elif name in {"run:done", "run:cancelled", "run:aborted"}:
        finish(summary, failed=name == "run:aborted", cancelled=bool(event.get("cancelled")) or name == "run:cancelled")
        return None
    if current is None:
        if name in {"ui:spotlight", "apply:add:done", "apply:update:done", "apply:remove:done"}:
            summary["pair_attribution_complete"] = False
        return None
    explicit = str(event.get("pair_id") or "")
    if explicit and explicit != str(current["pair"].get("id") or ""):
        summary["pair_attribution_complete"] = False
        return None
    if name == "feature:error":
        current["error"] = True
    feature = str(event.get("feature") or "").lower()
    if feature not in FEATURES:
        return current
    lane = current["features"].setdefault(feature, {"added": 0, "removed": 0, "updated": 0})
    if name in {"apply:add:done", "apply:update:done", "apply:remove:done", "ui:spotlight"}:
        action = str(event.get("action") or "") if name == "ui:spotlight" else name.split(":")[1]
        if action not in {"add", "update", "remove"}:
            return current
        raw_result = event.get("result")
        result = raw_result if isinstance(raw_result, Mapping) else {}
        if name == "ui:spotlight":
            items = event.get("items")
        else:
            field = {"add": "added", "update": "updated", "remove": "removed"}[action]
            lane[field] += int(result.get("count") or event.get("count") or 0)
            items = event.get("spotlight")
            if not isinstance(items, list) or not items:
                keys = result.get("confirmed_keys") or event.get("confirmed_keys") or []
                items = resolve(keys) if isinstance(keys, list) and keys else []
        if isinstance(items, list):
            merge(lane.setdefault(f"spotlight_{action}", []), items, limit=25)
    elif name == "debug" and event.get("msg") == "apply:add:corrected":
        lane["added"] = max(lane["added"], int(event.get("effective") or 0))
    return current


def scoped_summary(summary: Mapping[str, Any], pairs: Sequence[Mapping[str, Any]], *, scope: str,
                   global_progress: bool, merge: Callable) -> dict[str, Any]:
    allowed = {pair_key(pair) for pair in pairs}
    records = summary.get("pair_summaries")
    available = isinstance(records, list) and summary.get("pair_attribution_complete") is True
    rows = [row for row in records if isinstance(row, Mapping) and pair_key(row.get("pair") or {}) in allowed
            and (scope == DEFAULT_OVERVIEW_SCOPE or normalize_user_profile_id((row.get("pair") or {}).get("profile_id")) == scope)] if available and isinstance(records, list) else []
    features: dict[str, dict[str, Any]] = {feature: {"added": 0, "removed": 0, "updated": 0} for feature in FEATURES}
    for row in rows:
        for feature, lane in (row.get("features") or {}).items():
            if feature not in features or not isinstance(lane, Mapping):
                continue
            target = features[feature]
            for field in ("added", "removed", "updated"):
                target[field] += int(lane.get(field) or 0)
            for field in ("spotlight_add", "spotlight_remove", "spotlight_update"):
                merge(target.setdefault(field, []), deepcopy(lane.get(field) or []), limit=25)
    out: dict[str, Any] = {"features": features, "summary_scope": scope, "details_available": available,
           "run_busy": bool(summary.get("running")), "waiting": False,
           "progress_scoped": not global_progress,
           "pair_scope_ids": sorted({str(row["pair"].get("id") or "") for row in rows}),
           "provider_counts": {}, "current_pair": None, "running": False, "timeline": {}, "_phase": {}}
    if global_progress:
        for key in ("run_id", "running", "started_at", "finished_at", "raw_started_ts", "duration_sec", "exit_code",
                    "cancelled", "cancel_requested", "timeline", "_phase", "current_pair"):
            if key in summary:
                out[key] = deepcopy(summary[key])
    elif rows:
        active = next((row for row in rows if row.get("status") == "running"), None)
        started = any(row.get("status") != "pending" for row in rows)
        done = all(row.get("status") in TERMINAL for row in rows)
        waiting = bool(summary.get("running")) and not done and active is None
        out["waiting"] = waiting
        if started or waiting:
            out["run_id"] = summary.get("run_id")
        out["running"] = (started or waiting) and not done
        out["timeline"] = {"start": started or waiting, "pre": started, "post": started, "done": done}
        if active is not None:
            out["current_pair"] = deepcopy(active.get("current_pair"))
        phases = [(row.get("_phase") or {}).get("apply") or {} for row in rows]
        progress = sum(1.0 if row.get("status") in TERMINAL else float(phase.get("progress") or 0)
                       for row, phase in zip(rows, phases)) / len(rows)
        out["_phase"] = {"apply": {"total": max(len(rows), sum(int(p.get("total") or 0) for p in phases)),
                                  "done": sum(int(p.get("done") or 0) for p in phases), "progress": progress, "final": done}}
        if done:
            out["exit_code"] = 1 if any(row.get("status") == "failed" for row in rows) else 0
            out["cancelled"] = any(row.get("status") == "cancelled" for row in rows)
    elif not available:
        ids = {str(pair.get("id") or "") for pair in pairs}
        run_ids = set(summary.get("pair_scope_ids") or [])
        current = summary.get("current_pair")
        if run_ids and run_ids <= ids and isinstance(current, Mapping) and str(current.get("pair_id") or "") in ids:
            out["current_pair"] = deepcopy(current)
    for field, total in (("added", "added_last"), ("removed", "removed_last"), ("updated", "updated_last")):
        out[total] = sum(lane[field] for lane in features.values())
    return out

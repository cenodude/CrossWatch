# cw_platform/event_archive/stats.py
# CrossWatch - Operational statistics aggregation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import threading
import time
from collections import Counter, defaultdict
from typing import Any, Callable

from .db import get_conn
from .groups import _ensure_correlated, _ensure_activity_hashes, _read_snapshot
from ..reason_labels import friendly_reason

_HOUR = 3600
_DAY = 86400
_CACHE: dict[tuple[Any, ...], tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 30.0
_CACHE_CAP = 48
_LOCK = threading.RLock()
_TYPES = {"sync": "Sync runs", "watching": "Watching", "ratings": "Ratings", "capture": "Captures", "backup": "Backups", "audit": "Audits"}
_OUTCOMES = {"completed": "Completed", "warning": "Needs attention", "failed": "Failed", "active": "In progress", "skipped": "Skipped", "stopped": "Stopped before completion", "informational": "Informational"}
_EVENT_COLUMNS = "id,domain,event_type,created_at,run_id,group_id,severity,reason_code,pair_key,item_key,source_provider,source_instance,destination_provider,destination_instance,origin_provider,origin_instance"


def default_bucket(span: int) -> int:
    return _HOUR if span <= 2 * _DAY else _DAY if span <= 120 * _DAY else 7 * _DAY


def _delta(cur: Any, prev: Any) -> float | None:
    cur, prev = float(cur or 0), float(prev or 0)
    return (None if cur == 0 else 100.0) if prev <= 0 else round((cur - prev) / prev * 100.0, 1)


def _kpi(cur: Any, prev: Any) -> dict[str, Any]:
    return {"value": round(cur, 2) if isinstance(cur, float) else int(cur), "prev": round(prev, 2) if isinstance(prev, float) else int(prev), "delta": _delta(cur, prev)}


def _outcome(status: Any) -> str:
    status = str(status or "").lower()
    if status in ("completed", "rated", "resolved", "done", "success"):
        return "completed"
    if status in ("warning", "unresolved", "blackboxed"):
        return "warning"
    if status in ("running", "pending", "paused"):
        return "active"
    if status in ("failed", "error"):
        return "failed"
    return status if status in _OUTCOMES else "informational"


def _success(rows: list[dict[str, Any]]) -> float | None:
    counts = Counter(row["outcome"] for row in rows if row["type"] != "audit")
    total = sum(counts[key] for key in ("completed", "warning", "failed"))
    return round(counts["completed"] / total * 100.0, 1) if total else None


def _compute(conn: Any, since: int, until: int, bucket: int, row_filter: Callable | None = None) -> dict[str, Any]:
    previous = since - max(1, until - since)
    allowed = row_filter or (lambda row: True)
    events = [dict(row) for row in conn.execute(
        f"SELECT {_EVENT_COLUMNS} FROM events WHERE created_at>=? AND created_at<?", (previous, until),
    ) if allowed(dict(row))]
    groups = [dict(row) for row in conn.execute(
        "SELECT g.*, (SELECT e.source_kind FROM events e WHERE e.group_id=g.id ORDER BY e.id LIMIT 1) source_kind "
        "FROM event_groups g WHERE g.domain IN ('scrobble','capture','backup','audit') AND g.last_event_at>=? AND g.last_event_at<?",
        (previous, until),
    ) if allowed(dict(row))]
    run_ids = {row["run_id"] for row in events if row["run_id"] and row["domain"] == "sync"}
    runs = [dict(row) for row in conn.execute("SELECT * FROM sync_runs WHERE started_at>=? AND started_at<?", (previous, until))
            if row_filter is None or row["run_id"] in run_ids]
    run_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in events:
        if row["run_id"] and row["domain"] == "sync":
            run_events[row["run_id"]].append(row)
    activities = []
    for run in runs:
        types = {row["event_type"] for row in run_events[run["run_id"]]}
        if run["finished_at"] is None:
            outcome = "active"
        elif "write_failed" in types or (row_filter is None and (run["errors"] or run["status"] in ("failed", "error"))):
            outcome = "failed"
        elif types & {"unresolved_recorded", "blackbox_promoted", "blackbox_blocked"} or (row_filter is None and (run["unresolved"] or run["blocked"])):
            outcome = "warning"
        else:
            outcome = _outcome(run["status"])
        activities.append({"type": "sync", "outcome": outcome, "at": run["started_at"]})
    bundles: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for group in groups:
        if group["domain"] == "scrobble":
            bundles[group["activity_hash"] or str(group["id"])].append(group)
        else:
            activities.append({"type": group["domain"], "outcome": _outcome(group["status"]), "at": group["last_event_at"]})
    for rows in bundles.values():
        latest = max(rows, key=lambda row: (row["last_event_at"], row["id"]))
        statuses = {row["status"] for row in rows}
        status = "failed" if "failed" in statuses else "rated" if "rated" in statuses else latest["status"]
        activities.append({
            "type": "ratings" if latest["feature"] == "ratings" else "watching",
            "outcome": _outcome(status), "at": latest["last_event_at"],
            "source": "webhook" if latest["source_kind"] == "webhook" else "watcher",
        })

    def period(a: int, b: int) -> list[dict[str, Any]]:
        return [row for row in activities if a <= row["at"] < b]

    current, old = period(since, until), period(previous, since)
    counts, prev_counts = Counter(row["type"] for row in current), Counter(row["type"] for row in old)
    kpis = {key: _kpi(counts[kind], prev_counts[kind]) for key, kind in (
        ("sync_runs", "sync"), ("scrobbles", "watching"), ("ratings", "ratings"), ("captures", "capture"), ("backups", "backup"), ("audits", "audit"),
    )}
    kpis["activities"] = _kpi(len(current), len(old))
    kpis["failures"] = _kpi(sum(row["outcome"] == "failed" for row in current), sum(row["outcome"] == "failed" for row in old))
    kpis["skipped"] = _kpi(sum(row["outcome"] == "skipped" for row in current), sum(row["outcome"] == "skipped" for row in old))
    kpis["delivery_skips"] = _kpi(*[sum(row["domain"] == "scrobble" and row["status"] == "skipped" and a <= row["last_event_at"] < b for row in groups) for a, b in ((since, until), (previous, since))])
    rate, prev_rate = _success(current), _success(old)
    kpis["success_rate"] = {"value": rate, "prev": prev_rate, "delta": _delta(rate, prev_rate) if rate is not None and prev_rate is not None else None}

    def blocked(a: int, b: int) -> int:
        rows = [row for row in events if a <= row["created_at"] < b and row["event_type"] in ("blackbox_promoted", "blackbox_blocked") and row["pair_key"] and row["run_id"] and row["item_key"]]
        latest = {}
        for row in rows:
            latest[row["pair_key"]] = max(latest.get(row["pair_key"], 0), row["created_at"])
        latest_runs = {(row["pair_key"], row["run_id"]) for row in rows if row["created_at"] == latest[row["pair_key"]]}
        return len({(row["pair_key"], row["item_key"]) for row in rows if (row["pair_key"], row["run_id"]) in latest_runs})

    kpis["blocked"] = _kpi(blocked(since, until), blocked(previous, since))
    durations = [(row["started_at"], row["finished_at"] - row["started_at"]) for row in runs if row["finished_at"] is not None and row["finished_at"] >= row["started_at"]]
    cur_durations = [duration for at, duration in durations if at >= since]
    prev_durations = [duration for at, duration in durations if at < since]
    kpis["avg_duration"] = _kpi(sum(cur_durations) / len(cur_durations) if cur_durations else 0.0, sum(prev_durations) / len(prev_durations) if prev_durations else 0.0)
    ordered = sorted(cur_durations)
    percentiles = {key: ordered[min(len(ordered) - 1, math.floor(p * (len(ordered) - 1)))] if ordered else None for key, p in (("p50", .5), ("p90", .9), ("p99", .99))}
    duration_buckets = defaultdict(list)
    for at, duration in durations:
        if at >= since:
            duration_buckets[at // bucket * bucket].append(duration)
    activity_buckets = defaultdict(list)
    for row in current:
        activity_buckets[row["at"] // bucket * bucket].append(row)
    trend = []
    for at, rows in sorted(activity_buckets.items()):
        trend.append({"t": at, **{kind: sum(row["type"] == kind for row in rows) for kind in _TYPES},
                      **{source: sum(row.get("source") == source for row in rows) for source in ("watcher", "webhook")},
                      "failed": sum(row["outcome"] == "failed" for row in rows), "rate": _success(rows)})
    outcome_counts = Counter(row["outcome"] for row in current)
    routes = defaultdict(list)
    for group in groups:
        if group["domain"] == "scrobble" and group["last_event_at"] >= since and group["destination_provider"]:
            routes[(group["source_provider"], group["destination_provider"])].append({"type": "scrobble", "outcome": _outcome(group["status"])})
    for run in runs:
        if run["started_at"] < since:
            continue
        per_route = defaultdict(set)
        for row in run_events[run["run_id"]]:
            if row["destination_provider"]:
                per_route[(row["source_provider"], row["destination_provider"])].add(row["event_type"])
        for route, types in per_route.items():
            status = "active" if run["finished_at"] is None else "failed" if "write_failed" in types else "warning" if types & {"unresolved_recorded", "blackbox_promoted", "blackbox_blocked"} else "completed"
            routes[route].append({"type": "sync", "outcome": status})
    route_rows = [{"source": src, "destination": dst, "volume": len(rows), "runs": sum(row["type"] == "sync" for row in rows),
                   "scrobbles": sum(row["type"] == "scrobble" for row in rows), "skipped": sum(row["outcome"] == "skipped" for row in rows), "success_rate": _success(rows)}
                  for (src, dst), rows in routes.items()]
    route_rows.sort(key=lambda row: row["volume"], reverse=True)
    reasons = []
    failure_counts = []
    for a, b in ((since, until), (previous, since)):
        unique = {(row["reason_code"], row["group_id"] or row["id"]) for row in events if a <= row["created_at"] < b and row["severity"] == "error" and row["reason_code"]}
        failure_counts.append(Counter(reason for reason, _ in unique))
    total = sum(failure_counts[0].values()) or 1
    for reason, count in failure_counts[0].most_common(8):
        prev = failure_counts[1][reason]
        reasons.append({"reason": reason, "label": friendly_reason(reason), "count": count, "prev": prev, "share": round(count / total * 100, 1), "delta": _delta(count, prev)})
    return {
        "ok": True, "range": {"since": since, "until": until, "bucket": bucket}, "kpis": kpis, "trend": trend,
        "duration_series": [{"t": at, "avg": round(sum(values) / len(values), 2), "n": len(values)} for at, values in sorted(duration_buckets.items())],
        "duration_percentiles": percentiles,
        "outcomes": [{"key": key, "label": label, "value": outcome_counts[key]} for key, label in _OUTCOMES.items() if outcome_counts[key]],
        "types": [{"key": key, "label": label, "value": counts[key]} for key, label in _TYPES.items()],
        "routes": route_rows[:8], "failure_reasons": reasons,
    }


def statistics(*, since: int, until: int, bucket: int | None = None, conn: Any = None, row_filter: Callable | None = None) -> dict[str, Any]:
    until = int(until)
    since = int(min(since, until))
    bs = max(60, int(bucket or default_bucket(until - since)))
    c = conn if conn is not None else get_conn()
    if c is None:
        return {"ok": False, "available": False, "range": {"since": since, "until": until, "bucket": bs}}
    _ensure_correlated(c)
    _ensure_activity_hashes(c)
    now = time.monotonic()
    key = (c, since, until, bs, c.total_changes, c.execute("PRAGMA data_version").fetchone()[0])
    cached = conn is None and row_filter is None
    with _LOCK:
        hit = _CACHE.get(key) if cached else None
        if hit and hit[0] > now:
            return hit[1]
    with _read_snapshot(c):
        payload = _compute(c, since, until, bs, row_filter)
    if cached:
        with _LOCK:
            if len(_CACHE) >= _CACHE_CAP:
                for old in sorted(_CACHE, key=lambda k: _CACHE[k][0])[:_CACHE_CAP // 2]:
                    _CACHE.pop(old, None)
            _CACHE[key] = (now + _CACHE_TTL, payload)
    return payload

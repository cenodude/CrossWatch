# /api/eventsAPI.py
# CrossWatch - Events archive API (SQLite-backed diagnostic history)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import math
from collections import Counter, defaultdict
from typing import Any, cast

from fastapi import APIRouter, Body, Query, Request
from fastapi.responses import JSONResponse

_LOG = logging.getLogger("crosswatch.api.events")

import time as _time

from cw_platform.event_archive import (
    get_conn,
    statistics as _statistics,
    recent as _recent,
    search as _search,
    by_item as _by_item,
    by_run as _by_run,
    status as _status,
    acknowledge as _acknowledge,
    unacknowledge as _unacknowledge,
    build_context as _build_context,
    build_group_context as _build_group_context,
    import_all as _import_all,
    correlate as _correlate,
    list_groups as _list_groups,
    list_tree as _list_tree,
    get_group as _get_group,
    group_events as _group_events,
    acknowledge_group as _acknowledge_group,
    unacknowledge_group as _unacknowledge_group,
    events_db_path,
)
from cw_platform.event_archive.groups import run_problem_items as _run_problem_items
from cw_platform.access_policy import filter_pairs_for_user, pair_refs, request_user, user_can_access_instance
from cw_platform.config_base import load_config
from cw_platform.provider_instances import normalize_instance_id
from cw_platform.reason_labels import friendly_reason

router = APIRouter(prefix="/api/events", tags=["events"])


def _ok(payload: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code)


def _scope_denied() -> JSONResponse:
    return _ok({"ok": False, "error": "profile_scope_denied"}, status_code=403)


def _provider_key(value: Any) -> str:
    return str(value or "").strip().upper()


def _pair_key_for(a: Any, b: Any) -> str:
    vals = sorted(v for v in (_provider_key(a), _provider_key(b)) if v)
    return "-".join(vals) if len(vals) == 2 else ""


def _configured_pairs(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    pairs = cfg.get("pairs")
    return [pair for pair in pairs if isinstance(pair, dict)] if isinstance(pairs, list) else []


def _allowed_pair_keys(cfg: dict[str, Any], user: Any) -> set[str]:
    out: set[str] = set()
    for pair in filter_pairs_for_user(cfg, user, _configured_pairs(cfg)):
        pid = str(pair.get("id") or "").strip()
        if pid:
            out.add(pid)
        (src, _src_inst), (dst, _dst_inst) = pair_refs(pair)
        key = _pair_key_for(src, dst)
        if key:
            out.add(key)
    return out


def _row_pair_allowed(cfg: dict[str, Any], user: Any, row: dict[str, Any]) -> bool:
    allowed = _allowed_pair_keys(cfg, user)
    if not allowed:
        return False
    keys = {str(row.get("pair_key") or "").strip().upper(), str(row.get("pair_id") or "").strip()}
    keys.add(_pair_key_for(row.get("source_provider"), row.get("destination_provider")))
    keys.add(_pair_key_for(row.get("origin_provider"), row.get("destination_provider")))
    return any(key and key in allowed for key in keys)


def _event_refs(row: dict[str, Any]) -> list[tuple[Any, Any]]:
    refs: list[tuple[Any, Any]] = []
    for prefix in ("source", "destination", "origin"):
        provider = row.get(f"{prefix}_provider")
        instance = row.get(f"{prefix}_instance")
        if provider:
            refs.append((provider, normalize_instance_id(instance or "default")))
    if not refs:
        provider = row.get("provider")
        instance = row.get("instance")
        if provider:
            refs.append((provider, normalize_instance_id(instance or "default")))
    return refs


def _has_explicit_instance(row: dict[str, Any]) -> bool:
    for key in ("source_instance", "destination_instance", "origin_instance", "instance"):
        if str(row.get(key) or "").strip():
            return True
    return False


def _group_has_allowed_event(cfg: dict[str, Any], request: Request | None, group_id: Any) -> bool:
    try:
        gid = int(group_id)
    except Exception:
        return False
    rows = _group_events(gid, order="asc", limit=1000, offset=0).get("items") or []
    return any(isinstance(row, dict) and _event_row_allowed(cfg, request, row, False) for row in rows)


def _event_row_allowed(cfg: dict[str, Any], request: Request | None, row: dict[str, Any] | None, allow_group_children: bool = True) -> bool:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return True
    if not isinstance(row, dict):
        return False
    refs = _event_refs(row)
    if refs and all(user_can_access_instance(cfg, user, provider, instance) for provider, instance in refs):
        return True
    if refs and _has_explicit_instance(row):
        return False
    if _row_pair_allowed(cfg, user, row):
        return True
    if allow_group_children and row.get("id") is not None:
        return _group_has_allowed_event(cfg, request, row.get("id"))
    return False


def _filter_event_payload(cfg: dict[str, Any], request: Request | None, payload: dict[str, Any]) -> dict[str, Any]:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return payload
    out = dict(payload or {})
    for key in ("items", "events", "groups", "children"):
        rows = out.get(key)
        if isinstance(rows, list):
            out[key] = [row for row in rows if isinstance(row, dict) and _event_row_allowed(cfg, request, row)]
            if key == "items":
                out["total"] = len(out[key])
    return out


def _managed_request(request: Request | None) -> bool:
    user = request_user(request)
    return bool(user and not bool(user.get("is_admin")))


def _audit_domain_requested(domain: Any) -> bool:
    return str(domain or "").strip().lower() == "audit"


@router.get("/status")
def events_status() -> JSONResponse:
    return _ok(_status())


_RANGE_SECONDS = {"24h": 86400, "7d": 7 * 86400, "30d": 30 * 86400, "90d": 90 * 86400}
_BUCKETS = {"hour": 3600, "day": 86400, "week": 7 * 86400}
_OUTCOME_LABEL = {"completed": "Completed", "warning": "Warning", "failed": "Failed", "active": "In progress"}
_PROBLEM_EVENTS = {"write_failed", "unresolved_recorded"}


def _delta(cur: Any, prev: Any) -> float | None:
    cur_f = float(cur or 0)
    prev_f = float(prev or 0)
    if prev_f <= 0:
        return None if cur_f == 0 else 100.0
    return round((cur_f - prev_f) / prev_f * 100.0, 1)


def _kpi(cur: Any, prev: Any) -> dict[str, Any]:
    value = round(float(cur or 0), 2) if isinstance(cur, float) else int(cur or 0)
    old = round(float(prev or 0), 2) if isinstance(prev, float) else int(prev or 0)
    return {"value": value, "prev": old, "delta": _delta(cur, prev)}


def _rate(ok: Any, total: Any) -> float:
    ok_f = float(ok or 0)
    total_f = float(total or 0)
    return round(ok_f / total_f * 100.0, 1) if total_f > 0 else 0.0


def _duration(row: dict[str, Any]) -> int | None:
    try:
        started = int(row.get("started_at") or 0)
        finished = int(row.get("finished_at") or 0)
    except Exception:
        return None
    return finished - started if started and finished >= started else None


def _percentile(values: list[int], p: float) -> int | None:
    if not values:
        return None
    vals = sorted(values)
    idx = min(len(vals) - 1, int(math.floor(float(p) * (len(vals) - 1))))
    return int(vals[idx])


def _rows_between(conn: Any, table: str, column: str, since: int, until: int) -> list[dict[str, Any]]:
    try:
        rows = conn.execute(f"SELECT * FROM {table} WHERE {column}>=? AND {column}<?", (since, until)).fetchall()
    except Exception:
        return []
    return [dict(row) for row in rows]


def _scoped_event_rows(cfg: dict[str, Any], request: Request | None, conn: Any, since: int, until: int) -> list[dict[str, Any]]:
    return [row for row in _rows_between(conn, "events", "created_at", since, until) if _event_row_allowed(cfg, request, row, False)]


def _scoped_group_rows(cfg: dict[str, Any], request: Request | None, conn: Any, since: int, until: int) -> list[dict[str, Any]]:
    return [row for row in _rows_between(conn, "event_groups", "last_event_at", since, until) if _event_row_allowed(cfg, request, row)]


def _scoped_run_rows(conn: Any, since: int, until: int, allowed_run_ids: set[str]) -> list[dict[str, Any]]:
    if not allowed_run_ids:
        return []
    return [row for row in _rows_between(conn, "sync_runs", "started_at", since, until) if str(row.get("run_id") or "") in allowed_run_ids]


def _default_bucket(span: int) -> int:
    if span <= 2 * 86400:
        return 3600
    if span <= 120 * 86400:
        return 86400
    return 7 * 86400


def _scoped_statistics(cfg: dict[str, Any], request: Request | None, since: int, until: int, bucket: int | None) -> dict[str, Any]:
    conn = get_conn()
    bs = int(bucket or _default_bucket(until - since))
    if conn is None:
        return {"ok": False, "available": False, "range": {"since": since, "until": until, "bucket": bs}}
    span = max(1, until - since)
    prev_since = since - span
    all_events = _scoped_event_rows(cfg, request, conn, prev_since, until)
    all_groups = _scoped_group_rows(cfg, request, conn, prev_since, until)
    allowed_run_ids = {str(row.get("run_id") or "") for row in all_events if str(row.get("run_id") or "")}
    all_runs = _scoped_run_rows(conn, prev_since, until, allowed_run_ids)

    cur_events = [row for row in all_events if int(row.get("created_at") or 0) >= since]
    prev_events = [row for row in all_events if int(row.get("created_at") or 0) < since]
    cur_groups = [row for row in all_groups if int(row.get("last_event_at") or 0) >= since]
    prev_groups = [row for row in all_groups if int(row.get("last_event_at") or 0) < since]
    cur_runs = [row for row in all_runs if int(row.get("started_at") or 0) >= since]
    prev_runs = [row for row in all_runs if int(row.get("started_at") or 0) < since]

    def failed_run_ids(rows: list[dict[str, Any]], events: list[dict[str, Any]]) -> set[str]:
        ids = {str(row.get("run_id") or "") for row in rows if int(row.get("errors") or 0) > 0 and str(row.get("run_id") or "")}
        ids.update(str(row.get("run_id") or "") for row in events if row.get("event_type") in _PROBLEM_EVENTS and str(row.get("run_id") or ""))
        return ids

    cur_failed_runs = failed_run_ids(cur_runs, cur_events)
    prev_failed_runs = failed_run_ids(prev_runs, prev_events)
    cur_warning_runs = {str(row.get("run_id") or "") for row in cur_events if row.get("event_type") in _PROBLEM_EVENTS and str(row.get("run_id") or "")} - cur_failed_runs
    prev_warning_runs = {str(row.get("run_id") or "") for row in prev_events if row.get("event_type") in _PROBLEM_EVENTS and str(row.get("run_id") or "")} - prev_failed_runs
    cur_run_ids = {str(row.get("run_id") or "") for row in cur_runs if str(row.get("run_id") or "")}
    prev_run_ids = {str(row.get("run_id") or "") for row in prev_runs if str(row.get("run_id") or "")}

    cur_domain_ok = sum(1 for row in cur_events if row.get("event_type") in ("scrobble_completed", "rating_applied"))
    prev_domain_ok = sum(1 for row in prev_events if row.get("event_type") in ("scrobble_completed", "rating_applied"))
    cur_domain_fail = sum(1 for row in cur_events if row.get("event_type") in ("scrobble_failed", "rating_failed"))
    prev_domain_fail = sum(1 for row in prev_events if row.get("event_type") in ("scrobble_failed", "rating_failed"))
    cur_ok_runs = max(0, len(cur_run_ids) - len(cur_failed_runs) - len(cur_warning_runs))
    prev_ok_runs = max(0, len(prev_run_ids) - len(prev_failed_runs) - len(prev_warning_runs))
    cur_ops = len(cur_run_ids) + cur_domain_ok + cur_domain_fail
    prev_ops = len(prev_run_ids) + prev_domain_ok + prev_domain_fail
    cur_ok_ops = cur_ok_runs + cur_domain_ok
    prev_ok_ops = prev_ok_runs + prev_domain_ok

    cur_durations = [dur for dur in (_duration(row) for row in cur_runs) if dur is not None]
    prev_durations = [dur for dur in (_duration(row) for row in prev_runs) if dur is not None]
    cur_avg = round(sum(cur_durations) / len(cur_durations), 2) if cur_durations else 0.0
    prev_avg = round(sum(prev_durations) / len(prev_durations), 2) if prev_durations else 0.0
    blocked_cur = len({(row.get("pair_key"), row.get("run_id"), row.get("item_key")) for row in cur_events if row.get("event_type") in ("blackbox_promoted", "blackbox_blocked") and row.get("pair_key") and row.get("run_id") and row.get("item_key")})
    blocked_prev = len({(row.get("pair_key"), row.get("run_id"), row.get("item_key")) for row in prev_events if row.get("event_type") in ("blackbox_promoted", "blackbox_blocked") and row.get("pair_key") and row.get("run_id") and row.get("item_key")})

    kpis = {
        "sync_runs": _kpi(len(cur_run_ids), len(prev_run_ids)),
        "scrobbles": _kpi(sum(1 for row in cur_events if row.get("event_type") == "scrobble_completed"), sum(1 for row in prev_events if row.get("event_type") == "scrobble_completed")),
        "avg_duration": _kpi(cur_avg, prev_avg),
        "failures": _kpi(len(cur_failed_runs) + len(cur_warning_runs) + cur_domain_fail, len(prev_failed_runs) + len(prev_warning_runs) + prev_domain_fail),
        "blocked": _kpi(blocked_cur, blocked_prev),
        "success_rate": _kpi(_rate(cur_ok_ops, cur_ops), _rate(prev_ok_ops, prev_ops)),
    }

    trend_map: dict[int, dict[str, Any]] = {}
    for row in cur_events:
        try:
            b = int(row.get("created_at") or 0) // bs
        except Exception:
            continue
        item = trend_map.setdefault(b, {"t": b * bs, "sync": 0, "webhook": 0, "watcher": 0, "failed": 0, "_ok": 0})
        et = str(row.get("event_type") or "")
        if et == "sync_run_finished":
            item["sync"] += 1
            item["_ok"] += 1
        elif et in ("scrobble_completed", "rating_applied"):
            key = "webhook" if str(row.get("source_kind") or "") == "webhook" else "watcher"
            item[key] += 1
            item["_ok"] += 1
        elif et in ("scrobble_failed", "rating_failed"):
            item["failed"] += 1
    for row in cur_runs:
        dur = _duration(row)
        if dur is None:
            continue
        b = int(row.get("started_at") or 0) // bs
        item = trend_map.setdefault(b, {"t": b * bs, "sync": 0, "webhook": 0, "watcher": 0, "failed": 0, "_ok": 0})
        if int(row.get("errors") or 0) > 0:
            item["failed"] += 1
    trend = []
    for b in sorted(trend_map):
        item = trend_map[b]
        ok = int(item.pop("_ok", 0))
        failed = int(item.get("failed") or 0)
        item["rate"] = round(ok / (ok + failed) * 100.0, 1) if (ok + failed) > 0 else None
        trend.append(item)

    dur_buckets: dict[int, list[int]] = defaultdict(list)
    for row in cur_runs:
        dur = _duration(row)
        if dur is not None:
            dur_buckets[int(row.get("started_at") or 0) // bs].append(dur)
    duration_series = [{"t": b * bs, "avg": round(sum(vals) / len(vals), 2), "n": len(vals)} for b, vals in sorted(dur_buckets.items())]

    scrobble_status = Counter(str(row.get("status") or "").lower() for row in cur_groups if str(row.get("domain") or "") == "scrobble")
    outcomes_count = {"completed": cur_ok_runs, "warning": len(cur_warning_runs), "failed": len(cur_failed_runs), "active": 0}
    outcomes_count["completed"] += scrobble_status.get("completed", 0) + scrobble_status.get("rated", 0)
    outcomes_count["warning"] += scrobble_status.get("warning", 0)
    outcomes_count["failed"] += scrobble_status.get("failed", 0)
    outcomes_count["active"] += scrobble_status.get("running", 0)
    outcomes = [{"key": key, "label": _OUTCOME_LABEL[key], "value": outcomes_count[key]} for key in ("completed", "warning", "failed", "active") if outcomes_count.get(key)]

    watcher_count = sum(1 for row in cur_events if row.get("event_type") in ("scrobble_completed", "rating_applied") and str(row.get("source_kind") or "") != "webhook" and str(row.get("domain") or "") == "scrobble")
    webhook_count = sum(1 for row in cur_events if row.get("event_type") in ("scrobble_completed", "rating_applied") and str(row.get("source_kind") or "") == "webhook")
    types = [
        {"key": "sync", "label": "Sync runs", "value": len(cur_run_ids)},
        {"key": "watcher", "label": "Watcher", "value": watcher_count},
        {"key": "webhook", "label": "Webhooks", "value": webhook_count},
    ]

    route_map: dict[tuple[Any, Any], dict[str, int]] = defaultdict(lambda: {"runs": 0, "scrobbles": 0, "ok": 0, "bad": 0})
    for row in cur_events:
        src = row.get("source_provider")
        dst = row.get("destination_provider")
        if not dst:
            continue
        entry = route_map[(src, dst)]
        if row.get("run_id"):
            entry["runs"] += 1 if row.get("event_type") == "sync_run_finished" else 0
            entry["bad"] += 1 if row.get("event_type") in _PROBLEM_EVENTS else 0
        elif row.get("event_type") in ("scrobble_completed", "rating_applied", "scrobble_failed", "rating_failed"):
            entry["scrobbles"] += 1
            entry["ok"] += 1 if row.get("event_type") in ("scrobble_completed", "rating_applied") else 0
            entry["bad"] += 1 if row.get("event_type") in ("scrobble_failed", "rating_failed") else 0
    routes = []
    for (src, dst), values in route_map.items():
        volume = values["runs"] + values["scrobbles"]
        if volume <= 0:
            continue
        ok = values["ok"] + max(0, values["runs"] - values["bad"])
        bad = values["bad"]
        routes.append({"source": src, "destination": dst, "volume": volume, "runs": values["runs"], "scrobbles": values["scrobbles"], "success_rate": round(ok / (ok + bad) * 100.0, 1) if (ok + bad) else None})
    routes.sort(key=lambda row: row["volume"], reverse=True)

    cur_fail_map = Counter(str(row.get("reason_code") or "") for row in cur_events if str(row.get("severity") or "") == "error" and str(row.get("reason_code") or ""))
    prev_fail_map = Counter(str(row.get("reason_code") or "") for row in prev_events if str(row.get("severity") or "") == "error" and str(row.get("reason_code") or ""))
    total_fail = sum(cur_fail_map.values()) or 1
    failure_reasons = [{
        "reason": key,
        "label": friendly_reason(key),
        "count": value,
        "prev": prev_fail_map.get(key, 0),
        "share": round(value / total_fail * 100.0, 1),
        "delta": _delta(value, prev_fail_map.get(key, 0)),
    } for key, value in cur_fail_map.most_common(8)]

    return {
        "ok": True,
        "range": {"since": since, "until": until, "bucket": bs},
        "kpis": kpis,
        "trend": trend,
        "duration_series": duration_series,
        "duration_percentiles": {"p50": _percentile(cur_durations, 0.5), "p90": _percentile(cur_durations, 0.9), "p99": _percentile(cur_durations, 0.99)},
        "outcomes": outcomes,
        "types": types,
        "routes": routes[:8],
        "failure_reasons": failure_reasons,
    }


@router.get("/statistics")
def events_statistics(
    range: str = Query("30d"),
    since: int | None = Query(None, ge=0),
    until: int | None = Query(None, ge=0),
    bucket: str | None = Query(None),
    request: Request = cast(Request, None),
) -> JSONResponse:
    now = int(_time.time())
    if since is not None:
        s, u = int(since), int(until or now)
    else:
        span = _RANGE_SECONDS.get(str(range).lower(), _RANGE_SECONDS["30d"])
        s, u = now - span, now
    bs = _BUCKETS.get(str(bucket or "").lower()) if bucket else None
    if _managed_request(request):
        return _ok(_scoped_statistics(load_config() or {}, request, s, u, bs))
    return _ok(_statistics(since=s, until=u, bucket=bs))


@router.get("/recent")
def events_recent(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    visibility: str = Query("open"),
    order: str = Query("newest"),
    view: str = Query("groups"),
    domain: str = Query("sync"),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if _managed_request(request) and _audit_domain_requested(domain):
        return _scope_denied()
    if str(view).lower() == "events":
        return _ok(_filter_event_payload(cfg, request, _recent(limit=limit, offset=offset, visibility=visibility, order=order, domain=domain)))
    return _ok(_filter_event_payload(cfg, request, _list_groups(visibility=visibility, order=order, limit=limit, offset=offset, domain=domain)))


@router.get("/search")
def events_search(
    q: str | None = None,
    event_type: str | None = None,
    provider: str | None = None,
    origin_provider: str | None = None,
    destination_provider: str | None = None,
    source_provider: str | None = None,
    feature: str | None = None,
    pair_key: str | None = None,
    item_key: str | None = None,
    run_id: str | None = None,
    reason_code: str | None = None,
    status: str | None = None,
    category: str | None = None,
    since: int | None = None,
    until: int | None = None,
    visibility: str = Query("open"),
    order: str = Query("newest"),
    view: str = Query("groups"),
    domain: str = Query("sync"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if _managed_request(request) and _audit_domain_requested(domain):
        return _scope_denied()
    if str(view).lower() == "events":
        return _ok(_filter_event_payload(cfg, request, _search(
            q=q, event_type=event_type, provider=provider, origin_provider=origin_provider,
            destination_provider=destination_provider, source_provider=source_provider,
            feature=feature, pair_key=pair_key, run_id=run_id, reason_code=reason_code,
            since=since, until=until, visibility=visibility, order=order, domain=domain, limit=limit, offset=offset,
        )))
    return _ok(_filter_event_payload(cfg, request, _list_groups(
        q=q, status=status, category=category, event_type=event_type, provider=provider, origin_provider=origin_provider,
        destination_provider=destination_provider, source_provider=source_provider,
        feature=feature, pair_key=pair_key, item_key=item_key, run_id=run_id, reason_code=reason_code,
        since=since, until=until, visibility=visibility, order=order, domain=domain, limit=limit, offset=offset,
    )))


@router.get("/groups")
def events_groups(
    q: str | None = None,
    event_type: str | None = None,
    provider: str | None = None,
    origin_provider: str | None = None,
    destination_provider: str | None = None,
    source_provider: str | None = None,
    feature: str | None = None,
    pair_key: str | None = None,
    item_key: str | None = None,
    run_id: str | None = None,
    reason_code: str | None = None,
    status: str | None = None,
    category: str | None = None,
    since: int | None = None,
    until: int | None = None,
    visibility: str = Query("open"),
    order: str = Query("newest"),
    domain: str = Query("sync"),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if _managed_request(request) and _audit_domain_requested(domain):
        return _scope_denied()
    return _ok(_filter_event_payload(cfg, request, _list_groups(
        q=q, status=status, category=category, event_type=event_type, provider=provider, origin_provider=origin_provider,
        destination_provider=destination_provider, source_provider=source_provider,
        feature=feature, pair_key=pair_key, item_key=item_key, run_id=run_id, reason_code=reason_code,
        since=since, until=until, visibility=visibility, order=order, domain=domain, limit=limit, offset=offset,
    )))


@router.get("/tree")
def events_tree(
    q: str | None = None,
    event_type: str | None = None,
    provider: str | None = None,
    origin_provider: str | None = None,
    destination_provider: str | None = None,
    source_provider: str | None = None,
    feature: str | None = None,
    pair_key: str | None = None,
    item_key: str | None = None,
    run_id: str | None = None,
    reason_code: str | None = None,
    status: str | None = None,
    category: str | None = None,
    since: int | None = None,
    until: int | None = None,
    visibility: str = Query("open"),
    order: str = Query("newest"),
    domain: str = Query("sync"),
    children: bool = Query(True),
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if _managed_request(request) and _audit_domain_requested(domain):
        return _scope_denied()
    return _ok(_filter_event_payload(cfg, request, _list_tree(
        order=order, limit=limit, offset=offset, include_children=children, domain=domain,
        q=q, status=status, category=category, event_type=event_type, provider=provider, origin_provider=origin_provider,
        destination_provider=destination_provider, source_provider=source_provider,
        feature=feature, pair_key=pair_key, item_key=item_key, run_id=run_id, reason_code=reason_code,
        since=since, until=until, visibility=visibility,
    )))


@router.get("/groups/{group_id}")
def events_group_detail(
    group_id: int,
    run_items_limit: int = Query(100, ge=1, le=500),
    run_items_offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if not _event_row_allowed(cfg, request, _get_group(group_id)):
        return _scope_denied()
    res = _build_group_context(group_id, run_items_limit=run_items_limit, run_items_offset=run_items_offset)
    return _ok(_filter_event_payload(cfg, request, res), status_code=200 if res.get("ok") else 404)


@router.get("/groups/{group_id}/run-items")
def events_group_run_items(
    group_id: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    group = _get_group(group_id)
    if not group:
        return _ok({"ok": False}, status_code=404)
    if not _event_row_allowed(cfg, request, group):
        return _scope_denied()
    if str(group.get("operation") or "") != "run":
        return _ok({"ok": True, "items": [], "total": 0, "limit": limit, "offset": offset})
    events = _group_events(group_id, order="asc", limit=500, offset=0).get("items") or []
    run_id = next((e.get("run_id") for e in events if e.get("run_id")), None)
    res = _run_problem_items(run_id, group.get("first_event_at"), group.get("last_event_at"), limit=limit, offset=offset)
    return _ok(_filter_event_payload(cfg, request, {
        "ok": True,
        "items": res.get("items") or [],
        "total": res.get("total") or 0,
        "limit": res.get("limit") or limit,
        "offset": res.get("offset") or offset,
    }))


@router.get("/groups/{group_id}/events")
def events_group_events(
    group_id: int,
    order: str = Query("asc"),
    limit: int = Query(500, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    if not _event_row_allowed(cfg, request, _get_group(group_id)):
        return _scope_denied()
    return _ok(_filter_event_payload(cfg, request, _group_events(group_id, order=order, limit=limit, offset=offset)))


@router.post("/groups/{group_id}/acknowledge")
def events_group_acknowledge(group_id: int, payload: dict[str, Any] | None = Body(None), request: Request = cast(Request, None)) -> JSONResponse:
    if not _event_row_allowed(load_config() or {}, request, _get_group(group_id)):
        return _scope_denied()
    by = str((payload or {}).get("by") or "").strip() or None
    return _ok(_acknowledge_group(group_id, by=by))


@router.post("/groups/{group_id}/unacknowledge")
def events_group_unacknowledge(group_id: int, request: Request = cast(Request, None)) -> JSONResponse:
    if not _event_row_allowed(load_config() or {}, request, _get_group(group_id)):
        return _scope_denied()
    return _ok(_unacknowledge_group(group_id))


@router.post("/correlate")
def events_correlate(payload: dict[str, Any] | None = Body(None), request: Request = cast(Request, None)) -> JSONResponse:
    if _managed_request(request):
        return _scope_denied()
    return _ok(_correlate(reset=bool((payload or {}).get("reset"))))


@router.get("/item/{item_key:path}")
def events_item(
    item_key: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    visibility: str = Query("all"),
    request: Request = cast(Request, None),
) -> JSONResponse:
    return _ok(_filter_event_payload(load_config() or {}, request, _by_item(item_key, limit=limit, offset=offset, visibility=visibility)))


@router.get("/run/{run_id}")
def events_run(
    run_id: str,
    limit: int = Query(500, ge=1, le=500),
    offset: int = Query(0, ge=0),
    visibility: str = Query("all"),
    request: Request = cast(Request, None),
) -> JSONResponse:
    return _ok(_filter_event_payload(load_config() or {}, request, _by_run(run_id, limit=limit, offset=offset, visibility=visibility)))


@router.post("/{event_id}/acknowledge")
def events_acknowledge(event_id: int, payload: dict[str, Any] | None = Body(None), request: Request = cast(Request, None)) -> JSONResponse:
    if _managed_request(request):
        return _scope_denied()
    by = str((payload or {}).get("by") or "").strip() or None
    return _ok(_acknowledge(event_id, by=by))


@router.post("/{event_id}/unacknowledge")
def events_unacknowledge(event_id: int, request: Request = cast(Request, None)) -> JSONResponse:
    if _managed_request(request):
        return _scope_denied()
    return _ok(_unacknowledge(event_id))


@router.get("/context")
def events_context(
    event_id: int | None = None,
    item_key: str | None = None,
    provider: str | None = None,
    feature: str | None = None,
    pair_key: str | None = None,
    run_id: str | None = None,
    source_provider: str | None = None,
    destination_provider: str | None = None,
    origin_provider: str | None = None,
    request: Request = cast(Request, None),
) -> JSONResponse:
    cfg = load_config() or {}
    res = _build_context(
        event_id=event_id, item_key=item_key, provider=provider, feature=feature,
        pair_key=pair_key, run_id=run_id, source_provider=source_provider,
        destination_provider=destination_provider, origin_provider=origin_provider,
    )
    if event_id not in (None, "") and not _event_row_allowed(cfg, request, res.get("event")):
        return _scope_denied()
    return _ok(_filter_event_payload(cfg, request, res))


@router.post("/import")
def events_import(request: Request = cast(Request, None)) -> JSONResponse:
    if _managed_request(request):
        return _scope_denied()
    return _ok(_import_all())


@router.post("/clear")
def events_clear(payload: dict[str, Any] | None = Body(None), request: Request = cast(Request, None)) -> JSONResponse:
    if _managed_request(request):
        return _scope_denied()
    if not bool((payload or {}).get("confirm")):
        return _ok({"ok": False, "error": "confirmation_required", "confirm": False}, status_code=400)
    conn = get_conn()
    if conn is None:
        return _ok({"ok": False, "available": False, "path": str(events_db_path())})
    domain = str((payload or {}).get("domain") or "").strip().lower()
    try:
        with conn:
            if domain in ("", "all"):
                conn.execute("DELETE FROM events")
                conn.execute("DELETE FROM event_groups")
                conn.execute("DELETE FROM sync_runs")
                conn.execute("DELETE FROM event_imports")
            else:
                conn.execute("DELETE FROM events WHERE domain=?", (domain,))
                conn.execute("DELETE FROM event_groups WHERE domain=?", (domain,))
                if domain == "sync":
                    conn.execute("DELETE FROM sync_runs")
                    conn.execute("DELETE FROM event_imports")
        return _ok({"ok": True, "cleared": True, "domain": domain or "all"})
    except Exception:
        _LOG.exception("events clear failed")
        return _ok({"ok": False, "error": "internal_error"}, status_code=500)

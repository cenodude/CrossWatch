# /api/eventsAPI.py
# CrossWatch - Events archive API (SQLite-backed diagnostic history)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, Body, Query
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

router = APIRouter(prefix="/api/events", tags=["events"])


def _ok(payload: dict[str, Any], status_code: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status_code)


@router.get("/status")
def events_status() -> JSONResponse:
    return _ok(_status())


_RANGE_SECONDS = {"24h": 86400, "7d": 7 * 86400, "30d": 30 * 86400, "90d": 90 * 86400}
_BUCKETS = {"hour": 3600, "day": 86400, "week": 7 * 86400}


@router.get("/statistics")
def events_statistics(
    range: str = Query("30d"),
    since: int | None = Query(None, ge=0),
    until: int | None = Query(None, ge=0),
    bucket: str | None = Query(None),
) -> JSONResponse:
    now = int(_time.time())
    if since is not None:
        s, u = int(since), int(until or now)
    else:
        span = _RANGE_SECONDS.get(str(range).lower(), _RANGE_SECONDS["30d"])
        s, u = now - span, now
    bs = _BUCKETS.get(str(bucket or "").lower()) if bucket else None
    return _ok(_statistics(since=s, until=u, bucket=bs))


@router.get("/recent")
def events_recent(
    limit: int = Query(50, ge=1, le=500),
    offset: int = Query(0, ge=0),
    visibility: str = Query("open"),
    order: str = Query("newest"),
    view: str = Query("groups"),
    domain: str = Query("sync"),
) -> JSONResponse:
    if str(view).lower() == "events":
        return _ok(_recent(limit=limit, offset=offset, visibility=visibility, order=order, domain=domain))
    return _ok(_list_groups(visibility=visibility, order=order, limit=limit, offset=offset, domain=domain))


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
) -> JSONResponse:
    if str(view).lower() == "events":
        return _ok(_search(
            q=q, event_type=event_type, provider=provider, origin_provider=origin_provider,
            destination_provider=destination_provider, source_provider=source_provider,
            feature=feature, pair_key=pair_key, run_id=run_id, reason_code=reason_code,
            since=since, until=until, visibility=visibility, order=order, domain=domain, limit=limit, offset=offset,
        ))
    return _ok(_list_groups(
        q=q, status=status, category=category, event_type=event_type, provider=provider, origin_provider=origin_provider,
        destination_provider=destination_provider, source_provider=source_provider,
        feature=feature, pair_key=pair_key, item_key=item_key, run_id=run_id, reason_code=reason_code,
        since=since, until=until, visibility=visibility, order=order, domain=domain, limit=limit, offset=offset,
    ))


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
) -> JSONResponse:
    return _ok(_list_groups(
        q=q, status=status, category=category, event_type=event_type, provider=provider, origin_provider=origin_provider,
        destination_provider=destination_provider, source_provider=source_provider,
        feature=feature, pair_key=pair_key, item_key=item_key, run_id=run_id, reason_code=reason_code,
        since=since, until=until, visibility=visibility, order=order, domain=domain, limit=limit, offset=offset,
    ))


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
) -> JSONResponse:
    return _ok(_list_tree(
        order=order, limit=limit, offset=offset, include_children=children, domain=domain,
        q=q, status=status, category=category, event_type=event_type, provider=provider, origin_provider=origin_provider,
        destination_provider=destination_provider, source_provider=source_provider,
        feature=feature, pair_key=pair_key, item_key=item_key, run_id=run_id, reason_code=reason_code,
        since=since, until=until, visibility=visibility,
    ))


@router.get("/groups/{group_id}")
def events_group_detail(
    group_id: int,
    run_items_limit: int = Query(100, ge=1, le=500),
    run_items_offset: int = Query(0, ge=0),
) -> JSONResponse:
    res = _build_group_context(group_id, run_items_limit=run_items_limit, run_items_offset=run_items_offset)
    return _ok(res, status_code=200 if res.get("ok") else 404)


@router.get("/groups/{group_id}/run-items")
def events_group_run_items(
    group_id: int,
    limit: int = Query(100, ge=1, le=500),
    offset: int = Query(0, ge=0),
) -> JSONResponse:
    group = _get_group(group_id)
    if not group:
        return _ok({"ok": False}, status_code=404)
    if str(group.get("operation") or "") != "run":
        return _ok({"ok": True, "items": [], "total": 0, "limit": limit, "offset": offset})
    events = _group_events(group_id, order="asc", limit=500, offset=0).get("items") or []
    run_id = next((e.get("run_id") for e in events if e.get("run_id")), None)
    res = _run_problem_items(run_id, group.get("first_event_at"), group.get("last_event_at"), limit=limit, offset=offset)
    return _ok({
        "ok": True,
        "items": res.get("items") or [],
        "total": res.get("total") or 0,
        "limit": res.get("limit") or limit,
        "offset": res.get("offset") or offset,
    })


@router.get("/groups/{group_id}/events")
def events_group_events(
    group_id: int,
    order: str = Query("asc"),
    limit: int = Query(500, ge=1, le=1000),
    offset: int = Query(0, ge=0),
) -> JSONResponse:
    return _ok(_group_events(group_id, order=order, limit=limit, offset=offset))


@router.post("/groups/{group_id}/acknowledge")
def events_group_acknowledge(group_id: int, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    by = str((payload or {}).get("by") or "").strip() or None
    return _ok(_acknowledge_group(group_id, by=by))


@router.post("/groups/{group_id}/unacknowledge")
def events_group_unacknowledge(group_id: int) -> JSONResponse:
    return _ok(_unacknowledge_group(group_id))


@router.post("/correlate")
def events_correlate(payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    return _ok(_correlate(reset=bool((payload or {}).get("reset"))))


@router.get("/item/{item_key:path}")
def events_item(
    item_key: str,
    limit: int = Query(200, ge=1, le=500),
    offset: int = Query(0, ge=0),
    visibility: str = Query("all"),
) -> JSONResponse:
    return _ok(_by_item(item_key, limit=limit, offset=offset, visibility=visibility))


@router.get("/run/{run_id}")
def events_run(
    run_id: str,
    limit: int = Query(500, ge=1, le=500),
    offset: int = Query(0, ge=0),
    visibility: str = Query("all"),
) -> JSONResponse:
    return _ok(_by_run(run_id, limit=limit, offset=offset, visibility=visibility))


@router.post("/{event_id}/acknowledge")
def events_acknowledge(event_id: int, payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
    by = str((payload or {}).get("by") or "").strip() or None
    return _ok(_acknowledge(event_id, by=by))


@router.post("/{event_id}/unacknowledge")
def events_unacknowledge(event_id: int) -> JSONResponse:
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
) -> JSONResponse:
    return _ok(_build_context(
        event_id=event_id, item_key=item_key, provider=provider, feature=feature,
        pair_key=pair_key, run_id=run_id, source_provider=source_provider,
        destination_provider=destination_provider, origin_provider=origin_provider,
    ))


@router.post("/import")
def events_import() -> JSONResponse:
    return _ok(_import_all())


@router.post("/clear")
def events_clear(payload: dict[str, Any] | None = Body(None)) -> JSONResponse:
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

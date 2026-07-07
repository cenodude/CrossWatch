# cw_platform/event_archive/groups.py
# CrossWatch - Event correlation into user-facing threads
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import time
from typing import Any

from .db import get_conn
from . import query as _query

_LOG = logging.getLogger("crosswatch.event_archive")

_GROUP_COLUMNS = (
    "id", "group_hash", "created_at", "updated_at", "first_event_at", "last_event_at",
    "event_count", "status", "severity", "feature", "operation",
    "source_provider", "source_instance", "destination_provider", "destination_instance",
    "origin_provider", "origin_instance", "pair_key", "direction",
    "item_key", "title", "year", "media_type", "season", "episode",
    "reason_code", "reason", "summary", "acknowledged_at", "acknowledged_by",
)

_FAIL_TYPES = ("write_failed", "unresolved_recorded", "blackbox_promoted", "blackbox_blocked")
_RESOLVE_TYPES = ("write_succeeded", "unresolved_cleared")
_RUN_TYPES = ("sync_run_started", "sync_run_finished")
_PAST = {"add": "added", "remove": "removed", "update": "updated"}
_SEVERITY = {
    "completed": "success", "resolved": "success", "blackboxed": "warning", "unresolved": "warning",
    "failed": "error", "running": "info", "pending": "info", "informational": "info",
}

# DO NOT FORGET to update the version when the correlation key/status/summary logic
CORRELATION_VERSION = 7


def _P(v: Any) -> str:
    return str(v or "").strip().upper()


def _I(v: Any) -> str:
    try:
        from ..provider_instances import normalize_instance_id
        n = normalize_instance_id(v)
    except Exception:
        n = v
    s = str(n or "").strip().lower()
    return s or "default"


def _norm_op(op: Any) -> str:
    o = str(op or "").strip().lower()
    if o in ("add", "insert", "create", "quarantine"):
        return "add"
    if o in ("remove", "delete", "unrate", "prune"):
        return "remove"
    if o in ("update", "rate", "rating", "change"):
        return "update"
    return o


def _route_anchor(r: Any) -> str:
    dp, di = _P(_g(r, "destination_provider")), _I(_g(r, "destination_instance"))
    sp, si = _P(_g(r, "source_provider")), _I(_g(r, "source_instance"))
    if dp:
        return f">{dp}/{di}"
    if sp:
        return f"{sp}/{si}>"
    pair = str(_g(r, "pair_key") or "").strip().upper()
    return f"pair:{pair}" if pair else ""


def _g(r: Any, key: str) -> Any:
    try:
        return r[key]
    except Exception:
        return None


def group_hash(r: Any) -> str:
    item_key = str(_g(r, "item_key") or "").strip()
    if item_key:
        parts = [
            str(_g(r, "feature") or "").strip().lower(),
            _norm_op(_g(r, "operation")),
            item_key,
            _route_anchor(r),
        ]
    else:
        # collapse them into one thread per sync run
        run_id = str(_g(r, "run_id") or "").strip()
        if run_id:
            parts = ["\x00run", run_id]
        else:
            parts = [
                str(_g(r, "feature") or "").strip().lower(),
                _norm_op(_g(r, "operation")),
                "",
                _route_anchor(r),
            ]
    return hashlib.sha256("\x1f".join(parts).encode("utf-8", "replace")).hexdigest()


def _derive_status(events: list[dict[str, Any]]) -> str:
    types = {e.get("event_type") for e in events}

    # status reflects the run's completion
    if "sync_run_finished" in types:
        fin = next((e for e in reversed(events) if e.get("event_type") == "sync_run_finished"), {})
        return "failed" if int(_detail(fin).get("errors") or 0) > 0 else "completed"
    if "sync_run_started" in types:
        return "running"

 
    status_by_type = {
        "write_succeeded": "resolved",
        "unresolved_cleared": "resolved",
        "blackbox_promoted": "blackboxed",
        "blackbox_blocked": "blackboxed",
        "unresolved_recorded": "unresolved",
        "write_failed": "failed",
        "write_attempted": "pending",
    }
    best_status = None
    best_ts = -1
    for e in events:
        st = status_by_type.get(str(e.get("event_type") or ""))
        if st is None:
            continue
        ts = int(e.get("created_at") or 0)
        if ts >= best_ts:
            best_ts = ts
            best_status = st
    return best_status or "informational"


def _detail(e: dict[str, Any]) -> dict[str, Any]:
    d = e.get("detail")
    if isinstance(d, dict):
        return d
    if isinstance(d, str) and d:
        try:
            v = json.loads(d)
            return v if isinstance(v, dict) else {}
        except Exception:
            return {}
    return {}


def _summarize(status: str, events: list[dict[str, Any]], feature: str, dst: str, norm_op: str, reason_code: str) -> str:
    verb = norm_op or "update"
    past = _PAST.get(norm_op, "updated")

    types = {e.get("event_type") for e in events}
    last = events[-1] if events else {}
    feat_disp = str(feature or "").title()

    # one sync run with its pair/feature jobs and health checks
    if types & {"sync_run_started", "sync_run_finished"}:
        pairs = sum(1 for e in events if e.get("event_type") == "plan_created")
        tail = f", {pairs} pairs" if pairs else ""
        if "sync_run_finished" in types:
            fin = next((e for e in reversed(events) if e.get("event_type") == "sync_run_finished"), last)
            d = _detail(fin)
            errs = int(d.get("errors") or 0)
            unres = int(d.get("unresolved") or 0)
            blocked = sum(int(_detail(e).get("blocked") or 0) for e in events if e.get("event_type") == "blackbox_blocked")
            head = "Sync run completed successfully" if errs == 0 else "Sync run completed with errors"
            bb = f", {blocked} blackboxed" if blocked else ""
            return f"{head}, {errs} errors, {unres} unresolved{bb}{tail}"
        return f"Sync run in progress{tail}"

    # provider health thread
    if types == {"provider_health"}:
        prov = _P(_pick(events, "source_provider")) or "Provider"
        ok = str(last.get("severity") or "").lower() in ("info", "ok")
        return f"{prov} health check OK" if ok else f"{prov} health check reported an issue"

    # aligned when no changes were needed
    if types <= {"plan_created"}:
        d = _detail(last)
        changes = sum(int(d.get(k) or 0) for k in ("adds", "removes", "updates"))
        if changes == 0:
            src = _P(_pick(events, "source_provider"))
            route = f"{src} → {dst}" if (src and dst) else (dst or src or "")
            left = f"{route}, " if route else ""
            return f"{left}{feat_disp or 'Feature'} aligned, no changes needed"

    if str(feature or "").lower() == "ratings":
        rat = None
        for e in events:
            if e.get("old_value") is not None or e.get("new_value") is not None:
                rat = e
        if rat is not None:
            s = f"Rating changed from {rat.get('old_value') if rat.get('old_value') is not None else '–'} to {rat.get('new_value') if rat.get('new_value') is not None else '–'}"
            origin = rat.get("origin_provider")
            return s + (f", origin {origin}" if origin else "")

    fail_events = [e for e in events if e.get("event_type") == "write_failed"]
    if status == "failed" and len(events) == 1 and fail_events:
        return f"Provider rejected item, {reason_code}" if reason_code else "Provider rejected item"

    phrases: list[str] = []

    def add(p: str) -> None:
        if p and p not in phrases:
            phrases.append(p)

    for e in events:
        t = e.get("event_type")
        if t == "write_failed":
            add(f"{dst} {verb} failed" if dst else f"{verb} failed")
        elif t == "unresolved_recorded":
            add("recorded unresolved")
        elif t == "blackbox_promoted":
            add("blackboxed")
        elif t == "blackbox_blocked":
            add("blocked by blackbox")
        elif t == "unresolved_cleared":
            add("unresolved cleared")
        elif t == "write_succeeded":
            add(f"item {past} successfully")
        elif t == "write_attempted":
            add(f"{verb} attempted")
        elif t == "plan_created":
            add(f"{verb} planned")
        elif t == "tombstone_created":
            add("tombstoned")
        elif t == "provider_health":
            add("provider health change")
    if not phrases:
        et = (events[-1].get("event_type") if events else "") or "event"
        phrases = [str(et).replace("_", " ")]

    tail = {"unresolved": "recorded unresolved", "blackboxed": "blackboxed"}.get(status)
    if tail and tail in phrases and phrases[-1] != tail:
        phrases.remove(tail)
        phrases.append(tail)
    if len(phrases) == 1:
        s = phrases[0]
    else:
        s = ", ".join(phrases[:-1]) + ", then " + phrases[-1]
    return s[:1].upper() + s[1:]


def _pick(events: list[dict[str, Any]], key: str) -> Any:
    for e in reversed(events):
        v = e.get(key)
        if v not in (None, ""):
            return v
    return None


def _recompute(conn: sqlite3.Connection, group_id: int, now: int) -> None:
    rows = conn.execute(
        f"SELECT {','.join(_query._COLUMNS)} FROM events WHERE group_id=? ORDER BY created_at ASC, id ASC",
        (group_id,),
    ).fetchall()
    events = [dict(r) for r in rows]
    if not events:
        return
    ts = [int(e.get("created_at") or 0) for e in events]
    types = {e.get("event_type") for e in events}
    is_run = bool(types & set(_RUN_TYPES))
    feature = str(_pick(events, "feature") or "")
    norm_op = _norm_op(_pick(events, "operation"))
    dst = _P(_pick(events, "destination_provider"))
    status = _derive_status(events)
    severity = _SEVERITY.get(status, "info")
    fail_evt = None
    for e in events:
        if e.get("event_type") in _FAIL_TYPES:
            fail_evt = e
    reason_code = str((fail_evt or {}).get("reason_code") or _pick(events, "reason_code") or "")
    reason = str((fail_evt or {}).get("reason") or _pick(events, "reason") or "")
    summary = _summarize(status, events, feature, dst, norm_op, reason_code)

    if is_run:
        vals = (
            now, min(ts), max(ts), len(events), status, severity,
            None, "run", None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, summary, group_id,
        )
    else:
        vals = (
            now, min(ts), max(ts), len(events), status, severity,
            feature or None, norm_op or None,
            _pick(events, "source_provider"), _pick(events, "source_instance"),
            _pick(events, "destination_provider"), _pick(events, "destination_instance"),
            _pick(events, "origin_provider"), _pick(events, "origin_instance"),
            _pick(events, "pair_key"), _pick(events, "direction"),
            _pick(events, "item_key"), _pick(events, "title"), _pick(events, "year"),
            _pick(events, "media_type"), _pick(events, "season"), _pick(events, "episode"),
            reason_code or None, reason or None, summary, group_id,
        )
    conn.execute(
        "UPDATE event_groups SET updated_at=?, first_event_at=?, last_event_at=?, event_count=?, "
        "status=?, severity=?, feature=?, operation=?, source_provider=?, source_instance=?, "
        "destination_provider=?, destination_instance=?, origin_provider=?, origin_instance=?, "
        "pair_key=?, direction=?, item_key=?, title=?, year=?, media_type=?, season=?, episode=?, "
        "reason_code=?, reason=?, summary=? WHERE id=?",
        vals,
    )


def _persist_titles(conn: sqlite3.Connection, ids: list[int]) -> None:
    if not ids:
        return
    try:
        from .context import best_title
    except Exception:
        return
    updates: list[tuple[Any, Any, Any, int]] = []
    CHUNK = 400
    for i in range(0, len(ids), CHUNK):
        chunk = ids[i:i + CHUNK]
        qm = ",".join("?" for _ in chunk)
        try:
            rows = conn.execute(
                f"SELECT id, item_key, title, year, media_type, season, episode FROM events "
                f"WHERE id IN ({qm}) AND item_key IS NOT NULL AND item_key<>''",
                list(chunk),
            ).fetchall()
        except Exception:
            return
        for r in rows:
            d = dict(r)
            info = best_title(
                d.get("item_key"), title=d.get("title"), media_type=d.get("media_type"),
                season=d.get("season"), episode=d.get("episode"),
            )
            if not info:
                continue
            new_title = info.get("title") or d.get("title")
            new_year = d.get("year") if d.get("year") not in (None, "") else info.get("year")
            new_mt = d.get("media_type") or info.get("media_type")
            if new_title == d.get("title") and new_year == d.get("year") and new_mt == d.get("media_type"):
                continue
            updates.append((new_title, new_year, new_mt, d["id"]))
    if updates:
        try:
            conn.executemany(
                "UPDATE events SET title=?, year=?, media_type=? WHERE id=?", updates
            )
        except Exception:
            pass


def correlate(*, conn: sqlite3.Connection | None = None, reset: bool = False) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "grouped": 0}
    if reset:
        try:
            with c:
                c.execute("UPDATE events SET group_id=NULL")
                c.execute("DELETE FROM event_groups")
        except Exception as exc:
            _LOG.warning("event correlation reset failed: %s", exc)
    try:
        rows = c.execute(
            "SELECT id, feature, operation, item_key, source_provider, source_instance, "
            "destination_provider, destination_instance, pair_key, run_id FROM events WHERE group_id IS NULL"
        ).fetchall()
    except Exception as exc:
        _LOG.warning("event correlation query failed: %s", exc)
        return {"ok": False, "error": "internal_error", "grouped": 0}
    if not rows:
        return {"ok": True, "grouped": 0, "groups_touched": 0}

    now = int(time.time())
    all_ids = [int(r["id"]) for r in rows]
    buckets: dict[str, list[int]] = {}
    for r in rows:
        buckets.setdefault(group_hash(r), []).append(int(r["id"]))

    touched: set[int] = set()
    try:
        with c:
            _persist_titles(c, all_ids)
            for gh, ids in buckets.items():
                c.execute(
                    "INSERT INTO event_groups (group_hash, created_at, updated_at) VALUES (?,?,?) "
                    "ON CONFLICT(group_hash) DO NOTHING",
                    (gh, now, now),
                )
                gid = int(c.execute("SELECT id FROM event_groups WHERE group_hash=?", (gh,)).fetchone()[0])
                c.executemany("UPDATE events SET group_id=? WHERE id=?", [(gid, i) for i in ids])
                touched.add(gid)
            for gid in touched:
                _recompute(c, gid, now)
    except Exception as exc:
        _LOG.warning("event correlation failed: %s", exc)
        return {"ok": False, "error": "internal_error", "grouped": 0}
    return {"ok": True, "grouped": len(rows), "groups_touched": len(touched)}


def _ensure_correlated(conn: sqlite3.Connection) -> None:
    try:
        appid = int(conn.execute("PRAGMA application_id").fetchone()[0] or 0)
    except Exception:
        appid = CORRELATION_VERSION
    if appid < CORRELATION_VERSION:
        correlate(conn=conn, reset=True)
        try:
            conn.execute(f"PRAGMA application_id={CORRELATION_VERSION}")
        except Exception:
            pass
        return
    try:
        pending = conn.execute("SELECT 1 FROM events WHERE group_id IS NULL LIMIT 1").fetchone()
    except Exception:
        return
    if pending:
        correlate(conn=conn)


def _vis_clause(visibility: str | None) -> str | None:
    v = str(visibility or "open").strip().lower()
    if v == "all":
        return None
    if v == "acknowledged":
        return "g.acknowledged_at IS NOT NULL"
    return "g.acknowledged_at IS NULL"


_CATEGORY_STATUS = {
    "successful": ("resolved", "completed"),
    "problems": ("failed", "blackboxed", "unresolved"),
    "informational": ("informational", "pending", "running"),
}


def list_groups(
    *,
    q: str | None = None,
    visibility: str | None = "open",
    status: str | None = None,
    category: str | None = None,
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
    since: int | None = None,
    until: int | None = None,
    order: str | None = "newest",
    limit: int = 50,
    offset: int = 0,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    _ensure_correlated(c)

    clauses: list[str] = []
    params: list[Any] = []

    def eq(col: str, val: Any, up: bool = False) -> None:
        if val not in (None, ""):
            clauses.append(f"g.{col}=?")
            params.append(str(val).upper() if up else val)

    eq("feature", feature)
    eq("status", status)
    eq("item_key", item_key)
    eq("pair_key", pair_key)

    cat = _CATEGORY_STATUS.get(str(category or "").strip().lower())
    if cat:
        clauses.append(f"g.status IN ({','.join('?' for _ in cat)})")
        params.extend(cat)
    eq("origin_provider", origin_provider, up=True)
    eq("destination_provider", destination_provider, up=True)
    eq("source_provider", source_provider, up=True)
    eq("reason_code", reason_code)

    if provider:
        p = provider.upper()
        clauses.append("(g.source_provider=? OR g.destination_provider=? OR g.origin_provider=?)")
        params.extend([p, p, p])

    if q:
        like = f"%{q}%"
        clauses.append(
            "(g.summary LIKE ? OR g.title LIKE ? OR g.item_key LIKE ? OR g.reason LIKE ? OR g.reason_code LIKE ? "
            "OR g.id IN (SELECT group_id FROM events WHERE title LIKE ? OR item_key LIKE ? OR reason LIKE ? OR reason_code LIKE ?))"
        )
        params.extend([like] * 9)

    if event_type:
        clauses.append("g.id IN (SELECT group_id FROM events WHERE event_type=?)")
        params.append(event_type)
    if run_id:
        clauses.append("g.id IN (SELECT group_id FROM events WHERE run_id=?)")
        params.append(run_id)

    if since not in (None, ""):
        try:
            clauses.append("g.last_event_at>=?")
            params.append(int(since))
        except Exception:
            pass
    if until not in (None, ""):
        try:
            clauses.append("g.last_event_at<=?")
            params.append(int(until))
        except Exception:
            pass

    vc = _vis_clause(visibility)
    if vc:
        clauses.append(vc)

    # hide health-only groups 
    explicit = str(visibility or "").strip().lower() == "all" or any(v not in (None, "") for v in (
        q, status, category, event_type, provider, origin_provider,
        destination_provider, source_provider, feature, pair_key, item_key, run_id, reason_code,
    ))
    if not explicit:
        clauses.append(
            "g.id NOT IN (SELECT group_id FROM events WHERE group_id IS NOT NULL "
            "GROUP BY group_id HAVING SUM(event_type='provider_health')=COUNT(*) AND COUNT(*)>0)"
        )

    where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
    dir_ = "ASC" if str(order or "").strip().lower() in ("oldest", "asc") else "DESC"
    lim = max(1, min(int(limit or 50), 500))
    off = max(0, int(offset or 0))
    try:
        total = int(c.execute(f"SELECT COUNT(*) FROM event_groups g{where}", params).fetchone()[0])
        rows = c.execute(
            f"SELECT {','.join('g.' + col for col in _GROUP_COLUMNS)} FROM event_groups g{where} "
            f"ORDER BY g.last_event_at {dir_}, g.id {dir_} LIMIT ? OFFSET ?",
            [*params, lim, off],
        ).fetchall()
    except Exception as exc:
        _LOG.warning("group list failed: %s", exc)
        return {"items": [], "total": 0, "limit": lim, "offset": off}
    return {"items": [dict(r) for r in rows], "total": total, "limit": lim, "offset": off}


def get_group(group_id: Any, *, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    c = conn or get_conn()
    if c is None:
        return None
    try:
        row = c.execute(
            f"SELECT {','.join(_GROUP_COLUMNS)} FROM event_groups WHERE id=?", (int(group_id),)
        ).fetchone()
    except Exception:
        return None
    return dict(row) if row else None


def group_events(group_id: Any, *, order: str | None = "asc", limit: int = 500, offset: int = 0, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"items": [], "total": 0}
    dir_ = "DESC" if str(order or "").strip().lower() in ("newest", "desc") else "ASC"
    lim = max(1, min(int(limit or 500), 1000))
    off = max(0, int(offset or 0))
    try:
        gid = int(group_id)
        total = int(c.execute("SELECT COUNT(*) FROM events WHERE group_id=?", (gid,)).fetchone()[0])
        rows = c.execute(
            f"SELECT {','.join(_query._COLUMNS)} FROM events WHERE group_id=? "
            f"ORDER BY created_at {dir_}, id {dir_} LIMIT ? OFFSET ?",
            (gid, lim, off),
        ).fetchall()
    except Exception:
        return {"items": [], "total": 0}
    return {"items": [dict(r) for r in rows], "total": total, "limit": lim, "offset": off}


def run_problem_items(run_id: Any, since: Any = None, until: Any = None, *, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    """Item threads that had a failure/unresolved/blackbox event during this run.

    Matches on run_id when present, but also on the run's time window: imported
    events carry a report-derived run_id that differs from the live run's id, so
    the window is the reliable link between a run and its problem items.
    """
    c = conn or get_conn()
    rid = str(run_id or "").strip()
    match: list[str] = []
    params: list[Any] = []
    if rid:
        match.append("run_id = ?")
        params.append(rid)
    if since is not None and until is not None:
        match.append("(created_at >= ? AND created_at <= ?)")
        params.extend([int(since), int(until)])
    if c is None or not match:
        return {"items": []}
    try:
        rows = c.execute(
            f"SELECT {','.join('g.' + col for col in _GROUP_COLUMNS)} FROM event_groups g "
            "WHERE g.item_key IS NOT NULL AND g.item_key <> '' AND g.id IN ("
            "  SELECT DISTINCT group_id FROM events WHERE group_id IS NOT NULL "
            "  AND event_type IN ('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked') "
            f"  AND ({' OR '.join(match)})"
            ") ORDER BY g.last_event_at DESC, g.id DESC",
            params,
        ).fetchall()
    except Exception as exc:
        _LOG.warning("run problem items failed: %s", exc)
        return {"items": []}
    return {"items": [dict(r) for r in rows]}


def list_tree(*, order: str | None = "newest", limit: int = 50, offset: int = 0,
              conn: sqlite3.Connection | None = None, **filters: Any) -> dict[str, Any]:
    """Grouped list as a tree: run threads are parents, the item threads that had
    a problem during each run are nested as `children`; items owned by a run are
    lifted out of the top level. Standalone item threads stay at the top level."""
    c = conn or get_conn()
    if c is None:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    flat = list_groups(order=order, limit=2000, offset=0, conn=c, **filters).get("items") or []
    runs: list[dict[str, Any]] = []
    items: list[dict[str, Any]] = []
    others: list[dict[str, Any]] = []
    for g in flat:
        if str(g.get("operation") or "") == "run":
            runs.append(g)
        elif g.get("item_key"):
            items.append(g)
        else:
            others.append(g)
    items_by_id = {g["id"]: g for g in items}
    owned: set[Any] = set()
    for r in runs:
        res = run_problem_items(None, r.get("first_event_at"), r.get("last_event_at"), conn=c).get("items") or []
        kids = [items_by_id[x["id"]] for x in res if x["id"] in items_by_id]
        r["children"] = kids
        owned.update(k["id"] for k in kids)
    top = runs + [g for g in items if g["id"] not in owned] + others
    rev = str(order or "newest").strip().lower() not in ("oldest", "asc")
    top.sort(key=lambda g: (int(g.get("last_event_at") or 0), int(g.get("id") or 0)), reverse=rev)
    total = len(top)
    lim = max(1, min(int(limit or 50), 500))
    off = max(0, int(offset or 0))
    return {"items": top[off:off + lim], "total": total, "limit": lim, "offset": off}


def acknowledge_group(group_id: Any, *, by: str | None = None, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    try:
        gid = int(group_id)
    except Exception:
        return {"ok": False, "error": "bad_id"}
    ts = int(time.time())
    try:
        with c:
            c.execute(
                "UPDATE event_groups SET acknowledged_at=?, acknowledged_by=? WHERE id=? AND acknowledged_at IS NULL",
                (ts, by, gid),
            )
        row = c.execute("SELECT acknowledged_at, acknowledged_by FROM event_groups WHERE id=?", (gid,)).fetchone()
    except Exception:
        _LOG.exception("group acknowledge failed")
        return {"ok": False, "error": "internal_error"}
    if row is None:
        return {"ok": False, "id": gid, "found": False}
    return {"ok": True, "id": gid, "acknowledged_at": int(row[0]) if row[0] is not None else ts, "acknowledged_by": row[1]}


def unacknowledge_group(group_id: Any, *, conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    try:
        gid = int(group_id)
    except Exception:
        return {"ok": False, "error": "bad_id"}
    try:
        with c:
            c.execute("UPDATE event_groups SET acknowledged_at=NULL, acknowledged_by=NULL WHERE id=?", (gid,))
        row = c.execute("SELECT id FROM event_groups WHERE id=?", (gid,)).fetchone()
    except Exception:
        _LOG.exception("group unacknowledge failed")
        return {"ok": False, "error": "internal_error"}
    if row is None:
        return {"ok": False, "id": gid, "found": False}
    return {"ok": True, "id": gid, "acknowledged_at": None}

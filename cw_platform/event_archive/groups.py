# cw_platform/event_archive/groups.py
# CrossWatch - Event correlation into user-facing threads
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import logging
import re
import sqlite3
import time
from typing import Any

from .db import get_conn
from . import query as _query
from .scrobble_recorder import session_token

_LOG = logging.getLogger("crosswatch.event_archive")

_GROUP_COLUMNS = (
    "id", "group_hash", "domain", "created_at", "updated_at", "first_event_at", "last_event_at",
    "event_count", "status", "severity", "feature", "operation",
    "source_provider", "source_instance", "destination_provider", "destination_instance",
    "origin_provider", "origin_instance", "pair_key", "direction",
    "item_key", "title", "year", "media_type", "season", "episode",
    "reason_code", "reason", "summary", "acknowledged_at", "acknowledged_by",
)

_FAIL_TYPES = ("write_failed", "unresolved_recorded", "blackbox_promoted", "blackbox_blocked")
_RESOLVE_TYPES = ("write_succeeded", "unresolved_cleared")
_RUN_TYPES = ("sync_run_started", "sync_run_finished")
_SCROBBLE_STATUS = {
    "scrobble_completed": "completed",
    "scrobble_failed": "failed",
    "scrobble_started": "running",
    "rating_applied": "rated",
    "rating_failed": "failed",
}
_PAST = {"add": "added", "remove": "removed", "update": "updated"}
_SEVERITY = {
    "completed": "success", "resolved": "success", "rated": "success",
    "warning": "warning", "blackboxed": "warning", "unresolved": "warning",
    "failed": "error", "running": "info", "pending": "info", "informational": "info",
}
_PROBLEM_TYPES_SQL = "('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked')"
_UNRESOLVED_TYPES_SQL = "('write_failed','unresolved_recorded')"

# DO NOT FORGET to update the version when the correlation key/status/summary logic
CORRELATION_VERSION = 13


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
    if str(_g(r, "domain") or "sync") == "scrobble":
        token = session_token(_g(r, "session_key"), _g(r, "created_at"))
        ident = str(_g(r, "item_key") or "").strip() or str(_g(r, "title") or "").strip().lower()
        parts = [
            "\x00scrobble",
            str(_g(r, "feature") or "").strip().lower(),
            str(_g(r, "source_kind") or "").strip().lower(),
            _P(_g(r, "source_provider")),
            _I(_g(r, "source_instance")),
            ident,
            str(_g(r, "season") or ""),
            str(_g(r, "episode") or ""),
            _route_anchor(r),
            token,
        ]
        return hashlib.sha256("\x1f".join(parts).encode("utf-8", "replace")).hexdigest()
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


def _derive_status(events: list[dict[str, Any]], extra_problems: int = 0) -> str:
    types = {e.get("event_type") for e in events}

    if types & set(_SCROBBLE_STATUS):
        best, best_ts = None, -1
        for e in events:
            st = _SCROBBLE_STATUS.get(str(e.get("event_type") or ""))
            if st is None:
                continue
            ts = int(e.get("created_at") or 0)
            if ts >= best_ts:
                best_ts, best = ts, st
        return best or "informational"

    # status reflects the run's completion
    if "sync_run_finished" in types:
        fin = next((e for e in reversed(events) if e.get("event_type") == "sync_run_finished"), {})
        d = _detail(fin)
        if int(d.get("errors") or 0) > 0:
            return "failed"
        if int(d.get("unresolved") or 0) > 0 or extra_problems > 0:
            return "warning"
        return "completed"
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


def _summarize(status: str, events: list[dict[str, Any]], feature: str, dst: str, norm_op: str, reason_code: str, feat_issues: dict[str, int] | None = None) -> str:
    verb = norm_op or "update"
    past = _PAST.get(norm_op, "updated")

    types = {e.get("event_type") for e in events}
    last = events[-1] if events else {}
    feat_disp = str(feature or "").title()

    if types & set(_SCROBBLE_STATUS):
        return _summarize_scrobble(status, events, dst)

    # one sync run with its pair/feature jobs and health checks
    if types & {"sync_run_started", "sync_run_finished"}:
        plan_events = [e for e in events if e.get("event_type") == "plan_created"]
        pair_keys = {str(e.get("pair_key") or "").strip().upper() for e in plan_events}
        pair_keys.discard("")
        pairs = len(pair_keys) or len(plan_events)
        feats: list[str] = []
        for e in plan_events:
            fe = str(e.get("feature") or "").strip().lower()
            if fe and fe not in feats:
                feats.append(fe)
        feat_disp = ", ".join(f.title() for f in feats)
        tail = f", {pairs} {'pair' if pairs == 1 else 'pairs'}" if pairs else ""
        if tail and feat_disp:
            tail += f" ({feat_disp})"
        if "sync_run_finished" in types:
            fin = next((e for e in reversed(events) if e.get("event_type") == "sync_run_finished"), last)
            d = _detail(fin)
            errs = int(d.get("errors") or 0)
            issues = {f: n for f, n in (feat_issues or {}).items() if n}
            unres = sum(issues.values()) or int(d.get("unresolved") or 0)
            blocked = sum(int(_detail(e).get("blocked") or 0) for e in events if e.get("event_type") == "blackbox_blocked")
            if errs > 0:
                head = "Sync run completed with errors"
            elif unres > 0:
                head = "Sync run completed with issues"
            else:
                head = "Sync run completed successfully"
            if issues:
                by_feat = ", ".join(f"{f.title()} {n}" for f, n in sorted(issues.items(), key=lambda x: -x[1]))
                unres_txt = f", {unres} unresolved ({by_feat})"
            else:
                unres_txt = f", {unres} unresolved" if unres else ""
            err_txt = f", {errs} errors" if errs else ""
            bb = f", {blocked} blackboxed" if blocked else ""
            clean = errs == 0 and unres == 0
            tail_final = tail if clean else (f", {pairs} {'pair' if pairs == 1 else 'pairs'}" if pairs else "")
            return f"{head}{err_txt}{unres_txt}{bb}{tail_final}"
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


def _title_of(e: dict[str, Any]) -> str:
    t = str(e.get("title") or e.get("item_key") or "").strip()
    s, ep = e.get("season"), e.get("episode")
    if s is not None and ep is not None:
        try:
            t = f"{t} S{int(s):02d}E{int(ep):02d}"
        except Exception:
            pass
    return t


def _summarize_scrobble(status: str, events: list[dict[str, Any]], dst: str) -> str:
    last = events[-1] if events else {}
    name = _title_of(last) or "item"
    target = dst or _P(_pick(events, "destination_provider"))
    to = f" → {target}" if target else ""
    if status == "rated":
        d = _detail(last)
        rating = d.get("rating")
        return f"Rated {name} {rating}/10{to}" if rating not in (None, "") else f"Rated {name}{to}"
    if status == "failed":
        rating_thread = any(str(e.get("feature") or "") == "ratings" for e in events)
        reason = str(_pick(events, "reason_code") or _pick(events, "reason") or "")
        head = "Rating forward failed" if rating_thread else "Scrobble failed"
        return f"{head}{to}, {reason}" if reason else f"{head}{to}"
    prog = _detail(last).get("progress")
    tail = f", {prog}%" if prog not in (None, "") else ""
    if status == "completed":
        return f"Watched {name}{to}{tail}"
    return f"Watching {name}{to}{tail}"


def _pick(events: list[dict[str, Any]], key: str) -> Any:
    for e in reversed(events):
        v = e.get(key)
        if v not in (None, ""):
            return v
    return None


def _run_feature_issues(conn: sqlite3.Connection, run_id: Any) -> dict[str, int]:
    rid = str(run_id or "").strip()
    if not rid:
        return {}
    try:
        rows = conn.execute(
            f"SELECT feature, COUNT(DISTINCT item_key) c FROM events WHERE run_id=? "
            f"AND event_type IN {_UNRESOLVED_TYPES_SQL} AND item_key IS NOT NULL AND item_key<>'' GROUP BY feature",
            (rid,),
        ).fetchall()
    except Exception:
        return {}
    return {str(r["feature"] or "").strip().lower(): int(r["c"]) for r in rows if int(r["c"] or 0)}


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
    feat_issues = _run_feature_issues(conn, _pick(events, "run_id")) if is_run else {}
    status = _derive_status(events, sum(feat_issues.values()))
    severity = _SEVERITY.get(status, "info")
    fail_evt = None
    for e in events:
        if e.get("event_type") in _FAIL_TYPES:
            fail_evt = e
    reason_code = str((fail_evt or {}).get("reason_code") or _pick(events, "reason_code") or "")
    reason = str((fail_evt or {}).get("reason") or _pick(events, "reason") or "")
    summary = _summarize(status, events, feature, dst, norm_op, reason_code, feat_issues)
    domain = str(_pick(events, "domain") or "sync")

    if is_run:
        vals = (
            domain, now, min(ts), max(ts), len(events), status, severity,
            None, "run", None, None, None, None, None, None, None, None,
            None, None, None, None, None, None, None, None, summary, group_id,
        )
    else:
        vals = (
            domain, now, min(ts), max(ts), len(events), status, severity,
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
        "UPDATE event_groups SET domain=?, updated_at=?, first_event_at=?, last_event_at=?, event_count=?, "
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
            "SELECT id, domain, feature, operation, item_key, title, season, episode, created_at, "
            "source_kind, session_key, source_provider, source_instance, destination_provider, destination_instance, "
            "pair_key, run_id FROM events WHERE group_id IS NULL"
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


def _event_vis_clause(visibility: str | None) -> str | None:
    v = str(visibility or "open").strip().lower()
    if v == "all":
        return None
    if v == "acknowledged":
        return "(e.acknowledged_at IS NOT NULL OR EXISTS (SELECT 1 FROM event_groups eg WHERE eg.id=e.group_id AND eg.acknowledged_at IS NOT NULL))"
    return "e.acknowledged_at IS NULL AND NOT EXISTS (SELECT 1 FROM event_groups eg WHERE eg.id=e.group_id AND eg.acknowledged_at IS NOT NULL)"


_CATEGORY_STATUS = {
    "successful": ("resolved", "completed", "rated"),
    "problems": ("failed", "warning", "blackboxed", "unresolved"),
    "informational": ("informational", "pending", "running"),
}


def list_groups(
    *,
    q: str | None = None,
    visibility: str | None = "open",
    domain: str | None = None,
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
    operation: str | None = None,
    top_level_only: bool = False,
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

    eq("domain", domain)
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
    eq("operation", operation)

    if top_level_only:
        clauses.append(
            "(g.operation='run' OR g.item_key IS NULL OR g.item_key='' OR NOT EXISTS ("
            "SELECT 1 FROM events pe JOIN event_groups rg "
            "ON rg.operation='run' AND pe.created_at>=rg.first_event_at AND pe.created_at<=rg.last_event_at "
            "WHERE pe.group_id=g.id AND pe.event_type IN "
            "('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked')"
            "))"
        )

    if provider:
        p = provider.upper()
        clauses.append("(g.source_provider=? OR g.destination_provider=? OR g.origin_provider=?)")
        params.extend([p, p, p])

    if q:
        like = f"%{q}%"
        evc = _event_vis_clause(visibility)
        event_where = "e.title LIKE ? OR e.item_key LIKE ? OR e.reason LIKE ? OR e.reason_code LIKE ?"
        if evc:
            event_where = f"({event_where}) AND {evc}"
        clauses.append(
            "(g.summary LIKE ? OR g.title LIKE ? OR g.item_key LIKE ? OR g.reason LIKE ? OR g.reason_code LIKE ? "
            f"OR g.id IN (SELECT e.group_id FROM events e WHERE e.group_id IS NOT NULL AND {event_where}))"
        )
        params.extend([like] * 9)

    if event_type:
        evc = _event_vis_clause(visibility)
        suffix = f" AND {evc}" if evc else ""
        clauses.append(f"g.id IN (SELECT e.group_id FROM events e WHERE e.group_id IS NOT NULL AND e.event_type=?{suffix})")
        params.append(event_type)
    if run_id:
        evc = _event_vis_clause(visibility)
        suffix = f" AND {evc}" if evc else ""
        clauses.append(f"g.id IN (SELECT e.group_id FROM events e WHERE e.group_id IS NOT NULL AND e.run_id=?{suffix})")
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
        destination_provider, source_provider, feature, pair_key, item_key, run_id, reason_code, operation,
    ))
    if not explicit and str(domain or "sync") != "scrobble":
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


def run_problem_items(
    run_id: Any,
    since: Any = None,
    until: Any = None,
    *,
    limit: int | None = None,
    offset: int = 0,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
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
        return {"items": [], "total": 0, "limit": limit, "offset": max(0, int(offset or 0))}
    off = max(0, int(offset or 0))
    lim = None if limit is None else max(1, min(int(limit or 100), 500))
    base_sql = (
        f"FROM event_groups g "
        "WHERE g.item_key IS NOT NULL AND g.item_key <> '' AND g.id IN ("
        "  SELECT DISTINCT group_id FROM events WHERE group_id IS NOT NULL "
        "  AND event_type IN ('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked') "
        f"  AND ({' OR '.join(match)})"
        ")"
    )
    try:
        total = int(c.execute(f"SELECT COUNT(*) {base_sql}", params).fetchone()[0])
        sql = (
            f"SELECT {','.join('g.' + col for col in _GROUP_COLUMNS)} {base_sql} "
            "ORDER BY g.last_event_at DESC, g.id DESC"
        )
        row_params = list(params)
        if lim is not None:
            sql += " LIMIT ? OFFSET ?"
            row_params.extend([lim, off])
        rows = c.execute(
            sql,
            row_params,
        ).fetchall()
    except Exception as exc:
        _LOG.warning("run problem items failed: %s", exc)
        return {"items": [], "total": 0, "limit": lim if lim is not None else limit, "offset": off}
    return {"items": [dict(r) for r in rows], "total": total, "limit": lim if lim is not None else total, "offset": off}


def _run_children_bulk(
    runs: list[dict[str, Any]],
    item_ids: set[Any],
    *,
    conn: sqlite3.Connection,
) -> dict[Any, list[Any]]:
    out: dict[Any, list[Any]] = {r.get("id"): [] for r in runs}
    windows: list[tuple[Any, int, int]] = []
    for run in runs:
        first_event_at = run.get("first_event_at")
        last_event_at = run.get("last_event_at")
        if first_event_at is None or last_event_at is None:
            continue
        try:
            windows.append((run.get("id"), int(first_event_at), int(last_event_at)))
        except (TypeError, ValueError):
            continue
    ids = [int(x) for x in item_ids if x not in (None, "")]
    if not windows or not ids:
        return out

    lo = min(x[1] for x in windows)
    hi = max(x[2] for x in windows)
    seen: dict[Any, set[Any]] = {run_id: set() for run_id, _, _ in windows}
    try:
        rows = conn.execute(
            "SELECT DISTINCT group_id, created_at FROM events "
            f"WHERE group_id IN ({','.join('?' for _ in ids)}) "
            "AND event_type IN ('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked') "
            "AND created_at>=? AND created_at<=? ORDER BY created_at DESC, group_id DESC",
            [*ids, lo, hi],
        ).fetchall()
    except Exception as exc:
        _LOG.warning("bulk run children failed: %s", exc)
        return out

    for row in rows:
        group_id = row["group_id"]
        created_at = int(row["created_at"] or 0)
        for run_id, since, until in windows:
            if since <= created_at <= until and group_id not in seen[run_id]:
                seen[run_id].add(group_id)
                out[run_id].append(group_id)
    return out


def _run_child_groups_bulk(
    runs: list[dict[str, Any]],
    *,
    visibility: str | None,
    conn: sqlite3.Connection,
) -> dict[Any, list[dict[str, Any]]]:
    """Load all visible problem children for the current run-parent page."""
    out: dict[Any, list[dict[str, Any]]] = {r.get("id"): [] for r in runs}
    windows: list[tuple[Any, int, int]] = []
    for run in runs:
        first_event_at = run.get("first_event_at")
        last_event_at = run.get("last_event_at")
        if first_event_at is None or last_event_at is None:
            continue
        try:
            windows.append((run.get("id"), int(first_event_at), int(last_event_at)))
        except (TypeError, ValueError):
            continue
    if not windows:
        return out
    lo = min(x[1] for x in windows)
    hi = max(x[2] for x in windows)
    vis = _vis_clause(visibility)
    vis_sql = f" AND {vis}" if vis else ""
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {','.join('g.' + col for col in _GROUP_COLUMNS)}, e.created_at AS problem_at "
            "FROM events e JOIN event_groups g ON g.id=e.group_id "
            "WHERE g.item_key IS NOT NULL AND g.item_key<>'' "
            "AND e.event_type IN ('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked') "
            f"AND e.created_at>=? AND e.created_at<=?{vis_sql} "
            "ORDER BY g.last_event_at DESC, g.id DESC",
            (lo, hi),
        ).fetchall()
    except Exception as exc:
        _LOG.warning("bulk run child groups failed: %s", exc)
        return out
    seen: dict[Any, set[Any]] = {run_id: set() for run_id, _, _ in windows}
    for row in rows:
        group_id = row["id"]
        problem_at = int(row["problem_at"] or 0)
        group = {col: row[col] for col in _GROUP_COLUMNS}
        for run_id, since, until in windows:
            if since <= problem_at <= until and group_id not in seen[run_id]:
                seen[run_id].add(group_id)
                out[run_id].append(group)
    return out


def _run_child_counts_bulk(
    runs: list[dict[str, Any]],
    *,
    visibility: str | None,
    conn: sqlite3.Connection,
) -> dict[Any, int]:
    out: dict[Any, int] = {r.get("id"): 0 for r in runs}
    windows: list[tuple[Any, int, int]] = []
    for run in runs:
        first_event_at = run.get("first_event_at")
        last_event_at = run.get("last_event_at")
        if first_event_at is None or last_event_at is None:
            continue
        try:
            windows.append((run.get("id"), int(first_event_at), int(last_event_at)))
        except (TypeError, ValueError):
            continue
    if not windows:
        return out
    lo = min(x[1] for x in windows)
    hi = max(x[2] for x in windows)
    vis = _vis_clause(visibility)
    vis_sql = f" AND {vis}" if vis else ""
    try:
        rows = conn.execute(
            "SELECT DISTINCT g.id AS group_id, e.created_at AS problem_at "
            "FROM events e JOIN event_groups g ON g.id=e.group_id "
            "WHERE g.item_key IS NOT NULL AND g.item_key<>'' "
            "AND e.event_type IN ('write_failed','unresolved_recorded','blackbox_promoted','blackbox_blocked') "
            f"AND e.created_at>=? AND e.created_at<=?{vis_sql} "
            "ORDER BY e.created_at DESC, g.id DESC",
            (lo, hi),
        ).fetchall()
    except Exception as exc:
        _LOG.warning("bulk run child counts failed: %s", exc)
        return out
    seen: dict[Any, set[Any]] = {run_id: set() for run_id, _, _ in windows}
    for row in rows:
        group_id = row["group_id"]
        problem_at = int(row["problem_at"] or 0)
        for run_id, since, until in windows:
            if since <= problem_at <= until and group_id not in seen[run_id]:
                seen[run_id].add(group_id)
    for run_id, ids in seen.items():
        out[run_id] = len(ids)
    return out


def _run_problem_status_counts_bulk(
    runs: list[dict[str, Any]],
    *,
    conn: sqlite3.Connection,
) -> dict[Any, dict[str, int]]:
    """Count unique problem groups by status for every visible run."""
    windows: list[tuple[Any, int, int]] = []
    for run in runs:
        first_event_at = run.get("first_event_at")
        last_event_at = run.get("last_event_at")
        if first_event_at is None or last_event_at is None:
            continue
        try:
            windows.append((run.get("id"), int(first_event_at), int(last_event_at)))
        except (TypeError, ValueError):
            continue
    out: dict[Any, dict[str, int]] = {run_id: {} for run_id, _, _ in windows}
    if not windows:
        return out
    lo = min(x[1] for x in windows)
    hi = max(x[2] for x in windows)
    matched: dict[Any, dict[str, set[Any]]] = {run_id: {} for run_id, _, _ in windows}
    try:
        rows = conn.execute(
            "SELECT DISTINCT e.group_id, e.created_at FROM events e "
            "WHERE e.group_id IS NOT NULL "
            "AND e.event_type IN ('blackbox_promoted','blackbox_blocked') "
            "AND e.created_at>=? AND e.created_at<=?",
            (lo, hi),
        ).fetchall()
    except Exception as exc:
        _LOG.warning("bulk run status counts failed: %s", exc)
        return out
    for row in rows:
        group_id = row["group_id"]
        created_at = int(row["created_at"] or 0)
        for run_id, since, until in windows:
            if since <= created_at <= until:
                matched[run_id].setdefault("blackboxed", set()).add(group_id)
    for run_id, statuses in matched.items():
        out[run_id] = {status: len(group_ids) for status, group_ids in statuses.items()}
    return out


def list_tree(*, order: str | None = "newest", limit: int = 50, offset: int = 0,
              include_children: bool = True, domain: str | None = None,
              conn: sqlite3.Connection | None = None, **filters: Any) -> dict[str, Any]:
    """Grouped list as a tree: run threads are parents, the item threads that had
    a problem during each run are nested as `children`; items owned by a run are
    lifted out of the top level. Standalone item threads stay at the top level."""
    c = conn or get_conn()
    if c is None:
        return {"items": [], "total": 0, "limit": limit, "offset": offset}
    visibility = filters.get("visibility", "open")
    active_filters = any(
        value not in (None, "")
        for key, value in filters.items()
        if key != "visibility"
    )
    if not active_filters:
        parent_page = list_groups(
            order=order,
            limit=limit,
            offset=offset,
            conn=c,
            visibility=visibility,
            domain=domain,
            top_level_only=True,
        )
        top = parent_page.get("items") or []
        runs = [g for g in top if str(g.get("operation") or "") == "run"]
        children_by_run = _run_child_groups_bulk(runs, visibility=visibility, conn=c) if include_children else {}
        child_counts_by_run = _run_child_counts_bulk(runs, visibility=visibility, conn=c) if not include_children else {}
        status_counts_by_run = _run_problem_status_counts_bulk(runs, conn=c)
        for run in runs:
            children = children_by_run.get(run.get("id"), [])
            run["children"] = children[:500] if include_children else []
            run["children_total"] = len(children) if include_children else int(child_counts_by_run.get(run.get("id")) or 0)
            blackboxed = int((status_counts_by_run.get(run.get("id")) or {}).get("blackboxed") or 0)
            summary = str(run.get("summary") or "")
            if blackboxed and "blackboxed" not in summary.lower():
                pair_suffix = re.search(r", \d+ pairs?(?: \([^)]*\))?$", summary)
                insert_at = pair_suffix.start() if pair_suffix else len(summary)
                run["summary"] = f"{summary[:insert_at]}, {blackboxed} blackboxed{summary[insert_at:]}"
        return {
            "items": top,
            "total": int(parent_page.get("total") or 0),
            "limit": int(parent_page.get("limit") or limit),
            "offset": int(parent_page.get("offset") or offset),
        }
    flat = list_groups(order=order, limit=2000, offset=0, conn=c, domain=domain, **filters).get("items") or []
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
    children_by_run = _run_children_bulk(runs, set(items_by_id), conn=c)
    status_counts_by_run = _run_problem_status_counts_bulk(runs, conn=c)
    for r in runs:
        kids = [items_by_id[x] for x in children_by_run.get(r.get("id"), []) if x in items_by_id]
        kids.sort(key=lambda g: (int(g.get("last_event_at") or 0), int(g.get("id") or 0)), reverse=True)
        r["children"] = kids
        blackboxed = int((status_counts_by_run.get(r.get("id")) or {}).get("blackboxed") or 0)
        summary = str(r.get("summary") or "")
        if blackboxed and "blackboxed" not in summary.lower():
            pair_suffix = re.search(r", \d+ pairs?$", summary)
            insert_at = pair_suffix.start() if pair_suffix else len(summary)
            r["summary"] = f"{summary[:insert_at]}, {blackboxed} blackboxed{summary[insert_at:]}"
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

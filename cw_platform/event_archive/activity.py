# cw_platform/event_archive/activity.py
# CrossWatch - Sync activity calendar aggregation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import threading
import time
from collections.abc import Iterable
from typing import Any

from .db import get_conn

_DAY = 86400
_TZ_LIMIT = 14 * 3600
_MAX_SCOPE_TERMS = 400
_MAX_RUNS = 500

_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 60.0
_CACHE_CAP = 32
_LOCK = threading.RLock()


def clamp_tz(value: Any) -> int:
    try:
        tz = int(value or 0)
    except Exception:
        return 0
    return max(-_TZ_LIMIT, min(_TZ_LIMIT, tz))


class Scope:
    """Resolved profile scope. `unrestricted` reads sync_runs without a join."""

    __slots__ = ("unrestricted", "pair_ids", "scope_keys")

    def __init__(self, *, unrestricted: bool, pair_ids: Iterable[str] = (), scope_keys: Iterable[str] = ()) -> None:
        self.unrestricted = bool(unrestricted)
        self.pair_ids = sorted({str(v) for v in pair_ids if str(v or "").strip()})[:_MAX_SCOPE_TERMS]
        self.scope_keys = sorted({str(v) for v in scope_keys if str(v or "").strip()})[:_MAX_SCOPE_TERMS]

    @property
    def empty(self) -> bool:
        return not self.unrestricted and not self.pair_ids and not self.scope_keys

    def clause(self) -> tuple[str, list[Any]]:
        terms: list[str] = []
        params: list[Any] = []
        if self.pair_ids:
            terms.append(f"p.pair_id IN ({','.join('?' for _ in self.pair_ids)})")
            params.extend(self.pair_ids)
        if self.scope_keys:
            terms.append(f"p.scope_key IN ({','.join('?' for _ in self.scope_keys)})")
            params.extend(self.scope_keys)
        return (f"({' OR '.join(terms)})" if terms else "0"), params

    def token(self) -> str:
        if self.unrestricted:
            return "all"
        raw = "\x1f".join(("i", *self.pair_ids, "s", *self.scope_keys))
        return hashlib.sha1(raw.encode("utf-8", "replace")).hexdigest()[:16]


UNRESTRICTED = Scope(unrestricted=True)


def _empty_payload(since: int, until: int, tz: int) -> dict[str, Any]:
    return {
        "ok": True,
        "range": {"since": since, "until": until, "tz_offset": tz},
        "days": [],
        "totals": {"runs": 0, "failed": 0, "added": 0, "removed": 0, "updated": 0, "changes": 0, "active_days": 0},
    }


def _day_rows(conn: Any, since: int, until: int, tz: int, scope: Scope) -> list[dict[str, Any]]:
    if scope.unrestricted:
        sql = (
            "SELECT (started_at + ?)/86400 AS d, COUNT(*) runs, "
            "SUM(CASE WHEN COALESCE(errors,0)>0 THEN 1 ELSE 0 END) failed, "
            "SUM(COALESCE(added,0)) added, SUM(COALESCE(removed,0)) removed, SUM(COALESCE(updated,0)) updated "
            "FROM sync_runs WHERE started_at>=? AND started_at<? GROUP BY d"
        )
        params: list[Any] = [tz, since, until]
    else:
        where, scope_params = scope.clause()
        sql = (
            "SELECT (r.started_at + ?)/86400 AS d, COUNT(DISTINCT r.run_id) runs, "
            "COUNT(DISTINCT CASE WHEN p.errors>0 THEN r.run_id END) failed, "
            "SUM(p.added) added, SUM(p.removed) removed, SUM(p.updated) updated "
            "FROM sync_runs r JOIN run_pairs p ON p.run_id=r.run_id "
            f"WHERE r.started_at>=? AND r.started_at<? AND {where} GROUP BY d"
        )
        params = [tz, since, until, *scope_params]
    return [dict(row) for row in conn.execute(sql, params).fetchall()]


def _compute(conn: Any, since: int, until: int, tz: int, scope: Scope) -> dict[str, Any]:
    days: list[dict[str, Any]] = []
    totals = {"runs": 0, "failed": 0, "added": 0, "removed": 0, "updated": 0}
    for row in _day_rows(conn, since, until, tz, scope):
        index = int(row["d"])
        entry = {
            "d": index,
            "ts": index * _DAY - tz,
            "runs": int(row["runs"] or 0),
            "failed": int(row["failed"] or 0),
            "added": int(row["added"] or 0),
            "removed": int(row["removed"] or 0),
            "updated": int(row["updated"] or 0),
        }
        entry["changes"] = entry["added"] + entry["removed"] + entry["updated"]
        for key in totals:
            totals[key] += entry[key]
        days.append(entry)
    days.sort(key=lambda item: item["d"])
    totals["changes"] = totals["added"] + totals["removed"] + totals["updated"]
    totals["active_days"] = len(days)
    return {
        "ok": True,
        "range": {"since": since, "until": until, "tz_offset": tz},
        "days": days,
        "totals": totals,
    }


def calendar(*, since: int, until: int, tz_offset: int = 0, scope: Scope | None = None, conn: Any = None) -> dict[str, Any]:
    until = int(until)
    since = int(min(since, until))
    tz = clamp_tz(tz_offset)
    sc = scope or UNRESTRICTED
    if sc.empty:
        return _empty_payload(since, until, tz)

    key = f"{since // _DAY}:{until // _DAY}:{tz}:{sc.token()}"
    now = time.monotonic()
    with _LOCK:
        hit = _CACHE.get(key)
        if hit and hit[0] > now:
            return hit[1]

    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "range": {"since": since, "until": until, "tz_offset": tz}}
    try:
        payload = _compute(c, since, until, tz, sc)
    except Exception:
        return {"ok": False, "error": "internal_error", "range": {"since": since, "until": until, "tz_offset": tz}}

    with _LOCK:
        if len(_CACHE) >= _CACHE_CAP:
            for stale in sorted(_CACHE, key=lambda k: _CACHE[k][0])[: _CACHE_CAP // 2]:
                _CACHE.pop(stale, None)
        _CACHE[key] = (now + _CACHE_TTL, payload)
    return payload


def invalidate() -> None:
    with _LOCK:
        _CACHE.clear()


def _run_pair_rows(conn: Any, run_ids: list[str], scope: Scope) -> dict[str, list[dict[str, Any]]]:
    out: dict[str, list[dict[str, Any]]] = {}
    if not run_ids:
        return out
    placeholders = ",".join("?" for _ in run_ids)
    where, scope_params = ("1", []) if scope.unrestricted else scope.clause()
    rows = conn.execute(
        f"SELECT p.run_id, p.pair_id, p.pair_key, p.feature, p.src_provider, p.src_instance, "
        f"p.dst_provider, p.dst_instance, p.added, p.removed, p.updated, p.errors "
        f"FROM run_pairs p WHERE p.run_id IN ({placeholders}) AND {where}",
        [*run_ids, *scope_params],
    ).fetchall()
    for row in rows:
        item = dict(row)
        item["changes"] = int(item["added"] or 0) + int(item["removed"] or 0) + int(item["updated"] or 0)
        out.setdefault(str(item.pop("run_id")), []).append(item)
    for entries in out.values():
        entries.sort(key=lambda e: (-int(e["changes"]), str(e["feature"])))
    return out


def day_runs(*, since: int, until: int, scope: Scope | None = None, limit: int = _MAX_RUNS, conn: Any = None) -> dict[str, Any]:
    until = int(until)
    since = int(min(since, until))
    cap = max(1, min(int(limit or _MAX_RUNS), _MAX_RUNS))
    sc = scope or UNRESTRICTED
    if sc.empty:
        return {"ok": True, "range": {"since": since, "until": until}, "runs": [], "total": 0}

    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "range": {"since": since, "until": until}}

    try:
        if sc.unrestricted:
            rows = c.execute(
                "SELECT run_id, started_at, finished_at, mode, dry_run, status, pairs, "
                "added, removed, updated, unresolved, blocked, errors FROM sync_runs "
                "WHERE started_at>=? AND started_at<? ORDER BY started_at DESC LIMIT ?",
                (since, until, cap),
            ).fetchall()
        else:
            where, scope_params = sc.clause()
            rows = c.execute(
                "SELECT r.run_id, r.started_at, r.finished_at, r.mode, r.dry_run, r.status, "
                "COUNT(DISTINCT p.pair_id || '\x1f' || p.scope_key) pairs, "
                "SUM(p.added) added, SUM(p.removed) removed, SUM(p.updated) updated, "
                "0 unresolved, 0 blocked, SUM(p.errors) errors "
                "FROM sync_runs r JOIN run_pairs p ON p.run_id=r.run_id "
                f"WHERE r.started_at>=? AND r.started_at<? AND {where} "
                "GROUP BY r.run_id ORDER BY r.started_at DESC LIMIT ?",
                (since, until, *scope_params, cap),
            ).fetchall()

        runs = [dict(row) for row in rows]
        pair_map = _run_pair_rows(c, [str(r["run_id"]) for r in runs], sc)
    except Exception:
        return {"ok": False, "error": "internal_error", "range": {"since": since, "until": until}}

    for run in runs:
        started = int(run.get("started_at") or 0)
        finished = int(run.get("finished_at") or 0)
        run["duration"] = finished - started if finished and finished >= started else None
        run["dry_run"] = bool(run.get("dry_run"))
        for key in ("added", "removed", "updated", "errors", "unresolved", "blocked", "pairs"):
            run[key] = int(run.get(key) or 0)
        run["changes"] = run["added"] + run["removed"] + run["updated"]
        run["pair_rows"] = pair_map.get(str(run.get("run_id")), [])

    return {"ok": True, "range": {"since": since, "until": until}, "runs": runs, "total": len(runs)}

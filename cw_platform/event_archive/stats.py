# cw_platform/event_archive/stats.py
# CrossWatch - Operational statistics aggregation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import math
import threading
import time
from typing import Any

from .db import get_conn
from ..reason_labels import friendly_reason

_HOUR = 3600
_DAY = 86400
_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}
_CACHE_TTL = 30.0
_CACHE_CAP = 48
_LOCK = threading.RLock()

_OUTCOME_LABEL = {"completed": "Completed", "warning": "Warning", "failed": "Failed", "active": "In progress"}
_PROBLEM = "('write_failed','unresolved_recorded')"


def default_bucket(span: int) -> int:
    if span <= 2 * _DAY:
        return _HOUR
    if span <= 120 * _DAY:
        return _DAY
    return 7 * _DAY


def _delta(cur: Any, prev: Any) -> float | None:
    cur = float(cur or 0)
    prev = float(prev or 0)
    if prev <= 0:
        return None if cur == 0 else 100.0
    return round((cur - prev) / prev * 100.0, 1)


def _kpi(cur: Any, prev: Any) -> dict[str, Any]:
    return {"value": round(float(cur or 0), 2) if isinstance(cur, float) else int(cur or 0), "prev": int(prev or 0) if not isinstance(prev, float) else round(float(prev or 0), 2), "delta": _delta(cur, prev)}


def _blocked_items(conn: Any, since: int, until: int) -> int:
    row = conn.execute(
        """
        WITH bb AS (
          SELECT pair_key, run_id, item_key, created_at FROM events
          WHERE event_type IN ('blackbox_promoted','blackbox_blocked')
            AND created_at>=? AND created_at<?
            AND pair_key IS NOT NULL AND pair_key<>''
            AND run_id IS NOT NULL AND run_id<>''
            AND item_key IS NOT NULL AND item_key<>''
        ),
        latest AS (SELECT pair_key, MAX(created_at) mx FROM bb GROUP BY pair_key),
        latest_run AS (
          SELECT DISTINCT bb.pair_key, bb.run_id FROM bb
          JOIN latest ON bb.pair_key=latest.pair_key AND bb.created_at=latest.mx
        )
        SELECT COUNT(*) FROM (
          SELECT DISTINCT bb.pair_key, bb.item_key FROM bb
          JOIN latest_run lr ON bb.pair_key=lr.pair_key AND bb.run_id=lr.run_id
        )
        """,
        (int(since), int(until)),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _percentile(conn: Any, since: int, until: int, p: float) -> int | None:
    n = int(conn.execute(
        "SELECT COUNT(*) FROM sync_runs WHERE started_at>=? AND started_at<? AND finished_at IS NOT NULL AND finished_at>=started_at",
        (since, until),
    ).fetchone()[0])
    if n <= 0:
        return None
    off = min(n - 1, int(math.floor(p * (n - 1))))
    row = conn.execute(
        "SELECT finished_at-started_at FROM sync_runs WHERE started_at>=? AND started_at<? "
        "AND finished_at IS NOT NULL AND finished_at>=started_at ORDER BY finished_at-started_at LIMIT 1 OFFSET ?",
        (since, until, off),
    ).fetchone()
    return int(row[0]) if row else None


def _compute(conn: Any, since: int, until: int, bucket: int) -> dict[str, Any]:
    span = max(1, until - since)
    prev_since = since - span
    p = {"s": since, "u": until, "ps": prev_since, "bs": max(60, int(bucket))}

    ev = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN created_at>=:s AND event_type='scrobble_completed' THEN 1 END),0) scrob_c,
          COALESCE(SUM(CASE WHEN created_at< :s AND event_type='scrobble_completed' THEN 1 END),0) scrob_p,
          COALESCE(SUM(CASE WHEN created_at>=:s AND event_type IN ('scrobble_completed','rating_applied') THEN 1 END),0) dok_c,
          COALESCE(SUM(CASE WHEN created_at< :s AND event_type IN ('scrobble_completed','rating_applied') THEN 1 END),0) dok_p,
          COALESCE(SUM(CASE WHEN created_at>=:s AND event_type IN ('scrobble_failed','rating_failed') THEN 1 END),0) dfail_c,
          COALESCE(SUM(CASE WHEN created_at< :s AND event_type IN ('scrobble_failed','rating_failed') THEN 1 END),0) dfail_p,
          COALESCE(SUM(CASE WHEN created_at>=:s AND domain='scrobble' AND source_kind='webhook' THEN 1 END),0) wh_c,
          COALESCE(SUM(CASE WHEN created_at>=:s AND domain='scrobble' AND source_kind<>'webhook' THEN 1 END),0) watch_c
        FROM events WHERE created_at>=:ps AND created_at<:u
        """,
        p,
    ).fetchone()

    rn = conn.execute(
        """
        SELECT
          COALESCE(SUM(CASE WHEN started_at>=:s THEN 1 END),0) runs_c,
          COALESCE(SUM(CASE WHEN started_at< :s THEN 1 END),0) runs_p,
          AVG(CASE WHEN started_at>=:s AND finished_at IS NOT NULL AND finished_at>=started_at THEN finished_at-started_at END) dur_c,
          AVG(CASE WHEN started_at< :s AND finished_at IS NOT NULL AND finished_at>=started_at THEN finished_at-started_at END) dur_p
        FROM sync_runs WHERE started_at>=:ps AND started_at<:u
        """,
        p,
    ).fetchone()

    def _run_states(a: int, b: int) -> tuple[int, int]:
        err = {row[0] for row in conn.execute(
            "SELECT run_id FROM sync_runs WHERE started_at>=? AND started_at<? AND COALESCE(errors,0)>0", (a, b))}
        prob = {row[0] for row in conn.execute(
            f"SELECT DISTINCT run_id FROM events WHERE created_at>=? AND created_at<? "
            f"AND run_id IS NOT NULL AND run_id<>'' AND event_type IN {_PROBLEM}", (a, b))}
        return len(err), len(prob - err)

    def _rate(ok: Any, tot: Any) -> float:
        ok, tot = float(ok or 0), float(tot or 0)
        return round(ok / tot * 100.0, 1) if tot > 0 else 0.0

    runs_c, runs_p = int(rn["runs_c"]), int(rn["runs_p"])
    failrun_c, warnrun_c = _run_states(since, until)
    failrun_p, warnrun_p = _run_states(prev_since, since)
    okrun_c = max(0, runs_c - failrun_c - warnrun_c)
    okrun_p = max(0, runs_p - failrun_p - warnrun_p)
    dok_c, dfail_c = int(ev["dok_c"]), int(ev["dfail_c"])
    dok_p, dfail_p = int(ev["dok_p"]), int(ev["dfail_p"])
    ops_c, ops_p = runs_c + dok_c + dfail_c, runs_p + dok_p + dfail_p
    okops_c, okops_p = okrun_c + dok_c, okrun_p + dok_p

    kpis = {
        "sync_runs": _kpi(runs_c, runs_p),
        "scrobbles": _kpi(ev["scrob_c"], ev["scrob_p"]),
        "avg_duration": _kpi(round(float(rn["dur_c"] or 0), 2), round(float(rn["dur_p"] or 0), 2)),
        "failures": _kpi(failrun_c + warnrun_c + dfail_c, failrun_p + warnrun_p + dfail_p),
        "blocked": _kpi(_blocked_items(conn, since, until), _blocked_items(conn, prev_since, since)),
        "success_rate": _kpi(_rate(okops_c, ops_c), _rate(okops_p, ops_p)),
    }

    trend_rows = conn.execute(
        """
        SELECT created_at/:bs AS b,
          COALESCE(SUM(CASE WHEN event_type='sync_run_finished' THEN 1 END),0) sync,
          COALESCE(SUM(CASE WHEN event_type IN ('scrobble_completed','rating_applied') AND source_kind='webhook' THEN 1 END),0) webhook,
          COALESCE(SUM(CASE WHEN event_type IN ('scrobble_completed','rating_applied') AND source_kind<>'webhook' AND domain='scrobble' THEN 1 END),0) watcher,
          COALESCE(SUM(CASE WHEN event_type IN ('scrobble_failed','rating_failed') THEN 1 END),0) sfail
        FROM events WHERE created_at>=:s AND created_at<:u GROUP BY b
        """,
        p,
    ).fetchall()
    frun_rows = conn.execute(
        "SELECT started_at/:bs AS b, COALESCE(SUM(CASE WHEN COALESCE(errors,0)>0 THEN 1 END),0) frun "
        "FROM sync_runs WHERE started_at>=:s AND started_at<:u GROUP BY b",
        p,
    ).fetchall()
    frun_by_b = {int(r["b"]): int(r["frun"]) for r in frun_rows}
    tmap: dict[int, dict[str, Any]] = {}
    for r in trend_rows:
        b = int(r["b"])
        failed = int(r["sfail"]) + frun_by_b.get(b, 0)
        ok = int(r["sync"]) + int(r["webhook"]) + int(r["watcher"])
        tot = ok + failed
        tmap[b] = {"t": b * p["bs"], "sync": int(r["sync"]), "webhook": int(r["webhook"]),
                   "watcher": int(r["watcher"]), "failed": failed, "rate": round(ok / tot * 100.0, 1) if tot > 0 else None}
    for b, frun in frun_by_b.items():
        if b not in tmap and frun:
            tmap[b] = {"t": b * p["bs"], "sync": 0, "webhook": 0, "watcher": 0, "failed": frun, "rate": 0.0}
    trend = [tmap[b] for b in sorted(tmap)]

    dur_rows = conn.execute(
        """
        SELECT started_at/:bs AS b, AVG(finished_at-started_at) avg, COUNT(*) n
        FROM sync_runs WHERE started_at>=:s AND started_at<:u AND finished_at IS NOT NULL AND finished_at>=started_at
        GROUP BY b ORDER BY b
        """,
        p,
    ).fetchall()
    duration_series = [{"t": int(r["b"]) * p["bs"], "avg": round(float(r["avg"] or 0), 2), "n": int(r["n"])} for r in dur_rows]

    sc_rows = conn.execute(
        "SELECT status, COUNT(*) c FROM event_groups WHERE domain='scrobble' AND last_event_at>=:s AND last_event_at<:u GROUP BY status",
        p,
    ).fetchall()
    oc = {"completed": okrun_c, "warning": warnrun_c, "failed": failrun_c, "active": 0}
    for r in sc_rows:
        st = str(r["status"] or "").lower()
        c = int(r["c"])
        if st in ("completed", "rated"):
            oc["completed"] += c
        elif st == "warning":
            oc["warning"] += c
        elif st == "failed":
            oc["failed"] += c
        elif st == "running":
            oc["active"] += c
    outcomes = [{"key": k, "label": _OUTCOME_LABEL[k], "value": oc[k]} for k in ("completed", "warning", "failed", "active") if oc.get(k)]

    types = [
        {"key": "sync", "label": "Sync runs", "value": runs_c},
        {"key": "watcher", "label": "Watcher", "value": int(ev["watch_c"])},
        {"key": "webhook", "label": "Webhooks", "value": int(ev["wh_c"])},
    ]

    rmap: dict[tuple[Any, Any], dict[str, int]] = {}
    for r in conn.execute(
        "SELECT source_provider src, destination_provider dst, COUNT(DISTINCT run_id) runs, "
        f"COUNT(DISTINCT CASE WHEN event_type IN {_PROBLEM} THEN run_id END) failed_runs "
        "FROM events WHERE created_at>=:s AND created_at<:u AND run_id IS NOT NULL AND run_id<>'' "
        "AND destination_provider IS NOT NULL AND destination_provider<>'' GROUP BY src, dst",
        p,
    ).fetchall():
        e = rmap.setdefault((r["src"], r["dst"]), {"runs": 0, "sessions": 0, "ok": 0, "bad": 0})
        runs, fr = int(r["runs"]), int(r["failed_runs"])
        e["runs"] += runs
        e["ok"] += runs - fr
        e["bad"] += fr
    for r in conn.execute(
        "SELECT source_provider src, destination_provider dst, COUNT(*) sessions, "
        "SUM(CASE WHEN status IN ('completed','rated') THEN 1 ELSE 0 END) ok, "
        "SUM(CASE WHEN status='failed' THEN 1 ELSE 0 END) bad "
        "FROM event_groups WHERE domain='scrobble' AND last_event_at>=:s AND last_event_at<:u "
        "AND destination_provider IS NOT NULL AND destination_provider<>'' GROUP BY src, dst",
        p,
    ).fetchall():
        e = rmap.setdefault((r["src"], r["dst"]), {"runs": 0, "sessions": 0, "ok": 0, "bad": 0})
        e["sessions"] += int(r["sessions"])
        e["ok"] += int(r["ok"])
        e["bad"] += int(r["bad"])
    routes = []
    for (src, dst), e in rmap.items():
        volume = e["runs"] + e["sessions"]
        if volume <= 0:
            continue
        okb = e["ok"] + e["bad"]
        routes.append({
            "source": src, "destination": dst, "volume": volume,
            "runs": e["runs"], "scrobbles": e["sessions"],
            "success_rate": round(e["ok"] / okb * 100.0, 1) if okb else None,
        })
    routes.sort(key=lambda x: x["volume"], reverse=True)
    routes = routes[:8]

    def _fail_map(a: int, b: int) -> dict[str, int]:
        rows = conn.execute(
            "SELECT reason_code, COUNT(*) c FROM events WHERE created_at>=? AND created_at<? "
            "AND severity='error' AND reason_code IS NOT NULL AND reason_code<>'' GROUP BY reason_code ORDER BY c DESC LIMIT 12",
            (a, b),
        ).fetchall()
        return {str(r["reason_code"]): int(r["c"]) for r in rows}

    cur_fails = _fail_map(since, until)
    prev_fails = _fail_map(prev_since, since)
    total_fail = sum(cur_fails.values()) or 1
    failure_reasons = [{
        "reason": k, "label": friendly_reason(k), "count": v, "prev": prev_fails.get(k, 0),
        "share": round(v / total_fail * 100.0, 1), "delta": _delta(v, prev_fails.get(k, 0)),
    } for k, v in sorted(cur_fails.items(), key=lambda x: x[1], reverse=True)[:8]]

    percentiles = {
        "p50": _percentile(conn, since, until, 0.5),
        "p90": _percentile(conn, since, until, 0.9),
        "p99": _percentile(conn, since, until, 0.99),
    }

    return {
        "ok": True,
        "range": {"since": since, "until": until, "bucket": p["bs"]},
        "kpis": kpis,
        "trend": trend,
        "duration_series": duration_series,
        "duration_percentiles": percentiles,
        "outcomes": outcomes,
        "types": types,
        "routes": routes,
        "failure_reasons": failure_reasons,
    }


def statistics(*, since: int, until: int, bucket: int | None = None, conn: Any = None) -> dict[str, Any]:
    until = int(until)
    since = int(min(since, until))
    bs = int(bucket) if bucket else default_bucket(until - since)
    ck = f"{since - since % bs}:{until - until % bs}:{bs}"
    now = time.monotonic()
    with _LOCK:
        hit = _CACHE.get(ck)
        if hit and hit[0] > now:
            return hit[1]
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False, "range": {"since": since, "until": until, "bucket": bs}}
    try:
        payload = _compute(c, since, until, bs)
    except Exception:
        return {"ok": False, "error": "internal_error", "range": {"since": since, "until": until, "bucket": bs}}
    with _LOCK:
        if len(_CACHE) >= _CACHE_CAP:
            for k in sorted(_CACHE, key=lambda k: _CACHE[k][0])[: _CACHE_CAP // 2]:
                _CACHE.pop(k, None)
        _CACHE[ck] = (now + _CACHE_TTL, payload)
    return payload

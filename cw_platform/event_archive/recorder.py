# cw_platform/event_archive/recorder.py
# CrossWatch - Runtime event recording
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import logging
import os
import sqlite3
import time
from collections.abc import Iterable, Mapping
from typing import Any

from .db import get_conn

_LOG = logging.getLogger("crosswatch.event_archive")

FIELDS = (
    "event_hash", "created_at", "run_id", "event_type", "severity",
    "feature", "operation", "pair_key", "direction",
    "source_provider", "source_instance",
    "destination_provider", "destination_instance",
    "origin_provider", "origin_instance", "origin_confidence",
    "item_key", "title", "year", "media_type", "season", "episode",
    "old_value", "new_value", "value_type",
    "reason_code", "reason", "match_basis",
    "source_kind", "source_file", "source_mtime", "detail",
)

_HASH_FIELDS = (
    "source_file", "source_mtime", "event_type", "item_key",
    "source_provider", "destination_provider", "origin_provider",
    "feature", "pair_key", "operation", "reason_code",
    "old_value", "new_value", "run_id",
)


def _as_map(v: Any) -> Mapping[str, Any]:
    return v if isinstance(v, Mapping) else {}


def _as_list(v: Any) -> list[Any]:
    return v if isinstance(v, list) else []


def _s(v: Any) -> str:
    if v is None:
        return ""
    return str(v)


def compute_event_hash(row: Mapping[str, Any], *, extra: Any = None) -> str:
    parts = [_s(row.get(k)) for k in _HASH_FIELDS]
    if extra is not None:
        parts.append(_s(extra))
    return hashlib.sha256("\x1f".join(parts).encode("utf-8", "replace")).hexdigest()


def make_event(*, hash_extra: Any = None, **kwargs: Any) -> dict[str, Any]:
    row: dict[str, Any] = {k: None for k in FIELDS}
    for k, v in kwargs.items():
        if k in row:
            row[k] = v
    if not row.get("created_at"):
        row["created_at"] = int(time.time())
    if not row.get("severity"):
        row["severity"] = "info"
    detail = row.get("detail")
    if detail is not None and not isinstance(detail, str):
        try:
            row["detail"] = json.dumps(detail, ensure_ascii=False, sort_keys=True)[:4000]
        except Exception:
            row["detail"] = None
    if not row.get("event_hash"):
        row["event_hash"] = compute_event_hash(row, extra=hash_extra)
    return row


def record_events(rows: Iterable[Mapping[str, Any]], *, conn: sqlite3.Connection | None = None) -> int:
    try:
        materialized = [dict(r) for r in (rows or [])]
    except Exception:
        return 0
    if not materialized:
        return 0
    c = conn or get_conn()
    if c is None:
        return 0
    prepared: list[tuple[Any, ...]] = []
    for r in materialized:
        if not r.get("event_hash"):
            r = make_event(**r)
        prepared.append(tuple(r.get(k) for k in FIELDS))
    placeholders = ",".join("?" for _ in FIELDS)
    sql = f"INSERT OR IGNORE INTO events ({','.join(FIELDS)}) VALUES ({placeholders})"
    try:
        with c:
            cur = c.executemany(sql, prepared)
            return int(cur.rowcount if cur.rowcount is not None and cur.rowcount >= 0 else len(prepared))
    except Exception as exc:
        _LOG.warning("event archive write failed: %s", exc)
        return 0


def record_run_started(
    run_id: str,
    *,
    started_at: int | None = None,
    mode: str | None = None,
    dry_run: bool = False,
    conn: sqlite3.Connection | None = None,
) -> None:
    c = conn or get_conn()
    if c is None:
        return
    try:
        with c:
            c.execute(
                "INSERT INTO sync_runs (run_id, started_at, mode, dry_run, status) VALUES (?,?,?,?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET started_at=excluded.started_at, mode=excluded.mode, "
                "dry_run=excluded.dry_run, status=excluded.status",
                (str(run_id), int(started_at or time.time()), mode, 1 if dry_run else 0, "running"),
            )
    except Exception as exc:
        _LOG.warning("event archive run start failed: %s", exc)


def record_run_finished(
    run_id: str,
    *,
    finished_at: int | None = None,
    status: str = "done",
    pairs: int = 0,
    summary: Mapping[str, Any] | None = None,
    conn: sqlite3.Connection | None = None,
) -> None:
    c = conn or get_conn()
    if c is None:
        return
    s = dict(summary or {})
    try:
        with c:
            c.execute(
                "INSERT INTO sync_runs (run_id, finished_at, status, pairs, added, removed, updated, unresolved, blocked, errors, summary) "
                "VALUES (?,?,?,?,?,?,?,?,?,?,?) "
                "ON CONFLICT(run_id) DO UPDATE SET finished_at=excluded.finished_at, status=excluded.status, "
                "pairs=excluded.pairs, added=excluded.added, removed=excluded.removed, updated=excluded.updated, "
                "unresolved=excluded.unresolved, blocked=excluded.blocked, errors=excluded.errors, summary=excluded.summary",
                (
                    str(run_id), int(finished_at or time.time()), status, int(pairs),
                    int(s.get("added") or 0), int(s.get("removed") or 0), int(s.get("updated") or 0),
                    int(s.get("unresolved") or 0), int(s.get("blocked") or 0), int(s.get("errors") or 0),
                    json.dumps(s, ensure_ascii=False, sort_keys=True)[:4000] if s else None,
                ),
            )
    except Exception as exc:
        _LOG.warning("event archive run finish failed: %s", exc)


def _norm_prov(v: Any) -> str:
    return str(v or "").strip().upper()


def _pair_key(a: str, b: str) -> str:
    return "-".join(sorted([_norm_prov(a), _norm_prov(b)]))


class RunRecorder:

    def __init__(self, inner_emit, *, run_id: str, conn: sqlite3.Connection | None = None):
        self._inner = inner_emit
        self._run_id = str(run_id)
        self._conn = conn
        self._buf: list[dict[str, Any]] = []
        self._src = ""
        self._dst = ""
        self._si = ""
        self._di = ""
        self._feature = ""
        self._pair = ""
        self._a = ""
        self._b = ""
        self._two = False
        self._started = False

    def emit(self, event: str, **fields: Any):
        try:
            return self._inner(event, **fields)
        finally:
            try:
                self._observe(str(event or ""), fields)
            except Exception:
                pass

    def _flush(self, force: bool = False) -> None:
        if not self._buf:
            return
        if not force and len(self._buf) < 100:
            return
        batch, self._buf = self._buf, []
        record_events(batch, conn=self._conn)

    def _ctx_providers(self) -> tuple[str, str]:
        if self._two:
            return self._a, self._b
        return self._src, self._dst

    def _add(self, **kw: Any) -> None:
        kw.setdefault("run_id", self._run_id)
        kw.setdefault("feature", self._feature or None)
        kw.setdefault("pair_key", self._pair or None)
        kw.setdefault("source_kind", "runtime")
        self._buf.append(make_event(hash_extra=len(self._buf), **kw))

    def _observe(self, event: str, f: Mapping[str, Any]) -> None:
        if event == "run:start":
            self._started = True
            record_run_started(
                self._run_id,
                mode=f.get("mode"),
                dry_run=bool(f.get("dry_run")),
                conn=self._conn,
            )
            self._add(event_type="sync_run_started", operation="run", detail=dict(f))
            return

        if event == "run:done":
            self._add(event_type="sync_run_finished", operation="run", detail=dict(f))
            self._flush(force=True)
            record_run_finished(
                self._run_id,
                status="done",
                pairs=int(f.get("pairs") or 0),
                summary={k: f.get(k) for k in ("added", "removed", "updated", "unresolved", "blocked", "errors")},
                conn=self._conn,
            )
            return

        if event in ("health", "provider_health"):
            self._add(
                event_type="provider_health",
                source_provider=_norm_prov(f.get("provider")),
                source_instance=f.get("instance"),
                severity="warn" if str(f.get("status")) not in ("ok", "up") else "info",
                reason_code=str(f.get("status") or ""),
                detail={k: f.get(k) for k in ("status", "ok", "latency_ms", "features")},
            )
            self._flush()
            return

        if event == "feature:start":
            self._two = False
            self._src = _norm_prov(f.get("src"))
            self._dst = _norm_prov(f.get("dst"))
            self._si = os.environ.get("CW_PAIR_SRC_INSTANCE") or ""
            self._di = os.environ.get("CW_PAIR_DST_INSTANCE") or ""
            self._feature = str(f.get("feature") or "")
            self._pair = _pair_key(self._src, self._dst)
            return

        if event == "two:start":
            self._two = True
            self._a = _norm_prov(f.get("a"))
            self._b = _norm_prov(f.get("b"))
            self._si = os.environ.get("CW_PAIR_SRC_INSTANCE") or ""
            self._di = os.environ.get("CW_PAIR_DST_INSTANCE") or ""
            self._feature = str(f.get("feature") or "")
            self._pair = _pair_key(self._a, self._b)
            return

        if event in ("one:plan", "two:plan"):
            src, dst = self._ctx_providers()
            self._add(
                event_type="plan_created",
                operation="plan",
                source_provider=src or None,
                destination_provider=dst or None,
                detail={k: f.get(k) for k in ("adds", "removes", "updates", "src_count", "dst_count")},
            )
            self._flush()
            return

        if event == "apply:unresolved":
            dst = _norm_prov(f.get("provider")) or self._dst
            src = self._src if self._src and self._src != dst else self._a
            for it in (f.get("items") or []):
                if not isinstance(it, Mapping):
                    continue
                self._add(
                    event_type="unresolved_recorded",
                    operation="add",
                    severity="warn",
                    source_provider=src or None,
                    destination_provider=dst or None,
                    source_instance=self._si or None,
                    destination_instance=self._di or None,
                    reason_code=str(it.get("reason") or ""),
                    reason=str(it.get("reason") or ""),
                    **_item_fields(it),
                )
            self._flush()
            return

        if event == "ui:spotlight":
            return

        if event.endswith("apply:add:done") or event.endswith("apply:remove:done") or event.endswith("apply:update:done"):
            op = "add" if "add" in event else ("remove" if "remove" in event else "update")
            dst = _norm_prov(f.get("dst")) or self._dst
            src, ctx_dst = self._ctx_providers()
            if not dst:
                dst = ctx_dst
            src_eff = src if src and src != dst else (self._b if dst == self._a else self._a)
            attempted = int(f.get("attempted") or 0)
            errors = int(f.get("errors") or 0)
            self._add(
                event_type="write_attempted",
                operation=op,
                severity="warn" if errors else "info",
                source_provider=src_eff or None,
                destination_provider=dst or None,
                reason_code="errors" if errors else None,
                detail={k: f.get(k) for k in ("attempted", "count", "added", "removed", "updated", "skipped", "unresolved", "errors") if k in f},
            )
            if errors and attempted:
                self._add(
                    event_type="write_failed",
                    operation=op,
                    severity="error",
                    source_provider=src_eff or None,
                    destination_provider=dst or None,
                    reason_code="errors",
                )
            self._flush()
            return

        if event == "archive:item_failures":
            dst = _norm_prov(f.get("provider")) or self._dst
            src = self._src if self._src and self._src != dst else self._a
            op = str(f.get("op") or "add")
            for row in (f.get("items") or []):
                if not isinstance(row, Mapping):
                    continue
                k = str(row.get("key") or "")
                if not k:
                    continue
                raw_it = row.get("item")
                it: Mapping[str, Any] = raw_it if isinstance(raw_it, Mapping) else {}
                reason = str(row.get("reason") or "apply:add:failed")
                base = dict(
                    source_provider=src or None, destination_provider=dst or None,
                    source_instance=self._si or None, destination_instance=self._di or None,
                    item_key=k, **_item_fields(it),
                )
                self._add(event_type="write_failed", operation=op, severity="error",
                          reason_code=reason, reason=reason, **base)
                self._add(event_type="unresolved_recorded", operation=op, severity="warn",
                          reason_code=reason, reason=reason, **base)
                if row.get("promoted"):
                    self._add(event_type="blackbox_promoted", operation="quarantine", severity="warn",
                              reason_code="flapper", reason="flapper", **base)
            self._flush()
            return

        if event == "debug" and str(f.get("msg") or "") == "blocked.counts":
            bb = int(f.get("blocked_blackbox") or 0)
            dst_p = _norm_prov(f.get("dst")) or None
            pair_k = str(f.get("pair") or self._pair) or None
            rows = f.get("blackbox_items") or []
            if bb > 0:
                self._add(
                    event_type="blackbox_blocked",
                    severity="warn",
                    destination_provider=dst_p,
                    pair_key=pair_k,
                    reason_code="blackbox",
                    detail={"blocked": bb},
                )
            for row in rows:
                if not isinstance(row, Mapping):
                    continue
                k = str(row.get("key") or "")
                if not k:
                    continue
                raw_it = row.get("item")
                it: Mapping[str, Any] = raw_it if isinstance(raw_it, Mapping) else {}
                self._add(
                    event_type="blackbox_blocked", operation="add", severity="warn",
                    destination_provider=dst_p, pair_key=pair_k, item_key=k,
                    reason_code="blackbox", reason="blackbox", **_item_fields(it),
                )
            if bb > 0 or rows:
                self._flush()
            return

    def close(self) -> None:
        try:
            self._flush(force=True)
        except Exception:
            pass
        try:
            from .groups import correlate
            correlate(conn=self._conn)
        except Exception as exc:
            _LOG.warning("event correlation after run failed: %s", exc)


def _item_fields(it: Mapping[str, Any]) -> dict[str, Any]:
    ids = _as_map(it.get("ids"))
    show_ids = _as_map(it.get("show_ids"))
    year = it.get("year")
    try:
        year = int(year) if year not in (None, "") else None
    except Exception:
        year = None
    season = it.get("season")
    episode = it.get("episode")
    try:
        season = int(season) if season not in (None, "") else None
    except Exception:
        season = None
    try:
        episode = int(episode) if episode not in (None, "") else None
    except Exception:
        episode = None
    is_ep = str(it.get("type") or "").lower() == "episode" or (season is not None and episode is not None)
    if is_ep:
        title = (it.get("series_title") or it.get("show_title") or it.get("show")
                 or it.get("grandparent_title") or it.get("title") or it.get("name"))
    else:
        title = it.get("title") or it.get("series_title") or it.get("show_title") or it.get("name")
    return {
        "title": str(title) if title else None,
        "year": year,
        "media_type": str(it.get("type") or "") or None,
        "season": season,
        "episode": episode,
        "match_basis": _first_id_token(ids) or _first_id_token(show_ids),
    }


def _first_id_token(ids: Mapping[str, Any]) -> str | None:
    if not isinstance(ids, Mapping):
        return None
    for k in ("imdb", "tmdb", "tvdb", "trakt", "simkl", "slug"):
        v = ids.get(k)
        if v not in (None, ""):
            return f"{k}:{v}"
    return None

# cw_platform/event_archive/titles.py
# CrossWatch - Per-title activity rows for the Events Titles view
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
from collections.abc import Callable, Iterable, Mapping, Sequence
from typing import Any

from .db import get_conn

_LOG = logging.getLogger("crosswatch.event_archive")

TITLES_META = "event_titles_backfill_version"
TITLES_VERSION = 2
_SESSION_LIMIT = 50
SUCCESS_CAP = 500
_CHUNK = 400
_BACKFILL_BATCH = 4000

SYNC_ITEM_TYPES = (
    "write_succeeded", "write_failed", "write_skipped", "unresolved_recorded",
    "unresolved_cleared", "blackbox_promoted", "blackbox_blocked",
)
_SYNC_STATE = {
    "write_succeeded": "ok",
    "unresolved_cleared": "ok",
    "write_failed": "fail",
    "unresolved_recorded": "fail",
    "write_skipped": "skip",
    "blackbox_promoted": "skip",
    "blackbox_blocked": "skip",
}
_GROUP_STATE = {
    "completed": "ok", "rated": "ok", "resolved": "ok",
    "failed": "fail", "unresolved": "fail", "blackboxed": "skip", "warning": "fail",
    "skipped": "skip", "informational": "skip",
    "running": "pending", "paused": "pending", "stopped": "pending", "pending": "pending",
}
_SAME_HINTS = ("already", "duplicate", "present", "exists", "unchanged")
_PROBLEM_GROUP_STATUSES = ("failed", "unresolved", "blackboxed", "warning")
_ID_NAMES = ("tmdb", "imdb", "tvdb")

TITLE_COLUMNS = (
    "id", "domain", "run_id", "first_event_at", "last_event_at", "event_count", "status", "attention",
    "item_key", "ident", "title", "year", "media_type", "season", "episode",
    "source_provider", "source_instance", "kinds", "destinations", "ok_dests", "ids",
    "rating", "progress", "sessions", "acknowledged_at",
)

_EVENT_FIELDS = (
    "id", "domain", "event_type", "feature", "operation", "created_at", "run_id", "item_key", "title", "year",
    "media_type", "season", "episode", "source_provider", "source_instance", "destination_provider",
    "destination_instance", "reason_code", "reason", "detail", "group_id", "source_kind", "session_key",
    "pair_key", "event_hash",
)

_backfill_lock = threading.Lock()
_backfill_thread: threading.Thread | None = None


def _g(row: Any, key: str) -> Any:
    try:
        return row[key]
    except Exception:
        return None


def _json(value: Any, default: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, str) and value:
        try:
            return json.loads(value)
        except (ValueError, TypeError):
            return default
    return default


def _dump(value: Any) -> str | None:
    return json.dumps(value, separators=(",", ":")) if value else None


def base_key(item_key: Any) -> str:
    return str(item_key or "").strip().lower().split("@", 1)[0]


def ident_of(title: Any, season: Any, episode: Any) -> str:
    name = " ".join(str(title or "").lower().split())
    if not name:
        return ""
    return f"{name}|{'' if season is None else season}|{'' if episode is None else episode}"


def _short_hash(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8", "replace")).hexdigest()[:24]


def _sync_hash(run_id: Any, created_at: Any, item_key: Any) -> str | None:
    key = base_key(item_key)
    if not key:
        return None
    run = str(run_id or "").strip() or f"day:{int(created_at or 0) // 86400}"
    return _short_hash(f"\x00title\x1fsync\x1f{run}\x1f{key}")


def _day_token(created_at: Any) -> str:
    return "d:" + time.strftime("%Y%j", time.localtime(int(created_at or 0)))


def _session_of(row: Any) -> str:
    sk = str(_g(row, "session_key") or "").strip()
    return f"s:{sk}" if sk else _day_token(_g(row, "created_at"))


def title_hash(row: Any) -> str | None:
    domain = str(_g(row, "domain") or "sync").strip().lower()
    if domain == "scrobble":
        if not (str(_g(row, "item_key") or "").strip() or str(_g(row, "title") or "").strip()):
            return None
        from .groups import group_hash

        activity = group_hash({**dict(row), "destination_provider": None, "pair_key": None, "session_key": None})
        return _short_hash(f"\x00title\x1fscrobble\x1f{activity}")
    if domain != "sync" or str(_g(row, "feature") or "").strip().lower() == "playlists":
        return None
    if str(_g(row, "event_type") or "") not in SYNC_ITEM_TYPES:
        return None
    return _sync_hash(_g(row, "run_id"), _g(row, "created_at"), _g(row, "item_key"))


def _in(ids: Sequence[Any]) -> str:
    return ",".join("?" for _ in ids)


def mark_dirty_for_groups(conn: sqlite3.Connection, group_ids: Iterable[Any]) -> None:
    gids = [int(g) for g in group_ids if g is not None]
    for i in range(0, len(gids), _CHUNK):
        part = gids[i:i + _CHUNK]
        conn.execute(
            f"UPDATE event_titles SET dirty=1 WHERE id IN (SELECT DISTINCT title_id FROM events "
            f"WHERE group_id IN ({_in(part)}) AND title_id>0)",
            part,
        )


def mark_dirty_for_events(conn: sqlite3.Connection, event_ids: Iterable[Any]) -> None:
    ids = [int(e) for e in event_ids if e is not None]
    for i in range(0, len(ids), _CHUNK):
        part = ids[i:i + _CHUNK]
        conn.execute(
            f"UPDATE event_titles SET dirty=1 WHERE id IN (SELECT DISTINCT title_id FROM events "
            f"WHERE id IN ({_in(part)}) AND title_id>0)",
            part,
        )


def _title_id(conn: sqlite3.Connection, th: str, domain: str, now: int) -> int:
    conn.execute(
        "INSERT INTO event_titles (title_hash, domain, created_at, dirty) VALUES (?,?,?,1) "
        "ON CONFLICT(title_hash) DO UPDATE SET dirty=1",
        (th, domain, now),
    )
    return int(conn.execute("SELECT id FROM event_titles WHERE title_hash=?", (th,)).fetchone()[0])


def _scrobble_title(conn: sqlite3.Connection, row: Any, base: str, now: int, seen: dict[int, int]) -> int:
    gid = _g(row, "group_id")
    if gid is not None:
        if int(gid) in seen:
            return seen[int(gid)]
        prior = conn.execute(
            "SELECT title_id FROM events WHERE group_id=? AND title_id>0 LIMIT 1",
            (int(gid),),
        ).fetchone()
        if prior is not None:
            seen[int(gid)] = int(prior[0])
            return int(prior[0])
    session = _session_of(row)
    created = int(_g(row, "created_at") or 0)
    th = base
    for cycle in range(2, 50):
        hit = conn.execute("SELECT id FROM event_titles WHERE title_hash=?", (th,)).fetchone()
        if hit is None:
            break
        finished = conn.execute(
            "SELECT session_key, created_at FROM events WHERE title_id=? AND +event_type='scrobble_completed' AND +created_at<=?",
            (int(hit[0]), created),
        ).fetchall()
        if not any(_session_of(r) != session for r in finished):
            break
        th = _short_hash(f"{base}\x1f{cycle}")
    tid = _title_id(conn, th, "scrobble", now)
    if gid is not None:
        seen[int(gid)] = tid
    return tid


def assign(conn: sqlite3.Connection, event_ids: Sequence[int]) -> set[int]:
    touched: set[int] = set()
    now = int(time.time())
    for i in range(0, len(event_ids), _CHUNK):
        part = list(event_ids[i:i + _CHUNK])
        rows = conn.execute(
            f"SELECT {','.join(_EVENT_FIELDS)} FROM events WHERE id IN ({_in(part)}) ORDER BY created_at, id", part,
        ).fetchall()
        none_ids: list[tuple[int]] = []
        by_hash: dict[str, tuple[str, list[int]]] = {}
        seen: dict[int, int] = {}
        for row in rows:
            th = title_hash(row)
            if th is None:
                none_ids.append((int(row["id"]),))
            elif str(row["domain"] or "") == "scrobble":
                tid = _scrobble_title(conn, row, th, now, seen)
                conn.execute("UPDATE events SET title_id=? WHERE id=?", (tid, int(row["id"])))
                touched.add(tid)
            else:
                by_hash.setdefault(th, (str(row["domain"] or "sync"), []))[1].append(int(row["id"]))
        if none_ids:
            conn.executemany("UPDATE events SET title_id=0 WHERE id=?", none_ids)
        for th, (domain, ids) in by_hash.items():
            tid = _title_id(conn, th, domain, now)
            conn.executemany("UPDATE events SET title_id=? WHERE id=?", [(tid, e) for e in ids])
            touched.add(tid)
    return touched


def _ok_entries(raw: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for entry in _json(raw, []) or []:
        if isinstance(entry, list) and len(entry) >= 5:
            out.append({"provider": entry[0], "instance": entry[1] or "default", "feature": entry[2], "op": entry[3], "at": entry[4]})
    return out


def _ok_pack(entries: Sequence[Mapping[str, Any]]) -> str | None:
    packed = [
        [e["provider"], "" if e.get("instance") in (None, "", "default") else e["instance"], e.get("feature") or "", e.get("op") or "add", int(e.get("at") or 0)]
        for e in entries
    ]
    return _dump(packed)


def _dest_key(d: Mapping[str, Any]) -> tuple[str, str, str]:
    return (str(d.get("provider") or ""), str(d.get("instance") or "default"), str(d.get("feature") or ""))


def _merged(stored: Sequence[Mapping[str, Any]], oks: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out = {_dest_key(d): dict(d) for d in stored}
    for d in oks:
        out.setdefault(_dest_key(d), {**d, "state": "ok"})
    return sorted(out.values(), key=_dest_key)


def compact_ids(ids: Any, show_ids: Any = None) -> dict[str, str]:
    out: dict[str, str] = {}
    own = ids if isinstance(ids, Mapping) else {}
    show = show_ids if isinstance(show_ids, Mapping) else {}
    for name in _ID_NAMES:
        for key, value in ((name, own.get(name)), (f"show_{name}", own.get(f"show_{name}") or own.get(f"{name}_show") or show.get(name))):
            if value not in (None, "") and key not in out:
                out[key] = str(value)[:40]
    return out


def _routes(entries: Iterable[Mapping[str, Any]]) -> str | None:
    keys = sorted({f"{str(d.get('provider') or '').upper()}/{str(d.get('instance') or 'default')}" for d in entries if d.get("provider")})
    return "|" + "|".join(keys) + "|" if keys else None


def record_successes(
    conn: sqlite3.Connection | None,
    *,
    run_id: str,
    feature: str,
    op: str,
    source: tuple[str, str],
    destination: tuple[str, str],
    items: Sequence[Mapping[str, Any]],
    at: int | None = None,
) -> int:
    c = conn or get_conn()
    if c is None or not items or str(feature).strip().lower() == "playlists":
        return 0
    from .groups import _GROUP_LOCK

    touched: list[int] = []

    now = int(at or time.time())
    dst, di = str(destination[0] or "").upper(), str(destination[1] or "default")
    sp, si = str(source[0] or "").upper(), str(source[1] or "default")
    feat = str(feature or "").strip().lower()

    def write() -> int:
        done = 0
        for item in items[:SUCCESS_CAP]:
            th = _sync_hash(run_id, now, item.get("key"))
            if th is None:
                continue
            tid = _title_id(c, th, "sync", now)
            touched.append(tid)
            row = c.execute(
                "SELECT ok_dests, ids, title, source_provider FROM event_titles WHERE id=?", (tid,)
            ).fetchone()
            entry = {"provider": dst, "instance": di, "feature": feat, "op": op, "at": now}
            oks = [d for d in _ok_entries(row["ok_dests"]) if _dest_key(d) != _dest_key(entry)]
            oks.append(entry)
            ids = _json(row["ids"], {}) or {}
            for key, value in compact_ids(item.get("ids")).items():
                ids.setdefault(key, value)
            season, episode = item.get("season"), item.get("episode")
            c.execute(
                "UPDATE event_titles SET ok_dests=?, ids=?, run_id=COALESCE(run_id, ?), "
                "item_key=COALESCE(item_key, ?), title=COALESCE(title, ?), year=COALESCE(year, ?), "
                "media_type=COALESCE(media_type, ?), season=COALESCE(season, ?), episode=COALESCE(episode, ?), "
                "ident=COALESCE(ident, ?), source_provider=COALESCE(source_provider, ?), "
                "source_instance=COALESCE(source_instance, ?), rating=COALESCE(?, rating), dirty=1 WHERE id=?",
                (
                    _ok_pack(oks), _dump(ids), run_id or None, base_key(item.get("key")) or None, item.get("title"),
                    item.get("year"), item.get("media_type"), season, episode,
                    ident_of(item.get("title"), season, episode) or None, sp or None, si if sp else None,
                    None if item.get("rating") in (None, "") else str(item.get("rating"))[:16], tid,
                ),
            )
            done += 1
        return done

    with _GROUP_LOCK:
        if c.in_transaction:
            done = write()
            for tid in dict.fromkeys(touched):
                recompute(c, tid, now)
        else:
            with c:
                done = write()
                for tid in dict.fromkeys(touched):
                    recompute(c, tid, now)
    return done


def _state_for(status: str, reason: str) -> str:
    state = _GROUP_STATE.get(status, "skip")
    if state == "skip" and any(h in reason for h in _SAME_HINTS):
        return "same"
    return state


def _merge_ids(target: dict[str, str], detail: dict[str, Any]) -> None:
    for key, value in compact_ids(detail.get("ids")).items():
        target.setdefault(key, value)


def _ids_from_key(target: dict[str, str], item_key: str) -> None:
    head, sep, _ = item_key.partition("#")
    ns, _, value = head.partition(":")
    ns = ns.split("_", 1)[0]
    if ns not in ("tmdb", "imdb", "tvdb") or not value:
        return
    key = f"show_{ns}" if sep else ns
    target.setdefault(key, value)


def _slim(dest: dict[str, Any]) -> dict[str, Any]:
    return {k: v for k, v in dest.items() if v not in (None, "")}


def recompute(conn: sqlite3.Connection, title_id: int, now: int | None = None) -> None:
    head = conn.execute(
        "SELECT ok_dests, ids, title, year, media_type, season, episode, item_key, source_provider, "
        "source_instance, run_id, rating, domain FROM event_titles WHERE id=?",
        (title_id,),
    ).fetchone()
    if head is None:
        return
    oks = _ok_entries(head["ok_dests"])
    rows = conn.execute(
        f"SELECT {','.join(_EVENT_FIELDS)} FROM events WHERE title_id=? ORDER BY created_at ASC, id ASC",
        (title_id,),
    ).fetchall()
    if not rows and not oks:
        conn.execute("DELETE FROM event_titles WHERE id=?", (title_id,))
        return
    events = [dict(r) for r in rows]
    domain = str((events[0].get("domain") if events else head["domain"]) or "sync")
    gids = sorted({int(e["group_id"]) for e in events if e.get("group_id") is not None})
    groups: dict[int, dict[str, Any]] = {}
    for i in range(0, len(gids), _CHUNK):
        part = gids[i:i + _CHUNK]
        for g in conn.execute(
            f"SELECT id, status, destination_provider, destination_instance, feature, reason_code, reason, "
            f"last_event_at, acknowledged_at FROM event_groups WHERE id IN ({_in(part)})",
            part,
        ).fetchall():
            groups[int(g["id"])] = dict(g)

    dests: dict[tuple[str, str, str], dict[str, Any]] = {}
    for d in oks:
        dests[_dest_key(d)] = {**d, "state": "ok", "ledger": True}
    kinds: set[str] = {f"{d['feature']}:{d.get('op') or 'add'}" for d in oks if d.get("feature")}
    ids: dict[str, str] = dict(_json(head["ids"], {}) or {})
    rating: Any = head["rating"]
    progress: Any = None
    playback_status = ""
    sources: dict[tuple[str, str], int] = {}
    if head["source_provider"]:
        sources[(str(head["source_provider"]), str(head["source_instance"] or "default"))] = 1

    for e in events:
        detail = _json(e.get("detail"), {}) or {}
        _merge_ids(ids, detail)
        if detail.get("rating") not in (None, ""):
            rating = detail.get("rating")
        if detail.get("progress") not in (None, ""):
            progress = detail.get("progress")
        sp = str(e.get("source_provider") or "").upper()
        if sp:
            key = (sp, str(e.get("source_instance") or "default"))
            sources[key] = sources.get(key, 0) + 1
        etype = str(e.get("event_type") or "")
        feature = str(e.get("feature") or "").strip().lower()
        if domain == "scrobble":
            kinds.add("scrobble:rating" if etype.startswith("rating_") else "scrobble:watch")
            continue
        op = str(e.get("operation") or "add").strip().lower() or "add"
        if feature:
            kinds.add(f"{feature}:{op}")
        dst = str(e.get("destination_provider") or "").upper()
        state = _SYNC_STATE.get(etype)
        if not dst or state is None:
            continue
        reason_code = str(e.get("reason_code") or "")
        if state == "skip" and any(h in reason_code.lower() for h in _SAME_HINTS):
            state = "same"
        dkey = (dst, str(e.get("destination_instance") or "default"), feature)
        at = int(e.get("created_at") or 0)
        current = dests.get(dkey)
        if current is not None and int(current.get("at") or 0) > at:
            continue
        gid = e.get("group_id")
        dests[dkey] = {
            "provider": dst, "instance": dkey[1], "feature": feature, "op": op, "state": state,
            "reason_code": reason_code or None, "reason": str(e.get("reason") or "") or None,
            "at": at, "group_id": int(gid) if gid is not None else None,
        }

    sessions = None
    if domain == "scrobble":
        sessions = len({_session_of(e) for e in events}) or None
        for gid, g in sorted(groups.items(), key=lambda kv: (int(kv[1].get("last_event_at") or 0), kv[0])):
            status = str(g.get("status") or "")
            dst = str(g.get("destination_provider") or "").upper()
            if not dst:
                playback_status = status or playback_status
                continue
            reason = f"{g.get('reason_code') or ''} {g.get('reason') or ''}".lower()
            dkey = (dst, str(g.get("destination_instance") or "default"), str(g.get("feature") or ""))
            state = _state_for(status, reason)
            current = dests.get(dkey)
            if current is not None and current["state"] == "fail" and state == "pending":
                continue
            dests[dkey] = {
                "provider": dst, "instance": dkey[1], "feature": dkey[2], "op": "watch",
                "state": state, "reason_code": g.get("reason_code"), "reason": g.get("reason"),
                "at": int(g.get("last_event_at") or 0), "group_id": gid,
            }

    attention = 0
    acked: list[int] = []
    problems = 0
    for d in dests.values():
        g = groups.get(d.get("group_id") or -1)
        if d["state"] != "fail" or g is None:
            continue
        problems += 1
        if g.get("acknowledged_at") is not None:
            acked.append(int(g["acknowledged_at"]))
        elif str(g.get("status") or "") in _PROBLEM_GROUP_STATUSES and int(d.get("at") or 0) >= int(g.get("last_event_at") or 0):
            attention = 1

    states = {d["state"] for d in dests.values()}
    if "fail" in states:
        status = "failed"
    elif "ok" in states:
        status = "completed"
    elif not dests and playback_status:
        status = playback_status
    elif "pending" in states:
        status = "running"
    elif states:
        status = "skipped"
    else:
        status = "informational"

    title_row = next((e for e in reversed(events) if e.get("title")), None) or {}
    title = title_row.get("title") or head["title"]
    season = title_row.get("season") if title_row else head["season"]
    episode = title_row.get("episode") if title_row else head["episode"]
    item_key = next((str(e["item_key"]) for e in events if e.get("item_key")), "") or str(head["item_key"] or "")
    if item_key:
        _ids_from_key(ids, base_key(item_key))
    source = max(sources.items(), key=lambda kv: kv[1])[0] if sources else ("", "")
    ordered = sorted(dests.values(), key=_dest_key)
    stored = [_slim(d) for d in ordered if not d.get("ledger")]
    times = [int(e.get("created_at") or 0) for e in events] + [int(d.get("at") or 0) for d in oks]
    acknowledged_at = max(acked) if problems and len(acked) == problems else None
    run_id = next((e["run_id"] for e in reversed(events) if e.get("run_id")), None) or head["run_id"]
    conn.execute(
        "UPDATE event_titles SET domain=?, updated_at=?, run_id=?, first_event_at=?, last_event_at=?, event_count=?, "
        "status=?, attention=?, item_key=?, ident=?, title=?, year=?, media_type=?, season=?, episode=?, "
        "source_provider=?, source_instance=?, kinds=?, states=?, dests=?, routes=?, destinations=?, ids=?, rating=?, "
        "progress=?, sessions=?, acknowledged_at=?, dirty=0 WHERE id=?",
        (
            domain, int(now or time.time()), run_id, min(times), max(times), len(events) + len(oks),
            status, attention, base_key(item_key) or None, ident_of(title, season, episode) or None,
            title, next((e["year"] for e in events if e.get("year")), None) or head["year"],
            next((e["media_type"] for e in events if e.get("media_type")), None) or head["media_type"],
            season, episode, source[0] or None, source[1] or None,
            "|" + "|".join(sorted(kinds)) + "|" if kinds else None,
            "|" + "|".join(sorted(states)) + "|" if states else None,
            "|" + "|".join(sorted({d["provider"] for d in ordered})) + "|" if ordered else None,
            _routes(ordered), _dump(stored), _dump(ids),
            None if rating in (None, "") else str(rating)[:16],
            None if progress in (None, "") else str(progress)[:8],
            sessions, acknowledged_at, title_id,
        ),
    )


def refresh_dirty(conn: sqlite3.Connection, *, limit: int | None = None) -> int:
    from .groups import _GROUP_LOCK

    done = 0
    while True:
        batch = _CHUNK if limit is None else max(0, min(_CHUNK, limit - done))
        if batch <= 0:
            return done
        with _GROUP_LOCK:
            ids = [int(r[0]) for r in conn.execute(
                "SELECT id FROM event_titles WHERE dirty=1 LIMIT ?", (batch,)
            ).fetchall()]
            if not ids:
                return done
            now = int(time.time())
            if conn.in_transaction:
                for tid in ids:
                    recompute(conn, tid, now)
            else:
                with conn:
                    for tid in ids:
                        recompute(conn, tid, now)
        done += len(ids)


def _meta_version(conn: sqlite3.Connection) -> int:
    row = conn.execute("SELECT value_int FROM local_meta WHERE key=?", (TITLES_META,)).fetchone()
    return int(row[0] or 0) if row is not None else 0


def _meta_done(conn: sqlite3.Connection) -> bool:
    return _meta_version(conn) >= TITLES_VERSION


def _regroup_scrobbles(conn: sqlite3.Connection) -> None:
    conn.execute("UPDATE events SET title_id=NULL WHERE domain='scrobble' AND title_id IS NOT NULL")
    conn.execute("DELETE FROM event_titles WHERE domain='scrobble'")
    conn.execute("UPDATE local_meta SET value_int=0 WHERE key=?", (TITLES_META,))


def backfill_pending(conn: sqlite3.Connection) -> bool:
    try:
        return not _meta_done(conn)
    except Exception:
        return False


def backfill(conn: sqlite3.Connection, *, stop: threading.Event | None = None) -> int:
    from .groups import _GROUP_LOCK

    total = 0
    while stop is None or not stop.is_set():
        with _GROUP_LOCK:
            ids = [int(r[0]) for r in conn.execute(
                "SELECT id FROM events WHERE title_id IS NULL AND group_id IS NOT NULL LIMIT ?", (_BACKFILL_BATCH,)
            ).fetchall()]
            if not ids:
                with conn:
                    _mark_backfilled(conn)
                break
            with conn:
                assign(conn, ids)
        total += len(ids)
        refresh_dirty(conn)
    return total


def start_backfill() -> bool:
    global _backfill_thread
    with _backfill_lock:
        if _backfill_thread is not None and _backfill_thread.is_alive():
            return True

        def run() -> None:
            c = get_conn()
            if c is None:
                return
            try:
                backfill(c)
            except Exception as exc:
                _LOG.warning("event title backfill failed: %s", exc, exc_info=True)

        _backfill_thread = threading.Thread(target=run, name="cw-event-titles-backfill", daemon=True)
        _backfill_thread.start()
        return True


def backfill_running() -> bool:
    return _backfill_thread is not None and _backfill_thread.is_alive()


def _mark_backfilled(conn: sqlite3.Connection) -> None:
    conn.execute(
        "INSERT INTO local_meta(key,value_int,value_type,updated_at) VALUES(?,?,'int',?) "
        "ON CONFLICT(key) DO UPDATE SET value_int=excluded.value_int,value_type=excluded.value_type,"
        "updated_at=excluded.updated_at",
        (TITLES_META, TITLES_VERSION, int(time.time())),
    )


def ensure_ready(conn: sqlite3.Connection) -> bool:
    if not backfill_pending(conn):
        return True
    if not conn.in_transaction and 0 < _meta_version(conn) < 2:
        from .groups import _GROUP_LOCK

        with _GROUP_LOCK, conn:
            if 0 < _meta_version(conn) < 2:
                _regroup_scrobbles(conn)
    if conn.execute("SELECT 1 FROM events WHERE title_id IS NULL AND group_id IS NOT NULL LIMIT 1").fetchone() is None:
        if conn.in_transaction:
            _mark_backfilled(conn)
        else:
            with conn:
                _mark_backfilled(conn)
        return True
    start_backfill()
    return False


def drop_orphans(conn: sqlite3.Connection) -> int:
    return int(conn.execute(
        "DELETE FROM event_titles WHERE ok_dests IS NULL "
        "AND NOT EXISTS (SELECT 1 FROM events e WHERE e.title_id=event_titles.id)"
    ).rowcount or 0)


def prune(conn: sqlite3.Connection, domain: str, cutoff: int) -> int:
    return int(conn.execute(
        "DELETE FROM event_titles WHERE domain=? AND COALESCE(last_event_at, created_at, 0)<? "
        "AND NOT EXISTS (SELECT 1 FROM events e WHERE e.title_id=event_titles.id)",
        (domain, int(cutoff)),
    ).rowcount or 0)


def _row(row: Any) -> dict[str, Any]:
    item = dict(row)
    stored = [d for d in _json(item.get("destinations"), []) if isinstance(d, dict)]
    item["destinations"] = _merged(stored, _ok_entries(item.pop("ok_dests", None)))
    item["ids"] = _json(item.get("ids"), {}) or {}
    item["kinds"] = [k for k in str(item.get("kinds") or "").split("|") if k]
    return item


_OUTCOME_STATE = {"done": "ok", "failed": "fail", "skipped": "skip", "already": "same", "pending": "pending"}
_WHAT_KINDS = {
    "watched": ("history:add", "scrobble:watch"),
    "ratings": ("ratings:", "scrobble:rating"),
    "watchlist": ("watchlist:",),
    "collection": ("collection:",),
    "progress": ("progress:",),
    "removals": (":remove",),
}


def _filters(
    domain: str, q: str | None, outcome: str | None, review: str | None, attention: bool, source: str | None,
    destination: str | None, what: str | None, run_id: str | None, since: int | None, until: int | None,
) -> tuple[list[str], list[Any]]:
    clauses = ["t.domain=?", "t.last_event_at IS NOT NULL"]
    params: list[Any] = [str(domain or "sync")]
    if q:
        like = f"%{str(q).strip()[:200]}%"
        clauses.append("(t.title LIKE ? OR t.item_key LIKE ? OR t.dests LIKE ?)")
        params.extend([like, like, like])
    state = _OUTCOME_STATE.get(str(outcome or "").strip().lower())
    if state:
        clauses.append("t.states LIKE ?")
        params.append(f"%|{state}|%")
    rv = str(review or "").strip().lower()
    if rv == "unreviewed" or attention:
        clauses.append("t.attention=1")
    elif rv == "reviewed":
        clauses.append("t.acknowledged_at IS NOT NULL")
    if source:
        clauses.append("t.source_provider=?")
        params.append(str(source).strip().upper())
    if destination:
        clauses.append("t.dests LIKE ?")
        params.append(f"%|{str(destination).strip().upper()}|%")
    kinds = _WHAT_KINDS.get(str(what or "").strip().lower())
    if kinds:
        clauses.append("(" + " OR ".join("t.kinds LIKE ?" for _ in kinds) + ")")
        params.extend(f"%{k}%" for k in kinds)
    if run_id:
        clauses.append("t.run_id=?")
        params.append(str(run_id))
    if since not in (None, ""):
        clauses.append("t.last_event_at>=?")
        params.append(int(since))
    if until not in (None, ""):
        clauses.append("t.last_event_at<=?")
        params.append(int(until))
    return clauses, params


def list_titles(
    *,
    domain: str = "sync",
    q: str | None = None,
    outcome: str | None = None,
    review: str | None = None,
    attention: bool = False,
    source: str | None = None,
    destination: str | None = None,
    what: str | None = None,
    run_id: str | None = None,
    since: int | None = None,
    until: int | None = None,
    order: str | None = "newest",
    limit: int = 50,
    offset: int = 0,
    row_filter: Callable[[dict[str, Any]], bool] | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    c = conn or get_conn()
    lim = max(1, min(int(limit or 50), 200))
    off = max(0, int(offset or 0))
    if c is None:
        return {"items": [], "total": 0, "limit": lim, "offset": off, "preparing": False}
    from .groups import _ensure_correlated

    if not c.in_transaction:
        _ensure_correlated(c)
        refresh_dirty(c)
    preparing = not ensure_ready(c)

    clauses, params = _filters(domain, q, outcome, review, attention, source, destination, what, run_id, since, until)
    fn = "cw_title_allowed"
    if row_filter:
        cache: dict[tuple[Any, ...], bool] = {}
        c.create_function(fn, 3, lambda sp, si, routes: int(_allowed(row_filter, cache, sp, si, routes)), deterministic=True)
        clauses.append(f"{fn}(t.source_provider, t.source_instance, t.routes)=1")
    where = " WHERE " + " AND ".join(clauses)
    dir_ = "ASC" if str(order or "").strip().lower() in ("oldest", "asc") else "DESC"
    attn_clauses, attn_params = _filters(domain, q, outcome, review, True, source, destination, None, run_id, since, until)
    if row_filter:
        attn_clauses.append(f"{fn}(t.source_provider, t.source_instance, t.routes)=1")
    try:
        total = int(c.execute(f"SELECT COUNT(*) FROM event_titles t{where}", params).fetchone()[0])
        attention_total = int(c.execute(
            f"SELECT COUNT(*) FROM event_titles t WHERE {' AND '.join(attn_clauses)}", attn_params,
        ).fetchone()[0])
        rows = c.execute(
            f"SELECT {','.join('t.' + col for col in TITLE_COLUMNS)} FROM event_titles t{where} "
            f"ORDER BY t.last_event_at {dir_}, t.id {dir_} LIMIT ? OFFSET ?",
            [*params, lim, off],
        ).fetchall()
    finally:
        if row_filter:
            c.create_function(fn, 3, None)
    items = [_row(r) for r in rows]
    if row_filter:
        for item in items:
            item["destinations"] = [d for d in item["destinations"] if row_filter(_route(item, d))]
    return {"items": items, "total": total, "attention_total": attention_total, "limit": lim, "offset": off, "preparing": preparing or backfill_running()}


def _route(item: Mapping[str, Any], dest: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "domain": item.get("domain"),
        "source_provider": item.get("source_provider"),
        "source_instance": item.get("source_instance"),
        "destination_provider": dest.get("provider"),
        "destination_instance": dest.get("instance"),
    }


def _allowed(row_filter: Callable[[dict[str, Any]], bool], cache: dict[tuple[Any, ...], bool], sp: Any, si: Any, routes: Any) -> bool:
    key = (sp, si, routes)
    hit = cache.get(key)
    if hit is not None:
        return hit
    base = {"source_provider": sp, "source_instance": si}
    pairs = [part.partition("/") for part in str(routes or "").split("|") if part]
    if not pairs:
        ok = bool(row_filter(base))
    else:
        ok = any(row_filter({**base, "destination_provider": p, "destination_instance": i or "default"}) for p, _, i in pairs)
    cache[key] = ok
    return ok


def get_title(title_id: Any, *, related_limit: int = 20, conn: sqlite3.Connection | None = None) -> dict[str, Any] | None:
    c = conn or get_conn()
    if c is None:
        return None
    try:
        tid = int(title_id)
    except Exception:
        return None
    if not c.in_transaction:
        refresh_dirty(c)
    row = c.execute(f"SELECT {','.join(TITLE_COLUMNS)} FROM event_titles WHERE id=?", (tid,)).fetchone()
    if row is None:
        return None
    item = _row(row)
    clauses: list[str] = []
    params: list[Any] = []
    if item.get("ident"):
        clauses.append("ident=?")
        params.append(item["ident"])
    if item.get("item_key"):
        clauses.append("item_key=?")
        params.append(item["item_key"])
    item["related"] = [_row(r) for r in c.execute(
        f"SELECT {','.join(TITLE_COLUMNS)} FROM event_titles WHERE id<>? AND last_event_at IS NOT NULL "
        f"AND ({' OR '.join(clauses)}) ORDER BY last_event_at DESC, id DESC LIMIT ?",
        [tid, *params, max(1, min(int(related_limit), 100))],
    ).fetchall()] if clauses else []
    if item.get("domain") == "scrobble" and int(item.get("sessions") or 0) > 1:
        item["session_list"] = _sessions(c, tid)
    return item


def _sessions(conn: sqlite3.Connection, title_id: int) -> list[dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for e in conn.execute(
        "SELECT session_key, created_at, event_type, detail FROM events WHERE title_id=? ORDER BY created_at, id",
        (title_id,),
    ).fetchall():
        key = _session_of(e)
        at = int(e["created_at"] or 0)
        s = out.setdefault(key, {"started_at": at, "last_at": at, "progress": None, "finished": False})
        s["last_at"] = max(s["last_at"], at)
        progress = (_json(e["detail"], {}) or {}).get("progress")
        if progress not in (None, ""):
            s["progress"] = progress
        if e["event_type"] == "scrobble_completed":
            s["finished"] = True
    return sorted(out.values(), key=lambda s: s["started_at"], reverse=True)[:_SESSION_LIMIT]


def acknowledge(title_id: Any, *, by: str | None = None, undo: bool = False, destinations: Sequence[Mapping[str, Any]] | None = None,
                conn: sqlite3.Connection | None = None) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    item = get_title(title_id, related_limit=1, conn=c)
    if item is None:
        return {"ok": False, "found": False}
    scope = item["destinations"] if destinations is None else destinations
    gids = sorted({int(d["group_id"]) for d in scope if d.get("group_id") is not None and d.get("state") == "fail"})
    from .groups import _GROUP_LOCK

    ts = int(time.time())
    with _GROUP_LOCK, c:
        for i in range(0, len(gids), _CHUNK):
            part = gids[i:i + _CHUNK]
            if undo:
                c.execute(f"UPDATE event_groups SET acknowledged_at=NULL, acknowledged_by=NULL WHERE id IN ({_in(part)})", part)
            else:
                c.execute(
                    f"UPDATE event_groups SET acknowledged_at=?, acknowledged_by=? WHERE id IN ({_in(part)}) "
                    "AND acknowledged_at IS NULL",
                    [ts, by, *part],
                )
        mark_dirty_for_groups(c, gids)
        c.execute("UPDATE event_titles SET dirty=1 WHERE id=?", (item["id"],))
    refresh_dirty(c)
    fresh = get_title(item["id"], related_limit=1, conn=c) or item
    return {"ok": True, "id": item["id"], "acknowledged_at": fresh.get("acknowledged_at"), "attention": fresh.get("attention")}


def acknowledge_matching(
    *,
    domain: str = "sync",
    q: str | None = None,
    outcome: str | None = None,
    source: str | None = None,
    destination: str | None = None,
    what: str | None = None,
    run_id: str | None = None,
    since: int | None = None,
    until: int | None = None,
    by: str | None = None,
    row_filter: Callable[[dict[str, Any]], bool] | None = None,
    conn: sqlite3.Connection | None = None,
) -> dict[str, Any]:
    c = conn or get_conn()
    if c is None:
        return {"ok": False, "available": False}
    from .groups import _GROUP_LOCK

    if not c.in_transaction:
        refresh_dirty(c)
    clauses, params = _filters(domain, q, outcome, None, True, source, destination, what, run_id, since, until)
    rows = c.execute(
        f"SELECT {','.join('t.' + col for col in TITLE_COLUMNS)} FROM event_titles t WHERE {' AND '.join(clauses)}", params,
    ).fetchall()
    gids: set[int] = set()
    titles_hit = 0
    for row in rows:
        item = _row(row)
        dests = [d for d in item["destinations"] if d.get("state") == "fail" and d.get("group_id") is not None]
        if row_filter:
            dests = [d for d in dests if row_filter(_route(item, d))]
        if dests:
            titles_hit += 1
            gids.update(int(d["group_id"]) for d in dests)
    ordered = sorted(gids)
    ts = int(time.time())
    with _GROUP_LOCK, c:
        for i in range(0, len(ordered), _CHUNK):
            part = ordered[i:i + _CHUNK]
            c.execute(
                f"UPDATE event_groups SET acknowledged_at=?, acknowledged_by=? WHERE id IN ({_in(part)}) AND acknowledged_at IS NULL",
                [ts, by, *part],
            )
        mark_dirty_for_groups(c, ordered)
    refresh_dirty(c)
    return {"ok": True, "titles": titles_hit, "groups": len(ordered)}

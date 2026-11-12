# cw_platform/tracker_automation.py
# CrossWatch - Local status ownership, queued inference and title removal
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from contextlib import AbstractContextManager, nullcontext
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from sqlite3 import Connection


def instance_options(cfg, instance="default"):
    root = cfg.get("crosswatch") or {}
    block = root if instance == "default" else (root.get("instances") or {}).get(instance, {})
    return {key: str(block.get(key, True)).lower() not in {"false", "0", "off"}
            for key in ("automatic_status", "month_review", "wrapped")}


def root_options(cfg, root):
    from .config_base import CONFIG
    from .tracker_store import tracker_roots
    for instance, path in tracker_roots(cfg, CONFIG):
        if path.resolve() == root.resolve():
            return instance_options(cfg, instance)
    return instance_options(cfg)


class TrackerAutomation:
    if TYPE_CHECKING:
        def _db(self, *, create: bool = False) -> AbstractContextManager[Connection | None]: ...
        def mutation(self) -> AbstractContextManager[Any]: ...
        def _write(self, conn: Connection) -> AbstractContextManager[Connection]: ...

        @staticmethod
        def _bump(conn: Connection) -> None: ...

        @staticmethod
        def _prune_changes(conn: Connection, now_ns: int) -> None: ...

        @staticmethod
        def _note_get(conn: Connection, key: str) -> dict[str, Any] | None: ...

        @staticmethod
        def _note_put(conn: Connection, key: str, note: Mapping[str, Any] | None, now: int) -> None: ...

        @staticmethod
        def _note_change(conn: Connection, batch: str, old_key: str, new_key: str | None, before: Any, after: Any, now: int) -> None: ...

        @staticmethod
        def _related_queries(*, show: bool, tmdb: str = "", imdb: str = "", title: str = "", base: str = "") -> list[tuple[str, list[Any]]]: ...

        @staticmethod
        def _related_rows(conn: Connection, queries: list[tuple[str, list[Any]]], limit: int | None) -> list[tuple[str, str, Any]]: ...

        def apply(
            self,
            changes: Iterable[tuple[str, str | None, str | None, Mapping[str, Any] | None]],
            *,
            action: str,
            expected: Mapping[Any, Any] | None = None,
            follow: Mapping[str, Any] | None = None,
            note_status: Mapping[str, Any] | None = None,
            note_changes: Mapping[str, Any] | None = None,
            control_changes: Mapping[str, Any] | None = None,
            _conn: Connection | None = None,
        ) -> str | None: ...

    @staticmethod
    def _automatic_key(kind, show_key, tmdb, imdb):
        if kind in {"episode", "season"}:
            return "show:" + show_key if show_key and show_key.startswith("tmdb:") else None
        if kind in {"show", "anime"}:
            return "show:tmdb:" + str(tmdb) if tmdb else None
        if kind == "movie":
            return "movie:tmdb:" + str(tmdb) if tmdb else "movie:imdb:" + str(imdb) if imdb else None
        return None

    @staticmethod
    def _control_get(conn, key):
        row = conn.execute("SELECT data FROM status_control WHERE title_key=?", (key,)).fetchone()
        return json.loads(row[0]) if row else None

    @staticmethod
    def _control_put(conn, key, value):
        if value is None:
            conn.execute("DELETE FROM status_control WHERE title_key=?", (key,))
        else:
            conn.execute("INSERT INTO status_control VALUES(?,?) ON CONFLICT(title_key) DO UPDATE SET data=excluded.data",
                         (key, json.dumps(value, sort_keys=True)))

    def _control_change(self, conn, batch, key, value, now):
        before = self._control_get(conn, key)
        if before == value:
            return False
        self._control_put(conn, key, value)
        conn.execute("INSERT INTO changes(batch,scope,feature,action,old_key,new_key,before,after,created_ns,find) VALUES(?,'','status_control','note',?,?,?,?,?,'')",
                     (batch, key, key if value is not None else None, json.dumps(before) if before is not None else None,
                      json.dumps(value) if value is not None else None, now))
        return True

    def _status_origin(self, conn, batch, key, origin, now):
        current = self._control_get(conn, key) or {}
        return self._control_change(conn, batch, key, {"origin": origin, "manual": origin == "manual" or current.get("manual", False)}, now)

    def status_control(self, key):
        with self._db() as conn:
            value = self._control_get(conn, key) if conn is not None else None
            note = self._note_get(conn, key) if conn is not None else None
            return value or {"origin": "existing" if (note or {}).get("status") else "automatic", "manual": False}

    def enable_title_automation(self, key):
        batch, now = uuid.uuid4().hex, time.time_ns()
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                changed = self._control_change(conn, batch, key, {"origin": "automatic", "manual": False}, now)
                conn.execute("INSERT INTO status_pending VALUES(?,hex(randomblob(16)),0) ON CONFLICT(title_key) DO UPDATE SET version=excluded.version,due=0", (key,))
                if changed:
                    self._bump(conn)
        return batch if changed else None

    def configure_automation(self, enabled):
        with self._db(create=True) as conn:
            assert conn is not None
            value = "1" if enabled else "0"
            row = conn.execute("SELECT value FROM meta WHERE key='automatic_status'", ()).fetchone()
            if row and row[0] == value:
                return
            with self._write(conn):
                conn.execute("INSERT INTO meta VALUES('automatic_status',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (value,))
                if not enabled:
                    conn.execute("DELETE FROM status_pending")
                    conn.execute("DELETE FROM meta WHERE key='status_catchup'")
                    job = self._review_get(conn)
                    if job.get("state") in {"scanning", "processing"}:
                        job["state"] = "cancelled"
                        self._review_put(conn, job)
                        conn.execute("DELETE FROM status_review")

    def pending_statuses(self, limit=25):
        with self._db() as conn:
            if conn is None:
                return []
            return conn.execute("SELECT title_key,version FROM status_pending WHERE due<=? ORDER BY due,title_key LIMIT ?", (time.time(), limit)).fetchall()

    def queue_metadata_statuses(self, keys):
        with self.mutation(), self._db() as conn:
            if conn is None:
                return
            eligible = []
            for key in keys:
                note, control = self._note_get(conn, key), self._control_get(conn, key)
                if (note or {}).get("status") == "watching" and control and not control.get("manual"):
                    eligible.append((key,))
            if eligible:
                with self._write(conn):
                    if conn.execute("SELECT value FROM meta WHERE key='automatic_status'").fetchone() == ("0",):
                        return
                    conn.executemany("INSERT INTO status_pending VALUES(?,hex(randomblob(16)),1) ON CONFLICT(title_key) DO UPDATE SET version=excluded.version,due=min(status_pending.due,excluded.due)", eligible)

    def status_catchup(self):
        with self.mutation(), self._db() as conn:
            if conn is None or not conn.execute("SELECT 1 FROM meta WHERE key='status_catchup'").fetchone():
                return False
            if conn.execute("SELECT 1 FROM status_pending WHERE due=0 LIMIT 1").fetchone():
                return True
            with self._write(conn):
                conn.execute("DELETE FROM meta WHERE key='status_catchup' AND NOT EXISTS(SELECT 1 FROM status_pending WHERE due=0)")
                return conn.execute("SELECT 1 FROM meta WHERE key='status_catchup'").fetchone() is not None

    @staticmethod
    def _request_status_catchup(conn):
        conn.execute("INSERT OR REPLACE INTO meta SELECT 'status_catchup','1' WHERE coalesce((SELECT value FROM meta WHERE key='automatic_status'),'1')='1' AND EXISTS(SELECT 1 FROM status_pending WHERE due=0)")

    def request_status_catchup(self):
        with self.mutation(), self._db() as conn:
            if conn is not None:
                with self._write(conn):
                    self._request_status_catchup(conn)

    @staticmethod
    def _status_where(key):
        kind, namespace, value = key.split(":", 2)
        if kind == "show" and namespace == "tmdb":
            return "show_key=? AND kind='episode'", ["tmdb:" + value]
        if kind == "movie" and namespace in {"tmdb", "imdb"}:
            return f"{namespace}=? AND kind='movie'", [value]
        return "0", []

    def _status_decision(self, conn, key, metadata: Mapping[str, Any] | Callable[[], Mapping[str, Any] | None] | None, *, retry=False):
        note, control = self._note_get(conn, key), self._control_get(conn, key)
        if (control and control.get("manual")) or (not control and (note or {}).get("status")):
            return None, "protected", note, False
        if retry and (control or {}).get("origin") == "sync" and not (key.startswith("show:") and (note or {}).get("status") == "watching"):
            return (note or {}).get("status"), "Synced status retained until new watch activity", note, False
        where, args = self._status_where(key)
        sql = "FROM items WHERE scope='' AND feature='history' AND " + where + " AND coalesce(when_at,'')!=''"
        first = conn.execute("SELECT data " + sql + " LIMIT 1", args).fetchone()
        if not first:
            desired = None if (control or {}).get("origin") == "automatic" else (note or {}).get("status")
            return desired, "No watch history remains", note, False
        item = json.loads(first[0])
        if not key.startswith("show:"):
            return "completed", "Movie watched", note or {"type": "movie", "title": item.get("title"), "tmdb": (item.get("ids") or {}).get("tmdb")}, False
        base = note or {"type": "show", "title": item.get("series_title") or item.get("title"), "tmdb": (item.get("show_ids") or {}).get("tmdb")}
        seen = {(s, e) for s, e in conn.execute("SELECT DISTINCT season,episode " + sql + " AND season>0 AND episode>0", args)}
        if not seen:
            return (note or {}).get("status"), "No regular episodes watched", base, False
        if callable(metadata):
            metadata = metadata()
        if not metadata:
            return (note or {}).get("status") or "watching", "Episode watched; completion metadata unavailable", base, True
        counts = {s["season_number"]: s["episode_count"] for s in metadata.get("seasons", [])
                  if isinstance(s, dict) and type(s.get("season_number")) is int and s["season_number"] > 0
                  and type(s.get("episode_count")) is int and s["episode_count"] > 0}
        expected = sum(counts.values())
        matched = {(s, e) for s, e in seen if s in counts and e <= counts[s]}
        consistent = metadata.get("number_of_episodes", expected) == expected and metadata.get("number_of_seasons", len(counts)) == len(counts)
        complete = expected > 0 and consistent and len(matched) == expected and matched == seen and metadata.get("status") in {"Ended", "Canceled"} and not metadata.get("next_episode_to_air")
        if complete:
            reason = f"All {expected} regular episodes watched; series ended"
        elif not expected or not consistent:
            reason = "Completion metadata is missing or inconsistent"
        elif matched != seen:
            reason = "Watched episode numbers do not match the cached episode list"
        elif len(matched) < expected:
            reason = f"{len(matched)} of {expected} regular episodes watched"
        elif metadata.get("next_episode_to_air"):
            reason = "All known episodes watched; another episode is announced"
        else:
            reason = "All known episodes watched; series is not marked ended or canceled"
        return ("completed" if complete else "watching"), reason, base, not (expected > 0 and consistent)

    def evaluate_status(self, key, metadata: Mapping[str, Any] | Callable[[], Mapping[str, Any] | None] | None = None, *, version=None, preview=False, prune=True, group=None, review=False, expected=None):
        from .tracker_store import _note_norm
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with nullcontext() if preview else self._write(conn):
                retry = review
                if review:
                    entry = conn.execute("SELECT done FROM status_review WHERE title_key=?", (key,)).fetchone()
                    if not entry or entry[0] or self._review_get(conn).get("state") != "processing":
                        return {"key": key, "skipped": "changed"}
                    if conn.execute("SELECT 1 FROM status_pending WHERE title_key=?", (key,)).fetchone():
                        self._review_done(conn, key)
                        return {"key": key, "skipped": "live"}
                if version is not None:
                    pending = conn.execute("SELECT version,due FROM status_pending WHERE title_key=?", (key,)).fetchone()
                    if not pending or pending[0] != version:
                        return {"key": key, "skipped": "changed"}
                    retry = pending[1] > 0
                desired, reason, note, deferred = self._status_decision(conn, key, metadata, retry=retry)
                before = self._note_get(conn, key)
                if deferred and (before or {}).get("status") == desired and (self._control_get(conn, key) or {}).get("origin") == "sync":
                    deferred = False
                result = {"key": key, "title": (note or {}).get("title") or key, "before": (before or {}).get("status"), "after": desired, "reason": reason, "deferred": deferred}
                if reason == "protected":
                    result.update(after=result["before"], skipped="protected")
                    if preview or expected is not None:
                        control = self._control_get(conn, key) or {}
                        result["reason"] = "Manual status kept" if control.get("manual") else "Existing status kept; enable automatic updates for this title to change it"
                if expected is not None and reason != "protected" and (result["before"] != expected.get("before") or result["after"] != expected.get("after")):
                    result.update(skipped="changed", reason="Status or watch data changed since preview; refresh to check again")
                    return result
                if preview:
                    return result
                if reason != "protected" and result["before"] != desired:
                    now, batch = time.time_ns(), uuid.uuid4().hex
                    after = _note_norm({**(note or {}), "status": desired})
                    if not any(after.get(f) for f in ("status", "started", "finished", "notes", "tags", "fields")):
                        after = None
                    self._note_put(conn, key, after, now)
                    self._note_change(conn, batch, key, key if after else None, before, after, now)
                    self._control_change(conn, batch, key, {"origin": "automatic", "manual": False, "reason": reason}, now)
                    conn.execute("INSERT INTO automatic_batches VALUES(?,?)", (batch, group or batch))
                    self._bump(conn)
                    if prune:
                        self._prune_changes(conn, now)
                    result["batch"] = batch
                if review:
                    self._review_done(conn, key)
                    if deferred:
                        conn.execute("INSERT OR IGNORE INTO status_pending VALUES(?,hex(randomblob(16)),?)", (key, time.time() + 86400))
                elif deferred:
                    conn.execute("UPDATE status_pending SET due=? WHERE title_key=?", (time.time() + 86400, key))
                else:
                    conn.execute("DELETE FROM status_pending WHERE title_key=?", (key,))
                return result

    @staticmethod
    def _review_get(conn) -> dict[str, Any]:
        row = conn.execute("SELECT value FROM meta WHERE key='automatic_review'").fetchone()
        return json.loads(row[0]) if row else {"state": "idle", "scanned": 0, "titles": 0, "processed": 0}

    @staticmethod
    def _review_put(conn, job):
        conn.execute("INSERT INTO meta VALUES('automatic_review',?) ON CONFLICT(key) DO UPDATE SET value=excluded.value", (json.dumps(job),))

    def review_status(self) -> dict[str, Any]:
        with self._db() as conn:
            return self._review_get(conn) if conn is not None else {"state": "idle", "scanned": 0, "titles": 0, "processed": 0}

    def start_review(self):
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                if self._review_get(conn).get("state") in {"scanning", "processing"}:
                    raise ValueError("review_running")
                ceiling = conn.execute("SELECT max(item_key) FROM items WHERE scope='' AND feature='history'").fetchone()[0]
                conn.execute("DELETE FROM status_review")
                job = {"state": "scanning" if ceiling is not None else "complete", "cursor": None, "ceiling": ceiling, "scanned": 0, "titles": 0, "processed": 0}
                self._review_put(conn, job)
                return job

    def cancel_review(self):
        with self.mutation(), self._db() as conn:
            if conn is None:
                return {"state": "idle"}
            with self._write(conn):
                job = self._review_get(conn)
                if job.get("state") in {"scanning", "processing"}:
                    job["state"] = "cancelled"
                    self._review_put(conn, job)
                    conn.execute("DELETE FROM status_review")
                return job

    def scan_review(self, limit=1000):
        with self.mutation(), self._db() as conn:
            if conn is None:
                return
            with self._write(conn):
                job = self._review_get(conn)
                if job.get("state") != "scanning":
                    return
                cursor = job.get("cursor")
                clause, args = (" AND item_key>?", [cursor]) if cursor is not None else ("", [])
                rows = conn.execute("SELECT item_key,kind,show_key,tmdb,imdb FROM items WHERE scope='' AND feature='history' AND item_key<=?" + clause + " ORDER BY item_key LIMIT ?", [job["ceiling"], *args, max(1, min(limit, 1000))]).fetchall()
                keys = {self._automatic_key(*row[1:]) for row in rows}
                before = conn.total_changes
                conn.executemany("INSERT OR IGNORE INTO status_review(title_key) VALUES(?)", [(key,) for key in keys if key])
                job["titles"] += conn.total_changes - before
                job["scanned"] += len(rows)
                if rows:
                    job["cursor"] = rows[-1][0]
                if not rows or rows[-1][0] == job["ceiling"]:
                    job["state"] = "processing" if job["titles"] else "complete"
                self._review_put(conn, job)

    def review_pending(self, limit=25):
        with self._db() as conn:
            if conn is None or self._review_get(conn).get("state") != "processing":
                return []
            return [row[0] for row in conn.execute("SELECT title_key FROM status_review WHERE done=0 ORDER BY title_key LIMIT ?", (limit,))]

    def _review_done(self, conn, key):
        if conn.execute("UPDATE status_review SET done=1 WHERE title_key=? AND done=0", (key,)).rowcount:
            job = self._review_get(conn)
            job["processed"] += 1
            if job["processed"] >= job["titles"]:
                job["state"] = "complete"
            self._review_put(conn, job)

    def prune_status_changes(self):
        with self.mutation(), self._db() as conn:
            if conn is not None:
                with self._write(conn):
                    self._prune_changes(conn, time.time_ns())

    def automation_candidates(self, after="", limit=100):
        from .tracker_store import _ITEM_NOTE_KEY
        with self._db() as conn:
            if conn is None:
                return []
            return [r[0] for r in conn.execute(f"SELECT DISTINCT {_ITEM_NOTE_KEY} AS title_key FROM items WHERE scope='' AND feature='history' AND ({_ITEM_NOTE_KEY})>? ORDER BY title_key LIMIT ?", (after, limit))]

    def queue_statuses(self, keys):
        with self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                conn.executemany("INSERT INTO status_pending VALUES(?,hex(randomblob(16)),0) ON CONFLICT(title_key) DO UPDATE SET version=excluded.version,due=0", [(key,) for key in keys])

    def title_removal(self, keys, everything=False, expected=None):
        from .tracker_store import TrackerConflict, _note_norm
        import hashlib
        rows, notes, controls = {}, {}, {}
        with self.mutation(), self._db() as conn, (self._write(conn) if conn is not None else nullcontext()):
            if conn is None:
                raise KeyError(keys[0])
            for key in dict.fromkeys(keys):
                kind, namespace, value = key.split(":", 2)
                queries = self._related_queries(show=kind == "show", tmdb=value if namespace == "tmdb" else "",
                                                imdb=value if namespace == "imdb" else "", title=value if namespace == "title" else "",
                                                base=value if namespace == "key" else "")
                if not everything:
                    queries = [(sql + " AND feature='history'", args) for sql, args in queries]
                for feature, item_key, data in self._related_rows(conn, queries, None):
                    rows[(feature, item_key)] = data
                notes[key] = self._note_get(conn, key)
                controls[key] = self._control_get(conn, key)
            changed_notes = {}
            for key, note in notes.items():
                control = controls[key] or {}
                if everything:
                    changed_notes[key] = None
                elif note and note.get("status") and control.get("origin") == "automatic" and not control.get("manual"):
                    after = _note_norm({**note, "status": None})
                    changed_notes[key] = after if any(after.get(f) for f in ("started", "finished", "notes", "tags", "fields")) else None
            token = hashlib.sha256(json.dumps([sorted((f, k, d) for (f, k), d in rows.items()), notes, controls], sort_keys=True).encode()).hexdigest()
            counts = {}
            for feature, _ in rows:
                counts[feature] = counts.get(feature, 0) + 1
            if everything:
                counts["notes"] = sum(v is not None for v in notes.values())
            elif changed_notes:
                counts["statuses"] = len(changed_notes)
            if expected is None:
                return {"counts": counts, "token": token, "count": len(rows), "titles": len(set(keys))}
            if token != expected:
                raise TrackerConflict("changed_since")
            if not rows and not changed_notes and not everything:
                return {"batch": None, "count": 0, "counts": counts}
            batch = self.apply([(f, k, None, None) for f, k in rows], action="delete", expected=rows,
                               note_changes=changed_notes, control_changes={k: None for k in controls} if everything else None, _conn=conn)
            return {"batch": batch, "count": len(rows), "counts": counts}

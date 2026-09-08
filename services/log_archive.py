# services/log_archive.py
# CrossWatch - Persistent diagnostic log archive
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations

import atexit
import json
import re
import sqlite3
import threading
import time
import uuid
from pathlib import Path
from datetime import datetime, timezone

from _logging import _redact_log_text
from cw_platform.local_db import crosswatch_db_path
from cw_platform.log_context import log_pair_id, log_run_id

RETENTION_DAYS = 7
MAX_BYTES = 100 * 1024 * 1024
ANSI = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
LEVEL = re.compile(r"(?:^|\]\s+|\d{2}:\d{2}:\d{2}\s+|\[)(ERROR|CRITICAL|WARNING|WARN|DEBUG|INFO|SUCCESS)\b", re.I)


def describe_line(text: str, tag: str) -> tuple[str, str, str]:
    text = _redact_log_text(ANSI.sub("", str(text))).rstrip()[:32768]
    matches = LEVEL.findall(text)
    level = (matches[-1].upper() if matches else "WARN" if text.startswith("[!]") else "INFO")
    level = {"WARNING": "WARN", "CRITICAL": "ERROR", "SUCCESS": "INFO"}.get(level, level)
    provider = tag.upper()
    tags = [value for value in re.findall(r"\[([A-Z][A-Z0-9_-]*)(?::[^\[\]]+)?\]", text)
            if value not in {'ERROR', 'CRITICAL', 'WARNING', 'WARN', 'DEBUG', 'INFO', 'SUCCESS'}]
    if tags:
        provider = tags[-1]
    try:
        obj = json.loads(text[text.index("{"):])
        if isinstance(obj, dict):
            provider = str(obj.get("provider") or obj.get("dst") or obj.get("src") or provider).upper()
            reported_level = str(obj.get('level') or '').upper()
            if reported_level in {'ERROR', 'CRITICAL', 'WARN', 'WARNING', 'INFO', 'DEBUG'}:
                level = {'CRITICAL':'ERROR', 'WARNING':'WARN'}.get(reported_level, reported_level)
            if obj.get("event") == "debug":
                level = "DEBUG"
            if obj.get("event") in {"error", "run:error"} or str(obj.get("errors") or '0') != '0':
                level = "ERROR"
            elif str(obj.get("unresolved") or '0') != '0' or obj.get("event") in {"apply:unresolved", "warning"}:
                level = "WARN"
    except (ValueError, TypeError):
        pass
    if re.search(r"\b(?:Sync error|exit code: [1-9])", text, re.I):
        level = "ERROR"
    return text, level, provider


class LogArchive:
    def __init__(self, path: Path, *, max_bytes: int = MAX_BYTES, retention_days: int = RETENTION_DAYS):
        path.parent.mkdir(parents=True, exist_ok=True)
        self.lock = threading.RLock()
        self.max_bytes = max_bytes
        self.retention_days = retention_days
        self.db = sqlite3.connect(path, check_same_thread=False, timeout=5)
        self.db.row_factory = sqlite3.Row
        self.db.executescript("""
          PRAGMA journal_mode=WAL;
          PRAGMA synchronous=NORMAL;
          PRAGMA foreign_keys=ON;
          CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY, run_id TEXT, channel TEXT NOT NULL, label TEXT NOT NULL,
            started REAL NOT NULL, ended REAL, status TEXT NOT NULL, pairs TEXT NOT NULL DEFAULT '[]',
            pinned INTEGER NOT NULL DEFAULT 0, bytes INTEGER NOT NULL DEFAULT 0,
            lines INTEGER NOT NULL DEFAULT 0, errors INTEGER NOT NULL DEFAULT 0,
            warnings INTEGER NOT NULL DEFAULT 0, truncated INTEGER NOT NULL DEFAULT 0);
          CREATE TABLE IF NOT EXISTS lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT, session_id TEXT NOT NULL REFERENCES sessions(id) ON DELETE CASCADE,
            ts REAL NOT NULL, level TEXT NOT NULL, provider TEXT NOT NULL, text TEXT NOT NULL);
          CREATE INDEX IF NOT EXISTS lines_session ON lines(session_id,id);
          CREATE INDEX IF NOT EXISTS sessions_run ON sessions(run_id);
        """)
        columns = {row['name'] for row in self.db.execute('PRAGMA table_info(lines)')}
        for name in ('pair_id', 'run_id'):
            if name not in columns:
                self.db.execute(f"ALTER TABLE lines ADD COLUMN {name} TEXT NOT NULL DEFAULT ''")
        self.db.execute('CREATE INDEX IF NOT EXISTS lines_pair ON lines(session_id,pair_id,run_id,id)')
        self.db.execute("UPDATE sessions SET ended=?,status='interrupted' WHERE ended IS NULL", (time.time(),))
        self.db.commit()
        self.active: dict[str, str] = {}
        self.used = int(self.db.execute("SELECT COALESCE(SUM(bytes),0) FROM sessions").fetchone()[0])
        self.last_commit = 0.0
        self.last_cleanup = 0.0
        self.pending = 0
        self.cleanup()

    def flush(self):
        with self.lock:
            self.db.commit()
            self.pending = 0
            self.last_commit = time.monotonic()

    def cleanup(self, *, before: float | None = None):
        with self.lock:
            midnight = int(time.time() // 86400) * 86400
            self.db.execute("UPDATE sessions SET ended=?,status='saved' WHERE run_id IS NULL AND ended IS NULL AND started<?", (midnight, midnight))
            cutoff = before if before is not None else time.time() - self.retention_days * 86400
            self.db.execute("DELETE FROM sessions WHERE pinned=0 AND ended IS NOT NULL AND ended<?", (cutoff,))
            self.used = int(self.db.execute("SELECT COALESCE(SUM(bytes),0) FROM sessions").fetchone()[0])
            self.last_cleanup = time.monotonic()
            self.flush()

    def _room(self, size: int) -> bool:
        if self.used + size <= self.max_bytes:
            return True
        for row in self.db.execute("SELECT id,bytes FROM sessions WHERE pinned=0 AND ended IS NOT NULL ORDER BY started").fetchall():
            self.db.execute("DELETE FROM sessions WHERE id=?", (row['id'],))
            self.used -= row['bytes']
            if self.used + size <= self.max_bytes:
                return True
        return False

    def start(self, run_id: str, pairs: list[dict]) -> str:
        with self.lock:
            previous = self.active.get('sync')
            if previous:
                self.db.execute("UPDATE sessions SET ended=?,status='saved' WHERE id=?", (time.time(), previous))
            sid = uuid.uuid4().hex
            if not self._room(512):
                self.flush()
                self.active.pop('sync', None)
                return ''
            labels = [f"{p.get('source', '?')} → {p.get('target', '?')}" for p in pairs]
            self.db.execute("INSERT INTO sessions(id,run_id,channel,label,started,status,pairs,bytes) VALUES(?,?,?,?,?,'running',?,512)",
                            (sid, run_id, 'sync', ', '.join(dict.fromkeys(labels)) or 'Sync run', time.time(), json.dumps(pairs)))
            self.used += 512
            self.active['sync'] = sid
            self.flush()
            return sid

    def finish(self, sid: str, status: str):
        with self.lock:
            self.db.execute("UPDATE sessions SET ended=?,status=? WHERE id=?", (time.time(), status, sid))
            if self.active.get('sync') == sid:
                del self.active['sync']
            self.flush()

    def append(self, channel: str, tag: str, text: str):
        text, level, provider = describe_line(text, tag)
        if not text or '::CLEAR::' in text:
            return
        with self.lock:
            now = time.time()
            if time.monotonic() - self.last_cleanup > 3600:
                self.cleanup()
            sid = self.active.get(channel)
            if channel != 'sync' or not sid or sid.startswith('sync-'):
                day = time.strftime('%Y-%m-%d', time.gmtime(now))
                sid = f'{channel}-{day}'
                # Continuous logs close at midnight and resume the same day after a restart.
                if self.active.get(channel) != sid:
                    previous = self.active.get(channel)
                    if previous:
                        self.db.execute("UPDATE sessions SET ended=?,status='saved' WHERE id=?", (now, previous))
                    if not self.session(sid):
                        if not self._room(512):
                            self.flush()
                            return
                        self.db.execute("INSERT INTO sessions(id,channel,label,started,status,bytes) VALUES(?,?,?,?,?,512)",
                                        (sid, channel, f'{channel.title()} · {day} UTC', now, 'live'))
                        self.used += 512
                    self.db.execute("UPDATE sessions SET ended=NULL,status='live' WHERE id=?", (sid,))
                    self.active[channel] = sid
            pair_id, run_id = log_pair_id.get(), log_run_id.get()
            if channel == 'sync':
                session = self.session(sid)
                if session is None:
                    return
                run_id = session['run_id'] or ''
                pairs = json.loads(session['pairs'])
                if not pair_id and len(pairs) == 1:
                    pair_id = str(pairs[0].get('id') or '')
            elif channel == 'debug' and run_id and not pair_id:
                session = self.session(self.active.get('sync', ''))
                if session and session['run_id'] == run_id:
                    pairs = json.loads(session['pairs'])
                    if len(pairs) == 1:
                        pair_id = str(pairs[0].get('id') or '')
            # Pair-start messages are emitted before entering the feature's log scope.
            try:
                event = json.loads(text[text.index('{'):])
                if event.get('event') in {'run:pair', 'run:pair:skip', 'pair:skip'}:
                    pair_id = str(event.get('pair_id') or pair_id)
            except (ValueError, TypeError, AttributeError):
                pass
            size = len(text.encode('utf-8')) + len(pair_id) + len(run_id) + 128
            if not self._room(size):
                self.db.execute("UPDATE sessions SET truncated=1 WHERE id=?", (sid,))
            else:
                self.db.execute("INSERT INTO lines(session_id,ts,level,provider,text,pair_id,run_id) VALUES(?,?,?,?,?,?,?)", (sid, now, level, provider, text, pair_id, run_id))
                self.db.execute("UPDATE sessions SET bytes=bytes+?,lines=lines+1,errors=errors+?,warnings=warnings+? WHERE id=?",
                                (size, int(level == 'ERROR'), int(level == 'WARN'), sid))
                self.used += size
            self.pending += 1
            if self.pending >= 100 or time.monotonic() - self.last_commit > 1:
                self.flush()

    def sessions(self, channel: str, run_id: str = '', pair_id: str = '') -> list[dict]:
        with self.lock:
            self.cleanup()
            rows = self.db.execute("""SELECT * FROM sessions s WHERE channel=?
                AND (?='' OR s.run_id=? OR EXISTS(SELECT 1 FROM lines l WHERE l.session_id=s.id AND l.run_id=?))
                AND (?='' OR EXISTS(SELECT 1 FROM lines l WHERE l.session_id=s.id AND l.pair_id=? AND (?='' OR l.run_id=?)))
                ORDER BY started DESC LIMIT 5000""", (channel, run_id, run_id, run_id, pair_id, pair_id, run_id, run_id)).fetchall()
            return [dict(row) for row in rows]

    def session(self, sid: str) -> dict | None:
        with self.lock:
            row = self.db.execute('SELECT * FROM sessions WHERE id=?', (sid,)).fetchone()
            return dict(row) if row else None

    def page(self, sid: str, *, q: str = '', level: str = '', provider: str = '', after: int = 0, offset: int = 0, limit: int = 500, pair_id: str = '', run_id: str = '') -> dict:
        with self.lock:
            where = "session_id=? AND id>? AND (?='' OR instr(lower(text),lower(?))>0) AND (?='' OR level=?) AND (?='' OR provider=?) AND (?='' OR pair_id=?) AND (?='' OR run_id=?)"
            args = (sid, after, q, q, level, level, provider, provider, pair_id, pair_id, run_id, run_id)
            total = self.db.execute(f'SELECT COUNT(*) FROM lines WHERE {where}', args).fetchone()[0]
            rows = self.db.execute(f'SELECT * FROM lines WHERE {where} ORDER BY id LIMIT ? OFFSET ?', (*args, limit, offset)).fetchall()
            return dict(items=[dict(r) for r in rows], total=total)

    def counts(self, sid: str, pair_id: str = '', run_id: str = '') -> dict:
        with self.lock:
            row = self.db.execute("""SELECT COUNT(*) AS lines, COALESCE(SUM(level='ERROR'),0) AS errors,
                COALESCE(SUM(level='WARN'),0) AS warnings FROM lines WHERE session_id=?
                AND (?='' OR pair_id=?) AND (?='' OR run_id=?)""", (sid, pair_id, pair_id, run_id, run_id)).fetchone()
            return dict(row)

    def context(self, sid: str, line_id: int, pair_id: str = '', run_id: str = '') -> list[dict]:
        with self.lock:
            scope = "session_id=? AND (?='' OR pair_id=?) AND (?='' OR run_id=?)"
            args = (sid, pair_id, pair_id, run_id, run_id, line_id)
            before = self.db.execute(f'SELECT * FROM lines WHERE {scope} AND id<=? ORDER BY id DESC LIMIT 6', args).fetchall()
            after = self.db.execute(f'SELECT * FROM lines WHERE {scope} AND id>? ORDER BY id LIMIT 5', args).fetchall()
            return [dict(r) for r in reversed(before)] + [dict(r) for r in after]

    def export(self, sid: str, *, q: str = '', level: str = '', provider: str = '', pair_id: str = '', run_id: str = ''):
        # Bound each read so exporting a long log does not copy the archive into memory.
        with self.lock:
            end = self.db.execute('SELECT COALESCE(MAX(id),0) FROM lines WHERE session_id=?', (sid,)).fetchone()[0]
        after = 0
        while after < end:
            with self.lock:
                rows = self.db.execute("""SELECT * FROM lines WHERE session_id=? AND id>? AND id<=?
                    AND (?='' OR instr(lower(text),lower(?))>0) AND (?='' OR level=?)
                    AND (?='' OR provider=?) AND (?='' OR pair_id=?) AND (?='' OR run_id=?) ORDER BY id LIMIT 1000""",
                    (sid, after, end, q, q, level, level, provider, provider, pair_id, pair_id, run_id, run_id)).fetchall()
            if not rows:
                return
            for row in rows:
                timestamp = datetime.fromtimestamp(row['ts'], timezone.utc).isoformat(timespec='milliseconds')
                yield f"{timestamp} [{row['provider']}] {row['level']} {row['text']}\n"
            after = rows[-1]['id']

    def pin(self, sid: str, value: bool):
        with self.lock:
            self.db.execute('UPDATE sessions SET pinned=? WHERE id=?', (int(value), sid))
            self.flush()

    def delete(self, sid: str):
        with self.lock:
            row = self.session(sid)
            if row and row['ended'] is None:
                raise ValueError('This log is still active.')
            self.db.execute('DELETE FROM sessions WHERE id=?', (sid,))
            self.used -= int((row or {}).get('bytes', 0))
            self.flush()

    def close(self):
        with self.lock:
            self.flush()
            self.db.close()


_archive: LogArchive | None = None
_init_lock = threading.Lock()


def archive() -> LogArchive:
    global _archive
    with _init_lock:
        if _archive is None:
            _archive = LogArchive(crosswatch_db_path().with_name('logs.db'))
            atexit.register(_archive.close)
        return _archive


def capture(tag: str, text: str, *, watcher: bool = False):
    try:
        archive().append('sync' if tag == 'SYNC' else 'watcher' if watcher else 'debug', tag, text)
    except Exception:
        # Logging must never prevent a sync from completing.
        pass

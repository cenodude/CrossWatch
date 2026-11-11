# cw_platform/tracker_store.py
# CrossWatch - Local tracker storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import errno
import json
import logging
import re
import shutil
import sqlite3
import threading
import time
import uuid
import zipfile
import zlib
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from .history_events import history_event_key
from .id_map import canonical_key
from .provider_instances import _safe_profile_dir_name, normalize_instance_id

FEATURES: tuple[str, ...] = ("watchlist", "history", "ratings", "progress", "collection")
DB_NAME = "tracker.db"
SCHEMA_VERSION = 6
NOTES = "notes"
NOTE_STATUSES: tuple[str, ...] = ("watching", "planned", "completed", "on_hold", "dropped")
_NOTES_FILE = "notes.json"
CHANGE_KINDS: dict[str, str] = {
    "logged": "action='add'",
    "edited": "action='edit'",
    "deleted": "action='delete'",
    "notes": "action='note'",
    "undone": "undone_ns IS NOT NULL",
}
_NOTE_FIELDS: tuple[str, ...] = ("type", "title", "year", "tmdb", "status", "started", "finished", "notes", "tags", "fields")
CHANGE_RETENTION_DAYS = 90
CHANGE_LIMIT = 50_000
_SIDECAR_MARKERS = frozenset({
    "unresolved", "restore_state", "shadow", "blackbox", "flap", "hidden", "tomb", "tombstones", "pending",
})
_PLAYLISTS = "playlists"
_LOG = logging.getLogger("crosswatch.tracker_store")
_IMPORT_LOCK = threading.Lock()
_MUTATION_GUARD = threading.Lock()
_MUTATION_LOCKS: dict[str, threading.RLock] = {}
_READY: set[str] = set()
_CACHE: dict[tuple[Any, ...], tuple[str, Any]] = {}
_CACHE_LOCK = threading.Lock()
_GUARD = threading.local()
_MISSING = object()
_CACHE_LIMIT = 256
_SNAPSHOT_MAGIC = b"CWSP\x01"
_SNAPSHOT_COMPACT_MIN = 16 * 1024 * 1024
_SNAPSHOT_HEADROOM = 64 * 1024 * 1024
_SNAPSHOT_WAL_LIMIT = 64 * 1024 * 1024
_WHEN_FIELDS: tuple[str, ...] = ("watched_at", "rated_at", "progress_at", "collected_at", "listed_at", "added_at")
_ITEM_COLUMNS: tuple[tuple[str, str], ...] = (
    ("kind", "TEXT"), ("when_at", "TEXT"), ("show_key", "TEXT"), ("season", "INTEGER"), ("episode", "INTEGER"),
    ("find", "TEXT"), ("sort_title", "TEXT"), ("tmdb", "TEXT"), ("imdb", "TEXT"),
)
_UPSERT_ITEM = (
    f"INSERT INTO items(scope, feature, item_key, data, {', '.join(n for n, _ in _ITEM_COLUMNS)})"
    f" VALUES(?, ?, ?, ?, {', '.join('?' for _ in _ITEM_COLUMNS)})"
    " ON CONFLICT(scope, feature, item_key) DO UPDATE SET data=excluded.data, "
    + ", ".join(f"{n}=excluded.{n}" for n, _ in _ITEM_COLUMNS)
)
_SHOW_KINDS = ("show", "season", "episode")
_ITEM_NOTE_KEY = (
    "CASE WHEN items.kind IN ('episode', 'season') THEN 'show:' || coalesce(items.show_key, 'title:')"
    " WHEN items.kind IN ('show', 'anime') THEN 'show:' || CASE WHEN coalesce(items.tmdb, '') != '' THEN 'tmdb:' || items.tmdb"
    " ELSE 'title:' || cw_lower(json_extract(items.data, '$.title')) END"
    " WHEN coalesce(items.tmdb, '') != '' THEN 'movie:tmdb:' || items.tmdb"
    " WHEN coalesce(items.imdb, '') != '' THEN 'movie:imdb:' || items.imdb"
    " ELSE 'movie:key:' || CASE WHEN instr(items.item_key, '@') > 0 THEN substr(items.item_key, 1, instr(items.item_key, '@') - 1) ELSE items.item_key END END"
)

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS states(scope TEXT NOT NULL, feature TEXT NOT NULL, ts INTEGER NOT NULL,"
    " updated_ns INTEGER NOT NULL, PRIMARY KEY(scope, feature))",
    "CREATE TABLE IF NOT EXISTS items(scope TEXT NOT NULL, feature TEXT NOT NULL, item_key TEXT NOT NULL,"
    f" data TEXT NOT NULL, {', '.join(f'{n} {t}' for n, t in _ITEM_COLUMNS)}, PRIMARY KEY(scope, feature, item_key))",
    "CREATE TABLE IF NOT EXISTS docs(scope TEXT NOT NULL, name TEXT NOT NULL, data TEXT NOT NULL,"
    " updated_ns INTEGER NOT NULL, PRIMARY KEY(scope, name))",
    "CREATE TABLE IF NOT EXISTS restore_points(id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL,"
    " feature TEXT NOT NULL, name TEXT NOT NULL, ts INTEGER NOT NULL, created_ns INTEGER NOT NULL,"
    " data TEXT NOT NULL, UNIQUE(scope, feature, name))",
    "CREATE TABLE IF NOT EXISTS changes(id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT NOT NULL, scope TEXT NOT NULL,"
    " feature TEXT NOT NULL, action TEXT NOT NULL, old_key TEXT NOT NULL, new_key TEXT, before TEXT, after TEXT,"
    " created_ns INTEGER NOT NULL, undone_ns INTEGER, undo_of TEXT, find TEXT)",
    "CREATE TABLE IF NOT EXISTS checks(scope TEXT NOT NULL, feature TEXT NOT NULL, item_key TEXT NOT NULL,"
    " code TEXT NOT NULL, detail TEXT NOT NULL, checked_ns INTEGER NOT NULL, runtime INTEGER,"
    " PRIMARY KEY(scope, feature, item_key))",
    "CREATE TABLE IF NOT EXISTS notes(title_key TEXT PRIMARY KEY, type TEXT NOT NULL, title TEXT NOT NULL, year INTEGER,"
    " tmdb TEXT, status TEXT, started TEXT, finished TEXT, body TEXT, fields TEXT NOT NULL, find TEXT NOT NULL,"
    " updated_ns INTEGER NOT NULL)",
    "CREATE TABLE IF NOT EXISTS note_tags(title_key TEXT NOT NULL, tag TEXT NOT NULL COLLATE NOCASE,"
    " PRIMARY KEY(title_key, tag)) WITHOUT ROWID",
)
_INDEXES = (
    "CREATE INDEX IF NOT EXISTS items_when ON items(scope, feature, when_at, item_key, kind, find)",
    "CREATE INDEX IF NOT EXISTS items_show ON items(scope, feature, show_key, kind, season, episode, when_at, sort_title)"
    " WHERE show_key IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS items_title ON items(scope, feature, sort_title, season, episode, item_key)",
    "CREATE INDEX IF NOT EXISTS items_tmdb ON items(scope, tmdb) WHERE tmdb IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS items_imdb ON items(scope, imdb) WHERE imdb IS NOT NULL",
    "CREATE INDEX IF NOT EXISTS notes_status ON notes(status, updated_ns)",
    "CREATE INDEX IF NOT EXISTS notes_recent ON notes(updated_ns)",
    "CREATE INDEX IF NOT EXISTS note_tags_tag ON note_tags(tag, title_key)",
    "DROP INDEX IF EXISTS changes_batch",
    "DROP INDEX IF EXISTS changes_batch_scope",
    "CREATE INDEX IF NOT EXISTS changes_filter ON changes(batch, scope, action, undone_ns, find)",
    "CREATE INDEX IF NOT EXISTS changes_created ON changes(created_ns)",
)
ATTENTION_CODES: tuple[str, ...] = ("no_tmdb", "not_found", "mismatch", "incomplete", "episode_missing", "episode_unverified")
_EDIT_FIELDS: tuple[str, ...] = (
    "type", "title", "year", "series_title", "series_year", "ids", "show_ids", "season", "episode", "rating",
    "rated_at", "watched_at", "progress_at", "progress_percent", "progress_ms", "collected_at",
)

LogFn = Callable[..., None]
ProgressFn = Callable[[int, int, int, int], None]


def _default_log(feature: str, level: str, msg: str, **fields: Any) -> None:
    _LOG.log(logging.WARNING if level in ("warn", "error") else logging.DEBUG, "%s %s %s", feature, msg, fields)


class TrackerConflict(RuntimeError):
    def __init__(self, code: str, key: str | None = None):
        super().__init__(code)
        self.code = code
        self.key = key


class TrackerUnreadable(RuntimeError):
    def __init__(self, feature: str, names: list[str]):
        super().__init__(f"crosswatch_{feature}_state_unreadable:{names[0]}")
        self.feature = feature
        self.names = names


@dataclass(frozen=True)
class StateRead:
    raw: Any
    restored_from: str | None = None


@dataclass(frozen=True)
class ChangeBatch:
    batch: str
    feature: str
    action: str
    created_ns: int
    undone_ns: int | None
    undo_of: str | None
    count: int
    rows: list[tuple[str, str | None, Any, Any]]


@dataclass(frozen=True)
class RestorePoint:
    name: str
    feature: str
    ts: int
    size: int
    mtime: float


def _name_ts(name: str) -> int | None:
    try:
        stamp = datetime.strptime(name.split("-", 1)[0], "%Y%m%dT%H%M%SZ")
    except Exception:
        return None
    return int(stamp.replace(tzinfo=timezone.utc).timestamp())


def _iso(ns: int) -> str | None:
    if not ns:
        return None
    return datetime.fromtimestamp(ns / 1e9, timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _json_filename(name: str, *, label: str) -> str:
    raw = str(name or "").strip()
    if not raw or not raw.lower().endswith(".json"):
        raise ValueError(f"Invalid {label}")
    posix = PurePosixPath(raw)
    win = PureWindowsPath(raw)
    if posix.name != raw or win.name != raw:
        raise ValueError(f"Invalid {label}")
    if posix.is_absolute() or win.is_absolute() or win.drive or win.root:
        raise ValueError(f"Invalid {label}")
    if raw in (".", ".."):
        raise ValueError(f"Invalid {label}")
    return raw


def _upload_items(raw: Mapping[str, Any], feature: str) -> dict[str, Any]:
    body = raw.get("items") if "items" in raw else None
    if isinstance(body, list) and body and all(isinstance(row, Mapping) and str(row.get("key") or "").strip() for row in body):
        return {str(row["key"]).strip(): {k: v for k, v in row.items() if k != "key"} for row in body}
    return _state_payload(raw, feature)[1]


class SnapshotMaintenanceDeferred(RuntimeError):
    pass


def _snapshot_failure_reason(exc: Exception) -> str | None:
    code = getattr(exc, "sqlite_errorcode", 0) & 255
    if code in (sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED):
        return "busy"
    if code == sqlite3.SQLITE_FULL or isinstance(exc, OSError) and exc.errno == errno.ENOSPC:
        return "disk_space"
    return None


def _snapshot_text(data: str | bytes) -> str:
    if isinstance(data, bytes):
        if not data.startswith(_SNAPSHOT_MAGIC):
            raise ValueError("Unknown snapshot encoding")
        return zlib.decompress(data[len(_SNAPSHOT_MAGIC):]).decode("utf-8")
    return data


def _pack_snapshot(text: str) -> bytes:
    return _SNAPSHOT_MAGIC + zlib.compress(text.encode("utf-8"))


def _snapshot_payload(text: str | bytes, feature: str) -> dict[str, Any]:
    ts, items = _state_payload(json.loads(_snapshot_text(text)), feature)
    return {"ts": ts, "items": items}


def _list_key(feature: str, item: Mapping[str, Any]) -> str:
    try:
        return history_event_key(item) if feature == "history" else canonical_key(item)
    except Exception:
        return ""


def _list_items(feature: str, raw: list[Any]) -> dict[str, Any]:
    items: dict[str, Any] = {}
    for pos, value in enumerate(raw):
        key = _list_key(feature, value) if isinstance(value, Mapping) else ""
        items[key if key and not key.startswith("unknown:") else str(pos)] = value
    return items


def _state_payload(raw: Any, feature: str) -> tuple[int, dict[str, Any]]:
    ts = 0
    if isinstance(raw, Mapping) and "items" in raw:
        try:
            ts = int(raw.get("ts") or 0)
        except Exception:
            ts = 0
        body = raw.get("items")
        if not isinstance(body, Mapping):
            raise ValueError("Tracker state has no item map")
        items = {str(k): v for k, v in body.items()}
    elif isinstance(raw, Mapping):
        items = {str(k): v for k, v in raw.items()}
    elif isinstance(raw, list):
        items = _list_items(feature, raw)
    else:
        raise ValueError("Tracker state is not an object or a list")
    if any(not isinstance(v, Mapping) for v in items.values()):
        raise ValueError("Tracker state holds a value that is not a record")
    return ts, items


def _text(value: Any) -> str | None:
    if value is None or isinstance(value, (dict, list)):
        return None
    if isinstance(value, bool):
        return "1" if value else "0"
    return str(value)


def _number(value: Any) -> Any:
    return value if isinstance(value, (int, float, str)) and not isinstance(value, bool) else None


def _columns(key: str, data: Any) -> tuple[Any, ...]:
    item = _map_of(data)
    ids = _map_of(item.get("ids"))
    show_ids = _map_of(item.get("show_ids"))
    title, series, show_tmdb = _text(item.get("title")), _text(item.get("series_title")), _text(show_ids.get("tmdb"))
    if show_tmdb is not None:
        show_key: str | None = f"tmdb:{show_tmdb}"
    else:
        show_key = f"title:{series.lower()}" if series is not None else None
    when = next((t for t in (_text(item.get(f)) for f in _WHEN_FIELDS) if t is not None), "")
    name = series if series is not None else title if title is not None else key
    kind = _text(item.get("type"))
    own = kind not in ("episode", "season")
    return (
        kind, when, show_key, _number(item.get("season")), _number(item.get("episode")),
        f"{title or ''} {series or ''}".lower(), name.lower(),
        _text(ids.get("tmdb")) if own else None, _text(ids.get("imdb")) if own else None,
    )


def _map_of(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _change_find(*rows: Any) -> str:
    names: list[str] = []
    for row in rows:
        data = _safe_loads(row) if isinstance(row, str) else row
        if not isinstance(data, Mapping):
            continue
        for field in ("series_title", "title"):
            text = _text(data.get(field))
            if text and text.strip() and text.lower() not in names:
                names.append(text.lower())
    return " ".join(names)


def _note_norm(note: Mapping[str, Any]) -> dict[str, Any]:
    tags: dict[str, str] = {}
    for tag in note.get("tags") or []:
        tags.setdefault(str(tag).lower(), str(tag))
    fields = note.get("fields")
    out = {name: note.get(name) or None for name in _NOTE_FIELDS}
    out.update(
        type=str(note.get("type") or "movie"), title=str(note.get("title") or ""),
        tags=sorted(tags.values(), key=str.lower), fields=dict(fields) if isinstance(fields, Mapping) else {},
    )
    return out


def _rev(conn: sqlite3.Connection) -> str:
    row = conn.execute("SELECT value FROM meta WHERE key='rev'").fetchone()
    return str(row[0]) if row else ""


def _runtime(detail: Mapping[str, Any]) -> int | None:
    try:
        value = int(detail.get("runtime"))  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _safe_loads(text: Any) -> Any:
    try:
        return json.loads(text)
    except Exception:
        return None


def _loads(text: Any) -> Any:
    return None if text is None else json.loads(text)


def _edit_view(data: Any) -> dict[str, Any]:
    src = data if isinstance(data, Mapping) else {}
    return {field: src.get(field) for field in _EDIT_FIELDS}


def _file_target(name: str) -> tuple[str, str, str] | None:
    parts = name.split(".")
    if len(parts) < 2 or parts[-1] != "json" or not parts[0]:
        return None
    body = parts[:-1]
    if body[0] in FEATURES:
        if len(body) == 1:
            return "state", "", body[0]
        if len(body) == 2:
            return ("doc", "", ".".join(body)) if body[1] in _SIDECAR_MARKERS else ("state", body[1], body[0])
        if len(body) == 3 and body[1] in _SIDECAR_MARKERS and body[2] not in _SIDECAR_MARKERS:
            return "doc", body[2], ".".join(body[:2])
    return "doc", "", ".".join(body)


def _file_name(scope: str, key: str) -> str:
    return f"{key}.{scope}.json" if scope else f"{key}.json"


def _snapshot_feature(name: str) -> str | None:
    return next((feature for feature in FEATURES if name.endswith(f"-{feature}.json")), None)


def _configured_instance(cfg: Mapping[str, Any], provider_instance: Any = None) -> tuple[str, Mapping[str, Any]]:
    requested = normalize_instance_id(provider_instance)
    if requested == "default":
        return "default", {}
    node = cfg.get("crosswatch") if isinstance(cfg, Mapping) else {}
    instances = node.get("instances") if isinstance(node, Mapping) else {}
    if not isinstance(instances, Mapping):
        return "default", {}
    for key, block in instances.items():
        candidate = normalize_instance_id(key)
        if candidate == requested:
            return candidate, block if isinstance(block, Mapping) else {}
    return "default", {}


def _resolve(raw: Any, config_dir: Path | str) -> Path | None:
    text = str(raw or "").strip()
    if not text:
        return None
    path = Path(text)
    return path if path.is_absolute() else Path(config_dir) / path


def tracker_base_root(cfg: Mapping[str, Any], config_dir: Path | str) -> Path:
    node = cfg.get("crosswatch") if isinstance(cfg, Mapping) else {}
    raw = (node.get("root_dir") or node.get("root") or node.get("dir")) if isinstance(node, Mapping) else None
    return _resolve(raw, config_dir) or Path(config_dir) / ".cw_provider"


def tracker_context(cfg: Mapping[str, Any], config_dir: Path | str, provider_instance: Any = None) -> tuple[str, Path]:
    inst, block = _configured_instance(cfg, provider_instance)
    base = tracker_base_root(cfg, config_dir)
    if inst == "default":
        return inst, base
    return inst, _resolve(block.get("root_dir"), config_dir) or base / "profiles" / _safe_profile_dir_name(inst)


def tracker_roots(cfg: Mapping[str, Any], config_dir: Path | str) -> list[tuple[str, Path]]:
    roots: list[tuple[str, Path]] = [("default", tracker_base_root(cfg, config_dir))]
    node = cfg.get("crosswatch") if isinstance(cfg, Mapping) else {}
    instances = node.get("instances") if isinstance(node, Mapping) else {}
    if isinstance(instances, Mapping):
        for raw_id in instances.keys():
            if normalize_instance_id(raw_id) != "default":
                roots.append(tracker_context(cfg, config_dir, raw_id))
    return roots


def tracker_snapshot_settings(cfg: Mapping[str, Any], instance: str = "default") -> dict[str, Any]:
    base = cfg.get("crosswatch") or cfg.get("CrossWatch") or {}
    base = base if isinstance(base, Mapping) else {}
    _, block = _configured_instance(cfg, instance)
    return {**base, **block}


class TrackerBackupPending(RuntimeError):
    pass


class TrackerStore:
    def __init__(
        self,
        root: Path | str,
        *,
        scope: str | None = None,
        chained: bool = True,
        log: LogFn | None = None,
        migration_settings: Mapping[str, Any] | None = None,
    ) -> None:
        self.root = Path(root)
        self._migration_settings = migration_settings
        self.scope = scope
        self.chained = chained
        self._scope = scope or ""
        self._log = log or _default_log
        self._shared: sqlite3.Connection | None = None

    @property
    def db_path(self) -> Path:
        return self.root / DB_NAME

    def revision(self) -> str:
        with self._db() as conn:
            return _rev(conn) if conn is not None else ""

    def mutation(self) -> threading.RLock:
        key = str(self.root.resolve())
        with _MUTATION_GUARD:
            return _MUTATION_LOCKS.setdefault(key, threading.RLock())

    def _legacy_files(self) -> tuple[list[Path], Path | None]:
        if not self.root.is_dir():
            return [], None
        files = [p for p in sorted(self.root.glob("*.json")) if p.is_file() and _file_target(p.name)]
        snaps = self.root / "snapshots"
        return files, snaps if snaps.is_dir() else None

    def _connect(self) -> sqlite3.Connection:
        self.root.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(str(self.db_path), timeout=30.0, isolation_level=None, check_same_thread=False)
        try:
            conn.create_function("cw_lower", 1, lambda value: str(value or "").lower(), deterministic=True)
            conn.execute("PRAGMA synchronous=NORMAL")
            conn.execute("PRAGMA cache_size=-32768")
            if conn.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
                conn.execute("PRAGMA journal_mode=WAL")
                conn.execute("BEGIN IMMEDIATE")
                if conn.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
                    self._upgrade(conn)
                    conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                conn.execute("COMMIT")
        except Exception:
            conn.close()
            raise
        return conn

    @staticmethod
    def _upgrade(conn: sqlite3.Connection) -> None:
        for statement in _SCHEMA:
            conn.execute(statement)
        have = {str(row[1]) for row in conn.execute("PRAGMA table_info(items)").fetchall()}
        missing = [(name, typ) for name, typ in _ITEM_COLUMNS if name not in have]
        for name, typ in missing:
            conn.execute(f"ALTER TABLE items ADD COLUMN {name} {typ}")
        if missing:
            fill = f"UPDATE items SET {', '.join(f'{n}=?' for n, _ in _ITEM_COLUMNS)} WHERE rowid=?"
            after = 0
            while rows := conn.execute(
                "SELECT rowid, item_key, data FROM items WHERE rowid > ? ORDER BY rowid LIMIT 5000", (after,)
            ).fetchall():
                conn.executemany(fill, [(*_columns(str(key), _safe_loads(data)), rowid) for rowid, key, data in rows])
                after = int(rows[-1][0])
        if "runtime" not in {str(row[1]) for row in conn.execute("PRAGMA table_info(checks)").fetchall()}:
            conn.execute("ALTER TABLE checks ADD COLUMN runtime INTEGER")
            rows = conn.execute("SELECT rowid, detail FROM checks").fetchall()
            conn.executemany(
                "UPDATE checks SET runtime=? WHERE rowid=?",
                [(_runtime(d), rowid) for rowid, d in ((r[0], _safe_loads(r[1])) for r in rows) if isinstance(d, Mapping)],
            )
        if "find" not in {str(row[1]) for row in conn.execute("PRAGMA table_info(changes)").fetchall()}:
            conn.execute("ALTER TABLE changes ADD COLUMN find TEXT")
            rows = conn.execute("SELECT id, before, after FROM changes").fetchall()
            conn.executemany("UPDATE changes SET find=? WHERE id=?", [(_change_find(b, a), i) for i, b, a in rows])
        for statement in _INDEXES:
            conn.execute(statement)

    def _cached(self, conn: sqlite3.Connection, name: str, args: tuple[Any, ...], compute: Callable[[], Any]) -> Any:
        rev = _rev(conn)
        key = (str(self.db_path), name, args)
        with _CACHE_LOCK:
            hit = _CACHE.get(key)
        if hit is not None and hit[0] == rev:
            return hit[1]
        value = compute()
        with _CACHE_LOCK:
            if len(_CACHE) >= _CACHE_LIMIT:
                _CACHE.clear()
            _CACHE[key] = (rev, value)
        return value

    @contextmanager
    def session(self) -> Generator[TrackerStore]:
        if self._shared is not None:
            yield self
            return
        with self._db() as conn:
            self._shared = conn
            try:
                yield self
            finally:
                self._shared = None

    @contextmanager
    def _db(self, *, create: bool = False) -> Generator[sqlite3.Connection | None]:
        if self._shared is not None:
            yield self._shared
            return
        key = str(self.db_path)
        if key not in _READY or not self.db_path.exists():
            files, snaps = self._legacy_files()
            if not create and not self.db_path.exists() and not files and snaps is None:
                yield None
                return
            conn = self._connect()
            try:
                if files or snaps is not None or self.needs_migration():
                    self._import_legacy(conn, files, snaps)
                _READY.add(key)
            except Exception:
                conn.close()
                raise
        else:
            conn = self._connect()
        try:
            yield conn
        finally:
            conn.close()

    @contextmanager
    def _write(self, conn: sqlite3.Connection) -> Generator[sqlite3.Connection]:
        conn.execute("BEGIN IMMEDIATE")
        try:
            yield conn
        except BaseException:
            if conn.in_transaction:
                conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    def _migrate_snapshots(
        self,
        conn: sqlite3.Connection,
        progress: Callable[[int, int, int], None] | None = None,
        cancelled: Callable[[], bool] | None = None,
    ) -> dict[str, int]:
        total = int(conn.execute("SELECT count(*) FROM restore_points WHERE typeof(data)='text'").fetchone()[0])
        done = saved = after = 0
        if progress:
            progress(done, total, saved)
        while not cancelled or not cancelled():
            point = conn.execute(
                "SELECT id, data FROM restore_points WHERE id>? AND typeof(data)='text' ORDER BY id LIMIT 1",
                (after,),
            ).fetchone()
            if point is None:
                break
            packed = _pack_snapshot(point[1])
            if _snapshot_text(packed) != point[1]:
                raise ValueError("Snapshot compression verification failed")
            raw_size = len(point[1].encode("utf-8"))
            if shutil.disk_usage(self.root).free < _SNAPSHOT_HEADROOM + 4 * (raw_size + len(packed)):
                raise SnapshotMaintenanceDeferred("disk_space")
            checkpoint = conn.execute("PRAGMA wal_checkpoint(PASSIVE)").fetchone()
            page_size = int(conn.execute("PRAGMA page_size").fetchone()[0])
            if max(0, checkpoint[1] - checkpoint[2]) * page_size >= _SNAPSHOT_WAL_LIMIT:
                raise SnapshotMaintenanceDeferred("wal_pressure")
            with self._write(conn):
                changed = conn.execute(
                    "UPDATE restore_points SET data=? WHERE id=? AND data=?", (packed, point[0], point[1]),
                ).rowcount
            after = int(point[0])
            done += 1
            if changed:
                saved += raw_size - len(packed)
            if progress:
                progress(done, total, saved)
        return {"done": done, "total": total, "bytes_saved": saved}

    def _compact_snapshots(self, conn: sqlite3.Connection) -> str:
        if conn.in_transaction:
            return "busy"
        try:
            now = int(time.time())
            row = conn.execute("SELECT value FROM meta WHERE key='snapshot_compact_checked'").fetchone()
            if row and now - int(row[0]) < 86400:
                return "skipped"
            pages = int(conn.execute("PRAGMA page_count").fetchone()[0])
            free = int(conn.execute("PRAGMA freelist_count").fetchone()[0])
            size = int(conn.execute("PRAGMA page_size").fetchone()[0])
            if free * size >= _SNAPSHOT_COMPACT_MIN and free >= pages / 4:
                if shutil.disk_usage(self.root).free < pages * size * 2:
                    self._log("common", "warn", "snapshot_compaction_deferred", reason="disk_space")
                    return "disk_space"
                conn.execute("VACUUM")
                checkpoint = conn.execute("PRAGMA wal_checkpoint(TRUNCATE)").fetchone()
                if checkpoint[0]:
                    return "busy"
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('snapshot_compact_checked', ?)"
                " ON CONFLICT(key) DO UPDATE SET value=excluded.value", (str(now),),
            )
            return "completed"
        except (OSError, sqlite3.Error) as e:
            self._log("common", "warn", "snapshot_compaction_deferred", error=str(e))
            reason = _snapshot_failure_reason(e)
            if reason:
                return reason
            raise

    @staticmethod
    def _bump(conn: sqlite3.Connection) -> None:
        conn.execute(
            "INSERT INTO meta(key, value) VALUES('rev', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            (uuid.uuid4().hex,),
        )

    def needs_migration(self) -> bool:
        files, snaps = self._legacy_files()
        if files or snaps is not None:
            return True
        if not self.db_path.is_file():
            return False
        with closing(sqlite3.connect(self.db_path.resolve().as_uri() + "?mode=ro", uri=True)) as conn:
            if not conn.execute("SELECT 1 FROM sqlite_master WHERE name='meta'").fetchone():
                return False
            row = conn.execute("SELECT value FROM meta WHERE key='legacy_import'").fetchone()
            return bool(row and json.loads(row[0]).get("backup_pending"))

    def migrate(self, progress: ProgressFn | None = None) -> dict[str, Any] | None:
        files, snaps = self._legacy_files()
        if not files and snaps is None and not self.needs_migration():
            return None
        conn = self._connect()
        try:
            stats = self._import_legacy(conn, files, snaps, progress)
            _READY.add(str(self.db_path))
        finally:
            conn.close()
        return stats

    def _legacy_snapshot_selection(self, conn: sqlite3.Connection, snap_files: list[tuple[Path, tuple[str, ...]]]) -> set[Path]:
        settings = self._migration_settings
        if settings is None:
            from .config_base import CONFIG, load_config

            cfg = load_config()
            settings = {}
            for instance, root in tracker_roots(cfg, CONFIG):
                if root.resolve() == self.root.resolve():
                    settings = tracker_snapshot_settings(cfg, instance)
                    break
        days = max(0, int(settings.get("retention_days", 30)))
        limit = max(0, int(settings.get("max_snapshots", 14)))
        groups: dict[tuple[str, str], list[Path]] = {}
        for path, rel in snap_files:
            feature = _snapshot_feature(path.name)
            if feature and len(rel) <= 2 and path.is_file():
                groups.setdefault((rel[0] if len(rel) == 2 else "", feature), []).append(path)
        selected: set[Path] = set()
        now = time.time_ns()
        for (scope, feature), paths in groups.items():
            paths.sort(key=lambda p: (p.stat().st_mtime_ns, p.name), reverse=True)
            eligible = [p for p in paths if not days or now - p.stat().st_mtime_ns <= days * 86400_000_000_000]
            selected.update(eligible[:limit] if limit else eligible)
            pinned = str(settings.get(f"restore_{feature}") or "").strip()
            if pinned.lower() not in {"", "latest", "last"}:
                selected.update(p for p in paths if pinned in (p.name, p.stem))
            if not self._state_row(conn, scope, feature):
                for path in paths:
                    try:
                        _snapshot_payload(path.read_text("utf-8"), feature)
                    except Exception:
                        continue
                    selected.add(path)
                    break
                else:
                    raise TrackerUnreadable(feature, [paths[0].relative_to(self.root).as_posix()])
        return selected

    def _finish_legacy_backup(self, conn: sqlite3.Connection, stats: dict[str, Any]) -> dict[str, Any]:
        if not stats.get("backup_pending"):
            return stats
        backup = self.root / stats["backup"]
        for name in list(stats["backup_pending"]):
            source, target = self.root / name, backup / name
            try:
                backup.mkdir(parents=True, exist_ok=True)
                if source.exists():
                    if target.exists():
                        raise FileExistsError(str(target))
                    source.rename(target)
                elif not target.exists():
                    raise FileNotFoundError(str(source))
                stats["backup_pending"].remove(name)
                stats["moved"] = int(stats.get("moved", 0)) + 1
                with self._write(conn):
                    conn.execute("UPDATE meta SET value=? WHERE key='legacy_import'", (_dumps(stats),))
            except (OSError, sqlite3.Error) as exc:
                raise TrackerBackupPending(f"Original JSON backup is incomplete: {name}") from exc
        return stats

    def _import_legacy(
        self,
        conn: sqlite3.Connection,
        files: list[Path],
        snaps: Path | None,
        progress: ProgressFn | None = None,
    ) -> dict[str, Any] | None:
        with _IMPORT_LOCK:
            previous = conn.execute("SELECT value FROM meta WHERE key='legacy_import'").fetchone()
            if previous:
                stats = json.loads(previous[0])
                if "backup_pending" not in stats and (files or snaps is not None):
                    stats.update(
                        backup=f"json-backup-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}",
                        backup_pending=[p.name for p in [*files, *([snaps] if snaps is not None else [])]], moved=0,
                    )
                    try:
                        with self._write(conn):
                            conn.execute("UPDATE meta SET value=? WHERE key='legacy_import'", (_dumps(stats),))
                    except sqlite3.Error as exc:
                        raise TrackerBackupPending("Could not record the remaining original JSON backup") from exc
                return self._finish_legacy_backup(conn, stats) if stats.get("backup_pending") else None
            snap_files = [(p, p.relative_to(snaps).parts) for p in sorted(snaps.rglob("*.json"))] if snaps is not None else []
            sizes = {path: _size(path) for path in [*files, *(path for path, _ in snap_files)]}
            total_files, total_bytes = len(sizes), sum(sizes.values())
            done_files = done_bytes = 0

            def advance(path: Path) -> None:
                nonlocal done_files, done_bytes
                done_files += 1
                done_bytes += sizes.get(path, 0)
                if progress is not None:
                    progress(done_files, total_files, done_bytes, total_bytes)

            with self._write(conn):
                if conn.execute("SELECT 1 FROM meta WHERE key='legacy_import'").fetchone():
                    return None
                if progress is not None:
                    progress(0, total_files, 0, total_bytes)
                states = docs = points = 0
                for path in files:
                    target = _file_target(path.name)
                    if target is None:
                        advance(path)
                        continue
                    kind, scope, key = target
                    try:
                        text = path.read_text("utf-8")
                        raw = json.loads(text)
                        mtime_ns = path.stat().st_mtime_ns
                    except Exception:
                        if kind == "state" or key == _PLAYLISTS:
                            raise TrackerUnreadable(key, [path.name])
                        self._log("common", "warn", "legacy_sidecar_skipped", file=path.name)
                        advance(path)
                        continue
                    if kind == "state":
                        try:
                            ts, items = _state_payload(raw, key)
                        except ValueError:
                            raise TrackerUnreadable(key, [path.name])
                        self._put_state(conn, scope, key, items, ts=ts, updated_ns=mtime_ns)
                        states += 1
                    else:
                        self._put_doc(conn, scope, key, raw, updated_ns=mtime_ns)
                        docs += 1
                    advance(path)
                selected = self._legacy_snapshot_selection(conn, snap_files)
                for path, rel in snap_files:
                    feature = _snapshot_feature(path.name)
                    if feature is None or path not in selected:
                        advance(path)
                        continue
                    try:
                        text = path.read_text("utf-8")
                        _snapshot_payload(text, feature)
                        mtime_ns = path.stat().st_mtime_ns
                    except Exception:
                        raise TrackerUnreadable(feature, ["/".join(["snapshots", *rel])])
                    self._put_restore_point(conn, rel[0] if len(rel) == 2 else "", feature, path.name, text, created_ns=mtime_ns)
                    points += 1
                    advance(path)
                stats = {
                    "ts": int(time.time()), "states": states, "docs": docs, "restore_points": points,
                    "snapshots_backup_only": len(snap_files) - points,
                    "backup": f"json-backup-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}-{uuid.uuid4().hex[:8]}",
                    "backup_pending": [p.name for p in [*files, *([snaps] if snaps is not None else [])]], "moved": 0,
                }
                conn.execute("INSERT INTO meta(key, value) VALUES('legacy_import', ?)", (_dumps(stats),))
            return self._finish_legacy_backup(conn, stats)

    @staticmethod
    def _put_state(
        conn: sqlite3.Connection,
        scope: str,
        feature: str,
        items: Mapping[str, Any],
        *,
        ts: int,
        updated_ns: int | None = None,
    ) -> None:
        if any(not isinstance(value, Mapping) for value in items.values()):
            raise ValueError(f"{feature} state holds a value that is not a record")
        fresh = {str(key): (_dumps(value), value) for key, value in items.items()}
        stored = dict(conn.execute("SELECT item_key, data FROM items WHERE scope=? AND feature=?", (scope, feature)).fetchall())
        gone = [(scope, feature, key) for key in stored.keys() - fresh.keys()]
        put = [(scope, feature, key, text, *_columns(key, value)) for key, (text, value) in fresh.items() if stored.get(key) != text]
        conn.executemany("DELETE FROM items WHERE scope=? AND feature=? AND item_key=?", gone)
        conn.executemany(_UPSERT_ITEM, put)
        if gone or put:
            TrackerStore._bump(conn)
        conn.execute(
            "INSERT INTO states(scope, feature, ts, updated_ns) VALUES(?, ?, ?, ?)"
            " ON CONFLICT(scope, feature) DO UPDATE SET ts=excluded.ts, updated_ns=excluded.updated_ns",
            (scope, feature, int(ts), int(updated_ns or time.time_ns())),
        )

    @staticmethod
    def _put_doc(conn: sqlite3.Connection, scope: str, name: str, data: Any, *, updated_ns: int | None = None) -> None:
        conn.execute(
            "INSERT INTO docs(scope, name, data, updated_ns) VALUES(?, ?, ?, ?)"
            " ON CONFLICT(scope, name) DO UPDATE SET data=excluded.data, updated_ns=excluded.updated_ns",
            (scope, name, _dumps(data), int(updated_ns or time.time_ns())),
        )

    @staticmethod
    def _put_restore_point(
        conn: sqlite3.Connection,
        scope: str,
        feature: str,
        name: str,
        text: str,
        *,
        created_ns: int,
    ) -> None:
        ts = _name_ts(name)
        conn.execute(
            "INSERT INTO restore_points(scope, feature, name, ts, created_ns, data) VALUES(?, ?, ?, ?, ?, ?)"
            " ON CONFLICT(scope, feature, name) DO UPDATE SET ts=excluded.ts, created_ns=excluded.created_ns,"
            " data=excluded.data",
            (scope, feature, name, ts if ts is not None else created_ns // 1_000_000_000, created_ns, _pack_snapshot(text)),
        )

    @staticmethod
    def _state_row(conn: sqlite3.Connection, scope: str, feature: str) -> tuple[int, int] | None:
        row = conn.execute("SELECT ts, updated_ns FROM states WHERE scope=? AND feature=?", (scope, feature)).fetchone()
        return (int(row[0]), int(row[1])) if row else None

    @staticmethod
    def _items(conn: sqlite3.Connection, scope: str, feature: str) -> dict[str, Any]:
        rows = conn.execute(
            "SELECT item_key, data FROM items WHERE scope=? AND feature=? ORDER BY item_key", (scope, feature)
        ).fetchall()
        try:
            out = {str(key): json.loads(data) for key, data in rows}
        except Exception:
            raise TrackerUnreadable(feature, [DB_NAME])
        if any(not isinstance(value, Mapping) for value in out.values()):
            raise TrackerUnreadable(feature, [DB_NAME])
        return out

    def _own_state(self, conn: sqlite3.Connection, feature: str) -> tuple[int, int] | None:
        own = self._state_row(conn, self._scope, feature)
        if own is not None or not self._scope:
            return own
        legacy = self._state_row(conn, "", feature)
        if legacy is None:
            return None
        with self._write(conn):
            self._put_state(conn, self._scope, feature, self._items(conn, "", feature), ts=legacy[0], updated_ns=legacy[1])
        return legacy

    def _latest_scope(self, conn: sqlite3.Connection, feature: str) -> str | None:
        row = conn.execute(
            "SELECT scope FROM states WHERE feature=? ORDER BY updated_ns DESC, scope LIMIT 1", (feature,)
        ).fetchone()
        return str(row[0]) if row else None

    def _read_scope(self, conn: sqlite3.Connection, feature: str, *, fallback: bool = True) -> str | None:
        own = self._own_state(conn, feature)
        latest = self._latest_scope(conn, feature) if self.chained else None
        if self.scope is not None and latest is not None and latest != self._scope:
            if own is None:
                return latest
            latest_row = self._state_row(conn, latest, feature)
            if latest_row and latest_row[1] > own[1]:
                return latest
        if own is not None:
            return self._scope
        return latest if fallback and latest is not None and latest != self._scope else None

    def _payload(self, conn: sqlite3.Connection, scope: str, feature: str) -> dict[str, Any]:
        row = self._state_row(conn, scope, feature)
        return {"ts": row[0] if row else 0, "items": self._items(conn, scope, feature)}

    def _fallback_point(self, conn: sqlite3.Connection, feature: str) -> tuple[str, str | bytes] | None:
        if self.chained:
            row = conn.execute(
                "SELECT name, data FROM restore_points WHERE feature=? ORDER BY created_ns DESC, id DESC LIMIT 1",
                (feature,),
            ).fetchone()
        else:
            row = conn.execute(
                "SELECT name, data FROM restore_points WHERE feature=? AND scope IN ('', ?)"
                " ORDER BY created_ns DESC, id DESC LIMIT 1",
                (feature, self._scope),
            ).fetchone()
        return (str(row[0]), row[1]) if row else None

    @contextmanager
    def guarded(self) -> Generator[None]:
        revs = getattr(_GUARD, "revs", None)
        owner = revs is None
        if owner:
            revs = _GUARD.revs = {}
        key = str(self.db_path)
        nested = key in revs
        if not nested:
            revs[key] = None
        try:
            yield
        finally:
            if not nested:
                revs.pop(key, None)
            if owner:
                _GUARD.revs = None

    def _guard_read(self, conn: sqlite3.Connection | None) -> None:
        revs = getattr(_GUARD, "revs", None)
        key = str(self.db_path)
        if revs is not None and key in revs and revs[key] is None:
            revs[key] = _rev(conn) if conn is not None else ""

    def _guard_write(self, conn: sqlite3.Connection, *, after: bool) -> None:
        revs = getattr(_GUARD, "revs", None)
        key = str(self.db_path)
        if revs is None or key not in revs:
            return
        if after:
            revs[key] = _rev(conn)
        elif revs[key] is not None and _rev(conn) != revs[key]:
            raise TrackerConflict("changed_since")

    def read_state(self, feature: str, *, snapshot_fallback: bool = True) -> StateRead | None:
        try:
            with self._db() as conn:
                self._guard_read(conn)
                if conn is None:
                    return None
                scope = self._read_scope(conn, feature)
                if scope is not None:
                    return StateRead(self._payload(conn, scope, feature))
                if not snapshot_fallback:
                    return None
                point = self._fallback_point(conn, feature)
                if point is None:
                    return None
                try:
                    return StateRead(_snapshot_payload(point[1], feature), point[0])
                except Exception:
                    raise TrackerUnreadable(feature, [point[0]])
        except sqlite3.Error as e:
            self._log(feature, "warn", "store_read_failed", path=str(self.db_path), error=str(e))
            raise TrackerUnreadable(feature, [DB_NAME]) from e

    def read_current(self, feature: str) -> Any:
        with self._db() as conn:
            scope = self._read_scope(conn, feature, fallback=False) if conn is not None else None
            if conn is None or scope is None:
                raise FileNotFoundError(f"CrossWatch {feature} state is missing")
            return self._payload(conn, scope, feature)

    def has_state(self, feature: str) -> bool:
        with self._db() as conn:
            return conn is not None and self._own_state(conn, feature) is not None

    def write_state(self, feature: str, items: Mapping[str, Any], *, ts: int | None = None) -> bool:
        try:
            with self._db(create=True) as conn:
                assert conn is not None
                with self._write(conn):
                    self._guard_write(conn, after=False)
                    self._put_state(conn, self._scope, feature, dict(items or {}), ts=int(ts or time.time()))
                    self._guard_write(conn, after=True)
            return True
        except TrackerConflict:
            raise
        except Exception as e:
            self._log(feature, "warn", "store_write_failed", path=str(self.db_path), error=str(e))
            return False

    def _doc(self, conn: sqlite3.Connection, scope: str, name: str) -> Any | None:
        row = conn.execute("SELECT data FROM docs WHERE scope=? AND name=?", (scope, name)).fetchone()
        return None if row is None else json.loads(row[0])

    def read_meta(self, key: str) -> Any | None:
        try:
            with self._db() as conn:
                if conn is None:
                    return None
                found = self._doc(conn, self._scope, key)
                if found is None and self._scope:
                    found = self._doc(conn, "", key)
                    if found is not None:
                        with self._write(conn):
                            self._put_doc(conn, self._scope, key, found)
                return found
        except Exception:
            return None

    def write_meta(self, key: str, data: Any) -> bool:
        try:
            with self._db(create=True) as conn:
                assert conn is not None
                with self._write(conn):
                    self._put_doc(conn, self._scope, key, data)
            return True
        except Exception as e:
            self._log("common", "warn", "store_write_failed", path=str(self.db_path), doc=key, error=str(e))
            return False

    def read_playlists(self) -> Any | None:
        try:
            with self._db() as conn:
                return None if conn is None else self._doc(conn, "", _PLAYLISTS)
        except Exception as e:
            raise TrackerUnreadable(_PLAYLISTS, [DB_NAME]) from e

    def write_playlists(self, payload: Mapping[str, Any]) -> bool:
        try:
            with self._db(create=True) as conn:
                assert conn is not None
                with self._write(conn):
                    self._put_doc(conn, "", _PLAYLISTS, dict(payload))
            return True
        except Exception as e:
            self._log("common", "warn", "store_write_failed", path=str(self.db_path), doc=_PLAYLISTS, error=str(e))
            return False

    def _point_rows(self, conn: sqlite3.Connection, feature: str, scope: str | None = None) -> list[tuple[int, str, int, int, int]]:
        return [
            (int(r[0]), str(r[1]), int(r[2]), int(r[3]), int(r[4]))
            for r in conn.execute(
                "SELECT id, name, ts, created_ns, length(data) FROM restore_points WHERE scope=? AND feature=?"
                " ORDER BY created_ns, id",
                (self._scope if scope is None else scope, feature),
            ).fetchall()
        ]

    def restore_points(self, feature: str) -> list[RestorePoint]:
        with self._db() as conn:
            if conn is None:
                return []
            return [RestorePoint(name, feature, ts, size, created / 1e9) for _, name, ts, created, size in self._point_rows(conn, feature)]

    def read_restore_point(self, feature: str, name: str) -> Any:
        with self._db() as conn:
            row = None if conn is None else conn.execute(
                "SELECT data FROM restore_points WHERE scope=? AND feature=? AND name=?", (self._scope, feature, name)
            ).fetchone()
        if row is None:
            raise ValueError("Snapshot not found")
        try:
            return _snapshot_payload(row[0], feature)
        except Exception:
            raise TrackerUnreadable(feature, [name])

    def add_restore_point(
        self,
        feature: str,
        items: Mapping[str, Any],
        *,
        reuse_window: int = 0,
        retention_days: int = 30,
        max_snapshots: int = 14,
    ) -> None:
        now_ns = time.time_ns()
        payload = {"ts": now_ns // 1_000_000_000, "items": dict(items or {})}
        text = json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        try:
            with self._db(create=True) as conn:
                assert conn is not None
                with self._write(conn):
                    rows = self._point_rows(conn, feature)
                    unchanged = False
                    payload_updated_ns = 0
                    if rows:
                        latest = conn.execute("SELECT data FROM restore_points WHERE id=?", (rows[-1][0],)).fetchone()
                        try:
                            previous = json.loads(_snapshot_text(latest[0]))
                            payload_updated_ns = int(previous.get("ts") or 0) * 1_000_000_000
                            unchanged = previous.get("items") == payload["items"]
                        except (ValueError, TypeError, AttributeError, zlib.error):
                            pass
                    if unchanged:
                        conn.execute("UPDATE restore_points SET created_ns=? WHERE id=?", (now_ns, rows[-1][0]))
                    elif reuse_window > 0 and rows and (now_ns - payload_updated_ns) / 1e9 <= reuse_window:
                        conn.execute("UPDATE restore_points SET data=?, created_ns=? WHERE id=?", (_pack_snapshot(text), now_ns, rows[-1][0]))
                    else:
                        name = f"{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime(now_ns / 1e9))}-{feature}.json"
                        self._put_restore_point(conn, self._scope, feature, name, text, created_ns=now_ns)
                    removed = self._apply_retention(conn, feature, retention_days, max_snapshots, now_ns)
        except Exception as e:
            self._log(feature, "warn", "store_write_failed", path=str(self.db_path), error=str(e))
            return
        for name, fields in removed:
            self._log(feature, "info", "snapshot_removed", snapshot=name, **fields)

    def _apply_retention(
        self,
        conn: sqlite3.Connection,
        feature: str,
        retention_days: int,
        max_snapshots: int,
        now_ns: int,
    ) -> list[tuple[str, dict[str, Any]]]:
        removed: list[tuple[str, dict[str, Any]]] = []
        keep: list[tuple[int, str]] = []
        for point_id, name, _, created, _ in self._point_rows(conn, feature):
            if retention_days > 0 and (now_ns - created) / 86400e9 > retention_days:
                removed.append((name, {"reason": "retention", "retention_days": retention_days}))
                conn.execute("DELETE FROM restore_points WHERE id=?", (point_id,))
            else:
                keep.append((point_id, name))
        if max_snapshots > 0 and len(keep) > max_snapshots:
            for point_id, name in keep[: len(keep) - max_snapshots]:
                removed.append((name, {"reason": "max_snapshots", "max_snapshots": max_snapshots}))
                conn.execute("DELETE FROM restore_points WHERE id=?", (point_id,))
        return removed

    def load_items(self, feature: str, restore_point: str | None = None) -> dict[str, Any]:
        if feature not in FEATURES:
            raise ValueError(f"Unsupported kind: {feature!r}")
        name = _json_filename(restore_point, label="snapshot name") if restore_point else None
        data: Any = {}
        with self._db() as conn:
            if conn is not None and name:
                row = conn.execute(
                    "SELECT data FROM restore_points WHERE scope='' AND feature=? AND name=?", (feature, name)
                ).fetchone()
                if row is None:
                    raise ValueError("Snapshot not found")
                try:
                    data = _snapshot_payload(row[0], feature)
                except Exception:
                    raise TrackerUnreadable(feature, [name])
            elif conn is not None and self._state_row(conn, "", feature) is not None:
                data = self._payload(conn, "", feature)
            elif name:
                raise ValueError("Snapshot not found")
        if not isinstance(data, Mapping):
            data = {}
        items = data.get("items") or {}
        ts = data.get("ts")
        return {
            "items": items if isinstance(items, dict) else {},
            "ts": ts if isinstance(ts, int) else int(datetime.now(timezone.utc).timestamp()),
        }

    def stamp(self, features: Iterable[str]) -> list[tuple[str, int, int]]:
        wanted = sorted(features)
        with self._db() as conn:
            if conn is None:
                return [(feature, 0, 0) for feature in wanted]
            out: list[tuple[str, int, int]] = []
            for feature in wanted:
                row = self._state_row(conn, "", feature)
                count = conn.execute("SELECT COUNT(*) FROM items WHERE scope='' AND feature=?", (feature,)).fetchone()[0]
                out.append((feature, row[1] if row else 0, int(count)))
            return out

    def counts(self) -> dict[str, int]:
        out = {feature: 0 for feature in FEATURES}
        with self._db() as conn:
            if conn is not None:
                for feature, count in self._cached(conn, "counts", (), lambda: tuple(conn.execute(
                    "SELECT feature, COUNT(*) FROM items WHERE scope='' GROUP BY feature"
                ).fetchall())):
                    if feature in out:
                        out[str(feature)] = int(count)
        return out

    def query(
        self,
        feature: str,
        *,
        text: str = "",
        media: str = "",
        sort: str = "recent",
        offset: int = 0,
        limit: int = 60,
        attention: bool = False,
        health: str = "",
        until: str = "",
        status: str = "",
        tag: str = "",
        duplicates: bool = False,
    ) -> tuple[int, list[tuple[str, Any]]]:
        where, args = self._filters(feature, text=text, media=media, attention=attention, health=health, duplicates=duplicates)
        note_where, note_args = self._note_filters(status=status, tag=tag)
        where.extend(note_where)
        args.extend(note_args)
        if until:
            where.append("when_at < ?")
            args.append(until)
        order = {
            "recent": "when_at DESC, item_key DESC",
            "oldest": "when_at ASC, item_key ASC",
            "title": "sort_title, season, episode, item_key",
        }.get(sort, "when_at DESC, item_key DESC")
        clause = " AND ".join(where)
        with self._db() as conn:
            if conn is None:
                return 0, []
            try:
                total = int(self._cached(conn, "count", (clause, *args), lambda: conn.execute(
                    f"SELECT COUNT(*) FROM items WHERE {clause}", args
                ).fetchone()[0]))
                rows = conn.execute(
                    f"SELECT item_key, data FROM items WHERE {clause} ORDER BY {order} LIMIT ? OFFSET ?",
                    [*args, max(1, min(int(limit), 200)), max(0, int(offset))],
                ).fetchall()
            except sqlite3.Error as exc:
                raise TrackerUnreadable(feature, [DB_NAME]) from exc
        try:
            return total, [(str(key), json.loads(data)) for key, data in rows]
        except Exception:
            raise TrackerUnreadable(feature, [DB_NAME])

    def months(self, feature: str, *, text: str = "", media: str = "", status: str = "", tag: str = "", duplicates: bool = False) -> list[dict[str, Any]]:
        where, args = self._filters(feature, text=text, media=media, duplicates=duplicates)
        note_where, note_args = self._note_filters(status=status, tag=tag)
        where.extend(note_where)
        args.extend(note_args)
        clause = " AND ".join(where)
        with self._db() as conn:
            if conn is None:
                return []
            try:
                rows = self._cached(conn, "months", (clause, *args), lambda: tuple(conn.execute(
                    f"SELECT substr(when_at, 1, 7) AS m, COUNT(*) FROM items WHERE {clause}"
                    " AND m GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]' GROUP BY m ORDER BY m DESC",
                    args,
                ).fetchall()))
            except sqlite3.Error as exc:
                raise TrackerUnreadable(feature, [DB_NAME]) from exc
        return [{"month": str(m), "count": int(c)} for m, c in rows]

    @staticmethod
    def _note_filters(*, status: str = "", tag: str = "", key: str = _ITEM_NOTE_KEY) -> tuple[list[str], list[Any]]:
        where: list[str] = []
        args: list[Any] = []
        if status == "none":
            where.append(f"NOT EXISTS (SELECT 1 FROM notes n WHERE n.title_key=({key}) AND coalesce(n.status, '') != '')")
        elif status:
            where.append(f"EXISTS (SELECT 1 FROM notes n WHERE n.title_key=({key}) AND n.status=?)")
            args.append(status)
        if tag:
            where.append(f"EXISTS (SELECT 1 FROM note_tags nt WHERE nt.title_key=({key}) AND nt.tag=?)")
            args.append(tag)
        return where, args

    @staticmethod
    def _filters(feature: str, *, text: str = "", media: str = "", attention: bool = False, health: str = "", duplicates: bool = False) -> tuple[list[str], list[Any]]:
        if feature not in FEATURES:
            raise ValueError(f"Unsupported kind: {feature!r}")
        where = ["scope=''", "feature=?"]
        args: list[Any] = [feature]
        if duplicates:
            if feature != "history":
                where.append("0")
            else:
                identity = f"({_ITEM_NOTE_KEY}), coalesce(kind, ''), coalesce(season, -1), coalesce(episode, -1), when_at"
                where.append(f"({identity}) IN (SELECT {identity} FROM items WHERE scope='' AND feature='history' AND coalesce(when_at, '') != '' GROUP BY {identity} HAVING COUNT(*) > 1)")
        marks = ", ".join("?" for _ in ATTENTION_CODES)
        if attention or health == "attention":
            where.append(f"item_key IN (SELECT item_key FROM checks WHERE scope='' AND feature=? AND code IN ({marks}))")
            args.extend([feature, *ATTENTION_CODES])
        elif health == "ok":
            where.append(f"item_key IN (SELECT item_key FROM checks WHERE scope='' AND feature=? AND code NOT IN ({marks}))")
            args.extend([feature, *ATTENTION_CODES])
        elif health == "unchecked":
            where.append("item_key NOT IN (SELECT item_key FROM checks WHERE scope='' AND feature=?)")
            args.append(feature)
        if media == "movie":
            where.append("kind='movie'")
        elif media == "show":
            where.append(f"kind IN ({', '.join(repr(k) for k in _SHOW_KINDS)})")
        needle = text.strip().lower()
        if needle:
            where.append("instr(find, ?) > 0")
            args.append(needle)
        return where, args

    def record_count(self) -> int:
        with self._db() as conn:
            return int(conn.execute("SELECT COUNT(*) FROM items WHERE scope=''").fetchone()[0]) if conn is not None else 0

    def iter_records(self, *, page: int = 500) -> Iterator[tuple[str, str, Any, str]]:
        after = (0,)
        while True:
            with self._db() as conn:
                if conn is None:
                    return
                rows = conn.execute(
                    "SELECT rowid, feature, item_key, data FROM items WHERE scope='' AND rowid > ? ORDER BY rowid LIMIT ?",
                    (after[0], max(1, int(page))),
                ).fetchall()
            if not rows:
                return
            for rowid, feature, key, data in rows:
                try:
                    parsed = json.loads(data)
                except Exception:
                    raise TrackerUnreadable(str(feature), [DB_NAME])
                yield str(feature), str(key), parsed, str(data)
            after = (int(rows[-1][0]),)

    def save_checks(self, rows: Iterable[tuple[str, str, str, Mapping[str, Any], str]], *, checked_ns: int | None = None) -> None:
        now = int(checked_ns or time.time_ns())
        values = [(f, k, code, _dumps(dict(detail)), now, _runtime(detail), f, k, text) for f, k, code, detail, text in rows]
        if not values:
            return
        with self._db() as conn:
            if conn is None:
                return
            with self._write(conn):
                conn.executemany(
                    "INSERT INTO checks(scope, feature, item_key, code, detail, checked_ns, runtime)"
                    " SELECT '', ?, ?, ?, ?, ?, ? WHERE EXISTS(SELECT 1 FROM items WHERE scope='' AND feature=? AND item_key=? AND data=?)"
                    " ON CONFLICT(scope, feature, item_key) DO UPDATE SET code=excluded.code, detail=excluded.detail,"
                    " checked_ns=excluded.checked_ns, runtime=excluded.runtime",
                    values,
                )
                self._bump(conn)

    def prune_checks(self) -> None:
        with self._db() as conn:
            if conn is None:
                return
            with self._write(conn):
                if conn.execute(
                    "DELETE FROM checks WHERE NOT EXISTS(SELECT 1 FROM items i WHERE i.scope=checks.scope"
                    " AND i.feature=checks.feature AND i.item_key=checks.item_key)"
                ).rowcount:
                    self._bump(conn)

    def checks_for(self, feature: str, keys: Iterable[str]) -> dict[str, tuple[str, dict[str, Any], int]]:
        wanted = list(dict.fromkeys(str(k) for k in keys))
        out: dict[str, tuple[str, dict[str, Any], int]] = {}
        if not wanted:
            return out
        with self._db() as conn:
            if conn is None:
                return out
            for start in range(0, len(wanted), 500):
                chunk = wanted[start:start + 500]
                rows = conn.execute(
                    "SELECT item_key, code, detail, checked_ns FROM checks WHERE scope='' AND feature=? AND item_key IN ("
                    + ", ".join("?" for _ in chunk) + ")",
                    [feature, *chunk],
                ).fetchall()
                for key, code, detail, checked_ns in rows:
                    try:
                        parsed = json.loads(detail)
                    except Exception:
                        parsed = {}
                    out[str(key)] = (str(code), parsed if isinstance(parsed, dict) else {}, int(checked_ns))
        return out

    def health_counts(self) -> dict[str, dict[str, int]]:
        out = {feature: {"ok": 0, "attention": 0, "unchecked": 0} for feature in FEATURES}
        marks = ", ".join("?" for _ in ATTENTION_CODES)
        with self._db() as conn:
            if conn is None:
                return out
            rows = self._cached(conn, "health", (), lambda: tuple(conn.execute(
                "SELECT i.feature, CASE WHEN c.code IS NULL THEN 'unchecked'"
                f" WHEN c.code IN ({marks}) THEN 'attention' ELSE 'ok' END AS bucket, COUNT(*)"
                " FROM items i LEFT JOIN checks c ON c.scope=i.scope AND c.feature=i.feature AND c.item_key=i.item_key"
                " WHERE i.scope='' GROUP BY i.feature, bucket",
                ATTENTION_CODES,
            ).fetchall()))
        for feature, bucket, count in rows:
            if feature in out:
                out[str(feature)][str(bucket)] = int(count)
        return out

    def attention_counts(self) -> dict[str, int]:
        out = {feature: 0 for feature in FEATURES}
        with self._db() as conn:
            if conn is not None:
                for feature, count in self._cached(conn, "attention", (), lambda: tuple(conn.execute(
                    "SELECT c.feature, COUNT(*) FROM checks c JOIN items i ON i.scope=c.scope AND i.feature=c.feature"
                    " AND i.item_key=c.item_key WHERE c.scope='' AND c.code IN ("
                    + ", ".join("?" for _ in ATTENTION_CODES) + ") GROUP BY c.feature",
                    ATTENTION_CODES,
                ).fetchall())):
                    if feature in out:
                        out[str(feature)] = int(count)
        return out

    def job(self, name: str) -> dict[str, Any] | None:
        with self._db() as conn:
            row = conn.execute("SELECT value FROM meta WHERE key=?", (f"job:{name}",)).fetchone() if conn is not None else None
        try:
            data = json.loads(row[0]) if row else None
        except Exception:
            return None
        return data if isinstance(data, dict) else None

    def save_job(self, name: str, job: Mapping[str, Any]) -> None:
        with self._db(create=True) as conn:
            assert conn is not None
            conn.execute(
                "INSERT INTO meta(key, value) VALUES(?, ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (f"job:{name}", _dumps(dict(job))),
            )

    @staticmethod
    def _forget_checks(conn: sqlite3.Connection, feature: str, keys: Iterable[str | None]) -> None:
        for key in {k for k in keys if k}:
            conn.execute("DELETE FROM checks WHERE scope='' AND feature=? AND item_key=?", (feature, key))

    def item(self, feature: str, key: str) -> Any | None:
        with self._db() as conn:
            text = self._item_text(conn, feature, key) if conn is not None else None
        try:
            return _loads(text)
        except Exception:
            raise TrackerUnreadable(feature, [DB_NAME])

    @staticmethod
    def _item_text(conn: sqlite3.Connection, feature: str, key: str) -> str | None:
        row = conn.execute(
            "SELECT data FROM items WHERE scope='' AND feature=? AND item_key=?", (feature, key)
        ).fetchone()
        return None if row is None else str(row[0])

    @staticmethod
    def _touch(conn: sqlite3.Connection, features: Iterable[str], now_ns: int) -> None:
        for feature in set(features):
            conn.execute(
                "INSERT INTO states(scope, feature, ts, updated_ns) VALUES('', ?, ?, ?)"
                " ON CONFLICT(scope, feature) DO UPDATE SET ts=excluded.ts, updated_ns=excluded.updated_ns",
                (feature, now_ns // 1_000_000_000, now_ns),
            )
        TrackerStore._bump(conn)

    @staticmethod
    def _prune_changes(conn: sqlite3.Connection, now_ns: int) -> None:
        conn.execute("DELETE FROM changes WHERE created_ns < ?", (now_ns - CHANGE_RETENTION_DAYS * 86_400 * 1_000_000_000,))
        conn.execute(
            "DELETE FROM changes WHERE batch IN (SELECT batch FROM changes GROUP BY batch HAVING max(id) <= (SELECT max(id) FROM changes) - ?)",
            (CHANGE_LIMIT,),
        )

    def change_items(
        self,
        feature: str,
        changes: Iterable[tuple[str, str | None, Mapping[str, Any] | None]],
        *,
        action: str,
        expected: Mapping[str, Any] | None = None,
        follow: Mapping[str, Any] | None = None,
        note_status: Mapping[str, Any] | None = None,
    ) -> str | None:
        if feature not in FEATURES:
            raise ValueError(f"Unsupported kind: {feature!r}")
        return self.apply([(feature, key, new_key, data) for key, new_key, data in changes], action=action, expected=expected, follow=follow, note_status=note_status)

    def apply(
        self,
        changes: Iterable[tuple[str, str | None, str | None, Mapping[str, Any] | None]],
        *,
        action: str,
        expected: Mapping[Any, Any] | None = None,
        follow: Mapping[str, Any] | None = None,
        note_status: Mapping[str, Any] | None = None,
    ) -> str | None:
        rows = list(changes)
        if not rows:
            raise ValueError("no_changes")
        if any(feature not in FEATURES for feature, *_ in rows):
            raise ValueError("Unsupported kind")
        if note_status is not None and note_status.get("status") not in ("", *NOTE_STATUSES):
            raise ValueError("invalid_status")
        batch = uuid.uuid4().hex
        now = time.time_ns()
        changed_features: set[str] = set()
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                for feature, key, new_key, data in rows:
                    before = self._item_text(conn, feature, key) if key else None
                    if key and before is None:
                        raise KeyError(key)
                    want = _MISSING if expected is None else expected.get((feature, key), expected.get(key, _MISSING))
                    if key and want is not _MISSING and _loads(before) != want:
                        raise TrackerConflict("changed_since", key)
                    target = (new_key or key) if data is not None else None
                    if not key and not target:
                        raise ValueError("no_changes")
                    if target and target != key and self._item_text(conn, feature, target) is not None:
                        raise TrackerConflict("key_exists", target)
                    after = _dumps(dict(data)) if data is not None else None
                    if note_status is not None and target == key and _loads(before) == data:
                        continue
                    if key:
                        conn.execute("DELETE FROM items WHERE scope='' AND feature=? AND item_key=?", (feature, key))
                    if target and data is not None:
                        conn.execute(_UPSERT_ITEM, ("", feature, target, after, *_columns(target, data)))
                    conn.execute(
                        "INSERT INTO changes(batch, scope, feature, action, old_key, new_key, before, after, created_ns, find)"
                        " VALUES(?, '', ?, ?, ?, ?, ?, ?, ?, ?)",
                        (batch, feature, action, key or target, target, before, after, now, _change_find(before, after)),
                    )
                    self._forget_checks(conn, feature, [key, target])
                    changed_features.add(feature)
                if follow:
                    self._follow_note(conn, follow, batch, now)
                if note_status is not None:
                    note_key = str(note_status["key"])
                    before_note = self._note_get(conn, note_key)
                    after_note = _note_norm({**note_status, **(before_note or {}), "status": note_status["status"]})
                    if not any(after_note.get(field) for field in ("status", "started", "finished", "notes", "tags", "fields")):
                        after_note = None
                    if before_note != after_note:
                        self._note_put(conn, note_key, after_note, now)
                        self._note_change(conn, batch, note_key, note_key if after_note is not None else None, before_note, after_note, now)
                if not conn.execute("SELECT 1 FROM changes WHERE batch=? LIMIT 1", (batch,)).fetchone():
                    return None
                self._touch(conn, changed_features, now)
                self._prune_changes(conn, now)
        self._log(rows[0][0], "info", "tracker_items_changed", action=action, count=len(rows), batch=batch)
        return batch

    def history_event_mode(self) -> bool:
        with self._db() as conn:
            if conn is None:
                return False
            return bool(self._cached(conn, "event_mode", (), lambda: conn.execute(
                "SELECT 1 FROM items WHERE scope='' AND feature='history' AND item_key LIKE '%@%' LIMIT 1"
            ).fetchone() is not None))

    def related(
        self, *, show: bool, tmdb: str = "", imdb: str = "", title: str = "", base: str = "", limit: int | None = 5000, feature: str = "",
    ) -> list[tuple[str, str, Any]]:
        queries = self._related_queries(show=show, tmdb=tmdb, imdb=imdb, title=title, base=base)
        if feature:
            queries = [(f"{sql} AND feature=?", [*args, feature]) for sql, args in queries]
        with self._db() as conn:
            if conn is None:
                return []
            try:
                return self._related_rows(conn, queries, limit)
            except (sqlite3.Error, ValueError) as exc:
                raise TrackerUnreadable("history", [DB_NAME]) from exc

    @staticmethod
    def _related_queries(*, show: bool, tmdb: str = "", imdb: str = "", title: str = "", base: str = "") -> list[tuple[str, list[Any]]]:
        features = ", ".join("?" for _ in FEATURES)
        base_sql = f"SELECT feature, item_key, data FROM items WHERE scope='' AND feature IN ({features})"
        queries: list[tuple[str, list[Any]]] = []
        if show:
            queries.append((f"{base_sql} AND show_key=?", [*FEATURES, f"tmdb:{tmdb}" if tmdb else f"title:{title.lower()}"]))
            if tmdb:
                queries.append(("SELECT feature, item_key, data FROM items WHERE scope='' AND tmdb=? AND kind IN ('show', 'anime')", [tmdb]))
            else:
                queries.append((f"{base_sql} AND sort_title=? AND kind IN ('show', 'anime')", [*FEATURES, title.lower()]))
        else:
            movie = " AND kind='movie'"
            if tmdb:
                queries.append((f"SELECT feature, item_key, data FROM items WHERE scope='' AND tmdb=?{movie}", [tmdb]))
            if imdb:
                queries.append((f"SELECT feature, item_key, data FROM items WHERE scope='' AND imdb=?{movie}", [imdb]))
            if base:
                queries.append((f"{base_sql} AND item_key=?{movie}", [*FEATURES, base]))
                queries.append((f"{base_sql} AND item_key >= ? AND item_key < ?{movie}", [*FEATURES, f"{base}@", f"{base}A"]))
        return queries

    @staticmethod
    def _related_rows(conn: sqlite3.Connection, queries: list[tuple[str, list[Any]]], limit: int | None) -> list[tuple[str, str, Any]]:
        found: dict[tuple[str, str], Any] = {}
        cap = f" LIMIT {int(limit)}" if limit else ""
        for sql, args in queries:
            for feature, key, data in conn.execute(f"{sql}{cap}", args).fetchall():
                if feature in FEATURES:
                    found.setdefault((str(feature), str(key)), data)
        rows = sorted(found.items())
        if limit:
            rows = rows[:limit]
        out: list[tuple[str, str, Any]] = []
        for (feature, key), data in rows:
            value = json.loads(data)
            if not isinstance(value, Mapping):
                raise ValueError(f"{feature}:{key} is not a record")
            out.append((feature, key, value))
        return out

    def watched_episodes(self, show_keys: Iterable[str]) -> dict[str, set[tuple[int, int]]]:
        keys = list(dict.fromkeys(show_keys))
        found: dict[str, set[tuple[int, int]]] = {}
        with self._db() as conn:
            if conn is None:
                return found
            try:
                for offset in range(0, len(keys), 200):
                    batch = keys[offset:offset + 200]
                    marks = ",".join("?" for _ in batch)
                    rows = conn.execute(
                        "SELECT DISTINCT show_key, season, episode FROM items WHERE scope='' AND feature='history'"
                        f" AND kind='episode' AND show_key IN ({marks}) AND season>0 AND episode>0", batch,
                    )
                    for key, season, episode in rows:
                        found.setdefault(key, set()).add((season, episode))
            except sqlite3.Error as exc:
                raise TrackerUnreadable("history", [DB_NAME]) from exc
        return found

    def status_keys(self, status: str) -> set[str]:
        with self._db() as conn:
            if conn is None:
                return set()
            return {str(k) for (k,) in conn.execute("SELECT title_key FROM notes WHERE status=?", (status,)).fetchall()}

    def show_summaries(self, *, text: str = "", offset: int = 0, limit: int = 50, exclude: Iterable[str] = (), status: str = "", tag: str = "") -> tuple[int, list[dict[str, Any]]]:
        skip = {str(k) for k in exclude}
        needle = text.strip().lower()
        clause = "scope='' AND feature='history' AND show_key IS NOT NULL AND kind='episode'"
        search = []
        search_args: list[Any] = []
        episode_match = re.search(r"(?<!\w)s(\d{1,3})(?:\s*e(\d{1,4}))?(?!\w)", needle)
        title = needle
        if episode_match:
            search.append("season=?")
            search_args.append(int(episode_match[1]))
            if episode_match[2] is not None:
                search.append("episode=?")
                search_args.append(int(episode_match[2]))
            title = " ".join((needle[:episode_match.start()] + " " + needle[episode_match.end():]).split())
        if title:
            search.append("instr(sort_title, ?) > 0")
            search_args.append(title)
        with self._db() as conn:
            if conn is None:
                return 0, []
            try:
                def compute() -> tuple[tuple[str, int, int, str, Any, Any], ...]:
                    shows: dict[str, list[Any]] = {}
                    rows = conn.execute(
                        f"SELECT show_key, season, episode, when_at, sort_title FROM items WHERE {clause}"
                        + (f" AND show_key IN (SELECT show_key FROM items WHERE {clause} AND {' AND '.join(search)})" if search else ""),
                        search_args,
                    ).fetchall()
                    for show_key, season, episode, when, _ in rows:
                        row = shows.get(show_key)
                        if row is None:
                            shows[show_key] = [{(season, episode)}, 1, when, season, episode]
                            continue
                        row[0].add((season, episode))
                        row[1] += 1
                        if when > row[2]:
                            row[2:5] = [when, season, episode]
                    ordered = sorted(shows.items(), key=lambda kv: (kv[1][2], kv[0]), reverse=True)
                    return tuple((k, len(v[0]), v[1], v[2], v[3], v[4]) for k, v in ordered)

                summary = self._cached(conn, "shows", (needle,), compute)
                note_where, note_args = self._note_filters(status=status, tag=tag, key="'show:' || show_key")
                if note_where:
                    matched = {str(key) for (key,) in conn.execute(
                        f"SELECT DISTINCT show_key FROM items WHERE {clause} AND {' AND '.join(note_where)}", note_args
                    ).fetchall()}
                    summary = tuple(row for row in summary if row[0] in matched)
                if skip:
                    summary = tuple(row for row in summary if row[0] not in skip)
                page = summary[max(0, int(offset)):max(0, int(offset)) + max(1, min(int(limit), 200))]
                out = []
                for show_key, episodes, plays, last, season, episode in page:
                    head = conn.execute(
                        f"SELECT data FROM items WHERE {clause} AND show_key=? ORDER BY when_at DESC LIMIT 1", (show_key,)
                    ).fetchone()
                    data = _safe_loads(head[0]) if head else None
                    data = data if isinstance(data, Mapping) else {}
                    out.append({
                        "key": str(show_key), "title": data.get("series_title"), "year": data.get("series_year"),
                        "episodes": int(episodes), "plays": int(plays), "last_watched": last or None,
                        "last_season": season, "last_episode": episode,
                    })
                return len(summary), out
            except sqlite3.Error as exc:
                raise TrackerUnreadable("history", [DB_NAME]) from exc

    def history_stats(self, now: datetime | None = None) -> dict[str, Any]:
        moment = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)
        stamp = "%Y-%m-%dT%H:%M:%S"
        week = (moment - timedelta(days=7)).strftime(stamp)
        month = (moment - timedelta(days=30)).strftime(stamp)
        year = f"{moment.year:04d}-01-01"
        next_year = f"{moment.year + 1:04d}-01-01"
        dated = "i.scope='' AND i.feature='history' AND i.when_at GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]*'"
        with self._db() as conn:
            if conn is None:
                return {"total": 0, "movies": 0, "episodes": 0, "week": 0, "month": 0, "year": 0, "minutes": 0, "timed": 0, "days": []}
            try:
                def compute() -> dict[str, Any]:
                    total, movies, episodes, in_week, in_month, in_year, minutes, timed = conn.execute(
                        "SELECT COUNT(*), COUNT(*) FILTER (WHERE i.kind='movie'), COUNT(*) FILTER (WHERE i.kind='episode'),"
                        " COUNT(*) FILTER (WHERE i.when_at >= ?), COUNT(*) FILTER (WHERE i.when_at >= ?),"
                        " COUNT(*) FILTER (WHERE i.when_at >= ? AND i.when_at < ?),"
                        " coalesce(SUM(c.runtime), 0), COUNT(c.runtime)"
                        " FROM items i LEFT JOIN checks c ON c.scope=i.scope AND c.feature=i.feature AND c.item_key=i.item_key"
                        f" WHERE {dated}",
                        (week, month, year, next_year),
                    ).fetchone()
                    days = [str(d) for (d,) in conn.execute(
                        f"SELECT DISTINCT substr(i.when_at, 1, 10) AS d FROM items i WHERE {dated} ORDER BY d DESC"
                    ).fetchall()]
                    return {
                        "total": int(total), "movies": int(movies), "episodes": int(episodes), "week": int(in_week),
                        "month": int(in_month), "year": int(in_year), "minutes": int(minutes), "timed": int(timed), "days": days,
                    }

                return dict(self._cached(conn, "stats", (week[:16], month[:16], year), compute))
            except sqlite3.Error as exc:
                raise TrackerUnreadable("history", [DB_NAME]) from exc

    @staticmethod
    def _note_get(conn: sqlite3.Connection, key: str) -> dict[str, Any] | None:
        row = conn.execute(
            "SELECT type, title, year, tmdb, status, started, finished, body, fields FROM notes WHERE title_key=?", (key,)
        ).fetchone()
        if row is None:
            return None
        tags = [str(t) for (t,) in conn.execute("SELECT tag FROM note_tags WHERE title_key=?", (key,)).fetchall()]
        typ, title, year, tmdb, status, started, finished, body, fields = row
        return _note_norm({
            "type": typ, "title": title, "year": year, "tmdb": tmdb, "status": status, "started": started,
            "finished": finished, "notes": body, "tags": tags, "fields": _safe_loads(fields),
        })

    @staticmethod
    def _note_put(conn: sqlite3.Connection, key: str, note: Mapping[str, Any] | None, now: int) -> None:
        conn.execute("DELETE FROM note_tags WHERE title_key=?", (key,))
        if note is None:
            conn.execute("DELETE FROM notes WHERE title_key=?", (key,))
            return
        conn.execute(
            "INSERT INTO notes(title_key, type, title, year, tmdb, status, started, finished, body, fields, find, updated_ns)"
            " VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) ON CONFLICT(title_key) DO UPDATE SET type=excluded.type,"
            " title=excluded.title, year=excluded.year, tmdb=excluded.tmdb, status=excluded.status, started=excluded.started,"
            " finished=excluded.finished, body=excluded.body, fields=excluded.fields, find=excluded.find,"
            " updated_ns=excluded.updated_ns",
            (
                key, note["type"], note["title"], note["year"], note["tmdb"], note["status"], note["started"],
                note["finished"], note["notes"], _dumps(note["fields"]), str(note["title"]).lower(), now,
            ),
        )
        conn.executemany("INSERT OR IGNORE INTO note_tags(title_key, tag) VALUES(?, ?)", [(key, t) for t in note["tags"]])

    @staticmethod
    def _note_change(conn: sqlite3.Connection, batch: str, old_key: str, new_key: str | None, before: Any, after: Any, now: int) -> None:
        conn.execute(
            "INSERT INTO changes(batch, scope, feature, action, old_key, new_key, before, after, created_ns, find)"
            " VALUES(?, '', ?, 'note', ?, ?, ?, ?, ?, ?)",
            (
                batch, NOTES, old_key, new_key, None if before is None else _dumps(before),
                None if after is None else _dumps(after), now, _change_find(before, after),
            ),
        )

    def note(self, key: str) -> dict[str, Any] | None:
        with self._db() as conn:
            return None if conn is None else self._note_get(conn, key)

    def notes_for(self, keys: Iterable[str]) -> dict[str, dict[str, Any]]:
        wanted = list(dict.fromkeys(str(k) for k in keys))
        out: dict[str, dict[str, Any]] = {}
        with self._db() as conn:
            if conn is None or not wanted:
                return out
            for start in range(0, len(wanted), 500):
                chunk = wanted[start:start + 500]
                found = conn.execute(
                    f"SELECT title_key FROM notes WHERE title_key IN ({', '.join('?' for _ in chunk)})", chunk
                ).fetchall()
                for (key,) in found:
                    note = self._note_get(conn, str(key))
                    if note is not None:
                        out[str(key)] = note
        return out

    def save_note(self, key: str, note: Mapping[str, Any] | None, *, expected: Any = ...) -> str | None:
        after = _note_norm(note) if note is not None else None
        now = time.time_ns()
        batch = uuid.uuid4().hex
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                before = self._note_get(conn, key)
                if expected is not ... and before != (_note_norm(expected) if expected is not None else None):
                    raise TrackerConflict("changed_since", key)
                if before == after:
                    return None
                self._note_put(conn, key, after, now)
                self._note_change(conn, batch, key, key if after is not None else None, before, after, now)
                self._bump(conn)
                self._prune_changes(conn, now)
        return batch

    def change_notes(self, keys: Iterable[str], *, status: str = "", remove: bool = False, defaults: Mapping[str, Mapping[str, Any]] | None = None) -> tuple[str | None, int]:
        if not remove and status not in ("", *NOTE_STATUSES):
            raise ValueError("invalid_status")
        now = time.time_ns()
        batch = uuid.uuid4().hex
        count = 0
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                for key in dict.fromkeys(keys):
                    before = self._note_get(conn, key)
                    if before is None and (defaults is None or key not in defaults or remove):
                        raise KeyError(key)
                    current = before if before is not None else (defaults or {}).get(key, {})
                    after = None if remove else _note_norm({**current, "status": status})
                    if after is not None and not any(after.get(field) for field in ("status", "started", "finished", "notes", "tags", "fields")):
                        after = None
                    if before == after:
                        continue
                    self._note_put(conn, key, after, now)
                    self._note_change(conn, batch, key, key if after is not None else None, before, after, now)
                    count += 1
                if count:
                    self._bump(conn)
                    self._prune_changes(conn, now)
        return (batch if count else None), count

    def _follow_note(self, conn: sqlite3.Connection, follow: Mapping[str, Any], batch: str, now: int) -> None:
        old, new = str(follow.get("old") or ""), str(follow.get("new") or "")
        if not old or not new or old == new:
            return
        before = self._note_get(conn, old)
        if before is None or self._note_get(conn, new) is not None:
            return
        if self._related_rows(conn, self._related_queries(**dict(follow.get("ident") or {})), 1):
            return
        meta = {k: v for k, v in dict(follow.get("meta") or {}).items() if k in ("type", "title", "year", "tmdb")}
        after = _note_norm({**before, **meta})
        self._note_put(conn, old, None, now)
        self._note_put(conn, new, after, now)
        self._note_change(conn, batch, old, new, before, after, now)

    def notes(self, *, status: str = "", tag: str = "", text: str = "", offset: int = 0, limit: int = 50) -> tuple[int, list[tuple[str, dict[str, Any], int]]]:
        where: list[str] = []
        args: list[Any] = []
        if status == "none":
            where.append("coalesce(status, '') = ''")
        elif status:
            where.append("status=?")
            args.append(status)
        if tag:
            where.append("title_key IN (SELECT title_key FROM note_tags WHERE tag=?)")
            args.append(tag)
        needle = text.strip().lower()
        if needle:
            where.append("instr(find, ?) > 0")
            args.append(needle)
        clause = f" WHERE {' AND '.join(where)}" if where else ""
        with self._db() as conn:
            if conn is None:
                return 0, []
            total = int(conn.execute(f"SELECT COUNT(*) FROM notes{clause}", args).fetchone()[0])
            keys = conn.execute(
                f"SELECT title_key, updated_ns FROM notes{clause} ORDER BY updated_ns DESC, title_key LIMIT ? OFFSET ?",
                [*args, max(1, min(int(limit), 200)), max(0, int(offset))],
            ).fetchall()
            rows = []
            for key, updated in keys:
                note = self._note_get(conn, str(key))
                if note is not None:
                    rows.append((str(key), note, int(updated)))
        return total, rows

    def note_summary(self) -> dict[str, Any]:
        with self._db() as conn:
            if conn is None:
                return {"total": 0, "statuses": {}, "tags": []}
            statuses = {str(s or ""): int(n) for s, n in conn.execute("SELECT status, COUNT(*) FROM notes GROUP BY status").fetchall()}
            tags = [
                {"tag": str(t), "count": int(n)}
                for t, n in conn.execute("SELECT tag, COUNT(*) FROM note_tags GROUP BY tag ORDER BY COUNT(*) DESC, tag LIMIT 500").fetchall()
            ]
        return {"total": sum(statuses.values()), "statuses": {k: v for k, v in statuses.items() if k}, "tags": tags}

    @staticmethod
    def _show_rows(conn: sqlite3.Connection, show_key: str) -> list[Any]:
        features = ", ".join("?" for _ in FEATURES)
        rows = conn.execute(
            f"SELECT data FROM items WHERE scope='' AND feature IN ({features}) AND show_key=? LIMIT 1", (*FEATURES, show_key)
        ).fetchall()
        kind, _, value = show_key.partition(":")
        if kind == "tmdb" and value:
            rows += conn.execute(
                "SELECT data FROM items WHERE scope='' AND tmdb=? AND kind IN ('show', 'anime') LIMIT 1", (value,)
            ).fetchall()
        return [data for data in (_safe_loads(r[0]) for r in rows) if isinstance(data, Mapping)]

    def note_show_tokens(self, status: str) -> set[str]:
        out: set[str] = set()
        with self._db() as conn:
            if conn is None:
                return out
            keys = [str(k) for (k,) in conn.execute("SELECT title_key FROM notes WHERE status=?", (status,)).fetchall()]
            for key in keys:
                kind, _, show_key = key.partition(":")
                if kind != "show":
                    continue
                if show_key.startswith("tmdb:"):
                    out.add(show_key.lower())
                for data in self._show_rows(conn, show_key):
                    ids = data.get("show_ids") if data.get("type") in ("episode", "season") else data.get("ids")
                    for name, value in _map_of(ids).items():
                        text = _text(value)
                        if text and text.strip():
                            out.add(f"{str(name).lower()}:{text.strip().lower()}")
        return out

    def set_show_status(self, rows: Iterable[tuple[str, Mapping[str, Any]]], status: str) -> tuple[str | None, int, int]:
        now = time.time_ns()
        batch = uuid.uuid4().hex
        changed = same = 0
        with self.mutation(), self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                for key, meta in rows:
                    before = self._note_get(conn, key)
                    base = dict(before) if before is not None else {"type": "show", **{k: v for k, v in meta.items() if k in ("title", "year", "tmdb")}}
                    if not base.get("title"):
                        head = next(iter(self._show_rows(conn, key.partition(":")[2])), {})
                        child = head.get("type") in ("episode", "season")
                        base["title"] = head.get("series_title" if child else "title") or ""
                        base["year"] = base.get("year") or head.get("series_year" if child else "year")
                    after = _note_norm({**base, "status": status})
                    if after == before:
                        same += 1
                        continue
                    self._note_put(conn, key, after, now)
                    self._note_change(conn, batch, key, key, before, after, now)
                    changed += 1
                if changed:
                    self._bump(conn)
                    self._prune_changes(conn, now)
        return (batch if changed else None), changed, same

    def note_fields(self) -> list[dict[str, Any]]:
        with self._db() as conn:
            row = None if conn is None else conn.execute("SELECT value FROM meta WHERE key='note_fields'").fetchone()
        data = _safe_loads(row[0]) if row else None
        return [dict(f) for f in data if isinstance(f, Mapping)] if isinstance(data, list) else []

    def save_note_fields(self, fields: Iterable[Mapping[str, Any]]) -> None:
        with self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('note_fields', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                    (_dumps([dict(f) for f in fields]),),
                )

    def batch_features(self, batch: str) -> set[str]:
        with self._db() as conn:
            if conn is None:
                return set()
            return {str(f) for (f,) in conn.execute("SELECT DISTINCT feature FROM changes WHERE batch=?", (batch,)).fetchall()}

    def undo(self, batch: str) -> str:
        now = time.time_ns()
        undo_batch = uuid.uuid4().hex
        with self.mutation(), self._db() as conn:
            if conn is None:
                raise KeyError(batch)
            with self._write(conn):
                rows = conn.execute(
                    "SELECT feature, action, old_key, new_key, before, after, undone_ns FROM changes"
                    " WHERE batch=? AND scope='' ORDER BY id DESC",
                    (batch,),
                ).fetchall()
                if not rows:
                    raise KeyError(batch)
                if any(row[1] == "undo" for row in rows):
                    raise TrackerConflict("not_undoable")
                if any(row[6] is not None for row in rows):
                    raise TrackerConflict("already_undone")
                for feature, old_key, new_key, before, after in (row[:1] + row[2:6] for row in rows):
                    if feature == NOTES:
                        self._undo_note(conn, old_key, new_key, _safe_loads(before), _safe_loads(after), now)
                        conn.execute(
                            "INSERT INTO changes(batch, scope, feature, action, old_key, new_key, before, after, created_ns, undo_of, find)"
                            " VALUES(?, '', ?, 'undo', ?, ?, ?, ?, ?, ?, ?)",
                            (
                                undo_batch, feature, new_key or old_key, old_key if before is not None else None, after, before,
                                now, batch, _change_find(before, after),
                            ),
                        )
                        continue
                    if new_key:
                        current = self._item_text(conn, feature, new_key)
                        if current is None or _edit_view(_loads(current)) != _edit_view(_loads(after)):
                            raise TrackerConflict("changed_since", new_key)
                        conn.execute("DELETE FROM items WHERE scope='' AND feature=? AND item_key=?", (feature, new_key))
                    if before is not None:
                        if self._item_text(conn, feature, old_key) is not None:
                            raise TrackerConflict("changed_since", old_key)
                        conn.execute(_UPSERT_ITEM, ("", feature, old_key, before, *_columns(old_key, _safe_loads(before))))
                    conn.execute(
                        "INSERT INTO changes(batch, scope, feature, action, old_key, new_key, before, after, created_ns, undo_of, find)"
                        " VALUES(?, '', ?, 'undo', ?, ?, ?, ?, ?, ?, ?)",
                        (undo_batch, feature, new_key or old_key, old_key, after, before, now, batch, _change_find(before, after)),
                    )
                    self._forget_checks(conn, feature, [old_key, new_key])
                conn.execute("UPDATE changes SET undone_ns=? WHERE batch=?", (now, batch))
                self._touch(conn, [str(row[0]) for row in rows if row[0] in FEATURES], now)
                self._prune_changes(conn, now)
        self._log(str(rows[0][0]), "info", "tracker_change_undone", batch=batch, count=len(rows))
        return undo_batch

    def _undo_note(self, conn: sqlite3.Connection, old_key: str, new_key: str | None, before: Any, after: Any, now: int) -> None:
        if new_key:
            if self._note_get(conn, new_key) != (_note_norm(after) if isinstance(after, Mapping) else None):
                raise TrackerConflict("changed_since", new_key)
            self._note_put(conn, new_key, None, now)
        if isinstance(before, Mapping):
            if self._note_get(conn, old_key) is not None:
                raise TrackerConflict("changed_since", old_key)
            self._note_put(conn, old_key, _note_norm(before), now)

    def change_count(self) -> int:
        with self._db() as conn:
            if conn is None:
                return 0
            return int(conn.execute("SELECT COUNT(DISTINCT batch) FROM changes WHERE scope=''").fetchone()[0])

    def change_log(
        self, *, offset: int = 0, limit: int = 50, preview: int = 4, kind: str = "", text: str = "",
    ) -> tuple[int, list[ChangeBatch]]:
        where = ["scope=''"]
        args: list[Any] = []
        if kind in CHANGE_KINDS:
            where.append(CHANGE_KINDS[kind])
        needle = text.strip().lower()
        if needle:
            where.append("instr(find, ?) > 0")
            args.append(needle)
        clause = " AND ".join(where)
        with self._db() as conn:
            if conn is None:
                return 0, []
            order: tuple[str, ...] = self._cached(conn, "changes", (clause, *args), lambda: tuple(
                str(row[0]) for row in conn.execute(
                    f"SELECT batch, max(id) AS last FROM changes WHERE {clause} GROUP BY batch ORDER BY last DESC", args
                ).fetchall()
            ))
            total = len(order)
            page = list(order[max(0, int(offset)):max(0, int(offset)) + max(1, min(int(limit), 200))])
            found = {
                str(row[0]): row[1:] for row in conn.execute(
                    "SELECT batch, min(created_ns), max(undone_ns), max(undo_of), COUNT(*), COUNT(*) FILTER (WHERE feature<>?)"
                    f" FROM changes WHERE scope='' AND batch IN ({', '.join('?' for _ in page)}) GROUP BY batch",
                    (NOTES, *page),
                ).fetchall()
            } if page else {}
            heads = [(batch, *found[batch]) for batch in page if batch in found]
            out: list[ChangeBatch] = []
            for batch, created_ns, undone_ns, undo_of, count, records in heads:
                rows = conn.execute(
                    "SELECT feature, action, old_key, new_key, before, after FROM changes WHERE batch=?"
                    " ORDER BY feature=?, id LIMIT ?",
                    (batch, NOTES, max(1, int(preview))),
                ).fetchall()
                feature, action = str(rows[0][0]), str(rows[0][1])
                count = records or count
                rows = [r for r in rows if (r[0] == NOTES) == (feature == NOTES)]
                try:
                    parsed = [(str(o), None if n is None else str(n), _loads(b), _loads(a)) for _, _, o, n, b, a in rows]
                except Exception:
                    raise TrackerUnreadable(str(feature), [DB_NAME])
                out.append(ChangeBatch(
                    str(batch), str(feature), str(action), int(created_ns),
                    None if undone_ns is None else int(undone_ns), None if undo_of is None else str(undo_of),
                    int(count), parsed,
                ))
        return total, out

    def inventory(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "exists": self.root.exists(),
            "state_files": [],
            "snapshots": [],
            "counts": {"state_files": 0, "snapshots": 0},
        }
        with self._db() as conn:
            if conn is None:
                return out
            state_files = [
                {"name": _file_name(str(scope), str(feature)), "size": int(size or 0), "mtime": _iso(int(updated))}
                for scope, feature, updated, size in conn.execute(
                    "SELECT s.scope, s.feature, s.updated_ns, (SELECT SUM(length(i.data)) FROM items i"
                    " WHERE i.scope=s.scope AND i.feature=s.feature) FROM states s"
                ).fetchall()
            ] + [
                {"name": _file_name(str(scope), str(name)), "size": int(size or 0), "mtime": _iso(int(updated))}
                for scope, name, updated, size in conn.execute("SELECT scope, name, updated_ns, length(data) FROM docs").fetchall()
            ]
            snapshots = [
                {"name": str(name), "size": int(size or 0), "mtime": _iso(int(created))}
                for name, created, size in conn.execute("SELECT name, created_ns, length(data) FROM restore_points").fetchall()
            ]
            core = conn.execute("SELECT COUNT(*) FROM states WHERE scope=''").fetchone()[0]
        out["state_files"] = sorted(state_files, key=lambda x: x["name"])
        out["snapshots"] = sorted(snapshots, key=lambda x: x["mtime"] or "")
        out["counts"] = {"state_files": int(core), "snapshots": len(snapshots)}
        return out

    def files(self, *, state: bool, snapshots: bool) -> list[Path]:
        if not (state or snapshots):
            return []
        return [p for p in (self.db_path, self.root / f"{DB_NAME}-wal") if p.is_file()]

    def clear(self, *, state: bool, snapshots: bool) -> dict[str, list[str]]:
        result: dict[str, list[str]] = {"state_files": [], "emptied_state_files": [], "snapshots": []}
        with self._db(create=state) as conn:
            if conn is None:
                return result
            with self._write(conn):
                if state:
                    keys = {(str(s), str(f)) for s, f in conn.execute("SELECT scope, feature FROM states").fetchall()}
                    keys |= {("", feature) for feature in FEATURES}
                    for scope, feature in sorted(keys):
                        self._put_state(conn, scope, feature, {}, ts=int(time.time()))
                        result["emptied_state_files"].append(_file_name(scope, feature))
                    for scope, name in conn.execute("SELECT scope, name FROM docs").fetchall():
                        result["state_files"].append(_file_name(str(scope), str(name)))
                    conn.execute("DELETE FROM docs")
                if snapshots:
                    result["snapshots"] = [str(r[0]) for r in conn.execute("SELECT name FROM restore_points").fetchall()]
                    conn.execute("DELETE FROM restore_points")
                if state:
                    conn.execute("DELETE FROM changes")
                    conn.execute("DELETE FROM checks")
                    self._bump(conn)
            conn.execute("VACUUM")
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        return result

    def export_zip(self) -> bytes:
        buf = BytesIO()
        with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf, self._db() as conn:
            if conn is not None:
                for scope, feature in conn.execute("SELECT scope, feature FROM states ORDER BY scope, feature").fetchall():
                    zf.writestr(_file_name(str(scope), str(feature)), _dumps(self._payload(conn, str(scope), str(feature))))
                for scope, name, data in conn.execute("SELECT scope, name, data FROM docs ORDER BY scope, name").fetchall():
                    zf.writestr(_file_name(str(scope), str(name)), str(data))
                for scope, name, data in conn.execute("SELECT scope, name, data FROM restore_points ORDER BY created_ns, id"):
                    zf.writestr("/".join(["snapshots", *([str(scope)] if scope else []), str(name)]), _snapshot_text(data))
                keys = [str(k) for (k,) in conn.execute("SELECT title_key FROM notes ORDER BY title_key").fetchall()]
                fields = conn.execute("SELECT value FROM meta WHERE key='note_fields'").fetchone()
                if keys or fields:
                    zf.writestr(_NOTES_FILE, _dumps({
                        "fields": _safe_loads(fields[0]) if fields else [],
                        "notes": {key: self._note_get(conn, key) for key in keys},
                    }))
        return buf.getvalue()

    def _import_notes(self, conn: sqlite3.Connection, raw: Mapping[str, Any]) -> None:
        now = time.time_ns()
        fields = raw.get("fields")
        if isinstance(fields, list):
            conn.execute(
                "INSERT INTO meta(key, value) VALUES('note_fields', ?) ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                (_dumps([dict(f) for f in fields if isinstance(f, Mapping)]),),
            )
        for key, note in _map_of(raw.get("notes")).items():
            if isinstance(note, Mapping):
                self._note_put(conn, str(key), _note_norm(note), now)
        self._bump(conn)

    def import_zip(self, fp: Any) -> dict[str, Any]:
        stats: dict[str, Any] = {"files": 0, "overwritten": 0, "states": 0, "snapshots": 0}
        with zipfile.ZipFile(fp) as zf, self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                for info in zf.infolist():
                    existed: Any = None
                    rel = PurePosixPath(info.filename)
                    if info.is_dir() or rel.is_absolute() or ".." in rel.parts or rel.suffix.lower() != ".json":
                        continue
                    try:
                        text = zf.read(info).decode("utf-8")
                        raw = json.loads(text)
                    except Exception as e:
                        raise ValueError(f"{info.filename} is not valid JSON") from e
                    parts = rel.parts
                    if parts == (_NOTES_FILE,):
                        if not isinstance(raw, Mapping):
                            raise ValueError(f"{info.filename} is not a notes export")
                        existed = conn.execute("SELECT 1 FROM notes LIMIT 1").fetchone()
                        self._import_notes(conn, raw)
                        stats["notes"] = len(_map_of(raw.get("notes")))
                    elif parts[0] == "snapshots" and len(parts) in (2, 3):
                        feature = _snapshot_feature(parts[-1])
                        if feature is None:
                            continue
                        scope = parts[1] if len(parts) == 3 else ""
                        _snapshot_payload(text, feature)
                        existed = conn.execute(
                            "SELECT 1 FROM restore_points WHERE scope=? AND feature=? AND name=?", (scope, feature, parts[-1])
                        ).fetchone()
                        self._put_restore_point(conn, scope, feature, parts[-1], text, created_ns=time.time_ns())
                        stats["snapshots"] += 1
                    elif len(parts) == 1 and (target := _file_target(parts[0])):
                        kind, scope, key = target
                        if kind == "state":
                            ts, items = _state_payload(raw, key)
                            existed = self._state_row(conn, scope, key)
                            self._put_state(conn, scope, key, items, ts=ts)
                            if not scope and key != "collection":
                                stats["states"] += 1
                        else:
                            existed = conn.execute("SELECT 1 FROM docs WHERE scope=? AND name=?", (scope, key)).fetchone()
                            self._put_doc(conn, scope, key, raw)
                    else:
                        continue
                    stats["files"] += 1
                    if existed:
                        stats["overwritten"] += 1
        return stats

    def import_json(
        self,
        payload: bytes,
        filename: str,
        *,
        retention_days: int = 30,
        max_snapshots: int = 14,
    ) -> dict[str, Any]:
        try:
            raw = json.loads(payload.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"File is not valid JSON: {e}") from e
        if not isinstance(raw, dict):
            raise ValueError("JSON root must be an object")
        ts_val = raw.get("ts")
        ts = int(ts_val) if isinstance(ts_val, int) else int(datetime.now(timezone.utc).timestamp())
        name = (filename or "upload.json").strip()
        lower = name.lower()
        if lower in {f"{feature}.json" for feature in FEATURES}:
            kind = lower.split(".")[0]
            target = "state"
            dest = f"{kind}.json"
        else:
            found = _snapshot_feature(lower)
            if found is None:
                raise ValueError(
                    "Could not infer target for JSON file. "
                    "Use filenames like 'watchlist.json' or "
                    "'YYYYMMDDTHHMMSSZ-watchlist.json'.",
                )
            kind = found
            target = "snapshot"
            dest = _json_filename(name, label="snapshot name")
        try:
            items = _upload_items(raw, kind)
        except ValueError as e:
            raise ValueError(f"{name} is not a tracker export: {e}") from e
        with self._db(create=True) as conn:
            assert conn is not None
            with self._write(conn):
                if target == "state":
                    existed = self._state_row(conn, "", kind) is not None
                    self._put_state(conn, "", kind, items, ts=ts)
                else:
                    existed = conn.execute(
                        "SELECT 1 FROM restore_points WHERE scope='' AND feature=? AND name=?", (kind, dest)
                    ).fetchone() is not None
                    self._put_restore_point(conn, "", kind, dest, _dumps({"items": items, "ts": ts}), created_ns=time.time_ns())
                    self._apply_editor_retention(conn, kind, retention_days, max_snapshots)
        return {
            "files": 1,
            "overwritten": 1 if existed else 0,
            "target": target,
            "kind": kind,
            "name": dest,
            "states": 1 if target == "state" else 0,
            "snapshots": 1 if target == "snapshot" else 0,
            "mode": "json",
        }

    def import_upload(
        self,
        payload: bytes,
        filename: str | None = None,
        *,
        retention_days: int = 30,
        max_snapshots: int = 14,
    ) -> dict[str, Any]:
        name = (filename or "").strip() or "upload.bin"
        buf = BytesIO(payload)
        try:
            is_zip = zipfile.is_zipfile(buf)
        except Exception:
            is_zip = False
        if is_zip:
            buf.seek(0)
            stats = self.import_zip(buf)
            stats.setdefault("mode", "zip")
            return stats
        try:
            return self.import_json(payload, name, retention_days=retention_days, max_snapshots=max_snapshots)
        except Exception as e:
            if name.lower().endswith(".json"):
                raise
            raise ValueError("Unsupported file type; expected a ZIP or JSON file") from e

    def _apply_editor_retention(self, conn: sqlite3.Connection, feature: str, retention_days: int, max_snapshots: int) -> None:
        rows = sorted(self._point_rows(conn, feature, ""), key=lambda r: r[2], reverse=True)
        now = datetime.now(timezone.utc)
        keep = [
            r[0] for r in rows
            if not (retention_days and (now - datetime.fromtimestamp(r[2], tz=timezone.utc)).days > retention_days)
        ]
        if max_snapshots and len(keep) > max_snapshots:
            keep = keep[:max_snapshots]
        for point_id, *_ in rows:
            if point_id not in keep:
                conn.execute("DELETE FROM restore_points WHERE id=?", (point_id,))

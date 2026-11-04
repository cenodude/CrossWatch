# cw_platform/tracker_store.py
# CrossWatch - Local tracker storage
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import shutil
import sqlite3
import threading
import time
import uuid
import zipfile
from collections.abc import Callable, Generator, Iterable, Iterator, Mapping
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path, PurePosixPath, PureWindowsPath
from typing import Any

from .history_events import history_event_key
from .id_map import canonical_key
from .provider_instances import _safe_profile_dir_name, normalize_instance_id

FEATURES: tuple[str, ...] = ("watchlist", "history", "ratings", "progress", "collection")
DB_NAME = "tracker.db"
SCHEMA_VERSION = 3
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

_SCHEMA = (
    "CREATE TABLE IF NOT EXISTS meta(key TEXT PRIMARY KEY, value TEXT NOT NULL)",
    "CREATE TABLE IF NOT EXISTS states(scope TEXT NOT NULL, feature TEXT NOT NULL, ts INTEGER NOT NULL,"
    " updated_ns INTEGER NOT NULL, PRIMARY KEY(scope, feature))",
    "CREATE TABLE IF NOT EXISTS items(scope TEXT NOT NULL, feature TEXT NOT NULL, item_key TEXT NOT NULL,"
    " data TEXT NOT NULL, PRIMARY KEY(scope, feature, item_key))",
    "CREATE TABLE IF NOT EXISTS docs(scope TEXT NOT NULL, name TEXT NOT NULL, data TEXT NOT NULL,"
    " updated_ns INTEGER NOT NULL, PRIMARY KEY(scope, name))",
    "CREATE TABLE IF NOT EXISTS restore_points(id INTEGER PRIMARY KEY AUTOINCREMENT, scope TEXT NOT NULL,"
    " feature TEXT NOT NULL, name TEXT NOT NULL, ts INTEGER NOT NULL, created_ns INTEGER NOT NULL,"
    " data TEXT NOT NULL, UNIQUE(scope, feature, name))",
    "CREATE TABLE IF NOT EXISTS changes(id INTEGER PRIMARY KEY AUTOINCREMENT, batch TEXT NOT NULL, scope TEXT NOT NULL,"
    " feature TEXT NOT NULL, action TEXT NOT NULL, old_key TEXT NOT NULL, new_key TEXT, before TEXT, after TEXT,"
    " created_ns INTEGER NOT NULL, undone_ns INTEGER, undo_of TEXT)",
    "CREATE INDEX IF NOT EXISTS changes_batch ON changes(batch)",
    "CREATE TABLE IF NOT EXISTS checks(scope TEXT NOT NULL, feature TEXT NOT NULL, item_key TEXT NOT NULL,"
    " code TEXT NOT NULL, detail TEXT NOT NULL, checked_ns INTEGER NOT NULL, PRIMARY KEY(scope, feature, item_key))",
)
ATTENTION_CODES: tuple[str, ...] = ("no_tmdb", "not_found", "mismatch", "incomplete", "episode_missing")
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


def _import_items(data: Any) -> dict[str, Any]:
    if isinstance(data, dict):
        return {str(k): v for k, v in data.items()}
    if isinstance(data, list):
        out: dict[str, Any] = {}
        for row in data:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").strip()
            if key:
                out[key] = {k: v for k, v in row.items() if k != "key"}
        return out
    return {}


def _list_key(feature: str, item: Mapping[str, Any]) -> str:
    try:
        return history_event_key(item) if feature == "history" else canonical_key(item)
    except Exception:
        return ""


def _state_payload(raw: Any, feature: str) -> tuple[int, dict[str, Any]]:
    if isinstance(raw, Mapping) and isinstance(raw.get("items"), Mapping):
        try:
            ts = int(raw.get("ts") or 0)
        except Exception:
            ts = 0
        return ts, {str(k): v for k, v in raw["items"].items()}
    if isinstance(raw, Mapping):
        return 0, {str(k): v for k, v in raw.items()}
    if isinstance(raw, list):
        items: dict[str, Any] = {}
        for pos, value in enumerate(raw):
            key = _list_key(feature, value) if isinstance(value, Mapping) else ""
            items[key if key and not key.startswith("unknown:") else str(pos)] = value
        return 0, items
    raise ValueError("Tracker state is not an object or a list")


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


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


class TrackerStore:
    def __init__(
        self,
        root: Path | str,
        *,
        scope: str | None = None,
        chained: bool = True,
        log: LogFn | None = None,
    ) -> None:
        self.root = Path(root)
        self.scope = scope
        self.chained = chained
        self._scope = scope or ""
        self._log = log or _default_log

    @property
    def db_path(self) -> Path:
        return self.root / DB_NAME

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
            conn.execute("PRAGMA busy_timeout=30000")
            conn.execute("PRAGMA journal_mode=WAL")
            conn.execute("PRAGMA synchronous=NORMAL")
            if conn.execute("PRAGMA user_version").fetchone()[0] < SCHEMA_VERSION:
                conn.execute("BEGIN IMMEDIATE")
                for statement in _SCHEMA:
                    conn.execute(statement)
                conn.execute(f"PRAGMA user_version={SCHEMA_VERSION}")
                conn.execute("COMMIT")
        except Exception:
            conn.close()
            raise
        return conn

    @contextmanager
    def _db(self, *, create: bool = False) -> Generator[sqlite3.Connection | None]:
        key = str(self.db_path)
        if key not in _READY or not self.db_path.exists():
            files, snaps = self._legacy_files()
            if not create and not self.db_path.exists() and not files and snaps is None:
                yield None
                return
            conn = self._connect()
            try:
                if files or snaps is not None:
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
            conn.execute("ROLLBACK")
            raise
        conn.execute("COMMIT")

    def needs_migration(self) -> bool:
        files, snaps = self._legacy_files()
        return bool(files) or snaps is not None

    def migrate(self, progress: ProgressFn | None = None) -> dict[str, Any] | None:
        files, snaps = self._legacy_files()
        if not files and snaps is None:
            return None
        conn = self._connect()
        try:
            stats = self._import_legacy(conn, files, snaps, progress)
            _READY.add(str(self.db_path))
        finally:
            conn.close()
        return stats

    def _import_legacy(
        self,
        conn: sqlite3.Connection,
        files: list[Path],
        snaps: Path | None,
        progress: ProgressFn | None = None,
    ) -> dict[str, Any] | None:
        with _IMPORT_LOCK:
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
                for path, rel in snap_files:
                    feature = _snapshot_feature(path.name)
                    if feature is None or len(rel) > 2 or not path.is_file():
                        advance(path)
                        continue
                    try:
                        text = path.read_text("utf-8")
                        json.loads(text)
                        mtime_ns = path.stat().st_mtime_ns
                    except Exception:
                        raise TrackerUnreadable(feature, ["/".join(["snapshots", *rel])])
                    self._put_restore_point(conn, rel[0] if len(rel) == 2 else "", feature, path.name, text, created_ns=mtime_ns)
                    points += 1
                    advance(path)
                conn.execute(
                    "INSERT INTO meta(key, value) VALUES('legacy_import', ?)",
                    (_dumps({"ts": int(time.time()), "states": states, "docs": docs, "restore_points": points}),),
                )
            backup = self.root / f"json-backup-{time.strftime('%Y%m%dT%H%M%SZ', time.gmtime())}"
            moved = 0
            for path in [*files, *([snaps] if snaps is not None else [])]:
                try:
                    backup.mkdir(parents=True, exist_ok=True)
                    shutil.move(str(path), str(backup / path.name))
                    moved += 1
                except Exception as e:
                    self._log("common", "warn", "legacy_json_backup_failed", path=str(path), error=str(e))
            self._log("common", "info", "legacy_json_imported", states=states, docs=docs,
                      restore_points=points, backup=str(backup), moved=moved)
            return {"states": states, "docs": docs, "restore_points": points, "backup": backup.name, "moved": moved}

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
        conn.execute("DELETE FROM items WHERE scope=? AND feature=?", (scope, feature))
        conn.executemany(
            "INSERT INTO items(scope, feature, item_key, data) VALUES(?, ?, ?, ?)",
            [(scope, feature, str(key), _dumps(value)) for key, value in items.items()],
        )
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
            (scope, feature, name, ts if ts is not None else created_ns // 1_000_000_000, created_ns, text),
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
            return {str(key): json.loads(data) for key, data in rows}
        except Exception:
            raise TrackerUnreadable(feature, [DB_NAME])

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

    def _fallback_point(self, conn: sqlite3.Connection, feature: str) -> tuple[str, str] | None:
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
        return (str(row[0]), str(row[1])) if row else None

    def read_state(self, feature: str, *, snapshot_fallback: bool = True) -> StateRead | None:
        try:
            with self._db() as conn:
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
                    return StateRead(json.loads(point[1]), point[0])
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
                    self._put_state(conn, self._scope, feature, dict(items or {}), ts=int(ts or time.time()))
            return True
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
        return json.loads(row[0])

    def add_restore_point(
        self,
        feature: str,
        items: Mapping[str, Any],
        *,
        reuse_window: int = 0,
        retention_days: int = 30,
        max_snapshots: int = 64,
    ) -> None:
        now_ns = time.time_ns()
        text = _dumps({"ts": now_ns // 1_000_000_000, "items": dict(items or {})})
        try:
            with self._db(create=True) as conn:
                assert conn is not None
                with self._write(conn):
                    rows = self._point_rows(conn, feature)
                    if reuse_window > 0 and rows and (now_ns - rows[-1][3]) / 1e9 <= reuse_window:
                        conn.execute("UPDATE restore_points SET data=?, created_ns=? WHERE id=?", (text, now_ns, rows[-1][0]))
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
                    data = json.loads(row[0])
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
                for feature, count in conn.execute(
                    "SELECT feature, COUNT(*) FROM items WHERE scope='' GROUP BY feature"
                ).fetchall():
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
    ) -> tuple[int, list[tuple[str, Any]]]:
        if feature not in FEATURES:
            raise ValueError(f"Unsupported kind: {feature!r}")
        where = ["scope=''", "feature=?"]
        args: list[Any] = [feature]
        if attention:
            where.append(
                "item_key IN (SELECT item_key FROM checks WHERE scope='' AND feature=? AND code IN ("
                + ", ".join("?" for _ in ATTENTION_CODES) + "))"
            )
            args.extend([feature, *ATTENTION_CODES])
        if media == "movie":
            where.append("json_extract(data, '$.type')='movie'")
        elif media == "show":
            where.append("json_extract(data, '$.type') IN ('show', 'season', 'episode')")
        needle = text.strip().lower()
        if needle:
            where.append(
                "lower(coalesce(json_extract(data, '$.title'), '') || ' ' || coalesce(json_extract(data, '$.series_title'), '')) LIKE ? ESCAPE '\\'"
            )
            args.append("%" + needle.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_") + "%")
        when = (
            "coalesce(json_extract(data, '$.watched_at'), json_extract(data, '$.rated_at'),"
            " json_extract(data, '$.progress_at'), json_extract(data, '$.collected_at'),"
            " json_extract(data, '$.listed_at'), json_extract(data, '$.added_at'), '')"
        )
        title = "lower(coalesce(json_extract(data, '$.series_title'), json_extract(data, '$.title'), item_key))"
        order = {
            "recent": f"{when} DESC, item_key",
            "oldest": f"{when} ASC, item_key",
            "title": f"{title}, json_extract(data, '$.season'), json_extract(data, '$.episode'), item_key",
        }.get(sort, f"{when} DESC, item_key")
        clause = " AND ".join(where)
        with self._db() as conn:
            if conn is None:
                return 0, []
            try:
                total = int(conn.execute(f"SELECT COUNT(*) FROM items WHERE {clause}", args).fetchone()[0])
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
        values = [(f, k, code, _dumps(dict(detail)), now, f, k, text) for f, k, code, detail, text in rows]
        if not values:
            return
        with self._db() as conn:
            if conn is None:
                return
            with self._write(conn):
                conn.executemany(
                    "INSERT INTO checks(scope, feature, item_key, code, detail, checked_ns)"
                    " SELECT '', ?, ?, ?, ?, ? WHERE EXISTS(SELECT 1 FROM items WHERE scope='' AND feature=? AND item_key=? AND data=?)"
                    " ON CONFLICT(scope, feature, item_key) DO UPDATE SET code=excluded.code, detail=excluded.detail,"
                    " checked_ns=excluded.checked_ns",
                    values,
                )

    def prune_checks(self) -> None:
        with self._db() as conn:
            if conn is None:
                return
            with self._write(conn):
                conn.execute(
                    "DELETE FROM checks WHERE NOT EXISTS(SELECT 1 FROM items i WHERE i.scope=checks.scope"
                    " AND i.feature=checks.feature AND i.item_key=checks.item_key)"
                )

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

    def attention_counts(self) -> dict[str, int]:
        out = {feature: 0 for feature in FEATURES}
        with self._db() as conn:
            if conn is not None:
                for feature, count in conn.execute(
                    "SELECT c.feature, COUNT(*) FROM checks c JOIN items i ON i.scope=c.scope AND i.feature=c.feature"
                    " AND i.item_key=c.item_key WHERE c.scope='' AND c.code IN ("
                    + ", ".join("?" for _ in ATTENTION_CODES) + ") GROUP BY c.feature",
                    ATTENTION_CODES,
                ).fetchall():
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

    @staticmethod
    def _prune_changes(conn: sqlite3.Connection, now_ns: int) -> None:
        conn.execute("DELETE FROM changes WHERE created_ns < ?", (now_ns - CHANGE_RETENTION_DAYS * 86_400 * 1_000_000_000,))
        conn.execute(
            "DELETE FROM changes WHERE batch IN (SELECT batch FROM changes WHERE id <= (SELECT max(id) FROM changes) - ?)",
            (CHANGE_LIMIT,),
        )

    def change_items(
        self,
        feature: str,
        changes: Iterable[tuple[str, str | None, Mapping[str, Any] | None]],
        *,
        action: str,
        expected: Mapping[str, Any] | None = None,
    ) -> str:
        if feature not in FEATURES:
            raise ValueError(f"Unsupported kind: {feature!r}")
        rows = list(changes)
        if not rows:
            raise ValueError("no_changes")
        batch = uuid.uuid4().hex
        now = time.time_ns()
        with self.mutation(), self._db() as conn:
            if conn is None:
                raise KeyError(rows[0][0])
            with self._write(conn):
                for key, new_key, data in rows:
                    before = self._item_text(conn, feature, key)
                    if before is None:
                        raise KeyError(key)
                    if expected is not None and key in expected and _loads(before) != expected[key]:
                        raise TrackerConflict("changed_since", key)
                    target = (new_key or key) if data is not None else None
                    if target and target != key and self._item_text(conn, feature, target) is not None:
                        raise TrackerConflict("key_exists", target)
                    after = _dumps(dict(data)) if data is not None else None
                    conn.execute("DELETE FROM items WHERE scope='' AND feature=? AND item_key=?", (feature, key))
                    if target:
                        conn.execute(
                            "INSERT INTO items(scope, feature, item_key, data) VALUES('', ?, ?, ?)", (feature, target, after)
                        )
                    conn.execute(
                        "INSERT INTO changes(batch, scope, feature, action, old_key, new_key, before, after, created_ns)"
                        " VALUES(?, '', ?, ?, ?, ?, ?, ?, ?)",
                        (batch, feature, action, key, target, before, after, now),
                    )
                    self._forget_checks(conn, feature, [key, target])
                self._touch(conn, [feature], now)
                self._prune_changes(conn, now)
        self._log(feature, "info", "tracker_items_changed", action=action, count=len(rows), batch=batch)
        return batch

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
                    if new_key:
                        current = self._item_text(conn, feature, new_key)
                        if current is None or _edit_view(_loads(current)) != _edit_view(_loads(after)):
                            raise TrackerConflict("changed_since", new_key)
                        conn.execute("DELETE FROM items WHERE scope='' AND feature=? AND item_key=?", (feature, new_key))
                    if self._item_text(conn, feature, old_key) is not None:
                        raise TrackerConflict("changed_since", old_key)
                    conn.execute(
                        "INSERT INTO items(scope, feature, item_key, data) VALUES('', ?, ?, ?)", (feature, old_key, before)
                    )
                    conn.execute(
                        "INSERT INTO changes(batch, scope, feature, action, old_key, new_key, before, after, created_ns, undo_of)"
                        " VALUES(?, '', ?, 'undo', ?, ?, ?, ?, ?, ?)",
                        (undo_batch, feature, new_key or old_key, old_key, after, before, now, batch),
                    )
                    self._forget_checks(conn, feature, [old_key, new_key])
                conn.execute("UPDATE changes SET undone_ns=? WHERE batch=?", (now, batch))
                self._touch(conn, [str(row[0]) for row in rows], now)
                self._prune_changes(conn, now)
        self._log(str(rows[0][0]), "info", "tracker_change_undone", batch=batch, count=len(rows))
        return undo_batch

    def change_log(self, *, offset: int = 0, limit: int = 50, preview: int = 4) -> tuple[int, list[ChangeBatch]]:
        with self._db() as conn:
            if conn is None:
                return 0, []
            total = int(conn.execute("SELECT COUNT(DISTINCT batch) FROM changes WHERE scope=''").fetchone()[0])
            heads = conn.execute(
                "SELECT batch, min(feature), min(action), min(created_ns), max(undone_ns), max(undo_of), COUNT(*)"
                " FROM changes WHERE scope='' GROUP BY batch ORDER BY max(id) DESC LIMIT ? OFFSET ?",
                (max(1, min(int(limit), 200)), max(0, int(offset))),
            ).fetchall()
            out: list[ChangeBatch] = []
            for batch, feature, action, created_ns, undone_ns, undo_of, count in heads:
                rows = conn.execute(
                    "SELECT old_key, new_key, before, after FROM changes WHERE batch=? ORDER BY id LIMIT ?",
                    (batch, max(1, int(preview))),
                ).fetchall()
                try:
                    parsed = [(str(o), None if n is None else str(n), _loads(b), _loads(a)) for o, n, b, a in rows]
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
                for scope, name, data in conn.execute("SELECT scope, name, data FROM restore_points ORDER BY created_ns, id").fetchall():
                    zf.writestr("/".join(["snapshots", *([str(scope)] if scope else []), str(name)]), str(data))
        return buf.getvalue()

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
                    if parts[0] == "snapshots" and len(parts) in (2, 3):
                        feature = _snapshot_feature(parts[-1])
                        if feature is None:
                            continue
                        scope = parts[1] if len(parts) == 3 else ""
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
        max_snapshots: int = 64,
    ) -> dict[str, Any]:
        try:
            raw = json.loads(payload.decode("utf-8"))
        except Exception as e:
            raise ValueError(f"File is not valid JSON: {e}") from e
        if not isinstance(raw, dict):
            raise ValueError("JSON root must be an object")
        items = _import_items(raw.get("items") if "items" in raw else raw)
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
        max_snapshots: int = 64,
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

# services/view_cache.py
# CrossWatch - Persistent page snapshots and bounded background refresh
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from contextvars import ContextVar
from pathlib import Path
from typing import Any, Callable

from fastapi.responses import JSONResponse

STRICT_READ = ContextVar("view_cache_strict_read", default=False)
_LOG = logging.getLogger("crosswatch.views")
_RETENTION = 24 * 60 * 60
_MAX_BODY = 2 * 1024 * 1024
_MAX_ENTRIES = 64
_MAX_PENDING = 32
_MAX_MEMORY = 32 * 1024 * 1024


def digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"), default=str).encode()).hexdigest()


class ViewSuperseded(Exception):
    pass


class ViewCache:
    def __init__(self, *, workers: int = 1) -> None:
        self.workers = workers
        self.lock = threading.RLock()
        self.publication = threading.Lock()
        self.entries: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.pending: dict[str, int] = {}
        self.failures: dict[str, float] = {}
        self.generation = 0
        self.invalidated_at = 0.0
        self.pool: ThreadPoolExecutor | None = None
        self.stopped = False
        self.directories: set[Path] = set()

    def _trim(self) -> None:
        size = sum(entry.get("bytes", len(entry["body"]) * 2) for entry in self.entries.values())
        while len(self.entries) > _MAX_ENTRIES or size > _MAX_MEMORY:
            _, old = self.entries.popitem(last=False)
            size -= old.get("bytes", len(old["body"]) * 2)

    def get(self, base: Path, scope: Any, query: Any, revision: Callable[[], Any], build: Callable[[], Any], *, retry: bool = False) -> JSONResponse:
        key = digest((str(Path(base).resolve()), scope, query))
        directory = Path(base) / ".cw_cache" / "views"
        path = directory / f"{key}.json"
        now = time.time()
        with self.lock:
            self.directories.add(directory)
            entry = self.entries.get(key)
            if entry is None:
                try:
                    if path.stat().st_size <= 3 * _MAX_BODY:
                        saved = json.loads(path.read_text("utf-8"))
                        if saved.get("schema") == 1 and self.invalidated_at < saved["generated"] and now - saved["generated"] < _RETENTION and isinstance(saved.get("body"), str):
                            payload = json.loads(saved["body"])
                            if isinstance(payload, dict) and payload.get("ok") is not False and digest(payload) == saved.get("version") and isinstance(saved.get("revision"), str):
                                entry = {**saved, "checked": 0.0}
                                self.entries[key] = entry
                except (OSError, ValueError, KeyError, TypeError):
                    pass
            if entry and now - entry["generated"] >= _RETENTION:
                self.entries.pop(key, None)
                entry = None
            if retry:
                self.failures.pop(key, None)
                if entry:
                    entry["checked"] = 0.0
            failed = self.failures.get(key, 0) > now - 30
            needs_check = entry is None or now - entry.get("checked", 0) >= 5
            if needs_check and key not in self.pending and not failed and not self.stopped and len(self.pending) < _MAX_PENDING:
                if self.pool is None:
                    self.pool = ThreadPoolExecutor(max_workers=self.workers, thread_name_prefix="view-refresh")
                self.pending[key] = self.generation
                self.pool.submit(self._refresh, key, path, revision, build, self.generation)
            pending = key in self.pending or (needs_check and not failed and not self.stopped)
            if entry:
                self.entries.move_to_end(key)
            self._trim()
            payload: dict[str, Any] = json.loads(entry["body"]) if entry else {"ok": True}
            payload["_view"] = {
                "ready": entry is not None,
                "refreshing": pending,
                "version": entry["version"] if entry else "",
                "revision": entry["revision"] if entry else "",
                "updated_at": entry["generated"] if entry else None,
                "checked_at": (entry.get("checked") or entry["generated"]) if entry else None,
                "error": "refresh_failed" if failed else None,
                "retry_after": 30 if failed else 2,
            }
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})

    def _refresh(self, key: str, path: Path, revision: Callable[[], Any], build: Callable[[], Any], generation: int) -> None:
        token = STRICT_READ.set(True)
        try:
            with self.lock:
                if generation != self.generation or self.stopped:
                    return
            before = digest(revision())
            with self.lock:
                previous = self.entries.get(key)
            if previous and previous["revision"] == before and time.time() - previous["generated"] < 300:
                with self.lock:
                    if generation == self.generation and self.entries.get(key) is previous:
                        previous["checked"] = time.time()
                return
            result = build()
            if isinstance(result, JSONResponse):
                if result.status_code >= 400:
                    raise RuntimeError("view_response_failed")
                result = json.loads(bytes(result.body))
            if not isinstance(result, dict) or result.get("ok") is False or result.get("not_modified"):
                raise RuntimeError("view_build_failed")
            body = json.dumps(result, separators=(",", ":"), ensure_ascii=False)
            if len(body.encode()) > _MAX_BODY:
                raise RuntimeError("view_snapshot_too_large")
            if digest(revision()) != before:
                return
            entry = {"schema": 1, "revision": before, "body": body, "bytes": len(body.encode()), "version": digest(result), "generated": time.time(), "checked": time.time()}
            with self.lock:
                if generation != self.generation or self.stopped:
                    return
                self.entries[key] = entry
                self.entries.move_to_end(key)
                self.failures.pop(key, None)
                self._trim()
            with self.publication:
                with self.lock:
                    publish = generation == self.generation and not self.stopped
                if publish:
                    self._persist(path, entry)
        except ViewSuperseded:
            with self.lock:
                if generation == self.generation and key in self.entries:
                    self.entries[key]["checked"] = 0.0
        except Exception:
            with self.lock:
                if generation == self.generation:
                    self.failures[key] = time.time()
                    while len(self.failures) > _MAX_ENTRIES:
                        self.failures.pop(next(iter(self.failures)))
            _LOG.warning("Page refresh failed", exc_info=True)
        finally:
            STRICT_READ.reset(token)
            with self.lock:
                if self.pending.get(key) == generation:
                    self.pending.pop(key, None)

    def mark_stale(self) -> None:
        with self.lock:
            for entry in self.entries.values():
                entry["checked"] = 0.0
            self.failures.clear()

    def invalidate(self, base: Path | None = None) -> None:
        with self.publication:
            with self.lock:
                if base is not None:
                    self.directories.add(Path(base) / ".cw_cache" / "views")
                self.generation += 1
                self.invalidated_at = time.time()
                self.entries.clear()
                self.failures.clear()
                directories = tuple(self.directories)
            for directory in directories:
                for path in directory.glob("*.json"):
                    try:
                        path.unlink(missing_ok=True)
                    except OSError:
                        _LOG.warning("Could not invalidate page snapshot", exc_info=True)

    def _persist(self, path: Path, entry: dict[str, Any]) -> None:
        try:
            path.parent.mkdir(parents=True, exist_ok=True)
            temporary = path.with_suffix(".tmp")
            temporary.write_text(json.dumps(entry, ensure_ascii=False), "utf-8")
            temporary.replace(path)
            files = sorted(path.parent.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
            for old in files[_MAX_ENTRIES:]:
                old.unlink(missing_ok=True)
        except OSError:
            _LOG.warning("Could not persist page snapshot", exc_info=True)

    def stop(self) -> None:
        with self.lock:
            self.stopped = True
            self.generation += 1
            self.pending.clear()
            pool, self.pool = self.pool, None
        if pool:
            pool.shutdown(wait=False, cancel_futures=True)

    def start(self) -> None:
        with self.lock:
            self.stopped = False


CACHE = ViewCache()
AIRING_CACHE = ViewCache(workers=2)
CACHES = (CACHE, AIRING_CACHE)

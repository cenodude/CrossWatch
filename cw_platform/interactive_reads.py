# cw_platform/interactive_reads.py
# CrossWatch - Session-scoped provider observations for interactive sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from functools import wraps
import inspect
import json
import os
from pathlib import Path
import pickle
import sqlite3
import tempfile
import threading
import weakref


_ACTIVE: ContextVar[tuple[ReviewReads, bool] | None] = ContextVar("interactive_reads", default=None)


def _encode(value):
    return json.dumps(value, sort_keys=True, default=lambda obj: sorted(obj) if isinstance(obj, (set, frozenset)) else str(obj))


class ReviewReads:
    def __init__(self):
        descriptor, path = tempfile.mkstemp(prefix="cw-sync-inputs-", suffix=".sqlite")
        os.close(descriptor)
        self.path = Path(path)
        self.lock = threading.RLock()
        self.db = sqlite3.connect(path, check_same_thread=False)
        self.db.execute("PRAGMA journal_mode=OFF")
        self.db.execute("PRAGMA synchronous=OFF")
        self.db.execute("CREATE TABLE inputs (key TEXT PRIMARY KEY, value BLOB NOT NULL)")
        self.cleanup = weakref.finalize(self, self._cleanup, self.db, self.path)

    @staticmethod
    def _cleanup(db, path):
        db.close()
        path.unlink(missing_ok=True)

    def close(self):
        self.cleanup()

    def read(self, key, fetch, collecting):
        with self.lock:
            row = self.db.execute("SELECT value FROM inputs WHERE key=?", (key,)).fetchone()
            if row is not None:
                return pickle.loads(row[0])
        if not collecting:
            raise RuntimeError("This review is missing provider data. Refresh the review before applying.")
        value = fetch()
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO inputs VALUES (?, ?)", (key, pickle.dumps(value)))
            self.db.commit()
        return value

    def put(self, key, value):
        with self.lock:
            self.db.execute("INSERT OR REPLACE INTO inputs VALUES (?, ?)", (key, pickle.dumps(value)))
            self.db.commit()


@contextmanager
def use_review_reads(reads: ReviewReads | None, *, collecting: bool = False):
    token = _ACTIVE.set((reads, collecting) if reads is not None else None)
    try:
        yield
    finally:
        _ACTIVE.reset(token)


def replaying():
    active = _ACTIVE.get()
    return active is not None and not active[1]


def retaining():
    return _ACTIVE.get() is not None


def read_once(key, fetch, *, scoped=True):
    active = _ACTIVE.get()
    if active is None:
        return fetch()
    scope = (os.environ.get("CW_PAIR_SCOPE") or os.environ.get("CW_PAIR_KEY") or "") if scoped else ""
    encoded = _encode([scope, key])
    return active[0].read(encoded, fetch, active[1])


def _retained_identity(fn, args, kwargs, instance=None):
    signature = inspect.signature(fn)
    bound = signature.bind(*args, **kwargs)
    bound.apply_defaults()
    arguments = dict(bound.arguments)
    adapter = arguments.pop(next(iter(signature.parameters)), None)
    for flag in ("force", "force_refresh", "live", "page_size"):
        arguments.pop(flag, None)
    identity = [fn.__module__, fn.__name__, arguments]
    if fn.__module__.endswith("._playlists"):
        identity.append(str(instance if instance is not None else getattr(adapter, "instance_id", "default")))
    return identity


def retained_read(fn):
    @wraps(fn)
    def wrapped(*args, **kwargs):
        if not retaining():
            return fn(*args, **kwargs)
        identity = _retained_identity(fn, args, kwargs)
        return read_once(identity, lambda: fn(*args, **kwargs))
    return wrapped


def replace_retained(fn, value, *args, instance=None):
    active = _ACTIVE.get()
    if active is None:
        return
    scope = os.environ.get("CW_PAIR_SCOPE") or os.environ.get("CW_PAIR_KEY") or ""
    identity = _retained_identity(fn, (None, *args), {}, instance=instance)
    key = _encode([scope, identity])
    active[0].put(key, value)


def accepted_result(result, keys):
    result["accepted_keys"] = list(keys)
    result["presence_confirmed_keys"] = list(result.get("presence_confirmed_keys") or [])
    result["accepted_not_seen_live_keys"] = list(keys)
    return result


def remember_resource(reader, adapter, resource):
    if replaying():
        resources = list(reader(adapter))
        resources.append(resource)
        replace_retained(reader, resources, instance=getattr(adapter, "instance_id", "default"))


def history_states(index, provider, parse_date):
    states = {}
    for row in index.values():
        ident = row.get(f"{provider}_item_id") or row.get(f"_{provider}_item_id") or (row.get("ids") or {}).get(provider)
        if not ident:
            continue
        ident = str(ident)
        stamp = parse_date(row.get("watched_at"))
        previous = states.get(ident, (False, None))[1]
        if ident not in states or stamp is not None and (previous is None or stamp > previous):
            states[ident] = (True, stamp)
    return states

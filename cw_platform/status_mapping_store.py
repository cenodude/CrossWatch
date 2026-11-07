# cw_platform/status_mapping_store.py
# CrossWatch - Durable status baselines and interrupted write tracking.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import hashlib
import sqlite3
from contextlib import ExitStack, closing, contextmanager
from dataclasses import asdict
from pathlib import Path

from .config_base import CONFIG_BASE
from .run_control import raise_if_cancelled
from .status_mapping import Baseline, Direction, Endpoint, ItemScope, Observation, PairScope, ReadState, Status


def encode_observation(value: Observation) -> dict:
    return asdict(value)


def decode_observation(value: dict) -> Observation:
    return Observation(ReadState(value["state"]), Status(value["status"]) if value.get("status") else None,
                       value.get("native_status", ""), value.get("protected_token", ""), value.get("watchlisted"), value.get("rewatching"))


def decode_item(value: dict) -> ItemScope:
    pair = value["pair"]
    return ItemScope(PairScope(pair["pair_id"], Endpoint(**pair["a"]), Endpoint(**pair["b"]), pair.get("two_way", False)),
                     value["media"], value["a_id"], value["b_id"])


def pending_destinations(item: ItemScope, pending: dict) -> set[tuple[Endpoint, str, str]]:
    forward = (item.pair.b, item.media, item.b_id)
    reverse = (item.pair.a, item.media, item.a_id)
    if pending.get("direction") == Direction.FORWARD:
        return {forward}
    if pending.get("direction") == Direction.REVERSE:
        return {reverse}
    return {forward, reverse}


class StatusStore:
    def __init__(self, path: Path | None = None):
        self.path = (path or CONFIG_BASE() / "state" / "status-mapping.sqlite3").resolve()

    @contextmanager
    def _instance_lock(self, endpoint: Endpoint):
        identity = json.dumps(asdict(endpoint), sort_keys=True).encode()
        path = self.path.parent / (self.path.name + ".locks") / (hashlib.sha256(identity).hexdigest() + ".sqlite3")
        path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(path, timeout=0.25)) as conn:
            while True:
                raise_if_cancelled()
                try:
                    conn.execute("BEGIN IMMEDIATE")
                    break
                except sqlite3.OperationalError as exc:
                    if exc.sqlite_errorcode not in {sqlite3.SQLITE_BUSY, sqlite3.SQLITE_LOCKED}:
                        raise
            try:
                raise_if_cancelled()
                yield
            finally:
                conn.rollback()

    @contextmanager
    def exclusive_instances(self, endpoints):
        with ExitStack() as locks:
            for endpoint in sorted(set(endpoints), key=lambda value: (value.provider, value.instance)):
                locks.enter_context(self._instance_lock(endpoint))
            yield

    @staticmethod
    def key(item: ItemScope) -> str:
        value = asdict(item)
        value["pair"].pop("two_way", None)
        return json.dumps(value, sort_keys=True, separators=(",", ":"))

    @staticmethod
    def legacy_keys(item: ItemScope) -> tuple[str, str]:
        value = asdict(item)
        keys = []
        for mode in (False, True):
            value["pair"]["two_way"] = mode
            keys.append(json.dumps(value, sort_keys=True, separators=(",", ":")))
        return keys[0], keys[1]

    def get(self, item: ItemScope) -> dict:
        if not self.path.exists():
            return {}
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=30)) as conn:
            return self._get(conn, item)

    def _get(self, conn: sqlite3.Connection, item: ItemScope) -> dict:
        rows = conn.execute("SELECT key,value FROM status_state WHERE key IN (?,?,?)", (self.key(item), *self.legacy_keys(item))).fetchall()
        values = []
        for key, raw in rows:
            value = json.loads(raw)
            if value.get("pending") and key != self.key(item):
                value["pending"].setdefault("item", json.loads(key))
            values.append(value)
        pending = [value for value in values if value.get("pending")]
        if len(pending) > 1:
            return {"pending_conflict": True}
        if pending:
            return pending[0]
        return next((value for (key, _), value in zip(rows, values) if key == self.key(item)), values[0] if values else {})

    def baseline(self, item: ItemScope, state: dict) -> Baseline | None:
        value = state.get("baseline")
        if not value:
            return None
        return Baseline(item, value["mapping_fingerprint"], decode_observation(value["a"]), decode_observation(value["b"]))

    @staticmethod
    def _pending_operations(conn: sqlite3.Connection) -> list[tuple[ItemScope, dict]]:
        result = []
        for key, raw in conn.execute("SELECT key,value FROM status_state WHERE json_extract(value, '$.pending') IS NOT NULL"):
            pending = json.loads(raw).get("pending")
            if pending:
                pending.setdefault("item", json.loads(key))
                result.append((decode_item(pending["item"]), pending))
        return result

    def pending_targets(self) -> dict[tuple[Endpoint, str, str], set[str]]:
        if not self.path.exists():
            return {}
        with closing(sqlite3.connect(self.path.as_uri() + "?mode=ro", uri=True, timeout=30)) as conn:
            operations = self._pending_operations(conn)
        targets = {}
        for item, pending in operations:
            for target in pending_destinations(item, pending):
                targets.setdefault(target, set()).add(self.key(item))
        return targets

    def _put(self, conn: sqlite3.Connection, item: ItemScope, state: dict) -> None:
        conn.execute("INSERT OR REPLACE INTO status_state VALUES (?,?,?)", (self.key(item), item.pair.pair_id, json.dumps(state)))
        conn.execute("DELETE FROM status_state WHERE key IN (?,?)", self.legacy_keys(item))

    @staticmethod
    def _ensure_schema(conn: sqlite3.Connection) -> None:
        conn.execute("CREATE TABLE IF NOT EXISTS status_state(key TEXT PRIMARY KEY, pair_id TEXT NOT NULL, value TEXT NOT NULL)")
        conn.execute("CREATE INDEX IF NOT EXISTS status_pending ON status_state(key) WHERE json_extract(value, '$.pending') IS NOT NULL")

    def reserve(self, item: ItemScope, state: dict, *, expected_pending: dict | None = None) -> bool:
        pending = state["pending"]
        destinations = pending_destinations(item, pending)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=30)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_schema(conn)
            operations = self._pending_operations(conn)
            own = [value for scope, value in operations if self.key(scope) == self.key(item)]
            if own:
                if len(own) != 1 or expected_pending is None or own[0] != expected_pending or pending != expected_pending:
                    return False
            elif expected_pending is not None:
                return False
            if any(self.key(scope) != self.key(item) and destinations & pending_destinations(scope, value)
                   for scope, value in operations):
                return False
            self._put(conn, item, state)
        return True

    def put(self, item: ItemScope, state: dict) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=30)) as conn, conn:
            self._ensure_schema(conn)
            self._put(conn, item, state)

    def settle(self, baseline: Baseline, *, expected_state: dict) -> bool:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with closing(sqlite3.connect(self.path, timeout=30)) as conn, conn:
            conn.execute("BEGIN IMMEDIATE")
            self._ensure_schema(conn)
            current = self._get(conn, baseline.item)
            if current != expected_state or current.get("pending_conflict"):
                return False
            self._put(conn, baseline.item, {"baseline": {"mapping_fingerprint": baseline.mapping_fingerprint,
                                                       "a": encode_observation(baseline.a), "b": encode_observation(baseline.b)}})
        return True

    def purge(self, pair_id: str) -> None:
        if self.path.exists():
            with closing(sqlite3.connect(self.path, timeout=30)) as conn, conn:
                conn.execute("DELETE FROM status_state WHERE pair_id=?", (pair_id,))

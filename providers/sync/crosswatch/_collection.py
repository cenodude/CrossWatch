# /providers/sync/crosswatch/_collection.py
# CrossWatch tracker Module for collection management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
from typing import Any

from cw_platform.id_map import canonical_key, merge_ids, minimal as id_minimal

from ._common import (
    _atomic_write,
    _capture_mode,
    _maybe_restore,
    _pair_scope,
    _record_unresolved,
    _root,
    _snapshot_state,
    current_state_only,
    latest_snapshot_file,
    latest_state_file,
    make_logger,
    may_persist,
    readonly,
    scoped_file,
    state_file_for_read,
)

_dbg, _info, _warn, _error = make_logger("collection")


def _collection_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "collection.json")


def _accepted(obj: Mapping[str, Any]) -> dict[str, Any]:
    item = id_minimal(obj)
    for key in ("collected_at", "listed_at", "added_at"):
        value = obj.get(key)
        if isinstance(value, str) and value.strip():
            item["collected_at"] = value.strip()
            break
    return item


def _load_state(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {"ts": 0, "items": {}}

    root = _root(adapter)
    path = _collection_path(adapter)

    def _read_json(p: Path) -> Any | None:
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return None

    read_path = state_file_for_read(root, "collection", path)
    raw = _read_json(read_path)
    if raw is None:
        alt = latest_state_file(root, "collection")
        if alt and alt != path:
            raw = _read_json(alt)
    if raw is None and current_state_only(adapter):
        return {"ts": 0, "items": {}}
    if raw is None:
        snap = latest_snapshot_file(root, "collection")
        if snap:
            raw = _read_json(snap)
    if raw is None:
        return {"ts": 0, "items": {}}

    if isinstance(raw, list):
        items: dict[str, dict[str, Any]] = {}
        for obj in raw:
            if not isinstance(obj, Mapping):
                continue
            try:
                accepted = _accepted(obj)
            except Exception:
                continue
            key = canonical_key(accepted)
            if key:
                items[key] = accepted
        state = {"ts": 0, "items": items}
        if items and may_persist(adapter, path):
            _atomic_write(path, {"ts": int(time.time()), "items": items})
        return state

    if isinstance(raw, Mapping):
        if "items" in raw and isinstance(raw.get("items"), Mapping):
            ts = int(raw.get("ts", 0) or 0)
            items_raw = raw.get("items") or {}
            items2: dict[str, dict[str, Any]] = {}
            for key, value in items_raw.items():
                if not isinstance(value, Mapping):
                    continue
                try:
                    accepted = _accepted(value)
                except Exception:
                    continue
                ck = str(key) or canonical_key(accepted)
                if ck:
                    items2[ck] = accepted
            state = {"ts": ts, "items": items2}
            if items2 and may_persist(adapter, path):
                _atomic_write(path, {"ts": ts or int(time.time()), "items": items2})
            return state

        items3: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                continue
            try:
                accepted = _accepted(value)
            except Exception:
                continue
            ck = str(key) or canonical_key(accepted)
            if ck:
                items3[ck] = accepted
        state = {"ts": 0, "items": items3}
        if items3 and may_persist(adapter, path):
            _atomic_write(path, {"ts": int(time.time()), "items": items3})
        return state

    return {"ts": 0, "items": {}}


def _save_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _capture_mode() or readonly(adapter) or _pair_scope() is None:
        return
    _atomic_write(_collection_path(adapter), {"ts": int(time.time()), "items": dict(items or {})})


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    _maybe_restore(adapter, "collection", _save_state)
    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("collection") if callable(prog_factory) else None
    state = _load_state(adapter)
    out: dict[str, dict[str, Any]] = {}
    for key, value in dict(state.get("items") or {}).items():
        if not isinstance(value, Mapping):
            continue
        try:
            accepted = _accepted(value)
        except Exception:
            continue
        ck = canonical_key(accepted) or str(key)
        if ck:
            out[ck] = accepted
    total = len(out)
    if prog:
        try:
            prog.tick(total, total=total, force=True)
            prog.done()
        except Exception:
            pass
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []
    _maybe_restore(adapter, "collection", _save_state)
    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0
    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            accepted = _accepted(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        existing = cur.get(key)
        if isinstance(existing, dict):
            ex_ids = existing.get("ids") if isinstance(existing.get("ids"), dict) else {}
            in_ids = accepted.get("ids") if isinstance(accepted.get("ids"), dict) else {}
            merged = merge_ids(ex_ids, in_ids)
            if merged:
                accepted["ids"] = merged
            if existing.get("collected_at") and not accepted.get("collected_at"):
                accepted["collected_at"] = existing.get("collected_at")
        if existing != accepted:
            cur[key] = accepted
            changed += 1
    if changed:
        _snapshot_state(adapter, cur, "collection", reuse_window=60)
        _save_state(adapter, cur)
    unresolved = _record_unresolved(adapter, unresolved_src, "collection") if unresolved_src else []
    return changed, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []
    _maybe_restore(adapter, "collection", _save_state)
    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0
    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            accepted = _accepted(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        if key in cur:
            del cur[key]
            changed += 1
    if changed:
        _snapshot_state(adapter, cur, "collection", reuse_window=60)
        _save_state(adapter, cur)
    unresolved = _record_unresolved(adapter, unresolved_src, "collection") if unresolved_src else []
    return changed, unresolved

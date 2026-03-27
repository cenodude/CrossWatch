# /providers/sync/crosswatch/_watchlist.py
# CrossWatch tracker Module for watchlist Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.config_base import load_config, save_config
from cw_platform.id_map import canonical_key, merge_ids, minimal as id_minimal
from cw_platform.metadata import MetadataManager

from ._common import (
    _atomic_write,
    _capture_mode,
    _maybe_restore,
    _pair_scope,
    _record_unresolved,
    _root,
    _snapshot_state,
    latest_snapshot_file,
    latest_state_file,
    make_logger,
    pair_scoped,
    scoped_file,
)

_dbg, _info, _warn, _error = make_logger("watchlist")

_META: MetadataManager | None = None


def _meta() -> MetadataManager | None:
    global _META
    if _META is not None:
        return _META
    try:
        _META = MetadataManager(load_config, save_config)
        return _META
    except Exception:
        _META = None
        return None


def _type_to_entity(typ: Any) -> str:
    t = str(typ or "").lower().strip()
    return "movie" if t == "movie" else "show"


def _normalize_imdb(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    if s.isdigit():
        return f"tt{s}"
    if not s.startswith("tt"):
        return f"tt{s}"
    return s


def _ensure_tmdb_for_item(item: dict[str, Any]) -> bool:
    ids = dict(item.get("ids") or {}) if isinstance(item.get("ids"), dict) else {}
    if ids.get("tmdb") or item.get("tmdb"):
        if item.get("tmdb") and not ids.get("tmdb"):
            ids = merge_ids(ids, {"tmdb": item.get("tmdb")})
            item["ids"] = ids
            return True
        return False

    imdb = _normalize_imdb(ids.get("imdb") or item.get("imdb"))
    if not imdb:
        return False

    mm = _meta()
    if not mm:
        return False

    try:
        res = mm.resolve(
            entity=_type_to_entity(item.get("type")),
            ids={"imdb": imdb},
            need={"poster": False, "backdrop": False, "overview": False},
        )
        tmdb = ((res or {}).get("ids") or {}).get("tmdb")
        if not tmdb:
            return False
        ids = merge_ids(ids, {"tmdb": tmdb, "imdb": imdb})
        item["ids"] = ids
        return True
    except Exception:
        return False


def _watchlist_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "watchlist.json")


def _load_state(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {"ts": 0, "items": {}}

    root = _root(adapter)
    path = _watchlist_path(adapter)
    raw: Any | None

    def _read_json(p: Path) -> Any | None:
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return None

    raw = _read_json(path)
    if raw is None:
        alt = latest_state_file(root, "watchlist")
        if alt and alt != path:
            raw = _read_json(alt)
    if raw is None:
        snap = latest_snapshot_file(root, "watchlist")
        if snap:
            raw = _read_json(snap)
    if raw is None:
        return {"ts": 0, "items": {}}

    if isinstance(raw, list):
        items: dict[str, dict[str, Any]] = {}
        for obj in raw:
            if not isinstance(obj, Mapping):
                continue
            key = canonical_key(obj)
            if not key:
                continue
            items[key] = id_minimal(obj)
        state = {"ts": 0, "items": items}
        if not pair_scoped() and items and not path.exists():
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
                ck = str(key) or canonical_key(value)
                if not ck:
                    continue
                items2[ck] = id_minimal(value)
            state = {"ts": ts, "items": items2}
            if not pair_scoped() and items2 and not path.exists():
                _atomic_write(path, {"ts": ts or int(time.time()), "items": items2})
            return state
        items3: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                continue
            ck = str(key) or canonical_key(value)
            if not ck:
                continue
            items3[ck] = id_minimal(value)
        state = {"ts": 0, "items": items3}
        if not pair_scoped() and items3 and not path.exists():
            _atomic_write(path, {"ts": int(time.time()), "items": items3})
        return state
    return {"ts": 0, "items": {}}


def _save_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    payload = {"ts": int(time.time()), "items": dict(items or {})}
    _atomic_write(_watchlist_path(adapter), payload)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    _maybe_restore(adapter, "watchlist", _save_state)
    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("watchlist") if callable(prog_factory) else None
    state = _load_state(adapter)
    items = dict(state.get("items") or {})

    changed = 0
    if not _capture_mode():
        for v in items.values():
            if not isinstance(v, dict):
                continue
            if _ensure_tmdb_for_item(v):
                changed += 1

    if changed:
        _snapshot_state(adapter, items, "watchlist")
        _save_state(adapter, items)
    out: dict[str, dict[str, Any]] = {}
    for key, value in items.items():
        if not isinstance(value, Mapping):
            continue
        ck = canonical_key(value) or str(key)
        if not ck:
            continue
        out[ck] = id_minimal(value)
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
    _maybe_restore(adapter, "watchlist", _save_state)
    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0
    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            minimal = id_minimal(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(minimal)
        if not key:
            unresolved_src.append(obj)
            continue
        existing = cur.get(key)
        if isinstance(existing, dict):
            ex_ids = existing.get("ids") if isinstance(existing.get("ids"), dict) else {}
            in_ids = minimal.get("ids") if isinstance(minimal.get("ids"), dict) else {}
            merged = merge_ids(ex_ids, in_ids)
            if merged:
                minimal["ids"] = merged

        if not _capture_mode():
            _ensure_tmdb_for_item(minimal)

        if existing != minimal:
            cur[key] = minimal
            changed += 1
    if changed:
        _snapshot_state(adapter, cur, "watchlist")
        _save_state(adapter, cur)
    unresolved = _record_unresolved(adapter, unresolved_src, "watchlist") if unresolved_src else []
    return changed, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []
    _maybe_restore(adapter, "watchlist", _save_state)
    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0
    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            minimal = id_minimal(obj)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(minimal)
        if not key:
            unresolved_src.append(obj)
            continue
        if key in cur:
            del cur[key]
            changed += 1
    if changed:
        _snapshot_state(adapter, cur, "watchlist")
        _save_state(adapter, cur)
    unresolved = _record_unresolved(adapter, unresolved_src, "watchlist") if unresolved_src else []
    return changed, unresolved

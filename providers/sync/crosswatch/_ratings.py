# /providers/sync/crosswatch/_ratings.py
# CrossWatch tracker Module for Ratings Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.id_map import canonical_key, minimal as id_minimal

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

_dbg, _info, _warn, _error = make_logger("ratings")


def _now_iso_z() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _accepted(obj: Mapping[str, Any]) -> dict[str, Any]:
    base = id_minimal(obj)
    out: dict[str, Any] = dict(base)

    typ = str(obj.get("type") or base.get("type") or "")
    if typ == "episode":
        st = obj.get("series_title") or obj.get("show_title") or obj.get("series") or obj.get("show")
        if st:
            out["series_title"] = str(st)
        if obj.get("series_year") is not None:
            out["series_year"] = obj.get("series_year")
        season = int(obj.get("season") or 0)
        episode = int(obj.get("episode") or 0)
        if season:
            out["season"] = season
        if episode:
            out["episode"] = episode
        if season and episode:
            out["title"] = f"S{season:02d}E{episode:02d}"
        elif "title" in obj:
            out["title"] = obj.get("title")
        if "year" in obj:
            out["year"] = obj.get("year")
        si = obj.get("show_ids")
        if isinstance(si, Mapping):
            out["show_ids"] = dict(si)
    else:
        for k in ("title", "year"):
            if k in obj:
                out[k] = obj.get(k)

    if obj.get("rating") is not None:
        out["rating"] = obj.get("rating")
    if obj.get("liked") is not None:
        out["liked"] = bool(obj.get("liked"))
    ra = obj.get("rated_at")
    if ra:
        out["rated_at"] = str(ra)
    elif obj.get("rating") is not None or obj.get("liked") is not None:
        out["rated_at"] = _now_iso_z()
    return out


def _ratings_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "ratings.json")


def _load_state(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {"ts": 0, "items": {}}
    root = _root(adapter)
    path = _ratings_path(adapter)
    raw: Any | None

    def _read_json(p: Path) -> Any | None:
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return None

    raw = _read_json(path)
    if raw is None:
        alt = latest_state_file(root, "ratings")
        if alt and alt != path:
            raw = _read_json(alt)
    if raw is None:
        snap = latest_snapshot_file(root, "ratings")
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
            items[key] = _accepted(obj)
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
                items2[ck] = _accepted(value)
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
            items3[ck] = _accepted(value)
        state = {"ts": 0, "items": items3}
        if not pair_scoped() and items3 and not path.exists():
            _atomic_write(path, {"ts": int(time.time()), "items": items3})
        return state

    return {"ts": 0, "items": {}}


def _save_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _capture_mode() or _pair_scope() is None:
        return
    payload = {"ts": int(time.time()), "items": dict(items or {})}
    _atomic_write(_ratings_path(adapter), payload)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    _maybe_restore(adapter, "ratings", _save_state)

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("ratings") if callable(prog_factory) else None

    state = _load_state(adapter)
    items = dict(state.get("items") or {})
    out: dict[str, dict[str, Any]] = {}

    for key, value in items.items():
        if not isinstance(value, Mapping):
            continue
        ck = canonical_key(value) or str(key)
        if not ck:
            continue
        out[ck] = _accepted(value)

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

    _maybe_restore(adapter, "ratings", _save_state)

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
        new_ts = str(accepted.get("rated_at") or "")
        old_ts = str((existing or {}).get("rated_at") or "")
        if existing is None or old_ts <= new_ts:
            cur[key] = accepted
            changed += 1

    if changed:
        _snapshot_state(adapter, cur, "ratings")
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src, "ratings") if unresolved_src else []
    return changed, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter, "ratings", _save_state)

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
        _snapshot_state(adapter, cur, "ratings")
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src, "ratings") if unresolved_src else []
    return changed, unresolved

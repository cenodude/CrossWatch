# /providers/sync/crosswatch/_progress.py
# CrossWatch tracker Module for Progress Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from collections.abc import Iterable, Mapping
from pathlib import Path
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
    may_persist,
    pair_scoped,
    readonly,
    scoped_file,
    state_file_for_read,
)

_dbg, _info, _warn, _error = make_logger("progress")


def _progress_path(adapter: Any) -> Path:
    return scoped_file(_root(adapter), "progress.json")


def _as_int(v: Any) -> int | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        return int(float(v))
    except Exception:
        return None


def _as_float(v: Any) -> float | None:
    try:
        if v is None or isinstance(v, bool):
            return None
        n = float(v)
        return n if n == n else None
    except Exception:
        return None


def _tmdb_metadata_provider(adapter: Any) -> Any | None:
    cache_key = "_crosswatch_progress_tmdb_metadata_provider"
    cached = getattr(adapter, cache_key, None)
    if cached is not None:
        return cached
    cfg = getattr(adapter, "config", None) or getattr(adapter, "raw_cfg", None) or {}
    if not isinstance(cfg, Mapping):
        return None
    tmdb_obj = cfg.get("tmdb")
    tmdb: Mapping[str, Any] = tmdb_obj if isinstance(tmdb_obj, Mapping) else {}
    metadata_obj = cfg.get("metadata")
    metadata: Mapping[str, Any] = metadata_obj if isinstance(metadata_obj, Mapping) else {}
    if not str(tmdb.get("api_key") or metadata.get("tmdb_api_key") or "").strip():
        return None
    try:
        from providers.metadata._meta_TMDB import TmdbProvider

        provider = TmdbProvider(lambda: dict(cfg), lambda _cfg: None)
    except Exception:
        return None
    try:
        setattr(adapter, cache_key, provider)
    except Exception:
        pass
    return provider


def _metadata_show_detail(adapter: Any, show_ids: Mapping[str, Any]) -> dict[str, Any]:
    provider = _tmdb_metadata_provider(adapter)
    fetch_ids = {
        key: str(show_ids.get(key) or "").strip()
        for key in ("tmdb", "imdb", "tvdb")
        if str(show_ids.get(key) or "").strip()
    }
    if provider is None or not fetch_ids:
        return {}
    try:
        detail = provider.fetch(entity="tv", ids=fetch_ids, need={"poster": False, "backdrop": False, "ids": False})
    except Exception:
        return {}
    if not isinstance(detail, Mapping):
        return {}
    out: dict[str, Any] = {}
    title = str(detail.get("title") or "").strip()
    if title:
        out["title"] = title
    year = _as_int(detail.get("year"))
    if year is not None:
        out["year"] = year
    return out


def _accepted(obj: Mapping[str, Any], adapter: Any | None = None) -> dict[str, Any]:
    base = id_minimal(obj)
    out: dict[str, Any] = dict(base)

    typ = str(obj.get("type") or base.get("type") or "").strip().lower()
    if typ == "episode":
        show_ids: dict[str, Any] = {}
        base_show_ids = base.get("show_ids")
        if isinstance(base_show_ids, Mapping):
            show_ids.update(dict(base_show_ids))
        raw_show_ids = obj.get("show_ids")
        if isinstance(raw_show_ids, Mapping):
            show_ids.update(dict(raw_show_ids))
        st = obj.get("series_title") or obj.get("show_title") or obj.get("series") or obj.get("show")
        if not st and adapter is not None and show_ids:
            detail = _metadata_show_detail(adapter, show_ids)
            st = detail.get("title")
            if obj.get("series_year") is None and detail.get("year") is not None:
                out["series_year"] = detail.get("year")
        if st:
            out["series_title"] = str(st)
        if obj.get("series_year") is not None:
            out["series_year"] = obj.get("series_year")
        season = _as_int(obj.get("season"))
        episode = _as_int(obj.get("episode"))
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
        if show_ids:
            out["show_ids"] = show_ids
    else:
        for k in ("title", "year"):
            if k in obj:
                out[k] = obj.get(k)

    progress_ms = None
    for k in ("progress_ms", "progressMs", "viewOffset", "progress"):
        progress_ms = _as_int(obj.get(k))
        if progress_ms is not None:
            break
    progress_percent = None
    for k in ("progress_percent", "progressPercent", "percent", "position_percent", "resume_percent"):
        progress_percent = _as_float(obj.get(k))
        if progress_percent is not None:
            break
    if progress_ms is None and progress_percent is None:
        raise ValueError("progress_ms_missing")
    if progress_ms is not None:
        out["progress_ms"] = max(0, progress_ms)
    if progress_percent is not None:
        out["progress_percent"] = round(max(0.0, min(100.0, float(progress_percent))), 3)

    duration_ms = None
    for k in ("duration_ms", "durationMs", "duration"):
        duration_ms = _as_int(obj.get(k))
        if duration_ms is not None:
            break
    if duration_ms is not None and duration_ms > 0:
        out["duration_ms"] = int(duration_ms)

    progress_at = (
        obj.get("progress_at")
        or obj.get("progressAt")
        or obj.get("last_played")
        or obj.get("lastPlayed")
        or obj.get("lastViewedAt")
    )
    if isinstance(progress_at, str) and progress_at.strip():
        out["progress_at"] = progress_at.strip()

    return out


def _load_state(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {"ts": 0, "items": {}}
    root = _root(adapter)
    path = _progress_path(adapter)
    raw: Any | None

    def _read_json(p: Path) -> Any | None:
        try:
            return json.loads(p.read_text("utf-8"))
        except Exception:
            return None

    read_path = state_file_for_read(root, "progress", path)
    raw = _read_json(read_path)
    if raw is None:
        alt = latest_state_file(root, "progress")
        if alt and alt != path:
            raw = _read_json(alt)
    if raw is None:
        snap = latest_snapshot_file(root, "progress")
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
                accepted = _accepted(obj, adapter)
            except Exception:
                continue
            key = canonical_key(accepted)
            if not key:
                continue
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
                    accepted = _accepted(value, adapter)
                except Exception:
                    continue
                ck = str(key) or canonical_key(accepted)
                if not ck:
                    continue
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
                accepted = _accepted(value, adapter)
            except Exception:
                continue
            ck = str(key) or canonical_key(accepted)
            if not ck:
                continue
            items3[ck] = accepted
        state = {"ts": 0, "items": items3}
        if items3 and may_persist(adapter, path):
            _atomic_write(path, {"ts": int(time.time()), "items": items3})
        return state

    return {"ts": 0, "items": {}}


def _save_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _capture_mode() or readonly(adapter) or _pair_scope() is None:
        return
    payload = {"ts": int(time.time()), "items": dict(items or {})}
    _atomic_write(_progress_path(adapter), payload)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    _maybe_restore(adapter, "progress", _save_state)

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("progress") if callable(prog_factory) else None

    state = _load_state(adapter)
    items = dict(state.get("items") or {})
    out: dict[str, dict[str, Any]] = {}

    for key, value in items.items():
        if not isinstance(value, Mapping):
            continue
        try:
            accepted = _accepted(value, adapter)
        except Exception:
            continue
        ck = canonical_key(accepted) or str(key)
        if not ck:
            continue
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

    _maybe_restore(adapter, "progress", _save_state)

    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        try:
            accepted = _accepted(obj, adapter)
        except Exception:
            unresolved_src.append(obj)
            continue
        key = canonical_key(accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        new_ms = _as_int(accepted.get("progress_ms"))
        new_percent = _as_float(accepted.get("progress_percent"))
        if (new_ms is None or new_ms <= 0) and (new_percent is None or new_percent <= 0):
            unresolved_src.append(obj)
            continue
        existing = cur.get(key)
        old_ms = _as_int((existing or {}).get("progress_ms"))
        old_percent = _as_float((existing or {}).get("progress_percent"))
        new_ts = str(accepted.get("progress_at") or "")
        old_ts = str((existing or {}).get("progress_at") or "")
        should_write = (
            existing is None
            or (new_ms is not None and (old_ms is None or new_ms > old_ms or (new_ms == old_ms and new_ts and old_ts <= new_ts)))
            or (new_ms is None and new_percent is not None and (old_percent is None or new_percent > old_percent or (abs(new_percent - old_percent) <= 0.001 and new_ts and old_ts <= new_ts)))
        )
        if should_write:
            cur[key] = accepted
            changed += 1

    if changed:
        _snapshot_state(adapter, cur, "progress", reuse_window=60)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src, "progress") if unresolved_src else []
    return changed, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter, "progress", _save_state)

    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        base = id_minimal(obj)
        key = canonical_key(base)
        if not key:
            unresolved_src.append(obj)
            continue
        if key in cur:
            del cur[key]
            changed += 1

    if changed:
        _snapshot_state(adapter, cur, "progress", reuse_window=60)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src, "progress") if unresolved_src else []
    return changed, unresolved

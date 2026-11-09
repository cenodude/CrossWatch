# /providers/sync/crosswatch/_history.py
# CrossWatch tracker Module for History Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from copy import copy
from typing import Any, Iterable, Mapping

from cw_platform.anime_mapping.episodes import resolve_absolute, resolve_axis_coordinate_map
from cw_platform.anime_mapping.history_coords import ALIAS_FIELD, coordinate_tokens
from cw_platform.anime_mapping.overrides import overrides_path
from cw_platform.anime_mapping.storage import index_ready, paths as mapping_paths
from cw_platform.history_events import has_history_watch, history_sync_key, minimal_history_item
from cw_platform.id_map import canonical_key
from cw_platform.tracker_history_identity import PLEX_ORIGINS, TYPE_ORIGINS, event_identities, plex_origins, type_origins

from ._common import (
    _capture_mode,
    _maybe_restore,
    _pair_scope,
    _record_unresolved,
    _snapshot_state,
    current_state_only,
    make_logger,
    may_persist,
    merge_tracker_identity,
    preserve_tracker_identity,
    pair_scoped,
    read_state,
    readonly,
    tracker_minimal,
    write_state,
)

_dbg, _info, _warn, _error = make_logger("history")
_ALIASES: dict[str, Any] = {"key": None, "rows": None}
_MISS_SECONDS = 300.0


def _rewatches_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_history_rewatches"))


def _history_key(adapter: Any, item: Mapping[str, Any], fallback_key: Any = None) -> str:
    return history_sync_key(item, fallback_key, event_mode=_rewatches_enabled(adapter))


def _mapping_tag(adapter: Any) -> str:
    cfg = getattr(adapter, "config", {}) or {}
    block = cfg.get("anime_mapping") or {}
    options = cfg.get("_cw_pair_feature_options") or {}
    if not block.get("enabled") or options.get("use_anime_mapping") is False:
        return ""
    return str(block.get("release_tag") or "v3")


def _alias_context(adapter: Any) -> tuple[str, dict[Any, Any] | None]:
    tag = _mapping_tag(adapter)
    if not tag:
        return "", None
    try:
        files = mapping_paths(tag)
        key = tuple((str(p), p.stat().st_mtime_ns, p.stat().st_size) if p.exists() else str(p)
                    for p in (files["db"], files["state"], overrides_path()))
        if _ALIASES["key"] != key:
            _ALIASES.update(key=key, rows={} if index_ready(tag) else None)
        return tag, _ALIASES["rows"]
    except Exception:
        return tag, None


def _resolve_aliases(tag: str, rows: dict[Any, Any], item: Mapping[str, Any], fresh: bool) -> list[str]:
    ids = {str(k): str(v) for k, v in (item.get("show_ids") or {}).items() if v}
    axes = [(p, i) for p, i in ids.items() if p in ("tvdb", "tmdb")]
    if not axes:
        return []
    own = (int(item["season"]), int(item["episode"]))
    key = (tuple(sorted(ids.items())), own)
    cached = rows.get(key)
    if fresh or cached is None or cached[1] < time.monotonic():
        hit = resolve_absolute(item, release_tag=tag)
        coords = resolve_axis_coordinate_map({hit.namespace: hit.target_id}, hit.absolute, release_tag=tag) if hit else {}
        tokens = sorted(f"{p}:{i}#s{s:02d}e{e:02d}".lower() for (p, i), (s, e) in coords.items())
        valid = all(coords.get(axis) == own for axis in axes) and len(coords) == len({p for p, _ in coords}) > 1
        rows[key] = (tokens, float("inf")) if valid else ([], time.monotonic() + _MISS_SECONDS)
    return rows[key][0]


def _anime_identity(item: Mapping[str, Any], context: tuple[str, dict[Any, Any] | None], fresh: bool = False) -> dict[str, Any]:
    tag, rows = context
    out = dict(item)
    saved = sorted(coordinate_tokens(item))
    out.pop(ALIAS_FIELD, None)
    if not tag or item.get("type") != "episode":
        return out
    aliases = saved
    if rows is not None:
        try:
            aliases = _resolve_aliases(tag, rows, item, fresh)
        except Exception:
            aliases = saved
    if aliases:
        out[ALIAS_FIELD] = aliases
    return out


def _refresh_saved_aliases(items: dict[str, dict[str, Any]], context: tuple[str, dict[Any, Any] | None]) -> bool:
    if not context[0] or context[1] is None:
        return False
    changed = False
    for key, value in items.items():
        saved = coordinate_tokens(value)
        if not saved:
            continue
        current = _anime_identity(value, context)
        if coordinate_tokens(current) != saved:
            items[key] = current
            changed = True
    return changed


def _anime_keys(adapter: Any, item: Mapping[str, Any], key: str = "", aliases: bool = True) -> set[str]:
    base = canonical_key(item)
    keys = {base, *(coordinate_tokens(item) if aliases and _mapping_tag(adapter) else ())}
    if _rewatches_enabled(adapter):
        _, at, event = (key or _history_key(adapter, item)).partition("@")
        keys = {k + at + event for k in keys}
    return keys


def _key_index(adapter: Any, items: Mapping[str, Mapping[str, Any]], aliases: bool) -> dict[str, set[str]]:
    out: dict[str, set[str]] = {}
    for key, value in items.items():
        for alias in _anime_keys(adapter, value, key, aliases):
            out.setdefault(alias, set()).add(key)
    return out


def _history_minimal(adapter: Any, item: Mapping[str, Any], fallback_key: Any = None,
                     previous: Mapping[str, Any] | None = None) -> dict[str, Any]:
    out = preserve_tracker_identity(minimal_history_item(item, fallback_key, event_mode=_rewatches_enabled(adapter)), item)
    if coordinate_tokens(item):
        out[ALIAS_FIELD] = sorted(coordinate_tokens(item))
    if item.get(TYPE_ORIGINS):
        out[TYPE_ORIGINS] = item[TYPE_ORIGINS]
    origins = plex_origins({"previous": previous, "current": item})
    if origins:
        out[PLEX_ORIGINS] = sorted(origins)
    return out


def _accepted(obj: Mapping[str, Any]) -> dict[str, Any]:
    base = tracker_minimal(obj)
    out: dict[str, Any] = dict(base)
    if obj.get("watched") is not None:
        out["watched"] = bool(obj.get("watched"))
    wa = obj.get("watched_at")
    if wa:
        out["watched_at"] = str(wa)

    typ = str(obj.get("type") or base.get("type") or "")
    if typ == "episode":
        st = obj.get("series_title") or obj.get("show_title") or obj.get("series") or obj.get("show")
        if st:
            out["series_title"] = str(st)
        if obj.get("series_year") is not None:
            out["series_year"] = obj.get("series_year")
        season_raw = obj.get("season")
        season = None if season_raw is None or season_raw == "" else int(season_raw)
        episode = int(obj.get("episode") or 0)
        if season is not None:
            out["season"] = season
        if episode:
            out["episode"] = episode
        if season is not None and episode:
            out["title"] = f"S{season:02d}E{episode:02d}"
        elif "title" in obj:
            out["title"] = obj.get("title")
        if "year" in obj:
            out["year"] = obj.get("year")
    else:
        for k in ("title", "year", "season", "episode", "series_title", "series_year"):
            if k in obj:
                out[k] = obj.get(k)

    si = obj.get("show_ids")
    if isinstance(si, Mapping):
        out["show_ids"] = dict(si)
    for k in ("provider_event_id", "history_id", "_history_id", "rewatch_id"):
        if obj.get(k) not in (None, ""):
            out[k] = obj.get(k)
    return out


def _load_state(adapter: Any) -> dict[str, Any]:
    if _pair_scope() is None:
        return {"ts": 0, "items": {}}

    raw = read_state(adapter, "history", snapshot_fallback=not current_state_only(adapter))
    if raw is None:
        return {"ts": 0, "items": {}}

    if isinstance(raw, list):
        items: dict[str, dict[str, Any]] = {}
        for obj in raw:
            if not isinstance(obj, Mapping):
                continue
            key = _history_key(adapter, obj)
            if not key:
                continue
            items[key] = _history_minimal(adapter, obj, key, items.get(key))
        state = {"ts": 0, "items": items}
        if items and may_persist(adapter, "history"):
            write_state(adapter, "history", items)
        return state

    if isinstance(raw, Mapping):
        if "items" in raw and isinstance(raw.get("items"), Mapping):
            ts = int(raw.get("ts", 0) or 0)
            items_raw = raw.get("items") or {}
            items: dict[str, dict[str, Any]] = {}
            for key, value in items_raw.items():
                if not isinstance(value, Mapping):
                    continue
                ck = _history_key(adapter, value, key)
                if not ck:
                    continue
                items[ck] = _history_minimal(adapter, value, key, items.get(ck))
            state = {"ts": ts, "items": items}
            if items and may_persist(adapter, "history"):
                write_state(adapter, "history", items, ts=ts)
            return state

        items: dict[str, dict[str, Any]] = {}
        for key, value in raw.items():
            if not isinstance(value, Mapping):
                continue
            ck = _history_key(adapter, value, key)
            if not ck:
                continue
            items[ck] = _history_minimal(adapter, value, key, items.get(ck))
        state = {"ts": 0, "items": items}
        if items and may_persist(adapter, "history"):
            write_state(adapter, "history", items)
        return state

    return {"ts": 0, "items": {}}


def _save_state(adapter: Any, items: Mapping[str, Mapping[str, Any]]) -> None:
    if _capture_mode() or readonly(adapter) or _pair_scope() is None:
        return
    write_state(adapter, "history", items)


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    _maybe_restore(adapter, "history", _save_state)

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("history") if callable(prog_factory) else None

    state = _load_state(adapter)
    items = dict(state.get("items") or {})
    out: dict[str, dict[str, Any]] = {}
    context = _alias_context(adapter)

    for key, value in items.items():
        if not isinstance(value, Mapping):
            continue
        ck = _history_key(adapter, value, key)
        if not ck:
            continue
        out[ck] = _anime_identity(_history_minimal(adapter, value, key), context)

    total = len(out)
    if prog:
        try:
            prog.tick(total, total=total, force=True)
            prog.done()
        except Exception:
            pass

    return out


def filter_cached_adds(adapter: Any, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    reader = copy(adapter)
    reader.config = {**getattr(adapter, "config", {}), "_cw_readonly": True, "_cw_history_rewatches": True}
    corrected = type_origins(_load_state(reader).get("items") or {})
    return [item for item in items if event_identities(item).isdisjoint(corrected)] if corrected else items


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter, "history", _save_state)

    state = _load_state(adapter)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    context = _alias_context(adapter)
    saved = context[1] is None
    refreshed = _refresh_saved_aliases(cur, context)
    aliases = _key_index(adapter, cur, saved)
    corrected = type_origins(cur)
    config = getattr(adapter, "config", {}) or {}
    unresolved_src: list[Mapping[str, Any]] = []
    changed = 0

    for obj in src:
        if not isinstance(obj, Mapping):
            continue
        if corrected and not event_identities(obj).isdisjoint(corrected):
            continue
        try:
            accepted = _anime_identity(_accepted(obj), context, fresh=True)
            if config.get("_cw_local_history_type_origins") and isinstance(obj.get(TYPE_ORIGINS), list):
                accepted[TYPE_ORIGINS] = list(obj[TYPE_ORIGINS])
            if config.get("_cw_local_plex_history_origins") and isinstance(obj.get(PLEX_ORIGINS), list):
                accepted[PLEX_ORIGINS] = sorted(plex_origins({"item": obj}))
        except Exception:
            unresolved_src.append(obj)
            continue
        key = _history_key(adapter, accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        existing = cur.get(key)
        if not has_history_watch(accepted):
            unresolved_src.append(obj)
            continue
        incoming_aliases = accepted.get(ALIAS_FIELD)
        matches = set().union(*(aliases.get(alias, ()) for alias in _anime_keys(adapter, accepted, key)))
        if existing is None and len(matches) == 1:
            key = matches.pop()
            existing = cur[key]
            if str(existing.get("watched_at") or "") >= str(accepted.get("watched_at") or ""):
                continue
            accepted = {**existing, **{field: accepted[field] for field in ("watched", "watched_at", "_cw_watched_state") if field in accepted}}
        if existing is None or (str(existing.get("watched_at") or "") <= str(accepted.get("watched_at") or "")):
            if isinstance(existing, Mapping):
                accepted = merge_tracker_identity(existing, accepted)
                if existing.get(TYPE_ORIGINS):
                    accepted[TYPE_ORIGINS] = existing[TYPE_ORIGINS]
                origins = plex_origins({"existing": existing, "incoming": accepted})
                if origins:
                    if config.get("_cw_local_plex_history_origins"):
                        accepted = dict(existing)
                        if config.get("_cw_local_history_type_origins"):
                            types = type_origins({"existing": existing, "incoming": obj})
                            if types:
                                accepted[TYPE_ORIGINS] = sorted(types)
                    accepted[PLEX_ORIGINS] = sorted(origins)
            accepted.pop(ALIAS_FIELD, None)
            kept = incoming_aliases or (saved and isinstance(existing, Mapping) and existing.get(ALIAS_FIELD))
            if kept:
                accepted[ALIAS_FIELD] = kept
            cur[key] = _history_minimal(adapter, accepted, key)
            for alias in _anime_keys(adapter, cur[key], key, saved):
                aliases.setdefault(alias, set()).add(key)
            changed += 1

    if changed or refreshed:
        _snapshot_state(adapter, cur, "history", reuse_window=60)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src, "history") if unresolved_src else []
    return changed, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    if _pair_scope() is None:
        return 0, []
    src = list(items or [])
    if not src:
        return 0, []

    _maybe_restore(adapter, "history", _save_state)

    # Preserve every stored viewing, including those of unselected media.
    reader = copy(adapter)
    reader.config = {**getattr(adapter, "config", {}), "_cw_history_rewatches": True}
    state = _load_state(reader)
    cur: dict[str, dict[str, Any]] = dict(state.get("items") or {})
    context = _alias_context(adapter)
    refreshed = _refresh_saved_aliases(cur, context)
    index = _key_index(adapter, cur, context[1] is None)
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
        key = _history_key(adapter, accepted)
        if not key:
            unresolved_src.append(obj)
            continue
        matches = set().union(*(index.get(alias, ()) for alias in _anime_keys(adapter, _anime_identity(accepted, context, fresh=True), key)))
        if _rewatches_enabled(adapter):
            matches.add(key)
        matches = {stored_key for stored_key in matches if stored_key in cur}
        if matches:
            for stored_key in matches:
                del cur[stored_key]
            changed += 1

    if changed or refreshed:
        _snapshot_state(adapter, cur, "history", reuse_window=60)
        _save_state(adapter, cur)

    unresolved = _record_unresolved(adapter, unresolved_src, "history") if unresolved_src else []
    return changed, unresolved

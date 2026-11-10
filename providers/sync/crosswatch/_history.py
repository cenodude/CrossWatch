# /providers/sync/crosswatch/_history.py
# CrossWatch tracker Module for History Management
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import copy
from typing import Any, Iterable, Mapping

from cw_platform.anime_mapping.history_coords import ALIAS_FIELD, alias_context, alias_release_tag, coordinate_tokens, current_aliases
from cw_platform.history_events import has_history_watch, history_epoch_from_item, history_sync_key, minimal_history_item
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
_WATCH_FIELDS = ("watched", "watched_at", "_cw_watched_state")
_AXIS_ORDER = ("tvdb", "tmdb")


def _axis_rank(item: Mapping[str, Any]) -> int:
    ids = item.get("show_ids") or {}
    return next((n for n, axis in enumerate(_AXIS_ORDER) if isinstance(ids, Mapping) and ids.get(axis)), len(_AXIS_ORDER))


def _replaces_numbering(incoming: Mapping[str, Any], stored: Mapping[str, Any]) -> bool:
    return _axis_rank(incoming) < _axis_rank(stored) and not stored.get(TYPE_ORIGINS) and not stored.get(PLEX_ORIGINS)


def _with_watch(item: Mapping[str, Any], source: Mapping[str, Any]) -> dict[str, Any]:
    return {**item, **{field: source[field] for field in _WATCH_FIELDS if field in source}}


def _rewatches_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_history_rewatches"))


def _history_key(adapter: Any, item: Mapping[str, Any], fallback_key: Any = None) -> str:
    return history_sync_key(item, fallback_key, event_mode=_rewatches_enabled(adapter))


def _mapping_tag(adapter: Any) -> str:
    return alias_release_tag(getattr(adapter, "config", None))


def _alias_context(adapter: Any) -> tuple[str, dict[Any, Any] | None]:
    return alias_context(getattr(adapter, "config", None))


def _anime_identity(item: Mapping[str, Any], context: tuple[str, dict[Any, Any] | None], fresh: bool = False) -> dict[str, Any]:
    out = dict(item)
    out.pop(ALIAS_FIELD, None)
    aliases = current_aliases(item, context, fresh)
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
    kept = [item for item in items if event_identities(item).isdisjoint(corrected)] if corrected else items
    return _distinct_episodes(adapter, kept)


def _distinct_episodes(adapter: Any, items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    context = _alias_context(adapter)
    if context[1] is None:
        return items
    slots: dict[str, int] = {}
    out: list[dict[str, Any]] = []
    for item in items:
        aliased = _anime_identity(item, context) if isinstance(item, Mapping) else {}
        keys = _anime_keys(adapter, aliased) if coordinate_tokens(aliased) else set()
        slot = next((slots[k] for k in keys if k in slots), None)
        if slot is None:
            slot = len(out)
            out.append(item)
        else:
            current = out[slot]
            newer = (history_epoch_from_item(item) or 0) > (history_epoch_from_item(current) or 0)
            if _axis_rank(item) < _axis_rank(current):
                out[slot] = item if newer else _with_watch(item, current)
            elif newer:
                out[slot] = _with_watch(current, item) if _axis_rank(current) < _axis_rank(item) else item
        for k in keys:
            slots.setdefault(k, slot)
    return out


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
        matches = {match for match in set().union(*(aliases.get(alias, ()) for alias in _anime_keys(adapter, accepted, key))) if match in cur}
        if existing is None and len(matches) == 1:
            match = matches.pop()
            stored = cur[match]
            newer = str(stored.get("watched_at") or "") < str(accepted.get("watched_at") or "")
            if _replaces_numbering(accepted, stored):
                if not newer:
                    accepted = _with_watch(accepted, stored)
                    key = _history_key(adapter, accepted) or key
                incoming_aliases = incoming_aliases or (saved and stored.get(ALIAS_FIELD))
                del cur[match]
            elif not newer:
                continue
            else:
                key, existing = match, stored
                accepted = _with_watch(existing, accepted)
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

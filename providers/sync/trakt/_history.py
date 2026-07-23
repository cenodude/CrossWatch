# /providers/sync/trakt/_history.py
# TRAKT Module for history sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping

from ._common import (
    key_of,
    ids_for_trakt,
    pick_trakt_kind,
    fetch_last_activities,
    update_watermarks_from_last_activities,
    extract_latest_ts,
    state_file,
    _pair_scope,
    _is_capture_mode,
    _now_iso,
    _last_limit_path,
    _record_limit_error,
    headers_for_adapter,
)
from .._mod_common import request_with_retries
from cw_platform.id_map import minimal as id_minimal, canonical_key
from .._log import log as cw_log

BASE = "https://api.trakt.tv"
URL_HIST_MOV = f"{BASE}/sync/history/movies"
URL_HIST_EPI = f"{BASE}/sync/history/episodes"
URL_ADD = f"{BASE}/sync/history"
URL_REMOVE = f"{BASE}/sync/history/remove"
URL_COLL_ADD = f"{BASE}/sync/collection"
RESOLVE_ENABLE = False
_CACHE_SCHEMA = 2

def _int_or_none(x: Any) -> int | None:
    if x is None:
        return None
    try:
        return int(x)
    except Exception:
        return None


def _cache_path() -> Path:
    return state_file("trakt_history.index.json")


def _bust_index_cache(reason: str) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        p = _cache_path()
        if p.exists():
            p.unlink()
            _dbg("cache_invalidated", cache="index", reason=reason)
    except Exception as e:
        _warn("cache_save_failed", cache="index", op="invalidate", reason=reason, error=str(e))



def _not_found_count(nf: Any) -> int:
    if not isinstance(nf, dict):
        return 0
    c = 0
    for v in nf.values():
        if isinstance(v, list):
            c += len(v)
    return c

_PROVIDER = "TRAKT"
_FEATURE = "history"


def _dbg(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "debug", event, **fields)

def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)

def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)

def _error(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "error", event, **fields)



def _iso8601(v: Any) -> str | None:
    if v is None:
        return None
    s = str(v).strip()
    if not s:
        return None

    epoch: int | None = None
    if s.isdigit() and len(s) >= 13:
        try:
            epoch = int(s) // 1000
        except Exception:
            return None
    elif s.isdigit():
        try:
            epoch = int(s)
        except Exception:
            return None
    else:
        if "T" not in s:
            return None
        try:
            from datetime import datetime
            iso = s
            if iso.endswith("Z"):
                iso = iso.replace("Z", "+00:00")
            else:
                tail = iso[10:]
                if "+" not in tail and "-" not in tail:
                    iso = iso + "+00:00"
            epoch = int(datetime.fromisoformat(iso).timestamp())
        except Exception:
            return None

    if epoch is None:
        return None

    # Trakt is moving watched_at to minute precision (seconds + milliseconds => 00.000Z).
    epoch = (epoch // 60) * 60
    return time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime(epoch))


def _as_epoch(iso: str) -> int | None:
    try:
        from datetime import datetime
        s = iso.replace("Z", "+00:00")
        return int(datetime.fromisoformat(s).timestamp())
    except Exception:
        return None


def _max_iso(a: str | None, b: str | None) -> str | None:
    if not a:
        return b
    if not b:
        return a
    ea = _as_epoch(_iso8601(a) or a) or 0
    eb = _as_epoch(_iso8601(b) or b) or 0
    return b if eb >= ea else a



def _cfg(adapter: Any) -> Any:
    return getattr(adapter, "cfg", None) or getattr(adapter, "config", {})


def _cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    c = _cfg(adapter)
    try:
        if hasattr(c, key):
            v = getattr(c, key)
            return default if v is None else v
    except Exception:
        pass
    if isinstance(c, Mapping):
        v = c.get(key, default)
        return default if v is None else v
    return default


def _cfg_num(adapter: Any, key: str, default: Any, cast: Callable[[Any], Any] = int) -> Any:
    try:
        v = _cfg_get(adapter, key, default)
        return cast(v)
    except Exception:
        return cast(default)


def _chunked_items(seq: list[Mapping[str, Any]], n: int) -> Iterable[list[Mapping[str, Any]]]:
    size = max(1, int(n or 1))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


_NATIVE_ANIME_PROVIDERS = {"SIMKL", "ANILIST"}


def _episodes_extended() -> str | None:
    for env_key in ("CW_PAIR_SRC", "CW_PAIR_DST"):
        prov = str(os.getenv(env_key) or "").strip().upper()
        if prov in _NATIVE_ANIME_PROVIDERS:
            return "full"
    return None


_SRC_SNAPSHOT: dict[str, Any] = {"scope": "", "by_key": {}}
_SIMKL_MAP_CACHE: dict[str, dict[str, Any]] = {}
_SIMKL_MAP_MISS: set[str] = set()
_SHOW_CATALOG_CACHE: dict[str, list[dict[str, Any]]] = {}
_EP_SEARCH_CACHE: dict[tuple[str, str], dict[str, Any] | None] = {}
_SHOW_PATH_MISS: set[str] = set()
_DEST_CLAIMS: dict[str, str] = {}
_TRAKT_EP_ID_KEYS = ("tvdb", "tmdb", "imdb")
_TRAKT_ID_FIELDS = ("trakt", "slug", "tmdb", "imdb", "tvdb")
_MAP_RUN: dict[str, Any] = {"scope": ""}
_ALIAS_STATE: dict[str, Any] = {"scope": "", "items": {}}
_ALIAS_REBUILD: dict[str, Any] = {"scope": "", "rebuilt": 0, "ambiguous": 0}


def _trakt_ids_only(ids: Mapping[str, Any] | None) -> dict[str, Any]:
    return {k: v for k, v in (ids or {}).items() if k in _TRAKT_ID_FIELDS and v not in (None, "")}


def _genuine_episode_ids(item: Mapping[str, Any]) -> dict[str, str]:
    ids = item.get("ids") or {}
    show_ids = item.get("show_ids") or {}
    out: dict[str, str] = {}
    for ns in _TRAKT_EP_ID_KEYS:
        value = str(ids.get(ns) or "").strip()
        if not value:
            continue
        if value == str(show_ids.get(ns) or "").strip():
            continue
        out[ns] = value
    return out


def _is_native_anime(item: Mapping[str, Any]) -> bool:
    if str(item.get("simkl_bucket") or "").strip().lower() == "anime":
        return True
    native = _int_or_none(item.get("_simkl_episode_number"))
    return native is not None and native > 0


def _pair_env(key: str) -> str:
    return str(os.getenv(key) or "").strip().upper()


def _simkl_to_trakt_active() -> bool:
    return _pair_env("CW_PAIR_SRC") == "SIMKL" and _pair_env("CW_PAIR_DST") == "TRAKT"


def _map_scope() -> str:
    run = str(os.getenv("CW_RUN_ID") or "").strip()
    return f"{run or 'no-run'}|{_pair_scope() or 'unscoped'}|{_pair_env('CW_PAIR_SRC')}>{_pair_env('CW_PAIR_DST')}"


def _reset_simkl_maps(scope: str) -> None:
    _SIMKL_MAP_CACHE.clear()
    _SIMKL_MAP_MISS.clear()
    _SHOW_CATALOG_CACHE.clear()
    _EP_SEARCH_CACHE.clear()
    _SHOW_PATH_CACHE.clear()
    _SHOW_PATH_MISS.clear()
    _DEST_CLAIMS.clear()
    _MAP_RUN["scope"] = scope


def _ensure_map_scope() -> None:
    scope = _map_scope()
    if not _MAP_RUN.get("scope") or _MAP_RUN.get("scope") != scope:
        _reset_simkl_maps(scope)


def _title_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return " ".join(part for part in re.split(r"[^a-z0-9]+", text) if part)


def _remember_source_items(by_key: Mapping[str, Mapping[str, Any]]) -> None:
    if not by_key:
        return
    store = _SRC_SNAPSHOT.setdefault("by_key", {})
    for k, it in by_key.items():
        if k:
            store[k] = dict(it)


def _source_item_for_key(key: str, req_index: Mapping[str, Any] | None = None) -> dict[str, Any]:
    rec = ((req_index or {}).get("src_items") or {}).get(key)
    if isinstance(rec, Mapping) and isinstance(rec.get("source"), Mapping):
        return dict(rec["source"])
    stored = (_SRC_SNAPSHOT.get("by_key") or {}).get(key)
    return dict(stored) if isinstance(stored, Mapping) else {}


def prepare_source_snapshot(items: Iterable[Mapping[str, Any]]) -> int:
    if not _simkl_to_trakt_active():
        return 0
    seq = [it for it in (items or []) if isinstance(it, Mapping)]
    episodes = [it for it in seq if str(it.get("type") or "").lower() == "episode"]
    if not episodes:
        return 0
    _ensure_map_scope()
    _alias_load()
    by_key: dict[str, dict[str, Any]] = {}
    genuine = 0
    copied = 0
    native = 0
    for it in episodes:
        try:
            k = str(canonical_key(it) or "")
        except Exception:
            k = ""
        if not k:
            continue
        raw_ids = it.get("ids") or {}
        show_ids = it.get("show_ids") or {}
        if _genuine_episode_ids(it):
            genuine += 1
        elif any(
            str(raw_ids.get(ns) or "").strip()
            and str(raw_ids.get(ns) or "").strip() == str(show_ids.get(ns) or "").strip()
            for ns in _TRAKT_EP_ID_KEYS
        ):
            copied += 1
        if _is_native_anime(it):
            native += 1
        by_key[k] = dict(it)
    _SRC_SNAPSHOT["scope"] = _map_scope()
    _SRC_SNAPSHOT["by_key"] = by_key
    _info(
        "source_snapshot_prepared",
        scope=_SRC_SNAPSHOT["scope"],
        episodes=len(episodes),
        genuine_episode_ids=genuine,
        copied_show_ids=copied,
        native_anime_episodes=native,
        regular_episodes=len(episodes) - native,
    )
    return len(by_key)


def _alias_path() -> Path:
    return state_file("trakt_history.pair_alias.json")


def _alias_scope() -> str:
    return f"{_pair_scope() or 'unscoped'}|{_pair_env('CW_PAIR_SRC')}>{_pair_env('CW_PAIR_DST')}"


def _alias_load() -> dict[str, dict[str, Any]]:
    scope = _alias_scope()
    if _ALIAS_STATE.get("scope") == scope and isinstance(_ALIAS_STATE.get("items"), dict):
        return _ALIAS_STATE["items"]
    items: dict[str, dict[str, Any]] = {}
    try:
        raw = json.loads(_alias_path().read_text("utf-8"))
        if isinstance(raw, Mapping) and str(raw.get("scope") or "") == scope:
            stored = raw.get("items")
            if isinstance(stored, Mapping):
                items = {str(k): dict(v) for k, v in stored.items() if isinstance(v, Mapping)}
    except Exception:
        items = {}
    _ALIAS_STATE["scope"] = scope
    _ALIAS_STATE["items"] = items
    return items


def _alias_save() -> None:
    if _is_capture_mode():
        return
    try:
        path = _alias_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(
            json.dumps({"scope": _ALIAS_STATE.get("scope") or _alias_scope(), "items": _ALIAS_STATE.get("items") or {}}, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        tmp.replace(path)
    except Exception as e:
        _warn("alias_save_failed", error=str(e))


def _alias_record(source_event_key: str, destination: Mapping[str, Any], *, basis: str = "") -> bool:
    if not source_event_key or not isinstance(destination, Mapping):
        return False
    dest_key = str(destination.get("key") or "")
    if not dest_key or dest_key == source_event_key:
        return False
    items = _alias_load()
    existing = items.get(source_event_key)
    if isinstance(existing, Mapping):
        prev = str(existing.get("destination_event_key") or "")
        if prev and prev != dest_key:
            _warn("alias_conflict", source=source_event_key, existing=prev, incoming=dest_key)
            return False
    raw_item = destination.get("item")
    item: Mapping[str, Any] = raw_item if isinstance(raw_item, Mapping) else {}
    raw_ids = item.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    raw_show_ids = item.get("show_ids")
    show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
    rec: dict[str, Any] = {"destination_event_key": dest_key, "destination_key": str(destination.get("plain_key") or dest_key.split("@", 1)[0])}
    ep_id = str(ids.get("trakt") or "").strip()
    if ep_id:
        rec["destination_episode_id"] = ep_id
    show_id = str(show_ids.get("trakt") or show_ids.get("tmdb") or show_ids.get("tvdb") or "").strip()
    if show_id:
        rec["destination_show_id"] = show_id
    for field in ("season", "episode"):
        value = _int_or_none(item.get(field))
        if value is not None:
            rec[field] = value
    watched = _iso8601(item.get("watched_at"))
    if watched:
        rec["watched_at"] = watched
    hid = _int_or_none(destination.get("history_id"))
    if hid is not None:
        rec["history_id"] = hid
    elif isinstance(existing, Mapping) and _int_or_none(existing.get("history_id")) is not None:
        rec["history_id"] = _int_or_none(existing.get("history_id"))
    resolution = basis or str(destination.get("basis") or "")
    if resolution:
        rec["basis"] = resolution
    items[source_event_key] = rec
    return True


def _alias_forget(source_event_keys: Iterable[str]) -> None:
    items = _alias_load()
    changed = False
    for k in source_event_keys or []:
        if k in items:
            items.pop(k, None)
            changed = True
    if changed:
        _alias_save()


def _alias_forget_by_history_ids(history_ids: Iterable[int]) -> int:
    wanted = {h for h in (history_ids or []) if h is not None}
    if not wanted:
        return 0
    items = _alias_load()
    stale = [
        key for key, rec in items.items()
        if isinstance(rec, Mapping) and _int_or_none(rec.get("history_id")) in wanted
    ]
    for key in stale:
        items.pop(key, None)
    if stale:
        _alias_save()
        _dbg("alias_forget_by_history_id", removed=len(stale), history_ids=len(wanted))
    return len(stale)


def _alias_forget_by_events(events: Iterable[tuple[str, str]]) -> int:
    wanted: dict[str, set[str]] = {}
    for key, watched in events or []:
        base = str(key or "").split("@", 1)[0]
        if not base:
            continue
        slot = wanted.setdefault(base, set())
        if watched:
            slot.add(watched)
    if not wanted:
        return 0
    items = _alias_load()
    stale: list[str] = []
    for source_event_key, rec in items.items():
        if not isinstance(rec, Mapping):
            continue
        bases = {
            str(source_event_key).split("@", 1)[0],
            str(rec.get("destination_key") or "").split("@", 1)[0],
            str(rec.get("destination_event_key") or "").split("@", 1)[0],
        }
        hit = next((b for b in bases if b and b in wanted), None)
        if hit is None:
            continue
        rec_watched = _iso8601(rec.get("watched_at"))
        watched_set = wanted[hit]
        if watched_set and rec_watched and rec_watched not in watched_set:
            continue
        stale.append(source_event_key)
    for key in stale:
        items.pop(key, None)
    if stale:
        _alias_save()
        _dbg("alias_forget_by_key", removed=len(stale), keys=len(wanted))
    return len(stale)


def _alias_reconcile(index: Mapping[str, Any]) -> int:
    items = _alias_load()
    if not items or not index:
        return 0
    enriched = 0
    for rec in items.values():
        if not isinstance(rec, dict) or _int_or_none(rec.get("history_id")) is not None:
            continue
        dest = index.get(str(rec.get("destination_event_key") or ""))
        if not isinstance(dest, Mapping):
            dest = index.get(str(rec.get("destination_key") or ""))
        if not isinstance(dest, Mapping):
            continue
        hid = _int_or_none(dest.get("_trakt_history_id") or dest.get("history_id"))
        if hid is None:
            continue
        rec["history_id"] = hid
        enriched += 1
    if enriched:
        _alias_save()
    return enriched


def _source_event_key(item: Mapping[str, Any]) -> str:
    try:
        base = str(canonical_key(item) or "")
    except Exception:
        base = ""
    if not base:
        return ""
    watched = _iso8601(item.get("watched_at"))
    ts = _as_epoch(watched) if watched else None
    return f"{base}@{ts}" if ts is not None else base


def _destination_event_key(item: Mapping[str, Any]) -> str:
    return _source_event_key(item)


def _sanitize_destination(item: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {"type": str(item.get("type") or "").lower() or "episode"}
    ids = _trakt_ids_only(item.get("ids") or {})
    if ids:
        out["ids"] = ids
    show_ids = _trakt_ids_only(item.get("show_ids") or {})
    if show_ids:
        out["show_ids"] = show_ids
    for field in ("season", "episode", "title", "year", "series_title", "watched_at"):
        value = item.get(field)
        if value not in (None, ""):
            out[field] = value
    return out


def _show_tokens(item: Mapping[str, Any]) -> set[tuple[str, str]]:
    out: set[tuple[str, str]] = set()
    for field, value in (item.get("show_ids") or {}).items():
        token = str(value or "").strip()
        if token:
            out.add((str(field).lower(), token))
    return out


def _rebuild_aliases_from_index(index: Mapping[str, Any], adapter: Any = None) -> tuple[int, int]:
    source = _SRC_SNAPSHOT.get("by_key") or {}
    if not source or not index:
        return 0, 0

    aliases = _alias_load()
    claimed_dest = {str((rec or {}).get("destination_event_key") or "") for rec in aliases.values()}

    dest_bases: set[str] = set()
    dest_items: dict[str, Mapping[str, Any]] = {}
    by_episode_watched: dict[tuple[str, int], list[str]] = {}
    by_coords_watched: dict[tuple[str, str, int, int, int], list[str]] = {}
    for dk, dv in index.items():
        if not isinstance(dv, Mapping):
            continue
        key = str(dk)
        dest_bases.add(key.split("@", 1)[0])
        dest_items[key] = dv
        watched = _as_epoch(_iso8601(dv.get("watched_at")) or "")
        if watched is None:
            continue
        ep_trakt = str((dv.get("ids") or {}).get("trakt") or "").strip()
        if ep_trakt:
            by_episode_watched.setdefault((ep_trakt, watched), []).append(key)
        season = _int_or_none(dv.get("season"))
        episode = _int_or_none(dv.get("episode"))
        if season is not None and episode is not None:
            for token in _show_tokens(dv):
                by_coords_watched.setdefault((*token, season, episode, watched), []).append(key)

    pending: list[Mapping[str, Any]] = []
    for src_item in source.values():
        src_event_key = _source_event_key(src_item)
        if not src_event_key or src_event_key in aliases:
            continue
        if src_event_key.split("@", 1)[0] in dest_bases:
            continue
        if _as_epoch(_iso8601(src_item.get("watched_at")) or "") is None:
            continue
        pending.append(src_item)

    if not pending:
        _ALIAS_REBUILD["scope"] = _alias_scope()
        _ALIAS_REBUILD["rebuilt"] = 0
        _ALIAS_REBUILD["ambiguous"] = 0
        return 0, 0

    if adapter is None:
        _ALIAS_REBUILD["scope"] = _alias_scope()
        _ALIAS_REBUILD["rebuilt"] = 0
        _ALIAS_REBUILD["ambiguous"] = len(pending)
        _warn("alias_rebuild_skipped", reason="no_adapter", pending=len(pending))
        return 0, len(pending)

    resolved_items, resolve_unresolved = _apply_simkl_resolution(adapter, pending)

    proposals: dict[str, str] = {}
    ambiguous = len(resolve_unresolved)
    for outgoing in resolved_items:
        src_key = str(outgoing.get("_cw_source_key") or "")
        src_item = source.get(src_key)
        if not isinstance(src_item, Mapping):
            continue
        src_event_key = _source_event_key(src_item)
        watched = _as_epoch(_iso8601(src_item.get("watched_at")) or "")
        if not src_event_key or watched is None:
            continue

        candidates: list[str] = []
        ep_trakt = str((outgoing.get("ids") or {}).get("trakt") or "").strip()
        if ep_trakt:
            candidates = list(by_episode_watched.get((ep_trakt, watched)) or [])
        if not candidates:
            season = _int_or_none(outgoing.get("season"))
            episode = _int_or_none(outgoing.get("episode"))
            if season is not None and episode is not None:
                seen: list[str] = []
                for token in _show_tokens(outgoing):
                    for dk in by_coords_watched.get((*token, season, episode, watched)) or []:
                        if dk not in seen:
                            seen.append(dk)
                candidates = seen

        candidates = [dk for dk in candidates if dk not in claimed_dest]
        if not candidates:
            continue
        if len(candidates) > 1:
            ambiguous += 1
            _dbg("alias_rebuild_ambiguous", source=src_event_key, candidates=len(candidates))
            continue
        proposals[src_event_key] = candidates[0]

    dest_claims: dict[str, list[str]] = {}
    for src_event_key, dest_key in proposals.items():
        dest_claims.setdefault(dest_key, []).append(src_event_key)

    rebuilt = 0
    for dest_key, src_keys in dest_claims.items():
        if len(src_keys) != 1:
            ambiguous += len(src_keys)
            _dbg("alias_rebuild_ambiguous", destination=dest_key, sources=len(src_keys))
            continue
        dest_item = dest_items.get(dest_key) or {}
        dest_plain = str(canonical_key(dest_item) or "") or dest_key.split("@", 1)[0]
        payload = {
            "key": dest_key,
            "plain_key": dest_plain,
            "item": _sanitize_destination(dest_item),
            "history_id": dest_item.get("_trakt_history_id") or dest_item.get("history_id"),
        }
        if _alias_record(src_keys[0], payload, basis="rebuilt_from_resolver"):
            rebuilt += 1

    if rebuilt:
        _alias_save()
    _ALIAS_REBUILD["scope"] = _alias_scope()
    _ALIAS_REBUILD["rebuilt"] = rebuilt
    _ALIAS_REBUILD["ambiguous"] = ambiguous
    if rebuilt or ambiguous:
        _info(
            "alias_rebuild",
            rebuilt=rebuilt,
            ambiguous=ambiguous,
            pending=len(pending),
            resolved=len(resolved_items),
            destination=len(index),
        )
    return rebuilt, ambiguous


def _alias_rebuild_incomplete() -> bool:
    return _ALIAS_REBUILD.get("scope") == _alias_scope() and int(_ALIAS_REBUILD.get("ambiguous") or 0) > 0


def destination_comparison_view(index: Mapping[str, Any], adapter: Any = None) -> dict[str, Any]:
    aliases = _alias_load()
    if _simkl_to_trakt_active():
        _rebuild_aliases_from_index(index, adapter)
        aliases = _alias_load()
    if not aliases:
        return dict(index)
    enriched = _alias_reconcile(index)
    exact_map: dict[str, str] = {}
    base_map: dict[str, str] = {}
    for src_key, rec in aliases.items():
        dk = str((rec or {}).get("destination_event_key") or "")
        if not dk:
            continue
        if "@" in dk:
            exact_map[dk] = src_key
        dest_base = str((rec or {}).get("destination_key") or dk.split("@", 1)[0])
        src_base = str(src_key).split("@", 1)[0]
        if dest_base and src_base and dest_base != src_base:
            base_map.setdefault(dest_base, src_base)
    out: dict[str, Any] = {}
    remapped = 0
    for k, v in (index or {}).items():
        kk = str(k)
        src = exact_map.get(kk) if "@" in kk else None
        if src is None:
            src = base_map.get(kk.split("@", 1)[0])
        if src:
            out[src] = v
            remapped += 1
        else:
            out[kk] = v
    if remapped or enriched:
        _dbg("alias_comparison_view", remapped=remapped, enriched=enriched, aliases=len(aliases), index=len(index or {}))
    return out


def _search_trakt_episode(adapter: Any, service: str, value: str, *, timeout: float, retries: int) -> dict[str, Any] | None:
    cache_key = (str(service), str(value))
    if cache_key in _EP_SEARCH_CACHE:
        return _EP_SEARCH_CACHE[cache_key]
    out: dict[str, Any] | None = None
    try:
        r = request_with_retries(
            adapter.client.session,
            "GET",
            f"{BASE}/search/{service}/{value}",
            headers=headers_for_adapter(adapter),
            params={"type": "episode"},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code == 200:
            rows = r.json() or []
            hits = [row for row in rows if isinstance(row, Mapping) and isinstance(row.get("episode"), Mapping)]
            if len(hits) == 1:
                hit = hits[0]
                episode = dict(hit.get("episode") or {})
                show = dict(hit.get("show") or {})
                out = {
                    "ids": {k: str(v) for k, v in (episode.get("ids") or {}).items() if v not in (None, "")},
                    "show_ids": {k: str(v) for k, v in (show.get("ids") or {}).items() if v not in (None, "")},
                    "season": _int_or_none(episode.get("season")),
                    "episode": _int_or_none(episode.get("number")),
                    "title": episode.get("title"),
                }
    except Exception as e:
        _dbg("simkl_trakt_lookup_failed", service=service, error=str(e))
        out = None
    _EP_SEARCH_CACHE[cache_key] = out
    return out


def _resolve_trakt_show_path(adapter: Any, show_ids: Mapping[str, Any], *, timeout: float, retries: int) -> str | None:
    direct = _pick_show_path_id(show_ids or {})
    if direct:
        return direct
    skey = json.dumps({k: str(v) for k, v in sorted((show_ids or {}).items()) if v not in (None, "")}, sort_keys=True)
    if skey in _SHOW_PATH_CACHE:
        return _SHOW_PATH_CACHE[skey]
    if skey in _SHOW_PATH_MISS:
        return None
    found: str | None = None
    for service in ("tmdb", "imdb", "tvdb"):
        value = str((show_ids or {}).get(service) or "").strip()
        if not value:
            continue
        try:
            r = request_with_retries(
                adapter.client.session,
                "GET",
                f"{BASE}/search/{service}/{value}",
                headers=headers_for_adapter(adapter),
                params={"type": "show"},
                timeout=timeout,
                max_retries=retries,
            )
        except Exception:
            continue
        if getattr(r, "status_code", 0) != 200:
            continue
        for hit in (r.json() or []):
            if not isinstance(hit, Mapping):
                continue
            pid = _pick_show_path_id((hit.get("show") or {}).get("ids") or {})
            if pid:
                found = pid
                break
        if found:
            break
    if found:
        _SHOW_PATH_CACHE[skey] = found
    else:
        _SHOW_PATH_MISS.add(skey)
    return found


def _trakt_show_catalog(adapter: Any, show_ids: Mapping[str, Any], *, timeout: float, retries: int) -> list[dict[str, Any]]:
    path_id = _resolve_trakt_show_path(adapter, show_ids, timeout=timeout, retries=retries)
    if not path_id:
        return []
    if path_id in _SHOW_CATALOG_CACHE:
        return _SHOW_CATALOG_CACHE[path_id]
    rows: list[dict[str, Any]] = []
    try:
        r = request_with_retries(
            adapter.client.session,
            "GET",
            f"{BASE}/shows/{path_id}/seasons",
            headers=headers_for_adapter(adapter),
            params={"extended": "episodes,full"},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code == 200:
            for season in (r.json() or []):
                if not isinstance(season, Mapping):
                    continue
                s_num = _int_or_none(season.get("number"))
                for ep in (season.get("episodes") or []):
                    if not isinstance(ep, Mapping):
                        continue
                    e_num = _int_or_none(ep.get("number"))
                    if s_num is None or e_num is None:
                        continue
                    rows.append(
                        {
                            "season": s_num,
                            "episode": e_num,
                            "number_abs": _int_or_none(ep.get("number_abs")),
                            "title": ep.get("title"),
                            "ids": {k: str(v) for k, v in (ep.get("ids") or {}).items() if v not in (None, "")},
                        }
                    )
    except Exception as e:
        _dbg("simkl_trakt_catalog_failed", show=str(path_id), error=str(e))
        rows = []
    _SHOW_CATALOG_CACHE[path_id] = rows
    return rows


def _map_cache_key(item: Mapping[str, Any], show_ids: Mapping[str, Any]) -> str:
    return json.dumps(
        {
            "record": str((item.get("show_ids") or {}).get("simkl") or ""),
            "native": _int_or_none(item.get("_simkl_episode_number")),
            "dst": "TRAKT",
            "show": str(show_ids.get("trakt") or show_ids.get("tmdb") or show_ids.get("tvdb") or ""),
            "s": _int_or_none(item.get("season")),
            "e": _int_or_none(item.get("episode")),
        },
        sort_keys=True,
    )


def _catalog_absolute_rows(catalog: list[dict[str, Any]]) -> list[tuple[int, dict[str, Any]]]:
    out: list[tuple[int, dict[str, Any]]] = []
    for row in catalog:
        if _int_or_none(row.get("season")) == 0:
            continue
        n = _int_or_none(row.get("number_abs"))
        if n is None or n <= 0:
            continue
        out.append((n, row))
    return out


def _catalog_is_continuous(catalog: list[dict[str, Any]]) -> bool:
    rows = _catalog_absolute_rows(catalog)
    if not rows:
        return False
    numbers = [n for n, _ in rows]
    return len(set(numbers)) == len(numbers)


def _catalog_index(catalog: list[dict[str, Any]]) -> dict[str, Any]:
    by_trakt: dict[str, dict[str, Any]] = {}
    by_ext: dict[tuple[str, str], list[dict[str, Any]]] = {}
    by_abs: dict[int, list[dict[str, Any]]] = {}
    by_title: dict[str, list[dict[str, Any]]] = {}
    for row in catalog:
        ids = row.get("ids") or {}
        tid = str(ids.get("trakt") or "").strip()
        if tid:
            by_trakt[tid] = row
        for ns in _TRAKT_EP_ID_KEYS:
            value = str(ids.get(ns) or "").strip()
            if value:
                by_ext.setdefault((ns, value), []).append(row)
        tkey = _title_key(row.get("title"))
        if tkey:
            by_title.setdefault(tkey, []).append(row)
    for abs_no, row in _catalog_absolute_rows(catalog):
        by_abs.setdefault(abs_no, []).append(row)
    return {
        "by_trakt": by_trakt,
        "by_ext": by_ext,
        "by_abs": by_abs,
        "by_title": by_title,
        "continuous": _catalog_is_continuous(catalog),
    }


def _resolve_from_catalog(item: Mapping[str, Any], index: Mapping[str, Any], show_ids: Mapping[str, Any]) -> tuple[dict[str, Any] | None, str]:
    def _built(row: Mapping[str, Any], basis: str) -> dict[str, Any]:
        return {
            "show_ids": dict(show_ids),
            "ids": _trakt_ids_only(row.get("ids") or {}),
            "season": row.get("season"),
            "episode": row.get("episode"),
            "basis": basis,
        }

    existing = str((item.get("ids") or {}).get("trakt") or "").strip()
    if existing:
        row = (index.get("by_trakt") or {}).get(existing)
        if row:
            return _built(row, "catalogue_exact_id"), "catalogue_exact_id"

    for ns, value in _genuine_episode_ids(item).items():
        rows = (index.get("by_ext") or {}).get((ns, value)) or []
        if len(rows) == 1:
            return _built(rows[0], "catalogue_exact_id"), "catalogue_exact_id"

    explicit_abs = _int_or_none(item.get("_trakt_number_abs"))
    native = _int_or_none(item.get("_simkl_episode_number"))
    abs_no: int | None = None
    if explicit_abs and explicit_abs > 0:
        abs_no = explicit_abs
    elif native and native > 0 and index.get("continuous"):
        abs_no = native
    if abs_no:
        rows = (index.get("by_abs") or {}).get(abs_no) or []
        if len(rows) == 1:
            return _built(rows[0], "catalogue_absolute"), "catalogue_absolute"
        if len(rows) > 1:
            return None, "trakt_absolute_number_unresolved"

    tkey = _title_key(item.get("title"))
    if tkey:
        rows = (index.get("by_title") or {}).get(tkey) or []
        if len(rows) == 1:
            return _built(rows[0], "unique_title"), "unique_title"
        if len(rows) > 1:
            return None, "trakt_episode_title_ambiguous"

    if _int_or_none(item.get("season")) == 0:
        return None, "trakt_special_unmapped"
    if explicit_abs or native:
        return None, "trakt_absolute_number_unresolved"
    return None, "trakt_episode_id_unresolved"


def _destination_token(resolved: Mapping[str, Any]) -> str:
    ids = resolved.get("ids") or {}
    if ids.get("trakt"):
        return f"trakt:{ids['trakt']}"
    show = resolved.get("show_ids") or {}
    show_token = show.get("trakt") or show.get("tmdb") or show.get("tvdb") or show.get("imdb") or ""
    return f"{show_token}|s{resolved.get('season')}e{resolved.get('episode')}"


def _apply_simkl_resolution(adapter: Any, items: list[Mapping[str, Any]]) -> tuple[list[Mapping[str, Any]], list[dict[str, Any]]]:
    if not _simkl_to_trakt_active():
        return list(items), []
    _ensure_map_scope()
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))

    native_items: list[Mapping[str, Any]] = []
    passthrough: list[Mapping[str, Any]] = []
    for it in items:
        if isinstance(it, Mapping) and str(it.get("type") or "").lower() == "episode" and _is_native_anime(it):
            native_items.append(it)
        else:
            passthrough.append(it)

    plan = {
        "regular_direct": len(passthrough),
        "native_anime": len(native_items),
        "anime_groups": 0,
        "catalog_requests": 0,
        "catalog_cache_hits": 0,
    }
    if not native_items:
        _info("simkl_trakt_resolution_plan", **plan)
        return list(items), []

    groups: dict[str, list[Mapping[str, Any]]] = {}
    group_show_ids: dict[str, dict[str, Any]] = {}
    for it in native_items:
        show_ids = dict(it.get("show_ids") or {})
        gkey = json.dumps(
            {
                "record": str(show_ids.get("simkl") or ""),
                "show": {k: str(v) for k, v in sorted(show_ids.items()) if k in _TRAKT_ID_FIELDS and v not in (None, "")},
            },
            sort_keys=True,
        )
        groups.setdefault(gkey, []).append(it)
        group_show_ids.setdefault(gkey, show_ids)
    plan["anime_groups"] = len(groups)

    out: list[Mapping[str, Any]] = list(passthrough)
    unresolved: list[dict[str, Any]] = []
    stats = {"resolved": 0, "unresolved": 0, "collisions": 0}
    basis_counts: dict[str, int] = {}

    for gkey, grouped in groups.items():
        show_ids = group_show_ids.get(gkey) or {}
        trakt_show_ids = _trakt_ids_only(show_ids)
        path_id = _resolve_trakt_show_path(adapter, trakt_show_ids, timeout=timeout, retries=retries) if trakt_show_ids else None
        catalog: list[dict[str, Any]] = []
        if path_id:
            if path_id in _SHOW_CATALOG_CACHE:
                plan["catalog_cache_hits"] += 1
            else:
                plan["catalog_requests"] += 1
            catalog = _trakt_show_catalog(adapter, trakt_show_ids, timeout=timeout, retries=retries)
        index = _catalog_index(catalog) if catalog else {}

        for it in grouped:
            src_key = str(canonical_key(it) or "")
            if not index:
                stats["unresolved"] += 1
                basis_counts["trakt_show_unresolved"] = basis_counts.get("trakt_show_unresolved", 0) + 1
                unresolved.append({"item": id_minimal(it), "hint": "trakt_show_unresolved", "key": src_key})
                continue
            resolved, basis = _resolve_from_catalog(it, index, trakt_show_ids)
            if resolved is None:
                stats["unresolved"] += 1
                basis_counts[basis] = basis_counts.get(basis, 0) + 1
                unresolved.append({"item": id_minimal(it), "hint": basis, "key": src_key})
                continue
            watched = _iso8601(it.get("watched_at") or it.get("watchedAt")) or ""
            token = f"{_destination_token(resolved)}@{watched}"
            owner = _DEST_CLAIMS.get(token)
            if owner and owner != src_key:
                stats["collisions"] += 1
                basis_counts["trakt_destination_collision"] = basis_counts.get("trakt_destination_collision", 0) + 1
                unresolved.append({"item": id_minimal(it), "hint": "trakt_destination_collision", "key": src_key})
                continue
            _DEST_CLAIMS[token] = src_key
            stats["resolved"] += 1
            basis_counts[basis] = basis_counts.get(basis, 0) + 1
            outgoing = dict(it)
            outgoing["show_ids"] = _trakt_ids_only(resolved.get("show_ids") or show_ids)
            outgoing["ids"] = _trakt_ids_only(resolved.get("ids") or {})
            outgoing["season"] = resolved.get("season")
            outgoing["episode"] = resolved.get("episode")
            outgoing["_cw_source_key"] = src_key
            outgoing["_cw_resolution_basis"] = basis
            outgoing.pop("_simkl_episode_number", None)
            outgoing.pop("simkl_bucket", None)
            out.append(outgoing)

    _info("simkl_trakt_resolution_plan", **plan)
    _info(
        "simkl_trakt_resolve_summary",
        checked=len(native_items),
        resolved=stats["resolved"],
        unresolved=stats["unresolved"],
        collisions=stats["collisions"],
        **{k: v for k, v in basis_counts.items()},
    )
    return out, unresolved

def _history_number_fallback_enabled(adapter: Any) -> bool:
    return True if not RESOLVE_ENABLE else bool(_cfg_get(adapter, "history_number_fallback", False))


def _history_collection_enabled(adapter: Any) -> bool:
    return bool(_cfg_get(adapter, "history_collection", False))


def _history_collection_types(adapter: Any) -> set[str]:
    raw = _cfg_get(adapter, "history_collection_types", None)
    allowed = {"movies", "shows"}
    vals: list[str] = []
    if isinstance(raw, str):
        vals = [x.strip().lower() for x in raw.split(",") if x and x.strip()]
    elif isinstance(raw, list):
        vals = [str(x).strip().lower() for x in raw if str(x).strip()]
    out = {x for x in vals if x in allowed}
    if _history_collection_enabled(adapter) and not out:
        out = {"movies"}
    return out



def _load_cache_doc() -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {}
    try:
        p = _cache_path()
        if not p.exists():
            return {}
        doc = json.loads(p.read_text("utf-8") or "{}")
        if not isinstance(doc, dict) or int(doc.get("schema") or 0) != _CACHE_SCHEMA:
            return {}
        return doc
    except Exception:
        return {}


def _save_cache_doc(items: Mapping[str, Any], watched_at: str | None, validated_at: str | None = None) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        cache_path = _cache_path()
        cache_path.parent.mkdir(parents=True, exist_ok=True)
        doc = {
            "schema": _CACHE_SCHEMA,
            "generated_at": _now_iso(),
            "items": dict(items),
            "wm": {"watched_at": watched_at or ""},
            "validated_at": _now_iso() if validated_at is None else str(validated_at),
        }
        tmp = cache_path.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, cache_path)
    except Exception as e:
        _warn("cache_save_failed", cache="index", path=str(_cache_path()), error=str(e))


def _cache_merge_from_source_items(adapter: Any, items: Iterable[Mapping[str, Any]]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        doc = _load_cache_doc()
        cache_items: dict[str, dict[str, Any]] = dict(doc.get("items") or {})
        wm_prev = str((doc.get("wm") or {}).get("watched_at") or "").strip() or None

        added = 0
        wm = wm_prev
        for it in items or []:
            if not isinstance(it, Mapping):
                continue
            m = id_minimal(it)
            if not isinstance(m, dict):
                continue
            w = _iso8601(m.get("watched_at") or it.get("watched_at"))
            ts = _as_epoch(w) if w else None
            if ts is None:
                continue

            typ = str(m.get("type") or "").lower()
            if typ == "episode" and isinstance(m.get("show_ids"), Mapping) and m.get("season") is not None and m.get("episode") is not None:
                base_key = canonical_key(
                    id_minimal(
                        {
                            "type": "episode",
                            "show_ids": dict(m.get("show_ids") or {}),
                            "season": m.get("season"),
                            "episode": m.get("episode"),
                        }
                    )
                )
            else:
                base_key = canonical_key(m)

            if not base_key:
                continue

            ek = f"{base_key}@{int(ts)}"
            if ek not in cache_items:
                out = dict(m)
                out["watched"] = True
                out["watched_at"] = w
                cache_items[ek] = out
                added += 1

            wm = _max_iso(wm, w)

        wm = _max_iso(wm, _now_iso())

        if added:
            _dbg("cache_merged", cache="index", count=added, cache_count=len(cache_items))

        _save_cache_doc(cache_items, wm or wm_prev, validated_at="")
    except Exception as e:
        _warn("cache_save_failed", cache="index", op="merge", error=str(e))


def _cache_remove_source_items(adapter: Any, items: Iterable[Mapping[str, Any]]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        doc = _load_cache_doc()
        cache_items: dict[str, dict[str, Any]] = dict(doc.get("items") or {})
        wm_prev = str((doc.get("wm") or {}).get("watched_at") or "").strip() or None
        if not cache_items:
            return

        base_keys: set[str] = set()
        raw_ids: set[str] = set()
        for it in items or []:
            if not isinstance(it, Mapping):
                continue
            m = id_minimal(it)
            if not isinstance(m, dict):
                continue
            k = str(key_of(m) or "").strip()
            if k:
                base_keys.add(k)
            hid = str(it.get("_trakt_history_id") or it.get("history_id") or "").strip()
            if hid:
                raw_ids.add(hid)

        if not base_keys and not raw_ids:
            return

        removed = 0
        for ek in list(cache_items.keys()):
            item = cache_items.get(ek) or {}
            base = str(ek).split("@", 1)[0]
            hid = str(item.get("_trakt_history_id") or item.get("history_id") or "").strip()
            if base in base_keys or (hid and hid in raw_ids):
                cache_items.pop(ek, None)
                removed += 1

        if removed <= 0:
            return

        _save_cache_doc(cache_items, wm_prev, validated_at="")
    except Exception as e:
        _warn("cache_save_failed", cache="index", op="remove", error=str(e))



def _hdr_int(headers: Mapping[str, Any], name: str) -> int | None:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == name.lower():
                return int(str(v).strip())
    except Exception:
        return None
    return None


def _preflight_total(
    sess: Any,
    headers: Mapping[str, Any],
    url: str,
    *,
    per_page: int,
    timeout: float,
    max_retries: int,
    max_pages: int | None,
) -> int | None:
    try:
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            params={"page": 1, "limit": per_page},
            timeout=timeout,
            max_retries=max_retries,
        )
        if r.status_code != 200:
            return None
        item_count = _hdr_int(r.headers, "X-Pagination-Item-Count")
        if item_count is None:
            page_count = _hdr_int(r.headers, "X-Pagination-Page-Count")
            limit_hdr = _hdr_int(r.headers, "X-Pagination-Limit") or per_page
            if page_count is not None and limit_hdr:
                item_count = int(page_count) * int(limit_hdr)
        if item_count is None:
            return None
        if max_pages and max_pages > 0:
            item_count = min(item_count, int(max_pages) * int(per_page))
        return int(item_count)
    except Exception:
        return None


def _history_params(*, page: int, limit: int, start_at: str | None = None, end_at: str | None = None, extended: str | None = None) -> dict[str, Any]:
    params: dict[str, Any] = {"page": int(page), "limit": int(limit)}
    if start_at:
        params["start_at"] = str(start_at)
    if end_at:
        params["end_at"] = str(end_at)
    if extended:
        params["extended"] = str(extended)
    return params

def _fetch_history(
    sess: Any,
    headers: Mapping[str, Any],
    url: str,
    *,
    per_page: int,
    max_pages: int,
    timeout: float,
    max_retries: int,
    start_at: str | None = None,
    end_at: str | None = None,
    extended: str | None = None,
    bump: Callable[[int], None] | None = None,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    total_pages: int | None = None
    while True:
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            params=_history_params(page=page, limit=per_page, start_at=start_at, end_at=end_at, extended=extended),
            timeout=timeout,
            max_retries=max_retries,
        )
        if r.status_code != 200:
            _warn("http_failed", op="index", url=url, page=page, status=r.status_code)
            break
        if total_pages is None:
            pc = _hdr_int(r.headers, "X-Pagination-Page-Count")
            if pc is not None:
                total_pages = pc
        rows = r.json() or []
        if not rows:
            break
        added = 0
        for row in rows:
            hid = row.get("id")
            w = row.get("watched_at")
            if not w:
                continue
            typ = (row.get("type") or "").lower()
            if typ == "movie" and isinstance(row.get("movie"), dict):
                mv = row["movie"]
                m = id_minimal(
                    {"type": "movie", "ids": mv.get("ids") or {}, "title": mv.get("title"), "year": mv.get("year")}
                )
                m["watched_at"] = w
                if hid is not None:
                    m["_trakt_history_id"] = str(hid)
                out.append(m)
                added += 1
            elif typ == "episode" and isinstance(row.get("episode"), dict):
                ep = row["episode"]
                show = row.get("show") or {}
                m = id_minimal(
                    {
                        "type": "episode",
                        "ids": ep.get("ids") or {},
                        "show_ids": show.get("ids") or {},
                        "season": ep.get("season"),
                        "episode": ep.get("number"),
                        "series_title": show.get("title"),
                        "title": ep.get("title"),
                    }
                )
                m["watched_at"] = w
                if hid is not None:
                    m["_trakt_history_id"] = str(hid)
                abs_no = _int_or_none(ep.get("number_abs"))
                if abs_no is not None and abs_no > 0:
                    m["_trakt_number_abs"] = abs_no
                out.append(m)
                added += 1
        if bump and added:
            try:
                bump(added)
            except Exception:
                pass
        page += 1
        if total_pages is not None and page > total_pages:
            break
        if total_pages is None and len(rows) < per_page:
            break
        if max_pages and page > max_pages:
            _warn("index_reconcile", reason="safety_cap_hit", strategy="paged_fetch", max_pages=max_pages)
            break
    return out


def build_index(adapter: Any, *, per_page: int = 100, max_pages: int = 100000) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("history") if callable(prog_mk) else None
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))
    cfg_per_page = int(_cfg_num(adapter, "history_per_page", per_page, int))
    cfg_per_page = max(1, min(100, cfg_per_page))
    cfg_max_pages = int(_cfg_num(adapter, "history_max_pages", max_pages, int))
    if cfg_max_pages <= 0:
        cfg_max_pages = max_pages

    epi_extended = _episodes_extended()

    doc = _load_cache_doc()
    cached_items: dict[str, dict[str, Any]] = dict(doc.get("items") or {})
    cached_wm = str((doc.get("wm") or {}).get("watched_at") or "").strip()

    acts = fetch_last_activities(sess, headers, timeout=timeout, max_retries=retries)
    update_watermarks_from_last_activities(acts)
    remote_wm = extract_latest_ts(acts or {}, (("movies", "watched_at"), ("episodes", "watched_at"))) if acts else None

    if cached_items and remote_wm and cached_wm:
        a = _as_epoch(_iso8601(remote_wm) or "")
        b = _as_epoch(_iso8601(cached_wm) or "")
        if a is not None and b is not None and a <= b:
           
            _CACHE_VALIDATE_INTERVAL_SEC = 0  # always re-check API counts on cache hits
            validated_at_str = str((doc.get("validated_at") or "")).strip()
            validated_epoch = _as_epoch(validated_at_str) if validated_at_str else None
            due_for_validation = (validated_epoch is None) or ((time.time() - validated_epoch) >= _CACHE_VALIDATE_INTERVAL_SEC)
            if due_for_validation:
                api_mov = _preflight_total(sess, headers, URL_HIST_MOV, per_page=cfg_per_page, timeout=timeout, max_retries=retries, max_pages=cfg_max_pages)
                api_epi = _preflight_total(sess, headers, URL_HIST_EPI, per_page=cfg_per_page, timeout=timeout, max_retries=retries, max_pages=cfg_max_pages)
                if api_mov is not None and api_epi is not None:
                    api_total = int(api_mov) + int(api_epi)
                    if api_total != len(cached_items):
                        _dbg("index_reconcile", reason="count_mismatch_on_cache_hit", strategy="full_fetch", api_total=api_total, cached=len(cached_items))
                        _bust_index_cache(reason="count_mismatch_on_cache_hit")
                        # fall through to full fetch
                    else:
                        # Count matches 
                        _save_cache_doc(cached_items, cached_wm, validated_at=_now_iso())
                        _dbg("index_cache_hit", reason="activities_unchanged", source="cache", count=len(cached_items))
                        if prog:
                            try:
                                prog.tick(0, total=len(cached_items), force=True)
                                prog.tick(len(cached_items), total=len(cached_items))
                                prog.done(ok=True, total=len(cached_items))
                            except Exception:
                                pass
                        _info("index_done", count=len(cached_items), source="cache")
                        return cached_items
                else:
                    # preflight failed
                    _save_cache_doc(cached_items, cached_wm, validated_at=_now_iso())
                    _dbg("index_cache_hit", reason="activities_unchanged", source="cache", count=len(cached_items))
                    if prog:
                        try:
                            prog.tick(0, total=len(cached_items), force=True)
                            prog.tick(len(cached_items), total=len(cached_items))
                            prog.done(ok=True, total=len(cached_items))
                        except Exception:
                            pass
                    _info("index_done", count=len(cached_items), source="cache")
                    return cached_items
            else:
                _dbg("index_cache_hit", reason="activities_unchanged", source="cache", count=len(cached_items))
                if prog:
                    try:
                        prog.tick(0, total=len(cached_items), force=True)
                        prog.tick(len(cached_items), total=len(cached_items))
                        prog.done(ok=True, total=len(cached_items))
                    except Exception:
                        pass
                _info("index_done", count=len(cached_items), source="cache")
                return cached_items

        if a is not None and b is not None and a > b:
            _cached_epoch = _as_epoch(_iso8601(cached_wm) or "")
            start_at = time.strftime("%Y-%m-%dT%H:%M:%S.000Z", time.gmtime((_cached_epoch or 0) - 300)) if _cached_epoch else cached_wm
            _dbg("index_reconcile", reason="cache_delta", strategy="delta", start_at=start_at, end_at=remote_wm, cached=len(cached_items))
            try:
                idx: dict[str, dict[str, Any]] = dict(cached_items)
                announced_total = None
                if prog:
                    try:
                        prog.tick(0, total=len(idx), force=True)
                    except Exception:
                        pass

                movies = _fetch_history(
                    sess,
                    headers,
                    URL_HIST_MOV,
                    per_page=cfg_per_page,
                    max_pages=cfg_max_pages,
                    timeout=timeout,
                    max_retries=retries,
                    start_at=start_at,
                )
                episodes = _fetch_history(
                    sess,
                    headers,
                    URL_HIST_EPI,
                    per_page=cfg_per_page,
                    max_pages=cfg_max_pages,
                    timeout=timeout,
                    max_retries=retries,
                    start_at=start_at,
                    extended=epi_extended,
                )

                added = 0

                for m in movies + episodes:
                    w = _iso8601(m.get("watched_at"))
                    ts = _as_epoch(w) if w else None
                    if ts is None:
                        continue
                    if (
                        m.get("type") == "episode"
                        and isinstance(m.get("show_ids"), dict)
                        and m.get("season") is not None
                        and m.get("episode") is not None
                    ):
                        base_key = canonical_key(
                            id_minimal(
                                {
                                    "type": "episode",
                                    "show_ids": m["show_ids"],
                                    "season": m["season"],
                                    "episode": m["episode"],
                                }
                            )
                        )
                    else:
                        base_key = canonical_key(id_minimal(m))
                    ek = f"{base_key}@{ts}"

                    if ek in idx:
                        # Already cached; keep existing.
                        continue

                    idx[ek] = m
                    added += 1

                _dbg("index_fetch_counts", source="cache_delta", added=added, count=len(idx))

                count_ok = True
                api_mov = _preflight_total(sess, headers, URL_HIST_MOV, per_page=cfg_per_page, timeout=timeout, max_retries=retries, max_pages=cfg_max_pages)
                api_epi = _preflight_total(sess, headers, URL_HIST_EPI, per_page=cfg_per_page, timeout=timeout, max_retries=retries, max_pages=cfg_max_pages)
                if api_mov is not None and api_epi is not None:
                    api_total = int(api_mov) + int(api_epi)
                    if api_total != len(idx):
                        _dbg("index_reconcile", reason="count_mismatch_after_delta", strategy="full_fetch", api_total=api_total, cached=len(idx))
                        _bust_index_cache(reason="count_mismatch_after_delta")
                        count_ok = False

                if count_ok:
                    if prog:
                        try:
                            prog.done(ok=True, total=len(idx))
                        except Exception:
                            pass
                    _info("index_done", count=len(idx), source="cache_delta")
                    _save_cache_doc(idx, remote_wm)
                    return idx
            except Exception as e:
                _warn("index_reconcile", reason="cache_delta_failed", strategy="delta", error=str(e))
    elif cached_items and not remote_wm:
        _dbg("index_cache_hit", reason="activities_unavailable", source="cache", count=len(cached_items))
        if prog:
            try:
                prog.tick(0, total=len(cached_items), force=True)
                prog.tick(len(cached_items), total=len(cached_items))
                prog.done(ok=True, total=len(cached_items))
            except Exception:
                pass
        _info("index_done", count=len(cached_items), source="cache")
        return cached_items

    total_mov = _preflight_total(
        sess,
        headers,
        URL_HIST_MOV,
        per_page=cfg_per_page,
        timeout=timeout,
        max_retries=retries,
        max_pages=cfg_max_pages,
    )
    total_epi = _preflight_total(
        sess,
        headers,
        URL_HIST_EPI,
        per_page=cfg_per_page,
        timeout=timeout,
        max_retries=retries,
        max_pages=cfg_max_pages,
    )
    announced_total: int | None = None
    if total_mov is not None and total_epi is not None:
        announced_total = int(total_mov) + int(total_epi)
        if prog:
            try:
                prog.tick(0, total=announced_total, force=True)
            except Exception:
                pass
    done = 0

    def bump(n: int) -> None:
        nonlocal done
        done += int(n or 0)
        if prog:
            try:
                if announced_total is not None:
                    prog.tick(done, total=announced_total)
                else:
                    prog.tick(done)
            except Exception:
                pass

    movies = _fetch_history(
        sess,
        headers,
        URL_HIST_MOV,
        per_page=cfg_per_page,
        max_pages=cfg_max_pages,
        timeout=timeout,
        max_retries=retries,
        bump=bump,
    )
    episodes = _fetch_history(
        sess,
        headers,
        URL_HIST_EPI,
        per_page=cfg_per_page,
        max_pages=cfg_max_pages,
        timeout=timeout,
        max_retries=retries,
        extended=epi_extended,
        bump=bump,
    )
    idx: dict[str, dict[str, Any]] = {}
    for m in movies + episodes:
        w = _iso8601(m.get("watched_at"))
        ts = _as_epoch(w) if w else None
        if ts is None:
            continue
        if (
            m.get("type") == "episode"
            and isinstance(m.get("show_ids"), dict)
            and m.get("season") is not None
            and m.get("episode") is not None
        ):
            base_key = canonical_key(
                id_minimal(
                    {
                        "type": "episode",
                        "show_ids": m["show_ids"],
                        "season": m["season"],
                        "episode": m["episode"],
                    }
                )
            )
        else:
            base_key = canonical_key(id_minimal(m))
        ek = f"{base_key}@{ts}"

        # Collision guard:
        if ek in idx:
            ids = m.get("ids") if isinstance(m.get("ids"), Mapping) else {}
            trakt_id = str(ids.get("trakt") or "").strip() if isinstance(ids, Mapping) else ""
            tmdb_id = str(ids.get("tmdb") or "").strip() if isinstance(ids, Mapping) else ""
            imdb_id = str(ids.get("imdb") or "").strip() if isinstance(ids, Mapping) else ""
            hid = str(m.get("_trakt_history_id") or "").strip()

            if trakt_id:
                alt_base = f"trakt:{trakt_id}".lower()
            elif tmdb_id:
                alt_base = f"tmdb:{tmdb_id}".lower()
            else:
                alt_base = str(base_key or "unknown:").lower()

            suffix = f"~h{hid}" if hid else "~dup"
            ek2 = f"{alt_base}@{ts}{suffix}"
            n = 2
            while ek2 in idx:
                ek2 = f"{alt_base}@{ts}{suffix}{n}"
                n += 1

            _warn("index_reconcile", reason="key_collision", key=ek, key2=ek2, imdb=imdb_id or None, trakt=trakt_id or None, tmdb=tmdb_id or None, history_id=hid or None)
            idx[ek2] = m
        else:
            idx[ek] = m

    if prog:
        try:
            if announced_total is not None:
                prog.done(ok=True, total=announced_total)
            else:
                prog.done(ok=True, total=len(idx))
        except Exception:
            pass
    _dbg("index_fetch_counts", movies=len(movies), episodes=len(episodes), per_page=cfg_per_page, max_pages=cfg_max_pages, source="current")
    _info("index_done", count=len(idx), movies=len(movies), episodes=len(episodes), per_page=cfg_per_page, max_pages=cfg_max_pages, source="current")
    _save_cache_doc(idx, remote_wm or cached_wm)
    return idx


# resolvers
_SHOW_PATH_CACHE: dict[str, str] = {}
_SEASON_EP_CACHE: dict[str, dict[int, dict[str, str]]] = {}
_EP_RESOLVE_CACHE: dict[str, dict[str, str]] = {}


def _stable_show_key(ids: Mapping[str, Any]) -> str:
    return json.dumps(
        {k: ids.get(k) for k in ("slug", "trakt", "tmdb", "imdb", "tvdb") if ids.get(k)},
        sort_keys=True,
    )


def _pick_show_path_id(ids: Mapping[str, Any]) -> str | None:
    slug = ids.get("slug")
    if slug:
        return str(slug)
    trakt_id = ids.get("trakt")
    if trakt_id:
        return str(trakt_id)
    return None



def _resolve_show_path_id(
    adapter: Any,
    show_ids: Mapping[str, Any],
    *,
    timeout: float,
    retries: int,
) -> str | None:
    if not RESOLVE_ENABLE:
        return _pick_show_path_id(show_ids or {})
    skey = _stable_show_key(show_ids or {})
    if skey in _SHOW_PATH_CACHE:
        return _SHOW_PATH_CACHE[skey]
    path_id = _pick_show_path_id(show_ids or {})
    if path_id:
        _SHOW_PATH_CACHE[skey] = path_id
        return path_id
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    for k in ("tmdb", "imdb", "tvdb"):
        v = (show_ids or {}).get(k)
        if not v:
            continue
        url = f"{BASE}/search/{k}/{v}"
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            params={"type": "show"},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code == 200:
            arr = r.json() or []
            for hit in arr:
                show = hit.get("show") or {}
                ids = show.get("ids") or {}
                pid = _pick_show_path_id(ids)
                if pid:
                    _SHOW_PATH_CACHE[skey] = pid
                    return pid
    return None


def _resolve_episode_ids_via_trakt(
    adapter: Any,
    show_ids: Mapping[str, Any],
    season: Any,
    number: Any,
    *,
    timeout: float,
    retries: int,
) -> dict[str, str]:
    if not RESOLVE_ENABLE:
        return {}
    try:
        s = int(season)
        e = int(number)
    except Exception:
        return {}
    path_id = _resolve_show_path_id(adapter, show_ids, timeout=timeout, retries=retries)
    if not path_id:
        return {}
    season_key = f"{path_id}|S{s}"
    if season_key not in _SEASON_EP_CACHE:
        sess = adapter.client.session
        headers = headers_for_adapter(adapter)
        url = f"{BASE}/shows/{path_id}/seasons/{s}"
        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=headers,
            timeout=timeout,
            max_retries=retries,
        )
        epmap: dict[int, dict[str, str]] = {}
        if r.status_code == 200:
            rows = r.json() or []
            for row in rows:
                num = row.get("number")
                ids = {
                    ik: str(iv)
                    for ik, iv in (row.get("ids") or {}).items()
                    if ik in ("tmdb", "imdb", "tvdb", "trakt") and iv
                }
                if isinstance(num, int) and ids:
                    epmap[num] = ids
        _SEASON_EP_CACHE[season_key] = epmap
    ids = _SEASON_EP_CACHE.get(season_key, {}).get(e)
    if ids:
        return ids
    cache_key = json.dumps({"p": path_id, "s": s, "e": e}, sort_keys=True)
    if cache_key in _EP_RESOLVE_CACHE:
        return dict(_EP_RESOLVE_CACHE[cache_key])
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    url = f"{BASE}/shows/{path_id}/seasons/{s}/episodes/{e}"
    r = request_with_retries(
        sess,
        "GET",
        url,
        headers=headers,
        timeout=timeout,
        max_retries=retries,
    )
    if r.status_code == 200:
        d = r.json() or {}
        ids = {
            ik: str(iv)
            for ik, iv in (d.get("ids") or {}).items()
            if ik in ("tmdb", "imdb", "tvdb", "trakt") and iv
        }
        if ids:
            _EP_RESOLVE_CACHE[cache_key] = ids
            return ids
    return {}


# batching helpers
def _extract_show_ids_for_episode(it: Mapping[str, Any]) -> dict[str, Any]:
    show_ids = dict(it.get("show_ids") or {})
    if not show_ids and (it.get("season") is not None and it.get("episode") is not None):
        show_ids = dict(it.get("ids") or {})

    out: dict[str, Any] = {}
    for k in ("trakt", "slug", "tmdb", "imdb", "tvdb"):
        v = show_ids.get(k)
        if not v:
            continue
        if k in ("trakt", "tmdb", "tvdb"):
            try:
                out[k] = int(v)
            except Exception:
                s = str(v).strip()
                if s.isdigit():
                    out[k] = int(s)
        elif k == "imdb":
            s = str(v).strip()
            if s:
                out[k] = s
        else:
            s = str(v).strip()
            if s:
                out[k] = s
    return out


def _history_when_for_add(item: Mapping[str, Any], kind: str) -> tuple[str | None, str | None]:
    raw = item.get("watched_at")
    if raw is None:
        return None, None
    s = str(raw).strip()
    if not s:
        return None, None
    special = s.lower()
    if kind == "episodes" and special in {"released", "unknown"}:
        return special, None
    when = _iso8601(raw)
    if when:
        return when, None
    return None, "invalid watched_at"


def _history_item_minimal(kind: str, item: Mapping[str, Any], ids: Mapping[str, Any] | None = None) -> dict[str, Any]:
    ids = dict(ids or {})
    if kind == "movies":
        return id_minimal({"type": "movie", "ids": ids})
    if kind == "shows":
        return id_minimal({"type": "show", "ids": ids})
    if kind == "seasons":
        out: dict[str, Any] = {"type": "season"}
        if ids:
            out["ids"] = ids
        show_ids = dict(item.get("show_ids") or {})
        if show_ids:
            out["show_ids"] = show_ids
        season_no = item.get("season")
        if season_no is None:
            season_no = item.get("number")
        if season_no is not None:
            sn = _int_or_none(season_no)
            if sn is not None:
                out["season"] = sn
        return id_minimal(out)
    out: dict[str, Any] = {"type": "episode"}
    if ids:
        out["ids"] = ids
    show_ids = dict(item.get("show_ids") or {})
    if show_ids:
        out["show_ids"] = show_ids
    season_no = item.get("season")
    if season_no is None:
        season_no = item.get("season_number")
    episode_no = item.get("episode")
    if episode_no is None:
        episode_no = item.get("episode_number")
    if season_no is not None:
        sn = _int_or_none(season_no)
        if sn is not None:
            out["season"] = sn
    if episode_no is not None:
        en = _int_or_none(episode_no)
        if en is not None:
            out["episode"] = en
    return id_minimal(out)


def _parse_raw_history_id(item: Mapping[str, Any]) -> int | None:
    raw = item.get("_trakt_history_id")
    if raw is None:
        raw = item.get("history_id")
    if raw is None:
        return None
    s = str(raw).strip()
    return int(s) if s.isdigit() else None


def _batch_add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], list[dict[str, Any]], list[str], dict[str, Any]]:
    movies: list[dict[str, Any]] = []
    shows_map: dict[str, dict[str, Any]] = {}
    seasons: list[dict[str, Any]] = []
    episodes_flat: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    accepted_keys: list[str] = []
    accepted_minimals: list[dict[str, Any]] = []

    skipped_keys: list[str] = []
    req_index: dict[str, Any] = {
        "by_ep_ids": {},
        "by_movie_ids": {},
        "by_show_ep": {},
        "by_show_season": {},
        "by_show": {},
        "src_items": {},
        "leaf_kind": {},
    }

    # De-dupe guards (Trakt will 409 on item+watched_at conflicts).
    seen_movies: set[tuple[str, str]] = set()
    seen_eps_flat: set[tuple[str, str]] = set()
    seen_show_eps: set[tuple[str, int, int, str]] = set()

    def _show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps(
            {k: str(ids[k]) for k in ("trakt", "slug", "tmdb", "imdb", "tvdb") if k in ids and ids[k]},
            sort_keys=True,
        )

    def _src_key(it: Mapping[str, Any], m: Mapping[str, Any]) -> str:
        k = str(it.get("_cw_source_key") or "").strip()
        if k:
            return k
        try:
            return str(key_of(m) or "")
        except Exception:
            return ""

    def _register(it: Mapping[str, Any], m: dict[str, Any], *, ids=None, show_ids=None, season=None, episode=None, movie=False) -> None:
        sk = _src_key(it, m)
        if not sk:
            return
        req_index["src_items"].setdefault(
            sk,
            {"source": _source_item_for_key(sk) or dict(it), "destination": dict(it)},
        )
        req_index["leaf_kind"].setdefault(sk, "movie" if movie else "episode")
        for field, value in (ids or {}).items():
            if value in (None, ""):
                continue
            bucket = "by_movie_ids" if movie else "by_ep_ids"
            req_index[bucket].setdefault((str(field), str(value)), []).append(sk)
        for field, value in (show_ids or {}).items():
            if value in (None, ""):
                continue
            token = (str(field), str(value))
            req_index["by_show"].setdefault(token, []).append(sk)
            if season is not None:
                req_index["by_show_season"].setdefault((*token, int(season)), []).append(sk)
                if episode is not None:
                    req_index["by_show_ep"].setdefault((*token, int(season), int(episode)), []).append(sk)

    def _accept(m: dict[str, Any]) -> None:
        accepted_minimals.append(m)
        accepted_keys.append(key_of(m))

    for it in items or []:
        kind = (pick_trakt_kind(it) or "movies").lower()
        ids = ids_for_trakt(it)
        show_ids = _extract_show_ids_for_episode(it)
        season_no = it.get("season")
        if season_no is None:
            season_no = it.get("season_number")
        if season_no is None:
            season_no = it.get("number")
        episode_no = it.get("episode")
        if episode_no is None:
            episode_no = it.get("episode_number")

        when, when_error = _history_when_for_add(it, kind)
        if when_error:
            m = _history_item_minimal(kind, it, ids)
            unresolved.append({"item": m, "hint": when_error})
            continue

        if kind == "movies":
            if not ids:
                m = _history_item_minimal(kind, it, ids)
                unresolved.append({"item": m, "hint": "missing ids"})
                continue
            obj: dict[str, Any] = {"ids": ids}
            if when:
                obj["watched_at"] = when
            sig = (json.dumps(ids, sort_keys=True), str(obj.get("watched_at") or ""))
            if sig in seen_movies:
                continue
            seen_movies.add(sig)
            movies.append(obj)
            m = _history_item_minimal(kind, it, ids)
            _register(it, m, ids=ids, movie=True)
            _accept(m)
            continue

        if kind == "shows":
            if not ids:
                m = _history_item_minimal(kind, it, ids)
                unresolved.append({"item": m, "hint": "missing ids"})
                continue
            skey = _show_key(ids)
            entry = shows_map.setdefault(skey, {"ids": ids, "seasons": {}})
            if when:
                entry["watched_at"] = when
            _accept(_history_item_minimal(kind, it, ids))
            continue

        if kind == "seasons":
            if ids:
                obj: dict[str, Any] = {"ids": ids}
                if when:
                    obj["watched_at"] = when
                seasons.append(obj)
                _accept(_history_item_minimal(kind, it, ids))
                continue
            if show_ids and season_no is not None:
                skey = _show_key(show_ids)
                entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
                season_i = _int_or_none(season_no)
                if season_i is None:
                    m = _history_item_minimal(kind, it, ids)
                    unresolved.append({"item": m, "hint": "invalid season number"})
                    continue
                season_entry = entry["seasons"].setdefault(season_i, {"number": season_i})
                if when:
                    season_entry["watched_at"] = when
                _accept(_history_item_minimal(kind, it, ids))
                continue
            m = _history_item_minimal(kind, it, ids)
            unresolved.append({"item": m, "hint": "season scope or ids missing"})
            continue
        if kind == "episodes":
            show_scope_ok = bool(show_ids and season_no is not None and episode_no is not None)
            strong_ids = bool(ids and ("trakt" in ids))
            use_ids = bool(ids) and (strong_ids or not show_scope_ok)
            if show_scope_ok and show_ids and not strong_ids:
                use_ids = False

            # Avoid writing roll-up episodes
            if not show_scope_ok and (season_no is None or episode_no is None) and not (ids and ("trakt" in ids)):
                m = _history_item_minimal(kind, it, ids)
                unresolved.append({"item": m, "hint": "missing season/episode"})
                continue

            if use_ids:
                obj: dict[str, Any] = {"ids": ids}
                if when:
                    obj["watched_at"] = when
                sig = (json.dumps(ids, sort_keys=True), str(obj.get("watched_at") or ""))
                if sig in seen_eps_flat:
                    continue
                seen_eps_flat.add(sig)
                episodes_flat.append(obj)
                m = _history_item_minimal(kind, it, ids)
                _register(it, m, ids=ids, show_ids=show_ids, season=_int_or_none(season_no), episode=_int_or_none(episode_no))
                _accept(m)
                continue

            if show_scope_ok:
                skey = _show_key(show_ids)
                entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
                season_i = _int_or_none(season_no)
                epn = _int_or_none(episode_no)
                if season_i is None or epn is None:
                    continue
                season_entry = entry["seasons"].setdefault(season_i, {"number": season_i, "episodes": []})

                ep_sig = (skey, season_i, epn, str(when or ""))
                if ep_sig in seen_show_eps:
                    continue
                seen_show_eps.add(ep_sig)

                ep_obj: dict[str, Any] = {"number": epn}
                if when:
                    ep_obj["watched_at"] = when
                season_entry.setdefault("episodes", []).append(ep_obj)
                m = _history_item_minimal(kind, it, ids)
                _register(it, m, ids=ids, show_ids=show_ids, season=season_i, episode=epn)
                _accept(m)
                continue

            m = _history_item_minimal(kind, it, ids)
            unresolved.append({"item": m, "hint": "episode scope or ids missing"})

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if shows_map:
        body["shows"] = []
        for entry in shows_map.values():
            obj: dict[str, Any] = {"ids": entry["ids"]}
            if entry.get("watched_at"):
                obj["watched_at"] = entry["watched_at"]
            if entry.get("seasons"):
                obj["seasons"] = list(entry["seasons"].values())
            body["shows"].append(obj)
    if seasons:
        body["seasons"] = seasons
    if episodes_flat:
        body["episodes"] = episodes_flat
    return body, unresolved, accepted_keys, accepted_minimals, skipped_keys, req_index


def _batch_remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]], list[str], list[dict[str, Any]], dict[int, dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows_map: dict[str, dict[str, Any]] = {}
    seasons: list[dict[str, Any]] = []
    episodes_flat: list[dict[str, Any]] = []
    raw_ids: list[int] = []
    raw_id_map: dict[int, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    accepted_keys: list[str] = []
    accepted_minimals: list[dict[str, Any]] = []

    def _show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps(
            {k: str(ids[k]) for k in ("trakt", "slug", "tmdb", "imdb", "tvdb") if k in ids and ids[k]},
            sort_keys=True,
        )

    def _accept(m: dict[str, Any]) -> None:
        accepted_minimals.append(m)
        accepted_keys.append(key_of(m))

    for it in items or []:
        raw_history_id = _parse_raw_history_id(it)
        if raw_history_id is not None:
            m = id_minimal(it)
            raw_ids.append(raw_history_id)
            raw_id_map[raw_history_id] = m
            _accept(m)
            continue

        kind = (pick_trakt_kind(it) or "movies").lower()
        ids = ids_for_trakt(it)
        show_ids = _extract_show_ids_for_episode(it)
        season_no = it.get("season")
        if season_no is None:
            season_no = it.get("season_number")
        if season_no is None:
            season_no = it.get("number")
        episode_no = it.get("episode")
        if episode_no is None:
            episode_no = it.get("episode_number")

        if kind == "movies":
            if not ids:
                m = _history_item_minimal(kind, it, ids)
                unresolved.append({"item": m, "hint": "missing ids"})
                continue
            movies.append({"ids": ids})
            _accept(_history_item_minimal(kind, it, ids))
            continue

        if kind == "shows":
            if not ids:
                m = _history_item_minimal(kind, it, ids)
                unresolved.append({"item": m, "hint": "missing ids"})
                continue
            skey = _show_key(ids)
            shows_map.setdefault(skey, {"ids": ids, "seasons": {}})
            _accept(_history_item_minimal(kind, it, ids))
            continue

        if kind == "seasons":
            if ids:
                seasons.append({"ids": ids})
                _accept(_history_item_minimal(kind, it, ids))
                continue
            if show_ids and season_no is not None:
                skey = _show_key(show_ids)
                entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
                season_i = _int_or_none(season_no)
                if season_i is None:
                    m = _history_item_minimal(kind, it, ids)
                    unresolved.append({"item": m, "hint": "invalid season number"})
                    continue
                entry["seasons"].setdefault(season_i, {"number": season_i})
                _accept(_history_item_minimal(kind, it, ids))
                continue
            m = _history_item_minimal(kind, it, ids)
            unresolved.append({"item": m, "hint": "season scope or ids missing"})
            continue

        if kind == "episodes":
            if show_ids and season_no is not None and episode_no is not None:
                skey = _show_key(show_ids)
                entry = shows_map.setdefault(skey, {"ids": show_ids, "seasons": {}})
                season_i = _int_or_none(season_no)
                ep_i = _int_or_none(episode_no)
                if season_i is None or ep_i is None:
                    m = _history_item_minimal(kind, it, ids)
                    unresolved.append({"item": m, "hint": "invalid season/episode number"})
                    continue
                season_entry = entry["seasons"].setdefault(season_i, {"number": season_i, "episodes": []})
                season_entry.setdefault("episodes", []).append({"number": ep_i})
                _accept(_history_item_minimal(kind, it, ids))
                continue
            if ids:
                episodes_flat.append({"ids": ids})
                _accept(_history_item_minimal(kind, it, ids))
                continue
            m = _history_item_minimal(kind, it, ids)
            unresolved.append({"item": m, "hint": "episode scope or ids missing"})

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if shows_map:
        body["shows"] = []
        for entry in shows_map.values():
            obj: dict[str, Any] = {"ids": entry["ids"]}
            if entry.get("seasons"):
                obj["seasons"] = list(entry["seasons"].values())
            body["shows"].append(obj)
    if seasons:
        body["seasons"] = seasons
    if episodes_flat:
        body["episodes"] = episodes_flat
    if raw_ids:
        body["ids"] = sorted(set(raw_ids))
    return body, unresolved, accepted_keys, accepted_minimals, raw_id_map

def _deleted_event_count(deleted: Any) -> int:
    if not isinstance(deleted, Mapping):
        return 0
    total = 0
    for bucket in ("movies", "episodes", "shows", "seasons"):
        total += int(_int_or_none(deleted.get(bucket)) or 0)
    return total


def _not_found_ids(not_found: Any) -> set[int]:
    out: set[int] = set()
    if not isinstance(not_found, Mapping):
        return out
    for raw in (not_found.get("ids") or []):
        value = _int_or_none(raw if not isinstance(raw, Mapping) else raw.get("id"))
        if value is not None:
            out.add(value)
    return out


def _requires_alias_removal(item: Mapping[str, Any]) -> bool:
    return _simkl_to_trakt_active() and _is_native_anime(item)


def _exact_destination_for_removal(
    item: Mapping[str, Any],
    adapter: Any,
    *,
    timeout: float,
    retries: int,
) -> dict[str, Any] | None:
    watched = _iso8601(item.get("watched_at") or item.get("watchedAt"))
    if not watched:
        return None

    episode_id = str((item.get("ids") or {}).get("trakt") or "").strip()
    show_trakt = str((item.get("show_ids") or {}).get("trakt") or "").strip()
    if episode_id and show_trakt and episode_id == show_trakt:
        _dbg("remove_copied_show_id_rejected", trakt=episode_id)
        episode_id = ""
    resolved: Mapping[str, Any] | None = None
    if not episode_id:
        for ns, value in _genuine_episode_ids(item).items():
            found = _search_trakt_episode(adapter, ns, value, timeout=timeout, retries=retries)
            if found and str((found.get("ids") or {}).get("trakt") or "").strip():
                resolved = found
                episode_id = str(found["ids"]["trakt"]).strip()
                break
    if not episode_id:
        return None

    out: dict[str, Any] = {"destination_episode_id": episode_id, "watched_at": watched}
    if isinstance(resolved, Mapping):
        dest_item = {
            "type": "episode",
            "ids": _trakt_ids_only(resolved.get("ids") or {}),
            "show_ids": _trakt_ids_only(resolved.get("show_ids") or {}),
            "season": resolved.get("season"),
            "episode": resolved.get("episode"),
            "watched_at": watched,
        }
        out["destination_key"] = str(canonical_key(dest_item) or "")
        out["season"] = resolved.get("season")
        out["episode"] = resolved.get("episode")
    out["basis"] = "exact_episode_identity"
    return out


def _alias_lookup(
    aliases: Mapping[str, Mapping[str, Any]],
    item: Mapping[str, Any],
) -> tuple[str, Mapping[str, Any] | None, str]:
    source_event_key = _source_event_key(item)
    rec = aliases.get(source_event_key) if source_event_key else None
    if isinstance(rec, Mapping):
        return source_event_key, rec, ""
    base = str(source_event_key).split("@", 1)[0]
    if not base:
        return "", None, "trakt_history_alias_missing"
    hits = [(k, v) for k, v in aliases.items() if str(k).split("@", 1)[0] == base and isinstance(v, Mapping)]
    if len(hits) == 1:
        return hits[0][0], hits[0][1], ""
    if len(hits) > 1:
        return "", None, "trakt_history_event_ambiguous"
    return "", None, "trakt_history_alias_missing"


def _cache_remove_event_keys(event_keys: Iterable[str], history_ids: Iterable[int]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    keys = {str(k) for k in (event_keys or []) if k}
    hids = {str(h) for h in (history_ids or []) if h is not None}
    if not keys and not hids:
        return
    try:
        doc = _load_cache_doc()
        cache_items: dict[str, dict[str, Any]] = dict(doc.get("items") or {})
        if not cache_items:
            return
        wm_prev = str((doc.get("wm") or {}).get("watched_at") or "").strip() or None
        removed = 0
        for ek in list(cache_items.keys()):
            item = cache_items.get(ek) or {}
            hid = str(item.get("_trakt_history_id") or item.get("history_id") or "").strip()
            if str(ek) in keys or (hid and hid in hids):
                cache_items.pop(ek, None)
                removed += 1
        if removed <= 0:
            return
        _save_cache_doc(cache_items, wm_prev, validated_at="")
        _dbg("cache_remove_exact", removed=removed, keys=len(keys), history_ids=len(hids))
    except Exception as e:
        _warn("cache_save_failed", cache="index", op="remove_exact", error=str(e))


def _lookup_history_id(
    adapter: Any,
    rec: Mapping[str, Any],
    *,
    timeout: float,
    retries: int,
) -> tuple[int | None, str]:
    episode_id = str(rec.get("destination_episode_id") or "").strip()
    watched = _iso8601(rec.get("watched_at"))
    if not episode_id or not watched:
        return None, "trakt_history_event_not_found"
    want = _as_epoch(watched)
    if want is None:
        return None, "trakt_history_event_not_found"
    try:
        r = request_with_retries(
            adapter.client.session,
            "GET",
            f"{BASE}/sync/history/episodes/{episode_id}",
            headers=headers_for_adapter(adapter),
            params={"limit": 100, "start_at": watched, "end_at": watched},
            timeout=timeout,
            max_retries=retries,
        )
    except Exception as e:
        _warn("history_lookup_failed", episode=episode_id, error=str(e))
        return None, "trakt_history_event_not_found"
    if r.status_code not in (200, 201):
        _warn("history_lookup_failed", episode=episode_id, status=r.status_code)
        return None, "trakt_history_event_not_found"
    try:
        rows = r.json() or []
    except Exception:
        rows = []
    matches: list[int] = []
    for row in rows if isinstance(rows, list) else []:
        if not isinstance(row, Mapping):
            continue
        got = _as_epoch(_iso8601(row.get("watched_at")) or "")
        if got is None or got != want:
            continue
        hid = _int_or_none(row.get("id"))
        if hid is not None and hid not in matches:
            matches.append(hid)
    if not matches:
        return None, "trakt_history_event_not_found"
    if len(matches) > 1:
        _warn("history_lookup_ambiguous", episode=episode_id, watched_at=watched, matches=len(matches))
        return None, "trakt_history_event_ambiguous"
    return matches[0], ""


def _history_body_to_collection(body: Mapping[str, Any], types: set[str]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    if "movies" in types:
        seen_movies: set[str] = set()
        for m in body.get("movies") or []:
            ids = (m or {}).get("ids") or {}
            if not ids:
                continue
            k = json.dumps(ids, sort_keys=True)
            if k in seen_movies:
                continue
            seen_movies.add(k)
            out.setdefault("movies", []).append(
                {
                    "ids": ids,
                    "collected_at": m.get("watched_at") or _now_iso(),
                }
            )

    if "shows" in types:
        seen_eps: set[str] = set()
        for e in body.get("episodes") or []:
            ids = (e or {}).get("ids") or {}
            if not ids:
                continue
            k = json.dumps(ids, sort_keys=True)
            if k in seen_eps:
                continue
            seen_eps.add(k)
            out.setdefault("episodes", []).append(
                {
                    "ids": ids,
                    "collected_at": e.get("watched_at") or _now_iso(),
                }
            )

        shows = body.get("shows") or []
        if shows:
            coll_shows: list[dict[str, Any]] = []
            for sh in shows:
                ids = (sh or {}).get("ids") or {}
                seasons_in = (sh or {}).get("seasons") or []
                if not ids or not seasons_in:
                    continue
                seasons_out: list[dict[str, Any]] = []
                for s in seasons_in:
                    num = s.get("number")
                    eps = s.get("episodes") or []
                    if num is None or not eps:
                        continue
                    eps_out: list[dict[str, Any]] = []
                    for ep in eps:
                        n = ep.get("number")
                        if n is None:
                            continue
                        eps_out.append(
                            {
                                "number": int(n),
                                "collected_at": ep.get("watched_at") or _now_iso(),
                            }
                        )
                    if eps_out:
                        seasons_out.append({"number": int(num), "episodes": eps_out})
                if seasons_out:
                    coll_shows.append({"ids": ids, "seasons": seasons_out})
            if coll_shows:
                out["shows"] = coll_shows
    return out

def _unresolved_from_not_found(nf: Any, raw_id_map: Mapping[int, Mapping[str, Any]] | None = None) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not isinstance(nf, dict):
        return out

    for bucket, typ in (("movies", "movie"), ("seasons", "season"), ("episodes", "episode")):
        for obj in nf.get(bucket) or []:
            if not isinstance(obj, dict):
                continue
            out.append({"item": id_minimal({"type": typ, "ids": obj.get("ids") or {}}), "hint": "not_found"})

    for obj in nf.get("shows") or []:
        if not isinstance(obj, dict):
            continue
        ids = obj.get("ids") or {}
        seasons = obj.get("seasons") or []
        if ids and not seasons:
            out.append({"item": id_minimal({"type": "show", "ids": ids}), "hint": "not_found"})

    out.extend(_unresolved_from_nf_shows(nf.get("shows")))

    if raw_id_map:
        for raw in nf.get("ids") or []:
            try:
                rid = int(raw)
            except Exception:
                continue
            item = raw_id_map.get(rid)
            if item:
                out.append({"item": dict(item), "hint": "not_found"})

    return out


def _unresolved_from_nf_shows(nf_shows: Any) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    if not isinstance(nf_shows, list):
        return out

    for sh in nf_shows:
        if not isinstance(sh, dict):
            continue
        show_ids = (sh.get("ids") or {})
        seasons = sh.get("seasons") or []
        if not isinstance(seasons, list):
            continue
        for s in seasons:
            if not isinstance(s, dict):
                continue
            sn = s.get("number")
            eps = s.get("episodes") or []
            if sn is None or not isinstance(eps, list):
                continue
            for ep in eps:
                if not isinstance(ep, dict):
                    continue
                en = ep.get("number")
                if en is None:
                    continue
                m = id_minimal(
                    {
                        "type": "episode",
                        "show_ids": show_ids,
                        "season": int(sn),
                        "episode": int(en),
                    }
                )
                out.append({"item": m, "hint": "not_found"})

    return out



def _not_found_scope_count(nf: Any) -> int:
    if not isinstance(nf, Mapping):
        return 0
    total = 0
    for bucket in ("movies", "episodes", "seasons"):
        rows = nf.get(bucket)
        total += len(rows) if isinstance(rows, list) else 0
    for sh in nf.get("shows") or []:
        if not isinstance(sh, Mapping):
            continue
        seasons = sh.get("seasons")
        if not isinstance(seasons, list) or not seasons:
            total += 1
            continue
        for season in seasons:
            if not isinstance(season, Mapping):
                continue
            eps = season.get("episodes")
            total += len(eps) if isinstance(eps, list) and eps else 1
    return total


def _nf_ids(obj: Mapping[str, Any], *nested: str) -> dict[str, str]:
    out: dict[str, str] = {}
    sources: list[Any] = [obj.get("ids")]
    for key in nested:
        inner = obj.get(key)
        if isinstance(inner, Mapping):
            sources.append(inner.get("ids"))
    for src in sources:
        if not isinstance(src, Mapping):
            continue
        for field, value in src.items():
            if value in (None, ""):
                continue
            out.setdefault(str(field), str(value))
    return out


def _nf_number(obj: Mapping[str, Any], *nested: str) -> int | None:
    n = _int_or_none(obj.get("number"))
    if n is not None:
        return n
    for key in nested:
        inner = obj.get(key)
        if isinstance(inner, Mapping):
            n = _int_or_none(inner.get("number"))
            if n is not None:
                return n
    return None


def _scope_keys(
    req_index: Mapping[str, Any],
    ids: Mapping[str, Any],
    *,
    season: int | None = None,
    episode: int | None = None,
) -> tuple[list[str], str]:
    tokens = [(str(f), str(v)) for f, v in (ids or {}).items() if v not in (None, "")]
    if not tokens:
        return [], ""

    def _gather(bucket: str, key_of_token) -> list[str]:
        out: list[str] = []
        table = req_index.get(bucket) or {}
        for token in tokens:
            out.extend(table.get(key_of_token(token)) or [])
        return list(dict.fromkeys(out))

    hits = _gather("by_ep_ids", lambda t: t)
    if hits:
        return hits, "episode_ids"

    if season is not None and episode is not None:
        hits = _gather("by_show_ep", lambda t: (*t, season, episode))
        if hits:
            return hits, "show_season_episode"

    if season is not None:
        hits = _gather("by_show_season", lambda t: (*t, season))
        if hits:
            return hits, "show_season"

    hits = _gather("by_show", lambda t: t)
    if hits:
        return hits, "show"
    return [], ""


def _nf_season_number(obj: Mapping[str, Any]) -> int | None:
    for key in ("season", "season_number"):
        n = _int_or_none(obj.get(key))
        if n is not None:
            return n
    for key in ("season", "episode"):
        inner = obj.get(key)
        if isinstance(inner, Mapping):
            n = _int_or_none(inner.get("season") if key == "episode" else inner.get("number"))
            if n is not None:
                return n
    return None


def _correlate_not_found(nf: Any, req_index: Mapping[str, Any]) -> tuple[list[str], dict[str, str], int, int, list[dict[str, Any]]]:
    if not isinstance(nf, Mapping):
        return [], {}, 0, 0, []
    hit_reason: dict[str, str] = {}
    expanded = 0
    matched_scopes = 0
    unmatched: list[dict[str, Any]] = []

    def _mark(keys: Iterable[str], reason: str) -> bool:
        found = False
        for k in keys or []:
            found = True
            if k and (k not in hit_reason or hit_reason[k] == "trakt_parent_not_found"):
                hit_reason[k] = reason
        return found

    def _record_unmatched(bucket: str, ids: Mapping[str, Any], season: Any, episode: Any, candidates: Iterable[str]) -> None:
        cands = [k for k in dict.fromkeys(candidates) if k]
        unmatched.append({
            "bucket": bucket,
            "ids": dict(ids or {}),
            "season": season,
            "episode": episode,
            "candidates": cands,
        })
        _dbg(
            "not_found_scope_unmatched",
            bucket=bucket,
            ids=dict(ids or {}),
            season=season,
            episode=episode,
            candidates=len(cands),
        )

    for obj in nf.get("movies") or []:
        if not isinstance(obj, Mapping):
            continue
        ids = _nf_ids(obj, "movie")
        hit = False
        for field, value in ids.items():
            hit = _mark(req_index.get("by_movie_ids", {}).get((field, value), []), "trakt_episode_not_found") or hit
        if hit:
            matched_scopes += 1
        else:
            _record_unmatched("movies", ids, None, None, [])

    for obj in nf.get("episodes") or []:
        if not isinstance(obj, Mapping):
            continue
        ids = _nf_ids(obj, "episode", "show")
        s_num = _nf_season_number(obj)
        e_num = _nf_number(obj, "episode")
        keys, basis = _scope_keys(req_index, ids, season=s_num, episode=e_num)
        if keys:
            reason = "trakt_episode_not_found" if basis in ("episode_ids", "show_season_episode") else "trakt_parent_not_found"
            if basis in ("show", "show_season"):
                expanded += len(keys)
            _mark(keys, reason)
            matched_scopes += 1
            continue
        _record_unmatched("episodes", ids, s_num, e_num, [])

    for obj in nf.get("seasons") or []:
        if not isinstance(obj, Mapping):
            continue
        ids = _nf_ids(obj, "season", "show")
        s_num = _nf_season_number(obj)
        keys, basis = _scope_keys(req_index, ids, season=s_num)
        if keys:
            if basis in ("show", "show_season"):
                expanded += len(keys)
            _mark(keys, "trakt_parent_not_found")
            matched_scopes += 1
            continue
        _record_unmatched("seasons", ids, s_num, None, [])

    for sh in nf.get("shows") or []:
        if not isinstance(sh, Mapping):
            continue
        show_ids = _nf_ids(sh, "show")
        seasons = sh.get("seasons") or []
        tokens = [(f, v) for f, v in show_ids.items()]

        def _show_candidates() -> list[str]:
            out: list[str] = []
            for token in tokens:
                out.extend(req_index.get("by_show", {}).get(token, []))
            return out

        if not isinstance(seasons, list) or not seasons:
            scope_keys: set[str] = set()
            for token in tokens:
                scope_keys.update(req_index.get("by_show", {}).get(token, []))
            expanded += len(scope_keys)
            hit = _mark(scope_keys, "trakt_parent_not_found")
            if hit:
                matched_scopes += 1
            else:
                _record_unmatched("shows", show_ids, None, None, _show_candidates())
            continue

        for season in seasons:
            if not isinstance(season, Mapping):
                continue
            s_num = _nf_number(season, "season")
            eps = season.get("episodes") or []
            if s_num is None:
                continue
            if isinstance(eps, list) and eps:
                for ep in eps:
                    if not isinstance(ep, Mapping):
                        continue
                    e_num = _nf_number(ep, "episode")
                    ep_ids = _nf_ids(ep, "episode")
                    hit = False
                    for field, value in ep_ids.items():
                        hit = _mark(req_index.get("by_ep_ids", {}).get((field, value), []), "trakt_episode_not_found") or hit
                    if not hit and e_num is not None:
                        for token in tokens:
                            hit = _mark(req_index.get("by_show_ep", {}).get((*token, s_num, e_num), []), "trakt_episode_not_found") or hit
                    if hit:
                        matched_scopes += 1
                        continue
                    _record_unmatched("shows.episodes", show_ids, s_num, e_num, _show_candidates())
                continue
            season_keys: set[str] = set()
            for token in tokens:
                season_keys.update(req_index.get("by_show_season", {}).get((*token, s_num), []))
            expanded += len(season_keys)
            hit = _mark(season_keys, "trakt_parent_not_found")
            if hit:
                matched_scopes += 1
            else:
                _record_unmatched("shows.seasons", show_ids, s_num, None, _show_candidates())

    return list(hit_reason.keys()), hit_reason, expanded, matched_scopes, unmatched

def _retry_failed_episodes(
    adapter: Any,
    keys: list[str],
    src_lookup: Any,
    *,
    timeout: float,
    retries: int,
    write_timeout: float,
) -> tuple[set[str], dict[str, dict[str, Any]], int]:
    recovered: set[str] = set()
    destinations: dict[str, dict[str, Any]] = {}
    searches = 0
    if not keys:
        return recovered, destinations, searches

    payload: list[dict[str, Any]] = []
    owner: dict[str, str] = {}
    for k in keys:
        item = src_lookup(k)
        if not item:
            continue
        genuine = _genuine_episode_ids(item)
        trakt_id = str((item.get("ids") or {}).get("trakt") or "").strip()
        hit: dict[str, Any] | None = None
        if trakt_id:
            hit = {"ids": {"trakt": trakt_id}, "season": item.get("season"), "episode": item.get("episode")}
        else:
            for ns, value in genuine.items():
                searches += 1
                found = _search_trakt_episode(adapter, ns, value, timeout=timeout, retries=retries)
                if found and found.get("ids", {}).get("trakt"):
                    hit = found
                    break
        if not hit:
            continue
        ep_trakt = str((hit.get("ids") or {}).get("trakt") or "").strip()
        if not ep_trakt:
            continue
        watched = _iso8601(item.get("watched_at") or item.get("watchedAt")) or ""
        token = f"trakt:{ep_trakt}@{watched}"
        claim = _DEST_CLAIMS.get(token)
        if claim and claim != k:
            continue
        _DEST_CLAIMS[token] = k
        obj: dict[str, Any] = {"ids": {"trakt": ep_trakt}}
        if watched:
            obj["watched_at"] = watched
        payload.append(obj)
        owner[ep_trakt] = k
        destinations[k] = {
            "ids": _trakt_ids_only(hit.get("ids") or {}),
            "show_ids": _trakt_ids_only(hit.get("show_ids") or {}),
            "season": hit.get("season"),
            "episode": hit.get("episode"),
            "type": "episode",
            "watched_at": watched,
        }

    if not payload:
        return recovered, destinations, searches

    try:
        r = request_with_retries(
            adapter.client.session,
            "POST",
            URL_ADD,
            headers=headers_for_adapter(adapter),
            json={"episodes": payload},
            timeout=write_timeout,
            max_retries=retries,
        )
    except Exception as e:
        _warn("write_failed", op="add_retry", error=str(e))
        return recovered, {}, searches

    if r.status_code not in (200, 201):
        _warn("write_failed", op="add_retry", status=r.status_code)
        return recovered, {}, searches

    d = r.json() or {}
    nf_ids: set[str] = set()
    for obj in ((d.get("not_found") or {}).get("episodes") or []):
        if isinstance(obj, Mapping):
            tid = str((obj.get("ids") or {}).get("trakt") or "").strip()
            if tid:
                nf_ids.add(tid)
    for ep_trakt, k in owner.items():
        if ep_trakt in nf_ids:
            destinations.pop(k, None)
            continue
        recovered.add(k)
    _info("trakt_retry_result", op="add", attempted=len(payload), recovered=len(recovered), searches=searches)
    return recovered, {k: v for k, v in destinations.items() if k in recovered}, searches


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))
    write_timeout = float(_cfg_num(adapter, "history_write_timeout", max(timeout, 60.0), float))
    items_list = list(items)

    source_keys: list[str] = []
    source_by_key: dict[str, Mapping[str, Any]] = {}
    for it in items_list:
        try:
            k = str(canonical_key(it) or "")
        except Exception:
            k = ""
        if k:
            source_keys.append(k)
            source_by_key[k] = it
    _remember_source_items(source_by_key)

    outgoing, resolve_unresolved = _apply_simkl_resolution(adapter, items_list)
    body, unresolved, accepted_keys, accepted_minimals, skipped_keys, req_index = _batch_add(adapter, outgoing)
    unresolved = list(resolve_unresolved) + list(unresolved)
    sent_keys = [k for k in (req_index.get("src_items") or {}).keys() if k]

    def _src(key: str) -> dict[str, Any]:
        return _source_item_for_key(key, req_index) or dict(source_by_key.get(key) or {})

    def _destination_for(k: str) -> dict[str, Any]:
        rec = (req_index.get("src_items") or {}).get(k)
        dest = rec.get("destination") if isinstance(rec, Mapping) else None
        return _sanitize_destination(dest) if isinstance(dest, Mapping) else {}

    def _basis_for(k: str) -> str:
        rec = (req_index.get("src_items") or {}).get(k)
        dest = rec.get("destination") if isinstance(rec, Mapping) else None
        return str((dest or {}).get("_cw_resolution_basis") or "") if isinstance(dest, Mapping) else ""

    def _result(*, ambiguous_keys: Iterable[str] = (), unmatched: int = 0, added_leaves: int = 0, existing_leaves: int = 0) -> dict[str, Any]:
        ukeys: list[str] = []
        reason_counts: dict[str, int] = {}
        for u in unresolved:
            if not isinstance(u, Mapping):
                continue
            hint = str(u.get("hint") or u.get("reason") or "unknown")
            reason_counts[hint] = reason_counts.get(hint, 0) + 1
            k = str(u.get("key") or "")
            if not k:
                inner = u.get("item")
                obj: Mapping[str, Any] = inner if isinstance(inner, Mapping) else u
                try:
                    k = str(canonical_key(obj) or "")
                except Exception:
                    k = ""
            if k:
                ukeys.append(k)
        skips = [k for k in dict.fromkeys(skipped_keys or []) if k]
        skip_set = set(skips)

        ambiguous_set = {k for k in ambiguous_keys if k}
        if ambiguous_set:
            seen_u = set(ukeys)
            for k in sent_keys:
                if k not in ambiguous_set or k in skip_set or k in seen_u:
                    continue
                unresolved.append({"item": id_minimal(_src(k)), "hint": "trakt_response_ambiguous", "key": k})
                reason_counts["trakt_response_ambiguous"] = reason_counts.get("trakt_response_ambiguous", 0) + 1
                ukeys.append(k)
                seen_u.add(k)

        ukeys = list(dict.fromkeys(ukeys))
        ukey_set = set(ukeys)
        present = [k for k in sent_keys if k not in ukey_set and k not in skip_set]

        confirmed = list(present)
        skips_out = list(skips)
        if added_leaves or existing_leaves:
            if existing_leaves and not added_leaves:
                confirmed = []
                skips_out = list(dict.fromkeys(skips_out + present))
            elif added_leaves and existing_leaves:
                confirmed = []
                skips_out = list(skips_out)
                for k in present:
                    if k not in ukey_set:
                        unresolved.append({"item": id_minimal(_src(k)), "hint": "trakt_added_existing_split_ambiguous", "key": k})
                        reason_counts["trakt_added_existing_split_ambiguous"] = reason_counts.get("trakt_added_existing_split_ambiguous", 0) + 1
                        ukeys.append(k)
                ukeys = list(dict.fromkeys(ukeys))

        destinations: dict[str, Any] = {}
        confirmed_set = set(confirmed)
        aliased = 0
        for k in present:
            dest_item = _destination_for(k)
            if not dest_item:
                continue
            dest_event_key = _destination_event_key(dest_item)
            dest_plain_key = str(canonical_key(dest_item) or "") or dest_event_key.split("@", 1)[0]
            destinations[k] = {
                "key": dest_plain_key,
                "event_key": dest_event_key,
                "item": dest_item,
                "status": "added" if k in confirmed_set else "existing",
            }
            src_event_key = _source_event_key(_src(k))
            alias_payload = dict(destinations[k])
            alias_payload["key"] = dest_event_key
            alias_payload["plain_key"] = dest_plain_key
            if src_event_key and _alias_record(src_event_key, alias_payload, basis=str(_basis_for(k) or "")):
                aliased += 1
        if aliased:
            _alias_save()
            _dbg("alias_recorded", count=aliased, present=len(present))

        out: dict[str, Any] = {
            "ok": True,
            "count": len(confirmed),
            "confirmed_keys": confirmed,
            "presence_confirmed_keys": present,
            "confirmed_destinations": destinations,
            "unresolved": unresolved,
            "unresolved_keys": ukeys,
            "skipped_keys": skips_out,
            "reason_counts": reason_counts,
            "ambiguous": bool(ambiguous_set),
        }
        _info(
            "trakt_write_result",
            op="add",
            attempted=len(source_keys),
            sent=len(sent_keys),
            added=len(confirmed),
            present=len(present),
            unresolved=len(ukeys),
            skipped=len(skips_out),
            ambiguous=bool(ambiguous_set),
            ambiguous_leaves=len(ambiguous_set),
            unmatched_scopes=int(unmatched),
        )
        return out

    if not body:
        _info("write_skipped", op="add", reason="empty_payload", unresolved=len(unresolved))
        return _result()

    _dbg("write_prepare", op="add", movies=len(body.get("movies") or []), shows=len(body.get("shows") or []), seasons=len(body.get("seasons") or []), episodes=len(body.get("episodes") or []), ids=len(body.get("ids") or []))
    r = request_with_retries(
        sess,
        "POST",
        URL_ADD,
        headers=headers,
        json=body,
        timeout=write_timeout,
        max_retries=retries,
    )

    if r.status_code in (200, 201):
        d = r.json() or {}
        added = d.get("added") or {}
        existing = d.get("existing") or {}
        added_leaves = int(added.get("movies") or 0) + int(added.get("episodes") or 0)
        existing_leaves = int(existing.get("movies") or 0) + int(existing.get("episodes") or 0)
        nf = d.get("not_found") or {}
        nf_scopes = _not_found_scope_count(nf)

        matched_keys, reasons, expanded, matched_scopes, unmatched_details = _correlate_not_found(nf, req_index)

        retry_keys = [k for k in matched_keys if k in set(sent_keys)]
        recovered, retry_dest, retry_searches = _retry_failed_episodes(
            adapter,
            retry_keys,
            _src,
            timeout=timeout,
            retries=retries,
            write_timeout=write_timeout,
        )
        if retry_keys:
            _info("simkl_trakt_resolution_plan", retry_candidates=len(retry_keys), exact_search_requests=retry_searches)
        for k in matched_keys:
            if k in recovered:
                continue
            unresolved.append({"item": id_minimal(_src(k)), "hint": reasons.get(k) or "trakt_episode_not_found", "key": k})
        if expanded:
            _info("trakt_not_found_expanded", op="add", parent_not_found_expanded=expanded, matched=len(matched_keys))

        unmatched_scopes = max(0, nf_scopes - matched_scopes)
        for k in recovered:
            rec = (req_index.get("src_items") or {}).get(k)
            if isinstance(rec, dict) and k in retry_dest:
                rec["destination"] = dict(retry_dest[k])

        sent_set = set(sent_keys)
        leaf_kind: Mapping[str, str] = req_index.get("leaf_kind") or {}
        matched_set = set(matched_keys) & sent_set
        resolved_set = matched_set | set(skipped_keys or [])

        ambiguous_keys: set[str] = set()
        for scope in unmatched_details:
            for k in scope.get("candidates") or []:
                if k in sent_set and k not in resolved_set:
                    ambiguous_keys.add(k)

        for kind, bucket in (("movie", "movies"), ("episode", "episodes")):
            group = [k for k in sent_keys if leaf_kind.get(k, "episode") == kind]
            if not group:
                continue
            reported = int(added.get(bucket) or 0) + int(existing.get(bucket) or 0)
            resolved_in_group = len([k for k in group if k in resolved_set])
            flagged_in_group = len([k for k in group if k in ambiguous_keys])
            shortfall = len(group) - (reported + resolved_in_group + flagged_in_group)
            if shortfall <= 0:
                continue
            _dbg("not_found_group_shortfall", media=bucket, sent=len(group), reported=reported,
                 resolved=resolved_in_group, flagged=flagged_in_group, shortfall=shortfall)
            for k in group:
                if k not in resolved_set:
                    ambiguous_keys.add(k)

        result = _result(
            ambiguous_keys=ambiguous_keys,
            unmatched=unmatched_scopes,
            added_leaves=added_leaves,
            existing_leaves=existing_leaves,
        )

        present_set = set(result["presence_confirmed_keys"])
        confirmed_dest = [
            _sanitize_destination(rec["destination"])
            for k, rec in (req_index.get("src_items") or {}).items()
            if k in present_set and isinstance(rec, Mapping) and isinstance(rec.get("destination"), Mapping)
        ]
        if confirmed_dest:
            _cache_merge_from_source_items(adapter, confirmed_dest)
            if _history_collection_enabled(adapter):
                coll_body = _history_body_to_collection(body, _history_collection_types(adapter))
                if coll_body:
                    try:
                        rc = request_with_retries(
                            sess,
                            "POST",
                            URL_COLL_ADD,
                            headers=headers,
                            json=coll_body,
                            timeout=write_timeout,
                            max_retries=retries,
                        )
                        if rc.status_code == 420:
                            _warn("rate_limit", op="add", target="collection", status=420)
                            _record_limit_error("collection")
                        elif rc.status_code not in (200, 201):
                            _warn("write_failed", op="add", target="collection", status=rc.status_code, body=((rc.text or "")[:200]))
                    except Exception as e:
                        _warn("write_failed", op="add", target="collection", error=str(e))
        return result

    if r.status_code == 409:
        _dbg("write_item_skipped", op="add", reason="duplicate", status=409, body=((r.text or "")[:200]))
        skipped_keys = list(dict.fromkeys(list(skipped_keys or []) + sent_keys))
        _bust_index_cache("write:add:duplicate")
        return _result()

    if r.status_code == 420:
        _warn("rate_limit", op="add", status=420)
        _record_limit_error("history")
        for k in sent_keys:
            unresolved.append({"item": id_minimal(_src(k)), "hint": "trakt_limit", "key": k})
        return _result()

    _warn("write_failed", op="add", status=r.status_code, body=((r.text or "")[:200]))
    for k in sent_keys:
        unresolved.append({"item": id_minimal(_src(k)), "hint": f"http:{r.status_code}", "key": k})
    return _result()


def _remove_exact(
    adapter: Any,
    plan: list[dict[str, Any]],
    *,
    timeout: float,
    retries: int,
    chunk_size: int,
) -> tuple[list[str], list[str], list[dict[str, Any]]]:
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    confirmed_keys: list[str] = []
    removed_destination_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []

    by_id: dict[int, dict[str, Any]] = {}
    for entry in plan:
        by_id[int(entry["history_id"])] = entry

    ids = sorted(by_id.keys())
    for i in range(0, len(ids), max(1, chunk_size)):
        batch = ids[i : i + max(1, chunk_size)]
        _dbg("write_prepare", op="remove", mode="exact_ids", ids=len(batch))
        r = request_with_retries(
            sess,
            "POST",
            URL_REMOVE,
            headers=headers,
            json={"ids": list(batch)},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code not in (200, 201):
            _warn("write_failed", op="remove", mode="exact_ids", status=r.status_code, body=((r.text or "")[:200]))
            for hid in batch:
                entry = by_id[hid]
                unresolved.append({"item": entry["item"], "hint": f"http:{r.status_code}", "key": entry["source_key"]})
            continue
        try:
            payload = r.json() or {}
        except Exception:
            payload = {}
        deleted = payload.get("deleted") or payload.get("removed") or {}
        not_found = payload.get("not_found") or {}
        nf_ids = _not_found_ids(not_found)
        deleted_events = _deleted_event_count(deleted)
        candidates = [hid for hid in batch if hid not in nf_ids]

        _dbg(
            "write_result",
            op="remove",
            mode="exact_ids",
            submitted_ids=len(batch),
            candidate_ids=len(candidates),
            deleted_events=deleted_events,
            already_absent=len(nf_ids),
        )

        if deleted_events != len(candidates):
            _warn(
                "remove_unconfirmed",
                mode="exact_ids",
                submitted_ids=len(batch),
                candidate_ids=len(candidates),
                deleted_events=deleted_events,
                already_absent=len(nf_ids),
            )
            for hid in candidates:
                entry = by_id[hid]
                unresolved.append({"item": entry["item"], "hint": "trakt_history_remove_unconfirmed", "key": entry["source_key"]})
            resolved_ids = sorted(nf_ids)
        else:
            resolved_ids = list(batch)

        for hid in resolved_ids:
            entry = by_id[hid]
            confirmed_keys.append(entry["source_key"])
            if entry.get("destination_key"):
                removed_destination_keys.append(str(entry["destination_key"]))

        if resolved_ids:
            _cache_remove_event_keys(
                [str(by_id[hid].get("destination_event_key") or "") for hid in resolved_ids],
                resolved_ids,
            )
            _cache_remove_source_items(
                adapter,
                [by_id[hid]["item"] for hid in resolved_ids if isinstance(by_id[hid].get("item"), Mapping)],
            )
            _alias_forget([by_id[hid]["source_event_key"] for hid in resolved_ids if by_id[hid].get("source_event_key")])

    return confirmed_keys, removed_destination_keys, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    timeout = float(_cfg_num(adapter, "timeout", 10, float))
    retries = int(_cfg_num(adapter, "max_retries", 3, int))
    chunk_size = int(_cfg_num(adapter, "history_chunk_size", 100, int))
    items_list = list(items or [])
    if not items_list:
        _info("write_skipped", op="remove", reason="empty_payload", unresolved=0)
        return {"ok": True, "count": 0, "confirmed_keys": [], "unresolved": [], "unresolved_keys": []}

    aliases = _alias_load()
    exact_plan: list[dict[str, Any]] = []
    passthrough: list[Mapping[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    lookup_needed: list[dict[str, Any]] = []

    for it in items_list:
        plain_key = str(canonical_key(it) or "")
        source_event_key, rec, miss_reason = _alias_lookup(aliases, it)
        if not isinstance(rec, Mapping):
            if not _requires_alias_removal(it):
                passthrough.append(it)
                continue
            if _alias_rebuild_incomplete():
                unresolved.append({"item": id_minimal(it), "hint": "trakt_history_alias_rebuild_pending", "key": plain_key})
                continue
            exact = _exact_destination_for_removal(it, adapter, timeout=timeout, retries=retries)
            if exact is None:
                unresolved.append({"item": id_minimal(it), "hint": miss_reason, "key": plain_key})
                continue
            lookup_needed.append({
                "source_key": plain_key,
                "source_event_key": "",
                "destination_event_key": "",
                "destination_key": exact.get("destination_key") or "",
                "item": id_minimal(it),
                "alias": exact,
            })
            continue
        entry = {
            "source_key": plain_key,
            "source_event_key": source_event_key,
            "destination_event_key": str(rec.get("destination_event_key") or ""),
            "destination_key": str(rec.get("destination_key") or str(rec.get("destination_event_key") or "").split("@", 1)[0]),
            "item": id_minimal(it),
            "alias": dict(rec),
        }
        hid = _int_or_none(rec.get("history_id"))
        if hid is not None:
            entry["history_id"] = hid
            exact_plan.append(entry)
        else:
            lookup_needed.append(entry)

    for entry in lookup_needed:
        hid, reason = _lookup_history_id(adapter, entry["alias"], timeout=timeout, retries=retries)
        if hid is None:
            unresolved.append({"item": entry["item"], "hint": reason, "key": entry["source_key"]})
            continue
        entry["history_id"] = hid
        exact_plan.append(entry)

    confirmed_keys: list[str] = []
    removed_destination_keys: list[str] = []
    if exact_plan:
        ex_keys, ex_dest, ex_unresolved = _remove_exact(
            adapter, exact_plan, timeout=timeout, retries=retries, chunk_size=chunk_size
        )
        confirmed_keys.extend(ex_keys)
        removed_destination_keys.extend(ex_dest)
        unresolved.extend(ex_unresolved)

    exact_confirmed = len(confirmed_keys)
    ok = exact_confirmed
    passthrough_ok, passthrough_exact, passthrough_coordinate = _remove_by_coordinates(
        adapter,
        passthrough,
        sess=sess,
        headers=headers,
        timeout=timeout,
        retries=retries,
        chunk_size=chunk_size,
        unresolved=unresolved,
        confirmed_keys=confirmed_keys,
    )
    ok += passthrough_ok

    ukeys: list[str] = []
    reason_counts: dict[str, int] = {}
    for u in unresolved:
        if not isinstance(u, Mapping):
            continue
        reason = str(u.get("hint") or "unknown")
        reason_counts[reason] = reason_counts.get(reason, 0) + 1
        k = str(u.get("key") or "")
        if not k:
            inner = u.get("item")
            obj: Mapping[str, Any] = inner if isinstance(inner, Mapping) else u
            try:
                k = str(canonical_key(obj) or "")
            except Exception:
                k = ""
        if k:
            ukeys.append(k)
    ukeys = list(dict.fromkeys(ukeys))
    confirmed_keys = [k for k in dict.fromkeys(confirmed_keys) if k]

    _info(
        "write_done",
        op="remove",
        ok=not unresolved,
        removed=len(confirmed_keys),
        exact=exact_confirmed + passthrough_exact,
        coordinate=passthrough_coordinate,
        applied=len(confirmed_keys),
        unresolved=len(unresolved),
    )
    return {
        "ok": True,
        "count": len(confirmed_keys),
        "confirmed_keys": confirmed_keys,
        "removed_destination_keys": list(dict.fromkeys(removed_destination_keys)),
        "removed_exact": exact_confirmed + passthrough_exact,
        "removed_coordinate": passthrough_coordinate,
        "unresolved": unresolved,
        "unresolved_keys": ukeys,
        "reason_counts": reason_counts,
    }


def _remove_by_coordinates(
    adapter: Any,
    items_list: list[Mapping[str, Any]],
    *,
    sess: Any,
    headers: Mapping[str, Any],
    timeout: float,
    retries: int,
    chunk_size: int,
    unresolved: list[dict[str, Any]],
    confirmed_keys: list[str],
) -> tuple[int, int, int]:
    if not items_list:
        return 0, 0, 0
    ok = 0
    exact_removed = 0
    coordinate_removed = 0
    confirmed_history_ids: set[int] = set()
    confirmed_scope_events: list[tuple[str, str]] = []
    for part in _chunked_items(items_list, chunk_size):
        body, part_unresolved, accepted_keys, accepted_minimals, raw_id_map = _batch_remove(adapter, part)
        unresolved.extend(part_unresolved)
        if not body:
            continue
        ids_body = {"ids": list(body.get("ids") or [])} if body.get("ids") else {}
        coord_body = {k: v for k, v in body.items() if k != "ids"}
        _dbg(
            "write_prepare",
            op="remove",
            chunk_size=chunk_size,
            chunk_items=len(part),
            movies=len(coord_body.get("movies") or []),
            shows=len(coord_body.get("shows") or []),
            seasons=len(coord_body.get("seasons") or []),
            episodes=len(coord_body.get("episodes") or []),
            ids=len(ids_body.get("ids") or []),
        )
        raw_key_by_id: dict[int, str] = {}
        raw_minimal_by_id: dict[int, Mapping[str, Any]] = {}
        for hid, minimal in (raw_id_map or {}).items():
            hid_i = _int_or_none(hid)
            if hid_i is None or not isinstance(minimal, Mapping):
                continue
            raw_key_by_id[hid_i] = str(key_of(minimal) or "")
            raw_minimal_by_id[hid_i] = minimal
        raw_keys = {k for k in raw_key_by_id.values() if k}

        confirmed_ids: list[int] = []
        if ids_body:
            r_ids = request_with_retries(
                sess, "POST", URL_REMOVE, headers=headers, json=ids_body,
                timeout=timeout, max_retries=retries,
            )
            if r_ids.status_code in (200, 201):
                try:
                    d_ids = r_ids.json() or {}
                except Exception:
                    d_ids = {}
                nf_id_set = _not_found_ids(d_ids.get("not_found") or {})
                deleted_events = _deleted_event_count(d_ids.get("deleted") or d_ids.get("removed") or {})
                submitted_ids = [i for i in (_int_or_none(x) for x in (ids_body.get("ids") or [])) if i is not None]
                candidate_ids = [i for i in submitted_ids if i not in nf_id_set]

                _dbg(
                    "write_result",
                    op="remove",
                    mode="raw_ids",
                    submitted_ids=len(submitted_ids),
                    candidate_ids=len(candidate_ids),
                    deleted_events=deleted_events,
                    already_absent=len(nf_id_set),
                )

                if deleted_events == len(candidate_ids):
                    confirmed_ids = list(candidate_ids) + sorted(nf_id_set)
                else:
                    _warn(
                        "remove_unconfirmed",
                        mode="raw_ids",
                        submitted_ids=len(submitted_ids),
                        candidate_ids=len(candidate_ids),
                        deleted_events=deleted_events,
                        already_absent=len(nf_id_set),
                    )
                    for hid in candidate_ids:
                        unresolved.append({
                            "item": dict(raw_minimal_by_id.get(hid) or {}),
                            "hint": "trakt_history_remove_unconfirmed",
                            "key": raw_key_by_id.get(hid, ""),
                        })
                    confirmed_ids = sorted(nf_id_set)
            else:
                _warn("write_failed", op="remove", mode="raw_ids", status=r_ids.status_code, body=((r_ids.text or "")[:200]))
                for hid in raw_key_by_id:
                    unresolved.append({
                        "item": dict(raw_minimal_by_id.get(hid) or {}),
                        "hint": f"http:{r_ids.status_code}",
                        "key": raw_key_by_id.get(hid, ""),
                    })

        confirmed_scope_keys: list[str] = []
        coordinate_candidates: list[str] = []
        nf_unresolved: list[dict[str, Any]] = []
        if coord_body:
            r_coord = request_with_retries(
                sess, "POST", URL_REMOVE, headers=headers, json=coord_body,
                timeout=timeout, max_retries=retries,
            )
            if r_coord.status_code in (200, 201):
                try:
                    d_coord = r_coord.json() or {}
                except Exception:
                    d_coord = {}
                nf_coord = d_coord.get("not_found") or {}
                deleted_scope_events = _deleted_event_count(d_coord.get("deleted") or d_coord.get("removed") or {})
                nf_unresolved = [u for u in _unresolved_from_not_found(nf_coord) if isinstance(u, Mapping)]

                nf_scope_keys: set[str] = set()
                for u in nf_unresolved:
                    explicit = str(u.get("key") or "")
                    if explicit:
                        nf_scope_keys.add(explicit)
                        continue
                    inner = u.get("item")
                    if isinstance(inner, Mapping):
                        try:
                            derived = str(canonical_key(inner) or "")
                        except Exception:
                            derived = ""
                        if derived:
                            nf_scope_keys.add(derived)

                unresolved.extend(nf_unresolved)

                coordinate_candidates = [
                    k for k in accepted_keys
                    if k and k not in raw_keys and k not in nf_scope_keys
                ]

                _dbg(
                    "write_result",
                    op="remove",
                    mode="coordinates",
                    candidate_ids=len(coordinate_candidates),
                    deleted_events=deleted_scope_events,
                    not_found_ids=len(nf_scope_keys),
                )

                if coordinate_candidates:
                    if deleted_scope_events == len(coordinate_candidates):
                        confirmed_scope_keys = list(coordinate_candidates)
                    else:
                        _warn(
                            "remove_unconfirmed",
                            mode="coordinates",
                            candidate_ids=len(coordinate_candidates),
                            deleted_events=deleted_scope_events,
                            not_found_ids=len(nf_scope_keys),
                        )
                        candidate_set = set(coordinate_candidates)
                        for m in accepted_minimals:
                            if not isinstance(m, Mapping):
                                continue
                            mk = str(key_of(m) or "")
                            if mk in candidate_set:
                                unresolved.append({
                                    "item": m,
                                    "hint": "trakt_history_remove_unconfirmed",
                                    "key": mk,
                                })
            else:
                _warn("write_failed", op="remove", mode="coordinates", status=r_coord.status_code, body=((r_coord.text or "")[:200]))
                for m in accepted_minimals:
                    if not isinstance(m, Mapping):
                        continue
                    mk = str(key_of(m) or "")
                    if mk and mk not in raw_keys:
                        unresolved.append({"item": m, "hint": f"http:{r_coord.status_code}", "key": mk})

        confirmed_raw_keys = [raw_key_by_id.get(hid, "") for hid in confirmed_ids]
        confirmed_raw_keys = [k for k in confirmed_raw_keys if k]

        if confirmed_ids:
            _cache_remove_event_keys([], confirmed_ids)
        if confirmed_scope_keys:
            scope_set = set(confirmed_scope_keys)
            scope_minimals = [
                m for m in accepted_minimals
                if isinstance(m, Mapping) and str(key_of(m) or "") in scope_set
            ]
            if scope_minimals:
                _cache_remove_source_items(adapter, scope_minimals)
            watched_by_key: dict[str, str] = {}
            for it in part:
                if not isinstance(it, Mapping):
                    continue
                ck = str(canonical_key(it) or "")
                if ck in scope_set:
                    watched_by_key[ck] = _iso8601(it.get("watched_at") or it.get("watchedAt")) or ""
            for k in scope_set:
                confirmed_scope_events.append((k, watched_by_key.get(k, "")))

        part_confirmed_keys = confirmed_raw_keys + confirmed_scope_keys
        if part_confirmed_keys:
            confirmed_keys.extend(part_confirmed_keys)

        confirmed_history_ids |= set(confirmed_ids)
        exact_removed += len(confirmed_raw_keys)
        coordinate_removed += len(confirmed_scope_keys)
        ok += len(part_confirmed_keys)

        if not part_confirmed_keys and not nf_unresolved and not coordinate_candidates and not ids_body:
            _dbg("write_prepare", op="remove", reason="noop_response", chunk_items=len(part))

    if confirmed_history_ids:
        _alias_forget_by_history_ids(confirmed_history_ids)
    if confirmed_scope_events:
        _alias_forget_by_events(confirmed_scope_events)
    return ok, exact_removed, coordinate_removed

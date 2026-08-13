# /providers/sync/mdblist/_history.py
# MDBList watched history sync module (delta semantics)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import re
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, Sequence, TypeGuard

from cw_platform.anime_mapping.coordinates import translate
from cw_platform.anime_mapping.episodes import resolve_absolute, resolve_axis_coordinates, resolve_source_coordinate
from cw_platform.anime_mapping.service import (
    PAIR_FEATURE_OPTIONS_KEY,
    mapping_enabled_for_feature,
    runtime_pair_feature_options,
)
from cw_platform.anime_mapping.storage import query_edges
from cw_platform.history_events import history_sync_key, minimal_history_item
from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log

from .._mod_common import request_with_retries

from ._common import (
    MDBListFetchError,
    START_OF_TIME_ISO,
    STATE_DIR,
    state_file,
    as_epoch,
    as_iso,
    cfg_int,
    cfg_section,
    get_watermark,
    has_auth,
    iso_ok,
    iso_z,
    mdblist_request,
    max_iso,
    now_iso,
    read_json,
    write_json,
    save_watermark,
    update_watermark_if_new,
)


BASE = "https://api.mdblist.com"
URL_LAST_ACTIVITIES = f"{BASE}/sync/last_activities"
URL_LIST = f"{BASE}/sync/watched"
URL_UPSERT = f"{BASE}/sync/watched"
URL_REMOVE = f"{BASE}/sync/watched/remove"
URL_REMOVE_PLAYS = f"{BASE}/sync/watched/plays/remove"

PLAY_REPORT_MAX_ITEMS = 100

IMDB_RE = re.compile(r'^tt\d+$')
EP_TAG_RE = re.compile(r'^S\d{2}E\d{2}$')

def _imdb_ok(value: object) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    if not s:
        return None
    return s if IMDB_RE.match(s) else None


def _type_norm(value: object) -> str:
    t = str(value or "").strip().lower()
    if t.endswith("s") and t in ("movies", "shows", "seasons", "episodes"):
        t = t[:-1]
    return t or "movie"


def _sync_visible(item: Mapping[str, Any]) -> bool:
    return _type_norm(item.get("type")) in ("movie", "show", "season", "episode")



def _dbg(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "history", "error", msg, **fields)


def _log(msg: str, **fields: Any) -> None:
    _dbg(msg, **fields)


def _cache_path() -> Path:
    return state_file("mdblist_history.index.json")

_cfg = cfg_section
_cfg_int = cfg_int
_now_iso = now_iso
_iso_ok = iso_ok
_iso_z = iso_z
_as_epoch = as_epoch
_as_iso = as_iso
_max_iso = max_iso


def _rewatches_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    return bool(isinstance(cfg, Mapping) and cfg.get("_cw_history_rewatches"))


def _anime_mapping_enabled(adapter: Any) -> bool:
    cfg = getattr(adapter, "config", None)
    if not isinstance(cfg, Mapping):
        return False
    opts = runtime_pair_feature_options(cfg, "history")
    if PAIR_FEATURE_OPTIONS_KEY in cfg:
        return bool(opts.get("use_anime_mapping"))
    return bool(opts.get("use_anime_mapping") or mapping_enabled_for_feature(cfg, "history"))


def _anime_release_tag(adapter: Any) -> str:
    cfg = getattr(adapter, "config", None)
    block = cfg.get("anime_mapping") if isinstance(cfg, Mapping) else {}
    if isinstance(block, Mapping):
        tag = str(block.get("release_tag") or "").strip()
        if tag:
            return tag
    return "v3"


def _history_key(adapter: Any, item: Mapping[str, Any], fallback_key: Any = None) -> str:
    return history_sync_key(item, fallback_key, event_mode=_rewatches_enabled(adapter))


def _history_minimal(adapter: Any, item: Mapping[str, Any], fallback_key: Any = None) -> dict[str, Any]:
    return minimal_history_item(item, fallback_key, event_mode=_rewatches_enabled(adapter))


def _anime_evidence(item: Mapping[str, Any]) -> bool:
    if str(item.get("simkl_bucket") or "").strip().lower() == "anime":
        return True
    if isinstance(item.get("_cw_anime_map"), Mapping):
        return True
    if item.get("_simkl_episode_number") not in (None, ""):
        return True
    for ids in (item.get("show_ids"), item.get("ids")):
        if not isinstance(ids, Mapping):
            continue
        if any(str(ids.get(k) or "").strip() for k in ("mal", "anilist", "anidb", "kitsu")):
            return True
    return False


def _anime_native_ids(item: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for ids in (item.get("show_ids"), item.get("ids")):
        if not isinstance(ids, Mapping):
            continue
        for key in ("anidb", "mal", "anilist", "kitsu", "simkl"):
            value = str(ids.get(key) or "").strip()
            if value:
                out.setdefault(key, value)
    amap = item.get("_cw_anime_map")
    if isinstance(amap, Mapping):
        ns = str(amap.get("namespace") or "").strip().lower()
        tid = str(amap.get("target_id") or "").strip()
        if ns and tid:
            out.setdefault(ns, tid)
    return out


def _anime_absolute_from_item(item: Mapping[str, Any]) -> int | None:
    amap = item.get("_cw_anime_map")
    if isinstance(amap, Mapping):
        value = _as_int(amap.get("absolute"))
        if value is not None and value > 0:
            return value
    for field in ("_simkl_episode_number", "_trakt_number_abs", "number_abs"):
        value = _as_int(item.get(field))
        if value is not None and value > 0:
            return value
    if _anime_evidence(item):
        try:
            res = resolve_absolute(item)
        except Exception:
            res = None
        if res is not None and int(res.absolute) > 0:
            return int(res.absolute)
    return None


def _tmdb_show_id(item: Mapping[str, Any]) -> str:
    for ids in (item.get("show_ids"), item.get("ids")):
        if not isinstance(ids, Mapping):
            continue
        value = str(ids.get("tmdb") or "").strip()
        if value:
            return value
    return ""


def _season_from_scope(value: Any) -> int | None:
    text = str(value or "").strip().lower()
    if not text.startswith("s"):
        return None
    return _as_int(text[1:])


def _tmdb_coordinates_for_absolute(
    item: Mapping[str, Any],
    absolute: int,
    *,
    release_tag: str,
) -> set[tuple[int, int]]:
    native_ids = _anime_native_ids(item)
    show_tmdb = _tmdb_show_id(item)
    out: set[tuple[int, int]] = set()
    for namespace in ("anidb", "mal", "anilist", "kitsu"):
        ident = native_ids.get(namespace)
        if not ident:
            continue
        try:
            rows = query_edges(release_tag, namespace, ident)
        except Exception:
            rows = []
        for row in rows:
            if str(row.get("target_provider") or "").strip().lower() != "tmdb":
                continue
            if str(row.get("target_kind") or "").strip().lower() != "show":
                continue
            target_id = str(row.get("target_id") or "").strip()
            if show_tmdb and target_id and target_id != show_tmdb:
                continue
            season = _season_from_scope(row.get("target_scope"))
            if season is None or season < 0:
                continue
            episode = translate(row.get("source_range"), row.get("target_range"), absolute)
            if episode is None or int(episode) <= 0:
                continue
            out.add((int(season), int(episode)))
    return out


def _mdblist_coordinate_for_anime(
    item: Mapping[str, Any],
    *,
    release_tag: str,
) -> tuple[int, int] | None:
    if _type_norm(item.get("type")) != "episode" or not _anime_evidence(item):
        return None
    absolute = _anime_absolute_from_item(item)
    if absolute is None or absolute <= 0:
        return None

    tmdb_coords = _tmdb_coordinates_for_absolute(item, absolute, release_tag=release_tag)
    if len(tmdb_coords) == 1:
        return next(iter(tmdb_coords))

    native_ids = _anime_native_ids(item)
    try:
        src_coord = resolve_source_coordinate(native_ids, absolute, release_tag=release_tag)
    except Exception:
        src_coord = None
    if src_coord is not None:
        return int(src_coord.season), int(src_coord.episode)

    try:
        axis = resolve_axis_coordinates(native_ids, absolute, release_tag=release_tag)
    except Exception:
        axis = set()
    if len(axis) == 1:
        return next(iter(axis))
    if (1, absolute) in axis:
        return 1, absolute
    return None


def _with_mdblist_anime_coordinate(
    adapter: Any,
    item: Mapping[str, Any],
    *,
    release_tag: str | None = None,
) -> dict[str, Any]:
    out = dict(item or {})
    if not _anime_mapping_enabled(adapter):
        return out
    coord = _mdblist_coordinate_for_anime(out, release_tag=release_tag or _anime_release_tag(adapter))
    if coord is None:
        return out
    season, episode = coord
    if season < 0 or episode <= 0:
        return out
    out["season"] = season
    out["episode"] = episode
    out["title"] = f"S{season:02d}E{episode:02d}"
    return out


def _comparison_alias_keys_for_episode(
    adapter: Any,
    item: Mapping[str, Any],
    *,
    release_tag: str,
) -> dict[str, dict[str, Any]]:
    if _type_norm(item.get("type")) != "episode":
        return {}
    try:
        res = resolve_absolute(item, release_tag=release_tag)
    except Exception:
        res = None
    if res is None or int(res.absolute) <= 0:
        return {}

    native_ids = {str(res.namespace): str(res.target_id)}
    coords: set[tuple[int, int]] = set()
    try:
        coords.update(resolve_axis_coordinates(native_ids, int(res.absolute), release_tag=release_tag))
    except Exception:
        pass
    try:
        src_coord = resolve_source_coordinate(native_ids, int(res.absolute), release_tag=release_tag)
    except Exception:
        src_coord = None
    if src_coord is not None:
        coords.add((int(src_coord.season), int(src_coord.episode)))
    coords.add((1, int(res.absolute)))

    out: dict[str, dict[str, Any]] = {}
    current = (_as_int(item.get("season")), _as_int(item.get("episode")))
    for season, episode in coords:
        if season is None or episode is None or int(season) < 0 or int(episode) <= 0:
            continue
        if (int(season), int(episode)) == current:
            continue
        alias_item = dict(item)
        alias_item["season"] = int(season)
        alias_item["episode"] = int(episode)
        alias_item["title"] = f"S{int(season):02d}E{int(episode):02d}"
        key = _history_key(adapter, alias_item)
        if key:
            out[key] = _history_minimal(adapter, alias_item, key)
    return out


def _migrate_cache(items: Mapping[str, Any]) -> tuple[dict[str, Any], bool]:
    out: dict[str, Any] = {}
    changed = False
    for k, v in (items or {}).items():
        if not isinstance(v, Mapping):
            changed = True
            continue
        item = dict(v)
        if not _sync_visible(item):
            changed = True
            continue

        typ = _type_norm(item.get("type"))
        if typ == "episode":
            st = str(item.get("series_title") or "").strip()
            if not st:
                t = str(item.get("title") or "").strip()
                if t and not EP_TAG_RE.match(t):
                    item["series_title"] = t
                    changed = True
            try:
                s = int(item.get("season") or 0)
                e = int(item.get("episode") or 0)
            except Exception:
                s = 0
                e = 0
            if s > 0 and e > 0:
                tag = f"S{s:02d}E{e:02d}"
                if str(item.get("title") or "") != tag:
                    item["title"] = tag
                    changed = True
        elif typ == "season":
            st = str(item.get("series_title") or "").strip()
            if not st:
                t = str(item.get("title") or "").strip()
                if t:
                    item["series_title"] = t
                    changed = True
        ek = _event_key(item)
        if not ek:
            changed = True
            continue
        if ek != str(k):
            changed = True
        cur = out.get(ek)
        if not cur:
            out[ek] = item
            continue
        w_new = str(item.get('watched_at') or '')
        w_old = str(cur.get('watched_at') or '')
        if w_new >= w_old:
            out[ek] = item
            changed = True
    normalized, dropped = _normalize_rollups(out)
    if dropped["shows"] or dropped["seasons"]:
        changed = True
        _dbg(
            "index_reconcile",
            reason="rollups_pruned",
            shows=dropped["shows"],
            seasons=dropped["seasons"],
            scope="cache",
        )
    if len(normalized) != len(items):
        changed = True
    return normalized, changed


def _load_cache() -> dict[str, Any]:
    try:
        p = _cache_path()
        doc = read_json(p)
        if not isinstance(doc, dict):
            return {}
        items = dict(doc.get('items') or {})
        if not items:
            return {}
        migrated, changed = _migrate_cache(items)
        if changed:
            _save_cache(migrated)
        return migrated
    except Exception:
        return {}


def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        write_json(_cache_path(), doc)
    except Exception as e:
        _warn("cache_save_failed", error=str(e))


def _fetch_last_activities(adapter: Any, *, timeout: float, retries: int) -> dict[str, Any] | None:
    client = getattr(adapter, "client", None)
    if client and hasattr(client, "last_activities"):
        try:
            data = client.last_activities()
            if isinstance(data, Mapping) and "error" not in data and "status" not in data:
                return dict(data)
        except Exception:
            pass
        return None

    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        return None

    sess = adapter.client.session
    try:
        r = mdblist_request(
            adapter,
            "GET",
            URL_LAST_ACTIVITIES,
            params={"apikey": apikey},
            timeout=timeout,
            max_retries=retries,
        )
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            return dict(data) if isinstance(data, Mapping) else None
    except Exception:
        return None
    return None


def _base_key(item: Mapping[str, Any]) -> str:
    if (
        item.get("type") in ("episode", "season")
        and isinstance(item.get("show_ids"), Mapping)
        and item.get("season") is not None
    ):
        base_obj: dict[str, Any] = {
            "type": str(item.get("type")),
            "show_ids": dict(item.get("show_ids") or {}),
            "season": item.get("season"),
        }
        if item.get("type") == "episode" and item.get("episode") is not None:
            base_obj["episode"] = item.get("episode")
        return canonical_key(id_minimal(base_obj))
    return canonical_key(id_minimal(item))


def _event_key(item: Mapping[str, Any]) -> str | None:
    w = item.get("watched_at") or item.get("last_watched_at")
    if not _iso_ok(w):
        return None
    ts = _as_epoch(_iso_z(w))
    if not ts:
        return None
    return f"{_base_key(item)}@{ts}"


def _merge_event(dst: dict[str, Any], item: Mapping[str, Any]) -> str | None:
    ek = _event_key(item)
    if not ek:
        return None
    cur = dst.get(ek)
    if not cur:
        dst[ek] = dict(item)
        return ek
    w_new = str(item.get("watched_at") or "")
    w_old = str(cur.get("watched_at") or "")
    if w_new >= w_old:
        merged = dict(cur)
        merged.update(dict(item))
        dst[ek] = merged
    return ek


def _journal_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    if str(row.get("category") or "").strip().lower() != "watched":
        return None
    typ = _type_norm(row.get("item_type"))
    ids = _ids_pick(row.get("ids") or {})
    if not ids:
        return None
    out: dict[str, Any] = {"type": typ}
    if typ in ("season", "episode"):
        out["show_ids"] = ids
        out["ids"] = ids
        out["season"] = row.get("season")
        if typ == "episode":
            out["episode"] = row.get("episode")
    else:
        out["ids"] = ids
    title = str(row.get("title") or row.get("name") or "").strip()
    show_title = str(row.get("series_title") or row.get("show_title") or row.get("show") or "").strip()
    if typ == "show" and title:
        out["title"] = title
    elif typ == "season":
        if show_title:
            out["series_title"] = show_title
        elif title:
            out["series_title"] = title
    elif typ == "episode":
        if show_title:
            out["series_title"] = show_title
        elif title and not re.match(r"^s\d{1,3}e\d{1,3}$", title, flags=re.I):
            out["series_title"] = title
        if title:
            out["title"] = title
    status = str(row.get("status") or "").strip().lower()
    value_at = row.get("value_at") or row.get("action_at")
    if status == "added" and _iso_ok(value_at):
        out["watched_at"] = _iso_z(value_at)
    return out


def _remove_base_entries(items: dict[str, Any], base_item: Mapping[str, Any]) -> int:
    typ = _type_norm(base_item.get("type"))
    show_key = _show_rollup_key(base_item) if typ in ("show", "season", "episode") else None
    try:
        season_no = int(base_item.get("season") or 0) if typ == "season" else 0
    except Exception:
        season_no = 0
    try:
        episode_no = int(base_item.get("episode") or 0) if typ == "episode" else 0
    except Exception:
        episode_no = 0

    try:
        target = _base_key(base_item)
    except Exception:
        return 0
    if not target:
        return 0
    removed = 0
    for key, value in list((items or {}).items()):
        try:
            if not isinstance(value, Mapping):
                current = ""
                cur_typ = ""
                cur_show_key = None
                cur_season_no = 0
            else:
                current = _base_key(value)
                cur_typ = _type_norm(value.get("type"))
                cur_show_key = _show_rollup_key(value)
                try:
                    cur_season_no = int(value.get("season") or 0) if cur_typ in ("season", "episode") else 0
                except Exception:
                    cur_season_no = 0
                try:
                    cur_episode_no = int(value.get("episode") or 0) if cur_typ == "episode" else 0
                except Exception:
                    cur_episode_no = 0
        except Exception:
            current = ""
            cur_typ = ""
            cur_show_key = None
            cur_season_no = 0
            cur_episode_no = 0

        remove = bool(current and current == target)
        if not remove and typ == "show" and show_key and cur_show_key == show_key:
            remove = True
        if (
            not remove
            and typ == "season"
            and show_key
            and season_no > 0
            and cur_show_key == show_key
            and cur_typ in ("season", "episode")
            and cur_season_no == season_no
        ):
            remove = True
        if (
            not remove
            and typ == "episode"
            and show_key
            and season_no >= 0
            and episode_no > 0
            and cur_show_key == show_key
            and cur_typ == "episode"
            and cur_season_no == season_no
            and cur_episode_no == episode_no
        ):
            remove = True

        if remove:
            items.pop(key, None)
            removed += 1
    return removed


def _apply_journal_rows(
    cached: Mapping[str, Any],
    rows: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], dict[str, int], str | None]:
    merged: dict[str, Any] = {
        str(k): dict(v) for k, v in (cached or {}).items() if isinstance(v, Mapping)
    }
    stats = {"removed": 0}
    latest_seen: str | None = None

    def _sort_key(row: Mapping[str, Any]) -> str:
        return str(row.get("action_at") or row.get("value_at") or "")

    for row in sorted((r for r in (rows or []) if isinstance(r, Mapping)), key=_sort_key):
        item = _journal_item(row)
        if not item:
            continue
        latest_seen = _max_iso(latest_seen, row.get("action_at") or row.get("value_at"))
        status = str(row.get("status") or "").strip().lower()
        if status == "removed":
            stats["removed"] += _remove_base_entries(merged, item)
    return merged, stats, latest_seen

def _show_rollup_key(item: Mapping[str, Any]) -> str | None:
    ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    if not isinstance(ids, Mapping):
        return None
    picked = _ids_pick(ids)
    for k in ("tmdb", "imdb", "trakt", "tvdb", "kitsu"):
        v = picked.get(k)
        if v is not None and v != "":
            return f"{k}:{v}"
    return None


def _normalize_rollups(items: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, int]]:
    src: dict[str, Any] = {
        str(k): dict(v) for k, v in (items or {}).items() if isinstance(v, Mapping)
    }
    if not src:
        return {}, {"shows": 0, "seasons": 0}

    episode_children: set[tuple[str, int]] = set()
    episode_show_children: set[str] = set()
    season_children: set[str] = set()

    for item in src.values():
        typ = _type_norm(item.get("type"))
        show_key = _show_rollup_key(item)
        if not show_key:
            continue
        if typ == "season":
            season_children.add(show_key)
            continue
        if typ != "episode":
            continue
        episode_show_children.add(show_key)
        season_children.add(show_key)
        try:
            season_no = int(item.get("season") or 0)
        except Exception:
            season_no = 0
        if season_no > 0:
            episode_children.add((show_key, season_no))

    out: dict[str, Any] = {}
    dropped = {"shows": 0, "seasons": 0}

    for key, item in src.items():
        typ = _type_norm(item.get("type"))
        if typ == "season":
            show_key = _show_rollup_key(item)
            try:
                season_no = int(item.get("season") or 0)
            except Exception:
                season_no = 0
            if show_key and show_key in episode_show_children:
                dropped["seasons"] += 1
                continue
            if show_key and season_no > 0 and (show_key, season_no) in episode_children:
                dropped["seasons"] += 1
                continue
        elif typ == "show":
            show_key = _show_rollup_key(item)
            if show_key and show_key in season_children:
                dropped["shows"] += 1
                continue
        out[key] = item

    return out, dropped

def _ids_pick(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids_raw: dict[str, Any] = dict(obj.get('ids') or {})
    out: dict[str, Any] = {}
    for k in ('tmdb', 'imdb', 'tvdb', 'trakt', 'kitsu', 'mdblist'):
        v = ids_raw.get(k) or obj.get(k) or obj.get(f'{k}_id')
        if v is None or v == '':
            continue
        if k == 'imdb':
            vv = _imdb_ok(v)
            if not vv:
                continue
            out[k] = vv
            continue
        if k == 'mdblist':
            out[k] = str(v).strip().lower()
            continue
        if k in ('tmdb', 'tvdb', 'trakt'):
            try:
                out[k] = int(v)
            except Exception:
                continue
            continue
        out[k] = str(v)
    return out


def _copy_source_event_meta(src: Mapping[str, Any], dst: dict[str, Any]) -> None:
    for key in ("provider_event_id", "history_id", "_history_id", "_cw_event_key", "_cw_rewatch_sync"):
        if key in src and src.get(key) not in (None, ""):
            dst[key] = src.get(key)


def _copy_play_meta(row: Mapping[str, Any], out: dict[str, Any]) -> None:
    raw_id = (
        row.get("play_id")
        if row.get("play_id") not in (None, "")
        else row.get("history_id")
        if row.get("history_id") not in (None, "")
        else row.get("id")
    )
    if raw_id not in (None, ""):
        out["provider_event_id"] = str(raw_id).strip()
        out["history_id"] = str(raw_id).strip()
    if row.get("added") is not None:
        out["_mdblist_added"] = bool(row.get("added"))


def _row_movie(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        mv = row.get("movie") or {}
        ids = _ids_pick(mv)
        if not ids:
            return None
        out: dict[str, Any] = {"type": "movie", "ids": ids}
        title = str(mv.get("title") or mv.get("name") or "").strip()
        if title:
            out["title"] = title
        y = mv.get("year") or mv.get("release_year")
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)
        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_show(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sh = row.get("show") or {}
        ids = _ids_pick(sh)
        if not ids:
            return None
        out: dict[str, Any] = {"type": "show", "ids": ids}
        title = str(sh.get("title") or sh.get("name") or "").strip()
        if title:
            out["title"] = title
        y = sh.get("year") or sh.get("first_air_year")
        if not y:
            fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)
        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_season(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sv = row.get("season") or {}
        show = sv.get("show") or {}
        sids = _ids_pick(sv)
        sh_ids = _ids_pick(show)
        ids = sids or sh_ids
        if not ids:
            return None

        out: dict[str, Any] = {"type": "season", "ids": ids, "season": sv.get("number")}
        if sh_ids:
            out["show_ids"] = sh_ids
        show_title = str(show.get("title") or show.get("name") or "").strip()
        if not show_title:
            show_title = str(row.get("title") or "").strip()
        if show_title:
            out["series_title"] = show_title

        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)

        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_episode(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        ev = row.get("episode") or {}
        show = ev.get("show") or row.get("show") or {}
        eids = _ids_pick(ev) or _ids_pick(row)
        sh_ids = _ids_pick(show)
        ids = eids or sh_ids
        if not ids:
            return None

        num = (
            ev.get("number")
            if ev.get("number") is not None
            else ev.get("episode")
            if ev.get("episode") is not None
            else row.get("number")
            if row.get("number") is not None
            else row.get("episode")
        )
        season = ev.get("season") if ev.get("season") is not None else row.get("season")
        out: dict[str, Any] = {
            "type": "episode",
            "ids": ids,
            "season": season,
            "episode": num,
        }
        if sh_ids:
            out["show_ids"] = sh_ids
        show_title = str(show.get("title") or show.get("name") or "").strip()
        if not show_title:
            show_title = str(row.get("title") or "").strip()
        if show_title:
            out["series_title"] = show_title

        try:
            s = int(out.get("season") or 0)
            e = int(out.get("episode") or 0)
        except Exception:
            s = 0
            e = 0
        if s > 0 and e > 0:
            out["title"] = f"S{s:02d}E{e:02d}"
        elif show_title:
            out["title"] = show_title

        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)

        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _put_history_item(adapter: Any, out: dict[str, dict[str, Any]], item: Mapping[str, Any]) -> str | None:
    key = _history_key(adapter, item)
    if not key:
        return None
    out[key] = _history_minimal(adapter, item, key)
    return key


def _play_response_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = _type_norm(row.get("type"))
    if typ == "episode":
        item = _row_episode(row)
    elif typ == "show":
        item = _row_show(row)
    elif typ == "season":
        item = _row_season(row)
    else:
        movie_row = row if isinstance(row.get("movie"), Mapping) else {"movie": row, **dict(row)}
        item = _row_movie(movie_row)
    if item:
        _copy_play_meta(row, item)
    return item


def _build_event_index(
    adapter: Any,
    *,
    apikey: str,
    per_page: int,
    max_pages: int,
    timeout: float,
    retries: int,
) -> dict[str, dict[str, Any]]:
    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("history") if callable(prog_factory) else None

    out: dict[str, dict[str, Any]] = {}
    latest_seen: str | None = None
    offset = 0
    pages = 0
    tick = 0
    complete_fetch = True

    while True:
        params: dict[str, Any] = {"apikey": apikey, "offset": offset, "limit": per_page, "plays": "all"}
        try:
            r = mdblist_request(
                adapter,
                "GET",
                URL_LIST,
                params=params,
                timeout=timeout,
                max_retries=retries,
            )
        except Exception as e:
            _warn("http_failed", op="index", method="GET", url=URL_LIST, offset=offset, plays="all", error=f"{type(e).__name__}: {e}")
            complete_fetch = False
            break

        if r.status_code != 200:
            _warn("http_failed", op="index", method="GET", url=URL_LIST, status=r.status_code, offset=offset, plays="all", body=(r.text or '')[:160])
            complete_fetch = False
            break

        data = r.json() if (r.text or "").strip() else {}
        buckets = {
            "movies": data.get("movies") or [],
            "shows": data.get("shows") or [],
            "seasons": data.get("seasons") or [],
            "episodes": data.get("episodes") or [],
        }
        added = 0
        for bucket_name, parser in (
            ("movies", _row_movie),
            ("shows", _row_show),
            ("seasons", _row_season),
            ("episodes", _row_episode),
        ):
            for row in buckets[bucket_name]:
                m = parser(row) if isinstance(row, Mapping) else None
                if not m:
                    continue
                _copy_play_meta(row, m)
                if _put_history_item(adapter, out, m):
                    latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                    added += 1

        tick += added
        if prog and added:
            try:
                prog.tick(tick, total=max(tick, tick + 1))
            except Exception:
                pass

        pages += 1
        if pages >= max_pages:
            _warn("index_reconcile", reason="safety_cap_hit", strategy="event_snapshot", max_pages=max_pages)
            complete_fetch = False
            break

        pag = data.get("pagination") if isinstance(data, Mapping) else None
        if isinstance(pag, Mapping) and pag.get("has_more") is False:
            break

        rows_total = sum(len(v) for v in buckets.values() if isinstance(v, list))
        if rows_total == 0:
            break
        offset += per_page

    if not complete_fetch:
        _warn("index_reconcile", reason="incomplete_snapshot", strategy="event_snapshot", fetched=len(out), pages=pages)
        raise MDBListFetchError("history plays snapshot incomplete")

    if prog:
        try:
            prog.tick(len(out), total=len(out))
        except Exception:
            pass

    _dbg("index_fetch_counts", count=len(out), latest_seen=latest_seen or "-", source="plays", complete=complete_fetch)
    _info("index_done", count=len(out), source="plays")
    return out


def build_index(
    adapter: Any,
    *,
    per_page: int = 1000,
    max_pages: int = 250,
) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    event_mode = _rewatches_enabled(adapter)
    cached_raw = {} if event_mode else _load_cache()
    cached: dict[str, dict[str, Any]] = {
        str(k): dict(v) for k, v in (cached_raw or {}).items() if isinstance(v, Mapping)
    }

    apikey = str(cfg.get("api_key") or "").strip()
    if not has_auth(cfg):
        if event_mode:
            _warn("index_reconcile", reason="missing_auth", strategy="event_snapshot", rewatches=True)
            raise MDBListFetchError("history plays snapshot unavailable: missing auth")
        if cached:
            _dbg("index_cache_hit", source="cache", reason="missing_auth", count=len(cached))
            _info("index_done", count=len(cached), source="cache")
        else:
            _dbg("index_reconcile", reason="missing_auth", strategy="empty")
            _info("index_done", count=0, source="empty")
        return cached

    per_page = _cfg_int(cfg, "history_per_page", per_page)
    per_page = max(1, min(int(per_page), 5000))
    max_pages = _cfg_int(cfg, "history_max_pages", max_pages)
    max_pages = max(1, min(int(max_pages), 2000))

    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    if event_mode:
        return _build_event_index(
            adapter,
            apikey=apikey,
            per_page=per_page,
            max_pages=max_pages,
            timeout=timeout,
            retries=retries,
        )

    acts = _fetch_last_activities(adapter, timeout=timeout, retries=retries) or {}
    acts_candidates = []
    if isinstance(acts, Mapping):
        acts_candidates.extend([
            acts.get("watched_at"),
            acts.get("season_watched_at"),
            acts.get("episode_watched_at"),
            acts.get("history"),
            acts.get("updated_at"),
        ])
    acts_watched_iso: str | None = None
    for candidate in acts_candidates:
        if _iso_ok(candidate):
            acts_watched_iso = _max_iso(acts_watched_iso, _iso_z(candidate))

    journal_ts = acts.get("journal_at") if isinstance(acts, Mapping) else None
    journal_iso = _iso_z(journal_ts) if _iso_ok(journal_ts) else None
    wm = get_watermark("history")
    journal_wm = get_watermark("history_journal")
    force_baseline = False

    if acts_watched_iso and journal_iso and wm and journal_wm:
        a = _as_epoch(acts_watched_iso) or 0
        b = _as_epoch(wm) or 0
        jn = _as_epoch(journal_iso) or 0
        jw = _as_epoch(journal_wm) or 0
        if a <= b and jn <= jw:
            _dbg("index_cache_hit", source="cache", reason="activities_unchanged", watched_at=acts_watched_iso, journal_at=journal_iso, watermark=wm, journal_watermark=journal_wm, count=len(cached))
            _info("index_done", count=len(cached), source="cache")
            return cached
        if cached and jn > jw:
            journal_limit = max(50, min(per_page, 1000))
            journal = adapter.fetch_journal(since=journal_wm, limit=journal_limit, category="watched")
            journal_oldest = str(journal.get("journal_oldest_at") or "").strip()
            if _iso_ok(journal_oldest) and (_as_epoch(journal_wm) or 0) < (_as_epoch(_iso_z(journal_oldest)) or 0):
                _warn("index_reconcile", reason="journal_window_stale", strategy="full_replace", journal_oldest_at=journal_oldest, journal_watermark=journal_wm)
                force_baseline = True
            if not force_baseline:
                rows = journal.get("rows") if isinstance(journal.get("rows"), list) else []
                has_adds = any(
                    isinstance(row, Mapping)
                    and str(row.get("category") or "").strip().lower() == "watched"
                    and str(row.get("status") or "").strip().lower() == "added"
                    for row in rows
                )
                if has_adds:
                    _dbg(
                        "index_reconcile",
                        reason="journal_contains_adds",
                        strategy="full_merge",
                        rows=len(rows),
                        journal_at=journal_iso,
                        journal_watermark=journal_wm,
                    )
                else:
                    merged_journal, journal_stats, latest_journal_seen = _apply_journal_rows(cached, rows)
                    normalized_journal, dropped_journal = _normalize_rollups(merged_journal)
                    if dropped_journal["shows"] or dropped_journal["seasons"]:
                        _dbg("index_reconcile", reason="rollups_pruned", shows=dropped_journal["shows"], seasons=dropped_journal["seasons"], scope="journal")
                    _save_cache(normalized_journal)
                    update_watermark_if_new("history", _max_iso(latest_journal_seen, acts_watched_iso))
                    update_watermark_if_new("history_journal", journal_iso)
                    _dbg("index_reconcile", reason="journal_applied", strategy="journal", rows=len(rows), removed=journal_stats["removed"], journal_at=journal_iso, journal_watermark=journal_wm)
                    _info("index_done", count=len(normalized_journal), source="journal")
                    return normalized_journal

    if acts_watched_iso and journal_iso and (not journal_wm) and cached:
        force_baseline = True

    if acts_watched_iso and (not wm) and cached and not force_baseline:
        save_watermark("history", acts_watched_iso)
        if journal_iso:
            save_watermark("history_journal", journal_iso)
        _dbg("index_cache_hit", source="cache", reason="baseline_watermark_set", watermark=acts_watched_iso, count=len(cached))
        _info("index_done", count=len(cached), source="cache")
        return cached

    since_req: str | None = None

    if acts_watched_iso:
        _dbg("index_reconcile", reason="activities_changed", strategy="full_merge", watched_at=acts_watched_iso, journal_at=journal_iso or "-", watermark=wm or "-", journal_watermark=journal_wm or "-")
    else:
        _dbg("index_reconcile", reason="baseline_fetch", strategy="baseline", journal_at=journal_iso or "-")

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("history") if callable(prog_factory) else None

    out: dict[str, dict[str, Any]] = {}
    latest_seen: str | None = None
    offset = 0
    pages = 0
    tick = 0
    complete_fetch = True
    while True:
        params: dict[str, Any] = {"apikey": apikey, "offset": offset, "limit": per_page}
        if since_req:
            params["since"] = since_req
        try:
            r = mdblist_request(
                adapter,
                "GET",
                URL_LIST,
                params=params,
                timeout=timeout,
                max_retries=retries,
            )
        except Exception as e:
            _warn("http_failed", op="index", method="GET", url=URL_LIST, offset=offset, error=f"{type(e).__name__}: {e}")
            complete_fetch = False
            break

        if r.status_code != 200:
            _warn("http_failed", op="index", method="GET", url=URL_LIST, status=r.status_code, offset=offset, body=(r.text or '')[:160])
            complete_fetch = False
            break

        data = r.json() if (r.text or "").strip() else {}
        buckets = {
            "movies": data.get("movies") or [],
            "shows": data.get("shows") or [],
            "seasons": data.get("seasons") or [],
            "episodes": data.get("episodes") or [],
        }
        added = 0
        for row in buckets["movies"]:
            m = _row_movie(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["shows"]:
            m = _row_show(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["seasons"]:
            m = _row_season(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["episodes"]:
            m = _row_episode(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1

        tick += added
        if prog and added:
            try:
                prog.tick(tick, total=max(tick, tick + 1))
            except Exception:
                pass

        pages += 1
        if pages >= max_pages:
            _warn("index_reconcile", reason="safety_cap_hit", strategy="delta", max_pages=max_pages)
            complete_fetch = False
            break

        pag = data.get("pagination") if isinstance(data, Mapping) else None
        if isinstance(pag, Mapping) and pag.get("has_more") is False:
            break

        rows_total = sum(len(v) for v in buckets.values() if isinstance(v, list))
        if rows_total == 0:
            break
        offset += per_page
        
    normalized_out, dropped_out = _normalize_rollups(out)
    if dropped_out["shows"] or dropped_out["seasons"]:
        _dbg(
            "index_reconcile",
            reason="rollups_pruned",
            shows=dropped_out["shows"],
            seasons=dropped_out["seasons"],
            scope="delta",
        )

    if normalized_out and (force_baseline or complete_fetch):
        # /sync/watched is fetched as a full current snapshot here.
        merged_base = {str(k): dict(v) for k, v in normalized_out.items()}
    else:
        merged_base = dict(cached)
        if normalized_out:
            for k, v in normalized_out.items():
                merged_base[str(k)] = dict(v)

    merged, dropped_merged = _normalize_rollups(merged_base)
    if dropped_merged["shows"] or dropped_merged["seasons"]:
        _dbg(
            "index_reconcile",
            reason="rollups_pruned",
            shows=dropped_merged["shows"],
            seasons=dropped_merged["seasons"],
            scope="merged",
        )

    if out or force_baseline or dropped_merged["shows"] or dropped_merged["seasons"]:
        _save_cache(merged)

    update_watermark_if_new("history", latest_seen or acts_watched_iso)
    if journal_iso:
        update_watermark_if_new("history_journal", journal_iso)

    if prog:
        try:
            prog.tick(len(out), total=len(out))
        except Exception:
            pass

    _dbg("index_fetch_counts", count=len(out), latest_seen=latest_seen or "-", watermark=get_watermark("history") or "-", source="current")
    _info("index_done", count=len(merged), source="current")
    return merged


def destination_comparison_view(index: Mapping[str, Any], adapter: Any = None) -> dict[str, Any]:
    if adapter is None or not _anime_mapping_enabled(adapter):
        return dict(index or {})

    release_tag = _anime_release_tag(adapter)
    out: dict[str, Any] = {}
    aliases = 0
    remapped = 0
    for key, item in (index or {}).items():
        if not isinstance(item, Mapping):
            out[str(key)] = item
            continue
        alias_items = _comparison_alias_keys_for_episode(adapter, item, release_tag=release_tag)
        if alias_items:
            remapped += 1
            canonical_alias = next(iter(alias_items.values()))
            out[str(key)] = dict(canonical_alias)
            for alias_key, alias_item in alias_items.items():
                if alias_key in out:
                    continue
                out[alias_key] = alias_item
                aliases += 1
            continue
        out[str(key)] = dict(item)
    if aliases or remapped:
        _dbg("anime_comparison_view", aliases=aliases, remapped=remapped, before=len(index or {}), after=len(out))
    return out


def _stable_show_key(ids: Mapping[str, Any]) -> str:
    keep = {k: ids.get(k) for k in ("tmdb", "imdb", "tvdb", "trakt") if ids.get(k) is not None}
    return json.dumps(keep, sort_keys=True)


def _tmdb_id_value(ids: Mapping[str, Any]) -> int | None:
    raw = ids.get("tmdb") if isinstance(ids, Mapping) else None
    if raw in (None, ""):
        return None
    try:
        value = int(raw)
    except Exception:
        return None
    return value if value > 0 else None


def _flat_episode_tmdb_row(
    item: Mapping[str, Any],
    ids: Mapping[str, Any],
    show_ids: Mapping[str, Any],
    watched_iso: str | None,
) -> dict[str, Any] | None:
    tmdb = _tmdb_id_value(ids)
    if tmdb is None:
        return None

    show_tmdb = _tmdb_id_value(show_ids)
    if show_tmdb is None or show_tmdb == tmdb:
        return None

    row: dict[str, Any] = {"ids": {"tmdb": tmdb}}
    if watched_iso:
        row["watched_at"] = watched_iso
    return row


def _as_int(value: Any) -> int | None:
    try:
        return int(value) if value is not None else None
    except Exception:
        return None


def _unresolved_entry(item: Mapping[str, Any], hint: str) -> dict[str, Any]:
    minimal_item = id_minimal(item)
    for field in ("provider_event_id", "history_id", "_history_id", "_cw_event_key", "_cw_rewatch_sync"):
        if field in item and item.get(field) not in (None, ""):
            minimal_item[field] = item.get(field)
    entry: dict[str, Any] = {"item": minimal_item, "hint": hint}
    key = ""
    if item.get("_cw_rewatch_sync") is True:
        key = str(item.get("_cw_event_key") or "").strip()
    if not key:
        try:
            key = str(canonical_key(minimal_item) or "").strip()
        except Exception:
            key = ""
    if key:
        entry["key"] = key
    return entry


def _id_aliases(ids: Any) -> set[str]:
    out: set[str] = set()
    if not isinstance(ids, Mapping):
        return out
    for k, v in ids.items():
        if v in (None, ""):
            continue
        out.add(f"{str(k).strip().lower()}:{str(v).strip().lower()}")
    return out


def _match_sent_row(ids: Any, sent_rows: Any) -> Mapping[str, Any] | None:
    aliases = _id_aliases(ids)
    if not aliases or not isinstance(sent_rows, (list, tuple)):
        return None
    for sent in sent_rows:
        if not isinstance(sent, Mapping):
            continue
        if aliases & _id_aliases(sent.get("ids")):
            return sent
    return None


def _seasons_of(row: Any) -> list[Mapping[str, Any]]:
    seasons = row.get("seasons") if isinstance(row, Mapping) else None
    if not isinstance(seasons, list):
        return []
    return [s for s in seasons if isinstance(s, Mapping)]


def _show_meta(*rows: Any) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            continue
        title = row.get("title") or row.get("series_title")
        if isinstance(title, str) and title.strip() and "series_title" not in out:
            out["series_title"] = title.strip()
        year = _as_int(row.get("year") if row.get("year") is not None else row.get("series_year"))
        if year and "series_year" not in out:
            out["series_year"] = year
    return out


def _expand_season(ids: Mapping[str, Any], season: Mapping[str, Any], meta: Mapping[str, Any]) -> list[dict[str, Any]]:
    season_number = _as_int(season.get("number"))
    episodes = season.get("episodes")
    if not isinstance(episodes, list) or not episodes:
        return [{"type": "season", "ids": dict(ids), "show_ids": dict(ids), "season": season_number, **meta}]
    out: list[dict[str, Any]] = []
    for episode in episodes:
        if not isinstance(episode, Mapping):
            continue
        out.append(
            {
                "type": "episode",
                "ids": dict(ids),
                "show_ids": dict(ids),
                "season": season_number,
                "episode": _as_int(episode.get("number")),
                **meta,
            }
        )
    return out


def _show_node(row: Any) -> Mapping[str, Any] | None:
    node = row.get("show") if isinstance(row, Mapping) else None
    return node if isinstance(node, Mapping) else None


def _identity_ids(row: Mapping[str, Any], kind: str) -> dict[str, Any]:
    show = _show_node(row)
    show_ids = show.get("ids") if show is not None else None
    own_ids = row.get("ids") if isinstance(row.get("ids"), Mapping) else None
    if kind in ("seasons", "episodes"):
        if isinstance(show_ids, Mapping) and show_ids:
            return dict(show_ids)
        return dict(own_ids or {})
    if isinstance(own_ids, Mapping) and own_ids:
        return dict(own_ids)
    return dict(show_ids or {}) if isinstance(show_ids, Mapping) else {}


def _items_for_not_found(row: Mapping[str, Any], kind: str, sent_rows: Any = None) -> list[dict[str, Any]]:
    ids = _identity_ids(row, kind)
    sent = _match_sent_row(ids, sent_rows)
    meta = _show_meta(_show_node(row), row, sent)

    if kind == "movies":
        title = meta.get("series_title")
        year = meta.get("series_year")
        movie: dict[str, Any] = {"type": "movie", "ids": ids}
        if title:
            movie["title"] = title
        if year:
            movie["year"] = year
        return [movie]

    if kind == "episodes":
        season_number = _as_int(row.get("season") if row.get("season") is not None else row.get("season_number"))
        episode_number = _as_int(row.get("number") if row.get("number") is not None else row.get("episode"))
        if season_number is not None and episode_number is not None:
            return [
                {
                    "type": "episode",
                    "ids": ids,
                    "show_ids": ids,
                    "season": season_number,
                    "episode": episode_number,
                    **meta,
                }
            ]

    if kind == "seasons":
        season_number = _as_int(row.get("number") if row.get("number") is not None else row.get("season"))
        if season_number is not None:
            sent_season = next((s for s in _seasons_of(sent) if _as_int(s.get("number")) == season_number), None)
            return _expand_season(ids, sent_season or {"number": season_number}, meta)

    seasons = _seasons_of(row) or _seasons_of(sent)
    show: dict[str, Any] = {"type": "show", "ids": ids}
    if meta.get("series_title"):
        show["title"] = meta["series_title"]
    if meta.get("series_year"):
        show["year"] = meta["series_year"]
    if not seasons:
        return [show]

    out: list[dict[str, Any]] = []
    for season in seasons:
        out.extend(_expand_season(ids, season, meta))
    return out or [show]


def _bucketize(adapter: Any, items: Iterable[Mapping[str, Any]], *, unwatch: bool) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    shows_nested: dict[str, dict[str, Any]] = {}
    shows_plain: dict[str, dict[str, Any]] = {}

    nested_show_keys: set[str] = set()

    accepted: list[dict[str, Any]] = []
    
    def _carry_meta_for_mdblist(src: Mapping[str, Any], dst: dict[str, Any]) -> None:
        typ = str(src.get("type") or "").strip().lower()
        if typ.endswith("s") and typ in ("movies", "shows", "seasons", "episodes"):
            typ = typ[:-1]
        if typ in ("season", "episode"):
            title = src.get("series_title") or src.get("title")
            y = src.get("series_year") if src.get("year") is None else src.get("year")
        else:
            title = src.get("title") or src.get("series_title")
            y = src.get("year")
        if isinstance(title, str):
            title = title.strip()
        if title:
            dst["title"] = title
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            dst["year"] = year


    release_tag = _anime_release_tag(adapter)
    for raw in items or []:
        m = _with_mdblist_anime_coordinate(adapter, raw or {}, release_tag=release_tag)
        typ_raw = m.get("type")
        typ = str(typ_raw or "movie").strip().lower()
        if typ.endswith("s") and typ in ("movies", "shows", "seasons", "episodes"):
            typ = typ[:-1]
        if typ not in ("movie", "show", "season", "episode"):
            _dbg("write_item_skipped", reason="unknown_type", item=id_minimal({"type": str(typ_raw or "unknown"), "ids": _ids_pick(m)}))
            continue
        ids = _ids_pick(m)
        show_ids = _ids_pick(m.get("show_ids") or {}) if isinstance(m.get("show_ids"), Mapping) else {}

        watched_at = m.get("watched_at") or m.get("last_watched_at")
        watched_iso = _iso_z(watched_at) if _iso_ok(watched_at) else None
        if not watched_iso and not unwatch:
            _dbg("write_item_skipped", reason="missing_watched_at", item=id_minimal({"type": typ, "ids": ids or show_ids}))
            continue

        if typ == "movie":
            if not ids:
                continue
            row: dict[str, Any] = {"ids": ids}
            if watched_iso:
                row["watched_at"] = watched_iso
            _carry_meta_for_mdblist(m, row)
            movies.append(row)
            acc = {"type": "movie", "ids": ids, **({"watched_at": watched_iso} if watched_iso else {})}
            _carry_meta_for_mdblist(m, acc)
            _copy_source_event_meta(m, acc)
            accepted.append(acc)
            continue

        if typ == "show":
            if not ids:
                continue
            key = _stable_show_key(ids)
            sh = shows_plain.get(key) or {"ids": ids}
            if watched_iso:
                sh["watched_at"] = watched_iso
            _carry_meta_for_mdblist(m, sh)
            shows_plain[key] = sh
            acc = {"type": "show", "ids": ids, **({"watched_at": watched_iso} if watched_iso else {})}
            _carry_meta_for_mdblist(m, acc)
            _copy_source_event_meta(m, acc)
            accepted.append(acc)
            continue

        if typ == "episode":
            flat_episode = _flat_episode_tmdb_row(m, ids, show_ids, watched_iso)
            if flat_episode is not None:
                episodes.append(flat_episode)
                acc = {
                    "type": "episode",
                    "ids": ids,
                    **({"watched_at": watched_iso} if watched_iso else {}),
                }
                if show_ids:
                    acc["show_ids"] = show_ids
                if m.get("season") is not None:
                    acc["season"] = m.get("season")
                ep_num = m.get("episode") if m.get("episode") is not None else m.get("number")
                if ep_num is not None:
                    acc["episode"] = ep_num
                _carry_meta_for_mdblist(m, acc)
                _copy_source_event_meta(m, acc)
                accepted.append(acc)
                continue

        season_num = m.get("season")
        if season_num is None:
            continue
        try:
            s = int(season_num)
        except Exception:
            continue

        sh_ids = show_ids or ids
        if not sh_ids:
            continue
        skey = _stable_show_key(sh_ids)
        nested_show_keys.add(skey)
        show_obj = shows_nested.get(skey) or {"ids": sh_ids, "seasons": []}
        _carry_meta_for_mdblist(m, show_obj)
        if not isinstance(show_obj.get("seasons"), list):
            show_obj["seasons"] = []
        seasons_list: list[dict[str, Any]] = show_obj["seasons"]
        season_obj: dict[str, Any] | None = None
        for candidate in seasons_list:
            candidate_raw = candidate.get("number")
            if candidate_raw is None:
                continue
            try:
                candidate_number = int(candidate_raw)
            except (TypeError, ValueError):
                continue
            if candidate_number == s:
                season_obj = candidate
                break
        if not season_obj:
            season_obj = {"number": s}
            seasons_list.append(season_obj)

        if typ == "season":
            if watched_iso:
                season_obj["watched_at"] = watched_iso
            shows_nested[skey] = show_obj
            accepted.append(
                {
                    "type": "season",
                    "ids": ids or sh_ids,
                    "show_ids": sh_ids,
                    "season": s,
                    **({"watched_at": watched_iso} if watched_iso else {}),
                }
            )
            _carry_meta_for_mdblist(m, accepted[-1])
            _copy_source_event_meta(m, accepted[-1])
            continue

        ep_num = m.get("episode") if m.get("episode") is not None else m.get("number")
        if ep_num is None:
            continue
        try:
            e = int(ep_num)
        except Exception:
            continue
        ep: dict[str, Any] = {"number": e}
        if watched_iso:
            ep["watched_at"] = watched_iso
        episodes_list = season_obj.get("episodes")
        if not isinstance(episodes_list, list):
            episodes_list = []
            season_obj["episodes"] = episodes_list
        episodes_list.append(ep)
        shows_nested[skey] = show_obj
        accepted.append(
            {
                "type": "episode",
                "ids": ids or sh_ids,
                "show_ids": sh_ids,
                "season": s,
                "episode": e,
                **({"watched_at": watched_iso} if watched_iso else {}),
            }
        )
        _carry_meta_for_mdblist(m, accepted[-1])
        _copy_source_event_meta(m, accepted[-1])

    skipped_nested = 0
    if shows_plain and nested_show_keys:
        for k in list(shows_plain.keys()):
            if k in nested_show_keys:
                shows_plain.pop(k, None)
                skipped_nested += 1

    skipped_meta = 0
    if not unwatch and shows_plain:
        for k, v in list(shows_plain.items()):
            if (not v.get("title") and not v.get("year")) and not v.get("ids"):
                shows_plain.pop(k, None)
                skipped_meta += 1

    if skipped_nested or skipped_meta:
        _dbg("write_prepare", skipped_nested=skipped_nested, skipped_meta=skipped_meta)

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if episodes:
        body["episodes"] = episodes
    if shows_nested:
        for grp in shows_nested.values():
            seasons_list2 = grp.get("seasons")
            if isinstance(seasons_list2, list):
                grp["seasons"] = sorted(seasons_list2, key=lambda x: int(x.get("number") or 0))
                for sp in grp["seasons"]:
                    eps = sp.get("episodes")
                    if isinstance(eps, list):
                        sp["episodes"] = sorted(eps, key=lambda x: int(x.get("number") or 0))
        body["shows_nested"] = list(shows_nested.values())
    if shows_plain:
        body["shows_plain"] = list(shows_plain.values())
    body = {k: v for k, v in body.items() if v}
    return body, accepted


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _source_event_key(adapter: Any, item: Mapping[str, Any]) -> str:
    if item.get("_cw_rewatch_sync") is True:
        key = str(item.get("_cw_event_key") or "").strip()
        if key:
            return key
    return _history_key(adapter, item)


def _event_write_result(
    *,
    confirmed_keys: Iterable[str],
    unresolved: list[dict[str, Any]],
    attempted: int,
) -> dict[str, Any]:
    confirmed = [str(k) for k in dict.fromkeys(k for k in confirmed_keys if k)]
    unresolved_keys = [
        str(u.get("key") or "").strip()
        for u in unresolved
        if isinstance(u, Mapping) and str(u.get("key") or "").strip()
    ]
    unresolved_key_set = set(unresolved_keys)
    confirmed = [k for k in confirmed if k not in unresolved_key_set]
    return {
        "ok": not unresolved,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved": unresolved,
        "unresolved_keys": list(dict.fromkeys(unresolved_keys)),
        "attempted": attempted,
    }


def _merge_event_results(
    first: dict[str, Any] | None,
    second: dict[str, Any],
) -> dict[str, Any]:
    if not first:
        return second
    unresolved = list(first.get("unresolved") or []) + list(second.get("unresolved") or [])
    confirmed = list(first.get("confirmed_keys") or []) + list(second.get("confirmed_keys") or [])
    return _event_write_result(
        confirmed_keys=confirmed,
        unresolved=unresolved,
        attempted=int(second.get("attempted") or 0),
    )


def _play_id(item: Mapping[str, Any]) -> int | None:
    for field in ("_mdblist_play_id", "play_id", "provider_event_id", "history_id", "_history_id", "id"):
        value = item.get(field)
        if value in (None, ""):
            continue
        try:
            play_id = int(str(value).strip())
        except Exception:
            continue
        if play_id > 0:
            return play_id
    return None


def _remove_plays(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    apikey: str,
    timeout: float,
    retries: int,
    chunk_size: int,
    delay_ms: int = 0,
) -> dict[str, Any]:
    items_list = [dict(it or {}) for it in items or [] if isinstance(it, Mapping)]
    by_id: dict[int, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    for item in items_list:
        pid = _play_id(item)
        if pid is None:
            unresolved.append(_unresolved_entry(item, "missing_mdblist_play_id"))
            continue
        by_id.setdefault(pid, item)

    confirmed: list[str] = []
    for ids_part in _chunk(list(by_id.keys()), chunk_size):
        try:
            r = mdblist_request(
                adapter,
                "POST",
                URL_REMOVE_PLAYS,
                params={"apikey": apikey},
                json={"play_ids": ids_part},
                timeout=timeout,
                max_retries=retries,
            )
        except Exception as e:
            for pid in ids_part:
                unresolved.append(_unresolved_entry(by_id[pid], f"exception:{type(e).__name__}"))
            continue

        if r.status_code not in (200, 201, 204):
            _warn("write_failed", op="remove_play", status=r.status_code, body=(r.text or '')[:200])
            for pid in ids_part:
                unresolved.append(_unresolved_entry(by_id[pid], f"http:{r.status_code}"))
            continue

        if r.status_code == 204 or not (r.text or "").strip():
            data: Mapping[str, Any] = {"removed": ids_part}
        else:
            try:
                parsed = r.json()
                data = parsed if isinstance(parsed, Mapping) else {}
            except Exception:
                data = {}

        removed_ids = {
            int(x)
            for x in (data.get("removed") or ids_part)
            if str(x).strip().lstrip("-").isdigit()
        }
        not_found_ids = {
            int(x)
            for x in (data.get("not_found") or [])
            if str(x).strip().lstrip("-").isdigit()
        }
        for pid in ids_part:
            item = by_id[pid]
            if pid in not_found_ids:
                unresolved.append(_unresolved_entry(item, "play_not_found"))
                continue
            if pid in removed_ids:
                confirmed.append(_source_event_key(adapter, item))

        if delay_ms > 0:
            time.sleep(max(0.0, delay_ms / 1000.0))

    _info("write_done", op="remove_play", ok=len(unresolved) == 0, applied=len(confirmed), unresolved=len(unresolved))
    return _event_write_result(confirmed_keys=confirmed, unresolved=unresolved, attempted=len(items_list))


def _same_event_candidate(adapter: Any, left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    left_key = _history_key(adapter, left)
    right_key = _history_key(adapter, right)
    if left_key and right_key and left_key == right_key:
        return True
    if _type_norm(left.get("type")) != _type_norm(right.get("type")):
        return False
    left_raw = str(left.get("watched_at") or "")
    right_raw = str(right.get("watched_at") or "")
    left_w = _iso_z(left_raw) if _iso_ok(left_raw) else left_raw
    right_w = _iso_z(right_raw) if _iso_ok(right_raw) else right_raw
    if left_w and right_w and left_w != right_w:
        return False
    left_ids = _id_aliases(left.get("ids")) | _id_aliases(left.get("show_ids"))
    right_ids = _id_aliases(right.get("ids")) | _id_aliases(right.get("show_ids"))
    if left_ids and right_ids and not (left_ids & right_ids):
        return False
    if _type_norm(left.get("type")) == "episode":
        for field in ("season", "episode"):
            lv = _as_int(left.get(field))
            rv = _as_int(right.get(field))
            if lv is not None and rv is not None and lv != rv:
                return False
    return True


def _match_response_play(
    adapter: Any,
    play_item: Mapping[str, Any],
    accepted: Sequence[Mapping[str, Any]],
    used: set[int],
) -> Mapping[str, Any] | None:
    for idx, item in enumerate(accepted or []):
        if idx in used:
            continue
        if _same_event_candidate(adapter, play_item, item):
            used.add(idx)
            return item
    return None


def _write(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    unwatch: bool = False,
) -> Any:
    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    items_list = list(items or [])
    event_mode = _rewatches_enabled(adapter)
    if not has_auth(cfg):
        unresolved = [{"item": id_minimal(it), "hint": "missing_auth"} for it in items_list]
        _info("write_skipped", op="remove" if unwatch else "add", reason="missing_auth", unresolved=len(unresolved))
        return 0, unresolved

    sess = adapter.client.session
    tmo = adapter.cfg.timeout
    rr = adapter.cfg.max_retries

    chunk_size = _cfg_int(cfg, "history_chunk_size", 25)
    delay_ms = _cfg_int(cfg, "history_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(cfg, "history_max_backoff_ms", 8000)

    if event_mode and not unwatch:
        chunk_size = max(1, min(chunk_size, PLAY_REPORT_MAX_ITEMS))

    attempted_total = len(items_list)
    play_result: dict[str, Any] | None = None
    if event_mode and unwatch:
        rollup_items = [it for it in items_list if _type_norm(it.get("type")) in ("show", "season")]
        play_items = [it for it in items_list if _type_norm(it.get("type")) not in ("show", "season")]
        play_result = _remove_plays(
            adapter,
            play_items,
            apikey=apikey,
            timeout=tmo,
            retries=rr,
            chunk_size=chunk_size,
            delay_ms=delay_ms,
        )
        if not rollup_items:
            return play_result
        items_list = rollup_items

    body, accepted = _bucketize(adapter, items_list, unwatch=unwatch)
    if not body:
        _info("write_skipped", op="remove" if unwatch else "add", reason="empty_payload", unresolved=0)
        if event_mode:
            return _merge_event_results(
                play_result,
                _event_write_result(confirmed_keys=[], unresolved=[], attempted=attempted_total),
            )
        return 0, []

    ok = 0
    confirmed_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    matched_accepted: set[int] = set()
    accepted_episode_tmdb: dict[str, dict[str, Any]] = {}
    for item in accepted:
        if _type_norm(item.get("type")) != "episode":
            continue
        raw_ids = item.get("ids")
        tmdb = _tmdb_id_value(raw_ids if isinstance(raw_ids, Mapping) else {})
        if tmdb is not None:
            accepted_episode_tmdb.setdefault(str(tmdb), dict(item))

    def _episode_attempt_for_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
        ids = row.get("ids") if isinstance(row.get("ids"), Mapping) else row
        tmdb = _tmdb_id_value(ids if isinstance(ids, Mapping) else {})
        if tmdb is None:
            return None
        return accepted_episode_tmdb.get(str(tmdb))

    def _unresolved_for_sent(bucket_name: str, sent: Mapping[str, Any], hint: str) -> list[dict[str, Any]]:
        ids = sent.get("ids") if isinstance(sent.get("ids"), Mapping) else {}
        if bucket_name == "episodes":
            attempted = _episode_attempt_for_row(sent)
            if attempted is not None:
                return [_unresolved_entry(attempted, hint)]
            return [_unresolved_entry({"type": "episode", "ids": ids}, hint)]
        if bucket_name == "shows":
            return [_unresolved_entry(item_nf, hint) for item_nf in _items_for_not_found(sent, "shows")]
        return [_unresolved_entry({"type": "movie", "ids": ids}, hint)]

    def _apply_play_report(data: Mapping[str, Any]) -> int | None:
        plays = data.get("plays")
        if not isinstance(plays, list):
            return None
        applied = 0
        for row in plays:
            if not isinstance(row, Mapping):
                continue
            play_item = _play_response_item(row)
            if not play_item:
                continue
            matched = _match_response_play(adapter, play_item, accepted, matched_accepted)
            if matched is None:
                unresolved.append(_unresolved_entry(play_item, "play_response_unmatched"))
                continue
            if row.get("added") is False:
                unresolved.append(_unresolved_entry(matched, "mdblist_play_collapsed"))
                continue
            confirmed_keys.append(_source_event_key(adapter, matched))
            applied += 1
        return applied

    def _apply_success(payload_rows: list[dict[str, Any]], bucket_name: str, data: Mapping[str, Any], *, is_unwatch: bool) -> int:
        kinds = ("movies", "shows", "seasons", "episodes")
        if is_unwatch:
            removed = data.get("removed") or data.get("deleted") or data.get("unwatched") or {}
            n = sum(int(removed.get(k) or 0) for k in kinds)
            if n <= 0:
                _dbg("write_prepare", op="remove", reason="noop_response", bucket=bucket_name, rows=len(payload_rows))
            return n

        not_found = data.get("not_found") or {}
        for kind in kinds:
            rows_nf = not_found.get(kind) or []
            if not isinstance(rows_nf, list):
                continue
            for row_nf in rows_nf:
                if not isinstance(row_nf, Mapping):
                    continue
                if kind == "episodes":
                    attempted = _episode_attempt_for_row(row_nf)
                    if attempted is not None:
                        unresolved.append(_unresolved_entry(attempted, "not_found"))
                        continue
                for item_nf in _items_for_not_found(row_nf, kind, payload_rows):
                    unresolved.append(_unresolved_entry(item_nf, "not_found"))

        if event_mode:
            play_applied = _apply_play_report(data)
            if play_applied is not None:
                if play_applied <= 0 and not unresolved:
                    _dbg("write_prepare", op="add", reason="noop_play_response", bucket=bucket_name, rows=len(payload_rows))
                return play_applied
            _dbg("write_prepare", op="add", reason="play_report_missing", bucket=bucket_name, rows=len(payload_rows))

        updated = data.get("updated") or {}
        added = data.get("added") or {}
        existing = data.get("existing") or {}
        n = 0
        n += sum(int(updated.get(k) or 0) for k in kinds)
        n += sum(int(added.get(k) or 0) for k in kinds)
        n += sum(int(existing.get(k) or 0) for k in kinds)
        if n <= 0 and not unresolved:
            _dbg("write_prepare", op="add", reason="noop_response", bucket=bucket_name, rows=len(payload_rows))
        return n

    _dbg(
        "write_start",
        op="remove" if unwatch else "add",
        movies=len(body.get("movies") or []),
        episodes=len(body.get("episodes") or []),
        shows_nested=len(body.get("shows_nested") or []),
        shows_plain=len(body.get("shows_plain") or []),
        chunk_size=chunk_size,
    )

    stages: list[tuple[str, str]] = [
        ("movies", "movies"),
        ("episodes", "episodes"),
        ("shows_nested", "shows"),
        ("shows_plain", "shows"),
    ]

    for body_key, bucket in stages:
        rows = body.get(body_key) or []
        if not rows:
            continue

        stage = "" if body_key == bucket else body_key

        for part in _chunk(rows, chunk_size):
            payload = {bucket: part}
            url = URL_REMOVE if unwatch else URL_UPSERT
            attempt = 0
            backoff = delay_ms

            while True:
                r = mdblist_request(
                    adapter,
                    "POST",
                    url,
                    params={"apikey": apikey, **({"report_added": "true"} if event_mode and not unwatch else {})},
                    json=payload,
                    timeout=tmo,
                    max_retries=rr,
                )

                if r.status_code in (200, 201, 204):
                    d: dict[str, Any]
                    if r.status_code == 204 or not (r.text or "").strip():
                        d = {}
                    else:
                        try:
                            d = r.json()
                        except Exception:
                            d = {}
                    n = _apply_success(part, stage or bucket, d, is_unwatch=unwatch)
                    if r.status_code == 204 and unwatch and n <= 0:
                        n = len(part)
                    ok += n

                    time.sleep(max(0.0, delay_ms / 1000.0))
                    break

                if r.status_code in (429, 503):
                    _warn(
                        "rate_limit",
                        op="remove" if unwatch else "add",
                        bucket=bucket,
                        stage=stage or bucket,
                        status=r.status_code,
                        attempt=attempt,
                        backoff_ms=backoff,
                        body=(r.text or '')[:180],
                    )
                    time.sleep(min(max_backoff_ms, backoff) / 1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff * 1.6) + 200)
                    if attempt <= 4:
                        continue

                if (
                    r.status_code == 500
                    and bucket == "shows"
                    and body_key == "shows_plain"
                    and len(part) > 1
                ):
                    preview = [id_minimal({"type": "show", "ids": (x.get("ids") or {})}) for x in part[:3]]
                    _warn(
                        "write_failed",
                        op="remove" if unwatch else "add",
                        bucket=bucket,
                        stage=body_key,
                        status=500,
                        reason="retry_split",
                        rows=len(part),
                        preview=preview,
                    )

                    for single in part:
                        r2 = mdblist_request(
                            adapter,
                            "POST",
                            url,
                            params={"apikey": apikey, **({"report_added": "true"} if event_mode and not unwatch else {})},
                            json={bucket: [single]},
                            timeout=tmo,
                            max_retries=rr,
                        )
                        if r2.status_code in (200, 201, 204):
                            d2: dict[str, Any]
                            if r2.status_code == 204 or not (r2.text or "").strip():
                                d2 = {}
                            else:
                                try:
                                    d2 = r2.json()
                                except Exception:
                                    d2 = {}
                            n2 = _apply_success([single], body_key, d2, is_unwatch=unwatch)
                            if r2.status_code == 204 and unwatch and n2 <= 0:
                                n2 = 1
                            ok += n2
                            time.sleep(max(0.0, delay_ms / 1000.0))
                            continue

                        unresolved.extend(_unresolved_for_sent("shows", single, f"http:{r2.status_code}"))
                    break

                _warn(
                    "write_failed",
                    op="remove" if unwatch else "add",
                    bucket=bucket,
                    stage=stage or bucket,
                    status=r.status_code,
                    body=(r.text or '')[:200],
                )
                for x in part:
                    unresolved.extend(_unresolved_for_sent(bucket, x, f"http:{r.status_code}"))
                break

    if ok > 0 and not unresolved and not event_mode:
        cache = _load_cache()
        if unwatch:
            for it in accepted:
                if not _sync_visible(it):
                    continue
                base = _base_key(it)
                for k in list(cache.keys()):
                    if str(k).startswith(base + "@"):
                        cache.pop(k, None)
        else:
            for it in accepted:
                if not _sync_visible(it):
                    continue
                ek = _event_key(it) if it.get("watched_at") else None
                if ek:
                    cache[ek] = dict(it)
        _save_cache(cache)

        newest: str | None = None
        for it in accepted:
            if not _sync_visible(it):
                continue
            newest = _max_iso(newest, it.get("watched_at"))
        update_watermark_if_new("history", newest)

    _info("write_done", op="remove" if unwatch else "add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    if event_mode:
        failed_keys = {
            str(u.get("key") or "").strip()
            for u in unresolved
            if isinstance(u, Mapping) and str(u.get("key") or "").strip()
        }
        for it in accepted:
            key = _source_event_key(adapter, it)
            if not key:
                continue
            try:
                base = str(canonical_key(id_minimal(it)) or "").strip()
            except Exception:
                base = ""
            if base and base in failed_keys:
                continue
            confirmed_keys.append(key)
        return _merge_event_results(
            play_result,
            _event_write_result(confirmed_keys=confirmed_keys, unresolved=unresolved, attempted=attempted_total),
        )
    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> Any:
    return _write(adapter, items, unwatch=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> Any:
    return _write(adapter, items, unwatch=True)

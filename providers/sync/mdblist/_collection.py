# /providers/sync/mdblist/_collection.py
# MDBList collection sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.anime_mapping.service import mapped_or_default_media_type
from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import (
    _is_capture_mode,
    _pair_scope,
    as_epoch,
    cfg_bool,
    cfg_int,
    cfg_section,
    get_watermark,
    has_auth,
    iso_ok,
    iso_z,
    mdblist_request,
    now_iso,
    read_json,
    save_watermark,
    state_file,
    write_json,
)

BASE = "https://api.mdblist.com"
URL_LIST = f"{BASE}/sync/collection"
URL_REMOVE = f"{BASE}/sync/collection/remove"

_cfg = cfg_section
_cfg_int = cfg_int
_cfg_bool = cfg_bool
_as_epoch = as_epoch
_iso_ok = iso_ok
_iso_z = iso_z
_now_iso = now_iso


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "collection", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "collection", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "collection", "warn", msg, **fields)


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        s = str(value).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _key_of(item: Mapping[str, Any]) -> str:
    try:
        key = canonical_key(item)
        if key:
            return str(key)
    except Exception:
        pass
    return f"obj:{hash(json.dumps(dict(item), sort_keys=True, default=str)) & 0xffffffff}"


def _unresolved_entry(item: Mapping[str, Any], hint: str, *, key: str | None = None) -> dict[str, Any]:
    minimal_item = id_minimal(item)
    entry: dict[str, Any] = {"item": minimal_item, "hint": hint}
    resolved_key = _as_str(key)
    if not resolved_key:
        try:
            resolved_key = _as_str(canonical_key(minimal_item))
        except Exception:
            resolved_key = None
    if resolved_key:
        entry["key"] = resolved_key
    return entry


def _collection_write_result(
    *,
    confirmed_keys: Iterable[str],
    unresolved: list[dict[str, Any]],
    attempted: int,
    accepted_keys: Iterable[str] | None = None,
    presence_confirmed_keys: Iterable[str] | None = None,
    accepted_not_seen_live_keys: Iterable[str] | None = None,
) -> dict[str, Any]:
    unresolved_keys = [
        str(u.get("key") or "").strip()
        for u in unresolved
        if isinstance(u, Mapping) and str(u.get("key") or "").strip()
    ]
    unresolved_set = set(unresolved_keys)
    confirmed = [
        str(k)
        for k in dict.fromkeys(str(k) for k in confirmed_keys if k)
        if str(k) not in unresolved_set
    ]
    out: dict[str, Any] = {
        "ok": not unresolved,
        "count": len(confirmed),
        "confirmed_keys": confirmed,
        "unresolved": unresolved,
        "unresolved_keys": list(dict.fromkeys(unresolved_keys)),
        "attempted": int(attempted or 0),
    }
    if accepted_keys is not None:
        out["accepted_keys"] = [str(k) for k in dict.fromkeys(str(k) for k in accepted_keys if k)]
    if presence_confirmed_keys is not None:
        out["presence_confirmed_keys"] = [
            str(k) for k in dict.fromkeys(str(k) for k in presence_confirmed_keys if k)
        ]
    if accepted_not_seen_live_keys is not None:
        out["accepted_not_seen_live_keys"] = [
            str(k) for k in dict.fromkeys(str(k) for k in accepted_not_seen_live_keys if k)
        ]
    return out


def _shadow_path() -> Path:
    return state_file("mdblist_collection.shadow.json")


def _shadow_load() -> dict[str, Any]:
    doc = read_json(_shadow_path())
    if not isinstance(doc, dict):
        return {"ts": 0, "items": {}}
    doc.setdefault("ts", 0)
    if not isinstance(doc.get("items"), dict):
        doc["items"] = {}
    return doc


def _shadow_save(items: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    write_json(_shadow_path(), {"ts": int(time.time()), "items": dict(items)}, indent=None, sort_keys=False)


def _shadow_bust() -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        p = _shadow_path()
        if p.exists():
            p.unlink()
            _dbg("cache_invalidated", cache="shadow", reason="write_applied")
    except Exception:
        pass


def _ids_clean(ids: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    aliases = {
        "imdbid": "imdb",
        "imdb_id": "imdb",
        "tmdbid": "tmdb",
        "tmdb_id": "tmdb",
        "tvdbid": "tvdb",
        "tvdb_id": "tvdb",
        "traktid": "trakt",
        "trakt_id": "trakt",
        "mdblistid": "mdblist",
        "mdblist_id": "mdblist",
        "kitsuid": "kitsu",
        "kitsu_id": "kitsu",
    }
    for k, v in dict(ids or {}).items():
        if v is None:
            continue
        key = aliases.get(str(k).strip().lower(), str(k).strip().lower())
        s = str(v).strip()
        if s:
            out[key] = s
    return out


def _ids_pick(*nodes: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for node in nodes:
        if not isinstance(node, Mapping):
            continue
        out.update(_ids_clean(node.get("ids") if isinstance(node.get("ids"), Mapping) else None))
        out.update(
            _ids_clean(
                {
                    "imdb": node.get("imdb") or node.get("imdb_id") or node.get("imdbid"),
                    "tmdb": node.get("tmdb") or node.get("tmdb_id") or node.get("tmdbid"),
                    "tvdb": node.get("tvdb") or node.get("tvdb_id") or node.get("tvdbid"),
                    "trakt": node.get("trakt") or node.get("trakt_id") or node.get("traktid"),
                    "mdblist": node.get("mdblist") or node.get("mdblist_id") or node.get("mdblistid"),
                    "kitsu": node.get("kitsu") or node.get("kitsu_id") or node.get("kitsuid"),
                }
            )
        )
    return out


def _title(node: Any) -> Any:
    if not isinstance(node, Mapping):
        return None
    return node.get("title") or node.get("name") or node.get("episode_title")


def _year(node: Any) -> int | None:
    if not isinstance(node, Mapping):
        return None
    year = _as_int(node.get("year") or node.get("release_year") or node.get("first_air_year"))
    if year is not None:
        return year
    first_air = _as_str(node.get("first_air_date") or node.get("first_aired"))
    return _as_int(first_air[:4]) if first_air and len(first_air) >= 4 else None


def _number_from(node: Any, *keys: str) -> int | None:
    if not isinstance(node, Mapping):
        return None
    for key in keys:
        value = node.get(key)
        if isinstance(value, Mapping):
            value = value.get("number")
        num = _as_int(value)
        if num is not None:
            return num
    return None


def _show_node(row: Mapping[str, Any], child: Any = None) -> Mapping[str, Any] | None:
    if isinstance(child, Mapping) and isinstance(child.get("show"), Mapping):
        return child["show"]
    if isinstance(row.get("show"), Mapping):
        return row["show"]
    return None


def _ids_for_mdblist(item: Mapping[str, Any], *, show_scope: bool = False) -> dict[str, Any]:
    source = item.get("show_ids") if show_scope and isinstance(item.get("show_ids"), Mapping) else item.get("ids")
    ids_raw = dict(source or {})
    if not ids_raw and not show_scope:
        ids_raw = {
            "imdb": item.get("imdb") or item.get("imdb_id"),
            "tmdb": item.get("tmdb") or item.get("tmdb_id"),
            "tvdb": item.get("tvdb") or item.get("tvdb_id"),
            "trakt": item.get("trakt") or item.get("trakt_id"),
            "mdblist": item.get("mdblist") or item.get("mdblist_id"),
            "kitsu": item.get("kitsu") or item.get("kitsu_id"),
        }

    out: dict[str, Any] = {}
    imdb = _as_str(ids_raw.get("imdb"))
    if imdb:
        out["imdb"] = imdb
    mdblist = _as_str(ids_raw.get("mdblist") or ids_raw.get("mdblist_id"))
    if mdblist:
        out["mdblist"] = mdblist
    for key in ("tmdb", "tvdb", "trakt", "kitsu"):
        ident = _as_int(ids_raw.get(key) or ids_raw.get(f"{key}_id"))
        if ident is not None:
            out[key] = ident
    return out


def _item_media_type(item: Mapping[str, Any]) -> str:
    t = str(item.get("type") or item.get("media_type") or "").strip().lower()
    if t in ("episode", "season"):
        return t
    if mapped_or_default_media_type(item) == "show" or t in ("show", "shows", "series", "tv"):
        return "show"
    return "movie"


def _movie_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    movie = row.get("movie") if isinstance(row.get("movie"), Mapping) else row
    if not isinstance(movie, Mapping):
        return None
    item = id_minimal({"type": "movie", "title": _title(movie), "year": _year(movie), "ids": _ids_pick(movie, row)})
    collected_at = row.get("collected_at")
    if collected_at:
        item["collected_at"] = str(collected_at)
    return item


def _show_base(row: Mapping[str, Any]) -> dict[str, Any] | None:
    show = row.get("show") if isinstance(row.get("show"), Mapping) else row
    if not isinstance(show, Mapping):
        return None
    item = id_minimal({"type": "show", "title": _title(show), "year": _year(show), "ids": _ids_pick(show, row)})
    collected_at = row.get("collected_at")
    if collected_at:
        item["collected_at"] = str(collected_at)
    return item


def _season_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    season = row.get("season") if isinstance(row.get("season"), Mapping) else row
    if not isinstance(season, Mapping):
        return None
    show = _show_node(row, season)
    row_show_ids = row.get("show_ids") if isinstance(row.get("show_ids"), Mapping) else None
    show_ids = _ids_pick(show) or _ids_pick(row_show_ids) or _ids_pick(row)
    season_no = _number_from(season, "number", "season", "season_number") or _number_from(
        row, "number", "season", "season_number"
    )
    if season_no is None or not show_ids:
        return None
    item = id_minimal(
        {
            "type": "season",
            "title": _title(show) or _title(row),
            "series_title": _title(show) or _title(row),
            "year": _year(show) or _year(row),
            "show_ids": show_ids,
            "ids": _ids_pick(season) or show_ids,
            "season": season_no,
        }
    )
    collected_at = season.get("collected_at") or row.get("collected_at")
    if collected_at:
        item["collected_at"] = str(collected_at)
    return item


def _episode_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    episode = row.get("episode") if isinstance(row.get("episode"), Mapping) else row
    if not isinstance(episode, Mapping):
        return None
    show = _show_node(row, episode)
    show_ids = _ids_pick(show)
    if not show_ids:
        row_show_ids = row.get("show_ids") if isinstance(row.get("show_ids"), Mapping) else None
        show_ids = _ids_pick(row_show_ids) or _ids_pick(row)
    season_no = _number_from(episode, "season", "season_number") or _number_from(row, "season", "season_number")
    episode_no = _number_from(episode, "number", "episode", "episode_number") or _number_from(row, "number", "episode", "episode_number")
    ids = _ids_pick(episode)
    if episode is row:
        ids = {}
    if season_no is None or episode_no is None or not (show_ids or ids):
        return None
    scope_ids = show_ids or ids
    item = id_minimal(
        {
            "type": "episode",
            "title": _title(episode),
            "series_title": _title(show) or _title(row),
            "year": _year(show) or _year(row),
            "show_ids": scope_ids,
            "ids": ids or scope_ids,
            "season": season_no,
            "episode": episode_no,
        }
    )
    collected_at = episode.get("collected_at") or row.get("collected_at")
    if collected_at:
        item["collected_at"] = str(collected_at)
    return item


def _items_from_row(row: Mapping[str, Any], bucket: str) -> list[dict[str, Any]]:
    typ = str(row.get("type") or "").strip().lower()
    if typ.endswith("s") and typ in ("movies", "shows", "seasons", "episodes"):
        typ = typ[:-1]
    if bucket == "episodes" or typ == "episode":
        item = _episode_from_row(row)
        return [item] if item else []
    if bucket == "seasons" or typ == "season":
        item = _season_from_row(row)
        return [item] if item else []
    if bucket == "movies" or isinstance(row.get("movie"), Mapping):
        item = _movie_from_row(row)
        return [item] if item else []

    show_item = _show_base(row)
    show = row.get("show") if isinstance(row.get("show"), Mapping) else row
    seasons = row.get("seasons")
    if not isinstance(seasons, list) and isinstance(show, Mapping):
        seasons = show.get("seasons")
    if not isinstance(seasons, list) or not seasons:
        return [show_item] if show_item else []

    show_ids = dict((show_item or {}).get("ids") or {})
    title = (show_item or {}).get("title")
    year = (show_item or {}).get("year")
    out: list[dict[str, Any]] = []
    for season in seasons:
        if not isinstance(season, Mapping):
            continue
        season_no = season.get("number")
        episodes = season.get("episodes")
        collected_at = season.get("collected_at") or row.get("collected_at")
        if isinstance(episodes, list) and episodes:
            for ep in episodes:
                if not isinstance(ep, Mapping):
                    continue
                ep_no = ep.get("number")
                if season_no is None or ep_no is None:
                    continue
                item = id_minimal({"type": "episode", "title": ep.get("title"), "series_title": title, "year": year, "show_ids": show_ids, "ids": _ids_clean(ep.get("ids")), "season": season_no, "episode": ep_no})
                if ep.get("collected_at") or collected_at:
                    item["collected_at"] = str(ep.get("collected_at") or collected_at)
                out.append(item)
        elif season_no is not None:
            item = id_minimal({"type": "season", "title": title, "series_title": title, "year": year, "show_ids": show_ids, "ids": _ids_clean(season.get("ids")), "season": season_no})
            if collected_at:
                item["collected_at"] = str(collected_at)
            out.append(item)
    return out or ([show_item] if show_item else [])


def _fetch_last_activities(adapter: Any, *, apikey: str, timeout: float, retries: int) -> dict[str, Any] | None:
    client = getattr(adapter, "client", None)
    if client and hasattr(client, "last_activities"):
        try:
            data = client.last_activities()
            if isinstance(data, Mapping) and "error" not in data and "status" not in data:
                return dict(data)
        except Exception:
            pass
    try:
        r = mdblist_request(adapter, "GET", f"{BASE}/sync/last_activities", params={"apikey": apikey}, timeout=timeout, max_retries=retries)
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            return dict(data) if isinstance(data, Mapping) else None
    except Exception:
        pass
    return None


def _activity_collected_at(acts: Mapping[str, Any]) -> str | None:
    values = [acts.get("collected_at"), acts.get("collection_at"), acts.get("updated_at")]
    for nested_name in ("movies", "shows"):
        nested = acts.get(nested_name)
        if isinstance(nested, Mapping):
            values.extend([nested.get("collected_at"), nested.get("collection_at")])
    latest: str | None = None
    for value in values:
        if _iso_ok(value):
            latest = str(value) if latest is None else max(latest, str(value))
    return _iso_z(latest) if _iso_ok(latest) else None


def build_index(adapter: Any, *, per_page: int = 1000, max_pages: int = 250) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    apikey = _as_str(cfg.get("api_key")) or ""
    shadow = _shadow_load()
    cached: dict[str, dict[str, Any]] = dict(shadow.get("items") or {})
    if not has_auth(cfg):
        source = "shadow" if cached else "empty"
        _info("index_done", count=len(cached), source=source)
        return cached

    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries
    acts = _fetch_last_activities(adapter, apikey=apikey, timeout=timeout, retries=retries) or {}
    acts_ts = _activity_collected_at(acts)
    wm = get_watermark("collection")
    if acts_ts and wm and (_as_epoch(acts_ts) or 0) <= (_as_epoch(wm) or 0) and cached:
        _dbg("index_cache_hit", source="shadow", reason="activities_unchanged", collected_at=acts_ts, watermark=wm, count=len(cached))
        _info("index_done", count=len(cached), source="shadow")
        return cached
    if acts_ts and (not wm) and cached:
        save_watermark("collection", acts_ts)
        _dbg("index_cache_hit", source="shadow", reason="baseline_watermark_set", watermark=acts_ts, count=len(cached))
        _info("index_done", count=len(cached), source="shadow")
        return cached

    limit = max(1, min(_cfg_int(cfg, "collection_per_page", per_page), 5000))
    max_pages = max(1, min(_cfg_int(cfg, "collection_max_pages", max_pages), 2000))
    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("collection") if callable(prog_factory) else None
    out: dict[str, dict[str, Any]] = {}
    offset = 0
    pages = 0
    total_tick = 0

    while pages < max_pages:
        r = mdblist_request(adapter, "GET", URL_LIST, params={"apikey": apikey, "offset": offset, "limit": limit}, timeout=timeout, max_retries=retries)
        if r.status_code != 200:
            _warn("http_failed", op="index", status=r.status_code, offset=offset)
            break
        data = r.json() if (r.text or "").strip() else {}
        if not isinstance(data, Mapping):
            break
        rows_seen = 0
        for bucket in ("movies", "shows", "seasons", "episodes"):
            raw = data.get(bucket)
            if not isinstance(raw, list):
                continue
            for row in raw:
                if not isinstance(row, Mapping):
                    continue
                rows_seen += 1
                for item in _items_from_row(row, bucket):
                    key = _key_of(item)
                    if key:
                        out[key] = item
        total_tick += rows_seen
        if prog:
            prog.tick(total_tick, total=max(total_tick, offset + rows_seen))
        pag = data.get("pagination") if isinstance(data.get("pagination"), Mapping) else {}
        has_more = bool(pag.get("has_more")) if isinstance(pag, Mapping) else rows_seen >= limit
        if not rows_seen or not has_more:
            break
        offset += int(pag.get("limit") or limit) if isinstance(pag, Mapping) else limit
        pages += 1

    if out:
        _shadow_save(out)
    if acts_ts:
        save_watermark("collection", acts_ts)
    _info("index_done", count=len(out), source="current")
    return out


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    size = max(1, int(n or 1))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def _batch_payload(items: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for item in items or []:
        m = id_minimal(item)
        typ = _item_media_type(m)
        ids = _ids_for_mdblist(m, show_scope=typ in ("season", "episode"))
        if typ in ("season", "episode") and not ids:
            ids = _ids_for_mdblist(m)
        if not ids:
            rejected.append({"item": m, "hint": "missing_ids"})
            continue
        entry = {"type": typ, "ids": ids, "collected_at": m.get("collected_at")}
        if typ in ("season", "episode"):
            entry["season"] = m.get("season") if m.get("season") is not None else m.get("number")
        if typ == "episode":
            entry["episode"] = m.get("episode") if m.get("episode") is not None else m.get("episode_number")
        if typ == "season" and entry.get("season") is None:
            rejected.append({"item": m, "hint": "missing_season"})
            continue
        if typ == "episode" and (entry.get("season") is None or entry.get("episode") is None):
            rejected.append({"item": m, "hint": "missing_episode_scope"})
            continue
        accepted.append(entry)
    return accepted, rejected


def _attempted_items_by_key(items: Iterable[Mapping[str, Any]]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for item in items or []:
        m = id_minimal(item)
        key = _key_of(m)
        if key:
            out.setdefault(key, m)
    return out


def _unresolved_key_set(unresolved: Iterable[Mapping[str, Any]]) -> set[str]:
    out: set[str] = set()
    for row in unresolved or []:
        if not isinstance(row, Mapping):
            continue
        explicit = _as_str(row.get("key") or row.get("unresolved_key"))
        if explicit:
            out.add(explicit)
            continue
        item = row.get("item") if isinstance(row.get("item"), Mapping) else row
        if isinstance(item, Mapping):
            key = _key_of(id_minimal(item))
            if key:
                out.add(key)
    return out


def _with_unresolved_keys(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        item = row.get("item") if isinstance(row.get("item"), Mapping) else row
        hint = _as_str(row.get("hint") or row.get("reason") or row.get("error")) or "unknown"
        key = _as_str(row.get("key") or row.get("unresolved_key"))
        if isinstance(item, Mapping):
            out.append(_unresolved_entry(item, hint, key=key))
    return out


def _payload_from_accepted(items: list[dict[str, Any]], *, include_dates: bool) -> dict[str, Any]:
    body: dict[str, Any] = {}
    shows_by_key: dict[str, dict[str, Any]] = {}

    def show_key(ids: Mapping[str, Any]) -> str:
        return json.dumps({k: ids.get(k) for k in ("imdb", "tmdb", "tvdb", "trakt", "mdblist", "kitsu") if ids.get(k) is not None}, sort_keys=True)

    for item in items:
        typ = str(item.get("type") or "").lower()
        ids = dict(item.get("ids") or {})
        collected_at = item.get("collected_at")
        if typ == "movie":
            entry: dict[str, Any] = {"ids": ids}
            if include_dates and collected_at:
                entry["collected_at"] = str(collected_at)
            body.setdefault("movies", []).append(entry)
            continue
        if typ == "show":
            entry: dict[str, Any] = {"ids": ids}
            if include_dates and collected_at:
                entry["collected_at"] = str(collected_at)
            body.setdefault("shows", []).append(entry)
            continue

        skey = show_key(ids)
        show_entry = shows_by_key.setdefault(skey, {"ids": ids, "seasons": {}})
        season_raw = item.get("season")
        if season_raw is None:
            continue
        try:
            season_i = int(season_raw)
        except Exception:
            continue
        season_entry = show_entry["seasons"].setdefault(season_i, {"number": season_i})
        if include_dates and collected_at:
            season_entry.setdefault("collected_at", str(collected_at))
        if typ == "episode":
            episode_raw = item.get("episode")
            if episode_raw is None:
                continue
            try:
                episode_i = int(episode_raw)
            except Exception:
                continue
            ep_entry: dict[str, Any] = {"number": episode_i}
            if include_dates and collected_at:
                ep_entry["collected_at"] = str(collected_at)
            season_entry.setdefault("episodes", []).append(ep_entry)

    if shows_by_key:
        body.setdefault("shows", []).extend(
            {"ids": v["ids"], "seasons": list(v["seasons"].values())}
            for v in shows_by_key.values()
            if v.get("ids")
        )
    return body


def _count_result(value: Any) -> int:
    if not isinstance(value, Mapping):
        return 0
    return sum(int(_as_int(value.get(k)) or 0) for k in ("movies", "shows", "seasons", "episodes"))


def _write(adapter: Any, op: str, items: Iterable[Mapping[str, Any]]) -> dict[str, Any] | tuple[int, list[dict[str, Any]]]:
    cfg = _cfg(adapter)
    apikey = _as_str(cfg.get("api_key")) or ""
    item_list = list(items or [])
    if not has_auth(cfg):
        return 0, _with_unresolved_keys({"item": id_minimal(it), "hint": "missing_auth"} for it in item_list)
    accepted, unresolved = _batch_payload(item_list)
    unresolved = _with_unresolved_keys(unresolved)
    attempted_by_key = _attempted_items_by_key(item_list)
    if not accepted:
        _info("write_skipped", op=op, reason="empty_payload", unresolved=len(unresolved))
        return 0, unresolved

    chunk_size = _cfg_int(cfg, "collection_batch_size", 25)
    delay_ms = _cfg_int(cfg, "collection_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(cfg, "collection_max_backoff_ms", 8000)
    url = URL_LIST if op == "add" else URL_REMOVE
    ok = 0
    body = _payload_from_accepted(accepted, include_dates=(op == "add"))
    if not body:
        _info("write_skipped", op=op, reason="empty_payload", unresolved=len(unresolved))
        return 0, unresolved

    _dbg(
        "write_start",
        op=op,
        movies=len(body.get("movies") or []),
        shows=len(body.get("shows") or []),
        chunk_size=chunk_size,
    )

    for bucket in ("movies", "shows"):
        rows = body.get(bucket) or []
        if not rows:
            continue
        for part in _chunk(rows, chunk_size):
            payload = {bucket: part}
            attempt = 0
            backoff = delay_ms

            while True:
                r = mdblist_request(
                    adapter,
                    "POST",
                    url,
                    params={"apikey": apikey},
                    json=payload,
                    timeout=adapter.cfg.timeout,
                    max_retries=adapter.cfg.max_retries,
                )
                if r.status_code in (200, 201, 204):
                    chunk_ok = 0
                    if r.status_code == 204 or not (r.text or "").strip():
                        body_any: Any = {}
                    else:
                        try:
                            body_any = r.json()
                        except Exception:
                            body_any = {}
                    if isinstance(body_any, Mapping):
                        chunk_ok = _count_result(
                            body_any.get("updated")
                            or body_any.get("added")
                            or body_any.get("existing")
                            or body_any.get("deleted")
                            or body_any.get("removed")
                        )
                    ok += chunk_ok or len(part)
                    time.sleep(max(0.0, delay_ms / 1000.0))
                    break

                if r.status_code in (429, 503):
                    _warn(
                        "rate_limit",
                        op=op,
                        bucket=bucket,
                        status=r.status_code,
                        attempt=attempt,
                        backoff_ms=backoff,
                        body=(r.text or "")[:180],
                    )
                    time.sleep(min(max_backoff_ms, backoff) / 1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff * 1.6) + 200)
                    if attempt <= 4:
                        continue

                _warn("write_failed", op=op, bucket=bucket, status=r.status_code, body=(r.text or "")[:200])
                item_type = "show" if bucket == "shows" else "movie"
                for x in part:
                    unresolved.extend(
                        _with_unresolved_keys(
                            [{"item": id_minimal({"type": item_type, "ids": x.get("ids") or {}}), "hint": f"http:{r.status_code}"}]
                        )
                    )
                break
    if ok:
        _shadow_bust()
    if op == "add" and _cfg_bool(cfg, "collection_verify_after_write", True):
        rejected = _unresolved_key_set(unresolved)
        accepted_keys = [key for key in attempted_by_key if key not in rejected]
        live_keys: set[str] = set()
        verified_live = False
        if accepted_keys:
            try:
                live_keys = set(build_index(adapter).keys())
                verified_live = True
            except Exception as exc:
                _warn("verify_failed", error=f"{type(exc).__name__}: {exc}")
        if accepted_keys and not verified_live:
            _info("write_done", op=op, ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved), verified=False)
            return ok, unresolved
        presence = [key for key in accepted_keys if key in live_keys]
        not_seen = [key for key in accepted_keys if key not in live_keys]
        for key in not_seen:
            item = attempted_by_key.get(key)
            if item:
                unresolved.append(_unresolved_entry(item, "not_seen_live", key=key))
        if not_seen:
            _warn("write_unverified", op=op, accepted=len(accepted_keys), confirmed=len(presence), not_seen=len(not_seen))
        _info("write_done", op=op, ok=len(unresolved) == 0, applied=len(presence), unresolved=len(unresolved))
        return _collection_write_result(
            confirmed_keys=presence,
            unresolved=unresolved,
            attempted=len(item_list),
            accepted_keys=accepted_keys,
            presence_confirmed_keys=presence,
            accepted_not_seen_live_keys=not_seen,
        )
    _info("write_done", op=op, ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any] | tuple[int, list[dict[str, Any]]]:
    return _write(adapter, "add", items)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any] | tuple[int, list[dict[str, Any]]]:
    return _write(adapter, "remove", items)

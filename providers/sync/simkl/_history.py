# SIMKL Module for history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import re
import time
from datetime import datetime, timedelta, timezone
from difflib import SequenceMatcher
from itertools import chain
from pathlib import Path
from typing import Any, Iterable, Mapping, cast

from cw_platform.id_map import canonical_key as _canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import (
    SIMKLFetchError,
    adapter_headers,
    cache_anime_mappings,
    fetch_activities,
    extract_latest_ts,
    get_watermark,
    load_json_state,
    maybe_map_tvdb_ids,
    normalize_flat_watermarks,
    simkl_api_params_from_headers,
    key_of as simkl_key_of,
    normalize as simkl_normalize,
    save_json_state,
    slug_to_title,
    update_watermark_if_new,
    state_file,
)

BASE = "https://api.simkl.com"
URL_ALL_ITEMS = f"{BASE}/sync/all-items"
URL_ADD = f"{BASE}/sync/history"
URL_REMOVE = f"{BASE}/sync/history/remove"
URL_REDIRECT = f"{BASE}/redirect"
URL_ANIME_EPISODES = f"{BASE}/anime/episodes"
_CACHE_SCHEMA = 2


def _unresolved_path() -> str:
    return str(state_file("simkl_history.unresolved.json"))


def _anime_resolve_path() -> str:
    return str(state_file("simkl_history.anime_resolve.json"))


def _anime_episode_map_path() -> str:
    return str(state_file("simkl_history.anime_episode_map.json"))


def _anime_episode_alias_path() -> str:
    return str(state_file("simkl_history.anime_episode_alias.json"))


def _trakt_history_index_path() -> str:
    return str(state_file("trakt_history.index.json"))


ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")
_MOVIE_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl")  # anime IDs excluded to prevent SIMKL misrouting to anime bucket
_EPISODE_LOOKUP_ID_KEYS = ("tvdb", "anidb")

def _maybe_map_tvdb(adapter: Any, ids: Mapping[str, Any]) -> dict[str, str]:
    def _fetch_rows() -> Iterable[Mapping[str, Any]]:
        headers = _headers(adapter, force_refresh=True)
        try:
            resp = adapter.client.session.get(
                f"{BASE}/sync/all-items/anime",
                headers=headers,
                params=simkl_api_params_from_headers(headers, extended="full_anime_seasons"),
                timeout=adapter.cfg.timeout,
            )
            data = resp.json() if resp.ok else {}
        except Exception:
            return []
        if isinstance(data, Mapping):
            rows = data.get("anime")
            return rows if isinstance(rows, list) else []
        return data if isinstance(data, list) else []

    return maybe_map_tvdb_ids(adapter, ids, fetch_rows=_fetch_rows)


def _dedupe_history_movies(out: dict[str, dict[str, Any]]) -> None:
    if not out:
        return

    bucket_ids: dict[str, dict[str, Any]] = {}
    by_tvdb: dict[str, list[str]] = {}
    by_tmdb: dict[str, list[str]] = {}

    for event_key, item in out.items():
        if not isinstance(item, Mapping):
            continue
        if str(item.get("type") or "").lower() != "movie":
            continue
        bucket_key = event_key.split("@", 1)[0]
        ids = dict(item.get("ids") or {})
        if not ids:
            continue
        if bucket_key in bucket_ids:
            continue
        bucket_ids[bucket_key] = ids
        tvdb = (str(ids.get("tvdb") or "")).strip()
        tmdb = (str(ids.get("tmdb") or "")).strip()
        if tvdb:
            by_tvdb.setdefault(tvdb, []).append(bucket_key)
        if tmdb:
            by_tmdb.setdefault(tmdb, []).append(bucket_key)

    if not bucket_ids:
        return

    drop_buckets: set[str] = set()

    def pick(groups: dict[str, list[str]]) -> None:
        for _gid, keys in groups.items():
            if len(keys) < 2:
                continue
            canonical: str | None = None

            for k in keys:
                ids = bucket_ids.get(k) or {}
                if ids.get("plex") or ids.get("guid"):
                    canonical = k
                    break
            if canonical is None:
                canonical = keys[0]
            for k in keys:
                if k != canonical:
                    drop_buckets.add(k)

    pick(by_tvdb)
    pick(by_tmdb)

    if not drop_buckets:
        return

    to_drop: list[str] = [
        ek for ek in list(out.keys())
        if ek.split("@", 1)[0] in drop_buckets
    ]
    for ek in to_drop:
        out.pop(ek, None)

    _dbg("index_reconcile", reason="dedupe_applied", strategy="prefer_plex_guid", buckets=len(drop_buckets), events=len(to_drop))

def _safe_int(value: Any) -> int:
    try:
        n = int(value)
        return n if n > 0 else 0
    except Exception:
        return 0


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except Exception:
        return None


def _chunk_items(seq: list[Mapping[str, Any]], n: int) -> Iterable[list[Mapping[str, Any]]]:
    size = max(1, int(n or 1))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]

def _log(msg: str, *, level: str = "debug", **fields: Any) -> None:
    cw_log("SIMKL", "history", level, msg, **fields)


def _dbg(event: str, **fields: Any) -> None:
    _log(event, level="debug", **fields)


def _info(event: str, **fields: Any) -> None:
    _log(event, level="info", **fields)


def _warn(event: str, **fields: Any) -> None:
    _log(event, level="warn", **fields)


def _now_epoch() -> int:
    return int(time.time())


def _as_epoch(value: Any) -> int | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return int(value)
    if isinstance(value, datetime):
        dt = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    if isinstance(value, str):
        try:
            return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None


def _as_iso(ts: int) -> str:
    epoch = datetime(1970, 1, 1, tzinfo=timezone.utc)
    return (
        (epoch + timedelta(seconds=int(ts)))
        .isoformat()
        .replace("+00:00", "Z")
    )


def _history_activity_markers(acts: Mapping[str, Any]) -> tuple[str | None, str | None, str | None, str | None, str | None, str | None]:
    movie_latest = extract_latest_ts(acts, (("movies", "all"), ("movies", "completed")))
    show_latest = extract_latest_ts(acts, (("tv_shows", "all"), ("shows", "all"), ("tv_shows", "watching"), ("shows", "watching"), ("tv_shows", "completed"), ("shows", "completed")))
    anime_latest = extract_latest_ts(acts, (("anime", "all"), ("anime", "watching"), ("anime", "completed")))
    movie_removed = extract_latest_ts(acts, (("movies", "removed_from_list"), ("movies", "removed")))
    show_removed = extract_latest_ts(acts, (("tv_shows", "removed_from_list"), ("shows", "removed_from_list"), ("tv_shows", "removed"), ("shows", "removed")))
    anime_removed = extract_latest_ts(acts, (("anime", "removed_from_list"), ("anime", "removed")))
    return movie_latest, show_latest, anime_latest, movie_removed, show_removed, anime_removed


def _headers(adapter: Any, *, force_refresh: bool = False) -> dict[str, str]:
    return adapter_headers(adapter, force_refresh=force_refresh)


def _ids_of(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids = dict(obj.get("ids") or {})
    return {k: ids[k] for k in ID_KEYS if ids.get(k)}


def _episode_lookup_ids(item: Mapping[str, Any]) -> dict[str, str]:
    ids = dict(item.get("ids") or {})
    return {k: str(ids[k]) for k in _EPISODE_LOOKUP_ID_KEYS if ids.get(k)}


def _raw_show_ids(item: Mapping[str, Any]) -> dict[str, Any]:
    return dict(item.get("show_ids") or {})


def _thaw_key(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or "").lower()
    return simkl_key_of(item) if typ == "episode" else simkl_key_of(id_minimal(item))


def _show_ids_of_episode(item: Mapping[str, Any]) -> dict[str, Any]:
    show_ids = _raw_show_ids(item)
    return {k: show_ids[k] for k in ID_KEYS if show_ids.get(k)}


def _scope_ids_for_freeze(item: Mapping[str, Any]) -> dict[str, Any]:
    typ = str(item.get("type") or "").lower()
    if typ in ("season", "episode"):
        scoped = _show_ids_of_episode(item)
        if scoped:
            return scoped
    return _ids_of(item)


def _load_json(path: str) -> dict[str, Any]:
    return load_json_state(path)


def _save_json(path: str, data: Mapping[str, Any]) -> None:
    save_json_state(path, data)


def _is_null_env(row: Any) -> bool:
    return isinstance(row, Mapping) and row.get("type") == "null" and row.get("body") is None


def _load_unresolved() -> dict[str, Any]:
    return _load_json(_unresolved_path())


def _save_unresolved(data: Mapping[str, Any]) -> None:
    _save_json(_unresolved_path(), data)


def _freeze(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    ids_sent: Mapping[str, Any],
    watched_at: str | None,
) -> None:
    key = _thaw_key(item)
    data = _load_unresolved()
    row = data.get(key) or {
        "feature": "history",
        "action": action,
        "first_seen": _now_epoch(),
        "attempts": 0,
    }
    row.update({"item": id_minimal(item), "last_attempt": _now_epoch()})
    existing_reasons: list[str] = list(row.get("reasons", [])) if isinstance(row.get("reasons"), list) else []
    row["reasons"] = sorted(set(existing_reasons) | set(reasons or []))
    row["ids_sent"] = dict(ids_sent or {})
    if watched_at:
        row["watched_at"] = watched_at
    row["attempts"] = int(row.get("attempts", 0)) + 1
    data[key] = row
    _save_unresolved(data)


def _unfreeze(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for key in set(keys or []):
        if key in data:
            del data[key]
            changed = True
    if changed:
        _save_unresolved(data)



def _slug_to_title(slug: str | None) -> str:
    return slug_to_title(slug)


def _cache_path() -> str:
    return str(state_file("simkl.history.cache.json"))


def _cache_load() -> dict[str, dict[str, Any]]:
    data = _load_json(_cache_path())
    if not isinstance(data, dict):
        return {}
    if int(data.get("schema") or 0) != _CACHE_SCHEMA:
        return {}
    items = data.get("items")
    if not isinstance(items, dict):
        return {}
    out = {str(k): dict(v) for k, v in items.items() if isinstance(v, Mapping)}
    pruned = [k for k, v in out.items() if str((v or {}).get("type") or "").lower() == "season"]
    if pruned:
        for k in pruned:
            out.pop(k, None)
        _cache_save(out)
        _dbg("cache_pruned", reason="unsupported_season_rollups", count=len(pruned))
    return out


def _cache_doc_is_stale() -> bool:
    data = _load_json(_cache_path())
    if not isinstance(data, dict) or not data:
        return False
    return int(data.get("schema") or 0) != _CACHE_SCHEMA


def _cache_save(items: Mapping[str, Any]) -> None:
    _save_json(_cache_path(), {"schema": _CACHE_SCHEMA, "generated_at": _as_iso(_now_epoch()), "items": dict(items)})


def _evict_removes_from_cache(items_list: list[Mapping[str, Any]]) -> None:
    """Remove successfully-deleted items from the local history cache.

    The delta strategy only adds entries; it never subtracts them. Without this
    eviction, items deleted from SIMKL via the remove API remain in the cache,
    causing the orchestrator to re-plan the same removes on every subsequent sync.
    """
    if not items_list:
        return
    base_keys: set[str] = set()
    for item in items_list:
        if not isinstance(item, Mapping):
            continue
        k = _thaw_key(item)
        if k:
            base_keys.add(k)
    if not base_keys:
        return
    cached = _cache_load()
    evicted = [ek for ek in cached if ek.split("@", 1)[0] in base_keys]
    if not evicted:
        return
    for ek in evicted:
        del cached[ek]
    _cache_save(cached)
    _dbg("cache_evicted", count=len(evicted))


def _inject_adds_into_cache(items_list: list[Mapping[str, Any]]) -> None:
    """Inject newly-written items into the history cache immediately after a write.

    SIMKL's /sync/all-items?date_from filters by watched_at, not by ingestion time.
    Items added with historical watched_at dates (older than the current watermark)
    will never appear in future delta fetches, causing the orchestrator to re-plan
    them as missing on every sync. Updating the cache here prevents that loop.
    """
    if not items_list:
        return
    to_inject: dict[str, dict[str, Any]] = {}
    for item in items_list:
        if not isinstance(item, Mapping):
            continue
        watched_at = str(item.get("watched_at") or "").strip()
        if not watched_at:
            continue
        ts = _as_epoch(watched_at)
        if not ts:
            continue
        bucket_key = simkl_key_of(item)
        if not bucket_key:
            continue
        event_key = f"{bucket_key}@{ts}"
        item_type = str(item.get("type") or "").lower()
        entry: dict[str, Any] = {"type": item_type, "watched": True, "watched_at": watched_at}
        if item.get("ids"):
            entry["ids"] = {k: v for k, v in item["ids"].items() if v}
        if item.get("show_ids"):
            entry["show_ids"] = {k: v for k, v in item["show_ids"].items() if v}
        if item_type == "season":
            continue
        if item_type == "episode":
            entry["season"] = item.get("season")
            entry["episode"] = item.get("episode")
            entry["series_title"] = item.get("series_title")
            entry["simkl_bucket"] = str(item.get("simkl_bucket") or "shows").strip().lower() or "shows"
        else:
            entry["title"] = item.get("title")
            entry["year"] = item.get("year")
            bucket = str(item.get("simkl_bucket") or "").strip().lower()
            entry["simkl_bucket"] = bucket if bucket in {"movies", "shows", "anime"} else ("movies" if item_type == "movie" else "shows")
            anime_type = str(item.get("anime_type") or "").strip().lower()
            if anime_type:
                entry["anime_type"] = anime_type
        to_inject[event_key] = entry

    if not to_inject:
        return
    cached = _cache_load()
    cached.update(to_inject)
    _cache_save(cached)
    _dbg("cache_injected", count=len(to_inject))


def _response_bucket(simkl_type: Any) -> str | None:
    typ = str(simkl_type or "").strip().lower()
    if typ in {"movie", "movies"}:
        return "movies"
    if typ in {"show", "shows", "tv", "tv_show", "tv_shows"}:
        return "shows"
    if typ == "anime":
        return "anime"
    return None


def _response_classification(row: Any) -> dict[str, str]:
    if not isinstance(row, Mapping):
        return {}
    response = row.get("response")
    src: Mapping[str, Any] = response if isinstance(response, Mapping) else row
    out: dict[str, str] = {}
    bucket = _response_bucket(src.get("simkl_type") or src.get("type") or row.get("simkl_type"))
    if bucket:
        out["simkl_bucket"] = bucket
    anime_type = src.get("anime_type") or src.get("animeType") or row.get("anime_type") or row.get("animeType")
    if isinstance(anime_type, str) and anime_type.strip():
        out["anime_type"] = anime_type.strip().lower()
    status = src.get("status") or row.get("status")
    if isinstance(status, str) and status.strip():
        out["simkl_status"] = status.strip().lower()
    return out


def _response_ids(row: Any) -> dict[str, str]:
    if not isinstance(row, Mapping):
        return {}
    response = row.get("response")
    src: Mapping[str, Any] = response if isinstance(response, Mapping) else row
    ids = src.get("ids") if isinstance(src.get("ids"), Mapping) else row.get("ids")
    if not isinstance(ids, Mapping):
        return {}
    return {str(k).lower(): str(v) for k, v in ids.items() if v not in (None, "")}


def _classification_key(item: Mapping[str, Any]) -> str:
    return json.dumps(dict(_scope_ids_for_freeze(item) or _ids_of(item) or {}), sort_keys=True)


def _apply_response_classification(items_list: list[Mapping[str, Any]], payload: Mapping[str, Any]) -> None:
    added = payload.get("added")
    statuses = added.get("statuses") if isinstance(added, Mapping) else None
    if not isinstance(statuses, list):
        return

    by_key: dict[str, list[Mapping[str, Any]]] = {}
    by_id: dict[tuple[str, str], list[Mapping[str, Any]]] = {}
    for item in items_list:
        if not isinstance(item, Mapping):
            continue
        by_key.setdefault(_classification_key(item), []).append(item)
        for field, value in (_scope_ids_for_freeze(item) or _ids_of(item) or {}).items():
            if value not in (None, ""):
                by_id.setdefault((str(field).lower(), str(value)), []).append(item)

    fallback = [item for item in items_list if isinstance(item, Mapping)]
    for idx, row in enumerate(statuses):
        cls = _response_classification(row)
        if not cls:
            continue
        matches: list[Mapping[str, Any]] = []
        ids = _response_ids(row)
        for field, value in ids.items():
            matches.extend(by_id.get((field, value), []))
        if not matches and ids:
            matches = by_key.get(json.dumps(ids, sort_keys=True), [])
        if not matches and idx < len(fallback):
            matches = [fallback[idx]]
        for item in matches:
            if isinstance(item, dict):
                item.update(cls)


def _fetch_all_items(
    session: Any,
    headers: Mapping[str, str],
    *,
    since_iso: str | None,
    timeout: float,
) -> dict[str, list[dict[str, Any]]]:
    params = simkl_api_params_from_headers(
        headers,
        extended="full_anime_seasons",
        episode_watched_at="yes",
        include_all_episodes="yes",
    )
    if since_iso:
        params["date_from"] = since_iso
    try:
        resp = session.get(URL_ALL_ITEMS, headers=headers, params=params, timeout=timeout)
    except Exception as exc:
        _warn("http_failed", op="index", method="GET", url=URL_ALL_ITEMS, error=str(exc))
        raise SIMKLFetchError("history all-items request failed") from exc
    if not resp.ok:
        _warn("http_failed", op="index", method="GET", url=URL_ALL_ITEMS, status=resp.status_code)
        raise SIMKLFetchError(f"history all-items request failed with HTTP {resp.status_code}")
    try:
        body = resp.json()
    except Exception as exc:
        _warn("http_failed", op="index", method="GET", url=URL_ALL_ITEMS, reason="invalid_json")
        raise SIMKLFetchError("history all-items returned invalid JSON") from exc
    out: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": [], "anime": []}
    # SIMKL preserves a legacy response contract: applications with an internal
    # app ID <= 58447 receive JSON null for an empty result, while newer apps
    # receive the documented {}. SIMKL cannot change the legacy shape without
    # breaking backward compatibility, so CrossWatch accepts both.
    if body is None:
        _dbg("index_empty_response", shape="null", compatibility="legacy")
        return out
    if not isinstance(body, Mapping):
        response_headers = getattr(resp, "headers", {})
        content_type = ""
        if isinstance(response_headers, Mapping):
            content_type = str(
                response_headers.get("Content-Type")
                or response_headers.get("content-type")
                or ""
            ).split(";", 1)[0].strip()
        json_count = len(body) if isinstance(body, (list, tuple)) else None
        _warn(
            "http_failed",
            op="index",
            method="GET",
            url=URL_ALL_ITEMS,
            status=getattr(resp, "status_code", None),
            reason="invalid_response_shape",
            content_type=content_type or "unknown",
            json_type=type(body).__name__,
            json_count=json_count,
        )
        raise SIMKLFetchError("history all-items returned an invalid response shape")
    for kind in ("movies", "shows", "anime"):
        rows = body.get(kind)
        if isinstance(rows, list):
            out[kind] = [x for x in rows if isinstance(x, dict) and not _is_null_env(x)]
    cache_anime_mappings(out["anime"])
    return out


def _anime_type_from_row(row: Mapping[str, Any], show: Any, base: Mapping[str, Any]) -> str | None:
    for node in (show, row.get("anime"), row.get("show"), row, base):
        if not isinstance(node, Mapping):
            continue
        at = node.get("anime_type") or node.get("animeType")
        if isinstance(at, str) and at.strip():
            return at.strip().lower()
    return None


def _show_ids_overlap(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    for key in ("tmdb", "imdb", "tvdb", "trakt", "simkl"):
        lv = left.get(key)
        rv = right.get(key)
        if lv not in (None, "") and rv not in (None, "") and str(lv) == str(rv):
            return True
    return False


def _source_title_episode_aliases(show_ids: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    try:
        path = Path(_trakt_history_index_path())
        data = json.loads(path.read_text("utf-8")) if path.exists() else {}
    except Exception:
        data = {}
    items = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(items, Mapping):
        return {}

    grouped: dict[str, dict[tuple[int, int], dict[str, Any]]] = {}
    for item in items.values():
        if not isinstance(item, Mapping):
            continue
        if str(item.get("type") or "").lower() != "episode":
            continue
        raw_show_ids = item.get("show_ids")
        item_show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
        if not _show_ids_overlap(show_ids, item_show_ids):
            continue
        title_key = _title_match_key(item.get("title"))
        if not title_key:
            continue
        source = _source_season_episode(item)
        if source is None:
            continue
        s_num, e_num = source
        grouped.setdefault(title_key, {})[(s_num, e_num)] = {
            "season": s_num,
            "episode": e_num,
            "show_ids": dict(item_show_ids),
            "title": item.get("title"),
            "series_title": item.get("series_title"),
            "series_year": item.get("series_year") or item.get("year"),
        }

    out: dict[str, dict[str, Any]] = {}
    for title_key, choices in grouped.items():
        if len(choices) == 1:
            out[title_key] = next(iter(choices.values()))
    return out


def _source_abs_episode_aliases(show_ids: Mapping[str, Any]) -> dict[int, dict[str, Any]]:
    try:
        path = Path(_trakt_history_index_path())
        data = json.loads(path.read_text("utf-8")) if path.exists() else {}
    except Exception:
        data = {}
    items = data.get("items") if isinstance(data, Mapping) else None
    if not isinstance(items, Mapping):
        return {}

    grouped: dict[int, dict[tuple[int, int], dict[str, Any]]] = {}
    for item in items.values():
        if not isinstance(item, Mapping):
            continue
        if str(item.get("type") or "").lower() != "episode":
            continue
        abs_num = _int_or_none(item.get("_trakt_number_abs"))
        if abs_num is None or abs_num <= 0:
            continue
        raw_show_ids = item.get("show_ids")
        item_show_ids: Mapping[str, Any] = raw_show_ids if isinstance(raw_show_ids, Mapping) else {}
        if not _show_ids_overlap(show_ids, item_show_ids):
            continue
        source = _source_season_episode(item)
        if source is None:
            continue
        s_num, e_num = source
        grouped.setdefault(abs_num, {})[(s_num, e_num)] = {
            "season": s_num,
            "episode": e_num,
            "show_ids": dict(item_show_ids),
            "title": item.get("title"),
            "series_title": item.get("series_title"),
            "series_year": item.get("series_year") or item.get("year"),
        }

    out: dict[int, dict[str, Any]] = {}
    for abs_num, choices in grouped.items():
        if len(choices) == 1:
            out[abs_num] = next(iter(choices.values()))
    return out


def _apply_since_limit(
    out: dict[str, dict[str, Any]],
    *,
    since: int | None,
    limit: int | None,
) -> None:
    if since is not None:
        cutoff = int(since)
        for k in list(out.keys()):
            ts = _safe_int(str(k).rsplit("@", 1)[-1])
            if ts and ts < cutoff:
                out.pop(k, None)

    if limit is None:
        return
    try:
        lim = int(limit)
    except Exception:
        return
    if lim <= 0 or len(out) <= lim:
        return

    scored: list[tuple[int, str]] = []
    for k in out.keys():
        ts = _safe_int(str(k).rsplit("@", 1)[-1])
        scored.append((ts, str(k)))
    scored.sort(reverse=True)
    keep = {k for _ts, k in scored[:lim]}
    for k in list(out.keys()):
        if k not in keep:
            out.pop(k, None)


def _parse_rows(
    movie_rows: list[Any],
    show_rows: list[Any],
    anime_rows: list[Any],
    *,
    session: Any | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: float | None = None,
    limit: int | None,
) -> tuple[dict[str, dict[str, Any]], set[str], int | None, int | None, int | None, int, int]:
    """Parse raw API rows into history event dicts. Returns (out, thaw, latest_movies, latest_shows, latest_anime, movies_cnt, eps_cnt)."""
    out: dict[str, dict[str, Any]] = {}
    thaw: set[str] = set()
    latest_ts_movies: int | None = None
    latest_ts_shows: int | None = None
    latest_ts_anime: int | None = None
    added = 0
    movies_cnt = 0
    eps_cnt = 0
    anime_aliases = _load_anime_episode_alias_cache()
    anime_episode_cache = _load_anime_episode_map_cache()
    source_title_alias_cache: dict[str, dict[str, dict[str, Any]]] = {}
    source_abs_alias_cache: dict[str, dict[int, dict[str, Any]]] = {}

    for row in movie_rows:
        if not isinstance(row, Mapping):
            continue
        watched_at = (row.get("last_watched_at") or row.get("watched_at") or "").strip()
        ts = _as_epoch(watched_at)
        if not ts:
            continue
        movie_media = {"movie": row.get("movie")} if isinstance(row.get("movie"), Mapping) else row
        movie_norm = simkl_normalize(cast(Mapping[str, Any], movie_media))
        if not movie_norm or str(movie_norm.get("type") or "").lower() != "movie":
            continue
        movie_norm["watched"] = True
        movie_norm["watched_at"] = watched_at
        movie_norm["simkl_bucket"] = "movies"
        bucket_key = simkl_key_of(movie_norm)
        event_key = f"{bucket_key}@{ts}"
        if event_key in out:
            continue
        out[event_key] = movie_norm
        thaw.add(bucket_key)
        movies_cnt += 1
        added += 1
        latest_ts_movies = max(latest_ts_movies or 0, ts)
        if limit and added >= limit:
            return out, thaw, latest_ts_movies, latest_ts_shows, latest_ts_anime, movies_cnt, eps_cnt

    for row, row_kind in chain(((r, "shows") for r in show_rows), ((r, "anime") for r in anime_rows)):
        if not isinstance(row, Mapping):
            continue
        show = row.get("show") or row
        if not show:
            continue
        base = simkl_normalize(row)
        show_ids = _ids_of(base) or _ids_of(show)
        if not show_ids:
            continue
        show_title = str(
            base.get("title") or (show.get("title") if isinstance(show, Mapping) else "") or "",
        ).strip()
        show_year = base.get("year") or (show.get("year") if isinstance(show, Mapping) else None)
        series_name: str | None = show_title or (base.get("title") if isinstance(base, Mapping) else None)
        if row_kind == "anime":
            raw_ids = show.get("ids") if isinstance(show, Mapping) else None
            if isinstance(raw_ids, Mapping):
                slug = raw_ids.get("tvdbslug") or raw_ids.get("trakttvslug")
                if isinstance(slug, str) and slug:
                    series_name = _slug_to_title(slug) or series_name
        if not (series_name.strip() if isinstance(series_name, str) else ""):
            sid = str(show_ids.get("simkl") or "").strip()
            series_name = f"SIMKL:{sid}" if sid else "Unknown Series"
        if row_kind == "anime":
            anime_type = _anime_type_from_row(row, show, base)
            if anime_type == "movie":
                watched_at = (row.get("last_watched_at") or row.get("watched_at") or "").strip()
                if not watched_at:
                    best_ts = 0
                    best = ""
                    for season in row.get("seasons") or []:
                        season = season if isinstance(season, Mapping) else {}
                        for episode in (season.get("episodes") or []):
                            episode = episode if isinstance(episode, Mapping) else {}
                            wa = (episode.get("watched_at") or episode.get("last_watched_at") or "").strip()
                            ts_wa = _as_epoch(wa)
                            if ts_wa and ts_wa > best_ts:
                                best_ts = ts_wa
                                best = wa
                    watched_at = best
                ts = _as_epoch(watched_at)
                if ts:
                    movie_item: dict[str, Any] = {
                        "type": "movie",
                        "title": series_name,
                        "year": show_year,
                        "ids": dict(show_ids),
                        "simkl_bucket": "anime",
                        "anime_type": "movie",
                        "watched": True,
                        "watched_at": watched_at,
                    }
                    bucket_key = simkl_key_of(movie_item)
                    event_key = f"{bucket_key}@{ts}"
                    if event_key not in out:
                        out[event_key] = movie_item
                        thaw.add(bucket_key)
                        added += 1
                        latest_ts_anime = max(latest_ts_anime or 0, ts)
                        if limit and added >= limit:
                            return out, thaw, latest_ts_movies, latest_ts_shows, latest_ts_anime, movies_cnt, eps_cnt
                continue
        for season in row.get("seasons") or []:
            season = season if isinstance(season, Mapping) else {}
            raw_season = season.get("number") if season.get("number") is not None else season.get("season")
            s_num_internal = _int_or_none(raw_season)
            if s_num_internal is None or s_num_internal < 0:
                continue
            for episode in (season.get("episodes") or []):
                episode = episode if isinstance(episode, Mapping) else {}
                raw_episode = episode.get("number") if episode.get("number") is not None else episode.get("episode")
                e_num_internal = _int_or_none(raw_episode)
                if e_num_internal is None or e_num_internal <= 0:
                    continue
                s_num = s_num_internal
                e_num = e_num_internal
                alias: Mapping[str, Any] | None = None
                if row_kind == "anime":
                    alias_key = _anime_episode_alias_key(show_ids, e_num_internal)
                    alias_raw = anime_aliases.get(alias_key or "")
                    if isinstance(alias_raw, Mapping):
                        alias = alias_raw
                    if alias is None:
                        show_key = json.dumps({str(k): str(v) for k, v in show_ids.items() if v not in (None, "")}, sort_keys=True)
                        if show_key not in source_abs_alias_cache:
                            source_abs_alias_cache[show_key] = _source_abs_episode_aliases(show_ids)
                        abs_alias = source_abs_alias_cache.get(show_key, {}).get(e_num_internal)
                        if isinstance(abs_alias, Mapping):
                            alias = abs_alias
                    if alias is None:
                        title_value = episode.get("title")
                        if not _title_match_key(title_value) and session is not None and headers is not None and timeout is not None:
                            title_value = _anime_episode_title_for_number(
                                session,
                                headers,
                                timeout,
                                show_ids,
                                e_num_internal,
                                anime_episode_cache,
                            )
                        title_key = _title_match_key(title_value)
                        if title_key:
                            if show_key not in source_title_alias_cache:
                                source_title_alias_cache[show_key] = _source_title_episode_aliases(show_ids)
                            source_alias = source_title_alias_cache.get(show_key, {}).get(title_key)
                            if isinstance(source_alias, Mapping):
                                alias = source_alias
                            if alias is None:
                                alias = _source_episode_alias_for_native_number(
                                    source_title_alias_cache.get(show_key, {}),
                                    title_value,
                                    e_num_internal,
                                )
                    tvdb_map = episode.get("tvdb")
                    if alias is not None:
                        s_m = _int_or_none(alias.get("season"))
                        e_m = _int_or_none(alias.get("episode"))
                        if s_m is not None and s_m >= 0 and e_m is not None and e_m > 0:
                            s_num = s_m
                            e_num = e_m
                    elif isinstance(tvdb_map, Mapping):
                        s_m = int(tvdb_map.get("season") or 0)
                        e_m = int(tvdb_map.get("episode") or 0)
                        if s_m >= 1 and e_m >= 1:
                            s_num = s_m
                            e_num = e_m
                    elif session is not None and headers is not None and timeout is not None:
                        mapped = _anime_tvdb_season_episode_for_number(
                            session,
                            headers,
                            timeout,
                            show_ids,
                            e_num_internal,
                            anime_episode_cache,
                        )
                        if mapped is not None:
                            s_num, e_num = mapped
                watched_at = (episode.get("watched_at") or episode.get("last_watched_at") or "").strip()
                ts = _as_epoch(watched_at)
                if not ts or s_num < 0 or e_num <= 0:
                    continue
                episode_ids = _episode_lookup_ids(episode)
                alias_show_ids = alias.get("show_ids") if isinstance(alias, Mapping) and isinstance(alias.get("show_ids"), Mapping) else None
                alias_title = alias.get("title") if isinstance(alias, Mapping) else None
                alias_series_title = alias.get("series_title") if isinstance(alias, Mapping) else None
                alias_series_year = alias.get("series_year") if isinstance(alias, Mapping) else None
                ep = {
                    "type": "episode",
                    "season": s_num,
                    "episode": e_num,
                    "ids": dict(episode_ids or alias_show_ids or show_ids),
                    "title": alias_title if isinstance(alias_title, str) and alias_title.strip() else f"S{s_num:02d}E{e_num:02d}",
                    "year": None,
                    "series_title": alias_series_title if isinstance(alias_series_title, str) and alias_series_title.strip() else series_name,
                    "series_year": alias_series_year if alias_series_year not in (None, "") else show_year,
                    "show_ids": dict(alias_show_ids or show_ids),
                    "watched": True,
                    "watched_at": watched_at,
                    "simkl_bucket": row_kind,
                }
                bucket_key = simkl_key_of(ep)
                event_key = f"{bucket_key}@{ts}"
                if event_key in out:
                    continue
                out[event_key] = ep
                thaw.add(bucket_key)
                eps_cnt += 1
                added += 1
                if row_kind == "anime":
                    latest_ts_anime = max(latest_ts_anime or 0, ts)
                else:
                    latest_ts_shows = max(latest_ts_shows or 0, ts)
                if limit and added >= limit:
                    return out, thaw, latest_ts_movies, latest_ts_shows, latest_ts_anime, movies_cnt, eps_cnt

    return out, thaw, latest_ts_movies, latest_ts_shows, latest_ts_anime, movies_cnt, eps_cnt


def _delta_touched_scope_keys(
    fetched: Mapping[str, Mapping[str, Any]],
) -> tuple[set[str], set[str]]:
    movie_keys: set[str] = set()
    show_keys: set[str] = set()

    for item in (fetched or {}).values():
        if not isinstance(item, Mapping):
            continue
        typ = str(item.get("type") or "").lower()
        if typ == "movie":
            mk = simkl_key_of(item)
            if mk:
                movie_keys.add(mk)
            continue
        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if not show_ids and typ == "show":
            show_ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else None
        if not show_ids:
            continue
        sk = _canonical_key({"type": "show", "ids": dict(show_ids)})
        if sk:
            show_keys.add(sk)

    return movie_keys, show_keys


def _evict_delta_touched_scopes(
    cached: Mapping[str, Mapping[str, Any]],
    *,
    touched_movie_keys: set[str],
    touched_show_keys: set[str],
) -> dict[str, dict[str, Any]]:
    if not cached:
        return {}
    if not touched_movie_keys and not touched_show_keys:
        return {str(k): dict(v) for k, v in cached.items() if isinstance(v, Mapping)}

    out: dict[str, dict[str, Any]] = {}
    for ek, item in cached.items():
        if not isinstance(item, Mapping):
            continue
        base_key = str(ek).split("@", 1)[0]
        if base_key in touched_movie_keys:
            continue

        show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else None
        if show_ids:
            sk = _canonical_key({"type": "show", "ids": dict(show_ids)})
            if sk and sk in touched_show_keys:
                continue

        out[str(ek)] = dict(item)
    return out


def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    session = adapter.client.session
    timeout = adapter.cfg.timeout
    normalize_flat_watermarks()

    cached = _cache_load()
    cache_stale = _cache_doc_is_stale()
    wm = "" if cache_stale else (get_watermark("history") or "")
    removed_wm = get_watermark("history_removed") or ""

    acts, _ = fetch_activities(session, _headers(adapter, force_refresh=True), timeout=timeout)

    act_latest: str | None = None
    rm_m: str | None = None
    rm_s: str | None = None
    rm_a: str | None = None
    removal_changed = False

    if isinstance(acts, Mapping):
        lm, ls, la, rm_m, rm_s, rm_a = _history_activity_markers(acts)
        candidates = [t for t in (lm, ls, la) if isinstance(t, str) and t]
        act_latest = max(candidates) if candidates else None

        removal_candidates = [t for t in (rm_m, rm_s, rm_a) if isinstance(t, str) and t]
        removal_changed = bool(removed_wm) and any(t > removed_wm for t in removal_candidates)

        unchanged = bool(wm) and (not act_latest or act_latest <= wm) and not removal_changed
        if unchanged and cached:
            _dbg("index_cache_hit", source="cache", reason="activities_unchanged", watermark=wm, count=len(cached))
            _info("index_done", count=len(cached), source="cache")
            out = dict(cached)
            _apply_since_limit(out, since=since, limit=limit)
            return out
    else:
        # Activities fetch failed - using cache to avoid full fetch
        if cached:
            _warn("index_reconcile", reason="activities_fetch_failed", source="cache_fallback")
            _info("index_done", count=len(cached), source="cache_fallback")
            out = dict(cached)
            _apply_since_limit(out, since=since, limit=limit)
            return out

    if not wm:
        # First sync: full fetch without date_from
        date_from: str | None = None
        strategy = "full"
        reason = "cold_start"
    elif removal_changed:
        # removed_from_list changed: full fetch to get current state, replace cache
        date_from = None
        strategy = "full_replace"
        reason = "removed_from_list_changed"
    else:
        # Activities changed: delta fetch from watermark, merge into cache
        date_from = wm
        strategy = "delta"
        reason = "activities_changed"

    _dbg("index_reconcile", reason=reason, strategy=strategy, date_from=date_from or "-", watermark=wm or "-")

    headers = _headers(adapter, force_refresh=True)
    try:
        rows_by_kind = _fetch_all_items(session, headers, since_iso=date_from, timeout=timeout)
    except SIMKLFetchError:
        if not cached:
            raise
        _warn("index_reconcile", reason="all_items_fetch_failed", source="cache_fallback")
        _info("index_done", count=len(cached), source="cache_fallback")
        out = dict(cached)
        _apply_since_limit(out, since=since, limit=limit)
        return out
    movie_rows = list(rows_by_kind.get("movies") or [])
    show_rows = list(rows_by_kind.get("shows") or [])
    anime_rows = list(rows_by_kind.get("anime") or [])

    fetched, thaw, latest_ts_movies, latest_ts_shows, latest_ts_anime, movies_cnt, eps_cnt = _parse_rows(
        movie_rows,
        show_rows,
        anime_rows,
        session=session,
        headers=headers,
        timeout=timeout,
        limit=None,  # apply limit to final result only
    )
    _dedupe_history_movies(fetched)
    _dbg("index_fetch_counts", movies=movies_cnt, episodes=eps_cnt, from_date=date_from or "")

    # An empty delta is complete: retain the cache and let the activity
    # watermark advance below. Do not turn it into another full-library call.
    if strategy == "delta" and not fetched and (movies_cnt == 0 and eps_cnt == 0):
        _dbg(
            "index_reconcile",
            reason="incremental_empty",
            strategy="delta_keep_cache",
            watermark=wm or "-",
        )

    # Build final index
    if strategy == "delta":
        touched_movie_keys, touched_show_keys = _delta_touched_scope_keys(fetched)
        final = _evict_delta_touched_scopes(
            cached,
            touched_movie_keys=touched_movie_keys,
            touched_show_keys=touched_show_keys,
        )
        final.update(fetched)
        _dedupe_history_movies(final)
    else:
        final = fetched

    _cache_save(final)

    # Update watermarks
    latest_any = max([t for t in (latest_ts_movies, latest_ts_shows, latest_ts_anime) if isinstance(t, int)], default=None)
    if latest_any is not None:
        update_watermark_if_new("history", _as_iso(latest_any))
    elif act_latest:
        update_watermark_if_new("history", act_latest)

    # Initialize watermark
    removal_candidates = [t for t in (rm_m, rm_s, rm_a) if isinstance(t, str) and t]
    if removal_candidates:
        update_watermark_if_new("history_removed", max(removal_candidates))

    _unfreeze(thaw)
    _info("index_done", count=len(final), strategy=strategy, source="live")

    result = dict(final)
    _apply_since_limit(result, since=since, limit=limit)
    return result

def _movie_add_entry(item: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = {k: v for k, v in _ids_of(item).items() if k in _MOVIE_ID_KEYS}
    watched_at = (item.get("watched_at") or item.get("watchedAt") or "").strip()
    if not ids or not watched_at:
        return None
    return {"ids": ids, "watched_at": watched_at}


def _is_anime_like(item: Mapping[str, Any], ids: Mapping[str, Any]) -> bool:
    bucket = str(item.get("simkl_bucket") or "").strip().lower()
    if bucket == "anime":
        return True
    typ = str(item.get("type") or "").lower()
    if typ == "anime":
        return True
    for k in ("mal", "anidb", "anilist", "kitsu"):
        if ids.get(k):
            return True
    return False


def _show_add_entry(adapter: Any, item: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(item)
    if not ids:
        return None
    if _is_anime_like(item, ids):
        ids = _maybe_map_tvdb(adapter, ids)
    return {"ids": ids, "use_tvdb_anime_seasons": True}


def _show_scope_entry(
    adapter: Any,
    item: Mapping[str, Any],
    raw_show_ids: Mapping[str, Any],
    *,
    force_anime: bool = False,
) -> dict[str, Any] | None:
    show_ids = {k: str(raw_show_ids[k]) for k in ID_KEYS if raw_show_ids.get(k)}
    anime_like = force_anime or _is_anime_like(item, show_ids)
    if anime_like:
        show_ids = _maybe_map_tvdb(adapter, show_ids)
    if not show_ids:
        return None

    show: dict[str, Any] = {"ids": show_ids}
    if not anime_like:
        show["use_tvdb_anime_seasons"] = True

    show_title = item.get("series_title") or item.get("title")
    if isinstance(show_title, str) and show_title.strip():
        show["title"] = show_title.strip()

    series_year = item.get("series_year") or item.get("year")
    if isinstance(series_year, int):
        show["year"] = series_year
    elif isinstance(series_year, str) and series_year.isdigit():
        show["year"] = int(series_year)

    return show


def _episode_add_entry(
    adapter: Any, item: Mapping[str, Any]
) -> tuple[tuple[dict[str, Any], int, int, str, dict[str, str]] | None, str | None]:
    show_ids_raw = _show_ids_of_episode(item)
    if not show_ids_raw:
        return None, "missing_show_ids"
    raw_season = item.get("season") if item.get("season") is not None else item.get("season_number")
    s_num = _safe_int(raw_season)
    e_num = _safe_int(item.get("episode") or item.get("episode_number"))
    watched_at = item.get("watched_at") or item.get("watchedAt")
    episode_ids = _episode_lookup_ids(item)
    if not e_num:
        return None, "missing_episode_number"
    if not isinstance(watched_at, str) or not watched_at:
        return None, "missing_watched_at"
    if not s_num:
        if _int_or_none(raw_season) == 0:
            s_num = 0
        else:
            return None, "missing_season"

    if s_num == 0 and not episode_ids:
        return None, "season_zero_missing_episode_lookup_id"

    anime_force = s_num > 0 and _is_anime_like(item, show_ids_raw)
    show = _show_scope_entry(adapter, item, show_ids_raw, force_anime=anime_force)
    if not show:
        return None, "missing_show_ids"

    return (show, s_num, e_num, watched_at, episode_ids), None


def _merge_show_group(groups: dict[str, dict[str, Any]], show_entry: Mapping[str, Any]) -> dict[str, Any]:
    ids_key = json.dumps(show_entry.get("ids") or {}, sort_keys=True)
    group = groups.setdefault(ids_key, {"ids": dict(show_entry.get("ids") or {}), "seasons": []})
    for key in ("title", "year", "use_tvdb_anime_seasons"):
        value = show_entry.get(key)
        if value not in (None, "", False):
            group[key] = value
    return group


def _merge_show_season(group: dict[str, Any], season_number: int, *, watched_at: str | None = None) -> dict[str, Any]:
    # Ensure seasons list exists and is well-typed for analyzers.
    seasons_obj = group.setdefault("seasons", [])
    if not isinstance(seasons_obj, list):
        seasons_obj = []
        group["seasons"] = seasons_obj
    seasons = cast(list[dict[str, Any]], seasons_obj)
    season: dict[str, Any] | None = next(
        (s for s in seasons if isinstance(s, dict) and s.get("number") == season_number),
        None,
    )
    if season is None:
        season = {"number": season_number}
        seasons.append(season)
    if isinstance(watched_at, str) and watched_at and not season.get("watched_at"):
        season["watched_at"] = watched_at
    return season


def _write_failure_hint(resp: Any = None, exc: Exception | None = None, *, reason: str = "write_failed") -> str:
    if exc is not None:
        name = exc.__class__.__name__ or "Exception"
        return f"simkl_{reason}:{name}"
    status = getattr(resp, "status_code", None)
    if status is not None:
        return f"simkl_{reason}:http_{status}"
    return f"simkl_{reason}"


def _unresolved_for_items(items_list: Iterable[Mapping[str, Any]], hint: str) -> list[dict[str, Any]]:
    return [{"item": id_minimal(item), "hint": hint, "reason": hint} for item in items_list if isinstance(item, Mapping)]


def _freeze_failed_adds(items_list: Iterable[Mapping[str, Any]], hint: str) -> None:
    for item in items_list:
        ids = _scope_ids_for_freeze(item)
        watched_at = item.get("watched_at") or item.get("watchedAt") or None
        watched_str = watched_at if isinstance(watched_at, str) else None
        if ids:
            _freeze(
                item,
                action="add",
                reasons=[hint],
                ids_sent=ids,
                watched_at=watched_str,
            )


def _parse_add_payload(resp: Any, *, op: str) -> tuple[dict[str, Any], dict[str, int], dict[str, list[Any]], str | None]:
    added_new = {"movies": 0, "shows": 0, "episodes": 0}
    not_found: dict[str, list[Any]] = {"movies": [], "shows": [], "episodes": []}
    text = str(getattr(resp, "text", "") or "")
    if not text.strip():
        return {}, added_new, not_found, None
    try:
        payload_raw = resp.json()
    except Exception as exc:
        _dbg("parse_failed", op=op, error=str(exc))
        return {}, added_new, not_found, "simkl_write_response_malformed:json"
    if not isinstance(payload_raw, dict):
        return {}, added_new, not_found, f"simkl_write_response_malformed:{type(payload_raw).__name__}"
    a = payload_raw.get("added")
    if isinstance(a, dict):
        for key in ("movies", "shows", "episodes"):
            try:
                added_new[key] = int(a.get(key) or 0)
            except Exception:
                added_new[key] = 0
    nf = payload_raw.get("not_found")
    if isinstance(nf, dict):
        for key in ("movies", "shows", "episodes"):
            value = nf.get(key)
            not_found[key] = list(value) if isinstance(value, list) else []
    return payload_raw, added_new, not_found, None


def _reported_add_count(
    added_new: Mapping[str, int],
    *,
    movies_count: int,
    whole_shows_count: int,
    episodes_count: int,
) -> int:
    return (
        min(int(added_new.get("movies") or 0), max(0, movies_count))
        + min(int(added_new.get("shows") or 0), max(0, whole_shows_count))
        + min(int(added_new.get("episodes") or 0), max(0, episodes_count))
    )


def _has_not_found(not_found: Mapping[str, list[Any]]) -> bool:
    return any(bool(not_found.get(key)) for key in ("movies", "shows", "episodes"))


def _not_found_confirms_anime(obj: Mapping[str, Any]) -> bool:
    for node in (obj.get("response"), obj.get("show"), obj.get("anime"), obj):
        if not isinstance(node, Mapping):
            continue
        if _node_confirms_anime(node):
            return True
    return False


def _node_confirms_anime(node: Mapping[str, Any]) -> bool:
    bucket = _response_bucket(node.get("simkl_type") or node.get("type"))
    if bucket == "anime":
        return True
    anime_type = node.get("anime_type") or node.get("animeType")
    if isinstance(anime_type, str) and anime_type.strip():
        return True
    ids = node.get("ids")
    return isinstance(ids, Mapping) and any(ids.get(k) for k in ("mal", "anidb", "anilist", "kitsu"))


def _anime_retry_show_ids(item: Mapping[str, Any]) -> dict[str, str]:
    show_ids = _show_ids_of_episode(item)
    return {k: str(show_ids[k]) for k in ("tvdb",) if show_ids.get(k)}


def _response_show_retry_ids(obj: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for source, node in (("response", obj.get("response")), ("show", obj.get("show")), ("anime", obj.get("anime")), ("request", obj)):
        if not isinstance(node, Mapping):
            continue
        ids = node.get("ids")
        if not isinstance(ids, Mapping):
            continue
        value = ids.get("tvdb")
        if value not in (None, ""):
            out["tvdb"] = str(value)
        value = ids.get("simkl")
        if value not in (None, "") and source in {"response", "anime"} and _node_confirms_anime(node):
            out["simkl"] = str(value)
    return out


def _load_anime_resolve_cache() -> dict[str, str]:
    data = _load_json(_anime_resolve_path())
    rows = data.get("tvdb_to_simkl") if isinstance(data, Mapping) else None
    return {str(k): str(v) for k, v in dict(rows or {}).items() if k and v}


def _save_anime_resolve_cache(rows: Mapping[str, str]) -> None:
    _save_json(_anime_resolve_path(), {"tvdb_to_simkl": dict(rows), "updated_at": _as_iso(_now_epoch())})


def _load_anime_episode_map_cache() -> dict[str, list[dict[str, Any]]]:
    data = _load_json(_anime_episode_map_path())
    shows = data.get("shows") if isinstance(data, Mapping) else None
    out: dict[str, list[dict[str, Any]]] = {}
    if not isinstance(shows, Mapping):
        return out
    for key, rows in shows.items():
        if not key or not isinstance(rows, list):
            continue
        clean = [dict(row) for row in rows if isinstance(row, Mapping)]
        if clean:
            out[str(key)] = clean
    return out


def _save_anime_episode_map_cache(rows: Mapping[str, list[dict[str, Any]]]) -> None:
    _save_json(_anime_episode_map_path(), {"shows": dict(rows), "updated_at": _as_iso(_now_epoch())})


def _load_anime_episode_alias_cache() -> dict[str, dict[str, Any]]:
    data = _load_json(_anime_episode_alias_path())
    rows = data.get("episodes") if isinstance(data, Mapping) else None
    if not isinstance(rows, Mapping):
        return {}
    return {str(k): dict(v) for k, v in rows.items() if k and isinstance(v, Mapping)}


def _save_anime_episode_alias_cache(rows: Mapping[str, Mapping[str, Any]]) -> None:
    _save_json(_anime_episode_alias_path(), {"episodes": dict(rows), "updated_at": _as_iso(_now_epoch())})


def _anime_episode_alias_key(ids: Mapping[str, Any], episode_number: int) -> str | None:
    simkl_id = str(ids.get("simkl") or ids.get("simkl_id") or "").strip()
    if not simkl_id:
        return None
    try:
        ep_num = int(episode_number)
    except Exception:
        return None
    if ep_num <= 0:
        return None
    return f"{simkl_id}:{ep_num}"


def _remember_anime_episode_aliases(
    retry_payload: Mapping[str, Any],
    retry_index: Mapping[tuple[str, int], Mapping[str, Any]],
    accepted: set[str],
) -> None:
    if not accepted:
        return
    rows = _load_anime_episode_alias_cache()
    changed = False
    ids_by_group: dict[str, dict[str, Any]] = {}
    for anime in retry_payload.get("anime") or []:
        if not isinstance(anime, Mapping):
            continue
        ids = {str(k): str(v) for k, v in dict(anime.get("ids") or {}).items() if v is not None}
        if ids:
            ids_by_group[json.dumps(ids, sort_keys=True)] = ids
    for (group_key, ep_num), item in retry_index.items():
        item_key = _thaw_key(item)
        if item_key not in accepted:
            continue
        alias_key = _anime_episode_alias_key(ids_by_group.get(group_key) or {}, ep_num)
        if not alias_key:
            continue
        raw_season = item.get("season") if item.get("season") is not None else item.get("season_number")
        raw_episode = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
        s_num = _int_or_none(raw_season)
        e_num = _int_or_none(raw_episode)
        if s_num is None or s_num < 0 or e_num is None or e_num <= 0:
            continue
        show_ids = _show_ids_of_episode(item)
        if not show_ids:
            continue
        rows[alias_key] = {
            "season": s_num,
            "episode": e_num,
            "show_ids": {str(k): str(v) for k, v in show_ids.items() if v is not None},
            "title": item.get("title"),
            "series_title": item.get("series_title"),
            "series_year": item.get("series_year") or item.get("year"),
        }
        changed = True
    if changed:
        _save_anime_episode_alias_cache(rows)


def _title_match_key(value: Any) -> str:
    text = str(value or "").strip().lower()
    if not text:
        return ""
    return " ".join(part for part in re.split(r"[^a-z0-9]+", text) if part)


def _title_loose_match(left: Any, right: Any) -> bool:
    left_key = _title_match_key(left)
    right_key = _title_match_key(right)
    if not left_key or not right_key:
        return False
    if left_key == right_key:
        return True
    left_tokens = set(left_key.split())
    right_tokens = set(right_key.split())
    overlap = len(left_tokens & right_tokens)
    token_score = overlap / max(len(left_tokens | right_tokens), 1)
    sequence_score = SequenceMatcher(None, left_key, right_key).ratio()
    return token_score >= 0.4 or sequence_score >= 0.6 or (overlap >= 3 and token_score >= 0.2 and sequence_score >= 0.4)


def _title_match_score(left: Any, right: Any) -> float:
    left_key = _title_match_key(left)
    right_key = _title_match_key(right)
    if not left_key or not right_key:
        return 0.0
    left_tokens = set(left_key.split())
    right_tokens = set(right_key.split())
    token_score = len(left_tokens & right_tokens) / max(len(left_tokens | right_tokens), 1)
    sequence_score = SequenceMatcher(None, left_key, right_key).ratio()
    return max(token_score, sequence_score)


def _source_episode_alias_for_native_number(
    aliases: Mapping[str, Mapping[str, Any]],
    title: Any,
    episode_number: int,
) -> Mapping[str, Any] | None:
    if episode_number < 30:
        return None
    scored: list[tuple[float, Mapping[str, Any]]] = []
    for alias in aliases.values():
        if _int_or_none(alias.get("episode")) != episode_number:
            continue
        if not _title_loose_match(title, alias.get("title")):
            continue
        scored.append((_title_match_score(title, alias.get("title")), alias))
    if not scored:
        return None
    scored.sort(key=lambda row: row[0], reverse=True)
    if len(scored) > 1 and scored[0][0] == scored[1][0]:
        return None
    return scored[0][1]


def _anime_source_episode_absolute_match(item: Mapping[str, Any], rows: list[dict[str, Any]], e_num: int) -> int | None:
    if e_num < 30:
        return None
    native_hits = [row for row in rows if _row_anime_episode_number(row) == e_num]
    if not native_hits:
        return None
    row = max(native_hits, key=lambda candidate: _title_match_score(item.get("title"), candidate.get("title")))
    if not _title_loose_match(item.get("title"), row.get("title")):
        return None
    return e_num


def _anime_episode_rows(
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    simkl_id: str,
    cache: dict[str, list[dict[str, Any]]],
) -> list[dict[str, Any]]:
    simkl_id = str(simkl_id or "").strip()
    if not simkl_id:
        return []
    cached = cache.get(simkl_id)
    if isinstance(cached, list):
        return cached
    try:
        resp = session.get(
            f"{URL_ANIME_EPISODES}/{simkl_id}",
            headers=headers,
            params=simkl_api_params_from_headers(headers),
            timeout=timeout,
        )
    except Exception as exc:
        _dbg("anime_episode_map_failed", simkl=simkl_id, error=exc.__class__.__name__)
        return []
    if not (200 <= getattr(resp, "status_code", 0) < 300):
        _dbg("anime_episode_map_miss", simkl=simkl_id, status=getattr(resp, "status_code", None))
        return []
    try:
        payload = resp.json() if (getattr(resp, "text", "") or "").strip() else []
    except Exception as exc:
        _dbg("anime_episode_map_malformed", simkl=simkl_id, error=exc.__class__.__name__)
        return []
    if not isinstance(payload, list):
        return []
    rows: list[dict[str, Any]] = []
    for row in payload:
        if not isinstance(row, Mapping):
            continue
        episode = _int_or_none(row.get("episode"))
        if episode is None or episode <= 0:
            continue
        tvdb = row.get("tvdb") if isinstance(row.get("tvdb"), Mapping) else {}
        mapped: dict[str, Any] = {
            "episode": episode,
            "title": row.get("title"),
            "tvdb": {
                "season": _int_or_none(tvdb.get("season")) if isinstance(tvdb, Mapping) else None,
                "episode": _int_or_none(tvdb.get("episode")) if isinstance(tvdb, Mapping) else None,
            },
        }
        rows.append(mapped)
    if rows:
        cache[simkl_id] = rows
        _save_anime_episode_map_cache(cache)
    return rows


def _row_anime_episode_number(row: Mapping[str, Any]) -> int | None:
    e_num = _int_or_none(row.get("episode"))
    return e_num if e_num is not None and e_num > 0 else None


def _anime_tvdb_season_episode_for_number(
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    ids: Mapping[str, Any],
    episode_number: int,
    episode_cache: dict[str, list[dict[str, Any]]],
) -> tuple[int, int] | None:
    rows = _anime_episode_rows(session, headers, timeout, str(ids.get("simkl") or ""), episode_cache)
    if not rows:
        return None
    for row in rows:
        if _int_or_none(row.get("episode")) != int(episode_number):
            continue
        tvdb = row.get("tvdb")
        if not isinstance(tvdb, Mapping):
            return None
        season = _int_or_none(tvdb.get("season"))
        episode = _int_or_none(tvdb.get("episode"))
        if season is not None and season >= 0 and episode is not None and episode > 0:
            return season, episode
        return None
    return None


def _anime_episode_title_for_number(
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    ids: Mapping[str, Any],
    episode_number: int,
    episode_cache: dict[str, list[dict[str, Any]]],
) -> str | None:
    rows = _anime_episode_rows(session, headers, timeout, str(ids.get("simkl") or ""), episode_cache)
    for row in rows:
        if _int_or_none(row.get("episode")) != int(episode_number):
            continue
        title = row.get("title")
        return title if isinstance(title, str) and title.strip() else None
    return None


def _anime_retry_episode_number(
    item: Mapping[str, Any],
    ids: Mapping[str, str],
    *,
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    episode_cache: dict[str, list[dict[str, Any]]],
) -> int | None:
    raw_season = item.get("season") if item.get("season") is not None else item.get("season_number")
    raw_episode = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
    s_num = _int_or_none(raw_season)
    e_num = _int_or_none(raw_episode)
    if s_num is None or s_num < 0 or e_num is None or e_num <= 0:
        return None
    rows = _anime_episode_rows(session, headers, timeout, str(ids.get("simkl") or ""), episode_cache)
    if rows:
        direct = [
            row for row in rows
            if isinstance(row.get("tvdb"), Mapping)
            and _int_or_none(cast(Mapping[str, Any], row["tvdb"]).get("season")) == s_num
            and _int_or_none(cast(Mapping[str, Any], row["tvdb"]).get("episode")) == e_num
        ]
        if len(direct) == 1:
            mapped = _row_anime_episode_number(direct[0])
            if mapped:
                return mapped
        abs_num = _int_or_none(item.get("_trakt_number_abs"))
        if abs_num is not None and abs_num > 0:
            abs_hits = [row for row in rows if _row_anime_episode_number(row) == abs_num]
            if len(abs_hits) == 1:
                mapped = _row_anime_episode_number(abs_hits[0])
                if mapped:
                    return mapped
        title_key = _title_match_key(item.get("title"))
        if title_key:
            title_hits = [row for row in rows if _title_match_key(row.get("title")) == title_key]
            if len(title_hits) == 1:
                mapped = _row_anime_episode_number(title_hits[0])
                if mapped:
                    return mapped
        absolute = _anime_source_episode_absolute_match(item, rows, e_num)
        if absolute:
            return absolute
    return None


def _source_season_episode(item: Mapping[str, Any]) -> tuple[int, int] | None:
    raw_season = item.get("season") if item.get("season") is not None else item.get("season_number")
    raw_episode = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
    s_num = _int_or_none(raw_season)
    e_num = _int_or_none(raw_episode)
    if s_num is None or s_num < 0 or e_num is None or e_num <= 0:
        return None
    return s_num, e_num


def _anime_retry_episode_numbers_for_group(
    items_list: list[Mapping[str, Any]],
    ids: Mapping[str, str],
    *,
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    episode_cache: dict[str, list[dict[str, Any]]],
) -> dict[str, int]:
    mapped: dict[str, int] = {}
    anchors_by_season: dict[int, list[tuple[int, int]]] = {}
    for item in items_list:
        item_key = _thaw_key(item)
        source = _source_season_episode(item)
        if source is None:
            continue
        ep_num = _anime_retry_episode_number(
            item,
            ids,
            session=session,
            headers=headers,
            timeout=timeout,
            episode_cache=episode_cache,
        )
        if ep_num is None:
            continue
        mapped[item_key] = ep_num
        s_num, e_num = source
        anchors_by_season.setdefault(s_num, []).append((e_num, ep_num))

    rows = _anime_episode_rows(session, headers, timeout, str(ids.get("simkl") or ""), episode_cache)
    max_episode = max((_int_or_none(row.get("episode")) or 0 for row in rows), default=0)
    for item in items_list:
        item_key = _thaw_key(item)
        if item_key in mapped:
            continue
        source = _source_season_episode(item)
        if source is None:
            continue
        s_num, e_num = source
        candidates: set[int] = set()
        for anchor_source, anchor_mapped in anchors_by_season.get(s_num) or []:
            inferred = anchor_mapped + (e_num - anchor_source)
            if inferred > 0 and (not max_episode or inferred <= max_episode):
                candidates.add(inferred)
        if len(candidates) == 1:
            mapped[item_key] = next(iter(candidates))
    return mapped


def _resolved_anime_ids_for_tvdb(session: Any, headers: Mapping[str, str], timeout: float, tvdb: str, cache: dict[str, str]) -> dict[str, str]:
    tvdb = str(tvdb or "").strip()
    if not tvdb:
        return {}
    if tvdb in cache:
        return {"simkl": cache[tvdb], "tvdb": tvdb}
    try:
        resp = session.get(
            URL_REDIRECT,
            headers=headers,
            params=simkl_api_params_from_headers(headers, to="simkl", tvdb=tvdb),
            timeout=timeout,
            allow_redirects=False,
        )
    except Exception as exc:
        _dbg("anime_resolve_failed", method="redirect", tvdb=tvdb, error=exc.__class__.__name__)
        return {}
    location = ""
    resp_headers = getattr(resp, "headers", {})
    if isinstance(resp_headers, Mapping):
        location = str(resp_headers.get("Location") or resp_headers.get("location") or "")
    marker = "/anime/"
    if marker not in location:
        _dbg("anime_resolve_miss", method="redirect", tvdb=tvdb, status=getattr(resp, "status_code", None))
        return {}
    tail = location.split(marker, 1)[1]
    simkl_id = tail.split("/", 1)[0].strip()
    if not simkl_id.isdigit():
        return {}
    cache[tvdb] = simkl_id
    _save_anime_resolve_cache(cache)
    return {"simkl": simkl_id, "tvdb": tvdb}


def _native_anime_ids_for_mismatched_show(
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    item: Mapping[str, Any],
    cache: dict[str, str],
    misses: set[str] | None = None,
) -> dict[str, str]:
    show_ids = _show_ids_of_episode(item)
    tvdb = str(show_ids.get("tvdb") or "").strip()
    current_simkl = str(show_ids.get("simkl") or "").strip()
    if not tvdb:
        return {}
    if misses is not None and tvdb in misses:
        return {}
    resolved = _resolved_anime_ids_for_tvdb(session, headers, timeout, tvdb, cache)
    resolved_simkl = str(resolved.get("simkl") or "").strip()
    if resolved_simkl:
        return resolved
    if misses is not None:
        misses.add(tvdb)
    return {}


def _build_anime_retry_payload(
    items_list: Iterable[Mapping[str, Any]],
    *,
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    confirmed_keys: set[str] | None = None,
    response_ids_by_key: Mapping[str, Mapping[str, str]] | None = None,
) -> tuple[dict[str, Any], dict[tuple[str, int], Mapping[str, Any]], list[Mapping[str, Any]]]:
    groups: dict[str, dict[str, Any]] = {}
    index: dict[tuple[str, int], Mapping[str, Any]] = {}
    retry_items: list[Mapping[str, Any]] = []
    confirmed = set(confirmed_keys or set())
    response_ids = dict(response_ids_by_key or {})
    resolve_cache = _load_anime_resolve_cache()
    episode_cache = _load_anime_episode_map_cache()
    eligible_by_group: dict[str, tuple[dict[str, str], list[Mapping[str, Any]]]] = {}
    for item in items_list:
        item_key = _thaw_key(item)
        if item_key not in confirmed:
            continue
        ids = _anime_retry_show_ids(item)
        ids.update({k: str(v) for k, v in dict(response_ids.get(item_key) or {}).items() if k in {"simkl", "tvdb"} and v})
        if ids.get("tvdb") and not ids.get("simkl"):
            ids.update(_resolved_anime_ids_for_tvdb(session, headers, timeout, ids["tvdb"], resolve_cache))
        if not ids:
            continue
        group_key = json.dumps(ids, sort_keys=True)
        _ids, grouped_items = eligible_by_group.setdefault(group_key, (dict(ids), []))
        grouped_items.append(item)

    for group_key, (ids, grouped_items) in eligible_by_group.items():
        mapped_by_key = _anime_retry_episode_numbers_for_group(
            grouped_items,
            ids,
            session=session,
            headers=headers,
            timeout=timeout,
            episode_cache=episode_cache,
        )
        for item in grouped_items:
            item_key = _thaw_key(item)
            mapped_episode = mapped_by_key.get(item_key)
            watched_at = item.get("watched_at") or item.get("watchedAt")
            if mapped_episode is None or not isinstance(watched_at, str) or not watched_at:
                continue
            group = groups.setdefault(group_key, {"ids": dict(ids), "episodes": []})
            group.setdefault("episodes", []).append({"number": mapped_episode, "watched_at": watched_at})
            index[(group_key, mapped_episode)] = item
            retry_items.append(item)
    if not groups:
        return {}, {}, []
    return {"anime": list(groups.values())}, index, retry_items


def _match_show_group_key(obj: Mapping[str, Any], id_index: Mapping[tuple[str, str], str]) -> str | None:
    for field, value in (obj.get("ids") or {}).items():
        if value is None:
            continue
        matched = id_index.get((str(field), str(value)))
        if matched:
            return matched
    return None


def _match_retry_group_key(obj: Mapping[str, Any], retry_payload: Mapping[str, Any]) -> str | None:
    obj_ids = {str(k): str(v) for k, v in dict(obj.get("ids") or {}).items() if v is not None}
    if not obj_ids:
        return None
    for show in retry_payload.get("anime") or []:
        if not isinstance(show, Mapping):
            continue
        ids = {str(k): str(v) for k, v in dict(show.get("ids") or {}).items() if v is not None}
        if any(obj_ids.get(k) == v for k, v in ids.items()):
            return json.dumps(ids, sort_keys=True)
    return None


def _retry_anime_not_found(
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    retry_candidates: list[Mapping[str, Any]],
    *,
    confirmed_keys: set[str] | None = None,
    response_ids_by_key: Mapping[str, Mapping[str, str]] | None = None,
) -> tuple[set[str], set[str], set[str], list[dict[str, Any]]]:
    body, retry_index, retry_items = _build_anime_retry_payload(
        retry_candidates,
        session=session,
        headers=headers,
        timeout=timeout,
        confirmed_keys=confirmed_keys,
        response_ids_by_key=response_ids_by_key,
    )
    confirmed = set(confirmed_keys or set())
    retry_item_keys = {_thaw_key(item) for item in retry_items}
    # Native anime candidates must not fall back to the normal show payload.
    # If we're unable to map them to SIMKL episode numbers, keep them visibly unresolved.
    # Experimental logic
    unmapped_items = [
        item for item in retry_candidates
        if _thaw_key(item) in confirmed and _thaw_key(item) not in retry_item_keys
    ]
    unmapped_keys = {_thaw_key(item) for item in unmapped_items}
    unmapped_unresolved = _unresolved_for_items(unmapped_items, "simkl_anime_retry_unmapped:episodes")
    if not body:
        return set(), unmapped_keys, set(), unmapped_unresolved
    retry_keys = {_thaw_key(item) for item in retry_items}
    try:
        resp = session.post(
            URL_ADD,
            headers=headers,
            params=simkl_api_params_from_headers(headers),
            json=body,
            timeout=timeout,
        )
    except Exception as exc:
        hint = _write_failure_hint(exc=exc, reason="anime_retry_failed")
        _warn("write_failed", op="add_anime_retry", error=str(exc))
        _freeze_failed_adds(retry_items, hint)
        return set(), retry_keys, set(), _unresolved_for_items(retry_items, hint)
    if not (200 <= resp.status_code < 300):
        hint = _write_failure_hint(resp, reason="anime_retry_failed")
        _warn("write_failed", op="add_anime_retry", status=resp.status_code, body=(resp.text or "")[:200])
        _freeze_failed_adds(retry_items, hint)
        return set(), retry_keys, set(), _unresolved_for_items(retry_items, hint)

    payload, added_new, not_found, parse_error = _parse_add_payload(resp, op="add_anime_retry_response")
    if parse_error:
        _freeze_failed_adds(retry_items, parse_error)
        return set(), retry_keys, set(), _unresolved_for_items(retry_items, parse_error)

    failed: set[str] = set(unmapped_keys)
    unresolved: list[dict[str, Any]] = list(unmapped_unresolved)
    for obj in not_found["shows"][:50]:
        if not isinstance(obj, Mapping):
            continue
        group_key = _match_retry_group_key(obj, body)
        if not group_key:
            continue
        for (idx_group, _e_num), orig in retry_index.items():
            if idx_group == group_key:
                key = _thaw_key(orig)
                failed.add(key)
                unresolved.append({"item": id_minimal(orig), "hint": "simkl_not_found:anime_retry:shows", "reason": "simkl_not_found:anime_retry:shows"})
    for obj in not_found["episodes"][:50]:
        if not isinstance(obj, Mapping):
            continue
        group_key = _match_retry_group_key(obj, body)
        if not group_key:
            continue
        for episode in obj.get("episodes") or []:
            if not isinstance(episode, Mapping):
                continue
            e_num = _int_or_none(episode.get("number") if episode.get("number") is not None else episode.get("episode"))
            if e_num is None:
                continue
            orig = retry_index.get((group_key, e_num))
            if orig is not None:
                key = _thaw_key(orig)
                failed.add(key)
                unresolved.append({"item": id_minimal(orig), "hint": "simkl_not_found:anime_retry:episodes", "reason": "simkl_not_found:anime_retry:episodes"})
        for season in obj.get("seasons") or []:
            if not isinstance(season, Mapping):
                continue
            for episode in season.get("episodes") or []:
                if not isinstance(episode, Mapping):
                    continue
                e_num = _int_or_none(episode.get("number") if episode.get("number") is not None else episode.get("episode"))
                if e_num is None:
                    continue
                orig = retry_index.get((group_key, e_num))
                if orig is not None:
                    key = _thaw_key(orig)
                    failed.add(key)
                    unresolved.append({"item": id_minimal(orig), "hint": "simkl_not_found:anime_retry:episodes", "reason": "simkl_not_found:anime_retry:episodes"})

    reported_episode_adds = min(int(added_new.get("episodes") or 0), len(retry_items))
    accepted_candidates = [item for item in retry_items if _thaw_key(item) not in failed]
    accepted = {_thaw_key(item) for item in accepted_candidates[:reported_episode_adds]}
    unconfirmed = {_thaw_key(item) for item in accepted_candidates[reported_episode_adds:]}
    if unconfirmed:
        failed.update(unconfirmed)
        unresolved.extend(_unresolved_for_items([item for item in retry_items if _thaw_key(item) in unconfirmed], "simkl_write_response_ambiguous:anime_retry_count"))
    if failed:
        failed_items = [item for item in retry_candidates if _thaw_key(item) in failed]
        _freeze_failed_adds(failed_items, "simkl_not_found:anime_retry")
    skipped = retry_keys - accepted - failed
    if accepted:
        _remember_anime_episode_aliases(body, retry_index, accepted)
        _unfreeze(accepted)
    if accepted or skipped:
        log_fields: dict[str, Any] = {
            "op": "add",
            "applied": len(accepted),
            "skipped": len(skipped),
            "unresolved": len(failed),
            "anime": len((body.get("anime") or [])),
        }
        if skipped:
            log_fields["reason"] = "simkl_write_response_ambiguous:anime_retry_count"
        _info("anime_retry_done", **log_fields)
    return accepted, failed, skipped, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    setattr(adapter, "_simkl_history_add_confirmed_keys", [])
    setattr(adapter, "_simkl_history_add_skipped_keys", [])
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_scoped: dict[str, dict[str, Any]] = {}
    scoped_items: dict[str, list[Mapping[str, Any]]] = {}  # ids_key for original items (seasons)
    scoped_ep_index: dict[tuple[str, int, int], Mapping[str, Any]] = {}  # (ids_key, season, ep) for original episode item
    scoped_ep_id_index: dict[tuple[str, str], Mapping[str, Any]] = {}  # episode-level lookup ids for original episode item
    scoped_id_index: dict[tuple[str, str], str] = {}  # (field, str(value)) ids_key, for matching
    failed_thaw_keys: set[str] = set()  # thaw keys of items confirmed as not_found, excluded from cache injection
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    main_thaw_keys: list[str] = []
    main_items_list: list[Mapping[str, Any]] = []
    items_list: list[Mapping[str, Any]] = list(items or [])
    native_retry_candidates: list[Mapping[str, Any]] = []
    native_retry_confirmed_keys: set[str] = set()
    native_retry_response_ids: dict[str, dict[str, str]] = {}
    native_resolve_cache = _load_anime_resolve_cache()
    native_resolve_misses: set[str] = set()

    guid_eps = sum(
        1
        for it in items_list
        if str((it.get("ids") or {}).get("guid") or "").startswith("plex://show/")
    )
    guid_mov = sum(
        1
        for it in items_list
        if str((it.get("ids") or {}).get("guid") or "").startswith("plex://movie/")
    )
    _dbg("write_prepare", op="add", item_count=len(items_list), guid_eps=guid_eps, guid_movies=guid_mov)

    for item in items_list:
        if isinstance(item, dict):
            item["_adapter"] = adapter

    unresolved_eps_missing = 0

    for item in items_list:
        typ = str(item.get("type") or "").lower()
        bucket = str(item.get("simkl_bucket") or "").strip().lower()
        if typ == "movie" and bucket == "anime":
            entry = _show_add_entry(adapter, item)
            watched_at = str(item.get("watched_at") or "").strip()
            if entry and watched_at:
                entry["watched_at"] = watched_at
            if entry:
                shows_whole.append(entry)
                key = _thaw_key(item)
                thaw_keys.append(key)
                main_thaw_keys.append(key)
                main_items_list.append(item)
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue

        if typ == "movie":
            entry = _movie_add_entry(item)
            if entry:
                movies.append(entry)
                key = _thaw_key(item)
                thaw_keys.append(key)
                main_thaw_keys.append(key)
                main_items_list.append(item)
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue

        if typ == "season":
            unresolved.append(
                {"item": id_minimal(item), "hint": "unsupported_history_season_rollup"},
            )
            continue

        if typ == "episode":
            native_ids = _native_anime_ids_for_mismatched_show(
                session,
                headers,
                timeout,
                item,
                native_resolve_cache,
                native_resolve_misses,
            )
            if native_ids:
                key = _thaw_key(item)
                native_retry_candidates.append(item)
                native_retry_confirmed_keys.add(key)
                native_retry_response_ids[key] = native_ids
                thaw_keys.append(key)
                continue

            packed, reason = _episode_add_entry(adapter, item)
            if packed is None:
                unresolved_eps_missing += 1
                unresolved.append(
                    {"item": id_minimal(item), "hint": reason or "missing_show_ids_or_s/e_or_watched_at"},
                )
                continue

            show_entry, s_num, e_num, watched_at, episode_ids = packed
            ids_key = json.dumps(dict(show_entry.get("ids") or {}), sort_keys=True)
            group = _merge_show_group(shows_scoped, show_entry)
            season = _merge_show_season(group, s_num)
            ep_payload: dict[str, Any] = {"number": e_num, "watched_at": watched_at}
            if episode_ids:
                ep_payload["ids"] = dict(episode_ids)
            season.setdefault("episodes", []).append(ep_payload)
            scoped_items.setdefault(ids_key, []).append(item)
            scoped_ep_index[(ids_key, s_num, e_num)] = item
            for _f, _v in episode_ids.items():
                if _v is not None:
                    scoped_ep_id_index.setdefault((_f, str(_v)), item)
            for _f, _v in (show_entry.get("ids") or {}).items():
                if _v is not None:
                    scoped_id_index.setdefault((_f, str(_v)), ids_key)

            key = _thaw_key(item)
            thaw_keys.append(key)
            main_thaw_keys.append(key)
            main_items_list.append(item)
            continue

        entry = _show_add_entry(adapter, item)
        if entry:
            shows_whole.append(entry)
            key = _thaw_key(item)
            thaw_keys.append(key)
            main_thaw_keys.append(key)
            main_items_list.append(item)
        else:
            unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})

    _dbg("write_prepare", op="add", movies=len(movies), shows_whole=len(shows_whole), shows_scoped=len(shows_scoped), unresolved_eps_missing=unresolved_eps_missing)

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies

    shows_payload: list[dict[str, Any]] = []
    if shows_whole:
        shows_payload.extend(shows_whole)
    if shows_scoped:
        shows_payload.extend(list(shows_scoped.values()))
    if shows_payload:
        body["shows"] = shows_payload

    native_accepted: set[str] = set()
    native_failed: set[str] = set()
    native_skipped: set[str] = set()
    native_unresolved: list[dict[str, Any]] = []
    if native_retry_candidates:
        native_accepted, native_failed, native_skipped, native_unresolved = _retry_anime_not_found(
            session,
            headers,
            timeout,
            native_retry_candidates,
            confirmed_keys=native_retry_confirmed_keys,
            response_ids_by_key=native_retry_response_ids,
        )
        unresolved.extend(native_unresolved)
        failed_thaw_keys.update(native_failed)
        if body:
            time.sleep(1.05)

    if not body:
        confirmed_keys = list(dict.fromkeys(native_accepted))
        confirmed_key_set = set(confirmed_keys)
        failed_key_set = set(failed_thaw_keys)
        skipped_keys = [
            key for key in dict.fromkeys(thaw_keys)
            if key and key not in confirmed_key_set and key not in failed_key_set
        ]
        setattr(adapter, "_simkl_history_add_confirmed_keys", confirmed_keys)
        setattr(adapter, "_simkl_history_add_skipped_keys", skipped_keys)
        if confirmed_keys:
            _items_to_inject = [it for it in items_list if _thaw_key(it) in confirmed_key_set]
            _inject_adds_into_cache(_items_to_inject)
        _info("write_skipped", op="add", reason="empty_payload", unresolved=len(unresolved))
        return len(confirmed_keys), unresolved

    try:
        resp = session.post(
            URL_ADD,
            headers=headers,
            params=simkl_api_params_from_headers(headers),
            json=body,
            timeout=timeout,
        )
        if 200 <= resp.status_code < 300:
            _unfreeze(main_thaw_keys)
            payload: dict[str, Any] = {}
            eps_count = sum(
                len(season.get("episodes", []))
                for group in shows_scoped.values()
                for season in group.get("seasons", [])
            )
            seasons_count = sum(
                1
                for group in shows_scoped.values()
                for season in group.get("seasons", [])
                if season.get("watched_at")
            )

            payload, added_new, not_found, parse_error = _parse_add_payload(resp, op="add_response")
            if parse_error:
                _freeze_failed_adds(main_items_list, parse_error)
                unresolved.extend(_unresolved_for_items(main_items_list, parse_error))
                _info("write_done", op="add", ok=False, applied=0, unresolved=len(unresolved), reason=parse_error)
                return 0, unresolved
            if isinstance(payload, dict):
                _apply_response_classification(items_list, payload)

            reported_main_adds = _reported_add_count(
                added_new,
                movies_count=len(movies),
                whole_shows_count=len(shows_whole),
                episodes_count=eps_count,
            )
            if isinstance(payload.get("added"), Mapping) and not _has_not_found(not_found) and reported_main_adds < len(main_thaw_keys):
                hint = "simkl_write_response_ambiguous:add_count"
                main_confirmed_keys: list[str] = []
                if reported_main_adds > 0:
                    for item in main_items_list:
                        if len(main_confirmed_keys) >= reported_main_adds:
                            break
                        key = _thaw_key(item)
                        if key:
                            main_confirmed_keys.append(key)
                ambiguous_failed = [item for item in main_items_list if _thaw_key(item) not in set(main_confirmed_keys)]
                if ambiguous_failed:
                    _freeze_failed_adds(ambiguous_failed, hint)
                    unresolved.extend(_unresolved_for_items(ambiguous_failed, hint))
                confirmed_keys = list(dict.fromkeys(native_accepted))
                confirmed_keys = list(dict.fromkeys(main_confirmed_keys + confirmed_keys))
                confirmed_key_set = set(confirmed_keys)
                failed_thaw_keys.update(_thaw_key(item) for item in ambiguous_failed)
                skipped_keys: list[str] = []
                setattr(adapter, "_simkl_history_add_confirmed_keys", confirmed_keys)
                setattr(adapter, "_simkl_history_add_skipped_keys", skipped_keys)
                if confirmed_keys:
                    _items_to_inject = [it for it in items_list if _thaw_key(it) in confirmed_key_set]
                    _inject_adds_into_cache(_items_to_inject)
                _info(
                    "write_done",
                    op="add",
                    ok=len(unresolved) == 0,
                    applied=len(confirmed_keys),
                    skipped=len(skipped_keys),
                    unresolved=len(unresolved),
                    reason=hint,
                    movies=len(movies),
                    shows_payload=len(shows_payload),
                    seasons=seasons_count,
                    episodes=eps_count,
                    reported_movies=int(added_new.get("movies") or 0),
                    reported_shows=int(added_new.get("shows") or 0),
                    reported_episodes=int(added_new.get("episodes") or 0),
                )
                return len(confirmed_keys), unresolved

            unknown_failed = len(not_found["movies"])
            if not_found["shows"] or not_found["movies"] or not_found["episodes"]:
                _dbg("resolve_miss", op="add", movies=len(not_found["movies"]), shows=len(not_found["shows"]), episodes=len(not_found["episodes"]))

            for obj in not_found["movies"][:50]:
                if isinstance(obj, dict):
                    unresolved.append({"item": obj, "hint": "simkl_not_found:movies", "reason": "simkl_not_found:movies"})
                else:
                    unresolved.append({"item": {"raw": obj}, "hint": "simkl_not_found:movies", "reason": "simkl_not_found:movies"})

            for obj in not_found["shows"][:50]:
                originals = None
                if isinstance(obj, dict):
                    for _f, _v in (obj.get("ids") or {}).items():
                        if _v is not None:
                            _ikey = scoped_id_index.get((_f, str(_v)))
                            if _ikey:
                                originals = scoped_items.get(_ikey)
                                break
                if originals:
                    for orig in originals:
                        failed_thaw_keys.add(_thaw_key(orig))
                        unresolved.append({"item": id_minimal(orig), "hint": "simkl_not_found:shows", "reason": "simkl_not_found:shows"})
                else:
                    unknown_failed += 1
                    if isinstance(obj, dict):
                        unresolved.append({"item": obj, "hint": "simkl_not_found:shows", "reason": "simkl_not_found:shows"})
                    else:
                        unresolved.append({"item": {"raw": obj}, "hint": "simkl_not_found:shows", "reason": "simkl_not_found:shows"})

            retry_candidates: list[Mapping[str, Any]] = []
            retry_confirmed_keys: set[str] = set()
            retry_response_ids: dict[str, dict[str, str]] = {}
            for obj in not_found["episodes"][:50]:
                if not isinstance(obj, dict):
                    unresolved.append({"item": {"raw": obj}, "hint": "simkl_not_found:episodes", "reason": "simkl_not_found:episodes"})
                    unknown_failed += 1
                    continue
                _matched_ids_key = _match_show_group_key(obj, scoped_id_index)
                obj_confirms_anime = _not_found_confirms_anime(obj)
                obj_retry_ids = _response_show_retry_ids(obj)
                if _matched_ids_key:
                    for _s in (obj.get("seasons") or []):
                        if not isinstance(_s, Mapping):
                            continue
                        _snum = _int_or_none(_s.get("number") if _s.get("number") is not None else _s.get("season"))
                        for _e in (_s.get("episodes") or []):
                            if not isinstance(_e, Mapping):
                                continue
                            _enum = _int_or_none(_e.get("number") if _e.get("number") is not None else _e.get("episode"))
                            if _snum is None or _enum is None:
                                unknown_failed += 1
                                unresolved.append({"item": obj, "hint": "simkl_not_found:episodes", "reason": "simkl_not_found:episodes"})
                                continue
                            _orig = scoped_ep_index.get((_matched_ids_key, _snum, _enum))
                            if _orig is None:
                                for _f, _v in (_e.get("ids") or {}).items():
                                    if _v is None:
                                        continue
                                    _orig = scoped_ep_id_index.get((_f, str(_v)))
                                    if _orig is not None:
                                        break
                            if _orig is not None:
                                retry_candidates.append(_orig)
                                retry_key = _thaw_key(_orig)
                                retry_ids = dict(obj_retry_ids)
                                if not retry_ids:
                                    retry_ids = _anime_retry_show_ids(_orig)
                                if obj_confirms_anime or retry_ids.get("tvdb"):
                                    retry_confirmed_keys.add(retry_key)
                                    if retry_ids:
                                        retry_response_ids[retry_key] = retry_ids
                            else:
                                unknown_failed += 1
                                unresolved.append({"item": obj, "hint": "simkl_not_found:episodes", "reason": "simkl_not_found:episodes"})
                else:
                    _ep_count = sum(len(_s.get("episodes") or []) for _s in (obj.get("seasons") or []) if isinstance(_s, Mapping))
                    unknown_failed += _ep_count if _ep_count > 0 else 1
                    unresolved.append({"item": obj, "hint": "simkl_not_found:episodes", "reason": "simkl_not_found:episodes"})

            retry_accepted, retry_failed, retry_skipped, retry_unresolved = _retry_anime_not_found(
                session,
                headers,
                timeout,
                retry_candidates,
                confirmed_keys=retry_confirmed_keys,
                response_ids_by_key=retry_response_ids,
            )
            if retry_candidates:
                _dbg(
                    "anime_retry",
                    op="add",
                    candidates=len(retry_candidates),
                    accepted=len(retry_accepted),
                    failed=len(retry_failed),
                    skipped=len(retry_skipped),
                    ineligible=len(retry_candidates) - len(retry_accepted) - len(retry_failed) - len(retry_skipped),
                )
            retry_attempted_keys = retry_accepted | retry_failed | retry_skipped | native_accepted | native_failed | native_skipped
            unresolved.extend(retry_unresolved)
            for orig in retry_candidates:
                key = _thaw_key(orig)
                if key in retry_accepted:
                    continue
                if key in retry_failed:
                    failed_thaw_keys.add(key)
                    continue
                if key in retry_skipped:
                    continue
                failed_thaw_keys.add(key)
                unresolved.append({"item": id_minimal(orig), "hint": "simkl_not_found:episodes", "reason": "simkl_not_found:episodes"})

            retry_candidate_keys = {_thaw_key(item) for item in retry_candidates} | {_thaw_key(item) for item in native_retry_candidates}
            main_confirmed_keys: list[str] = []
            if reported_main_adds > 0:
                for item in items_list:
                    if len(main_confirmed_keys) >= reported_main_adds:
                        break
                    key = _thaw_key(item)
                    if not key or key in failed_thaw_keys or key in retry_candidate_keys:
                        continue
                    if key not in thaw_keys:
                        continue
                    main_confirmed_keys.append(key)
            confirmed_keys = list(dict.fromkeys(main_confirmed_keys + list(retry_accepted) + list(native_accepted)))
            confirmed_key_set = set(confirmed_keys)
            failed_key_set = set(failed_thaw_keys)
            skipped_keys = [
                key for key in dict.fromkeys(thaw_keys)
                if key and key not in confirmed_key_set and key not in failed_key_set
            ]
            ok = len(confirmed_keys)
            setattr(adapter, "_simkl_history_add_confirmed_keys", confirmed_keys)
            setattr(adapter, "_simkl_history_add_skipped_keys", skipped_keys)
            if ok > 0:
                _items_to_inject = [it for it in items_list if _thaw_key(it) in confirmed_key_set]
                _inject_adds_into_cache(_items_to_inject)
            _info(
                "write_done",
                op="add",
                ok=len(unresolved) == 0 and ok == len(thaw_keys),
                applied=ok,
                unresolved=len(unresolved),
                movies=len(movies),
                shows_payload=len(shows_payload),
                seasons=seasons_count,
                episodes=eps_count,
                not_found=len(failed_thaw_keys) + unknown_failed,
                anime_retry=len(retry_attempted_keys),
                reported_movies=int(added_new.get("movies") or 0),
                reported_shows=int(added_new.get("shows") or 0),
                reported_episodes=int(added_new.get("episodes") or 0),
            )
            return ok, unresolved

        failure_hint = _write_failure_hint(resp, reason="write_failed")
        _warn("write_failed", op="add", status=resp.status_code, body=(resp.text or '')[:200])
    except Exception as exc:
        failure_hint = _write_failure_hint(exc=exc, reason="write_failed")
        _warn("write_failed", op="add", error=str(exc))

    _freeze_failed_adds(main_items_list, failure_hint)
    unresolved.extend(_unresolved_for_items(main_items_list, failure_hint))
    _info("write_done", op="add", ok=False, applied=0, unresolved=len(unresolved))
    return 0, unresolved

def _native_anime_remove_body(
    items_list: list[Mapping[str, Any]],
    *,
    session: Any,
    headers: Mapping[str, str],
    timeout: float,
    resolve_cache: dict[str, str],
    misses: set[str],
) -> tuple[dict[str, Any], list[str], set[int], list[Mapping[str, Any]]]:
    episode_cache = _load_anime_episode_map_cache()
    groups: dict[str, tuple[dict[str, str], list[Mapping[str, Any]]]] = {}
    detected: set[int] = set()
    for item in items_list:
        if str(item.get("type") or "").lower() != "episode":
            continue
        native_ids = _native_anime_ids_for_mismatched_show(session, headers, timeout, item, resolve_cache, misses)
        ids = {k: str(v) for k, v in native_ids.items() if k in {"simkl", "tvdb"} and v}
        if not ids.get("simkl"):
            continue
        detected.add(id(item))
        gkey = json.dumps(ids, sort_keys=True)
        _ids, grouped = groups.setdefault(gkey, (dict(ids), []))
        grouped.append(item)

    anime_out: list[dict[str, Any]] = []
    thaw: list[str] = []
    mapped_ids: set[int] = set()
    for _gkey, (ids, grouped) in groups.items():
        mapped = _anime_retry_episode_numbers_for_group(
            grouped, ids, session=session, headers=headers, timeout=timeout, episode_cache=episode_cache
        )
        episodes: list[dict[str, Any]] = []
        for item in grouped:
            number = mapped.get(_thaw_key(item))
            if number is None:
                continue
            episodes.append({"number": number})
            thaw.append(_thaw_key(item))
            mapped_ids.add(id(item))
        if episodes:
            anime_out.append({"ids": dict(ids), "episodes": episodes})

    unmapped = [item for item in items_list if id(item) in detected and id(item) not in mapped_ids]
    body = {"anime": anime_out} if anime_out else {}
    return body, thaw, detected, unmapped


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    unresolved: list[dict[str, Any]] = []
    items_list: list[Mapping[str, Any]] = list(items or [])
    if not items_list:
        _info("write_skipped", op="remove", reason="empty_payload", unresolved=len(unresolved))
        return 0, unresolved
    chunk_size = max(1, int(getattr(adapter.cfg, "history_chunk_size", 100) or 100))
    native_resolve_cache = _load_anime_resolve_cache()
    native_resolve_misses: set[str] = set()
    ok = 0
    for part in _chunk_items(items_list, chunk_size):
        native_body, native_thaw, native_detected, native_unmapped = _native_anime_remove_body(
            list(part),
            session=session,
            headers=headers,
            timeout=timeout,
            resolve_cache=native_resolve_cache,
            misses=native_resolve_misses,
        )
        for item in native_unmapped:
            unresolved.append({"item": id_minimal(item), "hint": "simkl_anime_remove_unmapped:episodes", "reason": "simkl_anime_remove_unmapped:episodes"})
        if native_body:
            try:
                resp = session.post(
                    URL_REMOVE,
                    headers=headers,
                    params=simkl_api_params_from_headers(headers),
                    json=native_body,
                    timeout=timeout,
                )
                if 200 <= resp.status_code < 300:
                    _unfreeze(native_thaw)
                    ok += len(native_thaw)
                    _evict_removes_from_cache([it for it in part if id(it) in native_detected and id(it) not in {id(u) for u in native_unmapped}])
                else:
                    _warn("write_failed", op="remove", target="anime", status=resp.status_code, body=(resp.text or '')[:200])
                    for item in part:
                        if id(item) in native_detected and id(item) not in {id(u) for u in native_unmapped}:
                            unresolved.append({"item": id_minimal(item), "hint": _write_failure_hint(resp, reason="anime_remove_failed")})
            except Exception as exc:
                _warn("write_failed", op="remove", target="anime", error=str(exc))
                for item in part:
                    if id(item) in native_detected and id(item) not in {id(u) for u in native_unmapped}:
                        unresolved.append({"item": id_minimal(item), "hint": _write_failure_hint(exc=exc, reason="anime_remove_failed")})
        movies: list[dict[str, Any]] = []
        shows_whole: list[dict[str, Any]] = []
        shows_scoped: dict[str, dict[str, Any]] = {}
        thaw_keys: list[str] = []
        for item in part:
            if id(item) in native_detected:
                continue
            typ = str(item.get("type") or "").lower()
            bucket = str(item.get("simkl_bucket") or "").strip().lower()
            if typ == "movie" and bucket == "anime":
                ids = _ids_of(item)
                if not ids:
                    unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
                    continue
                shows_whole.append({"ids": ids})
                thaw_keys.append(_thaw_key(item))
                continue
            if typ == "movie":
                ids = _ids_of(item)
                if not ids:
                    unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
                    continue
                movies.append({"ids": ids})
                thaw_keys.append(_thaw_key(item))
                continue
            if typ == "season":
                show_ids = _raw_show_ids(item)
                show_entry = _show_scope_entry(adapter, item, show_ids) if show_ids else None
                s_num = int(item.get("season") or item.get("season_number") or 0)
                if not show_entry or not s_num:
                    unresolved.append({"item": id_minimal(item), "hint": "missing_show_ids_or_season"})
                    continue
                group = _merge_show_group(shows_scoped, show_entry)
                _merge_show_season(group, s_num)
                thaw_keys.append(_thaw_key(item))
                continue
            if typ == "episode":
                show_ids = _show_ids_of_episode(item)
                raw_season = item.get("season") if item.get("season") is not None else item.get("season_number")
                raw_episode = item.get("episode") if item.get("episode") is not None else item.get("episode_number")
                s_num = _safe_int(raw_season)
                e_num = _safe_int(raw_episode)
                episode_ids = _episode_lookup_ids(item)
                if not s_num:
                    if _int_or_none(raw_season) == 0 and episode_ids:
                        s_num = 0
                    else:
                        unresolved.append({"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"})
                        continue
                if not show_ids or e_num <= 0:
                    unresolved.append({"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"})
                    continue
                show_entry = _show_scope_entry(adapter, item, show_ids)
                if not show_entry:
                    unresolved.append({"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"})
                    continue
                group = _merge_show_group(shows_scoped, show_entry)
                season = _merge_show_season(group, s_num)
                episode_payload: dict[str, Any] = {"number": e_num}
                if episode_ids:
                    episode_payload["ids"] = dict(episode_ids)
                season.setdefault("episodes", []).append(episode_payload)
                thaw_keys.append(_thaw_key(item))
                continue
            ids = _ids_of(item)
            if ids:
                shows_whole.append({"ids": ids})
                thaw_keys.append(_thaw_key(item))
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})

        body: dict[str, Any] = {}
        if movies:
            body["movies"] = movies
        shows_payload: list[dict[str, Any]] = []
        if shows_whole:
            shows_payload.extend(shows_whole)
        if shows_scoped:
            shows_payload.extend(list(shows_scoped.values()))
        if shows_payload:
            body["shows"] = shows_payload
        if not body:
            continue
        try:
            resp = session.post(
                URL_REMOVE,
                headers=headers,
                params=simkl_api_params_from_headers(headers),
                json=body,
                timeout=timeout,
            )
            if 200 <= resp.status_code < 300:
                _unfreeze(thaw_keys)
                ok += len(thaw_keys)
                _evict_removes_from_cache(list(part))
                continue
            _warn("write_failed", op="remove", status=resp.status_code, body=(resp.text or '')[:200])
        except Exception as exc:
            _warn("write_failed", op="remove", error=str(exc))
        for item in part:
            ids = _scope_ids_for_freeze(item)
            if ids:
                _freeze(
                    item,
                    action="remove",
                    reasons=["write_failed"],
                    ids_sent=ids,
                    watched_at=None,
                )
    _info("write_done", op="remove", ok=len(unresolved) == 0 and ok > 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved

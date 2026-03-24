# SIMKL Module for history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from itertools import chain
from typing import Any, Iterable, Mapping, cast

from cw_platform.id_map import canonical_key as _canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import (
    adapter_headers,
    fetch_activities,
    extract_latest_ts,
    get_watermark,
    load_json_state,
    maybe_map_tvdb_ids,
    normalize_flat_watermarks,
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
URL_TV_EPISODES = f"{BASE}/tv/episodes"
URL_ANIME_EPISODES = f"{BASE}/anime/episodes"


def _unresolved_path() -> str:
    return str(state_file("simkl_history.unresolved.json"))


ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")
_MOVIE_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl")  # anime IDs excluded to prevent SIMKL misrouting to anime bucket

_EP_LOOKUP_MEMO: dict[str, dict[tuple[int, int], dict[str, Any]]] = {}
def _maybe_map_tvdb(adapter: Any, ids: Mapping[str, Any]) -> dict[str, str]:
    def _fetch_rows() -> Iterable[Mapping[str, Any]]:
        try:
            resp = adapter.client.session.get(
                f"{BASE}/sync/all-items/anime",
                headers=_headers(adapter, force_refresh=True),
                params={"extended": "full_anime_seasons"},
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


def _raw_show_ids(item: Mapping[str, Any]) -> dict[str, Any]:
    return dict(item.get("show_ids") or {})


def _thaw_key(item: Mapping[str, Any]) -> str:
    typ = str(item.get("type") or "").lower()
    return simkl_key_of(item) if typ == "episode" else simkl_key_of(id_minimal(item))


def _episode_lookup(
    session: Any,
    headers: Mapping[str, str],
    *,
    timeout: float,
    show_ids: Mapping[str, Any],
    kind: str,
) -> dict[tuple[int, int], dict[str, Any]]:
    ids = dict(show_ids or {})
    candidates = [
        str(ids.get("simkl") or "").strip(),
        str(ids.get("tvdb") or "").strip(),
        str(ids.get("tmdb") or "").strip(),
        str(ids.get("imdb") or "").strip(),
    ]
    candidates = [c for c in candidates if c]
    if not candidates:
        return {}

    base_url = URL_ANIME_EPISODES if str(kind).lower() == "anime" else URL_TV_EPISODES

    for cand in candidates:
        memo_key = f"{str(kind).lower()}:{cand}"
        if memo_key in _EP_LOOKUP_MEMO:
            return _EP_LOOKUP_MEMO[memo_key]

    def _as_title(v: Any) -> str | None:
        if isinstance(v, str):
            t = v.strip()
            return t or None
        if isinstance(v, Mapping):
            for k in ("en", "title", "name", "original", "en_title"):
                t = _as_title(v.get(k))
                if t:
                    return t
            for vv in v.values():
                t = _as_title(vv)
                if t:
                    return t
        if isinstance(v, list):
            for it in v:
                t = _as_title(it)
                if t:
                    return t
        return None

    def _pick_title(row: Mapping[str, Any]) -> str | None:
        for key in ("title", "name", "en_title", "episode_title", "episodeName", "episodeTitle", "episode_name"):
            t = _as_title(row.get(key))
            if t:
                return t
        ep = row.get("episode")
        if isinstance(ep, Mapping):
            for key in ("title", "name", "en_title", "episode_title"):
                t = _as_title(ep.get(key))
                if t:
                    return t
        for key in ("titles", "names"):
            t = _as_title(row.get(key))
            if t:
                return t
        return None

    def _extract_ids(row: Mapping[str, Any]) -> dict[str, str]:
        raw = row.get("ids")
        d = dict(raw) if isinstance(raw, Mapping) else {}
        if d.get("simkl_id") and not d.get("simkl"):
            d["simkl"] = d["simkl_id"]
        return {k: str(d[k]) for k in ID_KEYS if d.get(k)}

    client_id = str(headers.get("simkl-api-key") or "").strip()

    for cand in candidates:
        out: dict[tuple[int, int], dict[str, Any]] = {}
        url = f"{base_url}/{cand}"
        params: dict[str, str] = {"extended": "full"}
        if client_id:
            params["client_id"] = client_id
        try:
            resp = session.get(url, headers=dict(headers), params=params, timeout=timeout)
            if not resp.ok:
                _warn("http_failed", op="episode_lookup", kind=str(kind).lower(), candidate=cand, status=resp.status_code)
                _EP_LOOKUP_MEMO[memo_key] = {}
                continue
            body = resp.json() if (resp.text or "").strip() else []
        except Exception as exc:
            _warn("http_failed", op="episode_lookup", kind=str(kind).lower(), candidate=cand, error=str(exc))
            _EP_LOOKUP_MEMO[memo_key] = {}
            continue

        if not isinstance(body, list):
            _dbg("parse_failed", op="episode_lookup", kind=str(kind).lower(), candidate=cand, reason="non_list_body")
            _EP_LOOKUP_MEMO[memo_key] = {}
            continue
        if not body:
            _dbg("resolve_miss", op="episode_lookup", kind=str(kind).lower(), candidate=cand, reason="empty_response")
            _EP_LOOKUP_MEMO[memo_key] = {}
            continue

        for row in body:
            if not isinstance(row, Mapping):
                continue
            s_num = _safe_int(row.get("season") or row.get("season_number"))
            e_num = _safe_int(row.get("episode") or row.get("episode_number") or row.get("number"))
            if not s_num or not e_num:
                continue
            out[(s_num, e_num)] = {
                "title": _pick_title(row),
                "ids": _extract_ids(row),
            }

        if out:
            for k in candidates:
                _EP_LOOKUP_MEMO[f"{str(kind).lower()}:{k}"] = out
            _dbg("resolve_hit", op="episode_lookup", kind=str(kind).lower(), candidate=cand, episodes=len(out))
            return out

        _EP_LOOKUP_MEMO[memo_key] = {}

    return {}

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
    items = data.get("items")
    if not isinstance(items, dict):
        return {}
    return {str(k): dict(v) for k, v in items.items() if isinstance(v, Mapping)}


def _cache_save(items: Mapping[str, Any]) -> None:
    _save_json(_cache_path(), {"generated_at": _as_iso(_now_epoch()), "items": dict(items)})


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
        if item_type == "episode":
            entry["season"] = item.get("season")
            entry["episode"] = item.get("episode")
            entry["series_title"] = item.get("series_title")
            entry["simkl_bucket"] = "shows"
        else:
            entry["title"] = item.get("title")
            entry["year"] = item.get("year")
            entry["simkl_bucket"] = "movies"
        to_inject[event_key] = entry

    if not to_inject:
        return
    cached = _cache_load()
    cached.update(to_inject)
    _cache_save(cached)
    _dbg("cache_injected", count=len(to_inject))


def _fetch_all_items(
    session: Any,
    headers: Mapping[str, str],
    *,
    since_iso: str | None,
    timeout: float,
) -> dict[str, list[dict[str, Any]]]:
    params: dict[str, str] = {
        "extended": "full_anime_seasons",
        "episode_watched_at": "yes",
    }
    if since_iso:
        params["date_from"] = since_iso
    resp = session.get(URL_ALL_ITEMS, headers=headers, params=params, timeout=timeout)
    if not resp.ok:
        _warn("http_failed", op="index", method="GET", url=URL_ALL_ITEMS, status=resp.status_code)
        return {"movies": [], "shows": [], "anime": []}
    try:
        body = resp.json() or {}
    except Exception:
        body = {}
    out: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": [], "anime": []}
    if not isinstance(body, Mapping):
        return out
    for kind in ("movies", "shows", "anime"):
        rows = body.get(kind)
        if isinstance(rows, list):
            out[kind] = [x for x in rows if isinstance(x, dict) and not _is_null_env(x)]
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
            at = (show.get("anime_type") or show.get("animeType")) if isinstance(show, Mapping) else None
            anime_type = at.strip().lower() if isinstance(at, str) and at.strip() else None
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
            s_num_internal = int((season.get("number") or season.get("season") or 0))
            for episode in (season.get("episodes") or []):
                episode = episode if isinstance(episode, Mapping) else {}
                e_num_internal = int((episode.get("number") or episode.get("episode") or 0))
                s_num = s_num_internal
                e_num = e_num_internal
                if row_kind == "anime":
                    tvdb_map = episode.get("tvdb")
                    if isinstance(tvdb_map, Mapping):
                        s_m = int(tvdb_map.get("season") or 0)
                        e_m = int(tvdb_map.get("episode") or 0)
                        if s_m >= 1 and e_m >= 1:
                            s_num = s_m
                            e_num = e_m
                watched_at = (episode.get("watched_at") or episode.get("last_watched_at") or "").strip()
                ts = _as_epoch(watched_at)
                if not ts and row_kind == "shows":
                    watched_at = (row.get("last_watched_at") or "").strip()
                    ts = _as_epoch(watched_at)
                if not ts or not s_num or not e_num:
                    continue
                ep = {
                    "type": "episode",
                    "season": s_num,
                    "episode": e_num,
                    "ids": dict(show_ids),
                    "title": f"S{s_num:02d}E{e_num:02d}",
                    "year": None,
                    "series_title": series_name,
                    "series_year": show_year,
                    "show_ids": dict(show_ids),
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


def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    session = adapter.client.session
    timeout = adapter.cfg.timeout
    normalize_flat_watermarks()

    cached = _cache_load()
    wm = get_watermark("history") or ""
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
    rows_by_kind = _fetch_all_items(session, headers, since_iso=date_from, timeout=timeout)
    movie_rows = list(rows_by_kind.get("movies") or [])
    show_rows = list(rows_by_kind.get("shows") or [])
    anime_rows = list(rows_by_kind.get("anime") or [])

    fetched, thaw, latest_ts_movies, latest_ts_shows, latest_ts_anime, movies_cnt, eps_cnt = _parse_rows(
        movie_rows, show_rows, anime_rows, limit=None  # apply limit to final result only
    )
    _dedupe_history_movies(fetched)
    _dbg("index_fetch_counts", movies=movies_cnt, episodes=eps_cnt, from_date=date_from or "")

    # Build final index
    if strategy == "delta":
        final: dict[str, dict[str, Any]] = dict(cached)
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
    
    _season_synt: dict[str, dict[str, Any]] = {}
    for _e in result.values():
        if str(_e.get("type") or "").lower() != "episode":
            continue
        _s_num = _e.get("season")
        if _s_num is None:
            continue
        _sids = _e.get("show_ids") or {}
        if not _sids:
            continue
        _show_key = _canonical_key({"type": "show", "ids": _sids})
        if not _show_key:
            continue
        _sk = f"{_show_key}#season:{_s_num}"
        if _sk in result:
            continue
        _wat = _e.get("watched_at") or ""
        if _sk not in _season_synt or _wat > (_season_synt[_sk].get("watched_at") or ""):
            _season_synt[_sk] = {"type": "season", "season": _s_num, "show_ids": dict(_sids), "watched": True}
    result.update(_season_synt)

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
    ids = _maybe_map_tvdb(adapter, ids)
    entry: dict[str, Any] = {"ids": ids}
    if _is_anime_like(item, ids):
        entry["use_tvdb_anime_seasons"] = True
    return entry


def _show_scope_entry(adapter: Any, item: Mapping[str, Any], raw_show_ids: Mapping[str, Any]) -> dict[str, Any] | None:
    show_ids = {k: str(raw_show_ids[k]) for k in ID_KEYS if raw_show_ids.get(k)}
    show_ids = _maybe_map_tvdb(adapter, show_ids)
    if not show_ids:
        return None

    show: dict[str, Any] = {"ids": show_ids}
    if _is_anime_like(item, show_ids):
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


def _season_add_entry(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any], int, str] | None:
    show_ids_raw = _raw_show_ids(item)
    if not show_ids_raw:
        return None
    show = _show_scope_entry(adapter, item, show_ids_raw)
    if not show:
        return None

    s_num = _safe_int(item.get("season") or item.get("season_number"))
    watched_at = item.get("watched_at") or item.get("watchedAt")
    if not s_num or not isinstance(watched_at, str) or not watched_at:
        return None
    return show, s_num, watched_at


def _episode_add_entry(adapter: Any, item: Mapping[str, Any]) -> tuple[dict[str, Any], int, int, str] | None:
    show_ids_raw = _show_ids_of_episode(item)
    if not show_ids_raw:
        return None
    show = _show_scope_entry(adapter, item, show_ids_raw)
    if not show:
        return None

    s_num = _safe_int(item.get("season") or item.get("season_number"))
    e_num = _safe_int(item.get("episode") or item.get("episode_number"))
    watched_at = item.get("watched_at") or item.get("watchedAt")
    if not s_num or not e_num or not isinstance(watched_at, str) or not watched_at:
        return None

    return show, s_num, e_num, watched_at


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

def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_scoped: dict[str, dict[str, Any]] = {}
    scoped_items: dict[str, list[Mapping[str, Any]]] = {}  # ids_key for original items (seasons)
    scoped_ep_index: dict[tuple[str, int, int], Mapping[str, Any]] = {}  # (ids_key, season, ep) for original episode item
    scoped_id_index: dict[tuple[str, str], str] = {}  # (field, str(value)) ids_key, for matching
    failed_thaw_keys: set[str] = set()  # thaw keys of items confirmed as not_found, excluded from cache injection
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    items_list: list[Mapping[str, Any]] = list(items or [])

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
                thaw_keys.append(_thaw_key(item))
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue

        if typ == "movie":
            entry = _movie_add_entry(item)
            if entry:
                movies.append(entry)
                thaw_keys.append(_thaw_key(item))
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue

        if typ == "season":
            packed = _season_add_entry(adapter, item)
            if not packed:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_season_or_watched_at"},
                )
                continue

            show_entry, s_num, watched_at = packed
            ids_key = json.dumps(dict(show_entry.get("ids") or {}), sort_keys=True)
            group = _merge_show_group(shows_scoped, show_entry)
            _merge_show_season(group, s_num, watched_at=watched_at)
            scoped_items.setdefault(ids_key, []).append(item)
            for _f, _v in (show_entry.get("ids") or {}).items():
                if _v is not None:
                    scoped_id_index.setdefault((_f, str(_v)), ids_key)

            thaw_keys.append(_thaw_key(item))
            continue

        if typ == "episode":
            packed = _episode_add_entry(adapter, item)
            if not packed:
                unresolved_eps_missing += 1
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e_or_watched_at"},
                )
                continue

            show_entry, s_num, e_num, watched_at = packed
            ids_key = json.dumps(dict(show_entry.get("ids") or {}), sort_keys=True)
            group = _merge_show_group(shows_scoped, show_entry)
            season = _merge_show_season(group, s_num)
            season.setdefault("episodes", []).append({"number": e_num, "watched_at": watched_at})
            scoped_items.setdefault(ids_key, []).append(item)
            scoped_ep_index[(ids_key, s_num, e_num)] = item
            for _f, _v in (show_entry.get("ids") or {}).items():
                if _v is not None:
                    scoped_id_index.setdefault((_f, str(_v)), ids_key)

            thaw_keys.append(_thaw_key(item))
            continue

        entry = _show_add_entry(adapter, item)
        if entry:
            shows_whole.append(entry)
            thaw_keys.append(_thaw_key(item))
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

    if not body:
        _info("write_skipped", op="add", reason="empty_payload", unresolved=len(unresolved))
        return 0, unresolved

    try:
        resp = session.post(URL_ADD, headers=headers, json=body, timeout=timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze(thaw_keys)
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

            added_new = {"movies": 0, "shows": 0, "episodes": 0}
            not_found = {"movies": [], "shows": [], "episodes": []}
            try:
                payload = resp.json() if (resp.text or '').strip() else {}
                if isinstance(payload, dict):
                    a = payload.get("added")
                    if isinstance(a, dict):
                        added_new["movies"] = int(a.get("movies") or 0)
                        added_new["shows"] = int(a.get("shows") or 0)
                        added_new["episodes"] = int(a.get("episodes") or 0)
                    nf = payload.get("not_found")
                    if isinstance(nf, dict):
                        not_found["movies"] = list(nf.get("movies") or [])
                        not_found["shows"] = list(nf.get("shows") or [])
                        not_found["episodes"] = list(nf.get("episodes") or [])
            except Exception as exc:
                _dbg("parse_failed", op="add_response", error=str(exc))

            nf_total = len(not_found["movies"])  # episodes counted per-item in loop below
            if not_found["shows"] or not_found["movies"] or not_found["episodes"]:
                _dbg("resolve_miss", op="add", movies=len(not_found["movies"]), shows=len(not_found["shows"]), episodes=len(not_found["episodes"]))

            for obj in not_found["movies"][:50]:
                if isinstance(obj, dict):
                    unresolved.append({"item": obj, "hint": "simkl_not_found:movies"})
                else:
                    unresolved.append({"item": {"raw": obj}, "hint": "simkl_not_found:movies"})

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
                    nf_total += len(originals)
                    for orig in originals:
                        failed_thaw_keys.add(_thaw_key(orig))
                        unresolved.append({"item": id_minimal(orig), "hint": "simkl_not_found:shows"})
                else:
                    # Whole-show entry (not scoped) counts as 1
                    nf_total += 1
                    if isinstance(obj, dict):
                        unresolved.append({"item": obj, "hint": "simkl_not_found:shows"})
                    else:
                        unresolved.append({"item": {"raw": obj}, "hint": "simkl_not_found:shows"})

            for obj in not_found["episodes"][:50]:
                # not_found.episodes is a show-level container with nested seasons/episodes
                if not isinstance(obj, dict):
                    unresolved.append({"item": {"raw": obj}, "hint": "simkl_not_found:episodes"})
                    nf_total += 1
                    continue
                _matched_ids_key = None
                for _f, _v in (obj.get("ids") or {}).items():
                    if _v is not None:
                        _matched_ids_key = scoped_id_index.get((_f, str(_v)))
                        if _matched_ids_key:
                            break
                if _matched_ids_key:
                    for _s in (obj.get("seasons") or []):
                        _snum = int(_s.get("number") or 0)
                        for _e in (_s.get("episodes") or []):
                            _enum = int(_e.get("number") or 0)
                            _orig = scoped_ep_index.get((_matched_ids_key, _snum, _enum))
                            nf_total += 1
                            if _orig is not None:
                                failed_thaw_keys.add(_thaw_key(_orig))
                                unresolved.append({"item": id_minimal(_orig), "hint": "simkl_not_found:episodes"})
                            else:
                                unresolved.append({"item": obj, "hint": "simkl_not_found:episodes"})
                else:
                    # Can't match show; count episodes from nested structure
                    _ep_count = sum(len(_s.get("episodes") or []) for _s in (obj.get("seasons") or []))
                    nf_total += _ep_count if _ep_count > 0 else 1
                    unresolved.append({"item": obj, "hint": "simkl_not_found:episodes"})

            ok = max(0, len(thaw_keys) - nf_total)
            if ok > 0:
                _items_to_inject = [it for it in items_list if _thaw_key(it) not in failed_thaw_keys]
                _inject_adds_into_cache(_items_to_inject)
            _info("write_done", op="add", ok=len(unresolved) == 0 and ok == len(thaw_keys), applied=ok, unresolved=len(unresolved), movies=len(movies), shows_payload=len(shows_payload), seasons=seasons_count, episodes=eps_count, not_found=nf_total)
            return ok, unresolved

        _warn("write_failed", op="add", status=resp.status_code, body=(resp.text or '')[:200])
    except Exception as exc:
        _warn("write_failed", op="add", error=str(exc))

    for item in items_list:
        ids = _scope_ids_for_freeze(item)
        watched_at = item.get("watched_at") or item.get("watchedAt") or None
        watched_str = watched_at if isinstance(watched_at, str) else None
        if ids:
            _freeze(
                item,
                action="add",
                reasons=["write_failed"],
                ids_sent=ids,
                watched_at=watched_str,
            )
    _info("write_done", op="add", ok=False, applied=0, unresolved=len(unresolved))
    return 0, unresolved

def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_scoped: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    items_list: list[Mapping[str, Any]] = list(items or [])
    for item in items_list:
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
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_season"},
                )
                continue
            group = _merge_show_group(shows_scoped, show_entry)
            _merge_show_season(group, s_num)
            thaw_keys.append(_thaw_key(item))
            continue
        if typ == "episode":
            show_ids = _show_ids_of_episode(item)
            s_num = int(item.get("season") or item.get("season_number") or 0)
            e_num = int(item.get("episode") or item.get("episode_number") or 0)
            if not show_ids or not s_num or not e_num:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"},
                )
                continue
            show_entry = _show_scope_entry(adapter, item, show_ids)
            if not show_entry:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e"},
                )
                continue
            group = _merge_show_group(shows_scoped, show_entry)
            season = _merge_show_season(group, s_num)
            season.setdefault("episodes", []).append({"number": e_num})
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
        _info("write_skipped", op="remove", reason="empty_payload", unresolved=len(unresolved))
        return 0, unresolved
    try:
        resp = session.post(URL_REMOVE, headers=headers, json=body, timeout=timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze(thaw_keys)
            ok = len(thaw_keys)
            _info("write_done", op="remove", ok=len(unresolved) == 0 and ok == len(thaw_keys), applied=ok, unresolved=len(unresolved), movies=len(movies), shows_payload=len(shows_payload))
            return ok, unresolved
        _warn("write_failed", op="remove", status=resp.status_code, body=(resp.text or '')[:200])
    except Exception as exc:
        _warn("write_failed", op="remove", error=str(exc))
    for item in items_list:
        ids = _scope_ids_for_freeze(item)
        if ids:
            _freeze(
                item,
                action="remove",
                reasons=["write_failed"],
                ids_sent=ids,
                watched_at=None,
            )
    _info("write_done", op="remove", ok=False, applied=0, unresolved=len(unresolved))
    return 0, unresolved
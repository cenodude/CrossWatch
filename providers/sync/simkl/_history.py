# /providers/sync/simkl/_history.py
# SIMKL Module for history sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from ._common import (
    build_headers,
    coalesce_date_from,
    extract_latest_ts,
    fetch_activities,
    get_watermark,
    key_of as simkl_key_of,
    normalize as simkl_normalize,
    update_watermark_if_new,
    shadow_ttl_seconds as _shadow_ttl_seconds,
)

BASE = "https://api.simkl.com"
URL_ALL_ITEMS = f"{BASE}/sync/all-items"
URL_ADD = f"{BASE}/sync/history"
URL_REMOVE = f"{BASE}/sync/history/remove"

STATE_DIR = Path("/config/.cw_state")
UNRESOLVED_PATH = str(STATE_DIR / "simkl_history.unresolved.json")
SHADOW_PATH = str(STATE_DIR / "simkl.history.shadow.json")
SHOW_MAP_PATH = str(STATE_DIR / "simkl.show.map.json")

ID_KEYS = ("simkl", "imdb", "tmdb", "tvdb")


def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_SIMKL_DEBUG"):
        print(f"[SIMKL:history] {msg}")


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
    return (
        datetime.fromtimestamp(int(ts), tz=timezone.utc)
        .isoformat()
        .replace("+00:00", "Z")
    )


def _headers(adapter: Any, *, force_refresh: bool = False) -> dict[str, str]:
    return build_headers(
        {"simkl": {"api_key": adapter.cfg.api_key, "access_token": adapter.cfg.access_token}},
        force_refresh=force_refresh,
    )


def _ids_of(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids = dict(obj.get("ids") or {})
    return {k: ids[k] for k in ID_KEYS if ids.get(k)}


def _raw_show_ids(item: Mapping[str, Any]) -> dict[str, Any]:
    return dict(item.get("show_ids") or {})


def _show_ids_of_episode(item: Mapping[str, Any]) -> dict[str, Any]:
    show_ids = _raw_show_ids(item)
    return {k: show_ids[k] for k in ID_KEYS if show_ids.get(k)}


def _load_json(path: str) -> dict[str, Any]:
    try:
        return json.loads(Path(path).read_text("utf-8"))
    except Exception:
        return {}


def _save_json(path: str, data: Mapping[str, Any]) -> None:
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as exc:
        _log(f"save {Path(path).name} failed: {exc}")


def _is_null_env(row: Any) -> bool:
    return isinstance(row, Mapping) and row.get("type") == "null" and row.get("body") is None


def _load_unresolved() -> dict[str, Any]:
    return _load_json(UNRESOLVED_PATH)


def _save_unresolved(data: Mapping[str, Any]) -> None:
    _save_json(UNRESOLVED_PATH, data)


def _freeze(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    ids_sent: Mapping[str, Any],
    watched_at: str | None,
) -> None:
    key = simkl_key_of(id_minimal(item))
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


def _shadow_load() -> dict[str, Any]:
    return _load_json(SHADOW_PATH) or {"events": {}}


def _shadow_save(obj: Mapping[str, Any]) -> None:
    _save_json(SHADOW_PATH, obj)


def _shadow_put_all(items: Iterable[Mapping[str, Any]]) -> None:
    items_list = list(items or [])
    if not items_list:
        return
    shadow = _shadow_load()
    events: dict[str, Any] = dict(shadow.get("events") or {})
    now = _now_epoch()
    ttl = _shadow_ttl_seconds()
    for item in items_list:
        ts = _as_epoch(item.get("watched_at"))
        bucket_key = simkl_key_of(id_minimal(item))
        if not ts or not bucket_key:
            continue
        events[f"{bucket_key}@{ts}"] = {"item": id_minimal(item), "exp": now + ttl}
    shadow["events"] = events
    _shadow_save(shadow)


def _shadow_merge_into(out: dict[str, dict[str, Any]], thaw: set[str]) -> None:
    shadow = _shadow_load()
    events: dict[str, Any] = dict(shadow.get("events") or {})
    if not events:
        return
    now = _now_epoch()
    ttl = _shadow_ttl_seconds()
    changed = False
    merged = 0
    for event_key, record in list(events.items()):
        exp = int(record.get("exp") or 0)
        if exp:
            if exp < now or (ttl > 0 and exp - now > ttl * 2):
                del events[event_key]
                changed = True
                continue
        if event_key in out:
            del events[event_key]
            changed = True
            continue
        item = record.get("item")
        if isinstance(item, Mapping):
            item_dict: dict[str, Any] = dict(item)
            out[event_key] = item_dict
            thaw.add(simkl_key_of(id_minimal(item_dict)))
            merged += 1
    if merged:
        _log(f"shadow merged {merged} backfill events")
    if changed or merged:
        _shadow_save({"events": events})

_RESOLVE_CACHE: dict[str, dict[str, str]] = {}

def _load_show_map() -> dict[str, Any]:
    return _load_json(SHOW_MAP_PATH) or {"map": {}}


def _save_show_map(obj: Mapping[str, Any]) -> None:
    _save_json(SHOW_MAP_PATH, obj)


def _persist_show_map(key: str, ids: Mapping[str, Any]) -> None:
    ok = {k: str(v) for k, v in ids.items() if k in ("tvdb", "tmdb", "imdb", "simkl") and v}
    if not ok:
        return
    data = _load_show_map()
    mapping: dict[str, Any] = dict(data.get("map") or {})
    if mapping.get(key) == ok:
        return
    mapping[key] = ok
    data["map"] = mapping
    _save_show_map(data)


def _norm_title(value: str | None) -> str:
    return "".join(ch for ch in (value or "").lower() if ch.isalnum())


def _best_ids(obj: Mapping[str, Any]) -> dict[str, str]:
    ids = dict(obj.get("ids") or obj or {})
    return {k: str(ids[k]) for k in ("tvdb", "tmdb", "imdb", "simkl") if ids.get(k)}


def _simkl_search_show(adapter: Any, title: str, year: int | None) -> dict[str, str]:
    if not title or os.getenv("CW_SIMKL_AUTO_RESOLVE", "1") == "0":
        return {}
    session = adapter.client.session
    headers = _headers(adapter, force_refresh=True)
    try:
        resp = session.get(
            f"{BASE}/search/tv",
            headers=headers,
            params={"q": title, "limit": 5, "extended": "full"},
            timeout=adapter.cfg.timeout,
        )
        if not resp.ok:
            return {}
        arr = resp.json() or []
    except Exception:
        return {}
    want = _norm_title(title)
    pick: dict[str, Any] = {}
    best = -1
    if isinstance(arr, list):
        for x in arr:
            show = (x.get("show") if isinstance(x, Mapping) else None) or x
            if not isinstance(show, Mapping):
                continue
            ids = _best_ids(show)
            show_title = (show or {}).get("title") or ""
            show_year = (show or {}).get("year")
            if not ids:
                continue
            score = 0
            if _norm_title(show_title) == want:
                score += 2
            if year and show_year:
                try:
                    if abs(int(show_year) - int(year)) <= 1:
                        score += 1
                except Exception:
                    pass
            if score > best:
                best = score
                pick = {"ids": ids, "title": show_title, "year": show_year}
    raw_ids = pick.get("ids")
    ids_source: Mapping[str, Any]
    if isinstance(raw_ids, Mapping):
        ids_source = raw_ids
    else:
        ids_source = pick
    return _best_ids(ids_source)


def _simkl_resolve_show_via_episode_id(adapter: Any, item: Mapping[str, Any]) -> dict[str, str]:
    ids = dict(item.get("ids") or {})
    params: dict[str, str] = {}
    for key in ("imdb", "tvdb", "tmdb"):
        value = ids.get(key)
        if value:
            params[key] = str(value)
    if not params:
        return {}
    session = adapter.client.session
    headers = _headers(adapter, force_refresh=True)
    try:
        resp = session.get(
            f"{BASE}/search/id",
            headers=headers,
            params=params,
            timeout=adapter.cfg.timeout,
        )
        if not resp.ok:
            return {}
        body = resp.json() or {}
    except Exception:
        return {}
    candidate: Any = body
    if isinstance(body, list) and body:
        candidate = body[0]
    if isinstance(candidate, Mapping):
        show = candidate.get("show") if isinstance(candidate.get("show"), Mapping) else candidate
        if isinstance(show, Mapping):
            return _best_ids(show)
    return {}


def _resolve_show_ids(adapter: Any, item: Mapping[str, Any], raw_show_ids: Mapping[str, Any]) -> dict[str, str]:
    have = {k: raw_show_ids[k] for k in ("tvdb", "tmdb", "imdb", "simkl") if raw_show_ids.get(k)}
    if have:
        return {k: str(v) for k, v in have.items()}
    plex = raw_show_ids.get("plex")
    title = (
        item.get("series_title")
        or item.get("show_title")
        or item.get("grandparent_title")
        or item.get("title")
        or ""
    )
    key = f"plex:{plex}" if plex else _norm_title(title)
    if key in _RESOLVE_CACHE:
        return _RESOLVE_CACHE[key]
    mapping = _load_show_map().get("map", {})
    if isinstance(mapping, Mapping) and key in mapping:
        cached = dict(mapping[key])
        _RESOLVE_CACHE[key] = cached
        return cached
    year = item.get("series_year") or item.get("year")
    year_int = year if isinstance(year, int) else None
    found = _simkl_search_show(adapter, title, year_int)
    if not found:
        found = _simkl_resolve_show_via_episode_id(adapter, item)
    if found:
        _persist_show_map(key, found)
        _RESOLVE_CACHE[key] = found
        return found
    return {}


def _fetch_kind(
    session: Any,
    headers: Mapping[str, str],
    *,
    kind: str,
    since_iso: str,
    timeout: float,
) -> list[dict[str, Any]]:
    params = {"extended": "full", "episode_watched_at": "yes", "date_from": since_iso}
    resp = session.get(f"{URL_ALL_ITEMS}/{kind}", headers=headers, params=params, timeout=timeout)
    if not resp.ok:
        _log(f"GET {URL_ALL_ITEMS}/{kind} -> {resp.status_code}")
        return []
    try:
        body = resp.json() or []
    except Exception:
        body = []
    if isinstance(body, list):
        return [x for x in body if not _is_null_env(x)]
    if isinstance(body, Mapping):
        arr = body.get(kind) or body.get("items") or []
        if isinstance(arr, list):
            return [x for x in arr if not _is_null_env(x)]
    return []


def build_index(adapter: Any, since: int | None = None, limit: int | None = None) -> dict[str, dict[str, Any]]:
    session = adapter.client.session
    timeout = adapter.cfg.timeout
    out: dict[str, dict[str, Any]] = {}
    thaw: set[str] = set()

    activities, _rate = fetch_activities(session, _headers(adapter, force_refresh=True), timeout=timeout)
    if isinstance(activities, Mapping) and since is None:
        wm_movies = get_watermark("history:movies") or ""
        wm_shows = get_watermark("history:shows") or ""
        latest_movies = extract_latest_ts(
            activities,
            (("movies", "completed"), ("movies", "watched"), ("history", "movies"), ("movies", "all")),
        )
        latest_shows = extract_latest_ts(
            activities,
            (("shows", "completed"), ("shows", "watched"), ("history", "shows"), ("shows", "all")),
        )
        no_change = (latest_movies is None or latest_movies <= wm_movies) and (
            latest_shows is None or latest_shows <= wm_shows
        )
        if no_change:
            _shadow_merge_into(out, thaw)
            _unfreeze(thaw)
            _log(
                "activities unchanged; history noop "
                f"(movies={latest_movies}, shows={latest_shows}); shadow={len(out)}"
            )
            _log(f"index size: {len(out)}")
            return out

    headers = _headers(adapter, force_refresh=True)
    added = 0
    latest_ts_movies: int | None = None
    latest_ts_shows: int | None = None
    cfg_iso = _as_iso(since) if since else None
    df_movies_iso = coalesce_date_from("history:movies", cfg_date_from=cfg_iso)
    df_shows_iso = coalesce_date_from("history:shows", cfg_date_from=cfg_iso)
    if since:
        since_iso = _as_iso(int(since))
        try:
            sm = max(_as_epoch(df_movies_iso) or 0, _as_epoch(since_iso) or 0)
            ss = max(_as_epoch(df_shows_iso) or 0, _as_epoch(since_iso) or 0)
            df_movies_iso = _as_iso(sm)
            df_shows_iso = _as_iso(ss)
        except Exception:
            pass

    movie_rows = _fetch_kind(session, headers, kind="movies", since_iso=df_movies_iso, timeout=timeout)
    movies_cnt = 0
    for row in movie_rows:
        if not isinstance(row, Mapping):
            continue
        movie = row.get("movie") or row
        watched_at = (row.get("last_watched_at") or row.get("watched_at") or "").strip()
        ts = _as_epoch(watched_at)
        if not movie or not ts:
            continue
        movie_norm = simkl_normalize(movie)
        movie_norm["watched"] = True
        movie_norm["watched_at"] = watched_at
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
            break

    if not limit or added < limit:
        show_rows = _fetch_kind(session, headers, kind="shows", since_iso=df_shows_iso, timeout=timeout)
        eps_cnt = 0
        for row in show_rows:
            if not isinstance(row, Mapping):
                continue
            show = row.get("show") or row
            if not show:
                continue
            base = simkl_normalize(show)
            show_ids = _ids_of(base) or _ids_of(show)
            if not show_ids:
                continue
            for season in row.get("seasons") or []:
                s_num = int((season or {}).get("number") or (season or {}).get("season") or 0)
                for episode in season.get("episodes") or []:
                    e_num = int((episode or {}).get("number") or (episode or {}).get("episode") or 0)
                    watched_at = (episode.get("watched_at") or episode.get("last_watched_at") or "").strip()
                    ts = _as_epoch(watched_at)
                    if not ts or not s_num or not e_num:
                        continue
                    ep = {
                        "type": "episode",
                        "season": s_num,
                        "episode": e_num,
                        "ids": show_ids,
                        "title": base.get("title"),
                        "year": base.get("year"),
                        "watched": True,
                        "watched_at": watched_at,
                    }
                    bucket_key = simkl_key_of(id_minimal(ep))
                    event_key = f"{bucket_key}@{ts}"
                    if event_key in out:
                        continue
                    out[event_key] = ep
                    thaw.add(bucket_key)
                    eps_cnt += 1
                    added += 1
                    latest_ts_shows = max(latest_ts_shows or 0, ts)
                    if limit and added >= limit:
                        break
                if limit and added >= limit:
                    break
        _shadow_merge_into(out, thaw)
        _log(
            f"movies={movies_cnt} episodes={eps_cnt} from_movies={df_movies_iso} from_shows={df_shows_iso}",
        )
    else:
        _shadow_merge_into(out, thaw)
        _log(
            f"movies={movies_cnt} episodes=0 from_movies={df_movies_iso} from_shows={df_shows_iso}",
        )

    if latest_ts_movies:
        update_watermark_if_new("history:movies", _as_iso(latest_ts_movies))
    if latest_ts_shows:
        update_watermark_if_new("history:shows", _as_iso(latest_ts_shows))

    _unfreeze(thaw)
    try:
        _shadow_put_all(out.values())
    except Exception as exc:
        _log(f"shadow.put index skipped: {exc}")
    _log(f"index size: {len(out)}")
    return out


def _movie_add_entry(item: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(item)
    watched_at = (item.get("watched_at") or item.get("watchedAt") or "").strip()
    if not ids or not watched_at:
        return None
    return {"ids": ids, "watched_at": watched_at}


def _show_add_entry(item: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(item)
    return {"ids": ids} if ids else None


def _episode_add_entry(item: Mapping[str, Any]) -> tuple[dict[str, Any], int, int, str] | None:
    s_num = int(item.get("season") or item.get("season_number") or 0)
    e_num = int(item.get("episode") or item.get("episode_number") or 0)
    watched_at = (item.get("watched_at") or item.get("watchedAt") or "").strip()
    if not s_num or not e_num or not watched_at:
        return None
    show_ids_raw = _raw_show_ids(item)
    adapter = item.get("_adapter") if isinstance(item, Mapping) else None
    if adapter:
        show_ids = _resolve_show_ids(adapter, item, show_ids_raw) or {
            k: show_ids_raw.get(k) for k in ID_KEYS if show_ids_raw.get(k)
        }
    else:
        show_ids = {k: show_ids_raw.get(k) for k in ID_KEYS if show_ids_raw.get(k)}
    if not show_ids:
        return None
    show = {
        "ids": show_ids,
        "title": item.get("series_title") or item.get("title"),
        "year": item.get("series_year") or item.get("year"),
    }
    return show, s_num, e_num, watched_at


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_with_eps: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    shadow_events: list[dict[str, Any]] = []
    items_list: list[Mapping[str, Any]] = list(items or [])
    for item in items_list:
        if isinstance(item, dict):
            item["_adapter"] = adapter
    for item in items_list:
        typ = str(item.get("type") or "").lower()
        if typ == "movie":
            entry = _movie_add_entry(item)
            if entry:
                movies.append(entry)
                thaw_keys.append(simkl_key_of(id_minimal(item)))
                ev = dict(id_minimal(item))
                ev["watched_at"] = entry.get("watched_at")
                if ev.get("watched_at"):
                    shadow_events.append(ev)
            else:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids_or_watched_at"})
            continue
        if typ == "episode":
            packed = _episode_add_entry(item)
            if not packed:
                unresolved.append(
                    {"item": id_minimal(item), "hint": "missing_show_ids_or_s/e_or_watched_at"},
                )
                continue
            show_entry, s_num, e_num, watched_at = packed
            ids_key = json.dumps(show_entry["ids"], sort_keys=True)
            group = shows_with_eps.setdefault(
                ids_key,
                {
                    "ids": show_entry["ids"],
                    "title": show_entry.get("title"),
                    "year": show_entry.get("year"),
                    "seasons": [],
                },
            )
            season = next((s for s in group["seasons"] if s.get("number") == s_num), None)
            if not season:
                season = {"number": s_num, "episodes": []}
                group["seasons"].append(season)
            season["episodes"].append({"number": e_num, "watched_at": watched_at})
            thaw_keys.append(simkl_key_of(id_minimal(item)))
            ev = dict(id_minimal(item))
            ev["watched_at"] = watched_at
            shadow_events.append(ev)
            continue
        entry = _show_add_entry(item)
        if entry:
            shows_whole.append(entry)
            thaw_keys.append(simkl_key_of(id_minimal(item)))
        else:
            unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    shows_payload: list[dict[str, Any]] = []
    if shows_whole:
        shows_payload.extend(shows_whole)
    if shows_with_eps:
        shows_payload.extend(list(shows_with_eps.values()))
    if shows_payload:
        body["shows"] = shows_payload
    if not body:
        return 0, unresolved
    try:
        resp = session.post(URL_ADD, headers=headers, json=body, timeout=timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze(thaw_keys)
            eps_count = sum(
                len(season.get("episodes", []))
                for group in shows_with_eps.values()
                for season in group.get("seasons", [])
            )
            ok = len(movies) + eps_count + len(shows_whole)
            _log(
                f"add done http:{resp.status_code} movies={len(movies)} shows={len(shows_payload)} episodes={eps_count}",
            )
            try:
                _shadow_put_all(shadow_events)
            except Exception as exc:
                _log(f"shadow.put skipped: {exc}")
            return ok, unresolved
        _log(f"ADD failed {resp.status_code}: {(resp.text or '')[:200]}")
    except Exception as exc:
        _log(f"ADD error: {exc}")
    for item in items_list:
        ids = _ids_of(item) or _show_ids_of_episode(item)
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
    return 0, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    timeout = adapter.cfg.timeout
    movies: list[dict[str, Any]] = []
    shows_whole: list[dict[str, Any]] = []
    shows_with_eps: dict[str, dict[str, Any]] = {}
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    items_list: list[Mapping[str, Any]] = list(items or [])
    for item in items_list:
        typ = str(item.get("type") or "").lower()
        if typ == "movie":
            ids = _ids_of(item)
            if not ids:
                unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
                continue
            movies.append({"ids": ids})
            thaw_keys.append(simkl_key_of(id_minimal(item)))
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
            ids_key = json.dumps(show_ids, sort_keys=True)
            group = shows_with_eps.setdefault(ids_key, {"ids": show_ids, "seasons": []})
            season = next((s for s in group["seasons"] if s.get("number") == s_num), None)
            if not season:
                season = {"number": s_num, "episodes": []}
                group["seasons"].append(season)
            season["episodes"].append({"number": e_num})
            thaw_keys.append(simkl_key_of(id_minimal(item)))
            continue
        ids = _ids_of(item)
        if ids:
            shows_whole.append({"ids": ids})
            thaw_keys.append(simkl_key_of(id_minimal(item)))
        else:
            unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    shows_payload: list[dict[str, Any]] = []
    if shows_whole:
        shows_payload.extend(shows_whole)
    if shows_with_eps:
        shows_payload.extend(list(shows_with_eps.values()))
    if shows_payload:
        body["shows"] = shows_payload
    if not body:
        return 0, unresolved
    try:
        resp = session.post(URL_REMOVE, headers=headers, json=body, timeout=timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze(thaw_keys)
            ok = len(movies) + len(shows_payload)
            _log(
                f"remove done http:{resp.status_code} movies={len(movies)} shows={len(shows_payload)}",
            )
            return ok, unresolved
        _log(f"REMOVE failed {resp.status_code}: {(resp.text or '')[:200]}")
    except Exception as exc:
        _log(f"REMOVE error: {exc}")
    for item in items_list:
        ids = _ids_of(item) or _show_ids_of_episode(item)
        if ids:
            _freeze(
                item,
                action="remove",
                reasons=["write_failed"],
                ids_sent=ids,
                watched_at=None,
            )
    return 0, unresolved

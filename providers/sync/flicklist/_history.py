# CrossWatch - FlickList history sync
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

from ._common import (
    HISTORY_PAGE_DEFAULT,
    HISTORY_PAGE_MAX,
    URL_HISTORY,
    URL_WATCHED_MOVIES,
    URL_WATCHED_SHOWS,
    as_int,
    cfg_int,
    cfg_section,
    chunk,
    classify_write,
    error_of,
    event_key,
    flicklist_request,
    iso_z,
    has_write_counters,
    header_int,
    key_of,
    not_future,
    now_iso,
    ok_status,
    read_minimal,
    read_shape,
    safe_json,
    write_ident,
    write_batch_size,
    _dbg,
    _info,
    _warn,
)

FEATURE = "history"


def _sum_counters(rows: list[dict[str, Any]]) -> dict[str, int]:
    out: dict[str, int] = {}
    for row in rows:
        for key, value in (row or {}).items():
            out[key] = out.get(key, 0) + int(value or 0)
    return out


def _rows(data: Any) -> list[Any]:
    if isinstance(data, list):
        return data
    if isinstance(data, Mapping):
        for key in ("items", "data", "results", "history", "events"):
            value = data.get(key)
            if isinstance(value, list):
                return value
    return []


def _page_count(data: Any, current: int) -> int:
    if not isinstance(data, Mapping):
        return current
    for key in ("page_count", "pageCount", "pages", "total_pages", "totalPages"):
        value = as_int(data.get(key))
        if value is not None:
            return max(current, value)
    meta = data.get("pagination")
    if isinstance(meta, Mapping):
        return _page_count(meta, current)
    return current


def _response_page_count(resp: Any, data: Any, current: int) -> int:
    header_count = header_int(resp, "X-FlickList-Page-Count")
    if header_count is not None:
        return max(current, header_count)
    return _page_count(data, current)


def _row_to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    item = read_minimal(row, default_type=row.get("type") or row.get("media_type") or "movie")
    if not item:
        return None
    watched_at = row.get("watched_at")
    if watched_at:
        item["watched_at"] = watched_at
    event_id = str(row.get("id") or "").strip()
    if event_id:
        item["_flicklist_history_id"] = event_id
    return item


def _movie_row_to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    item = read_minimal(row, default_type="movie")
    if not item:
        return None
    watched_at = row.get("last_watched_at") or row.get("watched_at")
    if watched_at:
        item["watched_at"] = watched_at
    plays = as_int(row.get("plays"))
    if plays is not None:
        item["plays"] = plays
    item["_flicklist_watched_movie"] = True
    return item


def _supplement_watched_movies(adapter: Any, out: dict[str, dict[str, Any]]) -> tuple[int, int]:
    resp = flicklist_request(adapter, "GET", URL_WATCHED_MOVIES)
    if not ok_status(resp):
        _warn(FEATURE, "watched_movies_failed", status=int(resp.status_code), error=error_of(resp))
        return 0, 0
    data = safe_json(resp)
    rows = _rows(data)
    if not rows:
        _dbg(FEATURE, "watched_movies_empty_response", **read_shape(resp, data))
    added = 0
    skipped = 0
    existing_bases = {str(key).split("@", 1)[0].lower() for key in out}
    for row in rows:
        if not isinstance(row, Mapping):
            skipped += 1
            continue
        item = _movie_row_to_minimal(row)
        if not item:
            skipped += 1
            continue
        key = key_of(item)
        if not key:
            skipped += 1
            continue
        base = key.split("@", 1)[0].lower()
        if base in existing_bases or key in out:
            skipped += 1
            continue
        out[key] = item
        existing_bases.add(base)
        added += 1
    _info(FEATURE, "watched_movies_supplement_done", added=added, skipped=skipped)
    return added, skipped


def _supplement_watched_shows(adapter: Any, out: dict[str, dict[str, Any]]) -> tuple[int, int]:
    resp = flicklist_request(adapter, "GET", URL_WATCHED_SHOWS)
    if not ok_status(resp):
        _warn(FEATURE, "watched_shows_failed", status=int(resp.status_code), error=error_of(resp))
        return 0, 0
    data = safe_json(resp)
    rows = _rows(data)
    if not rows:
        _dbg(FEATURE, "watched_shows_empty_response", **read_shape(resp, data))
    added = 0
    skipped = 0
    existing_bases = {str(key).split("@", 1)[0].lower() for key in out}
    for row in rows:
        if not isinstance(row, Mapping):
            skipped += 1
            continue
        raw_show = row.get("show")
        show: Mapping[str, Any] = raw_show if isinstance(raw_show, Mapping) else row
        raw_ids = show.get("ids")
        ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
        if not ids:
            skipped += 1
            continue
        title = str(show.get("title") or row.get("title") or "").strip()
        year = show.get("year") or row.get("year")
        seasons = row.get("seasons")
        if not isinstance(seasons, list):
            skipped += 1
            continue
        for season_row in seasons:
            if not isinstance(season_row, Mapping):
                skipped += 1
                continue
            season = as_int(season_row.get("number") if season_row.get("number") is not None else season_row.get("season"))
            episodes = season_row.get("episodes")
            if season is None or not isinstance(episodes, list):
                skipped += 1
                continue
            for ep_row in episodes:
                if not isinstance(ep_row, Mapping):
                    skipped += 1
                    continue
                episode = as_int(ep_row.get("number") if ep_row.get("number") is not None else ep_row.get("episode"))
                if episode is None:
                    skipped += 1
                    continue
                payload: dict[str, Any] = {
                    "type": "episode",
                    "title": title,
                    "season_number": season,
                    "episode_number": episode,
                    "ids": ids,
                }
                if year is not None:
                    payload["year"] = year
                ep_title = str(ep_row.get("title") or ep_row.get("name") or ep_row.get("episode_name") or "").strip()
                if ep_title:
                    payload["episode_name"] = ep_title
                item = read_minimal(payload, default_type="episode")
                if not item:
                    skipped += 1
                    continue
                watched_at = ep_row.get("last_watched_at") or ep_row.get("watched_at") or row.get("last_watched_at")
                if watched_at:
                    item["watched_at"] = watched_at
                plays = as_int(ep_row.get("plays"))
                if plays is not None:
                    item["plays"] = plays
                item["_flicklist_watched_show"] = True
                key = key_of(item)
                if not key:
                    skipped += 1
                    continue
                base = key.split("@", 1)[0].lower()
                if base in existing_bases or key in out:
                    skipped += 1
                    continue
                out[key] = item
                existing_bases.add(base)
                added += 1
    _info(FEATURE, "watched_shows_supplement_done", added=added, skipped=skipped)
    return added, skipped


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    section = cfg_section(adapter) or {}
    per_page = max(1, min(cfg_int(section, "history_per_page", HISTORY_PAGE_DEFAULT), HISTORY_PAGE_MAX))
    max_pages = max(1, cfg_int(section, "history_max_pages", 500))
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    page = 1
    while page <= max_pages:
        resp = flicklist_request(adapter, "GET", URL_HISTORY, params={"page": page, "limit": per_page})
        if not ok_status(resp):
            _warn(FEATURE, "index_failed", page=page, status=int(resp.status_code), error=error_of(resp))
            break
        data = safe_json(resp)
        rows = _rows(data)
        if not rows:
            if page == 1:
                _dbg(FEATURE, "index_empty_response", **read_shape(resp, data))
            break
        for row in rows:
            if not isinstance(row, Mapping):
                skipped += 1
                continue
            item = _row_to_minimal(row)
            if not item:
                skipped += 1
                continue
            key = event_key(item)
            if not key:
                skipped += 1
                continue
            if key in out:
                suffix = str(item.get("_flicklist_history_id") or len(out))
                key = f"{key}~fl{suffix}"
            out[key] = item
        if page >= _response_page_count(resp, data, page):
            break
        page += 1
    if not any((item.get("type") == "movie") for item in out.values()):
        added, extra_skipped = _supplement_watched_movies(adapter, out)
        skipped += extra_skipped
        if added:
            _info(FEATURE, "index_movie_snapshot_supplemented", count=added)
    if not any((item.get("type") == "episode") for item in out.values()):
        added, extra_skipped = _supplement_watched_shows(adapter, out)
        skipped += extra_skipped
        if added:
            _info(FEATURE, "index_show_snapshot_supplemented", count=added)
    _info(FEATURE, "index_done", count=len(out), skipped=skipped, pages=page)
    return out


def _accepted(items: Iterable[Mapping[str, Any]], *, include_watched_at: bool) -> tuple[list[tuple[str, dict[str, Any]]], list[str], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        key = str(raw.get("_cw_event_key") or "").strip() or event_key(raw) or key_of(raw)
        payload = write_ident(raw)
        if not key or payload is None:
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id"})
            continue
        if payload.get("media_type") == "show" and payload.get("season") is None:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "unsupported_media_type"})
            continue
        if include_watched_at:
            payload["watched_at"] = not_future(iso_z(raw.get("watched_at"))) or now_iso()
        if key in seen:
            continue
        seen.add(key)
        accepted.append((key, payload))
    return accepted, unresolved_keys, unresolved


def _write(adapter: Any, method: str, items: Iterable[Mapping[str, Any]], *, include_watched_at: bool, dry_run: bool = False) -> dict[str, Any]:
    accepted, unresolved_keys, unresolved = _accepted(items, include_watched_at=include_watched_at)
    confirmed: list[str] = []
    ok = True
    if dry_run:
        return {"ok": True, "count": len(accepted), "confirmed_keys": [k for k, _ in accepted], "unresolved_keys": unresolved_keys, "unresolved": unresolved}
    totals: list[dict[str, Any]] = []
    sampled = False
    batch_size = write_batch_size(adapter, FEATURE)
    for batch in chunk([{"key": k, "payload": p} for k, p in accepted], batch_size):
        sent = [(str(row["key"]), row["payload"]) for row in batch]
        if not sampled:
            sampled = True
            _dbg(FEATURE, "write_payload_sample", method=method, batch=len(sent), sample=json.dumps([p for _k, p in sent[:3]], sort_keys=True))
        resp = flicklist_request(adapter, method, URL_HISTORY, json={"items": [p for _k, p in sent]})
        if ok_status(resp):
            body = safe_json(resp)
            if not has_write_counters(body, method):
                ok = False
                for key, _payload in sent:
                    unresolved_keys.append(key)
                    unresolved.append({"key": key, "status": "invalid_response", "error": "missing FlickList write counters"})
                _warn(FEATURE, "write_invalid_response", method=method, status=int(resp.status_code), count=len(sent))
                continue
            yes, misses, miss_rows, counters = classify_write(sent, body, method=method)
            totals.append(counters)
            if misses:
                ok = False
                _warn(FEATURE, "write_partial", method=method, sent=len(sent), confirmed=len(yes), unresolved=len(misses), **counters)
            confirmed.extend(yes)
            unresolved_keys.extend(misses)
            unresolved.extend(miss_rows)
            continue
        ok = False
        for key, _payload in sent:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "write_failed", method=method, status=int(resp.status_code), error=error_of(resp))
    _info(FEATURE, "write_done", action=method.lower(), sent=len(accepted), confirmed=len(confirmed), unresolved=len(unresolved_keys), dry_run=bool(dry_run), **_sum_counters(totals))
    return {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}


def add(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, "POST", items, include_watched_at=True, dry_run=dry_run)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]], *, dry_run: bool = False) -> dict[str, Any]:
    return _write(adapter, "DELETE", items, include_watched_at=False, dry_run=dry_run)

# /providers/sync/punchplay/_history.py
# PunchPlay history sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    HISTORY_PAGE_MAX,
    URL_HISTORY,
    URL_TITLE_HISTORY,
    URL_WATCH_HISTORY,
    bulk_write,
    cfg_int,
    cfg_section,
    client_item_id,
    error_of,
    has_write_id,
    ids_for_punchplay,
    iso_z,
    not_future,
    punchplay_request,
    request_id_of,
    safe_json,
    title_path_id,
    _dbg,
    _info,
    _warn,
)

FEATURE = "history"

HISTORY_ID_FIELD = "_punchplay_history_ids"


def _key_of(obj: Mapping[str, Any]) -> str:
    try:
        return str(canonical_key(id_minimal(obj)) or "").strip()
    except Exception:
        return ""


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        out = int(str(value).strip())
    except Exception:
        return None
    return out


def _row_to_minimal(row: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = str(row.get("type") or "").strip().lower()
    watched = iso_z(row.get("watchedAt"))
    if not watched:
        return None

    if typ == "episode":
        show_tmdb = _as_int(row.get("showTmdbId"))
        season = _as_int(row.get("season"))
        episode = _as_int(row.get("episode"))
        if not show_tmdb or season is None or episode is None:
            return None
        out: dict[str, Any] = {
            "type": "episode",
            "show_ids": {"tmdb": str(show_tmdb)},
            "ids": {},
            "season": season,
            "episode": episode,
        }
        ep_tmdb = _as_int(row.get("tmdbId"))
        if ep_tmdb:
            out["ids"] = {"tmdb": str(ep_tmdb)}
        series_title = str(row.get("title") or "").strip()
        if series_title:
            out["series_title"] = series_title
        ep_title = str(row.get("episodeTitle") or "").strip()
        if ep_title:
            out["title"] = ep_title
    else:
        tmdb = _as_int(row.get("tmdbId"))
        if not tmdb:
            return None
        out = {"type": "movie", "ids": {"tmdb": str(tmdb)}}
        title = str(row.get("title") or "").strip()
        if title:
            out["title"] = title
        year = _as_int(row.get("year"))
        if year:
            out["year"] = year

    out["watched_at"] = watched
    entry_id = _as_int(row.get("id"))
    if entry_id:
        out[HISTORY_ID_FIELD] = [str(entry_id)]
    return out


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    section = cfg_section(adapter) or {}
    per_page = max(1, min(cfg_int(section, "history_per_page", HISTORY_PAGE_MAX), HISTORY_PAGE_MAX))
    max_pages = cfg_int(section, "history_max_pages", 5000)

    collected: dict[str, dict[str, Any]] = {}
    cursor: str | None = None
    pages = 0

    while True:
        pages += 1
        if max_pages and pages > max_pages:
            _warn(FEATURE, "index_page_cap_hit", max_pages=max_pages)
            break
        params: dict[str, Any] = {"limit": per_page}
        if cursor:
            params["cursor"] = cursor
        resp = punchplay_request(adapter, "GET", URL_HISTORY, params=params)
        if resp.status_code != 200:
            _warn(FEATURE, "history_fetch_failed", status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            break
        data = safe_json(resp) or {}
        if not isinstance(data, Mapping):
            break
        rows = data.get("items")
        rows = [r for r in rows if isinstance(r, Mapping)] if isinstance(rows, list) else []

        for row in rows:
            minimal = _row_to_minimal(row)
            if not minimal:
                continue
            key = _key_of(minimal)
            if not key:
                continue
            existing = collected.get(key)
            if existing is None:
                collected[key] = minimal
                continue
            ids = list(existing.get(HISTORY_ID_FIELD) or [])
            for hid in minimal.get(HISTORY_ID_FIELD) or []:
                if hid not in ids:
                    ids.append(hid)
            if str(minimal.get("watched_at") or "") > str(existing.get("watched_at") or ""):
                minimal[HISTORY_ID_FIELD] = ids
                collected[key] = minimal
            else:
                existing[HISTORY_ID_FIELD] = ids

        cursor = data.get("nextCursor") or None
        if not cursor or not rows:
            break

    _info(FEATURE, "index_done", count=len(collected), pages=pages)
    return collected


def _payload_for(item: Mapping[str, Any]) -> tuple[dict[str, Any], str] | None:
    key = _key_of(item)
    if not key:
        return None

    typ = str(item.get("type") or "").strip().lower()
    is_episode = typ == "episode"

    source = item
    if is_episode and isinstance(item.get("show_ids"), Mapping) and item.get("show_ids"):
        source = {"ids": item.get("show_ids")}
    payload = ids_for_punchplay(source)
    if not has_write_id(payload):
        return None

    watched = not_future(iso_z(item.get("watched_at")))
    if not watched:
        return None

    payload["kind"] = "episode" if is_episode else "movie"
    payload["watched_at"] = watched

    if is_episode:
        season = _as_int(item.get("season"))
        episode = _as_int(item.get("episode"))
        if season is None or episode is None or episode < 1:
            return None
        payload["season"] = season
        payload["episode"] = episode
        ep_title = str(item.get("title") or "").strip()
        if ep_title:
            payload["episode_title"] = ep_title[:500]
        series_title = str(item.get("series_title") or "").strip()
        if series_title:
            payload["title"] = series_title[:500]
    else:
        title = str(item.get("title") or "").strip()
        if title:
            payload["title"] = title[:500]
        year = _as_int(item.get("year"))
        if year and 1888 <= year <= 2100:
            payload["year"] = year

    runtime = _as_int(item.get("runtime") or item.get("runtime_minutes"))
    if runtime and runtime > 0:
        payload["runtime_minutes"] = runtime

    payload["client_item_id"] = client_item_id("cw", "history", key, watched)
    return payload, key


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    entries: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []

    for item in items or []:
        built = _payload_for(item)
        if built is None:
            key = _key_of(item)
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id_or_watched_at"})
                _dbg(FEATURE, "item_unresolved_before_write", key=key)
            continue
        payload, key = built
        entries.append({"payload": payload, "key": key})

    if not entries:
        return {
            "ok": True,
            "confirmed_keys": [],
            "unresolved_keys": unresolved_keys,
            "deferred_keys": [],
            "unresolved": unresolved,
        }

    _dbg(FEATURE, "write_prepare", action="add", items=len(entries), skipped=len(unresolved_keys))
    res = bulk_write(adapter, "history", entries, _key_of, feature=FEATURE)
    res["unresolved_keys"] = list(res.get("unresolved_keys") or []) + unresolved_keys
    res["unresolved"] = list(res.get("unresolved") or []) + unresolved
    _info(
        FEATURE,
        "write_done",
        action="add",
        confirmed=len(res.get("confirmed_keys") or []),
        deferred=len(res.get("deferred_keys") or []),
        unresolved=len(res.get("unresolved_keys") or []),
    )
    return res


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    confirmed: list[str] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    ok = True

    for item in items or []:
        key = _key_of(item)
        if not key:
            continue

        typ = str(item.get("type") or "").strip().lower()
        entry_ids = [str(x) for x in (item.get(HISTORY_ID_FIELD) or []) if str(x or "").strip()]

        if entry_ids:
            deleted = 0
            for entry_id in entry_ids:
                resp = punchplay_request(adapter, "DELETE", URL_WATCH_HISTORY.format(entry_id=entry_id))
                if 200 <= resp.status_code < 300 or resp.status_code == 404:
                    deleted += 1
                else:
                    ok = False
                    _warn(FEATURE, "history_delete_failed", key=key, entry_id=entry_id, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))
            if deleted:
                confirmed.append(key)
            else:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "delete_failed"})
            continue

        if typ == "episode":
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_history_entry_id"})
            _dbg(FEATURE, "item_unresolved_before_write", key=key, reason="episode_delete_needs_entry_id")
            continue

        payload = ids_for_punchplay(item)
        path_id = title_path_id(payload)
        if not path_id:
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": "missing_supported_id"})
            continue

        resp = punchplay_request(adapter, "DELETE", URL_TITLE_HISTORY.format(kind="movie", title_id=path_id))
        if 200 <= resp.status_code < 300 or resp.status_code == 404:
            confirmed.append(key)
        else:
            ok = False
            unresolved_keys.append(key)
            unresolved.append({"key": key, "status": f"http:{resp.status_code}", "error": error_of(resp)})
            _warn(FEATURE, "history_delete_failed", key=key, status=resp.status_code, error=error_of(resp), request_id=request_id_of(resp))

    _info(FEATURE, "write_done", action="remove", confirmed=len(confirmed), unresolved=len(unresolved_keys))
    return {
        "ok": ok,
        "confirmed_keys": confirmed,
        "unresolved_keys": unresolved_keys,
        "deferred_keys": [],
        "unresolved": unresolved,
    }

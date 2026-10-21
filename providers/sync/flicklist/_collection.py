# providers/sync/flicklist/_collection.py
# CrossWatch - FlickList collection sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from typing import Any

from cw_platform.collection_media import from_trakt_metadata, normalize_format, to_flicklist_fields
from providers.auth._auth_FLICKLIST import app_client_id

from ._common import (
    URL_COLLECTION,
    URL_COLLECTION_MOVIES,
    URL_COLLECTION_SHOWS,
    FlickListError,
    as_int,
    cfg_section,
    chunk,
    classify_write,
    error_of,
    flicklist_request,
    has_write_counters,
    ids_from_flicklist,
    iso_z,
    key_of,
    not_future,
    ok_status,
    safe_json,
    write_batch_size,
    write_ident,
    _info,
    _warn,
)

FEATURE = "collection"
_ITEM_INDEX = re.compile(r"items\[(\d+)\]")


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _own_source(adapter: Any) -> str:
    return app_client_id(cfg_section(adapter)).lower()


def _copies(raw: Any) -> list[dict[str, str]]:
    out: list[dict[str, str]] = []
    for copy in raw if isinstance(raw, list) else []:
        if not isinstance(copy, Mapping):
            continue
        meta = _mapping(copy.get("metadata"))
        out.append({"format": normalize_format(meta.get("media_type")) or "digital", "source": str(copy.get("source") or "").strip()})
    return out


def _with_copy_info(item: dict[str, Any], row: Mapping[str, Any]) -> dict[str, Any]:
    fmt, media = from_trakt_metadata(row.get("metadata"))
    if fmt:
        item["format"] = fmt
    if media:
        item["media_info"] = media
    copy_dates = sorted(d for c in row.get("copies") or [] if isinstance(c, Mapping) and (d := iso_z(c.get("collected_at"))))
    collected_at = iso_z(row.get("collected_at")) or (copy_dates[0] if copy_dates else None)
    if collected_at:
        item["collected_at"] = collected_at
    copies = _copies(row.get("copies"))
    if copies:
        item["_flicklist_copies"] = copies
    return item


def _split_ids(raw: Any) -> tuple[dict[str, str], str]:
    ids = ids_from_flicklist(_mapping(raw))
    fldb = ids.pop("fldb", "")
    return ids, fldb


def _movie_item(row: Mapping[str, Any]) -> dict[str, Any] | None:
    movie = _mapping(row.get("movie"))
    ids, fldb = _split_ids(movie.get("ids"))
    if not ids:
        return None
    item: dict[str, Any] = {"type": "movie", "title": movie.get("title"), "year": as_int(movie.get("year")), "ids": ids}
    if fldb:
        item["_flicklist_fldb"] = fldb
    return _with_copy_info(item, row)


def _show_items(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    show = _mapping(row.get("show"))
    ids, fldb = _split_ids(show.get("ids"))
    if not ids:
        return []
    title = show.get("title")
    year = as_int(show.get("year"))
    base: dict[str, Any] = {"_flicklist_fldb": fldb} if fldb else {}
    out: list[dict[str, Any]] = []
    if row.get("scope") == "show":
        out.append(_with_copy_info({"type": "show", "title": title, "year": year, "ids": dict(ids), **base}, row))
    for season in row.get("seasons") or []:
        if not isinstance(season, Mapping):
            continue
        season_no = as_int(season.get("number"))
        if season_no is None:
            continue
        if season.get("scope") == "season":
            item = {"type": "season", "title": title, "series_title": title, "year": year, "show_ids": dict(ids), "season": season_no, **base}
            out.append(_with_copy_info(item, season))
        for ep in season.get("episodes") or []:
            if not isinstance(ep, Mapping):
                continue
            ep_no = as_int(ep.get("number"))
            if ep_no is None:
                continue
            item = {"type": "episode", "title": ep.get("title"), "series_title": title, "year": year, "show_ids": dict(ids), "season": season_no, "episode": ep_no, **base}
            out.append(_with_copy_info(item, ep))
    return out


def _fetch(adapter: Any, url: str) -> list[Any]:
    resp = flicklist_request(adapter, "GET", url)
    if not ok_status(resp):
        _warn(FEATURE, "index_failed", url=url, status=int(resp.status_code), error=error_of(resp))
        raise FlickListError("flicklist_collection_index_failed")
    data = safe_json(resp)
    if not isinstance(data, list):
        _warn(FEATURE, "index_invalid_response", url=url, status=int(resp.status_code))
        raise FlickListError("flicklist_collection_index_invalid")
    return data


def build_index(adapter: Any, **_kwargs: Any) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    skipped = 0
    rows = [(_movie_item(row) if isinstance(row, Mapping) else None) for row in _fetch(adapter, URL_COLLECTION_MOVIES)]
    for row in _fetch(adapter, URL_COLLECTION_SHOWS):
        rows.extend((_show_items(row) if isinstance(row, Mapping) else []) or [None])
    for item in rows:
        key = key_of(item) if item else ""
        if not item or not key:
            skipped += 1
            continue
        out[key] = item
    _info(FEATURE, "index_done", count=len(out), skipped=skipped)
    return out


def _ident(item: Mapping[str, Any]) -> dict[str, Any] | None:
    typ = str(item.get("type") or "").strip().lower()
    if typ in {"movie", "show", "series", "tv", "season", "episode"}:
        return write_ident(item, seasons=True)
    return None


def _accepted(items: Iterable[Mapping[str, Any]]) -> tuple[list[tuple[str, dict[str, Any]]], list[str], list[dict[str, Any]]]:
    accepted: list[tuple[str, dict[str, Any]]] = []
    unresolved_keys: list[str] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = key_of(raw)
        ident = _ident(raw)
        if not key or ident is None:
            if key:
                unresolved_keys.append(key)
                unresolved.append({"key": key, "status": "missing_supported_id"})
            continue
        if key in seen:
            continue
        seen.add(key)
        payload = {**ident, **to_flicklist_fields(raw)}
        collected_at = None if raw.get("_cw_media_update") else not_future(iso_z(raw.get("collected_at")))
        if collected_at:
            payload["collected_at"] = collected_at
        accepted.append((key, payload))
    return accepted, unresolved_keys, unresolved


def _rejected_index(resp: Any, size: int) -> int | None:
    if int(getattr(resp, "status_code", 0) or 0) != 400:
        return None
    data = safe_json(resp)
    match = _ITEM_INDEX.search(str(data.get("detail") or "") if isinstance(data, Mapping) else "")
    if not match:
        return None
    idx = int(match.group(1))
    return idx if 0 <= idx < size else None


def _send(adapter: Any, method: str, rows: list[tuple[str, dict[str, Any]]], result: dict[str, Any]) -> None:
    sent = list(rows)
    while sent:
        resp = flicklist_request(adapter, method, URL_COLLECTION, json={"items": [payload for _key, payload in sent]})
        if ok_status(resp):
            body = safe_json(resp)
            if not has_write_counters(body, method):
                result["ok"] = False
                for key, _payload in sent:
                    result["unresolved_keys"].append(key)
                    result["unresolved"].append({"key": key, "status": "invalid_response", "error": "missing FlickList write counters"})
                _warn(FEATURE, "write_invalid_response", method=method, status=int(resp.status_code), count=len(sent))
                return
            yes, misses, miss_rows, counters = classify_write(sent, body, method=method)
            if misses:
                result["ok"] = False
                _warn(FEATURE, "write_partial", method=method, sent=len(sent), confirmed=len(yes), unresolved=len(misses), **counters)
            result["confirmed_keys"].extend(yes)
            result["unresolved_keys"].extend(misses)
            result["unresolved"].extend(miss_rows)
            return
        rejected = _rejected_index(resp, len(sent))
        result["ok"] = False
        if rejected is not None:
            key, _payload = sent.pop(rejected)
            result["unresolved_keys"].append(key)
            result["unresolved"].append({"key": key, "status": "http:400", "error": str((safe_json(resp) or {}).get("detail") or "")[:200]})
            _warn(FEATURE, "write_item_rejected", method=method, key=key, error=error_of(resp))
            continue
        for key, _payload in sent:
            result["unresolved_keys"].append(key)
            result["unresolved"].append({"key": key, "status": f"http:{int(resp.status_code)}", "error": error_of(resp)})
        _warn(FEATURE, "write_failed", method=method, status=int(resp.status_code), error=error_of(resp))
        return


def _new_result() -> dict[str, Any]:
    return {"ok": True, "confirmed_keys": [], "unresolved_keys": [], "unresolved": [], "skipped_keys": []}


def _finish(result: dict[str, Any], action: str, sent: int) -> dict[str, Any]:
    result["confirmed_keys"] = list(dict.fromkeys(result["confirmed_keys"]))
    result["count"] = len(result["confirmed_keys"])
    _info(FEATURE, "write_done", action=action, sent=sent, confirmed=result["count"], unresolved=len(result["unresolved_keys"]), skipped=len(result["skipped_keys"]))
    return result


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    accepted, unresolved_keys, unresolved = _accepted(items)
    result = _new_result()
    result["unresolved_keys"].extend(unresolved_keys)
    result["unresolved"].extend(unresolved)
    if unresolved_keys:
        result["ok"] = False
    for batch in chunk([{"key": k, "payload": p} for k, p in accepted], write_batch_size(adapter, FEATURE)):
        _send(adapter, "POST", [(str(row["key"]), row["payload"]) for row in batch], result)
    return _finish(result, "add", len(accepted))


def _tokens(item: Mapping[str, Any]) -> set[str]:
    typ = str(item.get("type") or "").strip().lower()
    season = as_int(item.get("season"))
    episode = as_int(item.get("episode"))
    if typ == "episode" and season is not None and episode is not None:
        frag = f"#s{season}e{episode}"
    elif typ == "season" and season is not None:
        frag = f"#s{season}"
    else:
        frag = f"#{'movie' if typ == 'movie' else 'show'}"
    ids = (_mapping(item.get("show_ids")) if frag.startswith("#s") else {}) or _mapping(item.get("ids"))
    out = {f"{k}:{str(ids[k]).strip().lower()}{frag}" for k in ("tmdb", "imdb", "tvdb") if str(ids.get(k) or "").strip()}
    fldb = str(item.get("_flicklist_fldb") or "").strip().lower()
    if fldb:
        out.add(f"fldb:{fldb}{frag}")
    return out


def _live_copies(adapter: Any) -> tuple[dict[str, list[dict[str, str]]], dict[str, str]]:
    by_key: dict[str, list[dict[str, str]]] = {}
    by_token: dict[str, str] = {}
    for key, item in build_index(adapter).items():
        by_key[key] = list(item.get("_flicklist_copies") or [])
        for token in _tokens(item):
            by_token.setdefault(token, key)
    return by_key, by_token


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    result = _new_result()
    rows = [dict(x) for x in items or [] if isinstance(x, Mapping)]
    if not rows:
        return _finish(result, "remove", 0)
    by_key, by_token = _live_copies(adapter)
    own = _own_source(adapter)
    sent: list[tuple[str, dict[str, Any]]] = []
    seen: set[str] = set()
    for raw in rows:
        key = key_of(raw)
        if not key or key in seen:
            continue
        seen.add(key)
        ident = _ident(raw)
        if ident is None:
            result["ok"] = False
            result["unresolved_keys"].append(key)
            result["unresolved"].append({"key": key, "status": "missing_supported_id"})
            continue
        live_key = key if key in by_key else next((by_token[t] for t in _tokens(raw) if t in by_token), "")
        if not live_key:
            result["confirmed_keys"].append(key)
            continue
        formats = list(dict.fromkeys(c["format"] for c in by_key[live_key] if c.get("source", "").lower() == own))
        if not formats:
            result["skipped_keys"].append(key)
            continue
        sent.extend((key, {**ident, "format": fmt}) for fmt in formats)
    for batch in chunk([{"key": k, "payload": p} for k, p in sent], write_batch_size(adapter, FEATURE)):
        _send(adapter, "DELETE", [(str(row["key"]), row["payload"]) for row in batch], result)
    if result["skipped_keys"]:
        _info(FEATURE, "remove_kept_foreign_copies", count=len(result["skipped_keys"]))
    return _finish(result, "remove", len(sent))

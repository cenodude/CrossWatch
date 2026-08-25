# /providers/sync/trakt/_collection.py
# TRAKT collection sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import (
    _chunk,
    _is_capture_mode,
    _pair_scope,
    build_watchlist_body,
    fetch_last_activities,
    headers_for_adapter,
    ids_for_trakt,
    key_of,
    normalize_watchlist_row,
    pick_trakt_kind,
    state_file,
    update_watermarks_from_last_activities,
)
from .._log import log as cw_log
from .._mod_common import request_with_retries

BASE = "https://api.trakt.tv"
URL_ADD = f"{BASE}/sync/collection"
URL_MEDIA = f"{BASE}/sync/collection/media"
URL_MOVIES = f"{BASE}/sync/collection/movies"
URL_SHOWS = f"{BASE}/sync/collection/shows"
URL_REMOVE = f"{BASE}/sync/collection/remove"

_PROVIDER = "TRAKT"
_FEATURE = "collection"
_SHADOW_SCHEMA = 3


def _dbg(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _shadow_path() -> Path:
    return state_file("trakt_collection.shadow.json")


def _cfg(adapter: Any) -> Mapping[str, Any]:
    c = getattr(adapter, "config", {}) or {}
    if isinstance(c, dict) and isinstance(c.get("trakt"), dict):
        return c["trakt"]
    return {}


def _cfg_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try:
        return int(d.get(key, default))
    except Exception:
        return default


def _cfg_bool(d: Mapping[str, Any], key: str, default: bool) -> bool:
    v = d.get(key, default)
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on"):
        return True
    if s in ("0", "false", "no", "off"):
        return False
    return default


def _shadow_load() -> dict[str, Any]:
    if _is_capture_mode() or _pair_scope() is None:
        return {"etag": None, "ts": 0, "items": {}}
    try:
        raw = json.loads(_shadow_path().read_text("utf-8"))
        if not isinstance(raw, Mapping) or int(raw.get("schema") or 0) != _SHADOW_SCHEMA:
            return {"etag": None, "ts": 0, "items": {}}
        return dict(raw)
    except Exception:
        return {"etag": None, "ts": 0, "items": {}}


def _shadow_save(etags: Mapping[str, str | None], items: Mapping[str, Any], bucket_items: Mapping[str, Mapping[str, Any]]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        path = _shadow_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(".tmp")
        tmp.write_text(
            json.dumps(
                {
                    "schema": _SHADOW_SCHEMA,
                    "etag": None,
                    "etags": dict(etags or {}),
                    "ts": int(time.time()),
                    "items": dict(items),
                    "bucket_items": {str(k): dict(v or {}) for k, v in dict(bucket_items or {}).items()},
                },
                ensure_ascii=False,
            ),
            "utf-8",
        )
        os.replace(tmp, path)
    except Exception:
        pass


def _shadow_bust() -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    try:
        path = _shadow_path()
        if path.exists():
            path.unlink()
            _dbg("cache_invalidated", cache="shadow", reason="write_applied")
    except Exception:
        pass


def _ids_clean(ids: Mapping[str, Any] | None) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in dict(ids or {}).items():
        if v is None:
            continue
        s = str(v).strip()
        if s:
            out[str(k)] = s
    return out


def _item_key(item: Mapping[str, Any]) -> str:
    return canonical_key(item) or key_of(item) or ""


def _hdr_int(headers: Mapping[str, Any], name: str) -> int | None:
    try:
        for k, v in (headers or {}).items():
            if str(k).lower() == name.lower():
                return int(str(v).strip())
    except Exception:
        return None
    return None


def _movie_from_row(row: Mapping[str, Any]) -> dict[str, Any] | None:
    movie = row.get("movie") if isinstance(row.get("movie"), Mapping) else row
    if not isinstance(movie, Mapping):
        return None
    item = id_minimal({"type": "movie", "title": movie.get("title"), "year": movie.get("year"), "ids": _ids_clean(movie.get("ids"))})
    collected_at = row.get("collected_at") or row.get("listed_at")
    if collected_at:
        item["collected_at"] = str(collected_at)
    return item


def _show_base(row: Mapping[str, Any]) -> dict[str, Any] | None:
    show = row.get("show") if isinstance(row.get("show"), Mapping) else row
    if not isinstance(show, Mapping):
        return None
    item = id_minimal({"type": "show", "title": show.get("title"), "year": show.get("year"), "ids": _ids_clean(show.get("ids"))})
    collected_at = row.get("collected_at") or row.get("listed_at")
    if collected_at:
        item["collected_at"] = str(collected_at)
    return item


def _items_from_collection_row(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    typ = str(row.get("type") or "").strip().lower()
    if typ == "movie":
        movie = _movie_from_row(row)
        return [movie] if movie else []
    if typ == "episode" and isinstance(row.get("episode"), Mapping):
        ep = row["episode"]
        show_raw = row.get("show")
        show: Mapping[str, Any] = show_raw if isinstance(show_raw, Mapping) else {}
        item = id_minimal(
            {
                "type": "episode",
                "title": ep.get("title"),
                "series_title": show.get("title"),
                "year": show.get("year"),
                "show_ids": _ids_clean(show.get("ids")),
                "season": ep.get("season"),
                "episode": ep.get("number"),
                "ids": _ids_clean(ep.get("ids")),
            }
        )
        collected_at = row.get("collected_at") or ep.get("collected_at")
        if collected_at:
            item["collected_at"] = str(collected_at)
        return [item]
    seasons = row.get("seasons")
    if not isinstance(seasons, list):
        show_raw = row.get("show")
        show: Mapping[str, Any] = show_raw if isinstance(show_raw, Mapping) else row
        seasons = show.get("seasons") if isinstance(show, Mapping) else None
    if typ and not (typ == "show" and isinstance(seasons, list) and seasons):
        return [normalize_watchlist_row(row)]
    if isinstance(row.get("movie"), Mapping):
        movie = _movie_from_row(row)
        return [movie] if movie else []
    show_item = _show_base(row)
    show_raw = row.get("show")
    show = show_raw if isinstance(show_raw, Mapping) else row
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
                item = id_minimal(
                    {
                        "type": "episode",
                        "title": ep.get("title"),
                        "series_title": title,
                        "year": year,
                        "show_ids": show_ids,
                        "season": season_no,
                        "episode": ep_no,
                        "ids": _ids_clean(ep.get("ids")),
                    }
                )
                if collected_at or ep.get("collected_at"):
                    item["collected_at"] = str(ep.get("collected_at") or collected_at)
                out.append(item)
        elif season_no is not None:
            item = id_minimal({"type": "season", "title": title, "series_title": title, "year": year, "show_ids": show_ids, "season": season_no, "ids": _ids_clean(season.get("ids"))})
            if collected_at:
                item["collected_at"] = str(collected_at)
            out.append(item)
    return out or ([show_item] if show_item else [])


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    use_etag = _cfg_bool(cfg, "collection_use_etag", True)
    ttl_h = _cfg_int(cfg, "collection_shadow_ttl_hours", 168)
    per_page = max(1, min(100, _cfg_int(cfg, "collection_per_page", _cfg_int(cfg, "history_per_page", 100))))
    max_pages = _cfg_int(cfg, "collection_max_pages", _cfg_int(cfg, "history_max_pages", 10000))
    if max_pages <= 0:
        max_pages = 10000
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("collection") if callable(prog_mk) else None

    sess = adapter.client.session
    headers = headers_for_adapter(adapter)
    try:
        acts = fetch_last_activities(sess, headers, timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries)
        update_watermarks_from_last_activities(acts)
    except Exception:
        pass

    shadow = _shadow_load()
    bucket_cache = shadow.get("bucket_items") if isinstance(shadow.get("bucket_items"), Mapping) else {}
    shadow_etags = shadow.get("etags") if isinstance(shadow.get("etags"), Mapping) else {}
    fresh = True
    if ttl_h > 0 and shadow.get("ts"):
        fresh = (int(time.time()) - int(shadow.get("ts") or 0)) <= ttl_h * 3600

    def _fetch_bucket(name: str, url: str) -> tuple[dict[str, dict[str, Any]], str | None, bool]:
        req_headers = dict(headers)
        cached = bucket_cache.get(name) if isinstance(bucket_cache, Mapping) else None
        etag = shadow_etags.get(name) if isinstance(shadow_etags, Mapping) else None
        if use_etag and fresh and etag and isinstance(cached, Mapping):
            req_headers["If-None-Match"] = str(etag)

        idx: dict[str, dict[str, Any]] = {}
        page = 1
        total_pages: int | None = None
        etag_out: str | None = None
        total_hint: int | None = None
        rows_seen = 0

        r = request_with_retries(
            sess,
            "GET",
            url,
            headers=req_headers,
            params={"page": page, "limit": per_page},
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )
        if r.status_code == 304 and use_etag and isinstance(cached, Mapping):
            return dict(cached), str(etag), True
        if r.status_code != 200:
            _warn("http_failed", op="index", bucket=name, status=r.status_code)
            return (dict(cached) if isinstance(cached, Mapping) else {}), (str(etag) if etag else None), True

        while True:
            if page > 1:
                r = request_with_retries(
                    sess,
                    "GET",
                    url,
                    headers=headers,
                    params={"page": page, "limit": per_page},
                    timeout=adapter.cfg.timeout,
                    max_retries=adapter.cfg.max_retries,
                )
                if r.status_code != 200:
                    _warn("http_failed", op="index", bucket=name, page=page, status=r.status_code)
                    break

            if page == 1:
                etag_out = r.headers.get("ETag")
                total_pages = _hdr_int(r.headers, "X-Pagination-Page-Count")
                total_hint = _hdr_int(r.headers, "X-Pagination-Item-Count")

            data = r.json() if (r.text or "").strip() else []
            rows: list[Mapping[str, Any]] = []
            if isinstance(data, list):
                rows = [x for x in data if isinstance(x, Mapping)]
            elif isinstance(data, Mapping):
                raw = data.get(name)
                if isinstance(raw, list):
                    rows = [x for x in raw if isinstance(x, Mapping)]
            if not rows:
                break

            for row in rows:
                for item in _items_from_collection_row(row):
                    key = _item_key(item)
                    if key:
                        idx[key] = item
            rows_seen += len(rows)
            if prog:
                prog.tick(rows_seen, total=total_hint or max(rows_seen, len(rows)))

            page += 1
            if total_pages is not None and page > total_pages:
                break
            if total_pages is None and len(rows) < per_page:
                break
            if max_pages and page > max_pages:
                _warn("index_reconcile", reason="safety_cap_hit", strategy="paged_fetch", bucket=name, max_pages=max_pages)
                break

        _dbg("index_fetch_bucket", bucket=name, rows=rows_seen, items=len(idx), per_page=per_page, max_pages=max_pages, pages=(page - 1))
        return idx, etag_out, False

    media_idx, media_etag, media_cached = _fetch_bucket("media", URL_MEDIA)
    if media_idx or not media_cached:
        if use_etag:
            _shadow_save({"media": media_etag}, media_idx, {"media": media_idx})
        source = "shadow" if media_cached else "live"
        _info("index_done", count=len(media_idx), source=source, endpoint="media", per_page=per_page, max_pages=max_pages)
        return media_idx

    movie_idx, movie_etag, movie_cached = _fetch_bucket("movies", URL_MOVIES)
    show_idx, show_etag, show_cached = _fetch_bucket("shows", URL_SHOWS)

    idx: dict[str, dict[str, Any]] = {}
    idx.update(movie_idx)
    idx.update(show_idx)

    if use_etag:
        _shadow_save({"movies": movie_etag, "shows": show_etag}, idx, {"movies": movie_idx, "shows": show_idx})
    source = "shadow" if movie_cached and show_cached else ("mixed" if movie_cached or show_cached else "live")
    _info("index_done", count=len(idx), source=source, per_page=per_page, max_pages=max_pages)
    return idx


def _batch_payload(items: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    for it in items or []:
        m = id_minimal(it)
        collected_at = it.get("collected_at")
        if collected_at:
            m["collected_at"] = str(collected_at)
            m["watched_at"] = str(collected_at)
        kind = pick_trakt_kind(m)
        ids = ids_for_trakt(m)
        show_ids = dict(m.get("show_ids") or {})
        season_no = m.get("season") if m.get("season") is not None else m.get("number")
        episode_no = m.get("episode") if m.get("episode") is not None else m.get("episode_number")
        if kind in ("movies", "shows") and ids:
            accepted.append(m)
        elif kind == "seasons" and (ids or (show_ids and season_no is not None)):
            accepted.append(m)
        elif kind == "episodes" and (ids or (show_ids and season_no is not None and episode_no is not None)):
            accepted.append(m)
        else:
            rejected.append({"item": m, "hint": "missing ids" if not show_ids else "missing scope"})
    return accepted, rejected


def _record_not_found(not_found: Mapping[str, Any], unresolved: list[dict[str, Any]]) -> list[str]:
    keys: list[str] = []
    if not isinstance(not_found, Mapping):
        return keys
    for bucket in ("movies", "shows", "seasons", "episodes"):
        raw = not_found.get(bucket)
        if not isinstance(raw, list):
            continue
        for obj in raw:
            if isinstance(obj, Mapping):
                typ = bucket[:-1] if bucket.endswith("s") else bucket
                item = id_minimal({"type": typ, "ids": dict(obj.get("ids") or obj)})
                key = _item_key(item)
                if key:
                    keys.append(key)
                unresolved.append({"item": item, "hint": "not_found", "key": key})
    return keys


def _write(adapter: Any, op: str, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    cfg = _cfg(adapter)
    batch = _cfg_int(cfg, "collection_batch_size", _cfg_int(cfg, "watchlist_batch_size", 100))
    accepted, unresolved = _batch_payload(items)
    if not accepted:
        _info("write_skipped", op=op, reason="empty_payload", unresolved=len(unresolved))
        return {"ok": len(unresolved) == 0, "count": 0, "confirmed": 0, "unresolved": unresolved, "confirmed_keys": [], "skipped_keys": []}

    url = URL_ADD if op == "add" else URL_REMOVE
    ok = 0
    skipped = 0
    confirmed_keys: list[str] = []
    skipped_keys: list[str] = []
    chunks: Iterable[list[dict[str, Any]]]
    if op == "add" and len(accepted) <= max(1, batch):
        chunks = ([x] for x in accepted)
    else:
        chunks = _chunk(accepted, batch)
    for sl in chunks:
        payload = build_watchlist_body(sl, date_field="watched_at" if op == "add" else None)
        if not payload:
            continue
        r = request_with_retries(adapter.client.session, "POST", url, headers=headers_for_adapter(adapter), json=payload, timeout=adapter.cfg.timeout, max_retries=adapter.cfg.max_retries)
        if r.status_code in (200, 201):
            body = r.json() if (r.text or "").strip() else {}
            result = body.get("added") if op == "add" else body.get("deleted") or body.get("removed")
            existing = body.get("existing") if op == "add" else {}
            result = result if isinstance(result, Mapping) else {}
            existing = existing if isinstance(existing, Mapping) else {}
            added_count = sum(int(result.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
            existing_count = sum(int(existing.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
            ok += added_count
            skipped += existing_count
            not_found_keys = set(_record_not_found(body.get("not_found") or {}, unresolved))
            slice_keys = [_item_key(x) for x in sl]
            slice_keys = [k for k in slice_keys if k and k not in not_found_keys]
            if added_count and not existing_count:
                confirmed_keys.extend(slice_keys)
            elif existing_count and not added_count:
                skipped_keys.extend(slice_keys)
        else:
            _warn("write_failed", op=op, status=r.status_code, body=(r.text or "")[:180])
            for x in sl:
                unresolved.append({"item": x, "hint": f"http:{r.status_code}"})
    if ok:
        _shadow_bust()
    _info("write_done", op=op, ok=len(unresolved) == 0, applied=ok, existing=skipped, unresolved=len(unresolved))
    return {
        "ok": len(unresolved) == 0,
        "count": ok,
        "confirmed": ok,
        "confirmed_keys": list(dict.fromkeys(confirmed_keys)),
        "skipped": skipped,
        "skipped_keys": list(dict.fromkeys(skipped_keys)),
        "unresolved": unresolved,
    }


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, "add", items)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, "remove", items)

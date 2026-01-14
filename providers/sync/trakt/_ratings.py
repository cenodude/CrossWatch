# /providers/sync/trakt/_ratings.py
# TRAKT Module for ratings sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from collections.abc import Iterable, Mapping
from typing import Any

from ._common import (
    build_headers,
    ids_for_trakt,
    key_of,
    normalize_watchlist_row,
    pick_trakt_kind,
    fetch_last_activities,
    update_watermarks_from_last_activities,
    state_file,
)
from cw_platform.id_map import minimal as id_minimal

BASE = "https://api.trakt.tv"
URL_RAT_MOV = f"{BASE}/sync/ratings/movies"
URL_RAT_SHO = f"{BASE}/sync/ratings/shows"
URL_RAT_SEA = f"{BASE}/sync/ratings/seasons"
URL_RAT_EPI = f"{BASE}/sync/ratings/episodes"
URL_UPSERT = f"{BASE}/sync/ratings"
URL_UNRATE = f"{BASE}/sync/ratings/remove"

def _cache_path() -> Path:
    return state_file("trakt_ratings.index.json")



_RETRYABLE_STATUS = {429, 500, 502, 503, 504}
_MAX_BACKOFF_SECONDS = 30


def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_TRAKT_DEBUG"):
        print(f"[TRAKT:ratings] {msg}")


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _valid_rating(v: Any) -> int | None:
    try:
        i = int(str(v).strip())
        return i if 1 <= i <= 10 else None
    except Exception:
        return None


def _sleep_backoff(attempt: int, retry_after: str | None) -> None:
    ra = (retry_after or "").strip()
    if ra.isdigit():
        time.sleep(min(int(ra), _MAX_BACKOFF_SECONDS))
        return
    delay = min(2 ** max(attempt, 0), _MAX_BACKOFF_SECONDS)
    time.sleep(delay)


def _chunk_iter(lst: list[dict[str, Any]], size: int) -> Iterable[list[dict[str, Any]]]:
    n = int(size or 0)
    if n <= 0:
        n = 100
    for i in range(0, len(lst), n):
        yield lst[i : i + n]


def _load_cache_doc() -> dict[str, Any]:
    try:
        p = _cache_path()
        if not p.exists():
            return {}
        return json.loads(p.read_text("utf-8") or "{}")
    except Exception:
        return {}


def _save_cache_doc(items: Mapping[str, Any], wm: Mapping[str, Any]) -> None:
    try:
        p = _cache_path()
        p.parent.mkdir(parents=True, exist_ok=True)
        doc = {"generated_at": _now_iso(), "items": dict(items), "wm": dict(wm or {})}
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
        _log(f"cache.saved -> {p} ({len(items)})")
    except Exception as e:
        _log(f"cache.save failed: {e}")


def _extract_ratings_wm(acts: Mapping[str, Any]) -> dict[str, str]:
    def g(k: str) -> str:
        v = acts.get(k) or {}
        return str(v.get("rated_at") or "")

    return {"movies": g("movies"), "shows": g("shows"), "seasons": g("seasons"), "episodes": g("episodes")}


def _sanitize_ids_for_trakt(kind: str, ids: Mapping[str, Any]) -> dict[str, Any]:
    allowed = ("trakt", "tmdb", "tvdb", "imdb") if kind == "episodes" else ("trakt", "tmdb", "tvdb") if kind == "seasons" else ("trakt", "imdb", "tmdb", "tvdb")
    out: dict[str, Any] = {}
    for k in allowed:
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out[k] = s if k == "imdb" else (int(s) if s.isdigit() else None)
    return {k: v for k, v in out.items() if v is not None}


def _merge_by_canonical(dst: dict[str, Any], src: Iterable[Mapping[str, Any]]) -> None:
    def q(x: Mapping[str, Any]) -> tuple[int, str]:
        ids = x.get("ids") or {}
        score = sum(1 for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k))
        return score, str(x.get("rated_at") or "")

    for m in src or []:
        k = key_of(m)
        cur = dst.get(k)
        if not cur or q(m) >= q(cur):
            dst[k] = dict(m)


def _accepted_minimal_for_cache(
    t: str,
    ids: Mapping[str, Any],
    src: Mapping[str, Any],
    *,
    rating: int | None = None,
    rated_at: str | None = None,
) -> dict[str, Any]:
    m: dict[str, Any] = {"type": t, "ids": dict(ids)}
    if rating is not None:
        m["rating"] = rating
    if rated_at:
        m["rated_at"] = rated_at

    keep_show_ids: dict[str, Any] | None = None
    show_ids = src.get("show_ids")
    if t in ("season", "episode") and isinstance(show_ids, Mapping) and show_ids:
        keep_show_ids = dict(show_ids)
        m["show_ids"] = keep_show_ids

    season = None
    if t in ("season", "episode"):
        season = src.get("season")
        if season is None:
            season = src.get("number")
        if season is not None:
            m["season"] = season

    ep = None
    if t == "episode":
        ep = src.get("episode")
        if ep is None:
            ep = src.get("number")
        if ep is not None:
            m["episode"] = ep

    res = id_minimal(m)
    if keep_show_ids:
        res["show_ids"] = keep_show_ids
    if season is not None and t in ("season", "episode"):
        res["season"] = season
    if ep is not None and t == "episode":
        res["episode"] = ep
    return res


def _fetch_bucket(
    sess: Any,
    headers: Mapping[str, Any],
    url: str,
    typ_hint: str,
    per_page: int,
    max_pages: int,
    tmo: float,
    rr: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        last_status: int | None = None
        for attempt in range(max(int(rr or 0), 0) + 1):
            try:
                r = sess.get(url, headers=headers, params={"page": page, "limit": per_page}, timeout=tmo)
                last_status = r.status_code
                if r.status_code == 200:
                    rows = r.json() or []
                    if not rows:
                        return out
                    for row in rows:
                        val = _valid_rating(row.get("rating"))
                        if not val:
                            continue
                        t = (row.get("type") or typ_hint).lower()
                        ra = row.get("rated_at") or row.get("user_rated_at")

                        if t == "movie" and isinstance(row.get("movie"), dict):
                            m = normalize_watchlist_row({"type": "movie", "movie": row["movie"]})
                        elif t == "show" and isinstance(row.get("show"), dict):
                            m = normalize_watchlist_row({"type": "show", "show": row["show"]})
                        elif t == "season" and isinstance(row.get("season"), dict):
                            se = row["season"]
                            show = row.get("show") or {}
                            show_ids = show.get("ids") or {}
                            season_no = se.get("number")
                            m = id_minimal(
                                {
                                    "type": "season",
                                    "ids": se.get("ids") or {},
                                    "show_ids": show_ids,
                                    "season": season_no,
                                    "series_title": show.get("title"),
                                    "title": show.get("title"),
                                }
                            )
                            if isinstance(show_ids, Mapping) and show_ids:
                                m["show_ids"] = dict(show_ids)
                            if season_no is not None:
                                m["season"] = season_no
                        elif t == "episode" and isinstance(row.get("episode"), dict):
                            ep = row["episode"]
                            show = row.get("show") or {}
                            show_ids = show.get("ids") or {}
                            season_no = ep.get("season")
                            ep_no = ep.get("number")
                            m = id_minimal(
                                {
                                    "type": "episode",
                                    "ids": ep.get("ids") or {},
                                    "show_ids": show_ids,
                                    "season": season_no,
                                    "episode": ep_no,
                                    "series_title": show.get("title"),
                                    "title": ep.get("title") or show.get("title"),
                                }
                            )
                            if isinstance(show_ids, Mapping) and show_ids:
                                m["show_ids"] = dict(show_ids)
                            if season_no is not None:
                                m["season"] = season_no
                            if ep_no is not None:
                                m["episode"] = ep_no
                        else:
                            continue

                        m["rating"] = val
                        if ra:
                            m["rated_at"] = ra
                        out.append(m)

                    if len(rows) < per_page:
                        return out
                    break

                if r.status_code in _RETRYABLE_STATUS and attempt < rr:
                    _log(f"GET {url} page={page} -> {r.status_code} (retry {attempt + 1}/{rr})")
                    _sleep_backoff(attempt, r.headers.get("Retry-After"))
                    continue

                _log(f"GET {url} page={page} failed -> {r.status_code}: {(r.text or '')[:200]}")
                return out

            except Exception as e:
                if attempt < rr:
                    _log(f"GET {url} page={page} error: {e} (retry {attempt + 1}/{rr})")
                    _sleep_backoff(attempt, None)
                    continue
                _log(f"GET {url} page={page} error: {e}")
                return out

        if last_status is not None and last_status != 200:
            return out

    return out


def _dedupe_canonical(items: Iterable[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    idx: dict[str, dict[str, Any]] = {}
    for m in items:
        k = key_of(m)
        cur = idx.get(k)
        if not cur:
            idx[k] = m
        else:
            if str(m.get("rated_at") or "") >= str(cur.get("rated_at") or ""):
                idx[k] = m
    return idx


def build_index(adapter: Any, *, per_page: int = 200, max_pages: int = 50) -> dict[str, dict[str, Any]]:
    per_page = int(getattr(adapter.cfg, "ratings_per_page", per_page) or per_page)
    max_pages = int(getattr(adapter.cfg, "ratings_max_pages", max_pages) or max_pages)

    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    tmo = adapter.cfg.timeout
    rr = int(getattr(adapter.cfg, "max_retries", 3) or 3)

    doc = _load_cache_doc()
    cached_items = dict(doc.get("items") or {})
    cached_wm = dict(doc.get("wm") or {})

    acts = fetch_last_activities(sess, headers, timeout=tmo, max_retries=rr)
    update_watermarks_from_last_activities(acts)
    wm_remote = _extract_ratings_wm(acts or {}) if acts else None

    if wm_remote and cached_items:
        for k in ("movies", "shows", "seasons", "episodes"):
            if str(wm_remote.get(k, "")) > str(cached_wm.get(k, "")):
                break
        else:
            _log(f"index (cache, activities unchanged): {len(cached_items)}")
            return cached_items
    elif cached_items and not wm_remote:
        _log(f"index (cache, activities unavailable): {len(cached_items)}")
        return cached_items

    movies = _fetch_bucket(sess, headers, URL_RAT_MOV, "movie", per_page, max_pages, tmo, rr)
    shows = _fetch_bucket(sess, headers, URL_RAT_SHO, "show", per_page, max_pages, tmo, rr)
    seasons = _fetch_bucket(sess, headers, URL_RAT_SEA, "season", per_page, max_pages, tmo, rr)
    episodes = _fetch_bucket(sess, headers, URL_RAT_EPI, "episode", per_page, max_pages, tmo, rr)

    all_items = movies + shows + seasons + episodes
    _log(f"fetched: {len(all_items)}")
    idx = _dedupe_canonical(all_items)
    _log(f"index size: {len(idx)} (m={len(movies)}, sh={len(shows)}, se={len(seasons)}, ep={len(episodes)})")

    _save_cache_doc(idx, wm_remote or cached_wm)
    return idx


def _bucketize_for_upsert(
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    body: dict[str, list[dict[str, Any]]] = {}
    accepted: list[dict[str, Any]] = []

    def push(bucket: str, obj: dict[str, Any]) -> None:
        body.setdefault(bucket, []).append(obj)

    for it in items or []:
        rating = _valid_rating(it.get("rating"))
        if rating is None:
            continue

        t_raw = str(it.get("type") or "").strip().lower()
        if t_raw not in {"movie", "show", "season", "episode", "series", "tv"}:
            continue
        kind = (pick_trakt_kind(it) or "").lower()
        if kind not in {"movies", "shows", "seasons", "episodes"}:
            continue

        ids = _sanitize_ids_for_trakt(kind, ids_for_trakt(it) or {})
        if not ids:
            continue

        obj: dict[str, Any] = {"ids": ids, "rating": rating}
        ra = it.get("rated_at")
        if ra:
            obj["rated_at"] = ra

        if kind == "movies":
            push("movies", obj)
            t = "movie"
        elif kind == "shows":
            push("shows", obj)
            t = "show"
        elif kind == "seasons":
            push("seasons", obj)
            t = "season"
        elif kind == "episodes":
            push("episodes", obj)
            t = "episode"
        else:
            continue

        accepted.append(_accepted_minimal_for_cache(t, ids, it, rating=rating, rated_at=str(ra) if ra else None))

    return body, accepted

def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    tmo = adapter.cfg.timeout

    body, accepted = _bucketize_for_upsert(items)
    if not body:
        return 0, []

    chunk = int(getattr(adapter.cfg, "ratings_chunk_size", 100) or 100)
    ok_total = 0
    unresolved: list[dict[str, Any]] = []

    for bucket in ("movies", "shows", "seasons", "episodes"):
        rows = body.get(bucket) or []
        for part in _chunk_iter(rows, chunk):
            payload = {bucket: part}
            r = sess.post(URL_UPSERT, headers=headers, json=payload, timeout=tmo)
            if r.status_code in (200, 201):
                d = r.json() or {}
                added = d.get("added") or {}
                updated = d.get("updated") or {}
                ok_total += sum(int(added.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
                ok_total += sum(int(updated.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
            else:
                _log(f"UPSERT failed {r.status_code}: {(r.text or '')[:200]}")

    if ok_total > 0:
        doc = _load_cache_doc()
        cache = dict(doc.get("items") or {})
        _merge_by_canonical(cache, accepted)
        _save_cache_doc(cache, doc.get("wm") or {})

    return ok_total, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    tmo = adapter.cfg.timeout

    buckets: dict[str, list[dict[str, Any]]] = {}
    accepted_minimals: list[dict[str, Any]] = []

    def push(bucket: str, obj: dict[str, Any]) -> None:
        buckets.setdefault(bucket, []).append(obj)

    for it in items or []:
        t_raw = str(it.get("type") or "").strip().lower()
        if t_raw not in {"movie", "show", "season", "episode", "series", "tv"}:
            continue
        kind = (pick_trakt_kind(it) or "").lower()
        if kind not in {"movies", "shows", "seasons", "episodes"}:
            continue

        ids = _sanitize_ids_for_trakt(kind, ids_for_trakt(it) or {})
        if not ids:
            continue
        if kind == "movies":
            push("movies", {"ids": ids})
            t = "movie"
        elif kind == "shows":
            push("shows", {"ids": ids})
            t = "show"
        elif kind == "seasons":
            push("seasons", {"ids": ids})
            t = "season"
        elif kind == "episodes":
            push("episodes", {"ids": ids})
            t = "episode"
        else:
            continue
        
        accepted_minimals.append(_accepted_minimal_for_cache(t, ids, it))

    if not buckets:
        return 0, []

    chunk = int(getattr(adapter.cfg, "ratings_chunk_size", 100) or 100)
    ok_total = 0
    unresolved: list[dict[str, Any]] = []

    for bucket in ("movies", "shows", "seasons", "episodes"):
        rows = buckets.get(bucket) or []
        for part in _chunk_iter(rows, chunk):
            payload = {bucket: part}
            r = sess.post(URL_UNRATE, headers=headers, json=payload, timeout=tmo)
            if r.status_code in (200, 201):
                d = r.json() or {}
                deleted = d.get("deleted") or d.get("removed") or {}
                ok_total += sum(int(deleted.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
            else:
                _log(f"UNRATE failed {r.status_code}: {(r.text or '')[:200]}")

    if ok_total > 0:
        doc = _load_cache_doc()
        cache = dict(doc.get("items") or {})
        for m in accepted_minimals:
            cache.pop(key_of(m), None)
        _save_cache_doc(cache, doc.get("wm") or {})

    return ok_total, unresolved

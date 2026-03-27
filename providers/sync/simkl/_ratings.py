# /providers/sync/simkl/_ratings.py
# SIMKL Module for ratings sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping, cast

from cw_platform.id_map import minimal as id_minimal

from .._log import log as cw_log
from ._common import (
    adapter_headers,
    extract_latest_ts,
    fetch_activities,
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
URL_ADD = f"{BASE}/sync/ratings"
URL_REMOVE = f"{BASE}/sync/ratings/remove"


def _unresolved_path() -> str:
    return str(state_file("simkl_ratings.unresolved.json"))


def _shadow_path() -> str:
    return str(state_file("simkl.ratings.shadow.json"))


ID_KEYS = ("tmdb", "imdb", "tvdb", "simkl", "trakt", "mal", "anilist", "kitsu", "anidb")


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



def _is_unknown_key(k: str) -> bool:
    return (k or "").startswith("unknown:")


def _log(msg: str, *, level: str = "debug", **fields: Any) -> None:
    cw_log("SIMKL", "ratings", level, msg, **fields)


def _dbg(event: str, **fields: Any) -> None:
    _log(event, level="debug", **fields)


def _info(event: str, **fields: Any) -> None:
    _log(event, level="info", **fields)


def _warn(event: str, **fields: Any) -> None:
    _log(event, level="warn", **fields)


def _headers(adapter: Any, *, force_refresh: bool = False) -> dict[str, str]:
    return adapter_headers(adapter, force_refresh=force_refresh)


def _norm_rating(v: Any) -> int | None:
    try:
        n = int(round(float(v)))
    except Exception:
        return None
    return n if 1 <= n <= 10 else None


def _now() -> int:
    return int(time.time())


def _as_epoch(v: Any) -> int | None:
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return int(v)
    if isinstance(v, datetime):
        return int((v if v.tzinfo else v.replace(tzinfo=timezone.utc)).timestamp())
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                n = int(s)
                return n // 1000 if len(s) >= 13 else n
            except Exception:
                return None
        try:
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        except Exception:
            return None
    return None


def _pick_rated_at(it: Mapping[str, Any]) -> str:
    ra = (it.get("rated_at") or it.get("ratedAt") or it.get("user_rated_at") or it.get("user_ratedAt") or "").strip()
    if ra:
        return ra
    inner = it.get("item") or {}
    if isinstance(inner, Mapping):
        ra = (inner.get("rated_at") or inner.get("ratedAt") or inner.get("user_rated_at") or inner.get("user_ratedAt") or "").strip()
        if ra:
            return ra
    return ""

def _as_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: str) -> dict[str, Any]:
    return load_json_state(path)


def _save_json(path: str, data: Mapping[str, Any]) -> None:
    save_json_state(path, data)


def _load_unresolved() -> dict[str, Any]:
    data = _load_json(_unresolved_path())
    if not isinstance(data, dict):
        return {}
    cleaned = False
    for k in list(data.keys()):
        if _is_unknown_key(k):
            data.pop(k, None)
            cleaned = True
    if cleaned:
        _save_json(_unresolved_path(), data)
    return data


def _save_unresolved(data: Mapping[str, Any]) -> None:
    _save_json(_unresolved_path(), data)


def _is_frozen(item: Mapping[str, Any]) -> bool:
    key = simkl_key_of(id_minimal(dict(item)))
    return key in _load_unresolved()


def _freeze(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    ids_sent: Mapping[str, Any],
    rating: int | None,
) -> None:
    key = simkl_key_of(id_minimal(dict(item)))
    if _is_unknown_key(key):
        return
    data = _load_unresolved()
    row = data.get(key) or {"feature": "ratings", "action": action, "first_seen": _now(), "attempts": 0}
    row.update({"item": id_minimal(dict(item)), "last_attempt": _now()})
    existing_reasons: list[str] = list(row.get("reasons", [])) if isinstance(row.get("reasons"), list) else []
    row["reasons"] = sorted(set(existing_reasons) | set(reasons or []))
    row["ids_sent"] = dict(ids_sent or {})
    if rating is not None:
        row["rating"] = int(rating)
    row["attempts"] = int(row.get("attempts", 0)) + 1
    data[key] = row
    _save_unresolved(data)


def _unfreeze_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for k in set(keys or []):
        if k in data:
            del data[k]
            changed = True
    if changed:
        _save_unresolved(data)


def _rshadow_load() -> dict[str, Any]:
    sh = _load_json(_shadow_path()) or {"items": {}}
    if not isinstance(sh, dict):
        sh = {"items": {}}
    store = sh.get("items")
    if not isinstance(store, dict):
        store = {}
    cleaned = False
    for k in list(store.keys()):
        if _is_unknown_key(k):
            store.pop(k, None)
            cleaned = True
    if cleaned:
        sh["items"] = store
        _save_json(_shadow_path(), sh)
    return sh


def _rshadow_save(obj: Mapping[str, Any]) -> None:
    _save_json(_shadow_path(), obj)


def _rshadow_items_from(items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    store: dict[str, Any] = {}
    now = _now()
    for it in items or []:
        mini = id_minimal(dict(it))
        bk = simkl_key_of(mini)
        if not bk or _is_unknown_key(bk):
            continue
        rt = _norm_rating(it.get("rating"))
        if rt is None:
            continue
        ra = _pick_rated_at(it)
        ts = _as_epoch(ra) or 0
        store[bk] = {
            "item": mini,
            "rating": rt,
            "rated_at": ra or (_as_iso(ts) if ts else _as_iso(now)),
        }
    return store


def _rshadow_put_all(items: Iterable[Mapping[str, Any]]) -> None:
    rows = list(items or [])
    if not rows:
        return
    sh = _rshadow_load()
    store: dict[str, Any] = dict(sh.get("items") or {})
    now = _now()
    for it in rows:
        mini = id_minimal(dict(it))
        bk = simkl_key_of(mini)
        if not bk or _is_unknown_key(bk):
            continue
        rt = _norm_rating(it.get("rating"))
        if rt is None:
            continue
        ra = _pick_rated_at(it)
        ts = _as_epoch(ra) or 0
        old = store.get(bk) or {}
        old_ra = (old.get("rated_at") or "").strip()
        old_ts = _as_epoch(old_ra) or 0
        if not ts and old_ts:
            store[bk] = {"item": mini, "rating": rt, "rated_at": old_ra}
            continue
        if not ts and not old_ts:
            store[bk] = {"item": mini, "rating": rt, "rated_at": _as_iso(now)}
            continue
        if ts >= old_ts:
            store[bk] = {"item": mini, "rating": rt, "rated_at": ra or _as_iso(ts)}
    sh["items"] = store
    _rshadow_save(sh)


def _rshadow_replace_all(items: Iterable[Mapping[str, Any]]) -> None:
    sh = _rshadow_load()
    sh["items"] = _rshadow_items_from(items)
    _rshadow_save(sh)



def _rshadow_merge_into(out: dict[str, dict[str, Any]], thaw: set[str]) -> None:
    sh = _rshadow_load()
    store: dict[str, Any] = dict(sh.get("items") or {})
    if not store:
        return
    changed = False
    merged = 0
    cleaned = 0
    for bk, rec_any in list(store.items()):
        if _is_unknown_key(bk):
            store.pop(bk, None)
            changed = True
            cleaned += 1
            continue
        rec = rec_any if isinstance(rec_any, Mapping) else {}
        rec_rt = _norm_rating(rec.get("rating"))
        rec_ra = rec.get("rated_at") or ""
        if rec_rt is None:
            store.pop(bk, None)
            changed = True
            cleaned += 1
            continue
        rec_ts = _as_epoch(rec_ra) or 0
        cur = out.get(bk)
        if not cur:
            item0 = rec.get("item") or {}
            base = id_minimal(dict(item0)) if isinstance(item0, Mapping) else {}
            m = dict(base)
            m["rating"] = rec_rt
            m["rated_at"] = rec_ra
            out[bk] = m
            thaw.add(bk)
            merged += 1
            continue
        cur_rt = _norm_rating(cur.get("rating"))
        cur_ts = _as_epoch(cur.get("rated_at")) or 0
        if (cur_rt == rec_rt) and (cur_ts >= rec_ts):
            store.pop(bk, None)
            changed = True
            cleaned += 1
            continue
        if rec_ts > cur_ts:
            m = dict(cur)
            m["rating"] = rec_rt
            m["rated_at"] = rec_ra
            out[bk] = m
            merged += 1
    if merged:
        _dbg("cache_merged", cache="shadow", count=merged)
    if cleaned or changed:
        sh["items"] = store
        _rshadow_save(sh)


def _shadow_has_anime_items() -> bool:
    sh = _rshadow_load()
    store = sh.get("items") or {}
    if not isinstance(store, Mapping):
        return False
    for rec_any in store.values():
        rec = rec_any if isinstance(rec_any, Mapping) else {}
        item = rec.get("item") if isinstance(rec.get("item"), Mapping) else {}
        if not isinstance(item, Mapping):
            continue
        if str(item.get("anime_type") or "").strip():
            return True
        ids_any = item.get("ids")
        ids: Mapping[str, Any] = ids_any if isinstance(ids_any, Mapping) else {}
        if any(ids.get(k) for k in ("anilist", "mal", "kitsu", "anidb")):
            return True
    return False


def _dedupe_prefer_plex_id(out: dict[str, dict[str, Any]]) -> None:
    if not out:
        return
    by_tvdb: dict[str, list[str]] = {}
    by_tmdb: dict[str, list[str]] = {}
    for key, row in out.items():
        ids = row.get("ids") or {}
        tvdb = str(ids.get("tvdb") or "").strip()
        tmdb = str(ids.get("tmdb") or "").strip()
        if tvdb:
            by_tvdb.setdefault(tvdb, []).append(key)
        if tmdb:
            by_tmdb.setdefault(tmdb, []).append(key)

    drop: set[str] = set()

    def pick(groups: dict[str, list[str]]) -> None:
        for _id, keys in groups.items():
            if len(keys) < 2:
                continue
            canonical: str | None = None
            for k in keys:
                ids = (out.get(k) or {}).get("ids") or {}
                if ids.get("plex") or ids.get("guid"):
                    canonical = k
                    break
            if canonical is None:
                canonical = keys[0]
            for k in keys:
                if k != canonical:
                    drop.add(k)

    pick(by_tvdb)
    pick(by_tmdb)

    for k in drop:
        out.pop(k, None)
    if drop:
        _dbg("index_reconcile", reason="dedupe_applied", strategy="prefer_plex_guid", count=len(drop))


def _row_ids(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids: dict[str, Any] = {}

    raw_ids = obj.get("ids")
    if isinstance(raw_ids, Mapping):
        for k in ID_KEYS:
            v = raw_ids.get(k)
            if v:
                ids[k] = v

    for k in ID_KEYS:
        v = obj.get(k)
        if v:
            ids.setdefault(k, v)

    simkl_id = obj.get("simkl_id") or obj.get("simklId")
    if simkl_id:
        ids.setdefault("simkl", simkl_id)

    for nested_key in ("movie", "show"):
        nested = obj.get(nested_key)
        if isinstance(nested, Mapping):
            nids = _row_ids(cast(Mapping[str, Any], nested))
            for k, v in nids.items():
                ids.setdefault(k, v)

    return {k: ids[k] for k in ID_KEYS if ids.get(k)}


_SLUG_ID_KEYS = ("tvdbslug", "trakttvslug", "traktmslug", "letterslug", "slug")

def _slug_to_title(slug: str) -> str:
    return slug_to_title(slug)


def _title_from_slug_ids(ids: Mapping[str, Any]) -> str:
    for k in _SLUG_ID_KEYS:
        v = ids.get(k)
        if isinstance(v, str) and v.strip():
            return _slug_to_title(v)
    return ""


def _title_from_slug_row(row: Mapping[str, Any]) -> str:
    for obj in (row, row.get("show"), row.get("anime"), row.get("movie")):
        if not isinstance(obj, Mapping):
            continue
        ids = obj.get("ids")
        if isinstance(ids, Mapping):
            title = _title_from_slug_ids(cast(Mapping[str, Any], ids))
            if title:
                return title
    return ""

def _title_year_from_row(row: Mapping[str, Any]) -> tuple[str, int | None]:
    t = row.get("title") or row.get("name") or row.get("en_title")
    title = t.strip() if isinstance(t, str) else ""
    y = row.get("year")
    year: int | None = None
    if isinstance(y, int):
        year = y
    elif isinstance(y, str) and y.strip().isdigit():
        year = int(y.strip())

    if (not title) or (year is None):
        for key in ("movie", "show", "anime"):
            nested = row.get(key)
            if isinstance(nested, Mapping):
                t2, y2 = _title_year_from_row(cast(Mapping[str, Any], nested))
                if (not title) and t2:
                    title = t2
                if (year is None) and (y2 is not None):
                    year = y2
    if not title:
        st = _title_from_slug_row(row)
        if st:
            title = st
    return title, year


def _media_from_row(kind: str, row: Mapping[str, Any]) -> Mapping[str, Any]:
    if kind == "movies":
        raw = row.get("movie")
        if isinstance(raw, Mapping):
            return {"movie": cast(Mapping[str, Any], raw)}
    if kind == "shows":
        raw = row.get("show")
        if isinstance(raw, Mapping):
            return {"show": cast(Mapping[str, Any], raw)}
    if kind == "anime":
        raw = row.get("show") or row.get("anime")
        if isinstance(raw, Mapping):
            return {"anime": cast(Mapping[str, Any], raw)}

    ids = _row_ids(row)
    title, year = _title_year_from_row(row)
    if ids or title or year:
        m: dict[str, Any] = {}
        if ids:
            m["ids"] = ids
        if title:
            m["title"] = title
        if year is not None:
            m["year"] = year
        return m
    return {}



def _merge_row_identity(m: dict[str, Any], row: Mapping[str, Any]) -> None:
    ids = dict(m.get("ids") or {})
    ids2 = _row_ids(row)
    for k, v in ids2.items():
        ids.setdefault(k, v)
    if ids:
        m["ids"] = {k: ids[k] for k in ID_KEYS if ids.get(k)}

    title_cur = m.get("title")
    if not (title_cur.strip() if isinstance(title_cur, str) else ""):
        title, _year = _title_year_from_row(row)
        if title:
            m["title"] = title

    if m.get("year") is None:
        _title, year = _title_year_from_row(row)
        if year is not None:
            m["year"] = year


def _resolve_by_simkl_id(
    sess: Any,
    hdrs: Mapping[str, str],
    *,
    kind: str,
    simkl_id: int,
    timeout: float,
) -> Mapping[str, Any]:
    client_id = str(hdrs.get("simkl-api-key") or "").strip()
    params: dict[str, str] = {"extended": "full"}
    if client_id:
        params["client_id"] = client_id

    k = str(kind or "").lower()
    if k == "movies":
        url = f"{BASE}/movies/{simkl_id}"
    elif k == "anime":
        url = f"{BASE}/anime/{simkl_id}"
    else:
        url = f"{BASE}/tv/{simkl_id}"

    try:
        resp = sess.get(url, headers=dict(hdrs), params=params, timeout=timeout)
        if resp.status_code != 200:
            return {}
        data = resp.json()
        return data if isinstance(data, Mapping) else {}
    except Exception:
        return {}


RATINGS_ALL = "1,2,3,4,5,6,7,8,9,10"


def _fetch_rows_current(
    sess: Any,
    hdrs: Mapping[str, str],
    *,
    kind: str,
    timeout: float,
) -> tuple[list[Mapping[str, Any]], bool]:
    if kind not in {"movies", "shows", "anime"}:
        return [], False
    url = f"{BASE}/sync/ratings/{kind}/{RATINGS_ALL}"
    try:
        resp = sess.post(url, headers=dict(hdrs), timeout=timeout)
        if resp.status_code != 200:
            _warn("http_failed", op="index", kind=kind, status=resp.status_code, body=(resp.text or "")[:200])
            return [], False
        data = resp.json()
        if not isinstance(data, Mapping):
            if isinstance(data, list):
                _dbg("index_reconcile", op="index", kind=kind, reason="non_mapping_bare_list", count=len(data))
                return [r for r in data if isinstance(r, Mapping)], True
            _dbg("http_failed", op="index", kind=kind, reason="non_mapping_response_assumed_empty")
            return [], True
        rows_any = data.get(kind)
        if not isinstance(rows_any, list):
            return [], True
        return [r for r in rows_any if isinstance(r, Mapping)], True
    except Exception as exc:
        _warn("http_failed", op="index", kind=kind, error=str(exc))
        return [], False


def _filter_rows_since(
    rows: Iterable[Mapping[str, Any]],
    *,
    since_iso: str | None,
) -> list[Mapping[str, Any]]:
    floor = _as_epoch(since_iso)
    if floor is None:
        return [dict(r) for r in rows if isinstance(r, Mapping)]
    out: list[Mapping[str, Any]] = []
    for row in rows or []:
        if not isinstance(row, Mapping):
            continue
        rated_at = row.get("user_rated_at") if "user_rated_at" in row else row.get("rated_at")
        ts = _as_epoch(rated_at)
        if ts is not None and ts < floor:
            continue
        out.append(dict(row))
    return out


def build_index(adapter: Any, *, since_iso: str | None = None) -> dict[str, dict[str, Any]]:
    sess = adapter.client.session
    tmo = adapter.cfg.timeout
    normalize_flat_watermarks()
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("ratings") if callable(prog_mk) else None

    out: dict[str, dict[str, Any]] = {}
    thaw: set[str] = set()
    shadow_has_data = bool((_rshadow_load().get("items") or {}))

    act_latest: str | None = None

    acts, _rate = fetch_activities(sess, _headers(adapter, force_refresh=True), timeout=tmo)
    if isinstance(acts, Mapping):
        wm = get_watermark("ratings") or ""
        lm = extract_latest_ts(acts, (("movies", "rated_at"),))
        ls = extract_latest_ts(acts, (("tv_shows", "rated_at"), ("shows", "rated_at")))
        la = extract_latest_ts(acts, (("anime", "rated_at"),))
        candidates = [x for x in (lm, ls, la) if x]
        act_latest = max(candidates) if candidates else None
        unchanged = bool(wm) and (lm is None or lm <= wm) and (ls is None or ls <= wm) and (la is None or la <= wm)
        if unchanged and shadow_has_data:
            _dbg("index_cache_hit", source="shadow", reason="activities_unchanged", movies=lm or "", shows=ls or "", anime=la or "")
            _rshadow_merge_into(out, thaw)
            _dedupe_prefer_plex_id(out)
            if prog:
                try:
                    prog.done(ok=True, total=len(out))
                except Exception:
                    pass
            _unfreeze_if_present(thaw)
            try:
                _rshadow_put_all(out.values())
            except Exception as exc:
                _warn("cache_save_failed", cache="shadow", op="index", source="shadow", error=str(exc))
            _info("index_done", count=len(out), source="shadow")
            return out

    hdrs = _headers(adapter, force_refresh=True)
    rows_movies_raw, ok_movies = _fetch_rows_current(sess, hdrs, kind="movies", timeout=tmo)
    rows_shows_raw, ok_shows = _fetch_rows_current(sess, hdrs, kind="shows", timeout=tmo)
    rows_anime_raw, ok_anime = _fetch_rows_current(sess, hdrs, kind="anime", timeout=tmo)
    if ok_movies and ok_shows and not ok_anime:
        if _shadow_has_anime_items():
            _warn("index_reconcile", reason="anime_bucket_unavailable_shadow_has_items", source="current")
        else:
            _dbg("index_reconcile", reason="anime_bucket_unavailable_assumed_empty", source="current")
        rows_anime_raw = []
        ok_anime = True
    fetch_ok = ok_movies and ok_shows and ok_anime

    if not fetch_ok and shadow_has_data:
        _warn("index_reconcile", reason="current_fetch_incomplete", source="shadow_fallback")
        _rshadow_merge_into(out, thaw)
        _dedupe_prefer_plex_id(out)
        if prog:
            try:
                prog.done(ok=True, total=len(out))
            except Exception:
                pass
        _info("index_done", count=len(out), source="shadow_fallback")
        return out

    rows_movies = _filter_rows_since(rows_movies_raw, since_iso=since_iso)
    rows_shows = _filter_rows_since(rows_shows_raw, since_iso=since_iso)
    rows_anime = _filter_rows_since(rows_anime_raw, since_iso=since_iso)

    grand_total = len(rows_movies) + len(rows_shows) + len(rows_anime)

    if prog:
        try:
            prog.tick(0, total=grand_total, force=True)
        except Exception:
            pass

    done = 0
    max_movies: int | None = None
    max_shows: int | None = None
    max_anime: int | None = None

    def _ingest(kind: str, rows: list[Mapping[str, Any]]) -> int | None:
        nonlocal done, max_movies, max_shows, max_anime
        latest: int | None = None

        for row in rows:
            rt = _norm_rating(row.get("user_rating") if "user_rating" in row else row.get("rating"))
            if rt is None:
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=grand_total)
                    except Exception:
                        pass
                continue

            media0 = _media_from_row(kind, row)
            m0 = simkl_normalize(cast(Mapping[str, Any], media0)) if media0 else {}
            m = dict(m0) if isinstance(m0, Mapping) else {}

            m["simkl_bucket"] = kind
            if kind == "movies":
                m["type"] = "movie"
            elif kind == "anime":
                raw = media0.get("anime") if isinstance(media0, Mapping) else None
                at = (raw.get("anime_type") or raw.get("animeType")) if isinstance(raw, Mapping) else None
                anime_type = at.strip().lower() if isinstance(at, str) and at.strip() else None
                m["type"] = "movie" if anime_type == "movie" else "show"
                if anime_type:
                    m["anime_type"] = anime_type
            else:
                m["type"] = "show"
            m["rating"] = rt
            m["rated_at"] = row.get("user_rated_at") or row.get("rated_at") or ""

            _merge_row_identity(m, row)

            ids_map = m.get("ids")
            if not (isinstance(ids_map, Mapping) and any(ids_map.get(k2) for k2 in ID_KEYS)):
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=grand_total)
                    except Exception:
                        pass
                continue

            if kind == "anime":
                st = _title_from_slug_row(row)
                if st:
                    cur0 = m.get("title")
                    src_title = ""
                    for obj2 in (row, row.get("show"), row.get("anime")):
                        if isinstance(obj2, Mapping):
                            v2 = obj2.get("title")
                            if isinstance(v2, str) and v2.strip():
                                src_title = v2.strip()
                                break
                    cur = cur0.strip() if isinstance(cur0, str) else ""
                    if (not cur) or (src_title and cur == src_title):
                        m["title"] = st

            title_cur = m.get("title")
            if not (title_cur.strip() if isinstance(title_cur, str) else ""):
                ids_hint = ""
                ids_map = m.get("ids")
                if isinstance(ids_map, Mapping):
                    for k2 in ("tmdb", "imdb", "tvdb", "simkl"):
                        v2 = ids_map.get(k2)
                        if v2:
                            ids_hint = f"{k2.upper()}:{v2}"
                            break
                m["title"] = ids_hint or "Unknown Title"

            k = simkl_key_of(m)

            if not k or _is_unknown_key(k):
                done += 1
                if prog:
                    try:
                        prog.tick(done, total=grand_total)
                    except Exception:
                        pass
                continue

            out[k] = m
            thaw.add(k)

            ts = _as_epoch(m.get("rated_at"))
            if ts is not None:
                latest = max(latest or 0, ts)

            done += 1
            if prog:
                try:
                    prog.tick(done, total=grand_total)
                except Exception:
                    pass

        if kind == "movies":
            max_movies = latest
        elif kind == "anime":
            max_anime = latest
        else:
            max_shows = latest
        return latest

    _ingest("movies", rows_movies)
    _ingest("shows", rows_shows)
    _ingest("anime", rows_anime)

    _dedupe_prefer_plex_id(out)

    if prog:
        try:
            prog.done(ok=True, total=grand_total)
        except Exception:
            pass

    _dbg("index_fetch_counts", movies=len(rows_movies), shows=len(rows_shows), anime=len(rows_anime), source="current")

    if act_latest:
        update_watermark_if_new("ratings", act_latest)

    latest_any = max([t for t in (max_movies, max_shows, max_anime) if isinstance(t, int)], default=None)
    if latest_any is not None:
        update_watermark_if_new("ratings", _as_iso(latest_any))

    _unfreeze_if_present(thaw)
    try:
        _rshadow_replace_all(out.values())
    except Exception as exc:
        _warn("cache_save_failed", cache="shadow", op="index", source="live", error=str(exc))

    _info("index_done", count=len(out), source="current")
    return out


def _ids_of(it: Mapping[str, Any]) -> dict[str, Any]:
    src = dict(it.get("ids") or {})
    return {k: src[k] for k in ID_KEYS if src.get(k)}


def _show_ids_of_episode(it: Mapping[str, Any]) -> dict[str, Any]:
    sids = dict(it.get("show_ids") or {})
    return {k: sids[k] for k in ID_KEYS if sids.get(k)}


def _movie_entry_add(it: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(it)
    rating = _norm_rating(it.get("rating"))
    if not ids or rating is None:
        return None
    ent: dict[str, Any] = {"ids": ids, "rating": rating}
    ra = _pick_rated_at(it)
    if ra:
        ent["rated_at"] = ra
    return ent


def _show_entry_add(adapter: Any, it: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(it)
    rating = _norm_rating(it.get("rating"))
    if not ids or rating is None:
        return None
    ids = _maybe_map_tvdb(adapter, ids)
    ent: dict[str, Any] = {"ids": ids, "rating": rating}
    ra = _pick_rated_at(it)
    if ra:
        ent["rated_at"] = ra
    return ent


def _write_group(item: Mapping[str, Any]) -> str:
    bucket = str(item.get("simkl_bucket") or "").strip().lower()
    if bucket == "movies":
        return "movies"
    if bucket in ("shows", "anime"):
        return "shows"
    typ = str(item.get("type") or "").strip().lower()
    return "movies" if typ == "movie" else "shows"


def _chunk_items(seq: list[Mapping[str, Any]], n: int) -> Iterable[list[Mapping[str, Any]]]:
    size = max(1, int(n or 1))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    hdrs = _headers(adapter)
    items_list: list[Mapping[str, Any]] = list(items or [])
    if not items_list:
        _info("write_skipped", op="add", reason="empty_payload", unresolved=0)
        return 0, []

    chunk_size = max(1, int(getattr(adapter.cfg, "ratings_chunk_size", 100) or 100))
    ok = 0
    unresolved: list[dict[str, Any]] = []
    for part in _chunk_items(items_list, chunk_size):
        movies: list[dict[str, Any]] = []
        shows: list[dict[str, Any]] = []
        thaw_keys: list[str] = []
        rshadow_events: list[dict[str, Any]] = []
        attempted: list[Mapping[str, Any]] = []

        for it in part:
            mini = id_minimal(dict(it))
            key = simkl_key_of(mini)
            if _is_unknown_key(key):
                unresolved.append({"item": mini, "hint": "missing_identity"})
                continue

            typ = str(it.get("type") or "").strip().lower()
            group = _write_group(it)
            if typ not in {"movie", "show", "season", "episode"}:
                unresolved.append({"item": mini, "hint": "missing_or_invalid_type"})
                continue

            if _is_frozen(it):
                continue

            if typ == "movie" and group == "movies":
                ent = _movie_entry_add(it)
                if ent:
                    movies.append(ent)
                    thaw_keys.append(key)
                    attempted.append(it)
                    ev = dict(mini)
                    ev["rating"] = ent["rating"]
                    ev["rated_at"] = ent.get("rated_at", "")
                    rshadow_events.append(ev)
                else:
                    unresolved.append({"item": mini, "hint": "missing_ids_or_rating"})
                continue

            if typ == "movie" and group == "shows":
                ent = _show_entry_add(adapter, it)
                if ent:
                    shows.append(ent)
                    thaw_keys.append(key)
                    attempted.append(it)
                    ev = dict(mini)
                    ev["rating"] = ent["rating"]
                    ev["rated_at"] = ent.get("rated_at", "")
                    rshadow_events.append(ev)
                else:
                    unresolved.append({"item": mini, "hint": "missing_ids_or_rating"})
                continue

            if typ in ("episode", "season"):
                unresolved.append({"item": mini, "hint": "unsupported_type"})
                continue

            ent = _show_entry_add(adapter, it)
            if ent:
                shows.append(ent)
                thaw_keys.append(key)
                attempted.append(it)
                ev = dict(mini)
                ev["rating"] = ent["rating"]
                ev["rated_at"] = ent.get("rated_at", "")
                rshadow_events.append(ev)
            else:
                unresolved.append({"item": mini, "hint": "missing_ids_or_rating"})

        if not (movies or shows):
            continue

        body: dict[str, Any] = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows

        try:
            resp = sess.post(URL_ADD, headers=hdrs, json=body, timeout=adapter.cfg.timeout)
            if 200 <= resp.status_code < 300:
                _unfreeze_if_present(thaw_keys)
                ok += len(movies) + len(shows)
                try:
                    _rshadow_put_all(rshadow_events)
                except Exception as exc:
                    _warn("cache_save_failed", cache="shadow", op="add", error=str(exc))
            else:
                _warn("write_failed", op="add", status=resp.status_code, body=(resp.text or '')[:180])
                for it in attempted:
                    ids = _maybe_map_tvdb(adapter, _ids_of(it))
                    rating = _norm_rating(it.get("rating"))
                    if ids and rating is not None:
                        _freeze(it, action="add", reasons=["write_failed"], ids_sent=ids, rating=rating)
        except Exception as exc:
            _warn("write_failed", op="add", error=str(exc))
            for it in attempted:
                ids = _maybe_map_tvdb(adapter, _ids_of(it))
                rating = _norm_rating(it.get("rating"))
                if ids and rating is not None:
                    _freeze(it, action="add", reasons=["write_failed"], ids_sent=ids, rating=rating)

    _info("write_done", op="add", ok=bool(items_list) and len(unresolved) == 0 and ok == len(items_list), applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    hdrs = _headers(adapter)
    items_list: list[Mapping[str, Any]] = list(items or [])
    if not items_list:
        _info("write_skipped", op="remove", reason="empty_payload", unresolved=0)
        return 0, []

    chunk_size = max(1, int(getattr(adapter.cfg, "ratings_chunk_size", 100) or 100))
    ok = 0
    unresolved: list[dict[str, Any]] = []
    for part in _chunk_items(items_list, chunk_size):
        movies: list[dict[str, Any]] = []
        shows: list[dict[str, Any]] = []
        thaw_keys: list[str] = []
        attempted: list[Mapping[str, Any]] = []

        for it in part:
            mini = id_minimal(dict(it))
            key = simkl_key_of(mini)
            if _is_unknown_key(key):
                unresolved.append({"item": mini, "hint": "missing_identity"})
                continue

            if _is_frozen(it):
                continue

            ids = _ids_of(it) or _show_ids_of_episode(it)
            ids = _maybe_map_tvdb(adapter, ids)
            if not ids:
                unresolved.append({"item": mini, "hint": "missing_ids"})
                continue

            typ = str(it.get("type") or "").strip().lower()
            group = _write_group(it)
            if typ not in {"movie", "show", "season", "episode"}:
                unresolved.append({"item": mini, "hint": "missing_or_invalid_type"})
                continue

            if typ == "movie" and group == "movies":
                movies.append({"ids": ids})
            elif typ == "movie" and group == "shows":
                shows.append({"ids": ids})
            elif typ in ("episode", "season"):
                unresolved.append({"item": mini, "hint": "unsupported_type"})
                continue
            else:
                shows.append({"ids": ids})

            thaw_keys.append(key)
            attempted.append(it)

        if not (movies or shows):
            continue

        body: dict[str, Any] = {}
        if movies:
            body["movies"] = movies
        if shows:
            body["shows"] = shows

        try:
            resp = sess.post(URL_REMOVE, headers=hdrs, json=body, timeout=adapter.cfg.timeout)
            if 200 <= resp.status_code < 300:
                _unfreeze_if_present(thaw_keys)
                try:
                    sh = _rshadow_load()
                    store: dict[str, Any] = dict(sh.get("items") or {})
                    changed = False
                    for k in thaw_keys:
                        if k in store:
                            store.pop(k, None)
                            changed = True
                    if changed:
                        sh["items"] = store
                        _rshadow_save(sh)
                except Exception:
                    pass
                ok += len(movies) + len(shows)
                continue
            _warn("write_failed", op="remove", status=resp.status_code, body=(resp.text or '')[:180])
        except Exception as exc:
            _warn("write_failed", op="remove", error=str(exc))

        for it in attempted:
            ids = _ids_of(it) or _show_ids_of_episode(it)
            ids = _maybe_map_tvdb(adapter, ids)
            if ids:
                _freeze(it, action="remove", reasons=["write_failed"], ids_sent=ids, rating=None)

    _info("write_done", op="remove", ok=bool(items_list) and len(unresolved) == 0 and ok == len(items_list), applied=ok, unresolved=len(unresolved))
    return ok, unresolved

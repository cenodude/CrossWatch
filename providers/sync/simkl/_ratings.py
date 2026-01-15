# /providers/sync/simkl/_ratings.py
# SIMKL Module for ratings sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, cast

from cw_platform.id_map import minimal as id_minimal

from ._common import (
    build_headers,
    coalesce_date_from,
    extract_latest_ts,
    fetch_activities,
    get_watermark,
    normalize_flat_watermarks,
    key_of as simkl_key_of,
    normalize as simkl_normalize,
    update_watermark_if_new,
    state_file,
    _pair_scope,
)

BASE = "https://api.simkl.com"
URL_ADD = f"{BASE}/sync/ratings"
URL_REMOVE = f"{BASE}/sync/ratings/remove"
URL_SEARCH_ID = f"{BASE}/search/id"


def _unresolved_path() -> str:
    return str(state_file("simkl_ratings.unresolved.json"))


def _shadow_path() -> str:
    return str(state_file("simkl.ratings.shadow.json"))


ID_KEYS = ("imdb", "tmdb", "tvdb", "simkl")


def _is_unknown_key(k: str) -> bool:
    return (k or "").startswith("unknown:")


def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_SIMKL_DEBUG"):
        print(f"[SIMKL:ratings] {msg}")


def _headers(adapter: Any, *, force_refresh: bool = False) -> dict[str, str]:
    return build_headers(
        {"simkl": {"api_key": adapter.cfg.api_key, "access_token": adapter.cfg.access_token}},
        force_refresh=force_refresh,
    )


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


def _as_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")


def _legacy_path(path: Path) -> Path | None:
    parts = path.stem.split(".")
    if len(parts) < 2:
        return None
    legacy_name = ".".join(parts[:-1]) + path.suffix
    legacy = path.with_name(legacy_name)
    return None if legacy == path else legacy


def _migrate_legacy_json(path: Path) -> None:
    if path.exists():
        return
    if _pair_scope() is None:
        return
    legacy = _legacy_path(path)
    if not legacy or not legacy.exists():
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(f"{path.name}.tmp")
        tmp.write_bytes(legacy.read_bytes())
        os.replace(tmp, path)
    except Exception:
        pass


def _load_json(path: str) -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    p = Path(path)
    _migrate_legacy_json(p)
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {}


def _save_json(path: str, data: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        p = Path(path)
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as exc:
        _log(f"save {Path(path).name} failed: {exc}")


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
        ra = it.get("rated_at") or it.get("ratedAt") or ""
        ts = _as_epoch(ra) or now
        if rt is None:
            continue
        old = store.get(bk) or {}
        old_ts = _as_epoch(old.get("rated_at")) or 0
        if ts >= old_ts:
            store[bk] = {"item": mini, "rating": rt, "rated_at": ra or _as_iso(ts)}
    sh["items"] = store
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
        _log(f"shadow merged {merged} rating items")
    if cleaned or changed:
        sh["items"] = store
        _rshadow_save(sh)


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
        _log(f"deduped {len(drop)} rating ids (prefer plex/guid)")


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


def _title_year_from_row(row: Mapping[str, Any]) -> tuple[str, int | None]:
    t = row.get("title") or row.get("name") or row.get("en_title")
    title = t.strip() if isinstance(t, str) else ""
    y = row.get("year")
    year: int | None = None
    if isinstance(y, int):
        year = y
    elif isinstance(y, str) and y.strip().isdigit():
        year = int(y.strip())
    return title, year


def _media_from_row(kind: str, row: Mapping[str, Any]) -> Mapping[str, Any]:
    if kind == "movies":
        raw = row.get("movie")
        if isinstance(raw, Mapping):
            return cast(Mapping[str, Any], raw)
    if kind in ("shows", "anime"):
        raw = row.get("show") or row.get("anime")
        if isinstance(raw, Mapping):
            return cast(Mapping[str, Any], raw)

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
    """Resolve a Simkl ID into a full media object per API blueprint."""
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


def _fetch_rows_any(
    sess: Any,
    hdrs: Mapping[str, str],
    *,
    kind: str,
    df_iso: str,
    timeout: float,
) -> list[Mapping[str, Any]]:
    if kind not in {"movies", "shows", "anime"}:
        return []
    url = f"{BASE}/sync/ratings/{kind}/{RATINGS_ALL}?date_from={df_iso}"
    try:
        resp = sess.post(url, headers=dict(hdrs), timeout=timeout)
        if resp.status_code != 200:
            return []
        data = resp.json()
        if not isinstance(data, Mapping):
            return []
        rows_any = data.get(kind)
        if not isinstance(rows_any, list) or not rows_any:
            return []
        return [r for r in rows_any if isinstance(r, Mapping)]
    except Exception:
        return []


def _search_id_enrich(
    sess: Any,
    hdrs: Mapping[str, str],
    *,
    ids: Mapping[str, Any],
    timeout: float,
    cache: dict[str, tuple[str, int | None]],
) -> tuple[str, int | None]:
    imdb = str(ids.get("imdb") or "").strip()
    tmdb = str(ids.get("tmdb") or "").strip()
    tvdb = str(ids.get("tvdb") or "").strip()
    simkl = str(ids.get("simkl") or "").strip()

    cache_key = ""
    params: dict[str, str] = {}
    if imdb:
        cache_key = f"imdb:{imdb}"
        params["imdb"] = imdb
    elif tmdb:
        cache_key = f"tmdb:{tmdb}"
        params["tmdb"] = tmdb
    elif tvdb:
        cache_key = f"tvdb:{tvdb}"
        params["tvdb"] = tvdb
    elif simkl:
        cache_key = f"simkl:{simkl}"
        params["simkl"] = simkl

    if not cache_key:
        return "", None
    if cache_key in cache:
        return cache[cache_key]

    try:
        resp = sess.get(URL_SEARCH_ID, headers=dict(hdrs), params=params, timeout=timeout)
        if resp.status_code != 200:
            cache[cache_key] = ("", None)
            return "", None
        data = resp.json()
    except Exception:
        cache[cache_key] = ("", None)
        return "", None

    obj: Mapping[str, Any] | None = None
    if isinstance(data, list) and data:
        first = data[0]
        obj = first if isinstance(first, Mapping) else None
    elif isinstance(data, Mapping):
        obj = data

    if not obj:
        cache[cache_key] = ("", None)
        return "", None

    title, year = _title_year_from_row(obj)
    cache[cache_key] = (title, year)
    return title, year


def build_index(adapter: Any, *, since_iso: str | None = None) -> dict[str, dict[str, Any]]:
    sess = adapter.client.session
    tmo = adapter.cfg.timeout
    normalize_flat_watermarks()
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("ratings") if callable(prog_mk) else None

    out: dict[str, dict[str, Any]] = {}
    thaw: set[str] = set()

    if since_iso is None:
        acts, _rate = fetch_activities(sess, _headers(adapter, force_refresh=True), timeout=tmo)
        if isinstance(acts, Mapping):
            wm = get_watermark("ratings") or ""
            lm = extract_latest_ts(acts, (("movies", "rated_at"),))
            ls = extract_latest_ts(acts, (("tv_shows", "rated_at"), ("shows", "rated_at")))
            la = extract_latest_ts(acts, (("anime", "rated_at"),))
            unchanged = (lm is None or lm <= wm) and (ls is None or ls <= wm) and (la is None or la <= wm)
            if unchanged:
                _log(f"activities unchanged; ratings from shadow (m={lm} s={ls} a={la})")
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
                    _log(f"shadow.put index skipped: {exc}")
                _log(f"index size: {len(out)} (shadow)")
                return out

    hdrs = _headers(adapter, force_refresh=True)

    df_all = coalesce_date_from("ratings", cfg_date_from=since_iso)

    rows_movies = _fetch_rows_any(sess, hdrs, kind="movies", df_iso=df_all, timeout=tmo)
    rows_shows = _fetch_rows_any(sess, hdrs, kind="shows", df_iso=df_all, timeout=tmo)
    rows_anime = _fetch_rows_any(sess, hdrs, kind="anime", df_iso=df_all, timeout=tmo)

    grand_total = len(rows_movies) + len(rows_shows) + len(rows_anime)

    if prog:
        try:
            prog.tick(0, total=grand_total, force=True)
        except Exception:
            pass

    resolved_cache: dict[tuple[str, int], Mapping[str, Any]] = {}
    search_cache: dict[str, tuple[str, int | None]] = {}
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

            if kind == "movies":
                m["type"] = "movie"
            elif kind == "anime":
                m["type"] = "anime"
            else:
                m["type"] = "show"
            m["rating"] = rt
            m["rated_at"] = row.get("user_rated_at") or row.get("rated_at") or ""

            _merge_row_identity(m, row)

            ids_map = m.get("ids")
            title_cur = m.get("title")
            title_ok = bool(title_cur.strip() if isinstance(title_cur, str) else "")
            if (not title_ok or m.get("year") is None) and isinstance(ids_map, Mapping):
                t2, y2 = _search_id_enrich(
                    sess,
                    hdrs,
                    ids=cast(Mapping[str, Any], ids_map),
                    timeout=tmo,
                    cache=search_cache,
                )
                if t2 and not title_ok:
                    m["title"] = t2
                if y2 is not None and m.get("year") is None:
                    m["year"] = y2

            k = simkl_key_of(m)

            if (not k or _is_unknown_key(k)) and isinstance(m.get("ids"), Mapping):
                simkl_raw = cast(Mapping[str, Any], m.get("ids")).get("simkl")
                try:
                    simkl_id = int(simkl_raw) if simkl_raw is not None else 0
                except Exception:
                    simkl_id = 0
                if simkl_id:
                    ck = (kind, simkl_id)
                    meta = resolved_cache.get(ck)
                    if meta is None:
                        meta = _resolve_by_simkl_id(sess, hdrs, kind=kind, simkl_id=simkl_id, timeout=tmo)
                        resolved_cache[ck] = meta
                    if meta:
                        mm0 = simkl_normalize(cast(Mapping[str, Any], meta))
                        mm = dict(mm0) if isinstance(mm0, Mapping) else {}
                        mm["type"] = m["type"]
                        mm["rating"] = rt
                        mm["rated_at"] = m.get("rated_at", "")
                        _merge_row_identity(mm, row)
                        m = mm
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

    _rshadow_merge_into(out, thaw)
    _dedupe_prefer_plex_id(out)

    if prog:
        try:
            prog.done(ok=True, total=grand_total)
        except Exception:
            pass

    _log(f"counts movies={len(rows_movies)} shows={len(rows_shows)} anime={len(rows_anime)} from={df_all}")

    latest_any = max([t for t in (max_movies, max_shows, max_anime) if isinstance(t, int)], default=None)
    if latest_any is not None:
        update_watermark_if_new("ratings", _as_iso(latest_any))

    _unfreeze_if_present(thaw)
    try:
        _rshadow_put_all(out.values())
    except Exception as exc:
        _log(f"shadow.put index skipped: {exc}")

    _log(f"index size: {len(out)}")
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
    ra = (it.get("rated_at") or it.get("ratedAt") or "").strip()
    if ra:
        ent["rated_at"] = ra
    return ent


def _show_entry_add(it: Mapping[str, Any]) -> dict[str, Any] | None:
    ids = _ids_of(it)
    rating = _norm_rating(it.get("rating"))
    if not ids or rating is None:
        return None
    ent: dict[str, Any] = {"ids": ids, "rating": rating}
    ra = (it.get("rated_at") or it.get("ratedAt") or "").strip()
    if ra:
        ent["rated_at"] = ra
    return ent


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    hdrs = _headers(adapter)

    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    rshadow_events: list[dict[str, Any]] = []
    attempted: list[Mapping[str, Any]] = []

    for it in items or []:
        mini = id_minimal(dict(it))
        key = simkl_key_of(mini)
        if _is_unknown_key(key):
            unresolved.append({"item": mini, "hint": "missing_identity"})
            continue

        typ = str(it.get("type") or "").strip().lower()
        if typ not in {"movie", "show", "season", "episode"}:
            unresolved.append({"item": mini, "hint": "missing_or_invalid_type"})
            continue

        if _is_frozen(it):
            continue

        if typ == "movie":
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

        if typ in ("episode", "season"):
            unresolved.append({"item": mini, "hint": "unsupported_type"})
            continue

        ent = _show_entry_add(it)
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
        return 0, unresolved

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if shows:
        body["shows"] = shows

    try:
        resp = sess.post(URL_ADD, headers=hdrs, json=body, timeout=adapter.cfg.timeout)
        if 200 <= resp.status_code < 300:
            _unfreeze_if_present(thaw_keys)
            ok = len(movies) + len(shows)
            try:
                _rshadow_put_all(rshadow_events)
            except Exception:
                pass
            _log(f"add done: +{ok}")
            return ok, unresolved
        _log(f"ADD failed {resp.status_code}: {(resp.text or '')[:180]}")
    except Exception as exc:
        _log(f"ADD error: {exc}")

    for it in attempted:
        ids = _ids_of(it)
        rating = _norm_rating(it.get("rating"))
        if ids and rating is not None:
            _freeze(it, action="add", reasons=["write_failed"], ids_sent=ids, rating=rating)

    return 0, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    sess = adapter.client.session
    hdrs = _headers(adapter)

    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    thaw_keys: list[str] = []
    attempted: list[Mapping[str, Any]] = []

    for it in items or []:
        mini = id_minimal(dict(it))
        key = simkl_key_of(mini)
        if _is_unknown_key(key):
            unresolved.append({"item": mini, "hint": "missing_identity"})
            continue

        if _is_frozen(it):
            continue

        ids = _ids_of(it) or _show_ids_of_episode(it)
        if not ids:
            unresolved.append({"item": mini, "hint": "missing_ids"})
            continue

        typ = str(it.get("type") or "").strip().lower()
        if typ not in {"movie", "show", "season", "episode"}:
            unresolved.append({"item": mini, "hint": "missing_or_invalid_type"})
            continue

        if typ == "movie":
            movies.append({"ids": ids})
        elif typ in ("episode", "season"):
            unresolved.append({"item": mini, "hint": "unsupported_type"})
            continue
        else:
            shows.append({"ids": ids})

        thaw_keys.append(key)
        attempted.append(it)

    if not (movies or shows):
        return 0, unresolved

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
            ok = len(movies) + len(shows)
            _log(f"remove done: -{ok}")
            return ok, unresolved
        _log(f"REMOVE failed {resp.status_code}: {(resp.text or '')[:180]}")
    except Exception as exc:
        _log(f"REMOVE error: {exc}")

    for it in attempted:
        ids = _ids_of(it) or _show_ids_of_episode(it)
        if ids:
            _freeze(it, action="remove", reasons=["write_failed"], ids_sent=ids, rating=None)

    return 0, unresolved

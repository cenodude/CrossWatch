# /providers/sync/mdblist/_history.py
# MDBList watched history sync module (delta semantics)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, TypeGuard

from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._mod_common import request_with_retries

from ._common import (
    START_OF_TIME_ISO,
    STATE_DIR,
    state_file,
    as_epoch,
    as_iso,
    cfg_int,
    cfg_section,
    coalesce_since,
    get_watermark,
    iso_ok,
    iso_z,
    make_logger,
    max_iso,
    now_iso,
    read_json,
    write_json,
    pad_since_iso,
    save_watermark,
    update_watermark_if_new,
)


BASE = "https://api.mdblist.com"
URL_LAST_ACTIVITIES = f"{BASE}/sync/last_activities"
URL_LIST = f"{BASE}/sync/watched"
URL_UPSERT = f"{BASE}/sync/watched"
URL_REMOVE = f"{BASE}/sync/watched/remove"

def _cache_path() -> Path:
    return state_file("mdblist_history.index.json")

_log = make_logger("history")
_cfg = cfg_section
_cfg_int = cfg_int
_now_iso = now_iso
_iso_ok = iso_ok
_iso_z = iso_z
_as_epoch = as_epoch
_as_iso = as_iso
_max_iso = max_iso
_pad_since_iso = pad_since_iso


def _load_cache() -> dict[str, Any]:
    try:
        p = _cache_path()
        doc = read_json(p)
        if not isinstance(doc, dict):
            return {}
        return dict(doc.get("items") or {})
    except Exception:
        return {}


def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        write_json(_cache_path(), doc)
    except Exception as e:
        _log(f"cache.save failed: {e}")


def _fetch_last_activities(adapter: Any, *, timeout: float, retries: int) -> dict[str, Any] | None:
    try:
        client = getattr(adapter, "client", None)
        if client and hasattr(client, "last_activities"):
            data = client.last_activities()
            if isinstance(data, Mapping) and "error" not in data and "status" not in data:
                return dict(data)
    except Exception:
        pass

    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        return None

    sess = adapter.client.session
    try:
        r = request_with_retries(
            sess,
            "GET",
            URL_LAST_ACTIVITIES,
            params={"apikey": apikey},
            timeout=timeout,
            max_retries=retries,
        )
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            return dict(data) if isinstance(data, Mapping) else None
    except Exception:
        return None
    return None


def _base_key(item: Mapping[str, Any]) -> str:
    if (
        item.get("type") in ("episode", "season")
        and isinstance(item.get("show_ids"), Mapping)
        and item.get("season") is not None
    ):
        base_obj: dict[str, Any] = {
            "type": str(item.get("type")),
            "show_ids": dict(item.get("show_ids") or {}),
            "season": item.get("season"),
        }
        if item.get("type") == "episode" and item.get("episode") is not None:
            base_obj["episode"] = item.get("episode")
        return canonical_key(id_minimal(base_obj))
    return canonical_key(id_minimal(item))


def _event_key(item: Mapping[str, Any]) -> str | None:
    w = item.get("watched_at") or item.get("last_watched_at")
    if not _iso_ok(w):
        return None
    ts = _as_epoch(_iso_z(w))
    if not ts:
        return None
    return f"{_base_key(item)}@{ts}"


def _merge_event(dst: dict[str, Any], item: Mapping[str, Any]) -> str | None:
    ek = _event_key(item)
    if not ek:
        return None
    cur = dst.get(ek)
    if not cur:
        dst[ek] = dict(item)
        return ek
    w_new = str(item.get("watched_at") or "")
    w_old = str(cur.get("watched_at") or "")
    if w_new >= w_old:
        merged = dict(cur)
        merged.update(dict(item))
        dst[ek] = merged
    return ek


def _ids_pick(obj: Mapping[str, Any]) -> dict[str, Any]:
    ids_raw: dict[str, Any] = dict(obj.get("ids") or {})
    out: dict[str, Any] = {}
    for k in ("imdb", "tmdb", "tvdb", "trakt", "kitsu", "mdblist"):
        v = ids_raw.get(k) or obj.get(k) or obj.get(f"{k}_id")
        if v is None or v == "":
            continue
        if k in ("tmdb", "tvdb", "trakt"):
            try:
                out[k] = int(v)
            except Exception:
                continue
        else:
            out[k] = str(v)
    if out.get("imdb") and out.get("tmdb") and out.get("tvdb"):
        out.pop("tvdb", None)
    return out


def _row_movie(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        mv = row.get("movie") or {}
        ids = _ids_pick(mv)
        if not ids:
            return None
        out: dict[str, Any] = {"type": "movie", "ids": ids}
        title = str(mv.get("title") or mv.get("name") or "").strip()
        if title:
            out["title"] = title
        y = mv.get("year") or mv.get("release_year")
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)
        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_show(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sh = row.get("show") or {}
        ids = _ids_pick(sh)
        if not ids:
            return None
        out: dict[str, Any] = {"type": "show", "ids": ids}
        title = str(sh.get("title") or sh.get("name") or "").strip()
        if title:
            out["title"] = title
        y = sh.get("year") or sh.get("first_air_year")
        if not y:
            fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)
        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_season(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sv = row.get("season") or {}
        show = sv.get("show") or {}
        sids = _ids_pick(sv)
        sh_ids = _ids_pick(show)
        ids = sids or sh_ids
        if not ids:
            return None

        out: dict[str, Any] = {"type": "season", "ids": ids, "season": sv.get("number")}
        if sh_ids:
            out["show_ids"] = sh_ids
        show_title = str(show.get("title") or show.get("name") or "").strip()
        if show_title:
            out["series_title"] = show_title

        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)

        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def _row_episode(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        ev = row.get("episode") or {}
        show = ev.get("show") or {}
        eids = _ids_pick(ev)
        sh_ids = _ids_pick(show)
        ids = eids or sh_ids
        if not ids:
            return None

        num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
        out: dict[str, Any] = {
            "type": "episode",
            "ids": ids,
            "season": ev.get("season"),
            "episode": num,
        }
        if sh_ids:
            out["show_ids"] = sh_ids
        show_title = str(show.get("title") or show.get("name") or "").strip()
        if show_title:
            out["series_title"] = show_title
        title = str(ev.get("name") or ev.get("title") or "").strip()
        if title:
            out["title"] = title

        w = row.get("watched_at") or row.get("last_watched_at")
        if _iso_ok(w):
            out["watched_at"] = _iso_z(w)

        plays = row.get("plays") or row.get("times_watched")
        try:
            if plays is not None:
                out["plays"] = int(plays)
        except Exception:
            pass
        return out if out.get("watched_at") else None
    except Exception:
        return None


def build_index(
    adapter: Any,
    *,
    per_page: int = 1000,
    max_pages: int = 250,
) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    cached_raw = _load_cache()
    cached: dict[str, dict[str, Any]] = {
        str(k): dict(v) for k, v in (cached_raw or {}).items() if isinstance(v, Mapping)
    }

    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        if cached:
            _log("missing api_key - using cached shadow")
        else:
            _log("missing api_key - empty history snapshot")
        return cached

    per_page = _cfg_int(cfg, "history_per_page", per_page)
    per_page = max(1, min(int(per_page), 5000))
    max_pages = _cfg_int(cfg, "history_max_pages", max_pages)
    max_pages = max(1, min(int(max_pages), 2000))

    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    acts = _fetch_last_activities(adapter, timeout=timeout, retries=retries) or {}
    acts_watched = acts.get("watched_at") if isinstance(acts, Mapping) else None
    acts_watched_iso = _iso_z(acts_watched) if _iso_ok(acts_watched) else None

    wm = get_watermark("history")
    force_baseline = False
    if acts_watched_iso and wm:
        a = _as_epoch(acts_watched_iso) or 0
        b = _as_epoch(wm) or 0
        if a <= b:
            if cached:
                _log(f"no-op (watched_at={acts_watched_iso} <= watermark={wm}) - using cached shadow")
                return cached
            _log(f"no-op (watched_at={acts_watched_iso} <= watermark={wm}) but cache empty - forcing baseline refresh")
            force_baseline = True

    if acts_watched_iso and (not wm) and cached:
        save_watermark("history", acts_watched_iso)
        _log(f"baseline watermark set to {acts_watched_iso} (using cached shadow)")
        return cached

    cfg_since = str(cfg.get("history_since") or "").strip() or None
    since_wm = START_OF_TIME_ISO if force_baseline else coalesce_since("history", cfg_since, env_any="MDBLIST_HISTORY_SINCE")
    since_req = _pad_since_iso(since_wm)

    if acts_watched_iso:
        _log(f"history changed (watched_at={acts_watched_iso} watermark={wm or '-'}) - delta since={since_req}")
    else:
        _log(f"history delta since={since_req}")

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("history") if callable(prog_factory) else None

    out: dict[str, dict[str, Any]] = {}
    latest_seen: str | None = None
    page = 1
    pages = 0
    tick = 0

    while True:
        params: dict[str, Any] = {"apikey": apikey, "page": page, "limit": per_page, "since": since_req}
        try:
            r = request_with_retries(
                sess,
                "GET",
                URL_LIST,
                params=params,
                timeout=timeout,
                max_retries=retries,
            )
        except Exception as e:
            _log(f"GET watched delta page {page} failed: {type(e).__name__}: {e}")
            break

        if r.status_code != 200:
            _log(f"GET watched delta page {page} -> {r.status_code}: {(r.text or '')[:160]}")
            break

        data = r.json() if (r.text or "").strip() else {}
        buckets = {
            "movies": data.get("movies") or [],
            "shows": data.get("shows") or [],
            "seasons": data.get("seasons") or [],
            "episodes": data.get("episodes") or [],
        }

        added = 0
        for row in buckets["movies"]:
            m = _row_movie(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["shows"]:
            m = _row_show(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["seasons"]:
            m = _row_season(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1
        for row in buckets["episodes"]:
            m = _row_episode(row) if isinstance(row, Mapping) else None
            if m:
                _merge_event(out, m)
                latest_seen = _max_iso(latest_seen, m.get("watched_at"))
                added += 1

        tick += added
        if prog and added:
            try:
                prog.tick(tick, total=max(tick, tick + 1))
            except Exception:
                pass

        pages += 1
        if pages >= max_pages:
            _log(f"delta.stop safety cap hit max_pages={max_pages}")
            break

        pag = data.get("pagination") if isinstance(data, Mapping) else None
        if isinstance(pag, Mapping) and pag.get("has_more") is False:
            break

        rows_total = sum(len(v) for v in buckets.values() if isinstance(v, list))
        if rows_total == 0:
            break

        page += 1

    merged = dict(cached)
    if out:
        for k, v in out.items():
            merged[str(k)] = dict(v)
        _save_cache(merged)

    update_watermark_if_new("history", latest_seen or acts_watched_iso)

    if prog:
        try:
            prog.tick(len(out), total=len(out))
        except Exception:
            pass

    _log(
        f"index size: {len(merged)} delta={len(out)} latest_seen={latest_seen or '-'} "
        f"watermark={get_watermark('history') or '-'}"
    )
    return merged


def _stable_show_key(ids: Mapping[str, Any]) -> str:
    keep = {k: ids.get(k) for k in ("imdb", "tmdb", "tvdb", "trakt", "mdblist") if ids.get(k) is not None}
    return json.dumps(keep, sort_keys=True)


def _bucketize(items: Iterable[Mapping[str, Any]], *, unwatch: bool) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows_nested: dict[str, dict[str, Any]] = {}
    shows_plain: dict[str, dict[str, Any]] = {}

    accepted: list[dict[str, Any]] = []
    
    def _carry_meta(src: Mapping[str, Any], dst: dict[str, Any]) -> None:
        for k in ("title", "series_title", "year"):
            v = src.get(k)
            if v is None or v == "":
                continue
            if k == "year":
                try:
                    dst[k] = int(v)
                except Exception:
                    continue
            else:
                dst[k] = v

    for raw in items or []:
        m = dict(raw or {})
        typ_raw = m.get("type")
        typ = str(typ_raw or "movie").strip().lower()
        if typ.endswith("s") and typ in ("movies", "shows", "seasons", "episodes"):
            typ = typ[:-1]
        if typ not in ("movie", "show", "season", "episode"):
            _log(
                f"skip write: unknown type={typ} item={id_minimal({'type': str(typ_raw or 'unknown'), 'ids': _ids_pick(m)})}"
            )
            continue
        ids = _ids_pick(m)
        show_ids = _ids_pick(m.get("show_ids") or {}) if isinstance(m.get("show_ids"), Mapping) else {}

        watched_at = m.get("watched_at") or m.get("last_watched_at")
        watched_iso = _iso_z(watched_at) if _iso_ok(watched_at) else None
        if not watched_iso and not unwatch:
            _log(
                f"skip write: missing watched_at type={typ} item={id_minimal({'type': typ, 'ids': ids or show_ids})}"
            )
            continue

        if typ == "movie":
            if not ids:
                continue
            row: dict[str, Any] = {"ids": ids}
            if watched_iso:
                row["watched_at"] = watched_iso
            movies.append(row)
            acc = {"type": "movie", "ids": ids, **({"watched_at": watched_iso} if watched_iso else {})}
            _carry_meta(m, acc)
            accepted.append(acc)
            continue

        if typ == "show":
            if not ids:
                continue
            key = _stable_show_key(ids)
            sh = shows_plain.get(key) or {"ids": ids}
            if watched_iso:
                sh["watched_at"] = watched_iso
            shows_plain[key] = sh
            acc = {"type": "show", "ids": ids, **({"watched_at": watched_iso} if watched_iso else {})}
            _carry_meta(m, acc)
            accepted.append(acc)
            continue

        season_num = m.get("season")
        if season_num is None:
            continue
        try:
            s = int(season_num)
        except Exception:
            continue

        sh_ids = show_ids or ids
        if not sh_ids:
            continue
        skey = _stable_show_key(sh_ids)
        show_obj = shows_nested.get(skey) or {"ids": sh_ids, "seasons": []}
        if not isinstance(show_obj.get("seasons"), list):
            show_obj["seasons"] = []
        seasons_list: list[dict[str, Any]] = show_obj["seasons"]
        season_obj: dict[str, Any] | None = next((x for x in seasons_list if int(x.get("number") or -1) == s), None)
        if not season_obj:
            season_obj = {"number": s}
            seasons_list.append(season_obj)

        if typ == "season":
            if watched_iso:
                season_obj["watched_at"] = watched_iso
            shows_nested[skey] = show_obj
            accepted.append(
                {
                    "type": "season",
                    "ids": ids or sh_ids,
                    "show_ids": sh_ids,
                    "season": s,
                    **({"watched_at": watched_iso} if watched_iso else {}),
                }
            )
            _carry_meta(m, accepted[-1])
            continue

        ep_num = m.get("episode") if m.get("episode") is not None else m.get("number")
        if ep_num is None:
            continue
        try:
            e = int(ep_num)
        except Exception:
            continue
        ep: dict[str, Any] = {"number": e}
        if watched_iso:
            ep["watched_at"] = watched_iso
        episodes_list = season_obj.get("episodes")
        if not isinstance(episodes_list, list):
            episodes_list = []
            season_obj["episodes"] = episodes_list
        episodes_list.append(ep)
        shows_nested[skey] = show_obj
        accepted.append(
            {
                "type": "episode",
                "ids": ids or sh_ids,
                "show_ids": sh_ids,
                "season": s,
                "episode": e,
                **({"watched_at": watched_iso} if watched_iso else {}),
            }
        )
        _carry_meta(m, accepted[-1])

    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if shows_nested:
        for grp in shows_nested.values():
            seasons_list2 = grp.get("seasons")
            if isinstance(seasons_list2, list):
                grp["seasons"] = sorted(seasons_list2, key=lambda x: int(x.get("number") or 0))
                for sp in grp["seasons"]:
                    eps = sp.get("episodes")
                    if isinstance(eps, list):
                        sp["episodes"] = sorted(eps, key=lambda x: int(x.get("number") or 0))
        body["shows_nested"] = list(shows_nested.values())
    if shows_plain:
        body["shows_plain"] = list(shows_plain.values())
    body = {k: v for k, v in body.items() if v}
    return body, accepted


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _write(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    unwatch: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    if not apikey:
        _log("write abort: missing api_key")
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]

    sess = adapter.client.session
    tmo = adapter.cfg.timeout
    rr = adapter.cfg.max_retries

    chunk_size = _cfg_int(cfg, "history_chunk_size", 25)
    delay_ms = _cfg_int(cfg, "history_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(cfg, "history_max_backoff_ms", 8000)

    body, accepted = _bucketize(items, unwatch=unwatch)
    if not body:
        _log("nothing to write (empty body after aggregate)")
        return 0, []

    ok = 0
    unresolved: list[dict[str, Any]] = []

    stages: list[tuple[str, str]] = [
        ("movies", "movies"),
        ("shows_nested", "shows"),
        ("shows_plain", "shows"),
    ]

    for body_key, bucket in stages:
        rows = body.get(body_key) or []
        if not rows:
            continue

        stage = "" if body_key == bucket else f" stage={body_key}"
        verb = "UNWATCH" if unwatch else "WATCH"
        _log(f"{verb} bucket={bucket}{stage} rows={len(rows)} chunk={chunk_size}")

        for part in _chunk(rows, chunk_size):
            payload = {bucket: part}
            url = URL_REMOVE if unwatch else URL_UPSERT
            attempt = 0
            backoff = delay_ms

            while True:
                r = request_with_retries(
                    sess,
                    "POST",
                    url,
                    params={"apikey": apikey},
                    json=payload,
                    timeout=tmo,
                    max_retries=rr,
                )

                if r.status_code in (200, 201, 204):
                    d: dict[str, Any]
                    if r.status_code == 204 or not (r.text or "").strip():
                        d = {}
                    else:
                        try:
                            d = r.json()
                        except Exception:
                            d = {}

                    kinds = ("movies", "shows", "seasons", "episodes")
                    if unwatch:
                        removed = d.get("removed") or d.get("deleted") or d.get("unwatched") or {}
                        n = sum(int(removed.get(k) or 0) for k in kinds)
                        if n <= 0:
                            n = len(part)
                        ok += n
                    else:
                        updated = d.get("updated") or {}
                        added = d.get("added") or {}
                        existing = d.get("existing") or {}
                        n = 0
                        n += sum(int(updated.get(k) or 0) for k in kinds)
                        n += sum(int(added.get(k) or 0) for k in kinds)
                        n += sum(int(existing.get(k) or 0) for k in kinds)
                        if n <= 0:
                            n = len(part)
                        ok += n

                    time.sleep(max(0.0, delay_ms / 1000.0))
                    break

                if r.status_code in (429, 503):
                    _log(
                        f"{'UNWATCH' if unwatch else 'WATCH'} throttled {r.status_code} "
                        f"bucket={bucket}{stage} attempt={attempt} backoff_ms={backoff}: {(r.text or '')[:180]}"
                    )
                    time.sleep(min(max_backoff_ms, backoff) / 1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff * 1.6) + 200)
                    if attempt <= 4:
                        continue

                _log(
                    f"{'UNWATCH' if unwatch else 'WATCH'} failed {r.status_code} "
                    f"bucket={bucket}{stage}: {(r.text or '')[:200]}"
                )
                for x in part:
                    ids = x.get("ids") or {}
                    t = "show" if bucket == "shows" else "movie"
                    unresolved.append({"item": id_minimal({"type": t, "ids": ids}), "hint": f"http:{r.status_code}"})
                break

    if ok > 0 and not unresolved:
        cache = _load_cache()
        if unwatch:
            for it in accepted:
                base = _base_key(it)
                for k in list(cache.keys()):
                    if str(k).startswith(base + "@"):
                        cache.pop(k, None)
        else:
            for it in accepted:
                ek = _event_key(it) if it.get("watched_at") else None
                if ek:
                    cache[ek] = dict(it)
        _save_cache(cache)

        newest: str | None = None
        for it in accepted:
            newest = _max_iso(newest, it.get("watched_at"))
        update_watermark_if_new("history", newest)

    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unwatch=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unwatch=True)

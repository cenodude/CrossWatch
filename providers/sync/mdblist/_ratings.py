# /providers/sync/mdblist/_ratings.py
# MDBList ratings sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from .._mod_common import request_with_retries

BASE = "https://api.mdblist.com"
URL_LIST = f"{BASE}/sync/ratings"
URL_UPSERT = f"{BASE}/sync/ratings"
URL_UNRATE = f"{BASE}/sync/ratings/remove"

CACHE_PATH = Path("/config/.cw_state/mdblist_ratings.index.json")
CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)


def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST:ratings] {msg}")


def _cfg(adapter: Any) -> Mapping[str, Any]:
    cfg = getattr(adapter, "config", {}) or {}
    if isinstance(cfg, dict) and isinstance(cfg.get("mdblist"), dict):
        return cfg["mdblist"]
    try:
        runtime_cfg = getattr(getattr(adapter, "cfg", None), "config", {}) or {}
        if isinstance(runtime_cfg, dict) and isinstance(runtime_cfg.get("mdblist"), dict):
            return runtime_cfg["mdblist"]
    except Exception:
        pass
    return {}


def _cfg_int(data: Mapping[str, Any], key: str, default: int) -> int:
    raw = data.get(key)
    if raw is None:
        return default
    try:
        return int(raw)
    except Exception:
        return default


def _ids_for_mdblist(item: Mapping[str, Any]) -> dict[str, Any]:
    ids_raw: dict[str, Any] = dict(item.get("ids") or {})
    if not ids_raw:
        ids_raw = {
            "imdb": item.get("imdb") or item.get("imdb_id"),
            "tmdb": item.get("tmdb") or item.get("tmdb_id"),
            "tvdb": item.get("tvdb") or item.get("tvdb_id"),
        }
    out: dict[str, Any] = {}
    imdb_val = ids_raw.get("imdb")
    if imdb_val:
        out["imdb"] = str(imdb_val)
    tmdb_val = ids_raw.get("tmdb")
    if tmdb_val is not None:
        try:
            out["tmdb"] = int(tmdb_val)
        except Exception:
            pass
    tvdb_val = ids_raw.get("tvdb")
    if tvdb_val is not None:
        try:
            out["tvdb"] = int(tvdb_val)
        except Exception:
            pass
    if out.get("imdb") and out.get("tmdb") and out.get("tvdb"):
        out.pop("tvdb", None)
    return out


def _pick_kind(item: Mapping[str, Any]) -> str:
    t = str(item.get("type") or item.get("mediatype") or "").strip().lower()
    if t in ("movie", "movies"):
        return "movies"
    if t in ("show", "tv", "series", "shows"):
        return "shows"
    if t in ("season", "seasons"):
        return "seasons"
    if t in ("episode", "episodes"):
        return "episodes"
    if str(item.get("movie") or "").lower() == "true":
        return "movies"
    if str(item.get("show") or "").lower() == "true":
        return "shows"
    return "movies"


def _key_of(obj: Mapping[str, Any]) -> str:
    kind = str(obj.get("type") or "").lower()

    ids_src: Any = obj.get("ids") or obj
    if kind in ("season", "episode"):
        show_ids = obj.get("show_ids")
        if isinstance(show_ids, Mapping) and show_ids:
            ids_src = show_ids

    ids: dict[str, Any] = dict(ids_src or {})
    imdb = str(ids.get("imdb") or ids.get("imdb_id") or "").strip()

    base = ""
    if imdb:
        base = f"imdb:{imdb}"
    else:
        tmdb_val = ids.get("tmdb") or ids.get("tmdb_id")
        if tmdb_val is not None:
            try:
                base = f"tmdb:{int(tmdb_val)}"
            except Exception:
                base = ""
        if not base:
            tvdb_val = ids.get("tvdb") or ids.get("tvdb_id")
            if tvdb_val is not None:
                try:
                    base = f"tvdb:{int(tvdb_val)}"
                except Exception:
                    base = ""
        if not base:
            mdbl = ids.get("mdblist") or ids.get("id")
            if mdbl:
                base = f"mdblist:{mdbl}"

    if kind in ("season", "episode") and not base:
        title = str(obj.get("series_title") or obj.get("title") or "").strip()
        year_val = obj.get("year")
        base = f"title:{title}|year:{year_val}" if title and year_val else ""

    if kind == "season":
        s = obj.get("season")
        if base and s is not None:
            return f"season:{base}:S{int(s)}"
        if base:
            return f"season:{base}"

    if kind == "episode":
        s = obj.get("season")
        e = obj.get("number")
        if e is None:
            e = obj.get("episode")
        if base and s is not None and e is not None:
            return f"episode:{base}:{int(s)}x{int(e)}"
        if base:
            return f"episode:{base}"

    if base:
        return base

    title = str(obj.get("title") or "").strip()
    year_val = obj.get("year")
    if title and year_val:
        return f"title:{title}|year:{year_val}"
    return f"obj:{hash(json.dumps(obj, sort_keys=True)) & 0xffffffff}"


def _valid_rating(value: Any) -> int | None:
    try:
        i = int(str(value).strip())
        return i if 1 <= i <= 10 else None
    except Exception:
        return None


def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _load_cache() -> dict[str, Any]:
    try:
        if not CACHE_PATH.exists():
            return {}
        doc = json.loads(CACHE_PATH.read_text("utf-8") or "{}")
        return dict(doc.get("items") or {})
    except Exception:
        return {}


def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        tmp = CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, CACHE_PATH)
        # _log(f"cache.saved -> {CACHE_PATH} ({len(items)})")
    except Exception as e:
        _log(f"cache.save failed: {e}")


def _merge_by_key(dst: dict[str, Any], src: Iterable[Mapping[str, Any]]) -> None:
    for m in src or []:
        key = _key_of(m)
        cur = dst.get(key)
        if not cur:
            dst[key] = dict(m)
        else:
            rated_new = str(m.get("rated_at") or "")
            rated_old = str(cur.get("rated_at") or "")
            if rated_new >= rated_old:
                dst[key] = dict(m)


def _row_movie(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None:
            return None
        mv = row.get("movie") or {}
        ids_raw = mv.get("ids") or {}
        ids = {"imdb": ids_raw.get("imdb"), "tmdb": ids_raw.get("tmdb"), "tvdb": ids_raw.get("tvdb")}
        ids = {k: v for k, v in ids.items() if v}
        if not ids:
            return None
        title = str(mv.get("title") or mv.get("name") or "").strip()
        y = mv.get("year") or mv.get("release_year")
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        out: dict[str, Any] = {"type": "movie", "ids": ids, "rating": rating}
        if row.get("rated_at"):
            out["rated_at"] = row["rated_at"]
        if title:
            out["title"] = title
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def _row_show(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None:
            return None
        sh = row.get("show") or {}
        ids_raw = sh.get("ids") or {}
        ids = {"imdb": ids_raw.get("imdb"), "tmdb": ids_raw.get("tmdb"), "tvdb": ids_raw.get("tvdb")}
        ids = {k: v for k, v in ids.items() if v}
        if not ids:
            return None
        title = str(sh.get("title") or sh.get("name") or "").strip()
        y = sh.get("year") or sh.get("first_air_year")
        if not y:
            fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        out: dict[str, Any] = {"type": "show", "ids": ids, "rating": rating}
        if row.get("rated_at"):
            out["rated_at"] = row["rated_at"]
        if title:
            out["title"] = title
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def _row_season(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None:
            return None
        sv = row.get("season") or {}
        show = sv.get("show") or {}
        sids_raw = sv.get("ids") or {}
        sids = {"tmdb": sids_raw.get("tmdb"), "tvdb": sids_raw.get("tvdb")}
        sids = {k: v for k, v in sids.items() if v}
        sh_ids_raw = show.get("ids") or {}
        sh_ids = {
            "imdb": sh_ids_raw.get("imdb"),
            "tmdb": sh_ids_raw.get("tmdb"),
            "tvdb": sh_ids_raw.get("tvdb"),
        }
        sh_ids = {k: v for k, v in sh_ids.items() if v}
        ids = sids or sh_ids
        if not ids:
            return None
        show_title = str(show.get("title") or show.get("name") or "").strip()
        title = show_title or str(sv.get("name") or "").strip()
        y = show.get("year") or show.get("first_air_year")
        if not y:
            fa = str(show.get("first_air_date") or show.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        out: dict[str, Any] = {"type": "season", "ids": ids, "rating": rating, "season": sv.get("number")}
        if sh_ids:
            out["show_ids"] = sh_ids
        if show_title:
            out["series_title"] = show_title
        if row.get("rated_at"):
            out["rated_at"] = row["rated_at"]
        if title:
            out["title"] = title
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def _row_episode(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None:
            return None
        ev = row.get("episode") or {}
        show = ev.get("show") or {}
        eids_raw = ev.get("ids") or {}
        eids = {"tmdb": eids_raw.get("tmdb"), "tvdb": eids_raw.get("tvdb")}
        eids = {k: v for k, v in eids.items() if v}
        sh_ids_raw = show.get("ids") or {}
        sh_ids = {
            "imdb": sh_ids_raw.get("imdb"),
            "tmdb": sh_ids_raw.get("tmdb"),
            "tvdb": sh_ids_raw.get("tvdb"),
        }
        sh_ids = {k: v for k, v in sh_ids.items() if v}
        ids = eids or sh_ids
        if not ids:
            return None
        show_title = str(show.get("title") or show.get("name") or "").strip()
        ep_title = str(ev.get("name") or ev.get("title") or "").strip()
        title = ep_title or show_title
        y = show.get("year") or show.get("first_air_year")
        if not y:
            fa = str(show.get("first_air_date") or show.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
        out: dict[str, Any] = {
            "type": "episode",
            "ids": ids,
            "rating": rating,
            "season": ev.get("season"),
            "episode": num,
        }
        if sh_ids:
            out["show_ids"] = sh_ids
        if show_title:
            out["series_title"] = show_title
        if row.get("rated_at"):
            out["rated_at"] = row["rated_at"]
        if title:
            out["title"] = title
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def build_index(
    adapter: Any,
    *,
    per_page: int = 1000,
    max_pages: int = 250,
) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    cached = _load_cache()
    if not apikey:
        _log("missing api_key → empty ratings index")
        if cached:
            _log(f"fallback cache (missing api_key) size={len(cached)}")
            return dict(cached)
        _save_cache({})
        return {}
    per_page = _cfg_int(cfg, "ratings_per_page", per_page)
    per_page = max(1, min(int(per_page), 5000))
    max_pages = _cfg_int(cfg, "ratings_max_pages", max_pages)
    max_pages = max(1, min(int(max_pages), 2000))
    since = str(cfg.get("ratings_since") or "").strip()
    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries
    out: dict[str, dict[str, Any]] = {}
    page = 1
    pages = 0
    _log(f"index.start per_page={per_page} max_pages={max_pages} timeout={timeout} retries={retries}")
    while True:
        try:
            r = request_with_retries(
                sess,
                "GET",
                URL_LIST,
                params={"apikey": apikey, "page": page, "limit": per_page, **({"since": since} if since else {})},
                timeout=timeout,
                max_retries=retries,
            )
        except Exception as e:
            _log(f"GET /sync/ratings page {page} failed: {type(e).__name__}: {e}")
            if cached:
                _log(f"fallback cache size={len(cached)}")
                return dict(cached)
            raise
        if r.status_code != 200:
            _log(f"GET /sync/ratings page {page} -> {r.status_code}")
            if cached:
                _log(f"fallback cache size={len(cached)}")
                return dict(cached)
            break
        data = r.json() if (r.text or "").strip() else {}
        movies = data.get("movies") or []
        shows = data.get("shows") or []
        seasons_top = data.get("seasons") or []
        episodes_top = data.get("episodes") or []
        _log(
            f"page {page} -> movies:{len(movies)} shows:{len(shows)} "
            f"seasons:{len(seasons_top)} episodes:{len(episodes_top)}"
        )
        minis: list[dict[str, Any]] = []
        for row in movies:
            m = _row_movie(row)
            if m:
                minis.append(m)
        for row in shows:
            m = _row_show(row)
            if m:
                minis.append(m)
            sh = row.get("show") or {}
            sh_ids_raw = sh.get("ids") or {}
            ids_sh = {
                k: v
                for k, v in {
                    "imdb": sh_ids_raw.get("imdb"),
                    "tmdb": sh_ids_raw.get("tmdb"),
                    "tvdb": sh_ids_raw.get("tvdb"),
                }.items()
                if v
            }
            show_title = str(sh.get("title") or sh.get("name") or "").strip()
            y = sh.get("year") or sh.get("first_air_year")
            if not y:
                fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
                if len(fa) >= 4 and fa[:4].isdigit():
                    y = int(fa[:4])
            try:
                year = int(y) if y is not None else None
            except Exception:
                year = None
            for sv in row.get("seasons") or []:
                sr = _valid_rating(sv.get("rating"))
                sids_raw = sv.get("ids") or {}
                sids = {k: sids_raw.get(k) for k in ("tmdb", "tvdb") if sids_raw.get(k)}
                ids_for_season = sids or {k: ids_sh.get(k) for k in ("tmdb", "tvdb") if ids_sh.get(k)}
                if sr is not None and ids_for_season:
                    sm: dict[str, Any] = {
                        "type": "season",
                        "ids": ids_for_season,
                        "show_ids": ids_sh,
                        "rating": sr,
                        "season": sv.get("number"),
                    }
                    ra = sv.get("rated_at")
                    if ra:
                        sm["rated_at"] = ra
                    if show_title:
                        sm["series_title"] = show_title
                        sm["title"] = show_title
                    if year:
                        sm["year"] = year
                    minis.append(sm)
                for ev in sv.get("episodes") or []:
                    er = _valid_rating(ev.get("rating"))
                    if er is None:
                        continue
                    eids_raw = ev.get("ids") or {}
                    eids = {k: eids_raw.get(k) for k in ("tmdb", "tvdb") if eids_raw.get(k)}
                    ids_for_episode = eids or ids_sh
                    if not ids_for_episode:
                        continue
                    num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
                    em: dict[str, Any] = {
                        "type": "episode",
                        "ids": ids_for_episode,
                        "show_ids": ids_sh,
                        "rating": er,
                        "season": sv.get("number"),
                        "episode": num,
                    }
                    rae = ev.get("rated_at")
                    if rae:
                        em["rated_at"] = rae
                    ep_title = str(ev.get("name") or ev.get("title") or "").strip()
                    if show_title:
                        em["series_title"] = show_title
                    if ep_title:
                        em["title"] = ep_title
                    elif show_title:
                        em["title"] = show_title
                    if year:
                        em["year"] = year
                    minis.append(em)
        for row in seasons_top:
            m = _row_season(row)
            if m:
                minis.append(m)
        for row in episodes_top:
            m = _row_episode(row)
            if m:
                minis.append(m)
        for m in minis:
            out[_key_of(m)] = m
        pag = data.get("pagination") or {}
        has_more = pag.get("has_more")
        if has_more is None:
            has_more = any(len(x) >= per_page for x in (movies, shows, seasons_top, episodes_top))

        pages += 1
        if not bool(has_more) or pages >= max_pages:
            break
        page += 1

    fetch_types = bool(cfg.get("ratings_fetch_type_pages") in (True, "1", 1))
    if fetch_types:
        def _fetch_type_pages(type_name: str) -> None:
            p = 1
            done = 0
            while True:
                r_ = request_with_retries(
                    sess,
                    "GET",
                    URL_LIST,
                    params={"apikey": apikey, "page": p, "limit": per_page, "type": type_name, **({"since": since} if since else {})},
                    timeout=timeout,
                    max_retries=retries,
                )
                if r_.status_code != 200:
                    _log(f"GET /sync/ratings?type={type_name} page {p} -> {r_.status_code}")
                    break
                d = r_.json() if (r_.text or "").strip() else {}
                rows = d.get(type_name) or []
                if not rows:
                    break
                if type_name == "seasons":
                    for row_ in rows:
                        m_ = _row_season(row_)
                        if m_:
                            out[_key_of(m_)] = m_
                elif type_name == "episodes":
                    for row_ in rows:
                        m_ = _row_episode(row_)
                        if m_:
                            out[_key_of(m_)] = m_
                pag_ = d.get("pagination") or {}
                done += 1
                has_more = pag_.get("has_more")
                if has_more is None:
                    has_more = len(rows) >= per_page
                if not bool(has_more) or done >= max_pages:
                    break
                p += 1
        _fetch_type_pages("seasons")
        _fetch_type_pages("episodes")
    kinds: dict[str, int] = {"movie": 0, "show": 0, "season": 0, "episode": 0}
    for v in out.values():
        k = str(v.get("type") or "").lower()
        if k in kinds:
            kinds[k] += 1
    _save_cache(out)
    _log(f"index size: {len(out)} by-kind {kinds}")
    return out


def _show_key(ids: Mapping[str, Any]) -> str:
    if ids.get("imdb"):
        return f"imdb:{ids['imdb']}"
    if ids.get("tmdb"):
        return f"tmdb:{ids['tmdb']}"
    if ids.get("tvdb"):
        return f"tvdb:{ids['tvdb']}"
    return json.dumps(ids, sort_keys=True)


def _bucketize(
    items: Iterable[Mapping[str, Any]],
    *,
    unrate: bool = False,
) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
    body: dict[str, list[dict[str, Any]]] = {"movies": []}
    accepted: list[dict[str, Any]] = []

    seen: dict[str, int] = {"movies": 0, "shows": 0, "seasons": 0, "episodes": 0}
    kept: dict[str, int] = {"movies": 0, "shows": 0, "seasons": 0, "episodes": 0}
    attach: dict[str, int] = {"season_to_show": 0, "episode_to_season": 0}
    skip: dict[str, int] = {
        "invalid_rating": 0,
        "missing_ids": 0,
        "missing_season": 0,
        "missing_episode": 0,
        "missing_show_ids": 0,
    }

    shows_nested: dict[str, dict[str, Any]] = {}
    shows_plain: dict[str, dict[str, Any]] = {}
    seasons_index: dict[tuple[str, int], dict[str, Any]] = {}

    def ensure_show_nested(ids: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        sk = _show_key(ids)
        grp = shows_nested.get(sk)
        if not grp:
            grp = {"ids": ids}
            shows_nested[sk] = grp
        return sk, grp

    def ensure_show_plain(ids: dict[str, Any]) -> dict[str, Any]:
        sk = _show_key(ids)
        grp = shows_plain.get(sk)
        if not grp:
            grp = {"ids": ids}
            shows_plain[sk] = grp
        return grp

    for item in items or []:
        kind = _pick_kind(item)
        if kind in seen:
            seen[kind] += 1

        if kind in ("seasons", "episodes"):
            ids = _ids_for_mdblist(item.get("show_ids") or {})
            if not ids:
                ids = _ids_for_mdblist(item)
            if not ids:
                skip["missing_show_ids"] += 1
                continue
        else:
            ids = _ids_for_mdblist(item)
            if not ids:
                skip["missing_ids"] += 1
                continue

        rating = _valid_rating(item.get("rating"))
        rated_at = item.get("rated_at")

        if kind == "movies":
            if rating is None and not unrate:
                skip["invalid_rating"] += 1
                continue

            obj: dict[str, Any] = {"ids": ids}
            if not unrate:
                obj["rating"] = rating
                if rated_at:
                    obj["rated_at"] = rated_at
            body["movies"].append(obj)

            acc: dict[str, Any] = {"type": "movie", "ids": ids}
            if not unrate:
                acc["rating"] = rating
                if rated_at:
                    acc["rated_at"] = rated_at
            accepted.append(acc)

            kept["movies"] += 1
            continue

        if kind == "shows":
            if rating is None and not unrate:
                skip["invalid_rating"] += 1
                continue

            grp_plain = ensure_show_plain(ids)
            if not unrate and rating is not None:
                grp_plain["rating"] = rating
                if rated_at:
                    grp_plain["rated_at"] = rated_at

            acc2: dict[str, Any] = {"type": "show", "ids": ids}
            if not unrate:
                acc2["rating"] = rating
                if rated_at:
                    acc2["rated_at"] = rated_at
            accepted.append(acc2)

            kept["shows"] += 1
            continue

        if kind == "seasons":
            s_raw = item.get("season") or item.get("number")
            if s_raw is None:
                skip["missing_season"] += 1
                continue

            sk, grp = ensure_show_nested(ids)
            s = int(s_raw)

            sp: dict[str, Any] | None = seasons_index.get((sk, s))
            if sp is None:
                sp = {"number": s}
                seasons_index[(sk, s)] = sp
                seasons_list = grp.get("seasons")
                if not isinstance(seasons_list, list):
                    seasons_list = []
                    grp["seasons"] = seasons_list
                seasons_list.append(sp)
                attach["season_to_show"] += 1

            if not unrate and rating is not None:
                sp["rating"] = rating
                if rated_at:
                    sp["rated_at"] = rated_at

            acc3: dict[str, Any] = {"type": "season", "ids": ids, "season": s}
            if not unrate:
                acc3["rating"] = rating
                if rated_at:
                    acc3["rated_at"] = rated_at
            accepted.append(acc3)

            kept["seasons"] += 1
            continue

        s_raw = item.get("season")
        e_raw = item.get("number") if item.get("number") is not None else item.get("episode")
        if s_raw is None or e_raw is None:
            skip["missing_episode"] += 1
            continue

        sk, grp = ensure_show_nested(ids)
        s = int(s_raw)
        e = int(e_raw)

        sp2: dict[str, Any] | None = seasons_index.get((sk, s))
        if sp2 is None:
            sp2 = {"number": s}
            seasons_index[(sk, s)] = sp2
            seasons_list2 = grp.get("seasons")
            if not isinstance(seasons_list2, list):
                seasons_list2 = []
                grp["seasons"] = seasons_list2
            seasons_list2.append(sp2)
            attach["season_to_show"] += 1

        ep: dict[str, Any] = {"number": e}
        if not unrate and rating is not None:
            ep["rating"] = rating
            if rated_at:
                ep["rated_at"] = rated_at
        episodes_list = sp2.get("episodes")
        if not isinstance(episodes_list, list):
            episodes_list = []
            sp2["episodes"] = episodes_list
        episodes_list.append(ep)
        attach["episode_to_season"] += 1

        acc4: dict[str, Any] = {"type": "episode", "ids": ids, "season": s, "episode": e}
        if not unrate:
            acc4["rating"] = rating
            if rated_at:
                acc4["rated_at"] = rated_at
        accepted.append(acc4)

        kept["episodes"] += 1

    if shows_nested:
        for grp in shows_nested.values():
            seasons_list3 = grp.get("seasons")
            if isinstance(seasons_list3, list):
                grp["seasons"] = sorted(seasons_list3, key=lambda x: int(x.get("number") or 0))
                for sp3 in grp["seasons"]:
                    episodes_list3 = sp3.get("episodes")
                    if isinstance(episodes_list3, list):
                        sp3["episodes"] = sorted(episodes_list3, key=lambda x: int(x.get("number") or 0))
        body["shows_nested"] = list(shows_nested.values())

    if shows_plain:
        body["shows_plain"] = list(shows_plain.values())

    body = {k: v for k, v in body.items() if v}
    out_sizes = {k: len(v) for k, v in body.items()}

    _log(f"aggregate seen={seen} attach={attach} skip={skip} out_sizes={out_sizes} unrate={unrate}")

    if body.get("shows_nested"):
        try:
            sample = body["shows_nested"][0]
            _log(f"shows.sample ids={sample.get('ids')} seasons={len(sample.get('seasons', []))}")
        except Exception:
            pass
    if body.get("shows_plain"):
        try:
            sample2 = body["shows_plain"][0]
            _log("shows.sample_plain ids=%s seasons=0" % (sample2.get("ids"),))
        except Exception:
            pass

    return body, accepted


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _write(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
    *,
    unrate: bool = False,
) -> tuple[int, list[dict[str, Any]]]:
    cfg = _cfg(adapter)
    apikey = str(cfg.get("api_key") or "").strip()
    cached = _load_cache()
    if not apikey:
        _log("write abort: missing api_key")
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]

    sess = adapter.client.session
    tmo = adapter.cfg.timeout
    rr = adapter.cfg.max_retries

    chunk_size = _cfg_int(cfg, "ratings_chunk_size", 25)
    delay_ms = _cfg_int(cfg, "ratings_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(cfg, "ratings_max_backoff_ms", 8000)

    body, accepted = _bucketize(items, unrate=unrate)
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
        _log(f"{'UNRATE' if unrate else 'UPSERT'} bucket={bucket}{stage} rows={len(rows)} chunk={chunk_size}")

        for part in _chunk(rows, chunk_size):
            payload = {bucket: part}
            url = URL_UNRATE if unrate else URL_UPSERT

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
                    if r.status_code == 204 or not (r.text or "").strip():
                        d: dict[str, Any] = {}
                    else:
                        try:
                            d = r.json()
                        except Exception:
                            d = {}

                    kinds = ("movies", "shows", "seasons", "episodes")

                    if r.status_code == 204:
                        ok += len(part)
                        _log(f"{'UNRATE' if unrate else 'UPSERT'} ok bucket={bucket}{stage} (204) count={len(part)}")
                    else:
                        if unrate:
                            removed = d.get("removed") or {}
                            n = sum(int(removed.get(k) or 0) for k in kinds)
                            if n <= 0:
                                n = len(part)
                            _log(f"UNRATE ok bucket={bucket}{stage} removed={removed} counted={n}")
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
                            _log(
                                f"UPSERT ok bucket={bucket}{stage} updated={updated} "
                                f"added={added} existing={existing} counted={n}"
                            )
                            ok += n

                    time.sleep(max(0.0, delay_ms / 1000.0))
                    break

                if r.status_code in (429, 503):
                    _log(
                        f"{'UNRATE' if unrate else 'UPSERT'} throttled {r.status_code} "
                        f"bucket={bucket}{stage} attempt={attempt} backoff_ms={backoff}: {(r.text or '')[:180]}"
                    )
                    time.sleep(min(max_backoff_ms, backoff) / 1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff * 1.6) + 200)
                    if attempt <= 4:
                        continue

                _log(
                    f"{'UNRATE' if unrate else 'UPSERT'} failed {r.status_code} "
                    f"bucket={bucket}{stage}: {(r.text or '')[:200]}"
                )
                try:
                    sample = part[0]
                    if bucket == "shows":
                        _log(f"payload.sample shows.ids={sample.get('ids')} seasons={len(sample.get('seasons', []))}")
                    else:
                        _log(f"payload.sample movies.ids={sample.get('ids')}")
                except Exception:
                    pass

                for x in part:
                    iid = x.get("ids") or {}
                    t = "show" if bucket == "shows" else "movie"
                    unresolved.append({"item": id_minimal({"type": t, "ids": iid}), "hint": f"http:{r.status_code}"})
                break

    if ok > 0 and not unresolved and not unrate:
        cache = _load_cache()
        _merge_by_key(cache, accepted)
        _save_cache(cache)

    if ok > 0 and not unresolved and unrate:
        cache2 = _load_cache()
        for it in accepted:
            cache2.pop(_key_of(it), None)
        _save_cache(cache2)

    if unresolved:
        _log(f"unresolved count={len(unresolved)}")
    else:
        _log("all writes resolved")

    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unrate=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unrate=True)

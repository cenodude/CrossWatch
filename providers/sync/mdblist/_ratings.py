# /providers/sync/mdblist/_ratings.py
# MDBList ratings sync module (activity-gated delta)
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable, Mapping, TypeGuard

from cw_platform.id_map import minimal as id_minimal

from .._mod_common import request_with_retries

from ._common import (
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
    pad_since_iso,
    save_watermark,
    update_watermark_if_new,
    START_OF_TIME_ISO,
)


BASE = "https://api.mdblist.com"
URL_LIST = f"{BASE}/sync/ratings"
URL_UPSERT = f"{BASE}/sync/ratings"
URL_UNRATE = f"{BASE}/sync/ratings/remove"
URL_LAST_ACTIVITIES = f"{BASE}/sync/last_activities"

def _cache_path() -> Path:
    return state_file("mdblist_ratings.index.json")

_log = make_logger("ratings")
_cfg = cfg_section
_cfg_int = cfg_int
_iso_ok = iso_ok
_iso_z = iso_z
_as_epoch = as_epoch
_as_iso = as_iso
_max_iso = max_iso
_pad_since_iso = pad_since_iso
_now_iso = now_iso


def _load_cache() -> dict[str, Any]:
    try:
        p = _cache_path()
        if not p.exists():
            return {}
        doc = json.loads(p.read_text("utf-8") or "{}")
        return dict(doc.get("items") or {})
    except Exception:
        return {}


def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        p = _cache_path()
        tmp = p.with_name(f"{p.name}.tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as e:
        _log(f"cache.save failed: {e}")


def _fetch_last_activities(adapter: Any, *, apikey: str, timeout: float, retries: int) -> dict[str, Any] | None:
    try:
        client = getattr(adapter, "client", None)
        if client and hasattr(client, "last_activities"):
            data = client.last_activities()
            if isinstance(data, Mapping) and "error" not in data and "status" not in data:
                return dict(data)
    except Exception:
        pass
    try:
        r = request_with_retries(
            adapter.client.session,
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
    if t.endswith("s") and t in ("movies", "shows", "seasons", "episodes"):
        t = t[:-1]
    if t == "movie":
        return "movies"
    if t == "show":
        return "shows"
    if t == "season":
        return "seasons"
    if t == "episode":
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


def _row_movie(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        mv = row.get("movie") or {}
        ids = _ids_for_mdblist(mv.get("ids") or mv)
        if not ids:
            return None

        rating = _valid_rating(row.get("rating"))
        rated_at = row.get("rated_at")

        out: dict[str, Any] = {"type": "movie", "ids": ids}
        if rating is None:
            out["_removed"] = True
        else:
            out["rating"] = rating
        if rated_at:
            out["rated_at"] = rated_at

        title = str(mv.get("title") or mv.get("name") or "").strip()
        y = mv.get("year") or mv.get("release_year")
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if title:
            out["title"] = title
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def _row_show(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sh = row.get("show") or {}
        ids = _ids_for_mdblist(sh.get("ids") or sh)
        if not ids:
            return None

        rating = _valid_rating(row.get("rating"))
        rated_at = row.get("rated_at")

        out: dict[str, Any] = {"type": "show", "ids": ids}
        if rating is None:
            out["_removed"] = True
        else:
            out["rating"] = rating
        if rated_at:
            out["rated_at"] = rated_at

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
        if title:
            out["title"] = title
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def _row_season(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        sv = row.get("season") or {}
        show = sv.get("show") or {}
        sh_ids = _ids_for_mdblist(show.get("ids") or show)
        ids = _ids_for_mdblist(sv.get("ids") or sv) or sh_ids
        if not ids:
            return None

        rating = _valid_rating(row.get("rating"))
        rated_at = row.get("rated_at")

        out: dict[str, Any] = {"type": "season", "ids": ids, "season": sv.get("number")}
        if sh_ids:
            out["show_ids"] = sh_ids
        if rating is None:
            out["_removed"] = True
        else:
            out["rating"] = rating
        if rated_at:
            out["rated_at"] = rated_at

        show_title = str(show.get("title") or show.get("name") or "").strip()
        if show_title:
            out["series_title"] = show_title
            out["title"] = show_title

        y = show.get("year") or show.get("first_air_year")
        if not y:
            fa = str(show.get("first_air_date") or show.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
        if year:
            out["year"] = year
        return out
    except Exception:
        return None


def _row_episode(row: Mapping[str, Any]) -> dict[str, Any] | None:
    try:
        ev = row.get("episode") or {}
        show = ev.get("show") or {}
        sh_ids = _ids_for_mdblist(show.get("ids") or show)
        ids = _ids_for_mdblist(ev.get("ids") or ev) or sh_ids
        if not ids:
            return None

        rating = _valid_rating(row.get("rating"))
        rated_at = row.get("rated_at")

        num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
        out: dict[str, Any] = {
            "type": "episode",
            "ids": ids,
            "season": ev.get("season"),
            "episode": num,
        }
        if sh_ids:
            out["show_ids"] = sh_ids
        if rating is None:
            out["_removed"] = True
        else:
            out["rating"] = rating
        if rated_at:
            out["rated_at"] = rated_at

        show_title = str(show.get("title") or show.get("name") or "").strip()
        ep_title = str(ev.get("name") or ev.get("title") or "").strip()
        if show_title:
            out["series_title"] = show_title
        if ep_title:
            out["title"] = ep_title
        elif show_title:
            out["title"] = show_title

        y = show.get("year") or show.get("first_air_year")
        if not y:
            fa = str(show.get("first_air_date") or show.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit():
                y = int(fa[:4])
        try:
            year = int(y) if y is not None else None
        except Exception:
            year = None
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
        _log("missing api_key - ratings index from cache only")
        return dict(cached) if cached else {}

    per_page = _cfg_int(cfg, "ratings_per_page", per_page)
    per_page = max(1, min(int(per_page), 5000))
    max_pages = _cfg_int(cfg, "ratings_max_pages", max_pages)
    max_pages = max(1, min(int(max_pages), 2000))

    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    acts = _fetch_last_activities(adapter, apikey=apikey, timeout=timeout, retries=retries) or {}
    acts_ts_raw = acts.get("rated_at") if isinstance(acts, Mapping) else None
    acts_ts = _iso_z(acts_ts_raw) if _iso_ok(acts_ts_raw) else None
    force_full = False

    wm = get_watermark("ratings")
    if acts_ts and wm:
        a = _as_epoch(acts_ts) or 0
        b = _as_epoch(wm) or 0
        if a <= b:
            if cached:
                _log(f"no-op (rated_at={acts_ts} <= watermark={wm}) - using cached snapshot")
                return dict(cached)
            _log(f"no-op (rated_at={acts_ts} <= watermark={wm}) but no cached snapshot - forcing refresh")
            force_full = True

    if acts_ts and (not wm) and cached:
        save_watermark("ratings", acts_ts)
        _log(f"baseline watermark set to {acts_ts} (using cached snapshot)")
        return dict(cached)
    cfg_since = str(cfg.get("ratings_since") or "").strip() or None
    if force_full:
        env_since = str(os.getenv("MDBLIST_RATINGS_SINCE") or "").strip() or None
        since_base = cfg_since or env_since or START_OF_TIME_ISO
    else:
        since_base = coalesce_since("ratings", cfg_since, env_any="MDBLIST_RATINGS_SINCE")
    since_req = _pad_since_iso(since_base)

    sess = adapter.client.session
    out: dict[str, dict[str, Any]] = {}
    page = 1
    pages = 0
    latest_seen: str | None = None

    _log(f"delta.start since={since_req} per_page={per_page} max_pages={max_pages} timeout={timeout} retries={retries}")
    while True:
        r = request_with_retries(
            sess,
            "GET",
            URL_LIST,
            params={"apikey": apikey, "page": page, "limit": per_page, "since": since_req},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code != 200:
            _log(f"GET /sync/ratings page {page} -> {r.status_code}: {(r.text or '')[:160]}")
            return dict(cached)

        data = r.json() if (r.text or "").strip() else {}
        movies = data.get("movies") or []
        shows = data.get("shows") or []
        seasons_top = data.get("seasons") or []
        episodes_top = data.get("episodes") or []

        minis: list[dict[str, Any]] = []
        for row in movies:
            m = _row_movie(row) if isinstance(row, Mapping) else None
            if m:
                minis.append(m)
        for row in shows:
            m = _row_show(row) if isinstance(row, Mapping) else None
            if m:
                minis.append(m)

            # Optional nested season/episode payloads under show rows (kept for backwards-compat)
            if isinstance(row, Mapping) and isinstance(row.get("seasons"), list):
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
                    if ids_for_season:
                        sm: dict[str, Any] = {
                            "type": "season",
                            "ids": ids_for_season,
                            "show_ids": ids_sh,
                            "season": sv.get("number"),
                        }
                        if sr is None:
                            sm["_removed"] = True
                        else:
                            sm["rating"] = sr
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
                            "season": sv.get("number"),
                            "episode": num,
                        }
                        if er is None:
                            em["_removed"] = True
                        else:
                            em["rating"] = er
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
            m = _row_season(row) if isinstance(row, Mapping) else None
            if m:
                minis.append(m)
        for row in episodes_top:
            m = _row_episode(row) if isinstance(row, Mapping) else None
            if m:
                minis.append(m)

        for m in minis:
            k = _key_of(m)
            out[k] = m
            ra = m.get("rated_at")
            if _iso_ok(ra):
                latest_seen = _max_iso(latest_seen, ra)

        pag = data.get("pagination") or {}
        has_more = pag.get("has_more")
        if has_more is None:
            has_more = any(len(x) >= per_page for x in (movies, shows, seasons_top, episodes_top))

        pages += 1
        if not bool(has_more) or pages >= max_pages:
            break
        page += 1
    merged = dict(cached)
    if out:
        for k, v in out.items():
            if v.get("_removed") or v.get("rating") is None:
                merged.pop(k, None)
            else:
                merged[k] = v
        _save_cache(merged)

    update_watermark_if_new("ratings", _max_iso(latest_seen, acts_ts))

    _log(f"delta size: {len(out)} latest_seen={latest_seen or '-'} watermark={get_watermark('ratings') or '-'}")
    return merged


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

    shows_nested: dict[str, dict[str, Any]] = {}
    shows_plain: dict[str, dict[str, Any]] = {}
    seasons_index: dict[tuple[str, int], dict[str, Any]] = {}
    
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

        if kind in ("seasons", "episodes"):
            ids = _ids_for_mdblist(item.get("show_ids") or {})
            if not ids:
                ids = _ids_for_mdblist(item)
            if not ids:
                continue
        else:
            ids = _ids_for_mdblist(item)
            if not ids:
                continue

        rating = _valid_rating(item.get("rating"))
        rated_at = item.get("rated_at")

        if kind == "movies":
            if rating is None and not unrate:
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
            _carry_meta(item, acc)
            accepted.append(acc)
            continue

        if kind == "shows":
            if rating is None and not unrate:
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
            _carry_meta(item, acc2)
            accepted.append(acc2)
            continue

        if kind == "seasons":
            s_raw = item.get("season") or item.get("number")
            if s_raw is None:
                continue
            s = int(s_raw)

            sk, grp = ensure_show_nested(ids)
            sp: dict[str, Any] | None = seasons_index.get((sk, s))
            if sp is None:
                sp = {"number": s}
                seasons_index[(sk, s)] = sp
                seasons_list = grp.get("seasons")
                if not isinstance(seasons_list, list):
                    seasons_list = []
                    grp["seasons"] = seasons_list
                seasons_list.append(sp)

            if not unrate and rating is not None:
                sp["rating"] = rating
                if rated_at:
                    sp["rated_at"] = rated_at

            acc3: dict[str, Any] = {"type": "season", "ids": ids, "season": s}
            if not unrate:
                acc3["rating"] = rating
                if rated_at:
                    acc3["rated_at"] = rated_at
            if isinstance(item.get("show_ids"), Mapping):
                acc3["show_ids"] = _ids_for_mdblist(item.get("show_ids") or {}) or acc3.get("show_ids")
            _carry_meta(item, acc3)
            accepted.append(acc3)
            continue

        s_raw = item.get("season")
        e_raw = item.get("number") if item.get("number") is not None else item.get("episode")
        if s_raw is None or e_raw is None:
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

        acc4: dict[str, Any] = {"type": "episode", "ids": ids, "season": s, "episode": e}
        if not unrate and rating is not None:
            acc4["rating"] = rating
            if rated_at:
                acc4["rated_at"] = rated_at
        if isinstance(item.get("show_ids"), Mapping):
            acc4["show_ids"] = _ids_for_mdblist(item.get("show_ids") or {}) or acc4.get("show_ids")
        _carry_meta(item, acc4)
        accepted.append(acc4)

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
                    else:
                        if unrate:
                            removed = d.get("removed") or {}
                            n = sum(int(removed.get(k) or 0) for k in kinds) or len(part)
                            ok += n
                        else:
                            updated = d.get("updated") or {}
                            added = d.get("added") or {}
                            existing = d.get("existing") or {}
                            n = 0
                            n += sum(int(updated.get(k) or 0) for k in kinds)
                            n += sum(int(added.get(k) or 0) for k in kinds)
                            n += sum(int(existing.get(k) or 0) for k in kinds)
                            ok += n or len(part)

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
                for x in part:
                    iid = x.get("ids") or {}
                    t = "show" if bucket == "shows" else "movie"
                    unresolved.append({"item": id_minimal({"type": t, "ids": iid}), "hint": f"http:{r.status_code}"})
                break

    if ok > 0 and not unresolved:
        cache = _load_cache()
        if unrate:
            for it in accepted:
                cache.pop(_key_of(it), None)
        else:
            for it in accepted:
                cache[_key_of(it)] = dict(it)
        _save_cache(cache)
        update_watermark_if_new("ratings", _now_iso())

    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unrate=False)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, items, unrate=True)

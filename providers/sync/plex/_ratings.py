# /providers/sync/plex/_ratings.py
# Plex Module for ratings synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
import json
from threading import Lock
from typing import Any, Iterable, Mapping, cast

from ._common import (
    _as_base_url,
    _xml_to_container,
    active_pms_token,
    as_epoch as _as_epoch,
    configure_plex_context,
    episode_rating_key_from_show,
    force_episode_title as _force_episode_title,
    has_external_ids,
    home_scope_enter,
    home_scope_exit,
    item_guid_candidates,
    iso_from_epoch as _iso,
    minimal_from_history_row,
    normalize as plex_normalize,
    normalize_discover_row,
    plex_cfg_get,
    plex_feature_library_ids,
    plex_headers,
    server_find_rating_key_by_guid,
    plex_worker_count,
    season_rating_key_from_show,
    section_allowed,
    unresolved_store,
    emit,
    make_logger,
)

_UNRES = unresolved_store("ratings")

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

_dbg, _info, _warn, _error, _log = make_logger("ratings")


def _emit(evt: dict[str, Any]) -> None:
    emit(evt, default_feature="ratings")


def _preferred_pms_token(adapter: Any) -> tuple[str, str]:
    cli = getattr(adapter, "client", None)
    srv = getattr(cli, "server", None)

    for source, value in (
        ("client._pms_token", getattr(cli, "_pms_token", None)),
        ("cfg.pms_token", getattr(getattr(cli, "cfg", None), "pms_token", None)),
        ("server._token", getattr(srv, "_token", None)),
        ("active_pms_token", active_pms_token(adapter)),
    ):
        tok = str(value or "").strip()
        if tok:
            return tok, source
    return "", "none"

def _container_from_plex_response(resp: Any) -> Mapping[str, Any] | None:
    try:
        headers = getattr(resp, "headers", None) or {}
        ct = str(headers.get("Content-Type", "") or "").lower()
        if "json" in ct:
            try:
                data = resp.json()
            except Exception:
                data = json.loads(resp.text or "{}")
            return cast(Mapping[str, Any], data) if isinstance(data, dict) else None
        txt = str(getattr(resp, "text", "") or "")
        return cast(Mapping[str, Any], _xml_to_container(txt)) if txt else None
    except Exception:
        return None

def _norm_rating(v: Any) -> int | None:
    if v is None:
        return None
    try:
        f = float(v)
    except Exception:
        return None
    if f < 0:
        return None

    i = int(f + 0.5)
    if i > 10:
        i = 10
    return i

def _resolve_rating_key(adapter: Any, it: Mapping[str, Any]) -> str | None:
    if not isinstance(it, Mapping):
        return None
    ids = ids_from(cast(Mapping[str, Any], it))
    show_ids: dict[str, Any] = {}
    try:
        raw_show_ids = it.get("show_ids") or {}
        if isinstance(raw_show_ids, Mapping):
            show_ids = ids_from({"ids": dict(raw_show_ids)})
    except Exception:
        show_ids = {}
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        return None

    kind_raw = str(it.get("type") or "").strip().lower()
    kind = {"movies":"movie","shows":"show","series":"show","anime":"show","tv":"show","tv_shows":"show","tvshows":"show"}.get(kind_raw, kind_raw)
    if kind not in {"movie", "show", "season", "episode"}:
        return None

    is_episode = kind == "episode"
    is_season = kind == "season"
    is_show = kind == "show"
    is_movie = kind == "movie"

    def _otype(o: Any) -> str:
        return str(getattr(o, "type", "") or "").strip().lower()

    def _accept_obj(o: Any) -> bool:
        t = _otype(o)
        if is_movie:
            return t == "movie"
        if is_show:
            return t == "show"
        if is_season:
            return t in {"season", "show"}
        if is_episode:
            return t in {"episode", "show"}
        return False

    rk = ids.get("plex")
    if rk:
        try:
            obj0 = srv.fetchItem(int(rk))
            if obj0 and _accept_obj(obj0):
                return str(rk)
        except Exception:
            pass

    title = (it.get("title") or "").strip()
    series_title = (it.get("series_title") or "").strip()
    query_title = series_title if (is_episode or is_season) and series_title else title
    strict = bool(plex_cfg_get(adapter, "strict_id_matching", False))
    if not query_title and not ids and not show_ids:
        return None

    year = it.get("year")
    season = it.get("season") or it.get("season_number")
    episode = it.get("episode") or it.get("episode_number")

    allow = plex_feature_library_ids(adapter, "ratings")
    sec_types = ("show",) if (is_episode or is_season or is_show) else ("movie",)

    hits: list[Any] = []

    if ids or (show_ids and (is_episode or is_season)):
        try:
            guids = item_guid_candidates(ids, show_ids if (is_episode or is_season) else {}, it)
            rk_any = server_find_rating_key_by_guid(srv, guids)
        except Exception:
            rk_any = None
        if rk_any:
            try:
                obj = srv.fetchItem(int(rk_any))
                if obj and _accept_obj(obj):
                    if section_allowed(obj, allow):
                        hits.append(obj)
            except Exception:
                pass

    if not strict and query_title:
        for sec in adapter.libraries(types=sec_types) or []:
            sid = str(getattr(sec, "key", "")).strip()
            if allow and sid not in allow:
                continue
            try:
                found = sec.search(title=query_title) or []
                for o in found:
                    if _accept_obj(o):
                        hits.append(o)
            except Exception:
                continue

        if not hits:
            try:
                med = "episode" if is_episode else ("season" if is_season else ("show" if is_show else "movie"))
                hs = srv.search(query_title, mediatype=med) or []
                for o in hs:
                    if not _accept_obj(o):
                        continue
                    if not section_allowed(o, allow):
                        continue
                    hits.append(o)
            except Exception:
                pass

    if not hits:
        return None

    def _score(obj: Any) -> int:
        sc = 0
        try:
            if is_episode:
                ot = getattr(obj, "grandparentTitle", None) or ""
            elif is_season:
                ot = getattr(obj, "parentTitle", None) or getattr(obj, "grandparentTitle", None) or ""
            else:
                ot = getattr(obj, "title", None) or ""
            if ot.strip().lower() == query_title.lower():
                sc += 3
            if year is not None and getattr(obj, "year", None) == year:
                sc += 2
            if is_episode:
                s_ok = (season is None) or (
                    getattr(obj, "seasonNumber", None) == season or getattr(obj, "parentIndex", None) == season
                )
                e_ok = (episode is None) or (getattr(obj, "index", None) == episode)
                if s_ok and e_ok:
                    sc += 2
            if is_season:
                s_ok = (season is None) or (
                    getattr(obj, "index", None) == season or getattr(obj, "seasonNumber", None) == season
                )
                if s_ok:
                    sc += 2
            norm = plex_normalize(obj) or {}
            mids = norm.get("ids") or {}
            for k in ("tmdb", "imdb", "tvdb"):
                if k in mids and k in ids and mids[k] == ids[k]:
                    sc += 4
        except Exception:
            pass
        return sc

    if is_episode:
        ep_hits = [o for o in hits if _otype(o) == "episode"]
        if ep_hits:
            best_ep = max(ep_hits, key=_score)
            rk2 = getattr(best_ep, "ratingKey", None)
            return str(rk2) if rk2 else None
        show_hits = [o for o in hits if _otype(o) == "show"]
        for show in sorted(show_hits, key=_score, reverse=True):
            rk2 = episode_rating_key_from_show(show, season, episode)
            if rk2:
                return rk2
        return None

    if is_season:
        sn_hits = [o for o in hits if _otype(o) == "season"]
        if sn_hits:
            best_sn = max(sn_hits, key=_score)
            rk2 = getattr(best_sn, "ratingKey", None)
            return str(rk2) if rk2 else None
        show_hits = [o for o in hits if _otype(o) == "show"]
        for show in sorted(show_hits, key=_score, reverse=True):
            rk2 = season_rating_key_from_show(show, season)
            if rk2:
                return rk2
        return None

    if is_show:
        show_hits = [o for o in hits if _otype(o) == "show"]
        if not show_hits:
            return None
        best = max(show_hits, key=_score)
        rk2 = getattr(best, "ratingKey", None)
        return str(rk2) if rk2 else None

    mv_hits = [o for o in hits if _otype(o) == "movie"]
    if not mv_hits:
        return None
    best = max(mv_hits, key=_score)
    rk2 = getattr(best, "ratingKey", None)
    return str(rk2) if rk2 else None

def _rate(srv: Any, rating_key: Any, rating_1to10: int) -> bool:
    try:
        url = srv.url("/:/rate")
        params = {"key": int(rating_key), "identifier": "com.plexapp.plugins.library", "rating": int(rating_1to10)}
        r = srv._session.get(url, params=params, timeout=10)
        return r.ok
    except Exception:
        return False

def build_index(adapter: Any, limit: int | None = None) -> dict[str, dict[str, Any]]:
    need_scope, did_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            raise RuntimeError("PLEX server not bound")
    
        prog_mk = getattr(adapter, "progress_factory", None)
        prog: Any = prog_mk("ratings") if callable(prog_mk) else None
    
        if plex_cfg_get(adapter, "fallback_GUID", False) or plex_cfg_get(adapter, "fallback_guid", False):
            _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "ratings"})
        fallback_guid = bool(plex_cfg_get(adapter, "fallback_GUID", False) or plex_cfg_get(adapter, "fallback_guid", False))
    
        base = _as_base_url(srv)
        if not base:
            base = str(getattr(srv, "baseurl", None) or getattr(srv, "_baseurl", None) or "").strip().rstrip("/")
    
        client = getattr(adapter, "client", None)
        ses = getattr(srv, "_session", None) or getattr(client, "session", None)
    
        tok, tok_source = _preferred_pms_token(adapter)
        configure_plex_context(baseurl=base, token=tok)

        cli = getattr(adapter, "client", None)
    
    
        if not (base and tok and ses):
            raise RuntimeError(f"PLEX ratings fast query unavailable (base={bool(base)} tok={bool(tok)} ses={bool(ses)})")
    
        hdrs = plex_headers(tok)
        tmo = float(plex_cfg_get(adapter, "timeout", 10) or 10)
        page_size = int(plex_cfg_get(adapter, "ratings_page_size", 120) or 120)
        page_size = max(10, min(page_size, 200))
        workers = plex_worker_count(adapter, "rating_workers", "CW_PLEX_RATING_WORKERS", 12)

        allow = plex_feature_library_ids(adapter, "ratings")
    
        out: dict[str, dict[str, Any]] = {}
        added = 0
        scanned = 0
        total = 0
        fb_try = 0
        fb_ok = 0
    
        if prog is not None:
            try:
                prog.tick(0, total=0, force=True)
            except Exception:
                pass
    
        # Plex library types: 1=movie, 2=show, 3=season, 4=episode
        type_hint = {1: "movie", 2: "show", 3: "season", 4: "episode"}

        show_ids_cache: dict[str, dict[str, Any]] = {}
        show_ids_lock = Lock()

        def _show_ids_for_rating_key(rk: Any) -> dict[str, Any]:
            rk_s = str(rk or "").strip()
            if not rk_s:
                return {}
            with show_ids_lock:
                if rk_s in show_ids_cache:
                    return dict(show_ids_cache[rk_s])
            try:
                obj = srv.fetchItem(int(rk_s))
            except Exception:
                with show_ids_lock:
                    show_ids_cache[rk_s] = {}
                return {}
    
            norm = plex_normalize(obj) or {}
            ids0 = dict((norm.get("ids") or {}) if isinstance(norm, Mapping) else {})
            guid = getattr(obj, "guid", None)
            try:
                if isinstance(guid, Mapping):
                    ids0.update(ids_from(cast(Mapping[str, Any], guid)) or {})
                elif isinstance(guid, str) and guid.strip():
                    ids0.update(ids_from({"guid": guid.strip()}) or {})
            except Exception:
                pass
    
            out_ids: dict[str, Any] = {}
            for k in ("tmdb", "imdb", "tvdb"):
                v = ids0.get(k)
                if v:
                    out_ids[k] = str(v)
            with show_ids_lock:
                show_ids_cache[rk_s] = dict(out_ids)
            return out_ids
    
        def _tick(force: bool = False) -> None:
            if prog is None:
                return
            try:
                prog.tick(scanned, total=max(total, scanned) if total else None, force=force)
            except Exception:
                pass
    
        section_specs: list[tuple[str, tuple[int, ...]]] = []
        try:
            for sec in adapter.libraries(types=("movie", "show")) or []:
                sid = str(getattr(sec, "key", "") or "").strip()
                if not sid:
                    continue
                if allow and sid not in allow:
                    continue
                stype = str(getattr(sec, "type", "") or "").strip().lower()
                if stype == "movie":
                    section_specs.append((sid, (1,)))
                elif stype == "show":
                    section_specs.append((sid, (2, 3, 4)))
        except Exception:
            section_specs = []

        def _iter_rating_rows(path: str, tnum: int) -> Iterable[Mapping[str, Any]]:
            nonlocal total
            start = 0
            while True:
                params = {
                    "type": int(tnum),
                    "includeGuids": 1,
                    "includeUserState": 1,
                    "sort": "lastRatedAt:desc",
                    "X-Plex-Container-Start": start,
                    "X-Plex-Container-Size": page_size,
                    "userRating>>": 0,
                }
                r = ses.get(path, params=params, headers=hdrs, timeout=tmo)
                if not r.ok:
                    raise RuntimeError(f"PLEX ratings fast query failed (status={r.status_code})")

                cont = _container_from_plex_response(r)
                if not cont:
                    head = (r.text or "")[:140].replace("\n", " ")
                    raise RuntimeError(f"PLEX ratings fast query parse failed (ct={(r.headers or {}).get('Content-Type')}; head={head!r})")

                mc = cont.get("MediaContainer") or {}
                if start == 0:
                    try:
                        total += int(mc.get("totalSize") or 0)
                    except Exception:
                        pass
                    _tick(force=True)

                rows = mc.get("Metadata") or []
                if not rows:
                    break
                for row in rows:
                    if isinstance(row, Mapping):
                        yield cast(Mapping[str, Any], row)
                if len(rows) < page_size:
                    break
                start += len(rows)

        def _consume_rows(rows: list[Mapping[str, Any]], tnum: int) -> bool:
            nonlocal scanned, added, fb_try, fb_ok
            def _process_rating_row(row: Mapping[str, Any]) -> tuple[str, dict[str, Any], int, int] | None:
                # /library/all is global; enforce library allow-list here.
                sid = row.get("librarySectionID") or row.get("sectionID") or row.get("librarySectionId") or row.get("sectionId")
                sid_s = str(sid).strip() if sid is not None else ""
                if allow and sid_s and sid_s not in allow:
                    return None

                rating = _norm_rating(row.get("userRating"))
                if not rating or rating <= 0:
                    return None

                m = normalize_discover_row(row, token=tok) or {}
                if not m:
                    return None

                m = dict(m)
                m["rating"] = rating
                ts = _as_epoch(row.get("lastRatedAt"))
                if ts:
                    m["rated_at"] = _iso(ts)
                m["type"] = str(m.get("type") or type_hint.get(tnum) or "movie").lower()

                if m["type"] in ("season", "episode") and not m.get("show_ids"):
                    show_rk = row.get("parentRatingKey") if m["type"] == "season" else row.get("grandparentRatingKey")
                    if show_rk is None:
                        show_rk = row.get("grandparentRatingKey") or row.get("parentRatingKey")
                    show_ids = _show_ids_for_rating_key(show_rk)
                    if show_ids:
                        m["show_ids"] = show_ids
                        if show_ids.get("imdb"):
                            ids0 = dict(m.get("ids") or {})
                            ids0.setdefault("imdb", show_ids["imdb"])
                            m["ids"] = ids0

                fb_try_local = 0
                fb_ok_local = 0
                if fallback_guid and not has_external_ids(m.get("ids") or {}):
                    fb_try_local += 1
                    try:
                        fb = minimal_from_history_row(row, token=tok, allow_discover=True)
                    except Exception:
                        fb = None
                    if isinstance(fb, Mapping):
                        ids_fb = dict(fb.get("ids") or {})
                        show_ids_fb = dict(fb.get("show_ids") or {})
                    else:
                        ids_fb = {}
                        show_ids_fb = {}
                    if ids_fb or show_ids_fb:
                        fb_ok_local += 1
                        ids0 = dict(m.get("ids") or {})
                        ids0.update({k: v for k, v in ids_fb.items() if v})
                        m["ids"] = ids0
                        if show_ids_fb:
                            si0 = dict(m.get("show_ids") or {})
                            si0.update({k: v for k, v in show_ids_fb.items() if v})
                            m["show_ids"] = si0
                _force_episode_title(m)

                k = canonical_key(m)
                if not k:
                    return None
                return k, m, fb_try_local, fb_ok_local

            if workers > 1 and len(rows) > 1:
                executor = ThreadPoolExecutor(max_workers=workers, thread_name_prefix="plex-ratings")
                try:
                    result_iter = executor.map(_process_rating_row, rows)
                    for result in result_iter:
                        scanned += 1
                        if result:
                            k, m, fb_try_local, fb_ok_local = result
                            out[k] = m
                            added += 1
                            fb_try += fb_try_local
                            fb_ok += fb_ok_local
                            if limit is not None and added >= limit:
                                if prog is not None:
                                    try:
                                        prog.done(ok=True, total=max(total, scanned) if total else None)
                                    except Exception:
                                        pass
                                _info("index_done", count=len(out), added=added, scanned=scanned, fb_try=fb_try, fb_ok=fb_ok, workers=workers, truncated=True, limit=limit)
                                return True
                        _tick()
                finally:
                    executor.shutdown(wait=True, cancel_futures=False)
            else:
                for row in rows:
                    scanned += 1
                    result = _process_rating_row(row)
                    if result:
                        k, m, fb_try_local, fb_ok_local = result
                        out[k] = m
                        added += 1
                        fb_try += fb_try_local
                        fb_ok += fb_ok_local
                        if limit is not None and added >= limit:
                            if prog is not None:
                                try:
                                    prog.done(ok=True, total=max(total, scanned) if total else None)
                                except Exception:
                                    pass
                            _info("index_done", count=len(out), added=added, scanned=scanned, fb_try=fb_try, fb_ok=fb_ok, workers=workers, truncated=True, limit=limit)
                            return True
                    _tick()
            return False

        try:
            for tnum in (1, 2, 3, 4):
                rows = list(_iter_rating_rows(f"{base}/library/all", tnum))
                if _consume_rows(rows, tnum):
                    return out
        except RuntimeError as e:
            if "status=401" not in str(e) and "status=403" not in str(e):
                raise
            _warn("fast_query_fallback", mode="section_scan", reason=str(e))
            if not section_specs:
                raise

            total = 0
            if prog is not None:
                try:
                    prog.tick(0, total=0, force=True)
                except Exception:
                    pass
            for sid, tnums in section_specs:
                for tnum in tnums:
                    rows = list(_iter_rating_rows(f"{base}/library/sections/{sid}/all", tnum))
                    if _consume_rows(rows, tnum):
                        return out
    
        if prog is not None:
            try:
                prog.done(ok=True, total=max(total, scanned) if total else None)
            except Exception:
                pass
    
        _info("index_done", count=len(out), added=added, scanned=scanned, fb_try=fb_try, fb_ok=fb_ok, workers=workers)
        return out
    finally:
        home_scope_exit(adapter, did_switch)

def _get_existing_rating(srv: Any, rating_key: Any) -> int | None:
    try:
        it = srv.fetchItem(int(rating_key))
    except Exception:
        return None
    return _norm_rating(getattr(it, "userRating", None))

def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    _, did_switch, _, _ = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            unresolved: list[dict[str, Any]] = []
            for it in items or []:
                _UNRES.freeze(it, action="add", reasons=["no_plex_server"])
                unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
            _info("write_skipped", op="add", reason="no_server")
            return 0, unresolved
    
        ok = 0
        unresolved: list[dict[str, Any]] = []
    
        for it in items or []:
            if _UNRES.is_frozen(it):
                _dbg("skip_frozen", title=id_minimal(it).get("title"))
                continue
    
            rating = _norm_rating(it.get("rating"))
            if rating is None or rating <= 0:
                _UNRES.freeze(it, action="add", reasons=["missing_or_invalid_rating"])
                unresolved.append({"item": id_minimal(it), "hint": "missing_or_invalid_rating"})
                continue
    
            rk = _resolve_rating_key(adapter, it)
            if not rk:
                _UNRES.freeze(it, action="add", reasons=["not_in_library"])
                unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
                continue
    
            existing = _get_existing_rating(srv, rk)
            if existing is not None and existing == rating:
                _dbg("skip_same_rating", title=id_minimal(it).get("title"))
                _UNRES.unfreeze([_UNRES.event_key(it)])
                continue
    
            if _rate(srv, rk, rating):
                ok += 1
                _UNRES.unfreeze([_UNRES.event_key(it)])
            else:
                _UNRES.freeze(it, action="add", reasons=["rate_failed"])
                unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})
    
        _info("write_done", op="add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
        return ok, unresolved


    finally:
        home_scope_exit(adapter, did_switch)
def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    _, did_switch, _, _ = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            unresolved: list[dict[str, Any]] = []
            for it in items or []:
                _UNRES.freeze(it, action="remove", reasons=["no_plex_server"])
                unresolved.append({"item": id_minimal(it), "hint": "no_plex_server"})
            _info("write_skipped", op="remove", reason="no_server")
            return 0, unresolved
    
        ok = 0
        unresolved: list[dict[str, Any]] = []
    
        for it in items or []:
            if _UNRES.is_frozen(it):
                _dbg("skip_frozen", title=id_minimal(it).get("title"))
                continue
    
            rk = _resolve_rating_key(adapter, it)
            if not rk:
                _UNRES.freeze(it, action="remove", reasons=["not_in_library"])
                unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
                continue
    
            if _rate(srv, rk, 0):
                ok += 1
                _UNRES.unfreeze([_UNRES.event_key(it)])
            else:
                _UNRES.freeze(it, action="remove", reasons=["clear_failed"])
                unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})
    
        _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
        return ok, unresolved
    finally:
        home_scope_exit(adapter, did_switch)

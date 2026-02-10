# /providers/sync/plex/_ratings.py
# Plex Module for ratings synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any, Iterable, Mapping, cast

from ._common import (
    _as_base_url,
    _xml_to_container,
    as_epoch as _as_epoch,
    candidate_guids_from_ids,
    configure_plex_context,
    force_episode_title as _force_episode_title,
    home_scope_enter,
    home_scope_exit,
    iso_from_epoch as _iso,
    minimal_from_history_row,
    normalize as plex_normalize,
    normalize_discover_row,
    plex_headers,
    server_find_rating_key_by_guid,
    sort_guid_candidates,
    unresolved_store,
    emit,
    make_logger,
    _plex_cfg,
)

_UNRES = unresolved_store("ratings")

from cw_platform.id_map import canonical_key, minimal as id_minimal, ids_from

_dbg, _info, _warn, _error, _log = make_logger("ratings")


def _emit(evt: dict[str, Any]) -> None:
    emit(evt, default_feature="ratings")

def _plex_cfg_get(adapter: Any, key: str, default: Any = None) -> Any:
    c = _plex_cfg(adapter)
    v = c.get(key, default) if isinstance(c, Mapping) else default
    return default if v is None else v

def _allowed_ratings_sec_ids(adapter: Any) -> set[str]:
    try:
        cfg = getattr(adapter, "config", {}) or {}
        plex = cfg.get("plex", {}) if isinstance(cfg, dict) else {}
        arr = (plex.get("ratings") or {}).get("libraries") or []
        return {str(int(x)) for x in arr if str(x).strip()}
    except Exception:
        return set()

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

def _active_pms_token(adapter: Any) -> str | None:
    cli = getattr(adapter, "client", None)
    try:
        ses = getattr(cli, "session", None)
        tok = ses.headers.get("X-Plex-Token") if ses and hasattr(ses, "headers") else None
        if tok and str(tok).strip():
            return str(tok).strip()
    except Exception:
        pass
    srv = getattr(cli, "server", None)
    try:
        tok = getattr(srv, "_token", None) or getattr(srv, "token", None)
        if tok and str(tok).strip():
            return str(tok).strip()
    except Exception:
        pass
    try:
        tok = getattr(getattr(cli, "cfg", None), "token", None)
        if tok and str(tok).strip():
            return str(tok).strip()
    except Exception:
        pass
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

def _has_ext_ids(m: Mapping[str, Any]) -> bool:
    ids = (m.get("ids") if isinstance(m, Mapping) else None) or {}
    return bool(ids.get("imdb") or ids.get("tmdb") or ids.get("tvdb"))

def _event_key(it: Mapping[str, Any]) -> str:
    return unresolved_store("ratings").event_key(it)

def _season_rk_from_show(show_obj: Any, season: Any) -> str | None:
    try:
        try:
            s_target = int(season) if season is not None else None
        except Exception:
            s_target = season
        if s_target is None:
            return None
        try:
            seasons = show_obj.seasons() or []
        except Exception:
            seasons = []
        for sn in seasons:
            try:
                idx = getattr(sn, "index", None)
                if idx is not None and int(idx) == int(s_target):
                    rk = getattr(sn, "ratingKey", None)
                    return str(rk) if rk else None
            except Exception:
                continue
    except Exception:
        pass
    return None

def _episode_rk_from_show(show_obj: Any, season: Any, episode: Any) -> str | None:
    try:
        try:
            s_target = int(season) if season is not None else None
        except Exception:
            s_target = season
        try:
            e_target = int(episode) if episode is not None else None
        except Exception:
            e_target = episode

        try:
            episodes = show_obj.episodes() or []
        except Exception:
            episodes = []
        for ep in episodes:
            try:
                s_ok = (
                    s_target is None
                    or getattr(ep, "parentIndex", None) == s_target
                    or getattr(ep, "seasonNumber", None) == s_target
                )
                e_ok = e_target is None or getattr(ep, "index", None) == e_target
                if s_ok and e_ok:
                    rk = getattr(ep, "ratingKey", None)
                    return str(rk) if rk else None
            except Exception:
                continue
    except Exception:
        pass
    return None

def _resolve_rating_key(adapter: Any, it: Mapping[str, Any]) -> str | None:
    if not isinstance(it, Mapping):
        return None
    ids = ids_from(cast(Mapping[str, Any], it))
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
    strict = bool(_plex_cfg_get(adapter, "strict_id_matching", False))
    if not query_title and not ids:
        return None

    year = it.get("year")
    season = it.get("season") or it.get("season_number")
    episode = it.get("episode") or it.get("episode_number")

    allow = _allowed_ratings_sec_ids(adapter)
    sec_types = ("show",) if (is_episode or is_season or is_show) else ("movie",)

    hits: list[Any] = []

    if ids:
        try:
            guids = sort_guid_candidates(candidate_guids_from_ids({"ids": ids}))
            rk_any = server_find_rating_key_by_guid(srv, guids)
        except Exception:
            rk_any = None
        if rk_any:
            try:
                obj = srv.fetchItem(int(rk_any))
                if obj and _accept_obj(obj):
                    sid = str(getattr(obj, "librarySectionID", "") or getattr(obj, "sectionID", "") or "")
                    if not allow or not sid or sid in allow:
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
                    sid = str(getattr(o, "librarySectionID", "") or getattr(o, "sectionID", "") or "")
                    if allow and sid and sid not in allow:
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
            for k in ("imdb", "tmdb", "tvdb"):
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
            rk2 = _episode_rk_from_show(show, season, episode)
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
            rk2 = _season_rk_from_show(show, season)
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
    _, did_switch, _, _ = home_scope_enter(adapter)
    try:
        srv = getattr(getattr(adapter, "client", None), "server", None)
        if not srv:
            raise RuntimeError("PLEX server not bound")
    
        prog_mk = getattr(adapter, "progress_factory", None)
        prog: Any = prog_mk("ratings") if callable(prog_mk) else None
    
        plex_cfg = _plex_cfg(adapter)
        if plex_cfg.get("fallback_GUID") or plex_cfg.get("fallback_guid"):
            _emit({"event": "debug", "msg": "fallback_guid.enabled", "provider": "PLEX", "feature": "ratings"})
        fallback_guid = bool(_plex_cfg_get(adapter, "fallback_GUID", False) or _plex_cfg_get(adapter, "fallback_guid", False))
    
        base = _as_base_url(srv)
        if not base:
            base = str(getattr(srv, "baseurl", None) or getattr(srv, "_baseurl", None) or "").strip().rstrip("/")
    
        client = getattr(adapter, "client", None)
        ses = getattr(srv, "_session", None) or getattr(client, "session", None)
    
        tok = str(_active_pms_token(adapter) or "").strip()
        configure_plex_context(baseurl=base, token=tok)
    
    
        if not (base and tok and ses):
            raise RuntimeError(f"PLEX ratings fast query unavailable (base={bool(base)} tok={bool(tok)} ses={bool(ses)})")
    
        hdrs = plex_headers(tok)
        tmo = float(_plex_cfg_get(adapter, "timeout", 10) or 10)
        page_size = int(_plex_cfg_get(adapter, "ratings_page_size", 120) or 120)
        page_size = max(10, min(page_size, 200))
    
        allow = _allowed_ratings_sec_ids(adapter)
    
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
    
        def _show_ids_for_rating_key(rk: Any) -> dict[str, Any]:
            rk_s = str(rk or "").strip()
            if not rk_s:
                return {}
            if rk_s in show_ids_cache:
                return show_ids_cache[rk_s]
            try:
                obj = srv.fetchItem(int(rk_s))
            except Exception:
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
            for k in ("imdb", "tmdb", "tvdb"):
                v = ids0.get(k)
                if v:
                    out_ids[k] = str(v)
            show_ids_cache[rk_s] = out_ids
            return out_ids
    
        def _tick(force: bool = False) -> None:
            if prog is None:
                return
            try:
                prog.tick(scanned, total=max(total, scanned) if total else None, force=force)
            except Exception:
                pass
    
        for tnum in (1, 2, 3, 4):
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
                r = ses.get(f"{base}/library/all", params=params, headers=hdrs, timeout=tmo)
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
                    scanned += 1
    
                    # /library/all is global; enforce library allow-list here.
                    sid = row.get("librarySectionID") or row.get("sectionID") or row.get("librarySectionId") or row.get("sectionId")
                    sid_s = str(sid).strip() if sid is not None else ""
                    if allow and sid_s and sid_s not in allow:
                        _tick()
                        continue
    
                    rating = _norm_rating(row.get("userRating"))
                    if not rating or rating <= 0:
                        _tick()
                        continue
    
                    m = normalize_discover_row(row, token=tok) or {}
                    if not m:
                        _tick()
                        continue
    
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
    
                    # Keep fallback GUID enrichment intact.
                    if fallback_guid and not _has_ext_ids(m):
                        fb_try += 1
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
                            fb_ok += 1
                            ids0 = dict(m.get("ids") or {})
                            ids0.update({k: v for k, v in ids_fb.items() if v})
                            m["ids"] = ids0
                            if show_ids_fb:
                                si0 = dict(m.get("show_ids") or {})
                                si0.update({k: v for k, v in show_ids_fb.items() if v})
                                m["show_ids"] = si0
                    _force_episode_title(m)
    
                    k = canonical_key(m)
                    if k:
                        out[k] = m
                        added += 1
                        if limit is not None and added >= limit:
                            if prog is not None:
                                try:
                                    prog.done(ok=True, total=max(total, scanned) if total else None)
                                except Exception:
                                    pass
                            _info("index_truncated", limit=limit)
                            _info("index_done", count=len(out), added=added, scanned=scanned, fb_try=fb_try, fb_ok=fb_ok)
                            return out
    
                    _tick()
    
                if len(rows) < page_size:
                    break
                start += len(rows)
    
        if prog is not None:
            try:
                prog.done(ok=True, total=max(total, scanned) if total else None)
            except Exception:
                pass
    
        _info("index_done", count=len(out), added=added, scanned=scanned, fb_try=fb_try, fb_ok=fb_ok)
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
                _UNRES.unfreeze([_event_key(it)])
                continue
    
            if _rate(srv, rk, rating):
                ok += 1
                _UNRES.unfreeze([_event_key(it)])
            else:
                _UNRES.freeze(it, action="add", reasons=["rate_failed"])
                unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})
    
        _info("write_done", op="add", ok=ok, unresolved=len(unresolved))
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
                _UNRES.unfreeze([_event_key(it)])
            else:
                _UNRES.freeze(it, action="remove", reasons=["clear_failed"])
                unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})
    
        _info("write_done", op="remove", ok=ok, unresolved=len(unresolved))
        return ok, unresolved
    finally:
        home_scope_exit(adapter, did_switch)

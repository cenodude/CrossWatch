#/providers/scrobble/plextrakt.py
# CrossWatch - Plex Trakt Scrobble Webhook Module
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
import base64, hashlib, hmac, json, re, time, requests, threading
from typing import Any, Dict, Mapping, Optional, Callable, Iterable
import xml.etree.ElementTree as ET
try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None
    
from providers.scrobble.currently_watching import update_from_payload as _cw_update

TRAKT_API = "https://api.trakt.tv"
_SCROBBLE_STATE: Dict[str, Dict[str, Any]] = {}
_TRAKT_ID_CACHE: Dict[tuple, Any] = {}
_LAST_FINISH_BY_ACC: Dict[str, Dict[str, Any]] = {}

_PAT_IMDB = re.compile(r"(?:com\.plexapp\.agents\.imdb|imdb)://(tt\d+)", re.I)
_PAT_TMDB = re.compile(r"(?:com\.plexapp\.agents\.tmdb|tmdb)://(\d+)", re.I)
_PAT_TVDB = re.compile(r"(?:com\.plexapp\.agents\.thetvdb|thetvdb|tvdb)://(\d+)", re.I)

_DEF_WEBHOOK = {
    "pause_debounce_seconds": 5,
    "suppress_start_at": 99,
    "suppress_autoplay_seconds": 12,
    "filters_plex": {"username_whitelist": [], "server_uuid": ""},
    "probe_session_progress": True,
}
_DEF_TRAKT = {"stop_pause_threshold": 80, "force_stop_at": 95, "regress_tolerance_percent": 5}

from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from _watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except Exception:
    _rm_across_api = None

def _call_remove_across(ids: Dict[str, Any], media_type: str) -> None:
    if not isinstance(ids, dict) or not ids: return
    try:
        cfg = _load_config()
        s = (cfg.get("scrobble") or {})
        if not s.get("delete_plex"): return
        tps = s.get("delete_plex_types") or []
        mt = (media_type or "").strip().lower()
        allow = False
        if isinstance(tps, list):
            allow = (mt in tps) or ((mt.rstrip("s") + "s") in tps)
        elif isinstance(tps, str):
            allow = mt in tps
        if not allow: return
    except Exception:
        pass
    try:
        if callable(_rm_across): _rm_across(ids, media_type); return
    except Exception:
        pass
    try:
        if callable(_rm_across_api): _rm_across_api(ids, media_type); return
    except Exception:
        pass

def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)

def _save_config(cfg: Dict[str, Any]) -> None:
    try:
        from crosswatch import save_config as _save
        _save(cfg)
    except Exception:
        with open("config.json", "w", encoding="utf-8") as f:
            json.dump(cfg, f, indent=2)

def _is_debug() -> bool:
    try:
        rt = (_load_config().get("runtime") or {})
        return bool(rt.get("debug") or rt.get("debug_mods"))
    except Exception:
        return False

def _emit(logger: Optional[object], msg: str, level: str = "INFO"):
    lvl_raw = str(level or "INFO")
    lvl_up = lvl_raw.upper()
    try:
        if lvl_up == "DEBUG" and not _is_debug():
            return
    except Exception:
        pass
    try:
        if logger is not None:
            if callable(logger):
                logger(msg, level=lvl_raw, module="SCROBBLE")
                return

            logmeth = getattr(logger, "log", None)
            if callable(logmeth):
                lvlno = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}.get(lvl_up, 20)
                logmeth(lvlno, msg)
                return

            levmeth = getattr(logger, lvl_raw.lower(), None)
            if callable(levmeth):
                levmeth(msg)
                return
    except Exception:
        pass
    try:
        if BASE_LOG:
            logr = BASE_LOG.child("SCROBBLE")
            if lvl_up == "DEBUG":
                logr.debug(msg)
            elif lvl_up == "INFO":
                logr.info(msg)
            elif lvl_up == "WARN":
                logr.warn(msg)
            elif lvl_up == "ERROR":
                logr.error(msg)
            else:
                logr(msg, level=lvl_raw)
            return
    except Exception:
        pass
    try:
        print(f"[SCROBBLE] {lvl_up} {msg}")
    except Exception:
        pass

def _ensure_scrobble(cfg: Dict[str, Any]) -> Dict[str, Any]:
    changed = False
    sc = cfg.setdefault("scrobble", {})
    wh = sc.setdefault("webhook", {})
    trk = sc.setdefault("trakt", {})
    if "pause_debounce_seconds" not in wh:
        wh["pause_debounce_seconds"] = _DEF_WEBHOOK["pause_debounce_seconds"]; changed = True
    if "suppress_start_at" not in wh:
        wh["suppress_start_at"] = _DEF_WEBHOOK["suppress_start_at"]; changed = True
    if "suppress_autoplay_seconds" not in wh:
        wh["suppress_autoplay_seconds"] = _DEF_WEBHOOK["suppress_autoplay_seconds"]; changed = True
    if "probe_session_progress" not in wh:
        wh["probe_session_progress"] = _DEF_WEBHOOK["probe_session_progress"]; changed = True
    flt = wh.setdefault("filters_plex", {})
    if "username_whitelist" not in flt:
        flt["username_whitelist"] = []; changed = True
    if "server_uuid" not in flt:
        flt["server_uuid"] = ""; changed = True
    for k, dv in _DEF_TRAKT.items():
        if k not in trk: trk[k] = dv; changed = True
    if changed: _save_config(cfg)
    return cfg

def _tokens(cfg: Dict[str, Any]) -> Dict[str, str]:
    tr = cfg.get("trakt") or {}
    au = ((cfg.get("auth") or {}).get("trakt") or {})
    return {
        "client_id": (tr.get("client_id") or "").strip(),
        "client_secret": (tr.get("client_secret") or "").strip(),
        "access_token": (au.get("access_token") or tr.get("access_token") or "").strip(),
        "refresh_token": (au.get("refresh_token") or tr.get("refresh_token") or "").strip(),
    }

def _app_meta(cfg: Dict[str, Any]) -> Dict[str, str]:
    rt = (cfg.get("runtime") or {})
    av = str(rt.get("version") or "CrossWatch/Scrobble")
    ad = (rt.get("build_date") or "").strip()
    return {"app_version": av, **({"app_date": ad} if ad else {})}

def _headers(cfg: Dict[str, Any]) -> Dict[str, str]:
    t = _tokens(cfg)
    h = {
        "Content-Type": "application/json",
        "Accept": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": t["client_id"],
        "User-Agent": "CrossWatch/Scrobble",
    }
    if t["access_token"]:
        h["Authorization"] = f"Bearer {t['access_token']}"
    return h

def _del_trakt(path: str, cfg: Dict[str, Any]) -> requests.Response:
    url = f"{TRAKT_API}{path}"
    r = requests.delete(url, headers=_headers(cfg), timeout=12)
    if r.status_code == 401:
        try:
            from providers.auth._auth_TRAKT import PROVIDER as TRAKT_AUTH
            TRAKT_AUTH.refresh(cfg)
            _save_config(cfg)
        except Exception:
            return r
        try:
            r = requests.delete(url, headers=_headers(cfg), timeout=12)
        except Exception:
            pass
    return r

def _get_trakt_watching(cfg: Dict[str, Any]) -> None:
    try:
        r = requests.get(f"{TRAKT_API}/users/me/watching", headers=_headers(cfg), timeout=8)
        try: body = r.json()
        except Exception: body = (r.text or "")[:200]
        _emit(None, f"trakt watching {r.status_code}: {str(body)[:200]}", "DEBUG")
    except Exception as e:
        _emit(None, f"trakt watching check error: {e}", "DEBUG")

def _post_trakt(path: str, body: Dict[str, Any], cfg: Dict[str, Any]) -> requests.Response:
    url = f"{TRAKT_API}{path}"
    body = {**body, **_app_meta(cfg)}
    r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    if r.status_code == 401:
        try:
            from providers.auth._auth_TRAKT import PROVIDER as TRAKT_AUTH
            TRAKT_AUTH.refresh(cfg); _save_config(cfg)
        except Exception:
            pass
        r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    if r.status_code == 409:
        if _is_debug():
            _get_trakt_watching(cfg)
        txt = (r.text or "")
        if ("expires_at" in txt or "watched_at" in txt):
            try:
                _del_trakt("/checkin", cfg)
                time.sleep(0.35)
            except Exception:
                pass
            r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
            if _is_debug() and r.status_code == 409:
                _get_trakt_watching(cfg)
    if r.status_code in (429, 500, 502, 503, 504):
        try: ra = float(r.headers.get("Retry-After") or "1")
        except Exception: ra = 1.0
        time.sleep(min(max(ra, 0.5), 3.0))
        r = requests.post(url, json=body, headers=_headers(cfg), timeout=15)
    return r

def _ids_from_candidates_show_first(candidates: Iterable[Any]) -> Dict[str, Any]:
    for c in candidates:
        if not c: continue
        s = str(c)
        m = _PAT_TVDB.search(s)
        if m: return {"tvdb": int(m.group(1))}
        m = _PAT_TMDB.search(s)
        if m: return {"tmdb": int(m.group(1))}
        m = _PAT_IMDB.search(s)
        if m: return {"imdb": m.group(1)}
    return {}

def _gather_guid_candidates(md: Dict[str, Any]) -> list[str]:
    cand: list[str] = []
    for k in ("guid", "grandparentGuid", "parentGuid"):
        v = md.get(k)
        if v: cand.append(v)
    gi = md.get("Guid") or []
    for g in gi:
        if isinstance(g, dict):
            v = g.get("id")
            if v: cand.append(v)
        elif isinstance(g, str):
            cand.append(g)
    seen, out = set(), []
    for v in cand:
        if v not in seen:
            seen.add(v); out.append(v)
    return out

def _all_ids_from_metadata(md: Dict[str, Any]) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    for s in _gather_guid_candidates(md):
        if not s: continue
        m = _PAT_IMDB.search(s)
        if m: ids.setdefault("imdb", m.group(1))
        m = _PAT_TMDB.search(s)
        if m: ids.setdefault("tmdb", int(m.group(1)))
        m = _PAT_TVDB.search(s)
        if m: ids.setdefault("tvdb", int(m.group(1)))
    return ids

def _episode_ids_from_md(md: Dict[str, Any]) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    s = str(md.get("guid") or "")
    if s:
        m = _PAT_TVDB.search(s)
        if m: ids["tvdb"] = int(m.group(1))
        m = _PAT_TMDB.search(s)
        if m: ids["tmdb"] = int(m.group(1))
        m = _PAT_IMDB.search(s)
        if m: ids["imdb"] = m.group(1)
    gi = md.get("Guid") or []
    for g in gi:
        v = g.get("id") if isinstance(g, dict) else (g if isinstance(g, str) else "")
        if not v: continue
        m = _PAT_TMDB.search(v)
        if m and "tmdb" not in ids: ids["tmdb"] = int(m.group(1))
        m = _PAT_IMDB.search(v)
        if m and "imdb" not in ids: ids["imdb"] = m.group(1)
        m = _PAT_TVDB.search(v)
        if m and "tvdb" not in ids: ids["tvdb"] = int(m.group(1))
    return ids

def _show_ids_from_md(md: Dict[str, Any]) -> Dict[str, Any]:
    ids: Dict[str, Any] = {}
    for k in ("grandparentGuid", "parentGuid"):
        s = md.get(k)
        if not s: continue
        m = _PAT_TMDB.search(s)
        if m and "tmdb" not in ids: ids["tmdb"] = int(m.group(1))
        m = _PAT_IMDB.search(s)
        if m and "imdb" not in ids: ids["imdb"] = m.group(1)
        m = _PAT_TVDB.search(s)
        if m and "tvdb" not in ids: ids["tvdb"] = int(m.group(1))
    return ids

def _plex_show_ids_from_metadata(cfg: Dict[str, Any], md: Dict[str, Any], logger=None) -> Dict[str, Any]:
    try:
        rk_candidates: list[str] = []
        for key in ("grandparentRatingKey", "parentRatingKey"):
            val = md.get(key)
            if val is None:
                continue
            s = str(val).strip()
            if not s:
                continue
            if s not in rk_candidates:
                rk_candidates.append(s)

        if not rk_candidates:
            return {}

        base, token = _plex_base_token(cfg)
        if not token:
            return {}

        for rk in rk_candidates:
            try:
                r = requests.get(
                    f"{base}/library/metadata/{rk}",
                    headers={"X-Plex-Token": token},
                    timeout=5,
                )
            except Exception as e:
                _emit(logger, f"plex show-ids lookup error rk={rk}: {e}", "DEBUG")
                continue
            if r.status_code != 200:
                continue
            try:
                root = ET.fromstring(r.text or "")
            except Exception:
                continue

            guids: list[str] = []
            for g in root.iter("Guid"):
                gid = g.get("id") or ""
                if gid:
                    guids.append(gid)

            if not guids:
                continue

            ids = _ids_from_candidates_show_first(guids)
            if ids:
                _emit(
                    logger,
                    f"plex metadata resolved SHOW ids from rk={rk}: {_describe_ids(ids)}",
                    "DEBUG",
                )
                return ids
        return {}
    except Exception:
        return {}
    
def _cw_ids_for_payload(
    media_type: str,
    md: Dict[str, Any],
    ids_all: Dict[str, Any],
    cfg: Dict[str, Any],
    logger=None,
) -> Dict[str, Any]:
    cw_ids: Dict[str, Any] = dict(ids_all or {})

    mt = (media_type or "").lower()
    if mt != "episode":
        return cw_ids
    try:
        show_ids = _show_ids_from_md(md)
    except Exception:
        show_ids = {}

    for key in ("imdb", "tmdb", "tvdb"):
        val = show_ids.get(key)
        if val is not None:
            cw_ids.setdefault(f"{key}_show", val)

    if "tmdb_show" not in cw_ids:
        extra = _plex_show_ids_from_metadata(cfg, md, logger=logger)
        for key in ("imdb", "tmdb", "tvdb"):
            val = extra.get(key)
            if val is not None:
                cw_ids.setdefault(f"{key}_show", val)

    if "tmdb_show" not in cw_ids and cw_ids.get("imdb_show"):
        extra2 = _trakt_show_ids_from_imdb_show(cw_ids["imdb_show"], cfg, logger=logger)
        for key in ("imdb", "tmdb", "tvdb"):
            val = extra2.get(key)
            if val is not None:
                cw_ids.setdefault(f"{key}_show", val)

    return cw_ids

def _describe_ids(ids: Dict[str, Any] | str) -> str:
    if isinstance(ids, dict):
        if "trakt" in ids: return f"trakt:{ids['trakt']}"
        if "imdb" in ids: return f"imdb:{ids['imdb']}"
        if "tmdb" in ids: return f"tmdb:{ids['tmdb']}"
        if "tvdb" in ids: return f"tvdb:{ids['tvdb']}"
        return "none"
    return str(ids)

def _progress(payload: Dict[str, Any]) -> float:
    md = payload.get("Metadata") or {}
    vo = payload.get("viewOffset") or md.get("viewOffset") or 0
    dur = md.get("duration") or 0
    if not dur: return 0.0
    p = max(0.0, min(100.0, (float(vo) * 100.0) / float(dur)))
    return round(p, 2)

def _plex_base_token(cfg: Dict[str, Any]) -> tuple[str, str]:
    px = cfg.get("plex") or {}
    base = (px.get("server_url") or px.get("base_url") or "http://127.0.0.1:32400").strip().rstrip("/")
    if "://" not in base: base = f"http://{base}"
    return base, (px.get("account_token") or px.get("token") or "")

def _probe_session_progress(cfg: Dict[str, Any], rating_key: Any, session_key: Any) -> Optional[int]:
    try:
        base, token = _plex_base_token(cfg)
        if not token: return None
        r = requests.get(f"{base}/status/sessions", headers={"X-Plex-Token": token}, timeout=5)
        if r.status_code != 200: return None
        root = ET.fromstring(r.text or "")
        rk_str = str(rating_key) if rating_key is not None else ""
        sk_str = str(session_key) if session_key is not None else ""
        for v in root.iter("Video"):
            rk = v.get("ratingKey") or ""
            sk = v.get("sessionKey") or ""
            if (rk_str and rk == rk_str) or (sk_str and sk == sk_str):
                d = int(v.get("duration") or "0") or 0
                vo = int(v.get("viewOffset") or "0") or 0
                if d <= 0: return None
                pct = int(round(100.0 * max(0, min(vo, d)) / float(d)))
                return pct
    except Exception:
        return None
    return None

def _probe_played_status(cfg: Dict[str, Any], rating_key: Any) -> bool:
    if rating_key in (None, "", 0): return False
    try:
        base, token = _plex_base_token(cfg)
        if not token: return False
        r = requests.get(f"{base}/library/metadata/{rating_key}", headers={"X-Plex-Token": token}, timeout=5)
        if r.status_code != 200: return False
        root = ET.fromstring(r.text or "")
        v = root.find(".//Video")
        if v is None: return False
        vc = int(v.get("viewCount") or "0")
        return vc >= 1
    except Exception:
        return False

def _cache_get(key: tuple) -> Optional[Any]:
    try: return _TRAKT_ID_CACHE.get(key)
    except Exception: return None

def _cache_put(key: tuple, value: Any) -> None:
    try:
        if len(_TRAKT_ID_CACHE) > 2048: _TRAKT_ID_CACHE.clear()
        _TRAKT_ID_CACHE[key] = value
    except Exception:
        pass

def _resolve_trakt_movie_id(ids_all: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Optional[int]:
    key = ("movie", ids_all.get("imdb"), ids_all.get("tmdb"), ids_all.get("tvdb"))
    c = _cache_get(key)
    if c is not None: return c
    for k in ("imdb", "tmdb", "tvdb"):
        val = ids_all.get(k)
        if not val: continue
        try:
            r = requests.get(f"{TRAKT_API}/search/{k}/{val}", params={"type": "movie", "limit": 1},
                             headers=_headers(cfg), timeout=10)
            if r.status_code != 200: continue
            arr = r.json() or []
            if not arr: continue
            tid = (((arr[0] or {}).get("movie") or {}).get("ids") or {}).get("trakt")
            if tid:
                _cache_put(key, int(tid)); return int(tid)
        except Exception as e:
            _emit(logger, f"trakt movie id resolve error: {e}", "DEBUG")
    _cache_put(key, None); return None

def _resolve_trakt_show_id(ids_all: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Optional[int]:
    key = ("show", ids_all.get("imdb"), ids_all.get("tmdb"), ids_all.get("tvdb"))
    c = _cache_get(key)
    if c is not None: return c
    for k in ("imdb", "tmdb", "tvdb"):
        val = ids_all.get(k)
        if not val: continue
        try:
            r = requests.get(f"{TRAKT_API}/search/{k}/{val}", params={"type": "show", "limit": 1},
                             headers=_headers(cfg), timeout=10)
            if r.status_code != 200: continue
            arr = r.json() or []
            if not arr: continue
            tid = (((arr[0] or {}).get("show") or {}).get("ids") or {}).get("trakt")
            if tid:
                _cache_put(key, int(tid)); return int(tid)
        except Exception as e:
            _emit(logger, f"trakt show id resolve error: {e}", "DEBUG")
    _cache_put(key, None); return None
    
def _trakt_show_ids_from_imdb_show(imdb_show: str, cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    imdb_show = str(imdb_show or "").strip()
    if not imdb_show:
        return {}

    key = ("show_ids_imdb", imdb_show)
    c = _cache_get(key)
    if isinstance(c, dict):
        return c
    if c is not None:
        return {}

    try:
        r = requests.get(
            f"{TRAKT_API}/search/imdb/{imdb_show}",
            params={"type": "show", "limit": 1},
            headers=_headers(cfg),
            timeout=10,
        )
        if r.status_code != 200:
            _cache_put(key, None)
            return {}
        arr = r.json() or []
        if not arr:
            _cache_put(key, None)
            return {}

        ids = (((arr[0] or {}).get("show") or {}).get("ids") or {})
        out = {k: ids[k] for k in ("trakt", "imdb", "tmdb", "tvdb") if ids.get(k)}

        _cache_put(key, out if out else None)
        if out:
            _emit(logger, f"trakt show ids from imdb_show {imdb_show}: {out}", "DEBUG")
        return out
    except Exception as e:
        _emit(logger, f"trakt show ids from imdb_show {imdb_show} error: {e}", "DEBUG")
        _cache_put(key, None)
        return {}

def _guid_search_episode(ids_hint: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any] | None:
    for key in ("tmdb", "imdb", "tvdb"):
        val = ids_hint.get(key)
        if not val: continue
        try:
            r = requests.get(f"{TRAKT_API}/search/{key}/{val}",
                             params={"type": "episode", "limit": 1},
                             headers=_headers(cfg), timeout=10)
        except Exception:
            continue
        if r.status_code != 200: continue
        try: arr = r.json() or []
        except Exception: arr = []
        for hit in arr:
            epi_ids = ((hit.get("episode") or {}).get("ids") or {})
            out = {k: epi_ids[k] for k in ("trakt", "imdb", "tmdb", "tvdb") if epi_ids.get(k)}
            if out:
                _emit(logger, f"guid search resolved episode ids: {out}", "DEBUG")
                return out
    return None

def _show_ids_from_episode_hint(ids_hint: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    for key in ("tmdb", "imdb", "tvdb"):
        val = ids_hint.get(key)
        if not val:
            continue
        try:
            r = requests.get(f"{TRAKT_API}/search/{key}/{val}",
                             params={"type": "episode", "limit": 1},
                             headers=_headers(cfg), timeout=10)
            if r.status_code != 200:
                continue
            arr = r.json() or []
        except Exception:
            continue
        for hit in arr:
            show_ids = ((hit.get("show") or {}).get("ids") or {})
            out = {k: show_ids[k] for k in ("trakt","tvdb","tmdb","imdb") if show_ids.get(k)}
            if out:
                _emit(logger, f"guid search resolved SHOW ids from episode: {out}", "DEBUG")
                return out
    return {}

def _title_search_show_ids(title: str, cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    try:
        if not title: return {}
        r = requests.get(f"{TRAKT_API}/search/show", params={"query": title, "limit": 1},
                         headers=_headers(cfg), timeout=10)
        if r.status_code != 200: return {}
        arr = r.json() or []
        if not arr: return {}
        ids = (((arr[0] or {}).get("show") or {}).get("ids") or {})
        return {k: ids[k] for k in ("trakt","tvdb","tmdb","imdb") if ids.get(k)}
    except Exception:
        return {}

def _resolve_trakt_episode_id(md: Dict[str, Any], ids_all: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Optional[int]:
    s = md.get("parentIndex"); e = md.get("index")
    key = ("episode", ids_all.get("imdb"), ids_all.get("tmdb"), ids_all.get("tvdb"), s, e)
    c = _cache_get(key)
    if c is not None: return c
    hint = {**_episode_ids_from_md(md), **ids_all, **_all_ids_from_metadata(md)}
    found = _guid_search_episode(hint, cfg, logger=logger)
    tid = (found or {}).get("trakt")
    if isinstance(tid, int):
        _cache_put(key, tid); return tid
    show_tid = _resolve_trakt_show_id(ids_all, cfg, logger=logger)
    if show_tid and isinstance(s, int) and isinstance(e, int):
        try:
            r = requests.get(f"{TRAKT_API}/shows/{show_tid}/seasons/{s}/episodes/{e}",
                             headers=_headers(cfg), timeout=10)
            if r.status_code == 200:
                ej = r.json() or {}
                tid = ((ej.get("ids") or {}).get("trakt"))
                if tid:
                    _cache_put(key, int(tid)); return int(tid)
        except Exception as ex:
            _emit(logger, f"trakt ep id resolve error: {ex}", "DEBUG")
    _cache_put(key, None); return None

def _best_id_key_order(media_type: str) -> tuple[str, ...]:
    return ("imdb", "tmdb", "tvdb") if media_type == "movie" else ("tmdb", "imdb", "tvdb")

def _build_primary_body(media_type: str, md: Dict[str, Any], ids_all: Dict[str, Any],
                        prog: float, cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    p = float(round(prog, 2))
    if media_type == "movie":
        tid = _resolve_trakt_movie_id(ids_all, cfg, logger=logger)
        if tid: return {"progress": p, "movie": {"ids": {"trakt": tid}}}
        for k in _best_id_key_order("movie"):
            if k in ids_all: return {"progress": p, "movie": {"ids": {k: ids_all[k]}}}
        return {}
    tid = _resolve_trakt_episode_id(md, ids_all, cfg, logger=logger)
    if tid: return {"progress": p, "episode": {"ids": {"trakt": tid}}}
    s = md.get("parentIndex"); n = md.get("index")
    show_ids = _show_ids_from_md(md)
    if not show_ids:
        hint = {**_episode_ids_from_md(md), **ids_all, **_all_ids_from_metadata(md)}
        show_ids = _show_ids_from_episode_hint(hint, cfg, logger=logger)
    if not show_ids:
        show_tid = _resolve_trakt_show_id(ids_all, cfg, logger=logger)
        if show_tid: show_ids = {"trakt": int(show_tid)}
    if not show_ids:
        title = (md.get("grandparentTitle") or "").strip()
        if title:
            show_ids = _title_search_show_ids(title, cfg, logger=logger)
    if show_ids and isinstance(s, int) and isinstance(n, int):
        return {"progress": p, "show": {"ids": show_ids}, "episode": {"season": s, "number": n}}
    return {}

def _body_ids_desc(b: Dict[str, Any]) -> str:
    if not b: return "none"
    ids = ((b.get("movie") or {}).get("ids")) or ((b.get("show") or {}).get("ids")) or ((b.get("episode") or {}).get("ids"))
    return _describe_ids(ids if ids else "none")

def _account_matches(allow_users: set[str], payload: Dict[str, Any], logger=None) -> bool:
    if not allow_users: return True
    def norm(s: str) -> str: return re.sub(r"[^a-z0-9]+", "", (s or "").lower())
    title = ((payload.get("Account") or {}).get("title") or "")
    acc_id = str((payload.get("Account") or {}).get("id") or "")
    acc_uuid = str((payload.get("Account") or {}).get("uuid") or "").lower()
    try:
        psn0 = (payload.get("PlaySessionStateNotification") or [None])[0] or {}
        acc_id = acc_id or str(psn0.get("accountID") or "")
        acc_uuid = acc_uuid or str(psn0.get("accountUUID") or "").lower()
    except Exception:
        pass
    wl = [str(x).strip() for x in allow_users if str(x).strip()]
    for e in wl:
        s = e.lower()
        if s.startswith("id:") and acc_id and s.split(":",1)[1] == acc_id: return True
        if s.startswith("uuid:") and acc_uuid and s.split(":",1)[1] == acc_uuid: return True
        if not s.startswith(("id:","uuid:")) and norm(e) == norm(title): return True
    return False

def _account_key(payload: Dict[str, Any]) -> str:
    acc = payload.get("Account") or {}
    acc_uuid = str(acc.get("uuid") or "").lower()
    acc_id = str(acc.get("id") or "")
    title = str(acc.get("title") or "")
    return acc_uuid or f"id:{acc_id}" or title or "unknown"

def _player_state_from_sessions(cfg: Dict[str, Any], target_rk: str, target_sk: str, acc_id: str, acc_title: str) -> str:
    try:
        base, token = _plex_base_token(cfg)
        if not token: return "unknown"
        r = requests.get(f"{base}/status/sessions", headers={"X-Plex-Token": token}, timeout=5)
        if r.status_code != 200: return "unknown"
        root = ET.fromstring(r.text or "")
        for v in root.iter("Video"):
            rk = v.get("ratingKey") or ""
            sk = v.get("sessionKey") or ""
            user = v.find("User")
            u_id = (user.get("id") if user is not None else "") or ""
            u_title = (user.get("title") if user is not None else "") or ""
            if ((target_sk and sk == target_sk) or (target_rk and rk == target_rk)) and (u_id == acc_id or u_title == acc_title):
                p = v.find("Player")
                state = ((p.get("state") if p is not None else "") or "").lower()
                _emit(None, f"autoplay probe match rk={rk} sk={sk} user={u_title or u_id} state={state}", "DEBUG")
                return state or "unknown"
        _emit(None, f"autoplay probe no match for rk={target_rk} sk={target_sk}", "DEBUG")
        return "none"
    except Exception as e:
        _emit(None, f"autoplay probe error: {e}", "DEBUG")
        return "unknown"

def _check_autoplay_after(cfg: Dict[str, Any], payload: Dict[str, Any], md: Dict[str, Any],
                          ids_all2: Dict[str, Any], wait_s: int, suppress_start_at: float,
                          stop_pause_threshold: float, force_stop_at: float, logger=None) -> None:
    try:
        rk = str(md.get("ratingKey") or "")
        sk = str(payload.get("sessionKey") or md.get("sessionKey") or "")
        acc = payload.get("Account") or {}
        acc_id = str(acc.get("id") or "")
        acc_title = str(acc.get("title") or "")
        time.sleep(max(0.0, float(wait_s)))
        state = _player_state_from_sessions(cfg, rk, sk, acc_id, acc_title)

        if state in ("playing", "buffering"):
            sess = str(md.get("ratingKey") or sk or "n/a")
            st = _SCROBBLE_STATE.get(sess) or {}
            prog = max(2.0, float(st.get("prog", 0.0)) or 2.0)
            media_type = (md.get("type") or "").lower()
            body = _build_primary_body(media_type, md, ids_all2, prog, cfg, logger=logger)
            if not body:
                _emit(logger, "autoplay promote: no IDs", "DEBUG")
                _SCROBBLE_STATE[sess] = {"ts": time.time(), "last_event": "media.play", "prog": prog, "sk": sk,
                                         "finished": False, "autoplay_pending": False, "autoplay_until": 0.0}
                return
            _emit(logger, "autoplay window expired; player playing → send start", "DEBUG")
            r = _post_trakt("/scrobble/start", body, cfg)
            try: rj = r.json()
            except Exception: rj = {"raw": (r.text or "")[:200]}
            _emit(logger, f"trakt /scrobble/start -> {r.status_code}", "DEBUG")
            _SCROBBLE_STATE[sess] = {"ts": time.time(), "last_event": "media.play", "prog": prog, "sk": sk,
                                     "finished": False, "autoplay_pending": False, "autoplay_until": 0.0}
        else:
            _emit(logger, "autoplay window expired; player not playing → clear quarantine", "DEBUG")
            sess = str(md.get("ratingKey") or sk or "n/a")
            st = _SCROBBLE_STATE.get(sess) or {}
            _SCROBBLE_STATE[sess] = {"ts": time.time(), "last_event": "autoplay_cleared", "prog": 0.0,
                                     "sk": sk, "finished": False, "autoplay_pending": False, "autoplay_until": 0.0,
                                     **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {})}
    except Exception as e:
        _emit(logger, f"autoplay check error: {e}", "DEBUG")

def process_webhook(
    payload: Dict[str, Any],
    headers: Mapping[str, str],
    raw: Optional[bytes] = None,
    logger: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    cfg = _ensure_scrobble(_load_config())

    sc = cfg.get("scrobble") or {}
    if not sc.get("enabled", True) or str(sc.get("mode", "webhook")).lower() != "webhook":
        _emit(logger, "scrobble webhook disabled by config", "DEBUG"); return {"ok": True, "ignored": True}

    secret = ((cfg.get("plex") or {}).get("webhook_secret") or "").strip()
    if not _verify_signature(raw, headers, secret):
        _emit(logger, "invalid X-Plex-Signature", "WARN"); return {"ok": False, "error": "invalid_signature"}

    if not payload:
        _emit(logger, "empty payload", "WARN"); return {"ok": True, "ignored": True}

    if ((cfg.get("trakt") or {}).get("client_id") or "") == "":
        _emit(logger, "missing trakt.client_id", "ERROR"); return {"ok": False}

    wh = (sc.get("webhook") or {})
    pause_debounce = int(wh.get("pause_debounce_seconds", _DEF_WEBHOOK["pause_debounce_seconds"]) or 0)
    suppress_start_at = float(wh.get("suppress_start_at", _DEF_WEBHOOK["suppress_start_at"]) or 99)
    suppress_autoplay = int(wh.get("suppress_autoplay_seconds", _DEF_WEBHOOK["suppress_autoplay_seconds"]) or 0)
    probe_progress = bool(wh.get("probe_session_progress", True))
    flt = (wh.get("filters_plex") or {})
    allow_users = set((flt.get("username_whitelist") or []))
    srv_uuid_cfg = (flt.get("server_uuid") or "").strip() or ((cfg.get("plex") or {}).get("server_uuid") or "").strip()

    tset = (sc.get("trakt") or {})
    stop_pause_threshold = float(tset.get("stop_pause_threshold", _DEF_TRAKT["stop_pause_threshold"]))
    force_stop_at = float(tset.get("force_stop_at", stop_pause_threshold))
    regress_tol = float(tset.get("regress_tolerance_percent", _DEF_TRAKT["regress_tolerance_percent"]))

    acc_title = ((payload.get("Account") or {}).get("title") or "").strip()
    srv_uuid_evt = ((payload.get("Server") or {}).get("uuid") or "").strip()
    event = (payload.get("event") or "").lower()
    md = payload.get("Metadata") or {}
    media_type = (md.get("type") or "").lower()
    media_name_dbg = md.get("title") or md.get("grandparentTitle") or "?"
    if media_type == "episode":
        try:
            _show = (md.get("grandparentTitle") or "").strip()
            _ep = (md.get("title") or "").strip()
            _s = md.get("parentIndex"); _e = md.get("index")
            if isinstance(_s, int) and isinstance(_e, int) and _show:
                media_name_dbg = f"{_show} S{_s:02}E{_e:02}" + (f" — {_ep}" if _ep else "")
            elif _show and _ep:
                media_name_dbg = f"{_show} — {_ep}"
            else:
                media_name_dbg = _show or _ep or media_name_dbg
        except Exception:
            pass

    _emit(logger, f"incoming '{event}' user='{acc_title}' server='{srv_uuid_evt}' media='{media_name_dbg}'", "DEBUG")

    if srv_uuid_cfg and srv_uuid_evt and srv_uuid_evt != srv_uuid_cfg:
        _emit(logger, f"ignored server '{srv_uuid_evt}' (expect '{srv_uuid_cfg}')", "DEBUG"); return {"ok": True, "ignored": True}

    if not _account_matches(allow_users, payload, logger=logger):
        _emit(logger, f"ignored user '{acc_title}'", "DEBUG"); return {"ok": True, "ignored": True}

    if not md or media_type not in ("movie", "episode"):
        return {"ok": True, "ignored": True}

    show_ids = _show_ids_from_md(md)
    epi_ids = _episode_ids_from_md(md)
    all_ids = _all_ids_from_metadata(md)
    ids_all2 = {**show_ids, **epi_ids} if (show_ids or epi_ids) else dict(all_ids)
    _emit(logger, f"ids resolved: {media_name_dbg} -> {_describe_ids((show_ids or epi_ids) or all_ids)}", "DEBUG")

    prog_raw = _progress(payload)

    rk = md.get("ratingKey") or md.get("ratingkey")
    sk = md.get("sessionKey") or md.get("sessionkey")
    if probe_progress:
        p_probe = _probe_session_progress(cfg, rk, sk)
        if isinstance(p_probe, int) and abs(p_probe - int(round(prog_raw))) >= 5:
            best = p_probe
            if 5 <= best <= 95 or (best >= 96 and prog_raw >= 95):
                _emit(logger, f"probe correction: {prog_raw:.0f}% → {best:.0f}%", "DEBUG")
                prog_raw = float(best)

    sess = str(md.get("ratingKey") or payload.get("sessionKey") or md.get("sessionKey") or "n/a")
    now = time.time()
    st = _SCROBBLE_STATE.get(sess) or {}

    acc_key = _account_key(payload)
    sk_current = str(payload.get("sessionKey") or md.get("sessionKey") or "")
    if event in ("media.play", "media.resume") and suppress_autoplay > 0:
        fin = _LAST_FINISH_BY_ACC.get(acc_key)
        if fin:
            dt = now - float(fin.get("ts", 0))
            rk_new = str(md.get("ratingKey") or "")
            rk_old = str(fin.get("rk") or "")
            if rk_new and rk_new != rk_old and dt <= suppress_autoplay and (prog_raw <= 5.0):
                _emit(logger, f"quarantine autoplay start dt={dt:.1f}s (rk {rk_old}->{rk_new})", "DEBUG")
                _SCROBBLE_STATE[sess] = {
                    "ts": now, "last_event": "autoplay_quarantined", "prog": 0.0, "sk": sk_current,
                    "autoplay_pending": True, "autoplay_until": now + float(suppress_autoplay), "finished": False,
                }
                threading.Thread(
                    target=_check_autoplay_after,
                    args=(cfg, payload, md, ids_all2, suppress_autoplay, suppress_start_at, stop_pause_threshold, force_stop_at, logger),
                    daemon=True,
                ).start()
                return {"ok": True, "quarantined": True}

    if st.get("last_event") == event and (now - float(st.get("ts", 0))) < 1.0:
        return {"ok": True, "dedup": True}
    if event == "media.pause" and (now - float(st.get("last_pause_ts", 0))) < pause_debounce:
        _emit(logger, f"debounce pause ({pause_debounce}s)", "DEBUG")
        _SCROBBLE_STATE[sess] = {**st, "ts": now, "last_event": event}; return {"ok": True, "debounced": True}

    is_start = event in ("media.play", "media.resume")
    finished_flag = bool(st.get("finished"))
    fresh_start_rewatch = (
        is_start and float(prog_raw) <= 5.0 and (
            finished_flag or
            (st.get("last_event") in ("media.stop", "media.scrobble")) or
            (sk_current and sk_current != st.get("sk")) or
            (float(st.get("prog", 0.0)) >= force_stop_at)
        )
    )
    fresh_start_quarantined = bool(st.get("autoplay_pending") and now <= float(st.get("autoplay_until", 0)))
    fresh_start = fresh_start_rewatch or fresh_start_quarantined

    if fresh_start_quarantined and event == "media.stop" and _progress(payload) < 2.0:
        _emit(logger, "autoplay stopped immediately (<2%) → ignore", "DEBUG")
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": 0.0, "sk": sk_current, "finished": False,
                                 "autoplay_pending": False, "autoplay_until": 0.0}
        return {"ok": True, "ignored": True}

    last_prog = float(st.get("prog", 0.0))
    tol_pts = max(0.0, regress_tol)
    prog = prog_raw

    last_prog_for_clamp = 0.0 if fresh_start else last_prog
    if prog + tol_pts < last_prog_for_clamp:
        _emit(logger, f"regression clamp {prog_raw:.2f}% -> {last_prog_for_clamp:.2f}% (tol={tol_pts}%)", "DEBUG")
        prog = last_prog_for_clamp

    if event == "media.pause" and prog >= 99.9 and last_prog > 0.0:
        newp = max(last_prog, 95.0); _emit(logger, f"pause@100 clamp {prog:.2f}% -> {newp:.2f}%", "DEBUG"); prog = newp

    if event == "media.stop" and last_prog >= force_stop_at and prog < last_prog:
        _emit(logger, f"promote STOP: using last progress {last_prog:.1f}% (current {prog:.1f}%)", "DEBUG")
        prog = last_prog

    if event in ("media.stop", "media.scrobble") and prog < force_stop_at:
        if _probe_played_status(cfg, rk):
            _emit(logger, f"PMS says played → force STOP at ≥95%", "DEBUG")
            prog = max(prog, last_prog, 95.0)

    if event == "media.stop" and prog < force_stop_at:
        dt = now - float(st.get("ts", 0))
        if dt < 2.0:
            _emit(logger, f"drop stop due to debounce dt={dt:.2f}s p={prog:.1f}% (<{force_stop_at}%)", "DEBUG")
            _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog, "sk": sk_current,
                                     "finished": (prog >= force_stop_at),
                                     "autoplay_pending": False, "autoplay_until": 0.0}
            return {"ok": True, "suppressed": True}

    path = _map_event(event)
    if not path:
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog, "sk": sk_current,
                                 "finished": (prog >= force_stop_at),
                                 "autoplay_pending": False, "autoplay_until": 0.0}
        return {"ok": True, "ignored": True}

    if path == "/scrobble/start" and prog >= suppress_start_at:
        _emit(logger, f"suppress start at {prog:.1f}% (>= {suppress_start_at}%)", "DEBUG")
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog, "sk": sk_current,
                                 "finished": (prog >= force_stop_at),
                                 "autoplay_pending": False, "autoplay_until": 0.0}
        return {"ok": True, "suppressed": True}

    intended = path

    if event == "media.pause" and (prog >= force_stop_at or last_prog >= force_stop_at):
        _emit(logger, f"promote PAUSE→STOP at {max(prog, last_prog):.1f}%", "DEBUG")
        intended = "/scrobble/stop"
        prog = max(prog, last_prog, 95.0)

    if intended == "/scrobble/stop":
        if prog < stop_pause_threshold: intended = "/scrobble/pause"
        elif prog < force_stop_at: intended = "/scrobble/pause"
        elif last_prog >= 0 and (prog - last_prog) >= 30 and last_prog < stop_pause_threshold and prog >= 98:
            _emit(logger, f"Demote STOP→PAUSE jump {last_prog:.0f}%→{prog:.0f}% (thr={stop_pause_threshold})", "DEBUG")
            intended = "/scrobble/pause"; prog = last_prog

    if intended == "/scrobble/start" and prog < 2.0: prog = 2.0
    if intended == "/scrobble/pause" and prog < 1.0: prog = 1.0

    if event == "media.stop" and st.get("last_event") == "media.stop" and abs((st.get("prog", 0.0)) - prog) <= 1.0:
        _emit(logger, "suppress duplicate stop", "DEBUG")
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog, "sk": sk_current,
                                 "finished": (prog >= force_stop_at),
                                 "autoplay_pending": False, "autoplay_until": 0.0}
        return {"ok": True, "suppressed": True}

    if event in ("media.stop", "media.scrobble") and prog >= force_stop_at:
        _LAST_FINISH_BY_ACC[_account_key(payload)] = {"rk": str(rk or ""), "ts": now}

    try:
        stop_flag = (intended == "/scrobble/stop")
        title = (md.get("title") or md.get("grandparentTitle") or "").strip()
        year = md.get("year")

        season_val = None
        episode_val = None
        if (media_type or "").lower() == "episode":
            try:
                season_val = int(md.get("parentIndex") or 0) or None
            except Exception:
                season_val = None
            try:
                episode_val = int(md.get("index") or 0) or None
            except Exception:
                episode_val = None

        duration_ms = None
        try:
            dur = md.get("duration")
            if dur is not None:
                duration_ms = int(dur)
        except Exception:
            duration_ms = None

        state_val = "playing"
        if stop_flag:
            state_val = "stopped"
        elif intended == "/scrobble/pause":
            state_val = "paused"

        cw_ids = _cw_ids_for_payload(media_type, md, ids_all2, cfg, logger=logger)
        _cw_update(
            source="plextrakt",
            media_type=(media_type or ""),
            title=title,
            year=year,
            season=season_val,
            episode=episode_val,
            progress=prog,
            stop=stop_flag,
            duration_ms=duration_ms,
            cover=None,
            state=state_val,
            clear_on_stop=True,
            ids=cw_ids,
        )

    except Exception:
        pass

    body = _build_primary_body(media_type, md, ids_all2, prog, cfg, logger=logger)
    if not body:
        _emit(logger, "no usable IDs; skip scrobble", "DEBUG")
        _SCROBBLE_STATE[sess] = {"ts": now, "last_event": event, "prog": prog, "sk": sk_current,
                                 "finished": (prog >= force_stop_at),
                                 "autoplay_pending": False, "autoplay_until": 0.0}
        return {"ok": True, "ignored": True}

    if intended == "/scrobble/stop" and prog >= force_stop_at:
        try: _del_trakt("/checkin", cfg)
        except Exception: pass
        time.sleep(0.15)

    _emit(logger, f"trakt intent {intended} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")
    r = _post_trakt(intended, body, cfg)
    try: rj = r.json()
    except Exception: rj = {"raw": (r.text or "")[:200]}
    _emit(logger, f"trakt {intended} -> {r.status_code} action={rj.get('action') or intended.rsplit('/',1)[-1]}", "DEBUG")

    if r.status_code == 404 and media_type == "episode":
        epi_hint = {**(_episode_ids_from_md(md) or {}), **ids_all2}
        found = _guid_search_episode(epi_hint, cfg, logger=logger)
        if found:
            body2 = {"progress": float(round(prog, 2)), "episode": {"ids": found}}
            _emit(logger, f"trakt intent {intended} using {_describe_ids(found)} (rescue)", "DEBUG")
            r = _post_trakt(intended, body2, cfg)
            try: rj = r.json()
            except Exception: rj = {"raw": (r.text or "")[:200]}
            _emit(logger, f"trakt {intended} (rescue) -> {r.status_code}", "DEBUG")

    if r.status_code == 409 and intended == "/scrobble/stop":
        raw_txt = r.text or ""
        if ("expires_at" in raw_txt or "watched_at" in raw_txt):
            if prog >= force_stop_at and not (st.get("wl_removed") is True):
                try:
                    _call_remove_across(ids_all2 or {}, media_type)
                    st = {**st, "wl_removed": True}
                except Exception:
                    pass
            _SCROBBLE_STATE[sess] = {
                "ts": now, "last_event": event, "last_pause_ts": st.get("last_pause_ts", 0),
                "prog": prog, "sk": sk_current, "finished": True,
                "autoplay_pending": False, "autoplay_until": 0.0,
                **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
            }
            _LAST_FINISH_BY_ACC[_account_key(payload)] = {"rk": str(rk or ""), "ts": now}
            return {"ok": True, "status": 200, "action": intended, "trakt": rj, "note": "409_checkin"}

    if r.status_code < 400:
        if intended == "/scrobble/stop" and prog >= force_stop_at and not (st.get("wl_removed") is True):
            try:
                _call_remove_across(ids_all2 or {}, media_type)
                st = {**st, "wl_removed": True}
            except Exception:
                pass
        _SCROBBLE_STATE[sess] = {
            "ts": now, "last_event": event,
            "last_pause_ts": (now if intended == "/scrobble/pause" else st.get("last_pause_ts", 0)),
            "prog": prog, "sk": sk_current,
            "finished": (intended == "/scrobble/stop" and prog >= force_stop_at),
            "autoplay_pending": False, "autoplay_until": 0.0,
            **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
        }
        if intended == "/scrobble/stop" and prog >= force_stop_at:
            _LAST_FINISH_BY_ACC[_account_key(payload)] = {"rk": str(rk or ""), "ts": now}
        try:
            action_name = intended.rsplit("/", 1)[-1]
            _emit(logger, f"user='{acc_title}' {action_name} {prog:.1f}% • {media_name_dbg}", "INFO")
        except Exception:
            pass
        return {"ok": True, "status": 200, "action": intended, "trakt": rj}

    if event in ("media.stop", "media.scrobble") and prog >= force_stop_at:
        _LAST_FINISH_BY_ACC[_account_key(payload)] = {"rk": str(rk or ""), "ts": now}

    _emit(logger, f"{intended} {r.status_code} {(str(rj)[:180])}", "ERROR")
    _SCROBBLE_STATE[sess] = {
        "ts": now, "last_event": event, "last_pause_ts": st.get("last_pause_ts", 0),
        "prog": prog, "sk": sk_current, "finished": (prog >= force_stop_at),
        "autoplay_pending": False, "autoplay_until": 0.0,
        **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
    }
    return {"ok": False, "status": r.status_code, "trakt": rj}

def _map_event(event: str) -> Optional[str]:
    e = (event or "").lower()
    if e in ("media.play", "media.resume"): return "/scrobble/start"
    if e == "media.pause": return "/scrobble/pause"
    if e in ("media.stop", "media.scrobble"): return "/scrobble/stop"
    return None

def _verify_signature(raw: Optional[bytes], headers: Mapping[str, str], secret: str) -> bool:
    if not secret: return True
    if not raw: return False
    sig = headers.get("X-Plex-Signature") or headers.get("x-plex-signature")
    if not sig: return False
    digest = hmac.new(secret.encode("utf-8"), raw, hashlib.sha1).digest()
    expected = base64.b64encode(digest).decode("ascii")
    return hmac.compare_digest(sig.strip(), expected.strip())

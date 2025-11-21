# providers/webhooks/jellyfintrakt.py
# CrossWatch - Jellyfin Trakt Scrobbler Webhook Module
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
import json, time, requests
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Callable
try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.currently_watching import update_from_payload as _cw_update

TRAKT_API = "https://api.trakt.tv"
_SCROBBLE_STATE: Dict[str, Dict[str, Any]] = {}
_TRAKT_ID_CACHE: Dict[tuple, Any] = {}

_DEF_WEBHOOK = {
    "pause_debounce_seconds": 5,
    "suppress_start_at": 99,
    "filters_jellyfin": {"username_whitelist": []},
    "suppress_autoplay_seconds": 0,
    "post_stop_play_guard_seconds": 0,
    "start_guard_min_progress": 0,
    "guard_autoplay_seconds": 0,
    "cancel_checkin_on_stop": True,
    "anti_autoplay_seconds": 0,
}
_DEF_TRAKT = {"stop_pause_threshold": 80, "force_stop_at": 95, "regress_tolerance_percent": 5, "complete_at": 95}

from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from _watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except Exception:
    _rm_across_api = None

def _call_remove_across(ids: Dict[str, Any], media_type: str) -> None:
    if not isinstance(ids, dict) or not ids:
        return
    try:
        cfg = _load_config()
        s = (cfg.get("scrobble") or {})
        if not s.get("delete_plex"):
            return
        tps = s.get("delete_plex_types") or []
        mt = (media_type or "").strip().lower()
        allow = False
        if isinstance(tps, list):
            allow = (mt in tps) or ((mt.rstrip("s") + "s") in tps)
        elif isinstance(tps, str):
            allow = mt in tps
        if not allow:
            return
    except Exception:
        pass
    try:
        if callable(_rm_across):
            _rm_across(ids, media_type); return
    except Exception:
        pass
    try:
        if callable(_rm_across_api):
            _rm_across_api(ids, media_type); return
    except Exception:
        pass

def _load_config() -> Dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        pass
    for p in ("/config/config.json", "config.json"):
        try:
            fp = Path(p)
            if fp.exists():
                return json.loads(fp.read_text(encoding="utf-8"))
        except Exception:
            continue
    return {}

def _save_config(cfg: Dict[str, Any]) -> None:
    try:
        from crosswatch import save_config as _save
        _save(cfg); return
    except Exception:
        pass
    try:
        Path("/config/config.json").write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
    except Exception:
        try:
            Path("config.json").write_text(json.dumps(cfg, indent=2, ensure_ascii=False), encoding="utf-8")
        except Exception:
            pass

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
                logr(msg, level=lvl_up)
            return
    except Exception:
        pass
    print(f"[SCROBBLE] {lvl_up} {msg}")


def _ensure_scrobble(cfg: Dict[str, Any]) -> Dict[str, Any]:
    changed = False
    sc = cfg.setdefault("scrobble", {})
    wh = sc.setdefault("webhook", {})
    trk = sc.setdefault("trakt", {})
    if "pause_debounce_seconds" not in wh:
        wh["pause_debounce_seconds"] = _DEF_WEBHOOK["pause_debounce_seconds"]; changed = True
    if "suppress_start_at" not in wh:
        wh["suppress_start_at"] = _DEF_WEBHOOK["suppress_start_at"]; changed = True
    if "filters_jellyfin" not in wh:
        wh["filters_jellyfin"] = {"username_whitelist": []}; changed = True
    if "filters" in wh:
        del wh["filters"]; changed = True
    for k, dv in _DEF_WEBHOOK.items():
        if k not in wh:
            wh[k] = dv; changed = True
    for k, dv in _DEF_TRAKT.items():
        if k not in trk:
            trk[k] = dv; changed = True
    if changed:
        _save_config(cfg)
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
    return requests.delete(url, headers=_headers(cfg), timeout=12)

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
        try: txt = (r.text or "")
        except Exception: txt = ""
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

def _cache_get(key: tuple) -> Optional[Any]:
    try: return _TRAKT_ID_CACHE.get(key)
    except Exception: return None

def _cache_put(key: tuple, value: Any) -> None:
    try:
        if len(_TRAKT_ID_CACHE) > 2048: _TRAKT_ID_CACHE.clear()
        _TRAKT_ID_CACHE[key] = value
    except Exception:
        pass

def _grab(d: Mapping[str, Any], keys: list[str]) -> Any:
    for k in keys:
        if k in d and d[k] not in (None, ""):
            return d[k]
    return None

def _ids_from_providerids(md: Mapping[str, Any], root: Mapping[str, Any]) -> Dict[str, Any]:
    ids = {}
    pids = (md.get("ProviderIds") or {}) if isinstance(md, dict) else {}
    flat = {
        "tmdb": root.get("Provider_tmdb") or root.get("Tmdb") or root.get("TheMovieDb") or root.get("SeriesTmdbId") or root.get("SeriesTmdb"),
        "imdb": root.get("Provider_imdb") or root.get("Imdb") or root.get("SeriesImdbId") or root.get("SeriesImdb"),
        "tvdb": root.get("Provider_tvdb") or root.get("Tvdb") or root.get("TheTVDB") or root.get("TheTvdb") or root.get("SeriesTvdbId") or root.get("SeriesTvdb"),
    }
    def norm_imdb(v): s = str(v).strip(); return s if s.startswith("tt") else (f"tt{s}" if s else "")
    def maybe_int(v):
        s = str(v).strip()
        return int(s) if s.isdigit() else (s if s else None)
    tmdb = pids.get("Tmdb") or pids.get("tmdb") or pids.get("TheMovieDb") or flat["tmdb"]
    imdb = pids.get("Imdb") or pids.get("imdb") or flat["imdb"]
    tvdb = pids.get("Tvdb") or pids.get("tvdb") or pids.get("TheTVDB") or pids.get("TheTvdb") or flat["tvdb"]
    if tmdb:
        v = maybe_int(tmdb)
        if v is not None: ids["tmdb"] = v
    if imdb:
        imdb = norm_imdb(imdb)
        if imdb: ids["imdb"] = imdb
    if tvdb:
        v = maybe_int(tvdb)
        if v is not None: ids["tvdb"] = v
    return ids

def _episode_numbers(md: Mapping[str, Any], root: Mapping[str, Any]) -> tuple[Any, Any]:
    season = _grab(md, ["ParentIndexNumber", "SeasonIndexNumber", "season"])
    number = _grab(md, ["IndexNumber", "episode"])
    season = season if season is not None else _grab(root, ["SeasonNumber"])
    number = number if number is not None else _grab(root, ["EpisodeNumber"])
    return season, number

def _ids_desc(ids: Dict[str, Any] | str) -> str:
    if isinstance(ids, dict):
        if "tmdb" in ids: return f"tmdb:{ids['tmdb']}"
        if "imdb" in ids: return f"imdb:{ids['imdb']}"
        if "tvdb" in ids: return f"tvdb:{ids['tvdb']}"
        if "trakt" in ids: return f"trakt:{ids['trakt']}"
        return "none"
    return str(ids)

def _guid_search_episode(ids_hint: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any] | None:
    for key in ("tmdb", "imdb", "tvdb"):
        val = ids_hint.get(key)
        if not val: continue
        try:
            r = requests.get(f"{TRAKT_API}/search/{key}/{val}", params={"type": "episode", "limit": 1},
                             headers=_headers(cfg), timeout=10)
        except Exception:
            continue
        if r.status_code != 200: continue
        try: arr = r.json() or []
        except Exception: arr = []
        for hit in arr:
            epi_ids = ((hit.get("episode") or {}).get("ids") or {})
            out = {k: epi_ids[k] for k in ("trakt", "tmdb", "imdb", "tvdb") if epi_ids.get(k)}
            if out:
                _emit(logger, f"guid search resolved episode ids: {out}", "DEBUG")
                return out
    return None

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

def _resolve_trakt_episode_id(md: Dict[str, Any], ids_all: Dict[str, Any], cfg: Dict[str, Any], logger=None, root: Optional[Mapping[str, Any]] = None) -> Optional[int]:
    s, e = _episode_numbers(md, root or md)
    key = ("episode", ids_all.get("imdb"), ids_all.get("tmdb"), ids_all.get("tvdb"), s, e)
    c = _cache_get(key)
    if c is not None: return c
    hint = {**_ids_from_providerids(md, md)}
    found = _guid_search_episode(hint if hint else ids_all, cfg, logger=logger)
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

def _series_ids_from_payload(md: Mapping[str, Any], root: Mapping[str, Any]) -> Dict[str, Any]:
    sp = (md.get("SeriesProviderIds") or root.get("SeriesProviderIds") or {}) or {}
    out = {}
    vals = {
        "tvdb": sp.get("Tvdb") or sp.get("tvdb") or sp.get("TheTVDB") or sp.get("TheTvdb") or root.get("SeriesTvdbId") or root.get("SeriesTvdb"),
        "tmdb": sp.get("Tmdb") or sp.get("tmdb") or sp.get("TheMovieDb") or root.get("SeriesTmdbId") or root.get("SeriesTmdb"),
        "imdb": sp.get("Imdb") or sp.get("imdb") or root.get("SeriesImdbId") or root.get("SeriesImdb"),
    }
    try:
        if vals["tvdb"]: out["tvdb"] = int(str(vals["tvdb"]).strip())
    except: pass
    try:
        if vals["tmdb"]: out["tmdb"] = int(str(vals["tmdb"]).strip())
    except: pass
    if vals["imdb"]:
        s = str(vals["imdb"]).strip()
        out["imdb"] = s if s.startswith("tt") else (f"tt{s}" if s else "")
    return {k: v for k, v in out.items() if v}

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
            out = {k: show_ids[k] for k in ("trakt","tmdb","imdb","tvdb") if show_ids.get(k)}
            if out:
                _emit(logger, f"guid search resolved SHOW ids from episode: {out}", "DEBUG")
                return out
    return {}

def _resolve_episode_by_showids(show_ids: Dict[str, Any], season: Any, number: Any, cfg: Dict[str, Any], logger=None) -> Optional[int]:
    if not isinstance(season, int) or not isinstance(number, int):
        return None
    for pref in ("trakt","tmdb","imdb","tvdb"):
        sid = show_ids.get(pref)
        if not sid:
            continue
        try:
            r = requests.get(f"{TRAKT_API}/shows/{sid}/seasons/{season}/episodes/{number}",
                             headers=_headers(cfg), timeout=10)
            if r.status_code == 200:
                ej = r.json() or {}
                tid = ((ej.get("ids") or {}).get("trakt"))
                if tid:
                    return int(tid)
        except Exception as ex:
            _emit(logger, f"episode lookup via show ids error: {ex}", "DEBUG")
    return None

def _build_primary_body(media_type: str, md: Dict[str, Any], ids_all: Dict[str, Any],
                        prog: float, cfg: Dict[str, Any], logger=None, root: Optional[Mapping[str, Any]] = None) -> Dict[str, Any]:
    p = float(round(prog, 2))

    if media_type == "movie":
        tid = _resolve_trakt_movie_id(ids_all, cfg, logger=logger)
        if tid:
            return {"progress": p, "movie": {"ids": {"trakt": tid}}}
        for k in _best_id_key_order("movie"):
            if k in ids_all:
                return {"progress": p, "movie": {"ids": {k: ids_all[k]}}}
        return {}

    tid = _resolve_trakt_episode_id(md, ids_all, cfg, logger=logger, root=root)
    if tid:
        return {"progress": p, "episode": {"ids": {"trakt": tid}}}

    s, n = _episode_numbers(md, root or md)

    show_ids = {}
    try:
        show_ids = _series_ids_from_payload(md, root or md) or {}
    except Exception:
        pass

    if not show_ids:
        try:
            episode_hint = {**_ids_from_providerids(md, root or md), **(ids_all or {})}
            show_ids = _show_ids_from_episode_hint(episode_hint, cfg, logger=logger) or {}
            if show_ids:
                _emit(logger, f"derived SHOW ids from episode hint: {show_ids}", "DEBUG")
        except Exception:
            pass

    if not show_ids:
        try:
            title = (md.get("SeriesName") or (root or {}).get("SeriesName") or (root or {}).get("SeriesTitle") or "").strip()
            if title:
                r = requests.get(f"{TRAKT_API}/search/show",
                                 params={"query": title, "limit": 1},
                                 headers=_headers(cfg), timeout=10)
                if r.status_code == 200:
                    arr = r.json() or []
                    if arr:
                        si = ((arr[0] or {}).get("show") or {}).get("ids") or {}
                        if si:
                            show_ids = {k: si[k] for k in ("trakt","tmdb","imdb","tvdb") if si.get(k)}
        except Exception:
            pass

    if show_ids and isinstance(s, int) and isinstance(n, int):
        etid = _resolve_episode_by_showids(show_ids, s, n, cfg, logger=logger)
        if isinstance(etid, int):
            return {"progress": p, "episode": {"ids": {"trakt": etid}}}
        return {"progress": p, "show": {"ids": show_ids}, "episode": {"season": s, "number": n}}

    return {}

def _body_ids_desc(b: Dict[str, Any]) -> str:
    ids = ((b.get("movie") or {}).get("ids")) or ((b.get("show") or {}).get("ids")) or ((b.get("episode") or {}).get("ids"))
    return _ids_desc(ids if ids else "none")

def _progress(payload: Mapping[str, Any], md: Mapping[str, Any]) -> float:
    if isinstance(payload.get("Progress"), (int, float)):
        return round(max(0.0, min(100.0, float(payload["Progress"]))), 2)
    ps = payload.get("PlayState") or {}
    pb = payload.get("Playback") or {}
    pos = (payload.get("PlaybackPositionTicks") or payload.get("PositionTicks") or payload.get("PositionMs") or
           ps.get("PositionTicks") or ps.get("PositionMs") or pb.get("PositionTicks") or pb.get("PositionMs") or 0)
    dur = ((md.get("RunTimeTicks") or 0) or payload.get("RunTimeTicks") or ps.get("RunTimeTicks") or
           pb.get("RunTimeTicks") or payload.get("DurationMs") or 0)
    def to_ms(v: Any) -> float:
        try: v = float(v)
        except Exception: return 0.0
        return v / 10_000 if v > 10_000_000 else v
    pos_ms, dur_ms = to_ms(pos), to_ms(dur)
    if dur_ms <= 0: return 0.0
    return round(max(0.0, min(100.0, (pos_ms * 100.0) / dur_ms)), 2)

def _played_override(payload: Mapping[str, Any], md: Mapping[str, Any]) -> bool:
    ud = (payload.get("UserData") or {}) if isinstance(payload, dict) else {}
    if not ud and isinstance(md, dict): ud = (md.get("UserData") or {})
    try:
        if bool(ud.get("Played")): return True
        pc = int(ud.get("PlayCount") or 0)
        if pc >= 1: return True
    except Exception:
        pass
    return False

def _map_event(event: str) -> Optional[str]:
    e = (event or "").strip().lower()
    if e in ("playbackstart", "playbackstarted", "playbackresume", "unpause", "play"): return "/scrobble/start"
    if e in ("playbackpause", "playbackpaused", "pause"): return "/scrobble/pause"
    if e in ("playbackstop", "playbackstopped", "stop", "scrobble"): return "/scrobble/stop"
    return None

def _as_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool): return v
    if v is None: return None
    s = str(v).strip().lower()
    if s in ("1","true","yes","y","on"): return True
    if s in ("0","false","no","n","off"): return False
    return None

def _extract_paused(payload: Mapping[str, Any]) -> Optional[bool]:
    ps = payload.get("PlayState") or {}
    pb = payload.get("Playback") or {}
    for k in ("IsPaused", "Paused"):
        b = _as_bool(payload.get(k))
        if b is not None: return b
        b = _as_bool(ps.get(k))
        if b is not None: return b
        b = _as_bool(pb.get(k))
        if b is not None: return b
    return None

def _session_media_key(md: Mapping[str, Any], ids_all: Mapping[str, Any], root: Optional[Mapping[str, Any]] = None) -> str:
    v = md.get("Id")
    if v: return str(v)
    for k in ("imdb","tmdb","tvdb","trakt"):
        vv = ids_all.get(k)
        if vv: return f"{k}:{vv}"
    name = (md.get("SeriesName") or md.get("Name") or "")
    s, n = _episode_numbers(md, root or md)
    if name and isinstance(s, int) and isinstance(n, int):
        return f"{name}|S{s}E{n}"
    y = md.get("ProductionYear") or ""
    return f"{name}|{y}"

def _make_session_id(payload: Mapping[str, Any], md: Mapping[str, Any], ids_all: Mapping[str, Any]) -> str:
    base = str(payload.get("PlaySessionId") or payload.get("SessionId") or payload.get("DeviceId") or "n/a")
    return base + "|" + _session_media_key(md, ids_all, root=payload)

def process_webhook(
    payload: Dict[str, Any],
    headers: Mapping[str, str],
    raw: Optional[bytes] = None,
    logger: Optional[Callable[..., None]] = None,
) -> Dict[str, Any]:
    try:
        cfg = _ensure_scrobble(_load_config())

        sc = cfg.get("scrobble") or {}
        if not sc.get("enabled", True) or str(sc.get("mode", "webhook")).lower() != "webhook":
            _emit(logger, "scrobble webhook disabled", "DEBUG"); return {"ok": True, "ignored": True}

        if not payload:
            _emit(logger, "empty payload", "WARN"); return {"ok": True, "ignored": True}

        if ((cfg.get("trakt") or {}).get("client_id") or "") == "":
            _emit(logger, "missing trakt.client_id", "ERROR"); return {"ok": False}

        wh = (sc.get("webhook") or {})
        pause_debounce = int(wh.get("pause_debounce_seconds", _DEF_WEBHOOK["pause_debounce_seconds"]) or 0)
        suppress_start_at = float(wh.get("suppress_start_at", _DEF_WEBHOOK["suppress_start_at"]) or 99)
        allow_users = set(((wh.get("filters_jellyfin") or {}).get("username_whitelist") or []))
        guard_autoplay = float(wh.get("guard_autoplay_seconds") or wh.get("suppress_autoplay_seconds") or 0)
        post_stop_guard = float(wh.get("post_stop_play_guard_seconds") or 0)
        start_guard_min = float(wh.get("start_guard_min_progress") or 0)
        anti_autoplay = float(wh.get("anti_autoplay_seconds") or 0)
        cancel_checkin_on_stop = bool(wh.get("cancel_checkin_on_stop", True))

        tset = (sc.get("trakt") or {})
        stop_pause_threshold = float(tset.get("stop_pause_threshold", _DEF_TRAKT["stop_pause_threshold"]))
        force_stop_at = float(tset.get("force_stop_at", _DEF_TRAKT["force_stop_at"]))
        complete_at = float(tset.get("complete_at", _DEF_TRAKT["complete_at"]))
        regress_tol = float(tset.get("regress_tolerance_percent", _DEF_TRAKT["regress_tolerance_percent"]))

        md = (payload.get("Item") or payload.get("item") or {})
        md.setdefault("Type", _grab(payload, ["ItemType", "type"]) or md.get("Type"))
        md.setdefault("Name", _grab(payload, ["Name", "ItemName", "title"]) or md.get("Name"))
        md.setdefault("SeriesName", _grab(payload, ["SeriesName", "SeriesTitle", "grandparentTitle"]) or md.get("SeriesName"))
        md.setdefault("RunTimeTicks", payload.get("RunTimeTicks") or md.get("RunTimeTicks"))
        pids = dict(md.get("ProviderIds") or {})
        for k_src, k_norm in [("Provider_tmdb", "Tmdb"), ("Provider_imdb", "Imdb"), ("Provider_tvdb", "Tvdb"), ("TheMovieDb","TheMovieDb"), ("TheTVDB","TheTVDB")]:
            if payload.get(k_src) and not pids.get(k_norm):
                pids[k_norm] = payload[k_src]
        if pids: md["ProviderIds"] = pids

        media_type_raw = (md.get("Type") or "").strip().lower()
        media_type = "movie" if media_type_raw == "movie" else ("episode" if media_type_raw == "episode" else "")
        event = (_grab(payload, ["NotificationType", "Event", "event"]) or "").strip()
        acc_title = (_grab(payload, ["NotificationUsername", "Username", "UserName"]) or "").strip()
        if not acc_title:
            acc_title = ((_grab(payload, ["User", "user"]) or {}) or {}).get("Name") or ""

        media_name_dbg = md.get("Name") or md.get("SeriesName") or "?"
        if media_type == "episode":
            try:
                show = (md.get("SeriesName") or _grab(payload, ["SeriesName", "SeriesTitle"]) or "").strip()
                ep = (md.get("Name") or md.get("EpisodeTitle") or "").strip()
                season, number = _episode_numbers(md, payload)
                if isinstance(season, int) and isinstance(number, int) and show:
                    media_name_dbg = f"{show} S{season:02}E{number:02}" + (f" — {ep}" if ep else "")
                elif show and ep:
                    media_name_dbg = f"{show} — {ep}"
                else:
                    media_name_dbg = show or ep or media_name_dbg
            except Exception:
                pass

        _emit(logger, f"incoming '{event}' user='{acc_title}' media='{media_name_dbg}'", "DEBUG")

        if allow_users and acc_title and acc_title not in allow_users:
            _emit(logger, f"ignored user '{acc_title}'", "DEBUG"); return {"ok": True, "ignored": True}
        if not md or media_type not in ("movie", "episode"):
            return {"ok": True, "ignored": True}

        ids_epi  = _ids_from_providerids(md, payload) or {}
        ids_show = _series_ids_from_payload(md, payload) or {}
        ids_all  = {**ids_show, **ids_epi}
        _emit(logger, f"ids resolved: {media_name_dbg} -> {_ids_desc(ids_show or ids_epi)}", "DEBUG")

        prog_raw = _progress(payload, md)
        sess = _make_session_id(payload, md, ids_all)
        now = time.time()
        st = _SCROBBLE_STATE.get(sess) or {}

        ev_lc = (event or "").lower()
        paused_flag = _extract_paused(payload)
        _emit(logger, f"pause-state prev={st.get('paused')} now={paused_flag}", "DEBUG")

        if st.get("last_event") == ev_lc and (now - float(st.get("ts", 0))) < 1.0 and not (paused_flag is not None and paused_flag != st.get("paused")):
            return {"ok": True, "dedup": True}

        is_pause_like = ev_lc in ("playbackpause", "playbackpaused")
        if (is_pause_like or paused_flag is True) and (now - float(st.get("last_pause_ts", 0))) < pause_debounce:
            _emit(logger, f"debounce pause ({pause_debounce}s)", "DEBUG")
            _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog_raw, "sk": str(payload.get("PlaySessionId") or payload.get("SessionId") or ""), "finished": (prog_raw >= complete_at), "paused": True}
            return {"ok": True, "debounced": True}

        sk_current = str(payload.get("PlaySessionId") or payload.get("SessionId") or "")
        is_start = ev_lc in ("playbackstart", "playbackstarted", "playbackresume", "unpause", "play")
        finished_flag = bool(st.get("finished"))
        fresh_start = (is_start and float(prog_raw) <= 5.0 and (
            finished_flag or
            (st.get("last_event") in ("playbackstop", "playbackstopped", "scrobble")) or
            (sk_current and sk_current != st.get("sk")) or
            (float(st.get("prog", 0.0)) >= complete_at)
        ))

        last_prog = float(st.get("prog", 0.0))
        prog = prog_raw
        tol_pts = max(0.0, regress_tol)
        last_prog_for_clamp = 0.0 if fresh_start else last_prog
        if not st and ev_lc == "playbackprogress":
            last_prog_for_clamp = 0.0
        is_seek_jump = (ev_lc == "playbackprogress") and (last_prog > 0.0) and (abs(prog_raw - last_prog) >= 20.0)
        if is_seek_jump:
            _emit(logger, f"seek jump {last_prog:.2f}% → {prog_raw:.2f}% (no clamp)", "DEBUG")
            last_prog_for_clamp = 0.0
        if ev_lc == "playbackprogress" and last_prog >= complete_at and prog_raw + tol_pts < last_prog:
            last_prog_for_clamp = 0.0
        if prog + tol_pts < last_prog_for_clamp:
            _emit(logger, f"regression clamp {prog_raw:.2f}% -> {last_prog_for_clamp:.2f}% (tol={tol_pts}%)", "DEBUG")
            prog = last_prog_for_clamp

        if ev_lc in ("playbackpause", "playbackpaused") and prog >= 99.9 and last_prog > 0.0:
            np = max(last_prog, complete_at); _emit(logger, f"pause@100 clamp {prog:.2f}% -> {np:.2f}%", "DEBUG"); prog = np

        if ev_lc in ("playbackstop", "playbackstopped") and last_prog >= complete_at and prog < last_prog:
            _emit(logger, f"promote STOP: using last progress {last_prog:.1f}% (current {prog:.1f}%)", "DEBUG")
            prog = last_prog

        if ev_lc in ("playbackstop", "playbackstopped", "scrobble") and prog < complete_at and _played_override(payload, md):
            _emit(logger, f"server says played → force STOP at ≥{complete_at:.0f}%", "DEBUG")
            prog = max(prog, last_prog, complete_at)

        if ev_lc in ("playbackstop", "playbackstopped") and prog < complete_at:
            dt = now - float(st.get("ts", 0))
            if dt < 2.0:
                _emit(logger, f"drop stop due to debounce dt={dt:.2f}s p={prog:.1f}% (<{complete_at}%)", "DEBUG")
                _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog, "sk": sk_current, "finished": (prog >= complete_at), "paused": st.get("paused")}
                return {"ok": True, "suppressed": True}

        derived = None
        if ev_lc == "playbackprogress":
            if paused_flag is True and st.get("paused") is not True:
                derived = "/scrobble/pause"
            elif paused_flag is False and st.get("paused") is True:
                derived = "/scrobble/start"

        intended = derived or _map_event(event)

        if intended is None:
            _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog, "sk": sk_current, "finished": (prog >= complete_at), "paused": st.get("paused") if paused_flag is None else paused_flag}
            return {"ok": True, "ignored": True}

        if intended == "/scrobble/start":
            if prog >= suppress_start_at:
                _emit(logger, f"suppress start at {prog:.1f}% (>= {suppress_start_at}%)", "DEBUG")
                _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog, "sk": sk_current, "finished": (prog >= complete_at), "paused": False}
                return {"ok": True, "suppressed": True}
            last_stop_ts = float(st.get("last_stop_ts") or 0)
            guard_window = max(guard_autoplay, post_stop_guard, anti_autoplay)
            if guard_window > 0 and last_stop_ts > 0 and (now - last_stop_ts) <= guard_window and prog < max(0.0, start_guard_min):
                _emit(logger, f"suppress autoplay start dt={now - last_stop_ts:.1f}s p={prog:.1f}% (<{start_guard_min}%)", "DEBUG")
                _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog, "sk": sk_current, "finished": False, "paused": False, "last_stop_ts": last_stop_ts}
                return {"ok": True, "suppressed": True}

        if intended == "/scrobble/stop":
            if prog < stop_pause_threshold: intended = "/scrobble/pause"
            elif prog < complete_at: intended = "/scrobble/pause"
            elif last_prog >= 0 and (prog - last_prog) >= 30 and last_prog < stop_pause_threshold and prog >= 98:
                _emit(logger, f"Demote STOP→PAUSE jump {last_prog:.0f}%→{prog:.0f}% (thr={stop_pause_threshold})", "DEBUG")
                intended = "/scrobble/pause"; prog = last_prog

        if intended == "/scrobble/start" and prog < 1.0: prog = 1.0
        if intended == "/scrobble/pause" and prog < 0.1: prog = 0.1

        if ev_lc in ("playbackstop", "playbackstopped") and st.get("last_event") in ("playbackstop", "playbackstopped") and abs((st.get("prog", 0.0)) - prog) <= 1.0:
            _emit(logger, "suppress duplicate stop", "DEBUG")
            _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog, "sk": sk_current, "finished": (prog >= complete_at), "paused": st.get("paused"), "last_stop_ts": now}
            return {"ok": True, "suppressed": True}

        try:
            stop_flag = (intended == "/scrobble/stop")

            if media_type == "episode":
                title = (md.get("SeriesName") or md.get("Name") or "").strip()
            else:
                title = (md.get("Name") or md.get("SeriesName") or "").strip()

            year = md.get("ProductionYear") or md.get("Year")

            season_val = None
            episode_val = None
            if media_type == "episode":
                try:
                    season_val, episode_val = _episode_numbers(md, payload)
                except Exception:
                    season_val = episode_val = None

            duration_ms = None
            try:
                rticks = md.get("RunTimeTicks") or payload.get("RunTimeTicks")
                if rticks:
                    duration_ms = to_ms(rticks)
            except Exception:
                duration_ms = None

            if intended == "/scrobble/start":
                cw_state = "playing"
            elif intended == "/scrobble/pause":
                cw_state = "paused"
            elif intended == "/scrobble/stop":
                cw_state = "stopped"
            else:
                cw_state = None

            _cw_update(
                source="jellyfintrakt",
                media_type=media_type,
                title=title,
                year=year,
                season=season_val,
                episode=episode_val,
                progress=prog,
                stop=stop_flag,
                duration_ms=duration_ms,
                cover=None,
                state=cw_state,
            )
        except Exception:
            pass

        body = _build_primary_body(media_type, dict(md), ids_all, prog, cfg, logger=logger, root=payload)
        if not body:
            _emit(logger, "no usable IDs; skip scrobble", "DEBUG")
            _SCROBBLE_STATE[sess] = {"ts": now, "last_event": ev_lc, "prog": prog, "sk": sk_current, "finished": (prog >= complete_at), "paused": st.get("paused")}
            return {"ok": True, "ignored": True}

        if intended == "/scrobble/stop" and prog >= complete_at and cancel_checkin_on_stop:
            try: _del_trakt("/checkin", cfg)
            except Exception: pass
            time.sleep(0.15)

        _emit(logger, f"trakt intent {intended} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")
        r = _post_trakt(intended, body, cfg)
        try: rj = r.json()
        except Exception: rj = {"raw": (r.text or "")[:200]}
        _emit(logger, f"trakt {intended} -> {r.status_code} action={rj.get('action') or intended.rsplit('/',1)[-1]}", "DEBUG")

        if r.status_code == 404 and media_type == "episode":
            epi_hint = _ids_from_providerids(md, md) or {}
            found = _guid_search_episode(epi_hint, cfg, logger=logger)
            if found:
                body2 = {"progress": float(round(prog, 2)), "episode": {"ids": found}}
                _emit(logger, f"trakt intent {intended} using {_ids_desc(found)} (rescue)", "DEBUG")
                r = _post_trakt(intended, body2, cfg)
                try: rj = r.json()
                except Exception: rj = {"raw": (r.text or "")[:200]}
                _emit(logger, f"trakt {intended} (rescue) -> {r.status_code}", "DEBUG")

        if r.status_code == 409 and intended == "/scrobble/stop":
            raw_txt = r.text or ""
            if ("expires_at" in raw_txt or "watched_at" in raw_txt):
                if prog >= complete_at and not (st.get("wl_removed") is True):
                    try:
                        _call_remove_across(ids_all or {}, media_type)
                        st = {**st, "wl_removed": True}
                    except Exception:
                        pass
                _SCROBBLE_STATE[sess] = {
                    "ts": now, "last_event": ev_lc, "last_pause_ts": st.get("last_pause_ts", 0),
                    "prog": prog, "sk": sk_current, "finished": True,
                    **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
                    "paused": False,
                    "last_stop_ts": now,
                }
                return {"ok": True, "status": 200, "action": intended, "trakt": rj, "note": "409_checkin"}

        if r.status_code < 400:
            if intended == "/scrobble/stop" and prog >= complete_at and not (st.get("wl_removed") is True):
                try:
                    _call_remove_across(ids_all or {}, media_type)
                    st = {**st, "wl_removed": True}
                except Exception:
                    pass
            _SCROBBLE_STATE[sess] = {
                "ts": now, "last_event": ev_lc,
                "last_pause_ts": (now if intended == "/scrobble/pause" else st.get("last_pause_ts", 0)),
                "prog": prog,
                "sk": sk_current,
                "finished": (intended == "/scrobble/stop" and prog >= complete_at),
                **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
                "paused": (intended == "/scrobble/pause"),
                **({"last_stop_ts": now} if intended == "/scrobble/stop" else {}),
            }
            try:
                action_name = intended.rsplit("/", 1)[[-1]][0]
            except Exception:
                action_name = intended.rsplit("/", 1)[-1]
            try:
                _emit(logger, f"user='{acc_title}' {action_name} {prog:.1f}% • {media_name_dbg}", "WebHook")
            except Exception:
                pass
            return {"ok": True, "status": 200, "action": intended, "trakt": rj}

        _emit(logger, f"{intended} {r.status_code} {(str(rj)[:180])}", "ERROR")
        _SCROBBLE_STATE[sess] = {
            "ts": now,
            "last_event": ev_lc,
            "last_pause_ts": st.get("last_pause_ts", 0),
            "prog": prog,
            "sk": sk_current,
            "finished": (prog >= complete_at),
            **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
            "paused": st.get("paused") if paused_flag is None else paused_flag,
        }
        return {"ok": False, "status": r.status_code, "trakt": rj}
    except Exception as e:
        _emit(logger, f"process_webhook error: {e}", "ERROR")
        return {"ok": False, "error": str(e)}

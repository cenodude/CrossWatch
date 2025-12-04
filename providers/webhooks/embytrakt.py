# providers/webhooks/embytrakt.py
# CrossWatch - Emby Trakt Scrobbler Webhook Module
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
from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from api.watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except Exception:
    _rm_across_api = None

TRAKT_API = "https://api.trakt.tv"
_SCROBBLE_STATE: Dict[str, Dict[str, Any]] = {}
_TRAKT_ID_CACHE: Dict[tuple, Any] = {}

_DEF_WEBHOOK = {
    "pause_debounce_seconds": 5,
    "suppress_start_at": 99,
    "filters_emby": {"username_whitelist": []},
    "suppress_autoplay_seconds": 0,
    "post_stop_play_guard_seconds": 0,
    "start_guard_min_progress": 0,
    "guard_autoplay_seconds": 0,
    "cancel_checkin_on_stop": True,
    "anti_autoplay_seconds": 0,
}
_DEF_TRAKT = {
    "stop_pause_threshold": 80,
    "force_stop_at": 95,
    "regress_tolerance_percent": 5,
    "complete_at": 95,
}

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
    if "filters_emby" not in wh:
        wh["filters_emby"] = {"username_whitelist": []}; changed = True
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
    return r

def _cache_get(key: tuple) -> Optional[Any]:
    try:
        return _TRAKT_ID_CACHE.get(key)
    except Exception:
        return None

def _cache_put(key: tuple, value: Any) -> None:
    try:
        if len(_TRAKT_ID_CACHE) > 2048:
            _TRAKT_ID_CACHE.clear()
        _TRAKT_ID_CACHE[key] = value
    except Exception:
        pass

def _as_bool(v: Any) -> Optional[bool]:
    if isinstance(v, bool): return v
    if isinstance(v, (int, float)): return bool(v)
    if isinstance(v, str):
        s = v.strip().lower()
        if s in ("true","1","yes","y","on"): return True
        if s in ("false","0","no","n","off"): return False
    return None

def _grab(m: Mapping[str, Any], keys: list[str]) -> Any:
    for k in keys:
        if k in m and m[k] is not None:
            return m[k]
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

def _progress(payload: Mapping[str, Any], md: Mapping[str, Any]) -> float:
    pbinfo = payload.get("PlaybackInfo") or {}
    if pbinfo.get("PlayedToCompletion") is True:
        return 100.0

    if isinstance(payload.get("Progress"), (int, float)):
        return round(max(0.0, min(100.0, float(payload["Progress"]))), 2)

    ps = payload.get("PlayState") or {}
    pb = payload.get("Playback") or {}
    pos = (
        payload.get("SessionPlaybackPositionTicks") or
        pbinfo.get("PositionTicks") or
        payload.get("PlaybackPositionTicks") or payload.get("PositionTicks") or payload.get("PositionMs") or
        ps.get("PositionTicks") or ps.get("PositionMs") or pb.get("PositionTicks") or pb.get("PositionMs") or 0
    )
    dur = (
        (md.get("RunTimeTicks") or 0) or payload.get("RunTimeTicks") or ps.get("RunTimeTicks") or
        pb.get("RunTimeTicks") or payload.get("DurationMs") or 0
    )
    def to_ms(v: Any) -> float:
        try: v = float(v)
        except Exception: return 0.0
        return v / 10_000 if v > 10_000_000 else v
    pos_ms, dur_ms = to_ms(pos), to_ms(dur)
    if dur_ms <= 0: return 0.0
    return round(max(0.0, min(100.0, (pos_ms * 100.0) / dur_ms)), 2)

def _episode_numbers(md: Mapping[str, Any], root: Mapping[str, Any]) -> tuple[Optional[int], Optional[int]]:
    s = (
        md.get("ParentIndexNumber") or md.get("SeasonIndexNumber") or md.get("ItemParentIndex") or
        _grab(root, ["SeasonNumber","season","ItemParentIndex"])
    )
    e = (
        md.get("IndexNumber") or md.get("ItemIndex") or
        _grab(root, ["EpisodeNumber","episode","ItemIndex"])
    )
    try: s = int(s)
    except Exception: s = None
    try: e = int(e)
    except Exception: e = None
    return s, e

def _ids_from_providerids(md: Mapping[str, Any], root: Mapping[str, Any]) -> Dict[str, Any]:
    pids = md.get("ProviderIds") or root.get("ProviderIds") or {}
    out = {}
    def put(k: str, v: Any):
        if v is None: return
        sv = str(v).strip()
        if sv: out[k] = sv if k != "tmdb" else int(sv) if sv.isdigit() else sv
    put("imdb", pids.get("Imdb") or pids.get("IMDb"))
    put("tmdb", pids.get("Tmdb") or pids.get("TMDB") or pids.get("TheMovieDb"))
    put("tvdb", pids.get("Tvdb") or pids.get("TVDB") or pids.get("TheTVDB"))
    return out

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
    base = str(
        payload.get("PlaySessionId") or payload.get("SessionId") or payload.get("SessionID") or
        payload.get("DeviceId") or payload.get("DeviceID") or "n/a"
    )
    return base + "|" + _session_media_key(md, ids_all, root=payload)

def _guid_search_episode(epi_hint: Dict[str, Any], cfg: Dict[str, Any], logger=None) -> Dict[str, Any]:
    try:
        q = {k: epi_hint.get(k) for k in ("imdb","tmdb","tvdb") if epi_hint.get(k)}
        if not q:
            return {}
        r = requests.get(f"{TRAKT_API}/search/episode", params=q, headers=_headers(cfg), timeout=10)
        if r.status_code != 200:
            return {}
        arr = r.json() or []
        for it in arr:
            ep = (it or {}).get("episode") or {}
            ids = (ep.get("ids") or {})
            if ids.get("trakt"):
                return {k: ids[k] for k in ("trakt","tmdb","imdb","tvdb") if ids.get(k)}
    except Exception:
        pass
    return {}

def _show_ids_from_episode_hint(
    ids_hint: Dict[str, Any],
    cfg: Dict[str, Any],
    logger=None,
) -> Dict[str, Any]:
    cache_key = (
        "show_ids_from_episode_hint",
        ids_hint.get("imdb"),
        ids_hint.get("tmdb"),
        ids_hint.get("tvdb"),
    )

    c = _cache_get(cache_key)
    if isinstance(c, dict):
        return c
    if c is not None:
        return {}

    try:
        out = _guid_search_episode(ids_hint, cfg, logger=logger)
        if out:
            _cache_put(cache_key, out)
            return out
    except Exception:
        pass

    for key in ("tmdb", "imdb", "tvdb"):
        val = ids_hint.get(key)
        if not val:
            continue
        try:
            r = requests.get(
                f"{TRAKT_API}/search/{key}/{val}",
                params={"type": "episode", "limit": 1},
                headers=_headers(cfg),
                timeout=10,
            )
            if r.status_code != 200:
                continue
            arr = r.json() or []
        except Exception:
            continue

        for hit in arr:
            show_ids = ((hit.get("show") or {}).get("ids") or {})
            out = {k: show_ids[k] for k in ("trakt", "tmdb", "imdb", "tvdb") if show_ids.get(k)}
            if out:
                _emit(logger, f"resolved SHOW ids from episode hint: {out}", "DEBUG")
                _cache_put(cache_key, out)
                return out

    _cache_put(cache_key, None)
    return {}

def _resolve_episode_by_showids(show_ids: Dict[str, Any], s: int, n: int, cfg: Dict[str, Any], logger=None) -> Optional[int]:
    try:
        tid = show_ids.get("trakt")
        if not tid:
            return None
        r = requests.get(f"{TRAKT_API}/shows/{tid}/seasons/{s}", headers=_headers(cfg), timeout=10)
        if r.status_code != 200:
            return None
        eps = r.json() or []
        for ep in eps:
            if int(ep.get("number") or -1) == int(n):
                ids = (ep.get("ids") or {})
                if ids.get("trakt"):
                    return int(ids["trakt"])
    except Exception:
        pass
    return None

def _series_ids_from_payload(md: Mapping[str, Any], root: Mapping[str, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    t_md = str(md.get("Type") or "").strip().lower()
    t_root = str(root.get("Type") or "").strip().lower()
    type_val = t_md or t_root
    sp = (md.get("SeriesProviderIds") or root.get("SeriesProviderIds") or {}) or {}

    def maybe_int(v: Any) -> Any:
        s = str(v).strip()
        return int(s) if s.isdigit() else (s if s else None)

    def norm_imdb(v: Any) -> Optional[str]:
        s = str(v).strip()
        if not s:
            return None
        return s if s.startswith("tt") else f"tt{s}"

    tvdb = (
        sp.get("Tvdb") or sp.get("tvdb") or sp.get("TVDB") or sp.get("TheTVDB")
        or root.get("SeriesTvdbId") or root.get("SeriesTvdb")
    )
    tmdb = (
        sp.get("Tmdb") or sp.get("tmdb") or sp.get("TMDB") or sp.get("TheMovieDb")
        or root.get("SeriesTmdbId") or root.get("SeriesTmdb")
    )
    imdb = (
        sp.get("Imdb") or sp.get("imdb") or sp.get("IMDb")
        or root.get("SeriesImdbId") or root.get("SeriesImdb")
    )

    if not (tvdb or tmdb or imdb) and type_val in ("series", "tvshow"):
        pids = (md.get("ProviderIds") or root.get("ProviderIds") or {}) or {}
        tvdb = tvdb or (pids.get("Tvdb") or pids.get("tvdb") or pids.get("TVDB") or pids.get("TheTVDB"))
        tmdb = tmdb or (pids.get("Tmdb") or pids.get("tmdb") or pids.get("TMDB") or pids.get("TheMovieDb"))
        imdb = imdb or (pids.get("Imdb") or pids.get("imdb") or pids.get("IMDb"))

    v_tvdb = maybe_int(tvdb) if tvdb is not None else None
    v_tmdb = maybe_int(tmdb) if tmdb is not None else None
    v_imdb = norm_imdb(imdb) if imdb is not None else None

    if v_tvdb is not None:
        out["tvdb"] = v_tvdb
    if v_tmdb is not None:
        out["tmdb"] = v_tmdb
    if v_imdb:
        out["imdb"] = v_imdb

    return out

def _cw_ids_for_payload(
    media_type: str,
    md: Mapping[str, Any],
    ids_all: Dict[str, Any],
    cfg: Dict[str, Any],
    root: Optional[Mapping[str, Any]] = None,
    logger=None,
) -> Dict[str, Any]:

    cw_ids: Dict[str, Any] = dict(ids_all or {})
    mt = (media_type or "").strip().lower()
    if mt != "episode":
        return cw_ids

    root = root or md

    try:
        show_ids = _series_ids_from_payload(md, root) or {}
    except Exception:
        show_ids = {}

    # GET /Users/{UserId}/Items/{SeriesId}
    if not show_ids:
        try:
            e = cfg.get("emby") or {}
            base = str(e.get("server", "")).strip().rstrip("/")
            tok = str(e.get("access_token") or "").strip()
            uid = (
                str(root.get("UserId") or "") or
                str((root.get("User") or {}).get("Id") or "") or
                str(e.get("user_id") or "")
            ).strip()

            grand_id = md.get("SeriesId") or md.get("ParentId") or md.get("SeriesItemId")

            if base and "://" not in base:
                base = "http://" + base

            if base and tok and uid and grand_id:
                url = f"{base}/Users/{uid}/Items/{grand_id}?format=json"
                headers = {
                    "Accept": "application/json",
                    "X-Emby-Token": tok,
                    "X-MediaBrowser-Token": tok,
                }
                timeout = float(e.get("timeout", 6))
                verify = bool(e.get("verify_ssl", True))

                r = requests.get(url, headers=headers, timeout=timeout, verify=verify)
                if r.status_code == 200:
                    info = r.json() or {}
                    show_ids = _series_ids_from_payload(info, info) or {}
                    if logger:
                        logger(
                            f"resolved show ids via Emby {url.replace(base, '')}: {show_ids}",
                            level="DEBUG",
                            module="SCROBBLE",
                        )
        except Exception as e:
            if logger:
                logger(f"Emby show metadata fetch failed: {e}", level="DEBUG", module="SCROBBLE")

    for key in ("imdb", "tmdb", "tvdb"):
        val = show_ids.get(key)
        if val is not None:
            cw_ids.setdefault(f"{key}_show", val)

    if "tmdb_show" not in cw_ids:
        try:
            try:
                hint = {**(_ids_from_providerids(md, root) or {}), **(ids_all or {})}
            except Exception:
                hint = dict(ids_all or {})

            extra = _show_ids_from_episode_hint(hint, cfg, logger=logger) or {}
            for key in ("imdb", "tmdb", "tvdb"):
                val = extra.get(key)
                if val is not None:
                    cw_ids.setdefault(f"{key}_show", val)
        except Exception:
            pass

    return cw_ids

def _build_primary_body(media_type: str, md: Dict[str, Any], ids_all: Dict[str, Any], prog: float, cfg: Dict[str, Any], logger=None, root=None) -> Dict[str, Any]:
    p = float(round(prog, 2))
    if media_type == "movie":
        ids = {k: ids_all[k] for k in ("trakt", "tmdb", "imdb") if ids_all.get(k)}
        return {"progress": p, "movie": {"ids": ids}} if ids else {}

    if media_type == "episode":
        s, n = _episode_numbers(md, root or md)
        show_ids = {}
        try:
            show_ids = _series_ids_from_payload(md, root or md) or {}
        except Exception:
            pass

        if not show_ids:
            mapped = {}
            for src, dest in (("imdb_show", "imdb"), ("tmdb_show", "tmdb"), ("tvdb_show", "tvdb")):
                val = ids_all.get(src)
                if val is not None:
                    mapped[dest] = val
            show_ids = mapped

        if show_ids and isinstance(s, int) and isinstance(n, int):
            etid = _resolve_episode_by_showids(show_ids, s, n, cfg, logger=logger)
            if isinstance(etid, int):
                return {"progress": p, "episode": {"ids": {"trakt": etid}}}
            return {"progress": p, "show": {"ids": show_ids}, "episode": {"season": s, "number": n}}
    return {}

def _body_ids_desc(b: Dict[str, Any]) -> str:
    ids = ((b.get("movie") or {}).get("ids")) or ((b.get("show") or {}).get("ids")) or ((b.get("episode") or {}).get("ids"))
    return str(ids or "none")

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
        allow_users = set(((wh.get("filters_emby") or {}).get("username_whitelist") or []))
        ga_raw = wh.get("guard_autoplay_seconds")
        if ga_raw is None:
            ga_raw = wh.get("suppress_autoplay_seconds", 0)
        try:
            guard_autoplay = float(ga_raw)
        except Exception:
            guard_autoplay = 0.0
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
        for k_src, k_norm in [
            ("Provider_tmdb", "Tmdb"),
            ("Provider_imdb", "Imdb"),
            ("Provider_tvdb", "Tvdb"),
            ("TheMovieDb", "TheMovieDb"),
            ("TheTVDB", "TheTVDB"),
        ]:
            if payload.get(k_src) and not pids.get(k_norm):
                pids[k_norm] = payload[k_src]
        if pids:
            md["ProviderIds"] = pids

        event = (payload.get("NotificationType") or payload.get("Event") or "").strip().lower()
        if "." in event:
            event = event.replace(".", "")
        event = event.replace("_", "")

        ids_all = _ids_from_providerids(md, payload) or {}
        ses = _make_session_id(payload, md, ids_all)

        acc_title = (
            ((payload.get("User") or {}).get("Name"))
            or payload.get("UserName")
            or ((payload.get("Server") or {}).get("UserName"))
            or "unknown"
        ).strip()

        if allow_users and acc_title and acc_title not in allow_users:
            _emit(logger, f"user '{acc_title}' blocked by filters_emby", "DEBUG")
            return {"ok": True, "ignored": True}

        media_type = (md.get("Type") or md.get("type") or "").strip().lower()
        if media_type in ("movie","movies"):
            media_type = "movie"
        elif media_type in ("episode","episodes"):
            media_type = "episode"
        else:
            _emit(logger, f"unsupported media type '{media_type}'", "DEBUG")
            return {"ok": True, "ignored": True}

        prog = _progress(payload, md)
        paused_flag = _extract_paused(payload)
        now = time.time()
        st = _SCROBBLE_STATE.get(ses) or {}
        ev_lc = event

        intended = None
        if ev_lc in ("playbackstart","playbackunpause","play","playing","resume","unpause"):
            intended = "/scrobble/start"
        elif ev_lc in ("playbackpause","pause","paused"):
            intended = "/scrobble/pause"
        elif ev_lc in ("playbackstop","playbackstopped","playbackscrobble","stop","stopped","scrobble"):
            intended = "/scrobble/stop"
        else:
            _emit(logger, f"ignore event '{event}'", "DEBUG")
            return {"ok": True, "ignored": True}

        if intended == "/scrobble/start" and prog < 1.0:
            prog = 1.0

        # Trakt requires >= 1.0% for pause/stop
        if intended in ("/scrobble/pause", "/scrobble/stop") and prog < 1.0:
            prog = 1.0

        if intended == "/scrobble/pause":
            last_pause_ts = st.get("last_pause_ts", 0)
            if pause_debounce and (now - last_pause_ts) < pause_debounce:
                _emit(logger, "debounce pause", "DEBUG")
                _SCROBBLE_STATE[ses] = {**st, "ts": now, "last_event": ev_lc, "prog": prog, "paused": True, "last_pause_ts": last_pause_ts}
                return {"ok": True, "debounced": True}

        if intended == "/scrobble/start" and prog >= suppress_start_at:
            _emit(logger, "suppress late start", "DEBUG")
            _SCROBBLE_STATE[ses] = {"ts": now, "last_event": ev_lc, "prog": prog}
            return {"ok": True, "suppressed": True}

        if guard_autoplay and intended == "/scrobble/start":
            if prog < guard_autoplay:
                _emit(logger, "guard autoplay start", "DEBUG")
                _SCROBBLE_STATE[ses] = {"ts": now, "last_event": ev_lc, "prog": prog}
                return {"ok": True, "ignored": True}

        if start_guard_min and intended == "/scrobble/start":
            if prog < start_guard_min:
                _emit(logger, "start guard min progress", "DEBUG")
                _SCROBBLE_STATE[ses] = {"ts": now, "last_event": ev_lc, "prog": prog}
                return {"ok": True, "ignored": True}

        if anti_autoplay and intended == "/scrobble/start":
            last_stop_ts = st.get("last_stop_ts", 0)
            if last_stop_ts and (now - last_stop_ts) < anti_autoplay:
                _emit(logger, "anti autoplay start", "DEBUG")
                _SCROBBLE_STATE[ses] = {"ts": now, "last_event": ev_lc, "prog": prog}
                return {"ok": True, "ignored": True}

        if intended == "/scrobble/stop" and post_stop_guard:
            last_play_ts = st.get("last_play_ts", 0)
            if last_play_ts and (now - last_play_ts) < post_stop_guard:
                _emit(logger, "post stop guard", "DEBUG")
                _SCROBBLE_STATE[ses] = {**st, "ts": now, "last_event": ev_lc, "prog": prog}
                return {"ok": True, "ignored": True}

        if intended == "/scrobble/stop":
            if prog >= force_stop_at:
                prog = 100.0
            elif prog >= stop_pause_threshold:
                prog = max(prog, stop_pause_threshold)

        st_prog = float(st.get("prog", 0.0) or 0.0)
        if intended != "/scrobble/start" and st_prog and (st_prog - prog) > regress_tol:
            _emit(logger, f"regress {st_prog}->{prog} within tol {regress_tol}", "DEBUG")
            prog = st_prog
        cw_ids: Dict[str, Any] = dict(ids_all)

        try:
            stop_flag = (intended == "/scrobble/stop")
            if media_type == "episode":
                title = (md.get("SeriesName") or md.get("Name") or "").strip()
            else:
                title = (md.get("Name") or md.get("SeriesName") or "").strip()

            year = md.get("ProductionYear") or md.get("Year")
            season_val = episode_val = None
            if media_type == "episode":
                season_val, episode_val = _episode_numbers(md, payload)

            duration_ms = None
            try:
                rticks = md.get("RunTimeTicks") or payload.get("RunTimeTicks")
                if rticks:
                    duration_ms = rticks / 10_000
            except Exception:
                duration_ms = None

            cw_state = (
                "playing" if intended == "/scrobble/start"
                else "paused" if intended == "/scrobble/pause"
                else "stopped" if intended == "/scrobble/stop"
                else None
            )

            cw_ids = _cw_ids_for_payload(media_type, md, ids_all, cfg, root=payload, logger=logger)

            _cw_update(
                source="embytrakt",
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
                ids=cw_ids,
            )
        except Exception:
            pass

        body = _build_primary_body(media_type, dict(md), cw_ids, prog, cfg, logger=logger, root=payload)

        if not body:
            _emit(logger, "no usable IDs; skip scrobble", "DEBUG")
            _SCROBBLE_STATE[ses] = {
                "ts": now,
                "last_event": ev_lc,
                "last_pause_ts": st.get("last_pause_ts", 0),
                "prog": prog,
                "finished": (prog >= complete_at),
                "wl_removed": st.get("wl_removed"),
                "paused": st.get("paused"),
            }
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
                _emit(logger, f"trakt intent {intended} using {_body_ids_desc(body2)} (rescue)", "DEBUG")
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
                _SCROBBLE_STATE[ses] = {
                    "ts": now, "last_event": ev_lc, "last_pause_ts": st.get("last_pause_ts", 0),
                    "prog": prog, "finished": True,
                    **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
                    "paused": False, "last_stop_ts": now,
                }
                return {"ok": True, "status": 200, "action": intended, "trakt": rj, "note": "409_checkin"}
    
        if r.status_code < 400:
            if intended == "/scrobble/stop" and prog >= complete_at and not (st.get("wl_removed") is True):
                try:
                    _call_remove_across(ids_all or {}, media_type)
                    st = {**st, "wl_removed": True}
                except Exception:
                    pass

            _SCROBBLE_STATE[ses] = {
                "ts": now,
                "last_event": ev_lc,
                "last_pause_ts": (now if intended == "/scrobble/pause" else st.get("last_pause_ts", 0)),
                "prog": prog,
                "finished": (intended == "/scrobble/stop" and prog >= complete_at),
                **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
                "paused": (intended == "/scrobble/pause"),
                **({"last_stop_ts": now} if intended == "/scrobble/stop" else {}),
                **({"last_play_ts": now} if intended == "/scrobble/start" else {}),
            }

            try:
                action_name = intended.rsplit("/", 1)[-1]
                name_dbg = (md.get("SeriesName") or md.get("Name") or "").strip() or "?"
                if media_type == "episode":
                    s_num, e_num = _episode_numbers(md, payload)
                    if isinstance(s_num, int) and isinstance(e_num, int):
                        name_dbg = f"{name_dbg} S{s_num:02d}E{e_num:02d}"
                _emit(logger, f"user='{acc_title}' {action_name} {prog:.1f}% • {name_dbg}", "INFO")
            except Exception:
                pass

            return {"ok": True, "status": 200, "action": intended, "trakt": rj}

        _emit(logger, f"{intended} {r.status_code} {(str(rj)[:180])}", "ERROR")
        _SCROBBLE_STATE[ses] = {
            "ts": now, "last_event": ev_lc, "last_pause_ts": st.get("last_pause_ts", 0),
            "prog": prog, "finished": (prog >= complete_at),
            **({"wl_removed": st.get("wl_removed")} if st.get("wl_removed") else {}),
            "paused": st.get("paused") if paused_flag is None else paused_flag,
        }
        return {"ok": False, "status": r.status_code, "trakt": rj}
    except Exception as e:
        _emit(logger, f"process_webhook error: {e}", "ERROR")
        return {"ok": False, "error": str(e)}

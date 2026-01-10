# providers/scrobble/emby/watch.py
# CrossWatch - Emby Watcher Service
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re, time, threading
from typing import Any, Iterable, Mapping

import requests

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.config_base import load_config
from providers.scrobble.scrobble import Dispatcher, ScrobbleSink, ScrobbleEvent, MediaType
from providers.scrobble.currently_watching import update_from_event as _cw_update

TRAKT_API = "https://api.trakt.tv"
_TRAKT_ID_CACHE: dict[tuple, Any] = {}
_VIEW_CACHE_TTL_SECS = 60.0
_SERIES_IDS_CACHE: dict[tuple[str, str, str], tuple[float, dict[str, Any] | None]] = {}
_SERIES_IDS_CACHE_TTL_SECS = 60 * 60
_SERIES_IDS_NEG_TTL_SECS = 60
_SERIES_IDS_CACHE_MAX = 4096
_SERIES_IDS_CACHE_MISS = object()


def _series_ids_cache_get(base: str, uid: str, series_id: str) -> dict[str, Any] | None | object:
    try:
        bkey = str(base).rstrip("/").lower()
        key = (bkey, str(uid), str(series_id))
        hit = _SERIES_IDS_CACHE.get(key)
        if not hit:
            return _SERIES_IDS_CACHE_MISS
        exp, val = hit
        if time.time() > float(exp or 0.0):
            try:
                _SERIES_IDS_CACHE.pop(key, None)
            except Exception:
                pass
            return _SERIES_IDS_CACHE_MISS
        return val
    except Exception:
        return _SERIES_IDS_CACHE_MISS


def _series_ids_cache_put(base: str, uid: str, series_id: str, val: dict[str, Any] | None) -> None:
    try:
        if len(_SERIES_IDS_CACHE) > _SERIES_IDS_CACHE_MAX:
            _SERIES_IDS_CACHE.clear()
        bkey = str(base).rstrip("/").lower()
        key = (bkey, str(uid), str(series_id))
        v = dict(val or {})
        ttl = _SERIES_IDS_CACHE_TTL_SECS if v else _SERIES_IDS_NEG_TTL_SECS
        _SERIES_IDS_CACHE[key] = (time.time() + float(ttl), (v or None))
    except Exception:
        pass



def _trakt_tokens(cfg: dict[str, Any]) -> dict[str, str]:
    tr = cfg.get("trakt") or {}
    au = (cfg.get("auth") or {}).get("trakt") or {}
    return {
        "client_id": str(tr.get("client_id") or "").strip(),
        "access_token": str(au.get("access_token") or tr.get("access_token") or "").strip(),
    }


def _trakt_headers(cfg: dict[str, Any]) -> dict[str, str]:
    t = _trakt_tokens(cfg)
    headers: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": t["client_id"],
    }
    if t["access_token"]:
        headers["Authorization"] = f"Bearer {t['access_token']}"
    return headers


def _cache_get(key: tuple) -> Any | None:
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


def _cfg() -> dict[str, Any]:
    try:
        return load_config()
    except Exception:
        return {}


def _as_set_str(v: Any) -> set[str]:
    it = v if isinstance(v, (list, tuple, set)) else ([v] if v is not None else [])
    out: set[str] = set()
    for x in it:
        s = str(x).strip()
        if s:
            out.add(s)
    return out


def _is_debug() -> bool:
    try:
        v = ((_cfg().get("runtime") or {}).get("debug"))
        if isinstance(v, bool):
            return v
        if isinstance(v, (int, float)):
            return v != 0
        if isinstance(v, str):
            return v.strip().lower() in ("1", "true", "yes", "on", "y", "t")
        return False
    except Exception:
        return False


def _emby_bt(cfg: dict[str, Any]) -> tuple[str, str]:
    e = cfg.get("emby") or {}
    base = str(e.get("server", "")).strip().rstrip("/")
    tok = str(e.get("access_token", "")).strip()
    if not base or not tok:
        return "", ""
    if "://" not in base:
        base = "http://" + base
    return base, tok


def _hdr(tok: str, cfg: dict[str, Any]) -> dict[str, str]:
    e = cfg.get("emby") or {}
    did = str(e.get("device_id") or "crosswatch")
    return {
        "Accept": "application/json",
        "X-Emby-Token": tok,
        "X-MediaBrowser-Token": tok,
        "Authorization": f'Emby Client="CrossWatch", Device="CrossWatch", DeviceId="{did}", Version="1.0.0"',
    }


def _get_json(base: str, tok: str, path: str) -> Any:
    cfg = _cfg()
    e = cfg.get("emby") or {}
    r = requests.get(
        f"{base}{path}",
        headers=_hdr(tok, cfg),
        timeout=float(e.get("timeout", 6)),
        verify=bool(e.get("verify_ssl", True)),
    )
    r.raise_for_status()
    return r.json()


def _ticks_to_pct(pos_ticks: Any, dur_ticks: Any) -> int:
    try:
        p = max(0, int(pos_ticks or 0))
        d = max(1, int(dur_ticks or 0))
        return max(0, min(100, int(round((p / float(d)) * 100))))
    except Exception:
        return 0


def _map_provider_ids(item: dict[str, Any]) -> dict[str, Any]:
    ids: dict[str, Any] = {}
    prov = item.get("ProviderIds") or {}

    def put(k: str, v: Any) -> None:
        if v:
            ids[k] = str(v)

    put("imdb", prov.get("Imdb"))
    put("tmdb", prov.get("Tmdb") or prov.get("TmdbId"))
    put("tvdb", prov.get("Tvdb") or prov.get("TvdbId"))

    sprov = item.get("SeriesProviderIds") or {}
    if sprov:
        if sprov.get("Imdb"):
            ids["imdb_show"] = str(sprov.get("Imdb"))
        if sprov.get("Tmdb") or sprov.get("TmdbId"):
            ids["tmdb_show"] = str(sprov.get("Tmdb") or sprov.get("TmdbId"))
        if sprov.get("Tvdb") or sprov.get("TvdbId"):
            ids["tvdb_show"] = str(sprov.get("Tvdb") or sprov.get("TvdbId"))

    return ids


def _ids_from_providerids(md: Mapping[str, Any], root: Mapping[str, Any]) -> dict[str, Any]:
    pids = md.get("ProviderIds") or root.get("ProviderIds") or {}
    out: dict[str, Any] = {}

    def put(k: str, v: Any) -> None:
        if v is None:
            return
        sv = str(v).strip()
        if not sv:
            return
        if k == "tmdb" and sv.isdigit():
            out[k] = int(sv)
        else:
            out[k] = sv

    put("imdb", pids.get("Imdb") or pids.get("IMDb"))
    put("tmdb", pids.get("Tmdb") or pids.get("TMDB") or pids.get("TheMovieDb"))
    put("tvdb", pids.get("Tvdb") or pids.get("TVDB") or pids.get("TheTVDB"))
    return out


def _series_ids_from_payload(md: Mapping[str, Any], root: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    t_md = str(md.get("Type") or "").strip().lower()
    t_root = str(root.get("Type") or "").strip().lower()
    type_val = t_md or t_root
    sp = (md.get("SeriesProviderIds") or root.get("SeriesProviderIds") or {}) or {}

    def maybe_int(v: Any) -> Any:
        s = str(v).strip()
        return int(s) if s.isdigit() else (s if s else None)

    def norm_imdb(v: Any) -> str | None:
        s = str(v).strip()
        if not s:
            return None
        return s if s.startswith("tt") else f"tt{s}"

    tvdb = (
        sp.get("Tvdb")
        or sp.get("tvdb")
        or sp.get("TVDB")
        or sp.get("TheTVDB")
        or root.get("SeriesTvdbId")
        or root.get("SeriesTvdb")
    )
    tmdb = (
        sp.get("Tmdb")
        or sp.get("tmdb")
        or sp.get("TMDB")
        or sp.get("TheMovieDb")
        or root.get("SeriesTmdbId")
        or root.get("SeriesTmdb")
    )
    imdb = (
        sp.get("Imdb")
        or sp.get("imdb")
        or sp.get("IMDb")
        or root.get("SeriesImdbId")
        or root.get("SeriesImdb")
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


def _guid_search_episode(epi_hint: dict[str, Any], cfg: dict[str, Any], logger=None) -> dict[str, Any]:
    try:
        t = _trakt_tokens(cfg)
        if not t.get("client_id"):
            return {}
        q = {k: epi_hint.get(k) for k in ("imdb", "tmdb", "tvdb") if epi_hint.get(k)}
        if not q:
            return {}
        r = requests.get(
            f"{TRAKT_API}/search/episode",
            params=q,
            headers=_trakt_headers(cfg),
            timeout=10,
        )
        if r.status_code != 200:
            return {}
        arr = r.json() or []
        for it in arr:
            ep = (it or {}).get("episode") or {}
            ids = (ep.get("ids") or {})
            if ids.get("trakt"):
                return {k: ids[k] for k in ("trakt", "tmdb", "imdb", "tvdb") if ids.get(k)}
    except Exception:
        pass
    return {}


def _show_ids_from_episode_hint(ids_hint: dict[str, Any], cfg: dict[str, Any], logger=None) -> dict[str, Any]:
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

    t = _trakt_tokens(cfg)
    if not t.get("client_id"):
        _cache_put(cache_key, None)
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
                headers=_trakt_headers(cfg),
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
                if logger and _is_debug():
                    try:
                        logger(f"resolved SHOW ids from episode hint: {out}", "DEBUG")
                    except Exception:
                        pass
                _cache_put(cache_key, out)
                return out

    _cache_put(cache_key, None)
    return {}


def _enrich_episode_ids(
    item: dict[str, Any],
    sess: dict[str, Any],
    ids_all: dict[str, Any],
    logger=None,
) -> dict[str, Any]:
    cw_ids: dict[str, Any] = dict(ids_all or {})
    root: Mapping[str, Any] = sess or item
    cfg = _cfg()

    try:
        show_ids = _series_ids_from_payload(item, root) or {}
    except Exception:
        show_ids = {}

    if not show_ids:
        base, tok = _emby_bt(cfg)
        uid = str(root.get("UserId") or "").strip() or str((cfg.get("emby") or {}).get("user_id") or "").strip()
        series_id = item.get("SeriesId") or item.get("ParentId") or item.get("SeriesItemId")
        if base and tok and uid and series_id:
            sid = str(series_id).strip()
            cached = _series_ids_cache_get(base, uid, sid)
            if cached is not _SERIES_IDS_CACHE_MISS:
                if isinstance(cached, dict):
                    show_ids = dict(cached)
            else:
                path = f"/Users/{uid}/Items/{sid}?format=json"
                try:
                    info = _get_json(base, tok, path)
                    show_ids = _series_ids_from_payload(info, info) or {}
                    _series_ids_cache_put(base, uid, sid, show_ids or None)
                    if show_ids and logger and _is_debug():
                        logger(f"resolved show ids via Emby {path}: {show_ids}", "DEBUG")
                except Exception as e:
                    _series_ids_cache_put(base, uid, sid, None)
                    if logger and _is_debug():
                        logger(f"Emby series lookup failed: {e}", "DEBUG")

    for key in ("imdb", "tmdb", "tvdb"):
        val = show_ids.get(key)
        if val is not None and f"{key}_show" not in cw_ids:
            cw_ids[f"{key}_show"] = val

    if "tmdb_show" not in cw_ids:
        try:
            hint: dict[str, Any] = {}
            try:
                hint.update(_ids_from_providerids(item, root))
            except Exception:
                pass
            try:
                base_ids = ids_all or {}
                for key in ("imdb", "tmdb", "tvdb"):
                    val = base_ids.get(key)
                    if val is not None and key not in hint:
                        hint[key] = val
            except Exception:
                pass
            if hint:
                extra = _show_ids_from_episode_hint(hint, cfg, logger=logger) or {}
                for key in ("imdb", "tmdb", "tvdb"):
                    val = extra.get(key)
                    if val is not None and f"{key}_show" not in cw_ids:
                        cw_ids[f"{key}_show"] = val
        except Exception:
            pass

    return cw_ids


def _media_from_session(sess: dict[str, Any]) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    item = (sess.get("NowPlayingItem") or {})
    ps = (sess.get("PlayState") or {})
    return (item if item else None), ps


def _server_id(base: str, tok: str) -> str | None:
    try:
        info = _get_json(base, tok, "/System/Info/Public")
        return str(info.get("Id") or "") or None
    except Exception:
        return None


def _cfg_for_dispatch(server_id: str | None) -> dict[str, Any]:
    cfg = _cfg()
    if not server_id:
        return cfg
    cfg = dict(cfg)

    px = dict(cfg.get("plex") or {})
    px["server_uuid"] = server_id
    cfg["plex"] = px

    s = dict(cfg.get("scrobble") or {})
    w = dict(s.get("watch") or {})
    f = dict(w.get("filters") or {})

    if "server_uuid" in f:
        val = str(f.get("server_uuid") or "").strip()
        if not val or val != str(server_id):
            f["server_uuid"] = ""

    w["filters"] = f
    s["watch"] = w
    cfg["scrobble"] = s

    return cfg


def _ids_desc(ids: dict[str, Any] | None) -> str:
    d = ids or {}
    for k in ("trakt", "imdb", "tmdb", "tvdb"):
        if d.get(k):
            return f"{k}:{d[k]}"
    for k in ("trakt_show", "imdb_show", "tmdb_show", "tvdb_show"):
        if d.get(k):
            return f"{k.replace('_show', '')}:{d[k]}"
    return "none"


def _media_name(ev: ScrobbleEvent) -> str:
    if (ev.media_type or "").lower() == "episode":
        s = ev.season if isinstance(ev.season, int) else None
        n = ev.number if isinstance(ev.number, int) else None
        base = ev.title or "?"
        if s is not None and n is not None:
            return f"{base} S{s:02}E{n:02}"
        return base
    return ev.title or "?"


def _normalize_ids(ids: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in (ids or {}).items():
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        out[str(k)] = s
    return out


class EmbyWatchService:
    def __init__(self, sinks: Iterable[ScrobbleSink] | None = None, poll_secs: float = 1.5) -> None:
        self._base, self._tok = _emby_bt(_cfg())
        self._disabled = False
        if not self._base or not self._tok:
            self._disabled = True
            self._server_id: str | None = None
        else:
            self._server_id = _server_id(self._base, self._tok)

        self._sinks = list(sinks or [])
        self._dispatch = Dispatcher(self._sinks, cfg_provider=lambda: _cfg_for_dispatch(self._server_id))
        self._poll = max(0.5, float(poll_secs))
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None
        self._last: dict[str, dict[str, Any]] = {}
        self._last_emit: dict[str, tuple[str, int]] = {}
        self._allowed_sessions: set[str] = set()
        self._filtered_sessions: set[str] = set()
        self._dbg_last_total = None
        self._dbg_last_playing = None
        self._dbg_last_ts = 0.0
        self._cw_last_heartbeat: dict[str, float] = {}
        self._view_roots_cache: dict[str, tuple[float, set[str]]] = {}
        self._item_root_cache: dict[str, str | None] = {}
        self._log(f"Ensuring Watcher is running; wired sinks: {len(self._sinks)}", "INFO")

    def _log(self, msg: str, level: str = "INFO") -> None:
        lvl = (str(level) or "INFO").upper()
        if lvl == "DEBUG" and not _is_debug():
            return
        if BASE_LOG is not None:
            try:
                BASE_LOG(msg, level=lvl, module="EMBY ")
                return
            except Exception:
                pass
        ts = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print(f"[{ts}] [EMBY ] {lvl} {msg}")

    def _dbg(self, msg: str) -> None:
        self._log(msg, "DEBUG")

    def _scrobble_whitelist(self) -> set[str]:
        try:
            cfg = _cfg() or {}
            libs = ((((cfg.get("emby") or {}).get("scrobble") or {}).get("libraries")) or [])
            return _as_set_str(libs)
        except Exception:
            return set()

    def _view_roots(self, uid: str) -> set[str]:
        uid = str(uid or "").strip()
        if not uid:
            return set()
        now = time.time()
        cached = self._view_roots_cache.get(uid)
        if cached and (now - float(cached[0] or 0.0)) < _VIEW_CACHE_TTL_SECS:
            return cached[1]

        roots: set[str] = set()
        try:
            j = _get_json(self._base, self._tok, f"/Users/{uid}/Views") or {}
            items = (j.get("Items") if isinstance(j, dict) else None) or []
            for it in items:
                rid = (it or {}).get("Id") or (it or {}).get("Key")
                if rid is not None:
                    s = str(rid).strip()
                    if s:
                        roots.add(s)
        except Exception:
            roots = set()

        self._view_roots_cache[uid] = (now, roots)
        return roots

    def _view_id_via_ancestors(self, uid: str, item_id: str) -> str | None:
        uid = str(uid or "").strip()
        iid = str(item_id or "").strip()
        if not uid or not iid:
            return None

        if iid in self._item_root_cache:
            return self._item_root_cache[iid]

        roots = self._view_roots(uid)
        if not roots:
            self._item_root_cache[iid] = None
            return None

        found: str | None = None
        try:
            arr = _get_json(self._base, self._tok, f"/Items/{iid}/Ancestors?Fields=Id&UserId={uid}") or []
            if isinstance(arr, list):
                for a in arr:
                    aid = str((a or {}).get("Id") or "").strip()
                    if aid and aid in roots:
                        found = aid
                        break
        except Exception:
            found = None

        if len(self._item_root_cache) > 4096:
            self._item_root_cache.clear()
        self._item_root_cache[iid] = found
        return found

    def _session_view_id(self, sess: Mapping[str, Any]) -> str | None:
        uid = str(sess.get("UserId") or "").strip()
        item = (sess.get("NowPlayingItem") or {}) if isinstance(sess, Mapping) else {}
        iid = str((item.get("Id") or "") if isinstance(item, Mapping) else "").strip()
        if uid and iid:
            vid = self._view_id_via_ancestors(uid, iid)
            if vid:
                return vid

        sid = str((item.get("SeriesId") or item.get("ParentId") or "") if isinstance(item, Mapping) else "").strip()
        if uid and sid:
            return self._view_id_via_ancestors(uid, sid)
        return None

    def _passes_filters(self, ev: ScrobbleEvent) -> bool:
        sk = str(ev.session_key or "")
        if sk and sk in self._allowed_sessions:
            return True

        cfg = _cfg() or {}
        filt = (((cfg.get("scrobble") or {}).get("watch") or {}).get("filters") or {})
        wl = filt.get("username_whitelist")
        wl_list = wl if isinstance(wl, list) else ([wl] if wl else [])

        def norm(s: str) -> str:
            return re.sub(r"[^a-z0-9]+", "", (s or "").lower())

        user_ok = True
        if wl_list:
            user_ok = False
            if any(
                not str(x).lower().startswith(("id:", "uuid:"))
                and norm(str(x)) == norm(ev.account or "")
                for x in wl_list
            ):
                user_ok = True
            else:
                raw = ev.raw or {}
                uid = str(raw.get("UserId") or "").strip().lower()
                for e in wl_list:
                    s = str(e).strip().lower()
                    if s.startswith("id:") and uid and s.split(":", 1)[1].strip().lower() == uid:
                        user_ok = True
                        break
                    if s.startswith("uuid:") and uid and s.split(":", 1)[1].strip().lower() == uid:
                        user_ok = True
                        break
        if not user_ok:
            return False

        libs = self._scrobble_whitelist()
        if libs:
            view_id = self._session_view_id(ev.raw or {})
            if not view_id or view_id not in libs:
                if _is_debug():
                    item = ((ev.raw or {}).get("NowPlayingItem") or {}) if isinstance(ev.raw, Mapping) else {}
                    name = (item.get("Name") or item.get("SeriesName") or "?") if isinstance(item, Mapping) else "?"
                    self._dbg(f"event filtered by scrobble whitelist: view={view_id or 'none'} allowed={sorted(libs)} item={name}")
                return False

        if sk:
            self._allowed_sessions.add(sk)
        return True

    def _build_event(self, sess: dict[str, Any], action: str, progress: int) -> ScrobbleEvent | None:
        item, _ps = _media_from_session(sess)
        if not item:
            return None

        mtype = "episode" if (item.get("Type") or "").lower() == "episode" else "movie"
        ids_raw = _map_provider_ids(item)
        if mtype == "episode":
            ids_raw = _enrich_episode_ids(item, sess, ids_raw, logger=self._log)
        ids = _normalize_ids(ids_raw)

        title = item.get("SeriesName") if mtype == "episode" else item.get("Name") or item.get("OriginalTitle")
        year = item.get("ProductionYear")
        season = item.get("ParentIndexNumber") if mtype == "episode" else None
        number = item.get("IndexNumber") if mtype == "episode" else None

        mt: MediaType = "episode" if mtype == "episode" else "movie"
        act = "start" if action == "playing" else "pause" if action == "paused" else "stop"

        return ScrobbleEvent(
            action=act,
            media_type=mt,
            ids=ids,
            title=title,
            year=year,
            season=season,
            number=number,
            progress=progress,
            account=(sess.get("UserName") or sess.get("UserId") or None),
            server_uuid=self._server_id,
            session_key=str(sess.get("Id") or ""),
            raw=sess,
        )

    def _current_sessions(self) -> list[dict[str, Any]]:
        try:
            e = (_cfg().get("emby") or {})
            uid = str(e.get("user_id") or "").strip().lower()
            q = "/Sessions?ActiveWithinSeconds=15"
            all_sessions = _get_json(self._base, self._tok, q) or []
            playing: list[dict[str, Any]] = []
            for s in all_sessions:
                if not (s.get("NowPlayingItem") or {}):
                    continue
                if uid and str(s.get("UserId") or "").strip().lower() != uid:
                    continue
                playing.append(s)
            return playing
        except Exception as ex:
            self._log(f"session poll failed: {ex}", "ERROR")
            return []

    def _meta_from_event(self, ev: ScrobbleEvent) -> dict[str, Any]:
        return {
            "media_type": ev.media_type,
            "ids": dict(ev.ids or {}),
            "title": ev.title,
            "year": ev.year,
            "season": ev.season,
            "number": ev.number,
            "account": ev.account,
        }

    def _emit(self, ev: ScrobbleEvent) -> None:
        sk = str(ev.session_key or "")
        if not self._passes_filters(ev):
            if sk and sk not in self._filtered_sessions:
                self._dbg(f"event filtered: user={ev.account} server={ev.server_uuid}")
                self._filtered_sessions.add(sk)
            return

        if sk and sk in self._filtered_sessions:
            try:
                self._filtered_sessions.remove(sk)
            except Exception:
                pass

        if sk:
            last = self._last_emit.get(sk)
            if last and last[0] == ev.action and last[1] == ev.progress:
                self._dbg(f"suppress duplicate {ev.action} sess={sk} p={ev.progress}")
                return

        try:
            _cw_update("emby", ev)
        except Exception:
            pass

        act = "playing" if ev.action == "start" else ("paused" if ev.action == "pause" else "stop")
        self._log(
            f"incoming '{act}' user='{ev.account}' server='{ev.server_uuid}' media='{_media_name(ev)}'",
            "DEBUG",
        )
        self._log(f"ids resolved: {_media_name(ev)} -> {_ids_desc(ev.ids)}", "DEBUG")
        self._log(f"event {ev.action} {ev.media_type} user={ev.account} p={ev.progress} sess={sk}")
        self._dispatch.dispatch(ev)

        if sk:
            last_meta = self._last.get(sk) or {}
            last_meta["meta"] = self._meta_from_event(ev)
            self._last[sk] = last_meta
            self._last_emit[sk] = (ev.action, ev.progress)

    def _tick(self) -> None:
        now = time.time()
        cur = self._current_sessions()
        seen: set[str] = set()

        try:
            debounce = float((((_cfg().get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds") or 0))
        except Exception:
            debounce = 0.0
        try:
            suppress_start_at = int((((_cfg().get("scrobble") or {}).get("watch") or {}).get("suppress_start_at") or 0))
        except Exception:
            suppress_start_at = 0
        try:
            force_at = int((((_cfg().get("scrobble") or {}).get("trakt") or {}).get("force_stop_at") or 95))
        except Exception:
            force_at = 95
        try:
            hb = float((((_cfg().get("scrobble") or {}).get("watch") or {}).get("cw_heartbeat_seconds") or 30))
        except Exception:
            hb = 30.0

        heartbeat_secs = max(5.0, hb)

        for s in cur:
            sid = str(s.get("Id") or "")
            if not sid:
                continue

            item, ps = _media_from_session(s)
            if not item:
                continue

            dur = item.get("RunTimeTicks") or 0
            pos = ps.get("PositionTicks") if isinstance(ps, dict) else None
            state = "paused" if (ps.get("IsPaused") if isinstance(ps, dict) else False) else "playing"
            p = _ticks_to_pct(pos or 0, dur or 0)

            key = f"{item.get('Id') or item.get('InternalId') or item.get('Name')}-{mhash(dur)}"
            last = self._last.get(sid) or {}
            last_key = last.get("key")
            last_state = last.get("state")
            state_ts = float(last.get("state_ts") or 0.0)

            emit_action: str | None = None
            did_emit = False

            if key != last_key:
                emit_action = "start"
            elif last_state != state:
                if state == "playing":
                    emit_action = "start"
                else:
                    if (now - (state_ts or now)) >= debounce:
                        emit_action = "pause"

            if emit_action == "start" and p < 1:
                p = 1

            if emit_action == "start" and last_key != key and suppress_start_at and p >= suppress_start_at:
                emit_action = None

            if emit_action:
                ev = self._build_event(s, "playing" if emit_action == "start" else "paused", p)
                if ev:
                    last_em = self._last_emit.get(sid)
                    if not (last_em and last_em[0] == ev.action and last_em[1] == ev.progress):
                        self._emit(ev)
                        state_ts = now
                        did_emit = True
            elif state == "playing" and sid in self._last_emit:
                last_em = self._last_emit.get(sid)
                last_prog: int | None = None

                if last_em:
                    try:
                        last_prog = int(last_em[1])
                    except Exception:
                        last_prog = None

                if last_prog is None:
                    try:
                        last_prog = int(last.get("p") or 0)
                    except Exception:
                        last_prog = 0

                if last_prog is not None and abs(int(p) - int(last_prog)) >= 5:
                    ev = self._build_event(s, "playing", p)
                    if ev:
                        self._emit(ev)
                        did_emit = True

            self._last[sid] = {
                "key": key,
                "state": state,
                "p": p,
                "ts": now,
                "state_ts": state_ts or now,
                "meta": (self._last.get(sid) or {}).get("meta"),
            }
            seen.add(sid)

            if state == "playing":
                last_hb = float(self._cw_last_heartbeat.get(sid) or 0.0)
                if did_emit:
                    self._cw_last_heartbeat[sid] = now
                elif now - last_hb >= heartbeat_secs:
                    ev_hb = self._build_event(s, "playing", p)
                    if ev_hb and self._passes_filters(ev_hb):
                        try:
                            _cw_update("emby", ev_hb)
                        except Exception:
                            pass
                    self._cw_last_heartbeat[sid] = now
            else:
                try:
                    self._cw_last_heartbeat.pop(sid, None)
                except Exception:
                    pass

        for sid, memo in list(self._last.items()):
            if sid in seen:
                continue

            last_p = int(memo.get("p") or 0)
            dt = now - float(memo.get("ts", 0))
            if last_p < 1:
                last_p = 1

            meta = memo.get("meta") or {}
            if not meta:
                del self._last[sid]
                try:
                    self._filtered_sessions.discard(sid)
                except Exception:
                    pass
                try:
                    self._cw_last_heartbeat.pop(sid, None)
                except Exception:
                    pass
                continue

            if last_p >= force_at or dt >= 2.0:
                fake = {"Id": sid, "UserName": meta.get("account"), "NowPlayingItem": {}, "PlayState": {}}
                mt_raw = str(meta.get("media_type") or "").strip().lower()
                mt: MediaType = "episode" if mt_raw == "episode" else "movie"

                ids_stop = _normalize_ids(dict(meta.get("ids") or {}))

                ev = ScrobbleEvent(
                    action="stop",
                    media_type=mt,
                    ids=ids_stop,
                    title=meta.get("title"),
                    year=meta.get("year"),
                    season=meta.get("season"),
                    number=meta.get("number"),
                    progress=last_p,
                    account=meta.get("account"),
                    server_uuid=self._server_id,
                    session_key=sid,
                    raw=fake,
                )
                last_em = self._last_emit.get(sid)
                if not (last_em and last_em[0] == "stop"):
                    self._emit(ev)

                del self._last[sid]
                try:
                    self._filtered_sessions.discard(sid)
                except Exception:
                    pass
                try:
                    self._cw_last_heartbeat.pop(sid, None)
                except Exception:
                    pass

    def start(self) -> None:
        self._stop.clear()
        if self._disabled:
            self._log("Missing emby.server or emby.access_token in config.json", "ERROR")
            return
        self._log("Watcher connected", "INFO")
        while not self._stop.is_set():
            self._tick()
            time.sleep(self._poll)

    def stop(self) -> None:
        self._stop.set()
        self._log("Emby watcher stopping", "INFO")

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._bg = threading.Thread(target=self.start, name="EmbyWatch", daemon=True)
        self._bg.start()

    def is_alive(self) -> bool:
        return bool(self._bg and self._bg.is_alive())


def mhash(x: Any) -> int:
    try:
        return abs(hash(int(x)))
    except Exception:
        return abs(hash(str(x)))


def make_default_watch(sinks: Iterable[ScrobbleSink]) -> EmbyWatchService:
    return EmbyWatchService(sinks=sinks)

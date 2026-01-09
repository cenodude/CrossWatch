# providers/scrobble/mdblist/sink.py
# CrossWatch - Scrobble MDBList Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

import requests

from cw_platform.config_base import load_config

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across

try:
    from api.watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except ImportError:
    _rm_across_api = None  # type: ignore

try:
    from providers.scrobble.scrobble import ScrobbleSink, ScrobbleEvent  # type: ignore
except ImportError:
    class ScrobbleSink:
        def send(self, event: Any) -> None: ...

    class ScrobbleEvent:  # pragma: no cover
        ...


MDBLIST_API = "https://api.mdblist.com"
APP_AGENT = "CrossWatch/Scrobble/0.4"
_AR_TTL = 60

_RESOLVE_TTL_S = 30 * 86400
_RESOLVE_NEG_TTL_S = 6 * 3600

_TVDB_SHOW_ID_MAX = 9_999_999


def _cfg() -> dict[str, Any]:
    try:
        return load_config()
    except Exception:
        return {}


def _is_debug() -> bool:
    try:
        return bool((_cfg().get("runtime") or {}).get("debug"))
    except Exception:
        return False


def _log(msg: str, lvl: str = "INFO") -> None:
    level = (str(lvl) or "INFO").upper()
    if level == "DEBUG" and not _is_debug():
        return
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=level, module="MDBLIST")
            return
        except Exception:
            pass
    print(f"[MDBLIST:{level}] {msg}")


def _app_meta(cfg: dict[str, Any]) -> dict[str, str]:
    rt = cfg.get("runtime") or {}
    av = str(rt.get("version") or APP_AGENT)
    ad = (rt.get("build_date") or "").strip()
    return {"app_version": av, **({"app_date": ad} if ad else {})}


def _timeout(cfg: dict[str, Any]) -> float:
    try:
        m = cfg.get("mdblist") or {}
        return float(m.get("timeout", 10))
    except Exception:
        return 10.0


def _max_retries(cfg: dict[str, Any]) -> int:
    try:
        m = cfg.get("mdblist") or {}
        return int(m.get("max_retries", 3))
    except Exception:
        return 3


def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("stop_pause_threshold", 85))
    except Exception:
        return 85


def _complete_at(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("force_stop_at", 95))
    except Exception:
        return 95


def _regress_tolerance_percent(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("regress_tolerance_percent", 5))
    except Exception:
        return 5


def _watch_pause_debounce(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5))
    except Exception:
        return 5


def _watch_suppress_start_at(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99))
    except Exception:
        return 99


def _resolve_enabled(cfg: dict[str, Any]) -> bool:
    try:
        m = cfg.get("mdblist") or {}
        v = m.get("resolve")
        return True if v is None else bool(v)
    except Exception:
        return True


def _resolve_ttl(cfg: dict[str, Any]) -> int:
    try:
        m = cfg.get("mdblist") or {}
        return int(m.get("resolve_ttl_seconds", _RESOLVE_TTL_S))
    except Exception:
        return _RESOLVE_TTL_S


def _resolve_neg_ttl(cfg: dict[str, Any]) -> int:
    try:
        m = cfg.get("mdblist") or {}
        return int(m.get("resolve_negative_ttl_seconds", _RESOLVE_NEG_TTL_S))
    except Exception:
        return _RESOLVE_NEG_TTL_S


def _resolve_enrich_enabled(cfg: dict[str, Any]) -> bool:
    try:
        m = cfg.get("mdblist") or {}
        v = m.get("resolve_enrich")
        return True if v is None else bool(v)
    except Exception:
        return True


def _state_dir() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base


def _as_int(v: Any) -> int | None:
    try:
        return int(v)
    except Exception:
        return None

def _imdb_id_sane(v: Any) -> str | None:
    s = str(v or "").strip()
    if not s:
        return None
    if not s.startswith("tt"):
        return None
    tail = s[2:]
    if not tail.isdigit():
        return None
    if len(tail) < 6:
        return None
    return s



def _tvdb_show_id_sane(v: Any) -> int | None:
    i = _as_int(v)
    if not i or i <= 0:
        return None
    return i if i <= _TVDB_SHOW_ID_MAX else None


def _norm_type(s: str) -> str:
    s = (s or "").strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    if s == "series":
        s = "show"
    return s


def _cfg_delete_enabled(cfg: dict[str, Any], media_type: str) -> bool:
    s = cfg.get("scrobble") or {}
    if not s.get("delete_plex"):
        return False
    types = s.get("delete_plex_types") or []
    mt = _norm_type(media_type)
    if isinstance(types, str):
        return _norm_type(types) == mt
    try:
        allowed = {_norm_type(x) for x in types if str(x).strip()}
    except Exception:
        return False
    return mt in allowed


def _ids(ev: Any) -> dict[str, Any]:
    ids = getattr(ev, "ids", {}) or {}
    out: dict[str, Any] = {}
    imdb = _imdb_id_sane(ids.get("imdb"))
    if imdb:
        out["imdb"] = imdb
    for k in ("tmdb", "trakt", "kitsu"):
        if ids.get(k) is None:
            continue
        try:
            out[k] = int(ids[k])
        except Exception:
            continue
    if ids.get("mdblist"):
        out["mdblist"] = str(ids["mdblist"])
    return out




def _show_ids(ev: Any) -> dict[str, Any]:
    ids = getattr(ev, "ids", {}) or {}
    m: dict[str, Any] = {}

    imdb = _imdb_id_sane(ids.get("imdb_show"))
    if imdb:
        m["imdb"] = imdb

    for k in ("tmdb_show", "trakt_show", "kitsu_show"):
        if ids.get(k) is None:
            continue
        try:
            m[k.replace("_show", "")] = int(ids[k])
        except Exception:
            continue

    if ids.get("tvdb_show") is not None:
        sane = _tvdb_show_id_sane(ids.get("tvdb_show"))
        if sane is not None:
            m["tvdb"] = sane

    if ids.get("mdblist_show"):
        m["mdblist"] = str(ids["mdblist_show"])

    if getattr(ev, "media_type", "") == "episode" and "tvdb" not in m and ids.get("tvdb"):
        sane = _tvdb_show_id_sane(ids.get("tvdb"))
        if sane is not None:
            m["tvdb"] = sane
    return m




def _best_ids_for_scrobble(ids: dict[str, Any], media_type: str) -> dict[str, Any]:
    mt = _norm_type(media_type)
    order = ("trakt", "tmdb", "tvdb", "imdb", "kitsu", "mdblist") if mt == "show" else ("trakt", "tmdb", "imdb", "kitsu", "mdblist")
    for k in order:
        v = ids.get(k)
        if v is None or v == "" or v == 0:
            continue
        if k == "imdb":
            sane = _imdb_id_sane(v)
            if sane:
                return {"imdb": sane}
            continue
        if k in ("trakt", "tmdb", "tvdb", "kitsu"):
            try:
                return {k: int(v)}
            except Exception:
                continue
        return {k: str(v)}
    return {}


def _mdblist_media_info_ids(provider: str, media_type: str, media_id: Any, api_key: str, cfg: dict[str, Any]) -> dict[str, Any] | None:
    try:
        r = requests.get(
            f"{MDBLIST_API}/{provider}/{media_type}/{media_id}",
            params={"apikey": api_key},
            timeout=_timeout(cfg),
            headers={"Accept": "application/json", "User-Agent": APP_AGENT},
        )
    except Exception:
        return None
    if r.status_code != 200:
        return None
    try:
        j = r.json() or {}
    except Exception:
        return None
    ids = (j.get("ids") or {}) if isinstance(j, dict) else {}
    if not isinstance(ids, dict):
        return None
    out: dict[str, Any] = {}
    imdb = _imdb_id_sane(ids.get("imdb"))
    if imdb:
        out["imdb"] = imdb
    if ids.get("tmdb") is not None:
        try:
            out["tmdb"] = int(ids["tmdb"])
        except Exception:
            pass
    if ids.get("trakt") is not None:
        try:
            out["trakt"] = int(ids["trakt"])
        except Exception:
            pass
    if _norm_type(media_type) == "show" and ids.get("tvdb") is not None:
        sane = _tvdb_show_id_sane(ids.get("tvdb"))
        if sane is not None:
            out["tvdb"] = sane
    if ids.get("kitsu") is not None:
        try:
            out["kitsu"] = int(ids["kitsu"])
        except Exception:
            pass
    if ids.get("mdblist"):
        out["mdblist"] = str(ids["mdblist"])
    return out or None


def _enrich_ids_via_info(media_type: str, base_ids: dict[str, Any], api_key: str, cfg: dict[str, Any]) -> dict[str, Any]:
    if not base_ids or not _resolve_enrich_enabled(cfg):
        return dict(base_ids or {})
    mt = _norm_type(media_type)
    info_type = "show" if mt == "show" else "movie"
    for prov in ("trakt", "tmdb", "imdb", "tvdb", "mdblist"):
        if not base_ids.get(prov):
            continue
        got = _mdblist_media_info_ids(prov, info_type, base_ids[prov], api_key, cfg)
        if not got:
            continue
        merged = dict(got)
        for k, v in (base_ids or {}).items():
            if v and k not in merged:
                merged[k] = v
        return merged
    return dict(base_ids or {})


def _try_enrich_event_ids(ev: Any, media_type: str, api_key: str, cfg: dict[str, Any]) -> bool:
    ids = getattr(ev, "ids", None)
    if not isinstance(ids, dict):
        return False

    mt = _norm_type(media_type)
    if mt == "show":
        sh = _show_ids(ev)
        if not sh or sh.get("trakt"):
            return False
        enriched = _enrich_ids_via_info("show", sh, api_key, cfg)
        if not enriched or enriched == sh:
            return False
        if enriched.get("imdb"):
            sane = _imdb_id_sane(enriched.get("imdb"))
            if sane:
                ids["imdb_show"] = sane
        if enriched.get("tmdb") is not None:
            ids["tmdb_show"] = int(enriched["tmdb"])
        if enriched.get("trakt") is not None:
            ids["trakt_show"] = int(enriched["trakt"])
        if enriched.get("tvdb") is not None:
            sane = _tvdb_show_id_sane(enriched.get("tvdb"))
            if sane is not None:
                ids["tvdb_show"] = sane
        if enriched.get("mdblist"):
            ids["mdblist_show"] = str(enriched["mdblist"])
        return True

    mo = _ids(ev)
    if not mo or mo.get("trakt"):
        return False
    enriched = _enrich_ids_via_info("movie", mo, api_key, cfg)
    if not enriched or enriched == mo:
        return False
    if enriched.get("imdb"):
        sane = _imdb_id_sane(enriched.get("imdb"))
        if sane:
            ids["imdb"] = sane
    if enriched.get("tmdb") is not None:
        ids["tmdb"] = int(enriched["tmdb"])
    if enriched.get("trakt") is not None:
        ids["trakt"] = int(enriched["trakt"])
    if enriched.get("kitsu") is not None:
        ids["kitsu"] = int(enriched["kitsu"])
    if enriched.get("mdblist"):
        ids["mdblist"] = str(enriched["mdblist"])
    return True


def _ar_key(ids: dict[str, Any], media_type: str) -> str:
    parts = [media_type]
    for k in ("imdb", "tmdb", "tvdb", "trakt", "kitsu", "mdblist"):
        if ids.get(k):
            parts.append(f"{k}:{ids[k]}")
    return "|".join(parts)


def _ar_state_file() -> str:
    try:
        return str(_state_dir() / "auto_remove_seen.json")
    except Exception:
        return ".cw_state/auto_remove_seen.json"


def _ar_seen(key: str) -> bool:
    p = _ar_state_file()
    try:
        data = json.loads(open(p, "r", encoding="utf-8").read()) or {}
    except Exception:
        data = {}
    now = time.time()
    try:
        data = {k: v for k, v in data.items() if (now - float(v)) < _AR_TTL}
    except Exception:
        data = {}
    if key in data:
        try:
            open(p, "w", encoding="utf-8").write(json.dumps(data))
        except Exception:
            pass
        return True
    data[key] = now
    try:
        open(p, "w", encoding="utf-8").write(json.dumps(data))
    except Exception:
        pass
    return False


def _auto_remove_across(ev: Any, cfg: dict[str, Any]) -> None:
    mt = _norm_type(str(getattr(ev, "media_type", "") or ""))
    if not _cfg_delete_enabled(cfg, mt):
        return
    ids = _show_ids(ev) if mt == "episode" else _ids(ev)
    if not ids:
        ids = _ids(ev)
    if not ids:
        return
    key = _ar_key(ids, mt)
    if _ar_seen(key):
        return
    try:
        _rm_across(ids, mt)
        return
    except Exception:
        pass
    try:
        if _rm_across_api:
            _rm_across_api(ids, mt)  # type: ignore[misc]
            return
    except Exception:
        pass


def _media_name(ev: Any) -> str:
    if getattr(ev, "media_type", "") == "episode":
        s = int(getattr(ev, "season", 0) or 0)
        n = int(getattr(ev, "number", 0) or 0)
        t = getattr(ev, "title", None) or "?"
        return f"{t} S{s:02d}E{n:02d}"
    t = getattr(ev, "title", None) or "?"
    y = getattr(ev, "year", None)
    return f"{t} ({y})" if y else t



def _bodies(ev: Any, progress: float) -> list[dict[str, Any]]:
    mt = getattr(ev, "media_type", "") or ""
    if mt == "episode":
        raw = _show_ids(ev)
        sh_ids = _best_ids_for_scrobble(raw, "show")
        season = int(getattr(ev, "season", 0) or 0)
        number = int(getattr(ev, "number", 0) or 0)
        show: dict[str, Any] = {"ids": sh_ids} if sh_ids else {}
        if not sh_ids:
            title = getattr(ev, "title", None)
            year = getattr(ev, "year", None)
            if title:
                show["title"] = title
            if year:
                show["year"] = int(year)
        show["season"] = {"number": season, "episode": {"number": number}}
        return [{"show": show, "progress": progress}]

    raw_ids = _ids(ev)
    ids = _best_ids_for_scrobble(raw_ids, "movie")
    movie: dict[str, Any] = {"ids": ids} if ids else {}
    if not ids:
        title = getattr(ev, "title", None)
        year = getattr(ev, "year", None)
        if title:
            movie["title"] = title
        if year:
            movie["year"] = int(year)
    return [{"movie": movie, "progress": progress}]


def _resolve_state_file() -> Path:
    return _state_dir() / "mdblist_resolve_cache.json"


def _resolve_load() -> dict[str, Any]:
    p = _resolve_state_file()
    try:
        return json.loads(p.read_text("utf-8")) or {}
    except Exception:
        return {}


def _resolve_save(data: dict[str, Any]) -> None:
    p = _resolve_state_file()
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(p)
    except Exception:
        try:
            p.write_text(json.dumps(data, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        except Exception:
            pass


def _resolve_key(media_type: str, title: str, year: Any) -> str:
    y = int(year) if str(year).isdigit() else 0
    return f"{_norm_type(media_type)}|{(title or '').strip().lower()}|{y}"


def _resolve_prune(cache: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
    now = time.time()
    ttl = max(60, _resolve_ttl(cfg))
    neg_ttl = max(60, _resolve_neg_ttl(cfg))
    out: dict[str, Any] = {}
    for k, v in (cache or {}).items():
        if not isinstance(v, dict):
            continue
        ts = float(v.get("_ts", 0) or 0)
        ids = v.get("ids") or {}
        live = (now - ts) < (ttl if ids else neg_ttl)
        if live:
            out[k] = v
    return out


def _mdblist_search(media_type: str, title: str, year: Any, api_key: str, cfg: dict[str, Any]) -> dict[str, Any] | None:
    if not (title or "").strip():
        return None

    def fetch(mt: str) -> list[dict[str, Any]]:
        params: dict[str, Any] = {"apikey": api_key, "query": title, "limit": 10, "sort_by_score": True}
        if str(year).isdigit():
            params["year"] = int(year)
        try:
            r = requests.get(
                f"{MDBLIST_API}/search/{mt}",
                params=params,
                timeout=_timeout(cfg),
                headers={"Accept": "application/json", "User-Agent": APP_AGENT},
            )
        except Exception:
            return []
        if r.status_code != 200:
            return []
        try:
            j = r.json() or {}
        except Exception:
            return []
        items = j.get("search") or []
        return items if isinstance(items, list) else []

    want = _norm_type(media_type)
    items = fetch(want)
    if not items:
        any_items = fetch("any")
        items = [x for x in any_items if _norm_type(str(x.get("type") or "")) == want]

    if not items:
        return None

    best = items[0]
    ids = (best.get("ids") or {})
    out: dict[str, Any] = {}
    imdb = _imdb_id_sane(ids.get("imdbid"))
    if imdb:
        out["imdb"] = imdb
    if ids.get("tmdbid") is not None:
        out["tmdb"] = int(ids["tmdbid"])
    if ids.get("traktid") is not None:
        out["trakt"] = int(ids["traktid"])
    if want == "show" and ids.get("tvdbid") is not None:
        sane = _tvdb_show_id_sane(ids.get("tvdbid"))
        if sane is not None:
            out["tvdb"] = sane
    return out or None


def _try_resolve_ids_for_mdblist(ev: Any, media_type: str, api_key: str, cfg: dict[str, Any]) -> tuple[bool, bool]:
    ids = getattr(ev, "ids", None)
    if not isinstance(ids, dict):
        return False, False

    title = (getattr(ev, "title", None) or "").strip()
    year = getattr(ev, "year", None)
    if not title:
        return False, False

    cache = _resolve_load()
    cache = _resolve_prune(cache, cfg)
    ck = _resolve_key(media_type, title, year)
    hit = cache.get(ck) if isinstance(cache, dict) else None

    if isinstance(hit, dict):
        resolved = dict(hit.get("ids") or {})
        if not resolved:
            return False, True
    else:
        resolved = _mdblist_search(media_type, title, year, api_key, cfg) or {}
        cache[ck] = {"_ts": time.time(), "ids": resolved}
        _resolve_save(cache)
        if not resolved:
            return False, True


    if resolved:
        enriched = _enrich_ids_via_info(media_type, resolved, api_key, cfg)
        if enriched and enriched != resolved:
            resolved = dict(enriched)
            cache[ck] = {"_ts": time.time(), "ids": resolved}
            _resolve_save(cache)

    if _norm_type(media_type) == "show":
        if resolved.get("imdb"):
            sane = _imdb_id_sane(resolved.get("imdb"))
            if sane:
                ids["imdb_show"] = sane
        if resolved.get("tmdb") is not None:
            ids["tmdb_show"] = int(resolved["tmdb"])
        if resolved.get("trakt") is not None:
            ids["trakt_show"] = int(resolved["trakt"])
        if resolved.get("tvdb") is not None:
            sane = _tvdb_show_id_sane(resolved.get("tvdb"))
            if sane is not None:
                ids["tvdb_show"] = sane
        if resolved.get("mdblist"):
            ids["mdblist_show"] = str(resolved["mdblist"])
    else:
        if resolved.get("imdb"):
            sane = _imdb_id_sane(resolved.get("imdb"))
            if sane:
                ids["imdb"] = sane
        if resolved.get("tmdb") is not None:
            ids["tmdb"] = int(resolved["tmdb"])
        if resolved.get("trakt") is not None:
            ids["trakt"] = int(resolved["trakt"])
        if resolved.get("mdblist"):
            ids["mdblist"] = str(resolved["mdblist"])
    return True, False


def _payload_ids_keys(body: dict[str, Any]) -> list[str]:
    if "show" in body:
        ids = ((body.get("show") or {}).get("ids") or {})
    elif "movie" in body:
        ids = ((body.get("movie") or {}).get("ids") or {})
    else:
        ids = {}
    return sorted([str(k) for k, v in (ids or {}).items() if v])


def _ids_desc_map(ids: dict[str, Any]) -> str:
    for k in ("imdb", "trakt", "tmdb", "tvdb", "kitsu", "mdblist"):
        v = ids.get(k)
        if v is not None:
            return f"{k}:{v}"
    return "title/year"


def _body_ids_desc(b: dict[str, Any]) -> str:
    ids = (
        (b.get("movie") or {}).get("ids")
        or (b.get("show") or {}).get("ids")
        or (b.get("episode") or {}).get("ids")
        or {}
    )
    return _ids_desc_map(ids if isinstance(ids, dict) else {})


class MDBListSink(ScrobbleSink):
    def __init__(self) -> None:
        self._last_sent: dict[str, float] = {}
        self._p_glob: dict[str, int] = {}
        self._last_intent_path: dict[str, str] = {}
        self._last_intent_prog: dict[str, int] = {}
        self._warn_no_key = False
        self._no_match_logged: dict[str, float] = {}
        self._enriched_keys: set[str] = set()

    def _mkey(self, ev: Any) -> str:
        ids = getattr(ev, "ids", {}) or {}
        parts: list[str] = []
        for k in ("imdb", "tmdb", "trakt", "kitsu", "mdblist"):
            if ids.get(k):
                parts.append(f"{k}:{ids[k]}")
        if getattr(ev, "media_type", "") == "episode":
            for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show", "kitsu_show", "mdblist_show"):
                if ids.get(k):
                    parts.append(f"{k}:{ids[k]}")
            parts.append(f"S{int(getattr(ev, 'season', 0) or 0):02d}E{int(getattr(ev, 'number', 0) or 0):02d}")
        if not parts:
            t = getattr(ev, "title", None) or ""
            y = getattr(ev, "year", None) or 0
            base = f"{t}|{y}"
            if getattr(ev, "media_type", "") == "episode":
                base += f"|S{int(getattr(ev, 'season', 0) or 0):02d}E{int(getattr(ev, 'number', 0) or 0):02d}"
            parts.append(base)
        return "|".join(parts)

    def _ckey(self, ev: Any) -> str:
        ids = getattr(ev, "ids", {}) or {}
        if ids.get("plex"):
            return f"plex:{ids.get('plex')}"
        return self._mkey(ev)

    def _debounced(self, session_key: str | None, action: str, debounce_s: int) -> bool:
        now = time.time()
        key = f"{action}:{session_key or '_'}"
        last = self._last_sent.get(key, 0.0)
        if (now - last) < max(0.5, debounce_s):
            return True
        self._last_sent[key] = now
        return False

    def _should_log_intent(self, key: str, path: str, prog: int) -> bool:
        lp = self._last_intent_path.get(key)
        pp = self._last_intent_prog.get(key, -1)
        changed = (lp != path) or (abs(int(prog) - int(pp)) >= 5)
        if changed:
            self._last_intent_path[key] = path
            self._last_intent_prog[key] = int(prog)
        return changed

    def _post(self, path: str, body: dict[str, Any], api_key: str, cfg: dict[str, Any]) -> requests.Response:
        headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": APP_AGENT}
        return requests.post(
            f"{MDBLIST_API}{path}",
            headers=headers,
            params={"apikey": api_key},
            json=body,
            timeout=_timeout(cfg),
        )

    def _send_http(self, path: str, body: dict[str, Any], api_key: str, cfg: dict[str, Any]) -> dict[str, Any]:
        max_retries = max(0, _max_retries(cfg))
        backoff = 0.6
        last_err: str | None = None
        for attempt in range(max_retries + 1):
            try:
                r = self._post(path, body, api_key, cfg)
            except Exception as e:
                last_err = f"request_error:{e}"
                if attempt >= max_retries:
                    break
                time.sleep(min(6.0, backoff))
                backoff *= 1.8
                continue

            if r.status_code in (200, 201, 204):
                try:
                    resp: Any = r.json()
                except Exception:
                    resp = (getattr(r, "text", "") or "")[:400]
                return {"ok": True, "status": r.status_code, "resp": resp}

            if r.status_code == 401:
                return {"ok": False, "status": 401, "error": "invalid_api_key"}

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                try:
                    wait_s = max(0.5, float(ra)) if ra else 2.0
                except Exception:
                    wait_s = 2.0
                if attempt >= max_retries:
                    return {"ok": False, "status": 429, "error": "rate_limited"}
                time.sleep(min(15.0, wait_s))
                continue

            if 500 <= r.status_code <= 599 and attempt < max_retries:
                time.sleep(min(6.0, backoff))
                backoff *= 1.8
                continue

            try:
                j = r.json()
            except Exception:
                j = (getattr(r, "text", "") or "")[:250]
            return {"ok": False, "status": r.status_code, "error": j, "resp": j}

        return {"ok": False, "status": 0, "error": last_err or "unknown"}

    def _log_no_match(self, key: str, msg: str) -> None:
        now = time.time()
        last = self._no_match_logged.get(key, 0.0)
        if (now - last) < 60.0:
            return
        self._no_match_logged[key] = now
        _log(msg, "WARN")

    def send(self, ev: ScrobbleEvent) -> None:
        cfg = _cfg()
        m = cfg.get("mdblist") or {}
        api_key = str(m.get("api_key") or "").strip()

        if not api_key:
            if not self._warn_no_key:
                _log("Missing mdblist.api_key in config.json — skipping scrobble", "ERROR")
                self._warn_no_key = True
            return

        action = (getattr(ev, "action", "") or "").lower()
        p_raw = float(getattr(ev, "progress", 0) or 0)
        p_raw = max(0.0, min(100.0, p_raw))
        if action == "start" and p_raw < 2.0:
            p_raw = 2.0

        thr = _stop_pause_threshold(cfg)
        comp = _complete_at(cfg)
        comp_thr = max(thr, comp)
        suppress_at = _watch_suppress_start_at(cfg)
        name = _media_name(ev)
        key = self._ckey(ev)

        p_send = round(float(p_raw), 2)

        if action == "start" and p_send >= suppress_at:
            if p_send > (self._p_glob.get(key, -1) if key else -1):
                self._p_glob[key] = int(p_send)
            return

        if comp and p_send >= comp and action not in ("stop", "start"):
            action = "stop"

        sess = getattr(ev, "session_key", None) or getattr(ev, "session", None)
        if action == "pause" and self._debounced(sess, action, _watch_pause_debounce(cfg)):
            return

        tol = _regress_tolerance_percent(cfg)
        p_glob = self._p_glob.get(key, -1)
        if p_glob >= 0 and p_raw < max(0, p_glob - tol) and action != "start":
            return
        self._p_glob[key] = max(p_glob, int(p_raw))

        if action == "start":
            path = "/scrobble/start"
        elif action == "pause":
            path = "/scrobble/pause"
        else:
            path = "/scrobble/stop"

        mt = getattr(ev, "media_type", "") or ""
        attempted_resolve = False
        attempted_enrich = False

        if _resolve_enabled(cfg):
            if mt == "episode" and not _show_ids(ev):
                ok, cached_negative = _try_resolve_ids_for_mdblist(ev, "show", api_key, cfg)
                attempted_resolve = attempted_resolve or ok or cached_negative
                if cached_negative and not ok and not _show_ids(ev):
                    self._log_no_match(key, f"mdblist resolve: no match for show '{name}' — skipping scrobble")
                    return
            if mt != "episode" and not _ids(ev):
                ok, cached_negative = _try_resolve_ids_for_mdblist(ev, "movie", api_key, cfg)
                attempted_resolve = attempted_resolve or ok or cached_negative
                if cached_negative and not ok and not _ids(ev):
                    self._log_no_match(key, f"mdblist resolve: no match for movie '{name}' — skipping scrobble")
                    return


        if _resolve_enabled(cfg) and _resolve_enrich_enabled(cfg):
            if mt == "episode":
                sh = _show_ids(ev)
                if sh and not sh.get("trakt"):
                    ek_parts: list[str] = []
                    for k in ("trakt", "tmdb", "imdb", "tvdb", "mdblist"):
                        if sh.get(k):
                            ek_parts.append(f"{k}:{sh[k]}")
                    ek = "show|" + ("|".join(ek_parts) if ek_parts else (getattr(ev, "title", "") or "").strip().lower())
                    if ek and ek not in self._enriched_keys:
                        self._enriched_keys.add(ek)
                        attempted_enrich = _try_enrich_event_ids(ev, "show", api_key, cfg) or attempted_enrich
            else:
                mo = _ids(ev)
                if mo and not mo.get("trakt"):
                    ek_parts: list[str] = []
                    for k in ("trakt", "tmdb", "imdb", "mdblist", "kitsu"):
                        if mo.get(k):
                            ek_parts.append(f"{k}:{mo[k]}")
                    ek = "movie|" + ("|".join(ek_parts) if ek_parts else (getattr(ev, "title", "") or "").strip().lower())
                    if ek and ek not in self._enriched_keys:
                        self._enriched_keys.add(ek)
                        attempted_enrich = _try_enrich_event_ids(ev, "movie", api_key, cfg) or attempted_enrich

        bodies = [{**b, **_app_meta(cfg)} for b in _bodies(ev, p_send)]

        for body in bodies:
            intent_prog = int(float(body.get("progress") or p_send))
            if self._should_log_intent(key, path, intent_prog):
                _log(f"mdblist intent {path} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")

            res = self._send_http(path, body, api_key, cfg)
            if not res.get("ok") and res.get("status") == 404 and _resolve_enabled(cfg):
                if not attempted_resolve:
                    if mt == "episode":
                        ok, cached_negative = _try_resolve_ids_for_mdblist(ev, "show", api_key, cfg)
                    else:
                        ok, cached_negative = _try_resolve_ids_for_mdblist(ev, "movie", api_key, cfg)
                    attempted_resolve = True
                    if ok:
                        retry_bodies = [{**b, **_app_meta(cfg)} for b in _bodies(ev, p_send)]
                        body = retry_bodies[0]
                        res = self._send_http(path, body, api_key, cfg)
                    elif cached_negative:
                        self._log_no_match(key, f"mdblist resolve: no match for '{name}' — skipping scrobble")
                        return
                elif not attempted_enrich and _resolve_enrich_enabled(cfg):
                    attempted_enrich = True
                    if mt == "episode":
                        enriched_ok = _try_enrich_event_ids(ev, "show", api_key, cfg)
                    else:
                        enriched_ok = _try_enrich_event_ids(ev, "movie", api_key, cfg)
                    if enriched_ok:
                        retry_bodies = [{**b, **_app_meta(cfg)} for b in _bodies(ev, p_send)]
                        body = retry_bodies[0]
                        res = self._send_http(path, body, api_key, cfg)


            if not res.get("ok"):
                _log(f"{path} failed for {name}: {res}", "WARN")
                continue

            try:
                act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
            except Exception:
                act = path.rsplit("/", 1)[-1]
            _log(f"mdblist {path} -> {res.get('status')} action={act}", "DEBUG")
            try:
                acc = getattr(ev, "account", None)
                prog_val = float(body.get("progress") or p_send)
                _log(f"user='{acc}' {act} {prog_val:.1f}% • {name}", "INFO")
            except Exception:
                pass

        if action == "stop" and int(p_send) >= comp_thr:
            _auto_remove_across(ev, cfg)

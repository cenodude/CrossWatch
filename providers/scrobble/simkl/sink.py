# providers/scrobble/simkl/sink.py
# Simkl.com scrobble sink for CrossWatch
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time, json, requests
from pathlib import Path
from typing import Any

SIMKL_API  = "https://api.simkl.com"
APP_AGENT  = "CrossWatch/Scrobble/0.3"

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

def _log(msg: str, lvl: str = "INFO") -> None:
    if BASE_LOG:
        try:
            BASE_LOG.child("SIMKL").log(lvl, msg)
            return
        except Exception:
            pass
    print(f"[SIMKL:{lvl}] {msg}")

def _cfg() -> dict[str, Any]:
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        pass
    for path in ("config.json", "/config/config.json"):
        p = Path(path)
        try:
            if p.exists():
                return json.loads(p.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}

def _is_debug() -> bool:
    try: return bool((( _cfg().get("runtime") or {}).get("debug")))
    except Exception: return False

def _app_meta(cfg: dict[str, Any]) -> dict[str, str]:
    rt = (cfg.get("runtime") or {})
    av = str(rt.get("version") or APP_AGENT)
    ad = (rt.get("build_date") or "").strip()
    return {"app_version": av, **({"app_date": ad} if ad else {})}

def _hdr(cfg: dict[str, Any]) -> dict[str, str]:
    s = (cfg.get("simkl") or {})
    api_key = str(s.get("api_key") or s.get("client_id") or "")
    token   = str(s.get("access_token") or "")
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": APP_AGENT,
        "simkl-api-key": api_key,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h

def _post(path: str, body: dict[str, Any], cfg: dict[str, Any]):
    return requests.post(f"{SIMKL_API}{path}", headers=_hdr(cfg), json=body, timeout=10)

def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try:
        s = (cfg.get("scrobble") or {})
        return int(((s.get("trakt") or {})).get("stop_pause_threshold", 85))
    except Exception:
        return 85

def _complete_at(cfg: dict[str, Any]) -> int:
    try:
        s = (cfg.get("scrobble") or {})
        return int(((s.get("trakt") or {})).get("force_stop_at", 95))
    except Exception:
        return 95

def _regress_tolerance_percent(cfg: dict[str, Any]) -> int:
    try:
        s = (cfg.get("scrobble") or {})
        return int(((s.get("trakt") or {})).get("regress_tolerance_percent", 5))
    except Exception:
        return 5

def _watch_pause_debounce(cfg: dict[str, Any]) -> int:
    try: return int((((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5)))
    except Exception: return 5

from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from _watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except Exception:
    _rm_across_api = None

# --- cross-sink auto-remove dedupe (short TTL) ---
_AR_TTL = 60
def _ar_state_file() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try: base.mkdir(parents=True, exist_ok=True)
    except Exception: pass
    return base / "auto_remove_seen.json"

def _ar_seen(key: str) -> bool:
    p = _ar_state_file()
    try:
        data = json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        data = {}
    now = time.time()
    try:
        data = {k: v for k, v in data.items() if (now - float(v)) < _AR_TTL}
    except Exception:
        data = {}
    if key in data:
        try: p.write_text(json.dumps(data), encoding="utf-8")
        except Exception: pass
        return True
    data[key] = now
    try: p.write_text(json.dumps(data), encoding="utf-8")
    except Exception: pass
    return False

def _ar_key(ids: dict, media_type: str) -> str:
    for k in ("imdb","tmdb","tvdb","trakt","simkl"):
        v = ids.get(k)
        if v:
            return f"{media_type}:{k}:{v}"
    try:
        return f"{media_type}:{json.dumps(ids, sort_keys=True)}"
    except Exception:
        return f"{media_type}:title/year"
# --- end dedupe helpers ---

def _norm_type(t: str) -> str:
    s = (t or "").strip().lower()
    if s.endswith("s"): s = s[:-1]
    if s == "series": s = "show"
    return s

def _cfg_delete_enabled(cfg: dict[str, Any], media_type: str) -> bool:
    s = (cfg.get("scrobble") or {})
    if not s.get("delete_plex"): return False
    types = s.get("delete_plex_types") or []
    mt = _norm_type(media_type)
    if isinstance(types, str): return _norm_type(types) == mt
    try:
        allowed = {_norm_type(x) for x in types if str(x).strip()}
    except Exception:
        return False
    return mt in allowed

def _auto_remove_across(ev, cfg: dict[str, Any]) -> None:
    try:
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
            _rm_across_api(ids, mt)  # type: ignore
            return
        except Exception:
            pass
    except Exception:
        pass

def _ids(ev) -> dict[str, Any]:
    ids = ev.ids or {}
    return {k: ids[k] for k in ("imdb","tmdb","tvdb","simkl") if ids.get(k)}

def _show_ids(ev) -> dict[str, Any]:
    ids = ev.ids or {}; m = {}
    for k in ("imdb_show","tmdb_show","tvdb_show","simkl_show"):
        if ids.get(k): m[k.replace("_show","")] = ids[k]
    return m

def _media_name(ev) -> str:
    if getattr(ev, "media_type", "") == "episode":
        s = ev.season if ev.season is not None else 0
        n = ev.number if ev.number is not None else 0
        t = ev.title or "?"
        try: return f"{t} S{int(s):02d}E{int(n):02d}"
        except Exception: return f"{t}"
    return ev.title or "?"

def _ids_desc_map(ids: dict[str, Any]) -> str:
    for k in ("simkl","imdb","tmdb","tvdb"):
        v = ids.get(k)
        if v is not None: return f"{k}:{v}"
    return "title/year"

def _body_ids_desc(b: dict[str, Any]) -> str:
    ids = ((b.get("movie") or {}).get("ids")) or ((b.get("show") or {}).get("ids")) or ((b.get("episode") or {}).get("ids")) or {}
    return _ids_desc_map(ids if isinstance(ids, dict) else {})

try:
    from providers.scrobble.scrobble import ScrobbleSink  # type: ignore
except Exception:
    class ScrobbleSink:
        def send(self, event): ...

class SimklSink(ScrobbleSink):
    def __init__(self, logger=None):
        self._logr = logger if logger else (BASE_LOG.child("SIMKL") if (BASE_LOG and hasattr(BASE_LOG,"child")) else None)
        try:
            if self._logr and hasattr(self._logr,"set_level"):
                self._logr.set_level("DEBUG" if _is_debug() else "INFO")
        except Exception:
            pass
        self._last_sent: dict[str, float] = {}
        self._p_sess:   dict[tuple[str, str], int] = {}
        self._p_glob:   dict[str, int] = {}
        self._best:     dict[str, dict[str, Any]] = {}
        self._ids_logged: set[str] = set()
        self._last_intent_path: dict[str, str] = {}
        self._last_intent_prog: dict[str, int] = {}
        self._warn_no_token = False
        self._warn_no_key   = False

    def _mkey(self, ev) -> str:
        ids = ev.ids or {}; parts=[]
        for k in ("imdb","tmdb","tvdb","simkl"):
            if ids.get(k): parts.append(f"{k}:{ids[k]}")
        if getattr(ev,"media_type","") == "episode":
            for k in ("imdb_show","tmdb_show","tvdb_show","simkl_show"):
                if ids.get(k): parts.append(f"{k}:{ids[k]}")
            parts.append(f"S{(ev.season or 0):02d}E{(ev.number or 0):02d}")
        if not parts:
            t,y = ev.title or "", ev.year or 0
            parts.append(f"{t}|{y}" + (f"|S{(ev.season or 0):02d}E{(ev.number or 0):02d}" if getattr(ev,"media_type","")=="episode" else ""))
        return "|".join(parts)

    def _ckey(self, ev) -> str:
        ids = ev.ids or {}
        if ids.get("plex"): return f"plex:{ids.get('plex')}"
        return self._mkey(ev)

    def _debounced(self, session_key: str | None, action: str, debounce_s: int) -> bool:
        now = time.time()
        key = f"{action}:{session_key or '_'}"
        last = self._last_sent.get(key, 0.0)
        if (now - last) < max(0.5, debounce_s):
            return True
        self._last_sent[key] = now
        return False

    def _bodies(self, ev, p) -> list[dict[str, Any]]:
        ids = _ids(ev); show = _show_ids(ev)
        is_anime_type = str(getattr(ev, "media_type", "") or "").lower() == "anime"
        has_anime_ids = any((ev.ids or {}).get(k) for k in ("mal","anidb","anilist","kitsu","mal_show","anidb_show","anilist_show","kitsu_show"))
        parent = "anime" if (is_anime_type or has_anime_ids) else "show"
        if getattr(ev, "media_type", "") == "movie":
            if ids:
                return [{"progress": p, "movie": {"ids": ids}}]
            return [{"progress": p, "movie": {"title": ev.title, **({"year": ev.year} if ev.year is not None else {})}}]

        bodies: list[dict[str, Any]] = []
        has_sn = (ev.season is not None and ev.number is not None)
        if has_sn and show:
            bodies.append({"progress": p, parent: {"ids": show}, "episode": {"season": ev.season, "number": ev.number}})
        if has_sn and not show:
            s = {"title": ev.title, **({"year": ev.year} if ev.year is not None else {})}
            bodies.append({"progress": p, parent: s, "episode": {"season": ev.season, "number": ev.number}})
        if ids:
            bodies.append({"progress": p, parent: {}, "episode": {"ids": ids}})
        return bodies or [{"progress": p, parent: {"ids": show or {}}, "episode": {"season": ev.season or 0, "number": ev.number or 0}}]

    def _should_log_intent(self, key: str, path: str, prog: int) -> bool:
        lp = self._last_intent_path.get(key)
        pp = self._last_intent_prog.get(key, -1)
        ch = (lp != path) or (abs(int(prog) - int(pp)) >= 5)
        if ch:
            self._last_intent_path[key] = path; self._last_intent_prog[key] = int(prog)
        return ch

    def send(self, ev) -> None:
        cfg = _cfg()

        s = (cfg.get("simkl") or {})
        api_key = s.get("api_key") or s.get("client_id")
        token   = s.get("access_token")
        if not api_key:
            if not self._warn_no_key:
                _log("Missing simkl.api_key/client_id in config.json — skipping scrobble", "ERROR")
                self._warn_no_key = True
            return
        if not token:
            if not self._warn_no_token:
                _log("Missing SIMKL access_token — connect SIMKL to enable scrobble", "ERROR")
                self._warn_no_token = True
            return

        action = (getattr(ev, "action", "") or "").lower()
        p_raw = float(getattr(ev, "progress", 0) or 0)
        comp_thr = max(_stop_pause_threshold(cfg), _complete_at(cfg))
        name = _media_name(ev)
        key = self._ckey(ev)

        sess = getattr(ev, "session_key", None) or getattr(ev, "session", None)
        if action == "pause" and self._debounced(sess, action, _watch_pause_debounce(cfg)):
            return

        tol = _regress_tolerance_percent(cfg)
        p_glob = self._p_glob.get(key, -1)
        if p_glob >= 0 and p_raw < max(0, p_glob - tol) and action != "start":
            return
        self._p_glob[key] = max(p_glob, int(p_raw))

        path = "/scrobble/start" if action == "start" else "/scrobble/pause" if action == "pause" else "/scrobble/stop"

        p_send = round(float(p_raw), 2)
        bodies = [{**b, **_app_meta(cfg)} for b in self._bodies(ev, p_send)]

        last_err = None
        for i, body in enumerate(bodies):
            if self._should_log_intent(key, path, int(float(body.get("progress") or p_send))):
                _log(f"simkl intent {path} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")
            res = self._send_http(path, body, cfg)
            if res.get("ok"):
                try: act = (res.get("resp") or {}).get("action") or path.rsplit("/",1)[-1]
                except Exception: act = path.rsplit("/",1)[-1]
                _log(f"simkl {path} -> {res['status']} action={act}", "DEBUG")
                try:
                    _log(f"user='{getattr(ev,'account',None)}' {act} {float(body.get('progress') or p_send):.1f}% • {name}", "INFO")
                except Exception:
                    pass
                if action == "stop" and p_send >= comp_thr:
                    _auto_remove_across(ev, cfg)
                return
            last_err = res
            if res.get("status") == 404:
                _log("404 with current representation → trying alternate", "WARN"); continue
            break

        if last_err and last_err.get("status") == 409 and action == "stop":
            _log("Treating 409 (duplicate stop) as watched; proceeding to auto-remove", "WARN")
            if p_send >= comp_thr:
                _auto_remove_across(ev, cfg)
            return

        if last_err:
            _log(f"{path} {last_err.get('status')} err={last_err.get('resp')}", "ERROR")

    def _send_http(self, path: str, body: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
        backoff = 1.0
        for _ in range(6):
            try:
                r = _post(path, body, cfg)
            except Exception as e:
                time.sleep(backoff); backoff = min(8.0, backoff*2); continue
            s = r.status_code
            if s in (423, 429):
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else backoff
                except Exception:
                    wait = backoff
                time.sleep(max(1.0, min(20.0, wait)))
                backoff = min(8.0, backoff*2)
                continue
            if 500 <= s < 600:
                time.sleep(backoff); backoff = min(8.0, backoff*2); continue
            if s >= 400:
                short = (r.text or "")[:400]
                try:
                    j = r.json()
                    return {"ok": False, "status": s, "resp": j}
                except Exception:
                    return {"ok": False, "status": s, "resp": short}
            try:
                return {"ok": True, "status": s, "resp": r.json()}
            except Exception:
                return {"ok": True, "status": s, "resp": (r.text or "")[:400]}
        return {"ok": False, "status": 429, "resp": "rate_limited"}

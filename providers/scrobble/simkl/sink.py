# providers/scrobble/simkl/sink.py
# CrossWatch - Scrobble Simkl Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json, time
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
    from providers.scrobble.scrobble import ScrobbleSink  # type: ignore
except ImportError:
    class ScrobbleSink:
        def send(self, event: Any) -> None: ...


SIMKL_API = "https://api.simkl.com"
APP_AGENT = "CrossWatch/Scrobble/0.3"
_AR_TTL = 60


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
            BASE_LOG(str(msg), level=level, module="SIMKL")
            return
        except Exception:
            pass
    print(f"[SIMKL:{level}] {msg}")


def _app_meta(cfg: dict[str, Any]) -> dict[str, str]:
    rt = cfg.get("runtime") or {}
    av = str(rt.get("version") or APP_AGENT)
    ad = (rt.get("build_date") or "").strip()
    return {"app_version": av, **({"app_date": ad} if ad else {})}


def _hdr(cfg: dict[str, Any]) -> dict[str, str]:
    s = cfg.get("simkl") or {}
    api_key = str(s.get("api_key") or s.get("client_id") or "")
    token = str(s.get("access_token") or "")
    h: dict[str, str] = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "User-Agent": APP_AGENT,
        "simkl-api-key": api_key,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _post(path: str, body: dict[str, Any], cfg: dict[str, Any]) -> requests.Response:
    return requests.post(f"{SIMKL_API}{path}", headers=_hdr(cfg), json=body, timeout=10)


def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("stop_pause_threshold", 85))
    except Exception:
        return 85


def _force_stop_at(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("force_stop_at", 95))
    except Exception:
        return 95


def _complete_at(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("complete_at", 0))
    except Exception:
        return 0


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


def _watch_suppress_start_at(cfg: dict[str, Any]) -> float:
    try:
        return float(((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99))
    except Exception:
        return 99.0


def _clamp(p: Any) -> int:
    try:
        v = int(float(p))
    except Exception:
        v = 0
    return max(0, min(100, v))


def _ar_state_file() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
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
        try:
            p.write_text(json.dumps(data), encoding="utf-8")
        except Exception:
            pass
        return True
    data[key] = now
    try:
        p.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass
    return False


def _ar_key(ids: dict[str, Any], media_type: str) -> str:
    for k in ("imdb", "tmdb", "tvdb", "trakt", "simkl"):
        v = ids.get(k)
        if v:
            return f"{media_type}:{k}:{v}"
    try:
        return f"{media_type}:{json.dumps(ids, sort_keys=True)}"
    except Exception:
        return f"{media_type}:title/year"


def _norm_type(t: str) -> str:
    s = (t or "").strip().lower()
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
    return {k: ids[k] for k in ("imdb", "tmdb", "tvdb", "simkl") if ids.get(k)}


def _show_ids(ev: Any) -> dict[str, Any]:
    ids = getattr(ev, "ids", {}) or {}
    m: dict[str, Any] = {}
    for k in ("imdb_show", "tmdb_show", "tvdb_show", "simkl_show"):
        if ids.get(k):
            m[k.replace("_show", "")] = ids[k]
    return m


def _auto_remove_across(ev: Any, cfg: dict[str, Any]) -> None:
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
            if _rm_across_api:
                _rm_across_api(ids, mt)  # type: ignore[misc]
            return
        except Exception:
            pass
    except Exception:
        pass


def _media_name(ev: Any) -> str:
    if getattr(ev, "media_type", "") == "episode":
        s = getattr(ev, "season", None)
        n = getattr(ev, "number", None)
        t = getattr(ev, "title", None) or "?"
        try:
            return f"{t} S{int(s or 0):02d}E{int(n or 0):02d}"
        except Exception:
            return t
    return getattr(ev, "title", None) or "?"


def _ids_desc_map(ids: dict[str, Any]) -> str:
    for k in ("simkl", "imdb", "tmdb", "tvdb"):
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


def _bodies(ev: Any, p: float) -> list[dict[str, Any]]:
    ids = _ids(ev)
    show = _show_ids(ev)
    media_type = str(getattr(ev, "media_type", "") or "").lower()
    is_anime_type = media_type == "anime"
    has_anime_ids = any((getattr(ev, "ids", {}) or {}).get(k) for k in (
        "mal",
        "anidb",
        "anilist",
        "kitsu",
        "mal_show",
        "anidb_show",
        "anilist_show",
        "kitsu_show",
    ))
    parent = "anime" if (is_anime_type or has_anime_ids) else "show"

    if media_type == "movie":
        if ids:
            return [{"progress": p, "movie": {"ids": ids}}]
        payload: dict[str, Any] = {"title": getattr(ev, "title", None)}
        year = getattr(ev, "year", None)
        if year is not None:
            payload["year"] = year
        return [{"progress": p, "movie": payload}]

    bodies: list[dict[str, Any]] = []
    season = getattr(ev, "season", None)
    number = getattr(ev, "number", None)
    has_sn = season is not None and number is not None

    if has_sn and show:
        bodies.append(
            {
                "progress": p,
                parent: {"ids": show},
                "episode": {"season": season, "number": number},
            }
        )
    if has_sn and not show:
        s_payload: dict[str, Any] = {"title": getattr(ev, "title", None)}
        year = getattr(ev, "year", None)
        if year is not None:
            s_payload["year"] = year
        bodies.append(
            {
                "progress": p,
                parent: s_payload,
                "episode": {"season": season, "number": number},
            }
        )
    if ids:
        bodies.append({"progress": p, parent: {}, "episode": {"ids": ids}})

    if bodies:
        return bodies

    return [
        {
            "progress": p,
            parent: {"ids": show or {}},
            "episode": {"season": season or 0, "number": number or 0},
        }
    ]


class SimklSink(ScrobbleSink):
    def __init__(self, logger: Any | None = None) -> None:
        self._last_sent: dict[str, float] = {}
        self._p_sess: dict[tuple[str, str], int] = {}
        self._p_glob: dict[str, int] = {}
        self._best: dict[str, dict[str, Any]] = {}
        self._ids_logged: set[str] = set()
        self._last_intent_path: dict[str, str] = {}
        self._last_intent_prog: dict[str, int] = {}
        self._warn_no_token = False
        self._warn_no_key = False

    def _mkey(self, ev: Any) -> str:
        ids = getattr(ev, "ids", {}) or {}
        parts: list[str] = []
        for k in ("imdb", "tmdb", "tvdb", "simkl"):
            if ids.get(k):
                parts.append(f"{k}:{ids[k]}")
        if getattr(ev, "media_type", "") == "episode":
            for k in ("imdb_show", "tmdb_show", "tvdb_show", "simkl_show"):
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
        if action == "start":
            return False
        k = f"{session_key or '?'}:{action}"
        now = time.time()
        if (now - self._last_sent.get(k, 0.0)) < max(1, int(debounce_s)):
            return True
        self._last_sent[k] = now
        return False

    def _should_log_intent(self, key: str, path: str, prog: int) -> bool:
        lp = self._last_intent_path.get(key)
        pp = self._last_intent_prog.get(key, -1)
        changed = (lp != path) or (abs(int(prog) - int(pp)) >= 5)
        if changed:
            self._last_intent_path[key] = path
            self._last_intent_prog[key] = int(prog)
        return changed

    def send(self, ev: Any) -> None:
        cfg = _cfg()
        s = cfg.get("simkl") or {}
        api_key = s.get("api_key") or s.get("client_id")
        token = s.get("access_token")

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

        action_in = (getattr(ev, "action", "") or "").lower().strip()
        action = action_in if action_in in ("start", "pause", "stop") else "stop"
        sess = getattr(ev, "session_key", None) or getattr(ev, "session", None)
        sk = str(sess or "?")
        mk = self._mkey(ev)

        p_now = _clamp(getattr(ev, "progress", 0) or 0)
        tol = _regress_tolerance_percent(cfg)
        p_sess = self._p_sess.get((sk, mk), -1)
        p_glob = self._p_glob.get(mk, -1)

        name = _media_name(ev)
        key = self._ckey(ev)

        if action == "start":
            if p_now <= 2 and (p_sess >= 10 or p_glob >= 10):
                p_send = 2
                self._p_glob[mk] = max(2, p_glob if p_glob >= 0 else 2)
                self._p_sess[(sk, mk)] = 2
            else:
                if p_now == 0 and p_glob > 0:
                    p_send = max(2, p_glob)
                elif p_glob >= 0 and (p_glob - p_now) > 0 and (p_glob - p_now) <= tol and p_now > 2:
                    p_send = p_glob
                else:
                    p_send = max(2, p_now)
        else:
            p_base = p_now
            if action == "pause" and p_base >= 98 and p_sess >= 0 and p_sess < 95:
                _log(f"Clamp suspicious pause 100% → {p_sess}%", "DEBUG")
                p_base = p_sess
            if p_sess < 0 or p_base >= p_sess or (p_sess - p_base) >= tol:
                p_send = p_base
            else:
                p_send = p_sess

        thr = _stop_pause_threshold(cfg)
        last_sess = p_sess
        comp = _complete_at(cfg)
        suppress_at = _watch_suppress_start_at(cfg)

        if action == "start" and p_send >= suppress_at:
            _log(f"suppress start at {p_send}% (>= {suppress_at}%)", "DEBUG")
            if p_send > (p_glob if p_glob >= 0 else -1):
                self._p_glob[mk] = p_send
            self._p_sess[(sk, mk)] = p_send
            return

        if comp and p_send >= comp and action not in ("stop", "start"):
            action = "stop"

        if action_in == "stop":
            if p_send >= _force_stop_at(cfg) or (comp and p_send >= comp):
                action = "stop"
            elif p_send >= 98 and last_sess >= 0 and last_sess < thr and (p_send - last_sess) >= 30:
                _log(f"Demote STOP→PAUSE (jump {last_sess}%→{p_send}%, thr={thr})", "DEBUG")
                action = "pause"
                p_send = last_sess
            elif p_send < thr:
                action = "pause"

        if p_send != p_sess:
            self._p_sess[(sk, mk)] = p_send
        if p_send > (p_glob if p_glob >= 0 else -1):
            self._p_glob[mk] = p_send

        comp_thr = max(_force_stop_at(cfg), comp or 0)
        if not (action == "stop" and p_send >= comp_thr):
            if self._debounced(sk, action, _watch_pause_debounce(cfg)):
                return

        path = {"start": "/scrobble/start", "pause": "/scrobble/pause", "stop": "/scrobble/stop"}[action]

        bodies = [{**b, **_app_meta(cfg)} for b in _bodies(ev, float(p_send))]

        last_err: dict[str, Any] | None = None
        for body in bodies:
            intent_prog = int(float(body.get("progress") or p_send))
            if self._should_log_intent(key, path, intent_prog):
                _log(f"simkl intent {path} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")
            res = self._send_http(path, body, cfg)
            if res.get("ok"):
                try:
                    act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
                except Exception:
                    act = path.rsplit("/", 1)[-1]
                _log(f"simkl {path} -> {res['status']} action={act}", "DEBUG")
                try:
                    acc = getattr(ev, "account", None)
                    prog_val = float(body.get("progress") or p_send)
                    _log(f"user='{acc}' {act} {prog_val:.1f}% • {name}", "INFO")
                except Exception:
                    pass
                if action == "stop" and p_send >= comp_thr:
                    _auto_remove_across(ev, cfg)
                return
            last_err = res
            if res.get("status") == 404:
                _log("404 with current representation → trying alternate", "WARN")
                continue
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
            except Exception:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

            s = r.status_code
            if s in (423, 429):
                ra = r.headers.get("Retry-After")
                try:
                    wait = float(ra) if ra else backoff
                except Exception:
                    wait = backoff
                time.sleep(max(1.0, min(20.0, wait)))
                backoff = min(8.0, backoff * 2)
                continue

            if 500 <= s < 600:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

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

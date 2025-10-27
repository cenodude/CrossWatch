# providers/scrobble/trakt/sink.py
from __future__ import annotations

import time, json, requests
from pathlib import Path
from typing import Any

TRAKT_API   = "https://api.trakt.tv"
APP_AGENT   = "CrossWatch/Scrobble/1.1"
_TOKEN_OVERRIDE: str | None = None

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink

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

def _save_cfg(cfg: dict[str, Any]) -> None:
    try:
        from crosswatch import save_config
        save_config(cfg); return
    except Exception:
        pass
    try:
        Path("/config/config.json").write_text(json.dumps(cfg, ensure_ascii=False, indent=2), encoding="utf-8")
    except Exception:
        pass

def _is_debug() -> bool:
    try: return bool((( _cfg().get("runtime") or {}).get("debug")))
    except Exception: return False

def _app_meta(cfg: dict[str, Any]) -> dict[str, str]:
    rt = (cfg.get("runtime") or {})
    av = str(rt.get("version") or APP_AGENT)
    ad = (rt.get("build_date") or "").strip()
    return {"app_version": av, **({"app_date": ad} if ad else {})}

def _hdr(cfg: dict[str, Any]) -> dict[str, str]:
    t = (cfg.get("trakt") or {})
    client_id = str(t.get("client_id") or t.get("api_key") or "")
    token = _TOKEN_OVERRIDE or t.get("access_token") or ((cfg.get("auth") or {}).get("trakt") or {}).get("access_token") or ""
    h = {"Content-Type":"application/json","trakt-api-version":"2","trakt-api-key":client_id,"User-Agent":APP_AGENT}
    if token: h["Authorization"] = f"Bearer {token}"
    return h

def _get(path: str, cfg: dict[str, Any]):  return requests.get(f"{TRAKT_API}{path}", headers=_hdr(cfg), timeout=10)
def _post(path: str, body: dict[str, Any], cfg: dict[str, Any]): return requests.post(f"{TRAKT_API}{path}", headers=_hdr(cfg), json=body, timeout=10)
def _del(path: str, cfg: dict[str, Any]):  return requests.delete(f"{TRAKT_API}{path}", headers=_hdr(cfg), timeout=10)

def _tok_refresh(cfg: dict[str, Any]) -> bool:
    global _TOKEN_OVERRIDE
    t = (cfg.get("trakt") or {})
    client_id  = t.get("client_id") or t.get("api_key")
    client_sec = t.get("client_secret") or t.get("client_secret_id")
    rtok = t.get("refresh_token") or (((cfg.get("auth") or {}).get("trakt") or {}).get("refresh_token"))
    if not (client_id and client_sec and rtok):
        _log("Missing credentials for token refresh", "ERROR"); return False
    try:
        r = requests.post(f"{TRAKT_API}/oauth/token",
                          json={"grant_type":"refresh_token","refresh_token":rtok,"client_id":client_id,"client_secret":client_sec},
                          headers={"User-Agent":APP_AGENT,"Content-Type":"application/json"}, timeout=10)
    except Exception as e:
        _log(f"Token refresh failed (network): {e}", "ERROR"); return False
    if r.status_code != 200:
        _log(f"Token refresh failed {r.status_code}: {(r.text or '')[:400]}", "ERROR"); return False
    try: data = r.json()
    except Exception: data = {}
    acc = data.get("access_token")
    if not acc:
        _log("Token refresh: missing access_token", "ERROR"); return False
    _TOKEN_OVERRIDE = acc
    new_rt = data.get("refresh_token") or rtok
    try:
        new_cfg = dict(cfg); t2 = dict(new_cfg.get("trakt") or {})
        t2["access_token"], t2["refresh_token"] = acc, new_rt
        new_cfg["trakt"] = t2; _save_cfg(new_cfg)
        _log("Trakt token refreshed (persisted)", "DEBUG")
    except Exception:
        _log("Trakt token refreshed (runtime only)", "DEBUG")
    return True

def _ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}; return {k: ids[k] for k in ("imdb","tmdb","tvdb","trakt") if ids.get(k)}

def _show_ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}; m={}
    for k in ("imdb_show","tmdb_show","tvdb_show","trakt_show"):
        if ids.get(k): m[k.replace("_show","")] = ids[k]
    return m

def _clamp(p: Any) -> int:
    try: p = int(p)
    except Exception: p = 0
    return max(0, min(100, p))

def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try: return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 80))
    except Exception: return 80

def _force_stop_at(cfg: dict[str, Any]) -> int:
    try: return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at", 95))
    except Exception: return 95

def _regress_tol(cfg: dict[str, Any]) -> int:
    try: return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("regress_tolerance_percent", 5))
    except Exception: return 5

def _guid_search(ev: ScrobbleEvent, cfg: dict[str, Any]) -> dict[str, Any] | None:
    ids = ev.ids or {}
    for key in ("imdb","tvdb","tmdb"):
        val = ids.get(key)
        if not val: continue
        try: r = _get(f"/search/{key}/{val}?type=episode", cfg)
        except Exception: continue
        if r.status_code == 401 and _tok_refresh(cfg):
            try: r = _get(f"/search/{key}/{val}?type=episode", cfg)
            except Exception: continue
        if r.status_code != 200: continue
        try: arr = r.json() or []
        except Exception: arr = []
        for hit in arr:
            epi_ids = ((hit.get("episode") or {}).get("ids") or {})
            out = {k:v for k,v in epi_ids.items() if k in ("trakt","imdb","tmdb","tvdb") and v}
            if out: return out
    return None

def _log(msg: str, level: str = "INFO") -> None:
    if level.upper() == "DEBUG" and not _is_debug(): return
    print(f"{level} [TRAKT] {msg}")

def _dbg(msg: str) -> None:
    if _is_debug(): print(f"DEBUG [TRAKT] {msg}")

try:
    from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
except Exception:
    _rm_across = None
try:
    from _watchlistAPI import remove_across_providers_by_ids as _rm_across_api  # type: ignore
except Exception:
    _rm_across_api = None

def _cfg_delete_enabled(cfg: dict[str, Any], media_type: str) -> bool:
    s = (cfg.get("scrobble") or {})
    if not s.get("delete_plex"): return False
    types = s.get("delete_plex_types") or []
    if isinstance(types, list): return (media_type in types) or (media_type.rstrip("s")+"s" in types)
    if isinstance(types, str):  return media_type in types
    return False

def _auto_remove_across(ev: ScrobbleEvent, cfg: dict[str, Any]) -> None:
    if not _cfg_delete_enabled(cfg, ev.media_type): return
    ids = _ids(ev)
    if not ids: return
    try:
        if callable(_rm_across):
            _log(f"Auto-remove across providers via _auto_remove_watchlist ids={ids}", "INFO")
            _rm_across(ids, ev.media_type)
            return
    except Exception as e:
        _log(f"Auto-remove across (_auto_remove_watchlist) failed: {e}", "WARN")
    try:
        if callable(_rm_across_api):
            _log(f"Auto-remove across providers via _watchlistAPI ids={ids}", "INFO")
            _rm_across_api(ids, ev.media_type)  # type: ignore
            return
    except Exception as e:
        _log(f"Auto-remove across (_watchlistAPI) failed: {e}", "WARN")

def _clear_active_checkin(cfg: dict[str, Any]) -> bool:
    try:
        r = _del("/checkin", cfg)
        return r.status_code in (204, 200)
    except Exception:
        return False

def _ids_desc_map(ids: dict[str, Any]) -> str:
    for k in ("trakt","imdb","tmdb","tvdb"):
        v = ids.get(k)
        if v is not None: return f"{k}:{v}"
    return "title/year"

def _media_name(ev: ScrobbleEvent) -> str:
    if ev.media_type == "episode":
        s = ev.season if ev.season is not None else 0
        n = ev.number if ev.number is not None else 0
        t = ev.title or "?"
        try: return f"{t} S{int(s):02d}E{int(n):02d}"
        except Exception: return f"{t}"
    return ev.title or "?"

def _extract_skeleton_from_body(b: dict[str, Any]) -> dict[str, Any]:
    out = dict(b)
    out.pop("progress", None)
    out.pop("app_version", None)
    out.pop("app_date", None)
    return out

def _body_ids_desc(b: dict[str, Any]) -> str:
    ids = ((b.get("movie") or {}).get("ids")) or ((b.get("show") or {}).get("ids")) or ((b.get("episode") or {}).get("ids")) or {}
    return _ids_desc_map(ids if isinstance(ids, dict) else {})

class TraktSink(ScrobbleSink):
    def __init__(self, logger=None):
        self._logr = logger if logger else (BASE_LOG.child("TRAKT") if (BASE_LOG and hasattr(BASE_LOG,"child")) else None)
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

    def _mkey(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}; parts=[]
        for k in ("imdb","tmdb","tvdb","trakt"):
            if ids.get(k): parts.append(f"{k}:{ids[k]}")
        if ev.media_type == "episode":
            for k in ("imdb_show","tmdb_show","tvdb_show","trakt_show"):
                if ids.get(k): parts.append(f"{k}:{ids[k]}")
            parts.append(f"S{(ev.season or 0):02d}E{(ev.number or 0):02d}")
        if not parts:
            t,y = ev.title or "", ev.year or 0
            parts.append(f"{t}|{y}" + (f"|S{(ev.season or 0):02d}E{(ev.number or 0):02d}" if ev.media_type=="episode" else ""))
        return "|".join(parts)

    def _ckey(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        if ids.get("plex"): return f"plex:{ids.get('plex')}"
        return self._mkey(ev)

    def _debounced(self, session_key: str | None, action: str) -> bool:
        if action == "start": return False
        k = f"{session_key}:{action}"; now=time.time()
        if now - self._last_sent.get(k,0.0) < 5.0: return True
        self._last_sent[k] = now; return False

    def _bodies(self, ev: ScrobbleEvent, p: int) -> list[dict[str, Any]]:
        ids = _ids(ev); show = _show_ids(ev)
        if ev.media_type == "movie":
            return [{"progress": p, "movie": {"ids": ids}}] if ids else [{"progress": p, "movie": {"title": ev.title, **({"year":ev.year} if ev.year is not None else {})}}]
        bodies: list[dict[str, Any]] = []
        has_sn = (ev.season is not None and ev.number is not None)
        if ids: bodies.append({"progress": p, "episode": {"ids": ids}})
        if has_sn and show:
            bodies.append({"progress": p, "show": {"ids": show}, "episode": {"season": ev.season, "number": ev.number}})
        if has_sn and not show:
            s = {"title": ev.title, **({"year":ev.year} if ev.year is not None else {})}
            bodies.append({"progress": p, "show": s, "episode": {"season": ev.season, "number": ev.number}})
        return bodies or [{"progress": p, "episode": {"ids": ids}}]

    def _send_http(self, path: str, body: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
        backoff, tried_refresh, tried_checkin_clear = 1.0, False, False
        for _ in range(6):
            try: r = _post(path, body, cfg)
            except Exception:
                time.sleep(backoff); backoff=min(8.0, backoff*2); continue
            s = r.status_code
            if s == 401 and not tried_refresh:
                _log("401 Unauthorized → refreshing token", "WARN")
                if _tok_refresh(cfg): tried_refresh=True; continue
                return {"ok":False,"status":401,"resp":"Unauthorized and token refresh failed"}
            if s == 409:
                txt = (r.text or "")
                if not tried_checkin_clear and ("expires_at" in txt or "watched_at" in txt):
                    _log("409 Conflict (active check-in) — clearing /checkin and retrying", "WARN")
                    tried_checkin_clear = True
                    if _clear_active_checkin(cfg):
                        time.sleep(0.35)
                        continue
                return {"ok":False,"status":409,"resp":txt[:400]}
            if s == 429:
                try: wait = float(r.headers.get("Retry-After") or backoff)
                except Exception: wait = backoff
                time.sleep(max(0.5, min(30.0, wait))); backoff=min(8.0, backoff*2); continue
            if 500 <= s < 600:
                time.sleep(backoff); backoff=min(8.0, backoff*2); continue
            if s >= 400:
                short = (r.text or "")[:400]
                if s == 404: short += " (Trakt could not match the item)"
                try:
                    j = r.json()
                    return {"ok":False,"status":s,"resp":j}
                except Exception:
                    return {"ok":False,"status":s,"resp":short}
            try:
                return {"ok":True,"status":s,"resp":r.json()}
            except Exception:
                return {"ok":True,"status":s,"resp":(r.text or "")[:400]}
        return {"ok":False,"status":429,"resp":"rate_limited"}

    def _should_log_intent(self, key: str, path: str, prog: int) -> bool:
        last_p = self._last_intent_prog.get(key, None)
        last_path = self._last_intent_path.get(key, None)
        if last_path != path: ok = True
        elif last_p is None: ok = True
        else: ok = (prog - int(last_p)) >= 5
        if ok:
            self._last_intent_path[key] = path
            self._last_intent_prog[key] = int(prog)
        return ok

    def send(self, ev: ScrobbleEvent) -> None:
        cfg = _cfg()
        sk, mk = str(ev.session_key or "?"), self._mkey(ev)
        p_now  = _clamp(ev.progress)
        tol    = _regress_tol(cfg)
        p_sess = self._p_sess.get((sk,mk), -1)
        p_glob = self._p_glob.get(mk, -1)

        name = _media_name(ev)
        ids_now = _ids(ev)
        key = self._ckey(ev)
        if key not in self._ids_logged:
            _log(f"ids resolved: {name} -> {_ids_desc_map(ids_now)}", "DEBUG")
            self._ids_logged.add(key)

        if ev.action == "start":
            if p_now <= 2 and (p_sess >= 10 or p_glob >= 10):
                _log("Restart detected: align start floor to 2% (no 0%)", "DEBUG")
                p_send = 2
                self._p_glob[mk] = max(2, p_glob if p_glob >= 0 else 2)
                self._p_sess[(sk,mk)] = 2
            else:
                if p_now == 0 and p_glob > 0: p_send = max(2, p_glob)
                elif p_glob >= 0 and (p_glob - p_now) > 0 and (p_glob - p_now) <= tol and p_now > 2: p_send = p_glob
                else: p_send = max(2, p_now)
        else:
            p_base = p_now
            if ev.action == "pause" and p_base >= 98 and p_sess >= 0 and p_sess < 95:
                _dbg(f"Clamp suspicious pause 100% → {p_sess}%"); p_base = p_sess
            p_send = p_base if (p_sess < 0 or p_base >= p_sess or (p_sess - p_base) >= tol) else p_sess

        thr = _stop_pause_threshold(cfg)
        last_sess = p_sess
        action = ev.action
        if ev.action == "stop":
            if p_send >= 98 and last_sess >= 0 and last_sess < thr and (p_send - last_sess) >= 30:
                _log(f"Demote STOP→PAUSE (jump {last_sess}%→{p_send}%, thr={thr})", "DEBUG")
                action = "pause"; p_send = last_sess
            elif p_send < thr:
                action = "pause"

        if p_send != p_sess: self._p_sess[(sk,mk)] = p_send
        if p_send > (p_glob if p_glob >= 0 else -1): self._p_glob[mk] = p_send

        if not (action == "stop" and p_send >= _force_stop_at(cfg)) and self._debounced(ev.session_key, action): return

        path = { "start":"/scrobble/start", "pause":"/scrobble/pause", "stop":"/scrobble/stop" }[action]
        last_err = None

        best = self._best.get(key)
        if not best and ev.media_type == "episode":
            found = _guid_search(ev, cfg)
            if found:
                epi_ids = {"trakt": found["trakt"]} if "trakt" in found else found
                skeleton = {"episode": {"ids": epi_ids}}
                self._best[key] = {"skeleton": skeleton, "ids_desc": _ids_desc_map(epi_ids), "ts": time.time()}
                best = self._best.get(key)

        bodies: list[dict[str, Any]] = []
        if best and isinstance(best.get("skeleton"), dict):
            b0 = {"progress": p_send, **best["skeleton"], **_app_meta(cfg)}
            if self._should_log_intent(key, path, int(b0.get("progress") or p_send)):
                _log(f"trakt intent {path} using cached {best.get('ids_desc','title/year')}, prog={b0.get('progress')}", "DEBUG")
            bodies.append(b0)
        else:
            bodies = [{**b, **_app_meta(cfg)} for b in self._bodies(ev, p_send)]

        for i, body in enumerate(bodies):
            if not (best and i == 0):
                prog_i = int(float(body.get("progress") or p_send))
                if self._should_log_intent(key, path, prog_i):
                    _log(f"trakt intent {path} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")
            res = self._send_http(path, body, cfg)
            if res.get("ok"):
                try: act = (res.get("resp") or {}).get("action") or path.rsplit("/",1)[-1]
                except Exception: act = path.rsplit("/",1)[-1]
                _log(f"trakt {path} -> {res['status']} action={act}", "DEBUG")
                _log(f"{path} {res['status']}", "DEBUG")
                skeleton = _extract_skeleton_from_body(body)
                self._best[key] = {"skeleton": skeleton, "ids_desc": _body_ids_desc(body), "ts": time.time()}
                if action == "stop" and p_send >= _force_stop_at(cfg):
                    _auto_remove_across(ev, cfg)
                try:
                    _log(f"user='{ev.account}' {act} {float(body.get('progress') or p_send):.1f}% • {name}", "INFO")
                except Exception:
                    pass
                return
            last_err = res
            if res.get("status") == 404:
                _log("404 with current representation → trying alternate", "WARN"); continue
            break

        if last_err and last_err.get("status") == 404 and ev.media_type == "episode":
            epi_ids = _guid_search(ev, cfg)
            if epi_ids:
                body = {"progress": p_send, "episode": {"ids": epi_ids}, **_app_meta(cfg)}
                if self._should_log_intent(key, path, int(body.get("progress") or p_send)):
                    _log(f"trakt intent {path} using {_ids_desc_map(epi_ids)}, prog={body.get('progress')}", "DEBUG")
                res = self._send_http(path, body, cfg)
                if res.get("ok"):
                    try: act = (res.get("resp") or {}).get("action") or path.rsplit("/",1)[-1]
                    except Exception: act = path.rsplit("/",1)[-1]
                    _log(f"trakt {path} -> {res['status']} action={act}", "DEBUG")
                    _log(f"{path} {res['status']}", "DEBUG")
                    skeleton = _extract_skeleton_from_body(body)
                    self._best[key] = {"skeleton": skeleton, "ids_desc": _ids_desc_map(epi_ids), "ts": time.time()}
                    if action == "stop" and p_send >= _force_stop_at(cfg):
                        _auto_remove_across(ev, cfg)
                    try:
                        _log(f"user='{ev.account}' {act} {float(body.get('progress') or p_send):.1f}% • {name}", "INFO")
                    except Exception:
                        pass
                    return
                last_err = res

        if last_err and last_err.get("status") == 409 and action == "stop" and ("watched_at" in str(last_err.get("resp"))):
            _log("Treating 409 with watched_at as watched; proceeding to auto-remove", "WARN")
            if p_send >= _force_stop_at(cfg):
                _auto_remove_across(ev, cfg)
            return

        if last_err: _log(f"{path} {last_err.get('status')} err={last_err.get('resp')}", "ERROR")

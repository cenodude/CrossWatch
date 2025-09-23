"""Trakt scrobble sink
- refactored at 21-09-2025
- Retries with 401 refresh + rate-limit backoff
- Debounce non-start events (STOP bypass at high progress)
- STOP->PAUSE under threshold + demote suspicious STOP jumps
- Progress memory (session/global), clamp suspicious 100% (non-STOP)
- Prefer episode GUIDs; on 404, try alternates; last-resort GUID search
"""

import time, json, requests

TRAKT_API = "https://api.trakt.tv"
APP_AGENT  = "CrossWatch/Scrobble/1.1"
_TOKEN_OVERRIDE = None  # runtime access token

# Optional project logger
try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink


# --- config / http -------------------------------------------------------------

def _cfg():
    try:
        from crosswatch import load_config
        return load_config()
    except Exception:
        with open("config.json", "r", encoding="utf-8") as f:
            return json.load(f)

def _hdr(cfg):
    t = (cfg.get("trakt") or {})
    client_id = t.get("client_id") or t.get("api_key") or ""
    token = _TOKEN_OVERRIDE or t.get("access_token") or ((cfg.get("auth") or {}).get("trakt") or {}).get("access_token") or ""
    h = {"Content-Type":"application/json","trakt-api-version":"2","trakt-api-key":client_id,"User-Agent":APP_AGENT}
    if token: h["Authorization"] = f"Bearer {token}"
    return h

def _get(path, cfg):
    return requests.get(f"{TRAKT_API}{path}", headers=_hdr(cfg), timeout=10)

def _post(path, body, cfg):
    return requests.post(f"{TRAKT_API}{path}", headers=_hdr(cfg), json=body, timeout=10)

def _tok_refresh(cfg):
    """Refresh OAuth token; update runtime + persist if possible."""
    global _TOKEN_OVERRIDE
    t = (cfg.get("trakt") or {})
    client_id  = t.get("client_id") or t.get("api_key")
    client_sec = t.get("client_secret") or t.get("client_secret_id")
    rtok = t.get("refresh_token") or (((cfg.get("auth") or {}).get("trakt") or {}).get("refresh_token"))
    if not (client_id and client_sec and rtok):
        if BASE_LOG: BASE_LOG("Missing credentials for token refresh", level="ERROR", module="TRAKT")
        return False
    try:
        r = requests.post(f"{TRAKT_API}/oauth/token",
                          json={"grant_type":"refresh_token","refresh_token":rtok,"client_id":client_id,"client_secret":client_sec},
                          headers={"User-Agent":APP_AGENT,"Content-Type":"application/json"}, timeout=10)
    except Exception as e:
        if BASE_LOG: BASE_LOG(f"Token refresh failed (network): {e}", level="ERROR", module="TRAKT")
        return False
    if r.status_code != 200:
        if BASE_LOG: BASE_LOG(f"Token refresh failed {r.status_code}: {(r.text or '')[:400]}", level="ERROR", module="TRAKT")
        return False
    data = {}
    try: data = r.json()
    except: pass
    acc = data.get("access_token")
    if not acc:
        if BASE_LOG: BASE_LOG("Token refresh: missing access_token", level="ERROR", module="TRAKT")
        return False
    _TOKEN_OVERRIDE = acc
    rt = data.get("refresh_token") or rtok
    try:
        from crosswatch import save_config
        new_cfg = dict(cfg)
        new_cfg.setdefault("trakt", dict(new_cfg.get("trakt") or {}))
        new_cfg["trakt"]["access_token"]  = acc
        new_cfg["trakt"]["refresh_token"] = rt
        save_config(new_cfg)
        if BASE_LOG: BASE_LOG("Trakt token refreshed (persisted)", level="INFO", module="TRAKT")
    except Exception:
        if BASE_LOG: BASE_LOG("Trakt token refreshed (runtime only)", level="INFO", module="TRAKT")
    return True


# --- id utils ------------------------------------------------------------------

def _ids(ev: ScrobbleEvent):
    ids = ev.ids or {}
    return {k: ids[k] for k in ("imdb","tmdb","tvdb","trakt") if ids.get(k)}

def _show_ids(ev: ScrobbleEvent):
    ids = ev.ids or {}
    m = {}
    for k in ("imdb_show","tmdb_show","tvdb_show","trakt_show"):
        if ids.get(k): m[k.replace("_show","")] = ids[k]
    return m

def _clamp(p):  # 0..100 int
    try: p = int(p)
    except: p = 0
    return max(0, min(100, p))

def _stop_pause_threshold(cfg):  # default 80
    try: return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 80))
    except: return 80

def _force_stop_at(cfg):  # bypass debounce at/above this; default 95
    try: return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at", 95))
    except: return 95

def _regress_tol(cfg):  # default 5
    try: return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("regress_tolerance_percent", 5))
    except: return 5

def _guid_search(ev: ScrobbleEvent, cfg):
    """Best-effort episode ID resolve via Trakt search."""
    ids = ev.ids or {}
    for key in ("imdb","tvdb","tmdb"):
        val = ids.get(key)
        if not val: continue
        try: r = _get(f"/search/{key}/{val}?type=episode", cfg)
        except: continue
        if r.status_code == 401 and _tok_refresh(cfg):
            try: r = _get(f"/search/{key}/{val}?type=episode", cfg)
            except: continue
        if r.status_code != 200: continue
        try: arr = r.json() or []
        except: arr = []
        for hit in arr:
            epi_ids = ((hit.get("episode") or {}).get("ids") or {})
            out = {k:v for k,v in epi_ids.items() if k in ("trakt","imdb","tmdb","tvdb") and v}
            if out: return out
    return None


# --- sink ----------------------------------------------------------------------

class TraktSink(ScrobbleSink):
    def __init__(self, logger=None):
        self._logr = logger if logger else BASE_LOG.child("TRAKT") if (BASE_LOG and hasattr(BASE_LOG,"child")) else None
        try:
            if self._logr and hasattr(self._logr,"set_level"): self._logr.set_level("INFO")
        except: pass
        self._last_sent = {}               # debounce: "session:action" -> ts
        self._p_sess   = {}                # (session, media) -> %
        self._p_glob   = {}                # media -> %

    def _log(self, msg, level="INFO"):
        if BASE_LOG:
            try: BASE_LOG(str(msg), level=level.upper(), module="TRAKT"); return
            except: pass
        print(f"{level} [TRAKT] {msg}")

    def _mkey(self, ev: ScrobbleEvent):
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

    def _debounced(self, session_key, action):
        if action == "start": return False
        k = f"{session_key}:{action}"; now=time.time()
        if now - self._last_sent.get(k,0) < 5: return True
        self._last_sent[k] = now; return False

    def _bodies(self, ev: ScrobbleEvent, p: int):
        ids = _ids(ev); show = _show_ids(ev)
        if ev.media_type == "movie":
            return [{"progress": p, "movie": {"ids": ids}}] if ids else [{"progress": p, "movie": {"title": ev.title, **({"year":ev.year} if ev.year is not None else {})}}]
        bodies=[]
        has_sn = (ev.season is not None and ev.number is not None)
        if ids: bodies.append({"progress": p, "episode": {"ids": ids}})
        if has_sn and show:
            bodies.append({"progress": p, "show": {"ids": show}, "episode": {"season": ev.season, "number": ev.number}})
        if has_sn and not show:
            s = {"title": ev.title, **({"year":ev.year} if ev.year is not None else {})}
            bodies.append({"progress": p, "show": s, "episode": {"season": ev.season, "number": ev.number}})
        return bodies or [{"progress": p, "episode": {"ids": ids}}]

    def _send(self, path, body, cfg):
        backoff, tried_refresh = 1.0, False
        for _ in range(5):
            try: r = _post(path, body, cfg)
            except: time.sleep(backoff); backoff=min(8.0, backoff*2); continue
            s = r.status_code
            if s == 401 and not tried_refresh:
                self._log("401 Unauthorized → refreshing token", "WARN")
                if _tok_refresh(cfg): tried_refresh=True; continue
                return {"ok":False,"status":401,"resp":"Unauthorized and token refresh failed"}
            if s == 429:
                try: wait = float(r.headers.get("Retry-After") or backoff)
                except: wait = backoff
                time.sleep(max(0.5, min(30.0, wait))); backoff=min(8.0, backoff*2); continue
            if 500 <= s < 600:
                time.sleep(backoff); backoff=min(8.0, backoff*2); continue
            if s >= 400:
                short = (r.text or "")[:400]
                if s == 404: short += " (Trakt could not match the item)"
                return {"ok":False,"status":s,"resp":short}
            try: return {"ok":True,"status":s,"resp":r.json()}
            except: return {"ok":True,"status":s,"resp":(r.text or "")[:400]}
        return {"ok":False,"status":429,"resp":"rate_limited"}

    def send(self, ev: ScrobbleEvent):
        cfg = _cfg()
        sk, mk   = str(ev.session_key or "?"), self._mkey(ev)
        p_now    = _clamp(ev.progress)
        tol      = _regress_tol(cfg)
        p_sess   = self._p_sess.get((sk,mk), -1)
        p_glob   = self._p_glob.get(mk, -1)

        # Effective progress
        if ev.action == "start":
            if p_now <= 2 and (p_sess >= 10 or p_glob >= 10):
                self._log("Restart detected: honoring 0% and clearing memory")
                p_send = 0; self._p_glob[mk]=0; self._p_sess[(sk,mk)]=0
            else:
                if p_now == 0 and p_glob > 0: p_send = p_glob
                elif p_glob >= 0 and (p_glob - p_now) >= tol and p_now > 2: p_send = p_glob
                else: p_send = p_now
        else:
            p_base = p_now
            # clamp only for PAUSE; never clamp STOP
            if ev.action == "pause" and p_base >= 98 and p_sess >= 0 and p_sess < 95:
                self._log(f"Clamp suspicious pause 100% → {p_sess}%")
                p_base = p_sess
            if p_sess < 0 or p_base >= p_sess: p_send = p_base
            else: p_send = p_base if (p_sess - p_base) >= tol else p_sess

        # Decide final action (demote suspicious STOP jumps; then threshold)
        thr  = _stop_pause_threshold(cfg)
        last = max(p_sess, p_glob)
        action = ev.action
        if ev.action == "stop":
            if p_send >= 98 and last >= 0 and last < thr and (p_send - last) >= 30:
                self._log(f"Demote STOP→PAUSE (jump {last}%→{p_send}%, thr={thr})")
                action = "pause"
            elif p_send < thr:
                action = "pause"

        # Update memory
        if p_send != p_sess: self._p_sess[(sk,mk)] = p_send
        if p_send > (p_glob if p_glob >= 0 else -1): self._p_glob[mk] = p_send

        # Debounce (bypass only for final STOP at high progress)
        bypass = (action == "stop" and p_send >= _force_stop_at(cfg))
        if not bypass and self._debounced(ev.session_key, action): return

        path = { "start":"/scrobble/start", "pause":"/scrobble/pause", "stop":"/scrobble/stop" }[action]
        last_err = None

        # Preferred bodies (episode.ids first)
        for body in self._bodies(ev, p_send):
            self._log(f"→ {path} body={body}")
            res = self._send(path, body, cfg)
            if res.get("ok"):
                self._log(f"{path} {res['status']}")
                return
            last_err = res
            if res.get("status") == 404:
                self._log("404 with current representation → trying alternate", "WARN")
                continue
            break  # non-404

        # Last resort: GUID search
        if last_err and last_err.get("status") == 404 and ev.media_type == "episode":
            epi_ids = _guid_search(ev, cfg)
            if epi_ids:
                body = {"progress": p_send, "episode": {"ids": epi_ids}}
                self._log(f"Resolved via search; retry ids={epi_ids}")
                res = self._send(path, body, cfg)
                if res.get("ok"):
                    self._log(f"{path} {res['status']}")
                    return
                last_err = res

        if last_err:
            self._log(f"{path} {last_err.get('status')} err={last_err.get('resp')}", "ERROR")

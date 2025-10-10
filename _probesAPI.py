from __future__ import annotations
from typing import Any, Dict, Tuple, Optional
import os, json, time, urllib.request, urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

try:
    from plexapi.myplex import MyPlexAccount
    HAVE_PLEXAPI = True
except Exception:
    HAVE_PLEXAPI = False

# Tunables
HTTP_TIMEOUT = int(os.environ.get("CW_PROBE_HTTP_TIMEOUT", "3"))  # faster fail when down
STATUS_TTL   = int(os.environ.get("CW_STATUS_TTL", "60"))
PROBE_TTL    = int(os.environ.get("CW_PROBE_TTL", "15"))
USERINFO_TTL = int(os.environ.get("CW_USERINFO_TTL", "600"))

STATUS_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
PROBE_CACHE: Dict[str, Tuple[float, bool]] = {k: (0.0, False) for k in ("plex","simkl","trakt","jellyfin")}
PROBE_DETAIL_CACHE: Dict[str, Tuple[float, bool, str]] = {k: (0.0, False, "") for k in ("plex","simkl","trakt","jellyfin")}
_USERINFO_CACHE: Dict[str, Tuple[float, dict]] = {"plex": (0.0, {}), "trakt": (0.0, {})}

# HTTP
def _http_get(url: str, headers: Dict[str, str], timeout: int = HTTP_TIMEOUT) -> Tuple[int, bytes]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.getcode(), r.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read() if getattr(e, "fp", None) else b""
    except Exception:
        return 0, b""

def _json_loads(b: bytes) -> dict:
    try:
        return json.loads(b.decode("utf-8", errors="ignore"))
    except Exception:
        return {}

# Reason strings
def _reason_http(code: int, provider: str) -> str:
    if code == 0:   return f"{provider}: network error/timeout"
    if code == 401: return f"{provider}: unauthorized (token expired/revoked)"
    if code == 403: return f"{provider}: forbidden/invalid client id or scope"
    if code == 404: return f"{provider}: endpoint not found"
    if 500 <= code < 600: return f"{provider}: service error ({code})"
    return f"{provider}: http {code}"

# Basic probes (bool) kept for compatibility
def probe_plex(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["plex"]; now = time.time()
    if now - ts < max_age_sec: return ok
    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        PROBE_CACHE["plex"] = (now, False); return False
    headers = {
        "X-Plex-Token": token, "X-Plex-Client-Identifier": "crosswatch",
        "X-Plex-Product": "CrossWatch", "X-Plex-Version": "1.0",
        "Accept": "application/xml", "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://plex.tv/users/account", headers=headers)
    ok = (code == 200); PROBE_CACHE["plex"] = (now, ok); return ok

def probe_simkl(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["simkl"]; now = time.time()
    if now - ts < max_age_sec: return ok
    sk = (cfg.get("simkl") or cfg.get("SIMKL") or {})
    cid = (sk.get("client_id") or "").strip()
    tok = (sk.get("access_token") or sk.get("token") or "").strip()
    if not cid or not tok:
        PROBE_CACHE["simkl"] = (now, False); return False
    headers = {"Authorization": f"Bearer {tok}", "simkl-api-key": cid,
               "Accept": "application/json", "User-Agent": "CrossWatch/1.0"}
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers)
    ok = (code == 200); PROBE_CACHE["simkl"] = (now, ok); return ok

def probe_trakt(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["trakt"]; now = time.time()
    if now - ts < max_age_sec: return ok
    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        PROBE_CACHE["trakt"] = (now, False); return False
    headers = {"Authorization": f"Bearer {tok}", "trakt-api-key": cid, "trakt-api-version": "2",
               "Accept": "application/json", "User-Agent": "CrossWatch/1.0"}
    code, _ = _http_get("https://api.trakt.tv/users/settings", headers=headers)
    ok = (code == 200); PROBE_CACHE["trakt"] = (now, ok); return ok

def probe_jellyfin(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["jellyfin"]; now = time.time()
    if now - ts < max_age_sec: return ok
    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {})
    ok = bool((jf.get("server") or "").strip() and (jf.get("access_token") or jf.get("token") or "").strip())
    PROBE_CACHE["jellyfin"] = (now, ok); return ok

# Detailed probes (bool, reason)
def _probe_plex_detail(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> Tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["plex"]; now = time.time()
    if now - ts < max_age_sec: return ok, rsn
    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        rsn = "Plex: missing account_token"; PROBE_DETAIL_CACHE["plex"] = (now, False, rsn); return False, rsn
    headers = {
        "X-Plex-Token": token, "X-Plex-Client-Identifier": "crosswatch",
        "X-Plex-Product": "CrossWatch", "X-Plex-Version": "1.0",
        "Accept": "application/xml", "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://plex.tv/users/account", headers=headers)
    ok = (code == 200); rsn = "" if ok else _reason_http(code, "Plex")
    PROBE_DETAIL_CACHE["plex"] = (now, ok, rsn); return ok, rsn

def _probe_simkl_detail(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> Tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["simkl"]; now = time.time()
    if now - ts < max_age_sec: return ok, rsn
    sk = (cfg.get("simkl") or cfg.get("SIMKL") or {})
    cid = (sk.get("client_id") or "").strip()
    tok = (sk.get("access_token") or sk.get("token") or "").strip()
    if not cid or not tok:
        rsn = "SIMKL: missing token/client id"; PROBE_DETAIL_CACHE["simkl"] = (now, False, rsn); return False, rsn
    headers = {"Authorization": f"Bearer {tok}", "simkl-api-key": cid,
               "Accept": "application/json", "User-Agent": "CrossWatch/1.0"}
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers)
    ok = (code == 200); rsn = "" if ok else _reason_http(code, "SIMKL")
    PROBE_DETAIL_CACHE["simkl"] = (now, ok, rsn); return ok, rsn

def _probe_trakt_detail(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> Tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["trakt"]; now = time.time()
    if now - ts < max_age_sec: return ok, rsn
    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        rsn = "Trakt: missing token/client id"; PROBE_DETAIL_CACHE["trakt"] = (now, False, rsn); return False, rsn
    headers = {"Authorization": f"Bearer {tok}", "trakt-api-key": cid, "trakt-api-version": "2",
               "Accept": "application/json", "User-Agent": "CrossWatch/1.0"}
    code, _ = _http_get("https://api.trakt.tv/users/settings", headers=headers)
    ok = (code == 200); rsn = "" if ok else _reason_http(code, "Trakt")
    PROBE_DETAIL_CACHE["trakt"] = (now, ok, rsn); return ok, rsn

def _probe_jellyfin_detail(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> Tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["jellyfin"]; now = time.time()
    if now - ts < max_age_sec: return ok, rsn
    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {})
    if not (jf.get("server") or "").strip():
        rsn = "Jellyfin: missing server URL"; PROBE_DETAIL_CACHE["jellyfin"] = (now, False, rsn); return False, rsn
    if not (jf.get("access_token") or jf.get("token") or "").strip():
        rsn = "Jellyfin: missing access token"; PROBE_DETAIL_CACHE["jellyfin"] = (now, False, rsn); return False, rsn
    PROBE_DETAIL_CACHE["jellyfin"] = (now, True, ""); return True, ""

# Badges
def plex_user_info(cfg: Dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict:
    ts, info = _USERINFO_CACHE["plex"]; now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict): return info
    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        _USERINFO_CACHE["plex"] = (now, {}); return {}
    plexpass = plan = status = None
    if HAVE_PLEXAPI:
        try:
            acc = MyPlexAccount(token=token)
            plexpass = bool(getattr(acc, "subscriptionActive", None) or getattr(acc, "hasPlexPass", None))
            plan = getattr(acc, "subscriptionPlan", None) or None
            status = getattr(acc, "subscriptionStatus", None) or None
        except Exception:
            pass
    if plexpass is None:
        headers = {"X-Plex-Token": token, "X-Plex-Client-Identifier": "crosswatch",
                   "X-Plex-Product": "CrossWatch", "X-Plex-Version": "1.0",
                   "Accept": "application/json", "User-Agent": "CrossWatch/1.0"}
        code, body = _http_get("https://plex.tv/api/v2/user", headers=headers)
        if code == 200:
            j = _json_loads(body); sub = (j.get("subscription") or {})
            plexpass = bool(sub.get("active") or j.get("hasPlexPass"))
            plan = sub.get("plan") or plan; status = sub.get("status") or status
    out = {}
    if plexpass is not None:
        out["plexpass"] = bool(plexpass)
        out["subscription"] = {"plan": plan, "status": status}
    _USERINFO_CACHE["plex"] = (now, out); return out

def trakt_user_info(cfg: Dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict:
    ts, info = _USERINFO_CACHE["trakt"]; now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict): return info
    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        _USERINFO_CACHE["trakt"] = (now, {}); return {}
    headers = {"Authorization": f"Bearer {tok}", "trakt-api-key": cid, "trakt-api-version": "2",
               "Accept": "application/json", "User-Agent": "CrossWatch/1.0"}
    code, body = _http_get("https://api.trakt.tv/users/settings", headers=headers)
    out = {}
    if code == 200:
        j = _json_loads(body); u = j.get("user") or {}
        vip = bool(u.get("vip") or u.get("vip_og") or u.get("vip_ep"))
        vip_type = "vip"; 
        if u.get("vip_og"): vip_type = "vip_og"
        if u.get("vip_ep"): vip_type = "vip_ep"
        out = {"vip": vip, "vip_type": vip_type}
    _USERINFO_CACHE["trakt"] = (now, out); return out

# Helpers
def _prov_configured(cfg: dict, name: str) -> bool:
    n = (name or "").strip().lower()
    if n == "plex":     return bool((cfg.get("plex") or {}).get("account_token"))
    if n == "trakt":    return bool((cfg.get("trakt") or {}).get("access_token") or (cfg.get("auth") or {}).get("trakt", {}).get("access_token"))
    if n == "simkl":    return bool((cfg.get("simkl") or {}).get("access_token"))
    if n == "jellyfin":
        jf = cfg.get("jellyfin") or {}
        return bool((jf.get("server") or "").strip() and (jf.get("access_token") or jf.get("token") or "").strip())
    return False

def _pair_ready(cfg: dict, pair: dict) -> bool:
    if not isinstance(pair, dict): return False
    if pair.get("enabled", True) is False: return False
    def _name(x): 
        if isinstance(x, str): return x
        if isinstance(x, dict): return x.get("provider") or x.get("name") or x.get("id") or x.get("type") or ""
        return ""
    a = _name(pair.get("source") or pair.get("a") or pair.get("src") or pair.get("from"))
    b = _name(pair.get("target") or pair.get("b") or pair.get("dst") or pair.get("to"))
    return bool(_prov_configured(cfg, a) and _prov_configured(cfg, b))

def _safe_probe_detail(fn, cfg, max_age_sec=0) -> Tuple[bool, str]:
    try:
        return fn(cfg, max_age_sec=max_age_sec)
    except Exception as e:
        return False, f"probe failed: {e}"

def _safe_userinfo(fn, cfg, max_age_sec=0) -> dict:
    try:
        return fn(cfg, max_age_sec=max_age_sec) or {}
    except Exception as e:
        print(f"[status] userinfo {getattr(fn, '__name__', 'fn')} failed: {e}")
        return {}

def connected_status(cfg: Dict[str, Any]) -> Tuple[bool, bool, bool, bool]:
    # Back-compat: booleans only
    plex_ok, _  = _safe_probe_detail(_probe_plex_detail,  cfg, max_age_sec=PROBE_TTL)
    simkl_ok, _ = _safe_probe_detail(_probe_simkl_detail, cfg, max_age_sec=PROBE_TTL)
    trakt_ok, _ = _safe_probe_detail(_probe_trakt_detail, cfg, max_age_sec=PROBE_TTL)
    debug = bool(cfg.get("runtime", {}).get("debug"))
    return plex_ok, simkl_ok, trakt_ok, debug

# FastAPI
def register_probes(app: FastAPI, load_config_fn):
    @app.get("/api/status", tags=["Probes"])
    def api_status(fresh: int = Query(0)):
        now = time.time()
        cached = STATUS_CACHE["data"]
        age = (now - STATUS_CACHE["ts"]) if cached else 1e9
        if not fresh and cached and age < STATUS_TTL:
            return JSONResponse(cached, headers={"Cache-Control": "no-store"})

        cfg = load_config_fn() or {}
        pairs = cfg.get("pairs") or []
        any_pair_ready = any(_pair_ready(cfg, p) for p in pairs)

        probe_age = 0 if fresh else PROBE_TTL

        # Run probes in parallel for snappy UI
        with ThreadPoolExecutor(max_workers=4) as ex:
            futs = {
                ex.submit(_safe_probe_detail, _probe_plex_detail,  cfg, probe_age):   "PLEX",
                ex.submit(_safe_probe_detail, _probe_simkl_detail, cfg, probe_age):   "SIMKL",
                ex.submit(_safe_probe_detail, _probe_trakt_detail, cfg, probe_age):   "TRAKT",
                ex.submit(_safe_probe_detail, _probe_jellyfin_detail, cfg, probe_age):"JELLYFIN",
            }
            results: Dict[str, Tuple[bool, str]] = {}
            for f in as_completed(futs):
                name = futs[f]
                try:
                    results[name] = f.result()
                except Exception as e:
                    results[name] = (False, f"probe failed: {e}")

        plex_ok, plex_reason     = results["PLEX"]
        simkl_ok, simkl_reason   = results["SIMKL"]
        trakt_ok, trakt_reason   = results["TRAKT"]
        jelly_ok, jelly_reason   = results["JELLYFIN"]

        debug = bool(cfg.get("runtime", {}).get("debug"))
        info_plex  = _safe_userinfo(plex_user_info,  cfg, max_age_sec=USERINFO_TTL) if plex_ok  else {}
        info_trakt = _safe_userinfo(trakt_user_info, cfg, max_age_sec=USERINFO_TTL) if trakt_ok else {}

        data = {
            "plex_connected":     plex_ok,
            "simkl_connected":    simkl_ok,
            "trakt_connected":    trakt_ok,
            "jellyfin_connected": jelly_ok,
            "debug":              debug,
            "can_run":            bool(any_pair_ready),
            "ts":                 int(now),
            "providers": {
                "PLEX": {
                    "connected": plex_ok,
                    **({} if plex_ok else {"reason": plex_reason}),
                    **({} if not info_plex else {"plexpass": bool(info_plex.get("plexpass")),
                                                 "subscription": info_plex.get("subscription") or {}})
                },
                "SIMKL": {"connected": simkl_ok, **({} if simkl_ok else {"reason": simkl_reason})},
                "TRAKT": {"connected": trakt_ok, **({} if trakt_ok else {"reason": trakt_reason}),
                          **({} if not info_trakt else {"vip": bool(info_trakt.get("vip")),
                                                        "vip_type": info_trakt.get("vip_type")})},
                "JELLYFIN": {"connected": jelly_ok, **({} if jelly_ok else {"reason": jelly_reason})},
            },
        }
        STATUS_CACHE["ts"] = now
        STATUS_CACHE["data"] = data
        return JSONResponse(data, headers={"Cache-Control": "no-store"})

    @app.post("/api/debug/clear_probe_cache", tags=["Probes"])
    def clear_probe_cache():
        for k in list(PROBE_CACHE.keys()):
            PROBE_CACHE[k] = (0.0, False)
        for k in list(PROBE_DETAIL_CACHE.keys()):
            PROBE_DETAIL_CACHE[k] = (0.0, False, "")
        STATUS_CACHE["ts"] = 0.0
        STATUS_CACHE["data"] = None
        for k in list(_USERINFO_CACHE.keys()):
            _USERINFO_CACHE[k] = (0.0, {})
        return {"ok": True}

    app.state.PROBE_CACHE = PROBE_CACHE
    app.state.PROBE_DETAIL_CACHE = PROBE_DETAIL_CACHE
    app.state.USERINFO_CACHE = _USERINFO_CACHE

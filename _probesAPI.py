# _probesAPI.py
from __future__ import annotations
from typing import Any, Dict, Tuple, Optional
import json, time, re, urllib.request, urllib.error

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

# Optional plexapi for richer Plex user info
try:
    from plexapi.myplex import MyPlexAccount
    HAVE_PLEXAPI = True
except Exception:
    HAVE_PLEXAPI = False

# --- TTLs / caches (module-scope) ---
STATUS_CACHE: Dict[str, Any] = {"ts": 0.0, "data": None}
STATUS_TTL = 3600  # /api/status result cache (seconds)
PROBE_TTL  = 30    # connectivity probe max age (seconds)
USERINFO_TTL = 600 # Plex/Trakt user-capability cache (seconds)

PROBE_CACHE: Dict[str, Tuple[float, bool]] = {
    "plex": (0.0, False),
    "simkl": (0.0, False),
    "trakt": (0.0, False),
    "jellyfin": (0.0, False),
}

_USERINFO_CACHE: Dict[str, Tuple[float, dict]] = {
    "plex":  (0.0, {}),
    "trakt": (0.0, {}),
}

# --- tiny HTTP helpers (keep probes self-contained) ---
def _http_get(url: str, headers: Dict[str, str], timeout: int = 8) -> Tuple[int, bytes]:
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

# --- connectivity probes ---
def probe_plex(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok
    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        PROBE_CACHE["plex"] = (now, False)
        return False
    headers = {
        "X-Plex-Token": token,
        "X-Plex-Client-Identifier": "crosswatch",
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": "1.0",
        "Accept": "application/xml",
        "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://plex.tv/users/account", headers=headers, timeout=8)
    ok = (code == 200)
    PROBE_CACHE["plex"] = (now, ok)
    return ok

def probe_simkl(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["simkl"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok
    sk = (cfg.get("simkl") or cfg.get("SIMKL") or {})
    cid = (sk.get("client_id") or "").strip()
    tok = (sk.get("access_token") or sk.get("token") or "").strip()
    if not cid or not tok:
        PROBE_CACHE["simkl"] = (now, False)
        return False
    headers = {
        "Authorization": f"Bearer {tok}",
        "simkl-api-key": cid,
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers, timeout=8)
    ok = (code == 200)
    PROBE_CACHE["simkl"] = (now, ok)
    return ok

def probe_trakt(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok
    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        PROBE_CACHE["trakt"] = (now, False)
        return False
    headers = {
        "Authorization": f"Bearer {tok}",
        "trakt-api-key": cid,
        "trakt-api-version": "2",
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
    }
    code, _ = _http_get("https://api.trakt.tv/users/settings", headers=headers, timeout=8)
    ok = (code == 200)
    PROBE_CACHE["trakt"] = (now, ok)
    return ok

def probe_jellyfin(cfg: Dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE.get("jellyfin", (0.0, False))
    now = time.time()
    if now - ts < max_age_sec:
        return ok
    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {})
    ok = bool((jf.get("server") or "").strip() and (jf.get("access_token") or jf.get("token") or "").strip())
    PROBE_CACHE["jellyfin"] = (now, ok)
    return ok

# --- user info (PlexPass/VIP badges) ---
def plex_user_info(cfg: Dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict:
    ts, info = _USERINFO_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info
    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        _USERINFO_CACHE["plex"] = (now, {})
        return {}
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
        headers = {
            "X-Plex-Token": token,
            "X-Plex-Client-Identifier": "crosswatch",
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
            "Accept": "application/json",
            "User-Agent": "CrossWatch/1.0",
        }
        code, body = _http_get("https://plex.tv/api/v2/user", headers=headers, timeout=8)
        if code == 200:
            j = _json_loads(body)
            sub = (j.get("subscription") or {})
            plexpass = bool(sub.get("active") or j.get("hasPlexPass"))
            plan = sub.get("plan") or plan
            status = sub.get("status") or status
    out = {}
    if plexpass is not None:
        out["plexpass"] = bool(plexpass)
        out["subscription"] = {"plan": plan, "status": status}
    _USERINFO_CACHE["plex"] = (now, out)
    return out

def trakt_user_info(cfg: Dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict:
    ts, info = _USERINFO_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info
    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {})
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        _USERINFO_CACHE["trakt"] = (now, {})
        return {}
    headers = {
        "Authorization": f"Bearer {tok}",
        "trakt-api-key": cid,
        "trakt-api-version": "2",
        "Accept": "application/json",
        "User-Agent": "CrossWatch/1.0",
    }
    code, body = _http_get("https://api.trakt.tv/users/settings", headers=headers, timeout=8)
    out = {}
    if code == 200:
        j = _json_loads(body)
        u = j.get("user") or {}
        vip = bool(u.get("vip") or u.get("vip_og") or u.get("vip_ep"))
        vip_type = "vip"
        if u.get("vip_og"): vip_type = "vip_og"
        if u.get("vip_ep"): vip_type = "vip_ep"
        out = {"vip": vip, "vip_type": vip_type}
    _USERINFO_CACHE["trakt"] = (now, out)
    return out

# --- helpers used by /api/status ---
def _prov_configured(cfg: dict, name: str) -> bool:
    n = (name or "").strip().lower()
    if n == "plex":     return bool((cfg.get("plex") or {}).get("account_token"))
    if n == "trakt":    return bool((cfg.get("trakt") or {}).get("access_token"))
    if n == "simkl":    return bool((cfg.get("simkl") or {}).get("access_token"))
    if n == "jellyfin":
        jf = cfg.get("jellyfin") or {}
        return bool((jf.get("server") or "").strip() and (jf.get("access_token") or "").strip())
    return False

def _pair_ready(cfg: dict, pair: dict) -> bool:
    if not isinstance(pair, dict): return False
    enabled = pair.get("enabled", True) is not False
    def _name(x):
        if isinstance(x, str): return x
        if isinstance(x, dict): return x.get("provider") or x.get("name") or x.get("id") or x.get("type") or ""
        return ""
    a = _name(pair.get("source") or pair.get("a") or pair.get("src") or pair.get("from"))
    b = _name(pair.get("target") or pair.get("b") or pair.get("dst") or pair.get("to"))
    return bool(enabled and _prov_configured(cfg, a) and _prov_configured(cfg, b))

def _safe_probe(fn, cfg, max_age_sec=0) -> bool:
    try:
        return bool(fn(cfg, max_age_sec=max_age_sec))
    except Exception as e:
        print(f"[status] probe {getattr(fn, '__name__', 'fn')} failed: {e}")
        return False

def _safe_userinfo(fn, cfg, max_age_sec=0) -> dict:
    try:
        return fn(cfg, max_age_sec=max_age_sec) or {}
    except Exception as e:
        print(f"[status] userinfo {getattr(fn, '__name__', 'fn')} failed: {e}")
        return {}

def connected_status(cfg: Dict[str, Any]) -> Tuple[bool, bool, bool, bool]:
    return probe_plex(cfg), probe_simkl(cfg), probe_trakt(cfg), bool(cfg.get("runtime", {}).get("debug"))

# --- FastAPI wiring ---
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
        plex_ok  = _safe_probe(probe_plex,  cfg, max_age_sec=probe_age)
        simkl_ok = _safe_probe(probe_simkl, cfg, max_age_sec=probe_age)
        trakt_ok = _safe_probe(probe_trakt, cfg, max_age_sec=probe_age)
        jf_cfg = (cfg.get("jellyfin") or {})
        jelly_ok = bool((jf_cfg.get("server") or "").strip() and (jf_cfg.get("access_token") or "").strip())

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
                    **({} if not info_plex else {
                        "plexpass": bool(info_plex.get("plexpass")),
                        "subscription": info_plex.get("subscription") or {}
                    })
                },
                "SIMKL": {"connected": simkl_ok},
                "TRAKT": {
                    "connected": trakt_ok,
                    **({} if not info_trakt else {
                        "vip": bool(info_trakt.get("vip")),
                        "vip_type": info_trakt.get("vip_type")
                    })
                },
                "JELLYFIN": {"connected": jelly_ok},
            },
        }
        STATUS_CACHE["ts"] = now
        STATUS_CACHE["data"] = data
        return JSONResponse(data, headers={"Cache-Control": "no-store"})

    @app.post("/api/debug/clear_probe_cache", tags=["Probes"])
    def clear_probe_cache():
        for k in list(PROBE_CACHE.keys()):
            PROBE_CACHE[k] = (0.0, False)
        STATUS_CACHE["ts"] = 0.0
        STATUS_CACHE["data"] = None
        for k in list(_USERINFO_CACHE.keys()):
            _USERINFO_CACHE[k] = (0.0, {})
        return {"ok": True}

    app.state.PROBE_CACHE = PROBE_CACHE
    app.state.USERINFO_CACHE = _USERINFO_CACHE

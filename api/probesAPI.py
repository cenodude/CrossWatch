# /api/probesAPI.py
# CrossWatch - Probes API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import hashlib
import os
import threading
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Mapping

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from cw_platform.config_base import load_config as _load_config

from cw_platform.provider_instances import get_provider_block, list_instance_ids, normalize_instance_id, provider_key

try:
    from providers.auth._auth_TRAKT import PROVIDER as TRAKT_AUTH_PROVIDER
except Exception:
    TRAKT_AUTH_PROVIDER = None

try:
    from plexapi.myplex import MyPlexAccount
    HAVE_PLEXAPI = True
except Exception:
    HAVE_PLEXAPI = False

# env
HTTP_TIMEOUT = int(os.environ.get("CW_PROBE_HTTP_TIMEOUT", "3"))
STATUS_TTL = int(os.environ.get("CW_STATUS_TTL", "60"))
PROBE_TTL = int(os.environ.get("CW_PROBE_TTL", "15"))
USERINFO_TTL = int(os.environ.get("CW_USERINFO_TTL", "600"))
PROVIDERS: tuple[str, ...] = (
    "plex",
    "simkl",
    "trakt",
    "anilist",
    "jellyfin",
    "emby",
    "tmdb",
    "tmdb_sync",
    "mdblist",
    "tautulli",
)

# Caches
STATUS_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
STATUS_LOCK = threading.Lock()
PROBE_CACHE: dict[str, tuple[float, bool]] = {k: (0.0, False) for k in PROVIDERS}

# Keyed by per-credential probe key 
PROBE_DETAIL_CACHE: dict[str, tuple[float, bool, str]] = {}
_USERINFO_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}

_CACHE_LOCK = threading.Lock()
_BUST_SEEN: set[str] = set()

UA: dict[str, str] = {
    "Accept": "application/json",
    "User-Agent": "CrossWatch/1.0",
}

PROBE_CFG_KEY: dict[str, str] = {
    "PLEX": "plex",
    "SIMKL": "simkl",
    "TRAKT": "trakt",
    "ANILIST": "anilist",
    "JELLYFIN": "jellyfin",
    "EMBY": "emby",
    "TMDB": "tmdb_sync",
    "TMDB_SYNC": "tmdb_sync",
    "MDBLIST": "mdblist",
    "TAUTULLI": "tautulli",
}

_FALLBACK_KEYS: dict[str, tuple[str, ...]] = {
    "simkl": ("client_id", "client_secret"),
    "trakt": ("client_id", "client_secret"),
}


def _h(v: str) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    return hashlib.sha1(s.encode("utf-8")).hexdigest()[:10]


def _norm_url(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return ""
    while s.endswith("/"):
        s = s[:-1]
    return s


def _cfg_key(provider: Any) -> str:
    code = str(provider or "").upper().strip()
    if not code:
        return ""
    return PROBE_CFG_KEY.get(code) or provider_key(code)


def _instance_block(cfg: Mapping[str, Any], cfg_key: str, instance_id: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    base_raw = cfg.get(cfg_key) if isinstance(cfg, Mapping) else None
    base = dict(base_raw or {}) if isinstance(base_raw, Mapping) else {}

    if inst == "default":
        return base

    sub = get_provider_block(cfg, cfg_key, inst)
    out = dict(sub or {}) if isinstance(sub, Mapping) else {}
    for k in _FALLBACK_KEYS.get(cfg_key, ()):
        if not str(out.get(k) or "").strip() and str(base.get(k) or "").strip():
            out[k] = base.get(k)
    return out


def _cfg_view_for(cfg: Mapping[str, Any], provider_code: str, instance_id: Any) -> dict[str, Any]:
    ck = _cfg_key(provider_code)
    if not ck:
        return dict(cfg or {})
    out = dict(cfg or {})
    out[ck] = _instance_block(cfg, ck, instance_id)
    return out


def _probe_key(provider_id: str, cfg: Mapping[str, Any]) -> str:
    p = str(provider_id or "").strip().lower()
    if not p:
        return "unknown|unconfigured"

    if p == "plex":
        token = str(((cfg.get("plex") or {}).get("account_token") or "")).strip()
        return f"plex|tok:{_h(token)}" if token else "plex|unconfigured"

    if p == "simkl":
        s = cfg.get("simkl") or {}
        cid = str((s.get("client_id") or "")).strip()
        tok = str((s.get("access_token") or "")).strip()
        return f"simkl|cid:{_h(cid)}|tok:{_h(tok)}" if (cid and tok) else "simkl|unconfigured"

    if p == "trakt":
        t = cfg.get("trakt") or {}
        cid = str((t.get("client_id") or "")).strip()
        tok = str((t.get("access_token") or t.get("token") or "")).strip()
        return f"trakt|cid:{_h(cid)}|tok:{_h(tok)}" if (cid and tok) else "trakt|unconfigured"

    if p == "anilist":
        a = cfg.get("anilist") or {}
        tok = str((a.get("access_token") or a.get("token") or "")).strip()
        return f"anilist|tok:{_h(tok)}" if tok else "anilist|unconfigured"

    if p == "tmdb_sync":
        t = cfg.get("tmdb_sync") or {}
        api_key = str((t.get("api_key") or "")).strip()
        sess = str((t.get("session_id") or "")).strip()
        return f"tmdb_sync|key:{_h(api_key)}|sess:{_h(sess)}" if (api_key and sess) else "tmdb_sync|unconfigured"

    if p == "mdblist":
        m = cfg.get("mdblist") or {}
        key = str((m.get("api_key") or m.get("key") or "")).strip()
        return f"mdblist|key:{_h(key)}" if key else "mdblist|unconfigured"

    if p == "tautulli":
        t = cfg.get("tautulli") or {}
        base = _norm_url(t.get("server_url"))
        key = str((t.get("api_key") or "")).strip()
        return f"tautulli|srv:{_h(base)}|key:{_h(key)}" if (base and key) else "tautulli|unconfigured"

    if p == "jellyfin":
        jf = cfg.get("jellyfin") or {}
        server = _norm_url(jf.get("server"))
        tok = str((jf.get("access_token") or jf.get("token") or "")).strip()
        return f"jellyfin|srv:{_h(server)}|tok:{_h(tok)}" if (server and tok) else "jellyfin|unconfigured"

    if p == "emby":
        em = cfg.get("emby") or {}
        server = _norm_url(em.get("server"))
        tok = str((em.get("access_token") or em.get("token") or em.get("api_key") or "")).strip()
        return f"emby|srv:{_h(server)}|tok:{_h(tok)}" if (server and tok) else "emby|unconfigured"

    return f"{p}|unconfigured"


def _consume_bust(provider_id: str) -> float:
    p = str(provider_id or "").strip().lower()
    now = time.time()
    try:
        ts, _ = PROBE_CACHE.get(p, (0.0, False))
    except Exception:
        ts = 0.0

    if ts != 0.0 and p in _BUST_SEEN:
        return 0.0

    if ts == 0.0 and p in _BUST_SEEN:
        pass

    if ts == 0.0:
        with _CACHE_LOCK:
            pref = f"{p}|"
            for k in [k for k in PROBE_DETAIL_CACHE.keys() if str(k).startswith(pref)]:
                PROBE_DETAIL_CACHE.pop(k, None)
            for k in [k for k in _USERINFO_CACHE.keys() if str(k).startswith(pref)]:
                _USERINFO_CACHE.pop(k, None)
            PROBE_CACHE[p] = (now, False)
            _BUST_SEEN.add(p)
        return now

    _BUST_SEEN.add(p)
    return 0.0


# Helpers
def _http_get_with_headers(
    url: str,
    headers: dict[str, str],
    timeout: int = HTTP_TIMEOUT,
) -> tuple[int, bytes, dict[str, str]]:
    req = urllib.request.Request(url, headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            body = r.read()
            hdrs = {str(k).lower(): str(v) for k, v in (r.headers.items() if r.headers else [])}
            return r.getcode(), body, hdrs
    except urllib.error.HTTPError as e:
        body = e.read() if getattr(e, "fp", None) else b""
        hdrs = {str(k).lower(): str(v) for k, v in (e.headers.items() if e.headers else [])}
        return e.code, body, hdrs
    except Exception:
        return 0, b"", {}

def _http_get(url: str, headers: dict[str, str], timeout: int = HTTP_TIMEOUT) -> tuple[int, bytes]:
    code, body, _ = _http_get_with_headers(url, headers=headers, timeout=timeout)
    return code, body



def _http_post_with_headers(
    url: str,
    headers: dict[str, str],
    data: bytes,
    timeout: int = HTTP_TIMEOUT,
) -> tuple[int, bytes, dict[str, str]]:
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:  # noqa: S310
            body = r.read()
            hdrs = {str(k).lower(): str(v) for k, v in (r.headers.items() if r.headers else [])}
            return r.getcode(), body, hdrs
    except urllib.error.HTTPError as e:
        body = e.read() if getattr(e, "fp", None) else b""
        hdrs = {str(k).lower(): str(v) for k, v in (e.headers.items() if e.headers else [])}
        return e.code, body, hdrs
    except Exception:
        return 0, b"", {}


def _http_post(url: str, headers: dict[str, str], data: bytes, timeout: int = HTTP_TIMEOUT) -> tuple[int, bytes]:
    code, body, _ = _http_post_with_headers(url, headers=headers, data=data, timeout=timeout)
    return code, body

def _http_post_json(
    url: str,
    headers: dict[str, str],
    payload: Mapping[str, Any],
    timeout: int = HTTP_TIMEOUT,
) -> tuple[int, bytes, dict[str, str]]:
    h = dict(headers or {})
    h.setdefault("Content-Type", "application/json")
    data = json.dumps(dict(payload)).encode("utf-8")
    return _http_post_with_headers(url, headers=h, data=data, timeout=timeout)

def _json_loads(b: bytes) -> dict[str, Any]:
    try:
        return json.loads(b.decode("utf-8", errors="ignore"))
    except Exception:
        return {}

def _hdr_int(headers: Mapping[str, str], key: str) -> int | None:
    try:
        v = headers.get(key.lower()) or headers.get(key)
        if v is None:
            return None
        return int(str(v).strip())
    except Exception:
        return None


def _load_trakt_last_limit_error(
    path: str = "/config/.cw_state/trakt_last_limit_error.json",
) -> dict[str, Any]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}

def _trakt_limits_used(
    client_id: str,
    token: str,
    timeout: int = HTTP_TIMEOUT,
) -> dict[str, int]:
    out: dict[str, int] = {}
    if not client_id or not token:
        return out

    headers = {
        **UA,
        "Authorization": f"Bearer {token}",
        "trakt-api-key": client_id,
        "trakt-api-version": "2",
    }
    base = "https://api.trakt.tv"

    def _count_items(url: str) -> int:
        code, body = _http_get(url, headers=headers, timeout=timeout)
        if code != 200:
            return 0
        data = _json_loads(body) or []
        if isinstance(data, list):
            return len(data)
        return 0

    # Watchlist total
    wl_count = _count_items(f"{base}/sync/watchlist")
    if wl_count:
        out["watchlist"] = wl_count

    # Collection = movies and shows
    movies_count = _count_items(f"{base}/sync/collection/movies")
    shows_count = _count_items(f"{base}/sync/collection/shows")
    if movies_count or shows_count:
        out["collection"] = movies_count + shows_count

    return out

def _reason_http(code: int, provider: str) -> str:
    if code == 0:
        return f"{provider}: network error/timeout"
    if code == 401:
        return f"{provider}: unauthorized (token expired/revoked)"
    if code == 403:
        return f"{provider}: forbidden/invalid client id or scope"
    if code == 404:
        return f"{provider}: endpoint not found"
    if code == 412:
        return "Daily API limit reached."
    if 500 <= code < 600:
        return f"{provider}: service error ({code})"
    return f"{provider}: http {code}"

# Probes
def probe_plex(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
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
    code, _ = _http_get("https://plex.tv/users/account", headers=headers)
    ok = code == 200
    PROBE_CACHE["plex"] = (now, ok)
    return ok


def probe_simkl(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["simkl"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    sk = (cfg.get("simkl") or cfg.get("SIMKL") or {}) or {}
    cid = (sk.get("client_id") or "").strip()
    tok = (sk.get("access_token") or sk.get("token") or "").strip()
    if not cid or not tok:
        PROBE_CACHE["simkl"] = (now, False)
        return False

    headers = {**UA, "Authorization": f"Bearer {tok}", "simkl-api-key": cid}
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers)
    ok = code == 200
    PROBE_CACHE["simkl"] = (now, ok)
    return ok


def probe_trakt(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {}) or {}
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        PROBE_CACHE["trakt"] = (now, False)
        return False

    headers = {
        **UA,
        "Authorization": f"Bearer {tok}",
        "trakt-api-key": cid,
        "trakt-api-version": "2",
    }
    code, _ = _http_get("https://api.trakt.tv/users/settings", headers=headers)
    ok = code == 200
    PROBE_CACHE["trakt"] = (now, ok)
    return ok



def probe_anilist(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["anilist"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    an = (cfg.get("anilist") or cfg.get("ANILIST") or {}) or {}
    auth_an = (cfg.get("auth") or {}).get("anilist") or (cfg.get("auth") or {}).get("ANILIST") or {}
    tok = str(
        an.get("access_token")
        or an.get("token")
        or (an.get("oauth") or {}).get("access_token")
        or (auth_an.get("access_token") if isinstance(auth_an, dict) else "")
        or (auth_an.get("token") if isinstance(auth_an, dict) else "")
        or ((auth_an.get("oauth") or {}).get("access_token") if isinstance(auth_an, dict) else "")
        or ""
    ).strip()

    if not tok:
        PROBE_CACHE["anilist"] = (now, False)
        return False

    headers = {**UA, "Authorization": f"Bearer {tok}"}
    code, body, _ = _http_post_json(
        "https://graphql.anilist.co",
        headers=headers,
        payload={"query": "query { Viewer { id } }"},
        timeout=HTTP_TIMEOUT,
    )
    j = _json_loads(body) or {}
    data = j.get("data") if isinstance(j, dict) else None
    viewer = (data or {}).get("Viewer") if isinstance(data, dict) else None
    ok = code == 200 and isinstance(viewer, dict) and bool(viewer.get("id"))
    PROBE_CACHE["anilist"] = (now, ok)
    return ok


def probe_mdblist(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["mdblist"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    info = mdblist_user_info(cfg, max_age_sec=max_age_sec)
    ok = bool(info)
    PROBE_CACHE["mdblist"] = (now, ok)
    return ok


def probe_jellyfin(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["jellyfin"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {}) or {}
    ok = bool(
        (jf.get("server") or "").strip()
        and (jf.get("access_token") or jf.get("token") or "").strip()
    )
    PROBE_CACHE["jellyfin"] = (now, ok)
    return ok


def probe_emby(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> bool:
    ts, ok = PROBE_CACHE["emby"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok

    em = (cfg.get("emby") or cfg.get("EMBY") or {}) or {}
    ok = bool(
        (em.get("server") or "").strip()
        and (em.get("access_token") or em.get("token") or em.get("api_key") or "").strip()
    )
    PROBE_CACHE["emby"] = (now, ok)
    return ok


# Detailed probes
def _probe_plex_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("plex", cfg)
    bust_ts = _consume_bust("plex")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    token = str(((cfg.get("plex") or {}).get("account_token") or "")).strip()
    if not token:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "not configured")
        return False, "not configured"

    url = "https://plex.tv/api/v2/user"
    headers = {
        **UA,
        "X-Plex-Token": token,
        "X-Plex-Client-Identifier": "crosswatch",
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Version": "1.0",
    }
    code, _ = _http_get(url, headers=headers)
    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Plex")
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_simkl_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("simkl", cfg)
    bust_ts = _consume_bust("simkl")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    s: Mapping[str, Any] = (cfg.get("simkl") or {}) if isinstance(cfg.get("simkl"), Mapping) else {}
    cid = str((s.get("client_id") or "")).strip()
    tok = str((s.get("access_token") or "")).strip()
    if not cid:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "SIMKL: missing client_id")
        return False, "SIMKL: missing client_id"
    if not tok:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "SIMKL: missing access token")
        return False, "SIMKL: missing access token"

    url = "https://api.simkl.com/users/settings"
    headers = {**UA, "Authorization": f"Bearer {tok}", "simkl-api-key": cid}
    code, _ = _http_get(url, headers=headers, timeout=HTTP_TIMEOUT)

    ok = code == 200
    rsn = "" if ok else _reason_http(code, "SIMKL")
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_trakt_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("trakt", cfg)
    bust_ts = _consume_bust("trakt")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    auth_tr: dict[str, Any] = {}
    try:
        if isinstance(cfg.get("auth"), dict):
            auth_tr = (cfg.get("auth") or {}).get("trakt") or {}
    except Exception:
        auth_tr = {}

    t = cfg.get("trakt") or {}
    cid = str((t.get("client_id") or auth_tr.get("client_id") or "")).strip()
    tok = str((t.get("access_token") or t.get("token") or auth_tr.get("access_token") or "")).strip()
    if not cid:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "TRAKT: missing client_id")
        return False, "TRAKT: missing client_id"
    if not tok:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "TRAKT: missing access token")
        return False, "TRAKT: missing access token"

    url = "https://api.trakt.tv/users/settings"
    headers = {**UA, "Content-Type": "application/json", "trakt-api-version": "2", "trakt-api-key": cid, "Authorization": f"Bearer {tok}"}
    code, _ = _http_get(url, headers=headers, timeout=HTTP_TIMEOUT)

    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Trakt")
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_anilist_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("anilist", cfg)
    bust_ts = _consume_bust("anilist")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    a = cfg.get("anilist") or {}
    tok = str((a.get("access_token") or a.get("token") or "")).strip()
    if not tok:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "AniList: missing access token")
        return False, "AniList: missing access token"

    url = "https://graphql.anilist.co"
    q = {"query": "query { Viewer { id name } }"}
    payload = json.dumps(q).encode("utf-8")
    headers = {**UA, "Content-Type": "application/json", "Authorization": f"Bearer {tok}"}
    code, body = _http_post(url, headers=headers, data=payload, timeout=HTTP_TIMEOUT)

    ok = code == 200
    rsn = "" if ok else _reason_http(code, "AniList")

    if ok:
        j = _json_loads(body) or {}
        if isinstance(j, dict) and j.get("errors"):
            ok = False
            rsn = "AniList: auth error"

    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_tmdb_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("tmdb_sync", cfg)
    bust_ts = _consume_bust("tmdb_sync")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    t: Mapping[str, Any] = (cfg.get("tmdb_sync") or {}) if isinstance(cfg.get("tmdb_sync"), Mapping) else {}
    api_key = str((t.get("api_key") or "")).strip()
    sess = str((t.get("session_id") or "")).strip()
    if not api_key:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "TMDb: missing api_key")
        return False, "TMDb: missing api_key"
    if not sess:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "TMDb: missing session_id")
        return False, "TMDb: missing session_id"

    url = f"https://api.themoviedb.org/3/account?api_key={api_key}&session_id={sess}"
    code, _ = _http_get(url, headers=UA, timeout=HTTP_TIMEOUT)
    ok = code == 200
    rsn = "" if ok else _reason_http(code, "TMDb")
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_mdblist_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("mdblist", cfg)
    bust_ts = _consume_bust("mdblist")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    m: Mapping[str, Any] = (cfg.get("mdblist") or {}) if isinstance(cfg.get("mdblist"), Mapping) else {}
    api_key = str((m.get("api_key") or m.get("key") or "")).strip()
    if not api_key:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "MDBList: missing api_key")
        return False, "MDBList: missing api_key"

    url = f"https://mdblist.com/api/?apikey={api_key}&s=batman"
    code, body = _http_get(url, headers=UA, timeout=HTTP_TIMEOUT)

    if code != 200:
        rsn = _reason_http(code, "MDBList")
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, rsn)
        return False, rsn

    j = _json_loads(body) or {}
    ok = bool(isinstance(j, dict) and (j.get("title") or j.get("search")))
    rsn = "" if ok else "MDBList: invalid response"
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_tautulli_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("tautulli", cfg)
    bust_ts = _consume_bust("tautulli")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    t = cfg.get("tautulli") or {}
    base = str(t.get("server_url") or "").strip().rstrip("/")
    apikey = str(t.get("api_key") or "").strip()
    if not base or not apikey:
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, "not configured")
        return False, "not configured"

    url = f"{base}/api/v2?apikey={apikey}&cmd=get_server_info"
    code, body = _http_get(url, headers=UA, timeout=HTTP_TIMEOUT)
    if code != 200:
        rsn = f"HTTP {code}" if code else "HTTP 0"
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, rsn)
        return False, rsn

    j = _json_loads(body) or {}
    resp = j.get("response") if isinstance(j, dict) else None
    if isinstance(resp, dict) and str(resp.get("result") or "").lower() == "success":
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, True, "")
        return True, ""

    rsn = str(resp.get("message") or "invalid response") if isinstance(resp, dict) else "invalid response"
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, False, rsn)
    return False, rsn

def _probe_jellyfin_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("jellyfin", cfg)
    bust_ts = _consume_bust("jellyfin")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {}) or {}
    server = (jf.get("server") or "").strip()
    token = (jf.get("access_token") or jf.get("token") or "").strip()

    if not server:
        rsn = "Jellyfin: missing server URL"
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, rsn)
        return False, rsn
    if not token:
        rsn = "Jellyfin: missing access token"
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, rsn)
        return False, rsn

    url = f"{server.rstrip('/')}/System/Info/Public"
    code, _ = _http_get(url, headers={**UA}, timeout=HTTP_TIMEOUT)
    if code == 404:
        url2 = f"{server.rstrip('/')}/System/Info"
        code, _ = _http_get(url2, headers={**UA, "X-Emby-Token": token}, timeout=HTTP_TIMEOUT)

    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Jellyfin")
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def _probe_emby_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    key = _probe_key("emby", cfg)
    bust_ts = _consume_bust("emby")
    now = time.time()
    cached = PROBE_DETAIL_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts):
        return cached[1], cached[2]

    em = (cfg.get("emby") or cfg.get("EMBY") or {}) or {}
    server = (em.get("server") or "").strip()
    token = (em.get("access_token") or em.get("token") or em.get("api_key") or "").strip()
    if not server:
        rsn = "Emby: missing server URL"
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, rsn)
        return False, rsn
    if not token:
        rsn = "Emby: missing access token"
        with _CACHE_LOCK:
            PROBE_DETAIL_CACHE[key] = (now, False, rsn)
        return False, rsn

    url = f"{server.rstrip('/')}/System/Info"
    headers = {**UA, "X-Emby-Token": token}
    code, _ = _http_get(url, headers=headers, timeout=HTTP_TIMEOUT)
    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Emby")
    with _CACHE_LOCK:
        PROBE_DETAIL_CACHE[key] = (now, ok, rsn)
    return ok, rsn

def plex_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    key = _probe_key("plex", cfg)
    bust_ts = _consume_bust("plex")
    now = time.time()
    cached = _USERINFO_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts) and isinstance(cached[1], dict):
        return cached[1]

    token = str(((cfg.get("plex") or {}).get("account_token") or "")).strip()
    if not token:
        with _CACHE_LOCK:
            _USERINFO_CACHE[key] = (now, {})
        return {}

    plexpass: bool | None = None
    plan: str | None = None
    status: str | None = None

    if HAVE_PLEXAPI:
        try:
            acc = MyPlexAccount(token=token)  # type: ignore[call-arg]
            plexpass = bool(getattr(acc, "subscriptionActive", None) or getattr(acc, "hasPlexPass", None))
            plan = getattr(acc, "subscriptionPlan", None) or None
            status = getattr(acc, "subscriptionStatus", None) or None
        except Exception:
            pass

    if plexpass is None:
        headers = {
            **UA,
            "X-Plex-Token": token,
            "X-Plex-Client-Identifier": "crosswatch",
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
        }
        code, body = _http_get("https://plex.tv/api/v2/user", headers=headers)
        if code == 200:
            j = _json_loads(body)
            sub = j.get("subscription") or {}
            plexpass = bool(sub.get("active") or j.get("hasPlexPass"))
            plan = sub.get("plan") or plan
            status = sub.get("status") or status

    out: dict[str, Any] = {}
    if plexpass is not None:
        out["plexpass"] = bool(plexpass)
        out["subscription"] = {"plan": plan, "status": status}

    with _CACHE_LOCK:
        _USERINFO_CACHE[key] = (now, out)
    return out

def mdblist_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    key = _probe_key("mdblist", cfg)
    bust_ts = _consume_bust("mdblist")
    now = time.time()
    cached = _USERINFO_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts) and isinstance(cached[1], dict):
        return cached[1]

    md = (cfg.get("mdblist") or cfg.get("MDBLIST") or {}) or {}
    api_key = str((md.get("api_key") or md.get("key") or "")).strip()
    if not api_key:
        with _CACHE_LOCK:
            _USERINFO_CACHE[key] = (now, {})
        return {}

    from urllib.parse import quote

    url = f"https://api.mdblist.com/user?apikey={quote(api_key)}"
    code, body = _http_get(url, headers=UA, timeout=6)

    out: dict[str, Any] = {}
    if code == 200:
        j = _json_loads(body) or {}

        def _to_int(v: Any) -> int:
            try:
                return int(v)
            except Exception:
                return 0

        limits = {"api_requests": _to_int(j.get("api_requests")), "api_requests_count": _to_int(j.get("api_requests_count"))}
        patron_status = j.get("patron_status") or None
        is_supporter = bool(j.get("is_supporter"))
        vip = is_supporter or (str(patron_status).lower() in ("active_patron", "patron", "supporter"))

        out = {
            "vip": vip,
            "vip_type": "patron" if vip else None,
            "patron_status": patron_status,
            "username": j.get("username"),
            "user_id": j.get("user_id"),
            "limits": limits,
        }

    with _CACHE_LOCK:
        _USERINFO_CACHE[key] = (now, out)
    return out

def trakt_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    key = _probe_key("trakt", cfg)
    bust_ts = _consume_bust("trakt")
    now = time.time()
    cached = _USERINFO_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts) and isinstance(cached[1], dict):
        return cached[1]

    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {}) or {}
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = str((tr.get("client_id") or auth_tr.get("client_id") or "")).strip()
    tok = str((auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "")).strip()
    if not cid or not tok:
        with _CACHE_LOCK:
            _USERINFO_CACHE[key] = (now, {})
        return {}

    headers = {**UA, "Authorization": f"Bearer {tok}", "trakt-api-key": cid, "trakt-api-version": "2"}
    code, body = _http_get("https://api.trakt.tv/users/settings", headers=headers)

    out: dict[str, Any] = {}
    if code == 200:
        j = _json_loads(body) or {}
        u = j.get("user") or {}

        vip = bool(u.get("vip") or u.get("vip_og") or u.get("vip_ep"))
        vip_type = "vip_og" if u.get("vip_og") else ("vip_ep" if u.get("vip_ep") else ("vip" if vip else ""))

        limits_raw = j.get("limits") or {}

        def _int_or_none(v: Any) -> int | None:
            try:
                return int(v)
            except Exception:
                return None

        used_counts = _trakt_limits_used(cid, tok)
        limits_out: dict[str, Any] = {}

        wl_raw = limits_raw.get("watchlist") or {}
        wl_limit = _int_or_none(wl_raw.get("item_count"))
        wl_used = used_counts.get("watchlist") if isinstance(used_counts.get("watchlist"), int) else None
        if wl_limit is not None or wl_used is not None:
            limits_out["watchlist"] = {"item_count": wl_limit if wl_limit is not None else int(wl_used or 0), "used": int(wl_used or 0)}

        coll_raw = limits_raw.get("collection") or {}
        coll_limit = _int_or_none(coll_raw.get("item_count"))
        coll_used = used_counts.get("collection") if isinstance(used_counts.get("collection"), int) else None
        if coll_limit is not None or coll_used is not None:
            limits_out["collection"] = {"item_count": coll_limit if coll_limit is not None else int(coll_used or 0), "used": int(coll_used or 0)}

        out = {"vip": vip, "vip_type": vip_type}
        if limits_out:
            out["limits"] = limits_out

        last_err = _load_trakt_last_limit_error()
        if isinstance(last_err, dict) and last_err.get("feature") and last_err.get("ts"):
            out["last_limit_error"] = {"feature": str(last_err.get("feature")), "ts": str(last_err.get("ts"))}

    with _CACHE_LOCK:
        _USERINFO_CACHE[key] = (now, out)
    return out

def emby_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    key = _probe_key("emby", cfg)
    bust_ts = _consume_bust("emby")
    now = time.time()
    cached = _USERINFO_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts) and isinstance(cached[1], dict):
        return cached[1]

    em = (cfg.get("emby") or cfg.get("EMBY") or {}) or {}
    server = str(em.get("server") or "").strip()
    token = str(em.get("access_token") or em.get("token") or em.get("api_key") or "").strip()
    if not server or not token:
        with _CACHE_LOCK:
            _USERINFO_CACHE[key] = (now, {})
        return {}

    url = f"{server.rstrip('/')}/System/Info"
    headers = {**UA, "X-Emby-Token": token}
    code, body = _http_get(url, headers=headers)

    out: dict[str, Any] = {}
    if code == 200:
        j = _json_loads(body) or {}
        cand = [
            "HasEmbyPremiere",
            "HasPremium",
            "HasSupporterMembership",
            "HasSupporterKey",
            "HasValidSupporterKey",
            "IsMBSupporter",
            "IsPremiere",
            "Premiere",
            "SupportsPremium",
        ]

        def _truthy(v: Any) -> bool:
            if isinstance(v, bool):
                return v
            if isinstance(v, (int, float)):
                return v != 0
            if isinstance(v, str):
                return v.strip().lower() not in ("", "0", "false", "no", "none", "null")
            return False

        prem = any(_truthy(j.get(k)) for k in cand)
        if not prem:
            for k, v in j.items():
                if isinstance(k, str) and "supporter" in k.lower() and _truthy(v):
                    prem = True
                    break

        out = {"premiere": bool(prem)}

    with _CACHE_LOCK:
        _USERINFO_CACHE[key] = (now, out)
    return out

def anilist_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    key = _probe_key("anilist", cfg)
    bust_ts = _consume_bust("anilist")
    now = time.time()
    cached = _USERINFO_CACHE.get(key)
    if cached and (now - cached[0]) < max_age_sec and (not bust_ts or cached[0] >= bust_ts) and isinstance(cached[1], dict):
        return cached[1]

    an = (cfg.get("anilist") or cfg.get("ANILIST") or {}) or {}
    auth_an = (cfg.get("auth") or {}).get("anilist") or (cfg.get("auth") or {}).get("ANILIST") or {}
    tok = str(
        an.get("access_token")
        or an.get("token")
        or (an.get("oauth") or {}).get("access_token")
        or (auth_an.get("access_token") if isinstance(auth_an, dict) else "")
        or (auth_an.get("token") if isinstance(auth_an, dict) else "")
        or ((auth_an.get("oauth") or {}).get("access_token") if isinstance(auth_an, dict) else "")
        or ""
    ).strip()

    if not tok:
        with _CACHE_LOCK:
            _USERINFO_CACHE[key] = (now, {})
        return {}

    headers = {**UA, "Authorization": f"Bearer {tok}"}
    code, body, _ = _http_post_json(
        "https://graphql.anilist.co",
        headers=headers,
        payload={"query": "query { Viewer { id name } }"},
        timeout=HTTP_TIMEOUT,
    )

    out: dict[str, Any] = {}
    if code == 200:
        j = _json_loads(body) or {}
        data = j.get("data") if isinstance(j, dict) else None
        viewer = (data or {}).get("Viewer") if isinstance(data, dict) else None
        if isinstance(viewer, dict) and viewer.get("id"):
            out = {"user": {"id": viewer.get("id"), "name": viewer.get("name")}}

    with _CACHE_LOCK:
        _USERINFO_CACHE[key] = (now, out)
    return out

def _prov_configured(cfg: dict[str, Any], name: str, instance_id: Any = "default") -> bool:
    n = str(name or "").strip().upper()

    # CrossWatch local/virtual provider
    if n in ("CROSSWATCH", "CW"):
        cw = cfg.get("crosswatch") or cfg.get("CrossWatch") or {}
        enabled = cw.get("enabled")
        return bool(enabled) if isinstance(enabled, bool) else True

    ck = _cfg_key(n)
    inst = normalize_instance_id(instance_id)

    # TMDb legacy key
    if n == "TMDB" and ck == "tmdb_sync" and not isinstance(cfg.get("tmdb_sync"), Mapping):
        legacy_raw = cfg.get("tmdb")
        legacy = dict(legacy_raw) if isinstance(legacy_raw, Mapping) else {}
        if legacy:
            return bool(str(legacy.get("api_key") or "").strip() and str(legacy.get("session_id") or "").strip())

    if not ck:
        return False

    blk = _instance_block(cfg, ck, inst)

    if ck == "plex":
        return bool(str(blk.get("account_token") or "").strip())

    if ck == "trakt":
        return bool(str(blk.get("access_token") or blk.get("token") or "").strip() and str(blk.get("client_id") or "").strip())

    if ck == "simkl":
        return bool(str(blk.get("access_token") or "").strip() and str(blk.get("client_id") or "").strip())

    if ck == "anilist":
        return bool(str(blk.get("access_token") or blk.get("token") or "").strip())

    if ck == "jellyfin":
        return bool(str(blk.get("server") or "").strip() and str(blk.get("access_token") or blk.get("token") or "").strip())

    if ck == "emby":
        return bool(str(blk.get("server") or "").strip() and str(blk.get("access_token") or blk.get("token") or blk.get("api_key") or "").strip())

    if ck == "mdblist":
        return bool(str(blk.get("api_key") or blk.get("key") or "").strip())

    if ck == "tmdb_sync":
        return bool(str(blk.get("api_key") or "").strip() and str(blk.get("session_id") or "").strip())

    if ck == "tautulli":
        return bool(str(blk.get("server_url") or "").strip() and str(blk.get("api_key") or "").strip())

    return False

def _pair_ready(cfg: dict[str, Any], pair: dict[str, Any]) -> bool:
    if not isinstance(pair, dict):
        return False
    if pair.get("enabled", True) is False:
        return False

    def _name(x: Any) -> str:
        if isinstance(x, str):
            return x
        if isinstance(x, dict):
            return x.get("provider") or x.get("name") or x.get("id") or x.get("type") or ""
        return ""

    a = _name(pair.get("source") or pair.get("a") or pair.get("src") or pair.get("from"))
    b = _name(pair.get("target") or pair.get("b") or pair.get("dst") or pair.get("to"))

    a_inst = normalize_instance_id(pair.get("source_instance") or pair.get("a_instance") or "default")
    b_inst = normalize_instance_id(pair.get("target_instance") or pair.get("b_instance") or "default")
    return bool(_prov_configured(cfg, a, a_inst) and _prov_configured(cfg, b, b_inst))

def _safe_probe_detail(
    fn: Callable[..., tuple[bool, str]],
    cfg: dict[str, Any],
    max_age_sec: int = 0,
) -> tuple[bool, str]:
    try:
        return fn(cfg, max_age_sec=max_age_sec)
    except Exception as e:
        return False, f"probe failed: {e}"

def _safe_userinfo(
    fn: Callable[..., dict[str, Any]],
    cfg: dict[str, Any],
    max_age_sec: int = 0,
) -> dict[str, Any]:
    try:
        return fn(cfg, max_age_sec=max_age_sec) or {}
    except Exception:
        return {}

# Connection status
def connected_status(cfg: dict[str, Any]) -> tuple[bool, bool, bool, bool, bool, bool, bool]:
    plex_ok, _ = _safe_probe_detail(_probe_plex_detail, cfg, max_age_sec=PROBE_TTL)
    simkl_ok, _ = _safe_probe_detail(_probe_simkl_detail, cfg, max_age_sec=PROBE_TTL)
    trakt_ok, _ = _safe_probe_detail(_probe_trakt_detail, cfg, max_age_sec=PROBE_TTL)
    jelly_ok, _ = _safe_probe_detail(_probe_jellyfin_detail, cfg, max_age_sec=PROBE_TTL)
    emby_ok, _ = _safe_probe_detail(_probe_emby_detail, cfg, max_age_sec=PROBE_TTL)
    mdbl_ok, _ = _safe_probe_detail(_probe_mdblist_detail, cfg, max_age_sec=PROBE_TTL)
    debug = bool((cfg.get("runtime") or {}).get("debug"))
    return plex_ok, simkl_ok, trakt_ok, jelly_ok, emby_ok, mdbl_ok, debug


# Mappings
DETAIL_PROBES: dict[str, Callable[..., tuple[bool, str]]] = {
    "PLEX": _probe_plex_detail,
    "SIMKL": _probe_simkl_detail,
    "TRAKT": _probe_trakt_detail,
    "ANILIST": _probe_anilist_detail,
    "JELLYFIN": _probe_jellyfin_detail,
    "EMBY": _probe_emby_detail,
    "TMDB": _probe_tmdb_detail,
    "MDBLIST": _probe_mdblist_detail,
    "TAUTULLI": _probe_tautulli_detail,
}
USERINFO_FNS: dict[str, Callable[..., dict[str, Any]]] = {
    "PLEX": plex_user_info,
    "TRAKT": trakt_user_info,
    "ANILIST": anilist_user_info,
    "EMBY": emby_user_info,
    "MDBLIST": mdblist_user_info,
}

# Registry API
def register_probes(app: FastAPI, load_config_fn: Callable[[], dict[str, Any]]) -> None:
    @app.get("/api/status", tags=["Probes"])
    def api_status(fresh: int = Query(0)) -> JSONResponse:
        now = time.time()
        cached = STATUS_CACHE["data"]
        age = (now - STATUS_CACHE["ts"]) if cached else 1e9
        if not fresh and cached and age < STATUS_TTL:
            return JSONResponse(cached, headers={"Cache-Control": "no-store"})

        with STATUS_LOCK:
            now = time.time()
            cached = STATUS_CACHE["data"]
            age = (now - STATUS_CACHE["ts"]) if cached else 1e9
            if not fresh and cached and age < STATUS_TTL:
                return JSONResponse(cached, headers={"Cache-Control": "no-store"})

            cfg = load_config_fn() or {}
            pairs = cfg.get("pairs") or []
            enabled_pairs = [p for p in pairs if isinstance(p, dict) and p.get("enabled", True) is not False]
            any_pair_ready = any(_pair_ready(cfg, p) for p in enabled_pairs)

            probe_age = 0 if fresh else PROBE_TTL
            user_age = USERINFO_TTL

            def _pair_targets() -> set[tuple[str, str]]:
                used: set[tuple[str, str]] = set()

                def _name(x: Any) -> str:
                    if isinstance(x, str):
                        return x
                    if isinstance(x, dict):
                        return x.get("provider") or x.get("name") or x.get("id") or x.get("type") or ""
                    return ""

                for p in enabled_pairs:
                    a = _name(p.get("source") or p.get("a") or p.get("src") or p.get("from")).upper().strip()
                    b = _name(p.get("target") or p.get("b") or p.get("dst") or p.get("to")).upper().strip()
                    if a in DETAIL_PROBES:
                        used.add((a, normalize_instance_id(p.get("source_instance") or "default")))
                    if b in DETAIL_PROBES:
                        used.add((b, normalize_instance_id(p.get("target_instance") or "default")))
                return used

            used_targets = _pair_targets()
            active_providers = {p for p, _ in used_targets}
            targets: set[tuple[str, str]] = set(used_targets)

            # Hide providers that are not part of any enabled pair.
            debug = bool((cfg.get("runtime") or {}).get("debug"))
            if not targets:
                data: dict[str, Any] = {
                    "plex_connected": False,
                    "simkl_connected": False,
                    "trakt_connected": False,
                    "anilist_connected": False,
                    "jellyfin_connected": False,
                    "emby_connected": False,
                    "tmdb_connected": False,
                    "mdblist_connected": False,
                    "tautulli_connected": False,
                    "debug": debug,
                    "can_run": bool(any_pair_ready),
                    "ts": int(now),
                    "providers": {},
                }
                STATUS_CACHE["ts"] = now
                STATUS_CACHE["data"] = data
                return JSONResponse(data, headers={"Cache-Control": "no-store"})

            targets: set[tuple[str, str]] = set(used_targets)

            def _pick_one_configured(prov: str) -> str | None:
                ck = _cfg_key(prov)
                ids: list[str] = []
                try:
                    ids = list_instance_ids(cfg, ck)
                except Exception:
                    ids = ["default"]
                for inst in ids or ["default"]:
                    view = _cfg_view_for(cfg, prov, inst)
                    if not _probe_key(_cfg_key(prov), view).endswith("|unconfigured"):
                        return normalize_instance_id(inst)
                return None

            for prov in DETAIL_PROBES.keys():
                if any(p == prov for p, _ in targets):
                    continue
                picked = _pick_one_configured(prov)
                if picked:
                    targets.add((prov, picked))
                    
            active_providers = {p for p, _ in targets}

            if not targets:
                targets = {(prov, "default") for prov in DETAIL_PROBES.keys()}

            jobs_by_key: dict[str, tuple[str, dict[str, Any], Callable[..., tuple[bool, str]]]] = {}
            refs: dict[tuple[str, str], str] = {}
            for prov, inst in sorted(targets):
                view = _cfg_view_for(cfg, prov, inst)
                pid = _cfg_key(prov)
                pkey = _probe_key(pid, view)
                refs[(prov, inst)] = pkey
                if pkey not in jobs_by_key:
                    jobs_by_key[pkey] = (prov, view, DETAIL_PROBES[prov])

            results_by_key: dict[str, tuple[bool, str]] = {}
            with ThreadPoolExecutor(max_workers=max(1, min(12, len(jobs_by_key)))) as ex:
                futs = {ex.submit(_safe_probe_detail, fn, view, probe_age): pkey for pkey, (prov, view, fn) in jobs_by_key.items()}
                for f in as_completed(futs):
                    pkey = futs[f]
                    try:
                        results_by_key[pkey] = f.result()
                    except Exception as e:
                        results_by_key[pkey] = (False, f"probe failed: {e}")

            # Per-provider aggregation
            per: dict[str, dict[str, tuple[bool, str, dict[str, Any]]]] = {}
            for (prov, inst), pkey in refs.items():
                ok, rsn = results_by_key.get(pkey, (False, ""))
                per.setdefault(prov, {})[inst] = (ok, rsn, _cfg_view_for(cfg, prov, inst))

            def _pick_rep(prov: str) -> tuple[bool, str, dict[str, Any]]:
                items = per.get(prov) or {}
                if not items:
                    return False, "not configured", dict(cfg)

                if "default" in items and items["default"][0]:
                    return items["default"]
                for inst, tup in items.items():
                    if tup[0]:
                        return tup
                if "default" in items:
                    return items["default"]
                return next(iter(items.values()))

            rep = {prov: _pick_rep(prov) for prov in active_providers}

            plex_ok, plex_reason, cfg_plex = rep.get("PLEX", (False, "", dict(cfg)))
            simkl_ok, simkl_reason, cfg_simkl = rep.get("SIMKL", (False, "", dict(cfg)))
            trakt_ok, trakt_reason, cfg_trakt = rep.get("TRAKT", (False, "", dict(cfg)))
            jelly_ok, jelly_reason, cfg_jelly = rep.get("JELLYFIN", (False, "", dict(cfg)))
            emby_ok, emby_reason, cfg_emby = rep.get("EMBY", (False, "", dict(cfg)))
            tmdb_ok, tmdb_reason, cfg_tmdb = rep.get("TMDB", (False, "", dict(cfg)))
            mdbl_ok, mdbl_reason, cfg_mdbl = rep.get("MDBLIST", (False, "", dict(cfg)))
            taut_ok, taut_reason, cfg_taut = rep.get("TAUTULLI", (False, "", dict(cfg)))
            anilist_ok, anilist_reason, cfg_anilist = rep.get("ANILIST", (False, "", dict(cfg)))

            debug = bool((cfg.get("runtime") or {}).get("debug"))

            info_plex = _safe_userinfo(plex_user_info, cfg_plex, max_age_sec=user_age) if plex_ok else {}
            info_trakt = _safe_userinfo(trakt_user_info, cfg_trakt, max_age_sec=user_age) if trakt_ok else {}
            info_anilist = _safe_userinfo(anilist_user_info, cfg_anilist, max_age_sec=user_age) if anilist_ok else {}
            info_emby = _safe_userinfo(emby_user_info, cfg_emby, max_age_sec=user_age) if emby_ok else {}
            info_mdbl = _safe_userinfo(mdblist_user_info, cfg_mdbl, max_age_sec=user_age) if mdbl_ok else {}

            trakt_block: dict[str, Any] = {"connected": trakt_ok}
            if not trakt_ok:
                trakt_block["reason"] = trakt_reason
            if info_trakt:
                trakt_block["vip"] = bool(info_trakt.get("vip"))
                trakt_block["vip_type"] = info_trakt.get("vip_type")

                limits_info = info_trakt.get("limits") or {}
                if isinstance(limits_info, dict) and limits_info:
                    watchlist = limits_info.get("watchlist") or {}
                    collection = limits_info.get("collection") or {}
                    if watchlist or collection:
                        trakt_block["limits"] = {}
                        if watchlist:
                            trakt_block["limits"]["watchlist"] = {"item_count": int((watchlist.get("item_count") or 0)), "used": int((watchlist.get("used") or 0))}
                        if collection:
                            trakt_block["limits"]["collection"] = {"item_count": int((collection.get("item_count") or 0)), "used": int((collection.get("used") or 0))}

                last_err = info_trakt.get("last_limit_error")
                if isinstance(last_err, dict) and last_err.get("feature") and last_err.get("ts"):
                    trakt_block["last_limit_error"] = {"feature": str(last_err.get("feature")), "ts": str(last_err.get("ts"))}

            providers_out: dict[str, Any] = {}
            if "PLEX" in active_providers:
                providers_out["PLEX"] = {
                    "connected": plex_ok,
                    **({} if plex_ok else {"reason": plex_reason}),
                    **({} if not info_plex else {"plexpass": bool(info_plex.get("plexpass")), "subscription": info_plex.get("subscription") or {}}),
                }
            if "SIMKL" in active_providers:
                providers_out["SIMKL"] = {"connected": simkl_ok, **({} if simkl_ok else {"reason": simkl_reason})}
            if "ANILIST" in active_providers:
                providers_out["ANILIST"] = {
                    "connected": anilist_ok,
                    **({} if anilist_ok else {"reason": anilist_reason}),
                    **({} if not info_anilist else {"user": (info_anilist.get("user") or {})}),
                }
            if "TRAKT" in active_providers:
                providers_out["TRAKT"] = trakt_block
            if "JELLYFIN" in active_providers:
                providers_out["JELLYFIN"] = {"connected": jelly_ok, **({} if jelly_ok else {"reason": jelly_reason})}
            if "EMBY" in active_providers:
                providers_out["EMBY"] = {
                    "connected": emby_ok,
                    **({} if emby_ok else {"reason": emby_reason}),
                    **({} if not info_emby else {"premiere": bool(info_emby.get("premiere"))}),
                }
            if "TMDB" in active_providers:
                providers_out["TMDB"] = {"connected": tmdb_ok, **({} if tmdb_ok else {"reason": tmdb_reason})}
            if "TAUTULLI" in active_providers:
                providers_out["TAUTULLI"] = {"connected": taut_ok, **({} if taut_ok else {"reason": taut_reason})}
            if "MDBLIST" in active_providers:
                providers_out["MDBLIST"] = {
                    "connected": mdbl_ok,
                    **({} if mdbl_ok else {"reason": mdbl_reason}),
                    **(
                        {}
                        if not info_mdbl
                        else {
                            "vip": bool(info_mdbl.get("vip")),
                            "vip_type": info_mdbl.get("vip_type"),
                            "patron_status": info_mdbl.get("patron_status"),
                            "limits": {
                                "api_requests": int(((info_mdbl.get("limits") or {}).get("api_requests") or 0)),
                                "api_requests_count": int(((info_mdbl.get("limits") or {}).get("api_requests_count") or 0)),
                            },
                        }
                    ),
                }


            data: dict[str, Any] = {
                "plex_connected": plex_ok,
                "simkl_connected": simkl_ok,
                "trakt_connected": trakt_ok,
                "anilist_connected": anilist_ok,
                "jellyfin_connected": jelly_ok,
                "emby_connected": emby_ok,
                "tmdb_connected": tmdb_ok,
                "mdblist_connected": mdbl_ok,
                "tautulli_connected": taut_ok,
                "debug": debug,
                "can_run": bool(any_pair_ready),
                "ts": int(now),
                "providers": providers_out,
            }

            STATUS_CACHE["ts"] = now
            STATUS_CACHE["data"] = data
            return JSONResponse(data, headers={"Cache-Control": "no-store"})

    @app.post("/api/debug/clear_probe_cache", tags=["Probes"])
    def clear_probe_cache() -> dict[str, Any]:
        with STATUS_LOCK:
            for k in list(PROBE_CACHE.keys()):
                PROBE_CACHE[k] = (0.0, False)
            with _CACHE_LOCK:
                PROBE_DETAIL_CACHE.clear()
                _USERINFO_CACHE.clear()
                _BUST_SEEN.clear()
            STATUS_CACHE["ts"] = 0.0
            STATUS_CACHE["data"] = None
        return {"ok": True}

    app.state.PROBE_CACHE = PROBE_CACHE
    app.state.PROBE_DETAIL_CACHE = PROBE_DETAIL_CACHE
    app.state.USERINFO_CACHE = _USERINFO_CACHE
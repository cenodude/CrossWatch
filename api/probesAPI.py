# _probesAPI.py
# CrossWatch - Probes API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
import urllib.error
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any, Callable, Mapping

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

from cw_platform.config_base import load_config as _load_config

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
PROVIDERS: tuple[str, ...] = ("plex", "simkl", "trakt", "jellyfin", "emby", "mdblist")

# Caches
STATUS_CACHE: dict[str, Any] = {"ts": 0.0, "data": None}
PROBE_CACHE: dict[str, tuple[float, bool]] = {k: (0.0, False) for k in PROVIDERS}
PROBE_DETAIL_CACHE: dict[str, tuple[float, bool, str]] = {
    k: (0.0, False, "") for k in PROVIDERS
}
_USERINFO_CACHE: dict[str, tuple[float, dict[str, Any]]] = {
    k: (0.0, {}) for k in ("plex", "trakt", "emby", "mdblist")
}

UA: dict[str, str] = {
    "Accept": "application/json",
    "User-Agent": "CrossWatch/1.0",
}

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

    endpoints: dict[str, list[str]] = {
        "watchlist": [f"{base}/sync/watchlist?page=1&limit=1"],
        "collection": [
            f"{base}/sync/collection/movies?page=1&limit=1",
            f"{base}/sync/collection/shows?page=1&limit=1",
        ],
    }

    for feature, urls in endpoints.items():
        total = 0
        have = False
        for url in urls:
            code, _body, hdrs = _http_get_with_headers(url, headers=headers, timeout=timeout)
            if code != 200:
                continue
            cnt = _hdr_int(hdrs, "x-pagination-item-count")
            if cnt is None:
                continue
            total += cnt
            have = True
        if have:
            out[feature] = total

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
    ts, ok, rsn = PROBE_DETAIL_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok, rsn

    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        rsn = "Plex: missing account_token"
        PROBE_DETAIL_CACHE["plex"] = (now, False, rsn)
        return False, rsn

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
    rsn = "" if ok else _reason_http(code, "Plex")
    PROBE_DETAIL_CACHE["plex"] = (now, ok, rsn)
    return ok, rsn


def _probe_simkl_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["simkl"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok, rsn

    sk = (cfg.get("simkl") or cfg.get("SIMKL") or {}) or {}
    cid = (sk.get("client_id") or "").strip()
    tok = (sk.get("access_token") or sk.get("token") or "").strip()
    if not cid or not tok:
        rsn = "SIMKL: missing token/client id"
        PROBE_DETAIL_CACHE["simkl"] = (now, False, rsn)
        return False, rsn

    headers = {**UA, "Authorization": f"Bearer {tok}", "simkl-api-key": cid}
    code, _ = _http_get("https://api.simkl.com/users/settings", headers=headers)
    ok = code == 200
    rsn = "" if ok else _reason_http(code, "SIMKL")
    PROBE_DETAIL_CACHE["simkl"] = (now, ok, rsn)
    return ok, rsn


def _probe_trakt_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok, rsn

    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {}) or {}
    cid = (tr.get("client_id") or "").strip()
    tok = (tr.get("access_token") or tr.get("token") or "").strip()

    if not cid or not tok:
        rsn = "Trakt: missing token/client id"
        PROBE_DETAIL_CACHE["trakt"] = (now, False, rsn)
        return False, rsn

    def _call(token: str) -> int:
        headers = {
            **UA,
            "Authorization": f"Bearer {token}",
            "trakt-api-key": cid,
            "trakt-api-version": "2",
        }
        code, _ = _http_get("https://api.trakt.tv/users/settings", headers=headers)
        return code

    code = _call(tok)

    if code in (401, 403) and TRAKT_AUTH_PROVIDER is not None:
        try:
            res = TRAKT_AUTH_PROVIDER.refresh(cfg)  # type: ignore[union-attr]
        except Exception as e:
            res = {"ok": False, "error": f"exception_in_refresh:{e}"}

        if isinstance(res, dict) and res.get("ok"):
            try:
                cfg2 = _load_config()
            except Exception:
                cfg2 = cfg

            tr2 = (cfg2.get("trakt") or cfg2.get("TRAKT") or {}) or {}
            new_tok = (tr2.get("access_token") or tr2.get("token") or "").strip()
            if new_tok:
                code = _call(new_tok)

    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Trakt")
    PROBE_DETAIL_CACHE["trakt"] = (now, ok, rsn)
    return ok, rsn

def _probe_mdblist_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["mdblist"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok, rsn

    info = mdblist_user_info(cfg, max_age_sec=0 if max_age_sec == 0 else USERINFO_TTL)

    if not info:
        ok = False
        rsn = "MDBLIST: user lookup failed"
    else:
        ok = True
        rsn = ""

    PROBE_DETAIL_CACHE["mdblist"] = (now, ok, rsn)
    return ok, rsn


def _probe_jellyfin_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["jellyfin"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok, rsn

    jf = (cfg.get("jellyfin") or cfg.get("JELLYFIN") or {}) or {}
    server = (jf.get("server") or "").strip()
    token = (jf.get("access_token") or jf.get("token") or "").strip()

    if not server:
        rsn = "Jellyfin: missing server URL"
        PROBE_DETAIL_CACHE["jellyfin"] = (now, False, rsn)
        return False, rsn
    if not token:
        rsn = "Jellyfin: missing access token"
        PROBE_DETAIL_CACHE["jellyfin"] = (now, False, rsn)
        return False, rsn

    url = f"{server.rstrip('/')}/System/Info/Public"
    code, _ = _http_get(url, headers={**UA})

    if code == 404:
        url2 = f"{server.rstrip('/')}/System/Info"
        code, _ = _http_get(url2, headers={**UA, "X-Emby-Token": token})

    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Jellyfin")
    PROBE_DETAIL_CACHE["jellyfin"] = (now, ok, rsn)
    return ok, rsn


def _probe_emby_detail(cfg: dict[str, Any], max_age_sec: int = PROBE_TTL) -> tuple[bool, str]:
    ts, ok, rsn = PROBE_DETAIL_CACHE["emby"]
    now = time.time()
    if now - ts < max_age_sec:
        return ok, rsn

    em = (cfg.get("emby") or cfg.get("EMBY") or {}) or {}
    server = (em.get("server") or "").strip()
    token = (em.get("access_token") or em.get("token") or em.get("api_key") or "").strip()
    if not server:
        rsn = "Emby: missing server URL"
        PROBE_DETAIL_CACHE["emby"] = (now, False, rsn)
        return False, rsn
    if not token:
        rsn = "Emby: missing access token"
        PROBE_DETAIL_CACHE["emby"] = (now, False, rsn)
        return False, rsn

    url = f"{server.rstrip('/')}/System/Info"
    headers = {**UA, "X-Emby-Token": token}
    code, _ = _http_get(url, headers=headers)
    ok = code == 200
    rsn = "" if ok else _reason_http(code, "Emby")
    PROBE_DETAIL_CACHE["emby"] = (now, ok, rsn)
    return ok, rsn


# VIP badges
def plex_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    ts, info = _USERINFO_CACHE["plex"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info

    token = ((cfg.get("plex") or {}).get("account_token") or "").strip()
    if not token:
        _USERINFO_CACHE["plex"] = (now, {})
        return {}

    plexpass: bool | None = None
    plan: str | None = None
    status: str | None = None

    if HAVE_PLEXAPI:
        try:
            acc = MyPlexAccount(token=token)  # type: ignore[call-arg]
            plexpass = bool(
                getattr(acc, "subscriptionActive", None)
                or getattr(acc, "hasPlexPass", None)
            )
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

    _USERINFO_CACHE["plex"] = (now, out)
    return out


def mdblist_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    ts, info = _USERINFO_CACHE.get("mdblist", (0.0, {}))
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info

    md = (cfg.get("mdblist") or cfg.get("MDBLIST") or {}) or {}
    api_key = (md.get("api_key") or "").strip()
    if not api_key:
        _USERINFO_CACHE["mdblist"] = (now, {})
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

        limits = {
            "api_requests": _to_int(j.get("api_requests")),
            "api_requests_count": _to_int(j.get("api_requests_count")),
        }
        patron_status = j.get("patron_status") or None
        is_supporter = bool(j.get("is_supporter"))
        vip = is_supporter or (
            str(patron_status).lower()
            in ("active_patron", "patron", "supporter")
        )

        out = {
            "vip": vip,
            "vip_type": "patron" if vip else None,
            "patron_status": patron_status,
            "username": j.get("username"),
            "user_id": j.get("user_id"),
            "limits": limits,
        }

    _USERINFO_CACHE["mdblist"] = (now, out)
    return out


def trakt_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    ts, info = _USERINFO_CACHE["trakt"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info

    tr = (cfg.get("trakt") or cfg.get("TRAKT") or {}) or {}
    auth_tr = (cfg.get("auth") or {}).get("trakt") or (cfg.get("auth") or {}).get("TRAKT") or {}
    cid = (tr.get("client_id") or auth_tr.get("client_id") or "").strip()
    tok = (auth_tr.get("access_token") or tr.get("access_token") or tr.get("token") or "").strip()
    if not cid or not tok:
        _USERINFO_CACHE["trakt"] = (now, {})
        return {}

    headers = {
        **UA,
        "Authorization": f"Bearer {tok}",
        "trakt-api-key": cid,
        "trakt-api-version": "2",
    }
    code, body = _http_get("https://api.trakt.tv/users/settings", headers=headers)
    out: dict[str, Any] = {}
    if code == 200:
        j = _json_loads(body) or {}
        u = j.get("user") or {}

        vip = bool(u.get("vip") or u.get("vip_og") or u.get("vip_ep"))
        vip_type = (
            "vip_og"
            if u.get("vip_og")
            else ("vip_ep" if u.get("vip_ep") else ("vip" if vip else ""))
        )

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
            limits_out["watchlist"] = {
                "item_count": wl_limit if wl_limit is not None else int(wl_used or 0),
                "used": int(wl_used or 0),
            }

        coll_raw = limits_raw.get("collection") or {}
        coll_limit = _int_or_none(coll_raw.get("item_count"))
        coll_used = used_counts.get("collection") if isinstance(used_counts.get("collection"), int) else None
        if coll_limit is not None or coll_used is not None:
            limits_out["collection"] = {
                "item_count": coll_limit if coll_limit is not None else int(coll_used or 0),
                "used": int(coll_used or 0),
            }

        out = {
            "vip": vip,
            "vip_type": vip_type,
        }
        if limits_out:
            out["limits"] = limits_out

        last_err = _load_trakt_last_limit_error()
        if isinstance(last_err, dict) and last_err.get("feature") and last_err.get("ts"):
            out["last_limit_error"] = {
                "feature": str(last_err.get("feature")),
                "ts": str(last_err.get("ts")),
            }

    _USERINFO_CACHE["trakt"] = (now, out)
    return out

def emby_user_info(cfg: dict[str, Any], max_age_sec: int = USERINFO_TTL) -> dict[str, Any]:
    ts, info = _USERINFO_CACHE["emby"]
    now = time.time()
    if now - ts < max_age_sec and isinstance(info, dict):
        return info

    em = (cfg.get("emby") or cfg.get("EMBY") or {}) or {}
    server = (em.get("server") or "").strip()
    token = (em.get("access_token") or em.get("token") or em.get("api_key") or "").strip()
    if not server or not token:
        _USERINFO_CACHE["emby"] = (now, {})
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
                return v.strip().lower() not in (
                    "",
                    "0",
                    "false",
                    "no",
                    "none",
                    "null",
                )
            return False

        prem = any(_truthy(j.get(k)) for k in cand)
        if not prem:
            for k, v in j.items():
                if isinstance(k, str) and "supporter" in k.lower() and _truthy(v):
                    prem = True
                    break

        out = {"premiere": bool(prem)}

    _USERINFO_CACHE["emby"] = (now, out)
    return out

def _prov_configured(cfg: dict[str, Any], name: str) -> bool:
    n = (name or "").strip().lower()
    if n == "plex":
        return bool((cfg.get("plex") or {}).get("account_token"))
    if n == "trakt":
        return bool(
            (cfg.get("trakt") or {}).get("access_token")
            or (cfg.get("auth") or {}).get("trakt", {}).get("access_token")
        )
    if n == "simkl":
        return bool((cfg.get("simkl") or {}).get("access_token"))
    if n == "jellyfin":
        jf = cfg.get("jellyfin") or {}
        return bool(
            (jf.get("server") or "").strip()
            and (jf.get("access_token") or jf.get("token") or "").strip()
        )
    if n == "emby":
        em = cfg.get("emby") or {}
        return bool(
            (em.get("server") or "").strip()
            and (em.get("access_token") or em.get("token") or em.get("api_key") or "").strip()
        )
    if n == "mdblist":
        md = cfg.get("mdblist") or {}
        return bool((md.get("api_key") or "").strip())
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
            return (
                x.get("provider")
                or x.get("name")
                or x.get("id")
                or x.get("type")
                or ""
            )
        return ""

    a = _name(pair.get("source") or pair.get("a") or pair.get("src") or pair.get("from"))
    b = _name(pair.get("target") or pair.get("b") or pair.get("dst") or pair.get("to"))
    return bool(_prov_configured(cfg, a) and _prov_configured(cfg, b))

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
    "JELLYFIN": _probe_jellyfin_detail,
    "EMBY": _probe_emby_detail,
    "MDBLIST": _probe_mdblist_detail,
}
USERINFO_FNS: dict[str, Callable[..., dict[str, Any]]] = {
    "PLEX": plex_user_info,
    "TRAKT": trakt_user_info,
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

        cfg = load_config_fn() or {}
        pairs = cfg.get("pairs") or []
        any_pair_ready = any(_pair_ready(cfg, p) for p in pairs)

        probe_age = 0 if fresh else PROBE_TTL
        user_age = USERINFO_TTL

        with ThreadPoolExecutor(max_workers=len(DETAIL_PROBES)) as ex:
            futs = {
                ex.submit(_safe_probe_detail, fn, cfg, probe_age): name
                for name, fn in DETAIL_PROBES.items()
            }
            results: dict[str, tuple[bool, str]] = {}
            for f in as_completed(futs):
                name = futs[f]
                try:
                    results[name] = f.result()
                except Exception as e:
                    results[name] = (False, f"probe failed: {e}")

        plex_ok, plex_reason = results.get("PLEX", (False, ""))
        simkl_ok, simkl_reason = results.get("SIMKL", (False, ""))
        trakt_ok, trakt_reason = results.get("TRAKT", (False, ""))
        jelly_ok, jelly_reason = results.get("JELLYFIN", (False, ""))
        emby_ok, emby_reason = results.get("EMBY", (False, ""))
        mdbl_ok, mdbl_reason = results.get("MDBLIST", (False, ""))

        debug = bool((cfg.get("runtime") or {}).get("debug"))

        info_plex = (
            _safe_userinfo(plex_user_info, cfg, max_age_sec=user_age) if plex_ok else {}
        )
        info_trakt = (
            _safe_userinfo(trakt_user_info, cfg, max_age_sec=user_age) if trakt_ok else {}
        )
        info_emby = (
            _safe_userinfo(emby_user_info, cfg, max_age_sec=user_age) if emby_ok else {}
        )
        info_mdbl = (
            _safe_userinfo(mdblist_user_info, cfg, max_age_sec=user_age) if mdbl_ok else {}
        )
        
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
                        trakt_block["limits"]["watchlist"] = {
                            "item_count": int((watchlist.get("item_count") or 0)),
                            "used": int((watchlist.get("used") or 0)),
                        }
                    if collection:
                        trakt_block["limits"]["collection"] = {
                            "item_count": int((collection.get("item_count") or 0)),
                            "used": int((collection.get("used") or 0)),
                        }

            last_err = info_trakt.get("last_limit_error")
            if isinstance(last_err, dict) and last_err.get("feature") and last_err.get("ts"):
                trakt_block["last_limit_error"] = {
                    "feature": str(last_err.get("feature")),
                    "ts": str(last_err.get("ts")),
                }

        data: dict[str, Any] = {
            "plex_connected": plex_ok,
            "simkl_connected": simkl_ok,
            "trakt_connected": trakt_ok,
            "jellyfin_connected": jelly_ok,
            "emby_connected": emby_ok,
            "mdblist_connected": mdbl_ok,
            "debug": debug,
            "can_run": bool(any_pair_ready),
            "ts": int(now),
            "providers": {
                "PLEX": {
                    "connected": plex_ok,
                    **({} if plex_ok else {"reason": plex_reason}),
                    **(
                        {}
                        if not info_plex
                        else {
                            "plexpass": bool(info_plex.get("plexpass")),
                            "subscription": info_plex.get("subscription") or {},
                        }
                    ),
                },
                "SIMKL": {
                    "connected": simkl_ok,
                    **({} if simkl_ok else {"reason": simkl_reason}),
                },
                "TRAKT": trakt_block,

                "JELLYFIN": {
                    "connected": jelly_ok,
                    **({} if jelly_ok else {"reason": jelly_reason}),
                },
                "EMBY": {
                    "connected": emby_ok,
                    **({} if emby_ok else {"reason": emby_reason}),
                    **(
                        {}
                        if not info_emby
                        else {"premiere": bool(info_emby.get("premiere"))}
                    ),
                },
                "MDBLIST": {
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
                                "api_requests": int(
                                    ((info_mdbl.get("limits") or {}).get("api_requests") or 0)
                                ),
                                "api_requests_count": int(
                                    (
                                        (info_mdbl.get("limits") or {}).get(
                                            "api_requests_count"
                                        )
                                        or 0
                                    )
                                ),
                            },
                        }
                    ),
                },
            },
        }

        STATUS_CACHE["ts"] = now
        STATUS_CACHE["data"] = data
        return JSONResponse(data, headers={"Cache-Control": "no-store"})

    @app.post("/api/debug/clear_probe_cache", tags=["probes"])
    def clear_probe_cache() -> dict[str, Any]:
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
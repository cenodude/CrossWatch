# /providers/sync/plex/_utils.py
# Plex Utils for CrossWatch - use across multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import re
import time
import ipaddress
import xml.etree.ElementTree as ET
from urllib.parse import urlparse
from typing import Any, Mapping

import requests
from requests.exceptions import ConnectionError, SSLError

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import normalize_instance_id

from ._common import make_logger, plex_headers
def _boolish(value: Any, default: bool) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    s = str(value).strip().lower()
    if s in ("0", "false", "no", "off", "n"):
        return False
    if s in ("1", "true", "yes", "on", "y"):
        return True
    return default


_dbg, _info, _warn, _error, _log = make_logger("utils")


_LIB_TTL_S = int(os.environ.get("CW_PLEX_LIB_TTL_S", "600"))
_ACCT_TTL_S = int(os.environ.get("CW_PLEX_ACCT_TTL_S", "900"))
_MIN_HTTP_S = float(os.environ.get("CW_PLEX_MIN_HTTP_INTERVAL_S", "5"))


_CACHE: dict[str, dict[str, Any]] = {
    "libs": {"key": None, "ts": 0.0, "data": []},
    "owner": {"key": None, "ts": 0.0, "data": (None, None)},
    "aid_by_user": {},
}
_LAST_HTTP: dict[str, float] = {}


def _cache_hit(ts: float, ttl: int) -> bool:
    return (time.time() - float(ts or 0.0)) < max(1, int(ttl))


def _throttle(path: str) -> bool:
    now = time.time()
    last = float(_LAST_HTTP.get(path) or 0.0)
    if (now - last) < max(0.0, _MIN_HTTP_S):
        return True
    _LAST_HTTP[path] = now
    return False


def _plex(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    plex = cfg.get("plex")
    if not isinstance(plex, dict):
        plex = {}
        if isinstance(cfg, dict):
            cfg["plex"] = plex  # type: ignore[assignment]
        return plex
    inst = normalize_instance_id(instance_id)
    if inst == "default":
        return plex
    insts = plex.get("instances")
    if not isinstance(insts, dict):
        if not isinstance(cfg, dict):
            return {}
        insts = {}
        plex["instances"] = insts
    blk = insts.get(inst)
    if isinstance(blk, dict):
        return blk
    if not isinstance(cfg, dict):
        return {}
    out: dict[str, Any] = {}
    insts[inst] = out
    return out


def _insert_key_first_inplace(d: dict[str, Any], key: str, value: Any) -> bool:
    if key in d:
        if d[key] != value:
            d[key] = value
            return True
        return False
    new_dict: dict[str, Any] = {key: value}
    new_dict.update(d)
    d.clear()
    d.update(new_dict)
    return True


def _insert_key_after_inplace(d: dict[str, Any], after: str, key: str, value: Any) -> bool:
    if key in d:
        if d[key] != value:
            d[key] = value
            return True
        return False
    new_dict: dict[str, Any] = {}
    inserted = False
    for existing_key, existing_value in d.items():
        new_dict[existing_key] = existing_value
        if not inserted and existing_key == after:
            new_dict[key] = value
            inserted = True
    if not inserted:
        new_dict[key] = value
    d.clear()
    d.update(new_dict)
    return True


def _is_empty(value: Any) -> bool:
    return value is None or (isinstance(value, str) and value.strip() == "")


def _plex_headers(token: str) -> dict[str, str]:
    return plex_headers(
        token or "",
        platform="Web",
        version="1.0",
        accept="application/xml, application/json;q=0.9,*/*;q=0.5",
        user_agent="CrossWatch/1.0",
    )


def _resource_token_for_connection(token: str, client_id: str | None, baseurl: str, timeout: float = 8.0) -> tuple[str | None, str | None]:
    tok = (token or "").strip()
    base = (baseurl or "").strip()
    if not tok or not base:
        return None, None
    try:
        u = urlparse(base)
        want_host = (u.hostname or "").strip().lower()
        want_port = int(u.port or 32400)
        want_scheme = (u.scheme or "").strip().lower()
    except Exception:
        return None, None

    headers = _plex_headers(tok)
    if client_id and str(client_id).strip():
        headers["X-Plex-Client-Identifier"] = str(client_id).strip()

    r = requests.get(
        "https://plex.tv/api/resources",
        params={"includeHttps": 1, "includeRelay": 1, "includeIPv6": 1},
        headers=headers,
        timeout=float(timeout),
    )
    r.raise_for_status()
    root = ET.fromstring(r.text or "")

    best: tuple[int, str | None, str | None] = (-1, None, None)
    for dev in list(root):
        if (dev.tag or "").lower() != "device":
            continue
        attrs = dev.attrib or {}
        provides = (attrs.get("provides") or "").lower()
        if "server" not in provides:
            continue
        mid = (attrs.get("clientIdentifier") or attrs.get("machineIdentifier") or "").strip() or None
        atok = (attrs.get("accessToken") or "").strip() or None
        if not atok:
            continue

        score = -1
        for conn in dev.findall(".//Connection"):
            uri = (conn.attrib.get("uri") or "").strip()
            if not uri:
                continue
            try:
                cu = urlparse(uri)
                host = (cu.hostname or "").strip().lower()
                port = int(cu.port or 32400)
                scheme = (cu.scheme or "").strip().lower()
            except Exception:
                continue
            if host != want_host or port != want_port:
                continue
            score = max(score, 100 if scheme == want_scheme else 80)

        if score > best[0]:
            best = (score, mid, atok)

    return best[1], best[2]


def _resource_host_flags(uri: str) -> tuple[str, bool, bool]:
    try:
        u = urlparse(uri)
        host = (u.hostname or "").strip().lower()
        if not host:
            return "", False, False
        is_private = False
        is_ip = False
        try:
            ip = ipaddress.ip_address(host)
            is_ip = True
            is_private = bool(ip.is_private or ip.is_link_local)
            return host, is_private, is_ip
        except Exception:  # noqa: BLE001
            pass
        m = re.match(r"^(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct$", host, re.IGNORECASE)
        if m:
            dotted = m.group(1).replace("-", ".")
            try:
                ip = ipaddress.ip_address(dotted)
                is_private = bool(ip.is_private or ip.is_link_local)
            except Exception:  # noqa: BLE001
                is_private = False
        return host, is_private, is_ip
    except Exception:  # noqa: BLE001
        return "", False, False


def _resource_conn_score(uri: str, local: bool, relay: bool) -> int:
    uri = (uri or "").strip()
    https = uri.startswith("https://")
    host, is_private, is_ip = _resource_host_flags(uri)
    score = 0
    score += 8 if not local else 1
    score += 6 if https else 0
    score += 4 if not relay else 2
    score += 3 if not is_private else 0
    score += 2 if host.endswith(".plex.direct") else 0
    score += 1 if (host and not is_ip) else 0
    return score


def discover_pms_access_from_cloud(
    token: str,
    base_url: str | None = None,
    machine_id: str | None = None,
    timeout: float = 8.0,
) -> tuple[str | None, str | None, str | None]:

    t = (token or "").strip()
    if not t:
        return None, None, None

    want_host = ""
    want_port = 32400
    want_scheme = ""
    if base_url:
        try:
            u = urlparse((base_url or "").strip())
            want_host = (u.hostname or "").strip().lower()
            want_port = int(u.port or 32400)
            want_scheme = (u.scheme or "").strip().lower()
        except Exception:  # noqa: BLE001
            want_host, want_port, want_scheme = "", 32400, ""

    headers = _plex_headers(t)
    try:
        r = requests.get(
            "https://plex.tv/api/resources",
            params={"includeHttps": 1, "includeRelay": 1, "includeIPv6": 1},
            headers=headers,
            timeout=float(timeout),
        )
        if not r.ok or not (r.text or "").lstrip().startswith("<"):
            return None, None, None
        root = ET.fromstring(r.text or "")
    except Exception:  # noqa: BLE001
        return None, None, None

    best: tuple[int, str | None, str | None, str | None] = (-1, None, None, None)

    for dev in list(root):
        if (dev.tag or "").lower() != "device":
            continue
        attrs = dev.attrib or {}
        provides = (attrs.get("provides") or "").lower()
        if "server" not in provides:
            continue

        dev_mid = (attrs.get("clientIdentifier") or attrs.get("machineIdentifier") or "").strip() or None
        dev_tok = (attrs.get("accessToken") or "").strip() or None
        if not dev_tok:
            continue

        hard_match = False
        soft_match = False
        if machine_id and dev_mid and dev_mid == str(machine_id).strip():
            hard_match = True

        best_uri = ""
        best_uri_score = -1
        base_match_score = -1

        for conn in dev.findall(".//Connection"):
            uri = (conn.attrib.get("uri") or "").strip().rstrip("/")
            if not uri:
                continue
            local = (conn.attrib.get("local") or "").strip().lower() in ("1", "true", "yes")
            relay = (conn.attrib.get("relay") or "").strip().lower() in ("1", "true", "yes")

            cs = _resource_conn_score(uri, local=local, relay=relay)
            if cs > best_uri_score:
                best_uri_score = cs
                best_uri = uri

            if want_host:
                try:
                    cu = urlparse(uri)
                    host = (cu.hostname or "").strip().lower()
                    port = int(cu.port or 32400)
                    scheme = (cu.scheme or "").strip().lower()
                except Exception:  # noqa: BLE001
                    continue
                if host == want_host and port == want_port:
                    soft_match = True
                    base_match_score = max(base_match_score, 200 if scheme == want_scheme else 160)

        if not (hard_match or soft_match):
            continue

        dev_score = best_uri_score
        if hard_match:
            dev_score += 1000
        if soft_match:
            dev_score += 800
        dev_score += base_match_score if base_match_score > 0 else 0

        if dev_score > best[0]:
            best = (dev_score, dev_tok, dev_mid, best_uri or None)

    return best[1], best[2], best[3]

def _resolve_verify_from_cfg(cfg: Mapping[str, Any], url: str, instance_id: Any = None) -> bool:
    if not str(url).lower().startswith("https"):
        return True
    plex = _plex(cfg, instance_id)
    env = os.environ.get("CW_PLEX_VERIFY")
    if env is not None:
        return _boolish(env, True)
    if "verify_ssl" in plex:
        return _boolish(plex.get("verify_ssl"), True)
    if "verify_ssl" in cfg:
        return _boolish(cfg.get("verify_ssl"), True)
    return True


def _build_session(token: str, verify: bool) -> requests.Session:
    session = requests.Session()
    session.trust_env = False
    session.verify = verify
    session.headers.update(_plex_headers(token))
    return session


_ipplex = re.compile(r"^(https?://)(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct(:\d+)?$", re.IGNORECASE)


def _fallback_bases(base_url: str) -> list[str]:
    bases: list[str] = []
    if base_url.startswith("https://"):
        bases.append("http://" + base_url[8:])
    match = _ipplex.match(base_url)
    if match:
        dotted = match.group(2).replace("-", ".")
        port = match.group(3) or ""
        bases.append(f"https://{dotted}{port}")
        bases.append(f"http://{dotted}{port}")
    return [b.rstrip("/") for b in bases if b]


def _try_get(session: requests.Session, base: str, path: str, timeout: float) -> requests.Response | None:
    url = f"{base.rstrip('/')}{path}"
    try:
        return session.get(url, timeout=timeout)
    except (SSLError, ConnectionError) as e:
        _warn("http_primary_failed", url=url, error=str(e))
        for fb in _fallback_bases(base):
            try:
                _info("http_fallback_try", url=f"{fb}{path}")
                session.verify = fb.startswith("https://") and session.verify
                response = session.get(f"{fb}{path}", timeout=timeout)
                if response is not None:
                    return response
            except Exception as ee:  # noqa: BLE001
                _warn("http_fallback_failed", url=f"{fb}{path}", error=str(ee))
    except Exception as e:  # noqa: BLE001
        _warn("http_request_failed", url=url, error=str(e))
    return None


def _pick_server_url_from_resources(xml_text: str) -> str:
    try:
        root = ET.fromstring(xml_text)
        best_uri = ""
        best_score = -1
        for dev in root.findall(".//Device"):
            if "server" not in (dev.attrib.get("provides") or ""):
                continue
            for conn in dev.findall(".//Connection"):
                uri = (conn.attrib.get("uri") or "").strip().rstrip("/")
                if not uri:
                    continue
                local = (conn.attrib.get("local") or "").strip().lower() in ("1", "true", "yes")
                relay = (conn.attrib.get("relay") or "").strip().lower() in ("1", "true", "yes")
                score = _resource_conn_score(uri, local=local, relay=relay)
                if score > best_score:
                    best_score = score
                    best_uri = uri
        return best_uri
    except Exception:  # noqa: BLE001
        return ""


def discover_server_url_from_cloud(token: str, timeout: float = 10.0) -> str | None:
    try:
        response = requests.get(
            "https://plex.tv/api/resources?includeHttps=1&includeRelay=1&includeIPv6=1",
            headers={"X-Plex-Token": token, "Accept": "application/xml"},
            timeout=timeout,
        )
        if response.ok and (response.text or "").lstrip().startswith("<"):
            picked = _pick_server_url_from_resources(response.text)
            return picked or None
    except Exception:  # noqa: BLE001
        pass
    return None


def fetch_cloud_user_info(token: str, timeout: float = 8.0) -> dict[str, Any] | None:
    t = (token or "").strip()
    if not t:
        return None
    try:
        response = requests.get("https://plex.tv/api/v2/user", headers=_plex_headers(t), timeout=timeout)
        if not response.ok:
            return None
        data = response.json()
        return data if isinstance(data, dict) else None
    except Exception as e:  # noqa: BLE001
        _warn("cloud_user_fetch_failed", error=str(e))
        return None


def fetch_cloud_home_users(token: str, timeout: float = 8.0) -> list[dict[str, Any]]:
    t = (token or "").strip()
    if not t:
        return []

    def from_xml(xml_text: str) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        try:
            root = ET.fromstring(xml_text)
            for u in root.findall(".//User") + root.findall(".//user"):
                uid_raw = u.attrib.get("id") or u.attrib.get("ID")
                try:
                    uid = int(uid_raw or 0)
                except Exception:  # noqa: BLE001
                    uid = 0
                if uid <= 0:
                    continue
                title = (u.attrib.get("title") or u.attrib.get("name") or "").strip()
                uname = (u.attrib.get("username") or title or "").strip()
                email = (u.attrib.get("email") or "").strip()
                admin = (u.attrib.get("admin") or u.attrib.get("isAdmin") or "").strip().lower()
                is_admin = admin in ("1", "true", "yes")
                out.append({"id": uid, "username": uname, "title": title or uname, "email": email, "type": "owner" if is_admin else "managed"})
        except Exception:  # noqa: BLE001
            return []
        return out

    def from_json(data: Any) -> list[dict[str, Any]]:
        arr: list[Any] = []
        if isinstance(data, list):
            arr = data
        elif isinstance(data, dict):
            cand = data.get("users") or data.get("homeUsers") or data.get("home_users")
            if isinstance(cand, list):
                arr = cand
        out: list[dict[str, Any]] = []
        for it in arr:
            if not isinstance(it, dict):
                continue
            uid = it.get("id")
            try:
                uid_i = int(uid or 0)
            except Exception:  # noqa: BLE001
                uid_i = 0
            if uid_i <= 0:
                continue
            title = str(it.get("title") or it.get("name") or "").strip()
            uname = str(it.get("username") or title or "").strip()
            email = str(it.get("email") or "").strip()
            is_admin = bool(it.get("admin") or it.get("isAdmin") or it.get("is_admin"))
            out.append({"id": uid_i, "username": uname, "title": title or uname, "email": email, "type": "owner" if is_admin else "managed"})
        return out

    urls = ("https://plex.tv/api/v2/home/users", "https://plex.tv/api/home/users")
    headers = _plex_headers(t)
    for url in urls:
        try:
            response = requests.get(url, headers=headers, timeout=timeout)
            if not response.ok:
                continue
            text = (response.text or "").lstrip()
            if text.startswith("<"):
                users = from_xml(text)
                if users:
                    return users
                continue
            users = from_json(response.json())
            if users:
                return users
        except Exception:  # noqa: BLE001
            continue
    return []


def fetch_cloud_account_users(token: str, timeout: float = 8.0) -> list[dict[str, Any]]:
    t = (token or "").strip()
    if not t:
        return []

    headers = _plex_headers(t)
    headers["Accept"] = "application/xml"
    urls = ("https://plex.tv/api/users", "https://plex.tv/api/users/")
    for url in urls:
        try:
            r = requests.get(url, headers=headers, timeout=timeout)
            if not r.ok:
                r = requests.get(url, headers={k: v for k, v in headers.items() if k != "X-Plex-Token"}, params={"X-Plex-Token": t}, timeout=timeout)
            if not r.ok:
                continue
            text = (r.text or "").lstrip()
            if not text.startswith("<"):
                continue
            root = ET.fromstring(text)
            out: list[dict[str, Any]] = []
            for u in root.findall(".//User") + root.findall(".//user"):
                uid_raw = u.attrib.get("id") or u.attrib.get("ID")
                try:
                    uid = int(uid_raw or 0)
                except Exception:  # noqa: BLE001
                    uid = 0
                if uid <= 0:
                    continue
                title = (u.attrib.get("title") or u.attrib.get("name") or "").strip()
                uname = (u.attrib.get("username") or title or "").strip()
                email = (u.attrib.get("email") or "").strip()
                out.append({"id": uid, "username": uname or title or f"user{uid}", "title": title or uname, "email": email, "type": "friend"})
            if out:
                return out
        except Exception:  # noqa: BLE001
            continue
    return []


def _pms_id_from_attr_map(attrs: Mapping[str, Any]) -> int | None:
    value = attrs.get("id") or attrs.get("ID")
    if value is None:
        return None
    try:
        return int(value)
    except Exception:  # noqa: BLE001
        return None


def _looks_cloudish(value: int | None) -> bool:
    try:
        return int(value or -1) >= 100000
    except Exception:  # noqa: BLE001
        return True


def _parse_accounts_all(xml_text: str) -> list[tuple[int, str]]:
    out: list[tuple[int, str]] = []
    try:
        root = ET.fromstring(xml_text)
        for account in root.findall(".//Account"):
            aid = _pms_id_from_attr_map(account.attrib)
            if aid is None:
                continue
            name = (account.attrib.get("name") or account.attrib.get("username") or "").strip()
            out.append((aid, name))
    except Exception:  # noqa: BLE001
        pass
    return out


def _pick_owner_id(accounts: list[tuple[int, str]]) -> tuple[str | None, int | None]:
    locals_only = [(aid, name) for (aid, name) in accounts if aid > 0 and not _looks_cloudish(aid)]
    if not locals_only:
        return (accounts[0][1], accounts[0][0]) if accounts else (None, None)
    locals_only.sort(key=lambda t: t[0])
    aid, name = locals_only[0]
    if any(it[0] == 1 for it in locals_only):
        aid, name = next((ii, nn) for (ii, nn) in locals_only if ii == 1)
    return name, aid


def _parse_accounts_xml_for_username(xml_text: str, username: str) -> int | None:
    target = (username or "").strip().lower()
    for aid, name in _parse_accounts_all(xml_text):
        if (name or "").lower() == target:
            return int(aid)
    return None


def fetch_accounts_owner(
    base_url: str,
    token: str,
    verify: bool,
    timeout: float = 10.0,
) -> tuple[str | None, int | None]:
    key = (base_url.rstrip("/"), token or "", bool(verify))
    ent = _CACHE["owner"]
    if ent["key"] == key and _cache_hit(ent["ts"], _ACCT_TTL_S):
        return tuple(ent["data"])  # type: ignore[return-value]
    if _throttle("/accounts"):
        return tuple(ent["data"])  # type: ignore[return-value]
    out: tuple[str | None, int | None] = (None, None)
    try:
        session = _build_session(token, verify)
        response = _try_get(session, base_url, "/accounts", timeout)
        if response and response.ok and (response.text or "").lstrip().startswith("<"):
            out = _pick_owner_id(_parse_accounts_all(response.text))
    except Exception as e:  # noqa: BLE001
        _warn("owner_fetch_failed", error=str(e))
    _CACHE["owner"] = {"key": key, "ts": time.time(), "data": out}
    return out


def fetch_account_id_for_username(
    base_url: str,
    token: str,
    username: str,
    verify: bool,
    timeout: float = 10.0,
) -> int | None:
    uname = (username or "").strip()
    if not uname:
        return None
    cache_key = f"{base_url.rstrip('/')}\n{token or ''}\n{uname.lower()}\n{1 if verify else 0}"
    bucket = _CACHE["aid_by_user"]
    ent = bucket.get(cache_key)
    if ent and _cache_hit(ent.get("ts", 0.0), _ACCT_TTL_S):
        return ent.get("aid")
    if _throttle("/accounts"):
        return ent.get("aid") if ent else None
    aid: int | None = None
    try:
        session = _build_session(token, verify)
        response = _try_get(session, base_url, "/accounts", timeout)
        if response and response.ok and (response.text or "").lstrip().startswith("<"):
            aid = _parse_accounts_xml_for_username(response.text, uname)
    except Exception as e:  # noqa: BLE001
        _warn("account_id_fetch_failed", error=str(e))
    bucket[cache_key] = {"ts": time.time(), "aid": aid}
    return aid



def _parse_accounts_xml_for_cloud_id(xml_text: str, cloud_account_id: int) -> int | None:
    try:
        target = int(cloud_account_id)
    except Exception:  # noqa: BLE001
        return None
    try:
        root = ET.fromstring(xml_text)
    except Exception:  # noqa: BLE001
        return None
    for account in root.findall(".//Account"):
        pms_id = _pms_id_from_attr_map(account.attrib)
        if pms_id is None or pms_id <= 0:
            continue
        attrs = account.attrib or {}
        raw = (
            attrs.get("accountID")
            or attrs.get("accountId")
            or attrs.get("account_id")
            or attrs.get("cloudID")
            or attrs.get("cloudId")
            or attrs.get("cloud_id")
        )
        try:
            cid = int(raw) if raw is not None and str(raw).strip().isdigit() else None
        except Exception:  # noqa: BLE001
            cid = None
        if cid is not None and cid == target:
            return int(pms_id)
    return None


def fetch_account_id_for_cloud_id(
    base_url: str,
    token: str,
    cloud_account_id: int,
    verify: bool,
    timeout: float = 10.0,
) -> int | None:
    try:
        cid = int(cloud_account_id)
    except Exception:  # noqa: BLE001
        return None
    if cid <= 0:
        return None
    cache_key = f"{base_url.rstrip('/')}\n{token or ''}\ncloud:{cid}\n{1 if verify else 0}"
    bucket = _CACHE["aid_by_user"]
    ent = bucket.get(cache_key)
    if ent and _cache_hit(ent.get("ts", 0.0), _ACCT_TTL_S):
        return ent.get("aid")
    if _throttle("/accounts"):
        return ent.get("aid") if ent else None
    aid: int | None = None
    try:
        session = _build_session(token, verify)
        response = _try_get(session, base_url, "/accounts", timeout)
        if response and response.ok and (response.text or "").lstrip().startswith("<"):
            aid = _parse_accounts_xml_for_cloud_id(response.text, cid)
    except Exception as e:  # noqa: BLE001
        _warn("account_id_cloud_fetch_failed", error=str(e))
    bucket[cache_key] = {"ts": time.time(), "aid": aid}
    return aid


def _libs_key(base_url: str, token: str, verify: bool) -> tuple[str, str, bool]:
    return base_url.rstrip("/"), token or "", bool(verify)


def fetch_libraries(
    base_url: str,
    token: str,
    verify: bool,
    timeout: float = 10.0,
) -> list[dict[str, Any]]:
    key = _libs_key(base_url, token, verify)
    ent = _CACHE["libs"]
    if ent["key"] == key and _cache_hit(ent["ts"], _LIB_TTL_S):
        return list(ent["data"])
    if _throttle("/library/sections"):
        return list(ent["data"])
    libs: list[dict[str, Any]] = []
    try:
        session = _build_session(token, verify)
        response = _try_get(session, base_url, "/library/sections", timeout)
        if response and response.ok and (response.text or "").lstrip().startswith("<"):
            root = ET.fromstring(response.text)
            for directory in root.findall(".//Directory"):
                keyv = directory.attrib.get("key")
                title = directory.attrib.get("title")
                lib_type = directory.attrib.get("type")
                if keyv and title:
                    libs.append({"key": str(keyv), "title": title, "type": lib_type or "lib"})
    except Exception as e:  # noqa: BLE001
        _warn("sections_fetch_failed", error=str(e))
    _CACHE["libs"] = {"key": key, "ts": time.time(), "data": list(libs)}
    return libs


def fetch_libraries_from_cfg(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> list[dict[str, Any]]:
    cfg = load_config() if cfg is None else cfg
    plex = _plex(cfg, instance_id)
    cloud_token = (plex.get("account_token") or "").strip()
    pms_token = (plex.get("pms_token") or "").strip()
    base = (plex.get("server_url") or "").strip()
    if not cloud_token:
        return []
    if not base:
        base_url = discover_server_url_from_cloud(cloud_token) or ""
        if base_url:
            _insert_key_first_inplace(plex, "server_url", base_url)
            save_config(cfg)
        base = base_url
    if not base:
        return []

    if not pms_token:
        try:
            mid2, tok2 = _resource_token_for_connection(cloud_token, plex.get("client_id"), base, timeout=8.0)
            if tok2:
                _insert_key_after_inplace(plex, "account_token", "pms_token", tok2)
                pms_token = tok2
            if mid2 and not str(plex.get("machine_id") or "").strip():
                _insert_key_after_inplace(plex, "client_id" if "client_id" in plex else "pms_token", "machine_id", mid2)
            if tok2 or mid2:
                save_config(cfg)
        except Exception as e:  # noqa: BLE001
            _warn("pms_token_discovery_failed", error=str(e))

    verify = _resolve_verify_from_cfg(cfg, base, instance_id)
    libs = fetch_libraries(base, (pms_token or cloud_token), verify=verify)
    if not libs and verify:
        _info("libs_retry_insecure")
        libs = fetch_libraries(base, (pms_token or cloud_token), verify=False)
    return libs


def inspect_and_persist(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> dict[str, Any]:
    cfg = load_config() if cfg is None else cfg
    plex = _plex(cfg, instance_id)
    token = (plex.get("account_token") or "").strip()
    pms_token = (plex.get("pms_token") or "").strip()
    base = (plex.get("server_url") or "").strip()
    machine_id = (plex.get("machine_id") or "").strip()
    username = plex.get("username") or ""
    account_id = plex.get("account_id")
    cloud_account_id: int | None = None
    try:
        raw_cid = ((plex.get("_cloud") or {}) if isinstance(plex.get("_cloud"), dict) else {}).get("account_id")
        cloud_account_id = int(raw_cid) if raw_cid is not None and str(raw_cid).strip().isdigit() else None
        if cloud_account_id is not None and cloud_account_id <= 0:
            cloud_account_id = None
    except Exception:
        cloud_account_id = None


    cloud: dict[str, Any] | None = None
    if token and (_is_empty(username) or cloud_account_id is None):
        cloud = fetch_cloud_user_info(token) or None
        if isinstance(cloud, dict):
            if cloud_account_id is None:
                try:
                    cid = cloud.get("id")
                    if isinstance(cid, int):
                        cloud_account_id = cid if cid > 0 else None
                    elif cid is not None and str(cid).strip().isdigit():
                        cloud_account_id = int(str(cid).strip())
                        if cloud_account_id <= 0:
                            cloud_account_id = None
                except Exception:  # noqa: BLE001
                    pass
            if cloud_account_id is not None:
                try:
                    if not isinstance(plex.get("_cloud"), dict):
                        plex["_cloud"] = {}
                    plex["_cloud"]["account_id"] = int(cloud_account_id)
                except Exception:
                    pass
            if _is_empty(username):
                u = ((cloud or {}).get("username") or (cloud or {}).get("title") or "").strip()
                if u:
                    after = "account_id" if "account_id" in plex else "client_id"
                    _insert_key_after_inplace(plex, after, "username", u)
                    username = u


    if token and not base:
        base_url = discover_server_url_from_cloud(token) or ""
        if base_url:
            _insert_key_first_inplace(plex, "server_url", base_url)
            save_config(cfg)
            _info("server_url_discovered", server_url=base_url)
        base = base_url

    if token and base:
        if not pms_token or not machine_id:
            try:
                mid2, tok2 = _resource_token_for_connection(token, plex.get("client_id"), base, timeout=8.0)
                if tok2 and not pms_token:
                    _insert_key_after_inplace(plex, "account_token", "pms_token", tok2)
                    pms_token = tok2
                if mid2 and not machine_id:
                    _insert_key_after_inplace(plex, "client_id" if "client_id" in plex else "pms_token", "machine_id", mid2)
                    machine_id = mid2
            except Exception as e:  # noqa: BLE001
                _warn("pms_token_discovery_failed", error=str(e))

        verify = _resolve_verify_from_cfg(cfg, base, instance_id)


        # account_id is PMS-local; resolve it from /accounts.
        legacy_placeholder = str(account_id).strip() == "1"
        if (username or "").strip() and (_is_empty(account_id) or legacy_placeholder) or (cloud_account_id is not None and (_is_empty(account_id) or legacy_placeholder)):
            server_aid: int | None = None
            try:
                if cloud_account_id is not None:
                    server_aid = fetch_account_id_for_cloud_id(base, (pms_token or token), int(cloud_account_id), verify=verify)
            except Exception:  # noqa: BLE001
                server_aid = None
            if server_aid is None and (username or "").strip():
                server_aid = fetch_account_id_for_username(base, (pms_token or token), username, verify=verify)
            if server_aid is not None:
                _insert_key_after_inplace(plex, "client_id", "account_id", int(server_aid))
                account_id = int(server_aid)


    save_config(cfg)
    return {"server_url": base, "username": username, "account_id": account_id, "machine_id": machine_id, "pms_token": bool(pms_token)}


def resolve_owner_account_id(srv: Any, token: str) -> int | None:
    try:
        accounts = srv.systemAccounts() or []
        locals_only = [a.id for a in accounts if a.id and a.id > 0 and not _looks_cloudish(a.id)]
        if locals_only:
            return 1 if 1 in locals_only else sorted(locals_only)[0]
    except Exception:  # noqa: BLE001
        pass
    try:
        sess = getattr(srv, "_session", None)
        if not sess:
            return None
        response = sess.get(srv.url("/accounts"), headers=_plex_headers(token), timeout=10)
        if response.ok and (response.text or "").lstrip().startswith("<"):
            _, aid = _pick_owner_id(_parse_accounts_all(response.text))
            return aid
    except Exception:  # noqa: BLE001
        pass
    return None


def resolve_account_id_by_username(srv: Any, token: str, username: str) -> int | None:
    uname = (username or "").strip()
    if not uname:
        return None
    try:
        for account in srv.systemAccounts() or []:
            if (account.name or "").strip().lower() == uname.lower():
                return int(account.id)
    except Exception:  # noqa: BLE001
        pass
    try:
        sess = getattr(srv, "_session", None)
        if not sess:
            return None
        response = sess.get(srv.url("/accounts"), headers=_plex_headers(token), timeout=10)
        if response.ok and (response.text or "").lstrip().startswith("<"):
            return _parse_accounts_xml_for_username(response.text, uname)
    except Exception:  # noqa: BLE001
        pass
    return None


def resolve_user_scope(
    account: Any,
    srv: Any,
    token: str,
    cfg_username: str | None,
    cfg_account_id: int | None,
) -> tuple[str | None, int | None]:
    cfg_uname = (cfg_username or "").strip() or None
    cfg_aid = int(cfg_account_id) if cfg_account_id is not None else None
    if cfg_uname and cfg_aid is not None and int(cfg_aid) != 1:
        return cfg_uname, cfg_aid
    if cfg_aid is not None and int(cfg_aid) != 1:
        return None, cfg_aid
    try:
        owner_name = getattr(account, "username", None)
    except Exception:  # noqa: BLE001
        owner_name = None
    username = cfg_uname or (str(owner_name).strip() if owner_name else None)
    aid = resolve_account_id_by_username(srv, token, username) if (username and srv) else None
    if aid is None:
        aid = resolve_owner_account_id(srv, token)
    return username, (int(aid) if aid is not None else None)


def ensure_whitelist_defaults(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> bool:
    cfg = load_config() if cfg is None else cfg
    plex = _plex(cfg, instance_id)
    changed = False
    if not isinstance(plex.get("history"), dict):
        plex["history"] = {}
        changed = True
    if not isinstance(plex.get("ratings"), dict):
        plex["ratings"] = {}
        changed = True
    if not isinstance(plex.get("scrobble"), dict):
        plex["scrobble"] = {}
        changed = True
    if not isinstance(plex["history"].get("libraries"), list):
        plex["history"]["libraries"] = []
        changed = True
    if not isinstance(plex["ratings"].get("libraries"), list):
        plex["ratings"]["libraries"] = []
        changed = True
    if not isinstance(plex["scrobble"].get("libraries"), list):
        plex["scrobble"]["libraries"] = []
        changed = True
    for sec in ("history", "ratings", "scrobble"):
        libs = plex[sec]["libraries"]
        norm = sorted({str(x).strip() for x in libs if str(x).strip()})
        if libs != norm:
            plex[sec]["libraries"] = norm
            changed = True
    if changed:
        save_config(cfg)
        _info("whitelist_defaults_ensured")
    return changed


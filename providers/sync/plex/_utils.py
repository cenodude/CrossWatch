# /providers/sync/plex/_utils.py
# Plex Utils for CrossWatch - use across multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import re
import time
import ipaddress
import xml.etree.ElementTree as ET
from typing import Any, Mapping

from .._log import log as cw_log

import requests
from requests.exceptions import ConnectionError, SSLError

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import normalize_instance_id

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


def _dbg(event: str, **fields: Any) -> None:
    cw_log("PLEX", "utils", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PLEX", "utils", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PLEX", "utils", "warn", event, **fields)


def _error(event: str, **fields: Any) -> None:
    cw_log("PLEX", "utils", "error", event, **fields)


def _log(msg: str) -> None:
    _dbg(msg)


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
    cid = os.environ.get("CW_PLEX_CID") or os.environ.get("PLEX_CLIENT_IDENTIFIER") or "CrossWatch"
    return {
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Platform": "Web",
        "X-Plex-Version": "1.0",
        "X-Plex-Client-Identifier": cid,
        "X-Plex-Token": token or "",
        "Accept": "application/xml, application/json;q=0.9,*/*;q=0.5",
        "User-Agent": "CrossWatch/1.0",
    }


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
        from urllib.parse import urlparse

        def host_flags(uri: str) -> tuple[bool, bool]:
            host = (urlparse(uri).hostname or "").strip()
            if not host:
                return False, False
            try:
                ip = ipaddress.ip_address(host)
                return True, bool(ip.is_private or ip.is_link_local)
            except Exception:  # noqa: BLE001
                pass
            m = re.match(r"^(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct$", host, re.IGNORECASE)
            if m:
                dotted = m.group(1).replace("-", ".")
                try:
                    ip = ipaddress.ip_address(dotted)
                    return False, bool(ip.is_private or ip.is_link_local)
                except Exception:  # noqa: BLE001
                    pass
            return False, False

        root = ET.fromstring(xml_text)
        servers: list[tuple[bool, bool, bool, bool, bool, str]] = []
        for dev in root.findall(".//Device"):
            if "server" in (dev.attrib.get("provides") or ""):
                for conn in dev.findall(".//Connection"):
                    uri = (conn.attrib.get("uri") or "").strip()
                    if not uri:
                        continue
                    local = (conn.attrib.get("local") or "") in ("1", "true", "yes")
                    relay = (conn.attrib.get("relay") or "") in ("1", "true", "yes")
                    direct = not relay
                    http = uri.startswith("http://")
                    is_ip, is_private = host_flags(uri)
                    servers.append((local, direct, is_private, http, is_ip, uri.rstrip("/")))
        servers.sort(key=lambda t: (t[0], t[1], t[2], t[3], t[4]), reverse=True)
        return servers[0][5] if servers else ""
    except Exception:  # noqa: BLE001
        return ""


def discover_server_url_from_cloud(token: str, timeout: float = 10.0) -> str | None:
    try:
        response = requests.get(
            "https://plex.tv/api/resources?includeHttps=1",
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
    """Fetch all Plex account users (incl. friends) from plex.tv.

    plex.tv/api/v2/home/users covers Plex Home only. The older plex.tv/api/users
    endpoint returns a broader set including users you shared with.
    """

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
                # Some deployments only accept the token as a query param.
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
        if (name or "").lower() == target and not _looks_cloudish(aid):
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
    token = (plex.get("account_token") or "").strip()
    base = (plex.get("server_url") or "").strip()
    if not token:
        return []
    if not base:
        base_url = discover_server_url_from_cloud(token) or ""
        if base_url:
            _insert_key_first_inplace(plex, "server_url", base_url)
            save_config(cfg)
        base = base_url
    if not base:
        return []
    verify = _resolve_verify_from_cfg(cfg, base, instance_id)
    libs = fetch_libraries(base, token, verify=verify)
    if not libs and verify:
        _info("libs_retry_insecure")
        libs = fetch_libraries(base, token, verify=False)
    return libs


def inspect_and_persist(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> dict[str, Any]:
    cfg = load_config() if cfg is None else cfg
    plex = _plex(cfg, instance_id)
    token = (plex.get("account_token") or "").strip()
    base = (plex.get("server_url") or "").strip()
    username = plex.get("username") or ""
    account_id = plex.get("account_id")

    if token and not base:
        base_url = discover_server_url_from_cloud(token) or ""
        if base_url:
            _insert_key_first_inplace(plex, "server_url", base_url)
            save_config(cfg)
            _info("server_url_discovered", server_url=base_url)
        base = base_url

    if token and base:
        verify = _resolve_verify_from_cfg(cfg, base, instance_id)
        server_user: str | None = None
        server_aid: int | None = None

        if (username or "").strip():
            server_aid = fetch_account_id_for_username(base, token, username, verify=verify)
        if server_aid is None:
            server_user, server_aid = fetch_accounts_owner(base, token, verify=verify)

        if _is_empty(account_id) and server_aid is not None:
            _insert_key_after_inplace(plex, "client_id", "account_id", int(server_aid))
            account_id = int(server_aid)

        if _is_empty(username) and server_user:
            after = "account_id" if "account_id" in plex else "client_id"
            _insert_key_after_inplace(plex, after, "username", server_user)
            username = server_user

    if token and _is_empty(username):
        try:
            response = requests.get("https://plex.tv/api/v2/user", headers=_plex_headers(token), timeout=8)
            if response.ok:
                data = response.json()
                u = (data.get("username") or data.get("title") or "").strip()
                if u:
                    after = "account_id" if "account_id" in plex else "client_id"
                    _insert_key_after_inplace(plex, after, "username", u)
                    username = u
                cid = data.get("id")
                if isinstance(cid, int):
                    plex.setdefault("_cloud", {})["account_id"] = cid
        except Exception as e:  # noqa: BLE001
            _warn("cloud_user_probe_failed", error=str(e))

    save_config(cfg)
    return {"server_url": base, "username": username, "account_id": account_id}


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
            if (account.name or "").strip().lower() == uname.lower() and not _looks_cloudish(account.id):
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
    if cfg_uname and cfg_aid is not None:
        return cfg_uname, cfg_aid
    if cfg_aid is not None:
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


def patch_history_with_account_id(data: Any, account_id: int | None) -> Any:
    if account_id is None:
        return data
    aid = int(account_id)

    def apply(item: Any) -> Any:
        if isinstance(item, dict):
            for key in ("account_id", "accountID", "accountId", "user_id", "userID", "userId"):
                if not item.get(key):
                    item[key] = aid
        return item

    if isinstance(data, list):
        return [apply(it) for it in data]
    return apply(data)

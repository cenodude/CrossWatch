from __future__ import annotations
import os, re, json, time, ipaddress, xml.etree.ElementTree as ET, requests
from requests.exceptions import SSLError, ConnectionError
from typing import Any, Dict, Mapping, Optional, Tuple, List

CONFIG_PATH = "/config/config.json"

def _log(msg: str) -> None:
    if _boolish(os.environ.get("CW_DEBUG"), False) or _boolish(os.environ.get("CW_PLEX_DEBUG"), False):
        print(f"[PLEX:utils] {msg}")

_LIB_TTL_S   = int(os.environ.get("CW_PLEX_LIB_TTL_S",   "600"))
_ACCT_TTL_S  = int(os.environ.get("CW_PLEX_ACCT_TTL_S",  "900"))
_MIN_HTTP_S  = float(os.environ.get("CW_PLEX_MIN_HTTP_INTERVAL_S", "5"))

_CACHE = {
    "libs": {"key": None, "ts": 0.0, "data": []},
    "owner": {"key": None, "ts": 0.0, "data": (None, None)},
    "aid_by_user": {},
}
_LAST_HTTP: Dict[str, float] = {}

def _cache_hit(ts: float, ttl: int) -> bool:
    return (time.time() - float(ts or 0.0)) < max(1, int(ttl))

def _throttle(path: str) -> bool:
    now = time.time()
    last = float(_LAST_HTTP.get(path) or 0.0)
    if (now - last) < max(0.0, _MIN_HTTP_S):
        return True
    _LAST_HTTP[path] = now
    return False

def _read_json(p: str) -> Dict[str, Any]:
    try:
        with open(p, "r", encoding="utf-8") as f: return json.load(f) or {}
    except Exception: return {}

def _write_json_atomic(p: str, data: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(p) or ".", exist_ok=True)
        t = p + ".tmp"
        with open(t, "w", encoding="utf-8") as w: json.dump(data, w, ensure_ascii=False, indent=2)
        os.replace(t, p)
    except Exception as e: _log(f"config write failed: {e}")

def _is_empty(v: Any) -> bool: return v is None or (isinstance(v, str) and v.strip() == "")
def load_config(path: str = CONFIG_PATH) -> Dict[str, Any]: return _read_json(path)
def save_config(cfg: Mapping[str, Any], path: str = CONFIG_PATH) -> None: _write_json_atomic(path, dict(cfg))

def _plex(cfg: Dict[str, Any]) -> Dict[str, Any]:
    if "plex" not in cfg or not isinstance(cfg["plex"], dict): cfg["plex"] = {}
    return cfg["plex"]

def _insert_key_first_inplace(d: Dict[str, Any], k: str, v: Any) -> bool:
    if k in d:
        if d[k] != v: d[k] = v; return True
        return False
    nd = {k: v}; nd.update(d); d.clear(); d.update(nd); return True

def _insert_key_after_inplace(d: Dict[str, Any], after: str, k: str, v: Any) -> bool:
    if k in d:
        if d[k] != v: d[k] = v; return True
        return False
    nd: Dict[str, Any] = {}; ins = False
    for kk, vv in d.items():
        nd[kk] = vv
        if not ins and kk == after: nd[k] = v; ins = True
    if not ins: nd[k] = v
    d.clear(); d.update(nd); return True

def _plex_headers(token: str) -> Dict[str, str]:
    cid = os.environ.get("CW_PLEX_CID") or os.environ.get("PLEX_CLIENT_IDENTIFIER") or "CrossWatch"
    return {"X-Plex-Product":"CrossWatch","X-Plex-Platform":"Web","X-Plex-Version":"1.0","X-Plex-Client-Identifier":cid,"X-Plex-Token":token or "","Accept":"application/xml, application/json;q=0.9,*/*;q=0.5","User-Agent":"CrossWatch/1.0"}

def _boolish(x: Any, default: bool) -> bool:
    if isinstance(x, bool): return x
    if isinstance(x, (int, float)): return bool(x)
    s = str(x).strip().lower()
    if s in ("0","false","no","off","n"): return False
    if s in ("1","true","yes","on","y"): return True
    return default

def _resolve_verify_from_cfg(cfg: Mapping[str, Any], url: str) -> bool:
    if not str(url).lower().startswith("https"): return True
    plex = (cfg.get("plex") or {}) if isinstance(cfg, dict) else {}
    env = os.environ.get("CW_PLEX_VERIFY")
    if env is not None: return _boolish(env, True)
    if "verify_ssl" in plex: return _boolish(plex.get("verify_ssl"), True)
    if "verify_ssl" in cfg:  return _boolish(cfg.get("verify_ssl"), True)
    return True

def _build_session(token: str, verify: bool) -> requests.Session:
    s = requests.Session()
    s.trust_env = False
    s.verify = verify
    s.headers.update(_plex_headers(token))
    return s

_ipplex = re.compile(r"^(https?://)(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct(:\d+)?$", re.I)
def _fallback_bases(base_url: str) -> List[str]:
    out = []
    if base_url.startswith("https://"): out.append("http://" + base_url[8:])
    m = _ipplex.match(base_url)
    if m:
        dotted = m.group(2).replace("-", ".")
        port = m.group(3) or ""
        out.append(f"https://{dotted}{port}")
        out.append(f"http://{dotted}{port}")
    return [b.rstrip("/") for b in out if b]

def _try_get(s: requests.Session, base: str, path: str, timeout: float) -> Optional[requests.Response]:
    url = f"{base.rstrip('/')}{path}"
    try:
        # only log on errors/fallbacks
        return s.get(url, timeout=timeout)
    except (SSLError, ConnectionError) as e:
        _log(f"primary failed: {e}")
        for fb in _fallback_bases(base):
            try:
                _log(f"fallback → {fb}{path}")
                s.verify = fb.startswith("https://") and s.verify
                r = s.get(f"{fb}{path}", timeout=timeout)
                if r is not None:
                    return r
            except Exception as ee:
                _log(f"fallback failed: {ee}")
    except Exception as e:
        _log(f"request error: {e}")
    return None

def discover_server_url_from_server(srv) -> Optional[str]:
    try:
        base = getattr(srv, "_baseurl", None) or getattr(srv, "baseurl", None)
        if isinstance(base, str) and base.strip(): return base.rstrip("/")
    except Exception: pass
    return None

def _pick_server_url_from_resources(xml_text: str) -> str:
    try:
        from urllib.parse import urlparse
        import re, ipaddress

        def host_flags(uri: str):
            # (is_ip_host, is_private_ip)
            h = (urlparse(uri).hostname or "").strip()
            if not h:
                return False, False

            # plain IP host
            try:
                ip = ipaddress.ip_address(h)
                return True, bool(ip.is_private or ip.is_link_local)
            except Exception:
                pass

            # hyphenated plex.direct -> treat as IP
            m = re.match(r"^(\d{1,3}(?:-\d{1,3}){3})\.plex\.direct$", h, re.I)
            if m:
                dotted = m.group(1).replace("-", ".")
                try:
                    ip = ipaddress.ip_address(dotted)
                    return False, bool(ip.is_private or ip.is_link_local)
                except Exception:
                    pass

            return False, False

        root = ET.fromstring(xml_text); servers: List[tuple] = []
        for dev in root.findall(".//Device"):
            if "server" in (dev.attrib.get("provides") or ""):
                for c in dev.findall(".//Connection"):
                    uri = (c.attrib.get("uri") or "").strip()
                    if not uri:
                        continue
                    local = (c.attrib.get("local") or "") in ("1", "true", "yes")
                    relay = (c.attrib.get("relay") or "") in ("1", "true", "yes")
                    direct = not relay
                    http = uri.startswith("http://")
                    is_ip, is_priv = host_flags(uri)

                    # local > direct > private LAN > http > ip-host > everything else
                    servers.append((local, direct, is_priv, http, is_ip, uri.rstrip("/")))

        servers.sort(key=lambda t: (t[0], t[1], t[2], t[3], t[4]), reverse=True)
        return servers[0][5] if servers else ""
    except Exception:
        return ""

def discover_server_url_from_cloud(token: str, timeout: float = 10.0) -> Optional[str]:
    try:
        r = requests.get("https://plex.tv/api/resources?includeHttps=1", headers={"X-Plex-Token": token, "Accept": "application/xml"}, timeout=timeout)
        if r.ok and (r.text or "").lstrip().startswith("<"): return _pick_server_url_from_resources(r.text) or None
    except Exception: pass
    return None

def _pms_id_from_attr_map(m: Mapping[str, Any]) -> Optional[int]:
    try: return int(m.get("id") or m.get("ID"))
    except Exception: return None

def _looks_cloudish(v: Optional[int]) -> bool:
    try: return int(v or -1) >= 100000
    except Exception: return True

def _parse_accounts_all(xml_text: str) -> List[Tuple[int, str]]:
    out: List[Tuple[int, str]] = []
    try:
        root = ET.fromstring(xml_text)
        for acc in root.findall(".//Account"):
            aid = _pms_id_from_attr_map(acc.attrib)
            if aid is None: continue
            nm = (acc.attrib.get("name") or acc.attrib.get("username") or "").strip()
            out.append((aid, nm))
    except Exception: pass
    return out

def _pick_owner_id(a_list: List[Tuple[int, str]]) -> Tuple[Optional[str], Optional[int]]:
    locals_only = [(i, n) for (i, n) in a_list if i > 0 and not _looks_cloudish(i)]
    if not locals_only: return (a_list[0][1], a_list[0][0]) if a_list else (None, None)
    locals_only.sort(key=lambda t: t[0])
    i, n = locals_only[0]
    if any(x[0] == 1 for x in locals_only): i, n = next((ii, nn) for (ii, nn) in locals_only if ii == 1)
    return n, i

def _parse_accounts_xml_for_username(xml_text: str, username: str) -> Optional[int]:
    u = (username or "").strip().lower()
    for aid, nm in _parse_accounts_all(xml_text):
        if (nm or "").lower() == u and not _looks_cloudish(aid): return int(aid)
    return None

def fetch_accounts_owner(base_url: str, token: str, verify: bool, timeout: float = 10.0) -> Tuple[Optional[str], Optional[int]]:
    key = (base_url.rstrip("/"), token or "", bool(verify))
    ent = _CACHE["owner"]
    if ent["key"] == key and _cache_hit(ent["ts"], _ACCT_TTL_S):
        return ent["data"]
    if _throttle("/accounts"):
        return ent["data"]
    out: Tuple[Optional[str], Optional[int]] = (None, None)
    try:
        s = _build_session(token, verify)
        r = _try_get(s, base_url, "/accounts", timeout)
        if r and r.ok and (r.text or "").lstrip().startswith("<"):
            out = _pick_owner_id(_parse_accounts_all(r.text))
    except Exception as e: _log(f"owner fetch failed: {e}")
    _CACHE["owner"] = {"key": key, "ts": time.time(), "data": out}
    return out

def fetch_account_id_for_username(base_url: str, token: str, username: str, verify: bool, timeout: float = 10.0) -> Optional[int]:
    uname = (username or "").strip()
    if not uname: return None
    key = (base_url.rstrip("/"), token or "", uname.lower(), bool(verify))
    ent = _CACHE["aid_by_user"].get(key)
    if ent and _cache_hit(ent.get("ts", 0.0), _ACCT_TTL_S):
        return ent.get("aid")
    if _throttle("/accounts"):
        return ent.get("aid") if ent else None
    aid: Optional[int] = None
    try:
        s = _build_session(token, verify)
        r = _try_get(s, base_url, "/accounts", timeout)
        if r and r.ok and (r.text or "").lstrip().startswith("<"):
            aid = _parse_accounts_xml_for_username(r.text, uname)
    except Exception as e: _log(f"account-id fetch failed: {e}")
    _CACHE["aid_by_user"][key] = {"ts": time.time(), "aid": aid}
    return aid

def inspect_and_persist(cfg_path: str = CONFIG_PATH) -> Dict[str, Any]:
    cfg = load_config(cfg_path); plex = _plex(cfg)
    token = (plex.get("account_token") or "").strip()
    base  = (plex.get("server_url") or "").strip()
    username = plex.get("username") or ""
    account_id = plex.get("account_id")

    if token and not base:
        base = discover_server_url_from_cloud(token) or ""
        if base: _insert_key_first_inplace(plex, "server_url", base); save_config(cfg, cfg_path); _log(f"server_url={base}")

    if token and base:
        verify = _resolve_verify_from_cfg(cfg, base)
        server_user: Optional[str] = None
        server_aid: Optional[int] = None

        if (username or "").strip():
            server_aid = fetch_account_id_for_username(base, token, username, verify=verify)
        if server_aid is None:
            server_user, server_aid = fetch_accounts_owner(base, token, verify=verify)

        if _is_empty(account_id) and (server_aid is not None):
            _insert_key_after_inplace(plex, "client_id", "account_id", int(server_aid))
            account_id = int(server_aid)

        if _is_empty(username) and server_user:
            after = "account_id" if "account_id" in plex else "client_id"
            _insert_key_after_inplace(plex, after, "username", server_user)
            username = server_user

    if token and _is_empty(username):
        try:
            r = requests.get("https://plex.tv/api/v2/user", headers=_plex_headers(token), timeout=8)
            if r.ok:
                j = r.json()
                u = (j.get("username") or j.get("title") or "").strip()
                if u:
                    after = "account_id" if "account_id" in plex else "client_id"
                    _insert_key_after_inplace(plex, after, "username", u)
                    username = u
                cid = j.get("id")
                if isinstance(cid, int):
                    plex.setdefault("_cloud", {})["account_id"] = cid
        except Exception as e:
            _log(f"plex.tv user probe failed: {e}")

    save_config(cfg, cfg_path)
    return {"server_url": base, "username": username, "account_id": account_id}

def fetch_libraries(base_url: str, token: str, verify: bool, timeout: float = 10.0) -> List[Dict[str, Any]]:
    key = (base_url.rstrip("/"), token or "", bool(verify))
    ent = _CACHE["libs"]
    if ent["key"] == key and _cache_hit(ent["ts"], _LIB_TTL_S):
        return list(ent["data"])
    if _throttle("/library/sections"):
        return list(ent["data"])
    libs: List[Dict[str, Any]] = []
    try:
        s = _build_session(token, verify)
        r = _try_get(s, base_url, "/library/sections", timeout)
        if r and r.ok and (r.text or "").lstrip().startswith("<"):
            root = ET.fromstring(r.text)
            for d in root.findall(".//Directory"):
                keyv = d.attrib.get("key"); title = d.attrib.get("title"); typ = d.attrib.get("type")
                if keyv and title:
                    libs.append({"key": str(keyv), "title": title, "type": (typ or "lib")})
    except Exception as e: _log(f"sections fetch failed: {e}")
    _CACHE["libs"] = {"key": key, "ts": time.time(), "data": list(libs)}
    return libs

def fetch_libraries_from_cfg(cfg_path: str = CONFIG_PATH) -> List[Dict[str, Any]]:
    cfg = load_config(cfg_path); plex = _plex(cfg)
    token = (plex.get("account_token") or "").strip()
    base  = (plex.get("server_url") or "").strip()
    if not token: return []
    if not base:
        base = discover_server_url_from_cloud(token) or ""
        if base: _insert_key_first_inplace(plex, "server_url", base); save_config(cfg, cfg_path)
    if not base: return []
    verify = _resolve_verify_from_cfg(cfg, base)
    libs = fetch_libraries(base, token, verify=verify)
    if not libs and verify:
        _log("empty libs; re-try with verify=False")
        libs = fetch_libraries(base, token, verify=False)
    return libs

def resolve_owner_account_id(srv, token: str) -> Optional[int]:
    try:
        accts = (srv.systemAccounts() or [])
        locals_only = [a.id for a in accts if a.id and a.id > 0 and not _looks_cloudish(a.id)]
        if locals_only:
            return 1 if 1 in locals_only else sorted(locals_only)[0]
    except Exception: pass
    try:
        sess = getattr(srv, "_session", None)
        if not sess: return None
        r = sess.get(srv.url("/accounts"), headers=_plex_headers(token), timeout=10)
        if r.ok and (r.text or "").lstrip().startswith("<"):
            n, i = _pick_owner_id(_parse_accounts_all(r.text))
            return i
    except Exception: pass
    return None

def resolve_account_id_by_username(srv, token: str, username: str) -> Optional[int]:
    uname = (username or "").strip()
    if not uname: return None
    try:
        for a in (srv.systemAccounts() or []):
            if (a.name or "").strip().lower() == uname.lower() and not _looks_cloudish(a.id): return int(a.id)
    except Exception: pass
    try:
        sess = getattr(srv, "_session", None)
        if not sess: return None
        r = sess.get(srv.url("/accounts"), headers=_plex_headers(token), timeout=10)
        if r.ok and (r.text or "").lstrip().startswith("<"):
            return _parse_accounts_xml_for_username(r.text, uname)
    except Exception: pass
    return None

def resolve_user_scope(account, srv, token: str, cfg_username: Optional[str], cfg_account_id: Optional[int]) -> Tuple[Optional[str], Optional[int]]:
    if cfg_username and (cfg_account_id is not None): return cfg_username, int(cfg_account_id)
    try: owner_name = getattr(account, "username", None)
    except Exception: owner_name = None
    username = (cfg_username or owner_name or None)
    if cfg_account_id is not None: return username, int(cfg_account_id)
    aid = resolve_account_id_by_username(srv, token, username) if (username and srv) else None
    if aid is None:
        aid = resolve_owner_account_id(srv, token)
    return username, (int(aid) if aid is not None else None)

def persist_server_url_if_empty(path: str, server_url: Optional[str]) -> bool:
    if not server_url or not str(server_url).strip(): return False
    cfg = load_config(path); plex = _plex(cfg)
    if str(plex.get("server_url") or "").strip(): return False
    val = str(server_url).strip().rstrip("/")
    ch = _insert_key_first_inplace(plex, "server_url", val)
    if ch: save_config(cfg, path); _log(f"server_url set -> {val}")
    return ch

def persist_user_scope_if_empty(path: str, username: Optional[str], account_id: Optional[int]) -> None:
    cfg = load_config(path); plex = _plex(cfg); ch = False
    if _is_empty(plex.get("account_id")) and (account_id is not None): ch |= _insert_key_after_inplace(plex, "client_id", "account_id", int(account_id))
    if _is_empty(plex.get("username")) and username:
        after = "account_id" if "account_id" in plex else "client_id"
        ch |= _insert_key_after_inplace(plex, after, "username", username)
    if ch: save_config(cfg, path); _log(f"user scope username={plex.get('username')} account_id={plex.get('account_id')}")

def ensure_whitelist_defaults(cfg_path: str = CONFIG_PATH) -> bool:
    cfg = load_config(cfg_path)
    plex = cfg.setdefault("plex", {})
    changed = False

    if not isinstance(plex.get("history"), dict):
        plex["history"] = {}
        changed = True
    if not isinstance(plex.get("ratings"), dict):
        plex["ratings"] = {}
        changed = True

    if not isinstance(plex["history"].get("libraries"), list):
        plex["history"]["libraries"] = []
        changed = True
    if not isinstance(plex["ratings"].get("libraries"), list):
        plex["ratings"]["libraries"] = []
        changed = True

    for sec in ("history", "ratings"):
        libs = plex[sec]["libraries"]
        norm = sorted({str(x).strip() for x in libs if str(x).strip()})
        if libs != norm:
            plex[sec]["libraries"] = norm
            changed = True

    if changed:
        save_config(cfg, cfg_path)
        _log("whitelist defaults ensured")
    return changed

def patch_history_with_account_id(data: Any, account_id: Optional[int]) -> Any:
    if not account_id: return data
    a = int(account_id)
    def apply(x: Any) -> Any:
        if isinstance(x, dict):
            for k in ("account_id","accountID","accountId","user_id","userID","userId"):
                if not x.get(k): x[k] = a
        return x
    return [apply(i) for i in data] if isinstance(data, list) else apply(data)

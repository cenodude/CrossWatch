# providers/sync/emby/_utils.py
from __future__ import annotations
from typing import Any, Dict, List, Tuple
from urllib.parse import urljoin
import requests

from cw_platform.config_base import load_config, save_config

UA = "CrossWatch/1.0"

def _clean(url: str) -> str:
    u = (url or "").strip()
    if not u: return ""
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "http://" + u
    if not u.endswith("/"): u += "/"
    return u

def _mb_auth(token: str | None, device_id: str) -> str:
    base = f'MediaBrowser Client="CrossWatch", Device="Web", DeviceId="{device_id}", Version="1.0"'
    return f'{base}, Token="{token}"' if token else base

def _headers(token: str | None, device_id: str) -> Dict[str, str]:
    auth = _mb_auth(token, device_id)
    h = {
        "Accept": "application/json",
        "User-Agent": UA,
        "Authorization": auth,
        "X-Emby-Authorization": auth,
    }
    if token:
        h["X-Emby-Token"] = token  # Emby token header
    return h

def ensure_whitelist_defaults() -> None:
    cfg = load_config()
    em = cfg.setdefault("emby", {})
    changed = False
    if "history" not in em or "libraries" not in em.get("history", {}):
        em.setdefault("history", {}).setdefault("libraries", [])
        changed = True
    if "ratings" not in em or "libraries" not in em.get("ratings", {}):
        em.setdefault("ratings", {}).setdefault("libraries", [])
        changed = True
    if changed:
        save_config(cfg)


def _cfg_triplet() -> Tuple[str, str | None, str]:
    cfg = load_config()
    em = cfg.get("emby") or {}
    server = _clean(em.get("server", ""))
    token  = (em.get("access_token") or "").strip() or None
    devid  = (em.get("device_id") or "crosswatch").strip() or "crosswatch"
    return server, token, devid

def inspect_and_persist() -> Dict[str, Any]:
    cfg = load_config()
    em  = cfg.setdefault("emby", {})
    server, token, devid = _cfg_triplet()

    out = {
        "server_url": server or em.get("server", "") or "",
        "username": em.get("user") or em.get("username") or "",
        "user_id": em.get("user_id") or "",
    }

    changed = False
    if server and token:
        try:
            r = requests.get(urljoin(server, "Users/Me"), headers=_headers(token, devid), timeout=8)
            if r.ok:
                me = r.json() or {}
                name = (me.get("Name") or out["username"] or "").strip()
                uid  = (me.get("Id")   or out["user_id"]  or "").strip()
                if name and em.get("user") != name:
                    em["user"] = name; changed = True
                if uid and em.get("user_id") != uid:
                    em["user_id"] = uid; changed = True
                out["username"] = em.get("user") or name
                out["user_id"]  = em.get("user_id") or uid
        except Exception:
            pass

    norm = _clean(em.get("server", "") or server)
    if norm and em.get("server") != norm:
        em["server"] = norm; changed = True
    if changed:
        save_config(cfg)
    out["server_url"] = em.get("server") or out["server_url"]
    return out

def fetch_libraries_from_cfg() -> List[Dict[str, Any]]:
    server, token, devid = _cfg_triplet()
    if not (server and token): return []

    cfg = load_config(); em = cfg.get("emby") or {}
    uid = (em.get("user_id") or "").strip()
    url = urljoin(server, f"Users/{uid}/Views") if uid else urljoin(server, "Library/MediaFolders")
    try:
        r = requests.get(url, headers=_headers(token, devid), timeout=10)
        if not r.ok: return []
        j = r.json() or {}
        items = j.get("Items") or j.get("ItemsList") or j.get("Items") or []
        libs: List[Dict[str, Any]] = []
        for it in items:
            lid   = str(it.get("Id") or it.get("Key") or it.get("Id"))
            title = (it.get("Name") or it.get("Title") or "Library").strip()
            ctyp  = (it.get("CollectionType") or it.get("Type") or "").lower()
            typ   = "movie" if "movie" in ctyp else ("show" if ("series" in ctyp or "tv" in ctyp) else (ctyp or "lib"))
            if lid and title:
                libs.append({"key": lid, "title": title, "type": typ})
        libs.sort(key=lambda x: x["title"].lower())
        return libs
    except Exception:
        return []

# /providers/sync/emby/_utils.py
# EMBY Module for utilities
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from urllib.parse import urljoin

import requests

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, normalize_instance_id

UA = "CrossWatch/1.0"


def _clean(url: str) -> str:
    u = (url or "").strip()
    if not u:
        return ""
    if not (u.startswith("http://") or u.startswith("https://")):
        u = "http://" + u
    if not u.endswith("/"):
        u += "/"
    return u


def _mb_auth(token: str | None, device_id: str) -> str:
    base = f'MediaBrowser Client="CrossWatch", Device="Web", DeviceId="{device_id}", Version="1.0"'
    return f'{base}, Token="{token}"' if token else base


def _headers(token: str | None, device_id: str) -> dict[str, str]:
    auth = _mb_auth(token, device_id)
    h: dict[str, str] = {
        "Accept": "application/json",
        "User-Agent": UA,
        "Authorization": auth,
        "X-Emby-Authorization": auth,
    }
    if token:
        h["X-Emby-Token"] = token
    return h


def _emby(cfg: dict[str, Any], instance_id: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    return ensure_instance_block(cfg, "emby", inst)


def ensure_whitelist_defaults(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> None:
    cfg2 = cfg or load_config()
    em = _emby(cfg2, instance_id)
    changed = False

    if "history" not in em or "libraries" not in (em.get("history") or {}):
        em.setdefault("history", {}).setdefault("libraries", [])
        changed = True

    if "ratings" not in em or "libraries" not in (em.get("ratings") or {}):
        em.setdefault("ratings", {}).setdefault("libraries", [])
        changed = True

    if "scrobble" not in em or "libraries" not in (em.get("scrobble") or {}):
        em.setdefault("scrobble", {}).setdefault("libraries", [])
        changed = True

    if changed:
        save_config(cfg2)


def _cfg_triplet(cfg: dict[str, Any], instance_id: Any) -> tuple[str, str | None, str, bool, float]:
    em = _emby(cfg, instance_id)
    server = _clean(em.get("server", ""))
    token = (em.get("access_token") or "").strip() or None
    devid = (em.get("device_id") or "crosswatch").strip() or "crosswatch"
    verify = bool(em.get("verify_ssl", False))
    timeout = float(em.get("timeout", 15) or 15)
    return server, token, devid, verify, timeout


def inspect_and_persist(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> dict[str, Any]:
    cfg2 = cfg or load_config()
    em = _emby(cfg2, instance_id)
    server, token, devid, verify, timeout = _cfg_triplet(cfg2, instance_id)

    out: dict[str, Any] = {
        "server_url": server or em.get("server", "") or "",
        "username": em.get("user") or em.get("username") or "",
        "user_id": em.get("user_id") or "",
    }

    changed = False
    if server and token:
        try:
            r = requests.get(
                urljoin(server, "Users/Me"),
                headers=_headers(token, devid),
                timeout=timeout,
                verify=verify,
            )
            if r.ok:
                me = r.json() or {}
                name = (me.get("Name") or out["username"] or "").strip()
                uid = (me.get("Id") or out["user_id"] or "").strip()

                if name and em.get("user") != name:
                    em["user"] = name
                    changed = True
                if uid and em.get("user_id") != uid:
                    em["user_id"] = uid
                    changed = True

                out["username"] = em.get("user") or name
                out["user_id"] = em.get("user_id") or uid
        except Exception:
            pass

    norm = _clean(em.get("server", "") or server)
    if norm and em.get("server") != norm:
        em["server"] = norm
        changed = True

    if changed:
        save_config(cfg2)

    out["server_url"] = em.get("server") or out["server_url"]
    return out


def fetch_libraries_from_cfg(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> list[dict[str, Any]]:
    cfg2 = cfg or load_config()
    server, token, devid, verify, timeout = _cfg_triplet(cfg2, instance_id)
    if not (server and token):
        return []

    em = _emby(cfg2, instance_id)
    uid = (em.get("user_id") or "").strip()
    url = urljoin(server, f"Users/{uid}/Views") if uid else urljoin(server, "Library/MediaFolders")

    try:
        r = requests.get(url, headers=_headers(token, devid), timeout=timeout, verify=verify)
        if not r.ok:
            return []

        j = r.json() or {}
        items = j.get("Items") if isinstance(j, dict) else j
        if not isinstance(items, list):
            items = []

        libs: list[dict[str, Any]] = []
        for it in items:
            lid = str((it or {}).get("Id") or (it or {}).get("Key") or "")
            title = ((it or {}).get("Name") or (it or {}).get("Title") or "Library").strip()
            ctyp = ((it or {}).get("CollectionType") or (it or {}).get("Type") or "").lower()
            if "movie" in ctyp:
                typ = "movie"
            elif "series" in ctyp or "tv" in ctyp:
                typ = "show"
            else:
                typ = ctyp or "lib"
            if lid and title:
                libs.append({"key": lid, "title": title, "type": typ})

        libs.sort(key=lambda x: x["title"].lower())
        return libs
    except Exception:
        return []

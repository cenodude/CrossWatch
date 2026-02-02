# providers/sync/jellyfin/_utils.py
# JELLYFIN Module for utilities
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
        h["X-MediaBrowser-Token"] = token
    return h




def _jellyfin(cfg: dict[str, Any], instance_id: Any) -> dict[str, Any]:
    inst = normalize_instance_id(instance_id)
    return ensure_instance_block(cfg, "jellyfin", inst)


def ensure_whitelist_defaults(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> None:
    cfg2 = cfg or load_config()
    jf = _jellyfin(cfg2, instance_id)
    changed = False

    for sec in ("history", "ratings", "scrobble"):
        if sec not in jf or not isinstance(jf.get(sec), dict):
            jf[sec] = {}
            changed = True
        if not isinstance(jf[sec].get("libraries"), list):
            jf[sec]["libraries"] = []
            changed = True

    if changed:
        save_config(cfg2)


def _cfg_triplet(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> tuple[str, str | None, str]:
    cfg2 = cfg or load_config()
    jf = _jellyfin(cfg2, instance_id)
    server = _clean(jf.get("server", ""))
    token = (jf.get("access_token") or "").strip() or None
    devid = (jf.get("device_id") or "crosswatch").strip() or "crosswatch"
    return server, token, devid


def inspect_and_persist(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> dict[str, Any]:
    cfg2 = cfg or load_config()
    jf = _jellyfin(cfg2, instance_id)
    server, token, devid = _cfg_triplet(cfg2, instance_id)

    out: dict[str, Any] = {
        "server_url": server or jf.get("server", "") or "",
        "username": jf.get("user") or jf.get("username") or "",
        "user_id": jf.get("user_id") or "",
    }

    changed = False
    if server and token:
        try:
            r = requests.get(urljoin(server, "Users/Me"), headers=_headers(token, devid), timeout=8)
            if r.ok:
                me = r.json() or {}
                name = (me.get("Name") or out["username"] or "").strip()
                uid = (me.get("Id") or out["user_id"] or "").strip()

                if name and jf.get("user") != name:
                    jf["user"] = name
                    changed = True
                if uid and jf.get("user_id") != uid:
                    jf["user_id"] = uid
                    changed = True

                out["username"] = jf.get("user") or name
                out["user_id"] = jf.get("user_id") or uid
        except Exception:
            pass

    norm = _clean(jf.get("server", "") or server)
    if norm and jf.get("server") != norm:
        jf["server"] = norm
        changed = True

    if changed:
        save_config(cfg2)

    out["server_url"] = jf.get("server") or out["server_url"]
    return out


def fetch_libraries_from_cfg(cfg: dict[str, Any] | None = None, instance_id: Any = None) -> list[dict[str, Any]]:
    cfg2 = cfg or load_config()
    server, token, devid = _cfg_triplet(cfg2, instance_id)
    if not (server and token):
        return []

    jf = _jellyfin(cfg2, instance_id)
    uid = (jf.get("user_id") or "").strip()
    url = urljoin(server, f"Users/{uid}/Views") if uid else urljoin(server, "Library/MediaFolders")

    try:
        r = requests.get(url, headers=_headers(token, devid), timeout=10)
        if not r.ok:
            return []

        j = r.json() or {}
        items = j.get("Items") or j.get("ItemsList") or j.get("Items") or []

        libs: list[dict[str, Any]] = []
        for it in items:
            lid = str(it.get("Id") or it.get("Key") or it.get("Id"))
            title = (it.get("Name") or it.get("Title") or "Library").strip()
            ctyp = (it.get("CollectionType") or it.get("Type") or "").lower()
            typ = (
                "movie"
                if "movie" in ctyp
                else ("show" if ("series" in ctyp or "tv" in ctyp) else (ctyp or "lib"))
            )
            if lid and title:
                libs.append({"key": lid, "title": title, "type": typ})

        libs.sort(key=lambda x: x["title"].lower())
        return libs
    except Exception:
        return []

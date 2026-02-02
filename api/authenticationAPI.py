# /api/authenticationAPI.py
# CrossWatch - Authentication API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Callable, Optional

import importlib
import secrets
import threading
import time
import xml.etree.ElementTree as ET

import requests
from fastapi import Body, Request, HTTPException, Response, Query
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from cw_platform.config_base import load_config, save_config
from cw_platform.provider_instances import ensure_instance_block, ensure_provider_block, normalize_instance_id
from providers.sync.emby._utils import (
    ensure_whitelist_defaults as emby_ensure_whitelist_defaults,
    fetch_libraries_from_cfg as emby_fetch_libraries_from_cfg,
    inspect_and_persist as emby_inspect_and_persist,
)
from providers.sync.jellyfin._utils import (
    ensure_whitelist_defaults as jf_ensure_whitelist_defaults,
    fetch_libraries_from_cfg as jf_fetch_libraries_from_cfg,
    inspect_and_persist as jf_inspect_and_persist,
)
from providers.sync.plex._utils import (
    ensure_whitelist_defaults,
    fetch_libraries_from_cfg,
    inspect_and_persist,
)
import providers.sync.plex._utils as plex_utils

__all__ = ["register_auth"]

# Helpers
def _status_from_msg(msg: str) -> int:
    m = (msg or "").lower()
    if any(x in m for x in ("401", "403", "invalid credential", "unauthor")): return 401
    if "timeout" in m: return 504
    if any(x in m for x in ("dns", "ssl", "connection", "refused", "unreachable", "getaddrinfo", "name or service")): return 502
    return 502

def _import_provider(modname: str, symbol: str = "PROVIDER"):
    try:
        mod = importlib.import_module(modname)
    except ImportError:
        return None
    return getattr(mod, symbol, None)

def _safe_log(fn: Optional[Callable[[str, str], None]], tag: str, msg: str) -> None:
    try:
        if callable(fn): fn(tag, msg)
    except Exception:
        pass
    
def _to_int(val: Any, default: int = 0) -> int:
    try:
        return int(val)
    except Exception:
        return default

def register_auth(app, *, log_fn: Optional[Callable[[str, str], None]] = None, probe_cache: Optional[dict[str, Any]] = None) -> None:
    def _probe_bust(name: str) -> None:
        try:
            if isinstance(probe_cache, dict): probe_cache[name] = (0.0, False)
        except Exception:
            pass

    # provider registry
    try:
        from providers.auth.registry import auth_providers_html, auth_providers_manifests
    except ImportError:
        auth_providers_html = lambda: "<div class='sub'>No providers found.</div>"
        auth_providers_manifests = lambda: []
    
    @app.get("/api/auth/providers", tags=["auth"])
    def api_auth_providers():
        return JSONResponse(auth_providers_manifests())

    @app.get("/api/auth/providers/html", tags=["auth"])
    def api_auth_providers_html():
        return HTMLResponse(auth_providers_html())

    # PLEX
    def plex_request_pin(instance_id: Any) -> dict[str, Any]:
        cfg = load_config()
        inst = normalize_instance_id(instance_id)

        base = ensure_provider_block(cfg, "plex")
        plex = ensure_instance_block(cfg, "plex", inst)

        cid = str((base.get("client_id") or plex.get("client_id") or "")).strip()
        if not cid:
            cid = secrets.token_hex(12)
            base["client_id"] = cid
        plex["client_id"] = cid
        save_config(cfg)

        headers = {
            "Accept": "application/json", "User-Agent": "CrossWatch/1.0",
            "X-Plex-Product": "CrossWatch", "X-Plex-Version": "1.0",
            "X-Plex-Client-Identifier": cid, "X-Plex-Platform": "Web",
        }

        _PLEX_PROVIDER = _import_provider("providers.auth._auth_PLEX")
        code: Optional[str] = None
        pin_id: Optional[int] = None
        try:
            if _PLEX_PROVIDER:
                res = _PLEX_PROVIDER.start(cfg, redirect_uri="", instance_id=inst) or {}
                save_config(cfg)
                code = str((res or {}).get("pin") or "").strip() or None
                pend = (ensure_instance_block(cfg, "plex", inst).get("_pending_pin") or {}) if isinstance(cfg, dict) else {}
                pin_id = pend.get("id")
                if not code:
                    code = pend.get("code")
        except Exception as e:
            raise RuntimeError(f"Plex PIN error: {e}") from e
        if not code or not pin_id:
            raise RuntimeError("Plex PIN could not be issued")

        return {"id": pin_id, "code": code, "expires_epoch": int(time.time()) + 300, "headers": headers}

    
    def plex_wait_for_token(pin_id: int, instance_id: Any, *, timeout_sec: int = 300, interval: float = 1.0) -> Optional[str]:
        inst = normalize_instance_id(instance_id)
        _PLEX_PROVIDER = _import_provider("providers.auth._auth_PLEX")
        deadline = time.time() + max(0, int(timeout_sec))
        sleep_s = max(0.2, float(interval))
        try:
            cfg0 = load_config()
            plex0 = ensure_instance_block(cfg0, "plex", inst)
            pend = plex0.get("_pending_pin") or {}
            if not pend.get("id") and pin_id:
                plex0["_pending_pin"] = {"id": pin_id}
                save_config(cfg0)
        except Exception:
            pass
        while time.time() < deadline:
            cfg = load_config()
            token = str((ensure_instance_block(cfg, "plex", inst).get("account_token") or "")).strip()
            if token:
                return token
            try:
                if _PLEX_PROVIDER:
                    _PLEX_PROVIDER.finish(cfg, instance_id=inst)
                    save_config(cfg)
            except Exception:
                pass
            time.sleep(sleep_s)
        return None

    
    @app.post("/api/plex/pin/new", tags=["auth"])
    def api_plex_pin_new(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        try:
            info = plex_request_pin(inst)
            pin_id, code, exp_epoch = info["id"], info["code"], int(info["expires_epoch"])
            cfg2 = load_config()
            plex2 = ensure_instance_block(cfg2, "plex", inst)
            plex2["_pending_pin"] = {"id": pin_id, "code": code}
            save_config(cfg2)

            def waiter(_pin_id: int, _inst: str):
                token = plex_wait_for_token(_pin_id, _inst, timeout_sec=360, interval=1.0)
                if token:
                    cfg = load_config()
                    plex_cfg = ensure_instance_block(cfg, "plex", _inst)

                    plex_cfg["account_token"] = token
                    existing_url = (plex_cfg.get("server_url") or "").strip()
                    existing_user = (plex_cfg.get("username") or "").strip()
                    existing_aid = str(plex_cfg.get("account_id") or "").strip()
                    need_auto_inspect = not (existing_url or existing_user or existing_aid)

                    save_config(cfg)

                    _safe_log(log_fn, "PLEX", f"\x1b[92m[PLEX:{_inst}]\x1b[0m Token acquired and saved.")
                    _probe_bust("plex")

                    if need_auto_inspect:
                        try:
                            ensure_whitelist_defaults(cfg, instance_id=_inst)
                        except Exception:
                            pass
                        try:
                            inspect_and_persist(cfg, instance_id=_inst)
                        except Exception as e:
                            _safe_log(log_fn, "PLEX", f"[PLEX:{_inst}] auto-inspect failed: {e}")
                else:
                    _safe_log(log_fn, "PLEX", f"\x1b[91m[PLEX:{_inst}]\x1b[0m PIN expired or not authorized.")

            threading.Thread(target=waiter, args=(pin_id, inst), daemon=True).start()
            remaining = max(0, exp_epoch - int(time.time()))
            return {
                "ok": True,
                "code": code,
                "pin_id": pin_id,
                "id": pin_id,
                "expiresIn": remaining,
                "expires_epoch": exp_epoch,
                "instance": inst,
            }
        except Exception as e:
            _safe_log(log_fn, "PLEX", f"[PLEX:{inst}] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    
    @app.get("/api/plex/inspect", tags=["media providers"])
    def plex_inspect(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        ensure_whitelist_defaults(cfg, instance_id=inst)
        out = inspect_and_persist(cfg, instance_id=inst)
        if isinstance(out, dict):
            out["instance"] = inst
            return out
        return {"ok": True, "instance": inst}
    
    
    @app.post("/api/plex/token/delete", tags=["auth"])
    def api_plex_token_delete(instance: str | None = Query(None)) -> dict[str, Any]:
        cfg = load_config()
        inst = normalize_instance_id(instance)
        plex = ensure_instance_block(cfg, "plex", inst)
        plex["account_token"] = ""
        plex.pop("_pending_pin", None)
        save_config(cfg)
        _probe_bust("plex")
        return {"ok": True, "instance": inst}

    
    @app.get("/api/plex/libraries", tags=["media providers"])
    def plex_libraries(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        ensure_whitelist_defaults(cfg, instance_id=inst)
        return {"libraries": fetch_libraries_from_cfg(cfg, instance_id=inst), "instance": inst}

    
    @app.get("/api/plex/pms/probe", tags=["media providers"])
    def plex_pms_probe(timeout: float = 5.0, instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        plex = ensure_instance_block(cfg, "plex", inst)
        token = (plex.get("account_token") or "").strip()
        base = (plex.get("server_url") or "").strip().rstrip("/")

        out: dict[str, Any] = {
            "connected": bool(token),
            "server_url": base or "",
            "reachable": False,
            "status": None,
            "instance": inst,
        }

        if not token:
            out["error"] = "Not connected"
            return out

        if not base:
            try:
                ensure_whitelist_defaults(cfg, instance_id=inst)
                info = inspect_and_persist(cfg, instance_id=inst) or {}
                base = (info.get("server_url") or "").strip().rstrip("/")
                out["server_url"] = base or ""
            except Exception:
                pass

        if not base:
            out["error"] = "No server_url configured"
            return out

        try:
            verify = plex_utils._resolve_verify_from_cfg(cfg, base, instance_id=inst)
            s = plex_utils._build_session(token, verify)
            r = plex_utils._try_get(s, base, "/identity", timeout=float(timeout))
            if r is None:
                out["error"] = "No response"
                return out
            out["status"] = int(r.status_code)
            out["reachable"] = bool(getattr(r, "ok", False))
            if not out["reachable"] and verify:
                s2 = plex_utils._build_session(token, False)
                r2 = plex_utils._try_get(s2, base, "/identity", timeout=float(timeout))
                if r2 is not None and getattr(r2, "ok", False):
                    out["reachable"] = True
                    out["status"] = int(r2.status_code)
                    out["verify_ssl"] = False
        except Exception as e:
            out["error"] = str(e)
        return out


    @app.get("/api/plex/pickusers", tags=["media providers"])
    def plex_pickusers(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        plex = ensure_instance_block(cfg, "plex", inst)
        token = (plex.get("account_token") or "").strip()
        base = (plex.get("server_url") or "").strip()
        if not token:
            return {"users": [], "count": 0, "instance": inst}

        norm = lambda s: (s or "").strip().lower()
        is_local_id = (
            lambda x: (isinstance(x, int) and 0 < x < 100000)
            or (str(x).isdigit() and 0 < int(x) < 100000)
        )
        rank = {"owner": 0, "managed": 1, "friend": 2}

        pms_by_cloud: dict[int, dict[str, Any]] = {}
        pms_rows = []
        if base:
            verify = plex_utils._resolve_verify_from_cfg(cfg, base, instance_id=inst)
            s = plex_utils._build_session(token, verify)
            r = plex_utils._try_get(s, base, "/accounts", timeout=10.0)
            if r and r.ok and (r.text or "").lstrip().startswith("<"):
                try:
                    root = ET.fromstring(r.text)
                    for acc in root.findall(".//Account"):
                        pid = str(acc.attrib.get("id") or acc.attrib.get("ID") or "").strip()
                        pms_id = _to_int(pid) if is_local_id(pid) else None
                        try:
                            cloud_id = _to_int(
                                acc.attrib.get("accountID")
                                or acc.attrib.get("accountId")
                                or acc.attrib.get("account_id")
                                or acc.attrib.get("cloudID")
                                or acc.attrib.get("cloudId")
                                or acc.attrib.get("cloud_id")
                            )
                        except Exception:
                            cloud_id = None

                        if cloud_id is not None and int(cloud_id) <= 0:
                            cloud_id = None
                        # Friends/shared users frequently show up with a cloud-ish "id" on /accounts.
                        if cloud_id is None and pid.isdigit() and not is_local_id(pid):
                            cloud_id = _to_int(pid, 0) or None

                        if not (pms_id or cloud_id):
                            continue

                        uname = (acc.attrib.get("name") or acc.attrib.get("username") or acc.attrib.get("title") or "").strip()
                        kind = (acc.attrib.get("type") or "").strip().lower()
                        if kind not in ("owner", "managed", "friend"):
                            kind = "friend"

                        row = {
                            "pms_account_id": pms_id,
                            "cloud_account_id": cloud_id,
                            "username": uname,
                            "type": kind,
                            "label": ("Owner" if kind == "owner" else ("Managed" if kind == "managed" else "Friend")),
                        }
                        pms_rows.append(row)
                        if cloud_id is not None:
                            pms_by_cloud[int(cloud_id)] = row
                except Exception:
                    pass

        try:
            cloud = plex_utils.fetch_cloud_user_info(token) or {}
            cloud_user = (cloud.get("username") or cloud.get("title") or "").strip()
            cloud_id = cloud.get("id")
        except Exception:
            cloud_user = ""
            cloud_id = None

        users: list[dict[str, Any]] = []
        seen: set[tuple[str, int]] = set()

        def add(u: dict[str, Any]) -> None:
            uname = str(u.get("username") or u.get("title") or "").strip()
            try:
                aid = int(u.get("account_id") or u.get("id") or 0)
            except Exception:
                aid = 0
            if aid <= 0 and (u.get("cloud_account_id") or u.get("pms_account_id")):
                try:
                    aid = int(u.get("cloud_account_id") or u.get("pms_account_id") or 0)
                except Exception:
                    aid = 0
            if aid <= 0:
                return
            key = (uname.lower(), aid)
            if key in seen:
                return
            seen.add(key)
            u["username"] = uname
            u["account_id"] = aid
            u.setdefault("id", aid)
            users.append(u)

        if cloud_user:
            add({"username": cloud_user, "account_id": cloud_id or 1, "type": "owner", "label": "Owner", "source": "cloud"})

        for u in (plex_utils.fetch_cloud_home_users(token) or []):
            add({"username": u.get("username") or u.get("title") or "", "account_id": u.get("id"), "type": u.get("type") or "managed", "label": "Home", "source": "cloud"})

        # Friends / shared users: plex.tv/api/users
        for u in (plex_utils.fetch_cloud_account_users(token) or []):
            add({"username": u.get("username") or u.get("title") or "", "account_id": u.get("id"), "type": "friend", "label": "Friend", "source": "cloud"})
        if cloud_id is not None and int(cloud_id) in pms_by_cloud:
            r0 = pms_by_cloud[int(cloud_id)]
            add({"username": r0.get("username") or cloud_user, "account_id": cloud_id, "type": r0.get("type") or "owner", "label": r0.get("label") or "Owner", "source": "pms"})

        for row in pms_rows:
            add({
                "username": row.get("username") or "",
                "account_id": row.get("cloud_account_id") or row.get("pms_account_id") or 1,
                "type": row.get("type") or "friend",
                "label": row.get("label") or "Friend",
                "source": "pms",
            })

        users.sort(key=lambda x: (rank.get(str(x.get("type") or "friend"), 9), str(x.get("username") or "").lower()))
        return {"users": users, "count": len(users), "instance": inst}


    @app.get("/api/plex/users", tags=["media providers"])
    def plex_users(instance: str | None = Query(None)) -> dict[str, Any]:
        return plex_pickusers(instance=instance)

    # JELLYFIN
    @app.post("/api/jellyfin/login", tags=["auth"])
    def api_jellyfin_login(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> JSONResponse:
        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "Malformed request"}, 400)

        inst = normalize_instance_id(instance)
        cfg = load_config()
        ensure_provider_block(cfg, "jellyfin")
        jf = ensure_instance_block(cfg, "jellyfin", inst)

        for k in ("server", "username", "password"):
            v = (payload.get(k) or "").strip()
            if v:
                jf[k] = v
        if "verify_ssl" in payload:
            jf["verify_ssl"] = bool(payload.get("verify_ssl"))

        if not all(jf.get(k) for k in ("server", "username", "password")):
            return JSONResponse({"ok": False, "error": "Missing: server/username/password"}, 400)

        try:
            prov = _import_provider("providers.auth._auth_JELLYFIN")
            if not prov:
                return JSONResponse({"ok": False, "error": "Provider missing"}, 500)
            res = prov.start(cfg, redirect_uri="", instance_id=inst)
            save_config(cfg)

            if res.get("ok"):
                jf2 = ensure_instance_block(cfg, "jellyfin", inst)
                return JSONResponse(
                    {
                        "ok": True,
                        "user_id": res.get("user_id") or jf2.get("user_id") or "",
                        "username": jf2.get("user") or jf2.get("username") or None,
                        "server": jf2.get("server") or None,
                        "instance": inst,
                    },
                    200,
                )

            msg = res.get("error") or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))
        except Exception as e:
            msg = str(e) or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))

    @app.post("/api/jellyfin/token/delete", tags=["auth"])
    def api_jellyfin_token_delete(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        jf = ensure_instance_block(cfg, "jellyfin", inst)
        jf["access_token"] = ""
        save_config(cfg)
        return {"ok": True}

    @app.get("/api/jellyfin/status", tags=["auth"])
    def api_jellyfin_status(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        jf = ensure_instance_block(cfg, "jellyfin", inst)
        return {
            "connected": bool(jf.get("access_token") and jf.get("server")),
            "user": jf.get("user") or jf.get("username") or None,
            "instance": inst,
        }

    @app.get("/api/jellyfin/inspect", tags=["media providers"])
    def jf_inspect(instance: str | None = Query(None)):
        cfg = load_config()
        jf_ensure_whitelist_defaults(cfg, instance_id=instance)
        return jf_inspect_and_persist(cfg, instance_id=instance)

    @app.get("/api/jellyfin/libraries", tags=["media providers"])
    def jf_libraries(instance: str | None = Query(None)):
        cfg = load_config()
        inst = normalize_instance_id(instance)
        jf_ensure_whitelist_defaults(cfg, instance_id=instance)
        return {
            "libraries": jf_fetch_libraries_from_cfg(cfg, instance_id=instance),
            "instance": inst,
        }

    @app.get("/api/jellyfin/users", tags=["media providers"])
    def jf_users(instance: str | None = Query(None)):
        inst = normalize_instance_id(instance)
        cfg = load_config()
        jf = ensure_instance_block(cfg, "jellyfin", inst)
        server = str((jf.get("server") or "")).rstrip("/")
        token = str((jf.get("access_token") or "")).strip()
        if not server or not token:
            return JSONResponse({"ok": False, "error": "Not connected to Jellyfin"}, 401)

        timeout = float(jf.get("timeout", 15) or 15)
        verify = bool(jf.get("verify_ssl", False))
        devid = str(jf.get("device_id") or "crosswatch").strip() or "crosswatch"

        base = f'MediaBrowser Client="CrossWatch", Device="Web", DeviceId="{devid}", Version="1.0"'
        auth = f'{base}, Token="{token}"'
        headers = {
            "Accept": "application/json",
            "User-Agent": "CrossWatch/1.0",
            "Authorization": auth,
            "X-Emby-Authorization": auth,
            "X-MediaBrowser-Token": token,
        }

        users: list[dict[str, Any]] = []
        try:
            r = requests.get(f"{server}/Users", headers=headers, timeout=timeout, verify=verify)
            if r.ok:
                data = r.json() or []
                arr = data.get("Items") if isinstance(data, dict) else data
                if isinstance(arr, list):
                    for u in arr:
                        pol = (u or {}).get("Policy") if isinstance((u or {}).get("Policy"), dict) else {}
                        users.append(
                            {
                                "id": (u or {}).get("Id") or (u or {}).get("id"),
                                "username": (u or {}).get("Name") or (u or {}).get("name") or "",
                                "IsAdministrator": bool((pol or {}).get("IsAdministrator"))
                                if isinstance(pol, dict)
                                else bool((u or {}).get("IsAdministrator") or False),
                                "IsHidden": bool((pol or {}).get("IsHidden"))
                                if isinstance(pol, dict)
                                else bool((u or {}).get("IsHidden") or False),
                                "IsDisabled": bool((pol or {}).get("IsDisabled"))
                                if isinstance(pol, dict)
                                else bool((u or {}).get("IsDisabled") or False),
                            }
                        )
            else:
                mr = requests.get(f"{server}/Users/Me", headers=headers, timeout=timeout, verify=verify)
                if mr.ok:
                    me = mr.json() or {}
                    users = [{"id": me.get("Id") or me.get("id"), "username": me.get("Name") or me.get("name") or ""}]
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, _status_from_msg(str(e)))

        users = [u for u in users if (u or {}).get("username")]
        return {"users": users, "count": len(users), "instance": inst}

    # EMBY
    @app.post("/api/emby/login", tags=["auth"])
    def api_emby_login(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> JSONResponse:
        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "Malformed request"}, 400)

        inst = normalize_instance_id(instance)
        cfg = load_config()
        base = ensure_provider_block(cfg, "emby")
        em = ensure_instance_block(cfg, "emby", inst)

        for k in ("server", "username", "password"):
            v = (payload.get(k) or "").strip()
            if v:
                em[k] = v

        if "verify_ssl" in payload:
            em["verify_ssl"] = bool(payload.get("verify_ssl"))
        if "timeout" in payload:
            em["timeout"] = _to_int(payload.get("timeout"), 15)

        if not all(em.get(k) for k in ("server", "username", "password")):
            return JSONResponse({"ok": False, "error": "Missing: server/username/password"}, 400)

        try:
            prov = _import_provider("providers.auth._auth_EMBY")
            if not prov:
                return JSONResponse({"ok": False, "error": "Provider missing"}, 500)

            res = prov.start(cfg, redirect_uri="", instance_id=inst)  # type: ignore[attr-defined]
            save_config(cfg)

            em2 = ensure_instance_block(cfg, "emby", inst)
            if isinstance(res, dict) and res.get("ok"):
                return JSONResponse(
                    {
                        "ok": True,
                        "user_id": res.get("user_id") or em2.get("user_id") or "",
                        "username": em2.get("user") or em2.get("username") or "",
                        "server": em2.get("server") or "",
                        "instance": inst,
                    },
                    200,
                )

            msg = (res or {}).get("error") if isinstance(res, dict) else None
            msg = msg or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))
        except Exception as e:
            msg = str(e) or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))

    @app.get("/api/emby/status", tags=["auth"])
    def api_emby_status(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        em = ensure_instance_block(cfg, "emby", inst)
        return {
            "connected": bool(em.get("access_token") and em.get("server")),
            "user": em.get("user") or em.get("username") or None,
            "instance": inst,
        }

    @app.post("/api/emby/token/delete", tags=["auth"])
    def api_emby_token_delete(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        em = ensure_instance_block(cfg, "emby", inst)
        em["access_token"] = ""
        save_config(cfg)
        _probe_bust("emby")
        return {"ok": True, "instance": inst}

    @app.get("/api/emby/inspect", tags=["media providers"])
    def emby_inspect(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        try:
            emby_ensure_whitelist_defaults(cfg, instance_id=inst)
        except TypeError:
            emby_ensure_whitelist_defaults()
        out: dict[str, Any]
        try:
            out = emby_inspect_and_persist(cfg, instance_id=inst)  # type: ignore[arg-type]
        except TypeError:
            out = emby_inspect_and_persist()
        try:
            if not (out or {}).get("user_id"):
                em = ensure_instance_block(cfg, "emby", inst)
                server = (em.get("server") or "").rstrip("/")
                token = (em.get("access_token") or "").strip()
                if server and token:
                    r = requests.get(
                        f"{server}/Users/Me",
                        headers={"X-Emby-Token": token, "Accept": "application/json"},
                        timeout=float(em.get("timeout", 15) or 15),
                        verify=bool(em.get("verify_ssl", False)),
                    )
                    if r.ok:
                        me = r.json() or {}
                        out = dict(out or {})
                        out.setdefault("user_id", me.get("Id") or me.get("id") or "")
                        out.setdefault("username", me.get("Name") or me.get("name") or "")
        except Exception:
            pass
        out = dict(out or {})
        out["instance"] = inst
        return out

    @app.get("/api/emby/libraries", tags=["media providers"])
    def emby_libraries(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        try:
            emby_ensure_whitelist_defaults(cfg, instance_id=inst)
        except TypeError:
            emby_ensure_whitelist_defaults()
        try:
            libs = emby_fetch_libraries_from_cfg(cfg, instance_id=inst)  # type: ignore[arg-type]
        except TypeError:
            libs = emby_fetch_libraries_from_cfg()
        return {"libraries": libs, "instance": inst}

    @app.get("/api/emby/users", tags=["media providers"], response_model=None)
    def emby_users(instance: str | None = Query(None)) -> dict[str, Any] | JSONResponse:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        em = ensure_instance_block(cfg, "emby", inst)

        server = (em.get("server") or "").rstrip("/")
        token = (em.get("access_token") or "").strip()
        if not server or not token:
            return JSONResponse({"ok": False, "error": "Not connected to Emby"}, 401)

        timeout = float(em.get("timeout", 15) or 15)
        verify = bool(em.get("verify_ssl", False))
        headers = {"X-Emby-Token": token, "Accept": "application/json"}

        users: list[dict[str, Any]] = []
        try:
            r = requests.get(f"{server}/Users", headers=headers, timeout=timeout, verify=verify)
            if r.ok:
                data = r.json() or []
                arr = data.get("Items") if isinstance(data, dict) else data
                if isinstance(arr, list):
                    for u in arr:
                        pol = (u or {}).get("Policy") if isinstance((u or {}).get("Policy"), dict) else {}
                        users.append(
                            {
                                "id": (u or {}).get("Id") or (u or {}).get("id"),
                                "username": (u or {}).get("Name") or (u or {}).get("name") or "",
                                "IsAdministrator": bool((pol or {}).get("IsAdministrator")) if isinstance(pol, dict) else bool((u or {}).get("IsAdministrator") or False),
                                "IsHidden": bool((pol or {}).get("IsHidden")) if isinstance(pol, dict) else bool((u or {}).get("IsHidden") or False),
                                "IsDisabled": bool((pol or {}).get("IsDisabled")) if isinstance(pol, dict) else bool((u or {}).get("IsDisabled") or False),
                            }
                        )
            else:
                mr = requests.get(f"{server}/Users/Me", headers=headers, timeout=timeout, verify=verify)
                if mr.ok:
                    me = mr.json() or {}
                    users = [{"id": me.get("Id") or me.get("id"), "username": me.get("Name") or me.get("name") or ""}]
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, _status_from_msg(str(e)))

        users = [u for u in users if (u or {}).get("username")]
        return {"users": users, "count": len(users), "instance": inst}

    # TMDB
    @app.post("/api/tmdb/save", tags=["auth"])
    def api_tmdb_save(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        try:
            key = str((payload or {}).get("api_key") or "").strip()
            cfg = load_config(); cfg.setdefault("tmdb", {})["api_key"] = key
            save_config(cfg)
            _safe_log(log_fn, "TMDB", "[TMDB] api_key saved")
            if isinstance(probe_cache, dict): probe_cache["tmdb"] = (0.0, False)
            return {"ok": True}
        except Exception as e:
            _safe_log(log_fn, "TMDB", f"[TMDB] ERROR save: {e}")
            return {"ok": False, "error": str(e)}

    @app.post("/api/tmdb/disconnect", tags=["auth"])
    def api_tmdb_disconnect() -> dict[str, Any]:
        try:
            cfg = load_config(); cfg.setdefault("tmdb", {})["api_key"] = ""
            save_config(cfg)
            _safe_log(log_fn, "TMDB", "[TMDB] disconnected")
            if isinstance(probe_cache, dict): probe_cache["tmdb"] = (0.0, False)
            return {"ok": True}
        except Exception as e:
            _safe_log(log_fn, "TMDB", f"[TMDB] ERROR disconnect: {e}")
            return {"ok": False, "error": str(e)}

    # TMDB Sync (v3 session)
    TMDB_API_BASE = "https://api.themoviedb.org/3"

    def _tmdb_v3_request_token(api_key: str) -> dict[str, Any]:
        r = requests.get(f"{TMDB_API_BASE}/authentication/token/new", params={"api_key": api_key}, timeout=15)
        r.raise_for_status()
        return r.json() or {}

    def _tmdb_v3_create_session(api_key: str, request_token: str) -> dict[str, Any]:
        r = requests.post(
            f"{TMDB_API_BASE}/authentication/session/new",
            params={"api_key": api_key},
            json={"request_token": request_token},
            timeout=15,
        )
        r.raise_for_status()
        return r.json() or {}

    def _tmdb_v3_account(api_key: str, session_id: str) -> dict[str, Any]:
        r = requests.get(f"{TMDB_API_BASE}/account", params={"api_key": api_key, "session_id": session_id}, timeout=15)
        r.raise_for_status()
        return r.json() or {}

    @app.post("/api/tmdb_sync/connect/start", tags=["auth"])
    def api_tmdb_sync_connect_start(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        key = str((payload or {}).get("api_key") or "").strip()
        if not key:
            return {"ok": False, "error": "Missing api_key"}
        try:
            cfg = load_config()
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            tm["api_key"] = key

            j = _tmdb_v3_request_token(key)
            token = str((j or {}).get("request_token") or "").strip()
            if not token:
                return {"ok": False, "error": "TMDb did not return a request token"}
            tm["_pending_request_token"] = token
            tm.pop("_pending_request_token_ts", None)
            tm.pop("_pending_created_at", None)
            tm["_pending_created_at"] = int(time.time())
            save_config(cfg)

            _probe_bust("tmdb_sync")
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] request_token issued instance={inst}")
            return {
                "ok": True,
                "request_token": token,
                "auth_url": f"https://www.themoviedb.org/authenticate/{token}",
                "expires_at": (j or {}).get("expires_at"),
                "instance": inst,
            }
        except Exception as e:
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] ERROR connect/start: {e}")
            return {"ok": False, "error": str(e)}

    @app.post("/api/tmdb_sync/connect/finish", tags=["auth"])
    def api_tmdb_sync_connect_finish(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        try:
            cfg = load_config()
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            key = str((payload or {}).get("api_key") or tm.get("api_key") or "").strip()
            token = str((payload or {}).get("request_token") or tm.get("_pending_request_token") or "").strip()
            if not key:
                return {"ok": False, "error": "Missing api_key"}
            if not token:
                return {"ok": False, "error": "Missing request_token. Click Connect first."}

            j = _tmdb_v3_create_session(key, token)
            sess = str((j or {}).get("session_id") or "").strip()
            if not sess:
                return {"ok": False, "error": "TMDb did not return a session id. Did you approve the request?"}

            tm["api_key"] = key
            tm["session_id"] = sess
            tm.pop("_pending_request_token", None)
            tm.pop("_pending_request_token_ts", None)
            tm.pop("_pending_created_at", None)

            try:
                me = _tmdb_v3_account(key, sess)
                tm["account_id"] = str((me or {}).get("id") or "").strip()
                tm["username"] = str((me or {}).get("username") or "").strip()
            except Exception:
                pass

            save_config(cfg)
            _probe_bust("tmdb_sync")
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] session created instance={inst}")
            return {"ok": True, "session_id": sess, "account_id": tm.get("account_id") or "", "instance": inst}
        except Exception as e:
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] ERROR connect/finish: {e}")
            return {"ok": False, "error": str(e)}

    @app.post("/api/tmdb_sync/save", tags=["auth"])
    def api_tmdb_sync_save(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        try:
            key = str((payload or {}).get("api_key") or "").strip()
            sess = str((payload or {}).get("session_id") or "").strip()
            cfg = load_config()
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            tm["api_key"] = key
            tm["session_id"] = sess
            tm.pop("_pending_request_token", None)
            tm.pop("_pending_request_token_ts", None)
            tm.pop("_pending_created_at", None)
            save_config(cfg)
            _probe_bust("tmdb_sync")
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] credentials saved instance={inst}")
            return {"ok": True, "instance": inst}
        except Exception as e:
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] ERROR save: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/api/tmdb_sync/verify", tags=["auth"])
    def api_tmdb_sync_verify(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        try:
            cfg = load_config()
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            key = str((tm or {}).get("api_key") or "").strip()
            sess = str((tm or {}).get("session_id") or "").strip()
            token = str((tm or {}).get("_pending_request_token") or "").strip()

            if not key:
                return {"ok": True, "connected": False, "pending": bool(token), "error": "Missing api_key", "instance": inst}

            if not sess and token:
                try:
                    j = _tmdb_v3_create_session(key, token)
                    new_sess = str((j or {}).get("session_id") or "").strip()
                    if new_sess:
                        tm["session_id"] = new_sess
                        tm.pop("_pending_request_token", None)
                        tm.pop("_pending_request_token_ts", None)
                        tm.pop("_pending_created_at", None)
                        try:
                            me = _tmdb_v3_account(key, new_sess)
                            tm["account_id"] = str((me or {}).get("id") or "").strip()
                            tm["username"] = str((me or {}).get("username") or "").strip()
                        except Exception:
                            pass
                        save_config(cfg)
                        _probe_bust("tmdb_sync")
                        _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] auto-finish: session created instance={inst}")
                        sess = new_sess
                except Exception:
                    return {"ok": True, "connected": False, "pending": True, "error": "", "instance": inst}

            if not sess:
                return {"ok": True, "connected": False, "pending": bool(token), "error": "Missing session_id", "instance": inst}

            me = _tmdb_v3_account(key, sess)
            acc_id = str((me or {}).get("id") or "").strip()
            if acc_id and str((tm or {}).get("account_id") or "").strip() != acc_id:
                tm["account_id"] = acc_id
                tm["username"] = str((me or {}).get("username") or "").strip()
                try:
                    save_config(cfg)
                except Exception:
                    pass
            return {"ok": True, "connected": True, "pending": False, "account": {"id": (me or {}).get("id"), "username": (me or {}).get("username")}, "instance": inst}
        except Exception as e:
            return {"ok": False, "connected": False, "pending": False, "error": str(e), "instance": inst}

    @app.post("/api/tmdb_sync/disconnect", tags=["auth"])
    def api_tmdb_sync_disconnect(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        try:
            cfg = load_config()
            tm = ensure_instance_block(cfg, "tmdb_sync", inst)
            tm["api_key"] = ""
            tm["session_id"] = ""
            tm.pop("account_id", None)
            tm.pop("username", None)
            tm.pop("_pending_request_token", None)
            tm.pop("_pending_request_token_ts", None)
            tm.pop("_pending_created_at", None)
            save_config(cfg)
            _probe_bust("tmdb_sync")
            _safe_log(log_fn, "TMDB_SYNC", f"[TMDB_SYNC] disconnected instance={inst}")
            return {"ok": True, "instance": inst}
        except Exception as e:
            return {"ok": False, "error": str(e), "instance": inst}

    @app.post("/api/mdblist/save", tags=["auth"])
    def api_mdblist_save(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> dict[str, Any]:
        try:
            key = str((payload or {}).get("api_key") or "").strip()
            cfg = load_config()
            m = ensure_instance_block(cfg, "mdblist", instance)
            m["api_key"] = key
            save_config(cfg)
            _safe_log(log_fn, "MDBLIST", f"[MDBLIST] api_key saved instance={normalize_instance_id(instance)}")
            if isinstance(probe_cache, dict):
                probe_cache["mdblist"] = (0.0, False)
            return {"ok": True, "instance": normalize_instance_id(instance)}
        except Exception as e:
            _safe_log(log_fn, "MDBLIST", f"[MDBLIST] ERROR save: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/api/mdblist/status", tags=["auth"])
    def api_mdblist_status(instance: str | None = Query(None)) -> dict[str, Any]:
        cfg = load_config()
        m = ensure_instance_block(cfg, "mdblist", instance)
        has = bool(str(m.get("api_key") or "").strip())
        return {"connected": has, "instance": normalize_instance_id(instance)}

    @app.post("/api/mdblist/disconnect", tags=["auth"])
    def api_mdblist_disconnect(instance: str | None = Query(None)) -> dict[str, Any]:
        try:
            cfg = load_config()
            m = ensure_instance_block(cfg, "mdblist", instance)
            m["api_key"] = ""
            save_config(cfg)
            _safe_log(log_fn, "MDBLIST", f"[MDBLIST] disconnected instance={normalize_instance_id(instance)}")
            if isinstance(probe_cache, dict):
                probe_cache["mdblist"] = (0.0, False)
            return {"ok": True, "instance": normalize_instance_id(instance)}
        except Exception as e:
            _safe_log(log_fn, "MDBLIST", f"[MDBLIST] ERROR disconnect: {e}")
            return {"ok": False, "error": str(e)}


    # TAUTULLI
    @app.post("/api/tautulli/save", tags=["auth"])
    def api_tautulli_save(payload: dict[str, Any] = Body(...), instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        server = str((payload or {}).get("server_url") or (payload or {}).get("server") or "").strip().rstrip("/")
        key_in = str((payload or {}).get("api_key") or (payload or {}).get("key") or "").strip()
        user_id = str((payload or {}).get("user_id") or ((payload or {}).get("history") or {}).get("user_id") or "").strip()

        if server and not server.startswith(("http://", "https://")):
            server = "http://" + server

        cfg = load_config()
        t = ensure_instance_block(cfg, "tautulli", inst)

        if server:
            t["server_url"] = server
        if key_in and key_in not in ("••••••••", "********", "**********"):
            t["api_key"] = key_in

        t.setdefault("history", {})
        if (
            "user_id" in (payload or {})
            or (
                "history" in (payload or {})
                and isinstance((payload or {}).get("history"), dict)
                and "user_id" in (((payload or {}).get("history") or {}))
            )
        ):
            t["history"]["user_id"] = user_id

        final_server = str(t.get("server_url") or "").strip()
        final_key = str(t.get("api_key") or "").strip()

        if not final_server:
            raise HTTPException(status_code=400, detail="server_url required")
        if not final_key:
            raise HTTPException(status_code=400, detail="api_key required")

        save_config(cfg)
        _safe_log(log_fn, "TAUTULLI", f"[TAUTULLI] saved instance={inst}")
        if isinstance(probe_cache, dict):
            probe_cache["tautulli"] = (0.0, False)
        return {"ok": True, "server_url": final_server, "has_key": bool(final_key), "instance": inst}

    @app.get("/api/tautulli/status", tags=["auth"])
    def api_tautulli_status(instance: str | None = Query(None), verify: int | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        t = ensure_instance_block(cfg, "tautulli", inst)

        server = str(t.get("server_url") or "").strip().rstrip("/")
        key = str(t.get("api_key") or "").strip()
        if not server or not key:
            return {"connected": False, "instance": inst}

        if not verify:
            return {"connected": True, "instance": inst}

        if server and not server.startswith(("http://", "https://")):
            server = "http://" + server

        try:
            r = requests.get(
                f"{server}/api/v2",
                params={"apikey": key, "cmd": "get_server_info"},
                headers={"Accept": "application/json"},
                timeout=float(t.get("timeout", 10) or 10),
                verify=bool(t.get("verify_ssl", True)),
            )
            j = {}
            try:
                j = r.json() if r.ok else {}
            except Exception:
                j = {}
            resp = j.get("response") if isinstance(j, dict) else None
            ok = bool(
                r.ok
                and isinstance(resp, dict)
                and str(resp.get("result") or "").lower() == "success"
            )
            if ok:
                return {"connected": True, "instance": inst}
            reason = "verify_failed"
            if isinstance(resp, dict):
                reason = str(resp.get("message") or reason)
            elif not r.ok:
                reason = f"HTTP {r.status_code}"
            return {"connected": False, "instance": inst, "reason": reason}
        except Exception as e:
            return {"connected": False, "instance": inst, "reason": str(e)}

    @app.post("/api/tautulli/disconnect", tags=["auth"])
    def api_tautulli_disconnect(instance: str | None = Query(None)) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        try:
            cfg = load_config()
            t = ensure_instance_block(cfg, "tautulli", inst)
            t["server_url"] = ""
            t["api_key"] = ""
            save_config(cfg)
            _safe_log(log_fn, "TAUTULLI", f"[TAUTULLI] disconnected instance={inst}")
            if isinstance(probe_cache, dict):
                probe_cache["tautulli"] = (0.0, False)
            return {"ok": True, "instance": inst}
        except Exception as e:
            _safe_log(log_fn, "TAUTULLI", f"[TAUTULLI] ERROR disconnect: {e}")
            return {"ok": False, "error": str(e), "instance": inst}



    # TRAKT
    def trakt_request_pin(instance_id: Any) -> dict[str, Any]:
        prov = _import_provider("providers.auth._auth_TRAKT")
        if not prov:
            raise RuntimeError("Trakt provider not available")

        cfg = load_config()
        inst = normalize_instance_id(instance_id)
        # Ensure instance has client keys so pair config views remain self-contained.
        base = ensure_provider_block(cfg, "trakt")
        tr = ensure_instance_block(cfg, "trakt", inst)
        if base.get("client_id") and not tr.get("client_id"):
            tr["client_id"] = base.get("client_id")
        if base.get("client_secret") and not tr.get("client_secret"):
            tr["client_secret"] = base.get("client_secret")
        res = prov.start(cfg, redirect_uri="", instance_id=inst)  # type: ignore[attr-defined]
        save_config(cfg)

        pend = (ensure_instance_block(cfg, "trakt", inst).get("_pending_device") or {}) if isinstance(cfg, dict) else {}
        user_code = pend.get("user_code") or (res or {}).get("user_code")
        device_code = pend.get("device_code") or (res or {}).get("device_code")
        verification_url = (
            pend.get("verification_url")
            or (res or {}).get("verification_url")
            or "https://trakt.tv/activate"
        )
        exp_epoch = int((pend.get("expires_at") or 0) or (time.time() + 600))

        if not user_code or not device_code:
            raise RuntimeError("Trakt PIN could not be issued")

        return {
            "user_code": user_code,
            "device_code": device_code,
            "verification_url": verification_url,
            "expires_epoch": exp_epoch,
        }

    def trakt_wait_for_token(
        device_code: str,
        *,
        instance_id: Any,
        timeout_sec: int = 600,
        interval: float = 2.0,
    ) -> Optional[str]:
        prov = _import_provider("providers.auth._auth_TRAKT")
        if not prov:
            return None

        inst = normalize_instance_id(instance_id)

        deadline = time.time() + max(0, int(timeout_sec))
        sleep_s = max(0.5, float(interval))

        while time.time() < deadline:
            cfg = load_config()
            try:
                res = prov.finish(cfg, device_code=device_code, instance_id=inst)  # type: ignore[attr-defined]
                if isinstance(res, dict):
                    status = (res.get("status") or "").lower()
                    if res.get("ok"):
                        save_config(cfg)
                        return "ok"
                    if status in ("expired_token", "no_device_code", "missing_client"):
                        return None
            except Exception:
                pass
            time.sleep(sleep_s)

        return None

    @app.post("/api/trakt/pin/new", tags=["auth"])
    def api_trakt_pin_new(payload: Optional[dict[str, Any]] = Body(None), instance: str = Query("default")) -> dict[str, Any]:
        try:
            inst = normalize_instance_id(instance)
            if payload:
                cid = str(payload.get("client_id") or "").strip()
                secr = str(payload.get("client_secret") or "").strip()
                if cid or secr:
                    cfg = load_config()
                    base = ensure_provider_block(cfg, "trakt")
                    tr = ensure_instance_block(cfg, "trakt", inst)
                    if cid:
                        base["client_id"] = cid
                        tr["client_id"] = cid
                    if secr:
                        base["client_secret"] = secr
                        tr["client_secret"] = secr
                    save_config(cfg)

            info = trakt_request_pin(inst)
            user_code = str(info["user_code"])
            verification_url = str(
                info.get("verification_url") or "https://trakt.tv/activate"
            )
            exp_epoch = int(info.get("expires_epoch") or 0)
            device_code = str(info["device_code"])

            def waiter(_device_code: str, _inst: str) -> None:
                token = trakt_wait_for_token(_device_code, instance_id=_inst, timeout_sec=600, interval=2.0)
                if token:
                    _safe_log(
                        log_fn,
                        "TRAKT",
                        "\x1b[92m[TRAKT]\x1b[0m Token acquired and saved.",
                    )
                    _probe_bust("trakt")
                else:
                    _safe_log(
                        log_fn,
                        "TRAKT",
                        "\x1b[91m[TRAKT]\x1b[0m Device code expired or not authorized.",
                    )

            threading.Thread(target=waiter, args=(device_code, inst), daemon=True).start()
            return {
                "ok": True,
                "user_code": user_code,
                "verificationUrl": verification_url,
                "verification_url": verification_url,
                "expiresIn": max(0, exp_epoch - int(time.time())),
            }
        except Exception as e:
            _safe_log(log_fn, "TRAKT", f"[TRAKT] ERROR: {e}")
            return {"ok": False, "error": str(e)}
        
    @app.post("/api/trakt/token/delete", tags=["auth"])
    def api_trakt_token_delete(instance: str = Query("default")) -> dict[str, Any]:
        try:
            inst = normalize_instance_id(instance)
            cfg = load_config()
            tr = ensure_instance_block(cfg, "trakt", inst)
            tr["access_token"] = ""
            tr["refresh_token"] = ""
            tr["scope"] = ""
            tr["token_type"] = ""
            tr["expires_at"] = 0
            try:
                tr.pop("_pending_device", None)
            except Exception:
                pass
            save_config(cfg)
            _safe_log(log_fn, "TRAKT", "[TRAKT] token cleared")
            _probe_bust("trakt")
            return {"ok": True}
        except Exception as e:
            _safe_log(log_fn, "TRAKT", f"[TRAKT] ERROR token delete: {e}")
            return {"ok": False, "error": str(e)}

    # ANILIST
    @app.post("/api/anilist/save", tags=["auth"])
    def api_anilist_save(payload: dict[str, Any] = Body(...), instance: str = Query("default")) -> dict[str, Any]:
        try:
            inst = normalize_instance_id(instance)
            cfg = load_config()
            a = ensure_instance_block(cfg, "anilist", inst)

            cid = str((payload or {}).get("client_id") or "").strip()
            sec = str((payload or {}).get("client_secret") or "").strip()
            if cid:
                a["client_id"] = cid
            if sec:
                a["client_secret"] = sec

            save_config(cfg)
            return {"ok": True, "instance": inst}
        except Exception as e:
            _safe_log(log_fn, "ANILIST", f"[ANILIST] ERROR save: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/api/anilist/status", tags=["auth"])
    def api_anilist_status(instance: str = Query("default")) -> dict[str, Any]:
        inst = normalize_instance_id(instance)
        cfg = load_config()
        a = ensure_instance_block(cfg, "anilist", inst)
        user = a.get("user") or {}
        uname = None
        if isinstance(user, dict):
            uname = user.get("name")
        return {"connected": bool(str(a.get("access_token") or "").strip()), "user": uname, "instance": inst}

    @app.post("/api/anilist/authorize", tags=["auth"])
    def api_anilist_authorize(payload: dict[str, Any] = Body(...), instance: str = Query("default")) -> dict[str, Any]:
        try:
            origin = (payload or {}).get("origin") or ""
            if not origin:
                return {"ok": False, "error": "origin missing"}

            _anilist_prune_state()

            inst = normalize_instance_id(instance)
            cfg = load_config()
            a = ensure_instance_block(cfg, "anilist", inst)

            client_id = str(a.get("client_id") or "").strip()
            client_secret = str(a.get("client_secret") or "").strip()
            if not client_id or not client_secret:
                return {"ok": False, "error": "AniList client_id/client_secret missing"}

            redirect_uri = f"{origin}/callback/anilist"
            state = secrets.token_urlsafe(16)
            ANILIST_STATE[state] = {"instance": inst, "redirect_uri": redirect_uri, "created_at": int(time.time())}

            url = anilist_build_authorize_url(client_id, redirect_uri, state, instance_id=inst)
            return {"ok": True, "authorize_url": url}
        except Exception as e:
            _safe_log(log_fn, "ANILIST", f"[ANILIST] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/callback/anilist", tags=["auth"])
    def oauth_anilist_callback(request: Request) -> Response:
        try:
            params = dict(request.query_params)
            code = params.get("code")
            state = params.get("state")
            if not code or not state:
                return PlainTextResponse("Missing code or state.", 400)

            _anilist_prune_state()
            st = ANILIST_STATE.get(state)
            if not isinstance(st, dict):
                return PlainTextResponse("State mismatch.", 400)

            inst = normalize_instance_id(st.get("instance"))
            redirect_uri = str(st.get("redirect_uri") or "").strip()
            if not redirect_uri:
                return PlainTextResponse("Missing redirect URI.", 400)

            cfg = load_config()
            a = ensure_instance_block(cfg, "anilist", inst)
            if not str(a.get("client_id") or "").strip() or not str(a.get("client_secret") or "").strip():
                return PlainTextResponse("AniList client_id/client_secret missing.", 400)

            tok = anilist_exchange_code_for_token(code=code, redirect_uri=redirect_uri, instance_id=inst)
            if not tok or "access_token" not in tok:
                return PlainTextResponse("AniList token exchange failed.", 400)

            a["access_token"] = tok["access_token"]
            if tok.get("user"):
                a["user"] = tok["user"]
            save_config(cfg)

            try:
                ANILIST_STATE.pop(state, None)
            except Exception:
                pass

            _safe_log(log_fn, "ANILIST", "[92m[ANILIST][0m Access token saved.")
            _probe_bust("anilist")
            return PlainTextResponse("AniList authorized. You can close this tab and return to the app.", 200)
        except Exception as e:
            _safe_log(log_fn, "ANILIST", f"[ANILIST] ERROR: {e}")
            return PlainTextResponse(f"Error: {e}", 500)

    @app.post("/api/anilist/token/delete", tags=["auth"])
    def api_anilist_token_delete(instance: str = Query("default")) -> dict[str, Any]:
        cfg = load_config()
        inst = normalize_instance_id(instance)
        a = ensure_instance_block(cfg, "anilist", inst)
        a["access_token"] = ""
        a.pop("user", None)
        save_config(cfg)
        _probe_bust("anilist")
        return {"ok": True}


    # SIMKL
    @app.post("/api/simkl/authorize", tags=["auth"])
    def api_simkl_authorize(payload: dict[str, Any] = Body(...), instance: str = Query("default")) -> dict[str, Any]:
        try:
            origin = (payload or {}).get("origin") or ""
            if not origin:
                return {"ok": False, "error": "origin missing"}
            inst = normalize_instance_id(instance)
            cfg = load_config()
            base = ensure_provider_block(cfg, "simkl")
            simkl_cfg = ensure_instance_block(cfg, "simkl", inst)

            client_id = (simkl_cfg.get("client_id") or "").strip() or (base.get("client_id") or "").strip()
            client_secret = (simkl_cfg.get("client_secret") or "").strip() or (base.get("client_secret") or "").strip()
            bad_cid = (not client_id) or (client_id.upper() == "YOUR_SIMKL_CLIENT_ID")
            bad_sec = (not client_secret) or (client_secret.upper() == "YOUR_SIMKL_CLIENT_SECRET")
            if bad_cid or bad_sec:
                return {"ok": False, "error": "SIMKL client_id and client_secret must be set in settings first"}

            state = secrets.token_urlsafe(24)
            redirect_uri = f"{origin}/callback"
            _simkl_prune_state()
            SIMKL_STATE[state] = {"instance": inst, "redirect_uri": redirect_uri, "created_at": int(time.time())}

            url = simkl_build_authorize_url(cfg, inst, client_id, redirect_uri, state)
            save_config(cfg)
            return {"ok": True, "authorize_url": url, "instance": inst}
        except Exception as e:
            _safe_log(log_fn, "SIMKL", f"[SIMKL] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/callback", tags=["auth"])
    def oauth_simkl_callback(request: Request) -> Response:
        try:
            params = dict(request.query_params)
            code = params.get("code")
            state = params.get("state")
            if not code or not state:
                return PlainTextResponse("Missing code or state.", 400)

            st = SIMKL_STATE.get(state)
            if not isinstance(st, dict):
                return PlainTextResponse("State mismatch.", 400)

            inst = normalize_instance_id(st.get("instance"))
            redirect_uri = str(st.get("redirect_uri") or "")

            cfg = load_config()
            base = ensure_provider_block(cfg, "simkl")
            simkl_cfg = ensure_instance_block(cfg, "simkl", inst)

            client_id = (simkl_cfg.get("client_id") or "").strip() or (base.get("client_id") or "").strip()
            client_secret = (simkl_cfg.get("client_secret") or "").strip() or (base.get("client_secret") or "").strip()
            if not client_id or not client_secret:
                return PlainTextResponse("SIMKL client_id/client_secret missing.", 400)

            tokens = simkl_exchange_code(cfg, inst, client_id, client_secret, str(code), redirect_uri)
            if not isinstance(tokens, dict) or not str(tokens.get("access_token") or "").strip():
                return PlainTextResponse("SIMKL token exchange failed.", 400)

            simkl_cfg["access_token"] = str(tokens.get("access_token") or "").strip()
            if tokens.get("refresh_token"):
                simkl_cfg["refresh_token"] = str(tokens.get("refresh_token") or "").strip()
            if tokens.get("expires_in"):
                simkl_cfg["token_expires_at"] = int(time.time()) + int(tokens["expires_in"])
            save_config(cfg)

            SIMKL_STATE.pop(state, None)
            _safe_log(log_fn, "SIMKL", "[92m[SIMKL][0m Access token saved.")
            _probe_bust("simkl")
            return PlainTextResponse("SIMKL authorized. You can close this tab and return to the app.", 200)
        except Exception as e:
            _safe_log(log_fn, "SIMKL", f"[SIMKL] ERROR: {e}")
            return PlainTextResponse(f"Error: {e}", 500)

    @app.post("/api/simkl/token/delete", tags=["auth"])
    def api_simkl_token_delete(instance: str = Query("default")) -> dict[str, Any]:
        cfg = load_config()
        inst = normalize_instance_id(instance)
        s = ensure_instance_block(cfg, "simkl", inst)
        for k in ("access_token", "refresh_token", "token_expires_at", "scopes", "account"):
            s.pop(k, None)
        save_config(cfg)
        _probe_bust("simkl")
        return {"ok": True, "instance": inst}



# ANILIST
# Instance-aware OAuth. We store callback state per instance to support parallel connects.
ANILIST_STATE: dict[str, dict[str, Any]] = {}

def _anilist_prune_state(max_age_s: int = 900) -> None:
    try:
        now = int(time.time())
        dead = [k for k, v in ANILIST_STATE.items() if not isinstance(v, dict) or (now - int(v.get("created_at") or 0)) > max_age_s]
        for k in dead:
            ANILIST_STATE.pop(k, None)
    except Exception:
        pass

def anilist_build_authorize_url(client_id: str, redirect_uri: str, state: str, *, instance_id: Any = None) -> str:
    prov = _import_provider("providers.auth._auth_ANILIST")
    inst = normalize_instance_id(instance_id)

    url = f"https://anilist.co/api/v2/oauth/authorize?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
    try:
        cfg = load_config()
        a = ensure_instance_block(cfg, "anilist", inst)
        if client_id:
            a["client_id"] = (client_id or "").strip()
        if prov:
            res = prov.start(cfg, redirect_uri=redirect_uri, instance_id=inst) or {}
            url = res.get("url") or url
            save_config(cfg)
    except Exception:
        pass

    if "state=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}state={state}"
    return url

def anilist_exchange_code_for_token(*, code: str, redirect_uri: str, instance_id: Any = None) -> dict[str, Any] | None:
    inst = normalize_instance_id(instance_id)
    prov = _import_provider("providers.auth._auth_ANILIST")
    try:
        if prov:
            cfg = load_config()
            prov.finish(cfg, redirect_uri=redirect_uri, code=code, instance_id=inst)
            save_config(cfg)
    except Exception:
        pass

    cfg2 = load_config()
    a = ensure_instance_block(cfg2, "anilist", inst)
    access = str(a.get("access_token") or "").strip()
    user = a.get("user")
    if access:
        out: dict[str, Any] = {"access_token": access}
        if isinstance(user, dict) and user:
            out["user"] = user
        return out

    # Fallback: direct exchange for this instance
    client_id = str(a.get("client_id") or "").strip()
    client_secret = str(a.get("client_secret") or "").strip()
    if not client_id or not client_secret:
        return None

    payload = {
        "grant_type": "authorization_code",
        "client_id": client_id,
        "client_secret": client_secret,
        "redirect_uri": redirect_uri,
        "code": code,
    }
    headers = {"Accept": "application/json", "User-Agent": "CrossWatch/1.0"}

    r = requests.post("https://anilist.co/api/v2/oauth/token", json=payload, headers=headers, timeout=15)
    if r.status_code >= 400:
        r = requests.post("https://anilist.co/api/v2/oauth/token", data=payload, headers=headers, timeout=15)
    if r.status_code >= 400:
        return None

    j = r.json() or {}
    tok = str(j.get("access_token") or "").strip()
    if not tok:
        return None

    viewer = None
    try:
        vr = requests.post(
            "https://graphql.anilist.co",
            json={"query": "query { Viewer { id name } }"},
            headers={"Authorization": f"Bearer {tok}", "Content-Type": "application/json", "Accept": "application/json"},
            timeout=15,
        )
        if vr.ok:
            viewer = (vr.json() or {}).get("data", {}).get("Viewer")
    except Exception:
        viewer = None

    out = {"access_token": tok}
    if isinstance(viewer, dict) and viewer:
        out["user"] = viewer

    a["access_token"] = tok
    if isinstance(viewer, dict) and viewer:
        a["user"] = viewer
    save_config(cfg2)

    return out

# SIMKL
SIMKL_STATE: dict[str, dict[str, Any]] = {}

def _simkl_prune_state(max_age_s: int = 900) -> None:
    try:
        now = int(time.time())
        dead = [k for k, v in SIMKL_STATE.items() if not isinstance(v, dict) or (now - int(v.get("created_at") or 0)) > max_age_s]
        for k in dead:
            SIMKL_STATE.pop(k, None)
    except Exception:
        pass

def simkl_build_authorize_url(cfg: dict[str, Any], instance_id: Any, client_id: str, redirect_uri: str, state: str) -> str:
    prov = _import_provider("providers.auth._auth_SIMKL")
    inst = normalize_instance_id(instance_id)
    s = ensure_instance_block(cfg, "simkl", inst)
    s["client_id"] = (client_id or s.get("client_id") or "").strip()
    url = f"https://simkl.com/oauth/authorize?response_type=code&client_id={s['client_id']}&redirect_uri={redirect_uri}"
    try:
        if prov:
            cfg_view = dict(cfg); cfg_view["simkl"] = s
            res = prov.start(cfg_view, redirect_uri=redirect_uri) or {}
            url = (res or {}).get("url") or url
    except Exception:
        pass
    if "state=" not in url:
        sep = "&" if "?" in url else "?"
        url = f"{url}{sep}state={state}"
    return url

def simkl_exchange_code(cfg: dict[str, Any], instance_id: Any, client_id: str, client_secret: str, code: str, redirect_uri: str) -> dict[str, Any] | None:
    prov = _import_provider("providers.auth._auth_SIMKL")
    inst = normalize_instance_id(instance_id)
    s = ensure_instance_block(cfg, "simkl", inst)
    s["client_id"] = (client_id or "").strip()
    s["client_secret"] = (client_secret or "").strip()
    try:
        if prov:
            cfg_view = dict(cfg); cfg_view["simkl"] = s
            prov.finish(cfg_view, redirect_uri=redirect_uri, code=code)
    except Exception:
        pass
    access = str(s.get("access_token") or "").strip()
    if not access:
        return None
    refresh = str(s.get("refresh_token") or "").strip()
    exp_at = int(s.get("token_expires_at", 0) or 0)
    expires_in = max(0, exp_at - int(time.time())) if exp_at else 0
    out: dict[str, Any] = {"access_token": access}
    if refresh:
        out["refresh_token"] = refresh
    if expires_in:
        out["expires_in"] = expires_in
    return out


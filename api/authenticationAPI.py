# api/authenticationAPI.py
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
from fastapi import Body, Request
from fastapi.responses import HTMLResponse, JSONResponse, PlainTextResponse

from cw_platform.config_base import load_config, save_config
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

    # ---------- provider registry ----------
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

    # ---------- PLEX ----------
    def plex_request_pin() -> dict[str, Any]:
        cfg = load_config(); plex = cfg.setdefault("plex", {})
        cid = plex.get("client_id")
        if not cid:
            cid = secrets.token_hex(12)
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
                res = _PLEX_PROVIDER.start(cfg, redirect_uri="") or {}
                save_config(cfg)
                code = (res or {}).get("pin")
                pend = (cfg.get("plex") or {}).get("_pending_pin") or {}
                pin_id = pend.get("id")
        except Exception as e:
            raise RuntimeError(f"Plex PIN error: {e}") from e
        if not code or not pin_id:
            raise RuntimeError("Plex PIN could not be issued")

        return {"id": pin_id, "code": code, "expires_epoch": int(time.time()) + 300, "headers": headers}

    def plex_wait_for_token(pin_id: int, *, timeout_sec: int = 300, interval: float = 1.0) -> Optional[str]:
        _PLEX_PROVIDER = _import_provider("providers.auth._auth_PLEX")
        deadline = time.time() + max(0, int(timeout_sec))
        sleep_s = max(0.2, float(interval))
        try:
            cfg0 = load_config(); plex0 = cfg0.setdefault("plex", {})
            pend = plex0.get("_pending_pin") or {}
            if not pend.get("id") and pin_id:
                plex0["_pending_pin"] = {"id": pin_id}
                save_config(cfg0)
        except Exception:
            pass
        while time.time() < deadline:
            cfg = load_config()
            token = (cfg.get("plex") or {}).get("account_token")
            if token: return token
            try:
                if _PLEX_PROVIDER:
                    _PLEX_PROVIDER.finish(cfg)
                    save_config(cfg)
            except Exception:
                pass
            time.sleep(sleep_s)
        return None

    @app.post("/api/plex/pin/new", tags=["auth"])
    def api_plex_pin_new() -> dict[str, Any]:
        try:
            info = plex_request_pin()
            pin_id, code, exp_epoch = info["id"], info["code"], int(info["expires_epoch"])
            cfg2 = load_config()
            cfg2.setdefault("plex", {})["_pending_pin"] = {"id": pin_id, "code": code}
            save_config(cfg2)

            def waiter(_pin_id: int):
                token = plex_wait_for_token(_pin_id, timeout_sec=360, interval=1.0)
                if token:
                    cfg = load_config()
                    plex_cfg = cfg.setdefault("plex", {})

                    plex_cfg["account_token"] = token
                    existing_url = (plex_cfg.get("server_url") or "").strip()
                    existing_user = (plex_cfg.get("username") or "").strip()
                    existing_aid  = str(plex_cfg.get("account_id") or "").strip()
                    need_auto_inspect = not (existing_url or existing_user or existing_aid)

                    save_config(cfg)

                    _safe_log(log_fn, "PLEX", "\x1b[92m[PLEX]\x1b[0m Token acquired and saved.")
                    _probe_bust("plex")

                    if need_auto_inspect:
                        try:
                            ensure_whitelist_defaults()
                        except Exception:
                            pass
                        try:
                            inspect_and_persist()
                        except Exception as e:
                            _safe_log(log_fn, "PLEX", f"[PLEX] auto-inspect failed: {e}")
                else:
                    _safe_log(log_fn, "PLEX", "\x1b[91m[PLEX]\x1b[0m PIN expired or not authorized.")
            threading.Thread(target=waiter, args=(pin_id,), daemon=True).start()
            remaining = max(0, exp_epoch - int(time.time()))
            return {
                "ok": True,
                "code": code,
                "pin_id": pin_id,
                "id": pin_id,
                "expiresIn": remaining,
                "expires_epoch": exp_epoch,
            }
        except Exception as e:
            _safe_log(log_fn, "PLEX", f"[PLEX] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/api/plex/inspect", tags=["media providers"])
    def plex_inspect():
        ensure_whitelist_defaults()
        return inspect_and_persist()
    
    @app.post("/api/plex/token/delete", tags=["auth"])
    def api_plex_token_delete() -> dict[str, Any]:
        cfg = load_config(); p = cfg.setdefault("plex", {})
        p["account_token"] = ""
        save_config(cfg)
        return {"ok": True}

    @app.get("/api/plex/libraries", tags=["media providers"])
    def plex_libraries():
        ensure_whitelist_defaults()
        return {"libraries": fetch_libraries_from_cfg()}

    @app.get("/api/plex/pickusers", tags=["media providers"])
    def plex_pickusers() -> dict[str, Any]:
        cfg = load_config()
        plex = (cfg.get("plex") or {})
        token = (plex.get("account_token") or "").strip()
        base  = (plex.get("server_url") or "").strip()
        if not token or not base:
            return {"users": [], "count": 0}

        norm = lambda s: (s or "").strip().lower()
        is_local_id = (
            lambda x: (isinstance(x, int) and 0 < x < 100000)
            or (str(x).isdigit() and 0 < int(x) < 100000)
        )
        rank = {"owner": 0, "managed": 1, "friend": 2}

        verify = plex_utils._resolve_verify_from_cfg(cfg, base)
        s = plex_utils._build_session(token, verify)
        r = plex_utils._try_get(s, base, "/accounts", timeout=10.0)

        pms_by_cloud: dict[int, dict[str, Any]] = {}
        pms_by_user: dict[str, dict[str, Any]] = {}
        pms_rows = []
        if r and r.ok and (r.text or "").lstrip().startswith("<"):
            try:
                root = ET.fromstring(r.text)
                for acc in root.findall(".//Account"):
                    pid = acc.attrib.get("id") or acc.attrib.get("ID")
                    if not is_local_id(pid):
                        continue
                    pms_id = _to_int(pid)
                    try:
                        cloud_id = _to_int(
                            acc.attrib.get("accountID")
                            or acc.attrib.get("accountId")
                        )
                    except Exception:
                        cloud_id = 0
                    own = str(acc.attrib.get("own") or "").lower() in ("1", "true", "yes")
                    username = (
                        acc.attrib.get("username") or acc.attrib.get("name") or ""
                    ).strip()
                    typ = "owner" if own else "managed"
                    row = {"pms_id": pms_id, "username": username, "type": typ}
                    pms_rows.append(row)
                    if cloud_id:
                        pms_by_cloud[cloud_id] = row
                    if username:
                        pms_by_user[norm(username)] = row
            except Exception:
                pass
        cloud_users = []
        try:
            cr = requests.get(
                "https://plex.tv/api/users",
                headers={"X-Plex-Token": token, "Accept": "application/xml"},
                timeout=10,
            )
            if cr.ok and (cr.text or "").lstrip().startswith("<"):
                root = ET.fromstring(cr.text)
                for u in root.findall(".//User"):
                    cloud_users.append(
                        {
                            "cloud_id": _to_int(u.attrib.get("id")),
                            "username": u.attrib.get("username") or "",
                            "title": u.attrib.get("title") or u.attrib.get("username") or "",
                            "type": "friend",
                        }
                    )
        except Exception:
            pass

        try:
            me = requests.get(
                "https://plex.tv/api/v2/user",
                headers={"X-Plex-Token": token},
                timeout=8,
            )
            if me.ok:
                j = me.json()
                cloud_users.append(
                    {
                        "cloud_id": _to_int(j.get("id")),
                        "username": (j.get("username") or j.get("title") or "") or "",
                        "title": (j.get("title") or j.get("username") or "") or "",
                        "type": "owner",
                    }
                )
        except Exception:
            pass

        merged = []
        for r0 in pms_rows:
            uname = r0["username"] or f"user#{r0['pms_id']}"
            merged.append({"id": r0["pms_id"], "username": uname, "type": r0["type"]})
        for cu in cloud_users:
            hit = pms_by_cloud.get(cu["cloud_id"]) or pms_by_user.get(norm(cu["username"]))
            if hit:
                uname = cu["username"] or hit["username"] or f"user#{hit['pms_id']}"
                merged.append({"id": hit["pms_id"], "username": uname, "type": hit["type"]})
            else:
                uname = cu["username"] or cu["title"] or f"user#{cu['cloud_id']}"
                merged.append({"id": cu["cloud_id"], "username": uname, "type": cu["type"]})

        best: dict[str, dict[str, Any]] = {}
        for u in merged:
            key = norm(u["username"]) or f"__id_{u['id']}"
            uid = u["id"]
            is_pms = isinstance(uid, int) and uid < 100000
            cur = best.get(key)
            if not cur:
                best[key] = u
                continue
            cur_is_pms = isinstance(cur["id"], int) and cur["id"] < 100000
            better = (
                (is_pms and not cur_is_pms)
                or (rank.get(u["type"], 9) < rank.get(cur["type"], 9))
                or (isinstance(uid, int) and isinstance(cur["id"], int) and uid < cur["id"])
            )
            if better:
                best[key] = u

        users = sorted(
            best.values(),
            key=lambda x: (rank.get(x["type"], 9), x["username"].lower()),
        )
        return {"users": users, "count": len(users)}

    @app.get("/api/plex/users", tags=["media providers"])
    def plex_users():
        return plex_pickusers()

    # ---------- JELLYFIN ----------
    @app.post("/api/jellyfin/login", tags=["auth"])
    def api_jellyfin_login(payload: dict[str, Any] = Body(...)) -> JSONResponse:
        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "Malformed request"}, 400)
        cfg = load_config(); jf = cfg.setdefault("jellyfin", {})
        for k in ("server", "username", "password"):
            v = (payload.get(k) or "").strip()
            if v: jf[k] = v
        if not all(jf.get(k) for k in ("server", "username", "password")):
            return JSONResponse({"ok": False, "error": "Missing: server/username/password"}, 400)

        try:
            prov = _import_provider("providers.auth._auth_JELLYFIN")
            if not prov: return JSONResponse({"ok": False, "error": "Provider missing"}, 500)
            res = prov.start(cfg, redirect_uri=""); save_config(cfg)
            if res.get("ok"):
                return JSONResponse({"ok": True, "user_id": res.get("user_id"),
                                     "username": jf.get("user") or jf.get("username"),
                                     "server": jf.get("server")}, 200)
            msg = res.get("error") or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))
        except Exception as e:
            msg = str(e) or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))
        
    @app.post("/api/jellyfin/token/delete", tags=["auth"])
    def api_jellyfin_token_delete() -> dict[str, Any]:
        cfg = load_config(); jf = cfg.setdefault("jellyfin", {})
        jf["access_token"] = ""
        save_config(cfg)
        return {"ok": True}

    @app.get("/api/jellyfin/status", tags=["auth"])
    def api_jellyfin_status() -> dict[str, Any]:
        cfg = load_config(); jf = (cfg.get("jellyfin") or {})
        return {"connected": bool(jf.get("access_token") and jf.get("server")),
                "user": jf.get("user") or jf.get("username") or None}

    @app.get("/api/jellyfin/inspect", tags=["media providers"])
    def jf_inspect():
        jf_ensure_whitelist_defaults()
        return jf_inspect_and_persist()

    @app.get("/api/jellyfin/libraries", tags=["media providers"])
    def jf_libraries():
        jf_ensure_whitelist_defaults()
        return {"libraries": jf_fetch_libraries_from_cfg()}

    # ---------- EMBY ----------
    @app.post("/api/emby/login", tags=["auth"])
    def api_emby_login(payload: dict[str, Any] = Body(...)) -> JSONResponse:
        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "Malformed request"}, 400)
        cfg = load_config(); em = cfg.setdefault("emby", {})
        for k in ("server", "username", "password"):
            v = (payload.get(k) or "").strip()
            if v: em[k] = v
        if "verify_ssl" in payload: em["verify_ssl"] = bool(payload.get("verify_ssl"))
        if not all(em.get(k) for k in ("server", "username", "password")):
            return JSONResponse({"ok": False, "error": "Missing: server/username/password"}, 400)

        try:
            prov = _import_provider("providers.auth._auth_EMBY")
            if not prov: return JSONResponse({"ok": False, "error": "Provider missing"}, 500)
            res = prov.start(cfg, redirect_uri=""); save_config(cfg)
            if res.get("ok"):
                return JSONResponse({"ok": True, "user_id": res.get("user_id"),
                                     "username": em.get("user") or em.get("username"),
                                     "server": em.get("server")}, 200)
            msg = res.get("error") or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))
        except Exception as e:
            msg = str(e) or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _status_from_msg(msg))

    @app.get("/api/emby/status", tags=["auth"])
    def api_emby_status() -> dict[str, Any]:
        cfg = load_config(); em = (cfg.get("emby") or {})
        return {"connected": bool(em.get("access_token") and em.get("server")),
                "user": em.get("user") or em.get("username") or None}
        
    @app.post("/api/emby/token/delete", tags=["auth"])
    def api_emby_token_delete() -> dict[str, Any]:
        cfg = load_config(); em = cfg.setdefault("emby", {})
        em["access_token"] = ""
        save_config(cfg)
        return {"ok": True}

    @app.get("/api/emby/inspect", tags=["media providers"])
    def emby_inspect():
        emby_ensure_whitelist_defaults()
        out = emby_inspect_and_persist()
        try:
            if not (out or {}).get("user_id"):
                cfg = load_config(); em = (cfg.get("emby") or {})
                server = (em.get("server") or "").rstrip("/")
                token = (em.get("access_token") or "").strip()
                if server and token:
                    r = requests.get(f"{server}/Users/Me", headers={"X-Emby-Token": token, "Accept": "application/json"}, timeout=float(em.get("timeout", 15) or 15), verify=bool(em.get("verify_ssl", False)))
                    if r.ok:
                        me = r.json() or {}
                        out = dict(out or {})
                        out.setdefault("user_id", me.get("Id") or me.get("id") or "")
                        out.setdefault("username", me.get("Name") or me.get("name") or "")
        except Exception:
            pass
        return out

    @app.get("/api/emby/libraries", tags=["media providers"])
    def emby_libraries():
        emby_ensure_whitelist_defaults()
        return {"libraries": emby_fetch_libraries_from_cfg()}

    @app.get("/api/emby/users", tags=["media providers"])
    def emby_users():
        cfg = load_config(); em = (cfg.get("emby") or {})
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
                        users.append({
                            "id": (u or {}).get("Id") or (u or {}).get("id"),
                            "username": (u or {}).get("Name") or (u or {}).get("name") or "",
                            "IsAdministrator": bool(((u or {}).get("Policy") or {}).get("IsAdministrator")) if isinstance((u or {}).get("Policy"), dict) else bool((u or {}).get("IsAdministrator") or False),
                            "IsHidden": bool(((u or {}).get("Policy") or {}).get("IsHidden")) if isinstance((u or {}).get("Policy"), dict) else bool((u or {}).get("IsHidden") or False),
                            "IsDisabled": bool(((u or {}).get("Policy") or {}).get("IsDisabled")) if isinstance((u or {}).get("Policy"), dict) else bool((u or {}).get("IsDisabled") or False),
                        })
            else:
                mr = requests.get(f"{server}/Users/Me", headers=headers, timeout=timeout, verify=verify)
                if mr.ok:
                    me = mr.json() or {}
                    users = [{"id": me.get("Id") or me.get("id"), "username": me.get("Name") or me.get("name") or ""}]
        except Exception as e:
            return JSONResponse({"ok": False, "error": str(e)}, _status_from_msg(str(e)))
        users = [u for u in users if (u or {}).get("username")]
        return {"users": users, "count": len(users)}
    
    # ---------- MDBLIST ----------
    @app.post("/api/mdblist/save", tags=["auth"])
    def api_mdblist_save(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        try:
            key = str((payload or {}).get("api_key") or "").strip()
            cfg = load_config(); cfg.setdefault("mdblist", {})["api_key"] = key
            save_config(cfg)
            _safe_log(log_fn, "MDBLIST", "[MDBLIST] api_key saved")
            if isinstance(probe_cache, dict): probe_cache["mdblist"] = (0.0, False)
            return {"ok": True}
        except Exception as e:
            _safe_log(log_fn, "MDBLIST", f"[MDBLIST] ERROR save: {e}")
            return {"ok": False, "error": str(e)}

    @app.post("/api/mdblist/disconnect", tags=["auth"])
    def api_mdblist_disconnect() -> dict[str, Any]:
        try:
            cfg = load_config(); cfg.setdefault("mdblist", {})["api_key"] = ""
            save_config(cfg)
            _safe_log(log_fn, "MDBLIST", "[MDBLIST] disconnected")
            if isinstance(probe_cache, dict): probe_cache["mdblist"] = (0.0, False)
            return {"ok": True}
        except Exception as e:
            _safe_log(log_fn, "MDBLIST", f"[MDBLIST] ERROR disconnect: {e}")
            return {"ok": False, "error": str(e)}
        
    # ---------- TAUTULLI ----------
    @app.post("/api/tautulli/save", tags=["auth"])
    def tautulli_save(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        cfg = load_config()
        t = cfg.setdefault("tautulli", {})
        server = str(payload.get("server_url") or payload.get("server") or "").strip()
        key = str(payload.get("api_key") or payload.get("key") or "").strip()
        user_id = str(payload.get("user_id") or "").strip()

        t["server_url"] = server
        t["api_key"] = key
        if "verify_ssl" in payload:
            t["verify_ssl"] = bool(payload.get("verify_ssl"))
        if user_id:
            t.setdefault("history", {})["user_id"] = user_id

        save_config(cfg)
        _probe_bust("tautulli")
        return {"ok": True}

    @app.post("/api/tautulli/disconnect", tags=["auth"])
    def tautulli_disconnect() -> dict[str, Any]:
        cfg = load_config()
        t = cfg.setdefault("tautulli", {})
        t["server_url"] = ""
        t["api_key"] = ""
        save_config(cfg)
        _probe_bust("tautulli")
        return {"ok": True}

    # ---------- TRAKT ----------
    def trakt_request_pin() -> dict[str, Any]:
        prov = _import_provider("providers.auth._auth_TRAKT")
        if not prov:
            raise RuntimeError("Trakt provider not available")

        cfg = load_config()
        res = prov.start(cfg, redirect_uri="")  # type: ignore[attr-defined]
        save_config(cfg)

        pend = (cfg.get("trakt") or {}).get("_pending_device") or {}
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
        timeout_sec: int = 600,
        interval: float = 2.0,
    ) -> Optional[str]:
        prov = _import_provider("providers.auth._auth_TRAKT")
        if not prov:
            return None

        deadline = time.time() + max(0, int(timeout_sec))
        sleep_s = max(0.5, float(interval))

        while time.time() < deadline:
            cfg = load_config()
            try:
                res = prov.finish(cfg, device_code=device_code)  # type: ignore[attr-defined]
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
    def api_trakt_pin_new(payload: Optional[dict[str, Any]] = Body(None)) -> dict[str, Any]:
        try:
            if payload:
                cid = str(payload.get("client_id") or "").strip()
                secr = str(payload.get("client_secret") or "").strip()
                if cid or secr:
                    cfg = load_config()
                    tr = cfg.setdefault("trakt", {})
                    if cid:
                        tr["client_id"] = cid
                    if secr:
                        tr["client_secret"] = secr
                    save_config(cfg)

            info = trakt_request_pin()
            user_code = str(info["user_code"])
            verification_url = str(
                info.get("verification_url") or "https://trakt.tv/activate"
            )
            exp_epoch = int(info.get("expires_epoch") or 0)
            device_code = str(info["device_code"])

            def waiter(_device_code: str) -> None:
                token = trakt_wait_for_token(_device_code, timeout_sec=600, interval=2.0)
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

            threading.Thread(target=waiter, args=(device_code,), daemon=True).start()
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
    def api_trakt_token_delete() -> dict[str, Any]:
        try:
            cfg = load_config()
            tr = cfg.setdefault("trakt", {})
            tr["access_token"] = ""
            tr["refresh_token"] = ""
            tr["token_expires_at"] = 0
            tr["scopes"] = ""
            save_config(cfg)
            _safe_log(log_fn, "TRAKT", "[TRAKT] token cleared")
            _probe_bust("trakt")
            return {"ok": True}
        except Exception as e:
            _safe_log(log_fn, "TRAKT", f"[TRAKT] ERROR token delete: {e}")
            return {"ok": False, "error": str(e)}

    # ---------- SIMKL ----------
    SIMKL_STATE: dict[str, Any] = {}

    @app.post("/api/simkl/authorize", tags=["auth"])
    def api_simkl_authorize(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
        try:
            origin = (payload or {}).get("origin") or ""
            if not origin: return {"ok": False, "error": "origin missing"}

            cfg = load_config(); simkl = cfg.get("simkl", {}) or {}
            client_id = (simkl.get("client_id") or "").strip()
            client_secret = (simkl.get("client_secret") or "").strip()
            bad_cid = (not client_id) or (client_id.upper() == "YOUR_SIMKL_CLIENT_ID")
            bad_sec = (not client_secret) or (client_secret.upper() == "YOUR_SIMKL_CLIENT_SECRET")
            if bad_cid or bad_sec:
                return {"ok": False, "error": "SIMKL client_id and client_secret must be set in settings first"}

            state = secrets.token_urlsafe(24)
            redirect_uri = f"{origin}/callback"
            SIMKL_STATE["state"], SIMKL_STATE["redirect_uri"] = state, redirect_uri

            url = simkl_build_authorize_url(client_id, redirect_uri, state)
            return {"ok": True, "authorize_url": url}
        except Exception as e:
            _safe_log(log_fn, "SIMKL", f"[SIMKL] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/callback", tags=["auth"])
    def oauth_simkl_callback(request: Request) -> PlainTextResponse:
        try:
            params = dict(request.query_params)
            code = params.get("code"); state = params.get("state")
            if not code or not state: return PlainTextResponse("Missing code or state.", 400)
            if state != SIMKL_STATE.get("state"): return PlainTextResponse("State mismatch.", 400)

            cfg = load_config(); simkl_cfg = cfg.setdefault("simkl", {})
            client_id = (simkl_cfg.get("client_id") or "").strip()
            client_secret = (simkl_cfg.get("client_secret") or "").strip()
            redirect_uri = SIMKL_STATE.get("redirect_uri") or ""

            tokens = simkl_exchange_code(client_id, client_secret, code, redirect_uri)
            if not tokens or "access_token" not in tokens:
                return PlainTextResponse("SIMKL token exchange failed.", 400)

            simkl_cfg["access_token"] = tokens["access_token"]
            if tokens.get("refresh_token"): simkl_cfg["refresh_token"] = tokens["refresh_token"]
            if tokens.get("expires_in"): simkl_cfg["token_expires_at"] = int(time.time()) + int(tokens["expires_in"])
            save_config(cfg)

            _safe_log(log_fn, "SIMKL", "\x1b[92m[SIMKL]\x1b[0m Access token saved."); _probe_bust("simkl")
            return PlainTextResponse("SIMKL authorized. You can close this tab and return to the app.", 200)
        except Exception as e:
            _safe_log(log_fn, "SIMKL", f"[SIMKL] ERROR: {e}")
            return PlainTextResponse(f"Error: {e}", 500)
        
    @app.post("/api/simkl/token/delete", tags=["auth"])
    def api_simkl_token_delete() -> dict[str, Any]:
        cfg = load_config(); s = cfg.setdefault("simkl", {})
        s["access_token"] = ""
        s["refresh_token"] = ""
        s["token_expires_at"] = 0
        s["scopes"] = ""
        save_config(cfg)
        return {"ok": True}

    def simkl_build_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
        prov = _import_provider("providers.auth._auth_SIMKL")
        cfg = load_config(); cfg.setdefault("simkl", {})["client_id"] = (client_id or cfg.get("simkl", {}).get("client_id") or "").strip()
        url = f"https://simkl.com/oauth/authorize?response_type=code&client_id={cfg['simkl']['client_id']}&redirect_uri={redirect_uri}"
        try:
            if prov:
                res = prov.start(cfg, redirect_uri=redirect_uri) or {}
                url = res.get("url") or url
                save_config(cfg)
        except Exception:
            pass
        if "state=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}state={state}"
        return url

    def simkl_exchange_code(client_id: str, client_secret: str, code: str, redirect_uri: str) -> dict[str, Any]:
        prov = _import_provider("providers.auth._auth_SIMKL")
        cfg = load_config(); s = cfg.setdefault("simkl", {})
        s["client_id"] = client_id.strip(); s["client_secret"] = client_secret.strip()
        try:
            if prov:
                prov.finish(cfg, redirect_uri=redirect_uri, code=code); save_config(cfg)
        except Exception:
            pass
        s = load_config().get("simkl", {}) or {}
        access, refresh = s.get("access_token", ""), s.get("refresh_token", "")
        exp_at = int(s.get("token_expires_at", 0) or 0)
        expires_in = max(0, exp_at - int(time.time())) if exp_at else 0
        out = {"access_token": access}
        if refresh: out["refresh_token"] = refresh
        if expires_in: out["expires_in"] = expires_in
        return out

    return None
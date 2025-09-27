# _authentication.py - authentication routes for FastAPI app
# Refactoring project: _authentication.py (v0.1)

from __future__ import annotations

from typing import Any, Dict, Optional
import time, threading, secrets, importlib

from fastapi import Body, Request
from fastapi.responses import JSONResponse, PlainTextResponse

from cw_platform.config_base import load_config, save_config

# -- public API
__all__ = ["register_auth"]


def register_auth(app, *, log_fn=None, probe_cache: Optional[dict] = None) -> None:
    """Register all auth routes onto the given FastAPI app.
    log_fn: callable(tag: str, msg: str) used for app logs; optional.
    probe_cache: dict like {"plex": (ts,bool), ...}; optional, used to bust probes.
    """

    # small helper wrappers ---------------------------------------------------
    def _log(tag: str, msg: str) -> None:
        try:
            if callable(log_fn):
                log_fn(tag, msg)
        except Exception:
            pass  # no drama in auth flows

    def _probe_bust(name: str) -> None:
        try:
            if isinstance(probe_cache, dict):
                probe_cache[name] = (0.0, False)
        except Exception:
            pass

    # ---------------- Jellyfin login & status -------------------------------
    @app.post("/api/jellyfin/login")
    def api_jellyfin_login(payload: Dict[str, Any] = Body(...)) -> JSONResponse:  # noqa: N802
        # Keep HTTP codes aligned with existing UX
        if not isinstance(payload, dict):
            return JSONResponse({"ok": False, "error": "Malformed request"}, 400)

        cfg = load_config()
        jf = cfg.setdefault("jellyfin", {})
        for k in ("server", "username", "password"):
            v = (payload.get(k) or "").strip()
            if v:
                jf[k] = v
        if not all(jf.get(k) for k in ("server", "username", "password")):
            return JSONResponse({"ok": False, "error": "Missing: server/username/password"}, 400)

        def _code(msg: str) -> int:
            m = (msg or "").lower()
            if any(x in m for x in ("401", "403", "invalid credential", "unauthor")): return 401
            if "timeout" in m: return 504
            if any(x in m for x in ("dns", "ssl", "connection", "refused", "unreachable", "getaddrinfo", "name or service")): return 502
            return 502

        try:
            mod = importlib.import_module("providers.auth._auth_JELLYFIN")
            prov = getattr(mod, "PROVIDER", None)
            if not prov:
                return JSONResponse({"ok": False, "error": "Provider missing"}, 500)

            res = prov.start(cfg, redirect_uri="")
            save_config(cfg)

            if res.get("ok"):
                return JSONResponse({
                    "ok": True,
                    "user_id": res.get("user_id"),
                    "username": jf.get("user") or jf.get("username"),
                    "server": jf.get("server"),
                }, 200)

            msg = res.get("error") or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _code(msg))
        except Exception as e:  # noqa: BLE001
            msg = str(e) or "Login failed"
            return JSONResponse({"ok": False, "error": msg}, _code(msg))

    @app.get("/api/jellyfin/status")
    def api_jellyfin_status() -> Dict[str, Any]:  # noqa: N802
        cfg = load_config()
        jf = (cfg.get("jellyfin") or {})
        return {
            "connected": bool(jf.get("access_token") and jf.get("server")),
            "user": jf.get("user") or jf.get("username") or None,
        }

    # ---------------- Plex PIN auth -----------------------------------------
    def plex_request_pin() -> dict:
        cfg = load_config()
        plex = cfg.setdefault("plex", {})
        cid = plex.get("client_id")
        if not cid:
            import secrets as _secrets
            cid = _secrets.token_hex(12)
            plex["client_id"] = cid
            save_config(cfg)

        headers = {
            "Accept": "application/json",
            "User-Agent": "CrossWatch/1.0",
            "X-Plex-Product": "CrossWatch",
            "X-Plex-Version": "1.0",
            "X-Plex-Client-Identifier": cid,
            "X-Plex-Platform": "Web",
        }

        try:
            from providers.auth._auth_PLEX import PROVIDER as _PLEX_PROVIDER  # type: ignore
        except Exception:  # noqa: BLE001
            _PLEX_PROVIDER = None  # type: ignore

        code: Optional[str] = None
        pin_id: Optional[int] = None
        try:
            if _PLEX_PROVIDER is not None:  # type: ignore[truthy-bool]
                res = _PLEX_PROVIDER.start(cfg, redirect_uri="") or {}
                save_config(cfg)
                code = (res or {}).get("pin")
                pend = (cfg.get("plex") or {}).get("_pending_pin") or {}
                pin_id = pend.get("id")
        except Exception as e:  # noqa: BLE001
            raise RuntimeError(f"Plex PIN error: {e}") from e

        if not code or not pin_id:
            raise RuntimeError("Plex PIN could not be issued")

        expires_epoch = int(time.time()) + 300
        return {"id": pin_id, "code": code, "expires_epoch": expires_epoch, "headers": headers}

    def plex_wait_for_token(pin_id: int, headers: Optional[dict] = None, timeout_sec: int = 300, interval: float = 1.0) -> Optional[str]:
        try:
            from providers.auth._auth_PLEX import PROVIDER as _PLEX_PROVIDER  # type: ignore
        except Exception:  # noqa: BLE001
            _PLEX_PROVIDER = None  # type: ignore

        deadline = time.time() + max(0, int(timeout_sec))
        sleep_s = max(0.2, float(interval))

        # ensure pending pin id exists (resilience)
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
            if token:
                return token
            try:
                if _PLEX_PROVIDER is not None:  # type: ignore[truthy-bool]
                    _PLEX_PROVIDER.finish(cfg)
                    save_config(cfg)
            except Exception:
                pass
            time.sleep(sleep_s)
        return None

    @app.post("/api/plex/pin/new")
    def api_plex_pin_new() -> Dict[str, Any]:  # noqa: N802
        try:
            info = plex_request_pin()
            pin_id = info["id"]
            code = info["code"]
            exp_epoch = int(info["expires_epoch"])
            headers = info["headers"]

            # persist pending pin
            cfg2 = load_config(); plex2 = cfg2.setdefault("plex", {})
            plex2["_pending_pin"] = {"id": pin_id, "code": code}
            save_config(cfg2)

            def waiter(_pin_id: int, _headers: Dict[str, str]):
                token = plex_wait_for_token(_pin_id, headers=_headers, timeout_sec=360, interval=1.0)
                if token:
                    cfg = load_config(); cfg.setdefault("plex", {})["account_token"] = token
                    save_config(cfg)
                    _log("PLEX", "\x1b[92m[PLEX]\x1b[0m Token acquired and saved.")
                    _probe_bust("plex")
                else:
                    _log("PLEX", "\x1b[91m[PLEX]\x1b[0m PIN expired or not authorized.")

            threading.Thread(target=waiter, args=(pin_id, headers), daemon=True).start()
            expires_in = max(0, exp_epoch - int(time.time()))
            return {"ok": True, "code": code, "pin_id": pin_id, "expiresIn": expires_in}
        except Exception as e:  # noqa: BLE001
            _log("PLEX", f"[PLEX] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    # ---------------- Trakt PIN (device) -------------------------------------
    def trakt_request_pin() -> dict:
        try:
            from providers.auth._auth_TRAKT import PROVIDER as _TRAKT_PROVIDER  # type: ignore
        except Exception:  # noqa: BLE001
            _TRAKT_PROVIDER = None  # type: ignore
        if _TRAKT_PROVIDER is None:
            raise RuntimeError("Trakt provider not available")

        cfg = load_config(); res = _TRAKT_PROVIDER.start(cfg, redirect_uri="")
        save_config(cfg)

        pend = (cfg.get("trakt") or {}).get("_pending_device") or {}
        user_code = (pend.get("user_code") or (res or {}).get("user_code"))
        device_code = (pend.get("device_code") or (res or {}).get("device_code"))
        verification_url = (pend.get("verification_url") or (res or {}).get("verification_url") or "https://trakt.tv/activate")
        exp_epoch = int((pend.get("expires_at") or 0) or (time.time() + 600))

        if not user_code or not device_code:
            raise RuntimeError("Trakt PIN could not be issued")

        return {"user_code": user_code, "device_code": device_code, "verification_url": verification_url, "expires_epoch": exp_epoch}

    def trakt_wait_for_token(device_code: str, timeout_sec: int = 600, interval: float = 2.0) -> Optional[str]:
        try:
            from providers.auth._auth_TRAKT import PROVIDER as _TRAKT_PROVIDER  # type: ignore
        except Exception:  # noqa: BLE001
            _TRAKT_PROVIDER = None  # type: ignore

        deadline = time.time() + max(0, int(timeout_sec))
        sleep_s = max(0.5, float(interval))

        while time.time() < deadline:
            cfg = load_config() or {}
            tok = None
            if _TRAKT_PROVIDER is not None:
                try:
                    tok = _TRAKT_PROVIDER.read_token_file(cfg, device_code)  # type: ignore[attr-defined]
                except Exception:
                    tok = None

            if tok:
                try:
                    if _TRAKT_PROVIDER is not None:
                        _TRAKT_PROVIDER.finish(cfg, device_code=device_code)
                        save_config(cfg)
                    else:
                        # extremely defensive fallback
                        if isinstance(tok, str):
                            try:
                                import json as _json
                                tok = _json.loads(tok)
                            except Exception:
                                tok = {}
                        if isinstance(tok, dict):
                            tr = cfg.setdefault("trakt", {})
                            tr["access_token"]  = tok.get("access_token")  or tr.get("access_token", "")
                            tr["refresh_token"] = tok.get("refresh_token") or tr.get("refresh_token", "")
                            exp = int(tok.get("created_at") or 0) + int(tok.get("expires_in") or 0)
                            if not exp:
                                exp = int(time.time()) + 90 * 24 * 3600
                            tr["expires_at"] = exp
                            tr["token_type"] = tok.get("token_type") or "bearer"
                            tr["scope"] = tok.get("scope") or tr.get("scope", "public")
                            save_config(cfg)
                except Exception:
                    pass
                return "ok"

            if _TRAKT_PROVIDER is not None:
                try:
                    _TRAKT_PROVIDER.finish(cfg, device_code=device_code)
                    save_config(cfg)
                except Exception:
                    pass

            time.sleep(sleep_s)

        return None

    @app.post("/api/trakt/pin/new")
    def api_trakt_pin_new(payload: Optional[dict] = Body(None)) -> Dict[str, Any]:  # noqa: N802
        try:
            if payload:
                cid = str(payload.get("client_id") or "").strip()
                secr = str(payload.get("client_secret") or "").strip()
                if cid or secr:
                    cfg = load_config(); tr = cfg.setdefault("trakt", {})
                    if cid: tr["client_id"] = cid
                    if secr: tr["client_secret"] = secr
                    save_config(cfg)

            info = trakt_request_pin()
            user_code = info["user_code"]
            verification_url = info["verification_url"]
            exp_epoch = int(info["expires_epoch"])
            device_code = info["device_code"]

            def waiter(_device_code: str):
                token = trakt_wait_for_token(_device_code, timeout_sec=600, interval=2.0)
                if token:
                    _log("TRAKT", "\x1b[92m[TRAKT]\x1b[0m Token acquired and saved.")
                    _probe_bust("trakt")
                else:
                    _log("TRAKT", "\x1b[91m[TRAKT]\x1b[0m Device code expired or not authorized.")

            threading.Thread(target=waiter, args=(device_code,), daemon=True).start()
            expires_in = max(0, exp_epoch - int(time.time()))
            return {"ok": True, "user_code": user_code, "verification_url": verification_url, "expiresIn": expires_in}
        except Exception as e:  # noqa: BLE001
            _log("TRAKT", f"[TRAKT] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    # ---------------- SIMKL OAuth (auth code) --------------------------------
    SIMKL_STATE: Dict[str, Any] = {}

    @app.post("/api/simkl/authorize")
    def api_simkl_authorize(payload: Dict[str, Any] = Body(...)) -> Dict[str, Any]:  # noqa: N802
        try:
            origin = (payload or {}).get("origin") or ""
            if not origin:
                return {"ok": False, "error": "origin missing"}

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
        except Exception as e:  # noqa: BLE001
            _log("SIMKL", f"[SIMKL] ERROR: {e}")
            return {"ok": False, "error": str(e)}

    @app.get("/callback")
    def oauth_simkl_callback(request: Request) -> PlainTextResponse:  # noqa: N802
        try:
            params = dict(request.query_params)
            code = params.get("code"); state = params.get("state")
            if not code or not state:
                return PlainTextResponse("Missing code or state.", status_code=400)
            if state != SIMKL_STATE.get("state"):
                return PlainTextResponse("State mismatch.", status_code=400)

            cfg = load_config(); simkl_cfg = cfg.setdefault("simkl", {})
            client_id = (simkl_cfg.get("client_id") or "").strip()
            client_secret = (simkl_cfg.get("client_secret") or "").strip()
            redirect_uri = SIMKL_STATE.get("redirect_uri") or ""

            tokens = simkl_exchange_code(client_id, client_secret, code, redirect_uri)
            if not tokens or "access_token" not in tokens:
                return PlainTextResponse("SIMKL token exchange failed.", status_code=400)

            simkl_cfg["access_token"] = tokens["access_token"]
            if tokens.get("refresh_token"):
                simkl_cfg["refresh_token"] = tokens["refresh_token"]
            if tokens.get("expires_in"):
                simkl_cfg["token_expires_at"] = int(time.time()) + int(tokens["expires_in"])
            save_config(cfg)

            _log("SIMKL", "\x1b[92m[SIMKL]\x1b[0m Access token saved.")
            _probe_bust("simkl")
            return PlainTextResponse("SIMKL authorized. You can close this tab and return to the app.", status_code=200)
        except Exception as e:  # noqa: BLE001
            _log("SIMKL", f"[SIMKL] ERROR: {e}")
            return PlainTextResponse(f"Error: {e}", status_code=500)

    # helpers to cooperate with provider modules -----------------------------
    def simkl_build_authorize_url(client_id: str, redirect_uri: str, state: str) -> str:
        try:
            from providers.auth._auth_SIMKL import PROVIDER as _SIMKL_PROVIDER  # type: ignore
        except Exception:  # noqa: BLE001
            _SIMKL_PROVIDER = None  # type: ignore

        cfg = load_config(); cfg.setdefault("simkl", {})["client_id"] = (client_id or cfg.get("simkl", {}).get("client_id") or "").strip()
        url = f"https://simkl.com/oauth/authorize?response_type=code&client_id={cfg['simkl']['client_id']}&redirect_uri={redirect_uri}"
        try:
            if _SIMKL_PROVIDER is not None:  # type: ignore[truthy-bool]
                res = _SIMKL_PROVIDER.start(cfg, redirect_uri=redirect_uri) or {}
                url = res.get("url") or url
                save_config(cfg)
        except Exception:
            pass
        if "state=" not in url:
            sep = "&" if "?" in url else "?"
            url = f"{url}{sep}state={state}"
        return url

    def simkl_exchange_code(client_id: str, client_secret: str, code: str, redirect_uri: str) -> dict:
        try:
            from providers.auth._auth_SIMKL import PROVIDER as _SIMKL_PROVIDER  # type: ignore
        except Exception:  # noqa: BLE001
            _SIMKL_PROVIDER = None  # type: ignore

        cfg = load_config(); s = cfg.setdefault("simkl", {})
        s["client_id"] = client_id.strip(); s["client_secret"] = client_secret.strip()
        try:
            if _SIMKL_PROVIDER is not None:  # type: ignore[truthy-bool]
                _SIMKL_PROVIDER.finish(cfg, redirect_uri=redirect_uri, code=code)
                save_config(cfg)
        except Exception:
            pass

        s = load_config().get("simkl", {}) or {}
        access = s.get("access_token", ""); refresh = s.get("refresh_token", "")
        exp_at = int(s.get("token_expires_at", 0) or 0)
        expires_in = max(0, exp_at - int(time.time())) if exp_at else 0
        out = {"access_token": access}
        if refresh: out["refresh_token"] = refresh
        if expires_in: out["expires_in"] = expires_in
        return out

    # done -------------------------------------------------------------------
    return None
from __future__ import annotations
import time, requests
from typing import Any, Mapping, MutableMapping
from ._auth_base import AuthProvider, AuthStatus, AuthManifest
from _logging import log

SIMKL_AUTH = "https://simkl.com/oauth/authorize"
SIMKL_TOKEN = "https://api.simkl.com/oauth/token"
UA = "CrossWatch/1.0"

class SimklAuth(AuthProvider):
    name = "SIMKL"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="SIMKL",
            label="SIMKL",
            flow="oauth",
            fields=[
                {"key": "simkl.client_id", "label": "Client ID", "type": "text", "required": True},
                {"key": "simkl.client_secret", "label": "Client Secret", "type": "password", "required": True},
            ],
            actions={"start": True, "finish": False, "refresh": True, "disconnect": True},
            notes="Authorize with SIMKL; you'll be redirected back to the app.",
        )

    def capabilities(self) -> dict:
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "collections": {"read": False, "write": False},
                "ratings": {"read": True, "write": True, "scale": "1-10"},
                "watched": {"read": True, "write": True},
                "liked_lists": {"read": True, "write": False},
            },
            "entity_types": ["movie", "show"],
        }

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        s = cfg.get("simkl") or {}
        ok = bool(s.get("access_token"))
        return AuthStatus(
            connected=ok,
            label="SIMKL",
            user=s.get("account") or None,
            expires_at=int(s.get("token_expires_at") or 0) or None,
            scopes=s.get("scopes") or None,
        )

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, str]:
        client_id = (cfg.get("simkl") or {}).get("client_id") or ""
        url = f"{SIMKL_AUTH}?response_type=code&client_id={client_id}&redirect_uri={redirect_uri}"
        log("SIMKL: start OAuth", level="INFO", module="AUTH", extra={"redirect_uri": redirect_uri})
        return {"url": url}

    def finish(self, cfg: MutableMapping[str, Any], **payload) -> AuthStatus:
        s = cfg.setdefault("simkl", {})
        data = {
            "grant_type": "authorization_code",
            "client_id": s.get("client_id", ""),
            "client_secret": s.get("client_secret", ""),
            "redirect_uri": payload.get("redirect_uri", ""),
            "code": payload.get("code", ""),
        }
        log("SIMKL: exchange code", level="INFO", module="AUTH")
        r = requests.post(SIMKL_TOKEN, json=data, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=12)
        r.raise_for_status()
        j = r.json()
        s["access_token"] = j.get("access_token", "")
        s["refresh_token"] = j.get("refresh_token", "")
        s["token_expires_at"] = int(time.time()) + int(j.get("expires_in", 0) or 0)
        log("SIMKL: tokens stored", level="SUCCESS", module="AUTH")
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        s = cfg.setdefault("simkl", {})
        if not s.get("refresh_token"):
            log("SIMKL: no refresh token", level="WARNING", module="AUTH")
            return self.get_status(cfg)
        data = {
            "grant_type": "refresh_token",
            "client_id": s.get("client_id", ""),
            "client_secret": s.get("client_secret", ""),
            "refresh_token": s.get("refresh_token", ""),
        }
        log("SIMKL: refresh token", level="INFO", module="AUTH")
        r = requests.post(SIMKL_TOKEN, json=data, headers={"User-Agent": UA, "Accept": "application/json"}, timeout=12)
        r.raise_for_status()
        j = r.json()
        s["access_token"] = j.get("access_token", "")
        s["refresh_token"] = j.get("refresh_token", "") or s.get("refresh_token", "")
        s["token_expires_at"] = int(time.time()) + int(j.get("expires_in", 0) or 0)
        log("SIMKL: refresh ok", level="SUCCESS", module="AUTH")
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        s = cfg.setdefault("simkl", {})
        for k in ("access_token", "refresh_token", "token_expires_at", "scopes", "account"):
            s.pop(k, None)
        log("SIMKL: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg)

PROVIDER = SimklAuth()

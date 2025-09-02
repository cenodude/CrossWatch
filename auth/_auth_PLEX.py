from __future__ import annotations
import time, requests
from typing import Any, Mapping, MutableMapping
from ._auth_base import AuthProvider, AuthStatus, AuthManifest
from _logging import log

PLEX_PIN_URL = "https://plex.tv/api/v2/pins"
UA = "CrossWatch/1.0"

class PlexAuth(AuthProvider):
    name = "PLEX"

    def manifest(self) -> AuthManifest:
        return AuthManifest(
            name="PLEX",
            label="Plex",
            flow="device_pin",
            fields=[],
            actions={"start": True, "finish": True, "refresh": False, "disconnect": True},
            verify_url="https://plex.tv/pin",
            notes="Open Plex, enter the PIN, then click 'Check PIN'.",
        )

    def capabilities(self) -> dict:
        # Plex can read/write watchlist, ratings, watched; collections read-only
        return {
            "features": {
                "watchlist": {"read": True, "write": True},
                "collections": {"read": True, "write": False},
                "ratings": {"read": True, "write": True, "scale": "1-10"},
                "watched": {"read": True, "write": True},
                "liked_lists": {"read": False, "write": False},
            },
            "entity_types": ["movie", "show"],
        }

    def get_status(self, cfg: Mapping[str, Any]) -> AuthStatus:
        token = (cfg.get("plex") or {}).get("account_token") or ""
        return AuthStatus(connected=bool(token), label="Plex")

    def start(self, cfg: MutableMapping[str, Any], redirect_uri: str) -> dict[str, str]:
        log("Plex: request PIN", level="INFO", module="AUTH")
        r = requests.post(PLEX_PIN_URL, headers={"Accept":"application/json","User-Agent":UA}, timeout=10)
        r.raise_for_status()
        j = r.json()
        cfg.setdefault("plex", {})["_pending_pin"] = {"id": j["id"], "code": j["code"], "created": int(time.time())}
        log("Plex: PIN issued", level="SUCCESS", module="AUTH", extra={"pin_id": j["id"]})
        return {"pin": j["code"], "verify_url": "https://plex.tv/pin"}

    def finish(self, cfg: MutableMapping[str, Any], **payload) -> AuthStatus:
        plex = cfg.setdefault("plex", {})
        pend = plex.get("_pending_pin") or {}
        if not pend:
            log("Plex: no pending PIN", level="WARNING", module="AUTH")
            return self.get_status(cfg)
        url = f"{PLEX_PIN_URL}/{pend['id']}"
        r = requests.get(url, headers={"Accept":"application/json","User-Agent":UA}, timeout=10)
        r.raise_for_status()
        j = r.json()
        if j.get("authToken"):
            plex["account_token"] = j["authToken"]
            plex.pop("_pending_pin", None)
            log("Plex: token stored", level="SUCCESS", module="AUTH")
        else:
            log("Plex: token not ready", level="INFO", module="AUTH")
        return self.get_status(cfg)

    def refresh(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        log("Plex: refresh noop", level="DEBUG", module="AUTH")
        return self.get_status(cfg)

    def disconnect(self, cfg: MutableMapping[str, Any]) -> AuthStatus:
        cfg.setdefault("plex", {}).pop("account_token", None)
        cfg["plex"].pop("_pending_pin", None)
        log("Plex: disconnected", level="INFO", module="AUTH")
        return self.get_status(cfg)

PROVIDER = PlexAuth()

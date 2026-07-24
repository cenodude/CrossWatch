# providers/sync/_mod_NUVIO.py
# CrossWatch Nuvio sync module
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from collections.abc import Iterable, Mapping
from typing import Any

from providers.auth._auth_NUVIO import (
    NuvioAuthError,
    NuvioClient,
    NuvioInvalidResponse,
    NuvioProfileUnavailable,
    NuvioServiceUnavailable,
    NuvioTokenRefreshError,
    is_configured as nuvio_is_configured,
    profile_id_value,
    provider_block,
)

__VERSION__ = "0.1"
__all__ = ["get_manifest", "NUVIOModule", "OPS"]


def _features_flags() -> dict[str, bool]:
    return {"watchlist": False, "ratings": False, "history": False, "progress": False, "playlists": False}


def get_manifest() -> Mapping[str, Any]:
    return {
        "name": "NUVIO",
        "label": "Nuvio",
        "version": __VERSION__,
        "type": "sync",
        "bidirectional": False,
        "experimental": True,
        "features": _features_flags(),
        "requires": [],
        "capabilities": {"bidirectional": False, "experimental": True},
    }


class NUVIOModule:
    def __init__(self, cfg: Mapping[str, Any]):
        self.config = cfg or {}
        self.instance_id = "default"
        self.client = NuvioClient(self.config, instance_id=self.instance_id)

    @staticmethod
    def supported_features() -> dict[str, bool]:
        return _features_flags()

    def manifest(self) -> Mapping[str, Any]:
        return get_manifest()

    def health(self) -> Mapping[str, Any]:
        start = time.perf_counter()
        status = "not_configured"
        ok = False
        reason: str | None = None
        try:
            block = provider_block(self.config, self.instance_id)
            if not nuvio_is_configured(block):
                if not profile_id_value(block):
                    reason = "missing_profile"
                else:
                    reason = "missing_authentication"
            else:
                profiles = self.client.pull_profiles(self.config, refresh=True)
                pid = profile_id_value(block)
                if not isinstance(profiles, list):
                    status = "invalid_response"
                    reason = "profiles_not_list"
                elif not any(int(p.get("profile_id") or 0) == pid for p in profiles if isinstance(p, Mapping)):
                    status = "profile_unavailable"
                    reason = "profile_unavailable"
                else:
                    ok = True
                    status = "ok"
        except NuvioTokenRefreshError:
            status = "token_refresh_failed"
            reason = "token_refresh_failed"
        except NuvioAuthError:
            status = "auth_failed"
            reason = "auth_failed"
        except NuvioProfileUnavailable:
            status = "profile_unavailable"
            reason = "profile_unavailable"
        except NuvioInvalidResponse:
            status = "invalid_response"
            reason = "invalid_response"
        except NuvioServiceUnavailable:
            status = "service_unavailable"
            reason = "service_unavailable"
        except Exception:
            status = "service_unavailable"
            reason = "service_unavailable"
        latency_ms = int((time.perf_counter() - start) * 1000)
        return {
            "ok": ok,
            "status": status,
            "latency_ms": latency_ms,
            "features": _features_flags(),
            "details": {"reason": reason} if reason else None,
            "api": {"profiles": {"status": status}},
        }


class _NUVIOOPS:
    def name(self) -> str:
        return "NUVIO"

    def label(self) -> str:
        return "Nuvio"

    def features(self) -> Mapping[str, bool]:
        return _features_flags()

    def state_read_features(self) -> Mapping[str, bool]:
        return _features_flags()

    def capabilities(self) -> Mapping[str, Any]:
        return get_manifest()["capabilities"]

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return nuvio_is_configured(provider_block(cfg, "default"))

    def _adapter(self, cfg: Mapping[str, Any]) -> NUVIOModule:
        return NUVIOModule(cfg)

    def health(self, cfg: Mapping[str, Any]) -> Mapping[str, Any]:
        return self._adapter(cfg).health()

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        return {}

    def add(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return {"ok": False, "count": 0, "unresolved": list(items or []), "unsupported": True}

    def remove(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        return {"ok": False, "count": 0, "unresolved": list(items or []), "unsupported": True}


OPS = _NUVIOOPS()

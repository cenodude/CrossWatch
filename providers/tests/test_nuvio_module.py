# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest


def _cfg(profile_id: int = 1) -> dict[str, Any]:
    return {
        "nuvio": {
            "base_url": "https://api.nuvio.tv",
            "access_token": "access",
            "refresh_token": "refresh",
            "expires_at": 1_900_000_000,
            "profile_id": profile_id,
            "profile_name": f"Profile {profile_id}",
        }
    }


def test_nuvio_ops_and_manifest_keep_all_sync_features_disabled() -> None:
    import sync._mod_NUVIO as mod

    manifest = mod.get_manifest()

    assert manifest["name"] == "NUVIO"
    assert manifest["label"] == "Nuvio"
    assert manifest["type"] == "sync"
    assert manifest["bidirectional"] is False
    assert manifest["experimental"] is True
    assert manifest["features"] == {"watchlist": False, "ratings": False, "history": False, "progress": False, "playlists": False}
    assert mod.OPS.features() == manifest["features"]
    assert mod.OPS.state_read_features() == manifest["features"]
    assert mod.OPS.capabilities()["bidirectional"] is False


def test_nuvio_is_configured_requires_auth_and_profile() -> None:
    import sync._mod_NUVIO as mod

    assert mod.OPS.is_configured({}) is False
    assert mod.OPS.is_configured({"nuvio": {"base_url": "https://api.nuvio.tv", "refresh_token": "refresh", "profile_id": 1}}) is True
    assert mod.OPS.is_configured({"nuvio": {"base_url": "https://api.nuvio.tv", "refresh_token": "refresh"}}) is False
    assert mod.OPS.is_configured({"nuvio": {"base_url": "https://api.nuvio.tv", "profile_id": 1}}) is False


def test_nuvio_health_success_and_no_write(monkeypatch: pytest.MonkeyPatch) -> None:
    import sync._mod_NUVIO as mod
    from providers.auth import _auth_NUVIO as common

    monkeypatch.setattr(common, "save_config", lambda _cfg: pytest.fail("health must not write config"))
    monkeypatch.setattr(mod.NuvioClient, "pull_profiles", lambda self, cfg, refresh=True: [{"profile_id": 1, "name": "Profile 1"}])

    health = mod.NUVIOModule(_cfg()).health()

    assert health["ok"] is True
    assert health["status"] == "ok"
    assert health["features"] == {"watchlist": False, "ratings": False, "history": False, "progress": False, "playlists": False}


def test_nuvio_health_reports_profile_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    import sync._mod_NUVIO as mod

    monkeypatch.setattr(mod.NuvioClient, "pull_profiles", lambda self, cfg, refresh=True: [{"profile_id": 2, "name": "Other"}])

    health = mod.NUVIOModule(_cfg()).health()

    assert health["ok"] is False
    assert health["status"] == "profile_unavailable"


def test_nuvio_health_reports_invalid_response(monkeypatch: pytest.MonkeyPatch) -> None:
    import sync._mod_NUVIO as mod

    def invalid(self, cfg, refresh=True):
        raise mod.NuvioInvalidResponse("profiles_not_list")

    monkeypatch.setattr(mod.NuvioClient, "pull_profiles", invalid)

    health = mod.NUVIOModule(_cfg()).health()

    assert health["ok"] is False
    assert health["status"] == "invalid_response"


def test_nuvio_health_reports_auth_failure(monkeypatch: pytest.MonkeyPatch) -> None:
    import sync._mod_NUVIO as mod

    def auth_failed(self, cfg, refresh=True):
        raise mod.NuvioAuthError("authentication_failed")

    monkeypatch.setattr(mod.NuvioClient, "pull_profiles", auth_failed)

    health = mod.NUVIOModule(_cfg()).health()

    assert health["ok"] is False
    assert health["status"] == "auth_failed"

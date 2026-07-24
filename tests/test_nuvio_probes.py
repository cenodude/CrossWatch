# CrossWatch test scripts
from __future__ import annotations

import json
from typing import Any


def _configured(profile_id: int = 1, token: str = "access-token") -> dict[str, Any]:
    return {
        "nuvio": {
            "base_url": "https://api.nuvio.tv",
            "access_token": token,
            "refresh_token": "refresh-token",
            "expires_at": 1_900_000_000,
            "profile_id": profile_id,
            "profile_name": f"Profile {profile_id}",
        }
    }


def test_nuvio_probe_registration_and_unconfigured_reason() -> None:
    from api import probesAPI as probes

    assert "nuvio" in probes.PROVIDERS
    assert probes.PROBE_CFG_KEY["NUVIO"] == "nuvio"
    assert probes.DETAIL_PROBES["NUVIO"] is probes._probe_nuvio_detail

    ok, reason = probes._probe_nuvio_detail({"nuvio": {}}, max_age_sec=0)

    assert ok is False
    assert reason == "Nuvio: missing authentication"


def test_nuvio_probe_validates_selected_profile(monkeypatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_NUVIO as common

    probes.invalidate_provider_caches("nuvio")
    monkeypatch.setattr(common.NuvioClient, "pull_profiles", lambda self, cfg, refresh=True: [{"profile_id": 1, "name": "Main"}])

    ok, reason = probes._probe_nuvio_detail(_configured(1), max_age_sec=0)
    missing_ok, missing_reason = probes._probe_nuvio_detail(_configured(2), max_age_sec=0)

    assert (ok, reason) == (True, "")
    assert (missing_ok, missing_reason) == (False, "Nuvio: profile unavailable")


def test_nuvio_probe_reports_refresh_failure(monkeypatch) -> None:
    from api import probesAPI as probes
    from providers.auth import _auth_NUVIO as common

    probes.invalidate_provider_caches("nuvio")

    def fail_refresh(self, cfg, refresh=True):
        raise common.NuvioTokenRefreshError("invalid_refresh")

    monkeypatch.setattr(common.NuvioClient, "pull_profiles", fail_refresh)

    ok, reason = probes._probe_nuvio_detail(_configured(1), max_age_sec=0)

    assert ok is False
    assert reason == "Nuvio: token refresh failed"


def test_nuvio_probe_keys_are_profile_specific_and_redacted() -> None:
    from api import probesAPI as probes

    key1 = probes._probe_key("nuvio", _configured(1, token="same-raw-token"))
    key2 = probes._probe_key("nuvio", _configured(2, token="same-raw-token"))

    assert key1 != key2
    assert "same-raw-token" not in key1
    assert "same-raw-token" not in key2
    assert "refresh-token" not in key1
    assert "refresh-token" not in key2
    assert "|profile:1" in key1
    assert "|profile:2" in key2


def test_status_includes_nuvio_instance_payload_without_secrets(monkeypatch) -> None:
    from fastapi import FastAPI
    from fastapi.testclient import TestClient

    from api import probesAPI as probes

    cfg = _configured(1, token="raw-status-token")
    cfg["nuvio"]["refresh_token"] = "raw-refresh-token"

    def fake_probe(view, max_age_sec=0):
        provider = str((view.get("_cw_probe") or {}).get("provider") or "").upper()
        return (provider == "NUVIO", "" if provider == "NUVIO" else "not configured")

    monkeypatch.setattr(probes, "DETAIL_PROBES", {key: fake_probe for key in probes.DETAIL_PROBES})
    probes.invalidate_provider_caches("nuvio")
    app = FastAPI()
    probes.register_probes(app, lambda: cfg)

    data = TestClient(app).get("/api/status?fresh=1").json()
    body = json.dumps(data)

    assert data["nuvio_connected"] is True
    assert data["providers"]["NUVIO"]["connected"] is True
    assert data["providers"]["NUVIO"]["profile_name"] == "Profile 1"
    assert data["providers"]["NUVIO"]["instances"]["default"]["configured"] is True
    assert data["providers"]["NUVIO"]["instances"]["default"]["probed"] is True
    assert "raw-status-token" not in body
    assert "raw-refresh-token" not in body
    assert "public-key" not in body


def test_nuvio_cache_invalidation_clears_detail_and_status_cache() -> None:
    from api import probesAPI as probes

    probes.PROBE_CACHE["nuvio"] = (123.0, True)
    probes.PROBE_DETAIL_CACHE["nuvio|tok:x"] = (123.0, True, "")
    probes.STATUS_CACHE["ts"] = 123.0
    probes.STATUS_CACHE["data"] = {"nuvio_connected": True}

    probes.invalidate_provider_caches("nuvio")

    assert probes.PROBE_CACHE["nuvio"] == (0.0, False)
    assert "nuvio|tok:x" not in probes.PROBE_DETAIL_CACHE
    assert probes.STATUS_CACHE["ts"] == 0.0
    assert probes.STATUS_CACHE["data"] is None

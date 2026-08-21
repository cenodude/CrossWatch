# CrossWatch BingeBase probe tests
from __future__ import annotations

from typing import Any


def _configured(**override: Any) -> dict[str, Any]:
    block = {
        "access_token": "device-access",
        "username": "lee",
        "user_id": "usr_1",
        "webhook_url": "https://bingebase.com/api/webhooks/jellyfin?token=secret-token",
        "api_key": "bearer-secret",
    }
    block.update(override)
    return {"bingebase": block}


def test_bingebase_probe_key_redacts_secret_url() -> None:
    from api import probesAPI as probes

    key = probes._probe_key("bingebase", _configured())

    assert "secret-token" not in key
    assert "bearer-secret" not in key
    assert "token=" not in key
    assert key.startswith("bingebase|")


def test_bingebase_configured_detection_accepts_auth_or_webhook() -> None:
    from api import probesAPI as probes

    assert probes._prov_configured(_configured(), "BINGEBASE", "default") is True
    assert probes._prov_configured(_configured(access_token=""), "BINGEBASE", "default") is True
    assert probes._prov_configured({"bingebase": {}}, "BINGEBASE", "default") is False


def test_bingebase_detail_probe_is_local_and_config_based() -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("bingebase")

    ok, reason = probes._probe_bingebase_detail(_configured(), max_age_sec=0)
    webhook_only_ok, webhook_only_reason = probes._probe_bingebase_detail(_configured(access_token=""), max_age_sec=0)
    missing_ok, missing_reason = probes._probe_bingebase_detail({"bingebase": {}}, max_age_sec=0)

    assert ok is True and reason == ""
    assert webhook_only_ok is True
    assert webhook_only_reason == "BingeBase: realtime webhook configured"
    assert missing_ok is False
    assert missing_reason == "BingeBase: missing authentication or webhook URL"


def test_bingebase_user_info_never_includes_webhook_url() -> None:
    from api import probesAPI as probes

    probes.invalidate_provider_caches("bingebase")
    info = probes.bingebase_user_info(_configured(), max_age_sec=0)

    assert info["username"] == "lee"
    assert info["user_id"] == "usr_1"
    assert info["auth_configured"] is True
    assert info["webhook_configured"] is True
    assert info["api_key_configured"] is True
    assert "webhook_url" not in info
    assert "api_key" not in info
    assert "secret-token" not in repr(info)
    assert "bearer-secret" not in repr(info)


def test_bingebase_is_registered_as_a_probe_provider() -> None:
    from api import probesAPI as probes

    assert "bingebase" in probes.PROVIDERS
    assert probes.PROBE_CFG_KEY["BINGEBASE"] == "bingebase"
    assert probes.DETAIL_PROBES["BINGEBASE"] is probes._probe_bingebase_detail
    assert probes.USERINFO_FNS["BINGEBASE"] is probes.bingebase_user_info

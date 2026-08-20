from __future__ import annotations

from typing import Any

import pytest

from cw_platform.provider_instances import build_provider_config_view
from services.playback_progress import get_service
from services.playback_progress.service import _profile_has_explicit_identity


SCROB_CREDS = {"server_url": "http://scrob.local", "api_key": "k", "username": "u", "password": "p"}
NAMED_ONLY = {"scrob": {"instances": {"SCROB-P01": dict(SCROB_CREDS)}}}


def _scrob_specs(cfg: Any) -> list[str]:
    return [s["instance_id"] for s in get_service().provider_instances(cfg) if s["provider"] == "scrob"]


def test_unconfigured_default_is_not_enumerated() -> None:
    assert _profile_has_explicit_identity(NAMED_ONLY, "scrob", "default") is False


def test_named_profile_is_enumerated() -> None:
    assert _profile_has_explicit_identity(NAMED_ONLY, "scrob", "SCROB-P01") is True


def test_only_the_named_profile_reaches_the_refresh() -> None:
    assert _scrob_specs(NAMED_ONLY) == ["SCROB-P01"]


def test_legacy_single_instance_default_still_enumerated() -> None:
    cfg = {"scrob": dict(SCROB_CREDS)}

    assert _profile_has_explicit_identity(cfg, "scrob", "default") is True
    assert _scrob_specs(cfg) == ["default"]


@pytest.mark.parametrize(
    "cfg",
    [
        {"plex": {"account_token": "tok"}},
        {"plex": {"account_token": "tok", "instances": {"PLEX-P01": {"account_token": "t2"}}}},
    ],
    ids=["legacy_only", "legacy_plus_named"],
)
def test_configured_default_is_still_enumerated(cfg: dict[str, Any]) -> None:
    assert _profile_has_explicit_identity(cfg, "plex", "default") is True


def test_configured_default_still_reports_readable_capabilities() -> None:
    cfg = {"scrob": dict(SCROB_CREDS)}
    adapter = get_service()._adapter("scrob")
    assert adapter is not None

    cap = adapter.capabilities(
        build_provider_config_view(cfg, "scrob", "default"),
        instance_id="default",
        instance_label="Default",
    )

    assert cap.configured is True
    assert cap.read is True


def test_crosswatch_default_is_always_enumerated(tmp_path) -> None:
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "instances": {"CW-P01": {"label": "Desk"}}}}

    assert _profile_has_explicit_identity(cfg, "crosswatch", "default") is True
    assert _profile_has_explicit_identity(cfg, "crosswatch", "CW-P01") is True
    assert _profile_has_explicit_identity(cfg, "crosswatch", "CW-NOPE") is False

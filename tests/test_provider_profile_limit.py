# tests/test_provider_profile_limit.py
# CrossWatch - Configurable Profiles Per Provider Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import pytest

import api.providerInstancesAPI as provider_api


def _cfg(limit: Any = None) -> dict[str, Any]:
    runtime: dict[str, Any] = {}
    if limit is not None:
        runtime["max_profiles_per_provider"] = limit
    return {"runtime": runtime}


def _insts(count: int, prov: str = "PLEX") -> dict[str, Any]:
    out: dict[str, Any] = {"default": {}}
    for n in range(1, count + 1):
        out[f"{prov}-P{n:02d}"] = {}
    return out


def test_default_limit_is_ten_total():
    assert provider_api._max_profiles_per_provider(_cfg()) == 10
    assert provider_api._max_generated_profiles(_cfg()) == 9


def test_config_key_raises_the_limit():
    assert provider_api._max_profiles_per_provider(_cfg(25)) == 25
    assert provider_api._max_generated_profiles(_cfg(25)) == 24


@pytest.mark.parametrize(("raw", "expected"), [(0, 1), (-5, 1), (500, 100), ("no", 10), (None, 10)])
def test_limit_is_clamped_and_falls_back(raw, expected):
    assert provider_api._max_profiles_per_provider(_cfg(raw)) == expected


def test_next_profile_id_stops_at_the_default_limit():
    assert provider_api._next_profile_id("PLEX", _insts(8), _cfg()) == "PLEX-P09"
    assert provider_api._next_profile_id("PLEX", _insts(9), _cfg()) == ""


def test_next_profile_id_continues_past_ten_when_raised():
    assert provider_api._next_profile_id("PLEX", _insts(9), _cfg(12)) == "PLEX-P10"
    assert provider_api._next_profile_id("PLEX", _insts(11), _cfg(12)) == ""


def test_ids_past_ten_are_rejected_by_default_but_valid_when_raised():
    assert provider_api._canonical_profile_id("PLEX", "PLEX-P10", _cfg()) == ""
    assert provider_api._canonical_profile_id("PLEX", "PLEX-P10", _cfg(12)) == "PLEX-P10"


def test_two_digit_ids_keep_their_format():
    assert provider_api._canonical_profile_id("PLEX", "PLEX-P11", _cfg(20)) == "PLEX-P11"
    assert provider_api._next_profile_id("PLEX", _insts(10), _cfg(20)) == "PLEX-P11"


def test_default_instance_and_junk_are_still_handled():
    assert provider_api._canonical_profile_id("PLEX", "default", _cfg(20)) is None
    assert provider_api._canonical_profile_id("PLEX", "PLEX-P00", _cfg(20)) == ""
    assert provider_api._canonical_profile_id("PLEX", "nonsense", _cfg(20)) == ""


def test_limit_resolves_from_config_when_not_passed(monkeypatch):
    monkeypatch.setattr(provider_api, "load_config", lambda: _cfg(15))
    assert provider_api._max_profiles_per_provider() == 15
    assert provider_api._next_profile_id("PLEX", _insts(9)) == "PLEX-P10"

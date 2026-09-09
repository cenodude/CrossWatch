# tests/test_provider_profile_limit_e2e.py
# CrossWatch - Configurable Profiles Per Provider End To End Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

import api.providerInstancesAPI as provider_api
from cw_platform.config_base import load_config


def _write_cfg(base: Path, runtime: dict[str, Any] | None) -> None:
    cfg: dict[str, Any] = {"plex": {"instances": {}}}
    if runtime is not None:
        cfg["runtime"] = runtime
    (base / "config.json").write_text(json.dumps(cfg), "utf-8")


def _req():
    from tests.test_app_auth_api import _request

    return _request("/api/provider-instances/PLEX/next", method="POST")


def _create_until_blocked(max_attempts: int = 40) -> tuple[list[str], dict[str, Any]]:
    created: list[str] = []
    last: dict[str, Any] = {}
    for _ in range(max_attempts):
        res = provider_api.api_provider_instances_create_next("PLEX", {}, _req())
        last = res if isinstance(res, dict) else {}
        if not last.get("ok"):
            break
        created.append(str(last.get("id")))
    return created, last


def test_default_config_stops_at_ten_profiles(config_base: Path):
    _write_cfg(config_base, None)

    created, last = _create_until_blocked()

    assert len(created) == 9
    assert created[0] == "PLEX-P01"
    assert created[-1] == "PLEX-P09"
    assert last.get("error") == "profile_limit_reached"
    assert last.get("limit") == 10

    insts = (load_config().get("plex") or {}).get("instances") or {}
    assert len(insts) == 9


def test_raising_the_key_allows_more_profiles(config_base: Path):
    _write_cfg(config_base, {"max_profiles_per_provider": 15})

    created, last = _create_until_blocked()

    assert len(created) == 14
    assert created[-1] == "PLEX-P14"
    assert last.get("error") == "profile_limit_reached"
    assert last.get("limit") == 15

    insts = (load_config().get("plex") or {}).get("instances") or {}
    assert "PLEX-P10" in insts and "PLEX-P14" in insts


def test_missing_runtime_block_behaves_like_the_default(config_base: Path):
    _write_cfg(config_base, None)
    assert load_config()["runtime"]["max_profiles_per_provider"] == 10

    created, last = _create_until_blocked()

    assert len(created) == 9
    assert last.get("limit") == 10


def test_explicit_null_falls_back_to_the_default(config_base: Path):
    _write_cfg(config_base, {"max_profiles_per_provider": None})

    created, last = _create_until_blocked()

    assert len(created) == 9
    assert last.get("limit") == 10


def test_profiles_past_ten_survive_a_config_reload(config_base: Path):
    _write_cfg(config_base, {"max_profiles_per_provider": 12})
    created, _ = _create_until_blocked()
    assert "PLEX-P11" in created

    reloaded = (load_config().get("plex") or {}).get("instances") or {}
    assert "PLEX-P11" in reloaded

    canon = provider_api._canonical_profile_id("PLEX", "PLEX-P11", load_config())
    assert canon == "PLEX-P11"


def test_lowering_the_limit_keeps_existing_profiles_readable(config_base: Path):
    _write_cfg(config_base, {"max_profiles_per_provider": 12})
    created, _ = _create_until_blocked()
    assert "PLEX-P11" in created

    cfg = json.loads((config_base / "config.json").read_text("utf-8"))
    cfg["runtime"]["max_profiles_per_provider"] = 10
    (config_base / "config.json").write_text(json.dumps(cfg), "utf-8")

    insts = (load_config().get("plex") or {}).get("instances") or {}
    assert "PLEX-P11" in insts

    _, last = _create_until_blocked(max_attempts=3)
    assert last.get("error") == "profile_limit_reached"
    assert last.get("limit") == 10


@pytest.mark.parametrize("limit", [1, 2, 3])
def test_small_limits_are_honoured(config_base: Path, limit: int):
    _write_cfg(config_base, {"max_profiles_per_provider": limit})

    created, last = _create_until_blocked()

    assert len(created) == limit - 1
    assert last.get("limit") == limit

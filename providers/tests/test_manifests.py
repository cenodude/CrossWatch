from __future__ import annotations

import importlib
from dataclasses import dataclass
from typing import Any, Mapping

import pytest


@dataclass(frozen=True)
class ProviderCase:
    module_path: str
    expected_name: str
    minimal_cfg: dict[str, Any]
    empty_configured: bool
    can_construct_without_network: bool = True


CASES: tuple[ProviderCase, ...] = (
    ProviderCase(
        module_path="sync._mod_CROSSWATCH",
        expected_name="CROSSWATCH",
        minimal_cfg={"crosswatch": {"enabled": True}},
        empty_configured=True,
    ),
    ProviderCase(
        module_path="sync._mod_ANILIST",
        expected_name="ANILIST",
        minimal_cfg={"anilist": {"access_token": "tok"}},
        empty_configured=False,
    ),
    ProviderCase(
        module_path="sync._mod_SIMKL",
        expected_name="SIMKL",
        minimal_cfg={"simkl": {"api_key": "k", "access_token": "tok"}},
        empty_configured=False,
    ),
    ProviderCase(
        module_path="sync._mod_TMDB",
        expected_name="TMDB",
        minimal_cfg={"tmdb_sync": {"api_key": "k", "session_id": "s", "account_id": "1"}},
        empty_configured=False,
    ),
    ProviderCase(
        module_path="sync._mod_MDBLIST",
        expected_name="MDBLIST",
        minimal_cfg={"mdblist": {"api_key": "k"}},
        empty_configured=False,
    ),
    ProviderCase(
        module_path="sync._mod_EMBY",
        expected_name="EMBY",
        minimal_cfg={"emby": {"server": "http://localhost", "access_token": "tok", "user_id": "u"}},
        empty_configured=False,
    ),
    ProviderCase(
        module_path="sync._mod_JELLYFIN",
        expected_name="JELLYFIN",
        minimal_cfg={"jellyfin": {"server": "http://localhost", "access_token": "tok", "user_id": "u"}},
        empty_configured=False,
    ),
    ProviderCase(
        module_path="sync._mod_TAUTULLI",
        expected_name="TAUTULLI",
        minimal_cfg={"tautulli": {"server_url": "http://localhost", "api_key": "k"}},
        empty_configured=False,
    ),
    # TRAKT's connect() performs a network preflight
    ProviderCase(
        module_path="sync._mod_TRAKT",
        expected_name="TRAKT",
        minimal_cfg={"trakt": {"client_id": "cid", "access_token": "tok"}},
        empty_configured=False,
        can_construct_without_network=False,
    ),
)


def _import_provider(case: ProviderCase):
    if case.expected_name == "PLEX":
        pytest.importorskip("plexapi")
    return importlib.import_module(case.module_path)


@pytest.mark.parametrize("case", CASES)
def test_get_manifest_shape(case: ProviderCase):
    mod = _import_provider(case)
    manifest = mod.get_manifest()

    assert isinstance(manifest, Mapping)
    assert manifest.get("name") == case.expected_name
    assert isinstance(manifest.get("version"), str)
    assert manifest.get("type") == "sync"

    feats = manifest.get("features")
    assert isinstance(feats, Mapping)
    assert all(isinstance(v, bool) for v in feats.values())

    caps = manifest.get("capabilities")
    assert isinstance(caps, Mapping)
    assert "bidirectional" in caps
    assert "provides_ids" in caps
    assert "index_semantics" in caps


@pytest.mark.parametrize("case", CASES)
def test_ops_contract(case: ProviderCase):
    mod = _import_provider(case)
    ops = mod.OPS

    assert ops.name() == case.expected_name
    assert isinstance(ops.label(), str) and ops.label().strip()

    feats = ops.features()
    assert isinstance(feats, Mapping)
    assert all(isinstance(v, bool) for v in feats.values())

    assert bool(ops.is_configured({})) is case.empty_configured
    assert ops.is_configured(case.minimal_cfg) is True


@pytest.mark.parametrize("case", CASES)
def test_module_requires_config(case: ProviderCase, monkeypatch: pytest.MonkeyPatch):
    mod = _import_provider(case)

    # CrossWatch is always constructible.
    module_cls = getattr(mod, f"{case.expected_name}Module", None)
    assert module_cls is not None

    if case.expected_name == "CROSSWATCH":
        module_cls({})
        return

    # Modules that require config should raise if given empty config, unless they can be constructed without network access.
    if case.expected_name == "TRAKT":
        monkeypatch.setattr(mod.TRAKTClient, "connect", lambda self: self)

    with pytest.raises(Exception):
        module_cls({})

    module = module_cls(case.minimal_cfg)
    if hasattr(module, "manifest"):
        manifest = module.manifest()
    else:
        manifest = mod.get_manifest()
    assert manifest.get("name") == case.expected_name

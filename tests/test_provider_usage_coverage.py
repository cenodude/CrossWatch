# CrossWatch test scripts
from __future__ import annotations

from typing import Any
from types import SimpleNamespace

import pytest

from cw_platform.provider_usage import (
    WEBHOOK_SOURCE_PROVIDERS,
    find_provider_usage,
    provider_label,
)
from cw_platform.modules_registry import provider_names
from providers.scrobble.routes import ROUTE_PROVIDERS, ROUTE_SINKS


def webhook_cfg(sinks: list[str]) -> dict[str, Any]:
    return {
        "scrobble": {"enabled": True, "sources": {"webhook": True}, "webhook": {"sinks": list(sinks)}},
        "plex": {"account_token": "t"},
    }


def route_cfg(provider: str, sink: str) -> dict[str, Any]:
    return {
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True},
            "watch": {"routes": [{"id": "R1", "enabled": True, "provider": provider, "sink": sink}]},
        }
    }


@pytest.mark.parametrize("sink", sorted(ROUTE_SINKS))
def test_every_webhook_sink_is_protected_from_deletion(sink: str):
    usages = find_provider_usage(webhook_cfg(sorted(ROUTE_SINKS)), sink)
    assert usages, f"{sink} is an enabled webhook destination but the delete guard does not see it"
    assert any(u["feature"] == "webhook" and u["role"] == "sink" for u in usages), usages


@pytest.mark.parametrize("sink", sorted(ROUTE_SINKS))
def test_a_sink_that_is_not_selected_has_no_sink_usage(sink: str):
    others = [s for s in sorted(ROUTE_SINKS) if s != sink]
    usages = find_provider_usage(webhook_cfg(others), sink)
    assert not any(u["role"] == "sink" for u in usages)
    if sink not in WEBHOOK_SOURCE_PROVIDERS:
        assert usages == []


@pytest.mark.parametrize("provider", sorted(ROUTE_PROVIDERS))
def test_every_watcher_route_provider_is_protected(provider: str):
    usages = find_provider_usage(route_cfg(provider, "trakt"), provider)
    assert any(u["feature"] == "watcher" and u["role"] == "provider" for u in usages), usages


@pytest.mark.parametrize("sink", sorted(ROUTE_SINKS))
def test_every_watcher_route_sink_is_protected(sink: str):
    usages = find_provider_usage(route_cfg("plex", sink), sink)
    assert any(u["feature"] == "watcher" and u["role"] == "sink" for u in usages), usages


@pytest.mark.parametrize("provider", provider_names(upper=False))
def test_registered_providers_use_their_declared_label(provider: str):
    from cw_platform.modules_registry import load_sync_ops
    from providers.auth.registry import auth_provider_manifest

    ops = load_sync_ops(provider)
    expected = ops.label() if ops is not None else auth_provider_manifest(provider)["label"]
    assert provider_label(provider) == expected
    assert provider_label(provider, "P01") == f"{expected} P01"


@pytest.mark.parametrize("provider", provider_names(upper=False))
@pytest.mark.parametrize("role", ["source", "target"])
@pytest.mark.parametrize("enabled", [True, False])
def test_all_registered_providers_share_instance_scoped_delete_guard(provider, role, enabled):
    from api.provider_guard import usage_conflict_response
    import json

    cfg = {"pairs": [{"id": "shared", "enabled": enabled, role: provider.upper(), f"{role}_instance": "P01"}]}
    response = usage_conflict_response(cfg, provider, "P01")
    assert response is not None and response.status_code == 409
    payload = json.loads(response.body)
    assert payload["error"] == "provider_in_use"
    assert payload["usages"][0]["role"] == role
    assert payload["usages"][0]["enabled"] is enabled
    assert provider_label(provider, "P01") in payload["message"]
    assert usage_conflict_response(cfg, provider, "P02") is None


def test_new_sync_registration_needs_no_usage_label_entry(monkeypatch):
    from cw_platform import modules_registry

    monkeypatch.setitem(modules_registry.MODULES["SYNC"], "_mod_NEW", "test.new")
    monkeypatch.setattr(modules_registry, "import_module", lambda path: SimpleNamespace(OPS=SimpleNamespace(label=lambda: "New Provider")))
    assert provider_label("new", "P01") == "New Provider P01"


def test_new_auth_only_registration_needs_no_usage_label_entry(monkeypatch):
    from cw_platform import modules_registry
    from providers.auth import registry

    monkeypatch.setitem(modules_registry.MODULES["AUTH"], "_auth_NEW", "test.auth_new")
    monkeypatch.setattr(registry, "_safe_import", lambda path: SimpleNamespace(PROVIDER=SimpleNamespace(manifest=lambda: {"label": "New Auth Provider"})))
    assert provider_label("new") == "New Auth Provider"


def test_unavailable_sync_module_falls_back_to_auth_manifest(monkeypatch):
    from cw_platform import modules_registry

    def unavailable(name):
        raise ImportError("unavailable")

    monkeypatch.setattr(modules_registry, "load_sync_ops", unavailable)
    assert provider_label("kitsu") == "Kitsu"


def test_label_falls_back_without_crashing():
    assert provider_label("totally-unknown") == "TOTALLY-UNKNOWN"
    assert provider_label("") == "provider"


def test_instance_labels_are_suffixed():
    assert provider_label("scrob", "default") == "Scrob"
    assert provider_label("scrob", "second") == "Scrob second"


def test_webhook_source_role_is_still_detected():
    usages = find_provider_usage(webhook_cfg(["trakt"]), "plex")
    assert any(u["feature"] == "webhook" and u["role"] == "provider" for u in usages), usages


def test_disabled_webhook_source_releases_its_sinks():
    cfg = webhook_cfg(sorted(ROUTE_SINKS))
    cfg["scrobble"]["sources"]["webhook"] = False
    for sink in sorted(ROUTE_SINKS):
        assert find_provider_usage(cfg, sink) == []


def test_sync_pair_source_instance_is_protected_from_deletion():
    cfg = {
        "pairs": [
            {
                "id": "alice-sync",
                "enabled": True,
                "source": "JELLYFIN",
                "source_instance": "JELLYFIN-P02",
                "target": "CROSSWATCH",
                "target_instance": "CW-P01",
            }
        ]
    }

    usages = find_provider_usage(cfg, "jellyfin", "JELLYFIN-P02")

    assert any(u["feature"] == "sync_pair" and u["role"] == "source" for u in usages), usages


def test_sync_pair_target_instance_is_protected_from_deletion():
    cfg = {
        "pairs": [
            {
                "id": "alice-sync",
                "enabled": False,
                "source": "CROSSWATCH",
                "src_instance": "CW-P01",
                "target": "JELLYFIN",
                "dst_instance": "JELLYFIN-P02",
            }
        ]
    }

    usages = find_provider_usage(cfg, "jellyfin", "JELLYFIN-P02")

    assert any(u["feature"] == "sync_pair" and u["role"] == "target" and u["enabled"] is False for u in usages), usages


@pytest.mark.parametrize("sink", sorted(ROUTE_SINKS))
def test_every_route_sink_is_buildable_by_both_sink_factories(sink: str):
    from providers.scrobble.watch_manager import _make_sink as watcher_sink
    from providers.webhooks.dispatch import _make_sink as webhook_sink

    assert webhook_sink(sink, "default", lambda: {}) is not None, (
        f"{sink} can be selected as a webhook destination but the webhook dispatcher cannot build it"
    )
    assert watcher_sink(sink, lambda: {}, "default") is not None, (
        f"{sink} can be selected as a watcher route sink but the watcher cannot build it"
    )


def test_selectable_webhook_sinks_match_the_route_sinks():
    from providers.webhooks.config import _SINK_CREDENTIALS, _SINKS

    assert _SINKS == ROUTE_SINKS
    assert set(_SINK_CREDENTIALS) | {"crosswatch"} == ROUTE_SINKS


def read_asset(rel: str) -> str:
    from pathlib import Path

    return (Path(__file__).resolve().parents[1] / rel).read_text(encoding="utf-8", errors="ignore")


@pytest.mark.parametrize("modal", ["watcher", "webhook"])
def test_modals_enforce_instance_scoped_destinations(modal: str):
    import shutil
    import subprocess
    from pathlib import Path

    node = shutil.which("node")
    if not node:
        pytest.skip("Node.js unavailable")
    result = subprocess.run([node, "--test", f"--test-name-pattern={modal}", "tests/scrobbler-sink-instances.test.mjs"],
                            cwd=Path(__file__).resolve().parents[1], capture_output=True, text=True)
    assert result.returncode == 0, result.stdout + result.stderr


def test_both_modals_offer_every_route_sink():
    for rel in ("assets/js/modals/scrobbler-route/index.js", "assets/js/modals/scrobbler-webhook/index.js"):
        js = read_asset(rel)
        listed = js.split("const sinks = [", 1)[1].split("]", 1)[0]
        offered = {p.strip().strip('"') for p in listed.split(",") if p.strip()}
        assert offered == ROUTE_SINKS, f"{rel} offers {offered}, ROUTE_SINKS is {ROUTE_SINKS}"


@pytest.mark.parametrize("provider", sorted(ROUTE_PROVIDERS & ROUTE_SINKS))
def test_source_providers_require_distinct_destination_instances(provider: str):
    from providers.scrobble.routes import same_scrobble_endpoint

    assert same_scrobble_endpoint(provider, "default", provider, "default")
    assert same_scrobble_endpoint(provider, "P01", provider, "P01")
    assert not same_scrobble_endpoint(provider, "default", provider, "P01")
    assert not same_scrobble_endpoint(provider, "P01", provider, "default")

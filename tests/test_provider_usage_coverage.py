# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest

from cw_platform.provider_usage import (
    WEBHOOK_SOURCE_PROVIDERS,
    _PROVIDER_LABELS,
    find_provider_usage,
    provider_label,
)
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
def test_a_sink_that_is_not_selected_stays_deletable(sink: str):
    others = [s for s in sorted(ROUTE_SINKS) if s != sink]
    assert find_provider_usage(webhook_cfg(others), sink) == []


@pytest.mark.parametrize("provider", sorted(ROUTE_PROVIDERS))
def test_every_watcher_route_provider_is_protected(provider: str):
    usages = find_provider_usage(route_cfg(provider, "trakt"), provider)
    assert any(u["feature"] == "watcher" and u["role"] == "provider" for u in usages), usages


@pytest.mark.parametrize("sink", sorted(ROUTE_SINKS))
def test_every_watcher_route_sink_is_protected(sink: str):
    usages = find_provider_usage(route_cfg("plex", sink), sink)
    assert any(u["feature"] == "watcher" and u["role"] == "sink" for u in usages), usages


@pytest.mark.parametrize("provider", sorted(ROUTE_PROVIDERS | ROUTE_SINKS | set(WEBHOOK_SOURCE_PROVIDERS)))
def test_scrobbler_providers_have_a_display_label(provider: str):
    assert provider in _PROVIDER_LABELS, (
        f"{provider} can appear in the Scrobbler screen and the delete-conflict message, "
        "so it needs a label or it renders as an uppercase key"
    )
    assert provider_label(provider) == _PROVIDER_LABELS[provider]


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


def test_route_modal_never_offers_a_provider_as_its_own_destination():
    js = read_asset("assets/js/modals/scrobbler-route/index.js")

    assert 'function sinkProviders(selected = "", source = "") {' in js
    assert 'const available = sinks.filter((p) => p !== self && allSinkProfiles(p).length);' in js
    assert 'if (current === self) return available;' in js

    assert 'function ratingSinkProviders(selected = [], source = "") {' in js
    assert 'ratingSinks.includes(x) && x !== self' in js

    assert 'optionsForProviders("sink", r.sink, r.provider)' in js
    assert 'const sink = sinkProviders("", srcProvider)[0] || "";' in js
    assert 'ratingSinkProviders(ratingTargets, r.provider)' in js
    assert 'if (draft.sink === draft.provider) draft.sink = sinkProviders("", draft.provider)[0] || "";' in js


def test_webhook_modal_never_offers_a_provider_as_its_own_destination():
    js = read_asset("assets/js/modals/scrobbler-webhook/index.js")

    assert "function sourceProviderKey() {" in js
    assert "return sinks.filter((s) => s !== self && sinkProfiles(s).length > 0);" in js
    assert "return ratingSinks.filter((s) => s !== self && sinkProfiles(s).length > 0);" in js


def test_both_modals_offer_every_route_sink():
    for rel in ("assets/js/modals/scrobbler-route/index.js", "assets/js/modals/scrobbler-webhook/index.js"):
        js = read_asset(rel)
        listed = js.split("const sinks = [", 1)[1].split("]", 1)[0]
        offered = {p.strip().strip('"') for p in listed.split(",") if p.strip()}
        assert offered == ROUTE_SINKS, f"{rel} offers {offered}, ROUTE_SINKS is {ROUTE_SINKS}"


def test_scrob_is_the_only_provider_that_could_self_route():
    from providers.scrobble.routes import ROUTE_PROVIDERS

    assert ROUTE_PROVIDERS & ROUTE_SINKS == {"scrob"}

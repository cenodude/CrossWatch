# tests/test_editor_statuses.py
# CrossWatch - Read-only provider status lookup for the Editor.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from dataclasses import replace

import pytest
from fastapi import HTTPException

from api import editorAPI as api
from cw_platform.run_control import SyncCancelled
from cw_platform.status_mapping import Observation, ReadState, Status
from providers.sync import _status_mapping as adapters


@pytest.fixture
def status_reader(monkeypatch):
    configs = []
    record = adapters.Record("private-native-key", "shows", {"ids": {"tmdb": 1, "secret": "hidden"}},
                             Observation(ReadState.PRESENT, Status.ON_HOLD, "native", "private-fingerprint", False, False), {"private": "data"})
    class Adapter:
        def __init__(self, cfg):
            configs.append(cfg)

        def read(self):
            return {"private-native-key": record}

    monkeypatch.setattr(api, "load_config", lambda: {"crosswatch": {"instances": {"P01": {"label": "Personal"}}}})
    monkeypatch.setattr(api, "_instance_for_request", lambda cfg, request, provider, instance: instance or "P01")
    monkeypatch.setattr(api, "_require_instance_scope", lambda *args: None)
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: {provider.lower(): {"selected_instance": instance}})
    monkeypatch.setattr(adapters, "adapter_class", lambda provider: Adapter)
    return configs, record, Adapter


def test_status_lookup_is_instance_scoped_and_returns_only_display_fields(status_reader):
    configs, _, _ = status_reader
    result = api.api_editor_statuses("crosswatch", "P01")
    assert result == {"supported": True, "records": [{"media": "shows", "ids": {"tmdb": "1"}, "status": "on_hold"}]}
    assert configs[0]["_cw_readonly"] is True
    assert configs[0]["_cw_provider_instance"] == "P01"
    assert configs[0]["crosswatch"]["selected_instance"] == "P01"
    assert "P01" in configs[0]["crosswatch"]["instances"]


def test_status_lookup_denies_other_instances_before_reading(status_reader, monkeypatch):
    configs, _, _ = status_reader
    def deny(*args):
        raise HTTPException(403, "profile_scope_denied")
    monkeypatch.setattr(api, "_require_instance_scope", deny)
    with pytest.raises(HTTPException) as error:
        api.api_editor_statuses("crosswatch", "other")
    assert error.value.status_code == 403 and not configs


def test_incomplete_status_is_unknown(status_reader, monkeypatch):
    _, record, adapter = status_reader
    monkeypatch.setattr(adapter, "read", lambda self: {"1": replace(record, observation=replace(record.observation, rewatching=None))})
    assert api.api_editor_statuses("crosswatch")["records"][0]["status"] is None


@pytest.mark.parametrize("failure,code", [(TimeoutError("private token"), 502), (SyncCancelled(), 503)])
def test_status_read_failure_is_safe_and_retryable(status_reader, monkeypatch, failure, code):
    def fail(self):
        raise failure
    monkeypatch.setattr(status_reader[2], "read", fail)
    with pytest.raises(HTTPException) as error:
        api.api_editor_statuses("crosswatch")
    assert error.value.status_code == code
    assert "private token" not in error.value.detail


def test_unsupported_status_provider_does_not_read(status_reader, monkeypatch):
    monkeypatch.setattr(adapters, "adapter_class", lambda provider: None)
    assert api.api_editor_statuses("plex") == {"supported": False, "records": []}
    assert not status_reader[0]

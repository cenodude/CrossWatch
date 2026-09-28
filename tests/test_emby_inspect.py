# tests/test_emby_inspect.py
# CrossWatch - Emby selected user inspection regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from unittest.mock import Mock

import pytest
import requests

from providers.sync.emby import _utils


@pytest.mark.parametrize("instance", ["default", "secondary"])
@pytest.mark.parametrize("selected_user_id", ["child-id", ""])
def test_inspect_resolves_selected_user_or_discovers_token_owner(monkeypatch, instance, selected_user_id):
    admin = {
        "server": "http://emby.test/",
        "access_token": "test-token",
        "user": "Admin",
        "username": "Admin",
        "user_id": "admin-id",
    }
    cfg = {"emby": {**deepcopy(admin), "instances": {"secondary": deepcopy(admin)}}}
    selected = cfg["emby"] if instance == "default" else cfg["emby"]["instances"][instance]
    selected.update(user="Old name", username="Old name", user_id=selected_user_id)
    before = deepcopy(cfg)
    expected_id = selected_user_id or "admin-id"
    expected_name = "Child" if selected_user_id else "Admin"

    def get(url, **kwargs):
        user = {"Id": "admin-id", "Name": "Admin"} if url.endswith("/Users/Me") else {"Id": "child-id", "Name": "Child"}
        return Mock(ok=True, json=lambda: user)

    request = Mock(side_effect=get)
    saved = []
    monkeypatch.setattr(_utils.requests, "get", request)
    monkeypatch.setattr(_utils, "save_config", lambda data: saved.append(deepcopy(data)))

    result = _utils.inspect_and_persist(cfg, instance_id=instance)

    assert result["user_id"] == expected_id
    assert result["username"] == expected_name
    assert selected["user_id"] == expected_id
    assert selected["user"] == selected["username"] == expected_name
    assert selected["access_token"] == "test-token"
    assert saved == [cfg]
    assert request.call_count == 1
    assert request.call_args.args[0] == f"http://emby.test/Users/{selected_user_id or 'Me'}"
    assert request.call_args.kwargs["headers"]["X-Emby-Token"] == "test-token"
    if instance == "secondary":
        assert {k: v for k, v in cfg["emby"].items() if k != "instances"} == admin
    else:
        assert cfg["emby"]["instances"] == before["emby"]["instances"]


@pytest.mark.parametrize("failure", [403, 404, "timeout"])
def test_inspect_failure_keeps_selected_user_without_falling_back_to_admin(monkeypatch, failure):
    cfg = {"emby": {
        "server": "http://emby.test/", "access_token": "test-token",
        "user": "Child", "username": "Child", "user_id": "child-id",
    }}
    before = deepcopy(cfg)
    request = Mock(return_value=Mock(ok=False, status_code=failure))
    if failure == "timeout":
        request.side_effect = requests.Timeout()
    save = Mock()
    monkeypatch.setattr(_utils.requests, "get", request)
    monkeypatch.setattr(_utils, "save_config", save)

    result = _utils.inspect_and_persist(cfg)

    assert result["user_id"] == "child-id"
    assert result["username"] == "Child"
    assert cfg == before
    assert request.call_count == 1
    assert request.call_args.args[0] == "http://emby.test/Users/child-id"
    save.assert_not_called()

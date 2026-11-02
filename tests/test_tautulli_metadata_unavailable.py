# tests/test_tautulli_metadata_unavailable.py
# CrossWatch - Tautulli unavailable metadata regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest
import requests

from providers.sync import _mod_TAUTULLI as tautulli


def _response(status, result, data=None, message=None):
    response = requests.Response()
    response.status_code = status
    response.headers["Content-Type"] = "application/json"
    response._content = json.dumps({
        "response": {"result": result, "data": data, "message": message},
    }).encode()
    return response


def _module():
    return tautulli.TAUTULLIModule({
        "tautulli": {"server_url": "http://tautulli.test", "api_key": "test-key"},
    })


@pytest.mark.parametrize("status", [200, 400])
@pytest.mark.parametrize("media_type", ["movie", "episode"])
def test_missing_metadata_preserves_history_and_continues(monkeypatch, status, media_type):
    missing = {
        "media_type": media_type,
        "rating_key": "12495",
        "title": "Unavailable item",
        "date": 1704067200,
        "watched_status": 1,
    }
    if media_type == "episode":
        missing.update({
            "grandparent_rating_key": "100",
            "grandparent_title": "Example series",
            "parent_media_index": 1,
            "media_index": 2,
        })
    rows = [missing, dict(missing, date=1704153600), {
        "media_type": "movie",
        "rating_key": "42",
        "guid": "com.plexapp.agents.imdb://tt1234567",
        "title": "Available item",
        "date": 1704067200,
        "watched_status": 1,
    }]
    metadata_calls = []
    warnings = []

    def request(session, method, url, **kwargs):
        params = kwargs["params"]
        if params["cmd"] == "get_history":
            start, length = params["start"], params["length"]
            return _response(200, "success", {
                "data": rows[start:start + length], "recordsFiltered": len(rows),
            })
        assert params["cmd"] == "get_metadata"
        key = params["rating_key"]
        metadata_calls.append(key)
        return _response(status, "error", message=f"Unable to retrieve metadata for rating_key '{key}'")

    monkeypatch.setattr(tautulli, "request_with_retries", request)
    monkeypatch.setattr(tautulli, "_warn", lambda event, **fields: warnings.append(event))
    module = _module()
    module.client.raw_cfg["tautulli"]["history"] = {"per_page": 1}
    index = module.build_index("history")

    assert len(index) == 2
    assert {item["title"] for item in index.values()} == {"Unavailable item", "Available item"}
    item = next(item for item in index.values() if item["title"] == "Unavailable item")
    assert item["ids"]["plex"] == "12495"
    assert item["watched_at"] == "2024-01-02T00:00:00Z"
    if media_type == "episode":
        assert (item["season"], item["episode"], item["series_title"]) == (1, 2, "Example series")
    assert metadata_calls == (["12495", "100"] if media_type == "episode" else ["12495"])
    assert warnings == ["metadata_unavailable"] * len(metadata_calls)


@pytest.mark.parametrize(("cmd", "status", "message"), [
    ("get_metadata", 401, "Invalid apikey"),
    ("get_metadata", 400, "Invalid apikey"),
    ("get_metadata", 500, "Unable to retrieve metadata for rating_key '12495'"),
    ("get_metadata", 400, "Unexpected API failure"),
    ("get_metadata", 400, "Unable to retrieve metadata for rating_key '999'"),
    ("get_history", 400, "Unable to retrieve metadata for rating_key '12495'"),
])
def test_other_api_errors_still_fail(monkeypatch, cmd, status, message):
    monkeypatch.setattr(tautulli, "request_with_retries", lambda *args, **kwargs: _response(
        status, "error", message=message,
    ))
    with pytest.raises(RuntimeError) as error:
        _module().client.call(cmd, rating_key="12495")
    assert str(error.value) == message


@pytest.mark.parametrize("error_type", [requests.ConnectionError, requests.Timeout])
def test_transport_errors_still_fail(monkeypatch, error_type):
    def request(*args, **kwargs):
        raise error_type("Request failed")

    monkeypatch.setattr(tautulli, "request_with_retries", request)
    with pytest.raises(error_type):
        _module().client.call("get_metadata", rating_key="12495")

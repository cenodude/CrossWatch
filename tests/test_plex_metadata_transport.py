# /tests/test_plex_metadata_transport.py
# Plex metadata transport, SSL and logging regression tests.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from types import SimpleNamespace
import json

import pytest
import requests

from providers.sync.plex import _common as common


PMS = "https://plex.test:32400"


class MetadataTransport(requests.adapters.BaseAdapter):
    def __init__(self, *, status=200, fail=False):
        self.status = status
        self.fail = fail
        self.calls = []

    def send(self, request, **kwargs):
        self.calls.append((request, kwargs))
        if self.fail:
            raise requests.exceptions.SSLError("certificate verification failed")
        response = requests.Response()
        response.status_code = self.status
        response.headers["content-type"] = "application/json"
        response._content = json.dumps({"MediaContainer": {"Metadata": [{
            "Guid": [{"id": "tmdb://603"}], "grandparentGuid": "tmdb://1399",
        }]}}).encode()
        return response

    def close(self):
        pass


@pytest.fixture
def metadata(monkeypatch):
    for name in ("_GUID_CACHE", "_EP_SHOW_IDS_CACHE", "_SHOW_PMS_GUID_CACHE"):
        monkeypatch.setattr(common, name, {})
    monkeypatch.setattr(common, "_HYDRATE_404", set())
    session = requests.Session()
    transport = MetadataTransport()
    session.mount("https://", transport)
    public_session = requests.Session()
    public_transport = MetadataTransport(status=404)
    public_session.mount("https://", public_transport)
    monkeypatch.setattr(common.requests, "get", public_session.get)
    with common.isolated_plex_context():
        common.configure_plex_context(baseurl=PMS, token="server-token", account_token="cloud-token", session=session)
        yield SimpleNamespace(session=session, transport=transport, public=public_transport)
    session.close()
    public_session.close()


def hydrate(kind, session):
    if kind == "item":
        return common.hydrate_external_ids("server-token", "123")
    if kind == "episode_parent":
        return common._hydrate_show_ids_from_episode_rk("server-token", "123")
    server = SimpleNamespace(_baseurl=PMS, _token="server-token", _session=session)
    return common._hydrate_show_ids_from_pms(SimpleNamespace(_server=server, grandparentRatingKey="123"))


@pytest.mark.parametrize("kind", ["item", "episode_parent", "show"])
@pytest.mark.parametrize("verify", [True, False])
def test_metadata_uses_server_session_and_ssl_setting(metadata, monkeypatch, kind, verify):
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "environment-ca.pem")
    metadata.session.verify = verify

    assert hydrate(kind, metadata.session)

    request, kwargs = metadata.transport.calls[0]
    assert request.headers["X-Plex-Token"] == "server-token"
    assert kwargs["verify"] == ("environment-ca.pem" if verify else False)
    assert not metadata.public.calls


@pytest.mark.parametrize("kind", ["item", "episode_parent"])
def test_local_ssl_failure_is_logged_before_cloud_fallback(metadata, monkeypatch, kind):
    logs = []
    metadata.transport.fail = True
    monkeypatch.setattr(common, "_warn", lambda event, **fields: logs.append((event, fields)))

    assert not hydrate(kind, metadata.session)
    assert not hydrate(kind, metadata.session)

    assert len(metadata.transport.calls) == len(metadata.public.calls) == 1
    request, kwargs = metadata.public.calls[0]
    assert request.headers["X-Plex-Token"] == "cloud-token"
    assert kwargs["verify"] is False
    assert ("hydrate_request_failed", {"rk": "123", "source": "pms", "error_type": "SSLError"}) in logs


@pytest.mark.parametrize("kind", ["item", "episode_parent"])
@pytest.mark.parametrize("verify", [True, False])
def test_cloud_metadata_disables_validation_independently_of_server_setting(metadata, monkeypatch, kind, verify):
    monkeypatch.setenv("REQUESTS_CA_BUNDLE", "environment-ca.pem")
    metadata.session.verify = verify
    metadata.transport.status = 404

    assert not hydrate(kind, metadata.session)

    assert metadata.transport.calls[0][1]["verify"] == ("environment-ca.pem" if verify else False)
    assert metadata.public.calls[0][1]["verify"] is False


@pytest.mark.parametrize("kind", ["item", "episode_parent", "show"])
def test_changing_ssl_setting_retries_cached_metadata_miss(metadata, kind):
    metadata.transport.status = 404
    assert not hydrate(kind, metadata.session)
    assert not hydrate(kind, metadata.session)
    assert len(metadata.transport.calls) == 1

    metadata.session.verify = False
    metadata.transport.status = 200

    assert hydrate(kind, metadata.session)
    assert len(metadata.transport.calls) == 2
    assert metadata.transport.calls[-1][1]["verify"] is False


def test_connection_switch_replaces_metadata_session(metadata):
    other_session = requests.Session()
    other_session.verify = False
    other_transport = MetadataTransport()
    other_session.mount("https://", other_transport)
    try:
        assert common.hydrate_external_ids("server-token", "123")
        common.configure_plex_context(baseurl=PMS, token="other-token", session=other_session)
        assert common.hydrate_external_ids("other-token", "123")
        assert len(metadata.transport.calls) == len(other_transport.calls) == 1
        request, kwargs = other_transport.calls[0]
        assert request.headers["X-Plex-Token"] == "other-token"
        assert kwargs["verify"] is False
    finally:
        other_session.close()


def test_summary_mode_does_not_repeat_enrichment_miss_warnings(monkeypatch):
    logs = []
    monkeypatch.setenv("CW_PLEX_META_ENRICH_LOG", "summary")
    monkeypatch.setattr(common, "_META_ENRICH_LAST_FLUSH", common.time.monotonic())
    monkeypatch.setattr(common, "_META_ENRICH_COUNTS", {})
    monkeypatch.setattr(common, "cw_log", lambda *args, **kwargs: logs.append((args, kwargs)))

    for _ in range(3):
        common._emit({"event": "meta_enrich", "action": "enrich_by_rk_miss", "rk": "123"})

    assert not logs
    assert common._META_ENRICH_COUNTS["enrich_by_rk_miss"] == 3

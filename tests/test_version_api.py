# CrossWatch test scripts
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import responses


def test_version_endpoints_mock_github_release(monkeypatch) -> None:
    from api import versionAPI as v

    monkeypatch.setattr(v, "CURRENT_VERSION", "v0.1.0")
    monkeypatch.setattr(v, "REPO", "example/CrossWatch")
    v._cached_latest_release.cache_clear()

    app = FastAPI()
    app.include_router(v.router)
    client = TestClient(app)

    url = f"https://api.github.com/repos/{v.REPO}/releases/latest"

    with responses.RequestsMock() as rsps:
        rsps.add(
            responses.GET,
            url,
            json={
                "tag_name": "v0.2.0",
                "html_url": "https://github.com/example/CrossWatch/releases/tag/v0.2.0",
                "body": "Notes",
                "published_at": "2026-01-01T00:00:00Z",
            },
            status=200,
        )

        r = client.get("/api/version")
        assert r.status_code == 200
        data = r.json()
        assert data["current"] == "0.1.0"
        assert data["latest"] == "0.2.0"
        assert data["update_available"] is True

        r2 = client.get("/api/update")
        assert r2.status_code == 200
        u = r2.json()
        assert u["latest_version"] == "0.2.0"
        assert u["body"] == "Notes"
        assert u["update_available"] is True


def test_installed_release_notes_use_matching_tag_and_cache(monkeypatch) -> None:
    from api import versionAPI as v

    monkeypatch.setattr(v, "CURRENT_VERSION", "v0.11.7")
    monkeypatch.setattr(v, "REPO", "example/CrossWatch")
    monkeypatch.setattr(v, "_ttl_marker", lambda _: 1)
    v._cached_installed_release.cache_clear()
    app = FastAPI()
    app.include_router(v.router)
    client = TestClient(app)
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, "https://api.github.com/repos/example/CrossWatch/releases/tags/v0.11.7",
                 json={"tag_name": "v0.11.7", "body": "## Highlights\n- A change"})
        first = client.get("/api/version/release-notes").json()
        assert first == client.get("/api/version/release-notes").json()
        assert first["version"] == "0.11.7"
        assert first["body"] == "## Highlights\n- A change"
        assert first["html_url"] == "https://github.com/example/CrossWatch/releases/tag/v0.11.7"
        assert len(rsps.calls) == 1


def test_unpublished_build_does_not_fall_back_to_latest_release() -> None:
    from api import versionAPI as v

    v._cached_installed_release.cache_clear()
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, "https://api.github.com/repos/example/CrossWatch/releases/tags/v0.12.0-dev",
                 status=404)
        result = v._cached_installed_release("example/CrossWatch", "0.12.0-dev", 1)
        assert result["body"] == ""
        assert result["html_url"] == "https://github.com/example/CrossWatch/releases"
        assert len(rsps.calls) == 1


def test_release_notes_reject_mismatched_tag_and_handle_network_failure() -> None:
    from api import versionAPI as v
    import requests

    v._cached_installed_release.cache_clear()
    url = "https://api.github.com/repos/example/CrossWatch/releases/tags/v0.11.7"
    with responses.RequestsMock() as rsps:
        rsps.add(responses.GET, url, json={"tag_name": "v0.12.0", "body": "Wrong release"})
        assert v._cached_installed_release("example/CrossWatch", "0.11.7", 1)["body"] == ""
        rsps.add(responses.GET, url, body=requests.ConnectionError("offline"))
        assert v._cached_installed_release("example/CrossWatch", "0.11.7", 2)["body"] == ""

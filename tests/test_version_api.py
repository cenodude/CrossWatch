# CrossWatch test scripts
from __future__ import annotations

from fastapi import FastAPI
from fastapi.testclient import TestClient

import responses


def test_version_endpoints_mock_github_release() -> None:
    from api import versionAPI as v

    v.CURRENT_VERSION = "v0.1.0"
    v.REPO = "example/CrossWatch"
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

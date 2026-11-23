# tests/test_user_pages.py
# CrossWatch - Independent Profile and Hub routes
# Copyright (c) 2025-2026 CrossWatch / Cenodude
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import ui_frontend
from api import authOidcAPI


@pytest.mark.parametrize("is_admin,write", [(True, True), (False, True), (False, False)])
def test_profile_and_hub_have_independent_routes(is_admin, write):
    user = {"is_admin": is_admin, "username": "pascal", "permissions": {"write": write}}
    app = FastAPI()

    @app.middleware("http")
    async def identity(request, call_next):
        request.state.cw_user = user
        return await call_next(request)

    ui_frontend.register_ui_root(app)
    client = TestClient(app)
    profile = client.get("/profile")
    hub = client.get("/hub")
    assert profile.status_code == hub.status_code == 200
    assert profile.headers["cache-control"] == hub.headers["cache-control"] == "no-store"
    assert 'data-cw-page="profile"' in profile.text
    assert 'data-cw-page="hub"' in hub.text
    assert 'id="profile-settings-avatar"' in profile.text
    assert 'id="settings-datetime"' in profile.text
    assert 'id="profile-hero"' not in profile.text
    assert 'id="profile-panel-account"' not in hub.text
    assert 'data-profile-tab="account"' not in hub.text
    assert 'id="profile-airing"' in hub.text
    for script in ("overview-profile", "dashboard-widgets", "view-cache", "playing-card", "activity"):
        assert f"/{script}.js" not in profile.text
        assert f"/{script}.js" in hub.text
    if not is_admin:
        assert client.get("/", follow_redirects=False).headers["location"] == "/hub"
        assert client.get("/?view=watchlist", follow_redirects=False).headers["location"] == "/hub#watchlist"
        assert client.get("/?view=playback_progress", follow_redirects=False).headers["location"] == "/hub#playback"


@pytest.mark.parametrize("path", ["/profile", "/hub"])
def test_both_user_pages_require_login(monkeypatch, path):
    from api import appAuthAPI

    monkeypatch.setattr(appAuthAPI, "auth_required", lambda cfg: False)
    app = FastAPI()
    ui_frontend.register_ui_root(app)
    response = TestClient(app).get(path, follow_redirects=False)
    assert response.status_code == 302
    assert response.headers["location"] == "/login"


@pytest.mark.parametrize("destination,expected", [
    ("/profile#account", "/profile#account"),
    ("/profile?linked=oidc#linked", "/profile?linked=oidc#linked"),
    ("/hub#collection", "/hub#collection"),
    ("/", "/hub"),
    ("/#settings", "/hub"),
])
def test_managed_oidc_returns_to_account_or_hub(destination, expected):
    assert authOidcAPI._login_destination(destination, {"is_admin": False}) == expected
    assert authOidcAPI._login_destination(destination, {"is_admin": True}) == destination


def test_managed_access_accepts_both_pages():
    import crosswatch

    user = {"is_admin": False, "permissions": {"write": False}}
    assert crosswatch._non_admin_permission_allowed(user, "/profile")
    assert crosswatch._non_admin_permission_allowed(user, "/hub")

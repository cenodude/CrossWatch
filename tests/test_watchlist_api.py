# tests/test_watchlist_api.py
# CrossWatch - Watchlist API regression checks

from __future__ import annotations

import json

from starlette.requests import Request


def _request(path: str, headers: dict[str, str] | None = None) -> Request:
    raw_headers = [(b"host", b"testserver")]
    for key, value in (headers or {}).items():
        raw_headers.append((str(key).lower().encode("latin-1"), str(value).encode("latin-1")))
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "GET",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("latin-1"),
            "query_string": b"",
            "headers": raw_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )


def _json_body(resp) -> dict:
    return json.loads(resp.body.decode("utf-8"))


def test_watchlist_api_filters_non_admin_to_assigned_profile(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import watchlistAPI
    import cw_platform.config_base as config_base
    import cw_platform.provider_instances as provider_instances

    profile_id = "11111111111141118111111111111111"
    cfg = {
        "tmdb": {"api_key": "tmdb-key"},
        "plex": {"instances": {"PLEX-P02": {}}},
        "mdblist": {"instances": {"MDBLIST-P01": {}}},
        "user_profiles": {
            profile_id: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"], "MDBLIST": ["MDBLIST-P01"]}},
        },
        "app_auth": {
            "enabled": True,
            "username": "admin",
            "reset_required": False,
            "remember_session_enabled": False,
            "remember_session_days": 30,
            "password": auth._password_hash("secrett1"),
            "session": {"token_hash": "", "expires_at": 0},
            "sessions": [],
            "last_login_at": 0,
            "users": {
                "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa": {
                    "username": "bob",
                    "enabled": True,
                    "role": "user",
                    "profile_id": profile_id,
                    "permissions": {"dashboard": True, "watchlist": True},
                    "password": auth._password_hash("secrett2"),
                }
            },
        },
    }
    provider_instances.ensure_provider_instance_uids(cfg)
    user = auth._public_user("aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa", cfg["app_auth"]["users"]["aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)

    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    monkeypatch.setattr(watchlistAPI, "_load_watchlist_state", lambda: {"last_sync_epoch": 123})
    monkeypatch.setattr(
        watchlistAPI,
        "build_watchlist",
        lambda _state, tmdb_ok: [
            {
                "key": "alice",
                "title": "Alice Item",
                "sources": ["plex"],
                "sources_by_provider": {"plex": ["default"]},
            },
            {
                "key": "bob",
                "title": "Bob Item",
                "sources": ["plex", "mdblist", "simkl", "scrob", "trakt"],
                "sources_by_provider": {
                    "mdblist": ["MDBLIST-P01"],
                    "plex": ["PLEX-P02"],
                    "scrob": ["default"],
                    "simkl": ["default"],
                    "trakt": ["default"],
                },
            },
        ],
    )

    request = _request("/api/watchlist", headers={"cookie": f"{auth.COOKIE_NAME}={token}"})
    data = _json_body(watchlistAPI.api_watchlist(request=request, limit=0, max_meta=0, user_profile=""))

    assert data["ok"] is True
    assert data["last_sync_epoch"] == 123
    assert [item["key"] for item in data["items"]] == ["bob"]
    assert data["items"][0]["sources"] == ["mdblist", "plex"]
    assert data["items"][0]["sources_by_provider"] == {"mdblist": ["MDBLIST-P01"], "plex": ["PLEX-P02"]}
    assert data["items"][0]["sync_count"] == 2

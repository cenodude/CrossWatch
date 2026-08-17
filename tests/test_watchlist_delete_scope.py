# tests/test_watchlist_delete_scope.py
# CrossWatch - Watchlist delete profile-scope checks

from __future__ import annotations

import json

from starlette.requests import Request

PROFILE_ID = "11111111111141118111111111111111"
USER_ID = "aaaaaaaaaaaa4aaa8aaaaaaaaaaaaaaa"


def _request(path: str, headers: dict[str, str] | None = None) -> Request:
    raw_headers = [(b"host", b"testserver")]
    for key, value in (headers or {}).items():
        raw_headers.append((str(key).lower().encode("latin-1"), str(value).encode("latin-1")))
    return Request(
        {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": "POST",
            "scheme": "http",
            "path": path,
            "raw_path": path.encode("latin-1"),
            "query_string": b"",
            "headers": raw_headers,
            "client": ("127.0.0.1", 12345),
            "server": ("testserver", 80),
        }
    )


def _cfg(auth):
    return {
        "plex": {"instances": {"PLEX-P01": {}, "PLEX-P02": {}}},
        "user_profiles": {
            PROFILE_ID: {"label": "Bob", "instances": {"PLEX": ["PLEX-P02"]}},
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
                USER_ID: {
                    "username": "bob",
                    "enabled": True,
                    "role": "user",
                    "profile_id": PROFILE_ID,
                    "permissions": {"dashboard": True, "watchlist": True, "write": True},
                    "password": auth._password_hash("secrett2"),
                }
            },
        },
    }


def _managed_request(monkeypatch):
    from api import appAuthAPI as auth
    import cw_platform.config_base as config_base
    import cw_platform.provider_instances as provider_instances

    cfg = _cfg(auth)
    provider_instances.ensure_provider_instance_uids(cfg)
    user = auth._public_user(USER_ID, cfg["app_auth"]["users"][USER_ID])
    token, _exp = auth._issue_session(cfg, _request("/api/app-auth/login"), user)
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    return _request("/api/watchlist/delete", headers={"cookie": f"{auth.COOKIE_NAME}={token}"}), cfg


def test_managed_user_cannot_delete_other_profile_instance(monkeypatch) -> None:
    from api import watchlistAPI

    request, _cfg_obj = _managed_request(monkeypatch)
    called: list = []
    monkeypatch.setattr(watchlistAPI, "_bulk_delete", lambda *a, **k: called.append((a, k)))

    resp = watchlistAPI.api_watchlist_delete_multi(
        request=request,
        payload={"provider": "PLEX", "provider_instance": "PLEX-P01", "keys": ["tmdb:603"]},
    )

    assert resp.status_code == 403
    assert json.loads(resp.body.decode("utf-8"))["error"] == "profile_scope_denied"
    assert called == []


def test_managed_user_can_delete_own_instance(monkeypatch) -> None:
    from api import watchlistAPI

    request, _cfg_obj = _managed_request(monkeypatch)
    seen: dict = {}

    def _fake(provider, keys, provider_instance=None, allowed_instances=None):
        seen.update(provider=provider, keys=keys, instance=provider_instance, allowed=allowed_instances)
        return {"ok": True}

    monkeypatch.setattr(watchlistAPI, "_bulk_delete", _fake)

    out = watchlistAPI.api_watchlist_delete_multi(
        request=request,
        payload={"provider": "PLEX", "provider_instance": "PLEX-P02", "keys": ["tmdb:603"]},
    )

    assert out == {"ok": True}
    assert seen["provider"] == "PLEX"
    assert seen["instance"] == "PLEX-P02"
    assert seen["allowed"] == {"PLEX": ["PLEX-P02"]}


def test_managed_user_all_provider_is_scoped_to_own_instances(monkeypatch) -> None:
    from api import watchlistAPI

    request, _cfg_obj = _managed_request(monkeypatch)
    seen: dict = {}

    def _fake(provider, keys, provider_instance=None, allowed_instances=None):
        seen.update(provider=provider, allowed=allowed_instances)
        return {"ok": True}

    monkeypatch.setattr(watchlistAPI, "_bulk_delete", _fake)

    watchlistAPI.api_watchlist_delete_multi(request=request, payload={"provider": "ALL", "keys": ["tmdb:603"]})

    assert seen["provider"] == "ALL"
    assert seen["allowed"] == {"PLEX": ["PLEX-P02"]}


def test_admin_delete_stays_unscoped(monkeypatch) -> None:
    from api import appAuthAPI as auth
    from api import watchlistAPI
    import cw_platform.config_base as config_base
    import cw_platform.provider_instances as provider_instances

    cfg = _cfg(auth)
    provider_instances.ensure_provider_instance_uids(cfg)
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    seen: dict = {}

    def _fake(provider, keys, provider_instance=None, allowed_instances=None):
        seen.update(allowed=allowed_instances)
        return {"ok": True}

    monkeypatch.setattr(watchlistAPI, "_bulk_delete", _fake)

    watchlistAPI.api_watchlist_delete_multi(
        request=_request("/api/watchlist/delete"),
        payload={"provider": "ALL", "keys": ["tmdb:603"]},
    )

    assert seen["allowed"] is None


def test_delete_watchlist_batch_never_loads_foreign_instance_credentials(monkeypatch) -> None:
    import services.watchlist as wl

    state = {
        "providers": {
            "PLEX": {
                "instances": {
                    "PLEX-P01": {"watchlist": {"baseline": {"items": {"tmdb:603": {"title": "A"}}}}},
                    "PLEX-P02": {"watchlist": {"baseline": {"items": {"tmdb:603": {"title": "A"}}}}},
                }
            }
        }
    }
    views: list = []
    monkeypatch.setattr(wl, "build_config_view", lambda cfg, sel: views.append(sel) or {})
    monkeypatch.setattr(wl, "_registry_sync_providers", lambda: ["PLEX", "TRAKT"])
    monkeypatch.setattr(wl, "_delete_on_plex_batch", lambda *a, **k: None)
    monkeypatch.setattr(wl, "_save_sync_state", lambda *a, **k: None)

    wl.delete_watchlist_batch(
        ["tmdb:603"],
        "ALL",
        state,
        {},
        allowed_instances={"PLEX": ["PLEX-P02"]},
    )

    assert views == [{"PLEX": "PLEX-P02"}]


def test_delete_watchlist_batch_routes_generic_watchlist_remove_providers(monkeypatch) -> None:
    import services.watchlist as wl

    state = {
        "providers": {
            "STREMIO": {"watchlist": {"baseline": {"items": {"imdb:tt123": {"type": "movie", "ids": {"imdb": "tt123"}}}}}},
            "NUVIO": {"watchlist": {"baseline": {"items": {"tmdb:603": {"type": "movie", "ids": {"tmdb": "603"}}}}}},
        }
    }
    called: list[tuple[str, list[str]]] = []

    def _fake_ops(provider, items, cfg):
        called.append((provider, [str(x.get("key")) for x in items]))

    monkeypatch.setattr(wl, "_registry_sync_providers", lambda: ["STREMIO", "NUVIO"])
    monkeypatch.setattr(wl, "_ops_supports_watchlist_remove", lambda p: p in {"STREMIO", "NUVIO"})
    monkeypatch.setattr(wl, "_delete_on_ops_watchlist_batch", _fake_ops)
    monkeypatch.setattr(wl, "_save_sync_state", lambda *a, **k: None)

    out = wl.delete_watchlist_batch(["imdb:tt123", "tmdb:603"], "ALL", state, {})

    assert out["ok"] is True
    assert ("STREMIO", ["imdb:tt123", "tmdb:603"]) in called
    assert ("NUVIO", ["imdb:tt123", "tmdb:603"]) in called
    assert out["details"]["STREMIO"]["removed"] == 1
    assert out["details"]["NUVIO"]["removed"] == 1


def test_watchlist_aliases_include_simkl_and_mdblist_native_ids(monkeypatch) -> None:
    import services.watchlist as wl

    monkeypatch.setattr(wl, "_registry_sync_providers", lambda: ["SIMKL", "MDBLIST"])
    monkeypatch.setattr(wl, "_load_hide_set", lambda: set())

    state = {
        "providers": {
            "SIMKL": {"watchlist": {"baseline": {"items": {"simkl:42": {"type": "show", "ids": {"simkl": "42"}, "title": "Native"}}}}},
            "MDBLIST": {"watchlist": {"baseline": {"items": {"mdblist:99": {"type": "movie", "ids": {"mdblist": "99"}, "title": "Native"}}}}},
        }
    }

    rows = wl.build_watchlist(state, tmdb_ok=False)
    by_key = {row["key"]: row for row in rows}

    assert "simkl:42" in by_key
    assert "simkl:42" in by_key["simkl:42"]["aliases"]
    assert "mdblist:99" in by_key
    assert "mdblist:99" in by_key["mdblist:99"]["aliases"]

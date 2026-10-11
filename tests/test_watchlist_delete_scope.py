# tests/test_watchlist_delete_scope.py
# CrossWatch - Watchlist delete profile-scope checks

from __future__ import annotations

import json

import pytest

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


def test_build_watchlist_merges_cross_provider_id_aliases(monkeypatch) -> None:
    import services.watchlist as wl

    monkeypatch.setattr(wl, "_registry_sync_providers", lambda: ["TRAKT", "STREMIO"])
    monkeypatch.setattr(wl, "_load_hide_set", lambda: set())

    state = {
        "providers": {
            "TRAKT": {
                "watchlist": {
                    "baseline": {
                        "items": {
                            "tmdb:1662317": {
                                "type": "movie",
                                "title": "The Crash",
                                "year": 2026,
                                "ids": {"tmdb": "1662317", "imdb": "tt40792117"},
                            }
                        }
                    }
                }
            },
            "STREMIO": {
                "watchlist": {
                    "baseline": {
                        "items": {
                            "imdb:tt40792117": {
                                "type": "movie",
                                "title": "The Crash",
                                "ids": {"imdb": "tt40792117"},
                            }
                        }
                    }
                }
            },
        }
    }

    rows = wl.build_watchlist(state, tmdb_ok=False)

    assert len(rows) == 1
    assert rows[0]["key"] == "tmdb:1662317"
    assert rows[0]["sources"] == ["stremio", "trakt"]
    assert rows[0]["ids"] == {"tmdb": "1662317", "imdb": "tt40792117"}


def test_jellyfin_and_emby_delete_headers_carry_the_token_in_authorization():
    from services import watchlist

    headers = watchlist._jf_headers({"access_token": "jf-token", "device_id": "dev-1"})
    assert headers["Authorization"].startswith("MediaBrowser ")
    assert 'DeviceId="dev-1"' in headers["Authorization"]
    assert headers["Authorization"].endswith('Token="jf-token"')
    assert headers["X-Emby-Token"] == "jf-token"
    assert "Token=" not in headers["X-Emby-Authorization"]
    assert "Authorization" not in watchlist._jf_headers({})


def _tracker(tmp_path, content=None):
    from types import SimpleNamespace

    if content is not None:
        text = content if isinstance(content, str) else json.dumps(content)
        (tmp_path / "watchlist.json").write_text(text, encoding="utf-8")
    return SimpleNamespace(cfg=SimpleNamespace(base_path=tmp_path), config={})


def _crosswatch_delete(tmp_path, monkeypatch, tracker) -> tuple[dict, dict]:
    import services.watchlist as wl

    _tracker(tmp_path, tracker)
    state = {
        "providers": {
            "CROSSWATCH": {"watchlist": {"baseline": {"items": {
                "tmdb:1": {"type": "movie", "ids": {"tmdb": "1", "imdb": "tt0000001", "plex": "123"}, "title": "Stale"},
            }}}},
        }
    }
    cfg = {"crosswatch": {"root_dir": str(tmp_path), "connected": True}}
    monkeypatch.setattr(wl, "build_config_view", lambda cfg, sel: cfg)
    monkeypatch.setattr(wl, "_save_sync_state", lambda *a, **k: None)
    out = wl.delete_watchlist_batch(["tmdb:1"], "CROSSWATCH", state, cfg)
    return out, state["providers"]["CROSSWATCH"]["watchlist"]["baseline"]["items"]


def _other(**extra):
    return {"type": "movie", "title": "Other", **extra}


def test_crosswatch_delete_clears_cached_row_already_gone_from_tracker(tmp_path, monkeypatch) -> None:
    tracker = {"ts": 1, "items": {"tmdb:2": _other(ids={"tmdb": "2"})}}
    out, cached = _crosswatch_delete(tmp_path, monkeypatch, tracker)

    assert out["ok"] is True
    assert out["deleted"] == 1
    assert not cached
    assert json.loads((tmp_path / "watchlist.json").read_text(encoding="utf-8")) == tracker


def test_crosswatch_delete_clears_cached_row_for_an_empty_tracker(tmp_path, monkeypatch) -> None:
    out, cached = _crosswatch_delete(tmp_path, monkeypatch, {"ts": 1, "items": {}})

    assert out["ok"] is True
    assert not cached


@pytest.mark.parametrize(
    "tracker",
    [
        None,
        "{broken",
        "7",
        {"ts": 1},
        {"ts": 1, "items": [_other(ids={"tmdb": "2"})]},
        {"ts": 1, "items": {"tmdb:2": "x"}},
        [_other(ids={"tmdb": "2"}), "x"],
        {"ts": 1, "items": {"tmdb:2": _other(ids={"tmdb": "2", "imdb": "tt0000001"})}},
        {"ts": 1, "items": {"tmdb:2": _other(ids={"tmdb": "2", "plex": "123"})}},
        {"ts": 1, "items": {"imdb:tt0000001": _other()}},
        {"ts": 1, "items": {"tmdb:1#show": {"type": "show", "title": "Same number"}}},
        [_other(tmdb="2", imdb="tt0000001")],
        [_other()],
        {"ts": 1, "items": {"title:other": _other()}},
    ],
    ids=[
        "missing_file", "invalid_json", "scalar", "no_items", "items_as_list", "non_object_entry",
        "non_object_in_list", "shared_secondary_id", "shared_server_id", "id_only_in_record_key",
        "same_id_other_type", "legacy_top_level_ids", "legacy_row_without_identity", "row_without_identity",
    ],
)
def test_crosswatch_delete_keeps_cached_row_when_absence_is_not_proven(tmp_path, monkeypatch, tracker) -> None:
    out, cached = _crosswatch_delete(tmp_path, monkeypatch, tracker)

    assert out["ok"] is False
    assert out["deleted"] == 0
    assert "tmdb:1" in cached


def test_crosswatch_delete_keeps_cached_row_when_tracker_read_is_denied(tmp_path, monkeypatch) -> None:
    from pathlib import Path

    def denied(self, *args, **kwargs):
        raise PermissionError("denied")

    _tracker(tmp_path, {"ts": 1, "items": {}})
    monkeypatch.setattr(Path, "read_text", denied)
    out, cached = _crosswatch_delete(tmp_path, monkeypatch, None)

    assert out["ok"] is False
    assert "tmdb:1" in cached

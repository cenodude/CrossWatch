# tests/test_auto_remove_profile_scope.py
# CrossWatch - Auto-remove-after-watch must stay inside the watching profile

from __future__ import annotations

ALICE = "11111111111141118111111111111111"
BOB = "22222222222242228222222222222222"


def _cfg():
    return {
        "plex": {"instances": {"PLEX-A": {}, "PLEX-B": {}}},
        "trakt": {"instances": {"TRAKT-A": {}, "TRAKT-B": {}}},
        "user_profiles": {
            ALICE: {"label": "Alice", "instances": {"PLEX": ["PLEX-A"], "TRAKT": ["TRAKT-A"]}},
            BOB: {"label": "Bob", "instances": {"PLEX": ["PLEX-B"], "TRAKT": ["TRAKT-B"]}},
        },
    }


def _state():
    def block(key):
        return {"watchlist": {"baseline": {"items": {key: {"title": "Movie", "type": "movie"}}}}}

    return {
        "providers": {
            "PLEX": {"instances": {"PLEX-A": block("tmdb:603"), "PLEX-B": block("tmdb:603")}},
            "TRAKT": {"instances": {"TRAKT-A": block("tmdb:603"), "TRAKT-B": block("tmdb:603")}},
        }
    }


def _prepare(monkeypatch):
    import cw_platform.provider_instances as provider_instances

    cfg = _cfg()
    provider_instances.ensure_provider_instance_uids(cfg)
    return cfg


def test_origin_owner_instances_resolves_the_watching_profile(monkeypatch) -> None:
    from cw_platform.access_policy import origin_owner_instances

    cfg = _prepare(monkeypatch)

    assert origin_owner_instances(cfg, "PLEX", "PLEX-A") == {"PLEX": ["PLEX-A"], "TRAKT": ["TRAKT-A"]}
    assert origin_owner_instances(cfg, "PLEX", "PLEX-B") == {"PLEX": ["PLEX-B"], "TRAKT": ["TRAKT-B"]}


def test_admin_only_instance_stays_unscoped(monkeypatch) -> None:
    from cw_platform.access_policy import origin_owner_instances

    cfg = _prepare(monkeypatch)
    cfg["plex"]["instances"]["PLEX-ADMIN"] = {}
    import cw_platform.provider_instances as provider_instances

    provider_instances.ensure_provider_instance_uids(cfg)

    assert origin_owner_instances(cfg, "PLEX", "PLEX-ADMIN") is None


def test_auto_remove_only_touches_the_watching_profile(monkeypatch) -> None:
    import api.watchlistAPI as WLAPI
    import cw_platform.config_base as config_base
    import services.watchlist as wl

    cfg = _prepare(monkeypatch)
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    monkeypatch.setattr(WLAPI, "_load_watchlist_state", lambda: _state())
    monkeypatch.setattr(WLAPI, "_active_pair_watchlist_providers", lambda _cfg: ["PLEX", "TRAKT"])
    monkeypatch.setattr(WLAPI, "_active_providers", lambda _cfg: ["PLEX", "TRAKT"])

    views: list = []
    monkeypatch.setattr(wl, "build_config_view", lambda c, sel: views.append(sel) or {})
    monkeypatch.setattr(wl, "_registry_sync_providers", lambda: ["PLEX", "TRAKT"])
    monkeypatch.setattr(wl, "_delete_on_plex_batch", lambda *a, **k: None)
    monkeypatch.setattr(wl, "_delete_on_trakt_batch", lambda *a, **k: None)
    monkeypatch.setattr(wl, "_save_sync_state", lambda *a, **k: None)

    WLAPI.remove_across_providers_by_ids({"tmdb": "603"}, "movie", origin="plex:PLEX-A")

    assert views, "expected at least one provider delete"
    assert {tuple(sorted(v.items()))[0] for v in views} == {("PLEX", "PLEX-A"), ("TRAKT", "TRAKT-A")}
    assert not any("PLEX-B" in v.values() or "TRAKT-B" in v.values() for v in views)


def test_auto_remove_without_origin_stays_unscoped(monkeypatch) -> None:
    import api.watchlistAPI as WLAPI
    import cw_platform.config_base as config_base
    import services.watchlist as wl

    cfg = _prepare(monkeypatch)
    monkeypatch.setattr(config_base, "load_config", lambda: cfg)
    monkeypatch.setattr(WLAPI, "_load_watchlist_state", lambda: _state())
    monkeypatch.setattr(WLAPI, "_active_pair_watchlist_providers", lambda _cfg: ["PLEX"])
    monkeypatch.setattr(WLAPI, "_active_providers", lambda _cfg: ["PLEX"])

    views: list = []
    monkeypatch.setattr(wl, "build_config_view", lambda c, sel: views.append(sel) or {})
    monkeypatch.setattr(wl, "_registry_sync_providers", lambda: ["PLEX"])
    monkeypatch.setattr(wl, "_delete_on_plex_batch", lambda *a, **k: None)
    monkeypatch.setattr(wl, "_save_sync_state", lambda *a, **k: None)

    WLAPI.remove_across_providers_by_ids({"tmdb": "603"}, "movie")

    instances = {v.get("PLEX") for v in views}
    assert "PLEX-A" in instances and "PLEX-B" in instances


def test_shim_forwards_scope_as_origin(monkeypatch) -> None:
    import api.watchlistAPI as WLAPI
    import providers.scrobble._auto_remove_watchlist as auto

    seen: dict = {}
    monkeypatch.setattr(auto, "_once_per_ttl", lambda _k: True)
    monkeypatch.setattr(
        WLAPI,
        "remove_across_providers_by_ids",
        lambda ids, media_type="", origin=None: seen.update(origin=origin) or {"ok": True},
    )

    auto.remove_across_providers_by_ids({"tmdb": "603"}, "movie", scope="trakt:TRAKT-A")

    assert seen["origin"] == "trakt:TRAKT-A"

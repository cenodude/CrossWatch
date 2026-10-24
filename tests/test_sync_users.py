# tests/test_sync_users.py
# CrossWatch - Per-user connections nested under a media server connection
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
from typing import Any

from cw_platform import provider_instances as pi


def _cfg() -> dict[str, Any]:
    return {
        "emby": {
            "server": "http://emby1:8096",
            "access_token": "admin-token",
            "user_id": "admin-id",
            "history": {"libraries": ["1"]},
            "sync_users": {"u1": {"user_id": "u1", "username": "user1"}},
            "instances": {
                "EMBY-P01": {
                    "server": "http://emby2:8096",
                    "access_token": "admin2-token",
                    "user_id": "admin2-id",
                    "sync_users": {"u2": {"user_id": "u2", "username": "user2"}},
                },
            },
        },
        "jellyfin": {
            "server": "http://jf:8096",
            "access_token": "jf-admin",
            "user_id": "jf-admin-id",
            "sync_users": {"j1": {"user_id": "j1", "username": "user1"}},
        },
        "plex": {
            "server_url": "https://plex:32400",
            "account_token": "owner",
            "pms_token": "owner-pms",
            "history": {"libraries": ["4"]},
            "sync_users": {"716": {"account_id": 716, "username": "friend"}},
        },
    }


def test_ids_round_trip() -> None:
    assert pi.sync_user_instance_id("EMBY-P01", "u2") == "EMBY-P01~u2"
    assert pi.split_sync_user_id("EMBY-P01~u2") == ("EMBY-P01", "u2")
    assert pi.split_sync_user_id("default~u1") == ("default", "u1")
    assert pi.split_sync_user_id("EMBY-P01") is None
    assert pi.split_sync_user_id("~u1") is None
    assert pi.split_sync_user_id("EMBY-P01~") is None
    assert pi.normalize_instance_id("EMBY-P01~u2") == "EMBY-P01~u2"


def test_sync_user_inherits_default_connection() -> None:
    block = pi.get_provider_block(_cfg(), "emby", "default~u1")
    assert block["server"] == "http://emby1:8096"
    assert block["access_token"] == "admin-token"
    assert block["user_id"] == "u1"
    assert block["history"] == {"libraries": ["1"]}
    assert "sync_users" not in block and "instances" not in block


def test_sync_user_inherits_named_connection() -> None:
    block = pi.get_provider_block(_cfg(), "emby", "EMBY-P01~u2")
    assert block["server"] == "http://emby2:8096"
    assert block["access_token"] == "admin2-token"
    assert block["user_id"] == "u2"
    assert "sync_users" not in block and "instances" not in block


def test_parent_changes_apply_to_users_live() -> None:
    cfg = _cfg()
    cfg["emby"]["server"] = "http://emby-new:8096"
    assert pi.get_provider_block(cfg, "emby", "default~u1")["server"] == "http://emby-new:8096"


def test_unknown_sync_user_resolves_to_nothing() -> None:
    cfg = _cfg()
    assert pi.get_provider_block(cfg, "emby", "default~nobody") == {}
    assert pi.get_provider_block(cfg, "emby", "EMBY-P09~u2") == {}
    assert pi._provider_ref_exists(cfg, "EMBY", "default~nobody") is False
    assert pi.instance_exists(cfg, "emby", "default~nobody") is False
    assert pi._provider_ref_exists(cfg, "EMBY", "EMBY-P01~u2") is True
    assert pi.instance_exists(cfg, "emby", "default~u1") is True


def test_connection_lists_stay_unchanged() -> None:
    cfg = _cfg()
    assert pi.list_instance_ids(cfg, "emby") == ["default", "EMBY-P01"]
    assert pi.list_instance_ids(cfg, "jellyfin") == ["default"]


def test_list_sync_user_ids() -> None:
    cfg = _cfg()
    assert pi.list_sync_user_ids(cfg, "emby") == ["default~u1", "EMBY-P01~u2"]
    assert pi.list_sync_user_ids(cfg, "emby", "EMBY-P01") == ["EMBY-P01~u2"]
    assert pi.list_sync_user_ids(cfg, "trakt") == []


def test_get_instance_block_never_creates_entries() -> None:
    cfg = _cfg()
    before = copy.deepcopy(cfg)
    assert pi.get_instance_block(cfg, "emby", "default~nobody", create=True) == {}
    assert pi.get_instance_block(cfg, "emby", "EMBY-P09~u2", create=True) == {}
    assert cfg == before
    entry = pi.get_instance_block(cfg, "emby", "default~u1")
    entry["username"] = "renamed"
    assert cfg["emby"]["sync_users"]["u1"]["username"] == "renamed"


def test_uid_registry_tracks_sync_users() -> None:
    cfg = _cfg()
    pi.ensure_provider_instance_uids(cfg)
    assert pi.provider_instance_uids_for_ref(cfg, "EMBY", "default~u1")
    assert pi.provider_instance_uids_for_ref(cfg, "EMBY", "EMBY-P01~u2")
    del cfg["emby"]["sync_users"]["u1"]
    pi.ensure_provider_instance_uids(cfg)
    assert pi.provider_instance_uids_for_ref(cfg, "EMBY", "default~u1") == []
    assert pi.provider_instance_uids_for_ref(cfg, "EMBY", "EMBY-P01~u2")


def test_emby_module_merge_resolves_sync_user_from_raw_config() -> None:
    from providers.sync._mod_EMBY import _merge_instance_block

    raw = _cfg()["emby"]
    assert _merge_instance_block(raw, "EMBY-P01~u2")["user_id"] == "u2"
    assert _merge_instance_block(raw, "EMBY-P01~u2")["server"] == "http://emby2:8096"
    assert _merge_instance_block(raw, "EMBY-P01")["user_id"] == "admin2-id"
    view = pi.get_provider_block(_cfg(), "emby", "default~u1")
    assert _merge_instance_block(view, "default~u1")["user_id"] == "u1"


def test_jellyfin_module_merge_resolves_sync_user_from_raw_config() -> None:
    from providers.sync._mod_JELLYFIN import _merge_instance_block

    raw = _cfg()["jellyfin"]
    merged = _merge_instance_block(raw, "default~j1")
    assert merged["user_id"] == "j1" and merged["access_token"] == "jf-admin"
    assert _merge_instance_block(raw, "default")["user_id"] == "jf-admin-id"


def test_plex_helper_returns_read_only_merged_view() -> None:
    from providers.sync.plex._utils import _plex

    cfg = _cfg()
    before = copy.deepcopy(cfg)
    view = _plex(cfg, "default~716")
    assert view["account_id"] == 716 and view["account_token"] == "owner"
    assert view["history"] == {"libraries": ["4"]}
    view["history"]["libraries"] = []
    view["server_url"] = "changed"
    assert cfg == before
    assert "default~716" not in (cfg["plex"].get("instances") or {})


def _pair_cfg(**pair_extra: Any) -> dict[str, Any]:
    cfg = _cfg()
    cfg["emby"]["sync_users"]["u3"] = {"user_id": "u3", "username": "Carmen"}
    cfg["jellyfin"]["sync_users"]["j3"] = {"user_id": "j3", "username": "carmen"}
    cfg["jellyfin"]["sync_users"]["j4"] = {"user_id": "j4", "username": "other"}
    cfg["pairs"] = [{"id": "p1", "source": "EMBY", "source_instance": "default", "target": "JELLYFIN", "target_instance": "default",
                     "mode": "two-way", "features": {"history": {"enable": True}}, **pair_extra}]
    return cfg


def _ctx(events: list[tuple[str, dict[str, Any]]]) -> Any:
    from types import SimpleNamespace

    return SimpleNamespace(pair_scope_ids=[], emit=lambda name, **kw: events.append((name, kw)))


def test_pair_without_match_users_is_untouched() -> None:
    from cw_platform.orchestrator._pairs import _pairs_for_run

    cfg = _pair_cfg()
    pairs = _pairs_for_run(_ctx([]), cfg)
    assert pairs == cfg["pairs"] and pairs[0] is cfg["pairs"][0]


def test_match_users_expands_per_matched_username() -> None:
    from cw_platform.orchestrator._pairs import _pairs_for_run

    cfg = _pair_cfg(match_users=True)
    pairs = _pairs_for_run(_ctx([]), cfg)
    assert [(p["source_instance"], p["target_instance"], p["_cw_sync_user"]) for p in pairs] == [
        ("default~u3", "default~j3", "Carmen"),
        ("default~u1", "default~j1", "user1"),
    ]
    assert all(p["id"] == "p1" for p in pairs)
    assert "_cw_sync_user" not in cfg["pairs"][0]


def test_manual_user_matches_override_and_exclude() -> None:
    from cw_platform.orchestrator._pairs import _pairs_for_run

    cfg = _pair_cfg(match_users=True, user_matches={"u1": "j4", "u3": ""})
    pairs = _pairs_for_run(_ctx([]), cfg)
    assert [(p["source_instance"], p["target_instance"]) for p in pairs] == [("default~u1", "default~j4")]


def test_no_matched_users_skips_pair_with_reason() -> None:
    from cw_platform.orchestrator._pairs import _pairs_for_run

    cfg = _pair_cfg(match_users=True)
    cfg["jellyfin"]["sync_users"] = {}
    events: list[tuple[str, dict[str, Any]]] = []
    assert _pairs_for_run(_ctx(events), cfg) == []
    assert events == [("run:pair:skip", {"pair_id": "p1", "src": "EMBY", "dst": "JELLYFIN", "reason": "no_matched_users"})]


def test_match_users_ignored_for_non_media_server_pairs() -> None:
    from cw_platform.orchestrator._pairs import _pairs_for_run

    cfg = _pair_cfg(match_users=True)
    cfg["pairs"][0]["target"] = "TRAKT"
    assert _pairs_for_run(_ctx([]), cfg) == cfg["pairs"]


def test_each_user_gets_its_own_state_scope_and_parent_scope_is_stable() -> None:
    from cw_platform.orchestrator._pairs import _pairs_for_run
    from cw_platform.pair_scope import pair_feature_scope

    cfg = _pair_cfg(match_users=True)
    scopes = {pair_feature_scope(cfg, p, "history") for p in _pairs_for_run(_ctx([]), cfg)}
    assert len(scopes) == 2
    parent = dict(cfg["pairs"][0], match_users=False)
    before = pair_feature_scope(cfg, parent, "history")
    cfg["emby"]["sync_users"]["u9"] = {"user_id": "u9", "username": "new"}
    cfg["emby"]["instances"]["EMBY-P01"]["sync_users"]["u8"] = {"user_id": "u8", "username": "x"}
    assert pair_feature_scope(cfg, parent, "history") == before
    plain = copy.deepcopy(cfg)
    for block in (plain["emby"], plain["emby"]["instances"]["EMBY-P01"], plain["jellyfin"]):
        block.pop("sync_users", None)
    assert pair_feature_scope(plain, parent, "history") == before


def test_health_uses_the_parent_connection() -> None:
    from cw_platform.orchestrator._pairs import _health_instance

    assert _health_instance("EMBY-P01~u2") == "EMBY-P01"
    assert _health_instance("default~u1") == "default"
    assert _health_instance("EMBY-P01") == "EMBY-P01"


def _api_store(monkeypatch, store: dict[str, Any]) -> Any:
    import json

    import api.providerInstancesAPI as provider_api

    def fake_load() -> dict[str, Any]:
        return json.loads(json.dumps(store))

    def fake_save(cfg: dict[str, Any]) -> None:
        store.clear()
        store.update(cfg)

    def fake_update(mutator):
        cfg = fake_load()
        result = mutator(cfg)
        fake_save(cfg)
        return cfg, result

    monkeypatch.setattr(provider_api, "load_config", fake_load)
    monkeypatch.setattr(provider_api, "save_config", fake_save)
    monkeypatch.setattr(provider_api, "update_config", fake_update)
    monkeypatch.setattr(provider_api, "_admin_required_response", lambda request, cfg: None)
    return provider_api


def _api_cfg() -> dict[str, Any]:
    return {
        "emby": {"server": "http://emby:8096", "access_token": "admin", "user_id": "admin-id", "instances": {"EMBY-P01": {"server": "http://emby2:8096"}}},
        "plex": {"server_url": "https://plex:32400", "account_token": "owner", "home_pin": "9999", "username": "owner"},
        "trakt": {"client_id": "c"},
    }


def test_sync_users_api_rejects_unsupported_targets(monkeypatch) -> None:
    api = _api_store(monkeypatch, _api_cfg())
    assert api.api_provider_sync_users("trakt", "default")["error"] == "unsupported_provider"
    assert api.api_provider_sync_users("emby", "EMBY-P09")["error"] == "not_found"
    assert api.api_provider_sync_users("emby", "default~u1")["error"] == "invalid_instance_id"


def test_sync_users_api_tick_and_untick(monkeypatch) -> None:
    store = _api_cfg()
    api = _api_store(monkeypatch, store)
    monkeypatch.setattr(api.time, "time", lambda: 1000)
    res = api.api_provider_sync_users_update("emby", "EMBY-P01", {"users": [{"key": "u1", "username": "User1"}, {"key": "u2", "username": "user2"}]})
    assert res["ok"] and [u["username"] for u in res["users"]] == ["User1", "user2"]
    assert store["emby"]["instances"]["EMBY-P01"]["sync_users"]["u1"] == {"user_id": "u1", "username": "User1", "user": "User1", "added_at": 1000}
    assert pi.provider_instance_uids_for_ref(store, "EMBY", "EMBY-P01~u1")
    assert pi.list_instance_ids(store, "emby") == ["default", "EMBY-P01"]

    monkeypatch.setattr(api.time, "time", lambda: 2000)
    api.api_provider_sync_users_update("emby", "EMBY-P01", {"users": [{"key": "u2", "username": "user2"}]})
    users = store["emby"]["instances"]["EMBY-P01"]["sync_users"]
    assert list(users) == ["u2"] and users["u2"]["added_at"] == 1000
    assert pi.provider_instance_uids_for_ref(store, "EMBY", "EMBY-P01~u1") == []

    api.api_provider_sync_users_update("emby", "EMBY-P01", {"users": [{"key": "u1", "username": "User1"}, {"key": "u2", "username": "user2"}]})
    assert store["emby"]["instances"]["EMBY-P01"]["sync_users"]["u1"]["added_at"] == 2000

    api.api_provider_sync_users_update("emby", "EMBY-P01", {"users": []})
    assert "sync_users" not in store["emby"]["instances"]["EMBY-P01"]


def test_retick_gets_a_fresh_state_scope(monkeypatch) -> None:
    from cw_platform.pair_scope import pair_feature_scope

    store = _api_cfg()
    store["jellyfin"] = {"server": "http://jf", "access_token": "jf", "sync_users": {"j1": {"user_id": "j1", "username": "user1", "added_at": 1}}}
    api = _api_store(monkeypatch, store)
    pair = {"id": "p1", "source": "EMBY", "source_instance": "default~u1", "target": "JELLYFIN", "target_instance": "default~j1"}
    monkeypatch.setattr(api.time, "time", lambda: 1000)
    api.api_provider_sync_users_update("emby", "default", {"users": [{"key": "u1", "username": "user1"}]})
    first = pair_feature_scope(store, pair, "history")
    api.api_provider_sync_users_update("emby", "default", {"users": []})
    monkeypatch.setattr(api.time, "time", lambda: 5000)
    api.api_provider_sync_users_update("emby", "default", {"users": [{"key": "u1", "username": "user1"}]})
    assert pair_feature_scope(store, pair, "history") != first


def test_plex_sync_users_never_inherit_the_owner_pin(monkeypatch) -> None:
    store = _api_cfg()
    api = _api_store(monkeypatch, store)
    api.api_provider_sync_users_update("plex", "default", {"users": [{"key": "877", "username": "test"}, {"key": "716", "username": "friend"}]})
    assert pi.get_provider_block(store, "plex", "default~877")["home_pin"] == ""
    assert pi.get_provider_block(store, "plex", "default~877")["account_id"] == 877
    api.api_provider_sync_users_update("plex", "default", {"users": [{"key": "877", "username": "test", "home_pin": "1111"}, {"key": "716", "username": "friend"}]})
    api.api_provider_sync_users_update("plex", "default", {"users": [{"key": "877", "username": "test"}, {"key": "716", "username": "friend"}]})
    assert pi.get_provider_block(store, "plex", "default~877")["home_pin"] == "1111"
    rows = api.api_provider_sync_users("plex", "default")["users"]
    assert {r["key"]: r["has_pin"] for r in rows} == {"877": True, "716": False}


def test_sync_users_limit(monkeypatch) -> None:
    store = _api_cfg()
    store["runtime"] = {"max_sync_users_per_connection": 2}
    api = _api_store(monkeypatch, store)
    res = api.api_provider_sync_users_update("emby", "default", {"users": [{"key": f"u{i}", "username": f"u{i}"} for i in range(3)]})
    assert res == {"ok": False, "error": "sync_user_limit_reached", "limit": 2}
    assert "sync_users" not in store["emby"]


def test_unticked_user_leaves_user_profiles(monkeypatch) -> None:
    store = _api_cfg()
    api = _api_store(monkeypatch, store)
    api.api_provider_sync_users_update("emby", "default", {"users": [{"key": "u1", "username": "user1"}]})
    pi.upsert_user_profile(store, "user1", label="User1", instances={"EMBY": ["default~u1"]})
    assert pi.instances_for_user_profile(store, "user1") == {"EMBY": ["default~u1"]}
    api.api_provider_sync_users_update("emby", "default", {"users": []})
    assert pi.instances_for_user_profile(store, "user1") == {}


def test_pair_user_matching_fields_stay_out_of_normal_pairs() -> None:
    from api.syncAPI import _apply_pair_user_matching

    pair: dict[str, Any] = {"id": "p1"}
    _apply_pair_user_matching(pair, {})
    assert pair == {"id": "p1"}
    _apply_pair_user_matching(pair, {"match_users": True, "user_matches": {"u1": "j4", " ": "x"}})
    assert pair == {"id": "p1", "match_users": True, "user_matches": {"u1": "j4"}}
    _apply_pair_user_matching(pair, {"match_users": False, "user_matches": {}})
    assert pair == {"id": "p1"}


def _scrobble_cfg(**route_extra: Any) -> dict[str, Any]:
    cfg = _pair_cfg()
    cfg["scrobble"] = {"watch": {"routes": [{"id": "R1", "enabled": True, "provider": "emby", "provider_instance": "default",
                                             "sink": "jellyfin", "sink_instance": "default", "filters": {}, **route_extra}]}}
    return cfg


def test_route_without_match_users_is_unchanged() -> None:
    from providers.scrobble.routes import expand_sync_user_route, normalize_routes

    cfg = _scrobble_cfg()
    route = normalize_routes(cfg)[0]
    assert "match_users" not in route
    assert expand_sync_user_route(cfg, route) == [route]


def test_route_expands_per_matched_user_with_identity_filter() -> None:
    from providers.scrobble.routes import expand_sync_user_route, normalize_routes

    cfg = _scrobble_cfg(match_users=True)
    route = normalize_routes(cfg)[0]
    derived = expand_sync_user_route(cfg, route)
    assert [(d["id"], d["sink_instance"], d["filters"]["username_whitelist"]) for d in derived] == [
        ("R1~u1", "default~j1", ["id:u1"]),
        ("R1~u3", "default~j3", ["id:u3"]),
    ]
    assert all(d["provider_instance"] == "default" and d["_cw_parent_route_id"] == "R1" for d in derived)


def test_route_existing_whitelist_limits_users() -> None:
    from providers.scrobble.routes import expand_sync_user_route, normalize_routes

    cfg = _scrobble_cfg(match_users=True, filters={"username_whitelist": ["carmen"]})
    derived = expand_sync_user_route(cfg, normalize_routes(cfg)[0])
    assert [d["id"] for d in derived] == ["R1~u3"]


def test_find_route_resolves_per_user_ids() -> None:
    from providers.scrobble.routes import build_route_cfg_by_id, find_route

    cfg = _scrobble_cfg(match_users=True)
    route = find_route(cfg, "R1~u3")
    assert route is not None and route["sink_instance"] == "default~j3"
    assert find_route(cfg, "R1~nobody") is None
    built = build_route_cfg_by_id(cfg, "R1~u3")
    assert built is not None
    assert built["jellyfin"]["user_id"] == "j3"
    assert built["scrobble"]["watch"]["filters"]["username_whitelist"] == ["id:u3"]


def test_real_route_id_with_tilde_wins() -> None:
    from providers.scrobble.routes import find_route

    cfg = _scrobble_cfg(match_users=True)
    cfg["scrobble"]["watch"]["routes"].append({"id": "R1~u3", "enabled": True, "provider": "emby", "provider_instance": "default",
                                               "sink": "trakt", "sink_instance": "default"})
    route = find_route(cfg, "R1~u3")
    assert route is not None and route["sink"] == "trakt"


def test_route_to_a_tracker_ignores_match_users() -> None:
    from providers.scrobble.routes import expand_sync_user_route, normalize_routes

    cfg = _scrobble_cfg(match_users=True)
    cfg["scrobble"]["watch"]["routes"][0]["sink"] = "trakt"
    route = normalize_routes(cfg)[0]
    assert expand_sync_user_route(cfg, route) == [route]


def test_route_validation_drops_match_users_for_trackers() -> None:
    from api.scrobblerManagementAPI import _validate_route

    cfg = _scrobble_cfg()
    cfg["trakt"] = {"access_token": "t"}
    media = _validate_route(cfg, {"id": "R9", "provider": "emby", "provider_instance": "default", "sink": "jellyfin", "sink_instance": "default", "match_users": True})
    assert media["match_users"] is True
    tracker = _validate_route(cfg, {"id": "R9", "provider": "emby", "provider_instance": "default", "sink": "trakt", "sink_instance": "default", "match_users": True})
    assert "match_users" not in tracker


def _webhook_dispatch(monkeypatch, cfg: dict[str, Any], account: str) -> tuple[Any, list[str]]:
    import cw_platform.event_archive.playback_recorder as recorder
    import providers.webhooks.dispatch as dispatch

    sent: list[str] = []

    class FakeSink:
        def __init__(self, inst: str) -> None:
            self.inst = inst

        def send(self, event: Any, cfg: Any = None) -> dict[str, Any]:
            sent.append(self.inst)
            return {"ok": True}

    monkeypatch.setattr(dispatch, "_make_sink", lambda sink, inst, provider: FakeSink(inst))
    monkeypatch.setattr(dispatch, "sink_configured", lambda cfg, sink, inst: True)
    monkeypatch.setattr(dispatch, "maybe_enrich_event_for_sink", lambda event, sink, route_cfg: event)
    monkeypatch.setattr(recorder, "prepare_playback", lambda ev: ev)
    monkeypatch.setattr(recorder, "record_playback", lambda *a, **k: None)
    res = dispatch.dispatch_scrobble("emby", "stop", media_type="movie", ids={"tmdb": "1"}, title="Movie", progress=95,
                                     account=account, cfg=cfg, provider_instance="default")
    return res, sent


def _webhook_cfg(match_users: bool) -> dict[str, Any]:
    cfg = _pair_cfg()
    cfg["scrobble"] = {"webhook": {"profiles": {"emby": {"default": {"enabled": True, "sinks": ["jellyfin"],
                                                                     "sink_instances": {"jellyfin": "default"},
                                                                     **({"match_users": True} if match_users else {})}}}}}
    return cfg


def test_webhook_sends_each_play_to_the_matched_user(monkeypatch) -> None:
    res, sent = _webhook_dispatch(monkeypatch, _webhook_cfg(True), "carmen")
    assert sent == ["default~j3"]
    assert res.json()["targets"][0]["target_instance"] == "default~j3"


def test_webhook_skips_unmatched_user(monkeypatch) -> None:
    res, sent = _webhook_dispatch(monkeypatch, _webhook_cfg(True), "stranger")
    assert sent == []
    assert res.json()["targets"] == [{"target": "jellyfin", "target_instance": "default", "ok": True, "skipped": True, "reason": "no_matched_user"}]


def test_webhook_without_match_users_is_unchanged(monkeypatch) -> None:
    _res, sent = _webhook_dispatch(monkeypatch, _webhook_cfg(False), "carmen")
    assert sent == ["default"]


def _seed_state(tmp_path) -> Any:
    from cw_platform.local_db import state as sqlite_state

    def block(key: str) -> dict[str, Any]:
        return {"baseline": {"items": {key: {"type": "movie", "title": key, "ids": {"tmdb": key.split(":")[1]}}}}, "checkpoint": None}

    sqlite_state.save_pair_blocks(tmp_path, "scope-admin", {("EMBY", "default", "history"): block("tmdb:1")})
    sqlite_state.save_pair_blocks(tmp_path, "scope-user", {("EMBY", "default~u1", "history"): block("tmdb:2")})
    return sqlite_state


def test_global_state_hides_sync_users_by_default(tmp_path) -> None:
    sqlite_state = _seed_state(tmp_path)
    plain = sqlite_state.load_state_features(tmp_path, {"history"})
    assert "instances" not in plain["providers"]["EMBY"]
    full = sqlite_state.load_state_features(tmp_path, {"history"}, include_sync_users=True)
    assert list(full["providers"]["EMBY"]["instances"]) == ["default~u1"]
    assert "instances" not in sqlite_state.load_state(tmp_path)["providers"]["EMBY"]
    assert "default~u1" in sqlite_state.load_state(tmp_path, include_sync_users=True)["providers"]["EMBY"]["instances"]
    assert [row["instance"] for row in sqlite_state.feature_inventory(tmp_path)] == ["default"]
    assert sqlite_state.provider_feature_counts(tmp_path, "history") == {"EMBY": 1}


def test_pair_state_still_holds_the_user_baseline(tmp_path) -> None:
    sqlite_state = _seed_state(tmp_path)
    pair = sqlite_state.load_pair_state(tmp_path, "scope-user", {"history"})
    assert list(pair["providers"]["EMBY"]["instances"]["default~u1"]["history"]["baseline"]["items"]) == ["tmdb:2"]


def test_profile_loaders_only_opt_in_for_profiles_with_sync_users() -> None:
    from api import profileAPI

    calls: list[tuple[Any, ...]] = []

    def loader(*args: Any) -> dict[str, Any]:
        calls.append(args)
        return {}

    profileAPI._scoped_loader(loader, False)()
    profileAPI._scoped_loader(loader, True)()
    assert calls == [(), (True,)]
    assert profileAPI._filter_has_sync_users({"EMBY": ["default~u1"]}) is True
    assert profileAPI._filter_has_sync_users({"EMBY": ["default", "EMBY-P01"]}) is False
    assert pi.instances_include_sync_users({"PLEX": "default~716"}) is True


def test_provider_label_names_sync_users(monkeypatch) -> None:
    import cw_platform.config_base as config_base
    from cw_platform.provider_usage import provider_label

    monkeypatch.setattr(config_base, "load_config", lambda: _cfg())
    assert provider_label("emby", "default~u1") == "Emby · user1"
    assert provider_label("emby", "EMBY-P01~u2") == "Emby EMBY-P01 · user2"
    assert provider_label("emby", "default~gone") == "Emby · gone"
    assert provider_label("emby", "EMBY-P01") == "Emby EMBY-P01"
    assert provider_label("emby", "default") == "Emby"


def test_sync_user_labels_endpoint(monkeypatch) -> None:
    import json

    api = _api_store(monkeypatch, _cfg())
    monkeypatch.setattr(api, "_managed_user_instance_scope", lambda cfg, request: None)
    body = json.loads(api.api_sync_user_labels().body)
    assert body == {"EMBY": {"default~u1": "user1", "EMBY-P01~u2": "user2"}, "JELLYFIN": {"default~j1": "user1"}, "PLEX": {"default~716": "friend"}}
    monkeypatch.setattr(api, "_managed_user_instance_scope", lambda cfg, request: {"EMBY": {"default~u1"}})
    assert json.loads(api.api_sync_user_labels().body) == {"EMBY": {"default~u1": "user1"}}


def test_ticked_users_join_the_user_profile_with_the_same_name(monkeypatch) -> None:
    store = _api_cfg()
    pi.upsert_user_profile(store, "carmen", label="Carmen", instances={})
    api = _api_store(monkeypatch, store)
    api.api_provider_sync_users_update("emby", "default", {"users": [{"key": "u3", "username": "carmen"}, {"key": "u4", "username": "nobody"}]})
    assert pi.instances_for_user_profile(store, "carmen") == {"EMBY": ["default~u3"]}
    pi.upsert_user_profile(store, "other", label="Other", instances={})
    pi.remove_instance_from_user_profiles(store, "EMBY", "default~u3")
    pi.assign_instance_to_user_profile_id(store, "other", "EMBY", "default~u3")
    api.api_provider_sync_users_update("emby", "default", {"users": [{"key": "u3", "username": "carmen"}]})
    assert pi.instances_for_user_profile(store, "carmen") == {}


def test_analyzer_sees_per_user_pairs(monkeypatch) -> None:
    import services.analyzer as analyzer

    cfg = _pair_cfg(match_users=True)
    monkeypatch.setattr(analyzer, "load_config", lambda: cfg)
    pairs = analyzer._cfg()["pairs"]
    assert [(p["id"], p["source_instance"], p["target_instance"]) for p in pairs] == [
        ("p1", "default~u3", "default~j3"),
        ("p1", "default~u1", "default~j1"),
    ]
    plain = _pair_cfg()
    monkeypatch.setattr(analyzer, "load_config", lambda: plain)
    assert analyzer._cfg() is plain


def test_unknown_sync_user_never_falls_back_to_the_admin() -> None:
    from providers.sync._mod_EMBY import _merge_instance_block as emby_merge
    from providers.sync._mod_JELLYFIN import _merge_instance_block as jf_merge

    assert emby_merge(_cfg()["emby"], "default~removed") == {}
    assert emby_merge({"server": "s", "user_id": "admin-id"}, "default~removed") == {}
    assert jf_merge(_cfg()["jellyfin"], "default~removed") == {}
    view = pi.get_provider_block(_cfg(), "emby", "default~u1")
    assert emby_merge(view, "default~u1")["user_id"] == "u1"
    assert emby_merge(view, "default~u9") == {}


def test_event_matching_prefers_ids_and_exact_names() -> None:
    from providers.scrobble.routes import sync_user_match_for_account

    matches = [
        {"key": "u2", "username": "ab", "sink_instance": "default~j2", "ids": ["u2"]},
        {"key": "u1", "username": "a.b", "sink_instance": "default~j1", "ids": ["u1"]},
    ]
    assert (sync_user_match_for_account(matches, ["ab"], user_id="u1") or {}).get("key") == "u1"
    assert sync_user_match_for_account(matches, ["a.b"], user_id="u9") is None
    assert (sync_user_match_for_account(matches, ["A.B"]) or {}).get("key") == "u1"
    assert sync_user_match_for_account(matches, ["a-b"]) is None
    twins = [dict(matches[0]), {**matches[0], "key": "u3", "ids": ["u3"]}]
    assert sync_user_match_for_account(twins, ["ab"]) is None


def test_plex_users_keep_their_plex_tv_id_for_matching(monkeypatch) -> None:
    from providers.scrobble.routes import sync_user_matches

    store = _api_cfg()
    store["emby"]["sync_users"] = {"e1": {"user_id": "e1", "username": "friend"}}
    api = _api_store(monkeypatch, store)
    api.api_provider_sync_users_update("plex", "default", {"users": [{"key": "12", "username": "friend", "cloud_account_id": "716210920"}]})
    assert store["plex"]["sync_users"]["12"]["cloud_account_id"] == "716210920"
    match = sync_user_matches(store, "plex", "default", "emby", "default")[0]
    assert match["ids"] == ["12", "716210920"]

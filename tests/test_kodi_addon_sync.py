# tests/test_kodi_addon_sync.py
# CrossWatch - Kodi add-on library sync transport, queue and endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import gzip
import json
import threading
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from providers.auth._auth_KODI import KodiAuthError
from providers.sync import _mod_KODI as mod
from providers.sync.kodi import _addon_sync as addon_sync
from providers.sync.kodi import _common as common

TOKEN = "kodi-sync-test-token-0001"
_SEQ_BY_UPLOAD: dict[str, int] = {}
_SEQ_COUNTER = [0]


def next_seq() -> int:
    _SEQ_COUNTER[0] += 1
    return _SEQ_COUNTER[0]


@pytest.fixture(autouse=True)
def _sync_root(config_base, monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(addon_sync, "_root", lambda: config_base / ".cw_state" / "kodi_addon_sync")
    monkeypatch.setattr(addon_sync, "_clock_floor", lambda: 0)
    addon_sync._PENDING_RUNS.clear()
    addon_sync._CACHE.clear()
    addon_sync._NEXT_CHANGE.clear()
    _SEQ_BY_UPLOAD.clear()
    _SEQ_COUNTER[0] = 0
    yield config_base
    for timer in list(addon_sync._DELAYED.values()):
        timer.cancel()
    addon_sync._DELAYED.clear()


def make_cfg(**kodi: Any) -> dict[str, Any]:
    return {
        "security": {"webhook_ids": {"kodiwatcher:default": TOKEN}},
        "kodi": dict(kodi),
        "pairs": [],
    }


def ping(cfg: dict[str, Any], viewers: list[dict[str, str]] | None = None, enabled: bool = True, library_id: str = "lib-1",
         device: str = "dev-1") -> dict[str, Any]:
    block = {"version": 1, "enabled": enabled, "viewers": viewers if viewers is not None else [{"viewer_id": "kv-1", "name": "user1"}],
             "library_id": library_id, "kodi_version": "21.2", "jsonrpc_version": "13.5.0"}
    return addon_sync.handle_ping(cfg, "default", {"event": "ping", "device": {"id": device}, "library_sync": block}) or {}


def movie(**extra: Any) -> dict[str, Any]:
    row = {"movieid": 12, "uniqueid": {"tmdb": "550"}, "title": "Fight Club", "year": 1999, "file": "nfs://nas/movies/fc.mkv",
           "playcount": 1, "lastplayed": "2026-01-02 03:04:05", "userrating": 8, "resume": {"position": 0, "total": 8340}}
    row.update(extra)
    return row


def unwatched(**extra: Any) -> dict[str, Any]:
    return movie(**{"playcount": 0, "lastplayed": "", "userrating": 0, **extra})


def episode(**extra: Any) -> dict[str, Any]:
    row = {"episodeid": 40, "tvshowid": 3, "uniqueid": {"tvdb": "3254641"}, "title": "Home", "showtitle": "The Expanse",
           "season": 2, "episode": 5, "file": "nfs://nas/tv/expanse/s02e05.mkv", "playcount": 0, "lastplayed": "",
           "userrating": 0, "resume": {"position": 0, "total": 0}}
    row.update(extra)
    return row


def tvshow() -> dict[str, Any]:
    return {"tvshowid": 3, "uniqueid": {"tmdb": "63639"}, "title": "The Expanse", "year": 2015}


def full_body(movies=None, episodes=None, tvshows=None, **extra: Any) -> dict[str, Any]:
    body = {"version": 1, "viewer_id": "kv-1", "mode": "full", "upload_id": "u-1", "part": 1, "parts": 1, "applied_seq": 0,
            "timezone": "UTC", "sources": [{"file": "nfs://nas/movies/", "label": "Movies"}],
            "movies": [movie()] if movies is None else movies,
            "episodes": [episode()] if episodes is None else episodes,
            "tvshows": [tvshow()] if tvshows is None else tvshows}
    body.update(extra)
    if "upload_seq" not in body:
        body["upload_seq"] = _SEQ_BY_UPLOAD.setdefault(f"{body['viewer_id']}|{body['upload_id']}", next_seq())
    return body


def full(cfg: dict[str, Any], movies=None, episodes=None, tvshows=None, **extra: Any) -> dict[str, Any]:
    return upload(cfg, full_body(movies, episodes, tvshows, **extra))


def delta(cfg: dict[str, Any], **extra: Any) -> dict[str, Any]:
    body = {"version": 1, "viewer_id": "kv-1", "mode": "delta", **extra}
    body.setdefault("upload_seq", next_seq())
    return upload(cfg, body)


def upload(cfg: dict[str, Any], body: dict[str, Any], device: str = "dev-1") -> dict[str, Any]:
    return addon_sync.receive_upload(cfg, "default", body, device_id=device)


def queue(cfg: dict[str, Any], viewer: str = "kv-1", **kwargs: Any) -> dict[str, Any]:
    return addon_sync.fetch_queue(cfg, "default", viewer, device_id=kwargs.pop("device", "dev-1"), **kwargs)


def ack(cfg: dict[str, Any], *results: tuple[int, str]) -> dict[str, Any]:
    ids = {r.get("seq"): r.get("change_id") for r in addon_sync._load_changes("default", "kv-1")["records"].values()}
    return addon_sync.receive_ack(cfg, "default", {"viewer_id": "kv-1", "results": [{"seq": s, "status": st, "change_id": ids.get(s) or ""}
                                                                                    for s, st in results]},
                                  device_id="dev-1")


def module(cfg: dict[str, Any]) -> mod.KODIModule:
    return mod.KODIModule({**cfg, "_cw_provider_instance": "default"})


def titles(cfg: dict[str, Any], feature: str) -> set[str]:
    return {row["title"] for row in module(cfg).build_index(feature).values()}


def watch_fight_club(cfg: dict[str, Any]) -> dict[str, Any]:
    return module(cfg).add("history", [{"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-01-03T10:00:00Z"}])


def unwatch_fight_club(cfg: dict[str, Any]) -> dict[str, Any]:
    return module(cfg).remove("history", [{"type": "movie", "ids": {"tmdb": "550"}}])


def rate_fight_club(cfg: dict[str, Any], rating: int) -> dict[str, Any]:
    return module(cfg).add("ratings", [{"type": "movie", "ids": {"tmdb": "550"}, "rating": rating}])


def ratings(cfg: dict[str, Any]) -> dict[str, int]:
    return {row["title"]: row["rating"] for row in module(cfg).build_index("ratings").values()}


def test_ping_without_sync_block_stays_off_and_writes_nothing():
    cfg = make_cfg()
    off = {"version": 1, "enabled": False, "viewers": [], "queue_pending": 0}
    assert addon_sync.handle_ping(cfg, "default", {"event": "ping"}) == off
    assert addon_sync.handle_ping({**cfg, "runtime": {"kodi_addon_sync": False}}, "default", {"event": "ping"}) == off
    assert not addon_sync._path("default", "device").exists()


def test_ping_maps_single_viewer_to_device():
    cfg = make_cfg()
    block = ping(cfg)
    assert block == {"version": 1, "enabled": True, "queue_pending": 0,
                     "viewers": [{"viewer_id": "kv-1", "instance": "default", "upload_seq": 0, "next_change": 1, "run_pending": False}]}
    assert addon_sync.device_info("default")["library_id"] == "lib-1"
    full(cfg)
    assert ping(cfg)["viewers"][0]["upload_seq"] == 1


def test_several_viewers_need_sync_users():
    cfg = make_cfg()
    viewers = [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}]
    assert ping(cfg, viewers)["viewers"] == []
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg)
    assert err.value.code == "viewer_unknown"
    cfg = make_cfg(sync_users={"kv-1": {"username": "user1"}, "kv-2": {"username": "user2"}})
    assert [(v["viewer_id"], v["instance"], v["upload_seq"]) for v in ping(cfg, viewers)["viewers"]] == [
        ("kv-1", "default~kv-1", 0), ("kv-2", "default~kv-2", 0)]
    assert full(cfg)["instance"] == "default~kv-1"


def test_sync_user_pairs_find_their_own_upload():
    from cw_platform.provider_instances import build_pair_config_view, build_provider_config_view

    cfg = make_cfg(sync_users={"kv-1": {"username": "user1"}, "kv-2": {"username": "user2"}})
    ping(cfg, [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}])
    full(cfg)
    full(cfg, viewer_id="kv-2", movies=[unwatched()], episodes=[], tvshows=[])
    parent = build_provider_config_view(cfg, "KODI", "default")
    assert mod.KODIModule({**parent, "_cw_provider_instance": "default"}).health()["ok"] is True
    for inst, expected in (("default~kv-1", {"Fight Club", "Home"}), ("default~kv-2", {"Fight Club"})):
        view = {**build_pair_config_view(cfg, "TRAKT", "default", "KODI", inst), "_cw_provider_instance": inst}
        adapter = mod.KODIModule(view)
        assert adapter.health()["ok"] is True
        assert {row["title"] for row in adapter.build_index("history").values()} | {
            row["title"] for row in adapter.build_index("collection").values()} >= expected
    view = {**build_pair_config_view(cfg, "TRAKT", "default", "KODI", "default~kv-2"), "_cw_provider_instance": "default~kv-2"}
    assert mod.KODIModule(view).add("ratings", [{"type": "movie", "ids": {"tmdb": "550"}, "rating": 9}])["count"] == 1
    assert [c["params"]["userrating"] for c in queue(cfg, "kv-2")["changes"]] == [9]
    assert queue(cfg)["changes"] == []
    assert addon_sync.viewer_instance(view, "default", "kv-2") == "default~kv-2"
    assert addon_sync.viewer_instance({**view, "_cw_provider_instance": "living~kv-2"}, "default", "kv-2") is None


def test_sync_users_of_a_named_instance_pass_parent_health():
    from cw_platform.provider_instances import build_pair_config_view, build_provider_config_view

    cfg = {"security": {"webhook_ids": {"kodiwatcher:living": TOKEN}}, "pairs": [],
           "kodi": {"instances": {"living": {"sync_users": {"kv-1": {"username": "user1"}, "kv-2": {"username": "user2"}}}}}}
    viewers = [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}]
    addon_sync.handle_ping(cfg, "living", {"event": "ping", "device": {"id": "dev-1"},
                                           "library_sync": {"version": 1, "enabled": True, "viewers": viewers, "library_id": "lib-1"}})
    assert addon_sync.receive_upload(cfg, "living", full_body(), device_id="dev-1")["instance"] == "living~kv-1"
    parent = {**build_provider_config_view(cfg, "KODI", "living"), "_cw_provider_instance": "living"}
    assert mod.KODIModule(parent).health()["ok"] is True
    view = {**build_pair_config_view(cfg, "TRAKT", "default", "KODI", "living~kv-1"), "_cw_provider_instance": "living~kv-1"}
    assert {row["title"] for row in mod.KODIModule(view).build_index("history").values()} == {"Fight Club"}
    other = {**build_provider_config_view(cfg, "KODI", "living"), "_cw_provider_instance": "default"}
    assert addon_sync._parent_sync_users(other, "living") == []


def test_sync_user_waits_while_only_its_sibling_uploaded():
    from cw_platform.provider_instances import build_pair_config_view

    cfg = make_cfg(sync_users={"kv-1": {"username": "user1"}, "kv-2": {"username": "user2"}})
    ping(cfg, [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}])
    full(cfg)
    view = {**build_pair_config_view(cfg, "TRAKT", "default", "KODI", "default~kv-2"), "_cw_provider_instance": "default~kv-2"}
    assert not common.is_configured(view, "default~kv-2")
    assert common.is_configured(cfg, "default")


def test_sync_users_made_by_viewer_name_still_map():
    from cw_platform.provider_instances import build_pair_config_view

    cfg = make_cfg(sync_users={"user1": {"username": "User1"}, "user2": {"username": "user2"}})
    viewers = [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}]
    assert [(v["viewer_id"], v["instance"]) for v in ping(cfg, viewers)["viewers"]] == [
        ("kv-1", "default~user1"), ("kv-2", "default~user2")]
    assert full(cfg)["instance"] == "default~user1"
    view = {**build_pair_config_view(cfg, "TRAKT", "default", "KODI", "default~user1"), "_cw_provider_instance": "default~user1"}
    assert {row["title"] for row in mod.KODIModule(view).build_index("history").values()} == {"Fight Club"}


def test_name_mapping_never_guesses():
    same = [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user1"}]
    cfg = make_cfg(sync_users={"user1": {"username": "user1"}})
    assert ping(cfg, same)["viewers"] == []
    addon_sync._CACHE.clear()
    cfg = make_cfg(sync_users={"kv-2": {"username": "user1"}})
    viewers = [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}]
    assert [(v["viewer_id"], v["instance"]) for v in ping(cfg, viewers)["viewers"]] == [("kv-2", "default~kv-2")]


def test_kodi_users_offer_sync_viewers_by_viewer_id(monkeypatch: pytest.MonkeyPatch):
    from api.authenticationAPI import register_auth

    cfg = make_cfg()
    ping(cfg, [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}])
    monkeypatch.setattr("api.authenticationAPI.load_config", lambda: cfg)
    monkeypatch.setattr("api.authenticationAPI.save_config", lambda *_a, **_k: None)
    app = FastAPI()
    register_auth(app)
    users = TestClient(app).get("/api/kodi/users").json()["users"]
    assert [(u["id"], u["username"]) for u in users] == [("kv-1", "user1"), ("kv-2", "user2")]


def _setup_client(cfg: dict[str, Any]) -> TestClient:
    from api.authenticationAPI import register_auth
    from cw_platform import config_base

    config_base.save_config(cfg)
    app = FastAPI()
    register_auth(app)
    return TestClient(app)


def _saved() -> dict[str, Any]:
    from cw_platform import config_base

    return config_base.load_config()


def _with_trakt(cfg: dict[str, Any]) -> dict[str, Any]:
    return {**cfg, "trakt": {"client_id": "c" * 32, "client_secret": "s" * 32, "access_token": "t" * 32, "refresh_token": "r" * 32}}


def test_sync_setup_for_the_only_viewer_adds_one_two_way_pair(monkeypatch: pytest.MonkeyPatch):
    triggered: list[str] = []
    monkeypatch.setattr(addon_sync, "trigger_pair_runs", lambda cfg, inst: triggered.append(inst) or [])
    cfg = _with_trakt(make_cfg())
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    http = _setup_client(cfg)
    view = http.get("/api/kodi/addon/sync").json()
    assert {"provider": "TRAKT", "instance": "default", "label": "Trakt"} in view["targets"]
    assert [(v["viewer_id"], v["instance"], v["pairs"]) for v in view["viewers"]] == [("kv-1", "default", [])]
    body = {"viewer_id": "kv-1", "target": "TRAKT", "target_instance": "default"}
    done = http.post("/api/kodi/addon/sync-setup", json=body).json()
    assert [(p["provider"], p["instance"]) for p in done["viewers"][0]["pairs"]] == [("TRAKT", "default")]
    assert http.post("/api/kodi/addon/sync-setup", json=body).json()["ok"] is True
    pairs = _saved()["pairs"]
    assert len(pairs) == 1
    pair = pairs[0]
    assert (pair["source"], pair["source_instance"], pair["target"], pair["target_instance"], pair["mode"], pair["enabled"]) == (
        "KODI", "default", "TRAKT", "default", "two-way", True)
    assert pair["features"]["history"]["enable"] is True and pair["features"]["history"]["remove"] is False
    assert pair["features"]["ratings"]["enable"] is True and "watchlist" not in pair["features"]
    assert pair["features"]["ratings"]["types"] == ["movies", "shows", "seasons", "episodes"]
    assert pair["features"]["ratings"]["mode"] == "all" and pair["features"]["ratings"]["remove"] is False
    assert triggered == ["default"]


def test_sync_setup_gives_a_second_viewer_its_own_sync_user(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(addon_sync, "trigger_pair_runs", lambda cfg, inst: [])
    cfg = _with_trakt(make_cfg())
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    viewers = [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}]
    ping(cfg, viewers)
    http = _setup_client(cfg)
    done = http.post("/api/kodi/addon/sync-setup", json={"viewer_id": "kv-2", "target": "TRAKT"}).json()
    assert [(v["viewer_id"], v["instance"]) for v in done["viewers"]] == [("kv-1", "default"), ("kv-2", "default~kv-2")]
    saved = _saved()
    assert saved["kodi"]["sync_users"]["kv-2"]["username"] == "user2"
    assert [(p["source_instance"], p["target"]) for p in saved["pairs"]] == [("default~kv-2", "TRAKT")]
    assert [(v["viewer_id"], v["instance"]) for v in ping(saved, viewers)["viewers"]] == [("kv-1", "default"), ("kv-2", "default~kv-2")]


def test_sync_setup_refuses_what_it_cannot_set_up(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    http = _setup_client(cfg)
    assert http.post("/api/kodi/addon/sync-setup", json={"viewer_id": "kv-1", "target": "TRAKT"}).status_code == 400
    assert http.post("/api/kodi/addon/sync-setup", json={"viewer_id": "kv-9", "target": "TRAKT"}).status_code == 404
    http = _setup_client(_with_trakt(make_cfg(server="http://kodi:8080", connection_verified=True)))
    assert http.post("/api/kodi/addon/sync-setup", json={"viewer_id": "kv-1", "target": "TRAKT"}).json()["error"] == "This Kodi syncs over JSON-RPC"
    assert _saved()["pairs"] == []


def test_old_sync_switch_is_removed_from_saved_config():
    from cw_platform import config_base

    cfg: dict[str, Any] = {"runtime": {"kodi_addon_sync": True, "debug": False}}
    assert config_base.cleanup_obsolete_config_keys(cfg) == ["runtime.kodi_addon_sync"]
    assert cfg == {"runtime": {"debug": False}}


def test_not_ready_until_full_upload_and_read_never_empty():
    cfg = make_cfg()
    ping(cfg)
    assert not common.is_configured({**cfg, "_cw_provider_instance": "default"})
    with pytest.raises(KodiAuthError):
        module(cfg).build_index("history")
    assert full(cfg)["complete"] is True
    assert common.is_configured({**cfg, "_cw_provider_instance": "default"})
    assert module(cfg).health()["ok"] is True


def test_full_upload_in_parts_commits_on_last_part():
    cfg = make_cfg()
    ping(cfg)
    assert full(cfg, episodes=[], tvshows=[], parts=2)["complete"] is False
    assert not addon_sync.addon_ready(cfg, "default")
    assert full(cfg, movies=[], part=2, parts=2)["complete"] is True
    assert len(module(cfg).build_index("collection")) == 2


def test_times_are_utc_and_device_zone_is_optional():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, timezone="Asia/Tokyo")
    assert [row["watched_at"] for row in module(cfg).build_index("history").values()] == ["2026-01-01T18:04:05Z"]
    full(cfg, movies=[movie(lastplayed="2026-01-05T08:00:00Z")], upload_id="u-2", timezone="")
    assert [row["watched_at"] for row in module(cfg).build_index("history").values()] == ["2026-01-05T08:00:00Z"]
    with pytest.raises(addon_sync.SyncRequestError):
        full(cfg, upload_id="u-3", timezone="Not/AZone")
    with pytest.raises(addon_sync.SyncRequestError):
        full(cfg, movies=[movie(lastplayed="yesterday")], upload_id="u-4")
    assert addon_sync.to_utc("2026-10-25 00:30:00") == "2026-10-25T00:30:00Z"
    assert addon_sync.to_utc("2026-10-25 01:30:00") == "2026-10-25T01:30:00Z"
    assert addon_sync.AddonKodiClient.kodi_time("2026-10-25T01:30:00Z") == "2026-10-25T01:30:00Z"


def test_copies_of_one_item_get_one_change():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched(), unwatched(movieid=13, file="nfs://nas/movies/fc-4k.mkv")], episodes=[], tvshows=[])
    assert module(cfg).add("history", [{"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-01-03T10:00:00Z"}])["count"] == 1
    assert rate_fight_club(cfg, 9)["count"] == 1
    changes = queue(cfg)["changes"]
    assert [c["params"]["movieid"] for c in changes] == [12, 12]
    assert ratings(cfg) == {"Fight Club": 9}


def test_after_counts_only_applied_uploads():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    first = addon_sync.last_upload_seq("default", "kv-1")
    source = {"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-01-03T10:00:00Z"}
    full(cfg, upload_id="u-2", part=1, parts=2)
    assert addon_sync.last_upload_seq("default", "kv-1") > first
    assert module(cfg).add("history", [source])["count"] == 1
    assert [c["after"] for c in queue(cfg)["changes"]] == [first]
    later = next_seq()
    delta(cfg, upload_seq=later, movies=[movie(userrating=6)])
    assert module(cfg).add("ratings", [{**source, "rating": 9}])["count"] == 1
    assert [c["after"] for c in queue(cfg)["changes"]] == [first, later]
    full(cfg, upload_id="u-2", part=2, parts=2)
    assert addon_sync._applied_upload_seq("default", "kv-1") == later


def test_write_is_queued_with_item_and_never_undone_by_an_ack():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    source = {"type": "episode", "ids": {"tvdb": "3254641"}, "show_ids": {"tmdb": "63639"}, "season": 2, "episode": 5,
              "watched_at": "2026-01-03T10:00:00Z"}
    assert module(cfg).add("history", [source])["count"] == 1
    fetched = queue(cfg)
    assert fetched["ok"] is True
    assert fetched["changes"][0].pop("queued_at")
    assert fetched["changes"][0].pop("after") == 1
    assert len(fetched["changes"][0].pop("change_id")) == 16
    assert fetched["changes"] == [{"seq": 1, "method": "VideoLibrary.SetEpisodeDetails",
                                 "params": {"episodeid": 40, "playcount": 1, "lastplayed": "2026-01-03T10:00:00Z"},
                                 "item": {"type": "episode", "uniqueid": {"tvdb": "3254641"}, "title": "Home",
                                          "file": "nfs://nas/tv/expanse/s02e05.mkv", "showtitle": "The Expanse",
                                          "season": 2, "episode": 5, "show_uniqueid": {"tmdb": "63639"}}}]
    assert len(module(cfg).build_index("history")) == 2
    assert ping(cfg)["queue_pending"] == 1
    assert ack(cfg, (1, "failed")) == {"ok": True, "applied": 0, "failed": 1}
    assert addon_sync.pending_changes(cfg, "default") == []
    assert len(module(cfg).build_index("history")) == 2


def test_changes_coalesce_per_item_and_field():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 6)
    rate_fight_club(cfg, 9)
    watch_fight_club(cfg)
    changes = addon_sync.pending_changes(cfg, "default")
    assert [(c["seq"], c["params"]) for c in changes] == [(2, {"movieid": 12, "userrating": 9}),
                                                           (3, {"movieid": 12, "playcount": 1, "lastplayed": "2026-01-03T10:00:00Z"})]
    assert ack(cfg, (1, "applied"))["applied"] == 0
    assert ratings(cfg) == {"Fight Club": 9}


def test_rows_and_queue_are_read_under_one_lock(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    original = addon_sync._load_changes
    held: list[bool] = []

    def probe(device, viewer_id):
        got: list[bool] = []
        worker = threading.Thread(target=lambda: got.append(addon_sync._LOCK.acquire(timeout=0.05)))
        worker.start()
        worker.join()
        if got and got[0]:
            addon_sync._LOCK.release()
        held.append(not (got and got[0]))
        return original(device, viewer_id)

    monkeypatch.setattr(addon_sync, "_load_changes", probe)
    module(cfg).build_index("history")
    assert held == [True]


def test_zero_playcount_only_unwatches_with_explicit_record():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    delta(cfg, movies=[unwatched()])
    assert titles(cfg, "history") == {"Fight Club"}
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2")
    assert titles(cfg, "history") == {"Fight Club"}
    delta(cfg, movies=[unwatched(unwatched=True)])
    assert titles(cfg, "history") == set()


def test_partial_delta_does_not_end_another_items_change():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    module(cfg).add("history", [{"type": "episode", "ids": {"tvdb": "3254641"}, "show_ids": {"tmdb": "63639"},
                                 "season": 2, "episode": 5, "watched_at": "2026-01-03T10:00:00Z"}])
    ack(cfg, (1, "applied"))
    delta(cfg, applied_seq=1, movies=[unwatched()])
    assert titles(cfg, "history") == {"Home"}


def test_left_out_rows_and_resets_never_remove_anything():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    full(cfg, movies=[], episodes=[], tvshows=[], upload_id="u-2")
    assert titles(cfg, "history") == {"Fight Club"}
    assert ratings(cfg) == {"Fight Club": 8}
    assert titles(cfg, "collection") == set()
    ping(cfg, library_id="lib-2")
    full(cfg, movies=[], episodes=[episode()], tvshows=[tvshow()], upload_id="u-3")
    assert titles(cfg, "history") == {"Fight Club"}


def test_pending_change_follows_its_item_to_a_new_library():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    watch_fight_club(cfg)
    ping(cfg, library_id="lib-2")
    assert not addon_sync.addon_ready(cfg, "default")
    assert queue(cfg)["changes"] == []
    full(cfg, movies=[unwatched(movieid=33)], upload_id="u-2")
    assert [c["params"]["movieid"] for c in addon_sync.pending_changes(cfg, "default")] == [33]
    assert titles(cfg, "history") == {"Fight Club"}


def test_library_switch_and_back_still_needs_full_upload():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    watch_fight_club(cfg)
    ping(cfg, library_id="lib-2")
    ping(cfg, library_id="lib-1")
    assert not addon_sync.addon_ready(cfg, "default")
    assert queue(cfg)["changes"] == []
    full(cfg, movies=[unwatched()], upload_id="u-2")
    assert [c["seq"] for c in addon_sync.pending_changes(cfg, "default")] == [1]


def test_failed_rating_is_not_cleared_by_an_upload_without_one():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 7)
    ack(cfg, (1, "failed"))
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", applied_seq=1)
    assert ratings(cfg) == {"Fight Club": 7}
    full(cfg, movies=[unwatched(userrating=5)], episodes=[], tvshows=[], upload_id="u-3", applied_seq=1)
    assert ratings(cfg) == {"Fight Club": 5}


def test_reflected_unwatch_stays_unwatched():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    unwatch_fight_club(cfg)
    ack(cfg, (1, "applied"))
    full(cfg, movies=[unwatched(userrating=8)], episodes=[], tvshows=[], upload_id="u-2", applied_seq=1)
    assert titles(cfg, "history") == set()
    assert ratings(cfg) == {"Fight Club": 8}


def test_unreflected_change_wins_over_an_older_upload():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    watch_fight_club(cfg)
    ack(cfg, (1, "applied"))
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", applied_seq=0)
    assert titles(cfg, "history") == {"Fight Club"}
    full(cfg, movies=[movie(lastplayed="2026-01-03 10:00:00", userrating=0)], episodes=[], tvshows=[], upload_id="u-3", applied_seq=1)
    assert titles(cfg, "history") == {"Fight Club"}


def test_unreflected_rating_wins_over_an_older_upload():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 9)
    ack(cfg, (1, "applied"))
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", applied_seq=0)
    assert ratings(cfg) == {"Fight Club": 9}
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-3", applied_seq=1)
    assert ratings(cfg) == {}


def test_stale_multipart_full_keeps_newer_delta_rating():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    delta(cfg, movies=[unwatched(userrating=9)])
    full(cfg, movies=[], episodes=[episode()], tvshows=[tvshow()], upload_id="u-2", part=2, parts=2)
    assert ratings(cfg) == {"Fight Club": 9}


def test_stale_multipart_full_keeps_newer_delta():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    watch_fight_club(cfg)
    ack(cfg, (1, "applied"))
    delta(cfg, applied_seq=1, movies=[movie(lastplayed="2026-01-03 10:00:00", userrating=0)])
    full(cfg, movies=[], episodes=[episode()], tvshows=[tvshow()], upload_id="u-2", part=2, parts=2)
    assert titles(cfg, "history") == {"Fight Club"}


def test_retried_full_upload_is_not_committed_twice():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    delta(cfg, movies=[movie(lastplayed="2026-01-03 10:00:00")])
    assert full(cfg, movies=[unwatched()])["complete"] is True
    assert titles(cfg, "history") == {"Fight Club"}


def test_malformed_uploads_are_refused_and_keep_previous_state():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    envelope = {k: v for k, v in full_body(upload_id="u-2").items() if k not in ("movies", "episodes", "tvshows")}
    with pytest.raises(addon_sync.SyncRequestError):
        upload(cfg, envelope)
    for index, bad in enumerate([{"movieid": "x"}, {"playcount": "invalid"}, {"userrating": 11},
                                 {"resume": {"position": "x", "total": 0}}, {"lastplayed": None}, {"playcount": True},
                                 {"unwatched": "yes"}]):
        with pytest.raises(addon_sync.SyncRequestError) as err:
            full(cfg, movies=[movie(**bad)], upload_id=f"u-bad-{index}")
        assert err.value.code == "invalid_request"
    with pytest.raises(addon_sync.SyncRequestError):
        full(cfg, movies=["nope"], upload_id="u-nope")
    with pytest.raises(addon_sync.SyncRequestError):
        delta(cfg, movies=[{k: v for k, v in movie().items() if k != "userrating"}])
    assert titles(cfg, "history") == {"Fight Club"}


def test_removed_file_keeps_history_but_is_never_written_or_collected():
    cfg = make_cfg()
    ping(cfg)
    ghost = {"uniqueid": {"tmdb": "603"}, "title": "The Matrix", "year": 1999, "playcount": 1, "lastplayed": "2025-05-05 20:00:00",
             "userrating": 9, "resume": {"position": 0, "total": 0}, "in_library": False, "movieid": 99}
    ghost_episode = episode(in_library=False, episodeid=77, uniqueid={"tvdb": "1"}, playcount=1, lastplayed="2025-05-05 21:00:00",
                            show={"uniqueid": {"tmdb": "1399"}, "title": "Game of Thrones", "year": 2011})
    full(cfg, movies=[movie(), ghost], episodes=[ghost_episode])
    assert titles(cfg, "history") == {"Fight Club", "The Matrix", "Home"}
    assert titles(cfg, "collection") == {"Fight Club"}
    assert module(cfg).add("ratings", [{"type": "movie", "ids": {"tmdb": "603"}, "rating": 7}])["count"] == 0
    assert addon_sync.pending_changes(cfg, "default") == []
    delta(cfg, movies=[movie(movieid=120, uniqueid={"tmdb": "603"}, title="The Matrix", lastplayed="2025-05-05 20:00:00")])
    matrix = [row for row in module(cfg).build_index("history").values() if row["title"] == "The Matrix"]
    assert len(matrix) == 1 and matrix[0]["_kodi_id"] == 120


def test_live_row_becomes_removed_through_delta():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    removed = {k: v for k, v in movie().items() if k != "movieid"}
    delta(cfg, movies=[{**removed, "in_library": False}])
    assert titles(cfg, "history") == {"Fight Club"}
    assert titles(cfg, "collection") == set()
    assert rate_fight_club(cfg, 6)["count"] == 0
    assert addon_sync.pending_changes(cfg, "default") == []


def test_new_sole_viewer_needs_the_binding_released():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    watch_fight_club(cfg)
    ping(cfg, [{"viewer_id": "kv-9", "name": "user9"}])
    assert ping(cfg, [{"viewer_id": "kv-9", "name": "user9"}])["viewers"] == []
    assert not addon_sync.addon_ready(cfg, "default")
    for body in ({"version": 1, "viewer_id": "kv-9", "mode": "delta", "upload_seq": next_seq()},
                 full_body(movies=[unwatched()], viewer_id="kv-9", upload_id="u-9")):
        with pytest.raises(addon_sync.SyncRequestError) as err:
            upload(cfg, body)
        assert err.value.code == "viewer_unknown"
    ping(cfg, [{"viewer_id": "kv-9", "name": "user9"}], enabled=False)
    ping(cfg, [{"viewer_id": "kv-9", "name": "user9"}])
    upload(cfg, full_body(movies=[unwatched()], viewer_id="kv-9", upload_id="u-10"))
    assert addon_sync.pending_changes(cfg, "default") == []
    assert titles(cfg, "history") == set()


def test_new_library_between_index_and_write_refuses_the_write():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    adapter = module(cfg)
    adapter.build_index("history")
    ping(cfg, library_id="lib-2")
    full(cfg, movies=[unwatched(uniqueid={"tmdb": "603"}, title="The Matrix")], upload_id="u-2")
    res = adapter.add("history", [{"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-01-03T10:00:00Z"}])
    assert res["count"] == 0
    assert addon_sync.pending_changes(cfg, "default") == []


def test_write_keeps_the_identity_of_its_own_index_after_another_read():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    adapter = module(cfg)
    adapter.build_index("history")
    full(cfg, movies=[unwatched(uniqueid={"tmdb": "603"}, title="The Matrix")], upload_id="u-2")
    adapter.build_index("ratings")
    res = adapter.add("history", [{"type": "movie", "ids": {"tmdb": "550"}, "watched_at": "2026-01-03T10:00:00Z"}])
    assert res["count"] == 0
    assert addon_sync.pending_changes(cfg, "default") == []
    with pytest.raises(KodiAuthError):
        adapter.client.rpc("VideoLibrary.SetMovieDetails", {"movieid": 12, "playcount": 1})


def test_reused_kodi_id_keeps_previous_history_and_new_item_is_writable():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    full(cfg, movies=[unwatched(uniqueid={"tmdb": "603"}, title="The Matrix")], episodes=[], tvshows=[], upload_id="u-2")
    assert titles(cfg, "history") == {"Fight Club"}
    assert titles(cfg, "collection") == {"The Matrix"}
    assert module(cfg).add("history", [{"type": "movie", "ids": {"tmdb": "603"}, "watched_at": "2026-01-03T10:00:00Z"}])["count"] == 1
    assert addon_sync.pending_changes(cfg, "default")[0]["params"]["movieid"] == 12
    assert titles(cfg, "history") == {"Fight Club", "The Matrix"}


def test_reused_kodi_id_in_delta_keeps_previous_history():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    delta(cfg, movies=[unwatched(uniqueid={"tmdb": "603"}, title="The Matrix")])
    assert titles(cfg, "history") == {"Fight Club"}
    assert titles(cfg, "collection") == {"The Matrix"}


def test_dropped_full_upload_cannot_be_resumed_by_retries(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    monkeypatch.setattr(addon_sync, "MAX_ROWS", 2)
    delta(cfg, movies=[movie(lastplayed="2026-01-03 10:00:00", userrating=0), unwatched(movieid=13, uniqueid={"tmdb": "603"}, title="The Matrix")])
    assert not addon_sync._path("default", "staging").exists()
    for part in (1, 2):
        with pytest.raises(addon_sync.SyncRequestError) as err:
            full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", part=part, parts=2)
        assert err.value.code == "upload_stale"
    assert titles(cfg, "history") == {"Fight Club"}


def test_old_parts_are_refused_after_a_library_change():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    full(cfg, movies=[movie(uniqueid={"tmdb": "603"}, title="The Matrix")], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    ping(cfg, library_id="lib-2")
    for part in (2, 1, 2):
        with pytest.raises(addon_sync.SyncRequestError) as err:
            full(cfg, movies=[movie(uniqueid={"tmdb": "603"}, title="The Matrix")], episodes=[], tvshows=[], upload_id="u-2", part=part, parts=2)
        assert err.value.code == "upload_stale"
    assert not addon_sync.addon_ready(cfg, "default")
    full(cfg, episodes=[], tvshows=[], upload_id="u-3")
    assert addon_sync.addon_ready(cfg, "default")


def test_old_full_upload_stays_refused_after_many_newer_ones():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    for index in range(2, 80):
        full(cfg, episodes=[], tvshows=[], upload_id=f"u-{index}")
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    assert err.value.code == "upload_stale"
    assert titles(cfg, "history") == {"Fight Club"}


def test_old_delta_is_refused_but_a_repeat_of_the_last_one_is_fine():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    first = next_seq()
    delta(cfg, upload_seq=first, movies=[movie(lastplayed="2026-01-03 10:00:00", userrating=0)])
    second = next_seq()
    delta(cfg, upload_seq=second, movies=[unwatched(unwatched=True)])
    assert delta(cfg, upload_seq=second, movies=[unwatched(unwatched=True)])["ok"] is True
    with pytest.raises(addon_sync.SyncRequestError) as err:
        delta(cfg, upload_seq=first, movies=[movie(lastplayed="2026-01-03 10:00:00", userrating=0)])
    assert err.value.code == "upload_stale"
    assert titles(cfg, "history") == set()


def test_upload_order_survives_a_viewer_remap():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    delta(cfg, movies=[movie(lastplayed="2026-01-03 10:00:00", userrating=0)])
    remapped = make_cfg(sync_users={"kv-1": {"username": "user1"}})
    assert [(v["viewer_id"], v["instance"], v["upload_seq"]) for v in ping(remapped)["viewers"]] == [("kv-1", "default~kv-1", 3)]
    for part in (2, 1):
        with pytest.raises(addon_sync.SyncRequestError) as err:
            full(remapped, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", part=part, parts=2)
        assert err.value.code == "upload_stale"
    assert not addon_sync.addon_ready(remapped, "default~kv-1")


def test_parts_together_over_the_row_limit_are_refused(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    monkeypatch.setattr(addon_sync, "MAX_ROWS", 3)
    assert full(cfg, episodes=[], upload_id="u-2", parts=2)["complete"] is False
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg, movies=[movie(movieid=13, uniqueid={"tmdb": "603"}, title="The Matrix")], upload_id="u-2", part=2, parts=2)
    assert err.value.code == "payload_too_large"
    assert not addon_sync._path("default", "staging").exists()
    assert titles(cfg, "history") == {"Fight Club"}


def test_viewer_state_has_limits_too(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    full(cfg, movies=[movie(movieid=13, uniqueid={"tmdb": "603"}, title="The Matrix")], episodes=[], tvshows=[], upload_id="u-2")
    monkeypatch.setattr(addon_sync, "MAX_ROWS", 1)
    with pytest.raises(addon_sync.SyncRequestError):
        full(cfg, movies=[movie(movieid=14, uniqueid={"tmdb": "13"}, title="Forrest Gump")], episodes=[], tvshows=[], upload_id="u-3")
    monkeypatch.setattr(addon_sync, "MAX_ROWS", 250_000)
    assert titles(cfg, "history") == {"Fight Club", "The Matrix"}
    size = addon_sync._viewer_path("default", "kv-1", "view.json").stat().st_size
    monkeypatch.setattr(addon_sync, "MAX_STORED_BYTES", size + 10)
    with pytest.raises(addon_sync.SyncRequestError):
        full(cfg, movies=[movie(movieid=14, uniqueid={"tmdb": "13"}, title="Forrest Gump")], episodes=[], tvshows=[], upload_id="u-4")
    assert titles(cfg, "history") == {"Fight Club", "The Matrix"}


def test_storage_names_do_not_collide():
    assert addon_sync._path("default~kv-2", "rows") != addon_sync._path("default_kv-2", "rows")
    long_a, long_b = "a" * 150 + "1", "a" * 150 + "2"
    assert addon_sync._path(long_a, "queue") != addon_sync._path(long_b, "queue")
    assert addon_sync._viewer_path("default", "kv-1", "view.json") != addon_sync._viewer_path("default", "kv-2", "view.json")


def test_library_path_filter_applies_to_uploaded_rows():
    cfg = make_cfg(history={"libraries": ["nfs://nas/tv"]})
    ping(cfg)
    full(cfg, episodes=[episode(playcount=1, lastplayed="2026-01-02 03:04:05")])
    assert titles(cfg, "history") == {"Home"}


def test_only_known_methods_and_fields_are_queued():
    with pytest.raises(ValueError, match="method_not_allowed"):
        addon_sync.validate_change("VideoLibrary.RemoveMovie", {"movieid": 1})
    with pytest.raises(ValueError, match="field_not_allowed"):
        addon_sync.validate_change("VideoLibrary.SetMovieDetails", {"movieid": 1, "title": "x"})
    with pytest.raises(ValueError, match="not_in_library"):
        addon_sync.validate_change("VideoLibrary.SetMovieDetails", {"movieid": -5, "playcount": 1})
    assert addon_sync.validate_change("VideoLibrary.SetMovieDetails", {"movieid": 1, "userrating": 7}) == (
        "VideoLibrary.SetMovieDetails", {"movieid": 1, "userrating": 7})


def test_delta_before_full_and_sync_off_on_device_are_refused():
    cfg = make_cfg()
    ping(cfg)
    with pytest.raises(addon_sync.SyncRequestError) as err:
        delta(cfg)
    assert err.value.code == "upload_incomplete"
    full(cfg)
    ping(cfg, enabled=False)
    assert addon_sync.addon_client(cfg, "default") is None
    assert not addon_sync.addon_ready(cfg, "default")
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg)
    assert err.value.code == "sync_disabled"


def test_json_rpc_instances_are_untouched():
    cfg = {"kodi": {"server": "http://kodi.local:8080", "connection_verified": True}}
    assert isinstance(module(cfg).client, common.KodiClient)
    assert common.is_configured({"kodi": {}}) is False


def test_json_rpc_keeps_precedence_with_sync_on():
    cfg = make_cfg(server="http://kodi.local:8080", connection_verified=True)
    assert ping(cfg)["viewers"] == []
    assert isinstance(module(cfg).client, common.KodiClient)
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg)
    assert err.value.code == "viewer_unknown"


def test_probe_counts_addon_instance_as_configured():
    from api.probesAPI import _prov_configured

    cfg = make_cfg()
    ping(cfg)
    assert _prov_configured(cfg, "KODI", "default") is False
    full(cfg)
    assert _prov_configured(cfg, "KODI", "default") is True


def test_pair_ids_use_expanded_pairs(monkeypatch: pytest.MonkeyPatch):
    from cw_platform.orchestrator import _pairs

    cfg = make_cfg()
    cfg["pairs"] = [{"id": "p1", "source": "KODI", "source_instance": "default", "target": "TRAKT", "match_users": True},
                    {"id": "p2", "source": "TRAKT", "target": "KODI", "target_instance": "default~kv-2"},
                    {"id": "p3", "source": "KODI", "source_instance": "default~kv-2", "target": "TRAKT", "enabled": False}]
    monkeypatch.setattr(_pairs, "expand_sync_user_pairs", lambda c: [{**c["pairs"][0], "source_instance": "default~kv-2"}, *c["pairs"][1:]])
    assert addon_sync.pair_ids_for(cfg, "default~kv-2") == ["p1", "p2"]


def test_busy_sync_is_retried_until_accepted(monkeypatch: pytest.MonkeyPatch):
    answers = [{"ok": False, "error": "Sync already running"}, {"ok": True, "run_id": "1"}]
    calls: list[list[str]] = []
    monkeypatch.setattr(addon_sync, "RUN_RETRY_SECONDS", 0)
    monkeypatch.setattr(addon_sync, "_submit_run", lambda ids: calls.append(ids) or answers[len(calls) - 1])
    addon_sync._PENDING_RUNS.add("default")
    assert addon_sync.run_with_retry("default", ["p1"]) is True
    assert calls == [["p1"], ["p1"]]
    assert "default" not in addon_sync._PENDING_RUNS


@pytest.fixture()
def client(monkeypatch: pytest.MonkeyPatch):
    from api import kodiSyncAPI

    state: dict[str, Any] = {"cfg": make_cfg()}
    monkeypatch.setattr(kodiSyncAPI, "load_config", lambda: state["cfg"])
    app = FastAPI()
    app.include_router(kodiSyncAPI.router)
    return TestClient(app), state


def test_endpoints_auth_and_flow(client):
    http, state = client
    base = "/webhook/kodiwatcher/sync/v1"
    headers = {"X-CrossWatch-Token": TOKEN, "X-CrossWatch-Device": "dev-1"}
    assert http.get(f"{base}/queue?viewer_id=kv-1&token={TOKEN}").json()["error"]["code"] == "invalid_request"
    assert http.get(f"{base}/queue?viewer_id=kv-1", headers={"X-CrossWatch-Token": "wrong-token-000000000"}).status_code == 401
    assert http.get(f"{base}/queue?viewer_id=kv-1", headers=headers).json()["error"]["code"] == "sync_disabled"
    ping(state["cfg"])
    assert http.get(f"{base}/queue?viewer_id=nobody", headers=headers).status_code == 409
    body = full_body(movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-9")
    res = http.post(f"{base}/upload", content=gzip.compress(json.dumps(body).encode()), headers={**headers, "Content-Encoding": "gzip"})
    assert res.status_code == 200 and res.json()["complete"] is True
    watch_fight_club(state["cfg"])
    changes = http.get(f"{base}/queue?viewer_id=kv-1", headers=headers).json()["changes"]
    assert [c["seq"] for c in changes] == [1]
    res = http.post(f"{base}/queue/ack", json={"version": 1, "viewer_id": "kv-1",
                                              "results": [{"seq": 1, "status": "applied", "change_id": changes[0]["change_id"]}]}, headers=headers)
    assert res.json() == {"ok": True, "applied": 1, "failed": 0}
    assert http.get(f"{base}/queue?viewer_id=kv-1", headers=headers).json()["changes"] == []


def test_delta_with_a_person_change_starts_one_run_after_a_short_wait(client, monkeypatch: pytest.MonkeyPatch):
    http, state = client
    base = "/webhook/kodiwatcher/sync/v1"
    headers = {"X-CrossWatch-Token": TOKEN, "X-CrossWatch-Device": "dev-1"}
    runs: list[str] = []
    fired = threading.Event()

    def record(cfg, inst):
        runs.append(inst)
        fired.set()
        return ["p1"]

    monkeypatch.setattr(addon_sync, "trigger_pair_runs", record)
    monkeypatch.setattr(addon_sync, "DELTA_RUN_DELAY", 30.0)
    state["cfg"] = {**make_cfg(), "pairs": [{"id": "p1", "source": "TRAKT", "target": "KODI", "target_instance": "default"}]}
    ping(state["cfg"])
    assert http.post(f"{base}/upload", json=full_body(movies=[unwatched()], episodes=[], tvshows=[]), headers=headers).status_code == 200
    assert runs == ["default"]
    runs.clear()
    fired.clear()

    def send(row, seq=None):
        body = {"version": 1, "viewer_id": "kv-1", "mode": "delta", "upload_seq": seq or next_seq(), "movies": [row]}
        return http.post(f"{base}/upload", json=body, headers=headers).json()

    quiet = next_seq()
    assert send(unwatched(), quiet)["changed"] is False
    assert send(unwatched(), quiet)["changed"] is False
    assert "default" not in addon_sync._DELAYED
    assert ping(state["cfg"])["viewers"][0]["run_pending"] is False
    assert send(unwatched(userrating=7))["changed"] is True
    rated = next_seq()
    assert send(unwatched(userrating=8), rated)["changed"] is True
    assert send(unwatched(userrating=8), rated)["changed"] is True
    assert ping(state["cfg"])["viewers"][0]["run_pending"] is True
    assert runs == []
    waiting = addon_sync._DELAYED["default"]
    waiting.cancel()
    now = threading.Timer(0, addon_sync._delayed_run, args=waiting.args)
    with addon_sync._LOCK:
        addon_sync._DELAYED["default"] = now
    now.start()
    assert fired.wait(5)
    assert runs == ["default"]
    assert "default" not in addon_sync._DELAYED


def test_full_upload_run_replaces_a_waiting_delta_run(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(addon_sync, "DELTA_RUN_DELAY", 30.0)
    monkeypatch.setattr(addon_sync, "run_with_retry", lambda inst, ids: True)
    cfg = {**make_cfg(), "pairs": [{"id": "p1", "source": "TRAKT", "target": "KODI", "target_instance": "default"}]}
    assert addon_sync.schedule_pair_runs(cfg, "default") is True
    timer = addon_sync._DELAYED["default"]
    assert addon_sync.trigger_pair_runs(cfg, "default") == ["p1"]
    assert "default" not in addon_sync._DELAYED
    assert timer.finished.is_set()
    assert addon_sync.schedule_pair_runs(make_cfg(), "default") is False


def test_timer_that_already_fired_never_adds_a_second_run(monkeypatch: pytest.MonkeyPatch):
    started: list[list[str]] = []
    monkeypatch.setattr(addon_sync, "DELTA_RUN_DELAY", 30.0)
    monkeypatch.setattr(addon_sync, "run_with_retry", lambda inst, ids: started.append(ids))
    cfg = {**make_cfg(), "pairs": [{"id": "p1", "source": "TRAKT", "target": "KODI", "target_instance": "default"}]}
    addon_sync.schedule_pair_runs(cfg, "default")
    timer = addon_sync._DELAYED["default"]
    addon_sync.trigger_pair_runs(cfg, "default")
    late = threading.Thread(target=addon_sync._delayed_run, args=(cfg, "default"))
    late.start()
    late.join(5)
    assert addon_sync._RERUN.get("default") is None
    assert timer.finished.is_set()
    addon_sync._PENDING_RUNS.discard("default")


def test_full_upload_run_waits_until_a_firing_timer_has_started_its_run(monkeypatch: pytest.MonkeyPatch):
    started: list[list[str]] = []
    entered, release = threading.Event(), threading.Event()
    real_pair_ids = addon_sync.pair_ids_for
    monkeypatch.setattr(addon_sync, "DELTA_RUN_DELAY", 0.05)
    monkeypatch.setattr(addon_sync, "run_with_retry", lambda inst, ids: started.append(ids))
    cfg = {**make_cfg(), "pairs": [{"id": "p1", "source": "TRAKT", "target": "KODI", "target_instance": "default"}]}

    def gated(cfg_, inst):
        if isinstance(threading.current_thread(), threading.Timer):
            entered.set()
            release.wait(5)
        return real_pair_ids(cfg_, inst)

    monkeypatch.setattr(addon_sync, "pair_ids_for", gated)
    ping(cfg)
    addon_sync.schedule_pair_runs(cfg, "default")
    assert entered.wait(5)
    full_run = threading.Thread(target=addon_sync.trigger_pair_runs, args=(cfg, "default"))
    full_run.start()
    pings: list[dict[str, Any]] = []
    pinger = threading.Thread(target=lambda: pings.append(ping(cfg)))
    pinger.start()
    full_run.join(0.3)
    assert full_run.is_alive() and pinger.is_alive()
    release.set()
    full_run.join(5)
    pinger.join(5)
    assert not full_run.is_alive()
    assert started == [["p1"]]
    assert pings[0]["viewers"][0]["run_pending"] is True
    assert "default" not in addon_sync._DELAYED
    addon_sync._PENDING_RUNS.discard("default")
    addon_sync._RERUN.pop("default", None)


def test_retried_delta_keeps_its_run_after_a_failed_commit(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    original = addon_sync._save_changes
    calls = {"n": 0}

    def flaky(*args, **kwargs):
        if calls["n"] == 0:
            calls["n"] += 1
            raise OSError("disk full")
        return original(*args, **kwargs)

    monkeypatch.setattr(addon_sync, "_save_changes", flaky)
    seq = next_seq()
    with pytest.raises(OSError):
        delta(cfg, upload_seq=seq, movies=[unwatched(userrating=7)])
    assert delta(cfg, upload_seq=seq, movies=[unwatched(userrating=7)])["changed"] is True
    assert delta(cfg, movies=[unwatched(userrating=7)])["changed"] is False


def test_oversized_streamed_body_is_refused(client, monkeypatch: pytest.MonkeyPatch):
    from api import kodiSyncAPI

    http, state = client
    ping(state["cfg"])
    monkeypatch.setattr(kodiSyncAPI, "MAX_BODY_BYTES", 64)

    def chunks():
        for _ in range(10):
            yield b"x" * 32

    res = http.post("/webhook/kodiwatcher/sync/v1/upload", content=chunks(), headers={"X-CrossWatch-Token": TOKEN, "X-CrossWatch-Device": "dev-1"})
    assert res.status_code == 413 and res.json()["error"]["code"] == "payload_too_large"


def test_second_device_is_refused_and_cannot_change_device_info():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    reply = ping(cfg, library_id="lib-other", device="dev-2")
    assert reply["enabled"] is False and reply["reason"] == "not_sync_device"
    assert addon_sync.device_info("default")["library_id"] == "lib-1"
    with pytest.raises(addon_sync.SyncRequestError) as err:
        upload(cfg, full_body(upload_id="u-2"), device="dev-2")
    assert (err.value.status, err.value.code) == (403, "not_sync_device")
    with pytest.raises(addon_sync.SyncRequestError) as err:
        queue(cfg, device="dev-2")
    assert err.value.code == "not_sync_device"
    with pytest.raises(addon_sync.SyncRequestError) as err:
        upload(cfg, full_body(upload_id="u-3"), device="")
    assert err.value.code == "invalid_request"
    assert addon_sync.addon_ready(cfg, "default")


def test_turning_sync_off_releases_the_device():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    ping(cfg, enabled=False)
    assert ping(cfg, device="dev-2")["enabled"] is True
    with pytest.raises(addon_sync.SyncRequestError) as err:
        queue(cfg)
    assert err.value.code == "not_sync_device"


def test_pairing_again_resets_the_binding():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    repaired = make_cfg()
    repaired["security"]["webhook_ids"]["kodiwatcher:default"] = "kodi-sync-test-token-0002"
    assert ping(repaired, device="dev-2")["enabled"] is True


def test_first_commit_for_a_viewer_clears_old_kodi_baselines():
    from pathlib import Path

    from cw_platform.config_base import CONFIG_BASE
    from cw_platform.local_db import state as db

    base = Path(CONFIG_BASE())
    item = {"type": "movie", "title": "Old", "ids": {"tmdb": "1"}, "watched": True, "watched_at": "2026-01-01T00:00:00Z"}

    def kodi_rows() -> int:
        return sum(1 for row in db.feature_inventory(base) if str(row.get("provider")).upper() == "KODI")

    db.save_feature_baseline(base, provider="KODI", instance="default", feature="history", items={"tmdb:1": item})
    db.save_feature_baseline(base, provider="TRAKT", instance="default", feature="history", items={"tmdb:1": item})
    db.save_pair_blocks(base, "scope-1", {("KODI", "default", "history"): {"baseline": {"items": {"tmdb:1": item}}}})
    assert kodi_rows() == 1 and db.pair_state_counts(base, "scope-1") == (1, 1)
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    assert kodi_rows() == 0
    assert db.pair_state_counts(base, "scope-1") == (0, 0)
    assert any(str(row.get("provider")).upper() == "TRAKT" for row in db.feature_inventory(base))
    db.save_feature_baseline(base, provider="KODI", instance="default", feature="history", items={"tmdb:1": item})
    full(cfg, upload_id="u-2")
    assert kodi_rows() == 1


def test_queue_pages_with_after_and_limit():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched(), unwatched(movieid=13, uniqueid={"tmdb": "603"}, title="The Matrix"),
                      unwatched(movieid=14, uniqueid={"tmdb": "13"}, title="Forrest Gump")], episodes=[], tvshows=[])
    module(cfg).add("history", [{"type": "movie", "ids": {"tmdb": tmdb}, "watched_at": "2026-01-03T10:00:00Z"} for tmdb in ("550", "603", "13")])
    first = queue(cfg, limit=2)
    assert len(first["changes"]) == 2 and first["has_more"] is True and first["next_after"] == first["changes"][-1]["seq"]
    rest = queue(cfg, after=first["next_after"], limit=2)
    assert len(rest["changes"]) == 1 and rest["has_more"] is False and rest["next_after"] is None
    with pytest.raises(addon_sync.SyncRequestError):
        queue(cfg, limit="x")


def test_parts_over_the_row_limit_get_413(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    monkeypatch.setattr(addon_sync, "MAX_PART_ROWS", 1)
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg, movies=[movie(), movie(movieid=13, uniqueid={"tmdb": "603"}, title="The Matrix")])
    assert (err.value.status, err.value.code) == (413, "payload_too_large")


def test_waiting_instance_is_skipped_not_failed():
    cfg = make_cfg()
    ping(cfg)
    health = module(cfg).health()
    assert health["ok"] is False and health["status"] == "addon_sync_waiting"
    assert set(health["features"].values()) == {False}


def test_endpoint_needs_device_header(client):
    http, state = client
    ping(state["cfg"])
    res = http.get("/webhook/kodiwatcher/sync/v1/queue?viewer_id=kv-1", headers={"X-CrossWatch-Token": TOKEN})
    assert res.status_code == 400 and res.json()["error"]["code"] == "invalid_request"


def view_file():
    return addon_sync._view_file("default", "kv-1")


@pytest.mark.parametrize("damage", ["missing", "corrupt"])
def test_lost_viewer_state_fails_reads_and_rebuilds_from_a_full_upload(damage: str):
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    if damage == "missing":
        view_file().unlink()
    else:
        view_file().write_bytes(b"{not json")
    addon_sync._CACHE.clear()
    with pytest.raises(KodiAuthError):
        module(cfg).build_index("history")
    assert not addon_sync.addon_ready(cfg, "default")
    with pytest.raises(addon_sync.SyncRequestError) as err:
        delta(cfg, movies=[unwatched()])
    assert err.value.code == "upload_incomplete"
    full(cfg, upload_id="u-2")
    assert titles(cfg, "history") == {"Fight Club"}


def test_corrupt_change_log_line_fails_reads():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    watch_fight_club(cfg)
    log = addon_sync._log_file("default", "kv-1", 1)
    log.write_bytes(b"garbage\n" + log.read_bytes())
    with pytest.raises(KodiAuthError):
        module(cfg).build_index("history")


def test_partial_full_commit_is_completed_by_a_retry(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    original = addon_sync._write
    calls = {"n": 0}

    def flaky(path, data):
        if path.name.endswith(".view.json") and calls["n"] == 0:
            calls["n"] += 1
            raise OSError("disk full")
        return original(path, data)

    monkeypatch.setattr(addon_sync, "_write", flaky)
    with pytest.raises(OSError):
        full(cfg, upload_id="u-2")
    assert full(cfg, upload_id="u-2")["complete"] is True
    assert titles(cfg, "history") == {"Fight Club"}


def test_interrupted_compaction_does_not_bring_changes_back(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()])
    watch_fight_club(cfg)
    monkeypatch.setattr(addon_sync, "_unlink", lambda path: None)
    ack(cfg, (1, "applied"))
    assert addon_sync.pending_changes(cfg, "default") == []
    assert queue(cfg)["changes"] == []


def test_device_handoff_during_an_upload_refuses_the_old_device(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    original = addon_sync._clean_rows

    def handoff(*args, **kwargs):
        ping(cfg, enabled=False)
        ping(cfg, device="dev-2")
        return original(*args, **kwargs)

    monkeypatch.setattr(addon_sync, "_clean_rows", handoff)
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg, upload_id="u-2")
    assert err.value.code == "not_sync_device"


def test_first_activation_waits_while_a_sync_runs(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    monkeypatch.setattr(addon_sync, "_sync_running", lambda: True)
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg)
    assert (err.value.status, err.value.code, err.value.retryable) == (503, "temporarily_unavailable", True)
    assert not addon_sync.addon_ready(cfg, "default")
    monkeypatch.setattr(addon_sync, "_sync_running", lambda: False)
    assert full(cfg)["complete"] is True
    assert addon_sync.addon_ready(cfg, "default")


def test_removed_episodes_from_different_shows_stay_apart():
    cfg = make_cfg()
    ping(cfg)
    first = episode(in_library=False, uniqueid={}, playcount=1, lastplayed="2025-05-05 21:00:00",
                    show={"uniqueid": {"tmdb": "100"}, "title": "The Office", "year": 2005})
    second = episode(in_library=False, uniqueid={}, playcount=1, lastplayed="2025-05-06 21:00:00",
                     show={"uniqueid": {"tmdb": "200"}, "title": "The Office", "year": 2001})
    full(cfg, movies=[], episodes=[first, second], tvshows=[])
    assert len(module(cfg).build_index("history")) == 2


def test_live_episodes_without_ids_use_their_show():
    cfg = make_cfg()
    ping(cfg)
    shows = [{"tvshowid": 3, "uniqueid": {"tmdb": "100"}, "title": "The Office", "year": 2005},
             {"tvshowid": 4, "uniqueid": {"tmdb": "200"}, "title": "The Office", "year": 2001}]
    rows = [episode(uniqueid={}, playcount=1, lastplayed="2025-05-05 21:00:00"),
            episode(episodeid=41, tvshowid=4, uniqueid={}, playcount=1, lastplayed="2025-05-06 21:00:00")]
    full(cfg, movies=[], episodes=rows, tvshows=shows)
    assert len(module(cfg).build_index("history")) == 2
    full(cfg, movies=[], episodes=rows[:1], tvshows=shows, upload_id="u-2")
    assert len(module(cfg).build_index("history")) == 2


def test_inventory_update_stays_fast_on_large_libraries():
    import time as _time

    inventory = {"movies": {f"id:{i}": {"movieid": i, "uniqueid": {"tmdb": str(i)}, "file": f"/m/{i}.mkv"} for i in range(1, 20001)},
                 "episodes": {}, "tvshows": {}}
    incoming = {"movies": {f"id:{i}": {"movieid": i, "uniqueid": {"tmdb": str(i)}, "file": f"/m/{i}.mkv"} for i in range(15001, 25001)}}
    started = _time.perf_counter()
    addon_sync._apply_inventory(inventory, incoming)
    assert _time.perf_counter() - started < 5.0
    assert len(inventory["movies"]) == 25000


def test_failed_staging_write_during_a_delta_is_repaired_by_its_retry(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    original = addon_sync._write
    calls = {"n": 0}

    def flaky(path, data):
        if path.name.endswith(".staging.json") and calls["n"] == 0:
            calls["n"] += 1
            raise OSError("disk full")
        return original(path, data)

    monkeypatch.setattr(addon_sync, "_write", flaky)
    seq = next_seq()
    with pytest.raises(OSError):
        delta(cfg, upload_seq=seq, movies=[unwatched(movieid=120)])
    delta(cfg, upload_seq=seq, movies=[unwatched(movieid=120)])
    full(cfg, movies=[], episodes=[], tvshows=[], upload_id="u-2", part=2, parts=2)
    watch_fight_club(cfg)
    assert [c["params"]["movieid"] for c in addon_sync.pending_changes(cfg, "default")] == [120]


def test_change_numbers_are_never_reused_after_a_reset_and_restart():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 7)
    ack(cfg, (1, "applied"))
    full(cfg, movies=[unwatched(userrating=7)], episodes=[], tvshows=[], upload_id="u-2", applied_seq=1)
    view_file().unlink()
    addon_sync._CACHE.clear()
    full(cfg, movies=[unwatched(userrating=7)], episodes=[], tvshows=[], upload_id="u-3", applied_seq=1)
    addon_sync._NEXT_CHANGE.clear()
    addon_sync._CACHE.clear()
    rate_fight_club(cfg, 9)
    assert [c["seq"] for c in addon_sync.pending_changes(cfg, "default")][0] > 1
    full(cfg, movies=[unwatched(userrating=7)], episodes=[], tvshows=[], upload_id="u-4", applied_seq=1)
    assert ratings(cfg) == {"Fight Club": 9}


@pytest.mark.parametrize("acked", [False, True])
def test_lost_change_snapshot_fails_reads(acked: bool):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    watch_fight_club(cfg)
    if acked:
        ack(cfg, (1, "applied"))
    addon_sync._changes_file("default", "kv-1").unlink()
    addon_sync._CACHE.clear()
    with pytest.raises(KodiAuthError):
        module(cfg).build_index("history")
    with pytest.raises(addon_sync.SyncRequestError) as err:
        delta(cfg, movies=[unwatched()])
    assert err.value.code == "upload_incomplete"


def test_adding_an_id_keeps_the_items_state_and_changes():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    rate_fight_club(cfg, 6)
    delta(cfg, movies=[movie(uniqueid={"tmdb": "550", "imdb": "tt0137523"}, playcount=0, lastplayed="", unwatched=True)])
    assert titles(cfg, "history") == set()
    assert len(module(cfg).build_index("collection")) == 1
    assert [(c["params"]["movieid"], c["params"]["userrating"]) for c in addon_sync.pending_changes(cfg, "default")] == [(12, 6)]
    delta(cfg, movies=[movie(uniqueid={"imdb": "tt0137523"}, lastplayed="2026-02-01 10:00:00")])
    assert titles(cfg, "history") == {"Fight Club"}
    assert len(module(cfg).build_index("history")) == 1


def test_retried_delta_never_beats_a_change_made_while_it_was_unfinished(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    original = addon_sync._write
    calls = {"n": 0}

    def flaky(path, data):
        if path.name.endswith(".staging.json") and calls["n"] == 0:
            calls["n"] += 1
            raise OSError("disk full")
        return original(path, data)

    monkeypatch.setattr(addon_sync, "_write", flaky)
    seq = next_seq()
    with pytest.raises(OSError):
        delta(cfg, upload_seq=seq, movies=[unwatched(userrating=7)])
    rate_fight_club(cfg, 9)
    change = addon_sync.pending_changes(cfg, "default")[0]["seq"]
    ack(cfg, (change, "failed"))
    delta(cfg, upload_seq=seq, movies=[unwatched(userrating=7)])
    assert ratings(cfg) == {"Fight Club": 9}


def test_unfinished_last_log_line_is_cut_before_the_next_append():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    watch_fight_club(cfg)
    log = addon_sync._log_file("default", "kv-1", 1)
    log.write_bytes(log.read_bytes() + b'{"op":"set","rec":{"seq"')
    addon_sync._CACHE.clear()
    rate_fight_club(cfg, 8)
    assert sorted(c["seq"] for c in addon_sync.pending_changes(cfg, "default")) == [1, 2]
    assert ratings(cfg) == {"Fight Club": 8}
    assert titles(cfg, "history") == {"Fight Club"}


def test_show_gaining_an_id_keeps_its_episode_state():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[], episodes=[episode(uniqueid={}, playcount=1, lastplayed="2025-05-05 21:00:00")], tvshows=[tvshow()])
    module(cfg).add("ratings", [{"type": "episode", "ids": {}, "show_ids": {"tmdb": "63639"}, "season": 2, "episode": 5, "rating": 7}])
    richer = {**tvshow(), "uniqueid": {"tmdb": "63639", "tvdb": "280619"}}
    delta(cfg, episodes=[episode(uniqueid={}, playcount=0, lastplayed="", unwatched=True)], tvshows=[richer])
    assert titles(cfg, "history") == set()
    assert len(module(cfg).build_index("collection")) == 1
    assert [c["params"].get("userrating") for c in addon_sync.pending_changes(cfg, "default")] == [7]


def test_ids_seen_apart_are_merged_once_an_upload_joins_them():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    delta(cfg, movies=[movie(uniqueid={"imdb": "tt0137523"})])
    delta(cfg, movies=[movie(uniqueid={"tmdb": "550", "imdb": "tt0137523"}, playcount=0, lastplayed="", unwatched=True)])
    assert titles(cfg, "history") == set()
    assert len(module(cfg).build_index("ratings")) == 1


def test_removed_row_without_ids_retires_the_live_row_by_file():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    rate_fight_club(cfg, 6)
    delta(cfg, movies=[{**{k: v for k, v in movie().items() if k != "movieid"}, "uniqueid": {}, "in_library": False}])
    assert titles(cfg, "collection") == set()
    assert [c["params"] for c in addon_sync.pending_changes(cfg, "default")] == [{"userrating": 6}]
    full(cfg, episodes=[], tvshows=[], upload_id="u-2")
    delta(cfg, movies=[{**{k: v for k, v in movie().items() if k != "movieid"}, "uniqueid": {"tmdb": "999"}, "in_library": False}])
    assert titles(cfg, "collection") == {"Fight Club"}


def test_queued_writes_do_not_reread_the_change_log(monkeypatch: pytest.MonkeyPatch):
    from pathlib import Path

    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched(movieid=i, uniqueid={"tmdb": str(i)}, title=f"M{i}", file=f"/m/{i}.mkv") for i in range(1, 201)],
         episodes=[], tvshows=[])
    reads: list[str] = []
    original = Path.read_bytes

    def counting(self):
        if self.name.endswith(".log"):
            reads.append(self.name)
        return original(self)

    monkeypatch.setattr(Path, "read_bytes", counting)
    module(cfg).add("history", [{"type": "movie", "ids": {"tmdb": str(i)}, "watched_at": "2026-01-03T10:00:00Z"} for i in range(1, 201)])
    assert reads == []
    assert len(addon_sync.pending_changes(cfg, "default")) == 200


@pytest.mark.parametrize("explicit", [False, True])
def test_merging_only_lets_an_explicit_newer_unwatch_beat_a_watch(explicit: bool):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, episodes=[], tvshows=[])
    delta(cfg, movies=[movie(uniqueid={"imdb": "tt0137523"}, playcount=0, lastplayed="", **({"unwatched": True} if explicit else {}))])
    delta(cfg, movies=[movie(uniqueid={"tmdb": "550", "imdb": "tt0137523"}, playcount=0, lastplayed="")])
    assert titles(cfg, "history") == (set() if explicit else {"Fight Club"})
    assert len(module(cfg).build_index("ratings")) == 1


def test_episode_gaining_its_first_own_id_keeps_its_state_and_changes():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[], episodes=[episode(uniqueid={}, playcount=1, lastplayed="2025-05-05 21:00:00")], tvshows=[tvshow()])
    module(cfg).add("ratings", [{"type": "episode", "ids": {}, "show_ids": {"tmdb": "63639"}, "season": 2, "episode": 5, "rating": 7}])
    delta(cfg, episodes=[episode(uniqueid={"tvdb": "999"}, playcount=0, lastplayed="", unwatched=True)])
    assert titles(cfg, "history") == set()
    assert len(module(cfg).build_index("collection")) == 1
    assert [(c["params"]["episodeid"], c["params"].get("userrating")) for c in addon_sync.pending_changes(cfg, "default")] == [(40, 7)]


@pytest.mark.parametrize("older_unwatch", [True, False])
def test_plain_refresh_does_not_change_which_history_event_wins_a_merge(older_unwatch: bool):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[], episodes=[], tvshows=[])
    tmdb_only = {"movieid": 12, "uniqueid": {"tmdb": "550"}, "file": "nfs://nas/movies/fc.mkv"}
    imdb_only = {"movieid": 13, "uniqueid": {"imdb": "tt0137523"}, "file": "nfs://nas/movies/fc2.mkv"}
    watch, unwatch = {"lastplayed": "2026-01-03 10:00:00"}, {"playcount": 0, "lastplayed": "", "unwatched": True}
    delta(cfg, movies=[movie(**tmdb_only, **(unwatch if older_unwatch else watch))])
    delta(cfg, movies=[movie(**imdb_only, **(watch if older_unwatch else unwatch))])
    delta(cfg, movies=[movie(**tmdb_only, playcount=0, lastplayed="")])
    delta(cfg, movies=[movie(**{**tmdb_only, "uniqueid": {"tmdb": "550", "imdb": "tt0137523"}}, playcount=0, lastplayed="")])
    assert titles(cfg, "history") == ({"Fight Club"} if older_unwatch else set())


def test_full_upload_rows_use_their_own_show_map_not_a_later_delta():
    cfg = make_cfg()
    ping(cfg)
    show_a = {"tvshowid": 3, "uniqueid": {"tmdb": "100"}, "title": "Show A", "year": 2001}
    show_b = {"tvshowid": 3, "uniqueid": {"tmdb": "200"}, "title": "Show B", "year": 2002}
    plain = episode(uniqueid={}, file="nfs://nas/tv/a/s02e05.mkv")
    full(cfg, movies=[], episodes=[plain], tvshows=[show_a])
    full(cfg, movies=[], episodes=[], tvshows=[], upload_id="u-2", parts=2)
    delta(cfg, episodes=[episode(uniqueid={}, file="nfs://nas/tv/b/s02e05.mkv", playcount=1, lastplayed="2026-02-01 10:00:00")],
          tvshows=[show_b])
    full(cfg, movies=[], episodes=[episode(uniqueid={}, file="nfs://nas/tv/a/s02e05.mkv", playcount=1, lastplayed="2026-01-03 10:00:00")],
         tvshows=[show_a], upload_id="u-2", part=2, parts=2)
    entries = addon_sync._load_view("default", "kv-1")["entries"]
    watched = {key for key, entry in entries.items() if entry.get("history", {}).get("watched")}
    assert any("show:tmdb:100" in key for key in watched)


def test_upload_with_missing_parent_show_is_refused_and_history_stays():
    cfg = make_cfg()
    ping(cfg)
    watched = episode(playcount=1, lastplayed="2026-01-03 10:00:00")
    full(cfg, movies=[], episodes=[watched], tvshows=[tvshow()])
    with pytest.raises(addon_sync.SyncRequestError) as err:
        full(cfg, movies=[], episodes=[watched], tvshows=[], upload_id="u-2")
    assert err.value.code == "invalid_request"
    with pytest.raises(addon_sync.SyncRequestError):
        delta(cfg, episodes=[episode(episodeid=41, tvshowid=9, uniqueid={"tvdb": "777"})])
    assert titles(cfg, "history") == {"Home"}


def test_show_losing_its_ids_keeps_its_episodes_in_history():
    cfg = make_cfg()
    ping(cfg)
    watched = episode(uniqueid={}, playcount=1, lastplayed="2026-01-03 10:00:00")
    full(cfg, movies=[], episodes=[watched], tvshows=[tvshow()])
    bare = {**tvshow(), "uniqueid": {}}
    full(cfg, movies=[], episodes=[watched], tvshows=[bare], upload_id="u-2")
    assert titles(cfg, "history") == {"Home"}
    assert len(module(cfg).build_index("collection")) == 1
    assert len(module(cfg).build_index("history")) == 1
    delta(cfg, tvshows=[bare])
    assert titles(cfg, "history") == {"Home"}
    assert len(module(cfg).build_index("collection")) == 1



def test_undelivered_change_is_not_ended_by_a_covering_applied_seq():
    cfg = make_cfg()
    ping(cfg)
    other = unwatched(movieid=13, uniqueid={"tmdb": "603"}, title="The Matrix", file="nfs://nas/movies/m.mkv")
    full(cfg, movies=[unwatched(), other], episodes=[], tvshows=[])
    rate_fight_club(cfg, 7)
    module(cfg).add("ratings", [{"type": "movie", "ids": {"tmdb": "603"}, "rating": 6}])
    full(cfg, movies=[other], episodes=[], tvshows=[], upload_id="u-2")
    changes = addon_sync.pending_changes(cfg, "default")
    assert [(c["seq"], "movieid" in c["params"]) for c in changes] == [(1, False), (2, True)]
    ack(cfg, (2, "applied"))
    full(cfg, movies=[unwatched(), other], episodes=[], tvshows=[], upload_id="u-3", applied_seq=2)
    assert ratings(cfg).get("Fight Club") == 7
    ack(cfg, (1, "applied"))
    full(cfg, movies=[unwatched(userrating=7), other], episodes=[], tvshows=[], upload_id="u-4", applied_seq=2)
    assert ratings(cfg).get("Fight Club") == 7


def test_second_viewer_keeps_the_first_on_the_paired_instance():
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    block = ping(cfg, [{"viewer_id": "kv-1", "name": "user1"}, {"viewer_id": "kv-2", "name": "user2"}])
    assert [(v["viewer_id"], v["instance"]) for v in block["viewers"]] == [("kv-1", "default")]
    assert addon_sync.addon_ready(cfg, "default") is False
    full(cfg, upload_id="u-2")
    assert addon_sync.addon_ready(cfg, "default") is True


def test_second_connection_on_the_same_library_is_refused():
    cfg = make_cfg()
    cfg["security"]["webhook_ids"]["kodiwatcher:kodi2"] = "kodi-sync-test-token-0099"
    ping(cfg)
    other = addon_sync.handle_ping(cfg, "kodi2", {"event": "ping", "device": {"id": "dev-2"},
                                                  "library_sync": {"version": 1, "enabled": True, "library_id": "lib-1",
                                                                   "viewers": [{"viewer_id": "kv-9", "name": "user9"}]}})
    assert other["enabled"] is False and other["reason"] == "library_has_sync_device"
    with pytest.raises(addon_sync.SyncRequestError) as err:
        addon_sync.receive_upload(cfg, "kodi2", full_body(viewer_id="kv-9", upload_id="u-k2"), device_id="dev-2")
    assert err.value.code == "sync_disabled"
    assert ping(cfg)["enabled"] is True


def test_ping_reports_run_pending_and_next_change():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 7)
    addon_sync._PENDING_RUNS.add("default")
    viewer = ping(cfg)["viewers"][0]
    assert viewer["run_pending"] is True and viewer["next_change"] == 2


def test_change_numbers_follow_the_clock_after_a_restart(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 7)
    addon_sync._NEXT_CHANGE.clear()
    addon_sync._CACHE.clear()
    monkeypatch.setattr(addon_sync, "_clock_floor", lambda: 5_000_000)
    rate_fight_club(cfg, 8)
    assert addon_sync.pending_changes(cfg, "default")[0]["seq"] >= 5_000_000


def test_fresh_full_upload_clears_kodi_baselines_for_the_same_viewer():
    from pathlib import Path

    from cw_platform.config_base import CONFIG_BASE
    from cw_platform.local_db import state as db

    base = Path(CONFIG_BASE())
    item = {"type": "movie", "title": "Old", "ids": {"tmdb": "1"}, "watched": True, "watched_at": "2026-01-01T00:00:00Z"}
    cfg = make_cfg()
    ping(cfg)
    full(cfg)
    db.save_feature_baseline(base, provider="KODI", instance="default", feature="history", items={"tmdb:1": item})
    full(cfg, upload_id="u-2")
    assert any(str(r.get("provider")).upper() == "KODI" for r in db.feature_inventory(base))
    full(cfg, upload_id="u-3", fresh=True)
    assert not any(str(r.get("provider")).upper() == "KODI" for r in db.feature_inventory(base))



def test_full_upload_during_a_run_gets_a_follow_up_run(monkeypatch: pytest.MonkeyPatch):
    cfg = make_cfg()
    cfg["pairs"] = [{"id": "p1", "source": "KODI", "source_instance": "default", "target": "TRAKT"}]
    calls: list[list[str]] = []

    def submit(ids):
        calls.append(ids)
        if len(calls) == 1:
            assert addon_sync.trigger_pair_runs(cfg, "default") == []
        return {"ok": True}

    monkeypatch.setattr(addon_sync, "_submit_run", submit)
    addon_sync._PENDING_RUNS.add("default")
    assert addon_sync.run_with_retry("default", ["p1"]) is True
    assert calls == [["p1"], ["p1"]]
    assert "default" not in addon_sync._PENDING_RUNS and "default" not in addon_sync._RERUN


def test_library_holder_is_found_without_the_instance_field():
    cfg = make_cfg()
    cfg["security"]["webhook_ids"]["kodiwatcher:kodi2"] = "kodi-sync-test-token-0099"
    ping(cfg)
    path = addon_sync._path("default", "device")
    data = json.loads(path.read_text(encoding="utf-8"))
    data.pop("instance", None)
    path.write_text(json.dumps(data), encoding="utf-8")
    addon_sync._CACHE.clear()
    other = addon_sync.handle_ping(cfg, "kodi2", {"event": "ping", "device": {"id": "dev-2"},
                                                  "library_sync": {"version": 1, "enabled": True, "library_id": "lib-1",
                                                                   "viewers": [{"viewer_id": "kv-9", "name": "user9"}]}})
    assert other["reason"] == "library_has_sync_device"


def test_ack_needs_the_change_id_of_the_change():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    rate_fight_club(cfg, 7)
    change = addon_sync.pending_changes(cfg, "default")[0]
    for change_id in ("", "not-this-one"):
        result = addon_sync.receive_ack(cfg, "default", {"viewer_id": "kv-1", "results": [
            {"seq": change["seq"], "status": "applied", "change_id": change_id}]}, device_id="dev-1")
        assert result["applied"] == 0
    assert addon_sync.pending_changes(cfg, "default")[0]["seq"] == change["seq"]


def test_upload_applied_seq_raises_the_change_counter():
    cfg = make_cfg()
    ping(cfg)
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[])
    full(cfg, movies=[unwatched()], episodes=[], tvshows=[], upload_id="u-2", applied_seq=9_000_000)
    rate_fight_club(cfg, 7)
    assert addon_sync.pending_changes(cfg, "default")[0]["seq"] > 9_000_000
    addon_sync._NEXT_CHANGE.clear()
    addon_sync._CACHE.clear()
    rate_fight_club(cfg, 8)
    assert addon_sync.pending_changes(cfg, "default")[0]["seq"] > 9_000_000



def test_trigger_right_after_the_last_run_is_not_lost(monkeypatch: pytest.MonkeyPatch):
    import time as _time

    cfg = make_cfg()
    cfg["pairs"] = [{"id": "p1", "source": "KODI", "source_instance": "default", "target": "TRAKT"}]
    calls: list[int] = []
    monkeypatch.setattr(addon_sync, "_run_once", lambda inst, ids: calls.append(1) or True)
    real = addon_sync._LOCK
    fired = {"done": False}

    class HookLock:
        def __enter__(self):
            return real.__enter__()

        def __exit__(self, *exc):
            result = real.__exit__(*exc)
            if calls and not fired["done"]:
                fired["done"] = True
                worker = threading.Thread(target=lambda: addon_sync.trigger_pair_runs(cfg, "default"))
                worker.start()
                worker.join()
            return result

        def acquire(self, *args, **kwargs):
            return real.acquire(*args, **kwargs)

        def release(self):
            return real.release()

    monkeypatch.setattr(addon_sync, "_LOCK", HookLock())
    addon_sync._PENDING_RUNS.add("default")
    addon_sync.run_with_retry("default", ["p1"])
    deadline = _time.time() + 5
    while len(calls) < 2 and _time.time() < deadline:
        _time.sleep(0.01)
    assert len(calls) == 2



def test_worker_waits_for_the_run_it_started(monkeypatch: pytest.MonkeyPatch):
    joined: list[str] = []

    class Run:
        def __init__(self, name):
            self.name = name

        def join(self, timeout=None):
            joined.append(self.name)

    monkeypatch.setattr(addon_sync, "_submit_run", lambda ids: {"ok": True, "_run": Run("mine")})
    addon_sync._PENDING_RUNS.add("default")
    assert addon_sync.run_with_retry("default", ["p1"]) is True
    assert joined == ["mine"]

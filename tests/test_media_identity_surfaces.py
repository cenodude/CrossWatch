# tests/test_media_identity_surfaces.py
# CrossWatch - Capture and legacy provider cache identity regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from cw_platform.id_map import canonical_key
from services import snapshots


MOVIE = {"type": "movie", "title": "Movie", "ids": {"tmdb": "121", "imdb": "tt0167261"}, "rating": 8}
SHOW = {"type": "show", "title": "Show", "ids": {"tmdb": "121", "imdb": "tt0056751"}, "rating": 6}


@pytest.mark.parametrize("kind", ["show", "tv", "series", "anime"])
@pytest.mark.parametrize("tracker", [False, True])
def test_latest_ratings_widget_preserves_tmdb_movie_show_pair(monkeypatch, kind, tracker):
    from services import dashboard_widgets

    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **kw: rows)
    movie = {**MOVIE, "rating": 9}
    show = {**SHOW, "type": kind, "rating": 7}
    items = {"tmdb:121": movie, "tmdb:121#show": show}
    state = {"providers": {"WETRAKR": {"ratings": {"baseline": {"items": items}}}}}
    result = dashboard_widgets.latest_ratings_widget({} if tracker else state, tracker_items=items if tracker else None)
    rows = {row["key"]: row for row in result["items"]}
    assert result["total"] == 2
    assert (rows["tmdb:121"]["type"], rows["tmdb:121"]["title"], rows["tmdb:121"]["rating"]) == ("movie", "Movie", 9)
    assert (rows["tmdb:121#show"]["type"], rows["tmdb:121#show"]["title"], rows["tmdb:121#show"]["rating"]) == ("show", "Show", 7)


def test_latest_ratings_widget_merges_legacy_and_typed_show_rows(monkeypatch):
    from services import dashboard_widgets

    monkeypatch.setattr(dashboard_widgets, "_resolve_missing_art_rows", lambda rows, **kw: rows)
    state = {"providers": {
        "WETRAKR": {"ratings": {"baseline": {"items": {"tmdb:121": MOVIE, "tmdb:121#show": SHOW}}}},
        "CROSSWATCH": {"ratings": {"baseline": {"items": {"tmdb:121": SHOW}}}},
    }}
    result = dashboard_widgets.latest_ratings_widget(state)
    rows = {row["key"]: row for row in result["items"]}
    assert result["total"] == 2
    assert {source["provider"] for source in rows["tmdb:121#show"]["sources"]} == {"WETRAKR", "CROSSWATCH"}
    assert {source["provider"] for source in rows["tmdb:121"]["sources"]} == {"WETRAKR"}


def test_profile_and_dashboard_watchlist_wall_preserve_pair_count(monkeypatch):
    from fastapi import FastAPI
    from api import profileAPI, wallAPI
    from services import watchlist

    state = {"providers": {"WETRAKR": {"watchlist": {"baseline": {"items": {"tmdb:121": MOVIE, "tmdb:121#show": SHOW}}}}}}
    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["WETRAKR"])
    monkeypatch.setattr(watchlist, "_load_hide_set", lambda: set())
    monkeypatch.setattr(profileAPI, "_load_watchlist_state", lambda: state)
    profile_rows = profileAPI._watchlist_items({}, {})
    assert {row["key"] for row in profile_rows} == {"tmdb:121", "tmdb:121#show"}
    monkeypatch.setattr(wallAPI, "load_config", lambda: {})
    monkeypatch.setattr(wallAPI, "_load_state", lambda: state)
    monkeypatch.setattr(wallAPI, "_cache_key", lambda **kw: ("identity-test",))
    monkeypatch.setattr(wallAPI, "_wall_cache_get", lambda key: None)
    monkeypatch.setattr(wallAPI, "_wall_cache_put", lambda *args: None)
    app = FastAPI()
    wallAPI.register_wall(app)
    endpoint = next(route.endpoint for route in app.routes if getattr(route, "path", "") == "/api/state/wall")
    result = endpoint(request=None, both_only=False, active_only=False, limit=1, user_profile="", known_version="")
    assert result["total"] == 2
    assert len(result["items"]) == 1


@pytest.mark.parametrize("kind", ["show", "tv", "anime"])
@pytest.mark.parametrize("legacy", [False, True])
def test_watchlist_page_keeps_tmdb_movie_show_separate(monkeypatch, kind, legacy):
    from services import watchlist

    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["WETRAKR", "CROSSWATCH"])
    monkeypatch.setattr(watchlist, "_load_hide_set", lambda: set())
    show = {**SHOW, "type": kind}
    state = {"providers": {
        "WETRAKR": {"watchlist": {"baseline": {"items": {"tmdb:121": MOVIE}}}},
        "CROSSWATCH": {"watchlist": {"baseline": {"items": {("tmdb:121" if legacy else "tmdb:121#show"): show}}}},
    }}
    rows = {row["key"]: row for row in watchlist.build_watchlist(state, False)}
    assert set(rows) == {"tmdb:121", "tmdb:121#show"}
    assert rows["tmdb:121"]["sources"] == ["wetrakr"]
    assert rows["tmdb:121"]["title"] == "Movie"
    assert rows["tmdb:121"]["type"] == "movie"
    assert rows["tmdb:121#show"]["sources"] == ["crosswatch"]
    assert rows["tmdb:121#show"]["title"] == "Show"
    assert rows["tmdb:121#show"]["type"] == ("anime" if kind == "anime" else "tv")
    assert rows["tmdb:121#show"]["ids"]["tmdb"] == "121"
    assert watchlist._del_key_from_provider_items(state, "CROSSWATCH", "tmdb:121#show")
    assert not state["providers"]["CROSSWATCH"]["watchlist"]["baseline"]["items"]
    assert set(state["providers"]["WETRAKR"]["watchlist"]["baseline"]["items"]) == {"tmdb:121"}


def test_watchlist_page_merges_only_matching_show_aliases(monkeypatch):
    from services import watchlist

    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["WETRAKR", "CROSSWATCH"])
    monkeypatch.setattr(watchlist, "_load_hide_set", lambda: set())
    state = {"providers": {
        "WETRAKR": {"watchlist": {"baseline": {"items": {"tmdb:121": MOVIE, "tmdb:121#show": SHOW}}}},
        "CROSSWATCH": {"watchlist": {"baseline": {"items": {"imdb:tt0056751": {**SHOW, "ids": {"imdb": "tt0056751"}}}}}},
    }}
    rows = {row["key"]: row for row in watchlist.build_watchlist(state, False)}
    assert len(rows) == 2
    assert rows["tmdb:121"]["sources"] == ["wetrakr"]
    assert rows["tmdb:121#show"]["sources"] == ["crosswatch", "wetrakr"]


def test_watchlist_show_key_supplies_type_and_clean_ids(monkeypatch):
    from services import watchlist

    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["CROSSWATCH"])
    monkeypatch.setattr(watchlist, "_load_hide_set", lambda: set())
    state = {"providers": {"CROSSWATCH": {"watchlist": {"baseline": {"items": {"tmdb:121#show": {"title": "Show"}}}}}}}
    row, = watchlist.build_watchlist(state, False)
    assert row["key"] == "tmdb:121#show"
    assert row["type"] == "tv"
    assert row["ids"] == {"tmdb": "121"}
    assert watchlist._type_from_item_or_guess({}, row["key"]) == "tv"
    assert watchlist._guid_variants_from_key_or_item(row["key"]) == [
        "tmdb://121", "com.plexapp.agents.tmdb://121", "com.plexapp.agents.tmdb://121?lang=en",
    ]


@pytest.mark.parametrize("target_kind", ["movie", "show"])
@pytest.mark.parametrize("legacy", [False, True])
def test_watchlist_plex_delete_respects_tmdb_media_type(monkeypatch, target_kind, legacy):
    from services import watchlist

    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["PLEX"])
    monkeypatch.setattr(watchlist, "_HAVE_PLEXAPI", True)
    remote = [SimpleNamespace(type="movie", guid="tmdb://121"), SimpleNamespace(type="show", guid="tmdb://121")]
    for obj in remote:
        obj.removeFromWatchlist = lambda row=obj: remote.remove(row)
    account = SimpleNamespace(watchlist=lambda **kw: list(remote))
    monkeypatch.setattr(watchlist, "MyPlexAccount", lambda **kw: account)
    item = MOVIE if target_kind == "movie" else SHOW
    requested_key = canonical_key(item)
    stored_key = "tmdb:121" if legacy else requested_key
    state = {"providers": {"PLEX": {"watchlist": {"baseline": {"items": {stored_key: item}}}}}}
    watchlist._delete_on_plex_single(requested_key, state, {"plex": {"account_token": "test-token"}})
    assert [row.type for row in remote] == (["show"] if target_kind == "movie" else ["movie"])


def test_watchlist_plex_delete_keeps_imdb_only_lookup(monkeypatch):
    from services import watchlist

    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["PLEX"])
    monkeypatch.setattr(watchlist, "_HAVE_PLEXAPI", True)
    remote = [SimpleNamespace(type="show", guid="imdb://tt0056751")]
    remote[0].removeFromWatchlist = lambda: remote.clear()
    monkeypatch.setattr(watchlist, "MyPlexAccount", lambda **kw: SimpleNamespace(watchlist=lambda **kw: list(remote)))
    watchlist._delete_on_plex_single("imdb:tt0056751", {}, {"plex": {"account_token": "test-token"}})
    assert remote == []


@pytest.mark.parametrize("with_metadata", [False, True])
def test_watchlist_batch_removal_sends_show_with_clean_tmdb(monkeypatch, tmp_path, with_metadata):
    from services import watchlist

    received = []
    ops = SimpleNamespace(
        features=lambda: {"watchlist": True}, capabilities=lambda: {"watchlist": {"remove": True}},
        remove=lambda cfg, items, **kw: (received.extend(items) or {"ok": True, "count": len(items)}),
    )
    monkeypatch.setattr(watchlist, "_registry_sync_providers", lambda: ["WETRAKR"])
    monkeypatch.setattr(watchlist, "load_sync_ops", lambda provider: ops)
    monkeypatch.setattr(watchlist, "build_config_view", lambda cfg, instances: cfg)
    monkeypatch.setattr(watchlist, "_save_sync_state", lambda *a, **kw: None)
    show = SHOW if with_metadata else {"title": "Show"}
    items = {"tmdb:121": MOVIE, "tmdb:121#show": show}
    state = {"providers": {"WETRAKR": {"watchlist": {"baseline": {"items": items}}}}}
    result = watchlist.delete_watchlist_batch(["tmdb:121#show"], "WETRAKR", state, {}, state_path=tmp_path / "state.json")
    assert result["ok"] and result["deleted"] == 1
    assert received[0]["type"] == "show"
    assert received[0]["ids"]["tmdb"] == "121"
    assert set(items) == {"tmdb:121"}


@pytest.mark.parametrize("feature", ["watchlist", "ratings", "collection"])
@pytest.mark.parametrize("source_kind,target_kind,expected", [("show", "movie", False), ("movie", "show", False), ("show", "show", True), ("movie", "movie", True)])
@pytest.mark.parametrize("legacy", [False, True])
def test_analyzer_tmdb_peer_matching_respects_media_type(feature, source_kind, target_kind, expected, legacy):
    from services import analyzer

    source = {"type": source_kind, "ids": {"tmdb": "121"}}
    target = {"type": target_kind, "ids": {"tmdb": "121"}}
    source_key = "tmdb:121" if legacy else canonical_key(source)
    target_key = "tmdb:121" if legacy else canonical_key(target)
    state = {"providers": {
        "WETRAKR": {feature: {"baseline": {"items": {source_key: source}}}},
        "CROSSWATCH": {feature: {"baseline": {"items": {target_key: target}}}},
    }}
    ctx = analyzer._analysis_context(state, {})
    assert bool(analyzer._target_peer_match(ctx, "WETRAKR", feature, source_key, source, "CROSSWATCH")) is expected


@pytest.mark.parametrize("kind", ["show", "anime", "tv", "series"])
def test_analyzer_show_aliases_keep_tmdb_type(kind):
    from services import analyzer

    aliases = analyzer._alias_keys({"type": kind, "ids": {"tmdb": "121"}, "_key": "tmdb:121"})
    assert "tmdb:121#show" in aliases
    assert "tmdb:121" not in aliases


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("kind", ["movie", "show"])
def test_event_presence_does_not_match_other_media_type(monkeypatch, legacy, kind):
    from services import analyzer
    from cw_platform.event_archive import context

    record = MOVIE if kind == "movie" else SHOW
    key = "tmdb:121" if legacy else canonical_key(record)
    state = {"providers": {"CROSSWATCH": {"watchlist": {"baseline": {"items": {key: record}}}}}}
    monkeypatch.setattr(analyzer, "_load_state", lambda *a, **kw: state)
    assert context.current_analyzer_findings("tmdb:121", "watchlist", kind)["count"] == 1
    other = "movie" if kind == "show" else "show"
    assert context.current_analyzer_findings("tmdb:121", "watchlist", other)["count"] == 0
    assert context.current_analyzer_findings("tmdb:121#show", "watchlist")["count"] == int(kind == "show")


@pytest.mark.parametrize("legacy", [False, True])
@pytest.mark.parametrize("reverse", [False, True])
def test_event_titles_preserve_movie_show_pair(monkeypatch, legacy, reverse):
    from cw_platform.event_archive import context

    records = [MOVIE, SHOW]
    if reverse:
        records.reverse()
    states = [{"providers": {"CROSSWATCH": {"ratings": {"baseline": {"items": {
        ("tmdb:121" if legacy else canonical_key(row)): row,
    }}}}}} for row in records]
    monkeypatch.setattr(context, "_TITLE_CACHE", {"index": None, "ts": 0.0})
    monkeypatch.setattr(context, "_baseline_states", lambda: states)
    monkeypatch.setattr(context, "_iter_state_files", lambda *a, **kw: iter(()))
    assert context.resolve_title("tmdb:121", "movie")["title"] == "Movie"
    assert context.resolve_title("tmdb:121", "show")["title"] == "Show"
    assert context.resolve_title("tmdb:121#show")["title"] == "Show"


def test_show_event_has_no_movie_title_fallback(monkeypatch):
    from cw_platform.event_archive import context

    monkeypatch.setattr(context, "_title_index", lambda: {"tmdb:121": MOVIE})
    assert context.resolve_title("tmdb:121#show", "show") is None
    assert context.resolve_title("tmdb:121", "show") is None


def test_event_episode_exact_title_keeps_priority(monkeypatch):
    from cw_platform.event_archive import context

    episode = {"type": "episode", "title": "Episode", "season": 1, "episode": 2}
    monkeypatch.setattr(context, "_title_index", lambda: {"tmdb:121": MOVIE, "tmdb:121#s01e02": episode})
    assert context.resolve_title("tmdb:121#s01e02")["title"] == "Episode"


@pytest.mark.parametrize("present_kind", ["movie", "show"])
def test_legacy_archived_show_event_uses_stored_type(monkeypatch, config_base, present_kind):
    from services import analyzer
    from cw_platform.event_archive import context
    from cw_platform.event_archive.db import connect
    from cw_platform.event_archive.recorder import make_event, record_events

    record = MOVIE if present_kind == "movie" else SHOW
    state = {"providers": {"CROSSWATCH": {"watchlist": {"baseline": {"items": {canonical_key(record): record}}}}}}
    monkeypatch.setattr(analyzer, "_load_state", lambda *a, **kw: state)
    conn = connect(":memory:")
    try:
        record_events([make_event(event_type="write_succeeded", feature="watchlist", operation="add",
                                  item_key="tmdb:121", media_type="show", destination_provider="CROSSWATCH")], conn=conn)
        event_id = conn.execute("SELECT id FROM events").fetchone()[0]
        result = context.build_context(event_id=event_id, conn=conn)
        assert result["context"]["analyzer_findings"]["count"] == int(present_kind == "show")
        assert result["context"]["item_state"]["present_somewhere"] is (present_kind == "show")
        assert conn.execute("SELECT item_key FROM events").fetchone()[0] == "tmdb:121"
    finally:
        conn.close()


@pytest.mark.parametrize("provider", ["PUBLICMETADB", "CROSSWATCH", "TMDB"])
@pytest.mark.parametrize("feature", ["watchlist", "ratings", "collection"])
def test_capture_index_preserves_movie_and_show(provider, feature):
    result = snapshots._canonicalize_index(provider, feature, {"movie": MOVIE, "show": SHOW})
    assert set(result) == {"tmdb:121", "tmdb:121#show"}
    assert result["tmdb:121"]["type"] == "movie"
    assert result["tmdb:121#show"]["type"] == "show"


@pytest.mark.parametrize("provider", ["PUBLICMETADB", "CROSSWATCH"])
@pytest.mark.parametrize("feature", ["watchlist", "ratings"])
def test_capture_compare_restore_roundtrip_preserves_both_types(tmp_path, monkeypatch, provider, feature):
    from test_capture_restore_compare_tools import MutableSyncOps

    ops = MutableSyncOps({feature: {"movie": {**MOVIE, "id": "movie"}, "show": {**SHOW, "id": "show"}}})
    monkeypatch.setattr(snapshots, "CONFIG", tmp_path)
    monkeypatch.setattr(snapshots, "load_sync_ops", lambda *a: ops)
    monkeypatch.setattr(snapshots, "build_provider_config_view", lambda *a: {})
    timestamp = datetime(2026, 9, 28, 12, tzinfo=timezone.utc)
    monkeypatch.setattr(snapshots, "_utc_now", lambda: timestamp)
    first = snapshots.create_snapshot(provider, feature, cfg={})
    assert first["ok"]
    loaded = snapshots.read_snapshot(first["path"])
    assert loaded["stats"]["count"] == 2
    assert set(loaded["items"]) == {"tmdb:121", "tmdb:121#show"}
    ops.current_by_feature[feature].pop("show")
    timestamp += timedelta(minutes=1)
    second = snapshots.create_snapshot(provider, feature, cfg={})
    compared = snapshots.diff_snapshots(first["path"], second["path"])
    assert compared["summary"]["removed"] == 1
    assert compared["summary"]["added"] == 0
    assert compared["removed"][0]["key"] == "tmdb:121#show"
    restored = snapshots.restore_snapshot(first["path"], mode="merge", cfg={})
    assert restored["ok"] and restored["added"] == 1
    assert set(ops.current_by_feature[feature]) == {"movie", "show"}
    assert snapshots.restore_snapshot(first["path"], mode="merge", cfg={})["added"] == 0
    loaded["items"] = {"tmdb:121": {**SHOW, "id": "show"}}
    loaded["stats"]["count"] = 1
    (tmp_path / "snapshots" / first["path"]).write_text(json.dumps(loaded), encoding="utf-8")
    ops.current_by_feature[feature].pop("show")
    restored = snapshots.restore_snapshot(first["path"], mode="merge", cfg={})
    assert restored["ok"] and restored["added"] == 1
    assert ops.current_by_feature[feature]["movie"]["type"] == "movie"
    assert ops.current_by_feature[feature]["show"]["type"] == "show"


@pytest.mark.parametrize("provider,id_key", [("WETRAKR", "wetrakr"), ("PLEX", "plex"), ("ANILIST", "anilist")])
def test_capture_native_ids_remain_unchanged(provider, id_key):
    item = {**SHOW, "ids": {**SHOW["ids"], id_key: "987"}}
    assert snapshots._canonical_item_key(provider, "watchlist", "old", item) == f"{id_key}:987"


@pytest.mark.parametrize("kind,suffix", [("season", "#season:1"), ("episode", "#s01e02")])
def test_capture_coordinates_remain_unchanged(kind, suffix):
    item = {"type": kind, "ids": {"tmdb": "121"}, "season": 1, "episode": 2}
    assert snapshots._canonical_item_key("PUBLICMETADB", "ratings", "old", item) == f"tmdb:121{suffix}"


def test_stremio_remove_clears_legacy_show_rating(tmp_path, monkeypatch):
    from providers.sync.stremio import _ratings

    path = tmp_path / "ratings.json"
    path.write_text(json.dumps({"items": {"tmdb:121": SHOW}}))
    monkeypatch.setattr(_ratings, "_cache_path", lambda *a: path)
    monkeypatch.setattr(_ratings, "is_capture_mode", lambda: False)
    requests = []

    def post(url, **kwargs):
        requests.append(kwargs["json"])
        return SimpleNamespace(status_code=200)

    adapter = SimpleNamespace(client=SimpleNamespace(session=SimpleNamespace(post=post), auth_key=lambda: "test"))
    assert _ratings.remove(adapter, [SHOW])["ok"]
    assert _ratings.build_index(adapter) == {}
    assert json.loads(path.read_text())["items"] == {}
    assert requests[0]["mediaType"] == "series" and requests[0]["status"] is None


def test_stremio_update_migrates_legacy_show_without_duplicate_or_lost_movie(tmp_path, monkeypatch):
    from providers.sync.stremio import _ratings

    path = tmp_path / "ratings.json"
    path.write_text(json.dumps({"items": {"tmdb:121": SHOW}}))
    monkeypatch.setattr(_ratings, "_cache_path", lambda *a: path)
    monkeypatch.setattr(_ratings, "is_capture_mode", lambda: False)
    monkeypatch.setattr(_ratings, "_remote_status", lambda *a: "loved")
    monkeypatch.setattr(_ratings, "_status_for_rating", lambda *a: "loved")
    adapter = SimpleNamespace()
    assert _ratings.add(adapter, [{**SHOW, "rating": 9}, MOVIE])["ok"]
    first = _ratings.build_index(adapter)
    assert set(first) == {"tmdb:121", "tmdb:121#show"}
    assert first["tmdb:121#show"]["rating"] == 9
    assert first["tmdb:121"]["rating"] == 8
    assert _ratings.add(adapter, [{**SHOW, "rating": 9}, MOVIE])["ok"]
    assert _ratings.build_index(adapter) == first


@pytest.mark.parametrize("ignored", [True, False])
def test_anilist_legacy_shadow_preserves_ignore_and_remote_entry(tmp_path, monkeypatch, ignored):
    from providers.sync.anilist import _watchlist

    monkeypatch.setenv("CW_PAIR_SCOPE", "identity-test")
    monkeypatch.delenv("CW_CAPTURE_MODE", raising=False)
    entry = {"type": "show", "source_ids": {"tmdb": "121"}, "ignored": ignored,
             "anilist_id": 456, "list_entry_id": 789}
    path = tmp_path / "watchlist.json"
    path.write_text(json.dumps({"tmdb:121": entry}))
    monkeypatch.setattr(_watchlist, "_shadow_path", lambda: path)
    monkeypatch.setattr(_watchlist, "_anime_enrich", lambda adapter, item: dict(item))
    monkeypatch.setattr(_watchlist, "_resolve_media_id", lambda *a: pytest.fail("Legacy shadow must avoid resolving again"))
    calls = []

    def gql(query, variables, **kwargs):
        calls.append((query, variables))
        return {"DeleteMediaListEntry": {"deleted": True}}

    adapter = SimpleNamespace(key_of=canonical_key, client=SimpleNamespace(viewer=lambda: {"id": 1}, gql=gql))
    assert _watchlist._shadow_load() == {"tmdb:121#show": entry}
    if ignored:
        result = _watchlist.add_detailed(adapter, [SHOW])
        assert result["skipped_keys"] == ["tmdb:121#show"]
        assert not calls
    assert _watchlist.remove(adapter, [SHOW]) == (1, [])
    assert _watchlist._shadow_load() == {}
    assert len(calls) == (0 if ignored else 1)
    if calls:
        assert calls[0][1] == {"id": 789}


@pytest.mark.parametrize("with_movie", [False, True])
def test_jellyfin_legacy_show_shadow_does_not_create_phantom_rating(tmp_path, monkeypatch, with_movie):
    from providers.sync.jellyfin import _ratings, _common

    shadow_path = tmp_path / "shadow.json"
    shadow_path.write_text(json.dumps({"tmdb:121": 1}))
    monkeypatch.setattr(_ratings, "_shadow_path", lambda: str(shadow_path))
    monkeypatch.setattr(_ratings, "_meta_path", lambda: str(tmp_path / "meta.json"))
    monkeypatch.setattr(_ratings, "_pair_scope", lambda: "test")
    monkeypatch.setattr(_ratings, "_is_capture_mode", lambda: False)
    monkeypatch.setattr(_ratings, "_limit", lambda *a: 100)
    monkeypatch.setattr(_ratings, "jf_scope_ratings", lambda *a: {})
    monkeypatch.setattr(_ratings, "jf_get_library_roots", lambda *a: {})
    monkeypatch.setattr(_ratings, "jf_resolve_library_id", lambda *a: "")
    monkeypatch.setattr(_ratings, "_thaw_if_present", lambda *a: None)
    monkeypatch.setattr(_ratings, "_flush", lambda: None)
    rows = [{"Id": "show-id", "Type": "Series", "Name": "Show", "ProviderIds": {"Tmdb": "121"}, "UserRating": 6}]
    if with_movie:
        rows.append({"Id": "movie-id", "Type": "Movie", "Name": "Movie", "ProviderIds": {"Tmdb": "121"}, "UserRating": 8})
    client = SimpleNamespace(get=lambda *a, **kw: SimpleNamespace(json=lambda: {"Items": rows, "TotalRecordCount": len(rows)}))
    adapter = SimpleNamespace(client=client, cfg=SimpleNamespace(user_id="test"))
    index = _ratings.build_index(adapter)
    assert set(index) == ({"tmdb:121", "tmdb:121#show"} if with_movie else {"tmdb:121#show"})
    assert not any(row.get("shadow") for row in index.values())
    monkeypatch.setattr(_common, "resolve_item_id", lambda *a: "show-id")

    def rate(http, uid, item_id, value):
        rows[:] = [row for row in rows if row["Id"] != item_id]
        return True

    monkeypatch.setattr(_ratings, "_rate", rate)
    assert _ratings.remove(adapter, [SHOW]) == (1, [])
    after = _ratings.build_index(adapter)
    assert set(after) == ({"tmdb:121"} if with_movie else set())
    if with_movie:
        assert after["tmdb:121"]["rating"] == 8
        assert json.loads(shadow_path.read_text())["items"] == {"tmdb:121": 1}


def test_jellyfin_migrated_shadow_preserves_pending_movie(tmp_path, monkeypatch):
    from providers.sync.jellyfin import _ratings

    path = tmp_path / "shadow.json"
    monkeypatch.setattr(_ratings, "_shadow_path", lambda: str(path))
    monkeypatch.setattr(_ratings, "_pair_scope", lambda: "test")
    monkeypatch.setattr(_ratings, "_is_capture_mode", lambda: False)
    _ratings._shadow_save({"tmdb:121": 1})
    assert json.loads(path.read_text())["identity_schema"] == 2
    assert _ratings._shadow_load(current_items={"tmdb:121#show": SHOW}) == {"tmdb:121": 1}


@pytest.mark.parametrize("deferred", [False, True])
def test_jellyfin_write_does_not_mark_unread_legacy_shadow_migrated(tmp_path, monkeypatch, deferred):
    from providers.sync.jellyfin import _ratings

    path = tmp_path / "shadow.json"
    path.write_text(json.dumps({"tmdb:121": 1}))
    monkeypatch.setattr(_ratings, "_shadow_path", lambda: str(path))
    monkeypatch.setattr(_ratings, "_pair_scope", lambda: "test")
    monkeypatch.setattr(_ratings, "_is_capture_mode", lambda: False)
    if deferred:
        assert _ratings._shadow_load(current_items={}) == {"tmdb:121": 1}
    _ratings._shadow_save({**_ratings._shadow_load(), "tmdb:555": 1})
    assert json.loads(path.read_text())["legacy_keys"] == ["tmdb:121"]
    assert _ratings._shadow_load(current_items={"tmdb:121#show": SHOW, "tmdb:555#show": SHOW}) == {"tmdb:555": 1}
    assert json.loads(path.read_text())["identity_schema"] == 2
    assert "legacy_keys" not in json.loads(path.read_text())

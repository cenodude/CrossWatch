# tests/test_scrobble_history_identity.py
# CrossWatch - Scrobble History Identity Regression Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from importlib import import_module
from types import SimpleNamespace

import pytest

from cw_platform.id_map import canonical_key, keys_for_item, minimal
from providers.scrobble.crosswatch import sink as crosswatch
from providers.scrobble.scrobble import ScrobbleEvent


def episode_event(ids: dict[str, str], title: str | None = "Series") -> ScrobbleEvent:
    return ScrobbleEvent("stop", "episode", ids, title, 2020, 0, 2, 95,
                         "user", "server", "session", {})


@pytest.mark.parametrize("ids,expected", [
    ({"imdb": "tt1234567", "tmdb": "10", "tvdb": "20", "plex": "native", "jellyfin": "native", "emby": "native",
      "trakt": "30", "simkl": "40", "slug": "episode"}, {"imdb": "tt1234567", "tmdb": "10", "tvdb": "20"}),
    ({"imdb": "tt1111111", "imdb_episode": "tt2222222"}, {"imdb": "tt2222222"}),
    ({"tmdb": "10", "tmdb_show": "10", "tvdb_episode": "20", "tvdb_show": "20"}, {}),
])
def test_crosswatch_preserves_only_episode_provider_ids(monkeypatch, ids, expected):
    monkeypatch.setattr(crosswatch, "_tmdb_enrich", lambda *a, **kw: {})
    item = crosswatch._item_from_event(episode_event(ids), {}, 95)
    assert item is not None
    assert item.get("ids", {}) == expected
    assert item["season"] == 0 and item["episode"] == 2


def test_episode_id_is_rejected_when_it_matches_enriched_show_identity(monkeypatch):
    monkeypatch.setattr(crosswatch, "_tmdb_enrich", lambda *a, **kw: {"ids": {"tmdb": "10"}, "title": "Series"})
    item = crosswatch._item_from_event(episode_event({"tmdb_episode": "10", "imdb_episode": "tt1234567"}), {}, 95)
    assert item is not None
    assert item.get("ids") == {"imdb": "tt1234567"}
    assert item["show_ids"] == {"tmdb": "10"}


@pytest.mark.parametrize("show_ids", [None, {}, {"unsupported": "10"}])
def test_legacy_episode_keys_keep_show_ids_and_coordinates(show_ids):
    item = {"type": "episode", "ids": {"tmdb": "1399"}, "season": 1, "episode": 4}
    if show_ids is not None:
        item["show_ids"] = show_ids
    for candidate in (item, minimal(item)):
        assert canonical_key(candidate) == "tmdb:1399#s01e04"
        assert canonical_key(candidate) in keys_for_item(candidate)
        other = {**candidate, "episode": 5}
        assert canonical_key(other) == "tmdb:1399#s01e05"
        assert canonical_key(candidate) != canonical_key(other)


@pytest.mark.parametrize("show_ids", [{}, {"mdblist_show": "10"}])
def test_crosswatch_skips_episode_without_show_identity(monkeypatch, show_ids):
    monkeypatch.setattr(crosswatch, "_tmdb_enrich", lambda *a, **kw: {})
    item = crosswatch._item_from_event(episode_event({"imdb_episode": "tt1234567", **show_ids}, title=None), {}, 95)
    assert item is None


@pytest.mark.parametrize("namespace", ["mal", "anilist", "anidb", "kitsu", "simkl"])
@pytest.mark.parametrize("season", [0, 2])
def test_anime_only_show_identity_survives_crosswatch_for_simkl(monkeypatch, namespace, season):
    from dataclasses import replace
    from providers.sync.crosswatch import _history as tracker_history
    from providers.sync.simkl import _history as simkl_history

    monkeypatch.setattr(crosswatch, "_tmdb_enrich", lambda *a, **kw: {})
    raw = {"_cw_anime_map": {"namespace": namespace, "target_id": "813", "absolute": 52},
           "_anime_absolute": 52, "_simkl_episode_number": 52, "simkl_bucket": "anime", "anime_type": "tv"}
    event = replace(episode_event({f"{namespace}_show": "813", "tvdb_episode": "999001"}, title=None),
                    season=season, raw=raw)
    item = crosswatch._item_from_event(event, {}, 95)
    assert item is not None
    item["watched_at"] = "2026-09-01T12:00:00Z"
    adapter = SimpleNamespace()
    stored = tracker_history._history_minimal(adapter, tracker_history._accepted(item))
    assert tracker_history._history_key(adapter, stored) == f"{namespace}:813#s{season:02d}e02"
    assert stored["ids"] == {"tvdb": "999001"}
    assert simkl_history._show_ids_of_episode(stored) == {namespace: "813"}
    assert stored["season"] == season and stored["episode"] == 2
    for field, value in raw.items():
        assert stored[field] == value


def test_legacy_episode_key_cannot_collide_with_movie_or_show():
    ids = {"tmdb": "10"}
    episode = {"type": "episode", "ids": ids, "season": 0, "episode": 2}
    assert canonical_key(episode) == "tmdb:10#s00e02"
    assert canonical_key(episode) != canonical_key({"type": "movie", "ids": ids})
    assert canonical_key(episode) != canonical_key({"type": "show", "ids": ids})


@pytest.mark.parametrize("show_ids,expected_key", [({}, "slug:series#s00e02"), ({"tmdb_show": "10"}, "tmdb:10#s00e02")])
@pytest.mark.parametrize("provider", ["jellyfin", "emby"])
def test_tracker_episode_ids_reach_destination_id_resolver(monkeypatch, provider, show_ids, expected_key):
    from providers.sync.crosswatch import _history as tracker_history

    monkeypatch.setattr(crosswatch, "_tmdb_enrich", lambda *a, **kw: {})
    item = crosswatch._item_from_event(episode_event({"imdb_episode": "tt1234567", **show_ids}), {}, 95)
    assert item is not None
    item["watched_at"] = "2026-09-01T12:00:00Z"
    item = tracker_history._accepted(item)
    adapter = SimpleNamespace(client=SimpleNamespace(), cfg=SimpleNamespace(user_id="user", strict_id_matching=True))
    assert tracker_history._history_key(adapter, item) == expected_key
    assert "imdb:tt1234567#s00e02" not in keys_for_item(item)
    history = import_module(f"providers.sync.{provider}._history")
    common = import_module(f"providers.sync.{provider}._common")
    target_id = "1234567890abcdef1234567890abcdef"
    row = {"Id": target_id, "Type": "Episode", "ProviderIds": {"Imdb": "tt1234567"}, "ParentIndexNumber": 0, "IndexNumber": 2}
    if provider == "jellyfin":
        _, prepared, error = history._prepare_want(item)
        assert error is None
        monkeypatch.setattr(common, "_targeted_lookup_item_id", lambda *a, **kw: None)
        monkeypatch.setattr(common, "build_provider_index", lambda *a, **kw: {"imdb.tt1234567": [row]})
    else:
        prepared, _ = history._normalize_for_write(item)
        def query(http, uid, pairs, kind, scope):
            assert pairs == ["imdb.tt1234567"]
            assert kind == "Episode,Series"
            return [row]
        monkeypatch.setattr(common, "_direct_query_by_pairs", query)
    assert prepared is not None
    assert prepared["watched_at"] == item["watched_at"]
    assert common.resolve_item_id(adapter, prepared) == target_id


@pytest.mark.parametrize("provider", ["jellyfin", "emby"])
@pytest.mark.parametrize("ids", [None, {}, {"imdb": "tt1234567"}])
def test_history_write_preserves_metadata_without_episode_ids(provider, ids):
    module = import_module(f"providers.sync.{provider}._history")
    item = {"type": "episode", "title": "Special", "series_title": "Series", "season": 0, "episode": 2,
            "watched_at": "2026-09-01T12:00:00Z", "show_ids": {"tmdb": "10"}, "series_year": 2020}
    if ids is not None:
        item["ids"] = ids
    if provider == "jellyfin":
        key, prepared, error = module._prepare_want(item)
        assert error is None and key == "tmdb:10#s00e02"
    else:
        prepared, _ = module._normalize_for_write(item)
    assert prepared is not None
    for field, value in item.items():
        assert prepared.get(field) == value
    assert canonical_key(prepared) == "tmdb:10#s00e02"


def test_jellyfin_progress_uses_supported_fields_and_keeps_user_timestamp():
    from providers.sync.jellyfin import _progress

    row = {"Id": "movie", "Type": "Movie", "Name": "Movie", "ProviderIds": {"Tmdb": "10"},
           "RunTimeTicks": 1_000_000_000,
           "UserData": {"PlaybackPositionTicks": 300_000_000, "PlayCount": 1, "LastPlayedDate": "2026-09-01T12:00:00Z"}}
    calls = []
    class Http:
        def get(self, path, params=None):
            assert params is not None
            calls.append((path, params))
            assert set(params.get("Fields", "").split(",")) <= {"ProviderIds", "ParentId"}
            assert params["EnableUserData"] is True
            return SimpleNamespace(status_code=200, json=lambda: {"Items": [row], "TotalRecordCount": 1})
    adapter = SimpleNamespace(client=Http(), cfg=SimpleNamespace(user_id="user"))
    result = _progress.build_index(adapter)
    assert result["tmdb:10"]["progress_at"] == "2026-09-01T12:00:00Z"
    assert result["tmdb:10"]["progress_ms"] == 30_000
    assert len(calls) == 1


def test_jellyfin_progress_revalidates_scope_without_membership_fields():
    from providers.sync.jellyfin import _progress

    rows = [{"Id": str(index), "Type": "Movie", "Name": "Movie", "ParentId": parent,
             "ProviderIds": {"Tmdb": str(index)}, "UserData": {"PlaybackPositionTicks": 300_000_000}}
            for index, parent in enumerate(("folder", "folder", "outside", "library-b"), 1)]
    calls = []

    class Http:
        def get(self, path, params=None):
            assert params is not None and params["userId"] == "user"
            calls.append(path)
            if path == "/Items":
                assert params["ParentId"] in {"library-a", "library-b"}
                body = {"Items": rows, "TotalRecordCount": len(rows)}
            elif path == "/Items/folder/Ancestors":
                body = [{"Id": "library-b"}, {"Id": "root"}]
            elif path == "/Items/outside/Ancestors":
                body = [{"Id": "library-c"}, {"Id": "root"}]
            else:
                pytest.fail(f"Unexpected request: {path}")
            return SimpleNamespace(status_code=200, json=lambda: body)

    adapter = SimpleNamespace(client=Http(), cfg=SimpleNamespace(user_id="user", progress_libraries=["library-a", "library-b"]))
    for _ in range(2):
        result = _progress.build_index(adapter)
        assert set(result) == {"tmdb:1", "tmdb:2", "tmdb:4"}
        assert all(item["library_id"] == "library-b" for item in result.values())
    assert calls.count("/Items/folder/Ancestors") == 2
    assert calls.count("/Items/outside/Ancestors") == 2


@pytest.mark.parametrize("status,body", [(503, []), (200, {"Items": []})])
def test_jellyfin_progress_does_not_return_an_index_when_scope_cannot_be_verified(status, body):
    from providers.sync.jellyfin import _progress

    class Http:
        def get(self, path, params=None):
            assert params is not None and params["userId"] == "user"
            if path == "/Items":
                row = {"Id": "movie", "Type": "Movie", "ProviderIds": {"Tmdb": "10"},
                       "UserData": {"PlaybackPositionTicks": 300_000_000}}
                return SimpleNamespace(status_code=200, json=lambda: {"Items": [row], "TotalRecordCount": 1})
            assert path == "/Items/movie/Ancestors"
            return SimpleNamespace(status_code=status, json=lambda: body)

    adapter = SimpleNamespace(client=Http(), cfg=SimpleNamespace(user_id="user", progress_libraries=["library"]))
    with pytest.raises(RuntimeError, match="progress_library_scope_"):
        _progress.build_index(adapter)


@pytest.mark.parametrize("field,value", [("LibraryId", "library"), ("CollectionFolderId", "library"), ("AncestorIds", ["library"])])
def test_jellyfin_progress_retains_supplied_library_membership(field, value):
    from providers.sync.jellyfin import _progress

    class Http:
        def get(self, path, params=None):
            assert path == "/Items"
            rows = [{"Id": str(index), "Type": "Movie", "ProviderIds": {"Tmdb": str(index)}, field: membership,
                     "UserData": {"PlaybackPositionTicks": 300_000_000}}
                    for index, membership in enumerate((value, ["outside"] if isinstance(value, list) else "outside"), 1)]
            return SimpleNamespace(status_code=200, json=lambda: {"Items": rows, "TotalRecordCount": 2})

    adapter = SimpleNamespace(client=Http(), cfg=SimpleNamespace(user_id="user", progress_libraries=["library"]))
    assert set(_progress.build_index(adapter)) == {"tmdb:1"}

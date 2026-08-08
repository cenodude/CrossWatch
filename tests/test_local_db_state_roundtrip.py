# tests/test_local_db_state_roundtrip.py
# CrossWatch - SQLite state round-trip tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.local_db import get_conn
from cw_platform.local_db import state as sqlite_state
from cw_platform.orchestrator._state_store import StateStore


def test_state_database_preserves_current_sync_item_fields(tmp_path) -> None:
    item = {
        "type": "episode",
        "title": "Pilot",
        "year": 2026,
        "season": 1,
        "episode": 2,
        "series_title": "Series",
        "ids": {
            "tmdb": "1",
            "imdb": "tt0000001",
            "tvdb": "2",
            "trakt": "3",
            "simkl": "4",
            "mal": "5",
            "anilist": "6",
            "kitsu": "7",
            "anidb": "8",
            "plex": "9",
            "jellyfin": "10",
            "mdblist": "list",
            "emby": "11",
            "guid": "plex://episode",
            "slug": "pilot",
        },
        "show_ids": {
            "tmdb": "100",
            "imdb": "tt0000100",
            "tvdb": "200",
            "trakt": "300",
            "simkl": "400",
        },
        "watched": True,
        "watched_at": "2026-01-02T03:04:05Z",
        "rating": 8.5,
        "rated_at": "2026-01-03T03:04:05Z",
        "progress_ms": 120000,
        "progress_percent": 45.25,
        "duration_ms": 3600000,
        "progress_at": "2026-01-04T03:04:05Z",
        "progress_at_source": "PLEX",
        "history_id": "history-1",
        "_trakt_history_id": "trakt-history-1",
        "simkl_bucket": "completed",
        "anime_type": "anime",
        "_simkl_episode_number": 12,
        "_floppy_consumption_id": "floppy-consumption-1",
        "_floppy_list_item_id": "floppy-list-1",
        "_floppy_season": 1,
        "_floppy_episode": 2,
        "_trakt_number_abs": 14,
        "_cw_marked": True,
        "_cw_instance": "home",
        "provider_item_id": "provider-item-1",
        "provider_event_id": "provider-event-1",
    }
    state = {
        "providers": {
            "TRAKT": {
                "history": {
                    "baseline": {"items": {"tmdb:1#s01e02": item}},
                    "checkpoint": "checkpoint-1",
                }
            }
        },
        "last_sync_epoch": 123,
    }

    store = StateStore(tmp_path)
    store.save_state(state)
    loaded = store.load_state()

    loaded_item = loaded["providers"]["TRAKT"]["history"]["baseline"]["items"]["tmdb:1#s01e02"]
    assert loaded_item == item
    assert loaded["providers"]["TRAKT"]["history"]["checkpoint"] == "checkpoint-1"
    assert loaded["last_sync_epoch"] == 123


def _feature_row_ids(base_path) -> dict[tuple[str, str, str], int]:
    conn = get_conn(base_path)
    assert conn is not None
    rows = conn.execute("SELECT id,provider,instance,feature FROM provider_feature_state").fetchall()
    return {
        (str(row["provider"]), str(row["instance"]), str(row["feature"])): int(row["id"])
        for row in rows
    }


def _baseline_item_rows(base_path, provider="TRAKT", instance="default", feature="watchlist") -> dict[str, dict]:
    conn = get_conn(base_path)
    assert conn is not None
    rows = conn.execute(
        "SELECT b.item_key,b.id,b.updated_at,b.title,p.id AS pfs_id,p.updated_at AS pfs_updated_at,p.checkpoint_text "
        "FROM provider_feature_state p "
        "JOIN baseline_items b ON b.provider_state_id=p.id "
        "WHERE p.provider=? AND p.instance=? AND p.feature=? "
        "ORDER BY b.item_key",
        (provider, instance, feature),
    ).fetchall()
    return {str(row["item_key"]): dict(row) for row in rows}


def test_state_save_keeps_unchanged_feature_rows(tmp_path) -> None:
    store = StateStore(tmp_path)
    store.save_state(
        {
            "providers": {
                "TRAKT": {"watchlist": {"baseline": {"items": {"movie:1": {"title": "One"}}}}},
                "SIMKL": {"ratings": {"baseline": {"items": {"movie:2": {"title": "Two", "rating": 8}}}}},
            },
            "last_sync_epoch": 100,
        }
    )
    before = _feature_row_ids(tmp_path)

    store.save_state(
        {
            "providers": {
                "TRAKT": {
                    "watchlist": {
                        "baseline": {
                            "items": {
                                "movie:1": {"title": "One"},
                                "movie:3": {"title": "Three"},
                            }
                        }
                    }
                },
                "SIMKL": {"ratings": {"baseline": {"items": {"movie:2": {"title": "Two", "rating": 8}}}}},
            },
            "last_sync_epoch": 101,
        }
    )
    after = _feature_row_ids(tmp_path)

    assert after[("SIMKL", "default", "ratings")] == before[("SIMKL", "default", "ratings")]
    assert after[("TRAKT", "default", "watchlist")] == before[("TRAKT", "default", "watchlist")]
    assert store.load_state()["last_sync_epoch"] == 101


def test_feature_save_updates_only_changed_item_rows(tmp_path, monkeypatch) -> None:
    ticks = iter([1000, 2000])
    monkeypatch.setattr(sqlite_state, "_now", lambda: next(ticks))
    store = StateStore(tmp_path)
    store.save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={
            "movie:1": {"title": "One"},
            "movie:2": {"title": "Two"},
            "movie:3": {"title": "Three"},
        },
    )
    before = _baseline_item_rows(tmp_path)

    store.save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={
            "movie:1": {"title": "One"},
            "movie:2": {"title": "Two updated"},
            "movie:4": {"title": "Four"},
        },
    )
    after = _baseline_item_rows(tmp_path)

    assert set(after) == {"movie:1", "movie:2", "movie:4"}
    assert after["movie:1"]["id"] == before["movie:1"]["id"]
    assert after["movie:1"]["updated_at"] == before["movie:1"]["updated_at"]
    assert after["movie:2"]["id"] == before["movie:2"]["id"]
    assert after["movie:2"]["updated_at"] == 2000
    assert after["movie:2"]["title"] == "Two updated"
    assert after["movie:4"]["updated_at"] == 2000
    assert after["movie:1"]["pfs_id"] == before["movie:1"]["pfs_id"]


def test_feature_checkpoint_update_does_not_rewrite_item_rows(tmp_path, monkeypatch) -> None:
    ticks = iter([1000, 2000])
    monkeypatch.setattr(sqlite_state, "_now", lambda: next(ticks))
    store = StateStore(tmp_path)
    store.save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={"movie:1": {"title": "One"}},
        checkpoint="a",
    )
    before = _baseline_item_rows(tmp_path)

    store.save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={"movie:1": {"title": "One"}},
        checkpoint="b",
    )
    after = _baseline_item_rows(tmp_path)

    assert after["movie:1"]["id"] == before["movie:1"]["id"]
    assert after["movie:1"]["updated_at"] == before["movie:1"]["updated_at"]
    assert after["movie:1"]["pfs_updated_at"] == 2000
    assert after["movie:1"]["checkpoint_text"] == "b"


def test_partial_feature_save_updates_only_target_feature(tmp_path) -> None:
    store = StateStore(tmp_path)
    store.save_state(
        {
            "providers": {
                "TRAKT": {"watchlist": {"baseline": {"items": {"movie:1": {"title": "One"}}}}},
                "SIMKL": {"ratings": {"baseline": {"items": {"movie:2": {"title": "Two", "rating": 8}}}}},
            },
            "last_sync_epoch": 100,
        }
    )
    before = _feature_row_ids(tmp_path)

    store.save_feature_baseline(
        provider="TRAKT",
        feature="watchlist",
        items={"movie:1": {"title": "One"}, "movie:4": {"title": "Four"}},
        last_sync_epoch=200,
    )
    after = _feature_row_ids(tmp_path)
    loaded = store.load_state()

    assert after[("SIMKL", "default", "ratings")] == before[("SIMKL", "default", "ratings")]
    assert set(loaded["providers"]["TRAKT"]["watchlist"]["baseline"]["items"]) == {"movie:1", "movie:4"}
    assert loaded["providers"]["SIMKL"]["ratings"]["baseline"]["items"]["movie:2"]["rating"] == 8
    assert loaded["last_sync_epoch"] == 200


def test_state_feature_read_helpers_load_only_requested_features(tmp_path) -> None:
    store = StateStore(tmp_path)
    store.save_state(
        {
            "providers": {
                "TRAKT": {
                    "watchlist": {"baseline": {"items": {"movie:1": {"title": "One"}}}},
                    "history": {"baseline": {"items": {"movie:2@1700000000": {"title": "Two", "watched_at": "2026-01-01T00:00:00Z"}}}},
                },
                "SIMKL": {
                    "watchlist": {"baseline": {"items": {"show:3": {"title": "Three", "type": "show"}}}},
                    "instances": {
                        "anime": {
                            "watchlist": {"baseline": {"items": {"show:3": {"title": "Three", "type": "show"}}}},
                        }
                    }
                },
            },
            "last_sync_epoch": 300,
        }
    )

    watchlist = store.load_state_features({"watchlist"})
    counts = store.provider_feature_counts("watchlist")

    assert "history" not in watchlist["providers"]["TRAKT"]
    assert set(watchlist["providers"]["TRAKT"]["watchlist"]["baseline"]["items"]) == {"movie:1"}
    assert set(watchlist["providers"]["SIMKL"]["instances"]["anime"]["watchlist"]["baseline"]["items"]) == {"show:3"}
    assert counts["TRAKT"] == 1
    assert counts["SIMKL"] == 1
    assert watchlist["last_sync_epoch"] == 300

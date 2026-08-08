# tests/test_activity_db.py
# CrossWatch - SQLite recent activity tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.local_db import close_conn
from services import activity


def test_recent_activity_round_trips_through_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(activity, "state_dir", lambda: tmp_path)
    close_conn()
    activity.clear_events()

    activity.add_event(
        {
            "kind": "scrobble",
            "method": "webhook",
            "event": "scrobble_stop",
            "status": "ok",
            "source": "plex",
            "target": "trakt",
            "media_type": "movie",
            "title": "Heat",
            "year": 1995,
            "progress": 92,
            "watched_at": 1767225600,
            "captured_at": 1767229200,
            "ids": {"tmdb": 949, "imdb": "tt0113277"},
        }
    )

    payload = activity.list_events(limit=10, group_routes=False)

    assert payload["total"] == 1
    assert payload["items"][0]["title"] == "Heat"
    assert payload["items"][0]["ids"] == {"tmdb": "949", "imdb": "tt0113277"}
    assert activity.activity_path().name == "crosswatch.sqlite3"
    assert not (tmp_path / "activity_history.json").exists()

    result = activity.clear_scrobble_events()

    assert result["removed"] == 1
    assert activity.list_events(group_routes=False)["total"] == 0
    close_conn()


def test_recent_activity_groups_route_targets_from_database(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(activity, "state_dir", lambda: tmp_path)
    close_conn()
    activity.clear_events()

    base = {
        "kind": "scrobble",
        "method": "watcher",
        "event": "scrobble_stop",
        "status": "ok",
        "source": "plex",
        "source_instance": "P01",
        "media_type": "movie",
        "title": "Arrival",
        "year": 2016,
        "progress": 100,
        "watched_at": 1767225600,
        "captured_at": 1767229200,
    }
    activity.add_event({**base, "target": "trakt", "target_instance": "default"})
    activity.add_event({**base, "target": "simkl", "target_instance": "anime"})

    payload = activity.list_events(limit=10, group_routes=True)

    assert payload["total"] == 1
    assert payload["items"][0]["targets"] == [
        {"target": "simkl", "target_instance": "anime"},
        {"target": "trakt", "target_instance": "default"},
    ]
    close_conn()

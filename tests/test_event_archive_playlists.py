from __future__ import annotations

from cw_platform.event_archive.db import connect
from cw_platform.event_archive.groups import group_events, list_groups
from cw_platform.event_archive.recorder import record_events


def test_playlist_item_events_group_by_mapping_batch() -> None:
    conn = connect(":memory:")

    record_events(
        [
            {
                "domain": "sync",
                "created_at": 1770000000,
                "run_id": "run-1",
                "event_type": "playlist_add",
                "severity": "info",
                "feature": "playlists",
                "operation": "add",
                "pair_key": "MAP-01",
                "direction": "one_way",
                "source_provider": "TRAKT",
                "source_instance": "default",
                "destination_provider": "PLEX",
                "destination_instance": "default",
                "item_key": key,
                "title": title,
                "media_type": "movie",
            }
            for key, title in (
                ("tmdb:1", "Avengers: Infinity War"),
                ("tmdb:2", "Guardians of the Galaxy"),
                ("tmdb:3", "Captain Marvel"),
            )
        ],
        conn=conn,
    )

    groups = list_groups(feature="playlists", operation="add", visibility="all", conn=conn)

    assert groups["total"] == 1
    thread = groups["items"][0]
    assert thread["event_count"] == 3
    assert thread["item_key"] is None
    assert thread["title"] is None
    assert thread["summary"] == "TRAKT -> PLEX playlist added, 3 items"

    events = group_events(thread["id"], conn=conn)["items"]
    assert [row["title"] for row in events] == [
        "Avengers: Infinity War",
        "Guardians of the Galaxy",
        "Captain Marvel",
    ]


def test_playlist_item_events_without_run_group_by_timestamp_batch() -> None:
    conn = connect(":memory:")

    record_events(
        [
            {
                "domain": "sync",
                "created_at": 1770000000,
                "event_type": "playlist_remove",
                "severity": "info",
                "feature": "playlists",
                "operation": "remove",
                "pair_key": "MAP-01",
                "source_provider": "TRAKT",
                "destination_provider": "PLEX",
                "item_key": key,
                "title": title,
            }
            for key, title in (("tmdb:1", "One"), ("tmdb:2", "Two"))
        ],
        conn=conn,
    )

    groups = list_groups(feature="playlists", operation="remove", visibility="all", conn=conn)

    assert groups["total"] == 1
    assert groups["items"][0]["summary"] == "TRAKT -> PLEX playlist removed, 2 items"

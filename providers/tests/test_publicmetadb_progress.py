from __future__ import annotations


def test_payload_for_item_derives_position_from_percent_and_duration() -> None:
    from providers.sync.publicmetadb import _progress

    payload, reason = _progress._payload_for_item(
        {
            "type": "movie",
            "ids": {"tmdb": "550"},
            "progress_percent": 25,
            "duration_ms": 600_000,
        }
    )

    assert reason is None
    assert payload is not None
    assert payload["position_ms"] == 150_000
    assert payload["runtime_ms"] == 600_000


def test_payload_for_item_reports_missing_duration_for_percent_only_media_server_target() -> None:
    from providers.sync.publicmetadb import _progress

    payload, reason = _progress._payload_for_item(
        {
            "type": "movie",
            "ids": {"tmdb": "550"},
            "progress_percent": 25,
        }
    )

    assert payload is None
    assert reason == "missing_duration"


def test_payload_for_episode_prefers_show_tmdb_when_ids_duplicate_show_id() -> None:
    from providers.sync.publicmetadb import _progress

    payload, reason = _progress._payload_for_item(
        {
            "type": "episode",
            "ids": {"tmdb": "94997"},
            "show_ids": {"tmdb": "94997"},
            "season": 2,
            "episode": 7,
            "progress_percent": 25,
            "duration_ms": 600_000,
        }
    )

    assert reason is None
    assert payload is not None
    assert payload["tmdb_id"] == 94997
    assert payload["media_type"] == "tv"
    assert payload["season"] == 2
    assert payload["episode"] == 7

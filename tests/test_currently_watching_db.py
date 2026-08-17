from __future__ import annotations

import json
from typing import Any

import pytest

from cw_platform.local_db import close_conn
from providers.scrobble import currently_watching


def _loads_body(body: bytes | memoryview[int]) -> Any:
    return json.loads(bytes(body))


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    close_conn()
    currently_watching.clear_state()
    yield
    close_conn()


def test_currently_watching_round_trips_through_db_and_api(isolated_db) -> None:
    currently_watching.update_from_payload(
        "plex",
        "movie",
        "Heat",
        1995,
        None,
        None,
        44,
        False,
        duration_ms=10_200,
        cover="/art/heat",
        state="playing",
        ids={"tmdb": 949},
        session_key="session-1",
        provider_instance="P01",
    )

    state = currently_watching.load_state()
    stream = state["streams"]["plex:P01:session-1"]

    assert stream["title"] == "Heat"
    assert stream["provider_instance"] == "P01"
    assert stream["ids"] == {"tmdb": "949"}

    from api.scrobbleAPI import api_currently_watching

    response = api_currently_watching()
    payload = _loads_body(response.body)

    assert payload["streams_count"] == 1
    assert payload["currently_watching"]["_key"] == "plex:P01:session-1"
    assert payload["currently_watching"]["title"] == "Heat"


def test_currently_watching_stop_removes_stream(isolated_db) -> None:
    currently_watching.update_from_payload(
        "jellyfin",
        "episode",
        "The Bear",
        2025,
        4,
        3,
        12,
        False,
        state="playing",
        ids={"tmdb_show": 136315},
        session_key="jf-1",
    )
    currently_watching.update_from_payload(
        "jellyfin",
        "episode",
        "The Bear",
        2025,
        4,
        3,
        91,
        True,
        state="stopped",
        ids={"tmdb_show": 136315},
        session_key="jf-1",
    )

    assert currently_watching.load_state()["streams"] == {}


def test_currently_watching_preserves_stable_fields(isolated_db) -> None:
    currently_watching.update_from_payload(
        "emby",
        "movie",
        "Arrival",
        2016,
        None,
        None,
        7,
        False,
        duration_ms=6_000_000,
        cover="/art/arrival",
        state="playing",
        ids={"tmdb": 329865},
        session_key="emby-1",
    )
    currently_watching.update_from_payload(
        "emby",
        "movie",
        "Arrival",
        2016,
        None,
        None,
        16,
        False,
        state="paused",
        ids={"tmdb": 329865},
        session_key="emby-1",
    )

    stream = currently_watching.load_state()["streams"]["emby:emby-1"]

    assert stream["duration_ms"] == 6_000_000
    assert stream["cover"] == "/art/arrival"
    assert stream["state"] == "paused"


def test_playback_progress_reads_currently_watching_db(isolated_db) -> None:
    currently_watching.update_from_payload(
        "plex",
        "movie",
        "Pressure",
        2026,
        None,
        None,
        18,
        False,
        state="playing",
        ids={"tmdb": 123},
        provider_instance="P01",
        session_key="plex-2",
    )

    from services.playback_progress.service import _load_live_streams

    streams = _load_live_streams()

    assert len(streams) == 1
    assert streams[0]["_live_provider"] == "plex"
    assert streams[0]["_live_instance_id"] == "P01"

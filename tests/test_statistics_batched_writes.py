from __future__ import annotations

from pathlib import Path

from cw_platform.local_db import statistics


def _payload(n: int) -> dict:
    data = statistics.default_statistics()
    data["current"] = {
        f"tmdb:{i}": {"src": "PLEX", "title": f"Movie {i}", "type": "movie", "providers": ["plex", "trakt"]}
        for i in range(n)
    }
    data["current_by_feature"] = {
        "history": {
            f"tmdb:{i}:ep": {"src": "TRAKT", "title": f"Episode {i}", "type": "episode", "providers": ["trakt"]}
            for i in range(n)
        }
    }
    return data


def _round_trip(base: Path, n: int) -> dict:
    statistics.save_statistics(base, _payload(n))
    return statistics.load_statistics(base)


def test_writes_spanning_several_batches_round_trip(tmp_path: Path):
    n = statistics._WRITE_BATCH * 2 + 7
    loaded = _round_trip(tmp_path, n)
    assert len(loaded["current"]) == n
    assert len(loaded["current_by_feature"]["history"]) == n
    assert loaded["current"]["tmdb:0"]["title"] == "Movie 0"
    assert loaded["current"][f"tmdb:{n - 1}"]["type"] == "movie"
    assert set(loaded["current"]["tmdb:5"].get("providers") or []) == {"plex", "trakt"}


def test_a_single_batch_round_trips(tmp_path: Path):
    loaded = _round_trip(tmp_path, 3)
    assert len(loaded["current"]) == 3
    assert loaded["current_by_feature"]["history"]["tmdb:1:ep"]["type"] == "episode"


def test_an_empty_payload_round_trips(tmp_path: Path):
    loaded = _round_trip(tmp_path, 0)
    assert loaded["current"] == {}
    assert loaded["current_by_feature"].get("history", {}) == {}


def test_a_later_save_replaces_the_previous_rows(tmp_path: Path):
    _round_trip(tmp_path, statistics._WRITE_BATCH + 5)
    loaded = _round_trip(tmp_path, 2)
    assert len(loaded["current"]) == 2
    assert len(loaded["current_by_feature"]["history"]) == 2

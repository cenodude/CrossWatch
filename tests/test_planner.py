from __future__ import annotations

from cw_platform.orchestrator._planner import diff, diff_ratings


def test_diff_adds_and_removes_minimally() -> None:
    src = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}},
        "imdb:tt02": {"type": "movie", "title": "B", "year": 2001, "ids": {"imdb": "tt02"}},
    }
    dst = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}},
        "imdb:tt03": {"type": "movie", "title": "C", "year": 2002, "ids": {"imdb": "tt03"}},
    }

    adds, removes = diff(src, dst)
    assert [it.get("ids", {}).get("imdb") for it in adds] == ["tt02"]
    assert [it.get("ids", {}).get("imdb") for it in removes] == ["tt03"]
    assert set(adds[0].keys()) >= {"type", "title", "year", "ids"}


def test_diff_ratings_upserts_and_unrates() -> None:
    src = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}, "rating": 7},
        "imdb:tt02": {"type": "movie", "title": "B", "year": 2001, "ids": {"imdb": "tt02"}, "rating": 8},
    }
    dst = {
        "imdb:tt01": {"type": "movie", "title": "A", "year": 2000, "ids": {"imdb": "tt01"}, "rating": 6},
        "imdb:tt03": {"type": "movie", "title": "C", "year": 2002, "ids": {"imdb": "tt03"}, "rating": 9},
    }

    upserts, unrates = diff_ratings(src, dst)
    assert {it.get("ids", {}).get("imdb") for it in upserts} == {"tt01", "tt02"}
    assert [it.get("ids", {}).get("imdb") for it in unrates] == ["tt03"]


def test_diff_ratings_timestamp_propagation() -> None:
    src = {
        "imdb:tt01": {
            "type": "movie",
            "title": "A",
            "year": 2000,
            "ids": {"imdb": "tt01"},
            "rating": 7,
            "rated_at": "2024-06-02T12:00:00Z",
        },
    }
    dst = {
        "imdb:tt01": {
            "type": "movie",
            "title": "A",
            "year": 2000,
            "ids": {"imdb": "tt01"},
            "rating": 7,
            "rated_at": "2024-06-01T12:00:00Z",
        },
    }

    upserts, unrates = diff_ratings(src, dst, propagate_timestamp_updates=True)
    assert len(upserts) == 1
    assert unrates == []
    assert upserts[0]["rated_at"].startswith("2024-06-02")

# CrossWatch test scripts
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


def test_diff_matches_episode_level_ids_when_coordinates_differ() -> None:
    src = {
        "tmdb:37854#s23e01": {
            "type": "episode",
            "series_title": "One Piece",
            "season": 23,
            "episode": 1,
            "ids": {"tvdb": "11526346"},
            "show_ids": {"tmdb": "37854", "tvdb": "81797"},
        }
    }
    dst = {
        "tmdb:37854#s23e1156": {
            "type": "episode",
            "series_title": "One Piece",
            "season": 23,
            "episode": 1156,
            "ids": {"tmdb": "7099881", "tvdb": "11526346"},
            "show_ids": {"tmdb": "37854", "tvdb": "81797"},
        }
    }

    assert diff(src, dst) == ([], [])


def test_diff_does_not_match_inherited_show_ids_as_episode_ids() -> None:
    src = {
        "tmdb:37854#s23e01": {
            "type": "episode",
            "season": 23,
            "episode": 1,
            "ids": {"tmdb": "37854"},
            "show_ids": {"tmdb": "37854"},
        }
    }
    dst = {
        "tmdb:37854#s23e02": {
            "type": "episode",
            "season": 23,
            "episode": 2,
            "ids": {"tmdb": "37854"},
            "show_ids": {"tmdb": "37854"},
        }
    }

    adds, removes = diff(src, dst)
    assert len(adds) == 1
    assert len(removes) == 1


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

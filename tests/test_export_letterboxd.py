# CrossWatch test scripts
from __future__ import annotations

import pytest

import services.export as export_service


def _movie(rating: object) -> tuple[str, dict]:
    return (
        "tmdb:101",
        {
            "type": "movie",
            "title": "Leon: The Professional",
            "year": 1994,
            "ids": {"imdb": "tt0110413", "tmdb": "101"},
            "rating": rating,
            "watched_at": "2026-02-08T20:00:00Z",
        },
    )


@pytest.mark.parametrize(
    ("rating", "expected"),
    [("9.0", "9"), (10, "10"), (7.5, "8"), (8.5, "9"), (0.4, "1"), (12, "10"), (None, ""), ("bad", ""), (0, "")],
)
def test_letterboxd_ratings_use_integer_rating10(rating: object, expected: str) -> None:
    header, rows = export_service._letterboxd_rows("ratings", [_movie(rating)])

    assert header == ["imdbID", "tmdbID", "Title", "Year", "Rating10"]
    assert rows[0][4] == expected


def test_letterboxd_combined_uses_rating10_with_watched_date() -> None:
    header, rows = export_service._letterboxd_rows("combined", [_movie("9.0")])

    assert header == ["imdbID", "tmdbID", "Title", "Year", "Rating10", "WatchedDate"]
    assert rows == [["tt0110413", "101", "Leon: The Professional", "1994", "9", "2026-02-08"]]

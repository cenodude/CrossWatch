# tests/test_movie_matching.py
# CrossWatch - Diagnosis for movies that do not match between providers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import pytest
from fastapi import HTTPException

from services.movie_matching import diagnose


def film(title="Insidious: Out of the Further", year=2026, **ids):
    return dict(type="movie", title=title, year=year, ids=ids)


def run(item, there=(), found="unsupported", results=None, tmdb=None):
    row = dict(item=item, provider="SIMKL", instance="default")
    def search(cfg, row, query, *, catalog):
        rows = results if catalog == "destination" else tmdb
        if rows is None:
            raise HTTPException(409, "no catalog")
        return dict(results=rows)
    return diagnose({}, row, list(there), by_id=lambda *a: found, search=search)


def test_a_synced_film_with_other_ids_is_recognised_as_the_same_film():
    result = run(film(imdb="tt1"), there=[film(tmdb="5"), film("Other", 2026, tmdb="6")])
    assert result["status"] == "twin" and result["candidate"]["ids"] == {"tmdb": "5"}
    assert "very likely the same film" in result["message"]


def test_a_twin_with_conflicting_ids_asks_the_user_to_check():
    result = run(film(imdb="tt1", tmdb="4"), there=[film(year=2027, tmdb="5")])
    assert result["status"] == "twin" and "IDs disagree" in result["message"]


def test_films_that_already_share_an_id_or_differ_in_year_are_not_twins():
    result = run(film(imdb="tt1"), there=[film(imdb="tt1"), film(year=2011, tmdb="9")], found=None, results=[])
    assert result["status"] == "not_found"


@pytest.mark.parametrize("theirs,status", [({"imdb": "tt1", "tmdb": "5", "simkl": "7"}, "known"),
                                           ({"imdb": "tt1", "simkl": "7"}, "recognised"), ({"imdb": "tt2"}, "conflict")])
def test_catalogue_lookup_by_id_explains_what_the_destination_knows(theirs, status):
    result = run(film(imdb="tt1"), found=film(**theirs))
    assert result["status"] == status
    if status == "known":
        assert result["candidate"]["ids"] == {"imdb": "tt1", "tmdb": "5"}
    if status == "conflict":
        assert result["candidate"]["ids"] == {"imdb": "tt2"} and "different IMDB ID" in result["message"]
    if status == "recognised":
        assert result["candidate"] is None and "not an ID problem" in result["message"]


def test_ids_that_resolve_to_another_title_are_flagged():
    result = run(film(imdb="tt1"), found=film("Completely Different", 1999, imdb="tt1"))
    assert result["status"] == "different" and "wrong ID" in result["message"]


def test_title_search_finds_one_candidate_or_reports_ambiguity_or_nothing():
    hit = dict(type="movie", title="Insidious: Out of the Further", year=2026, ids={"tmdb": "5"})
    assert run(film(imdb="tt1"), found=None, results=[hit, dict(hit, title="Insidious", year=2011)])["status"] == "candidate"
    assert run(film(imdb="tt1"), found=None, results=[hit, dict(hit, ids={"tmdb": "6"})])["status"] == "ambiguous"
    assert run(film(imdb="tt1"), found=None, results=[])["status"] == "not_found"
    assert run(film(imdb="tt1"), found=None, results=[dict(hit, mapping_unavailable="x")])["status"] == "not_found"


def test_tmdb_is_searched_when_the_destination_has_no_catalogue():
    hit = dict(type="movie", title="Insidious: Out of the Further", year=2026, ids={"tmdb": "5"})
    result = run(film(imdb="tt1"), tmdb=[hit])
    assert result["status"] == "candidate" and result["checked"][-1] == "TMDb search by title"
    assert run(film(imdb="tt1"))["status"] == "unknown"


def test_only_movies_are_diagnosed():
    with pytest.raises(HTTPException):
        run(dict(type="episode", title="x", ids={"tmdb": "1"}))

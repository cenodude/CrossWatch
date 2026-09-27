# tests/test_export_trakt.py
# CrossWatch - Trakt export compatibility and selection tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import csv
import io
import zipfile

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

import services.export as exporter


def movie(imdb: str, **fields) -> dict:
    return {"type": "movie", "title": imdb, "ids": {"imdb": imdb}, **fields}


@pytest.fixture
def trakt_export(monkeypatch):
    history = {
        "imdb:tt0068646@1732564800": movie("tt0068646", watched_at="2024-11-25T20:00:00Z"),
        "imdb:tt0068646@1729886400": movie("tt0068646", watched_at="2024-10-25T22:00:00+02:00"),
        "imdb:tt0133093": movie("tt0133093"),
    }
    ratings = {
        "imdb:tt0068646": movie("tt0068646", rating=7, rated_at="2024-10-26T21:00:00Z"),
        "imdb:tt15239678": movie("tt15239678", rating=9, rated_at="2024-10-25T21:00:00Z"),
        "imdb:tt4281724": movie("tt4281724", rating=6, rated_at="2024-01-12T02:00:00Z"),
    }
    watchlist = {
        "imdb:tt0068646": movie("tt0068646", listed_at="2024-10-01T10:00:00Z"),
        "imdb:tt15239678": movie("tt15239678", watchlisted_at="2024-04-30T11:00:00Z"),
    }
    block = {name: {"baseline": {"items": items}} for name, items in
             (("history", history), ("ratings", ratings), ("watchlist", watchlist))}
    state = {"providers": {"CROSSWATCH": block}}
    monkeypatch.setattr(exporter, "_load_state", lambda _features=None: state)
    monkeypatch.setattr(exporter, "_load_config_safe", lambda: {})
    monkeypatch.setattr(exporter, "_provider_rewatch_read_supported", lambda _provider: True)
    app = FastAPI()
    app.include_router(exporter.router)
    return TestClient(app), block


def download(client, feature="combined", **kwargs):
    return client.post("/api/export/file", json={
        "provider": "CROSSWATCH", "format": "trakt", "feature": feature, **kwargs,
    })


def read_csv(content):
    return list(csv.DictReader(io.StringIO(content.decode("utf-8"))))


def test_trakt_options_advertise_supported_media(trakt_export):
    client, _ = trakt_export
    options = client.get("/api/export/options").json()
    assert options["labels"]["trakt"] == "Trakt CSV"
    assert options["capabilities"]["trakt"]["media_types"] == ["episode", "movie", "season", "show"]
    assert all("trakt" in options["formats"][feature] for feature in ("watchlist", "history", "ratings", "combined"))


def test_rewatches_are_separate_ordered_csvs_with_instructions(trakt_export):
    client, _ = trakt_export
    response = download(client)
    assert response.status_code == 200
    assert response.headers["content-type"] == "application/zip"
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert archive.namelist() == ["01_trakt_combined.csv", "02_trakt_combined.csv", "README.txt"]
        passes = [read_csv(archive.read(name)) for name in archive.namelist() if name.endswith(".csv")]
        instructions = archive.read("README.txt").decode()
    assert all(len(rows) == len({r["imdb_id"] for r in rows}) for rows in passes)
    assert [next(r["watched_at"] for r in rows if r["imdb_id"] == "tt0068646") for rows in passes] == [
        "2024-10-25T20:00:00Z", "2024-11-25T20:00:00Z",
    ]
    rows = [r for batch in passes for r in batch]
    assert len(rows) == 5
    assert next(r for r in rows if r["imdb_id"] == "tt0133093")["watched_at"] == "unknown"
    for imdb, rating, date in (("tt0068646", "7", "2024-10-26T21:00:00Z"),
                               ("tt15239678", "9", "2024-10-25T21:00:00Z"),
                               ("tt4281724", "6", "2024-01-12T02:00:00Z")):
        rated = [r for r in rows if r["imdb_id"] == imdb and r["rating"]]
        assert len(rated) == 1
        assert (rated[0]["rating"], rated[0]["rated_at"]) == (rating, date)
    assert all(not r["watched_at"] for r in rows if r["imdb_id"] in {"tt15239678", "tt4281724"})
    assert "one at a time" in instructions
    assert "watchlist last" in instructions.lower()
    assert "01_trakt_combined.csv" in instructions


@pytest.mark.parametrize("feature", ["history", "ratings", "watchlist", "combined"])
def test_single_selection_stays_csv_and_get_matches_post(trakt_export, feature):
    client, block = trakt_export
    key = next(iter(block["history" if feature == "combined" else feature]["baseline"]["items"]))
    response = download(client, feature, mode="selected", row_ids=[key])
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/csv")
    assert len(read_csv(response.content)) == 1
    via_get = client.get("/api/export/file", params={"provider": "CROSSWATCH", "format": "trakt", "feature": feature, "ids": key})
    assert via_get.content == response.content


def test_empty_or_excluded_selection_never_exports_all(trakt_export):
    client, block = trakt_export
    for selection in ({"mode": "selected", "row_ids": []},
                      {"mode": "selected", "row_ids": ["missing"]},
                      {"excluded_row_ids": list(block["history"]["baseline"]["items"])}):
        response = download(client, "history", **selection)
        assert response.status_code == 200
        assert read_csv(response.content) == []


def test_preview_and_file_share_strict_validation(trakt_export):
    client, block = trakt_export
    block["history"]["baseline"]["items"].update({
        "title-only": {"type": "movie", "title": "No ID"},
        "wrong-id": movie("garbage1234567"),
        "tmdb-only": {"type": "movie", "ids": {"tmdb": "550"}},
        "show": movie("tt0903747", type="show"),
        "bad-date": movie("tt4281724", watched_at="not-a-date"),
    })
    preview = client.get("/api/export/sample", params={"provider": "CROSSWATCH", "format": "trakt", "feature": "history", "media_types": "movie,show"}).json()
    assert preview["matched_total"] == 8
    assert preview["total"] == 5
    assert preview["dropped_total"] == 3
    assert preview["validation"]["missing_identity_total"] == 2
    assert preview["validation"]["invalid_data_total"] == 1
    assert "IMDb" in " ".join(preview["warnings"])
    assert "ZIP" in " ".join(preview["warnings"])
    response = download(client, "history", media_types="movie,show")
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        assert sum(len(read_csv(archive.read(n))) for n in archive.namelist() if n.endswith(".csv")) == preview["total"]


def test_watchlist_dates_are_preserved_in_file_with_importer_warning(trakt_export):
    client, _ = trakt_export
    rows = read_csv(download(client, "watchlist").content)
    assert [r["watchlisted_at"] for r in rows] == ["2024-10-01T10:00:00Z", "2024-04-30T11:00:00Z"]
    assert all(not r["watched_at"] and not r["rating"] for r in rows)
    preview = client.get("/api/export/sample", params={"provider": "CROSSWATCH", "format": "trakt", "feature": "watchlist"}).json()
    assert "import time" in " ".join(preview["warnings"])


def test_missing_dates_are_explicit_and_rating_dates_never_become_watches(trakt_export):
    client, block = trakt_export
    block["watchlist"]["baseline"]["items"] = {"missing": movie("tt0068646")}
    row = read_csv(download(client, "watchlist").content)[0]
    assert row["watchlisted_at"].endswith("Z")
    assert not row["watched_at"]
    preview = client.get("/api/export/sample", params={"provider": "CROSSWATCH", "format": "trakt", "feature": "ratings"}).json()
    assert all(not row["watched_at"] for row in preview["items"])


@pytest.mark.parametrize("rating", [0, 11, -1, 7.5, "nan", "inf", "bad", True])
def test_invalid_ratings_are_not_silently_truncated_or_clamped(trakt_export, rating):
    client, block = trakt_export
    block["ratings"]["baseline"]["items"] = {"invalid": movie("tt0068646", rating=rating)}
    assert read_csv(download(client, "ratings").content) == []


def test_disabling_rewatches_exports_only_the_latest_watch(trakt_export):
    client, _ = trakt_export
    rows = read_csv(download(client, "history", include_rewatches=False).content)
    assert len(rows) == 2
    assert next(r for r in rows if r["imdb_id"] == "tt0068646")["watched_at"] == "2024-11-25T20:00:00Z"


def test_instance_scope_and_search_are_respected(trakt_export, monkeypatch):
    client, block = trakt_export
    block["instances"] = {"second": {"history": {"baseline": {"items": {"second": movie("tt4281724")}}}}}
    rows = read_csv(download(client, "history", provider_instance="second").content)
    assert [r["imdb_id"] for r in rows] == ["tt4281724"]
    assert read_csv(download(client, "history", q="no match").content) == []
    monkeypatch.setattr(exporter, "request_user", lambda _request: {"is_admin": False})
    monkeypatch.setattr(exporter, "managed_profile_instances", lambda *_args: {"CROSSWATCH": ["default"]})
    monkeypatch.setattr(exporter, "user_can_access_instance", lambda *_args: False)
    assert download(client, "history", provider_instance="second").status_code == 403


def test_history_without_date_stays_unknown_when_merged_with_rating(trakt_export):
    client, block = trakt_export
    block["ratings"]["baseline"]["items"]["imdb:tt0133093"] = movie(
        "tt0133093", rating=8, rated_at="2024-01-02", watched_at="2024-01-03",
    )
    response = download(client, mode="selected", row_ids=["imdb:tt0133093"])
    row = read_csv(response.content)[0]
    assert row["watched_at"] == "unknown"
    assert row["rated_at"] == "2024-01-02T00:00:00Z"


def test_rating_only_item_with_incidental_watch_metadata_stays_rating_only(trakt_export):
    client, block = trakt_export
    block["ratings"]["baseline"]["items"]["imdb:tt4281724"]["watched_at"] = "2024-01-01"
    row = read_csv(download(client, mode="selected", row_ids=["imdb:tt4281724"]).content)[0]
    assert row["rating"] == "6"
    assert not row["watched_at"]


def test_duplicate_rating_identity_never_becomes_an_empty_action_row(trakt_export):
    client, block = trakt_export
    block["ratings"]["baseline"]["items"]["alternate-key"] = movie("tt4281724", rating=6)
    response = download(client, "ratings")
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        rows = [r for n in archive.namelist() if n.endswith(".csv") for r in read_csv(archive.read(n))]
    assert all(row["rating"] and not row["watched_at"] for row in rows)


@pytest.mark.parametrize("feature,field", [("history", "watched_at"), ("watchlist", "listed_at"), ("ratings", "rated_at")])
def test_invalid_calendar_dates_are_reported_and_skipped(trakt_export, feature, field):
    client, block = trakt_export
    block[feature]["baseline"]["items"] = {"bad": movie("tt0068646", rating=7, **{field: "2024-02-30T12:00:00Z"})}
    preview = client.get("/api/export/sample", params={"provider": "CROSSWATCH", "format": "trakt", "feature": feature}).json()
    assert preview["total"] == 0
    assert preview["dropped_total"] == 1
    assert read_csv(download(client, feature).content) == []


def test_zip_selection_deduplicates_requested_keys_and_respects_exclusions(trakt_export):
    client, block = trakt_export
    keys = list(block["history"]["baseline"]["items"])
    response = download(client, "history", mode="selected", row_ids=keys + keys, excluded_row_ids=["imdb:tt0133093"])
    with zipfile.ZipFile(io.BytesIO(response.content)) as archive:
        rows = [r for n in archive.namelist() if n.endswith(".csv") for r in read_csv(archive.read(n))]
    assert len(rows) == 2
    assert all(r["imdb_id"] == "tt0068646" for r in rows)


@pytest.mark.parametrize("media_type", ["movie", "show", "season", "episode"])
@pytest.mark.parametrize("namespace,value", [("tmdb", 550), ("imdb", "tt0137523"), ("tvdb", 12345), ("trakt", 1391)])
@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings", "combined"])
def test_each_identifier_can_be_used_without_imdb(trakt_export, media_type, namespace, value, feature):
    client, block = trakt_export
    for bucket in ("history", "ratings", "watchlist"):
        block[bucket]["baseline"]["items"] = {}
    source = "history" if feature == "combined" else feature
    block[source]["baseline"]["items"] = {
        "entry": {"type": media_type, "ids": {namespace: value}, "rating": 8, "watched_at": "2024-01-01"},
    }
    rows = read_csv(download(client, feature, media_types=media_type).content)
    if (media_type, namespace) in {("movie", "tvdb"), ("season", "imdb")}:
        assert rows == []
    else:
        assert len(rows) == 1
        assert rows[0][f"{namespace}_id"] == str(value)
        assert rows[0]["type"] == media_type
        assert all(rows[0][f"{key}_id"] == "" for key in ("tmdb", "imdb", "tvdb", "trakt") if key != namespace)


def test_csv_preserves_all_available_ids(trakt_export):
    client, block = trakt_export
    ids = {"tmdb": "00550", "imdb": "tt0137523", "tvdb": 12345, "trakt": 1391, "plex": "ignored"}
    block["history"]["baseline"]["items"] = {"entry": movie("", ids=ids)}
    row = read_csv(download(client, "history").content)[0]
    assert list(row)[:4] == ["tmdb_id", "imdb_id", "tvdb_id", "trakt_id"]
    assert {key: row[f"{key}_id"] for key in ("tmdb", "imdb", "tvdb", "trakt")} == {
        "tmdb": "550", "imdb": "tt0137523", "tvdb": "12345", "trakt": "1391",
    }


@pytest.mark.parametrize("invalid", [True, -1, 0, 12.5, "12junk", "nan", "-12", "1e2"])
def test_invalid_numeric_ids_fall_back_without_losing_valid_ids(trakt_export, invalid):
    client, block = trakt_export
    block["history"]["baseline"]["items"] = {"entry": movie("", ids={"tmdb": invalid, "trakt": 1391})}
    row = read_csv(download(client, "history").content)[0]
    assert row["tmdb_id"] == ""
    assert row["trakt_id"] == "1391"


@pytest.mark.parametrize("media_type", ["season", "episode"])
def test_parent_show_ids_are_not_exported_as_child_ids(trakt_export, media_type):
    client, block = trakt_export
    block["history"]["baseline"]["items"] = {"entry": {
        "type": media_type, "ids": {"tmdb": "00123", "trakt": 456},
        "show_ids": {"tmdb": 123, "trakt": 789}, "season": 1, "episode": 2,
    }}
    row = read_csv(download(client, "history", media_types=media_type).content)[0]
    assert row["tmdb_id"] == ""
    assert row["trakt_id"] == "456"
    assert "season" not in row and "episode" not in row


def test_ambiguous_positional_episode_ids_are_skipped_like_adapter(trakt_export):
    client, block = trakt_export
    block["history"]["baseline"]["items"] = {"entry": {
        "type": "episode", "ids": {"tmdb": 123}, "season": 1, "episode": 2,
    }}
    assert read_csv(download(client, "history", media_types="episode").content) == []


def test_rewatch_grouping_links_transitive_ids_and_keeps_media_scopes_separate(trakt_export):
    client, block = trakt_export
    block["history"]["baseline"]["items"] = {
        "first": movie("", ids={"tmdb": 550}, watched_at="2024-01-01"),
        "second": movie("tt0137523", watched_at="2024-02-01"),
        "bridge": movie("tt0137523", ids={"tmdb": 550, "imdb": "tt0137523", "trakt": 1391}, watched_at="2024-03-01"),
        "last": movie("", ids={"trakt": 1391}, watched_at="2024-04-01"),
        "show": {"type": "show", "ids": {"tmdb": 550}, "watched_at": "2024-01-01"},
    }
    preview = client.get("/api/export/sample", params={"provider": "CROSSWATCH", "format": "trakt", "feature": "history", "media_types": "movie,show"}).json()
    assert "ZIP with 4 CSV files" in " ".join(preview["warnings"])
    with zipfile.ZipFile(io.BytesIO(download(client, "history", media_types="movie,show").content)) as archive:
        batches = [read_csv(archive.read(name)) for name in archive.namelist() if name.endswith(".csv")]
    assert [len(batch) for batch in batches] == [2, 1, 1, 1]
    assert [next(row["watched_at"] for row in batch if row["type"] == "movie") for batch in batches] == [
        f"2024-0{month}-01T00:00:00Z" for month in range(1, 5)
    ]

# CrossWatch test scripts
from __future__ import annotations

import io
import zipfile
from pathlib import Path
from typing import Any

import pytest


def test_importer_rejects_zip_member_size() -> None:
    import services.importer as importer

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("watched.csv", b"x" * (importer.MAX_ZIP_MEMBER_BYTES + 1))

    with pytest.raises(Exception) as exc:
        importer._safe_zip_members(buf.getvalue())

    assert getattr(exc.value, "detail", {}).get("code") == importer.ERROR_ZIP_TOO_LARGE


def test_importer_accepts_large_paginated_trakt_zip() -> None:
    import services.importer as importer

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for index in range(127):
            zf.writestr(f"watched-history-{index + 1}.json", b"[]")

    members = importer._safe_zip_members(buf.getvalue())

    assert len(members) == 127


def test_importer_still_rejects_excessive_zip_members() -> None:
    import services.importer as importer

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for index in range(importer.MAX_ZIP_FILES + 1):
            zf.writestr(f"watched-history-{index + 1}.json", b"[]")

    with pytest.raises(Exception) as exc:
        importer._safe_zip_members(buf.getvalue())

    assert getattr(exc.value, "detail", {}).get("code") == importer.ERROR_ZIP_TOO_MANY_FILES


def test_letterboxd_preview_shapes_selectable_rows(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(importer, "_existing_keys", lambda _cfg, _inst: {f: set() for f in importer.FEATURES})

    raw = importer._parse_files(
        "letterboxd",
        [
            (
                "watched.csv",
                b"Date,Name,Year,Letterboxd URI\n2026-01-02,Fight Club,1999,https://letterboxd.com/film/fight-club/\n",
            ),
            (
                "ratings.csv",
                b"Date,Name,Year,Letterboxd URI,Rating\n2026-01-03,Fight Club,1999,https://letterboxd.com/film/fight-club/,4.5\n",
            ),
        ],
    )
    rows = importer._shape_rows(raw, {"crosswatch": {"connected": True}}, "default", "letterboxd")

    assert [row["feature"] for row in rows] == ["history", "ratings"]
    assert all(row["status"] == "ready" for row in rows)
    assert rows[0]["item"]["watched_at"] == "2026-01-02T12:00:00Z"
    assert rows[1]["item"]["rating"] == 9.0


def test_trakt_nested_json_rows_are_normalized(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(importer, "_existing_keys", lambda _cfg, _inst: {f: set() for f in importer.FEATURES})

    raw = importer._parse_files(
        "trakt",
        [
            (
                "history.json",
                b"""[
                  {"watched_at":"2026-02-01T00:00:00Z","movie":{"title":"Heat","year":1995,"ids":{"tmdb":949}}},
                  {"watched_at":"2026-02-02T00:00:00Z","episode":{"season":1,"number":2,"title":"Two"},"show":{"title":"Show","ids":{"tmdb":22}}}
                ]""",
            )
        ],
    )
    rows = importer._shape_rows(raw, {"crosswatch": {"connected": True}}, "default", "trakt")

    assert rows[0]["key"] == "tmdb:949@1769904000"
    assert rows[1]["media_type"] == "episode"
    assert rows[1]["item"]["show_ids"] == {"tmdb": "22"}
    assert rows[1]["item"]["season"] == 1
    assert rows[1]["item"]["episode"] == 2


def test_trakt_issue_export_watched_history_preserves_event_id() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "trakt",
        [
            (
                "watched-history-1.json",
                b"""[
                  {
                    "id": 587475858,
                    "watched_at": "2014-04-05T15:27:00.000Z",
                    "action": "scrobble",
                    "type": "movie",
                    "movie": {
                      "ids": {
                        "imdb": "tt0468569",
                        "plex": {"guid": "plex://movie/5d77683f54f42c001f8c049a", "slug": "the-dark-knight"},
                        "slug": "the-dark-knight-2008",
                        "tmdb": 155,
                        "trakt": 120
                      },
                      "year": 2008,
                      "title": "The Dark Knight"
                    }
                  }
                ]""",
            )
        ],
    )

    assert len(raw) == 1
    item = raw[0]["item"]
    assert raw[0]["feature"] == "history"
    assert item["type"] == "movie"
    assert item["title"] == "The Dark Knight"
    assert item["year"] == 2008
    assert item["ids"]["tmdb"] == "155"
    assert item["ids"]["imdb"] == "tt0468569"
    assert item["ids"]["trakt"] == "120"
    assert item["watched_at"] == "2014-04-05T15:27:00Z"
    assert item["_trakt_history_id"] == "587475858"


def test_trakt_episode_rating_uses_show_title_not_raw_show_object() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "trakt",
        [
            (
                "ratings-episodes-1.json",
                b"""[
                  {
                    "rated_at": "2026-01-03T20:00:00Z",
                    "rating": 8,
                    "type": "episode",
                    "show": {
                      "title": "The Pitt",
                      "year": 2025,
                      "ids": {"trakt": 232884, "slug": "the-pitt", "tvdb": 448176, "imdb": "tt31938062", "tmdb": 250307}
                    },
                    "episode": {"season": 1, "number": 2, "title": "1:00 P.M.", "ids": {"trakt": 12345}}
                  }
                ]""",
            )
        ],
    )

    assert len(raw) == 1
    item = raw[0]["item"]
    assert item["title"] == "1:00 P.M."
    assert item["series_title"] == "The Pitt"


def test_trakt_episode_history_preview_title_and_ids_are_episode_specific(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(importer, "_existing_keys", lambda _cfg, _inst: {f: {} for f in importer.FEATURES})

    raw = importer._parse_files(
        "trakt",
        [
            (
                "watched-history-1.json",
                b"""[
                  {
                    "id": 12093947887,
                    "watched_at": "2026-02-12T23:16:00.000Z",
                    "type": "episode",
                    "episode": {
                      "ids": {"imdb": "tt15242998", "tmdb": 5469142, "tvdb": 10592768, "trakt": 12103036},
                      "title": "Cold Harbor",
                      "number": 10,
                      "season": 2
                    },
                    "show": {
                      "ids": {"imdb": "tt11280740", "slug": "severance", "tmdb": 95396, "tvdb": 371980, "trakt": 154997},
                      "year": 2022,
                      "title": "Severance"
                    }
                  }
                ]""",
            )
        ],
    )
    rows = importer._shape_rows(raw, {}, "default", "trakt")
    public = importer._public_rows(rows, 100, 0)

    assert rows[0]["title"] == "Severance - S02E10 - Cold Harbor"
    assert rows[0]["key"].startswith("tmdb:95396#s02e10@")
    assert public[0]["ids"] == {"tmdb": "5469142", "imdb": "tt15242998", "tvdb": "10592768", "trakt": "12103036"}
    assert public[0]["show_ids"]["tmdb"] == "95396"


def test_existing_same_rating_is_exists_but_changed_rating_is_ready(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(
        importer,
        "_existing_keys",
        lambda _cfg, _inst: {
            "history": set(),
            "watchlist": set(),
            "ratings": {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}, "rating": 6}},
        },
    )

    rows = importer._shape_rows(
        [
            {"feature": "ratings", "item": {"type": "movie", "title": "Same", "ids": {"tmdb": "1"}, "rating": 6}},
            {"feature": "ratings", "item": {"type": "movie", "title": "Changed", "ids": {"tmdb": "2"}, "rating": 9}},
        ],
        {},
        "default",
        "trakt",
    )

    assert rows[0]["status"] == "exists"
    assert rows[0]["reason"] == "already_exists"
    assert rows[0]["importable"] is True
    assert rows[0]["default_included"] is False
    assert rows[1]["status"] == "ready"
    assert rows[1]["reason"] == ""


def test_existing_changed_rating_is_ready_update(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(
        importer,
        "_existing_keys",
        lambda _cfg, _inst: {
            "history": set(),
            "watchlist": set(),
            "ratings": {"tmdb:1": {"type": "movie", "ids": {"tmdb": "1"}, "rating": 6}},
        },
    )

    rows = importer._shape_rows(
        [{"feature": "ratings", "item": {"type": "movie", "title": "Changed", "ids": {"tmdb": "1"}, "rating": 9}}],
        {},
        "default",
        "trakt",
    )

    assert rows[0]["status"] == "ready"
    assert rows[0]["reason"] == "rating_update"


def test_missing_existing_key_is_ready_not_already_exists(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(
        importer,
        "_existing_keys",
        lambda _cfg, _inst: {
            "history": {},
            "watchlist": {},
            "ratings": {},
        },
    )

    rows = importer._shape_rows(
        [
            {
                "feature": "history",
                "item": {
                    "type": "movie",
                    "title": "New",
                    "ids": {"tmdb": "1"},
                    "watched_at": "2026-01-01T00:00:00Z",
                },
            },
            {"feature": "watchlist", "item": {"type": "movie", "title": "New", "ids": {"tmdb": "2"}}},
        ],
        {},
        "default",
        "trakt",
    )

    assert [row["status"] for row in rows] == ["ready", "ready"]
    assert [row["reason"] for row in rows] == ["", ""]


def test_simkl_backup_episode_rows_keep_show_identity(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(importer, "_existing_keys", lambda _cfg, _inst: {f: {} for f in importer.FEATURES})

    raw = importer._parse_files(
        "simkl",
        [
            (
                "SimklBackup.json",
                b"""{
                    "shows": [{
                        "status": "completed",
                        "user_rating": 8,
                        "user_rated_at": "2022-01-01T00:00:00Z",
                        "show": {
                          "title": "Shark Tank",
                          "year": 2009,
                          "ids": {"simkl": 16051, "slug": "shark-tank", "imdb": "tt1442550", "tmdb": "30703", "tvdb": "100981"}
                    },
                    "seasons": [{
                      "number": 12,
                      "episodes": [
                        {"number": 1, "watched_at": "2021-12-25T23:51:00Z"},
                        {"number": 2, "watched_at": "2021-12-25T23:51:00Z"}
                      ]
                    }]
                  }]
                }""",
            )
        ],
    )
    rows = importer._shape_rows(raw, {}, "default", "simkl")
    public = importer._public_rows(rows, 100, 0)

    assert [row["feature"] for row in raw] == ["history", "history", "ratings"]
    assert [row["title"] for row in rows[:2]] == ["Shark Tank - S12E01", "Shark Tank - S12E02"]
    assert rows[0]["key"].startswith("tmdb:30703#s12e01@")
    assert rows[1]["key"].startswith("tmdb:30703#s12e02@")
    assert [row["status"] for row in rows[:2]] == ["ready", "ready"]
    assert rows[2]["feature"] == "ratings"
    assert rows[2]["media_type"] == "show"
    assert rows[2]["item"]["rating"] == 8.0
    assert public[0]["ids"]["tmdb"] == "30703"
    assert public[0]["show_ids"]["tmdb"] == "30703"


def test_simkl_backup_preserves_season_zero_episode(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    monkeypatch.setattr(importer, "_existing_keys", lambda _cfg, _inst: {f: {} for f in importer.FEATURES})

    raw = importer._parse_files(
        "simkl",
        [
            (
                "SimklBackup.json",
                b"""{
                  "anime": [{
                    "anime_type": "movie",
                    "show": {
                      "title": "Demon Slayer",
                      "year": 2025,
                      "ids": {"simkl": 2498112, "tmdb": "1311031", "tvdb": "357928"}
                    },
                    "seasons": [{
                      "number": 0,
                      "episodes": [{"number": 1, "watched_at": "2026-06-23T11:13:19Z"}]
                    }]
                  }]
                }""",
            )
        ],
    )
    rows = importer._shape_rows(raw, {}, "default", "simkl")

    assert rows[0]["title"] == "Demon Slayer - S00E01"
    assert rows[0]["key"].startswith("tmdb:1311031#s00e01@")
    assert rows[0]["status"] == "ready"


SIMKL_BACKUP_MIXED = b"""{
  "movies": [
    {
      "status": "plantowatch",
      "added_to_watchlist_at": "2026-07-04T22:16:27Z",
      "last_watched_at": null,
      "user_rating": 7,
      "user_rated_at": "2026-07-05T10:00:00Z",
      "movie": {"title": "Project Hail Mary", "year": 2026, "ids": {"simkl": 1, "tmdb": "555", "imdb": "tt1"}}
    },
    {
      "status": "completed",
      "added_to_watchlist_at": "2026-07-04T22:16:27Z",
      "last_watched_at": "2026-07-04T22:16:27Z",
      "user_rating": null,
      "movie": {"title": "Leon", "year": 1994, "ids": {"simkl": 2, "tmdb": "101"}}
    }
  ],
  "shows": [
    {
      "status": "plantowatch",
      "added_to_watchlist_at": "2026-03-05T00:20:19Z",
      "show": {"title": "Planned Show", "year": 2020, "ids": {"simkl": 3, "tmdb": "777"}}
    }
  ]
}"""


def test_simkl_plantowatch_becomes_watchlist_and_keeps_its_rating() -> None:
    import services.importer as importer

    raw = importer._parse_files("simkl", [("SimklBackup.json", SIMKL_BACKUP_MIXED)])
    by_feature: dict[str, list[dict[str, Any]]] = {}
    for row in raw:
        by_feature.setdefault(row["feature"], []).append(row)

    watchlist = by_feature.get("watchlist") or []
    assert [row["item"]["title"] for row in watchlist] == ["Project Hail Mary", "Planned Show"]
    assert [row["item"]["type"] for row in watchlist] == ["movie", "show"]

    ratings = by_feature.get("ratings") or []
    assert len(ratings) == 1
    assert ratings[0]["item"]["title"] == "Project Hail Mary"
    assert ratings[0]["item"]["rating"] == 7.0
    assert ratings[0]["item"]["rated_at"] == "2026-07-05T10:00:00Z"

    history = by_feature.get("history") or []
    assert [row["item"]["title"] for row in history] == ["Leon"]
    assert history[0]["item"]["watched_at"] == "2026-07-04T22:16:27Z"


def test_simkl_episode_title_is_not_the_show_title() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "simkl",
        [
            (
                "SimklBackup.json",
                b"""{
                  "shows": [{
                    "status": "watching",
                    "show": {"title": "Shark Tank", "year": 2009, "ids": {"tmdb": "30703"}},
                    "seasons": [{"number": 12, "episodes": [{"number": 1, "watched_at": "2021-12-25T23:51:00Z"}]}]
                  }]
                }""",
            )
        ],
    )

    assert len(raw) == 1
    item = raw[0]["item"]
    assert item["title"] is None
    assert item["series_title"] == "Shark Tank"


def test_trakt_custom_lists_are_skipped_not_silently_dropped() -> None:
    import services.importer as importer

    list_items = b"""[
      {"type": "movie", "rank": 1, "listed_at": "2026-07-18T20:15:38Z",
       "movie": {"title": "Iron Man", "year": 2008, "ids": {"tmdb": 1726}}}
    ]"""
    watchlist_items = b"""[
      {"type": "movie", "rank": 1, "listed_at": "2026-05-04T23:43:19Z",
       "movie": {"title": "Walter Mitty", "year": 2013, "ids": {"tmdb": 116745}}}
    ]"""

    raw = importer._parse_files(
        "trakt",
        [
            ("lists-list-36262939-tstje.json", list_items),
            ("lists-favorites.json", list_items),
            ("lists-watchlist.json", watchlist_items),
        ],
    )

    assert [(row["feature"], row["item"]["title"]) for row in raw] == [("watchlist", "Walter Mitty")]


def test_trakt_season_rating_is_importable_by_default() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "trakt",
        [
            (
                "ratings-seasons.json",
                b"""[
                  {"rated_at": "2025-11-12T11:21:41Z", "rating": 7, "type": "season",
                   "season": {"number": 3, "ids": {"trakt": 176990, "tmdb": 118751}},
                   "show": {"title": "The Handmaid's Tale", "year": 2017, "ids": {"tmdb": 69478}}}
                ]""",
            )
        ],
    )
    rows = importer._shape_rows(raw, {}, "default", "trakt")

    assert rows[0]["media_type"] == "season"
    assert rows[0]["status"] == "ready"
    assert "season" in importer.DEFAULT_MEDIA_TYPES
    assert "season" in set(importer.ImportCommitRequest(import_id="x" * 10).media_types)


def test_letterboxd_watched_csv_does_not_fabricate_a_second_play() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "letterboxd",
        [
            ("watched.csv", b"Date,Name,Year,Letterboxd URI\n2026-01-02,Fight Club,1999,https://boxd.it/2b9M\n2026-01-05,Heat,1995,https://boxd.it/29Xk\n"),
            (
                "diary.csv",
                b"Date,Name,Year,Letterboxd URI,Rating,Rewatch,Tags,Watched Date\n"
                b"2026-01-02,Fight Club,1999,https://boxd.it/2b9M,4.5,Yes,noir,2026-01-01\n",
            ),
            (
                "reviews.csv",
                b"Date,Name,Year,Letterboxd URI,Rating,Rewatch,Review,Tags,Watched Date\n"
                b"2026-01-02,Fight Club,1999,https://boxd.it/2b9M,4.5,Yes,Great,,2026-01-01\n",
            ),
        ],
    )
    history = [row for row in raw if row["feature"] == "history"]

    assert [(row["item"]["title"], row["item"]["watched_at"]) for row in history] == [
        ("Fight Club", "2026-01-01T12:00:00Z"),
        ("Heat", "2026-01-05T12:00:00Z"),
    ]
    assert all(row["source_path"] != "reviews.csv" for row in raw)


def test_imdb_watchlist_keeps_rated_entries_in_the_watchlist() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "imdb",
        [
            (
                "watchlist.csv",
                b"Position,Const,Created,Modified,Description,Title,Original Title,URL,Title Type,"
                b"IMDb Rating,Runtime (mins),Year,Genres,Num Votes,Release Date,Directors,Your Rating,Date Rated\n"
                b"1,tt0468569,2023-07-09,2024-02-01,,The Dark Knight,The Dark Knight,https://imdb.com/,Movie,"
                b"9,152,2008,Action,3041074,2008-07-18,Christopher Nolan,8,2025-02-02\n"
                b"2,tt0111161,2023-07-11,2024-02-03,,The Shawshank Redemption,The Shawshank Redemption,https://imdb.com/,Movie,"
                b"9.3,142,1994,Drama,3066765,1994-09-23,Frank Darabont,,\n",
            )
        ],
    )

    assert [(row["feature"], row["item"]["title"]) for row in raw] == [
        ("watchlist", "The Dark Knight"),
        ("ratings", "The Dark Knight"),
        ("watchlist", "The Shawshank Redemption"),
    ]
    assert raw[1]["item"]["rating"] == 8.0
    assert raw[1]["item"]["rated_at"] == "2025-02-02T12:00:00Z"


YAMTRACK_HEADER = (
    b'"media_id","source","media_type","title","image","season_number","episode_number",'
    b'"score","progress","status","start_date","end_date","notes","progressed_at"\n'
)


def _yamtrack_row(media_type: str = "movie", source: str = "tmdb", status: str = "Completed", end: str = "2024-02-09") -> bytes:
    return (
        YAMTRACK_HEADER
        + f'"550","{source}","{media_type}","Thing","","","","8.0","1","{status}","","{end}","","2024-03-01T10:00:00Z"\n'.encode()
    )


@pytest.mark.parametrize(
    ("status", "feature"),
    [("Completed", "history"), ("In progress", "history"), ("Paused", "history"), ("Dropped", "history"), ("Planning", "watchlist")],
)
def test_yamtrack_status_routes_to_the_right_feature(status: str, feature: str) -> None:
    import services.importer as importer

    end = "" if status == "Planning" else "2024-02-09"
    raw = importer._parse_files("yamtrack", [("yamtrack.csv", _yamtrack_row(status=status, end=end))])

    assert [row["feature"] for row in raw] == [feature, "ratings"]


@pytest.mark.parametrize("media_type", ["manga", "game", "book", "comic", "boardgame"])
def test_yamtrack_non_video_media_is_unsupported_not_a_movie(media_type: str) -> None:
    import services.importer as importer

    raw = importer._parse_files("yamtrack", [("yamtrack.csv", _yamtrack_row(media_type=media_type, source="mal"))])
    rows = importer._shape_rows(raw, {}, "default", "yamtrack")

    assert {row["media_type"] for row in rows} == {media_type}
    assert {row["status"] for row in rows} == {"unsupported"}
    assert {row["reason"] for row in rows} == {"unsupported_media_type"}


def test_yamtrack_video_media_types_map_onto_crosswatch_types() -> None:
    import services.importer as importer

    rows = []
    for media_type in ("tv", "anime", "movie", "season", "episode"):
        raw = importer._parse_files("yamtrack", [("yamtrack.csv", _yamtrack_row(media_type=media_type))])
        rows.append(importer._shape_rows(raw, {}, "default", "yamtrack")[0]["media_type"])

    assert rows == ["show", "show", "movie", "season", "episode"]


TVTIME_GDPR = [
    (
        "tracking-prod-records.csv",
        b"uuid,user_id,type,series_name,series_id,season_number,episode_number,episode_id,"
        b"movie_name,movie_id,release_date,runtime,created_at,updated_at\n"
        b"a1,1,watch,Severance,318493,2,10,10592768,,,,,2026-02-12 23:16:00,2026-02-12 23:16:00\n"
        b"a2,1,watch,Severance,318493,2,9,10592767,,,,,2026-02-05 22:00:00,2026-02-05 22:00:00\n"
        b"a3,1,watch,,,,,,Dune,438631,2021-09-15,155,2026-02-14 20:00:00,2026-02-14 20:00:00\n"
        b"a4,1,follow,The Pitt,232884,,,,,,,,2026-01-01 10:00:00,2026-01-01 10:00:00\n"
        b"a5,1,towatch,,,,,,Sicario,273481,2015-09-18,121,2026-01-03 10:00:00,2026-01-03 10:00:00\n"
        b"a6,1,count-week,Severance,318493,,,,,,,,2026-01-04 10:00:00,2026-01-04 10:00:00\n",
    ),
    (
        "followed_tv_show.csv",
        b"user_id,tv_show_id,tv_show_name,active,archived,created_at\n"
        b"1,318493,Severance,1,0,2026-01-01 10:00:00\n"
        b"1,999,Dropped Show,0,1,2025-01-01 10:00:00\n",
    ),
    (
        "ratings-v2-prod-votes.csv",
        b"uuid,user_id,entity_type,series_name,season_number,episode_number,episode_id,vote_key,created_at\n"
        b"r1,1,episode,Severance,2,10,10592768,1-10592768-3,2026-02-13 08:00:00\n"
        b"r2,1,episode,Severance,2,9,10592767,1-10592767-29,2026-02-06 08:00:00\n",
    ),
    ("ratings-live-votes.csv", b"uuid,user_id,movie_name,movie_id,vote_key,created_at\n1,1,Dune,438631,1-438631-27,2026-02-15 08:00:00\n"),
    ("access_token.csv", b"user_id,token\n1,secret\n"),
]


def test_tvtime_gdpr_export_is_detected_and_accepted() -> None:
    import services.importer as importer

    assert importer._detect_source("auto", "gdpr-export.zip", TVTIME_GDPR) == "tvtime"
    validation = importer._validate_source_files("tvtime", TVTIME_GDPR, requested="tvtime")
    assert validation["matched_files"] == [
        "tracking-prod-records.csv",
        "followed_tv_show.csv",
        "ratings-v2-prod-votes.csv",
        "ratings-live-votes.csv",
    ]
    assert validation["warnings"] == []


def test_tvtime_gdpr_export_imports_history_watchlist_and_reactions() -> None:
    import services.importer as importer

    rows = importer._shape_rows(importer._parse_files("tvtime", TVTIME_GDPR), {}, "default", "tvtime")
    summary = importer._summary(rows)

    assert summary["by_feature"] == {"history": 3, "ratings": 3, "watchlist": 3}
    assert summary["ready"] == 9

    keyed = {(row["feature"], row["key"]): row for row in rows}
    assert ("history", "slug:tvtime-severance#s02e10@1770938160") in keyed
    assert ("history", "slug:tvtime-severance#s02e09@1770328800") in keyed
    assert ("history", "movie|title:dune|year:2021@1771099200") in keyed
    assert ("watchlist", "slug:tvtime-pitt") in keyed
    assert ("watchlist", "movie|title:sicario|year:2015") in keyed

    assert keyed[("ratings", "slug:tvtime-severance#s02e10")]["item"]["rating"] == 10.0
    assert keyed[("ratings", "slug:tvtime-severance#s02e09")]["item"]["rating"] == 4.0
    assert keyed[("ratings", "movie|title:dune|year:")]["item"]["rating"] == 7.0


def test_tvtime_gdpr_skips_aggregates_inactive_follows_and_unknown_files() -> None:
    import services.importer as importer

    raw = importer._parse_files("tvtime", TVTIME_GDPR)
    titles = [str(row["item"].get("series_title") or row["item"].get("title") or "") for row in raw]

    assert "Dropped Show" not in titles
    assert all(row["source_path"] != "access_token.csv" for row in raw)
    assert sum(1 for row in raw if row["feature"] == "watchlist" and row["item"].get("title") == "Severance") == 1


def test_preview_never_writes_to_the_target_tracker(tmp_path: Path) -> None:
    import services.importer as importer
    from cw_platform.modules_registry import load_sync_ops
    from cw_platform.provider_instances import build_provider_config_view

    ops = load_sync_ops("CROSSWATCH")
    assert ops is not None

    root = tmp_path / "cw"
    cfg = {"crosswatch": {"connected": True, "enabled": True, "root_dir": str(root)}}
    view = dict(build_provider_config_view(cfg, "CROSSWATCH", "default"))
    view["_cw_history_rewatches"] = True

    ops.add(
        view,
        [{"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "watched_at": "2026-01-01T00:00:00Z"}],
        feature="history",
    )
    state = root / "history.json"
    snapshots = sorted((root / "snapshots").glob("*-history.json"))
    assert state.exists() and snapshots

    state.unlink()
    existing = importer._existing_keys(cfg, "default")

    assert not state.exists(), "preview recreated the tracker state file from a snapshot"
    assert existing["history"] == {}


def test_preview_ignores_existing_keys_when_crosswatch_target_is_disconnected(tmp_path: Path) -> None:
    import services.importer as importer
    from cw_platform.modules_registry import load_sync_ops
    from cw_platform.provider_instances import build_provider_config_view

    ops = load_sync_ops("CROSSWATCH")
    assert ops is not None

    root = tmp_path / "cw"
    cfg = {"crosswatch": {"connected": True, "enabled": True, "root_dir": str(root)}}
    view = dict(build_provider_config_view(cfg, "CROSSWATCH", "default"))
    ops.add(
        view,
        [{"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "rating": 8}],
        feature="ratings",
    )

    disconnected = {"crosswatch": {"connected": False, "enabled": True, "root_dir": str(root)}}
    existing = importer._existing_keys(disconnected, "default")

    assert existing == {f: {} for f in importer.FEATURES}


def test_preview_status_filter_keeps_full_summary() -> None:
    import services.importer as importer

    importer._PREVIEWS["test-preview"] = {
        "created_at": 999999999999,
        "rows": [
            {"feature": "history", "media_type": "movie", "status": "ready", "reason": "", "item": {"ids": {"tmdb": "1"}}},
            {
                "feature": "history",
                "media_type": "movie",
                "status": "exists",
                "reason": "already_exists",
                "item": {"ids": {"tmdb": "2"}},
            },
            {
                "feature": "ratings",
                "media_type": "movie",
                "status": "invalid",
                "reason": "missing_rating",
                "item": {"ids": {"tmdb": "3"}},
            },
        ],
    }
    try:
        data = importer.api_import_preview_rows(
            "test-preview",
            features="history,ratings,watchlist",
            media_types="movie,show,episode",
            status="ready",
            q="",
            limit=100,
            offset=0,
        )
    finally:
        importer._PREVIEWS.pop("test-preview", None)

    assert data["total"] == 3
    assert data["filtered_total"] == 1
    assert data["summary"]["ready"] == 1
    assert data["summary"]["exists"] == 1
    assert data["summary"]["by_status"]["exists"] == 1
    assert data["summary"]["by_status"]["invalid"] == 1


def test_summary_feature_split_adds_up_to_what_will_be_imported() -> None:
    import services.importer as importer

    summary = importer._summary(
        [
            {"feature": "history", "status": "ready"},
            {"feature": "history", "status": "ready"},
            {"feature": "history", "status": "exists"},
            {"feature": "ratings", "status": "ready"},
            {"feature": "ratings", "status": "exists"},
            {"feature": "watchlist", "status": "exists"},
            {"feature": "watchlist", "status": "duplicate"},
            {"feature": "history", "status": "invalid"},
        ]
    )

    assert summary["ready"] == 3
    assert summary["exists"] == 3
    assert summary["by_feature_ready"] == {"history": 2, "ratings": 1, "watchlist": 0}
    assert summary["by_feature_exists"] == {"history": 1, "ratings": 1, "watchlist": 1}

    for include_existing, headline in ((False, summary["ready"]), (True, summary["ready"] + summary["exists"])):
        shown = sum(
            summary["by_feature_ready"][f] + (summary["by_feature_exists"][f] if include_existing else 0)
            for f in ("history", "ratings", "watchlist")
        )
        assert shown == headline

    assert summary["by_status"]["duplicate"] == 1
    assert summary["by_status"]["invalid"] == 1
    assert summary["total"] == 8


def test_preview_all_orders_ready_rows_before_skipped() -> None:
    import services.importer as importer

    rows = [
        {
            "id": "skipped",
            "title": "Already",
            "feature": "ratings",
            "media_type": "movie",
            "status": "exists",
            "reason": "already_exists",
        },
        {
            "id": "ready",
            "title": "Ready",
            "feature": "history",
            "media_type": "movie",
            "status": "ready",
            "reason": "",
        },
    ]

    filtered = importer._filtered_rows(
        rows,
        features={"history", "ratings"},
        media_types={"movie"},
        status="all",
        q="",
    )

    assert [row["id"] for row in filtered] == ["ready", "skipped"]


def test_trakt_export_skips_hidden_progress_and_aggregate_history_when_events_exist() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "trakt",
        [
            (
                "watched-history-1.json",
                b"""[
                  {"id":1,"watched_at":"2026-01-02T20:00:00Z","type":"movie","movie":{"title":"Heat","year":1995,"ids":{"tmdb":949}}}
                ]""",
            ),
            (
                "watched-movies-1.json",
                b"""[
                  {"last_watched_at":"2026-01-02T20:00:00Z","movie":{"title":"Heat","year":1995,"ids":{"tmdb":949}}}
                ]""",
            ),
            (
                "hidden-progress-watched.json",
                b"""[
                  {"hidden_at":"2026-01-03T20:00:00Z","type":"season","season":{"number":1,"ids":{"tmdb":1}},"show":{"title":"Hidden","ids":{"tmdb":2}}}
                ]""",
            ),
            (
                "watched-playback.json",
                b"""[
                  {"progress":50,"paused_at":"2026-01-03T20:00:00Z","type":"movie","movie":{"title":"Paused","ids":{"tmdb":3}}}
                ]""",
            ),
        ],
    )

    assert len(raw) == 1
    assert raw[0]["source_path"] == "watched-history-1.json"
    assert raw[0]["item"]["title"] == "Heat"


def test_letterboxd_official_diary_csv_stays_history() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "letterboxd",
        [
            (
                "diary.csv",
                b"Date,Name,Year,Letterboxd URI,Rating,Rewatch,Tags,WatchedDate\n"
                b"2026-01-03,Heat,1995,https://letterboxd.com/film/heat-1995/,4.5,Yes,crime,2026-01-02\n",
            )
        ],
    )

    assert len(raw) == 2
    assert raw[0]["feature"] == "history"
    assert raw[0]["item"]["watched_at"] == "2026-01-02T12:00:00Z"
    assert raw[0]["item"]["rating"] == 9.0
    assert raw[1]["feature"] == "ratings"


def test_letterboxd_official_rating10_column() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "letterboxd",
        [("ratings.csv", b"Title,Year,Rating10\nTop Gun,1986,9\n")],
    )

    assert len(raw) == 1
    assert raw[0]["feature"] == "ratings"
    assert raw[0]["item"]["rating"] == 9.0


def test_source_validation_rejects_wrong_explicit_provider() -> None:
    import services.importer as importer

    with pytest.raises(Exception) as exc:
        importer._validate_source_files("simkl", [("watched.csv", b"Title\nHeat\n")], requested="simkl")

    detail = getattr(exc.value, "detail", {})
    assert detail.get("code") == importer.ERROR_SOURCE_MISMATCH
    assert "SimklBackup.json" in detail.get("expected_files", [])


def test_simkl_official_backup_filename_matches_validation() -> None:
    import services.importer as importer

    validation = importer._validate_source_files("simkl", [("SimklBackup.json", b"{}")], requested="simkl")

    assert validation["matched_files"] == ["SimklBackup.json"]


def test_imdb_watchlist_and_ratings_csv_are_importable() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "imdb",
        [
            (
                "watchlist.csv",
                b"Const,Title,Year,Title Type,IMDb Rating\n"
                b"tt0113277,Heat,1995,movie,8.3\n",
            ),
            (
                "ratings.csv",
                b"Const,Title,Year,Title Type,Your Rating,Date Rated\n"
                b"tt0468569,The Dark Knight,2008,movie,10,2026-02-03\n",
            ),
        ],
    )

    assert [row["feature"] for row in raw] == ["watchlist", "ratings"]
    assert raw[0]["item"]["ids"]["imdb"] == "tt0113277"
    assert raw[1]["item"]["rating"] == 10.0
    assert raw[1]["item"]["rated_at"] == "2026-02-03T12:00:00Z"


def test_tvtime_nested_json_imports_show_and_watched_episode() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "tvtime",
        [
            (
                "tvtime-series-2026-08-10.json",
                b"""[
                  {
                    "uuid": "show-1",
                    "title": "Example Show",
                    "id": {"tvdb": 100, "imdb": "tt100"},
                    "seasons": [
                      {"number": 1, "episodes": [
                        {"number": 2, "id": {"tvdb": 200}, "is_watched": true, "watched_at": "2026-01-02T20:00:00Z"}
                      ]}
                    ]
                  }
                ]""",
            )
        ],
    )

    assert [row["feature"] for row in raw] == ["watchlist", "history"]
    episode = raw[1]["item"]
    assert episode["type"] == "episode"
    assert episode["series_title"] == "Example Show"
    assert episode["show_ids"] == {"tvdb": "100", "imdb": "tt100"}
    assert episode["season"] == 1
    assert episode["episode"] == 2
    assert episode["watched_at"] == "2026-01-02T20:00:00Z"


def test_tvtime_episode_csv_imports_history() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "tvtime",
        [
            (
                "tvtime-series-episodes-2026-08-10.csv",
                b"series_tvdb_id,series_imdb_id,series_uuid,title,season,episode,tvdb_id,is_watched,watched_at,rewatch_count,special\n"
                b"100,tt100,show-1,Example Show,1,2,200,true,2026-01-02T20:00:00Z,0,false\n",
            )
        ],
    )

    assert len(raw) == 1
    assert raw[0]["feature"] == "history"
    assert raw[0]["item"]["show_ids"] == {"tvdb": "100", "imdb": "tt100", "slug": "show-1"}
    assert raw[0]["item"]["ids"]["tvdb"] == "200"


def test_yamtrack_csv_imports_history_and_rating_rows() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "yamtrack",
        [
            (
                "yamtrack_history.csv",
                b"media_id,source,media_type,title,image,season_number,episode_number,score,progress,status,start_date,end_date,notes,progressed_at\n"
                b"949,tmdb,movie,Heat,, , ,8,1,Completed,,2026-01-02T20:00:00Z,,2026-01-02T20:00:00Z\n",
            )
        ],
    )

    assert [row["feature"] for row in raw] == ["history", "ratings"]
    assert raw[0]["item"]["ids"]["tmdb"] == "949"
    assert raw[0]["item"]["watched_at"] == "2026-01-02T20:00:00Z"
    assert raw[1]["item"]["rating"] == 8.0


def test_generic_csv_fallback_imports_common_columns() -> None:
    import services.importer as importer

    raw = importer._parse_files(
        "generic",
        [("my_export.csv", b"Title,Year,Type,tmdb,watched_at,rating\nHeat,1995,movie,949,2026-01-02,8\n")],
    )

    assert [row["feature"] for row in raw] == ["history", "ratings"]
    assert raw[0]["item"]["ids"]["tmdb"] == "949"


def test_import_commit_writes_only_selected_ready_rows(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import services.importer as importer

    seen: list[tuple[str, list[dict[str, Any]], dict[str, Any]]] = []

    class Ops:
        def add(self, cfg: dict[str, Any], items: list[dict[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
            seen.append((feature, list(items), dict(cfg)))
            return {"ok": True, "count": len(items), "confirmed_keys": ["ok"] * len(items), "unresolved": []}

    monkeypatch.setattr(importer, "load_sync_ops", lambda name: Ops() if name == "CROSSWATCH" else None)
    monkeypatch.setattr(
        importer,
        "load_config",
        lambda: {"crosswatch": {"connected": True, "root_dir": str(tmp_path / "cw")}},
    )

    importer._PREVIEWS.clear()
    importer._PREVIEWS["import123"] = {
        "created_at": importer.time.time(),
        "rows": [
            {
                "id": "h1",
                "feature": "history",
                "media_type": "movie",
                "status": "ready",
                "item": {"type": "movie", "title": "One", "ids": {"tmdb": "1"}, "watched_at": "2026-01-01T00:00:00Z"},
            },
            {
                "id": "r1",
                "feature": "ratings",
                "media_type": "movie",
                "status": "ready",
                "item": {"type": "movie", "title": "One", "ids": {"tmdb": "1"}, "rating": 8},
            },
            {
                "id": "dup",
                "feature": "watchlist",
                "media_type": "movie",
                "status": "duplicate",
                "item": {"type": "movie", "title": "Dup", "ids": {"tmdb": "2"}},
            },
        ],
    }

    res = importer.api_import_commit(
        importer.ImportCommitRequest(
            import_id="import123",
            mode="selected",
            row_ids=["h1", "r1", "dup"],
            target_instance="default",
        )
    )

    assert res["applied"] == 2
    assert [call[0] for call in seen] == ["history", "ratings"]
    assert seen[0][2]["_cw_history_rewatches"] is True


def test_import_commit_can_replay_rows_already_in_the_tracker(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import services.importer as importer

    seen: list[tuple[str, list[dict[str, Any]]]] = []

    class Ops:
        def add(self, cfg: dict[str, Any], items: list[dict[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
            seen.append((feature, list(items)))
            return {"ok": True, "count": len(items), "unresolved": []}

    monkeypatch.setattr(importer, "load_sync_ops", lambda name: Ops() if name == "CROSSWATCH" else None)
    monkeypatch.setattr(
        importer,
        "load_config",
        lambda: {"crosswatch": {"connected": True, "root_dir": str(tmp_path / "cw")}},
    )

    rows = [
        {
            "id": "new",
            "feature": "history",
            "media_type": "movie",
            "status": "ready",
            "item": {"type": "movie", "title": "New", "ids": {"tmdb": "1"}, "watched_at": "2026-01-01T00:00:00Z"},
        },
        {
            "id": "held",
            "feature": "history",
            "media_type": "movie",
            "status": "exists",
            "item": {"type": "movie", "title": "Held", "ids": {"tmdb": "2"}, "watched_at": "2026-01-02T00:00:00Z"},
        },
        {
            "id": "dupe",
            "feature": "history",
            "media_type": "movie",
            "status": "duplicate",
            "item": {"type": "movie", "title": "Dupe", "ids": {"tmdb": "3"}, "watched_at": "2026-01-03T00:00:00Z"},
        },
    ]

    importer._PREVIEWS.clear()
    importer._PREVIEWS["replay01x"] = {"created_at": importer.time.time(), "rows": rows}
    try:
        without = importer.api_import_commit(importer.ImportCommitRequest(import_id="replay01x"))
        assert [item["title"] for item in seen[0][1]] == ["New"]
        assert without["applied"] == 1

        seen.clear()
        with_existing = importer.api_import_commit(
            importer.ImportCommitRequest(import_id="replay01x", include_existing=True)
        )
        assert [item["title"] for item in seen[0][1]] == ["New", "Held"]
        assert with_existing["applied"] == 2
    finally:
        importer._PREVIEWS.clear()


def test_commit_refuses_when_the_tracker_is_not_connected(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    import services.importer as importer

    class Ops:
        def is_configured(self, cfg: dict[str, Any]) -> bool:
            root = cfg.get("crosswatch") or {}
            return root.get("connected") is True

        def add(self, cfg: dict[str, Any], items: list[dict[str, Any]], *, feature: str, dry_run: bool = False) -> dict[str, Any]:
            raise AssertionError("nothing may be written to a disconnected tracker")

    monkeypatch.setattr(importer, "load_sync_ops", lambda name: Ops() if name == "CROSSWATCH" else None)
    monkeypatch.setattr(
        importer,
        "load_config",
        lambda: {"crosswatch": {"connected": False, "enabled": True, "root_dir": str(tmp_path / "cw")}},
    )

    rows = [
        {
            "id": "r1",
            "feature": "history",
            "media_type": "movie",
            "status": "ready",
            "item": {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}, "watched_at": "2026-01-01T00:00:00Z"},
        }
    ]
    importer._PREVIEWS.clear()
    importer._PREVIEWS["notconnected"] = {"created_at": importer.time.time(), "rows": rows, "target_instance": "default"}
    try:
        with pytest.raises(Exception) as exc:
            importer.api_import_commit(importer.ImportCommitRequest(import_id="notconnected"))
    finally:
        importer._PREVIEWS.clear()

    assert getattr(exc.value, "detail", {}).get("code") == importer.ERROR_TARGET_UNAVAILABLE
    assert getattr(exc.value, "status_code", None) == 503


def test_options_reports_whether_each_target_is_connected(monkeypatch: pytest.MonkeyPatch) -> None:
    import services.importer as importer

    class Ops:
        def is_configured(self, cfg: dict[str, Any]) -> bool:
            return (cfg.get("crosswatch") or {}).get("connected") is True

    monkeypatch.setattr(importer, "load_sync_ops", lambda name: Ops() if name == "CROSSWATCH" else None)

    monkeypatch.setattr(importer, "load_config", lambda: {"crosswatch": {"connected": False}})
    assert importer.api_import_options()["targets"] == [{"id": "default", "label": "Default", "connected": False}]

    monkeypatch.setattr(importer, "load_config", lambda: {"crosswatch": {"connected": True}})
    assert importer.api_import_options()["targets"] == [{"id": "default", "label": "Default", "connected": True}]

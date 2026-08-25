from __future__ import annotations

import importlib
from typing import Any, Mapping


collection = importlib.import_module("providers.sync.plex._collection")
common = importlib.import_module("providers.sync.plex._common")


def test_plex_collection_manifest_declares_show_scope():
    from providers.sync._mod_PLEX import get_manifest

    assert get_manifest()["capabilities"]["collection"]["types"] == {
        "movies": True,
        "shows": True,
        "seasons": True,
        "episodes": True,
    }


def test_collection_normalize_never_enables_guid_discovery(monkeypatch):
    seen: dict[str, Any] = {}

    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        seen["row"] = dict(row)
        seen["token"] = token
        seen["allow_discover"] = allow_discover
        return {
            "type": row.get("type"),
            "title": "Attack on Titan",
            "ids": {"tmdb": "1429"},
            "show_ids": {"tmdb": "1429"},
            "season": 1,
            "episode": 1,
        }

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    item = collection._normalize_row(
        {"ratingKey": "45573", "type": "episode"},
        library_id="12",
        fallback_type="episode",
        token="plex-token",
    )

    assert seen["allow_discover"] is False
    assert seen["token"] == "plex-token"
    assert seen["row"]["librarySectionID"] == "12"
    assert item["type"] == "episode"
    assert item["library_id"] == "12"


def test_collection_normalize_maps_plex_added_at_to_collected_at(monkeypatch):
    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        return {"type": row.get("type"), "title": "Heat", "ids": {"tmdb": "949"}}

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    item = collection._normalize_row(
        {"ratingKey": "1", "type": "movie", "addedAt": 1787600000},
        library_id="19",
        fallback_type="movie",
        token="plex-token",
    )

    assert item["collected_at"] == "2026-08-24T19:33:20Z"


def test_collection_xml_rows_keep_plex_added_at(monkeypatch):
    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        return {"type": row.get("type"), "title": row.get("title"), "ids": {"tmdb": "1007757"}}

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    parsed = common._xml_to_container(
        """
        <MediaContainer size="1" totalSize="1">
          <Video ratingKey="47811" type="movie" title="Verwisseld" year="2026" addedAt="1778748605">
            <Guid id="tmdb://1007757"/>
          </Video>
        </MediaContainer>
        """
    )
    row = parsed["MediaContainer"]["Metadata"][0]

    item = collection._normalize_row(row, library_id="9", fallback_type="movie", token="plex-token")

    assert row["addedAt"] == 1778748605
    assert item["collected_at"] == "2026-05-14T08:50:05Z"


def test_collection_normalize_preserves_show_rows(monkeypatch):
    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        return {"type": row.get("type"), "title": "The Expanse", "ids": {"tmdb": "63639"}}

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    item = collection._normalize_row(
        {"ratingKey": "2", "type": "show", "addedAt": 1787600000},
        library_id="3",
        fallback_type="show",
        token="plex-token",
    )

    assert item["type"] == "show"
    assert item["ids"] == {"tmdb": "63639"}
    assert item["library_id"] == "3"


def test_collection_normalize_preserves_season_rows(monkeypatch):
    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        return {
            "type": row.get("type"),
            "series_title": "The Expanse",
            "show_ids": {"tmdb": "63639"},
            "season": 1,
        }

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    item = collection._normalize_row(
        {"ratingKey": "3", "type": "season", "title": "Season 1", "addedAt": 1787600000},
        library_id="3",
        fallback_type="season",
        token="plex-token",
    )

    assert item["type"] == "season"
    assert item["show_ids"] == {"tmdb": "63639"}
    assert item["season"] == 1
    assert item["library_id"] == "3"


def test_collection_normalize_skips_season_rows_without_scope(monkeypatch):
    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        return {"type": row.get("type"), "title": "Season 1", "ids": {"tmdb": "123"}}

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    item = collection._normalize_row(
        {"ratingKey": "3", "type": "season", "title": "Season 1"},
        library_id="3",
        fallback_type="season",
        token="plex-token",
    )

    assert item is None


def test_collection_normalize_skips_episode_rows_without_scope(monkeypatch):
    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        return {"type": row.get("type"), "title": "Kaamelott", "ids": {"tmdb": "123"}}

    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    item = collection._normalize_row(
        {"ratingKey": "9952", "type": "episode", "title": "Kaamelott"},
        library_id="3",
        fallback_type="episode",
        token="plex-token",
    )

    assert item is None


def test_collection_index_scans_show_libraries_as_shows_seasons_and_episodes(monkeypatch):
    calls: list[tuple[str, int]] = []

    class Section:
        key = "3"
        type = "show"

    class Client:
        server = object()

    class Adapter:
        client = Client()

        def libraries(self, types=()):
            return [Section()]

    def fake_fetch_section_guid_rows(srv: Any, section_id: str, plex_type: int):
        calls.append((section_id, plex_type))
        if plex_type == 2:
            return [{"ratingKey": "2", "type": "show", "title": "The Expanse"}], 1
        if plex_type == 3:
            return [{"ratingKey": "3", "type": "season", "title": "Season 1"}], 1
        if plex_type == 4:
            return [{"ratingKey": "4", "type": "episode", "title": "Dulcinea"}], 1
        return [], 0

    def fake_minimal_from_history_row(
        row: Mapping[str, Any],
        *,
        token: str | None = None,
        allow_discover: bool = False,
    ) -> dict[str, Any]:
        if row.get("type") == "show":
            return {"type": "show", "title": "The Expanse", "ids": {"tmdb": "63639"}}
        if row.get("type") == "season":
            return {"type": "season", "series_title": "The Expanse", "show_ids": {"tmdb": "63639"}, "season": 1}
        return {
            "type": "episode",
            "series_title": "The Expanse",
            "show_ids": {"tmdb": "63639"},
            "season": 1,
            "episode": 1,
            "ids": {"tmdb": "999001"},
        }

    monkeypatch.setattr(collection, "_fetch_section_guid_rows", fake_fetch_section_guid_rows)
    monkeypatch.setattr(collection, "minimal_from_history_row", fake_minimal_from_history_row)

    idx = collection.build_index(Adapter())

    assert calls == [("3", 2), ("3", 3), ("3", 4)]
    assert idx["tmdb:63639"]["type"] == "show"
    assert idx["tmdb:63639#season:1"]["type"] == "season"
    assert idx["tmdb:63639#s01e01"]["type"] == "episode"

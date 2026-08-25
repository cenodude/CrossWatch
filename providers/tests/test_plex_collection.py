from __future__ import annotations

import importlib
from typing import Any, Mapping


collection = importlib.import_module("providers.sync.plex._collection")
common = importlib.import_module("providers.sync.plex._common")


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
        return {"type": row.get("type"), "title": "Attack on Titan", "ids": {"tmdb": "1429"}}

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

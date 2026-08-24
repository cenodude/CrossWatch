from __future__ import annotations

import importlib
from typing import Any, Mapping


collection = importlib.import_module("providers.sync.plex._collection")


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

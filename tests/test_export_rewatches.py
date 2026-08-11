# CrossWatch test scripts
from __future__ import annotations

import services.export as export_service


def _rewatch_state() -> dict:
    return {
        "providers": {
            "TRAKT": {
                "history": {
                    "baseline": {
                        "items": {
                            "tmdb:550@1700000000": {
                                "type": "movie",
                                "title": "Fight Club",
                                "year": 1999,
                                "ids": {"tmdb": "550"},
                                "watched_at": "2023-11-14T22:13:20Z",
                            },
                            "tmdb:550@1700100000": {
                                "type": "movie",
                                "title": "Fight Club",
                                "year": 1999,
                                "ids": {"tmdb": "550"},
                                "watched_at": "2023-11-16T02:00:00Z",
                            },
                        }
                    }
                }
            }
        }
    }


def test_export_sample_keeps_rewatch_events_for_supported_provider(monkeypatch) -> None:
    monkeypatch.setattr(export_service, "_load_state", lambda _features=None: _rewatch_state())
    monkeypatch.setattr(export_service, "_provider_rewatch_read_supported", lambda _provider: True)

    res = export_service.api_export_sample(
        provider="TRAKT",
        provider_instance="all",
        feature="history",
        format="letterboxd",
        media_types="movie",
        include_rewatches=True,
        q="",
    )

    assert res["rewatches_supported"] is True
    assert res["include_rewatches"] is True
    assert res["total"] == 2
    assert [row["key"] for row in res["items"]] == ["tmdb:550@1700000000", "tmdb:550@1700100000"]


def test_export_sample_can_collapse_rewatch_events(monkeypatch) -> None:
    monkeypatch.setattr(export_service, "_load_state", lambda _features=None: _rewatch_state())
    monkeypatch.setattr(export_service, "_provider_rewatch_read_supported", lambda _provider: True)

    res = export_service.api_export_sample(
        provider="TRAKT",
        provider_instance="all",
        feature="history",
        format="letterboxd",
        media_types="movie",
        include_rewatches=False,
        q="",
    )

    assert res["include_rewatches"] is False
    assert res["total"] == 1
    assert res["items"][0]["key"] == "tmdb:550"
    assert res["items"][0]["watched_at"] == "2023-11-16T02:00:00Z"

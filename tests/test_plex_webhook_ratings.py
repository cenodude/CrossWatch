# CrossWatch test scripts
from __future__ import annotations

from typing import Any


def test_plex_webhook_ratings_forward_to_crosswatch_and_floppy(monkeypatch) -> None:
    from providers.scrobble.plex import ratings_sync
    from providers.webhooks import plex

    sent: list[dict[str, Any]] = []

    def fake_send_rating(provider: str, cfg: dict[str, Any], instance: str, item: dict[str, Any], rating: int) -> dict[str, Any]:
        sent.append({"provider": provider, "instance": instance, "item": item, "rating": rating})
        return {"ok": True, "resp": {"confirmed_keys": ["tmdb:550"]}}

    monkeypatch.setattr(plex, "_save_config", lambda _cfg: None)
    monkeypatch.setattr(ratings_sync, "send_rating", fake_send_rating)
    plex._LAST_RATING_BY_ACC.clear()

    cfg = {
        "scrobble": {
            "enabled": True,
            "sources": {"webhook": True},
            "webhook": {
                "sinks": ["crosswatch", "floppy"],
                "sink_instances": {"crosswatch": "CW-P01", "floppy": "F1"},
                "plex_crosswatch_ratings": True,
                "plex_floppy_ratings": True,
            },
        },
        "crosswatch": {"connected": True, "instances": {"CW-P01": {"connected": True}}},
        "floppy": {
            "server_url": "http://floppy.test",
            "api_token": "token",
            "instances": {"F1": {"server_url": "http://floppy.test", "api_token": "token"}},
        },
    }
    payload = {
        "event": "media.rate",
        "Account": {"title": "pasca"},
        "Metadata": {
            "type": "movie",
            "title": "Fight Club",
            "year": 1999,
            "ratingKey": "rk-550",
            "userRating": 8,
            "Guid": [{"id": "tmdb://550"}],
        },
    }

    result = plex.process_webhook(payload, {}, cfg=cfg)

    assert result["ok"] is True
    assert {call["provider"] for call in sent} == {"crosswatch", "floppy"}
    assert {call["instance"] for call in sent} == {"CW-P01", "F1"}
    assert all(call["item"]["ids"]["tmdb"] == 550 for call in sent)
    assert all(call["rating"] == 8 for call in sent)

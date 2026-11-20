# /tests/test_plex_rating_skips.py
# CrossWatch - Skipped Plex rating event regression tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json

import pytest
import requests

from cw_platform import event_archive
from cw_platform.event_archive import scrobble_recorder
from providers.scrobble.plex import ratings_sync, watch
from providers.webhooks import plex


@pytest.mark.parametrize("source", ["watcher", "route", "webhook"])
@pytest.mark.parametrize("provider", ["simkl", "mdblist", "anilist", "kitsu", "myanimelist", "silo", "all"])
@pytest.mark.parametrize(("mixed", "with_ids"), [(False, False), (False, True), (True, True)])
def test_episode_rating_skip_is_visible_in_events(monkeypatch, source, provider, mixed, with_ids):
    conn = event_archive.connect(":memory:")
    monkeypatch.setattr(scrobble_recorder, "record_events", lambda rows: event_archive.record_events(rows, conn=conn))
    monkeypatch.setattr(scrobble_recorder, "_seen", lambda _: False)
    monkeypatch.setattr(plex, "_save_config", lambda _: None)
    monkeypatch.setattr(ratings_sync, "send_rating", lambda *args: {"ok": True})

    def unexpected_request(*args, **kwargs):
        pytest.fail("Skipped episode ratings must not make a network request")

    monkeypatch.setattr(requests.Session, "send", unexpected_request)
    watch._LAST_RATING_BY_ACC.clear()
    plex._LAST_RATING_BY_ACC.clear()
    skip_targets = ["simkl", "mdblist", "anilist", "kitsu", "myanimelist", "silo"] if provider == "all" else [provider]
    targets = skip_targets + (["crosswatch"] if mixed else [])
    monkeypatch.setattr(plex, "_sinks_from_webhook_cfg", lambda *args: set(targets))
    flags = {f"plex_{target}_ratings": True for target in targets}
    cfg = {
        "scrobble": {
            "enabled": True,
            "sources": {"watcher": True, "webhook": True},
            "watch": {
                **flags, "route_provider": "plex", "route_provider_instance": "P01",
                "route_sink_instance": "S01",
                "route_options": {"ratings": {"mode": "custom", "targets": targets}},
            },
            "webhook": {**flags, "sinks": targets, "sink_instances": {target: "S01" for target in targets}},
        },
        "simkl": {"access_token": "test-token", "instances": {"S01": {"access_token": "test-token"}}},
        "crosswatch": {"enabled": True, "instances": {"S01": {"enabled": True}}},
    }
    md = {
        "type": "episode", "title": "Episode Two", "grandparentTitle": "Example Show",
        "parentIndex": 1, "index": 2, "ratingKey": "48971", "userRating": 6,
    }
    if with_ids:
        md.update(Guid=[{"id": "tvdb://456"}], grandparentGuid="tmdb://123")
    payload = {"event": "media.rate", "Account": {"title": "test-user"}, "Metadata": md}

    def process():
        if source == "webhook":
            return plex.process_webhook(payload, {}, cfg=cfg, provider_instance="P01")
        return watch.process_rating_webhook(payload, {}, cfg_override=cfg, route_hook={} if source == "route" else None)

    try:
        result = process()
        for target in skip_targets:
            assert result[target] == {"ok": True, "skipped": True, "reason": "episode_ratings_unsupported"}
        if mixed:
            assert result["crosswatch"]["ok"] is True
        assert process().get("dedup") is True
        rows = conn.execute("SELECT * FROM events WHERE event_type='rating_skipped'").fetchall()
        assert {row["destination_provider"] for row in rows} == {target.upper() for target in skip_targets}
        assert len(rows) == len(skip_targets)
        for row in rows:
            assert row["severity"] == "info"
            assert row["source_instance"] == "P01"
            assert row["destination_instance"] == "S01"
            assert json.loads(row["detail"])["rating"] == 6
        groups = event_archive.list_groups(conn=conn, domain="scrobble", status="skipped")
        assert groups["total"] == len(skip_targets)
        for group in groups["items"]:
            assert "Rating skipped: Example Show S01E02" in group["summary"]
            assert "Episode ratings are unsupported by this integration" in group["summary"]
            assert group["feature"] == "ratings"
    finally:
        conn.close()


def test_ignored_rating_results_remain_unarchived(monkeypatch):
    rows = []
    monkeypatch.setattr(event_archive, "record_webhook", lambda **kwargs: rows.append(kwargs))
    ratings_sync.archive_rating_results(
        {"simkl": {"ok": True, "ignored": True}}, media_type="episode", md={}, ids={}, account=None,
        rating=6, source_instance="default", instance_for=lambda _: "default",
    )
    assert rows == []


@pytest.mark.parametrize("provider", ["trakt", "crosswatch", "floppy", "punchplay", "flicklist", "scrob", "wetrakr"])
def test_supported_episode_rating_targets_are_forwarded(monkeypatch, provider):
    calls = []
    rows = []
    monkeypatch.setattr(ratings_sync, "send_rating", lambda *args: calls.append(args) or {"ok": True})
    monkeypatch.setattr(watch, "_trakt_send_rating", lambda *args: calls.append(args) or {"ok": True})
    monkeypatch.setattr(event_archive, "record_webhook", lambda **kwargs: rows.append(kwargs))
    watch._LAST_RATING_BY_ACC.clear()
    cfg = {"scrobble": {"enabled": True, "sources": {"watcher": True}, "watch": {f"plex_{provider}_ratings": True}}}
    payload = {"event": "media.rate", "Metadata": {
        "type": "episode", "title": "Episode Two", "grandparentTitle": "Example Show",
        "parentIndex": 1, "index": 2, "ratingKey": "123", "userRating": 6,
        "Guid": [{"id": "tvdb://456"}], "grandparentGuid": "tmdb://123",
    }}
    result = watch.process_rating_webhook(payload, {}, cfg_override=cfg)
    assert result[provider] == {"ok": True}
    assert len(calls) == 1
    assert len(rows) == 1 and rows[0]["event_type"] == "rating_applied"
    assert rows[0]["destination_provider"] == provider


@pytest.mark.parametrize("source", ["watcher", "webhook"])
@pytest.mark.parametrize("media_type", ["movie", "show", "episode"])
def test_simkl_supported_ratings_and_disabled_episode_ratings(monkeypatch, source, media_type):
    calls = []
    rows = []
    enabled = media_type != "episode"
    monkeypatch.setattr(watch, "_simkl_send_rating", lambda *args: calls.append(args) or {"ok": True})
    monkeypatch.setattr(event_archive, "record_webhook", lambda **kwargs: rows.append(kwargs))
    monkeypatch.setattr(plex, "_save_config", lambda _: None)
    watch._LAST_RATING_BY_ACC.clear()
    plex._LAST_RATING_BY_ACC.clear()
    cfg = {
        "scrobble": {
            "enabled": True, "sources": {"watcher": True, "webhook": True},
            "watch": {"plex_simkl_ratings": enabled},
            "webhook": {"plex_simkl_ratings": enabled, "sinks": ["simkl"]},
        },
        "simkl": {"access_token": "test-token"},
    }
    payload = {"event": "media.rate", "Metadata": {
        "type": media_type, "title": "Example", "ratingKey": "123", "userRating": 6,
        "Guid": [{"id": "tmdb://123"}],
    }}
    if source == "watcher":
        result = watch.process_rating_webhook(payload, {}, cfg_override=cfg)
    else:
        result = plex.process_webhook(payload, {}, cfg=cfg)
    if enabled:
        assert result["simkl"]["ok"] is True
        assert len(calls) == 1 and calls[0][0] == media_type
        assert len(rows) == 1 and rows[0]["event_type"] == "rating_applied"
    else:
        assert result.get("ignored") is True
        assert calls == [] and rows == []

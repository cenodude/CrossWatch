# tests/test_event_activities.py
# CrossWatch - Activity destination grouping and pagination tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

import pytest

from cw_platform.event_archive import groups
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.recorder import make_event, record_events


@pytest.fixture
def activity_db():
    conn = connect(":memory:")
    for session in range(14):
        for index, provider in enumerate(("CROSSWATCH", "TRAKT", "SIMKL")):
            record_events([make_event(
                domain="scrobble", event_type="rating_skipped" if provider == "SIMKL" else "rating_applied",
                created_at=2_000_000_000 + session * 100 + index, feature="ratings", operation="update",
                source_kind="plexwatcher-webhook", source_provider="PLEX", source_instance="default",
                destination_provider=provider, destination_instance="default", item_key="tvdb:100",
                title="Example", media_type="episode", season=1, episode=2,
                session_key=f"rating-{session}", hash_extra=f"{session}-{provider}", detail={"rating": 6},
                reason_code="episode_rating_unsupported" if provider == "SIMKL" else None,
            )], conn=conn)
    groups.correlate(conn=conn)
    yield conn
    conn.close()


def test_activity_pagination_counts_actions_and_keeps_all_destinations(activity_db):
    first = groups.list_groups(conn=activity_db, domain="scrobble", visibility="all", activities=True, limit=10)
    last = groups.list_groups(conn=activity_db, domain="scrobble", visibility="all", activities=True, limit=10, offset=10)
    assert first["total"] == last["total"] == 14
    assert len(first["items"]) == 10
    assert len(last["items"]) == 4
    assert not {row["id"] for row in first["items"]} & {row["id"] for row in last["items"]}
    for row in first["items"] + last["items"]:
        assert row["status"] == "rated"
        assert row["event_count"] == 3
        assert row["rating"] == 6
        assert {dest["destination_provider"] for dest in row["destinations"]} == {"CROSSWATCH", "TRAKT", "SIMKL"}
    assert groups.list_groups(conn=activity_db, domain="scrobble", visibility="all")["total"] == 42


def test_activity_permissions_apply_before_grouping_and_counting(activity_db):
    result = groups.list_groups(
        conn=activity_db, domain="scrobble", visibility="all", activities=True, limit=10, offset=10,
        row_filter=lambda row: row["destination_provider"] == "TRAKT" and row["last_event_at"] >= 2_000_000_500,
    )
    assert result["total"] == 9
    assert result["items"] == []
    result = groups.list_groups(
        conn=activity_db, domain="scrobble", visibility="all", activities=True,
        row_filter=lambda row: row["destination_provider"] == "TRAKT",
    )
    assert result["total"] == 14
    assert all(len(row["destinations"]) == 1 for row in result["items"])
    assert all(row["destinations"][0]["destination_provider"] == "TRAKT" for row in result["items"])


def test_activity_filters_and_review_state_preserve_route_records(activity_db):
    result = groups.list_groups(conn=activity_db, domain="scrobble", visibility="all", activities=True, provider="SIMKL", status="skipped")
    assert result["total"] == 14
    assert all(row["status"] == "skipped" for row in result["items"])
    assert all(len(row["destinations"]) == 1 for row in result["items"])
    selected = result["items"][0]
    groups.acknowledge_group(selected["id"], conn=activity_db)
    result = groups.list_groups(conn=activity_db, domain="scrobble", visibility="all", activities=True)
    assert result["items"][0]["acknowledged_at"] is None
    result = groups.list_groups(conn=activity_db, domain="scrobble", visibility="acknowledged", activities=True)
    assert result["total"] == 1
    assert len(result["items"][0]["destinations"]) == 1


def test_activity_uses_latest_progress_and_separates_source_profiles(activity_db):
    for instance in ("default", "family"):
        for event_type, at, progress in (("scrobble_started", 2_000_002_000, 0), ("scrobble_stopped", 2_000_002_050, 30)):
            record_events([make_event(
                domain="scrobble", event_type=event_type, created_at=at, feature="history",
                source_provider="PLEX", source_instance=instance, destination_provider="TRAKT",
                item_key="tvdb:100", session_key="playback-one", detail={"progress": progress},
                hash_extra=f"{instance}-{event_type}",
            )], conn=activity_db)
    groups.correlate(conn=activity_db)
    result = groups.list_groups(conn=activity_db, domain="scrobble", visibility="all", activities=True, feature="history")
    assert result["total"] == 2
    assert {row["source_instance"] for row in result["items"]} == {"default", "family"}
    assert all(row["progress"] == 30 for row in result["items"])


def test_activity_api_filters_destinations_before_returning_counts(activity_db, monkeypatch):
    from api import eventsAPI

    monkeypatch.setattr(eventsAPI, "load_config", lambda: {})
    monkeypatch.setattr(eventsAPI, "request_user", lambda request: {"is_admin": False})
    monkeypatch.setattr(eventsAPI, "_admin_only_hidden", lambda request: False)
    monkeypatch.setattr(eventsAPI, "_event_row_allowed", lambda cfg, request, row: row.get("destination_provider") == "TRAKT")
    monkeypatch.setattr(eventsAPI, "_list_groups", lambda **kwargs: groups.list_groups(conn=activity_db, **kwargs))
    response = eventsAPI.events_groups(domain="scrobble", activities=True, visibility="all", order="newest", limit=10, offset=10)
    payload = json.loads(response.body)
    assert payload["total"] == 14
    assert len(payload["items"]) == 4
    assert all(row["event_count"] == 1 for row in payload["items"])
    assert all(row["destinations"][0]["destination_provider"] == "TRAKT" for row in payload["items"])


def test_activity_page_only_decodes_selected_delivery_payloads(activity_db, monkeypatch):
    groups.list_groups(conn=activity_db, domain="scrobble", activities=True, visibility="all")
    decoded = []
    original = groups.json.loads

    def decode(value, *args, **kwargs):
        decoded.append(value)
        return original(value, *args, **kwargs)

    monkeypatch.setattr(groups.json, "loads", decode)
    result = groups.list_groups(conn=activity_db, domain="scrobble", activities=True, visibility="all", limit=2, offset=10)
    assert result["total"] == 14
    assert len(result["items"]) == 2
    assert len(decoded) == 6
    decoded.clear()
    result = groups.list_groups(conn=activity_db, domain="scrobble", activities=True, visibility="all", limit=2, offset=20)
    assert result["total"] == 14
    assert result["items"] == []
    assert decoded == []


def test_existing_archives_backfill_activity_keys_once(activity_db, monkeypatch):
    from cw_platform.event_archive.schema import apply_schema

    activity_db.execute("DROP INDEX idx_groups_activity")
    activity_db.execute("ALTER TABLE event_groups DROP COLUMN activity_hash")
    apply_schema(activity_db)
    assert activity_db.execute("SELECT COUNT(*) FROM event_groups WHERE activity_hash IS NULL").fetchone()[0] == 42
    result = groups.list_groups(conn=activity_db, domain="scrobble", activities=True, visibility="all")
    assert result["total"] == 14
    assert activity_db.execute("SELECT COUNT(*) FROM event_groups WHERE activity_hash IS NULL").fetchone()[0] == 0
    assert activity_db.execute("SELECT COUNT(DISTINCT activity_hash) FROM event_groups").fetchone()[0] == 14

    def unexpected_hash(row):
        pytest.fail("Paging an existing archive must reuse persisted activity keys")

    monkeypatch.setattr(groups, "_activity_hash", unexpected_hash)
    result = groups.list_groups(conn=activity_db, domain="scrobble", activities=True, visibility="all", limit=10, offset=10)
    assert result["total"] == 14
    assert len(result["items"]) == 4


def test_activity_pages_remain_consistent_in_both_orders(activity_db):
    pages = {}
    for order in ("newest", "oldest"):
        pages[order] = [row for offset in (0, 10) for row in groups.list_groups(
            conn=activity_db, domain="scrobble", activities=True, visibility="all", order=order, limit=10, offset=offset,
        )["items"]]
        assert len({row["last_event_at"] for row in pages[order]}) == 14
    assert [row["last_event_at"] for row in pages["newest"]] == [row["last_event_at"] for row in reversed(pages["oldest"])]

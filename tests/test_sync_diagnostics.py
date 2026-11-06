# tests/test_sync_diagnostics.py
# CrossWatch - Shared Sync Diagnostics Tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import Mock

import pytest

from cw_platform.id_map import canonical_key
from cw_platform.matching_details import match_details
from cw_platform.orchestrator import _unresolved
from cw_platform.orchestrator._applier import _normalize
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator._pairs_oneway import _emit_item_failures
from cw_platform.reason_labels import reason_message
from providers.sync.kodi import _history
from providers.sync.kodi._common import LibraryIndex
from services.interactive_sync_export import COLUMNS, csv_row
from services.interactive_sync_report import SyncReport
from services.interactive_sync_store import ReviewStore


@pytest.mark.parametrize("provider", ["KODI", "EMBY", "JELLYFIN", "PLEX", "SILO", "TOFA", "SCROB", "UNKNOWN"])
def test_shared_ambiguity_is_not_absence_or_confirmed_copies(provider):
    message = reason_message("ambiguous", provider=provider, feature="history")
    assert "Multiple possible" in message
    assert "No item was updated" in message
    assert "duplicat" not in message.lower()
    assert message != reason_message("not_found", provider=provider, feature="history")


def test_specific_response_ambiguity_keeps_its_meaning():
    message = reason_message("simkl_write_response_ambiguous:add_count", provider="SIMKL", feature="history")
    assert "SIMKL" in message and "Multiple possible" not in message
    assert "Plex" in reason_message("resolve_ambiguous", provider="PLEX", feature="history")
    assert reason_message("future_provider_reason", provider="KODI") == ""


@pytest.mark.parametrize("code, expected", [
    ("nuvio_id_ambiguous", "Removal was skipped"),
    ("ambiguous_provider_instance", "connections"),
    ("ambiguous_watch_event", "history event"),
    ("trakt_history_event_ambiguous", "history events"),
    ("trakt_episode_title_ambiguous", "episodes"),
    ("trakt_response_ambiguous", "confirm the outcome"),
    ("trakt_added_existing_split_ambiguous", "watches were added"),
])
def test_ambiguity_explanations_distinguish_media_events_and_responses(code, expected):
    assert expected in reason_message(code)


def test_candidate_evidence_is_bounded_and_omits_private_fields():
    details = match_details({"candidates": [{"local_id": i, "title": "<movie>", "ids": {"tmdb": i, "token": "secret"},
                                            "path": "smb://user:secret@host/movie", "error": "secret", "raw": {"token": "secret"}}
                                           for i in range(15)]})
    assert details["candidate_count"] == 15
    assert len(details["candidates"]) == 10 and details["truncated"]
    assert "secret" not in json.dumps(details)
    assert match_details(details) == details
    assert match_details(None) == match_details({"candidates": "invalid"}) == {}


@pytest.mark.parametrize("stored", [{"reason": "not_found"}, {"reason": "ambiguous", "ts": 1234567890},
                                    {"reason": "kodi_x"}, {"reason": "write_failed", "hint": "mark_played_failed"}])
def test_previous_failure_survives_store_and_can_be_retried(monkeypatch, stored):
    item = {"type": "movie", "title": "Movie", "ids": {"tmdb": "123"}}
    key = canonical_key(item)
    monkeypatch.setattr(_unresolved, "load_unresolved_map", lambda *a, **kw: {key: stored})
    store = ReviewStore()
    try:
        plan = InteractivePlan(rows=store.rows)
        plan.filter("history", "KODI", "home", "add", [item])
        row = next(iter(store.rows.values()))
        assert row["reason_origin"] == "previous_attempt"
        assert row["reason_label"]
        assert row["previous_attempt"] == stored
        assert row["reason"].startswith("Previous attempt:")
        assert ". Select and apply to retry." in row["reason"]
        assert ".. Select" not in row["reason"]
        assert row["selectable"] and row["result"] == "unresolved"
        plain = InteractivePlan()
        monkeypatch.setattr(_unresolved, "load_unresolved_map", lambda *a, **kw: {})
        plain.filter("history", "KODI", "home", "add", [item])
        assert row["id"] in plain.rows
        execution = InteractivePlan(preview=False, selected={row["id"]})
        assert execution.filter("history", "KODI", "home", "add", [item]) == [item]
        assert dict(zip(COLUMNS, csv_row(row)))["reason_origin"] == "previous_attempt"
    finally:
        store.cleanup()


def test_provider_evidence_survives_persistence_and_authoritative_report(config_base, monkeypatch):
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    movie = {"movieid": 1, "title": "Movie", "uniqueid": {"tmdb": "123", "imdb": "tt1234567"}}
    index = LibraryIndex([movie, {**movie, "movieid": 2}], [], [])
    client = Mock()
    adapter = SimpleNamespace(client=client, _kodi_library_index_history=index)
    item = {"type": "movie", "title": "Movie", "ids": {"tmdb": "123"}}
    key = canonical_key(item)
    result = _history.add(adapter, [item])
    assert result["unresolved"][0]["reason"] == "ambiguous"
    assert not client.mock_calls
    events = []
    emit = lambda event, **payload: events.append(dict(event=event, **payload))
    normalized = _normalize(result, [item], "apply:add", dst="KODI", feature="history", emit=emit, instance="home")
    saved = _unresolved.load_unresolved_map("KODI", "history", cross_features=False, instance="home")[key]
    assert saved["reason"] == "ambiguous"
    assert saved["match_details"]["candidate_count"] == 2
    _unresolved.clear_unresolved("KODI", "history", [key], instance="home")
    _emit_item_failures(emit, "KODI", "history", "p1", [key], {key: item}, {}, instance="home", failure_details=normalized["failure_details"])
    store = ReviewStore()
    try:
        report = SyncReport(store, {"source": "SCROB", "target": "KODI"})
        for event in events:
            report.event(event)
        rows = store.report_page()["items"]
        assert len(rows) == 1
        row = rows[0]
        assert row["reason"] == "ambiguous" and row["result"] == "failed"
        assert row["reason_label"] == "Multiple matching items found"
        assert row["reason_origin"] == "current_attempt" and row["instance"] == "home"
        assert row["explanation"] == reason_message("ambiguous", provider="KODI", feature="history")
        assert {candidate["local_id"] for candidate in row["match_details"]["candidates"]} == {"1", "2"}
    finally:
        store.cleanup()


def test_legacy_report_without_details_and_source_show_ids():
    store = ReviewStore()
    try:
        SyncReport(store).event(dict(event="archive:item_failures", provider="KODI", feature="history", items=[
            dict(key="episode", item={"type": "episode", "show_ids": {"tvdb": "42"}}, reason="ambiguous")]))
        row = store.report_page()["items"][0]
        assert row["explanation"] and "match_details" not in row
        assert row["item"]["show_ids"] == {"tvdb": "42"}
    finally:
        store.cleanup()


def test_reason_code_survives_provider_exception_hint(config_base, monkeypatch):
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    item = {"type": "movie", "ids": {"tmdb": "123"}}
    events = []
    result = {"ok": False, "unresolved": [{"item": item, "reason": "write_failed", "hint": "Transport exception details"}]}
    _normalize(result, [item], "apply:add", dst="KODI", feature="history",
               emit=lambda event, **payload: events.append(dict(event=event, **payload)))
    event = next(row for row in events if row["event"] == "apply:unresolved")
    assert event["items"][0]["reason"] == "write_failed"
    assert event["items"][0]["explanation"] == reason_message("write_failed")
    assert "hint" not in event["items"][0]
    saved = _unresolved.load_unresolved_map("KODI", "history", cross_features=False)[canonical_key(item)]
    assert saved["reason"] == "write_failed"
    assert "hint" not in saved


@pytest.mark.parametrize("fields,expected", [
    ({"status": "outside_supported_progress_window"}, "outside_supported_progress_window"),
    ({"status": "http:503", "error": "Server prose"}, "http:503"),
    ({"status": "http:503", "reason": "write_failed"}, "write_failed"),
    ({"status": "http:503", "hint": "mark_played_failed"}, "mark_played_failed"),
    ({"status": "unresolved", "error": "legacy_error"}, "legacy_error"),
    ({"status": 503, "error": "legacy_error"}, "legacy_error"),
    ({"status": "future_status", "error": "legacy_error"}, "legacy_error"),
])
def test_known_status_failure_codes_survive_normalization(config_base, monkeypatch, fields, expected):
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    item = {"type": "movie", "ids": {"tmdb": "550"}}
    key = canonical_key(item)
    result = _normalize({"ok": False, "unresolved_keys": [key], "unresolved": [{"key": key, **fields}]},
                        [item], "apply:add", dst="SCROB", feature="progress", emit=lambda *a, **kw: None)
    assert result["failure_details"][key]["reason"] == expected


def test_scrob_progress_failure_reaches_normalized_event(config_base, monkeypatch):
    from providers.sync.scrob import _progress

    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    item = {"type": "movie", "ids": {"tmdb": "550"}, "progress_percent": 95, "duration_ms": 100000}
    events = []
    result = _normalize(_progress.add(None, [item]), [item], "apply:add", dst="SCROB", feature="progress",
                        emit=lambda event, **payload: events.append(dict(event=event, **payload)))
    code = "outside_supported_progress_window"
    assert result["failure_details"][canonical_key(item)]["reason"] == code
    event = next(row for row in events if row["event"] == "apply:unresolved")
    assert event["items"][0]["reason"] == code
    assert event["items"][0]["explanation"] == reason_message(code, provider="SCROB", feature="progress")


@pytest.mark.parametrize("provider", ["EMBY", "JELLYFIN"])
@pytest.mark.parametrize("reason,hint", [("write_failed", "mark_played_failed"),
                                        ("write_failed", "unmark_played_failed"),
                                        ("readback_mismatch", "readback_not_played")])
def test_specific_provider_hint_survives_generic_reason(config_base, monkeypatch, provider, reason, hint):
    from api.syncAPI import _format_unresolved_line

    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    item = {"type": "movie", "title": "Movie", "ids": {"tmdb": "123"}}
    key = canonical_key(item)
    events = []
    emit = lambda event, **payload: events.append(dict(event=event, **payload))
    result = _normalize({"ok": False, "unresolved": [{"item": item, "reason": reason, "hint": hint}]},
                        [item], "apply:add", dst=provider, feature="history", emit=emit)
    saved = _unresolved.load_unresolved_map(provider, "history", cross_features=False)[key]
    assert (saved["reason"], saved["hint"]) == (reason, hint)
    assert _unresolved.load_unresolved_pending(provider, "history")[0]["hint"] == hint
    assert _unresolved.load_unresolved_items(provider)[0]["hint"] == hint
    sample = next(row for row in events if row["event"] == "apply:unresolved")
    assert sample["reason_counts"] == {reason: 1}
    assert sample["items"][0]["hint"] == hint
    _emit_item_failures(emit, provider, "history", "p1", [key], {key: item}, {}, failure_details=result["failure_details"])
    store = ReviewStore()
    try:
        report = SyncReport(store)
        for event in events:
            report.event(event)
        row = store.report_page()["items"][0]
        assert (row["reason"], row["hint"]) == (reason, hint)
        assert row["reason_label"] != reason
        assert row["explanation"] == reason_message(hint)
        assert f"[{reason}; {hint}]" in _format_unresolved_line(row)
        assert reason_message(hint) in _format_unresolved_line(row)
    finally:
        store.cleanup()


@pytest.mark.parametrize("provider", ["TRAKT", "SIMKL", "UNKNOWN", None])
def test_generic_not_found_does_not_assume_a_library(provider):
    assert "librar" not in reason_message("not_found", provider=provider).lower()
    assert "selected destination libraries" in reason_message("not_found", provider="KODI")


def test_normal_log_uses_shared_message_and_keeps_code(config_base):
    from api.syncAPI import _format_unresolved_line

    line = _format_unresolved_line({"item": {"title": "Movie"}, "provider": "KODI", "feature": "history", "reason": "ambiguous"})
    assert reason_message("ambiguous", provider="KODI", feature="history") in line
    assert line.endswith("[ambiguous]")


def test_current_failure_does_not_reuse_saved_ambiguity(config_base, monkeypatch):
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    item = {"type": "movie", "ids": {"tmdb": "123"}, "_cw_unresolved_hint": "ambiguous"}
    key = canonical_key(item)
    _unresolved.record_unresolved("KODI", "history", [item])
    events = []
    _emit_item_failures(lambda event, **payload: events.append(payload), "KODI", "history", "p1", [key], {key: item}, {}, failure_details={})
    assert events[0]["items"][0]["reason"] == "apply:add:failed"
    assert not events[0]["items"][0]["match_details"]


def test_failure_evidence_survives_chunk_aggregation(config_base, monkeypatch):
    from cw_platform.orchestrator._applier import _apply_chunked

    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base)
    items = [{"type": "movie", "ids": {"tmdb": str(n)}} for n in (1, 2)]
    def write(chunk):
        return {"ok": True, "unresolved": [{"item": chunk[0], "reason": "ambiguous", "match_details":
                {"candidate_count": 2, "candidates": [{"local_id": 10}, {"local_id": 20}]}}]}
    result = _apply_chunked("apply:add", items=items, call=write, dst="KODI", feature="history",
                            emit=lambda *a, **kw: None, dbg=lambda *a, **kw: None, chunk_size=1, chunk_pause_ms=0)
    for item in items:
        assert result["failure_details"][canonical_key(item)]["match_details"]["candidate_count"] == 2


def test_candidate_details_preserve_anime_ids():
    details = match_details({"candidates": [{"ids": {"kitsu": "123", "anidb": "456"}, "show_ids": {"kitsu": "789", "anidb": "101"}}]})
    assert details["candidates"][0]["ids"] == {"kitsu": "123", "anidb": "456"}
    assert details["candidates"][0]["show_ids"] == {"kitsu": "789", "anidb": "101"}

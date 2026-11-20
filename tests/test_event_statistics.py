# tests/test_event_statistics.py
# CrossWatch - Complete activity statistics and profile isolation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace

import pytest

from cw_platform.event_archive import stats
from cw_platform.event_archive.db import connect
from cw_platform.event_archive.recorder import make_event, record_events, record_run_started, record_run_finished


def event(conn, kind, at, domain="scrobble", **kwargs):
    detail = kwargs.pop("detail", {})
    instance = kwargs.pop("source_instance", "default")
    return record_events([make_event(
        domain=domain, event_type=kind, created_at=at,
        source_provider="PLEX", source_instance=instance, detail=detail,
        hash_extra=f"{kind}-{at}-{kwargs}", **kwargs,
    )], conn=conn)


@pytest.fixture
def archive():
    conn = connect(":memory:")
    for index, (name, summary) in enumerate((("ok", {}), ("bad", {"errors": 1}), ("active", None), ("warn", {"unresolved": 1}))):
        at = 1100 + index * 100
        record_run_started(name, started_at=at, conn=conn)
        event(conn, "sync_run_started", at, "sync", run_id=name, operation="run")
        if summary is not None:
            record_run_finished(name, finished_at=at + 10, summary=summary, conn=conn)
            event(conn, "sync_run_finished", at + 10, "sync", run_id=name, operation="run", detail=summary)
        if name in ("bad", "warn"):
            event(conn, "write_failed" if name == "bad" else "unresolved_recorded", at + 5, "sync", run_id=name, destination_provider="TRAKT", item_key=name, severity="error" if name == "bad" else "warn", reason_code="provider_error")
    for index, provider in enumerate(("CROSSWATCH", "TRAKT", "SIMKL")):
        event(conn, "rating_skipped" if provider == "SIMKL" else "rating_applied", 1500 + index, feature="ratings", session_key="rating-one", item_key="tvdb:1", destination_provider=provider, source_kind="webhook")
    event(conn, "rating_skipped", 1510, feature="ratings", session_key="rating-two", item_key="tvdb:2", destination_provider="SIMKL", source_kind="webhook")
    for provider in ("TRAKT", "CROSSWATCH"):
        event(conn, "scrobble_completed", 1520, feature="history", session_key="watched", item_key="tvdb:3", destination_provider=provider, source_kind="watcher")
        event(conn, "scrobble_playback", 1530, feature="history", session_key="stopped", item_key="tvdb:4", destination_provider=provider, source_kind="watcher", detail={"state": "stopped", "progress": 30})
    event(conn, "scrobble_failed", 1540, feature="history", session_key="failed", item_key="tvdb:5", destination_provider="TRAKT", source_kind="webhook", severity="error", reason_code="provider_error")
    for domain, at in (("capture", 1550), ("backup", 1570)):
        event(conn, f"{domain}_completed", at, domain, feature=domain)
        event(conn, f"{domain}_failed", at + 10, domain, feature=domain, severity="error", reason_code=f"{domain}_failed")
    event(conn, "audit_login", 1590, "audit")
    event(conn, "audit_login_failed", 1600, "audit", severity="error", reason_code="login_failed")
    event(conn, "capture_completed", 900, "capture", feature="capture")
    yield conn
    conn.close()


def calculate(conn, **kwargs):
    return stats.statistics(conn=conn, since=1000, until=2000, bucket=100, **kwargs)


def test_all_domains_and_grouped_deliveries_are_counted_once(archive):
    result = calculate(archive)
    values = {key: row["value"] for key, row in result["kpis"].items()}
    assert values["activities"] == 15
    assert {key: values[key] for key in ("sync_runs", "scrobbles", "ratings", "captures", "backups", "audits")} == {"sync_runs": 4, "scrobbles": 3, "ratings": 2, "captures": 2, "backups": 2, "audits": 2}
    assert result["kpis"]["captures"] == {"value": 2, "prev": 1, "delta": 100.0}
    assert values["failures"] == 5
    assert values["skipped"] == 1
    assert values["delivery_skips"] == 2
    assert values["success_rate"] == 50.0
    assert sum(row["value"] for row in result["types"]) == 15
    assert sum(row["value"] for row in result["outcomes"]) == 15
    outcomes = {row["key"]: row["value"] for row in result["outcomes"]}
    assert outcomes == {"completed": 6, "warning": 1, "failed": 5, "active": 1, "skipped": 1, "stopped": 1}
    assert sum(sum(row[k] for k in ("sync", "watching", "ratings", "capture", "backup", "audit")) for row in result["trend"]) == 15
    assert sum(row["webhook"] for row in result["trend"]) == 3
    assert sum(row["watcher"] for row in result["trend"]) == 2
    assert {row["reason"] for row in result["failure_reasons"]} >= {"capture_failed", "backup_failed", "login_failed"}
    simkl = next(row for row in result["routes"] if row["destination"] == "SIMKL")
    assert simkl["volume"] == simkl["skipped"] == 2
    assert simkl["success_rate"] is None


def test_permissions_filter_destinations_before_grouping(archive):
    result = calculate(archive, row_filter=lambda row: row["domain"] not in ("audit", "backup") and row.get("destination_provider") != "SIMKL")
    assert result["kpis"]["ratings"]["value"] == 1
    assert result["kpis"]["delivery_skips"]["value"] == 0
    assert result["kpis"]["backups"]["value"] == result["kpis"]["audits"]["value"] == 0
    assert all(row["destination"] != "SIMKL" for row in result["routes"])
    assert all(row["reason"] not in ("backup_failed", "login_failed") for row in result["failure_reasons"])
    assert calculate(archive, row_filter=lambda row: True) == calculate(archive)


def test_empty_and_skip_only_periods_have_no_success_rate():
    conn = connect(":memory:")
    try:
        assert calculate(conn)["kpis"]["success_rate"]["value"] is None
        event(conn, "rating_skipped", 1100, feature="ratings", destination_provider="SIMKL")
        result = calculate(conn)
        assert result["kpis"]["activities"]["value"] == 1
        assert result["kpis"]["success_rate"]["value"] is None
        assert result["kpis"]["failures"]["value"] == 0
        assert result["trend"][0]["rate"] is None
    finally:
        conn.close()


def test_statistics_cache_tracks_writes_and_connections(archive, monkeypatch):
    monkeypatch.setattr(stats, "get_conn", lambda: archive)
    before = stats.statistics(since=1000, until=2000)
    event(archive, "backup_completed", 1700, "backup", feature="backup")
    after = stats.statistics(since=1000, until=2000)
    assert after["kpis"]["activities"]["value"] == before["kpis"]["activities"]["value"] + 1
    other = connect(":memory:")
    try:
        assert calculate(other)["kpis"]["activities"]["value"] == 0
    finally:
        other.close()
        stats._CACHE.clear()


def test_scoped_api_uses_shared_statistics_and_monitor_filter(archive, monkeypatch):
    from api import eventsAPI

    monkeypatch.setattr(eventsAPI, "get_conn", lambda: archive)
    monkeypatch.setattr(eventsAPI, "load_config", lambda: {})
    monkeypatch.setattr(eventsAPI, "_event_row_allowed", lambda cfg, request, row: row["domain"] not in ("audit", "backup"))
    monkeypatch.setattr(eventsAPI, "_admin_only_hidden", lambda request: True)
    result = json.loads(eventsAPI.events_statistics(since=1000, until=2000, bucket="hour", request=None).body)
    assert result["kpis"]["captures"]["value"] == 2
    assert result["kpis"]["backups"]["value"] == result["kpis"]["audits"]["value"] == 0


def test_statistics_respect_real_profile_and_monitor_permissions(archive, monkeypatch):
    from api import eventsAPI
    from cw_platform.provider_instances import DEFAULT_OVERVIEW_SCOPE

    cfg = {"plex": {"instances": {"PLEX-P01": {}}}, "user_profiles": {"alice": {"instances": {"PLEX": ["PLEX-P01"]}}}}
    monkeypatch.setattr(eventsAPI, "get_conn", lambda: archive)
    monkeypatch.setattr(stats, "get_conn", lambda: archive)
    monkeypatch.setattr(eventsAPI, "load_config", lambda: cfg)
    monkeypatch.setattr("cw_platform.config_base.load_config", lambda: cfg)
    event(archive, "capture_completed", 1700, "capture", feature="capture", source_instance="PLEX-P01")

    def response(user, scope="all"):
        request = SimpleNamespace(state=SimpleNamespace(cw_user=user), cookies={}, query_params={"user_profile": scope}, headers={})
        return json.loads(eventsAPI.events_statistics(since=1000, until=2000, bucket="hour", request=request).body)["kpis"]

    admin = {"is_admin": True}
    assert response(admin)["captures"]["value"] == 3
    assert response(admin, DEFAULT_OVERVIEW_SCOPE)["captures"]["value"] == 2
    assert response(admin, "alice")["captures"]["value"] == 1
    managed = response({"is_admin": False, "profile_id": "alice"})
    assert managed["activities"]["value"] == managed["captures"]["value"] == 1
    assert managed["backups"]["value"] == managed["audits"]["value"] == 0
    monitor = response({"is_admin": True, "monitor_view": True})
    assert monitor["captures"]["value"] == 3
    assert monitor["backups"]["value"] == monitor["audits"]["value"] == 0


def test_period_boundaries_and_source_instances_remain_distinct():
    conn = connect(":memory:")
    try:
        for at in (999, 1000, 1999, 2000):
            event(conn, "backup_completed", at, "backup", feature="backup")
        for instance in ("default", "PLEX-P01"):
            event(conn, "rating_applied", 1500, feature="ratings", source_instance=instance, session_key="same", item_key="tvdb:1", destination_provider="TRAKT")
        result = calculate(conn)
        assert result["kpis"]["backups"]["value"] == 2
        assert result["kpis"]["backups"]["prev"] == 1
        assert result["kpis"]["ratings"]["value"] == 2
    finally:
        conn.close()

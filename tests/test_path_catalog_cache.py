# tests/test_path_catalog_cache.py
# CrossWatch - Persistent path catalogue isolation and refresh tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from types import SimpleNamespace
from datetime import datetime
from email.utils import formatdate
import sqlite3
from xml.etree.ElementTree import Element

import pytest

from cw_platform.local_db import close_conn, get_conn
from providers.sync import _path_matching as paths


@pytest.fixture
def catalogue(config_base, monkeypatch):
    clock = [1000000.0]
    monkeypatch.setattr(paths.time, "time", lambda: clock[0])
    state = {"stamp": "2026-01-01", "count": 1, "full": 0, "checks": 0, "fail": False, "malformed": False, "delta_supported": True}

    def get(url, *, params):
        if params["Limit"] == 1:
            state["checks"] += 1
            body = {"TotalRecordCount": state["count"], "Items": [{"Id": "1", "DateLastSaved": state["stamp"]}]}
            if "MinDateLastSaved" in params and state.get("delta_supported"):
                since = datetime.fromisoformat(params["MinDateLastSaved"]).timestamp()
                body["Items"] = [{"Id": "1"}] if state.get("changed") and since <= clock[0] + state.get("skew", 0) else []
        else:
            state["full"] += 1
            body = {"Items": [{"Id": "1", "Type": "Movie", "Path": "/movies/A/A.mkv", "ProviderIds": {"Tmdb": "1"}}]}
            if state["malformed"]:
                body = {}
        return SimpleNamespace(status_code=503 if state["fail"] else 200, json=lambda: body, headers={"Date": formatdate(clock[0] + state.get("skew", 0), usegmt=True)})

    def adapter(**kw):
        cfg = {"server": "http://test", "user_id": "user", "access_token": "secret", "history_libraries": [], "progress_libraries": []}
        cfg.update(kw)
        return SimpleNamespace(path_matching=True, cfg=SimpleNamespace(**cfg), client=SimpleNamespace(get=get))

    yield state, clock, adapter
    close_conn()


def test_reuses_across_instances_features_and_database_reopen(catalogue):
    state, _, adapter = catalogue
    first = paths._catalog(adapter(), "EMBY", "history")
    close_conn()
    second = paths._catalog(adapter(), "EMBY", "progress")
    assert second.items == first.items
    assert state["full"] == 1
    assert state["checks"] == 4
    row = get_conn().execute("SELECT * FROM media_path_catalogues").fetchone()
    assert "secret" not in str(tuple(row))


@pytest.mark.parametrize("change", [{"server": "http://other"}, {"user_id": "other"}, {"access_token": "other"}, {"history_libraries": ["library"]}])
def test_scope_changes_never_reuse_another_catalogue(catalogue, change):
    state, _, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    paths._catalog(adapter(**change), "EMBY", "history")
    assert state["full"] == 2


@pytest.mark.parametrize("field,value", [("changed", True), ("count", 2)])
def test_metadata_or_membership_change_refreshes(catalogue, field, value):
    state, _, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    state[field] = value
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


def test_daily_full_refresh_even_with_unchanged_revision(catalogue):
    state, clock, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    clock[0] += 86401
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


def test_missing_server_revision_has_short_bounded_lifetime(catalogue):
    state, clock, adapter = catalogue
    state["delta_supported"] = False
    paths._catalog(adapter(), "EMBY", "history")
    clock[0] += 299
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 1
    clock[0] += 2
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


def test_failure_does_not_serve_stale_data_or_replace_complete_cache(catalogue):
    state, _, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    before = tuple(get_conn().execute("SELECT * FROM media_path_catalogues").fetchone())
    state["fail"] = True
    with pytest.raises(RuntimeError):
        paths._catalog(adapter(), "EMBY", "history")
    assert tuple(get_conn().execute("SELECT * FROM media_path_catalogues").fetchone()) == before
    state.update(fail=False, changed=True, malformed=True)
    with pytest.raises(ValueError):
        paths._catalog(adapter(), "EMBY", "history")
    assert tuple(get_conn().execute("SELECT * FROM media_path_catalogues").fetchone()) == before


def test_corrupt_cache_is_rebuilt(catalogue):
    state, _, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    conn = get_conn()
    with conn:
        conn.execute("UPDATE media_path_catalogues SET items_json='bad json'")
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


def test_clock_rollback_rebuilds(catalogue):
    state, clock, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    clock[0] -= 10
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


def test_supported_delta_filter_reuses_then_refreshes_without_saved_timestamp(catalogue):
    state, clock, adapter = catalogue
    state.update(stamp=None, delta_supported=True)
    paths._catalog(adapter(), "EMBY", "history")
    clock[0] += 3600
    paths._catalog(adapter(), "EMBY", "progress")
    assert state["full"] == 1
    state["changed"] = True
    clock[0] += 3600
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2
    state["changed"] = False
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


@pytest.mark.parametrize("signal", ["updatedAt", "scannedAt", "contentChangedAt"])
def test_plex_checks_fresh_library_revision_and_scoped_token(config_base, monkeypatch, signal):
    from providers.sync.plex import _common as common

    monkeypatch.setattr(common, "home_scope_enter", lambda _: (False, False, None, None))
    monkeypatch.setattr(common, "home_scope_exit", lambda *_: None)
    monkeypatch.setattr(common, "plex_feature_library_ids", lambda *a: set())
    monkeypatch.setattr(common, "normalize", lambda row: {"type": "movie", "media_paths": ["/movies/A/A.mkv"]})
    counts = {"reload": 0, "full": 0}
    def all_items(**kwargs):
        counts["full"] += 1
        return [SimpleNamespace(ratingKey="1")]
    section = SimpleNamespace(key=1, type="movie", updatedAt="one", uuid="library", all=all_items, _data=Element("Directory", scannedAt="one", contentChangedAt="one"))
    library = SimpleNamespace(sections=lambda: [section], reload=lambda: counts.update(reload=counts["reload"] + 1))
    server = SimpleNamespace(_baseurl="http://plex", _token="token", library=library)
    def adapter():
        return SimpleNamespace(cfg=SimpleNamespace(account_id=1), client=SimpleNamespace(server=server))
    paths._catalog(adapter(), "PLEX", "history")
    paths._catalog(adapter(), "PLEX", "progress")
    assert counts == {"reload": 2, "full": 1}
    if signal == "updatedAt":
        section.updatedAt = "two"
    else:
        section._data.set(signal, "two")
    paths._catalog(adapter(), "PLEX", "history")
    assert counts["full"] == 2
    server._token = "other-user-token"
    paths._catalog(adapter(), "PLEX", "history")
    assert counts["full"] == 3
    monkeypatch.setattr(common, "home_scope_enter", lambda _: (True, False, None, None))
    with pytest.raises(RuntimeError, match="home_scope_not_applied"):
        paths._catalog(adapter(), "PLEX", "history")


def test_truncated_catalogue_is_never_persisted(catalogue):
    _, _, adapter = catalogue
    ad = adapter()
    def get(url, *, params):
        if params["Limit"] == 1:
            body = {"Items": [{"Id": "1", "DateLastSaved": "date"}], "TotalRecordCount": 10}
        else:
            body = {"Items": [], "TotalRecordCount": 10}
        return SimpleNamespace(status_code=200, json=lambda: body)
    ad.client.get = get
    with pytest.raises(ValueError, match="incomplete_response"):
        paths._catalog(ad, "EMBY", "history")
    assert get_conn().execute("SELECT COUNT(*) FROM media_path_catalogues").fetchone()[0] == 0


@pytest.mark.parametrize("operation", ["load", "save"])
def test_database_failure_keeps_live_scan_usable(catalogue, monkeypatch, operation):
    state, _, adapter = catalogue
    def fail(*args):
        raise sqlite3.OperationalError("database is locked")
    monkeypatch.setattr(paths.persisted, operation, fail)
    assert paths._catalog(adapter(), "EMBY", "history").items
    assert state["full"] == 1


def test_revision_failure_forces_live_scan_instead_of_stale_cache(catalogue):
    state, _, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    ad = adapter()
    original = ad.client.get
    ad.client.get = lambda url, *, params: SimpleNamespace(status_code=500) if params["Limit"] == 1 else original(url, params=params)
    assert paths._catalog(ad, "EMBY", "history").items
    assert state["full"] == 2


@pytest.mark.parametrize("total", [0, 1, 500])
def test_full_page_does_not_trust_understated_total(catalogue, total):
    _, _, adapter = catalogue
    ad = adapter()
    original = ad.client.get
    def get(url, *, params):
        if params["Limit"] == 1:
            return original(url, params=params)
        start = params["StartIndex"]
        rows = [{"Id": str(i), "Type": "Movie", "Path": f"/movies/{i}/{i}.mkv"} for i in range(start, min(start + 500, 501))]
        return SimpleNamespace(status_code=200, json=lambda: {"Items": rows, "TotalRecordCount": total})
    ad.client.get = get
    assert len(paths._catalog(ad, "EMBY", "history").items) == 501


@pytest.mark.parametrize("skew", [-3600, 3600])
def test_delta_uses_server_clock_when_local_clock_differs(catalogue, skew):
    state, clock, adapter = catalogue
    state["skew"] = skew
    paths._catalog(adapter(), "EMBY", "history")
    clock[0] += 300
    state["changed"] = True
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2


def test_delta_filter_is_used_even_when_saved_timestamp_is_returned(catalogue):
    state, clock, adapter = catalogue
    paths._catalog(adapter(), "EMBY", "history")
    clock[0] += 300
    state["changed"] = True
    paths._catalog(adapter(), "EMBY", "history")
    assert state["full"] == 2
    assert state["stamp"] == "2026-01-01"


def test_plex_updated_time_alone_is_not_a_content_signal():
    section = SimpleNamespace(key=1, type="movie", updatedAt="one", uuid="library")
    library = SimpleNamespace(sections=lambda: [section], reload=lambda: None)
    adapter = SimpleNamespace(client=SimpleNamespace(server=SimpleNamespace(library=library)))
    assert paths._revision(adapter, "PLEX", set()) is None


def test_maintenance_clear_removes_catalogue_and_invalidates_adapter(catalogue, config_base, monkeypatch):
    from api import maintenanceAPI
    from cw_platform.orchestrator._state_store import StateStore

    state, _, adapter = catalogue
    ad = adapter()
    paths._catalog(ad, "EMBY", "history")
    store = StateStore(config_base)
    store.save_state({"providers": {"EMBY": {"history": {"baseline": {"items": {"1": {"title": "A"}}}}}}})
    baseline = store.load_state()
    monkeypatch.setattr(maintenanceAPI, "_cw", lambda: (config_base / "cache", config_base, config_base / ".cw_state", None, None, None))
    preview = maintenanceAPI.maintenance_action_status("cache")
    metrics = {item["label"]: item["value"] for item in preview["metrics"]}
    assert metrics["Path catalogues"] == 1
    assert metrics["Path catalogue storage"] > 0
    result = maintenanceAPI.clear_cache()
    assert result["before"]["path_catalogues"]["count"] == 1
    assert result["after"]["path_catalogues"] == {"count": 0, "bytes": 0}
    assert result["summary"]["removed_items"] == 1
    assert store.load_state() == baseline
    paths._catalog(ad, "EMBY", "history")
    assert state["full"] == 2

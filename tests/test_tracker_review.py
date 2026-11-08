# tests/test_tracker_review.py
# CrossWatch - Shared interactive import review and remembered sources
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from unittest.mock import Mock

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import interactiveSyncAPI, syncAPI, trackerAPI
from cw_platform.tracker_store import TrackerStore
from cw_platform.orchestrator._state_store import StateStore
from providers.sync._mod_CROSSWATCH import OPS
from services import interactive_sync, tracker_import
from test_interactive_sync_features import feature_item, feature_setup
from test_tracker_import import cfg, fake_sources


@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings", "progress", "collection"])
def test_review_only_writes_selected_changes_to_the_selected_tracker(config_base, monkeypatch, feature):
    cfg, source, other = feature_setup(config_base, monkeypatch, feature, [feature_item(feature, 1), feature_item(feature, 2)], [])
    cfg["crosswatch"] = {"root_dir": str(config_base / "default"), "instances": {"desk": {"root_dir": str(config_base / "desk")}}}
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": source, "DST": other, "CROSSWATCH": OPS})
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    monkeypatch.setattr(source, "build_index", Mock(wraps=source.build_index))
    pair = tracker_import.pair_for("SRC", "default", "desk", [feature])
    session = interactive_sync.Session(pair_id=pair["id"], owner="local", import_pair=pair)
    session.pair = pair
    target = TrackerStore(config_base / "desk")
    target.write_state(feature, {"existing": feature_item(feature, 9)})
    TrackerStore(config_base / "default").write_state(feature, {})
    before = target.read_current(feature)
    original_cfg = deepcopy(cfg)
    baselines = deepcopy(StateStore(config_base).load_state())
    try:
        interactive_sync.refresh(session, cfg, {})
        assert session.status == "review", session.message
        assert len(session.plan.rows) == 2
        assert all(r["provider"] == "CROSSWATCH" and r["operation"] == "add" for r in session.plan.rows.values())
        assert not source.add_calls and not other.add_calls
        assert target.read_current(feature) == before
        assert target.job("import_sources") is None
        selected = {rid for rid, row in session.plan.rows.items() if row["item"]["title"] == "Movie 2"}
        assert len(selected) == 1
        interactive_sync.apply(session, cfg, selected)
        assert session.status == "complete", session.message
        assert {r["title"] for r in target.read_current(feature)["items"].values()} == {"Movie 9", "Movie 2"}
        assert not TrackerStore(config_base / "default").read_current(feature)["items"]
        assert not source.add_calls and not other.add_calls
        assert source.build_index.call_count == 1
        assert cfg == original_cfg
        assert StateStore(config_base).load_state() == baselines
        assert target.job("import_sources"), session.summary
        assert target.job("import_sources")["sources"][0]["features"] == [feature]
        assert session.report
    finally:
        session.close()


def test_empty_import_review_does_not_automatically_apply(config_base, monkeypatch):
    cfg, source, target = feature_setup(config_base, monkeypatch, "watchlist", [], [])
    pair = tracker_import.pair_for("SRC", "default", "default", ["watchlist"])
    pair["target"] = "DST"
    session = interactive_sync.Session(pair_id=pair["id"], owner="local", import_pair=pair)
    applied = Mock()
    monkeypatch.setattr(interactive_sync, "apply", applied)
    try:
        interactive_sync.refresh(session, cfg, {})
        assert session.status == "review"
        assert not session.plan.rows
        applied.assert_not_called()
        assert not source.add_calls and not target.add_calls
    finally:
        session.close()


def test_preview_api_creates_private_review_without_persisting_a_pair(cfg, fake_sources, monkeypatch):
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(interactiveSyncAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    launch = Mock()
    monkeypatch.setattr(interactiveSyncAPI, "launch", launch)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    app.include_router(interactiveSyncAPI.router)
    client = TestClient(app)
    original = deepcopy(cfg)
    response = client.post("/api/tracker/import/preview", json={"source": "TRAKT", "provider_instance": "CW-P01", "features": ["history"]})
    assert response.status_code == 200, response.text
    session = interactive_sync.SESSIONS[response.json()["session"]["id"]]
    try:
        assert session.import_pair["target_instance"] == "CW-P01"
        assert response.json()["session"]["tracker_import"] is True
        launch.assert_called_once()
        assert cfg == original
        assert client.get(f"/api/interactive-sync/{session.id}").status_code == 200
        with pytest.raises(HTTPException) as error:
            interactiveSyncAPI.mapping_row(session, "row")
        assert error.value.status_code == 400
        monkeypatch.setattr("cw_platform.access_policy.user_can_access_instance", lambda *_: False)
        assert client.get(f"/api/interactive-sync/{session.id}").status_code == 403
        assert TrackerStore(cfg["crosswatch"]["instances"]["CW-P01"]["root_dir"]).query("history")[0] == 0
    finally:
        session.close()
        interactive_sync.SESSIONS.pop(session.id, None)


def test_remembered_sources_merge_successful_features_and_stay_tracker_scoped(cfg):
    cfg["trakt"] = {"instances": {"TRAKT-P01": {}}}
    target = TrackerStore(cfg["crosswatch"]["instances"]["CW-P01"]["root_dir"])
    tracker_import.remember_source(target, "TRAKT", "TRAKT-P01", {"features": {"history": {"added": 2}}})
    tracker_import.remember_source(target, "TRAKT", "TRAKT-P01", {"features": {"ratings": {"updated": 1}, "watchlist": {"added": 0}}})
    assert len(target.job("import_sources")["sources"]) == 1
    for feature in ("history", "ratings"):
        assert [(r["provider"], r["instance"]) for r in tracker_import.origin_sources(cfg, "CW-P01", feature)] == [("TRAKT", "TRAKT-P01")]
    assert tracker_import.origin_sources(cfg, "CW-P01", "watchlist") == []
    assert tracker_import.origin_sources(cfg, "default", "history") == []


def test_origin_reuses_editor_matching_and_filters_import_sources_by_access(cfg, monkeypatch):
    from starlette.requests import Request
    from api import editorAPI
    from services import editor_origin

    item = {"type": "movie", "title": "Heat", "ids": {"tmdb": "949"}}
    cfg["trakt"] = {"instances": {"TRAKT-P01": {}}}
    store = TrackerStore(cfg["crosswatch"]["instances"]["CW-P01"]["root_dir"])
    tracker_import.remember_source(store, "TRAKT", "TRAKT-P01", {"features": {"history": {"added": 1}}})
    monkeypatch.setattr(editorAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(editorAPI, "_load_policy", lambda: {})
    monkeypatch.setattr(editorAPI, "_load_current_state_features", lambda _: {})
    monkeypatch.setattr(editorAPI, "_load_state_items", lambda *args, **kwargs: {"movie": item})
    request = Request({"type": "http", "headers": []})
    payload = editor_origin.OriginRequest(provider="CROSSWATCH", instance="CW-P01", feature="history", original=item)
    result = editor_origin.item_origin(payload, request)
    assert result["import_sources"] == [{"provider": "TRAKT", "instance": "TRAKT-P01"}]
    assert [row["title"] for row in result["sources"]] == ["Heat"]
    monkeypatch.setattr(editor_origin, "user_can_access_instance", lambda *args: args[2] == "CROSSWATCH")
    denied = editor_origin.item_origin(payload, request)
    assert denied["sources"] == [] and denied["import_sources"] == []


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("rewatches", [False, True])
def test_corrected_history_is_not_reproposed_but_other_watches_still_import(config_base, monkeypatch, mode, rewatches):
    from cw_platform.history_events import history_sync_key
    from cw_platform.tracker_history_identity import TYPE_ORIGINS, event_identities
    from cw_platform.orchestrator import Orchestrator
    from cw_platform.orchestrator._interactive import InteractivePlan

    original = {"type": "episode", "title": "Pilot", "series_title": "Wrong show", "season": 1, "episode": 2,
                "show_ids": {"tmdb": "12"}, "watched_at": "2024-01-01T12:00:00Z"}
    cfg, source, _ = feature_setup(config_base, monkeypatch, "history", [original], [], mode)
    source.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present", "history": {"rewatches": True}}
    cfg["crosswatch"] = {"connected": True, "root_dir": str(config_base / "tracker")}
    pair = cfg["pairs"][0]
    pair.update(target="CROSSWATCH", target_instance="default")
    pair["features"]["history"].update(remove=False, rewatches=rewatches)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": source, "CROSSWATCH": OPS})
    store = TrackerStore(config_base / "tracker")
    movie = {"type": "movie", "title": "Correct movie", "ids": {"tmdb": "42"}, "watched_at": original["watched_at"], TYPE_ORIGINS: sorted(event_identities(original))}
    latest = {**movie, "watched_at": "2025-01-01T12:00:00Z"}
    latest.pop(TYPE_ORIGINS)
    store.write_state("history", {history_sync_key(item, event_mode=True): item for item in (movie, latest)})
    before = store.read_current("history")["items"]
    for _ in range(2):
        plan = InteractivePlan()
        Orchestrator(cfg, interactive=plan).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
        assert not [row for row in plan.rows.values() if row["provider"] == "CROSSWATCH"], list(plan.rows.values())
        assert store.read_current("history")["items"] == before
    live = Orchestrator(cfg).run(pair_scope_ids=["p1"], write_state_json=False)
    assert live["ok"]
    assert store.read_current("history")["items"] == before
    neighbours = [{**original, "episode": 3}, {**original, "watched_at": "2026-01-01T12:00:00Z"}]
    source.index = {history_sync_key(item, event_mode=rewatches): item for item in neighbours}
    plan = InteractivePlan()
    Orchestrator(cfg, interactive=plan).run(dry_run=True, pair_scope_ids=["p1"], write_state_json=False)
    into_tracker = [row for row in plan.rows.values() if row["provider"] == "CROSSWATCH"]
    assert len(into_tracker) == 2
    exported = OPS.build_index({**cfg, "_cw_readonly": True, "_cw_history_rewatches": True}, feature="history")
    assert {item["type"] for item in exported.values()} == {"movie"}


@pytest.fixture
def pending_import(config_base, monkeypatch):
    cfg, source, other = feature_setup(config_base, monkeypatch, "watchlist", [feature_item("watchlist", 1)], [])
    cfg["crosswatch"] = {"root_dir": str(config_base / "tracker")}
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": source, "DST": other, "CROSSWATCH": OPS})
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    writes = []
    def update(mutator):
        draft = deepcopy(cfg)
        result = mutator(draft)
        writes.append(deepcopy(draft))
        cfg.clear()
        cfg.update(draft)
        return deepcopy(cfg), result
    monkeypatch.setattr("cw_platform.config_base.update_config", update)
    pending = tracker_import.preview_tracker(cfg, "Reviewed import")
    pair = tracker_import.pair_for("SRC", "default", pending["id"], ["watchlist"])
    session = interactive_sync.Session(pair_id=pair["id"], owner="local", import_pair=pair, new_tracker=pending)
    session.pair = pair
    try:
        yield cfg, session, pending, writes
    finally:
        session.close()


def test_new_tracker_is_only_created_after_selection_recheck_and_apply(pending_import):
    from pathlib import Path

    cfg, session, pending, writes = pending_import
    before = deepcopy(cfg)
    root = Path(pending["block"]["root_dir"])
    interactive_sync.refresh(session, cfg, {})
    assert session.status == "review", session.message
    assert len(session.plan.rows) == 1
    assert cfg == before and writes == [] and not root.exists()
    interactive_sync.recalculate(session, cfg, {})
    assert cfg == before and writes == [] and not root.exists()
    interactive_sync.apply(session, cfg, set(session.plan.rows))
    assert session.status == "complete", session.message
    assert len(writes) == 1
    assert session.new_tracker is None
    assert cfg["crosswatch"]["instances"][pending["id"]]["label"] == pending["block"]["label"]
    assert len(TrackerStore(root).read_current("watchlist")["items"]) == 1
    assert cfg["pairs"] == before["pairs"]


@pytest.mark.parametrize("conflict", ["settings", "directory", "selection", "mapping"])
def test_new_tracker_preapply_checks_do_not_create_an_empty_instance(pending_import, monkeypatch, conflict):
    from pathlib import Path

    cfg, session, pending, writes = pending_import
    interactive_sync.refresh(session, cfg, {})
    selected = set(session.plan.rows)
    if conflict == "settings":
        cfg["runtime"]["debug"] = True
    elif conflict == "directory":
        Path(pending["block"]["root_dir"]).mkdir(parents=True)
    elif conflict == "mapping":
        monkeypatch.setattr(interactive_sync, "mapping_version", lambda _: "changed")
    else:
        selected = {"stale-row"}
    session.status = "applying"
    interactive_sync.apply(session, cfg, selected)
    assert session.status == "review", session.message
    assert writes == []
    assert pending["id"] not in cfg["crosswatch"].get("instances", {})


@pytest.mark.parametrize("fail_launch", [False, True])
def test_closing_or_failing_a_new_tracker_preview_leaves_config_unchanged(cfg, fake_sources, monkeypatch, fail_launch):
    from pathlib import Path

    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(interactiveSyncAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    def launch(session, *args):
        if fail_launch:
            raise HTTPException(409, "Another sync is running")
        session.status = "review"
    monkeypatch.setattr(interactiveSyncAPI, "launch", launch)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    app.include_router(interactiveSyncAPI.router)
    client = TestClient(app)
    before = deepcopy(cfg)
    response = client.post("/api/tracker/import/preview", json={"source": "TRAKT", "provider_instance": "new", "name": "Review me", "features": ["history"]})
    if fail_launch:
        assert response.status_code == 409, response.text
    else:
        assert response.status_code == 200, response.text
        sid = response.json()["session"]["id"]
        session = interactive_sync.SESSIONS[sid]
        assert not Path(session.new_tracker["block"]["root_dir"]).exists()
        assert client.delete(f"/api/interactive-sync/{sid}").status_code == 200
    assert cfg == before


def test_competing_tracker_creation_cannot_redirect_a_pending_import(pending_import):
    from cw_platform.orchestrator._interactive import fingerprint

    cfg, session, pending, writes = pending_import
    expected = fingerprint(session.config(cfg))
    cfg["crosswatch"]["instances"] = {pending["id"]: {"connected": True, "label": "Other Tracker"}}
    before = deepcopy(cfg)
    with pytest.raises(ValueError, match="now in use"):
        tracker_import.create_preview_tracker(pending, session.import_pair, expected)
    assert cfg == before and writes == []


def test_new_tracker_apply_rechecks_config_atomically_before_creating(pending_import, monkeypatch):
    from cw_platform.orchestrator._interactive import fingerprint

    cfg, session, pending, writes = pending_import
    expected = fingerprint(session.config(cfg))
    cfg["runtime"]["debug"] = True
    with pytest.raises(ValueError, match="Settings changed"):
        tracker_import.create_preview_tracker(pending, session.import_pair, expected)
    assert pending["id"] not in cfg["crosswatch"].get("instances", {})
    assert writes == []

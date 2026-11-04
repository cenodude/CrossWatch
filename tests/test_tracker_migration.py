# tests/test_tracker_migration.py
# CrossWatch - Background move of the tracker from JSON files to its database
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
import json
from pathlib import Path

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services import tracker_migration

REPO = Path(__file__).resolve().parents[1]
MOVIE = {"type": "movie", "title": "Heat", "year": 1995, "ids": {"tmdb": "949"}, "watched_at": "2026-01-02T00:00:00.000Z"}


@pytest.fixture(autouse=True)
def clean_state(monkeypatch):
    monkeypatch.setattr(tracker_migration, "_STATE", {})
    monkeypatch.setattr(tracker_migration, "_ROOTS", set())


def _legacy(root: Path, history: str | None = None) -> None:
    (root / "snapshots").mkdir(parents=True)
    payload = json.dumps({"ts": 1, "items": {"tmdb:949": MOVIE}})
    (root / "history.json").write_text(history if history is not None else payload, "utf-8")
    (root / "ratings.json").write_text(json.dumps({"ts": 1, "items": {}}), "utf-8")
    for day in (1, 2):
        (root / "snapshots" / f"2026100{day}T000000Z-history.json").write_text(payload, "utf-8")


def _cfg(base: Path, **instances) -> dict:
    return {"crosswatch": {"root_dir": str(base), "instances": instances}}


def test_migrate_reports_progress_and_moves_the_originals(tmp_path):
    root = tmp_path / "tracker"
    _legacy(root)
    store = TrackerStore(root)
    assert store.needs_migration()
    seen = []
    stats = store.migrate(lambda *values: seen.append(values))
    assert seen[0][:3:2] == (0, 0) and seen[-1][0] == seen[-1][1] == 4 and seen[-1][2] == seen[-1][3] > 0
    assert (stats["states"], stats["restore_points"]) == (2, 2)
    assert (root / stats["backup"] / "history.json").is_file()
    assert not store.needs_migration()
    assert TrackerStore(root).read_current("history")["items"]["tmdb:949"]["title"] == "Heat"
    assert store.migrate() is None


def test_start_moves_every_instance_and_reports_the_result(tmp_path):
    base = tmp_path / "base"
    _legacy(base)
    _legacy(base / "profiles" / "CW-P01")
    assert tracker_migration.start(_cfg(base, **{"CW-P01": {}}), tmp_path, background=False)
    state = tracker_migration.status()
    assert state["state"] == "completed" and state["instance_count"] == 2
    assert [row["instance"] for row in state["results"]] == ["default", "CW-P01"]
    assert state["results"][0]["backup"].startswith("json-backup-")
    assert not tracker_migration.busy()


def test_an_unreadable_tracker_stays_on_json_and_the_others_still_move(tmp_path):
    base = tmp_path / "base"
    _legacy(base, history="{broken")
    _legacy(base / "profiles" / "CW-P01")
    tracker_migration.start(_cfg(base, **{"CW-P01": {}}), tmp_path, background=False)
    state = tracker_migration.status()
    assert state["state"] == "failed"
    assert state["errors"] == [{"instance": "default", "file": "history.json"}]
    assert [row["instance"] for row in state["results"]] == ["CW-P01"]
    assert (base / "history.json").is_file()


def test_nothing_to_move_leaves_no_notification(tmp_path):
    base = tmp_path / "base"
    TrackerStore(base).write_state("history", {"tmdb:949": MOVIE})
    assert tracker_migration.start(_cfg(base), tmp_path, background=False) is False
    assert tracker_migration.status() is None


@pytest.fixture()
def moving(tmp_path, monkeypatch):
    base = tmp_path / "base"
    _legacy(base)
    cfg = _cfg(base)
    tracker_migration._ROOTS.add(tracker_migration._key(base))
    tracker_migration._STATE.update(id="m1", state="running", instance="default", instance_index=1, instance_count=1,
                                    files_done=1, files_total=4, bytes_done=10, bytes_total=40, results=[], errors=[])
    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    app = FastAPI()
    app.include_router(trackerAPI.router)
    return base, cfg, TestClient(app)


def test_tracker_pages_wait_while_the_move_runs(moving):
    base, _, client = moving
    for path in ("/api/tracker/summary", "/api/tracker/items", "/api/tracker/changes", "/api/tracker/check"):
        res = client.get(path)
        assert res.status_code == 503 and res.json()["detail"] == "tracker_migrating"
    assert client.post("/api/tracker/edit", json={}).status_code == 503
    current = client.get("/api/tracker/check/current").json()
    assert current["job"] is None
    assert (current["migration"]["state"], current["migration"]["files_done"], current["migration"]["label"]) == ("running", 1, "Default")
    assert not (base / "tracker.db").exists()


def test_other_tracker_readers_skip_a_tracker_that_is_moving(moving, monkeypatch):
    base, cfg, _ = moving
    from api import dashboardAPI, maintenanceAPI

    assert dashboardAPI._dashboard_tracker_stamp(base.parent, cfg, {"history"}) == []
    monkeypatch.setattr(maintenanceAPI, "_cw_tracker_config", lambda _: cfg)
    with pytest.raises(HTTPException) as exc:
        maintenanceAPI._cw_tracker_context(base.parent)
    assert exc.value.status_code == 503
    assert not (base / "tracker.db").exists()


def test_the_move_starts_with_the_app_before_the_scheduler():
    source = (REPO / "crosswatch.py").read_text("utf-8")
    assert source.index("_start_tracker_migration(load_config() or {}, _tracker_config_dir)") < source.index("scheduler.start()")

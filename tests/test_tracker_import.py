# tests/test_tracker_import.py
# CrossWatch - Importing a connected service into a CrossWatch tracker instance
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import sys
import threading
import types
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from cw_platform.tracker_store import TrackerStore
from services import tracker_import

SOURCES = [
    {"id": "TRAKT", "label": "Trakt", "instances": [{"id": "default", "label": "Default"}, {"id": "TRAKT-P01", "label": "Work"}],
     "features": ["history", "watchlist", "ratings"]},
    {"id": "CROSSWATCH", "label": "CrossWatch", "instances": [{"id": "default", "label": "Default"}, {"id": "CW-P01", "label": "Desk"}],
     "features": ["history", "watchlist", "ratings", "progress", "collection"]},
]


@pytest.fixture(autouse=True)
def _clean_jobs():
    tracker_import._ACTIVE.clear()
    yield
    tracker_import._ACTIVE.clear()


@pytest.fixture()
def cfg(tmp_path):
    return {"crosswatch": {"root_dir": str(tmp_path / "default"), "instances": {"CW-P01": {"label": "Desk", "root_dir": str(tmp_path / "desk")}}}}


@pytest.fixture()
def fake_sources(monkeypatch):
    monkeypatch.setattr(tracker_import, "sources", lambda _cfg: SOURCES)


class Launcher:
    def __init__(self, result: dict[str, Any] | None = None) -> None:
        self.result = result or {"ok": True}
        self.calls: list[tuple[dict[str, Any], str]] = []
        self.done = None

    def __call__(self, pair, run_id, on_done):
        self.calls.append((dict(pair), run_id))
        self.done = on_done
        return {**self.result, "run_id": run_id} if self.result.get("ok") else self.result


def _start(cfg, store, launch, **kw):
    args = {"source": "TRAKT", "source_instance": "TRAKT-P01", "instance": "CW-P01", "features": ["history", "ratings"], **kw}
    return tracker_import.start(cfg, store, launch=launch, **args)


def test_sources_lists_configured_instances_and_shared_features(monkeypatch):
    class Ops:
        def __init__(self, ready):
            self.ready = ready

        def is_configured(self, view):
            return self.ready(view)

    support = {
        "CROSSWATCH": {"history", "watchlist", "ratings", "progress", "collection"},
        "TRAKT": {"history", "watchlist", "ratings", "playlists"},
        "KODI": {"history"},
        "TMDB": set(),
    }
    ops = {
        "TRAKT": Ops(lambda view: bool((view.get("trakt") or {}).get("access_token"))),
        "KODI": Ops(lambda view: False),
        "TMDB": Ops(lambda view: True),
        "CROSSWATCH": Ops(lambda view: True),
    }
    monkeypatch.setattr(tracker_import, "sync_provider_names", lambda upper=True: list(ops))
    monkeypatch.setattr(tracker_import, "load_sync_ops", lambda name: ops[name])
    monkeypatch.setattr(tracker_import, "sync_provider_supports_feature", lambda name, feature: feature in support[str(name).upper()])
    monkeypatch.setattr(tracker_import, "provider_display_label", lambda name: name.title())
    cfg = {"trakt": {"instances": {"TRAKT-P01": {"label": "Work", "access_token": "y"}, "TRAKT-P02": {}}}}

    rows = tracker_import.sources(cfg)

    assert [r["id"] for r in rows] == ["TRAKT", "CROSSWATCH"]
    assert rows[0]["features"] == ["history", "watchlist", "ratings"]
    assert rows[0]["instances"] == [{"id": "TRAKT-P01", "label": "Work"}]


def test_import_pair_only_adds_into_the_tracker():
    pair = tracker_import.pair_for("TRAKT", "TRAKT-P01", "CW-P01", ["history", "ratings"])

    assert pair["mode"] == "one-way"
    assert (pair["source"], pair["source_instance"], pair["target"], pair["target_instance"]) == ("TRAKT", "TRAKT-P01", "CROSSWATCH", "CW-P01")
    assert pair["features"] == {"history": {"enable": True, "add": True, "remove": False}, "ratings": {"enable": True, "add": True, "remove": False}}
    assert pair["id"] == "tracker-import-trakt-trakt-p01-cw-p01"


def test_import_runs_once_reports_progress_and_keeps_the_result(cfg, tmp_path, fake_sources):
    store = TrackerStore(tmp_path / "desk")
    launch = Launcher()

    job = _start(cfg, store, launch)

    assert job["ok"] and job["state"] == "running"
    pair, run_id = launch.calls[0]
    assert run_id == job["run_id"]
    assert pair["features"].keys() == {"history", "ratings"}
    assert tracker_import.status(store)["phase"] == "starting"
    assert _start(cfg, TrackerStore(tmp_path / "default"), Launcher(), instance="default")["error"] == "import_running"

    reading = {"run_id": run_id, "timeline": {"start": True}, "_phase": {"apply": {"total": 0}}}
    assert tracker_import.status(store, reading)["phase"] == "reading"
    writing = {"run_id": run_id, "timeline": {"post": True}, "_phase": {"apply": {"total": 40, "done": 10, "progress": 0.6, "feature_index": 2, "feature_count": 2}}}
    shown = tracker_import.status(store, writing)
    assert (shown["phase"], shown["done"], shown["total"], shown["progress"], shown["feature_index"]) == ("writing", 10, 40, 0.6, 2)

    launch.done({"run_id": run_id, "exit_code": 0, "unresolved": 2, "features": {"history": {"added": 30}, "ratings": {"added": 5, "updated": 1}}})

    final = tracker_import.status(store)
    assert final["state"] == "completed"
    assert (final["added"], final["added_by_feature"], final["updated"], final["unresolved"]) == (35, {"history": 30, "ratings": 5}, 1, 2)
    assert not tracker_import.busy()
    assert TrackerStore(tmp_path / "desk").job("import")["state"] == "completed"


@pytest.mark.parametrize(("summary", "state"), [
    ({"exit_code": 0, "cancelled": True}, "cancelled"),
    ({"exit_code": 1}, "failed"),
    ({"exit_code": 0, "run_id": "other"}, "failed"),
])
def test_import_outcomes(cfg, tmp_path, fake_sources, summary, state):
    store = TrackerStore(tmp_path / "desk")
    launch = Launcher()
    job = _start(cfg, store, launch)

    launch.done({"run_id": job["run_id"], **summary})

    assert tracker_import.status(store)["state"] == state


def test_a_running_sync_blocks_the_import_and_leaves_nothing_running(cfg, tmp_path, fake_sources):
    store = TrackerStore(tmp_path / "desk")

    result = _start(cfg, store, Launcher({"ok": False, "error": "sync_running"}))

    assert result == {"ok": False, "error": "sync_running"}
    assert not tracker_import.busy()
    assert tracker_import.status(store)["state"] == "idle"


def test_restart_during_an_import_reads_as_interrupted(cfg, tmp_path, fake_sources):
    store = TrackerStore(tmp_path / "desk")
    _start(cfg, store, Launcher())
    tracker_import._ACTIVE.clear()

    assert tracker_import.status(TrackerStore(tmp_path / "desk"))["state"] == "interrupted"


@pytest.mark.parametrize(("args", "code"), [
    ({"source": "PLEX"}, "unknown_source"),
    ({"source_instance": "TRAKT-P09"}, "unknown_source"),
    ({"instance": "CW-P07"}, "unknown_instance"),
    ({"source": "CROSSWATCH", "source_instance": "CW-P01"}, "same_instance"),
    ({"features": ["progress"]}, "unsupported_feature"),
    ({"features": ["history", "playlists"]}, "unsupported_feature"),
    ({"features": []}, "unsupported_feature"),
])
def test_import_rejects_bad_requests(cfg, tmp_path, fake_sources, args, code):
    launch = Launcher()

    with pytest.raises(ValueError, match=code):
        _start(cfg, TrackerStore(tmp_path / "desk"), launch, **args)

    assert launch.calls == []


@pytest.fixture()
def client(cfg, monkeypatch, fake_sources):
    from api import syncAPI

    monkeypatch.setattr(trackerAPI, "load_config", lambda: cfg)
    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    launch = Launcher()
    snapshots: dict[str, Any] = {}
    cancels: list[str] = []
    monkeypatch.setattr(syncAPI, "start_import_run", launch)
    monkeypatch.setattr(syncAPI, "import_run_snapshot", lambda run_id: snapshots.get(run_id))
    monkeypatch.setattr(syncAPI, "cancel_import_run", lambda run_id: (cancels.append(run_id), True)[1])
    app = FastAPI()
    app.include_router(trackerAPI.router)
    test_client = TestClient(app)
    return test_client, launch, snapshots, cancels


def test_import_api_starts_reports_and_cancels(client):
    api, launch, snapshots, cancels = client

    listed = api.get("/api/tracker/import/sources").json()
    assert [s["id"] for s in listed["sources"]] == ["TRAKT", "CROSSWATCH"]
    assert [i["id"] for i in listed["instances"]] == ["default", "CW-P01"]

    res = api.post("/api/tracker/import", json={"source": "TRAKT", "source_instance": "default", "provider_instance": "CW-P01", "features": ["history"]})
    assert res.status_code == 200, res.text
    run_id = res.json()["job"]["run_id"]
    assert launch.calls[0][0]["target_instance"] == "CW-P01"

    snapshots[run_id] = {"run_id": run_id, "timeline": {"post": True}, "_phase": {"apply": {"total": 8, "done": 3, "progress": 0.4}}}
    job = api.get("/api/tracker/import?provider_instance=CW-P01").json()["job"]
    assert (job["state"], job["phase"], job["done"], job["total"]) == ("running", "writing", 3, 8)
    assert api.get("/api/tracker/summary?provider_instance=CW-P01").json()["import"]["phase"] == "writing"
    current = api.get("/api/tracker/check/current").json()["import"]
    assert (current["instance"], current["label"], current["source_label"]) == ("CW-P01", "Desk", "Trakt")

    assert api.post("/api/tracker/import/cancel", json={"provider_instance": "CW-P01"}).json()["cancelled"] is True
    assert cancels == [run_id]

    launch.done({"run_id": run_id, "exit_code": 0, "features": {"history": {"added": 4}}})
    assert api.get("/api/tracker/check/current").json()["import"]["added"] == 4
    assert api.post("/api/tracker/import/cancel", json={"provider_instance": "CW-P01"}).json()["cancelled"] is False


def test_import_api_refuses_unknown_instances_running_syncs_and_pair_scoped_trackers(client, monkeypatch):
    api, launch, _snapshots, _cancels = client
    body = {"source": "TRAKT", "source_instance": "default", "features": ["history"]}

    unknown = api.post("/api/tracker/import", json={**body, "provider_instance": "CW-P05"})
    assert (unknown.status_code, unknown.json()["detail"]) == (404, "unknown_instance")
    bad = api.post("/api/tracker/import", json={**body, "provider_instance": "CW-P01", "features": ["playlists"]})
    assert (bad.status_code, bad.json()["error"]) == (400, "unsupported_feature")

    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: True)
    busy = api.post("/api/tracker/import", json={**body, "provider_instance": "CW-P01"})
    assert (busy.status_code, busy.json()["error"]) == (409, "sync_running")

    monkeypatch.setattr(trackerAPI, "_sync_running", lambda: False)
    monkeypatch.setattr(trackerAPI, "_pair_scoped", lambda: True)
    scoped = api.post("/api/tracker/import", json={**body, "provider_instance": "CW-P01"})
    assert (scoped.status_code, scoped.json()["error"]) == (409, "pair_scoped")
    assert launch.calls == []


def _sync_harness(monkeypatch, tmp_path, running: bool = False):
    import api.syncAPI as sync

    logs: dict[str, list[str]] = {"SYNC": []}
    procs: dict[str, Any] = {}
    if running:
        procs["SYNC"] = types.SimpleNamespace(is_alive=lambda: True)
    monkeypatch.setitem(sys.modules, "crosswatch", types.SimpleNamespace(
        LOG_BUFFERS=logs, RUNNING_PROCS=procs, SYNC_PROC_LOCK=threading.Lock(),
        STATS=types.SimpleNamespace(refresh_from_state=lambda _s: None, record_summary=lambda *_a: None),
        REPORT_DIR=tmp_path, strip_ansi=lambda value: str(value),
        _append_log=lambda kind, message: logs.setdefault(kind, []).append(str(message)),
        minimal=lambda value: value, canonical_key=lambda value: str(value),
    ))
    cfg = {"pairs": [{"id": "plex-trakt", "enabled": True, "source": "PLEX", "target": "TRAKT"}], "sync": {"write_state_json": True}}
    monkeypatch.setattr(sync, "_env", lambda: (lambda: cfg, lambda _cfg: None))
    monkeypatch.setattr(sync, "_provider_count_defaults", lambda: {})
    from services import log_archive

    monkeypatch.setattr(log_archive, "archive", lambda: types.SimpleNamespace(start=lambda *_a: None))
    seen: dict[str, Any] = {}

    class FakeOrchestrator:
        def __init__(self, config):
            seen["pairs"] = config["pairs"]

        def run_pairs(self, **kwargs):
            seen["kwargs"] = kwargs
            kwargs["progress"](json.dumps({"event": "apply:add:done", "feature": "history", "added": 2, "count": 2}))
            return {"added": 2}

    real_import = sync.importlib.import_module
    monkeypatch.setattr(sync.importlib, "import_module", lambda name: types.SimpleNamespace(Orchestrator=FakeOrchestrator, __file__="fake.py")
                        if name == "cw_platform.orchestrator" else real_import(name))
    return sync, seen, procs


def test_import_run_uses_only_its_own_pair_and_writes_no_baselines(monkeypatch, tmp_path):
    sync, seen, _procs = _sync_harness(monkeypatch, tmp_path)
    pair = tracker_import.pair_for("TRAKT", "default", "CW-P01", ["history"])
    finished: list[dict[str, Any]] = []
    done = threading.Event()

    started = sync.start_import_run(pair, "run-import-1", lambda snap: (finished.append(snap), done.set()))
    assert done.wait(10)

    assert started == {"ok": True, "run_id": "run-import-1"}
    assert seen["pairs"] == [pair]
    assert seen["kwargs"]["write_state_json"] is False
    assert finished and finished[0]["run_id"] == "run-import-1"
    assert finished[0]["exit_code"] == 0


def test_import_run_waits_for_a_running_sync(monkeypatch, tmp_path):
    sync, seen, _procs = _sync_harness(monkeypatch, tmp_path, running=True)

    result = sync.start_import_run(tracker_import.pair_for("TRAKT", "default", "default", ["history"]), "run-import-2", lambda _s: None)

    assert result == {"ok": False, "error": "sync_running"}
    assert seen == {}

# tests/test_tracker_dropped.py
# CrossWatch - The CrossWatch tracker reads and writes dropped shows through the dropped show modes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, Mapping

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from api import trackerAPI
from api.syncAPI import _normalize_pair_providers
from cw_platform.tracker_store import TrackerStore
from providers.sync._mod_CROSSWATCH import OPS
from services import tracker_import
from tests.test_dropped_show_modes import DroppedOps, _episode, _index, _run, _sent, _tagged
from tests.test_history_baseline_native import HistoryOps

SEVERANCE = {
    "type": "episode", "title": "S01E01", "series_title": "Severance", "series_year": 2022, "season": 1, "episode": 1,
    "ids": {"tmdb": "1"}, "show_ids": {"tmdb": "95396", "tvdb": "371980", "imdb": "tt11280740"},
    "watched_at": "2026-01-01T00:00:00.000Z",
}
BEAR = {**SEVERANCE, "series_title": "The Bear", "show_ids": {"tmdb": "136315"}}


def _cfg(root: Path, **extra: Any) -> dict[str, Any]:
    return {"crosswatch": {"root_dir": str(root)}, **extra}


@pytest.fixture()
def root(tmp_path):
    TrackerStore(tmp_path).write_state("history", {"tmdb:95396#s01e01": SEVERANCE, "tmdb:136315#s01e01": BEAR})
    return tmp_path


def test_dropped_shows_become_tokens_with_every_show_id(root):
    store = TrackerStore(root)
    store.save_note("show:tmdb:95396", {"type": "show", "title": "Severance", "status": "dropped"})
    store.save_note("show:tmdb:136315", {"type": "show", "title": "The Bear", "status": "watching"})
    store.save_note("movie:tmdb:949", {"type": "movie", "title": "Heat", "status": "dropped"})

    assert OPS.dropped_show_tokens(_cfg(root)) == {"tmdb:95396", "tvdb:371980", "imdb:tt11280740"}


def test_tagging_marks_shows_dropped_keeps_notes_and_undoes_in_one_step(root):
    store = TrackerStore(root)
    store.save_note("show:tmdb:136315", {"type": "show", "title": "The Bear", "status": "watching", "tags": ["Kitchen"], "notes": "Yes chef"})

    res = OPS.tag_dropped_shows(_cfg(root), [
        {"type": "show", "ids": {"tmdb": "95396", "tvdb": "371980"}},
        {"type": "show", "title": "The Bear", "ids": {"tmdb": "136315"}},
        {"type": "show", "ids": {"tvdb": "1"}},
    ])

    assert res == {"attempted": 2, "confirmed": 2, "skipped": 1, "not_found": 0, "errors": 0}
    severance = store.note("show:tmdb:95396")
    assert (severance["status"], severance["title"], severance["year"]) == ("dropped", "Severance", 2022)
    bear = store.note("show:tmdb:136315")
    assert (bear["status"], bear["tags"], bear["notes"]) == ("dropped", ["Kitchen"], "Yes chef")

    total, log = store.change_log()
    assert (total, log[0].feature, log[0].count) == (2, "notes", 2)
    again = OPS.tag_dropped_shows(_cfg(root), [{"type": "show", "ids": {"tmdb": "95396"}}])
    assert again["confirmed"] == 1 and store.change_log()[0] == 2

    store.undo(log[0].batch)
    assert store.note("show:tmdb:95396") is None
    assert store.note("show:tmdb:136315")["status"] == "watching"


def test_a_read_only_view_writes_nothing(root):
    res = OPS.tag_dropped_shows(_cfg(root, _cw_readonly=True), [{"type": "show", "ids": {"tmdb": "95396"}}])
    assert res["confirmed"] == 0
    assert TrackerStore(root).note("show:tmdb:95396") is None


@dataclass
class TrackerOps(HistoryOps):
    root: Path | None = None

    def dropped_show_tokens(self, cfg: Mapping[str, Any]) -> set[str]:
        return OPS.dropped_show_tokens(_cfg(self.root))

    def tag_dropped_shows(self, cfg: Mapping[str, Any], items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
        return OPS.tag_dropped_shows(_cfg(self.root), items)


def test_service_to_tracker_tag_mode_marks_the_show_dropped(monkeypatch, root):
    src = DroppedOps(provider="SRC", index=_index(_episode("95396", 1, 1), _episode("2", 1, 1)), dropped={"tmdb:95396"})
    dst = TrackerOps(provider="DST", index=_index(_episode("95396", 1, 1)), root=root)

    _run(monkeypatch, src, dst, providers={"src": {"history_dropped_mode": "tag"}})

    assert TrackerStore(root).note("show:tmdb:95396")["status"] == "dropped"
    assert TrackerStore(root).note("show:tmdb:2") is None


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_synced_drop_survives_automation_and_downstream_ignore(monkeypatch, root, mode):
    from services import tracker_automation as worker

    src = DroppedOps(provider="SRC", index=_index(_episode("95396", 1, 1), _episode("95396", 1, 2), _episode("2", 1, 1)), dropped={"tmdb:95396"})
    dst = TrackerOps(provider="DST", index=_index(_episode("95396", 1, 1)), root=root)
    original_add = dst.add
    def add(cfg, items, **kwargs):
        batch = list(items)
        result = OPS.add(_cfg(root), batch, **kwargs)
        assert result.get("ok") is not False
        return original_add(cfg, batch, **kwargs)
    monkeypatch.setattr(dst, "add", add)
    _run(monkeypatch, src, dst, mode=mode, providers={"src": {"history_dropped_mode": "tag"}})
    assert ("95396", 1, 2) in _sent(dst)
    store = TrackerStore(root)
    assert any(row.get("episode") == 2 and str(row.get("show_ids", {}).get("tmdb")) == "95396" for row in store.read_current("history")["items"].values())
    assert store.note("show:tmdb:95396")["status"] == "dropped"
    reads = []
    monkeypatch.setattr(worker, "busy", lambda: False)
    monkeypatch.setattr(worker, "cached_metadata", lambda key: reads.append(key) or {"status": "Returning Series", "seasons": [{"season_number": 1, "episode_count": 10}]})
    worker.run_once(_cfg(root), root)
    assert "show:tmdb:95396" not in reads
    assert store.note("show:tmdb:95396")["status"] == "dropped"
    assert store.status_control("show:tmdb:95396") == {"origin": "sync", "manual": False}
    ignored = DroppedOps(provider="DST", index={})
    _run(monkeypatch, TrackerOps(provider="SRC", index=src.index, root=root), ignored, providers={"src": {"history_dropped_mode": "ignore"}})
    assert _sent(ignored) == {("2", 1, 1)}


def test_tracker_to_service_tags_or_ignores_shows_dropped_in_the_tracker(monkeypatch, root):
    TrackerStore(root).save_note("show:tmdb:95396", {"type": "show", "title": "Severance", "status": "dropped"})
    plays = _index(_episode("95396", 1, 1), _episode("2", 1, 1))

    dst = DroppedOps(provider="DST", index={})
    _run(monkeypatch, TrackerOps(provider="SRC", index=plays, root=root), dst, providers={"src": {"history_dropped_mode": "tag"}})
    assert _tagged(dst) == ["95396"]

    ignored = DroppedOps(provider="DST", index={})
    _run(monkeypatch, TrackerOps(provider="SRC", index=plays, root=root), ignored, providers={"src": {"history_dropped_mode": "ignore"}})
    assert _sent(ignored) == {("2", 1, 1)}


def test_pair_save_keeps_the_tracker_dropped_mode():
    assert _normalize_pair_providers({"crosswatch": {"history_dropped_mode": "tag"}})["crosswatch"] == {"history_dropped_mode": "tag"}


def test_import_can_bring_in_dropped_shows():
    pair = tracker_import.pair_for("SIMKL", "default", "CW-P01", ["history", "watchlist"], dropped=True)
    assert pair["providers"] == {"simkl": {"history_dropped_mode": "tag"}}
    assert "providers" not in tracker_import.pair_for("SIMKL", "default", "CW-P01", ["watchlist"], dropped=True)
    assert "providers" not in tracker_import.pair_for("SIMKL", "default", "CW-P01", ["history"])


def test_import_start_only_tags_for_sources_that_know_dropped_shows(tmp_path, monkeypatch):
    rows = [
        {"id": "SIMKL", "label": "SIMKL", "instances": [{"id": "default", "label": "Default"}], "features": ["history"], "dropped": True},
        {"id": "PLEX", "label": "Plex", "instances": [{"id": "default", "label": "Default"}], "features": ["history"], "dropped": False},
    ]
    monkeypatch.setattr(tracker_import, "sources", lambda _cfg: rows)
    monkeypatch.setattr(tracker_import, "_instance_ids", lambda _cfg, _name: ["default"])
    launched: list[dict[str, Any]] = []

    def launch(pair, run_id, on_done):
        launched.append(pair)
        return {"ok": True}

    store = TrackerStore(tmp_path)
    for source in ("SIMKL", "PLEX"):
        tracker_import.start({}, store, source=source, source_instance="default", instance="default",
                             features=["history"], launch=launch, dropped=True)
        tracker_import._ACTIVE.clear()
    assert launched[0]["providers"] == {"simkl": {"history_dropped_mode": "tag"}}
    assert "providers" not in launched[1]


def test_shows_view_can_hide_dropped_shows(root, monkeypatch):
    TrackerStore(root).save_note("show:tmdb:95396", {"type": "show", "title": "Severance", "status": "dropped"})
    monkeypatch.setattr(trackerAPI, "load_config", lambda: _cfg(root))
    app = FastAPI()
    app.include_router(trackerAPI.router)
    api = TestClient(app)

    every = api.get("/api/tracker/shows").json()
    assert every["total"] == 2
    assert next(s for s in every["shows"] if s["key"] == "tmdb:95396")["status"] == "dropped"
    hidden = api.get("/api/tracker/shows?hide_dropped=true").json()
    assert (hidden["total"], [s["key"] for s in hidden["shows"]]) == (1, ["tmdb:136315"])

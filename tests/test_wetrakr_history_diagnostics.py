# tests/test_wetrakr_history_diagnostics.py
# CrossWatch - WeTrakr unreadable history records and feature error attribution
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Any, Mapping

import pytest

from cw_platform.orchestrator._pairs import failure_attribution
from cw_platform.orchestrator.facade import Orchestrator
from providers.sync.wetrakr import _history
from providers.sync.wetrakr._common import WeTrakrSyncError
from services.interactive_sync_report import notice_subject

SHOW = {"id": 900, "title": "Example Show", "ids": {"tmdb": 1400, "imdb": "tt0000900"}}


def _episode(eid: int, season: Any, number: Any, show: Any = SHOW) -> dict[str, Any]:
    return {"type": "episode", "id": eid, "ids": {"tmdb": eid + 10000}, "title": f"Episode {eid}",
            "season_number": season, "number": number, "show": show, "watched_at": "2026-01-02T03:04:05Z"}


def _movie(mid: int) -> dict[str, Any]:
    return {"type": "movie", "id": mid, "ids": {"tmdb": mid}, "title": f"Movie {mid}", "watched_at": "2026-01-01T00:00:00Z"}


@pytest.fixture
def wetrakr(monkeypatch):
    rows = {"movie": [_movie(1)], "episode": [_episode(11, 1, 1), _episode(12, 1, 0), _episode(13, None, 2)]}
    logs: list[tuple[tuple[Any, ...], dict[str, Any]]] = []
    monkeypatch.setattr(_history, "tracking_rows", lambda adapter, feature, force=False: rows)
    monkeypatch.setattr(_history, "log", lambda *args, **fields: logs.append((args, fields)))
    return SimpleNamespace(adapter=SimpleNamespace(config={}), rows=rows, logs=logs)


def test_unreadable_history_records_fail_the_read_and_are_named(wetrakr):
    with pytest.raises(WeTrakrSyncError) as caught:
        _history.build_index(wetrakr.adapter)
    message = str(caught.value)
    assert caught.value.reason == "invalid_history_records"
    assert "WeTrakr returned 2 unreadable history records" in message
    assert "episode 'Example Show' season=1 episode=0 id=12 show_id=900 ids=tmdb:1400,imdb:tt0000900 (invalid_episode_coordinates)" in message
    assert "season=missing episode=2 id=13" in message
    logged = [fields for args, fields in wetrakr.logs if args[3] == "history_record_invalid"]
    assert [row["id"] for row in logged] == ["12", "13"]
    assert logged[0]["season"] == "1" and logged[0]["episode"] == "0"
    assert logged[0]["show_ids"] == {"tmdb": "1400", "imdb": "tt0000900"}


@pytest.mark.parametrize("total, suffix", [(5, "; 2 more in the log"), (60, "; 57 more, the log lists 17 of them")])
def test_overflow_counts_match_what_the_log_contains(wetrakr, total, suffix):
    wetrakr.rows["episode"] = [_episode(20 + i, 1, 0) for i in range(total)]
    with pytest.raises(WeTrakrSyncError) as caught:
        _history.build_index(wetrakr.adapter)
    assert f"{total} unreadable history records" in str(caught.value)
    assert str(caught.value).endswith(suffix)
    assert len([1 for args, _ in wetrakr.logs if args[3] == "history_record_invalid"]) == min(total, 20)
    summary = [fields for args, fields in wetrakr.logs if args[3] == "history_records_invalid"]
    assert summary == [{"count": total, "logged": min(total, 20)}]


def test_summary_fits_the_notice_cap_and_cuts_at_record_boundaries(wetrakr):
    long_show = dict(SHOW, title="T" * 300)
    wetrakr.rows["episode"] = [_episode(20 + i, 1, 0, show=dict(long_show, id=900 + i)) for i in range(4)]
    with pytest.raises(WeTrakrSyncError) as caught:
        _history.build_index(wetrakr.adapter)
    message = str(caught.value)
    assert len(message) <= 500
    assert message.count("(invalid_episode_coordinates)") == message.count("episode '")
    assert "T" * 81 not in message
    assert message.endswith("more in the log")


def test_structured_id_values_are_reduced_to_their_id(wetrakr):
    show = {"id": {"url": "https://x.test/?token=SECRET"}, "title": "Example Show",
            "ids": {"tmdb": {"id": 3, "url": "https://x.test/?token=SECRET"}, "imdb": ["SECRET"], "tvdb": True}}
    wetrakr.rows["episode"] = [_episode(12, 1, 0, show=show)]
    with pytest.raises(WeTrakrSyncError) as caught:
        _history.build_index(wetrakr.adapter)
    assert "SECRET" not in str(caught.value) and "SECRET" not in repr(wetrakr.logs)
    assert "ids=tmdb:3 (invalid_episode_coordinates)" in str(caught.value)


def test_event_history_names_the_play_of_an_unreadable_record(wetrakr):
    wetrakr.adapter.config["_cw_history_rewatches"] = True
    wetrakr.rows["movie"] = [{"id": "p1", "watched_at": "2026-01-01T00:00:00Z", "movie": _movie(1)},
                             {"id": "p2", "movie": _movie(2)}]
    wetrakr.rows["episode"] = [{"id": "p3", "watched_at": "2026-01-01T00:00:00Z"}]
    with pytest.raises(WeTrakrSyncError) as caught:
        _history.build_index(wetrakr.adapter)
    assert "movie 'Movie 2' id=2 play_id=p2 ids=tmdb:2 (missing_watch_timestamp)" in str(caught.value)
    assert "episode 'untitled' season=missing episode=missing play_id=p3 (invalid_history_item)" in str(caught.value)


def test_readable_history_is_unchanged(wetrakr):
    wetrakr.rows["episode"] = [_episode(11, 1, 1), _episode(14, 0, 3)]
    assert len(_history.build_index(wetrakr.adapter)) == 3


def test_index_integrity_errors_keep_their_own_reason(wetrakr):
    wetrakr.rows["episode"] = [_episode(11, 1, 1), _episode(11, 1, 1)]
    with pytest.raises(WeTrakrSyncError) as caught:
        _history.build_index(wetrakr.adapter)
    assert caught.value.reason == "duplicate_history_identity"


def test_feature_errors_are_attributed_to_the_failing_side():
    assert failure_attribution(WeTrakrSyncError("x"), "WETRAKR", "FLOPPY") == {"provider": "WETRAKR", "side": "source"}
    assert failure_attribution(WeTrakrSyncError("x"), "FLOPPY", "WETRAKR") == {"provider": "WETRAKR", "side": "destination"}
    assert failure_attribution(WeTrakrSyncError("x"), "WETRAKR", "FLOPPY", "two-way") == {"provider": "WETRAKR", "side": "side A"}
    assert failure_attribution(RuntimeError("x"), "WETRAKR", "FLOPPY") == {}
    error = RuntimeError("x")
    error._cw_failed_provider = "FLOPPY"
    assert failure_attribution(error, "WETRAKR", "FLOPPY") == {"provider": "FLOPPY", "side": "destination"}
    error._cw_failed_provider = "WETRAKR"
    assert failure_attribution(error, "WETRAKR", "WETRAKR") == {"provider": "WETRAKR"}


def test_feature_error_notice_uses_the_failing_provider_not_the_destination():
    event = {"event": "feature:error", "src": "WETRAKR", "dst": "FLOPPY", "provider": "WETRAKR", "side": "source"}
    assert notice_subject(event) == {"provider": "WETRAKR", "side": "source", "route": "WETRAKR to FLOPPY"}
    assert notice_subject({"event": "feature:error", "src": "WETRAKR", "dst": "FLOPPY"}) == {"provider": "", "route": "WETRAKR to FLOPPY"}
    assert notice_subject({"event": "writes:skipped", "dst": "FLOPPY", "provider": "X"}) == {"provider": "FLOPPY"}


@dataclass
class Ops:
    provider: str
    fail: bool = False
    adds: list[Any] = field(default_factory=list)
    removes: list[Any] = field(default_factory=list)

    def name(self) -> str:
        return self.provider

    def label(self) -> str:
        return self.provider

    def features(self) -> Mapping[str, bool]:
        return {"history": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {"features": {"history": True}, "observed_deletes": True, "index_semantics": "present"}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"history": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, Any]:
        if self.fail:
            raise RuntimeError("unreadable records")
        return {}

    def add(self, cfg: Mapping[str, Any], items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.adds.append(list(items))
        return {"ok": True, "count": 0}

    def remove(self, cfg: Mapping[str, Any], items: Any, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        self.removes.append(list(items))
        return {"ok": True, "count": 0}


@pytest.mark.parametrize("failing", ["SRC", "DST"])
def test_failed_read_is_attributed_to_the_provider_that_failed(config_base, monkeypatch, failing):
    src, dst = Ops("SRC", fail=failing == "SRC"), Ops("DST", fail=failing == "DST")
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"SRC": src, "DST": dst})
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_blackbox_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs_blocklist.load_unresolved_keys", lambda *_a, **_k: set())
    monkeypatch.setattr("cw_platform.orchestrator._pairs.record_health", lambda *_a, **_k: None)
    events: list[dict[str, Any]] = []

    def progress(line: str) -> None:
        try:
            event = json.loads(line)
        except (TypeError, ValueError):
            return
        if isinstance(event, dict):
            events.append(event)

    cfg = {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": True, "include_observed_deletes": True},
        "pairs": [{"id": "p1", "enabled": True, "source": "SRC", "target": "DST", "mode": "one-way", "feature": "history",
                   "features": {"history": {"enable": True, "add": True, "remove": True}}}],
    }
    Orchestrator(cfg, on_progress=progress).run()
    errors = [e for e in events if e.get("event") == "feature:error"]
    assert [(e["provider"], e["side"]) for e in errors] == [(failing, "source" if failing == "SRC" else "destination")]
    assert errors[0]["error"] == "unreadable records"
    assert src.adds == dst.adds == [] and src.removes == dst.removes == []

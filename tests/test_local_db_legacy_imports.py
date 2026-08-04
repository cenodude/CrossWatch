# tests/test_local_db_legacy_imports.py
# CrossWatch - Local database legacy import policy tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json

from cw_platform.local_db import last_sync, manual_policy, statistics
from cw_platform.local_db.legacy_files import (
    LAST_SYNC_JSON,
    STATE_JSON,
    STATE_MANUAL_JSON,
    STATISTICS_JSON,
    SYNC_REPORTS_DIR,
    WATCHLIST_HIDE_JSON,
    legacy_root,
    legacy_path,
)
from cw_platform.orchestrator._state_store import StateStore


def test_local_database_does_not_import_legacy_json(tmp_path) -> None:
    state_base = tmp_path / "state"
    last_sync_base = tmp_path / "last-sync"
    statistics_base = tmp_path / "statistics"
    manual_base = tmp_path / "manual"
    for base in (state_base, last_sync_base, statistics_base, manual_base):
        base.mkdir()

    legacy_path(state_base, STATE_JSON).write_text(
        json.dumps({"providers": {"TRAKT": {"watchlist": {"baseline": {"items": {"movie:1": {"title": "Old"}}}}}}}),
        encoding="utf-8",
    )
    legacy_path(last_sync_base, LAST_SYNC_JSON).write_text(
        json.dumps({"started_at": 123, "result": {"added": 4}}),
        encoding="utf-8",
    )
    legacy_path(statistics_base, STATISTICS_JSON).write_text(
        json.dumps({"counters": {"added": 9, "removed": 3}}),
        encoding="utf-8",
    )
    legacy_path(manual_base, STATE_MANUAL_JSON).write_text(
        json.dumps(
            {
                "version": 1,
                "providers": {
                    "TRAKT": {
                        "watchlist": {
                            "blocks": ["movie:block"],
                            "adds": {"items": {"movie:add": {"title": "Manual", "type": "movie"}}},
                        }
                    }
                },
            }
        ),
        encoding="utf-8",
    )

    assert StateStore(state_base).load_state()["providers"] == {}
    assert last_sync.load_last_sync(last_sync_base) == {}
    assert statistics.load_statistics(statistics_base)["counters"] == {"added": 0, "removed": 0}

    assert manual_policy.load_policy(manual_base) == {"version": 1, "providers": {}}
    assert manual_policy.has_policy(manual_base) is False


def test_local_database_moves_legacy_artifacts_to_legacy_folder(tmp_path) -> None:
    legacy_path(tmp_path, STATE_JSON).write_text("{}", encoding="utf-8")
    legacy_path(tmp_path, LAST_SYNC_JSON).write_text("{}", encoding="utf-8")
    legacy_path(tmp_path, STATE_MANUAL_JSON).write_text("{}", encoding="utf-8")
    legacy_path(tmp_path, STATISTICS_JSON).write_text("{}", encoding="utf-8")
    legacy_path(tmp_path, WATCHLIST_HIDE_JSON).write_text("[]", encoding="utf-8")

    reports = tmp_path / SYNC_REPORTS_DIR
    reports.mkdir()
    (reports / "sync-20260101.json").write_text("{}", encoding="utf-8")

    cw_state = tmp_path / ".cw_state"
    cw_state.mkdir()
    for name in ("activity_history.json", "currently_watching.json", "auto_remove_seen.json", "watchlist_wl_autoremove.json"):
        (cw_state / name).write_text("{}", encoding="utf-8")
    (cw_state / "tombstones.json").write_text("{}", encoding="utf-8")

    StateStore(tmp_path).load_state()
    legacy = legacy_root(tmp_path)

    for name in (STATE_JSON, LAST_SYNC_JSON, STATE_MANUAL_JSON, STATISTICS_JSON, WATCHLIST_HIDE_JSON):
        assert not (tmp_path / name).exists()
        assert (legacy / name).exists()
    assert not reports.exists()
    assert (legacy / SYNC_REPORTS_DIR / "sync-20260101.json").exists()
    for name in ("activity_history.json", "currently_watching.json", "auto_remove_seen.json", "watchlist_wl_autoremove.json"):
        assert not (cw_state / name).exists()
        assert (legacy / ".cw_state" / name).exists()
    assert (cw_state / "tombstones.json").exists()


def test_local_database_does_not_overwrite_existing_legacy_artifacts(tmp_path) -> None:
    legacy = legacy_root(tmp_path)
    legacy.mkdir()
    (legacy / STATE_JSON).write_text('{"old":true}', encoding="utf-8")
    legacy_path(tmp_path, STATE_JSON).write_text('{"new":true}', encoding="utf-8")

    StateStore(tmp_path).load_state()

    assert not legacy_path(tmp_path, STATE_JSON).exists()
    assert (legacy / STATE_JSON).read_text(encoding="utf-8") == '{"old":true}'
    assert (legacy / "state.1.json").read_text(encoding="utf-8") == '{"new":true}'

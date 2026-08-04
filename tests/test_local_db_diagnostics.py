# tests/test_local_db_diagnostics.py
# CrossWatch - Local database diagnostics tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.local_db import get_conn
from cw_platform.local_db.diagnostics import diagnostics
from cw_platform.orchestrator._state_store import StateStore


def test_database_diagnostics_reports_native_state_counts(tmp_path) -> None:
    StateStore(tmp_path).save_state(
        {
            "providers": {
                "TRAKT": {
                    "watchlist": {
                        "baseline": {"items": {"movie:1": {"title": "One", "type": "movie"}}}
                    }
                }
            },
            "last_sync_epoch": 1760000000,
        }
    )

    report = diagnostics(tmp_path)

    assert report["ok"] is True
    assert report["healthy"] is True
    assert report["integrity"] == "ok"
    assert report["schema_version"] == report["expected_schema_version"]
    assert report["table_counts"]["provider_feature_state"] == 1
    assert report["table_counts"]["baseline_items"] == 1
    assert report["last_sync"]["state_epoch"] == 1760000000


def test_database_diagnostics_reports_orphan_rows(tmp_path) -> None:
    conn = get_conn(tmp_path)
    assert conn is not None
    conn.execute("PRAGMA foreign_keys=OFF")
    with conn:
        conn.execute(
            "INSERT INTO manual_policy_blocks(feature_id,item_key,ordinal,updated_at) VALUES(?,?,?,?)",
            (404, "movie:missing", 0, 1),
        )
    conn.execute("PRAGMA foreign_keys=ON")

    report = diagnostics(tmp_path)

    assert report["ok"] is True
    assert report["healthy"] is False
    assert report["orphan_counts"]["manual_policy_blocks_missing_feature"] == 1

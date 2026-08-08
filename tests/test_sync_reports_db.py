# tests/test_sync_reports_db.py
# CrossWatch - SQLite sync report tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import pytest

from cw_platform.local_db import close_conn
from cw_platform.local_db.sync_reports import clear_reports, list_reports, report_count, save_report


@pytest.fixture()
def isolated_db(tmp_path, monkeypatch):
    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    close_conn()
    yield tmp_path
    close_conn()


def test_sync_report_round_trips_native_rows(isolated_db) -> None:
    run_id = save_report(
        isolated_db,
        {
            "run_id": "run-1",
            "started_at": "2026-08-04T10:00:00Z",
            "finished_at": "2026-08-04T10:00:05Z",
            "duration_sec": 5.2,
            "result": "success",
            "exit_code": 0,
            "added_last": 3,
            "removed_last": 1,
            "updated_last": 2,
            "features_enabled": {"watchlist": True, "ratings": False},
            "features": {
                "watchlist": {
                    "added": 3,
                    "removed": 1,
                    "updated": 2,
                    "spotlight_add": [
                        {
                            "key": "movie:949",
                            "title": "Heat",
                            "type": "movie",
                            "year": 1995,
                            "source": "PLEX",
                            "ids": {"tmdb": 949, "imdb": "tt0113277"},
                        }
                    ],
                },
                "ratings": {"added": 0, "removed": 0, "updated": 0},
            },
            "provider_counts_pre": {"plex": 10},
            "provider_counts_post": {"plex": 12, "trakt": 9},
            "timeline": {"pull": True, "apply": False},
        },
    )

    assert run_id == "run-1"
    assert report_count(isolated_db) == 1

    rows = list_reports(isolated_db, limit=5, feature_keys=["watchlist", "ratings", "history"])
    row = rows[0]

    assert row["finished_at"] == "2026-08-04T10:00:05Z"
    assert row["added"] == 3
    assert row["removed"] == 1
    assert row["updated_total"] == 2
    assert row["plex_post"] == 12
    assert row["trakt_post"] == 9
    assert row["features_enabled"] == {"watchlist": True, "ratings": False}
    assert row["features"]["history"] == {
        "added": 0,
        "removed": 0,
        "updated": 0,
        "spotlight_add": [],
        "spotlight_remove": [],
        "spotlight_update": [],
    }
    assert row["features"]["watchlist"]["spotlight_add"][0]["ids"] == {
        "tmdb": "949",
        "imdb": "tt0113277",
    }
    assert row["timeline"] == {"apply": False, "pull": True}
    assert clear_reports(isolated_db) == 1
    assert list_reports(isolated_db) == []


def test_statistics_ingests_feature_totals_from_sync_report_db(isolated_db) -> None:
    save_report(
        isolated_db,
        {
            "run_id": "run-stats",
            "finished_at": "2026-08-04T11:00:00Z",
            "features": {
                "watchlist": {
                    "added": 2,
                    "removed": 0,
                    "updated": 1,
                    "spotlight_add": [{"key": "movie:1", "title": "One", "type": "movie"}],
                }
            },
        },
    )

    from services.statistics import Stats

    stats = Stats(isolated_db / "statistics.json")
    stats._ingest_latest_report_features_once()

    assert "run-stats" in stats.data["ingested_runs"]
    assert any(
        row.get("run_id") == "run-stats" and row.get("feature") == "watchlist" and row.get("added") == 2
        for row in stats.data["feature_totals"]
    )
    assert any(row.get("key") == "movie:1" for row in stats.data["events"])

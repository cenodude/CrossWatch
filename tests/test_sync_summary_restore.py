from __future__ import annotations

from cw_platform.local_db import close_conn
from cw_platform.local_db.sync_reports import save_report


def test_summary_response_restores_latest_completed_report(monkeypatch, tmp_path) -> None:
    from api import syncAPI as sync

    monkeypatch.setenv("CROSSWATCH_DB", str(tmp_path / "crosswatch.sqlite3"))
    close_conn()
    sync.SUMMARY.clear()
    try:
        save_report(
            tmp_path,
            {
                "run_id": "run-restored",
                "started_at": "2026-08-04T10:00:00Z",
                "finished_at": "2026-08-04T10:00:05Z",
                "exit_code": 0,
                "features": {
                    "watchlist": {
                        "added": 3,
                        "removed": 0,
                        "updated": 1,
                        "spotlight_add": [{"key": "movie:1", "title": "One", "type": "movie"}],
                    }
                },
                "timeline": {"done": True},
            },
        )

        snap = sync._summary_snapshot_for_response()

        assert snap["run_id"] == "run-restored"
        assert snap["running"] is False
        assert snap["exit_code"] == 0
        assert snap["timeline"] == {"start": True, "pre": True, "post": True, "done": True}
        assert snap["features"]["watchlist"]["added"] == 3
        assert sync._summary_snapshot()["run_id"] == "run-restored"
    finally:
        sync.SUMMARY.clear()
        close_conn()


def test_summary_stream_replays_only_active_run_logs() -> None:
    from api import syncAPI as sync

    buf = ["old start", "old progress", "old done"]

    assert sync._summary_stream_replay_start_index(
        buf,
        {"running": False, "exit_code": 0, "timeline": {"done": True}},
    ) == len(buf)
    assert sync._summary_stream_replay_start_index(
        buf,
        {"running": True, "exit_code": None, "timeline": {"start": True, "done": False}},
    ) == 0
    assert sync._summary_stream_replay_start_index(
        buf,
        {"running": True, "exit_code": None, "timeline": {"start": True, "done": False}},
        log_visible=False,
    ) == len(buf)

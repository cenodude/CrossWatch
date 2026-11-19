# tests/test_settings_overview_and_plan_state.py
# CrossWatch - Settings overview, Events profile chip and advanced plan state tests
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import sqlite3
from pathlib import Path

import pytest


def _runs_db(rows: list[tuple[str, int, str, int, int]]) -> sqlite3.Connection:
    conn = sqlite3.connect(":memory:")
    conn.execute("CREATE TABLE sync_runs (run_id TEXT PRIMARY KEY, finished_at INTEGER, status TEXT, errors INTEGER, dry_run INTEGER)")
    conn.executemany("INSERT INTO sync_runs VALUES (?,?,?,?,?)", rows)
    return conn


@pytest.mark.parametrize("rows,expected", [
    ([("ok", 100, "done", 0, 0), ("failed", 300, "done", 2, 0), ("dry", 400, "done", 0, 1), ("busy", 500, "running", 0, 0)], 100),
    ([], 0),
])
def test_last_successful_sync_comes_from_finished_runs(monkeypatch, rows, expected):
    from api import insightAPI
    from cw_platform.event_archive import db

    monkeypatch.setattr(db, "get_conn", lambda: _runs_db(rows))

    assert insightAPI._settings_last_sync_at() == expected


def test_cancelled_run_is_not_a_successful_sync(monkeypatch):
    from api import insightAPI
    from cw_platform.event_archive import db, recorder

    conn = db.connect(":memory:")
    for run_id, finished, cancelled in (("finished-run", 1_000, False), ("cancelled-run", 2_000, True)):
        monkeypatch.setattr(recorder.time, "time", lambda value=finished: value)
        run = recorder.RunRecorder(lambda *_a, **_k: None, run_id=run_id, conn=conn)
        run.emit("run:start", mode="v3")
        run.emit("run:done", pairs=1, errors=0, cancelled=cancelled)
    monkeypatch.setattr(db, "get_conn", lambda: conn)

    statuses = dict(conn.execute("SELECT run_id, status FROM sync_runs").fetchall())
    assert statuses == {"finished-run": "done", "cancelled-run": "cancelled"}
    assert insightAPI._settings_last_sync_at() == 1_000


def test_settings_overview_drops_the_activity_card():
    api = Path("api/insightAPI.py").read_text("utf-8")
    js = Path("assets/js/settings-insight.js").read_text("utf-8")

    assert "recent_activity" not in api
    assert '"last_sync_at": last_sync_at' in api
    assert "Recent activity" not in js
    assert "activityRows" not in js
    assert "data?.last_sync_at" in js


def test_events_profile_chip_refreshes_after_profiles_load():
    js = Path("assets/js/events/index.js").read_text("utf-8")
    block = js[js.index('const data = await fjson("/api/user-profiles"'):]
    block = block[:block.index("} catch {}")]

    assert "window.CW?.OverviewProfile?.label" in js
    assert "updateHidden();" in block
    assert 'eventScope = "";' in block


def test_advanced_plan_off_is_visible():
    js = Path("assets/js/scheduler.js").read_text("utf-8")

    assert '$("#schAdv")?.classList.toggle("is-plan-off", !_advEnabled);' in js
    assert '"Not running, advanced plan is off"' in js
    assert "#schAdv.is-plan-off .sch-adv-section{opacity:.55" in js

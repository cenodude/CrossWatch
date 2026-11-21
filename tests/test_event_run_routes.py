# tests/test_event_run_routes.py
# CrossWatch - Sync run thread summaries name their pair routes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from cw_platform.event_archive.groups import _summarize


def _plan(src, dst, si="default", di="default", feature="watchlist"):
    return {"event_type": "plan_created", "feature": feature, "pair_key": "-".join(sorted((src, dst))),
            "source_provider": src, "source_instance": si, "destination_provider": dst, "destination_instance": di}


def _run(*plans, errors=0):
    return [{"event_type": "sync_run_started"}, *plans, {"event_type": "sync_run_finished", "detail": {"errors": errors}}]


def test_one_way_and_two_way_routes():
    events = _run(_plan("PLEX", "TRAKT"), _plan("SIMKL", "TRAKT"), _plan("TRAKT", "SIMKL"))
    assert _summarize("completed", events, "", "", "", "") == (
        "Sync run completed successfully, PLEX → TRAKT · SIMKL ⇄ TRAKT (Watchlist)"
    )


def test_instances_shown_when_not_default():
    events = _run(_plan("PLEX", "TRAKT", si="home"))
    assert _summarize("completed", events, "", "", "", "").endswith("PLEX home → TRAKT (Watchlist)")


def test_same_source_is_grouped():
    events = _run(*[_plan("PLEX", d) for d in ("MDBLIST", "SIMKL", "TRAKT")], _plan("EMBY", "TRAKT"))
    assert _summarize("completed", events, "", "", "", "").endswith("PLEX → MDBLIST, SIMKL, TRAKT · EMBY → TRAKT (Watchlist)")


def test_many_routes_are_capped():
    events = _run(*[_plan(p, "TRAKT") for p in ("PLEX", "EMBY", "JELLYFIN", "SIMKL", "MDBLIST")],
                  *[_plan("PLEX", d) for d in ("SIMKL", "MDBLIST", "ANILIST", "FLOPPY")])
    assert _summarize("completed", events, "", "", "", "").endswith(
        "PLEX → TRAKT, SIMKL, MDBLIST +2 more · EMBY → TRAKT · JELLYFIN → TRAKT +2 more (Watchlist)"
    )


def test_errors_keep_routes():
    events = _run(_plan("PLEX", "TRAKT"), errors=2)
    assert _summarize("failed", events, "", "", "", "") == "Sync run completed with errors, 2 errors, PLEX → TRAKT"


def test_sync_users_shown_by_name():
    carmen = {**_plan("PLEX", "SILO", si="default~176467484", di="default~62310e3f", feature="history"), "detail": {"sync_user": "Carmen"}}
    owner = {**_plan("PLEX", "SILO", si="default~1", di="default~4b5eadfa", feature="history"), "detail": {"sync_user": "Owner"}}
    assert _summarize("completed", _run(carmen, owner), "", "", "", "") == (
        "Sync run completed successfully, PLEX → SILO (Carmen, Owner) (History)"
    )


def test_recorder_tags_pair_events_with_sync_user():
    from cw_platform.event_archive.recorder import RunRecorder

    rec = RunRecorder(lambda *a, **k: None, run_id="r1")
    rec.emit("run:pair", src="PLEX", dst="SILO", sync_user="Carmen")
    rec.emit("feature:start", src="PLEX", dst="SILO", feature="history")
    rec.emit("one:plan", adds=1)
    rec.emit("run:pair", src="PLEX", dst="TRAKT")
    rec.emit("one:plan", adds=0)
    plans = [r for r in rec._buf if r["event_type"] == "plan_created"]
    assert '"sync_user": "Carmen"' in plans[0]["detail"]
    assert "sync_user" not in plans[1]["detail"]


def test_refresh_run_summaries_once(tmp_path):
    from cw_platform.event_archive.db import connect
    from cw_platform.event_archive.groups import correlate, refresh_run_summaries
    from cw_platform.event_archive.recorder import make_event, record_events

    c = connect(tmp_path / "events.db")
    rows = [make_event(event_type="sync_run_started", run_id="r1", created_at=1),
            make_event(event_type="plan_created", run_id="r1", created_at=2, feature="watchlist", pair_key="PLEX-TRAKT",
                       source_provider="PLEX", destination_provider="TRAKT"),
            make_event(event_type="sync_run_finished", run_id="r1", created_at=3)]
    record_events(rows, conn=c)
    correlate(conn=c)
    with c:
        c.execute("UPDATE event_groups SET summary='old'")
    assert refresh_run_summaries(conn=c) == 1
    assert c.execute("SELECT summary FROM event_groups").fetchone()[0] == "Sync run completed successfully, PLEX → TRAKT (Watchlist)"
    with c:
        c.execute("UPDATE event_groups SET summary='old'")
    assert refresh_run_summaries(conn=c) == 0
    c.close()


def test_two_way_plan_records_both_directions_with_counts():
    from cw_platform.event_archive.recorder import RunRecorder

    rec = RunRecorder(lambda *a, **k: None, run_id="r1")
    rec.emit("two:start", a="SIMKL", b="TRAKT", feature="watchlist")
    rec.emit("two:plan", add_to_A=2, add_to_B=0, upd_to_A=0, upd_to_B=1, rem_from_A=0, rem_from_B=3)
    plans = [r for r in rec._buf if r["event_type"] == "plan_created"]
    assert [(p["source_provider"], p["destination_provider"]) for p in plans] == [("SIMKL", "TRAKT"), ("TRAKT", "SIMKL")]
    assert '"adds": 0' in plans[0]["detail"] and '"removes": 3' in plans[0]["detail"] and '"updates": 1' in plans[0]["detail"]
    assert '"adds": 2' in plans[1]["detail"]


def test_plan_only_thread_names_its_counts():
    plans = [{**_plan("SIMKL", "TRAKT"), "detail": {"adds": 0, "removes": 3, "updates": 1}},
             {**_plan("TRAKT", "SIMKL"), "detail": {"adds": 2, "removes": 0, "updates": 0}}]
    assert _summarize("completed", plans, "watchlist", "", "", "") == "SIMKL ⇄ TRAKT, Watchlist plan, 2 to add, 3 to remove, 1 to update"
    empty = [{**_plan("PLEX", "TRAKT"), "detail": {"adds": 0, "removes": 0, "updates": 0}}]
    assert _summarize("completed", empty, "watchlist", "TRAKT", "", "") == "PLEX → TRAKT, Watchlist aligned, no changes needed"


def test_quiet_runs_list_their_pairs(tmp_path):
    from cw_platform.event_archive.activity import Scope, day_runs
    from cw_platform.event_archive.db import connect
    from cw_platform.event_archive.recorder import make_event, record_events, record_run_started

    c = connect(tmp_path / "events.db")
    record_run_started("r1", started_at=1000, conn=c)
    record_events([make_event(event_type="plan_created", run_id="r1", created_at=1001, feature="watchlist",
                              source_provider="PLEX", destination_provider="TRAKT", destination_instance="TRAKT-P01")], conn=c)
    run = day_runs(since=0, until=2000, conn=c)["runs"][0]
    assert run["pair_rows"] == []
    assert run["plans"] == [{"feature": "watchlist", "src_provider": "PLEX", "src_instance": "default",
                             "dst_provider": "TRAKT", "dst_instance": "trakt-p01"}]
    assert day_runs(since=0, until=2000, scope=Scope(unrestricted=False, pair_ids=["p1"]), conn=c)["runs"] == []
    c.close()

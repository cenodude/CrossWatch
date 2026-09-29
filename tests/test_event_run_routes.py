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

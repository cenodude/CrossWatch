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
        "Sync run completed successfully, PLEX → TRAKT, SIMKL ⇄ TRAKT (Watchlist)"
    )


def test_instances_shown_when_not_default():
    events = _run(_plan("PLEX", "TRAKT", si="home"))
    assert _summarize("completed", events, "", "", "", "").endswith("PLEX home → TRAKT (Watchlist)")


def test_many_routes_are_capped():
    events = _run(*[_plan(p, "TRAKT") for p in ("PLEX", "EMBY", "JELLYFIN", "SIMKL", "MDBLIST")])
    assert _summarize("completed", events, "", "", "", "").endswith(
        "PLEX → TRAKT, EMBY → TRAKT, JELLYFIN → TRAKT +2 more (Watchlist)"
    )


def test_errors_keep_routes():
    events = _run(_plan("PLEX", "TRAKT"), errors=2)
    assert _summarize("failed", events, "", "", "", "") == "Sync run completed with errors, 2 errors, PLEX → TRAKT"

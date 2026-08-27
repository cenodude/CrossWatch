from __future__ import annotations

from cw_platform.event_archive.db import connect
from cw_platform.event_archive.recorder import RunRecorder


def _emit_blackbox_run(
    conn,
    run_id: str,
    *,
    pair: str = "MDBLIST-SIMKL",
    dst: str = "SIMKL",
    item_key: str = "tmdb:123",
) -> None:
    recorder = RunRecorder(lambda *_a, **_k: None, run_id=run_id, conn=conn)
    recorder.emit("two:start", feature="watchlist", a="MDBLIST", b="SIMKL")
    recorder.emit(
        "debug",
        msg="blocked.counts",
        feature="watchlist",
        dst=dst,
        pair=pair,
        blocked_blackbox=1,
        blocked_total=1,
        blackbox_items=[
            {
                "key": item_key,
                "item": {
                    "title": "Already Blocked",
                    "year": 2024,
                    "type": "movie",
                    "ids": {"tmdb": 123},
                },
            }
        ],
    )
    recorder.close()


def test_blackbox_blocked_items_are_coalesced_across_runs() -> None:
    conn = connect(":memory:")

    _emit_blackbox_run(conn, "run-1")
    _emit_blackbox_run(conn, "run-2", pair="SIMKL-MDBLIST")

    item_rows = conn.execute(
        "SELECT COUNT(*) FROM events WHERE event_type='blackbox_blocked' AND item_key='tmdb:123'"
    ).fetchone()[0]
    aggregate_rows = conn.execute(
        "SELECT COUNT(*) FROM events WHERE event_type='blackbox_blocked' AND item_key IS NULL"
    ).fetchone()[0]

    assert item_rows == 1
    assert aggregate_rows == 2


def test_blackbox_blocked_items_are_scoped_by_pair_and_destination() -> None:
    conn = connect(":memory:")

    _emit_blackbox_run(conn, "run-1", pair="MDBLIST-SIMKL", dst="SIMKL")
    _emit_blackbox_run(conn, "run-2", pair="JELLYFIN-MDBLIST", dst="MDBLIST")

    item_rows = conn.execute(
        "SELECT COUNT(*) FROM events WHERE event_type='blackbox_blocked' AND item_key='tmdb:123'"
    ).fetchone()[0]

    assert item_rows == 2

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync.tautulli import _history as history


START = 1704067200
RESUME = START + 1800
STOP = START + 3600


def _iso(epoch: int) -> str:
    return datetime.fromtimestamp(epoch, timezone.utc).isoformat().replace("+00:00", "Z")


def _row(**overrides: Any) -> dict[str, Any]:
    return {
        "row_id": 10,
        "reference_id": 10,
        "media_type": "movie",
        "title": "Example movie",
        "guid": "com.plexapp.agents.themoviedb://550",
        "started": START,
        "date": START,
        "stopped": STOP,
        "watched_status": 1,
        **overrides,
    }


def _adapter(rows: list[dict[str, Any]], *, rewatches: bool = False) -> SimpleNamespace:
    def call(cmd: str, **params: Any) -> dict[str, Any]:
        assert cmd == "get_history"
        start, length = params["start"], params["length"]
        return {"data": rows[start:start + length], "recordsFiltered": len(rows)}

    return SimpleNamespace(cfg={"_cw_history_rewatches": rewatches}, client=SimpleNamespace(call=call))


@pytest.mark.parametrize("rewatches", [False, True])
@pytest.mark.parametrize("grouped", [False, True])
@pytest.mark.parametrize("media_type", ["movie", "episode"])
def test_completion_time_wins_over_start_fields(rewatches, grouped, media_type):
    row = _row(
        media_type=media_type,
        parent_media_index=1,
        media_index=2,
        date=RESUME if grouped else START,
        group_count=2 if grouped else 1,
        group_ids="10,11" if grouped else "10",
        time=START - 60,
    )
    out = history.build_index(_adapter([row], rewatches=rewatches))

    assert len(out) == 1
    assert next(iter(out.values()))["watched_at"] == _iso(STOP)
    if rewatches:
        assert next(iter(out)).endswith(f"@{STOP}")


@pytest.mark.parametrize("stopped", [STOP, str(STOP), STOP * 1000, str(STOP * 1000), _iso(STOP)])
def test_supported_completion_timestamp_formats(stopped):
    out = history.build_index(_adapter([_row(stopped=stopped)]))
    assert next(iter(out.values()))["watched_at"] == _iso(STOP)


@pytest.mark.parametrize(
    "invalid",
    [None, "", "garbage", 0, "0", -1, "-1", True, False, float("nan"), float("inf"),
     10**30, "1969-12-31T23:59:59Z", "1970-01-01T00:00:00Z", {}, []],
)
@pytest.mark.parametrize("fallback", ["date", "started", "time"])
def test_invalid_timestamp_fields_fall_back_in_order(invalid, fallback):
    row = _row(stopped=invalid, date=invalid, started=invalid, time=invalid)
    row[fallback] = START
    fields = ("stopped", "date", "started", "time")
    for field in fields[fields.index(fallback) + 1:]:
        row[field] = STOP

    out = history.build_index(_adapter([row]))
    assert next(iter(out.values()))["watched_at"] == _iso(START)


def test_missing_completion_field_uses_legacy_date():
    row = _row()
    del row["stopped"]
    out = history.build_index(_adapter([row]))
    assert next(iter(out.values()))["watched_at"] == _iso(START)


def test_no_valid_timestamp_skips_only_the_invalid_row():
    invalid = _row(stopped="invalid", date=None, started=0, time=False)
    out = history.build_index(_adapter([invalid, _row()], rewatches=True))
    assert list(out) == [f"tmdb:550@{STOP}"]


@pytest.mark.parametrize("separation", [0, 1, 86400])
def test_genuine_watches_remain_separate_even_with_close_or_equal_timestamps(separation):
    rows = [_row(), _row(row_id=20, reference_id=20, stopped=STOP + separation)]
    out = history.build_index(_adapter(rows, rewatches=True))

    assert len(out) == 2
    assert sorted(item["watched_at"] for item in out.values()) == [_iso(STOP), _iso(STOP + separation)]
    assert all(item["_cw_rewatch_sync"] for item in out.values())


@pytest.mark.parametrize("rewatches", [False, True])
@pytest.mark.parametrize("watched_only", [False, True])
@pytest.mark.parametrize("nested_payload", [False, True])
def test_requests_grouped_completed_history_on_every_page(rewatches, watched_only, nested_payload):
    segments = [
        _row(row_id=11, date=RESUME, started=RESUME, watched_status=1),
        _row(stopped=START + 900, watched_status=0.25),
    ]
    grouped = _row(row_id=11, date=RESUME, group_count=2, group_ids="10,11")
    rewatch = _row(row_id=20, reference_id=20, date=STOP + 1, started=STOP + 1, stopped=STOP + 3600)
    live = _row(row_id=None, reference_id=None, date=STOP + 7200, stopped=0, state="playing")
    calls = []

    def call(cmd: str, **params: Any) -> dict[str, Any]:
        assert cmd == "get_history"
        calls.append(params)
        rows = [rewatch, grouped] if params.get("grouping", 0) else [rewatch, *segments]
        if params.get("include_activity", 1):
            rows = [live, *rows]
        start, length = params["start"], params["length"]
        payload = {"data": rows[start:start + length], "recordsFiltered": len(rows)}
        return {"data": payload} if nested_payload else payload

    cfg = {
        "_cw_history_rewatches": rewatches,
        "tautulli": {"history": {"user_id": "7", "per_page": 1, "watched_only": watched_only}},
    }
    out = history.build_index(SimpleNamespace(cfg=cfg, client=SimpleNamespace(call=call)))

    expected = [f"tmdb:550@{STOP}", f"tmdb:550@{STOP + 3600}"] if rewatches else ["tmdb:550"]
    assert sorted(out) == expected
    assert sorted(item["watched_at"] for item in out.values()) == (
        [_iso(STOP), _iso(STOP + 3600)] if rewatches else [_iso(STOP + 3600)]
    )
    assert [params["start"] for params in calls] == [0, 1]
    assert all(params["grouping"] == 1 and params["include_activity"] == 0 for params in calls)
    assert all(params["user_id"] == "7" for params in calls)


def test_non_rewatch_import_keeps_latest_completion_across_pages():
    rows = [_row(date=RESUME, started=RESUME), _row(row_id=20, reference_id=20, stopped=STOP + 60)]
    out = history.build_index(_adapter(rows), per_page=1)

    assert list(out) == ["tmdb:550"]
    assert out["tmdb:550"]["watched_at"] == _iso(STOP + 60)
    assert "_cw_rewatch_sync" not in out["tmdb:550"]

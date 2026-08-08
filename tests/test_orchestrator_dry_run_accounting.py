# CrossWatch test scripts
from __future__ import annotations

from typing import Any

from cw_platform.orchestrator._applier import apply_add, apply_remove, apply_update


class _Ops:
    """Mirrors the provider contract: a dry run reports every item as written."""

    def __init__(self) -> None:
        self.add_calls: list[dict[str, Any]] = []
        self.remove_calls: list[dict[str, Any]] = []

    def add(self, cfg, items, *, feature, dry_run=False):
        self.add_calls.append({"items": list(items), "dry_run": dry_run})
        if dry_run:
            return {"ok": True, "count": len(list(items)), "dry_run": True}
        return {"ok": True, "count": len(list(items)), "confirmed_keys": [str(i["key"]) for i in items]}

    def remove(self, cfg, items, *, feature, dry_run=False):
        self.remove_calls.append({"items": list(items), "dry_run": dry_run})
        if dry_run:
            return {"ok": True, "count": len(list(items)), "dry_run": True}
        return {"ok": True, "count": len(list(items)), "confirmed_keys": [str(i["key"]) for i in items]}


def _items(n):
    return [{"key": f"imdb:tt{i:04d}", "type": "movie", "ids": {"imdb": f"tt{i:04d}"}} for i in range(n)]


def _run(fn, ops, dry_run):
    events: list[tuple[str, dict[str, Any]]] = []
    res = fn(
        dst_ops=ops,
        cfg={},
        dst_name="SIMKL",
        feature="history",
        items=_items(5),
        dry_run=dry_run,
        emit=lambda name, **kw: events.append((name, kw)),
        dbg=lambda *a, **k: None,
        chunk_size=0,
        chunk_pause_ms=0,
    )
    return res, events


def _done(events, name):
    return next(kw for ev, kw in events if ev == name)


def test_dry_run_add_confirms_nothing():
    res, events = _run(apply_add, _Ops(), True)

    assert res["confirmed"] == 0
    assert res["count"] == 0
    assert res["confirmed_keys"] == []
    assert res["dry_run"] is True
    done = _done(events, "apply:add:done")
    assert done["added"] == 0
    assert done["count"] == 0
    assert done["attempted"] == 5
    assert done["dry_run"] is True


def test_dry_run_remove_confirms_nothing():
    res, events = _run(apply_remove, _Ops(), True)

    assert res["confirmed"] == 0
    assert res["dry_run"] is True
    done = _done(events, "apply:remove:done")
    assert done["removed"] == 0
    assert done["dry_run"] is True


def test_dry_run_update_confirms_nothing():
    res, events = _run(apply_update, _Ops(), True)

    assert res["confirmed"] == 0
    assert res["dry_run"] is True
    done = _done(events, "apply:update:done")
    assert done["updated"] == 0
    assert done["dry_run"] is True


def test_dry_run_still_reports_what_would_be_attempted():
    _, events = _run(apply_add, _Ops(), True)

    start = _done(events, "apply:add:start")
    assert start["count"] == 5


def test_real_add_still_confirms():
    res, events = _run(apply_add, _Ops(), False)

    assert res["confirmed"] == 5
    assert res.get("dry_run") is not True
    done = _done(events, "apply:add:done")
    assert done["added"] == 5
    assert done["dry_run"] is False


def test_real_remove_still_confirms():
    res, events = _run(apply_remove, _Ops(), False)

    assert res["confirmed"] == 5
    done = _done(events, "apply:remove:done")
    assert done["removed"] == 5
    assert done["dry_run"] is False

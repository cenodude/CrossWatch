from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Iterable, Mapping

import pytest

from cw_platform import run_control
from cw_platform.id_map import canonical_key
from cw_platform.orchestrator.facade import Orchestrator


@pytest.fixture(autouse=True)
def _clear_cancel():
    run_control.clear_cancel()
    yield
    run_control.clear_cancel()


@dataclass
class FakeOps:
    provider: str
    index: dict[str, dict[str, Any]]
    hooks: dict[str, Any] = field(default_factory=dict)
    index_calls: list[str] = field(default_factory=list)
    add_calls: list[list[dict[str, Any]]] = field(default_factory=list)
    remove_calls: list[list[dict[str, Any]]] = field(default_factory=list)

    def name(self) -> str:
        return self.provider

    def label(self) -> str:
        return self.provider

    def features(self) -> Mapping[str, bool]:
        return {"watchlist": True}

    def capabilities(self) -> Mapping[str, Any]:
        return {"features": {"watchlist": True}, "observed_deletes": True, "index_semantics": "present"}

    def is_configured(self, cfg: Mapping[str, Any]) -> bool:
        return True

    def health(self, cfg: Mapping[str, Any], **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"watchlist": True}, "api": {}}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        self.index_calls.append(feature)
        hook = self.hooks.get("build_index")
        if hook:
            hook(self)
        return dict(self.index)

    def add(self, cfg, items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False):
        batch = [dict(x) for x in items]
        self.add_calls.append(batch)
        if not dry_run:
            for it in batch:
                k = canonical_key(it)
                if k:
                    self.index[k] = dict(it)
        return {"ok": True, "added": len(batch), "count": len(batch)}

    def remove(self, cfg, items: Iterable[Mapping[str, Any]], *, feature: str, dry_run: bool = False):
        batch = [dict(x) for x in items]
        self.remove_calls.append(batch)
        return {"ok": True, "removed": len(batch), "count": len(batch)}


def _movie(tag: str) -> dict[str, Any]:
    return {"type": "movie", "title": tag, "year": 2000, "ids": {"imdb": f"tt{tag}"}}


def _cfg(pairs: list[dict[str, Any]]) -> dict[str, Any]:
    return {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": False, "allow_mass_delete": True},
        "pairs": pairs,
    }


def _pair(pid: str, src: str, dst: str) -> dict[str, Any]:
    return {
        "id": pid, "enabled": True, "source": src, "target": dst, "mode": "one-way",
        "feature": "watchlist",
        "features": {"watchlist": {"enable": True, "add": True, "remove": False}},
    }


def _wire(monkeypatch, providers: dict[str, FakeOps]) -> None:
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: providers)
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)


def test_cancel_during_index_build_skips_the_write(config_base, monkeypatch) -> None:
    src = FakeOps("SRC", {"imdb:tt01": _movie("01")})
    dst = FakeOps("DST", {})
    seen = []

    def hook(ops: FakeOps) -> None:
        seen.append(ops.provider)
        if len(seen) == 2:
            run_control.request_cancel()

    src.hooks["build_index"] = hook
    dst.hooks["build_index"] = hook
    _wire(monkeypatch, {"SRC": src, "DST": dst})

    result = Orchestrator(_cfg([_pair("p1", "SRC", "DST")])).run()

    assert len(seen) == 2
    assert dst.add_calls == []
    assert result["cancelled"] is True
    assert result["added"] == 0


def test_cancel_stops_before_the_second_pair(config_base, monkeypatch) -> None:
    a1 = FakeOps("A1", {"imdb:tt01": _movie("01")})
    b1 = FakeOps("B1", {})
    a2 = FakeOps("A2", {"imdb:tt02": _movie("02")})
    b2 = FakeOps("B2", {})

    def hook(ops: FakeOps) -> None:
        run_control.request_cancel()

    b1.hooks["build_index"] = hook
    _wire(monkeypatch, {"A1": a1, "B1": b1, "A2": a2, "B2": b2})

    result = Orchestrator(_cfg([_pair("p1", "A1", "B1"), _pair("p2", "A2", "B2")])).run()

    assert b1.add_calls == []
    assert a2.index_calls == []
    assert b2.index_calls == []
    assert result["cancelled"] is True


def test_uncancelled_run_still_writes(config_base, monkeypatch) -> None:
    src = FakeOps("SRC", {"imdb:tt01": _movie("01")})
    dst = FakeOps("DST", {})
    _wire(monkeypatch, {"SRC": src, "DST": dst})

    result = Orchestrator(_cfg([_pair("p1", "SRC", "DST")])).run()

    assert [it["ids"]["imdb"] for it in dst.add_calls[0]] == ["tt01"]
    assert result["cancelled"] is False
    assert result["added"] == 1


def _twoway(pid: str, src: str, dst: str) -> dict[str, Any]:
    return {
        "id": pid, "enabled": True, "source": src, "target": dst, "mode": "two-way",
        "feature": "watchlist",
        "features": {"watchlist": {"enable": True, "add": True, "remove": True}},
    }


def test_one_cancel_stops_every_remaining_pair(config_base, monkeypatch) -> None:
    ops: dict[str, FakeOps] = {}
    for i in (1, 2, 3, 4):
        ops[f"S{i}"] = FakeOps(f"S{i}", {f"imdb:tt{i}": _movie(str(i))})
        ops[f"D{i}"] = FakeOps(f"D{i}", {})
    ops["D1"].hooks["build_index"] = lambda _ops: run_control.request_cancel()
    _wire(monkeypatch, ops)

    cfg = _cfg([_twoway(f"p{i}", f"S{i}", f"D{i}") for i in (1, 2, 3, 4)])
    cfg["sync"]["enable_remove"] = True
    result = Orchestrator(cfg).run()

    assert result["cancelled"] is True
    for i in (2, 3, 4):
        assert ops[f"S{i}"].index_calls == [], f"pair {i} source was read"
        assert ops[f"D{i}"].index_calls == [], f"pair {i} target was read"
        assert ops[f"D{i}"].add_calls == [], f"pair {i} was written"


def test_cancel_marks_queue_stopped_until_consumed() -> None:
    run_control.clear_queue_stop()
    assert run_control.queue_stopped() is False

    run_control.request_cancel("run-1")
    assert run_control.queue_stopped() is True

    run_control.clear_cancel()
    assert run_control.queue_stopped() is True, "a finished run must not clear the queue stop"

    assert run_control.consume_queue_stop() is True
    assert run_control.queue_stopped() is False
    assert run_control.consume_queue_stop() is False

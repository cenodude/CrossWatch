# CrossWatch test scripts
from __future__ import annotations

from typing import Any, Mapping

import pytest

from cw_platform.orchestrator import _snapshots
from cw_platform.orchestrator._snapshots import (
    build_snapshots_for_feature,
    prepare_source_snapshot,
)


class _Ops:
    def __init__(self, name: str, index: Mapping[str, Any], trace: list[str], *, hook: bool = False):
        self._name = name
        self._index = dict(index)
        self._trace = trace
        self.prepared: list[tuple[str, dict[str, Any]]] = []
        if hook:
            setattr(self, "prepare_source_snapshot", self._prepare)

    def name(self) -> str:
        return self._name

    def features(self) -> Mapping[str, bool]:
        return {"history": True}

    def build_index(self, cfg: Mapping[str, Any], *, feature: str) -> Mapping[str, dict[str, Any]]:
        self._trace.append(f"build:{self._name}")
        return dict(self._index)

    def _prepare(self, cfg: Mapping[str, Any], *, feature: str, items: Mapping[str, Any]) -> int:
        self._trace.append(f"prepare:{self._name}")
        self.prepared.append((feature, dict(items)))
        return len(items)


def _run(providers, *, build_order, on_snapshot, monkeypatch):
    monkeypatch.setattr(_snapshots, "provider_configured", lambda _cfg, _name: True)
    return build_snapshots_for_feature(
        feature="history",
        config={},
        providers=providers,
        snap_cache={},
        snap_ttl_sec=0,
        dbg=lambda *a, **k: None,
        emit_info=lambda _m: None,
        build_order=build_order,
        on_snapshot=on_snapshot,
    )


def test_source_is_prepared_before_destination_is_built(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[str] = []
    item = {
        "type": "episode",
        "season": 2,
        "episode": 4,
        "ids": {"tvdb": "5057304"},
        "show_ids": {"tmdb": "42009"},
    }
    src = _Ops("SRC", {"tmdb:42009#s02e04": item}, trace)
    dst = _Ops("DST", {}, trace, hook=True)

    def _on_snapshot(name: str, idx: Mapping[str, Any]) -> None:
        if name == "SRC":
            prepare_source_snapshot(dst, config={}, feature="history", items=idx)

    snaps = _run({"SRC": src, "DST": dst}, build_order=["SRC", "DST"], on_snapshot=_on_snapshot, monkeypatch=monkeypatch)

    assert trace == ["build:SRC", "prepare:DST", "build:DST"]
    assert set(snaps) == {"SRC", "DST"}
    assert dst.prepared and dst.prepared[0][0] == "history"
    assert list(dst.prepared[0][1].values())[0]["ids"] == {"tvdb": "5057304"}


def test_build_order_is_respected(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[str] = []
    a = _Ops("A", {}, trace)
    b = _Ops("B", {}, trace)

    _run({"A": a, "B": b}, build_order=["B", "A"], on_snapshot=None, monkeypatch=monkeypatch)

    assert trace == ["build:B", "build:A"]


def test_provider_without_hook_is_unchanged(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[str] = []
    src = _Ops("SRC", {"k1": {"type": "episode"}}, trace)
    dst = _Ops("DST", {}, trace)

    assert not hasattr(dst, "prepare_source_snapshot")

    def _on_snapshot(name: str, idx: Mapping[str, Any]) -> None:
        if name == "SRC":
            assert prepare_source_snapshot(dst, config={}, feature="history", items=idx) is False

    _run({"SRC": src, "DST": dst}, build_order=["SRC", "DST"], on_snapshot=_on_snapshot, monkeypatch=monkeypatch)

    assert trace == ["build:SRC", "build:DST"]


def test_prepare_hook_failure_does_not_break_snapshotting(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[str] = []
    src = _Ops("SRC", {"k1": {"type": "episode"}}, trace)
    dst = _Ops("DST", {}, trace, hook=True)

    def _boom(cfg, *, feature, items):
        raise RuntimeError("hook exploded")

    setattr(dst, "prepare_source_snapshot", _boom)

    def _on_snapshot(name: str, idx: Mapping[str, Any]) -> None:
        if name == "SRC":
            assert prepare_source_snapshot(dst, config={}, feature="history", items=idx) is False

    snaps = _run({"SRC": src, "DST": dst}, build_order=["SRC", "DST"], on_snapshot=_on_snapshot, monkeypatch=monkeypatch)

    assert trace == ["build:SRC", "build:DST"]
    assert set(snaps) == {"SRC", "DST"}


def test_empty_source_snapshot_skips_hook(monkeypatch: pytest.MonkeyPatch) -> None:
    trace: list[str] = []
    src = _Ops("SRC", {}, trace)
    dst = _Ops("DST", {}, trace, hook=True)

    def _on_snapshot(name: str, idx: Mapping[str, Any]) -> None:
        if name == "SRC":
            assert prepare_source_snapshot(dst, config={}, feature="history", items=idx) is False

    _run({"SRC": src, "DST": dst}, build_order=["SRC", "DST"], on_snapshot=_on_snapshot, monkeypatch=monkeypatch)

    assert dst.prepared == []

# CrossWatch test scripts
from __future__ import annotations

from typing import Any

import pytest

from cw_platform.orchestrator import _pairs_twoway as twoway


class _Ops:
    def __init__(self, view: dict[str, Any] | None = None, boom: bool = False) -> None:
        self.calls: list[dict[str, Any]] = []
        self._view = view
        self._boom = boom

    def destination_comparison_view(self, cfg, *, feature, index):
        self.calls.append({"feature": feature, "index": dict(index)})
        if self._boom:
            raise RuntimeError("hook exploded")
        return self._view if self._view is not None else index


def _dbg(*_args, **_kwargs) -> None:
    return None


def test_returns_index_when_provider_has_no_hook() -> None:
    index = {"a": {"x": 1}}
    out = twoway._comparison_view(object(), {}, "history", index, side="A", dbg=_dbg)
    assert out == index


def test_applies_the_provider_view() -> None:
    ops = _Ops({"src:1": {"x": 1}})
    out = twoway._comparison_view(ops, {}, "history", {"dst:1": {"x": 1}}, side="A", dbg=_dbg)
    assert out == {"src:1": {"x": 1}}
    assert ops.calls[0]["feature"] == "history"


def test_hook_failure_falls_back_to_the_original_index() -> None:
    index = {"dst:1": {"x": 1}}
    out = twoway._comparison_view(_Ops(boom=True), {}, "history", index, side="A", dbg=_dbg)
    assert out == index


def test_empty_view_is_ignored() -> None:
    index = {"dst:1": {"x": 1}}
    out = twoway._comparison_view(_Ops({}), {}, "history", index, side="A", dbg=_dbg)
    assert out == index


@pytest.mark.parametrize("feature", ["history", "ratings", "progress"])
def test_view_is_used_for_comparison_features(feature: str) -> None:
    ops = _Ops({"src:1": {"x": 1}})
    twoway._comparison_view(ops, {}, feature, {"dst:1": {"x": 1}}, side="B", dbg=_dbg)
    assert len(ops.calls) == 1


def test_source_only_reads_the_hook_when_anime_mapping_is_on() -> None:
    source = twoway.__loader__.get_source(twoway.__name__)
    anchor = source.index("if bool(anime_pair_opts.get(\"use_anime_mapping\", False)):")
    gate = source[anchor : source.index("\n    now = int(_t.time())", anchor)]
    assert "_comparison_view(aops" in gate
    assert "_comparison_view(bops" in gate
    assert source.count("_comparison_view(aops") == 1
    assert source.count("_comparison_view(bops") == 1

from __future__ import annotations

from typing import Any, Mapping

from cw_platform.orchestrator._planner import diff_progress
from cw_platform.orchestrator._progress_completion import fcfg_for_progress_target


class _Ops:
    def __init__(self, progress_caps: Mapping[str, Any]):
        self._progress_caps = dict(progress_caps)

    def capabilities(self) -> dict[str, Any]:
        return {"progress": dict(self._progress_caps)}


def _movie(progress_percent: float, *, duration_ms: int = 7_200_000) -> dict[str, Any]:
    return {
        "type": "movie",
        "title": "Example",
        "year": 2026,
        "ids": {"tmdb": "100"},
        "progress_percent": progress_percent,
        "duration_ms": duration_ms,
    }


def test_publicmetadb_progress_write_policy_skips_at_eighty_percent() -> None:
    target = _Ops({"completion_policy": {"progress_write": {"mode": "auto_complete", "percent": 80}}})
    fcfg = fcfg_for_progress_target({}, target)

    adds_79, clears_79 = diff_progress({"tmdb:100": _movie(79)}, {}, fcfg=fcfg)
    adds_80, clears_80 = diff_progress({"tmdb:100": _movie(80)}, {}, fcfg=fcfg)

    assert len(adds_79) == 1
    assert clears_79 == []
    assert adds_80 == []
    assert clears_80 == []


def test_nuvio_progress_write_policy_uses_ninety_percent_with_duration_floor() -> None:
    target = _Ops({
        "completion_policy": {
            "progress_write": {
                "mode": "auto_complete",
                "percent": 90,
                "min_duration_seconds": 60,
            }
        }
    })
    fcfg = fcfg_for_progress_target({}, target)

    adds_89, _ = diff_progress({"tmdb:100": _movie(89)}, {}, fcfg=fcfg)
    adds_90, _ = diff_progress({"tmdb:100": _movie(90)}, {}, fcfg=fcfg)
    short_90, _ = diff_progress({"tmdb:100": _movie(90, duration_ms=30_000)}, {}, fcfg=fcfg)

    assert len(adds_89) == 1
    assert adds_90 == []
    assert len(short_90) == 1


def test_stop_scrobble_threshold_does_not_cap_progress_writes() -> None:
    target = _Ops({
        "completion_policy": {
            "progress_write": {"mode": "none"},
            "stop_scrobble": {"marks_watched_percent": 80, "comparison": "gte"},
        }
    })
    fcfg = fcfg_for_progress_target({}, target)

    adds_80, clears_80 = diff_progress({"tmdb:100": _movie(80)}, {}, fcfg=fcfg)

    assert len(adds_80) == 1
    assert clears_80 == []

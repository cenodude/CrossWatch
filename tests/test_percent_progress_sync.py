from __future__ import annotations

from pathlib import Path


def test_diff_progress_upserts_percent_only_source() -> None:
    from cw_platform.orchestrator._planner import diff_progress

    adds, clears = diff_progress(
        {
            "tmdb:69478#s06e02": {
                "type": "episode",
                "show_ids": {"tmdb": "69478"},
                "season": 6,
                "episode": 2,
                "progress_percent": 21.0,
                "progress_at": "2026-07-25T19:30:00Z",
            }
        },
        {},
    )

    assert clears == []
    assert len(adds) == 1
    assert adds[0]["progress_percent"] == 21.0


def test_diff_progress_clears_percent_only_source() -> None:
    from cw_platform.orchestrator._planner import diff_progress

    adds, clears = diff_progress(
        {
            "tmdb:69478#s06e02": {
                "type": "episode",
                "show_ids": {"tmdb": "69478"},
                "season": 6,
                "episode": 2,
                "progress_percent": 0,
            }
        },
        {
            "tmdb:69478#s06e02": {
                "type": "episode",
                "show_ids": {"tmdb": "69478"},
                "season": 6,
                "episode": 2,
                "progress_percent": 21,
            }
        },
    )

    assert adds == []
    assert len(clears) == 1
    assert clears[0]["progress_ms"] == 0


def test_two_way_minimal_progress_preserves_percent_only_source() -> None:
    from cw_platform.orchestrator._pairs_twoway import _minimal_keep_progress

    item = {
        "type": "episode",
        "show_ids": {"tmdb": "69478"},
        "season": 6,
        "episode": 2,
        "progress_percent": 21.1234,
        "progress_at": "2026-07-25T19:30:00Z",
    }

    out = _minimal_keep_progress(item)

    assert out["progress_percent"] == 21.123
    assert out["progress_at"] == "2026-07-25T19:30:00Z"


def test_crosswatch_progress_accepts_percent_only_items(tmp_path: Path, monkeypatch) -> None:
    from providers.sync.crosswatch import _progress

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_SCOPE", "SIMKL-CROSSWATCH")
    adapter = type("Adapter", (), {"cfg": type("Cfg", (), {"base_path": str(tmp_path)})()})()
    item = {
        "type": "episode",
        "show_ids": {"tmdb": "69478"},
        "season": 6,
        "episode": 2,
        "progress_percent": 21.0,
        "progress_at": "2026-07-25T19:30:00Z",
    }

    count, unresolved = _progress.add(adapter, [item])
    index = _progress.build_index(adapter)

    assert count == 1
    assert unresolved == []
    assert index["tmdb:69478#s06e02"]["progress_percent"] == 21.0

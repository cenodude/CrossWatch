from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any


def _adapter(root: Path) -> Any:
    return type("Adapter", (), {"cfg": type("Cfg", (), {"base_path": str(root)})()})()


def _write_state(path: Path, key: str, tmdb: str, mtime: int) -> None:
    path.write_text(
        json.dumps({"ts": mtime, "items": {key: {"type": "movie", "ids": {"tmdb": tmdb}}}}),
        "utf-8",
    )
    os.utime(path, (mtime, mtime))


def test_crosswatch_source_reads_newer_pair_scoped_state(tmp_path: Path, monkeypatch) -> None:
    from providers.sync.crosswatch import _watchlist

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_SCOPE", "downstream")
    monkeypatch.setenv("CW_PAIR_SRC", "CROSSWATCH")
    _write_state(tmp_path / "watchlist.downstream.json", "tmdb:1", "1", 100)
    _write_state(tmp_path / "watchlist.upstream.json", "tmdb:2", "2", 200)

    index = _watchlist.build_index(_adapter(tmp_path))

    assert "tmdb:2" in index
    assert "tmdb:1" not in index


def test_crosswatch_target_keeps_current_pair_scoped_state(tmp_path: Path, monkeypatch) -> None:
    from providers.sync.crosswatch import _watchlist

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_SCOPE", "downstream")
    monkeypatch.setenv("CW_PAIR_SRC", "SIMKL")
    _write_state(tmp_path / "watchlist.downstream.json", "tmdb:1", "1", 100)
    _write_state(tmp_path / "watchlist.upstream.json", "tmdb:2", "2", 200)

    index = _watchlist.build_index(_adapter(tmp_path))

    assert "tmdb:1" in index
    assert "tmdb:2" not in index


def test_crosswatch_progress_enriches_episode_series_title(monkeypatch) -> None:
    from providers.sync.crosswatch import _progress

    monkeypatch.setattr(
        _progress,
        "_metadata_show_detail",
        lambda adapter, show_ids: {"title": "House of the Dragon", "year": 2022},
    )
    adapter = object()

    item = _progress._accepted(
        {
            "type": "episode",
            "show_ids": {"tmdb": "94997"},
            "season": 2,
            "episode": 7,
            "progress_ms": 1770000,
            "duration_ms": 3821000,
        },
        adapter,
    )

    assert item["series_title"] == "House of the Dragon"
    assert item["series_year"] == 2022
    assert item["title"] == "S02E07"
    assert item["show_ids"] == {"tmdb": "94997"}

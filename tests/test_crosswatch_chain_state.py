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


def test_crosswatch_history_preserves_rich_source_identity(tmp_path: Path) -> None:
    from providers.sync.crosswatch import _history

    rich = {
        "type": "episode",
        "title": "Mission: Forgetter IV",
        "series_title": "Spy Kyoushitsu",
        "year": 2023,
        "season": 2,
        "episode": 5,
        "watched": True,
        "watched_at": "2026-09-01T14:27:40Z",
        "ids": {"tvdb": "9896573", "anidb": "269769", "source_episode": "kodi-14372"},
        "show_ids": {
            "tmdb": "194986",
            "tvdb": "415821",
            "anidb": "17979",
            "mal": "54947",
            "anilist": "163542",
            "source_show": "kodi-show-12",
        },
        "_cw_anime_map": {"absolute": 5, "namespace": "anidb", "target_id": "17979", "release_tag": "v3"},
        "_anime_absolute": 5,
        "simkl_bucket": "anime",
        "_simkl_episode_number": 5,
    }

    added, unresolved = _history.add(_adapter(tmp_path), [rich])
    index = _history.build_index(_adapter(tmp_path))
    item = next(iter(index.values()))

    assert added == 1
    assert unresolved == []
    assert item["ids"]["anidb"] == "269769"
    assert item["ids"]["source_episode"] == "kodi-14372"
    assert item["show_ids"]["anidb"] == "17979"
    assert item["show_ids"]["mal"] == "54947"
    assert item["show_ids"]["source_show"] == "kodi-show-12"
    assert item["_cw_anime_map"]["target_id"] == "17979"
    assert item["_anime_absolute"] == 5
    assert item["_simkl_episode_number"] == 5
    assert item["simkl_bucket"] == "anime"


def test_crosswatch_history_sparse_update_keeps_existing_identity(tmp_path: Path) -> None:
    from providers.sync.crosswatch import _history

    adapter = _adapter(tmp_path)
    first = {
        "type": "episode",
        "series_title": "Spy Kyoushitsu",
        "season": 1,
        "episode": 5,
        "watched_at": "2026-09-01T14:27:40Z",
        "ids": {"tvdb": "9896573", "anidb": "269769"},
        "show_ids": {"tvdb": "415821", "anidb": "17979", "mal": "54947"},
        "_cw_anime_map": {"absolute": 5, "namespace": "anidb", "target_id": "17979", "release_tag": "v3"},
    }
    sparse = {
        "type": "episode",
        "series_title": "Spy Kyoushitsu",
        "season": 1,
        "episode": 5,
        "watched_at": "2026-09-01T14:29:00Z",
        "ids": {"tvdb": "9896573"},
        "show_ids": {"tvdb": "415821"},
    }

    _history.add(adapter, [first])
    _history.add(adapter, [sparse])
    item = next(iter(_history.build_index(adapter).values()))

    assert item["watched_at"] == "2026-09-01T14:29:00Z"
    assert item["ids"] == {"tvdb": "9896573", "anidb": "269769"}
    assert item["show_ids"] == {"tvdb": "415821", "anidb": "17979", "mal": "54947"}
    assert item["_cw_anime_map"]["absolute"] == 5


def test_crosswatch_watchlist_tmdb_enrichment_keeps_source_identity(monkeypatch) -> None:
    from providers.sync.crosswatch import _watchlist

    class Meta:
        def resolve(self, **_kwargs):
            return {"ids": {"tmdb": "194986"}}

    item = {
        "type": "show",
        "title": "Spy Kyoushitsu",
        "ids": {"imdb": "tt20259190", "anidb": "17979", "source_show": "kodi-show-12"},
    }
    monkeypatch.setattr(_watchlist, "_meta", lambda: Meta())

    changed = _watchlist._ensure_tmdb_for_item(item)

    assert changed is True
    assert item["ids"]["tmdb"] == "194986"
    assert item["ids"]["imdb"] == "tt20259190"
    assert item["ids"]["anidb"] == "17979"
    assert item["ids"]["source_show"] == "kodi-show-12"

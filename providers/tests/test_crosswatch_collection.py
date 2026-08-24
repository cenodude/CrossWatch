from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

from providers.sync._mod_CROSSWATCH import CROSSWATCHModule


def _cfg(root: Any) -> dict[str, Any]:
    return {
        "CrossWatch": {
            "connected": True,
            "enabled": True,
            "root_dir": str(root),
            "auto_snapshot": True,
            "max_snapshots": 64,
        }
    }


def test_crosswatch_collection_is_pair_scoped_and_read_write(monkeypatch: Any, tmp_path: Any) -> None:
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_KEY", "plex-mdblist")

    adapter = CROSSWATCHModule(_cfg(tmp_path))
    episode = {
        "type": "episode",
        "series_title": "Severance",
        "show_ids": {"tmdb": "95396"},
        "season": 1,
        "episode": 2,
        "ids": {},
        "collected_at": "2026-08-24T20:00:00Z",
    }

    added = adapter.add("collection", [episode])

    assert added["ok"] is True
    assert added["count"] == 1
    assert added["confirmed_keys"] == ["tmdb:95396#s01e02"]

    state_path = tmp_path / "collection.plex-mdblist.json"
    payload = json.loads(state_path.read_text("utf-8"))
    assert payload["items"]["tmdb:95396#s01e02"]["show_ids"] == {"tmdb": "95396"}
    assert payload["items"]["tmdb:95396#s01e02"]["collected_at"] == "2026-08-24T20:00:00Z"

    index = adapter.build_index("collection")
    assert index["tmdb:95396#s01e02"]["type"] == "episode"
    assert index["tmdb:95396#s01e02"]["collected_at"] == "2026-08-24T20:00:00Z"

    removed = adapter.remove("collection", [episode])
    assert removed["ok"] is True
    assert removed["count"] == 1
    assert removed["confirmed_keys"] == ["tmdb:95396#s01e02"]
    assert adapter.build_index("collection") == {}


def test_crosswatch_collection_reuses_history_style_snapshot_window(monkeypatch: Any, tmp_path: Any) -> None:
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_KEY", "plex-trakt")

    adapter = CROSSWATCHModule(_cfg(tmp_path))
    first = {"type": "movie", "title": "Dune", "year": 2021, "ids": {"tmdb": "438631"}}
    second = {"type": "movie", "title": "Arrival", "year": 2016, "ids": {"tmdb": "329865"}}

    assert adapter.add("collection", [first])["count"] == 1
    assert adapter.add("collection", [second])["count"] == 1

    snaps = sorted((tmp_path / "snapshots" / "plex-trakt").glob("*-collection.json"))
    assert len(snaps) == 1

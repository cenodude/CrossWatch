from __future__ import annotations

import json
from typing import Any

from cw_platform.playlists import supports_playlists
from providers.sync import _mod_CROSSWATCH as mod
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


def _movie(tmdb: int, title: str = "Movie") -> dict[str, Any]:
    return {"type": "movie", "title": title, "ids": {"tmdb": str(tmdb)}}


def _episode() -> dict[str, Any]:
    return {
        "type": "episode",
        "series_title": "Severance",
        "show_ids": {"tmdb": "95396"},
        "season": 1,
        "episode": 2,
        "ids": {},
    }


def test_crosswatch_playlist_local_crud_and_order(tmp_path: Any) -> None:
    adapter = CROSSWATCHModule(_cfg(tmp_path))
    res = mod.feat_playlists.create(adapter, "Local", items=[_movie(1, "One"), _episode()])

    assert res.provider == "CROSSWATCH"
    assert res.can_add is True
    assert res.can_remove is True
    assert res.can_reorder is True

    snap = mod.feat_playlists.get_snapshot(adapter, res.id)
    assert snap.ordered_keys() == ["tmdb:1", "tmdb:95396#s01e02"]

    add = mod.feat_playlists.add(adapter, res.id, [_movie(2, "Two"), _movie(1, "One")])
    assert add["count"] == 1
    assert add["confirmed_keys"] == ["tmdb:2", "tmdb:1"]

    reorder = mod.feat_playlists.reorder(adapter, res.id, ["tmdb:2", "tmdb:1", "tmdb:95396#s01e02"])
    assert reorder["reordered"] == 3
    assert mod.feat_playlists.get_snapshot(adapter, res.id).ordered_keys() == ["tmdb:2", "tmdb:1", "tmdb:95396#s01e02"]

    renamed = mod.feat_playlists.rename(adapter, res.id, "Renamed")
    assert renamed.name == "Renamed"

    remove = mod.feat_playlists.remove(adapter, res.id, [_movie(1, "One")])
    assert remove["count"] == 1
    assert mod.feat_playlists.get_snapshot(adapter, res.id).ordered_keys() == ["tmdb:2", "tmdb:95396#s01e02"]

    deleted = mod.feat_playlists.delete(adapter, res.id)
    assert deleted["ok"] is True
    payload = json.loads((tmp_path / "playlists.json").read_text("utf-8"))
    assert payload["lists"] == {}


def test_crosswatch_ops_exposes_playlist_contract(tmp_path: Any) -> None:
    assert mod.get_manifest()["features"]["playlists"] is True
    assert mod.CROSSWATCHModule.supported_features()["playlists"] is True
    assert supports_playlists(mod.OPS)

    cfg = _cfg(tmp_path)
    created = mod.OPS.create_playlist(cfg, "Sink")
    resources = mod.OPS.list_playlist_resources(cfg)
    assert [r.id for r in resources] == [created.id]
    assert resources[0].name == "Sink"
    assert mod.OPS.add_playlist_items(cfg, created.id, [_movie(99)])["count"] == 1
    assert mod.OPS.get_playlist_snapshot(cfg, created.id).ordered_keys() == ["tmdb:99"]

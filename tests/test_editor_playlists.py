from __future__ import annotations

from typing import Any

import pytest
from fastapi import HTTPException

from api import editorAPI as api
from cw_platform.playlists import PLAYLIST_KIND_SMART, PlaylistItem, PlaylistResource, PlaylistSnapshot


class FakePlaylistOps:
    def __init__(self, resource: PlaylistResource | None = None):
        self.resource = resource or PlaylistResource(
            provider="PLEX",
            id="pl1",
            name="Movies",
            can_add=True,
            can_remove=True,
            can_reorder=True,
            media_types=("movie", "show"),
        )
        self.items = [
            {"type": "movie", "title": "One", "ids": {"tmdb": "1"}},
            {"type": "movie", "title": "Two", "ids": {"tmdb": "2"}},
        ]
        self.calls: list[tuple[str, Any]] = []

    def list_playlist_resources(self, cfg, *, instance=None):
        return [self.resource]

    def get_playlist_snapshot(self, cfg, playlist_id, *, instance=None):
        return PlaylistSnapshot(
            resource=self.resource,
            items=[PlaylistItem.from_media(item, position=i) for i, item in enumerate(self.items)],
        )

    def create_playlist(self, cfg, name, *, media_type=None, instance=None, dry_run=False):
        return self.resource

    def add_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("add", [dict(x) for x in items]))
        self.items.extend(dict(x) for x in items)
        return {"ok": True, "count": len(items), "unresolved": []}

    def remove_playlist_items(self, cfg, playlist_id, items, *, instance=None, dry_run=False):
        self.calls.append(("remove", [dict(x) for x in items]))
        remove_ids = {str((x.get("ids") or {}).get("tmdb") or "") for x in items}
        self.items = [x for x in self.items if str((x.get("ids") or {}).get("tmdb") or "") not in remove_ids]
        return {"ok": True, "count": len(items), "unresolved": []}

    def reorder_playlist_items(self, cfg, playlist_id, ordered_keys, *, instance=None, dry_run=False):
        self.calls.append(("reorder", list(ordered_keys)))
        return {"ok": True, "reordered": len(ordered_keys)}


def _endpoint() -> dict[str, Any]:
    return {
        "id": "EP-01",
        "name": "Films",
        "provider": "PLEX",
        "provider_label": "Plex",
        "instance": "default",
        "playlist_id": "pl1",
        "playlist_name": "Movies",
    }


@pytest.fixture
def editor_playlist(monkeypatch):
    ops = FakePlaylistOps()
    cfg = {"playlists": {"endpoints": [_endpoint()]}}
    monkeypatch.setattr(api, "load_config", lambda: cfg)
    monkeypatch.setattr(api.playlist_svc, "list_endpoints", lambda _cfg: [_endpoint()])
    monkeypatch.setattr(api.playlists_runner, "get_endpoint", lambda _cfg, eid: _endpoint() if eid == "EP-01" else None)
    monkeypatch.setattr(api, "load_sync_ops", lambda provider: ops if provider == "PLEX" else None)
    monkeypatch.setattr(api, "build_provider_config_view", lambda cfg, provider, instance: {"provider": provider, "instance": instance})
    return ops


def test_editor_lists_playlist_endpoints(editor_playlist) -> None:
    data = api.api_editor_playlist_endpoints()
    assert data["ok"] is True
    assert data["endpoints"][0]["id"] == "EP-01"


def test_editor_loads_playlist_endpoint_snapshot(editor_playlist) -> None:
    data = api.api_editor_get_state(kind="watchlist", source="playlist", endpoint="EP-01")
    assert data["source"] == "playlist"
    assert data["resource"]["can_add"] is True
    assert data["resource"]["media_types"] == ["movie", "show"]
    assert set(data["items"]) == {"tmdb:1", "tmdb:2"}
    assert data["original_keys"] == ["tmdb:1", "tmdb:2"]


def test_editor_saves_playlist_diff_through_provider_ops(editor_playlist) -> None:
    result = api.api_editor_save_state(
        {
            "source": "playlist",
            "endpoint": "EP-01",
            "items": {
                "tmdb:2": {"type": "movie", "title": "Two", "ids": {"tmdb": "2"}},
                "tmdb:3": {"type": "movie", "title": "Three", "ids": {"tmdb": "3"}},
            },
        }
    )
    assert result["planned_additions"] == 1
    assert result["planned_removals"] == 1
    assert result["added"] == 1
    assert result["removed"] == 1
    assert any(call[0] == "add" and call[1][0]["ids"]["tmdb"] == "3" for call in editor_playlist.calls)
    assert any(call[0] == "remove" for call in editor_playlist.calls)


def test_editor_blocks_readonly_playlist_endpoint(monkeypatch, editor_playlist) -> None:
    editor_playlist.resource = PlaylistResource(
        provider="PLEX",
        id="pl1",
        name="Smart",
        kind=PLAYLIST_KIND_SMART,
        can_read=True,
        can_add=False,
        can_remove=False,
        can_reorder=False,
    )
    with pytest.raises(HTTPException) as exc:
        api.api_editor_save_state(
            {
                "source": "playlist",
                "endpoint": "EP-01",
                "items": {"tmdb:1": {"type": "movie", "title": "One", "ids": {"tmdb": "1"}}},
            }
        )
    assert exc.value.status_code == 400


def test_editor_ui_exposes_playlist_source() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "editor.js").read_text(encoding="utf-8")
    assert 'option[value="playlist"]' in js
    assert "/api/editor/playlists/endpoints" in js
    assert "payload.endpoint = state.snapshot" in js
    assert "state.playlistOriginalKeys" in js
    assert 'const SOURCES = ["state", "tracker", "playlist"];' in js
    assert "state.source = normalizeSource(state.source);" in js

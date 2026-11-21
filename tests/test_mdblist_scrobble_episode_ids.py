# tests/test_mdblist_scrobble_episode_ids.py
# CrossWatch - MDBList scrobble and progress writes by episode ids
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from types import SimpleNamespace
from typing import Any

import pytest

from providers.scrobble.mdblist import sink
from providers.sync.mdblist import _progress


KAI_S2E6_ROW = {
    "season": 2,
    "number": 6,
    "title": "The Star Player Appears! Ginyu vs. Goku!",
    "ids": {"tmdb": 1024341, "tvdb": 1280931},
}
KAI_SHOW_ROW = {"title": "Dragon Ball Z Kai", "year": 2009, "ids": {"imdb": "tt1409055", "tmdb": 61709, "tvdb": 88031}}


def event(ids: dict[str, Any], season: int = 1, number: int = 32, media_type: str = "episode"):
    return SimpleNamespace(media_type=media_type, ids=dict(ids), season=season, number=number, title="Kai", year=2009)


def episode_of(body: dict[str, Any]) -> dict[str, Any]:
    return body["show"]["season"]["episode"]


def test_scrobble_body_carries_episode_ids_next_to_numbers():
    ev = event({"tvdb": "1280931", "tmdb": "1024341", "imdb": "tt1544685", "tvdb_show": "88031", "tmdb_show": "61709"})

    body = sink._bodies(ev, 95.0)[0]

    assert episode_of(body) == {"number": 32, "ids": {"tmdb": 1024341, "tvdb": 1280931}}
    assert body["show"]["season"]["number"] == 1
    assert body["show"]["ids"] == {"tmdb": 61709, "tvdb": 88031}


@pytest.mark.parametrize(
    "ids",
    [
        {"tvdb": "88031", "tmdb_show": "61709"},
        {"tvdb": "1280931", "tmdb_show": "61709"},
        {"tmdb": "61709", "tmdb_show": "61709"},
        {"imdb": "tt1544685", "tmdb_show": "61709", "tvdb_show": "88031"},
    ],
    ids=["legacy_show_tvdb", "show_tvdb_unknown", "show_tmdb_only", "imdb_only"],
)
def test_scrobble_body_without_usable_episode_ids_stays_numbers_only(ids):
    body = sink._bodies(event(ids), 50.0)[0]

    assert episode_of(body) == {"number": 32}


def test_scrobble_movie_body_is_unchanged():
    body = sink._bodies(event({"tmdb": "550", "imdb": "tt0137523", "tvdb": "123"}, media_type="movie"), 50.0)[0]

    assert body["movie"] == {"ids": {"tmdb": 550, "imdb": "tt0137523"}}


class Response:
    def __init__(self, status_code: int, payload: Any) -> None:
        self.status_code = status_code
        self._payload = payload
        self.text = json.dumps(payload)

    def json(self) -> Any:
        return self._payload


class IdsWinClient:
    BASE = "https://api.mdblist.com"

    def __init__(self, rows: list[dict[str, Any]] | None = None) -> None:
        self.rows = list(rows or [])
        self.posts: list[tuple[str, dict[str, Any]]] = []

    def get(self, _url: str, **_kwargs: Any) -> Response:
        return Response(200, list(self.rows))

    def post(self, url: str, json: dict[str, Any], **_kwargs: Any) -> Response:
        self.posts.append((url, json))
        if url.endswith("/scrobble/pause"):
            sent = json["show"]["season"]["episode"]
            assert sent.get("ids", {}).get("tvdb") == 1280931
            self.rows.append({"id": 77, "type": "episode", "progress": json["progress"], "paused_at": "2026-11-21T12:00:00Z",
                              "movie": None, "episode": dict(KAI_S2E6_ROW), "show": dict(KAI_SHOW_ROW)})
            return Response(200, {"id": 77, "action": "pause"})
        if url.endswith("/scrobble/clear"):
            self.rows = [row for row in self.rows if row["id"] != json["id"]]
            return Response(200, {"action": "clear", "deleted": True})
        return Response(404, {})


def progress_adapter(rows=None):
    return SimpleNamespace(client=IdsWinClient(rows), cfg=SimpleNamespace(api_key="k"))


def tvdb_ordered_item(**extra: Any) -> dict[str, Any]:
    item = {
        "type": "episode",
        "ids": {"tvdb": 1280931, "imdb": "tt1544685"},
        "show_ids": {"imdb": "tt1409055", "tmdb": 61709, "tvdb": 88031},
        "season": 1,
        "episode": 32,
        "progress_ms": 600_000,
        "duration_ms": 1_500_000,
        "progress_at": "2026-11-21T11:00:00Z",
    }
    item.update(extra)
    return item


def test_progress_pause_body_carries_episode_ids():
    body, reason = _progress._scrobble_body(tvdb_ordered_item())

    assert reason is None
    assert body["show"]["season"] == {"number": 1, "episode": {"number": 32, "ids": {"tvdb": 1280931}}}


def test_progress_write_in_other_numbering_is_confirmed():
    adapter = progress_adapter()
    item = tvdb_ordered_item()

    result = _progress.add(adapter, [item])

    assert result["ok"] is True
    assert result["unresolved"] == []
    assert len(result["confirmed_keys"]) == 1


def test_progress_unchanged_in_other_numbering_is_skipped():
    row = {"id": 77, "type": "episode", "progress": 40.0, "paused_at": "2026-11-21T12:00:00Z",
           "movie": None, "episode": dict(KAI_S2E6_ROW), "show": dict(KAI_SHOW_ROW)}
    adapter = progress_adapter([row])

    result = _progress.add(adapter, [tvdb_ordered_item()])

    assert result["skipped"] == 1
    assert adapter.client.posts == []


def test_progress_remove_finds_row_by_episode_id():
    row = {"id": 77, "type": "episode", "progress": 40.0, "paused_at": "2026-11-21T12:00:00Z",
           "movie": None, "episode": dict(KAI_S2E6_ROW), "show": dict(KAI_SHOW_ROW)}
    adapter = progress_adapter([row])

    result = _progress.remove(adapter, [tvdb_ordered_item()])

    assert result["ok"] is True
    assert adapter.client.posts == [("https://api.mdblist.com/scrobble/clear", {"id": 77})]
    assert adapter.client.rows == []

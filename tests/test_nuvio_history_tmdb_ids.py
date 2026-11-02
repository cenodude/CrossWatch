# CrossWatch test scripts
from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from providers.sync import _mod_NUVIO as mod
from providers.sync.nuvio import _history as nuvio
from providers.sync.scrob import _history as scrob
from providers.sync.stremio import _common as stremio_common

SHAWSHANK = "tt0111161"
WESTWORLD = "tt0475784"
BLEACH = "tt0434665"
WATCHED_AT = 1_767_323_045_000

TMDB_IDS = {
    SHAWSHANK: {"tmdb": "278"},
    WESTWORLD: {"tmdb": "63247", "tvdb": "296762"},
    BLEACH: {"tmdb": "30984", "tvdb": "74796"},
}
SEASONS = {
    ("63247", 1): [{"episode_number": 1, "name": "The Original", "air_date": "2016-10-02"},
                   {"episode_number": 2, "name": "Chestnut", "air_date": "2016-10-09"}],
    ("30984", 2): [{"episode_number": 1, "name": "The Blood Warfare", "air_date": "2022-10-11"}],
}
VIDEOS = {
    WESTWORLD: [{"id": f"{WESTWORLD}:1:1", "season": 1, "episode": 1, "name": "The Original", "released": "2016-10-02T00:00:00.000Z"},
                {"id": f"{WESTWORLD}:1:2", "season": 1, "episode": 2, "name": "Chestnut", "released": "2016-10-09T00:00:00.000Z"}],
    BLEACH: [{"id": f"{BLEACH}:2:1", "season": 2, "episode": 1, "name": "Enter! The World of the Shinigami", "released": "2005-03-02T00:00:00.000Z"}],
}


class FakeTmdb:
    def __init__(self) -> None:
        self.finds: list[str] = []

    def find_imdb(self, imdb: str, entity: str) -> dict[str, str]:
        self.finds.append(imdb)
        return dict(TMDB_IDS.get(imdb) or {})

    def _get(self, url: str, params: Any = None, **_: Any) -> dict[str, Any]:
        parts = url.rsplit("/", 3)
        return {"episodes": SEASONS.get((parts[1], int(parts[3])), [])}


def movie_row(content_id: str = SHAWSHANK, watched_at: int = WATCHED_AT) -> dict[str, Any]:
    return {"content_id": content_id, "content_type": "movie", "title": "A Movie", "watched_at": watched_at, "profile_id": 1}


def episode_row(content_id: str, season: int, episode: int, watched_at: int = WATCHED_AT) -> dict[str, Any]:
    return {"content_id": content_id, "content_type": "series", "title": "A Show", "season": season, "episode": episode,
            "watched_at": watched_at, "profile_id": 1}


@pytest.fixture()
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    state = SimpleNamespace(rows=[], calls=[], tmdb=FakeTmdb(), cinemeta_calls=[], cinemeta_down=False)

    def cinemeta(adapter: Any, imdb: str, *, fresh: bool = False) -> list[dict[str, Any]]:
        state.cinemeta_calls.append((imdb, fresh))
        if fresh and not state.cinemeta_down:
            adapter._stremio_cinemeta_refreshed = {imdb}
        return VIDEOS[imdb]

    def rpc(_adapter: Any, name: str, payload: Any = None) -> dict[str, Any]:
        state.calls.append((name, payload))
        if name == "sync_delete_watched_items":
            for key in payload["p_keys"]:
                state.rows = [row for row in state.rows if row["content_id"] != key["content_id"]]
        return {}

    monkeypatch.setattr(stremio_common, "cinemeta_cache_dir", lambda: tmp_path / "cinemeta")
    monkeypatch.setattr(nuvio, "pull_watched_rows", lambda _adapter: state.rows)
    monkeypatch.setattr(nuvio, "cinemeta_videos", cinemeta)
    monkeypatch.setattr(nuvio, "rpc", rpc)
    monkeypatch.setattr(nuvio, "selected_profile_id", lambda _adapter: 1)
    monkeypatch.setattr(nuvio, "metadata_title_for_content_id", lambda *_a: "A Show")
    state.adapter = SimpleNamespace(config={}, instance_id="default", _stremio_tmdb_provider=state.tmdb)
    return state


def test_imdb_rows_gain_ids_and_keep_their_nuvio_identity(world: SimpleNamespace) -> None:
    world.rows = [movie_row(), episode_row(WESTWORLD, 1, 1)]
    index = nuvio.sync_index(world.adapter)

    movie, episode = index["tmdb:278"], index["tmdb:63247#s01e01"]
    assert movie["ids"] == {"imdb": SHAWSHANK, "tmdb": "278"}
    assert episode["show_ids"] == {"imdb": WESTWORLD, "tmdb": "63247", "tvdb": "296762"}
    assert episode["ids"] == {"imdb": WESTWORLD}
    assert (movie["_nuvio_content_id"], episode["_nuvio_content_id"]) == (SHAWSHANK, WESTWORLD)


def test_imdb_rows_become_writable_to_tmdb_only_destinations(world: SimpleNamespace) -> None:
    world.rows = [movie_row(), episode_row(WESTWORLD, 1, 1)]
    index = nuvio.sync_index(world.adapter)

    assert scrob._payload_for(index["tmdb:278"])["tmdb_id"] == 278
    assert scrob._payload_for(index["tmdb:63247#s01e01"])["series_tmdb_id"] == 63247


def test_each_title_is_looked_up_once(world: SimpleNamespace) -> None:
    world.rows = [episode_row(WESTWORLD, 1, 1), episode_row(WESTWORLD, 1, 2)]
    nuvio.sync_index(world.adapter)
    assert world.tmdb.finds == [WESTWORLD]


def test_episode_that_disagrees_with_tmdb_is_held_back(world: SimpleNamespace) -> None:
    world.rows = [episode_row(BLEACH, 2, 1)]
    index = nuvio.sync_index(world.adapter)

    assert list(index) == [f"imdb:{BLEACH}#s02e01"]
    assert scrob._payload_for(index[f"imdb:{BLEACH}#s02e01"]) is None


def test_tmdb_rows_are_left_alone(world: SimpleNamespace) -> None:
    world.rows = [movie_row("tmdb:278")]
    assert list(nuvio.sync_index(world.adapter)) == ["tmdb:278"]
    assert world.tmdb.finds == []


def test_without_a_tmdb_key_items_stay_imdb_only(world: SimpleNamespace) -> None:
    world.rows = [movie_row()]
    world.adapter = SimpleNamespace(config={}, instance_id="default")
    assert list(nuvio.sync_index(world.adapter)) == [f"imdb:{SHAWSHANK}"]


def test_failed_lookup_keeps_the_read(world: SimpleNamespace) -> None:
    def down(*_a: Any) -> dict[str, str]:
        raise RuntimeError("tmdb down")

    world.tmdb.find_imdb = down
    world.rows = [movie_row(), episode_row(WESTWORLD, 1, 1)]
    assert sorted(nuvio.sync_index(world.adapter)) == [f"imdb:{SHAWSHANK}", f"imdb:{WESTWORLD}#s01e01"]


def test_cinemeta_failure_keeps_the_episode_imdb_only(world: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    def down(*_a: Any, **_k: Any) -> list[dict[str, Any]]:
        raise OSError("cinemeta down")

    monkeypatch.setattr(nuvio, "cinemeta_videos", down)
    world.rows = [movie_row(), episode_row(WESTWORLD, 1, 1)]
    assert sorted(nuvio.sync_index(world.adapter)) == [f"imdb:{WESTWORLD}#s01e01", "tmdb:278"]


def test_raw_index_keeps_content_keys_for_write_verification(world: SimpleNamespace) -> None:
    world.rows = [movie_row()]
    assert list(nuvio.build_index(world.adapter)) == [f"imdb:{SHAWSHANK}"]


def test_episode_missing_from_the_cached_list_refreshes_it_once(world: SimpleNamespace) -> None:
    world.rows = [episode_row(WESTWORLD, 1, 3)]
    for _ in range(3):
        nuvio.sync_index(world.adapter)
    assert world.cinemeta_calls.count((WESTWORLD, True)) == 1


def test_failed_refresh_is_tried_again_on_the_next_sync(world: SimpleNamespace) -> None:
    world.rows = [episode_row(WESTWORLD, 1, 3)]
    world.cinemeta_down = True
    nuvio.sync_index(world.adapter)
    assert "checked_missing" not in stremio_common.read_cinemeta_entry(WESTWORLD)

    world.cinemeta_down = False
    world.adapter = SimpleNamespace(config={}, instance_id="default", _stremio_tmdb_provider=world.tmdb)
    nuvio.sync_index(world.adapter)
    assert world.cinemeta_calls.count((WESTWORLD, True)) == 2
    assert stremio_common.read_cinemeta_entry(WESTWORLD)["checked_missing"] == f"{WESTWORLD}:1:3"


def test_remove_finds_the_imdb_row_from_a_tmdb_keyed_item(world: SimpleNamespace) -> None:
    world.rows = [movie_row()]
    item = nuvio.sync_index(world.adapter)["tmdb:278"]
    result = nuvio.remove(world.adapter, [{"type": "movie", "ids": {"tmdb": "278", "imdb": SHAWSHANK}}])

    assert result["ok"] and result["confirmed_keys"] == ["tmdb:278"]
    assert world.calls[0][1]["p_keys"] == [{"content_id": item["_nuvio_content_id"]}]
    assert world.rows == []


def test_remove_episode_finds_the_imdb_row(world: SimpleNamespace) -> None:
    world.rows = [episode_row(WESTWORLD, 1, 1)]
    item = {"type": "episode", "season": 1, "episode": 1, "ids": {}, "show_ids": {"tmdb": "63247", "imdb": WESTWORLD}}
    nuvio.remove(world.adapter, [item])

    assert world.calls[0][1]["p_keys"] == [{"content_id": WESTWORLD, "season": 1, "episode": 1}]


def test_add_does_not_duplicate_an_existing_imdb_row(world: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(nuvio, "resolve_content_id_for_item", lambda _adapter, _item: "tmdb:278")
    world.rows = [movie_row()]
    item = {"type": "movie", "ids": {"tmdb": "278", "imdb": SHAWSHANK}, "watched_at": "2020-01-01T00:00:00Z"}

    result = nuvio.add(world.adapter, [item])

    assert result["skipped_keys"] == ["tmdb:278"]
    assert result["confirmed_destinations"]["tmdb:278"]["key"] == f"imdb:{SHAWSHANK}"
    assert not any(name == "sync_push_watched_items" for name, _ in world.calls)


def test_module_reads_history_through_the_sync_index(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(mod.feat_history, "sync_index", lambda _adapter: {"synced": {}})
    assert mod.NUVIOModule.build_index(SimpleNamespace(), "history") == {"synced": {}}

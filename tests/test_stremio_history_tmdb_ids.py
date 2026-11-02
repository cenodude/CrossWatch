# CrossWatch test scripts
from __future__ import annotations

import json
import re
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import requests

from cw_platform.orchestrator import _unresolved
from cw_platform.orchestrator._interactive import InteractivePlan
from cw_platform.orchestrator.facade import Orchestrator
from cw_platform.reason_labels import friendly_reason, reason_message
from providers.metadata._meta_TMDB import TmdbProvider
from providers.sync._mod_SCROB import OPS as SCROB_OPS
from providers.sync._mod_STREMIO import OPS as STREMIO_OPS
from providers.sync.scrob import _common as scrob_common, _history as scrob
from providers.sync.stremio import _common as stremio_common, _history as stremio

SHAWSHANK = "tt0111161"
WELLNESS = "tt4731136"
WESTWORLD = "tt0475784"
BLEACH = "tt0434665"
BEBOP = "tt0213338"
UNKNOWN = "tt9999999"

TMDB_IDS = {
    SHAWSHANK: {"tmdb": "278"},
    WELLNESS: {"tmdb": "340837"},
    WESTWORLD: {"tmdb": "63247", "tvdb": "296762"},
    BLEACH: {"tmdb": "30984", "tvdb": "74796"},
    BEBOP: {"tmdb": "30991", "tvdb": "76885"},
}
TMDB_SEASONS = {
    ("63247", 1): [
        {"episode_number": 1, "name": "The Original", "air_date": "2016-10-02"},
        {"episode_number": 2, "name": "Chestnut", "air_date": "2016-10-09"},
        {"episode_number": 3, "name": "The Stray", "air_date": "2016-10-16"},
    ],
    ("30984", 2): [{"episode_number": 1, "name": "The Blood Warfare", "air_date": "2022-10-11"}],
    ("30991", 1): [{"episode_number": 2, "name": "Honky Tonk Women", "air_date": "1998-04-10"}],
}
VIDEOS = {
    WESTWORLD: [
        {"id": f"{WESTWORLD}:1:1", "season": 1, "episode": 1, "name": "The Original", "released": "2016-10-02T00:00:00.000Z"},
        {"id": f"{WESTWORLD}:1:2", "season": 1, "episode": 2, "name": "Chestnut", "released": "2016-10-09T00:00:00.000Z"},
        {"id": f"{WESTWORLD}:1:3", "season": 1, "episode": 3, "name": "The Stray", "released": "2016-10-16T00:00:00.000Z"},
    ],
    BLEACH: [{"id": f"{BLEACH}:2:1", "season": 2, "episode": 1, "name": "Enter! The World of the Shinigami", "released": "2005-03-02T00:00:00.000Z"}],
    BEBOP: [{"id": f"{BEBOP}:1:2", "season": 1, "episode": 2, "name": "Stray Dog Strut", "released": "1998-04-04T00:00:00.000Z"}],
}


class FakeTmdb:
    def __init__(self) -> None:
        self.down = False
        self.finds: list[str] = []

    def find_imdb(self, imdb: str, entity: str) -> dict[str, str]:
        self.finds.append(imdb)
        if self.down:
            raise RuntimeError("tmdb unavailable")
        return dict(TMDB_IDS.get(imdb) or {})

    def _get(self, url: str, params: Any = None, **_: Any) -> dict[str, Any]:
        match = re.search(r"/tv/(\d+)/season/(\d+)$", url)
        return {"episodes": TMDB_SEASONS.get((match.group(1), int(match.group(2))), []) if match else []}


def movie_record(imdb: str, name: str, watched_at: str = "2026-01-02T03:04:05Z") -> dict[str, Any]:
    return {"_id": imdb, "type": "movie", "name": name, "state": {"timesWatched": 1, "lastWatched": watched_at}}


def series_record(imdb: str, name: str, watched: list[bool], watched_at: str = "2026-02-03T04:05:06Z") -> dict[str, Any]:
    ids = [str(video["id"]) for video in VIDEOS[imdb]]
    return {"_id": imdb, "type": "series", "name": name,
            "state": {"watched": stremio.serialize_watched_bits(watched, ids), "lastWatched": watched_at}}


def default_library(watched_at: str = "2026-02-03T04:05:06Z") -> list[dict[str, Any]]:
    return [movie_record(SHAWSHANK, "The Shawshank Redemption"), movie_record(WELLNESS, "A Cure for Wellness"),
            series_record(WESTWORLD, "Westworld", [True, True, False], watched_at)]


def adapter(tmdb: Any) -> SimpleNamespace:
    return SimpleNamespace(instance_id="default", config={}, _stremio_history_baseline={}, _stremio_tmdb_provider=tmdb)


class Library:
    def __init__(self, records: list[dict[str, Any]]) -> None:
        self.records = records


@pytest.fixture()
def cinemeta_disk(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    root = tmp_path / "cinemeta"
    monkeypatch.setattr(stremio_common, "cinemeta_cache_dir", lambda: root)
    return root


@pytest.fixture()
def library(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> Library:
    state = Library(default_library())
    monkeypatch.setattr(stremio, "library_records", lambda _adapter, incremental=False: state.records)
    monkeypatch.setattr(stremio, "cinemeta_videos", lambda _adapter, imdb, **_: VIDEOS[imdb])
    return state


class Response:
    def __init__(self, status: int, body: Any) -> None:
        self.status_code = status
        self.body = body
        self.text = json.dumps(body)

    def json(self) -> Any:
        return self.body


class ScrobServer:
    def __init__(self) -> None:
        self.rows: list[dict[str, Any]] = []
        self.posts: list[dict[str, Any]] = []
        self.next_id = 1000

    @staticmethod
    def media(payload: dict[str, Any]) -> dict[str, Any]:
        if payload["media_type"] == "movie":
            return {"id": payload["tmdb_id"], "type": "movie", "tmdb_id": payload["tmdb_id"], "title": "movie", "release_date": "2000-01-01"}
        return {"id": payload["series_tmdb_id"] * 1000 + payload["episode_number"], "type": "episode", "tmdb_id": None, "title": "episode",
                "season_number": payload["season_number"], "episode_number": payload["episode_number"], "show_title": "show",
                "show_tmdb_id": payload["series_tmdb_id"], "show_tvdb_id": payload.get("series_tvdb_id")}

    def store(self, payload: dict[str, Any]) -> None:
        self.next_id += 1
        self.rows.append({"id": self.next_id, "completed": True, "watched_at": payload["watched_at"].replace(".000Z", ""), "media": self.media(payload)})

    def __call__(self, _adapter: Any, method: str, path: str, **kwargs: Any) -> Response:
        if method == "GET" and path == scrob_common.PATH_HISTORY:
            page, size = kwargs["params"]["page"], kwargs["params"]["page_size"]
            pages = max(1, (len(self.rows) + size - 1) // size)
            return Response(200, {"results": self.rows[(page - 1) * size:page * size], "total_results": len(self.rows), "total_pages": pages})
        assert (method, path) == ("POST", scrob_common.PATH_HISTORY)
        payload = kwargs["json"]
        self.posts.append(payload)
        when = datetime.fromisoformat(payload["watched_at"].replace(".000Z", ""))
        for row in self.rows:
            gap = abs((datetime.fromisoformat(row["watched_at"]) - when).total_seconds())
            if row["media"]["id"] == self.media(payload)["id"] and gap <= 300:
                return Response(409, {"detail": {"error": "duplicate_watch", "existing_event_id": row["id"]}})
        self.store(payload)
        return Response(200, {"id": self.next_id})


class StremioOps:
    def __init__(self, tmdb: Any) -> None:
        self.adapter = adapter(tmdb)
        self.added: list[dict[str, Any]] = []

    def name(self) -> str:
        return "STREMIO"

    def label(self) -> str:
        return "Stremio"

    def features(self) -> dict[str, bool]:
        return {"history": True}

    def capabilities(self) -> Any:
        return STREMIO_OPS.capabilities()

    def is_configured(self, _cfg: Any) -> bool:
        return True

    def health(self, _cfg: Any, **_: Any) -> dict[str, Any]:
        return {"ok": True, "status": "ok", "features": {"history": True}, "api": {}}

    def build_index(self, _cfg: Any, *, feature: str, **_: Any) -> dict[str, dict[str, Any]]:
        return stremio.build_index(self.adapter)

    def add(self, _cfg: Any, items: Any, *, feature: str, dry_run: bool = False, **_: Any) -> dict[str, Any]:
        batch = [dict(item) for item in items]
        self.added.extend(batch)
        return {"ok": True, "count": len(batch), "confirmed_keys": [stremio_common.canonical_item_key(item) for item in batch]}

    def remove(self, _cfg: Any, items: Any, *, feature: str, dry_run: bool = False, **_: Any) -> dict[str, Any]:
        raise AssertionError("nothing may be removed from Stremio")


class ScrobOps(StremioOps):
    def __init__(self) -> None:
        self.adapter = SimpleNamespace(instance_id="default", config={})

    def name(self) -> str:
        return "SCROB"

    def label(self) -> str:
        return "Scrob"

    def capabilities(self) -> Any:
        return SCROB_OPS.capabilities()

    def build_index(self, _cfg: Any, *, feature: str, **_: Any) -> dict[str, dict[str, Any]]:
        return scrob.build_index(self.adapter)

    def add(self, _cfg: Any, items: Any, *, feature: str, dry_run: bool = False, **_: Any) -> dict[str, Any]:
        return scrob.add(self.adapter, items, dry_run=dry_run)

    def remove(self, _cfg: Any, items: Any, *, feature: str, dry_run: bool = False, **_: Any) -> dict[str, Any]:
        raise AssertionError("nothing may be removed from Scrob")


def pair_config(mode: str = "one-way") -> dict[str, Any]:
    return {
        "runtime": {"debug": False, "snapshot_ttl_sec": 0, "apply_chunk_size": 0, "apply_chunk_pause_ms": 0},
        "blackbox": {"enabled": True, "block_adds": True, "block_removes": True, "promote_after": 3, "pair_scoped": True},
        "sync": {"dry_run": False, "enable_add": True, "enable_remove": False, "include_observed_deletes": False, "allow_mass_delete": False},
        "pairs": [{"id": "p1", "enabled": True, "source": "STREMIO", "target": "SCROB", "mode": mode, "feature": "history",
                   "features": {"history": {"enable": True, "add": True, "remove": False}}}],
    }


@pytest.fixture()
def world(config_base: Path, library: Library, monkeypatch: pytest.MonkeyPatch) -> SimpleNamespace:
    for name, module in list(sys.modules.items()):
        if name.startswith("cw_platform.orchestrator") and hasattr(module, "STATE_DIR"):
            monkeypatch.setattr(module, "STATE_DIR", config_base / ".cw_state", raising=False)
    monkeypatch.setattr("cw_platform.orchestrator._phantoms._DIR", str(config_base / ".cw_state"))
    server = ScrobServer()
    tmdb = FakeTmdb()
    source = StremioOps(tmdb)
    target = ScrobOps()
    monkeypatch.setattr(scrob_common, "scrob_request", server)
    monkeypatch.setattr(scrob, "scrob_request", server)
    monkeypatch.setattr("cw_platform.orchestrator._snapshots.provider_configured", lambda _cfg, _name: True)
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"STREMIO": source, "SCROB": target})

    def sync(mode: str = "one-way") -> int:
        before = len(server.posts)
        Orchestrator(pair_config(mode)).run()
        return len(server.posts) - before

    def pending() -> list[str]:
        files = (config_base / ".cw_state").glob("scrob_history.unresolved.pending.*.json")
        return sorted(key for path in files for key in json.loads(path.read_text("utf-8")).get("keys") or [])

    def blackbox() -> list[str]:
        files = (config_base / ".cw_state").glob("scrob_history.*.blackbox.json")
        return sorted(key for path in files for key in json.loads(path.read_text("utf-8")))

    return SimpleNamespace(sync=sync, server=server, tmdb=tmdb, source=source, library=library, pending=pending, blackbox=blackbox)


def test_find_imdb_reads_only_the_expected_type(monkeypatch: pytest.MonkeyPatch) -> None:
    provider = TmdbProvider(lambda: {"tmdb": {"api_key": "k"}}, lambda _cfg: None)
    calls: list[str] = []

    def fake_get(url: str, params: Any = None, **_: Any) -> dict[str, Any]:
        calls.append(url)
        if url.endswith("/external_ids"):
            return {"tvdb_id": 81189}
        return {"movie_results": [{"id": 278}], "tv_results": []} if "tt0111161" in url else {"movie_results": [], "tv_results": [{"id": 1396}]}

    monkeypatch.setattr(provider, "_get", fake_get)

    assert provider.find_imdb("tt0111161", "movie") == {"tmdb": "278"}
    assert len(calls) == 1
    assert provider.find_imdb("tt0903747", "tv") == {"tmdb": "1396", "tvdb": "81189"}
    assert provider.find_imdb("tt0111161", "tv") == {}
    assert provider.find_imdb("tt0903747", "movie") == {}


class TmdbServer:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.matches = {"tt1": [{"id": 278}]}

    def get(self, url: str, params: Any = None, **_: Any) -> SimpleNamespace:
        self.calls.append(url)
        imdb = url.rsplit("/", 1)[-1]
        return SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None,
                               json=lambda: {"movie_results": self.matches.get(imdb, []), "tv_results": []})


def disk_provider(root: Path) -> TmdbProvider:
    provider = TmdbProvider(lambda: {"tmdb": {"api_key": "k"}}, lambda _cfg: None)
    provider._disk_dir = root
    return provider


def test_found_ids_never_expire_until_the_cache_is_cleared(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = TmdbServer()
    monkeypatch.setattr("providers.metadata._meta_TMDB.requests.get", server.get)

    assert disk_provider(tmp_path).find_imdb("tt1", "movie") == {"tmdb": "278"}
    for path in tmp_path.iterdir():
        path.write_text(json.dumps({**json.loads(path.read_text("utf-8")), "ts": 1}), encoding="utf-8")
    assert disk_provider(tmp_path).find_imdb("tt1", "movie") == {"tmdb": "278"}
    assert len(server.calls) == 1

    for path in tmp_path.iterdir():
        path.unlink()
    assert disk_provider(tmp_path).find_imdb("tt1", "movie") == {"tmdb": "278"}
    assert len(server.calls) == 2


def test_a_title_without_a_match_is_retried_on_the_next_sync(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = TmdbServer()
    monkeypatch.setattr("providers.metadata._meta_TMDB.requests.get", server.get)

    assert disk_provider(tmp_path).find_imdb("tt2", "movie") == {}
    server.matches["tt2"] = [{"id": 9}]
    assert disk_provider(tmp_path).find_imdb("tt2", "movie") == {"tmdb": "9"}


class StaleSeasonTmdb(FakeTmdb):
    def __init__(self) -> None:
        super().__init__()
        self.fresh_calls = 0

    def _get(self, url: str, params: Any = None, **options: Any) -> dict[str, Any]:
        if options.get("fresh"):
            self.fresh_calls += 1
            return super()._get(url, params)
        return {"episodes": TMDB_SEASONS[("63247", 1)][:2]}


def test_new_episode_of_an_airing_show_is_found_with_fresh_season_data(library: Library) -> None:
    tmdb = StaleSeasonTmdb()
    library.records = [series_record(WESTWORLD, "Westworld", [True, True, False])]
    index = stremio.build_index(adapter(tmdb))
    assert (tmdb.fresh_calls, sorted(index)) == (0, ["tmdb:63247#s01e01", "tmdb:63247#s01e02"])

    library.records = [series_record(WESTWORLD, "Westworld", [True, True, True])]
    index = stremio.build_index(adapter(tmdb))
    assert tmdb.fresh_calls == 1
    assert "tmdb:63247#s01e03" in index


def test_older_held_episode_only_refreshes_its_season_once(library: Library) -> None:
    tmdb = StaleSeasonTmdb()
    library.records = [series_record(BEBOP, "Cowboy Bebop", [True])]
    for _ in range(3):
        assert list(stremio.build_index(adapter(tmdb))) == [f"imdb:{BEBOP}#s01e02"]
    assert tmdb.fresh_calls == 1


def test_recent_held_episode_keeps_checking_tmdb(library: Library, monkeypatch: pytest.MonkeyPatch) -> None:
    tmdb = StaleSeasonTmdb()
    monkeypatch.setattr(stremio, "_recently_released", lambda _video: True)
    library.records = [series_record(BEBOP, "Cowboy Bebop", [True])]
    for _ in range(3):
        stremio.build_index(adapter(tmdb))
    assert tmdb.fresh_calls == 3


def test_recently_released_covers_two_weeks() -> None:
    day = 24 * 60 * 60 * 1000
    assert stremio._recently_released({"released": stremio_common.now_ms() - 13 * day}) is True
    assert stremio._recently_released({"released": stremio_common.now_ms() - 15 * day}) is False
    assert stremio._recently_released({}) is False


class SeasonFailure(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(status)
        self.response = SimpleNamespace(status_code=status)


class FailingSeasonTmdb(FakeTmdb):
    def __init__(self, status: int) -> None:
        super().__init__()
        self.status = status
        self.fresh_calls = 0

    def _get(self, url: str, params: Any = None, **options: Any) -> dict[str, Any]:
        self.fresh_calls += 1 if options.get("fresh") else 0
        raise SeasonFailure(self.status)


@pytest.mark.parametrize(("status", "fresh_calls"), [(503, 3), (404, 1)])
def test_outage_is_not_remembered_as_a_check_but_a_missing_season_is(library: Library, status: int, fresh_calls: int) -> None:
    tmdb = FailingSeasonTmdb(status)
    library.records = [series_record(BEBOP, "Cowboy Bebop", [True])]
    for _ in range(3):
        stremio.build_index(adapter(tmdb))
    assert tmdb.fresh_calls == fresh_calls


class CinemetaServer:
    def __init__(self, videos: list[dict[str, Any]]) -> None:
        self.videos = videos
        self.calls = 0
        self.down = False

    def get(self, url: str, **_: Any) -> SimpleNamespace:
        self.calls += 1
        if self.down:
            raise OSError("cinemeta down")
        return SimpleNamespace(raise_for_status=lambda: None, json=lambda: {"meta": {"videos": self.videos}})


def fresh_adapter() -> SimpleNamespace:
    return SimpleNamespace(instance_id="default", config={})


def bitfield(video_ids: list[str]) -> str:
    return stremio.serialize_watched_bits([True] * len(video_ids), video_ids)


def videos_for(numbers: range) -> list[dict[str, Any]]:
    return [{"id": f"{WESTWORLD}:1:{n}", "season": 1, "episode": n, "name": f"E{n}", "overview": "x" * 50} for n in numbers]


def test_episode_list_survives_a_restart_and_stays_small(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(videos_for(range(1, 3)))
    monkeypatch.setattr(stremio.requests, "get", server.get)

    for _ in range(3):
        rows = stremio.cinemeta_videos(fresh_adapter(), WESTWORLD)

    assert server.calls == 1
    assert rows[0] == {"id": f"{WESTWORLD}:1:1", "season": 1, "episode": 1, "name": "E1"}


def test_a_stale_list_refreshes_when_stremio_is_ahead_of_it(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(videos_for(range(1, 3)))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    stremio.cinemeta_videos(fresh_adapter(), WESTWORLD)
    server.videos = videos_for(range(1, 4))
    watched = bitfield([str(video["id"]) for video in server.videos])
    record = {"_id": WESTWORLD, "type": "series"}

    orders, _ = stremio.video_orders_for_series_record(fresh_adapter(), record, watched)
    assert len(orders[0]) == 3
    assert server.calls == 2

    stremio.video_orders_for_series_record(fresh_adapter(), record, watched)
    assert server.calls == 2


def test_a_list_that_can_never_match_is_only_checked_once_per_watched_state(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(videos_for(range(1, 3)))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    record = {"_id": WESTWORLD, "type": "series"}
    first = bitfield([f"{WESTWORLD}:1:{n}" for n in range(1, 8)])
    second = bitfield([f"{WESTWORLD}:1:{n}" for n in range(1, 9)])

    for _ in range(3):
        stremio.video_orders_for_series_record(fresh_adapter(), record, first)
    assert server.calls == 1

    stremio.video_orders_for_series_record(fresh_adapter(), record, second)
    assert server.calls == 2


def test_failed_refresh_keeps_the_cached_list(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(videos_for(range(1, 3)))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    stremio.cinemeta_videos(fresh_adapter(), WESTWORLD)
    server.down = True
    instance = fresh_adapter()

    stremio.cinemeta_videos(instance, WESTWORLD)
    assert len(stremio.cinemeta_videos(instance, WESTWORLD, fresh=True)) == 2


def test_writing_a_new_episode_refreshes_the_list_once(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(videos_for(range(1, 3)))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    stremio.cinemeta_videos(fresh_adapter(), WESTWORLD)
    server.videos = videos_for(range(1, 4))
    instance = fresh_adapter()
    record: dict[str, Any] = {"_id": WESTWORLD, "type": "series", "state": {}}

    assert stremio._apply_episode(instance, record, {"season": 1, "episode": 3}, True) is None
    assert stremio.watched_anchor(record["state"]["watched"]) == (f"{WESTWORLD}:1:3", 3)
    assert server.calls == 2

    assert stremio._apply_episode(instance, record, {"season": 1, "episode": 9}, True) == "stremio_episode_unresolved"
    assert stremio._apply_episode(instance, record, {"season": 1, "episode": 10}, True) == "stremio_episode_unresolved"
    assert server.calls == 2


def test_movies_and_episodes_gain_ids_and_keep_their_stremio_identity(library: Library) -> None:
    index = stremio.build_index(adapter(FakeTmdb()))

    movie = index["tmdb:278"]
    episode = index["tmdb:63247#s01e01"]
    assert movie["ids"] == {"imdb": SHAWSHANK, "tmdb": "278"}
    assert episode["show_ids"] == {"imdb": WESTWORLD, "tmdb": "63247", "tvdb": "296762"}
    assert episode["ids"] == {"imdb": WESTWORLD}
    assert stremio_common.stremio_write_id_for_item(movie) == SHAWSHANK
    assert stremio_common.stremio_write_id_for_item(episode) == WESTWORLD
    assert scrob._payload_for(movie)["tmdb_id"] == 278
    payload = scrob._payload_for(episode)
    assert (payload["series_tmdb_id"], payload["series_tvdb_id"], payload["tmdb_id"]) == (63247, 296762, 0)


def test_each_title_is_looked_up_once(library: Library) -> None:
    tmdb = FakeTmdb()
    stremio.build_index(adapter(tmdb))
    assert sorted(tmdb.finds) == sorted([SHAWSHANK, WELLNESS, WESTWORLD])


@pytest.mark.parametrize(("imdb", "name", "key"), [(BLEACH, "Bleach", f"imdb:{BLEACH}#s02e01"), (BEBOP, "Cowboy Bebop", f"imdb:{BEBOP}#s01e02")])
def test_episode_that_disagrees_with_tmdb_is_held_back(library: Library, imdb: str, name: str, key: str) -> None:
    library.records = [series_record(imdb, name, [True])]
    index = stremio.build_index(adapter(FakeTmdb()))
    assert list(index) == [key]
    assert index[key]["show_ids"] == {"imdb": imdb}
    assert scrob._payload_for(index[key]) is None


@pytest.mark.parametrize(
    ("video", "row", "expected"),
    [
        ({"name": "Pilot", "released": "2020-01-01T00:00:00.000Z"}, {"title": "Pilot!", "released": "2021-01-01"}, True),
        ({"name": "Part One", "released": "2020-01-01T00:00:00.000Z"}, {"title": "Different", "released": "2020-01-02"}, True),
        ({"name": "Part One", "released": "2020-01-01T00:00:00.000Z"}, {"title": "Different", "released": "2020-01-08"}, False),
        ({"name": "Episode 5", "released": "2020-01-01T00:00:00.000Z"}, {"title": "Episode 5", "released": "2022-01-01"}, False),
        ({"name": "Part 1", "released": "2020-01-01T00:00:00.000Z"}, {"title": "Part 10", "released": "2022-01-01"}, False),
        ({"name": "Episode 1: Arrival", "released": "2020-01-01T00:00:00.000Z"}, {"title": "Episode 1", "released": "2022-01-01"}, False),
        ({"name": "Part One"}, {"title": "Different"}, False),
    ],
)
def test_episode_needs_a_real_title_or_a_close_date(video: dict[str, Any], row: dict[str, Any], expected: bool) -> None:
    assert stremio_common.episode_agrees(video, row, [row]) is expected


def test_date_only_match_must_point_at_a_single_episode() -> None:
    shifted = {"title": "Tuesday", "released": "2020-01-07"}
    daily = [{"title": "Monday", "released": "2020-01-06"}, shifted, {"title": "Wednesday", "released": "2020-01-08"}]
    weekly = [{"title": "Earlier", "released": "2019-12-30"}, shifted, {"title": "Later", "released": "2020-01-14"}]
    video = {"name": "Monday", "released": "2020-01-06T00:00:00.000Z"}

    assert stremio_common.episode_agrees(video, shifted, daily) is False
    assert stremio_common.episode_agrees({**video, "released": "2020-01-07T00:00:00.000Z"}, shifted, weekly) is True


def test_binge_released_episodes_still_match_by_title() -> None:
    rows = [{"title": f"Chapter {name}", "released": "2020-01-01"} for name in ("One", "Two", "Three")]
    assert stremio_common.episode_agrees({"name": "Chapter Two", "released": "2020-01-01T00:00:00.000Z"}, rows[1], rows) is True
    assert stremio_common.episode_agrees({"name": "Chapter Three", "released": "2020-01-01T00:00:00.000Z"}, rows[1], rows) is False


@pytest.mark.parametrize("title", ["Episode 3", "Chapter 3", "001", "○△□"])
def test_generic_titles_match_when_the_date_agrees_too(title: str) -> None:
    rows = [{"title": name, "released": "2020-01-01"} for name in ("Episode 1", "Episode 2", title)]
    video = {"name": title, "released": "2020-01-01T00:00:00.000Z"}
    assert stremio_common.episode_agrees(video, rows[2], rows) is True
    assert stremio_common.episode_agrees({**video, "released": "2022-01-01T00:00:00.000Z"}, rows[2], rows) is False
    assert stremio_common.episode_agrees(video, rows[1], rows) is False


def test_generic_title_needs_the_same_day_when_the_date_is_ambiguous() -> None:
    rows = [{"title": f"Episode {number}", "released": f"2020-01-0{number + 2}"} for number in (3, 4, 5)]
    video = {"name": "Episode 4", "released": "2020-01-05T00:00:00.000Z"}
    assert stremio_common.episode_agrees(video, rows[1], rows) is False
    assert stremio_common.episode_agrees({**video, "released": "2020-01-06T00:00:00.000Z"}, rows[1], rows) is True


def test_season_aliases_use_show_identity() -> None:
    season = {"type": "season", "season": 1, "ids": {"tmdb": "999"}, "show_ids": {"tmdb": "10", "imdb": "tt1"}}
    assert _unresolved._alias_keys(season) == {"imdb:tt1#season:1"}
    assert _unresolved._alias_keys({"type": "season", "season": 1, "show_ids": {"tvdb": "7", "imdb": "tt1"}}) == {"tvdb:7#season:1"}


def test_removal_retry_survives_alias_cleanup_behind_a_pending_hint(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(_unresolved, "STATE_DIR", config_base / ".cw_state")
    blocking = _unresolved._blocking_path("SCROB", "history")
    blocking.parent.mkdir(parents=True, exist_ok=True)
    blocking.write_text(json.dumps({f"imdb:{SHAWSHANK}": {"reason": "apply:remove:unconfirmed", "action": "remove"}}), "utf-8")
    _unresolved.record_unresolved("SCROB", "history", [{"type": "movie", "ids": {"imdb": SHAWSHANK}}], hint="apply:add:failed")

    _unresolved.clear_superseded_keys("SCROB", "history", [{"type": "movie", "ids": {"tmdb": "278", "imdb": SHAWSHANK}}])

    assert f"imdb:{SHAWSHANK}" in _unresolved.load_unresolved_map("SCROB", "history", cross_features=False)


def test_without_a_tmdb_key_items_stay_imdb_only(library: Library) -> None:
    index = stremio.build_index(SimpleNamespace(instance_id="default", config={}, _stremio_history_baseline={}))
    assert sorted(index) == sorted([f"imdb:{SHAWSHANK}", f"imdb:{WELLNESS}", f"imdb:{WESTWORLD}#s01e01", f"imdb:{WESTWORLD}#s01e02"])


def test_failed_lookup_keeps_the_read_and_stops_further_lookups(library: Library) -> None:
    tmdb = FakeTmdb()
    tmdb.down = True
    index = stremio.build_index(adapter(tmdb))
    assert len(index) == 4
    assert len(tmdb.finds) == 1
    assert all(key.startswith("imdb:") for key in index)


def test_unknown_title_stays_imdb_only(library: Library) -> None:
    library.records = [movie_record(UNKNOWN, "Unknown")]
    assert list(stremio.build_index(adapter(FakeTmdb()))) == [f"imdb:{UNKNOWN}"]


def test_scrob_counts_a_duplicate_watch_as_skipped(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [Response(409, {"detail": {"error": "duplicate_watch"}}), Response(409, {"detail": "Conflict"})]
    monkeypatch.setattr(scrob, "scrob_request", lambda *_a, **_k: responses.pop(0))
    item = {"type": "movie", "ids": {"tmdb": "278"}, "watched_at": "2026-01-02T03:04:05Z"}

    skipped = scrob.add(None, [item])
    failed = scrob.add(None, [item])

    assert (skipped["ok"], skipped["count"], len(skipped["skipped_keys"]), skipped["unresolved"]) == (True, 0, 1, [])
    assert (failed["ok"], failed["skipped_keys"], failed["unresolved"][0]["status"]) == (False, [], "http:409")


def test_scrob_failed_rows_carry_the_item_and_reason() -> None:
    result = scrob.add(None, [{"type": "movie", "ids": {"imdb": SHAWSHANK}, "title": "The Shawshank Redemption"}])
    row = result["unresolved"][0]
    assert (row["status"], row["reason"], row["item"]["ids"]) == ("missing_supported_id", "missing_supported_id", {"imdb": SHAWSHANK})


def test_reason_is_readable_for_scrob_only() -> None:
    assert "TMDB ID" in reason_message("missing_supported_id", provider="SCROB", feature="history")
    assert reason_message("missing_supported_id", provider="TRAKT", feature="history") == ""
    assert friendly_reason("missing_supported_id") == "Missing supported ID"


def test_scrob_confirms_plain_writes_under_the_plain_key(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(scrob, "scrob_request", lambda *_a, **_k: Response(200, {"id": 1}))
    plain = {"type": "movie", "ids": {"tmdb": "278"}, "watched_at": "2026-01-02T03:04:05Z"}
    episode = {"type": "episode", "ids": {}, "show_ids": {"tmdb": "63247"}, "season": 1, "episode": 2, "watched_at": "2026-01-02T03:04:05Z"}
    event = {**plain, "_cw_event_key": "tmdb:278@1767323045"}

    assert scrob.add(None, [plain, episode, event])["confirmed_keys"] == ["tmdb:278", "tmdb:63247#s01e02", "tmdb:278@1767323045"]


def test_second_and_third_sync_write_nothing(world: SimpleNamespace) -> None:
    assert [world.sync() for _ in range(3)] == [4, 0, 0]
    assert len(world.server.rows) == 4
    assert world.pending() == []


def test_old_failed_retries_clear_once_the_item_is_written(world: SimpleNamespace) -> None:
    world.tmdb.down = True
    assert world.sync() == 0
    assert len(world.pending()) == 4
    world.tmdb.down = False
    assert world.sync() == 4
    assert world.pending() == []


def test_held_episode_follows_the_blackbox_rule_and_is_never_written(world: SimpleNamespace) -> None:
    world.library.records = [series_record(BLEACH, "Bleach", [True])]
    assert [world.sync() for _ in range(2)] == [0, 0]
    assert (world.pending(), world.blackbox()) == ([f"imdb:{BLEACH}#s02e01"], [])
    assert [world.sync() for _ in range(3)] == [0, 0, 0]
    assert (world.pending(), world.blackbox()) == ([], [f"imdb:{BLEACH}#s02e01"])
    assert world.server.posts == []


def test_successful_writes_leave_existing_blackbox_entries_alone(world: SimpleNamespace) -> None:
    world.tmdb.down = True
    [world.sync() for _ in range(3)]
    entries = world.blackbox()
    world.tmdb.down = False
    assert world.sync() == 2
    assert world.blackbox() == entries


def test_a_real_write_failure_still_reaches_the_blackbox(world: SimpleNamespace, monkeypatch: pytest.MonkeyPatch) -> None:
    world.library.records = [movie_record(SHAWSHANK, "The Shawshank Redemption")]
    monkeypatch.setattr(scrob, "scrob_request", lambda _adapter, method, path, **kwargs: (
        world.server(_adapter, method, path, **kwargs) if method == "GET" else Response(500, {"detail": "boom"})))
    [world.sync() for _ in range(3)]
    assert world.blackbox() == ["tmdb:278"]


def test_dry_run_keeps_old_failed_retries(world: SimpleNamespace) -> None:
    world.tmdb.down = True
    world.sync()
    world.tmdb.down = False
    Orchestrator(pair_config()).run(dry_run=True, write_state_json=False)
    assert len(world.pending()) == 4
    assert world.server.posts == []


def test_moved_show_date_does_not_write_again(world: SimpleNamespace) -> None:
    assert world.sync() == 4
    world.library.records = default_library("2026-03-04T05:06:07Z")
    assert world.sync() == 0
    assert len(world.server.rows) == 4


def test_only_a_new_watch_is_written(world: SimpleNamespace) -> None:
    assert world.sync() == 4
    world.library.records = default_library()[:2] + [series_record(WESTWORLD, "Westworld", [True, True, True])]
    assert [world.sync(), world.sync()] == [1, 0]
    assert len(world.server.rows) == 5


def test_tmdb_outage_writes_nothing_and_recovers(world: SimpleNamespace) -> None:
    assert world.sync() == 4
    world.tmdb.down = True
    assert world.sync() == 0
    world.tmdb.down = False
    assert [world.sync(), world.sync()] == [0, 0]
    assert len(world.server.rows) == 4


def test_titles_already_on_scrob_are_not_written_twice(world: SimpleNamespace) -> None:
    world.server.store({"media_type": "movie", "tmdb_id": 278, "watched_at": "2020-05-05T05:05:05.000Z"})
    world.server.store({"media_type": "episode", "series_tmdb_id": 63247, "series_tvdb_id": 296762, "season_number": 1,
                        "episode_number": 1, "watched_at": "2020-05-05T05:05:05.000Z"})
    assert [world.sync(), world.sync()] == [2, 0]
    assert len(world.server.rows) == 4


def test_two_way_pair_sends_nothing_back_to_stremio(world: SimpleNamespace) -> None:
    runs = [world.sync("two-way") for _ in range(3)]
    assert runs[1:] == [0, 0]
    assert world.source.added == []


def test_mismatched_episode_is_never_written_and_shows_a_reason(world: SimpleNamespace) -> None:
    world.library.records = [series_record(BLEACH, "Bleach", [True]), movie_record(SHAWSHANK, "The Shawshank Redemption")]
    assert [world.sync(), world.sync()] == [1, 0]
    assert [post["media_type"] for post in world.server.posts] == ["movie"]
    assert world.pending() == [f"imdb:{BLEACH}#s02e01"]

    plan = InteractivePlan(preview=True)
    Orchestrator(pair_config(), interactive=plan).run(dry_run=True, write_state_json=False)
    row = next(iter(plan.rows.values()))
    assert (row["key"], row["result"]) == (f"imdb:{BLEACH}#s02e01", "unresolved")
    assert "TMDB ID" in row["reason"]


def ordered_videos(*numbers: int) -> list[dict[str, Any]]:
    return [videos_for(range(number, number + 1))[0] for number in numbers]


def watched_value(videos: list[dict[str, Any]], *numbers: int) -> str:
    ids = [str(video["id"]) for video in videos]
    return stremio.serialize_watched_bits([int(value.rsplit(":", 1)[-1]) in numbers for value in ids], ids)


def decoded(value: str) -> set[str]:
    orders, _ = stremio.video_orders_for_series_record(fresh_adapter(), {"_id": WESTWORLD, "type": "series"}, value)
    return stremio.decode_watched_episodes(value, [str(video["id"]) for video in orders[0]])


def episode_ids(*numbers: int) -> set[str]:
    return {f"{WESTWORLD}:1:{number}" for number in numbers}


def test_changed_watched_state_is_read_with_the_current_episode_order(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2, 3))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    assert decoded(watched_value(ordered_videos(1, 2, 3), 3)) == episode_ids(3)

    server.videos = ordered_videos(2, 1, 3)
    assert decoded(watched_value(ordered_videos(2, 1, 3), 2, 3)) == episode_ids(2, 3)


def test_unchanged_watched_state_makes_no_cinemeta_request(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2, 3))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    value = watched_value(ordered_videos(1, 2, 3), 1, 2)

    for _ in range(3):
        decoded(value)

    assert server.calls == 1


def test_cinemeta_outage_does_not_mark_the_watched_state_checked(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    decoded(watched_value(ordered_videos(1, 2), 1))

    value = watched_value(ordered_videos(1, 2, 3), 3)
    server.videos, server.down = ordered_videos(1, 2, 3), True
    with pytest.raises(stremio.EpisodeIndexUnavailable):
        decoded(value)
    assert stremio_common.read_cinemeta_entry(WESTWORLD)["checked_watched"] != value

    server.down = False
    assert decoded(value) == episode_ids(3)
    assert stremio_common.read_cinemeta_entry(WESTWORLD)["checked_watched"] == value


def test_outage_never_decodes_a_changed_state_with_an_unverified_order(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2, 3))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    decoded(watched_value(ordered_videos(1, 2, 3), 3))

    server.videos, server.down = ordered_videos(2, 1, 3), True
    value = watched_value(ordered_videos(2, 1, 3), 2, 3)
    assert stremio.order_matches_anchor(value, [str(video["id"]) for video in ordered_videos(1, 2, 3)])

    with pytest.raises(stremio.EpisodeIndexUnavailable):
        decoded(value)

    server.down = False
    assert decoded(value) == episode_ids(2, 3)


@pytest.mark.parametrize("response", [SimpleNamespace(status_code=404), None])
def test_title_cinemeta_no_longer_lists_does_not_block_the_read(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch, response: Any) -> None:
    server = CinemetaServer(ordered_videos(1, 2))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    decoded(watched_value(ordered_videos(1, 2), 1))

    def gone(*_a: Any, **_k: Any) -> SimpleNamespace:
        def check() -> None:
            if response is not None:
                raise requests.exceptions.HTTPError("gone", response=response)
        return SimpleNamespace(raise_for_status=check, json=lambda: {"meta": {}})

    monkeypatch.setattr(stremio.requests, "get", gone)
    assert decoded(watched_value(ordered_videos(1, 2), 2)) == episode_ids(2)


def test_empty_cinemeta_refresh_keeps_the_cached_list(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    decoded(watched_value(ordered_videos(1, 2), 1))

    value = watched_value(ordered_videos(1, 2), 2)
    server.videos = []
    assert decoded(value) == episode_ids(2)
    assert stremio_common.read_cinemeta_entry(WESTWORLD)["checked_watched"] != value


def test_writes_use_the_current_episode_order(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2, 3))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    value = watched_value(ordered_videos(1, 2, 3), 3)
    decoded(value)

    server.videos = ordered_videos(2, 1, 3)
    record: dict[str, Any] = {"_id": WESTWORLD, "type": "series", "state": {"watched": value}}

    assert stremio._apply_episode(fresh_adapter(), record, {"season": 1, "episode": 2}, True) is None
    assert stremio.decode_watched_episodes(record["state"]["watched"], [str(video["id"]) for video in ordered_videos(2, 1, 3)]) == episode_ids(2, 3)


def test_write_is_held_back_when_the_episode_order_cannot_be_refreshed(cinemeta_disk: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    server = CinemetaServer(ordered_videos(1, 2, 3))
    monkeypatch.setattr(stremio.requests, "get", server.get)
    value = watched_value(ordered_videos(1, 2, 3), 3)
    decoded(value)

    server.down = True
    record: dict[str, Any] = {"_id": WESTWORLD, "type": "series", "state": {"watched": value}}

    assert stremio._apply_episode(fresh_adapter(), record, {"season": 1, "episode": 2}, True)
    assert record["state"]["watched"] == value


class NativeTmdb(FakeTmdb):
    def __init__(self) -> None:
        super().__init__()
        self.refresh_failures: set[int] = set()
        self.detail_down = False

    def fetch(self, **options: Any) -> dict[str, Any]:
        if options.get("fresh") and self.detail_down:
            raise OSError("tmdb down")
        return {"detail": {"number_of_seasons": 2}}

    def _get(self, url: str, params: Any = None, **options: Any) -> dict[str, Any]:
        season = int(url.rsplit("/", 1)[-1])
        if season == 0:
            raise SeasonFailure(404)
        if options.get("fresh") and season in self.refresh_failures:
            raise SeasonFailure(503)
        return {"episodes": [{"episode_number": number, "name": f"E{number}"} for number in (1, 2)]}


NATIVE_ID = "tmdb:500"
NATIVE_UNKNOWN = bitfield([f"{NATIVE_ID}:9:9"])


def test_partial_tmdb_refresh_fails_the_read_and_is_not_marked_checked(cinemeta_disk: Path) -> None:
    tmdb = NativeTmdb()
    instance = adapter(tmdb)
    stremio.tmdb_native_video_orders(instance, "500", video_prefix=NATIVE_ID)
    tmdb.refresh_failures = {2}

    with pytest.raises(stremio.EpisodeIndexUnavailable):
        stremio._native_orders_for(instance, "500", NATIVE_ID, NATIVE_UNKNOWN)
    assert stremio_common.read_cinemeta_entry("tmdb_500") == {}


def test_failed_detail_refresh_is_not_marked_checked_and_recovers(cinemeta_disk: Path) -> None:
    tmdb = NativeTmdb()
    instance = adapter(tmdb)
    stremio.tmdb_native_video_orders(instance, "500", video_prefix=NATIVE_ID)
    tmdb.detail_down = True

    with pytest.raises(stremio.EpisodeIndexUnavailable):
        stremio._native_orders_for(instance, "500", NATIVE_ID, NATIVE_UNKNOWN)
    assert stremio_common.read_cinemeta_entry("tmdb_500") == {}

    tmdb.detail_down = False
    stremio._native_orders_for(adapter(tmdb), "500", NATIVE_ID, NATIVE_UNKNOWN)
    assert stremio_common.read_cinemeta_entry("tmdb_500")["checked_watched"] == NATIVE_UNKNOWN


def test_strict_detail_refresh_reports_an_outage_instead_of_cached_data(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    provider = TmdbProvider(lambda: {"tmdb": {"api_key": "k"}, "metadata": {"backoff_max_retries": 0}}, lambda _cfg: None)
    provider._disk_dir = tmp_path
    body = {"id": 500, "name": "Show", "number_of_seasons": 1}
    monkeypatch.setattr("providers.metadata._meta_TMDB.requests.get",
                        lambda *_a, **_k: SimpleNamespace(status_code=200, headers={}, raise_for_status=lambda: None, json=lambda: body))
    assert provider.fetch(entity="tv", ids={"tmdb": "500"}, need={"ids": True})

    def down(*_a: Any, **_k: Any) -> Any:
        raise requests.exceptions.ConnectionError("tmdb down")

    monkeypatch.setattr("providers.metadata._meta_TMDB.requests.get", down)
    monkeypatch.setattr("providers.metadata._meta_TMDB.time.sleep", lambda _seconds: None)
    assert provider.fetch(entity="tv", ids={"tmdb": "500"}, need={"ids": True}, fresh=True)
    with pytest.raises(requests.exceptions.ConnectionError):
        provider.fetch(entity="tv", ids={"tmdb": "500"}, need={"ids": True}, fresh=True, strict=True)


def test_show_without_specials_still_counts_as_refreshed(cinemeta_disk: Path) -> None:
    stremio._native_orders_for(adapter(NativeTmdb()), "500", NATIVE_ID, NATIVE_UNKNOWN)

    assert stremio_common.read_cinemeta_entry("tmdb_500")["checked_watched"] == NATIVE_UNKNOWN


def test_concurrent_cache_updates_keep_every_field(cinemeta_disk: Path) -> None:
    from concurrent.futures import ThreadPoolExecutor

    with ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda number: stremio_common.update_cinemeta_entry(WESTWORLD, **{f"field_{number}": number}), range(40)))

    assert len(stremio_common.read_cinemeta_entry(WESTWORLD)) == 40


class TrackerOps(StremioOps):
    def __init__(self) -> None:
        self.adapter = SimpleNamespace(instance_id="default", config={})
        self.rows: dict[str, dict[str, Any]] = {}
        self.adds: list[str] = []
        self.removes: list[str] = []

    def name(self) -> str:
        return "SCROB"

    def label(self) -> str:
        return "Tracker"

    def capabilities(self) -> Any:
        return SCROB_OPS.capabilities()

    def build_index(self, _cfg: Any, *, feature: str, **_: Any) -> dict[str, dict[str, Any]]:
        return dict(self.rows)

    def add(self, _cfg: Any, items: Any, *, feature: str, dry_run: bool = False, **_: Any) -> dict[str, Any]:
        keys = [stremio_common.canonical_item_key(item) for item in items]
        for key, item in zip(keys, items):
            self.rows[key] = dict(item)
        self.adds.extend(keys)
        return {"ok": True, "count": len(keys), "confirmed_keys": keys}

    def remove(self, _cfg: Any, items: Any, *, feature: str, dry_run: bool = False, **_: Any) -> dict[str, Any]:
        keys = [stremio_common.canonical_item_key(item) for item in items]
        for key in keys:
            self.rows.pop(key, None)
        self.removes.extend(keys)
        return {"ok": True, "count": len(keys), "confirmed_keys": keys}


def removal_config(mode: str) -> dict[str, Any]:
    cfg = pair_config(mode)
    cfg["sync"].update({"enable_remove": True, "include_observed_deletes": True, "allow_mass_delete": True})
    cfg["pairs"][0]["features"]["history"]["remove"] = True
    return cfg


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_upgrade_to_tmdb_keys_removes_and_adds_nothing(world: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    tracker = TrackerOps()
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"STREMIO": world.source, "SCROB": tracker})
    world.tmdb.down = True
    Orchestrator(removal_config(mode)).run()
    written = list(tracker.adds)
    assert written and all(key.startswith("imdb:") for key in written)

    world.tmdb.down = False
    assert "tmdb:278" in world.source.build_index({}, feature="history")
    for _ in range(2):
        Orchestrator(removal_config(mode)).run()
    assert (tracker.adds, tracker.removes, world.source.added) == (written, [], [])

    world.tmdb.down = True
    Orchestrator(removal_config(mode)).run()
    assert (tracker.adds, tracker.removes, world.source.added) == (written, [], [])


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_a_real_stremio_removal_still_reaches_the_target_after_the_upgrade(world: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    tracker = TrackerOps()
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"STREMIO": world.source, "SCROB": tracker})
    world.tmdb.down = True
    Orchestrator(removal_config(mode)).run()
    world.tmdb.down = False
    Orchestrator(removal_config(mode)).run()

    world.library.records = [record for record in world.library.records if record["_id"] != SHAWSHANK]
    Orchestrator(removal_config(mode)).run()

    assert tracker.removes == [f"imdb:{SHAWSHANK}"]


CINEMETA_VIDEOS = stremio.cinemeta_videos


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_cinemeta_outage_fails_the_read_instead_of_removing_the_title(world: SimpleNamespace, monkeypatch: pytest.MonkeyPatch, mode: str) -> None:
    server = CinemetaServer(VIDEOS[WESTWORLD])
    monkeypatch.setattr(stremio, "cinemeta_videos", CINEMETA_VIDEOS)
    monkeypatch.setattr(stremio.requests, "get", server.get)
    tracker = TrackerOps()
    monkeypatch.setattr("cw_platform.orchestrator.facade.load_sync_providers", lambda: {"STREMIO": world.source, "SCROB": tracker})
    Orchestrator(removal_config(mode)).run()
    written = list(tracker.adds)
    assert "tmdb:63247#s01e01" in written

    world.library.records = [movie_record(SHAWSHANK, "The Shawshank Redemption"), movie_record(WELLNESS, "A Cure for Wellness"),
                             series_record(WESTWORLD, "Westworld", [True, True, True])]
    server.down = True
    world.source.adapter = adapter(world.tmdb)
    Orchestrator(removal_config(mode)).run()
    assert (tracker.adds, tracker.removes) == (written, [])

    server.down = False
    world.source.adapter = adapter(world.tmdb)
    Orchestrator(removal_config(mode)).run()
    assert tracker.removes == []
    assert "tmdb:63247#s01e03" in tracker.adds

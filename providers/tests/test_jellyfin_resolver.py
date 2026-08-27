from __future__ import annotations

import importlib
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

common = importlib.import_module("providers.sync.jellyfin._common")
history = importlib.import_module("providers.sync.jellyfin._history")
progress = importlib.import_module("providers.sync.jellyfin._progress")


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class FakeHttp:
    def __init__(self, items: list[dict[str, Any]], episodes: dict[str, list[dict[str, Any]]] | None = None) -> None:
        self.items = items
        self.episodes = episodes or {}
        self.calls: list[dict[str, Any]] = []

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> FakeResp:
        params = dict(params or {})
        self.calls.append({"method": "GET", "path": path, "params": params})
        assert "AnyProviderIdEquals" not in params
        if path == "/Sessions":
            return FakeResp(200, [])
        if path.startswith("/Items/"):
            item_id = path.removeprefix("/Items/")
            for item in self.items:
                if str(item.get("Id") or "") == item_id:
                    return FakeResp(200, item)
            return FakeResp(404, {})
        if path == "/Items":
            return FakeResp(200, {"Items": self.items, "TotalRecordCount": len(self.items)})
        if path.startswith("/Shows/") and path.endswith("/Episodes"):
            series_id = path.split("/")[2]
            rows = self.episodes.get(series_id, [])
            return FakeResp(200, {"Items": rows, "TotalRecordCount": len(rows)})
        return FakeResp(404, {})

    def post(self, path: str, *, params: dict[str, Any] | None = None, json: Any = None) -> FakeResp:
        self.calls.append({"method": "POST", "path": path, "params": dict(params or {}), "json": json})
        return FakeResp(204, {})


class FakeCfg:
    user_id = "U1"
    strict_id_matching = False
    targeted_lookup = True
    watchlist_guid_priority = None
    history_guid_priority = None
    history_libraries = None
    progress_libraries = None


class FakeAdapter:
    def __init__(self, http: FakeHttp) -> None:
        self.client = http
        self.cfg = FakeCfg()


def jf_row(
    iid: str,
    typ: str,
    name: str,
    *,
    tmdb: str | None = None,
    tvdb: str | None = None,
    series_id: str | None = None,
    season: int | None = None,
    episode: int | None = None,
    path: str | None = None,
) -> dict[str, Any]:
    provider_ids: dict[str, str] = {}
    if tmdb:
        provider_ids["Tmdb"] = tmdb
    if tvdb:
        provider_ids["Tvdb"] = tvdb
    row: dict[str, Any] = {"Id": iid, "Type": typ, "Name": name, "ProviderIds": provider_ids}
    if series_id:
        row["SeriesId"] = series_id
    if season is not None:
        row["ParentIndexNumber"] = season
    if episode is not None:
        row["IndexNumber"] = episode
    if path:
        row["Path"] = path
    return row


def test_episode_provider_id_resolves_from_index_without_jellyfin_filter_query():
    episode = jf_row("E1", "Episode", "Pilot", tmdb="7535444", series_id="S1", season=1, episode=1)
    adapter = FakeAdapter(FakeHttp([episode]))

    item = {"type": "episode", "title": "Pilot", "season": 1, "episode": 1, "ids": {"tmdb": "7535444"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "E1"
    assert not any("AnyProviderIdEquals" in call["params"] for call in adapter.client.calls)


def test_episode_resolves_by_show_provider_id_and_episode_number_from_index():
    series = jf_row("S1", "Series", "Example Show", tmdb="999")
    episode = jf_row("E2", "Episode", "Second", series_id="S1", season=1, episode=2)
    adapter = FakeAdapter(FakeHttp([series], episodes={"S1": [episode]}))

    item = {
        "type": "episode",
        "title": "Second",
        "series_title": "Example Show",
        "season": 1,
        "episode": 2,
        "ids": {},
        "show_ids": {"tmdb": "999"},
    }

    assert common.resolve_item_id(adapter, item, feature="history") == "E2"
    assert not any("AnyProviderIdEquals" in call["params"] for call in adapter.client.calls)


def test_episode_can_resolve_by_path_when_provider_ids_are_missing():
    path = r"Z:\TV\Example Show\Season 01\S01E02.mkv"
    episode = jf_row("E3", "Episode", "Second", series_id="S1", season=1, episode=2, path=path)
    adapter = FakeAdapter(FakeHttp([episode]))

    item = {"type": "episode", "title": "Second", "season": 1, "episode": 2, "ids": {}, "path": path}

    assert common.resolve_item_id(adapter, item, feature="history") == "E3"


def test_jellyfin_long_numeric_item_ids_are_valid_backend_ids():
    row = jf_row("7214430293560476068", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([row]))

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "7214430293560476068"


def test_normalize_does_not_export_native_jellyfin_id():
    row = jf_row("M1", "Movie", "Encanto", tmdb="568124")

    item = common.normalize(row)

    assert item["ids"] == {"tmdb": "568124"}
    assert item["jellyfin_item_id"] == "M1"
    assert "jellyfin" not in item["ids"]


def test_resolve_rejects_stale_native_jellyfin_id_when_public_id_disagrees():
    wrong = jf_row("M0", "Movie", "Wrong Movie", tmdb="1")
    right = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([wrong, right]))

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"jellyfin": "M0", "tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "M1"
    assert any(call["path"] == "/Items/M0" for call in adapter.client.calls)
    assert any(call["params"].get("SearchTerm") == "Encanto" for call in adapter.client.calls)


def test_resolve_rejects_stale_jellyfin_item_id_when_public_id_disagrees():
    wrong = jf_row("M0", "Movie", "Wrong Movie", tmdb="1")
    right = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([wrong, right]))

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"tmdb": "568124"}, "jellyfin_item_id": "M0"}

    assert common.resolve_item_id(adapter, item, feature="history") == "M1"
    assert any(call["path"] == "/Items/M0" for call in adapter.client.calls)
    assert any(call["params"].get("SearchTerm") == "Encanto" for call in adapter.client.calls)


def test_progress_write_does_not_trust_stale_jellyfin_item_id():
    wrong = jf_row("M0", "Movie", "Wrong Movie", tmdb="1")
    current = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    current["RunTimeTicks"] = 6_000_000_000
    current["UserData"] = {"PlaybackPositionTicks": 0}
    adapter = FakeAdapter(FakeHttp([wrong, current]))

    item = {
        "type": "movie",
        "title": "Encanto",
        "year": 2021,
        "ids": {"tmdb": "568124"},
        "jellyfin_item_id": "M0",
        "progress_ms": 120_000,
        "duration_ms": 600_000,
        "progress_at": "2026-08-01T00:00:00Z",
    }

    applied, unresolved = progress.add(adapter, [item])

    assert applied == 1
    assert unresolved == []
    assert not any(call["path"] == "/UserItems/M0/UserData" for call in adapter.client.calls)
    assert any(call["path"] == "/UserItems/M1/UserData" for call in adapter.client.calls)


def test_movie_targeted_lookup_resolves_without_full_provider_index():
    row = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([row]))

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "M1"
    assert any(call["params"].get("SearchTerm") == "Encanto" for call in adapter.client.calls)
    assert not any("StartIndex" in call["params"] for call in adapter.client.calls)


def test_movie_targeted_lookup_disabled_falls_back_to_provider_index():
    row = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([row]))
    adapter.cfg.targeted_lookup = False

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "M1"
    assert not any(call["params"].get("SearchTerm") == "Encanto" for call in adapter.client.calls)
    assert any("StartIndex" in call["params"] for call in adapter.client.calls)


def test_strict_targeted_lookup_requires_provider_id_match():
    wrong = jf_row("M0", "Movie", "Encanto", tmdb="1")
    right = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([wrong, right]))
    adapter.cfg.strict_id_matching = True

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "M1"
    assert any(call["params"].get("SearchTerm") == "Encanto" for call in adapter.client.calls)
    assert not any("StartIndex" in call["params"] for call in adapter.client.calls)


def test_episode_targeted_lookup_uses_series_search_and_episode_numbers():
    series = jf_row("S1", "Series", "Example Show", tmdb="999")
    episode = jf_row("E2", "Episode", "Second", series_id="S1", season=1, episode=2)
    adapter = FakeAdapter(FakeHttp([series], episodes={"S1": [episode]}))

    item = {
        "type": "episode",
        "title": "Second",
        "series_title": "Example Show",
        "season": 1,
        "episode": 2,
        "ids": {},
        "show_ids": {"tmdb": "999"},
    }

    assert common.resolve_item_id(adapter, item, feature="history") == "E2"
    assert any(call["path"] == "/Items" and call["params"].get("SearchTerm") == "Example Show" for call in adapter.client.calls)
    assert any(call["path"] == "/Shows/S1/Episodes" for call in adapter.client.calls)
    assert not any("StartIndex" in call["params"] for call in adapter.client.calls if call["path"] == "/Items")


def test_jellyfin_scoped_provider_index_trusts_rows_without_library_metadata():
    row = jf_row("M1", "Movie", "Encanto", tmdb="568124")
    adapter = FakeAdapter(FakeHttp([row]))
    adapter.cfg.history_libraries = ["LIB1"]

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "M1"
    assert any(call["params"].get("ParentId") == "LIB1" for call in adapter.client.calls)


def test_jellyfin_mark_played_uses_date_played_param_name():
    http = FakeHttp([])

    assert history._mark_played(http, "U1", "E1", date_played_iso="2026-08-02T12:00:00Z")

    call = http.calls[-1]
    assert call["method"] == "POST"
    assert call["params"]["userId"] == "U1"
    assert call["params"]["DatePlayed"] == "2026-08-02T12:00:00Z"

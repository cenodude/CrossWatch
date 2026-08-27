from __future__ import annotations

import importlib
from typing import Any


common = importlib.import_module("providers.sync.emby._common")
history = importlib.import_module("providers.sync.emby._history")
progress = importlib.import_module("providers.sync.emby._progress")
watchlist = importlib.import_module("providers.sync.emby._watchlist")


class FakeResp:
    def __init__(self, status: int, payload: Any = None):
        self.status_code = status
        self._payload = payload
        self.text = ""

    def json(self) -> Any:
        return self._payload


class FakeHttp:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.items = items
        self.calls: list[dict[str, Any]] = []

    def get(self, path: str, *, params: dict[str, Any] | None = None) -> FakeResp:
        params = dict(params or {})
        self.calls.append({"method": "GET", "path": path, "params": params})

        prefix = "/Users/U1/Items/"
        if path.startswith(prefix):
            item_id = path.removeprefix(prefix)
            for item in self.items:
                if str(item.get("Id") or "") == item_id:
                    return FakeResp(200, item)
            return FakeResp(404, {})

        if path == "/Users/U1/Views":
            return FakeResp(200, {"Items": []})

        if path == "/Users/U1/Items":
            if params.get("Filters") in {"IsPlayed", "IsResumable", "IsFavorite"}:
                include = {x.strip() for x in str(params.get("IncludeItemTypes") or "").split(",") if x.strip()}
                rows = [item for item in self.items if str(item.get("Type") or "") in include]
                return FakeResp(200, {"Items": rows, "TotalRecordCount": len(rows)})

            provider_terms = {x.strip() for x in str(params.get("AnyProviderIdEquals") or "").split(",") if x.strip()}
            include = {x.strip() for x in str(params.get("IncludeItemTypes") or "").split(",") if x.strip()}
            rows: list[dict[str, Any]] = []
            for item in self.items:
                if include and str(item.get("Type") or "") not in include:
                    continue
                provider_ids = {str(k).lower(): str(v).strip() for k, v in (item.get("ProviderIds") or {}).items()}
                pairs = {f"{key}.{int(value)}" if key in {"tmdb", "tvdb"} and value.isdigit() else f"{key}.{value}" for key, value in provider_ids.items()}
                if provider_terms & pairs:
                    rows.append(item)
            return FakeResp(200, {"Items": rows, "TotalRecordCount": len(rows)})

        return FakeResp(404, {})

    def post(self, path: str, *, params: dict[str, Any] | None = None, json: Any = None) -> FakeResp:
        self.calls.append({"method": "POST", "path": path, "params": dict(params or {}), "json": json})
        return FakeResp(204, {})

    def delete(self, path: str, *, params: dict[str, Any] | None = None) -> FakeResp:
        self.calls.append({"method": "DELETE", "path": path, "params": dict(params or {})})
        return FakeResp(204, {})


class FakeCfg:
    user_id = "U1"
    strict_id_matching = False
    history_guid_priority = None
    watchlist_guid_priority = None
    history_libraries = None
    progress_libraries = None
    watchlist_libraries = None
    watchlist_mode = "favorites"
    watchlist_query_limit = 25
    history_query_limit = 25
    history_write_delay_ms = 0


class FakeAdapter:
    def __init__(self, items: list[dict[str, Any]]) -> None:
        self.client = FakeHttp(items)
        self.cfg = FakeCfg()


def emby_movie(iid: str, title: str, tmdb: str, *, played_at: str | None = None) -> dict[str, Any]:
    row: dict[str, Any] = {
        "Id": iid,
        "Type": "Movie",
        "Name": title,
        "ProductionYear": 2021,
        "ProviderIds": {"Tmdb": tmdb},
    }
    if played_at:
        row["UserData"] = {"Played": True, "PlayCount": 1, "LastPlayedDate": played_at}
    return row


def assert_public_ids_without_native_emby(item: dict[str, Any], *, has_current_item_id: bool = True) -> None:
    assert item["ids"] == {"tmdb": "568124"}
    if has_current_item_id:
        assert item["emby_item_id"]
    assert "emby" not in item["ids"]


def test_resolve_rejects_stale_native_emby_id_when_public_id_disagrees() -> None:
    adapter = FakeAdapter(
        [
            emby_movie("10", "Wrong Movie", "1"),
            emby_movie("20", "Encanto", "568124"),
        ],
    )

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"emby": "10", "tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "20"
    assert any(call["path"] == "/Users/U1/Items/10" for call in adapter.client.calls)
    assert any(call["params"].get("AnyProviderIdEquals") == "tmdb.568124" for call in adapter.client.calls)


def test_resolve_accepts_native_emby_id_when_public_id_matches() -> None:
    adapter = FakeAdapter([emby_movie("10", "Encanto", "568124")])

    item = {"type": "movie", "title": "Encanto", "year": 2021, "ids": {"emby": "10", "tmdb": "568124"}}

    assert common.resolve_item_id(adapter, item, feature="history") == "10"
    assert not any(call["params"].get("AnyProviderIdEquals") for call in adapter.client.calls)


def test_history_index_does_not_export_native_emby_ids_for_movies() -> None:
    adapter = FakeAdapter([emby_movie("10", "Encanto", "568124", played_at="2026-08-01T00:00:00Z")])

    index = history.build_index(adapter)

    assert len(index) == 1
    item = next(iter(index.values()))
    assert_public_ids_without_native_emby(item, has_current_item_id=False)


def test_progress_index_does_not_export_native_emby_ids_for_movies() -> None:
    row = emby_movie("10", "Encanto", "568124")
    row["RunTimeTicks"] = 6_000_000_000
    row["UserData"] = {
        "PlaybackPositionTicks": 1_200_000_000,
        "LastPlayedDate": "2026-08-01T00:00:00Z",
    }
    adapter = FakeAdapter([row])

    index = progress.build_index(adapter)

    assert len(index) == 1
    item = next(iter(index.values()))
    assert_public_ids_without_native_emby(item)


def test_progress_write_does_not_trust_stale_emby_item_id() -> None:
    wrong = emby_movie("10", "Wrong Movie", "1")
    current = emby_movie("20", "Encanto", "568124")
    current["RunTimeTicks"] = 6_000_000_000
    current["UserData"] = {"PlaybackPositionTicks": 0}
    adapter = FakeAdapter([wrong, current])

    item = {
        "type": "movie",
        "title": "Encanto",
        "year": 2021,
        "ids": {"tmdb": "568124"},
        "emby_item_id": "10",
        "progress_ms": 120_000,
        "duration_ms": 600_000,
        "progress_at": "2026-08-01T00:00:00Z",
    }

    applied, unresolved = progress.add(adapter, [item])

    assert applied == 1
    assert unresolved == []
    assert not any(call["path"] == "/Users/U1/Items/10/UserData" for call in adapter.client.calls)
    assert any(call["path"] == "/Users/U1/Items/20/UserData" for call in adapter.client.calls)


def test_watchlist_index_does_not_export_native_emby_ids_for_movies() -> None:
    row = emby_movie("10", "Encanto", "568124")
    row["UserData"] = {"IsFavorite": True}
    adapter = FakeAdapter([row])

    index = watchlist.build_index(adapter)

    assert len(index) == 1
    item = next(iter(index.values()))
    assert_public_ids_without_native_emby(item)

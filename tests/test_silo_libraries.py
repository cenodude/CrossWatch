# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

import pytest

SERVER = "http://silo.local:8090"
CREDS = {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "profile_name": "Alice", "user_id": "1"}

MOVIES_4K = [{"content_id": "movie-tmdb-200", "title": "Beta", "added_at": "2026-10-27T12:00:00Z"}]
MOVIES = [{"content_id": "movie-tmdb-100", "title": "Alpha", "added_at": "2026-10-27T12:00:00Z"}]
SERIES = [{"content_id": "series-tvdb-300", "title": "Gamma", "added_at": "2026-10-27T12:00:00Z"}]
EPISODES = [{"content_id": "episode-tvdb-300-1-1", "series_id": "series-tvdb-300", "season_number": 1, "episode_number": 1, "added_at": "2026-10-27T12:00:00Z"}]
BY_LIBRARY = {
    "1": {"movie": MOVIES, "series": [], "episode": []},
    "2": {"movie": [], "series": SERIES, "episode": EPISODES},
    "3": {"movie": MOVIES_4K, "series": [], "episode": []},
}


@dataclass
class ResponseStub:
    status_code: int = 200
    payload: Any = None
    headers: dict[str, str] = field(default_factory=dict)

    @property
    def text(self) -> str:
        return "" if self.payload is None else "x"

    def json(self) -> Any:
        return self.payload


def _page(items: list[dict[str, Any]]) -> ResponseStub:
    return ResponseStub(200, {"items": items, "page": {"has_more": False}})


class FakeServer:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.libraries_status = 200

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        params = kwargs.get("params") or {}
        self.calls.append(f"{method} {path}")
        if path == "/user/libraries":
            return ResponseStub(self.libraries_status, {"items": [{"id": "1", "name": "Movies", "type": "movies"},
                                                                  {"id": "2", "name": "Series", "type": "series"},
                                                                  {"id": "3", "name": "Movies 4K", "type": "movies"}]})
        if path == "/catalog":
            lid = params.get("library_id")
            if lid:
                return _page(BY_LIBRARY[lid][params["type"]])
            return _page({"movie": MOVIES + MOVIES_4K, "series": SERIES, "episode": EPISODES}[params["type"]])
        if path == "/progress":
            rows = [{"media_item_id": cid, "completed": True, "updated_at": "2026-10-27T18:00:00Z"}
                    for cid in ("movie-tmdb-100", "movie-tmdb-200", "episode-tvdb-300-1-1")]
            return _page(rows if params.get("status") == "completed" else [])
        raise AssertionError(f"unexpected {method} {path}")


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> FakeServer:
    from providers.sync.silo import _common

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    _common._LIBRARIES.clear()
    yield fake
    _common._LIBRARIES.clear()


def _adapter(**features: list[str]) -> Any:
    from providers.sync._mod_SILO import SILOModule

    block: dict[str, Any] = dict(CREDS)
    for feature, libs in features.items():
        block[feature] = {"libraries": libs}
    return SILOModule({"silo": block})


def test_no_whitelist_reads_everything_without_membership_calls(server: FakeServer) -> None:
    idx = _adapter().build_index("history")

    assert sorted(idx) == ["tmdb:100", "tmdb:200", "tvdb:300#s01e01"]
    assert "GET /user/libraries" not in server.calls


def test_history_whitelist_limits_index_and_writes(server: FakeServer) -> None:
    ad = _adapter(history=["1"], collection=["2"])

    assert sorted(ad.build_index("history")) == ["tmdb:100"]
    assert sorted(ad.build_index("collection")) == ["tvdb:300", "tvdb:300#s01e01"]
    assert sorted(ad.build_index("history")) == ["tmdb:100"]
    assert server.calls.count("GET /user/libraries") == 1

    res = ad.add("history", [{"type": "movie", "ids": {"tmdb": "200"}, "watched_at": "2026-10-27T19:00:00Z"}])
    assert res["count"] == 0 and res["unresolved_keys"] == ["tmdb:200"]
    assert res["unresolved"][0]["status"] == "not_in_library"


def test_failed_library_list_is_not_an_empty_scope(server: FakeServer) -> None:
    server.libraries_status = 500
    with pytest.raises(Exception, match="silo_libraries_http_500"):
        _adapter(history=["1"]).build_index("history")


def test_watcher_skips_sessions_outside_scrobble_libraries(server: FakeServer) -> None:
    from providers.scrobble.silo import watch

    class Dispatch:
        def needs_user_resolution(self) -> bool:
            return False

        def dispatch(self, ev: Any) -> bool:
            return True

    cfg = {"silo": {**CREDS, "scrobble": {"libraries": ["1"]}}}
    svc = watch.SiloWatchService(dispatcher=Dispatch(), cfg_provider=lambda: cfg)
    assert svc._outside_whitelist("movie-tmdb-200") is True
    assert svc._outside_whitelist("movie-tmdb-100") is False
    assert svc._create_session({"content_id": "movie-tmdb-200", "media_type": "movie"}, "s1") is None
    cfg["silo"]["scrobble"] = {"libraries": []}
    assert svc._outside_whitelist("movie-tmdb-200") is False


def test_sink_uses_route_libraries_before_connection_whitelist(server: FakeServer) -> None:
    from providers.scrobble._media_server import DeliveryError
    from providers.scrobble.silo.sink import SiloSink

    sink = SiloSink(cfg_provider=lambda: {"silo": dict(CREDS)})
    ad = _adapter(history=["3"])
    item = {"type": "movie", "ids": {"tmdb": "200"}}
    with pytest.raises(DeliveryError, match="unmatched_in_silo"):
        sink._deliver(ad, item, True, 100.0, {"libraries": ["1"], "all_copies": True}, 0)


def test_pair_libraries_override_reaches_named_instance(server: FakeServer) -> None:
    from cw_platform.orchestrator._pairs_utils import config_with_pair_libraries
    from cw_platform.provider_instances import build_provider_config_view
    from providers.sync._mod_SILO import SILOModule

    cfg = {"silo": {"instances": {"SILO-P01": {**CREDS, "history": {"libraries": ["1", "3"]}}}}}
    view = {**build_provider_config_view(cfg, "silo", "SILO-P01"), "_cw_provider_instance": "SILO-P01"}
    scoped = config_with_pair_libraries(view, {"libraries": {"SILO#SILO-P01": ["3"]}}, "history", ("SILO", "TRAKT"))

    assert sorted(SILOModule(view).build_index("history")) == ["tmdb:100", "tmdb:200"]
    assert sorted(SILOModule(scoped).build_index("history")) == ["tmdb:200"]

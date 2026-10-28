# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

from cw_platform.provider_instances import build_provider_config_view

SERVER = "http://silo.local:8090"
CREDS = {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "profile_name": "Alice", "user_id": "1"}


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


def _version(width: int, height: int, *, range_type: str = "SDR", codec: str = "eac3", profile: str = "Dolby Digital Plus + Dolby Atmos",
             layout: str = "5.1(side)", channels: int = 6, bitrate: int = 8000) -> dict[str, Any]:
    return {"bitrate": bitrate,
            "video_tracks": [{"width": width, "height": height, "interlaced": False, "video_range_type": range_type, "hdr10_plus": False}],
            "audio_tracks": [{"codec": codec, "profile": profile, "layout": layout, "channels": channels, "language": "en", "default": True}]}


class FakeServer:
    def __init__(self) -> None:
        self.calls: list[str] = []
        self.details: dict[str, Any] = {
            "movie-tmdb-100": {"content_id": "movie-tmdb-100", "versions": [_version(1920, 1080), _version(3840, 2160, range_type="DOVI", codec="truehd", profile="Dolby TrueHD + Dolby Atmos", layout="7.1", channels=8)]},
            "episode-tvdb-300-1-1": {"content_id": "episode-tvdb-300-1-1", "versions": [_version(1920, 1080, codec="aac", profile="LC", layout="stereo", channels=2)]},
        }
        self.pages = {
            "movie": [{"content_id": "movie-tmdb-100", "title": "Alpha", "year": 2024, "added_at": "2026-10-27T12:54:20.871Z"},
                      {"content_id": "local-abc123", "title": "Home video", "added_at": "2026-10-27T12:54:21.000Z"}],
            "series": [{"content_id": "series-tvdb-300", "title": "Gamma", "year": 2020, "added_at": "2026-10-27T12:56:02.242Z"}],
            "episode": [{"content_id": "episode-tvdb-300-1-1", "series_id": "series-tvdb-300", "series_title": "Gamma", "title": "Pilot",
                         "season_number": 1, "episode_number": 1, "added_at": "2026-10-27T12:58:40.218Z"}],
        }

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        self.calls.append(f"{method} {path}")
        if method == "GET" and path == "/catalog":
            return _page(self.pages[kwargs["params"]["type"]])
        if method == "GET" and path.startswith("/catalog/items/"):
            cid = path.rsplit("/", 1)[1]
            return ResponseStub(200, self.details[cid]) if cid in self.details else ResponseStub(404, {})
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


def _adapter(media_info: bool = False) -> Any:
    from providers.sync._mod_SILO import SILOModule

    block = dict(CREDS)
    if media_info:
        block["collection"] = {"media_info": True}
    return SILOModule({"silo": block})


def test_collection_index_lists_library_with_added_dates(server: FakeServer) -> None:
    idx = _adapter().build_index("collection")

    assert sorted(idx) == ["tmdb:100", "tvdb:300", "tvdb:300#s01e01"]
    assert idx["tmdb:100"]["type"] == "movie" and idx["tmdb:100"]["collected_at"] == "2026-10-27T12:54:20Z"
    assert idx["tvdb:300"]["type"] == "show" and idx["tvdb:300"]["collected_at"] == "2026-10-27T12:56:02Z"
    ep = idx["tvdb:300#s01e01"]
    assert ep["type"] == "episode" and ep["show_ids"] == {"tvdb": "300"} and ep["collected_at"] == "2026-10-27T12:58:40Z"
    assert "media_info" not in idx["tmdb:100"] and "format" not in idx["tmdb:100"]
    assert not any(c.startswith("GET /catalog/items/") for c in server.calls)


def test_collection_media_info_is_opt_in_best_version_and_cached(server: FakeServer) -> None:
    idx = _adapter(media_info=True).build_index("collection")

    movie = idx["tmdb:100"]
    assert movie["format"] == "digital"
    assert movie["media_info"] == {"resolution": "uhd_4k", "hdr": "dolby_vision", "audio": "dolby_atmos", "audio_channels": "7.1"}
    assert idx["tvdb:300#s01e01"]["media_info"] == {"resolution": "hd_1080p", "audio": "aac", "audio_channels": "2.0"}
    assert "media_info" not in idx["tvdb:300"]
    assert sorted(c for c in server.calls if c.startswith("GET /catalog/items/")) == ["GET /catalog/items/episode-tvdb-300-1-1", "GET /catalog/items/movie-tmdb-100"]

    from providers.sync.silo import _common

    _common._LIBRARIES.clear()
    server.calls.clear()
    again = _adapter(media_info=True).build_index("collection")
    assert again["tmdb:100"]["media_info"] == movie["media_info"]
    assert not any(c.startswith("GET /catalog/items/") for c in server.calls)


def test_collection_is_read_only_source(server: FakeServer) -> None:
    from providers.sync._mod_SILO import OPS

    caps = OPS.capabilities()["collection"]
    assert OPS.features()["collection"] is True
    assert caps["read"] is True and caps["upsert"] is False and caps["types"]["seasons"] is False
    res = _adapter().add("collection", [{"type": "movie", "ids": {"tmdb": "100"}}])
    assert res["count"] == 0 and res["unresolved_keys"] == ["tmdb:100"]


def test_pair_media_info_option_reaches_named_instance() -> None:
    from cw_platform.orchestrator._pairs import _config_with_pair_feature_options
    from providers.auth._auth_SILO import provider_block

    cfg = {"silo": {"instances": {"SILO-P01": dict(CREDS)}}}
    view = build_provider_config_view(cfg, "silo", "SILO-P01")
    out = _config_with_pair_feature_options(view, {"enable": True, "media_info": True}, ("SILO", "TRAKT"), "collection")
    assert provider_block(out, "SILO-P01")["collection"] == {"media_info": True}
    same = _config_with_pair_feature_options(view, {"enable": True, "media_info": True}, ("SILO", "TRAKT"), "history")
    assert same is view


def test_failed_media_lookup_keeps_cached_media_info(server: FakeServer) -> None:
    from providers.sync.silo import _common

    first = _adapter(media_info=True).build_index("collection")
    _common._LIBRARIES.clear()
    server.pages["movie"][0]["added_at"] = "2026-10-28T09:00:00Z"
    del server.details["movie-tmdb-100"]
    again = _adapter(media_info=True).build_index("collection")
    assert again["tmdb:100"]["media_info"] == first["tmdb:100"]["media_info"]

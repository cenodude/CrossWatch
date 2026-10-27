# CrossWatch test scripts
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Callable

import pytest

from cw_platform.provider_instances import build_provider_config_view

SERVER = "http://silo.local:8090"
CREDS = {"server_url": SERVER, "access_token": "a", "refresh_token": "r", "profile_id": "p-1", "profile_name": "Alice", "user_id": "1"}
NAMED_ONLY = {"silo": {"instances": {"SILO-P01": dict(CREDS)}}}


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
        self.routes: dict[tuple[str, str], Callable[[dict[str, Any]], ResponseStub]] = {}
        self.calls: list[dict[str, Any]] = []
        pages = {
            "movie": [{"content_id": "movie-tmdb-100", "title": "Alpha", "year": 2024, "runtime": 100}],
            "series": [{"content_id": "series-tvdb-300", "title": "Gamma"}],
            "episode": [{"content_id": "episode-tvdb-300-1-1", "series_id": "series-tvdb-300", "series_title": "Gamma", "title": "Pilot", "season_number": 1, "episode_number": 1, "runtime": 45}],
        }
        self.on("GET", "/catalog", lambda kw: _page(pages[kw["params"]["type"]]))

    def on(self, method: str, path: str, handler: Callable[[dict[str, Any]], ResponseStub]) -> None:
        self.routes[(method, path)] = handler

    def __call__(self, session: Any, method: str, url: str, **kwargs: Any) -> ResponseStub:
        path = url.split("/api/v2", 1)[1]
        self.calls.append({"method": method, "path": path, **kwargs})
        handler = self.routes.get((method, path))
        if handler is None:
            raise AssertionError(f"unexpected {method} {path}")
        return handler(kwargs)


@pytest.fixture()
def server(monkeypatch: pytest.MonkeyPatch, tmp_path: Any) -> FakeServer:
    from providers.sync.silo import _common

    fake = FakeServer()
    monkeypatch.setattr(_common, "request_with_auth", fake)
    monkeypatch.setattr(_common, "STATE_DIR", tmp_path)
    _common._LIBRARIES.clear()
    yield fake
    _common._LIBRARIES.clear()


def _adapter() -> Any:
    from services.playback_progress import get_service

    adapter = get_service()._adapter("silo")
    assert adapter is not None
    return adapter


def _rows() -> list[dict[str, Any]]:
    return [
        {"media_item_id": "movie-tmdb-100", "completed": False, "position_seconds": 1500.0, "duration_seconds": 6000, "updated_at": "2026-10-27T18:00:00.000Z"},
        {"media_item_id": "episode-tvdb-300-1-1", "completed": False, "position_seconds": 600.0, "duration_seconds": 2700, "updated_at": "2026-10-27T17:00:00.000Z"},
    ]


def test_service_enumerates_named_profile_and_reports_capabilities() -> None:
    from services.playback_progress import get_service
    from services.playback_progress.service import PHASE1_PROVIDERS, _profile_has_explicit_identity

    assert "silo" in PHASE1_PROVIDERS
    assert _profile_has_explicit_identity(NAMED_ONLY, "silo", "default") is False
    assert [s["instance_id"] for s in get_service().provider_instances(NAMED_ONLY) if s["provider"] == "silo"] == ["SILO-P01"]
    view = build_provider_config_view(NAMED_ONLY, "silo", "SILO-P01")
    cap = _adapter().capabilities(view, instance_id="SILO-P01", instance_label="Alice")
    assert cap.configured and cap.read and cap.update_progress and cap.mark_watched and cap.remove_progress
    empty = _adapter().capabilities(build_provider_config_view(NAMED_ONLY, "silo", "default"), instance_id="default", instance_label="Default")
    assert empty.configured is False


def test_list_progress_builds_records_and_blocks_active_playback(server: FakeServer) -> None:
    server.on("GET", "/progress", lambda kw: _page(_rows()))
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(200, {"items": [{"content_id": "episode-tvdb-300-1-1", "profile_id": "p-1"}]}))
    view = build_provider_config_view(NAMED_ONLY, "silo", "SILO-P01")

    res = _adapter().list_progress(view, instance_id="SILO-P01", instance_label="Alice")

    assert res.ok
    by_key = {r.canonical_key: r for r in res.items}
    movie = by_key["tmdb:100"]
    assert movie.remote_id == "movie-tmdb-100" and movie.media_type == "movie" and movie.title == "Alpha"
    assert movie.progress_percent == pytest.approx(25.0) and movie.duration_seconds == 6000 and movie.remaining_seconds == 4500
    assert movie.can_update_progress and movie.can_remove_progress and movie.can_mark_watched
    ep = by_key["tvdb:300#s01e01"]
    assert ep.series_title == "Gamma" and ep.episode_title == "Pilot" and ep.season == 1 and ep.episode == 1
    assert ep.can_update_progress is False and ep.capability_messages == ["Playback is active."]


def test_update_remove_and_mark_watched(server: FakeServer) -> None:
    rows = _rows()
    completed: list[dict[str, Any]] = []
    sent: list[Any] = []

    def progress(kw: dict[str, Any]) -> ResponseStub:
        return _page(completed if kw["params"]["status"] == "completed" else rows)

    def write(kw: dict[str, Any]) -> ResponseStub:
        sent.append(kw["json"])
        for e in kw["json"]["items"]:
            if e["position_ms"] >= e["duration_ms"]:
                completed.append({"media_item_id": e["media_item_id"], "completed": True, "updated_at": e.get("updated_at")})
            else:
                rows[0]["position_seconds"] = e["position_ms"] / 1000
        return ResponseStub(200, {"items": [{"index": i, "media_item_id": e["media_item_id"], "status": "success"} for i, e in enumerate(kw["json"]["items"])], "summary": {}})

    server.on("GET", "/progress", progress)
    server.on("GET", "/profiles/household/sessions", lambda kw: ResponseStub(200, {"items": []}))
    server.on("POST", "/sync/progress", write)
    server.on("DELETE", "/progress/movie-tmdb-100/position", lambda kw: ResponseStub(204, None))
    view = build_provider_config_view(NAMED_ONLY, "silo", "SILO-P01")
    adapter = _adapter()
    record = next(r for r in adapter.list_progress(view, instance_id="SILO-P01", instance_label="Alice").items if r.canonical_key == "tmdb:100").to_dict()

    upd = adapter.update_progress(view, record, 50.0, instance_id="SILO-P01", instance_label="Alice")
    assert upd.ok and upd.status == "applied"
    assert sent[0]["items"][0]["position_ms"] == 3_000_000 and sent[0]["items"][0]["duration_ms"] == 6_000_000

    rem = adapter.remove_progress(view, record, instance_id="SILO-P01", instance_label="Alice")
    assert rem.ok

    done = adapter.mark_watched(view, record, instance_id="SILO-P01", instance_label="Alice", watched_at="2026-10-27T19:00:00Z")
    assert done.ok and done.history_result is not None
    assert sent[-1]["items"][0] == {"client_ref": "tmdb:100", "media_item_id": "movie-tmdb-100", "position_ms": 6_000_000, "duration_ms": 6_000_000, "updated_at": "2026-10-27T19:00:00Z"}

    bad = adapter.update_progress(view, record, 100.0, instance_id="SILO-P01", instance_label="Alice")
    assert bad.ok is False and bad.error_code == "invalid_progress"


def test_list_failure_is_reported_not_empty(server: FakeServer) -> None:
    server.on("GET", "/progress", lambda kw: ResponseStub(500, {}))
    view = build_provider_config_view(NAMED_ONLY, "silo", "SILO-P01")

    res = _adapter().list_progress(view, instance_id="SILO-P01", instance_label="Alice")

    assert res.ok is False and res.retryable is True and res.items == []

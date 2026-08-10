# CrossWatch test scripts
from __future__ import annotations

import json
from typing import Any

import pytest


class _Resp:
    def __init__(self, status_code: int = 200, payload: Any = None) -> None:
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = json.dumps(self._payload)
        self.headers: dict[str, str] = {}

    def json(self) -> Any:
        return self._payload


CFG = {"punchplay": {"access_token": "at", "device_id": "crosswatch-abc"}}

RECORD = {
    "remote_id": "42",
    "canonical_key": "tmdb:69478#s06e02",
    "media_type": "episode",
    "ids": {"tmdb": "69478"},
    "season": 6,
    "episode": 2,
    "series_title": "The Handmaid's Tale",
    "episode_title": "Exile",
    "duration_seconds": 3307,
}


@pytest.fixture(autouse=True)
def _reset_governor():
    from providers.sync.punchplay import _common as c

    c._GOVERNORS.clear()
    yield
    c._GOVERNORS.clear()


@pytest.fixture()
def adapter(monkeypatch: pytest.MonkeyPatch):
    from services.playback_progress.adapters import punchplay as mod

    calls: list[dict[str, Any]] = []

    def fake(client: Any, method: str, url: str, **kw: Any) -> _Resp:
        calls.append({"method": method, "url": url, **kw})
        return _Resp(200, {})

    monkeypatch.setattr(mod, "punchplay_request", fake)
    return mod.PunchPlayPlaybackAdapter(), calls


def test_adapter_is_registered_in_the_service() -> None:
    from services.playback_progress.adapters.punchplay import PunchPlayPlaybackAdapter
    from services.playback_progress.service import PlaybackProgressService

    found = PlaybackProgressService()._adapter("punchplay")
    assert isinstance(found, PunchPlayPlaybackAdapter)


def test_provider_instances_include_punchplay_profiles() -> None:
    from services.playback_progress.service import PlaybackProgressService

    cfg = {
        "punchplay": {
            "access_token": "default-token",
            "instances": {"PUNCHPLAY-P01": {"access_token": "profile-token"}},
        }
    }

    specs = [spec for spec in PlaybackProgressService().provider_instances(cfg) if spec["provider"] == "punchplay"]

    assert [spec["instance_id"] for spec in specs] == ["default", "PUNCHPLAY-P01"]


def test_capabilities_reflect_connection_state() -> None:
    from services.playback_progress.adapters.punchplay import PunchPlayPlaybackAdapter

    a = PunchPlayPlaybackAdapter()
    on = a.capabilities(CFG, instance_id="default", instance_label="Default")
    off = a.capabilities({"punchplay": {}}, instance_id="default", instance_label="Default")

    assert (on.configured, on.read, on.remove_progress, on.mark_watched, on.update_progress) == (True, True, True, True, True)
    assert (off.configured, off.read) == (False, False)
    assert off.reason


def test_remove_progress_dismisses_by_remote_id(adapter) -> None:
    a, calls = adapter

    res = a.remove_progress(CFG, RECORD, instance_id="default", instance_label="Default")

    assert res.ok is True
    assert calls[0]["method"] == "DELETE"
    assert calls[0]["url"].endswith("/playback/in-progress/42")
    assert calls[0]["no_wait"] is True


def test_remove_without_remote_id_fails_cleanly(adapter) -> None:
    a, calls = adapter

    res = a.remove_progress(CFG, dict(RECORD, remote_id=""), instance_id="default", instance_label="Default")

    assert res.ok is False
    assert res.error_code == "missing_remote_id"
    assert calls == []


def test_update_progress_posts_incomplete_stop(adapter) -> None:
    a, calls = adapter

    res = a.update_progress(CFG, RECORD, 50.0, instance_id="default", instance_label="Default")

    assert res.ok is True
    p = calls[0]["json"]
    assert calls[0]["url"].endswith("/playback/stop")
    assert p["media_type"] == "episode"
    assert p["tmdb_id"] == 69478
    assert p["season"] == 6 and p["episode"] == 2
    assert abs(p["progress"] - 0.5) < 0.01
    assert p["playback_session_id"] and p["event_id"]
    assert p["watched"] is False
    assert p["watched_threshold"] == 1.0


def test_mark_watched_posts_stop_with_watched_flag(adapter) -> None:
    a, calls = adapter

    res = a.mark_watched(CFG, RECORD, instance_id="default", instance_label="Default")

    assert res.ok is True
    p = calls[0]["json"]
    assert calls[0]["url"].endswith("/playback/stop")
    assert p["watched"] is True
    assert p["progress"] == 1.0


def test_missing_ids_fails_without_a_request(adapter) -> None:
    a, calls = adapter

    res = a.update_progress(CFG, dict(RECORD, ids={"anidb": "99"}), 50.0, instance_id="default", instance_label="Default")

    assert res.ok is False
    assert res.error_code == "missing_ids"
    assert calls == []


# --- rate limiting is the whole point of this adapter -------------------------

def test_writes_use_no_wait_so_the_ui_never_blocks(adapter) -> None:
    a, calls = adapter

    a.update_progress(CFG, RECORD, 25.0, instance_id="default", instance_label="Default")
    a.mark_watched(CFG, RECORD, instance_id="default", instance_label="Default")
    a.remove_progress(CFG, RECORD, instance_id="default", instance_label="Default")

    assert all(c.get("no_wait") is True for c in calls), "playback UI calls must not sleep on the governor"


def test_exhausted_playback_budget_returns_retryable_instead_of_hanging(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync.punchplay import _common as c
    from services.playback_progress.adapters.punchplay import PunchPlayPlaybackAdapter

    slept: list[float] = []
    monkeypatch.setattr(c.time, "sleep", lambda s: slept.append(s))

    gov = c.rate_governor("default", CFG["punchplay"])
    limit, _window = c.RATE_BUDGETS["playback"]
    url = c.URL_PLAYBACK.format(action="progress")
    for _ in range(limit):
        assert gov.try_acquire("POST", url) == 0.0

    a = PunchPlayPlaybackAdapter()
    res = a.update_progress(CFG, RECORD, 50.0, instance_id="default", instance_label="Default")

    assert res.ok is False
    assert res.error_code == "rate_limited"
    assert res.retryable is True
    assert res.remote_status == 429
    assert "retry in" in res.message
    assert slept == [], "must fail fast, not sleep"


def test_playback_budget_matches_the_documented_limit() -> None:
    from providers.sync.punchplay._common import RATE_BUDGETS

    assert RATE_BUDGETS["playback"] == (120, 300.0)


def test_list_progress_builds_records_from_the_index(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync import _mod_PUNCHPLAY as mod
    from services.playback_progress.adapters.punchplay import PunchPlayPlaybackAdapter

    index = {
        "tmdb:69478#s06e02": {
            "type": "episode",
            "show_ids": {"tmdb": "69478"},
            "ids": {"tmdb": "5978363"},
            "season": 6,
            "episode": 2,
            "series_title": "The Handmaid's Tale",
            "title": "Exile",
            "progress_ms": 753000,
            "duration_ms": 3307930,
            "progress_percent": 22.764,
            "progress_at": "2026-07-31T19:43:27.000Z",
            "_punchplay_progress_id": "7",
        }
    }
    monkeypatch.setattr(mod.OPS, "build_index", lambda cfg, *, feature: dict(index))

    res = PunchPlayPlaybackAdapter().list_progress(CFG, instance_id="default", instance_label="Default")

    assert res.ok is True
    assert len(res.items) == 1
    rec = res.items[0]
    assert rec.remote_id == "7"
    assert rec.media_type == "episode"
    assert rec.season == 6 and rec.episode == 2
    assert rec.progress_percent is not None and abs(rec.progress_percent - 22.764) < 0.01
    assert rec.duration_seconds == 3307


def test_list_progress_enriches_missing_episode_title_from_tmdb(monkeypatch: pytest.MonkeyPatch) -> None:
    from providers.sync import _mod_PUNCHPLAY as mod
    from services.playback_progress.adapters import punchplay as punchplay_adapter
    from services.playback_progress.adapters.punchplay import PunchPlayPlaybackAdapter

    class _Tmdb:
        def fetch(self, *, entity: str, ids: dict[str, str], need: dict[str, bool]) -> dict[str, Any]:
            assert entity == "tv"
            assert ids == {"tmdb": "69478"}
            return {
                "title": "The Handmaid's Tale",
                "year": 2017,
                "images": {
                    "poster": [{"url": "https://img.example/poster.jpg"}],
                    "backdrop": [{"url": "https://img.example/backdrop.jpg"}],
                },
            }

    index = {
        "tmdb:69478#s06e02": {
            "type": "episode",
            "show_ids": {"tmdb": "69478"},
            "ids": {"tmdb": "5978363"},
            "season": 6,
            "episode": 2,
            "progress_ms": 753000,
            "duration_ms": 3307930,
            "progress_percent": 22.764,
            "_punchplay_progress_id": "7",
        }
    }
    monkeypatch.setattr(mod.OPS, "build_index", lambda cfg, *, feature: dict(index))
    monkeypatch.setattr(punchplay_adapter, "tmdb_metadata_provider", lambda cfg: _Tmdb())

    res = PunchPlayPlaybackAdapter().list_progress(CFG, instance_id="default", instance_label="Default")

    rec = res.items[0]
    assert rec.title == "The Handmaid's Tale"
    assert rec.series_title == "The Handmaid's Tale"
    assert rec.poster_url == "https://img.example/poster.jpg"
    assert rec.backdrop_url == "https://img.example/backdrop.jpg"


def test_playback_ui_list_includes_punchplay() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    js = (root / "assets" / "js" / "playback_progress.js").read_text(encoding="utf-8")
    keys = js.split("PLAYBACK_PROVIDER_KEYS")[1].split("]")[0]
    assert '"punchplay"' in keys

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

import pytest

from cw_platform.anime_mapping import overrides as ov
from cw_platform.anime_mapping import storage
from providers.scrobble.anime_mapping import maybe_enrich_event_for_sink
from providers.scrobble import anime_mapping as watcher_anime
from providers.scrobble.crosswatch.sink import CrossWatchSink
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent
from providers.scrobble.simkl import sink as simkl_sink


MAPPINGS: dict[str, Any] = {
    "tvdb_show:81472:s2": {
        "anilist:813": {"1-35": "40-74"},
        "mal:813": {"1-35": "40-74"},
    },
}


@pytest.fixture()
def index(config_base: Path) -> Path:
    paths = storage.paths("v3")
    paths["root"].mkdir(parents=True, exist_ok=True)
    paths["mappings"].write_text(json.dumps(MAPPINGS), encoding="utf-8")
    storage.rebuild_sqlite_from_mappings(release_tag="v3")
    return paths["db"]


def _cfg(*, enabled: bool = True, route: bool = True) -> dict[str, Any]:
    return {
        "anime_mapping": {"enabled": enabled, "release_tag": "v3"},
        "scrobble": {"watch": {"route_options": {"watch": {"anime_mapping": route}}}},
    }


def _event(show_id: str = "81472", season: int = 2, episode: int = 13) -> ScrobbleEvent:
    return ScrobbleEvent(
        action="start",
        media_type="episode",
        ids={"tvdb_show": show_id, "tvdb": "999001"},
        title="Clean",
        year=1999,
        season=season,
        number=episode,
        progress=12,
        account="user",
        server_uuid="server",
        session_key="sess",
        raw={},
    )


def test_watcher_anime_mapping_is_gated(index: Path) -> None:
    ev = _event()

    assert maybe_enrich_event_for_sink(ev, "simkl", _cfg(route=False)) is ev
    assert maybe_enrich_event_for_sink(ev, "trakt", _cfg(route=True)) is ev
    assert maybe_enrich_event_for_sink(ev, "simkl", _cfg(enabled=False, route=True)) is ev


def test_watcher_anime_mapping_adds_show_ids_and_absolute(index: Path) -> None:
    mapped = maybe_enrich_event_for_sink(_event(), "simkl", _cfg())

    assert mapped.ids["tvdb_show"] == "81472"
    assert mapped.ids["mal_show"] == "813"
    assert mapped.ids["anilist_show"] == "813"
    assert mapped.raw["_cw_anime_map"]["absolute"] == 52
    assert mapped.raw["_cw_anime_map"]["namespace"] == "mal"
    assert mapped.raw["_simkl_episode_number"] == 52
    assert mapped.raw["simkl_bucket"] == "anime"


def test_watcher_anime_mapping_is_cached(index: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}
    real = watcher_anime.resolve_absolute

    def wrapped(*args: Any, **kwargs: Any):
        calls["count"] += 1
        return real(*args, **kwargs)

    watcher_anime._CACHE.clear()
    monkeypatch.setattr(watcher_anime, "resolve_absolute", wrapped)

    first = maybe_enrich_event_for_sink(_event(), "simkl", _cfg())
    second = maybe_enrich_event_for_sink(_event(), "simkl", _cfg())

    assert first.ids == second.ids
    assert first.raw["_simkl_episode_number"] == second.raw["_simkl_episode_number"]
    assert calls["count"] == 1


def test_simkl_episode_is_not_enriched_when_only_ids_map(config_base: Path) -> None:
    ov.upsert_override(
        {
            "media_type": "show",
            "match_provider": "tvdb",
            "match_id": "404404",
            "target_namespace": "mal",
            "target_id": "777",
        }
    )
    ev = _event(show_id="404404", season=9, episode=9)

    assert maybe_enrich_event_for_sink(ev, "simkl", _cfg()) is ev


def test_watcher_anime_mapping_caches_noop_results(config_base: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    calls = {"count": 0}

    def unresolved(*_args: Any, **_kwargs: Any):
        calls["count"] += 1
        return None

    with watcher_anime._CACHE_LOCK:
        watcher_anime._CACHE.clear()
    monkeypatch.setattr(watcher_anime, "resolve_absolute", unresolved)
    ev = _event(show_id="404404", season=1, episode=1)

    assert maybe_enrich_event_for_sink(ev, "simkl", _cfg()) is ev
    assert maybe_enrich_event_for_sink(ev, "simkl", _cfg()) is ev
    assert calls["count"] == 1


def test_crosswatch_can_receive_id_only_mapping(config_base: Path) -> None:
    ov.upsert_override(
        {
            "media_type": "show",
            "match_provider": "tvdb",
            "match_id": "404404",
            "target_namespace": "mal",
            "target_id": "777",
        }
    )

    mapped = maybe_enrich_event_for_sink(_event(show_id="404404", season=9, episode=9), "crosswatch", _cfg())

    assert mapped.ids["mal_show"] == "777"
    assert "_cw_anime_map" not in mapped.raw


def test_watcher_anime_mapping_uses_custom_episode_overrides(config_base: Path) -> None:
    ov.upsert_override(
        {
            "media_type": "show",
            "title": "Custom",
            "match_provider": "tvdb",
            "match_id": "404404",
            "match_season": 1,
            "target_namespace": "simkl",
            "target_id": "41487",
            "episode_from": 1,
            "episode_to": None,
            "episode_start_at": 100,
        }
    )

    mapped = maybe_enrich_event_for_sink(_event(show_id="404404", season=1, episode=7), "simkl", _cfg())

    assert mapped.ids["simkl_show"] == "41487"
    assert mapped.raw["_cw_anime_map"]["absolute"] == 106
    assert mapped.raw["_cw_anime_map"]["entry"].startswith("override:")


def test_dispatcher_enriches_only_supported_sink(index: Path) -> None:
    seen: list[ScrobbleEvent] = []

    class SimklSink:
        def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
            seen.append(event)

    class TraktSink:
        def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
            seen.append(event)

    Dispatcher([SimklSink(), TraktSink()], cfg_provider=lambda: _cfg()).dispatch(_event())

    assert "mal_show" in seen[0].ids
    assert "mal_show" not in seen[1].ids


def test_webhook_dispatcher_enriches_supported_sink(monkeypatch: pytest.MonkeyPatch, index: Path) -> None:
    import providers.webhooks.dispatch as dispatch

    seen: list[tuple[str, ScrobbleEvent, dict[str, Any]]] = []

    class Sink:
        def __init__(self, sink: str) -> None:
            self.sink = sink

        def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
            seen.append((self.sink, event, cfg or {}))

    with watcher_anime._CACHE_LOCK:
        watcher_anime._CACHE.clear()
    monkeypatch.setattr(dispatch, "webhook_sinks", lambda cfg, provider, instance: ["simkl", "trakt"])
    monkeypatch.setattr(dispatch, "webhook_sink_instance", lambda settings, sink: "default")
    monkeypatch.setattr(dispatch, "sink_configured", lambda cfg, sink, instance: True)
    monkeypatch.setattr(dispatch, "_make_sink", lambda sink, instance, cfg_provider: Sink(sink))

    cfg = {
        "anime_mapping": {"enabled": True, "release_tag": "v3"},
        "scrobble": {"webhook": {"anime_mapping_simkl": True, "sinks": ["simkl", "trakt"]}},
    }

    res = dispatch.dispatch_scrobble(
        "plex",
        "/scrobble/start",
        media_type="episode",
        ids={"tvdb_show": "81472"},
        title="Clean",
        season=2,
        episode=13,
        progress=12,
        cfg=cfg,
    )

    assert res.status_code == 200
    assert seen[0][0] == "simkl"
    assert seen[0][1].ids["mal_show"] == "813"
    assert seen[0][2]["scrobble"]["watch"]["route_options"]["watch"]["anime_mapping"] is True
    assert seen[1][0] == "trakt"
    assert "mal_show" not in seen[1][1].ids


def test_simkl_scrobble_prefers_mapped_anime_absolute() -> None:
    ev = ScrobbleEvent(
        action="start",
        media_type="episode",
        ids={"tvdb_show": "81472", "tmdb_show": "12971", "mal_show": "813"},
        title="Clean",
        year=1999,
        season=2,
        number=13,
        progress=12,
        account="user",
        server_uuid="server",
        session_key="sess",
        raw={"_simkl_episode_number": 52, "_cw_anime_map": {"namespace": "mal", "target_id": "813", "absolute": 52}},
    )

    bodies = simkl_sink._bodies(ev, 12)

    assert bodies[0]["anime"]["ids"] == {"mal": "813"}
    assert bodies[0]["episode"] == {"number": 52}
    assert bodies[1]["episode"] == {"season": 2, "number": 13}


def test_crosswatch_sink_preserves_watcher_anime_identity(monkeypatch, tmp_path: Path) -> None:
    captured: list[dict[str, Any]] = []

    class Ops:
        def is_configured(self, _cfg: dict[str, Any]) -> bool:
            return True

        def add(self, _cfg: dict[str, Any], items, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
            captured.extend(items)
            return {"ok": True, "count": len(items)}

        def remove(self, *_args, **_kwargs) -> dict[str, Any]:
            return {"ok": True, "count": 1}

    monkeypatch.setattr("providers.scrobble.crosswatch.sink.CROSSWATCH_OPS", Ops())
    monkeypatch.setattr("providers.scrobble.crosswatch.sink._tmdb_enrich", lambda *_args, **_kwargs: {})

    ev = ScrobbleEvent(
        action="start",
        media_type="episode",
        ids={"tvdb_show": "81472", "mal_show": "813", "anilist_show": "813"},
        title="Clean",
        year=1999,
        season=2,
        number=13,
        progress=12,
        account="user",
        server_uuid="server",
        session_key="sess",
        raw={"_cw_anime_map": {"absolute": 52}, "_simkl_episode_number": 52, "simkl_bucket": "anime"},
    )
    cfg = {"crosswatch": {"root_dir": str(tmp_path)}, "scrobble": {"trakt": {"progress_step": 5}}}

    CrossWatchSink(cfg_provider=lambda: cfg).send(ev)

    assert captured[0]["show_ids"]["mal"] == "813"
    assert captured[0]["show_ids"]["anilist"] == "813"
    assert captured[0]["_cw_anime_map"]["absolute"] == 52
    assert captured[0]["_simkl_episode_number"] == 52
    assert captured[0]["simkl_bucket"] == "anime"

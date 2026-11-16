from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any

import pytest

from api.scrobblerManagementAPI import _normalize_filters
from providers.scrobble.emby.watch import EmbyWatchService
from providers.scrobble.jellyfin.watch import JellyfinWatchService
from providers.scrobble.plex.watch import WatchService as PlexWatchService
from providers.scrobble.routes import normalize_route
from providers.scrobble.media_filters import event_ignore_reason, media_filter_ignore_reason
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent


def _event(raw: dict[str, Any], *, title: str = "Soldier's Bones") -> ScrobbleEvent:
    return ScrobbleEvent(
        action="start",
        media_type="movie",
        ids={"tmdb": "1638209"},
        title=title,
        year=2026,
        season=None,
        number=None,
        progress=1,
        account="user",
        server_uuid="server",
        session_key="sess",
        raw=raw,
    )


def test_agregarr_edition_filename_is_ignored() -> None:
    raw = {
        "Metadata": {
            "Media": [
                {
                    "Part": [
                        {
                            "file": "/data/media/placeholder/movies/Soldier's Bones (2026)/Soldier's Bones (2026) {tmdb-1638209} {edition-Trailer}.mp4"
                        }
                    ]
                }
            ]
        }
    }

    reason = media_filter_ignore_reason({"ignore_agregarr_trailers": True}, raw)

    assert reason == "filename_pattern:{edition-trailer}"


def test_title_with_trailer_word_is_not_ignored_without_media_path_or_edition() -> None:
    raw = {"Metadata": {"title": "Trailer Park Boys", "type": "movie"}}

    assert media_filter_ignore_reason({"ignore_agregarr_trailers": True}, raw) is None


def test_custom_path_prefix_is_ignored() -> None:
    raw = {"NowPlayingItem": {"Path": r"Z:\data\media\placeholder\movies\Movie\file.mp4"}}

    reason = media_filter_ignore_reason({"ignored_path_prefixes": [r"Z:\data\media\placeholder"]}, raw)

    assert reason == "path_prefix:z:/data/media/placeholder"


def test_jellyfin_emby_now_playing_path_is_ignored() -> None:
    raw = {"NowPlayingItem": {"Path": "/data/media/placeholder/movies/The Odyssey (2026)/The Odyssey.mp4"}}

    reason = media_filter_ignore_reason({"ignored_path_prefixes": ["/data/media/placeholder"]}, raw)

    assert reason == "path_prefix:/data/media/placeholder"


def test_kodi_item_file_is_ignored() -> None:
    raw = {"provider": "kodi", "item": {"file": "/data/media/placeholder/movies/The Odyssey (2026)/The Odyssey {edition-Trailer}.mp4"}}

    reason = media_filter_ignore_reason({"ignore_agregarr_trailers": True}, raw)

    assert reason == "filename_pattern:{edition-trailer}"


def test_enriched_plex_file_path_list_is_ignored() -> None:
    raw = {"_cw_file_paths": ["/media/The Odyssey (2026)/The Odyssey (2026) {edition-Trailer}.mp4"]}

    reason = media_filter_ignore_reason({"ignore_agregarr_trailers": True}, raw)

    assert reason == "filename_pattern:{edition-trailer}"


def test_enriched_plex_edition_title_is_ignored() -> None:
    raw = {"_cw_edition_title": "Trailer"}

    reason = media_filter_ignore_reason({"ignore_agregarr_trailers": True}, raw)

    assert reason == "edition:trailer"


def test_agregarr_marker_file_is_ignored(tmp_path) -> None:
    movie_dir = tmp_path / "movies" / "Movie (2026)"
    movie_dir.mkdir(parents=True)
    (movie_dir / ".comingsoon").write_text("", encoding="utf-8")
    raw = {"file": str(movie_dir / "Movie (2026).mp4")}

    reason = media_filter_ignore_reason({"ignore_agregarr_trailers": True}, raw)

    assert reason == "marker_file:.comingsoon"


def test_dispatcher_filters_ignored_media() -> None:
    calls: list[ScrobbleEvent] = []

    class Sink:
        def send(self, event: ScrobbleEvent) -> None:
            calls.append(event)

    cfg = {"scrobble": {"watch": {"filters": {"ignore_agregarr_trailers": True}}}}
    dispatcher = Dispatcher([Sink()], cfg_provider=lambda: cfg)

    assert dispatcher.dispatch(_event({"file": "/media/Movie (2026) {edition-Trailer}.mp4"})) is False
    assert calls == []


def test_webhook_dispatch_reports_ignored_without_sending(monkeypatch) -> None:
    import providers.webhooks.dispatch as dispatch

    calls: list[ScrobbleEvent] = []

    class Sink:
        def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
            calls.append(event)

    monkeypatch.setattr(dispatch, "webhook_sinks", lambda cfg, provider, instance: ["trakt"])
    monkeypatch.setattr(dispatch, "webhook_sink_instance", lambda settings, sink: "default")
    monkeypatch.setattr(dispatch, "sink_configured", lambda cfg, sink, instance: True)
    monkeypatch.setattr(dispatch, "_make_sink", lambda sink, instance, cfg_provider: Sink())

    cfg = {"scrobble": {"webhook": {"filters_plex": {"ignore_agregarr_trailers": True}}}}
    res = dispatch.dispatch_scrobble(
        "plex",
        "/scrobble/start",
        media_type="movie",
        ids={"tmdb": "1638209"},
        title="Soldier's Bones",
        raw={"file": "/media/Soldier's Bones (2026) {edition-Trailer}.mp4"},
        cfg=cfg,
    )
    payload = res.json()

    assert res.status_code == 200
    assert payload["activity_recorded"] is False
    assert payload["targets"][0]["ignored"] is True
    assert calls == []


def test_jellyfin_webhook_dispatch_uses_jellyfin_filters(monkeypatch) -> None:
    import providers.webhooks.dispatch as dispatch

    calls: list[ScrobbleEvent] = []

    class Sink:
        def send(self, event: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
            calls.append(event)

    monkeypatch.setattr(dispatch, "webhook_sinks", lambda cfg, provider, instance: ["simkl"])
    monkeypatch.setattr(dispatch, "webhook_sink_instance", lambda settings, sink: "default")
    monkeypatch.setattr(dispatch, "sink_configured", lambda cfg, sink, instance: True)
    monkeypatch.setattr(dispatch, "_make_sink", lambda sink, instance, cfg_provider: Sink())

    cfg = {"scrobble": {"webhook": {"filters_jellyfin": {"ignored_path_prefixes": ["/data/media/placeholder"]}}}}
    res = dispatch.dispatch_scrobble(
        "jellyfin",
        "/scrobble/start",
        media_type="movie",
        ids={"tmdb": "1368337"},
        title="The Odyssey",
        raw={"NowPlayingItem": {"Path": "/data/media/placeholder/movies/The Odyssey (2026)/The Odyssey.mp4"}},
        cfg=cfg,
    )
    payload = res.json()

    assert res.status_code == 200
    assert payload["activity_recorded"] is False
    assert payload["targets"][0]["ignored"] is True
    assert calls == []


def test_normalize_filters_accepts_media_ignore_fields() -> None:
    filters = _normalize_filters(
        {
            "ignore_agregarr_trailers": True,
            "ignored_path_prefixes": "/data/media/placeholder",
            "ignored_filename_patterns": ["{edition-Trailer}"],
            "ignored_editions": "Trailer",
            "ignored_marker_files": ".comingsoon",
        },
        "jellyfin",
    )

    assert filters == {
        "ignore_agregarr_trailers": True,
        "ignored_path_prefixes": ["/data/media/placeholder"],
        "ignored_filename_patterns": ["{edition-Trailer}"],
        "ignored_editions": ["Trailer"],
        "ignored_marker_files": [".comingsoon"],
    }


def _live_cfg(enabled: bool) -> dict[str, Any]:
    return {"scrobble": {"watch": {"filters": {"ignore_live_tv_dvr": enabled}}}}


@pytest.mark.parametrize("item_type", ["TvChannel", "Program"])
def test_jellyfin_emby_live_tv_is_ignored_when_enabled(item_type: str) -> None:
    ev = _event({"NowPlayingItem": {"Type": item_type, "Name": "BBC One"}})

    assert event_ignore_reason(ev, _live_cfg(True)) == "live_tv"
    assert event_ignore_reason(ev, _live_cfg(False)) is None
    assert event_ignore_reason(ev, {}) is None


@pytest.mark.parametrize("item_type", ["Movie", "Episode"])
def test_jellyfin_emby_movies_and_episodes_are_not_live_tv(item_type: str) -> None:
    ev = _event({"NowPlayingItem": {"Type": item_type, "Name": "Soldier's Bones"}})

    assert event_ignore_reason(ev, _live_cfg(True)) is None


@pytest.mark.parametrize("provider", ["plex", "jellyfin", "emby"])
def test_normalize_filters_keeps_live_tv_for_media_servers(provider: str) -> None:
    assert _normalize_filters({"ignore_live_tv_dvr": True}, provider) == {"ignore_live_tv_dvr": True}


def test_normalize_filters_drops_live_tv_for_other_sources() -> None:
    assert _normalize_filters({"ignore_live_tv_dvr": True}, "kodi") == {}


def test_plex_live_tv_guid_is_ignored_when_enabled() -> None:
    ev = _event({"Metadata": {"guid": "tv.plex.xmltv://episode/abc", "Guid": [{"id": "tmdb://1"}]}})

    assert event_ignore_reason(ev, _live_cfg(True)) == "live_tv"
    assert event_ignore_reason(ev, _live_cfg(False)) is None


def test_plex_library_guid_is_not_live_tv() -> None:
    ev = _event({"Metadata": {"guid": "plex://movie/5d776", "Guid": [{"id": "tmdb://1"}]}})

    assert event_ignore_reason(ev, _live_cfg(True)) is None


def test_live_tv_guid_text_outside_guid_fields_is_not_live_tv() -> None:
    ev = _event({
        "Metadata": {
            "guid": "plex://movie/5d776",
            "title": "tv.plex.xmltv",
            "summary": "recorded from tv.plex.xmltv",
            "Media": [{"Part": [{"file": "/data/tv.plex.xmltv/movie.mkv"}]}],
        },
        "NowPlayingItem": {"Type": "Movie", "Path": "/data/tv.plex.xmltv/movie.mkv"},
    })

    assert event_ignore_reason(ev, _live_cfg(True)) is None


def test_plex_manual_mark_keeps_live_tv_guid() -> None:
    watcher = object.__new__(PlexWatchService)
    watcher._event_with_session_identity = lambda ev, ident: ev
    watcher._enrich_event_with_plex = lambda ev: ev
    el = ET.Element("Video", {"type": "movie", "ratingKey": "42", "title": "Recorded", "guid": "tv.plex.xmltv://movie/abc"})

    ev = watcher._manual_mark_event(el, {}, "server")

    assert ev is not None
    assert event_ignore_reason(ev, _live_cfg(True)) == "live_tv"


@pytest.mark.parametrize("provider", ["plex", "jellyfin", "emby"])
def test_media_server_routes_ignore_live_tv_by_default(provider: str) -> None:
    route = normalize_route({"provider": provider, "sink": "trakt", "filters": {}}, "R1")

    assert route["filters"]["ignore_live_tv_dvr"] is True


def test_route_keeps_live_tv_opt_out() -> None:
    route = normalize_route({"provider": "plex", "sink": "trakt", "filters": {"ignore_live_tv_dvr": False}}, "R1")

    assert route["filters"]["ignore_live_tv_dvr"] is False


def test_live_tv_default_skips_other_sources_and_the_shared_watcher() -> None:
    assert "ignore_live_tv_dvr" not in normalize_route({"provider": "kodi", "sink": "trakt"}, "R1")["filters"]
    assert "ignore_live_tv_dvr" not in normalize_route({"provider": "jellyfin", "sink": ""}, "R1")["filters"]


@pytest.mark.parametrize("service", [JellyfinWatchService, EmbyWatchService])
def test_synthetic_stop_keeps_live_tv_type_for_mixed_routes(service: type) -> None:
    watcher = object.__new__(service)
    watcher._last_emit = {}
    watcher._poll = 10
    watcher._server_id = "server"
    sent: list[ScrobbleEvent] = []
    watcher._emit = lambda ev, _cfg: sent.append(ev)
    start = _event({"NowPlayingItem": {"Type": "TvChannel", "Name": "BBC One"}})
    memo = {"meta": watcher._meta_from_event(start), "p": 40, "ts": 0, "state": "playing"}

    watcher._emit_synthetic_stop("sess", memo, {}, 100.0, None)

    assert len(sent) == 1
    assert sent[0].raw["NowPlayingItem"] == {}
    assert event_ignore_reason(sent[0], _live_cfg(True)) == "live_tv"
    assert event_ignore_reason(sent[0], _live_cfg(False)) is None

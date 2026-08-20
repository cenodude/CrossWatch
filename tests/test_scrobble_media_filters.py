from __future__ import annotations

from typing import Any

from api.scrobblerManagementAPI import _normalize_filters
from providers.scrobble.media_filters import media_filter_ignore_reason
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

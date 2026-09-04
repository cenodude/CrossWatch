from __future__ import annotations

from types import SimpleNamespace
from typing import Any


class _Response:
    status_code = 200

    def __init__(self, payload: Any) -> None:
        self._payload = payload

    def json(self) -> Any:
        return self._payload


class _Http:
    def __init__(self, rows: list[dict[str, Any]]) -> None:
        self.rows = rows

    def get(self, path: str, params: dict[str, Any] | None = None) -> _Response:
        start = int((params or {}).get("StartIndex") or 0)
        if path.endswith("/Items") or path == "/Items":
            return _Response({"Items": self.rows if start == 0 else []})
        return _Response({"Items": []})


def test_emby_history_keys_episode_from_parent_show_ids(monkeypatch: Any) -> None:
    from providers.sync.emby import _history

    row = {
        "Id": "episode-3",
        "Type": "Episode",
        "Name": "Sleep Hypnosis",
        "SeriesName": "What We Do in the Shadows",
        "SeriesId": "series-wwdits",
        "ParentIndexNumber": 6,
        "IndexNumber": 3,
        "ProviderIds": {"Imdb": "tt33029958"},
        "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
    }
    adapter = SimpleNamespace(client=_Http([row]), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "_emby_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "prefetch_series_minimals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_minimal_from_episode", lambda *_args, **_kwargs: {"ids": {"tmdb": "83631", "imdb": "tt7908628"}})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "tmdb:83631#s06e03@1767312000" in out
    assert "imdb:tt33029958#s06e03@1767312000" not in out
    assert out["tmdb:83631#s06e03@1767312000"]["show_ids"] == {"tmdb": "83631", "imdb": "tt7908628"}
    assert out["tmdb:83631#s06e03@1767312000"]["ids"] == {"imdb": "tt33029958"}


def test_jellyfin_history_keys_episode_from_parent_show_ids(monkeypatch: Any) -> None:
    from providers.sync.jellyfin import _history

    row = {
        "Id": "episode-3",
        "Type": "Episode",
        "Name": "Sleep Hypnosis",
        "SeriesName": "What We Do in the Shadows",
        "SeriesId": "series-wwdits",
        "ParentIndexNumber": 6,
        "IndexNumber": 3,
        "ProviderIds": {"Imdb": "tt33029958"},
        "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
    }
    adapter = SimpleNamespace(client=_Http([row]), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "jf_get_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "_prefetch_series_meta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_ids_for", lambda *_args, **_kwargs: {"tmdb": "83631", "imdb": "tt7908628"})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "tmdb:83631#s06e03@1767312000" in out
    assert "imdb:tt33029958#s06e03@1767312000" not in out
    assert out["tmdb:83631#s06e03@1767312000"]["_cw_key"] == "tmdb:83631#s06e03"
    assert out["tmdb:83631#s06e03@1767312000"]["ids"] == {"imdb": "tt33029958"}


def test_emby_history_keeps_anime_id_when_enriched_movie_event_has_no_public_ids(monkeypatch: Any) -> None:
    from providers.sync.emby import _history

    row = {
        "Id": "anime-movie-1",
        "Type": "Movie",
        "Name": "Some Anime Movie",
        "ProductionYear": 2016,
        "ProviderIds": {"MyAnimeList": "32281"},
        "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
    }
    adapter = SimpleNamespace(client=_Http([row]), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "_emby_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "mal:32281@1767312000" in out
    assert "movie|title:some anime movie|year:2016@1767312000" not in out


def test_emby_history_no_id_episode_title_fallback_uses_original_row(monkeypatch: Any) -> None:
    from providers.sync.emby import _history

    rows = [
        {
            "Id": "episode-a",
            "Type": "Episode",
            "Name": "Ep One",
            "SeriesName": "Show A",
            "SeriesId": "series-a",
            "ProductionYear": 2020,
            "ParentIndexNumber": 6,
            "IndexNumber": 3,
            "ProviderIds": {},
            "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
        },
        {
            "Id": "episode-b",
            "Type": "Episode",
            "Name": "Ep Two",
            "SeriesName": "Show B",
            "SeriesId": "series-b",
            "ProductionYear": 2021,
            "ParentIndexNumber": 6,
            "IndexNumber": 3,
            "ProviderIds": {},
            "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
        },
    ]
    adapter = SimpleNamespace(client=_Http(rows), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "_emby_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "prefetch_series_minimals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_minimal_from_episode", lambda *_args, **_kwargs: {"ids": {}})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "episode|title:ep one|year:2020@1767312000" in out
    assert "episode|title:ep two|year:2021@1767312000" in out
    assert "episode|title:s06e03|year:@1767312000" not in out


def test_jellyfin_history_no_id_episode_title_fallback_uses_original_row(monkeypatch: Any) -> None:
    from providers.sync.jellyfin import _history

    rows = [
        {
            "Id": "episode-a",
            "Type": "Episode",
            "Name": "Ep One",
            "SeriesName": "Show A",
            "SeriesId": "series-a",
            "ProductionYear": 2020,
            "ParentIndexNumber": 6,
            "IndexNumber": 3,
            "ProviderIds": {},
            "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
        },
        {
            "Id": "episode-b",
            "Type": "Episode",
            "Name": "Ep Two",
            "SeriesName": "Show B",
            "SeriesId": "series-b",
            "ProductionYear": 2021,
            "ParentIndexNumber": 6,
            "IndexNumber": 3,
            "ProviderIds": {},
            "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
        },
    ]
    adapter = SimpleNamespace(client=_Http(rows), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "jf_get_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "_prefetch_series_meta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_ids_for", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "episode|title:ep one|year:2020@1767312000" in out
    assert "episode|title:ep two|year:2021@1767312000" in out
    assert "episode|title:s06e03|year:@1767312000" not in out

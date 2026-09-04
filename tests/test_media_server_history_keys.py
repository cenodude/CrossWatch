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
    monkeypatch.setattr(
        _history,
        "_series_minimal_from_episode",
        lambda _http, _uid, ep, _cache: {"ids": {}, "year": 2019 if ep.get("SeriesId") == "series-a" else 2018},
    )
    monkeypatch.setattr(_history, "_series_ids_via_item", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "show|title:show a|year:2019#s06e03@1767312000" in out
    assert "show|title:show b|year:2018#s06e03@1767312000" in out
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
    monkeypatch.setattr(
        _history,
        "_series_year_for",
        lambda _http, _uid, sid: 2019 if sid == "series-a" else 2018,
    )
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "show|title:show a|year:2019#s06e03@1767312000" in out
    assert "show|title:show b|year:2018#s06e03@1767312000" in out
    assert "episode|title:s06e03|year:@1767312000" not in out


def test_emby_history_never_keys_episode_on_its_own_imdb_id(monkeypatch: Any) -> None:
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
    monkeypatch.setattr(_history, "_series_minimal_from_episode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_ids_via_item", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "imdb:tt33029958#s06e03@1767312000" not in out
    assert "show|title:what we do in the shadows|year:#s06e03@1767312000" in out


def test_jellyfin_history_never_keys_episode_on_its_own_imdb_id(monkeypatch: Any) -> None:
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
    monkeypatch.setattr(_history, "_series_ids_for", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(_history, "_series_year_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "imdb:tt33029958#s06e03@1767312000" not in out
    assert "show|title:what we do in the shadows|year:#s06e03@1767312000" in out


def test_jellyfin_history_falls_back_to_parent_series_ids(monkeypatch: Any) -> None:
    from providers.sync.jellyfin import _history

    row = {
        "Id": "episode-3",
        "Type": "Episode",
        "Name": "Sleep Hypnosis",
        "SeriesName": "What We Do in the Shadows",
        "SeriesId": "",
        "ParentId": "series-wwdits",
        "ParentIndexNumber": 6,
        "IndexNumber": 3,
        "ProviderIds": {"Imdb": "tt33029958"},
        "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
    }
    adapter = SimpleNamespace(client=_Http([row]), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "jf_get_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "_prefetch_series_meta", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        _history,
        "_series_ids_for",
        lambda _http, _uid, sid: {"tmdb": "83631", "imdb": "tt7908628"} if sid == "series-wwdits" else {},
    )
    monkeypatch.setattr(_history, "_series_meta_type", lambda sid: "Series" if sid == "series-wwdits" else "")
    monkeypatch.setattr(_history, "_series_year_for", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "tmdb:83631#s06e03@1767312000" in out
    assert out["tmdb:83631#s06e03@1767312000"]["show_ids"] == {"tmdb": "83631", "imdb": "tt7908628"}


def test_emby_presence_episode_never_keys_on_its_own_imdb_id(monkeypatch: Any) -> None:
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
        "UserData": {"Played": True, "PlayCount": 1},
    }
    adapter = SimpleNamespace(client=_Http([row]), cfg=SimpleNamespace(user_id="user-1"))

    monkeypatch.setattr(_history, "_emby_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "prefetch_series_minimals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_minimal_from_episode", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_ids_via_item", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(_history, "_prefetch_played_ts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    assert "imdb:tt33029958#s06e03" not in out
    assert "show|title:what we do in the shadows|year:#s06e03" in out


def test_emby_presence_and_timed_paths_agree_on_the_fallback_key(monkeypatch: Any) -> None:
    from providers.sync.emby import _history

    def _row(iid: str, played_date: str | None) -> dict[str, Any]:
        ud: dict[str, Any] = {"Played": True, "PlayCount": 1}
        if played_date:
            ud["LastPlayedDate"] = played_date
        return {
            "Id": iid,
            "Type": "Episode",
            "Name": "Ep",
            "SeriesName": "Show A",
            "SeriesId": "series-a",
            "ParentIndexNumber": 6,
            "IndexNumber": 3,
            "ProviderIds": {"Imdb": "tt33029958"},
            "UserData": ud,
        }

    adapter = SimpleNamespace(
        client=_Http([_row("timed", "2026-01-02T00:00:00Z"), _row("untimed", None)]),
        cfg=SimpleNamespace(user_id="user-1"),
    )

    monkeypatch.setattr(_history, "_emby_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "prefetch_series_minimals", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_series_minimal_from_episode", lambda *_args, **_kwargs: {"ids": {}, "year": 2019})
    monkeypatch.setattr(_history, "_series_ids_via_item", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(_history, "_prefetch_played_ts", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})

    out = _history.build_index(adapter)

    bases = {k.split("@", 1)[0] for k in out}
    assert bases == {"show|title:show a|year:2019#s06e03"}


class _SeriesHttp:
    # Emby-shaped server: /Users/{uid}/Items?Ids= is the user scoped lookup,
    # /Items/{id} is the admin only one that non owner accounts cannot use.
    def __init__(self, rows: list[dict[str, Any]], pool: dict[str, dict[str, Any]], mode: str = "ok") -> None:
        self.rows = rows
        self.pool = pool
        self.mode = mode
        self.id_calls: list[str] = []
        self.admin_calls: list[str] = []

    def get(self, path: str, params: dict[str, Any] | None = None) -> _Response:
        params = params or {}
        if path.startswith("/Items/"):
            self.admin_calls.append(path)
            return _Response(None)
        if path.endswith("/Items") and "Ids" in params:
            ids = [i for i in str(params["Ids"]).split(",") if i]
            self.id_calls.append(str(params["Ids"]))
            if self.mode == "batch_fails" and len(ids) > 1:
                return _Response(None)
            if self.mode == "batch_omits" and len(ids) > 1:
                return _Response({"Items": []})
            return _Response({"Items": [self.pool[i] for i in ids if i in self.pool]})
        if path.endswith("/Items"):
            start = int(params.get("StartIndex") or 0)
            return _Response({"Items": self.rows if start == 0 else []})
        return _Response({"Items": []})


_SERIES_ROW = {
    "Id": "S1", "Type": "Series", "Name": "What We Do in the Shadows",
    "ProductionYear": 2019, "ProviderIds": {"Tmdb": "83631", "Imdb": "tt7908628"},
}
_SEASON_ROW = {
    "Id": "SE1", "Type": "Season", "Name": "Season 6",
    "SeriesId": "S1", "ParentId": "S1", "ProviderIds": {"Tvdb": "999999"},
}


def _episode_row(iid: str, number: int, **extra: Any) -> dict[str, Any]:
    row = {
        "Id": iid, "Type": "Episode", "Name": "Ep",
        "SeriesName": "What We Do in the Shadows",
        "ParentIndexNumber": 6, "IndexNumber": number,
        "ProviderIds": {"Imdb": f"tt3302995{number}", "Tvdb": f"1052128{number}"},
        "UserData": {"Played": True, "LastPlayedDate": "2026-01-02T00:00:00Z"},
    }
    row.update(extra)
    return row


def _quiet_index(monkeypatch: Any, http: Any) -> dict[str, Any]:
    from providers.sync.emby import _history

    monkeypatch.setattr(_history, "_emby_library_roots", lambda _adapter: {})
    monkeypatch.setattr(_history, "_prefetch_played_ts", lambda *_a, **_k: None)
    monkeypatch.setattr(_history, "_shadow_load", lambda: {})
    monkeypatch.setattr(_history, "_bb_load", lambda: {})
    adapter = SimpleNamespace(client=http, cfg=SimpleNamespace(user_id="user-1"))
    return _history.build_index(adapter)


def test_emby_resolves_show_ids_when_episode_row_has_no_series_id(monkeypatch: Any) -> None:
    # SeriesId absent, ParentId points at the Season: hop Season -> Series.
    http = _SeriesHttp([_episode_row("e1", 3, ParentId="SE1")], {"S1": _SERIES_ROW, "SE1": _SEASON_ROW})
    out = _quiet_index(monkeypatch, http)

    assert "tmdb:83631#s06e03@1767312000" in out
    assert out["tmdb:83631#s06e03@1767312000"]["show_ids"] == {"tmdb": "83631", "imdb": "tt7908628"}


def test_emby_never_takes_show_ids_from_a_season_row(monkeypatch: Any) -> None:
    # The Season carries its own TVDb id, which must never become the show id.
    orphan_season = {"Id": "SE9", "Type": "Season", "Name": "S6", "ProviderIds": {"Tvdb": "999999"}}
    http = _SeriesHttp([_episode_row("e1", 3, ParentId="SE9")], {"SE9": orphan_season})
    out = _quiet_index(monkeypatch, http)

    assert not any(k.startswith("tvdb:999999") for k in out)
    assert "show|title:what we do in the shadows|year:#s06e03@1767312000" in out


_SERIES_ROW_2 = {
    "Id": "S2", "Type": "Series", "Name": "Reacher",
    "ProductionYear": 2022, "ProviderIds": {"Tmdb": "108978", "Imdb": "tt9288030"},
}


def _two_show_rows() -> list[dict[str, Any]]:
    # Two distinct shows so the prefetch really sends a multi ID batch.
    a = _episode_row("e1", 3, SeriesId="S1")
    b = _episode_row("e2", 4, SeriesId="S2")
    b["SeriesName"] = "Reacher"
    return [a, b]


def test_emby_recovers_when_the_series_batch_call_fails(monkeypatch: Any) -> None:
    http = _SeriesHttp(_two_show_rows(), {"S1": _SERIES_ROW, "S2": _SERIES_ROW_2}, mode="batch_fails")
    out = _quiet_index(monkeypatch, http)

    assert {"tmdb:83631#s06e03@1767312000", "tmdb:108978#s06e04@1767312000"} <= set(out)


def test_emby_recovers_when_the_series_batch_omits_the_show(monkeypatch: Any) -> None:
    http = _SeriesHttp(_two_show_rows(), {"S1": _SERIES_ROW, "S2": _SERIES_ROW_2}, mode="batch_omits")
    out = _quiet_index(monkeypatch, http)

    assert {"tmdb:83631#s06e03@1767312000", "tmdb:108978#s06e04@1767312000"} <= set(out)


def test_emby_does_not_retry_an_unresolvable_show_per_episode(monkeypatch: Any) -> None:
    # A show with no provider IDs must cost one lookup, not one per episode.
    ghost = {"Id": "G1", "Type": "Series", "Name": "Ghost Show", "ProviderIds": {}}
    rows = [_episode_row(f"e{i}", i, SeriesId="G1") for i in range(1, 41)]
    http = _SeriesHttp(rows, {"G1": ghost})
    out = _quiet_index(monkeypatch, http)

    assert len(out) == 40
    assert len(http.id_calls) <= 2, http.id_calls
    assert all(k.startswith("show|title:") for k in out)

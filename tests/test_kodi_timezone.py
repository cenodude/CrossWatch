from __future__ import annotations

from types import SimpleNamespace


def test_kodi_lastplayed_round_trips_through_utc_using_local_timezone() -> None:
    from providers.sync.kodi._common import kodi_lastplayed_to_iso, watched_at_to_kodi

    raw = "2026-01-15 08:49:39"
    watched_at = kodi_lastplayed_to_iso(raw)

    assert watched_at is not None
    assert watched_at.endswith("Z")
    assert watched_at_to_kodi(watched_at) == raw


class _FakeKodiClient:
    def __init__(self) -> None:
        self.movies: list[tuple[int, dict]] = []

    def set_movie(self, movieid: int, payload: dict) -> None:
        self.movies.append((movieid, dict(payload)))

    def set_episode(self, episodeid: int, payload: dict) -> None:
        raise AssertionError("unexpected episode write")


def _rated_movie_index(userrating: int) -> object:
    from providers.sync.kodi._common import LibraryIndex

    return LibraryIndex(
        [
            {
                "movieid": 42,
                "title": "Arrival",
                "year": 2016,
                "uniqueid": {"tmdb": "329865"},
                "userrating": userrating,
            }
        ],
        [],
        [],
    )


def test_kodi_history_index_preserves_raw_lastplayed_and_timezone() -> None:
    from providers.sync.kodi import _history
    from providers.sync.kodi._common import KodiConfig, LibraryIndex, feature_index, kodi_lastplayed_to_iso

    raw = "2026-01-15 08:49:39"
    index = LibraryIndex(
        [
            {
                "movieid": 42,
                "title": "Arrival",
                "year": 2016,
                "uniqueid": {"tmdb": "329865"},
                "playcount": 1,
                "lastplayed": raw,
            }
        ],
        [],
        [],
    )
    client = _FakeKodiClient()
    adapter = SimpleNamespace(
        cfg=KodiConfig(server="http://kodi.local:8080"),
        client=client,
        _kodi_library_index_history=index,
    )

    rows = feature_index(adapter, "history")
    row = rows["tmdb:329865"]

    assert row["watched_at"] == kodi_lastplayed_to_iso(raw)
    assert row["kodi_lastplayed_raw"] == raw
    assert row["kodi_timezone"].startswith("UTC") or row["kodi_timezone"] == "local"
    assert row["watched_at_source"] == "kodi_lastplayed"

    result = _history.add(adapter, [row])

    assert result["ok"] is True
    assert client.movies == [(42, {"playcount": 1, "lastplayed": raw})]


def test_kodi_history_logs_unmappable_lastplayed(monkeypatch) -> None:
    from providers.sync.kodi import _common
    from providers.sync.kodi._common import KodiConfig, LibraryIndex, feature_index

    logs: list[tuple[str, str, str, dict]] = []
    monkeypatch.setattr(
        _common,
        "log",
        lambda feature, level, event, **fields: logs.append((feature, level, event, fields)),
    )
    index = LibraryIndex(
        [
            {
                "movieid": 42,
                "title": "Arrival",
                "year": 2016,
                "uniqueid": {"tmdb": "329865"},
                "playcount": 1,
                "lastplayed": "not-a-kodi-date",
            }
        ],
        [],
        [],
    )
    adapter = SimpleNamespace(
        cfg=KodiConfig(server="http://kodi.local:8080"),
        client=_FakeKodiClient(),
        _kodi_library_index_history=index,
    )

    rows = feature_index(adapter, "history")

    assert rows["tmdb:329865"]["watched_at"] is None
    events = [event for _feature, _level, event, _fields in logs]
    assert "timestamp_normalize_failed" in events
    assert "timestamp_missing_after_normalize" in events
    failed = next(fields for _feature, _level, event, fields in logs if event == "timestamp_normalize_failed")
    assert failed["raw_lastplayed"] == "not-a-kodi-date"
    assert failed["media_type"] == "movie"
    assert failed["kodi_id"] == 42


def test_kodi_history_logs_unmappable_write_timestamp(monkeypatch) -> None:
    from providers.sync.kodi import _common

    logs: list[tuple[str, str, str, dict]] = []
    monkeypatch.setattr(
        _common,
        "log",
        lambda feature, level, event, **fields: logs.append((feature, level, event, fields)),
    )

    result = _common.watched_at_to_kodi(
        "not-a-crosswatch-date",
        item={"_kodi_type": "movie", "_kodi_id": 42, "title": "Arrival", "year": 2016},
    )

    assert result == "not-a-crosswatch-date"
    assert len(logs) == 1
    feature, level, event, fields = logs[0]
    assert (feature, level, event) == ("history", "warning", "timestamp_write_normalize_failed")
    assert fields["raw_watched_at"] == "not-a-crosswatch-date"
    assert fields["media_type"] == "movie"
    assert fields["kodi_id"] == 42


def test_kodi_ratings_get_first_observed_timestamp(monkeypatch) -> None:
    from providers.sync.kodi import _common
    from providers.sync.kodi._common import KodiConfig, feature_index

    monkeypatch.setattr(_common, "_utc_now_iso", lambda: "2026-07-29T00:47:01Z")
    adapter = SimpleNamespace(
        cfg=KodiConfig(server="http://kodi.local:8080"),
        client=_FakeKodiClient(),
        _kodi_library_index_ratings=_rated_movie_index(7),
    )

    rows = feature_index(adapter, "ratings")
    row = rows["tmdb:329865"]

    assert row["rating"] == 7
    assert row["rated_at"] == "2026-07-29T00:47:01Z"
    assert row["rated_at_source"] == "kodi_first_observed"


def test_kodi_ratings_keep_prior_timestamp_when_rating_is_unchanged(monkeypatch) -> None:
    from providers.sync.kodi import _common
    from providers.sync.kodi._common import KodiConfig, feature_index

    monkeypatch.setattr(_common, "_utc_now_iso", lambda: "2026-07-29T00:47:01Z")
    adapter = SimpleNamespace(
        cfg=KodiConfig(server="http://kodi.local:8080"),
        client=_FakeKodiClient(),
        _kodi_library_index_ratings=_rated_movie_index(7),
        _kodi_ratings_baseline={
            "tmdb:329865": {
                "type": "movie",
                "ids": {"tmdb": "329865"},
                "rating": 7,
                "rated_at": "2026-07-28T10:00:00Z",
                "rated_at_source": "kodi_first_observed",
            }
        },
    )

    row = feature_index(adapter, "ratings")["tmdb:329865"]

    assert row["rated_at"] == "2026-07-28T10:00:00Z"
    assert row["rated_at_source"] == "kodi_first_observed"


def test_kodi_capture_mode_ignores_synthetic_timestamp_baseline(monkeypatch) -> None:
    from providers.sync.kodi import _common
    from providers.sync.kodi._common import KodiConfig, feature_index

    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setattr(_common, "_utc_now_iso", lambda: "2026-07-29T00:47:01Z")
    adapter = SimpleNamespace(
        cfg=KodiConfig(server="http://kodi.local:8080"),
        client=_FakeKodiClient(),
        _kodi_library_index_ratings=_rated_movie_index(7),
        _kodi_ratings_baseline={
            "tmdb:329865": {
                "type": "movie",
                "ids": {"tmdb": "329865"},
                "rating": 7,
                "rated_at": "2026-07-28T10:00:00Z",
                "rated_at_source": "kodi_first_observed",
            }
        },
    )

    row = feature_index(adapter, "ratings")["tmdb:329865"]

    assert row["rated_at"] == "2026-07-29T00:47:01Z"
    assert row["rated_at_source"] == "kodi_first_observed"


def test_kodi_ratings_update_timestamp_when_rating_changes(monkeypatch) -> None:
    from providers.sync.kodi import _common
    from providers.sync.kodi._common import KodiConfig, feature_index

    monkeypatch.setattr(_common, "_utc_now_iso", lambda: "2026-07-29T00:47:01Z")
    adapter = SimpleNamespace(
        cfg=KodiConfig(server="http://kodi.local:8080"),
        client=_FakeKodiClient(),
        _kodi_library_index_ratings=_rated_movie_index(8),
        _kodi_ratings_baseline={
            "tmdb:329865": {
                "type": "movie",
                "ids": {"tmdb": "329865"},
                "rating": 7,
                "rated_at": "2026-07-28T10:00:00Z",
                "rated_at_source": "kodi_first_observed",
            }
        },
    )

    row = feature_index(adapter, "ratings")["tmdb:329865"]

    assert row["rating"] == 8
    assert row["rated_at"] == "2026-07-29T00:47:01Z"
    assert row["rated_at_source"] == "kodi_rating_changed"

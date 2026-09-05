from __future__ import annotations

import time
from typing import Any

from providers.sync.simkl import _history


def _state(misses: dict[str, int] | None = None, resolved: dict[str, str] | None = None) -> Any:
    return _history._AnimeResolveState(resolved=dict(resolved or {}), misses=dict(misses or {}))


def _not_found_echo(tvdb: str) -> dict[str, Any]:
    return {
        "ids": {"tvdb": tvdb, "tmdb": "34307", "imdb": "tt1586680"},
        "title": "Shameless (US)",
        "seasons": [{"number": 1, "episodes": [{"number": 1}]}],
    }


def _confirms(tvdb: str, state: Any) -> bool:
    obj = _not_found_echo(tvdb)
    retry_ids = _history._response_show_retry_ids(obj)
    retry_tvdb = retry_ids.get("tvdb")
    return bool(
        _history._not_found_confirms_anime(obj)
        or (retry_tvdb and not _history._tvdb_known_non_anime(retry_tvdb, state))
    )


def test_known_non_anime_tvdb_is_not_confirmed_as_anime() -> None:
    state = _state(misses={"161511": int(time.time())})
    assert _history._tvdb_known_non_anime("161511", state) is True
    assert _confirms("161511", state) is False


def test_resolved_anime_tvdb_stays_confirmed() -> None:
    state = _state(resolved={"81797": "12345"})
    assert _history._tvdb_known_non_anime("81797", state) is False
    assert _confirms("81797", state) is True


def test_unprobed_tvdb_stays_confirmed() -> None:
    state = _state()
    assert _history._tvdb_known_non_anime("999999", state) is False
    assert _confirms("999999", state) is True


def test_expired_miss_stays_confirmed() -> None:
    stale = int(time.time()) - _history._ANIME_RESOLVE_MISS_TTL - 1
    state = _state(misses={"161511": stale})
    assert _history._tvdb_known_non_anime("161511", state) is False
    assert _confirms("161511", state) is True


def test_response_confirming_anime_wins_over_negative_cache() -> None:
    state = _state(misses={"161511": int(time.time())})
    obj = _not_found_echo("161511")
    obj["ids"]["anidb"] = "4563"
    assert _history._not_found_confirms_anime(obj) is True
    retry_ids = _history._response_show_retry_ids(obj)
    retry_tvdb = retry_ids.get("tvdb")
    assert bool(
        _history._not_found_confirms_anime(obj)
        or (retry_tvdb and not _history._tvdb_known_non_anime(retry_tvdb, state))
    ) is True


def test_missing_tvdb_and_missing_state_are_safe() -> None:
    assert _history._tvdb_known_non_anime("", _state(misses={"161511": int(time.time())})) is False
    assert _history._tvdb_known_non_anime(None, _state()) is False
    assert _history._tvdb_known_non_anime("161511", None) is False

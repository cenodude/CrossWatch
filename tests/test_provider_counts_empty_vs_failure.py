from __future__ import annotations

from typing import Any

import pytest

from api import syncAPI


@pytest.fixture(autouse=True)
def _reset_cache():
    before = dict(syncAPI._PROVIDER_COUNTS_CACHE)
    syncAPI._PROVIDER_COUNTS_CACHE["ts"] = 0.0
    syncAPI._PROVIDER_COUNTS_CACHE["data"] = None
    yield
    syncAPI._PROVIDER_COUNTS_CACHE.update(before)


def _stub_store(monkeypatch, counts: Any) -> None:
    class _Store:
        def __init__(self, _cfg):
            pass

        def provider_feature_counts(self, _feature):
            if isinstance(counts, Exception):
                raise counts
            return counts

    monkeypatch.setattr(
        "cw_platform.orchestrator._state_store.StateStore", _Store, raising=True
    )


def test_no_recorded_rows_is_empty_not_failure(monkeypatch) -> None:
    _stub_store(monkeypatch, {})

    assert syncAPI._provider_counts_from_db() == {}


def test_store_failure_still_reports_none(monkeypatch) -> None:
    _stub_store(monkeypatch, RuntimeError("db gone"))

    assert syncAPI._provider_counts_from_db() is None


def test_populated_rows_are_returned(monkeypatch) -> None:
    _stub_store(monkeypatch, {"plex": 12, "trakt": 9})

    out = syncAPI._provider_counts_from_db()

    assert out is not None
    assert out["PLEX"] == 12
    assert out["TRAKT"] == 9


def test_empty_result_falls_back_to_cache(monkeypatch) -> None:
    _stub_store(monkeypatch, {})
    syncAPI._PROVIDER_COUNTS_CACHE["data"] = {"PLEX": 7}

    assert syncAPI._provider_counts_current() == {"PLEX": 7}


def test_empty_result_does_not_overwrite_cached_counts(monkeypatch) -> None:
    _stub_store(monkeypatch, {})
    syncAPI._PROVIDER_COUNTS_CACHE["data"] = {"PLEX": 7}

    assert syncAPI._provider_counts_fast(force=True) == {"PLEX": 7}
    assert syncAPI._PROVIDER_COUNTS_CACHE["data"] == {"PLEX": 7}

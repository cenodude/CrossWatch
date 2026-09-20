from __future__ import annotations

from typing import Any
from types import SimpleNamespace

import pytest

from providers.sync._mod_PLEX import _progress_skipped_keys


def _row(status: str, key: str) -> dict[str, Any]:
    return {"status": status, "key": key, "provider": "plex", "remote_item_id": "1"}


def test_only_skipped_rows_are_collected() -> None:
    results = [
        _row("applied", "tmdb:1"),
        _row("skipped", "tmdb:2"),
        _row("skipped", "tmdb:3"),
        {"status": "unresolved", "reason": "not_found", "item": {}},
        _row("failed", "tmdb:4"),
    ]

    assert _progress_skipped_keys(results) == ["tmdb:2", "tmdb:3"]


def test_duplicates_and_keyless_rows_are_dropped() -> None:
    results = [_row("skipped", "tmdb:2"), _row("skipped", "tmdb:2"), _row("skipped", "")]

    assert _progress_skipped_keys(results) == ["tmdb:2"]


def test_non_progress_results_yield_nothing() -> None:
    assert _progress_skipped_keys([]) == []
    assert _progress_skipped_keys(None) == []


def test_issue_790_run_no_longer_reports_skipped_writes_as_added() -> None:
    from cw_platform.orchestrator._applier import _normalize

    attempted = [{"type": "movie", "ids": {"tmdb": str(n)}} for n in range(1, 13)]
    skipped_keys = [f"tmdb:{n}" for n in range(1, 11)]
    unresolved = [
        {"status": "unresolved", "reason": "not_found", "item": attempted[10]},
        {"status": "unresolved", "reason": "not_found", "item": attempted[11]},
    ]

    res = {
        "ok": True,
        "count": 0,
        "unresolved": unresolved,
        "confirmed_keys": [],
        "unresolved_keys": ["tmdb:11", "tmdb:12"],
        "skipped_keys": skipped_keys,
    }

    out = _normalize(
        res,
        attempted,
        "apply:add",
        dst="PLEX",
        feature="progress",
        emit=lambda *a, **k: None,
    )

    assert out["attempted"] == 12
    assert out["confirmed"] == 0
    assert out["skipped"] == 10
    assert out["unresolved"] == 2


@pytest.mark.parametrize("operation", ["add", "remove"])
@pytest.mark.parametrize("instance,destination,expected", [
    ("default", "P01", "default"),
    ("P01", "P01", "P01"),
    (None, "P01", "P01"),
    (None, None, "default"),
])
def test_progress_results_use_the_endpoint_instance(monkeypatch, operation, instance, destination, expected):
    from providers.sync.plex import _progress as progress

    monkeypatch.setenv("CW_PAIR_SRC", "PLEX" if instance is not None else "SIMKL")
    monkeypatch.setenv("CW_PAIR_DST", "PLEX")
    monkeypatch.setenv("CW_PAIR_SRC_INSTANCE", "default")
    if destination is None:
        monkeypatch.delenv("CW_PAIR_DST_INSTANCE", raising=False)
    else:
        monkeypatch.setenv("CW_PAIR_DST_INSTANCE", destination)
    writes = []
    obj = SimpleNamespace(viewOffset=0, duration=600000, markUnplayed=lambda: writes.append("remove"))
    server = SimpleNamespace(fetchItem=lambda key: obj, sessions=lambda: [])
    config = {} if instance is None else {"_cw_provider_instance": instance}
    adapter = SimpleNamespace(config=config, client=SimpleNamespace(server=server))
    monkeypatch.setattr(progress, "home_scope_enter", lambda adapter: (False, False, None, None))
    monkeypatch.setattr(progress, "home_scope_exit", lambda adapter, switched: None)
    monkeypatch.setattr(progress, "_resolve_rating_key", lambda adapter, item: "11")
    monkeypatch.setattr(progress, "_timeline_progress", lambda adapter, srv, key, ms, duration: writes.append("add"))
    item = {"type": "movie", "ids": {"tmdb": "1"}, "progress_ms": 60000, "duration_ms": 600000,
            "progress_at": "2026-09-20T12:00:00Z"}

    count, unresolved = getattr(progress, operation)(adapter, [item])

    assert count == 1 and not unresolved
    assert writes == [operation]
    assert len(adapter._progress_write_results) == 1
    assert adapter._progress_write_results[0]["provider_instance"] == expected

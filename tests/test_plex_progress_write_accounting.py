from __future__ import annotations

from typing import Any

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

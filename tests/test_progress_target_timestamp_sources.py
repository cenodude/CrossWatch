from __future__ import annotations

from typing import Any

from providers.sync import _progress_policy as policy


SOURCE_TS = "2026-08-04 16:39:44.925Z"
RECENT_METADATA_TS = "2026-08-19T06:10:02Z"
PROGRESS_MS = 1_800_000
RUNTIME_MS = 12_000_000


def _decide(**overrides: Any) -> policy.ProgressDecision:
    kwargs: dict[str, Any] = {
        "active_session": False,
        "source_timestamp": SOURCE_TS,
        "target_timestamp": None,
        "source_progress_ms": PROGRESS_MS,
        "source_duration_ms": RUNTIME_MS,
        "target_progress_ms": None,
        "target_duration_ms": RUNTIME_MS,
        "target_watched": False,
        "same_origin": False,
        "replay_enabled": False,
        "timestamp_tolerance_seconds": 30,
    }
    kwargs.update(overrides)
    return policy.decide_progress_write(**kwargs)


def test_never_played_target_accepts_the_write() -> None:
    decision = _decide()

    assert decision.apply is True
    assert decision.reason == "apply"


def test_a_metadata_timestamp_would_have_blocked_the_write() -> None:
    decision = _decide(target_timestamp=RECENT_METADATA_TS)

    assert decision.apply is False
    assert decision.reason == "target_newer"


class _PlexObj:
    type = "movie"
    ratingKey = "11"
    librarySectionID = "1"
    viewOffset = None
    viewCount = 0
    duration = RUNTIME_MS
    lastViewedAt = None
    viewedAt = None
    updatedAt = RECENT_METADATA_TS
    isWatched = False
    isPlayed = False


class _Srv:
    @staticmethod
    def fetchItem(rating_key):
        return _PlexObj()

    @staticmethod
    def sessions():
        return []


class _PlexAdapter:
    client = type("C", (), {"server": _Srv()})()


def _run_plex_add(monkeypatch):
    from providers.sync.plex import _progress as pr

    written: list[dict[str, Any]] = []
    monkeypatch.setattr(pr, "home_scope_enter", lambda _a: (False, False, None, None))
    monkeypatch.setattr(pr, "home_scope_exit", lambda _a, _b: None)
    monkeypatch.setattr(pr, "_resolve_rating_key", lambda _a, _i: "11")
    monkeypatch.setattr(pr, "_same_plex_endpoint", lambda: False)
    monkeypatch.setattr(pr, "_progress_write_options", lambda _a: (False, 30))
    monkeypatch.setattr(
        pr,
        "_timeline_progress",
        lambda adapter, srv, rk, ms, dur: written.append({"rk": rk, "ms": ms, "duration": dur}),
    )

    adapter = _PlexAdapter()
    item = {
        "type": "movie",
        "title": "The Godfather Part II",
        "ids": {"tmdb": "240"},
        "progress_at": SOURCE_TS,
        "progress_ms": PROGRESS_MS,
        "duration_ms": RUNTIME_MS,
    }
    applied, unresolved = pr.add(adapter, [item])
    return pr, adapter, applied, unresolved, written


def test_never_played_plex_item_gets_the_resume_position_written(monkeypatch) -> None:
    pr, adapter, applied, unresolved, written = _run_plex_add(monkeypatch)

    assert unresolved == []
    assert applied == 1
    assert written == [{"rk": "11", "ms": PROGRESS_MS, "duration": RUNTIME_MS}]

    results = getattr(adapter, "_progress_write_results", [])
    assert [row["status"] for row in results] == ["applied"]


def test_write_results_carry_the_canonical_key_for_accounting(monkeypatch) -> None:
    from providers.sync._mod_PLEX import _progress_skipped_keys

    pr, adapter, _applied, _unresolved, _written = _run_plex_add(monkeypatch)
    results = getattr(adapter, "_progress_write_results", [])

    assert results[0]["key"] == "tmdb:240"
    assert _progress_skipped_keys(results) == []

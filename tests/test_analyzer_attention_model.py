from __future__ import annotations

from types import SimpleNamespace

import services.analyzer as A


def _mismatch(provider, feature, key, targets, alias_keys, *, blocked=False):
    return {
        "provider": provider,
        "feature": feature,
        "key": key,
        "targets": list(targets),
        "alias_keys": list(alias_keys),
        "blocked": blocked,
    }


def _record(provider, feature, key, alias_keys, *, reason="simkl_not_found:episodes"):
    return {
        "provider": provider,
        "feature": feature,
        "key": key,
        "alias_keys": list(alias_keys),
        "ids": {},
        "item": {},
        "reason": reason,
    }


def test_live_mismatch_only_one_row():
    model = A._attention_model([_mismatch("TRAKT", "history", "tmdb:1", ["SIMKL"], ["tmdb:1"])], [])
    assert model["counts"] == {"current_mismatch": 1, "pending_retry": 0, "blocked": 0, "total": 1}
    row = model["rows"][0]
    assert row["current_mismatch"] and not row["unresolved"] and not row["blocked"]


def test_unresolved_only_one_pending_row():
    model = A._attention_model([], [_record("SIMKL", "history", "tmdb:1", ["tmdb:1"])])
    assert model["counts"]["pending_retry"] == 1
    assert model["counts"]["current_mismatch"] == 0
    row = model["rows"][0]
    assert row["unresolved"] and not row["current_mismatch"]


def test_same_item_in_both_is_one_row_with_both_states():
    mismatch = [_mismatch("TRAKT", "history", "tmdb:1", ["SIMKL"], ["tmdb:1", "imdb:tt1"])]
    records = [_record("SIMKL", "history", "tmdb:1", ["tmdb:1"])]
    model = A._attention_model(mismatch, records)
    assert model["counts"]["total"] == 1
    row = model["rows"][0]
    assert row["current_mismatch"] and row["unresolved"]
    assert model["counts"]["current_mismatch"] == 1
    assert model["counts"]["pending_retry"] == 1


def test_pending_count_matches_full_unresolved_subset():
    mismatch = [
        _mismatch("TRAKT", "history", "tmdb:1", ["SIMKL"], ["tmdb:1"]),
        _mismatch("TRAKT", "history", "tmdb:2", ["SIMKL"], ["tmdb:2"]),
    ]
    records = [
        _record("SIMKL", "history", "tmdb:2", ["tmdb:2"]),
        _record("SIMKL", "history", "tmdb:3", ["tmdb:3"]),
    ]
    model = A._attention_model(mismatch, records)
    pending_rows = [r for r in model["rows"] if r["unresolved"]]
    assert model["counts"]["pending_retry"] == len(pending_rows)
    assert model["counts"]["pending_retry"] == 2


def test_pair_filtering_applies_to_both_sources(monkeypatch):
    problems = [
        {
            "type": "missing_peer",
            "provider": "TRAKT",
            "feature": "history",
            "key": "tmdb:1",
            "targets": ["SIMKL"],
            "ids": {"tmdb": "1"},
            "item_type": "movie",
        }
    ]
    monkeypatch.setattr(
        A,
        "_unresolved_records",
        lambda allowed: [
            _record("SIMKL", "history", "tmdb:9", ["tmdb:9"]),
            _record("PLEX", "history", "tmdb:5", ["tmdb:5"]),
        ],
    )
    ctx = SimpleNamespace(pairs={("TRAKT", "history"): ["SIMKL"]})
    model = A._attention_from_analysis(problems, None, ctx)
    providers = {r["provider"] for r in model["rows"]}
    assert "PLEX" not in providers
    assert model["counts"]["current_mismatch"] == 1
    assert model["counts"]["pending_retry"] == 1


def test_confirmed_retry_clears_unresolved():
    before = A._attention_model([], [_record("SIMKL", "history", "tmdb:1", ["tmdb:1"])])
    assert before["counts"]["pending_retry"] == 1
    after = A._attention_model([], [])
    assert after["counts"]["pending_retry"] == 0


def test_read_back_removes_current_mismatch():
    before = A._attention_model([_mismatch("TRAKT", "history", "tmdb:1", ["SIMKL"], ["tmdb:1"])], [])
    assert before["counts"]["current_mismatch"] == 1
    after = A._attention_model([], [])
    assert after["counts"]["current_mismatch"] == 0


def test_provider_neutral():
    mismatch = [_mismatch("EMBY", "history", "imdb:tt7", ["JELLYFIN"], ["imdb:tt7"])]
    records = [_record("JELLYFIN", "history", "imdb:tt7", ["imdb:tt7"])]
    model = A._attention_model(mismatch, records)
    assert model["counts"]["total"] == 1
    row = model["rows"][0]
    assert row["current_mismatch"] and row["unresolved"]
    assert row["provider"] == "JELLYFIN"


def test_blocked_item_distinct_from_failed_write():
    mismatch = [_mismatch("TRAKT", "history", "tmdb:1", ["SIMKL"], ["tmdb:1"], blocked=True)]
    records = [_record("SIMKL", "history", "tmdb:2", ["tmdb:2"], reason="simkl_write_failed:http_500")]
    model = A._attention_model(mismatch, records)
    blocked_rows = [r for r in model["rows"] if r["blocked"]]
    pending_rows = [r for r in model["rows"] if r["unresolved"]]
    assert len(blocked_rows) == 1 and not blocked_rows[0]["current_mismatch"]
    assert len(pending_rows) == 1 and not pending_rows[0]["blocked"]
    assert model["counts"]["blocked"] == 1
    assert model["counts"]["pending_retry"] == 1

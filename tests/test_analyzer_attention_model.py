from __future__ import annotations

from types import SimpleNamespace

import pytest

import services.analyzer as A
from cw_platform.orchestrator._state_store import StateStore


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


def test_collection_pair_is_analyzed_like_other_features():
    item = {
        "type": "movie",
        "title": "Owned Movie",
        "ids": {"tmdb": "10"},
    }
    state = {
        "providers": {
            "PLEX": {"collection": {"baseline": {"items": {"tmdb:10": item}}}},
            "TRAKT": {"collection": {"baseline": {"items": {}}}},
        }
    }
    cfg = {
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "PLEX",
                "target": "TRAKT",
                "mode": "one-way",
                "features": {"collection": {"enable": True}},
            }
        ]
    }
    ctx = A._analysis_context(state, cfg)

    assert A._pair_map(cfg, state) == {("PLEX", "collection"): ["TRAKT"]}
    assert A._counts(state)["PLEX"]["collection"] == 1

    problems = A._problems(state, None, cfg=cfg, ctx=ctx, include_system=False, include_hints=False)
    missing = [p for p in problems if p.get("type") == "missing_peer"]

    assert len(missing) == 1
    assert missing[0]["feature"] == "collection"
    assert missing[0]["targets"] == ["TRAKT"]


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


def test_analyzer_main_db_edit_preserves_non_analyzer_features(tmp_path, monkeypatch):
    monkeypatch.setattr(A, "CONFIG_DIR", tmp_path)
    monkeypatch.setattr(A, "load_config", lambda: {})
    store = StateStore(tmp_path)
    store.save_state(
        {
            "providers": {
                "TRAKT": {
                    "history": {"baseline": {"items": {"tmdb:1": {"title": "Old", "ids": {"tmdb": "1"}}}}},
                    "playlists": {"baseline": {"items": {"list:1": {"title": "Keep"}}}},
                }
            }
        }
    )

    res = A.api_edit(
        {
            "provider": "TRAKT",
            "feature": "history",
            "key": "tmdb:1",
            "updates": {"title": "New"},
        }
    )

    loaded = store.load_state()
    assert res["ok"] is True
    assert loaded["providers"]["TRAKT"]["history"]["baseline"]["items"]["tmdb:1"]["title"] == "New"
    assert loaded["providers"]["TRAKT"]["playlists"]["baseline"]["items"]["list:1"]["title"] == "Keep"


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


def test_episode_with_show_fallback_id_does_not_report_missing_tmdb():
    item = {
        "type": "episode",
        "series_title": "Westworld",
        "season": 1,
        "episode": 1,
        "watched": True,
        "ids": {"imdb": "tt0475784"},
        "show_ids": {"imdb": "tt0475784"},
    }
    state = {
        "providers": {
            "STREMIO": {"history": {"baseline": {"items": {"imdb:tt0475784#s01e01": item}}}},
            "SIMKL": {"history": {"baseline": {"items": {"imdb:tt0475784#s01e01": item}}}},
        }
    }
    cfg = {
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "STREMIO",
                "target": "SIMKL",
                "features": {"history": {"enable": True}},
            }
        ]
    }
    ctx = A._analysis_context(state, cfg)

    problems = A._problems(state, None, cfg=cfg, ctx=ctx, include_system=False, include_hints=False)

    assert [p for p in problems if p.get("type") == "missing_ids"] == []


def test_movie_with_fallback_id_does_not_report_missing_tmdb():
    item = {
        "type": "movie",
        "title": "Example",
        "watched": True,
        "ids": {"imdb": "tt1234567"},
    }
    state = {
        "providers": {
            "STREMIO": {"history": {"baseline": {"items": {"imdb:tt1234567": item}}}},
            "SIMKL": {"history": {"baseline": {"items": {"imdb:tt1234567": item}}}},
        }
    }
    cfg = {
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "source": "STREMIO",
                "target": "SIMKL",
                "features": {"history": {"enable": True}},
            }
        ]
    }
    ctx = A._analysis_context(state, cfg)

    problems = A._problems(state, None, cfg=cfg, ctx=ctx, include_system=False, include_hints=False)

    assert [p for p in problems if p.get("type") == "missing_ids"] == []


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("extra_count", [7, 11, 50])
@pytest.mark.parametrize("extra_side", ["source", "target"])
def test_history_show_drift_only_reports_actionable_missing_items(mode, extra_count, extra_side):
    def episode(title, tmdb):
        return {
            "type": "episode",
            "series_title": title,
            "season": 1,
            "episode": 1,
            "watched_at": "2026-08-22T12:00:00Z",
            "show_ids": {"tmdb": str(tmdb)},
            "ids": {},
        }

    src_items = {f"tmdb:{idx}#s01e01": episode(f"Show {idx}", idx) for idx in range(81)}
    dst_items = dict(src_items)
    extra_items = {f"tmdb:{1000 + idx}#s01e01": episode(f"Extra {idx}", 1000 + idx) for idx in range(extra_count)}
    (src_items if extra_side == "source" else dst_items).update(extra_items)
    state = {
        "providers": {
            "STREMIO": {"history": {"baseline": {"items": src_items}}},
            "SIMKL": {"instances": {"SIMKL-P01": {"history": {"baseline": {"items": dst_items}}}}},
        }
    }
    cfg = {
        "pairs": [
            {
                "id": "p1",
                "enabled": True,
                "mode": mode,
                "source": "STREMIO",
                "target": "SIMKL",
                "target_instance": "SIMKL-P01",
                "features": {"history": {"enable": True}},
            }
        ]
    }

    problems = A._problems(state, cfg=cfg, include_system=False, include_hints=False)

    assert not [row for row in problems if row["type"] == "history_show_normalization"]
    missing = [row for row in problems if row["type"] == "missing_peer"]
    expected = set(extra_items) if extra_side == "source" or mode == "two-way" else set()
    assert {row["key"] for row in missing} == expected
    assert len(missing) == len(expected)


@pytest.mark.parametrize("pending", [False, True])
def test_provider_diagnostics_reach_pending_rows_and_snapshot_hints(monkeypatch, pending):
    item = {"type": "movie", "title": "Example", "ids": {"tmdb": "1"}}
    evidence = {"candidates": [{"title": "Candidate", "ids": {"tmdb": "2"}, "secret": "omit"}] * 12}
    rec = {"reason": "write_failed", "hint": "resolve_ambiguous", "match_details": evidence}
    monkeypatch.setattr(A, "_iter_unresolved_files", lambda scopes: iter([
        ("KODI", "history", "unresolved", pending, "saved.json", [("tmdb:1", item, rec)])
    ]))

    records = A._unresolved_records(None)
    model = A._attention_model(
        [_mismatch("TRAKT", "history", "tmdb:1", ["KODI"], ["tmdb:1"])], records
    )
    row = model["rows"][0]
    hints = A._missing_peer_hints(A._unresolved_index(None), "history", ["tmdb:1"], ["KODI"], False)

    for result in (records[0], row, hints[0]):
        assert result["reason"] == "write_failed"
        assert result["hint"] == "resolve_ambiguous"
        assert "Multiple possible destination matches" in result["hint_message"]
        assert result["match_details"]["candidate_count"] == 12
        assert result["match_details"]["truncated"] is True
        assert len(result["match_details"]["candidates"]) == 10
        assert "secret" not in result["match_details"]["candidates"][0]
    assert row["current_mismatch"] and row["unresolved"]


def test_unresolved_diagnostics_ignore_invalid_or_duplicate_hints():
    assert A._unresolved_diagnostics({"reason": "not_found", "hint": "not_found", "match_details": []}, "KODI", "history") == {}
    assert A._unresolved_diagnostics({"hint": "<invalid>"}, "KODI", "history") == {}


def test_unknown_provider_hint_is_retained_without_raw_display():
    result = A._unresolved_diagnostics({"reason": "write_failed", "hint": "new_provider_code"}, "KODI", "history")
    assert result["hint"] == "new_provider_code"
    assert result["hint_message"] == ""

from copy import deepcopy

import pytest

import services.analyzer as A
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope


@pytest.fixture
def watch_pair(config_base, monkeypatch):
    monkeypatch.setattr(A, "CONFIG_DIR", config_base)
    monkeypatch.setattr(A, "CWS_DIR", config_base / ".cw_state")
    for cache in ("_STATE_CACHE", "_SCOPED_ROWS_CACHE", "_ANALYSIS_CACHE"):
        monkeypatch.setattr(A, cache, {})
    cfg = {"pairs": [{"id": "cw-simkl", "source": "CROSSWATCH", "target": "SIMKL", "enabled": True,
                      "mode": "one-way", "features": {"history": {"enable": True, "rewatches": True}}}]}
    monkeypatch.setattr(A, "_cfg", lambda: cfg)
    source = dict(type="episode", series_title="Breaking Bad", season=2, episode=1,
                  ids={"tvdb": "438912", "tmdb": "972873"}, show_ids={"tmdb": "1396", "tvdb": "81189"},
                  watched_at="2026-09-04T18:12:30Z")
    peer = {**source, "ids": {"tvdb": "438912"}, "watched_at": "2026-09-04T18:07:36Z"}
    key = "tmdb:1396#s02e01@1788545550"
    peer_key = "tmdb:1396#s02e01@1788545256"
    state = {"providers": {name: {"history": {"baseline": {"items": items}}} for name, items in
                           (("CROSSWATCH", {key: source}), ("SIMKL", {peer_key: peer}))}}
    return cfg, state, key, peer_key


def save_pair(cfg, state):
    store = StateStore(A.CONFIG_DIR)
    scope = pair_feature_scope(cfg, cfg["pairs"][0], "history")
    store.for_pair(scope).save_feature_blocks({(name, "default", "history"): value["history"]
                                              for name, value in state["providers"].items()})


def test_watch_time_mismatch_stays_visible_in_analysis_attention_and_detail(watch_pair):
    cfg, state, key, peer_key = watch_pair
    before = deepcopy(state)
    save_pair(cfg, state)
    result = A._cached_analysis("cw-simkl", include_hints=True)
    problem = next(p for p in result["problems"] if p["type"] == "missing_peer")
    difference = problem["watch_time_differences"][0]
    assert difference["difference_seconds"] == 294
    assert difference["target_key"] == peer_key
    assert difference["source_watched_at"] == "2026-09-04T18:12:30Z"
    assert difference["target_watched_at"] == "2026-09-04T18:07:36Z"
    assert not problem.get("target_show_info")
    assert result["attention"]["counts"]["current_mismatch"] == 1
    assert result["attention"]["rows"][0]["watch_time_differences"] == [difference]
    detail = A._detail_for_item("cw-simkl", "CROSSWATCH", "history", key)
    assert detail["watch_time_differences"] == [difference]
    assert detail["targets"] == ["SIMKL"]
    assert state == before


@pytest.mark.parametrize("case", ["exact", "different_episode", "unread", "non_rewatch"])
def test_time_diagnosis_requires_an_unmatched_event_with_matching_identity(watch_pair, case):
    cfg, state, key, peer_key = watch_pair
    dst = state["providers"]["SIMKL"]["history"]["baseline"]["items"]
    if case == "exact":
        dst[peer_key]["watched_at"] = "2026-09-04T18:12:30.900Z"
    elif case == "different_episode":
        dst[peer_key]["episode"] = 2
    elif case == "unread":
        del state["providers"]["SIMKL"]
    else:
        cfg["pairs"][0]["features"]["history"]["rewatches"] = False
    save_pair(cfg, state)
    result = A._cached_analysis("cw-simkl")
    assert not any(p.get("watch_time_differences") for p in result["problems"])
    assert result["attention"]["counts"]["current_mismatch"] == int(case == "different_episode")


def test_closest_watch_is_context_not_confirmation(watch_pair):
    cfg, state, key, peer_key = watch_pair
    dst = state["providers"]["SIMKL"]["history"]["baseline"]["items"]
    dst["older"] = {**dst[peer_key], "watched_at": "2026-09-03T18:12:30Z"}
    save_pair(cfg, state)
    detail = A._detail_for_item("cw-simkl", "CROSSWATCH", "history", key)
    assert detail["watch_time_differences"][0]["candidate_count"] == 2
    assert detail["watch_time_differences"][0]["target_key"] == peer_key
    assert detail["targets"] == ["SIMKL"]


def test_retry_block_matches_exact_event_and_stays_in_its_scope(watch_pair, monkeypatch):
    cfg, state, key, peer_key = watch_pair
    item = state["providers"]["CROSSWATCH"]["history"]["baseline"]["items"][key]
    documents = {
        "SIMKL_history.unresolved.first.json": {key: {"item": item, "reason": "apply:add:no_confirmations_fallback"}},
        "SIMKL_history.blackbox.first.json": {key: {"since": 1}},
        "SIMKL_history.unresolved.second.json": {key: {"item": item}},
        "SIMKL_history.blackbox.second.json": {peer_key: {"since": 1}},
    }
    monkeypatch.setattr(A, "_read_cw_state", lambda scopes: {name: data for name, data in documents.items()
                                                          if any(name.endswith(f".{scope}.json") for scope in scopes)})
    first = A._unresolved_records({"first"})
    second = A._unresolved_records({"second"})
    assert first[0]["retry_blocked"] is True
    assert second[0]["retry_blocked"] is False
    model = A._attention_model([], first)
    assert model["rows"][0]["retry_blocked"] is True
    assert model["rows"][0]["reason"] == "apply:add:no_confirmations_fallback"
    assert "did not confirm" in model["rows"][0]["reason_message"]
    assert model["counts"]["pending_retry"] == 1
    index = A._unresolved_index({"first"})
    aliases = A._alias_keys({**item, "_key": key})
    exact = A._missing_peer_hints(index, "history", aliases, ["SIMKL"], False, key)
    other = A._missing_peer_hints(index, "history", aliases, ["SIMKL"], False, peer_key)
    assert any(h.get("kind") == "blackbox" for h in exact)
    assert not any(h.get("kind") == "blackbox" for h in other)

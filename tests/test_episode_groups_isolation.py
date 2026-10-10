# tests/test_episode_groups_isolation.py
# CrossWatch - Ordinary sync and scrobble isolation from episode groups
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.local_db import manual_policy
from cw_platform.orchestrator import _episode_groups as sync_groups
from providers.scrobble import episode_groups as scrobble_groups
from test_episode_group_scrobbles import event, setup
from test_episode_groups import group, watched


@pytest.mark.parametrize("feature", ["history", "watchlist", "ratings", "progress", "collection"])
@pytest.mark.parametrize("other_pair", [False, True])
def test_no_groups_for_pair_preserves_inputs_and_never_touches_group_state(monkeypatch, feature, other_pair):
    policy = dict(providers={}, pairs={"other": {"episode_groups": [group()]}} if other_pair else {})
    reads = []
    monkeypatch.setattr(sync_groups, "load_policy", lambda base: reads.append(base) or policy)
    def unexpected(*args, **kwargs):
        pytest.fail("Ordinary sync must not evaluate groups or access group observations")
    monkeypatch.setattr(sync_groups, "plan_group", unexpected)
    monkeypatch.setattr(sync_groups.storage, "load", unexpected)
    monkeypatch.setattr(sync_groups.storage, "save", unexpected)
    ctx = SimpleNamespace(state_store=SimpleNamespace(base_path="unused", pair_scope="scope", mapping_pair_id="p1"), emit=unexpected)
    items = {"original": watched(4)}
    changes = [watched(5)]
    before = deepcopy((items, changes, policy))
    handler = sync_groups.HistoryGroups(ctx, feature, [("SRC", "default"), ("DST", "default")], [items, {}])
    for side in (0, 1):
        assert handler.strip(side, items) is items
        assert handler.restore(side, items) is items
        assert handler.ordinary(side, changes) is changes
    assert handler.adds == [[], []]
    handler.commit([items, {}])
    assert (items, changes, policy) == before
    assert bool(reads) == (feature == "history")


@pytest.mark.parametrize("action", ["start", "pause", "stop"])
@pytest.mark.parametrize("case", ["no_groups", "opt_out", "other_pair", "unrelated_episode", "movie", "disabled_history"])
def test_unmapped_scrobbles_preserve_event_result_and_normal_sink_call(config_base, setup, monkeypatch, action, case):
    cfg, _ = setup
    original = event(action=action, progress=40 if action != "stop" else 95)
    policy = manual_policy.load_policy(config_base)
    if case == "no_groups":
        policy["pairs"]["p1"]["episode_groups"] = []
    elif case == "opt_out":
        policy["pairs"]["p1"]["episode_groups"][0]["scrobble"] = False
    elif case == "other_pair":
        policy["pairs"]["other"] = policy["pairs"].pop("p1")
    elif case == "unrelated_episode":
        original = event(4, action=action)
    elif case == "movie":
        original = event(action=action, media_type="movie")
    else:
        cfg["pairs"][0]["features"]["history"]["enable"] = False
    manual_policy.save_policy(config_base, policy)
    def unexpected(*args, **kwargs):
        pytest.fail("Ordinary scrobbles must not access group completion state")
    for method in ("load", "save", "load_scrobble", "save_scrobble"):
        monkeypatch.setattr(scrobble_groups.storage, method, unexpected)
    before = deepcopy(original)
    result = {"ok": True, "normal_sink_result": object()}
    calls = []
    assert scrobble_groups.send_grouped(original, cfg, lambda ev: calls.append(ev) or result) is result
    assert len(calls) == 1 and calls[0] is original
    assert original == before
    assert manual_policy.load_policy(config_base) == policy

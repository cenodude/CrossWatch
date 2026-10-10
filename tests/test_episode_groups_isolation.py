# tests/test_episode_groups_isolation.py
# CrossWatch - Ordinary sync and scrobble isolation from episode groups
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy
from types import SimpleNamespace

import pytest

from cw_platform.local_db import manual_policy
from cw_platform.orchestrator import _episode_groups as sync_groups
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

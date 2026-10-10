# tests/test_episode_groups.py
# CrossWatch - History episode group planning and persistence regressions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from copy import deepcopy

import pytest

from cw_platform.episode_groups import plan_group, validate_groups
from cw_platform.id_map import canonical_key
from cw_platform.local_db import manual_policy
from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan
from test_interactive_sync import setup_ops
from test_orchestrator_dry_run_no_side_effects import _cfg


def episode(number, **extra):
    return dict(type="episode", title="Example", show_ids={"tmdb": "100"}, season=1, episode=number, **extra)


def group():
    return dict(id="finale", name="Finale", scrobble=False, source=dict(provider="SRC", instance="default", episodes=[episode(2), episode(3)]),
                target=dict(provider="DST", instance="default", episodes=[episode(2)]))


def watched(number):
    return episode(number, watched_at=f"2026-10-0{number}T12:00:00Z")


def index(rows):
    return {canonical_key(row): row for row in rows}


def setup(config_base, monkeypatch, source, target, mode="two-way"):
    src, dst = setup_ops(config_base, monkeypatch, source, target)
    for ops in (src, dst):
        ops.features = lambda: {"history": True}
        ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present", "observed_deletes": True}
        ops.health = lambda *_a, **_kw: {"ok": True, "status": "ok", "features": {"history": True}}
    cfg = _cfg(False)
    cfg["sync"].update(enable_remove=True, include_observed_deletes=True)
    cfg["pairs"][0].update(mode=mode, feature="history", features={"history": {"enable": True, "add": True, "remove": True}})
    manual_policy.save_policy(config_base, dict(version=1, providers={}, pairs={"p1": dict(providers={}, episode_groups=[group()])}))
    return src, dst, cfg


def run(cfg, plan=None, dry=False):
    return Orchestrator(cfg, interactive=plan).run(dry_run=dry or bool(plan and plan.preview), pair_scope_ids=["p1"],
                                                   write_state_json=not bool(plan and plan.preview))


@pytest.mark.parametrize("parts,combined,expected", [([], False, (0, 0)), ([2], False, (0, 0)),
    ([3], False, (0, 0)), ([2, 3], False, (0, 1)), ([], True, (2, 0)), ([2], True, (1, 0)), ([2, 3], True, (0, 0))])
def test_group_truth_table(parts, combined, expected):
    adds, state, reason = plan_group(group(), {"source": index([watched(n) for n in parts]),
                                             "target": index([watched(2)] if combined else [])}, {})
    assert (len(adds["source"]), len(adds["target"])) == expected
    assert not reason and not state["held"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("parts", [[2], [3], [2, 3]])
def test_real_sync_groups_parts_without_duplicates(config_base, monkeypatch, mode, parts):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(n) for n in parts], [], mode)
    assert run(cfg)["ok"]
    assert set(dst.index) == (set(index([watched(2)])) if len(parts) == 2 else set())
    assert len(dst.add_calls) == (1 if len(parts) == 2 else 0)
    if dst.add_calls:
        assert dst.add_calls[0][0]["watched_at"] == watched(3)["watched_at"]
    assert run(cfg)["ok"]
    assert len(dst.add_calls) == (1 if len(parts) == 2 else 0)
    assert not src.add_calls


def test_reverse_and_partial_apply_survive_restart(config_base, monkeypatch):
    src, dst, cfg = setup(config_base, monkeypatch, [], [watched(2)])
    preview = InteractivePlan()
    assert run(cfg, preview)["ok"]
    assert len(preview.rows) == 2
    assert all(row["provider"] == "SRC" for row in preview.rows.values())
    assert all(row["item"]["_cw_episode_group"] == "Finale" for row in preview.rows.values())
    assert run(cfg, InteractivePlan(preview=False, selected={next(iter(preview.rows))}))["ok"]
    assert len(src.index) == 1
    assert run(cfg)["ok"]
    assert len(src.index) == 2
    assert run(cfg)["ok"]
    assert len(src.add_calls) == 2


def test_removal_is_held_across_repeated_runs(config_base, monkeypatch):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2), watched(3)], [watched(2)])
    assert run(cfg)["ok"]
    src.index.pop(canonical_key(watched(3)))
    for _ in range(3):
        assert run(cfg)["ok"]
    assert len(src.index) == len(dst.index) == 1
    assert not src.add_calls and not dst.add_calls
    review = InteractivePlan()
    assert run(cfg, review)["ok"]
    assert not review.rows
    assert any("marked unwatched" in notice["reason"] for notice in review.notices)


def test_later_completion_and_pair_isolation(config_base, monkeypatch):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2)], [])
    assert run(cfg)["ok"]
    src.index.update(index([watched(3)]))
    assert run(cfg)["ok"]
    assert len(dst.index) == 1
    cfg["pairs"].append({**deepcopy(cfg["pairs"][0]), "id": "p2"})
    review = InteractivePlan()
    assert Orchestrator(cfg, interactive=review).run(dry_run=True, pair_scope_ids=["p2"], write_state_json=False)["ok"]
    assert any(row["item"]["episode"] == 3 for row in review.rows.values())


def test_reordering_group_members_preserves_partial_watch_observations(config_base, monkeypatch):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2)], [])
    assert run(cfg)["ok"]
    policy = manual_policy.load_policy(config_base)
    policy["pairs"]["p1"]["episode_groups"][0]["source"]["episodes"].reverse()
    manual_policy.save_policy(config_base, policy)
    assert run(cfg)["ok"]
    src.index.update(index([watched(3)]))
    assert run(cfg)["ok"]
    assert set(dst.index) == set(index([watched(2)]))
    assert len(dst.add_calls) == 1


@pytest.mark.parametrize("bad", ["duplicate", "overlap", "one_to_one", "same_endpoint", "different_shows"])
def test_invalid_groups_rejected(bad):
    groups = [group()]
    if bad == "duplicate":
        groups[0]["source"]["episodes"].append(episode(2))
    elif bad == "overlap":
        groups.append({**group(), "id": "other"})
    elif bad == "one_to_one":
        groups[0]["source"]["episodes"].pop()
    elif bad == "different_shows":
        groups[0]["source"]["episodes"][1]["show_ids"] = {"tmdb": "200"}
    else:
        groups[0]["target"]["provider"] = "SRC"
    with pytest.raises(ValueError):
        validate_groups(groups)


def test_epoch_zero_is_a_watched_group_member():
    adds, state, reason = plan_group(group(), {"source": {}, "target": index([episode(2, watched_at="1970-01-01T00:00:00Z")])}, {})
    assert len(adds["source"]) == 2
    assert all(row["watched_at"] == "1970-01-01T00:00:00Z" for row in adds["source"])
    assert not reason and not state["held"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_shifted_later_episode_can_use_same_number_as_source_part(config_base, monkeypatch, mode):
    from api import editorAPI

    src, dst, cfg = setup(config_base, monkeypatch, [watched(2), watched(4)], [], mode)
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    original, corrected = watched(4), watched(3)
    editorAPI._save_policy_manual_batch([("history", "SRC", index([corrected]), [canonical_key(original)], "default")],
        pair_id="p1", mappings={("history", "SRC", "default"): {canonical_key(corrected): dict(
            original_key=canonical_key(original), original=original)}})
    assert run(cfg)["ok"]
    assert set(dst.index) == set(index([corrected]))
    assert not src.add_calls
    assert run(cfg)["ok"]
    assert len(dst.add_calls) == 1
    assert not src.add_calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_disabled_adds_and_dry_runs_do_not_write_group_state(config_base, monkeypatch, mode):
    from cw_platform.local_db.db import get_conn

    src, dst, cfg = setup(config_base, monkeypatch, [watched(2), watched(3)], [], mode)
    assert run(cfg, dry=True)["ok"]
    assert not dst.index
    assert get_conn(config_base).execute("SELECT COUNT(*) FROM episode_group_state").fetchone()[0] == 0
    cfg["pairs"][0]["features"]["history"]["add"] = False
    calls = len(dst.add_calls)
    assert run(cfg)["ok"]
    assert len(dst.add_calls) == calls


def test_failed_group_write_is_retried_without_marking_it_present(config_base, monkeypatch):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2), watched(3)], [])
    original = dst.add
    dst.add = lambda *_a, **_kw: {"ok": False, "count": 0, "errors": 1}
    run(cfg)
    assert not dst.index
    dst.add = original
    assert run(cfg)["ok"]
    assert len(dst.index) == len(dst.add_calls) == 1


def test_group_rename_does_not_release_removal_hold(config_base, monkeypatch):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2), watched(3)], [watched(2)])
    assert run(cfg)["ok"]
    dst.index.clear()
    assert run(cfg)["ok"]
    policy = manual_policy.load_policy(config_base)
    policy["pairs"]["p1"]["episode_groups"][0]["name"] = "Renamed finale"
    manual_policy.save_policy(config_base, policy)
    assert run(cfg)["ok"]
    assert not dst.index and not dst.add_calls


def test_group_timestamps_use_instants_and_do_not_copy_native_event_ids():
    source = [episode(2, watched_at="2026-10-03T01:00:00+02:00", provider_event_id="source-only"),
              episode(3, watched_at="2026-10-02T23:30:00Z", _trakt_history_id=123)]
    adds, _, _ = plan_group(group(), {"source": index(source), "target": {}}, {})
    item = adds["target"][0]
    assert item["watched_at"] == "2026-10-02T23:30:00Z"
    assert "provider_event_id" not in item and "_trakt_history_id" not in item


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("guard", ["down", "rewatches", "blocked", "specials"])
def test_group_respects_safety_gates(config_base, monkeypatch, mode, guard):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2), watched(3)], [], mode)
    if guard == "down":
        src.health = lambda *_a, **_kw: {"ok": False, "status": "down"}
    elif guard == "rewatches":
        for ops in (src, dst):
            ops.capabilities = lambda: {"features": {"history": True}, "index_semantics": "present",
                                        "history": {"rewatches": {"read": True, "write": True}}}
        cfg["pairs"][0]["features"]["history"]["rewatches"] = True
    else:
        policy = manual_policy.load_policy(config_base)
        if guard == "blocked":
            policy["pairs"]["p1"]["providers"] = {"SRC": {"history": {"blocks": [canonical_key(watched(3))]}}}
        else:
            value = policy["pairs"]["p1"]["episode_groups"][0]
            value["target"]["episodes"][0]["season"] = 0
            cfg["pairs"][0]["features"]["history"]["include_specials"] = False
        manual_policy.save_policy(config_base, policy)
    review = InteractivePlan()
    run(cfg, review)
    assert not review.rows
    assert not src.add_calls and not dst.add_calls


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_one_to_many_mapping_and_failed_part_retry(config_base, monkeypatch, mode):
    src, dst, cfg = setup(config_base, monkeypatch, [watched(2)], [], mode)
    policy = manual_policy.load_policy(config_base)
    value = policy["pairs"]["p1"]["episode_groups"][0]
    value["source"]["episodes"], value["target"]["episodes"] = value["target"]["episodes"], value["source"]["episodes"]
    manual_policy.save_policy(config_base, policy)
    original = dst.add
    def partial(config, items, **kwargs):
        items = list(items)
        original(config, items[:1], **kwargs)
        return {"ok": True, "count": 1, "confirmed_keys": [canonical_key(items[0])],
                "unresolved": [{"item": items[1], "hint": "temporary"}]}
    dst.add = partial
    run(cfg)
    assert len(dst.index) == 1
    dst.add = original
    assert run(cfg)["ok"]
    assert set(dst.index) == set(index([watched(2), watched(3)]))
    assert len(dst.add_calls) == 2


def test_unknown_watched_dates_remain_unknown():
    adds, _, _ = plan_group(group(), {"source": index([episode(2, watched=True), episode(3, watched=True)]), "target": {}}, {})
    assert adds["target"][0]["watched_at"] == "unknown"

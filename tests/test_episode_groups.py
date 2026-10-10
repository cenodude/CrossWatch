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


def season_groups():
    side = lambda provider, numbers: dict(provider=provider, instance="default", episodes=[episode(n) for n in numbers])
    return [dict(id="double", name="Double", scrobble=False, source=side("SRC", [15, 16]), target=side("DST", [15])),
            dict(id="finale", name="Finale", scrobble=False, source=side("SRC", [24, 25]), target=side("DST", [23]))]


def stamped(number):
    return episode(number, watched_at=f"2026-10-{number:02d}T12:00:00Z")


def shift_season(config_base, monkeypatch, mode, target, stepwise):
    from api import editorAPI

    src, dst, cfg = setup(config_base, monkeypatch, [stamped(n) for n in range(15, 26)], target, mode)
    manual_policy.save_policy(config_base, dict(version=1, providers={}, pairs={"p1": dict(providers={}, episode_groups=season_groups())}))
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    edits = []
    for number in range(17, 24):
        original, corrected = stamped(number), {**stamped(number), "episode": number - 1}
        edits.append(([("history", "SRC", index([corrected]), [canonical_key(original)], "default")],
                      {("history", "SRC", "default"): {canonical_key(corrected): dict(original_key=canonical_key(original), original=original)}}))
    if stepwise:
        for rows, records in edits:
            editorAPI._save_policy_manual_batch(rows, pair_id="p1", mappings=records)
    else:
        merged = {}
        for _, records in edits:
            merged.update(records[("history", "SRC", "default")])
        editorAPI._save_policy_manual_batch([row for rows, _ in edits for row in rows], pair_id="p1",
                                            mappings={("history", "SRC", "default"): merged})
    return src, dst, cfg


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("stepwise", [False, True])
def test_shifted_season_between_two_groups_syncs_every_episode(config_base, monkeypatch, mode, stepwise):
    src, dst, cfg = shift_season(config_base, monkeypatch, mode, [], stepwise)
    assert run(cfg)["ok"]
    assert set(dst.index) == set(index([episode(n) for n in range(15, 24)]))
    assert dst.index[canonical_key(episode(16))]["watched_at"] == stamped(17)["watched_at"]
    assert dst.index[canonical_key(episode(22))]["watched_at"] == stamped(23)["watched_at"]
    assert dst.index[canonical_key(episode(23))]["watched_at"] == stamped(25)["watched_at"]
    calls = len(dst.add_calls)
    assert run(cfg)["ok"]
    assert len(dst.add_calls) == calls and not src.add_calls
    assert set(src.index) == set(index([episode(n) for n in range(15, 26)]))


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_deliberately_blocked_correction_in_a_chain_is_not_synced(config_base, monkeypatch, mode):
    src, dst, cfg = shift_season(config_base, monkeypatch, mode, [], True)
    policy = manual_policy.load_policy(config_base)
    policy["pairs"]["p1"]["providers"]["SRC"]["history"]["mappings"][canonical_key(episode(17))]["blocked"] = True
    manual_policy.save_policy(config_base, policy)
    assert run(cfg)["ok"]
    assert set(dst.index) == set(index([episode(n) for n in range(15, 24) if n != 17]))


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_shifted_season_already_in_sync_plans_nothing(config_base, monkeypatch, mode):
    src, dst, cfg = shift_season(config_base, monkeypatch, mode, [stamped(n) for n in range(15, 24)], True)
    review = InteractivePlan()
    assert run(cfg, review)["ok"]
    assert not review.rows
    assert not any("blocked" in notice["reason"] for notice in review.notices)


def identified(number, episode_id):
    return {**stamped(number), "series_title": "Example", "ids": {"tvdb": str(episode_id), "imdb": f"tt{episode_id}"}}


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_same_number_with_different_episode_ids_is_flagged_until_corrected(config_base, monkeypatch, mode):
    from api import editorAPI

    source = [identified(1, 501), identified(2, 502), identified(3, 503)]
    target = [identified(1, 501), identified(2, 503)]
    src, dst, cfg = setup(config_base, monkeypatch, source, target, mode)
    manual_policy.save_policy(config_base, dict(version=1, providers={}, pairs={}))
    review = InteractivePlan()
    assert run(cfg, review)["ok"]
    notices = [notice["reason"] for notice in review.notices if notice["event"] == "history:episode_identity"]
    assert len(notices) == 1 and notices[0].startswith("1 episode share") and "Example S01E02" in notices[0]
    monkeypatch.setattr(editorAPI, "_STATE_BASE", config_base)
    original, corrected = source[2], {**source[2], "episode": 2}
    editorAPI._save_policy_manual_batch([("history", "SRC", index([corrected]), [canonical_key(original)], "default")],
        pair_id="p1", mappings={("history", "SRC", "default"): {canonical_key(corrected): dict(
            original_key=canonical_key(original), original=original)}})
    review = InteractivePlan()
    assert run(cfg, review)["ok"]
    assert not [notice for notice in review.notices if notice["event"] == "history:episode_identity"]


@pytest.mark.parametrize("mode", ["one-way", "two-way"])
def test_unattended_sync_warns_once_per_distinct_mismatch(config_base, monkeypatch, mode):
    source = [identified(1, 501), identified(2, 502), identified(3, 503)]
    src, dst, cfg = setup(config_base, monkeypatch, source, [identified(1, 501), identified(2, 503)], mode)
    manual_policy.save_policy(config_base, dict(version=1, providers={}, pairs={}))
    lines = []
    def sync(dry=False):
        lines.clear()
        assert Orchestrator(cfg).run(dry_run=dry, pair_scope_ids=["p1"], progress=lines.append)["ok"]
        return [line for line in lines if line.startswith("[!] History:")], [line for line in lines if '"history:episode_identity"' in line]
    assert sync(dry=True) == ([], [])
    warned, events = sync()
    assert len(warned) == len(events) == 1 and "Example S01E02" in warned[0] and "Fix numbering" in warned[0]
    assert sync() == ([], [])
    src.index.update({canonical_key(identified(4, 0)): identified(4, 504)})
    dst.index.update({canonical_key(identified(3, 0)): identified(3, 504)})
    assert len(sync()[0]) == 1
    assert sync() == ([], [])


def test_mismatch_warning_returns_after_it_was_resolved(config_base):
    from types import SimpleNamespace
    from cw_platform.orchestrator._episode_groups import note_identity_conflicts

    lines = []
    ctx = SimpleNamespace(state_store=SimpleNamespace(base_path=config_base, pair_scope="scope"), interactive=None,
                          emit=lambda *a, **k: None, emit_info=lines.append)
    good, bad, fixed = index([identified(2, 502), identified(3, 503)]), index([identified(2, 503)]), index([identified(2, 502)])
    for target, expected in ((bad, 1), (bad, 1), (fixed, 1), (bad, 2)):
        note_identity_conflicts(ctx, "history", [good, target], [{}, {}], [set(), set()], announce=True)
        assert len(lines) == expected
    note_identity_conflicts(ctx, "history", [good, index([identified(2, 501), identified(1, 502)])], [{}, {}], [set(), set()])
    assert len(lines) == 2


def test_episode_identity_ignores_missing_or_show_level_ids():
    from cw_platform.episode_identity import conflicts

    base = identified(2, 502)
    same = {**base, "ids": {"tvdb": "999", "imdb": "tt502"}}
    copied = {**base, "ids": {"tmdb": "100"}}
    bare = {key: value for key, value in base.items() if key != "show_ids"}
    for other in (same, copied, {**base, "ids": {}}, bare, None):
        assert not conflicts(index([base]), {canonical_key(base): other} if other else {})
    assert not conflicts(index([base]), index([identified(2, 777)]))
    stale = [{**identified(n, 600 + n), "ids": {"tvdb": str(600 + n)}} for n in range(1, 9)]
    fresh = [{**identified(n, 600 + n), "ids": {"tvdb": str(600 + n if n == 1 else 900 + n)}} for n in range(1, 9)]
    assert not conflicts(index(stale), index(fresh))
    shifted = conflicts(index([base, identified(3, 503)]), index([identified(2, 503)]))
    assert [key for key, _, _ in shifted] == [canonical_key(base)]
    reused = [{**identified(n, 600), "ids": {"tmdb": "4242"}} for n in (2, 3)]
    assert not conflicts(index(reused), index([{**identified(2, 777), "ids": {"tmdb": "8"}}]))


def test_mismatch_summary_groups_episodes_by_show():
    from cw_platform.episode_identity import summary

    rows = [(canonical_key(identified(n, n)), identified(n, n), None) for n in (4, 2, 3)]
    assert "(Example S01E02, S01E03, S01E04)" in summary(rows) and summary(rows).startswith("3 episodes share")

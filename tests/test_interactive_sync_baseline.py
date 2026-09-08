from copy import deepcopy
from unittest.mock import Mock

import pytest

from api import syncAPI
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from services import interactive_sync as svc
from test_interactive_sync_features import FEATURES, feature_item, feature_setup


@pytest.mark.parametrize("feature", FEATURES)
@pytest.mark.parametrize("mode", ["one-way", "two-way"])
@pytest.mark.parametrize("empty", [False, True])
def test_empty_review_saves_pair_baselines_and_inventory(config_base, monkeypatch, feature, mode, empty):
    items = [] if empty else [feature_item(feature, 1)]
    cfg, src, dst = feature_setup(config_base, monkeypatch, feature, items, items, mode)
    for ops in (src, dst):
        monkeypatch.setattr(ops, "remove", Mock(wraps=ops.remove))
    monkeypatch.setattr(syncAPI, "_env", lambda: (lambda: deepcopy(cfg), lambda *_: None))
    store = StateStore(config_base)
    other = store.for_pair("other-pair")
    other.save_feature_blocks({("SRC", "default", feature): {"baseline": {"items": {"other": feature_item(feature, 2)}}}})
    before_other = deepcopy(other.load_state())
    pair_store = store.for_pair(pair_feature_scope(cfg, cfg["pairs"][0], feature))
    assert not pair_store.load_state()["providers"]
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert session.status == "complete", session.message
        assert session.report["outcome"] == "success"
        assert session.report["requested"] == 0
        for state in (pair_store.load_state(), store.load_state()):
            for provider in ("SRC", "DST"):
                baseline = state["providers"][provider][feature]["baseline"]["items"]
                assert set(baseline) == (set() if empty else {"imdb:tt0000001"})
                if not empty:
                    assert baseline["imdb:tt0000001"]["title"] == items[0]["title"]
        assert other.load_state() == before_other
        assert not src.add_calls and not dst.add_calls
        src.remove.assert_not_called()
        dst.remove.assert_not_called()
    finally:
        session.close()


@pytest.mark.parametrize("dry_run", [True, "true"])
def test_empty_dry_run_does_not_save_baselines(config_base, monkeypatch, dry_run):
    cfg, _, _ = feature_setup(config_base, monkeypatch, "watchlist", [], [])
    cfg["sync"]["dry_run"] = dry_run
    monkeypatch.setattr(syncAPI, "_run_pairs_thread", lambda *a, **k: pytest.fail("dry run reached execution"))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert session.status == "review"
        assert session.report is None
        assert not StateStore(config_base).load_state()["providers"]
    finally:
        session.close()


@pytest.mark.parametrize("problem", ["cancelled", "errors", "unresolved", "blocked", "notice", "failed"])
def test_incomplete_empty_review_does_not_run(config_base, monkeypatch, problem):
    cfg, _, _ = feature_setup(config_base, monkeypatch, "watchlist", [], [])
    original_build = svc.build

    def build(*args, **kwargs):
        plan, summary = original_build(*args, **kwargs)
        if problem == "notice":
            plan.notices.append(dict(event="feature:unsupported"))
        elif problem == "failed":
            summary["ok"] = False
        else:
            summary[problem] = 1
        return plan, summary

    monkeypatch.setattr(svc, "build", build)
    monkeypatch.setattr(syncAPI, "_run_pairs_thread", lambda *a, **k: pytest.fail("incomplete review reached execution"))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert session.status != "complete"
        assert session.report is None
        assert not StateStore(config_base).load_state()["providers"]
    finally:
        session.close()


def test_new_changes_during_empty_review_recheck_return_to_review(config_base, monkeypatch):
    common = feature_item("watchlist", 1)
    cfg, src, dst = feature_setup(config_base, monkeypatch, "watchlist", [common], [common])
    original_build = svc.build
    builds = []

    def build(*args, **kwargs):
        plan, summary = original_build(*args, **kwargs)
        builds.append(True)
        if len(builds) == 1:
            src.index["imdb:tt0000002"] = feature_item("watchlist", 2)
        return plan, summary

    monkeypatch.setattr(svc, "build", build)
    monkeypatch.setattr(syncAPI, "_run_pairs_thread", lambda *a, **k: pytest.fail("changed review reached execution"))
    session = svc.Session(pair_id="p1", owner="local")
    try:
        svc.refresh(session, cfg, {})
        assert len(builds) == 2
        assert session.status == "review"
        assert session.store.counts["changes"] == 1
        assert session.apply_review["applied"] == 0
        assert session.report is None
        assert not src.add_calls and not dst.add_calls
        assert not StateStore(config_base).load_state()["providers"]
    finally:
        session.close()

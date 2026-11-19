# tests/test_pair_state_maintenance.py
# CrossWatch - Daily cleanup of obsolete sync pair state
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import threading
from copy import deepcopy
from pathlib import Path

from cw_platform.local_db.operation_lock import exclusive_operation
from cw_platform.local_db.state import pair_state_counts
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.pair_scope import pair_feature_scope
from services import pair_state_maintenance as cleanup
from services.pair_state import adopt_legacy_scopes
from test_database_maintenance_lifecycle import seed


def _changed_pair(base):
    pair = {"id": "one", "source": "PLEX", "target": "TRAKT", "features": {"history": {"enable": True}}}
    cfg = {"pairs": [pair]}
    old = seed(base, cfg, pair)
    pair["mode"] = "two-way"
    current = seed(base, cfg, pair)
    return cfg, old, current


def test_obsolete_scopes_are_pruned_and_current_scopes_kept(config_base):
    cfg, old, current = _changed_pair(config_base)

    result = cleanup.run_once(cfg, config_base)

    assert result is not None and result["items"] == 1
    assert pair_state_counts(config_base, old) == (0, 0)
    assert pair_state_counts(config_base, current) == (1, 1)


def test_running_sync_defers_cleanup(config_base):
    cfg, old, _ = _changed_pair(config_base)
    held, release = threading.Event(), threading.Event()

    def sync():
        with exclusive_operation(config_base):
            held.set()
            release.wait(10)

    t = threading.Thread(target=sync)
    t.start()
    held.wait(5)
    try:
        assert cleanup.run_once(cfg, config_base) is None
    finally:
        release.set()
        t.join(5)
    assert pair_state_counts(config_base, old) == (1, 1)


def _oauth_cfg(expires_at):
    pair = {"id": "one", "source": "PLEX", "target": "PUNCHPLAY", "features": {"history": {"enable": True}}}
    return {"plex": {"account_token": "x"},
            "punchplay": {"access_token": "a", "refresh_token": "r", "expires_at": expires_at,
                          "session_refreshed_at": expires_at, "expires_in": 3600}, "pairs": [pair]}, pair


def test_token_refresh_keeps_the_pair_scope(config_base):
    cfg, pair = _oauth_cfg(1000)
    refreshed, _ = _oauth_cfg(2000)
    assert pair_feature_scope(cfg, pair, "history") == pair_feature_scope(refreshed, pair, "history")
    assert pair_feature_scope(cfg, pair, "history", legacy=True) != pair_feature_scope(refreshed, pair, "history", legacy=True)
    changed = deepcopy(cfg)
    changed["pairs"][0]["mode"] = "two-way"
    assert pair_feature_scope(changed, changed["pairs"][0], "history") != pair_feature_scope(cfg, pair, "history")


def _seed_legacy(base, cfg, pair):
    old = pair_feature_scope(cfg, pair, "history", 1, legacy=True)
    StateStore(base).for_pair(old, pair_id="one").save_feature_blocks({
        ("PLEX", "default", "history"): {"baseline": {"items": {"imdb:tt1": {"title": "One", "type": "movie"}}}}
    })
    state = base / ".cw_state"
    state.mkdir(exist_ok=True)
    (state / f"punchplay_history.shadow.{old}.json").write_bytes(b"{}")
    store = StateStore(base)
    store.save_tomb({"keys": {f"history:{old.upper()}|imdb:tt9": 1, "history:OTHER|imdb:tt8": 2}, "pruned_at": None})
    return old


def test_existing_state_moves_to_the_stable_scope(config_base):
    cfg, pair = _oauth_cfg(1000)
    old = _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)

    assert adopt_legacy_scopes(config_base, cfg) == 1

    assert pair_state_counts(config_base, old) == (0, 0)
    assert pair_state_counts(config_base, new) == (1, 1)
    names = {p.name for p in (config_base / ".cw_state").iterdir()}
    assert f"punchplay_history.shadow.{new}.json" in names
    assert not any(old in name for name in names)
    keys = set(StateStore(config_base).load_tomb()["keys"])
    assert keys == {f"history:{new.upper()}|imdb:tt9", "history:OTHER|imdb:tt8"}
    assert adopt_legacy_scopes(config_base, cfg) == 0


def test_adoption_never_overwrites_newer_state(config_base):
    cfg, pair = _oauth_cfg(1000)
    old = _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)
    StateStore(config_base).for_pair(new, pair_id="one").save_feature_blocks({
        ("PLEX", "default", "history"): {"baseline": {"items": {"imdb:tt2": {"title": "Two", "type": "movie"}, "imdb:tt3": {"title": "Three", "type": "movie"}}}}
    })

    assert adopt_legacy_scopes(config_base, cfg) == 0
    assert pair_state_counts(config_base, new) == (1, 2)
    assert pair_state_counts(config_base, old) == (1, 1)


def test_scoped_tracker_pairs_are_not_adopted(config_base, monkeypatch):
    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    cfg, pair = _oauth_cfg(1000)
    pair["source"] = "CROSSWATCH"
    old = _seed_legacy(config_base, cfg, pair)

    assert adopt_legacy_scopes(config_base, cfg) == 0
    assert pair_state_counts(config_base, old) == (1, 1)


def test_locked_files_move_on_a_later_pass(config_base, monkeypatch):
    cfg, pair = _oauth_cfg(1000)
    old = _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)
    real = Path.replace

    def locked(self, target):
        raise PermissionError("in use")

    monkeypatch.setattr(Path, "replace", locked)
    assert adopt_legacy_scopes(config_base, cfg) == 1
    assert any(old in p.name for p in (config_base / ".cw_state").iterdir())

    monkeypatch.setattr(Path, "replace", real)
    adopt_legacy_scopes(config_base, cfg)
    names = {p.name for p in (config_base / ".cw_state").iterdir()}
    assert f"punchplay_history.shadow.{new}.json" in names
    assert not any(old in name for name in names)


def test_pending_moves_survive_a_token_refresh(config_base, monkeypatch):
    cfg, pair = _oauth_cfg(1000)
    old = _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)
    real = Path.replace

    def locked(self, target):
        raise PermissionError("in use")

    monkeypatch.setattr(Path, "replace", locked)
    assert adopt_legacy_scopes(config_base, cfg) == 1
    monkeypatch.setattr(Path, "replace", real)

    refreshed, _ = _oauth_cfg(2000)
    adopt_legacy_scopes(config_base, refreshed)

    names = {p.name for p in (config_base / ".cw_state").iterdir()}
    assert f"punchplay_history.shadow.{new}.json" in names
    assert not any(old in name for name in names)
    assert set(StateStore(config_base).load_tomb()["keys"]) == {f"history:{new.upper()}|imdb:tt9", "history:OTHER|imdb:tt8"}
    from cw_platform.local_db.state import pending_scope_moves
    assert pending_scope_moves(config_base) == []


def test_pair_reset_cancels_pending_moves(config_base, monkeypatch):
    from cw_platform.local_db.state import pending_scope_moves
    from services.pair_state import clear_pair_state

    cfg, pair = _oauth_cfg(1000)
    old = _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)
    real = Path.replace

    def locked(self, target):
        raise PermissionError("in use")

    monkeypatch.setattr(Path, "replace", locked)
    assert adopt_legacy_scopes(config_base, cfg) == 1
    monkeypatch.setattr(Path, "replace", real)

    assert not clear_pair_state(config_base, config_base / ".cw_state", [new])["errors"]
    adopt_legacy_scopes(config_base, cfg)

    names = {p.name for p in (config_base / ".cw_state").iterdir()}
    assert not any(old in name or new in name for name in names)
    keys = set(StateStore(config_base).load_tomb()["keys"])
    assert keys == {"history:OTHER|imdb:tt8"}
    assert pending_scope_moves(config_base) == []
    assert pair_state_counts(config_base, new) == (0, 0)


def test_full_state_reset_cancels_pending_moves(config_base, monkeypatch):
    from cw_platform.local_db.state import pending_scope_moves

    cfg, pair = _oauth_cfg(1000)
    _seed_legacy(config_base, cfg, pair)
    monkeypatch.setattr(Path, "replace", lambda self, target: (_ for _ in ()).throw(PermissionError("in use")))
    assert adopt_legacy_scopes(config_base, cfg) == 1
    assert pending_scope_moves(config_base)

    StateStore(config_base).clear_state()

    assert pending_scope_moves(config_base) == []


def test_unfinished_move_is_reported_pending_for_its_scope(config_base, monkeypatch):
    from cw_platform.pair_scope import scope_move_pending

    cfg, pair = _oauth_cfg(1000)
    _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)
    monkeypatch.setattr(Path, "replace", lambda self, target: (_ for _ in ()).throw(PermissionError("in use")))
    adopt_legacy_scopes(config_base, cfg)

    assert scope_move_pending(config_base, new)
    assert not scope_move_pending(config_base, "cw2_other")


def test_cleanup_reads_config_while_holding_the_lock(config_base):
    cfg, old, current = _changed_pair(config_base)
    seen = {}

    def load():
        try:
            with exclusive_operation(config_base):
                seen["free"] = True
        except RuntimeError:
            seen["free"] = False
        return cfg

    cleanup.run_once(load, config_base)

    assert seen == {"free": False}
    assert pair_state_counts(config_base, current) == (1, 1)


def test_adoption_keeps_rows_already_under_the_new_scope(config_base):
    from cw_platform.local_db.db import get_conn

    cfg, pair = _oauth_cfg(1000)
    old = _seed_legacy(config_base, cfg, pair)
    new = pair_feature_scope(cfg, pair, "history", 1)
    conn = get_conn(config_base)
    with conn:
        conn.execute("INSERT INTO episode_group_scrobbles(pair_scope,delivery_scope,state_json) VALUES(?,?,?)", (new, "d1", '{"done":1}'))
        conn.execute("INSERT INTO episode_group_scrobbles(pair_scope,delivery_scope,state_json) VALUES(?,?,?)", (old, "d1", '{"done":0}'))
        conn.execute("INSERT INTO episode_group_scrobbles(pair_scope,delivery_scope,state_json) VALUES(?,?,?)", (old, "d2", '{"done":0}'))

    assert adopt_legacy_scopes(config_base, cfg) == 1

    rows = dict(conn.execute("SELECT delivery_scope, state_json FROM episode_group_scrobbles WHERE pair_scope=?", (new,)).fetchall())
    assert rows == {"d1": '{"done":1}', "d2": '{"done":0}'}
    assert conn.execute("SELECT COUNT(*) FROM episode_group_scrobbles WHERE pair_scope=?", (old,)).fetchone()[0] == 0


def test_cleanup_adopts_before_pruning(config_base):
    cfg, pair = _oauth_cfg(1000)
    _seed_legacy(config_base, cfg, pair)

    cleanup.run_once(cfg, config_base)

    assert pair_state_counts(config_base, pair_feature_scope(cfg, pair, "history", 1)) == (1, 1)


def test_missing_config_or_pairs_never_prunes(config_base):
    cfg, old, _ = _changed_pair(config_base)

    assert cleanup.run_once(None, config_base) is None
    assert cleanup.run_once({**deepcopy(cfg), "pairs": []}, config_base) == {"skipped": "no_pairs"}
    assert pair_state_counts(config_base, old) == (1, 1)

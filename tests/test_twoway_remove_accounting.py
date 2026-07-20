from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import _pairs_twoway as twoway


WATCHED_AT = "2023-10-12T12:28:00.000Z"
WATCHED_EPOCH = 1697113680


def _ep(n):
    return {
        "type": "episode",
        "title": f"E{n}",
        "season": 1,
        "episode": n,
        "watched_at": WATCHED_AT,
        "ids": {"tvdb": str(900000 + n)},
        "show_ids": {"tmdb": "1402", "tvdb": "153021"},
    }


def _key(n):
    return canonical_key(_ep(n))


class _StateStore:
    def __init__(self, state):
        self.state = state
        self.tomb: dict[str, Any] = {"keys": {}}

    def load_state(self):
        return self.state

    def save_state(self, value):
        self.state = value

    def load_tomb(self):
        return self.tomb

    def save_tomb(self, value):
        self.tomb = value


class _Ops:
    def __init__(self, remove_result):
        self.removed: list[dict[str, Any]] = []
        self._remove_result = remove_result

    def add(self, _cfg, items, *, feature, dry_run=False):
        return {"ok": True, "count": len(items), "confirmed_keys": [], "unresolved": []}

    def remove(self, _cfg, items, *, feature, dry_run=False):
        lst = [dict(i) for i in items]
        self.removed.extend(lst)
        return self._remove_result(lst)

    def update(self, *_a, **_k):
        return {"ok": True, "count": 0}

    def capabilities(self):
        return {"history": {"observed_deletes": False}}


def _run(monkeypatch, *, count, n_items, remove_result):
    for name, val in (
        ("_supports_feature", lambda _o, _f: True),
        ("_health_feature_ok", lambda _h, _f: True),
        ("_health_status", lambda _h: "up"),
        ("_resolve_flags", lambda _f, _s: {"allow_adds": False, "allow_removals": True}),
        ("_anime_pair_feature_options", lambda *_a, **_k: {"use_anime_mapping": False}),
        ("_anime_config_with_pair_feature_options", lambda cfg, _o: cfg),
        ("_index_semantics", lambda *_a, **_k: "full"),
        ("prev_checkpoint", lambda *_a, **_k: None),
        ("module_checkpoint", lambda *_a, **_k: None),
        ("keys_for_feature", lambda *_a, **_k: {}),
        ("_manual_policy", lambda *_a, **_k: ([], set())),
        ("_provider_ignore_dropped_enabled", lambda *_a, **_k: False),
        ("apply_blocklist", lambda _s, items, **_k: list(items)),
        ("_maybe_block_massdelete", lambda items, **_k: list(items)),
        ("effective_chunk_size", lambda *_a, **_k: 100),
        ("load_blackbox_keys", lambda *_a, **_k: set()),
        ("record_attempts", lambda *_a, **_k: {"ok": True, "count": 0}),
        ("record_success", lambda *_a, **_k: {"ok": True, "count": 0}),
    ):
        monkeypatch.setattr(twoway, name, val)

    recorded: list[dict[str, Any]] = []
    cleared: list[list[str]] = []
    monkeypatch.setattr(
        twoway, "record_unresolved",
        lambda dst, feature, items, *, hint="": recorded.append({"hint": hint, "items": [dict(i) for i in items]}) or {"ok": True},
    )
    monkeypatch.setattr(twoway, "clear_unresolved", lambda dst, feature, keys: cleared.append(list(keys)) or {"ok": True})

    items = {_key(n): _ep(n) for n in range(1, n_items + 1)}
    monkeypatch.setattr(twoway, "build_snapshots_for_feature", lambda **_k: {"TRAKT": dict(items), "SIMKL": {}})

    state = {"providers": {"SIMKL": {"history": {"baseline": {"items": dict(items)}}}}}
    store = _StateStore(state)
    trakt = _Ops(remove_result)
    ctx = SimpleNamespace(
        config={"sync": {"include_observed_deletes": False, "blackbox": {"enabled": False}}, "runtime": {}},
        providers={"TRAKT": trakt, "SIMKL": _Ops(lambda _l: {"ok": True, "count": 0})},
        emit=lambda *_a, **_k: None,
        emit_info=lambda *_a, **_k: None,
        dbg=lambda *_a, **_k: None,
        dry_run=False,
        snap_cache={},
        snap_ttl_sec=0,
        state_store=store,
        stats_manual_blocked=0,
        apply_chunk_pause_ms=0,
    )
    result = twoway._two_way_sync(ctx, "TRAKT", "SIMKL", feature="history", fcfg={}, health_map={})
    return result, trakt, store, recorded, cleared


def _tomb_keys(store):
    return {k for k in (store.tomb.get("keys") or {})}


def test_twoway_exact_keys_tombstone_only_confirmed(monkeypatch):
    confirmed = [canonical_key(_ep(1)), canonical_key(_ep(2))]

    result, trakt, store, recorded, cleared = _run(
        monkeypatch,
        count=5,
        n_items=5,
        remove_result=lambda lst: {"ok": True, "count": 5, "confirmed_keys": confirmed, "unresolved": []},
    )

    assert len(trakt.removed) == 5
    assert result["rem_from_A"] == 2

    tombs = _tomb_keys(store)
    assert any(canonical_key(_ep(1)) in t for t in tombs)
    assert not any(canonical_key(_ep(5)) in t for t in tombs)

    assert cleared == [confirmed]
    assert len(recorded) == 1
    assert recorded[0]["hint"] == "two:apply:remove:unconfirmed"
    assert len(recorded[0]["items"]) == 3


def test_twoway_ambiguous_partial_tombstones_nothing(monkeypatch):
    result, trakt, store, recorded, cleared = _run(
        monkeypatch,
        count=4,
        n_items=10,
        remove_result=lambda lst: {"ok": True, "count": 4, "confirmed_keys": [], "unresolved": []},
    )

    assert len(trakt.removed) == 10
    assert result["rem_from_A"] == 0
    assert _tomb_keys(store) == set()
    assert cleared == []
    assert len(recorded) == 1
    assert len(recorded[0]["items"]) == 10


def test_twoway_clean_full_count_without_exact_keys_is_accepted(monkeypatch):
    result, trakt, store, recorded, cleared = _run(
        monkeypatch,
        count=3,
        n_items=3,
        remove_result=lambda lst: {"ok": True, "count": 3, "confirmed_keys": [], "unresolved": []},
    )

    assert result["rem_from_A"] == 3
    assert len(_tomb_keys(store)) >= 3
    assert cleared and len(cleared[0]) == 3
    assert recorded == []

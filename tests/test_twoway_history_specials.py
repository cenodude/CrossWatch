from __future__ import annotations

from types import SimpleNamespace
from typing import Any

from cw_platform.id_map import canonical_key
from cw_platform.orchestrator import _pairs_twoway as twoway


WATCHED_AT = "2023-10-12T12:28:00.000Z"
WATCHED_EPOCH = 1697113680
TRAKT_SPECIAL = {
    "type": "episode",
    "title": "The Making of The Walking Dead",
    "season": 0,
    "episode": 48,
    "watched_at": WATCHED_AT,
    "ids": {"tmdb": "63372", "tvdb": "2960601", "trakt": "5942444"},
    "show_ids": {"tmdb": "1402", "imdb": "tt1520211", "tvdb": "153021", "trakt": "1393"},
}
SIMKL_SPECIAL = {
    **TRAKT_SPECIAL,
    "ids": {"tvdb": "2960601"},
    "show_ids": {"tvdb": "153021"},
}


class _StateStore:
    def __init__(self) -> None:
        self.state: dict[str, Any] = {}

    def load_state(self) -> dict[str, Any]:
        return self.state

    def save_state(self, value: dict[str, Any]) -> None:
        self.state = value

    def load_tomb(self) -> dict[str, Any]:
        return {}

    def save_tomb(self, _value: dict[str, Any]) -> None:
        return None


class _Ops:
    def __init__(self) -> None:
        self.added: list[dict[str, Any]] = []

    def add(self, _cfg, items, *, feature, dry_run=False):
        self.added.extend(dict(item) for item in items)
        return {
            "ok": True,
            "count": len(items),
            "confirmed_keys": [canonical_key(item) for item in items],
            "unresolved": [],
        }

    def remove(self, *_args, **_kwargs):
        return {"ok": True, "count": 0}

    def update(self, *_args, **_kwargs):
        return {"ok": True, "count": 0}

    def capabilities(self):
        return {"history": {"observed_deletes": False}}


def _run_two_way(monkeypatch, snapshots: dict[str, dict[str, dict[str, Any]]]):
    trakt = _Ops()
    simkl = _Ops()
    monkeypatch.setattr(twoway, "build_snapshots_for_feature", lambda **_kwargs: snapshots)
    ctx = SimpleNamespace(
        config={"sync": {"include_observed_deletes": False, "blackbox": {"enabled": False}}, "runtime": {}},
        providers={"TRAKT": trakt, "SIMKL": simkl},
        emit=lambda *_args, **_kwargs: None,
        emit_info=lambda *_args, **_kwargs: None,
        dbg=lambda *_args, **_kwargs: None,
        dry_run=False,
        snap_cache={},
        snap_ttl_sec=0,
        state_store=_StateStore(),
        stats_manual_blocked=0,
        apply_chunk_pause_ms=0,
    )
    result = twoway._two_way_sync(ctx, "TRAKT", "SIMKL", feature="history", fcfg={}, health_map={})
    return result, trakt, simkl


def test_trakt_simkl_two_way_routes_and_converges_special(monkeypatch) -> None:
    monkeypatch.setattr(twoway, "_supports_feature", lambda _ops, _feature: True)
    monkeypatch.setattr(twoway, "_health_feature_ok", lambda _health, _feature: True)
    monkeypatch.setattr(twoway, "_health_status", lambda _health: "up")
    monkeypatch.setattr(twoway, "_resolve_flags", lambda _fcfg, _sync: {"allow_adds": True, "allow_removals": False})
    monkeypatch.setattr(twoway, "_anime_pair_feature_options", lambda *_args, **_kwargs: {"use_anime_mapping": False})
    monkeypatch.setattr(twoway, "_anime_config_with_pair_feature_options", lambda cfg, _opts: cfg)
    monkeypatch.setattr(twoway, "_index_semantics", lambda *_args, **_kwargs: "full")
    monkeypatch.setattr(twoway, "prev_checkpoint", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(twoway, "module_checkpoint", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(twoway, "keys_for_feature", lambda *_args, **_kwargs: {})
    monkeypatch.setattr(twoway, "_manual_policy", lambda *_args, **_kwargs: ([], set()))
    monkeypatch.setattr(twoway, "_provider_ignore_dropped_enabled", lambda *_args, **_kwargs: False)
    monkeypatch.setattr(twoway, "apply_blocklist", lambda _state, items, **_kwargs: list(items))
    monkeypatch.setattr(twoway, "_maybe_block_massdelete", lambda items, **_kwargs: list(items))
    monkeypatch.setattr(twoway, "effective_chunk_size", lambda *_args, **_kwargs: 100)
    monkeypatch.setattr(twoway, "load_blackbox_keys", lambda *_args, **_kwargs: set())
    monkeypatch.setattr(twoway, "record_attempts", lambda *_args, **_kwargs: {"ok": True, "count": 0})
    monkeypatch.setattr(twoway, "record_success", lambda *_args, **_kwargs: {"ok": True, "count": 0})

    trakt_key = f"tmdb:1402#s00e48@{WATCHED_EPOCH}"
    missing_result, trakt, simkl = _run_two_way(monkeypatch, {"TRAKT": {trakt_key: TRAKT_SPECIAL}, "SIMKL": {}})

    assert missing_result["adds_to_A"] == 0
    assert missing_result["adds_to_B"] == 1
    assert trakt.added == []
    assert len(simkl.added) == 1
    assert simkl.added[0]["season"] == 0
    assert simkl.added[0]["episode"] == 48

    simkl_key = f"tvdb:153021#s00e48@{WATCHED_EPOCH}"
    converged_result, trakt, simkl = _run_two_way(
        monkeypatch,
        {"TRAKT": {trakt_key: TRAKT_SPECIAL}, "SIMKL": {simkl_key: SIMKL_SPECIAL}}
    )

    assert converged_result["adds_to_A"] == 0
    assert converged_result["adds_to_B"] == 0
    assert trakt.added == []
    assert simkl.added == []

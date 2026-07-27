from __future__ import annotations

from pathlib import Path
from types import SimpleNamespace
from typing import Any

from cw_platform.id_map import canonical_key


def test_diff_progress_upserts_percent_only_source() -> None:
    from cw_platform.orchestrator._planner import diff_progress

    adds, clears = diff_progress(
        {
            "tmdb:69478#s06e02": {
                "type": "episode",
                "show_ids": {"tmdb": "69478"},
                "season": 6,
                "episode": 2,
                "progress_percent": 21.0,
                "progress_at": "2026-07-25T19:30:00Z",
            }
        },
        {},
    )

    assert clears == []
    assert len(adds) == 1
    assert adds[0]["progress_percent"] == 21.0


def test_diff_progress_clears_percent_only_source() -> None:
    from cw_platform.orchestrator._planner import diff_progress

    adds, clears = diff_progress(
        {
            "tmdb:69478#s06e02": {
                "type": "episode",
                "show_ids": {"tmdb": "69478"},
                "season": 6,
                "episode": 2,
                "progress_percent": 0,
            }
        },
        {
            "tmdb:69478#s06e02": {
                "type": "episode",
                "show_ids": {"tmdb": "69478"},
                "season": 6,
                "episode": 2,
                "progress_percent": 21,
            }
        },
    )

    assert adds == []
    assert len(clears) == 1
    assert clears[0]["progress_ms"] == 0


def test_two_way_minimal_progress_preserves_percent_only_source() -> None:
    from cw_platform.orchestrator._pairs_twoway import _minimal_keep_progress

    item = {
        "type": "episode",
        "show_ids": {"tmdb": "69478"},
        "season": 6,
        "episode": 2,
        "progress_percent": 21.1234,
        "progress_at": "2026-07-25T19:30:00Z",
    }

    out = _minimal_keep_progress(item)

    assert out["progress_percent"] == 21.123
    assert out["progress_at"] == "2026-07-25T19:30:00Z"


def test_two_way_minimal_progress_preserves_kodi_managed_timestamp_source() -> None:
    from cw_platform.orchestrator._pairs_twoway import _minimal_keep_progress

    item = {
        "type": "movie",
        "ids": {"tmdb": "329865"},
        "progress_ms": 120000,
        "duration_ms": 6000000,
        "progress_at": "2026-07-27T21:00:00Z",
        "progress_at_source": "kodi_first_observed",
    }

    out = _minimal_keep_progress(item)

    assert out["progress_at_source"] == "kodi_first_observed"


def test_crosswatch_progress_accepts_percent_only_items(tmp_path: Path, monkeypatch) -> None:
    from providers.sync.crosswatch import _progress

    monkeypatch.setenv("CW_CROSSWATCH_PAIR_SCOPED", "1")
    monkeypatch.setenv("CW_PAIR_SCOPE", "SIMKL-CROSSWATCH")
    adapter = type("Adapter", (), {"cfg": type("Cfg", (), {"base_path": str(tmp_path)})()})()
    item = {
        "type": "episode",
        "show_ids": {"tmdb": "69478"},
        "season": 6,
        "episode": 2,
        "progress_percent": 21.0,
        "progress_at": "2026-07-25T19:30:00Z",
    }

    count, unresolved = _progress.add(adapter, [item])
    index = _progress.build_index(adapter)

    assert count == 1
    assert unresolved == []
    assert index["tmdb:69478#s06e02"]["progress_percent"] == 21.0


class _ProgressStateStore:
    def load_state(self) -> dict[str, Any]:
        return {}

    def save_state(self, _value: dict[str, Any]) -> None:
        return None

    def load_tomb(self) -> dict[str, Any]:
        return {}

    def save_tomb(self, _value: dict[str, Any]) -> None:
        return None


class _ProgressOps:
    def __init__(self) -> None:
        self.added: list[dict[str, Any]] = []

    def add(self, _cfg: dict[str, Any], items, *, feature: str, dry_run: bool = False) -> dict[str, Any]:
        rows = [dict(item) for item in items]
        self.added.extend(rows)
        return {"ok": True, "count": len(rows), "confirmed_keys": [canonical_key(item) for item in rows], "unresolved": []}

    def remove(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True, "count": 0}

    def update(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        return {"ok": True, "count": 0}

    def capabilities(self) -> dict[str, Any]:
        return {"progress": {"observed_deletes": False}}


def test_two_way_progress_trusts_real_remote_over_kodi_first_observed(monkeypatch) -> None:
    from cw_platform.orchestrator import _pairs_twoway as twoway

    for name, val in (
        ("_supports_feature", lambda _ops, _feature: True),
        ("_health_feature_ok", lambda _health, _feature: True),
        ("_health_status", lambda _health: "up"),
        ("_resolve_flags", lambda _fcfg, _sync: {"allow_adds": True, "allow_removals": False}),
        ("_anime_pair_feature_options", lambda *_args, **_kwargs: {"use_anime_mapping": False}),
        ("_anime_config_with_pair_feature_options", lambda cfg, _opts: cfg),
        ("_index_semantics", lambda *_args, **_kwargs: "full"),
        ("prev_checkpoint", lambda *_args, **_kwargs: None),
        ("module_checkpoint", lambda *_args, **_kwargs: None),
        ("keys_for_feature", lambda *_args, **_kwargs: {}),
        ("_manual_policy", lambda *_args, **_kwargs: ([], set())),
        ("_provider_ignore_dropped_enabled", lambda *_args, **_kwargs: False),
        ("apply_blocklist", lambda _state, items, **_kwargs: list(items)),
        ("_maybe_block_massdelete", lambda items, **_kwargs: list(items)),
        ("effective_chunk_size", lambda *_args, **_kwargs: 100),
        ("load_blackbox_keys", lambda *_args, **_kwargs: set()),
        ("record_attempts", lambda *_args, **_kwargs: {"ok": True, "count": 0}),
        ("record_success", lambda *_args, **_kwargs: {"ok": True, "count": 0}),
    ):
        monkeypatch.setattr(twoway, name, val)

    key = "tmdb:329865"
    kodi_row = {
        "type": "movie",
        "title": "Arrival",
        "year": 2016,
        "ids": {"tmdb": "329865"},
        "progress_ms": 4_200_000,
        "duration_ms": 6_000_000,
        "progress_at": "2026-07-27T21:00:00Z",
        "progress_at_source": "kodi_first_observed",
    }
    trakt_row = {
        **kodi_row,
        "progress_ms": 3_000_000,
        "progress_at": "2026-07-25T21:00:00Z",
    }
    trakt_row.pop("progress_at_source", None)
    monkeypatch.setattr(twoway, "build_snapshots_for_feature", lambda **_kwargs: {"KODI": {key: kodi_row}, "TRAKT": {key: trakt_row}})

    kodi = _ProgressOps()
    trakt = _ProgressOps()
    ctx = SimpleNamespace(
        config={"sync": {"include_observed_deletes": False, "blackbox": {"enabled": False}}, "runtime": {}},
        providers={"KODI": kodi, "TRAKT": trakt},
        emit=lambda *_args, **_kwargs: None,
        emit_info=lambda *_args, **_kwargs: None,
        dbg=lambda *_args, **_kwargs: None,
        dry_run=False,
        snap_cache={},
        snap_ttl_sec=0,
        state_store=_ProgressStateStore(),
        stats_manual_blocked=0,
        apply_chunk_pause_ms=0,
    )

    result = twoway._two_way_sync(ctx, "KODI", "TRAKT", feature="progress", fcfg={}, health_map={})

    assert result["adds_to_A"] == 1
    assert result["adds_to_B"] == 0
    assert kodi.added[0]["progress_ms"] == 3_000_000
    assert trakt.added == []

from __future__ import annotations

import json
from types import SimpleNamespace

from api import maintenanceAPI


def test_clear_provider_cache_preserves_user_runtime_files(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()

    preserved = {
        "activity_history.json",
        "currently_watching.json",
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
    }
    sync_recovery_state = {
        "plex_history.default.phantoms.json",
        "plex_history.default.last_success.json",
        "emby.health.shadow.json",
        "trakt_dropped.index.json",
    }
    sync_owned_state = {
        "tombstones.json",
        "trakt_history.unresolved.json",
    }

    for name in preserved | sync_recovery_state | sync_owned_state:
        (state_dir / name).write_text("{}", encoding="utf-8")

    identity_dir = state_dir / "id"
    identity_dir.mkdir()
    (identity_dir / "index.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    removed = set(maintenanceAPI._clear_cw_state_files())

    assert removed == sync_recovery_state
    assert all((state_dir / name).exists() for name in preserved)
    assert (identity_dir / "index.json").exists()


def test_clear_provider_cache_returns_cleanup_receipt(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    (state_dir / "emby.health.shadow.json").write_bytes(b"12345")
    (state_dir / "currently_watching.json").write_bytes(b"keep")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.clear_cache()

    assert result["ok"] is True
    assert result["summary"] == {
        "removed_files": 1,
        "removed_items": 0,
        "freed_bytes": 5,
    }
    assert (state_dir / "currently_watching.json").exists()


def test_clear_state_returns_cleanup_receipt(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    (tmp_path / "state.json").write_bytes(b"provider baselines")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.clear_state_minimal()

    assert result["ok"] is True
    assert result["summary"] == {
        "removed_files": 1,
        "removed_items": 0,
        "freed_bytes": 18,
    }


def test_metadata_action_status_reports_recursive_storage(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "cache"
    nested = cache_dir / "artwork" / "posters"
    nested.mkdir(parents=True)
    (nested / "one.jpg").write_bytes(b"x" * 1536)
    (cache_dir / "metadata.json").write_bytes(b"{}")
    state_dir = tmp_path / ".cw_state"

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (
            cache_dir,
            tmp_path,
            state_dir,
            SimpleNamespace(path=tmp_path / "statistics.json"),
            None,
            None,
        ),
    )

    result = maintenanceAPI.maintenance_action_status("metadata")
    metrics = {item["label"]: item for item in result["metrics"]}

    assert result["ok"] is True
    assert metrics["Cached files"]["value"] == 2
    assert metrics["Cache storage"]["value"] == 1538
    assert metrics["Cache storage"]["format"] == "bytes"


def test_clear_metadata_cache_receipt_includes_nested_files(tmp_path, monkeypatch) -> None:
    cache_dir = tmp_path / "cache"
    nested = cache_dir / "artwork" / "posters"
    nested.mkdir(parents=True)
    (nested / "one.jpg").write_bytes(b"x" * 1536)
    (cache_dir / "metadata.json").write_bytes(b"{}")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (cache_dir, tmp_path, tmp_path / ".cw_state", None, None, None),
    )

    result = maintenanceAPI.clear_metadata_cache()

    assert result["ok"] is True
    assert result["summary"] == {
        "removed_files": 2,
        "removed_items": 0,
        "freed_bytes": 1538,
    }


def test_state_action_status_counts_provider_feature_baselines(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    state = {
        "providers": {
            "TRAKT": {
                "history": {"baseline": {"items": {}}},
                "watchlist": {"baseline": {"items": {}}},
            },
            "PLEX": {
                "ratings": {"baseline": {"items": {}}},
            },
        }
    }
    (tmp_path / "state.json").write_text(json.dumps(state), encoding="utf-8")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (
            tmp_path / "cache",
            tmp_path,
            state_dir,
            SimpleNamespace(path=tmp_path / "statistics.json"),
            None,
            None,
        ),
    )

    result = maintenanceAPI.maintenance_action_status("state")
    metrics = {item["label"]: item["value"] for item in result["metrics"]}

    assert metrics["Providers"] == 2
    assert metrics["Feature baselines"] == 3
    assert metrics["State storage"] > 0


def test_state_file_action_status_reports_largest_baseline(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    state = {
        "providers": {
            "TRAKT": {
                "history": {"baseline": {"items": {"a": {}, "b": {}}}},
            },
            "SIMKL": {
                "watchlist": {"baseline": {"items": {"c": {}}}},
            },
        }
    }
    (tmp_path / "state.json").write_text(json.dumps(state, indent=2), encoding="utf-8")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (
            tmp_path / "cache",
            tmp_path,
            state_dir,
            SimpleNamespace(path=tmp_path / "statistics.json"),
            None,
            None,
        ),
    )

    result = maintenanceAPI.maintenance_action_status("state-file")
    metrics = {item["label"]: item["value"] for item in result["metrics"]}

    assert result["ok"] is True
    assert metrics["Providers"] == 2
    assert metrics["Feature baselines"] == 2
    assert metrics["Baseline items"] == 3
    assert metrics["Largest baseline"] == "TRAKT history"


def test_compact_state_file_creates_backup_and_rewrites_json(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    payload = {"providers": {"TRAKT": {"watchlist": {"baseline": {"items": {"a": {"title": "A"}}}}}}}
    state_path = tmp_path / "state.json"
    state_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")
    before = state_path.stat().st_size
    backups: list[dict] = []

    def fake_backup(**kwargs):
        backups.append(kwargs)
        return {"ok": True, "path": "2026/pre-state-compact.zip"}

    import services.backups as backups_svc

    monkeypatch.setattr(backups_svc, "create_backup", fake_backup)
    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.compact_state_file()

    assert result["ok"] is True
    assert result["backup"]["path"] == "2026/pre-state-compact.zip"
    assert backups and backups[0]["scope"] == "app_state"
    assert backups[0]["trigger"] == "maintenance_state_compact"
    assert state_path.stat().st_size < before
    assert "\n" not in state_path.read_text(encoding="utf-8")
    assert json.loads(state_path.read_text(encoding="utf-8")) == payload


def test_compact_state_file_rejects_invalid_json_without_backup(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    (tmp_path / "state.json").write_text("{not valid", encoding="utf-8")

    def fail_backup(**_kwargs):
        raise AssertionError("backup should not be created for invalid JSON")

    import services.backups as backups_svc

    monkeypatch.setattr(backups_svc, "create_backup", fail_backup)
    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.compact_state_file()

    assert result["ok"] is False
    assert result["error"] == "state_json_invalid"


def test_clear_provider_cache_preserves_pair_scoped_history_mapping_state(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()

    sync_owned_state = {
        "trakt_history.pair_alias.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.source_alias.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.anime_episode_alias.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.anime_episode_map.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.anime_resolve.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.unresolved.one-way_SIMKL_default-TRAKT_default_p1.json",
        "trakt_history.unresolved.pending.one-way_SIMKL_default-TRAKT_default_p1.json",
        "trakt.history.cache.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl.history.cache.one-way_SIMKL_default-TRAKT_default_p1.json",
        "watermarks.json",
        "tombstones.json",
    }
    runtime_cache = {
        "simkl_history.unscoped.flap.json",
        "plex_history.default.phantoms.json",
        "emby.health.shadow.json",
    }
    preserved = {
        "activity_history.json",
        "currently_watching.json",
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
    }

    for name in sync_owned_state | runtime_cache | preserved:
        (state_dir / name).write_text("{}", encoding="utf-8")

    identity_dir = state_dir / "id"
    identity_dir.mkdir()
    (identity_dir / "index.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    scanned = {item["name"] for item in maintenanceAPI._scan_provider_cache()["files"]}
    assert scanned == runtime_cache

    result = maintenanceAPI.clear_cache()

    assert result["ok"] is True
    assert set(result["removed"]) == runtime_cache
    for name in sync_owned_state | preserved:
        assert (state_dir / name).exists()
    for name in runtime_cache:
        assert not (state_dir / name).exists()
    assert (identity_dir / "index.json").exists()


def _seed_rebuild_state(state_dir):
    scoped = {
        "trakt_history.pair_alias.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.source_alias.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.anime_episode_alias.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.anime_episode_map.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.anime_resolve.one-way_SIMKL_default-TRAKT_default_p1.json",
        "simkl_history.unresolved.one-way_SIMKL_default-TRAKT_default_p1.json",
        "trakt_history.unresolved.pending.one-way_SIMKL_default-TRAKT_default_p1.json",
        "trakt.history.cache.one-way_SIMKL_default-TRAKT_default_p1.json",
        "watermarks.json",
        "tombstones.json",
    }
    unrelated = {
        "activity_history.json",
        "currently_watching.json",
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
        "emby.health.shadow.json",
        "plex_history.default.phantoms.json",
        "simkl_history.unscoped.flap.json",
    }
    for name in scoped | unrelated:
        (state_dir / name).write_text("{}", encoding="utf-8")
    return scoped, unrelated


def test_rebuild_sync_state_removes_pair_scoped_history_mapping_files(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    scoped, unrelated = _seed_rebuild_state(state_dir)
    (tmp_path / "state.json").write_text("{}", encoding="utf-8")

    identity_dir = state_dir / "id"
    identity_dir.mkdir()
    (identity_dir / "index.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.clear_state_minimal()

    assert result["ok"] is True
    assert not (tmp_path / "state.json").exists()
    assert set(result["removed_sync_state"]) == scoped
    for name in scoped:
        assert not (state_dir / name).exists()
    for name in unrelated:
        assert (state_dir / name).exists()
    assert (identity_dir / "index.json").exists()


def test_rebuild_sync_state_reports_pair_mapping_metric(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    scoped, _ = _seed_rebuild_state(state_dir)
    (tmp_path / "state.json").write_text("{}", encoding="utf-8")

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    status = maintenanceAPI.maintenance_action_status("state")

    metric = next(m for m in status["metrics"] if m["label"] == "Pair mapping files")
    assert metric["value"] == len(scoped)


def test_crosswatch_tracker_clear_rejects_profile_path_input(tmp_path, monkeypatch) -> None:
    root = tmp_path / ".cw_provider"
    snaps = root / "snapshots"
    profile_root = root / "profiles" / "CW-P01"
    outside = tmp_path / "outside"
    snaps.mkdir(parents=True)
    profile_root.mkdir(parents=True)
    outside.mkdir()
    (root / "watchlist.json").write_text("{}", encoding="utf-8")
    (snaps / "20260101T000000Z-watchlist.json").write_text("{}", encoding="utf-8")
    (profile_root / "watchlist.json").write_text("{}", encoding="utf-8")
    (outside / "watchlist.json").write_text("{}", encoding="utf-8")
    (tmp_path / "config.json").write_text(
        json.dumps({"crosswatch": {"root_dir": str(root), "instances": {"CW-P01": {}}}}),
        encoding="utf-8",
    )

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, tmp_path / ".cw_state", None, None, None),
    )

    result = maintenanceAPI.crosswatch_tracker_clear(
        clear_state=True,
        clear_snapshots=True,
        provider_instance="../outside",
    )

    assert result["ok"] is True
    assert result["provider_instance"] == "default"
    assert not (root / "watchlist.json").exists()
    assert not (snaps / "20260101T000000Z-watchlist.json").exists()
    assert (profile_root / "watchlist.json").exists()
    assert (outside / "watchlist.json").exists()

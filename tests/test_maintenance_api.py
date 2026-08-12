from __future__ import annotations

import json
from types import SimpleNamespace

from api import maintenanceAPI
from cw_platform.local_db.legacy_files import STATE_JSON, STATISTICS_JSON, legacy_path, legacy_root
from cw_platform.orchestrator._state_store import StateStore


def _save_state(base, payload) -> None:
    StateStore(base).save_state(payload)


def _load_state(base) -> dict:
    return StateStore(base).load_state()


def test_clear_provider_cache_preserves_user_runtime_files(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()

    preserved = set()
    sync_recovery_state = {
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
        "activity_history.json",
        "plex_history.default.phantoms.json",
        "plex_history.default.last_success.json",
        "emby.health.shadow.json",
        "trakt_dropped.index.json",
        "currently_watching.json",
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
        "removed_files": 2,
        "removed_items": 0,
        "freed_bytes": 9,
    }
    assert not (state_dir / "currently_watching.json").exists()


def test_clear_state_returns_cleanup_receipt(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    _save_state(tmp_path, {"providers": {"TRAKT": {"watchlist": {"baseline": {"items": {"a": {"title": "A"}}}}}}})

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.clear_state_minimal()

    assert result["ok"] is True
    assert result["existed"] is True
    assert result["summary"]["removed_items"] == 0
    assert _load_state(tmp_path)["providers"] == {}
    assert not legacy_path(tmp_path, STATE_JSON).exists()


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
            SimpleNamespace(path=legacy_path(tmp_path, STATISTICS_JSON)),
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
    _save_state(tmp_path, state)

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (
            tmp_path / "cache",
            tmp_path,
            state_dir,
            SimpleNamespace(path=legacy_path(tmp_path, STATISTICS_JSON)),
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
    _save_state(tmp_path, state)

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (
            tmp_path / "cache",
            tmp_path,
            state_dir,
            SimpleNamespace(path=legacy_path(tmp_path, STATISTICS_JSON)),
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


def test_database_health_action_status_reports_local_db_counts(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    _save_state(
        tmp_path,
        {"providers": {"TRAKT": {"watchlist": {"baseline": {"items": {"a": {"title": "A"}}}}}}},
    )

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (
            tmp_path / "cache",
            tmp_path,
            state_dir,
            SimpleNamespace(path=legacy_path(tmp_path, STATISTICS_JSON)),
            None,
            None,
        ),
    )

    result = maintenanceAPI.maintenance_action_status("database-health")
    metrics = {item["label"]: item["value"] for item in result["metrics"]}

    assert result["ok"] is True
    assert metrics["Integrity"] == "ok"
    assert metrics["Feature baselines"] == 1
    assert metrics["Baseline items"] == 1
    assert metrics["Orphan rows"] == 0


def test_database_health_endpoint_is_read_only(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    _save_state(
        tmp_path,
        {"providers": {"TRAKT": {"watchlist": {"baseline": {"items": {"a": {"title": "A"}}}}}}},
    )

    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.maintenance_database_health()

    assert result["ok"] is True
    assert result["healthy"] is True
    assert result["table_counts"]["provider_feature_state"] == 1
    assert _load_state(tmp_path)["providers"]["TRAKT"]["watchlist"]["baseline"]["items"]["a"]["title"] == "A"


def test_compact_state_file_creates_backup_and_rewrites_database(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    payload = {"providers": {"TRAKT": {"watchlist": {"baseline": {"items": {"a": {"title": "A", "type": "movie", "ids": {}}}}}}}}
    state_path = legacy_path(tmp_path, STATE_JSON)
    _save_state(tmp_path, payload)
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
    assert not state_path.exists()
    assert _load_state(tmp_path)["providers"]["TRAKT"]["watchlist"]["baseline"]["items"]["a"]["title"] == "A"


def test_compact_state_file_returns_missing_without_database_or_legacy_state(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()

    def fail_backup(**_kwargs):
        raise AssertionError("backup should not be created without state")

    import services.backups as backups_svc

    monkeypatch.setattr(backups_svc, "create_backup", fail_backup)
    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.compact_state_file()

    assert result["ok"] is True
    assert result["existed"] is False


def test_prune_state_file_reports_stale_baselines(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    payload = {
        "providers": {
            "TRAKT": {"watchlist": {"baseline": {"items": {"a": {}}}}},
            "SIMKL": {"history": {"baseline": {"items": {"b": {}}}}},
            "PLEX": {"history": {"baseline": {"items": {"c": {}}}}},
        }
    }
    _save_state(tmp_path, payload)
    cfg = {"pairs": [{"source": "SIMKL", "target": "TRAKT"}]}

    monkeypatch.setattr(maintenanceAPI, "_load_config_for_state_prune", lambda _config_dir: cfg)
    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.maintenance_action_status("state-file-prune")
    metrics = {item["label"]: item["value"] for item in result["metrics"]}

    assert result["ok"] is True
    assert metrics["Stale providers"] == 1
    assert metrics["Stale baselines"] == 1
    assert metrics["Stale items"] == 1


def test_prune_state_file_creates_backup_and_removes_unconfigured_state(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    payload = {
        "providers": {
            "TRAKT": {"watchlist": {"baseline": {"items": {"keep-trakt": {}}}}},
            "SIMKL": {"history": {"baseline": {"items": {"keep-simkl": {}}}}},
            "PLEX": {"history": {"baseline": {"items": {"drop-plex": {}}}}},
            "JELLYFIN": {
                "instances": {
                    "home": {"history": {"baseline": {"items": {"keep-jf": {}}}}},
                    "old": {"history": {"baseline": {"items": {"drop-jf": {}}}}},
                }
            },
            "MDBLIST": {"ratings": {"baseline": {"items": {"keep-mdblist": {}}}}},
        }
    }
    state_path = legacy_path(tmp_path, STATE_JSON)
    _save_state(tmp_path, payload)
    cfg = {
        "pairs": [{"source": "SIMKL", "target": "TRAKT"}],
        "scrobble": {
            "watch": {
                "routes": [
                    {"provider": "jellyfin", "provider_instance": "home", "sink": "mdblist"}
                ]
            }
        },
    }
    backups: list[dict] = []

    def fake_backup(**kwargs):
        backups.append(kwargs)
        return {"ok": True, "path": "2026/pre-state-prune.zip"}

    import services.backups as backups_svc

    monkeypatch.setattr(backups_svc, "create_backup", fake_backup)
    monkeypatch.setattr(maintenanceAPI, "_load_config_for_state_prune", lambda _config_dir: cfg)
    monkeypatch.setattr(
        maintenanceAPI,
        "_cw",
        lambda: (tmp_path / "cache", tmp_path, state_dir, None, None, None),
    )

    result = maintenanceAPI.prune_state_file()
    pruned = _load_state(tmp_path)

    assert result["ok"] is True
    assert result["backup"]["path"] == "2026/pre-state-prune.zip"
    assert backups and backups[0]["trigger"] == "maintenance_state_prune"
    assert result["removed"]["removed_providers"] == 1
    assert result["removed"]["removed_instances"] == 1
    assert result["removed"]["removed_baselines"] == 2
    assert result["removed"]["removed_items"] == 2
    assert "PLEX" not in pruned["providers"]
    assert "old" not in pruned["providers"]["JELLYFIN"]["instances"]
    assert "home" in pruned["providers"]["JELLYFIN"]["instances"]
    assert "TRAKT" in pruned["providers"]
    assert "SIMKL" in pruned["providers"]
    assert "MDBLIST" in pruned["providers"]
    assert not state_path.exists()


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
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
        "activity_history.json",
        "simkl_history.unscoped.flap.json",
        "plex_history.default.phantoms.json",
        "emby.health.shadow.json",
        "currently_watching.json",
    }
    preserved = {"anime_mapping_overrides.json"}

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
    moved_legacy = {
        "activity_history.json",
        "currently_watching.json",
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
    }
    provider_runtime = unrelated - moved_legacy
    legacy_path(tmp_path, STATE_JSON).write_text("{}", encoding="utf-8")

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
    assert not legacy_path(tmp_path, STATE_JSON).exists()
    assert set(result["removed_sync_state"]) == scoped
    for name in scoped:
        assert not (state_dir / name).exists()
    for name in provider_runtime:
        assert (state_dir / name).exists()
    for name in moved_legacy:
        assert not (state_dir / name).exists()
        assert (legacy_root(tmp_path) / ".cw_state" / name).exists()
    assert (identity_dir / "index.json").exists()


def test_rebuild_sync_state_reports_pair_mapping_metric(tmp_path, monkeypatch) -> None:
    state_dir = tmp_path / ".cw_state"
    state_dir.mkdir()
    scoped, _ = _seed_rebuild_state(state_dir)
    legacy_path(tmp_path, STATE_JSON).write_text("{}", encoding="utf-8")

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

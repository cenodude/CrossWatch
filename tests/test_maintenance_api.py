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
        "tombstones.json",
        "trakt_history.unresolved.json",
        "plex_history.default.phantoms.json",
        "plex_history.default.last_success.json",
        "emby.health.shadow.json",
        "trakt_dropped.index.json",
    }

    for name in preserved | sync_recovery_state:
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
    (state_dir / "tombstones.json").write_bytes(b"12345")
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

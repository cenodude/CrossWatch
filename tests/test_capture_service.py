from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path


class FakeSyncOps:
    def __init__(self, feature_rows: dict[str, list[dict]]) -> None:
        self.feature_rows = feature_rows

    def is_configured(self, _cfg: dict) -> bool:
        return True

    def features(self) -> dict[str, bool]:
        return {
            "watchlist": "watchlist" in self.feature_rows,
            "ratings": "ratings" in self.feature_rows,
            "history": "history" in self.feature_rows,
            "progress": "progress" in self.feature_rows,
        }

    def build_index(self, _cfg: dict, *, feature: str) -> dict[str, dict]:
        rows = self.feature_rows.get(feature, [])
        return {str(row["id"]): dict(row) for row in rows}


def _patch_snapshot_env(monkeypatch, snapshots, tmp_path: Path, ops, ts: datetime) -> None:
    monkeypatch.setattr(snapshots, "CONFIG", tmp_path)
    monkeypatch.setattr(snapshots, "_utc_now", lambda: ts)
    monkeypatch.setattr(snapshots, "load_sync_ops", lambda provider: ops if provider == "PLEX" else None)
    monkeypatch.setattr(
        snapshots,
        "build_provider_config_view",
        lambda cfg, pid, inst: {"provider": pid, "instance": inst},
    )


def test_single_capture_name(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ts = datetime(2026, 3, 16, 9, 30, 0, tzinfo=timezone.utc)
    ops = FakeSyncOps(
        {
            "watchlist": [
                {"id": "m1", "type": "movie", "title": "Arrival"},
                {"id": "s1", "type": "show", "title": "Dark"},
            ]
        }
    )

    _patch_snapshot_env(monkeypatch, snapshots, tmp_path, ops, ts)

    created = snapshots.create_snapshot("PLEX", "watchlist", cfg={"version": "test"})

    assert created["ok"] is True
    assert created["label"] == "capture"
    assert created["path"].endswith("__watchlist__capture.json")

    saved_file = tmp_path / "snapshots" / created["path"]
    assert saved_file.exists()

    loaded = snapshots.read_snapshot(created["path"])
    assert loaded["feature"] == "watchlist"
    assert loaded["label"] == "capture"
    assert loaded["stats"]["count"] == 2


def test_full_capture_bundle(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ts = datetime(2026, 3, 16, 10, 0, 0, tzinfo=timezone.utc)
    ops = FakeSyncOps(
        {
            "watchlist": [{"id": "m1", "type": "movie", "title": "Heat"}],
            "ratings": [{"id": "m1", "type": "movie", "title": "Heat", "rating": 9}],
            "history": [{"id": "e1", "type": "episode", "title": "Pilot"}],
        }
    )

    _patch_snapshot_env(monkeypatch, snapshots, tmp_path, ops, ts)

    created = snapshots.create_snapshot("PLEX", "all", label="nightly", cfg={"version": "test"})

    assert created["ok"] is True
    assert created["feature"] == "all"
    assert created["label"] == "nightly"
    assert created["stats"]["count"] == 3
    assert created["stats"]["features"] == {"watchlist": 1, "ratings": 1, "history": 1}

    child_paths = {child["feature"]: child["path"] for child in created["children"] if "path" in child}
    assert set(child_paths) == {"watchlist", "ratings", "history"}

    bundle = snapshots.read_snapshot(created["path"])
    assert bundle["feature"] == "all"
    assert len(bundle["children"]) == 3

    listed_paths = {row["path"] for row in snapshots.list_snapshots()}
    assert created["path"] in listed_paths
    assert set(child_paths.values()).issubset(listed_paths)


def test_capture_retention_keeps_newest(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ops = FakeSyncOps({"watchlist": [{"id": "m1", "type": "movie", "title": "Heat"}]})
    _patch_snapshot_env(
        monkeypatch,
        snapshots,
        tmp_path,
        ops,
        datetime(2026, 3, 16, 10, 0, 0, tzinfo=timezone.utc),
    )

    stamps = [
        datetime(2026, 3, 14, 10, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 15, 10, 0, 0, tzinfo=timezone.utc),
        datetime(2026, 3, 16, 10, 0, 0, tzinfo=timezone.utc),
    ]
    paths: list[str] = []
    for ts in stamps:
        monkeypatch.setattr(snapshots, "_utc_now", lambda ts=ts: ts)
        created = snapshots.create_snapshot("PLEX", "watchlist", label=ts.strftime("%Y-%m-%d"), cfg={"version": "test"})
        paths.append(created["path"])

    monkeypatch.setattr(snapshots, "_utc_now", lambda: datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc))
    cleaned = snapshots.enforce_capture_retention(
        "PLEX",
        "watchlist",
        instance_id="default",
        max_captures=2,
        auto_delete_old=True,
    )

    assert cleaned["ok"] is True
    assert paths[0] in cleaned["deleted"]

    remaining = {row["path"] for row in snapshots.list_snapshots()}
    assert paths[0] not in remaining
    assert {paths[1], paths[2]}.issubset(remaining)


def test_delete_all_captures_preserves_unrelated_files(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    monkeypatch.setattr(snapshots, "CONFIG", tmp_path)
    capture_root = tmp_path / "snapshots"
    first_day = capture_root / "2026-03-15"
    second_day = capture_root / "2026-03-16"
    first_day.mkdir(parents=True)
    second_day.mkdir(parents=True)

    capture_paths = [
        first_day / "first.json",
        first_day / "second.json",
        second_day / "third.json",
    ]
    for path in capture_paths:
        path.write_text("{}", encoding="utf-8")

    keep = capture_root / "README.txt"
    keep.write_text("not a capture", encoding="utf-8")

    result = snapshots.delete_all_snapshots()

    assert result["ok"] is True
    assert result["deleted_count"] == 3
    assert result["summary"] == {
        "removed_files": 3,
        "removed_items": 0,
        "freed_bytes": 6,
    }
    assert all(not path.exists() for path in capture_paths)
    assert keep.exists()
    assert not first_day.exists()
    assert not second_day.exists()

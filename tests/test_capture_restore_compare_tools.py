from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest


class MutableSyncOps:
    def __init__(self, current_by_feature: dict[str, dict[str, dict]]) -> None:
        self.current_by_feature = current_by_feature

    def is_configured(self, _cfg: dict) -> bool:
        return True

    def features(self) -> dict[str, bool]:
        return {
            "watchlist": True,
            "ratings": True,
            "history": True,
            "progress": True,
        }

    def build_index(self, _cfg: dict, *, feature: str) -> dict[str, dict]:
        return {
            str(key): dict(value)
            for key, value in (self.current_by_feature.get(feature, {}) or {}).items()
        }

    def add(self, _cfg: dict, items: list[dict], *, feature: str, dry_run: bool = False) -> dict[str, int]:
        if dry_run:
            return {"count": 0}
        bucket = self.current_by_feature.setdefault(feature, {})
        for item in items:
            bucket[str(item["id"])] = dict(item)
        return {"count": len(items)}

    def remove(self, _cfg: dict, items: list[dict], *, feature: str, dry_run: bool = False) -> dict[str, int]:
        if dry_run:
            return {"count": 0}
        bucket = self.current_by_feature.setdefault(feature, {})
        for item in items:
            bucket.pop(str(item["id"]), None)
        return {"count": len(items)}


def _snapshot_path(tmp_path: Path, stamp: str, feature: str, payload: dict) -> str:
    day = stamp[:8]
    rel = f"{day[:4]}-{day[4:6]}-{day[6:8]}/{stamp}__PLEX__default__{feature}__capture.json"
    full = tmp_path / "snapshots" / rel
    full.parent.mkdir(parents=True, exist_ok=True)
    full.write_text(__import__("json").dumps(payload, indent=2), encoding="utf-8")
    return rel.replace("\\", "/")


def _feature_payload(feature: str, items: dict[str, dict]) -> dict:
    return {
        "kind": "snapshot",
        "created_at": datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "provider": "PLEX",
        "instance": "default",
        "feature": feature,
        "label": "capture",
        "stats": {"feature": feature, "count": len(items)},
        "items": items,
        "app_version": "test",
    }


def _bundle_payload(children: list[dict], *, count: int, features: dict[str, int]) -> dict:
    return {
        "kind": "snapshot_bundle",
        "created_at": datetime(2026, 3, 16, 12, 0, 0, tzinfo=timezone.utc).isoformat(),
        "provider": "PLEX",
        "instance": "default",
        "feature": "all",
        "label": "capture",
        "stats": {"feature": "all", "count": count, "features": features},
        "children": children,
        "app_version": "test",
    }


def _patch_ops(monkeypatch, snapshots, tmp_path: Path, ops) -> None:
    monkeypatch.setattr(snapshots, "CONFIG", tmp_path)
    monkeypatch.setattr(snapshots, "load_sync_ops", lambda provider: ops if provider == "PLEX" else None)
    monkeypatch.setattr(
        snapshots,
        "build_provider_config_view",
        lambda cfg, pid, inst: {"provider": pid, "instance": inst},
    )


def test_restore_merge(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ops = MutableSyncOps(
        {
            "watchlist": {
                "keep": {"id": "keep", "type": "movie", "title": "Heat"},
            }
        }
    )
    _patch_ops(monkeypatch, snapshots, tmp_path, ops)

    path = _snapshot_path(
        tmp_path,
        "20260316T120000Z",
        "watchlist",
        _feature_payload(
            "watchlist",
            {
                "keep": {"id": "keep", "type": "movie", "title": "Heat"},
                "add": {"id": "add", "type": "movie", "title": "Arrival"},
            },
        ),
    )

    restored = snapshots.restore_snapshot(path, mode="merge", cfg={"version": "test"})

    assert restored["ok"] is True
    assert restored["removed"] == 0
    assert restored["added"] == 1
    assert set(ops.current_by_feature["watchlist"]) == {"keep", "add"}


def test_restore_clear(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ops = MutableSyncOps(
        {
            "watchlist": {
                "keep": {"id": "keep", "type": "movie", "title": "Heat"},
                "drop": {"id": "drop", "type": "movie", "title": "Old title"},
            }
        }
    )
    _patch_ops(monkeypatch, snapshots, tmp_path, ops)

    path = _snapshot_path(
        tmp_path,
        "20260316T121500Z",
        "watchlist",
        _feature_payload(
            "watchlist",
            {
                "keep": {"id": "keep", "type": "movie", "title": "Heat"},
                "fresh": {"id": "fresh", "type": "movie", "title": "Alien"},
            },
        ),
    )

    restored = snapshots.restore_snapshot(path, mode="clear_restore", cfg={"version": "test"})

    assert restored["ok"] is True
    assert restored["removed"] == 2
    assert restored["added"] == 2
    assert set(ops.current_by_feature["watchlist"]) == {"keep", "fresh"}


def test_compare_changes(tmp_path: Path) -> None:
    import services.snapshots as snapshots

    monkeypatch_payload_a = _feature_payload(
        "watchlist",
        {
            "remove-me": {"id": "remove-me", "type": "movie", "title": "Gone Girl"},
            "change-me": {"id": "change-me", "type": "movie", "title": "Dune", "year": 2021},
        },
    )
    monkeypatch_payload_b = _feature_payload(
        "watchlist",
        {
            "change-me": {"id": "change-me", "type": "movie", "title": "Dune Part Two", "year": 2024},
            "add-me": {"id": "add-me", "type": "movie", "title": "Arrival"},
        },
    )

    path_a = _snapshot_path(tmp_path, "20260316T130000Z", "watchlist", monkeypatch_payload_a)
    path_b = _snapshot_path(tmp_path, "20260316T131000Z", "watchlist", monkeypatch_payload_b)

    snapshots.CONFIG = tmp_path
    diff = snapshots.diff_snapshots(path_a, path_b)

    assert diff["ok"] is True
    assert diff["summary"]["added"] == 1
    assert diff["summary"]["removed"] == 1
    assert diff["summary"]["updated"] == 1
    assert diff["added"][0]["key"] == "add-me"
    assert diff["removed"][0]["key"] == "remove-me"
    assert diff["updated"][0]["key"] == "change-me"


def test_tools_clear(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ops = MutableSyncOps(
        {
            "watchlist": {
                "one": {"id": "one", "type": "movie", "title": "Heat"},
                "two": {"id": "two", "type": "movie", "title": "Alien"},
            }
        }
    )
    _patch_ops(monkeypatch, snapshots, tmp_path, ops)

    cleared = snapshots.clear_provider_features("PLEX", ["watchlist"], cfg={"version": "test"})

    assert cleared["ok"] is True
    assert cleared["results"]["watchlist"]["removed"] == 2
    assert ops.current_by_feature["watchlist"] == {}


def test_restore_bundle(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ops = MutableSyncOps(
        {
            "watchlist": {
                "old-watch": {"id": "old-watch", "type": "movie", "title": "Old Watch"},
            },
            "ratings": {
                "old-rate": {"id": "old-rate", "type": "movie", "title": "Old Rate", "rating": 5},
            },
        }
    )
    _patch_ops(monkeypatch, snapshots, tmp_path, ops)

    watchlist_path = _snapshot_path(
        tmp_path,
        "20260316T140000Z",
        "watchlist",
        _feature_payload("watchlist", {"fresh-watch": {"id": "fresh-watch", "type": "movie", "title": "Arrival"}}),
    )
    ratings_path = _snapshot_path(
        tmp_path,
        "20260316T140000Z",
        "ratings",
        _feature_payload("ratings", {"fresh-rate": {"id": "fresh-rate", "type": "movie", "title": "Heat", "rating": 9}}),
    )
    bundle_path = _snapshot_path(
        tmp_path,
        "20260316T140000Z",
        "all",
        _bundle_payload(
            [
                {"feature": "watchlist", "path": watchlist_path, "stats": {"count": 1}},
                {"feature": "ratings", "path": ratings_path, "stats": {"count": 1}},
            ],
            count=2,
            features={"watchlist": 1, "ratings": 1},
        ),
    )

    restored = snapshots.restore_snapshot(bundle_path, mode="clear_restore", cfg={"version": "test"})

    assert restored["ok"] is True
    assert len(restored["children"]) == 2
    assert {child["feature"] for child in restored["children"]} == {"watchlist", "ratings"}
    assert set(ops.current_by_feature["watchlist"]) == {"fresh-watch"}
    assert set(ops.current_by_feature["ratings"]) == {"fresh-rate"}


def test_compare_guardrails(tmp_path: Path) -> None:
    import services.snapshots as snapshots

    snapshots.CONFIG = tmp_path
    left = _snapshot_path(
        tmp_path,
        "20260316T150000Z",
        "watchlist",
        _feature_payload("watchlist", {"one": {"id": "one", "type": "movie", "title": "Heat"}}),
    )

    right_provider = "2026-03-16/20260316T151000Z__TRAKT__default__watchlist__capture.json"
    right_provider_full = tmp_path / "snapshots" / right_provider
    right_provider_full.parent.mkdir(parents=True, exist_ok=True)
    right_provider_full.write_text(
        __import__("json").dumps(
            {
                **_feature_payload("watchlist", {"one": {"id": "one", "type": "movie", "title": "Heat"}}),
                "provider": "TRAKT",
            },
            indent=2,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="same provider and instance"):
        snapshots.diff_snapshots(left, right_provider)

    right_feature = _snapshot_path(
        tmp_path,
        "20260316T152000Z",
        "ratings",
        _feature_payload("ratings", {"one": {"id": "one", "type": "movie", "title": "Heat", "rating": 9}}),
    )

    with pytest.raises(ValueError, match="same feature"):
        snapshots.diff_snapshots(left, right_feature)


def test_compare_history(tmp_path: Path) -> None:
    import services.snapshots as snapshots

    snapshots.CONFIG = tmp_path
    path_a = _snapshot_path(
        tmp_path,
        "20260316T160000Z",
        "history",
        _feature_payload(
            "history",
            {
                "tmdb:10@1710583200": {
                    "id": "play-1",
                    "type": "movie",
                    "title": "Arrival",
                    "ids": {"tmdb": 10},
                    "watched_at": "2026-03-16T10:00:00Z",
                }
            },
        ),
    )
    path_b = _snapshot_path(
        tmp_path,
        "20260316T161000Z",
        "history",
        _feature_payload(
            "history",
            {
                "tmdb:10@1710583200": {
                    "id": "play-1",
                    "type": "movie",
                    "title": "Arrival",
                    "ids": {"tmdb": 10},
                    "watched_at": "2026-03-16T10:00:00Z",
                },
                "tmdb:10@1710586800": {
                    "id": "play-2",
                    "type": "movie",
                    "title": "Arrival",
                    "ids": {"tmdb": 10},
                    "watched_at": "2026-03-16T11:00:00Z",
                },
            },
        ),
    )

    diff = snapshots.diff_snapshots(path_a, path_b)

    assert diff["ok"] is True
    assert diff["summary"]["updated"] == 1
    assert diff["summary"]["added"] == 0
    assert diff["summary"]["removed"] == 0
    assert diff["updated"][0]["key"] == "tmdb:10"
    assert any(change["path"] == "watched_ats.added" for change in diff["updated"][0]["changes"])


def test_tools_skip_plex_progress(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    ops = MutableSyncOps(
        {
            "progress": {
                "ep1": {"id": "ep1", "type": "episode", "title": "Pilot", "progress": 87},
            }
        }
    )
    _patch_ops(monkeypatch, snapshots, tmp_path, ops)

    cleared = snapshots.clear_provider_features("PLEX", ["progress"], cfg={"version": "test"})

    assert cleared["ok"] is True
    assert cleared["results"]["progress"] == {"ok": True, "skipped": True, "reason": "unsupported_clear"}
    assert set(ops.current_by_feature["progress"]) == {"ep1"}

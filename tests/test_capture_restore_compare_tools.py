from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import json
import os
import time

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


def test_restore_background_job_tracks_progress(tmp_path: Path, monkeypatch) -> None:
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
        "20260316T122000Z",
        "watchlist",
        _feature_payload(
            "watchlist",
            {
                "keep": {"id": "keep", "type": "movie", "title": "Heat"},
                "add": {"id": "add", "type": "movie", "title": "Arrival"},
            },
        ),
    )

    job = snapshots.start_restore_job(path, mode="merge", cfg={"version": "test"}, progress_id="restore-job")

    assert job["ok"] is True
    progress = None
    for _ in range(50):
        progress = snapshots.get_capture_progress("restore-job")
        if progress and progress.get("done"):
            break
        time.sleep(0.02)

    assert progress is not None
    assert progress["done"] is True
    assert progress["operation"] == "restore"
    assert progress["added"] == 1
    assert progress["removed"] == 0
    assert progress["restore_result"]["ok"] is True
    assert set(ops.current_by_feature["watchlist"]) == {"keep", "add"}


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


def test_tools_clear_plex_progress(tmp_path: Path, monkeypatch) -> None:
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
    assert cleared["results"]["progress"]["removed"] == 1
    assert ops.current_by_feature["progress"] == {}


def test_provider_cleanup_crosswatch_progress_persists_empty_state(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots
    from cw_platform.id_map import canonical_key, minimal as id_minimal

    snapshots.CONFIG = tmp_path
    root = tmp_path / "cw"
    item = {"type": "movie", "title": "Heat", "ids": {"tmdb": 949}, "progress_ms": 120000, "duration_ms": 600000}
    key = canonical_key(id_minimal(item))
    assert key
    root.mkdir(parents=True)
    (root / "progress.json").write_text(json.dumps({"ts": 1, "items": {key: item}}), encoding="utf-8")
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setenv("CW_CAPTURE_PROVIDER", "PLEX")

    cleared = snapshots.clear_provider_features("CROSSWATCH", ["progress"], cfg={"version": "test", "crosswatch": {"root_dir": str(root)}})

    assert cleared["ok"] is True
    assert cleared["results"]["progress"]["removed"] == 1
    assert cleared["results"]["progress"]["remaining"] == 0
    assert os.environ.get("CW_CAPTURE_MODE") == "1"
    saved = json.loads((root / "progress.json").read_text(encoding="utf-8"))
    assert saved["items"] == {}


def test_restore_crosswatch_progress_persists_state(tmp_path: Path, monkeypatch) -> None:
    import services.snapshots as snapshots

    snapshots.CONFIG = tmp_path
    root = tmp_path / "cw"
    root.mkdir(parents=True)
    rel = "2026-03-16/20260316T180000Z__CROSSWATCH__default__progress__capture.json"
    path = tmp_path / "snapshots" / rel
    item = {"type": "movie", "title": "Arrival", "ids": {"tmdb": 329865}, "progress_ms": 300000, "duration_ms": 700000}
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "kind": "snapshot",
                "created_at": datetime(2026, 3, 16, 18, 0, 0, tzinfo=timezone.utc).isoformat(),
                "provider": "CROSSWATCH",
                "instance": "default",
                "feature": "progress",
                "label": "capture",
                "stats": {"feature": "progress", "count": 1},
                "items": {"arrival": item},
                "app_version": "test",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("CW_CAPTURE_MODE", "1")
    monkeypatch.setenv("CW_CAPTURE_PROVIDER", "PLEX")

    restored = snapshots.restore_snapshot(rel, mode="merge", cfg={"version": "test", "crosswatch": {"root_dir": str(root)}})

    assert restored["ok"] is True
    assert restored["added"] == 1
    assert os.environ.get("CW_CAPTURE_MODE") == "1"
    saved = json.loads((root / "progress.json").read_text(encoding="utf-8"))
    assert len(saved["items"]) == 1
    assert next(iter(saved["items"].values()))["title"] == "Arrival"


def test_provider_cleanup_background_job_tracks_progress(tmp_path: Path, monkeypatch) -> None:
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

    job = snapshots.start_provider_cleanup_job("PLEX", ["watchlist"], cfg={"version": "test"}, progress_id="cleanup-job")

    assert job["ok"] is True
    progress = None
    for _ in range(50):
        progress = snapshots.get_capture_progress("cleanup-job")
        if progress and progress.get("done"):
            break
        time.sleep(0.02)

    assert progress is not None
    assert progress["done"] is True
    assert progress["total_items"] == 2
    assert progress["cleanup_results"]["watchlist"]["removed"] == 2
    assert ops.current_by_feature["watchlist"] == {}


def test_provider_cleanup_modal_owns_provider_cleanup_ui() -> None:
    from pathlib import Path

    root = Path(__file__).resolve().parents[1]
    snapshots_js = (root / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    modal_js = (root / "assets" / "js" / "modals" / "provider-cleanup" / "index.js").read_text(encoding="utf-8")
    modals_js = (root / "assets" / "js" / "modals.js").read_text(encoding="utf-8")
    settings_py = (root / "ui_frontend.py").read_text(encoding="utf-8")
    profile_select_js = (root / "assets" / "helpers" / "profile-select.js").read_text(encoding="utf-8")

    assert "ss-tools-prov" not in snapshots_js
    assert "ss-tools-inst" not in snapshots_js
    assert "ss-clear-watchlist" not in snapshots_js
    assert "ss-clear-progress" not in snapshots_js
    assert "/api/snapshots/tools/clear" not in snapshots_js

    assert "/api/snapshots/tools/clear" in modal_js
    assert "/api/snapshots/manifest" in modal_js
    assert '"progress"' in modal_js
    assert "data-feature" in modal_js
    assert "Provider Cleanup" in settings_py
    assert "openProviderCleanupModal" in modals_js
    assert "openProviderCleanupModal()" in settings_py
    assert "profile-select.js" in settings_py
    assert "CW.ProfileSelect" in profile_select_js
    assert "enhancePair" in modal_js
    assert "ccc-provider-btn" not in modal_js
    assert "ccc-instance-btn" not in modal_js


def test_snapshots_ui_retries_after_auth_setup() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")

    assert "function retryInitAfterAuth()" in js
    assert "page && !page.children.length ? init() : refresh(!!force)" in js


def test_captures_ui_compare_selection_validation() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")

    assert "function compareSelectionState()" in js
    assert "selected.length !== 2" in js
    assert "Selected captures use different providers." in js
    assert "Selected captures use different profiles." in js
    assert "Selected captures use different features." in js
    assert "Comparison enabled" in js


def test_captures_ui_compare_summary_uses_icons() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    summary_body = js[js.index('<div class="ss-diff-summary">') : js.index("async function onDiffRun()")]

    assert "material-symbols-rounded" in summary_body
    assert "justify-content:center" in js
    assert ">added</span>" not in summary_body
    assert ">deleted</span>" not in summary_body
    assert ">updated</span>" not in summary_body
    assert ">unchanged</span>" not in summary_body
    assert 'aria-label="${sum.added ?? 0} added"' in summary_body


def test_captures_ui_compare_opens_advanced_modal() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    compare_markup = js[js.index('<div class="ss-compare-panel">') : js.index('<div id="ss-diff-out"')]
    run_body = js[js.index("async function onDiffRun()") : js.index("  function setBusy(")]

    assert "Compare Captures" in compare_markup
    assert "Compare selected captures" not in compare_markup
    assert "Open advanced" not in compare_markup
    assert "ss-diff-kind" not in js
    assert "ss-diff-limit" not in js
    assert "ss-diff-q" not in js
    assert "ss-diff-list" not in js
    assert "Filter compare results" not in js
    assert "/api/snapshots/diff" not in run_body
    assert "window.openCaptureCompare({ aPath: a, bPath: b });" in run_body


def test_captures_ui_details_toggle_and_scrollbar() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    details_body = js[js.index("async function onViewDetails()") : js.index("  async function onRestore()")]

    assert ".ss-detail-pre{scrollbar-width:thin" in js
    assert ".ss-detail-pre::-webkit-scrollbar-thumb" in js
    assert 'if (!out.classList.contains("hidden")) {' in details_body
    assert 'out.classList.add("hidden");' in details_body
    assert 'out.textContent = "";' in details_body


def test_captures_ui_scheduler_queue_hides_when_empty() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    queue_body = js[js.index("function renderScheduleQueue()") : js.index("  function setScheduleQueueFeedback(")]

    assert 'id="ss-schedule-queue-wrap" class="ss-queue hidden"' in js
    assert 'wrap.classList.toggle("hidden", !schedulerAvailable() || !items.length);' in queue_body
    assert 'host.innerHTML = "";' in queue_body
    assert '.join(" / ")' in queue_body
    assert "Ã" not in queue_body


def test_captures_ui_tools_use_flat_danger_and_cleanup_label() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")

    assert "Cleanup Captures" in js
    assert "Cleanup old captures" not in js
    assert "#page-snapshots .ss-tool-btn.danger{background:#432630!important" in js
    assert "color:#d99aa4" in js


def test_captures_ui_restore_mode_handling() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")

    assert 'data-restore-mode="merge"' in js
    assert 'data-restore-mode="clear_restore"' in js
    assert "Merge missing only" in js
    assert "Replace exactly" in js
    assert 'if (sel) sel.value = mode;' in js
    assert 'mode !== "clear_restore"' in js
    assert "Confirm restore" in js
    assert 'background: true' in js


def test_restore_progress_modal_requires_acknowledgement() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    restore_body = js[js.index("async function onRestore()") : js.index("  async function init()")]

    assert 'id="ss-capture-ack"' in js
    assert 'hideCaptureProgressModal(0);' in js
    assert "window.setTimeout(() => refresh(true, false), 80);" in js
    assert "hideCaptureProgressModal(" not in restore_body
    assert "stopCaptureProgressPoll();" in restore_body


def test_capture_progress_modal_requires_acknowledgement() -> None:
    from pathlib import Path

    js = (Path(__file__).resolve().parents[1] / "assets" / "js" / "snapshots.js").read_text(encoding="utf-8")
    create_body = js[js.index("async function onCreate()") : js.index("  async function onAddToScheduler()")]

    assert 'actions.classList.toggle("show", !!data.done || failed);' in js
    assert 'hideCaptureProgressModal(0);' in js
    assert "window.setTimeout(() => refresh(true, false), 80);" in js
    assert "hideCaptureProgressModal(" not in create_body
    assert 'state.captureProgressId = "";' not in create_body

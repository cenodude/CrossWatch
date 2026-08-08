# cw_platform/local_db/legacy_files.py
# CrossWatch - Legacy JSON filenames for SQLite-managed data
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from pathlib import Path

STATE_JSON = "state.json"
STATE_MANUAL_JSON = "state.manual.json"
LAST_SYNC_JSON = "last_sync.json"
STATISTICS_JSON = "statistics.json"
WATCHLIST_HIDE_JSON = "watchlist_hide.json"
SYNC_REPORTS_DIR = "sync_reports"
LEGACY_DIR = "legacy"
CW_STATE_LEGACY_ARTIFACTS = frozenset(
    {
        "activity_history.json",
        "currently_watching.json",
        "auto_remove_seen.json",
        "watchlist_wl_autoremove.json",
    }
)

DB_MANAGED_ARTIFACTS = frozenset(
    {
        STATE_JSON,
        STATE_MANUAL_JSON,
        LAST_SYNC_JSON,
        STATISTICS_JSON,
        WATCHLIST_HIDE_JSON,
    }
)


def legacy_path(base_path: str | Path, filename: str) -> Path:
    return Path(base_path) / filename


def legacy_root(base_path: str | Path) -> Path:
    return Path(base_path) / LEGACY_DIR


def _available_target(target: Path) -> Path:
    if not target.exists():
        return target
    stem = target.stem if target.suffix else target.name
    suffix = target.suffix
    parent = target.parent
    for i in range(1, 1000):
        candidate = parent / (f"{stem}.{i}{suffix}" if suffix else f"{stem}.{i}")
        if not candidate.exists():
            return candidate
    return parent / f"{target.name}.{id(target)}"


def _move_artifact(src: Path, target: Path) -> dict[str, str] | None:
    if not src.exists():
        return None
    dst = _available_target(target)
    dst.parent.mkdir(parents=True, exist_ok=True)
    src.replace(dst)
    return {"source": str(src), "target": str(dst)}


def move_legacy_artifacts(base_path: str | Path) -> list[dict[str, str]]:
    base = Path(base_path)
    legacy = legacy_root(base)
    moved: list[dict[str, str]] = []
    for name in sorted(DB_MANAGED_ARTIFACTS):
        item = _move_artifact(base / name, legacy / name)
        if item:
            moved.append(item)
    item = _move_artifact(base / SYNC_REPORTS_DIR, legacy / SYNC_REPORTS_DIR)
    if item:
        moved.append(item)
    for name in sorted(CW_STATE_LEGACY_ARTIFACTS):
        item = _move_artifact(base / ".cw_state" / name, legacy / ".cw_state" / name)
        if item:
            moved.append(item)
    return moved

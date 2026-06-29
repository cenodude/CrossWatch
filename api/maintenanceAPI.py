# /api/maintenanceAPI.py
# CrossWatch - Maintenance API for CrossWatch
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import shutil
import threading
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body

router = APIRouter(prefix="/api/maintenance", tags=["maintenance"])

CW_STATE_KEEP_DIRS = {"id"}
CW_STATE_KEEP_FILES = {
    "activity_history.json",
    "currently_watching.json",
    "auto_remove_seen.json",
    "watchlist_wl_autoremove.json",
}


def _cw() -> tuple[Any, Any, Any, Any, Any, Any]:
    from .syncAPI import _load_state
    from crosswatch import CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _append_log

    return CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log


def _safe_remove_path(p: Path) -> bool:
    try:
        if p.is_dir():
            shutil.rmtree(p, ignore_errors=True)
        elif p.exists():
            p.unlink(missing_ok=True)
        return True
    except Exception:
        return False


def _clear_cw_state_files() -> list[str]:
    _, _, CW_STATE_DIR, *_ = _cw()
    removed: list[str] = []
    if not CW_STATE_DIR.exists():
        return removed
    for p in CW_STATE_DIR.iterdir():
        if p.is_dir():
            # Preserve identity cache (.cw_state/id)
            if p.name in CW_STATE_KEEP_DIRS:
                continue
            continue
        if p.is_file():
            if p.name in CW_STATE_KEEP_FILES:
                continue
            try:
                p.unlink(missing_ok=True)
                removed.append(p.name)
            except Exception:
                pass
    return removed


# --- CrossWatch tracker (.cw_provider) functions ---
def _cw_tracker_root(config_dir: Path) -> Path:
    """Resolve CrossWatch tracker root dir from config.json or default."""
    cfg_path = config_dir / "config.json"
    root: str | None = None
    try:
        cfg = json.loads(cfg_path.read_text("utf-8"))
        cw_cfg = cfg.get("crosswatch") or {}
        root = (
            cw_cfg.get("root_dir")
            or cw_cfg.get("root")
            or cw_cfg.get("dir")
            or None
        )
    except Exception:
        root = None

    if not root:
        root = ".cw_provider"

    p = Path(root)
    if not p.is_absolute():
        p = config_dir / p
    return p


def _file_meta(path: Path) -> dict[str, Any]:
    try:
        st = path.stat()
    except Exception:
        return {"name": path.name, "size": 0, "mtime": None}
    return {
        "name": path.name,
        "size": st.st_size,
        "mtime": datetime.utcfromtimestamp(st.st_mtime).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }


def _path_usage(path: Path) -> dict[str, int | None]:
    """Return recursive file count, byte size and newest mtime."""
    files = 0
    size = 0
    newest: int | None = None
    if not path.exists():
        return {"files": 0, "bytes": 0, "modified": None}

    candidates: list[Path]
    if path.is_file():
        candidates = [path]
    else:
        candidates = []
        try:
            for base, _, names in os.walk(path, followlinks=False):
                candidates.extend(Path(base) / name for name in names)
        except Exception:
            pass

    for candidate in candidates:
        try:
            if candidate.is_symlink() or not candidate.is_file():
                continue
            stat = candidate.stat()
            files += 1
            size += int(stat.st_size)
            stamp = int(stat.st_mtime)
            newest = stamp if newest is None else max(newest, stamp)
        except Exception:
            continue
    return {"files": files, "bytes": size, "modified": newest}


def _paths_usage(paths: list[Path]) -> dict[str, int | None]:
    usages = [_path_usage(path) for path in paths]
    modified = [int(item["modified"]) for item in usages if item["modified"]]
    return {
        "files": sum(int(item["files"] or 0) for item in usages),
        "bytes": sum(int(item["bytes"] or 0) for item in usages),
        "modified": max(modified, default=None),
    }


def _cleanup_summary(
    before: dict[str, int | None],
    after: dict[str, int | None],
    *,
    removed_items: int = 0,
) -> dict[str, int]:
    """Return receipt for destructive maintenance actions."""
    return {
        "removed_files": max(0, int(before.get("files") or 0) - int(after.get("files") or 0)),
        "removed_items": max(0, int(removed_items or 0)),
        "freed_bytes": max(0, int(before.get("bytes") or 0) - int(after.get("bytes") or 0)),
    }


def _statistics_artifact_paths(
    cache_dir: Path,
    config_dir: Path,
    state_dir: Path,
    stats_path: Path,
    *,
    include_stats: bool,
    include_state: bool,
    include_reports: bool,
    include_insights: bool,
) -> list[Path]:
    paths: list[Path] = []
    if include_stats:
        paths.append(stats_path)
    if include_state:
        paths.append(config_dir / "state.json")
    if include_reports:
        try:
            from services.statistics import REPORT_DIR
        except Exception:
            REPORT_DIR = config_dir / "sync_reports"
        paths.extend(Path(REPORT_DIR).glob("sync-*.json"))
    if include_insights:
        for root in (state_dir, cache_dir, config_dir):
            if not root.exists():
                continue
            for pattern in ("insights*.json", ".insights*.json", "insight*.json", "series*.json"):
                paths.extend(root.glob(pattern))
    return list(dict.fromkeys(paths))


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return None


def _metric(label: str, value: Any, fmt: str = "number") -> dict[str, Any]:
    return {"label": label, "value": value, "format": fmt}


def _sync_state_inventory(path: Path) -> tuple[int, int]:
    payload = _read_json(path)
    providers = payload.get("providers") if isinstance(payload, dict) else None
    if not isinstance(providers, dict):
        return 0, 0

    baseline_count = 0
    feature_names = {"history", "ratings", "watchlist", "playlists", "progress"}
    for provider in providers.values():
        if not isinstance(provider, dict):
            continue
        for name, block in provider.items():
            if str(name).lower() not in feature_names or not isinstance(block, dict):
                continue
            if block.get("baseline") is not None or block:
                baseline_count += 1
    return len(providers), baseline_count


def _currently_playing_count(path: Path) -> int:
    payload = _read_json(path)
    if not isinstance(payload, dict):
        return 0
    streams = payload.get("streams")
    if isinstance(streams, dict):
        return len(streams)
    return 1 if payload.get("title") else 0


def maintenance_action_status(action: str) -> dict[str, Any]:
    """Build the read-only inventory shown when a maintenance action is selected."""
    CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log = _cw()
    action = str(action or "").strip().lower()
    response: dict[str, Any] = {"ok": True, "action": action, "metrics": []}

    if action == "state":
        path = CONFIG_DIR / "state.json"
        usage = _path_usage(path)
        providers, baselines = _sync_state_inventory(path)
        response.update(
            title="Rebuild sync state",
            note="These provider baselines are removed; the next sync reads fresh provider data.",
            metrics=[
                _metric("Providers", providers),
                _metric("Feature baselines", baselines),
                _metric("State storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "cache":
        info = _scan_provider_cache()
        files = list(info.get("files") or [])
        retry_names = ("unresolved", "phantom", "tombstone", "blackbox", "flap", "health", "dropped")
        retry_files = sum(1 for item in files if any(token in str(item.get("name") or "").lower() for token in retry_names))
        response.update(
            title="Retry provider items",
            note="Clears retry guards, tombstones, phantom records and provider health state. Playback and activity files stay untouched.",
            metrics=[
                _metric("Runtime files", len(files)),
                _metric("Retry / guard files", retry_files),
                _metric("Storage", sum(int(item.get("size") or 0) for item in files), "bytes"),
                _metric("Preserved files", len(CW_STATE_KEEP_FILES)),
            ],
        )
    elif action == "tracker":
        root = _cw_tracker_root(CONFIG_DIR)
        info = _scan_cw_tracker(root)
        usage = _path_usage(root)
        counts = info.get("counts") or {}
        response.update(
            title="Reset local tracker",
            note="Tracker state and saved tracker snapshots can be selected independently with the checkboxes on the card.",
            metrics=[
                _metric("Tracker states", counts.get("state_files", 0)),
                _metric("Tracker snapshots", counts.get("snapshots", 0)),
                _metric("Storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "playing":
        path = CW_STATE_DIR / "currently_watching.json"
        usage = _path_usage(path)
        response.update(
            title="Clear currently playing",
            note="Only CrossWatch's local live-playback sessions are affected; provider playback history is not changed.",
            metrics=[
                _metric("Active sessions", _currently_playing_count(path)),
                _metric("Storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "scrobbles":
        from services.activity import activity_path, list_events

        events = list_events(limit=1, kind="scrobble", group_routes=False)
        usage = _path_usage(activity_path())
        response.update(
            title="Clear Recent Scrobbles",
            note="Removes only scrobble rows from Recent Activity. Other activity and provider watch history remain.",
            metrics=[
                _metric("Scrobble rows", int(events.get("total") or 0)),
                _metric("Activity file", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "stats":
        try:
            from services.statistics import REPORT_DIR
        except Exception:
            REPORT_DIR = CONFIG_DIR / "sync_reports"
        stats_path = Path(getattr(STATS, "path", CONFIG_DIR / "statistics.json"))
        stats_usage = _path_usage(stats_path)
        report_paths = list(Path(REPORT_DIR).glob("sync-*.json")) if Path(REPORT_DIR).exists() else []
        report_usage = _paths_usage(report_paths)
        insight_paths: list[Path] = []
        for root in (CW_STATE_DIR, CACHE_DIR, CONFIG_DIR):
            if not root.exists():
                continue
            for pattern in ("insights*.json", ".insights*.json", "insight*.json", "series*.json"):
                insight_paths.extend(root.glob(pattern))
        insight_usage = _paths_usage(list(dict.fromkeys(insight_paths)))
        response.update(
            title="Rebuild statistics",
            note="Statistics, saved sync reports and generated insight caches are rebuilt from future sync activity.",
            metrics=[
                _metric("Sync reports", report_usage["files"]),
                _metric("Insight cache files", insight_usage["files"]),
                _metric("Data rebuilt", int(stats_usage["bytes"] or 0) + int(report_usage["bytes"] or 0) + int(insight_usage["bytes"] or 0), "bytes"),
                _metric("Last report", report_usage["modified"], "datetime"),
            ],
        )
    elif action == "metadata":
        usage = _path_usage(CACHE_DIR)
        response.update(
            title="Refresh artwork & metadata",
            note="All cached artwork and metadata are removed and downloaded again only when CrossWatch needs them.",
            metrics=[
                _metric("Cached files", usage["files"]),
                _metric("Cache storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "captures":
        from services.snapshots import list_snapshots

        rows = list_snapshots()
        providers = {str(row.get("provider") or "").upper() for row in rows if row.get("provider")}
        newest = max((int(row.get("mtime") or 0) for row in rows), default=None)
        response.update(
            title="Clear all captures",
            note="Deletes saved provider captures only; automatic tracker snapshots are stored separately.",
            metrics=[
                _metric("Saved captures", len(rows)),
                _metric("Providers", len(providers)),
                _metric("Capture storage", sum(int(row.get("size") or 0) for row in rows), "bytes"),
                _metric("Latest capture", newest, "datetime"),
            ],
        )
    elif action == "defaults":
        snapshots_dir = CONFIG_DIR / "snapshots"
        paths = [
            CONFIG_DIR / "last_sync.json",
            CONFIG_DIR / "state.json",
            CONFIG_DIR / "statistics.json",
            CONFIG_DIR / "sync_reports",
            CW_STATE_DIR,
            _cw_tracker_root(CONFIG_DIR),
            CACHE_DIR,
            CONFIG_DIR / "tls",
        ]
        usages = [_path_usage(path) for path in paths]
        captures = _path_usage(Path(snapshots_dir))
        response.update(
            title="Factory reset",
            note="Deletes all local runtime data and backs up config.json. Saved captures are kept.",
            metrics=[
                _metric("Files removed", sum(int(item["files"] or 0) for item in usages)),
                _metric("Data removed", sum(int(item["bytes"] or 0) for item in usages), "bytes"),
                _metric("Captures kept", captures["files"]),
                _metric("Capture storage kept", captures["bytes"], "bytes"),
            ],
        )
    else:
        return {"ok": False, "error": "unknown_maintenance_action", "action": action}

    return response

def _scan_provider_cache() -> dict[str, Any]:
    _, _, CW_STATE_DIR, *_ = _cw()
    exists = CW_STATE_DIR.exists()
    out: dict[str, Any] = {
        "exists": exists,
        "root": str(CW_STATE_DIR),
        "files": [],
        "count": 0,
    }
    if not exists:
        return out

    files: list[dict[str, Any]] = []
    try:
        candidates = list(CW_STATE_DIR.iterdir())
    except Exception:
        candidates = []
    for p in candidates:
        if p.name in CW_STATE_KEEP_FILES:
            continue
        if p.is_file():
            files.append(_file_meta(p))

    files.sort(key=lambda x: x.get("name") or "")
    out["files"] = files
    out["count"] = len(files)
    return out


def _scan_cache_dir(cache_dir: Path) -> dict[str, Any]:
    exists = cache_dir.exists()
    out: dict[str, Any] = {
        "exists": exists,
        "root": str(cache_dir),
        "entries": [],
        "count": 0,
    }
    if not exists:
        return out

    entries: list[dict[str, Any]] = []
    try:
        for p in cache_dir.iterdir():
            if p.is_file() or p.is_dir():
                entries.append(_file_meta(p))
    except Exception:
        pass

    entries.sort(key=lambda x: x.get("name") or "")
    out["entries"] = entries
    out["count"] = len(entries)
    return out


def _clear_cache_dir(cache_dir: Path) -> list[str]:
    removed: list[str] = []
    if not cache_dir.exists():
        return removed
    for p in cache_dir.iterdir():
        if _safe_remove_path(p):
            removed.append(p.name)
    return removed


@router.post("/clear-metadata-cache")
def clear_metadata_cache() -> dict[str, Any]:
    CACHE_DIR, *_ = _cw()

    before_usage = _path_usage(CACHE_DIR)
    before = _scan_cache_dir(CACHE_DIR)
    removed = _clear_cache_dir(CACHE_DIR)
    after = _scan_cache_dir(CACHE_DIR)
    after_usage = _path_usage(CACHE_DIR)

    return {
        "ok": True,
        "root": str(CACHE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
        "summary": _cleanup_summary(before_usage, after_usage),
    }


def _scan_cw_tracker(root: Path) -> dict[str, Any]:
    out: dict[str, Any] = {
        "exists": root.exists(),
        "state_files": [],
        "snapshots": [],
        "counts": {"state_files": 0, "snapshots": 0},
    }
    if not root.exists():
        return out

    state_files: list[dict[str, Any]] = []
    for p in root.glob("*.json"):
        if p.is_file():
            state_files.append(_file_meta(p))

    snaps_dir = root / "snapshots"
    snapshots: list[dict[str, Any]] = []
    if snaps_dir.exists():
        for p in snaps_dir.glob("*.json"):
            if p.is_file():
                snapshots.append(_file_meta(p))

    state_files.sort(key=lambda x: x.get("name") or "")
    snapshots.sort(key=lambda x: x.get("mtime") or "")
    core_names = {"history.json", "ratings.json", "watchlist.json"}
    state_count = sum(
        1 for f in state_files
        if (f.get("name") or "") in core_names
    )

    out["state_files"] = state_files
    out["snapshots"] = snapshots
    out["counts"] = {
        "state_files": state_count,
        "snapshots": len(snapshots),
    }
    return out


@router.get("/crosswatch-tracker")
def crosswatch_tracker_status() -> dict[str, Any]:
    """Inspect CrossWatch tracker folder (.cw_provider)."""
    _, CONFIG_DIR, *_ = _cw()
    root = _cw_tracker_root(CONFIG_DIR)
    info = _scan_cw_tracker(root)
    return {
        "ok": True,
        "root": str(root),
        **info,
    }


@router.post("/clear-state")
def clear_state_minimal() -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    state_path = CONFIG_DIR / "state.json"
    existed = state_path.exists()
    before_usage = _path_usage(state_path)
    try:
        state_path.unlink(missing_ok=True)
        after_usage = _path_usage(state_path)
        return {
            "ok": True,
            "path": str(state_path),
            "existed": bool(existed),
            "summary": _cleanup_summary(before_usage, after_usage),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": "clear_state_failed",
            "path": str(state_path),
            "existed": bool(existed),
        }


@router.post("/crosswatch-tracker/clear")
def crosswatch_tracker_clear(
    clear_state: bool = Body(True),
    clear_snapshots: bool = Body(False),
) -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    root = _cw_tracker_root(CONFIG_DIR)

    before = _scan_cw_tracker(root)
    selected_before_paths = []
    if clear_state:
        selected_before_paths.extend(root.glob("*.json"))
    if clear_snapshots:
        selected_before_paths.extend((root / "snapshots").glob("*.json"))
    before_usage = _paths_usage(list(selected_before_paths))
    removed_state: list[str] = []
    removed_snapshots: list[str] = []

    if clear_state and root.exists():
        for p in root.glob("*.json"):
            if p.is_file() and _safe_remove_path(p):
                removed_state.append(p.name)

    if clear_snapshots:
        snaps_dir = root / "snapshots"
        if snaps_dir.exists():
            for p in snaps_dir.glob("*.json"):
                if p.is_file() and _safe_remove_path(p):
                    removed_snapshots.append(p.name)

    after = _scan_cw_tracker(root)
    selected_after_paths = []
    if clear_state:
        selected_after_paths.extend(root.glob("*.json"))
    if clear_snapshots:
        selected_after_paths.extend((root / "snapshots").glob("*.json"))
    after_usage = _paths_usage(list(selected_after_paths))

    return {
        "ok": True,
        "root": str(root),
        "removed": {
            "state_files": removed_state,
            "snapshots": removed_snapshots,
        },
        "before": before,
        "after": after,
        "summary": _cleanup_summary(before_usage, after_usage),
    }


@router.post("/clear-cache")
def clear_cache() -> dict[str, Any]:
    _, _, CW_STATE_DIR, *_ = _cw()

    before = _scan_provider_cache()
    before_usage = {
        "files": len(before.get("files") or []),
        "bytes": sum(int(item.get("size") or 0) for item in before.get("files") or []),
        "modified": None,
    }
    removed = _clear_cw_state_files()
    after = _scan_provider_cache()
    after_usage = {
        "files": len(after.get("files") or []),
        "bytes": sum(int(item.get("size") or 0) for item in after.get("files") or []),
        "modified": None,
    }

    return {
        "ok": True,
        "root": str(CW_STATE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
        "summary": _cleanup_summary(before_usage, after_usage),
    }


@router.post("/clear-provider-sync-cache")
def clear_provider_sync_cache() -> dict[str, Any]:
    """Reset pair baselines and provider runtime cache"""
    state = clear_state_minimal()
    cache = clear_cache()
    summaries = [state.get("summary") or {}, cache.get("summary") or {}]
    return {
        "ok": bool(state.get("ok")) and bool(cache.get("ok")),
        "state": state,
        "cache": cache,
        "summary": {
            key: sum(int(item.get(key) or 0) for item in summaries)
            for key in ("removed_files", "removed_items", "freed_bytes")
        },
    }

@router.get("/provider-cache")
def provider_cache_status() -> dict[str, Any]:
    info = _scan_provider_cache()
    return {"ok": True, **info}


@router.get("/action-status/{action}")
def action_status(action: str) -> dict[str, Any]:
    return maintenance_action_status(action)


@router.post("/reset-all-default")
def reset_all_to_default(payload: dict[str, Any] | None = Body(None)) -> dict[str, Any]:
    _, CONFIG_DIR, *_rest = _cw()

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    report: dict[str, Any] = {
        "ok": True,
        "config_dir": str(CONFIG_DIR),
        "backup": None,
        "removed_files": [],
        "removed_dirs": [],
        "errors": [],
    }

    # Back up config.json first
    cfg_path = CONFIG_DIR / "config.json"
    if cfg_path.exists():
        base = f"config.json.backup_{ts}"
        dst = CONFIG_DIR / base
        i = 1
        while dst.exists():
            dst = CONFIG_DIR / f"{base}_{i}"
            i += 1
        try:
            cfg_path.rename(dst)
            report["backup"] = str(dst)
        except Exception as e:
            report["ok"] = False
            report["errors"].append(f"config_backup_failed: {e}")
            return report

    files = ["last_sync.json", "state.json", "statistics.json"]
    dirs = [".cw_state", "sync_reports", ".cw_provider", "cache", "tls"]

    for name in files:
        p = CONFIG_DIR / name
        if p.exists():
            if _safe_remove_path(p):
                report["removed_files"].append(name)
            else:
                report["ok"] = False
                report["errors"].append(f"remove_failed: {name}")

    for name in dirs:
        p = CONFIG_DIR / name
        if p.exists():
            if _safe_remove_path(p):
                report["removed_dirs"].append(name)
            else:
                report["ok"] = False
                report["errors"].append(f"remove_failed: {name}")

    if report["ok"] and bool((payload or {}).get("restart")):
        report["restart_scheduled"] = True

        def _kill() -> None:
            os._exit(0)

        threading.Timer(0.75, _kill).start()

    return report


@router.post("/restart")
def restart_crosswatch() -> dict[str, Any]:
    _, _, _, _, _, _append_log = _cw()
    try:
        _append_log(
            "TRBL",
            "\x1b[91m[TROUBLESHOOT]\x1b[0m Restart requested via /api/maintenance/restart.",
        )
    except Exception:
        pass

    def _kill() -> None:
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Terminating process for restart.",
            )
        except Exception:
            pass
        os._exit(0)

    threading.Timer(0.75, _kill).start()
    return {"ok": True, "message": "Restart scheduled"}

@router.post("/reset-currently-watching")
def reset_currently_watching() -> dict[str, Any]:
    _, _, CW_STATE_DIR, _, _, _append_log = _cw()
    path = CW_STATE_DIR / "currently_watching.json"
    existed = path.exists()
    before_usage = _path_usage(path)
    try:
        path.unlink(missing_ok=True)
        after_usage = _path_usage(path)
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Reset currently_watching.json (currently playing).",
            )
        except Exception:
            pass
        return {
            "ok": True,
            "path": str(path),
            "existed": bool(existed),
            "summary": _cleanup_summary(before_usage, after_usage),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": "reset_currently_watching_failed",
            "path": str(path),
            "existed": bool(existed),
        }

@router.post("/clear-activity-log")
def clear_activity_log() -> dict[str, Any]:
    try:
        from services.activity import clear_events

        res = clear_events()
        _, _, _, _, _, _append_log = _cw()
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Cleared local activity log.",
            )
        except Exception:
            pass
        return res
    except Exception:
        return {"ok": False, "error": "clear_activity_log_failed"}


@router.post("/clear-recent-scrobbles")
def clear_recent_scrobbles() -> dict[str, Any]:
    try:
        from services.activity import activity_path, clear_scrobble_events

        before_usage = _path_usage(activity_path())
        res = clear_scrobble_events()
        after_usage = _path_usage(activity_path())
        res["summary"] = _cleanup_summary(
            before_usage,
            after_usage,
            removed_items=int(res.get("removed") or 0),
        )
        _, _, _, _, _, _append_log = _cw()
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Cleared local recent scrobbles.",
            )
        except Exception:
            pass
        return res
    except Exception:
        return {"ok": False, "error": "clear_recent_scrobbles_failed"}

# --- statistics reset / recalculation ---
@router.post("/reset-stats")
def reset_stats(
    recalc: bool = Body(False),
    purge_file: bool = Body(False),
    purge_state: bool = Body(False),
    purge_reports: bool = Body(False),
    purge_insights: bool = Body(False),
) -> dict[str, Any]:
    CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_state, _append_log = _cw()

    if not any((recalc, purge_file, purge_state, purge_reports, purge_insights)):
        purge_file = purge_state = purge_reports = purge_insights = True
        recalc = False

    stats_path = Path(getattr(STATS, "path", CONFIG_DIR / "statistics.json"))
    before_usage = _paths_usage(
        _statistics_artifact_paths(
            CACHE_DIR,
            CONFIG_DIR,
            CW_STATE_DIR,
            stats_path,
            include_stats=purge_file,
            include_state=purge_state,
            include_reports=purge_reports,
            include_insights=purge_insights,
        )
    )

    try:
        try:
            from .syncAPI import _summary_reset, _PROVIDER_COUNTS_CACHE, _find_state_path
        except Exception:
            _summary_reset = None
            _PROVIDER_COUNTS_CACHE = None
            _find_state_path = None

        try:
            from crosswatch import LOG_BUFFERS
        except Exception:
            LOG_BUFFERS = {}

        if _summary_reset:
            _summary_reset()

        if isinstance(LOG_BUFFERS, dict):
            LOG_BUFFERS["SYNC"] = []

        if isinstance(_PROVIDER_COUNTS_CACHE, dict):
            _PROVIDER_COUNTS_CACHE["ts"] = 0.0
            _PROVIDER_COUNTS_CACHE["data"] = None

        STATS.reset()

        # --- stats file ---
        if purge_file:
            try:
                STATS.path.unlink(missing_ok=True)
            except Exception:
                pass
            STATS._load()
            STATS._save()

        # --- state.json ---
        if purge_state and _find_state_path:
            try:
                sp = _find_state_path()
                if sp and sp.exists():
                    sp.unlink()
            except Exception:
                pass

        # --- sync-*.json reports ---
        if purge_reports:
            try:
                try:
                    from services.statistics import REPORT_DIR
                except Exception:
                    from pathlib import Path as _P
                    REPORT_DIR = _P("/config/sync_reports")

                for f in REPORT_DIR.glob("sync-*.json"):
                    try:
                        f.unlink()
                    except Exception:
                        pass
            except Exception:
                pass

        # --- insights *.json files ---
        insights_files_dropped = 0
        if purge_insights:
            from pathlib import Path as _P

            roots = [
                p
                for p in (CW_STATE_DIR, CACHE_DIR, CONFIG_DIR)
                if isinstance(p, _P) and p.exists()
            ]
            patterns = ("insights*.json", ".insights*.json", "insight*.json", "series*.json")
            for root in roots:
                for pat in patterns:
                    for f in root.glob(pat):
                        try:
                            f.unlink()
                            insights_files_dropped += 1
                        except Exception:
                            pass

            try:
                from . import insightAPI as IA  # noqa

                for name, obj in list(vars(IA).items()):
                    key = name.lower()
                    if any(s in key for s in ("insight", "series")) and any(
                        s in key for s in ("cache", "memo", "state")
                    ):
                        try:
                            if hasattr(obj, "clear"):
                                obj.clear()  # type: ignore[call-arg]
                            if isinstance(obj, list):
                                obj[:] = []
                        except Exception:
                            pass

                for fn_name in ("reset_insights_cache", "clear_cache"):
                    fn = getattr(IA, fn_name, None)
                    if callable(fn):
                        try:
                            fn()
                        except Exception:
                            pass
            except Exception:
                pass

        # --- recalc from state.json ---
        if recalc:
            try:
                state = _load_state()
                if state:
                    STATS.refresh_from_state(state)
            except Exception:
                pass

        after_usage = _paths_usage(
            _statistics_artifact_paths(
                CACHE_DIR,
                CONFIG_DIR,
                CW_STATE_DIR,
                stats_path,
                include_stats=purge_file,
                include_state=purge_state,
                include_reports=purge_reports,
                include_insights=purge_insights,
            )
        )

        return {
            "ok": True,
            "dropped": {
                "stats_file": bool(purge_file),
                "state_file": bool(purge_state),
                "reports": bool(purge_reports),
                "insights_files": insights_files_dropped,
                "insights_mem": bool(purge_insights),
            },
            "recalculated": bool(recalc),
            "summary": _cleanup_summary(before_usage, after_usage),
        }
    except Exception:
        return {"ok": False, "error": "reset_stats_failed"}

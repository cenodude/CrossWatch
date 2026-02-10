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
    for p in CW_STATE_DIR.glob("*.json"):
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

    before = _scan_cache_dir(CACHE_DIR)
    removed = _clear_cache_dir(CACHE_DIR)
    after = _scan_cache_dir(CACHE_DIR)

    return {
        "ok": True,
        "root": str(CACHE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
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
    try:
        state_path.unlink(missing_ok=True)
        return {
            "ok": True,
            "path": str(state_path),
            "existed": bool(existed),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
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

    return {
        "ok": True,
        "root": str(root),
        "removed": {
            "state_files": removed_state,
            "snapshots": removed_snapshots,
        },
        "before": before,
        "after": after,
    }


@router.post("/clear-cache")
def clear_cache() -> dict[str, Any]:
    _, _, CW_STATE_DIR, *_ = _cw()

    before = _scan_provider_cache()
    removed = _clear_cw_state_files()
    after = _scan_provider_cache()

    return {
        "ok": True,
        "root": str(CW_STATE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
    }

@router.get("/provider-cache")
def provider_cache_status() -> dict[str, Any]:
    info = _scan_provider_cache()
    return {"ok": True, **info}


@router.post("/reset-all-default")
def reset_all_to_default() -> dict[str, Any]:
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
    try:
        path.unlink(missing_ok=True)
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Reset currently_watching.json (currently playing).",
            )
        except Exception:
            pass
        return {"ok": True, "path": str(path), "existed": bool(existed)}
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
            "path": str(path),
            "existed": bool(existed),
        }

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
        }
    except Exception as e:
        return {"ok": False, "error": str(e)}
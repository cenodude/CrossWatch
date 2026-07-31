# cw_platform/tracker_storage.py
# CrossWatch - Local tracker storage cleanup helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
import logging
import os
from pathlib import Path
from typing import Any
import shutil

from .provider_instances import list_instance_ids, normalize_instance_id

_LOG = logging.getLogger("crosswatch.tracker_storage")
_TRACKER_MARKERS = {
    "watchlist.json",
    "history.json",
    "ratings.json",
    "progress.json",
    "playlists.json",
    "snapshots",
    "profiles",
}
_DEFAULT_TRACKER_ROOT = "/config/.cw_provider"


def _configured_instance(cfg: Mapping[str, Any], instance_id: Any = None) -> str:
    requested = normalize_instance_id(instance_id)
    if requested == "default":
        return "default"
    for candidate in list_instance_ids(cfg, "crosswatch"):
        inst = normalize_instance_id(candidate)
        if inst == requested:
            return inst
    return "default"


def _safe_profile_dir(instance_id: Any) -> str:
    raw = normalize_instance_id(instance_id)
    safe = "".join(ch for ch in raw if ch.isalnum() or ch in "._-").strip("._- ")
    return safe or "default"


def _managed_base_root() -> Path:
    return Path(_DEFAULT_TRACKER_ROOT)


def crosswatch_storage_root(cfg: Mapping[str, Any], instance_id: Any = None) -> str:
    inst = _configured_instance(cfg, instance_id)
    base_root = str(_managed_base_root())
    return base_root if inst == "default" else f"{base_root}/profiles/{_safe_profile_dir(inst)}"


def _normalize_under_managed_root(path: Path) -> tuple[Path | None, str]:
    try:
        managed = os.path.realpath(str(_managed_base_root()))
        candidate = os.path.realpath(str(path))
        if os.path.commonpath([managed, candidate]) != managed:
            return None, "outside_tracker_root"
        return Path(candidate), ""
    except Exception:
        return None, "invalid_path"


def _safe_tracker_path(path: Path) -> tuple[bool, str]:
    resolved, reason = _normalize_under_managed_root(path)
    if resolved is None:
        return False, reason

    if not str(resolved).strip():
        return False, "empty_path"
    if resolved == Path(resolved.anchor):
        return False, "refuse_root_path"

    for protected in (Path.home(), Path.cwd()):
        try:
            if resolved == protected.resolve(strict=False):
                return False, "refuse_protected_path"
        except Exception:
            pass

    if not resolved.exists():
        return True, ""
    if not resolved.is_dir():
        return False, "not_directory"

    try:
        children = list(resolved.iterdir())
    except Exception:
        return False, "unreadable_directory"

    if not children:
        return True, ""
    names = {child.name for child in children}
    if names & _TRACKER_MARKERS:
        return True, ""
    if resolved.name in {".cw_provider", "cw_provider"}:
        return True, ""
    if resolved.parent.name == "profiles":
        return True, ""
    return False, "unsafe_tracker_path"


def remove_crosswatch_storage(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    inst = _configured_instance(cfg, instance_id)
    raw_root = crosswatch_storage_root(cfg, inst)
    path = Path(raw_root)
    ok, reason = _safe_tracker_path(path)
    if not ok:
        return {"ok": False, "error": reason, "path": raw_root, "instance": inst}

    try:
        resolved, reason = _normalize_under_managed_root(path)
        if resolved is None:
            return {"ok": False, "error": reason, "path": raw_root, "instance": inst}
        if resolved.exists():
            shutil.rmtree(resolved)
            return {"ok": True, "removed": True, "path": str(resolved), "instance": inst}
        return {"ok": True, "removed": False, "path": str(resolved), "instance": inst}
    except Exception as exc:
        _LOG.warning("CW tracker storage cleanup failed", exc_info=True)
        return {"ok": False, "error": "remove_failed", "path": raw_root, "instance": inst}

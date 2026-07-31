# cw_platform/tracker_storage.py
# CrossWatch - Local tracker storage cleanup helpers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from pathlib import Path
from typing import Any
import shutil

from .provider_instances import get_provider_block, normalize_instance_id

_TRACKER_MARKERS = {
    "watchlist.json",
    "history.json",
    "ratings.json",
    "progress.json",
    "playlists.json",
    "snapshots",
    "profiles",
}


def _configured_crosswatch_block(cfg: Mapping[str, Any], instance_id: Any = None) -> dict[str, Any]:
    block = get_provider_block(cfg, "crosswatch", instance_id)
    if block:
        return block
    raw = cfg.get("crosswatch") or cfg.get("CrossWatch")
    return dict(raw or {}) if isinstance(raw, Mapping) else {}


def crosswatch_storage_root(cfg: Mapping[str, Any], instance_id: Any = None) -> str:
    inst = normalize_instance_id(instance_id)
    block = _configured_crosswatch_block(cfg, inst)
    root = str(block.get("root_dir") or "").strip()
    if root:
        return root
    base = _configured_crosswatch_block(cfg, "default")
    base_root = str(base.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
    return base_root if inst == "default" else f"{base_root.rstrip('/').rstrip(chr(92))}/profiles/{inst}"


def _safe_tracker_path(path: Path) -> tuple[bool, str]:
    try:
        resolved = path.expanduser().resolve(strict=False)
    except Exception:
        return False, "invalid_path"

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
    inst = normalize_instance_id(instance_id)
    raw_root = crosswatch_storage_root(cfg, inst)
    path = Path(raw_root)
    ok, reason = _safe_tracker_path(path)
    if not ok:
        return {"ok": False, "error": reason, "path": raw_root, "instance": inst}

    try:
        resolved = path.expanduser().resolve(strict=False)
        if resolved.exists():
            shutil.rmtree(resolved)
            return {"ok": True, "removed": True, "path": str(resolved), "instance": inst}
        return {"ok": True, "removed": False, "path": str(resolved), "instance": inst}
    except Exception as exc:
        return {"ok": False, "error": "remove_failed", "detail": str(exc), "path": raw_root, "instance": inst}

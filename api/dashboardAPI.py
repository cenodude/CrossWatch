# /api/dashboardAPI.py
# CrossWatch - Main dashboard widget API
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, cast

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from services.dashboard_widgets import dashboard_widgets_payload

try:
    from _logging import log as _base_log

    LOG = _base_log.child("DASH")
except Exception:  # pragma: no cover
    LOG = None

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


def _dashboard_db_stamp(base_path: Any) -> tuple[float, float]:
    try:
        from cw_platform.local_db.db import crosswatch_db_path

        db = Path(str(crosswatch_db_path(base_path)))
        stamp = 0.0
        wal_stamp = 0.0
        try:
            stamp = db.stat().st_mtime
        except OSError:
            pass
        try:
            wal_stamp = db.with_name(db.name + "-wal").stat().st_mtime
        except OSError:
            pass
        return round(stamp, 3), round(wal_stamp, 3)
    except Exception:
        return 0.0, 0.0


def _dashboard_config_stamp(base_path: Any) -> float:
    try:
        return round((Path(base_path) / "config.json").stat().st_mtime, 3)
    except OSError:
        return 0.0


def _dashboard_tracker_stamp(
    base_path: Any,
    cfg: Mapping[str, Any],
    requested: set[str],
) -> list[tuple[str, int, int]]:
    tracker_features = requested & {"history", "ratings", "progress"}
    if not tracker_features:
        return []
    try:
        from cw_platform.provider_instances import normalize_instance_id

        node = cfg.get("crosswatch") if isinstance(cfg, Mapping) else {}
        raw_root = str(node.get("root_dir") or "").strip() if isinstance(node, Mapping) else ""
        root = Path(raw_root or ".cw_provider")
        if not root.is_absolute():
            root = Path(base_path) / root
        roots: list[tuple[str, Path]] = [("default", root)]
        instances = node.get("instances") if isinstance(node, Mapping) else {}
        if isinstance(instances, Mapping):
            for raw_id in instances.keys():
                inst = normalize_instance_id(raw_id)
                if inst != "default":
                    roots.append((inst, root / "profiles" / inst))
        stamp: list[tuple[str, int, int]] = []
        for inst, base in roots:
            for feature in sorted(tracker_features):
                path = base / f"{feature}.json"
                try:
                    st = path.stat()
                    stamp.append((f"{inst}:{feature}", int(st.st_mtime_ns), int(st.st_size)))
                except OSError:
                    stamp.append((f"{inst}:{feature}", 0, 0))
        return stamp
    except Exception:
        return []


def _dashboard_widgets_version(
    base_path: Any,
    *,
    cfg: Mapping[str, Any],
    requested: set[str],
    state_features: set[str],
    profile: str,
    limits: dict[str, int],
) -> str:
    try:
        from cw_platform.local_db import state as sqlite_state
        from cw_platform.local_db import manual_policy as sqlite_manual_policy

        state_fp = sqlite_state.fingerprint(base_path, state_features) if state_features else None
        policy_fp = sqlite_manual_policy.fingerprint(base_path, state_features) if state_features else None
    except Exception:
        state_fp = None
        policy_fp = None
    source = {
        "requested": sorted(requested),
        "state_features": sorted(state_features),
        "profile": str(profile or ""),
        "limits": limits,
        "state": state_fp,
        "policy": policy_fp,
        "db": _dashboard_db_stamp(base_path),
        "config": _dashboard_config_stamp(base_path),
        "tracker": _dashboard_tracker_stamp(base_path, cfg, requested),
    }
    blob = json.dumps(source, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()[:16]


@router.get("/widgets")
def dashboard_widgets(
    request: Request = cast(Request, None),
    history_limit: int = Query(8, ge=1, le=24),
    ratings_limit: int = Query(12, ge=1, le=24),
    scrobble_limit: int = Query(8, ge=1, le=24),
    progress_limit: int = Query(8, ge=1, le=24),
    playlists_limit: int = Query(8, ge=1, le=24),
    include: str = Query("history,ratings,scrobble,progress,playlists"),
    user_profile: str = Query("", alias="user_profile"),
    known_version: str = Query("", alias="known_version"),
) -> JSONResponse:
    try:
        from cw_platform.config_base import CONFIG, load_config
        from cw_platform.orchestrator._state_store import StateStore
        from api.appAuthAPI import COOKIE_NAME, effective_user_profile_id
        from cw_platform.provider_instances import instances_for_user_profile

        requested = {part.strip() for part in include.split(",") if part.strip()}
        state_features = requested & {"history", "ratings", "progress"}
        cfg = load_config() or {}
        token = request.cookies.get(COOKIE_NAME) if request is not None else None
        profile = effective_user_profile_id(cfg, token, user_profile)
        limits = {
            "history": history_limit,
            "ratings": ratings_limit,
            "scrobble": scrobble_limit,
            "progress": progress_limit,
            "playlists": playlists_limit,
        }
        version = _dashboard_widgets_version(
            CONFIG,
            cfg=cfg,
            requested=requested,
            state_features=state_features,
            profile=str(profile or "").strip(),
            limits=limits,
        )
        if known_version and str(known_version).strip() == version:
            return JSONResponse({"ok": True, "not_modified": True, "version": version}, headers={"Cache-Control": "no-store"})
        state = StateStore(CONFIG).load_state_features(state_features) if state_features else {}
        scoped = bool(str(profile or "").strip())
        user_filter = instances_for_user_profile(cfg, profile) if scoped else {}
        if scoped and not user_filter:
            user_filter = {"__NONE__": ["__NONE__"]}
        payload = dashboard_widgets_payload(
            state,
            history_limit=history_limit,
            ratings_limit=ratings_limit,
            scrobble_limit=scrobble_limit,
            progress_limit=progress_limit,
            playlists_limit=playlists_limit,
            include=requested,
            user_filter=user_filter,
        )
        payload["version"] = version
        if scoped:
            payload["user_profile"] = str(profile or "").strip()
        return JSONResponse(payload, headers={"Cache-Control": "no-store"})
    except Exception as exc:
        try:
            if LOG is not None:
                LOG.error("dashboard widgets payload failed", extra={"error": f"{type(exc).__name__}: {exc}"})
        except Exception:
            pass
        return JSONResponse(
            {"ok": False, "error": "dashboard_widgets_failed"},
            status_code=200,
            headers={"Cache-Control": "no-store"},
        )

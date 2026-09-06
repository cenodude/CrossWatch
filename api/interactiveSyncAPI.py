# api/interactiveSyncAPI.py
# CrossWatch - Interactive Sync API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
import threading
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request
from pydantic import BaseModel, Field

from cw_platform.access_policy import request_user, user_can_access_pair
from cw_platform.config_base import load_config
from cw_platform.id_map import canonical_key, coalesce_ids, keys_for_item, ID_KEYS
from cw_platform.value_coercion import coerce_bool
from services import interactive_sync as svc

router = APIRouter(prefix="/api/interactive-sync", tags=["synchronization"])


class Start(BaseModel):
    pair_id: str = Field(min_length=1, max_length=256)


class Revision(BaseModel):
    revision: int = Field(ge=0)


class Refresh(Revision):
    choices: dict[str, str] = Field(default_factory=dict)


class Apply(Revision):
    model_config = {"extra": "forbid"}
    selection_version: int = Field(ge=0)


class Selection(Revision):
    selection_version: int = Field(ge=0)
    selected: bool
    ids: list[str] | None = Field(default=None, min_length=1, max_length=200)
    feature: str = Field(default="", max_length=32)
    result: str = Field(default="", max_length=32)
    q: str = Field(default="", max_length=256)


class MappingEdit(Revision):
    row_id: str
    item: dict[str, Any]


def owner(request):
    user = request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    return str((user or {}).get("id") or (user or {}).get("username") or "local")


def pair_for(cfg, request, pair_id):
    pair = next((p for p in cfg.get("pairs", []) if str(p.get("id")) == pair_id), None)
    if not pair or not user_can_access_pair(cfg, request_user(request), pair):
        raise HTTPException(404, "Sync pair not found")
    if not coerce_bool(pair.get("enabled", True), True):
        raise HTTPException(409, "Enable this sync pair before running it")
    return pair


def get_session(sid, request, cfg):
    svc.prune()
    session = svc.SESSIONS.get(sid)
    if session is None or session.owner != owner(request):
        raise HTTPException(404, "Review expired or not found. Run the pair again.")
    pair_for(cfg, request, session.pair_id)
    session.touched = time.monotonic()
    return session


def check_revision(session, revision):
    if session.status in ("reading", "applying"):
        raise HTTPException(409, "This review is busy")
    if session.status == "complete":
        raise HTTPException(409, "This operation has finished. Run the pair again for a new review.")
    if session.revision != revision:
        raise HTTPException(409, "The plan changed. Reload this review.")


def check_selection(session, version):
    if session.selection_version != version:
        raise HTTPException(409, "The selection changed. Reload this review.")


def launch(session, task, *args, applying=False, prepare=None):
    from . import syncAPI

    rt = syncAPI._rt()
    with rt[2]:
        if syncAPI._is_sync_running():
            raise HTTPException(409, "Another sync is running. Try again when it finishes.")
        if prepare is not None:
            prepare()
        session.status = "applying" if applying else "reading"
        session.message = "Checking provider data before applying selections…" if applying else "Reading providers and building the sync plan…"
        thread = threading.Thread(target=svc.worker, args=(session, task, *args), daemon=True)
        rt[1]["SYNC"] = thread
        thread.start()


@router.post("")
def start(payload: Start, request: Request):
    cfg = load_config()
    user_id = owner(request)
    pair = pair_for(cfg, request, payload.pair_id)
    with svc.LOCK:
        svc.prune()
        if len(svc.SESSIONS) >= svc.MAX_SESSIONS:
            raise HTTPException(429, "Close an existing review before starting another")
        session = svc.Session(pair_id=payload.pair_id, owner=user_id)
        session.pair = {k: pair.get(k) for k in ("source", "target", "source_instance", "target_instance", "mode", "name")}
        launch(session, svc.refresh, cfg, {})
        svc.SESSIONS[session.id] = session
        return session.public()


@router.get("/{sid}")
def get(sid: str, request: Request):
    with svc.LOCK:
        return get_session(sid, request, load_config()).public()


@router.post("/{sid}/refresh")
def refresh(sid: str, payload: Refresh, request: Request):
    cfg = load_config()
    with svc.LOCK:
        session = get_session(sid, request, cfg)
        check_revision(session, payload.revision)
        choices = dict(session.plan.choices)
        for cid, winner in payload.choices.items():
            conflict = session.plan.conflicts.get(cid)
            if not conflict or winner not in (conflict["source"], conflict["target"]):
                raise HTTPException(400, "Invalid conflict choice")
            choices[cid] = winner
        launch(session, svc.refresh, cfg, choices)
        return session.public()


@router.get("/{sid}/report-issues")
def report_issues(sid: str, request: Request, offset: int = Query(default=0, ge=0),
                  limit: int = Query(default=75, ge=1, le=200), feature: str = Query(default="", max_length=32),
                  result: str = Query(default="", max_length=32), q: str = Query(default="", max_length=256)):
    with svc.LOCK:
        session = get_session(sid, request, load_config())
        if session.report is None or session.store is None:
            raise HTTPException(409, "The sync report is not available yet")
        return dict(ok=True, **session.store.report_page(offset=offset, limit=limit, feature=feature, result=result, q=q))


@router.get("/{sid}/rows")
def rows(sid: str, request: Request, revision: int = Query(ge=0), offset: int = Query(default=0, ge=0),
         limit: int = Query(default=75, ge=1, le=200), feature: str = Query(default="", max_length=32),
         result: str = Query(default="", max_length=32), q: str = Query(default="", max_length=256)):
    with svc.LOCK:
        session = get_session(sid, request, load_config())
        if revision != session.revision:
            raise HTTPException(409, "The plan changed. Reload this review.")
        if session.store is None:
            raise HTTPException(409, "Wait for the plan to finish")
        return dict(ok=True, revision=session.revision, selection_version=session.selection_version,
                    counts=dict(session.store.counts), **session.store.page(offset=offset, limit=limit, feature=feature, result=result, q=q))


@router.post("/{sid}/selection")
def selection(sid: str, payload: Selection, request: Request):
    with svc.LOCK:
        session = get_session(sid, request, load_config())
        check_revision(session, payload.revision)
        check_selection(session, payload.selection_version)
        if session.status != "review" or session.store is None:
            raise HTTPException(409, "Build a complete plan before selecting changes")
        try:
            session.store.select(payload.selected, ids=payload.ids, feature=payload.feature, result=payload.result, q=payload.q)
        except ValueError as error:
            raise HTTPException(400, str(error)) from error
        session.selection_version += 1
        return session.public()


@router.post("/{sid}/apply")
def apply(sid: str, payload: Apply, request: Request):
    cfg = load_config()
    with svc.LOCK:
        session = get_session(sid, request, cfg)
        check_revision(session, payload.revision)
        check_selection(session, payload.selection_version)
        if session.status != "review":
            raise HTTPException(409, "Build a complete plan before applying")
        if coerce_bool((cfg.get("sync") or {}).get("dry_run", False)):
            raise HTTPException(409, "Disable global dry run before applying changes")
        selected = session.store.selected_ids() if session.store else set()
        if not selected:
            raise HTTPException(400, "Select valid changes from this review")
        launch(session, svc.apply, cfg, selected, applying=True)
        return session.public()


@router.post("/{sid}/mapping")
def mapping(sid: str, payload: MappingEdit, request: Request):
    from .editorAPI import _require_instance_scope, _save_policy_manual

    cfg = load_config()
    with svc.LOCK:
        session = get_session(sid, request, cfg)
        check_revision(session, payload.revision)
        row = session.plan.rows.get(payload.row_id)
        if not row or row["operation"] not in ("add", "update") or row["feature"] == "playlists":
            raise HTTPException(400, "This change cannot be remapped")
        _require_instance_scope(cfg, request, row["source"], row["source_instance"])
        item = deepcopy(row["item"])
        for key in (*ID_KEYS, "_trakt_history_id", "history_id", "_simkl_history_id", "_plex_history_id", "watched_id", "play_id"):
            item.pop(key, None)
        for key in ("type", "title", "year", "season", "episode", "series_title", "series_year", "show_ids"):
            item.pop(key, None)
            if key in payload.item:
                item[key] = payload.item[key]
        ids = payload.item.get("ids")
        if not isinstance(ids, dict) or any(k not in ID_KEYS for k in ids):
            raise HTTPException(400, "Supply supported media identifiers")
        item["ids"] = coalesce_ids(ids)
        if "show_ids" in item:
            if not isinstance(item["show_ids"], dict) or any(k not in ID_KEYS for k in item["show_ids"]):
                raise HTTPException(400, "Supply supported show identifiers")
            item["show_ids"] = coalesce_ids(item["show_ids"])
        if item.get("type") not in ("movie", "show", "anime", "season", "episode"):
            raise HTTPException(400, "Invalid media type")
        key = canonical_key(item)
        if key == "unknown:" or not (item["ids"] or item.get("show_ids")):
            raise HTTPException(400, "A media identifier is required")
        original = row["key"]
        def save():
            _save_policy_manual(row["feature"], row["source"], {key: item},
                                [original] if original and key != original and original not in keys_for_item(item) else [],
                                row["source_instance"], merge=True)
        launch(session, svc.refresh, cfg, session.plan.choices, prepare=save)
        return session.public()


@router.delete("/{sid}")
def discard(sid: str, request: Request):
    with svc.LOCK:
        session = get_session(sid, request, load_config())
        if session.status in ("reading", "applying"):
            raise HTTPException(409, "Wait for the current operation to finish")
        session.close()
        del svc.SESSIONS[sid]
        return {"ok": True}

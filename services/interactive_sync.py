# services/interactive_sync.py
# CrossWatch - Interactive Sync Sessions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from copy import deepcopy
from dataclasses import dataclass, field
import json
import logging
import threading
import time
from typing import Any
import uuid

from cw_platform.orchestrator import Orchestrator
from cw_platform.orchestrator._interactive import InteractivePlan, fingerprint
from .interactive_sync_store import ReviewStore
from .interactive_sync_progress import SyncProgress
from .interactive_sync_report import SyncReport

SESSION_TTL = 3600
MAX_SESSIONS = 24
LOCK = threading.RLock()
SESSIONS: dict[str, Session] = {}
LOG = logging.getLogger(__name__)


@dataclass
class Session:
    pair_id: str
    owner: str
    id: str = field(default_factory=lambda: uuid.uuid4().hex)
    status: str = "reading"
    revision: int = 0
    touched: float = field(default_factory=time.monotonic)
    plan: InteractivePlan = field(default_factory=InteractivePlan)
    config_hash: str = ""
    mapping_version: str = ""
    pair: dict[str, Any] = field(default_factory=dict)
    message: str = "Reading providers and building the sync plan…"
    summary: dict[str, Any] = field(default_factory=dict)
    progress: SyncProgress = field(default_factory=SyncProgress)
    planned_at: int = field(default_factory=lambda: int(time.time()))
    store: ReviewStore | None = None
    selection_version: int = 0
    apply_review: dict[str, Any] | None = None
    report: dict[str, Any] | None = None

    def public(self):
        return dict(ok=True, id=self.id, pair_id=self.pair_id, pair=self.pair, status=self.status,
                    revision=self.revision, message=self.message, progress=self.progress.public(),
                    counts=dict(self.store.counts) if self.store else dict(changes=0, conflicts=0, attention=0, selected=0),
                    features=list(self.store.features) if self.store else [], selection_version=self.selection_version,
                    notices=self.plan.notices[:100], summary=self.summary, apply_review=self.apply_review, report=self.report)

    def close(self):
        if self.store is not None:
            self.store.close()
            self.store = None


def prune():
    now = time.monotonic()
    for sid, session in list(SESSIONS.items()):
        if session.status not in ("reading", "applying") and now - session.touched > SESSION_TTL:
            session.close()
            del SESSIONS[sid]


def mapping_version(cfg=None):
    from cw_platform.config_base import CONFIG_BASE
    from cw_platform.local_db.manual_policy import load_policy
    from cw_platform.anime_mapping.overrides import load_overrides
    from cw_platform.anime_mapping.storage import paths

    dataset = paths(((cfg or {}).get("anime_mapping") or {}).get("release_tag", "v3"))["db"]
    stamp = dataset.stat().st_mtime_ns if dataset.exists() else 0
    return fingerprint([load_policy(CONFIG_BASE()), load_overrides(), stamp])


def build(session: Session, cfg: dict[str, Any], choices: dict[str, str], *, store=None, selected=None) -> tuple[InteractivePlan, dict[str, Any]]:
    plan = InteractivePlan(choices=dict(choices), planned_at=session.planned_at,
                           record_rows=store is not None or selected is None, selected=selected or set(), copy_rows=store is None)
    plan.on_progress = session.progress.event
    if store is not None:
        plan.rows = store.rows
        plan.conflicts = store.conflicts
    preview_cfg = deepcopy(cfg)
    preview_cfg["_cw_readonly"] = True

    def progress(line):
        session.progress.event(line)
        try:
            event = json.loads(line)
        except (ValueError, TypeError):
            return
        name = str(event.get("event") or "")
        feature = str(event.get("feature") or "")
        if name in {"feature:error", "feature:unsupported", "pair:skip", "run:pair:skip", "playlist:mapping:error", "writes:skipped"} or "mass_delete" in name or "massdelete" in name:
            if len(plan.notices) < 100:
                plan.notices.append(dict(event=name, feature=feature, reason=str(event.get("reason") or name),
                                         provider=str(event.get("dst") or "")))

    mgr = Orchestrator(preview_cfg, on_progress=progress, interactive=plan)
    summary = mgr.run(dry_run=True, pair_scope_ids=[session.pair_id], write_state_json=False)
    return plan, summary


def refresh(session: Session, cfg: dict[str, Any], choices: dict[str, str], *, restart_progress=True):
    if restart_progress:
        session.progress.begin("preview")
    version = mapping_version(cfg)
    store = ReviewStore()
    try:
        plan, summary = build(session, cfg, choices, store=store)
        _finish_review(session, cfg, version, plan, summary, store)
    except Exception:
        if session.store is not store:
            store.close()
        raise


def _finish_review(session, cfg, version, plan, summary, store):
    session.progress.set_stage("finalizing", "Preparing pages and preserving selections")
    store.finish(session.store)
    with LOCK:
        session.close()
        session.store = store
        session.plan = plan
        session.summary = summary
        session.config_hash = fingerprint(cfg)
        session.mapping_version = version
        session.revision += 1
        session.selection_version += 1
        session.apply_review = None
        session.report = None
        session.status = "review" if summary.get("ok") else "error"
        session.message = "Review the proposed changes. Nothing has been applied." if summary.get("ok") else "The plan could not be completed. Check provider status and refresh."
        if summary.get("cancelled"):
            session.message = "Reading cancelled. Refresh to build a new plan."
        session.touched = time.monotonic()
        session.progress.finish("cancelled" if summary.get("cancelled") else session.status, session.message)


def _apply_needs_review(session, selected, reason):
    with LOCK:
        retained = len(selected & session.store.selected_ids()) if session.store else 0
        session.apply_review = dict(requested=len(selected), retained=retained, needs_review=len(selected) - retained,
                                    applied=0, reason=reason)
        session.message = "Nothing was applied. " + reason + " Review the refreshed plan before applying again."
        session.progress.finish(session.status, session.message)
    LOG.info("interactive_sync_recheck session=%s requested=%s retained=%s needs_review=%s reason=%s",
             session.id, len(selected), retained, len(selected) - retained, reason)


def apply(session: Session, cfg: dict[str, Any], selected: set[str]):
    from api import syncAPI

    session.progress.begin("apply")
    session.apply_review = None
    session.report = None
    report = SyncReport()
    if fingerprint(cfg) != session.config_hash or mapping_version(cfg) != session.mapping_version:
        refresh(session, cfg, session.plan.choices, restart_progress=False)
        _apply_needs_review(session, selected, "Settings or mappings changed.")
        return
    version = mapping_version(cfg)
    store = ReviewStore()
    try:
        plan, summary = build(session, cfg, session.plan.choices, selected=selected, store=store)
        if not summary.get("ok") or not selected <= plan.seen:
            if session.store is not None:
                for detail in store.recheck_details(session.store, selected - plan.seen):
                    LOG.info("interactive_sync_proposal_changed session=%s detail=%s", session.id, json.dumps(detail, sort_keys=True))
            _finish_review(session, cfg, version, plan, summary, store)
            reason = "The provider recheck could not be completed." if not summary.get("ok") else "Some selected proposals changed or are no longer available."
            _apply_needs_review(session, selected, reason)
            return
    finally:
        if session.store is not store:
            store.close()
    execution = InteractivePlan(preview=False, selected=selected, choices=dict(plan.choices), planned_at=session.planned_at, record_rows=False)
    def progress(event):
        session.progress.event(event)
        report.event(event)

    execution.on_progress = progress
    execution.on_result = report.record
    execution.valid = lambda: mapping_version(cfg) == session.mapping_version
    execution.config_hash = fingerprint(cfg)
    try:
        syncAPI._run_pairs_thread("interactive-" + session.id, {"pair_id": session.pair_id}, interactive=execution)
    finally:
        with LOCK:
            session.report = report.finish(session, execution, execution.result)
    with LOCK:
        result = execution.result
        session.summary = dict(result or {})
        session.summary["not_applied"] = len(selected - execution.seen)
        session.status = "complete" if result is not None else "error"
        session.message = "Selected changes processed. Review the results below." if result is not None else "Sync failed. Check Events before running again."
        if session.summary.get("cancelled"):
            session.message = "Sync cancelled. Changes already applied were kept."
        session.touched = time.monotonic()
        session.progress.finish("cancelled" if session.summary.get("cancelled") else session.status, session.message)


def worker(session: Session, task, *args):
    from api import syncAPI
    from cw_platform.run_control import clear_cancel

    try:
        clear_cancel()
        task(session, *args)
    except Exception:
        LOG.exception("interactive_sync_failed session=%s", session.id)
        with LOCK:
            session.status = "error"
            session.message = "Interactive Sync failed. Check the server logs and refresh the plan."
            session.progress.finish("error", session.message)
    finally:
        rt = syncAPI._rt()
        with rt[2]:
            if rt[1].get("SYNC") is threading.current_thread():
                rt[1].pop("SYNC", None)

# services/tracker_import.py
# CrossWatch - Import a connected service into a CrossWatch tracker instance
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import threading
import time
import uuid
from collections.abc import Callable, Iterable, Mapping
from typing import Any

from cw_platform.modules_registry import load_sync_ops, provider_display_label, sync_provider_names, sync_provider_supports_feature
from cw_platform.provider_instances import (
    build_provider_config_view,
    get_provider_block,
    list_instance_ids,
    normalize_instance_id,
    sanitize_instance_label,
)
from cw_platform.tracker_store import TrackerStore

_LOG = logging.getLogger("crosswatch.tracker_import")
_LOCK = threading.RLock()
_ACTIVE: dict[str, dict[str, Any]] = {}
JOB = "import"
TARGET = "CROSSWATCH"
FEATURES = ("history", "watchlist", "ratings", "progress", "collection")
Launch = Callable[[Mapping[str, Any], str, Callable[[Mapping[str, Any]], None]], Mapping[str, Any]]


def _label(cfg: Mapping[str, Any], provider: str, instance: str) -> str:
    block = get_provider_block(cfg, provider, instance)
    label = sanitize_instance_label((block or {}).get("label")) if isinstance(block, Mapping) else ""
    return label or ("Default" if instance == "default" else instance)


def _instance_ids(cfg: Mapping[str, Any], provider: str) -> list[str]:
    out: list[str] = []
    for raw in list_instance_ids(cfg, provider) or ["default"]:
        inst = normalize_instance_id(raw)
        if inst not in out:
            out.append(inst)
    return out or ["default"]


def _configured(ops: Any, cfg: Mapping[str, Any], provider: str, instance: str) -> bool:
    check = getattr(ops, "is_configured", None)
    if not callable(check):
        return True
    try:
        return bool(check(build_provider_config_view(dict(cfg), provider, instance)))
    except Exception:
        return False


def sources(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    targets = {f for f in FEATURES if sync_provider_supports_feature(TARGET, f)}
    out: list[dict[str, Any]] = []
    for name in sync_provider_names(upper=True):
        try:
            ops = load_sync_ops(name)
        except Exception:
            ops = None
        if ops is None:
            continue
        features = [f for f in FEATURES if f in targets and sync_provider_supports_feature(name, f)]
        if not features:
            continue
        instances = [{"id": inst, "label": _label(cfg, name, inst)} for inst in _instance_ids(cfg, name) if _configured(ops, cfg, name, inst)]
        if instances:
            dropped = "history" in features and callable(getattr(ops, "dropped_show_tokens", None))
            out.append({"id": name, "label": provider_display_label(name), "instances": instances, "features": features, "dropped": dropped})
    out.sort(key=lambda row: (row["id"] == TARGET, row["label"].lower()))
    return out


def pair_for(source: str, source_instance: str, instance: str, features: Iterable[str], *, dropped: bool = False) -> dict[str, Any]:
    picked = list(features)
    pair: dict[str, Any] = {
        "id": f"tracker-import-{source}-{source_instance}-{instance}".lower(),
        "name": f"Import {source} into CrossWatch",
        "enabled": True,
        "mode": "one-way",
        "source": source,
        "source_instance": source_instance,
        "target": TARGET,
        "target_instance": instance,
        "features": {f: {"enable": True, "add": True, "remove": False} for f in picked},
    }
    if dropped and "history" in picked:
        pair["providers"] = {source.lower(): {"history_dropped_mode": "tag"}}
    return pair


def busy() -> bool:
    with _LOCK:
        return bool(_ACTIVE)


def status(store: TrackerStore, run: Mapping[str, Any] | None = None) -> dict[str, Any]:
    key = str(store.db_path)
    with _LOCK:
        if key in _ACTIVE:
            return {**_ACTIVE[key], **progress(run)}
    job = store.job(JOB)
    if not job:
        return {"state": "idle"}
    if job.get("state") == "running":
        return {**job, "state": "interrupted"}
    return job


def _map(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def progress(run: Mapping[str, Any] | None) -> dict[str, Any]:
    if not run:
        return {"phase": "starting"}
    apply = _map(_map(run.get("_phase")).get("apply"))
    timeline = _map(run.get("timeline"))
    writing = bool(timeline.get("post")) and int(apply.get("total") or 0) > 0
    return {
        "phase": "writing" if writing else "reading",
        "done": int(apply.get("done") or 0) if writing else 0,
        "total": int(apply.get("total") or 0) if writing else 0,
        "progress": float(apply.get("progress") or 0.0),
        "feature_index": int(apply.get("feature_index") or 0),
        "feature_count": int(apply.get("feature_count") or 0),
        "cancel_requested": bool(run.get("cancel_requested")),
    }


def result(run: Mapping[str, Any]) -> dict[str, Any]:
    features = _map(run.get("features"))
    added = {str(f): int(_map(v).get("added") or 0) for f, v in features.items() if f in FEATURES}
    return {
        "added": sum(added.values()),
        "added_by_feature": added,
        "updated": sum(int(_map(v).get("updated") or 0) for v in features.values()),
        "skipped": int(run.get("skipped") or 0),
        "unresolved": int(run.get("unresolved") or 0),
        "errors": int(run.get("errors") or 0),
    }


def start(
    cfg: Mapping[str, Any],
    store: TrackerStore,
    *,
    source: Any,
    source_instance: Any,
    instance: Any,
    features: Iterable[Any],
    launch: Launch,
    dropped: bool = False,
) -> dict[str, Any]:
    src = str(source or "").strip().upper()
    src_inst = normalize_instance_id(source_instance)
    inst = normalize_instance_id(instance)
    row = next((r for r in sources(cfg) if r["id"] == src), None)
    if row is None or not any(i["id"] == src_inst for i in row["instances"]):
        raise ValueError("unknown_source")
    if inst not in _instance_ids(cfg, "crosswatch"):
        raise ValueError("unknown_instance")
    if src == TARGET and src_inst == inst:
        raise ValueError("same_instance")
    wanted = {str(f or "").strip().lower() for f in features or []}
    picked = [f for f in FEATURES if f in wanted]
    if not picked or wanted - set(picked) or any(f not in row["features"] for f in picked):
        raise ValueError("unsupported_feature")
    tag_dropped = bool(dropped and row.get("dropped") and "history" in picked)
    key = str(store.db_path)
    with _LOCK:
        if _ACTIVE:
            return {"ok": False, "error": "import_running"}
        job: dict[str, Any] = {
            "id": uuid.uuid4().hex, "state": "running", "started_at": time.time(), "instance": inst,
            "source": src, "source_label": row["label"], "source_instance": src_inst,
            "source_instance_label": next(i["label"] for i in row["instances"] if i["id"] == src_inst),
            "features": picked, "dropped": tag_dropped, "run_id": str(int(time.time())),
        }
        _ACTIVE[key] = job
    try:
        store.save_job(JOB, job)
        pair = pair_for(src, src_inst, inst, picked, dropped=tag_dropped)
        launched = launch(pair, job["run_id"], lambda run: finish(store, job, run))
    except BaseException:
        with _LOCK:
            _ACTIVE.pop(key, None)
        raise
    if not launched.get("ok"):
        with _LOCK:
            _ACTIVE.pop(key, None)
        store.save_job(JOB, {**job, "state": "idle"})
        return {"ok": False, "error": str(launched.get("error") or "sync_running")}
    with _LOCK:
        return {"ok": True, **job}


def finish(store: TrackerStore, job: dict[str, Any], run: Mapping[str, Any]) -> None:
    key = str(store.db_path)
    mine = str(run.get("run_id") or "") == str(job.get("run_id") or "")
    if not mine:
        state = "failed"
    elif run.get("cancelled"):
        state = "cancelled"
    elif run.get("exit_code") not in (0, "0"):
        state = "failed"
    else:
        state = "completed"
    with _LOCK:
        job.update(state=state, finished_at=time.time(), **(result(run) if mine else {}))
    try:
        store.save_job(JOB, job)
    except Exception:
        _LOG.exception("Could not save the tracker import result")
    with _LOCK:
        _ACTIVE.pop(key, None)
    _LOG.info("Tracker import %s from %s into %s: %s added", state, job.get("source"), job.get("instance"), job.get("added", 0))

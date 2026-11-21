# services/tracker_inventory.py
# CrossWatch - Keep the saved CrossWatch inventory read by the Editor and Hub in step with the tracker
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import sys
import threading
from collections.abc import Callable, Mapping
from pathlib import Path
from typing import Any

from cw_platform.memory_release import release_memory
from cw_platform.provider_instances import build_provider_config_view, list_instance_ids, normalize_instance_id
from cw_platform.tracker_store import FEATURES, TrackerStore, add_item_listener

_LOG = logging.getLogger(__name__)
_STOP = threading.Event()
_WAKE = threading.Event()
_THREAD: threading.Thread | None = None
_SEEN: dict[tuple[str, str, str], str] = {}
_QUIET: dict[tuple[str, str, str], str] = {}
_LOADED: set[tuple[str, str]] = set()
_FAILED: dict[tuple[str, ...], str] = {}
INTERVAL = 5.0
QUIET = 1.0
JOB = "shared_inventory"


def _saved_blocks() -> dict[str, dict[str, Mapping[str, Any]]]:
    from api import editorAPI as api

    out: dict[str, dict[str, Mapping[str, Any]]] = {}
    for row in api._state_store().feature_inventory():
        if row["provider"] == "CROSSWATCH" and row["feature"] in FEATURES:
            out.setdefault(normalize_instance_id(row["instance"]), {})[row["feature"]] = row
    return out


def _tracker_root(cfg: Mapping[str, Any], instance: str) -> Path:
    from providers.sync._mod_CROSSWATCH import CROSSWATCHModule

    view = dict(build_provider_config_view(cfg, "CROSSWATCH", instance))
    view["_cw_readonly"] = True
    return CROSSWATCHModule(view).cfg.base_path


def _busy() -> bool:
    from providers.sync.crosswatch._common import pair_scoped
    from services.tracker_automation import busy

    return pair_scoped() or busy()


def _warn(key: tuple[str, ...], message: str, *args: Any) -> None:
    error = repr(sys.exc_info()[1])
    if _FAILED.get(key) == error:
        return
    _FAILED[key] = error
    _LOG.warning(message, *args, exc_info=True)


def refresh(instance: str, feature: str, block: Mapping[str, Any]) -> bool:
    from api import editorAPI as api

    inst = normalize_instance_id(instance)
    collapsed = bool(block.get("items") and block.get("mode") == "collapsed")
    try:
        current = api.read_tracker_index(feature, inst, rewatches=not collapsed)
        items = {key: api._editor_minimal_item(feature, key, item) for key, item in current.items()}
        if inst != "default":
            items = {key: {**item, "_cw_instance": inst} for key, item in items.items()}
        api._state_store().save_feature_baseline(
            provider="CROSSWATCH", instance=inst, feature=feature, items=items, checkpoint=block.get("checkpoint"),
        )
    except Exception:
        _warn(("refresh", inst, feature), "Could not refresh the saved CrossWatch %s %s inventory", inst, feature)
        return False
    _FAILED.pop(("refresh", inst, feature), None)
    return True


def wake() -> None:
    _WAKE.set()


def run_once(cfg: Mapping[str, Any]) -> bool:
    if _busy():
        return False
    refreshed, pending = _check(cfg)
    if refreshed:
        release_memory()
    return pending


def _recall(store: TrackerStore, root: Path, inst: str, blocks: Mapping[str, Mapping[str, Mapping[str, Any]]]) -> None:
    saved = (store.job(JOB) or {}).get(inst)
    for feature, entry in saved.items() if isinstance(saved, Mapping) else ():
        block = blocks.get(inst, {}).get(feature)
        if block is not None and isinstance(entry, list) and len(entry) == 2 and entry[1] == block.get("updated_at"):
            _SEEN.setdefault((str(root), inst, feature), str(entry[0]))


def _remember(store: TrackerStore, root: Path, inst: str) -> None:
    try:
        blocks = _saved_blocks().get(inst, {})
        job = store.job(JOB) or {}
        job[inst] = {
            feature: [_SEEN[(str(root), inst, feature)], block.get("updated_at")]
            for feature, block in blocks.items() if (str(root), inst, feature) in _SEEN
        }
        store.save_job(JOB, job)
    except Exception:
        _warn(("remember", inst), "Could not save the CrossWatch %s inventory revision", inst)


def _check(cfg: Mapping[str, Any]) -> tuple[bool, bool]:
    from services.tracker_migration import migrating

    run: dict[str, Any] = {"refreshed": False, "pending": False, "blocks": None}
    for inst in dict.fromkeys(normalize_instance_id(i) for i in list_instance_ids(cfg, "CROSSWATCH")):
        if _STOP.is_set():
            break
        try:
            if not _check_instance(cfg, inst, migrating, run):
                break
            _FAILED.pop(("instance", inst), None)
        except Exception:
            _warn(("instance", inst), "Could not check the CrossWatch %s tracker", inst)
    return run["refreshed"], run["pending"]


def _check_instance(cfg: Mapping[str, Any], inst: str, migrating: Callable[[Path], bool], run: dict[str, Any]) -> bool:
    root = _tracker_root(cfg, inst)
    if migrating(root) or not (root / "tracker.db").is_file():
        return True
    store = TrackerStore(root)
    if (str(root), inst) not in _LOADED:
        if run["blocks"] is None:
            run["blocks"] = _saved_blocks()
        _recall(store, root, inst, run["blocks"])
        _LOADED.add((str(root), inst))
    revisions = store.item_revisions()
    mirrored = False
    for feature in FEATURES:
        key = (str(root), inst, feature)
        revision = revisions.get(feature, "")
        if _SEEN.get(key) == revision:
            continue
        if _QUIET.get(key) != revision:
            _QUIET[key] = revision
            run["pending"] = True
            continue
        if run["blocks"] is None:
            run["blocks"] = _saved_blocks()
        block = run["blocks"].get(inst, {}).get(feature)
        if block is not None:
            if _STOP.is_set() or _busy():
                return False
            if not refresh(inst, feature, block):
                continue
            run["refreshed"] = mirrored = True
        _SEEN[key] = revision
    if mirrored:
        _remember(store, root, inst)
    return True


def start(load_config: Callable[[], Mapping[str, Any] | None]) -> None:
    global _THREAD
    if _THREAD is not None and _THREAD.is_alive():
        return
    _STOP.clear()
    add_item_listener(wake)

    def work() -> None:
        delay = INTERVAL
        while not _STOP.is_set():
            if _WAKE.wait(delay):
                _WAKE.clear()
                if _STOP.wait(QUIET):
                    break
            if _STOP.is_set():
                break
            try:
                delay = QUIET if run_once(load_config() or {}) else INTERVAL
                _FAILED.pop(("run",), None)
            except Exception:
                delay = INTERVAL
                _warn(("run",), "Saved CrossWatch inventory refresh failed")

    _THREAD = threading.Thread(target=work, name="tracker-inventory", daemon=True)
    _THREAD.start()


def stop() -> None:
    _STOP.set()
    _WAKE.set()
    if _THREAD is not None:
        _THREAD.join(timeout=2)

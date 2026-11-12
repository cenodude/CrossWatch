# services/tracker_automation.py
# CrossWatch - Bounded cache-only local tracker status processing
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import logging
import threading
import uuid
from itertools import islice

from cw_platform.tracker_automation import instance_options
from cw_platform.tracker_store import TrackerStore, tracker_roots

_STOP = threading.Event()
_WAKE = threading.Event()
_THREAD = None
_NEXT_ROOT = 0
_LOG = logging.getLogger(__name__)
_METADATA_LOCK = threading.Lock()
_METADATA_PENDING: dict[str, None] = {}


def metadata_changed(tmdb_id):
    key = "show:tmdb:" + str(tmdb_id)
    with _METADATA_LOCK:
        _METADATA_PENDING[key] = None


def _queue_metadata(stores):
    with _METADATA_LOCK:
        keys = list(islice(_METADATA_PENDING, 25))
        for key in keys:
            del _METADATA_PENDING[key]
    if not keys:
        return
    try:
        for store, _, _ in stores:
            with store.session():
                store.queue_metadata_statuses(keys)
    except Exception:
        with _METADATA_LOCK:
            for key in keys:
                _METADATA_PENDING[key] = None
        raise


def cached_metadata(key):
    if not key.startswith("show:tmdb:"):
        return None
    from api.trackerAPI import _tmdb_provider
    try:
        hit = _tmdb_provider().peek_show(key.split(":", 2)[2])
        return hit.data if hit else None
    except Exception:
        return None


def busy():
    from services.tracker_import import busy as importing
    from api.syncAPI import _is_sync_running
    return importing() or _is_sync_running()


def run_once(cfg, config_dir):
    global _NEXT_ROOT
    from services.tracker_migration import migrating
    seen = set()
    stores = []
    metadata_enabled = False
    metadata_paused = False
    for instance, root in tracker_roots(cfg, config_dir):
        if _STOP.is_set():
            return
        identity = str(root.resolve())
        if identity in seen or not (root / "tracker.db").is_file():
            continue
        seen.add(identity)
        enabled = instance_options(cfg, instance)["automatic_status"]
        metadata_enabled = metadata_enabled or enabled
        if migrating(root):
            metadata_paused = metadata_paused or enabled
            continue
        store = TrackerStore(root)
        with store.session():
            store.configure_automation(enabled)
            if enabled:
                stores.append((store, store.status_catchup(), store.review_status().get("state") in {"scanning", "processing"}))
    if not metadata_enabled:
        with _METADATA_LOCK:
            _METADATA_PENDING.clear()
    if not stores or busy():
        return
    if not metadata_paused:
        _queue_metadata(stores)
    start = _NEXT_ROOT % len(stores)
    _NEXT_ROOT = start + 1
    budget = 25
    reviews = []
    fast = any(catchup or review for _, catchup, review in stores)
    for store, catchup, review in stores[start:] + stores[:start]:
        try:
            with store.session():
                changed = False
                group = uuid.uuid4().hex
                try:
                    for key, version in store.pending_statuses(250 if catchup else budget):
                        if _STOP.is_set() or busy():
                            return
                        result = store.evaluate_status(key, lambda: cached_metadata(key), version=version, prune=False, group=group)
                        changed = changed or bool(result.get("batch"))
                        budget -= 1
                finally:
                    if changed:
                        store.prune_status_changes()
                if store.review_status().get("state") in {"scanning", "processing"}:
                    reviews.append(store)
            if budget <= 0:
                return fast
        except Exception:
            _LOG.exception("Automatic status processing failed for %s", store.root)
            return False
    for store in reviews[:1]:
        try:
            if _STOP.is_set() or busy():
                return
            with store.session():
                store.scan_review()
                changed = False
                group = uuid.uuid4().hex
                try:
                    for key in store.review_pending(250):
                        if _STOP.is_set() or busy():
                            return
                        result = store.evaluate_status(key, lambda: cached_metadata(key), review=True, prune=False, group=group)
                        changed = changed or bool(result.get("batch"))
                        budget -= 1
                finally:
                    if changed:
                        store.prune_status_changes()
        except Exception:
            _LOG.exception("Tracker history review failed for %s", store.root)
            return False
    return fast


def wake():
    _WAKE.set()


def start(load_config, config_dir):
    global _THREAD
    if _THREAD is not None and _THREAD.is_alive():
        return
    _STOP.clear()

    def work():
        delay = 15
        while not _STOP.is_set():
            _WAKE.wait(delay)
            _WAKE.clear()
            if _STOP.is_set():
                break
            try:
                delay = .05 if run_once(load_config() or {}, config_dir) else 15
            except Exception:
                delay = 15
                _LOG.exception("Automatic tracker status processing failed")

    _THREAD = threading.Thread(target=work, name="tracker-status", daemon=True)
    _THREAD.start()


def stop():
    _STOP.set()
    _WAKE.set()
    if _THREAD is not None:
        _THREAD.join(timeout=2)

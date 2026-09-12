# services/editor_removal.py
# CrossWatch - Selective removal of records known to pair sync baselines
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from contextlib import contextmanager
import hashlib
import json
import logging
from typing import Any

from fastapi import HTTPException

from cw_platform.access_policy import request_user, user_can_access_instance, user_can_access_pair
from cw_platform.history_events import (
    EVENT_ID_FIELDS, base_key_from_history_event, is_history_event_key,
)
from cw_platform.id_map import canonical_key, unified_keys_from_ids
from cw_platform.local_db import state as sqlite_state
from cw_platform.modules_registry import load_sync_ops
from cw_platform.orchestrator._applier import apply_remove
from cw_platform.orchestrator._history_rewatches import collapse_history_latest, config_with_history_rewatches
from cw_platform.orchestrator._pairs_oneway import compute_effective_remove
from cw_platform.orchestrator._pairs import _feature_list_for_pair
from cw_platform.pair_scope import pair_feature_scope
from cw_platform.provider_instances import build_provider_config_view, normalize_instance_id
from services.snapshots import _capture_mode_env, _cleanup_feature_enabled, _crosswatch_write_env

LOG = logging.getLogger(__name__)
FEATURES = {"watchlist", "history", "ratings", "progress", "collection"}


def _event(key, item):
    return is_history_event_key(key) or is_history_event_key(item.get("_cw_event_key"))


def _tokens(key, item):
    """Typed IDs, with episode coordinates; never match siblings by a bare show ID."""
    typ = str(item.get("type") or "").lower()
    typ = "show" if typ == "tv" else typ
    ids = item.get("ids") if isinstance(item.get("ids"), Mapping) else {}
    show_ids = item.get("show_ids") if isinstance(item.get("show_ids"), Mapping) else {}
    tokens = set()
    if typ in {"episode", "season"}:
        try:
            season = int(item.get("season"))
            episode = int(item.get("episode")) if typ == "episode" else None
        except (TypeError, ValueError):
            return set()
        if season < 0 or (episode is not None and episode < 1):
            return set()
        fragment = f"#s{season:02d}e{episode:02d}" if episode is not None else f"#season:{season}"
        tokens.update(token + fragment for token in unified_keys_from_ids(show_ids or ids))
        if typ == "episode" and show_ids:
            tokens.update(token + fragment for token in unified_keys_from_ids(ids))
    else:
        tokens.update(unified_keys_from_ids(ids))
    base = base_key_from_history_event(key)
    if base and not base.startswith(("title:", "unknown:")) and (typ not in {"episode", "season"} or "#" in base):
        tokens.add(base)
    return {f"{typ}|{token}" for token in tokens}


def _matches(key, item, other_key, other, *, same_account=False, include_viewings=False):
    tokens = _tokens(key, item) & _tokens(other_key, other)
    if not same_account:
        # Server-local IDs are only meaningful within their own account.
        tokens = {token for token in tokens if token.split("|", 1)[1].split(":", 1)[0]
                  not in {"plex", "jellyfin", "emby", "guid", "slug"}}
    if not tokens:
        return False
    # Dated selections must never silently turn into whole-status removals.
    # Confirmed status removal also clears that item's locally cached dates.
    return not _event(key, item) and (include_viewings or not _event(other_key, other))


class _RecordIndex:
    def __init__(self, records):
        self.records = records
        self.tokens = {}
        for key, item in records.items():
            if isinstance(item, Mapping):
                for token in _tokens(key, item):
                    self.tokens.setdefault(token, set()).add(key)

    def matches(self, key, item, *, same_account=False):
        candidates = set()
        for token in _tokens(key, item):
            candidates.update(self.tokens.get(token, ()))
        return [(key2, self.records[key2]) for key2 in sorted(candidates)
                if _matches(key, item, key2, self.records[key2], same_account=same_account)]


def _items(state, provider, instance, feature):
    node = (state.get("providers") or {}).get(provider) or {}
    if instance != "default":
        node = (node.get("instances") or {}).get(instance) or {}
    return ((node.get(feature) or {}).get("baseline") or {}).get("items") or {}


def _selection(payload, request):
    from api import editorAPI as api

    user = request_user(request)
    if user and not user.get("is_admin") and not (user.get("permissions") or {}).get("write"):
        raise HTTPException(403, "Write permission required")
    feature = str(payload.get("kind") or "").lower()
    if feature not in FEATURES:
        raise HTTPException(400, "Unsupported feature")
    raw = payload.get("items")
    if not isinstance(raw, list) or not raw or len(raw) > 1000:
        raise HTTPException(400, "Select between 1 and 1000 rows")
    selected = []
    for row in raw:
        item = api._normalize_send_item(row, feature, operation="remove")
        if not item:
            raise HTTPException(400, "Invalid removal selection")
        key = str(item.get("key") or item.get("_cw_event_key") or canonical_key(item))
        if feature == "history" and _event(key, item):
            raise HTTPException(400, "Individual watch dates cannot be removed here. Select a watched-status row.")
        if not _tokens(key, item):
            raise HTTPException(400, "Selected rows need an identifiable movie, show or episode")
        selected.append((key, item))
    return feature, selected


def _can_remove(ops, feature, key, item):
    if not _cleanup_feature_enabled(ops, feature):
        return False
    caps = (ops.capabilities() or {}).get(feature) or {}
    types = caps.get("types") or {}
    typ = str(item.get("type") or "").lower()
    type_key = {"movie": "movies", "show": "shows", "tv": "shows", "season": "seasons", "episode": "episodes"}.get(typ)
    if type_key in types and not types[type_key]:
        return False
    if feature == "history" and _event(key, item):
        return False
    return True


def _plan(payload, request):
    from api import editorAPI as api

    feature, selected = _selection(payload, request)
    cfg, user = api.load_config(), request_user(request)
    pair_id = str(payload.get("pair_id") or "")
    source = str(payload.get("source_provider") or "").upper()
    source_instance = normalize_instance_id(payload.get("source_instance"))
    targets = {}
    missing_baselines = set()
    found_pair = not pair_id
    # Reuse labels and configured-instance filtering from Send. The baseline,
    # rather than provider discovery or the global inventory, grants eligibility.
    available = {(row["provider"], row["instance"]): row
                 for row in api._filter_targets_for_request(cfg, request, api._editor_send_targets(cfg, feature))}
    for index, pair in enumerate(cfg.get("pairs") or [], 1):
        if pair_id and str(pair.get("id") or "") != pair_id:
            continue
        if not user_can_access_pair(cfg, user, pair):
            continue
        if feature not in (pair.get("features") or {}) and feature not in _feature_list_for_pair(pair):
            continue
        found_pair = True
        scope = pair_feature_scope(cfg, pair, feature, index)
        state = sqlite_state.load_pair_state(api._STATE_BASE, scope, {feature})
        for side in ("source", "target"):
            provider = str(pair.get(side) or "").upper()
            instance = normalize_instance_id(pair.get(f"{side}_instance"))
            identity = (provider, instance)
            if identity not in available or not user_can_access_instance(cfg, user, provider, instance):
                continue
            node = (state.get("providers") or {}).get(provider) or {}
            if instance != "default":
                node = (node.get("instances") or {}).get(instance) or {}
            if feature not in node:
                missing_baselines.add((scope, provider, instance))
                continue
            ops = load_sync_ops(provider)
            baseline = _RecordIndex(_items(state, provider, instance, feature))
            for row_index, (key, item) in enumerate(selected):
                matches = baseline.matches(key, item, same_account=(provider == source and instance == source_instance))
                # Ambiguous identities must not become bulk deletions.
                if len(matches) != 1:
                    continue
                dest_key, dest = matches[0]
                if not _can_remove(ops, feature, dest_key, dest):
                    continue
                target = targets.setdefault(identity, {**available[identity], "records": {}, "rows": set()})
                target["rows"].add(row_index)
                target["records"].setdefault(dest_key, dest)
    if not found_pair:
        raise HTTPException(404, "Sync pair not found for this feature")
    rows = sorted(targets.values(), key=lambda t: (t["display"].casefold(), t["instance"]))
    signature = [(t["provider"], t["instance"], t["records"]) for t in rows]
    digest = hashlib.sha256(json.dumps([feature, pair_id, signature], sort_keys=True, default=str).encode()).hexdigest()
    return cfg, feature, selected, rows, digest, len(missing_baselines)


def preview_removal(payload, request=None):
    _, feature, selected, targets, digest, missing = _plan(payload, request)
    public = []
    for target in targets:
        row = {k: v for k, v in target.items() if k not in {"records", "rows"}}
        row["matched"] = len(target["rows"])
        row["count"] = len(target["records"])
        public.append(row)
    return dict(ok=True, operation="remove", kind=feature, selected=len(selected), providers=public, preview_id=digest,
                missing_baselines=missing,
                empty_reason="pair_baseline_unavailable" if not public and missing else "no_matching_records")


@contextmanager
def _sync_guard():
    from api import syncAPI
    lock = syncAPI._rt()[2]
    if not lock.acquire(blocking=False):
        raise HTTPException(409, "Another provider operation is running. Try again when it finishes.")
    try:
        if syncAPI._is_sync_running():
            raise HTTPException(409, "Another sync is running. Try again when it finishes.")
        yield
    finally:
        lock.release()


def _read_current(ops, config, provider, instance, feature):
    with _capture_mode_env(pid=provider, instance=instance, feat=feature):
        result = ops.build_index(config, feature=feature)
    if not isinstance(result, Mapping):
        raise RuntimeError("Provider returned no usable inventory")
    result = {str(k): dict(v) for k, v in result.items() if isinstance(v, Mapping)}
    if feature == "history":
        return collapse_history_latest(result)
    return result


def _emit(event, **data):
    from api.editorAPI import _emit_editor_operation
    _emit_editor_operation(event, **data)
    LOG.info("Editor removal: %s provider=%s feature=%s count=%s",
             event, data.get("dst"), data.get("feature"), data.get("count", data.get("attempted", "")))


def _remove_target(cfg, feature, target, *, dry_run):
    from api import editorAPI as api

    provider, instance = target["provider"], target["instance"]
    ops = load_sync_ops(provider)
    records = target["records"]
    single_watch = provider == "FLOPPY" and feature == "history"
    if feature == "history" and any(_event(k, v) for k, v in records.items()):
        raise ValueError("Individual watch dates cannot be removed here")
    view = config_with_history_rewatches(build_provider_config_view(cfg, provider, instance), False)
    current = _RecordIndex(_read_current(ops, view, provider, instance, feature))
    removed_keys, absent_keys, unresolved_keys = [], [], []
    still_watched = {}
    matched = {}
    for key, item in records.items():
        matches = current.matches(key, item, same_account=True)
        if not matches:
            absent_keys.append(key)
        elif len(matches) != 1:
            unresolved_keys.append(key)
        else:
            dest_key, dest = matches[0]
            dest = dict(dest)
            if feature == "history" and not single_watch:
                # A collapsed status can retain the newest play's native ID.
                # Keep aggregate IDs needed to clear status, never one play's ID.
                for field in (*EVENT_ID_FIELDS, "_mdblist_play_id", "play_id", "watched_at", "_cw_event_key", "_cw_rewatch_sync"):
                    dest.pop(field, None)
            matched[key] = (dest_key, dest)
    if matched:
        with _crosswatch_write_env() if provider == "CROSSWATCH" else _capture_mode_env(pid=provider, instance=instance, feat=feature):
            # Capture mode prefers provider-native IDs. Keep the same identities
            # for confirmation after leaving that context (which prefers TMDB).
            write_keys = {key: canonical_key(dest) for key, (_, dest) in matched.items()}
            result = apply_remove(dst_ops=ops, cfg=view, dst_name=provider, feature=feature,
                                  items=[v for _, v in matched.values()], dry_run=dry_run,
                                  emit=lambda event, **data: _emit(event, instance=instance, **data),
                                  dbg=lambda *a, **k: None, chunk_size=100, chunk_pause_ms=0)
        if not dry_run:
            decision = compute_effective_remove(
                attempted_keys=list(write_keys.values()),
                provider_confirmed_count=result.get("confirmed", 0),
                provider_confirmed_keys=result.get("confirmed_keys"),
                provider_unresolved_count=result.get("unresolved", 0), provider_errors=result.get("errors", 0),
            )
            confirmed = set(decision["success_keys"])
            remaining = _RecordIndex(_read_current(ops, view, provider, instance, feature))
            for key, (dest_key, dest) in matched.items():
                survivors = remaining.matches(dest_key, dest, same_account=True)
                deleted_watch = str(dest.get("_floppy_consumption_id") or "") if single_watch else ""
                watch_changed = bool(deleted_watch) and len(survivors) == 1 and all(
                    str(item.get("_floppy_consumption_id") or "") not in {"", deleted_watch}
                    for _, item in survivors
                )
                if write_keys[key] not in confirmed or (survivors and not watch_changed):
                    unresolved_keys.append(key)
                else:
                    removed_keys.append(key)
                    if survivors:
                        still_watched[key] = survivors[0][1]
            if not result.get("ok", True) and not removed_keys:
                LOG.warning("Editor removal was not confirmed for %s/%s", provider, instance)
    if not dry_run and removed_keys:
        state = sqlite_state.load_state_features(api._STATE_BASE, {feature})
        current = _items(state, provider, instance, feature)
        if single_watch:
            # Refresh only affected inventory rows. Keep surviving watches and
            # the pair baselines used by the next sync to compare providers.
            updated = dict(current)
            for key in removed_keys:
                deleted_watch = str(matched[key][1].get("_floppy_consumption_id") or "")
                for k, value in current.items():
                    if not _matches(key, records[key], k, value, same_account=True, include_viewings=True):
                        continue
                    if key in still_watched:
                        if not _event(k, value):
                            updated[k] = still_watched[key]
                        elif deleted_watch and str(value.get("_floppy_consumption_id") or "") == deleted_watch:
                            updated.pop(k, None)
                    else:
                        updated.pop(k, None)
            node = (state.get("providers") or {}).get(provider) or {}
            if instance != "default":
                node = (node.get("instances") or {}).get(instance) or {}
            sqlite_state.save_feature_baseline(api._STATE_BASE, provider=provider, instance=instance,
                                              feature=feature, items=updated,
                                              checkpoint=(node.get(feature) or {}).get("checkpoint"))
        else:
            keys = [k for k, v in current.items() if any(
                _matches(key, records[key], k, v, same_account=True, include_viewings=feature == "history") for key in removed_keys)]
            sqlite_state.remove_baseline_items(api._STATE_BASE, provider, instance, feature, keys)
    return dict(ok=not unresolved_keys, attempted=len(records), confirmed=len(removed_keys),
                removed=len(removed_keys), skipped=len(absent_keys), unresolved=len(unresolved_keys), errors=0,
                still_watched=len(still_watched),
                confirmed_keys=removed_keys, skipped_keys=absent_keys, unresolved_keys=unresolved_keys,
                dry_run=dry_run)


def remove_selected(payload, request=None):
    if payload.get("confirmed") is not True and payload.get("dry_run") is not True:
        raise HTTPException(400, "Confirm removal before continuing")
    with _sync_guard():
        cfg, feature, selected, targets, digest, _ = _plan(payload, request)
        if payload.get("preview_id") != digest:
            raise HTTPException(409, "The removal targets changed. Check the selection again.")
        requested = payload.get("providers")
        if not isinstance(requested, list) or not requested:
            raise HTTPException(400, "Select at least one provider profile")
        target_map = {(t["provider"], t["instance"]): t for t in targets}
        identities = []
        for row in requested:
            if not isinstance(row, Mapping):
                raise HTTPException(400, "Invalid provider selection")
            identity = (str(row.get("provider") or "").upper(), normalize_instance_id(row.get("instance")))
            if identity not in target_map:
                raise HTTPException(400, "Selected provider profile has no matching synced records")
            if identity not in identities:
                identities.append(identity)
        results = []
        for identity in identities:
            target = target_map[identity]
            entry = {k: target[k] for k in ("provider", "instance", "label", "instance_label", "display")}
            try:
                result = _remove_target(cfg, feature, target, dry_run=payload.get("dry_run") is True)
                entry.update(ok=result["ok"], result=result)
            except Exception:
                LOG.exception("Editor removal failed for %s/%s", *identity)
                entry.update(ok=False, error="remove_failed", result=dict(errors=1))
            results.append(entry)
        totals = {key: sum(int(r["result"].get(key, 0)) for r in results)
                  for key in ("attempted", "confirmed", "removed", "skipped", "unresolved", "errors")}
        return dict(ok=all(r["ok"] for r in results), operation="remove", kind=feature,
                    selected=len(selected), dry_run=payload.get("dry_run") is True, results=results, **totals)

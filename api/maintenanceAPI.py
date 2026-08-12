# /api/maintenanceAPI.py
# CrossWatch - Maintenance API for CrossWatch
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import copy
import io
import json
import logging
import os
import shutil
import threading
from collections.abc import Mapping
from datetime import datetime, timezone
from fnmatch import fnmatch
from pathlib import Path
from typing import Any

from fastapi import APIRouter, Body, File, Query, UploadFile
from fastapi.responses import StreamingResponse

from cw_platform.local_db import crosswatch_db_path
from cw_platform.local_db.currently_watching import clear_streams as clear_currently_watching_streams
from cw_platform.local_db.currently_watching import stream_count as currently_watching_stream_count
from cw_platform.local_db.diagnostics import diagnostics as local_db_diagnostics
from cw_platform.local_db.legacy_files import (
    LAST_SYNC_JSON,
    LEGACY_DIR,
    STATE_JSON,
    STATE_MANUAL_JSON,
    STATISTICS_JSON,
    legacy_path,
)
from cw_platform.local_db.sync_reports import (
    base_path_from_report_dir,
    clear_reports as clear_sync_reports,
    report_count as sync_report_count,
)
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.provider_instances import normalize_instance_id

_LOG = logging.getLogger("crosswatch.api.maintenance")

router = APIRouter(prefix="/api/maintenance", tags=["maintenance"])

SYNC_STATE_PATTERNS = (
    "*pair_alias*.json",
    "*source_alias*.json",
    "*anime_episode_alias*.json",
    "*anime_episode_map*.json",
    "*anime_resolve*.json",
    "*.unresolved*.json",
    "*history.cache*.json",
    "*_history.index*.json",
    "*watermark*.json",
    "tombstones.json",
    "*.tombstones.json",
)

CW_STATE_KEEP_DIRS = {"id"}
CW_STATE_KEEP_FILES: set[str] = {"anime_mapping_overrides.json"}


def _is_sync_state_file(name: str) -> bool:
    candidate = str(name or "")
    return any(fnmatch(candidate, pattern) for pattern in SYNC_STATE_PATTERNS)


def _sync_state_files() -> list[Path]:
    _, _, CW_STATE_DIR, *_ = _cw()
    if not CW_STATE_DIR.exists():
        return []
    found: dict[str, Path] = {}
    for pattern in SYNC_STATE_PATTERNS:
        for p in CW_STATE_DIR.glob(pattern):
            if p.is_file() and p.name not in CW_STATE_KEEP_FILES:
                found[p.name] = p
    return [found[name] for name in sorted(found)]


def _sync_state_storage_paths(config_dir: Path, scoped: list[Path] | None = None) -> list[Path]:
    paths = [legacy_path(config_dir, STATE_JSON)]
    try:
        paths.append(crosswatch_db_path(config_dir))
    except Exception:
        pass
    paths.extend(scoped or _sync_state_files())
    return list(dict.fromkeys(paths))


def _cw() -> tuple[Any, Any, Any, Any, Any, Any]:
    from crosswatch import CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _append_log

    def _load_statistics_state() -> dict[str, Any]:
        try:
            return StateStore(CONFIG_DIR).load_state_features({"watchlist", "history", "ratings", "playlists"}) or {}
        except Exception:
            return {}

    return CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_statistics_state, _append_log


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
            if p.name in CW_STATE_KEEP_FILES or _is_sync_state_file(p.name):
                continue
            try:
                p.unlink(missing_ok=True)
                removed.append(p.name)
            except Exception:
                pass
    return removed


# --- CrossWatch tracker (.cw_provider) functions ---
def _cw_tracker_config(config_dir: Path) -> dict[str, Any]:
    cfg_path = config_dir / "config.json"
    try:
        cfg = json.loads(cfg_path.read_text("utf-8"))
    except Exception:
        return {}
    return cfg if isinstance(cfg, dict) else {}


def _cw_tracker_configured_instance(cfg: Mapping[str, Any], provider_instance: Any = None) -> str:
    requested = normalize_instance_id(provider_instance)
    if requested == "default":
        return "default"
    node = cfg.get("crosswatch") if isinstance(cfg, Mapping) else {}
    instances = node.get("instances") if isinstance(node, Mapping) else {}
    if not isinstance(instances, Mapping):
        return "default"
    for key in instances.keys():
        candidate = normalize_instance_id(key)
        if candidate == requested:
            return candidate
    return "default"


def _cw_tracker_base_root(config_dir: Path, cfg: Mapping[str, Any]) -> Path:
    cw_cfg = cfg.get("crosswatch") if isinstance(cfg, Mapping) else {}
    root = (
        cw_cfg.get("root_dir")
        or cw_cfg.get("root")
        or cw_cfg.get("dir")
        or None
    ) if isinstance(cw_cfg, Mapping) else None
    if not root:
        root = ".cw_provider"

    p = Path(root)
    if not p.is_absolute():
        p = config_dir / p
    return p


def _cw_tracker_root(config_dir: Path, provider_instance: Any = None) -> Path:
    cfg = _cw_tracker_config(config_dir)
    inst = _cw_tracker_configured_instance(cfg, provider_instance)
    base = _cw_tracker_base_root(config_dir, cfg)
    if inst == "default":
        return base
    return base / "profiles" / inst


def _cw_tracker_context(config_dir: Path, provider_instance: Any = None) -> tuple[str, Path]:
    cfg = _cw_tracker_config(config_dir)
    inst = _cw_tracker_configured_instance(cfg, provider_instance)
    base = _cw_tracker_base_root(config_dir, cfg)
    root = base if inst == "default" else base / "profiles" / inst
    return inst, root


def _json_files_in_dir(root: Path) -> list[Path]:
    safe_root = root.resolve(strict=False)
    if not safe_root.exists():
        return []
    out: list[Path] = []
    for path in safe_root.glob("*.json"):
        resolved = path.resolve(strict=False)
        try:
            resolved.relative_to(safe_root)
        except ValueError:
            continue
        if resolved.is_file():
            out.append(resolved)
    return out


def _cw_tracker_snapshot_dir(root: Path) -> Path:
    safe_root = root.resolve(strict=False)
    snaps = (safe_root / "snapshots").resolve(strict=False)
    try:
        snaps.relative_to(safe_root)
    except ValueError as exc:
        raise ValueError("Invalid tracker snapshot path") from exc
    return snaps


def _file_meta(path: Path) -> dict[str, Any]:
    try:
        st = path.stat()
    except Exception:
        return {"name": path.name, "size": 0, "mtime": None}
    return {
        "name": path.name,
        "size": st.st_size,
        "mtime": datetime.fromtimestamp(st.st_mtime, timezone.utc).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        ),
    }


def _path_usage(path: Path) -> dict[str, int | None]:
    """Return recursive file count, byte size and newest mtime."""
    files = 0
    size = 0
    newest: int | None = None
    if not path.exists():
        return {"files": 0, "bytes": 0, "modified": None}

    candidates: list[Path]
    if path.is_file():
        candidates = [path]
    else:
        candidates = []
        try:
            for base, _, names in os.walk(path, followlinks=False):
                candidates.extend(Path(base) / name for name in names)
        except Exception:
            pass

    for candidate in candidates:
        try:
            if candidate.is_symlink() or not candidate.is_file():
                continue
            stat = candidate.stat()
            files += 1
            size += int(stat.st_size)
            stamp = int(stat.st_mtime)
            newest = stamp if newest is None else max(newest, stamp)
        except Exception:
            continue
    return {"files": files, "bytes": size, "modified": newest}


def _paths_usage(paths: list[Path]) -> dict[str, int | None]:
    usages = [_path_usage(path) for path in paths]
    modified = [int(item["modified"]) for item in usages if item["modified"]]
    return {
        "files": sum(int(item["files"] or 0) for item in usages),
        "bytes": sum(int(item["bytes"] or 0) for item in usages),
        "modified": max(modified, default=None),
    }


def _cleanup_summary(
    before: dict[str, int | None],
    after: dict[str, int | None],
    *,
    removed_items: int = 0,
) -> dict[str, int]:
    """Return receipt for destructive maintenance actions."""
    return {
        "removed_files": max(0, int(before.get("files") or 0) - int(after.get("files") or 0)),
        "removed_items": max(0, int(removed_items or 0)),
        "freed_bytes": max(0, int(before.get("bytes") or 0) - int(after.get("bytes") or 0)),
    }


def _statistics_artifact_paths(
    cache_dir: Path,
    config_dir: Path,
    state_dir: Path,
    stats_path: Path,
    *,
    include_stats: bool,
    include_state: bool,
    include_reports: bool,
    include_insights: bool,
) -> list[Path]:
    paths: list[Path] = []
    if include_stats:
        paths.append(stats_path)
        try:
            from cw_platform.local_db import crosswatch_db_path

            paths.append(crosswatch_db_path(config_dir))
        except Exception:
            pass
    if include_state:
        paths.extend(_sync_state_storage_paths(config_dir, []))
    if include_reports:
        try:
            from services.statistics import REPORT_DIR
        except Exception:
            REPORT_DIR = config_dir / "sync_reports"
        paths.extend(Path(REPORT_DIR).glob("sync-*.json"))
    if include_insights:
        for root in (state_dir, cache_dir, config_dir):
            if not root.exists():
                continue
            for pattern in ("insights*.json", ".insights*.json", "insight*.json", "series*.json"):
                paths.extend(root.glob(pattern))
    return list(dict.fromkeys(paths))


def _read_json(path: Path) -> Any:
    try:
        return json.loads(path.read_text("utf-8"))
    except Exception:
        return None


def _load_sync_state(config_dir: Path) -> dict[str, Any]:
    payload = StateStore(config_dir).load_state()
    return payload if isinstance(payload, dict) else {}


def _save_sync_state(config_dir: Path, payload: Mapping[str, Any]) -> None:
    StateStore(config_dir).save_state(payload)


def _metric(label: str, value: Any, fmt: str = "number") -> dict[str, Any]:
    return {"label": label, "value": value, "format": fmt}


def _sync_state_inventory(config_dir: Path) -> tuple[int, int]:
    payload = _load_sync_state(config_dir)
    providers = payload.get("providers") if isinstance(payload, dict) else None
    if not isinstance(providers, dict):
        return 0, 0

    baseline_count = 0
    feature_names = {"history", "ratings", "watchlist", "playlists", "progress"}
    for provider in providers.values():
        if not isinstance(provider, dict):
            continue
        for name, block in provider.items():
            if str(name).lower() not in feature_names or not isinstance(block, dict):
                continue
            if block.get("baseline") is not None or block:
                baseline_count += 1
    return len(providers), baseline_count


def _sync_state_baseline_inventory(config_dir: Path) -> dict[str, Any]:
    payload = _load_sync_state(config_dir)
    providers = payload.get("providers") if isinstance(payload, dict) else None
    if not isinstance(providers, dict):
        return {"providers": 0, "baselines": 0, "items": 0, "largest": None}

    feature_names = {"history", "ratings", "watchlist", "playlists", "progress"}
    baselines = 0
    items_total = 0
    largest: dict[str, Any] | None = None

    def _visit_provider(provider_name: str, node: Any, instance: str = "default") -> None:
        nonlocal baselines, items_total, largest
        if not isinstance(node, dict):
            return
        for name, block in node.items():
            feature = str(name).lower()
            if feature not in feature_names or not isinstance(block, dict):
                continue
            baseline = block.get("baseline") or {}
            items = baseline.get("items") if isinstance(baseline, dict) else None
            count = len(items) if isinstance(items, dict) else 0
            if block.get("baseline") is not None or block:
                baselines += 1
            items_total += count
            row = {
                "provider": provider_name,
                "instance": instance,
                "feature": feature,
                "items": count,
            }
            if largest is None or count > int(largest.get("items") or 0):
                largest = row

        insts = node.get("instances")
        if isinstance(insts, dict):
            for inst_name, inst_node in insts.items():
                _visit_provider(provider_name, inst_node, str(inst_name or "default"))

    for provider_name, provider_node in providers.items():
        _visit_provider(str(provider_name), provider_node)

    return {
        "providers": len(providers),
        "baselines": baselines,
        "items": items_total,
        "largest": largest,
    }


FEATURE_NAMES = {"history", "ratings", "watchlist", "playlists", "progress"}


def _feature_item_count(block: Any) -> int:
    if not isinstance(block, dict):
        return 0
    baseline = block.get("baseline")
    items = baseline.get("items") if isinstance(baseline, dict) else None
    return len(items) if isinstance(items, dict) else 0


def _count_feature_blocks(node: Any) -> tuple[int, int]:
    if not isinstance(node, dict):
        return 0, 0
    baselines = 0
    items = 0
    for name, block in node.items():
        if str(name).lower() in FEATURE_NAMES and isinstance(block, dict):
            baselines += 1
            items += _feature_item_count(block)
    insts = node.get("instances")
    if isinstance(insts, dict):
        for inst_node in insts.values():
            b, i = _count_feature_blocks(inst_node)
            baselines += b
            items += i
    return baselines, items


def _has_state_payload(node: Any) -> bool:
    if not isinstance(node, dict):
        return False
    for name, block in node.items():
        if str(name).lower() in FEATURE_NAMES and isinstance(block, dict):
            return True
    manual = node.get("manual")
    if isinstance(manual, dict) and manual:
        return True
    return False


def _load_config_for_state_prune(config_dir: Path) -> dict[str, Any] | None:
    try:
        from cw_platform.config_base import load_config

        cfg = load_config() or {}
        if isinstance(cfg, dict):
            return cfg
    except Exception:
        pass
    raw = _read_json(config_dir / "config.json")
    return raw if isinstance(raw, dict) else None


def _configured_state_refs(cfg: Mapping[str, Any]) -> set[tuple[str, str]]:
    refs: set[tuple[str, str]] = set()

    def add(provider: Any, instance: Any = "default") -> None:
        key = str(provider or "").strip().upper()
        if key:
            refs.add((key, normalize_instance_id(instance)))

    for pair in cfg.get("pairs") or []:
        if not isinstance(pair, Mapping):
            continue
        add(pair.get("source"), pair.get("source_instance"))
        add(pair.get("target"), pair.get("target_instance"))

    scrobble = cfg.get("scrobble")
    watch = scrobble.get("watch") if isinstance(scrobble, Mapping) else None
    routes = watch.get("routes") if isinstance(watch, Mapping) else None
    for route in routes if isinstance(routes, list) else []:
        if not isinstance(route, Mapping):
            continue
        add(route.get("provider"), route.get("provider_instance") or route.get("providerInstance"))
        add(route.get("sink"), route.get("sink_instance") or route.get("sinkInstance"))

        ratings = route.get("options")
        ratings = ratings.get("ratings") if isinstance(ratings, Mapping) else None
        targets = ratings.get("targets") if isinstance(ratings, Mapping) else None
        if isinstance(targets, Mapping):
            for provider, enabled in targets.items():
                if enabled is not False:
                    add(provider, route.get("sink_instance") or route.get("sinkInstance"))
        elif isinstance(targets, list):
            for provider in targets:
                add(provider, route.get("sink_instance") or route.get("sinkInstance"))

    return refs


def _prune_state_payload(payload: dict[str, Any], cfg: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, int], list[dict[str, Any]]]:
    pruned = copy.deepcopy(payload)
    providers = pruned.get("providers")
    if not isinstance(providers, dict):
        return pruned, {"removed_providers": 0, "removed_instances": 0, "removed_baselines": 0, "removed_items": 0}, []

    refs = _configured_state_refs(cfg)
    removed = {"removed_providers": 0, "removed_instances": 0, "removed_baselines": 0, "removed_items": 0}
    details: list[dict[str, Any]] = []

    def note(kind: str, provider: str, instance: str, baselines: int, items: int) -> None:
        removed["removed_baselines"] += baselines
        removed["removed_items"] += items
        details.append({
            "kind": kind,
            "provider": provider,
            "instance": instance,
            "baselines": baselines,
            "items": items,
        })

    for provider_name in list(providers.keys()):
        provider_key = str(provider_name or "").strip().upper()
        node = providers.get(provider_name)
        if not isinstance(node, dict):
            providers.pop(provider_name, None)
            removed["removed_providers"] += 1
            continue

        keep_default = (provider_key, "default") in refs
        insts = node.get("instances")
        if isinstance(insts, dict):
            for inst_name in list(insts.keys()):
                inst = normalize_instance_id(inst_name)
                if (provider_key, inst) in refs:
                    continue
                inst_node = insts.pop(inst_name, None)
                baselines, items = _count_feature_blocks(inst_node)
                removed["removed_instances"] += 1
                note("instance", provider_key, inst, baselines, items)
            if not insts:
                node.pop("instances", None)

        if not keep_default:
            removed_default = False
            for feat in list(node.keys()):
                if str(feat).lower() not in FEATURE_NAMES:
                    continue
                block = node.pop(feat, None)
                removed_default = True
                note("default_feature", provider_key, "default", 1, _feature_item_count(block))
            if isinstance(node.get("manual"), dict):
                node.pop("manual", None)
                removed_default = True
            if removed_default and not _has_state_payload(node) and not isinstance(node.get("instances"), dict):
                providers.pop(provider_name, None)
                removed["removed_providers"] += 1
        elif not _has_state_payload(node) and not isinstance(node.get("instances"), dict):
            providers.pop(provider_name, None)
            removed["removed_providers"] += 1

    return pruned, removed, details


def _write_json_compact_atomic(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".compact.tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), "utf-8")
        os.replace(tmp, path)
    except Exception:
        try:
            tmp.unlink(missing_ok=True)
        except Exception:
            pass
        raise


def _currently_playing_count(config_dir: Path) -> int:
    return currently_watching_stream_count(config_dir, active_only=True)


def maintenance_action_status(action: str) -> dict[str, Any]:
    """Build the read-only inventory shown when a maintenance action is selected."""
    CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_statistics_state, _append_log = _cw()
    action = str(action or "").strip().lower()
    response: dict[str, Any] = {"ok": True, "action": action, "metrics": []}

    if action == "state":
        providers, baselines = _sync_state_inventory(CONFIG_DIR)
        scoped = _sync_state_files()
        scoped_usage = _paths_usage(_sync_state_storage_paths(CONFIG_DIR, scoped))
        response.update(
            title="Rebuild sync state",
            note="These provider baselines are removed together with pair-scoped history aliases, mapping caches, watermarks and tombstones; the next sync reads fresh provider data and rebuilds translated aliases.",
            metrics=[
                _metric("Providers", providers),
                _metric("Feature baselines", baselines),
                _metric("Pair mapping files", len(scoped)),
                _metric("State storage", scoped_usage["bytes"], "bytes"),
                _metric("Last updated", scoped_usage["modified"], "datetime"),
            ],
        )
    elif action in {"state-file", "state-file-prune"}:
        usage = _paths_usage(_sync_state_storage_paths(CONFIG_DIR))
        inv = _sync_state_baseline_inventory(CONFIG_DIR)
        largest = inv.get("largest") if isinstance(inv.get("largest"), dict) else None
        largest_label = "None"
        if largest:
            inst = str(largest.get("instance") or "default")
            suffix = "" if inst == "default" else f" ({inst})"
            largest_label = f"{largest.get('provider')} {largest.get('feature')}{suffix}"
        metrics = [
            _metric("File size", usage["bytes"], "bytes"),
            _metric("Providers", inv.get("providers", 0)),
            _metric("Feature baselines", inv.get("baselines", 0)),
            _metric("Baseline items", inv.get("items", 0)),
            _metric("Largest baseline", largest_label, "text"),
            _metric("Last updated", usage["modified"], "datetime"),
        ]
        if action == "state-file-prune":
            cfg = _load_config_for_state_prune(CONFIG_DIR)
            payload = _load_sync_state(CONFIG_DIR)
            stale = {"removed_providers": 0, "removed_instances": 0, "removed_baselines": 0, "removed_items": 0}
            if isinstance(cfg, dict) and isinstance(payload, dict):
                _, stale, _ = _prune_state_payload(payload, cfg)
            metrics.extend([
                _metric("Stale providers", stale.get("removed_providers", 0)),
                _metric("Stale instances", stale.get("removed_instances", 0)),
                _metric("Stale baselines", stale.get("removed_baselines", 0)),
                _metric("Stale items", stale.get("removed_items", 0)),
            ])
        response.update(
            title="Prune sync state" if action == "state-file-prune" else "Sync state",
            note=(
                "Creates an app-state backup, then removes provider or instance baselines that are no longer referenced by configured sync pairs or scrobbler routes."
                if action == "state-file-prune"
                else "Inspects the local sync state baseline store."
            ),
            metrics=metrics,
        )
    elif action in {"support-export", "support-bundle"}:
        from services.support import list_scopes

        totals_raw = list_scopes().get("totals")
        totals: Mapping[str, Any] = totals_raw if isinstance(totals_raw, Mapping) else {}
        usage = _paths_usage(_sync_state_storage_paths(CONFIG_DIR))
        metrics = [
            _metric("Sync pairs", int(totals.get("pairs") or 0)),
            _metric("Feature baselines", int(totals.get("baselines") or 0)),
            _metric("Baseline items", int(totals.get("items") or 0)),
            _metric("Unreferenced baselines", int(totals.get("orphan_baselines") or 0)),
            _metric("State storage", usage["bytes"], "bytes"),
            _metric("Last updated", usage["modified"], "datetime"),
        ]
        if action == "support-export":
            response.update(
                title="Export sync state",
                note="Rebuilds the classic state.json from the database so it can be attached to a bug report. Choose all pairs or a single pair. Read-only; no secrets are included.",
                metrics=metrics,
            )
        else:
            response.update(
                title="Support bundle",
                note="Packs state.json, a redacted config, database and event-archive diagnostics, recent sync reports and log tails into one ZIP. Tokens and passwords are masked. Read-only.",
                metrics=metrics,
            )
    elif action == "database-health":
        health = local_db_diagnostics(CONFIG_DIR)
        tables_raw = health.get("table_counts")
        orphans_raw = health.get("orphan_counts")
        stale_raw = health.get("stale_counts")
        last_sync_raw = health.get("last_sync")
        tables: Mapping[str, Any] = tables_raw if isinstance(tables_raw, Mapping) else {}
        orphans: Mapping[str, Any] = orphans_raw if isinstance(orphans_raw, Mapping) else {}
        stale: Mapping[str, Any] = stale_raw if isinstance(stale_raw, Mapping) else {}
        last_sync: Mapping[str, Any] = last_sync_raw if isinstance(last_sync_raw, Mapping) else {}
        orphan_total = sum(int(value or 0) for value in orphans.values())
        response.update(
            title="Database health",
            note="Checks the local CrossWatch database integrity, schema, table counts and row consistency. Read-only.",
            metrics=[
                _metric("Integrity", health.get("integrity", "unknown"), "text"),
                _metric("Schema version", health.get("schema_version") or 0),
                _metric("Database size", int(health.get("size_bytes") or 0), "bytes"),
                _metric("Tables", len(tables)),
                _metric("Feature baselines", int(tables.get("provider_feature_state") or 0)),
                _metric("Baseline items", int(tables.get("baseline_items") or 0)),
                _metric("Orphan rows", orphan_total),
                _metric("Expired TTL rows", int(stale.get("ttl_expired") or 0)),
                _metric("Last sync", last_sync.get("state_epoch"), "datetime"),
            ],
        )
    elif action == "cache":
        info = _scan_provider_cache()
        files = list(info.get("files") or [])
        retry_names = ("unresolved", "phantom", "tombstone", "blackbox", "flap", "health", "dropped")
        retry_files = sum(1 for item in files if any(token in str(item.get("name") or "").lower() for token in retry_names))
        response.update(
            title="Retry provider items",
            note="Clears provider runtime caches such as retry guards, phantom records and provider health. Sync aliases, mappings, history indexes, watermarks, tombstones and your custom anime mapping rules stay untouched.",
            metrics=[
                _metric("Runtime files", len(files)),
                _metric("Retry / guard files", retry_files),
                _metric("Storage", sum(int(item.get("size") or 0) for item in files), "bytes"),
                _metric("Preserved files", len(CW_STATE_KEEP_FILES)),
            ],
        )
    elif action == "tracker":
        root = _cw_tracker_root(CONFIG_DIR)
        info = _scan_cw_tracker(root)
        usage = _path_usage(root)
        counts = info.get("counts") or {}
        response.update(
            title="Reset local tracker",
            note="Tracker state and saved tracker snapshots can be selected independently with the checkboxes on the card.",
            metrics=[
                _metric("Tracker states", counts.get("state_files", 0)),
                _metric("Tracker snapshots", counts.get("snapshots", 0)),
                _metric("Storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "playing":
        db_path = crosswatch_db_path(CONFIG_DIR)
        active_count = _currently_playing_count(CONFIG_DIR)
        usage = _path_usage(db_path)
        response.update(
            title="Clear currently playing",
            note="Only CrossWatch's local live-playback sessions are affected; provider playback history is not changed.",
            metrics=[
                _metric("Active sessions", active_count),
                _metric("Storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "scrobbles":
        from services.activity import activity_path, list_events

        events = list_events(limit=1, kind="scrobble", group_routes=False)
        usage = _path_usage(activity_path())
        response.update(
            title="Clear Recent Scrobbles",
            note="Removes only scrobble rows from Recent Activity. Other activity and provider watch history remain.",
            metrics=[
                _metric("Scrobble rows", int(events.get("total") or 0)),
                _metric("Activity storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "stats":
        try:
            from services.statistics import REPORT_DIR
        except Exception:
            REPORT_DIR = CONFIG_DIR / "sync_reports"
        stats_path = Path(getattr(STATS, "path", legacy_path(CONFIG_DIR, STATISTICS_JSON)))
        stats_usage = _paths_usage(
            _statistics_artifact_paths(
                CACHE_DIR,
                CONFIG_DIR,
                CW_STATE_DIR,
                stats_path,
                include_stats=True,
                include_state=False,
                include_reports=False,
                include_insights=False,
            )
        )
        report_paths = list(Path(REPORT_DIR).glob("sync-*.json")) if Path(REPORT_DIR).exists() else []
        report_usage = _paths_usage(report_paths)
        report_rows = sync_report_count(base_path_from_report_dir(REPORT_DIR))
        last_report = report_usage["modified"] or (stats_usage["modified"] if report_rows else 0)
        insight_paths: list[Path] = []
        for root in (CW_STATE_DIR, CACHE_DIR, CONFIG_DIR):
            if not root.exists():
                continue
            for pattern in ("insights*.json", ".insights*.json", "insight*.json", "series*.json"):
                insight_paths.extend(root.glob(pattern))
        insight_usage = _paths_usage(list(dict.fromkeys(insight_paths)))
        response.update(
            title="Rebuild statistics",
            note="Statistics, saved sync reports and generated insight caches are rebuilt from future sync activity.",
            metrics=[
                _metric("Sync reports", report_rows + int(report_usage["files"] or 0)),
                _metric("Insight cache files", insight_usage["files"]),
                _metric("Data rebuilt", int(stats_usage["bytes"] or 0) + int(report_usage["bytes"] or 0) + int(insight_usage["bytes"] or 0), "bytes"),
                _metric("Last report", last_report, "datetime"),
            ],
        )
    elif action == "metadata":
        usage = _path_usage(CACHE_DIR)
        response.update(
            title="Refresh artwork & metadata",
            note="All cached artwork and metadata are removed and downloaded again only when CrossWatch needs them.",
            metrics=[
                _metric("Cached files", usage["files"]),
                _metric("Cache storage", usage["bytes"], "bytes"),
                _metric("Last updated", usage["modified"], "datetime"),
            ],
        )
    elif action == "captures":
        from services.snapshots import list_snapshots

        rows = list_snapshots()
        providers = {str(row.get("provider") or "").upper() for row in rows if row.get("provider")}
        newest = max((int(row.get("mtime") or 0) for row in rows), default=None)
        response.update(
            title="Clear all captures",
            note="Deletes saved provider captures only; automatic tracker snapshots are stored separately.",
            metrics=[
                _metric("Saved captures", len(rows)),
                _metric("Providers", len(providers)),
                _metric("Capture storage", sum(int(row.get("size") or 0) for row in rows), "bytes"),
                _metric("Latest capture", newest, "datetime"),
            ],
        )
    elif action == "defaults":
        snapshots_dir = CONFIG_DIR / "snapshots"
        paths = [
            legacy_path(CONFIG_DIR, LAST_SYNC_JSON),
            legacy_path(CONFIG_DIR, STATE_JSON),
            legacy_path(CONFIG_DIR, STATISTICS_JSON),
            CONFIG_DIR / "sync_reports",
            CONFIG_DIR / LEGACY_DIR,
            CW_STATE_DIR,
            CONFIG_DIR / ".cw_databases",
            _cw_tracker_root(CONFIG_DIR),
            CACHE_DIR,
            CONFIG_DIR / "tls",
        ]
        usages = [_path_usage(path) for path in paths]
        captures = _path_usage(Path(snapshots_dir))
        response.update(
            title="Factory reset",
            note="Deletes all local runtime data and backs up config.json. The event history archive (.cw_databases) is also removed. Saved captures are kept.",
            metrics=[
                _metric("Files removed", sum(int(item["files"] or 0) for item in usages)),
                _metric("Data removed", sum(int(item["bytes"] or 0) for item in usages), "bytes"),
                _metric("Captures kept", captures["files"]),
                _metric("Capture storage kept", captures["bytes"], "bytes"),
            ],
        )
    elif action in ("events-health", "events-optimize", "events-rebuild"):
        from cw_platform import event_archive as _ea
        st = _ea.status()
        db_path = Path(_ea.events_db_path())
        usage = _path_usage(db_path)
        events = int(st.get("events") or 0)
        acknowledged = int(st.get("acknowledged") or 0)
        runs = int(st.get("runs") or 0)
        size = int(usage.get("bytes") or 0)
        if action == "events-health":
            response.update(
                title="Health check",
                note="Checks database integrity, archive size and event totals. Read-only.",
                metrics=[
                    _metric("Events", events),
                    _metric("Acknowledged", acknowledged),
                    _metric("Archive size", size, "bytes"),
                    _metric("Available", "yes" if st.get("available") else "no"),
                ],
            )
        elif action == "events-optimize":
            response.update(
                title="Optimize archive",
                note="Compacts and re-indexes the event archive to reclaim space.",
                metrics=[
                    _metric("Events", events),
                    _metric("Archive size", size, "bytes"),
                    _metric("Last updated", usage.get("modified"), "datetime"),
                ],
            )
        else:
            response.update(
                title="Clear archive",
                note="Deletes all events from the archive. New syncs record events again automatically. This cannot be undone.",
                metrics=[
                    _metric("Events removed", events),
                    _metric("Sync runs removed", runs),
                    _metric("Archive size", size, "bytes"),
                ],
            )
    else:
        return {"ok": False, "error": "unknown_maintenance_action", "action": action}

    return response

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
    try:
        candidates = list(CW_STATE_DIR.iterdir())
    except Exception:
        candidates = []
    for p in candidates:
        if p.name in CW_STATE_KEEP_FILES or _is_sync_state_file(p.name):
            continue
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

    before_usage = _path_usage(CACHE_DIR)
    before = _scan_cache_dir(CACHE_DIR)
    removed = _clear_cache_dir(CACHE_DIR)
    after = _scan_cache_dir(CACHE_DIR)
    after_usage = _path_usage(CACHE_DIR)

    return {
        "ok": True,
        "root": str(CACHE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
        "summary": _cleanup_summary(before_usage, after_usage),
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
    core_names = {"history.json", "ratings.json", "watchlist.json", "progress.json"}
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
def crosswatch_tracker_status(
    provider_instance: str | None = Query("default"),
) -> dict[str, Any]:
    """Inspect CrossWatch tracker folder (.cw_provider)."""
    _, CONFIG_DIR, *_ = _cw()
    inst, root = _cw_tracker_context(CONFIG_DIR, provider_instance)
    info = _scan_cw_tracker(root)
    return {
        "ok": True,
        "provider_instance": inst,
        "root": str(root),
        **info,
    }


@router.get("/crosswatch-tracker/export")
def crosswatch_tracker_export(
    provider_instance: str | None = Query("default"),
) -> StreamingResponse:
    from services.editor import export_tracker_zip

    _, CONFIG_DIR, *_ = _cw()
    inst, _ = _cw_tracker_context(CONFIG_DIR, provider_instance)
    data = export_tracker_zip(inst)
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=crosswatch-tracker.zip"},
    )


@router.post("/crosswatch-tracker/import")
async def crosswatch_tracker_import(
    provider_instance: str | None = Query("default"),
    file: UploadFile = File(...),
) -> dict[str, Any]:
    from services.editor import import_tracker_upload

    _, CONFIG_DIR, *_ = _cw()
    inst, _ = _cw_tracker_context(CONFIG_DIR, provider_instance)
    payload = await file.read()
    filename = file.filename or "upload.json"
    try:
        stats = import_tracker_upload(payload, filename, inst)
    except ValueError:
        _LOG.warning("tracker import rejected: %s", filename, exc_info=True)
        return {"ok": False, "error": "Import failed; expected a valid CrossWatch tracker ZIP or JSON file."}
    return {"ok": True, "provider_instance": inst, **stats}


@router.post("/clear-state")
def clear_state_minimal() -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    state_path = legacy_path(CONFIG_DIR, STATE_JSON)
    db_existed = False
    try:
        db_existed = crosswatch_db_path(CONFIG_DIR).exists()
    except Exception:
        db_existed = False
    existed = state_path.exists() or db_existed
    scoped = _sync_state_files()
    before_usage = _paths_usage(_sync_state_storage_paths(CONFIG_DIR, scoped))
    try:
        StateStore(CONFIG_DIR).clear_state()
        state_path.unlink(missing_ok=True)
        removed_scoped: list[str] = []
        for p in scoped:
            if _safe_remove_path(p):
                removed_scoped.append(p.name)
        after_usage = _paths_usage(_sync_state_storage_paths(CONFIG_DIR))
        return {
            "ok": True,
            "path": str(state_path),
            "existed": bool(existed),
            "removed_sync_state": removed_scoped,
            "summary": _cleanup_summary(before_usage, after_usage),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": "clear_state_failed",
            "path": str(state_path),
            "existed": bool(existed),
        }


@router.post("/state-file/compact")
def compact_state_file() -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    state_path = legacy_path(CONFIG_DIR, STATE_JSON)
    before = _paths_usage(_sync_state_storage_paths(CONFIG_DIR, []))

    try:
        state_exists = state_path.exists() or crosswatch_db_path(CONFIG_DIR).exists()
    except Exception:
        state_exists = state_path.exists()
    if not state_exists:
        return {
            "ok": True,
            "path": str(state_path),
            "existed": False,
            "backup": None,
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    payload = _load_sync_state(CONFIG_DIR)
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "error": "state_json_invalid",
            "path": str(state_path),
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    backup: dict[str, Any] | None = None
    try:
        from services.backups import create_backup

        backup = create_backup(
            scope="app_state",
            label="pre-state-compact",
            trigger="maintenance_state_compact",
        )
    except Exception:
        _LOG.exception("state compact backup failed")
        return {
            "ok": False,
            "error": "backup_failed",
            "path": str(state_path),
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    try:
        _save_sync_state(CONFIG_DIR, payload)
    except Exception:
        _LOG.exception("state compact write failed")
        return {
            "ok": False,
            "error": "compact_failed",
            "path": str(state_path),
            "backup": backup,
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    after = _paths_usage(_sync_state_storage_paths(CONFIG_DIR, []))
    summary = {
        "removed_files": 0,
        "removed_items": 0,
        "freed_bytes": max(0, int(before.get("bytes") or 0) - int(after.get("bytes") or 0)),
    }
    inv = _sync_state_baseline_inventory(CONFIG_DIR)
    return {
        "ok": True,
        "path": str(state_path),
        "existed": True,
        "backup": backup,
        "before_bytes": int(before.get("bytes") or 0),
        "after_bytes": int(after.get("bytes") or 0),
        "summary": summary,
        "inventory": inv,
    }


@router.post("/state-file/prune")
def prune_state_file() -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    state_path = legacy_path(CONFIG_DIR, STATE_JSON)
    before = _paths_usage(_sync_state_storage_paths(CONFIG_DIR, []))

    try:
        state_exists = state_path.exists() or crosswatch_db_path(CONFIG_DIR).exists()
    except Exception:
        state_exists = state_path.exists()
    if not state_exists:
        return {
            "ok": True,
            "path": str(state_path),
            "existed": False,
            "backup": None,
            "removed": {"removed_providers": 0, "removed_instances": 0, "removed_baselines": 0, "removed_items": 0},
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    payload = _load_sync_state(CONFIG_DIR)
    if not isinstance(payload, dict):
        return {
            "ok": False,
            "error": "state_json_invalid",
            "path": str(state_path),
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    cfg = _load_config_for_state_prune(CONFIG_DIR)
    if not isinstance(cfg, dict):
        return {
            "ok": False,
            "error": "config_unavailable",
            "path": str(state_path),
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    pruned, removed, details = _prune_state_payload(payload, cfg)
    if removed.get("removed_baselines", 0) <= 0 and removed.get("removed_instances", 0) <= 0 and removed.get("removed_providers", 0) <= 0:
        return {
            "ok": True,
            "path": str(state_path),
            "existed": True,
            "backup": None,
            "removed": removed,
            "details": details,
            "before_bytes": int(before.get("bytes") or 0),
            "after_bytes": int(before.get("bytes") or 0),
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
            "inventory": _sync_state_baseline_inventory(CONFIG_DIR),
        }

    backup: dict[str, Any] | None = None
    try:
        from services.backups import create_backup

        backup = create_backup(
            scope="app_state",
            label="pre-state-prune",
            trigger="maintenance_state_prune",
        )
    except Exception:
        _LOG.exception("state prune backup failed")
        return {
            "ok": False,
            "error": "backup_failed",
            "path": str(state_path),
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    try:
        _save_sync_state(CONFIG_DIR, pruned)
    except Exception:
        _LOG.exception("state prune write failed")
        return {
            "ok": False,
            "error": "prune_failed",
            "path": str(state_path),
            "backup": backup,
            "summary": {"removed_files": 0, "removed_items": 0, "freed_bytes": 0},
        }

    after = _paths_usage(_sync_state_storage_paths(CONFIG_DIR, []))
    summary = {
        "removed_files": 0,
        "removed_items": int(removed.get("removed_items") or 0),
        "freed_bytes": max(0, int(before.get("bytes") or 0) - int(after.get("bytes") or 0)),
    }
    return {
        "ok": True,
        "path": str(state_path),
        "existed": True,
        "backup": backup,
        "removed": removed,
        "details": details,
        "before_bytes": int(before.get("bytes") or 0),
        "after_bytes": int(after.get("bytes") or 0),
        "summary": summary,
        "inventory": _sync_state_baseline_inventory(CONFIG_DIR),
    }


@router.post("/crosswatch-tracker/clear")
def crosswatch_tracker_clear(
    clear_state: bool = Body(True),
    clear_snapshots: bool = Body(False),
    provider_instance: str | None = Body("default"),
) -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    inst, root = _cw_tracker_context(CONFIG_DIR, provider_instance)
    state_paths = _json_files_in_dir(root)
    snapshot_paths = _json_files_in_dir(_cw_tracker_snapshot_dir(root)) if clear_snapshots else []

    before = _scan_cw_tracker(root)
    selected_before_paths = []
    if clear_state:
        selected_before_paths.extend(state_paths)
    if clear_snapshots:
        selected_before_paths.extend(snapshot_paths)
    before_usage = _paths_usage(list(selected_before_paths))
    removed_state: list[str] = []
    removed_snapshots: list[str] = []

    if clear_state:
        for p in state_paths:
            if p.is_file() and _safe_remove_path(p):
                removed_state.append(p.name)

    if clear_snapshots:
        for p in snapshot_paths:
            if p.is_file() and _safe_remove_path(p):
                removed_snapshots.append(p.name)

    after = _scan_cw_tracker(root)
    selected_after_paths = []
    if clear_state:
        selected_after_paths.extend(_json_files_in_dir(root))
    if clear_snapshots:
        selected_after_paths.extend(_json_files_in_dir(_cw_tracker_snapshot_dir(root)))
    after_usage = _paths_usage(list(selected_after_paths))

    return {
        "ok": True,
        "provider_instance": inst,
        "root": str(root),
        "removed": {
            "state_files": removed_state,
            "snapshots": removed_snapshots,
        },
        "before": before,
        "after": after,
        "summary": _cleanup_summary(before_usage, after_usage),
    }


@router.post("/clear-cache")
def clear_cache() -> dict[str, Any]:
    _, _, CW_STATE_DIR, *_ = _cw()

    before = _scan_provider_cache()
    before_usage = {
        "files": len(before.get("files") or []),
        "bytes": sum(int(item.get("size") or 0) for item in before.get("files") or []),
        "modified": None,
    }
    removed = _clear_cw_state_files()
    after = _scan_provider_cache()
    after_usage = {
        "files": len(after.get("files") or []),
        "bytes": sum(int(item.get("size") or 0) for item in after.get("files") or []),
        "modified": None,
    }

    return {
        "ok": True,
        "root": str(CW_STATE_DIR),
        "removed": removed,
        "before": before,
        "after": after,
        "summary": _cleanup_summary(before_usage, after_usage),
    }


@router.post("/clear-provider-sync-cache")
def clear_provider_sync_cache() -> dict[str, Any]:
    """Reset pair baselines and provider runtime cache"""
    state = clear_state_minimal()
    cache = clear_cache()
    summaries = [state.get("summary") or {}, cache.get("summary") or {}]
    return {
        "ok": bool(state.get("ok")) and bool(cache.get("ok")),
        "state": state,
        "cache": cache,
        "summary": {
            key: sum(int(item.get(key) or 0) for item in summaries)
            for key in ("removed_files", "removed_items", "freed_bytes")
        },
    }

@router.get("/provider-cache")
def provider_cache_status() -> dict[str, Any]:
    info = _scan_provider_cache()
    return {"ok": True, **info}


@router.get("/action-status/{action}")
def action_status(action: str) -> dict[str, Any]:
    return maintenance_action_status(action)


def _split_csv(values: list[str] | None) -> list[str]:
    out: list[str] = []
    for raw in values or []:
        out.extend(part.strip() for part in str(raw or "").split(",") if part.strip())
    return list(dict.fromkeys(out))


@router.get("/support/scopes")
def support_scopes() -> dict[str, Any]:
    try:
        from services.support import list_scopes

        return list_scopes()
    except Exception:
        _LOG.exception("support scopes failed")
        return {"ok": False, "error": "internal_error"}


@router.get("/support/state")
def support_state(pairs: list[str] | None = Query(None)) -> StreamingResponse:
    from services.support import build_state, state_filename

    result = build_state(_split_csv(pairs))
    _LOG.info("support state export scope=%s pairs=%s", result["meta"].get("scope"), result["meta"].get("pair_ids"))
    meta = result["meta"]
    body = json.dumps(result["payload"], ensure_ascii=False, indent=2, default=str).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(body),
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{state_filename(meta)}"',
            "X-CrossWatch-Scope": str(meta.get("scope") or "all"),
            "Cache-Control": "no-store",
        },
    )


@router.get("/support/bundle")
def support_bundle(
    pairs: list[str] | None = Query(None),
    include: list[str] | None = Query(None),
) -> StreamingResponse:
    from services.support import bundle_filename, build_bundle

    data = build_bundle(_split_csv(pairs), _split_csv(include) or None)
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={
            "Content-Disposition": f'attachment; filename="{bundle_filename()}"',
            "Cache-Control": "no-store",
        },
    )


@router.post("/database-health")
def maintenance_database_health() -> dict[str, Any]:
    _, CONFIG_DIR, *_ = _cw()
    try:
        return local_db_diagnostics(CONFIG_DIR)
    except Exception:
        _LOG.exception("database-health failed")
        return {"ok": False, "error": "internal_error"}


@router.post("/events-health")
def maintenance_events_health() -> dict[str, Any]:
    try:
        from cw_platform.event_archive.maintenance import health
        return health()
    except Exception:
        _LOG.exception("events-health failed")
        return {"ok": False, "error": "internal_error"}


@router.post("/events-optimize")
def maintenance_events_optimize() -> dict[str, Any]:
    try:
        from cw_platform.event_archive.maintenance import optimize
        return optimize()
    except Exception:
        _LOG.exception("events-optimize failed")
        return {"ok": False, "error": "internal_error"}


@router.post("/events-rebuild")
def maintenance_events_rebuild(payload: dict[str, Any] | None = Body(None)) -> dict[str, Any]:
    if not bool((payload or {}).get("confirm")):
        return {"ok": False, "error": "confirmation_required", "confirm": False}
    try:
        from cw_platform.event_archive.maintenance import rebuild
        # DB-driven archive: clear only, do not re-import from runtime JSON state.
        return rebuild(reimport=False)
    except Exception:
        _LOG.exception("events-rebuild failed")
        return {"ok": False, "error": "internal_error"}


@router.post("/reset-all-default")
def reset_all_to_default(payload: dict[str, Any] | None = Body(None)) -> dict[str, Any]:
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
        except Exception:
            _LOG.exception("reset-all-default config backup failed")
            report["ok"] = False
            report["errors"].append("config_backup_failed")
            return report

    files = [LAST_SYNC_JSON, STATE_JSON, STATE_MANUAL_JSON, STATISTICS_JSON]
    dirs = [".cw_state", ".cw_databases", "sync_reports", LEGACY_DIR, ".cw_provider", "cache", "tls"]

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

    if report["ok"] and bool((payload or {}).get("restart")):
        report["restart_scheduled"] = True

        def _kill() -> None:
            os._exit(0)

        threading.Timer(0.75, _kill).start()

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
    _, CONFIG_DIR, _, _, _, _append_log = _cw()
    db_path = crosswatch_db_path(CONFIG_DIR)
    before_count = currently_watching_stream_count(CONFIG_DIR)
    before_usage = _path_usage(db_path)
    try:
        removed = clear_currently_watching_streams(CONFIG_DIR)
        after_usage = _path_usage(db_path)
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Reset currently playing sessions.",
            )
        except Exception:
            pass
        return {
            "ok": True,
            "path": str(db_path),
            "existed": bool(before_count),
            "summary": _cleanup_summary(before_usage, after_usage, removed_items=removed),
        }
    except Exception as e:
        return {
            "ok": False,
            "error": "reset_currently_watching_failed",
            "path": str(db_path),
            "existed": bool(before_count),
        }

@router.post("/clear-activity-log")
def clear_activity_log() -> dict[str, Any]:
    try:
        from services.activity import clear_events

        res = clear_events()
        _, _, _, _, _, _append_log = _cw()
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Cleared local activity log.",
            )
        except Exception:
            pass
        return res
    except Exception:
        return {"ok": False, "error": "clear_activity_log_failed"}


@router.post("/clear-recent-scrobbles")
def clear_recent_scrobbles() -> dict[str, Any]:
    try:
        from services.activity import activity_path, clear_scrobble_events

        before_usage = _path_usage(activity_path())
        res = clear_scrobble_events()
        after_usage = _path_usage(activity_path())
        res["summary"] = _cleanup_summary(
            before_usage,
            after_usage,
            removed_items=int(res.get("removed") or 0),
        )
        _, _, _, _, _, _append_log = _cw()
        try:
            _append_log(
                "TRBL",
                "\x1b[91m[TROUBLESHOOT]\x1b[0m Cleared local recent scrobbles.",
            )
        except Exception:
            pass
        return res
    except Exception:
        return {"ok": False, "error": "clear_recent_scrobbles_failed"}

# --- statistics reset / recalculation ---
@router.post("/reset-stats")
def reset_stats(
    recalc: bool = Body(False),
    purge_file: bool = Body(False),
    purge_state: bool = Body(False),
    purge_reports: bool = Body(False),
    purge_insights: bool = Body(False),
) -> dict[str, Any]:
    CACHE_DIR, CONFIG_DIR, CW_STATE_DIR, STATS, _load_statistics_state, _append_log = _cw()

    if not any((recalc, purge_file, purge_state, purge_reports, purge_insights)):
        purge_file = purge_state = purge_reports = purge_insights = True
        recalc = False

    stats_path = Path(getattr(STATS, "path", legacy_path(CONFIG_DIR, STATISTICS_JSON)))
    before_usage = _paths_usage(
        _statistics_artifact_paths(
            CACHE_DIR,
            CONFIG_DIR,
            CW_STATE_DIR,
            stats_path,
            include_stats=purge_file,
            include_state=purge_state,
            include_reports=purge_reports,
            include_insights=purge_insights,
        )
    )

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

        reports_rows_dropped = 0

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

        # --- sync state ---
        if purge_state and _find_state_path:
            try:
                from cw_platform.orchestrator._state_store import StateStore

                StateStore(CONFIG_DIR).clear_state()
                legacy_state = legacy_path(CONFIG_DIR, STATE_JSON)
                legacy_state.unlink(missing_ok=True)
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

                reports_rows_dropped = clear_sync_reports(base_path_from_report_dir(REPORT_DIR))
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

        # --- recalc from sync state ---
        if recalc:
            try:
                state = _load_statistics_state()
                if state:
                    STATS.refresh_from_state(state)
            except Exception:
                pass

        after_usage = _paths_usage(
            _statistics_artifact_paths(
                CACHE_DIR,
                CONFIG_DIR,
                CW_STATE_DIR,
                stats_path,
                include_stats=purge_file,
                include_state=purge_state,
                include_reports=purge_reports,
                include_insights=purge_insights,
            )
        )

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
            "summary": _cleanup_summary(before_usage, after_usage, removed_items=reports_rows_dropped),
        }
    except Exception:
        return {"ok": False, "error": "reset_stats_failed"}

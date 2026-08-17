# services/support.py
# CrossWatch - Support export helpers (state.json rebuild and diagnostic bundle)
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import io
import json
import os
import platform
import re
import shutil
import sys
import zipfile
from collections.abc import Iterable, Mapping, Sequence
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from cw_platform.config_base import CONFIG as CONFIG_DIR
from cw_platform.config_base import _current_version_norm, load_config, redact_config
from cw_platform.local_db import crosswatch_db_path
from cw_platform.local_db.diagnostics import diagnostics as local_db_diagnostics
from cw_platform.local_db.sync_reports import list_reports
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.provider_instances import normalize_instance_id, sanitize_instance_label

FEATURE_NAMES = ("watchlist", "ratings", "history", "playlists", "progress")
BUNDLE_SECTIONS = ("config", "diagnostics", "reports", "logs")
DEFAULT_SECTIONS = frozenset(BUNDLE_SECTIONS)

_ANSI_RE = re.compile(r"\x1b\[[0-9;]*[A-Za-z]")
_ISO_RE = re.compile(r"^\d{4}-\d{2}-\d{2}[T ]")
_EPOCH_RE = re.compile(r"^\d{9,}(\.\d+)?$")
_SAMPLE_LIMIT = 5
_LOG_TAIL_LINES = 4000
_REPORT_LIMIT = 25


def _now() -> datetime:
    return datetime.now(tz=timezone.utc)


def _stamp(ts: datetime | None = None) -> str:
    return (ts or _now()).strftime("%Y%m%d-%H%M%S")


def _iso(ts: datetime | None = None) -> str:
    return (ts or _now()).strftime("%Y-%m-%dT%H:%M:%SZ")


def _store() -> StateStore:
    return StateStore(CONFIG_DIR)


def _config() -> dict[str, Any]:
    try:
        cfg = load_config() or {}
    except Exception:
        cfg = {}
    return cfg if isinstance(cfg, dict) else {}


def _pairs(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    raw = cfg.get("pairs")
    return [p for p in raw if isinstance(p, dict)] if isinstance(raw, list) else []


def _instance_label(cfg: Mapping[str, Any], provider: str, instance: Any) -> str:
    inst = normalize_instance_id(instance)
    if inst == "default":
        return "Default"
    block = cfg.get(str(provider or "").strip().lower())
    insts = block.get("instances") if isinstance(block, Mapping) else None
    node = insts.get(inst) if isinstance(insts, Mapping) else None
    label = sanitize_instance_label(node.get("label") if isinstance(node, Mapping) else "")
    return label or inst


def _endpoint_label(cfg: Mapping[str, Any], provider: Any, instance: Any) -> str:
    prov = str(provider or "?").strip().upper()
    inst = normalize_instance_id(instance)
    return prov if inst == "default" else f"{prov} ({_instance_label(cfg, prov, inst)})"


def _pair_features(pair: Mapping[str, Any]) -> set[str]:
    features = pair.get("features")
    if not isinstance(features, Mapping):
        return set()
    out: set[str] = set()
    for name, block in features.items():
        key = str(name or "").strip().lower()
        if key not in FEATURE_NAMES:
            continue
        if isinstance(block, Mapping):
            if block.get("enable") is not False:
                out.add(key)
        elif block:
            out.add(key)
    return out


def _pair_label(cfg: Mapping[str, Any], pair: Mapping[str, Any]) -> str:
    src = _endpoint_label(cfg, pair.get("source"), pair.get("source_instance"))
    dst = _endpoint_label(cfg, pair.get("target"), pair.get("target_instance"))
    arrow = "<->" if str(pair.get("mode") or "").strip().lower() in {"two-way", "mirror"} else "->"
    return f"{src} {arrow} {dst}"


def _pair_endpoints(pair: Mapping[str, Any]) -> list[tuple[str, str]]:
    return [
        (str(pair.get("source") or "").strip().upper(), normalize_instance_id(pair.get("source_instance"))),
        (str(pair.get("target") or "").strip().upper(), normalize_instance_id(pair.get("target_instance"))),
    ]


def _allow_map(pairs: Iterable[Mapping[str, Any]]) -> dict[tuple[str, str], set[str]]:
    allow: dict[tuple[str, str], set[str]] = {}
    for pair in pairs:
        features = _pair_features(pair)
        if not features:
            continue
        for provider, instance in _pair_endpoints(pair):
            if provider:
                allow.setdefault((provider, instance), set()).update(features)
    return allow


def _selected_pairs(cfg: Mapping[str, Any], pair_ids: Sequence[str] | None) -> tuple[list[dict[str, Any]], list[str]]:
    wanted = [str(pid or "").strip() for pid in pair_ids or [] if str(pid or "").strip()]
    if not wanted or "all" in {w.lower() for w in wanted}:
        return _pairs(cfg), []
    by_id = {str(p.get("id") or ""): p for p in _pairs(cfg)}
    selected = [by_id[pid] for pid in wanted if pid in by_id]
    unknown = [pid for pid in wanted if pid not in by_id]
    return selected, unknown


def _canonical_key(item: Mapping[str, Any]) -> str:
    try:
        from cw_platform.id_map import canonical_key

        return str(canonical_key(item) or "")
    except Exception:
        return ""


def _wall_from_providers(providers: Mapping[str, Any]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    out: list[dict[str, Any]] = []

    def collect(node: Any) -> None:
        if not isinstance(node, Mapping):
            return
        block = node.get("watchlist")
        items = _block_items(block) if isinstance(block, Mapping) else {}
        for item in items.values():
            if not isinstance(item, Mapping):
                continue
            key = _canonical_key(item) or str(item.get("title") or id(item))
            if key in seen:
                continue
            seen.add(key)
            out.append(dict(item))

    for node in providers.values():
        collect(node)
        insts = node.get("instances") if isinstance(node, Mapping) else None
        if isinstance(insts, Mapping):
            for inst_node in insts.values():
                collect(inst_node)
    return out


def _filter_state(state: Mapping[str, Any], allow: Mapping[tuple[str, str], set[str]]) -> dict[str, Any]:
    providers = state.get("providers")
    providers = providers if isinstance(providers, Mapping) else {}
    kept: dict[str, Any] = {}

    for provider_raw, node in providers.items():
        provider = str(provider_raw or "").strip().upper()
        if not provider or not isinstance(node, Mapping):
            continue
        provider_features = set().union(*[v for (p, _), v in allow.items() if p == provider])
        if not provider_features:
            continue

        default_features = allow.get((provider, "default")) or set()
        new_node: dict[str, Any] = {
            str(name).lower(): block
            for name, block in node.items()
            if str(name).lower() in default_features
        }

        manual = node.get("manual")
        if isinstance(manual, Mapping):
            manual_kept = {k: v for k, v in manual.items() if str(k).lower() in provider_features}
            if manual_kept:
                new_node["manual"] = manual_kept

        insts = node.get("instances")
        if isinstance(insts, Mapping):
            kept_insts: dict[str, Any] = {}
            for inst_raw, inst_node in insts.items():
                inst = normalize_instance_id(inst_raw)
                inst_features = allow.get((provider, inst)) or set()
                if not inst_features or not isinstance(inst_node, Mapping):
                    continue
                sub = {
                    str(name).lower(): block
                    for name, block in inst_node.items()
                    if str(name).lower() in inst_features
                }
                if sub:
                    kept_insts[str(inst_raw)] = sub
            if kept_insts:
                new_node["instances"] = kept_insts

        if new_node:
            kept[provider] = new_node

    return {
        "providers": kept,
        "wall": _wall_from_providers(kept),
        "last_sync_epoch": state.get("last_sync_epoch"),
    }


def _iter_feature_blocks(state: Mapping[str, Any]) -> Iterable[tuple[str, str, str, Mapping[str, Any]]]:
    providers = state.get("providers")
    if not isinstance(providers, Mapping):
        return
    for provider_raw, node in providers.items():
        provider = str(provider_raw or "").strip().upper()
        if not isinstance(node, Mapping):
            continue
        for name, block in node.items():
            if str(name).lower() in FEATURE_NAMES and isinstance(block, Mapping):
                yield provider, "default", str(name).lower(), block
        insts = node.get("instances")
        if isinstance(insts, Mapping):
            for inst_raw, inst_node in insts.items():
                if not isinstance(inst_node, Mapping):
                    continue
                for name, block in inst_node.items():
                    if str(name).lower() in FEATURE_NAMES and isinstance(block, Mapping):
                        yield provider, normalize_instance_id(inst_raw), str(name).lower(), block


def _block_items(block: Mapping[str, Any]) -> dict[str, Any]:
    baseline = block.get("baseline")
    items = baseline.get("items") if isinstance(baseline, Mapping) else None
    return items if isinstance(items, dict) else {}


def _state_totals(state: Mapping[str, Any]) -> dict[str, int]:
    baselines = 0
    items = 0
    for _, _, _, block in _iter_feature_blocks(state):
        baselines += 1
        items += len(_block_items(block))
    providers = state.get("providers")
    return {
        "providers": len(providers) if isinstance(providers, Mapping) else 0,
        "baselines": baselines,
        "items": items,
    }


def _timestamp_flag(value: Any) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, bool):
        return "bool"
    if isinstance(value, (int, float)):
        return "epoch_ms" if float(value) > 1e11 else "epoch_seconds"
    text = str(value).strip()
    if _ISO_RE.match(text):
        return None
    if _EPOCH_RE.match(text):
        return "epoch_ms_string" if float(text) > 1e11 else "epoch_seconds_string"
    return "unparsed"


def _state_summary(state: Mapping[str, Any]) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for provider, instance, feature, block in _iter_feature_blocks(state):
        items = _block_items(block)
        types: dict[str, int] = {}
        id_coverage: dict[str, int] = {}
        timestamp_issues: dict[str, int] = {}
        samples: list[dict[str, Any]] = []
        without_ids = 0

        for key, item in items.items():
            if not isinstance(item, Mapping):
                continue
            media_type = str(item.get("type") or "unknown").lower()
            types[media_type] = types.get(media_type, 0) + 1
            ids = item.get("ids")
            ids = ids if isinstance(ids, Mapping) else {}
            present = [str(k) for k, v in ids.items() if v not in (None, "")]
            if not present:
                without_ids += 1
            for id_key in present:
                id_coverage[id_key] = id_coverage.get(id_key, 0) + 1
            for field in ("watched_at", "rated_at", "progress_at"):
                flag = _timestamp_flag(item.get(field))
                if flag:
                    label = f"{field}:{flag}"
                    timestamp_issues[label] = timestamp_issues.get(label, 0) + 1
                    if len(samples) < _SAMPLE_LIMIT:
                        samples.append({"key": str(key), "field": field, "value": item.get(field), "flag": flag})

        rows.append({
            "provider": provider,
            "instance": instance,
            "feature": feature,
            "items": len(items),
            "checkpoint": block.get("checkpoint"),
            "types": dict(sorted(types.items())),
            "id_coverage": dict(sorted(id_coverage.items(), key=lambda kv: (-kv[1], kv[0]))),
            "items_without_ids": without_ids,
            "event_keys": sum(1 for key in items if "@" in str(key)),
            "sample_keys": [str(key) for key in list(items)[:_SAMPLE_LIMIT]],
            "timestamp_issues": dict(sorted(timestamp_issues.items())),
            "timestamp_samples": samples,
        })

    return {
        "generated_at": _iso(),
        "totals": _state_totals(state),
        "last_sync_epoch": state.get("last_sync_epoch"),
        "baselines": rows,
    }


_REDACTED = "<redacted>"

_IDENTITY_KEYS = frozenset({
    "access_token", "account_id", "api_key", "api_token", "apikey", "auth_key", "authkey",
    "avatar", "base_url", "cert_file", "client_id", "client_secret", "default_url",
    "device_id", "display_name", "email", "failure_url", "file", "hash", "home_pin", "host", "hostname",
    "ip", "iss", "issuer", "key_file", "label", "linked_email", "linked_plex_account_id",
    "linked_thumb", "linked_username", "machine_id", "name", "passwd", "password", "path",
    "pending_secret", "picture", "pin", "playlist_id", "playlist_name", "pms_token", "pms_token_server",
    "profile_id", "profile_name", "refresh_token", "root", "root_dir", "salt", "secret",
    "server", "server_url", "server_uuid", "session_id", "start_url", "state_dir", "sub",
    "success_url", "thumb", "token",
    "token_hash", "ua", "uri", "url", "user", "user_id", "username", "uuid",
    "verification_url", "watchlist_list_id", "watchlist_name", "webhook_id", "webhook_token",
})

_COUNT_LIST_KEYS = frozenset({
    "devices", "libraries", "pairings", "recovery_codes", "server_uuid_blacklist",
    "server_uuid_whitelist", "sessions", "username_whitelist", "webhook_ids",
})

_URLISH_RE = re.compile(r"(?i)^(?:[a-z][a-z0-9+.-]*://|/[^\s]*/|[a-z]:[\\/])")
_IP_RE = re.compile(r"\b\d{1,3}(?:\.\d{1,3}){3}\b")
_EMAIL_RE = re.compile(r"[^@\s]+@[^@\s]+\.[A-Za-z]{2,}")
_OPAQUE_RE = re.compile(r"^[A-Za-z0-9_\-]{20,}$")
_MASKED_RE = re.compile(r"^[•*]+$")


def _looks_sensitive(value: str) -> bool:
    text = value.strip()
    if not text or _MASKED_RE.match(text):
        return False
    return bool(
        _URLISH_RE.match(text)
        or _IP_RE.search(text)
        or _EMAIL_RE.search(text)
        or _OPAQUE_RE.match(text)
    )


def _scrub(value: Any, key: str = "") -> Any:
    name = str(key or "").strip().lower()
    if isinstance(value, Mapping):
        return {str(k): _scrub(v, str(k)) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        if name in _COUNT_LIST_KEYS:
            return f"<{len(value)} item{'' if len(value) == 1 else 's'}>" if value else []
        return [_scrub(v, name) for v in value]
    if isinstance(value, bool) or value is None:
        return value
    if name in _IDENTITY_KEYS:
        if isinstance(value, str):
            return _REDACTED if value.strip() else ""
        return _REDACTED
    if isinstance(value, str) and _looks_sensitive(value):
        return _REDACTED
    return value


def scrub_config(cfg: Mapping[str, Any]) -> dict[str, Any]:
    try:
        base = redact_config(dict(cfg or {}))
    except Exception:
        base = dict(cfg or {})
    scrubbed = _scrub(base)
    return scrubbed if isinstance(scrubbed, dict) else {}


def _events_status() -> dict[str, Any]:
    try:
        from cw_platform import event_archive

        return {"status": event_archive.status(), "health": event_archive.health()}
    except Exception as exc:
        return {"error": f"{type(exc).__name__}: {exc}"}


def _environment() -> dict[str, Any]:
    try:
        usage = shutil.disk_usage(CONFIG_DIR)
        disk = {"total_bytes": usage.total, "used_bytes": usage.used, "free_bytes": usage.free}
    except Exception:
        disk = {}
    try:
        db_path = str(crosswatch_db_path(CONFIG_DIR))
    except Exception:
        db_path = ""
    return {
        "generated_at": _iso(),
        "app_version": _current_version_norm(),
        "python": sys.version.split()[0],
        "platform": platform.platform(),
        "machine": platform.machine(),
        "in_docker": Path("/.dockerenv").exists() or bool(os.getenv("CW_DOCKER")),
        "timezone": os.getenv("TZ") or datetime.now().astimezone().tzname() or "",
        "config_dir": str(CONFIG_DIR),
        "database_path": db_path,
        "disk": disk,
    }


def _sync_reports() -> list[dict[str, Any]]:
    try:
        return list_reports(CONFIG_DIR, limit=_REPORT_LIMIT)
    except Exception:
        return []


def _clean_log_line(line: str) -> str:
    try:
        from crosswatch import _redact_secrets_in_text

        line = _redact_secrets_in_text(line)
    except Exception:
        pass
    return _ANSI_RE.sub("", str(line))


def _log_tails() -> dict[str, str]:
    try:
        from crosswatch import LOG_BUFFERS
    except Exception:
        return {}
    out: dict[str, str] = {}
    for tag, lines in (LOG_BUFFERS or {}).items():
        if not isinstance(lines, list) or not lines:
            continue
        tail = lines[-_LOG_TAIL_LINES:]
        out[str(tag).lower()] = "\n".join(_clean_log_line(line) for line in tail)
    return out


def _scrobble_routes(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    scrobble = cfg.get("scrobble")
    watch = scrobble.get("watch") if isinstance(scrobble, Mapping) else None
    routes = watch.get("routes") if isinstance(watch, Mapping) else None
    out: list[dict[str, Any]] = []
    for route in routes if isinstance(routes, list) else []:
        if not isinstance(route, Mapping):
            continue
        out.append({
            "id": route.get("id"),
            "enabled": route.get("enabled", True),
            "provider": route.get("provider"),
            "provider_instance": normalize_instance_id(route.get("provider_instance") or route.get("providerInstance")),
            "sink": route.get("sink"),
            "sink_instance": normalize_instance_id(route.get("sink_instance") or route.get("sinkInstance")),
        })
    return out


def list_scopes() -> dict[str, Any]:
    cfg = _config()
    store = _store()
    inventory = store.feature_inventory()
    covered = _allow_map(_pairs(cfg))

    by_endpoint: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in inventory:
        by_endpoint.setdefault((row["provider"], row["instance"]), []).append(row)

    pairs: list[dict[str, Any]] = []
    for pair in _pairs(cfg):
        features = _pair_features(pair)
        baselines = 0
        items = 0
        for endpoint in _pair_endpoints(pair):
            for row in by_endpoint.get(endpoint, []):
                if row["feature"] in features:
                    baselines += 1
                    items += row["items"]
        pairs.append({
            "id": str(pair.get("id") or ""),
            "label": _pair_label(cfg, pair),
            "source": str(pair.get("source") or "").upper(),
            "source_instance": normalize_instance_id(pair.get("source_instance")),
            "target": str(pair.get("target") or "").upper(),
            "target_instance": normalize_instance_id(pair.get("target_instance")),
            "mode": str(pair.get("mode") or "one-way"),
            "enabled": pair.get("enabled", True) is not False,
            "features": sorted(features),
            "baselines": baselines,
            "items": items,
        })

    orphans = [
        row for row in inventory
        if row["feature"] not in (covered.get((row["provider"], row["instance"])) or set())
    ]
    return {
        "ok": True,
        "generated_at": _iso(),
        "pairs": pairs,
        "inventory": inventory,
        "orphans": orphans,
        "totals": {
            "pairs": len(pairs),
            "baselines": len(inventory),
            "items": sum(row["items"] for row in inventory),
            "orphan_baselines": len(orphans),
            "orphan_items": sum(row["items"] for row in orphans),
        },
    }


def build_state(pair_ids: Sequence[str] | None = None) -> dict[str, Any]:
    cfg = _config()
    state = _store().load_state()
    state = state if isinstance(state, dict) else {"providers": {}, "wall": [], "last_sync_epoch": None}

    selected, unknown = _selected_pairs(cfg, pair_ids)
    scoped = bool(pair_ids) and "all" not in {str(p).strip().lower() for p in pair_ids or []}

    if scoped:
        payload = _filter_state(state, _allow_map(selected))
    else:
        payload = {
            "providers": state.get("providers") or {},
            "wall": state.get("wall") or [],
            "last_sync_epoch": state.get("last_sync_epoch"),
        }

    meta = {
        "generated_at": _iso(),
        "app_version": _current_version_norm(),
        "scope": "pairs" if scoped else "all",
        "pair_ids": [str(p.get("id") or "") for p in selected] if scoped else [],
        "pair_labels": [_pair_label(cfg, p) for p in selected] if scoped else [],
        "unknown_pair_ids": unknown,
        "totals": _state_totals(payload),
    }
    return {"payload": payload, "meta": meta}


def state_filename(meta: Mapping[str, Any]) -> str:
    return f"crosswatch-state-{meta.get('scope') or 'all'}-{_stamp()}.json"


def bundle_filename() -> str:
    return f"crosswatch-support-{_stamp()}.zip"


def _normalize_sections(sections: Iterable[str] | None) -> set[str]:
    if sections is None:
        return set(DEFAULT_SECTIONS)
    wanted = {str(s or "").strip().lower() for s in sections if str(s or "").strip()}
    if "all" in wanted:
        return set(DEFAULT_SECTIONS)
    return {s for s in wanted if s in BUNDLE_SECTIONS}


def build_bundle(pair_ids: Sequence[str] | None = None, sections: Iterable[str] | None = None) -> bytes:
    wanted = _normalize_sections(sections)
    cfg = _config()
    state = build_state(pair_ids)
    payload = state["payload"]
    meta = dict(state["meta"])
    files: list[str] = []

    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w", zipfile.ZIP_DEFLATED) as zf:

        def write_json(name: str, data: Any) -> None:
            zf.writestr(name, json.dumps(data, ensure_ascii=False, indent=2, default=str))
            files.append(name)

        def write_text(name: str, text: str) -> None:
            zf.writestr(name, text)
            files.append(name)

        write_json("state.json", payload)
        pairs_snapshot = _scrub({
            "generated_at": _iso(),
            "pairs": _pairs(cfg),
            "scrobble_routes": _scrobble_routes(cfg),
        })
        write_json("pairs.json", pairs_snapshot if isinstance(pairs_snapshot, dict) else {})

        if "config" in wanted:
            try:
                write_json("config.redacted.json", scrub_config(cfg))
            except Exception as exc:
                write_json("config.redacted.json", {"error": f"{type(exc).__name__}: {exc}"})

        if "diagnostics" in wanted:
            try:
                write_json("diagnostics/database.json", local_db_diagnostics(CONFIG_DIR))
            except Exception as exc:
                write_json("diagnostics/database.json", {"error": f"{type(exc).__name__}: {exc}"})
            write_json("diagnostics/events.json", _events_status())
            write_json("diagnostics/environment.json", _environment())
            write_json("diagnostics/state_summary.json", _state_summary(payload))
            write_json("diagnostics/inventory.json", list_scopes())

        if "reports" in wanted:
            write_json("reports/sync_reports.json", _sync_reports())

        if "logs" in wanted:
            for tag, text in _log_tails().items():
                write_text(f"logs/{tag}.log", text)

        meta.update({
            "kind": "crosswatch_support_bundle",
            "schema_version": 1,
            "sections": sorted(wanted),
            "files": files,
        })
        zf.writestr("manifest.json", json.dumps(meta, ensure_ascii=False, indent=2, default=str))

    return buffer.getvalue()

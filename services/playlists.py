# services/playlists.py
# CrossWatch - Playlist endpoints, mapping profiles and run service
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
import time
import uuid
from contextlib import contextmanager
from typing import Any, Iterator, Mapping

from _logging import log as _cw_log
from cw_platform.playlists import BUILTIN_RULESETS, playlist_capabilities, supports_playlists, validate_ruleset
from cw_platform.provider_instances import (
    build_provider_config_view,
    list_instance_ids,
    normalize_instance_id,
)
from cw_platform import playlists_runner as runner

_MEMBERSHIP = {"add_only", "managed_only", "mirror"}
_ORDER = {"ignore", "preserve"}
_NAME_MAX = 10
_PLAYLIST_NAME_MAX = 20
_SAFE_NAME_CHARS = " _.'-&()"


def _internal_playlist_error(action: str, exc: Exception, **extra: Any) -> dict[str, Any]:
    try:
        _cw_log(
            f"playlist {action} failed",
            level="ERROR",
            module="PLAYLISTS",
            extra={
                "action": action,
                "error_type": exc.__class__.__name__,
                **{k: v for k, v in extra.items() if v not in (None, "", [], {})},
            },
        )
    except Exception:
        pass
    return {"ok": False, "error": f"{action} failed"}


def _safe_name_error(name: Any, label: str, max_len: int) -> str:
    s = str(name or "").strip()
    if not s:
        return f"{label} required"
    if len(s) > max_len:
        return f"{label} must be {max_len} characters or fewer"
    if not s[0].isalnum():
        return f"{label} must start with a letter or number"
    if any(not (ch.isalnum() or ch in _SAFE_NAME_CHARS) for ch in s):
        return f"{label} contains unsupported characters"
    return ""


def _name_error(name: Any, label: str) -> str:
    return _safe_name_error(name, label, _NAME_MAX)


def _playlist_name_error(name: Any) -> str:
    return _safe_name_error(name, "new playlist name", _PLAYLIST_NAME_MAX)


def _clean_target_ids(payload: Mapping[str, Any]) -> list[str]:
    raw = payload.get("target_endpoints")
    out: list[str] = []
    if isinstance(raw, list):
        for x in raw:
            s = str((x or {}).get("id") if isinstance(x, Mapping) else x).strip()
            if s and s not in out:
                out.append(s)
    return out



def _providers() -> dict[str, Any]:
    from cw_platform.orchestrator._providers import load_sync_providers

    return load_sync_providers()


def _label_for(ops: Any, provider: str) -> str:
    getter = getattr(ops, "label", None)
    if callable(getter):
        try:
            return str(getter())
        except Exception:
            pass
    return provider.title()


def _cfg_pairs(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    pairs = cfg.get("pairs")
    return [p for p in pairs if isinstance(p, dict)] if isinstance(pairs, list) else []


def _find_pair(cfg: Mapping[str, Any], pair_id: str) -> dict[str, Any] | None:
    pid = str(pair_id or "").strip()
    for p in _cfg_pairs(cfg):
        if str(p.get("id") or "") == pid:
            return p
    return None


# Providers / resources

def list_playlist_providers(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    provs = _providers()
    for name, ops in sorted(provs.items()):
        if not supports_playlists(ops):
            continue
        caps = playlist_capabilities(ops)
        label = _label_for(ops, name)
        is_conf = getattr(ops, "is_configured", None)
        for inst in list_instance_ids(cfg, name):
            configured = True
            if callable(is_conf):
                try:
                    configured = bool(is_conf(build_provider_config_view(cfg, name, inst)))
                except Exception:
                    configured = False
            out.append(
                {
                    "provider": name,
                    "instance": inst,
                    "label": label,
                    "configured": configured,
                    "capabilities": caps,
                }
            )
    return out


def list_resources(cfg: Mapping[str, Any], provider: str, instance: str | None = None) -> dict[str, Any]:
    name = str(provider or "").strip().upper()
    inst = normalize_instance_id(instance)
    ops = _providers().get(name)
    if not ops or not supports_playlists(ops):
        return {"ok": False, "error": "provider not playlist-capable", "resources": []}
    try:
        view = build_provider_config_view(cfg, name, inst)
        resources = ops.list_playlist_resources(view, instance=inst) or []
    except Exception as e:
        res = _internal_playlist_error("resource listing", e, provider=name, instance=inst)
        res["resources"] = []
        return res
    return {"ok": True, "provider": name, "instance": inst, "resources": [r.to_dict() for r in resources]}


# Endpoints

def _clean_endpoint(payload: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {
        "id": str(payload.get("id") or "").strip(),
        "name": str(payload.get("name") or "").strip(),
        "provider": str(payload.get("provider") or "").strip().upper(),
        "instance": normalize_instance_id(payload.get("instance")),
        "playlist_id": str(payload.get("playlist_id") or "").strip(),
        "playlist_name": str(payload.get("playlist_name") or "").strip(),
    }
    playlist_type = str(payload.get("playlist_type") or payload.get("endpoint_type") or "").strip().lower()
    if playlist_type:
        out["playlist_type"] = playlist_type
    media_types = payload.get("media_types")
    if isinstance(media_types, list):
        out["media_types"] = [str(x).strip().lower() for x in media_types if str(x).strip()]
    return out


def list_endpoints(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    provs = _providers()
    out: list[dict[str, Any]] = []
    for ep in runner.list_endpoints(cfg):
        ep = dict(ep)
        ep["provider_label"] = _label_for(provs.get(str(ep.get("provider") or "").upper()), str(ep.get("provider") or ""))
        out.append(ep)
    return out


def provider_count_summary(cfg: Mapping[str, Any]) -> dict[str, Any]:
    counts: dict[str, int] = {}
    inst_counts: dict[str, dict[str, int]] = {}

    def add(provider: Any, instance: Any, count: Any) -> None:
        key = str(provider or "").strip().lower()
        if not key:
            return
        try:
            n = max(0, int(count or 0))
        except Exception:
            return
        inst = str(instance or "default").strip() or "default"
        counts[key] = max(int(counts.get(key) or 0), n)
        by_inst = inst_counts.setdefault(key, {})
        by_inst[inst] = max(int(by_inst.get(inst) or 0), n)

    for ep in list_endpoints(cfg):
        if not isinstance(ep, Mapping):
            continue
        raw_count = ep.get("item_count")
        if raw_count is None:
            continue
        add(ep.get("provider"), ep.get("instance"), raw_count)

    try:
        profiles = runner.list_mapping_profiles(cfg) or []
    except Exception:
        profiles = []

    for profile in profiles:
        if not isinstance(profile, Mapping):
            continue
        resolved = runner.resolve_mapping(cfg, profile)
        if not resolved:
            continue
        try:
            result = runner.load_result(resolved) or {}
        except Exception:
            result = {}
        if not isinstance(result, Mapping) or not result.get("finished_at"):
            continue
        targets = resolved.get("targets") if isinstance(resolved, Mapping) else None
        if not isinstance(targets, list) or not targets:
            continue
        per_target = result.get("per_target")
        if isinstance(per_target, list) and per_target:
            by_eid: dict[str, int] = {}
            for row in per_target:
                if not isinstance(row, Mapping):
                    continue
                eid = str(row.get("endpoint_id") or "").strip()
                if not eid:
                    continue
                count = int(row.get("current_count") or 0)
                count += int(row.get("planned_additions") or 0)
                count -= int(row.get("planned_removals") or 0)
                by_eid[eid] = max(0, count)
            for target in targets:
                if not isinstance(target, Mapping):
                    continue
                count = by_eid.get(str(target.get("endpoint_id") or "").strip())
                if count is not None:
                    add(target.get("provider"), target.get("instance"), count)
            continue
        count = int(result.get("managed_count") or result.get("target_count") or 0)
        if count <= 0:
            continue
        target = targets[0]
        if isinstance(target, Mapping):
            add(target.get("provider"), target.get("instance"), count)

    return {"providers": counts, "providers_instances": inst_counts}


def _snapshot_count(cfg: Mapping[str, Any], provider: str, instance: str, playlist_id: str) -> int | None:
    ops = _providers().get(str(provider or "").upper())
    if not ops or not supports_playlists(ops) or not playlist_id:
        return None
    try:
        view = build_provider_config_view(cfg, provider, instance)
        snap = ops.get_playlist_snapshot(view, playlist_id, instance=instance)
        return len(snap.items)
    except Exception:
        return None


def _resource_meta(cfg: Mapping[str, Any], provider: str, instance: str, playlist_id: str) -> dict[str, Any]:
    ops = _providers().get(str(provider or "").upper())
    if not ops or not supports_playlists(ops) or not playlist_id:
        return {}
    try:
        view = build_provider_config_view(cfg, provider, instance)
        for res in ops.list_playlist_resources(view, instance=instance) or []:
            raw_id = str((res.extra or {}).get("raw_id") or "").strip()
            if res.id == playlist_id or raw_id == playlist_id:
                extra = res.extra or {}
                out: dict[str, Any] = {
                    "playlist_type": str(extra.get("endpoint_type") or res.kind or "").strip().lower(),
                    "media_types": list(res.media_types or []),
                    "can_reorder": bool(res.can_reorder),
                    "destructive_remove": bool(extra.get("destructive_remove")),
                    "remove_warning": str(extra.get("remove_warning") or "").strip(),
                    "warnings": [str(w) for w in (extra.get("warnings") or []) if str(w).strip()],
                }
                return {k: v for k, v in out.items() if v not in ("", [], None)}
    except Exception:
        return {}
    return {}


def _refresh_endpoint_meta(cfg: dict[str, Any], endpoint_id: str) -> dict[str, Any] | None:
    root = runner.pl_root(cfg, create=True)
    for ep in root["endpoints"]:
        if isinstance(ep, dict) and str(ep.get("id") or "") == str(endpoint_id):
            provider = str(ep.get("provider") or "").strip().upper()
            instance = normalize_instance_id(ep.get("instance"))
            playlist_id = str(ep.get("playlist_id") or "")
            ep.update(_resource_meta(cfg, provider, instance, playlist_id))
            cnt = _snapshot_count(cfg, provider, instance, playlist_id)
            if cnt is not None:
                ep["item_count"] = cnt
            ep["last_synced"] = int(time.time())
            return dict(ep)
    return None


def sync_endpoint(cfg: dict[str, Any], endpoint_id: str) -> dict[str, Any]:
    ep = _refresh_endpoint_meta(cfg, endpoint_id)
    if ep is None:
        return {"ok": False, "error": "endpoint not found"}
    _save(cfg)
    return {"ok": True, "endpoint": ep}


def activity(cfg: Mapping[str, Any], *, limit: int = 25) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for ep in list_endpoints(cfg):
        ts = ep.get("last_synced")
        if ts:
            rows.append({
                "ts": int(ts), "type": "Sync", "status": "completed",
                "label": f"{ep.get('provider_label') or ep.get('provider')} · {ep.get('name')}",
                "details": (f"{ep.get('item_count')} items" if ep.get("item_count") is not None else ""),
            })
    for m in list_mappings(cfg):
        r = m.get("last_result") or {}
        if isinstance(r, Mapping) and r.get("finished_at"):
            finished_at = int(r.get("finished_at") or 0)
            src = (m.get("source") or {}).get("label"); dst = (m.get("target") or {}).get("label")
            rows.append({
                "ts": finished_at, "type": "Run", "status": "completed" if r.get("ok", True) else "error",
                "label": f"{m.get('id')} · {src} → {dst}",
                "details": f"{(m.get('ruleset') or {}).get('name') or r.get('ruleset_id') or 'direct'}, {len(m.get('targets') or [])} target(s), +{int(r.get('added', 0))}/-{int(r.get('removed', 0))}" + (", capacity error" if r.get("capacity_error") else ""),
            })
    rows.sort(key=lambda x: x["ts"], reverse=True)
    return rows[: max(1, int(limit))]


def upsert_endpoint(cfg: dict[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    clean = _clean_endpoint(payload)
    name_err = _name_error(clean["name"], "endpoint name")
    if name_err:
        return {"ok": False, "error": name_err}
    if not clean["provider"]:
        return {"ok": False, "error": "provider required"}
    ops = _providers().get(clean["provider"])
    if not ops or not supports_playlists(ops):
        return {"ok": False, "error": "provider not playlist-capable"}

    create = bool(payload.get("create"))
    create_name = str(payload.get("create_name") or clean["name"] or "").strip()
    if create and not clean["playlist_id"]:
        create_err = _playlist_name_error(create_name)
        if create_err:
            return {"ok": False, "error": create_err}
        media_type = str(payload.get("media_type") or "movie").strip().lower()
        try:
            view = build_provider_config_view(cfg, clean["provider"], clean["instance"])
            res = ops.create_playlist(view, create_name, media_type=media_type, instance=clean["instance"])
            clean["playlist_id"] = res.id
            clean["playlist_name"] = clean["playlist_name"] or res.name
        except Exception as e:
            return _internal_playlist_error(
                "create",
                e,
                provider=clean["provider"],
                instance=clean["instance"],
                media_type=media_type,
            )

    if not clean["playlist_id"]:
        return {"ok": False, "error": "playlist required"}
    root = runner.pl_root(cfg, create=True)
    endpoints = root["endpoints"]
    if clean["id"]:
        for i, ep in enumerate(endpoints):
            if isinstance(ep, Mapping) and str(ep.get("id") or "") == clean["id"]:
                endpoints[i] = clean
                out = _refresh_endpoint_meta(cfg, clean["id"]) or clean
                _refresh_mapping_pairs_for_endpoint(cfg, clean["id"])
                _save(cfg)
                return {"ok": True, "endpoint": out, "created": False}
    clean["id"] = runner.next_id(endpoints, runner.ENDPOINT_PREFIX)
    endpoints.append(clean)
    out = _refresh_endpoint_meta(cfg, clean["id"]) or clean
    _save(cfg)
    return {"ok": True, "endpoint": out, "created": True}


def delete_endpoint(cfg: dict[str, Any], endpoint_id: str) -> dict[str, Any]:
    eid = str(endpoint_id or "").strip()
    used_by = [
        str(m.get("id"))
        for m in runner.list_mapping_profiles(cfg)
        if str(m.get("source_endpoint") or "") == eid or eid in _clean_target_ids(m)
    ]
    if used_by:
        return {"ok": False, "error": f"endpoint in use by {', '.join(used_by)}"}
    root = runner.pl_root(cfg, create=True)
    before = len(root["endpoints"])
    root["endpoints"] = [e for e in root["endpoints"] if str((e or {}).get("id") or "") != eid]
    if len(root["endpoints"]) == before:
        return {"ok": False, "error": "endpoint not found"}
    _save(cfg)
    return {"ok": True}


# Mapping profiles

def _clean_mapping(payload: Mapping[str, Any]) -> dict[str, Any]:
    membership = str(payload.get("membership") or "").strip().lower()
    if membership not in _MEMBERSHIP:
        membership = "managed_only"
    order = str(payload.get("order") or "").strip().lower()
    if order not in _ORDER:
        order = "ignore"
    target_ids = _clean_target_ids(payload)
    out: dict[str, Any] = {
        "id": str(payload.get("id") or "").strip(),
        "name": str(payload.get("name") or "").strip(),
        "source_endpoint": str(payload.get("source_endpoint") or "").strip(),
        "target_endpoints": target_ids,
        "ruleset_id": str(payload.get("ruleset_id") or "").strip(),
        "membership": membership,
        "order": order,
        "enabled": bool(payload.get("enabled", True)),
    }
    if payload.get("allow_mass_delete"):
        out["allow_mass_delete"] = True
    return out


def _endpoint_reorderable(cfg: Mapping[str, Any], endpoint_id: str) -> bool:
    ep = runner.get_endpoint(cfg, endpoint_id)
    if not ep:
        return True
    if "can_reorder" in ep:
        return bool(ep.get("can_reorder"))
    ops = _providers().get(str(ep.get("provider") or "").upper())
    if not ops:
        return True
    return bool(playlist_capabilities(ops).get("reorder", True))


def validate_mapping(cfg: Mapping[str, Any], payload: Mapping[str, Any]) -> tuple[bool, str]:
    clean = _clean_mapping(payload)
    name_err = _name_error(clean["name"], "mapping name")
    if name_err:
        return False, name_err
    if not clean["source_endpoint"] or not clean["target_endpoints"]:
        return False, "source and target endpoints required"
    if clean["source_endpoint"] in clean["target_endpoints"]:
        return False, "source and target must differ"
    if not runner.get_endpoint(cfg, clean["source_endpoint"]):
        return False, "source endpoint not found"
    if len(clean["target_endpoints"]) != len(set(clean["target_endpoints"])):
        return False, "duplicate target endpoints are not allowed"
    for tid in clean["target_endpoints"]:
        if not runner.get_endpoint(cfg, tid):
            return False, "target endpoint not found"
    target_pairs: set[tuple[str, str]] = set()
    for tid in clean["target_endpoints"]:
        ep = runner.get_endpoint(cfg, tid) or {}
        target_pairs.add((str(ep.get("provider") or "").strip().upper(), normalize_instance_id(ep.get("instance"))))
    if len(target_pairs) > 1:
        return False, "target endpoints must share the same provider profile"
    if clean["ruleset_id"]:
        rs = runner.get_ruleset(cfg, clean["ruleset_id"])
        if not rs:
            return False, "ruleset not found"
        if len(clean["target_endpoints"]) > int(rs.get("maximum_targets") or 1):
            return False, "too many target endpoints for ruleset"
    elif len(clean["target_endpoints"]) != 1:
        return False, "direct mappings require exactly one target endpoint"
    first_target = clean["target_endpoints"][0]
    if clean["order"] == "preserve" and not _endpoint_reorderable(cfg, first_target):
        return False, "target provider does not support ordering; use order 'ignore'"
    return True, ""


def assigned_pair_of(cfg: Mapping[str, Any], mapping_id: str) -> str | None:
    mid = str(mapping_id or "").strip()
    for p in _cfg_pairs(cfg):
        if mid in runner.pair_mapping_ids(p):
            return str(p.get("id") or "")
    return None


def _pair_playlist_block(pair: Mapping[str, Any]) -> dict[str, Any]:
    feats = pair.get("features") if isinstance(pair, Mapping) else None
    pl = feats.get("playlists") if isinstance(feats, Mapping) else None
    return dict(pl) if isinstance(pl, Mapping) else {}


def _managed_playlist_pair_for(pair: Mapping[str, Any], mapping_id: str) -> bool:
    pl = _pair_playlist_block(pair)
    return str(pl.get("managed_by") or "") == "playlists" and str(pl.get("mapping_id") or "") == str(mapping_id)


def _mapping_direction_mode(cfg: Mapping[str, Any], clean: Mapping[str, Any]) -> str:
    rs = runner.get_ruleset(cfg, clean.get("ruleset_id")) if clean.get("ruleset_id") else None
    return "two-way" if rs and str(rs.get("direction") or "") == "bidirectional" else "one-way"


def _new_pair_id(pairs: list[dict[str, Any]]) -> str:
    existing = {str(p.get("id") or "") for p in pairs if isinstance(p, Mapping)}
    for _ in range(20):
        pid = f"pair_playlist_{uuid.uuid4().hex[:12]}"
        if pid not in existing:
            return pid
    return f"pair_playlist_{int(time.time())}"


def _pair_payload_for_mapping(cfg: Mapping[str, Any], clean: Mapping[str, Any]) -> tuple[bool, str, dict[str, Any] | None]:
    src_ep = runner.get_endpoint(cfg, clean.get("source_endpoint")) or {}
    target_eps = [runner.get_endpoint(cfg, tid) or {} for tid in _clean_target_ids(clean)]
    target_pairs = {
        (str(ep.get("provider") or "").strip().upper(), normalize_instance_id(ep.get("instance")))
        for ep in target_eps
        if ep
    }
    if not src_ep or not target_pairs:
        return False, "source and target endpoints required", None
    if len(target_pairs) != 1:
        return False, "target endpoints must share the same provider profile", None
    target_provider, target_instance = next(iter(target_pairs))
    source_provider = str(src_ep.get("provider") or "").strip().upper()
    source_instance = normalize_instance_id(src_ep.get("instance"))
    mapping_id = str(clean.get("id") or "").strip()
    if not mapping_id:
        return False, "mapping id required", None
    return True, "", {
        "source": source_provider,
        "target": target_provider,
        "source_instance": source_instance,
        "target_instance": target_instance,
        "mode": _mapping_direction_mode(cfg, clean),
        "enabled": bool(clean.get("enabled", True)),
        "features": {
            "playlists": {
                "enable": True,
                "mappings": [mapping_id],
                "managed_by": "playlists",
                "mapping_id": mapping_id,
            }
        },
    }


def _ensure_mapping_pair(cfg: dict[str, Any], clean: Mapping[str, Any]) -> tuple[bool, str, str]:
    pairs = cfg.get("pairs")
    if not isinstance(pairs, list):
        pairs = []
        cfg["pairs"] = pairs
    ok, why, payload = _pair_payload_for_mapping(cfg, clean)
    if not ok or payload is None:
        return False, why, ""
    mid = str(clean.get("id") or "").strip()
    existing: dict[str, Any] | None = None
    for pair in pairs:
        if isinstance(pair, dict) and _managed_playlist_pair_for(pair, mid):
            existing = pair
            break
    if existing is None:
        for pair in pairs:
            if not isinstance(pair, dict):
                continue
            if mid in runner.pair_mapping_ids(pair):
                feats = pair.get("features")
                pl = feats.get("playlists") if isinstance(feats, dict) else None
                if isinstance(pl, dict):
                    pl["mappings"] = [x for x in runner.pair_mapping_ids(pair) if x != mid]
    if existing is None:
        item = dict(payload)
        item["id"] = _new_pair_id(pairs)
        pairs.append(item)
        return True, "", item["id"]
    existing.update(payload)
    existing.setdefault("id", _new_pair_id(pairs))
    return True, "", str(existing.get("id") or "")


def _remove_mapping_from_pairs(cfg: dict[str, Any], mapping_id: str) -> None:
    pairs = cfg.get("pairs")
    if not isinstance(pairs, list):
        return
    kept: list[dict[str, Any]] = []
    for pair in pairs:
        if not isinstance(pair, dict):
            kept.append(pair)
            continue
        feats = pair.get("features")
        pl = feats.get("playlists") if isinstance(feats, dict) else None
        if not isinstance(pl, dict):
            kept.append(pair)
            continue
        if _managed_playlist_pair_for(pair, mapping_id):
            continue
        ids = [x for x in runner.pair_mapping_ids(pair) if x != mapping_id]
        if ids != runner.pair_mapping_ids(pair):
            pl["mappings"] = ids
            if not ids and str(pl.get("managed_by") or "") == "playlists":
                continue
        kept.append(pair)
    cfg["pairs"] = kept


def _refresh_mapping_pairs_for_endpoint(cfg: dict[str, Any], endpoint_id: str) -> None:
    eid = str(endpoint_id or "").strip()
    if not eid:
        return
    for mapping in runner.list_mapping_profiles(cfg):
        if str(mapping.get("source_endpoint") or "") == eid or eid in _clean_target_ids(mapping):
            _ensure_mapping_pair(cfg, _clean_mapping(mapping))


def _refresh_mapping_pairs_for_ruleset(cfg: dict[str, Any], ruleset_id: str) -> None:
    rid = str(ruleset_id or "").strip()
    if not rid:
        return
    for mapping in runner.list_mapping_profiles(cfg):
        if str(mapping.get("ruleset_id") or "") == rid:
            _ensure_mapping_pair(cfg, _clean_mapping(mapping))


def reconcile_mapping_pairs(cfg: dict[str, Any]) -> dict[str, Any]:
    created: list[str] = []
    errors: list[str] = []
    for mapping in runner.list_mapping_profiles(cfg):
        clean = _clean_mapping(mapping)
        mid = clean.get("id") or ""
        if not mid or assigned_pair_of(cfg, mid):
            continue
        ok, why, pair_id = _ensure_mapping_pair(cfg, clean)
        if ok and pair_id:
            created.append(pair_id)
        elif why:
            errors.append(f"{mid}: {why}")
    if created:
        _save(cfg)
    return {"ok": not errors, "created": created, "errors": errors}


def _enrich_mapping(cfg: Mapping[str, Any], profile: Mapping[str, Any]) -> dict[str, Any]:
    provs = _providers()
    clean = _clean_mapping(profile)
    resolved = runner.resolve_mapping(cfg, profile)
    valid = resolved is not None
    src_ep = runner.get_endpoint(cfg, clean["source_endpoint"]) or {}
    first_target = clean["target_endpoints"][0] if clean["target_endpoints"] else ""
    dst_ep = runner.get_endpoint(cfg, first_target) or {}
    target_eps = [runner.get_endpoint(cfg, tid) or {} for tid in clean["target_endpoints"]]
    result = None
    if resolved:
        try:
            result = runner.load_result(resolved)
        except Exception:
            result = None
    clean.update(
        {
            "valid": valid,
            "source": {
                "endpoint_id": clean["source_endpoint"],
                "name": src_ep.get("name"),
                "provider": str(src_ep.get("provider") or "").upper(),
                "instance": normalize_instance_id(src_ep.get("instance")),
                "playlist_name": src_ep.get("playlist_name"),
                "label": _label_for(provs.get(str(src_ep.get("provider") or "").upper()), str(src_ep.get("provider") or "")),
            },
            "target": {
                "endpoint_id": first_target,
                "name": dst_ep.get("name"),
                "provider": str(dst_ep.get("provider") or "").upper(),
                "instance": normalize_instance_id(dst_ep.get("instance")),
                "playlist_name": dst_ep.get("playlist_name"),
                "label": _label_for(provs.get(str(dst_ep.get("provider") or "").upper()), str(dst_ep.get("provider") or "")),
            },
            "targets": [
                {
                    "endpoint_id": str(ep.get("id") or ""),
                    "name": ep.get("name"),
                    "provider": str(ep.get("provider") or "").upper(),
                    "instance": normalize_instance_id(ep.get("instance")),
                    "playlist_name": ep.get("playlist_name"),
                    "label": _label_for(provs.get(str(ep.get("provider") or "").upper()), str(ep.get("provider") or "")),
                }
                for ep in target_eps
                if ep
            ],
            "ruleset": runner.get_ruleset(cfg, clean["ruleset_id"]) if clean["ruleset_id"] else None,
            "assigned_pair": assigned_pair_of(cfg, clean["id"]),
            "last_result": result,
        }
    )
    pair_id = clean.get("assigned_pair") or assigned_pair_of(cfg, clean["id"])
    if pair_id:
        clean["assigned_pair"] = pair_id
        pair = _find_pair(cfg, pair_id)
        if pair:
            clean["assigned_pair_label"] = (
                f"{pair.get('source')}:{normalize_instance_id(pair.get('source_instance'))}"
                f" -> {pair.get('target')}:{normalize_instance_id(pair.get('target_instance'))}"
            )
    return clean


def list_mappings(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    if isinstance(cfg, dict):
        reconcile_mapping_pairs(cfg)
    return [_enrich_mapping(cfg, m) for m in runner.list_mapping_profiles(cfg)]


def get_mapping(cfg: Mapping[str, Any], mapping_id: str) -> dict[str, Any] | None:
    return runner.get_mapping_profile(cfg, mapping_id)


def upsert_mapping(cfg: dict[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    ok, why = validate_mapping(cfg, payload)
    if not ok:
        return {"ok": False, "error": why}
    clean = _clean_mapping(payload)
    root = runner.pl_root(cfg, create=True)
    mappings = root["mappings"]
    mid = clean["id"]
    if mid:
        for i, m in enumerate(mappings):
            if isinstance(m, Mapping) and str(m.get("id") or "") == mid:
                pair_ok, pair_error, pair_id = _ensure_mapping_pair(cfg, clean)
                if not pair_ok:
                    return {"ok": False, "error": pair_error}
                mappings[i] = clean
                _save(cfg)
                out = _enrich_mapping(cfg, clean)
                out["assigned_pair"] = pair_id or out.get("assigned_pair")
                return {"ok": True, "mapping": out, "created": False, "pair_id": pair_id}
    clean["id"] = runner.next_id(mappings, runner.MAPPING_PREFIX)
    pair_ok, pair_error, pair_id = _ensure_mapping_pair(cfg, clean)
    if not pair_ok:
        return {"ok": False, "error": pair_error}
    mappings.append(clean)
    _save(cfg)
    out = _enrich_mapping(cfg, clean)
    out["assigned_pair"] = pair_id or out.get("assigned_pair")
    return {"ok": True, "mapping": out, "created": True, "pair_id": pair_id}


def delete_mapping(cfg: dict[str, Any], mapping_id: str) -> dict[str, Any]:
    mid = str(mapping_id or "").strip()
    root = runner.pl_root(cfg, create=True)
    target = None
    for m in root["mappings"]:
        if isinstance(m, Mapping) and str(m.get("id") or "") == mid:
            target = m
            break
    if target is None:
        return {"ok": False, "error": "mapping not found"}
    resolved = runner.resolve_mapping(cfg, target)
    if resolved:
        try:
            runner.delete_baseline(resolved)
        except Exception:
            pass
    _remove_mapping_from_pairs(cfg, mid)
    root["mappings"] = [m for m in root["mappings"] if m is not target]
    for p in _cfg_pairs(cfg):
        feats = p.get("features") if isinstance(p, dict) else None
        pl = feats.get("playlists") if isinstance(feats, dict) else None
        if isinstance(pl, dict) and isinstance(pl.get("mappings"), list):
            pl["mappings"] = [x for x in pl["mappings"] if str(x).strip() != mid]
    _save(cfg)
    return {"ok": True}


def list_rulesets(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    return runner.list_rulesets(cfg)


def get_ruleset(cfg: Mapping[str, Any], ruleset_id: str) -> dict[str, Any] | None:
    return runner.get_ruleset(cfg, ruleset_id)


def upsert_ruleset(cfg: dict[str, Any], payload: Mapping[str, Any]) -> dict[str, Any]:
    raw = dict(payload or {})
    rid = str(raw.get("id") or "").strip()
    if rid in BUILTIN_RULESETS:
        return {"ok": False, "error": "built in rulesets are immutable"}
    raw["built_in"] = False
    ok, why, clean = validate_ruleset(raw, require_id=bool(rid))
    if not ok or not clean:
        return {"ok": False, "error": why}
    root = runner.pl_root(cfg, create=True)
    rulesets = root["rulesets"]
    if not clean["id"]:
        clean["id"] = runner.next_id(rulesets, runner.RULESET_PREFIX)
    for i, existing in enumerate(rulesets):
        if isinstance(existing, Mapping) and str(existing.get("id") or "") == clean["id"]:
            rulesets[i] = clean
            _refresh_mapping_pairs_for_ruleset(cfg, clean["id"])
            _save(cfg)
            return {"ok": True, "ruleset": clean, "created": False}
    rulesets.append(clean)
    _refresh_mapping_pairs_for_ruleset(cfg, clean["id"])
    _save(cfg)
    return {"ok": True, "ruleset": clean, "created": True}


def clone_ruleset(cfg: dict[str, Any], ruleset_id: str, payload: Mapping[str, Any] | None = None) -> dict[str, Any]:
    base = runner.get_ruleset(cfg, ruleset_id)
    if not base:
        return {"ok": False, "error": "ruleset not found"}
    raw = dict(base)
    raw["id"] = ""
    raw["name"] = str((payload or {}).get("name") or f"{base.get('name')} Custom").strip()
    raw["built_in"] = False
    return upsert_ruleset(cfg, raw)


def delete_ruleset(cfg: dict[str, Any], ruleset_id: str) -> dict[str, Any]:
    rid = str(ruleset_id or "").strip()
    if rid in BUILTIN_RULESETS:
        return {"ok": False, "error": "built in rulesets cannot be deleted"}
    used_by = [str(m.get("id")) for m in runner.list_mapping_profiles(cfg) if str(m.get("ruleset_id") or "") == rid]
    if used_by:
        return {"ok": False, "error": f"ruleset in use by {', '.join(used_by)}"}
    root = runner.pl_root(cfg, create=True)
    before = len(root["rulesets"])
    root["rulesets"] = [r for r in root["rulesets"] if str((r or {}).get("id") or "") != rid]
    if len(root["rulesets"]) == before:
        return {"ok": False, "error": "ruleset not found"}
    _save(cfg)
    return {"ok": True}


def validate_ruleset_payload(payload: Mapping[str, Any]) -> dict[str, Any]:
    ok, why, clean = validate_ruleset(dict(payload or {}), require_id=bool((payload or {}).get("id")))
    return {"ok": ok, "error": why, "ruleset": clean}


# Pair-facing

def mappings_for_pair(cfg: Mapping[str, Any], pair_id: str) -> dict[str, Any]:
    pair = _find_pair(cfg, pair_id)
    if not pair:
        return {"ok": False, "error": "pair not found", "mappings": []}
    selected = set(runner.pair_mapping_ids(pair))
    out: list[dict[str, Any]] = []
    for profile in runner.list_mapping_profiles(cfg):
        resolved = runner.resolve_mapping(cfg, profile)
        if not resolved:
            continue
        if not runner.mapping_compatible_with_pair(resolved, pair):
            continue
        mid = str(profile.get("id") or "")
        assigned = assigned_pair_of(cfg, mid)
        if assigned and assigned != str(pair.get("id") or ""):
            continue
        enriched = _enrich_mapping(cfg, profile)
        enriched["selected"] = mid in selected
        out.append(enriched)
    return {"ok": True, "pair_id": str(pair.get("id") or ""), "mappings": out}


# Run / preview

@contextmanager
def _mapping_env(resolved: Mapping[str, Any]) -> Iterator[None]:
    src = resolved["source"]
    dst = resolved["target"]
    keys = {
        "CW_PAIR_KEY": f"playlist:{resolved.get('id')}",
        "CW_PAIR_SRC": str(src.get("provider") or ""),
        "CW_PAIR_SRC_INSTANCE": str(src.get("instance") or "default"),
        "CW_PAIR_DST": str(dst.get("provider") or ""),
        "CW_PAIR_DST_INSTANCE": str(dst.get("instance") or "default"),
    }
    prev = {k: os.environ.get(k) for k in keys}
    os.environ.update({k: v for k, v in keys.items() if v})
    try:
        yield
    finally:
        for k, v in prev.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def preview_mapping(cfg: Mapping[str, Any], mapping_id: str) -> dict[str, Any]:
    resolved = runner.resolve_mapping_by_id(cfg, mapping_id)
    if not resolved:
        return {"ok": False, "error": "mapping not found or endpoints missing"}
    try:
        with _mapping_env(resolved):
            plan = runner.preview_mapping(cfg, resolved)
    except runner.PlaylistRunError as e:
        return _internal_playlist_error("preview", e, mapping_id=mapping_id)
    except Exception as e:
        return _internal_playlist_error("preview", e, mapping_id=mapping_id)
    return {"ok": True, "preview": plan}


def run_mapping(cfg: Mapping[str, Any], mapping_id: str, *, dry_run: bool = False) -> dict[str, Any]:
    resolved = runner.resolve_mapping_by_id(cfg, mapping_id)
    if not resolved:
        return {"ok": False, "error": "mapping not found or endpoints missing"}
    try:
        with _mapping_env(resolved):
            result = runner.run_mapping(cfg, resolved, dry_run=dry_run)
    except runner.PlaylistRunError as e:
        return _internal_playlist_error("run", e, mapping_id=mapping_id, dry_run=dry_run)
    except Exception as e:
        return _internal_playlist_error("run", e, mapping_id=mapping_id, dry_run=dry_run)
    return {"ok": True, "result": result}


def latest_result(cfg: Mapping[str, Any], mapping_id: str) -> dict[str, Any]:
    resolved = runner.resolve_mapping_by_id(cfg, mapping_id)
    if not resolved:
        return {"ok": False, "error": "mapping not found"}
    try:
        result = runner.load_result(resolved)
    except Exception:
        result = None
    return {"ok": True, "result": result}


def overview(cfg: Mapping[str, Any]) -> dict[str, Any]:
    mappings = list_mappings(cfg)
    enabled = [m for m in mappings if m.get("enabled") and m.get("assigned_pair")]
    last_sync = 0
    unresolved = 0
    warnings: list[str] = []
    for m in mappings:
        res = m.get("last_result") or {}
        if isinstance(res, Mapping):
            ts = int(res.get("finished_at") or 0)
            last_sync = max(last_sync, ts)
            unresolved += int(res.get("unresolved_count") or 0)
            for w in res.get("warnings") or []:
                warnings.append(str(w))
    return {
        "ok": True,
        "total_mappings": len(mappings),
        "enabled_mappings": len(enabled),
        "endpoints": len(runner.list_endpoints(cfg)),
        "last_sync_epoch": last_sync or None,
        "unresolved": unresolved,
        "warnings": sorted(set(warnings)),
    }


def cleanup_state(cfg: Mapping[str, Any]) -> dict[str, Any]:
    valid: set[str] = set()
    for profile in runner.list_mapping_profiles(cfg):
        resolved = runner.resolve_mapping(cfg, profile)
        if resolved:
            valid.add(runner.scope_key(resolved))
    removed = runner.prune_baselines(valid)
    return {"ok": True, "pruned": removed}


def _save(cfg: dict[str, Any]) -> None:
    from cw_platform.config_base import save_config

    save_config(cfg)

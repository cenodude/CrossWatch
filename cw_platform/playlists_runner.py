# cw_platform/playlists_runner.py
# CrossWatch - Playlist endpoints, mapping profiles and one-way sync runner
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Iterable, Mapping, Sequence

from .config_base import CONFIG_BASE
from .id_map import any_key_overlap, canonical_key, keys_for_item
from .playlists import (
    BUILTIN_RULESETS,
    BUILTIN_TRAKT_FREE_ACCOUNT_RULESET_ID,
    PlaylistItem,
    PlaylistResource,
    PlaylistSnapshot,
    builtin_rulesets,
    normalize_ruleset,
    supports_playlists,
    validate_ruleset,
)
from .provider_instances import build_provider_config_view, normalize_instance_id

MEMBERSHIP_ADD_ONLY = "add_only"
MEMBERSHIP_MANAGED_ONLY = "managed_only"
MEMBERSHIP_MIRROR = "mirror"
_MEMBERSHIP = {MEMBERSHIP_ADD_ONLY, MEMBERSHIP_MANAGED_ONLY, MEMBERSHIP_MIRROR}

ORDER_IGNORE = "ignore"
ORDER_PRESERVE = "preserve"
_ORDER = {ORDER_IGNORE, ORDER_PRESERVE}

ENDPOINT_PREFIX = "EP"
MAPPING_PREFIX = "MAP"
RULESET_PREFIX = "RULE"


class PlaylistRunError(RuntimeError):
    pass


def _state_path() -> Path:
    return Path(CONFIG_BASE()) / ".cw_state" / "playlists_state.json"


def _load_all_state() -> dict[str, Any]:
    p = _state_path()
    try:
        data = json.loads(p.read_text("utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_all_state(data: Mapping[str, Any]) -> None:
    p = _state_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(dict(data), ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        tmp.replace(p)
    except Exception:
        pass


def next_id(items: Sequence[Mapping[str, Any]], prefix: str) -> str:
    nums: list[int] = []
    for it in items or []:
        raw = str((it or {}).get("id") or "")
        if raw.upper().startswith(f"{prefix}-"):
            tail = raw.split("-", 1)[1]
            if tail.isdigit():
                nums.append(int(tail))
    n = (max(nums) + 1) if nums else 1
    return f"{prefix}-{n:02d}"


def pl_root(cfg: Mapping[str, Any], *, create: bool = False) -> dict[str, Any]:
    root = cfg.get("playlists") if isinstance(cfg, Mapping) else None
    if not isinstance(root, dict):
        if not create or not isinstance(cfg, dict):
            return {"endpoints": [], "mappings": [], "rulesets": []}
        root = {"endpoints": [], "mappings": [], "rulesets": []}
        cfg["playlists"] = root
    if not isinstance(root.get("endpoints"), list):
        root["endpoints"] = []
    if not isinstance(root.get("mappings"), list):
        root["mappings"] = []
    if not isinstance(root.get("rulesets"), list):
        root["rulesets"] = []
    return root


def list_builtin_rulesets() -> list[dict[str, Any]]:
    return builtin_rulesets()


def list_custom_rulesets(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for r in pl_root(cfg).get("rulesets", []):
        if isinstance(r, Mapping):
            ok, _why, clean = validate_ruleset(r)
            if ok and clean:
                clean["built_in"] = False
                out.append(clean)
    return out


def list_rulesets(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    custom = {r["id"]: r for r in list_custom_rulesets(cfg)}
    out = list_builtin_rulesets()
    out.extend(custom[k] for k in sorted(custom))
    return out


def get_ruleset(cfg: Mapping[str, Any], ruleset_id: Any) -> dict[str, Any] | None:
    rid = str(ruleset_id or "").strip()
    if not rid:
        return None
    if rid in BUILTIN_RULESETS:
        return normalize_ruleset(BUILTIN_RULESETS[rid], built_in=True)
    for r in list_custom_rulesets(cfg):
        if r.get("id") == rid:
            return r
    return None


def list_endpoints(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(e) for e in pl_root(cfg).get("endpoints", []) if isinstance(e, Mapping)]


def get_endpoint(cfg: Mapping[str, Any], endpoint_id: Any) -> dict[str, Any] | None:
    eid = str(endpoint_id or "").strip()
    if not eid:
        return None
    for e in pl_root(cfg).get("endpoints", []):
        if isinstance(e, Mapping) and str(e.get("id") or "") == eid:
            return dict(e)
    return None


def list_mapping_profiles(cfg: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(m) for m in pl_root(cfg).get("mappings", []) if isinstance(m, Mapping)]


def get_mapping_profile(cfg: Mapping[str, Any], mapping_id: Any) -> dict[str, Any] | None:
    mid = str(mapping_id or "").strip()
    if not mid:
        return None
    for m in pl_root(cfg).get("mappings", []):
        if isinstance(m, Mapping) and str(m.get("id") or "") == mid:
            return dict(m)
    return None


def _endpoint_view(ep: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "provider": str(ep.get("provider") or "").strip().upper(),
        "instance": normalize_instance_id(ep.get("instance")),
        "playlist_id": str(ep.get("playlist_id") or "").strip(),
        "playlist_name": ep.get("playlist_name") or ep.get("name"),
        "endpoint_id": str(ep.get("id") or "").strip(),
    }


def _target_ids(profile: Mapping[str, Any]) -> list[str]:
    raw = profile.get("target_endpoints")
    out: list[str] = []
    if isinstance(raw, list):
        for x in raw:
            s = str((x or {}).get("id") if isinstance(x, Mapping) else x).strip()
            if s and s not in out:
                out.append(s)
    return out


def _norm_membership_val(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in _MEMBERSHIP else MEMBERSHIP_MANAGED_ONLY


def _norm_order_val(v: Any) -> str:
    s = str(v or "").strip().lower()
    return s if s in _ORDER else ORDER_IGNORE


def resolve_mapping(cfg: Mapping[str, Any], profile: Mapping[str, Any]) -> dict[str, Any] | None:
    se = get_endpoint(cfg, profile.get("source_endpoint"))
    target_ids = _target_ids(profile)
    target_eps = [get_endpoint(cfg, tid) for tid in target_ids]
    targets = [e for e in target_eps if e]
    if not se or not targets or len(targets) != len(target_ids):
        return None
    te = targets[0]
    return {
        "id": str(profile.get("id") or "").strip(),
        "name": profile.get("name") or profile.get("id"),
        "source_endpoint": str(profile.get("source_endpoint") or "").strip(),
        "target_endpoints": [str(e.get("id") or "").strip() for e in targets],
        "source": _endpoint_view(se),
        "target": _endpoint_view(te),
        "targets": [_endpoint_view(e) for e in targets],
        "ruleset_id": str(profile.get("ruleset_id") or "").strip(),
        "membership": _norm_membership_val(profile.get("membership")),
        "order": _norm_order_val(profile.get("order")),
        "enabled": bool(profile.get("enabled", True)),
        "allow_mass_delete": bool(profile.get("allow_mass_delete")),
    }


def resolve_mapping_by_id(cfg: Mapping[str, Any], mapping_id: Any) -> dict[str, Any] | None:
    profile = get_mapping_profile(cfg, mapping_id)
    return resolve_mapping(cfg, profile) if profile else None


def _pair_id(pair: Mapping[str, Any]) -> str:
    return str(pair.get("id") or pair.get("pair_id") or "").strip() or "pair"


def _pair_providers(pair: Mapping[str, Any]) -> tuple[str, str, str, str]:
    src = str(pair.get("src") or pair.get("source") or "").strip().upper()
    dst = str(pair.get("dst") or pair.get("target") or "").strip().upper()
    src_inst = normalize_instance_id(pair.get("src_instance") or pair.get("source_instance"))
    dst_inst = normalize_instance_id(pair.get("dst_instance") or pair.get("target_instance"))
    return src, src_inst, dst, dst_inst


def _pair_mode(pair: Mapping[str, Any]) -> str:
    return "two-way" if "two" in str(pair.get("mode") or "one-way").strip().lower() else "one-way"


def pair_mapping_ids(pair: Mapping[str, Any]) -> list[str]:
    feats = pair.get("features") if isinstance(pair, Mapping) else None
    pl = feats.get("playlists") if isinstance(feats, Mapping) else None
    ids = pl.get("mappings") if isinstance(pl, Mapping) else None
    out: list[str] = []
    for x in ids or []:
        s = str(x).strip() if not isinstance(x, Mapping) else str(x.get("id") or "").strip()
        if s and s not in out:
            out.append(s)
    return out


def _mapping_targets(mapping: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    raw = mapping.get("targets")
    if not isinstance(raw, list):
        return []
    return [t for t in raw if isinstance(t, Mapping)]


def mapping_compatible_with_pair(resolved: Mapping[str, Any], pair: Mapping[str, Any]) -> bool:
    src, src_inst, dst, dst_inst = _pair_providers(pair)
    s = (resolved["source"]["provider"], resolved["source"]["instance"])
    targets = _mapping_targets(resolved)
    target_pairs = {(str(t.get("provider") or ""), str(t.get("instance") or "")) for t in targets if isinstance(t, Mapping)}
    if _pair_mode(pair) == "two-way":
        return (s == (src, src_inst) and target_pairs == {(dst, dst_inst)}) or (s == (dst, dst_inst) and target_pairs == {(src, src_inst)})
    return s == (src, src_inst) and target_pairs == {(dst, dst_inst)}


def resolve_pair_mappings(cfg: Mapping[str, Any], pair: Mapping[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for mid in pair_mapping_ids(pair):
        resolved = resolve_mapping_by_id(cfg, mid)
        if not resolved or not resolved.get("enabled", True):
            continue
        if not mapping_compatible_with_pair(resolved, pair):
            continue
        out.append(resolved)
    return out


def scope_key(mapping: Mapping[str, Any]) -> str:
    s = mapping.get("source") or {}
    targets = _mapping_targets(mapping)
    target_parts: list[str] = []
    for t in targets:
        target_parts.append("/".join([
            str(t.get("endpoint_id") or ""),
            str(t.get("provider") or ""),
            str(t.get("instance") or ""),
            str(t.get("playlist_id") or ""),
        ]))
    parts = [
        str(mapping.get("id") or ""),
        str(mapping.get("ruleset_id") or ""),
        str(s.get("provider") or ""),
        str(s.get("instance") or ""),
        str(s.get("playlist_id") or ""),
        ",".join(target_parts),
    ]
    return "|".join(parts)


def load_baseline(mapping: Mapping[str, Any]) -> set[str]:
    entry = _load_all_state().get(scope_key(mapping)) or {}
    managed = entry.get("managed") if isinstance(entry, Mapping) else None
    return {str(k) for k in (managed or []) if str(k).strip()}


def save_baseline(mapping: Mapping[str, Any], managed: set[str], *, meta: Mapping[str, Any] | None = None) -> None:
    data = _load_all_state()
    key = scope_key(mapping)
    raw_entry = data.get(key)
    entry = raw_entry if isinstance(raw_entry, dict) else {}
    entry["managed"] = sorted(managed)
    entry["updated_at"] = int(time.time())
    entry["meta"] = dict(meta or {})
    data[key] = entry
    _save_all_state(data)


def delete_baseline(mapping: Mapping[str, Any]) -> bool:
    data = _load_all_state()
    key = scope_key(mapping)
    if key in data:
        data.pop(key, None)
        _save_all_state(data)
        return True
    return False


def store_result(mapping: Mapping[str, Any], result: Mapping[str, Any]) -> None:
    data = _load_all_state()
    key = scope_key(mapping)
    raw = data.get(key)
    entry: dict[str, Any] = raw if isinstance(raw, dict) else {}
    entry["last_result"] = dict(result)
    data[key] = entry
    _save_all_state(data)


def load_result(mapping: Mapping[str, Any]) -> dict[str, Any] | None:
    entry = _load_all_state().get(scope_key(mapping)) or {}
    res = entry.get("last_result") if isinstance(entry, Mapping) else None
    return dict(res) if isinstance(res, Mapping) else None


def prune_baselines(valid_keys: set[str]) -> int:
    data = _load_all_state()
    removed = 0
    for key in list(data.keys()):
        if key not in valid_keys:
            data.pop(key, None)
            removed += 1
    if removed:
        _save_all_state(data)
    return removed


@dataclass
class PlaylistPlan:
    membership: str
    order: str
    source_count: int = 0
    target_count: int = 0
    resolved_count: int = 0
    unresolved_count: int = 0
    additions: list[str] = field(default_factory=list)
    removals: list[str] = field(default_factory=list)
    reorder_count: int = 0
    manual_affected: bool = False
    warnings: list[str] = field(default_factory=list)
    add_items: list[dict[str, Any]] = field(default_factory=list)
    remove_items: list[dict[str, Any]] = field(default_factory=list)
    target_order: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "membership": self.membership,
            "order": self.order,
            "source_count": self.source_count,
            "target_count": self.target_count,
            "resolved_count": self.resolved_count,
            "unresolved_count": self.unresolved_count,
            "additions": list(self.additions),
            "removals": list(self.removals),
            "planned_additions": len(self.additions),
            "planned_removals": len(self.removals),
            "reorder_count": self.reorder_count,
            "manual_affected": self.manual_affected,
            "warnings": list(self.warnings),
        }


def _providers() -> dict[str, Any]:
    from .orchestrator._providers import load_sync_providers

    return load_sync_providers()


def _find_resource(ops: Any, cfg: Mapping[str, Any], instance: str, playlist_id: str):
    try:
        for res in ops.list_playlist_resources(cfg, instance=instance) or []:
            if res.id == playlist_id or res.name == playlist_id:
                return res
    except Exception:
        return None
    return None


def _merge_warnings(target: dict[str, Any] | list[str], *sources: Any) -> None:
    warnings = target if isinstance(target, list) else target.setdefault("warnings", [])
    if not isinstance(warnings, list):
        return
    seen = {str(x) for x in warnings}
    for source in sources:
        raw = source
        if isinstance(source, PlaylistResource):
            extra = source.extra or {}
            raw = list(extra.get("warnings") or [])
            if extra.get("remove_warning"):
                raw.append(extra.get("remove_warning"))
        elif isinstance(source, Mapping):
            raw = source.get("warnings") or []
        if isinstance(raw, str):
            values = [raw]
        elif isinstance(raw, Iterable):
            values = list(raw)
        else:
            values = []
        for value in values:
            text = str(value or "").strip()
            if text and text not in seen:
                warnings.append(text)
                seen.add(text)


def _state_entry(mapping: Mapping[str, Any]) -> dict[str, Any]:
    raw = _load_all_state().get(scope_key(mapping))
    return dict(raw) if isinstance(raw, Mapping) else {}


def _save_state_entry(mapping: Mapping[str, Any], entry: Mapping[str, Any]) -> None:
    data = _load_all_state()
    data[scope_key(mapping)] = dict(entry)
    _save_all_state(data)


def _ops_cfg(providers: Mapping[str, Any], cfg: Mapping[str, Any], endpoint: Mapping[str, Any]) -> tuple[Any, dict[str, Any], str, str, str]:
    provider = str(endpoint.get("provider") or "").strip().upper()
    inst = normalize_instance_id(endpoint.get("instance"))
    playlist_id = str(endpoint.get("playlist_id") or "").strip()
    ops = providers.get(provider)
    if not ops or not supports_playlists(ops):
        raise PlaylistRunError(f"provider {provider or '?'} does not support playlists")
    return ops, build_provider_config_view(cfg, provider, inst), inst, provider, playlist_id


def _snapshot_endpoint(providers: Mapping[str, Any], cfg: Mapping[str, Any], endpoint: Mapping[str, Any]) -> tuple[PlaylistSnapshot, Any, Mapping[str, Any], str]:
    ops, view, inst, provider, playlist_id = _ops_cfg(providers, cfg, endpoint)
    if not playlist_id:
        raise PlaylistRunError("playlist id missing")
    snap = ops.get_playlist_snapshot(view, playlist_id, instance=inst)
    if not isinstance(snap, PlaylistSnapshot):
        raise PlaylistRunError(f"{provider} returned an invalid playlist snapshot")
    return snap, ops, view, inst


def _unique_snapshot_items(snapshot: PlaylistSnapshot) -> tuple[list[PlaylistItem], dict[str, PlaylistItem]]:
    ordered: list[PlaylistItem] = []
    by_key: dict[str, PlaylistItem] = {}
    for it in snapshot.items:
        if not it.key or it.key in by_key:
            continue
        by_key[it.key] = it
        ordered.append(it)
    return ordered, by_key


def _playlist_item_keys(item: PlaylistItem) -> set[str]:
    keys = {str(item.key or "").strip().lower()} if item.key else set()
    try:
        keys.update(keys_for_item(item.item or {}))
    except Exception:
        pass
    return {k for k in keys if k}


def _snapshot_key_union(items: Iterable[PlaylistItem]) -> set[str]:
    out: set[str] = set()
    for item in items:
        out.update(_playlist_item_keys(item))
    return out


def _assignment_map(entry: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    raw = entry.get("assignments") if isinstance(entry, Mapping) else None
    return {str(k): dict(v) for k, v in (raw or {}).items() if isinstance(v, Mapping)}


def _aggregate_targets(
    providers: Mapping[str, Any],
    cfg: Mapping[str, Any],
    targets: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    logical: dict[str, PlaylistItem] = {}
    physical: dict[str, list[dict[str, Any]]] = {}
    per_target: dict[str, dict[str, Any]] = {}
    target_ctx: dict[str, dict[str, Any]] = {}
    duplicates: list[dict[str, Any]] = []
    warnings: list[str] = []
    for pos, target in enumerate(targets):
        endpoint_id = str(target.get("endpoint_id") or "").strip()
        snap, ops, view, inst = _snapshot_endpoint(providers, cfg, target)
        resource = snap.resource
        _merge_warnings(warnings, resource)
        if resource.is_smart:
            raise PlaylistRunError(f"target {endpoint_id} is a smart playlist and cannot be written")
        if not (resource.can_add or resource.can_remove):
            raise PlaylistRunError(f"target {endpoint_id} is read only")
        ordered, by_key = _unique_snapshot_items(snap)
        per_target[endpoint_id] = {
            "endpoint_id": endpoint_id,
            "provider": target.get("provider"),
            "instance": target.get("instance"),
            "playlist_id": target.get("playlist_id"),
            "playlist_name": target.get("playlist_name"),
            "current_count": len(snap.items),
            "logical_count": len(by_key),
            "available_capacity": 0,
            "planned_additions": 0,
            "planned_removals": 0,
        }
        target_ctx[endpoint_id] = {"ops": ops, "cfg": view, "instance": inst, "playlist_id": target.get("playlist_id")}
        for it in ordered:
            loc = {"endpoint_id": endpoint_id, "order": pos, "item": it}
            physical.setdefault(it.key, []).append(loc)
            if it.key not in logical:
                logical[it.key] = it
            else:
                duplicates.append({"key": it.key, "endpoints": [x["endpoint_id"] for x in physical[it.key]]})
    return {"logical": logical, "physical": physical, "per_target": per_target, "target_ctx": target_ctx, "duplicates": duplicates, "warnings": warnings}


def _source_resource_writable(providers: Mapping[str, Any], cfg: Mapping[str, Any], source: Mapping[str, Any]) -> bool:
    ops, view, inst, _provider, playlist_id = _ops_cfg(providers, cfg, source)
    res = _find_resource(ops, view, inst, playlist_id)
    return bool(res and not res.is_smart and res.can_add and res.can_remove)


def _ruleset_for_mapping(cfg: Mapping[str, Any], mapping: Mapping[str, Any]) -> dict[str, Any] | None:
    rid = str(mapping.get("ruleset_id") or "").strip()
    return get_ruleset(cfg, rid) if rid else None


def _first_physical_endpoint(physical: Mapping[str, list[dict[str, Any]]], key: str) -> str | None:
    locs = physical.get(key) or []
    if not locs:
        return None
    return str(sorted(locs, key=lambda x: int(x.get("order") or 0))[0].get("endpoint_id") or "") or None


def _plan_ruleset(
    cfg: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    providers: Mapping[str, Any] | None = None,
) -> tuple[dict[str, Any], dict[str, Any]]:
    providers = providers or _providers()
    ruleset = _ruleset_for_mapping(cfg, mapping)
    if not ruleset:
        raise PlaylistRunError("ruleset not found")
    if ruleset["read_mode"] != "aggregate" or ruleset["write_mode"] != "partition":
        raise PlaylistRunError("ruleset mode is not supported by this engine")
    targets = _mapping_targets(mapping)
    if len(targets) < 1:
        raise PlaylistRunError("at least one target endpoint is required")
    if len(targets) > int(ruleset["maximum_targets"]):
        raise PlaylistRunError("too many target endpoints for ruleset")
    endpoint_ids = [str(t.get("endpoint_id") or "").strip() for t in targets if isinstance(t, Mapping)]
    if len(endpoint_ids) != len(set(endpoint_ids)):
        raise PlaylistRunError("duplicate target endpoints are not allowed")
    source = mapping.get("source") or {}
    source_snap, source_ops, source_cfg, source_inst = _snapshot_endpoint(providers, cfg, source)
    if ruleset["direction"] == "bidirectional" and not _source_resource_writable(providers, cfg, source):
        raise PlaylistRunError("source endpoint must support add and remove for bidirectional rulesets")
    source_items, source_by_key = _unique_snapshot_items(source_snap)
    source_keys = [it.key for it in source_items]
    source_set = set(source_by_key)
    agg = _aggregate_targets(providers, cfg, targets)
    warnings: list[str] = list(agg.get("warnings") or [])
    if ruleset["direction"] == "bidirectional":
        _merge_warnings(warnings, source_snap.resource)
    target_by_key: dict[str, PlaylistItem] = dict(agg["logical"])
    target_set = set(target_by_key)
    physical = agg["physical"]
    entry = _state_entry(mapping)
    assignments = _assignment_map(entry)
    initialized = bool(entry.get("last_success"))
    prev_source = {str(k) for k in ((entry.get("last_success") or {}).get("source_keys") or [])}
    prev_target = {str(k) for k in ((entry.get("last_success") or {}).get("target_keys") or [])}
    managed = {k for k, v in assignments.items() if v.get("managed", True)}
    if not initialized:
        managed = set(source_set)
    add_to_source: set[str] = set()
    remove_from_source: set[str] = set()
    add_to_target: set[str] = set()
    remove_from_target: set[str] = set()
    if initialized:
        add_to_target |= source_set - prev_source
        remove_from_target |= {k for k in (prev_source - source_set) if k in managed}
        add_to_source |= target_set - prev_target
        remove_from_source |= {k for k in (prev_target - target_set) if k in managed}
    else:
        add_to_target |= source_set - target_set
    final_source = (source_set | add_to_source) - remove_from_source
    final_managed = (managed | source_set | add_to_source | add_to_target) - remove_from_source - remove_from_target
    per_target = {k: dict(v) for k, v in agg["per_target"].items()}
    capacity = int(ruleset["per_endpoint_capacity"])
    aggregate_capacity = int(ruleset["aggregate_capacity"])
    for eid, row in per_target.items():
        row["available_capacity"] = max(0, capacity - int(row.get("current_count") or 0))
    target_additions: dict[str, list[str]] = {eid: [] for eid in endpoint_ids}
    target_removals: dict[str, list[str]] = {eid: [] for eid in endpoint_ids}
    assignment_target: dict[str, str] = {}
    for key in sorted(final_managed):
        current_eid = _first_physical_endpoint(physical, key)
        previous_eid = str((assignments.get(key) or {}).get("endpoint_id") or "")
        if current_eid:
            assignment_target[key] = current_eid
        elif previous_eid in per_target:
            assignment_target[key] = previous_eid
    for key in sorted(remove_from_target):
        eid = _first_physical_endpoint(physical, key) or str((assignments.get(key) or {}).get("endpoint_id") or "")
        if eid in target_removals:
            target_removals[eid].append(key)
    planned_counts = {eid: int(per_target[eid].get("current_count") or 0) - len(target_removals[eid]) for eid in endpoint_ids}
    for key in source_keys:
        if key not in final_managed or key in target_set:
            continue
        eid = assignment_target.get(key)
        if eid in planned_counts and planned_counts[eid] < capacity:
            pass
        else:
            eid = ""
            for candidate in endpoint_ids:
                if planned_counts[candidate] < capacity:
                    eid = candidate
                    break
        if eid:
            assignment_target[key] = eid
            target_additions[eid].append(key)
            planned_counts[eid] += 1
    overflow_keys = [k for k in source_keys if k in final_managed and k not in target_set and k not in assignment_target]
    total_capacity = min(aggregate_capacity, capacity * len(endpoint_ids))
    final_target_logical_count = len((target_set | set(add_to_target) | add_to_source) - remove_from_target - remove_from_source)
    overflow_count = len(overflow_keys)
    if final_target_logical_count > total_capacity:
        overflow_count = max(overflow_count, final_target_logical_count - total_capacity)
    for eid in endpoint_ids:
        per_target[eid]["planned_additions"] = len(target_additions[eid])
        per_target[eid]["planned_removals"] = len(target_removals[eid])
    unmanaged = target_set - final_managed
    plan = {
        "ruleset_id": ruleset["id"],
        "ruleset": ruleset,
        "blocked": overflow_count > 0,
        "capacity_error": overflow_count > 0,
        "logical_source_count": len(source_set),
        "logical_aggregated_target_count": len(target_set),
        "source_count": len(source_set),
        "target_count": len(target_set),
        "managed_target_count": len(target_set & final_managed),
        "unmanaged_target_count": len(unmanaged),
        "total_capacity": total_capacity,
        "available_capacity": sum(int(per_target[eid]["available_capacity"]) for eid in endpoint_ids),
        "overflow_count": overflow_count,
        "planned_additions": sum(len(v) for v in target_additions.values()) + len(add_to_source),
        "planned_removals": sum(len(v) for v in target_removals.values()) + len(remove_from_source),
        "duplicates": agg["duplicates"],
        "unresolved": [],
        "unresolved_count": 0,
        "per_target": [per_target[eid] for eid in endpoint_ids],
        "warnings": warnings,
        "initial": not initialized,
        "membership": ruleset["membership"],
        "order": ruleset["order"],
    }
    ctx = {
        "source_ops": source_ops,
        "source_cfg": source_cfg,
        "source_inst": source_inst,
        "source_playlist_id": source.get("playlist_id"),
        "source_by_key": source_by_key,
        "target_by_key": target_by_key,
        "target_ctx": agg["target_ctx"],
        "physical": physical,
        "assignments": assignments,
        "assignment_target": assignment_target,
        "target_additions": target_additions,
        "target_removals": target_removals,
        "add_to_source": add_to_source,
        "remove_from_source": remove_from_source,
        "final_managed": final_managed,
        "final_source": final_source,
        "source_keys": source_keys,
        "target_keys": sorted(target_set),
        "ruleset": ruleset,
        "state_entry": entry,
        "endpoint_ids": endpoint_ids,
    }
    return plan, ctx


def preview_ruleset_mapping(
    cfg: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    providers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    plan, _ctx = _plan_ruleset(cfg, mapping, providers=providers)
    return plan


def build_plan(
    cfg: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    providers: Mapping[str, Any] | None = None,
) -> tuple[PlaylistPlan, dict[str, Any]]:
    providers = providers or _providers()
    source = mapping.get("source") or {}
    target = mapping.get("target") or {}
    src = str(source.get("provider") or "").strip().upper()
    dst = str(target.get("provider") or "").strip().upper()
    src_inst = normalize_instance_id(source.get("instance"))
    dst_inst = normalize_instance_id(target.get("instance"))
    src_pl = str(source.get("playlist_id") or "").strip()
    dst_pl = str(target.get("playlist_id") or "").strip()
    membership = _norm_membership_val(mapping.get("membership"))
    order = _norm_order_val(mapping.get("order"))

    plan = PlaylistPlan(membership=membership, order=order)

    src_ops = providers.get(src)
    dst_ops = providers.get(dst)
    if not src_ops or not supports_playlists(src_ops):
        raise PlaylistRunError(f"source provider {src or '?'} does not support playlists")
    if not dst_ops or not supports_playlists(dst_ops):
        raise PlaylistRunError(f"target provider {dst or '?'} does not support playlists")
    if not src_pl:
        raise PlaylistRunError("mapping source playlist id missing")
    if not dst_pl:
        raise PlaylistRunError("mapping target playlist id missing")

    src_cfg = build_provider_config_view(cfg, src, src_inst)
    dst_cfg = build_provider_config_view(cfg, dst, dst_inst)

    dst_resource = _find_resource(dst_ops, dst_cfg, dst_inst, dst_pl)
    if dst_resource is None:
        raise PlaylistRunError(f"target playlist not found: {dst_pl}")
    if dst_resource.is_smart:
        raise PlaylistRunError("target playlist is a smart playlist and cannot be written")
    if not (dst_resource.can_add or dst_resource.can_remove):
        raise PlaylistRunError("target playlist is read only")
    _merge_warnings(plan.warnings, dst_resource)

    src_snap: PlaylistSnapshot = src_ops.get_playlist_snapshot(src_cfg, src_pl, instance=src_inst)
    dst_snap: PlaylistSnapshot = dst_ops.get_playlist_snapshot(dst_cfg, dst_pl, instance=dst_inst)

    src_by_key = src_snap.by_key()
    dst_by_key = dst_snap.by_key()
    src_keys = list(src_snap.ordered_keys())
    src_set = set(src_by_key.keys())
    dst_set = set(dst_by_key.keys())
    src_aliases = _snapshot_key_union(src_by_key.values())
    dst_aliases = _snapshot_key_union(dst_by_key.values())
    baseline = load_baseline(mapping)

    plan.source_count = len(src_set)
    plan.target_count = len(dst_set)

    additions = [k for k in src_keys if not any_key_overlap(_playlist_item_keys(src_by_key[k]), dst_aliases)]

    removals: list[str] = []
    if membership == MEMBERSHIP_MANAGED_ONLY:
        removals = sorted(k for k in (baseline & dst_set) if k in dst_by_key and not any_key_overlap(_playlist_item_keys(dst_by_key[k]), src_aliases))
    elif membership == MEMBERSHIP_MIRROR:
        removals = sorted(k for k in dst_set if k in dst_by_key and not any_key_overlap(_playlist_item_keys(dst_by_key[k]), src_aliases))

    plan.additions = additions
    plan.removals = removals
    plan.resolved_count = len(additions)
    plan.manual_affected = bool(set(removals) - baseline)

    plan.add_items = [dict(src_by_key[k].item) for k in additions if k in src_by_key]
    plan.remove_items = [dict(dst_by_key[k].item) for k in removals if k in dst_by_key]

    if order == ORDER_PRESERVE:
        projected = (dst_set | set(additions)) - set(removals)
        target_order = [k for k in src_keys if k in projected]
        plan.target_order = target_order
        current_order = [k for k in dst_snap.ordered_keys() if k in projected]
        if not dst_resource.can_reorder:
            plan.warnings.append("target playlist does not support reordering")
        elif target_order != current_order:
            plan.reorder_count = len(target_order)

    ctx = {
        "src": src,
        "src_inst": src_inst,
        "dst": dst,
        "dst_inst": dst_inst,
        "src_pl": src_pl,
        "dst_pl": dst_pl,
        "src_ops": src_ops,
        "dst_ops": dst_ops,
        "src_cfg": src_cfg,
        "dst_cfg": dst_cfg,
        "dst_resource": dst_resource,
        "baseline": baseline,
        "dst_set": dst_set,
        "src_set": src_set,
        "src_keys": src_keys,
    }
    return plan, ctx


def preview_mapping(
    cfg: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    providers: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if mapping.get("ruleset_id"):
        return preview_ruleset_mapping(cfg, mapping, providers=providers)
    plan, _ctx = build_plan(cfg, mapping, providers=providers)
    return plan.to_dict()


def _item_dict_from_sources(key: str, *sources: Mapping[str, Any]) -> dict[str, Any]:
    for src in sources:
        val = src.get(key) if isinstance(src, Mapping) else None
        if isinstance(val, PlaylistItem):
            return dict(val.item or {})
        if isinstance(val, Mapping):
            item = val.get("item")
            return dict(item if isinstance(item, Mapping) else val)
    return {"key": key}


def _apply_ruleset_mapping(
    cfg: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    dry_run: bool = False,
    providers: Mapping[str, Any] | None = None,
    emit: Callable[..., None] | None = None,
) -> dict[str, Any]:
    plan, ctx = _plan_ruleset(cfg, mapping, providers=providers)
    mapping_id = str(mapping.get("id") or "")
    result: dict[str, Any] = {
        "ok": not bool(plan.get("blocked")),
        "mapping_id": mapping_id,
        "ruleset_id": plan.get("ruleset_id"),
        "dry_run": bool(dry_run),
        "capacity_error": bool(plan.get("capacity_error")),
        "overflow_count": int(plan.get("overflow_count") or 0),
        "planned_additions": int(plan.get("planned_additions") or 0),
        "planned_removals": int(plan.get("planned_removals") or 0),
        "added": 0,
        "removed": 0,
        "reordered": 0,
        "unresolved": [],
        "warnings": list(plan.get("warnings") or []),
        "per_target": list(plan.get("per_target") or []),
        "logical_source_count": int(plan.get("logical_source_count") or 0),
        "logical_aggregated_target_count": int(plan.get("logical_aggregated_target_count") or 0),
        "managed_target_count": int(plan.get("managed_target_count") or 0),
        "unmanaged_target_count": int(plan.get("unmanaged_target_count") or 0),
        "total_capacity": int(plan.get("total_capacity") or 0),
        "available_capacity": int(plan.get("available_capacity") or 0),
        "duplicates": list(plan.get("duplicates") or []),
    }
    if plan.get("blocked"):
        result["error"] = "capacity exceeded"
        result["unresolved_count"] = 0
        result["finished_at"] = int(time.time())
        store_result(mapping, result)
        _emit(emit, "playlist:capacity", mapping=mapping_id, ruleset=plan.get("ruleset_id"), overflow=result["overflow_count"])
        return result
    if dry_run:
        result["added"] = int(plan.get("planned_additions") or 0)
        result["removed"] = int(plan.get("planned_removals") or 0)
        _emit(emit, "playlist:dry_run", mapping=mapping_id, additions=result["added"], removals=result["removed"])
        return result
    source_by_key = ctx["source_by_key"]
    target_by_key = ctx["target_by_key"]
    assignments = ctx["assignments"]
    applied_ok = True
    for eid, keys in ctx["target_removals"].items():
        if not keys:
            continue
        tctx = ctx["target_ctx"][eid]
        items = [_item_dict_from_sources(k, target_by_key, assignments) for k in keys]
        res = tctx["ops"].remove_playlist_items(tctx["cfg"], tctx["playlist_id"], items, instance=tctx["instance"]) or {}
        result["removed"] += int(res.get("count") or 0)
        result["unresolved"].extend(res.get("unresolved") or [])
        _merge_warnings(result, res)
        if res.get("capacity"):
            result["capacity_error"] = True
        if res.get("ok") is False:
            applied_ok = False
        _record_events(_event_rows(mapping_id=mapping_id, ctx={"src": ctx["ruleset"]["id"], "dst": eid, "dst_inst": ""}, operation="remove", items=items))
    for keyset, op_name in ((ctx["remove_from_source"], "remove"), (ctx["add_to_source"], "add")):
        if not keyset:
            continue
        items = [_item_dict_from_sources(k, source_by_key, target_by_key, assignments) for k in sorted(keyset)]
        if op_name == "remove":
            res = ctx["source_ops"].remove_playlist_items(ctx["source_cfg"], ctx["source_playlist_id"], items, instance=ctx["source_inst"]) or {}
            result["removed"] += int(res.get("count") or 0)
        else:
            res = ctx["source_ops"].add_playlist_items(ctx["source_cfg"], ctx["source_playlist_id"], items, instance=ctx["source_inst"]) or {}
            result["added"] += int(res.get("count") or 0)
        result["unresolved"].extend(res.get("unresolved") or [])
        _merge_warnings(result, res)
        if res.get("capacity"):
            result["capacity_error"] = True
        if res.get("ok") is False:
            applied_ok = False
        _record_events(_event_rows(mapping_id=mapping_id, ctx={"src": ctx["ruleset"]["id"], "dst": "source", "dst_inst": ""}, operation=op_name, items=items))
    for eid, keys in ctx["target_additions"].items():
        if not keys:
            continue
        tctx = ctx["target_ctx"][eid]
        items = [_item_dict_from_sources(k, source_by_key, target_by_key, assignments) for k in keys]
        res = tctx["ops"].add_playlist_items(tctx["cfg"], tctx["playlist_id"], items, instance=tctx["instance"]) or {}
        result["added"] += int(res.get("count") or 0)
        result["unresolved"].extend(res.get("unresolved") or [])
        _merge_warnings(result, res)
        if res.get("capacity"):
            result["capacity_error"] = True
        if res.get("ok") is False:
            applied_ok = False
        _record_events(_event_rows(mapping_id=mapping_id, ctx={"src": ctx["ruleset"]["id"], "dst": eid, "dst_inst": ""}, operation="add", items=items))
    result["unresolved_count"] = len(result["unresolved"])
    if result["unresolved_count"] or result["capacity_error"] or not applied_ok:
        result["ok"] = False
        result["finished_at"] = int(time.time())
        store_result(mapping, result)
        return result
    now = int(time.time())
    final_target_keys: set[str] = set(ctx["target_keys"])
    for keys in ctx["target_additions"].values():
        final_target_keys.update(keys)
    for keys in ctx["target_removals"].values():
        final_target_keys.difference_update(keys)
    next_assignments: dict[str, dict[str, Any]] = {}
    for key in sorted(ctx["final_managed"]):
        eid = str(ctx["assignment_target"].get(key) or _first_physical_endpoint(ctx["physical"], key) or "")
        if not eid:
            continue
        item = _item_dict_from_sources(key, source_by_key, target_by_key, assignments)
        next_assignments[key] = {
            "endpoint_id": eid,
            "managed": True,
            "previous_logical_source": key in ctx["final_source"],
            "previous_logical_target": key in final_target_keys,
            "previous_physical_target": eid,
            "item": item,
            "last_success": now,
        }
    entry = dict(ctx["state_entry"])
    entry["assignments"] = next_assignments
    entry["managed"] = sorted(next_assignments)
    entry["last_success"] = {
        "finished_at": now,
        "source_keys": sorted(ctx["final_source"]),
        "target_keys": sorted(final_target_keys),
        "ruleset_id": plan.get("ruleset_id"),
    }
    result["managed_count"] = len(next_assignments)
    result["finished_at"] = now
    entry["last_result"] = dict(result)
    _save_state_entry(mapping, entry)
    _emit(
        emit,
        "playlist:done",
        mapping=mapping_id,
        ruleset=plan.get("ruleset_id"),
        targets=len(ctx["endpoint_ids"]),
        added=result["added"],
        removed=result["removed"],
    )
    return result


def _emit(emit: Callable[..., None] | None, event: str, **fields: Any) -> None:
    if emit is None:
        return
    try:
        emit(event, **fields)
    except Exception:
        pass


def _record_events(rows: Sequence[Mapping[str, Any]]) -> None:
    if not rows:
        return
    try:
        from .event_archive import record_events

        record_events(rows)
    except Exception:
        pass


def _event_rows(
    *,
    mapping_id: str,
    ctx: Mapping[str, Any],
    operation: str,
    items: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    from .id_map import canonical_key

    now = int(time.time())
    rows: list[dict[str, Any]] = []
    for it in items or []:
        mt = str(it.get("type") or "").lower()
        media_type = "episode" if mt == "episode" else ("movie" if mt in ("movie", "") else mt)
        rows.append(
            {
                "domain": "sync",
                "created_at": now,
                "event_type": f"playlist_{operation}",
                "severity": "info",
                "feature": "playlists",
                "operation": operation,
                "pair_key": mapping_id,
                "direction": "one_way",
                "source_provider": ctx.get("src"),
                "source_instance": ctx.get("src_inst"),
                "destination_provider": ctx.get("dst"),
                "destination_instance": ctx.get("dst_inst"),
                "item_key": canonical_key(it),
                "title": it.get("title"),
                "year": it.get("year"),
                "media_type": media_type,
                "season": it.get("season"),
                "episode": it.get("episode"),
                "detail": {"target_playlist": ctx.get("dst_pl"), "mapping": mapping_id},
            }
        )
    return rows


def _spotlight_items(items: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    from .id_map import canonical_key

    out: list[dict[str, Any]] = []
    for it in list(items or [])[:25]:
        out.append(
            {
                "key": canonical_key(it),
                "title": it.get("title") or it.get("name") or "",
                "type": it.get("type") or "",
                "year": it.get("year"),
                "season": it.get("season"),
                "episode": it.get("episode"),
            }
        )
    return out


def run_mapping(
    cfg: Mapping[str, Any],
    mapping: Mapping[str, Any],
    *,
    dry_run: bool = False,
    providers: Mapping[str, Any] | None = None,
    emit: Callable[..., None] | None = None,
) -> dict[str, Any]:
    if mapping.get("ruleset_id"):
        return _apply_ruleset_mapping(cfg, mapping, dry_run=dry_run, providers=providers, emit=emit)
    plan, ctx = build_plan(cfg, mapping, providers=providers)

    dst_ops = ctx["dst_ops"]
    dst_cfg = ctx["dst_cfg"]
    dst_inst = ctx["dst_inst"]
    dst_pl = ctx["dst_pl"]
    baseline: set[str] = set(ctx["baseline"])
    mapping_id = str(mapping.get("id") or "")

    result: dict[str, Any] = {
        "ok": True,
        "mapping_id": mapping_id,
        "dry_run": bool(dry_run),
        "membership": plan.membership,
        "order": plan.order,
        "planned_additions": len(plan.additions),
        "planned_removals": len(plan.removals),
        "added": 0,
        "removed": 0,
        "reordered": 0,
        "unresolved": [],
        "warnings": list(plan.warnings),
        "manual_affected": plan.manual_affected,
    }

    remove_items = list(plan.remove_items)
    runtime = cfg.get("runtime") or {}
    min_prev = int(runtime.get("suspect_min_prev", 20) or 20)
    if (
        plan.membership in (MEMBERSHIP_MANAGED_ONLY, MEMBERSHIP_MIRROR)
        and remove_items
        and len(ctx["dst_set"]) >= min_prev
    ):
        allow_mass_delete = bool(mapping.get("allow_mass_delete"))
        ratio = float(runtime.get("suspect_shrink_ratio", 0.10) or 0.10)
        try:
            from .orchestrator._pairs_massdelete import maybe_block_mass_delete

            guarded = maybe_block_mass_delete(
                remove_items,
                len(ctx["dst_set"]),
                allow_mass_delete=allow_mass_delete,
                suspect_ratio=ratio,
                emit=(lambda ev, **kw: _emit(emit, ev, **kw)),
                dbg=(lambda *a, **k: None),
                dst_name=str(ctx["dst"]),
                feature="playlists",
            )
        except Exception:
            guarded = remove_items
        if len(guarded) != len(remove_items):
            result["warnings"].append("mass_delete_blocked")
        remove_items = guarded

    applied_add_keys: set[str] = set()
    applied_remove_keys: set[str] = set()

    if dry_run:
        result["added"] = len(plan.add_items)
        result["removed"] = len(remove_items)
        result["reordered"] = plan.reorder_count
        _emit(emit, "playlist:dry_run", mapping=mapping_id, additions=result["added"], removals=result["removed"])
        return result

    from .id_map import canonical_key

    if plan.add_items:
        add_res = dst_ops.add_playlist_items(dst_cfg, dst_pl, plan.add_items, instance=dst_inst) or {}
        result["added"] = int(add_res.get("count") or 0)
        result["unresolved"].extend(add_res.get("unresolved") or [])
        _merge_warnings(result, add_res)
        confirmed = set(add_res.get("confirmed_keys") or [])
        if not confirmed and result["added"]:
            confirmed = {canonical_key(it) for it in plan.add_items}
        applied_add_keys = confirmed
        if add_res.get("capacity"):
            result["warnings"].append(f"capacity:{add_res.get('capacity')}")
        added_items = [it for it in plan.add_items if canonical_key(it) in confirmed]
        _record_events(_event_rows(mapping_id=mapping_id, ctx=ctx, operation="add", items=added_items))
        if result["added"]:
            _emit(
                emit,
                "apply:add:done",
                feature="playlists",
                mapping=mapping_id,
                count=result["added"],
                result={"count": result["added"], "confirmed_keys": list(confirmed)},
                spotlight=_spotlight_items(added_items),
            )

    if remove_items:
        rm_res = dst_ops.remove_playlist_items(dst_cfg, dst_pl, remove_items, instance=dst_inst) or {}
        result["removed"] = int(rm_res.get("count") or 0)
        result["unresolved"].extend(rm_res.get("unresolved") or [])
        _merge_warnings(result, rm_res)
        confirmed_rm = set(rm_res.get("confirmed_keys") or [])
        if not confirmed_rm and result["removed"]:
            confirmed_rm = {canonical_key(it) for it in remove_items}
        applied_remove_keys = confirmed_rm
        removed_items = [it for it in remove_items if canonical_key(it) in confirmed_rm]
        _record_events(_event_rows(mapping_id=mapping_id, ctx=ctx, operation="remove", items=removed_items))
        if result["removed"]:
            _emit(
                emit,
                "apply:remove:done",
                feature="playlists",
                mapping=mapping_id,
                count=result["removed"],
                result={"count": result["removed"], "confirmed_keys": list(confirmed_rm)},
                spotlight=_spotlight_items(removed_items),
            )

    if plan.order == ORDER_PRESERVE and plan.reorder_count and plan.target_order:
        try:
            dst_snap2 = dst_ops.get_playlist_snapshot(dst_cfg, dst_pl, instance=dst_inst)
            present = set(dst_snap2.by_key().keys())
        except Exception:
            present = set()
        target_order = [k for k in plan.target_order if not present or k in present]
        if ctx["dst_resource"].can_reorder and target_order:
            ro = dst_ops.reorder_playlist_items(dst_cfg, dst_pl, target_order, instance=dst_inst) or {}
            result["reordered"] = int(ro.get("reordered") or ro.get("count") or 0)
            if result["reordered"]:
                _emit(
                    emit,
                    "apply:update:done",
                    feature="playlists",
                    mapping=mapping_id,
                    count=result["reordered"],
                    result={"count": result["reordered"]},
                )

    new_managed = (baseline | applied_add_keys) - applied_remove_keys
    if plan.membership == MEMBERSHIP_MIRROR:
        new_managed = (ctx["src_set"] & ((ctx["dst_set"] | applied_add_keys) - applied_remove_keys))
    save_baseline(
        mapping,
        new_managed,
        meta={
            "membership": plan.membership,
            "order": plan.order,
            "last_added": result["added"],
            "last_removed": result["removed"],
        },
    )
    result["managed_count"] = len(new_managed)
    result["unresolved_count"] = len(result["unresolved"])
    result["finished_at"] = int(time.time())
    store_result(mapping, result)
    _emit(
        emit,
        "playlist:done",
        mapping=mapping_id,
        added=result["added"],
        removed=result["removed"],
        reordered=result["reordered"],
    )
    return result

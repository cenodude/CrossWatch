# /api/editorAPI.py
# CrossWatch - Tracker editor API for history / ratings / watchlist / progress
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Mapping, cast

import io
import json
import os
from pathlib import Path

from fastapi import APIRouter, Body, File, HTTPException, Query, Request, UploadFile
from fastapi.responses import StreamingResponse

from cw_platform.access_policy import managed_profile_instances, request_user, user_can_access_instance
from cw_platform.config_base import CONFIG as CONFIG_DIR, load_config
from cw_platform.history_events import history_sync_key, is_history_event_key, minimal_history_item
from cw_platform.id_map import canonical_key, merge_ids, minimal
from cw_platform.local_db import crosswatch_db_path, manual_policy as sqlite_manual_policy
from cw_platform.modules_registry import load_sync_ops, state_read_features, sync_provider_names
from cw_platform.orchestrator._applier import apply_add
from cw_platform.orchestrator._snapshots import module_checkpoint
from cw_platform.orchestrator._state_store import StateStore
from cw_platform.playlists import PlaylistSnapshot, supports_playlists
from cw_platform import playlists_runner
from cw_platform.provider_instances import (
    build_provider_config_view,
    get_provider_block,
    list_instance_ids,
    normalize_instance_id,
    sanitize_instance_label,
)
from services import playlists as playlist_svc

from services.editor import (
    Kind,
)

router = APIRouter(prefix="/api/editor", tags=["editor"])

_STATE_BASE = Path(CONFIG_DIR)


def _is_admin_request(request: Request | None) -> bool:
    user = request_user(request)
    return not user or bool(user.get("is_admin"))


def _require_instance_scope(cfg: Mapping[str, Any], request: Request | None, provider: Any, instance: Any) -> None:
    if not user_can_access_instance(cfg, request_user(request), provider, instance):
        raise HTTPException(status_code=403, detail="profile_scope_denied")


def _instance_for_request(cfg: Mapping[str, Any], request: Request | None, provider: Any, instance: Any) -> str:
    inst = normalize_instance_id(instance)
    if instance not in (None, ""):
        return inst
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return inst
    allowed = managed_profile_instances(cfg, user).get(str(provider or "").strip().upper()) or []
    return normalize_instance_id(allowed[0]) if allowed else inst


def _filter_provider_names_for_request(cfg: Mapping[str, Any], request: Request | None, providers: list[str]) -> list[str]:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return providers
    allowed = set(managed_profile_instances(cfg, user).keys())
    return [provider for provider in providers if str(provider or "").strip().upper() in allowed]


def _filter_targets_for_request(cfg: Mapping[str, Any], request: Request | None, targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    user = request_user(request)
    if not user or bool(user.get("is_admin")):
        return targets
    return [
        target for target in targets
        if user_can_access_instance(cfg, user, target.get("provider"), target.get("instance") or "default")
    ]


def _instance_owner_labels(cfg: Mapping[str, Any], provider: Any, instance: Any) -> list[str]:
    try:
        from cw_platform.provider_instances import user_profiles_for_instance

        rows = user_profiles_for_instance(cfg, provider, instance)
    except Exception:
        return []
    labels = [str(row.get("label") or "").strip() for row in rows if isinstance(row, Mapping)]
    return sorted({label for label in labels if label})


def _shared_instance_note(cfg: Mapping[str, Any], provider: Any, instance: Any) -> dict[str, Any]:
    owners = _instance_owner_labels(cfg, provider, instance)
    return {"owners": owners, "shared": len(owners) > 1}


def _provider_instance_label(cfg: Mapping[str, Any], provider: str, instance: Any) -> str:
    inst = normalize_instance_id(instance)
    block = get_provider_block(cfg, provider, inst)
    friendly = sanitize_instance_label(block.get("label") if isinstance(block, Mapping) else "")
    return friendly or ("Default" if inst == "default" else inst)

def _load_current_state_features(features: set[str] | list[str] | tuple[str, ...]) -> dict[str, Any]:
    try:
        raw = StateStore(_STATE_BASE).load_state_features(features)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read state: {e}")
    return raw if isinstance(raw, dict) else {}


def _state_exists() -> bool:
    try:
        return crosswatch_db_path(_STATE_BASE).exists()
    except Exception:
        return False


def _state_mtime() -> int | None:
    try:
        p = crosswatch_db_path(_STATE_BASE)
        if p.exists():
            return int(p.stat().st_mtime)
    except Exception:
        pass
    return None


def _load_policy() -> dict[str, Any]:
    try:
        raw = sqlite_manual_policy.load_policy(_STATE_BASE)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read policy: {e}")
    if not isinstance(raw, dict):
        return {"version": 1, "providers": {}}
    prov = raw.get("providers")
    if not isinstance(prov, dict):
        raw["providers"] = {}
    if "version" not in raw:
        raw["version"] = 1
    return raw


def _save_policy(raw: dict[str, Any]) -> None:
    try:
        sqlite_manual_policy.save_policy(_STATE_BASE, raw)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write policy: {e}")


def _policy_exists() -> bool:
    try:
        return sqlite_manual_policy.has_policy(_STATE_BASE)
    except Exception:
        return False


def _policy_mtime() -> int | None:
    try:
        return sqlite_manual_policy.policy_mtime(_STATE_BASE)
    except Exception:
        return None


def _policy_providers(raw: dict[str, Any]) -> list[str]:
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return []
    return sorted([str(k) for k in providers.keys() if str(k).strip()])


def _union_providers(state_raw: dict[str, Any], policy_raw: dict[str, Any]) -> list[str]:
    a = _state_providers(state_raw)
    b = _policy_providers(policy_raw)
    seen: set[str] = set()
    out: list[str] = []
    for x in a + b:
        s = str(x).strip()
        if not s:
            continue
        sl = s.lower()
        if sl in seen:
            continue
        seen.add(sl)
        out.append(s)
    return out


def _merge_blocks(a: list[str], b: list[str]) -> list[str]:
    seen: set[str] = set()
    out: list[str] = []
    for x in (a or []) + (b or []):
        s = str(x).strip()
        if not s:
            continue
        sl = s.lower()
        if sl in seen:
            continue
        seen.add(sl)
        out.append(s)
    return out



def _policy_provider_node(
    raw: dict[str, Any],
    provider: str,
    provider_instance: str | None = None,
) -> dict[str, Any] | None:
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return None

    node = providers.get(provider)
    if not isinstance(node, dict):
        pl = str(provider).lower()
        for k, v in providers.items():
            if str(k).lower() == pl and isinstance(v, dict):
                node = v
                break
    if not isinstance(node, dict):
        return None

    inst = normalize_instance_id(provider_instance)
    if inst != "default":
        insts = node.get("instances") or {}
        if not isinstance(insts, dict):
            return None
        node = insts.get(inst)
        if not isinstance(node, dict):
            return None
    return node


def _load_policy_manual(
    kind: Kind,
    provider: str,
    provider_instance: str | None = None,
    raw_policy: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    raw = raw_policy if isinstance(raw_policy, dict) else _load_policy()
    node = _policy_provider_node(raw, provider, provider_instance)
    if not isinstance(node, dict):
        return {}, []

    f = node.get(kind) or {}
    if not isinstance(f, dict):
        return {}, []

    blocks_raw = f.get("blocks") or []
    blocks: list[str] = []
    seen: set[str] = set()
    if isinstance(blocks_raw, (list, tuple, set)):
        for x in blocks_raw:
            s = str(x).strip()
            if not s:
                continue
            sl = s.lower()
            if sl in seen:
                continue
            seen.add(sl)
            blocks.append(s)
    elif isinstance(blocks_raw, dict):
        for k in blocks_raw.keys():
            s = str(k).strip()
            if not s:
                continue
            sl = s.lower()
            if sl in seen:
                continue
            seen.add(sl)
            blocks.append(s)

    adds_raw = f.get("adds") or {}
    adds_items: dict[str, Any] = {}
    if isinstance(adds_raw, dict):
        items = adds_raw.get("items") or {}
        if isinstance(items, dict):
            adds_items = {str(k): v for k, v in items.items()}
    return adds_items, blocks


def _save_policy_manual(
    kind: Kind,
    provider: str,
    adds_items: dict[str, Any],
    blocks: list[str],
    provider_instance: str | None = None,
) -> None:
    adds_items = _canonicalize_manual_items(adds_items, kind)

    def _mutate(raw: dict[str, Any]) -> None:
        providers = raw.get("providers")
        if not isinstance(providers, dict):
            providers = {}
            raw["providers"] = providers

        key = None
        if provider in providers:
            key = provider
        else:
            pl = str(provider).lower()
            for k in providers.keys():
                if str(k).lower() == pl:
                    key = str(k)
                    break
        if key is None:
            key = provider
            providers[key] = {}

        node = providers.get(key)
        if not isinstance(node, dict):
            node = {}
            providers[key] = node

        inst = normalize_instance_id(provider_instance)
        if inst != "default":
            insts = node.get("instances")
            if not isinstance(insts, dict):
                insts = {}
                node["instances"] = insts
            in_node = insts.get(inst)
            if not isinstance(in_node, dict):
                in_node = {}
                insts[inst] = in_node
            node = in_node

        f = node.get(kind)
        if not isinstance(f, dict):
            f = {}
            node[kind] = f

        f["blocks"] = list(blocks or [])

        adds = f.get("adds")
        if not isinstance(adds, dict):
            adds = {}
            f["adds"] = adds
        adds["items"] = dict(adds_items or {})


    try:
        sqlite_manual_policy.update_policy(_STATE_BASE, _mutate)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write policy: {e}")


def _policy_from_state() -> dict[str, Any]:
    return {"version": 1, "providers": {}}


def _merge_policy(into: dict[str, Any], src: dict[str, Any], mode: str) -> dict[str, Any]:
    if mode == "replace":
        base = {"version": 1, "providers": {}}
        prov = src.get("providers") if isinstance(src, dict) else None
        base["providers"] = prov if isinstance(prov, dict) else {}
        return base

    out = into if isinstance(into, dict) else {"version": 1, "providers": {}}
    if "version" not in out:
        out["version"] = 1
    prov_out = out.get("providers")
    if not isinstance(prov_out, dict):
        prov_out = {}
        out["providers"] = prov_out

    prov_in = src.get("providers") if isinstance(src, dict) else None
    if not isinstance(prov_in, dict):
        return out

    def _merge_feature_block(tgt: dict[str, Any], node: dict[str, Any]) -> None:
        for kind in ("watchlist", "history", "ratings", "progress"):
            f = node.get(kind)
            if not isinstance(f, dict):
                continue
            t = tgt.get(kind)
            if not isinstance(t, dict):
                t = {}
                tgt[kind] = t

            blocks_in = f.get("blocks") or []
            blocks_in_list: list[str] = []
            if isinstance(blocks_in, (list, tuple, set)):
                blocks_in_list = [str(x) for x in blocks_in]
            elif isinstance(blocks_in, dict):
                blocks_in_list = [str(x) for x in blocks_in.keys()]

            blocks_out = t.get("blocks") or []
            blocks_out_list: list[str] = []
            if isinstance(blocks_out, (list, tuple, set)):
                blocks_out_list = [str(x) for x in blocks_out]
            elif isinstance(blocks_out, dict):
                blocks_out_list = [str(x) for x in blocks_out.keys()]

            t["blocks"] = _merge_blocks(blocks_out_list, blocks_in_list)

            adds_in = f.get("adds") or {}
            items_in = adds_in.get("items") if isinstance(adds_in, dict) else None
            if isinstance(items_in, dict):
                adds_out = t.get("adds")
                if not isinstance(adds_out, dict):
                    adds_out = {}
                    t["adds"] = adds_out
                items_out = adds_out.get("items")
                if not isinstance(items_out, dict):
                    items_out = {}
                merged = _canonicalize_manual_items(dict(items_out), kind)
                for mk, mv in _canonicalize_manual_items({str(k): v for k, v in items_in.items()}, kind).items():
                    merged[mk] = _merge_manual_item(merged.get(mk), mv)
                adds_out["items"] = merged

    for p, node in prov_in.items():
        if not isinstance(node, dict):
            continue
        target = prov_out.get(p)
        if not isinstance(target, dict):
            target = {}
            prov_out[p] = target

        _merge_feature_block(target, node)

        insts_in = node.get("instances") or {}
        if not isinstance(insts_in, dict):
            continue

        insts_out = target.get("instances")
        if not isinstance(insts_out, dict):
            insts_out = {}
            target["instances"] = insts_out

        for inst_id, inst_node in insts_in.items():
            if not isinstance(inst_node, dict):
                continue
            tgt_inst = insts_out.get(inst_id)
            if not isinstance(tgt_inst, dict):
                tgt_inst = {}
                insts_out[inst_id] = tgt_inst
            _merge_feature_block(tgt_inst, inst_node)

    return out


def _mirror_policy_into_state(
    raw_state: dict[str, Any] | None = None,
    raw_policy: dict[str, Any] | None = None,
) -> dict[str, Any] | None:
    return raw_state if isinstance(raw_state, dict) else None

def _policy_stats(pol: dict[str, Any]) -> dict[str, int]:
    prov = pol.get("providers") or {}
    if not isinstance(prov, dict):
        return {"providers": 0, "blocks": 0, "adds": 0}
    pcount = 0
    bcount = 0
    acount = 0
    for _, node in prov.items():
        if not isinstance(node, dict):
            continue
        pcount += 1
        for kind in ("watchlist", "history", "ratings", "progress"):
            f = node.get(kind)
            if not isinstance(f, dict):
                continue
            blocks = f.get("blocks") or []
            if isinstance(blocks, (list, tuple, set)):
                bcount += len(list(blocks))
            elif isinstance(blocks, dict):
                bcount += len(list(blocks.keys()))
            adds = f.get("adds") or {}
            if isinstance(adds, dict):
                items = adds.get("items") or {}
                if isinstance(items, dict):
                    acount += len(items)
    return {"providers": pcount, "blocks": bcount, "adds": acount}

def _always_listed_providers() -> list[str]:
    out: list[str] = []
    try:
        cfg = load_config() or {}
    except Exception:
        return out
    for name in ("CROSSWATCH",):
        ops = load_sync_ops(name)
        if not ops or not hasattr(ops, "is_configured"):
            continue
        try:
            instances = list_instance_ids(cfg, name) or ["default"]
        except Exception:
            instances = ["default"]
        for inst in instances:
            try:
                if ops.is_configured(build_provider_config_view(cfg, name, inst)):
                    out.append(name)
                    break
            except Exception:
                continue
    return out


def _state_providers(raw: dict[str, Any]) -> list[str]:
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return []
    return sorted([str(k) for k in providers.keys() if str(k).strip()])


def _state_provider_node(
    raw: dict[str, Any],
    provider: str,
    provider_instance: str | None = None,
) -> dict[str, Any] | None:
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return None

    node = providers.get(provider)
    if not isinstance(node, dict):
        pl = str(provider).lower()
        for k, v in providers.items():
            if str(k).lower() == pl and isinstance(v, dict):
                node = v
                break
    if not isinstance(node, dict):
        return None

    inst = normalize_instance_id(provider_instance)
    if inst != "default":
        insts = node.get("instances") or {}
        if not isinstance(insts, dict):
            return None
        node = insts.get(inst)
        if not isinstance(node, dict):
            return None
    return node


def _load_state_items(
    kind: Kind,
    provider: str,
    provider_instance: str | None = None,
    raw_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    raw = raw_state if isinstance(raw_state, dict) else _load_current_state_features({kind})
    node = _state_provider_node(raw, provider, provider_instance)
    items: Any = None
    if isinstance(node, dict):
        feature = node.get(kind)
        if isinstance(feature, dict):
            baseline = feature.get("baseline")
            if isinstance(baseline, dict):
                items = baseline.get("items")
    if isinstance(items, dict) and items:
        return items
    return _load_tracker_items(kind, provider, provider_instance)


def _load_tracker_items(kind: Kind, provider: str, provider_instance: str | None = None) -> dict[str, Any]:
    if str(provider or "").strip().upper() != "CROSSWATCH":
        return {}
    ops = load_sync_ops("CROSSWATCH")
    if not ops:
        return {}
    try:
        cfg = load_config() or {}
        view = dict(build_provider_config_view(cfg, "CROSSWATCH", normalize_instance_id(provider_instance)))
        view["_cw_readonly"] = True
        if kind == "history":
            view["_cw_history_rewatches"] = True
        idx = ops.build_index(view, feature=kind) or {}
    except Exception:
        return {}
    return {str(key): dict(value) for key, value in idx.items() if isinstance(value, Mapping)}


def _save_state_items(kind: Kind, provider: str, items: dict[str, Any], provider_instance: str | None = None) -> None:
    StateStore(_STATE_BASE).save_feature_baseline(
        provider=provider,
        instance=normalize_instance_id(provider_instance),
        feature=kind,
        items=dict(items or {}),
    )


def _load_state_manual(
    kind: Kind,
    provider: str,
    provider_instance: str | None = None,
    raw_state: dict[str, Any] | None = None,
) -> tuple[dict[str, Any], list[str]]:
    raw = raw_state if isinstance(raw_state, dict) else _load_current_state_features({kind})
    node = _state_provider_node(raw, provider, provider_instance)
    if not isinstance(node, dict):
        return {}, []

    manual = node.get("manual") or {}
    if not isinstance(manual, dict):
        return {}, []
    f = manual.get(kind) or {}
    if not isinstance(f, dict):
        return {}, []

    blocks_raw = f.get("blocks") or []
    blocks: list[str] = []
    seen: set[str] = set()
    if isinstance(blocks_raw, (list, tuple, set)):
        for x in blocks_raw:
            s = str(x).strip()
            if not s:
                continue
            sl = s.lower()
            if sl in seen:
                continue
            seen.add(sl)
            blocks.append(s)
    elif isinstance(blocks_raw, dict):
        for k in blocks_raw.keys():
            s = str(k).strip()
            if not s:
                continue
            sl = s.lower()
            if sl in seen:
                continue
            seen.add(sl)
            blocks.append(s)

    adds_raw = f.get("adds") or {}
    adds_items: dict[str, Any] = {}
    if isinstance(adds_raw, dict):
        items = adds_raw.get("items") or {}
        if isinstance(items, dict):
            adds_items = {str(k): v for k, v in items.items()}

    return adds_items, blocks


def _save_state_manual(
    kind: Kind,
    provider: str,
    adds_items: dict[str, Any],
    blocks: list[str],
    provider_instance: str | None = None,
) -> None:
    _save_policy_manual(kind, provider, adds_items, blocks, provider_instance)

def _normalize_kind(val: str | None) -> Kind:
    k = (val or "watchlist").strip().lower()
    if k not in ("watchlist", "history", "ratings", "progress"):
        raise HTTPException(status_code=400, detail=f"Unsupported kind: {k}")
    return k  # type: ignore[return-value]


def _playlist_endpoint(cfg: dict[str, Any], endpoint_id: str) -> dict[str, Any]:
    eid = str(endpoint_id or "").strip()
    if not eid:
        raise HTTPException(status_code=400, detail="Missing playlist endpoint")
    ep = playlists_runner.get_endpoint(cfg, eid)
    if not ep:
        raise HTTPException(status_code=404, detail="Playlist endpoint not found")
    provider = str(ep.get("provider") or "").strip().upper()
    ops = load_sync_ops(provider)
    if not ops or not supports_playlists(ops):
        raise HTTPException(status_code=400, detail=f"Provider not playlist-capable: {provider or '?'}")
    playlist_id = str(ep.get("playlist_id") or "").strip()
    if not playlist_id:
        raise HTTPException(status_code=400, detail="Playlist endpoint has no provider playlist id")
    inst = normalize_instance_id(ep.get("instance"))
    view = build_provider_config_view(cfg, provider, inst)
    return {"endpoint": dict(ep), "provider": provider, "instance": inst, "playlist_id": playlist_id, "ops": ops, "view": view}


def _playlist_endpoint_rows(snap: PlaylistSnapshot) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for item in snap.items or []:
        media = dict(item.item or {})
        try:
            media = minimal(media)
        except Exception:
            pass
        if item.playlist_item_id:
            media["_playlist_item_id"] = item.playlist_item_id
        if item.provider_media_id:
            media["_provider_media_id"] = item.provider_media_id
        if item.position is not None:
            media["_position"] = item.position
        key = str(item.key or canonical_key(media) or "").strip()
        if key:
            out[key] = media
    return out


def _playlist_resource_meta(resource: Any) -> dict[str, Any]:
    extra = getattr(resource, "extra", None)
    extra = extra if isinstance(extra, dict) else {}
    warnings = [str(w) for w in (extra.get("warnings") or []) if str(w).strip()]
    if extra.get("remove_warning"):
        warnings.append(str(extra.get("remove_warning")))
    seen: set[str] = set()
    warnings = [w for w in warnings if not (w in seen or seen.add(w))]
    return {
        "id": getattr(resource, "id", ""),
        "name": getattr(resource, "name", ""),
        "provider": getattr(resource, "provider", ""),
        "instance": getattr(resource, "instance", "default"),
        "kind": getattr(resource, "kind", ""),
        "can_read": bool(getattr(resource, "can_read", False)),
        "can_add": bool(getattr(resource, "can_add", False)),
        "can_remove": bool(getattr(resource, "can_remove", False)),
        "can_reorder": bool(getattr(resource, "can_reorder", False)),
        "smart": bool(getattr(resource, "is_smart", False)),
        "media_types": list(getattr(resource, "media_types", ()) or []),
        "warnings": warnings,
        "extra": dict(extra),
    }


def _playlist_clean_items(items_raw: Any) -> dict[str, dict[str, Any]]:
    items = _normalize_items(items_raw)
    out: dict[str, dict[str, Any]] = {}
    for key, raw in items.items():
        item = dict(raw) if isinstance(raw, dict) else {}
        try:
            clean = minimal(item)
        except Exception:
            clean = item
        final_key = str(canonical_key(clean) or key or "").strip()
        if final_key:
            out[final_key] = clean
    return out


def _merge_result_warnings(result: dict[str, Any], source: Any) -> None:
    raw = source.get("warnings") if isinstance(source, dict) else []
    values = [raw] if isinstance(raw, str) else list(raw or []) if isinstance(raw, list) else []
    warnings = result.setdefault("warnings", [])
    seen = {str(w) for w in warnings}
    for value in values:
        text = str(value or "").strip()
        if text and text not in seen:
            warnings.append(text)
            seen.add(text)


@router.get("/playlists/endpoints")
def api_editor_playlist_endpoints(request: Request = cast(Request, None)) -> dict[str, Any]:
    cfg = load_config() or {}
    endpoints = _filter_targets_for_request(cfg, request, playlist_svc.list_endpoints(cfg))
    return {"ok": True, "endpoints": endpoints}


@router.get("/playlists/{endpoint_id}")
def api_editor_playlist_endpoint(endpoint_id: str, request: Request = cast(Request, None)) -> dict[str, Any]:
    cfg = load_config() or {}
    ctx = _playlist_endpoint(cfg, endpoint_id)
    _require_instance_scope(cfg, request, ctx["provider"], ctx["instance"])
    snap = ctx["ops"].get_playlist_snapshot(ctx["view"], ctx["playlist_id"], instance=ctx["instance"])
    if not isinstance(snap, PlaylistSnapshot):
        raise HTTPException(status_code=500, detail="Provider returned an invalid playlist snapshot")
    items = _playlist_endpoint_rows(snap)
    resource = _playlist_resource_meta(snap.resource)
    return {
        "ok": True,
        "kind": "playlist",
        "source": "playlist",
        "endpoint": ctx["endpoint"],
        "resource": resource,
        "ts": None,
        "checkpoint": snap.checkpoint,
        "count": len(items),
        "items": items,
        "original_keys": list(items.keys()),
    }


@router.post("/playlists/{endpoint_id}")
def api_editor_playlist_endpoint_save(endpoint_id: str, payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> dict[str, Any]:
    cfg = load_config() or {}
    ctx = _playlist_endpoint(cfg, endpoint_id)
    _require_instance_scope(cfg, request, ctx["provider"], ctx["instance"])
    snap = ctx["ops"].get_playlist_snapshot(ctx["view"], ctx["playlist_id"], instance=ctx["instance"])
    if not isinstance(snap, PlaylistSnapshot):
        raise HTTPException(status_code=500, detail="Provider returned an invalid playlist snapshot")
    resource = snap.resource
    if resource.is_smart:
        raise HTTPException(status_code=400, detail="Smart or read-only playlist endpoints cannot be edited")
    submitted = _playlist_clean_items(payload.get("items"))
    current = snap.by_key()
    current_keys = set(current.keys())
    desired_keys = list(submitted.keys())
    desired_set = set(desired_keys)
    add_keys = [k for k in desired_keys if k not in current_keys]
    remove_keys = [k for k in current_keys if k not in desired_set]
    if add_keys and not resource.can_add:
        raise HTTPException(status_code=400, detail="Playlist endpoint does not support adding items")
    if remove_keys and not resource.can_remove:
        raise HTTPException(status_code=400, detail="Playlist endpoint does not support removing items")
    result: dict[str, Any] = {
        "ok": True,
        "source": "playlist",
        "endpoint_id": endpoint_id,
        "planned_additions": len(add_keys),
        "planned_removals": len(remove_keys),
        "added": 0,
        "removed": 0,
        "reordered": 0,
        "unresolved": [],
        "warnings": list(_playlist_resource_meta(resource).get("warnings") or []),
    }
    if add_keys:
        add_res = ctx["ops"].add_playlist_items(ctx["view"], ctx["playlist_id"], [submitted[k] for k in add_keys], instance=ctx["instance"]) or {}
        result["added"] = int(add_res.get("count") or 0)
        result["unresolved"].extend(add_res.get("unresolved") or [])
        _merge_result_warnings(result, add_res)
    if remove_keys:
        remove_res = ctx["ops"].remove_playlist_items(ctx["view"], ctx["playlist_id"], [dict(current[k].item or {}) for k in remove_keys if k in current], instance=ctx["instance"]) or {}
        result["removed"] = int(remove_res.get("count") or 0)
        result["unresolved"].extend(remove_res.get("unresolved") or [])
        _merge_result_warnings(result, remove_res)
    final_order = [k for k in desired_keys if k in desired_set]
    current_order = [k for k in snap.ordered_keys() if k in desired_set]
    if resource.can_reorder and final_order and final_order != current_order:
        reorder_res = ctx["ops"].reorder_playlist_items(ctx["view"], ctx["playlist_id"], final_order, instance=ctx["instance"]) or {}
        result["reordered"] = int(reorder_res.get("reordered") or reorder_res.get("count") or 0)
        _merge_result_warnings(result, reorder_res)
    result["unresolved_count"] = len(result["unresolved"])
    result["ok"] = result["unresolved_count"] == 0
    return result

def _normalize_blocks(blocks_raw: Any) -> list[str]:
    keys: Any
    if isinstance(blocks_raw, dict):
        keys = blocks_raw.keys()
    elif isinstance(blocks_raw, (list, tuple, set)):
        keys = blocks_raw
    else:
        return []
    blocks: list[str] = []
    seen: set[str] = set()
    for x in keys:
        s = str(x).strip()
        if not s:
            continue
        sl = s.lower()
        if sl in seen:
            continue
        seen.add(sl)
        blocks.append(s)
    return blocks


@router.get("/state/providers")
def api_editor_state_providers(request: Request = cast(Request, None)) -> dict[str, Any]:
    cfg = load_config() or {}
    state_providers = StateStore(_STATE_BASE).provider_names()
    raw_policy = _load_policy()
    providers = _union_providers({"providers": {name: {} for name in state_providers}}, raw_policy)
    known = {str(name).strip().lower() for name in providers}
    for name in _always_listed_providers():
        if name.lower() not in known:
            providers.append(name)
            known.add(name.lower())
    providers = _filter_provider_names_for_request(cfg, request, providers)
    return {"providers": providers}


@router.get("")
def api_editor_get_state(
    kind: str = "watchlist",
    snapshot: str | None = None,
    source: str = "state",
    provider: str | None = None,
    provider_instance: str | None = None,
    endpoint: str | None = None,
    request: Request = cast(Request, None),
) -> dict[str, Any]:
    k = _normalize_kind(kind)
    src = (source or "state").strip().lower()
    if src in ("playlist", "playlists", "playlist-endpoint"):
        return api_editor_playlist_endpoint((endpoint or snapshot or "").strip(), request=request)

    cfg = load_config() or {}
    if src in ("state", "current"):
        raw_state = _load_current_state_features({k})
        raw_policy = _load_policy()
        providers = _union_providers(raw_state, raw_policy)
        providers = _filter_provider_names_for_request(cfg, request, providers)
        chosen = (provider or "").strip() or (providers[0] if providers else "")
        if not chosen:
            return {
                "kind": k,
                "source": "state",
                "snapshot": None,
                "provider": None,
                "provider_instance": None,
                "ts": None,
                "count": 0,
                "items": {},
                "manual_adds": {},
                "manual_blocks": [],
            }

        inst = _instance_for_request(cfg, request, chosen, provider_instance)
        _require_instance_scope(cfg, request, chosen, inst)

        items = _load_state_items(k, chosen, inst, raw_state=raw_state)
        st_adds, st_blocks = _load_state_manual(k, chosen, inst, raw_state=raw_state) if raw_state else ({}, [])
        pol_adds, pol_blocks = _load_policy_manual(k, chosen, inst, raw_policy=raw_policy)

        manual_adds = dict(st_adds or {})
        manual_adds.update(dict(pol_adds or {}))
        manual_blocks = _merge_blocks(st_blocks or [], pol_blocks or [])

        ts = None
        try:
            if _state_exists():
                ts = _state_mtime()
            elif _policy_exists():
                ts = _policy_mtime()
        except Exception:
            ts = None
        return {
            "kind": k,
            "source": "state",
            "snapshot": None,
            "provider": chosen,
            "provider_instance": inst,
            "ts": ts,
            "count": len(items),
            "items": items,
            "manual_adds": manual_adds,
            "manual_blocks": manual_blocks,
            "instance_sharing": _shared_instance_note(cfg, chosen, inst),
        }
    if src in ("manual", "manual-overrides", "policy", "overrides"):
        raw_state = _load_current_state_features({k})
        raw_policy = _load_policy()
        providers = _union_providers(raw_state, raw_policy)
        providers = _filter_provider_names_for_request(cfg, request, providers)
        chosen = (provider or "").strip() or (providers[0] if providers else "")
        if not chosen:
            return {
                "kind": k,
                "source": "manual",
                "snapshot": None,
                "provider": None,
                "provider_instance": None,
                "ts": None,
                "count": 0,
                "items": {},
                "manual_adds": {},
                "manual_blocks": [],
            }

        inst = _instance_for_request(cfg, request, chosen, provider_instance)
        _require_instance_scope(cfg, request, chosen, inst)
        pol_adds, pol_blocks = _load_policy_manual(k, chosen, inst, raw_policy=raw_policy)
        if not pol_adds and not pol_blocks and raw_state:
            pol_adds, pol_blocks = _load_state_manual(k, chosen, inst, raw_state=raw_state)

        ts = None
        try:
            if _policy_exists():
                ts = _policy_mtime()
            elif _state_exists():
                ts = _state_mtime()
        except Exception:
            ts = None
        manual_adds = dict(pol_adds or {})
        manual_blocks = _normalize_blocks(pol_blocks or [])
        return {
            "kind": k,
            "source": "manual",
            "snapshot": None,
            "provider": chosen,
            "provider_instance": inst,
            "ts": ts,
            "count": len(manual_adds),
            "items": manual_adds,
            "manual_adds": manual_adds,
            "manual_blocks": manual_blocks,
            "instance_sharing": _shared_instance_note(cfg, chosen, inst),
        }
    raise HTTPException(status_code=400, detail=f"Unsupported source: {src}")

def _normalize_items(items: Any) -> dict[str, Any]:
    if isinstance(items, dict):
        return {str(k): v for k, v in items.items()}
    if isinstance(items, list):
        out: dict[str, Any] = {}
        for row in items:
            if not isinstance(row, dict):
                continue
            key = str(row.get("key") or "").strip()
            if not key:
                continue
            payload = {k: v for k, v in row.items() if k != "key"}
            out[key] = payload
        return out
    return {}


def _merge_manual_item(existing: Any, incoming: Any) -> dict[str, Any]:
    base = dict(existing) if isinstance(existing, dict) else {}
    nxt = dict(incoming) if isinstance(incoming, dict) else {}
    out = dict(base)
    for k, v in nxt.items():
        if k == "ids":
            continue
        if k not in out or out[k] in (None, "", [], {}):
            out[k] = v
    ids_existing = base.get("ids") if isinstance(base.get("ids"), dict) else {}
    ids_incoming = nxt.get("ids") if isinstance(nxt.get("ids"), dict) else {}
    ids_merged = merge_ids(ids_existing, ids_incoming)
    if ids_merged:
        out["ids"] = ids_merged
    return out


def _history_item_is_event(key: str, item: Mapping[str, Any]) -> bool:
    return is_history_event_key(key) or bool(item.get("_cw_rewatch_sync") is True or item.get("_cw_event_key"))


def _editor_item_key(feature: str, key: str, item: Mapping[str, Any]) -> str:
    if feature == "history" and _history_item_is_event(key, item):
        return history_sync_key(item, key, event_mode=True)
    try:
        return str(canonical_key(item) or key).strip().lower()
    except Exception:
        return str(key or "").strip().lower()


def _editor_minimal_item(feature: str, key: str, item: Mapping[str, Any]) -> dict[str, Any]:
    if feature == "history" and _history_item_is_event(key, item):
        return minimal_history_item(item, key, event_mode=True)
    return minimal(item)


def _canonicalize_manual_items(items: dict[str, Any], feature: str) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for raw_key, raw_item in (items or {}).items():
        key = str(raw_key or "").strip()
        item = dict(raw_item) if isinstance(raw_item, dict) else {}
        final_key = _editor_item_key(feature, key, item)
        if not final_key:
            continue
        if final_key in out:
            out[final_key] = _merge_manual_item(out[final_key], item)
        else:
            out[final_key] = item
    return out


@router.post("")
def api_editor_save_state(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> dict[str, Any]:
    kind = _normalize_kind(str(payload.get("kind") or "watchlist"))
    src = str(payload.get("source") or "state").strip().lower()
    items_raw = payload.get("items")
    items = _normalize_items(items_raw)
    if src in ("playlist", "playlists", "playlist-endpoint"):
        endpoint_id = str(payload.get("endpoint") or payload.get("snapshot") or "").strip()
        return api_editor_playlist_endpoint_save(endpoint_id, payload, request=request)

    if src in ("state", "current", "manual", "manual-overrides", "policy", "overrides"):
        cfg = load_config() or {}
        provider = str(payload.get("provider") or "").strip()
        if not provider:
            raise HTTPException(status_code=400, detail=f"Missing provider for source={src}")
        items = _canonicalize_manual_items(items, kind)

        inst = _instance_for_request(cfg, request, provider, payload.get("provider_instance"))
        _require_instance_scope(cfg, request, provider, inst)

        blocks = _normalize_blocks(payload.get("blocks"))

        _save_policy_manual(kind, provider, items, blocks, inst)
        ts = None
        try:
            ts = _policy_mtime()
        except Exception:
            ts = None
        return {
            "ok": True,
            "kind": kind,
            "source": "manual" if src not in ("state", "current") else "state",
            "provider": provider,
            "provider_instance": inst,
            "count": len(items),
            "blocks": len(blocks),
            "ts": ts,
        }
    raise HTTPException(status_code=400, detail=f"Unsupported source: {src}")

@router.get("/state/manual/export")
def api_editor_state_manual_export(request: Request = cast(Request, None)) -> StreamingResponse:
    if not _is_admin_request(request):
        raise HTTPException(status_code=403, detail="profile_scope_denied")
    pol = _load_policy()
    data = json.dumps(pol, ensure_ascii=False, sort_keys=True).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/json",
        headers={"Content-Disposition": "attachment; filename=crosswatch-state-policy.json"},
    )


@router.post("/state/manual/import")
async def api_editor_state_manual_import(
    mode: str = Query("merge"),
    file: UploadFile = File(...),
    request: Request = cast(Request, None),
) -> dict[str, Any]:
    if not _is_admin_request(request):
        raise HTTPException(status_code=403, detail="profile_scope_denied")
    payload = await file.read()
    try:
        incoming = json.loads(payload.decode("utf-8"))
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON")
    if not isinstance(incoming, dict) or not isinstance(incoming.get("providers"), dict):
        raise HTTPException(status_code=400, detail="Invalid policy format")

    mode_n = str(mode or "merge").strip().lower()
    if mode_n not in ("merge", "replace"):
        raise HTTPException(status_code=400, detail="Invalid mode")

    def _mutate(raw: dict[str, Any]) -> dict[str, Any]:
        merged = _merge_policy(raw, incoming, mode_n)
        raw.clear()
        raw.update(merged)
        return merged

    try:
        _raw, merged = sqlite_manual_policy.update_policy(_STATE_BASE, _mutate)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write policy: {e}")

    stats = _policy_stats(merged)
    return {"ok": True, "mode": mode_n, **stats}

def _import_enabled() -> bool:
    try:
        cfg = load_config()
        rt = cfg.get("runtime") or {}
        return bool(
            rt.get("debug_mods")
            or rt.get("debug")
            or os.environ.get("CW_DEBUG")
            or os.environ.get("CW_DEV_IMPORT")
        )
    except Exception:
        return bool(os.environ.get("CW_DEBUG") or os.environ.get("CW_DEV_IMPORT"))


def _state_store() -> StateStore:
    return StateStore(_STATE_BASE)


def _editor_send_targets(cfg: dict[str, Any], feature: str) -> list[dict[str, Any]]:
    feat = str(feature or "").strip().lower()
    if feat == "rating":
        feat = "ratings"
    if feat not in {"watchlist", "history", "ratings", "progress"}:
        return []

    targets: list[dict[str, Any]] = []
    for provider in sync_provider_names(upper=True):
        ops = load_sync_ops(provider)
        if not ops:
            continue
        try:
            supported = dict(ops.features() or {})
        except Exception:
            supported = {}
        if not bool(supported.get(feat)):
            continue

        try:
            instances = list_instance_ids(cfg, provider)
        except Exception:
            instances = ["default"]

        for raw_instance in instances:
            instance = normalize_instance_id(raw_instance)
            cfg_view = build_provider_config_view(cfg, provider, instance)
            try:
                configured = bool(ops.is_configured(cfg_view))
            except Exception:
                configured = False
            if not configured:
                continue

            label = provider.title()
            try:
                label = str(ops.label() or label)
            except Exception:
                pass
            instance_label = _provider_instance_label(cfg, provider, instance)

            targets.append(
                {
                    "provider": provider,
                    "instance": instance,
                    "label": label,
                    "instance_label": instance_label,
                    "display": label if instance == "default" else f"{label} ({instance_label})",
                    "feature": feat,
                    "history_enabled": bool(supported.get("history")),
                    "ratings_enabled": bool(supported.get("ratings")),
                    "watchlist_enabled": bool(supported.get("watchlist")),
                    "progress_enabled": bool(supported.get("progress")),
                }
            )

    targets.sort(key=lambda item: (str(item.get("label") or "").lower(), str(item.get("instance") or "")))
    return targets


@router.get("/send/providers")
def api_editor_send_providers(kind: str = Query("watchlist"), request: Request = cast(Request, None)) -> dict[str, Any]:
    cfg = load_config() or {}
    feature = _normalize_kind(kind)
    return {"ok": True, "kind": feature, "providers": _filter_targets_for_request(cfg, request, _editor_send_targets(cfg, feature))}


def _normalize_send_item(raw: Any, feature: str) -> dict[str, Any] | None:
    if not isinstance(raw, Mapping):
        return None
    item = dict(raw)
    key = str(item.pop("key", "") or "").strip()
    if feature == "history" and is_history_event_key(key):
        item["_cw_event_key"] = key
        item["_cw_rewatch_sync"] = True
    ids = item.get("ids")
    if not isinstance(ids, dict):
        ids = {}
    ids = {str(k).lower(): v for k, v in ids.items() if v not in (None, "")}
    for id_key in ("imdb", "tmdb", "tvdb", "trakt", "simkl", "anilist", "mal"):
        val = item.get(id_key)
        if val not in (None, "") and id_key not in ids:
            ids[id_key] = val
        item.pop(id_key, None)
    if ids:
        item["ids"] = ids

    show_ids = item.get("show_ids")
    if isinstance(show_ids, dict):
        item["show_ids"] = {str(k).lower(): v for k, v in show_ids.items() if v not in (None, "")}

    typ = str(item.get("type") or "").strip().lower()
    if typ == "tv":
        typ = "show"
    if typ:
        item["type"] = typ

    title = str(item.get("title") or item.get("name") or item.get("series_title") or "").strip()
    if title:
        item["title"] = title

    if feature == "ratings":
        rating = item.get("rating", item.get("user_rating", item.get("score")))
        try:
            rating_i = int(float(str(rating).strip()))
        except Exception:
            return None
        if rating_i < 1 or rating_i > 10:
            return None
        item["rating"] = rating_i
    elif feature == "history":
        watched_at = item.get("watched_at") or item.get("last_watched_at")
        if not watched_at:
            return None
        item["watched_at"] = watched_at
    elif feature == "progress":
        has_progress = any(item.get(k) not in (None, "") for k in ("progress_ms", "progressMs", "progress", "progress_percent", "progressPercent"))
        if not has_progress:
            return None

    if not ids and not key and not title:
        return None
    return item


def _items_confirmed_by_send(feature: Kind, items: list[dict[str, Any]], result: Mapping[str, Any]) -> list[dict[str, Any]]:
    key_fields = (
        "confirmed_keys",
        "skipped_keys",
        "accepted_keys",
        "presence_confirmed_keys",
        "live_confirmed_keys",
        "accepted_not_seen_live_keys",
        "date_confirmed_keys",
    )
    keep: set[str] = set()
    for field in key_fields:
        values = result.get(field)
        if isinstance(values, list):
            keep.update(str(v) for v in values if v)
    if keep:
        out: list[dict[str, Any]] = []
        for item in items:
            key = _editor_item_key(feature, str(item.get("_cw_event_key") or ""), item)
            if key and key in keep:
                out.append(item)
        return out

    confirmed = int(result.get("confirmed", result.get("count", 0)) or 0)
    if confirmed <= 0:
        return []
    return items[: min(confirmed, len(items))]


def _merge_sent_items_into_state(provider: str, instance: str, feature: Kind, items: list[dict[str, Any]]) -> None:
    if not items:
        return
    store = _state_store()
    inst = normalize_instance_id(instance)
    state = store.load_state_features({feature}) or {}
    current = dict(_load_state_items(feature, provider, inst, raw_state=state))

    for item in items:
        key = _editor_item_key(feature, str(item.get("_cw_event_key") or ""), item)
        if not key:
            continue
        try:
            current[str(key)] = _editor_minimal_item(feature, str(key), item)
        except Exception:
            current[str(key)] = dict(item)

    try:
        import time as _t
        last_sync_epoch = int(_t.time())
    except Exception:
        last_sync_epoch = None
    store.save_feature_baseline(
        provider=provider,
        instance=inst,
        feature=feature,
        items=current,
        last_sync_epoch=last_sync_epoch,
    )


@router.post("/send")
def api_editor_send(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> dict[str, Any]:
    feature = _normalize_kind(str(payload.get("kind") or "watchlist"))
    raw_items = payload.get("items")
    items_in = list(raw_items.values()) if isinstance(raw_items, dict) else raw_items
    if not isinstance(items_in, list) or not items_in:
        raise HTTPException(status_code=400, detail="No items selected")

    items: list[dict[str, Any]] = []
    invalid = 0
    for raw in items_in:
        item = _normalize_send_item(raw, feature)
        if item is None:
            invalid += 1
            continue
        items.append(item)
    if not items:
        raise HTTPException(status_code=400, detail=f"No selected rows can be sent as {feature}")

    selected_targets = payload.get("providers") or []
    if not isinstance(selected_targets, list) or not selected_targets:
        raise HTTPException(status_code=400, detail="No target providers selected")

    dry_run = bool(payload.get("dry_run", False))
    cfg = load_config() or {}
    available = _filter_targets_for_request(cfg, request, _editor_send_targets(cfg, feature))
    target_map = {
        (str(it.get("provider") or "").upper(), normalize_instance_id(it.get("instance") or "default")): it
        for it in available
    }

    def _emit(event: str, **data: Any) -> None:
        try:
            import crosswatch as CW  # type: ignore
            msg = f"[EDITOR] {event} {data.get('dst') or data.get('provider') or ''} {data.get('feature') or feature} count={data.get('count', data.get('attempted', ''))}"
            CW._append_log("SYNC", msg)
        except Exception:
            pass

    results: list[dict[str, Any]] = []
    totals = {"attempted": 0, "confirmed": 0, "skipped": 0, "unresolved": 0, "errors": 0}

    for target_raw in selected_targets:
        if not isinstance(target_raw, Mapping):
            continue
        provider = str(target_raw.get("provider") or "").strip().upper()
        instance = normalize_instance_id(target_raw.get("instance") or target_raw.get("provider_instance") or "default")
        if (provider, instance) not in target_map:
            results.append({"provider": provider, "instance": instance, "ok": False, "error": "provider_not_allowed"})
            totals["errors"] += 1
            continue

        ops = load_sync_ops(provider)
        if not ops:
            results.append({"provider": provider, "instance": instance, "ok": False, "error": "provider_unavailable"})
            totals["errors"] += 1
            continue

        cfg_view = build_provider_config_view(cfg, provider, instance)
        try:
            res = apply_add(
                dst_ops=ops,
                cfg=cfg_view,
                dst_name=provider,
                feature=feature,
                items=items,
                dry_run=dry_run,
                emit=_emit,
                dbg=lambda *a, **k: None,
                chunk_size=0,
                chunk_pause_ms=0,
            )
        except Exception:
            results.append({"provider": provider, "instance": instance, "ok": False, "error": "send_failed"})
            totals["errors"] += 1
            continue

        entry = {"provider": provider, "instance": instance, "ok": bool(res.get("ok", True)), "result": res}
        results.append(entry)
        for key in totals:
            totals[key] += int(res.get(key, 0) or 0)

        if not dry_run and bool(res.get("ok", True)) and int(res.get("confirmed", res.get("count", 0)) or 0) > 0:
            try:
                _merge_sent_items_into_state(provider, instance, feature, _items_confirmed_by_send(feature, items, res))
            except Exception:
                pass

    return {
        "ok": bool(results) and all(bool(r.get("ok")) for r in results),
        "kind": feature,
        "selected": len(items_in),
        "sent": len(items),
        "invalid": invalid,
        "dry_run": dry_run,
        "results": results,
        **totals,
    }



def _rebuild_watchlist_wall(state: dict[str, Any]) -> None:
    providers = state.get("providers")
    if not isinstance(providers, dict):
        return

    wall: list[dict[str, Any]] = []

    def _collect_from(node: Any) -> None:
        if not isinstance(node, dict):
            return
        fentry = node.get("watchlist") or {}
        if not isinstance(fentry, dict):
            return
        base = ((fentry.get("baseline") or {}).get("items") or {})
        if not isinstance(base, dict):
            return
        for v in base.values():
            try:
                wall.append(minimal(v))
            except Exception:
                wall.append(dict(v) if isinstance(v, dict) else {"title": str(v)})

    for _, pnode in providers.items():
        _collect_from(pnode)
        insts = (pnode or {}).get("instances") if isinstance(pnode, dict) else None
        if isinstance(insts, dict):
            for _, inode in insts.items():
                _collect_from(inode)

    seen: set[str] = set()
    uniq: list[dict[str, Any]] = []
    for it in wall:
        try:
            k = canonical_key(it)
        except Exception:
            k = str(it.get("title") or "")
        if not k or k in seen:
            continue
        seen.add(k)
        uniq.append(it)
    state["wall"] = uniq


@router.get("/state/import/providers")
def api_editor_state_import_providers(request: Request = cast(Request, None)) -> dict[str, Any]:
    if not _import_enabled():
        return {"enabled": False, "providers": []}

    cfg = load_config()
    out: list[dict[str, Any]] = []

    for name in sync_provider_names(upper=True):
        ops = load_sync_ops(name)
        if not ops:
            continue
        try:
            label = str(getattr(ops, "label", lambda: name)())
        except Exception:
            label = name
        feats = state_read_features(ops)
        try:
            inst_ids = list_instance_ids(cfg, name)
        except Exception:
            inst_ids = ["default"]
        user = request_user(request)
        if user and not bool(user.get("is_admin")):
            allowed = managed_profile_instances(cfg, user).get(str(name or "").strip().upper()) or []
            inst_ids = [inst for inst in inst_ids if normalize_instance_id(inst) in set(allowed)]
            if not inst_ids:
                continue

        configured = False
        if hasattr(ops, "is_configured"):
            for inst in inst_ids:
                try:
                    cfg_view = build_provider_config_view(cfg, name, inst)
                    configured = bool(ops.is_configured(cfg_view))
                except Exception:
                    configured = False
                if configured:
                    break
        else:
            configured = True

        out.append(
            {
                "name": name,
                "label": label or name,
                "configured": configured,
                "instances": [
                    {"id": normalize_instance_id(inst), "label": _provider_instance_label(cfg, name, inst)}
                    for inst in inst_ids
                ],
                "features": {
                    "watchlist": bool(feats.get("watchlist")),
                    "history": bool(feats.get("history")),
                    "ratings": bool(feats.get("ratings")),
                    "progress": bool(feats.get("progress")),
                },
            }
        )

    out.sort(key=lambda x: (not x.get("configured"), x.get("name") or ""))
    return {"enabled": True, "providers": out}


@router.post("/state/import")
def api_editor_state_import(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> dict[str, Any]:
    if not _import_enabled():
        raise HTTPException(status_code=403, detail="State import is disabled (enable runtime.debug_mods).")

    cfg = load_config()
    provider = str((payload or {}).get("provider") or "").strip().upper()
    provider_instance = _instance_for_request(cfg, request, provider, (payload or {}).get("provider_instance"))
    feats_in = (payload or {}).get("features")
    mode = str((payload or {}).get("mode") or "replace").strip().lower()
    dry_run = bool((payload or {}).get("dry_run") or False)

    if not provider:
        raise HTTPException(status_code=400, detail="Missing provider")
    if mode not in ("replace", "merge"):
        raise HTTPException(status_code=400, detail="Invalid mode")

    _require_instance_scope(cfg, request, provider, provider_instance)

    features: list[str]
    if isinstance(feats_in, list):
        features = [str(x).strip().lower() for x in feats_in if str(x).strip()]
    else:
        features = ["watchlist", "history", "ratings", "progress"]

    allowed = {"watchlist", "history", "ratings", "progress"}
    features = [f for f in features if f in allowed]
    if not features:
        raise HTTPException(status_code=400, detail="No features selected")

    cfg_view = build_provider_config_view(cfg, provider, provider_instance)

    ops = load_sync_ops(provider)
    if not ops:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

    if hasattr(ops, "is_configured"):
        try:
            if not ops.is_configured(cfg_view):
                raise HTTPException(status_code=400, detail=f"Provider not configured: {provider} ({provider_instance})")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=400, detail=f"Provider not configured: {provider} ({provider_instance})")

    feats_supported = state_read_features(ops)

    store = _state_store()
    state = store.load_state_features(set(features)) if not dry_run else {"providers": {}, "wall": [], "last_sync_epoch": None}

    providers_block = state.get("providers")
    if not isinstance(providers_block, dict):
        providers_block = {}
        state["providers"] = providers_block

    base_node = providers_block.setdefault(provider, {})
    if not isinstance(base_node, dict):
        base_node = {}
        providers_block[provider] = base_node

    if provider_instance == "default":
        prov_node = base_node
    else:
        insts = base_node.get("instances")
        if not isinstance(insts, dict):
            insts = {}
            base_node["instances"] = insts
        prov_node = insts.get(provider_instance)
        if not isinstance(prov_node, dict):
            prov_node = {}
            insts[provider_instance] = prov_node

    imported: dict[str, Any] = {
        "provider": provider,
        "provider_instance": provider_instance,
        "mode": mode,
        "dry_run": dry_run,
        "features": {},
    }

    import time as _t
    changed_blocks: dict[tuple[str, str, str], dict[str, Any]] = {}

    for feature in features:
        if not bool(feats_supported.get(feature)):
            imported["features"][feature] = {
                "ok": False,
                "skipped": True,
                "reason": "complete provider state unavailable",
            }
            continue

        t0 = _t.time()
        try:
            idx = dict(ops.build_index(cfg_view, feature=feature) or {})
        except Exception as e:
            imported["features"][feature] = {"ok": False, "error": str(e)}
            continue

        items_min: dict[str, Any] = {}
        for k, v in idx.items():
            try:
                items_min[str(k)] = minimal(v)
            except Exception:
                items_min[str(k)] = dict(v) if isinstance(v, dict) else {"title": str(v)}

        try:
            cp = module_checkpoint(ops, cfg_view, feature)
        except Exception:
            cp = None

        imported["features"][feature] = {
            "ok": True,
            "count": len(items_min),
            "checkpoint": cp,
            "elapsed_ms": int((_t.time() - t0) * 1000),
        }

        if dry_run:
            continue

        feat_node = prov_node.setdefault(feature, {})
        if not isinstance(feat_node, dict):
            feat_node = {}
            prov_node[feature] = feat_node

        base_feat = feat_node.setdefault("baseline", {})
        if not isinstance(base_feat, dict):
            base_feat = {}
            feat_node["baseline"] = base_feat

        if mode == "merge":
            cur = base_feat.get("items")
            cur_items = dict(cur) if isinstance(cur, dict) else {}
            for k, v in items_min.items():
                cur_items[k] = v
            base_feat["items"] = cur_items
        else:
            base_feat["items"] = items_min

        feat_node["checkpoint"] = cp
        changed_blocks[(provider, provider_instance, feature)] = feat_node

    if not dry_run:
        store.save_feature_blocks(changed_blocks, last_sync_epoch=int(_t.time()))

    return {"ok": True, **imported}

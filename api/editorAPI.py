# /api/editorAPI.py
# CrossWatch - Tracker editor API for history / ratings / watchlist
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any

import io
import json
import os
from pathlib import Path

from fastapi import APIRouter, Body, File, HTTPException, Query, UploadFile
from fastapi.responses import StreamingResponse

from cw_platform.config_base import load_config
from cw_platform.id_map import canonical_key, minimal
from cw_platform.modules_registry import load_sync_ops
from cw_platform.orchestrator._snapshots import module_checkpoint
from cw_platform.orchestrator._state_store import StateStore

from services.editor import (
    Kind,
    export_tracker_zip,
    import_tracker_upload,
    list_snapshots,
    load_state,
    save_state,
)

router = APIRouter(prefix="/api/editor", tags=["editor"])

_STATE_PATH = Path("/config/state.json")
_POLICY_PATH = Path("/config/state.manual.json")

def _atomic_write_json(path: Path, payload: Any) -> None:
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_suffix(path.suffix + ".tmp")
        tmp.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True), "utf-8")
        os.replace(tmp, path)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to write {path}: {e}")

def _load_current_state() -> dict[str, Any]:
    if not _STATE_PATH.exists():
        return {}
    try:
        raw = json.loads(_STATE_PATH.read_text("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read state file: {e}")
    return raw if isinstance(raw, dict) else {}


def _load_policy() -> dict[str, Any]:
    if not _POLICY_PATH.exists():
        return {"version": 1, "providers": {}}
    try:
        raw = json.loads(_POLICY_PATH.read_text("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to read policy file: {e}")
    if not isinstance(raw, dict):
        return {"version": 1, "providers": {}}
    prov = raw.get("providers")
    if not isinstance(prov, dict):
        raw["providers"] = {}
    if "version" not in raw:
        raw["version"] = 1
    return raw


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


def _load_policy_manual(kind: Kind, provider: str) -> tuple[dict[str, Any], list[str]]:
    raw = _load_policy()
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return {}, []
    node = providers.get(provider)
    if not isinstance(node, dict):
        for k, v in providers.items():
            if str(k).lower() == str(provider).lower() and isinstance(v, dict):
                node = v
                break
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
    adds_items = {}
    if isinstance(adds_raw, dict):
        items = adds_raw.get("items") or {}
        if isinstance(items, dict):
            adds_items = {str(k): v for k, v in items.items()}
    return adds_items, blocks


def _save_policy_manual(kind: Kind, provider: str, adds_items: dict[str, Any], blocks: list[str]) -> None:
    raw = _load_policy()
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

    _atomic_write_json(_POLICY_PATH, raw)


def _policy_from_state() -> dict[str, Any]:
    raw = _load_current_state()
    provs = raw.get("providers") or {}
    if not isinstance(provs, dict):
        return {"version": 1, "providers": {}}
    out: dict[str, Any] = {"version": 1, "providers": {}}
    dst = out["providers"]
    for p, node in provs.items():
        if not isinstance(node, dict):
            continue
        manual = node.get("manual")
        if not isinstance(manual, dict):
            continue
        entry: dict[str, Any] = {}
        for kind in ("watchlist", "history", "ratings"):
            f = manual.get(kind)
            if not isinstance(f, dict):
                continue
            blocks = f.get("blocks") or []
            adds = f.get("adds") or {}
            if not isinstance(adds, dict):
                adds = {}
            items = adds.get("items") or {}
            if not isinstance(items, dict):
                items = {}
            if blocks or items:
                entry[kind] = {"blocks": list(blocks) if isinstance(blocks, list) else list(blocks.keys()) if isinstance(blocks, dict) else [], "adds": {"items": dict(items)}}
        if entry:
            dst[str(p)] = entry
    return out


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

    for p, node in prov_in.items():
        if not isinstance(node, dict):
            continue
        target = prov_out.get(p)
        if not isinstance(target, dict):
            target = {}
            prov_out[p] = target
        for kind in ("watchlist", "history", "ratings"):
            f = node.get(kind)
            if not isinstance(f, dict):
                continue
            t = target.get(kind)
            if not isinstance(t, dict):
                t = {}
                target[kind] = t

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
                merged = dict(items_out)
                merged.update({str(k): v for k, v in items_in.items()})
                adds_out["items"] = merged

    return out


def _mirror_policy_into_state() -> None:
    if not _STATE_PATH.exists():
        return
    pol = _load_policy()
    prov_pol = pol.get("providers")
    if not isinstance(prov_pol, dict) or not prov_pol:
        return

    raw = _load_current_state()
    provs = raw.get("providers")
    if not isinstance(provs, dict):
        provs = {}
        raw["providers"] = provs

    changed = False

    def _find_key(name: str) -> str:
        if name in provs:
            return name
        nl = name.lower()
        for k in provs.keys():
            if str(k).lower() == nl:
                return str(k)
        return name

    for p, node in prov_pol.items():
        if not isinstance(node, dict):
            continue
        key = _find_key(str(p))
        cur = provs.get(key)
        if not isinstance(cur, dict):
            cur = {}
            provs[key] = cur
            changed = True
        manual = cur.get("manual")
        if not isinstance(manual, dict):
            manual = {}
            cur["manual"] = manual
            changed = True
        for kind in ("watchlist", "history", "ratings"):
            f = node.get(kind)
            if not isinstance(f, dict):
                continue
            t = manual.get(kind)
            if not isinstance(t, dict):
                t = {}
                manual[kind] = t
                changed = True

            adds_in, blocks_in = _load_policy_manual(kind, str(p))
            adds_state, blocks_state = _load_state_manual(kind, key)

            merged_blocks = _merge_blocks(blocks_state, blocks_in)
            if t.get("blocks") != merged_blocks:
                t["blocks"] = merged_blocks
                changed = True

            adds = t.get("adds")
            if not isinstance(adds, dict):
                adds = {}
                t["adds"] = adds
                changed = True
            items_out = adds.get("items")
            if not isinstance(items_out, dict):
                items_out = {}
            merged_items = dict(items_out)
            merged_items.update(adds_state)
            merged_items.update(adds_in)
            if items_out != merged_items:
                adds["items"] = merged_items
                changed = True

    if changed:
        _atomic_write_json(_STATE_PATH, raw)


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
        for kind in ("watchlist", "history", "ratings"):
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

def _state_providers(raw: dict[str, Any]) -> list[str]:
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return []
    return sorted([str(k) for k in providers.keys() if str(k).strip()])

def _load_state_items(kind: Kind, provider: str) -> dict[str, Any]:
    raw = _load_current_state()
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return {}
    node = providers.get(provider) or {}
    if not isinstance(node, dict):
        return {}
    feature = node.get(kind) or {}
    if not isinstance(feature, dict):
        return {}
    baseline = feature.get("baseline") or {}
    if not isinstance(baseline, dict):
        return {}
    items = baseline.get("items") or {}
    return items if isinstance(items, dict) else {}

def _save_state_items(kind: Kind, provider: str, items: dict[str, Any]) -> None:
    raw = _load_current_state()
    providers = raw.get("providers")
    if not isinstance(providers, dict):
        providers = {}
        raw["providers"] = providers
    node = providers.get(provider)
    if not isinstance(node, dict):
        node = {}
        providers[provider] = node
    feature = node.get(kind)
    if not isinstance(feature, dict):
        feature = {"baseline": {"items": {}}, "checkpoint": None}
        node[kind] = feature
    baseline = feature.get("baseline")
    if not isinstance(baseline, dict):
        baseline = {"items": {}}
        feature["baseline"] = baseline
    baseline["items"] = dict(items or {})
    _atomic_write_json(_STATE_PATH, raw)


def _load_state_manual(kind: Kind, provider: str) -> tuple[dict[str, Any], list[str]]:
    raw = _load_current_state()
    providers = raw.get("providers") or {}
    if not isinstance(providers, dict):
        return {}, []
    node = providers.get(provider)
    if not isinstance(node, dict):
        for k, v in providers.items():
            if str(k).lower() == str(provider).lower() and isinstance(v, dict):
                node = v
                break
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
    adds_items = {}
    if isinstance(adds_raw, dict):
        items = adds_raw.get("items") or {}
        if isinstance(items, dict):
            adds_items = {str(k): v for k, v in items.items()}

    return adds_items, blocks


def _save_state_manual(kind: Kind, provider: str, adds_items: dict[str, Any], blocks: list[str]) -> None:
    raw = _load_current_state()
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

    manual = node.get("manual")
    if not isinstance(manual, dict):
        manual = {}
        node["manual"] = manual

    f = manual.get(kind)
    if not isinstance(f, dict):
        f = {}
        manual[kind] = f

    f["blocks"] = list(blocks or [])

    adds = f.get("adds")
    if not isinstance(adds, dict):
        adds = {}
        f["adds"] = adds
    adds["items"] = dict(adds_items or {})

    _atomic_write_json(_STATE_PATH, raw)

def _normalize_kind(val: str | None) -> Kind:
    k = (val or "watchlist").strip().lower()
    if k not in ("watchlist", "history", "ratings"):
        raise HTTPException(status_code=400, detail=f"Unsupported kind: {k}")
    return k  # type: ignore[return-value]

@router.get("/state/providers")
def api_editor_state_providers() -> dict[str, Any]:
    raw_state = _load_current_state()
    raw_policy = _load_policy()
    return {"providers": _union_providers(raw_state, raw_policy)}

@router.get("")
def api_editor_get_state(
    kind: str = Query("watchlist"),
    snapshot: str | None = Query(None),
    source: str = Query("tracker"),
    provider: str | None = Query(None),
) -> dict[str, Any]:
    k = _normalize_kind(kind)
    src = (source or "tracker").strip().lower()
    if src in ("tracker", "cw", "crosswatch"):
        state = load_state(k, snapshot=snapshot)
        items = state.get("items") or {}
        if not isinstance(items, dict):
            items = {}
        return {
            "kind": k,
            "source": "tracker",
            "snapshot": snapshot,
            "provider": None,
            "ts": state.get("ts"),
            "count": len(items),
            "items": items,
        }
    if src in ("state", "current"):
        raw_state = _load_current_state()
        raw_policy = _load_policy()
        providers = _union_providers(raw_state, raw_policy)
        chosen = (provider or "").strip() or (providers[0] if providers else "")
        if not chosen:
            return {
                "kind": k,
                "source": "state",
                "snapshot": None,
                "provider": None,
                "ts": None,
                "count": 0,
                "items": {},
                "manual_adds": {},
                "manual_blocks": [],
            }

        if _STATE_PATH.exists() and _POLICY_PATH.exists():
            _mirror_policy_into_state()
            raw_state = _load_current_state()

        items = _load_state_items(k, chosen) if raw_state else {}
        st_adds, st_blocks = _load_state_manual(k, chosen) if raw_state else ({}, [])
        pol_adds, pol_blocks = _load_policy_manual(k, chosen)

        manual_adds = dict(st_adds or {})
        manual_adds.update(dict(pol_adds or {}))
        manual_blocks = _merge_blocks(st_blocks or [], pol_blocks or [])

        ts = None
        try:
            if _STATE_PATH.exists():
                ts = int(_STATE_PATH.stat().st_mtime)
            elif _POLICY_PATH.exists():
                ts = int(_POLICY_PATH.stat().st_mtime)
        except Exception:
            ts = None
        return {
            "kind": k,
            "source": "state",
            "snapshot": None,
            "provider": chosen,
            "ts": ts,
            "count": len(items),
            "items": items,
            "manual_adds": manual_adds,
            "manual_blocks": manual_blocks,
        }
    raise HTTPException(status_code=400, detail=f"Unsupported source: {src}")
@router.get("/snapshots")
def api_editor_list_snapshots(
    kind: str = Query("watchlist"),
) -> dict[str, Any]:
    k = _normalize_kind(kind)
    snaps = list_snapshots(k)
    return {"kind": k, "snapshots": snaps}

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

@router.post("")
def api_editor_save_state(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    kind = _normalize_kind(str(payload.get("kind") or "watchlist"))
    src = str(payload.get("source") or "tracker").strip().lower()
    items_raw = payload.get("items")
    items = _normalize_items(items_raw)
    if src in ("tracker", "cw", "crosswatch"):
        state = save_state(kind, items)
        return {
            "ok": True,
            "kind": kind,
            "source": "tracker",
            "provider": None,
            "count": len(items),
            "ts": state.get("ts"),
        }
    if src in ("state", "current"):
        provider = str(payload.get("provider") or "").strip()
        if not provider:
            raise HTTPException(status_code=400, detail="Missing provider for source=state")
        blocks_raw = payload.get("blocks") or []
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

        _save_policy_manual(kind, provider, items, blocks)
        if _STATE_PATH.exists():
            _save_state_manual(kind, provider, items, blocks)
        if _STATE_PATH.exists() and _POLICY_PATH.exists():
            _mirror_policy_into_state()
        ts = None
        try:
            ts = int(_STATE_PATH.stat().st_mtime)
        except Exception:
            ts = None
        return {
            "ok": True,
            "kind": kind,
            "source": "state",
            "provider": provider,
            "count": len(items),
            "blocks": len(blocks),
            "ts": ts,
        }
    raise HTTPException(status_code=400, detail=f"Unsupported source: {src}")

@router.get("/state/manual/export")
def api_editor_state_manual_export() -> StreamingResponse:
    pol = _load_policy()
    if _policy_stats(pol)["providers"] == 0:
        pol = _policy_from_state()
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
) -> dict[str, Any]:
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

    current = _load_policy()
    merged = _merge_policy(current, incoming, mode_n)
    _atomic_write_json(_POLICY_PATH, merged)

    if _STATE_PATH.exists():
        _mirror_policy_into_state()

    stats = _policy_stats(merged)
    return {"ok": True, "mode": mode_n, **stats}

@router.get("/export")
def api_editor_export() -> StreamingResponse:
    data = export_tracker_zip()
    return StreamingResponse(
        io.BytesIO(data),
        media_type="application/zip",
        headers={"Content-Disposition": "attachment; filename=crosswatch-tracker.zip"},
    )

@router.post("/import")
async def api_editor_import(file: UploadFile = File(...)) -> dict[str, Any]:
    payload = await file.read()
    filename = file.filename or "upload.json"
    try:
        stats = import_tracker_upload(payload, filename)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"ok": True, **stats}



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
    return StateStore(_STATE_PATH.parent)


def _rebuild_watchlist_wall(state: dict[str, Any]) -> None:
    providers = state.get("providers")
    if not isinstance(providers, dict):
        return

    wall: list[dict[str, Any]] = []
    for _, fmap in providers.items():
        fentry = (fmap or {}).get("watchlist") or {}
        base = ((fentry.get("baseline") or {}).get("items") or {})
        if not isinstance(base, dict):
            continue
        for v in base.values():
            try:
                wall.append(minimal(v))
            except Exception:
                wall.append(dict(v) if isinstance(v, dict) else {"title": str(v)})

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
def api_editor_state_import_providers() -> dict[str, Any]:
    if not _import_enabled():
        return {"enabled": False, "providers": []}

    cfg = load_config()
    names = ["PLEX", "SIMKL", "TRAKT", "JELLYFIN", "EMBY", "MDBLIST", "TAUTULLI"]
    out: list[dict[str, Any]] = []

    for name in names:
        ops = load_sync_ops(name)
        if not ops:
            continue
        try:
            label = str(getattr(ops, "label", lambda: name)())
        except Exception:
            label = name
        try:
            feats = dict(getattr(ops, "features", lambda: {})())
        except Exception:
            feats = {}
        try:
            configured = bool(getattr(ops, "is_configured", lambda _cfg: True)(cfg))
        except Exception:
            configured = False

        out.append(
            {
                "name": name,
                "label": label or name,
                "configured": configured,
                "features": {
                    "watchlist": bool(feats.get("watchlist")),
                    "history": bool(feats.get("history")),
                    "ratings": bool(feats.get("ratings")),
                },
            }
        )

    out.sort(key=lambda x: (not x.get("configured"), x.get("name") or ""))
    return {"enabled": True, "providers": out}


@router.post("/state/import")
def api_editor_state_import(payload: dict[str, Any] = Body(...)) -> dict[str, Any]:
    if not _import_enabled():
        raise HTTPException(status_code=403, detail="State import is disabled (enable runtime.debug_mods).")

    provider = str((payload or {}).get("provider") or "").strip().upper()
    feats_in = (payload or {}).get("features")
    mode = str((payload or {}).get("mode") or "replace").strip().lower()
    dry_run = bool((payload or {}).get("dry_run") or False)

    if not provider:
        raise HTTPException(status_code=400, detail="Missing provider")
    if mode not in ("replace", "merge"):
        raise HTTPException(status_code=400, detail="Invalid mode")

    features: list[str]
    if isinstance(feats_in, list):
        features = [str(x).strip().lower() for x in feats_in if str(x).strip()]
    else:
        features = ["watchlist", "history", "ratings"]

    allowed = {"watchlist", "history", "ratings"}
    features = [f for f in features if f in allowed]
    if not features:
        raise HTTPException(status_code=400, detail="No features selected")

    cfg = load_config()
    ops = load_sync_ops(provider)
    if not ops:
        raise HTTPException(status_code=400, detail=f"Unknown provider: {provider}")

    if hasattr(ops, "is_configured"):
        try:
            if not ops.is_configured(cfg):
                raise HTTPException(status_code=400, detail=f"Provider not configured: {provider}")
        except HTTPException:
            raise
        except Exception:
            raise HTTPException(status_code=400, detail=f"Provider not configured: {provider}")

    try:
        feats_supported = dict(getattr(ops, "features", lambda: {})())
    except Exception:
        feats_supported = {}

    store = _state_store()
    state = store.load_state() if not dry_run else {"providers": {}, "wall": [], "last_sync_epoch": None}

    providers_block = state.get("providers")
    if not isinstance(providers_block, dict):
        providers_block = {}
        state["providers"] = providers_block

    prov_node = providers_block.setdefault(provider, {})
    if not isinstance(prov_node, dict):
        prov_node = {}
        providers_block[provider] = prov_node

    imported: dict[str, Any] = {"provider": provider, "mode": mode, "dry_run": dry_run, "features": {}} 

    import time as _t

    for feature in features:
        if not bool(feats_supported.get(feature)):
            imported["features"][feature] = {"ok": False, "skipped": True, "reason": "feature disabled"}
            continue

        t0 = _t.time()
        try:
            idx = dict(ops.build_index(cfg, feature=feature) or {})
        except Exception as e:
            imported["features"][feature] = {"ok": False, "error": str(e)}
            continue

        items_min = {}
        for k, v in idx.items():
            try:
                items_min[str(k)] = minimal(v)
            except Exception:
                items_min[str(k)] = dict(v) if isinstance(v, dict) else {"title": str(v)}

        try:
            cp = module_checkpoint(ops, cfg, feature)
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

        base_node = feat_node.setdefault("baseline", {})
        if not isinstance(base_node, dict):
            base_node = {}
            feat_node["baseline"] = base_node

        if mode == "merge":
            cur = base_node.get("items")
            cur_items = dict(cur) if isinstance(cur, dict) else {}
            for k, v in items_min.items():
                cur_items[k] = v
            base_node["items"] = cur_items
        else:
            base_node["items"] = items_min

        feat_node["checkpoint"] = cp

    if not dry_run:
        state["last_sync_epoch"] = int(_t.time())
        if "watchlist" in features:
            _rebuild_watchlist_wall(state)
        store.save_state(state)

    return {"ok": True, **imported}

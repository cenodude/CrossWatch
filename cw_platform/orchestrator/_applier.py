# cw_platform/orchestration/_applier.py
# Applier logic for adding/removing items in destination services.
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
from collections.abc import Sequence, Mapping
from typing import Any, Callable, cast
from . import _unresolved as _unresolved_mod
record_unresolved = cast(Callable[..., dict[str, Any]], getattr(_unresolved_mod, "record_unresolved"))

def _retry(fn: Callable[[], Any], *, attempts: int = 3, base_sleep: float = 0.5) -> Any:
    last = None
    for i in range(attempts):
        try: return fn()
        except Exception as e:
            last = e
            __import__("time").sleep(base_sleep * (2 ** i))
    raise last  # type: ignore

def _normalize(
    res: dict[str, Any] | None,
    items: Sequence[Mapping[str, Any]],
    tag: str,
    *,
    dst: str,
    feature: str,
    emit,
) -> dict[str, Any]:
    res = dict(res or {})
    attempted = len(items)
    ok = bool(res.get("ok", True))
    ckeys = [str(x) for x in (res.get("confirmed_keys") or []) if x]
    skeys = [str(x) for x in (res.get("skipped_keys") or []) if x]
    confirmed = res.get("confirmed")
    if confirmed is None:
        if ckeys:
            confirmed = len(ckeys)
        elif ok:
            confirmed = int(res.get("count") or res.get("added") or res.get("removed") or 0)
        else:
            confirmed = 0

    unresolved_list = res.get("unresolved") or []
    unresolved_keys: list[str] = []

    if isinstance(unresolved_list, list) and unresolved_list:
        emit("apply:unresolved", provider=dst, feature=feature, count=len(unresolved_list))

        def _unwrap(x: Mapping[str, Any]) -> tuple[Mapping[str, Any], str | None]:
            inner = x.get("item")
            if isinstance(inner, Mapping):
                hint = x.get("hint") or x.get("reason") or x.get("error")
                return inner, (str(hint).strip() or None) if hint is not None else None
            hint = x.get("hint") or x.get("reason") or x.get("error")
            return x, (str(hint).strip() or None) if hint is not None else None

        def _has_ids(x: Mapping[str, Any] | None) -> bool:
            ids = (x or {}).get("ids") or {}
            return any(ids.get(k) for k in ("tmdb", "imdb", "tvdb", "trakt", "slug"))

        try:
            from ..id_map import canonical_key as _ckey  # type: ignore
        except Exception:  # pragma: no cover
            _ckey = None  # type: ignore

        # Extract unresolved canonical keys for the orchestrator (used for blackbox/flap decisions).
        try:
            for raw in unresolved_list:
                if not isinstance(raw, Mapping):
                    continue
                item_u, _ = _unwrap(raw)
                if not isinstance(item_u, Mapping):
                    continue
                if _ckey:
                    try:
                        k = _ckey(item_u) or ""
                    except Exception:
                        k = ""
                    if k:
                        unresolved_keys.append(str(k))
        except Exception:
            pass

        # Persist unresolved items so the operator can inspect them.
        try:
            to_store: list[dict[str, Any]] = []
            for raw in unresolved_list:
                if not isinstance(raw, Mapping):
                    continue
                item_u, hint_u = _unwrap(raw)
                if not isinstance(item_u, Mapping):
                    continue
                if not _has_ids(item_u):
                    continue
                d = dict(item_u)
                if hint_u:
                    d["_cw_unresolved_hint"] = hint_u
                to_store.append(d)

            if to_store:
                record_unresolved(dst, feature, to_store, hint=f"{tag}:provider_unresolved")
            elif int(confirmed or 0) == 0 and items:
                record_unresolved(dst, feature, [dict(it) for it in items], hint=f"{tag}:fallback_unresolved")
        except Exception:
            pass

    if unresolved_keys:
        seen: set[str] = set()
        unresolved_keys = [k for k in unresolved_keys if k and (k not in seen and not seen.add(k))]

    unresolved = len(unresolved_list) if isinstance(unresolved_list, list) else int(unresolved_list or 0)
    errors = int(res.get("errors") or 0)
    skipped_reported_raw = res.get("skipped")
    skipped_exact = len(skeys)
    inferred_remainder = max(0, attempted - int(confirmed) - unresolved - errors)
    if skipped_reported_raw is not None:
        skipped = max(0, int(skipped_reported_raw or 0))
        skipped_inferred = 0
        skip_basis = "provider_keys+count" if skeys else "provider_count"
    else:
        skipped = inferred_remainder
        skipped_inferred = max(0, skipped - skipped_exact)
        skip_basis = "provider_keys+inferred_remainder" if skeys else "inferred_remainder"
    out = {
        "ok": ok,
        "attempted": attempted,
        "confirmed": int(confirmed),
        "confirmed_keys": ckeys,
        "skipped": int(skipped),
        "skipped_keys": skeys,
        "skipped_exact": int(skipped_exact),
        "skipped_inferred": int(skipped_inferred),
        "skipped_reported": None if skipped_reported_raw is None else int(skipped_reported_raw or 0),
        "skip_basis": skip_basis,
        "unresolved_keys": list(unresolved_keys),
        "unresolved": int(unresolved),
        "errors": int(errors),
    }
    out["count"] = out["confirmed"]
    return out

def _apply_chunked(
    tag: str,
    *,
    dst: str,
    feature: str,
    items: Sequence[Mapping[str, Any]],
    call: Callable[[Sequence[Mapping[str, Any]]], dict[str, Any] | None],
    emit,
    dbg,
    chunk_size: int,
    chunk_pause_ms: int,
) -> dict[str, Any]:
    total = len(items)
    if total == 0:
        return {"ok": True, "attempted": 0, "confirmed": 0, "skipped": 0, "unresolved": 0, "errors": 0, "count": 0}
    csize = int(chunk_size or 0)
    if csize <= 0 or total <= csize:
        raw = _retry(lambda: call(items))
        return _normalize(raw, items, tag, dst=dst, feature=feature, emit=emit)

    done = 0
    agg: dict[str, Any] = {
        "ok": True,
        "attempted": 0,
        "confirmed": 0,
        "confirmed_keys": [],
        "skipped": 0,
        "skipped_keys": [],
        "skipped_exact": 0,
        "skipped_inferred": 0,
        "skipped_reported": 0,
        "skip_basis": "provider_keys",
        "unresolved": 0,
        "errors": 0,
    }
    for i in range(0, total, csize):
        chunk = items[i : i + csize]
        raw = _retry(lambda: call(chunk))
        res = _normalize(raw, chunk, tag, dst=dst, feature=feature, emit=emit)
        agg["ok"] = agg["ok"] and res["ok"]
        agg["attempted"] += res["attempted"]
        agg["confirmed"] += res["confirmed"]
        agg["skipped"] += res["skipped"]
        agg["skipped_exact"] += int(res.get("skipped_exact", 0) or 0)
        agg["skipped_inferred"] += int(res.get("skipped_inferred", 0) or 0)
        agg["skipped_reported"] += int(res.get("skipped_reported", 0) or 0)
        agg["unresolved"] += res["unresolved"]
        agg["errors"] += res["errors"]
        if res.get("confirmed_keys"):
            agg["confirmed_keys"].extend(res["confirmed_keys"])
        if res.get("skipped_keys"):
            agg["skipped_keys"].extend(res["skipped_keys"])
        basis = str(res.get("skip_basis") or "provider_keys")
        if agg.get("skip_basis") != basis:
            agg["skip_basis"] = "mixed"
        done += len(chunk)
        emit(f"{tag}:progress", dst=dst, feature=feature, done=done, total=total, ok=res["ok"])
        pause = int(chunk_pause_ms or 0)
        if pause:
            try:
                __import__("time").sleep(pause / 1000.0)
            except Exception:
                pass
    agg["count"] = agg["confirmed"]
    return agg

def apply_add(
    *,
    dst_ops,
    cfg,
    dst_name: str,
    feature: str,
    items: Sequence[Mapping[str, Any]],
    dry_run: bool,
    emit,
    dbg,
    chunk_size: int,
    chunk_pause_ms: int,
) -> dict[str, Any]:
    emit("apply:add:start", dst=dst_name, feature=feature, count=len(items))
    res = _apply_chunked(
        "apply:add",
        dst=dst_name,
        feature=feature,
        items=items,
        call=lambda ch: dst_ops.add(cfg, ch, feature=feature, dry_run=dry_run),
        emit=emit,
        dbg=dbg,
        chunk_size=chunk_size,
        chunk_pause_ms=chunk_pause_ms,
    )
    _conf = int(res.get("confirmed", 0))
    emit(
        "apply:add:done",
        dst=dst_name,
        feature=feature,
        count=_conf,
        attempted=int(res.get("attempted", 0)),
        added=_conf,
        skipped=int(res.get("skipped", 0)),
        skipped_exact=int(res.get("skipped_exact", 0)),
        skipped_inferred=int(res.get("skipped_inferred", 0)),
        skip_basis=str(res.get("skip_basis") or "provider_keys"),
        unresolved=int(res.get("unresolved", 0)),
        errors=int(res.get("errors", 0)),
        result=res,
    )
    return res

def apply_remove(
    *,
    dst_ops,
    cfg,
    dst_name: str,
    feature: str,
    items: Sequence[Mapping[str, Any]],
    dry_run: bool,
    emit,
    dbg,
    chunk_size: int,
    chunk_pause_ms: int,
) -> dict[str, Any]:
    emit("apply:remove:start", dst=dst_name, feature=feature, count=len(items))
    res = _apply_chunked(
        "apply:remove",
        dst=dst_name,
        feature=feature,
        items=items,
        call=lambda ch: dst_ops.remove(cfg, ch, feature=feature, dry_run=dry_run),
        emit=emit,
        dbg=dbg,
        chunk_size=chunk_size,
        chunk_pause_ms=chunk_pause_ms,
    )
    _conf = int(res.get("confirmed", 0))
    emit(
        "apply:remove:done",
        dst=dst_name,
        feature=feature,
        count=_conf,
        attempted=int(res.get("attempted", 0)),
        removed=_conf,
        skipped=int(res.get("skipped", 0)),
        skipped_exact=int(res.get("skipped_exact", 0)),
        skipped_inferred=int(res.get("skipped_inferred", 0)),
        skip_basis=str(res.get("skip_basis") or "provider_keys"),
        unresolved=int(res.get("unresolved", 0)),
        errors=int(res.get("errors", 0)),
        result=res,
    )
    return res

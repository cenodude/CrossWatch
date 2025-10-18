from __future__ import annotations
from typing import Any, Callable, Dict, List
from ._unresolved import record_unresolved

#--- Retry wrapper with exponential backoff -----------------------------------
def _retry(fn: Callable[[], Any], *, attempts: int = 3, base_sleep: float = 0.5) -> Any:
    last = None
    for i in range(attempts):
        try: return fn()
        except Exception as e:
            last = e; __import__("time").sleep(base_sleep * (2 ** i))
    raise last  # type: ignore

#--- Normalize provider response into standard structure ----------------------
def _normalize(res: Dict[str, Any] | None, items: List[Dict[str, Any]], tag: str, *, dst: str, feature: str, emit) -> Dict[str, Any]:
    res = dict(res or {})
    attempted = len(items or ())
    ok = bool(res.get("ok", True))
    ckeys = list(res.get("confirmed_keys") or [])
    confirmed = res.get("confirmed")
    if confirmed is None:
        if ckeys:
            confirmed = len(ckeys)
        elif ok:
            confirmed = int(res.get("count") or res.get("added") or res.get("removed") or 0)
        else:
            confirmed = 0

    unresolved_list = res.get("unresolved") or []
    if isinstance(unresolved_list, list) and unresolved_list:
        emit("apply:unresolved", provider=dst, feature=feature, count=len(unresolved_list))
        # If provider didn't include usable IDs, fall back to the items we attempted.
        def _has_ids(x):
            ids = (x or {}).get("ids") or {}
            return any(ids.get(k) for k in ("imdb","tmdb","tvdb","slug"))
        try:
            if any(isinstance(x, dict) for x in unresolved_list) and any(_has_ids(x) for x in unresolved_list if isinstance(x, dict)):
                record_unresolved(dst, feature, unresolved_list, hint=f"{tag}:provider_unresolved")
            else:
                if int(confirmed or 0) == 0 and items:
                    record_unresolved(dst, feature, items, hint=f"{tag}:fallback_unresolved")
        except Exception:
            pass

    unresolved = len(unresolved_list) if isinstance(unresolved_list, list) else int(unresolved_list or 0)
    errors = int(res.get("errors") or 0)
    skipped = max(0, attempted - int(confirmed) - unresolved - errors)

    out = {
        "ok": ok,
        "attempted": attempted,
        "confirmed": int(confirmed),
        "confirmed_keys": ckeys,
        "skipped": int(skipped),
        "unresolved": int(unresolved),
        "errors": int(errors),
    }
    out["count"] = out["confirmed"]
    return out

#--- Chunked apply wrapper ----------------------------------------------------
def _apply_chunked(tag: str, *, dst: str, feature: str, items: List[Dict[str, Any]],
                   call: Callable[[List[Dict[str, Any]]], Dict[str, Any]], emit, dbg,
                   chunk_size: int, chunk_pause_ms: int) -> Dict[str, Any]:
    total = len(items)
    if total == 0: return {"ok": True, "attempted": 0, "confirmed": 0, "skipped": 0, "unresolved": 0, "errors": 0, "count": 0}
    csize = int(chunk_size or 0)

    if csize <= 0 or total <= csize:
        raw = _retry(lambda: call(items))
        return _normalize(raw, items, tag, dst=dst, feature=feature, emit=emit)

    done = 0
    agg = {"ok": True, "attempted": 0, "confirmed": 0, "confirmed_keys": [], "skipped": 0, "unresolved": 0, "errors": 0}
    for i in range(0, total, csize):
        chunk = items[i:i + csize]
        raw = _retry(lambda: call(chunk))
        res = _normalize(raw, chunk, tag, dst=dst, feature=feature, emit=emit)
        agg["ok"] = agg["ok"] and res["ok"]
        agg["attempted"] += res["attempted"]
        agg["confirmed"] += res["confirmed"]
        agg["skipped"] += res["skipped"]
        agg["unresolved"] += res["unresolved"]
        agg["errors"] += res["errors"]
        if res.get("confirmed_keys"):
            agg["confirmed_keys"].extend(res["confirmed_keys"])
        done += len(chunk)
        emit(f"{tag}:progress", dst=dst, feature=feature, done=done, total=total, ok=res["ok"])
        pause = int(chunk_pause_ms or 0)
        if pause:
            try: __import__("time").sleep(pause / 1000.0)
            except Exception: pass
    agg["count"] = agg["confirmed"]
    return agg

#--- Public apply functions ---------------------------------------------------
def apply_add(*, dst_ops, cfg, dst_name: str, feature: str, items: List[Dict[str, Any]],
              dry_run: bool, emit, dbg, chunk_size: int, chunk_pause_ms: int) -> Dict[str, Any]:
    emit("apply:add:start", dst=dst_name, feature=feature, count=len(items))
    res = _apply_chunked(
        "apply:add", dst=dst_name, feature=feature, items=items,
        call=lambda ch: dst_ops.add(cfg, ch, feature=feature, dry_run=dry_run),
        emit=emit, dbg=dbg, chunk_size=chunk_size, chunk_pause_ms=chunk_pause_ms,
    )
    _conf = int(res.get("confirmed", 0))
    emit("apply:add:done",
         dst=dst_name, feature=feature,
         count=_conf, attempted=int(res.get("attempted", 0)),
         added=_conf, skipped=int(res.get("skipped", 0)),
         unresolved=int(res.get("unresolved", 0)), errors=int(res.get("errors", 0)),
         result=res)
    return res

#--- Public apply_remove functions ---------------------------------------------
def apply_remove(*, dst_ops, cfg, dst_name: str, feature: str, items: List[Dict[str, Any]],
                 dry_run: bool, emit, dbg, chunk_size: int, chunk_pause_ms: int) -> Dict[str, Any]:
    emit("apply:remove:start", dst=dst_name, feature=feature, count=len(items))
    res = _apply_chunked(
        "apply:remove", dst=dst_name, feature=feature, items=items,
        call=lambda ch: dst_ops.remove(cfg, ch, feature=feature, dry_run=dry_run),
        emit=emit, dbg=dbg, chunk_size=chunk_size, chunk_pause_ms=chunk_pause_ms,
    )
    _conf = int(res.get("confirmed", 0))
    emit("apply:remove:done",
         dst=dst_name, feature=feature,
         count=_conf, attempted=int(res.get("attempted", 0)),
         removed=_conf, skipped=int(res.get("skipped", 0)),
         unresolved=int(res.get("unresolved", 0)), errors=int(res.get("errors", 0)),
         result=res)
    return res

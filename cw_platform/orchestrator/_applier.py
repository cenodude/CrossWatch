from __future__ import annotations
from typing import Any, Callable, Dict, List
from ._unresolved import record_unresolved  # persist provider-declared unresolved

def _retry(fn: Callable[[], Any], *, attempts: int = 3, base_sleep: float = 0.5) -> Any:
    last = None
    for i in range(attempts):
        try: return fn()
        except Exception as e:
            last = e; __import__("time").sleep(base_sleep * (2 ** i))
    raise last  # type: ignore

def _apply_chunked(tag: str, *, dst: str, feature: str, items: List[Dict[str, Any]],
                   call: Callable[[List[Dict[str, Any]]], Dict[str, Any]], emit, dbg,
                   chunk_size: int, chunk_pause_ms: int) -> Dict[str, Any]:
    # Note: persist unresolved returned by provider; still return legacy shape for callers.
    total = len(items)
    if total == 0: return {"ok": True, "count": 0}
    csize = int(chunk_size or 0)

    if csize <= 0 or total <= csize:
        res = _retry(lambda: call(items))
        ok = bool((res or {}).get("ok", True))
        cnt = int((res or {}).get("count") or (res or {}).get("added") or (res or {}).get("removed") or total)
        unresolved = (res or {}).get("unresolved") or []
        if unresolved:
            emit("apply:unresolved", provider=dst, feature=feature, count=len(unresolved))
            try: record_unresolved(dst, feature, unresolved, hint=f"{tag}:provider_unresolved")
            except Exception: pass
        return {"ok": ok, "count": cnt}

    done = 0; ok_all = True; cnt_total = 0; un_total = 0
    for i in range(0, total, csize):
        chunk = items[i:i + csize]
        res = _retry(lambda: call(chunk))
        ok = bool((res or {}).get("ok", True))
        cnt = int((res or {}).get("count") or (res or {}).get("added") or (res or {}).get("removed") or len(chunk))
        unresolved = (res or {}).get("unresolved") or []
        if unresolved:
            emit("apply:unresolved", provider=dst, feature=feature, count=len(unresolved))
            un_total += len(unresolved)
            try: record_unresolved(dst, feature, unresolved, hint=f"{tag}:provider_unresolved")
            except Exception: pass
        ok_all = ok_all and ok; cnt_total += cnt; done += len(chunk)
        emit(f"{tag}:progress", dst=dst, feature=feature, done=done, total=total, ok=ok)
        pause = int(chunk_pause_ms or 0)
        if pause:
            try: __import__("time").sleep(pause / 1000.0)
            except Exception: pass
    return {"ok": ok_all, "count": cnt_total, "unresolved": un_total}

def apply_add(*, dst_ops, cfg, dst_name: str, feature: str, items: List[Dict[str, Any]],
              dry_run: bool, emit, dbg, chunk_size: int, chunk_pause_ms: int) -> Dict[str, Any]:
    emit("apply:add:start", dst=dst_name, feature=feature, count=len(items))
    res = _apply_chunked(
        "apply:add", dst=dst_name, feature=feature, items=items,
        call=lambda ch: dst_ops.add(cfg, ch, feature=feature, dry_run=dry_run),
        emit=emit, dbg=dbg, chunk_size=chunk_size, chunk_pause_ms=chunk_pause_ms,
    )
    emit("apply:add:done", dst=dst_name, feature=feature, count=int(res.get("count") or 0), result=res)
    return res

def apply_remove(*, dst_ops, cfg, dst_name: str, feature: str, items: List[Dict[str, Any]],
                 dry_run: bool, emit, dbg, chunk_size: int, chunk_pause_ms: int) -> Dict[str, Any]:
    emit("apply:remove:start", dst=dst_name, feature=feature, count=len(items))
    res = _apply_chunked(
        "apply:remove", dst=dst_name, feature=feature, items=items,
        call=lambda ch: dst_ops.remove(cfg, ch, feature=feature, dry_run=dry_run),
        emit=emit, dbg=dbg, chunk_size=chunk_size, chunk_pause_ms=chunk_pause_ms,
    )
    emit("apply:remove:done", dst=dst_name, feature=feature, count=int(res.get("count") or 0), result=res)
    return res

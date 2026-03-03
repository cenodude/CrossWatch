# /providers/sync/emby/_progress.py
# EMBY Module for progress (resume) synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from .._log import log as cw_log

from cw_platform.id_map import canonical_key

from ._common import (
    normalize as emby_normalize,
    resolve_item_id,
    resolve_item_ids,
)


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "progress", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "progress", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "progress", "warn", msg, **fields)


def _iso_now() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _ticks_to_ms(v: Any) -> int | None:
    if v is None:
        return None
    try:
        n = int(v)
        if n <= 0:
            return None
        # .NET ticks (100ns)
        return n // 10_000
    except Exception:
        return None


def _ms_to_ticks(v_ms: Any) -> int | None:
    if v_ms is None:
        return None
    try:
        n = int(float(v_ms))
        if n < 0:
            n = 0
        return n * 10_000
    except Exception:
        return None


def _pick_user_data(row: Mapping[str, Any]) -> Mapping[str, Any]:
    ud = row.get("UserData")
    return ud if isinstance(ud, Mapping) else {}


def build_index(adapter: Any, **_kwargs: Any) -> Mapping[str, dict[str, Any]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return {}

    params: dict[str, Any] = {
        "Recursive": True,
        "IncludeItemTypes": "Movie,Episode",
        "Fields": "UserData,ProviderIds,RunTimeTicks,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId,ParentId,Name",
        "EnableUserData": True,
        "IsResumable": True,
        "Limit": 10_000,
    }

    try:
        r = http.get(f"/Users/{uid}/Items", params=params)
        if getattr(r, "status_code", 0) != 200:
            _warn("index.http", status=getattr(r, "status_code", None))
            return {}
        body = r.json() or {}
        rows = body.get("Items") or []
        if not isinstance(rows, list):
            return {}
    except Exception as e:
        _warn("index.exception", error=str(e))
        return {}

    out: dict[str, dict[str, Any]] = {}
    total_rows = 0
    dup_keys = 0
    for raw in rows:
        if not isinstance(raw, Mapping):
            continue
        total_rows += 1
        try:
            item = emby_normalize(raw)
        except Exception:
            continue

        ud = _pick_user_data(raw)
        pos_ms = _ticks_to_ms(ud.get("PlaybackPositionTicks"))
        if pos_ms is None or pos_ms <= 0:
            continue

        dur_ms = _ticks_to_ms(raw.get("RunTimeTicks"))
        lp = ud.get("LastPlayedDate") or ud.get("LastPlayed") or ud.get("DatePlayed")
        if isinstance(lp, str) and lp.strip():
            item["progress_at"] = lp.strip()
        item["progress_ms"] = int(pos_ms)
        if dur_ms is not None and dur_ms > 0:
            item["duration_ms"] = int(dur_ms)

        ck = canonical_key(item)
        if not ck:
            continue

        action = "new"
        prev = out.get(ck)
        if not isinstance(prev, Mapping):
            out[ck] = item
        else:
            try:
                p0 = int(prev.get("progress_ms") or 0)
                p1 = int(item.get("progress_ms") or 0)
            except Exception:
                p0, p1 = 0, 0
            if p0 and p1 and p1 < p0:
                out[ck] = item
                dup_keys += 1
                action = "replace_lower"
            elif p0 and p1:
                dup_keys += 1
                action = "dup_keep"

        _dbg(
            "item",
            canonical_key=str(ck),
            type=str(item.get("type") or ""),
            title=str(item.get("title") or ""),
            series_title=str(item.get("series_title") or ""),
            season=item.get("season"),
            episode=item.get("episode"),
            chosen_lastPlayedAt=str(item.get("progress_at") or ""),
            chosen_viewOffset=int(item.get("progress_ms") or 0),
            duration_ms=item.get("duration_ms"),
            ids=dict(item.get("ids") or {}),
            emby_item_id=str(item.get("emby_item_id") or item.get("jellyfin_item_id") or ""),
            action=action,
        )

    _dbg("index.done", count=len(out), rows=total_rows, dup_keys=dup_keys)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    ok = 0
    unresolved: list[dict[str, Any]] = []

    for it in items or []:
        it0 = dict(it or {})
        ck = canonical_key(it0) or ""
        ids = dict(it0.get("ids") or {})
        iids = resolve_item_ids(adapter, it0)
        _dbg(
            "iid.resolve",
            canonical_key=str(ck),
            type=str(it0.get("type") or ""),
            ids=ids,
            resolved_ids=list(iids),
        )
        if not iids:
            unresolved.append({"item": it0, "hint": "not_found"})
            continue

        ms = it0.get("progress_ms") or it0.get("progress") or it0.get("viewOffset")
        ticks = _ms_to_ticks(ms)
        if ticks is None:
            unresolved.append({"item": it0, "hint": "missing_progress"})
            continue

        payload: dict[str, Any] = {"PlaybackPositionTicks": int(ticks)}
        pa = it0.get("progress_at")
        if isinstance(pa, str) and pa.strip():
            payload["LastPlayedDate"] = pa.strip()

        any_ok = False
        for iid in iids:
            try:
                r = http.post(f"/Users/{uid}/Items/{iid}/UserData", json=payload)
                sc = int(getattr(r, "status_code", 0) or 0)
                _dbg(
                    "add.write",
                    canonical_key=str(ck),
                    item_id=str(iid),
                    progress_ms=_ticks_to_ms(payload.get("PlaybackPositionTicks")),
                    status=sc,
                )
                if sc in (200, 204):
                    ok += 1
                    any_ok = True
                else:
                    unresolved.append({"item": it0, "hint": f"http_{sc}", "item_id": str(iid)})
            except Exception as e:
                _warn("add.exception", canonical_key=str(ck), item_id=str(iid), error=str(e))
                unresolved.append({"item": it0, "hint": f"exception:{e}", "item_id": str(iid)})
        if not any_ok:
            unresolved.append({"item": it0, "hint": "all_writes_failed"})

    _info("add.done", ok=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    ok = 0
    unresolved: list[dict[str, Any]] = []

    for it in items or []:
        it0 = dict(it or {})
        ck = canonical_key(it0) or ""
        iid = resolve_item_id(adapter, it0)
        _dbg("iid.resolve.one", canonical_key=str(ck), resolved_id=str(iid or ""))
        if not iid:
            unresolved.append({"item": it0, "hint": "not_found"})
            continue

        payload: dict[str, Any] = {"PlaybackPositionTicks": 0}
        try:
            r = http.post(f"/Users/{uid}/Items/{iid}/UserData", json=payload)
            sc = int(getattr(r, "status_code", 0) or 0)
            _dbg("remove.write", canonical_key=str(ck), item_id=str(iid), status=sc)
            if sc in (200, 204):
                ok += 1
            else:
                unresolved.append({"item": it0, "hint": f"http_{sc}"})
        except Exception as e:
            _warn("remove.exception", canonical_key=str(ck), item_id=str(iid), error=str(e))
            unresolved.append({"item": it0, "hint": f"exception:{e}"})

    _info("remove.done", ok=ok, unresolved=len(unresolved))
    return ok, unresolved

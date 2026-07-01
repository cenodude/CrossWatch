# /providers/sync/jellyfin/_progress.py
# JELLYFIN Module for progress (resume) synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key

from ._common import (
    _ids_from_provider_ids,
    chunked,
    jf_item_library_ids,
    jf_selected_library_ids,
    make_logger,
    normalize as jelly_normalize,
    resolve_item_id,
    resolve_item_ids,
)

_dbg, _info, _warn = make_logger("progress")


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


def _series_ids_by_item_id(
    http: Any,
    uid: str,
    rows: Iterable[tuple[Mapping[str, Any], str | None]],
) -> dict[str, dict[str, str]]:
    series_ids = sorted(
        {
            str(raw.get("SeriesId") or "").strip()
            for raw, _source_library_id in rows
            if str(raw.get("SeriesId") or "").strip()
        }
    )
    out: dict[str, dict[str, str]] = {}
    for batch in chunked(series_ids, 100):
        try:
            response = http.get(
                f"/Users/{uid}/Items",
                params={
                    "Ids": ",".join(batch),
                    "Fields": "ProviderIds,ProductionYear,Type,Name",
                },
            )
            if getattr(response, "status_code", 0) != 200:
                continue
            body = response.json() or {}
            for row in body.get("Items") or []:
                if not isinstance(row, Mapping):
                    continue
                series_id = str(row.get("Id") or "").strip()
                provider_ids = row.get("ProviderIds")
                if series_id and isinstance(provider_ids, Mapping):
                    out[series_id] = _ids_from_provider_ids(provider_ids)
        except Exception as exc:
            _warn("series_metadata_query_failed", series_ids=batch, error=str(exc))
    return out


def build_index(adapter: Any, **_kwargs: Any) -> Mapping[str, dict[str, Any]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return {}

    base_params: dict[str, Any] = {
        "Recursive": True,
        "IncludeItemTypes": "Movie,Episode",
        "Fields": "UserData,ProviderIds,RunTimeTicks,ProductionYear,Type,IndexNumber,ParentIndexNumber,SeriesId,ParentId,CollectionFolderId,AncestorIds,LibraryId,Name",
        "EnableUserData": True,
        "Filters": "IsResumable",
    }
    allowed = jf_selected_library_ids(adapter.cfg, "progress")
    if not allowed:
        _dbg("library_scope_not_configured")
    parents: list[str | None] = list(sorted(allowed))
    if not parents:
        parents.append(None)
    rows: list[tuple[Mapping[str, Any], str | None]] = []
    seen_item_ids: set[str] = set()
    page_size = 500
    for parent_id in parents:
        start = 0
        while True:
            params = dict(base_params)
            params.update({"StartIndex": start, "Limit": page_size, "EnableTotalRecordCount": True})
            if parent_id:
                params["ParentId"] = parent_id
            try:
                r = http.get(f"/Users/{uid}/Items", params=params)
                if getattr(r, "status_code", 0) != 200:
                    _warn("library_scope_query_failed", source_library_id=parent_id, allowed_library_ids=sorted(allowed), status=getattr(r, "status_code", None))
                    break
                body = r.json() or {}
                page = body.get("Items") or []
                if not isinstance(page, list):
                    break
            except Exception as e:
                _warn("library_scope_query_failed", source_library_id=parent_id, allowed_library_ids=sorted(allowed), error=str(e))
                break
            new_count = 0
            for raw in page:
                if not isinstance(raw, Mapping):
                    continue
                item_id = str(raw.get("Id") or "").strip()
                if item_id and item_id in seen_item_ids:
                    _dbg("duplicate_progress_item", provider_item_id=item_id, source_library_id=parent_id, item_title=str(raw.get("Name") or ""), media_type=str(raw.get("Type") or ""))
                    continue
                if item_id:
                    seen_item_ids.add(item_id)
                rows.append((raw, parent_id))
                new_count += 1
            start += len(page)
            total = int(body.get("TotalRecordCount") or 0)
            if not page or (total and start >= total) or len(page) < page_size or new_count == 0:
                break

    series_ids_by_item_id = _series_ids_by_item_id(http, str(uid), rows)
    out: dict[str, dict[str, Any]] = {}
    total_rows = 0
    dup_keys = 0
    for raw, source_library_id in rows:
        total_rows += 1
        if allowed:
            memberships = jf_item_library_ids(raw)
            if memberships and not (memberships & allowed):
                _dbg("outside_library_scope", provider_item_id=str(raw.get("Id") or ""), source_library_id=source_library_id, allowed_library_ids=sorted(allowed), item_title=str(raw.get("Name") or ""), media_type=str(raw.get("Type") or ""), provider_ids=dict(raw.get("ProviderIds") or {}))
                continue
        try:
            item = jelly_normalize(raw)
        except Exception:
            continue
        series_id = str(raw.get("SeriesId") or "").strip()
        if series_id and series_ids_by_item_id.get(series_id):
            item["show_ids"] = dict(series_ids_by_item_id[series_id])
        if source_library_id:
            item["library_id"] = source_library_id

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
            # If duplicates exist for the same canonical key, keep the minimum
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
            jellyfin_item_id=str(item.get("jellyfin_item_id") or ""),
            action=action,
        )

    _info("index_done", count=len(out), rows=total_rows, dup_keys=dup_keys, allowed_library_ids=sorted(allowed), scope_enabled=bool(allowed))
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
        iids = resolve_item_ids(adapter, it0, feature="progress")
        if not iids:
            _dbg("resolve_miss", canonical_key=str(ck), type=str(it0.get("type") or ""), ids=ids)
            unresolved.append({"item": it0, "hint": str(getattr(adapter, "_jellyfin_last_resolve_hint", "") or "not_found")})
            continue
        _dbg("resolve_hit", canonical_key=str(ck), type=str(it0.get("type") or ""), ids=ids, resolved=len(iids))

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
                _dbg("write_prepare", op="add", canonical_key=str(ck), item_id=str(iid), progress_ms=_ticks_to_ms(payload.get("PlaybackPositionTicks")), status=sc)
                if sc in (200, 204):
                    ok += 1
                    any_ok = True
                else:
                    unresolved.append({"item": it0, "hint": f"http_{sc}", "item_id": str(iid)})
            except Exception as e:
                _warn("write_failed", op="add", canonical_key=str(ck), item_id=str(iid), error=str(e))
                unresolved.append({"item": it0, "hint": f"exception:{e}", "item_id": str(iid)})
        if not any_ok:
            unresolved.append({"item": it0, "hint": "all_writes_failed"})

    _info("write_done", op="add", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
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
        iid = resolve_item_id(adapter, it0, feature="progress")
        if not iid:
            _dbg("resolve_miss", canonical_key=str(ck))
            unresolved.append({"item": it0, "hint": "not_found"})
            continue
        _dbg("resolve_hit", canonical_key=str(ck), resolved=1, resolved_id=str(iid))

        payload: dict[str, Any] = {"PlaybackPositionTicks": 0}
        try:
            r = http.post(f"/Users/{uid}/Items/{iid}/UserData", json=payload)
            sc = int(getattr(r, "status_code", 0) or 0)
            _dbg("write_prepare", op="remove", canonical_key=str(ck), item_id=str(iid), status=sc)
            if sc in (200, 204):
                ok += 1
            else:
                unresolved.append({"item": it0, "hint": f"http_{sc}"})
        except Exception as e:
            _warn("write_failed", op="remove", canonical_key=str(ck), item_id=str(iid), error=str(e))
            unresolved.append({"item": it0, "hint": f"exception:{e}"})

    _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved

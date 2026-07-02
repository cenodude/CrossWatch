# /providers/sync/jellyfin/_progress.py
# JELLYFIN Module for progress (resume) synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import os
from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key
from providers.sync._progress_policy import decide_progress_write, select_progress_record

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
from ._routes import items as items_route, played as played_route, user_data as user_data_route, user_params

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


def _same_origin(provider: str = "JELLYFIN") -> bool:
    source = str(os.getenv("CW_PAIR_SRC") or "").upper().strip()
    target = str(os.getenv("CW_PAIR_DST") or provider).upper().strip()
    source_instance = str(os.getenv("CW_PAIR_SRC_INSTANCE") or "default").lower().strip()
    target_instance = str(os.getenv("CW_PAIR_DST_INSTANCE") or "default").lower().strip()
    return source == target == provider and source_instance == target_instance


def _active_item_ids(http: Any, uid: str) -> set[str]:
    try:
        response = http.get("/Sessions")
        if getattr(response, "status_code", 0) != 200:
            return set()
        rows = response.json() or []
        return {
            str((row.get("NowPlayingItem") or {}).get("Id"))
            for row in rows
            if isinstance(row, Mapping)
            and str(row.get("UserId") or "") == str(uid)
            and isinstance(row.get("NowPlayingItem"), Mapping)
            and (row.get("NowPlayingItem") or {}).get("Id")
        }
    except Exception:
        return set()


def _target_state(http: Any, uid: str, item_id: str) -> dict[str, Any]:
    response = http.get(
        items_route(item_id),
        params=user_params(uid, {"Fields": "UserData,RunTimeTicks,LibraryId,CollectionFolderId"}),
    )
    if getattr(response, "status_code", 0) != 200:
        raise RuntimeError(f"target_state_http_{getattr(response, 'status_code', 0)}")
    row = response.json() or {}
    user_data = _pick_user_data(row)
    return {
        "watched": bool(user_data.get("Played") or user_data.get("IsPlayed")),
        "progress_ms": _ticks_to_ms(user_data.get("PlaybackPositionTicks")),
        "duration_ms": _ticks_to_ms(row.get("RunTimeTicks")),
        "timestamp": user_data.get("LastPlayedDate") or user_data.get("LastPlayed") or row.get("DateLastSaved"),
        "library_id": row.get("LibraryId") or row.get("CollectionFolderId"),
    }


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
                items_route(),
                params=user_params(uid, {
                    "Ids": ",".join(batch),
                    "Fields": "ProviderIds,ProductionYear,Type,Name",
                }),
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
        seen_pages: set[tuple[str, ...]] = set()
        while True:
            params = dict(base_params)
            params.update({"StartIndex": start, "Limit": page_size, "EnableTotalRecordCount": True})
            if parent_id:
                params["ParentId"] = parent_id
            try:
                r = http.get(items_route(), params=user_params(uid, params))
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
            signature = tuple(str(raw.get("Id") or "") for raw in page if isinstance(raw, Mapping))
            if page and signature in seen_pages:
                _warn("pagination_repeated_page", source_library_id=parent_id, start_index=start)
                break
            seen_pages.add(signature)
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
            start += len(page)
            total = int(body.get("TotalRecordCount") or 0)
            if not page or (total and start >= total) or len(page) < page_size:
                break

    series_ids_by_item_id = _series_ids_by_item_id(http, str(uid), rows)
    out: dict[str, dict[str, Any]] = {}
    total_rows = 0
    dup_keys = 0
    for raw, source_library_id in rows:
        total_rows += 1
        raw_type = str(raw.get("Type") or "").strip()
        if raw_type not in {"Movie", "Episode"}:
            _warn("unsupported_progress_row_type", provider_item_id=str(raw.get("Id") or ""), media_type=raw_type)
            continue
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
        if raw_type == "Episode" and not item.get("show_ids") and not item.get("season") and not item.get("episode"):
            _warn("incomplete_episode_progress_row", provider_item_id=str(raw.get("Id") or ""))
            continue
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

        prev = out.get(ck)
        selected, action = select_progress_record(prev, item)
        out[ck] = selected
        if isinstance(prev, Mapping):
            dup_keys += 1

        _dbg(
            "item",
            canonical_key=str(ck),
            type=str(item.get("type") or ""),
            title=str(item.get("title") or ""),
            series_title=str(item.get("series_title") or ""),
            season=item.get("season"),
            episode=item.get("episode"),
            chosen_lastPlayedAt=str(selected.get("progress_at") or ""),
            chosen_viewOffset=int(selected.get("progress_ms") or 0),
            duration_ms=selected.get("duration_ms"),
            ids=dict(selected.get("ids") or {}),
            jellyfin_item_id=str(selected.get("jellyfin_item_id") or ""),
            action=action,
        )

    _info("index_done", count=len(out), rows=total_rows, dup_keys=dup_keys, allowed_library_ids=sorted(allowed), scope_enabled=bool(allowed))
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    applied = 0
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []
    active_items = _active_item_ids(http, str(uid))
    replay_enabled = bool(getattr(adapter.cfg, "progress_replay_enabled", False))
    tolerance = int(getattr(adapter.cfg, "progress_timestamp_tolerance_seconds", 30) or 30)

    for it in items or []:
        it0 = dict(it or {})
        ck = canonical_key(it0) or ""
        ids = dict(it0.get("ids") or {})
        iids = resolve_item_ids(adapter, it0, feature="progress")
        if not iids:
            _dbg("resolve_miss", canonical_key=str(ck), type=str(it0.get("type") or ""), ids=ids)
            reason = str(getattr(adapter, "_jellyfin_last_resolve_hint", "") or "not_found")
            entry = {"status": "unresolved", "reason": reason, "item": it0}
            unresolved.append(entry)
            results.append(entry)
            continue
        _dbg("resolve_hit", canonical_key=str(ck), type=str(it0.get("type") or ""), ids=ids, resolved=len(iids))

        ms = it0.get("progress_ms") or it0.get("progress") or it0.get("viewOffset")
        ticks = _ms_to_ticks(ms)
        if ticks is None:
            entry = {"status": "unresolved", "reason": "missing_progress", "item": it0}
            unresolved.append(entry)
            results.append(entry)
            continue

        payload: dict[str, Any] = {"PlaybackPositionTicks": int(ticks)}
        pa = it0.get("progress_at")
        if isinstance(pa, str) and pa.strip():
            payload["LastPlayedDate"] = pa.strip()

        for iid in iids:
            try:
                target = _target_state(http, str(uid), str(iid))
                decision = decide_progress_write(
                    active_session=str(iid) in active_items,
                    source_timestamp=pa,
                    target_timestamp=target.get("timestamp"),
                    source_progress_ms=ms,
                    source_duration_ms=it0.get("duration_ms"),
                    target_progress_ms=target.get("progress_ms"),
                    target_duration_ms=target.get("duration_ms"),
                    target_watched=bool(target.get("watched")),
                    same_origin=_same_origin(),
                    replay_enabled=replay_enabled,
                    timestamp_tolerance_seconds=tolerance,
                )
                context = {
                    "provider": "jellyfin", "provider_instance": os.getenv("CW_PAIR_DST_INSTANCE") or "default",
                    "remote_item_id": str(iid), "library_id": target.get("library_id") or it0.get("library_id"),
                    "source_timestamp": pa, "target_timestamp": target.get("timestamp"),
                    "source_progress": ms, "target_progress": target.get("progress_ms"), "reason": decision.reason,
                }
                if not decision.apply:
                    results.append({"status": "skipped", **context})
                    _dbg("write_skipped", **{key: value for key, value in context.items() if key != "provider"})
                    continue
                if decision.unwatch_first:
                    unwatch = http.delete(played_route(str(iid)), params=user_params(uid))
                    if getattr(unwatch, "status_code", 0) not in (200, 204):
                        entry = {"status": "failed", **context, "reason": "replay_unwatch_failed", "hint": f"http_{getattr(unwatch, 'status_code', 0)}", "item": it0}
                        unresolved.append(entry)
                        results.append(entry)
                        continue
                r = http.post(user_data_route(str(iid)), params=user_params(uid), json=payload)
                sc = int(getattr(r, "status_code", 0) or 0)
                _dbg("write_prepare", op="add", canonical_key=str(ck), item_id=str(iid), progress_ms=_ticks_to_ms(payload.get("PlaybackPositionTicks")), status=sc)
                if sc in (200, 204):
                    applied += 1
                    results.append({"status": "applied", **context})
                else:
                    entry = {"status": "failed", **context, "hint": f"http_{sc}", "item": it0}
                    unresolved.append(entry)
                    results.append(entry)
            except Exception as e:
                _warn("write_failed", op="add", canonical_key=str(ck), item_id=str(iid), error=str(e))
                entry = {"status": "failed", "reason": "target_state_or_write_failed", "item": it0, "remote_item_id": str(iid), "hint": f"exception:{e}"}
                unresolved.append(entry)
                results.append(entry)

    setattr(adapter, "_progress_write_results", results)
    _info("write_done", op="add", ok=len(unresolved) == 0, applied=applied, skipped=sum(1 for row in results if row.get("status") == "skipped"), unresolved=sum(1 for row in results if row.get("status") == "unresolved"), failed=sum(1 for row in results if row.get("status") == "failed"))
    return applied, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = getattr(adapter, "client", None)
    uid = getattr(getattr(adapter, "cfg", None), "user_id", None)
    if not http or not uid:
        return 0, [{"item": dict(x), "hint": "not_configured"} for x in (items or [])]

    ok = 0
    unresolved: list[dict[str, Any]] = []
    results: list[dict[str, Any]] = []

    for it in items or []:
        it0 = dict(it or {})
        ck = canonical_key(it0) or ""
        iid = resolve_item_id(adapter, it0, feature="progress")
        if not iid:
            _dbg("resolve_miss", canonical_key=str(ck))
            entry = {"status": "unresolved", "reason": "not_found", "item": it0}
            unresolved.append(entry)
            results.append(entry)
            continue
        _dbg("resolve_hit", canonical_key=str(ck), resolved=1, resolved_id=str(iid))

        payload: dict[str, Any] = {"PlaybackPositionTicks": 0}
        try:
            r = http.post(user_data_route(str(iid)), params=user_params(uid), json=payload)
            sc = int(getattr(r, "status_code", 0) or 0)
            _dbg("write_prepare", op="remove", canonical_key=str(ck), item_id=str(iid), status=sc)
            if sc in (200, 204):
                ok += 1
                results.append({"status": "applied", "provider": "jellyfin", "remote_item_id": str(iid), "library_id": it0.get("library_id"), "source_progress": 0, "reason": "clear_progress"})
            else:
                entry = {"status": "failed", "reason": "clear_progress_failed", "item": it0, "remote_item_id": str(iid), "hint": f"http_{sc}"}
                unresolved.append(entry)
                results.append(entry)
        except Exception as e:
            _warn("write_failed", op="remove", canonical_key=str(ck), item_id=str(iid), error=str(e))
            entry = {"status": "failed", "reason": "clear_progress_failed", "item": it0, "remote_item_id": str(iid), "hint": f"exception:{e}"}
            unresolved.append(entry)
            results.append(entry)

    setattr(adapter, "_progress_write_results", results)
    _info("write_done", op="remove", ok=len(unresolved) == 0, applied=ok, unresolved=len(unresolved))
    return ok, unresolved

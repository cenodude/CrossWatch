# services/assistant_diagnostics.py
# CrossWatch - Read-only capture, scheduler and review diagnostics
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any

from fastapi import HTTPException, Request
from cw_platform.access_policy import request_user, request_is_monitor
from services.assistant_state import _item

TOOLS = {
    "compare_captures": ("Compare two saved captures from list_captures. Lists added, removed and updated items; this does not refresh providers or restore anything.",
        {"type": "object", "properties": {"a": {"type": "string"}, "b": {"type": "string"}, "feature": {"type": "string"}, "query": {"type": "string"}, "offset": {"type": "integer", "minimum": 0}, "limit": {"type": "integer", "minimum": 1, "maximum": 25}}, "required": ["a", "b"]}),
    "get_scheduler_details": ("Read scheduler running state, next run and configuration warnings. Does not trigger or replan jobs.", {"type": "object", "properties": {}}),
    "list_sync_reviews": ("Read Interactive Sync reviews owned by the current caller, including pending changes and approval status. Does not start, refresh, select or apply a review.", {"type": "object", "properties": {"profile_id": {"type": "string"}}}),
}


def admin(request):
    user = request_user(request)
    if request_is_monitor(request) or (user and not user.get("is_admin")):
        raise HTTPException(403, "Administrator access required")


def _fields(data, names):
    return {k: v[:500] if isinstance(v, str) else v for k, v in data.items() if k in names and isinstance(v, (str, int, float, bool, type(None)))}


def run(name: str, request: Request, args: dict[str, Any]):
    if name == "compare_captures":
        from api.snapshotsAPI import api_snapshots_diff_extended

        a, b = str(args.get("a") or ""), str(args.get("b") or "")
        if not a or not b or max(len(a), len(b)) > 512:
            raise HTTPException(400, "Choose two capture paths from list_captures")
        try:
            offset, limit = max(0, int(args.get("offset", 0))), max(1, min(25, int(args.get("limit", 10))))
        except (ValueError, TypeError, OverflowError):
            raise HTTPException(400, "offset and limit must be integers") from None
        response = api_snapshots_diff_extended(a=a, b=b, feature=str(args.get("feature") or "")[:32], kind="all", q=str(args.get("query") or "")[:256], offset=offset, limit=limit, max_changes=20, max_depth=4, request=request)
        data = json.loads(bytes(response.body))
        if response.status_code != 200 or data.get("ok") is False:
            raise HTTPException(response.status_code if response.status_code >= 400 else 400, "Captures unavailable or comparison failed")
        diff = data.get("diff") or {}
        meta = ("path", "provider", "instance", "feature", "created_at", "count")
        return {"a": _fields(diff.get("a") or {}, meta), "b": _fields(diff.get("b") or {}, meta),
                "summary": _fields(diff.get("summary") or {}, ("total_a", "total_b", "added", "removed", "updated", "unchanged")),
                "total": diff.get("total"), "offset": offset, "limit": limit,
                "items": [{"status": row.get("status"), "key": row.get("key"),
                           **{side: _item(row.get("key"), row[side]) for side in ("old", "new", "item") if side in row}}
                          for row in diff.get("items", [])[:limit]],
                "caution": "Saved captures only. Old/new item details show supported media fields; other changed fields may be omitted."}
    if name == "get_scheduler_details":
        admin(request)
        from api.schedulingAPI import sched_status

        data = sched_status(request)
        scheduler_result: dict[str, Any] = _fields(data, ("running", "busy", "enabled", "next_run_at", "next_run_iso", "last_run_at", "last_run_ok", "effective_mode", "last_pair_id", "last_job_id", "last_started_at", "last_finished_at", "warning", "interval_minutes", "mode"))
        warnings = data.get("scheduling_warnings") or []
        scheduler_result["warnings"] = [_fields(row, ("code", "message", "pair_id", "job_id", "workflow_id")) if isinstance(row, dict) else str(row)[:500] for row in warnings[:25]]
        return scheduler_result
    if name == "list_sync_reviews":
        from api.interactiveSyncAPI import reviews, get

        listing = reviews(request, user_profile=str(args.get("profile_id") or "")[:64])
        sessions = listing.get("sessions")
        rows = sessions if isinstance(sessions, list) else []
        result = []
        for row in rows[:20]:
            try:
                details = get(str(row["id"]), request)
                result.append({**_fields(row, ("id", "pair_id", "status", "planned_at")),
                               "counts": _fields(details.get("counts") or {}, ("changes", "conflicts", "attention", "selected", "removals", "not_in_library")),
                               "revision": details.get("revision")})
            except HTTPException:
                continue
        return {"total": len(rows), "reviews": result, "caution": "Only reviews owned by this caller are visible. No review is applied by this tool."}
    raise HTTPException(400, "Unknown tool")

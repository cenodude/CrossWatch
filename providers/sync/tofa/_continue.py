# /providers/sync/tofa/_continue.py
# Tofa Continue Watching row read and dismiss writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, Iterable, Mapping
from urllib.parse import quote

from providers.sync._continue_watching import SHOWN

from ._common import (
    TofaSyncError,
    as_int,
    completed_editions,
    error_of,
    iso_z,
    key_of,
    library,
    minimal_for,
    safe_json,
    tofa_request,
    watch_state,
    _info,
    _warn,
)

FEATURE = "progress"
LOG = "continue_watching"


def _row(adapter: Any) -> set[str]:
    resp = tofa_request(adapter, "GET", "/users/me/continue")
    if resp.status_code != 200:
        raise TofaSyncError(f"continue_watching_read_failed:{resp.status_code}:{error_of(resp)}")
    body = safe_json(resp)
    if not isinstance(body, list):
        raise TofaSyncError("continue_watching_read_invalid")
    return {str(row.get("media_id")) for row in body if isinstance(row, Mapping) and row.get("media_id")}


def view(adapter: Any) -> dict[str, dict[str, Any]]:
    lib = library(adapter, feature=FEATURE)
    state = watch_state(adapter, completed_only=False, feature=FEATURE)
    best: dict[tuple[str, Any], tuple[int, str | None]] = {}
    for fid, row in state.items():
        pos = as_int(row.get("position_ms")) or 0
        if row.get("completed") or pos <= 0:
            continue
        hit = lib.file_of.get(fid)
        if hit is None:
            continue
        target = (hit[0], hit[1])
        if completed_editions(lib, hit[0], hit[1], state):
            continue
        prev = best.get(target)
        if prev is None or pos > prev[0]:
            best[target] = (pos, iso_z(row.get("last_watched_at")))
    shown = _row(adapter)
    out: dict[str, dict[str, Any]] = {}
    for (kind, target), (pos, stamp) in best.items():
        item = minimal_for(lib, kind, target)
        if not item:
            continue
        media_id = str(target if kind == "movie" else target[0])
        item["progress_ms"] = pos
        if stamp:
            item["progress_at"] = stamp
        item["_tofa_media_id"] = media_id
        item[SHOWN] = media_id in shown
        key = key_of(item)
        if key:
            out[key] = item
    _info(LOG, "view_done", items=len(out), row_titles=len(shown), hidden=sum(1 for it in out.values() if it.get(SHOWN) is False))
    return out


def hide(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    rows = list(items)
    confirmed: list[str] = []
    errors = 0
    done: dict[str, bool] = {}
    for item in rows:
        media_id = str(item.get("_tofa_media_id") or "")
        if not media_id:
            errors += 1
            continue
        if media_id not in done:
            resp = tofa_request(adapter, "POST", f"/users/me/dismiss/{quote(media_id)}")
            done[media_id] = resp.status_code in (200, 204)
            if not done[media_id]:
                _warn(LOG, "hide_failed", media_id=media_id, status=resp.status_code, error=error_of(resp))
        if done[media_id]:
            confirmed.append(str(item.get("_cw_key") or ""))
        else:
            errors += 1
    return {"attempted": len(rows), "confirmed_keys": [k for k in confirmed if k], "errors": errors}

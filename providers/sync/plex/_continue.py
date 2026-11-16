# /providers/sync/plex/_continue.py
# PLEX Continue Watching hub read and remove-from-continue-watching writes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from datetime import datetime
from typing import Any, Iterable, Mapping
from urllib.parse import quote

from providers.sync._continue_watching import SHOWN

from . import _progress
from ._common import home_scope_enter, home_scope_exit, make_logger, raise_home_scope_not_applied

_dbg, _info, _warn, _error, _log = make_logger("continue_watching")


def _server(adapter: Any) -> Any:
    srv = getattr(getattr(adapter, "client", None), "server", None)
    if not srv:
        raise RuntimeError("PLEX server not bound")
    return srv


def _epoch(value: Any) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except Exception:
        pass
    try:
        return datetime.fromisoformat(str(value).replace("Z", "+00:00")).timestamp()
    except Exception:
        return None


def _rows(root: Any, reason: str) -> list[dict[str, Any]]:
    if root is None or getattr(root, "tag", None) != "MediaContainer":
        raise RuntimeError(f"plex_continue_invalid_{reason}")
    return [dict(getattr(el, "attrib", {}) or {}) for el in list(root)]


def _show_by_item(srv: Any, rating_keys: list[str]) -> dict[str, str]:
    out: dict[str, str] = {}
    for start in range(0, len(rating_keys), 50):
        batch = rating_keys[start:start + 50]
        for row in _rows(srv.query(f"/library/metadata/{','.join(batch)}"), "metadata"):
            rk = str(row.get("ratingKey") or "")
            gp = str(row.get("grandparentRatingKey") or "")
            if rk and gp:
                out[rk] = gp
    return out


def _hub(srv: Any) -> tuple[set[str], set[str]]:
    items: set[str] = set()
    shows: set[str] = set()
    for row in _rows(srv.query("/hubs/continueWatching/items"), "hub"):
        rk = str(row.get("ratingKey") or "")
        if str(row.get("type") or "").lower() == "episode" and row.get("grandparentRatingKey"):
            shows.add(str(row["grandparentRatingKey"]))
        if rk:
            items.add(rk)
    return items, shows


def _watched_after(srv: Any, show_key: str, since: float) -> bool:
    for row in _rows(srv.query(f"/library/metadata/{quote(show_key)}/allLeaves"), "leaves"):
        if not row.get("viewCount"):
            continue
        viewed = _epoch(row.get("lastViewedAt"))
        if viewed is not None and viewed > since:
            return True
    return False


def view(adapter: Any) -> dict[str, dict[str, Any]]:
    index = {str(k): dict(v) for k, v in dict(_progress.build_index(adapter)).items()}
    srv = _server(adapter)
    need_scope, did_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        if need_scope and not did_switch:
            raise_home_scope_not_applied("continue_watching", sel_aid, sel_uname)
        episodes = sorted({str((it.get("ids") or {}).get("plex")) for it in index.values()
                           if it.get("type") == "episode" and (it.get("ids") or {}).get("plex")})
        show_of = _show_by_item(srv, episodes) if episodes else {}
        items, shows = _hub(srv)
        later_watched: dict[str, bool] = {}
        for item in index.values():
            rk = str((item.get("ids") or {}).get("plex") or "")
            if not rk:
                item[SHOWN] = None
                continue
            if rk in items:
                item[SHOWN] = True
                continue
            if item.get("type") == "movie":
                item[SHOWN] = False
                continue
            show_key = show_of.get(rk)
            if not show_key:
                item[SHOWN] = None
                continue
            if show_key in shows:
                item[SHOWN] = True
                continue
            since = _epoch(item.get("progress_at"))
            if since is None:
                item[SHOWN] = None
                continue
            if show_key not in later_watched:
                later_watched[show_key] = _watched_after(srv, show_key, since)
            item[SHOWN] = None if later_watched[show_key] else False
    finally:
        home_scope_exit(adapter, did_switch)
    _info("view_done", items=len(index), row_items=len(items), row_shows=len(shows),
          hidden=sum(1 for it in index.values() if it.get(SHOWN) is False))
    return index


def hide(adapter: Any, items: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    srv = _server(adapter)
    rows = list(items)
    confirmed: list[str] = []
    errors = 0
    need_scope, did_switch, sel_aid, sel_uname = home_scope_enter(adapter)
    try:
        if need_scope and not did_switch:
            raise_home_scope_not_applied("continue_watching", sel_aid, sel_uname)
        for item in rows:
            rk = str((item.get("ids") or {}).get("plex") or "")
            if not rk:
                errors += 1
                continue
            try:
                srv.query(f"/actions/removeFromContinueWatching?ratingKey={quote(rk)}", method=srv._session.put)
            except Exception as exc:
                errors += 1
                _warn("hide_failed", provider_item_id=rk, error=str(exc)[:200])
                continue
            confirmed.append(str(item.get("_cw_key") or ""))
    finally:
        home_scope_exit(adapter, did_switch)
    return {"attempted": len(rows), "confirmed_keys": [k for k in confirmed if k], "errors": errors}

# cw_platform/event_archive/playback_recorder.py
# CrossWatch - Bounded playback session timelines and delivery outcomes
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import logging
import time
from dataclasses import replace
from types import SimpleNamespace
from typing import Any

from .db import get_conn
from .recorder import make_event, record_events
from .scrobble_recorder import _id_token, session_token

_LOG = logging.getLogger("crosswatch.event_archive")
TIMELINE_LIMIT = 50
_STATES = {"start": "running", "pause": "paused", "stop": "stopped"}


def prepare_playback(ev: Any) -> Any:
    raw = dict(ev.raw or {})
    raw["_cw_archive_item"] = {key: getattr(ev, key, None) for key in ("ids", "title", "year", "media_type", "season", "number")}
    return replace(ev, raw=raw)


def archive_event(ev: Any) -> Any:
    raw = getattr(ev, "raw", None)
    item = raw.get("_cw_archive_item") if isinstance(raw, dict) else None
    return SimpleNamespace(**{**vars(ev), **item}) if isinstance(item, dict) else ev


def record_playback(ev: Any, cfg: dict[str, Any], *, delivery: str | None = None, reason: str | None = None) -> None:
    try:
        _record(archive_event(ev), cfg, delivery=delivery, reason=reason)
    except Exception:
        _LOG.debug("playback archive update failed", exc_info=True)


def _record(ev: Any, cfg: dict[str, Any], *, delivery: str | None, reason: str | None) -> None:
    from .groups import _GROUP_LOCK, _recompute

    watch = (cfg.get("scrobble") or {}).get("watch") or {}
    source = str(watch.get("route_provider") or "").strip().upper()
    target = str(watch.get("route_sink") or "").strip().upper()
    if not source or not target or ev.media_type not in {"movie", "episode"}:
        return
    raw = ev.raw if isinstance(ev.raw, dict) else {}
    action = str(raw.get("_cw_playback_action") or ev.action)
    if action not in _STATES:
        return
    now = int(time.time())
    method = str(raw.get("_cw_activity_method") or "watcher")
    item_key = _id_token(ev.ids or {})
    if not item_key and not ev.title:
        return
    row = make_event(
        hash_extra=f"{session_token(ev.session_key, now)}|{method}|{ev.season}|{ev.number}|{ev.title if not item_key else ''}",
        domain="scrobble", event_type="scrobble_playback", feature="scrobble", operation="watch",
        severity="info", created_at=now, source_provider=source,
        source_instance=watch.get("route_provider_instance") or "default",
        destination_provider=target, destination_instance=watch.get("route_sink_instance") or "default",
        item_key=item_key, title=ev.title, year=ev.year, media_type=ev.media_type,
        season=ev.season, episode=ev.number, source_kind=method, session_key=ev.session_key,
    )
    c = get_conn()
    if c is None:
        return
    with _GROUP_LOCK, c:
        old = c.execute("SELECT id, group_id, detail FROM events WHERE event_hash=?", (row["event_hash"],)).fetchone()
        if old is None and delivery is not None:
            return
        detail = json.loads(old["detail"] or "{}") if old else {}
        history = detail.get("playback") or []
        if delivery is not None:
            outcome: dict[str, str | int] = {"status": delivery, "action": action}
            if reason:
                outcome["reason"] = str(reason)[:200]
            previous = detail.get("delivery") or {}
            if all(previous.get(k) == v for k, v in outcome.items()) and set(previous) - {"at"} == set(outcome):
                return
            outcome["at"] = now
            detail["delivery"] = outcome
        else:
            state = _STATES[action]
            if detail.get("state") == state or (detail.get("state") == "stopped" and action == "pause"):
                return
            label = "resume" if action == "start" and history else action
            progress = max(0, min(100, int(ev.progress or 0)))
            history.append([now, label, progress])
            if len(history) > TIMELINE_LIMIT + 1:
                removed = len(history) - TIMELINE_LIMIT - 1
                history = history[:1] + history[-TIMELINE_LIMIT:]
                detail["omitted"] = int(detail.get("omitted") or 0) + removed
            detail.update(playback=history, state=state, progress=progress)
            if ev.account:
                detail["account"] = str(ev.account)
        row["detail"] = json.dumps(detail, ensure_ascii=False, separators=(",", ":"))
        if old is None:
            record_events([row], conn=c)
        else:
            c.execute("UPDATE events SET detail=?, created_at=? WHERE id=?", (row["detail"], now, old["id"]))
            if old["group_id"] is not None:
                _recompute(c, int(old["group_id"]), now)

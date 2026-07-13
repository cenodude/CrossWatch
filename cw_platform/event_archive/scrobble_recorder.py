# cw_platform/event_archive/scrobble_recorder.py
# CrossWatch - Scrobble & rating archive recording
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import threading
import time
from collections import OrderedDict
from collections.abc import Mapping
from typing import Any

from .db import get_conn
from .recorder import make_event, record_events

_LIVE_TYPE = "scrobble_started"

_SEEN: "OrderedDict[str, None]" = OrderedDict()
_SEEN_CAP = 4096
_SEEN_LOCK = threading.Lock()

_ID_ORDER = ("imdb", "tmdb", "tvdb", "trakt", "simkl")
_SHOW_ID_ORDER = ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show")


def _P(v: Any) -> str:
    return str(v or "").strip().upper()


def _int(v: Any) -> int | None:
    try:
        return int(v) if v not in (None, "") else None
    except Exception:
        return None


def session_token(session_key: Any, ts: Any) -> str:
    sk = str(session_key or "").strip()
    if sk:
        return f"s:{sk}"
    return "d:" + time.strftime("%Y%j", time.localtime(int(ts or 0)))


def _id_token(ids: Mapping[str, Any]) -> str | None:
    for k in _ID_ORDER:
        v = ids.get(k)
        if v not in (None, ""):
            return f"{k}:{v}"
    for k in _SHOW_ID_ORDER:
        v = ids.get(k)
        if v not in (None, ""):
            return f"{k.split('_')[0]}:{v}"
    return None


def _seen(event_hash: str) -> bool:
    with _SEEN_LOCK:
        if event_hash in _SEEN:
            _SEEN.move_to_end(event_hash)
            return True
        _SEEN[event_hash] = None
        if len(_SEEN) > _SEEN_CAP:
            _SEEN.popitem(last=False)
        return False


def record(
    *,
    event_type: str,
    source_provider: str,
    destination_provider: str,
    source_instance: str = "default",
    destination_instance: str = "default",
    ids: Mapping[str, Any] | None = None,
    media_type: str = "",
    title: str | None = None,
    year: Any = None,
    season: Any = None,
    episode: Any = None,
    account: Any = None,
    progress: Any = None,
    rating: Any = None,
    session_key: Any = None,
    reason_code: str | None = None,
    reason: str | None = None,
    feature: str = "scrobble",
    operation: str = "watch",
    severity: str = "info",
    source_kind: str = "runtime",
    created_at: int | None = None,
) -> None:
    ids = ids if isinstance(ids, Mapping) else {}
    ts = int(created_at or time.time())
    sk = str(session_key).strip() if session_key not in (None, "") else None
    hx = f"{session_token(sk, ts)}|{source_kind}|{_int(season)}|{_int(episode)}"
    item_key = _id_token(ids)
    detail: dict[str, Any] = {}
    if account not in (None, ""):
        detail["account"] = str(account)
    p = _int(progress)
    if p is not None:
        detail["progress"] = p
    if rating not in (None, ""):
        detail["rating"] = _int(rating)
    if session_key not in (None, ""):
        detail["session_key"] = str(session_key)

    row = make_event(
        hash_extra=hx,
        domain="scrobble",
        created_at=ts,
        event_type=event_type,
        severity=severity,
        feature=feature,
        operation=operation,
        source_provider=_P(source_provider) or None,
        source_instance=source_instance or None,
        destination_provider=_P(destination_provider) or None,
        destination_instance=destination_instance or None,
        item_key=item_key,
        title=str(title).strip() if title else None,
        year=_int(year),
        media_type=(media_type or "").strip().lower() or None,
        season=_int(season),
        episode=_int(episode),
        reason_code=reason_code,
        reason=reason,
        match_basis=item_key,
        source_kind=source_kind or "runtime",
        session_key=sk,
        detail=detail or None,
    )
    if event_type == _LIVE_TYPE:
        _upsert_live(row, p)
        return
    if _seen(row["event_hash"]):
        return
    record_events([row])


def _upsert_live(row: dict[str, Any], progress: int | None) -> None:
    c = get_conn()
    if c is None:
        return
    try:
        ex = c.execute("SELECT id, group_id, detail FROM events WHERE event_hash=?", (row.get("event_hash"),)).fetchone()
    except Exception:
        return
    if ex is None:
        record_events([row])
        return
    if progress is None:
        return
    try:
        d = json.loads(ex["detail"] or "{}")
        if not isinstance(d, dict):
            d = {}
    except Exception:
        d = {}
    old = _int(d.get("progress"))
    if old is not None and progress <= old:
        return
    d["progress"] = progress
    gid = ex["group_id"]
    try:
        with c:
            c.execute("UPDATE events SET detail=? WHERE id=?", (json.dumps(d, ensure_ascii=False, sort_keys=True)[:4000], ex["id"]))
        if gid is not None:
            from .groups import _recompute
            with c:
                _recompute(c, int(gid), int(time.time()))
    except Exception:
        pass


_WATCH_TYPE = {"start": "scrobble_started", "stop": "scrobble_completed"}


def record_watch(
    ev: Any,
    *,
    action: str,
    source_provider: str,
    destination_provider: str,
    source_instance: str = "default",
    destination_instance: str = "default",
    status: str = "ok",
    progress: Any = None,
    reason: str | None = None,
) -> None:
    if status == "ok":
        event_type = _WATCH_TYPE.get(action)
        if not event_type:
            return
        severity = "info"
    else:
        event_type = "scrobble_failed"
        severity = "error"
    try:
        raw = getattr(ev, "raw", None)
        source_kind = "watcher"
        if isinstance(raw, Mapping):
            source_kind = str(raw.get("_cw_activity_method") or source_kind).strip().lower() or source_kind
        record(
            event_type=event_type,
            severity=severity,
            source_kind=source_kind,
            source_provider=source_provider,
            source_instance=source_instance,
            destination_provider=destination_provider,
            destination_instance=destination_instance,
            ids=getattr(ev, "ids", None) or {},
            media_type=getattr(ev, "media_type", "") or "",
            title=getattr(ev, "title", None),
            year=getattr(ev, "year", None),
            season=getattr(ev, "season", None),
            episode=getattr(ev, "number", None),
            account=getattr(ev, "account", None),
            progress=progress if progress is not None else getattr(ev, "progress", None),
            session_key=getattr(ev, "session_key", None),
            reason_code=reason,
            reason=reason,
        )
    except Exception:
        pass


def record_webhook(
    *,
    event_type: str,
    source_provider: str,
    destination_provider: str,
    media_type: str,
    md: Mapping[str, Any] | None = None,
    ids: Mapping[str, Any] | None = None,
    account: Any = None,
    progress: Any = None,
    rating: Any = None,
    session_key: Any = None,
    reason: str | None = None,
    feature: str = "scrobble",
    operation: str = "watch",
    destination_instance: str = "default",
    title: Any = None,
    year: Any = None,
    season: Any = None,
    episode: Any = None,
) -> None:
    md = md if isinstance(md, Mapping) else {}
    mt = (media_type or "").strip().lower()
    if title is None:
        title = (md.get("grandparentTitle") or md.get("title")) if mt == "episode" else (md.get("title") or md.get("grandparentTitle"))
    if year is None:
        year = md.get("year")
    if season is None and mt == "episode":
        season = md.get("parentIndex")
    if episode is None and mt == "episode":
        episode = md.get("index")
    severity = "error" if event_type in ("scrobble_failed", "rating_failed") else "info"
    try:
        record(
            event_type=event_type,
            severity=severity,
            feature=feature,
            operation=operation,
            source_kind="webhook",
            source_provider=source_provider,
            destination_provider=destination_provider,
            destination_instance=destination_instance,
            ids=ids or {},
            media_type=mt,
            title=title,
            year=year,
            season=season if mt == "episode" else None,
            episode=episode if mt == "episode" else None,
            account=account,
            progress=progress,
            rating=rating,
            session_key=session_key,
            reason_code=reason,
            reason=reason,
        )
    except Exception:
        pass

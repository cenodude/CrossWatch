# providers/scrobble/manual_marks.py
# CrossWatch - Manual played-mark detection for media server watchers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import threading
from datetime import datetime, timezone
from typing import Any, Iterable

from cw_platform.played_writes import written_by_crosswatch
from providers.scrobble.routes import watch_poll_seconds
from providers.scrobble.scrobble import ScrobbleEvent, mask_account

DEFAULT_INTERVAL_SECONDS = 30.0
MAX_INTERVAL_SECONDS = 30.0
CLOCK_SLACK_SECONDS = 120.0
SESSION_TTL_SECONDS = 6 * 3600.0
MAX_MARKS_PER_POLL = 100
FAILURE_WARN_AFTER = 3
USERS_LEDGER = "#users"
FUTURE_WARN_EVERY_SECONDS = 3600.0

_FRACTION = re.compile(r"(\.\d{6})\d+")


def interval_seconds(cfg: Any) -> float:
    return min(watch_poll_seconds(cfg, DEFAULT_INTERVAL_SECONDS, "idle_poll_seconds"), MAX_INTERVAL_SECONDS)


def route_allows(cfg: Any) -> bool:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, dict) else {}
    options = watch.get("route_options") if isinstance(watch, dict) else None
    route_watch = options.get("watch") if isinstance(options, dict) else None
    if not isinstance(route_watch, dict):
        return True
    return route_watch.get("manual_marks", True) is not False


def is_manual_mark(raw: Any) -> bool:
    return isinstance(raw, dict) and bool(raw.get("_cw_manual_mark"))


def parse_epoch(value: Any) -> float | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, (int, float)):
        return float(value) if value > 0 else None
    text = str(value).strip()
    if not text:
        return None
    if text.isdigit():
        return float(text) if int(text) > 0 else None
    text = _FRACTION.sub(r"\1", text.replace("Z", "+00:00"))
    try:
        dt = datetime.fromisoformat(text)
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.timestamp()


def mark_label(ev: ScrobbleEvent) -> str:
    base = ev.title or "?"
    if ev.media_type == "episode" and isinstance(ev.season, int) and isinstance(ev.number, int):
        return f"{base} S{ev.season:02}E{ev.number:02}"
    return base


def wants_marks(dispatch: Any) -> bool:
    fn = getattr(dispatch, "wants_manual_marks", None)
    if not callable(fn):
        return False
    try:
        return bool(fn())
    except Exception:
        return False


def log_read_failure(tracker: "MarkTracker", user: str, who: str, exc: Exception, log: Any) -> None:
    if tracker.read_failed(user):
        log(f"manual marks: reading played items failed {FAILURE_WARN_AFTER} times in a row for {who}: {exc}", "WARNING")
    else:
        log(f"manual marks: reading played items failed for {who}: {exc}", "DEBUG")


def log_read_health(tracker: "MarkTracker", user: str, who: str, now: float, log: Any) -> None:
    if tracker.read_ok(user):
        log(f"manual marks: reading played items works again for {who}", "INFO")
    future = tracker.future_warning(user, now)
    if future:
        log(f"manual marks: {future} played items for {who} are dated more than 2 minutes ahead of this machine; check the server clock", "WARNING")


class MarkTracker:
    def __init__(self) -> None:
        self._next_poll = 0.0
        self._watermarks: dict[str, float] = {}
        self._edges: dict[str, set[str] | None] = {}
        self._last_poll: dict[str, float] = {}
        self._sessions: dict[str, float] = {}
        self._future: dict[str, int] = {}
        self._failures: dict[str, int] = {}
        self._future_warned: dict[str, float] = {}
        self._lock = threading.Lock()

    def due(self, cfg: Any, now: float) -> bool:
        if now < self._next_poll:
            return False
        self._next_poll = now + interval_seconds(cfg)
        return True

    def seconds_until_due(self, now: float) -> float:
        return max(0.5, self._next_poll - now)

    def note_session(self, key: str, now: float) -> None:
        if not key:
            return
        with self._lock:
            if len(self._sessions) > 4096:
                for k, ts in list(self._sessions.items()):
                    if now - ts > SESSION_TTL_SECONDS:
                        self._sessions.pop(k, None)
            self._sessions[key] = now

    def seen_in_session(self, key: str, now: float) -> bool:
        with self._lock:
            ts = self._sessions.get(key)
        return ts is not None and now - ts <= SESSION_TTL_SECONDS

    def read_failed(self, user: str) -> bool:
        count = self._failures.get(user, 0) + 1
        self._failures[user] = count
        return count == FAILURE_WARN_AFTER

    def read_ok(self, user: str) -> bool:
        return self._failures.pop(user, 0) >= FAILURE_WARN_AFTER

    def future_warning(self, user: str, now: float) -> int:
        count = self._future.get(user, 0)
        if not count or now - self._future_warned.get(user, 0.0) < FUTURE_WARN_EVERY_SECONDS:
            return 0
        self._future_warned[user] = now
        return count

    def watermark(self, user: str) -> float | None:
        return self._watermarks.get(user)

    def fresh(self, user: str, rows: Iterable[tuple[str, Any, float | None]], now: float, slack: float) -> list[Any]:
        ceiling = now + CLOCK_SLACK_SECONDS
        listed = [(key, item, ts) for key, item, ts in rows if key and ts is not None]
        dated = [(key, item, ts) for key, item, ts in listed if ts <= ceiling]
        self._future[user] = len(listed) - len(dated)
        top = max((ts for _, _, ts in dated), default=0.0)
        mark = self._watermarks.get(user)
        last_poll = self._last_poll.get(user)
        self._last_poll[user] = now
        if mark is None or last_poll is None:
            self._watermarks[user] = top
            self._edges[user] = None
            return []
        edge = self._edges.get(user)
        floor = last_poll - slack
        found = [
            (item, ts) for key, item, ts in dated
            if (ts > mark or (ts == mark and edge is not None and key not in edge)) and ts >= floor
        ]
        if top > mark:
            self._watermarks[user] = top
            self._edges[user] = {key for key, _, ts in dated if ts == top}
        elif edge is not None:
            self._edges[user] = edge | {key for key, _, ts in dated if ts == mark}
        found.sort(key=lambda pair: pair[1])
        return [item for item, _ in found]


class MediaServerMarkPoller:
    _MARK_PROVIDER = ""
    _MARK_FIELDS = "ProviderIds,Path,ParentId"
    _MARK_PAGE = 50
    _MARK_MAX_PAGES = 4
    _MARK_USERS_TTL = 600.0

    _marks: MarkTracker
    _mark_users_cache: tuple[float, list[tuple[str, str]]]
    _dispatch: Any
    _server_id: str | None
    _allowed_sessions: set[str]
    _scrobble_whitelist_sessions: set[str]

    def _mark_get(self, path: str, cfg: dict[str, Any]) -> Any:
        raise NotImplementedError

    def _log(self, msg: str, level: str = "INFO") -> None:
        raise NotImplementedError

    def _build_mark_event(self, sess: dict[str, Any], cfg: dict[str, Any]) -> ScrobbleEvent | None:
        raise NotImplementedError

    def _passes_filters(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> bool:
        raise NotImplementedError

    def _note_mark_session(self, sess: dict[str, Any], now: float) -> None:
        item = sess.get("NowPlayingItem") or {}
        uid = str(sess.get("UserId") or "").strip().lower()
        iid = str(item.get("Id") or "").strip() if isinstance(item, dict) else ""
        if uid and iid:
            self._marks.note_session(f"{uid}:{iid}", now)

    def _mark_user_rows(self, cfg: dict[str, Any]) -> tuple[list[Any], Exception | None]:
        failure: Exception | None = None
        try:
            rows = self._mark_get("/Users", cfg)
            if isinstance(rows, list) and rows:
                return rows, None
        except Exception as exc:
            failure = exc
        own = str((cfg.get(self._MARK_PROVIDER) or {}).get("user_id") or "").strip()
        if not own:
            if failure is not None:
                raise failure
            return [], None
        row = self._mark_get(f"/Users/{own}", cfg)
        return ([row] if isinstance(row, dict) else []), failure

    def _mark_users(self, cfg: dict[str, Any], now: float) -> list[tuple[str, str]]:
        ts, users = self._mark_users_cache
        if not users or now - ts >= self._MARK_USERS_TTL:
            rows: list[Any] | None
            try:
                rows, failure = self._mark_user_rows(cfg)
            except Exception as exc:
                rows, failure = None, exc
            if failure is not None:
                log_read_failure(self._marks, USERS_LEDGER, "the user list", failure, self._log)
            else:
                log_read_health(self._marks, USERS_LEDGER, "the user list", now, self._log)
            if rows is not None and (failure is None or not users):
                users = []
                for row in rows:
                    if not isinstance(row, dict) or (row.get("Policy") or {}).get("IsDisabled"):
                        continue
                    uid = str(row.get("Id") or "").strip()
                    if uid:
                        users.append((uid, str(row.get("Name") or "").strip()))
                self._mark_users_cache = (now if failure is None else 0.0, users)
        accepts = getattr(self._dispatch, "accepts_user", None)
        out: list[tuple[str, str]] = []
        for uid, name in users:
            probe = ScrobbleEvent(
                action="stop",
                media_type="movie",
                ids={},
                title=None,
                year=None,
                season=None,
                number=None,
                progress=100,
                account=name or None,
                server_uuid=self._server_id,
                session_key=None,
                raw={"UserId": uid, "UserName": name, "_cw_manual_mark": True},
            )
            try:
                allowed = bool(accepts(probe)) if callable(accepts) else True
            except Exception:
                allowed = True
            if allowed:
                out.append((uid, name))
        return out

    def _mark_played_rows(self, uid: str, cfg: dict[str, Any], watermark: float | None) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for page in range(self._MARK_MAX_PAGES):
            path = (
                f"/Users/{uid}/Items?IncludeItemTypes=Movie,Episode&Recursive=true&Filters=IsPlayed"
                f"&SortBy=DatePlayed&SortOrder=Descending&StartIndex={page * self._MARK_PAGE}&Limit={self._MARK_PAGE}"
                f"&Fields={self._MARK_FIELDS}&EnableUserData=true&EnableImages=false&EnableTotalRecordCount=false"
            )
            body = self._mark_get(path, cfg)
            items = body.get("Items") if isinstance(body, dict) else None
            if not isinstance(items, list):
                raise ValueError("invalid played items response")
            rows.extend(item for item in items if isinstance(item, dict))
            if watermark is None or len(items) < self._MARK_PAGE:
                break
            last = items[-1] if isinstance(items[-1], dict) else {}
            oldest = parse_epoch((last.get("UserData") or {}).get("LastPlayedDate"))
            if oldest is None or oldest < watermark:
                break
        return rows

    def _emit_manual_mark(self, row: dict[str, Any], uid: str, name: str, cfg: dict[str, Any]) -> bool | None:
        played = str((row.get("UserData") or {}).get("LastPlayedDate") or "").strip()
        sk = f"mark:{uid}:{row.get('Id')}:{played}"
        sess = {"Id": sk, "UserId": uid, "UserName": name, "NowPlayingItem": row, "PlayState": {}}
        ev = self._build_mark_event(sess, cfg)
        if ev is None:
            return None
        raw = dict(ev.raw or {})
        raw["_cw_manual_mark"] = True
        raw["_cw_preserve_stop"] = True
        ev = ScrobbleEvent(**{**ev.__dict__, "raw": raw})
        try:
            if not self._passes_filters(ev, cfg):
                return None
            if not self._dispatch.dispatch(ev):
                return False
        finally:
            self._allowed_sessions.discard(sk)
            self._scrobble_whitelist_sessions.discard(sk)
        self._log(f"manual mark {ev.media_type} user={mask_account(ev.account)} media='{mark_label(ev)}'")
        return True

    def _poll_manual_marks(self, cfg: dict[str, Any], now: float) -> None:
        try:
            if not self._marks.due(cfg, now) or not wants_marks(self._dispatch):
                return
            slack = interval_seconds(cfg) + CLOCK_SLACK_SECONDS
            for uid, name in self._mark_users(cfg, now):
                key = uid.lower()
                who = f"user={mask_account(name)}"
                try:
                    rows = self._mark_played_rows(uid, cfg, self._marks.watermark(key))
                except Exception as exc:
                    log_read_failure(self._marks, key, who, exc, self._log)
                    continue
                dated = [
                    (str(row.get("Id") or "").strip(), row, parse_epoch((row.get("UserData") or {}).get("LastPlayedDate")))
                    for row in rows
                ]
                fresh = self._marks.fresh(key, dated, now, slack)
                log_read_health(self._marks, key, who, now, self._log)
                admitted = 0
                for row in fresh:
                    iid = str(row.get("Id") or "").strip()
                    if not iid or self._marks.seen_in_session(f"{key}:{iid}", now):
                        continue
                    played_at = parse_epoch((row.get("UserData") or {}).get("LastPlayedDate"))
                    if written_by_crosswatch(self._MARK_PROVIDER, uid, iid, played_at, now):
                        continue
                    if int((row.get("UserData") or {}).get("PlaybackPositionTicks") or 0) > 0:
                        continue
                    if admitted >= MAX_MARKS_PER_POLL:
                        self._log(f"manual marks: more than {MAX_MARKS_PER_POLL} marks for user={mask_account(name)}; the rest is left to sync", "WARNING")
                        break
                    if self._emit_manual_mark(row, uid, name, cfg) is not None:
                        admitted += 1
        except Exception as exc:
            self._log(f"manual marks poll failed: {exc}", "DEBUG")

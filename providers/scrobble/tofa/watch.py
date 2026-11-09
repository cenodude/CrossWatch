# providers/scrobble/tofa/watch.py
# CrossWatch - Tofa playback session watcher
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import hashlib
import json
import ssl
import threading
import time
from collections.abc import Callable, Mapping
from types import SimpleNamespace
from typing import Any, cast
from urllib.parse import quote, urlsplit, urlunsplit

import requests
import websocket

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from cw_platform.app_version import user_agent as http_user_agent
from cw_platform.config_base import load_config
from cw_platform.played_writes import written_by_crosswatch
from cw_platform.provider_instances import list_sync_user_ids, normalize_instance_id
from providers.auth._auth_TOFA import TofaClient, TofaError, is_configured, normalize_server_url, provider_block, verify_ssl_of
from providers.scrobble.currently_watching import update_from_event as _cw_update
from providers.scrobble.currently_watching import update_from_payload as _cw_update_payload
from providers.scrobble.manual_marks import (
    MAX_MARKS_PER_POLL,
    MarkTracker,
    log_read_failure,
    log_read_health,
    mark_label,
    parse_epoch,
    wants_marks,
)
from providers.scrobble.scrobble import Dispatcher, ScrobbleEvent, ScrobbleSink, mask_account
from providers.scrobble.sources import source_enabled
from providers.sync.tofa._common import (
    as_int,
    durations,
    editions_of,
    forget_library,
    library,
    minimal_for,
    safe_json,
    selected_libraries,
    tofa_request,
)
from providers.scrobble.routes import progress_step

RECV_TIMEOUT_SECONDS = 5.0
OFFLINE_INITIAL_RETRY_SECONDS = 15.0
OFFLINE_MAX_RETRY_SECONDS = 300.0
PROFILE_LOCKED_RETRY_SECONDS = 600.0
SEEK_JUMP_PERCENT = 10.0
STALE_PLAYING_SECONDS = 15 * 60.0
STALE_PAUSED_SECONDS = 6 * 3600.0
COMPLETION_SLACK_SECONDS = 60.0
COMPLETION_RECHECK_SECONDS = 5.0
COMPLETION_RECHECK_MIN_PROGRESS = 50.0
FEED_PAGE = 200
FEED_MAX_PAGES = 5
EMITTED_TTL_SECONDS = 6 * 3600.0


class _MarkPending(Exception):
    pass
WS_PATH = "/api/v1/ws"
STREAM_EVENTS = {"StreamStarted", "StreamProgress", "StreamStopped"}


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="TOFA-WATCH")
            return
        except Exception:
            pass
    try:
        print(f"[TOFA-WATCH:{lvl}] {msg}")
    except Exception:
        pass


def _float(value: Any) -> float | None:
    try:
        return float(value)
    except Exception:
        return None


def server_uuid_for(server: str) -> str:
    base = normalize_server_url(server)
    return "tofa:" + hashlib.sha1(base.encode("utf-8", "ignore")).hexdigest()[:16] if base else "tofa"


def ws_url_for(server: str) -> str:
    base = normalize_server_url(server)
    if not base:
        return ""
    parts = urlsplit(base)
    scheme = "wss" if parts.scheme == "https" else "ws"
    return urlunsplit((scheme, parts.netloc, parts.path.rstrip("/") + WS_PATH, "", ""))


class TofaWatchService:
    def __init__(
        self,
        sinks: list[ScrobbleSink] | None = None,
        *,
        dispatcher: Any | None = None,
        cfg_provider: Callable[[], dict[str, Any]] | None = None,
        instance_id: Any = "default",
        quiet_startup: bool = False,
    ) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._dispatch = dispatcher or Dispatcher(list(sinks or []), cfg_provider=self._active_cfg)
        self._stop = threading.Event()
        self._bg: threading.Thread | None = None
        self._ws: Any = None
        self._sessions: dict[str, dict[str, Any]] = {}
        self._skipped: set[str] = set()
        self._quiet_startup = bool(quiet_startup)
        self._offline = False
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
        self._last_error: tuple[str, float] = ("", 0.0)
        self._marks = MarkTracker()
        self._mark_cursors: dict[str, str] = {}
        self._emitted: dict[str, float] = {}
        self._mark_retry: dict[str, dict[str, tuple[dict[str, Any], float]]] = {}
        self._last_housekeeping = 0.0
        self.session = requests.Session()
        try:
            self.session.headers.setdefault("Accept", "application/json")
            self.session.headers["User-Agent"] = http_user_agent("Watcher", override_env="CW_TOFA_UA")
        except Exception:
            pass
        lvl = "DEBUG" if self._quiet_startup else "INFO"
        _log(f"Ensuring Watcher is running; inst={self._instance_id} | wired sinks: {self.sinks_count()}", lvl)

    def sinks_count(self) -> int:
        try:
            sc = getattr(self._dispatch, "sinks_count", None)
            if callable(sc):
                return int(cast(Any, sc()))
            return len(getattr(self._dispatch, "_dispatchers", None) or getattr(self._dispatch, "_sinks", None) or [])
        except Exception:
            return 0

    @property
    def config(self) -> dict[str, Any]:
        return self._active_cfg()

    @property
    def instance_id(self) -> str:
        return self._instance_id

    def _active_cfg(self) -> dict[str, Any]:
        if self._cfg_provider:
            try:
                cfg = self._cfg_provider() or {}
                return cfg if isinstance(cfg, dict) else {}
            except Exception:
                return {}
        try:
            cfg = load_config() or {}
            return cfg if isinstance(cfg, dict) else {}
        except Exception:
            return {}

    def _block(self, cfg: Mapping[str, Any] | None = None) -> dict[str, Any]:
        return provider_block(cfg if cfg is not None else self._active_cfg(), self._instance_id)

    def _log_error_limited(self, key: str, message: str) -> None:
        now = time.time()
        last_key, last_ts = self._last_error
        if key != last_key or (now - last_ts) >= 30.0:
            _log(message, "WARNING")
            self._last_error = (key, now)

    def _mark_offline(self, reason: str) -> None:
        if not self._offline:
            self._offline = True
            self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
            _log(f"Tofa watcher offline: {reason}; retrying with backoff", "WARNING")
            return
        self._offline_retry = min(OFFLINE_MAX_RETRY_SECONDS, max(OFFLINE_INITIAL_RETRY_SECONDS, self._offline_retry * 2.0))

    def _mark_online(self) -> None:
        if self._offline:
            _log("Tofa watcher reconnected", "INFO")
        self._offline = False
        self._offline_retry = OFFLINE_INITIAL_RETRY_SECONDS
        self._last_error = ("", 0.0)

    def _scrobble_setting(self, key: str, default: float) -> float:
        cfg = self._active_cfg()
        try:
            return float((((cfg.get("scrobble") or {}).get("trakt") or {}).get(key) or default))
        except Exception:
            return default

    def _client(self) -> TofaClient:
        return TofaClient(self._active_cfg(), instance_id=self._instance_id)

    def _wants_users(self) -> bool:
        needs = getattr(self._dispatch, "needs_user_resolution", None)
        try:
            return bool(needs()) if callable(needs) else False
        except Exception:
            return False

    def _profiles(self, cfg: Mapping[str, Any]) -> list[tuple[str, dict[str, Any]]]:
        found = [(self._instance_id, self._block(cfg))]
        if self._wants_users():
            found.extend((user_inst, provider_block(cfg, user_inst)) for user_inst in list_sync_user_ids(cfg, "tofa", self._instance_id))
        out: list[tuple[str, dict[str, Any]]] = []
        seen: set[str] = set()
        for inst, block in found:
            pid = str(block.get("profile_id") or "").strip()
            if pid and pid not in seen:
                seen.add(pid)
                out.append((inst, block))
        return out

    def _instance_for(self, event: Mapping[str, Any], cfg: Mapping[str, Any]) -> str:
        pid = str(event.get("profile_id") or "").strip()
        profiles = self._profiles(cfg)
        if pid:
            return next((inst for inst, block in profiles if str(block.get("profile_id") or "") == pid), "")
        uid = str(event.get("user_id") or "").strip()
        if not uid:
            return ""
        return next((inst for inst, block in profiles if uid == str(block.get("user_id") or "") and block.get("profile_primary")), "")

    def _reader(self, inst: str | None = None) -> Any:
        target = normalize_instance_id(inst) if inst else self._instance_id
        if target == self._instance_id:
            return self
        return SimpleNamespace(config=self._active_cfg(), instance_id=target, session=self.session)

    def _file_hit(self, file_id: str, inst: str | None = None) -> tuple[Any, tuple[str, Any, str] | None]:
        reader = self._reader(inst)
        lib = library(reader)
        hit = lib.file_of.get(file_id)
        if hit is None:
            forget_library(reader)
            lib = library(reader)
            hit = lib.file_of.get(file_id)
        return lib, hit

    def _outside_whitelist(self, file_id: str, inst: str | None = None) -> bool:
        reader = self._reader(inst)
        allowed = selected_libraries(reader, "scrobble")
        if not allowed:
            return False
        return file_id not in library(reader, libraries=allowed).file_of

    def _metadata(self, lib: Any, hit: tuple[str, Any, str]) -> dict[str, Any] | None:
        kind, target, _edition = hit
        item = minimal_for(lib, kind, target)
        if not item:
            return None
        if kind == "movie":
            return {"media_type": "movie", "ids": dict(item["ids"]), "title": item.get("title"), "year": None, "season": None, "number": None}
        ids = {f"{k}_show": v for k, v in dict(item.get("show_ids") or {}).items()}
        tmdb_episode = (lib.episodes.get(target) or {}).get("tmdb_episode_id")
        if tmdb_episode:
            ids["tmdb_episode"] = str(tmdb_episode)
        return {"media_type": "episode", "ids": ids, "title": item.get("series_title"), "year": None,
                "season": item.get("season"), "number": item.get("episode")}

    def _event(self, session: Mapping[str, Any], action: str, progress: float) -> ScrobbleEvent:
        meta = dict(session.get("meta") or {})
        duration_ms = as_int(session.get("duration_ms"))
        position_ms = as_int(session.get("position_ms"))
        raw = {
            "provider": "tofa",
            "provider_instance": self._instance_id,
            "session_key": session.get("source_session_key"),
            "content_id": session.get("file_id"),
            "profile_id": session.get("profile_id"),
            "_cw_session_identity": {"user_id": session.get("profile_id"), "name": session.get("account")},
            "_cw_seek": bool(session.get("seek_pending")),
            "_cw_preserve_stop": bool(action == "stop" and progress >= self._scrobble_setting("force_stop_at", 95.0)),
        }
        return ScrobbleEvent(
            action=action,  # type: ignore[arg-type]
            media_type=str(meta.get("media_type") or "movie"),  # type: ignore[arg-type]
            ids=dict(meta.get("ids") or {}),
            title=meta.get("title"),
            year=meta.get("year"),
            season=meta.get("season"),
            number=meta.get("number"),
            progress=max(0.0, min(100.0, float(progress or 0.0))),
            account=session.get("account"),
            server_uuid=session.get("server_uuid"),
            session_key=str(session.get("session_key") or ""),
            raw=raw,
            position_ms=position_ms,
            duration_ms=duration_ms if duration_ms and duration_ms > 0 else None,
        )

    def _dispatch_event(self, ev: ScrobbleEvent, duration_ms: int | None = None) -> bool:
        accepted = bool(self._dispatch.dispatch(ev))
        if accepted:
            try:
                _cw_update("tofa", ev, duration_ms=duration_ms, provider_instance=self._instance_id)
            except Exception:
                pass
            _log(f"event {ev.action} {ev.media_type} user={mask_account(ev.account)} p={ev.progress:.1f} sess={ev.session_key}", "DEBUG")
        elif ev.action == "stop":
            try:
                _cw_update_payload(
                    "tofa", ev.media_type, ev.title or "", ev.year, ev.season, ev.number, ev.progress, True,
                    state="stopped", clear_on_stop=True, ids=ev.ids, session_key=ev.session_key, provider_instance=self._instance_id,
                )
            except Exception:
                pass
        return accepted

    def _create_session(self, event: Mapping[str, Any], inst: str) -> dict[str, Any] | None:
        sid = str(event.get("session_id") or "").strip()
        fid = str(event.get("media_file_id") or "").strip()
        if not sid or not fid:
            return None
        if self._outside_whitelist(fid, inst):
            if sid not in self._skipped:
                self._skipped.add(sid)
                _log(f"Tofa session outside whitelisted libraries skipped file={fid}", "DEBUG")
            return None
        lib, hit = self._file_hit(fid, inst)
        meta = self._metadata(lib, hit) if hit else None
        if not hit or not meta:
            if sid not in self._skipped:
                self._skipped.add(sid)
                _log(f"Tofa session skipped file={fid} title='{event.get('media_title') or ''}': no usable ids or episode numbers", "DEBUG")
            return None
        duration_ms: int | None = None
        try:
            duration_ms = durations(self._reader(inst), lib, [(hit[0], hit[1])], feature="watch").get(fid)
        except Exception as exc:
            _log(f"Tofa duration lookup failed file={fid}: {exc}", "DEBUG")
        now = time.time()
        server = str(self._block().get("server_url") or "")
        session = {
            "session_key": f"tofa:{self._instance_id}:{sid}:{fid}",
            "source_session_key": sid,
            "file_id": fid,
            "instance": inst,
            "profile_id": str(event.get("profile_id") or provider_block(self._active_cfg(), inst).get("profile_id") or ""),
            "meta": meta,
            "account": str(event.get("profile_name") or "").strip() or None,
            "server_uuid": server_uuid_for(server),
            "state": "",
            "last_action": "",
            "last_progress": 0.0,
            "last_emitted_progress": None,
            "position_ms": None,
            "duration_ms": duration_ms,
            "seek_pending": False,
            "started_at": now,
            "last_event_at": now,
        }
        self._sessions[sid] = session
        self._marks.note_session(f"{session['profile_id']}:{fid}", now)
        return session

    def _on_started(self, event: Mapping[str, Any], inst: str) -> None:
        sid = str(event.get("session_id") or "").strip()
        current = self._sessions.get(sid)
        if current is not None:
            if current.get("file_id") == str(event.get("media_file_id") or ""):
                current["last_event_at"] = time.time()
                return
            self._stop_session(sid)
        self._create_session(event, inst)

    def _on_progress(self, event: Mapping[str, Any]) -> None:
        session = self._sessions.get(str(event.get("session_id") or "").strip())
        if session is None:
            return
        pct = _float(event.get("progress_pct"))
        ticks = as_int(event.get("position_ticks"))
        session["last_event_at"] = time.time()
        if pct is None:
            return
        percent = max(0.0, min(100.0, pct))
        session["last_progress"] = percent
        if ticks is not None and ticks >= 0:
            session["position_ms"] = ticks // 10000
            if not session.get("duration_ms") and percent >= 1.0:
                session["duration_ms"] = int(round(session["position_ms"] * 100.0 / percent))
        self._marks.note_session(f"{session.get('profile_id')}:{session.get('file_id')}", time.time())
        state = "paused" if event.get("is_paused") is True else "playing"
        prev_state = str(session.get("state") or "")
        prev_action = str(session.get("last_action") or "")
        prev_progress = session.get("last_emitted_progress")
        emit_action: str | None = None
        if not prev_action:
            emit_action = "start"
        elif state == "paused" and prev_state != "paused":
            emit_action = "pause"
        elif state == "playing" and prev_state == "paused":
            emit_action = "start"
        elif state == "playing" and prev_action == "start":
            try:
                last_emit = float(prev_progress if prev_progress is not None else percent)
            except Exception:
                last_emit = percent
            jump = abs(percent - last_emit)
            if jump >= progress_step(self._scrobble_setting("progress_step", 25.0)):
                emit_action = "start"
                session["seek_pending"] = bool(jump >= SEEK_JUMP_PERCENT)
        session["state"] = state
        if not emit_action:
            return
        emit_progress = max(1.0, percent) if emit_action == "start" else percent
        if self._dispatch_event(self._event(session, emit_action, emit_progress), session.get("duration_ms")):
            session["last_action"] = emit_action
            session["last_emitted_progress"] = emit_progress
        session["seek_pending"] = False

    def _completed_in_session(self, session: Mapping[str, Any]) -> bool:
        fid = str(session.get("file_id") or "")
        started = _float(session.get("started_at"))
        if not fid or started is None:
            return False
        try:
            resp = tofa_request(self._reader(str(session.get("instance") or "")), "GET", f"/media/{quote(fid, safe='')}/progress")
        except Exception as exc:
            _log(f"Tofa completion check failed file={fid}: {exc}", "DEBUG")
            return False
        data = safe_json(resp) if resp.status_code == 200 else None
        if not isinstance(data, Mapping) or not data.get("completed"):
            return False
        done = parse_epoch(data.get("completed_at"))
        return done is not None and done >= started - COMPLETION_SLACK_SECONDS

    def _completed_after_stop(self, session: Mapping[str, Any], progress: float) -> bool:
        if self._completed_in_session(session):
            return True
        if progress < COMPLETION_RECHECK_MIN_PROGRESS:
            return False
        if self._stop.wait(COMPLETION_RECHECK_SECONDS):
            return False
        return self._completed_in_session(session)

    def _stop_session(self, sid: str) -> None:
        session = self._sessions.pop(sid, None)
        if not session:
            return
        progress = float(session.get("last_progress") or 0.0)
        if progress < 100.0 and self._completed_after_stop(session, progress):
            _log(f"Tofa finished file={session.get('file_id')} at last seen p={progress:.1f}; stop sent as 100", "DEBUG")
            progress = 100.0
            if session.get("duration_ms"):
                session["position_ms"] = session["duration_ms"]
        self._dispatch_event(self._event(session, "stop", progress), session.get("duration_ms"))

    def handle_message(self, raw: str) -> None:
        try:
            event = json.loads(raw)
        except Exception:
            return
        if not isinstance(event, Mapping) or event.get("type") not in STREAM_EVENTS:
            return
        sid = str(event.get("session_id") or "").strip()
        if event["type"] == "StreamStarted":
            inst = self._instance_for(event, self._active_cfg())
            if not inst:
                return
            self._on_started(event, inst)
        elif event["type"] == "StreamProgress":
            self._on_progress(event)
        elif sid in self._sessions:
            self._stop_session(sid)
        else:
            self._skipped.discard(sid)

    def _expire_stale(self, now: float) -> None:
        for sid, session in list(self._sessions.items()):
            limit = STALE_PAUSED_SECONDS if session.get("state") == "paused" else STALE_PLAYING_SECONDS
            if now - float(session.get("last_event_at") or now) >= limit:
                _log(f"Tofa session went quiet file={session.get('file_id')}; closing it", "DEBUG")
                self._stop_session(sid)

    def _reconcile(self) -> None:
        if not self._sessions:
            return
        try:
            resp = tofa_request(self, "GET", "/system/playback/sessions")
        except Exception:
            return
        rows = safe_json(resp) if resp.status_code == 200 else None
        if not isinstance(rows, list):
            return
        live = {str(r.get("session_id") or "") for r in rows if isinstance(r, Mapping)}
        for sid in list(self._sessions):
            if sid not in live:
                self._stop_session(sid)

    def _connect(self) -> Any:
        cfg = self._active_cfg()
        block = self._block(cfg)
        url = ws_url_for(str(block.get("server_url") or ""))
        if not url:
            raise TofaError("invalid_url")
        client = TofaClient(cfg, instance_id=self._instance_id)
        token = client.access_token()
        sslopt = None if verify_ssl_of(block) else {"cert_reqs": ssl.CERT_NONE, "check_hostname": False}
        ws = websocket.create_connection(url, timeout=RECV_TIMEOUT_SECONDS, sslopt=sslopt,
                                         header=[f"User-Agent: {http_user_agent('Watcher', override_env='CW_TOFA_UA')}"])
        auth: dict[str, Any] = {"type": "auth", "token": token, "profile_id": str(block.get("profile_id") or "")}
        if str(block.get("profile_token") or "").strip():
            auth["profile_token"] = str(block["profile_token"])
        ws.send(json.dumps(auth))
        reply = safe_reply(ws.recv())
        if reply.get("type") == "auth_ok":
            return ws
        try:
            ws.close()
        except Exception:
            pass
        error = str(reply.get("error") or "auth_failed")
        if error in ("profile_locked", "invalid profile context"):
            client._flag_profile_pin()
            raise TofaError("profile_locked")
        if error == "unauthorized":
            client.refresh_token(cfg, force=True, rejected_token=token)
        raise TofaError(error)

    def _close(self) -> None:
        ws = self._ws
        self._ws = None
        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass

    def _feed_rows(self, reader: Any, cursor: str) -> tuple[list[dict[str, Any]], str, bool]:
        rows: list[dict[str, Any]] = []
        for _ in range(FEED_MAX_PAGES):
            resp = tofa_request(reader, "GET", "/users/me/state-changes", params={"cursor": cursor, "limit": FEED_PAGE})
            if resp.status_code != 200:
                raise RuntimeError(f"http:{int(resp.status_code)}")
            data = safe_json(resp)
            if not isinstance(data, Mapping) or not isinstance(data.get("changes"), list):
                raise RuntimeError("invalid_response")
            if data.get("requires_full_sync"):
                return [], "", True
            if not all(isinstance(r, Mapping) for r in data["changes"]):
                raise RuntimeError("invalid_response")
            rows.extend(dict(r) for r in data["changes"])
            cursor = str(data.get("next_cursor") or cursor)
            if not data.get("has_more"):
                break
        return rows, cursor, False

    def _feed_head(self, reader: Any) -> str:
        resp = tofa_request(reader, "GET", "/users/me/watch-state", params={"completed_only": "true", "limit": 1})
        if resp.status_code != 200:
            raise RuntimeError(f"http:{int(resp.status_code)}")
        data = safe_json(resp)
        cursor = str(data.get("changes_cursor") or "") if isinstance(data, Mapping) else ""
        if not cursor:
            raise RuntimeError("invalid_response")
        return cursor

    def _mark_profiles(self, cfg: Mapping[str, Any]) -> list[tuple[str, str, str]]:
        accepts = getattr(self._dispatch, "accepts_user", None)
        out: list[tuple[str, str, str]] = []
        for inst, block in self._profiles(cfg):
            pid = str(block.get("profile_id") or "").strip()
            name = str(block.get("profile_name") or "").strip()
            probe = ScrobbleEvent(
                action="stop", media_type="movie", ids={}, title=None, year=None, season=None, number=None, progress=100.0,
                account=name or None, server_uuid=None, session_key=None,
                raw={"_cw_session_identity": {"user_id": pid, "name": name}, "_cw_manual_mark": True},
            )
            try:
                allowed = bool(accepts(probe)) if callable(accepts) else True
            except Exception:
                allowed = True
            if allowed:
                out.append((inst, pid, name))
        return out

    def _file_completed(self, inst: str, fid: str, latest: Mapping[str, Mapping[str, Any]]) -> bool:
        row = latest.get(fid)
        if row is not None:
            return row.get("completed") is True
        try:
            resp = tofa_request(self._reader(inst), "GET", f"/media/{quote(fid, safe='')}/progress")
        except Exception as exc:
            raise _MarkPending(str(exc) or type(exc).__name__) from exc
        if resp.status_code == 404:
            return False
        if resp.status_code != 200:
            raise _MarkPending(f"http:{int(resp.status_code)}")
        body = (resp.text or "").strip()
        if not body or body == "null":
            return False
        data = safe_json(resp)
        if not isinstance(data, Mapping) or "completed" not in data or not isinstance(data["completed"], (bool, type(None))):
            raise _MarkPending("invalid_response")
        return data["completed"] is True

    def _emit_manual_mark(self, inst: str, pid: str, name: str, row: Mapping[str, Any],
                          latest: Mapping[str, Mapping[str, Any]] | None = None, titles: set[str] | None = None) -> bool | None:
        fid = str(row.get("media_file_id") or "").strip()
        if not fid or self._outside_whitelist(fid, inst):
            return None
        lib, hit = self._file_hit(fid, inst)
        meta = self._metadata(lib, hit) if hit else None
        if not meta or hit is None:
            return None
        kind, target, edition = hit
        siblings = [f for f in editions_of(lib, kind, target).get(edition) or [] if f != fid]
        if not all(self._file_completed(inst, f, latest or {}) for f in siblings):
            return None
        title = f"{kind}:{target!r}:{edition}"
        if titles is not None:
            if title in titles:
                return None
            titles.add(title)
        server = str(self._block().get("server_url") or "")
        session = {
            "session_key": f"tofa:{self._instance_id}:mark:{pid}:{fid}:{row.get('completed_at') or ''}",
            "source_session_key": f"mark:{pid}:{fid}",
            "file_id": fid,
            "instance": inst,
            "profile_id": pid,
            "meta": meta,
            "account": name or None,
            "server_uuid": server_uuid_for(server),
            "position_ms": None,
            "duration_ms": None,
            "seek_pending": False,
        }
        ev = self._event(session, "stop", 100.0)
        raw = dict(ev.raw or {})
        raw["_cw_manual_mark"] = True
        raw["_cw_preserve_stop"] = True
        stamp = parse_epoch(row.get("completed_at"))
        if stamp:
            raw["_cw_watched_at"] = int(stamp)
        ev = ScrobbleEvent(**{**ev.__dict__, "raw": raw})
        if not self._dispatch.dispatch(ev):
            return False
        _log(f"manual mark {ev.media_type} user={mask_account(ev.account)} media='{mark_label(ev)}'", "INFO")
        return True

    def _poll_profile_marks(self, inst: str, pid: str, name: str, now: float) -> None:
        reader = self._reader(inst)
        who = f"user={mask_account(name)}"
        try:
            cursor = self._mark_cursors.get(pid)
            if not cursor:
                self._mark_cursors[pid] = self._feed_head(reader)
                return
            rows, nxt, reset = self._feed_rows(reader, cursor)
        except Exception as exc:
            log_read_failure(self._marks, pid, who, exc, _log)
            return
        if reset:
            self._mark_cursors.pop(pid, None)
            return
        self._mark_cursors[pid] = nxt or cursor
        log_read_health(self._marks, pid, who, now, _log)
        latest: dict[str, Mapping[str, Any]] = {}
        for row in rows:
            if row.get("kind") == "watched" and row.get("media_file_id"):
                latest[str(row["media_file_id"])] = row
        retry = self._mark_retry.pop(pid, {})
        since: dict[str, float] = {}
        for fid, (row, first) in retry.items():
            if now - first < EMITTED_TTL_SECONDS:
                since[fid] = first
                latest.setdefault(fid, row)
        pending: dict[str, tuple[dict[str, Any], float]] = {}
        admitted = 0
        titles: set[str] = set()
        for fid, row in latest.items():
            if not row.get("completed"):
                continue
            stamp = parse_epoch(row.get("completed_at"))
            key = f"{pid}:{fid}:{row.get('completed_at') or ''}"
            if key in self._emitted or self._marks.seen_in_session(f"{pid}:{fid}", now):
                continue
            if written_by_crosswatch("tofa", pid, fid, stamp, now):
                continue
            if admitted >= MAX_MARKS_PER_POLL:
                _log(f"manual marks: more than {MAX_MARKS_PER_POLL} marks for {who}; the rest is left to sync", "WARNING")
                break
            self._emitted[key] = now
            try:
                if self._emit_manual_mark(inst, pid, name, row, latest, titles) is not None:
                    admitted += 1
            except _MarkPending as exc:
                self._emitted.pop(key, None)
                pending[fid] = (dict(row), since.get(fid, now))
                _log(f"manual marks: retrying file={fid} next poll: {exc}", "DEBUG")
            except Exception as exc:
                admitted += 1
                _log(f"manual marks: dispatch failed file={fid}: {exc}", "DEBUG")
        if pending:
            self._mark_retry[pid] = pending

    def _poll_manual_marks(self, cfg: Mapping[str, Any]) -> None:
        try:
            now = time.time()
            if not self._marks.due(cfg, now) or not wants_marks(self._dispatch):
                return
            for key, ts in list(self._emitted.items()):
                if now - ts > EMITTED_TTL_SECONDS:
                    self._emitted.pop(key, None)
            for inst, pid, name in self._mark_profiles(cfg):
                self._poll_profile_marks(inst, pid, name, now)
        except Exception as exc:
            _log(f"manual marks poll failed: {exc}", "DEBUG")

    def _housekeeping(self) -> None:
        self._last_housekeeping = time.monotonic()
        cfg = self._active_cfg()
        self._expire_stale(time.time())
        self._poll_manual_marks(cfg)

    def start(self) -> None:
        cfg = self._active_cfg()
        if not source_enabled(cfg, "watcher"):
            if not self._quiet_startup:
                _log("Watcher source is disabled; Tofa watcher not started.", "INFO")
            return
        if not is_configured(self._block(cfg)):
            if not self._quiet_startup:
                _log("Tofa is not connected; watcher not started.", "WARNING")
            return
        self._stop.clear()
        _log(f"Watcher connected; inst={self._instance_id}", "DEBUG" if self._quiet_startup else "INFO")
        while not self._stop.is_set():
            try:
                self._ws = self._connect()
            except Exception as exc:
                self._mark_offline(str(exc) or type(exc).__name__)
                wait = PROFILE_LOCKED_RETRY_SECONDS if str(exc) == "profile_locked" else self._offline_retry
                self._stop.wait(wait)
                continue
            self._mark_online()
            self._reconcile()
            while not self._stop.is_set():
                try:
                    raw = self._ws.recv()
                except websocket.WebSocketTimeoutException:
                    self._housekeeping()
                    continue
                except Exception as exc:
                    self._close()
                    if not self._stop.is_set():
                        self._mark_offline(str(exc) or type(exc).__name__)
                    break
                if raw:
                    try:
                        self.handle_message(raw)
                    except Exception as exc:
                        self._log_error_limited(type(exc).__name__, f"Tofa event failed: {exc}")
                if time.monotonic() - self._last_housekeeping >= RECV_TIMEOUT_SECONDS:
                    self._housekeeping()
            if not self._stop.is_set():
                self._stop.wait(self._offline_retry)
        self._close()

    def start_async(self) -> None:
        if self._bg and self._bg.is_alive():
            return
        self._stop.clear()
        self._bg = threading.Thread(target=self.start, name=f"TofaWatch:{self._instance_id}", daemon=True)
        self._bg.start()

    def stop(self) -> None:
        self._stop.set()
        self._close()
        bg = self._bg
        if bg and bg.is_alive() and bg is not threading.current_thread():
            bg.join(timeout=6.0)

    def is_alive(self) -> bool:
        bg = self._bg
        return bool(bg and bg.is_alive() and not self._stop.is_set())


def safe_reply(raw: Any) -> dict[str, Any]:
    try:
        data = json.loads(raw)
    except Exception:
        return {}
    return dict(data) if isinstance(data, Mapping) else {}


def make_default_watch(
    dispatcher: Any | None = None,
    cfg_provider: Callable[[], dict[str, Any]] | None = None,
    instance_id: Any = "default",
    sinks: list[ScrobbleSink] | None = None,
) -> TofaWatchService:
    return TofaWatchService(sinks=sinks, dispatcher=dispatcher, cfg_provider=cfg_provider, instance_id=instance_id)


WatchService = TofaWatchService

__all__ = ["TofaWatchService", "WatchService", "make_default_watch"]

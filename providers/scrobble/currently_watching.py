# providers/scrobble/currently_watching.py
# CrossWatch - Currently Watching State Management for Scrobbling Providers
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None  # type: ignore

from providers.scrobble.scrobble import ScrobbleEvent

STATE_VERSION = 2
PRUNE_AFTER_SEC = 10 * 60
ACTIVE_STATES = {"playing", "paused", "buffering"}


def _log(msg: str, lvl: str = "DEBUG") -> None:
    if BASE_LOG:
        try:
            BASE_LOG(str(msg), level=lvl, module="SCROBBLE")
            return
        except Exception:
            pass
    try:
        print(f"[{lvl}] currently_watching: {msg}")
    except Exception:
        pass


def _state_file() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base / "currently_watching.json"



def state_file() -> Path:
    return _state_file()


def _write_raw(payload: Optional[dict[str, Any]]) -> None:
    p = _state_file()
    if not payload:
        try:
            if p.exists():
                p.unlink()
        except Exception as e:
            _log(f"remove failed: {e}")
        return
    tmp = p.with_suffix(p.suffix + ".tmp")
    try:
        tmp.write_text(json.dumps(payload, ensure_ascii=False, separators=(",", ":")), encoding="utf-8")
        tmp.replace(p)
    except Exception as e:
        _log(f"write failed: {e}")


def _read_raw() -> Any:
    p = _state_file()
    if not p.exists():
        return None
    try:
        raw = p.read_text(encoding="utf-8")
        return json.loads(raw) if raw.strip() else None
    except Exception as e:
        _log(f"read failed: {e}", "ERROR")
        return None


def _extract_tmdb_ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = dict(getattr(ev, "ids", {}) or {})
    for key in ("tmdb", "tmdb_show"):
        v = ids.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit():
            ids[key] = int(s)
    return ids


def _coerce_int(v: Any) -> Optional[int]:
    try:
        if v is None or isinstance(v, bool):
            return None
        s = str(v).strip()
        if not s:
            return None
        return int(float(s))
    except Exception:
        return None


def _payload_key(source: str, payload: dict[str, Any]) -> str:
    pk = str(payload.get("_key") or "").strip()
    if pk:
        return pk

    sk = str(payload.get("session_key") or "").strip()
    if sk:
        return f"{source}:{sk}"

    #  Fallback construction for backward compatibility and non-session cases.
    raw_ids = payload.get("ids")
    ids: dict[str, Any] = raw_ids if isinstance(raw_ids, dict) else {}
    mt = str(payload.get("media_type") or payload.get("type") or "").lower()
    season = str(payload.get("season") or "").strip()
    episode = str(payload.get("episode") or "").strip()

    def _id(*keys: str) -> str:
        for k in keys:
            v = ids.get(k)
            if v is None:
                continue
            s = str(v).strip()
            if s:
                return s
        return ""

    if mt == "episode":
        base = _id("tmdb_show", "tvdb_show", "imdb_show", "tmdb")
        if base and season and episode:
            return f"{source}:{mt}:{base}:s{season}:e{episode}"
    else:
        base = _id("tmdb", "imdb", "tvdb")
        if base:
            return f"{source}:{mt or 'movie'}:{base}"

    mt = str(payload.get("media_type") or payload.get("type") or "").lower()
    title = str(payload.get("title") or "")
    year = str(payload.get("year") or "")
    season = str(payload.get("season") or "")
    episode = str(payload.get("episode") or "")
    return f"{source}:{mt}:{title}:{year}:{season}:{episode}"


def _as_v2(raw: Any, source_hint: str | None = None) -> dict[str, Any]:
    if isinstance(raw, dict) and raw.get("v") == STATE_VERSION and isinstance(raw.get("streams"), dict):
        return {"v": STATE_VERSION, "streams": dict(raw.get("streams") or {})}

    if isinstance(raw, dict) and raw.get("title"):
        src = str(raw.get("source") or source_hint or "").strip() or "unknown"
        k = _payload_key(src, raw)
        return {"v": STATE_VERSION, "streams": {k: dict(raw)}}

    return {"v": STATE_VERSION, "streams": {}}


def _prune(streams: dict[str, Any], now: int) -> None:
    try:
        for k, v in list(streams.items()):
            ts = int(v.get("updated") or 0) if isinstance(v, dict) else 0
            if ts and (now - ts) > PRUNE_AFTER_SEC:
                streams.pop(k, None)
    except Exception:
        pass


def _update_stream(source: str, payload: dict[str, Any], clear_on_stop: bool) -> None:
    now = int(time.time())
    raw = _read_raw()
    state = _as_v2(raw, source_hint=source)
    streams = state.get("streams") or {}
    if not isinstance(streams, dict):
        streams = {}

    _prune(streams, now)

    key = _payload_key(source, payload)
    st = str(payload.get("state") or "").lower()

    existing_started = 0
    existing = streams.get(key)
    if isinstance(existing, dict):
        try:
            existing_started = int(existing.get("started") or 0)
        except Exception:
            existing_started = 0

    if st == "stopped":
        streams.pop(key, None)
    else:
        if not existing_started:
            existing_started = now
        payload["started"] = existing_started
        streams[key] = payload

    if not streams:
        _write_raw(None)
        return

    _write_raw({"v": STATE_VERSION, "streams": streams})


def update_from_event(
    source: str,
    ev: ScrobbleEvent,
    duration_ms: int | None = None,
    cover: str | None = None,
    clear_on_stop: bool = False,
) -> None:
    try:
        action = (ev.action or "").lower()
        state_map = {"start": "playing", "pause": "paused", "stop": "stopped"}
        state = state_map.get(action, "unknown")

        media_type = (ev.media_type or "").lower()
        if media_type not in ("movie", "episode"):
            media_type = "movie"

        season = ev.season if media_type == "episode" else None
        episode = ev.number if media_type == "episode" else None

        ids = _extract_tmdb_ids(ev)

        payload: dict[str, Any] = {
            "source": str(source),
            "media_type": media_type,
            "title": ev.title or "",
            "year": ev.year,
            "season": season,
            "episode": episode,
            "progress": int(ev.progress),
            "duration_ms": int(duration_ms) if isinstance(duration_ms, int) else None,
            "cover": cover,
            "state": state,
            "updated": int(time.time()),
            "ids": ids,
            "account": ev.account,
            "server_uuid": ev.server_uuid,
            "session_key": ev.session_key,
        }
        _update_stream(str(source), payload, clear_on_stop=clear_on_stop)
    except Exception as e:
        _log(f"update_from_event failed: {e}", "ERROR")


def update_from_payload(
    source: str,
    media_type: str,
    title: str,
    year: Any,
    season: Any,
    episode: Any,
    progress: Any,
    stop: bool,
    duration_ms: Any = None,
    cover: str | None = None,
    state: str | None = None,
    clear_on_stop: bool = False,
    ids: dict[str, Any] | None = None,
    session_key: str | None = None,
) -> None:
    try:
        mt = (media_type or "").lower()
        if mt not in ("movie", "episode"):
            mt = "movie"

        prog_int = _coerce_int(progress) or 0

        if state is None:
            st_val = "stopped" if stop else ("playing" if prog_int > 0 else "unknown")
        else:
            st_val = str(state)

        payload: dict[str, Any] = {
            "source": str(source),
            "media_type": mt,
            "title": title or "",
            "year": _coerce_int(year),
            "season": _coerce_int(season) if mt == "episode" else None,
            "episode": _coerce_int(episode) if mt == "episode" else None,
            "progress": prog_int,
            "duration_ms": _coerce_int(duration_ms),
            "cover": cover,
            "state": st_val,
            "updated": int(time.time()),
            "session_key": session_key,
        }
        if isinstance(ids, dict) and ids:
            payload["ids"] = dict(ids)

        _update_stream(str(source), payload, clear_on_stop=clear_on_stop)
    except Exception as e:
        _log(f"update_from_payload failed: {e}", "ERROR")

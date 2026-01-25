# providers/scrobble/currently_watching.py
# CrossWatch - currently_watching middleware
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
        
def _extract_tmdb(ev: ScrobbleEvent) -> Optional[int]:
    try:
        ids = ev.ids or {}
    except Exception:
        return None

    for key in ("tmdb", "tmdb_show"):
        v = ids.get(key)
        if v is None:
            continue
        s = str(v).strip()
        if s.isdigit():
            return int(s)
    return None

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

        if clear_on_stop and state == "stopped":
            pass

        media_type = (ev.media_type or "").lower()
        if media_type not in ("movie", "episode"):
            media_type = "movie"

        season = ev.season if media_type == "episode" else None
        episode = ev.number if media_type == "episode" else None
        ids = dict(getattr(ev, "ids", {}) or {})
        tmdb = _extract_tmdb(ev)

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
            "tmdb": tmdb,
        }
        _write_raw(payload)
    except Exception as e:
        _log(f"update_from_event failed: {e}")

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
) -> None:
    try:
        mt = (media_type or "").lower()
        if mt not in ("movie", "episode"):
            mt = "movie"

        def _to_int(val: Any) -> Optional[int]:
            try:
                if val is None or isinstance(val, bool):
                    return None
                s = str(val).strip()
                if not s:
                    return None
                return int(float(s))
            except Exception:
                return None

        prog_int = _to_int(progress) or 0

        if state is None:
            if stop:
                st_val = "stopped"
            elif prog_int > 0:
                st_val = "playing"
            else:
                st_val = "unknown"
        else:
            st_val = state

        if clear_on_stop and st_val == "stopped":
            _write_raw(None)
            return

        payload: dict[str, Any] = {
            "source": str(source),
            "media_type": mt,
            "title": title or "",
            "year": _to_int(year),
            "season": _to_int(season) if mt == "episode" else None,
            "episode": _to_int(episode) if mt == "episode" else None,
            "progress": prog_int,
            "duration_ms": _to_int(duration_ms),
            "cover": cover,
            "state": st_val,
            "updated": int(time.time()),
        }
        if isinstance(ids, dict) and ids:
            payload["ids"] = dict(ids)
            tmdb_val = None
            try:
                v = ids.get("tmdb_show") or ids.get("tmdb")
                if v is not None:
                    s = str(v).strip()
                    if s.isdigit():
                        tmdb_val = int(s)
            except Exception:
                tmdb_val = None
            payload["tmdb"] = tmdb_val

        _write_raw(payload)
    except Exception as e:
        _log(f"update_from_payload failed: {e}")

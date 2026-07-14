# providers/scrobble/trakt/sink.py
# CrossWatch - Trakt.tv scrobble sink implementation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, replace
import json, time
import queue
import threading
from pathlib import Path
from typing import Any

import requests

from cw_platform.config_base import load_config
from cw_platform.provider_instances import normalize_instance_id

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None
    
try:
    from providers.auth._auth_TRAKT import PROVIDER as AUTH_TRAKT
except Exception:
    AUTH_TRAKT = None  # type: ignore[misc]

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink
from services.activity import record_scrobble_event
from cw_platform.event_archive import record_watch
from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from api.watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except ImportError:
    _rm_across_api = None  # type: ignore[misc]


TRAKT_API = "https://api.trakt.tv"
APP_AGENT = "CrossWatch/Watcher/1.0"
_TOKEN_OVERRIDE: dict[str, str] = {}
_AR_TTL = 60
_SENSITIVE_KEYS = {
    "access_token", "refresh_token", "token", "authorization",
    "client_secret", "password", "code", "api_key",
}


def _cfg() -> dict[str, Any]:
    try:
        return load_config()
    except Exception:
        return {}


def _is_debug() -> bool:
    try:
        return bool((_cfg().get("runtime") or {}).get("debug"))
    except Exception:
        return False


def _log(msg: str, level: str = "INFO") -> None:
    lvl = (str(level) or "INFO").upper()
    if lvl == "DEBUG" and not _is_debug():
        return
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=lvl, module="TRAKT-SCROBBLE")
            return
        except Exception:
            pass
    print(f"[TRAKT-SCROBBLE:{lvl}] {msg}")


def _mask_account(value: Any) -> str:
    s = str(value or "").strip()
    if not s:
        return "unknown"
    if len(s) <= 2:
        return s[0] + "*"
    return s[:2] + "***"


def _redact_log_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        out: dict[str, Any] = {}
        for k, v in value.items():
            key = str(k or "").strip().lower()
            out[str(k)] = "****" if key in _SENSITIVE_KEYS else _redact_log_value(v)
        return out
    if isinstance(value, list):
        return [_redact_log_value(v) for v in value]
    if isinstance(value, tuple):
        return tuple(_redact_log_value(v) for v in value)
    return value


def _safe_log_repr(value: Any) -> str:
    try:
        return repr(_redact_log_value(value))
    except Exception:
        return "<unavailable>"


def _merged_provider_block(cfg: Mapping[str, Any], key: str, instance_id: Any = None) -> dict[str, Any]:
    base = cfg.get(key) if isinstance(cfg, Mapping) else None
    blk = dict(base or {}) if isinstance(base, Mapping) else {}
    inst = normalize_instance_id(instance_id)
    if inst != "default":
        insts = blk.get("instances")
        if isinstance(insts, Mapping) and isinstance(insts.get(inst), Mapping):
            overlay = dict(insts.get(inst) or {})
            blk.pop("instances", None)
            out = dict(blk)
            out.update(overlay)
            return out
    blk.pop("instances", None)
    return blk


def _app_meta(cfg: dict[str, Any]) -> dict[str, str]:
    rt = cfg.get("runtime") or {}
    ver = str(rt.get("version") or APP_AGENT)
    bdate = (rt.get("build_date") or "").strip()
    out: dict[str, str] = {"app_version": ver}
    if bdate:
        out["app_date"] = bdate
    return out


def _hdr(cfg: dict[str, Any], instance_id: Any = None) -> dict[str, str]:
    inst = normalize_instance_id(instance_id)
    t = _merged_provider_block(cfg, "trakt", inst)
    client_id = str(t.get("client_id") or t.get("api_key") or "")

    auth = cfg.get("auth") if isinstance(cfg, dict) else None
    auth_trakt_base = (auth or {}).get("trakt") if isinstance(auth, dict) else {}
    auth_trakt = _merged_provider_block({"trakt": auth_trakt_base} if isinstance(auth_trakt_base, dict) else {}, "trakt", inst)

    token = _TOKEN_OVERRIDE.get(inst) or t.get("access_token") or auth_trakt.get("access_token") or ""
    h: dict[str, str] = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "User-Agent": APP_AGENT,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _get(path: str, cfg: dict[str, Any], instance_id: Any = None) -> requests.Response:
    return requests.get(f"{TRAKT_API}{path}", headers=_hdr(cfg, instance_id), timeout=10)


def _post(path: str, body: dict[str, Any], cfg: dict[str, Any], instance_id: Any = None) -> requests.Response:
    return requests.post(f"{TRAKT_API}{path}", headers=_hdr(cfg, instance_id), json=body, timeout=10)


def _tok_refresh(instance_id: Any = None) -> bool:
    inst = normalize_instance_id(instance_id)

    if AUTH_TRAKT is None:
        _log("AUTH_TRAKT provider missing, cannot refresh token", "ERROR")
        return False

    try:
        full_cfg = _cfg()
    except Exception:
        full_cfg = {}

    try:
        res = AUTH_TRAKT.refresh(full_cfg, instance_id=inst)
    except Exception as e:
        _log(f"Token refresh via AUTH_TRAKT failed: {e}", "ERROR")
        return False

    if not isinstance(res, dict) or not res.get("ok"):
        _log(f"Token refresh via AUTH_TRAKT failed: {_safe_log_repr(res)}", "ERROR")
        return False

    new_cfg = _cfg()
    t = _merged_provider_block(new_cfg, "trakt", inst)

    auth = new_cfg.get("auth") if isinstance(new_cfg, dict) else None
    auth_trakt_base = (auth or {}).get("trakt") if isinstance(auth, dict) else {}
    auth_trakt = _merged_provider_block({"trakt": auth_trakt_base} if isinstance(auth_trakt_base, dict) else {}, "trakt", inst)

    token = str(t.get("access_token") or auth_trakt.get("access_token") or "").strip()
    if not token:
        _log("Token refresh via AUTH_TRAKT succeeded but no access_token in config", "ERROR")
        return False

    _TOKEN_OVERRIDE[inst] = token
    _log("Trakt token refreshed via AUTH_TRAKT", "DEBUG")
    return True


def _ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}
    return {k: ids[k] for k in ("tmdb", "imdb", "tvdb", "trakt") if ids.get(k)}


def _show_ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}
    out: dict[str, Any] = {}
    for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show"):
        if ids.get(k):
            out[k.replace("_show", "")] = ids[k]
    return out


def _clamp(p: Any) -> float:
    try:
        v = float(p)
    except Exception:
        v = 0.0
    return max(0.0, min(100.0, v))


def _stop_pause_threshold(cfg: dict[str, Any]) -> float:
    try:
        return float(((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 80.0))
    except Exception:
        return 80.0


def _watched_at(cfg: dict[str, Any]) -> float:
    try:
        return float(((cfg.get("scrobble") or {}).get("trakt") or {}).get("watched_at", 90.0))
    except Exception:
        return 90.0


def _force_stop_at(cfg: dict[str, Any]) -> float:
    try:
        return float(((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at", 95.0))
    except Exception:
        return 95.0


def _complete_at(cfg: dict[str, Any]) -> float:
    try:
        return float(((cfg.get("scrobble") or {}).get("trakt") or {}).get("complete_at", 0))
    except Exception:
        return 0.0


def _watch_pause_debounce(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5))
    except Exception:
        return 5


def _watch_suppress_start_at(cfg: dict[str, Any]) -> float:
    try:
        return float(((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99))
    except Exception:
        return 99.0


@dataclass(frozen=True)
class _TraktDecision:
    path: str | None
    progress: float
    record_watched: bool
    bypass_debounce: bool
    held: bool = False


def _trakt_decision(action: str, progress: Any, cfg: dict[str, Any]) -> _TraktDecision:
    p = _clamp(progress)
    history_cutoff = 80.0
    watched_at = _watched_at(cfg)
    force_stop_at = _force_stop_at(cfg)
    if action == "start":
        return _TraktDecision("/scrobble/start", p, False, False)
    if action == "pause":
        if p < 1.0:
            return _TraktDecision(None, p, False, False)
        if p < history_cutoff:
            return _TraktDecision("/scrobble/stop", p, False, False)
        return _TraktDecision(None, p, False, False, held=True)
    if action == "stop":
        if p < 1.0:
            return _TraktDecision(None, p, False, False)
        if p < history_cutoff:
            return _TraktDecision("/scrobble/stop", p, False, False)
        if p < watched_at:
            return _TraktDecision(None, p, False, False, held=True)
        return _TraktDecision("/scrobble/stop", p, True, p >= force_stop_at)
    return _TraktDecision(None, p, False, False)

def _trakt_progress_step(cfg: dict[str, Any]) -> int:
    try:
        s = (cfg.get("scrobble") or {}).get("trakt") or {}
        step = s.get("progress_step")
        if step is None:
            step = (cfg.get("trakt") or {}).get("progress_step", 5)
        step = int(step)
    except Exception:
        step = 5
    return max(1, min(25, step))


def _quantize_progress(prog: float | int, step: int, action: str) -> int:
    try:
        p = int(float(prog))
    except Exception:
        p = 0
    if step <= 1 or action == "stop":
        return max(0, min(100, p))
    if p < step:
        return max(1, min(100, p))
    q = (p // step) * step
    if q <= 0:
        q = 1
    return max(1, min(100, q))


def _guid_search(ev: ScrobbleEvent, cfg: dict[str, Any], instance_id: Any = None) -> dict[str, Any] | None:
    ids = ev.ids or {}
    for key in ("tmdb", "tvdb", "imdb"):
        val = ids.get(key)
        if not val:
            continue
        try:
            r = _get(f"/search/{key}/{val}?type=episode", cfg, instance_id)
        except Exception:
            continue
        if r.status_code == 401 and _tok_refresh(instance_id):
            try:
                r = _get(f"/search/{key}/{val}?type=episode", cfg, instance_id)
            except Exception:
                continue
        if r.status_code != 200:
            continue
        try:
            arr = r.json() or []
        except Exception:
            arr = []
        for hit in arr:
            epi_ids = ((hit.get("episode") or {}).get("ids") or {}) or {}
            out = {k: v for k, v in epi_ids.items() if k in ("trakt", "tmdb", "imdb", "tvdb") and v}
            if out:
                return out
    return None


def _ar_state_file() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base / "auto_remove_seen.json"


def _ar_seen(key: str) -> bool:
    p = _ar_state_file()
    try:
        data = json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        data = {}
    now = time.time()
    try:
        data = {k: v for k, v in data.items() if (now - float(v)) < _AR_TTL}
    except Exception:
        data = {}
    if key in data:
        try:
            p.write_text(json.dumps(data), encoding="utf-8")
        except Exception:
            pass
        return True
    data[key] = now
    try:
        p.write_text(json.dumps(data), encoding="utf-8")
    except Exception:
        pass
    return False


def _ar_key(ids: dict[str, Any], media_type: str, scope: str = "") -> str:
    for k in ("tmdb", "imdb", "tvdb", "trakt", "simkl"):
        v = ids.get(k)
        if v:
            return f"{scope}|{media_type}:{k}:{v}" if scope else f"{media_type}:{k}:{v}"
    try:
        base = f"{media_type}:{json.dumps(ids, sort_keys=True)}"
        return f"{scope}|{base}" if scope else base
    except Exception:
        base = f"{media_type}:title/year"
        return f"{scope}|{base}" if scope else base


def _norm_type(t: str) -> str:
    s = (t or "").strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    if s == "series":
        s = "show"
    return s


def _cfg_delete_enabled(cfg: dict[str, Any], media_type: str) -> bool:
    s = cfg.get("scrobble") or {}
    watch = s.get("watch") or {}
    route_opts_raw = watch.get("route_options")
    route_opts: dict[str, Any] = route_opts_raw if isinstance(route_opts_raw, dict) else {}
    route_mode = str(route_opts.get("auto_remove_watchlist") or "inherit").strip().lower()
    if route_mode == "off":
        return False
    if not s.get("delete_plex"):
        if route_mode != "on":
            return False
    types = s.get("delete_plex_types") or []
    mt = _norm_type(media_type)
    if isinstance(types, str):
        return _norm_type(types) == mt
    try:
        allowed = {_norm_type(x) for x in types if str(x).strip()}
    except Exception:
        return False
    return mt in allowed


def _auto_remove_across(ev: ScrobbleEvent, cfg: dict[str, Any], scope: str = "") -> None:
    mt = _norm_type(str(getattr(ev, "media_type", "") or ""))
    if not _cfg_delete_enabled(cfg, mt):
        _log(f"Auto-remove skipped: disabled by config for type={mt or 'unknown'}", "DEBUG")
        return
    ids = _show_ids(ev) if mt == "episode" else _ids(ev)
    if not ids:
        ids = _ids(ev)
    if not ids:
        _log("Auto-remove skipped: no provider IDs available", "DEBUG")
        return
    key = _ar_key(ids, mt, scope=scope)
    if _ar_seen(key):
        _log("Auto-remove deduped (already handled by another sink)", "DEBUG")
        return
    try:
        _log(f"Auto-remove across providers ids={ids} media={mt}", "INFO")
        _rm_across(ids, mt, scope=scope)
        return
    except Exception as e:
        _log(f"Auto-remove across (_auto_remove_watchlist) failed: {e}", "WARN")
    try:
        if _rm_across_api:
            _log(f"Auto-remove across providers via _watchlistAPI ids={ids} media={mt}", "INFO")
            _rm_across_api(ids, mt)  # type: ignore[misc]
            return
    except Exception as e:
        _log(f"Auto-remove across (_watchlistAPI) failed: {e}", "WARN")
    _log("Auto-remove skipped: no available remove-across implementation", "DEBUG")


def _ids_desc_map(ids: dict[str, Any]) -> str:
    for k in ("trakt", "tmdb", "imdb", "tvdb"):
        v = ids.get(k)
        if v is not None:
            return f"{k}:{v}"
    return "title/year"


def _media_name(ev: ScrobbleEvent) -> str:
    if ev.media_type == "episode":
        s = ev.season if ev.season is not None else 0
        n = ev.number if ev.number is not None else 0
        t = ev.title or "?"
        try:
            return f"{t} S{int(s):02d}E{int(n):02d}"
        except Exception:
            return t
    return ev.title or "?"


def _route_source(cfg: dict[str, Any]) -> tuple[str, str]:
    watch = ((cfg.get("scrobble") or {}).get("watch") or {}) if isinstance(cfg, dict) else {}
    source = str(watch.get("route_provider") or "watcher").strip().lower() or "watcher"
    source_instance = str(watch.get("route_provider_instance") or "default").strip() or "default"
    return source, source_instance


def _extract_skeleton_from_body(b: dict[str, Any]) -> dict[str, Any]:
    out = dict(b)
    out.pop("progress", None)
    out.pop("app_version", None)
    out.pop("app_date", None)
    return out


def _body_ids_desc(b: dict[str, Any]) -> str:
    ids = (
        (b.get("movie") or {}).get("ids")
        or (b.get("show") or {}).get("ids")
        or (b.get("episode") or {}).get("ids")
        or {}
    )
    return _ids_desc_map(ids if isinstance(ids, dict) else {})


class TraktSink(ScrobbleSink):
    def __init__(self, logger: Any | None = None, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._last_sent: dict[str, float] = {}
        self._p_sess: dict[tuple[str, str], float] = {}
        self._p_step: dict[tuple[str, str], float] = {}
        self._a_sess: dict[tuple[str, str], str] = {}
        self._p_glob: dict[str, float] = {}
        self._best: dict[str, dict[str, Any]] = {}
        self._last_intent_path: dict[str, str] = {}
        self._last_intent_prog: dict[str, float] = {}
        self._sessions: dict[str, dict[str, Any]] = {}
        self._queue: queue.Queue[Any] = queue.Queue()
        self._queue_lock = threading.RLock()
        self._worker: threading.Thread | None = None
        self._pending_starts: dict[str, dict[str, Any]] = {}
        self._warn_no_token = False
        self._warn_no_client = False

    def _mkey(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        parts: list[str] = []
        for k in ("tmdb", "imdb", "tvdb", "trakt"):
            if ids.get(k):
                parts.append(f"{k}:{ids[k]}")
        if ev.media_type == "episode":
            for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show"):
                if ids.get(k):
                    parts.append(f"{k}:{ids[k]}")
            parts.append(f"S{(ev.season or 0):02d}E{(ev.number or 0):02d}")
        if not parts:
            t = ev.title or ""
            y = ev.year or 0
            base = f"{t}|{y}"
            if ev.media_type == "episode":
                base += f"|S{(ev.season or 0):02d}E{(ev.number or 0):02d}"
            parts.append(base)
        return "|".join(parts)

    def _ckey(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        if ids.get("plex"):
            return f"plex:{ids.get('plex')}"
        return self._mkey(ev)

    def _debounced(self, session_key: str | None, action: str, debounce_s: int) -> bool:
        if action == "start":
            return False
        k = f"{session_key}:{action}"
        now = time.time()
        if now - self._last_sent.get(k, 0.0) < max(1, int(debounce_s)):
            return True
        self._last_sent[k] = now
        return False

    def _bodies(self, ev: ScrobbleEvent, p: float) -> list[dict[str, Any]]:
        ids = _ids(ev)
        show = _show_ids(ev)
        if ev.media_type == "movie":
            if ids:
                return [{"progress": p, "movie": {"ids": ids}}]
            m: dict[str, Any] = {"title": ev.title}
            if ev.year is not None:
                m["year"] = ev.year
            return [{"progress": p, "movie": m}]
        bodies: list[dict[str, Any]] = []
        has_sn = ev.season is not None and ev.number is not None
        if ids:
            bodies.append({"progress": p, "episode": {"ids": ids}})
        if has_sn and show:
            bodies.append(
                {
                    "progress": p,
                    "show": {"ids": show},
                    "episode": {"season": ev.season, "number": ev.number},
                }
            )
        if has_sn and not show:
            s: dict[str, Any] = {"title": ev.title}
            if ev.year is not None:
                s["year"] = ev.year
            bodies.append(
                {
                    "progress": p,
                    "show": s,
                    "episode": {"season": ev.season, "number": ev.number},
                }
            )
        return bodies or [{"progress": p, "episode": {"ids": ids}}]

    def _send_http(self, path: str, body: dict[str, Any], cfg: dict[str, Any]) -> dict[str, Any]:
        inst = self._instance_id
        backoff = 1.0
        tried_refresh = False

        for _ in range(6):
            try:
                r = _post(path, body, cfg, inst)
            except Exception:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

            s = r.status_code
            if s == 401 and not tried_refresh:
                _log("401 Unauthorized → refreshing token", "WARN")
                if _tok_refresh(inst):
                    tried_refresh = True
                    continue
                return {"ok": False, "status": 401, "resp": "Unauthorized and token refresh failed"}

            if s == 409:
                try:
                    return {"ok": True, "status": 409, "resp": r.json(), "duplicate": True}
                except Exception:
                    return {"ok": True, "status": 409, "resp": (r.text or "")[:400], "duplicate": True}

            if s == 429:
                try:
                    wait = float(r.headers.get("Retry-After") or backoff)
                except Exception:
                    wait = backoff
                time.sleep(max(0.5, min(30.0, wait)))
                backoff = min(8.0, backoff * 2)
                continue

            if 500 <= s < 600:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

            if s >= 400:
                short = (r.text or "")[:400]
                if s == 404:
                    short += " (Trakt could not match the item)"
                try:
                    j = r.json()
                    return {"ok": False, "status": s, "resp": j}
                except Exception:
                    return {"ok": False, "status": s, "resp": short}

            try:
                return {"ok": True, "status": s, "resp": r.json()}
            except Exception:
                return {"ok": True, "status": s, "resp": (r.text or "")[:400]}

        return {"ok": False, "status": 429, "resp": "rate_limited"}

    def _should_log_intent(self, key: str, path: str, prog: float) -> bool:
        last_p = self._last_intent_prog.get(key)
        last_path = self._last_intent_path.get(key)
        if last_path != path:
            ok = True
        elif last_p is None:
            ok = True
        else:
            ok = (float(prog) - float(last_p)) >= 5.0
        if ok:
            self._last_intent_path[key] = path
            self._last_intent_prog[key] = float(prog)
        return ok

    def _note_watch(self, ev: ScrobbleEvent, action: str, cfg: dict[str, Any], prog: Any, status: str = "ok", reason: str | None = None) -> None:
        try:
            src, src_inst = _route_source(cfg)
            record_watch(
                ev, action=action, source_provider=src, source_instance=src_inst,
                destination_provider="trakt", destination_instance=self._instance_id,
                status=status, progress=prog, reason=reason,
            )
        except Exception:
            pass

    def _session_state_key(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> str:
        src, src_inst = _route_source(cfg)
        sk = str(ev.session_key or "").strip() or self._mkey(ev)
        account = str(ev.account or "").strip().lower()
        return f"{src}:{src_inst}:{self._instance_id}:{account}:{sk}"

    def _bind_event(self, ev: ScrobbleEvent, cfg: dict[str, Any]) -> ScrobbleEvent:
        state_key = self._session_state_key(ev, cfg)
        now = time.time()
        existing = self._sessions.get(state_key)
        if ev.action == "start" and (ev.ids or ev.title):
            src, src_inst = _route_source(cfg)
            self._sessions[state_key] = {
                "session_key": ev.session_key,
                "media_key": self._mkey(ev),
                "event": ev,
                "ids": dict(ev.ids or {}),
                "account": ev.account,
                "source_instance": src_inst,
                "source_provider": src,
                "progress": _clamp(ev.progress),
                "max_progress": _clamp(ev.progress),
                "initial_filter_decision": True,
                "last_action": "start",
                "completed": False,
                "ts": now,
            }
            return ev
        if ev.action in ("pause", "stop") and isinstance(existing, dict) and isinstance(existing.get("event"), ScrobbleEvent):
            bound = existing["event"]
            raw = dict(bound.raw or {})
            raw.update(dict(ev.raw or {}))
            return replace(
                bound,
                action=ev.action,
                progress=ev.progress,
                account=ev.account or bound.account,
                server_uuid=ev.server_uuid or bound.server_uuid,
                session_key=ev.session_key or bound.session_key,
                raw=raw,
            )
        return ev

    def _update_session_state(self, ev: ScrobbleEvent, cfg: dict[str, Any], decision: _TraktDecision) -> None:
        state_key = self._session_state_key(ev, cfg)
        src, src_inst = _route_source(cfg)
        state = dict(self._sessions.get(state_key) or {})
        if not state.get("event") and (ev.ids or ev.title):
            state["event"] = ev
        prev_max = _clamp(state.get("max_progress", 0.0))
        state.update(
            {
                "session_key": ev.session_key,
                "media_key": self._mkey(ev),
                "ids": dict(ev.ids or {}),
                "account": ev.account,
                "source_instance": src_inst,
                "source_provider": src,
                "progress": decision.progress,
                "max_progress": max(prev_max, decision.progress),
                "initial_filter_decision": state.get("initial_filter_decision", True),
                "last_action": ev.action,
                "completed": bool(state.get("completed")) or decision.record_watched,
                "held": decision.held,
                "ts": time.time(),
            }
        )
        self._sessions[state_key] = state

    def _ensure_worker(self) -> None:
        with self._queue_lock:
            if self._worker and self._worker.is_alive():
                return
            self._worker = threading.Thread(target=self._worker_loop, name=f"TraktScrobble-{self._instance_id}", daemon=True)
            self._worker.start()

    def _enqueue(self, job: dict[str, Any]) -> None:
        self._ensure_worker()
        if job.get("action") == "start":
            key = str(job.get("coalesce_key") or "")
            with self._queue_lock:
                self._pending_starts[key] = job
            self._queue.put(("start", key))
            return
        self._queue.put(job)

    def _worker_loop(self) -> None:
        while True:
            item = self._queue.get()
            try:
                if isinstance(item, tuple) and len(item) == 2 and item[0] == "start":
                    with self._queue_lock:
                        job = self._pending_starts.pop(str(item[1]), None)
                    if isinstance(job, dict):
                        self._deliver(job)
                elif isinstance(item, dict):
                    self._deliver(item)
            except Exception as e:
                _log(f"Trakt scrobble worker error: {e}", "ERROR")
            finally:
                try:
                    self._queue.task_done()
                except Exception:
                    pass

    def _deliver(self, job: dict[str, Any]) -> None:
        ev = job["event"]
        cfg = job["cfg"]
        path = str(job["path"])
        p_send = float(job["progress"])
        action = str(job["action"])
        watched = bool(job.get("record_watched"))
        sk = str(ev.session_key or "?")
        mk = self._mkey(ev)
        key = self._ckey(ev)
        name = _media_name(ev)
        last_err: dict[str, Any] | None = None
        best = self._best.get(key)

        if not best and ev.media_type == "episode":
            found = _guid_search(ev, cfg, self._instance_id)
            if found:
                epi_ids = {"trakt": found["trakt"]} if "trakt" in found else found
                skeleton = {"episode": {"ids": epi_ids}}
                self._best[key] = {
                    "skeleton": skeleton,
                    "ids_desc": _ids_desc_map(epi_ids),
                    "ts": time.time(),
                }
                best = self._best.get(key)

        if best and isinstance(best.get("skeleton"), dict):
            bodies = [{"progress": p_send, **best["skeleton"], **_app_meta(cfg)}]
        else:
            bodies = [{**b, **_app_meta(cfg)} for b in self._bodies(ev, p_send)]

        for i, body in enumerate(bodies):
            prog_i = float(body.get("progress") or p_send)
            if best and i == 0:
                if self._should_log_intent(key, path, prog_i):
                    _log(f"intent path={path} ids={best.get('ids_desc','title/year')} p={body.get('progress')}", "DEBUG")
            elif self._should_log_intent(key, path, prog_i):
                _log(f"intent path={path} ids={_body_ids_desc(body)} p={body.get('progress')}", "DEBUG")
            res = self._send_http(path, body, cfg)
            if res.get("ok"):
                if res.get("duplicate"):
                    _log(f"send path={path} status=409 duplicate", "DEBUG")
                    self._a_sess[(sk, mk)] = action
                    return
                try:
                    act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
                except Exception:
                    act = path.rsplit("/", 1)[-1]
                _log(f"send path={path} status={res['status']} action={act}", "DEBUG")
                skeleton = _extract_skeleton_from_body(body)
                self._best[key] = {
                    "skeleton": skeleton,
                    "ids_desc": _body_ids_desc(body),
                    "ts": time.time(),
                }
                if watched:
                    src, src_inst = _route_source(cfg)
                    try:
                        record_scrobble_event(
                            ev,
                            source=src,
                            source_instance=src_inst,
                            target="trakt",
                            target_instance=self._instance_id,
                            progress=p_send,
                        )
                    except Exception:
                        pass
                    _auto_remove_across(ev, cfg, scope=f"trakt:{self._instance_id}")
                self._a_sess[(sk, mk)] = action
                try:
                    account = _mask_account(ev.account)
                    prog = float(body.get("progress") or p_send)
                    _log(f"scrobble {act} user='{account}' p={prog:.1f}% media='{name}'", "INFO")
                except Exception:
                    pass
                return
            last_err = res
            if res.get("status") == 404:
                _log("404 with current representation; trying alternate", "WARN")
                continue
            break

        if last_err and last_err.get("status") == 404 and ev.media_type == "episode":
            epi_ids = _guid_search(ev, cfg, self._instance_id)
            if epi_ids:
                body = {
                    "progress": p_send,
                    "episode": {"ids": epi_ids},
                    **_app_meta(cfg),
                }
                if self._should_log_intent(key, path, float(body.get("progress") or p_send)):
                    _log(f"intent path={path} ids={_ids_desc_map(epi_ids)} p={body.get('progress')}", "DEBUG")
                res = self._send_http(path, body, cfg)
                if res.get("ok"):
                    if res.get("duplicate"):
                        _log(f"send path={path} status=409 duplicate", "DEBUG")
                        self._a_sess[(sk, mk)] = action
                        return
                    try:
                        act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
                    except Exception:
                        act = path.rsplit("/", 1)[-1]
                    _log(f"send path={path} status={res['status']} action={act}", "DEBUG")
                    skeleton = _extract_skeleton_from_body(body)
                    self._best[key] = {
                        "skeleton": skeleton,
                        "ids_desc": _ids_desc_map(epi_ids),
                        "ts": time.time(),
                    }
                    if watched:
                        src, src_inst = _route_source(cfg)
                        try:
                            record_scrobble_event(
                                ev,
                                source=src,
                                source_instance=src_inst,
                                target="trakt",
                                target_instance=self._instance_id,
                                progress=p_send,
                            )
                        except Exception:
                            pass
                        _auto_remove_across(ev, cfg, scope=f"trakt:{self._instance_id}")
                    self._a_sess[(sk, mk)] = action
                    try:
                        account = _mask_account(ev.account)
                        prog = float(body.get("progress") or p_send)
                        _log(f"scrobble {act} user='{account}' p={prog:.1f}% media='{name}'", "INFO")
                    except Exception:
                        pass
                    return
                last_err = res

        if last_err:
            _log(f"{path} {last_err.get('status')} err={_safe_log_repr(last_err.get('resp'))}", "ERROR")
            if action in ("start", "stop"):
                self._note_watch(ev, action, cfg, p_send, status="fail", reason=str(last_err.get("status") or ""))

    def send(self, ev: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
        cfg = cfg or (self._cfg_provider() if self._cfg_provider else None) or _cfg()
        if not isinstance(cfg, dict):
            cfg = {}

        inst = self._instance_id
        t = _merged_provider_block(cfg, "trakt", inst)

        auth = cfg.get("auth") if isinstance(cfg, dict) else None
        auth_trakt_base = (auth or {}).get("trakt") if isinstance(auth, dict) else {}
        auth_trakt = _merged_provider_block({"trakt": auth_trakt_base} if isinstance(auth_trakt_base, dict) else {}, "trakt", inst)

        cfg = dict(cfg)
        cfg["trakt"] = dict(t)

        client_id = t.get("client_id") or t.get("api_key")
        token = _TOKEN_OVERRIDE.get(inst) or t.get("access_token") or auth_trakt.get("access_token")

        if not client_id:
            if not self._warn_no_client:
                _log("Missing trakt.client_id/api_key in config.json - skipping scrobble", "WARNING")
                self._warn_no_client = True
            return

        if not token:
            if not self._warn_no_token:
                _log("Missing Trakt access_token - connect Trakt to enable scrobble", "WARNING")
                self._warn_no_token = True
            return

        ev = self._bind_event(ev, cfg)
        sk = str(ev.session_key or "?")
        mk = self._mkey(ev)
        p_now = _clamp(ev.progress)
        decision = _trakt_decision(ev.action, p_now, cfg)
        self._update_session_state(ev, cfg, decision)

        p_glob = float(self._p_glob.get(mk, -1))
        p_sess = float(self._p_sess.get((sk, mk), -1))
        self._p_sess[(sk, mk)] = decision.progress
        if decision.progress > p_glob:
            self._p_glob[mk] = decision.progress

        if ev.action == "start":
            self._note_watch(ev, "start", cfg, decision.progress)

        if decision.path is None:
            self._a_sess[(sk, mk)] = ev.action
            return

        if ev.action == "start":
            step = float(_trakt_progress_step(cfg))
            last_act = self._a_sess.get((sk, mk))
            force_seek = bool((getattr(ev, "raw", None) or {}).get("_cw_seek"))
            if last_act == "start" and not force_seek and p_sess >= 0 and abs(decision.progress - p_sess) < step:
                return
        elif not decision.bypass_debounce and self._debounced(ev.session_key, ev.action, _watch_pause_debounce(cfg)):
            return

        self._enqueue(
            {
                "event": ev,
                "cfg": dict(cfg),
                "path": decision.path,
                "progress": decision.progress,
                "record_watched": decision.record_watched,
                "action": ev.action,
                "coalesce_key": f"{self._instance_id}:{sk}:{mk}",
            }
        )

# providers/scrobble/trakt/sink.py
# CrossWatch - Trakt.tv scrobble sink implementation
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json, time
from pathlib import Path
from typing import Any

import requests

from cw_platform.config_base import load_config, save_config

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None
    
try:
    from providers.auth._auth_TRAKT import PROVIDER as AUTH_TRAKT
except Exception:
    AUTH_TRAKT = None  # type: ignore[misc]

from providers.scrobble.scrobble import ScrobbleEvent, ScrobbleSink
from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from api.watchlistAPI import remove_across_providers_by_ids as _rm_across_api
except ImportError:
    _rm_across_api = None  # type: ignore[misc]


TRAKT_API = "https://api.trakt.tv"
APP_AGENT = "CrossWatch/Watcher/1.0"
_TOKEN_OVERRIDE: str | None = None
_AR_TTL = 60


def _cfg() -> dict[str, Any]:
    try:
        return load_config()
    except Exception:
        return {}


def _save_cfg(cfg: dict[str, Any]) -> None:
    try:
        save_config(cfg)
    except Exception:
        pass


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
            BASE_LOG(str(msg), level=lvl, module="TRAKT")
            return
        except Exception:
            pass
    print(f"[TRAKT:{lvl}] {msg}")


def _dbg(msg: str) -> None:
    _log(msg, "DEBUG")


def _app_meta(cfg: dict[str, Any]) -> dict[str, str]:
    rt = cfg.get("runtime") or {}
    ver = str(rt.get("version") or APP_AGENT)
    bdate = (rt.get("build_date") or "").strip()
    out: dict[str, str] = {"app_version": ver}
    if bdate:
        out["app_date"] = bdate
    return out


def _hdr(cfg: dict[str, Any]) -> dict[str, str]:
    t = cfg.get("trakt") or {}
    client_id = str(t.get("client_id") or t.get("api_key") or "")
    auth_trakt = (cfg.get("auth") or {}).get("trakt") or {}
    token = _TOKEN_OVERRIDE or t.get("access_token") or auth_trakt.get("access_token") or ""
    h: dict[str, str] = {
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": client_id,
        "User-Agent": APP_AGENT,
    }
    if token:
        h["Authorization"] = f"Bearer {token}"
    return h


def _get(path: str, cfg: dict[str, Any]) -> requests.Response:
    return requests.get(f"{TRAKT_API}{path}", headers=_hdr(cfg), timeout=10)


def _post(path: str, body: dict[str, Any], cfg: dict[str, Any]) -> requests.Response:
    return requests.post(f"{TRAKT_API}{path}", headers=_hdr(cfg), json=body, timeout=10)


def _del(path: str, cfg: dict[str, Any]) -> requests.Response:
    return requests.delete(f"{TRAKT_API}{path}", headers=_hdr(cfg), timeout=10)


def _tok_refresh(cfg: dict[str, Any]) -> bool:
    global _TOKEN_OVERRIDE

    if AUTH_TRAKT is None:
        _log("AUTH_TRAKT provider missing, cannot refresh token", "ERROR")
        return False

    try:
        res = AUTH_TRAKT.refresh(dict(cfg))
    except Exception as e:
        _log(f"Token refresh via AUTH_TRAKT failed: {e}", "ERROR")
        return False

    if not isinstance(res, dict) or not res.get("ok"):
        _log(f"Token refresh via AUTH_TRAKT failed: {res!r}", "ERROR")
        return False

    try:
        new_cfg = _cfg()
    except Exception:
        new_cfg = cfg

    trakt = (new_cfg.get("trakt") or {}) if isinstance(new_cfg, dict) else {}
    token = str(trakt.get("access_token") or "").strip()
    if not token:
        _log("Token refresh via AUTH_TRAKT succeeded but no access_token in config", "ERROR")
        return False

    _TOKEN_OVERRIDE = token
    _log("Trakt token refreshed via AUTH_TRAKT", "DEBUG")
    return True


def _ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}
    return {k: ids[k] for k in ("imdb", "tmdb", "tvdb", "trakt") if ids.get(k)}


def _show_ids(ev: ScrobbleEvent) -> dict[str, Any]:
    ids = ev.ids or {}
    out: dict[str, Any] = {}
    for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show"):
        if ids.get(k):
            out[k.replace("_show", "")] = ids[k]
    return out


def _clamp(p: Any) -> int:
    try:
        v = int(p)
    except Exception:
        v = 0
    return max(0, min(100, v))


def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("stop_pause_threshold", 85))
    except Exception:
        return 85


def _force_stop_at(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("force_stop_at", 95))
    except Exception:
        return 95


def _complete_at(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("complete_at", 0))
    except Exception:
        return 0


def _regress_tol(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("trakt") or {}).get("regress_tolerance_percent", 5))
    except Exception:
        return 5


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


def _guid_search(ev: ScrobbleEvent, cfg: dict[str, Any]) -> dict[str, Any] | None:
    ids = ev.ids or {}
    for key in ("imdb", "tvdb", "tmdb"):
        val = ids.get(key)
        if not val:
            continue
        try:
            r = _get(f"/search/{key}/{val}?type=episode", cfg)
        except Exception:
            continue
        if r.status_code == 401 and _tok_refresh(cfg):
            try:
                r = _get(f"/search/{key}/{val}?type=episode", cfg)
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
            out = {k: v for k, v in epi_ids.items() if k in ("trakt", "imdb", "tmdb", "tvdb") and v}
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


def _ar_key(ids: dict[str, Any], media_type: str) -> str:
    for k in ("imdb", "tmdb", "tvdb", "trakt", "simkl"):
        v = ids.get(k)
        if v:
            return f"{media_type}:{k}:{v}"
    try:
        return f"{media_type}:{json.dumps(ids, sort_keys=True)}"
    except Exception:
        return f"{media_type}:title/year"


def _norm_type(t: str) -> str:
    s = (t or "").strip().lower()
    if s.endswith("s"):
        s = s[:-1]
    if s == "series":
        s = "show"
    return s


def _cfg_delete_enabled(cfg: dict[str, Any], media_type: str) -> bool:
    s = cfg.get("scrobble") or {}
    if not s.get("delete_plex"):
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


def _auto_remove_across(ev: ScrobbleEvent, cfg: dict[str, Any]) -> None:
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
    key = _ar_key(ids, mt)
    if _ar_seen(key):
        _log("Auto-remove deduped (already handled by another sink)", "DEBUG")
        return
    try:
        _log(f"Auto-remove across providers ids={ids} media={mt}", "INFO")
        _rm_across(ids, mt)
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


def _clear_active_checkin(cfg: dict[str, Any]) -> bool:
    try:
        r = _del("/checkin", cfg)
        return r.status_code in (204, 200)
    except Exception:
        return False


def _ids_desc_map(ids: dict[str, Any]) -> str:
    for k in ("trakt", "imdb", "tmdb", "tvdb"):
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
    def __init__(self, logger: Any | None = None) -> None:
        self._last_sent: dict[str, float] = {}
        self._p_sess: dict[tuple[str, str], int] = {}
        self._p_glob: dict[str, int] = {}
        self._best: dict[str, dict[str, Any]] = {}
        self._last_intent_path: dict[str, str] = {}
        self._last_intent_prog: dict[str, int] = {}
        self._warn_no_token = False
        self._warn_no_client = False

    def _mkey(self, ev: ScrobbleEvent) -> str:
        ids = ev.ids or {}
        parts: list[str] = []
        for k in ("imdb", "tmdb", "tvdb", "trakt"):
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

    def _bodies(self, ev: ScrobbleEvent, p: int) -> list[dict[str, Any]]:
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
        backoff = 1.0
        tried_refresh = False
        tried_checkin_clear = False

        for _ in range(6):
            try:
                r = _post(path, body, cfg)
            except Exception:
                time.sleep(backoff)
                backoff = min(8.0, backoff * 2)
                continue

            s = r.status_code
            if s == 401 and not tried_refresh:
                _log("401 Unauthorized → refreshing token", "WARN")
                if _tok_refresh(cfg):
                    tried_refresh = True
                    continue
                return {"ok": False, "status": 401, "resp": "Unauthorized and token refresh failed"}

            if s == 409:
                txt = r.text or ""
                if not tried_checkin_clear and ("expires_at" in txt or "watched_at" in txt):
                    _log("409 Conflict (active check-in) — clearing /checkin and retrying", "WARN")
                    tried_checkin_clear = True
                    if _clear_active_checkin(cfg):
                        time.sleep(0.35)
                        continue
                return {"ok": False, "status": 409, "resp": txt[:400]}

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

    def _should_log_intent(self, key: str, path: str, prog: int) -> bool:
        last_p = self._last_intent_prog.get(key)
        last_path = self._last_intent_path.get(key)
        if last_path != path:
            ok = True
        elif last_p is None:
            ok = True
        else:
            ok = (prog - int(last_p)) >= 5
        if ok:
            self._last_intent_path[key] = path
            self._last_intent_prog[key] = int(prog)
        return ok

    def send(self, ev: ScrobbleEvent) -> None:
        cfg = _cfg()

        t = cfg.get("trakt") or {}
        auth_trakt = (cfg.get("auth") or {}).get("trakt") or {}
        client_id = t.get("client_id") or t.get("api_key")
        token = _TOKEN_OVERRIDE or t.get("access_token") or auth_trakt.get("access_token")

        if not client_id:
            if not self._warn_no_client:
                _log("Missing trakt.client_id/api_key in config.json — skipping scrobble", "ERROR")
                self._warn_no_client = True
            return

        if not token:
            if not self._warn_no_token:
                _log("Missing Trakt access_token — connect Trakt to enable scrobble", "ERROR")
                self._warn_no_token = True
            return

        sk = str(ev.session_key or "?")
        mk = self._mkey(ev)
        p_now = _clamp(ev.progress)
        tol = _regress_tol(cfg)
        p_sess = self._p_sess.get((sk, mk), -1)
        p_glob = self._p_glob.get(mk, -1)

        name = _media_name(ev)
        key = self._ckey(ev)

        if ev.action == "start":
            if p_now <= 2 and (p_sess >= 10 or p_glob >= 10):
                _log("Restart detected: align start floor to 2% (no 0%)", "DEBUG")
                p_send = 2
                self._p_glob[mk] = max(2, p_glob if p_glob >= 0 else 2)
                self._p_sess[(sk, mk)] = 2
            else:
                if p_now == 0 and p_glob > 0:
                    p_send = max(2, p_glob)
                elif p_glob >= 0 and (p_glob - p_now) > 0 and (p_glob - p_now) <= tol and p_now > 2:
                    p_send = p_glob
                else:
                    p_send = max(2, p_now)
        else:
            p_base = p_now
            if ev.action == "pause" and p_base >= 98 and p_sess >= 0 and p_sess < 95:
                _dbg(f"Clamp suspicious pause 100% → {p_sess}%")
                p_base = p_sess
            if p_sess < 0 or p_base >= p_sess or (p_sess - p_base) >= tol:
                p_send = p_base
            else:
                p_send = p_sess

        thr = _stop_pause_threshold(cfg)
        last_sess = p_sess
        action = ev.action

        comp = _complete_at(cfg)
        suppress_at = _watch_suppress_start_at(cfg)

        if action == "start" and p_send >= suppress_at:
            _log(f"suppress start at {p_send}% (>= {suppress_at}%)", "DEBUG")
            if p_send > (p_glob if p_glob >= 0 else -1):
                self._p_glob[mk] = p_send
            self._p_sess[(sk, mk)] = p_send
            return

        if comp and p_send >= comp and action not in ("stop", "start"):
            action = "stop"

        if ev.action == "stop":
            if p_send >= _force_stop_at(cfg) or (comp and p_send >= comp):
                action = "stop"
            elif p_send >= 98 and last_sess >= 0 and last_sess < thr and (p_send - last_sess) >= 30:
                _log(f"Demote STOP→PAUSE (jump {last_sess}%→{p_send}%, thr={thr})", "DEBUG")
                action = "pause"
                p_send = last_sess
            elif p_send < thr:
                action = "pause"

        step = _trakt_progress_step(cfg)
        p_gate = int(float(p_send))
        if action == "start" and step > 1:
            p_i = int(float(p_send))
            bucket = (p_i // step) * step
            if p_sess >= 0 and bucket <= int(p_sess):
                return
            if p_i % step == 0:
                p_gate = bucket
            else:
                p_gate = (bucket + step) if bucket > 0 else step
        p_gate = max(1, min(100, int(p_gate)))
        if p_gate != p_sess:
            self._p_sess[(sk, mk)] = p_gate
        if p_send > (p_glob if p_glob >= 0 else -1):
            self._p_glob[mk] = p_send

        comp_thr = max(_force_stop_at(cfg), comp or 0)
        if not (action == "stop" and p_send >= comp_thr):
            if self._debounced(ev.session_key, action, _watch_pause_debounce(cfg)):
                return

        path = {
            "start": "/scrobble/start",
            "pause": "/scrobble/pause",
            "stop": "/scrobble/stop",
        }[action]

        last_err: dict[str, Any] | None = None
        best = self._best.get(key)

        if not best and ev.media_type == "episode":
            found = _guid_search(ev, cfg)
            if found:
                epi_ids = {"trakt": found["trakt"]} if "trakt" in found else found
                skeleton = {"episode": {"ids": epi_ids}}
                self._best[key] = {
                    "skeleton": skeleton,
                    "ids_desc": _ids_desc_map(epi_ids),
                    "ts": time.time(),
                }
                best = self._best.get(key)

        bodies: list[dict[str, Any]] = []
        if best and isinstance(best.get("skeleton"), dict):
            b0 = {"progress": p_send, **best["skeleton"], **_app_meta(cfg)}
            if self._should_log_intent(key, path, int(b0.get("progress") or p_send)):
                _log(
                    f"trakt intent {path} using cached {best.get('ids_desc','title/year')}, "
                    f"prog={b0.get('progress')}",
                    "DEBUG",
                )
            bodies.append(b0)
        else:
            bodies = [{**b, **_app_meta(cfg)} for b in self._bodies(ev, p_send)]

        for i, body in enumerate(bodies):
            if not (best and i == 0):
                prog_i = int(float(body.get("progress") or p_send))
                if self._should_log_intent(key, path, prog_i):
                    _log(f"trakt intent {path} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")
            res = self._send_http(path, body, cfg)
            if res.get("ok"):
                try:
                    act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
                except Exception:
                    act = path.rsplit("/", 1)[-1]
                _log(f"trakt {path} -> {res['status']} action={act}", "DEBUG")
                skeleton = _extract_skeleton_from_body(body)
                self._best[key] = {
                    "skeleton": skeleton,
                    "ids_desc": _body_ids_desc(body),
                    "ts": time.time(),
                }
                if action == "stop" and p_send >= comp_thr:
                    _auto_remove_across(ev, cfg)
                try:
                    _log(
                        f"user='{ev.account}' {act} {float(body.get('progress') or p_send):.1f}% • {name}",
                        "INFO",
                    )
                except Exception:
                    pass
                return
            last_err = res
            if res.get("status") == 404:
                _log("404 with current representation → trying alternate", "WARN")
                continue
            break

        if last_err and last_err.get("status") == 404 and ev.media_type == "episode":
            epi_ids = _guid_search(ev, cfg)
            if epi_ids:
                body = {
                    "progress": p_send,
                    "episode": {"ids": epi_ids},
                    **_app_meta(cfg),
                }
                if self._should_log_intent(key, path, int(body.get("progress") or p_send)):
                    _log(
                        f"trakt intent {path} using {_ids_desc_map(epi_ids)}, "
                        f"prog={body.get('progress')}",
                        "DEBUG",
                    )
                res = self._send_http(path, body, cfg)
                if res.get("ok"):
                    try:
                        act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
                    except Exception:
                        act = path.rsplit("/", 1)[-1]
                    _log(f"trakt {path} -> {res['status']} action={act}", "DEBUG")
                    skeleton = _extract_skeleton_from_body(body)
                    self._best[key] = {
                        "skeleton": skeleton,
                        "ids_desc": _ids_desc_map(epi_ids),
                        "ts": time.time(),
                    }
                    if action == "stop" and p_send >= comp_thr:
                        _auto_remove_across(ev, cfg)
                    try:
                        _log(
                            f"user='{ev.account}' {act} {float(body.get('progress') or p_send):.1f}% • {name}",
                            "INFO",
                        )
                    except Exception:
                        pass
                    return
                last_err = res

        if last_err and last_err.get("status") == 409 and action == "stop" and (
            "watched_at" in str(last_err.get("resp"))
        ):
            _log("Treating 409 with watched_at as watched; proceeding to auto-remove", "WARN")
            if p_send >= comp_thr:
                _auto_remove_across(ev, cfg)
            return

        if last_err:
            _log(f"{path} {last_err.get('status')} err={last_err.get('resp')}", "ERROR")

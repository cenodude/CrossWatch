# providers/scrobble/mdblist/sink.py
# CrossWatch - Scrobble MDBList Sink
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import time
from pathlib import Path
from typing import Any

import requests

from cw_platform.config_base import load_config
from cw_platform.provider_instances import normalize_instance_id

try:
    from _logging import log as BASE_LOG
except Exception:
    BASE_LOG = None

from providers.scrobble._auto_remove_watchlist import remove_across_providers_by_ids as _rm_across
try:
    from providers.scrobble.scrobble import ScrobbleSink, ScrobbleEvent  # type: ignore
except ImportError:
    class ScrobbleSink:
        def send(self, event: Any) -> None: ...

    class ScrobbleEvent:  # pragma: no cover
        ...


MDBLIST_API = "https://api.mdblist.com"
APP_AGENT = "CrossWatch/Watcher/1.0"
_AR_TTL = 60

_RESOLVE_TTL_S = 30 * 86400
_RESOLVE_NEG_TTL_S = 6 * 3600

_TVDB_SHOW_ID_MAX = 9_999_999


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


def _log(msg: str, lvl: str = "INFO") -> None:
    level = (str(lvl) or "INFO").upper()
    if level == "DEBUG" and not _is_debug():
        return
    if BASE_LOG is not None:
        try:
            BASE_LOG(str(msg), level=level, module="MDBLIST")
            return
        except Exception:
            pass
    print(f"[MDBLIST:{level}] {msg}")


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
    av = str(rt.get("version") or APP_AGENT)
    ad = (rt.get("build_date") or "").strip()
    return {"app_version": av, **({"app_date": ad} if ad else {})}


def _timeout(cfg: dict[str, Any]) -> float:
    try:
        m = cfg.get("mdblist") or {}
        return float(m.get("timeout", 10))
    except Exception:
        return 10.0


def _max_retries(cfg: dict[str, Any]) -> int:
    try:
        m = cfg.get("mdblist") or {}
        return int(m.get("max_retries", 3))
    except Exception:
        return 3


def _stop_pause_threshold(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("stop_pause_threshold", 85))
    except Exception:
        return 85


def _force_stop_at(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("force_stop_at", 95))
    except Exception:
        return 95


def _complete_at(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("complete_at", 0))
    except Exception:
        return 0


def _regress_tolerance_percent(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        return int((s.get("trakt") or {}).get("regress_tolerance_percent", 5))
    except Exception:
        return 5


def _watch_pause_debounce(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("watch") or {}).get("pause_debounce_seconds", 5))
    except Exception:
        return 5


def _watch_suppress_start_at(cfg: dict[str, Any]) -> int:
    try:
        return int(((cfg.get("scrobble") or {}).get("watch") or {}).get("suppress_start_at", 99))
    except Exception:
        return 99

def _progress_step(cfg: dict[str, Any]) -> int:
    try:
        s = cfg.get("scrobble") or {}
        step = (s.get("mdblist") or {}).get("progress_step")
        if step is None:
            step = (s.get("trakt") or {}).get("progress_step", 5)
        step_i = int(step)
    except Exception:
        step_i = 5
    return max(1, min(25, step_i))


def _quantize_progress(prog: int, step: int, action: str) -> int:
    p = _clamp(prog)
    if step <= 1 or action == "stop":
        return p
    if p < step:
        return max(1, p)
    q = (p // step) * step
    return max(1, min(100, q))


def _clamp(p: Any) -> int:
    try:
        v = int(float(p))
    except Exception:
        v = 0
    return max(0, min(100, v))


def _state_dir() -> Path:
    base = Path("/config/.cw_state") if Path("/config/config.json").exists() else Path(".cw_state")
    try:
        base.mkdir(parents=True, exist_ok=True)
    except Exception:
        pass
    return base


def _as_int(v: Any) -> int | None:
    try:
        return int(v)
    except Exception:
        return None

def _imdb_id_sane(v: Any) -> str | None:
    s = str(v or "").strip()
    if not s:
        return None
    if not s.startswith("tt"):
        return None
    tail = s[2:]
    if not tail.isdigit():
        return None
    if len(tail) < 6:
        return None
    return s


def _tvdb_show_id_sane(v: Any) -> int | None:
    i = _as_int(v)
    if not i or i <= 0:
        return None
    return i if i <= _TVDB_SHOW_ID_MAX else None


def _norm_type(s: str) -> str:
    s = (s or "").strip().lower()
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


def _ids(ev: Any) -> dict[str, Any]:
    ids = getattr(ev, "ids", {}) or {}
    out: dict[str, Any] = {}
    imdb = _imdb_id_sane(ids.get("imdb"))
    if imdb:
        out["imdb"] = imdb
    for k in ("tmdb", "trakt", "kitsu"):
        if ids.get(k) is None:
            continue
        try:
            out[k] = int(ids[k])
        except Exception:
            continue
    if ids.get("mdblist"):
        out["mdblist"] = str(ids["mdblist"])
    return out


def _show_ids(ev: Any) -> dict[str, Any]:
    ids = getattr(ev, "ids", {}) or {}
    m: dict[str, Any] = {}

    imdb = _imdb_id_sane(ids.get("imdb_show"))
    if imdb:
        m["imdb"] = imdb

    for k in ("tmdb_show", "trakt_show", "kitsu_show"):
        if ids.get(k) is None:
            continue
        try:
            m[k.replace("_show", "")] = int(ids[k])
        except Exception:
            continue

    if ids.get("tvdb_show") is not None:
        sane = _tvdb_show_id_sane(ids.get("tvdb_show"))
        if sane is not None:
            m["tvdb"] = sane

    if ids.get("mdblist_show"):
        m["mdblist"] = str(ids["mdblist_show"])

    if getattr(ev, "media_type", "") == "episode" and "tvdb" not in m and ids.get("tvdb"):
        sane = _tvdb_show_id_sane(ids.get("tvdb"))
        if sane is not None:
            m["tvdb"] = sane
    return m


def _best_ids_for_scrobble(ids: dict[str, Any], media_type: str) -> dict[str, Any]:
    mt = _norm_type(media_type)
    if mt == "show":
        order = ("imdb", "trakt", "tmdb", "tvdb", "mdblist")
    else:
        order = ("imdb", "tmdb", "trakt", "mdblist")

    out: dict[str, Any] = {}
    for k in order:
        v = ids.get(k)
        if v is None or v == "" or v == 0:
            continue
        if k == "imdb":
            sane = _imdb_id_sane(v)
            if sane:
                out["imdb"] = sane
            continue
        if k in ("trakt", "tmdb", "tvdb"):
            try:
                out[k] = int(v)
            except Exception:
                continue
            continue
        out[k] = str(v)

    return out
def _ar_key(ids: dict[str, Any], media_type: str) -> str:
    parts = [media_type]
    for k in ("imdb", "tmdb", "tvdb", "trakt", "kitsu", "mdblist"):
        if ids.get(k):
            parts.append(f"{k}:{ids[k]}")
    return "|".join(parts)


def _ar_state_file() -> str:
    try:
        return str(_state_dir() / "auto_remove_seen.json")
    except Exception:
        return ".cw_state/auto_remove_seen.json"


def _ar_seen(key: str) -> bool:
    p = _ar_state_file()
    try:
        data = json.loads(open(p, "r", encoding="utf-8").read()) or {}
    except Exception:
        data = {}
    now = time.time()
    try:
        data = {k: v for k, v in data.items() if (now - float(v)) < _AR_TTL}
    except Exception:
        data = {}
    if key in data:
        try:
            open(p, "w", encoding="utf-8").write(json.dumps(data))
        except Exception:
            pass
        return True
    data[key] = now
    try:
        open(p, "w", encoding="utf-8").write(json.dumps(data))
    except Exception:
        pass
    return False


def _auto_remove_across(ev: Any, cfg: dict[str, Any]) -> None:
    mt = _norm_type(str(getattr(ev, "media_type", "") or ""))
    if not _cfg_delete_enabled(cfg, mt):
        return
    ids = _show_ids(ev) if mt == "episode" else _ids(ev)
    if not ids:
        ids = _ids(ev)
    if not ids:
        return
    key = _ar_key(ids, mt)
    if _ar_seen(key):
        return
    try:
        _rm_across(ids, mt)
    except Exception:
        pass


def _media_name(ev: Any) -> str:
    if getattr(ev, "media_type", "") == "episode":
        s = int(getattr(ev, "season", 0) or 0)
        n = int(getattr(ev, "number", 0) or 0)
        t = getattr(ev, "title", None) or "?"
        return f"{t} S{s:02d}E{n:02d}"
    t = getattr(ev, "title", None) or "?"
    y = getattr(ev, "year", None)
    return f"{t} ({y})" if y else t


def _bodies(ev: Any, progress: float) -> list[dict[str, Any]]:
    mt = getattr(ev, "media_type", "") or ""
    if mt == "episode":
        raw = _show_ids(ev)
        sh_ids = _best_ids_for_scrobble(raw, "show")
        season = int(getattr(ev, "season", 0) or 0)
        number = int(getattr(ev, "number", 0) or 0)
        show: dict[str, Any] = {"ids": sh_ids} if sh_ids else {}
        if not sh_ids:
            title = getattr(ev, "title", None)
            year = getattr(ev, "year", None)
            if title:
                show["title"] = title
            if year:
                show["year"] = int(year)
        show["season"] = {"number": season, "episode": {"number": number}}
        return [{"show": show, "progress": progress}]

    raw_ids = _ids(ev)
    ids = _best_ids_for_scrobble(raw_ids, "movie")
    movie: dict[str, Any] = {"ids": ids} if ids else {}
    if not ids:
        title = getattr(ev, "title", None)
        year = getattr(ev, "year", None)
        if title:
            movie["title"] = title
        if year:
            movie["year"] = int(year)
    return [{"movie": movie, "progress": progress}]


def _ids_desc_map(ids: dict[str, Any], order: tuple[str, ...]) -> str:
    for k in order:
        v = ids.get(k)
        if v is not None and v != "" and v != 0:
            return f"{k}:{v}"
    return "title/year"


def _extract_skeleton_from_body(b: dict[str, Any]) -> dict[str, Any]:
    out = dict(b)
    out.pop("progress", None)
    out.pop("app_version", None)
    out.pop("app_date", None)
    return out


def _body_ids_desc(b: dict[str, Any]) -> str:
    if "show" in b:
        ids = ((b.get("show") or {}).get("ids") or {})
        order = ("imdb", "trakt", "tmdb", "tvdb", "mdblist")
    elif "movie" in b:
        ids = ((b.get("movie") or {}).get("ids") or {})
        order = ("imdb", "tmdb", "trakt", "mdblist")
    else:
        ids = (
            (b.get("movie") or {}).get("ids")
            or (b.get("show") or {}).get("ids")
            or (b.get("episode") or {}).get("ids")
            or {}
        )
        order = ("imdb", "tmdb", "trakt", "tvdb", "mdblist")
    return _ids_desc_map(ids if isinstance(ids, dict) else {}, order)


class MDBListSink(ScrobbleSink):
    def __init__(self, cfg_provider: Callable[[], dict[str, Any]] | None = None, instance_id: str | None = None) -> None:
        self._cfg_provider = cfg_provider
        self._instance_id = normalize_instance_id(instance_id)
        self._last_sent: dict[str, float] = {}
        self._p_glob: dict[str, int] = {}
        self._p_sess: dict[tuple[str, str], int] = {}
        self._p_step: dict[tuple[str, str], int] = {}
        self._a_sess: dict[tuple[str, str], str] = {}
        self._best: dict[str, dict[str, Any]] = {}
        self._last_intent_path: dict[str, str] = {}
        self._last_intent_prog: dict[str, int] = {}
        self._warn_no_key = False

    def _mkey(self, ev: Any) -> str:
        ids = getattr(ev, "ids", {}) or {}
        parts: list[str] = []
        for k in ("imdb", "tmdb", "trakt", "kitsu", "mdblist"):
            if ids.get(k):
                parts.append(f"{k}:{ids[k]}")
        if getattr(ev, "media_type", "") == "episode":
            for k in ("imdb_show", "tmdb_show", "tvdb_show", "trakt_show", "kitsu_show", "mdblist_show"):
                if ids.get(k):
                    parts.append(f"{k}:{ids[k]}")
            parts.append(f"S{int(getattr(ev, 'season', 0) or 0):02d}E{int(getattr(ev, 'number', 0) or 0):02d}")
        if not parts:
            t = getattr(ev, "title", None) or ""
            y = getattr(ev, "year", None) or 0
            base = f"{t}|{y}"
            if getattr(ev, "media_type", "") == "episode":
                base += f"|S{int(getattr(ev, 'season', 0) or 0):02d}E{int(getattr(ev, 'number', 0) or 0):02d}"
            parts.append(base)
        return "|".join(parts)

    def _ckey(self, ev: Any) -> str:
        ids = getattr(ev, "ids", {}) or {}
        if ids.get("plex"):
            return f"plex:{ids.get('plex')}"
        return self._mkey(ev)

    def _debounced(self, session_key: str | None, action: str, debounce_s: int) -> bool:
        if action == "start":
            return False
        k = f"{session_key or '?'}:{action}"
        now = time.time()
        if now - self._last_sent.get(k, 0.0) < max(1, int(debounce_s)):
            return True
        self._last_sent[k] = now
        return False

    def _should_log_intent(self, key: str, path: str, prog: int) -> bool:
        lp = self._last_intent_path.get(key)
        pp = self._last_intent_prog.get(key, -1)
        changed = (lp != path) or (abs(int(prog) - int(pp)) >= 5)
        if changed:
            self._last_intent_path[key] = path
            self._last_intent_prog[key] = int(prog)
        return changed

    def _post(self, path: str, body: dict[str, Any], api_key: str, cfg: dict[str, Any]) -> requests.Response:
        headers = {"Accept": "application/json", "Content-Type": "application/json", "User-Agent": APP_AGENT}
        return requests.post(
            f"{MDBLIST_API}{path}",
            headers=headers,
            params={"apikey": api_key},
            json=body,
            timeout=_timeout(cfg),
        )

    def _send_http(self, path: str, body: dict[str, Any], api_key: str, cfg: dict[str, Any]) -> dict[str, Any]:
        max_retries = max(0, _max_retries(cfg))
        backoff = 0.6
        last_err: str | None = None
        for attempt in range(max_retries + 1):
            try:
                r = self._post(path, body, api_key, cfg)
            except Exception as e:
                last_err = f"request_error:{e}"
                if attempt >= max_retries:
                    break
                time.sleep(min(6.0, backoff))
                backoff *= 1.8
                continue

            if r.status_code in (200, 201, 204):
                try:
                    resp: Any = r.json()
                except Exception:
                    resp = (getattr(r, "text", "") or "")[:400]
                return {"ok": True, "status": r.status_code, "resp": resp}

            if r.status_code == 401:
                return {"ok": False, "status": 401, "error": "invalid_api_key"}

            if r.status_code == 429:
                ra = r.headers.get("Retry-After")
                try:
                    wait_s = max(0.5, float(ra)) if ra else 2.0
                except Exception:
                    wait_s = 2.0
                if attempt >= max_retries:
                    return {"ok": False, "status": 429, "error": "rate_limited"}
                time.sleep(min(15.0, wait_s))
                continue

            if 500 <= r.status_code <= 599 and attempt < max_retries:
                time.sleep(min(6.0, backoff))
                backoff *= 1.8
                continue

            try:
                j = r.json()
            except Exception:
                j = (getattr(r, "text", "") or "")[:250]
            return {"ok": False, "status": r.status_code, "error": j, "resp": j}

        return {"ok": False, "status": 0, "error": last_err or "unknown"}



    def send(self, ev: ScrobbleEvent, cfg: dict[str, Any] | None = None) -> None:
        cfg = cfg or (self._cfg_provider() if self._cfg_provider else None) or _cfg()
        if not isinstance(cfg, dict):
            cfg = {}
        cfg = dict(cfg)
        cfg["mdblist"] = _merged_provider_block(cfg, "mdblist", self._instance_id)
        m = cfg.get("mdblist") or {}
        api_key = str(m.get("api_key") or "").strip()

        if not api_key:
            if not self._warn_no_key:
                _log("Missing mdblist.api_key in config.json — skipping scrobble", "ERROR")
                self._warn_no_key = True
            return

        action_in = (getattr(ev, "action", "") or "").lower().strip()
        action = action_in if action_in in ("start", "pause", "stop") else "stop"

        sk = str(getattr(ev, "session_key", None) or "?")
        mk = self._mkey(ev)
        p_now = _clamp(getattr(ev, "progress", 0) or 0)
        tol = _regress_tolerance_percent(cfg)
        p_sess = self._p_sess.get((sk, mk), -1)
        p_glob = self._p_glob.get(mk, -1)

        last_act = self._a_sess.get((sk, mk))
        last_bucket = self._p_step.get((sk, mk), -1)

        name = _media_name(ev)
        key = self._ckey(ev)

        if action == "start":
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
            if action == "pause" and p_base >= 98 and p_sess >= 0 and p_sess < 95:
                _log(f"Clamp suspicious pause 100% → {p_sess}%", "DEBUG")
                p_base = p_sess
            if p_sess < 0 or p_base >= p_sess or (p_sess - p_base) >= tol:
                p_send = p_base
            else:
                p_send = p_sess

        thr = _stop_pause_threshold(cfg)
        last_sess = p_sess
        comp = _complete_at(cfg)
        suppress_at = float(_watch_suppress_start_at(cfg))

        if action == "start" and p_send >= suppress_at:
            _log(f"suppress start at {p_send}% (>= {suppress_at}%)", "DEBUG")
            if p_send > (p_glob if p_glob >= 0 else -1):
                self._p_glob[mk] = p_send
            self._p_sess[(sk, mk)] = p_send
            return

        if comp and p_send >= comp and action not in ("stop", "start"):
            action = "stop"

        if action_in == "stop":
            if p_send >= _force_stop_at(cfg) or (comp and p_send >= comp):
                action = "stop"
            elif p_send >= 98 and last_sess >= 0 and last_sess < thr and (p_send - last_sess) >= 30:
                _log(f"Demote STOP→PAUSE (jump {last_sess}%→{p_send}%, thr={thr})", "DEBUG")
                action = "pause"
                p_send = last_sess

        step = _progress_step(cfg)
        p_payload = int(float(p_send))
        bucket: int | None = None
        if action == "start" and step > 1:
            bucket = (int(float(p_send)) // step) * step
            if bucket < 1:
                bucket = 1
            if last_act == "start" and last_bucket >= 0 and bucket <= int(last_bucket):
                self._p_sess[(sk, mk)] = int(p_send)
                if int(p_send) > (p_glob if p_glob >= 0 else -1):
                    self._p_glob[mk] = int(p_send)
                return
            if last_act == "start":
                p_payload = bucket

        self._p_sess[(sk, mk)] = int(p_send)
        if int(p_send) > (p_glob if p_glob >= 0 else -1):
            self._p_glob[mk] = int(p_send)

        comp_thr = max(_force_stop_at(cfg), comp or 0)
        if not (action == "stop" and p_send >= comp_thr):
            if self._debounced(sk, action, _watch_pause_debounce(cfg)):
                return

        path = {"start": "/scrobble/start", "pause": "/scrobble/pause", "stop": "/scrobble/stop"}[action]

        key0 = key
        key = self._ckey(ev) or key0

        best = self._best.get(key) or self._best.get(key0)
        best_skel: dict[str, Any] | None = None
        best_desc = "title/year"
        if isinstance(best, dict):
            skel = best.get("skeleton")
            if isinstance(skel, dict):
                best_skel = skel
                bd = best.get("ids_desc")
                if isinstance(bd, str):
                    best_desc = bd

        bodies = [{**b, **_app_meta(cfg)} for b in _bodies(ev, float(p_payload))]
        if best_skel is not None:
            b0 = {"progress": float(p_payload), **best_skel, **_app_meta(cfg)}
            if self._should_log_intent(key, path, int(float(b0.get("progress") or p_send))):
                _log(f"mdblist intent {path} using cached {best_desc}, prog={b0.get('progress')}", "DEBUG")
            bodies = [b0] + [b for b in bodies if _body_ids_desc(b) != best_desc]

        sent_ok = False
        for i, body in enumerate(bodies):
            if not (best_skel is not None and i == 0):
                intent_prog = int(float(body.get("progress") or p_send))
                if self._should_log_intent(key, path, intent_prog):
                    _log(f"mdblist intent {path} using {_body_ids_desc(body)}, prog={body.get('progress')}", "DEBUG")

            res = self._send_http(path, body, api_key, cfg)
            if not res.get("ok"):
                _log(f"{path} failed for {name}: {res}", "WARN")
                continue

            sent_ok = True

            self._a_sess[(sk, mk)] = action
            if action == "start" and step > 1 and bucket is not None:
                self._p_step[(sk, mk)] = int(bucket)
            item = {"skeleton": _extract_skeleton_from_body(body), "ids_desc": _body_ids_desc(body), "ts": time.time()}
            self._best[key] = item
            self._best[key0] = item

            try:
                act = (res.get("resp") or {}).get("action") or path.rsplit("/", 1)[-1]
            except Exception:
                act = path.rsplit("/", 1)[-1]
            _log(f"mdblist {path} -> {res.get('status')} action={act}", "DEBUG")
            try:
                acc = getattr(ev, "account", None)
                prog_val = float(body.get("progress") or p_send)
                _log(f"user='{acc}' {act} {prog_val:.1f}% • {name}", "INFO")
            except Exception:
                pass

        if sent_ok and action == "stop" and int(p_send) >= comp_thr:
            _auto_remove_across(ev, cfg)
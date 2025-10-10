# _syncAPI.py
from __future__ import annotations

# --- stdlib ---
from typing import Any, Dict, List, Optional
from pathlib import Path
from datetime import datetime, timezone, date
import dataclasses as _dc, importlib, inspect, json, os, pkgutil, re, shlex, threading, time, uuid

# --- third-party ---
from fastapi import APIRouter, Body
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

__all__ = ["router", "_is_sync_running", "_load_state", "_find_state_path", "_persist_state_via_orc"]

# Router
router = APIRouter(prefix="/api", tags=["Synchronization"])

# ----- Runtime/env helpers -----
def _env():
    from cw_platform.config_base import load_config, save_config
    return load_config, save_config

def _rt():
    # Bind to the running app module
    import sys, importlib
    m = sys.modules.get("crosswatch") or sys.modules.get("__main__")
    if m is None or not hasattr(m, "LOG_BUFFERS"):
        m = importlib.import_module("crosswatch")
    return (
        m.LOG_BUFFERS,      # 0
        m.RUNNING_PROCS,    # 1
        m.SYNC_PROC_LOCK,   # 2
        m.STATE_PATH,       # 3
        m.STATE_PATHS,      # 4
        m.STATS,            # 5
        m.REPORT_DIR,       # 6
        m.strip_ansi,       # 7
        m._append_log,      # 8
        m.minimal,          # 9
        m.canonical_key,    # 10
    )

# ----- Pair helpers -----
FEATURE_KEYS = ["watchlist", "ratings", "history", "playlists"]

# Ratings schema constants
_ALLOWED_RATING_TYPES: tuple[str, ...] = ("movies", "shows", "seasons", "episodes")
_ALLOWED_RATING_MODES: tuple[str, ...] = ("only_new", "from_date", "all")

def _normalize_ratings_block(v: dict | bool | None) -> dict:
    # Bool → dict; ensure sane defaults
    if isinstance(v, bool):
        return {
            "enable": bool(v), "add": bool(v), "remove": False,
            "types": ["movies", "shows"], "mode": "only_new", "from_date": ""
        }

    d = dict(v or {})
    d["enable"] = bool(d.get("enable", d.get("enabled", False)))
    d["add"]    = bool(d.get("add", True))
    d["remove"] = bool(d.get("remove", False))

    t = d.get("types", [])
    if isinstance(t, str): t = [t]
    t_norm = [str(x).strip().lower() for x in t if isinstance(x, str)]
    if "all" in t_norm:
        d["types"] = list(_ALLOWED_RATING_TYPES)
    else:
        keep = [x for x in _ALLOWED_RATING_TYPES if x in t_norm]
        d["types"] = keep or ["movies", "shows"]

    mode = str(d.get("mode", "only_new")).strip().lower()
    d["mode"] = mode if mode in _ALLOWED_RATING_MODES else "only_new"

    fd = str(d.get("from_date", "") or "").strip()
    if d["mode"] == "from_date":
        try:
            iso = date.fromisoformat(fd).isoformat()
            if date.fromisoformat(iso) > date.today():
                d["mode"], d["from_date"] = "only_new", ""
            else:
                d["from_date"] = iso
        except Exception:
            d["mode"], d["from_date"] = "only_new", ""
    else:
        d["from_date"] = ""

    return d

def _ensure_pair_ratings_defaults(cfg: Dict[str, Any]) -> None:
    # Normalize ratings blocks on read
    for p in (cfg.get("pairs") or []):
        feats = p.setdefault("features", {})
        feats["ratings"] = _normalize_ratings_block(feats.get("ratings"))

def _normalize_features(f: dict | None) -> dict:
    # Apply uniform feature shape
    f = dict(f or {})
    for k in FEATURE_KEYS:
        v = f.get(k)
        if k == "ratings":
            f[k] = _normalize_ratings_block(v)
        elif isinstance(v, bool):
            f[k] = {"enable": bool(v), "add": bool(v), "remove": False}
        elif isinstance(v, dict):
            v.setdefault("enable", True)
            v.setdefault("add", True)
            v.setdefault("remove", False)
    return f

def _cfg_pairs(cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
    arr = cfg.get("pairs")
    if not isinstance(arr, list): arr = []; cfg["pairs"] = arr
    return arr

def _gen_id(prefix: str = "pair") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

# ----- Orchestrator summary (in-memory) -----
SUMMARY_LOCK = threading.Lock()
SUMMARY: Dict[str, Any] = {}

def _summary_reset() -> None:
    with SUMMARY_LOCK:
        SUMMARY.clear()
        SUMMARY.update({
            "running": False, "started_at": None, "finished_at": None, "duration_sec": None,
            "cmd": "", "version": "", "plex_pre": None, "simkl_pre": None, "plex_post": None, "simkl_post": None,
            "result": "", "exit_code": None, "timeline": {"start": False, "pre": False, "post": False, "done": False},
            "raw_started_ts": None,
        })

def _summary_set(k: str, v: Any) -> None:
    with SUMMARY_LOCK: SUMMARY[k] = v

def _summary_set_timeline(flag: str, value: bool = True) -> None:
    with SUMMARY_LOCK:
        SUMMARY.setdefault("timeline", {}); SUMMARY["timeline"][flag] = value

def _summary_snapshot() -> Dict[str, Any]:
    with SUMMARY_LOCK: return dict(SUMMARY)

# ----- Sync progress + parsing -----
def _sync_progress_ui(msg: str):
    rt=_rt(); LOG_BUFFERS, strip_ansi, _append_log = rt[0], rt[7], rt[8]
    try:
        _append_log("SYNC", msg)
        try: _parse_sync_line(strip_ansi(msg))
        except Exception as e: _append_log("SYNC", f"[!] progress-parse failed: {e}")
    except Exception: pass

def _orc_progress(event: str, data: dict):
    rt=_rt(); _append_log = rt[8]
    try: payload = json.dumps({"event": event, **(data or {})}, default=str)
    except Exception: payload = f"{event} | {data}"
    _append_log("SYNC", payload[:2000])

def _feature_enabled(fmap: dict, name: str) -> tuple[bool, bool]:
    d = dict(fmap.get(name) or {})
    if isinstance(fmap.get(name), bool): return bool(fmap[name]), False
    return bool(d.get("enable", False)), bool(d.get("remove", False))

def _item_sig_key(v: dict) -> str:
    rt=_rt(); canonical_key = rt[10]
    try: return canonical_key(v)
    except Exception:
        ids = (v.get("ids") or {})
        for k in ("tmdb", "imdb", "tvdb", "slug"):
            val = ids.get(k)
            if val: return f"{k}:{val}".lower()
        t = (str(v.get("title") or v.get("name") or "")).strip().lower()
        y = str(v.get("year") or v.get("release_year") or "")
        typ = (v.get("type") or "").lower()
        return f"{typ}|title:{t}|year:{y}"

def _persist_state_via_orc(orc, *, feature: str = "watchlist") -> dict:
    # Save minimal state from orchestrator snapshots
    rt=_rt(); minimal = rt[9]
    snaps = orc.build_snapshots(feature=feature)
    providers: Dict[str, Any] = {}; wall: List[dict] = []; seen = set()
    for prov, idx in (snaps or {}).items():
        items_min = {k: minimal(v) for k, v in (idx or {}).items()}
        providers[prov] = {feature: {"baseline": {"items": items_min}, "checkpoint": None}}
        for item in items_min.values():
            key = _item_sig_key(item)
            if key in seen: continue
            seen.add(key); wall.append(minimal(item))
    state = {"providers": providers, "wall": wall, "last_sync_epoch": int(time.time())}
    orc.files.save_state(state)
    return state

def _run_pairs_thread(run_id: str, overrides: dict | None = None) -> None:
    rt=_rt(); LOG_BUFFERS, RUNNING_PROCS, STATE_PATH, _append_log = rt[0], rt[1], rt[3], rt[8]
    overrides = overrides or {}; _summary_reset()
    LOG_BUFFERS["SYNC"] = []; _sync_progress_ui("::CLEAR::")
    _sync_progress_ui(f"SYNC start: orchestrator pairs run_id={run_id}")
    try:
        orch_mod = importlib.import_module("cw_platform.orchestrator")
        try: orch_mod = importlib.reload(orch_mod)
        except Exception: pass
        OrchestratorClass = getattr(orch_mod, "Orchestrator")
        _sync_progress_ui(f"[i] Orchestrator module: {getattr(orch_mod, '__file__', '?')}")
        load_config, _save = _env()
        cfg = load_config(); mgr = OrchestratorClass(config=cfg)
        dry = bool(((cfg.get("sync") or {}).get("dry_run") or False)) or bool((overrides or {}).get("dry_run"))
        result = mgr.run_pairs(
            dry_run=dry,
            progress=_sync_progress_ui,
            write_state_json=True,
            state_path=STATE_PATH,
            use_snapshot=True
        )
        added = int(result.get("added", 0)); removed = int(result.get("removed", 0))
        try:
            state = _load_state()
            if state:
                _STATS = _rt()[5]
                _STATS.refresh_from_state(state)
                _STATS.record_summary(added, removed)
                try:
                    counts = _counts_from_state(state)
                    if counts is None:
                        _append_log("SYNC", "[!] Provider-counts: state malformed; falling back to live snapshots")
                        counts = _counts_from_orchestrator(cfg)
                    if counts:
                        _PROVIDER_COUNTS_CACHE["ts"] = time.time()
                        _PROVIDER_COUNTS_CACHE["data"] = counts
                except Exception as e:
                    _append_log("SYNC", f"[!] Provider-counts cache warm failed: {e}")
            else:
                _append_log("SYNC", "[!] No state found after sync; stats not updated.")
        except Exception as e:
            _append_log("SYNC", f"[!] Stats update failed: {e}")
        _sync_progress_ui(f"[i] Done. Total added: {added}, Total removed: {removed}")
        _sync_progress_ui("[SYNC] exit code: 0")
    except Exception as e:
        _sync_progress_ui(f"[!] Sync error: {e}")
        _sync_progress_ui("[SYNC] exit code: 1")
    finally:
        # Best-effort cache warm on exit (defensive)
        try:
            load_config, _ = _env(); cfg2 = load_config()
            state2 = _load_state()
            counts2 = _counts_from_state(state2) if state2 else None
            if counts2 is None:
                counts2 = _counts_from_orchestrator(cfg2)
            if counts2:
                _PROVIDER_COUNTS_CACHE["ts"] = time.time()
                _PROVIDER_COUNTS_CACHE["data"] = counts2
        except Exception:
            pass
        RUNNING_PROCS.pop("SYNC", None)

# ----- Stats → lanes -----
def _parse_epoch(v: Any) -> int:
    if v is None: return 0
    try:
        if isinstance(v, (int, float)): return int(v)
        s = str(v).strip()
        if s.isdigit(): return int(s)
        s = s.replace("Z","+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception: return 0

def _lanes_defaults() -> Dict[str, Dict[str, Any]]:
    def lane(): return {"added": 0, "removed": 0, "updated": 0, "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []}
    return {"watchlist": lane(), "ratings": lane(), "history": lane(), "playlists": lane()}

def _lanes_enabled_defaults() -> Dict[str, bool]:
    return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

def _compute_lanes_from_stats(since_epoch: int, until_epoch: int):
    _STATS = _rt()[5]
    feats = _lanes_defaults(); enabled = _lanes_enabled_defaults()
    with _STATS.lock:
        events = list(_STATS.data.get("events") or [])
    if not events:
        return feats, enabled

    s = int(since_epoch or 0)
    u = int(until_epoch or 0) or int(time.time())

    def _evt_epoch(e: dict) -> int:
        for k in ("sync_ts", "ingested_ts", "seen_ts", "ts"):
            try:
                v = int(e.get(k) or 0)
                if v:
                    return v
            except Exception:
                pass
        return 0

    # --- IMPORTANT: ignore synthetic aggregate rows and events without a title
    def _is_real_item_event(e: dict) -> bool:
        k = str(e.get("key") or "")
        if k.startswith("agg:"):         # synthetic “fan out” events
            return False
        t = (e.get("title") or "").strip()
        # Keep history/rating events even if some providers omit titles, but
        # for watchlist we require a title to show in spotlight.
        if (e.get("feature") or e.get("feat")) in ("watchlist", "ratings", "history"):
            return bool(t)
        return True

    rows = [e for e in events if s <= _evt_epoch(e) <= u and _is_real_item_event(e)]
    if not rows:
        return feats, enabled

    rows.sort(key=_evt_epoch)
    anyin = lambda s, toks: any(t in s for t in toks)

    for e in rows:
        action = str(e.get("action") or e.get("op") or e.get("change") or "").lower().replace(":", "_").replace("-", "_")
        feat   = str(e.get("feature") or e.get("feat") or "").lower().replace(":", "_").replace("-", "_")
        title  = (e.get("title") or e.get("key") or "item")
        slim   = {k: e.get(k) for k in ("title", "key", "type", "source", "ts") if k in e}

        if ("watchlist" in action) or (feat == "watchlist"):
            lane = "watchlist"
            if anyin(action, ("remove", "unwatchlist", "delete", "del", "rm", "clear")):
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(title)
            else:
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(title)
            continue

        if (action in ("rate", "rating", "update_rating", "unrate")) or ("rating" in action) or ("rating" in feat):
            lane = "ratings"
            feats[lane]["updated"] += 1
            feats[lane]["spotlight_update"].append(title)
            continue

        is_history_feat = (feat in ("history", "watch", "watched")) or ("history" in action)
        if "watchlist" not in action:
            is_add_like    = anyin(action, ("watch", "scrobble", "checkin", "mark_watched", "history_add", "add_history"))
            is_remove_like = anyin(action, ("unwatch", "remove_history", "history_remove", "delete_watch", "del_history"))
        else:
            is_add_like = is_remove_like = False
        if is_history_feat or is_add_like or is_remove_like:
            lane = "history"
            if is_remove_like:
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(title)
            elif is_add_like:
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(title)
            else:
                feats[lane]["updated"] += 1
                feats[lane]["spotlight_update"].append(title)
            continue

        if ("playlist" in action) or ("playlist" in feat):
            lane = "playlists"
            if anyin(action, ("remove", "delete", "rm", "del")):
                feats[lane]["removed"] += 1
                feats[lane]["spotlight_remove"].append(title)
            elif anyin(action, ("update", "rename", "move", "reorder")):
                feats[lane]["updated"] += 1
                feats[lane]["spotlight_update"].append(title)
            else:
                feats[lane]["added"] += 1
                feats[lane]["spotlight_add"].append(title)

    # only keep the last 3 spotlight entries per lane
    for lane in feats.values():
        lane["spotlight_add"]    = (lane["spotlight_add"]    or [])[-3:]
        lane["spotlight_remove"] = (lane["spotlight_remove"] or [])[-3:]
        lane["spotlight_update"] = (lane["spotlight_update"] or [])[-3:]

    return feats, enabled

def _parse_sync_line(line: str) -> None:
    s = _rt()[7](line).strip()
    try:
        o = json.loads(s)
        if isinstance(o, dict) and o.get("event"):
            ev = str(o.get("event") or ""); feat = str(o.get("feature") or "").lower()
            if feat in ("watchlist","history","ratings","playlists"):
                F = SUMMARY.setdefault("features", {})
                if feat not in F: F[feat] = {"added":0,"removed":0,"updated":0,"spotlight_add":[],"spotlight_remove":[],"spotlight_update":[]}
                getc = lambda obj: int(((obj.get("result") or {}).get("count") if isinstance(obj.get("result"), dict) else None) or obj.get("count") or 0)
                if ev == "apply:add:done": F[feat]["added"] += getc(o); _summary_set("features", F); return
                if ev == "apply:remove:done": F[feat]["removed"] += getc(o); _summary_set("features", F); return
                if ev == "apply:update:done": F[feat]["updated"] += getc(o); _summary_set("features", F); return
                if ev == "debug" and str(o.get("msg") or "") == "apply:add:corrected":
                    eff = int(o.get("effective") or 0)
                    if eff > int(F[feat].get("added") or 0): F[feat]["added"] = eff
                    _summary_set("features", F); return
    except Exception: pass

    m = re.match(r"^(?:>\s*)?SYNC start:\s+(?P<cmd>.+)$", s)
    if m:
        if not SUMMARY.get("running"):
            _summary_set("running", True); SUMMARY["raw_started_ts"] = time.time()
            _summary_set("started_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        cmd_str = m.group("cmd"); short_cmd = cmd_str
        try:
            parts = shlex.split(cmd_str)
            script = next((os.path.basename(p) for p in reversed(parts) if p.endswith(".py")), None)
            if script: short_cmd = script
            elif parts: short_cmd = os.path.basename(parts[0])
        except Exception: pass
        _summary_set("cmd", short_cmd); _summary_set_timeline("start", True); return

    m = re.search(r"Version\s+(?P<ver>[0-9][0-9A-Za-z\.\-\+_]*)", s)
    if m: _summary_set("version", m.group("ver")); return

    m = re.search(r"Pre-sync counts:\s*(?P<pairs>.+)$", s, re.IGNORECASE)
    if m:
        pairs = re.findall(r"\b([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\d+)", m.group("pairs"))
        for name, val in pairs:
            key = name.lower()
            try: val_i = int(val)
            except Exception: continue
            if key in ("plex","simkl","trakt","jellyfin"): _summary_set(f"{key}_pre", val_i)
        _summary_set_timeline("pre", True); return

    m = re.search(r"Post-sync:\s*(?P<rest>.+)$", s, re.IGNORECASE)
    if m:
        rest = m.group("rest")
        pairs = re.findall(r"\b([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\d+)", rest)
        for name, val in pairs:
            key = name.lower()
            try: val_i = int(val)
            except Exception: continue
            if key in ("plex","simkl","trakt","jellyfin"): _summary_set(f"{key}_post", val_i)
        mres = re.search(r"(?:→|->|=>)\s*([A-Za-z]+)", rest)
        if mres: _summary_set("result", mres.group(1).upper())
        _summary_set_timeline("post", True); return

    m = re.search(r"\[SYNC\]\s+exit code:\s+(?P<code>\d+)", s)
    if m:
        code = int(m.group("code")); _summary_set("exit_code", code)
        started = SUMMARY.get("raw_started_ts")
        if started: dur = max(0.0, time.time() - float(started)); _summary_set("duration_sec", round(dur, 2))
        _summary_set("finished_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        _summary_set("running", False); _summary_set_timeline("done", True)
        try:
            tl = SUMMARY.get("timeline") or {}
            if tl.get("done"):
                if not tl.get("pre"): _summary_set_timeline("pre", True)
                if not tl.get("post"): _summary_set_timeline("post", True)
        except Exception: pass
        try:
            snap = _summary_snapshot()
            since = _parse_epoch(snap.get("raw_started_ts") or snap.get("started_at"))
            until = _parse_epoch(snap.get("finished_at")) or int(time.time())
            feats_tmp, enabled_tmp = _compute_lanes_from_stats(since, until)

            snap.setdefault("features", {})
            for k, v in (feats_tmp or {}).items():
                dst = snap["features"].setdefault(k, {"added":0,"removed":0,"updated":0,
                                                      "spotlight_add":[],"spotlight_remove":[],"spotlight_update":[]})
                va = int((v or {}).get("added") or 0); vr = int((v or {}).get("removed") or 0); vu = int((v or {}).get("updated") or 0)
                dst["added"]   = max(int(dst.get("added") or 0), va)
                dst["removed"] = max(int(dst.get("removed") or 0), vr)
                dst["updated"] = max(int(dst.get("updated") or 0), vu)
                if not dst["spotlight_add"]:    dst["spotlight_add"]    = list((v or {}).get("spotlight_add")    or [])[-3:]
                if not dst["spotlight_remove"]: dst["spotlight_remove"] = list((v or {}).get("spotlight_remove") or [])[-3:]
                if not dst["spotlight_update"]: dst["spotlight_update"] = list((v or {}).get("spotlight_update") or [])[-3:]

            lanes = snap.get("features") or {}

            try:
                _STATS = _rt()[5]
                with _STATS.lock: evs = list(_STATS.data.get("events") or [])
                def _evt_ts(e): 
                    for k in ("ts","seen_ts","sync_ts","ingested_ts"):
                        try:
                            v = int(e.get(k) or 0)
                            if v: return v
                        except: pass
                    return 0
                for feat in ("ratings","history"):
                    lane = lanes.get(feat) or {}
                    if ((lane.get("added",0)+lane.get("removed",0)+lane.get("updated",0))>0 and
                        not (lane.get("spotlight_add") or lane.get("spotlight_remove") or lane.get("spotlight_update"))):
                        rows = [e for e in evs
                                if str(e.get("feature") or e.get("feat") or "").lower()==feat
                                and not str(e.get("key") or "").startswith("agg:")
                                and since <= _evt_ts(e) <= until]
                        rows.sort(key=_evt_ts)
                        for e in rows[-3:]:
                            act = str(e.get("action") or e.get("op") or e.get("change") or "").lower()
                            slim = {k: e.get(k) for k in ("title","key","type","source","ts") if k in e}
                            if any(t in act for t in ("remove","unrate","delete","clear")):
                                lane.setdefault("spotlight_remove",[]).append(slim)
                            elif ("update" in act) or ("rate" in act):
                                lane.setdefault("spotlight_update",[]).append(slim)
                            else:
                                lane.setdefault("spotlight_add",[]).append(slim)
                        lanes[feat] = lane
            except Exception:
                pass

            _summary_set("enabled", enabled_tmp); _summary_set("features", lanes)

            a = r = u = 0
            for k, d in (lanes or {}).items():
                if isinstance(enabled_tmp, dict) and enabled_tmp.get(k) is False: continue
                a += int((d or {}).get("added") or 0); r += int((d or {}).get("removed") or 0); u += int((d or {}).get("updated") or 0)
            _summary_set("added_last", a); _summary_set("removed_last", r); _summary_set("updated_last", u)

            _STATS = _rt()[5]
            run_id = snap.get("finished_at") or snap.get("started_at") or ""
            for name, lane in (lanes or {}).items():
                aa = int((lane or {}).get("added") or 0); rr = int((lane or {}).get("removed") or 0); uu = int((lane or {}).get("updated") or 0)
                if aa or rr or uu:
                    _STATS.record_feature_totals(name, added=aa, removed=rr, updated=uu, src="REPORT", run_id=run_id, expand_events=True)

        except Exception: pass
        try:
            REPORT_DIR = _rt()[6]
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            path = REPORT_DIR / f"sync-{ts}.json"
            with path.open("w", encoding="utf-8") as f: json.dump(_summary_snapshot(), f, indent=2)
        except Exception: pass

# ----- State file helpers -----
def _find_state_path() -> Optional[Path]:
    for p in _rt()[4]:
        if p.exists(): return p
    return None

def _load_state() -> Dict[str, Any]:
    sp = _find_state_path()
    if not sp: return {}
    try: return json.loads(sp.read_text(encoding="utf-8"))
    except Exception: return {}

# ----- Ratings spotlight glue -----
_R_ACTION_MAP = {"add": "add","rate": "add","remove": "remove","unrate": "remove","update": "update","update_rating": "update"}

def _lane_is_empty(v: dict | None) -> bool:
    if not isinstance(v, dict): return True
    has_counts = (v.get("added") or 0) + (v.get("removed") or 0) + (v.get("updated") or 0) > 0
    has_spots  = any(v.get(k) for k in ("spotlight_add","spotlight_remove","spotlight_update"))
    return not (has_counts or has_spots)

# ----- Legacy spotlight helpers (compat) -----
def _lane_init():
    return {"added": 0, "removed": 0, "updated": 0,
            "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []}

def _ensure_feature(summary_obj: dict, feature: str) -> dict:
    feats = summary_obj.setdefault("features", {})
    lane = feats.setdefault(feature, _lane_init())
    lane.setdefault("added", 0); lane.setdefault("removed", 0); lane.setdefault("updated", 0)
    lane.setdefault("spotlight_add", []); lane.setdefault("spotlight_remove", []); lane.setdefault("spotlight_update", [])
    return lane

def _push_spotlight(lane: dict, kind: str, items: list, max3: bool = True):
    key = {"add": "spotlight_add", "remove": "spotlight_remove", "update": "spotlight_update"}.get(kind, "spotlight_add")
    dst = lane.setdefault(key, [])
    seen = set(dst)
    for it in (items or []):
        t = (it.get("title") or it.get("name") or it.get("key") or str(it))[:200]
        if t and t not in seen:
            dst.append(t); seen.add(t)
            if max3 and len(dst) >= 3:
                break

def _push_spot_titles(dst: list, items: list, max3: bool = True):
    seen = set(dst)
    for it in (items or []):
        t = (it.get("title") or it.get("name") or it.get("key") or str(it))[:200]
        if t and t not in seen:
            dst.append(t); seen.add(t)
            if max3 and len(dst) >= 3:
                break

# ----- Sync run state -----
def _is_sync_running() -> bool:
    RUNNING_PROCS = _rt()[1]
    t = RUNNING_PROCS.get("SYNC")
    return bool(t and t.is_alive())

# ----- Providers discovery -----
@router.get("/sync/providers")
def api_sync_providers() -> JSONResponse:
    HIDDEN = {"BASE"}; PKG_CANDIDATES = ("providers.sync",); FEATURE_KEYS = ("watchlist", "ratings", "history", "playlists")

    def _asdict_dc(obj):
        try:
            if _dc.is_dataclass(obj): return _dc.asdict(obj if not isinstance(obj, type) else obj())
        except Exception: return None

    def _norm_features(f: dict | None) -> dict:
        f = dict(f or {})
        return {k: bool((f.get(k) or {}).get("enable", (f.get(k) or {}).get("enabled", False)) if isinstance(f.get(k), dict) else f.get(k)) for k in FEATURE_KEYS}

    def _norm_caps(caps: dict | None) -> dict:
        caps = dict(caps or {}); return {"bidirectional": bool(caps.get("bidirectional", False))}

    def _manifest_from_module(mod) -> dict | None:
        if hasattr(mod, "get_manifest") and callable(mod.get_manifest):
            try: mf = dict(mod.get_manifest())
            except Exception: mf = None
            if mf and not (mf.get("hidden") or mf.get("is_template")):
                return {
                    "name": (mf.get("name") or "").upper(), "label": mf.get("label") or (mf.get("name") or "").title(),
                    "features": _norm_features(mf.get("features")), "capabilities": _norm_caps(mf.get("capabilities")),
                    "version": mf.get("version"), "vendor": mf.get("vendor"), "description": mf.get("description"),
                }
        cand = [cls for _, cls in inspect.getmembers(mod, inspect.isclass) if cls.__module__ == mod.__name__ and cls.__name__.endswith("Module")]
        if cand:
            cls = cand[0]; info = getattr(cls, "info", None)
            if info is not None:
                caps = _asdict_dc(getattr(info, "capabilities", None)) or {}
                name = (getattr(info, "name", None) or getattr(cls, "__name__", "").replace("Module", "")).upper()
                label = (getattr(info, "name", None) or name).title()
                if bool(getattr(info, "hidden", False) or getattr(info, "is_template", False)): return None
                try: feats = dict(cls.supported_features()) if hasattr(cls, "supported_features") else {}
                except Exception: feats = {}
                return {
                    "name": name, "label": label, "features": _norm_features(feats), "capabilities": _norm_caps(caps),
                    "version": getattr(info, "version", None), "vendor": getattr(info, "vendor", None), "description": getattr(info, "description", None),
                }
        ops = getattr(mod, "OPS", None)
        if ops is not None:
            try:
                name = str(ops.name()).upper(); label = str(getattr(ops, "label")() if hasattr(ops, "label") else name.title())
                feats = dict(ops.features()) if hasattr(ops, "features") else {}; caps = dict(ops.capabilities()) if hasattr(ops, "capabilities") else {}
                return {"name": name, "label": label, "features": _norm_features(feats), "capabilities": _norm_caps(caps), "version": None, "vendor": None, "description": None}
            except Exception: return None
        return None

    items, seen = [], set()
    for pkg_name in PKG_CANDIDATES:
        try: pkg = importlib.import_module(pkg_name)
        except Exception: continue
        for pkg_path in getattr(pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(pkg_path)]):
                if not m.name.startswith("_mod_"): continue
                prov_key = m.name.replace("_mod_", "").upper()
                if prov_key in HIDDEN: continue
                try: mod = importlib.import_module(f"{pkg_name}.{m.name}")
                except Exception: continue
                mf = _manifest_from_module(mod)
                if not mf: continue
                mf["name"] = (mf["name"] or prov_key).upper(); mf["label"] = mf.get("label") or mf["name"].title()
                mf["features"] = _norm_features(mf.get("features")); mf["capabilities"] = _norm_caps(mf.get("capabilities"))
                if mf["name"] in seen: continue
                seen.add(mf["name"]); items.append(mf)
    items.sort(key=lambda x: (x.get("label") or x.get("name") or "").lower())
    return JSONResponse(items)

# ----- Pairs data models -----
class PairIn(BaseModel):
    source: str
    target: str
    mode: Optional[str] = None
    enabled: Optional[bool] = None
    features: Optional[Dict[str, Any]] = None

class PairPatch(BaseModel):
    source: Optional[str] = None
    target: Optional[str] = None
    mode: Optional[str] = None
    enabled: Optional[bool] = None
    features: Optional[Dict[str, Any]] = None

# ----- Pairs CRUD -----
@router.get("/pairs")
def api_pairs_list() -> JSONResponse:
    load_config, save_config = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        dirty = False
        for it in arr:
            newf = _normalize_features(it.get("features"))
            if newf != (it.get("features") or {}):
                it["features"] = newf
                dirty = True
        if dirty:
            save_config(cfg)
        return JSONResponse(arr)
    except Exception as e:
        try: _rt()[8]("TRBL", f"/api/pairs GET failed: {e}")
        except Exception: pass
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.post("/pairs")
def api_pairs_add(payload: PairIn = Body(...)) -> Dict[str, Any]:
    load_config, save_config = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)

        item = payload.model_dump()
        item.setdefault("mode", "one-way")
        item["enabled"] = bool(item.get("enabled", False))
        item["features"] = _normalize_features(item.get("features") or {"watchlist": True})
        item["id"] = _gen_id("pair")

        arr.append(item)
        save_config(cfg)
        return {"ok": True, "id": item["id"]}
    except Exception as e:
        try: _rt()[8]("TRBL", f"/api/pairs POST failed: {e}")
        except Exception: pass
        return {"ok": False, "error": str(e)}

@router.post("/pairs/reorder")
def api_pairs_reorder(order: List[str] = Body(...)) -> dict:
    load_config, save_config = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)

        index_map = {str(p.get("id")): i for i, p in enumerate(arr)}
        seen, wanted_ids = set(), []
        for pid in (order or []):
            spid = str(pid)
            if spid in index_map and spid not in seen:
                wanted_ids.append(spid); seen.add(spid)

        id_set = set(wanted_ids)
        head = [next(p for p in arr if str(p.get("id")) == pid) for pid in wanted_ids]
        tail = [p for p in arr if str(p.get("id")) not in id_set]
        new_arr = head + tail

        prev_ids = [str(p.get("id")) for p in arr]
        final_ids = [str(p.get("id")) for p in new_arr]
        changed = prev_ids != final_ids
        if changed:
            cfg["pairs"] = new_arr
            save_config(cfg)

        unknown_ids = [str(pid) for pid in (order or []) if str(pid) not in index_map]
        return {"ok": True, "reordered": changed, "count": len(new_arr), "unknown_ids": unknown_ids, "final_order": final_ids}
    except Exception as e:
        try: _rt()[8]("TRBL", f"/api/pairs/reorder failed: {e}")
        except Exception: pass
        return {"ok": False, "error": str(e)}

@router.put("/pairs/{pair_id}")
def api_pairs_update(pair_id: str, payload: PairPatch = Body(...)) -> Dict[str, Any]:
    load_config, save_config = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        upd = payload.model_dump(exclude_unset=True, exclude_none=True)

        for it in arr:
            if str(it.get("id")) == str(pair_id):
                if "features" in upd:
                    it["features"] = _normalize_features(upd.pop("features"))
                for k, v in upd.items():
                    it[k] = v
                save_config(cfg)
                return {"ok": True}
        return {"ok": False, "error": "not_found"}
    except Exception as e:
        try: _rt()[8]("TRBL", f"/api/pairs PUT failed: {e}")
        except Exception: pass
        return {"ok": False, "error": str(e)}

@router.delete("/pairs/{pair_id}")
def api_pairs_delete(pair_id: str) -> Dict[str, Any]:
    load_config, save_config = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)
        before = len(arr)
        arr[:] = [it for it in arr if str(it.get("id")) != str(pair_id)]
        if len(arr) != before:
            save_config(cfg)
        return {"ok": True, "deleted": before - len(arr)}
    except Exception as e:
        try: _rt()[8]("TRBL", f"/api/pairs DELETE failed: {e}")
        except Exception: pass
        return {"ok": False, "error": str(e)}

# ----- Provider counts (fast; state + tiny TTL cache) -----
_PROVIDER_COUNTS_CACHE = {"ts": 0.0, "data": None}
_PROVIDER_ORDER = ("PLEX", "SIMKL", "TRAKT", "JELLYFIN")

def _counts_from_state(state: dict | None) -> dict | None:
    """Defensive reader: tolerate shape drift; log offenders; return None only if state unusable."""
    if not isinstance(state, dict):
        return None
    provs = state.get("providers")
    if not isinstance(provs, dict) or not provs:
        return None

    out = {k: 0 for k in _PROVIDER_ORDER}
    _append_log = _rt()[8]

    for name, pdata in provs.items():
        key = str(name or "").upper()
        if key not in out:
            continue

        if not isinstance(pdata, dict):
            _append_log("SYNC", f"[!] counts: provider '{key}' node is {type(pdata).__name__} (expected dict); skipping")
            continue

        wl = pdata.get("watchlist")
        count = 0

        if isinstance(wl, dict):
            # preferred: checkpoint.items → baseline.items → items (dict/list/int/str)
            chk = wl.get("checkpoint")
            if isinstance(chk, dict) and isinstance(chk.get("items"), dict):
                count = len(chk["items"])
            else:
                base = wl.get("baseline")
                if isinstance(base, dict) and isinstance(base.get("items"), dict):
                    count = len(base["items"])
                else:
                    items_node = wl.get("items")
                    if isinstance(items_node, dict):
                        count = len(items_node)
                    elif isinstance(items_node, list):
                        count = len(items_node)
                    elif isinstance(items_node, (int, str)):
                        try: count = int(items_node)
                        except Exception:
                            _append_log("SYNC", f"[!] counts: provider '{key}' watchlist.items is non-numeric {type(items_node).__name__}; using 0")
                            count = 0
        elif isinstance(wl, list):
            count = len(wl)
        elif isinstance(wl, (int, str)):
            try: count = int(wl)
            except Exception:
                _append_log("SYNC", f"[!] counts: provider '{key}' watchlist is non-numeric {type(wl).__name__}; using 0")
                count = 0
        elif wl is not None:
            _append_log("SYNC", f"[!] counts: provider '{key}' watchlist unexpected type {type(wl).__name__}; using 0")

        out[key] = count

    return out

def _counts_from_orchestrator(cfg: dict) -> dict:
    # One snapshot per provider, then count
    from cw_platform.orchestrator import Orchestrator
    snaps = Orchestrator(config=cfg).build_snapshots(feature="watchlist")
    out = {k: 0 for k in _PROVIDER_ORDER}
    if isinstance(snaps, dict):
        for name in _PROVIDER_ORDER:
            out[name] = len((snaps.get(name) or {}) if isinstance(snaps.get(name), dict) else {})
    return out

def _provider_counts_fast(cfg: dict, *, max_age: int = 30, force: bool = False) -> dict:
    now = time.time()
    if not force and _PROVIDER_COUNTS_CACHE["data"] and (now - _PROVIDER_COUNTS_CACHE["ts"] < max(0, int(max_age))):
        return dict(_PROVIDER_COUNTS_CACHE["data"])
    counts = _counts_from_state(_load_state())
    if counts is None:
        counts = _counts_from_orchestrator(cfg)
    _PROVIDER_COUNTS_CACHE["ts"] = now
    _PROVIDER_COUNTS_CACHE["data"] = counts
    return counts

@router.get("/sync/providers/counts", tags=["Synchronization"])
def api_provider_counts(max_age: int = 30,
                        force: bool = False,
                        source: str = "auto") -> dict:
    """
    Fast counts.
    - source=state → only read the last sync state (no network)
    - source=auto  → state, else orchestrator
    - force=true   → ignore TTL cache
    - max_age=sec  → TTL for the in-process cache
    """
    load_config, _ = _env()
    cfg = load_config()

    src = (source or "auto").lower().strip()
    if src == "state":
        counts = _counts_from_state(_load_state()) or {k: 0 for k in _PROVIDER_ORDER}
        return counts

    return _provider_counts_fast(cfg, max_age=max_age, force=bool(force))

# ----- Run orchestration -----
@router.post("/run")
def api_run_sync(payload: dict | None = Body(None)) -> Dict[str, Any]:
    rt=_rt(); LOG_BUFFERS, RUNNING_PROCS, SYNC_PROC_LOCK = rt[0], rt[1], rt[2]
    with SYNC_PROC_LOCK:
        if _is_sync_running(): return {"ok": False, "error": "Sync already running"}
        cfg = _env()[0]()  # load_config
        pairs = list((cfg or {}).get("pairs") or [])
        if not any(p.get("enabled", True) for p in pairs):
            _rt()[8]("SYNC", "[i] No pairs configured — skipping sync.")
            return {"ok": True, "skipped": "no_pairs_configured"}
        run_id = str(int(time.time()))
        th = threading.Thread(target=_run_pairs_thread, args=(run_id,), kwargs={"overrides": (payload or {})}, daemon=True)
        th.start(); RUNNING_PROCS["SYNC"] = th; _rt()[8]("SYNC", f"[i] Triggered sync run {run_id}")
        return {"ok": True, "run_id": run_id}

@router.get("/run/summary")
def api_run_summary() -> JSONResponse:
    snap = _summary_snapshot()
    since = _parse_epoch(snap.get("raw_started_ts") or snap.get("started_at"))
    until = _parse_epoch(snap.get("finished_at"))
    if not until and snap.get("running"): until = int(time.time())

    stats_feats, enabled = _compute_lanes_from_stats(since, until)

    snap.setdefault("features", {})
    feats = snap["features"]

    for k, v in (stats_feats or {}).items():
        dst = feats.setdefault(k, {"added":0,"removed":0,"updated":0,"spotlight_add":[],"spotlight_remove":[],"spotlight_update":[]})
        va = int((v or {}).get("added") or 0); vr = int((v or {}).get("removed") or 0); vu = int((v or {}).get("updated") or 0)
        dst["added"]   = max(int(dst.get("added") or 0), va)
        dst["removed"] = max(int(dst.get("removed") or 0), vr)
        dst["updated"] = max(int(dst.get("updated") or 0), vu)
        if not dst["spotlight_add"]:    dst["spotlight_add"]    = list((v or {}).get("spotlight_add")    or [])[-3:]
        if not dst["spotlight_remove"]: dst["spotlight_remove"] = list((v or {}).get("spotlight_remove") or [])[-3:]
        if not dst["spotlight_update"]: dst["spotlight_update"] = list((v or {}).get("spotlight_update") or [])[-3:]

    try:
        _STATS = _rt()[5]
        with _STATS.lock: evs = list(_STATS.data.get("events") or [])
        def _evt_ts(e):
            for k in ("ts","seen_ts","sync_ts","ingested_ts"):
                try:
                    v = int(e.get(k) or 0)
                    if v: return v
                except: pass
            return 0
        for feat in ("ratings","history"):
            lane = feats.get(feat) or {}
            if ((lane.get("added",0)+lane.get("removed",0)+lane.get("updated",0))>0 and
                not (lane.get("spotlight_add") or lane.get("spotlight_remove") or lane.get("spotlight_update"))):
                rows = [e for e in evs
                        if str(e.get("feature") or e.get("feat") or "").lower()==feat
                        and not str(e.get("key") or "").startswith("agg:")
                        and (since <= _evt_ts(e) <= (until or _evt_ts(e)))]
                rows.sort(key=_evt_ts)
                for e in rows[-3:]:
                    act = str(e.get("action") or e.get("op") or e.get("change") or "").lower()
                    slim = {k: e.get(k) for k in ("title","key","type","source","ts") if k in e}
                    if any(t in act for t in ("remove","unrate","delete","clear")):
                        lane.setdefault("spotlight_remove",[]).append(slim)
                    elif ("update" in act) or ("rate" in act):
                        lane.setdefault("spotlight_update",[]).append(slim)
                    else:
                        lane.setdefault("spotlight_add",[]).append(slim)
                feats[feat] = lane
    except Exception:
        pass

    snap["features"] = feats
    snap.setdefault("enabled", enabled or _lanes_enabled_defaults())
    tl = snap.get("timeline") or {}
    if tl.get("done") and not tl.get("post"): tl["post"] = True; tl["pre"] = True; snap["timeline"] = tl
    return JSONResponse(snap)

@router.get("/run/summary/file")
def api_run_summary_file() -> Response:
    js = json.dumps(_summary_snapshot(), indent=2)
    return Response(content=js, media_type="application/json", headers={"Content-Disposition": 'attachment; filename="last_sync.json"'})

@router.get("/run/summary/stream")
def api_run_summary_stream() -> StreamingResponse:
    import html, re
    TAG_RE = re.compile(r"<[^>]+>")

    def dehtml(s: str) -> str:

        return html.unescape(TAG_RE.sub("", s or ""))

    def gen():
        last_key = None
        last_idx = 0
        LOG_BUFFERS = _rt()[0]

        while True:
            time.sleep(0.25)
            try:
                buf = LOG_BUFFERS.get("SYNC") or []
                if last_idx > len(buf): 
                    last_idx = 0
                if last_idx < len(buf):
                    for line in buf[last_idx:]:
                        raw = dehtml(line).strip()
                        if not raw.startswith("{"):
                            continue
                        try:
                            obj = json.loads(raw)
                        except Exception:
                            continue

                        evt = (str(obj.get("event") or "log").strip() or "log")
                        yield f"event: {evt}\n"
                        yield f"data: {json.dumps(obj, separators=(',',':'))}\n\n"

                        if evt.startswith("apply:"):
                            done = (obj.get("done")
                                    or (obj.get("result") or {}).get("count")
                                    or obj.get("count") or 0)
                            total = obj.get("total") or obj.get("count") or 0
                            final = evt.endswith(":done")
                            payload = {"done": int(done or 0), "total": int(total or 0), "final": bool(final)}
                            yield "event: progress:apply\n"
                            yield f"data: {json.dumps(payload, separators=(',',':'))}\n\n"

                    last_idx = len(buf)
            except Exception:
                pass

            snap = _summary_snapshot()
            key = (
                snap.get("running"),
                snap.get("exit_code"),
                snap.get("plex_post"),
                snap.get("simkl_post"),
                snap.get("result"),
                snap.get("duration_sec"),
                (snap.get("timeline", {}) or {}).get("done"),
                json.dumps(snap.get("features", {}), sort_keys=True),
                json.dumps(snap.get("enabled", {}), sort_keys=True),
            )
            if key != last_key:
                last_key = key
                yield f"data: {json.dumps(snap, separators=(',',':'))}\n\n"

    return StreamingResponse(gen(), media_type="text/event-stream")
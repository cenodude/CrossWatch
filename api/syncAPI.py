# _syncAPI.py
# CrossWatch - Synchronization API for multiple services
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any, cast
from pathlib import Path
from datetime import datetime, timezone, date

import dataclasses as _dc, importlib, inspect, json, os, pkgutil, re, shlex, threading, time, uuid
import asyncio

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import BaseModel

__all__ = ["router", "_is_sync_running", "_load_state", "_find_state_path", "_persist_state_via_orc"]

router = APIRouter(prefix="/api", tags=["synchronization"])

def _env():
    from cw_platform.config_base import load_config, save_config
    return load_config, save_config

def _rt():
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


FEATURE_KEYS = ["watchlist", "ratings", "history", "playlists"]
_ALLOWED_RATING_TYPES: tuple[str, ...] = ("movies", "shows", "seasons", "episodes")
_ALLOWED_RATING_MODES: tuple[str, ...] = ("only_new", "from_date", "all")

def _normalize_ratings_block(v: dict | bool | None) -> dict:
    if isinstance(v, bool):
        return {
            "enable": bool(v), "add": bool(v), "remove": False,
            "types": ["movies", "shows"], "mode": "only_new", "from_date": "",
        }

    d = dict(v or {})
    d["enable"] = bool(d.get("enable", d.get("enabled", False)))
    d["add"] = bool(d.get("add", True))
    d["remove"] = bool(d.get("remove", False))

    t = d.get("types", [])
    if isinstance(t, str):
        t = [t]
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

def _ensure_pair_ratings_defaults(cfg: dict[str, Any]) -> None:
    for p in (cfg.get("pairs") or []):
        feats = p.setdefault("features", {})
        feats["ratings"] = _normalize_ratings_block(feats.get("ratings"))

def _normalize_features(f: dict | None) -> dict:
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

def _cfg_pairs(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    arr = cfg.get("pairs")
    if not isinstance(arr, list):
        arr = []
        cfg["pairs"] = arr
    return arr

def _gen_id(prefix: str = "pair") -> str:
    return f"{prefix}_{uuid.uuid4().hex[:12]}"

# Orchestrator state loading
SUMMARY_LOCK = threading.Lock()
SUMMARY: dict[str, Any] = {}

def _summary_reset() -> None:
    with SUMMARY_LOCK:
        SUMMARY.clear()
        SUMMARY.update(
            {
                "running": False,
                "started_at": None,
                "finished_at": None,
                "duration_sec": None,
                "cmd": "",
                "version": "",
                "emby_pre": None,
                "emby_post": None,
                "plex_pre": None,
                "simkl_pre": None,
                "trakt_pre": None,
                "jellyfin_pre": None,
                "mdblist_pre": None,
                "crosswatch_pre": None,
                "plex_post": None,
                "simkl_post": None,
                "trakt_post": None,
                "jellyfin_post": None,
                "mdblist_post": None,
                "crosswatch_post": None,
                "result": "",
                "exit_code": None,
                "timeline": {"start": False, "pre": False, "post": False, "done": False},
                "raw_started_ts": None,
                "_phase": {
                    "snapshot": {"total": 0, "done": 0, "final": False},
                    "apply": {"total": 0, "done": 0, "final": False},
                },
            }
        )

def _summary_set(k: str, v: Any) -> None:
    with SUMMARY_LOCK:
        SUMMARY[k] = v

def _summary_set_timeline(flag: str, value: bool = True) -> None:
    with SUMMARY_LOCK:
        SUMMARY.setdefault("timeline", {})
        SUMMARY["timeline"][flag] = value

def _summary_snapshot() -> dict[str, Any]:
    with SUMMARY_LOCK:
        return dict(SUMMARY)

# Sync progress logging
def _sync_progress_ui(msg: str):
    rt = _rt()
    LOG_BUFFERS, strip_ansi, _append_log = rt[0], rt[7], rt[8]
    try:
        _append_log("SYNC", msg)
        try:
            _parse_sync_line(strip_ansi(msg))
        except Exception as e:
            _append_log("SYNC", f"[!] progress-parse failed: {e}")
    except Exception:
        pass

def _orc_progress(event: str, data: dict):
    rt = _rt()
    _append_log = rt[8]
    try:
        payload = json.dumps({"event": event, **(data or {})}, default=str)
    except Exception:
        payload = f"{event} | {data}"
    _append_log("SYNC", payload[:2000])

def _feature_enabled(fmap: dict, name: str) -> tuple[bool, bool]:
    d = dict(fmap.get(name) or {})
    if isinstance(fmap.get(name), bool):
        return bool(fmap[name]), False
    return bool(d.get("enable", False)), bool(d.get("remove", False))

def _item_sig_key(v: dict) -> str:
    rt = _rt()
    canonical_key = rt[10]
    try:
        return canonical_key(v)
    except Exception:
        ids = v.get("ids") or {}
        for k in ("tmdb", "imdb", "tvdb", "slug"):
            val = ids.get(k)
            if val:
                return f"{k}:{val}".lower()
        t = (str(v.get("title") or v.get("name") or "")).strip().lower()
        y = str(v.get("year") or v.get("release_year") or "")
        typ = (v.get("type") or "").lower()
        return f"{typ}|title:{t}|year:{y}"

# Live sync stats tracking
_LIVE_RUN_KEY: Any = None
_LIVE_LANES: dict[str, dict[str, Any]] = {}

def _live_reset_if_needed(snap: dict) -> None:
    global _LIVE_RUN_KEY, _LIVE_LANES
    running = bool(snap.get("running"))
    run_key = snap.get("raw_started_ts") or snap.get("started_at")
    if (not running) or (run_key != _LIVE_RUN_KEY):
        _LIVE_RUN_KEY = run_key if running else None
        _LIVE_LANES = {}

def _spot_sig(it: dict) -> str:
    try:
        return _item_sig_key(it)
    except Exception:
        t = (str(it.get("title") or it.get("name") or it.get("key") or "")).strip().lower()
        y = str(it.get("year") or it.get("release_year") or "")
        typ = (it.get("type") or "").lower()
        return f"{typ}|title:{t}|year:{y}"

# Orchestrator state loading
def _persist_state_via_orc(orc, *, feature: str = "watchlist") -> dict:
    rt = _rt()
    minimal = rt[9]
    snaps = orc.build_snapshots(feature=feature)
    providers: dict[str, Any] = {}
    wall: list[dict] = []
    seen = set()
    for prov, idx in (snaps or {}).items():
        items_min = {k: minimal(v) for k, v in (idx or {}).items()}
        providers[prov] = {feature: {"baseline": {"items": items_min}, "checkpoint": None}}
        for item in items_min.values():
            key = _item_sig_key(item)
            if key in seen:
                continue
            seen.add(key)
            wall.append(minimal(item))
    state = {"providers": providers, "wall": wall, "last_sync_epoch": int(time.time())}
    orc.files.save_state(state)
    return state

def _run_pairs_thread(run_id: str, overrides: dict | None = None) -> None:
    rt = _rt()
    LOG_BUFFERS, RUNNING_PROCS, STATE_PATH, _append_log, strip_ansi = rt[0], rt[1], rt[3], rt[8], rt[7]
    overrides = overrides or {}
    _summary_reset()
    LOG_BUFFERS["SYNC"] = []
    _sync_progress_ui("::CLEAR::")
    _sync_progress_ui(f"SYNC start: orchestrator pairs run_id={run_id}")

    def _totals_from_log(buf: list[str]) -> dict:
        t = {"attempted": 0, "added": 0, "removed": 0, "skipped": 0, "unresolved": 0, "errors": 0, "blocked": 0}
        for line in buf or []:
            s = strip_ansi(line).strip()
            if not s.startswith("{"):
                continue
            try:
                o = json.loads(s)
            except Exception:
                continue

            ev = str(o.get("event") or "")
            if ev == "apply:add:done":
                t["attempted"] += int(o.get("attempted", 0))
                t["skipped"] += int(o.get("skipped", 0))
                t["unresolved"] += int(o.get("unresolved", 0))
                t["errors"] += int(o.get("errors", 0))
                t["added"] += int(o.get("added", o.get("count", 0)) or 0)

            elif ev == "apply:remove:done":
                t["attempted"] += int(o.get("attempted", 0))
                t["skipped"] += int(o.get("skipped", 0))
                t["unresolved"] += int(o.get("unresolved", 0))
                t["errors"] += int(o.get("errors", 0))
                t["removed"] += int(o.get("removed", o.get("count", 0)) or 0)

            elif ev == "debug":
                msg = str(o.get("msg") or "")
                if msg == "manual.blocks":
                    t["blocked"] += int(o.get("adds_blocked", 0) or 0) + int(o.get("removes_blocked", 0) or 0)
                elif msg == "blocked.manual":
                    t["blocked"] += int(o.get("blocked_items", o.get("blocked_keys", 0)) or 0)

            elif ev == "run:done":
                t["blocked"] = max(t["blocked"], int(o.get("blocked", 0) or 0))

        return t

    try:
        orch_mod = importlib.import_module("cw_platform.orchestrator")
        try:
            orch_mod = importlib.reload(orch_mod)
        except Exception:
            pass
        OrchestratorClass = getattr(orch_mod, "Orchestrator")
        _sync_progress_ui(f"[i] Orchestrator module: {getattr(orch_mod, '__file__', '?')}")
        load_config, _save = _env()
        cfg = load_config()

        # Ensure pair defaults
        def _pair_has_enabled_features(p: dict) -> bool:
            fmap = p.get("features") or {}
            for _, fcfg in (fmap.items() or []):
                if isinstance(fcfg, bool) and fcfg:
                    return True
                if isinstance(fcfg, dict) and fcfg.get("enable"):
                    return True
            return False

        for pair in (cfg.get("pairs") or []):
            if not pair.get("enabled", True):
                continue
            if "features" in pair and not _pair_has_enabled_features(pair):
                src = pair.get("source") or "?"
                dst = pair.get("target") or "?"
                pid = pair.get("id") or ""
                _sync_progress_ui(
                    f"[!] Pair {src} → {dst} ({pid}) has no enabled features; "
                    f"it will not transfer any data."
                )

        mgr = OrchestratorClass(config=cfg)
        dry = bool(((cfg.get("sync") or {}).get("dry_run") or False)) or bool((overrides or {}).get("dry_run"))
        result = mgr.run_pairs(
            dry_run=dry,
            progress=_sync_progress_ui,
            write_state_json=True,
            state_path=STATE_PATH,
            use_snapshot=True,
        )
        added_res = int(result.get("added", 0))
        removed_res = int(result.get("removed", 0))
        try:
            state = _load_state()
            if state:
                _STATS = _rt()[5]
                _STATS.refresh_from_state(state)
                _STATS.record_summary(added_res, removed_res)
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

        totals = _totals_from_log(list(LOG_BUFFERS.get("SYNC") or []))

        def _merge_total(key: str) -> int:
            v_result = int(result.get(key) or 0)
            v_log = int(totals.get(key) or 0)
            return max(v_result, v_log)

        added = _merge_total("added")
        removed = _merge_total("removed")
        skipped = _merge_total("skipped")
        unresolved = _merge_total("unresolved")
        errors = _merge_total("errors")
        blocked = _merge_total("blocked")
        extra = f", Total blocked: {blocked}"
        
        _sync_progress_ui(
            f"[i] Done. Total added: {added}, Total removed: {removed}, "
            f"Total skipped: {skipped}, Total unresolved: {unresolved}, Total errors: {errors}{extra}"
        )

        _sync_progress_ui("[SYNC] exit code: 0")
    except Exception as e:
        _sync_progress_ui(f"[!] Sync error: {e}")
        _sync_progress_ui("[SYNC] exit code: 1")
    finally:
        try:
            load_config, _ = _env()
            cfg2 = load_config()
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

# Lane stats computation
def _parse_epoch(v: Any) -> int:
    if v is None:
        return 0
    try:
        if isinstance(v, (int, float)):
            return int(v)
        s = str(v).strip()
        if s.isdigit():
            return int(s)
        s = s.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        return int(dt.timestamp())
    except Exception:
        return 0

def _lanes_defaults() -> dict[str, dict[str, Any]]:
    def lane():
        return {
            "added": 0,
            "removed": 0,
            "updated": 0,
            "spotlight_add": [],
            "spotlight_remove": [],
            "spotlight_update": [],
        }

    return {"watchlist": lane(), "ratings": lane(), "history": lane(), "playlists": lane()}

def _lanes_enabled_defaults() -> dict[str, bool]:
    return {"watchlist": True, "ratings": True, "history": True, "playlists": True}

def _apply_live_stats_to_snap(snap: dict, stats_feats: dict, enabled: dict) -> dict:
    out = dict(snap or {})
    out.setdefault("features", {})
    feats = out["features"]

    running = bool(out.get("running"))
    _live_reset_if_needed(out)

    for k, v in (stats_feats or {}).items():
        dst = feats.setdefault(
            k,
            {
                "added": 0,
                "removed": 0,
                "updated": 0,
                "spotlight_add": [],
                "spotlight_remove": [],
                "spotlight_update": [],
            },
        )

        va = int((v or {}).get("added") or 0)
        vr = int((v or {}).get("removed") or 0)
        vu = int((v or {}).get("updated") or 0)

        add_list = list((v or {}).get("spotlight_add") or [])[:25]
        rem_list = list((v or {}).get("spotlight_remove") or [])[:25]
        upd_list = list((v or {}).get("spotlight_update") or [])[:25]

        if running:
            prev = _LIVE_LANES.get(k) or _lane_init()

            dst["added"] = max(int(prev.get("added") or 0), va)
            dst["removed"] = max(int(prev.get("removed") or 0), vr)
            dst["updated"] = max(int(prev.get("updated") or 0), vu)

            seen_all = set()
            for bucket in ("spotlight_add", "spotlight_remove", "spotlight_update"):
                for it in (prev.get(bucket) or []):
                    try:
                        seen_all.add(_spot_sig(it))
                    except Exception:
                        pass

            def _merge(prev_bucket: list, new_bucket: list) -> list:
                out_bucket = list(prev_bucket or [])
                for it in (new_bucket or []):
                    sig = _spot_sig(it)
                    if sig in seen_all:
                        continue
                    out_bucket.append(it)
                    seen_all.add(sig)
                return out_bucket[-25:]

            dst["spotlight_add"] = _merge(prev.get("spotlight_add") or [], add_list)
            dst["spotlight_remove"] = _merge(prev.get("spotlight_remove") or [], rem_list)
            dst["spotlight_update"] = _merge(prev.get("spotlight_update") or [], upd_list)

            _LIVE_LANES[k] = {
                "added": dst["added"],
                "removed": dst["removed"],
                "updated": dst["updated"],
                "spotlight_add": dst["spotlight_add"],
                "spotlight_remove": dst["spotlight_remove"],
                "spotlight_update": dst["spotlight_update"],
            }
        else:
            dst["added"] = max(int(dst.get("added") or 0), va)
            dst["removed"] = max(int(dst.get("removed") or 0), vr)
            dst["updated"] = max(int(dst.get("updated") or 0), vu)
            if not dst["spotlight_add"]:
                dst["spotlight_add"] = add_list
            if not dst["spotlight_remove"]:
                dst["spotlight_remove"] = rem_list
            if not dst["spotlight_update"]:
                dst["spotlight_update"] = upd_list

    out["features"] = feats
    out["enabled"] = enabled or _lanes_enabled_defaults()
    return out

def _compute_lanes_from_stats(since_epoch: int, until_epoch: int):
    _STATS = _rt()[5]
    feats = _lanes_defaults()
    enabled = _lanes_enabled_defaults()
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

    def _is_real_item_event(e: dict) -> bool:
        k = str(e.get("key") or "")
        if k.startswith("agg:"):
            return False

        act = str(e.get("action") or e.get("op") or e.get("change") or "").lower()
        ids = e.get("ids") or {}
        title = (e.get("title") or e.get("name") or "").strip()
        feat = str(e.get("feature") or e.get("feat") or "").lower()

        if act.startswith("apply:") and not title and not ids:
            return False

        if feat == "watchlist":
            return bool(title)
        if feat in ("ratings", "history", "playlists"):
            return bool(title or ids)

        return True

    rows = [e for e in events if s <= _evt_epoch(e) <= u and _is_real_item_event(e)]
    if not rows:
        return feats, enabled

    rows.sort(key=_evt_epoch)
    anyin = lambda s, toks: any(t in s for t in toks)

    seen = {
        "watchlist": {"add": set(), "remove": set(), "update": set()},
        "ratings": {"add": set(), "remove": set(), "update": set()},
        "history": {"add": set(), "remove": set(), "update": set()},
        "playlists": {"add": set(), "remove": set(), "update": set()},
    }

    def _sig_for_event(e: dict) -> str:
        k = str(e.get("key") or "").strip().lower()
        if k:
            return k
        ids = (e.get("ids") or {}) or {}
        for idk in ("tmdb", "imdb", "tvdb", "slug"):
            v = ids.get(idk)
            if v:
                return f"{idk}:{str(v).lower()}"
        t = (e.get("title") or "").strip().lower()
        y = str(e.get("year") or e.get("release_year") or "")
        typ = (e.get("type") or "").strip().lower()
        return f"{typ}|title:{t}|year:{y}"

    for e in rows:
        action = (
            str(e.get("action") or e.get("op") or e.get("change") or "")
            .lower()
            .replace(":", "_")
            .replace("-", "_")
        )
        feat = (
            str(e.get("feature") or e.get("feat") or "")
            .lower()
            .replace(":", "_")
            .replace("-", "_")
        )
        title = (e.get("title") or e.get("key") or "item")

        slim = {
            k: e.get(k)
            for k in (
                "title",
                "series_title",
                "name",
                "key",
                "type",
                "source",
                "year",
                "season",
                "episode",
                "added_at",
                "listed_at",
                "watched_at",
                "rated_at",
                "last_watched_at",
                "user_rated_at",
                "ts",
                "seen_ts",
                "sync_ts",
                "ingested_ts",
            )
            if k in e and e.get(k) is not None
        }
        if "title" not in slim:
            slim["title"] = title

        sig = _sig_for_event(e)

        # Watchlist lane
        if ("watchlist" in action) or (feat == "watchlist"):
            lane = "watchlist"
            if anyin(action, ("remove", "unwatchlist", "delete", "del", "rm", "clear")):
                if sig not in seen[lane]["remove"]:
                    seen[lane]["remove"].add(sig)
                    feats[lane]["removed"] += 1
                    feats[lane]["spotlight_remove"].append(slim)
            elif anyin(action, ("update", "rename", "edit", "move", "reorder", "relist")):
                if sig in seen[lane]["add"] or sig in seen[lane]["remove"]:
                    continue
                if sig not in seen[lane]["update"]:
                    seen[lane]["update"].add(sig)
                    feats[lane]["updated"] += 1
                    feats[lane]["spotlight_update"].append(slim)
            else:
                if sig not in seen[lane]["add"]:
                    seen[lane]["add"].add(sig)
                    feats[lane]["added"] += 1
                    feats[lane]["spotlight_add"].append(slim)
            continue

        # Ratings lane
        if (action in ("rate", "rating", "update_rating", "unrate")) or ("rating" in action) or ("rating" in feat):
            lane = "ratings"
            if anyin(action, ("unrate", "remove", "clear", "delete", "unset", "erase")):
                if sig not in seen[lane]["remove"]:
                    seen[lane]["remove"].add(sig)
                    feats[lane]["removed"] += 1
                    feats[lane]["spotlight_remove"].append(slim)
            elif anyin(action, ("rate", "add", "set", "set_rating", "update_rating")):
                if sig not in seen[lane]["add"]:
                    seen[lane]["add"].add(sig)
                    feats[lane]["added"] += 1
                    feats[lane]["spotlight_add"].append(slim)
            else:
                if sig in seen[lane]["add"] or sig in seen[lane]["remove"]:
                    continue
                if sig not in seen[lane]["update"]:
                    seen[lane]["update"].add(sig)
                    feats[lane]["updated"] += 1
                    feats[lane]["spotlight_update"].append(slim)
            continue

        # History lane
        is_history_feat = (feat in ("history", "watch", "watched")) or ("history" in action)
        if "watchlist" not in action:
            is_add_like = anyin(
                action,
                (
                    "watch",
                    "scrobble",
                    "checkin",
                    "mark_watched",
                    "history_add",
                    "add_history",
                    "apply_add",
                    "apply_add_done",
                ),
            )
            is_remove_like = anyin(
                action,
                (
                    "unwatch",
                    "remove_history",
                    "history_remove",
                    "delete_watch",
                    "del_history",
                    "apply_remove",
                    "apply_remove_done",
                ),
            )
        else:
            is_add_like = is_remove_like = False

        is_update_like = anyin(action, ("update", "edit", "fix", "repair", "adjust", "correct"))

        if is_history_feat or is_add_like or is_remove_like:
            lane = "history"
            if is_remove_like:
                if sig not in seen[lane]["remove"]:
                    seen[lane]["remove"].add(sig)
                    feats[lane]["removed"] += 1
                    feats[lane]["spotlight_remove"].append(slim)
            elif is_add_like:
                if sig not in seen[lane]["add"]:
                    seen[lane]["add"].add(sig)
                    feats[lane]["added"] += 1
                    feats[lane]["spotlight_add"].append(slim)
            elif is_update_like:
                if sig in seen[lane]["add"] or sig in seen[lane]["remove"]:
                    continue
                if sig not in seen[lane]["update"]:
                    seen[lane]["update"].add(sig)
                    feats[lane]["updated"] += 1
                    feats[lane]["spotlight_update"].append(slim)
            else:
                if sig not in seen[lane]["add"]:
                    seen[lane]["add"].add(sig)
                    feats[lane]["added"] += 1
                    feats[lane]["spotlight_add"].append(slim)
            continue

    for lane in feats.values():
        lane["spotlight_add"] = list((lane.get("spotlight_add") or [])[-25:])[::-1]
        lane["spotlight_remove"] = list((lane.get("spotlight_remove") or [])[-25:])[::-1]
        lane["spotlight_update"] = list((lane.get("spotlight_update") or [])[-25:])[::-1]
    return feats, enabled

def _parse_sync_line(line: str) -> None:
    s = _rt()[7](line).strip()
    try:
        o = json.loads(s)
        if isinstance(o, dict) and o.get("event"):
            ev = str(o.get("event") or "")
            
            if ev in ("one:plan", "two:plan"):
                phase = SUMMARY.setdefault("_phase", {})
                apply_phase = phase.setdefault(
                    "apply", {"total": 0, "done": 0, "final": False}
                )

                if ev == "one:plan":
                    adds = int(o.get("adds") or 0)
                    rems = int(o.get("removes") or 0)
                    delta = max(0, adds) + max(0, rems)
                else:
                    delta = 0
                    for k in ("add_to_A", "add_to_B", "rem_from_A", "rem_from_B"):
                        try:
                            delta += max(0, int(o.get(k) or 0))
                        except Exception:
                            pass

                apply_phase["total"] = int(apply_phase.get("total") or 0) + delta
                _summary_set("_phase", phase)
                return
            
            feat = str(o.get("feature") or "").lower()
            if feat in ("watchlist", "history", "ratings", "playlists"):
                F = SUMMARY.setdefault("features", {})
                if feat not in F:
                    F[feat] = {
                        "added": 0,
                        "removed": 0,
                        "updated": 0,
                        "spotlight_add": [],
                        "spotlight_remove": [],
                        "spotlight_update": [],
                    }
                getc = lambda obj: int(
                    (
                        (obj.get("result") or {}).get("count")
                        if isinstance(obj.get("result"), dict)
                        else None
                    )
                    or obj.get("count")
                    or 0
                )
                if ev in ("apply:add:done", "apply:remove:done", "apply:update:done"):
                    cnt = getc(o)
                    if ev == "apply:add:done":
                        F[feat]["added"] += cnt
                    elif ev == "apply:remove:done":
                        F[feat]["removed"] += cnt
                    else:
                        F[feat]["updated"] += cnt
                    _summary_set("features", F)

                    phase = SUMMARY.setdefault("_phase", {})
                    apply_phase = phase.setdefault(
                        "apply", {"total": 0, "done": 0, "final": False}
                    )
                    apply_phase["done"] = int(apply_phase.get("done") or 0) + cnt
                    _summary_set("_phase", phase)
                    return

                if ev == "debug" and str(o.get("msg") or "") == "apply:add:corrected":
                    eff = int(o.get("effective") or 0)
                    if eff > int(F[feat].get("added") or 0):
                        F[feat]["added"] = eff
                    _summary_set("features", F)
                    return
    except Exception:
        pass

    m = re.match(r"^(?:>\s*)?SYNC start:\s+(?P<cmd>.+)$", s)
    if m:
        if not SUMMARY.get("running"):
            _summary_set("running", True)
            SUMMARY["raw_started_ts"] = time.time()
            _summary_set("started_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        cmd_str = m.group("cmd")
        short_cmd = cmd_str
        try:
            parts = shlex.split(cmd_str)
            script = next((os.path.basename(p) for p in reversed(parts) if p.endswith(".py")), None)
            if script:
                short_cmd = script
            elif parts:
                short_cmd = os.path.basename(parts[0])
        except Exception:
            pass
        _summary_set("cmd", short_cmd)
        _summary_set_timeline("start", True)
        return

    m = re.search(r"Pre-sync counts:\s*(?P<pairs>.+)$", s, re.IGNORECASE)
    if m:
        pairs = re.findall(r"\b([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\d+)", m.group("pairs"))
        for name, val in pairs:
            key = name.lower()
            try:
                val_i = int(val)
            except Exception:
                continue
            if key in ("plex", "simkl", "trakt", "jellyfin", "emby", "mdblist", "crosswatch"):
                _summary_set(f"{key}_pre", val_i)
        _summary_set_timeline("pre", True)
        return

    m = re.search(r"Post-sync:\s*(?P<rest>.+)$", s, re.IGNORECASE)
    if m:
        rest = m.group("rest")
        pairs = re.findall(r"\b([A-Za-z][A-Za-z0-9_-]*)\s*=\s*(\d+)", rest)
        for name, val in pairs:
            key = name.lower()
            try:
                val_i = int(val)
            except Exception:
                continue
            if key in ("plex", "simkl", "trakt", "jellyfin", "emby", "mdblist", "crosswatch"):
                _summary_set(f"{key}_post", val_i)
        mres = re.search(r"(?:→|->|=>)\s*([A-Za-z]+)", rest)
        if mres:
            _summary_set("result", mres.group(1).upper())
        _summary_set_timeline("post", True)
        return

    m = re.search(r"\[SYNC\]\s+exit code:\s+(?P<code>\d+)", s)
    if m:
        code = int(m.group("code"))
        _summary_set("exit_code", code)
        started = SUMMARY.get("raw_started_ts")
        if started:
            dur = max(0.0, time.time() - float(started))
            _summary_set("duration_sec", round(dur, 2))
        _summary_set("finished_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
        _summary_set("running", False)
        _summary_set_timeline("done", True)
        try:
            phase = SUMMARY.setdefault("_phase", {})
            prev_apply = phase.get("apply") or {}
            snap_phase = phase.setdefault(
                "snapshot",
                {"total": 1, "done": 1, "final": True},
            )
            apply_phase = phase.setdefault(
                "apply",
                {
                    "total": int(prev_apply.get("total") or 0),
                    "done": int(prev_apply.get("done") or 0),
                    "final": True,
                },
            )
            apply_phase["final"] = True
            snap_phase["final"] = True
            if not snap_phase.get("total"):
                snap_phase["total"] = 1
            snap_phase["done"] = snap_phase.get("total")
            _summary_set("_phase", phase)
        except Exception:
            pass
        try:
            tl = SUMMARY.get("timeline") or {}
            if tl.get("done"):
                if not tl.get("pre"):
                    _summary_set_timeline("pre", True)
                if not tl.get("post"):
                    _summary_set_timeline("post", True)
        except Exception:
            pass
        try:
            snap = _summary_snapshot()
            since = _parse_epoch(snap.get("raw_started_ts") or snap.get("started_at"))
            until = _parse_epoch(snap.get("finished_at")) or int(time.time())
            feats_tmp, enabled_tmp = _compute_lanes_from_stats(since, until)

            snap.setdefault("features", {})
            for k, v in (feats_tmp or {}).items():
                dst = snap["features"].setdefault(
                    k,
                    {
                        "added": 0,
                        "removed": 0,
                        "updated": 0,
                        "spotlight_add": [],
                        "spotlight_remove": [],
                        "spotlight_update": [],
                    },
                )
                va = int((v or {}).get("added") or 0)
                vr = int((v or {}).get("removed") or 0)
                vu = int((v or {}).get("updated") or 0)
                dst["added"] = max(int(dst.get("added") or 0), va)
                dst["removed"] = max(int(dst.get("removed") or 0), vr)
                dst["updated"] = max(int(dst.get("updated") or 0), vu)
                if not dst["spotlight_add"]:
                    dst["spotlight_add"] = list((v or {}).get("spotlight_add") or [])[:25]
                if not dst["spotlight_remove"]:
                    dst["spotlight_remove"] = list((v or {}).get("spotlight_remove") or [])[:25]
                if not dst["spotlight_update"]:
                    dst["spotlight_update"] = list((v or {}).get("spotlight_update") or [])[:25]

            lanes = snap.get("features") or {}
            try:
                _STATS = _rt()[5]
                with _STATS.lock:
                    evs = list(_STATS.data.get("events") or [])

                def _evt_ts(e):
                    for k in ("ts", "seen_ts", "sync_ts", "ingested_ts"):
                        try:
                            v = int(e.get(k) or 0)
                            if v:
                                return v
                        except Exception:
                            pass
                    return 0

                for feat in ("ratings", "history"):
                    lane = lanes.get(feat) or {}
                    if (
                        (lane.get("added", 0) + lane.get("removed", 0) + lane.get("updated", 0)) > 0
                        and not (
                            lane.get("spotlight_add")
                            or lane.get("spotlight_remove")
                            or lane.get("spotlight_update")
                        )
                    ):
                        rows = [
                            e
                            for e in evs
                            if str(e.get("feature") or e.get("feat") or "").lower() == feat
                            and not str(e.get("key") or "").startswith("agg:")
                            and since <= _evt_ts(e) <= until
                        ]
                        rows.sort(key=_evt_ts)
                        for e in reversed(rows[-25:]):
                            act = str(
                                e.get("action") or e.get("op") or e.get("change") or ""
                            ).lower()
                            slim = {
                                k: e.get(k)
                                for k in (
                                    "title",
                                    "series_title",
                                    "name",
                                    "key",
                                    "type",
                                    "source",
                                    "year",
                                    "season",
                                    "episode",
                                    "added_at",
                                    "listed_at",
                                    "watched_at",
                                    "rated_at",
                                    "last_watched_at",
                                    "user_rated_at",
                                    "ts",
                                    "seen_ts",
                                    "sync_ts",
                                    "ingested_ts",
                                )
                                if k in e and e.get(k) is not None
                            }
                            if "title" not in slim:
                                slim["title"] = e.get("title") or e.get("key") or "item"
                            if any(t in act for t in ("remove", "unrate", "delete", "clear")):
                                lane.setdefault("spotlight_remove", []).append(slim)
                            elif ("update" in act) or ("rate" in act):
                                lane.setdefault("spotlight_update", []).append(slim)
                            else:
                                lane.setdefault("spotlight_add", []).append(slim)
                        lanes[feat] = lane
            except Exception:
                pass

            _summary_set("enabled", enabled_tmp)
            _summary_set("features", lanes)

            a = r = u = 0
            for k, d in (lanes or {}).items():
                if isinstance(enabled_tmp, dict) and enabled_tmp.get(k) is False:
                    continue
                a += int((d or {}).get("added") or 0)
                r += int((d or {}).get("removed") or 0)
                u += int((d or {}).get("updated") or 0)
            _summary_set("added_last", a)
            _summary_set("removed_last", r)
            _summary_set("updated_last", u)

            _STATS = _rt()[5]
            run_id = snap.get("finished_at") or snap.get("started_at") or ""
            for name, lane in (lanes or {}).items():
                aa = int((lane or {}).get("added") or 0)
                rr = int((lane or {}).get("removed") or 0)
                uu = int((lane or {}).get("updated") or 0)
                if aa or rr or uu:
                    _STATS.record_feature_totals(
                        name,
                        added=aa,
                        removed=rr,
                        updated=uu,
                        src="REPORT",
                        run_id=run_id,
                        expand_events=True,
                    )

        except Exception:
            pass
        try:
            REPORT_DIR = _rt()[6]
            ts = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            path = REPORT_DIR / f"sync-{ts}.json"
            with path.open("w", encoding="utf-8") as f:
                json.dump(_summary_snapshot(), f, indent=2)
        except Exception:
            pass

# State file helpers
def _find_state_path() -> Path | None:
    for p in _rt()[4]:
        if p.exists():
            return p
    return None

def _load_state() -> dict[str, Any]:
    sp = _find_state_path()
    if not sp:
        return {}
    try:
        return json.loads(sp.read_text(encoding="utf-8"))
    except Exception:
        return {}

# Rating/action mapping
_R_ACTION_MAP = {
    "add": "add",
    "rate": "add",
    "remove": "remove",
    "unrate": "remove",
    "update": "update",
    "update_rating": "update",
}

def _lane_is_empty(v: dict | None) -> bool:
    if not isinstance(v, dict):
        return True
    has_counts = (v.get("added") or 0) + (v.get("removed") or 0) + (v.get("updated") or 0) > 0
    has_spots = any(v.get(k) for k in ("spotlight_add", "spotlight_remove", "spotlight_update"))
    return not (has_counts or has_spots)

# Utility functions for lane summaries
def _lane_init():
    return {
        "added": 0,
        "removed": 0,
        "updated": 0,
        "spotlight_add": [],
        "spotlight_remove": [],
        "spotlight_update": [],
    }

def _ensure_feature(summary_obj: dict, feature: str) -> dict:
    feats = summary_obj.setdefault("features", {})
    lane = feats.setdefault(feature, _lane_init())
    lane.setdefault("added", 0)
    lane.setdefault("removed", 0)
    lane.setdefault("updated", 0)
    lane.setdefault("spotlight_add", [])
    lane.setdefault("spotlight_remove", [])
    lane.setdefault("spotlight_update", [])
    return lane

def _push_spotlight(lane: dict, kind: str, items: list, max3: bool = True):
    key = {
        "add": "spotlight_add",
        "remove": "spotlight_remove",
        "update": "spotlight_update",
    }.get(kind, "spotlight_add")
    dst = lane.setdefault(key, [])
    seen = set(dst)
    for it in (items or []):
        t = (it.get("title") or it.get("name") or it.get("key") or str(it))[:200]
        if t and t not in seen:
            dst.append(t)
            seen.add(t)
            if max3 and len(dst) >= 3:
                break

def _push_spot_titles(dst: list, items: list, max3: bool = True):
    seen = set(dst)
    for it in (items or []):
        t = (it.get("title") or it.get("name") or it.get("key") or str(it))[:200]
        if t and t not in seen:
            dst.append(t)
            seen.add(t)
            if max3 and len(dst) >= 3:
                break

# Check if sync is running
def _is_sync_running() -> bool:
    RUNNING_PROCS = _rt()[1]
    t = RUNNING_PROCS.get("SYNC")
    return bool(t and t.is_alive())

# API endpoint to list sync providers
@router.get("/sync/providers")
def api_sync_providers() -> JSONResponse:
    HIDDEN = {"BASE"}
    PKG_CANDIDATES = ("providers.sync",)
    FEATURE_KEYS = ("watchlist", "ratings", "history", "playlists")

    def _asdict_dc(obj):
        try:
            if _dc.is_dataclass(obj):
                return _dc.asdict(obj if not isinstance(obj, type) else obj())
        except Exception:
            return None

    def _norm_features(f: dict | None) -> dict:
        f = dict(f or {})
        return {
            k: bool(
                (f.get(k) or {}).get("enable", (f.get(k) or {}).get("enabled", False))
                if isinstance(f.get(k), dict)
                else f.get(k)
            )
            for k in FEATURE_KEYS
        }

    def _norm_caps(caps: dict | None) -> dict:
        caps = dict(caps or {})
        return {"bidirectional": bool(caps.get("bidirectional", False))}

    def _manifest_from_module(mod) -> dict | None:
        if hasattr(mod, "get_manifest") and callable(mod.get_manifest):
            try:
                mf = dict(cast(Any, mod.get_manifest()))
            except Exception:
                mf = None
                
            if mf and not (mf.get("hidden") or mf.get("is_template")):
                return {
                    "name": (mf.get("name") or "").upper(),
                    "label": mf.get("label") or (mf.get("name") or "").title(),
                    "features": _norm_features(mf.get("features")),
                    "capabilities": _norm_caps(mf.get("capabilities")),
                    "version": mf.get("version"),
                    "vendor": mf.get("vendor"),
                    "description": mf.get("description"),
                }
        cand = [
            cls
            for _, cls in inspect.getmembers(mod, inspect.isclass)
            if cls.__module__ == mod.__name__ and cls.__name__.endswith("Module")
        ]
        if cand:
            cls = cand[0]
            info = getattr(cls, "info", None)
            if info is not None:
                caps = _asdict_dc(getattr(info, "capabilities", None)) or {}
                name = (
                    getattr(info, "name", None)
                    or getattr(cls, "__name__", "").replace("Module", "")
                ).upper()
                label = (getattr(info, "name", None) or name).title()
                if bool(
                    getattr(info, "hidden", False) or getattr(info, "is_template", False)
                ):
                    return None
                try:
                    feats = dict(cls.supported_features()) if hasattr(cls, "supported_features") else {}
                except Exception:
                    feats = {}
                return {
                    "name": name,
                    "label": label,
                    "features": _norm_features(feats),
                    "capabilities": _norm_caps(caps),
                    "version": getattr(info, "version", None),
                    "vendor": getattr(info, "vendor", None),
                    "description": getattr(info, "description", None),
                }
        ops = getattr(mod, "OPS", None)
        if ops is not None:
            try:
                name = str(ops.name()).upper()
                label = str(ops.label() if hasattr(ops, "label") else name.title())
                feats = dict(ops.features()) if hasattr(ops, "features") else {}
                caps = dict(ops.capabilities()) if hasattr(ops, "capabilities") else {}
                return {
                    "name": name,
                    "label": label,
                    "features": _norm_features(feats),
                    "capabilities": _norm_caps(caps),
                    "version": None,
                    "vendor": None,
                    "description": None,
                }
            except Exception:
                return None
        return None

    items: list[dict[str, Any]] = []
    seen: set[str] = set()
    for pkg_name in PKG_CANDIDATES:
        try:
            pkg = importlib.import_module(pkg_name)
        except Exception:
            continue
        for pkg_path in getattr(pkg, "__path__", []):
            for m in pkgutil.iter_modules([str(pkg_path)]):
                if not m.name.startswith("_mod_"):
                    continue
                prov_key = m.name.replace("_mod_", "").upper()
                if prov_key in HIDDEN:
                    continue
                try:
                    mod = importlib.import_module(f"{pkg_name}.{m.name}")
                except Exception:
                    continue
                mf = _manifest_from_module(mod)
                if not mf:
                    continue
                mf["name"] = (mf["name"] or prov_key).upper()
                mf["label"] = mf.get("label") or mf["name"].title()
                mf["features"] = _norm_features(mf.get("features"))
                mf["capabilities"] = _norm_caps(mf.get("capabilities"))
                if mf["name"] in seen:
                    continue
                seen.add(mf["name"])
                items.append(mf)
    items.sort(key=lambda x: (x.get("label") or x.get("name") or "").lower())
    return JSONResponse(items)

# Pairs data models
class PairIn(BaseModel):
    source: str
    target: str
    mode: str | None = None
    enabled: bool | None = None
    features: dict[str, Any] | None = None

class PairPatch(BaseModel):
    source: str | None = None
    target: str | None = None
    mode: str | None = None
    enabled: bool | None = None
    features: dict[str, Any] | None = None

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
        try:
            _rt()[8]("TRBL", f"/api/pairs GET failed: {e}")
        except Exception:
            pass
        return JSONResponse({"ok": False, "error": str(e)}, status_code=500)

@router.post("/pairs")
def api_pairs_add(payload: PairIn = Body(...)) -> dict[str, Any]:
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
        try:
            _rt()[8]("TRBL", f"/api/pairs POST failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

@router.post("/pairs/reorder")
def api_pairs_reorder(order: list[str] = Body(...)) -> dict:
    load_config, save_config = _env()
    try:
        cfg = load_config()
        arr = _cfg_pairs(cfg)

        index_map = {str(p.get("id")): i for i, p in enumerate(arr)}
        seen: set[str] = set()
        wanted_ids: list[str] = []
        for pid in (order or []):
            spid = str(pid)
            if spid in index_map and spid not in seen:
                wanted_ids.append(spid)
                seen.add(spid)

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
        return {
            "ok": True,
            "reordered": changed,
            "count": len(new_arr),
            "unknown_ids": unknown_ids,
            "final_order": final_ids,
        }
    except Exception as e:
        try:
            _rt()[8]("TRBL", f"/api/pairs/reorder failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

@router.put("/pairs/{pair_id}")
def api_pairs_update(pair_id: str, payload: PairPatch = Body(...)) -> dict[str, Any]:
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
        try:
            _rt()[8]("TRBL", f"/api/pairs PUT failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

@router.delete("/pairs/{pair_id}")
def api_pairs_delete(pair_id: str) -> dict[str, Any]:
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
        try:
            _rt()[8]("TRBL", f"/api/pairs DELETE failed: {e}")
        except Exception:
            pass
        return {"ok": False, "error": str(e)}

# Provider counts endpoint
_PROVIDER_COUNTS_CACHE = {"ts": 0.0, "data": None}
_PROVIDER_ORDER = ("PLEX", "SIMKL", "TRAKT", "JELLYFIN", "EMBY", "MDBLIST", "CROSSWATCH")

def _counts_from_state(state: dict | None) -> dict | None:
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
            _append_log(
                "SYNC",
                f"[!] counts: provider '{key}' node is {type(pdata).__name__} (expected dict); skipping",
            )
            continue

        wl = pdata.get("watchlist")
        count = 0

        if isinstance(wl, dict):
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
                        try:
                            count = int(items_node)
                        except Exception:
                            _append_log(
                                "SYNC",
                                f"[!] counts: provider '{key}' watchlist.items is non-numeric {type(items_node).__name__}; using 0",
                            )
                            count = 0
        elif isinstance(wl, list):
            count = len(wl)
        elif isinstance(wl, (int, str)):
            try:
                count = int(wl)
            except Exception:
                _append_log(
                    "SYNC",
                    f"[!] counts: provider '{key}' watchlist is non-numeric {type(wl).__name__}; using 0",
                )
                count = 0
        elif wl is not None:
            _append_log(
                "SYNC",
                f"[!] counts: provider '{key}' watchlist unexpected type {type(wl).__name__}; using 0",
            )

        out[key] = count

    return out

def _counts_from_orchestrator(cfg: dict) -> dict:
    from cw_platform.orchestrator import Orchestrator
    snaps = Orchestrator(config=cfg).build_snapshots(feature="watchlist")
    out = {k: 0 for k in _PROVIDER_ORDER}
    if isinstance(snaps, dict):
        for name in _PROVIDER_ORDER:
            out[name] = len((snaps.get(name) or {}) if isinstance(snaps.get(name), dict) else {})
    return out

def _provider_counts_fast(cfg: dict, *, max_age: int = 30, force: bool = False) -> dict:
    now = time.time()
    if (
        not force
        and _PROVIDER_COUNTS_CACHE["data"]
        and (now - _PROVIDER_COUNTS_CACHE["ts"] < max(0, int(max_age)))
    ):
        return dict(_PROVIDER_COUNTS_CACHE["data"])
    counts = _counts_from_state(_load_state())
    if counts is None:
        counts = _counts_from_orchestrator(cfg)
    _PROVIDER_COUNTS_CACHE["ts"] = now
    _PROVIDER_COUNTS_CACHE["data"] = counts
    return counts

@router.get("/sync/providers/counts")
def api_provider_counts(
    max_age: int = 30,
    force: bool = False,
    source: str = "auto",
) -> dict:
    load_config, _ = _env()
    cfg = load_config()

    src = (source or "auto").lower().strip()
    if src == "state":
        counts = _counts_from_state(_load_state()) or {k: 0 for k in _PROVIDER_ORDER}
        return counts
    return _provider_counts_fast(cfg, max_age=max_age, force=bool(force))

# Trigger sync run endpoint
@router.post("/run")
def api_run_sync(payload: dict | None = Body(None)) -> dict[str, Any]:
    rt = _rt()
    LOG_BUFFERS, RUNNING_PROCS, SYNC_PROC_LOCK = rt[0], rt[1], rt[2]
    with SYNC_PROC_LOCK:
        if _is_sync_running():
            return {"ok": False, "error": "Sync already running"}
        cfg = _env()[0]()  # load_config
        pairs = list((cfg or {}).get("pairs") or [])
        if not any(p.get("enabled", True) for p in pairs):
            _summary_reset()
            _summary_set("raw_started_ts", str(time.time()))
            _summary_set("started_at", datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"))
            _summary_set_timeline("start", True)
            _sync_progress_ui("[i] No pairs configured - skipping sync. Configure/Enable one or more pairs to enable syncing.")
            _sync_progress_ui("[SYNC] exit code: 0")

            return {"ok": True, "skipped": "no_pairs_configured"}
        run_id = str(int(time.time()))
        th = threading.Thread(
            target=_run_pairs_thread,
            args=(run_id,),
            kwargs={"overrides": (payload or {})},
            daemon=True,
        )
        th.start()
        RUNNING_PROCS["SYNC"] = th
        _rt()[8]("SYNC", f"[i] Triggered sync run {run_id}")
        return {"ok": True, "run_id": run_id}

@router.get("/run/summary")
def api_run_summary() -> JSONResponse:
    snap0 = _summary_snapshot()
    since = _parse_epoch(snap0.get("raw_started_ts") or snap0.get("started_at"))
    until = _parse_epoch(snap0.get("finished_at"))
    if not until and snap0.get("running"):
        until = int(time.time())

    stats_feats, enabled = _compute_lanes_from_stats(since, until)
    snap = _apply_live_stats_to_snap(snap0, stats_feats, enabled)

    try:
        feats = snap.get("features") or {}
        _STATS = _rt()[5]
        with _STATS.lock:
            evs = list(_STATS.data.get("events") or [])

        def _evt_ts(e):
            for k in ("ts", "seen_ts", "sync_ts", "ingested_ts"):
                try:
                    v = int(e.get(k) or 0)
                    if v:
                        return v
                except Exception:
                    pass
            return 0

        for feat in ("ratings", "history"):
            lane = feats.get(feat) or {}
            if (
                (lane.get("added", 0) + lane.get("removed", 0) + lane.get("updated", 0)) > 0
                and not (
                    lane.get("spotlight_add")
                    or lane.get("spotlight_remove")
                    or lane.get("spotlight_update")
                )
            ):
                rows = [
                    e
                    for e in evs
                    if str(e.get("feature") or e.get("feat") or "").lower() == feat
                    and not str(e.get("key") or "").startswith("agg:")
                    and (since <= _evt_ts(e) <= (until or _evt_ts(e)))
                ]
                rows.sort(key=_evt_ts)
                for e in reversed(rows[-25:]):
                    act = str(e.get("action") or e.get("op") or e.get("change") or "").lower()
                    slim = {
                        k: e.get(k)
                        for k in (
                            "title",
                            "series_title",
                            "name",
                            "key",
                            "type",
                            "source",
                            "year",
                            "season",
                            "episode",
                            "added_at",
                            "listed_at",
                            "watched_at",
                            "rated_at",
                            "last_watched_at",
                            "user_rated_at",
                            "ts",
                            "seen_ts",
                            "sync_ts",
                            "ingested_ts",
                        )
                        if k in e and e.get(k) is not None
                    }
                    if "title" not in slim:
                        slim["title"] = e.get("title") or e.get("key") or "item"

                    if any(t in act for t in ("remove", "unrate", "delete", "clear")):
                        lane.setdefault("spotlight_remove", []).append(slim)
                    elif ("update" in act) or ("rate" in act):
                        lane.setdefault("spotlight_update", []).append(slim)
                    else:
                        lane.setdefault("spotlight_add", []).append(slim)
                feats[feat] = lane

        snap["features"] = feats
    except Exception:
        pass

    tl = snap.get("timeline") or {}
    if tl.get("done") and not tl.get("post"):
        tl["post"] = True
        tl["pre"] = True
        snap["timeline"] = tl

    return JSONResponse(snap)

@router.get("/run/summary/file")
def api_run_summary_file() -> Response:
    snap0 = _summary_snapshot()
    since = _parse_epoch(snap0.get("raw_started_ts") or snap0.get("started_at"))
    until = _parse_epoch(snap0.get("finished_at"))
    if not until and snap0.get("running"):
        until = int(time.time())

    stats_feats, enabled = _compute_lanes_from_stats(since, until)
    snap = _apply_live_stats_to_snap(snap0, stats_feats, enabled)

    js = json.dumps(snap, indent=2)
    return Response(
        content=js,
        media_type="application/json",
        headers={"Content-Disposition": 'attachment; filename="last_sync.json"'},
    )

@router.get("/run/summary/stream")
async def api_run_summary_stream(request: Request) -> StreamingResponse:
    import html, re
    TAG_RE = re.compile(r"<[^>]+>")

    def dehtml(s: str) -> str:
        return html.unescape(TAG_RE.sub("", s or ""))

    async def agen():
        last_key = None
        last_idx = 0
        LOG_BUFFERS = _rt()[0]

        while True:
            if await request.is_disconnected():
                break
            try:
                buf = LOG_BUFFERS.get("SYNC") or []
                if last_idx > len(buf):
                    last_idx = 0
                if last_idx < len(buf):
                    for line in buf[last_idx:]:
                        raw = dehtml(line).strip()
                        if raw.startswith("{"):
                            try:
                                obj = json.loads(raw)
                            except Exception:
                                continue
                            evt = (str(obj.get("event") or "log").strip() or "log")
                            yield f"event: {evt}\n"
                            yield f"data: {json.dumps(obj, separators=(',',':'))}\n\n"
                    last_idx = len(buf)
            except Exception:
                pass

            snap0 = _summary_snapshot()
            since = _parse_epoch(snap0.get("raw_started_ts") or snap0.get("started_at"))
            until = _parse_epoch(snap0.get("finished_at"))
            if not until and snap0.get("running"):
                until = int(time.time())

            stats_feats, enabled = _compute_lanes_from_stats(since, until)
            snap = _apply_live_stats_to_snap(snap0, stats_feats, enabled)

            key = (
                snap.get("running"),
                snap.get("exit_code"),
                snap.get("plex_post"),
                snap.get("simkl_post"),
                snap.get("trakt_post"),
                snap.get("jellyfin_post"),
                snap.get("emby_post"),
                snap.get("mdblist_post"),
                snap.get("crosswatch_post"),
                snap.get("result"),
                snap.get("duration_sec"),
                (snap.get("timeline", {}) or {}).get("done"),
                json.dumps(snap.get("features", {}), sort_keys=True),
                json.dumps(snap.get("enabled", {}), sort_keys=True),
            )

            if key != last_key:
                last_key = key
                yield f"data: {json.dumps(snap, separators=(',',':'))}\n\n"

            await asyncio.sleep(0.25)

    return StreamingResponse(agen(), media_type="text/event-stream")
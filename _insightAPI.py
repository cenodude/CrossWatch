# _inightAPI.py
# CrossWatch - Insights API for multiple services
# Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
import json, time
from typing import Any, Dict, List, Tuple, Optional, Callable
from contextlib import nullcontext
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse
from pathlib import Path
import datetime as _dt

def _env():
    try:
        import crosswatch as CW
        from cw_platform.config_base import load_config as _load_cfg
        from _metaAPI import get_runtime as _get_runtime
        return CW, _load_cfg, _get_runtime
    except Exception:
        return None, (lambda: {}), (lambda *a, **k: None)

def register_insights(app: FastAPI):
    @app.get("/api/stats/raw", tags=["insight"])
    def api_stats_raw() -> JSONResponse:
        CW, _, _ = _env()
        STATS = getattr(CW, "STATS", None)
        if STATS is None:
            return JSONResponse({})
        lock = getattr(STATS, "lock", None) or nullcontext()
        try:
            with lock:
                return JSONResponse(json.loads(json.dumps(STATS.data)))
        except Exception:
            return JSONResponse({})

    @app.get("/api/stats", tags=["insight"])
    def api_stats() -> Dict[str, Any]:
        CW, _, _ = _env()
        STATS       = getattr(CW, "STATS", None)
        _load_state = getattr(CW, "_load_state", lambda: None)
        StatsClass  = getattr(CW, "Stats", None)

        try:
            state = _load_state()
        except Exception:
            state = None

        base: Dict[str, Any] = {}
        try:
            if STATS and hasattr(STATS, "overview"):
                base = STATS.overview(state) or {}
        except Exception:
            base = {}

        try:
            if (not base.get("now")) and state and StatsClass and hasattr(StatsClass, "_build_union_map"):
                base["now"] = len(StatsClass._build_union_map(state, "watchlist"))
        except Exception:
            pass

        return {"ok": True, **base}
    
    @app.post("/api/crosswatch/select-snapshot")
    def api_select_snapshot(
        feature: str = Query(..., regex="^(watchlist|history|ratings)$"),
        snapshot: str = Query(...),
    ) -> Dict[str, Any]:
        CW, load_config, _ = _env()
        try:
            cfg = load_config() or {}
        except Exception:
            cfg = {}
        cw = (cfg.get("crosswatch") or cfg.get("CrossWatch") or {}) or {}
        key = f"restore_{feature}"
        cw[key] = snapshot
        cfg["crosswatch"] = cw
        try:
            config_dir = getattr(CW, "CONFIG_DIR", None)
            if config_dir is None:
                config_dir = Path("/config")
            cfg_path = Path(config_dir) / "config.json"
            cfg_path.write_text(
                json.dumps(cfg, indent=2, sort_keys=True),
                encoding="utf-8",
            )
        except Exception as e:
            return {"ok": False, "error": str(e)}
        return {"ok": True, "feature": feature, "snapshot": snapshot}

    @app.get("/api/insights", tags=["insight"])
    def api_insights(
        limit_samples: int = Query(60),
        history: int = Query(3),
        runtime: int = Query(0),
    ) -> JSONResponse:
        CW, load_config, get_runtime = _env()
        STATS       = getattr(CW, "STATS", None)
        REPORT_DIR  = getattr(CW, "REPORT_DIR", None)
        CACHE_DIR   = getattr(CW, "CACHE_DIR", None)
        _parse_epoch: Callable[[Any], int] = getattr(CW, "_parse_epoch", lambda *_: 0)
        _load_wall_snapshot = getattr(CW, "_load_wall_snapshot", lambda: [])
        _get_orchestrator   = getattr(CW, "_get_orchestrator", None)
        _append_log         = getattr(CW, "_append_log", lambda *a, **k: None)
        _compute_lanes_impl = getattr(CW, "_compute_lanes_from_stats", None)

        # Decorate event titles for the UI
        def _format_event_title(e: Dict[str, Any]) -> Dict[str, Any]:
            out = dict(e)
            t = str(e.get("type") or "").lower()

            if t == "movie":
                title = (e.get("title") or e.get("name") or "").strip()
                year = e.get("year")
                if title:
                    out["display_title"] = f"{title} ({year})" if year else title
                else:
                    out["display_title"] = "Movie"

            elif t == "episode":
                series_title = (e.get("series_title") or e.get("show_title") or "").strip()
                season = e.get("season")
                episode = e.get("episode")
                ep_title = (e.get("title") or e.get("episode_title") or "").strip()

                if series_title and isinstance(season, int) and isinstance(episode, int):
                    out["display_title"] = f"{series_title} S{int(season):02d}E{int(episode):02d}"
                elif series_title:
                    out["display_title"] = series_title
                else:
                    out["display_title"] = "Episode"

                if ep_title and ep_title.lower() != series_title.lower():
                    out["display_subtitle"] = ep_title

            else:
                title = (e.get("title") or e.get("name") or "").strip()
                out["display_title"] = title or "Item"

            return out

        # Feature keys are dynamic; keep watchlist first
        base_feats = ("watchlist", "ratings", "history", "playlists")
        def _features_from(obj) -> List[str]:
            keys = []
            try:
                if isinstance(obj, dict):
                    if isinstance(obj.get("features"), dict): keys += obj["features"].keys()
                    if isinstance(obj.get("stats"), dict):    keys += obj["stats"].keys()
            except Exception: pass
            ks = [k for k in dict.fromkeys([*(k for k in keys if k), *base_feats])]
            if "watchlist" in ks:
                ks = ["watchlist"] + [k for k in ks if k != "watchlist"]
            return ks or list(base_feats)
        feature_keys = _features_from(getattr(STATS, "data", {}) or {})

        def _safe_parse_epoch(v) -> int:
            try: return int(_parse_epoch(v) or 0)
            except Exception: return 0

        def _zero_lane() -> Dict[str, Any]:
            return {"added": 0, "removed": 0, "updated": 0,
                    "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []}

        def _empty_feats() -> Dict[str, Dict[str, Any]]:
            return {k: _zero_lane() for k in feature_keys}

        def _empty_enabled() -> Dict[str, bool]:
            return {k: False for k in feature_keys}

        def _safe_compute_lanes(since: int, until: int) -> Tuple[Dict[str, Dict[str, Any]], Dict[str, bool]]:
            try:
                if callable(_compute_lanes_impl):
                    feats, enabled = _compute_lanes_impl(int(since or 0), int(until or 0))
                    feats   = feats   if isinstance(feats, dict)   else _empty_feats()
                    enabled = enabled if isinstance(enabled, dict) else _empty_enabled()
                    for k in feature_keys:
                        feats.setdefault(k, _zero_lane())
                        enabled.setdefault(k, False)
                    return feats, enabled
            except Exception as e:
                _append_log("INSIGHTS", f"[!] _compute_lanes_from_stats failed: {e}")
            return _empty_feats(), _empty_enabled()

        # Stats → series/events/http
        series: List[Dict[str, int]] = []
        generated_at: Optional[str] = None
        events: List[Dict[str, Any]] = []
        http_block: Dict[str, Any] = {}

        if STATS is not None:
            lock = getattr(STATS, "lock", None) or nullcontext()
            try:
                with lock:
                    data = STATS.data or {}
                samples = list((data or {}).get("samples") or [])
                events  = [e for e in list((data or {}).get("events") or []) if not str(e.get("key", "")).startswith("agg:")]
                events = [_format_event_title(e) for e in events]
                http_block = dict((data or {}).get("http") or {})
                generated_at = (data or {}).get("generated_at")
                samples.sort(key=lambda r: int(r.get("ts") or 0))
                if int(limit_samples) > 0:
                    samples = samples[-int(limit_samples):]
                series = [{"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)} for r in samples]
            except Exception as e:
                _append_log("INSIGHTS", f"[!] samples load failed: {e}")
                series, events, http_block = [], [], {}

        series_by_feature: Dict[str, List[Dict[str, int]]] = {k: [] for k in feature_keys}
        series_by_feature["watchlist"] = list(series)

        # Recent syncs
        rows: List[Dict[str, Any]] = []
        try:
            files = []
            if REPORT_DIR is not None:
                files = sorted(REPORT_DIR.glob("sync-*.json"),
                               key=lambda p: p.stat().st_mtime, reverse=True)[:max(1, int(history))]
            for p in files:
                try:
                    d = json.loads(p.read_text(encoding="utf-8"))
                    lanes_in = (d.get("features") or {})
                    lanes = {k: (lanes_in.get(k) or _zero_lane()) for k in feature_keys}
                    since = _safe_parse_epoch(d.get("raw_started_ts") or d.get("started_at"))
                    until = _safe_parse_epoch(d.get("finished_at")) or int(p.stat().st_mtime)

                    stats_feats, stats_enabled = _safe_compute_lanes(since, until)
                    for name in feature_keys:
                        lane = lanes.get(name)
                        if not isinstance(lane, dict) or all((lane.get(x) or 0) == 0 for x in ("added","removed","updated")):
                            lanes[name] = stats_feats.get(name) or _zero_lane()

                    enabled = d.get("features_enabled") or d.get("enabled") or {}
                    if not isinstance(enabled, dict):
                        enabled = dict(stats_enabled)

                    provider_posts = {k[:-5]: v for k, v in d.items()
                                      if isinstance(k, str) and k.endswith("_post")}

                    rows.append({
                        "started_at":   d.get("started_at"),
                        "finished_at":  d.get("finished_at"),
                        "duration_sec": d.get("duration_sec"),
                        "result":       d.get("result") or "",
                        "exit_code":    d.get("exit_code"),
                        "added":        int(d.get("added_last") or 0),
                        "removed":      int(d.get("removed_last") or 0),
                        "features":         lanes,
                        "features_enabled": enabled,
                        "updated_total":    int(d.get("updated_last") or 0),
                        "provider_posts":   provider_posts,
                        "plex_post":     d.get("plex_post"),
                        "simkl_post":    d.get("simkl_post"),
                        "trakt_post":    d.get("trakt_post"),
                        "jellyfin_post": d.get("jellyfin_post"),
                        "emby_post":     d.get("emby_post"),
                        "mdblist_post":  d.get("mdblist_post"),
                        "crosswatch_post": d.get("crosswatch_post"),
                    })
                except Exception as e:
                    _append_log("INSIGHTS", f"[!] report parse failed {p.name}: {e}")
        except Exception as e:
            _append_log("INSIGHTS", f"[!] report scan failed: {e}")

        # Watchtime (approx; optional TMDB refinement)
        wall = _load_wall_snapshot()
        state = None
        if not wall and callable(_get_orchestrator):
            try:
                orc = _get_orchestrator()
                state = orc.files.load_state()
                if isinstance(state, dict):
                    wall = list(state.get("wall") or [])
            except Exception as e:
                _append_log("SYNC", f"[!] insights: orchestrator init failed: {e}")
                wall = []
        
        cfg = load_config() or {}
        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
        use_tmdb = bool(api_key) and bool(int(runtime)) and CACHE_DIR is not None

        def _build_crosswatch_snapshot_info() -> Dict[str, Any]:
            info: Dict[str, Any] = {}
            try:
                cw_cfg = (cfg.get("crosswatch") or cfg.get("CrossWatch") or {}) or {}
                root_dir = str(cw_cfg.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
                snap_dir = Path(root_dir).joinpath("snapshots")

                selected = {
                    "watchlist": str(cw_cfg.get("restore_watchlist") or "latest").strip() or "latest",
                    "history":   str(cw_cfg.get("restore_history") or "latest").strip() or "latest",
                    "ratings":   str(cw_cfg.get("restore_ratings") or "latest").strip() or "latest",
                }

                files: List[Path] = []
                if snap_dir.is_dir():
                    files = list(snap_dir.glob("*.json"))

                by_feat: Dict[str, List[str]] = {"watchlist": [], "history": [], "ratings": []}
                for p in files:
                    name = p.name
                    for feat in by_feat.keys():
                        if name.endswith(f"-{feat}.json"):
                            by_feat[feat].append(name)

                for feat, arr in by_feat.items():
                    arr.sort()
                    sel = selected.get(feat, "latest")
                    actual: Optional[str] = None
                    if arr:
                        if sel == "latest":
                            actual = arr[-1]
                        elif sel in arr:
                            actual = sel
                        else:
                            actual = arr[-1]

                    human: Optional[str] = None
                    iso_ts: Optional[str] = None
                    if actual:
                        try:
                            stem = actual.split("-", 1)[0]
                            dt = _dt.datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(tzinfo=_dt.timezone.utc)
                            iso_ts = dt.isoformat()
                            human = dt.strftime("%d-%b-%y")
                        except Exception:
                            pass

                    info[feat] = {
                        "selected": sel,
                        "actual": actual,
                        "human": human,
                        "ts": iso_ts,
                        "has_snapshots": bool(arr),
                    }
            except Exception:
                pass
            return info

        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
        use_tmdb = bool(api_key) and bool(int(runtime)) and CACHE_DIR is not None

        def _try_runtime_both(api_key: str, typ: str, tmdb_id: int):
            for t in (typ, ("movie" if typ == "tv" else "tv")):
                try:
                    m = get_runtime(api_key, t, int(tmdb_id), CACHE_DIR)
                    if m is not None: return m
                except Exception: pass
            return None

        movies = shows = total_min = tmdb_hits = tmdb_misses = fetched = 0
        fetch_cap = 50 if use_tmdb else 0

        for meta in wall:
            if not isinstance(meta, dict): continue
            typ = "movie" if str((meta.get("type") or "")).lower() == "movie" else "tv"
            movies += (typ == "movie"); shows += (typ != "movie")
            minutes = None
            tmdb_id = (meta.get("ids") or {}).get("tmdb")
            if use_tmdb and tmdb_id and fetched < fetch_cap:
                try:
                    minutes = _try_runtime_both(api_key, typ, int(str(tmdb_id)))
                except Exception:
                    minutes = None
                fetched += 1
                tmdb_hits += (minutes is not None)
                tmdb_misses += (minutes is None)
            if minutes is None: minutes = 115 if typ == "movie" else 45
            total_min += int(minutes)

        watchtime = {
            "movies": int(movies), "shows": int(shows),
            "minutes": total_min, "hours": round(total_min/60, 1),
            "days": round(total_min/1440, 1),
            "method": "tmdb" if tmdb_hits and not tmdb_misses else ("mixed" if tmdb_hits else "estimate"),
        }

        if state is None and callable(_get_orchestrator):
            try:
                orc = _get_orchestrator()
                state = orc.files.load_state()
            except Exception:
                state = None

        prov_block: dict = (state or {}).get("providers") or {}
        providers_set: set[str] = {str(k).strip().lower() for k in prov_block.keys() if isinstance(k, str)}

        active: Dict[str, bool] = {k: False for k in providers_set}
        try:
            cfg_pairs = (cfg.get("pairs") or cfg.get("connections") or []) or []
            for p in cfg_pairs:
                s = str(p.get("source") or "").strip().lower()
                t = str(p.get("target") or "").strip().lower()
                if s in active: active[s] = True
                if t in active: active[t] = True
        except Exception:
            pass

        def _count_items(node) -> int:
            try:
                if isinstance(node, dict):
                    base = (node.get("baseline") or {})
                    chk  = (node.get("checkpoint") or {})
                    pres = (node.get("present") or {})
                    for cand in (chk.get("items"), base.get("items"), pres.get("items"), node.get("items")):
                        if isinstance(cand, dict):  return len(cand)
                        if isinstance(cand, list):  return len(cand)
                        if isinstance(cand, (int, str)):
                            try: return int(cand)
                            except Exception: return 0
                    return 0
                if isinstance(node, list):       return len(node)
                if isinstance(node, (int, str)): return int(node)
            except Exception:
                return 0
            return 0

        providers_by_feature: Dict[str, Dict[str, int]] = {feat: {k: 0 for k in providers_set} for feat in feature_keys}
        try:
            for prov_upper, pdata in (prov_block or {}).items():
                key = str(prov_upper or "").strip().lower()
                for feat in feature_keys:
                    providers_by_feature[feat][key] = _count_items((pdata or {}).get(feat) or {})
        except Exception:
            pass

        now_ts = int(time.time())
        week_floor  = now_ts - 7 * 86400
        month_floor = now_ts - 30 * 86400

        def _last_run_lane(feat: str) -> Tuple[int, int, int]:
            for row in rows:
                try:
                    en = row.get("features_enabled") or {}
                    if en.get(feat) is False: continue
                    lane = (row.get("features") or {}).get(feat) or {}
                    return int(lane.get("added") or 0), int(lane.get("removed") or 0), int(lane.get("updated") or 0)
                except Exception:
                    continue
            return 0, 0, 0

        def _union_now(feat: str) -> int:
            counts = providers_by_feature.get(feat) or {}
            return max(counts.values()) if counts else 0

        def _lane_totals(days: int) -> Dict[str, Tuple[int, int, int]]:
            feats, _ = _safe_compute_lanes(now_ts - days * 86400, now_ts)
            out = {}
            for f in feature_keys:
                lane = feats.get(f) or {}
                out[f] = (int(lane.get("added") or 0), int(lane.get("removed") or 0), int(lane.get("updated") or 0))
            return out

        week_tot  = _lane_totals(7)
        month_tot = _lane_totals(30)

        ts_grid = [r["ts"] for r in series_by_feature.get("watchlist", [])]
        if len(ts_grid) < 2:
            base = now_ts - 11 * 3600
            ts_grid = [base + i * 3600 for i in range(12)]
        if ts_grid[-1] < now_ts:
            ts_grid = ts_grid + [now_ts]

        win = []
        for i in range(len(ts_grid) - 1):
            feats, _ = _safe_compute_lanes(ts_grid[i], ts_grid[i + 1])
            d: Dict[str, Tuple[int, int]] = {}
            for f in feature_keys:
                ln = feats.get(f) or {}
                d[f] = (int(ln.get("added") or 0), int(ln.get("removed") or 0))
            win.append(d)

        for f in [x for x in feature_keys if x != "watchlist"]:
            v = max(0, _union_now(f))
            out = [{"ts": ts_grid[-1], "count": v}]
            for i in range(len(ts_grid) - 2, -1, -1):
                a, r = win[i].get(f, (0, 0))
                v = max(0, v - (a - r))
                out.append({"ts": ts_grid[i], "count": v})
            series_by_feature[f] = list(reversed(out))

        def _val_at(series_list: List[Dict[str, int]], floor_ts: int) -> int:
            try:
                arr = sorted(series_list or [], key=lambda r: int(r.get("ts") or 0))
                if not arr: return 0
                val = int(arr[0].get("count") or 0)
                for r in arr:
                    t = int(r.get("ts") or 0)
                    if t <= floor_ts: val = int(r.get("count") or 0)
                    else: break
                return val
            except Exception:
                return 0

        feats_out: Dict[str, Dict[str, Any]] = {}
        for feat in feature_keys:
            add_last, rem_last, upd_last = _last_run_lane(feat)
            wa, wr, wu = week_tot.get(feat, (0, 0, 0))
            if not (add_last or rem_last or upd_last):
                add_last, rem_last, upd_last = wa, wr, wu
            s = series_by_feature.get(feat, [])
            feats_out[feat] = {
                "now":     _union_now(feat),
                "week":    _val_at(s, week_floor),
                "month":   _val_at(s, month_floor),
                "added":   add_last,
                "removed": rem_last,
                "updated": upd_last,
                "series":  s,
                "providers": providers_by_feature.get(feat, {}),
                "providers_active": active.copy(),
            }

        wl = feats_out.get("watchlist", {"now":0,"week":0,"month":0,"added":0,"removed":0})
        cw_snapshots = _build_crosswatch_snapshot_info()
        payload: Dict[str, Any] = {
            "series":               series_by_feature.get("watchlist", []),
            "series_by_feature":    series_by_feature,
            "history":              rows,
            "watchtime":            watchtime,
            "providers":            feats_out.get("watchlist", {}).get("providers", {}),
            "providers_by_feature": providers_by_feature,
            "providers_active":     active,
            "events":               events,
            "http":                 http_block,
            "generated_at":         generated_at,
            "features":             feats_out,
            "crosswatch_snapshots": cw_snapshots,
            "now": int(wl["now"]), "week": int(wl["week"]), "month": int(wl["month"]),
            "added": int(wl["added"]), "removed": int(wl["removed"]),
        }
        return JSONResponse(payload)
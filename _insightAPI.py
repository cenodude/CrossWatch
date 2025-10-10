from __future__ import annotations
import json, time
from typing import Any, Dict, List, Tuple
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

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
        try:
            with STATS.lock:
                return JSONResponse(json.loads(json.dumps(STATS.data)))
        except Exception:
            return JSONResponse({})

    @app.get("/api/stats", tags=["insight"])
    def api_stats() -> Dict[str, Any]:
        CW, _, _ = _env()
        STATS       = getattr(CW, "STATS", None)
        _load_state = getattr(CW, "_load_state", lambda: None)
        Stats       = getattr(CW, "Stats", None)
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
            if (not base.get("now")) and state and Stats and hasattr(Stats, "_build_union_map"):
                base["now"] = len(Stats._build_union_map(state))
        except Exception:
            pass
        return {"ok": True, **base}

    @app.get("/api/insights", tags=["insight"])
    def api_insights(
        limit_samples: int = Query(60),
        history: int = Query(3),
        runtime: int = Query(0),
    ) -> JSONResponse:
        CW, load_config, get_runtime = _env()
        STATS      = getattr(CW, "STATS", None)
        REPORT_DIR = getattr(CW, "REPORT_DIR", None)
        CACHE_DIR  = getattr(CW, "CACHE_DIR", None)
        _parse_epoch        = getattr(CW, "_parse_epoch", lambda *_: 0)
        _load_wall_snapshot = getattr(CW, "_load_wall_snapshot", lambda: [])
        _get_orchestrator   = getattr(CW, "_get_orchestrator", None)
        _append_log         = getattr(CW, "_append_log", lambda *a, **k: None)
        _compute_lanes_impl = getattr(CW, "_compute_lanes_from_stats", None)
        feature_keys = ("watchlist","ratings","history","playlists")

        def _safe_parse_epoch(v) -> int:
            try: return int(_parse_epoch(v) or 0)
            except Exception: return 0

        def _zero_lane() -> Dict[str, Any]:
            return {"added":0,"removed":0,"updated":0,"spotlight_add":[],"spotlight_remove":[],"spotlight_update":[]}

        def _empty_feats() -> Dict[str, Dict[str, Any]]:
            return {k:_zero_lane() for k in feature_keys}

        def _empty_enabled() -> Dict[str, bool]:
            return {k:False for k in feature_keys}

        def _safe_compute_lanes(since: int, until: int):
            try:
                if callable(_compute_lanes_impl):
                    feats, enabled = _compute_lanes_impl(int(since or 0), int(until or 0))
                    if not isinstance(feats, dict):   feats   = _empty_feats()
                    if not isinstance(enabled, dict): enabled = _empty_enabled()
                    for k in feature_keys:
                        feats.setdefault(k, _zero_lane()); enabled.setdefault(k, False)
                    return feats, enabled
            except Exception as e:
                _append_log("INSIGHTS", f"[!] _compute_lanes_from_stats failed: {e}")
            return _empty_feats(), _empty_enabled()

        series: List[Dict[str, int]] = []
        generated_at = None
        events: List[Dict[str, Any]] = []
        http_block: Dict[str, Any] = {}
        if STATS is not None:
            try:
                with getattr(STATS, "lock", None):
                    data = STATS.data or {}
                samples = list((data or {}).get("samples") or [])
                events  = [e for e in list((data or {}).get("events") or []) if not str(e.get("key","")).startswith("agg:")]
                http_block = dict((data or {}).get("http") or {})
                generated_at = (data or {}).get("generated_at")
                samples.sort(key=lambda r: int(r.get("ts") or 0))
                if int(limit_samples) > 0:
                    samples = samples[-int(limit_samples):]
                series = [{"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)} for r in samples]
            except Exception as e:
                _append_log("INSIGHTS", f"[!] samples failed: {e}")
                series, events, http_block = [], [], {}

        series_by_feature: Dict[str, List[Dict[str,int]]] = {k: [] for k in feature_keys}
        series_by_feature["watchlist"] = list(series)

        rows: List[Dict[str, Any]] = []
        try:
            files = []
            if REPORT_DIR is not None:
                files = sorted(REPORT_DIR.glob("sync-*.json"), key=lambda p: p.stat().st_mtime, reverse=True)[:max(1,int(history))]
            for p in files:
                try:
                    d = json.loads(p.read_text(encoding="utf-8"))
                    feats_in = d.get("features") or {}
                    lanes = {k:(feats_in.get(k) or _zero_lane()) for k in feature_keys}
                    since = _safe_parse_epoch(d.get("raw_started_ts") or d.get("started_at"))
                    until = _safe_parse_epoch(d.get("finished_at")) or int(p.stat().st_mtime)
                    stats_feats, stats_enabled = _safe_compute_lanes(since, until)
                    for name in feature_keys:
                        lane = lanes.get(name)
                        if not isinstance(lane, dict) or all((lane.get(x) or 0) == 0 for x in ("added","removed","updated")):
                            lanes[name] = stats_feats.get(name) or _zero_lane()
                    enabled = d.get("features_enabled") or d.get("enabled")
                    if not isinstance(enabled, dict):
                        enabled = dict(stats_enabled)
                    provider_posts = {k[:-5]: v for k, v in d.items() if isinstance(k,str) and k.endswith("_post")}
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
                    })
                except Exception as e:
                    _append_log("INSIGHTS", f"[!] report parse failed {p.name}: {e}")
                    continue
        except Exception as e:
            _append_log("INSIGHTS", f"[!] report scan failed: {e}")

        wall = _load_wall_snapshot()
        state = None
        if not wall and callable(_get_orchestrator):
            try:
                orc = _get_orchestrator(); state = orc.files.load_state()
                if isinstance(state, dict): wall = list(state.get("wall") or [])
            except Exception as e:
                _append_log("SYNC", f"[!] insights: orchestrator init failed: {e}")
                wall = []

        cfg = load_config()
        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
        use_tmdb = bool(api_key) and bool(int(runtime)) and CACHE_DIR is not None

        def _try_runtime_both(api_key: str, typ: str, tmdb_id: int):
            for t in (typ, ("movie" if typ == "tv" else "tv")):
                try:
                    m = get_runtime(api_key, t, int(tmdb_id), CACHE_DIR)
                    if m is not None: return m
                except Exception:
                    pass
            return None

        movies = shows = 0
        total_min = 0
        tmdb_hits = tmdb_misses = 0
        fetch_cap = 50 if use_tmdb else 0
        fetched = 0
        for meta in wall:
            if not isinstance(meta, dict): continue
            typ = "movie" if str((meta.get("type") or "")).lower() == "movie" else "tv"
            if typ == "movie": movies += 1
            else:              shows  += 1
            minutes = None
            ids = meta.get("ids") or {}
            tmdb_id = ids.get("tmdb")
            if use_tmdb and tmdb_id and fetched < fetch_cap:
                try:
                    tid = int(str(tmdb_id)); minutes = _try_runtime_both(api_key, typ, tid)
                except Exception:
                    minutes = None
                fetched += 1
                if minutes is not None: tmdb_hits += 1
                else:                   tmdb_misses += 1
            if minutes is None: minutes = 115 if typ == "movie" else 45
            total_min += int(minutes)

        method = "tmdb" if tmdb_hits and not tmdb_misses else ("mixed" if tmdb_hits else "fallback")
        watchtime = {"movies":movies,"shows":shows,"minutes":total_min,"hours":round(total_min/60,1),"days":round(total_min/1440,1),"method":method}

        try:
            if state is None and callable(_get_orchestrator):
                orc = _get_orchestrator(); state = orc.files.load_state()
        except Exception:
            state = None

        prov_block: dict = (state or {}).get("providers") or {}
        providers_set: set[str] = set(k.strip().lower() for k in prov_block.keys() if isinstance(k,str))
        if not providers_set: providers_set = {"plex","simkl","trakt","jellyfin"}

        active: dict[str, bool] = {k: False for k in providers_set}
        try:
            pairs3 = (load_config() or {}).get("pairs") or (load_config() or {}).get("connections") or []
            for p in pairs3:
                s = str(p.get("source") or "").strip().lower(); t = str(p.get("target") or "").strip().lower()
                if s in active: active[s] = True
                if t in active: active[t] = True
        except Exception:
            pass

        def _count_items(v) -> int:
            items = ((((v or {}).get("baseline") or {}).get("items") or {}))
            return int(len(items)) if isinstance(items, dict) else int(len(items or []))

        providers_by_feature: dict[str, dict[str, int]] = {feat:{k:0 for k in providers_set} for feat in feature_keys}
        try:
            for upcase, pdata in (prov_block or {}).items():
                key = str(upcase or "").strip().lower()
                for feat in feature_keys:
                    providers_by_feature[feat][key] = _count_items(((pdata or {}).get(feat) or {}))
        except Exception:
            pass

        now_ts = int(time.time())

        def _last_run_lane(feat: str) -> Tuple[int,int]:
            for row in rows:
                try:
                    en = row.get("features_enabled") or {}
                    if en.get(feat) is False: continue
                    lane = (row.get("features") or {}).get(feat) or {}
                    return int(lane.get("added") or 0), int(lane.get("removed") or 0)
                except Exception:
                    continue
            return 0, 0

        def _union_now(feat: str) -> int:
            counts = providers_by_feature.get(feat) or {}
            return max(counts.values()) if counts else 0

        def _lane_totals(days: int) -> Dict[str, Tuple[int,int,int]]:
            feats, _ = _safe_compute_lanes(now_ts - days*86400, now_ts)
            out = {}
            for f in feature_keys:
                lane = feats.get(f) or {}
                out[f] = (int(lane.get("added") or 0), int(lane.get("removed") or 0), int(lane.get("updated") or 0))
            return out

        w = _lane_totals(7); m = _lane_totals(30)

        # sparklines for ratings/history/playlists
        ts_grid = [r["ts"] for r in series_by_feature["watchlist"]]
        if len(ts_grid) < 2:
            base = now_ts - 11*3600
            ts_grid = [base + i*3600 for i in range(12)]
        if ts_grid[-1] < now_ts:
            ts_grid = ts_grid + [now_ts]
        win = []
        for i in range(len(ts_grid)-1):
            feats, _ = _safe_compute_lanes(ts_grid[i], ts_grid[i+1])
            d = {}
            for f in feature_keys:
                ln = feats.get(f) or {}
                d[f] = (int(ln.get("added") or 0), int(ln.get("removed") or 0))
            win.append(d)
        for f in ("ratings","history","playlists"):
            v = max(0, _union_now(f))
            out = [{"ts": ts_grid[-1], "count": v}]
            for i in range(len(ts_grid)-2, -1, -1):
                a, r = win[i].get(f, (0,0))
                v = max(0, v - (a - r))
                out.append({"ts": ts_grid[i], "count": v})
            series_by_feature[f] = list(reversed(out))

        feats_out: Dict[str, Dict[str, Any]] = {}
        for feat in feature_keys:
            add_last, rem_last = _last_run_lane(feat)
            wa, wr, wu = (w.get(feat) or (0,0,0))
            ma, mr, mu = (m.get(feat) or (0,0,0))
            if not (add_last or rem_last):
                add_last, rem_last = wa, wr
            feats_out[feat] = {
                "now":   _union_now(feat),
                "week":  wa + wr + wu,
                "month": ma + mr + mu,
                "added": add_last,
                "removed": rem_last,
                "updated": wu,
                "series": series_by_feature.get(feat, []),
                "providers": providers_by_feature.get(feat, {}),
                "providers_active": active.copy(),
            }

        wl = feats_out["watchlist"]
        payload: Dict[str, Any] = {
            "series":               series_by_feature["watchlist"],
            "series_by_feature":    series_by_feature,
            "history":              rows,
            "watchtime":            watchtime,
            "providers":            feats_out["watchlist"]["providers"],
            "providers_by_feature": providers_by_feature,
            "providers_active":     active,
            "events":               events,
            "http":                 http_block,
            "generated_at":         generated_at,
            "features":             feats_out,
            "now":    wl["now"],
            "week":   wl["week"],
            "month":  wl["month"],
            "added":  wl["added"],
            "removed":wl["removed"],
        }
        return JSONResponse(payload)

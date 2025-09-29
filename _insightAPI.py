# _insightAPI.py
from __future__ import annotations
import json, time
from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

# lazy env to avoid circular imports
def _env():
    try:
        import crosswatch as CW
        from cw_platform.config_base import load_config as _load_cfg
        from _metaAPI import get_runtime as _get_runtime
        return CW, _load_cfg, _get_runtime
    except Exception:
        return None, (lambda: {}), (lambda *a, **k: None)

def register_insights(app: FastAPI):
    @app.get("/api/insights", tags=["insight"])
    def api_insights(limit_samples: int = Query(60), history: int = Query(3), runtime: int = Query(0)) -> JSONResponse:
        """Compact insights payload; TMDb runtime optional."""

        CW, load_config, get_runtime = _env()

        STATS        = getattr(CW, "STATS", None)
        REPORT_DIR   = getattr(CW, "REPORT_DIR", None)
        CACHE_DIR    = getattr(CW, "CACHE_DIR", None)

        _parse_epoch              = getattr(CW, "_parse_epoch", lambda *_: 0)
        _compute_lanes_from_stats = getattr(CW, "_compute_lanes_from_stats", lambda *_: ({}, {}))
        _lane_is_empty            = getattr(CW, "_lane_is_empty", lambda *_: True)
        _load_wall_snapshot       = getattr(CW, "_load_wall_snapshot", lambda : [])
        _get_orchestrator         = getattr(CW, "_get_orchestrator", None)
        _append_log               = getattr(CW, "_append_log", lambda *a, **k: None)

        # Samples
        series = []
        if STATS is not None:
            try:
                with STATS.lock:
                    samples = list(STATS.data.get("samples") or [])
                samples.sort(key=lambda r: int(r.get("ts") or 0))
                if int(limit_samples) > 0:
                    samples = samples[-int(limit_samples):]
                series = [{"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)} for r in samples]
            except Exception:
                series = []

        # Recent sync history
        rows = []
        try:
            files = []
            if REPORT_DIR is not None:
                files = sorted(
                    REPORT_DIR.glob("sync-*.json"),
                    key=lambda p: p.stat().st_mtime,
                    reverse=True
                )[:max(1, int(history))]

            def zero_lane():
                return {"added": 0, "removed": 0, "updated": 0,
                        "spotlight_add": [], "spotlight_remove": [], "spotlight_update": []}

            for p in files:
                try:
                    d = json.loads(p.read_text(encoding="utf-8"))

                    feats_in = d.get("features") or {}
                    lanes = {
                        "watchlist": feats_in.get("watchlist") or zero_lane(),
                        "ratings":   feats_in.get("ratings")   or zero_lane(),
                        "history":   feats_in.get("history")   or zero_lane(),
                        "playlists": feats_in.get("playlists") or zero_lane(),
                    }

                    since = _parse_epoch(d.get("raw_started_ts") or d.get("started_at"))
                    until = _parse_epoch(d.get("finished_at")) or int(p.stat().st_mtime)

                    stats_feats, stats_enabled = _compute_lanes_from_stats(since, until)
                    for name in ("watchlist", "ratings", "history", "playlists"):
                        lane = lanes.get(name)
                        if name not in lanes or _lane_is_empty(lane):
                            lanes[name] = stats_feats.get(name) or zero_lane()

                    enabled = d.get("features_enabled") or d.get("enabled")
                    if not isinstance(enabled, dict):
                        enabled = dict(stats_enabled)

                    added_total   = d.get("added_last")
                    removed_total = d.get("removed_last")
                    updated_total = d.get("updated_last")
                    if added_total is None or removed_total is None or updated_total is None:
                        a = r = u = 0
                        for k, lane in lanes.items():
                            if enabled.get(k) is False:
                                continue
                            a += int((lane or {}).get("added") or 0)
                            r += int((lane or {}).get("removed") or 0)
                            u += int((lane or {}).get("updated") or 0)
                        if added_total   is None: added_total   = a
                        if removed_total is None: removed_total = r
                        if updated_total is None: updated_total = u

                    provider_posts = {}
                    for k, v in d.items():
                        if isinstance(k, str) and k.endswith("_post"):
                            provider_posts[k[:-5]] = v

                    rows.append({
                        "started_at":   d.get("started_at"),
                        "finished_at":  d.get("finished_at"),
                        "duration_sec": d.get("duration_sec"),
                        "result":       d.get("result") or "",
                        "exit_code":    d.get("exit_code"),
                        "added":        int(added_total or 0),
                        "removed":      int(removed_total or 0),
                        "features":         lanes,
                        "features_enabled": enabled,
                        "updated_total":    int(updated_total or 0),
                        "provider_posts": provider_posts,
                        "plex_post":     d.get("plex_post"),
                        "simkl_post":    d.get("simkl_post"),
                        "trakt_post":    d.get("trakt_post"),
                        "jellyfin_post": d.get("jellyfin_post"),
                    })
                except Exception:
                    continue
        except Exception:
            pass

        # Watchtime
        wall = _load_wall_snapshot()
        state = None
        if not wall and callable(_get_orchestrator):
            try:
                orc = _get_orchestrator()
                state = orc.files.load_state()
                if isinstance(state, dict):
                    wall = list(state.get("wall") or [])
                if not wall:
                    snaps = orc.build_snapshots(feature="watchlist")
                    for idx in (snaps or {}).values():
                        wall.extend(list(idx.values()))
            except Exception as e:
                _append_log("SYNC", f"[!] insights: orchestrator init failed: {e}")
                wall = []

        cfg = load_config()
        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
        use_tmdb = bool(api_key) and bool(int(runtime)) and CACHE_DIR is not None

        def _try_runtime_both(api_key: str, typ: str, tmdb_id: int):
            # Try declared type, then the other
            for t in (typ, ("movie" if typ == "tv" else "tv")):
                try:
                    m = get_runtime(api_key, t, int(tmdb_id), CACHE_DIR)
                    if m is not None:
                        return m
                except Exception:
                    pass
            return None

        movies = shows = 0
        total_min = 0
        tmdb_hits = tmdb_misses = 0
        fetch_cap = 50 if use_tmdb else 0
        fetched = 0

        for meta in wall:
            typ = "movie" if (str(meta.get("type") or "").lower() == "movie") else "tv"
            if typ == "movie": movies += 1
            else:              shows  += 1

            minutes = None
            tmdb_id = (meta.get("ids") or {}).get("tmdb")

            if use_tmdb and tmdb_id and fetched < fetch_cap:
                try:
                    tid = int(str(tmdb_id))
                    minutes = _try_runtime_both(api_key, typ, tid)
                except Exception:
                    minutes = None
                fetched += 1
                if minutes is not None: tmdb_hits += 1
                else:                   tmdb_misses += 1

            if minutes is None:
                minutes = 115 if typ == "movie" else 45

            total_min += int(minutes)

        method = "tmdb" if tmdb_hits and not tmdb_misses else ("mixed" if tmdb_hits else "fallback")
        watchtime = {
            "movies":  movies,
            "shows":   shows,
            "minutes": total_min,
            "hours":   round(total_min / 60, 1),
            "days":    round(total_min / 1440, 1),
            "method":  method,
        }

        # Provider universe
        providers_set: set[str] = set()
        prov_block: dict = {}
        try:
            if state is None and callable(_get_orchestrator):
                try:
                    orc = _get_orchestrator()
                    state = orc.files.load_state()
                except Exception:
                    state = None
            prov_block = (state or {}).get("providers") or {}
            for up in prov_block.keys():
                if isinstance(up, str):
                    providers_set.add(up.strip().lower())

            cfg2 = load_config() or {}
            pairs = (cfg2.get("pairs") or cfg2.get("connections") or []) or []
            for p in pairs:
                s = str(p.get("source") or "").strip().lower()
                t = str(p.get("target") or "").strip().lower()
                if s: providers_set.add(s)
                if t: providers_set.add(t)
        except Exception:
            pass

        if not providers_set:
            providers_set = {"plex", "simkl", "trakt", "jellyfin"}

        # Active map
        active: dict[str, bool] = {k: False for k in providers_set}
        try:
            cfg3 = load_config() or {}
            pairs3 = (cfg3.get("pairs") or cfg3.get("connections") or []) or []
            for p in pairs3:
                s = str(p.get("source") or "").strip().lower()
                t = str(p.get("target") or "").strip().lower()
                if s in active: active[s] = True
                if t in active: active[t] = True
        except Exception:
            pass

        # Totals by feature
        feature_keys = ["watchlist", "ratings", "history", "playlists"]
        providers_by_feature: dict[str, dict[str, int]] = {feat: {k: 0 for k in providers_set} for feat in feature_keys}
        try:
            for upcase, data in (prov_block or {}).items():
                key = str(upcase or "").strip().lower()
                if key not in providers_set:
                    continue
                for feat in feature_keys:
                    items = ((((data or {}).get(feat) or {}).get("baseline") or {}).get("items") or {})
                    providers_by_feature[feat][key] = int(len(items)) if isinstance(items, dict) else int(len(items or []))
        except Exception:
            pass

        # Back-compat top-level providers = watchlist totals
        providers = dict(providers_by_feature.get("watchlist", {}))

        # High-level counters from Stats (best-effort)
        top = {}
        try:
            if STATS is not None and hasattr(STATS, "overview"):
                top = STATS.overview(None) or {}
        except Exception:
            top = {}

        payload = {
            "series":               series,
            "history":              rows,
            "watchtime":            watchtime,
            "providers":            providers,
            "providers_by_feature": providers_by_feature,
            "providers_active":     active,
        }
        for k in ("now", "week", "month", "added", "removed", "new", "del"):
            if k in top:
                payload[k] = top[k]

        return JSONResponse(payload)

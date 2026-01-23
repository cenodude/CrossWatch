# /api/insightAPI.py
# CrossWatch - Insights API for multiple services
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import datetime as _dt
import json
import re
import time
from contextlib import nullcontext
from pathlib import Path
from typing import Any, Callable

from fastapi import FastAPI, Query
from fastapi.responses import JSONResponse

def _env() -> tuple[
    Any | None,
    Callable[[], dict[str, Any]],
    Callable[[dict[str, Any]], None],
    Callable[..., Any],
]:
    try:
        import crosswatch as CW
        from cw_platform.config_base import load_config as _load_cfg, save_config as _save_cfg
        from .metaAPI import get_runtime as _get_runtime
        return CW, _load_cfg, _save_cfg, _get_runtime
    except Exception:
        return None, (lambda: {}), (lambda _cfg: None), (lambda *a, **k: None)


def register_insights(app: FastAPI) -> None:
    @app.get("/api/stats/raw", tags=["insight"])
    def api_stats_raw() -> JSONResponse:
        CW, _, _, _ = _env()
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
    def api_stats() -> dict[str, Any]:
        CW, _, _, _ = _env()
        STATS = getattr(CW, "STATS", None)
        _load_state = getattr(CW, "_load_state", lambda: None)
        StatsClass = getattr(CW, "Stats", None)

        try:
            state = _load_state()
        except Exception:
            state = None

        base: dict[str, Any] = {}
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

    @app.post("/api/crosswatch/select-snapshot", tags=["insight"])
    def api_select_snapshot(
        feature: str = Query(..., pattern="^(watchlist|history|ratings)$"),
        snapshot: str = Query(...),
    ) -> dict[str, Any]:
        _, load_config, save_config, _ = _env()
        try:
            cfg = load_config() or {}
        except Exception:
            cfg = {}
        cw = (cfg.get("crosswatch") or cfg.get("CrossWatch") or {}) or {}
        key = f"restore_{feature}"
        cw[key] = snapshot
        cfg["crosswatch"] = cw

        try:
            save_config(cfg)
        except Exception as e:
            return {"ok": False, "error": str(e)}

        return {"ok": True, "feature": feature, "snapshot": snapshot}

    @app.get("/api/insights", tags=["insight"])
    def api_insights(
        limit_samples: int = Query(60),
        history: int = Query(3),
        runtime: int = Query(0),
    ) -> JSONResponse:
        CW, load_config, _, get_runtime = _env()
        STATS = getattr(CW, "STATS", None)
        REPORT_DIR = getattr(CW, "REPORT_DIR", None)
        CACHE_DIR = getattr(CW, "CACHE_DIR", None)
        _parse_epoch: Callable[[Any], int] = getattr(CW, "_parse_epoch", lambda *_: 0)
        _load_wall_snapshot = getattr(CW, "_load_wall_snapshot", lambda: [])
        _get_orchestrator = getattr(CW, "_get_orchestrator", None)
        _append_log = getattr(CW, "_append_log", lambda *a, **k: None)
        
        _compute_lanes_impl = getattr(CW, "_compute_lanes_from_stats", None)
        _load_state = getattr(CW, "_load_state", lambda: {})
        
        def _series_title_for_event(e: dict[str, Any]) -> str:
            series_title = (
                e.get("series_title")
                or e.get("show_title")
                or ""
            )
            return str(series_title).strip()

        def _format_event_title(e: dict[str, Any]) -> dict[str, Any]:
            out = dict(e)
            t = str(e.get("type") or "").lower()
            raw = (e.get("title") or "").strip()
            key = str(e.get("key") or "").strip()

            def _to_int(v: Any) -> int | None:
                if isinstance(v, int):
                    return v
                if isinstance(v, float) and v.is_integer():
                    return int(v)
                if isinstance(v, str):
                    s = v.strip()
                    if s.isdigit():
                        return int(s)
                return None

            m_key = re.search(r"#s(\d{1,3})e(\d{1,3})", key, flags=re.I)

            if not t:
                if m_key or re.match(r"^s\d{1,3}e\d{1,3}$", raw.lower()):
                    t = "episode"

            if t == "movie":
                title = (e.get("title") or e.get("name") or "").strip()
                year = e.get("year")
                if title:
                    out["display_title"] = f"{title} ({year})" if year else title
                else:
                    out["display_title"] = "Movie"

            elif t == "episode":
                series_title = _series_title_for_event(e)
                season = _to_int(e.get("season"))
                episode = _to_int(e.get("episode"))

                if m_key:
                    season = season if season is not None else int(m_key.group(1))
                    episode = episode if episode is not None else int(m_key.group(2))
                    out.setdefault("season", season)
                    out.setdefault("episode", episode)

                m_raw = re.match(r"^s(\d{1,3})e(\d{1,3})$", raw.lower())
                if m_raw:
                    season = season if season is not None else int(m_raw.group(1))
                    episode = episode if episode is not None else int(m_raw.group(2))
                    out.setdefault("season", season)
                    out.setdefault("episode", episode)

                code = ""
                if season is not None and episode is not None:
                    code = f"S{int(season):02d}E{int(episode):02d}"

                if series_title and code:
                    out["display_title"] = f"{series_title} - {code}"
                elif series_title:
                    out["display_title"] = series_title
                elif code:
                    out["display_title"] = code
                else:
                    out["display_title"] = "Episode"

                ep_title = (e.get("episode_title") or "").strip()
                if ep_title:
                    low = ep_title.lower()
                    if series_title and low == series_title.lower():
                        pass
                    elif code and low == code.lower():
                        pass
                    else:
                        out["display_subtitle"] = ep_title

            elif t == "season":
                series_title = _series_title_for_event(e)
                season_title = (e.get("title") or "").strip()
                season_num = _to_int(e.get("season"))

                if not season_title and season_num is not None:
                    season_title = f"Season {season_num}"

                if series_title and season_title:
                    out["display_title"] = f"{series_title} - {season_title}"
                elif series_title:
                    out["display_title"] = series_title
                elif season_title:
                    out["display_title"] = season_title
                else:
                    out["display_title"] = "Season"

            else:
                title = (e.get("title") or e.get("name") or "").strip()
                out["display_title"] = title or "Item"

            return out


        def _build_show_title_maps(state: dict[str, Any] | None) -> tuple[dict[str, str], dict[str, str]]:
            key_map: dict[str, str] = {}
            id_map: dict[str, str] = {}

            state = state or {}

            provs = (state or {}).get("providers") or {}
            if not isinstance(provs, dict):
                return key_map, id_map

            for _, pdata in provs.items():
                if not isinstance(pdata, dict):
                    continue

                for feat in ("history", "ratings", "watchlist", "playlists"):
                    node = pdata.get(feat)
                    if not isinstance(node, dict):
                        continue

                    baseline = node.get("baseline")
                    base: dict[str, Any] = baseline if isinstance(baseline, dict) else node
                    items = base.get("items")

                    if isinstance(items, dict):
                        iters = items.items()
                    elif isinstance(items, list):
                        iters = ((it.get("key"), it) for it in items if isinstance(it, dict))
                    else:
                        continue

                    for k, it in iters:
                        if not isinstance(it, dict):
                            continue

                        typ = str(it.get("type") or "").lower()
                        title = (it.get("series_title") or it.get("show_title") or "").strip()
                        if not title and typ in ("show", "series", "anime"):
                            title = (it.get("title") or it.get("name") or "").strip()
                        if not title:
                            continue

                        for kk in (k, it.get("key")):
                            if not kk:
                                continue
                            kk0 = str(kk).strip().lower()
                            key_map[kk0] = title
                            if "#" in kk0:
                                key_map[kk0.split("#", 1)[0]] = title

                        raw_show_ids = it.get("show_ids")
                        show_ids = raw_show_ids if isinstance(raw_show_ids, dict) else {}
                        raw_item_ids = it.get("ids")
                        item_ids = raw_item_ids if isinstance(raw_item_ids, dict) else {}
                        for ids in (show_ids, item_ids):
                            if not isinstance(ids, dict):
                                continue
                            for idk in ("tmdb", "tvdb", "simkl", "imdb", "slug"):
                                v = ids.get(idk)
                                if v:
                                    id_map[f"{idk}:{str(v).lower()}"] = title

            return key_map, id_map

        def _key_lookup_candidates(raw_key: Any) -> list[str]:
            k = str(raw_key or "").strip().lower()
            if not k:
                return []

            out: list[str] = []

            def add(x: str) -> None:
                x = str(x or "").strip().lower()
                if x and x not in out:
                    out.append(x)

            add(k)
            if "#" in k:
                base = k.split("#", 1)[0]
                add(base)

            parts = k.split(":")
            if len(parts) >= 3:
                add(f"{parts[0]}:{parts[-1]}")

            if "#" in k:
                parts2 = k.split("#", 1)[0].split(":")
                if len(parts2) >= 3:
                    add(f"{parts2[0]}:{parts2[-1]}")

            return out

        def _enrich_event_from_state(
            e: dict[str, Any],
            key_map: dict[str, str],
            id_map: dict[str, str],
        ) -> dict[str, Any]:
            out = dict(e)
            if out.get("series_title"):
                return out
            if out.get("show_title"):
                out["series_title"] = str(out.get("show_title") or "").strip()
                return out

            for k in _key_lookup_candidates(out.get("key")):
                if k in key_map:
                    out["series_title"] = key_map[k]
                    return out
                if k in id_map:
                    out["series_title"] = id_map[k]
                    return out

            raw_show_ids = out.get("show_ids")
            show_ids = raw_show_ids if isinstance(raw_show_ids, dict) else {}
            raw_item_ids = out.get("ids")
            item_ids = raw_item_ids if isinstance(raw_item_ids, dict) else {}
            for ids in (show_ids, item_ids):
                if not isinstance(ids, dict):
                    continue
                for idk in ("tmdb", "tvdb", "simkl", "imdb", "slug"):
                    v = ids.get(idk)
                    if not v:
                        continue
                    kk = f"{idk}:{str(v).lower()}"
                    if kk in id_map:
                        out["series_title"] = id_map[kk]
                        return out

            return out

        base_feats: tuple[str, ...] = ("watchlist", "ratings", "history", "playlists")

        def _features_from(obj: Any) -> list[str]:
            keys: list[str] = []
            try:
                if isinstance(obj, dict):
                    feats = obj.get("features")
                    if isinstance(feats, dict):
                        keys.extend(str(k) for k in feats.keys())
                    stats = obj.get("stats")
                    if isinstance(stats, dict):
                        keys.extend(str(k) for k in stats.keys())
            except Exception:
                pass

            merged: list[str] = []
            seen: set[str] = set()
            for name in [*keys, *base_feats]:
                if not name:
                    continue
                s = str(name)
                if s not in seen:
                    seen.add(s)
                    merged.append(s)

            if "watchlist" in merged:
                merged = ["watchlist"] + [k for k in merged if k != "watchlist"]

            return merged or list(base_feats)

        feature_keys = _features_from(getattr(STATS, "data", {}) or {})

        def _safe_parse_epoch(v: Any) -> int:
            try:
                return int(_parse_epoch(v) or 0)
            except Exception:
                return 0

        def _as_int(v: Any) -> int:
            try:
                return int(v)
            except Exception:
                return 0

        def _zero_lane() -> dict[str, Any]:
            return {
                "added": 0,
                "removed": 0,
                "updated": 0,
                "spotlight_add": [],
                "spotlight_remove": [],
                "spotlight_update": [],
            }
            

        def _empty_feats() -> dict[str, dict[str, Any]]:
            return {k: _zero_lane() for k in feature_keys}

        def _empty_enabled() -> dict[str, bool]:
            return {k: False for k in feature_keys}
        
        def _is_presence_stub(rec: dict[str, Any]) -> bool:
            if not rec:
                return True
            if set(rec.keys()) <= {"watched"}:
                return True
            if rec.get("watched") is True and not any(
                rec.get(k)
                for k in (
                    "type", "title", "name", "ids", "show_ids", "series_title", "show_title",
                    "season", "episode", "year", "series_year",
                    "watched_at", "last_watched_at",
                    "rated_at", "user_rated_at", "rating", "user_rating",
                )
            ):
                return True
            return False

        def _compute_history_breakdown(
            state_obj: dict[str, Any] | None,
            feature: str = "history",
        ) -> dict[str, int]:
            movies: set[str] = set()
            shows: set[str] = set()
            anime: set[str] = set()
            episodes: set[str] = set()

            try:
                prov_block = (state_obj or {}).get("providers") or {}
                for _prov_name, prov_data in prov_block.items():
                    feat_block = (prov_data or {}).get(feature) or {}
                    node = feat_block.get("baseline") or feat_block
                    items = node.get("items") or {}

                    if isinstance(items, dict):
                        it = items.values()
                    elif isinstance(items, list):
                        it = items
                    else:
                        continue

                    for rec in it:
                        if not isinstance(rec, dict):
                            continue
                        if _is_presence_stub(rec):
                            continue

                        typ = str(rec.get("type") or "").strip().lower()
                        ids = (rec.get("ids") or {}) or {}
                        show_ids_field = (rec.get("show_ids") or {}) or {}
                        has_show_meta = bool(
                            show_ids_field
                            or rec.get("series_title")
                            or rec.get("show_title")
                        )
                        is_anime = bool(
                            typ == "anime"
                            or ids.get("anilist") or ids.get("mal")
                            or show_ids_field.get("anilist") or show_ids_field.get("mal")
                        )

                        if typ == "episode":
                            s = int(rec.get("season") or 0)
                            ep = int(rec.get("episode") or 0)

                            ep_sig: str | None = None
                            for idk in ("imdb", "tmdb", "tvdb", "slug"):
                                v = ids.get(idk)
                                if v:
                                    ep_sig = f"{idk}:{str(v).lower()}|s{s}e{ep}"
                                    break
                            if ep_sig is None:
                                t = str(
                                    rec.get("title") or rec.get("name") or ""
                                ).strip().lower()
                                y = str(rec.get("year") or "")
                                ep_sig = f"{t}|year:{y}|s{s}e{ep}"
                            if ep_sig:
                                episodes.add(ep_sig)

                            show_ids = show_ids_field or ids
                            show_sig: str | None = None
                            for idk in ("anilist", "mal", "imdb", "tmdb", "tvdb", "slug"):
                                v = show_ids.get(idk)
                                if v:
                                    show_sig = f"{idk}:{str(v).lower()}"
                                    break
                            if show_sig is None:
                                title = (
                                    rec.get("series_title")
                                    or rec.get("show_title")
                                    or rec.get("title")
                                    or rec.get("name")
                                )
                                if title:
                                    y = rec.get("series_year") or rec.get("year")
                                    show_sig = (
                                        f"{str(title).strip().lower()}|year:{y}"
                                    )
                            if show_sig:
                                (anime if is_anime else shows).add(show_sig)
                            continue

                        if is_anime:
                            sig: str | None = None
                            ids_anime = show_ids_field if (show_ids_field.get("anilist") or show_ids_field.get("mal")) else ids
                            for idk in ("anilist", "mal", "tvdb", "tmdb", "imdb", "slug"):
                                v = ids_anime.get(idk)
                                if v:
                                    sig = f"{idk}:{str(v).lower()}"
                                    break
                            if sig is None:
                                title = str(
                                    rec.get("title") or rec.get("name") or ""
                                ).strip().lower()
                                y = str(rec.get("year") or "")
                                sig = f"{title}|year:{y}"
                            if sig:
                                anime.add(sig)
                            continue

                        if typ == "movie" and not has_show_meta:
                            sig: str | None = None
                            for idk in ("imdb", "tmdb", "tvdb", "slug"):
                                v = ids.get(idk)
                                if v:
                                    sig = f"{idk}:{str(v).lower()}"
                                    break
                            if sig is None:
                                title = str(
                                    rec.get("title") or rec.get("name") or ""
                                ).strip().lower()
                                y = str(rec.get("year") or "")
                                sig = f"{title}|year:{y}"
                            movies.add(sig)
                            continue

                        if typ == "show" or (typ == "movie" and has_show_meta):
                            ids_show = show_ids_field or ids
                            show_sig: str | None = None
                            for idk in ("imdb", "tmdb", "tvdb", "slug"):
                                v = ids_show.get(idk)
                                if v:
                                    show_sig = f"{idk}:{str(v).lower()}"
                                    break
                            if show_sig is None:
                                title = (
                                    rec.get("series_title")
                                    or rec.get("show_title")
                                    or rec.get("title")
                                    or rec.get("name")
                                )
                                if title:
                                    y = rec.get("series_year") or rec.get("year")
                                    show_sig = (
                                        f"{str(title).strip().lower()}|year:{y}"
                                    )
                            if show_sig:
                                shows.add(show_sig)
                            continue

                        # Fallback:
                        if has_show_meta:
                            ids_show = show_ids_field or ids
                            show_sig: str | None = None
                            for idk in ("imdb", "tmdb", "tvdb", "slug"):
                                v = ids_show.get(idk)
                                if v:
                                    show_sig = f"{idk}:{str(v).lower()}"
                                    break
                            if show_sig is None:
                                title = (
                                    rec.get("series_title")
                                    or rec.get("show_title")
                                    or rec.get("title")
                                    or rec.get("name")
                                )
                                if title:
                                    y = rec.get("series_year") or rec.get("year")
                                    show_sig = (
                                        f"{str(title).strip().lower()}|year:{y}"
                                    )
                            if show_sig:
                                shows.add(show_sig)
            except Exception as exc:
                _append_log(
                    "INSIGHTS",
                    f"[!] {feature} breakdown failed: {exc}",
                )

            return {
                "movies": len(movies),
                "shows": len(shows),
                "anime": len(anime),
                "episodes": len(episodes),
            }

        def _safe_compute_lanes(
            since: int,
            until: int,
        ) -> tuple[dict[str, dict[str, Any]], dict[str, bool]]:
            try:
                if callable(_compute_lanes_impl):
                    res: Any = _compute_lanes_impl(int(since or 0), int(until or 0))
                    feats_raw: Any = {}
                    enabled_raw: Any = {}
                    if isinstance(res, tuple) and len(res) == 2:
                        feats_raw, enabled_raw = res
                    feats: dict[str, dict[str, Any]] = (
                        feats_raw if isinstance(feats_raw, dict) else _empty_feats()
                    )
                    enabled: dict[str, bool] = (
                        enabled_raw if isinstance(enabled_raw, dict) else _empty_enabled()
                    )
                    for k in feature_keys:
                        feats.setdefault(k, _zero_lane())
                        enabled.setdefault(k, False)
                    return feats, enabled
            except Exception as e:
                _append_log("INSIGHTS", f"[!] _compute_lanes_from_stats failed: {e}")
            return _empty_feats(), _empty_enabled()

        series: list[dict[str, int]] = []
        generated_at: str | None = None
        events: list[dict[str, Any]] = []
        http_block: dict[str, Any] = {}

        if STATS is not None:
            lock = getattr(STATS, "lock", None) or nullcontext()
            try:
                with lock:
                    data = STATS.data or {}
                samples_raw = list((data or {}).get("samples") or [])
                
                events_raw = list((data or {}).get("events") or [])
                try:
                    state = _load_state() or {}
                except Exception:
                    state = {}
                key_map, id_map = _build_show_title_maps(state)

                events = [
                    _format_event_title(_enrich_event_from_state(e, key_map, id_map))
                    for e in events_raw
                    if isinstance(e, dict) and not str(e.get("key", "")).startswith("agg:")
                ]
                http_block = dict((data or {}).get("http") or {})
                generated_at = (data or {}).get("generated_at")

                samples: list[dict[str, Any]] = [r for r in samples_raw if isinstance(r, dict)]
                samples.sort(key=lambda r: int(r.get("ts") or 0))
                if int(limit_samples) > 0:
                    samples = samples[-int(limit_samples):]
                series = [
                    {"ts": int(r.get("ts") or 0), "count": int(r.get("count") or 0)}
                    for r in samples
                ]
            except Exception as e:
                _append_log("INSIGHTS", f"[!] samples load failed: {e}")
                series, events, http_block = [], [], {}

        series_by_feature: dict[str, list[dict[str, int]]] = {k: [] for k in feature_keys}
        series_by_feature["watchlist"] = list(series)

        rows: list[dict[str, Any]] = []
        try:
            files: list[Path] = []
            if REPORT_DIR is not None:
                try:
                    files = sorted(
                        REPORT_DIR.glob("sync-*.json"),
                        key=lambda p: p.stat().st_mtime,
                        reverse=True,
                    )[: max(1, int(history))]
                except Exception as e:
                    _append_log("INSIGHTS", f"[!] report glob failed: {e}")
                    files = []

            for p in files:
                try:
                    d = json.loads(p.read_text(encoding="utf-8"))
                    if not isinstance(d, dict):
                        continue

                    lanes_raw = d.get("features")
                    lanes_in: dict[str, Any] = lanes_raw if isinstance(lanes_raw, dict) else {}
                    lanes: dict[str, dict[str, Any]] = {}
                    for name in feature_keys:
                        lane_val = lanes_in.get(name)
                        lanes[name] = lane_val if isinstance(lane_val, dict) else _zero_lane()

                    since = _safe_parse_epoch(d.get("raw_started_ts") or d.get("started_at"))
                    until = _safe_parse_epoch(d.get("finished_at")) or int(p.stat().st_mtime)

                    stats_feats, stats_enabled = _safe_compute_lanes(since, until)
                    for name in feature_keys:
                        lane = lanes.get(name)
                        if not isinstance(lane, dict) or all(
                            (lane.get(x) or 0) == 0 for x in ("added", "removed", "updated")
                        ):
                            lanes[name] = stats_feats.get(name) or _zero_lane()

                    enabled_raw = d.get("features_enabled") or d.get("enabled") or {}
                    enabled: dict[str, bool] = (
                        enabled_raw if isinstance(enabled_raw, dict) else dict(stats_enabled)
                    )

                    provider_posts = {
                        k[:-5]: v
                        for k, v in d.items()
                        if isinstance(k, str) and k.endswith("_post")
                    }

                    rows.append(
                        {
                            "started_at": d.get("started_at"),
                            "finished_at": d.get("finished_at"),
                            "duration_sec": d.get("duration_sec"),
                            "result": d.get("result") or "",
                            "exit_code": d.get("exit_code"),
                            "added": _as_int(d.get("added_last")),
                            "removed": _as_int(d.get("removed_last")),
                            "features": lanes,
                            "features_enabled": enabled,
                            "updated_total": _as_int(d.get("updated_last")),
                            "provider_posts": provider_posts,
                            "plex_post": d.get("plex_post"),
                            "simkl_post": d.get("simkl_post"),
                            "trakt_post": d.get("trakt_post"),
                            "jellyfin_post": d.get("jellyfin_post"),
                            "emby_post": d.get("emby_post"),
                            "mdblist_post": d.get("mdblist_post"),
                            "crosswatch_post": d.get("crosswatch_post"),
                        }
                    )
                except Exception as e:
                    _append_log("INSIGHTS", f"[!] report parse failed {p.name}: {e}")
        except Exception as e:
            _append_log("INSIGHTS", f"[!] report scan failed: {e}")

        wall_raw = _load_wall_snapshot()
        wall: list[Any]
        if isinstance(wall_raw, list):
            wall = wall_raw
        else:
            wall = []

        state: dict[str, Any] | None = None
        if not wall and callable(_get_orchestrator):
            try:
                orc = _get_orchestrator()
                files_obj = getattr(orc, "files", None)
                if files_obj is not None and hasattr(files_obj, "load_state"):
                    state_candidate = files_obj.load_state()
                    if isinstance(state_candidate, dict):
                        state = state_candidate
                        wall = list(state.get("wall") or [])
            except Exception as e:
                _append_log("SYNC", f"[!] insights: orchestrator init failed: {e}")
                wall = []

        cfg = load_config() or {}
        api_key = str(((cfg.get("tmdb") or {}).get("api_key") or "")).strip()
        use_tmdb = bool(api_key) and bool(int(runtime)) and CACHE_DIR is not None

        def _build_crosswatch_snapshot_info() -> dict[str, Any]:
            info: dict[str, Any] = {}
            try:
                cw_cfg = (cfg.get("crosswatch") or cfg.get("CrossWatch") or {}) or {}
                root_dir = str(cw_cfg.get("root_dir") or "/config/.cw_provider").strip() or "/config/.cw_provider"
                snap_dir = Path(root_dir).joinpath("snapshots")

                selected: dict[str, str] = {
                    "watchlist": str(cw_cfg.get("restore_watchlist") or "latest").strip() or "latest",
                    "history": str(cw_cfg.get("restore_history") or "latest").strip() or "latest",
                    "ratings": str(cw_cfg.get("restore_ratings") or "latest").strip() or "latest",
                }

                files: list[Path] = []
                if snap_dir.is_dir():
                    files = list(snap_dir.glob("*.json"))

                by_feat: dict[str, list[str]] = {"watchlist": [], "history": [], "ratings": []}
                for p in files:
                    name = p.name
                    for feat in by_feat.keys():
                        if name.endswith(f"-{feat}.json"):
                            by_feat[feat].append(name)

                for feat, arr in by_feat.items():
                    arr.sort()
                    sel = selected.get(feat, "latest")
                    actual: str | None = None
                    if arr:
                        if sel == "latest":
                            actual = arr[-1]
                        elif sel in arr:
                            actual = sel
                        else:
                            actual = arr[-1]

                    human: str | None = None
                    iso_ts: str | None = None
                    if actual:
                        try:
                            stem = actual.split("-", 1)[0]
                            dt = _dt.datetime.strptime(stem, "%Y%m%dT%H%M%SZ").replace(
                                tzinfo=_dt.timezone.utc
                            )
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

        def _try_runtime_both(api_key_val: str, typ: str, tmdb_id: int) -> int | None:
            for t in (typ, ("movie" if typ == "tv" else "tv")):
                try:
                    m = get_runtime(api_key_val, t, int(tmdb_id), CACHE_DIR)
                    if m is not None:
                        return int(m)
                except Exception:
                    pass
            return None

        movies = 0
        shows = 0
        total_min = 0
        tmdb_hits = 0
        tmdb_misses = 0
        fetched = 0
        fetch_cap = 50 if use_tmdb else 0

        for meta in wall:
            if not isinstance(meta, dict):
                continue
            typ = "movie" if str((meta.get("type") or "")).lower() == "movie" else "tv"
            if typ == "movie":
                movies += 1
            else:
                shows += 1

            minutes: int | None = None
            tmdb_id = (meta.get("ids") or {}).get("tmdb")
            if use_tmdb and tmdb_id and fetched < fetch_cap:
                try:
                    minutes = _try_runtime_both(api_key, typ, int(str(tmdb_id)))
                except Exception:
                    minutes = None
                fetched += 1
                if minutes is not None:
                    tmdb_hits += 1
                else:
                    tmdb_misses += 1
            if minutes is None:
                minutes = 115 if typ == "movie" else 45
            total_min += int(minutes)

        watchtime = {
            "movies": int(movies),
            "shows": int(shows),
            "minutes": total_min,
            "hours": round(total_min / 60, 1),
            "days": round(total_min / 1440, 1),
            "method": "tmdb" if tmdb_hits and not tmdb_misses else ("mixed" if tmdb_hits else "estimate"),
        }

        if state is None and callable(_get_orchestrator):
            try:
                orc = _get_orchestrator()
                files_obj = getattr(orc, "files", None)
                if files_obj is not None and hasattr(files_obj, "load_state"):
                    st2 = files_obj.load_state()
                    if isinstance(st2, dict):
                        state = st2
            except Exception:
                state = None

        prov_block: dict[str, Any] = (state or {}).get("providers") or {}
        providers_set: set[str] = {
            str(k).strip().lower() for k in prov_block.keys() if isinstance(k, str)
        }

        active: dict[str, bool] = {k: False for k in providers_set}
        try:
            raw_pairs = (cfg.get("pairs") or cfg.get("connections") or [])
            cfg_pairs: list[Any] = raw_pairs if isinstance(raw_pairs, list) else []
            for p in cfg_pairs:
                if not isinstance(p, dict):
                    continue
                s = str(p.get("source") or "").strip().lower()
                t = str(p.get("target") or "").strip().lower()
                if s in active:
                    active[s] = True
                if t in active:
                    active[t] = True
        except Exception:
            pass
        
        def _iter_feature_items(node: Any) -> list[dict[str, Any]]:
            try:
                if not isinstance(node, dict):
                    return []
                base = node.get("baseline")
                if isinstance(base, dict):
                    items = base.get("items")
                    if isinstance(items, dict):
                        return [v for v in items.values() if isinstance(v, dict)]
                    if isinstance(items, list):
                        return [v for v in items if isinstance(v, dict)]
                items = node.get("items")
                if isinstance(items, dict):
                    return [v for v in items.values() if isinstance(v, dict)]
                if isinstance(items, list):
                    return [v for v in items if isinstance(v, dict)]
            except Exception:
                pass
            return []

        def _count_items(node: Any, feature: str | None = None) -> int:
            try:
                if feature in ("history", "ratings") and isinstance(node, dict):
                    recs = _iter_feature_items(node)
                    if feature == "history":
                        return sum(
                            1
                            for r in recs
                            if (not _is_presence_stub(r)) and (r.get("watched_at") or r.get("last_watched_at"))
                        )
                    return sum(
                        1
                        for r in recs
                        if (not _is_presence_stub(r))
                        and (r.get("rated_at") or r.get("user_rated_at") or r.get("rating") or r.get("user_rating"))
                    )

                if isinstance(node, dict):
                    base = node.get("baseline") or {}
                    chk = node.get("checkpoint") or {}
                    pres = node.get("present") or {}

                    for cand in (
                        (chk.get("items") if isinstance(chk, dict) else None),
                        (base.get("items") if isinstance(base, dict) else None),
                        (pres.get("items") if isinstance(pres, dict) else None),
                        node.get("items"),
                    ):
                        if isinstance(cand, dict):
                            return len(cand)
                        if isinstance(cand, list):
                            return len(cand)
                        if isinstance(cand, (int, str)):
                            try:
                                return int(cand)
                            except Exception:
                                return 0
                    return 0
                if isinstance(node, list):
                    return len(node)
                if isinstance(node, (int, str)):
                    return int(node)
            except Exception:
                return 0
            return 0

        providers_by_feature: dict[str, dict[str, int]] = {
            feat: {k: 0 for k in providers_set} for feat in feature_keys
        }
        try:
            for prov_upper, pdata in (prov_block or {}).items():
                key = str(prov_upper or "").strip().lower()
                for feat in feature_keys:
                    providers_by_feature[feat][key] = _count_items((pdata or {}).get(feat) or {}, feat)
        except Exception:
            pass

        providers_mse_by_feature: dict[str, dict[str, dict[str, int]]] = {
            feat: {} for feat in feature_keys
        }
        try:
            for prov_upper, pdata in (prov_block or {}).items():
                key = str(prov_upper or "").strip().lower()
                for feat in feature_keys:

                    if feat in ("history", "ratings"):
                        try:
                            per_counts = _compute_history_breakdown(
                                {"providers": {prov_upper: pdata}},
                                feat,
                            ) or {}
                        except Exception:
                            per_counts = {}
                        providers_mse_by_feature[feat][key] = {
                            "movies": int(per_counts.get("movies") or 0),
                            "shows": int(per_counts.get("shows") or 0),
                            "anime": int(per_counts.get("anime") or 0),
                            "episodes": int(per_counts.get("episodes") or 0),
                        }
                        continue

                    node = (pdata or {}).get(feat) or {}
                    recs = _iter_feature_items(node)
                    if not recs:
                        continue

                    m = s = a = e = 0
                    for rec in recs:
                        if not isinstance(rec, dict):
                            continue
                        typ = str(rec.get("type") or "").strip().lower()
                        has_show_meta = bool(
                            (rec.get("show_ids") or {})
                            or rec.get("series_title")
                            or rec.get("show_title")
                        )

                        if typ == "episode":
                            e += 1
                            continue
                        
                        if typ == "anime":
                            a += 1
                            continue

                        if typ == "show" or (typ == "movie" and has_show_meta):
                            s += 1
                            continue

                        if typ == "movie":
                            m += 1
                            continue

                        if has_show_meta:
                            s += 1
                        else:
                            m += 1

                    providers_mse_by_feature[feat][key] = {
                        "movies": m,
                        "shows": s,
                        "anime": a,
                        "episodes": e,
                    }
        except Exception:
            pass

        now_ts = int(time.time())
        week_floor = now_ts - 7 * 86400
        month_floor = now_ts - 30 * 86400

        def _last_run_lane(feat: str) -> tuple[int, int, int]:
            for row in rows:
                try:
                    en = row.get("features_enabled") or {}
                    if isinstance(en, dict) and en.get(feat) is False:
                        continue
                    feats_map = row.get("features") or {}
                    lane = feats_map.get(feat) if isinstance(feats_map, dict) else {}
                    if not isinstance(lane, dict):
                        lane = {}
                    return (
                        int(lane.get("added") or 0),
                        int(lane.get("removed") or 0),
                        int(lane.get("updated") or 0),
                    )
                except Exception:
                    continue
            return 0, 0, 0

        def _union_now(feat: str) -> int:
            counts = providers_by_feature.get(feat) or {}
            return max(counts.values()) if counts else 0

        def _lane_totals(days: int) -> dict[str, tuple[int, int, int]]:
            feats_any, _enabled_any = _safe_compute_lanes(now_ts - days * 86400, now_ts)
            feats = feats_any if isinstance(feats_any, dict) else {}
            out: dict[str, tuple[int, int, int]] = {}
            for f in feature_keys:
                lane = feats.get(f) or {}
                if not isinstance(lane, dict):
                    lane = {}
                out[f] = (
                    int(lane.get("added") or 0),
                    int(lane.get("removed") or 0),
                    int(lane.get("updated") or 0),
                )
            return out

        week_tot = _lane_totals(7)

        ts_grid = [r["ts"] for r in series_by_feature.get("watchlist", [])]
        if len(ts_grid) < 2:
            base_ts = now_ts - 11 * 3600
            ts_grid = [base_ts + i * 3600 for i in range(12)]
        if ts_grid[-1] < now_ts:
            ts_grid = ts_grid + [now_ts]

        win: list[dict[str, tuple[int, int]]] = []
        for i in range(len(ts_grid) - 1):
            feats_any, _enabled_any = _safe_compute_lanes(ts_grid[i], ts_grid[i + 1])
            feats = feats_any if isinstance(feats_any, dict) else {}
            d: dict[str, tuple[int, int]] = {}
            for f in feature_keys:
                ln = feats.get(f) or {}
                if not isinstance(ln, dict):
                    ln = {}
                d[f] = (
                    int(ln.get("added") or 0),
                    int(ln.get("removed") or 0),
                )
            win.append(d)

        for f in [x for x in feature_keys if x != "watchlist"]:
            v = max(0, _union_now(f))
            out_series: list[dict[str, int]] = [{"ts": ts_grid[-1], "count": v}]
            for i in range(len(ts_grid) - 2, -1, -1):
                a, r = win[i].get(f, (0, 0))
                v = max(0, v - (a - r))
                out_series.append({"ts": ts_grid[i], "count": v})
            series_by_feature[f] = list(reversed(out_series))

        def _val_at(series_list: list[dict[str, int]], floor_ts: int) -> int:
            try:
                arr = sorted(series_list or [], key=lambda r: int(r.get("ts") or 0))
                if not arr:
                    return 0
                val = int(arr[0].get("count") or 0)
                for r in arr:
                    t = int(r.get("ts") or 0)
                    if t <= floor_ts:
                        val = int(r.get("count") or 0)
                    else:
                        break
                return val
            except Exception:
                return 0
            
        history_counts = _compute_history_breakdown(state)

        feats_out: dict[str, dict[str, Any]] = {}
        for feat in feature_keys:
            add_last, rem_last, upd_last = _last_run_lane(feat)
            t = week_tot.get(feat)
            if isinstance(t, tuple) and len(t) == 3:
                wa, wr, wu = t
            else:
                wa, wr, wu = (0, 0, 0)

            if not (add_last or rem_last or upd_last):
                add_last, rem_last, upd_last = wa, wr, wu

            s = series_by_feature.get(feat, [])
            feats_out[feat] = {
                "now": _union_now(feat),
                "week": _val_at(s, week_floor),
                "month": _val_at(s, month_floor),
                "added": add_last,
                "removed": rem_last,
                "updated": upd_last,
                "series": s,
                "providers": providers_by_feature.get(feat, {}),
                "providers_active": active.copy(),
                "providers_mse": providers_mse_by_feature.get(feat, {}),
            }

            if feat == "history":
                feats_out[feat]["breakdown"] = history_counts

        wl = feats_out.get(
            "watchlist",
            {"now": 0, "week": 0, "month": 0, "added": 0, "removed": 0},
        )
        cw_snapshots = _build_crosswatch_snapshot_info()
        payload: dict[str, Any] = {
            "series": series_by_feature.get("watchlist", []),
            "series_by_feature": series_by_feature,
            "history": rows,
            "watchtime": watchtime,
            "providers": feats_out.get("watchlist", {}).get("providers", {}),
            "providers_by_feature": providers_by_feature,
            "providers_active": active,
            "events": events,
            "http": http_block,
            "generated_at": generated_at,
            "features": feats_out,
            "crosswatch_snapshots": cw_snapshots,
            "now": int(wl.get("now", 0) or 0),
            "week": int(wl.get("week", 0) or 0),
            "month": int(wl.get("month", 0) or 0),
            "added": int(wl.get("added", 0) or 0),
            "removed": int(wl.get("removed", 0) or 0),
        }
        return JSONResponse(payload)
from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from cw_platform.config_base import CONFIG
STATE_PATH = CONFIG / "state.json"
STATS_PATH = CONFIG / "statistics.json"
HIDE_PATH  = CONFIG / "watchlist_hide.json"

# Providers (real ones in production, stubbed in tests)
from providers.sync._mod_PLEX import (
    PLEXModule,
    plex_fetch_watchlist_items,
    gather_plex_rows,
    build_index as plex_build_index,
)
from providers.sync._mod_SIMKL import (
    SIMKLModule,
    simkl_ptw_full,
    build_index_from_simkl as simkl_build_index,
)


def _safe_get(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur


def _index_for(cfg: Dict[str, Any], name: str) -> Dict[str, dict]:
    n = name.upper()
    if n == "PLEX":
        token = _safe_get(cfg, "plex", "account_token", default="") or ""
        items = plex_fetch_watchlist_items(None, token, debug=False)
        rows = gather_plex_rows(items)
        rows_movies = [r for r in rows if r.get("type") == "movie"]
        rows_shows = [r for r in rows if r.get("type") == "show"]
        return plex_build_index(rows_movies, rows_shows)
    if n == "SIMKL":
        s = dict(cfg.get("simkl") or {})
        shows, movies = simkl_ptw_full(s)
        return simkl_build_index(movies, shows)
    return {}


def _items_by_type(idx: Dict[str, dict], keys: List[str]) -> Dict[str, List[dict]]:
    out = {"movies": [], "shows": []}
    for k in keys:
        it = idx.get(k) or {}
        typ = it.get("type") or "movie"
        entry = {"ids": it.get("ids") or {}}
        if typ == "show":
            out["shows"].append(entry)
        else:
            out["movies"].append(entry)
    return {k: v for k, v in out.items() if v}


def _normalize_title_year(entry: dict) -> Tuple[str, Optional[int]]:
    t = str((entry.get("title") or "")).strip().lower()
    t = " ".join(t.split())
    y = entry.get("year")
    try:
        y = int(y) if y is not None else None
    except Exception:
        y = None
    return (t, y)


def _diff_additions(a_idx: Dict[str, dict], b_idx: Dict[str, dict]) -> Dict[str, list]:
    missing_keys = list(set(a_idx.keys()) - set(b_idx.keys()))
    if not missing_keys:
        return {}

    dst_sig = set(_normalize_title_year(v) for v in b_idx.values() if v.get("title"))
    filtered = []
    for k in missing_keys:
        ent = a_idx.get(k) or {}
        if _normalize_title_year(ent) in dst_sig:
            continue
        filtered.append(k)

    filtered.sort()
    return _items_by_type(a_idx, filtered)

def _canonical_key(it: dict) -> Optional[str]:
    typ = "show" if (it.get("type") in ("show", "tv")) else "movie"
    ids = it.get("ids") or {}
    tmdb = ids.get("tmdb") or it.get("tmdb")
    return f"{typ}:tmdb:{tmdb}" if tmdb else None

def build_state(plex_idx: Dict[str, dict], simkl_idx: Dict[str, dict]) -> Dict[str, Any]:
    def minimal(idx: Dict[str, dict]) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for k, it in idx.items():
            key = _canonical_key(it) or k
            out[key] = {
                "type": "show" if it.get("type") in ("show", "tv") else "movie",
                "ids": it.get("ids") or {},
                "title": it.get("title") or it.get("name") or "",
                "year": it.get("year") or it.get("release_year"),
                "added": it.get("added") or it.get("added_at") or it.get("created_at"),
            }
        return out

    return {
        "last_sync_epoch": int(time.time()),
        "plex": {"items": minimal(plex_idx)},
        "simkl": {"items": minimal(simkl_idx)},
    }

def write_state(state: Dict[str, Any], path: Optional[Path] = None) -> Path:
    p = (path or (STATE_PATH)).resolve()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p

class Orchestrator:
    """Run configured pairs, compute deltas, trigger provider adds, and write a state snapshot."""

    def __init__(
        self,
        load_cfg: Callable[[], Dict[str, Any]],
        save_cfg: Callable[[Dict[str, Any]], None],
        logger: Callable[..., None] = lambda *a, **k: None,
    ):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.log = logger

    def _count(self, cfg: Dict[str, Any], name: str) -> int:
        if name.upper() == "PLEX":
            token = _safe_get(cfg, "plex", "account_token", default="") or ""
            items = plex_fetch_watchlist_items(None, token, debug=False)
            rows = gather_plex_rows(items)
            return len(rows)
        if name.upper() == "SIMKL":
            shows, movies = simkl_ptw_full(cfg.get("simkl") or {})
            return len(shows) + len(movies)
        return 0

    def run_pairs(
        self,
        *,
        dry_run: bool = False,
        progress: Optional[Callable[[str], None]] = None,
        write_state_json: bool = True,
        state_path: Optional[Path] = None,
    ) -> Dict[str, Any]:
        cfg = self.load_cfg() or {}
        pairs = list(cfg.get("pairs") or [])
        if not pairs:
            if write_state_json:
                st = build_state({}, {})
                write_state(st, state_path)
            return {"ok": True, "pairs": 0, "added": 0, "result": "NOOP"}

        t0 = time.time()
        added_total = 0

        plex_pre = self._count(cfg, "PLEX")
        simkl_pre = self._count(cfg, "SIMKL")
        if progress:
            progress(f"Pre-sync: Plex={plex_pre} vs SIMKL={simkl_pre}")

        for i, pair in enumerate(pairs, start=1):
            if not pair or not pair.get("enabled", True):
                continue
            if not (pair.get("features", {}).get("watchlist", False)):
                continue
            src = str(pair.get("source", "")).upper()
            dst = str(pair.get("target", "")).upper()
            mode = (pair.get("mode") or "one-way").lower()

            if progress:
                progress(f"[i] Pair {i}: {src} → {dst} (mode={mode})")

            idx_src = _index_for(cfg, src)
            idx_dst = _index_for(cfg, dst)

            # forward
            items_fw = _diff_additions(idx_src, idx_dst)
            if items_fw:
                if dst == "SIMKL":
                    res = SIMKLModule(cfg, self.log).simkl_add_to_ptw(items_fw, dry_run=dry_run)
                elif dst == "PLEX":
                    res = PLEXModule(cfg, self.log).plex_add(items_fw, dry_run=dry_run)
                else:
                    res = {"ok": False, "error": f"unknown target {dst}"}
                if res.get("ok"):
                    added_total += int(res.get("added", 0))
                    if progress:
                        progress(f"[i] Pair {i}: added {res.get('added', 0)} to {dst}")
                else:
                    if progress:
                        progress(f"[!] Pair {i}: add {src}→{dst} failed: {res.get('error')}")
            else:
                if progress:
                    progress(f"[i] Pair {i}: nothing to add {src}→{dst}")

            # backward
            if mode in ("two-way", "bi-directional", "bidirectional"):
                items_bw = _diff_additions(idx_dst, idx_src)
                if items_bw:
                    if src == "SIMKL":
                        res2 = SIMKLModule(cfg, self.log).simkl_add_to_ptw(items_bw, dry_run=dry_run)
                    elif src == "PLEX":
                        res2 = PLEXModule(cfg, self.log).plex_add(items_bw, dry_run=dry_run)
                    else:
                        res2 = {"ok": False, "error": f"unknown target {src}"}
                    if res2.get("ok"):
                        added_total += int(res2.get("added", 0))
                        if progress:
                            progress(f"[i] Pair {i}: added {res2.get('added', 0)} to {src}")
                    else:
                        if progress:
                            progress(f"[!] Pair {i}: add {dst}→{src} failed: {res2.get('error')}")
                else:
                    if progress:
                        progress(f"[i] Pair {i}: nothing to add {dst}→{src}")

        plex_post = self._count(cfg, "PLEX")
        simkl_post = self._count(cfg, "SIMKL")
        status = "EQUAL" if plex_post == simkl_post else "UPDATED"
        if progress:
            progress(f"Post-sync: Plex={plex_post} vs SIMKL={simkl_post} -> {status}")
    
        if write_state_json:
            try:
                plex_idx = _index_for(cfg, "PLEX")
                simkl_idx = _index_for(cfg, "SIMKL")
                st = build_state(plex_idx, simkl_idx)
                write_state(st, state_path)
            except Exception:
                pass

        dt = round(time.time() - t0, 2)
        return {
            "ok": True,
            "pairs": len(pairs),
            "added": added_total,
            "duration_sec": dt,
            "plex_pre": plex_pre,
            "simkl_pre": simkl_pre,
            "plex_post": plex_post,
            "simkl_post": simkl_post,
            "result": status,
        }

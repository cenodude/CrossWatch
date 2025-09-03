from __future__ import annotations
import time
from typing import Any, Dict, List, Tuple, Optional, Callable
try:
    from _logging import log as default_log
except Exception:
    def default_log(msg: str, **_: Any) -> None: print(msg)

# Types from base (optional imports for type hints)
try:
    from providers.sync._mod_base import SyncContext, SyncResult, SyncStatus  # type: ignore
except Exception:
    class SyncContext:
        def __init__(self, run_id: str, dry_run: bool=False, timeout_sec: Optional[int]=None):
            self.run_id, self.dry_run, self.timeout_sec = run_id, dry_run, timeout_sec
    class SyncResult: ...
    class SyncStatus: ...

# Import modules
from providers.sync._mod_PLEX import PLEXModule, plex_fetch_watchlist_items, gather_plex_rows, build_index  # type: ignore
from providers.sync._mod_SIMKL import SIMKLModule, simkl_ptw_full, build_index_from_simkl  # type: ignore

def _safe_get(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict): return default
        cur = cur.get(k, default)
    return cur

def _index_for(cfg: Dict[str, Any], name: str) -> Dict[str, dict]:
    n = name.upper()
    if n == "PLEX":
        token = _safe_get(cfg, "plex", "account_token", default="") or ""
        items = plex_fetch_watchlist_items(None, token, debug=False)
        rows = gather_plex_rows(items)
        rows_movies = [r for r in rows if r.get("type") == "movie"]
        rows_shows  = [r for r in rows if r.get("type") == "show"]
        return build_index(rows_movies, rows_shows)
    elif n == "SIMKL":
        s = dict(cfg.get("simkl") or {})
        shows, movies = simkl_ptw_full(s)
        return build_index_from_simkl(movies, shows)
    return {}

def _items_by_type(idx: Dict[str, dict], keys: List[str]) -> Dict[str, List[dict]]:
    out = {"movies": [], "shows": []}
    for k in keys:
        it = idx.get(k) or {}
        typ = it.get("type") or "movie"
        entry = {"ids": it.get("ids") or {}}
        if typ == "show": out["shows"].append(entry)
        else: out["movies"].append(entry)
    return {k:v for k,v in out.items() if v}

class Orchestrator:
    def __init__(self, load_cfg: Callable[[], Dict[str, Any]], save_cfg: Callable[[Dict[str, Any]], None], logger: Callable[..., None] = default_log):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.log = logger

    def _count(self, cfg: Dict[str, Any], name: str) -> int:
        if name.upper() == "PLEX":
            token = _safe_get(cfg, "plex", "account_token", default="") or ""
            items = plex_fetch_watchlist_items(None, token, debug=False)
            rows = gather_plex_rows(items)
            return len(rows)
        elif name.upper() == "SIMKL":
            shows, movies = simkl_ptw_full(cfg.get("simkl") or {})
            return len(shows) + len(movies)
        return 0

    def run_pairs(self, *, dry_run: bool = False, progress: Optional[Callable[[str], None]] = None) -> Dict[str, Any]:
        cfg = self.load_cfg() or {}
        pairs = list(cfg.get("pairs") or [])
        if not pairs:
            return {"ok": True, "pairs": 0, "added": 0, "result": "NOOP"}

        t0 = time.time()
        added_total = 0

        # Pre counts
        plex_pre = self._count(cfg, "PLEX")
        simkl_pre = self._count(cfg, "SIMKL")
        if progress: progress(f"Pre-sync: Plex={plex_pre} vs SIMKL={simkl_pre}")

        for i, pair in enumerate(pairs, start=1):
            if not pair or not pair.get("enabled", True): continue
            if not (pair.get("features", {}).get("watchlist", False)): continue
            src = str(pair.get("source","")).upper(); dst = str(pair.get("target","")).upper()
            mode = (pair.get("mode") or "one-way").lower()
            if progress: progress(f"[i] Pair {i}: {src} → {dst} (mode={mode})")
            idx_src = _index_for(cfg, src); idx_dst = _index_for(cfg, dst)
            to_add_keys = sorted(list(set(idx_src.keys()) - set(idx_dst.keys())))
            items_by_type = _items_by_type(idx_src, to_add_keys)
            if not items_by_type:
                if progress: progress(f"[i] Pair {i}: nothing to add"); continue

            if dst == "SIMKL":
                m = SIMKLModule(cfg, default_log)
                res = m.simkl_add_to_ptw(items_by_type, dry_run=dry_run)
            elif dst == "PLEX":
                m = PLEXModule(cfg, default_log)
                res = m.plex_add(items_by_type, dry_run=dry_run)
            else:
                res = {"ok": False, "error": f"unknown target {dst}"}
            if not res.get("ok"):
                if progress: progress(f"[!] Pair {i}: add failed: {res.get('error')}")
                continue
            added = int(res.get("added", 0)); added_total += added
            if progress: progress(f"[i] Pair {i}: added {added} to {dst}")

        plex_post = self._count(cfg, "PLEX")
        simkl_post = self._count(cfg, "SIMKL")
        status = "EQUAL" if plex_post == simkl_post else "UPDATED"
        if progress: progress(f"Post-sync: Plex={plex_post} vs SIMKL={simkl_post} -> {status}")

        dt = round(time.time()-t0, 2)
        return {"ok": True, "pairs": len(pairs), "added": added_total, "duration_sec": dt,
                "plex_pre": plex_pre, "simkl_pre": simkl_pre, "plex_post": plex_post, "simkl_post": simkl_post,
                "result": status}

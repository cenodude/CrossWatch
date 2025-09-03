from __future__ import annotations
import time
from typing import Any, Dict, List, Tuple, Optional, Callable
try:
    from _logging import log as default_log
except Exception:
    def default_log(message: str, **_: Any) -> None: print(message)

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
            
            # Compute forward additions (src -> dst)
            def _apply_add(target: str, items: Dict[str, list]) -> Dict[str, Any]:
                if target == "SIMKL":
                    m = SIMKLModule(cfg, default_log)
                    return m.simkl_add_to_ptw(items, dry_run=dry_run)
                elif target == "PLEX":
                    m = PLEXModule(cfg, default_log)
                    return m.plex_add(items, dry_run=dry_run)
                return {"ok": False, "error": f"unknown target {target}"}

            def _diff_additions(a_idx: Dict[str, dict], b_idx: Dict[str, dict]) -> Dict[str, list]:
                keys = sorted(list(set(a_idx.keys()) - set(b_idx.keys())))
                return _items_by_type(a_idx, keys)

            items_fw = _diff_additions(idx_src, idx_dst)
            if not items_fw:
                if progress: progress(f"[i] Pair {i}: nothing to add {src}→{dst}")
            else:
                res = _apply_add(dst, items_fw)
                if not res.get("ok"):
                    if progress: progress(f"[!] Pair {i}: add {src}→{dst} failed: {res.get('error')}")
                else:
                    added = int(res.get("added", 0)); added_total += added
                    if progress: progress(f"[i] Pair {i}: added {added} to {dst}")

            # Reverse direction for two-way
            if mode in ("two-way", "bi-directional", "bidirectional"):
                items_bw = _diff_additions(idx_dst, idx_src)
                if not items_bw:
                    if progress: progress(f"[i] Pair {i}: nothing to add {dst}→{src}")
                else:
                    res2 = _apply_add(src, items_bw)
                    if not res2.get("ok"):
                        if progress: progress(f"[!] Pair {i}: add {dst}→{src} failed: {res2.get('error')}")
                    else:
                        added2 = int(res2.get("added", 0)); added_total += added2
                        if progress: progress(f"[i] Pair {i}: added {added2} to {src}")


        plex_post = self._count(cfg, "PLEX")
        simkl_post = self._count(cfg, "SIMKL")
        status = "EQUAL" if plex_post == simkl_post else "UPDATED"
        if progress: progress(f"Post-sync: Plex={plex_post} vs SIMKL={simkl_post} -> {status}")

        dt = round(time.time()-t0, 2)
        return {"ok": True, "pairs": len(pairs), "added": added_total, "duration_sec": dt,
                "plex_pre": plex_pre, "simkl_pre": simkl_pre, "plex_post": plex_post, "simkl_post": simkl_post,
                "result": status}

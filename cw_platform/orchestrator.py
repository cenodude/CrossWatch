from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple, Set, Mapping
import requests

from cw_platform.config_base import CONFIG

# Persistent files
STATE_PATH = CONFIG / "state.json"
TOMBSTONES_PATH = CONFIG / "tombstones.json"

# Providers
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

# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _count_types(idx: Dict[str, dict]) -> tuple[int,int,int]:
    """Return (total, movies, shows) for an index."""
    if not isinstance(idx, dict): 
        return (0,0,0)
    m = sum(1 for v in idx.values() if (v.get("type") in ("movie","movies")))
    s = sum(1 for v in idx.values() if (v.get("type") in ("show","tv","shows")))
    return (m+s, m, s)

def _safe_get(d: dict, *path, default=None):
    cur = d
    for k in path:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k, default)
    return cur

def _normalize_title_year(entry: dict) -> Tuple[str, Optional[int]]:
    """Normalize to a case/space-folded (title, year) signature."""
    t = str((entry.get("title") or entry.get("name") or "")).strip().lower()
    t = " ".join(t.split())
    y = entry.get("year") or entry.get("release_year")
    try:
        y = int(y) if y is not None else None
    except Exception:
        y = None
    return (t, y)

def _sig(entry: dict) -> Tuple[str, Optional[int]]:
    return _normalize_title_year(entry)

def _collect_ids(entry: dict) -> Dict[str, str]:
    """Collect known external ids from an item record."""
    ids = (entry.get("ids") or {})
    out: Dict[str, str] = {}
    for k in ("imdb", "tmdb", "tvdb", "slug"):
        v = ids.get(k) or entry.get(k)
        if v:
            out[k] = str(v)
    return out

def _canonical_key(it: dict) -> Optional[str]:
    """Stable canonical key: '<type>:<idtype>:<id>' if available."""
    typ = "show" if (it.get("type") in ("show", "tv")) else "movie"
    ids = _collect_ids(it)
    for k in ("imdb", "tmdb", "tvdb", "slug"):
        v = ids.get(k)
        if v:
            return f"{typ}:{k}:{v}"
    return None

def _items_by_type_from_keys(idx: Dict[str, dict], keys: List[str]) -> Dict[str, List[dict]]:
    """Return {'movies':[...], 'shows':[...]} payload for the given index keys."""
    out: Dict[str, List[dict]] = {"movies": [], "shows": []}
    for k in keys:
        it = idx.get(k) or {}
        typ = "show" if (it.get("type") in ("show", "tv")) else "movie"
        entry = {
            "ids": _collect_ids(it),
            "title": it.get("title") or it.get("name") or "",
            "year": it.get("year") or it.get("release_year"),
        }
        (out["shows"] if typ == "show" else out["movies"]).append(entry)
    return {k: v for k, v in out.items() if v}

def _payload_from_snapshot(items: List[dict]) -> Dict[str, List[dict]]:
    """Build an add/remove payload from a list of snapshot items."""
    movies, shows = [], []
    for it in items:
        typ = "show" if (it.get("type") in ("show", "tv")) else "movie"
        ids = _collect_ids(it)
        entry = {
            "ids": ids if ids else {},
            "title": it.get("title") or it.get("name") or "",
            "year": it.get("year") or it.get("release_year"),
        }
        (shows if typ == "show" else movies).append(entry)
    out: Dict[str, List[dict]] = {}
    if movies: out["movies"] = movies
    if shows:  out["shows"] = shows
    return out

def _keyset(idx: Dict[str, dict]) -> Set[str]:
    return set(idx.keys())

def _sigset(idx: Dict[str, dict]) -> Set[Tuple[str, Optional[int]]]:
    return set(_sig(v) for v in idx.values() if v.get("title") or v.get("name"))

# ---------------------------------------------------------------------------
# Live index readers
# ---------------------------------------------------------------------------

def _index_for(cfg: Dict[str, Any], name: str) -> Dict[str, dict]:
    """Return the current watchlist index for the given provider."""
    n = name.upper()
    if n == "PLEX":
        token = _safe_get(cfg, "plex", "account_token", default="") or ""
        items = plex_fetch_watchlist_items(None, token, debug=False)
        rows = gather_plex_rows(items)
        rows_movies = [r for r in rows if r.get("type") == "movie"]
        rows_shows  = [r for r in rows if r.get("type") == "show"]
        return plex_build_index(rows_movies, rows_shows)
    if n == "SIMKL":
        s = dict(cfg.get("simkl") or {})
        shows, movies = simkl_ptw_full(s, debug=bool(_safe_get(cfg, "runtime", "debug", default=False)))
        idx = simkl_build_index(movies, shows)
        return idx
    return {}

# ---------------------------------------------------------------------------
# Additions and deletions planning
# ---------------------------------------------------------------------------

def _diff_additions(a_idx: Dict[str, dict], b_idx: Dict[str, dict]) -> Dict[str, list]:
    """
    Items present in A but missing in B by key.
    To avoid dupes when keys drift, also skip if (title,year) already exists in B.
    """
    missing = list(set(a_idx.keys()) - set(b_idx.keys()))
    if not missing:
        return {}
    dst_sig = _sigset(b_idx)
    filtered = []
    for k in missing:
        ent = a_idx.get(k) or {}
        if _sig(ent) in dst_sig:
            continue
        filtered.append(k)
    filtered.sort()
    return _items_by_type_from_keys(a_idx, filtered)

def _deletes_payload_from_snapshot(
    snap_src_idx: Dict[str, dict],
    live_src_idx: Dict[str, dict],
    *,
    drift_guard: bool = True,
) -> Dict[str, list]:
    """
    SAFE deletes:
    - Only delete items that are missing by KEY *and* by (title,year) signature.
    - If too many would be deleted (likely key drift or temporary outage), skip deletes.
    """
    if not snap_src_idx:
        return {}

    snap_keys = _keyset(snap_src_idx)
    live_keys = _keyset(live_src_idx)
    snap_sigs = _sigset(snap_src_idx)
    live_sigs = _sigset(live_src_idx)

    victims: List[dict] = []
    for k, it in snap_src_idx.items():
        if (k not in live_keys) and (_sig(it) not in live_sigs):
            victims.append(it)

    if drift_guard and victims:
        total = max(1, len(snap_src_idx))
        if len(victims) > max(3, int(0.5 * total)):
            # Suspicious: mass delete prevented.
            return {}

    if not victims:
        return {}

    return _payload_from_snapshot(victims)

# ---------------------------------------------------------------------------
# Snapshot I/O
# ---------------------------------------------------------------------------

def load_snapshot(path: Optional[Path] = None) -> Dict[str, Any]:
    p = (path or STATE_PATH).resolve()
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text(encoding="utf-8")) or {}
    except Exception:
        return {}

def snapshot_to_index(snap: Dict[str, Any], side: str) -> Dict[str, dict]:
    side = side.lower()
    items = _safe_get(snap, side, "items", default={}) or {}
    out: Dict[str, dict] = {}
    for k, it in items.items():
        typ = "show" if (it.get("type") in ("show", "tv")) else "movie"
        rec = {
            "type": typ,
            "ids": _collect_ids(it),
            "title": it.get("title") or it.get("name") or "",
            "year": it.get("year") or it.get("release_year"),
        }
        key = _canonical_key(rec) or k
        out[key] = rec
    return out

def build_state(plex_idx: Dict[str, dict], simkl_idx: Dict[str, dict]) -> Dict[str, Any]:
    def minimal(idx: Dict[str, dict]) -> Dict[str, dict]:
        out: Dict[str, dict] = {}
        for _, it in idx.items():
            typ = "show" if (it.get("type") in ("show", "tv")) else "movie"
            rec = {
                "type": typ,
                "ids": _collect_ids(it),
                "title": it.get("title") or it.get("name") or "",
                "year": it.get("year") or it.get("release_year"),
            }
            key = _canonical_key(rec) or (typ + ":" + rec.get("title", ""))
            out[key] = rec
        return out
    return {
        "last_sync_epoch": int(time.time()),
        "plex":  {"items": minimal(plex_idx)},
        "simkl": {"items": minimal(simkl_idx)},
    }

def write_state(state: Dict[str, Any], path: Optional[Path] = None) -> Path:
    p = (path or STATE_PATH).resolve()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(state, indent=2), encoding="utf-8")
    tmp.replace(p)
    return p

# ---------------------------------------------------------------------------
# Tombstones (deletion memory)
# ---------------------------------------------------------------------------

def load_tombstones(path: Optional[Path] = None) -> Dict[str, Any]:
    p = (path or TOMBSTONES_PATH).resolve()
    if not p.exists():
        return {"ttl_sec": 21600, "entries": {}}
    try:
        data = json.loads(p.read_text(encoding="utf-8")) or {}
        data.setdefault("ttl_sec", 21600)
        data.setdefault("entries", {})
        return data
    except Exception:
        return {"ttl_sec": 21600, "entries": {}}

def save_tombstones(tb: Dict[str, Any], path: Optional[Path] = None) -> None:
    p = (path or TOMBSTONES_PATH).resolve()
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(tb, indent=2), encoding="utf-8")
    tmp.replace(p)

def prune_tombstones(tb: Dict[str, Any], now: Optional[int] = None) -> None:
    now = now or int(time.time())
    ttl = int(tb.get("ttl_sec", 21600))
    entries = tb.get("entries", {})
    dead = [k for k, v in entries.items() if (now - int(v.get("ts", 0))) > ttl]
    for k in dead:
        entries.pop(k, None)

def mark_tombstones(tb: Dict[str, Any], payload: Dict[str, list], origin: str) -> None:
    """Remember deletions by best-effort canonical key for 48h to avoid churn."""
    now = int(time.time())
    entries = tb.setdefault("entries", {})
    def maybe_key(typ: str, ids: Dict[str, str]) -> Optional[str]:
        for k in ("imdb", "tmdb", "tvdb", "slug"):
            v = ids.get(k)
            if v:
                return f"{typ}:{k}:{v}"
        return None
    for typ, bucket in (("movie", payload.get("movies", [])),
                        ("show",  payload.get("shows",  []))):
        for it in bucket or []:
            key = maybe_key(typ, it.get("ids") or {})
            if key:
                entries[key] = {"origin": origin.upper(), "ts": now}

def filter_additions_with_tombstones(payload: Dict[str, list],
                                     tb: Dict[str, Any],
                                     *,
                                     block_target: Optional[str] = None,
                                     progress: Optional[Callable[[str], None]] = None) -> Dict[str, list]:
    """
    Directional tombstones:
    - Only block re-adding to the SAME side that originally deleted the item.
      (origin == block_target). If block_target is None, block globally.
    """
    if not payload:
        return {}
    entries = tb.get("entries", {})
    out: Dict[str, List[dict]] = {}
    skipped = 0
    for typ_key, bucket in (("movies", payload.get("movies", [])),
                            ("shows",  payload.get("shows",  []))):
        keep = []
        for it in bucket or []:
            ids = it.get("ids") or {}
            key = None
            for k in ("imdb", "tmdb", "tvdb", "slug"):
                v = ids.get(k)
                if v:
                    key = f"{('show' if typ_key=='shows' else 'movie')}:{k}:{v}"
                    break
            if key and key in entries:
                origin = (entries.get(key) or {}).get("origin")
                if (block_target is None) or (str(origin).upper() == str(block_target).upper()):
                    skipped += 1
                    continue
            keep.append(it)
        if keep:
            out[typ_key] = keep
    if skipped and progress:
        progress(f"[i] Tombstone blocked {skipped} additions to {block_target or 'ANY'}")
    return out

# ---------------------------------------------------------------------------
# TMDb helpers (for Plex "discover" adds)
# ---------------------------------------------------------------------------

TMDB_BASE = "https://api.themoviedb.org/3"

def _tmdb_find_by_imdb(api_key: str, imdb_id: str, typ: str) -> Optional[int]:
    try:
        r = requests.get(
            f"{TMDB_BASE}/find/{imdb_id}",
            params={"api_key": api_key, "external_source": "imdb_id"},
            timeout=12,
        )
        if not r.ok:
            return None
        j = r.json() or {}
        arr = j.get("movie_results") if typ == "movie" else j.get("tv_results")
        if arr:
            return int(arr[0].get("id"))
    except Exception:
        return None
    return None

def _tmdb_search(api_key: str, title: str, year: Optional[int], typ: str) -> Optional[int]:
    try:
        ep = "search/movie" if typ == "movie" else "search/tv"
        params = {"api_key": api_key, "query": title, "include_adult": "false"}
        if year:
            params["year" if typ == "movie" else "first_air_date_year"] = year # type: ignore
        r = requests.get(f"{TMDB_BASE}/{ep}", params=params, timeout=12)
        if not r.ok:
            return None
        j = r.json() or {}
        arr = j.get("results") or []
        if arr:
            return int(arr[0].get("id"))
    except Exception:
        return None
    return None

def _ensure_tmdb_ids(items_by_type: Mapping[str, List[Mapping[str, Any]]],
                     *,
                     tmdb_api_key: str,
                     logger: Callable[..., None] = lambda *a, **k: None) -> Dict[str, List[Dict[str, Any]]]:
    """
    Enrich entries with tmdb id when possible while preserving existing ids.
    Always keep the item (do not drop if tmdb is not found).
    Also embed title/year inside ids to aid Plex discovery.
    """
    out: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}

    def handle_bucket(bucket: str):
        typ = "movie" if bucket == "movies" else "show"
        for it in items_by_type.get(bucket, []) or []:
            title = (it.get("title") or "").strip()
            year = it.get("year")
            ids = dict(it.get("ids") or {})
            # Try to ensure tmdb id
            tmdb_id = None
            try:
                tmdb_val = ids.get("tmdb")
                if tmdb_val is not None and str(tmdb_val).isdigit():
                    tmdb_id = int(str(tmdb_val))
            except Exception:
                tmdb_id = None
            if tmdb_id is None and tmdb_api_key:
                imdb = ids.get("imdb")
                if isinstance(imdb, str) and imdb.strip():
                    tmdb_id = _tmdb_find_by_imdb(tmdb_api_key, imdb.strip(), typ)
                if tmdb_id is None and title:
                    try:
                        y = int(year) if year is not None else None
                    except Exception:
                        y = None
                    tmdb_id = _tmdb_search(tmdb_api_key, title, y, typ)
            if tmdb_id is not None:
                ids["tmdb"] = tmdb_id
            else:
                if title or year or ids:
                    logger(f"[TMDb] enrich skipped (no match) → keep {typ}: title={title!r} year={year} ids={ids}")
            # Always embed title/year for Plex discover
            if title and "title" not in ids:
                ids["title"] = title
            if year is not None and "year" not in ids:
                try: ids["year"] = int(year)
                except Exception: pass
            out[bucket].append({"ids": ids, "title": title, "year": year})

    handle_bucket("movies")
    handle_bucket("shows")

    if not out["movies"]:
        out.pop("movies", None)
    if not out["shows"]:
        out.pop("shows", None)
    return out

def _union_preserve(orig: Dict[str, List[Dict[str, Any]]],
                    enr: Optional[Dict[str, List[Dict[str, Any]]]]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Merge enriched results into the original payload.
    Keep unique items; ensure title/year are present inside ids.
    """
    if not enr:
        return dict(orig or {})
    out: Dict[str, List[Dict[str, Any]]] = {}
    for b in ("movies", "shows"):
        lst: List[Dict[str, Any]] = []
        e = enr.get(b, []) if isinstance(enr, dict) else []
        lst.extend(e)
        have_tmdb = {str((x.get("ids") or {}).get("tmdb") or "") for x in e}
        for it in (orig.get(b, []) or []):
            tmdb = str((it.get("ids") or {}).get("tmdb") or "")
            if not tmdb or tmdb not in have_tmdb:
                ids = dict(it.get("ids") or {})
                title = (it.get("title") or "").strip()
                year = it.get("year")
                if title and "title" not in ids: ids["title"] = title
                if year is not None and "year" not in ids:
                    try: ids["year"] = int(year)
                    except Exception: pass
                lst.append({"ids": ids, "title": title, "year": year})
        if lst:
            out[b] = lst
    return out

# ---------------------------------------------------------------------------
# Provider operations
# ---------------------------------------------------------------------------

def _add_to(name: str, cfg: Dict[str, Any], payload: Dict[str, list], dry_run: bool) -> Dict[str, Any]:
    """Add items to a provider watchlist/PTW."""
    if not payload:
        return {"ok": True, "added": 0}
    if name.upper() == "PLEX":
        return PLEXModule(cfg, lambda *a, **k: None).plex_add(payload, dry_run=dry_run)
    if name.upper() == "SIMKL":
        return SIMKLModule(cfg, lambda *a, **k: None).simkl_add_to_ptw(payload, dry_run=dry_run)
    return {"ok": False, "error": f"unknown target {name}"}

def _remove_from(name: str, cfg: Dict[str, Any], payload: Dict[str, list], dry_run: bool) -> Dict[str, Any]:
    """Remove items from a provider watchlist/PTW."""
    if not payload:
        return {"ok": True, "removed": 0}
    if name.upper() == "PLEX":
        return PLEXModule(cfg, lambda *a, **k: None).plex_remove(payload, dry_run=dry_run)
    if name.upper() == "SIMKL":
        return SIMKLModule(cfg, lambda *a, **k: None).simkl_remove_from_ptw(payload, dry_run=dry_run)
    return {"ok": False, "error": f"unknown target {name}"}

# ---------------------------------------------------------------------------
# Orchestrator
# ---------------------------------------------------------------------------

class Orchestrator:
    """
    Two modes: one-way or two-way.
    We always apply "delete first, then add". Deletes are guarded to avoid mass removals.
    Tombstones are directional to avoid churn.
    """

    def __init__(
        self,
        load_cfg: Callable[[], Dict[str, Any]],
        save_cfg: Callable[[Dict[str, Any]], None],
        logger: Callable[..., None] = lambda *a, **k: None,
    ):
        self.load_cfg = load_cfg
        self.save_cfg = save_cfg
        self.log = logger

    def run_pairs(
        self,
        *,
        dry_run: bool = False,
        progress: Optional[Callable[[str], None]] = None,
        write_state_json: bool = True,
        state_path: Optional[Path] = None,
        use_snapshot: bool = True,
        snapshot_guard_delete: bool = True,
        tombstone_ttl_sec: Optional[int] = None,
    ) -> Dict[str, Any]:
        cfg = self.load_cfg() or {}
        pairs = list(cfg.get("pairs") or [])

        baseline = load_snapshot(state_path) if use_snapshot else {}
        snap_plex  = snapshot_to_index(baseline, "plex")  if baseline else {}
        snap_simkl = snapshot_to_index(baseline, "simkl") if baseline else {}

        tomb = load_tombstones()
        if tombstone_ttl_sec is not None:
            tomb["ttl_sec"] = int(tombstone_ttl_sec)
        prune_tombstones(tomb)

        # Diagnostics
        if progress:
            progress(f"[i] Config: use_snapshot={use_snapshot}, snapshot_guard_delete={snapshot_guard_delete}")
            if baseline:
                progress(f"[i] Snapshot: PLEX={len(snap_plex)} keys, SIMKL={len(snap_simkl)} keys")
            else:
                progress("[i] Snapshot: empty (first run or disabled)")
            progress(f"[i] Tombstones: ttl={tomb.get('ttl_sec', 21600)}s, keys={len(tomb.get('entries', {}))}")

        # Pre counts
        try:
            pre_plex  = _index_for(cfg, "PLEX")
            pre_simkl = _index_for(cfg, "SIMKL")
            pt, pm, ps = _count_types(pre_plex)
            st, sm, ss = _count_types(pre_simkl)
            diff = st - pt
            stat = "equal" if diff == 0 else (f"SIMKL - Plex = {diff:+d}")
            if progress:
                progress(f"[i] Pre-sync counts: Plex={pt} (m={pm}, s={ps}) vs SIMKL={st} (m={sm}, s={ss}) ({stat})")
        except Exception:
            pass

        t0 = time.time()
        added_total = 0
        removed_total = 0

        # Bulk guard for adds
        MAX_BULK = int((_safe_get(cfg, "sync", "max_additions", default=200)) or 200) # type: ignore
        def _bulk_guard(payload, who):
            n = sum(len(v) for v in (payload or {}).values())
            if n > MAX_BULK:
                if progress: progress(f"[!] Bulk guard: planned add {n} > {MAX_BULK} → abort {who}")
                return True
            return False

        for i, pair in enumerate(pairs, start=1):
            if not pair or not pair.get("enabled", True):
                continue

            wl = (pair.get("features") or {}).get("watchlist") or {}
            can_add = bool(wl.get("add", True))
            can_remove = bool(wl.get("remove", False))

            src = str(pair.get("source", "")).upper()
            dst = str(pair.get("target", "")).upper()
            mode = (pair.get("mode") or "one-way").lower()

            if progress:
                progress(f"[i] Pair {i}: {src} → {dst} (mode={mode}, add={can_add}, remove={can_remove})")

            # Current live snapshots
            live_src = _index_for(cfg, src)
            live_dst = _index_for(cfg, dst)

            # --- DELETE FIRST (guarded) ---
            if can_remove:
                if mode == "one-way":
                    snap_src = snap_plex if src == "PLEX" else snap_simkl
                    dels_payload = _deletes_payload_from_snapshot(snap_src, live_src)
                    if snapshot_guard_delete and not baseline:
                        dels_payload = {}
                    if dels_payload:
                        if progress:
                            mc = len(dels_payload.get("movies", []) or [])
                            sc = len(dels_payload.get("shows", []) or [])
                            progress(f"[i] delete detected on {src}: movies={mc}, shows={sc} → remove on {dst}")
                        mark_tombstones(tomb, dels_payload, origin=src)
                        rd = _remove_from(dst, cfg, dels_payload, dry_run)
                        if rd.get("ok"):
                            removed_total += int(rd.get("removed", 0))
                            if progress: progress(f"[i] -{rd.get('removed', 0)} from {dst}")
                            live_dst = _index_for(cfg, dst)

                elif mode == "two-way":
                    # Deletes on SRC → remove on DST (based on SRC snapshot)
                    snap_src = snap_plex if src == "PLEX" else snap_simkl
                    dels_dst_payload = _deletes_payload_from_snapshot(snap_src, live_src)
                    if snapshot_guard_delete and not baseline:
                        dels_dst_payload = {}
                    # Deletes on DST → remove on SRC (based on DST snapshot)
                    snap_dst = snap_simkl if dst == "SIMKL" else snap_plex
                    dels_src_payload = _deletes_payload_from_snapshot(snap_dst, live_dst)
                    if snapshot_guard_delete and not baseline:
                        dels_src_payload = {}

                    if dels_dst_payload:
                        if progress:
                            mc = len(dels_dst_payload.get("movies", []) or [])
                            sc = len(dels_dst_payload.get("shows", []) or [])
                            progress(f"[i] delete detected on {src}: movies={mc}, shows={sc} → remove on {dst}")
                        mark_tombstones(tomb, dels_dst_payload, origin=src)
                        rd = _remove_from(dst, cfg, dels_dst_payload, dry_run)
                        if rd.get("ok"):
                            removed_total += int(rd.get("removed", 0))
                            if progress: progress(f"[i] -{rd.get('removed', 0)} from {dst}")
                            live_dst = _index_for(cfg, dst)

                    if dels_src_payload:
                        if progress:
                            mc = len(dels_src_payload.get("movies", []) or [])
                            sc = len(dels_src_payload.get("shows", []) or [])
                            progress(f"[i] delete detected on {dst}: movies={mc}, shows={sc} → remove on {src}")
                        mark_tombstones(tomb, dels_src_payload, origin=dst)
                        rs = _remove_from(src, cfg, dels_src_payload, dry_run)
                        if rs.get("ok"):
                            removed_total += int(rs.get("removed", 0))
                            if progress: progress(f"[i] -{rs.get('removed', 0)} from {src}")
                            live_src = _index_for(cfg, src)

            # --- THEN ADD ---
            if can_add:
                tmdb_key = (_safe_get(cfg, "tmdb", "api_key", default="") or "").strip()

                if mode == "one-way":
                    items_fw = _diff_additions(live_src, live_dst)
                    if progress:
                        mc = len((items_fw or {}).get('movies') or [])
                        sc = len((items_fw or {}).get('shows') or [])
                        progress(f"[i] plan add {src}→{dst}: movies={mc}, shows={sc}")
                    items_fw = filter_additions_with_tombstones(items_fw, tomb, block_target=dst, progress=progress)
                    if _bulk_guard(items_fw, f"{src}→{dst}"):
                        return {"ok": False, "added": added_total, "removed": removed_total, "aborted": True}

                    if items_fw and dst == "PLEX":
                        if tmdb_key:
                            enr = _ensure_tmdb_ids(items_fw, tmdb_api_key=tmdb_key, logger=self.log)
                            items_fw = _union_preserve(items_fw, enr)
                            if progress:
                                em = len((enr or {}).get('movies') or []); es = len((enr or {}).get('shows') or [])
                                pm = len((items_fw or {}).get('movies') or []); ps = len((items_fw or {}).get('shows') or [])
                                progress(f"[i] TMDb enrich for PLEX: +{em}m/{es}s (fallback keeps {pm}m/{ps}s)")
                        else:
                            # Ensure title/year are present in ids even without TMDb
                            items_fw = _union_preserve(items_fw, items_fw)

                    if items_fw:
                        r = _add_to(dst, cfg, items_fw, dry_run)
                        if r.get("ok"):
                            added_total += int(r.get("added", 0))
                            if progress: progress(f"[i] +{r.get('added', 0)} → {dst}")
                            live_dst = _index_for(cfg, dst)

                elif mode == "two-way":
                    # src -> dst
                    items_fw = _diff_additions(live_src, live_dst)
                    items_fw = filter_additions_with_tombstones(items_fw, tomb, block_target=dst, progress=progress)
                    if _bulk_guard(items_fw, f"{src}→{dst}"):
                        return {"ok": False, "added": added_total, "removed": removed_total, "aborted": True}
                    if items_fw and dst == "PLEX":
                        if tmdb_key:
                            enr = _ensure_tmdb_ids(items_fw, tmdb_api_key=tmdb_key, logger=self.log)
                            items_fw = _union_preserve(items_fw, enr)
                            if progress:
                                em = len((enr or {}).get('movies') or []); es = len((enr or {}).get('shows') or [])
                                pm = len((items_fw or {}).get('movies') or []); ps = len((items_fw or {}).get('shows') or [])
                                progress(f"[i] TMDb enrich for PLEX: +{em}m/{es}s (fallback keeps {pm}m/{ps}s)")
                        else:
                            items_fw = _union_preserve(items_fw, items_fw)
                    if items_fw:
                        r = _add_to(dst, cfg, items_fw, dry_run)
                        if r.get("ok"):
                            added_total += int(r.get("added", 0))
                            if progress: progress(f"[i] +{r.get('added', 0)} → {dst}")
                            live_dst = _index_for(cfg, dst)

                    # dst -> src
                    items_bw = _diff_additions(live_dst, live_src)
                    if progress:
                        mc2 = len((items_bw or {}).get('movies') or [])
                        sc2 = len((items_bw or {}).get('shows') or [])
                        progress(f"[i] plan add {dst}→{src}: movies={mc2}, shows={sc2}")
                    items_bw = filter_additions_with_tombstones(items_bw, tomb, block_target=src, progress=progress)
                    if _bulk_guard(items_bw, f"{dst}→{src}"):
                        return {"ok": False, "added": added_total, "removed": removed_total, "aborted": True}
                    if items_bw and src == "PLEX":
                        if tmdb_key:
                            enr2 = _ensure_tmdb_ids(items_bw, tmdb_api_key=tmdb_key, logger=self.log)
                            items_bw = _union_preserve(items_bw, enr2)
                            if progress:
                                em2 = len((enr2 or {}).get('movies') or []); es2 = len((enr2 or {}).get('shows') or [])
                                pm2 = len((items_bw or {}).get('movies') or []); ps2 = len((items_bw or {}).get('shows') or [])
                                progress(f"[i] TMDb enrich for PLEX: +{em2}m/{es2}s (fallback keeps {pm2}m/{ps2}s)")
                        else:
                            items_bw = _union_preserve(items_bw, items_bw)
                    if items_bw:
                        r2 = _add_to(src, cfg, items_bw, dry_run)
                        if r2.get("ok"):
                            added_total += int(r2.get("added", 0))
                            if progress: progress(f"[i] +{r2.get('added', 0)} → {src}")
                            live_src = _index_for(cfg, src)
            else:
                if progress: progress("[i] add disabled by features; skipping adds")

        # Persist snapshot + tombstones
        plex_live  = _index_for(cfg, "PLEX")
        simkl_live = _index_for(cfg, "SIMKL")
        if write_state_json:
            try:
                write_state(build_state(plex_live, simkl_live), state_path)
                if progress: progress("[i] Snapshot updated")
                try:
                    post_plex  = _index_for(cfg, "PLEX")
                    post_simkl = _index_for(cfg, "SIMKL")
                    pt2, pm2, ps2 = _count_types(post_plex)
                    st2, sm2, ss2 = _count_types(post_simkl)
                    eq = "EQUAL" if (pt2 == st2) else "NOT EQUAL"
                    if progress:
                        progress(f"[i] Post-sync: Plex={pt2} (m={pm2}, s={ps2}) vs SIMKL={st2} (m={sm2}, s={ss2}) → {eq}")
                except Exception:
                    pass
            except Exception as ex:
                if progress: progress(f"[!] Snapshot update failed: {ex}")

        prune_tombstones(tomb)
        save_tombstones(tomb)

        dt = round(time.time() - t0, 2)
        return {
            "ok": True,
            "pairs": len(pairs),
            "added": added_total,
            "removed": removed_total,
            "duration_sec": dt,
            "use_snapshot": bool(baseline),
            "snapshot_guard_delete": bool(snapshot_guard_delete),
            "tombstones_ttl_sec": int(tomb.get("ttl_sec", 21600)),
        }

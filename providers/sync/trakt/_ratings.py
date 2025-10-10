# /providers/sync/trakt/_ratings.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ._common import (
    build_headers,
    normalize_watchlist_row,
    key_of,                 # canonical key expected by orchestrator
    pick_trakt_kind,        # movies|shows|seasons|episodes
    ids_for_trakt,
)

try:
    from cw_platform.id_map import minimal as id_minimal
except Exception:
    from _id_map import minimal as id_minimal  # type: ignore

BASE = "https://api.trakt.tv"
URL_RAT_MOV = f"{BASE}/sync/ratings/movies"
URL_RAT_SHO = f"{BASE}/sync/ratings/shows"
URL_RAT_SEA = f"{BASE}/sync/ratings/seasons"
URL_RAT_EPI = f"{BASE}/sync/ratings/episodes"
URL_UPSERT  = f"{BASE}/sync/ratings"
URL_UNRATE  = f"{BASE}/sync/ratings/remove"

CACHE_PATH = "/config/.cw_state/trakt_ratings.index.json"

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_TRAKT_DEBUG"):
        print(f"[TRAKT:ratings] {msg}")

# -------------------- helpers --------------------

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

def _valid_rating(v: Any) -> Optional[int]:
    try:
        i = int(str(v).strip())
        return i if 1 <= i <= 10 else None
    except Exception:
        return None

def _load_cache() -> Dict[str, Any]:
    try:
        p = Path(CACHE_PATH)
        if not p.exists(): return {}
        doc = json.loads(p.read_text("utf-8") or "{}")
        return dict(doc.get("items") or {})
    except Exception:
        return {}

def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        p = Path(CACHE_PATH); p.parent.mkdir(parents=True, exist_ok=True)
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
        _log(f"cache.saved -> {p} ({len(items)})")
    except Exception as e:
        _log(f"cache.save failed: {e}")

def _merge_by_canonical(dst: Dict[str, Any], src: Iterable[Mapping[str, Any]]) -> None:
    # Prefer more IDs; tie -> newer rated_at
    def q(x: Mapping[str, Any]) -> Tuple[int, str]:
        ids = x.get("ids") or {}
        score = sum(1 for k in ("trakt","imdb","tmdb","tvdb") if ids.get(k))
        return score, str(x.get("rated_at") or "")
    for m in src or []:
        k = key_of(m)
        cur = dst.get(k)
        if not cur or q(m) >= q(cur):
            dst[k] = dict(m)

def _chunk_iter(lst: List[Dict[str, Any]], size: int) -> Iterable[List[Dict[str, Any]]]:
    # Simple chunker for POST payloads
    n = int(size or 0)
    if n <= 0: n = 100
    for i in range(0, len(lst), n):
        yield lst[i:i+n]

# -------------------- fetch (one-shot) --------------------

def _fetch_bucket(sess, headers, url: str, typ_hint: str, per_page: int, max_pages: int, tmo: float, rr: int) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for page in range(1, max_pages + 1):
        r = sess.get(url, headers=headers, params={"page": page, "limit": per_page}, timeout=tmo)
        if r.status_code != 200: break
        rows = r.json() or []
        if not rows: break
        for row in rows:
            val = _valid_rating(row.get("rating"))
            if not val: continue
            t = (row.get("type") or typ_hint).lower()
            ra = row.get("rated_at") or row.get("user_rated_at")

            if t == "movie" and isinstance(row.get("movie"), dict):
                m = normalize_watchlist_row({"type": "movie", "movie": row["movie"]})
            elif t == "show" and isinstance(row.get("show"), dict):
                m = normalize_watchlist_row({"type": "show", "show": row["show"]})
            elif t == "season" and isinstance(row.get("season"), dict):
                se = row["season"]; show = row.get("show") or {}
                m = id_minimal({"type": "season", "ids": se.get("ids") or {}, "show_ids": show.get("ids") or {}, "season": se.get("number"), "series_title": show.get("title")})
            elif t == "episode" and isinstance(row.get("episode"), dict):
                ep = row["episode"]; show = row.get("show") or {}
                m = id_minimal({"type": "episode", "ids": ep.get("ids") or {}, "show_ids": show.get("ids") or {}, "season": ep.get("season"), "episode": ep.get("number"), "series_title": show.get("title")})
            else:
                continue

            m["rating"] = val
            if ra: m["rated_at"] = ra
            out.append(m)

        if len(rows) < per_page: break
    return out

def _dedupe_canonical(items: Iterable[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    idx: Dict[str, Dict[str, Any]] = {}
    for m in items:
        k = key_of(m)
        cur = idx.get(k)
        if not cur:
            idx[k] = m
        else:
            # Prefer newer timestamp
            if str(m.get("rated_at") or "") >= str(cur.get("rated_at") or ""):
                idx[k] = m
    return idx

# -------------------- public: index --------------------

def build_index(adapter, *, per_page: int = 200, max_pages: int = 50) -> Dict[str, Dict[str, Any]]:
    """
    Contract for orchestrator: return {canonical_key: minimal_with_rating}.
    Strategy:
      1) If cache exists, trust it (fast path, stable -> no fake diffs).
      2) Else one-shot fetch, dedupe on canonical key, save cache, return.
    """
    # Allow config overrides (fallback to defaults if missing/empty)
    per_page  = int(getattr(adapter.cfg, "ratings_per_page",  per_page)  or per_page)
    max_pages = int(getattr(adapter.cfg, "ratings_max_pages", max_pages) or max_pages)

    # 1) trust cache if present
    cached = _load_cache()
    if cached:
        _log(f"index (cache): {len(cached)}")
        return cached

    # 2) first-time fetch
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    tmo = adapter.cfg.timeout; rr = getattr(adapter.cfg, "max_retries", 3)

    movies   = _fetch_bucket(sess, headers, URL_RAT_MOV, "movie",   per_page, max_pages, tmo, rr)
    shows    = _fetch_bucket(sess, headers, URL_RAT_SHO, "show",    per_page, max_pages, tmo, rr)
    seasons  = _fetch_bucket(sess, headers, URL_RAT_SEA, "season",  per_page, max_pages, tmo, rr)
    episodes = _fetch_bucket(sess, headers, URL_RAT_EPI, "episode", per_page, max_pages, tmo, rr)

    all_items = movies + shows + seasons + episodes
    _log(f"fetched: {len(all_items)}")
    idx = _dedupe_canonical(all_items)
    _log(f"index size: {len(idx)} (m={len(movies)}, sh={len(shows)}, se={len(seasons)}, ep={len(episodes)})")

    _save_cache(idx)
    return idx

# -------------------- public: writes --------------------

def _bucketize_for_upsert(items: Iterable[Mapping[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    """
    Build payload for /sync/ratings. Return (body, accepted_minimals_for_cache).
    NOTE: We don't resolve missing ids for episodes/seasons. Keep it simple.
    """
    body: Dict[str, List[Dict[str, Any]]] = {}
    accepted: List[Dict[str, Any]] = []

    def push(bucket: str, obj: Dict[str, Any]):
        body.setdefault(bucket, []).append(obj)

    for it in items or []:
        rating = _valid_rating(it.get("rating"))
        if rating is None:
            continue
        kind = (pick_trakt_kind(it) or "").lower()
        if not kind:
            # fallback guess
            t = (it.get("type") or "").lower()
            kind = {"movie":"movies","show":"shows","season":"seasons","episode":"episodes"}.get(t, "movies")

        ids = ids_for_trakt(it) or {}
        if not ids:
            continue  # skip if we can't identify the item minimally

        obj: Dict[str, Any] = {"ids": ids, "rating": rating}
        ra = it.get("rated_at")
        if ra: obj["rated_at"] = ra

        if   kind == "movies":   push("movies",   obj); t = "movie"
        elif kind == "shows":    push("shows",    obj); t = "show"
        elif kind == "seasons":  push("seasons",  obj); t = "season"
        else:                    push("episodes", obj); t = "episode"

        accepted.append(id_minimal({"type": t, "ids": ids, "rating": rating, "rated_at": ra}))
    return body, accepted

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Upsert ratings; merge accepted into cache so next run sees them present."""
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    tmo = adapter.cfg.timeout

    body, accepted = _bucketize_for_upsert(items)
    if not body:
        return 0, []

    # Chunked POSTs per bucket
    chunk = int(getattr(adapter.cfg, "ratings_chunk_size", 100) or 100)
    ok_total = 0
    unresolved: List[Dict[str, Any]] = []

    for bucket in ("movies", "shows", "seasons", "episodes"):
        rows = body.get(bucket) or []
        for part in _chunk_iter(rows, chunk):
            payload = {bucket: part}
            r = sess.post(URL_UPSERT, headers=headers, json=payload, timeout=tmo)
            if r.status_code in (200, 201):
                d = r.json() or {}
                added   = d.get("added")   or {}
                updated = d.get("updated") or {}
                ok_total += sum(int(added.get(k) or 0) for k in ("movies","shows","seasons","episodes")) + \
                            sum(int(updated.get(k) or 0) for k in ("movies","shows","seasons","episodes"))
            else:
                _log(f"UPSERT failed {r.status_code}: {(r.text or '')[:200]}")

    # Merge into cache (single pass)
    if ok_total > 0:
        cache = _load_cache()
        _merge_by_canonical(cache, accepted)
        _save_cache(cache)

    return ok_total, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    """Unrate; remove from cache so next run sees them gone."""
    sess = adapter.client.session
    headers = build_headers({"trakt": {"client_id": adapter.cfg.client_id, "access_token": adapter.cfg.access_token}})
    tmo = adapter.cfg.timeout

    # Build ids-only body and accepted list
    buckets: Dict[str, List[Dict[str, Any]]] = {}
    accepted_minimals: List[Dict[str, Any]] = []
    def push(bucket: str, obj: Dict[str, Any]):
        buckets.setdefault(bucket, []).append(obj)

    for it in items or []:
        kind = (pick_trakt_kind(it) or "").lower()
        ids = ids_for_trakt(it) or {}
        if not ids:
            continue
        if   kind == "movies":   push("movies",   {"ids": ids}); t = "movie"
        elif kind == "shows":    push("shows",    {"ids": ids}); t = "show"
        elif kind == "seasons":  push("seasons",  {"ids": ids}); t = "season"
        else:                    push("episodes", {"ids": ids}); t = "episode"
        accepted_minimals.append(id_minimal({"type": t, "ids": ids}))

    if not buckets:
        return 0, []

    # Chunked POSTs per bucket
    chunk = int(getattr(adapter.cfg, "ratings_chunk_size", 100) or 100)
    ok_total = 0
    unresolved: List[Dict[str, Any]] = []

    for bucket in ("movies", "shows", "seasons", "episodes"):
        rows = buckets.get(bucket) or []
        for part in _chunk_iter(rows, chunk):
            payload = {bucket: part}
            r = sess.post(URL_UNRATE, headers=headers, json=payload, timeout=tmo)
            if r.status_code in (200, 201):
                d = r.json() or {}
                deleted = d.get("deleted") or d.get("removed") or {}
                ok_total += sum(int(deleted.get(k) or 0) for k in ("movies","shows","seasons","episodes"))
            else:
                _log(f"UNRATE failed {r.status_code}: {(r.text or '')[:200]}")

    # Drop from cache (single pass)
    if ok_total > 0:
        cache = _load_cache()
        for m in accepted_minimals:
            cache.pop(key_of(m), None)
        _save_cache(cache)

    return ok_total, unresolved

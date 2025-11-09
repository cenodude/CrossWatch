# /providers/sync/mdblist/_ratings.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .._mod_common import request_with_retries
try:
    from cw_platform.id_map import minimal as id_minimal
except Exception:
    from _id_map import minimal as id_minimal  # type: ignore

BASE = "https://api.mdblist.com"
# switched to /sync endpoints
URL_LIST   = f"{BASE}/sync/ratings"          # GET (paginated)
URL_UPSERT = f"{BASE}/sync/ratings"          # POST add/update
URL_UNRATE = f"{BASE}/sync/ratings/remove"   # POST remove

CACHE_PATH = Path("/config/.cw_state/mdblist_ratings.index.json")
CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST:ratings] {msg}")

# ---------- cfg helpers ----------
def _cfg(adapter) -> Mapping[str, Any]:
    c = getattr(adapter, "config", {}) or {}
    if isinstance(c, dict) and isinstance(c.get("mdblist"), dict):
        return c["mdblist"]
    try:
        rc = getattr(getattr(adapter, "cfg", None), "config", {}) or {}
        if isinstance(rc, dict) and isinstance(rc.get("mdblist"), dict):
            return rc["mdblist"]
    except Exception:
        pass
    return {}

def _cfg_int(d: Mapping[str, Any], key: str, default: int) -> int:
    try: return int(d.get(key, default))
    except Exception: return default

def _cfg_float(d: Mapping[str, Any], key: str, default: float) -> float:
    try: return float(d.get(key, default))
    except Exception: return default

# ---------- ids / kind / key ----------
def _ids_for_mdblist(it: Mapping[str, Any]) -> Dict[str, Any]:
    ids = dict((it.get("ids") or {}))
    if not ids:
        ids = {
            "imdb": it.get("imdb") or it.get("imdb_id"),
            "tmdb": it.get("tmdb") or it.get("tmdb_id"),
            "tvdb": it.get("tvdb") or it.get("tvdb_id"),
        }
    out = {}
    if ids.get("imdb"): out["imdb"] = str(ids["imdb"])
    if ids.get("tmdb"): out["tmdb"] = int(ids["tmdb"])
    if ids.get("tvdb"): out["tvdb"] = int(ids["tvdb"])
    return out

def _pick_kind(it: Mapping[str, Any]) -> str:
    t = (it.get("type") or it.get("mediatype") or "").strip().lower()
    if t in ("movie","movies"): return "movies"
    if t in ("show","tv","series","shows"): return "shows"
    if str((it.get("movie") or "")).lower() == "true": return "movies"
    if str((it.get("show") or "")).lower()  == "true": return "shows"
    return "movies"

def _key_of(obj: Mapping[str, Any]) -> str:
    ids = dict((obj.get("ids") or obj) or {})
    imdb = (ids.get("imdb") or ids.get("imdb_id") or "").strip()
    if imdb: return f"imdb:{imdb}"
    tmdb = ids.get("tmdb") or ids.get("tmdb_id")
    if tmdb: return f"tmdb:{int(tmdb)}"
    tvdb = ids.get("tvdb") or ids.get("tvdb_id")
    if tvdb: return f"tvdb:{int(tvdb)}"
    mdbl = ids.get("mdblist") or ids.get("id")
    if mdbl: return f"mdblist:{mdbl}"
    t = (obj.get("title") or "").strip(); y = obj.get("year")
    return f"title:{t}|year:{y}" if t and y else f"obj:{hash(json.dumps(obj, sort_keys=True)) & 0xffffffff}"

# ---------- rating helpers ----------
def _valid_rating(v: Any) -> Optional[int]:
    try:
        i = int(str(v).strip())
        return i if 1 <= i <= 10 else None
    except Exception:
        return None

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

# ---------- cache ----------
def _load_cache() -> Dict[str, Any]:
    try:
        if not CACHE_PATH.exists(): return {}
        doc = json.loads(CACHE_PATH.read_text("utf-8") or "{}")
        return dict(doc.get("items") or {})
    except Exception:
        return {}

def _save_cache(items: Mapping[str, Any]) -> None:
    try:
        doc = {"generated_at": _now_iso(), "items": dict(items)}
        tmp = CACHE_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, CACHE_PATH)
        _log(f"cache.saved -> {CACHE_PATH} ({len(items)})")
    except Exception as e:
        _log(f"cache.save failed: {e}")

def _merge_by_key(dst: Dict[str, Any], src: Iterable[Mapping[str, Any]]) -> None:
    for m in src or []:
        k = _key_of(m)
        cur = dst.get(k)
        if not cur:
            dst[k] = dict(m)
        else:
            a = str(m.get("rated_at") or "")
            b = str(cur.get("rated_at") or "")
            if a >= b:
                dst[k] = dict(m)

# ---------- fetch index (patched for /sync/ratings) ----------
def _row_movie(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None: return None
        ids = row.get("movie", {}).get("ids") or {}
        ids = {"imdb": ids.get("imdb"), "tmdb": ids.get("tmdb"), "tvdb": ids.get("tvdb")}
        ids = {k:v for k,v in ids.items() if v}
        if not ids: return None
        out = {"type": "movie", "ids": ids, "rating": rating}
        if row.get("rated_at"): out["rated_at"] = row["rated_at"]
        return out
    except Exception:
        return None

def _row_show(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None: return None
        ids = row.get("show", {}).get("ids") or {}
        ids = {"imdb": ids.get("imdb"), "tmdb": ids.get("tmdb"), "tvdb": ids.get("tvdb")}
        ids = {k:v for k,v in ids.items() if v}
        if not ids: return None
        out = {"type": "show", "ids": ids, "rating": rating}
        if row.get("rated_at"): out["rated_at"] = row["rated_at"]
        return out
    except Exception:
        return None

def build_index(adapter, *, per_page: int = 1000, max_pages: int = 9999) -> Dict[str, Dict[str, Any]]:
    c = _cfg(adapter)
    apikey = str(c.get("api_key") or "").strip()
    if not apikey:
        _log("missing api_key → empty ratings index")
        _save_cache({})
        return {}

    per_page  = int(c.get("ratings_per_page")  or per_page)

    sess = adapter.client.session
    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    out: Dict[str, Dict[str, Any]] = {}
    page = 1
    pages = 0

    while True:
        r = request_with_retries(
            sess, "GET", URL_LIST,
            params={"apikey": apikey, "page": page, "limit": per_page},
            timeout=timeout, max_retries=retries
        )
        if r.status_code != 200:
            _log(f"GET /sync/ratings page {page} -> {r.status_code}")
            break
        data = r.json() if (r.text or "").strip() else {}
        movies = data.get("movies") or []
        shows  = data.get("shows") or []

        minis: List[Dict[str, Any]] = []
        for row in movies:
            m = _row_movie(row)
            if m: minis.append(m)
        for row in shows:
            m = _row_show(row)
            if m: minis.append(m)

        for m in minis:
            out[_key_of(m)] = m

        pag = data.get("pagination") or {}
        has_more = bool(pag.get("has_more"))
        pages += 1
        if not has_more or pages >= max_pages:
            break
        page += 1

    _save_cache(out)
    _log(f"index size: {len(out)}")
    return out

# ---------- writes ----------
def _chunk(seq: List[Any], n: int) -> Iterable[List[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _bucketize(items: Iterable[Mapping[str, Any]]) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    body: Dict[str, List[Dict[str, Any]]] = {}
    accepted: List[Dict[str, Any]] = []

    def push(bucket: str, obj: Dict[str, Any]):
        body.setdefault(bucket, []).append(obj)

    for it in items or []:
        rating = _valid_rating(it.get("rating"))
        if rating is None:
            continue
        ids = _ids_for_mdblist(it) or {}
        if not ids:
            continue
        kind = _pick_kind(it)
        obj = {"ids": ids, "rating": rating}  # nested ids for /sync/ratings
        ra = it.get("rated_at")
        if ra: obj["rated_at"] = ra
        if kind == "movies":
            push("movies", obj); t = "movie"
        else:
            push("shows", obj);  t = "show"
        accepted.append(id_minimal({"type": t, "ids": ids, "rating": rating, "rated_at": ra}))
    return body, accepted

def _write(adapter, items: Iterable[Mapping[str, Any]], *, unrate: bool = False) -> Tuple[int, List[Dict[str, Any]]]:
    c = _cfg(adapter)
    apikey = str(c.get("api_key") or "").strip()
    if not apikey:
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]

    sess = adapter.client.session
    tmo  = adapter.cfg.timeout
    rr   = adapter.cfg.max_retries

    chunk = _cfg_int(c, "ratings_chunk_size", 25)
    delay_ms = _cfg_int(c, "ratings_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(c, "ratings_max_backoff_ms", 8000)

    body, accepted = _bucketize(items)
    if not body:
        return 0, []

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    for bucket in ("movies", "shows"):
        rows = body.get(bucket) or []
        for part in _chunk(rows, chunk):
            payload = {bucket: part}
            url = URL_UNRATE if unrate else URL_UPSERT

            attempt = 0
            backoff = delay_ms
            while True:
                r = request_with_retries(
                    sess, "POST", url,
                    params={"apikey": apikey},
                    json=payload,
                    timeout=tmo, max_retries=rr
                )
                if r.status_code in (200, 201):
                    d = r.json() if (r.text or "").strip() else {}
                    if unrate:
                        removed = d.get("removed") or {}
                        ok += int(removed.get("movies") or 0) + int(removed.get("shows") or 0) \
                              + int(removed.get("seasons") or 0) + int(removed.get("episodes") or 0)
                    else:
                        updated = d.get("updated") or {}
                        delta = int(updated.get("movies") or 0) + int(updated.get("shows") or 0) \
                                + int(updated.get("seasons") or 0) + int(updated.get("episodes") or 0)
                        if delta == 0:
                            # fallback for any older/alternative shapes
                            added    = d.get("added") or {}
                            existing = d.get("existing") or {}
                            ok += int(added.get("movies") or 0) + int(added.get("shows") or 0) \
                                  + int(existing.get("movies") or 0) + int(existing.get("shows") or 0)
                        else:
                            ok += delta
                    time.sleep(max(0.0, delay_ms/1000.0))
                    break

                if r.status_code in (429, 503):
                    _log(f"{'UNRATE' if unrate else 'UPSERT'} throttled {r.status_code}: {(r.text or '')[:180]}")
                    time.sleep(min(max_backoff_ms, backoff)/1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff*1.6) + 200)
                    if attempt <= 4:
                        continue

                _log(f"{'UNRATE' if unrate else 'UPSERT'} failed {r.status_code}: {(r.text or '')[:200]}")
                for x in part:
                    iid = x.get("ids") or {}
                    t = "movie" if bucket == "movies" else "show"
                    unresolved.append({"item": id_minimal({"type": t, "ids": iid}), "hint": f"http:{r.status_code}"})
                break

    if ok > 0 and not unrate:
        cache = _load_cache()
        _merge_by_key(cache, accepted)
        _save_cache(cache)
    if ok > 0 and unrate:
        cache = _load_cache()
        for it in accepted:
            cache.pop(_key_of(it), None)
        _save_cache(cache)

    return ok, unresolved

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    return _write(adapter, items, unrate=False)

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    return _write(adapter, items, unrate=True)
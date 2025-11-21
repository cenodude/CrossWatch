# /providers/sync/simkl/_ratings.py
#  Simkl ratings sync module
#  Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timezone

from ._common import (
    build_headers,
    normalize as simkl_normalize,
    key_of as simkl_key_of,
    fetch_activities,
    extract_latest_ts,
    coalesce_date_from,
    update_watermark_if_new,
    get_watermark,
)
try:
    from cw_platform.id_map import minimal as id_minimal
except Exception:
    from _id_map import minimal as id_minimal  # type: ignore

BASE        = "https://api.simkl.com"
URL_GET_T   = lambda t, qs="": f"{BASE}/sync/ratings/{t}{qs}"
URL_ADD     = f"{BASE}/sync/ratings"
URL_REMOVE  = f"{BASE}/sync/ratings/remove"

RATINGS_KINDS = ("movies", "shows")

STATE_DIR        = Path("/config/.cw_state")
UNRESOLVED_PATH  = str(STATE_DIR / "simkl_ratings.unresolved.json")
R_SHADOW_PATH    = str(STATE_DIR / "simkl.ratings.shadow.json")

ID_KEYS = ("simkl","imdb","tmdb","tvdb")

def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_SIMKL_DEBUG"):
        print(f"[SIMKL:ratings] {msg}")

def _headers(adapter, *, force_refresh: bool = False) -> Dict[str, str]:
    return build_headers({"simkl": {"api_key": adapter.cfg.api_key, "access_token": adapter.cfg.access_token}}, force_refresh=force_refresh)

def _ids_of(it: Mapping[str, Any]) -> Dict[str, Any]:
    src = dict(it.get("ids") or {})
    return {k: src[k] for k in ID_KEYS if src.get(k)}

def _show_ids_of_episode(it: Mapping[str, Any]) -> Dict[str, Any]:
    sids = dict(it.get("show_ids") or {})
    return {k: sids[k] for k in ID_KEYS if sids.get(k)}

def _norm_rating(v: Any) -> Optional[int]:
    try:
        n = int(round(float(v)))
    except Exception:
        return None
    return n if 1 <= n <= 10 else None

def _now() -> int: return int(time.time())

def _as_epoch(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    if isinstance(v, datetime): return int((v if v.tzinfo else v.replace(tzinfo=timezone.utc)).timestamp())
    if isinstance(v, str):
        s = v.strip()
        if s.isdigit():
            try:
                n = int(s); return n // 1000 if len(s) >= 13 else n
            except Exception:
                return None
        try: return int(datetime.fromisoformat(s.replace("Z","+00:00")).timestamp())
        except Exception: return None
    return None

def _as_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00","Z")

def _load_json(path: str) -> Dict[str, Any]:
    try: return json.loads(Path(path).read_text("utf-8"))
    except Exception: return {}

def _save_json(path: str, data: Mapping[str, Any]) -> None:
    try:
        p = Path(path); p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as e:
        _log(f"save {Path(path).name} failed: {e}")

def _load_unresolved() -> Dict[str, Any]: return _load_json(UNRESOLVED_PATH)
def _save_unresolved(data: Mapping[str, Any]) -> None: _save_json(UNRESOLVED_PATH, data)

def _is_frozen(item: Mapping[str, Any]) -> bool:
    key = simkl_key_of(id_minimal(item))
    return key in _load_unresolved()

def _freeze(item: Mapping[str, Any], *, action: str, reasons: List[str], ids_sent: Dict[str, Any], rating: Optional[int]) -> None:
    key = simkl_key_of(id_minimal(item))
    data = _load_unresolved()
    row = data.get(key) or {"feature": "ratings", "action": action, "first_seen": _now(), "attempts": 0}
    row.update({"item": id_minimal(item), "last_attempt": _now()})
    row["reasons"] = sorted(set(row.get("reasons", [])) | set(reasons or []))
    row["ids_sent"] = dict(ids_sent or {})
    if rating is not None: row["rating"] = int(rating)
    row["attempts"] = int(row.get("attempts", 0)) + 1
    data[key] = row; _save_unresolved(data)

def _unfreeze_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in set(keys or []):
        if k in data: del data[k]; changed = True
    if changed: _save_unresolved(data)

def _rshadow_ttl_seconds() -> int:
    try: return int(os.getenv("CW_SIMKL_RATINGS_SHADOW_TTL", str(7*24*3600)))
    except Exception: return 7*24*3600

def _rshadow_load() -> Dict[str, Any]: return _load_json(R_SHADOW_PATH) or {"items": {}}
def _rshadow_save(obj: Mapping[str, Any]) -> None: _save_json(R_SHADOW_PATH, obj)

def _rshadow_put_all(items: Iterable[Mapping[str, Any]]) -> None:
    items = list(items or [])
    if not items: return
    sh = _rshadow_load(); store = dict(sh.get("items") or {})
    now = _now(); ttl = _rshadow_ttl_seconds()
    for it in items:
        bk = simkl_key_of(id_minimal(it))
        rt = _norm_rating(it.get("rating"))
        ra = it.get("rated_at") or it.get("ratedAt") or ""
        ts = _as_epoch(ra) or now
        if not bk or rt is None: continue
        old = store.get(bk) or {}
        old_ts = _as_epoch(old.get("rated_at")) or 0
        if ts >= old_ts:
            store[bk] = {"item": id_minimal(it), "rating": rt, "rated_at": ra or _as_iso(ts), "exp": now + ttl}
    sh["items"] = store; _rshadow_save(sh)

def _rshadow_merge_into(out: Dict[str, Dict[str, Any]], thaw: set) -> None:
    sh = _rshadow_load(); store = dict(sh.get("items") or {})
    if not store: return
    now = _now(); changed = False; merged = 0; cleaned = 0
    for bk, rec in list(store.items()):
        exp = int(rec.get("exp") or 0)
        if exp and exp < now:
            del store[bk]; changed = True; cleaned += 1; continue
        rec_rt = _norm_rating(rec.get("rating")); rec_ra = rec.get("rated_at") or ""
        if rec_rt is None: 
            del store[bk]; changed = True; cleaned += 1; continue
        rec_ts = _as_epoch(rec_ra) or 0
        cur = out.get(bk)
        if not cur:
            m = dict(id_minimal(rec.get("item") or {})); m["rating"] = rec_rt; m["rated_at"] = rec_ra
            out[bk] = m; thaw.add(bk); merged += 1
            continue
        cur_rt = _norm_rating(cur.get("rating"))
        cur_ts = _as_epoch(cur.get("rated_at")) or 0
        if (cur_rt == rec_rt) and (cur_ts >= rec_ts):
            del store[bk]; changed = True; cleaned += 1; continue
        if rec_ts > cur_ts:
            m = dict(cur); m["rating"] = rec_rt; m["rated_at"] = rec_ra
            out[bk] = m; merged += 1
    if merged: _log(f"shadow merged {merged} rating items")
    if cleaned or changed:
        sh["items"] = store; _rshadow_save(sh)

def build_index(adapter, *, since_iso: Optional[str] = None) -> Dict[str, Dict[str, Any]]:
    sess = adapter.client.session
    tmo  = adapter.cfg.timeout

    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("ratings") if callable(prog_mk) else None

    if since_iso is None:
        acts, _ = fetch_activities(sess, _headers(adapter, force_refresh=True), timeout=tmo)
        if isinstance(acts, Mapping):
            wm_m = get_watermark("ratings:movies") or ""
            wm_s = get_watermark("ratings:shows")  or ""
            lm = extract_latest_ts(acts, (("movies","rated"), ("ratings","movies"), ("movies","all")))
            ls = extract_latest_ts(acts, (("shows","rated"),  ("ratings","shows"),  ("shows","all")))

            if (lm is None or lm <= wm_m) and (ls is None or ls <= wm_s):
                _log(f"activities unchanged; ratings noop (m={lm} s={ls})")
                if prog:
                    try: prog.done(ok=True, total=0)
                    except Exception: pass
                return {}

    hdrs = _headers(adapter, force_refresh=True)
    out: Dict[str, Dict[str, Any]] = {}
    thaw: set[str] = set()

    df_movies = coalesce_date_from("ratings:movies", cfg_date_from=since_iso)
    df_shows  = coalesce_date_from("ratings:shows",  cfg_date_from=since_iso)

    def _fetch_rows(kind: str, df_iso: str) -> List[Mapping[str, Any]]:
        try:
            r = sess.post(URL_GET_T(kind, f"?date_from={df_iso}"), headers=hdrs, timeout=tmo)
            if r.status_code in (404, 405):
                r = sess.get(URL_GET_T(kind, f"?date_from={df_iso}"), headers=hdrs, timeout=tmo)
            if r.status_code != 200:
                _log(f"ratings/{kind} -> {r.status_code}")
                return []
            data = r.json() or {}
            rows = data.get(kind) if isinstance(data, dict) else (data if isinstance(data, list) else [])
            return rows if isinstance(rows, list) else []
        except Exception as e:
            _log(f"fetch {kind} error: {e}")
            return []

    rows_movies = _fetch_rows("movies", df_movies)
    rows_shows  = _fetch_rows("shows",  df_shows)
    grand_total = len(rows_movies) + len(rows_shows)
    if prog:
        try: prog.tick(0, total=grand_total, force=True)
        except Exception: pass

    done = 0
    max_movies: Optional[int] = None
    max_shows:  Optional[int] = None

    def _ingest(kind: str, rows: List[Mapping[str, Any]]) -> Optional[int]:
        nonlocal done, max_movies, max_shows
        latest: Optional[int] = None
        for row in rows:
            rt = _norm_rating(row.get("user_rating") if "user_rating" in row else row.get("rating"))
            if rt is None:
                done += 1
                if prog:
                    try: prog.tick(done, total=grand_total)
                    except Exception: pass
                continue
            media = (row.get("movie") or row.get("show") or {})
            m = simkl_normalize(media)
            m["rating"]   = rt
            m["rated_at"] = row.get("user_rated_at") or row.get("rated_at") or ""
            k = simkl_key_of(m)
            out[k] = m; thaw.add(k)
            ts = _as_epoch(m.get("rated_at"))
            if ts is not None: latest = max(latest or 0, ts)
            done += 1
            if prog:
                try: prog.tick(done, total=grand_total)
                except Exception: pass
        if kind == "movies": max_movies = latest
        elif kind == "shows": max_shows = latest
        return latest

    _ingest("movies", rows_movies)
    _ingest("shows",  rows_shows)
    _rshadow_merge_into(out, thaw)

    if prog:
        try: prog.done(ok=True, total=grand_total)
        except Exception: pass

    _log(f"counts movies={len(rows_movies)} shows={len(rows_shows)} from={df_movies}|{df_shows}")
    if max_movies is not None:
        update_watermark_if_new("ratings:movies", _as_iso(max_movies))
    if max_shows is not None:
        update_watermark_if_new("ratings:shows", _as_iso(max_shows))

    latest_any = max(
        [t for t in (max_movies, max_shows) if isinstance(t, int)] or [None]
    ) if any(x is not None for x in (max_movies, max_shows)) else None

    if isinstance(latest_any, int):
        update_watermark_if_new("ratings", _as_iso(latest_any))
    _unfreeze_if_present(thaw)
    try:
        _rshadow_put_all(out.values())
    except Exception as e:
        _log(f"shadow.put index skipped: {e}")

    _log(f"index size: {len(out)}")
    return out

def _movie_entry_add(it: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    ids = _ids_of(it); rating = _norm_rating(it.get("rating"))
    if not ids or rating is None: return None
    ent: Dict[str, Any] = {"ids": ids, "rating": rating}
    ra = (it.get("rated_at") or it.get("ratedAt") or "").strip()
    if ra: ent["rated_at"] = ra
    return ent

def _show_entry_add(it: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    ids = _ids_of(it); rating = _norm_rating(it.get("rating"))
    if not ids or rating is None: return None
    ent: Dict[str, Any] = {"ids": ids, "rating": rating}
    ra = (it.get("rated_at") or it.get("ratedAt") or "").strip()
    if ra: ent["rated_at"] = ra
    return ent

def _episode_entry_add(it: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    sids = _show_ids_of_episode(it); rating = _norm_rating(it.get("rating"))
    s_num = int(it.get("season") or it.get("season_number") or 0)
    e_num = int(it.get("episode") or it.get("episode_number") or 0)
    if not sids or rating is None or not s_num or not e_num: return None
    ent: Dict[str, Any] = {"ids": sids, "season": s_num, "episode": e_num, "rating": rating}
    ra = (it.get("rated_at") or it.get("ratedAt") or "").strip()
    if ra: ent["rated_at"] = ra
    return ent

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess, hdrs = adapter.client.session, _headers(adapter)
    movies: List[Dict[str, Any]] = []; shows: List[Dict[str, Any]] = []; episodes: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []; thaw_keys: List[str] = []
    rshadow_events: List[Dict[str, Any]] = []

    for it in items or []:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}"); continue
        typ = (it.get("type") or "movie").lower()
        if typ == "movie":
            ent = _movie_entry_add(it)
            if ent:
                movies.append(ent); thaw_keys.append(simkl_key_of(id_minimal(it)))
                ev = dict(id_minimal(it)); ev["rating"] = ent["rating"]; ev["rated_at"] = ent.get("rated_at",""); rshadow_events.append(ev)
            else:
                unresolved.append({"item": id_minimal(it), "hint": "missing_ids_or_rating"})
        elif typ in ("episode","season"):
            unresolved.append({"item": id_minimal(it), "hint": "unsupported_type"})
        else:
            ent = _show_entry_add(it)
            if ent:
                shows.append(ent); thaw_keys.append(simkl_key_of(id_minimal(it)))
                ev = dict(id_minimal(it)); ev["rating"] = ent["rating"]; ev["rated_at"] = ent.get("rated_at",""); rshadow_events.append(ev)
            else:
                unresolved.append({"item": id_minimal(it), "hint": "missing_ids_or_rating"})

    if not (movies or shows or episodes):
        return 0, unresolved
    body: Dict[str, Any] = {}
    if movies:   body["movies"]   = movies
    if shows:    body["shows"]    = shows
    if episodes: body["episodes"] = episodes

    try:
        r = sess.post(URL_ADD, headers=hdrs, json=body, timeout=adapter.cfg.timeout)
        if 200 <= r.status_code < 300:
            _unfreeze_if_present(thaw_keys)
            ok = len(movies) + len(shows) + len(episodes)
            _log(f"add done: +{ok}")
            try: _rshadow_put_all(rshadow_events)
            except Exception as e: _log(f"shadow.put skipped: {e}")
            return ok, unresolved
        _log(f"ADD failed {r.status_code}: {(r.text or '')[:180]}")
    except Exception as e:
        _log(f"ADD error: {e}")

    for it in items or []:
        ids = _ids_of(it) or _show_ids_of_episode(it)
        rating = _norm_rating(it.get("rating"))
        if ids and rating is not None:
            _freeze(it, action="add", reasons=["write_failed"], ids_sent=ids, rating=rating)
    return 0, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess, hdrs = adapter.client.session, _headers(adapter)
    movies: List[Dict[str, Any]] = []
    shows: List[Dict[str, Any]] = []
    episodes: List[Dict[str, Any]] = []
    unresolved: List[Dict[str, Any]] = []
    thaw_keys: List[str] = []

    for it in items or []:
        if _is_frozen(it):
            _log(f"skip frozen: {id_minimal(it).get('title')}")
            continue

        ids = _ids_of(it) or _show_ids_of_episode(it)
        if not ids:
            unresolved.append({"item": id_minimal(it), "hint": "missing_ids"})
            continue

        typ = (it.get("type") or "movie").lower()
        if typ == "movie":
            movies.append({"ids": ids})
        elif typ in ("episode", "season"):
            unresolved.append({"item": id_minimal(it), "hint": "unsupported_type"})
            continue
        else:
            shows.append({"ids": ids})

        thaw_keys.append(simkl_key_of(id_minimal(it)))

    if not (movies or shows or episodes):
        return 0, unresolved

    body: Dict[str, Any] = {}
    if movies:   body["movies"]   = movies
    if shows:    body["shows"]    = shows
    if episodes: body["episodes"] = episodes

    try:
        r = sess.post(URL_REMOVE, headers=hdrs, json=body, timeout=adapter.cfg.timeout)
        if 200 <= r.status_code < 300:
            _unfreeze_if_present(thaw_keys)
            try:
                sh = _rshadow_load()
                store = dict(sh.get("items") or {})
                changed = False
                for k in thaw_keys:
                    if k in store:
                        store.pop(k, None)
                        changed = True
                if changed:
                    sh["items"] = store
                    _rshadow_save(sh)
            except Exception:
                pass

            ok = len(movies) + len(shows) + len(episodes)
            _log(f"remove done: -{ok}")
            return ok, unresolved

        _log(f"REMOVE failed {r.status_code}: {(r.text or '')[:180]}")
    except Exception as e:
        _log(f"REMOVE error: {e}")

    for it in items or []:
        ids = _ids_of(it) or _show_ids_of_episode(it)
        if ids:
            _freeze(it, action="remove", reasons=["write_failed"], ids_sent=ids, rating=None)
    return 0, unresolved
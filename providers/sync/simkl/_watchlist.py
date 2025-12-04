# /providers/sync/simkl/_watchlist.py
#  Simkl watchlist synchronization logic.
#  Copyright (c) 2025 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)

from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from ._common import (
    build_headers,
    key_of as simkl_key_of,
    fetch_activities,
    save_watermark,
    get_watermark,
    coalesce_date_from,
    normalize as simkl_normalize,
)

from cw_platform.id_map import minimal as id_minimal

BASE = "https://api.simkl.com"
URL_INDEX_BUCKET = f"{BASE}/sync/all-items/{{bucket}}/plantowatch"
URL_INDEX_IDS    = f"{BASE}/sync/all-items/{{bucket}}/plantowatch"
URL_ADD          = f"{BASE}/sync/add-to-list"
URL_REMOVE       = f"{BASE}/sync/history/remove"
URL_SEARCH_ID    = f"{BASE}/search/id"

STATE_DIR       = Path("/config/.cw_state")
UNRESOLVED_PATH = STATE_DIR / "simkl.watchlist.unresolved.json"
SHADOW_PATH     = STATE_DIR / "simkl.watchlist.shadow.json"

WATCHLIST_BUCKETS = ("movies", "shows")

_ENRICH_MEMO: Dict[str, Dict[str, Any]] = {}

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_SIMKL_DEBUG"):
        print(f"[SIMKL:watchlist] {msg}")

# ---------- unresolved

def _load_unresolved() -> Dict[str, Any]:
    try: return json.loads(UNRESOLVED_PATH.read_text("utf-8"))
    except Exception: return {}

def _save_unresolved(data: Mapping[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = UNRESOLVED_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, UNRESOLVED_PATH)
    except Exception as e:
        _log(f"unresolved.save failed: {e}")

def _freeze(item: Mapping[str, Any], *, action: str, reasons: List[str], ids_sent: Dict[str, Any]) -> None:
    key = simkl_key_of(id_minimal(item))
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "watchlist", "action": action, "first_seen": int(time.time()), "attempts": 0}
    entry.update({"item": id_minimal(item), "last_attempt": int(time.time())})
    entry["reasons"] = sorted(set(entry.get("reasons", [])) | set(reasons or []))
    entry["ids_sent"] = dict(ids_sent or {})
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)

def _unfreeze_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]; changed = True
    if changed: _save_unresolved(data)

# ---------- shadow

def _shadow_load() -> Dict[str, Any]:
    try: return json.loads(SHADOW_PATH.read_text("utf-8"))
    except Exception: return {"ts": None, "items": {}}

def _shadow_save(ts: Optional[str], items: Mapping[str, Any]) -> None:
    try:
        STATE_DIR.mkdir(parents=True, exist_ok=True)
        tmp = SHADOW_PATH.with_suffix(".tmp")
        tmp.write_text(json.dumps({"ts": ts, "items": dict(items)}, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, SHADOW_PATH)
    except Exception:
        pass

def _shadow_age_seconds() -> float:
    try: return max(0.0, time.time() - SHADOW_PATH.stat().st_mtime)
    except Exception: return 1e9

def _shadow_ttl_seconds() -> float:
    try:
        v = os.getenv("CW_SIMKL_SHADOW_TTL") or "300"
        return float(v)
    except Exception:
        return 300.0

# ---------- helpers

_ALLOWED_ID_KEYS = ("simkl", "imdb", "tmdb", "tvdb")

def _ids_filter(ids_in: Mapping[str, Any]) -> Dict[str, Any]:
    return {k: ids_in.get(k) for k in _ALLOWED_ID_KEYS if ids_in.get(k)}

def _kind_group(it: Mapping[str, Any]) -> str:
    t = str(it.get("type") or "movie").lower()
    return "movies" if t in ("movie", "movies") else "shows"

def _sum_processed_from_body(body: Any) -> int:
    try:
        if not isinstance(body, dict): return 0
        total = 0
        for k in ("movies", "shows"):
            v = body.get(k)
            if isinstance(v, list): total += len(v)
            elif isinstance(v, int): total += v
        if total: return total
        for parent in ("added", "removed", "deleted"):
            sub = body.get(parent)
            if isinstance(sub, dict):
                s = 0
                for k in ("movies", "shows"):
                    v = sub.get(k)
                    if isinstance(v, int): s += v
                    elif isinstance(v, list): s += len(v)
                if s: return s
        return 0
    except Exception:
        return 0

def _rows_from_data(data: Any, bucket: str) -> List[Any]:
    if data is None:
        return []
    if isinstance(data, dict) and data.get("type") == "null" and data.get("body") is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data["items"]
        key = "movies" if bucket == "movies" else "shows"
        arr = data.get(key)
        if isinstance(arr, list):
            return arr
    return []

def _normalize_row(bucket: str, row: Any) -> Dict[str, Any]:
    if isinstance(row, Mapping):
        node = (row.get("movie") or row.get("show") or row or {})
        ids_src = node.get("ids") if isinstance(node.get("ids"), Mapping) else row
        ids = _ids_filter(dict(ids_src or {}))
        title = node.get("title")
        year = node.get("year")
    elif isinstance(row, (int, str)):
        ids = {"simkl": str(row)}
        title = None
        year = None
    else:
        ids = {}
        title = None
        year = None

    media_type = "movie" if bucket == "movies" else "show"
    return {"type": media_type, "title": title, "year": year, "ids": ids, "simkl_bucket": bucket}

def _acts_get(d: Mapping[str, Any], *path: str) -> Optional[str]:
    cur: Any = d
    for k in path:
        if isinstance(cur, Mapping) and k in cur: cur = cur[k]
        else: return None
    return str(cur) if isinstance(cur, str) else None

def _first(*vals: Optional[str]) -> Optional[str]:
    for v in vals:
        if v: return v
    return None

def _bucket_ts(acts: Mapping[str, Any]) -> Dict[str, Dict[str, Optional[str]]]:
    out = {
        "movies": {"ptw": None, "rm": None},
        "shows":  {"ptw": None, "rm": None},
    }
    out["movies"]["ptw"] = _first(
        _acts_get(acts, "movies", "plantowatch"),
        _acts_get(acts, "movies", "all"),
    )
    out["movies"]["rm"] = _first(
        _acts_get(acts, "movies", "removed_from_list"),
        _acts_get(acts, "movies", "removed"),
    )
    out["shows"]["ptw"] = _first(
        _acts_get(acts, "shows", "plantowatch"),
        _acts_get(acts, "shows", "all"),
        _acts_get(acts, "tv_shows", "plantowatch"),
        _acts_get(acts, "watchlist", "shows"),
    )
    out["shows"]["rm"] = _first(
        _acts_get(acts, "shows", "removed_from_list"),
        _acts_get(acts, "shows", "removed"),
        _acts_get(acts, "tv_shows", "removed_from_list"),
        _acts_get(acts, "tv_shows", "removed"),
    )
    return out

def _bucket_present(items: Mapping[str, Dict[str, Any]], bucket: str) -> bool:
    for v in (items or {}).values():
        if isinstance(v, Mapping) and v.get("simkl_bucket") == bucket:
            return True
    return False

def _has_all_buckets(items) -> bool:
    return all(_bucket_present(items, b) for b in WATCHLIST_BUCKETS)

# ---------- JIT enrichment

def _headers(adapter, *, force_refresh: bool = False) -> Dict[str, str]:
    h = build_headers({"simkl": {"api_key": adapter.cfg.api_key, "access_token": adapter.cfg.access_token}})
    if force_refresh:
        h.pop("If-None-Match", None)
        h["Cache-Control"] = "no-cache"
    return h

def _best_id_q(ids: Mapping[str, Any]) -> Optional[Dict[str, str]]:
    order = ("imdb", "tmdb", "tvdb", "simkl")
    for k in order:
        v = ids.get(k)
        if v:
            return {k: str(v)}
    return None

def _lookup_by_id(adapter, ids: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    q = _best_id_q(ids or {})
    if not q:
        return None
    memo_key = json.dumps(q, sort_keys=True)
    if memo_key in _ENRICH_MEMO:
        return _ENRICH_MEMO[memo_key]
    try:
        r = adapter.client.session.get(
            URL_SEARCH_ID,
            headers=_headers(adapter),
            params=q,
            timeout=min(6.0, adapter.cfg.timeout),
        )
        if 200 <= r.status_code < 300 and (r.text or "").strip():
            data = r.json()
            m = simkl_normalize(
                data if isinstance(data, Mapping) else (data[0] if isinstance(data, list) and data else {})
            )
            out = {
                "title": m.get("title"),
                "year": m.get("year"),
                "ids": _ids_filter(dict((m.get("ids") or {}))),
            }
            if out.get("title") or out.get("ids"):
                _ENRICH_MEMO[memo_key] = out
                return out
    except Exception:
        pass
    return None

def _merge_upsert(dst: Dict[str, Dict[str, Any]], src: Mapping[str, Dict[str, Any]]) -> None:
    for k, v in (src or {}).items():
        cur = dict(dst.get(k) or {})
        title = cur.get("title") or v.get("title")
        year  = cur.get("year")  or v.get("year")
        ids_a = dict(cur.get("ids") or {})
        ids_b = dict(v.get("ids") or {})
        ids = {**ids_a, **{kk: vv for kk, vv in ids_b.items() if vv}}
        bucket = v.get("simkl_bucket") or cur.get("simkl_bucket")
        typ = v.get("type") or cur.get("type")
        dst[k] = {"type": typ, "title": title, "year": year, "ids": ids, "simkl_bucket": bucket}

def _jit_enrich_missing(adapter, items: Dict[str, Dict[str, Any]], *, cap: int = None) -> int:
    limit = int(os.getenv("CW_SIMKL_ENRICH_LIMIT") or 6) if cap is None else int(cap)
    if limit <= 0:
        return 0

    enriched = 0
    for k, v in list(items.items()):
        if enriched >= limit:
            break

        ids = v.get("ids") or {}
        if v.get("title") and v.get("year") and any(ids.get(x) for x in ("imdb", "tmdb", "tvdb")):
            continue

        out = _lookup_by_id(adapter, ids)
        if out:
            v2 = dict(v)
            v2["title"] = v2.get("title") or out.get("title")
            v2["year"] = v2.get("year") or out.get("year")
            if out.get("ids"):
                ids_a = dict(v2.get("ids") or {})
                ids_b = dict(out["ids"])
                v2["ids"] = {**ids_a, **{kk: vv for kk, vv in ids_b.items() if vv}}
            items[k] = v2
            enriched += 1

    if enriched:
        _log(f"jit-enriched items: {enriched}")
    return enriched

# ---------- write-response → shadow helpers

def _keys_from_write_resp(body: Any) -> List[str]:
    keys: List[str] = []
    if not isinstance(body, dict): return keys

    def _collect(parent: Mapping[str, Any]):
        for bkt, typ in (("movies", "movie"), ("shows", "show")):
            v = parent.get(bkt)
            if isinstance(v, list):
                for it in v:
                    ids = _ids_filter((it.get("ids") or it) if isinstance(it, Mapping) else {})
                    if ids:
                        m = {"type": typ, "ids": ids, "simkl_bucket": bkt}
                        keys.append(simkl_key_of(m))

    for top in ("added", "removed", "deleted"):
        section = body.get(top)
        if isinstance(section, Mapping): _collect(section)
    _collect(body)

    seen = set(); uniq = []
    for k in keys:
        if k not in seen:
            uniq.append(k); seen.add(k)
    return uniq

def _mk_shadow_item(it: Mapping[str, Any]) -> Tuple[str, Dict[str, Any]]:
    ids = _ids_filter(dict((it.get("ids") or {})))
    grp = _kind_group(it)
    media_type = "movie" if grp == "movies" else "show"
    m = {"type": media_type, "title": it.get("title"), "year": it.get("year"), "ids": ids, "simkl_bucket": grp}
    return simkl_key_of(m), m

def _shadow_add_items(items: Iterable[Mapping[str, Any]]) -> None:
    sh = _shadow_load()
    cur = dict(sh.get("items") or {})
    patch: Dict[str, Dict[str, Any]] = {}
    for it in items or []:
        k, m = _mk_shadow_item(it); patch[k] = m
    if patch:
        _merge_upsert(cur, patch); _shadow_save(sh.get("ts"), cur)

def _shadow_remove_keys(keys: Iterable[str]) -> None:
    sh = _shadow_load()
    cur = dict(sh.get("items") or {})
    changed = False
    for k in list(keys or []):
        if k in cur: del cur[k]; changed = True
    if changed: _shadow_save(sh.get("ts"), cur)

# ---------- fetchers
def _pull_bucket(
    adapter,
    bucket: str,
    *,
    date_from: Optional[str],
    ids_only: bool,
    limit: Optional[int],
    force_refresh: bool = False,
) -> Dict[str, Dict[str, Any]]:
    sess = adapter.client.session
    out: Dict[str, Dict[str, Any]] = {}

    url_ids = URL_INDEX_IDS.format(bucket=bucket)
    url_full = URL_INDEX_BUCKET.format(bucket=bucket)

    base_params: Dict[str, Any] = {"extended": ("ids_only" if ids_only else "full")}
    if not ids_only and bucket == "shows":
        base_params["episode_watched_at"] = "yes"
    if date_from:
        base_params["date_from"] = date_from

    def _do_fetch(_url: str, _params: Dict[str, Any], _force: bool) -> Dict[str, Dict[str, Any]]:
        try:
            r = sess.get(
                _url,
                headers=_headers(adapter, force_refresh=_force),
                params=_params,
                timeout=adapter.cfg.timeout,
            )
            if r.status_code != 200:
                _log(f"GET {_url} -> {r.status_code}")
                return {}

            try:
                data = r.json()
            except Exception:
                data = None

            rows = _rows_from_data(data, bucket)
            if not rows:
                return {}

            count = 0
            out_local: Dict[str, Dict[str, Any]] = {}
            for row in rows:
                try:
                    m = _normalize_row(bucket, row)
                    if not (m.get("ids") or m.get("title")):
                        continue
                    out_local[simkl_key_of(m)] = m
                    count += 1
                    if limit and count >= int(limit):
                        break
                except Exception:
                    continue

            return out_local
        except Exception as e:
            _log(f"bucket pull error {bucket}: {e}")
            return {}

    # First pass
    first_url = url_ids if ids_only else url_full
    out = _do_fetch(first_url, dict(base_params), force_refresh)

    # Fallback:
    if not out:
        alt_params = dict(base_params)
        alt_params.pop("date_from", None)
        if ids_only:
            alt_params["extended"] = "full"
            alt_url = url_full
        else:
            alt_url = first_url
        out = _do_fetch(alt_url, alt_params, True)

    return out

# ---------- index (with progress-helper)
def build_index(adapter, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("watchlist") if callable(prog_mk) else None
    done = 0
    total_known = 0

    sess = adapter.client.session
    hdrs = _headers(adapter)

    acts, _rate = fetch_activities(sess, hdrs, timeout=adapter.cfg.timeout)
    ts_map = _bucket_ts(acts or {})

    def _composite_ts(tsm):
        def v(x): return x or "-"
        return "|".join([
            f"m:{v(tsm['movies']['ptw'])}/{v(tsm['movies']['rm'])}",
            f"s:{v(tsm['shows']['ptw'])}/{v(tsm['shows']['rm'])}",
        ])

    comp_ts = _composite_ts(ts_map)

    if os.getenv("CW_SIMKL_WATCHLIST_CLEAR") == "1":
        try:
            SHADOW_PATH.unlink(missing_ok=True)
        except Exception:
            pass

    shadow = _shadow_load()
    items: Dict[str, Dict[str, Any]] = dict(shadow.get("items") or {})
    # purge unexpected buckets
    items = {k: v for k, v in items.items()
            if (v.get("simkl_bucket") in WATCHLIST_BUCKETS)}

    if comp_ts and shadow.get("ts") == comp_ts and _has_all_buckets(items):
        age, ttl = _shadow_age_seconds(), _shadow_ttl_seconds()
        if age <= ttl:
            if prog:
                try:
                    total = len(items)
                    prog.tick(0, total=total, force=True)
                    prog.tick(total, total=total)
                except Exception:
                    pass
            _log(f"unchanged via activities (reuse shadow) size={len(items)} age={int(age)}s")
            return items

        _log(f"shadow stale (age={int(age)}s>{int(ttl)}s) → ids_only verify")
        for bucket in WATCHLIST_BUCKETS:
            df = coalesce_date_from(f"watchlist:{bucket}", cfg_date_from="1970-01-01T00:00:00Z")
            snap = _pull_bucket(adapter, bucket, date_from=df, ids_only=True, limit=limit, force_refresh=True)
            _merge_upsert(items, snap)
            cnt = len(snap)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass

        _shadow_save(comp_ts, items)
        if prog:
            try:
                final_total = len(items)
                prog.tick(done, total=final_total, force=True)
            except Exception:
                pass
        return items

    force_present = (os.getenv("CW_SIMKL_FORCE_PRESENT") or "").strip().lower()
    force_all = force_present in ("1", "true", "all")

    for bucket in WATCHLIST_BUCKETS:
        have_bucket = _bucket_present(items, bucket)
        ptw_ts = ts_map[bucket]["ptw"]
        rm_ts = ts_map[bucket]["rm"]

        rm_key = f"watchlist_removed:{bucket}"
        prev_rm = get_watermark(rm_key)

        if force_all or force_present == bucket:
            _log(f"{bucket}: forced present ids_only reconcile")
            df_force = coalesce_date_from(f"watchlist:{bucket}", cfg_date_from="1970-01-01T00:00:00Z")
            fresh = _pull_bucket(adapter, bucket, date_from=df_force, ids_only=True, limit=limit, force_refresh=True)
            _merge_upsert(items, fresh)
            cnt = len(fresh)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
            have_bucket = _bucket_present(items, bucket)

        elif rm_ts and rm_ts != prev_rm:
            if have_bucket:
                drop = [k for k, v in items.items()
                        if isinstance(v, Mapping) and v.get("simkl_bucket") == bucket]
                for k in drop:
                    items.pop(k, None)

            df_full = coalesce_date_from(f"watchlist:{bucket}", cfg_date_from="1970-01-01T00:00:00Z")
            fresh = _pull_bucket(adapter, bucket, date_from=df_full, ids_only=True, limit=limit, force_refresh=True)
            _merge_upsert(items, fresh)
            save_watermark(rm_key, rm_ts)
            _log(f"{bucket}: rebuilt via ids_only ({len(fresh)})")

            cnt = len(fresh)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
            have_bucket = _bucket_present(items, bucket)

        df_key = f"watchlist:{bucket}"
        date_from = coalesce_date_from(df_key)

        if ptw_ts and ptw_ts != get_watermark(df_key):
            inc = _pull_bucket(adapter, bucket, date_from=date_from, ids_only=False, limit=limit, force_refresh=False)
            if not inc:
                _log(f"{bucket}: incremental returned 0; fallback to present ids_only")
                df_full = coalesce_date_from(df_key, cfg_date_from="1970-01-01T00:00:00Z")
                inc = _pull_bucket(adapter, bucket, date_from=df_full, ids_only=True, limit=limit, force_refresh=True)

            _merge_upsert(items, inc)
            save_watermark(df_key, ptw_ts)
            _log(f"{bucket}: incremental {len(inc)} from {date_from or 'baseline'}")

            cnt = len(inc)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
            have_bucket = _bucket_present(items, bucket)

        if not have_bucket:
            _log(f"{bucket}: missing in shadow; forcing FULL snapshot")
            df_full = coalesce_date_from(f"watchlist:{bucket}", cfg_date_from="1970-01-01T00:00:00Z")
            snap = _pull_bucket(adapter, bucket, date_from=df_full, ids_only=False, limit=limit, force_refresh=True)
            if not snap:
                snap = _pull_bucket(adapter, bucket, date_from=None, ids_only=False, limit=limit, force_refresh=True)

            _merge_upsert(items, snap)

            cnt = len(snap)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass

            if ts_map[bucket]["ptw"]:
                save_watermark(df_key, ts_map[bucket]["ptw"])

    _jit_enrich_missing(adapter, items)
    _unfreeze_if_present(items.keys())
    _shadow_save(comp_ts, items)

    latest_any = max(
        [t for t in [ts_map["movies"]["ptw"], ts_map["shows"]["ptw"]] if t]
        or ["2000-01-01T00:00:00Z"]
    )
    if latest_any:
        save_watermark("watchlist", latest_any)

    if prog:
        try:
            final_total = len(items)
            prog.tick(done, total=final_total, force=True)
        except Exception:
            pass

    _log(f"index size: {len(items)}")
    return items

# ---------- writes

def _split_buckets(items: Iterable[Mapping[str, Any]]):
    movies, shows, unresolved = [], [], []
    for it in items:
        ids = dict((it.get("ids") or {}))
        body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
        if not body_ids:
            unresolved.append({"item": id_minimal(it), "hint": "missing_ids"}); continue
        grp = _kind_group(it)
        entry = {"ids": body_ids}
        if grp == "movies": movies.append(entry)
        else: shows.append(entry)
    payload: Dict[str, Any] = {}
    if movies: payload["movies"] = movies
    if shows:  payload["shows"]  = shows
    return payload, unresolved

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess = adapter.client.session
    hdrs = _headers(adapter)
    items = list(items or [])
    raw_payload, unresolved = _split_buckets(items)
    if not raw_payload: return 0, unresolved

    body: Dict[str, Any] = {}
    if raw_payload.get("movies"):
        body["movies"] = [{"ids": it["ids"], "to": "plantowatch"} for it in raw_payload["movies"]]
    if raw_payload.get("shows"):
        body["shows"]  = [{"ids": it["ids"], "to": "plantowatch"} for it in raw_payload["shows"]]

    ok = 0
    try:
        r = sess.post(URL_ADD, headers=hdrs, json=body, timeout=adapter.cfg.timeout)
        resp = r.json() if (r.text and "application/json" in (r.headers.get("Content-Type",""))) else {}
        if 200 <= r.status_code < 300:
            processed = _sum_processed_from_body(resp)
            if processed == 0:
                for it in items:
                    ids = dict((it.get("ids") or {}))
                    body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
                    if body_ids: _freeze(it, action="add", reasons=["not_processed"], ids_sent=body_ids)
                _log(f"ADD 2xx but no items processed; body={str(resp)[:180]}")
            ok = int(processed)

            if ok > 0:
                keys_from_resp = _keys_from_write_resp(resp)
                if keys_from_resp:
                    by_key = {simkl_key_of({"type": ("movie" if _kind_group(it)=="movies" else "show"),
                                             "ids": _ids_filter(it.get("ids") or {}),
                                             "simkl_bucket": _kind_group(it)}): it for it in items}
                    to_add = [by_key[k] for k in keys_from_resp if k in by_key]
                    if to_add: _shadow_add_items(to_add)
                elif ok == len(items):
                    _shadow_add_items(items)
                _unfreeze_if_present([simkl_key_of(id_minimal(it)) for it in items])
        else:
            _log(f"ADD failed {r.status_code}: {(r.text or '')[:180]}")
            for it in items:
                ids = dict((it.get("ids") or {}))
                body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
                if body_ids:
                    unresolved.append({"item": id_minimal(it), "hint": f"add_failed:{r.status_code}"})
                    _freeze(it, action="add", reasons=[f"write_failed:{r.status_code}"], ids_sent=body_ids)
    except Exception as e:
        _log(f"ADD error: {e}")
        for it in items:
            ids = dict((it.get("ids") or {}))
            body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
            if body_ids:
                unresolved.append({"item": id_minimal(it), "hint": "add_exception"})
                _freeze(it, action="add", reasons=["exception"], ids_sent=body_ids)

    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess = adapter.client.session
    hdrs = _headers(adapter)
    items = list(items or [])
    payload, unresolved = _split_buckets(items)
    if not payload: return 0, unresolved

    ok = 0
    try:
        r = sess.post(URL_REMOVE, headers=hdrs, json=payload, timeout=adapter.cfg.timeout)
        resp = r.json() if (r.text and "application/json" in (r.headers.get("Content-Type","").lower())) else {}
        if 200 <= r.status_code < 300:
            processed = _sum_processed_from_body(resp)
            if processed == 0:
                for it in items:
                    ids = dict((it.get("ids") or {}))
                    body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
                    if body_ids:
                        unresolved.append({"item": id_minimal(it), "hint": "not_removed"})
                        _freeze(it, action="remove", reasons=["not_processed"], ids_sent=body_ids)
                _log(f"REMOVE 2xx but no items processed; body={(str(resp)[:180] if resp else '∅')}")
            ok = int(processed)

            if ok > 0:
                keys_from_resp = _keys_from_write_resp(resp)
                if keys_from_resp:
                    _shadow_remove_keys(keys_from_resp)
                elif ok == len(items):
                    rm_keys = []
                    for it in items:
                        k = simkl_key_of({
                            "type": ("movie" if _kind_group(it) == "movies" else "show"),
                            "ids": _ids_filter(it.get("ids") or {}),
                            "simkl_bucket": _kind_group(it),
                        })
                        rm_keys.append(k)
                    _shadow_remove_keys(rm_keys)
                _unfreeze_if_present([simkl_key_of(id_minimal(it)) for it in items])
        else:
            _log(f"REMOVE failed {r.status_code}: {(r.text or '')[:180]}")
            for it in items:
                ids = dict((it.get("ids") or {}))
                body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
                if body_ids:
                    unresolved.append({"item": id_minimal(it), "hint": f"remove_failed:{r.status_code}"})
                    _freeze(it, action="remove", reasons=[f"write_failed:{r.status_code}"], ids_sent=body_ids)
    except Exception as e:
        _log(f"REMOVE error: {e}")
        for it in items:
            ids = dict((it.get("ids") or {}))
            body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
            if body_ids:
                unresolved.append({"item": id_minimal(it), "hint": "remove_exception"})
                _freeze(it, action="remove", reasons=["exception"], ids_sent=body_ids)

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

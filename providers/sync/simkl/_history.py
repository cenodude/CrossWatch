# /providers/sync/simkl/_history.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timezone

from ._common import (
    normalize as simkl_normalize,
    key_of as simkl_key_of,
    coalesce_date_from,
    update_watermark_if_new,
    build_headers,
    fetch_activities,
    extract_latest_ts,
    get_watermark,
)

try:
    from cw_platform.id_map import minimal as id_minimal
except Exception:
    from _id_map import minimal as id_minimal  # type: ignore

BASE = "https://api.simkl.com"
URL_ALL_ITEMS = f"{BASE}/sync/all-items"
URL_ADD       = f"{BASE}/sync/history"
URL_REMOVE    = f"{BASE}/sync/history/remove"

STATE_DIR       = Path("/config/.cw_state")
UNRESOLVED_PATH = str(STATE_DIR / "simkl_history.unresolved.json")
SHADOW_PATH     = str(STATE_DIR / "simkl.history.shadow.json")
SHOW_MAP_PATH   = str(STATE_DIR / "simkl.show.map.json")

ID_KEYS = ("simkl", "imdb", "tmdb", "tvdb")

def _log(msg: str) -> None:
    if os.getenv("CW_DEBUG") or os.getenv("CW_SIMKL_DEBUG"):
        print(f"[SIMKL:history] {msg}")

def _now_epoch() -> int: return int(time.time())

def _as_epoch(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    if isinstance(v, datetime):
        return int((v if v.tzinfo else v.replace(tzinfo=timezone.utc)).timestamp())
    if isinstance(v, str):
        try: return int(datetime.fromisoformat(v.replace("Z","+00:00")).timestamp())
        except Exception: return None
    return None

def _as_iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00","Z")

def _headers(adapter, *, force_refresh: bool = False) -> Dict[str, str]:
    return build_headers({"simkl": {"api_key": adapter.cfg.api_key, "access_token": adapter.cfg.access_token}}, force_refresh=force_refresh)

def _ids_of(obj: Mapping[str, Any]) -> Dict[str, Any]:
    ids = dict(obj.get("ids") or {})
    return {k: ids[k] for k in ID_KEYS if ids.get(k)}

def _raw_show_ids(it: Mapping[str, Any]) -> Dict[str, Any]: return dict(it.get("show_ids") or {})
def _show_ids_of_episode(it: Mapping[str, Any]) -> Dict[str, Any]:
    sids = _raw_show_ids(it); return {k: sids[k] for k in ID_KEYS if sids.get(k)}

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

def _is_null_env(row: Any) -> bool:
    return isinstance(row, Mapping) and row.get("type") == "null" and row.get("body") is None

def _load_unresolved() -> Dict[str, Any]: return _load_json(UNRESOLVED_PATH)
def _save_unresolved(data: Mapping[str, Any]) -> None: _save_json(UNRESOLVED_PATH, data)

def _freeze(item: Mapping[str, Any], *, action: str, reasons: List[str], ids_sent: Dict[str, Any], watched_at: Optional[str]) -> None:
    key = simkl_key_of(id_minimal(item))
    data = _load_unresolved()
    row = data.get(key) or {"feature":"history","action":action,"first_seen":_now_epoch(),"attempts":0}
    row.update({"item": id_minimal(item), "last_attempt": _now_epoch()})
    row["reasons"]  = sorted(set(row.get("reasons", [])) | set(reasons or []))
    row["ids_sent"] = dict(ids_sent or {})
    if watched_at: row["watched_at"] = watched_at
    row["attempts"] = int(row.get("attempts", 0)) + 1
    data[key] = row; _save_unresolved(data)

def _unfreeze(keys: Iterable[str]) -> None:
    data = _load_unresolved(); changed = False
    for k in set(keys or []):
        if k in data: del data[k]; changed = True
    if changed: _save_unresolved(data)

# write-through shadow
def _shadow_ttl_seconds() -> int:
    try: return int(os.getenv("CW_SIMKL_HISTORY_SHADOW_TTL", str(7*24*3600)))
    except Exception: return 7*24*3600

def _shadow_load() -> Dict[str, Any]: return _load_json(SHADOW_PATH) or {"events":{}}
def _shadow_save(obj: Mapping[str, Any]) -> None: _save_json(SHADOW_PATH, obj)

def _shadow_put_all(items: Iterable[Mapping[str, Any]]) -> None:
    items = list(items or [])
    if not items: return
    sh = _shadow_load(); ev = dict(sh.get("events") or {})
    now = _now_epoch(); ttl = _shadow_ttl_seconds()
    for it in items:
        ts = _as_epoch(it.get("watched_at"))
        bk = simkl_key_of(id_minimal(it))
        if not ts or not bk: continue
        ev[f"{bk}@{ts}"] = {"item": id_minimal(it), "exp": now + ttl}
    sh["events"] = ev; _shadow_save(sh)

def _shadow_merge_into(out: Dict[str, Dict[str, Any]], thaw: set) -> None:
    sh = _shadow_load(); ev = dict(sh.get("events") or {})
    if not ev: return
    now = _now_epoch(); changed = False; merged = 0
    for ek, rec in list(ev.items()):
        exp = int(rec.get("exp") or 0)
        if exp and exp < now: del ev[ek]; changed = True; continue
        if ek in out:        del ev[ek]; changed = True; continue
        it = rec.get("item")
        if isinstance(it, Mapping):
            out[ek] = it; thaw.add(simkl_key_of(id_minimal(it))); merged += 1
    if merged: _log(f"shadow merged {merged} backfill events")
    if changed or merged: _save_json(SHADOW_PATH, {"events": ev})

# auto show-id resolver (learn once, reuse forever)
_RESOLVE_CACHE: Dict[str, Dict[str, str]] = {}
def _load_show_map() -> Dict[str, Any]: return _load_json(SHOW_MAP_PATH) or {"map": {}}
def _save_show_map(obj: Mapping[str, Any]) -> None: _save_json(SHOW_MAP_PATH, obj)
def _persist_show_map(key: str, ids: Mapping[str, Any]) -> None:
    ok = {k: str(v) for k, v in ids.items() if k in ("tvdb","tmdb","imdb","simkl") and v}
    if not ok: return
    m = _load_show_map(); mp = dict(m.get("map") or {})
    if mp.get(key) == ok: return
    mp[key] = ok; m["map"] = mp; _save_show_map(m)

def _norm_title(s: Optional[str]) -> str: return "".join(ch for ch in (s or "").lower() if ch.isalnum())
def _best_ids(obj: Mapping[str, Any]) -> Dict[str, str]:
    ids = dict(obj.get("ids") or obj or {})
    return {k: str(ids[k]) for k in ("tvdb","tmdb","imdb","simkl") if ids.get(k)}

def _simkl_search_show(adapter, title: str, year: Optional[int]) -> Dict[str, str]:
    if not title or os.getenv("CW_SIMKL_AUTO_RESOLVE","1") == "0": return {}
    sess = adapter.client.session; hdrs = _headers(adapter, force_refresh=True)
    try:
        r = sess.get(f"{BASE}/search/tv", headers=hdrs, params={"q": title, "limit": 5, "extended": "full"}, timeout=adapter.cfg.timeout)
        if not r.ok: return {}
        arr = r.json() or []
    except Exception:
        return {}
    want = _norm_title(title); pick: Dict[str, Any] = {}; best = -1
    for x in (arr if isinstance(arr, list) else []):
        show = (x.get("show") if isinstance(x, Mapping) else None) or x
        ids = _best_ids(show); ttl = (show or {}).get("title") or ""; yr = (show or {}).get("year")
        if not ids: continue
        score = (2 if _norm_title(ttl) == want else 0) + (1 if (year and yr and abs(int(yr)-int(year))<=1) else 0)
        if score > best: best, pick = score, {"ids": ids, "title": ttl, "year": yr}
    return _best_ids(pick.get("ids") or pick)

# resolve show via episode IDs (when title/year fails)
def _simkl_resolve_show_via_episode_id(adapter, it: Mapping[str, Any]) -> Dict[str, str]:
    ids = dict(it.get("ids") or {})
    params = {}
    for k in ("imdb","tvdb","tmdb"):
        v = ids.get(k)
        if v: params[k] = str(v)
    if not params: return {}
    sess = adapter.client.session; hdrs = _headers(adapter, force_refresh=True)
    try:
        r = sess.get(f"{BASE}/search/id", headers=hdrs, params=params, timeout=adapter.cfg.timeout)
        if not r.ok: return {}
        body = r.json() or {}
    except Exception:
        return {}
    # Response may be a dict or list; try common shapes
    cand = body
    if isinstance(body, list) and body: cand = body[0]
    if isinstance(cand, Mapping):
        # prefer explicit show object, else general ids
        show = cand.get("show") if isinstance(cand.get("show"), Mapping) else cand
        return _best_ids(show)
    return {}

def _resolve_show_ids(adapter, it: Mapping[str, Any], sids_raw: Dict[str, Any]) -> Dict[str, str]:
    have = {k: sids_raw[k] for k in ("tvdb","tmdb","imdb","simkl") if sids_raw.get(k)}
    if have: return {k: str(v) for k, v in have.items()}
    plex = sids_raw.get("plex")
    key  = f"plex:{plex}" if plex else _norm_title(it.get("series_title") or it.get("show_title") or it.get("grandparent_title") or it.get("title"))
    if key in _RESOLVE_CACHE: return _RESOLVE_CACHE[key]
    mp = _load_show_map().get("map", {})
    if key in mp: _RESOLVE_CACHE[key] = dict(mp[key]); return _RESOLVE_CACHE[key]
    ttl = it.get("series_title") or it.get("show_title") or it.get("grandparent_title") or it.get("title") or ""
    yr  = it.get("series_year") or it.get("year")
    found = _simkl_search_show(adapter, ttl, int(yr) if isinstance(yr, int) else None)
    if not found:
        found = _simkl_resolve_show_via_episode_id(adapter, it)  # last-ditch: episode id → show ids
    if found:
        _persist_show_map(key, found); _RESOLVE_CACHE[key] = found; return found
    return {}

# ── index (delta) ─────────────────────────────────────────────────────────────

def _fetch_kind(sess, hdrs, *, kind: str, since_iso: str, timeout: float) -> List[Dict[str, Any]]:
    params = {"extended":"full", "episode_watched_at":"yes", "date_from": since_iso}
    r = sess.get(f"{URL_ALL_ITEMS}/{kind}", headers=hdrs, params=params, timeout=timeout)
    if not r.ok:
        _log(f"GET {URL_ALL_ITEMS}/{kind} -> {r.status_code}")
        return []
    try: body = r.json() or []
    except Exception: body = []
    if isinstance(body, list): return [x for x in body if not _is_null_env(x)]
    if isinstance(body, Mapping):
        arr = body.get(kind) or body.get("items") or []
        return [x for x in (arr if isinstance(arr, list) else []) if not _is_null_env(x)]
    return []

def build_index(adapter, since: Optional[int] = None, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    sess = adapter.client.session; tmo  = adapter.cfg.timeout
    acts, _rate = fetch_activities(sess, _headers(adapter, force_refresh=True), timeout=tmo)
    if isinstance(acts, Mapping) and since is None:
        wm_movies = get_watermark("history:movies") or ""; wm_shows  = get_watermark("history:shows") or ""
        latest_movies = extract_latest_ts(acts, (("movies","completed"), ("movies","watched"), ("history","movies"), ("movies","all")))
        latest_shows  = extract_latest_ts(acts, (("shows","completed"),  ("shows","watched"),  ("history","shows"),  ("shows","all")))
        no_change = (latest_movies is None or latest_movies <= (wm_movies or "")) and (latest_shows is None or latest_shows <= (wm_shows or ""))
        if no_change:
            _log(f"activities unchanged; history noop (movies={latest_movies}, shows={latest_shows})"); return {}

    hdrs = _headers(adapter, force_refresh=True)
    out: Dict[str, Dict[str, Any]] = {}; thaw: set[str] = set(); added = 0
    latest_ts_movies: Optional[int] = None; latest_ts_shows: Optional[int]  = None

    df_movies_iso = coalesce_date_from("history:movies", cfg_date_from=(_as_iso(since) if since else None))
    df_shows_iso  = coalesce_date_from("history:shows",  cfg_date_from=(_as_iso(since) if since else None))
    if since:
        since_iso = _as_iso(int(since))
        try:
            sm = max(_as_epoch(df_movies_iso) or 0, _as_epoch(since_iso) or 0)
            ss = max(_as_epoch(df_shows_iso) or 0, _as_epoch(since_iso) or 0)
            df_movies_iso = _as_iso(sm); df_shows_iso = _as_iso(ss)
        except Exception: pass

    m_rows = _fetch_kind(sess, hdrs, kind="movies", since_iso=df_movies_iso, timeout=tmo)
    movies_cnt = 0
    for row in m_rows:
        if not isinstance(row, Mapping): continue
        movie = row.get("movie") or row
        wa = (row.get("last_watched_at") or row.get("watched_at") or "").strip()
        ts = _as_epoch(wa)
        if not movie or not ts: continue
        m = simkl_normalize(movie); m["watched"] = True; m["watched_at"] = wa
        bk = simkl_key_of(m); ek = f"{bk}@{ts}"
        if ek in out: continue
        out[ek] = m; thaw.add(bk); movies_cnt += 1; added += 1
        latest_ts_movies = max(latest_ts_movies or 0, ts)
        if limit and added >= limit: break

    if not limit or added < limit:
        s_rows = _fetch_kind(sess, hdrs, kind="shows", since_iso=df_shows_iso, timeout=tmo)
        eps_cnt = 0
        for row in s_rows:
            if not isinstance(row, Mapping): continue
            show = row.get("show") or row
            if not show: continue
            base = simkl_normalize(show)
            show_ids = _ids_of(base) or _ids_of(show)
            if not show_ids: continue
            for s in (row.get("seasons") or []):
                s_num = int((s or {}).get("number") or (s or {}).get("season") or 0)
                for e in (s.get("episodes") or []):
                    e_num = int((e or {}).get("number") or (e or {}).get("episode") or 0)
                    wa = (e.get("watched_at") or e.get("last_watched_at") or "").strip()
                    ts = _as_epoch(wa)
                    if not ts or not s_num or not e_num: continue
                    ep = {"type":"episode","season":s_num,"episode":e_num,"ids":show_ids,"title":base.get("title"),"year":base.get("year"),"watched":True,"watched_at":wa}
                    bk = simkl_key_of(id_minimal(ep)); ek = f"{bk}@{ts}"
                    if ek in out: continue
                    out[ek] = ep; thaw.add(bk); eps_cnt += 1; added += 1
                    latest_ts_shows = max(latest_ts_shows or 0, ts)
                    if limit and added >= limit: break
                if limit and added >= limit: break
        _shadow_merge_into(out, thaw)
        _log(f"movies={movies_cnt} episodes={eps_cnt} from_movies={df_movies_iso} from_shows={df_shows_iso}")
    else:
        _shadow_merge_into(out, thaw)
        _log(f"movies={movies_cnt} episodes=0 from_movies={df_movies_iso} from_shows={df_shows_iso}")

    if latest_ts_movies: update_watermark_if_new("history:movies", _as_iso(latest_ts_movies))
    if latest_ts_shows:  update_watermark_if_new("history:shows",  _as_iso(latest_ts_shows))
    _unfreeze(thaw)

    _log(f"index size: {len(out)}"); return out

# ── writes ────────────────────────────────────────────────────────────────────

def _movie_add_entry(it: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    ids = _ids_of(it); wa = (it.get("watched_at") or it.get("watchedAt") or "").strip()
    if not ids or not wa: return None
    return {"ids": ids, "watched_at": wa}

def _show_add_entry(it: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    ids = _ids_of(it); return {"ids": ids} if ids else None

def _episode_add_entry(it: Mapping[str, Any]) -> Optional[Tuple[Dict[str, Any], int, int, str]]:
    s_num = int(it.get("season") or it.get("season_number") or 0)
    e_num = int(it.get("episode") or it.get("episode_number") or 0)
    wa = (it.get("watched_at") or it.get("watchedAt") or "").strip()
    if not s_num or not e_num or not wa: return None
    sids_raw = _raw_show_ids(it)
    adapter = it.get("_adapter") if isinstance(it, Mapping) else None
    if adapter:
        sids = _resolve_show_ids(adapter, it, sids_raw) or {k: sids_raw.get(k) for k in ID_KEYS if sids_raw.get(k)}
    else:
        sids = {k: sids_raw.get(k) for k in ID_KEYS if sids_raw.get(k)}
    if not sids: return None
    show = {"ids": sids, "title": it.get("series_title") or it.get("title"), "year": it.get("series_year") or it.get("year")}
    return show, s_num, e_num, wa

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess, hdrs = adapter.client.session, _headers(adapter); tmo = adapter.cfg.timeout
    movies: List[Dict[str, Any]] = []; shows_whole: List[Dict[str, Any]] = []
    shows_with_eps: Dict[str, Dict[str, Any]] = {}; unresolved: List[Dict[str, Any]] = []
    thaw_keys: List[str] = []; shadow_events: List[Dict[str, Any]] = []

    for _it in (items or []):
        if isinstance(_it, dict): _it["_adapter"] = adapter  # context for resolver

    for it in (items or []):
        typ = str(it.get("type") or "").lower()

        if typ == "movie":
            entry = _movie_add_entry(it)
            if entry:
                movies.append(entry); thaw_keys.append(simkl_key_of(id_minimal(it)))
                ev = dict(id_minimal(it)); ev["watched_at"] = entry.get("watched_at"); 
                if ev.get("watched_at"): shadow_events.append(ev)
            else:
                unresolved.append({"item": id_minimal(it), "hint":"missing_ids_or_watched_at"})
            continue

        if typ == "episode":
            packed = _episode_add_entry(it)
            if not packed:
                unresolved.append({"item": id_minimal(it), "hint":"missing_show_ids_or_s/e_or_watched_at"})
                continue
            show_entry, s_num, e_num, wa = packed
            ids_key = json.dumps(show_entry["ids"], sort_keys=True)
            group = shows_with_eps.setdefault(ids_key, {"ids": show_entry["ids"], "title": show_entry.get("title"), "year": show_entry.get("year"), "seasons": []})
            season = next((s for s in group["seasons"] if s.get("number") == s_num), None)
            if not season: season = {"number": s_num, "episodes": []}; group["seasons"].append(season)
            season["episodes"].append({"number": e_num, "watched_at": wa})
            thaw_keys.append(simkl_key_of(id_minimal(it)))
            ev = dict(id_minimal(it)); ev["watched_at"] = wa; shadow_events.append(ev); continue

        entry = _show_add_entry(it)
        if entry:
            shows_whole.append(entry); thaw_keys.append(simkl_key_of(id_minimal(it)))
        else:
            unresolved.append({"item": id_minimal(it), "hint":"missing_ids"})

    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    shows_payload: List[Dict[str, Any]] = []
    if shows_whole: shows_payload.extend(shows_whole)
    if shows_with_eps: shows_payload.extend(list(shows_with_eps.values()))
    if shows_payload: body["shows"] = shows_payload
    if not body: return 0, unresolved

    try:
        r = sess.post(URL_ADD, headers=hdrs, json=body, timeout=tmo)
        if 200 <= r.status_code < 300:
            _unfreeze(thaw_keys)
            eps_count = sum(len(s.get("episodes", [])) for g in shows_with_eps.values() for s in g.get("seasons", []))
            ok = len(movies) + eps_count + len(shows_whole)
            _log(f"add done http:{r.status_code} movies={len(movies)} shows={len(shows_payload)} episodes={eps_count}")
            try: _shadow_put_all(shadow_events)
            except Exception as e: _log(f"shadow.put skipped: {e}")
            return ok, unresolved
        _log(f"ADD failed {r.status_code}: {(r.text or '')[:200]}")
    except Exception as e:
        _log(f"ADD error: {e}")

    for it in (items or []):
        ids = _ids_of(it) or _show_ids_of_episode(it)
        wa  = (it.get("watched_at") or it.get("watchedAt") or None)
        if ids: _freeze(it, action="add", reasons=["write_failed"], ids_sent=ids, watched_at=(wa if isinstance(wa, str) else None))
    return 0, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    sess, hdrs = adapter.client.session, _headers(adapter); tmo = adapter.cfg.timeout
    movies: List[Dict[str, Any]] = []; shows_whole: List[Dict[str, Any]] = []
    shows_with_eps: Dict[str, Dict[str, Any]] = {}; unresolved: List[Dict[str, Any]] = []; thaw_keys: List[str] = []

    for it in (items or []):
        typ = str(it.get("type") or "").lower()
        if typ == "movie":
            ids = _ids_of(it)
            if not ids: unresolved.append({"item": id_minimal(it), "hint":"missing_ids"}); continue
            movies.append({"ids": ids}); thaw_keys.append(simkl_key_of(id_minimal(it))); continue
        if typ == "episode":
            sids = _show_ids_of_episode(it)
            s_num = int(it.get("season") or it.get("season_number") or 0)
            e_num = int(it.get("episode") or it.get("episode_number") or 0)
            if not sids or not s_num or not e_num:
                unresolved.append({"item": id_minimal(it), "hint":"missing_show_ids_or_s/e"}); continue
            ids_key = json.dumps(sids, sort_keys=True)
            group = shows_with_eps.setdefault(ids_key, {"ids": sids, "seasons": []})
            season = next((s for s in group["seasons"] if s.get("number") == s_num), None)
            if not season: season = {"number": s_num, "episodes": []}; group["seasons"].append(season)
            season["episodes"].append({"number": e_num})
            thaw_keys.append(simkl_key_of(id_minimal(it))); continue
        ids = _ids_of(it)
        if ids: shows_whole.append({"ids": ids}); thaw_keys.append(simkl_key_of(id_minimal(it)))
        else:   unresolved.append({"item": id_minimal(it), "hint":"missing_ids"})

    body: Dict[str, Any] = {}
    if movies: body["movies"] = movies
    shows_payload: List[Dict[str, Any]] = []
    if shows_whole: shows_payload.extend(shows_whole)
    if shows_with_eps: shows_payload.extend(list(shows_with_eps.values()))
    if shows_payload: body["shows"] = shows_payload
    if not body: return 0, unresolved

    try:
        r = sess.post(URL_REMOVE, headers=hdrs, json=body, timeout=tmo)
        if 200 <= r.status_code < 300:
            _unfreeze(thaw_keys)
            ok = len(movies) + len(shows_payload)
            _log(f"remove done http:{r.status_code} movies={len(movies)} shows={len(shows_payload)}")
            return ok, unresolved
        _log(f"REMOVE failed {r.status_code}: {(r.text or '')[:200]}")
    except Exception as e:
        _log(f"REMOVE error: {e}")

    for it in (items or []):
        ids = _ids_of(it) or _show_ids_of_episode(it)
        if ids: _freeze(it, action="remove", reasons=["write_failed"], ids_sent=ids, watched_at=None)
    return 0, unresolved

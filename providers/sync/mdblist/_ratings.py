# /providers/sync/mdblist/_ratings.py
from __future__ import annotations
import os, json, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

from .._mod_common import request_with_retries
from cw_platform.id_map import minimal as id_minimal

BASE = "https://api.mdblist.com"
URL_LIST   = f"{BASE}/sync/ratings"
URL_UPSERT = f"{BASE}/sync/ratings"
URL_UNRATE = f"{BASE}/sync/ratings/remove"

CACHE_PATH = Path("/config/.cw_state/mdblist_ratings.index.json")
CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)

def _log(msg: str):
    if os.getenv("CW_DEBUG") or os.getenv("CW_MDBLIST_DEBUG"):
        print(f"[MDBLIST:ratings] {msg}")

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
    if ids.get("tmdb"):
        try: out["tmdb"] = int(ids["tmdb"])
        except Exception: pass
    if ids.get("tvdb"):
        try: out["tvdb"] = int(ids["tvdb"])
        except Exception: pass
    return out

def _pick_kind(it: Mapping[str, Any]) -> str:
    t = (it.get("type") or it.get("mediatype") or "").strip().lower()
    if t in ("movie","movies"): return "movies"
    if t in ("show","tv","series","shows"): return "shows"
    if t in ("season","seasons"): return "seasons"
    if t in ("episode","episodes"): return "episodes"
    if str((it.get("movie") or "")).lower() == "true": return "movies"
    if str((it.get("show") or "")).lower()  == "true": return "shows"
    return "movies"

def _key_of(obj: Mapping[str, Any]) -> str:
    ids = dict((obj.get("ids") or obj) or {})
    kind = (obj.get("type") or "").lower()
    imdb = (ids.get("imdb") or ids.get("imdb_id") or "").strip()
    base = ""
    if imdb: base = f"imdb:{imdb}"
    else:
        tmdb = ids.get("tmdb") or ids.get("tmdb_id")
        if tmdb: base = f"tmdb:{int(tmdb)}"
        else:
            tvdb = ids.get("tvdb") or ids.get("tvdb_id")
            if tvdb: base = f"tvdb:{int(tvdb)}"
            else:
                mdbl = ids.get("mdblist") or ids.get("id")
                if mdbl: base = f"mdblist:{mdbl}"
    if kind in ("season","episode"):
        if not base:
            t = (obj.get("title") or "").strip(); y = obj.get("year")
            base = f"title:{t}|year:{y}" if t and y else ""
    if kind == "season":
        s = obj.get("season")
        if base and s is not None: return f"season:{base}:S{int(s)}"
        if base: return f"season:{base}"
    if kind == "episode":
        s = obj.get("season"); e = obj.get("number")
        if e is None: e = obj.get("episode")
        if base and s is not None and e is not None: return f"episode:{base}:{int(s)}x{int(e)}"
        if base: return f"episode:{base}"
    if base: return base
    t = (obj.get("title") or "").strip(); y = obj.get("year")
    return f"title:{t}|year:{y}" if t and y else f"obj:{hash(json.dumps(obj, sort_keys=True)) & 0xffffffff}"

def _valid_rating(v: Any) -> Optional[int]:
    try:
        i = int(str(v).strip())
        return i if 1 <= i <= 10 else None
    except Exception:
        return None

def _now_iso() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

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

def _row_movie(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None: return None
        mv = row.get("movie") or {}
        ids = (mv.get("ids") or {})
        ids = {"imdb": ids.get("imdb"), "tmdb": ids.get("tmdb"), "tvdb": ids.get("tvdb")}
        ids = {k:v for k,v in ids.items() if v}
        if not ids: return None
        title = (mv.get("title") or mv.get("name") or "").strip()
        y = mv.get("year") or mv.get("release_year")
        try: year = int(y) if y is not None else None
        except Exception: year = None
        out = {"type": "movie", "ids": ids, "rating": rating}
        if row.get("rated_at"): out["rated_at"] = row["rated_at"]
        if title: out["title"] = title
        if year: out["year"] = year
        return out
    except Exception:
        return None

def _row_show(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None: return None
        sh = row.get("show") or {}
        ids = (sh.get("ids") or {})
        ids = {"imdb": ids.get("imdb"), "tmdb": ids.get("tmdb"), "tvdb": ids.get("tvdb")}
        ids = {k:v for k,v in ids.items() if v}
        if not ids: return None
        title = (sh.get("title") or sh.get("name") or "").strip()
        y = sh.get("year") or sh.get("first_air_year")
        if not y:
            fa = (sh.get("first_air_date") or sh.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit(): y = int(fa[:4])
        try: year = int(y) if y is not None else None
        except Exception: year = None
        out = {"type": "show", "ids": ids, "rating": rating}
        if row.get("rated_at"): out["rated_at"] = row["rated_at"]
        if title: out["title"] = title
        if year: out["year"] = year
        return out
    except Exception:
        return None

def _row_season(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None: return None
        sv = row.get("season") or {}
        show = sv.get("show") or {}
        sids = dict((sv.get("ids") or {}))
        sids = {"tmdb": sids.get("tmdb"), "tvdb": sids.get("tvdb")}
        sids = {k:v for k,v in sids.items() if v}
        sh_ids = dict((show.get("ids") or {}))
        sh_ids = {"imdb": sh_ids.get("imdb"), "tmdb": sh_ids.get("tmdb"), "tvdb": sh_ids.get("tvdb")}
        sh_ids = {k:v for k,v in sh_ids.items() if v}
        ids = sids or sh_ids
        if not ids: return None
        title = (show.get("title") or show.get("name") or sv.get("name") or "").strip()
        y = show.get("year") or show.get("first_air_year")
        if not y:
            fa = (show.get("first_air_date") or show.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit(): y = int(fa[:4])
        try: year = int(y) if y is not None else None
        except Exception: year = None
        out = {"type": "season", "ids": ids, "rating": rating, "season": sv.get("number")}
        if row.get("rated_at"): out["rated_at"] = row["rated_at"]
        if title: out["title"] = title
        if year: out["year"] = year
        return out
    except Exception:
        return None

def _row_episode(row: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        rating = _valid_rating(row.get("rating"))
        if rating is None: return None
        ev = row.get("episode") or {}
        show = ev.get("show") or {}
        eids = dict((ev.get("ids") or {}))
        eids = {"tmdb": eids.get("tmdb"), "tvdb": eids.get("tvdb")}
        eids = {k:v for k,v in eids.items() if v}
        sh_ids = dict((show.get("ids") or {}))
        sh_ids = {"imdb": sh_ids.get("imdb"), "tmdb": sh_ids.get("tmdb"), "tvdb": sh_ids.get("tvdb")}
        sh_ids = {k:v for k,v in sh_ids.items() if v}
        ids = eids or sh_ids
        if not ids: return None
        title = (show.get("title") or show.get("name") or ev.get("name") or "").strip()
        y = show.get("year") or show.get("first_air_year")
        if not y:
            fa = (show.get("first_air_date") or show.get("first_aired") or "").strip()
            if len(fa) >= 4 and fa[:4].isdigit(): y = int(fa[:4])
        try: year = int(y) if y is not None else None
        except Exception: year = None
        num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
        out = {"type": "episode", "ids": ids, "rating": rating, "season": ev.get("season"), "number": num}
        if row.get("rated_at"): out["rated_at"] = row["rated_at"]
        if title: out["title"] = title
        if year: out["year"] = year
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
    _log(f"index.start per_page={per_page} max_pages={max_pages} timeout={timeout} retries={retries}")

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
        shows  = data.get("shows")  or []
        seasons_top = data.get("seasons") or []
        episodes_top = data.get("episodes") or []

        _log(f"page {page} -> movies:{len(movies)} shows:{len(shows)} seasons:{len(seasons_top)} episodes:{len(episodes_top)}")

        minis: List[Dict[str, Any]] = []
        for row in movies:
            m = _row_movie(row)
            if m: minis.append(m)

        for row in shows:
            m = _row_show(row)
            if m: minis.append(m)
            sh = row.get("show") or {}
            sh_ids = (sh.get("ids") or {})
            ids_sh = {k: v for k, v in {"imdb": sh_ids.get("imdb"), "tmdb": sh_ids.get("tmdb"), "tvdb": sh_ids.get("tvdb")}.items() if v}
            title = (sh.get("title") or sh.get("name") or "").strip()
            y = sh.get("year") or sh.get("first_air_year")
            if not y:
                fa = (sh.get("first_air_date") or sh.get("first_aired") or "").strip()
                if len(fa) >= 4 and fa[:4].isdigit(): y = int(fa[:4])
            try: year = int(y) if y is not None else None
            except Exception: year = None

            for sv in row.get("seasons") or []:
                sr = _valid_rating(sv.get("rating"))
                sids = dict((sv.get("ids") or {}))
                sids = {k: sids.get(k) for k in ("tmdb","tvdb") if sids.get(k)}
                ids_for_season = sids or {k: ids_sh.get(k) for k in ("tmdb","tvdb") if ids_sh.get(k)}
                if sr is not None and ids_for_season:
                    sm = {"type": "season", "ids": ids_for_season, "rating": sr, "season": sv.get("number")}
                    ra = sv.get("rated_at")
                    if ra: sm["rated_at"] = ra
                    if title: sm["title"] = title
                    if year: sm["year"] = year
                    minis.append(sm)
                for ev in sv.get("episodes") or []:
                    er = _valid_rating(ev.get("rating"))
                    if er is None: continue
                    eids = dict((ev.get("ids") or {}))
                    eids = {k: eids.get(k) for k in ("tmdb","tvdb") if eids.get(k)}
                    ids_for_episode = eids or ids_sh
                    if not ids_for_episode: continue
                    num = ev.get("number") if ev.get("number") is not None else ev.get("episode")
                    em = {"type": "episode", "ids": ids_for_episode, "rating": er, "season": sv.get("number"), "number": num}
                    rae = ev.get("rated_at")
                    if rae: em["rated_at"] = rae
                    if title: em["title"] = title
                    if year: em["year"] = year
                    minis.append(em)

        for row in seasons_top:
            m = _row_season(row)
            if m: minis.append(m)

        for row in episodes_top:
            m = _row_episode(row)
            if m: minis.append(m)

        for m in minis:
            out[_key_of(m)] = m

        pag = data.get("pagination") or {}
        has_more = bool(pag.get("has_more"))
        pages += 1
        if not has_more or pages >= max_pages:
            break
        page += 1

    def _fetch_type_pages(type_name: str):
        p = 1
        done = 0
        while True:
            r = request_with_retries(
                sess, "GET", URL_LIST,
                params={"apikey": apikey, "page": p, "limit": per_page, "type": type_name},
                timeout=timeout, max_retries=retries
            )
            if r.status_code != 200:
                _log(f"GET /sync/ratings?type={type_name} page {p} -> {r.status_code}")
                break
            d = r.json() if (r.text or "").strip() else {}
            rows = d.get(type_name) or []
            if not rows:
                break
            if type_name == "seasons":
                for row in rows:
                    m = _row_season(row)
                    if m: out[_key_of(m)] = m
            elif type_name == "episodes":
                for row in rows:
                    m = _row_episode(row)
                    if m: out[_key_of(m)] = m
            pag = d.get("pagination") or {}
            done += 1
            if not bool(pag.get("has_more")) or done >= max_pages:
                break
            p += 1

    _fetch_type_pages("seasons")
    _fetch_type_pages("episodes")

    kinds = {"movie":0,"show":0,"season":0,"episode":0}
    for v in out.values():
        k = (v.get("type") or "").lower()
        if k in kinds: kinds[k]+=1
    _save_cache(out)
    _log(f"index size: {len(out)} by-kind {kinds}")
    return out

def _show_key(ids: Mapping[str, Any]) -> str:
    if ids.get("imdb"): return f"imdb:{ids['imdb']}"
    if ids.get("tmdb"): return f"tmdb:{ids['tmdb']}"
    if ids.get("tvdb"): return f"tvdb:{ids['tvdb']}"
    return json.dumps(ids, sort_keys=True)

def _bucketize(
    items: Iterable[Mapping[str, Any]],
    *,
    unrate: bool = False
) -> Tuple[Dict[str, List[Dict[str, Any]]], List[Dict[str, Any]]]:
    body: Dict[str, List[Dict[str, Any]]] = {"movies": [], "shows": []}
    accepted: List[Dict[str, Any]] = []

    seen = {"movies": 0, "shows": 0, "seasons": 0, "episodes": 0}
    kept = {"movies": 0, "shows": 0, "seasons": 0, "episodes": 0}
    attach = {"season_to_show": 0, "episode_to_season": 0}
    skip  = {
        "invalid_rating": 0,
        "missing_ids": 0,
        "missing_season": 0,
        "missing_episode": 0,
        "missing_show_ids": 0,
    }

    shows: Dict[str, Dict[str, Any]] = {}
    seasons_index: Dict[Tuple[str, int], Dict[str, Any]] = {}

    def ensure_show(ids: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
        sk = _show_key(ids)
        grp = shows.get(sk)
        if not grp:
            grp = {"ids": ids}
            shows[sk] = grp
        return sk, grp

    for it in items or []:
        kind = _pick_kind(it)
        if kind in seen:
            seen[kind] += 1

        if kind in ("seasons", "episodes"):
            ids = _ids_for_mdblist(it.get("show_ids") or {})
            if not ids:
                skip["missing_show_ids"] += 1
                continue
        else:
            ids = _ids_for_mdblist(it)
            if not ids:
                skip["missing_ids"] += 1
                continue

        rating = _valid_rating(it.get("rating"))
        ra = it.get("rated_at")

        if kind == "movies":
            if rating is None and not unrate:
                skip["invalid_rating"] += 1
                continue

            obj = {"ids": ids}
            if not unrate:
                obj["rating"] = rating
                if ra:
                    obj["rated_at"] = ra
            body["movies"].append(obj)

            acc: Dict[str, Any] = {"type": "movie", "ids": ids}
            if not unrate:
                acc["rating"] = rating
                if ra:
                    acc["rated_at"] = ra
            accepted.append(acc)

            kept["movies"] += 1
            continue

        if kind == "shows":
            if rating is None and not unrate:
                skip["invalid_rating"] += 1
                continue

            sk, grp = ensure_show(ids)
            if not unrate and rating is not None:
                grp["rating"] = rating
                if ra:
                    grp["rated_at"] = ra

            acc = {"type": "show", "ids": ids}
            if not unrate:
                acc["rating"] = rating
                if ra:
                    acc["rated_at"] = ra
            accepted.append(acc)

            kept["shows"] += 1
            continue

        if kind == "seasons":
            s = it.get("season") or it.get("number")
            if s is None:
                skip["missing_season"] += 1
                continue

            sk, grp = ensure_show(ids)
            s = int(s)

            sp = seasons_index.get((sk, s))
            if not sp:
                sp = {"number": s}
                seasons_index[(sk, s)] = sp
                grp.setdefault("seasons", []).append(sp)
                attach["season_to_show"] += 1

            if not unrate and rating is not None:
                sp["rating"] = rating
                if ra:
                    sp["rated_at"] = ra

            acc = {"type": "season", "ids": ids, "season": s}
            if not unrate:
                acc["rating"] = rating
                if ra:
                    acc["rated_at"] = ra
            accepted.append(acc)

            kept["seasons"] += 1
            continue

        # episodes
        s = it.get("season")
        e = it.get("number") if it.get("number") is not None else it.get("episode")
        if s is None or e is None:
            skip["missing_episode"] += 1
            continue

        sk, grp = ensure_show(ids)
        s = int(s)
        e = int(e)

        sp = seasons_index.get((sk, s))
        if not sp:
            sp = {"number": s}
            seasons_index[(sk, s)] = sp
            grp.setdefault("seasons", []).append(sp)
            attach["season_to_show"] += 1

        ep: Dict[str, Any] = {"number": e}
        if not unrate and rating is not None:
            ep["rating"] = rating
            if ra:
                ep["rated_at"] = ra
        sp.setdefault("episodes", []).append(ep)
        attach["episode_to_season"] += 1

        acc = {"type": "episode", "ids": ids, "season": s, "number": e}
        if not unrate:
            acc["rating"] = rating
            if ra:
                acc["rated_at"] = ra
        accepted.append(acc)

        kept["episodes"] += 1

    if shows:
        for grp in shows.values():
            if "seasons" in grp:
                grp["seasons"] = sorted(grp["seasons"], key=lambda x: int(x.get("number") or 0))
                for sp in grp["seasons"]:
                    if "episodes" in sp:
                        sp["episodes"] = sorted(sp["episodes"], key=lambda x: int(x.get("number") or 0))
        body["shows"] = list(shows.values())

    body = {k: v for k, v in body.items() if v}

    _log(
        f"aggregate seen={seen} attach={attach} skip={skip} "
        f"out_sizes={{k:len(v) for k,v in body.items()}} unrate={unrate}"
    )
    if body.get("shows"):
        try:
            sample = body["shows"][0]
            _log(f"shows.sample ids={sample.get('ids')} seasons={len(sample.get('seasons',[]))}")
        except Exception:
            pass

    return body, accepted

def _chunk(seq: List[Any], n: int) -> Iterable[List[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i:i+n]

def _write(adapter, items: Iterable[Mapping[str, Any]], *, unrate: bool = False) -> Tuple[int, List[Dict[str, Any]]]:
    c = _cfg(adapter)
    apikey = str(c.get("api_key") or "").strip()
    if not apikey:
        _log("write abort: missing api_key")
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]

    sess = adapter.client.session
    tmo  = adapter.cfg.timeout
    rr   = adapter.cfg.max_retries

    chunk = _cfg_int(c, "ratings_chunk_size", 25)
    delay_ms = _cfg_int(c, "ratings_write_delay_ms", 600)
    max_backoff_ms = _cfg_int(c, "ratings_max_backoff_ms", 8000)

    body, accepted = _bucketize(items, unrate=unrate)
    if not body:
        _log("nothing to write (empty body after aggregate)")
        return 0, []

    ok = 0
    unresolved: List[Dict[str, Any]] = []

    for bucket in ("movies", "shows"):
        rows = body.get(bucket) or []
        if not rows:
            continue
        _log(f"{'UNRATE' if unrate else 'UPSERT'} bucket={bucket} rows={len(rows)} chunk={chunk}")
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
                    kinds = ("movies", "shows", "seasons", "episodes")
                    if unrate:
                        removed = d.get("removed") or {}
                        _log(f"UNRATE ok bucket={bucket} removed={removed}")
                        ok += sum(int(removed.get(k) or 0) for k in kinds)
                    else:
                        updated  = d.get("updated")  or {}
                        added    = d.get("added")    or {}
                        existing = d.get("existing") or {}
                        _log(f"UPSERT ok bucket={bucket} updated={updated} added={added} existing={existing}")
                        ok += sum(int(updated.get(k)  or 0) for k in kinds)
                        ok += sum(int(added.get(k)    or 0) for k in kinds)
                        ok += sum(int(existing.get(k) or 0) for k in kinds)
                    time.sleep(max(0.0, delay_ms/1000.0))
                    break

                if r.status_code in (429, 503):
                    _log(f"{'UNRATE' if unrate else 'UPSERT'} throttled {r.status_code} bucket={bucket} attempt={attempt} backoff_ms={backoff}: {(r.text or '')[:180]}")
                    time.sleep(min(max_backoff_ms, backoff)/1000.0)
                    attempt += 1
                    backoff = min(max_backoff_ms, int(backoff*1.6) + 200)
                    if attempt <= 4:
                        continue

                _log(f"{'UNRATE' if unrate else 'UPSERT'} failed {r.status_code} bucket={bucket}: {(r.text or '')[:200]}")
                try:
                    sample = part[0]
                    if bucket == "shows":
                        _log(f"payload.sample shows.ids={sample.get('ids')} seasons={len(sample.get('seasons',[]))}")
                    else:
                        _log(f"payload.sample movies.ids={sample.get('ids')}")
                except Exception:
                    pass
                for x in part:
                    iid = x.get("ids") or {}
                    t = "show" if bucket=="shows" else "movie"
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

    if unresolved:
        _log(f"unresolved count={len(unresolved)}")
    else:
        _log("all writes resolved")

    return ok, unresolved

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    return _write(adapter, items, unrate=False)

def remove(adapter, items: Iterable[Mapping[str, Any]]):
    return _write(adapter, items, unrate=True)

# _export.py
from __future__ import annotations

import csv, io, json, os, re, time
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple
from fastapi import APIRouter, HTTPException, Query, Response
from fastapi.responses import JSONResponse
from cw_platform.config_base import CONFIG as CONFIG_DIR

router = APIRouter(prefix="/api", tags=["export"])
STATE_PATH = Path(os.environ.get("CW_STATE_PATH", str((CONFIG_DIR / "state.json").resolve())))

# IO
def _load_state() -> Dict[str, Any]:
    if not STATE_PATH.exists():
        return {"providers": {}}
    return json.loads(STATE_PATH.read_text(encoding="utf-8"))

def _providers_in_state(s: Dict[str, Any]) -> List[str]:
    return sorted((s.get("providers") or {}).keys())

def _items_bucket(s: Dict[str, Any], provider: str, feature: str) -> Dict[str, Any]:
    try:
        return s["providers"][provider][feature]["baseline"]["items"]  # type: ignore[index]
    except KeyError:
        return {}

# Collect / filter
def _iter_items(s: Dict[str, Any], provider: str, feature: str) -> Iterable[Tuple[str, Dict[str, Any]]]:
    b = _items_bucket(s, provider, feature)
    for k, it in (b or {}).items():
        yield k, (it or {})

def _norm_ids(ids: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    if not isinstance(ids, dict):
        return out
    v = ids.get("imdb")
    if v:
        m = re.search(r"(\d+)", str(v))
        if m:
            out["imdb"] = f"tt{m.group(1)}"
    for ns in ("tmdb", "tvdb", "trakt", "simkl", "mdblist"):
        v = ids.get(ns)
        if v is None:
            continue
        m = re.search(r"(\d+)", str(v))
        out[ns] = m.group(1) if m else str(v)
    if ids.get("slug"):
        out["slug"] = str(ids["slug"])
    return out

def _pick_title(it: Dict[str, Any]) -> str:
    return str(it.get("title") or it.get("name") or it.get("series_title") or it.get("show_title") or "")

def _pick_year(it: Dict[str, Any]) -> str:
    # Try direct ints/strings
    for k in ("year", "release_year", "first_air_year", "movie_year", "show_year"):
        v = it.get(k)
        if v:
            return str(v)
    # Extract YYYY from common date fields
    for k in ("first_aired", "released", "air_date", "release_date"):
        v = it.get(k)
        if isinstance(v, str):
            m = re.search(r"\b(19|20)\d{2}\b", v)
            if m:
                return m.group(0)
    return ""

def _row_base(it: Dict[str, Any]) -> Tuple[str, str, str, str, Dict[str, str]]:
    t = str(it.get("type") or "")
    title = _pick_title(it)
    year = _pick_year(it)
    ids = _norm_ids(it.get("ids") or {})
    watched = (it.get("watched_at") or it.get("watchedAt") or it.get("viewed_at") or it.get("rated_at") or "") or ""
    return t, title, year, watched, ids

def _match_query(key: str, it: Dict[str, Any], q: str) -> bool:
    if not q:
        return True
    q = q.strip().lower()
    if not q:
        return True
    t, title, year, _wd, ids = _row_base(it)
    hay = " ".join(filter(None, [
        key.lower(),
        title.lower(),
        str(year or "").lower(),
        str(it.get("series_title") or "").lower(),
        *(f"{k}:{v}".lower() for k, v in (ids or {}).items()),
    ]))
    tokens = [tok for tok in re.split(r"\s+", q) if tok]
    return all(tok in hay for tok in tokens)

def _filter_keys(s: Dict[str, Any], provider: str, feature: str, q: str) -> List[str]:
    keys: List[str] = []
    for k, it in _iter_items(s, provider, feature):
        if _match_query(k, it, q):
            keys.append(k)
    return keys

# CSV
def _csv_response(filename: str, header: Optional[List[str]], rows: Iterable[List[str]]) -> Response:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    if header:
        w.writerow(header)
    for r in rows:
        w.writerow([str(x) if x is not None else "" for x in r])
    data = buf.getvalue().encode("utf-8")
    return Response(
        content=data,
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f'attachment; filename="{filename}"', "Cache-Control": "no-store"},
    )

# Helpers
def _rating_1_10(val: Any) -> str:
    try:
        f = float(val)
        if f <= 0:
            return ""
        if f > 10:
            f = 10
        return str(int(f) if f.is_integer() else f)
    except Exception:
        return ""

def _title_type_for_imdb(t: str) -> str:
    t = (t or "").lower()
    return "movie" if t == "movie" else "tvSeries"

# Formats
def _build_letterboxd(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    src_iter = ((k, it) for k, it in _iter_items(s, provider, feature) if (not keys or k in keys))
    if feature == "watchlist":
        header = ["imdbID", "tmdbID", "Title", "Year"]
        rows = (
            [ids.get("imdb", ""), ids.get("tmdb", ""), title, year]
            for k, it in src_iter
            for _t, title, year, _wd, ids in [_row_base(it)]
            if _t == "movie"
        )
    elif feature == "history":
        header = ["imdbID", "tmdbID", "Title", "Year", "WatchedDate"]
        rows = (
            [ids.get("imdb", ""), ids.get("tmdb", ""), title, year, watched]
            for k, it in src_iter
            for _t, title, year, watched, ids in [_row_base(it)]
        )
    elif feature == "ratings":
        header = ["imdbID", "tmdbID", "Title", "Year", "Rating"]
        rows = (
            [ids.get("imdb", ""), ids.get("tmdb", ""), title, year, str(it.get("rating") or it.get("user_rating") or "")]
            for k, it in src_iter
            for _t, title, year, _wd, ids in [_row_base(it)]
        )
    else:
        raise HTTPException(400, "Unsupported feature for Letterboxd")
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"letterboxd_{feature}_{provider.lower()}_{ts}.csv", header, rows)

def _build_imdb(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    if feature != "watchlist":
        raise HTTPException(400, "IMDb export supports watchlist only")
    header = ["const"]
    rows = []
    for k, it in _iter_items(s, provider, "watchlist"):
        if keys and k not in keys:
            continue
        _t, _title, _year, _wd, ids = _row_base(it)
        if ids.get("imdb"):
            rows.append([ids["imdb"]])
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"imdb_watchlist_{provider.lower()}_{ts}.csv", header, rows)

def _build_justwatch(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    header = ["tmdbID", "imdbID", "Title", "Year", "Type"]
    rows: List[List[str]] = []
    for k, it in _iter_items(s, provider, feature):
        if keys and k not in keys:
            continue
        t, title, year, _wd, ids = _row_base(it)
        rows.append([ids.get("tmdb", ""), ids.get("imdb", ""), title, year, t])
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"justwatch_{feature}_{provider.lower()}_{ts}.csv", header, rows)

def _build_yamtrack(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    header = ["imdbID", "tmdbID", "Title", "Year", "Rating", "WatchedDate", "Feature", "Provider"]
    rows: List[List[str]] = []
    for k, it in _iter_items(s, provider, feature):
        if keys and k not in keys:
            continue
        t, title, year, watched, ids = _row_base(it)
        rating = it.get("rating") or it.get("user_rating") or ""
        rows.append([ids.get("imdb", ""), ids.get("tmdb", ""), title, year, rating, watched, feature, provider])
    ts = time.strftime("%Y%m%d")
    return _csv_response(f"yamtrack_{feature}_{provider.lower()}_{ts}.csv", header, rows)

# TMDB auto-dialect
def _tmdb_build_imdb_v3(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    ts = time.strftime("%Y%m%d")
    if feature == "watchlist":
        header = [
            "Position","Const","Created","Modified","Description","Title","URL","Title Type",
            "IMDb Rating","Runtime (mins)","Year","Genres","Num Votes","Release Date",
            "Directors","Your Rating","Date Rated"
        ]
        rows: List[List[str]] = []
        pos = 0
        for k, it in _iter_items(s, provider, "watchlist"):
            if keys and k not in keys:
                continue
            t, title, year, _wd, ids = _row_base(it)
            imdb = ids.get("imdb")
            if not imdb:
                continue
            pos += 1
            url = f"https://www.imdb.com/title/{imdb}/"
            rows.append([pos, imdb, "", "", "", title, url, _title_type_for_imdb(t),
                         "", "", year, "", "", "", "", "", ""])
        return _csv_response(f"tmdb_imdbv3_watchlist_{provider.lower()}_{ts}.csv", header, rows)
    if feature == "ratings":
        header = [
            "Const","Your Rating","Date Rated","Title","URL","Title Type",
            "IMDb Rating","Runtime (mins)","Year","Genres","Num Votes","Release Date","Directors"
        ]
        rows: List[List[str]] = []
        for k, it in _iter_items(s, provider, "ratings"):
            if keys and k not in keys:
                continue
            t, title, year, watched, ids = _row_base(it)
            imdb = ids.get("imdb")
            if not imdb:
                continue
            rating = _rating_1_10(it.get("rating") or it.get("user_rating") or "")
            url = f"https://www.imdb.com/title/{imdb}/"
            date_rated = (it.get("rated_at") or watched or "") or ""
            rows.append([imdb, rating, date_rated, title, url, _title_type_for_imdb(t),
                         "", "", year, "", "", "", ""])
        return _csv_response(f"tmdb_imdbv3_ratings_{provider.lower()}_{ts}.csv", header, rows)
    raise HTTPException(400, "TMDB supports watchlist and ratings only")

def _tmdb_build_trakt_v2(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    header = [
        "rated_at","type","title","year","trakt_rating","trakt_id","imdb_id","tmdb_id","tvdb_id",
        "season","episode","show_title","show_year","show_trakt_id","show_imdb_id","show_tmdb_id","show_tvdb_id",
        "episode_imdb_id","episode_tmdb_id","episode_tvdb_id","genres","rating"
    ]
    ts = time.strftime("%Y%m%d")
    rows: List[List[str]] = []
    src = "ratings" if feature == "ratings" else "watchlist"
    for k, it in _iter_items(s, provider, src):
        if keys and k not in keys:
            continue
        t, title, year, watched, ids = _row_base(it)
        rating = _rating_1_10(it.get("rating") or it.get("user_rating") or "")
        rows.append([
            watched if feature == "ratings" else "",
            (t or "movie").lower(),
            title, year,
            "", ids.get("trakt",""), ids.get("imdb",""), ids.get("tmdb",""), ids.get("tvdb",""),
            "", "", "", "", "", "", "", "",
            "", "", "", "",
            rating if feature == "ratings" else ""
        ])
    return _csv_response(f"tmdb_traktv2_{src}_{provider.lower()}_{ts}.csv", header, rows)

def _tmdb_build_simkl_v1(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    header = ["SIMKL_ID","Title","Type","Year","Watchlist","LastEpWatched","WatchedDate","Rating","Memo","TVDB","TMDB","IMDB"]
    ts = time.strftime("%Y%m%d")
    rows: List[List[str]] = []
    src = "ratings" if feature == "ratings" else "watchlist"
    for k, it in _iter_items(s, provider, src):
        if keys and k not in keys:
            continue
        t, title, year, watched, ids = _row_base(it)
        rating = _rating_1_10(it.get("rating") or it.get("user_rating") or "")
        rows.append([
            ids.get("simkl",""),
            title,
            (t or "movie").capitalize(),
            year,
            "1" if src == "watchlist" else "",
            "",
            watched if src == "ratings" else "",
            rating if src == "ratings" else "",
            "",
            ids.get("tvdb",""),
            ids.get("tmdb",""),
            ids.get("imdb",""),
        ])
    return _csv_response(f"tmdb_simklv1_{src}_{provider.lower()}_{ts}.csv", header, rows)

def _build_tmdb(provider: str, feature: str, s: Dict[str, Any], keys: List[str]) -> Response:
    p = provider.upper().strip()
    if p == "TRAKT":
        return _tmdb_build_trakt_v2(provider, feature, s, keys)
    if p == "SIMKL":
        return _tmdb_build_simkl_v1(provider, feature, s, keys)
    # MDBLIST (and others) fall back to IMDb v3 dialect
    return _tmdb_build_imdb_v3(provider, feature, s, keys)

_BUILDERS = {
    "letterboxd": _build_letterboxd,
    "imdb": _build_imdb,
    "justwatch": _build_justwatch,
    "yamtrack": _build_yamtrack,
    "tmdb": _build_tmdb,
}

# API
@router.get("/export/options", response_class=JSONResponse)
def api_export_options():
    s = _load_state()
    provs = _providers_in_state(s)
    features = ["watchlist", "history", "ratings"]
    counts: Dict[str, Dict[str, int]] = {p: {f: len(_items_bucket(s, p, f) or {}) for f in features} for p in provs}
    formats = {
        "watchlist": ["letterboxd", "imdb", "justwatch", "yamtrack", "tmdb"],
        "history":   ["letterboxd", "justwatch", "yamtrack"],
        "ratings":   ["letterboxd", "yamtrack", "tmdb"],
    }
    labels = {
        "letterboxd": "Letterboxd",
        "imdb": "IMDb (list)",
        "justwatch": "JustWatch",
        "yamtrack": "Yamtrack",
        "tmdb": "TMDB (Auto: IMDb/Trakt/SIMKL)",
    }
    return {"providers": provs, "counts": counts, "formats": formats, "labels": labels}

@router.get("/export/sample", response_class=JSONResponse)
def api_export_sample(
    provider: str = Query("", description="TRAKT|PLEX|EMBY|JELLYFIN|SIMKL|MDBLIST"),
    feature: str = Query("watchlist", pattern="^(watchlist|history|ratings)$"),
    limit: int = Query(25, ge=1, le=250),
    q: str = Query("", description="case-insensitive multi-token contains"),
):
    s = _load_state()
    provider = (provider or "").upper().strip()
    if provider and provider in _providers_in_state(s):
        keys = _filter_keys(s, provider, feature, q)
    else:
        keys = []
    items = []
    for i, k in enumerate(keys):
        it = _items_bucket(s, provider, feature).get(k, {})
        t, title, year, watched, ids = _row_base(it)
        items.append({
            "key": k, "type": t, "title": title, "year": year,
            "watched_at": watched, "ids": ids, "rating": it.get("rating") or it.get("user_rating")
        })
        if i + 1 >= limit:
            break
    return {"items": items, "total": len(keys)}

@router.get("/export/file")
def api_export_file(
    provider: str = Query("", description="TRAKT|PLEX|EMBY|JELLYFIN|SIMKL|MDBLIST"),
    feature: str = Query("watchlist", pattern="^(watchlist|history|ratings)$"),
    format: str = Query("letterboxd", pattern="^(letterboxd|imdb|justwatch|yamtrack|tmdb)$"),
    q: str = Query("", description="optional search filter (server-side)"),
    ids: str = Query("", description="optional CSV of keys to include (overrides q)"),
):
    s = _load_state()
    provider = (provider or "").upper().strip()
    feature = feature.lower().strip()
    fmt = format.lower().strip()
    if fmt not in _BUILDERS:
        raise HTTPException(400, "Unknown format")
    if feature not in ("watchlist","ratings","history"):
        raise HTTPException(400, "Unsupported feature")
    if fmt == "tmdb" and feature == "history":
        raise HTTPException(400, "TMDB supports watchlist and ratings only")

    keys: List[str]
    if ids.strip():
        keys = [k.strip() for k in ids.split(",") if k.strip()]
    else:
        keys = _filter_keys(s, provider, feature, q) if provider in _providers_in_state(s) else []

    return _BUILDERS[fmt](provider or "TRAKT", feature, s, keys)
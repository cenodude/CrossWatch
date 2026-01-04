#!/usr/bin/env python3
# MDBList cleanup + backup/restore (watchlist + ratings).
from __future__ import annotations

import gzip
import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Iterable, Optional

import requests

CONFIG_PATH = Path("/config/config.json")
BACKUP_DIR = Path("/config/backup")
RETENTION_DAYS = 15

BASE = "https://api.mdblist.com"
WL_LIST = f"{BASE}/watchlist/items"
WL_MOD = f"{BASE}/watchlist/items/{{action}}"
RT_LIST = f"{BASE}/sync/ratings"
RT_UPSERT = f"{BASE}/sync/ratings"
RT_REMOVE = f"{BASE}/sync/ratings/remove"


# ---------- utils ----------
def jload(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {}


def cfg_block(cfg: dict[str, Any]) -> dict[str, Any]:
    b = cfg.get("mdblist") or {}
    return b if isinstance(b, dict) else {}


def safe_int(v: Any) -> Optional[int]:
    try:
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def retry_sleep(attempt: int) -> None:
    time.sleep(min(6.0, 0.6 * (attempt + 1)))


def ensure_backup_dir() -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def cleanup_old_backups() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    for p in BACKUP_DIR.glob("mdblist_backup_*.json.gz"):
        try:
            if datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) < cutoff:
                p.unlink()
        except Exception:
            pass


def list_backups() -> list[Path]:
    ensure_backup_dir()
    return sorted(BACKUP_DIR.glob("mdblist_backup_*.json.gz"), key=lambda p: p.stat().st_mtime, reverse=True)


def choose_backup() -> Optional[Path]:
    files = list_backups()
    if not files:
        print("No backups found.")
        return None
    print("\nAvailable backups:")
    for i, p in enumerate(files, 1):
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        print(f"{i}. {p.name} ({age.days}d ago)")
    s = input("Select backup #: ").strip()
    if not (s.isdigit() and 1 <= int(s) <= len(files)):
        return None
    return files[int(s) - 1]


def load_backup(p: Path) -> dict[str, Any]:
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return json.load(f) or {}


def show(rows: list[dict[str, Any]], cols: list[str], page: int = 25) -> None:
    if not rows:
        print("(none)")
        return
    for i in range(0, len(rows), page):
        chunk = rows[i : i + page]
        print(f"\nShowing {i + 1}-{i + len(chunk)} of {len(rows)}")
        head = " | ".join(c.ljust(12) for c in cols)
        print(head)
        print("-" * len(head))
        for r in chunk:
            print(" | ".join(str(r.get(c, ""))[:60].ljust(12) for c in cols))
        if i + page < len(rows):
            if input("[Enter]=Next, q=Quit: ").strip().lower() == "q":
                break


#  CLient
def build_meta(cfg: dict[str, Any]) -> dict[str, Any]:
    b = cfg_block(cfg)
    apikey = str(b.get("api_key") or "").strip()
    if not apikey:
        raise RuntimeError("Missing mdblist.api_key in /config/config.json.")
    return {
        "apikey": apikey,
        "timeout": float(b.get("timeout", 10) or 10),
        "retries": int(b.get("max_retries", 3) or 3),
        "wl_page_size": int(b.get("watchlist_page_size", 200) or 200),
        "wl_batch_size": int(b.get("watchlist_batch_size", 100) or 100),
        "rt_per_page": int(b.get("ratings_per_page", 1000) or 1000),
        "rt_chunk_size": int(b.get("ratings_chunk_size", 25) or 25),
        "rt_delay_ms": int(b.get("ratings_write_delay_ms", 600) or 600),
        "rt_max_backoff_ms": int(b.get("ratings_max_backoff_ms", 8000) or 8000),
    }


def request_json(
    ses: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    body: Optional[dict[str, Any]] = None,
    timeout: float = 10.0,
    retries: int = 3,
) -> tuple[int, Any, str]:
    last_text = ""
    for i in range(max(1, int(retries))):
        try:
            r = ses.request(method, url, params=params, json=body, timeout=timeout)
            last_text = (r.text or "")
            if r.status_code in (429, 503) and i < retries - 1:
                retry_sleep(i)
                continue
            if 500 <= r.status_code <= 599 and i < retries - 1:
                retry_sleep(i)
                continue
            if not (r.text or "").strip():
                return r.status_code, {}, last_text
            try:
                return r.status_code, r.json(), last_text
            except Exception:
                return r.status_code, {}, last_text
        except Exception:
            if i == retries - 1:
                raise
            retry_sleep(i)
    return 0, {}, last_text


# Watchlist
def _pick_wl_kind(row: dict[str, Any]) -> str:
    t = str(row.get("mediatype") or row.get("type") or "").strip().lower()
    return "show" if t in ("show", "tv", "series", "shows") else "movie"


def _wl_to_minimal(row: dict[str, Any]) -> dict[str, Any]:
    ids = {
        "imdb": row.get("imdb_id") or row.get("imdb"),
        "tmdb": row.get("tmdb_id") or row.get("tmdb"),
        "tvdb": row.get("tvdb_id") or row.get("tvdb"),
        "mdblist": row.get("id"),
    }
    title = str(
        row.get("title")
        or row.get("name")
        or row.get("original_title")
        or row.get("original_name")
        or ""
    ).strip()
    year = (
        row.get("year")
        or row.get("release_year")
        or (int(str(row.get("release_date"))[:4]) if row.get("release_date") else None)
        or row.get("first_air_year")
        or (int(str(row.get("first_air_date"))[:4]) if row.get("first_air_date") else None)
    )
    out: dict[str, Any] = {"type": _pick_wl_kind(row), "ids": {k: v for k, v in ids.items() if v}}
    if title:
        out["title"] = title
    if year is not None:
        y = safe_int(year)
        if y:
            out["year"] = y
    return out


def _wl_key(item: dict[str, Any]) -> str:
    ids = dict(item.get("ids") or {})
    imdb = str(ids.get("imdb") or "").strip()
    if imdb:
        return f"imdb:{imdb}"
    tmdb = safe_int(ids.get("tmdb"))
    if tmdb is not None:
        return f"tmdb:{tmdb}"
    tvdb = safe_int(ids.get("tvdb"))
    if tvdb is not None:
        return f"tvdb:{tvdb}"
    mdbl = ids.get("mdblist")
    if mdbl:
        return f"mdblist:{mdbl}"
    title = str(item.get("title") or "").strip()
    year = safe_int(item.get("year"))
    if title and year:
        return f"title:{title}|year:{year}"
    return json.dumps(item, sort_keys=True)


def _parse_rows(data: Any) -> tuple[list[dict[str, Any]], Optional[int]]:
    if isinstance(data, dict):
        total: Optional[int] = None
        for key in ("total_items", "total", "count", "items_total"):
            try:
                v = int(data.get(key) or 0)
                if v > 0:
                    total = v
                    break
            except Exception:
                pass

        pag = data.get("pagination")
        if total is None and isinstance(pag, dict):
            for key in ("total_items", "total", "count", "items_total"):
                try:
                    v = int(pag.get(key) or 0)
                    if v > 0:
                        total = v
                        break
                except Exception:
                    pass

        if "movies" in data or "shows" in data:
            rows: list[Any] = []
            for bucket in ("movies", "shows"):
                val = data.get(bucket)
                if isinstance(val, list):
                    rows.extend(val)
                elif isinstance(val, dict):
                    rows.extend(val.get("results") or val.get("items") or [])
            return [r for r in rows if isinstance(r, dict)], total

        rows = data.get("results") or data.get("items") or []
        return [r for r in rows if isinstance(r, dict)], total

    if isinstance(data, list):
        return [r for r in data if isinstance(r, dict)], None

    return [], None


def collect_watchlist(meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    seen: set[str] = set()
    offset = 0
    limit = int(meta["wl_page_size"])
    stagnant = 0
    while True:
        st, data, _ = request_json(
            ses,
            "GET",
            WL_LIST,
            params={"apikey": meta["apikey"], "limit": limit, "offset": offset, "unified": 1},
            timeout=meta["timeout"],
            retries=meta["retries"],
        )
        if st != 200:
            raise RuntimeError(f"GET watchlist failed: {st}")
        rows, total = _parse_rows(data)
        if not rows:
            break

        new_unique = 0
        for r in rows:
            m = _wl_to_minimal(r)
            k = _wl_key(m)
            if k in seen:
                continue
            seen.add(k)
            out.append(m)
            new_unique += 1

        offset += len(rows)
        if total is not None and len(seen) >= int(total):
            break

        if new_unique == 0:
            stagnant += 1
            if stagnant >= 2:
                break
        else:
            stagnant = 0

    return out


def _wl_payload(items: Iterable[dict[str, Any]]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for it in items:
        ids = dict((it.get("ids") or {}) if isinstance(it, dict) else {})
        imdb = ids.get("imdb")
        tmdb = ids.get("tmdb")
        if not (imdb or tmdb):
            unresolved.append({"item": it, "hint": "missing imdb/tmdb"})
            continue
        obj = {k: v for k, v in {"imdb": imdb, "tmdb": tmdb}.items() if v is not None}
        if str(it.get("type") or "").lower() in ("show", "tv", "series", "shows"):
            shows.append(obj)
        else:
            movies.append(obj)
    payload: dict[str, Any] = {}
    if movies:
        payload["movies"] = movies
    if shows:
        payload["shows"] = shows
    return payload, unresolved


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def write_watchlist(meta: dict[str, Any], ses: requests.Session, action: str, items: list[dict[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    batch = int(meta["wl_batch_size"])
    ok = 0
    unresolved: list[dict[str, Any]] = []
    for part in _chunk(items, batch):
        payload, unr = _wl_payload(part)
        unresolved.extend(unr)
        if not payload:
            continue
        st, data, text = request_json(
            ses,
            "POST",
            WL_MOD.format(action=action),
            params={"apikey": meta["apikey"]},
            body=payload,
            timeout=meta["timeout"],
            retries=meta["retries"],
        )
        if st not in (200, 201):
            for it in part:
                unresolved.append({"item": it, "hint": f"http:{st}"})
            continue
        if isinstance(data, dict):
            added = data.get("added") or {}
            existing = data.get("existing") or {}
            removed = data.get("deleted") or data.get("removed") or {}
            if action == "add":
                ok += int(added.get("movies") or 0) + int(added.get("shows") or 0)
                ok += int(existing.get("movies") or 0) + int(existing.get("shows") or 0)
            else:
                ok += int(removed.get("movies") or 0) + int(removed.get("shows") or 0)
        else:
            ok += len(part)
        if (text or "").strip() and os.getenv("CW_MDBLIST_DEBUG"):
            print(f"[MDBLIST:watchlist] {action} -> {st}")
    return ok, unresolved


# Ratings
def _valid_rating(v: Any) -> Optional[int]:
    try:
        i = int(str(v).strip())
        return i if 1 <= i <= 10 else None
    except Exception:
        return None


def _ids_from_obj(obj: Any) -> dict[str, Any]:
    o = obj if isinstance(obj, dict) else {}
    ids = dict(o.get("ids") or {})
    if not ids:
        ids = {
            "imdb": o.get("imdb") or o.get("imdb_id"),
            "tmdb": o.get("tmdb") or o.get("tmdb_id"),
            "tvdb": o.get("tvdb") or o.get("tvdb_id"),
        }
    out: dict[str, Any] = {}
    imdb_val = ids.get("imdb")
    if imdb_val:
        out["imdb"] = str(imdb_val)
    tmdb_int = safe_int(ids.get("tmdb"))
    if tmdb_int is not None:
        out["tmdb"] = tmdb_int
    tvdb_int = safe_int(ids.get("tvdb"))
    if tvdb_int is not None:
        out["tvdb"] = tvdb_int
    if out.get("imdb") and out.get("tmdb") and out.get("tvdb"):
        out.pop("tvdb", None)
    return out


def _row_movie(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    rating = _valid_rating(row.get("rating"))
    if rating is None:
        return None
    mv = row.get("movie") or {}
    ids = _ids_from_obj(mv)
    if not ids:
        return None
    title = str(mv.get("title") or mv.get("name") or "").strip()
    year = safe_int(mv.get("year") or mv.get("release_year"))
    out: dict[str, Any] = {"type": "movie", "ids": ids, "rating": rating}
    if row.get("rated_at"):
        out["rated_at"] = row["rated_at"]
    if title:
        out["title"] = title
    if year:
        out["year"] = year
    return out


def _row_show(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    rating = _valid_rating(row.get("rating"))
    if rating is None:
        return None
    sh = row.get("show") or {}
    ids = _ids_from_obj(sh)
    if not ids:
        return None
    title = str(sh.get("title") or sh.get("name") or "").strip()
    y = sh.get("year") or sh.get("first_air_year")
    if not y:
        fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
        if len(fa) >= 4 and fa[:4].isdigit():
            y = int(fa[:4])
    year = safe_int(y)
    out: dict[str, Any] = {"type": "show", "ids": ids, "rating": rating}
    if row.get("rated_at"):
        out["rated_at"] = row["rated_at"]
    if title:
        out["title"] = title
    if year:
        out["year"] = year
    return out


def _row_season_top(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    rating = _valid_rating(row.get("rating"))
    if rating is None:
        return None
    sv = row.get("season") or {}
    show = sv.get("show") or {}
    show_ids = _ids_from_obj(show)
    if not show_ids:
        return None
    s = safe_int(sv.get("number"))
    if s is None:
        return None
    title = str(show.get("title") or show.get("name") or "").strip()
    y = show.get("year") or show.get("first_air_year")
    if not y:
        fa = str(show.get("first_air_date") or show.get("first_aired") or "").strip()
        if len(fa) >= 4 and fa[:4].isdigit():
            y = int(fa[:4])
    year = safe_int(y)
    out: dict[str, Any] = {"type": "season", "show_ids": show_ids, "season": s, "rating": rating}
    if row.get("rated_at"):
        out["rated_at"] = row["rated_at"]
    if title:
        out["title"] = title
    if year:
        out["year"] = year
    return out


def _row_episode_top(row: dict[str, Any]) -> Optional[dict[str, Any]]:
    rating = _valid_rating(row.get("rating"))
    if rating is None:
        return None
    ev = row.get("episode") or {}
    show = ev.get("show") or {}
    show_ids = _ids_from_obj(show)
    if not show_ids:
        return None
    s = safe_int(ev.get("season"))
    n = safe_int(ev.get("number") if ev.get("number") is not None else ev.get("episode"))
    if s is None or n is None:
        return None
    title = str(show.get("title") or show.get("name") or "").strip()
    y = show.get("year") or show.get("first_air_year")
    if not y:
        fa = str(show.get("first_air_date") or show.get("first_aired") or "").strip()
        if len(fa) >= 4 and fa[:4].isdigit():
            y = int(fa[:4])
    year = safe_int(y)
    out: dict[str, Any] = {"type": "episode", "show_ids": show_ids, "season": s, "number": n, "rating": rating}
    if row.get("rated_at"):
        out["rated_at"] = row["rated_at"]
    if title:
        out["title"] = title
    if year:
        out["year"] = year
    return out


def _show_key(ids: dict[str, Any]) -> str:
    if ids.get("imdb"):
        return f"imdb:{ids['imdb']}"
    if ids.get("tmdb") is not None:
        return f"tmdb:{ids['tmdb']}"
    if ids.get("tvdb") is not None:
        return f"tvdb:{ids['tvdb']}"
    return json.dumps(ids, sort_keys=True)


def _rating_key(it: dict[str, Any]) -> str:
    typ = str(it.get("type") or "").strip().lower()
    if typ == "movie":
        return f"movie:{_show_key(_ids_from_obj(it.get('ids') or {}))}"
    if typ == "show":
        return f"show:{_show_key(_ids_from_obj(it.get('ids') or {}))}"
    if typ == "season":
        show_ids = _ids_from_obj(it.get("show_ids") or it.get("ids") or {})
        s = safe_int(it.get("season"))
        return f"season:{_show_key(show_ids)}:S{(s if s is not None else -1)}"
    if typ == "episode":
        show_ids = _ids_from_obj(it.get("show_ids") or it.get("ids") or {})
        s = safe_int(it.get("season"))
        n = safe_int(it.get("number") if it.get("number") is not None else it.get("episode"))
        ss = s if s is not None else -1
        nn = n if n is not None else -1
        return f"episode:{_show_key(show_ids)}:{ss}x{nn}"
    return f"other:{json.dumps(it, sort_keys=True)}"


def _merge_item(dst: dict[str, dict[str, Any]], item: dict[str, Any]) -> None:
    k = _rating_key(item)
    cur = dst.get(k)
    if not cur:
        dst[k] = item
        return
    a = str(cur.get("rated_at") or "")
    b = str(item.get("rated_at") or "")
    if b >= a:
        dst[k] = item


def _pag_limit(pag: Any, default: int) -> int:
    if isinstance(pag, dict):
        for k in ("limit", "per_page", "page_size"):
            v = safe_int(pag.get(k))
            if v and v > 0:
                return v
    return default


def _pag_total_pages(pag: Any) -> Optional[int]:
    if isinstance(pag, dict):
        return safe_int(pag.get("page_count") or pag.get("total_pages") or pag.get("pages"))
    return None


def _pag_has_more(pag: Any, *, page: int, per_page: int, lengths: Iterable[int]) -> bool:
    if isinstance(pag, dict):
        hm = pag.get("has_more")
        if hm is not None:
            return bool(hm)
        tp = _pag_total_pages(pag)
        if tp is not None:
            return page < tp
    eff = _pag_limit(pag, per_page)
    return any(int(x) >= eff for x in lengths)


def _collect_ratings_type(meta: dict[str, Any], ses: requests.Session, type_name: str, per_page: int) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    max_pages = 9999
    stagnant = 0
    while True:
        st, data, _ = request_json(
            ses,
            "GET",
            RT_LIST,
            params={"apikey": meta["apikey"], "page": page, "limit": per_page, "type": type_name},
            timeout=meta["timeout"],
            retries=meta["retries"],
        )
        if st != 200:
            break
        if not isinstance(data, dict):
            break
        rows = data.get(type_name) or []
        if not isinstance(rows, list) or not rows:
            break
        before = len(out)
        for row in rows:
            if not isinstance(row, dict):
                continue
            if type_name == "seasons":
                m = _row_season_top(row)
            else:
                m = _row_episode_top(row)
            if m:
                out.append(m)
        if len(out) == before:
            stagnant += 1
            if stagnant >= 2:
                break
        else:
            stagnant = 0
        pag = data.get("pagination") or {}
        has_more = _pag_has_more(pag, page=page, per_page=per_page, lengths=(len(rows),))
        if not bool(has_more) or page >= max_pages:
            break
        page += 1
    return out


def collect_ratings(meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    idx: dict[str, dict[str, Any]] = {}
    page = 1
    per_page = int(meta["rt_per_page"])
    max_pages = 9999
    stagnant = 0
    while True:
        st, data, _ = request_json(
            ses,
            "GET",
            RT_LIST,
            params={"apikey": meta["apikey"], "page": page, "limit": per_page},
            timeout=meta["timeout"],
            retries=meta["retries"],
        )
        if st != 200:
            raise RuntimeError(f"GET ratings failed: {st}")
        if not isinstance(data, dict):
            break
        movies = data.get("movies") or []
        shows = data.get("shows") or []
        seasons_top = data.get("seasons") or []
        episodes_top = data.get("episodes") or []

        before = len(idx)

        for row in movies:
            if isinstance(row, dict):
                m = _row_movie(row)
                if m:
                    _merge_item(idx, m)

        for row in shows:
            if not isinstance(row, dict):
                continue
            m = _row_show(row)
            if m:
                _merge_item(idx, m)

            sh = row.get("show") or {}
            show_ids = _ids_from_obj(sh)
            title = str(sh.get("title") or sh.get("name") or "").strip()
            y = sh.get("year") or sh.get("first_air_year")
            if not y:
                fa = str(sh.get("first_air_date") or sh.get("first_aired") or "").strip()
                if len(fa) >= 4 and fa[:4].isdigit():
                    y = int(fa[:4])
            year = safe_int(y)

            for sv in row.get("seasons") or []:
                if not isinstance(sv, dict):
                    continue
                sr = _valid_rating(sv.get("rating"))
                sn = safe_int(sv.get("number"))
                if sr is not None and sn is not None and show_ids:
                    sm: dict[str, Any] = {"type": "season", "show_ids": show_ids, "season": sn, "rating": sr}
                    if sv.get("rated_at"):
                        sm["rated_at"] = sv["rated_at"]
                    if title:
                        sm["title"] = title
                    if year:
                        sm["year"] = year
                    _merge_item(idx, sm)
                for ev in sv.get("episodes") or []:
                    if not isinstance(ev, dict):
                        continue
                    er = _valid_rating(ev.get("rating"))
                    en = safe_int(ev.get("number") if ev.get("number") is not None else ev.get("episode"))
                    if er is None or sn is None or en is None or not show_ids:
                        continue
                    em: dict[str, Any] = {
                        "type": "episode",
                        "show_ids": show_ids,
                        "season": sn,
                        "number": en,
                        "rating": er,
                    }
                    if ev.get("rated_at"):
                        em["rated_at"] = ev["rated_at"]
                    if title:
                        em["title"] = title
                    if year:
                        em["year"] = year
                    _merge_item(idx, em)

        for row in seasons_top:
            if isinstance(row, dict):
                m = _row_season_top(row)
                if m:
                    _merge_item(idx, m)
        for row in episodes_top:
            if isinstance(row, dict):
                m = _row_episode_top(row)
                if m:
                    _merge_item(idx, m)

        if len(idx) == before:
            stagnant += 1
            if stagnant >= 2:
                break
        else:
            stagnant = 0

        pag = data.get("pagination") or {}
        has_more = _pag_has_more(
            pag,
            page=page,
            per_page=per_page,
            lengths=(len(movies), len(shows), len(seasons_top), len(episodes_top)),
        )
        if not bool(has_more) or page >= max_pages:
            break
        page += 1

    for m in _collect_ratings_type(meta, ses, "seasons", per_page):
        _merge_item(idx, m)
    for m in _collect_ratings_type(meta, ses, "episodes", per_page):
        _merge_item(idx, m)

    return list(idx.values())


def bucketize_ratings(items: Iterable[dict[str, Any]], *, unrate: bool) -> dict[str, Any]:
    body: dict[str, Any] = {"movies": [], "shows": []}
    shows: dict[str, dict[str, Any]] = {}
    seasons_index: dict[tuple[str, int], dict[str, Any]] = {}

    def ensure_show(ids: dict[str, Any]) -> tuple[str, dict[str, Any]]:
        sk = _show_key(ids)
        grp = shows.get(sk)
        if not grp:
            grp2: dict[str, Any] = {}
            grp2["ids"] = ids
            shows[sk] = grp2
            return sk, grp2
        return sk, grp

    for it in items:
        typ = str(it.get("type") or "").strip().lower()
        if typ == "movie":
            ids = _ids_from_obj(it.get("ids") or {})
            if not ids:
                continue
            obj: dict[str, Any] = {"ids": ids}
            if not unrate:
                r = _valid_rating(it.get("rating"))
                if r is None:
                    continue
                obj["rating"] = r
                if it.get("rated_at"):
                    obj["rated_at"] = it["rated_at"]
            body["movies"].append(obj)
            continue

        if typ == "show":
            ids = _ids_from_obj(it.get("ids") or {})
            if not ids:
                continue
            _, grp = ensure_show(ids)
            if not unrate:
                r = _valid_rating(it.get("rating"))
                if r is not None:
                    grp["rating"] = r
                    if it.get("rated_at"):
                        grp["rated_at"] = it["rated_at"]
            continue

        if typ == "season":
            show_ids = _ids_from_obj(it.get("show_ids") or {})
            s = safe_int(it.get("season"))
            if not show_ids or s is None:
                continue
            sk, grp = ensure_show(show_ids)
            sp = seasons_index.get((sk, s))
            if not sp:
                sp2: dict[str, Any] = {}
                sp2["number"] = s
                seasons_index[(sk, s)] = sp2
                seasons = grp.get("seasons")
                if not isinstance(seasons, list):
                    seasons = []
                    grp["seasons"] = seasons
                seasons.append(sp2)
                sp = sp2
            if not unrate:
                r = _valid_rating(it.get("rating"))
                if r is None:
                    continue
                sp["rating"] = r
                if it.get("rated_at"):
                    sp["rated_at"] = it["rated_at"]
            continue

        if typ == "episode":
            show_ids = _ids_from_obj(it.get("show_ids") or {})
            s = safe_int(it.get("season"))
            n = safe_int(it.get("number") if it.get("number") is not None else it.get("episode"))
            if not show_ids or s is None or n is None:
                continue
            sk, grp = ensure_show(show_ids)
            sp = seasons_index.get((sk, s))
            if not sp:
                sp2: dict[str, Any] = {}
                sp2["number"] = s
                seasons_index[(sk, s)] = sp2
                seasons = grp.get("seasons")
                if not isinstance(seasons, list):
                    seasons = []
                    grp["seasons"] = seasons
                seasons.append(sp2)
                sp = sp2
            ep: dict[str, Any] = {"number": n}
            if not unrate:
                r = _valid_rating(it.get("rating"))
                if r is None:
                    continue
                ep["rating"] = r
                if it.get("rated_at"):
                    ep["rated_at"] = it["rated_at"]
            episodes = sp.get("episodes")
            if not isinstance(episodes, list):
                episodes = []
                sp["episodes"] = episodes
            episodes.append(ep)

    for grp in shows.values():
        seasons = grp.get("seasons")
        if isinstance(seasons, list):
            grp["seasons"] = sorted(seasons, key=lambda x: safe_int((x or {}).get("number")) or 0)
            for sp in grp["seasons"]:
                episodes = (sp or {}).get("episodes")
                if isinstance(episodes, list):
                    sp["episodes"] = sorted(episodes, key=lambda x: safe_int((x or {}).get("number")) or 0)

    if shows:
        body["shows"] = list(shows.values())

    body = {k: v for k, v in body.items() if v}
    return body


def write_ratings(meta: dict[str, Any], ses: requests.Session, *, unrate: bool, items: list[dict[str, Any]]) -> int:
    body = bucketize_ratings(items, unrate=unrate)
    if not body:
        return 0

    chunk_size = int(meta["rt_chunk_size"])
    delay_ms = int(meta["rt_delay_ms"])
    max_backoff_ms = int(meta["rt_max_backoff_ms"])
    url = RT_REMOVE if unrate else RT_UPSERT

    ok = 0
    for bucket in ("movies", "shows"):
        rows = body.get(bucket) or []
        if not rows:
            continue
        for part in _chunk(list(rows), chunk_size):
            payload = {bucket: part}
            attempt = 0
            backoff_ms = delay_ms
            while True:
                st, data, text = request_json(
                    ses,
                    "POST",
                    url,
                    params={"apikey": meta["apikey"]},
                    body=payload,
                    timeout=meta["timeout"],
                    retries=1,
                )
                if st in (200, 201, 204):
                    if st == 204 or not isinstance(data, dict):
                        ok += len(part)
                        time.sleep(max(0.0, delay_ms / 1000.0))
                        break
                    if unrate:
                        removed = data.get("removed") or data.get("deleted") or {}
                        n = sum(int(removed.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
                        ok += n if n > 0 else len(part)
                    else:
                        updated = data.get("updated") or {}
                        added = data.get("added") or {}
                        existing = data.get("existing") or {}
                        n = 0
                        n += sum(int(updated.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
                        n += sum(int(added.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
                        n += sum(int(existing.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
                        ok += n if n > 0 else len(part)
                    time.sleep(max(0.0, delay_ms / 1000.0))
                    break

                if st in (429, 503):
                    if os.getenv("CW_MDBLIST_DEBUG"):
                        print(f"[MDBLIST:ratings] throttled {st} attempt={attempt} backoff_ms={backoff_ms}: {(text or '')[:160]}")
                    time.sleep(min(max_backoff_ms, backoff_ms) / 1000.0)
                    attempt += 1
                    backoff_ms = min(max_backoff_ms, int(backoff_ms * 1.6) + 200)
                    if attempt <= 4:
                        continue

                if os.getenv("CW_MDBLIST_DEBUG"):
                    print(f"[MDBLIST:ratings] failed {st}: {(text or '')[:200]}")
                break

    return ok


# Backup / Restore
def backup_now(wl: list[dict[str, Any]], ratings: list[dict[str, Any]], meta: dict[str, Any]) -> Path:
    ensure_backup_dir()
    cleanup_old_backups()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"mdblist_backup_{ts}.json.gz"
    payload = {
        "schema": 1,
        "provider": "mdblist",
        "created_at": now_iso(),
        "counts": {"watchlist": len(wl), "ratings": len(ratings)},
        "watchlist": wl,
        "ratings": ratings,
        "note": "ratings include movie/show/season/episode entries; seasons/episodes use show_ids for restore/remove",
        "meta": {"timeout": meta["timeout"], "retries": meta["retries"]},
    }
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def restore_from_backup(meta: dict[str, Any], ses: requests.Session, b: dict[str, Any]) -> None:
    wl = b.get("watchlist") or []
    ratings = b.get("ratings") or []
    print(f"\nBackup: {b.get('created_at')}  wl={len(wl)} ratings={len(ratings)}")
    c = input("Restore [w]atchlist / [r]atings / [b]oth: ").strip().lower()

    if c in ("w", "b") and wl:
        n, unr = write_watchlist(meta, ses, "add", wl)
        print(f"Watchlist restore: ok={n}, unresolved={len(unr)}")

    if c in ("r", "b") and ratings:
        n2 = write_ratings(meta, ses, unrate=False, items=list(ratings))
        print(f"Ratings restore: ok={n2}")

    print("Restore done.")


# Menu
def menu() -> str:
    print("\n=== MDBList Cleanup and Backup/Restore ===")
    print("1. Show MDBList Watchlist")
    print("2. Show MDBList Ratings")
    print("3. Remove MDBList Watchlist")
    print("4. Remove MDBList Ratings")
    print("5. Backup MDBList (watchlist + ratings)")
    print("6. Restore MDBList from Backup")
    print("7. Clean MDBList (watchlist + ratings)")
    print("0. Exit")
    return input("Select: ").strip()


def main() -> None:
    cfg = jload(CONFIG_PATH)
    meta = build_meta(cfg)
    ses = requests.Session()

    while True:
        ch = menu()
        try:
            if ch == "0":
                return

            if ch == "1":
                wl = collect_watchlist(meta, ses)
                print(f"\nMDBList watchlist items: {len(wl)}")
                show(wl, ["type", "title", "year"])

            elif ch == "2":
                rats = collect_ratings(meta, ses)
                print(f"\nMDBList ratings entries: {len(rats)}")
                show(rats, ["type", "title", "year", "rating", "rated_at"])

            elif ch == "3":
                wl = collect_watchlist(meta, ses)
                print(f"\nFound {len(wl)} watchlist items.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n, unr = write_watchlist(meta, ses, "remove", wl)
                print(f"Done. Removed {n} watchlist items. Unresolved={len(unr)}")

            elif ch == "4":
                rats = collect_ratings(meta, ses)
                print(f"\nFound {len(rats)} ratings entries.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n2 = write_ratings(meta, ses, unrate=True, items=rats)
                print(f"Done. Removed ratings count={n2} (server-reported).")

            elif ch == "5":
                wl = collect_watchlist(meta, ses)
                rats = collect_ratings(meta, ses)
                p = backup_now(wl, rats, meta)
                print(f"Backup written: {p}")

            elif ch == "6":
                p = choose_backup()
                if not p:
                    continue
                restore_from_backup(meta, ses, load_backup(p))

            elif ch == "7":
                wl = collect_watchlist(meta, ses)
                rats = collect_ratings(meta, ses)
                print(f"\nWatchlist={len(wl)} | Ratings={len(rats)}")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                print("Cleaning...")
                nw, unr = write_watchlist(meta, ses, "remove", wl)
                nr = write_ratings(meta, ses, unrate=True, items=rats)
                print(f"Done. Cleared watchlist={nw} (unresolved={len(unr)}), ratings_removed={nr}.")

            else:
                print("Unknown option.")

        except Exception as e:
            print(f"[!] Error: {e}")


if __name__ == "__main__":
    main()

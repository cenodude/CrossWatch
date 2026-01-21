#!/usr/bin/env python3
from __future__ import annotations

import gzip
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable

import requests

CONFIG_PATH = Path("/config/config.json")
BACKUP_DIR = Path("/config/backup")
RETENTION_DAYS = 15

BASE = "https://api.simkl.com"
URL_ADD_TO_LIST = f"{BASE}/sync/add-to-list"
URL_HIST_ADD = f"{BASE}/sync/history"
URL_HIST_REMOVE = f"{BASE}/sync/history/remove"
URL_RAT_UPSERT = f"{BASE}/sync/ratings"
URL_RAT_REMOVE = f"{BASE}/sync/ratings/remove"

WL_URLS: dict[str, str] = {
    "movies": f"{BASE}/sync/all-items/movies/plantowatch",
    "shows": f"{BASE}/sync/all-items/shows/plantowatch",
    "anime": f"{BASE}/sync/all-items/anime/plantowatch",
}

HIST_URLS: dict[str, str] = {
    "movies": f"{BASE}/sync/all-items/movies",
    "shows": f"{BASE}/sync/all-items/shows",
    "anime": f"{BASE}/sync/all-items/anime",
}

RAT_BUCKETS = ("movies", "shows", "anime")

ANIME_ID_KEYS = ("mal", "anidb", "anilist", "kitsu")


def _is_anime_ids(ids: dict[str, Any]) -> bool:
    if not isinstance(ids, dict):
        return False
    for k in ANIME_ID_KEYS:
        if ids.get(k):
            return True
    return False


def jload(p: Path) -> dict[str, Any]:
    try:
        v = json.loads(p.read_text("utf-8"))
        return v if isinstance(v, dict) else {}
    except Exception:
        return {}


def simkl_block(cfg: dict[str, Any]) -> dict[str, Any]:
    b = cfg.get("simkl") or {}
    return b if isinstance(b, dict) else {}


def safe_int(v: Any) -> int | None:
    try:
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_date_from(v: Any) -> str:
    s = str(v or "").strip()
    if not s:
        return "1970-01-01T00:00:00Z"
    if len(s) == 10 and s[4] == "-" and s[7] == "-":
        return f"{s}T00:00:00Z"
    if s.endswith("Z"):
        return s
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")
    except Exception:
        return "1970-01-01T00:00:00Z"


def ensure_backup_dir() -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def cleanup_old_backups() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    for p in BACKUP_DIR.glob("simkl_backup_*.json.gz"):
        try:
            if datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) < cutoff:
                p.unlink()
        except Exception:
            pass


def list_backups() -> list[Path]:
    ensure_backup_dir()
    return sorted(BACKUP_DIR.glob("simkl_backup_*.json.gz"), key=lambda p: p.stat().st_mtime, reverse=True)


def choose_backup() -> Path | None:
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
        v = json.load(f) or {}
        return v if isinstance(v, dict) else {}


def build_meta(cfg: dict[str, Any]) -> dict[str, Any]:
    b = simkl_block(cfg)
    tok = str(b.get("access_token") or "").strip()
    cid = str(b.get("client_id") or b.get("api_key") or "").strip()
    if not tok:
        raise RuntimeError("Missing simkl.access_token in /config/config.json")
    if not cid:
        raise RuntimeError("Missing simkl.client_id in /config/config.json")

    timeout = float(b.get("timeout", 10) or 10)
    retries = int(b.get("max_retries", 3) or 3)
    delay_ms = int(b.get("write_delay_ms", 350) or 350)

    df = normalize_date_from(os.getenv("CW_SIMKL_DATE_FROM") or "1970-01-01T00:00:00Z")

    return {
        "token": tok,
        "cid": cid,
        "timeout": timeout,
        "retries": retries,
        "date_from": df,
        "wl_chunk": int(b.get("watchlist_chunk_size", 75) or 75),
        "rat_chunk": int(b.get("ratings_chunk_size", 75) or 75),
        "hist_movie_chunk": int(b.get("history_movie_chunk_size", 50) or 50),
        "hist_show_chunk": int(b.get("history_show_chunk_size", 4) or 4),
        "hist_episode_limit": int(b.get("history_episode_limit", 400) or 400),
        "delay_ms": delay_ms,
    }


def headers(meta: dict[str, Any]) -> dict[str, str]:
    return {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "simkl-api-key": str(meta["cid"]),
        "Authorization": f"Bearer {meta['token']}",
        "User-Agent": "CrossWatch-Cleanup/1.0",
    }


def retry_sleep(attempt: int, retry_after: str | None) -> None:
    if retry_after:
        try:
            v = float(retry_after)
            time.sleep(max(0.0, min(30.0, v)))
            return
        except Exception:
            pass
    time.sleep(min(8.0, 0.7 * (attempt + 1)))


def request_json(
    ses: requests.Session,
    method: str,
    url: str,
    *,
    meta: dict[str, Any],
    params: dict[str, Any] | None = None,
    body: dict[str, Any] | None = None,
) -> tuple[int, Any, str]:
    last_text = ""
    retries = max(1, int(meta.get("retries") or 1))
    for attempt in range(retries):
        try:
            resp = ses.request(
                method=method.upper(),
                url=url,
                headers=headers(meta),
                params=params,
                json=body,
                timeout=float(meta.get("timeout") or 10),
            )
            last_text = resp.text or ""
            if resp.status_code in (429, 500, 502, 503, 504):
                retry_sleep(attempt, resp.headers.get("Retry-After"))
                continue
            try:
                data = resp.json()
            except Exception:
                data = None
            return resp.status_code, data, last_text
        except Exception as e:
            last_text = str(e)
            retry_sleep(attempt, None)
    return 0, None, last_text


def _norm_ids(ids: dict[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in (ids or {}).items():
        if v is None:
            continue
        if isinstance(v, str) and not v.strip():
            continue
        if isinstance(v, (int, float)) and k != "imdb":
            out[k] = int(v)
        else:
            out[k] = str(v).strip() if isinstance(v, str) else v
    return out


def _ids_from_obj(obj: Any) -> dict[str, Any]:
    if not isinstance(obj, dict):
        return {}
    ids = obj.get("ids")
    if isinstance(ids, dict):
        return _norm_ids(ids)
    fallback = {
        "simkl": obj.get("simkl") or obj.get("simkl_id"),
        "imdb": obj.get("imdb") or obj.get("imdb_id"),
        "tmdb": obj.get("tmdb") or obj.get("tmdb_id"),
        "tvdb": obj.get("tvdb") or obj.get("tvdb_id"),
        "mal": obj.get("mal"),
    }
    return _norm_ids(fallback)


def _title_year(obj: Any) -> tuple[str, int | None]:
    if not isinstance(obj, dict):
        return "", None
    title = str(obj.get("title") or obj.get("name") or obj.get("en_title") or "").strip()
    year = safe_int(obj.get("year") or obj.get("release_year") or obj.get("first_air_year"))
    return title, year


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _as_list(data: Any) -> list[dict[str, Any]]:
    if isinstance(data, list):
        return [x for x in data if isinstance(x, dict)]
    if isinstance(data, dict):
        for key in ("items", "results", "movies", "shows", "anime", "tv_shows", "tvShows", "tv"):
            items = data.get(key)
            if isinstance(items, list):
                return [x for x in items if isinstance(x, dict)]
    return []


def collect_watchlist(meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    params = {"extended": "full", "date_from": meta["date_from"]}

    for bucket, url in WL_URLS.items():
        st, data, text = request_json(ses, "GET", url, meta=meta, params=params)
        rows = _as_list(data)

        if st == 200 and not rows:
            st, data, text = request_json(ses, "GET", url, meta=meta, params={"extended": "full"})
            rows = _as_list(data)

        if st != 200:
            raise RuntimeError(f"watchlist {bucket} failed: {st}: {(text or '')[:140]}")
        if not rows and os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:watchlist] {bucket} empty (200). Body head: {(text or '')[:200]}")

        for row in rows:
            if bucket == "movies":
                key = "movie"
            elif bucket == "anime":
                key = "anime"
            else:
                key = "show"
            obj = row.get(key) if isinstance(row.get(key), dict) else row
            if bucket == "anime" and not isinstance(obj, dict):
                alt = row.get("show")
                if isinstance(alt, dict):
                    obj = alt
            ids = _ids_from_obj(obj)
            if not ids:
                continue
            title, year = _title_year(obj)
            typ = "movie" if bucket == "movies" else ("anime" if bucket == "anime" else "show")
            item: dict[str, Any] = {"type": typ, "ids": ids}
            if title:
                item["title"] = title
            if year is not None:
                item["year"] = year
            out.append(item)

    return out


def _wl_split(items: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    anime: list[dict[str, Any]] = []
    for it in items:
        typ = str(it.get("type") or "").strip().lower()
        ids = _ids_from_obj(it.get("ids") if isinstance(it.get("ids"), dict) else it)
        if not ids:
            continue
        if typ == "movie":
            movies.append({"ids": ids, "to": "plantowatch"})
        elif typ == "anime" or _is_anime_ids(ids):
            anime.append({"ids": ids, "to": "plantowatch"})
        else:
            shows.append({"ids": ids, "to": "plantowatch"})
    return movies, shows, anime


def _wl_remove_split(items: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    anime: list[dict[str, Any]] = []
    for it in items:
        typ = str(it.get("type") or "").strip().lower()
        ids = _ids_from_obj(it.get("ids") if isinstance(it.get("ids"), dict) else it)
        if not ids:
            continue
        if typ == "movie":
            movies.append({"ids": ids})
        elif typ == "anime" or _is_anime_ids(ids):
            anime.append({"ids": ids})
        else:
            shows.append({"ids": ids})
    return movies, shows, anime


def write_watchlist(meta: dict[str, Any], ses: requests.Session, *, remove: bool, items: list[dict[str, Any]]) -> int:
    if not items:
        return 0
    delay = max(0.0, float(meta.get("delay_ms") or 0) / 1000.0)
    ok = 0

    def post(bucket: str, part: list[dict[str, Any]], *, fallback: str | None = None) -> int:
        st, _, text = request_json(
            ses,
            "POST",
            (URL_HIST_REMOVE if remove else URL_ADD_TO_LIST),
            meta=meta,
            body={bucket: part},
        )
        if st in (200, 201, 204):
            return st
        if fallback:
            st2, _, text2 = request_json(
                ses,
                "POST",
                (URL_HIST_REMOVE if remove else URL_ADD_TO_LIST),
                meta=meta,
                body={fallback: part},
            )
            if st2 in (200, 201, 204):
                return st2
            if os.getenv("CW_SIMKL_DEBUG"):
                print(
                    f"[SIMKL:watchlist] {('remove' if remove else 'add')} {bucket}->{fallback} failed: {st2}: {(text2 or '')[:200]}"
                )
        elif os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:watchlist] {('remove' if remove else 'add')} {bucket} -> {st}: {(text or '')[:200]}")
        return st

    if remove:
        mv, sh, an = _wl_remove_split(items)
        for bucket, rows, fallback in (("movies", mv, None), ("shows", sh, None), ("anime", an, "shows")):
            if not rows:
                continue
            for part in _chunk(rows, int(meta["wl_chunk"])):
                st = post(bucket, part, fallback=fallback)
                if st in (200, 201, 204):
                    ok += len(part)
                time.sleep(delay)
        return ok

    mv2, sh2, an2 = _wl_split(items)
    payloads: list[tuple[str, list[dict[str, Any]], str | None]] = []
    if mv2:
        payloads.append(("movies", mv2, None))
    if sh2:
        payloads.append(("shows", sh2, None))
    if an2:
        payloads.append(("anime", an2, "shows"))

    for bucket2, rows2, fallback2 in payloads:
        for part2 in _chunk(rows2, int(meta["wl_chunk"])):
            st = post(bucket2, part2, fallback=fallback2)
            if st in (200, 201, 204):
                ok += len(part2)
            time.sleep(delay)

    return ok


def collect_ratings(meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    def fetch(bucket: str) -> list[dict[str, Any]]:
        url = f"{BASE}/sync/ratings/{bucket}"
        params = {"date_from": meta["date_from"]}

        st, data, text = request_json(ses, "POST", url, meta=meta, params=params)
        if st in (404, 405):
            st, data, text = request_json(ses, "GET", url, meta=meta, params=params)
        if st != 200:
            raise RuntimeError(f"ratings {bucket} failed: {st}: {(text or '')[:140]}")

        if isinstance(data, dict):
            rows = data.get(bucket)
            rows = rows if isinstance(rows, list) else data.get("items")
            if isinstance(rows, list):
                return [x for x in rows if isinstance(x, dict)]
            return []

        if isinstance(data, list):
            return [x for x in data if isinstance(x, dict)]

        if os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:ratings] {bucket} unexpected body. Head: {(text or '')[:200]}")
        return []

    for bucket in RAT_BUCKETS:
        rows = fetch(bucket)
        if not rows and os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:ratings] {bucket} empty (200).")

        for row in rows:
            if bucket == "movies":
                key = "movie"
            elif bucket == "anime":
                key = "anime"
            else:
                key = "show"
            obj = row.get(key) if isinstance(row.get(key), dict) else row
            if bucket == "anime" and not isinstance(obj, dict):
                alt = row.get("show")
                if isinstance(alt, dict):
                    obj = alt
            ids = _ids_from_obj(obj)
            if not ids:
                continue
            rating = safe_int(row.get("user_rating") or row.get("rating"))
            if rating is None:
                continue
            rated_at = str(row.get("user_rated_at") or row.get("rated_at") or "").strip()
            title, year = _title_year(obj)
            typ = "movie" if bucket == "movies" else ("anime" if bucket == "anime" else "show")
            item: dict[str, Any] = {"type": typ, "ids": ids, "rating": rating}
            if rated_at:
                item["rated_at"] = rated_at
            if title:
                item["title"] = title
            if year is not None:
                item["year"] = year
            out.append(item)

    return out


def _ratings_split(
    items: Iterable[dict[str, Any]],
    *,
    unrate: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    anime: list[dict[str, Any]] = []
    for it in items:
        typ = str(it.get("type") or "").strip().lower()
        ids = _ids_from_obj(it.get("ids") if isinstance(it.get("ids"), dict) else it)
        if not ids:
            continue

        def build_obj() -> dict[str, Any] | None:
            obj: dict[str, Any] = {"ids": ids}
            if unrate:
                return obj
            r = safe_int(it.get("rating"))
            if r is None:
                return None
            obj["rating"] = r
            ra = str(it.get("rated_at") or "").strip()
            if ra:
                obj["rated_at"] = ra
            return obj

        objv = build_obj()
        if not objv:
            continue
        if typ == "movie":
            movies.append(objv)
        elif typ == "anime" or _is_anime_ids(ids):
            anime.append(objv)
        else:
            shows.append(objv)

    return movies, shows, anime


def write_ratings(meta: dict[str, Any], ses: requests.Session, *, unrate: bool, items: list[dict[str, Any]]) -> int:
    if not items:
        return 0
    mv, sh, an = _ratings_split(items, unrate=unrate)
    url = URL_RAT_REMOVE if unrate else URL_RAT_UPSERT
    chunk = int(meta["rat_chunk"])
    delay = max(0.0, float(meta.get("delay_ms") or 0) / 1000.0)
    ok = 0

    def post(bucket: str, part: list[dict[str, Any]], *, fallback: str | None = None) -> int:
        st, _, text = request_json(ses, "POST", url, meta=meta, body={bucket: part})
        if st in (200, 201, 204):
            return st
        if fallback:
            st2, _, text2 = request_json(ses, "POST", url, meta=meta, body={fallback: part})
            if st2 in (200, 201, 204):
                return st2
            if os.getenv("CW_SIMKL_DEBUG"):
                print(f"[SIMKL:ratings] {('remove' if unrate else 'upsert')} {bucket}->{fallback} failed: {st2}: {(text2 or '')[:200]}")
        elif os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:ratings] {('remove' if unrate else 'upsert')} {bucket} -> {st}: {(text or '')[:200]}")
        return st

    for bucket, rows, fallback in (("movies", mv, None), ("shows", sh, None), ("anime", an, "shows")):
        if not rows:
            continue
        for part in _chunk(rows, chunk):
            st = post(bucket, part, fallback=fallback)
            if st in (200, 201, 204):
                ok += len(part)
            time.sleep(delay)

    return ok


def _show_key(ids: dict[str, Any]) -> str:
    if ids.get("simkl") is not None:
        return f"simkl:{ids['simkl']}"
    if ids.get("imdb"):
        return f"imdb:{ids['imdb']}"
    if ids.get("tmdb") is not None:
        return f"tmdb:{ids['tmdb']}"
    if ids.get("tvdb") is not None:
        return f"tvdb:{ids['tvdb']}"
    return json.dumps(ids, sort_keys=True)


def collect_history(meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    def fetch(kind: str, *, episode_watched_at: bool) -> list[dict[str, Any]]:
        ext = "full_anime_seasons" if kind == "anime" else "full"
        params: dict[str, Any] = {"extended": ext, "date_from": meta["date_from"]}
        if episode_watched_at:
            params["episode_watched_at"] = "yes"

        st, data, text = request_json(ses, "GET", HIST_URLS[kind], meta=meta, params=params)
        rows = _as_list(data)

        if st == 200 and not rows:
            p2 = dict(params)
            p2.pop("date_from", None)
            st, data, text = request_json(ses, "GET", HIST_URLS[kind], meta=meta, params=p2)
            rows = _as_list(data)

        if st != 200:
            raise RuntimeError(f"history {kind} failed: {st}: {(text or '')[:140]}")
        if not rows and os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:history] {kind} empty (200). Body head: {(text or '')[:200]}")
        return rows

    for row in fetch("movies", episode_watched_at=False):
        mv = row.get("movie") if isinstance(row.get("movie"), dict) else row
        ids = _ids_from_obj(mv)
        if not ids:
            continue
        watched_at = str(row.get("last_watched_at") or row.get("watched_at") or "").strip()
        if not watched_at:
            continue
        title, year = _title_year(mv)
        item: dict[str, Any] = {"type": "movie", "ids": ids, "watched_at": watched_at}
        if title:
            item["title"] = title
        if year is not None:
            item["year"] = year
        out.append(item)

    def add_show_rows(rows: list[dict[str, Any]], *, obj_key: str, bucket: str) -> None:
        for row in rows:
            obj = row.get(obj_key) if isinstance(row.get(obj_key), dict) else None
            if not isinstance(obj, dict) and obj_key == "anime":
                alt = row.get("show")
                if isinstance(alt, dict):
                    obj = alt
            if not isinstance(obj, dict):
                continue
            show_ids = _ids_from_obj(obj)
            if not show_ids:
                continue
            show_title, show_year = _title_year(obj)
            seasons = row.get("seasons") or []
            if not isinstance(seasons, list):
                continue
            for sv in seasons:
                if not isinstance(sv, dict):
                    continue
                sn = safe_int(sv.get("number"))
                if sn is None:
                    continue
                eps = sv.get("episodes") or []
                if not isinstance(eps, list):
                    continue
                for ev in eps:
                    if not isinstance(ev, dict):
                        continue
                    en = safe_int(ev.get("number"))
                    if en is None:
                        continue
                    wa = str(ev.get("watched_at") or ev.get("last_watched_at") or "").strip()
                    if not wa:
                        continue
                    item2: dict[str, Any] = {
                        "type": "episode",
                        "bucket": bucket,
                        "show_ids": show_ids,
                        "season": sn,
                        "number": en,
                        "watched_at": wa,
                    }
                    if show_title:
                        item2["title"] = show_title
                    if show_year is not None:
                        item2["year"] = show_year
                    out.append(item2)

    add_show_rows(fetch("shows", episode_watched_at=True), obj_key="show", bucket="shows")
    add_show_rows(fetch("anime", episode_watched_at=True), obj_key="anime", bucket="anime")

    return out


def _history_split(
    items: Iterable[dict[str, Any]],
    *,
    remove: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows_map: dict[str, dict[str, Any]] = {}
    anime_map: dict[str, dict[str, Any]] = {}
    seasons_map: dict[tuple[str, int], dict[str, Any]] = {}
    anime_seasons_map: dict[tuple[str, int], dict[str, Any]] = {}

    def ensure_group(show_ids: dict[str, Any], *, is_anime_hint: bool) -> tuple[str, dict[str, Any], dict[tuple[str, int], dict[str, Any]]]:
        is_anime = bool(is_anime_hint) or _is_anime_ids(show_ids)
        target = anime_map if is_anime else shows_map
        seasons = anime_seasons_map if is_anime else seasons_map
        sk = _show_key(show_ids)
        grp = target.get(sk)
        if not grp:
            grp = {"ids": show_ids, "seasons": []}
            if is_anime:
                grp["use_tvdb_anime_seasons"] = True
            target[sk] = grp
        return sk, grp, seasons

    for it in items:
        typ = str(it.get("type") or "").strip().lower()
        if typ == "movie":
            ids = _ids_from_obj(it.get("ids") if isinstance(it.get("ids"), dict) else it)
            if not ids:
                continue
            obj: dict[str, Any] = {"ids": ids}
            if not remove:
                wa = str(it.get("watched_at") or "").strip()
                if not wa:
                    continue
                obj["watched_at"] = wa
            movies.append(obj)
            continue

        if typ == "episode":
            show_ids = _ids_from_obj(it.get("show_ids") if isinstance(it.get("show_ids"), dict) else {})
            sn = safe_int(it.get("season"))
            en = safe_int(it.get("number") if it.get("number") is not None else it.get("episode"))
            if not show_ids or sn is None or en is None:
                continue
            sk, grp, seasons = ensure_group(show_ids, is_anime_hint=(str(it.get("bucket") or "").lower() == "anime"))
            sp = seasons.get((sk, sn))
            if not sp:
                sp = {"number": sn, "episodes": []}
                seasons[(sk, sn)] = sp
                grp["seasons"].append(sp)
            ep: dict[str, Any] = {"number": en}
            if not remove:
                wa2 = str(it.get("watched_at") or "").strip()
                if not wa2:
                    continue
                ep["watched_at"] = wa2
            sp["episodes"].append(ep)

    def finalize(groups: list[dict[str, Any]]) -> None:
        for grp in groups:
            grp["seasons"] = sorted(grp.get("seasons") or [], key=lambda x: int(x.get("number") or 0))
            for sp in grp["seasons"]:
                sp["episodes"] = sorted(sp.get("episodes") or [], key=lambda x: int(x.get("number") or 0))

    shows = list(shows_map.values())
    anime = list(anime_map.values())
    finalize(shows)
    finalize(anime)
    return movies, shows, anime


def _episode_count(show_obj: dict[str, Any]) -> int:
    n = 0
    for sp in show_obj.get("seasons") or []:
        eps = sp.get("episodes") or []
        if isinstance(eps, list):
            n += len(eps)
    return n


def _chunk_shows(shows: list[dict[str, Any]], *, max_shows: int, max_eps: int) -> Iterable[list[dict[str, Any]]]:
    max_shows = max(1, int(max_shows))
    max_eps = max(1, int(max_eps))

    cur: list[dict[str, Any]] = []
    eps = 0
    for sh in shows:
        cnt = _episode_count(sh)
        if cur and (len(cur) >= max_shows or (eps + cnt) > max_eps):
            yield cur
            cur = []
            eps = 0
        cur.append(sh)
        eps += cnt
    if cur:
        yield cur


def write_history(meta: dict[str, Any], ses: requests.Session, *, remove: bool, items: list[dict[str, Any]]) -> int:
    if not items:
        return 0
    mv, sh, an = _history_split(items, remove=remove)
    url = URL_HIST_REMOVE if remove else URL_HIST_ADD
    delay = max(0.0, float(meta.get("delay_ms") or 0) / 1000.0)
    ok = 0

    def post(bucket: str, part: list[dict[str, Any]], *, fallback: str | None = None) -> int:
        st, _, text = request_json(ses, "POST", url, meta=meta, body={bucket: part})
        if st in (200, 201, 204):
            return st
        if fallback:
            st2, _, text2 = request_json(ses, "POST", url, meta=meta, body={fallback: part})
            if st2 in (200, 201, 204):
                return st2
            if os.getenv("CW_SIMKL_DEBUG"):
                print(f"[SIMKL:history] {('remove' if remove else 'add')} {bucket}->{fallback} failed: {st2}: {(text2 or '')[:200]}")
        elif os.getenv("CW_SIMKL_DEBUG"):
            print(f"[SIMKL:history] {('remove' if remove else 'add')} {bucket} -> {st}: {(text or '')[:200]}")
        return st

    for part in _chunk(mv, int(meta["hist_movie_chunk"])):
        st = post("movies", part)
        if st in (200, 201, 204):
            ok += len(part)
        time.sleep(delay)

    for bucket, groups, fallback in (("shows", sh, None), ("anime", an, "shows")):
        for part2 in _chunk_shows(groups, max_shows=int(meta["hist_show_chunk"]), max_eps=int(meta["hist_episode_limit"])):
            st2 = post(bucket, part2, fallback=fallback)
            if st2 in (200, 201, 204):
                ok += sum(_episode_count(x) for x in part2)
            time.sleep(delay)

    return ok


def backup_now(wl: list[dict[str, Any]], ratings: list[dict[str, Any]], history: list[dict[str, Any]], meta: dict[str, Any]) -> Path:
    ensure_backup_dir()
    cleanup_old_backups()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"simkl_backup_{ts}.json.gz"
    payload: dict[str, Any] = {
        "schema": 1,
        "provider": "simkl",
        "created_at": now_iso(),
        "counts": {"watchlist": len(wl), "ratings": len(ratings), "history": len(history)},
        "watchlist": wl,
        "ratings": ratings,
        "history": history,
        "meta": {"date_from": meta.get("date_from")},
    }
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def restore_from_backup(meta: dict[str, Any], ses: requests.Session, b: dict[str, Any]) -> None:
    wl = b.get("watchlist") or []
    ratings = b.get("ratings") or []
    history = b.get("history") or []
    wl_n = len(wl) if isinstance(wl, list) else 0
    rt_n = len(ratings) if isinstance(ratings, list) else 0
    hs_n = len(history) if isinstance(history, list) else 0
    print(f"\nBackup: {b.get('created_at')}  wl={wl_n} ratings={rt_n} history={hs_n}")
    c = input("Restore [w]atchlist / [r]atings / [h]istory / [b]oth(all): ").strip().lower()

    if c in ("w", "b") and isinstance(wl, list) and wl:
        n = write_watchlist(meta, ses, remove=False, items=wl)
        print(f"Watchlist restore: ok={n}")

    if c in ("r", "b") and isinstance(ratings, list) and ratings:
        n2 = write_ratings(meta, ses, unrate=False, items=ratings)
        print(f"Ratings restore: ok={n2}")

    if c in ("h", "b") and isinstance(history, list) and history:
        n3 = write_history(meta, ses, remove=False, items=history)
        print(f"History restore: ok={n3}")

    print("Restore done.")


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


def menu() -> str:
    print("\n=== SIMKL Cleanup and Backup/Restore ===")
    print("1. Show SIMKL Watchlist")
    print("2. Show SIMKL Ratings")
    print("3. Show SIMKL History")
    print("4. Remove SIMKL Watchlist")
    print("5. Remove SIMKL Ratings")
    print("6. Remove SIMKL History")
    print("7. Backup SIMKL (watchlist + ratings + history)")
    print("8. Restore SIMKL from Backup")
    print("9. Clean SIMKL (watchlist + ratings + history)")
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
                print(f"\nSIMKL watchlist items: {len(wl)}")
                show(wl, ["type", "title", "year"])

            elif ch == "2":
                rats = collect_ratings(meta, ses)
                print(f"\nSIMKL ratings entries: {len(rats)}")
                show(rats, ["type", "title", "year", "rating", "rated_at"])

            elif ch == "3":
                hist = collect_history(meta, ses)
                print(f"\nSIMKL history entries: {len(hist)}")
                show(hist, ["bucket", "type", "title", "year", "season", "number", "watched_at"])

            elif ch == "4":
                wl = collect_watchlist(meta, ses)
                print(f"\nFound {len(wl)} watchlist items.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n = write_watchlist(meta, ses, remove=True, items=wl)
                print(f"Done. Removed {n} watchlist items (best-effort count).")

            elif ch == "5":
                rats = collect_ratings(meta, ses)
                print(f"\nFound {len(rats)} ratings entries.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n2 = write_ratings(meta, ses, unrate=True, items=rats)
                print(f"Done. Removed ratings count={n2} (best-effort count).")

            elif ch == "6":
                hist = collect_history(meta, ses)
                print(f"\nFound {len(hist)} history entries.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n3 = write_history(meta, ses, remove=True, items=hist)
                print(f"Done. Removed history count={n3} (best-effort count).")

            elif ch == "7":
                wl = collect_watchlist(meta, ses)
                rats = collect_ratings(meta, ses)
                hist = collect_history(meta, ses)
                p = backup_now(wl, rats, hist, meta)
                print(f"Backup written: {p}")

            elif ch == "8":
                p = choose_backup()
                if not p:
                    continue
                restore_from_backup(meta, ses, load_backup(p))

            elif ch == "9":
                wl = collect_watchlist(meta, ses)
                rats = collect_ratings(meta, ses)
                hist = collect_history(meta, ses)
                print(f"\nWatchlist={len(wl)} | Ratings={len(rats)} | History={len(hist)}")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                print("Cleaning...")
                nw = write_watchlist(meta, ses, remove=True, items=wl)
                nr = write_ratings(meta, ses, unrate=True, items=rats)
                nh = write_history(meta, ses, remove=True, items=hist)
                print(f"Done. Cleared watchlist={nw}, ratings={nr}, history={nh}.")

            else:
                print("Unknown option.")

        except Exception as e:
            print(f"[!] Error: {e}")


if __name__ == "__main__":
    main()

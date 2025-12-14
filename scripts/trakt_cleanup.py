#!/usr/bin/env python3
# Trakt cleanup + backup/restore (watchlist + history + ratings).
from __future__ import annotations

import gzip
import json
import os
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Optional

import requests

CONFIG_PATH = Path("/config/config.json")
BACKUP_DIR = Path("/config/backup")
RETENTION_DAYS = 15

BASE = "https://api.trakt.tv"
OAUTH_TOKEN = f"{BASE}/oauth/token"

URL_WL_LIST = f"{BASE}/sync/watchlist"
URL_WL_ADD = f"{BASE}/sync/watchlist"
URL_WL_REMOVE = f"{BASE}/sync/watchlist/remove"

URL_HIST_MOVIES = f"{BASE}/sync/history/movies"
URL_HIST_EPISODES = f"{BASE}/sync/history/episodes"
URL_HIST_ADD = f"{BASE}/sync/history"
URL_HIST_REMOVE = f"{BASE}/sync/history/remove"

URL_RT_MOVIES = f"{BASE}/sync/ratings/movies"
URL_RT_SHOWS = f"{BASE}/sync/ratings/shows"
URL_RT_SEASONS = f"{BASE}/sync/ratings/seasons"
URL_RT_EPISODES = f"{BASE}/sync/ratings/episodes"
URL_RT_UPSERT = f"{BASE}/sync/ratings"
URL_RT_REMOVE = f"{BASE}/sync/ratings/remove"

UA = os.environ.get("CW_UA", "CrossWatch/3.0 (Trakt Cleanup)")


# ---------- utils ----------

def jload(p: Path) -> dict[str, Any]:
    try:
        return json.loads(p.read_text("utf-8"))
    except Exception:
        return {}


def jsave(p: Path, data: dict[str, Any]) -> None:
    p.parent.mkdir(parents=True, exist_ok=True)
    tmp = p.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), "utf-8")
    os.replace(tmp, p)


def cfg_block(cfg: dict[str, Any]) -> dict[str, Any]:
    b = cfg.get("trakt") or {}
    return b if isinstance(b, dict) else {}


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def retry_sleep(attempt: int, retry_after_s: Optional[int] = None) -> None:
    if retry_after_s is not None and retry_after_s > 0:
        time.sleep(min(30.0, float(retry_after_s)))
        return
    time.sleep(min(6.0, 0.6 * (attempt + 1)))


def ensure_backup_dir() -> None:
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)


def cleanup_old_backups() -> None:
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    for p in BACKUP_DIR.glob("trakt_backup_*.json.gz"):
        try:
            if datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) < cutoff:
                p.unlink()
        except Exception:
            pass


def list_backups() -> list[Path]:
    ensure_backup_dir()
    return sorted(BACKUP_DIR.glob("trakt_backup_*.json.gz"), key=lambda p: p.stat().st_mtime, reverse=True)


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
        head = " | ".join(c.ljust(14) for c in cols)
        print(head)
        print("-" * len(head))
        for r in chunk:
            print(" | ".join(str(r.get(c, ""))[:70].ljust(14) for c in cols))
        if i + page < len(rows):
            if input("[Enter]=Next, q=Quit: ").strip().lower() == "q":
                break


# ---------- Trakt client ----------

def build_meta(cfg: dict[str, Any]) -> dict[str, Any]:
    b = cfg_block(cfg)
    cid = str(b.get("client_id") or "").strip()
    csec = str(b.get("client_secret") or "").strip()
    tok = str(b.get("access_token") or "").strip()
    rtk = str(b.get("refresh_token") or "").strip()
    if not cid:
        raise RuntimeError("Missing trakt.client_id in /config/config.json.")
    if not tok and not (csec and rtk):
        raise RuntimeError("Missing trakt.access_token (or refresh_token+client_secret).")
    return {
        "client_id": cid,
        "client_secret": csec,
        "access_token": tok,
        "refresh_token": rtk,
        "timeout": float(b.get("timeout", 10) or 10),
        "retries": int(b.get("max_retries", 3) or 3),
        "per_page": int(b.get("per_page", 100) or 100),
        "max_pages": int(b.get("max_pages", 2000) or 2000),
        "chunk": int(b.get("chunk_size", 100) or 100),
    }


def headers(meta: dict[str, Any]) -> dict[str, str]:
    h = {
        "Accept": "application/json",
        "Content-Type": "application/json",
        "trakt-api-version": "2",
        "trakt-api-key": meta["client_id"],
        "User-Agent": UA,
    }
    tok = str(meta.get("access_token") or "").strip()
    if tok:
        h["Authorization"] = f"Bearer {tok}"
    return h


def _parse_retry_after(r: requests.Response) -> Optional[int]:
    try:
        v = r.headers.get("Retry-After")
        if v is None:
            return None
        return int(str(v).strip())
    except Exception:
        return None


def refresh_token(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session) -> bool:
    cid = str(meta.get("client_id") or "").strip()
    csec = str(meta.get("client_secret") or "").strip()
    rtk = str(meta.get("refresh_token") or "").strip()
    if not (cid and csec and rtk):
        return False

    body = {
        "grant_type": "refresh_token",
        "client_id": cid,
        "client_secret": csec,
        "refresh_token": rtk,
    }

    r = ses.post(OAUTH_TOKEN, json=body, timeout=meta["timeout"])
    if r.status_code != 200:
        return False
    d = r.json() if (r.text or "").strip() else {}
    at = str(d.get("access_token") or "").strip()
    rt2 = str(d.get("refresh_token") or "").strip()
    if not at:
        return False

    cfg.setdefault("trakt", {})
    cfg["trakt"]["access_token"] = at
    if rt2:
        cfg["trakt"]["refresh_token"] = rt2
    jsave(CONFIG_PATH, cfg)

    meta["access_token"] = at
    if rt2:
        meta["refresh_token"] = rt2
    return True


def request_json(
    cfg: dict[str, Any],
    meta: dict[str, Any],
    ses: requests.Session,
    method: str,
    url: str,
    *,
    params: Optional[dict[str, Any]] = None,
    body: Optional[dict[str, Any]] = None,
    allow_401_refresh: bool = True,
) -> tuple[int, Any, str, dict[str, Any]]:
    last_text = ""
    h = headers(meta)

    for i in range(max(1, int(meta["retries"]))):
        r = ses.request(method, url, headers=h, params=params, json=body, timeout=meta["timeout"])
        last_text = r.text or ""

        if r.status_code == 401 and allow_401_refresh:
            if refresh_token(cfg, meta, ses):
                h = headers(meta)
                return request_json(cfg, meta, ses, method, url, params=params, body=body, allow_401_refresh=False)

        if r.status_code in (429, 503) and i < meta["retries"] - 1:
            retry_sleep(i, _parse_retry_after(r))
            continue
        if 500 <= r.status_code <= 599 and i < meta["retries"] - 1:
            retry_sleep(i)
            continue

        if not (r.text or "").strip():
            return r.status_code, {}, last_text, dict(r.headers)
        try:
            return r.status_code, r.json(), last_text, dict(r.headers)
        except Exception:
            return r.status_code, {}, last_text, dict(r.headers)

    return 0, {}, last_text, {}


# ---------- payload helpers ----------

def _sanitize_ids(ids: dict[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k in ("trakt", "slug", "imdb", "tmdb", "tvdb"):
        v = ids.get(k)
        if v is None:
            continue
        s = str(v).strip()
        if not s:
            continue
        if k == "imdb" and not s.startswith("tt"):
            digits = "".join(ch for ch in s if ch.isdigit())
            if digits:
                s = f"tt{digits}"
        out[k] = s
    return out


def _pick_kind(t: str) -> str:
    tt = (t or "").strip().lower()
    if tt == "episode":
        return "episodes"
    if tt == "season":
        return "seasons"
    if tt in ("show", "series", "tv"):
        return "shows"
    return "movies"


def _chunk(xs: list[dict[str, Any]], n: int) -> list[list[dict[str, Any]]]:
    n2 = max(1, int(n))
    return [xs[i : i + n2] for i in range(0, len(xs), n2)]


# ---------- watchlist ----------

def collect_watchlist(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    sc, data, txt, _ = request_json(cfg, meta, ses, "GET", URL_WL_LIST)
    if sc != 200:
        raise RuntimeError(f"GET watchlist -> {sc}: {txt[:200]}")
    rows = data if isinstance(data, list) else []
    out: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        t = str(row.get("type") or "").lower()
        payload = row.get("movie") if t == "movie" else row.get("show")
        if not isinstance(payload, dict):
            continue
        ids = _sanitize_ids(dict(payload.get("ids") or {}))
        if not ids:
            continue
        m: dict[str, Any] = {
            "type": "movie" if t == "movie" else "show",
            "title": payload.get("title"),
            "year": payload.get("year"),
            "ids": ids,
        }
        out.append(m)
    return out


def _watchlist_split(items: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    for it in items:
        ids = _sanitize_ids(dict(it.get('ids') or {}))
        if not ids:
            continue
        if str(it.get('type') or '').lower() == 'show':
            shows.append({'ids': ids})
        else:
            movies.append({'ids': ids})
    return movies, shows


def clear_watchlist(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, items: list[dict[str, Any]]) -> int:
    movies, shows = _watchlist_split(items)
    total = 0
    for bucket, rows in (("movies", movies), ("shows", shows)):
        if not rows:
            continue
        for part in _chunk(rows, meta['chunk']):
            sc, data, txt, _ = request_json(cfg, meta, ses, 'POST', URL_WL_REMOVE, body={bucket: part})
            if sc not in (200, 201):
                raise RuntimeError(f"POST watchlist/remove ({bucket}) -> {sc}: {txt[:200]}")
            deleted = (data or {}).get('deleted') or (data or {}).get('removed') or {}
            total += int(deleted.get(bucket) or 0)
    return total


def restore_watchlist(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, items: list[dict[str, Any]]) -> tuple[int, int]:
    movies, shows = _watchlist_split(items)
    ok_total = 0
    attempted = 0
    for bucket, rows in (("movies", movies), ("shows", shows)):
        if not rows:
            continue
        for part in _chunk(rows, meta['chunk']):
            attempted += len(part)
            sc, data, txt, _ = request_json(cfg, meta, ses, 'POST', URL_WL_ADD, body={bucket: part})
            if sc == 420:
                print('[!] Trakt limit hit (420) while restoring watchlist.')
                return ok_total, attempted - ok_total
            if sc not in (200, 201):
                raise RuntimeError(f"POST watchlist ({bucket}) -> {sc}: {txt[:200]}")
            added = (data or {}).get('added') or {}
            existing = (data or {}).get('existing') or {}
            ok_total += int(added.get(bucket) or 0) + int(existing.get(bucket) or 0)
    return ok_total, attempted - ok_total


# ---------- ratings ----------

def _fetch_paged(
    cfg: dict[str, Any],
    meta: dict[str, Any],
    ses: requests.Session,
    url: str,
    *,
    per_page: int,
    max_pages: int,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    page = 1
    total_pages: Optional[int] = None
    while True:
        sc, data, txt, hdr = request_json(
            cfg,
            meta,
            ses,
            "GET",
            url,
            params={"page": page, "limit": int(per_page)},
        )
        if sc != 200:
            raise RuntimeError(f"GET {url} p{page} -> {sc}: {txt[:200]}")
        if total_pages is None:
            try:
                total_pages = int(str(hdr.get("X-Pagination-Page-Count") or "").strip() or 0) or None
            except Exception:
                total_pages = None
        rows = data if isinstance(data, list) else []
        if not rows:
            break
        out.extend([r for r in rows if isinstance(r, dict)])
        page += 1
        if total_pages is not None and page > total_pages:
            break
        if total_pages is None and len(rows) < int(per_page):
            break
        if max_pages and page > max_pages:
            print(f"[!] stopping early at safety cap: max_pages={max_pages}")
            break
    return out


def collect_ratings(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    per_page = meta["per_page"]
    max_pages = meta["max_pages"]

    def pull(url: str) -> list[dict[str, Any]]:
        return _fetch_paged(cfg, meta, ses, url, per_page=per_page, max_pages=max_pages)

    out: list[dict[str, Any]] = []

    for kind, url in (
        ("movie", URL_RT_MOVIES),
        ("show", URL_RT_SHOWS),
        ("season", URL_RT_SEASONS),
        ("episode", URL_RT_EPISODES),
    ):
        rows = pull(url)
        for row in rows:
            rating = row.get("rating")
            rated_at = row.get("rated_at")
            payload = row.get(kind)
            if not isinstance(payload, dict):
                continue
            ids = _sanitize_ids(dict(payload.get("ids") or {}))
            if not ids:
                continue
            m: dict[str, Any] = {
                "type": kind,
                "ids": ids,
                "rating": int(rating) if isinstance(rating, int) else rating,
                "rated_at": rated_at,
                "title": payload.get("title"),
            }
            if kind == "movie":
                m["year"] = payload.get("year")
            if kind == "episode":
                m["season"] = payload.get("season")
                m["episode"] = payload.get("number")
                show = row.get("show") or {}
                if isinstance(show, dict):
                    m["series_title"] = show.get("title")
                    m["show_ids"] = _sanitize_ids(dict(show.get("ids") or {}))
            if kind == "season":
                m["season"] = payload.get("number")
                show = row.get("show") or {}
                if isinstance(show, dict):
                    m["series_title"] = show.get("title")
                    m["show_ids"] = _sanitize_ids(dict(show.get("ids") or {}))
            out.append(m)

    return out


def _ratings_buckets(items: list[dict[str, Any]], *, include_values: bool) -> dict[str, list[dict[str, Any]]]:
    buckets: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": [], "seasons": [], "episodes": []}
    for it in items:
        kind = _pick_kind(str(it.get("type") or "movie"))
        ids = _sanitize_ids(dict(it.get("ids") or {}))
        if not ids:
            continue
        obj: dict[str, Any] = {"ids": ids}
        if include_values:
            r = it.get("rating")
            if r is None:
                continue
            try:
                r2 = int(r)
            except Exception:
                continue
            if not (1 <= r2 <= 10):
                continue
            obj["rating"] = r2
            if it.get("rated_at"):
                obj["rated_at"] = it.get("rated_at")
        buckets.setdefault(kind, []).append(obj)
    return buckets


def clear_ratings(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, items: list[dict[str, Any]]) -> int:
    buckets = _ratings_buckets(items, include_values=False)
    total = 0
    for bucket, rows in buckets.items():
        if not rows:
            continue
        for part in _chunk(rows, meta["chunk"]):
            sc, data, txt, _ = request_json(cfg, meta, ses, "POST", URL_RT_REMOVE, body={bucket: part})
            if sc not in (200, 201):
                raise RuntimeError(f"POST ratings/remove ({bucket}) -> {sc}: {txt[:200]}")
            deleted = (data or {}).get("deleted") or (data or {}).get("removed") or {}
            total += sum(int(deleted.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
    return total


def restore_ratings(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, items: list[dict[str, Any]]) -> tuple[int, int]:
    buckets = _ratings_buckets(items, include_values=True)
    ok_total = 0
    attempted = 0
    for bucket, rows in buckets.items():
        if not rows:
            continue
        for part in _chunk(rows, meta["chunk"]):
            attempted += len(part)
            sc, data, txt, _ = request_json(cfg, meta, ses, "POST", URL_RT_UPSERT, body={bucket: part})
            if sc == 420:
                print("[!] Trakt limit hit (420) while restoring ratings.")
                return ok_total, attempted - ok_total
            if sc not in (200, 201):
                raise RuntimeError(f"POST ratings ({bucket}) -> {sc}: {txt[:200]}")
            added = (data or {}).get("added") or {}
            updated = (data or {}).get("updated") or {}
            ok_total += sum(int(added.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
            ok_total += sum(int(updated.get(k) or 0) for k in ("movies", "shows", "seasons", "episodes"))
    return ok_total, attempted - ok_total


# ---------- history ----------

def collect_history(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session) -> list[dict[str, Any]]:
    per_page = meta["per_page"]
    max_pages = meta["max_pages"]

    movies = _fetch_paged(cfg, meta, ses, URL_HIST_MOVIES, per_page=per_page, max_pages=max_pages)
    episodes = _fetch_paged(cfg, meta, ses, URL_HIST_EPISODES, per_page=per_page, max_pages=max_pages)

    out: list[dict[str, Any]] = []

    for row in movies:
        when = row.get("watched_at")
        mv = row.get("movie")
        if not when or not isinstance(mv, dict):
            continue
        ids = _sanitize_ids(dict(mv.get("ids") or {}))
        if not ids:
            continue
        out.append(
            {
                "type": "movie",
                "title": mv.get("title"),
                "year": mv.get("year"),
                "ids": ids,
                "watched_at": when,
            }
        )

    for row in episodes:
        when = row.get("watched_at")
        ep = row.get("episode")
        sh = row.get("show") or {}
        if not when or not isinstance(ep, dict):
            continue
        ids = _sanitize_ids(dict(ep.get("ids") or {}))
        if not ids:
            continue
        show_ids = _sanitize_ids(dict(sh.get("ids") or {})) if isinstance(sh, dict) else {}
        out.append(
            {
                "type": "episode",
                "title": ep.get("title"),
                "series_title": sh.get("title") if isinstance(sh, dict) else None,
                "season": ep.get("season"),
                "episode": ep.get("number"),
                "ids": ids,
                "show_ids": show_ids,
                "watched_at": when,
            }
        )

    return out


def _history_body(items: list[dict[str, Any]]) -> dict[str, Any]:
    movies: list[dict[str, Any]] = []
    episodes: list[dict[str, Any]] = []
    for it in items:
        when = it.get("watched_at")
        if not when:
            continue
        ids = _sanitize_ids(dict(it.get("ids") or {}))
        if not ids:
            continue
        if str(it.get("type") or "").lower() == "episode":
            episodes.append({"ids": ids, "watched_at": when})
        else:
            movies.append({"ids": ids, "watched_at": when})
    body: dict[str, Any] = {}
    if movies:
        body["movies"] = movies
    if episodes:
        body["episodes"] = episodes
    return body


def _post_history(
    cfg: dict[str, Any],
    meta: dict[str, Any],
    ses: requests.Session,
    url: str,
    items: list[dict[str, Any]],
) -> tuple[int, int]:
    body = _history_body(items)
    if not body:
        return 0, 0

    ok_total = 0
    attempted = 0

    for bucket in ("movies", "episodes"):
        rows = body.get(bucket) or []
        if not isinstance(rows, list) or not rows:
            continue
        for part in _chunk(rows, meta["chunk"]):
            attempted += len(part)
            sc, data, txt, _ = request_json(cfg, meta, ses, "POST", url, body={bucket: part})
            if sc == 420:
                print("[!] Trakt limit hit (420) during history write.")
                return ok_total, attempted - ok_total
            if sc not in (200, 201):
                raise RuntimeError(f"POST history ({bucket}) -> {sc}: {txt[:200]}")
            added = (data or {}).get("added") or {}
            existing = (data or {}).get("existing") or {}
            deleted = (data or {}).get("deleted") or (data or {}).get("removed") or {}
            if url.endswith("/remove"):
                ok_total += int(deleted.get("movies") or 0) + int(deleted.get("episodes") or 0)
            else:
                ok_total += int(added.get("movies") or 0) + int(added.get("episodes") or 0)
                ok_total += int(existing.get("movies") or 0) + int(existing.get("episodes") or 0)

    return ok_total, attempted - ok_total


def clear_history(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, items: list[dict[str, Any]]) -> int:
    ok, _ = _post_history(cfg, meta, ses, URL_HIST_REMOVE, items)
    return ok


def restore_history(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, items: list[dict[str, Any]]) -> tuple[int, int]:
    return _post_history(cfg, meta, ses, URL_HIST_ADD, items)


# ---------- backup/restore ----------

def backup_now(hist: list[dict[str, Any]], rats: list[dict[str, Any]], wl: list[dict[str, Any]], meta: dict[str, Any]) -> Path:
    ensure_backup_dir()
    cleanup_old_backups()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"trakt_backup_{ts}.json.gz"
    payload = {
        "schema": 2,
        "provider": "trakt",
        "created_at": now_iso(),
        "counts": {"watchlist": len(wl), "history": len(hist), "ratings": len(rats)},
        "watchlist": wl,
        "history": hist,
        "ratings": rats,
    }
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path


def restore_from_backup(cfg: dict[str, Any], meta: dict[str, Any], ses: requests.Session, b: dict[str, Any]) -> None:
    wl = b.get("watchlist") or []
    hist = b.get("history") or []
    rats = b.get("ratings") or []

    print(f"\nBackup: {b.get('created_at')}  wl={len(wl)} hist={len(hist)} rats={len(rats)}")
    c = input("Restore [w]atchlist / [h]istory / [r]atings / [b]oth(all): ").strip().lower()

    if c in ("w", "b") and wl:
        ok, fail = restore_watchlist(cfg, meta, ses, wl)
        print(f"Watchlist restore: ok={ok}, fail={fail}")

    if c in ("h", "b") and hist:
        ok, fail = restore_history(cfg, meta, ses, hist)
        print(f"History restore: ok={ok}, fail={fail}")

    if c in ("r", "b") and rats:
        ok, fail = restore_ratings(cfg, meta, ses, rats)
        print(f"Ratings restore: ok={ok}, fail={fail}")

    print("Restore done.")


# ---------- menu ----------

def menu() -> str:
    print("\n=== Trakt Cleanup and Backup/Restore ===")
    print("1. Show Trakt Watchlist")
    print("2. Show Trakt History")
    print("3. Show Trakt Ratings")
    print("4. Remove Trakt Watchlist")
    print("5. Remove Trakt History")
    print("6. Remove Trakt Ratings")
    print("7. Backup Trakt (w+h+r)")
    print("8. Restore Trakt from Backup")
    print("9. Clean Trakt (w+h+r)")
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
                wl = collect_watchlist(cfg, meta, ses)
                print(f"\nTrakt watchlist items: {len(wl)}")
                show(wl, ["type", "title", "year", "ids"])

            elif ch == "2":
                hist = collect_history(cfg, meta, ses)
                print(f"\nTrakt history entries: {len(hist)}")
                show(hist, ["type", "series_title", "title", "season", "episode", "watched_at"])

            elif ch == "3":
                rats = collect_ratings(cfg, meta, ses)
                print(f"\nTrakt rated entries: {len(rats)}")
                show(rats, ["type", "series_title", "title", "rating", "rated_at"])

            elif ch == "4":
                wl = collect_watchlist(cfg, meta, ses)
                print(f"\nFound {len(wl)} watchlist items.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n = clear_watchlist(cfg, meta, ses, wl)
                print(f"Done. Cleared {n} watchlist items.")

            elif ch == "5":
                hist = collect_history(cfg, meta, ses)
                print(f"\nFound {len(hist)} history entries.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n = clear_history(cfg, meta, ses, hist)
                print(f"Done. Removed {n} history entries.")

            elif ch == "6":
                rats = collect_ratings(cfg, meta, ses)
                print(f"\nFound {len(rats)} rated entries.")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                n = clear_ratings(cfg, meta, ses, rats)
                print(f"Done. Cleared ratings on {n} entries.")

            elif ch == "7":
                wl = collect_watchlist(cfg, meta, ses)
                hist = collect_history(cfg, meta, ses)
                rats = collect_ratings(cfg, meta, ses)
                p = backup_now(hist, rats, wl, meta)
                print(f"Backup written: {p}")

            elif ch == "8":
                p = choose_backup()
                if not p:
                    continue
                restore_from_backup(cfg, meta, ses, load_backup(p))

            elif ch == "9":
                wl = collect_watchlist(cfg, meta, ses)
                hist = collect_history(cfg, meta, ses)
                rats = collect_ratings(cfg, meta, ses)
                print(f"\nWatchlist={len(wl)} | History={len(hist)} | Ratings={len(rats)}")
                if input("Type YES to continue: ").strip().upper() != "YES":
                    continue
                print("Cleaning...")
                nw = clear_watchlist(cfg, meta, ses, wl)
                nh = clear_history(cfg, meta, ses, hist)
                nr = clear_ratings(cfg, meta, ses, rats)
                print(f"Done. Cleared watchlist={nw}, history={nh}, ratings={nr}.")

            else:
                print("Unknown option.")

        except Exception as e:
            print(f"[!] Error: {e}")


if __name__ == "__main__":
    main()

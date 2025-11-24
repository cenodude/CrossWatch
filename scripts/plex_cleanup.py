#!/usr/bin/env python3
# Plex cleanup + backup/restore.
from __future__ import annotations
import json, gzip, time, re, os, uuid, xml.etree.ElementTree as ET
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, List, Optional, Iterable, Tuple, Dict

import requests
from plexapi.server import PlexServer

CONFIG_PATH = Path("/config/config.json")
BACKUP_DIR  = Path("/config/backup")
RETENTION_DAYS = 15

PLEX_TV  = "https://plex.tv"
DISCOVER = "https://discover.provider.plex.tv"
METADATA = "https://metadata.provider.plex.tv"
WL_PATH  = "/library/sections/watchlist/all"
ACT_ADD  = "/actions/addToWatchlist"
ACT_REM  = "/actions/removeFromWatchlist"
MATCHES  = "/library/metadata/matches"

CID = os.environ.get("CW_PLEX_CID") or os.environ.get("PLEX_CLIENT_IDENTIFIER") or str(uuid.uuid4())

# ---------- utils ----------
def jload(p: Path) -> dict:
    try: return json.loads(p.read_text("utf-8"))
    except Exception: return {}

def plex_block(cfg: dict) -> dict:
    b = cfg.get("plex") or {}
    return b if isinstance(b, dict) else {}

def safe_int(v: Any) -> Optional[int]:
    try:
        s = str(v).strip()
        return int(s) if s else None
    except Exception:
        return None

def as_epoch(v: Any) -> Optional[int]:
    if v is None: return None
    if isinstance(v, (int, float)): return int(v)
    if isinstance(v, datetime):
        if v.tzinfo is None: v = v.replace(tzinfo=timezone.utc)
        return int(v.timestamp())
    s = str(v).strip()
    if s.isdigit():
        n = int(s); return n // 1000 if len(s) >= 13 else n
    try:
        return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
    except Exception:
        return None

def iso(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).isoformat().replace("+00:00", "Z")

def retry(n: int, fn, *a, **kw):
    for i in range(n):
        try: return fn(*a, **kw)
        except Exception:
            if i == n - 1: raise
            time.sleep(0.5 * (i + 1))


# ---------- Plex client ----------
def build_client(cfg: dict) -> Tuple[PlexServer, dict]:
    pb = plex_block(cfg)
    base = (pb.get("server_url") or pb.get("baseurl") or "").strip().rstrip("/")
    tok  = (pb.get("account_token") or pb.get("token") or "").strip()
    if not (base and tok):
        raise RuntimeError("Missing plex config (server_url/account_token).")

    srv = PlexServer(base, tok, timeout=float(pb.get("timeout", 10) or 10))
    try: srv._session.verify = bool(pb.get("verify_ssl", True))
    except Exception: pass

    mid = pb.get("machine_identifier") or getattr(srv, "machineIdentifier", None) or getattr(srv, "machine_identifier", None)

    meta = {
        "base": base,
        "tok": tok,
        "timeout": float(pb.get("timeout", 10) or 10),
        "retries": int(pb.get("max_retries", 3) or 3),
        "pb": pb,
        "acct_id": safe_int(pb.get("account_id")),
        "username": (pb.get("username") or "").strip(),
        "cid": (pb.get("client_id") or CID),
        "mid": mid,
        "hist_tok": tok,
    }
    return srv, meta

# ---------- managed/shared user token ----------
def _xml_users(xml_text: str) -> Iterable[dict]:
    root = ET.fromstring(xml_text)
    for u in root.findall(".//User"):
        a = u.attrib or {}
        yield {
            "id": safe_int(a.get("id")),
            "title": (a.get("title") or a.get("username") or "").strip(),
            "token": (a.get("accessToken") or a.get("token") or "").strip(),
        }

def resolve_history_token(meta: dict) -> str:
    aid = meta.get("acct_id")
    uname = (meta.get("username") or "").lower().strip()
    mid = meta.get("mid")
    if not mid or (not aid and not uname):
        return meta["tok"]

    try:
        r = requests.get(
            f"{PLEX_TV}/api/servers/{mid}/shared_servers",
            params={"X-Plex-Token": meta["tok"]},
            headers={"Accept": "application/xml"},
            timeout=10,
        )
        if r.ok and (r.text or "").lstrip().startswith("<"):
            for u in _xml_users(r.text):
                if not u["token"]:
                    continue
                if aid and u["id"] == aid:
                    return u["token"]
                if uname and u["title"].lower() == uname:
                    return u["token"]
    except Exception:
        pass

    try:
        r = requests.get(
            f"{PLEX_TV}/api/home/users",
            params={"X-Plex-Token": meta["tok"]},
            headers={"Accept": "application/xml"},
            timeout=10,
        )
        if r.ok and (r.text or "").lstrip().startswith("<"):
            for u in _xml_users(r.text):
                if (aid and u["id"] == aid) or (uname and u["title"].lower() == uname):
                    print("[!] Plex Home user detected. No server token without PIN-switch; history restore may 401.")
                    break
    except Exception:
        pass

    return meta["tok"]


# ---------- Discover ----------
def d_headers(meta: dict, json_only: bool = False) -> dict:
    return {
        "X-Plex-Product": "CrossWatch",
        "X-Plex-Platform": "CrossWatch",
        "X-Plex-Version": "3.1.0",
        "X-Plex-Client-Identifier": str(meta["cid"] or CID),
        "X-Plex-Token": meta["tok"],
        "Accept": "application/json" if json_only else "application/json, application/xml;q=0.9,*/*;q=0.5",
    }

def xml_to_container(txt: str) -> dict:
    root = ET.fromstring(txt)
    mc = root if root.tag.endswith("MediaContainer") else root.find(".//MediaContainer")
    rows = []
    if mc is not None:
        for md in mc.findall("./Metadata"):
            a = md.attrib
            rows.append({
                "type": a.get("type"),
                "title": a.get("title"),
                "year": safe_int(a.get("year")),
                "guid": a.get("guid"),
                "ratingKey": a.get("ratingKey"),
                "Guid": [{"id": g.attrib.get("id")} for g in md.findall("./Guid") if g.attrib.get("id")],
            })
    return {"MediaContainer": {"Metadata": rows}}

def d_get(meta: dict, path: str, params: dict, json_only: bool = False) -> dict:
    url = f"{DISCOVER}{path}" if path.startswith("/") else path
    ses = requests.Session()
    def _do():
        r = ses.get(url, headers=d_headers(meta, json_only), params=params, timeout=meta["timeout"])
        if r.status_code == 401: raise RuntimeError("Unauthorized (bad Plex token).")
        if not r.ok: raise RuntimeError(f"GET {path} -> {r.status_code}: {(r.text or '')[:150]}")
        ctype = (r.headers.get("content-type") or "").lower()
        body = r.text or ""
        if "json" in ctype: return r.json() or {}
        if "xml" in ctype or body.lstrip().startswith("<"): return xml_to_container(body)
        return {}
    return retry(meta["retries"], _do)

def d_write(meta: dict, action_path: str, rating_key: str) -> bool:
    url = f"{DISCOVER}{action_path}"
    ses = requests.Session()
    def _do():
        r = ses.put(url, headers=d_headers(meta, True), params={"ratingKey": rating_key}, timeout=meta["timeout"])
        if r.status_code in (200, 201, 204, 409): return True
        if r.status_code in (404, 405):
            r2 = ses.post(url, headers=d_headers(meta, True), params={"ratingKey": rating_key}, timeout=meta["timeout"])
            return r2.status_code in (200, 201, 204, 409)
        body = (r.text or "").lower()
        if "not on the watchlist" in body or "already on the watchlist" in body:
            return True
        return False
    return bool(retry(meta["retries"], _do))

def iter_meta_rows(cont: Optional[dict]) -> Iterable[dict]:
    if not cont: return
    mc = cont.get("MediaContainer") or cont
    meta = mc.get("Metadata") if isinstance(mc, dict) else None
    if isinstance(meta, list):
        for row in meta:
            if isinstance(row, dict): yield row

def iter_search_rows(cont: Optional[dict]) -> Iterable[dict]:
    if not cont: return
    mc = cont.get("MediaContainer") or {}
    for sr in (mc.get("SearchResults") or []):
        for it in (sr.get("SearchResult") or []):
            md = it.get("Metadata")
            if isinstance(md, dict): yield md
            elif isinstance(md, list):
                for m in md:
                    if isinstance(m, dict): yield m
    mds = mc.get("Metadata")
    if isinstance(mds, list):
        for m in mds:
            if isinstance(m, dict): yield m
    elif isinstance(mds, dict):
        yield mds


# ---------- Watchlist ----------
def ids_from_guid(g: str) -> dict:
    if not g: return {}
    ids = {}
    m = re.search(r"(tt\d{5,10})", g); ids["imdb"] = m.group(1) if m else None
    m = re.search(r"(?:tmdb|themoviedb)[:/ -]?(\d{2,10})", g, re.I); ids["tmdb"] = m.group(1) if m else None
    m = re.search(r"(?:tvdb)[:/ -]?(\d{2,10})", g, re.I); ids["tvdb"] = m.group(1) if m else None
    return {k:v for k,v in ids.items() if v}

def hydrate_ids(meta: dict, rk: str) -> dict:
    try:
        r = requests.get(f"{METADATA}/library/metadata/{rk}", headers=d_headers(meta), timeout=meta["timeout"])
        if not r.ok: return {}
        cont = r.json() if "json" in (r.headers.get("content-type","").lower()) else xml_to_container(r.text or "")
        md = (cont.get("MediaContainer") or {}).get("Metadata") or []
        if not md or not isinstance(md, list): return {}
        row = md[0]
        out = ids_from_guid(str(row.get("guid") or ""))
        for gg in (row.get("Guid") or []):
            gid = (gg or {}).get("id")
            if gid: out.update(ids_from_guid(str(gid)))
        return out
    except Exception:
        return {}

def metadata_match(meta: dict, ids: dict, libtype: str, year: Optional[int]) -> Optional[str]:
    type_code = "show" if libtype == "show" else "movie"
    for k in ("imdb","tmdb","tvdb"):
        v = ids.get(k)
        if not v: continue
        params = {"type": type_code, "title": f"{k}-{v}"}
        if year: params["year"] = int(year)
        cont = d_get(meta, MATCHES, params, json_only=True)
        for row in iter_search_rows(cont):
            rk = str(row.get("ratingKey") or "")
            if not rk: continue
            row_ids = ids_from_guid(str(row.get("guid") or ""))
            if row_ids.get(k) == v: return rk
            ext = hydrate_ids(meta, rk)
            if ext.get(k) == v: return rk
    return None

def discover_search_resolve(meta: dict, title: str, year: Optional[int], libtype: str, ids: dict) -> Optional[str]:
    if not title: return None
    queries = [title] + ([f"{title} {year}"] if year else [])
    params_common = {
        "limit": 25, "searchTypes": "movies,tv", "searchProviders": "discover",
        "includeMetadata": 1, "includeExternalMedia": 1,
    }
    want_pairs = {(k, str(v)) for k, v in ids.items() if k in ("imdb","tmdb","tvdb")}
    for q in queries:
        cont = d_get(meta, "/library/search", {**params_common, "query": q}, json_only=True)
        for row in iter_search_rows(cont):
            rk = str(row.get("ratingKey") or "")
            if not rk: continue
            if year and safe_int(row.get("year")) not in (None, year):
                continue
            if want_pairs:
                ext = ids_from_guid(str(row.get("guid") or "")) | hydrate_ids(meta, rk)
                pairs = {(k, str(v)) for k, v in ext.items() if k in ("imdb","tmdb","tvdb")}
                if pairs & want_pairs: return rk
            else:
                return rk
    return None

def resolve_wl_rk(meta: dict, it: dict) -> Optional[str]:
    rk = str(it.get("ratingKey") or "").strip()
    if rk and rk.lower() not in ("none","null"): return rk

    guid = str(it.get("guid") or "")
    ids = ids_from_guid(guid)
    kind = (it.get("Type") or it.get("type") or "movie").lower()
    libtype = "show" if kind in ("show","series","tv") else "movie"
    year = safe_int(it.get("Year") or it.get("year"))
    title = str(it.get("Title") or it.get("title") or "").strip()

    if ids:
        m = metadata_match(meta, ids, libtype, year)
        if m: return m
    return discover_search_resolve(meta, title, year, libtype, ids)

def collect_watchlist(meta: dict) -> List[dict]:
    page_size = int(meta["pb"].get("watchlist_page_size", 100) or 100)
    out, start, total = [], 0, None
    base_params = {"includeCollections": 1, "includeExternalMedia": 1}
    while True:
        params = {**base_params,
                  "X-Plex-Container-Start": start,
                  "X-Plex-Container-Size": page_size,
                  "offset": start, "limit": page_size}
        cont = d_get(meta, WL_PATH, params)
        mc = cont.get("MediaContainer") or cont
        if total is None:
            total = safe_int(mc.get("totalSize") or mc.get("size"))
        rows = list(iter_meta_rows(cont))
        if not rows: break
        for r in rows:
            out.append({
                "ratingKey": safe_int(r.get("ratingKey")),
                "Type": (r.get("type") or "?").lower(),
                "Title": r.get("title") or "?",
                "Year": safe_int(r.get("year")),
                "guid": r.get("guid"),
            })
        start += len(rows)
        if total is not None and start >= total: break
        if total is None and len(rows) < page_size: break
    return out

def clear_watchlist(meta: dict, items: List[dict], srv: PlexServer) -> int:
    ok = 0
    for it in items:
        rk = resolve_wl_rk(meta, it)
        if rk and d_write(meta, ACT_REM, rk):
            ok += 1
        elif rk:
            try:
                srv.fetchItem(int(rk)).removeFromWatchlist()
                ok += 1
            except Exception:
                pass
    if ok != len(items):
        print(f"[!] Watchlist unresolved: {len(items)-ok}")
    return ok

def restore_watchlist(meta: dict, items: List[dict], srv: PlexServer) -> int:
    ok = 0
    for it in items:
        rk = resolve_wl_rk(meta, it)
        if rk and d_write(meta, ACT_ADD, rk):
            ok += 1
        elif rk:
            try:
                srv.fetchItem(int(rk)).addToWatchlist()
                ok += 1
            except Exception:
                pass
    if ok != len(items):
        print(f"[!] Watchlist unresolved: {len(items)-ok}")
    return ok


# ---------- History (playback log) ----------
def collect_history_log(srv: PlexServer, meta: dict) -> List[dict]:
    kwargs = {"accountID": meta["acct_id"]} if meta["acct_id"] else {}
    rows = retry(meta["retries"], srv.history, **kwargs) or []
    if kwargs and not rows:
        rows = retry(meta["retries"], srv.history) or []
    out = []
    for h in rows:
        ts = as_epoch(getattr(h, "viewedAt", None) or getattr(h, "viewed_at", None) or getattr(h, "lastViewedAt", None))
        rk = safe_int(getattr(h, "ratingKey", None) or getattr(h, "key", None))
        if ts is None or rk is None: continue
        out.append({"ratingKey": str(rk),
                    "Type": (getattr(h, "type", None) or "?").lower(),
                    "Title": getattr(h, "title", None) or "?",
                    "watched_at": iso(ts)})
    return out


# ---------- History (watched state) ----------
def user_server(meta: dict) -> PlexServer:
    tok = meta["hist_tok"] or meta["tok"]
    return PlexServer(meta["base"], tok, timeout=meta["timeout"])

def collect_watched_state(meta: dict) -> List[dict]:
    srv_u = user_server(meta)
    out: Dict[str, dict] = {}
    scanned = 0

    for sec in srv_u.library.sections():
        if (sec.type or "").lower() not in ("movie", "show"):
            continue
        try:
            items = sec.all() or []
        except Exception:
            items = []
        for obj in items:
            scanned += 1
            try:
                vc = getattr(obj, "viewCount", 0) or 0
                iw = getattr(obj, "isWatched", False)
                if not iw and vc <= 0:
                    continue
                rk = str(int(obj.ratingKey))
                out[rk] = {
                    "ratingKey": rk,
                    "Type": (getattr(obj, "type", None) or sec.type or "?").lower(),
                    "Title": getattr(obj, "title", None) or "?",
                    "Year": safe_int(getattr(obj, "year", None)),
                    "lastViewedAt": iso(as_epoch(getattr(obj, "lastViewedAt", None)) or int(time.time())),
                }
            except Exception:
                continue
            if scanned % 500 == 0:
                print(f"\rScanning watched state... {scanned}", end="")
    if scanned:
        print("\r" + " " * 60 + "\r", end="")

    return list(out.values())

def pms_req(srv: PlexServer, path: str, params: dict, tok: str):
    url = srv.url(path)
    p = dict(params or {})
    p["X-Plex-Token"] = tok
    return requests.get(url, params=p, timeout=10)

def unscrobble(srv: PlexServer, rk: str, tok: str):
    r = pms_req(srv, "/:/unscrobble",
                {"key": int(rk), "identifier": "com.plexapp.plugins.library"}, tok)
    if not r.ok: raise RuntimeError(r.status_code)

def scrobble_at(srv: PlexServer, rk: str, epoch: int, tok: str):
    r = pms_req(srv, "/:/scrobble",
                {"key": int(rk), "identifier": "com.plexapp.plugins.library", "viewedAt": int(epoch)}, tok)
    if not r.ok: raise RuntimeError(r.status_code)

def _try_tokens(fn, tokens: List[str]):
    last = None
    for t in tokens:
        try:
            fn(t); return True
        except Exception as e:
            last = e
    if last: raise last
    return False

def clear_history_state(srv: PlexServer, meta: dict, watched_items: List[dict]) -> int:
    toks = [meta["hist_tok"], meta["tok"]]
    ok = 0
    for it in watched_items:
        try:
            _try_tokens(lambda t: unscrobble(srv, it["ratingKey"], t), toks)
            ok += 1
        except Exception:
            pass
    if ok != len(watched_items):
        print(f"[!] History unresolved: {len(watched_items) - ok}")
    print("[i] Plex keeps playback logs; this only marks items unwatched.")
    return ok


# ---------- Ratings ----------
def norm_rating(v: Any) -> Optional[int]:
    try:
        f = float(v)
        if 0 <= f <= 5: f *= 2
        i = int(round(f))
        return i if 0 <= i <= 10 else None
    except Exception:
        return None

def collect_ratings(srv: PlexServer, meta: dict) -> List[dict]:
    rated = []
    for sec in srv.library.sections():
        if (sec.type or "").lower() not in ("movie","show"):
            continue
        for obj in (sec.all() or []):
            r = norm_rating(getattr(obj, "userRating", None))
            if r and r > 0:
                rated.append({
                    "ratingKey": str(int(obj.ratingKey)),
                    "Type": (getattr(obj, "type", None) or sec.type or "?").lower(),
                    "Title": getattr(obj, "title", None) or "?",
                    "Rating": int(r),
                    "rated_at": iso(as_epoch(getattr(obj, "lastRatedAt", None)) or int(time.time())),
                })
    return rated

def rate(srv: PlexServer, rk: str, r10: int):
    r = requests.get(srv.url("/:/rate"),
                     params={"key": int(rk), "identifier": "com.plexapp.plugins.library", "rating": int(r10),
                             "X-Plex-Token": getattr(srv, "_token", None) or getattr(srv, "token", None)},
                     timeout=10)
    if not r.ok: raise RuntimeError(r.status_code)

def clear_ratings(srv: PlexServer, meta: dict, items: List[dict]) -> int:
    for it in items:
        retry(meta["retries"], rate, srv, it["ratingKey"], 0)
    return len(items)


# ---------- Backup ----------
def ensure_backup_dir():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

def cleanup_old_backups():
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    for p in BACKUP_DIR.glob("plex_backup_*.json.gz"):
        try:
            if datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) < cutoff:
                p.unlink()
        except Exception:
            pass

def backup_now(hist: List[dict], rats: List[dict], wl: List[dict], meta: dict) -> Path:
    ensure_backup_dir(); cleanup_old_backups()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"plex_backup_{ts}.json.gz"
    payload = {
        "schema": 2, "provider": "plex",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "server_url": meta["base"], "username": meta["username"], "account_id": meta["acct_id"],
        "counts": {"watchlist": len(wl), "history": len(hist), "ratings": len(rats)},
        "watchlist": wl, "history": hist, "ratings": rats,
    }
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)
    return path

def list_backups() -> List[Path]:
    ensure_backup_dir()
    return sorted(BACKUP_DIR.glob("plex_backup_*.json.gz"), key=lambda p: p.stat().st_mtime, reverse=True)

def choose_backup() -> Optional[Path]:
    files = list_backups()
    if not files:
        print("No backups found."); return None
    print("\nAvailable backups:")
    for i, p in enumerate(files, 1):
        age = datetime.now(timezone.utc) - datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc)
        print(f"{i}. {p.name} ({age.days}d ago)")
    s = input("Select backup #: ").strip()
    return files[int(s)-1] if s.isdigit() and 1 <= int(s) <= len(files) else None

def load_backup(p: Path) -> dict:
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return json.load(f) or {}

def restore_from_backup(srv: PlexServer, meta: dict, b: dict):
    wl = b.get("watchlist") or []
    hist = b.get("history") or []
    rats = b.get("ratings") or []

    print(f"\nBackup: {b.get('created_at')}  wl={len(wl)} hist={len(hist)} rats={len(rats)}")
    c = input("Restore [w]atchlist / [h]istory / [r]atings / [b]oth(all): ").strip().lower()

    if c in ("w","b") and wl:
        restore_watchlist(meta, wl, srv)

    if c in ("h","b") and hist:
        toks = [meta["hist_tok"], meta["tok"]]
        ok = fail = 0
        for it in hist:
            rk = it.get("ratingKey"); ts = as_epoch(it.get("watched_at"))
            if not (rk and ts): continue
            try:
                _try_tokens(lambda t: scrobble_at(srv, rk, ts, t), toks)
                ok += 1
            except Exception:
                fail += 1
        print(f"History restore: ok={ok}, fail={fail}")

    if c in ("r","b") and rats:
        ok = fail = 0
        for it in rats:
            rk = it.get("ratingKey"); r10 = norm_rating(it.get("Rating"))
            if rk and r10 and r10 > 0:
                try:
                    retry(meta["retries"], rate, srv, rk, r10)
                    ok += 1
                except Exception:
                    fail += 1
        print(f"Ratings restore: ok={ok}, fail={fail}")

    print("Restore done.")


# ---------- menu ----------
def menu() -> str:
    print("\n=== Plex Cleanup and Backup/Restore ===")
    print("1. Show Plex Watchlist")
    print("2. Show Plex Watched Items (state)")
    print("3. Show Plex Ratings")
    print("4. Remove Plex Watchlist")
    print("5. Remove Plex Watched State (unwatch)")
    print("6. Remove Plex Ratings")
    print("7. Backup Plex (w+h+r)")
    print("8. Restore Plex from Backup")
    print("9. Clean Plex (w+h+r)")
    print("10. Show Plex Playback Log (raw history)")
    print("0. Exit")
    return input("Select: ").strip()

def show(rows: List[dict], cols: List[str], page: int = 25):
    if not rows: print("(none)"); return
    for i in range(0, len(rows), page):
        chunk = rows[i:i+page]
        print(f"\nShowing {i+1}-{i+len(chunk)} of {len(rows)}")
        head = " | ".join(c.ljust(12) for c in cols); print(head); print("-"*len(head))
        for r in chunk:
            print(" | ".join(str(r.get(c,""))[:60].ljust(12) for c in cols))
        if i + page < len(rows):
            if input("[Enter]=Next, q=Quit: ").strip().lower() == "q": break

def main():
    cfg = jload(CONFIG_PATH)
    try:
        srv, meta = build_client(cfg)
    except Exception as e:
        print(f"[!] Plex connect failed: {e}"); return

    meta["hist_tok"] = resolve_history_token(meta)

    while True:
        ch = menu()
        try:
            if ch == "0": return

            if ch == "1":
                wl = collect_watchlist(meta)
                print(f"\nPlex watchlist items: {len(wl)}")
                show(wl, ["Type","Title","Year","ratingKey"])

            elif ch == "2":
                watched = collect_watched_state(meta)
                print(f"\nPlex watched items (current state): {len(watched)}")
                show(watched, ["Type","Title","Year","lastViewedAt","ratingKey"])

            elif ch == "3":
                rats = collect_ratings(srv, meta)
                print(f"\nPlex rated items: {len(rats)}")
                show(rats, ["Type","Title","Rating","rated_at","ratingKey"])

            elif ch == "4":
                wl = collect_watchlist(meta)
                print(f"\nFound {len(wl)} watchlist items.")
                if input("Type YES to continue: ").strip().upper() != "YES": continue
                n = clear_watchlist(meta, wl, srv)
                print(f"Done. Cleared {n} watchlist items.")

            elif ch == "5":
                watched = collect_watched_state(meta)
                print(f"\nFound {len(watched)} watched items.")
                if input("Type YES to continue: ").strip().upper() != "YES": continue
                n = clear_history_state(srv, meta, watched)
                print(f"Done. Unwatched {n} items.")

            elif ch == "6":
                rats = collect_ratings(srv, meta)
                print(f"\nFound {len(rats)} rated items.")
                if input("Type YES to continue: ").strip().upper() != "YES": continue
                n = clear_ratings(srv, meta, rats)
                print(f"Done. Cleared ratings on {n} items.")

            elif ch == "7":
                wl = collect_watchlist(meta)
                hist = collect_history_log(srv, meta)
                rats = collect_ratings(srv, meta)
                p = backup_now(hist, rats, wl, meta)
                print(f"Backup written: {p}")

            elif ch == "8":
                p = choose_backup()
                if not p: continue
                restore_from_backup(srv, meta, load_backup(p))

            elif ch == "9":
                wl = collect_watchlist(meta)
                watched = collect_watched_state(meta)
                rats = collect_ratings(srv, meta)
                print(f"\nWatchlist={len(wl)} | Watched={len(watched)} | Ratings={len(rats)}")
                if input("Type YES to continue: ").strip().upper() != "YES": continue
                print("Cleaning...")
                nw = clear_watchlist(meta, wl, srv)
                nh = clear_history_state(srv, meta, watched)
                nr = clear_ratings(srv, meta, rats)
                print(f"Done. Cleared watchlist={nw}, watched(unwatch)={nh}, ratings={nr}.")

            elif ch == "10":
                hist = collect_history_log(srv, meta)
                print(f"\nPlex playback log entries: {len(hist)}")
                show(hist, ["Type","Title","watched_at","ratingKey"])

            else:
                print("Unknown option.")

        except Exception as e:
            print(f"[!] Error: {e}")

if __name__ == "__main__":
    main()
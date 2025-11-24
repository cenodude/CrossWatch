#!/usr/bin/env python3
# Emby cleanup + backup/restore (history only).
from __future__ import annotations
import json, gzip, time
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Iterable, Tuple
from urllib.parse import urlparse

import requests

CONFIG_PATH = Path("/config/config.json")
BACKUP_DIR = Path("/config/backup")
RETENTION_DAYS = 15
PAGE_SIZE = 200
SHOW_PAGE = 25
TIMEOUT = 30
HISTORY_TYPES = "Movie,Episode"

# ---------- utils ----------
def jload(p: Path) -> dict:
    try: return json.loads(p.read_text("utf-8"))
    except Exception: return {}

def netloc(url: str) -> str:
    try: return urlparse(url).netloc or url
    except Exception: return url

def confirm(msg: str) -> bool:
    print(msg)
    return input("Type YES to continue: ").strip().upper() == "YES"

def retry(n: int, fn, *a, **kw):
    for i in range(n):
        try: return fn(*a, **kw)
        except Exception:
            if i == n - 1: raise
            time.sleep(0.5 * (i + 1))

def emby_cfg(cfg: dict) -> Tuple[str, str, str]:
    b = cfg.get("emby") or {}
    server = (b.get("server") or "").strip().rstrip("/")
    tok = (b.get("access_token") or "").strip()
    uid = (b.get("user_id") or "").strip()
    if not (server and tok and uid):
        raise RuntimeError("Missing emby config (server/access_token/user_id).")
    return server, tok, uid

def headers(tok: str) -> dict:
    return {"X-Emby-Token": tok, "Content-Type": "application/json"}

def req(method: str, url: str, h: dict, **kw) -> dict:
    r = requests.request(method, url, headers=h, timeout=TIMEOUT, **kw)
    if r.status_code == 401:
        raise RuntimeError("Unauthorized (bad Emby token).")
    if not r.ok:
        raise RuntimeError(f"{method} {url} -> {r.status_code}: {(r.text or '')[:160]}")
    if r.text and "json" in (r.headers.get("content-type","").lower()):
        return r.json() or {}
    return {}

# ---------- paging / display ----------
def paginate(server: str, h: dict, uid: str, types: str, extra: dict) -> Iterable[dict]:
    start = 0
    while True:
        p = {
            "Recursive": "true",
            "IncludeItemTypes": types,
            "Limit": PAGE_SIZE,
            "StartIndex": start,
            **extra,
        }
        data = req("GET", f"{server}/Users/{uid}/Items", h, params=p)
        items = (data.get("Items") or []) if isinstance(data, dict) else []
        if not items: break
        yield from items
        start += PAGE_SIZE

def show(rows: List[dict], cols: List[str], page: int = SHOW_PAGE):
    if not rows:
        print("(none)"); return
    for i in range(0, len(rows), page):
        chunk = rows[i:i+page]
        print(f"\nShowing {i+1}-{i+len(chunk)} of {len(rows)}")
        head = " | ".join(c.ljust(10) for c in cols); print(head); print("-"*len(head))
        for r in chunk:
            print(" | ".join(str(r.get(c,""))[:70] for c in cols))
        if i + page < len(rows):
            if input("[Enter]=Next, q=Quit: ").strip().lower() == "q":
                break


# ---------- history ----------
def collect_history(server: str, h: dict, uid: str) -> List[dict]:
    out = []
    for it in paginate(server, h, uid, HISTORY_TYPES, {"IsPlayed": "true", "Fields": "Type,Name,SeriesId"}):
        out.append({
            "Id": it.get("Id"),
            "Type": it.get("Type") or "?",
            "Name": it.get("Name") or "?",
            "SeriesId": it.get("SeriesId"),
        })
    return out

def clear_history(server: str, h: dict, uid: str, items: List[dict]) -> int:
    for it in items:
        req("DELETE", f"{server}/Users/{uid}/PlayedItems/{it['Id']}", h)
    return len(items)

def restore_history(server: str, h: dict, uid: str, items: List[dict]) -> Tuple[int, int]:
    ok = fail = 0
    for it in items:
        try:
            req("POST", f"{server}/Users/{uid}/PlayedItems/{it['Id']}", h, json={})
            ok += 1
        except Exception:
            fail += 1
    return ok, fail

# ---------- backup ----------
def ensure_backup_dir():
    BACKUP_DIR.mkdir(parents=True, exist_ok=True)

def cleanup_old_backups():
    cutoff = datetime.now(timezone.utc) - timedelta(days=RETENTION_DAYS)
    for p in BACKUP_DIR.glob("emby_*.json.gz"):
        try:
            if datetime.fromtimestamp(p.stat().st_mtime, tz=timezone.utc) < cutoff:
                p.unlink()
        except Exception:
            pass

def create_backup(server: str, h: dict, uid: str) -> Path:
    ensure_backup_dir(); cleanup_old_backups()
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    path = BACKUP_DIR / f"emby_{ts}_backup.json.gz"
    hist = collect_history(server, h, uid)
    payload = {
        "schema_version": 1,
        "provider": "emby",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "user_id": uid,
        "server": netloc(server),
        "history": hist,
    }
    with gzip.open(path, "wt", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False)
    print(f"Backup written: {path}")
    return path

def list_backups() -> List[Path]:
    ensure_backup_dir()
    return sorted(BACKUP_DIR.glob("emby_*.json.gz"), key=lambda p: p.stat().st_mtime, reverse=True)

def choose_backup() -> Optional[Path]:
    files = list_backups()
    if not files:
        print("No backups found."); return None
    print("\nAvailable backups:")
    for i, p in enumerate(files, 1):
        dt = datetime.fromtimestamp(p.stat().st_mtime).strftime("%Y-%m-%d %H:%M:%S")
        print(f"{i}. {p.name} ({dt})")
    s = input("Select backup # or full path: ").strip()
    if s.isdigit():
        idx = int(s)
        return files[idx-1] if 1 <= idx <= len(files) else None
    p = Path(s)
    return p if p.exists() else None

def load_backup(p: Path) -> dict:
    with gzip.open(p, "rt", encoding="utf-8") as f:
        return json.load(f) or {}

def restore_from_backup(server: str, h: dict, uid: str):
    p = choose_backup()
    if not p: return
    b = load_backup(p)
    hist = b.get("history") or []
    print(f"\nBackup: {b.get('created_at')}  history={len(hist)}")
    if not hist:
        print("[!] Backup contains no history."); return
    if not confirm("This will APPLY the backup history to Emby."):
        print("Aborted."); return
    ok, fail = restore_history(server, h, uid, hist)
    print(f"History restore: ok={ok}, fail={fail}")

# ---------- menu ----------
def menu() -> str:
    print("\n=== Emby Cleanup and Backup/Restore ===")
    print("1. Show History")
    print("2. Remove History")
    print("3. Create Backup")
    print("4. Restore Backup")
    print("0. Exit")
    return input("Select: ").strip()

def main():
    cfg = jload(CONFIG_PATH)
    try:
        server, tok, uid = emby_cfg(cfg)
    except Exception as e:
        print(f"[!] Emby not configured: {e}"); return
    h = headers(tok)
    while True:
        ch = menu()
        try:
            if ch == "0": return

            if ch == "1":
                hist = collect_history(server, h, uid)
                print(f"\nPlayed items: {len(hist)}")
                show(hist, ["Type", "Name", "Id"])
            elif ch == "2":
                hist = collect_history(server, h, uid)
                print(f"\nFound {len(hist)} played items.")
                if not confirm("This will clear ALL Emby watched status for this user."):
                    continue
                n = clear_history(server, h, uid, hist)
                print(f"Done. Cleared {n} played items.")
            elif ch == "3":
                create_backup(server, h, uid)
            elif ch == "4":
                restore_from_backup(server, h, uid)
            else:
                print("Unknown option.")

        except Exception as e:
            print(f"[!] Error: {e}")

if __name__ == "__main__":
    main()
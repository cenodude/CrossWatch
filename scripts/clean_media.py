#!/usr/local/bin/python
"""
CrossWatch Jellyfin/Emby cleanup utility.
- Shows or clears Jellyfin history, Jellyfin ratings, Emby history.
- Reads /config/config.json for server/access_token/user_id.
"""

from __future__ import annotations
import json
import sys
from pathlib import Path
from typing import Any, Dict, Iterable, List, Tuple, Optional
import requests

CONFIG_PATH = Path("/config/config.json")
PAGE_SIZE = 200
SHOW_PAGE_SIZE = 25

JF_HISTORY_TYPES = "Movie,Episode"
JF_RATING_TYPES = "Movie,Series,Episode"
EMBY_HISTORY_TYPES = "Movie,Episode"

TIMEOUT = 30
BAR_WIDTH = 28
# ---------- config ----------
def load_cfg() -> Dict[str, Any]:
    try:
        return json.loads(CONFIG_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        print(f"[!] Failed to read {CONFIG_PATH}: {e}")
        return {}

def get_provider(cfg: Dict[str, Any], name: str) -> Tuple[str, str, str]:
    block = (cfg.get(name) or {})
    server = (block.get("server") or "").strip().rstrip("/")
    token = (block.get("access_token") or "").strip()
    user_id = (block.get("user_id") or "").strip()
    if not (server and token and user_id):
        raise RuntimeError(f"Missing {name} config (server/access_token/user_id).")
    return server, token, user_id

def jf_headers(token: str) -> Dict[str, str]:
    return {
        "X-MediaBrowser-Token": token,
        "Authorization": f'MediaBrowser Token="{token}"',
        "Content-Type": "application/json",
    }

def emby_headers(token: str) -> Dict[str, str]:
    return {"X-Emby-Token": token}

# ---------- helpers ----------
def progress_line(label: str, current: int, total: Optional[int] = None) -> None:
    if total and total > 0:
        pct = current / total
        fill = int(BAR_WIDTH * pct)
        bar = "#" * fill + "-" * (BAR_WIDTH - fill)
        msg = f"{label} [{bar}] {current}/{total}"
    else:
        # unknown total -> simple growing bar
        fill = min(BAR_WIDTH, current % (BAR_WIDTH + 1))
        bar = "#" * fill + "-" * (BAR_WIDTH - fill)
        msg = f"{label} [{bar}] {current}"
    sys.stdout.write("\r" + msg.ljust(80))
    sys.stdout.flush()

def done_line() -> None:
    sys.stdout.write("\r" + (" " * 80) + "\r")
    sys.stdout.flush()

def print_table(rows: List[Dict[str, Any]], cols: List[Tuple[str, str]]) -> None:
    if not rows:
        print("(none)")
        return
    widths = []
    for key, title in cols:
        w = max(len(title), *(len(str(r.get(key, ""))) for r in rows))
        widths.append(min(w, 60))
    header = " | ".join(title.ljust(widths[i]) for i, (_, title) in enumerate(cols))
    sep = "-+-".join("-" * widths[i] for i in range(len(cols)))
    print(header)
    print(sep)
    for r in rows:
        line = " | ".join(
            str(r.get(k, ""))[:widths[i]].ljust(widths[i])
            for i, (k, _) in enumerate(cols)
        )
        print(line)

def paged_display(rows: List[Dict[str, Any]], cols: List[Tuple[str, str]], page_size: int = SHOW_PAGE_SIZE) -> None:
    if not rows:
        print("(none)")
        return

    page = 0
    pages = (len(rows) + page_size - 1) // page_size

    while True:
        start = page * page_size
        end = min(len(rows), start + page_size)
        print(f"\nShowing {start + 1}-{end} of {len(rows)} (page {page + 1}/{pages})")
        print_table(rows[start:end], cols)

        if pages <= 1:
            return

        cmd = input("\n[Enter]=Next | p=Prev | q=Quit : ").strip().lower()
        if cmd == "q":
            return
        if cmd == "p":
            page = max(0, page - 1)
        else:
            page = min(pages - 1, page + 1)

def confirm_danger(msg: str) -> bool:
    print(msg)
    ans = input("Type YES to continue: ").strip().upper()
    return ans == "YES"

# http:
def _get(url: str, headers: Dict[str, str], params: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.get(url, headers=headers, params=params, timeout=TIMEOUT)
    r.raise_for_status()
    return r.json()

def _delete(url: str, headers: Dict[str, str]) -> None:
    r = requests.delete(url, headers=headers, timeout=TIMEOUT)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"DELETE {url} -> {r.status_code}: {r.text[:200]}")

def _post(url: str, headers: Dict[str, str], payload: Dict[str, Any]) -> None:
    r = requests.post(url, headers=headers, json=payload, timeout=TIMEOUT)
    if r.status_code not in (200, 204):
        raise RuntimeError(f"POST {url} -> {r.status_code}: {r.text[:200]}")

def paginate_items(
    server: str,
    headers: Dict[str, str],
    user_id: str,
    include_types: str,
    extra_params: Optional[Dict[str, Any]] = None,
) -> Iterable[Dict[str, Any]]:
    start = 0
    extra_params = extra_params or {}
    while True:
        params = {
            "Recursive": "true",
            "IncludeItemTypes": include_types,
            "Limit": PAGE_SIZE,
            "StartIndex": start,
            **extra_params,
        }
        data = _get(f"{server}/Users/{user_id}/Items", headers, params)
        items = data.get("Items") or []
        if not items:
            break
        for it in items:
            yield it
        start += PAGE_SIZE

# History:
def collect_played_items(
    server: str,
    headers: Dict[str, str],
    user_id: str,
    include_types: str,
    label: str,
) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    count = 0
    for it in paginate_items(
        server, headers, user_id, include_types,
        extra_params={"IsPlayed": "true", "Fields": "Type,Name,SeriesId"}
    ):
        out.append({
            "Id": it.get("Id"),
            "Type": it.get("Type") or "?",
            "Name": it.get("Name") or "?",
            "SeriesId": it.get("SeriesId"),
        })
        count += 1
        progress_line(label, count)
    done_line()
    return out

def history_stats(items: List[Dict[str, Any]]) -> Dict[str, int]:
    movies = sum(1 for i in items if i["Type"] == "Movie")
    episodes = sum(1 for i in items if i["Type"] == "Episode")
    shows = len({i["SeriesId"] for i in items if i["Type"] == "Episode" and i.get("SeriesId")})
    other = len(items) - movies - episodes
    return {"movies": movies, "episodes": episodes, "shows": shows, "other": other, "total": len(items)}

def print_stats_block(title: str, stats: Dict[str, int]) -> None:
    print(f"\n=== {title} ===")
    print(f"Movies played : {stats['movies']}")
    print(f"Episodes played: {stats['episodes']}  (from ~{stats['shows']} shows)")
    if stats["other"] > 0:
        print(f"Other played  : {stats['other']}")
    print(f"Total played  : {stats['total']}")

def clear_history(
    server: str,
    headers: Dict[str, str],
    user_id: str,
    include_types: str,
    label: str,
) -> int:
    items = collect_played_items(server, headers, user_id, include_types, f"{label} fetching")
    total = len(items)
    if total == 0:
        print("Nothing to clear.")
        return 0

    for idx, it in enumerate(items, 1):
        _delete(f"{server}/Users/{user_id}/PlayedItems/{it['Id']}", headers)
        progress_line(f"{label} deleting", idx, total)
    done_line()
    return total

# Ratings (jellyfin only) ----------
def collect_rated_items(
    server: str,
    headers: Dict[str, str],
    user_id: str,
    label: str,
) -> List[Dict[str, Any]]:
    rated: List[Dict[str, Any]] = []
    seen = 0
    for it in paginate_items(
        server, headers, user_id, JF_RATING_TYPES,
        extra_params={"Fields": "UserData,UserRating,Type,Name"}
    ):
        seen += 1
        ur = it.get("UserRating") or (it.get("UserData") or {}).get("Rating") or 0
        if (ur or 0) > 0:
            rated.append({
                "Id": it.get("Id"),
                "Type": it.get("Type") or "?",
                "Name": it.get("Name") or "?",
                "Rating": ur,
            })
        if seen % 10 == 0:
            progress_line(label, seen)
    done_line()
    return rated

def clear_ratings(
    server: str,
    headers: Dict[str, str],
    user_id: str,
    items: List[Dict[str, Any]],
    label: str,
) -> int:
    total = len(items)
    if total == 0:
        print("Nothing to clear.")
        return 0

    for idx, it in enumerate(items, 1):
        payload = {"Rating": 0, "PlayedPercentage": None}
        try:
            _post(f"{server}/UserItems/{it['Id']}/UserData", headers, payload)
        except Exception:
            _post(f"{server}/UserItems/{it['Id']}/UserData?userId={user_id}", headers, {"Rating": 0})

        progress_line(label, idx, total)

    done_line()
    return total

# ---------- menu ----------
def menu() -> str:
    print("\n=== Jellyfin / Emby Cleanup ===")
    print("1. Show Jellyfin History")
    print("2. Show Jellyfin Ratings")
    print("3. Show Emby History")
    print("4. Remove Jellyfin History")
    print("5. Remove Jellyfin Ratings")
    print("6. Remove Emby History")
    print("7. Clean Jellyfin (History and Ratings)")
    print("0. Exit")
    return input("Select: ").strip()


def main() -> None:
    cfg = load_cfg()

    # detect providers
    jf = em = None
    try:
        jf_server, jf_token, jf_user = get_provider(cfg, "jellyfin")
        jf = (jf_server, jf_token, jf_user)
    except Exception:
        pass

    try:
        emby_server, emby_token, emby_user = get_provider(cfg, "emby")
        em = (emby_server, emby_token, emby_user)
    except Exception:
        pass

    if not jf and not em:
        print("[!] No Jellyfin or Emby config found in /config/config.json.")
        return

    # ---- stats overview at startup ----
    print("\nFetching history stats...")
    if jf:
        items = collect_played_items(jf[0], jf_headers(jf[1]), jf[2], JF_HISTORY_TYPES, "Jellyfin stats")
        print_stats_block("Jellyfin History Stats", history_stats(items))
    if em:
        items = collect_played_items(em[0], emby_headers(em[1]), em[2], EMBY_HISTORY_TYPES, "Emby stats")
        print_stats_block("Emby History Stats", history_stats(items))

    while True:
        choice = menu()

        try:
            if choice == "0":
                print("Bye.")
                return

            elif choice == "1":
                if not jf:
                    print("[!] Jellyfin not configured.")
                    continue
                items = collect_played_items(jf[0], jf_headers(jf[1]), jf[2], JF_HISTORY_TYPES, "Jellyfin history")
                print(f"\nJellyfin played items: {len(items)}")
                paged_display(items, [("Type", "Type"), ("Name", "Name"), ("Id", "Id")])

            elif choice == "2":
                if not jf:
                    print("[!] Jellyfin not configured.")
                    continue
                items = collect_rated_items(jf[0], jf_headers(jf[1]), jf[2], "Jellyfin ratings scan")
                print(f"\nJellyfin rated items: {len(items)}")
                paged_display(items, [("Type", "Type"), ("Name", "Name"), ("Rating", "Rating"), ("Id", "Id")])

            elif choice == "3":
                if not em:
                    print("[!] Emby not configured.")
                    continue
                items = collect_played_items(em[0], emby_headers(em[1]), em[2], EMBY_HISTORY_TYPES, "Emby history")
                print(f"\nEmby played items: {len(items)}")
                paged_display(items, [("Type", "Type"), ("Name", "Name"), ("Id", "Id")])

            elif choice == "4":
                if not jf:
                    print("[!] Jellyfin not configured.")
                    continue
                if not confirm_danger("This will clear ALL Jellyfin watched status for this user (movies + episodes)."):
                    print("Aborted.")
                    continue
                total = clear_history(jf[0], jf_headers(jf[1]), jf[2], JF_HISTORY_TYPES, "Jellyfin history")
                print(f"Done. Cleared {total} played items.")

            elif choice == "5":
                if not jf:
                    print("[!] Jellyfin not configured.")
                    continue
                items = collect_rated_items(jf[0], jf_headers(jf[1]), jf[2], "Jellyfin ratings scan")
                print(f"\nFound {len(items)} rated items.")
                if items:
                    print_table(items[:20], [("Type", "Type"), ("Name", "Name"), ("Rating", "Rating"), ("Id", "Id")])
                if not items or not confirm_danger("This will clear ALL Jellyfin personal ratings for this user."):
                    print("Aborted.")
                    continue
                cleared = clear_ratings(jf[0], jf_headers(jf[1]), jf[2], items, "Jellyfin ratings clearing")
                print(f"Done. Cleared ratings on {cleared} items.")

            elif choice == "6":
                if not em:
                    print("[!] Emby not configured.")
                    continue
                if not confirm_danger("This will clear ALL Emby watched status for this user (movies + episodes)."):
                    print("Aborted.")
                    continue
                total = clear_history(em[0], emby_headers(em[1]), em[2], EMBY_HISTORY_TYPES, "Emby history")
                print(f"Done. Cleared {total} played items.")

            elif choice == "7":
                if not jf:
                    print("[!] Jellyfin not configured.")
                    continue
                if not confirm_danger("This will clear BOTH Jellyfin history and ratings for this user."):
                    print("Aborted.")
                    continue
                total_hist = clear_history(jf[0], jf_headers(jf[1]), jf[2], JF_HISTORY_TYPES, "Jellyfin history")
                rated_items = collect_rated_items(jf[0], jf_headers(jf[1]), jf[2], "Jellyfin ratings scan")
                total_rate = clear_ratings(jf[0], jf_headers(jf[1]), jf[2], rated_items, "Jellyfin ratings clearing")
                print(f"Done. Cleared history={total_hist} items, ratings={total_rate} items.")

            else:
                print("Unknown option.")

        except requests.HTTPError as e:
            print(f"[!] HTTP error: {e.response.status_code} {e.response.text[:300]}")
        except Exception as e:
            print(f"[!] Error: {e}")

if __name__ == "__main__":
    main()
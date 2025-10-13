# /providers/sync/jellyfin/_history.py
from __future__ import annotations
import os, json
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timezone

from ._common import (
    normalize as jelly_normalize,
    resolve_item_id,
    chunked,
    sleep_ms,
    jf_scope_history,
)

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

UNRESOLVED_PATH = "/config/.cw_state/jellyfin_history.unresolved.json"
SHADOW_PATH     = "/config/.cw_state/jellyfin_history.shadow.json"
BLACKBOX_PATH   = "/config/.cw_state/jellyfin_history.jellyfin-plex.blackbox.json"

def _log(msg: str):
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_JELLYFIN_DEBUG"):
        print(f"[JELLYFIN:history] {msg}")

# --- unresolved ---------------------------------------------------------------

def _unres_load() -> Dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _unres_save(obj: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        with open(UNRESOLVED_PATH, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass

def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(item)
    data = _unres_load(); ent = data.get(key) or {"feature": "history", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _unres_save(data)

def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _unres_load(); changed = False
    for k in list(keys or []):
        if k in data:
            data.pop(k, None); changed = True
    if changed: _unres_save(data)

# --- shadow (simple write echo) ----------------------------------------------

def _shadow_load() -> Dict[str, int]:
    try:
        with open(SHADOW_PATH, "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            return {str(k): int(v) for k, v in raw.items()}
    except Exception:
        return {}

def _shadow_save(d: Mapping[str, int]) -> None:
    try:
        os.makedirs(os.path.dirname(SHADOW_PATH), exist_ok=True)
        with open(SHADOW_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass

# --- blackbox (planner presence hints) ---------------------------------------

def _bb_load() -> Dict[str, Any]:
    try:
        with open(BLACKBOX_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}

def _bb_save(d: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(BLACKBOX_PATH), exist_ok=True)
        with open(BLACKBOX_PATH, "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass

# --- cfg helpers --------------------------------------------------------------

def _history_limit(adapter) -> int:
    v = getattr(getattr(adapter, "cfg", None), "history_query_limit", None)
    if v is None: v = getattr(getattr(adapter, "cfg", None), "watchlist_query_limit", 1000)
    try: return max(1, int(v))
    except Exception: return 1000

def _history_delay_ms(adapter) -> int:
    v = getattr(getattr(adapter, "cfg", None), "history_write_delay_ms", None)
    if v is None: v = getattr(getattr(adapter, "cfg", None), "watchlist_write_delay_ms", 0)
    try: return max(0, int(v))
    except Exception: return 0

# --- time utils ---------------------------------------------------------------

def _parse_iso_to_epoch(s: Optional[str]) -> Optional[int]:
    if not s: return None
    try:
        t = s.strip()
        if t.endswith("Z"):
            dt = datetime.fromisoformat(t.replace("Z", "+00:00"))
        else:
            dt = datetime.fromisoformat(t)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return None

def _epoch_to_iso_z(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _now_iso_z() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

# --- series ids helper --------------------------------------------------------

def _series_ids_for(http, series_id: Optional[str]) -> Dict[str, str]:
    sid = (str(series_id or "").strip()) or ""
    if not sid: return {}
    try:
        r = http.get(f"/Items/{sid}", params={"fields": "ProviderIds,ProductionYear"})
        if getattr(r, "status_code", 0) != 200: return {}
        body = r.json() or {}
        pids = (body.get("ProviderIds") or {}) if isinstance(body, Mapping) else {}
        out: Dict[str, str] = {}
        for k, v in (pids.items() if isinstance(pids, Mapping) else []):
            kl = str(k).lower(); sv = str(v).strip()
            if not sv: continue
            if kl == "imdb":
                out["imdb"] = sv if sv.startswith("tt") else f"tt{sv}"
            elif kl == "tmdb":
                try: out["tmdb"] = str(int(sv))
                except Exception: pass
            elif kl == "tvdb":
                try: out["tvdb"] = str(int(sv))
                except Exception: pass
        return out
    except Exception:
        return {}

# --- low-level Jellyfin writes ------------------------------------------------

def _mark_played(http, uid: str, item_id: str, *, date_played_iso: Optional[str]) -> bool:
    try:
        params = {"datePlayed": date_played_iso} if date_played_iso else None
        r = http.post(f"/Users/{uid}/PlayedItems/{item_id}", params=params)
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False

def _unmark_played(http, uid: str, item_id: str) -> bool:
    try:
        r = http.delete(f"/Users/{uid}/PlayedItems/{item_id}")
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception:
        return False

def _dst_user_state(http, uid: str, iid: str) -> Tuple[bool, int]:
    try:
        r = http.get(f"/Users/{uid}/Items/{iid}", params={"fields": "UserData"})
        if getattr(r, "status_code", 0) != 200: return False, 0
        data = r.json() or {}
        ud = data.get("UserData") or {}
        played = bool(ud.get("Played") or ud.get("IsPlayed"))
        ts = 0
        for k in ("LastPlayedDate", "DateLastPlayed", "LastPlayed"):
            v = ud.get(k) or data.get(k)
            if v:
                ts = _parse_iso_to_epoch(v) or 0
                if ts: break
        return played, ts
    except Exception:
        return False, 0

# --- event index (watched_at) + presence merge --------------------------------

def build_index(adapter, since: Optional[Any] = None, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None

    http = adapter.client; uid = adapter.cfg.user_id
    page_size = _history_limit(adapter)

    since_epoch = 0
    if isinstance(since, (int, float)):
        since_epoch = int(since)
    elif isinstance(since, str):
        since_epoch = int(_parse_iso_to_epoch(since) or 0)

    start = 0
    events: List[Tuple[int, Dict[str, Any]]] = []

    while True:
        params: Dict[str, Any] = {
            "includeItemTypes": "Movie,Episode",
            "recursive": True,
            "enableUserData": True,
            "fields": "ProviderIds,ProductionYear,UserData,Type,IndexNumber,ParentIndexNumber,SeriesName,SeriesId,Name,ParentId",
            "filters": "IsPlayed",
            "sortBy": "DateLastPlayed",
            "sortOrder": "Descending",
            "startIndex": start,
            "limit": page_size,
            "enableTotalRecordCount": True,
            "userId": uid,
        }
        params.update(jf_scope_history(adapter.cfg))

        r = http.get(f"/Users/{uid}/Items", params=params)
        body = r.json() or {}
        rows = body.get("Items") or []
        if not rows: break

        for row in rows:
            ud = row.get("UserData") or {}
            lp = ud.get("LastPlayedDate") or row.get("DateLastPlayed") or None
            ts = _parse_iso_to_epoch(lp) or 0
            if ts <= since_epoch:
                rows = []
                break

            m = jelly_normalize(row)
            watched_at = _epoch_to_iso_z(ts)
            typ = (row.get("Type") or "").strip()

            if typ == "Movie":
                event: Dict[str, Any] = {
                    "type": "movie",
                    "ids": dict(m.get("ids") or {}),
                    "title": m.get("title"),
                    "year": m.get("year"),
                    "watched_at": watched_at,
                    "watched": True,
                }
            elif typ == "Episode":
                show_ids = _series_ids_for(http, row.get("SeriesId"))
                event = {
                    "type": "episode",
                    "show_ids": show_ids,
                    "season": m.get("season"),
                    "episode": m.get("episode"),
                    "series_title": m.get("series_title") or row.get("SeriesName"),
                    "watched_at": watched_at,
                    "watched": True,
                }
            else:
                continue

            ev_key = f"{canonical_key(m)}@{ts}"
            events.append((ts, {"key": ev_key, "base": m}, event))

        start += len(body.get("Items") or [])
        if isinstance(limit, int) and limit > 0 and len(events) >= int(limit): break
        if not rows: break

    events.sort(key=lambda x: x[0], reverse=True)
    if isinstance(limit, int) and limit > 0:
        events = events[: int(limit)]

    total = len(events)
    out: Dict[str, Dict[str, Any]] = {}
    if prog:
        try: prog.tick(0, total=total, force=True)
        except Exception: pass

    done = 0
    for _, meta, event in events:
        out[meta["key"]] = event
        done += 1
        if prog:
            try: prog.tick(done, total=total)
            except Exception: pass

    shadow = _shadow_load()
    if shadow:
        added = 0
        for k in list(shadow.keys()):
            out.setdefault(k, {"watched": True})
            added += 1
        _log(f"shadow merged: +{added}")

    bb = _bb_load()
    if bb:
        added = 0
        for k, meta in bb.items():
            if isinstance(meta, dict) and str(meta.get("reason", "")).startswith("presence:"):
                out.setdefault(k, {"watched": True})
                added += 1
        if added:
            _log(f"blackbox presence merged: +{added}")

    _log(f"index size: {len(out)} (events+presence)")
    return out

# --- writes (event → present in Jellyfin) ------------------------------------

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client; uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    wants: Dict[str, Mapping[str, Any]] = {}
    for it in items or []:
        m = id_minimal(it)
        wants[canonical_key(m)] = m

    mids: List[Tuple[str, str]] = []
    unresolved: List[Dict[str, Any]] = []
    for k, m in wants.items():
        iid = resolve_item_id(adapter, m)
        if iid:
            mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"})
            _freeze(m, reason="resolve_failed")

    shadow = _shadow_load()
    bb = _bb_load()
    ok = 0

    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            src_ts = _parse_iso_to_epoch(wants[k].get("watched_at")) or 0
            src_iso = _epoch_to_iso_z(src_ts) if src_ts else None

            played, dst_ts = _dst_user_state(http, uid, iid)

            if played and dst_ts and src_ts and dst_ts >= src_ts:
                bb[k] = {"reason": "presence:existing_newer", "since": _now_iso_z()}
                continue

            if played and not dst_ts and not src_ts:
                bb[k] = {"reason": "presence:existing_untimed", "since": _now_iso_z()}
                continue

            if _mark_played(http, uid, iid, date_played_iso=src_iso):
                ok += 1
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
            else:
                unresolved.append({"item": wants[k], "hint": "mark_played_failed"})
                _freeze(wants[k], reason="write_failed")
            sleep_ms(delay)

    _shadow_save(shadow); _bb_save(bb)
    if ok: _thaw_if_present([k for k, _ in mids])
    _log(f"add done: +{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client; uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    wants: Dict[str, Mapping[str, Any]] = {}
    for it in items or []:
        m = id_minimal(it)
        wants[canonical_key(m)] = m

    mids: List[Tuple[str, str]] = []
    unresolved: List[Dict[str, Any]] = []
    for k, m in wants.items():
        iid = resolve_item_id(adapter, m)
        if iid:
            mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"})
            _freeze(m, reason="resolve_failed")

    shadow = _shadow_load()
    ok = 0
    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            cur = int(shadow.get(k, 0))
            nxt = max(0, cur - 1)
            shadow[k] = nxt
            if nxt == 0:
                if _unmark_played(http, uid, iid):
                    ok += 1
                else:
                    unresolved.append({"item": wants[k], "hint": "unmark_played_failed"})
                    _freeze(wants[k], reason="write_failed")
            sleep_ms(delay)

    shadow = {k: v for k, v in shadow.items() if v > 0}
    _shadow_save(shadow)
    if ok: _thaw_if_present([k for k, _ in mids])
    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved

# /providers/sync/emby/_history.py
from __future__ import annotations
import os, json, time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple
from datetime import datetime, timezone

from ._common import (
    normalize as emby_normalize,
    resolve_item_id,
    chunked,
    emby_scope_history,
)

try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

UNRESOLVED_PATH = "/config/.cw_state/emby_history.unresolved.json"
SHADOW_PATH     = "/config/.cw_state/emby_history.shadow.json"
BLACKBOX_PATH   = "/config/.cw_state/emby_history.emby.blackbox.json"

def sleep_ms(ms: int) -> None:
    try:
        time.sleep(max(0, int(ms)) / 1000.0)
    except Exception:
        pass

def _log(msg: str) -> None:
    if os.environ.get("CW_DEBUG") or os.environ.get("CW_EMBY_DEBUG"):
        print(f"[EMBY:history] {msg}")

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

def _bb_paths() -> List[str]:
    base = BLACKBOX_PATH
    paths = [base]
    try:
        d = os.path.dirname(base) or "."
        for fn in os.listdir(d):
            if fn.startswith("emby_history.emby") and fn.endswith(".blackbox.json"):
                p = os.path.join(d, fn)
                if p not in paths:
                    paths.append(p)
    except Exception:
        pass
    return paths

def _bb_load() -> Dict[str, Any]:
    merged: Dict[str, Any] = {}
    for p in _bb_paths():
        try:
            with open(p, "r", encoding="utf-8") as f:
                obj = json.load(f) or {}
                if isinstance(obj, dict):
                    merged.update(obj)
        except Exception:
            pass
    return merged

def _bb_save(d: Mapping[str, Any]) -> None:
    os.makedirs(os.path.dirname(BLACKBOX_PATH), exist_ok=True)
    for p in _bb_paths():
        try:
            with open(p, "w", encoding="utf-8") as f:
                json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
        except Exception:
            pass

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

def _epoch_to_emby_dateparam(ts: int) -> str:
    return datetime.fromtimestamp(int(ts), tz=timezone.utc).strftime("%Y%m%d%H%M%S")

def _now_iso_z() -> str:
    return datetime.now(tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

def _series_ids_for(http, series_id: Optional[str]) -> Dict[str, str]:
    sid = (str(series_id or "").strip()) or ""
    if not sid: return {}
    try:
        r = http.get(f"/Items/{sid}", params={"Fields": "ProviderIds,ProductionYear"})
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

def _resp_snip(r) -> str:
    try:
        j = r.json()
        s = json.dumps(j, ensure_ascii=False)
        return (s[:180] + "…") if len(s) > 180 else s
    except Exception:
        try:
            t = r.text() if callable(getattr(r, "text", None)) else getattr(r, "text", "")
            s = str(t or "")
            return (s[:180] + "…") if len(s) > 180 else s
        except Exception:
            return "<no-body>"

def _write_userdata(http, uid: str, item_id: str, *, date_iso: Optional[str]) -> bool:
    payload: Dict[str, Any] = {"Played": True, "PlayCount": 1}
    if date_iso:
        payload["LastPlayedDate"] = date_iso
        payload["DatePlayed"] = date_iso
    r = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json=payload)
    ok = getattr(r, "status_code", 0) in (200, 204)
    if not ok:
        _log(f"userData write failed id={item_id} status={getattr(r,'status_code',None)} body={_resp_snip(r)}")
    return ok

def _mark_played(http, uid: str, item_id: str, *, date_played_iso: Optional[str]) -> bool:
    try:
        date_param = None
        if date_played_iso:
            ts = _parse_iso_to_epoch(date_played_iso)
            if ts is not None:
                date_param = _epoch_to_emby_dateparam(ts)

        r = http.post(
            f"/Users/{uid}/PlayedItems/{item_id}",
            params={"DatePlayed": date_param} if date_param else None
        )
        if getattr(r, "status_code", 0) in (200, 204):
            return True
        _log(f"mark_played[A] failed id={item_id} status={getattr(r,'status_code',None)} body={_resp_snip(r)}")

        r2 = http.post(
            f"/Users/{uid}/PlayedItems/{item_id}",
            json={"DatePlayed": date_param} if date_param else {}
        )
        if getattr(r2, "status_code", 0) in (200, 204):
            return True
        _log(f"mark_played[B] failed id={item_id} status={getattr(r2,'status_code',None)} body={_resp_snip(r2)}")

        if _write_userdata(http, uid, item_id, date_iso=date_played_iso):
            return True

        return False
    except Exception as e:
        _log(f"mark_played exception id={item_id} err={e}")
        return False

def _unmark_played(http, uid: str, item_id: str) -> bool:
    try:
        r = http.delete(f"/Users/{uid}/PlayedItems/{item_id}")
        ok = getattr(r, "status_code", 0) in (200, 204)
        if not ok:
            _log(f"unmark_played failed id={item_id} status={getattr(r,'status_code',None)} body={_resp_snip(r)}")
        return ok
    except Exception as e:
        _log(f"unmark_played exception id={item_id} err={e}")
        return False

def _dst_user_state(http, uid: str, iid: str) -> Tuple[bool, int]:
    try:
        r = http.get(f"/Users/{uid}/Items/{iid}", params={"Fields": "UserData"})
        if getattr(r, "status_code", 0) != 200:
            return False, 0
        data = r.json() or {}
        ud = data.get("UserData") or {}
        played = bool(ud.get("Played") or ud.get("IsPlayed") or (ud.get("PlayCount") or 0) > 0)
        ts = _parse_iso_to_epoch(
            ud.get("DatePlayed") or ud.get("LastPlayedDate") or ud.get("LastPlayedAt")
        ) or 0
        return played, ts
    except Exception:
        return False, 0

def build_index(adapter, since: Optional[Any] = None, limit: Optional[int] = None) -> Dict[str, Dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog = prog_mk("history") if callable(prog_mk) else None

    http = adapter.client
    uid = adapter.cfg.user_id
    page_size = _history_limit(adapter)

    since_epoch = 0
    if isinstance(since, (int, float)):
        since_epoch = int(since)
    elif isinstance(since, str):
        since_epoch = int(_parse_iso_to_epoch(since) or 0)

    def _played_ts(row: Mapping[str, Any]) -> int:
        ud = (row.get("UserData") or {}) if isinstance(row, Mapping) else {}
        for v in (
            ud.get("DatePlayed"),
            ud.get("LastPlayedDate"),
            ud.get("LastPlayedAt"),
            row.get("DatePlayed"),
            row.get("DateLastPlayed"),
            row.get("LastPlayedDate"),
        ):
            ts = _parse_iso_to_epoch(v)
            if ts:
                return ts
        return 0

    events: List[Tuple[int, Dict[str, Any], Dict[str, Any]]] = []
    presence_keys: set[str] = set()
    start = 0

    while True:
        params: Dict[str, Any] = {
            "IncludeItemTypes": "Movie,Episode",
            "Recursive": True,
            "EnableUserData": True,
            "Fields": "ProviderIds,ProductionYear,UserData,Type,IndexNumber,ParentIndexNumber,SeriesName,SeriesId,Name,ParentId,DatePlayed,Path",
            "Filters": "IsPlayed",
            "SortBy": "DatePlayed",
            "SortOrder": "Descending",
            "StartIndex": start,
            "Limit": page_size,
            "EnableTotalRecordCount": True,
            "UserId": uid,
        }
        params.update(emby_scope_history(adapter.cfg))

        r = http.get(f"/Users/{uid}/Items", params=params)
        if getattr(r, "status_code", 0) != 200:
            _log(f"history: query failed status={getattr(r,'status_code',None)} body={_resp_snip(r)}")
            break

        try:
            body = r.json() or {}
            rows = body.get("Items") or []
        except Exception as e:
            _log(f"history: json parse failed; treating page as empty; err={e}")
            rows = []

        if not rows:
            break

        stop = False
        for row in rows:
            ts = _played_ts(row)
            if ts and since_epoch and ts <= since_epoch:
                stop = True
                break
            if not ts:
                ud = row.get("UserData") or {}
                if ud.get("Played") or ud.get("IsPlayed") or (ud.get("PlayCount") or 0) > 0:
                    try:
                        m0 = emby_normalize(row)
                        presence_keys.add(canonical_key(m0))
                    except Exception:
                        pass
                continue

            m = emby_normalize(row)
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

        start += len(rows)
        if stop or len(rows) < page_size:
            break

    events.sort(key=lambda x: x[0], reverse=True)
    if isinstance(limit, int) and limit > 0:
        events = events[: int(limit)]

    total = len(events)
    out: Dict[str, Dict[str, Any]] = {}
    if prog:
        try:
            prog.tick(0, total=total, force=True)
        except Exception:
            pass

    done = 0
    for _, meta, event in events:
        out[meta["key"]] = event
        done += 1
        if prog:
            try:
                prog.tick(done, total=total)
            except Exception:
                pass

    shadow = _shadow_load()
    if shadow:
        added = 0
        for k in list(shadow.keys()):
            if k not in out:
                out.setdefault(k, {"watched": True})
                added += 1
        if added:
            _log(f"shadow merged: +{added}")

    bb = _bb_load()
    if bb:
        added = 0
        for k, meta in bb.items():
            if isinstance(meta, dict) and str(meta.get("reason", "")).startswith("presence:"):
                if k not in out:
                    out.setdefault(k, {"watched": True})
                    added += 1
        if added:
            _log(f"blackbox presence merged: +{added}")

    if presence_keys:
        added = 0
        for k in presence_keys:
            if k not in out:
                out.setdefault(k, {"watched": True})
                added += 1
        if added:
            _log(f"presence merged: +{added}")

    _log(f"index size: {len(out)} (events+presence)")
    return out

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client; uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    do_force = bool(getattr(getattr(adapter, "cfg", None), "history_force_overwrite", False))
    do_back  = bool(getattr(getattr(adapter, "cfg", None), "history_backdate", False))
    tol      = int(getattr(getattr(adapter, "cfg", None), "history_backdate_tolerance_s", 300))

    try:
        from ._common import provider_index
        provider_index(adapter)
    except Exception:
        pass

    wants: Dict[str, Mapping[str, Any]] = {}
    for it in (items or []):
        m = id_minimal(it); wants[canonical_key(m)] = m

    mids: List[Tuple[str, str]] = []
    unresolved: List[Dict[str, Any]] = []
    for k, m in wants.items():
        try:
            iid = resolve_item_id(adapter, m)
        except Exception as e:
            _log(f"resolve exception: {e}"); iid = None
        if iid: mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"}); _freeze(m, reason="resolve_failed")

    shadow = _shadow_load(); bb = _bb_load()
    ok = 0

    stats = {
        "wrote": 0, "forced": 0, "backdated": 0,
        "skip_newer": 0, "skip_played_untimed": 0,
        "skip_missing_date": 0, "fail_mark": 0,
    }

    total = len(mids)
    if total: _log(f"apply:add:start dst=EMBY feature=history count={total}")

    processed = 0
    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            it = wants[k]
            src_ts = _parse_iso_to_epoch(it.get("watched_at")) or 0
            if not src_ts:
                unresolved.append({"item": id_minimal(it), "hint": "missing_watched_at"})
                _freeze(it, reason="missing_watched_at"); stats["skip_missing_date"] += 1; continue

            src_iso = _epoch_to_iso_z(src_ts)

            if do_force:
                _unmark_played(http, uid, iid)
                if _mark_played(http, uid, iid, date_played_iso=src_iso):
                    ok += 1; shadow[k] = int(shadow.get(k, 0)) + 1
                    bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                    stats["wrote"] += 1; stats["forced"] += 1
                else:
                    unresolved.append({"item": id_minimal(it), "hint": "mark_played_failed"})
                    _freeze(it, reason="write_failed"); stats["fail_mark"] += 1
                processed += 1
                if (processed % 25) == 0: _log(f"apply:add:progress done={processed}/{total} ok={ok} unresolved={len(unresolved)}")
                sleep_ms(delay); continue

            played, dst_ts = _dst_user_state(http, uid, iid)

            if played and dst_ts and dst_ts >= (src_ts - tol):
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:existing_newer", "since": _now_iso_z()}
                stats["skip_newer"] += 1
                continue

            if played and not dst_ts:
                if _mark_played(http, uid, iid, date_played_iso=src_iso):
                    ok += 1; shadow[k] = int(shadow.get(k, 0)) + 1
                    bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                    stats["wrote"] += 1; stats["backdated"] += 1
                    sleep_ms(delay); continue
                if do_back:
                    _unmark_played(http, uid, iid)
                    if _mark_played(http, uid, iid, date_played_iso=src_iso):
                        ok += 1; shadow[k] = int(shadow.get(k, 0)) + 1
                        bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                        stats["wrote"] += 1; stats["backdated"] += 1
                        sleep_ms(delay); continue
                shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:existing_untimed", "since": _now_iso_z()}
                stats["skip_played_untimed"] += 1
                continue

            if do_back and (not dst_ts or dst_ts >= (src_ts + tol)):
                _unmark_played(http, uid, iid)
                if _mark_played(http, uid, iid, date_played_iso=src_iso):
                    ok += 1; shadow[k] = int(shadow.get(k, 0)) + 1
                    bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                    stats["wrote"] += 1; stats["backdated"] += 1
                else:
                    unresolved.append({"item": id_minimal(it), "hint": "mark_played_failed"})
                    _freeze(it, reason="write_failed"); stats["fail_mark"] += 1
                processed += 1
                if (processed % 25) == 0: _log(f"apply:add:progress done={processed}/{total} ok={ok} unresolved={len(unresolved)}")
                sleep_ms(delay); continue

            if _mark_played(http, uid, iid, date_played_iso=src_iso):
                ok += 1; shadow[k] = int(shadow.get(k, 0)) + 1
                bb[k] = {"reason": "presence:shadow", "since": _now_iso_z()}
                stats["wrote"] += 1
            else:
                unresolved.append({"item": id_minimal(it), "hint": "mark_played_failed"})
                _freeze(it, reason="write_failed"); stats["fail_mark"] += 1

            processed += 1
            if (processed % 25) == 0: _log(f"apply:add:progress done={processed}/{total} ok={ok} unresolved={len(unresolved)}")
            sleep_ms(delay)

    _shadow_save(shadow); _bb_save(bb)
    if ok: _thaw_if_present([k for k, _ in mids])

    _log(
        "add done: "
        f"+{ok} / unresolved {len(unresolved)} | "
        f"wrote={stats['wrote']} forced={stats['forced']} backdated={stats['backdated']} "
        f"skip_newer={stats['skip_newer']} skip_played_untimed={stats['skip_played_untimed']} "
        f"skip_missing_date={stats['skip_missing_date']} fail_mark={stats['fail_mark']}"
    )
    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client; uid = adapter.cfg.user_id
    qlim = int(_history_limit(adapter) or 25)
    delay = _history_delay_ms(adapter)

    wants: Dict[str, Mapping[str, Any]] = {}
    for it in items or []:
        m = id_minimal(it); wants[canonical_key(m)] = m

    mids: List[Tuple[str, str]] = []
    unresolved: List[Dict[str, Any]] = []
    for k, m in wants.items():
        iid = resolve_item_id(adapter, m)
        if iid: mids.append((k, iid))
        else:
            unresolved.append({"item": id_minimal(m), "hint": "resolve_failed"})
            _freeze(m, reason="resolve_failed")

    shadow = _shadow_load()
    ok = 0
    for chunk in chunked(mids, qlim):
        for k, iid in chunk:
            cur = int(shadow.get(k, 0)); nxt = max(0, cur - 1)
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

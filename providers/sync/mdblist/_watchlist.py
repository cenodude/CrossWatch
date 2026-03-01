# /providers/sync/mdblist/_watchlist.py
# MDBList watchlist sync module (activity-gated)
# Copyright (c) 2025-2026 CrossWatch / Cenodude

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from .._log import log as cw_log
from .._mod_common import request_with_retries
from ._common import (
    _pair_scope,
    _is_capture_mode,
    as_epoch,
    cfg_bool,
    cfg_int,
    cfg_section,
    get_watermark,
    iso_ok,
    iso_z,
    now_iso,
    read_json,
    save_watermark,
    state_file,
    write_json,
)

BASE = "https://api.mdblist.com"
URL_LIST = f"{BASE}/watchlist/items"
URL_MODIFY = f"{BASE}/watchlist/items/{{action}}"
URL_LAST_ACTIVITIES = f"{BASE}/sync/last_activities"


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "watchlist", "debug", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("MDBLIST", "watchlist", "warn", msg, **fields)


def _log(msg: str, **fields: Any) -> None:
    # Back-compat alias; treat as debug.
    _dbg(msg, **fields)


def _as_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value
    try:
        s = str(value).strip()
        if not s:
            return None
        return int(s)
    except Exception:
        return None


def _as_str(value: Any) -> str | None:
    if value is None:
        return None
    s = str(value).strip()
    return s or None


def _shadow_path() -> Path:
    return state_file("mdblist_watchlist.shadow.json")


def _unresolved_path() -> Path:
    return state_file("mdblist_watchlist.unresolved.json")


_cfg = cfg_section
_cfg_int = cfg_int
_cfg_bool = cfg_bool
_iso_ok = iso_ok
_iso_z = iso_z
_as_epoch = as_epoch
_now_iso = now_iso


def _shadow_load() -> dict[str, Any]:
    p = _shadow_path()
    doc = read_json(p)
    if not isinstance(doc, dict):
        return {"ts": 0, "items": {}}
    doc.setdefault("ts", 0)
    items = doc.get("items")
    if not isinstance(items, dict):
        doc["items"] = {}
        return doc

    # Migrate legacy cache: /watchlist/items uses "id" as TMDB id; older code stored it as ids.mdblist.
    migrated: dict[str, Any] = {}
    changed = False
    for k, v in items.items():
        if not isinstance(v, Mapping):
            changed = True
            continue
        it = dict(v)
        ids_src = it.get("ids")
        ids = dict(ids_src) if isinstance(ids_src, Mapping) else {}
        if not ids.get("tmdb"):
            mdbl = ids.get("mdblist")
            tmdb_i = _as_int(mdbl)
            if tmdb_i is not None:
                ids["tmdb"] = str(tmdb_i)
                ids.pop("mdblist", None)
                it["ids"] = ids
                changed = True
        nk = _key_of(it)
        if nk != str(k):
            changed = True
        migrated[nk] = it

    if changed:
        doc["items"] = migrated
        write_json(p, doc)
    return doc


def _shadow_save(items: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    p = _shadow_path()
    try:
        tmp = p.with_name(f"{p.name}.tmp")
        tmp.write_text(json.dumps({"ts": int(time.time()), "items": dict(items)}, ensure_ascii=False), "utf-8")
        os.replace(tmp, p)
    except Exception:
        pass


def _shadow_bust() -> None:
    p = _shadow_path()
    try:
        if p.exists():
            p.unlink()
            _log("shadow.bust - file removed")
    except Exception:
        pass


def _fetch_last_activities(adapter: Any, *, apikey: str, timeout: float, retries: int) -> dict[str, Any] | None:
    try:
        client = getattr(adapter, "client", None)
        if client and hasattr(client, "last_activities"):
            data = client.last_activities()
            if isinstance(data, Mapping) and "error" not in data and "status" not in data:
                return dict(data)
    except Exception:
        pass
    try:
        r = request_with_retries(
            adapter.client.session,
            "GET",
            URL_LAST_ACTIVITIES,
            params={"apikey": apikey},
            timeout=timeout,
            max_retries=retries,
        )
        if 200 <= r.status_code < 300:
            data = r.json() if (r.text or "").strip() else {}
            return dict(data) if isinstance(data, Mapping) else None
    except Exception:
        return None
    return None


def _load_unresolved() -> dict[str, Any]:
    p = _unresolved_path()
    doc = read_json(p)
    return doc if isinstance(doc, dict) else {}


def _save_unresolved(data: Mapping[str, Any]) -> None:
    if _is_capture_mode() or _pair_scope() is None:
        return
    p = _unresolved_path()
    try:
        p.parent.mkdir(parents=True, exist_ok=True)
        tmp = p.with_name(f"{p.name}.tmp")
        tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, sort_keys=True), "utf-8")
        os.replace(tmp, p)
    except Exception as e:
        _warn("unresolved_save_failed", error=str(e))


def _key_of(obj: Mapping[str, Any]) -> str:
    ids_src = obj.get("ids")
    ids = dict(ids_src) if isinstance(ids_src, Mapping) else dict(obj)
    tmdb_i = _as_int(ids.get("tmdb") or ids.get("tmdb_id") or ids.get("id"))
    if tmdb_i is not None:
        return f"tmdb:{tmdb_i}"
    imdb = _as_str(ids.get("imdb") or ids.get("imdb_id"))
    if imdb:
        return f"imdb:{imdb}"
    tvdb_i = _as_int(ids.get("tvdb") or ids.get("tvdb_id"))
    if tvdb_i is not None:
        return f"tvdb:{tvdb_i}"
    mdbl = _as_str(ids.get("mdblist"))
    if mdbl:
        return f"mdblist:{mdbl}"
    title = _as_str(obj.get("title"))
    year_i = _as_int(obj.get("year"))
    if title and year_i is not None:
        return f"title:{title}|year:{year_i}"
    return f"obj:{hash(json.dumps(obj, sort_keys=True)) & 0xffffffff}"


def _freeze_item(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    details: Mapping[str, Any] | None = None,
) -> None:
    minimal = id_minimal(item)
    key = _key_of(minimal)
    data = _load_unresolved()
    entry = data.get(key) or {"feature": "watchlist", "action": action, "first_seen": _now_iso(), "attempts": 0}
    entry.update({"item": minimal, "last_attempt": _now_iso()})
    rset = set(entry.get("reasons", [])) | set(reasons or [])
    entry["reasons"] = sorted(rset)
    if details:
        old_details = entry.get("details") or {}
        entry["details"] = {**old_details, **details}
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)


def _unfreeze_keys_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for key in list(keys or []):
        if key in data:
            del data[key]
            changed = True
    if changed:
        _save_unresolved(data)


def _ids_for_mdblist(item: Mapping[str, Any]) -> dict[str, Any]:
    ids_src = item.get("ids")
    ids_raw = dict(ids_src) if isinstance(ids_src, Mapping) else {}
    if not ids_raw:
        ids_raw = {"imdb": item.get("imdb") or item.get("imdb_id"), "tmdb": item.get("tmdb") or item.get("tmdb_id"), "tvdb": item.get("tvdb") or item.get("tvdb_id")}
    out: dict[str, Any] = {}
    imdb_val = _as_str(ids_raw.get("imdb"))
    if imdb_val:
        out["imdb"] = imdb_val
    tmdb_i = _as_int(ids_raw.get("tmdb"))
    if tmdb_i is not None:
        out["tmdb"] = tmdb_i
    tvdb_i = _as_int(ids_raw.get("tvdb"))
    if tvdb_i is not None:
        out["tvdb"] = tvdb_i
    return out


def _pick_kind_from_row(row: Mapping[str, Any]) -> str:
    t = str(row.get("mediatype") or row.get("type") or "").strip().lower()
    if t in ("show", "tv", "series", "shows"):
        return "show"
    if row.get("tvdb_id") not in (None, ""):
        return "show"
    if row.get("first_air_date") or row.get("first_air_year"):
        return "show"
    if row.get("release_date") or row.get("release_year"):
        return "movie"
    ids = row.get("ids")
    if isinstance(ids, Mapping):
        if ids.get("tvdb") not in (None, ""):
            return "show"
    return "movie"


def _to_minimal(row: Mapping[str, Any]) -> dict[str, Any]:
    ids_src = row.get("ids")
    ids_block: Mapping[str, Any] = ids_src if isinstance(ids_src, Mapping) else {}
    imdb_val = ids_block.get("imdb") or row.get("imdb_id") or row.get("imdb")
    tvdb_val = ids_block.get("tvdb") or row.get("tvdb_id") or row.get("tvdb")
    tmdb_val = ids_block.get("tmdb") or row.get("tmdb_id") or row.get("tmdb") or row.get("id")
    mdblist_val = ids_block.get("mdblist")

    ids: dict[str, Any] = {}
    imdb_s = _as_str(imdb_val)
    if imdb_s:
        ids["imdb"] = imdb_s
    tmdb_i = _as_int(tmdb_val)
    if tmdb_i is not None:
        ids["tmdb"] = str(tmdb_i)
    tvdb_i = _as_int(tvdb_val)
    if tvdb_i is not None:
        ids["tvdb"] = str(tvdb_i)
    mdbl_s = _as_str(mdblist_val)
    if mdbl_s:
        ids["mdblist"] = mdbl_s

    typ = _pick_kind_from_row(row)
    title = str(row.get("title") or row.get("name") or row.get("original_title") or row.get("original_name") or "").strip()
    year = row.get("year") or row.get("release_year") or (int(str(row.get("release_date"))[:4]) if row.get("release_date") else None) or row.get("first_air_year") or (int(str(row.get("first_air_date"))[:4]) if row.get("first_air_date") else None)

    minimal: dict[str, Any] = {"type": typ, "ids": ids}
    if title:
        minimal["title"] = title
    year_i = _as_int(year)
    if year_i is not None:
        minimal["year"] = year_i
    return minimal


def _parse_rows_and_total(data: Any) -> tuple[list[Mapping[str, Any]], int | None]:
    if isinstance(data, dict):
        total: int | None = None
        for key in ("total_items", "total", "count", "items_total"):
            v = _as_int(data.get(key))
            if v is not None and v > 0:
                total = v
                break
        rows_any: Any
        if "movies" in data or "shows" in data:
            rows_any = list(data.get("movies", []) or []) + list(data.get("shows", []) or [])
        else:
            rows_any = (data.get("results") or data.get("items") or []) or []
        rows = [x for x in rows_any if isinstance(x, Mapping)] if isinstance(rows_any, list) else []
        return rows, total
    if isinstance(data, list):
        rows = [x for x in data if isinstance(x, Mapping)]
        return rows, None
    return [], None


def _peek_live(adapter: Any, apikey: str, timeout: float, retries: int) -> tuple[str | None, int | None]:
    try:
        r = request_with_retries(
            adapter.client.session,
            "GET",
            URL_LIST,
            params={"apikey": apikey, "limit": 1, "offset": 0, "unified": 1},
            timeout=timeout,
            max_retries=retries,
        )
        if r.status_code != 200:
            _log("peek_failed", status=r.status_code)
            return None, None
        rows, total = _parse_rows_and_total(r.json() if (r.text or "").strip() else {})
        if rows:
            key = _key_of(_to_minimal(rows[0]))
            return key, total
        return None, total
    except Exception as e:
        _log("peek_error", error=str(e))
        return None, None


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    cfg = _cfg(adapter)
    ttl_h = _cfg_int(cfg, "watchlist_shadow_ttl_hours", 24)
    validate_shadow = _cfg_bool(cfg, "watchlist_shadow_validate", False)
    limit = _cfg_int(cfg, "watchlist_page_size", 200)

    apikey = _as_str(cfg.get("api_key")) or ""
    shadow = _shadow_load()
    cached: dict[str, dict[str, Any]] = dict(shadow.get("items") or {})
    if not apikey:
        return cached

    timeout = adapter.cfg.timeout
    retries = adapter.cfg.max_retries

    acts = _fetch_last_activities(adapter, apikey=apikey, timeout=timeout, retries=retries) or {}
    acts_ts_raw = acts.get("watchlisted_at") if isinstance(acts, Mapping) else None
    acts_ts = _iso_z(acts_ts_raw) if _iso_ok(acts_ts_raw) else None

    wm = get_watermark("watchlist")
    if acts_ts and wm:
        a = _as_epoch(acts_ts) or 0
        b = _as_epoch(wm) or 0
        if a <= b:
            if cached:
                _log("no_op_using_shadow", watchlisted_at=acts_ts, watermark=wm)
                return cached
            _log("no_op_no_shadow_force_refresh", watchlisted_at=acts_ts, watermark=wm)

    if acts_ts and (not wm) and cached:
        save_watermark("watchlist", acts_ts)
        save_watermark("watchlist_removed", acts_ts)
        _log("baseline_watermark_set_using_shadow", watermark=acts_ts)
        return cached

    if acts_ts:
        _log("watchlist_changed_refresh", watchlisted_at=acts_ts, watermark=wm or "-")
    else:
        if ttl_h > 0 and shadow.get("ts") and cached:
            age = int(time.time()) - int(shadow.get("ts", 0))
            if age <= ttl_h * 3600:
                stale = False
                if validate_shadow:
                    k0, total_live = _peek_live(adapter, apikey, timeout, retries)
                    cached_count = len(cached)
                    if total_live is not None and int(total_live) != cached_count:
                        stale = True
                        _log("shadow_invalid_total_mismatch", live_total=total_live, cached=cached_count)
                    elif k0 and (k0 not in cached):
                        stale = True
                        _log("shadow_invalid_first_item_missing")
                if not stale:
                    return cached

    prog_factory = getattr(adapter, "progress_factory", None)
    prog: Any = prog_factory("watchlist") if callable(prog_factory) else None

    sess = adapter.client.session
    collected: dict[str, dict[str, Any]] = {}
    offset = 0
    total_tick = 0

    while True:
        params = {"apikey": apikey, "limit": limit, "offset": offset, "unified": 1}
        r = request_with_retries(sess, "GET", URL_LIST, params=params, timeout=timeout, max_retries=retries)
        if r.status_code != 200:
            _log("get_failed", status=r.status_code, offset=offset)
            break
        data = r.json() if (r.text or "").strip() else {}
        rows, _ = _parse_rows_and_total(data)
        if not rows:
            break
        for row in rows:
            minimal = _to_minimal(row)
            collected[_key_of(minimal)] = minimal
        batch_len = len(rows)
        total_tick += batch_len
        if prog:
            try:
                prog.tick(total_tick, total=max(total_tick, offset + batch_len))
            except Exception:
                pass
        if batch_len < limit:
            break
        offset += batch_len

    if collected:
        _shadow_save(collected)
        _unfreeze_keys_if_present(collected.keys())

    if acts_ts:
        save_watermark("watchlist", acts_ts)
        save_watermark("watchlist_removed", acts_ts)

    if prog:
        try:
            total = len(collected)
            prog.tick(total, total=total)
        except Exception:
            pass

    _log("index_size", count=len(collected))
    return collected


def _chunk(seq: list[Any], n: int) -> Iterable[list[Any]]:
    n = max(1, int(n))
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


def _batch_payload(items: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    rejected: list[dict[str, Any]] = []
    frozen = _load_unresolved()
    frozen_keys = set(frozen.keys())
    for item in items or []:
        if _key_of(id_minimal(item)) in frozen_keys:
            continue
        ids = _ids_for_mdblist(item)
        if not ids:
            rejected.append({"item": id_minimal(item), "hint": "missing_ids"})
            continue
        if not ids.get("imdb") and ids.get("tmdb") is None:
            rejected.append({"item": id_minimal(item), "hint": "missing_imdb_tmdb"})
            continue
        kind = "show" if str(item.get("type") or "").lower() in ("show", "shows", "tv", "series") else "movie"
        accepted.append({"type": kind, "ids": ids})
    return accepted, rejected


def _payload_from_accepted(accepted_slice: list[dict[str, Any]]) -> dict[str, Any]:
    movies = [{"imdb": x["ids"].get("imdb"), "tmdb": x["ids"].get("tmdb")} for x in accepted_slice if x["type"] == "movie"]
    shows = [{"imdb": x["ids"].get("imdb"), "tmdb": x["ids"].get("tmdb")} for x in accepted_slice if x["type"] == "show"]
    movies = [{k: v for k, v in d.items() if v is not None} for d in movies]
    shows = [{k: v for k, v in d.items() if v is not None} for d in shows]
    movies = [d for d in movies if d]
    shows = [d for d in shows if d]
    payload: dict[str, Any] = {}
    if movies:
        payload["movies"] = movies
    if shows:
        payload["shows"] = shows
    return payload


def _freeze_not_found(not_found: Any, *, action: str, unresolved: list[dict[str, Any]], add_details: bool) -> None:
    nf: Mapping[str, Any] = not_found if isinstance(not_found, Mapping) else {}
    for bucket in ("movies", "shows"):
        value = nf.get(bucket)
        if not isinstance(value, list):
            continue
        for obj in value:
            ids = {k: v for k, v in dict(obj or {}).items() if k in ("tmdb", "imdb")}
            typ = "movie" if bucket == "movies" else "show"
            minimal = id_minimal({"type": typ, "ids": ids})
            unresolved.append({"item": minimal, "hint": "not_found"})
            details = {"ids": ids} if add_details else None
            _freeze_item(minimal, action=action, reasons=[f"{action}:not-found"], details=details)


def _write(adapter: Any, action: str, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    cfg = _cfg(adapter)
    apikey = _as_str(cfg.get("api_key")) or ""
    if not apikey:
        return 0, [{"item": id_minimal(it), "hint": "missing_api_key"} for it in (items or [])]

    batch = _cfg_int(cfg, "watchlist_batch_size", 100)
    freeze_details = _cfg_bool(cfg, "watchlist_freeze_details", True)

    sess = adapter.client.session
    accepted, unresolved = _batch_payload(items)
    if not accepted:
        return 0, unresolved

    ok = 0
    for sl in _chunk(accepted, batch):
        payload = _payload_from_accepted(sl)
        if not payload:
            for x in sl:
                minimal = id_minimal({"type": x["type"], "ids": x["ids"]})
                unresolved.append({"item": minimal, "hint": "missing_imdb_tmdb"})
            continue

        r = request_with_retries(
            sess,
            "POST",
            URL_MODIFY.format(action=action),
            params={"apikey": apikey},
            json=payload,
            timeout=adapter.cfg.timeout,
            max_retries=adapter.cfg.max_retries,
        )

        if r.status_code in (200, 201):
            body_any = r.json() if (r.text or "").strip() else {}
            body: Mapping[str, Any] = body_any if isinstance(body_any, Mapping) else {}
            added_any = body.get("added")
            existing_any = body.get("existing")
            removed_any = body.get("deleted") or body.get("removed")
            added = added_any if isinstance(added_any, Mapping) else {}
            existing = existing_any if isinstance(existing_any, Mapping) else {}
            removed = removed_any if isinstance(removed_any, Mapping) else {}

            if action == "add":
                ok += int(_as_int(added.get("movies")) or 0)
                ok += int(_as_int(added.get("shows")) or 0)
                ok += int(_as_int(existing.get("movies")) or 0)
                ok += int(_as_int(existing.get("shows")) or 0)
            else:
                ok += int(_as_int(removed.get("movies")) or 0)
                ok += int(_as_int(removed.get("shows")) or 0)

            nf_any = body.get("not_found")
            _freeze_not_found(nf_any, action=action, unresolved=unresolved, add_details=freeze_details)

            not_found_keys: set[str] = set()
            nf_map: Mapping[str, Any] = nf_any if isinstance(nf_any, Mapping) else {}
            for bucket in ("movies", "shows"):
                value = nf_map.get(bucket)
                if not isinstance(value, list):
                    continue
                for obj in value:
                    ids_nf = {k: v for k, v in dict(obj or {}).items() if k in ("tmdb", "imdb")}
                    if ids_nf:
                        not_found_keys.add(_key_of({"ids": ids_nf}))

            ok_keys: list[str] = []
            for x in sl:
                k = _key_of({"type": x["type"], "ids": x["ids"]})
                if k not in not_found_keys:
                    ok_keys.append(k)
            if ok_keys:
                _unfreeze_keys_if_present(ok_keys)
        else:
            text = (r.text or "")[:200]
            _log("write_failed", action=action, status=r.status_code, text=text)
            for x in sl:
                minimal = id_minimal({"type": x["type"], "ids": x["ids"]})
                unresolved.append({"item": minimal, "hint": f"http:{r.status_code}"})
                details = {"status": r.status_code} if freeze_details else None
                _freeze_item(minimal, action=action, reasons=[f"http:{r.status_code}"], details=details)

    if ok > 0:
        _shadow_bust()
    return ok, unresolved


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, "add", items)


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    return _write(adapter, "remove", items)

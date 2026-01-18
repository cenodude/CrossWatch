# /providers/sync/jellyfin/_ratings.py
# JELLYFIN Module for ratings sync functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from .._log import log as cw_log

import json
import os
import time
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal
from ._common import (
    state_file,
    jf_get_library_roots,
    jf_resolve_library_id,
    jf_scope_ratings,
    normalize as jelly_normalize,
    _pair_scope,
)

def _unresolved_path() -> str:
    return str(state_file("jellyfin_ratings.unresolved.json"))

def _shadow_path() -> str:
    return str(state_file("jellyfin_ratings.shadow.json"))


def _meta_path() -> str:
    return str(state_file("jellyfin_ratings.meta.json"))


def _now_iso_z() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def _meta_load() -> dict[str, dict[str, Any]]:
    if _pair_scope() is None:
        return {}
    try:
        with open(_meta_path(), "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            out: dict[str, dict[str, Any]] = {}
            for k, v in (raw or {}).items():
                if isinstance(v, dict):
                    out[str(k)] = dict(v)
            return out
    except Exception:
        return {}


def _meta_save(meta: Mapping[str, Mapping[str, Any]]) -> None:
    if _pair_scope() is None:
        return
    try:
        os.makedirs(os.path.dirname(_meta_path()), exist_ok=True)
        with open(_meta_path(), "w", encoding="utf-8") as f:
            json.dump(meta, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass



# shadow 
def _shadow_load() -> dict[str, int]:
    if _pair_scope() is None:
        return {}
    try:
        with open(_shadow_path(), "r", encoding="utf-8") as f:
            raw = json.load(f) or {}
            return {str(k): int(v) for k, v in raw.items()}
    except Exception:
        return {}


def _shadow_save(d: Mapping[str, int]) -> None:
    if _pair_scope() is None:
        return
    try:
        os.makedirs(os.path.dirname(_shadow_path()), exist_ok=True)
        with open(_shadow_path(), "w", encoding="utf-8") as f:
            json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass


# logging
def _trc(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "ratings", "trace", msg, **fields)


def _dbg(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "ratings", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "ratings", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("JELLYFIN", "ratings", "warn", msg, **fields)


def _dbg_enabled() -> bool:
    # Keep debug-only expensive logs behind a fast env check.
    if os.getenv("CW_DEBUG") or os.getenv("CW_JELLYFIN_DEBUG"):
        return True
    lvl = (os.getenv("CW_JELLYFIN_LOG_LEVEL") or os.getenv("CW_LOG_LEVEL") or "").strip().lower()
    return lvl in ("debug", "trace")


# unresolved store
def _load() -> dict[str, Any]:
    if _pair_scope() is None:
        return {}
    try:
        with open(_unresolved_path(), "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save(obj: Mapping[str, Any]) -> None:
    if _pair_scope() is None:
        return
    try:
        os.makedirs(os.path.dirname(_unresolved_path()), exist_ok=True)
        with open(_unresolved_path(), "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False, indent=2, sort_keys=True)
    except Exception:
        pass


def _freeze(item: Mapping[str, Any], *, reason: str) -> None:
    key = canonical_key(item)
    data = _load()
    ent = data.get(key) or {"feature": "ratings", "attempts": 0}
    ent.update({"hint": id_minimal(item)})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
    ent["reason"] = reason
    data[key] = ent
    _save(data)


def _thaw_if_present(keys: Iterable[str]) -> None:
    data = _load()
    changed = False
    for k in list(keys or []):
        if k in data:
            data.pop(k, None)
            changed = True
    if changed:
        _save(data)


# cfg
def _limit(adapter: Any) -> int:
    cfg = getattr(adapter, "cfg", None)
    v = getattr(cfg, "ratings_query_page", None)
    if v is None:
        v = 500
    try:
        return max(50, int(v))
    except Exception:
        return 500


# http helpers
def _body_snip(r: Any, n: int = 240) -> str:
    try:
        t = r.text() if callable(getattr(r, "text", None)) else getattr(r, "text", "")
        if not t:
            return "no-body"
        return (t[:n] + "…") if len(t) > n else t
    except Exception:
        return "no-body"


# low-level write (numeric rating 0..10; accepts 0..5 upscale)
def _rate(http: Any, uid: str, item_id: str, rating: float | None) -> bool:
    try:
        payload: dict[str, Any] = {}
        if rating is None:
            payload["Rating"] = None
        else:
            r_val = float(rating)
            if 0.0 <= r_val <= 5.0:
                r_val *= 2.0
            r_val = max(0.0, min(10.0, r_val))
            payload["Rating"] = round(r_val, 1)

        r1 = http.post(f"/UserItems/{item_id}/UserData", params={"userId": uid}, json=payload)
        ok = getattr(r1, "status_code", 0) in (200, 204)
        if ok:
            return True

        r2 = http.post(f"/Users/{uid}/Items/{item_id}/UserData", json=payload)
        ok2 = getattr(r2, "status_code", 0) in (200, 204)

        if not ok2:
            body = _body_snip(r1) if getattr(r1,'status_code',0) not in (200,204) else _body_snip(r2)
            _warn("rate write failed", user_id=uid, item_id=item_id, status1=getattr(r1,'status_code',None), status2=getattr(r2,'status_code',None), body=body)
        return ok2
    except Exception as e:
        _warn("rate write exception", item_id=item_id, err=repr(e))
        return False


# index builder
def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("ratings") if callable(prog_mk) else None

    http = adapter.client
    uid = adapter.cfg.user_id
    page = _limit(adapter)
    start = 0

    scope_params = jf_scope_ratings(adapter.cfg)
    scope_libs: list[str] = []
    if isinstance(scope_params, Mapping):
        pid = scope_params.get("ParentId")
        if pid:
            scope_libs = [str(pid)]
        else:
            anc = scope_params.get("AncestorIds")
            if isinstance(anc, (list, tuple)):
                scope_libs = [str(x) for x in anc if x]

    roots = jf_get_library_roots(adapter)

    out: dict[str, dict[str, Any]] = {}
    total_seen = 0

    meta = _meta_load()
    meta_changed = False

    while True:
        params: dict[str, Any] = {
            "userId": uid,
            "recursive": True,
            "includeItemTypes": "Movie,Series,Episode",
            "enableUserData": True,
            "fields": (
                "ProviderIds,ProductionYear,UserData,UserRating,Type,"
                "IndexNumber,ParentIndexNumber,SeriesName,Name,"
                "ParentId,LibraryId,AncestorIds"
            ),
            "startIndex": start,
            "limit": page,
            "enableTotalRecordCount": True,
            "hasUserRating": True,
            "sortBy": "SortName",
            "sortOrder": "Ascending",
        }
        if scope_params:
            params.update(scope_params)

        r = http.get(f"/Users/{uid}/Items", params=params)
        body = r.json() or {}
        rows = body.get("Items") or []
        if not rows:
            break

        for row in rows:
            total_seen += 1
            ud = row.get("UserData") or {}

            rating_raw = row.get("UserRating")
            if rating_raw is None:
                rating_raw = ud.get("Rating")
            if rating_raw is None:
                continue

            try:
                rf = float(rating_raw)
            except Exception:
                continue

            if rf <= 0.0:
                continue

            try:
                m_norm = jelly_normalize(row)
                lib_id = jf_resolve_library_id(row, roots, scope_libs, http)
                m = dict(m_norm)
                m["library_id"] = lib_id
                m["rating"] = round(rf, 1)

                k = canonical_key(m)
                prev = meta.get(k)
                cur_rating = m.get("rating")
                if not isinstance(prev, dict):
                    prev = {"rating": cur_rating, "rated_at": _now_iso_z()}
                    meta[k] = prev
                    meta_changed = True
                else:
                    if prev.get("rating") != cur_rating:
                        prev["rating"] = cur_rating
                        prev["rated_at"] = _now_iso_z()
                        meta_changed = True
                ra = prev.get("rated_at")
                if ra:
                    m["rated_at"] = str(ra)
                jf_new = str((m.get("ids") or {}).get("jellyfin") or row.get("Id") or "")

                prev = out.get(k)
                if not prev:
                    out[k] = m
                else:
                    jf_prev = str((prev.get("ids") or {}).get("jellyfin") or "")
                    if jf_new and jf_prev and jf_new < jf_prev:
                        out[k] = m
            except Exception:
                pass

        start += len(rows)
        if prog:
            try:
                prog.tick(total_seen, total=max(total_seen, start))
            except Exception:
                pass
        if len(rows) < page:
            break

    if prog:
        try:
            prog.done(ok=True, total=len(out))
        except Exception:
            pass

    if _dbg_enabled():
        seen_libs: dict[str, int] = {}
        for m in out.values():
            lid = m.get("library_id") or "NONE"
            lid_s = str(lid)
            seen_libs[lid_s] = seen_libs.get(lid_s, 0) + 1
        _dbg("library_id_distribution", libs=seen_libs)

    shadow = _shadow_load()
    if shadow:
        added = 0
        for k in shadow.keys():
            if k not in out:
                if k not in out:
                    m_shadow: dict[str, Any] = {"shadow": True}
                    prev = meta.get(k)
                    if isinstance(prev, dict):
                        if prev.get("rating") is not None:
                            m_shadow["rating"] = prev.get("rating")
                        if prev.get("rated_at"):
                            m_shadow["rated_at"] = str(prev.get("rated_at"))
                    out[k] = m_shadow
                added += 1
        if added:
            _dbg("shadow merged", added=added)

    _thaw_if_present(out.keys())
    _info("index done", count=len(out))
    if meta_changed:
        _meta_save(meta)
    return out


# writes
def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    ok = 0
    unresolved: list[dict[str, Any]] = []
    shadow = _shadow_load()
    meta = _meta_load()
    meta_changed = False

    from ._common import resolve_item_id

    for it in items or []:
        base: dict[str, Any] = dict(it or {})
        base_ids_raw = base.get("ids")
        if isinstance(base_ids_raw, Mapping):
            base_ids: dict[str, Any] = dict(base_ids_raw)
        else:
            base_ids = {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())

        rating_raw = base.get("rating")
        if rating_raw is None:
            unresolved.append({"item": id_minimal(base), "hint": "invalid_rating"})
            _freeze(base, reason="invalid_rating")
            continue

        try:
            rf = float(rating_raw)
        except Exception:
            unresolved.append({"item": id_minimal(base), "hint": "invalid_rating"})
            _freeze(base, reason="invalid_rating")
            continue

        m = jelly_normalize(base) if not has_ids else base

        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            continue

        iid = resolve_item_id(adapter, m)
        if not iid:
            unresolved.append({"item": id_minimal(m), "hint": "not_in_library"})
            _freeze(m, reason="resolve_failed")
            continue

        if _rate(http, uid, iid, rf):
            ok += 1
            shadow[k] = int(shadow.get(k, 0)) + 1
            ra = str(base.get("rated_at") or m.get("rated_at") or "") or _now_iso_z()
            cur = meta.get(k) if isinstance(meta.get(k), dict) else {}
            cur = dict(cur or {})
            cur["rating"] = round(rf, 1)
            cur["rated_at"] = ra
            meta[k] = cur
            meta_changed = True
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "rate_failed"})
            _freeze(m, reason="write_failed")

    shadow = {k: v for k, v in shadow.items() if v > 0}
    _shadow_save(shadow)
    if meta_changed:
        _meta_save(meta)

    _info("add done", ok=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    ok = 0
    unresolved: list[dict[str, Any]] = []
    shadow = _shadow_load()
    meta = _meta_load()
    meta_changed = False

    from ._common import resolve_item_id

    for it in items or []:
        base: dict[str, Any] = dict(it or {})
        base_ids_raw = base.get("ids")
        if isinstance(base_ids_raw, Mapping):
            base_ids: dict[str, Any] = dict(base_ids_raw)
        else:
            base_ids = {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())

        m = jelly_normalize(base) if not has_ids else base

        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            continue

        iid = resolve_item_id(adapter, m)
        if not iid:
            unresolved.append({"item": id_minimal(m), "hint": "not_in_library"})
            _freeze(m, reason="resolve_failed")
            continue

        if _rate(http, uid, iid, None):
            ok += 1
            if k in meta:
                meta.pop(k, None)
                meta_changed = True
            cur = int(shadow.get(k, 0))
            nxt = max(0, cur - 1)
            if nxt > 0:
                shadow[k] = nxt
            else:
                shadow.pop(k, None)
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "clear_failed"})
            _freeze(m, reason="write_failed")

    shadow = {k: v for k, v in shadow.items() if v > 0}
    _shadow_save(shadow)
    if meta_changed:
        _meta_save(meta)

    _info("remove done", ok=ok, unresolved=len(unresolved))
    return ok, unresolved

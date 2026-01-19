# /providers/sync/emby/_ratings.py
# EMBY Module for ratings synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal
from .._log import log as cw_log
from ._common import normalize as emby_normalize, provider_index, resolve_item_id, state_file, _pair_scope

def _unresolved_path() -> str:
    return state_file("emby_ratings.unresolved.json")


def _meta_path() -> str:
    return state_file("emby_ratings.meta.json")


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



def _dbg(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "ratings", "debug", msg, **fields)


def _info(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "ratings", "info", msg, **fields)


def _warn(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "ratings", "warn", msg, **fields)


def _error(msg: str, **fields: Any) -> None:
    cw_log("EMBY", "ratings", "error", msg, **fields)


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
    ent.update({"hint": id_minimal(item), "reason": reason})
    ent["attempts"] = int(ent.get("attempts", 0)) + 1
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


def _like_threshold(adapter: Any) -> float:
    try:
        return float(getattr(adapter.cfg, "ratings_like_threshold", 6.0))
    except Exception:
        return 6.0


def _write_like_enabled(adapter: Any) -> bool:
    return bool(getattr(adapter.cfg, "ratings_write_like", True))


def _write_numeric_enabled(adapter: Any) -> bool:
    return bool(getattr(adapter.cfg, "ratings_write_numeric", True))


def _delay_ms(adapter: Any) -> int:
    try:
        return max(0, int(getattr(adapter.cfg, "ratings_write_delay_ms", 0)))
    except Exception:
        return 0


def _set_like(http: Any, uid: str, item_id: str, *, likes: bool | None) -> bool:
    try:
        if likes is None:
            r = http.delete(f"/Users/{uid}/Items/{item_id}/Rating")
            return getattr(r, "status_code", 0) in (200, 204)
        r = http.post(
            f"/Users/{uid}/Items/{item_id}/Rating",
            params={"Likes": "true" if likes else "false"},
        )
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception as e:
        _dbg("rating_write_thumbs_error", item_id=item_id, error=str(e))
        return False


def _set_numeric_rating(http: Any, uid: str, item_id: str, *, rating: float | None) -> bool:
    try:
        if rating is None:
            r = http.post(
                f"/Users/{uid}/Items/{item_id}/UserData",
                json={"Rating": None},
            )
            return getattr(r, "status_code", 0) in (200, 204)
        r = http.post(
            f"/Users/{uid}/Items/{item_id}/UserData",
            json={"Rating": float(rating)},
        )
        return getattr(r, "status_code", 0) in (200, 204)
    except Exception as e:
        _dbg("rating_write_numeric_error", item_id=item_id, error=str(e))
        return False


def _progress_tick(progress: Any | None, current: int, *, total: int, force: bool = False) -> None:
    if not progress:
        return
    try:
        tick = getattr(progress, "tick", None)
        if callable(tick):
            tick(current, total=total, force=force)
    except Exception:
        _dbg("progress_tick_failed")


def build_index(adapter: Any, *, progress: Any | None = None) -> dict[str, dict[str, Any]]:
    http = adapter.client
    uid = adapter.cfg.user_id

    meta = _meta_load()
    meta_changed = False

    pidx = provider_index(adapter)
    keys = sorted(pidx.keys())
    out: dict[str, dict[str, Any]] = {}

    done = 0
    total = len(keys)
    _progress_tick(progress, 0, total=total, force=True)

    for pref in keys:
        rows = pidx.get(pref) or []
        for it in rows:
            iid = it.get("Id")
            if not iid:
                continue
            r = http.get(
                f"/Items/{iid}",
                params={"UserId": uid, "Fields": "UserData"},
            )
            if getattr(r, "status_code", 0) != 200:
                continue
            body = r.json() or {}
            ud = body.get("UserData") or {}
            rating = ud.get("Rating")
            liked = ud.get("Likes")
            if rating is None and liked is None:
                continue
            try:
                norm = emby_normalize(body if body.get("Id") else it)
                if norm.get("type") == "episode":
                    try:
                        s = int(norm.get("season") or 0)
                        e = int(norm.get("episode") or 0)
                        if s and e:
                            norm["title"] = f"S{s:02d}E{e:02d}"
                    except Exception:
                        pass
                if rating is not None:
                    try:
                        rf = float(rating)
                        norm["rating"] = rf
                        norm["user_rating"] = rf
                    except Exception:
                        pass
                if liked is not None:
                    norm["liked"] = bool(liked)
                    norm["user_liked"] = bool(liked)
                k = canonical_key(norm)
                cur_rating = norm.get("rating")
                cur_liked = norm.get("liked")
                prev = meta.get(k)
                if not isinstance(prev, dict):
                    prev = {"rating": cur_rating, "liked": cur_liked, "rated_at": _now_iso_z()}
                    meta[k] = prev
                    meta_changed = True
                else:
                    if prev.get("rating") != cur_rating or prev.get("liked") != cur_liked:
                        prev["rating"] = cur_rating
                        prev["liked"] = cur_liked
                        prev["rated_at"] = _now_iso_z()
                        meta_changed = True
                ra = prev.get("rated_at")
                if ra:
                    norm["rated_at"] = str(ra)
                out[k] = norm
            except Exception:
                pass
        done += 1
        _progress_tick(progress, done, total=total)
    _info("index_done", count=len(out))
    if meta_changed:
        _meta_save(meta)
    return out


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    thresh = _like_threshold(adapter)
    do_num = _write_numeric_enabled(adapter)
    do_like = _write_like_enabled(adapter)
    delay = _delay_ms(adapter)
    ok = 0
    unresolved: list[dict[str, Any]] = []

    meta = _meta_load()
    meta_changed = False

    stats: dict[str, int] = {
        "numeric_set": 0,
        "numeric_cleared": 0,
        "thumbs_set": 0,
        "thumbs_cleared": 0,
        "invalid_rating": 0,
        "resolve_failed": 0,
        "write_failed": 0,
        "missing_ids_for_key": 0,
    }

    for it in items or []:
        base: dict[str, Any] = dict(it or {})
        base_ids = base.get("ids") if isinstance(base.get("ids"), dict) else {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())
        m: Mapping[str, Any] = emby_normalize(base) if not has_ids else base
        try:
            k = canonical_key(m) or canonical_key(base)
        except Exception:
            k = None
        if not k:
            unresolved.append({"item": id_minimal(base), "hint": "missing_ids_for_key"})
            _freeze(base, reason="missing_ids_for_key")
            stats["missing_ids_for_key"] += 1
            continue

        liked_flag = base.get("liked")
        rating_val = base.get("rating")
        rf: float | None = None
        if rating_val is not None:
            try:
                rf = float(rating_val)
            except Exception:
                unresolved.append({"item": id_minimal(base), "hint": "invalid_rating"})
                _freeze(base, reason="invalid_rating")
                stats["invalid_rating"] += 1
                continue

        if isinstance(liked_flag, bool):
            likes: bool | None = liked_flag
        elif rf is not None:
            likes = rf >= thresh
        else:
            likes = None

        iid = resolve_item_id(adapter, m)
        if not iid:
            unresolved.append({"item": id_minimal(m), "hint": "not_in_library"})
            _freeze(m, reason="resolve_failed")
            stats["resolve_failed"] += 1
            continue

        wrote = False
        if do_num and rf is not None:
            if _set_numeric_rating(http, uid, iid, rating=rf):
                wrote = True
                stats["numeric_set"] += 1
        if do_like and likes is not None:
            if _set_like(http, uid, iid, likes=likes):
                wrote = True
                stats["thumbs_set"] += 1
        elif do_like and likes is None:
            if _set_like(http, uid, iid, likes=None):
                wrote = True
                stats["thumbs_cleared"] += 1

        if wrote:
            ok += 1
            ra = str(base.get("rated_at") or m.get("rated_at") or "") or _now_iso_z()
            cur = meta.get(k) if isinstance(meta.get(k), dict) else {}
            if rf is None and likes is None:
                meta.pop(k, None)
            else:
                cur = dict(cur or {})
                cur["rating"] = rf
                cur["liked"] = likes
                cur["rated_at"] = ra
                meta[k] = cur
            meta_changed = True
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "rate_failed"})
            _freeze(m, reason="write_failed")
            stats["write_failed"] += 1

        if delay:
            time.sleep(delay / 1000.0)

    if meta_changed:
        _meta_save(meta)

    _info(
        "write_done",
        op="add",
        ok=ok,
        unresolved=len(unresolved),
        numeric_set=stats["numeric_set"],
        thumbs_set=stats["thumbs_set"],
        thumbs_cleared=stats["thumbs_cleared"],
        write_failed=stats["write_failed"],
    )
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    ok = 0
    unresolved: list[dict[str, Any]] = []

    meta = _meta_load()
    meta_changed = False

    for it in items or []:
        base: dict[str, Any] = dict(it or {})
        base_ids = base.get("ids") if isinstance(base.get("ids"), dict) else {}
        has_ids = bool(base_ids) and any(v not in (None, "", 0) for v in base_ids.values())
        m: Mapping[str, Any] = emby_normalize(base) if not has_ids else base
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

        like_ok = _set_like(http, uid, iid, likes=None)
        num_ok = _set_numeric_rating(http, uid, iid, rating=None)
        if like_ok or num_ok:
            ok += 1
            if k in meta:
                meta.pop(k, None)
                meta_changed = True
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "clear_failed"})
            _freeze(m, reason="write_failed")
    if meta_changed:
        _meta_save(meta)
    _info("write_done", op="remove", ok=ok, unresolved=len(unresolved))
    return ok, unresolved

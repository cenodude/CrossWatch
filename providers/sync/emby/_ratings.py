# /providers/sync/emby/_ratings.py
# EMBY Module for ratings synchronization
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
import os
import time
from typing import Any, Iterable, Mapping

from cw_platform.id_map import canonical_key, minimal as id_minimal

from ._common import normalize as emby_normalize, provider_index, resolve_item_id

UNRESOLVED_PATH = "/config/.cw_state/emby_ratings.unresolved.json"


def _dbg_on() -> bool:
    return bool(os.environ.get("CW_EMBY_DEBUG") or os.environ.get("CW_DEBUG"))


def _log(msg: str) -> None:
    if _dbg_on():
        print(f"[EMBY:ratings] {msg}")


def _load() -> dict[str, Any]:
    try:
        with open(UNRESOLVED_PATH, "r", encoding="utf-8") as f:
            return json.load(f) or {}
    except Exception:
        return {}


def _save(obj: Mapping[str, Any]) -> None:
    try:
        os.makedirs(os.path.dirname(UNRESOLVED_PATH), exist_ok=True)
        with open(UNRESOLVED_PATH, "w", encoding="utf-8") as f:
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
        if _dbg_on():
            _log(f"thumbs write error {item_id}: {e}")
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
        if _dbg_on():
            _log(f"numeric write error {item_id}: {e}")
        return False


def _progress_tick(progress: Any | None, current: int, *, total: int, force: bool = False) -> None:
    if not progress:
        return
    try:
        tick = getattr(progress, "tick", None)
        if callable(tick):
            tick(current, total=total, force=force)
    except Exception:
        if _dbg_on():
            _log("progress tick failed")


def build_index(adapter: Any, *, progress: Any | None = None) -> dict[str, dict[str, Any]]:
    http = adapter.client
    uid = adapter.cfg.user_id

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
                out[canonical_key(norm)] = norm
            except Exception:
                pass
        done += 1
        _progress_tick(progress, done, total=total)
    _log(f"index size: {len(out)}")
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
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "rate_failed"})
            _freeze(m, reason="write_failed")
            stats["write_failed"] += 1

        if delay:
            time.sleep(delay / 1000.0)

    _log(
        f"add done: +{ok} / unresolved {len(unresolved)} | "
        f"numeric_set={stats['numeric_set']} thumbs_set={stats['thumbs_set']}"
    )
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    http = adapter.client
    uid = adapter.cfg.user_id
    ok = 0
    unresolved: list[dict[str, Any]] = []

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
            _thaw_if_present([k])
        else:
            unresolved.append({"item": id_minimal(m), "hint": "clear_failed"})
            _freeze(m, reason="write_failed")

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
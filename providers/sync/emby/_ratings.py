# /providers/sync/emby/_ratings.py
from __future__ import annotations
import os, json, time
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple

# ===== ID helpers =====
try:
    from cw_platform.id_map import minimal as id_minimal, canonical_key
except Exception:
    from _id_map import minimal as id_minimal, canonical_key  # type: ignore

from ._common import (
    normalize as emby_normalize,
    provider_index,
    resolve_item_id,
)

UNRESOLVED_PATH = "/config/.cw_state/emby_ratings.unresolved.json"

# Debug logging
def _dbg_on() -> bool:
    return bool(os.environ.get("CW_EMBY_DEBUG") or os.environ.get("CW_DEBUG"))

def _log(msg: str) -> None:
    if _dbg_on():
        print(f"[EMBY:ratings] {msg}")


# Unresolved items persistence
def _load() -> Dict[str, Any]:
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

# Write configuration helpers
def _like_threshold(adapter) -> float:
    try:
        return float(getattr(adapter.cfg, "ratings_like_threshold", 6.0))
    except Exception:
        return 6.0

def _write_like_enabled(adapter) -> bool:
    return bool(getattr(adapter.cfg, "ratings_write_like", True))

def _write_numeric_enabled(adapter) -> bool:
    return bool(getattr(adapter.cfg, "ratings_write_numeric", True))

def _delay_ms(adapter) -> int:
    try:
        return max(0, int(getattr(adapter.cfg, "ratings_write_delay_ms", 0)))
    except Exception:
        return 0

# HTTP write operations
def _set_like(http, uid: str, item_id: str, *, likes: Optional[bool]) -> bool:
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

def _set_numeric_rating(http, uid: str, item_id: str, *, rating: Optional[float]) -> bool:
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

def build_index(adapter, *, progress=None):
    http = adapter.client
    uid  = adapter.cfg.user_id

    # Build provider index (all library items w/ provider IDs)
    pidx = provider_index(adapter)
    keys = sorted(pidx.keys())
    out: Dict[str, Dict[str, Any]] = {}

    done = 0
    total = len(keys)
    if progress:
        progress.tick(0, total=total, force=True)

    for pref in keys:
        rows = pidx.get(pref) or []

        for it in rows:
            iid = it.get("Id")
            if not iid:
                continue

            # IMPORTANT: use server-scoped item endpoint + UserId to get UserData under API-key auth
            r = http.get(
                f"/Items/{iid}",
                params={"UserId": uid, "Fields": "UserData"}
            )
            if getattr(r, "status_code", 0) != 200:
                continue

            body = r.json() or {}
            ud = body.get("UserData") or {}

            rating = ud.get("Rating")
            liked  = ud.get("Likes")

            if rating is None and liked is None:
                continue  # no user rating info

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
        if progress:
            progress.tick(done, total=total)

    _log(f"index size: {len(out)}")
    return out

def add(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client
    uid  = adapter.cfg.user_id
    thresh = _like_threshold(adapter)
    do_num = _write_numeric_enabled(adapter)
    do_like = _write_like_enabled(adapter)
    delay = _delay_ms(adapter)
    ok = 0
    unresolved: List[Dict[str, Any]] = []

    stats = {
        "numeric_set": 0, "numeric_cleared": 0,
        "thumbs_set": 0, "thumbs_cleared": 0,
        "invalid_rating": 0, "resolve_failed": 0, "write_failed": 0,
    }

    for it in items or []:
        liked_flag = it.get("liked")
        rating_val = it.get("rating")

        rf = None
        if rating_val is not None:
            try:
                rf = float(rating_val)
            except Exception:
                unresolved.append({"item": id_minimal(it), "hint": "invalid_rating"})
                _freeze(it, reason="invalid_rating")
                stats["invalid_rating"] += 1
                continue

        if isinstance(liked_flag, bool):
            likes = liked_flag
        elif rf is not None:
            likes = (rf >= thresh)
        else:
            likes = None

        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            stats["resolve_failed"] += 1
            continue

        wrote = False

        if do_num and rf is not None:
            if _set_numeric_rating(http, uid, iid, rating=rf):
                wrote = True
                stats["numeric_set"] += 1

        if do_like and (likes is not None):
            if _set_like(http, uid, iid, likes=likes):
                wrote = True
                stats["thumbs_set"] += 1
        elif do_like and likes is None:
            # clear thumbs
            if _set_like(http, uid, iid, likes=None):
                wrote = True
                stats["thumbs_cleared"] += 1
        if wrote:
            ok += 1
            _thaw_if_present([canonical_key(id_minimal(it))])
        else:
            unresolved.append({"item": id_minimal(it), "hint": "rate_failed"})
            _freeze(it, reason="write_failed")
            stats["write_failed"] += 1
        if delay:
            time.sleep(delay / 1000.0)
    _log(
        f"add done: +{ok} / unresolved {len(unresolved)} | "
        f"numeric_set={stats['numeric_set']} thumbs_set={stats['thumbs_set']} "
    )

    return ok, unresolved

def remove(adapter, items: Iterable[Mapping[str, Any]]) -> Tuple[int, List[Dict[str, Any]]]:
    http = adapter.client
    uid  = adapter.cfg.user_id
    ok = 0
    unresolved = []

    for it in items or []:
        iid = resolve_item_id(adapter, it)
        if not iid:
            unresolved.append({"item": id_minimal(it), "hint": "not_in_library"})
            _freeze(it, reason="resolve_failed")
            continue

        like_ok = _set_like(http, uid, iid, likes=None)
        num_ok = _set_numeric_rating(http, uid, iid, rating=None)

        if like_ok or num_ok:
            ok += 1
            _thaw_if_present([canonical_key(id_minimal(it))])
        else:
            unresolved.append({"item": id_minimal(it), "hint": "clear_failed"})
            _freeze(it, reason="write_failed")

    _log(f"remove done: -{ok} / unresolved {len(unresolved)}")
    return ok, unresolved
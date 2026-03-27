# /providers/sync/simkl/_watchlist.py
# SIMKL Module for watchlist sync
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import re
import os
import time
from typing import Any, Iterable, Mapping

from cw_platform.id_map import minimal as id_minimal

from .._log import log as cw_log
from ._common import (
    adapter_headers,
    coalesce_date_from,
    fetch_activities,
    get_watermark,
    load_json_state,
    normalize_flat_watermarks,
    key_of as simkl_key_of,
    save_json_state,
    save_watermark,
    state_file,
    _is_capture_mode,
)

BASE = "https://api.simkl.com"
URL_INDEX_ALL = f"{BASE}/sync/all-items/"
URL_INDEX_BUCKET = f"{BASE}/sync/all-items/{{bucket}}/plantowatch"
URL_INDEX_IDS = f"{BASE}/sync/all-items/{{bucket}}/plantowatch"
URL_ADD = f"{BASE}/sync/add-to-list"
URL_REMOVE = f"{BASE}/sync/history/remove"


def _unresolved_path():
    return state_file("simkl.watchlist.unresolved.json")


def _shadow_path():
    return state_file("simkl.watchlist.shadow.json")


WATCHLIST_BUCKETS = ("movies", "shows", "anime")



def _log(msg: str, *, level: str = "debug", **fields: Any) -> None:
    cw_log("SIMKL", "watchlist", level, msg, **fields)


def _dbg(event: str, **fields: Any) -> None:
    _log(event, level="debug", **fields)


def _info(event: str, **fields: Any) -> None:
    _log(event, level="info", **fields)


def _warn(event: str, **fields: Any) -> None:
    _log(event, level="warn", **fields)


def _load_unresolved() -> dict[str, Any]:
    return load_json_state(_unresolved_path())


def _save_unresolved(data: Mapping[str, Any]) -> None:
    if _is_capture_mode():
        return
    save_json_state(_unresolved_path(), data)


def _freeze(
    item: Mapping[str, Any],
    *,
    action: str,
    reasons: list[str],
    ids_sent: Mapping[str, Any],
) -> None:
    key = simkl_key_of(id_minimal(item))
    data = _load_unresolved()
    entry = data.get(key) or {
        "feature": "watchlist",
        "action": action,
        "first_seen": int(time.time()),
        "attempts": 0,
    }
    entry.update({"item": id_minimal(item), "last_attempt": int(time.time())})
    existing_reasons: list[str] = (
        list(entry.get("reasons", [])) if isinstance(entry.get("reasons"), list) else []
    )
    entry["reasons"] = sorted(set(existing_reasons) | set(reasons or []))
    entry["ids_sent"] = dict(ids_sent or {})
    entry["attempts"] = int(entry.get("attempts", 0)) + 1
    data[key] = entry
    _save_unresolved(data)


def _unfreeze_if_present(keys: Iterable[str]) -> None:
    data = _load_unresolved()
    changed = False
    for k in list(keys or []):
        if k in data:
            del data[k]
            changed = True
    if changed:
        _save_unresolved(data)



def _shadow_load() -> dict[str, Any]:
    if _is_capture_mode():
        return {"ts": None, "items": {}, "buckets_seen": {}}
    data = load_json_state(_shadow_path())
    if not isinstance(data, dict):
        return {"ts": None, "items": {}, "buckets_seen": {}}
    if "buckets_seen" not in data or not isinstance(data.get("buckets_seen"), dict):
        data["buckets_seen"] = {}
    if "items" not in data or not isinstance(data.get("items"), dict):
        data["items"] = {}
    data.setdefault("ts", None)
    return data


def _shadow_save(
    ts: str | None,
    items: Mapping[str, Any],
    buckets_seen: Mapping[str, Any] | None = None,
) -> None:
    if _is_capture_mode():
        return
    save_json_state(
        _shadow_path(),
        {
            "ts": ts,
            "items": dict(items),
            "buckets_seen": dict(buckets_seen or {}),
        },
    )


def _shadow_age_seconds() -> float:
    try:
        return max(0.0, time.time() - _shadow_path().stat().st_mtime)
    except Exception:
        return 1e9


def _shadow_ttl_seconds() -> float:
    try:
        value = os.getenv("CW_SIMKL_SHADOW_TTL") or "300"
        return float(value)
    except Exception:
        return 300.0


_ALLOWED_ID_KEYS = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")


def _ids_filter(ids_in: Mapping[str, Any]) -> dict[str, Any]:
    return {k: ids_in.get(k) for k in _ALLOWED_ID_KEYS if ids_in.get(k)}



_SLUG_SMALL_WORDS = {
    "a","an","and","as","at","but","by","for","in","nor","of","on","or","per","the","to","vs","via","with",
}

def _title_from_slug(slug: str) -> str | None:
    s = re.sub(r"[-_]+", " ", str(slug or "")).strip()
    if not s:
        return None
    words = [w for w in s.split(" ") if w]
    out: list[str] = []
    for i, w in enumerate(words):
        lw = w.lower()
        if i > 0 and lw in _SLUG_SMALL_WORDS:
            out.append(lw)
        else:
            out.append(lw[:1].upper() + lw[1:] if lw else lw)
    return " ".join(out) or None


def _anime_title_hint(ids_src: Any) -> str | None:
    if not isinstance(ids_src, Mapping):
        return None
    slug = ids_src.get("tvdbslug") or ids_src.get("trakttvslug") or ids_src.get("slug")
    return _title_from_slug(str(slug)) if slug else None


def _kind_group(item: Mapping[str, Any]) -> str:
    bucket = str(item.get("simkl_bucket") or "").strip().lower()
    if bucket == "movies":
        return "movies"
    if bucket in ("shows", "anime"):
        return "shows"
    media_type = str(item.get("type") or "movie").strip().lower()
    return "movies" if media_type in ("movie", "movies") else "shows"


def _sum_processed_from_body(body: Any) -> int:
    try:
        if not isinstance(body, dict):
            return 0
        total = 0
        for key in ("movies", "shows"):
            value = body.get(key)
            if isinstance(value, list):
                total += len(value)
            elif isinstance(value, int):
                total += value
        if total:
            return total
        for parent in ("added", "removed", "deleted"):
            sub = body.get(parent)
            if isinstance(sub, dict):
                subtotal = 0
                for key in ("movies", "shows"):
                    value = sub.get(key)
                    if isinstance(value, int):
                        subtotal += value
                    elif isinstance(value, list):
                        subtotal += len(value)
                if subtotal:
                    return subtotal
        return 0
    except Exception:
        return 0


def _rows_from_data(data: Any, bucket: str) -> list[Any]:
    if data is None:
        return []
    if isinstance(data, dict) and data.get("type") == "null" and data.get("body") is None:
        return []
    if isinstance(data, list):
        return data
    if isinstance(data, dict):
        if isinstance(data.get("items"), list):
            return data["items"]
        key = "movies" if bucket == "movies" else ("anime" if bucket == "anime" else "shows")
        arr = data.get(key)
        if isinstance(arr, list):
            return arr
    return []


def _normalize_row(bucket: str, row: Any) -> dict[str, Any]:
    anime_type: str | None = None

    if isinstance(row, Mapping):
        node_any = row.get("movie") or row.get("show") or row.get("anime") or row or {}
        node = node_any if isinstance(node_any, Mapping) else {}
        ids_src = node.get("ids") if isinstance(node.get("ids"), Mapping) else row
        ids = _ids_filter(dict(ids_src or {}))
        title = node.get("title")
        year = node.get("year")

        at = node.get("anime_type") or node.get("animeType")
        if isinstance(at, str) and at.strip():
            anime_type = at.strip().lower()

        if bucket == "anime":
            hint = _anime_title_hint(ids_src)
            if hint:
                title = hint
    elif isinstance(row, (int, str)):
        ids = {"simkl": str(row)}
        title = None
        year = None
    else:
        ids = {}
        title = None
        year = None

    if bucket == "movies":
        media_type = "movie"
    elif bucket == "anime":
        media_type = "movie" if anime_type == "movie" else "show"
    else:
        media_type = "show"

    out: dict[str, Any] = {
        "type": media_type,
        "title": title,
        "year": year,
        "ids": ids,
        "simkl_bucket": bucket,
    }
    if bucket == "anime" and anime_type:
        out["anime_type"] = anime_type
    return out


def _acts_get(data: Mapping[str, Any], *path: str) -> str | None:
    cur: Any = data
    for key in path:
        if isinstance(cur, Mapping) and key in cur:
            cur = cur[key]
        else:
            return None
    return str(cur) if isinstance(cur, str) else None


def _first(*values: str | None) -> str | None:
    for value in values:
        if value:
            return value
    return None


def _bucket_ts(acts: Mapping[str, Any]) -> dict[str, dict[str, str | None]]:
    out: dict[str, dict[str, str | None]] = {
        "movies": {"ptw": None, "rm": None},
        "shows": {"ptw": None, "rm": None},
        "anime": {"ptw": None, "rm": None},
    }
    out["movies"]["ptw"] = _first(
        _acts_get(acts, "movies", "plantowatch"),
        _acts_get(acts, "movies", "all"),
    )
    out["movies"]["rm"] = _first(
        _acts_get(acts, "movies", "removed_from_list"),
        _acts_get(acts, "movies", "removed"),
    )
    out["shows"]["ptw"] = _first(
        _acts_get(acts, "shows", "plantowatch"),
        _acts_get(acts, "shows", "all"),
        _acts_get(acts, "tv_shows", "plantowatch"),
        _acts_get(acts, "watchlist", "shows"),
    )
    out["shows"]["rm"] = _first(
        _acts_get(acts, "shows", "removed_from_list"),
        _acts_get(acts, "shows", "removed"),
        _acts_get(acts, "tv_shows", "removed_from_list"),
        _acts_get(acts, "tv_shows", "removed"),
    )
    out["anime"]["ptw"] = _first(
        _acts_get(acts, "anime", "plantowatch"),
        _acts_get(acts, "anime", "all"),
        _acts_get(acts, "watchlist", "anime"),
    )
    out["anime"]["rm"] = _first(
        _acts_get(acts, "anime", "removed_from_list"),
        _acts_get(acts, "anime", "removed"),
    )
    return out
def _bucket_present(items: Mapping[str, Mapping[str, Any]], bucket: str) -> bool:
    for value in (items or {}).values():
        if isinstance(value, Mapping) and value.get("simkl_bucket") == bucket:
            return True
    return False


def _bucket_ready(
    items: Mapping[str, Mapping[str, Any]],
    bucket: str,
    buckets_seen: Mapping[str, Any],
) -> bool:
    if _bucket_present(items, bucket):
        return True
    return bool((buckets_seen or {}).get(bucket))


def _has_all_buckets(
    items: Mapping[str, Mapping[str, Any]],
    buckets_seen: Mapping[str, Any],
) -> bool:
    return all(_bucket_ready(items, bucket, buckets_seen) for bucket in WATCHLIST_BUCKETS)



def _headers(adapter: Any, *, force_refresh: bool = False) -> dict[str, str]:
    headers = adapter_headers(adapter, force_refresh=force_refresh)
    if force_refresh:
        headers.pop("If-None-Match", None)
        headers["Cache-Control"] = "no-cache"
    return headers


def _merge_upsert(dst: dict[str, dict[str, Any]], src: Mapping[str, Mapping[str, Any]]) -> None:
    for key, value in (src or {}).items():
        current = dict(dst.get(key) or {})
        title = current.get("title") or value.get("title")
        year = current.get("year") or value.get("year")
        ids_a = dict(current.get("ids") or {})
        ids_b = dict(value.get("ids") or {})
        ids = {**ids_a, **{k: v for k, v in ids_b.items() if v}}
        bucket = value.get("simkl_bucket") or current.get("simkl_bucket")
        media_type = value.get("type") or current.get("type")
        dst[key] = {
            "type": media_type,
            "title": title,
            "year": year,
            "ids": ids,
            "simkl_bucket": bucket,
        }


def _keys_from_write_resp(body: Any) -> list[str]:
    keys: list[str] = []
    if not isinstance(body, dict):
        return keys

    def _collect(parent: Mapping[str, Any]) -> None:
        for bucket, media_type in (("movies", "movie"), ("shows", "show")):
            value = parent.get(bucket)
            if isinstance(value, list):
                for item in value:
                    ids = _ids_filter(
                        (item.get("ids") or item) if isinstance(item, Mapping) else {},
                    )
                    if ids:
                        m = {"type": media_type, "ids": ids, "simkl_bucket": bucket}
                        keys.append(simkl_key_of(m))

    for top in ("added", "removed", "deleted"):
        section = body.get(top)
        if isinstance(section, Mapping):
            _collect(section)
    _collect(body)
    seen: set[str] = set()
    uniq: list[str] = []
    for key in keys:
        if key not in seen:
            uniq.append(key)
            seen.add(key)
    return uniq


_SIG_ID_ORDER = ("tmdb", "imdb", "tvdb", "trakt", "simkl", "mal", "anilist", "kitsu", "anidb")


def _id_sig(ids: Mapping[str, Any]) -> str:
    if not isinstance(ids, Mapping):
        return ""
    for k in _SIG_ID_ORDER:
        v = ids.get(k)
        if v:
            return f"{k}:{v}"
    return ""


def _sigs_from_write_resp(body: Any) -> set[str]:
    sigs: set[str] = set()
    if not isinstance(body, dict):
        return sigs

    def _collect(parent: Mapping[str, Any]) -> None:
        for bucket in ("movies", "shows"):
            value = parent.get(bucket)
            if isinstance(value, list):
                for item in value:
                    ids = _ids_filter((item.get("ids") or item) if isinstance(item, Mapping) else {})
                    sig = _id_sig(ids)
                    if sig:
                        sigs.add(sig)

    for top in ("added", "removed", "deleted"):
        section = body.get(top)
        if isinstance(section, Mapping):
            _collect(section)
    _collect(body)
    return sigs


def _mk_shadow_item(item: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    ids = _ids_filter(dict(item.get("ids") or {}))
    bucket = str(item.get("simkl_bucket") or "").strip().lower()
    if bucket not in {"movies", "shows", "anime"}:
        bucket = "movies" if str(item.get("type") or "").strip().lower() == "movie" else "shows"

    media_type = "movie" if str(item.get("type") or "").strip().lower() == "movie" else "show"
    m = {
        "type": media_type,
        "title": item.get("title"),
        "year": item.get("year"),
        "ids": ids,
        "simkl_bucket": bucket,
    }
    return simkl_key_of(m), m


def _shadow_add_items(items: Iterable[Mapping[str, Any]]) -> None:
    shadow = _shadow_load()
    current: dict[str, dict[str, Any]] = dict(shadow.get("items") or {})
    patch: dict[str, dict[str, Any]] = {}
    for item in items or []:
        key, mapped = _mk_shadow_item(item)
        patch[key] = mapped
    if patch:
        _merge_upsert(current, patch)
        _shadow_save(shadow.get("ts"), current, shadow.get("buckets_seen"))


def _shadow_remove_keys(keys: Iterable[str]) -> None:
    shadow = _shadow_load()
    current: dict[str, dict[str, Any]] = dict(shadow.get("items") or {})
    changed = False
    for key in list(keys or []):
        if key in current:
            del current[key]
            changed = True
    if changed:
        _shadow_save(shadow.get("ts"), current, shadow.get("buckets_seen"))



def _pull_bucket(
    adapter: Any,
    bucket: str,
    *,
    date_from: str | None,
    ids_only: bool,
    limit: int | None,
    force_refresh: bool = False,
) -> dict[str, dict[str, Any]]:
    session = adapter.client.session
    url_ids = URL_INDEX_IDS.format(bucket=bucket)
    url_full = URL_INDEX_BUCKET.format(bucket=bucket)
    base_params: dict[str, Any] = {"extended": "ids_only" if ids_only else "full"}
    if bucket == "anime":
        base_params["extended"] = "full_anime_seasons"
    if not ids_only and bucket in ("shows", "anime"):
        base_params["episode_watched_at"] = "yes"
    if date_from:
        base_params["date_from"] = date_from

    def _do_fetch(
        url: str,
        params: dict[str, Any],
        force: bool,
    ) -> dict[str, dict[str, Any]]:
        try:
            resp = session.get(
                url,
                headers=_headers(adapter, force_refresh=force),
                params=params,
                timeout=adapter.cfg.timeout,
            )
            if resp.status_code != 200:
                _warn("http_failed", op="index", bucket=bucket, method="GET", url=url, status=resp.status_code)
                return {}
            try:
                data = resp.json()
            except Exception:
                data = None
            rows = _rows_from_data(data, bucket)
            if not rows:
                return {}
            count = 0
            out_local: dict[str, dict[str, Any]] = {}
            for row in rows:
                try:
                    mapped = _normalize_row(bucket, row)
                    if not mapped.get("ids"):
                        continue
                    out_local[simkl_key_of(mapped)] = mapped
                    count += 1
                    if limit and count >= int(limit):
                        break
                except Exception:
                    continue
            return out_local
        except Exception as exc:
            _warn("http_failed", op="index", bucket=bucket, method="GET", url=url, error=str(exc))
            return {}

    first_url = url_ids if ids_only else url_full
    result = _do_fetch(first_url, dict(base_params), force_refresh)
    if not result:
        alt_params = dict(base_params)
        alt_params.pop("date_from", None)
        if ids_only:
            alt_params["extended"] = "full"
            alt_url = url_full
        else:
            alt_url = first_url
        result = _do_fetch(alt_url, alt_params, True)
    return result


def _pull_all_watchlist(
    adapter: Any,
    *,
    date_from: str | None = None,
    limit: int | None,
    force_refresh: bool = False,
) -> dict[str, dict[str, Any]]:
    session = adapter.client.session
    params: dict[str, Any] = {}
    if date_from:
        params["date_from"] = date_from

    try:
        resp = session.get(
            URL_INDEX_ALL,
            headers=_headers(adapter, force_refresh=force_refresh),
            params=params or None,
            timeout=adapter.cfg.timeout,
        )
        if resp.status_code != 200:
            _warn("http_failed", op="index", method="GET", url=URL_INDEX_ALL, status=resp.status_code)
            return {}
        try:
            data = resp.json()
        except Exception:
            data = None
    except Exception as exc:
        _warn("http_failed", op="index", method="GET", url=URL_INDEX_ALL, error=str(exc))
        return {}

    out: dict[str, dict[str, Any]] = {}
    count = 0
    for bucket in WATCHLIST_BUCKETS:
        for row in _rows_from_data(data, bucket):
            if isinstance(row, Mapping):
                status = str(row.get("status") or "").strip().lower()
                if status and status != "plantowatch":
                    continue
            try:
                mapped = _normalize_row(bucket, row)
            except Exception:
                continue
            if not mapped.get("ids"):
                continue
            out[simkl_key_of(mapped)] = mapped
            count += 1
            if limit and count >= int(limit):
                return out
    return out



def build_index(adapter: Any, limit: int | None = None) -> dict[str, dict[str, Any]]:
    prog_mk = getattr(adapter, "progress_factory", None)
    prog: Any = prog_mk("watchlist") if callable(prog_mk) else None
    done = 0
    total_known = 0
    session = adapter.client.session
    headers = _headers(adapter)
    acts, _rate = fetch_activities(session, headers, timeout=adapter.cfg.timeout)
    ts_map = _bucket_ts(acts or {})
    normalize_flat_watermarks()

    def _composite_ts(tsm: Mapping[str, Mapping[str, str | None]]) -> str:
        def v(x: str | None) -> str:
            return x or "-"
        return "|".join(
            [
                f"m:{v(tsm['movies']['ptw'])}/{v(tsm['movies']['rm'])}",
                f"s:{v(tsm['shows']['ptw'])}/{v(tsm['shows']['rm'])}",
                f"a:{v(tsm['anime']['ptw'])}/{v(tsm['anime']['rm'])}",
            ]
        )

    comp_ts = _composite_ts(ts_map)

    if os.getenv("CW_SIMKL_WATCHLIST_CLEAR") == "1":
        try:
            _shadow_path().unlink(missing_ok=True)
        except Exception:
            pass

    shadow = _shadow_load()
    buckets_seen: dict[str, Any] = dict(shadow.get("buckets_seen") or {})
    raw_items = shadow.get("items") or {}
    items: dict[str, dict[str, Any]] = {}
    if isinstance(raw_items, Mapping):
        for key, value in raw_items.items():
            if not isinstance(key, str):
                continue
            if not isinstance(value, Mapping):
                continue
            if value.get("simkl_bucket") not in WATCHLIST_BUCKETS:
                continue
            items[key] = dict(value)

    if comp_ts and shadow.get("ts") == comp_ts and _has_all_buckets(items, buckets_seen):
        age = _shadow_age_seconds()
        ttl = _shadow_ttl_seconds()
        if age <= ttl:
            if prog:
                try:
                    total = len(items)
                    prog.tick(0, total=total, force=True)
                    prog.tick(total, total=total)
                except Exception:
                    pass
            _dbg("index_cache_hit", source="shadow", reason="activities_unchanged", count=len(items), age_s=int(age))
            _info("index_done", count=len(items), source="shadow")
            return items
        _dbg("index_reconcile", reason="shadow_stale", strategy="aggregate", age_s=int(age), ttl_s=int(ttl))
        fresh = _pull_all_watchlist(
            adapter,
            date_from=None,
            limit=limit,
            force_refresh=True,
        )
        items = dict(fresh)
        buckets_seen = {bucket: True for bucket in WATCHLIST_BUCKETS}
        total_known = len(items)
        done = len(items)
        _unfreeze_if_present(items.keys())
        _shadow_save(comp_ts, items, buckets_seen)
        if prog:
            try:
                prog.tick(0, total=done, force=True)
                prog.tick(done, total=done)
            except Exception:
                pass
        _info("index_done", count=len(items), source="aggregate")
        return items

    watchlist_wm = get_watermark("watchlist")
    removed_wm = get_watermark("watchlist_removed")
    shadow_complete = _has_all_buckets(items, buckets_seen)
    cold_start = not items and not any(bool(v) for v in (buckets_seen or {}).values())
    removal_changed = bool(
        removed_wm
        and any(
            isinstance(ts_map[bucket]["rm"], str) and str(ts_map[bucket]["rm"]) > removed_wm
            for bucket in WATCHLIST_BUCKETS
        )
    )
    needs_full_reconcile = (
        cold_start
        or not watchlist_wm
        or not removed_wm
        or not shadow_complete
        or removal_changed
    )

    if needs_full_reconcile:
        reason = "cold_start"
        if not cold_start:
            if removal_changed:
                reason = "removed_from_list_changed"
            elif not watchlist_wm or not removed_wm:
                reason = "missing_watermarks"
            elif not shadow_complete:
                reason = "partial_shadow"
        _dbg("index_reconcile", reason=reason, strategy="aggregate")
        fresh = _pull_all_watchlist(
            adapter,
            date_from=None,
            limit=limit,
            force_refresh=True,
        )
        items = dict(fresh)
        buckets_seen = {bucket: True for bucket in WATCHLIST_BUCKETS}
        total_known = len(items)
        done = len(items)
        _unfreeze_if_present(items.keys())
        _shadow_save(comp_ts, items, buckets_seen)

        candidates: list[str] = [
            t
            for t in (ts_map["movies"]["ptw"], ts_map["shows"]["ptw"], ts_map["anime"]["ptw"])
            if isinstance(t, str)
        ]
        if candidates:
            save_watermark("watchlist", max(candidates))

        rm_candidates: list[str] = [
            t
            for t in (ts_map["movies"]["rm"], ts_map["shows"]["rm"], ts_map["anime"]["rm"])
            if isinstance(t, str)
        ]
        if rm_candidates:
            save_watermark("watchlist_removed", max(rm_candidates))

        if prog:
            try:
                prog.tick(0, total=done, force=True)
                prog.tick(done, total=done)
            except Exception:
                pass
        _info("index_done", count=len(items), source="aggregate")
        return items

    force_present = (os.getenv("CW_SIMKL_FORCE_PRESENT") or "").strip().lower()
    force_all = force_present in ("1", "true", "all")

    for bucket in WATCHLIST_BUCKETS:
        have_bucket = _bucket_ready(items, bucket, buckets_seen)
        ptw_ts = ts_map[bucket]["ptw"]
        rm_ts = ts_map[bucket]["rm"]
        rm_key = "watchlist_removed"
        prev_rm = removed_wm

        if force_all or force_present == bucket:
            _dbg("index_reconcile", bucket=bucket, reason="force_present", strategy="ids_only")
            df_force = coalesce_date_from(
                "watchlist",
                cfg_date_from="1970-01-01T00:00:00Z",
            )
            fresh = _pull_bucket(
                adapter,
                bucket,
                date_from=df_force,
                ids_only=True,
                limit=limit,
                force_refresh=True,
            )
            buckets_seen[bucket] = True
            _merge_upsert(items, fresh)
            buckets_seen[bucket] = True
            cnt = len(fresh)
            _dbg("index_reconcile", bucket=bucket, reason="force_present_done", strategy="ids_only", count=cnt)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
            have_bucket = _bucket_ready(items, bucket, buckets_seen)

        elif rm_ts and (not prev_rm or rm_ts > prev_rm):
            if have_bucket:
                drop = [
                    key
                    for key, value in items.items()
                    if isinstance(value, Mapping) and value.get("simkl_bucket") == bucket
                ]
                for key in drop:
                    items.pop(key, None)

            df_full = coalesce_date_from(
                "watchlist",
                cfg_date_from="1970-01-01T00:00:00Z",
            )
            fresh = _pull_bucket(
                adapter,
                bucket,
                date_from=None,
                ids_only=True,
                limit=limit,
                force_refresh=True,
            )
            buckets_seen[bucket] = True
            _merge_upsert(items, fresh)
            buckets_seen[bucket] = True
            cnt = len(fresh)
            _dbg("index_reconcile", bucket=bucket, reason="removed_from_list_changed", strategy="ids_only", count=cnt)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
            have_bucket = _bucket_ready(items, bucket, buckets_seen)

        df_key = "watchlist"
        date_from = coalesce_date_from(df_key)

        wm = watchlist_wm
        if ptw_ts and (wm is None or ptw_ts > wm):
            inc = _pull_bucket(
                adapter,
                bucket,
                date_from=date_from,
                ids_only=False,
                limit=limit,
                force_refresh=False,
            )
            buckets_seen[bucket] = True
            if not inc:
                _dbg("index_reconcile", bucket=bucket, reason="incremental_empty", strategy="ids_only")
                df_full = coalesce_date_from(
                    df_key,
                    cfg_date_from="1970-01-01T00:00:00Z",
                )
                inc = _pull_bucket(
                    adapter,
                    bucket,
                    date_from=df_full,
                    ids_only=True,
                    limit=limit,
                    force_refresh=True,
                )
                buckets_seen[bucket] = True

            _merge_upsert(items, inc)
            buckets_seen[bucket] = True
            _dbg("index_reconcile", bucket=bucket, reason="activity_delta", strategy="incremental", count=len(inc), date_from=date_from or "baseline")

            cnt = len(inc)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
            have_bucket = _bucket_ready(items, bucket, buckets_seen)

        if not have_bucket:
            _dbg("index_reconcile", bucket=bucket, reason="missing_shadow_bucket", strategy="full")
            df_full = coalesce_date_from(
                "watchlist",
                cfg_date_from="1970-01-01T00:00:00Z",
            )
            snap = _pull_bucket(
                adapter,
                bucket,
                date_from=df_full,
                ids_only=False,
                limit=limit,
                force_refresh=True,
            )
            buckets_seen[bucket] = True
            if not snap:
                snap = _pull_bucket(
                    adapter,
                    bucket,
                    date_from=None,
                    ids_only=False,
                    limit=limit,
                    force_refresh=True,
                )
                buckets_seen[bucket] = True

            _merge_upsert(items, snap)
            buckets_seen[bucket] = True

            cnt = len(snap)
            if cnt and prog:
                try:
                    total_known += cnt
                    done += cnt
                    prog.tick(done, total=total_known, force=(done == cnt))
                except Exception:
                    pass
    _unfreeze_if_present(items.keys())
    _shadow_save(comp_ts, items, buckets_seen)

    candidates: list[str] = [
        t
        for t in (ts_map["movies"]["ptw"], ts_map["shows"]["ptw"], ts_map["anime"]["ptw"])
        if isinstance(t, str)
    ]
    if candidates:
        latest_any = max(candidates)
        save_watermark("watchlist", latest_any)

    rm_candidates: list[str] = [
        t
        for t in (ts_map["movies"]["rm"], ts_map["shows"]["rm"], ts_map["anime"]["rm"])
        if isinstance(t, str)
    ]
    if rm_candidates:
        latest_rm = max(rm_candidates)
        save_watermark("watchlist_removed", latest_rm)

    if prog:
        try:
            final_total = len(items)
            prog.tick(done, total=final_total, force=True)
        except Exception:
            pass

    _info("index_done", count=len(items), source="mixed")
    return items


def _split_buckets(
    items: Iterable[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    movies: list[dict[str, Any]] = []
    shows: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    for item in items:
        ids = dict(item.get("ids") or {})
        body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
        if not body_ids:
            unresolved.append({"item": id_minimal(item), "hint": "missing_ids"})
            continue
        group = _kind_group(item)
        entry = {"ids": body_ids}
        if group == "movies":
            movies.append(entry)
        else:
            shows.append(entry)
    payload: dict[str, Any] = {}
    if movies:
        payload["movies"] = movies
    if shows:
        payload["shows"] = shows
    return payload, unresolved


def _chunk_items(seq: list[Mapping[str, Any]], n: int) -> Iterable[list[Mapping[str, Any]]]:
    size = max(1, int(n or 1))
    for i in range(0, len(seq), size):
        yield seq[i : i + size]


def add(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    items_list: list[Mapping[str, Any]] = list(items or [])
    if not items_list:
        _info("write_skipped", op="add", reason="empty_payload", unresolved=0)
        return 0, []
    batch = max(1, int(getattr(adapter.cfg, "watchlist_batch_size", 100) or 100))
    ok = 0
    unresolved: list[dict[str, Any]] = []
    for part in _chunk_items(items_list, batch):
        raw_payload, part_unresolved = _split_buckets(part)
        unresolved.extend(part_unresolved)
        if not raw_payload:
            continue
        body: dict[str, Any] = {}
        movies_payload = raw_payload.get("movies")
        shows_payload = raw_payload.get("shows")
        if movies_payload:
            body["movies"] = [
                {"ids": entry["ids"], "to": "plantowatch"} for entry in movies_payload
            ]
        if shows_payload:
            body["shows"] = [
                {"ids": entry["ids"], "to": "plantowatch"} for entry in shows_payload
            ]
        try:
            resp = session.post(
                URL_ADD,
                headers=headers,
                json=body,
                timeout=adapter.cfg.timeout,
            )
            content_type = resp.headers.get("Content-Type", "")
            resp_body: Any = (
                resp.json()
                if resp.text and "application/json" in content_type
                else {}
            )
            if 200 <= resp.status_code < 300:
                processed = _sum_processed_from_body(resp_body)
                if processed == 0:
                    for item in part:
                        ids = dict(item.get("ids") or {})
                        body_ids = {
                            k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v
                        }
                        if body_ids:
                            _freeze(
                                item,
                                action="add",
                                reasons=["not_processed"],
                                ids_sent=body_ids,
                            )
                    _warn("write_failed", op="add", status=resp.status_code, reason="not_processed", body=str(resp_body)[:180])
                ok += int(processed)
                if processed > 0:
                    sigs = _sigs_from_write_resp(resp_body)
                    if sigs:
                        to_add: list[Mapping[str, Any]] = []
                        for item in part:
                            sig = _id_sig(_ids_filter(item.get("ids") or {}))
                            if sig and sig in sigs:
                                to_add.append(item)
                        if to_add:
                            _shadow_add_items(to_add)
                    elif processed == len(part):
                        _shadow_add_items(part)
                    _unfreeze_if_present([simkl_key_of(id_minimal(it)) for it in part])
            else:
                _warn("write_failed", op="add", status=resp.status_code, body=(resp.text or '')[:180])
                for item in part:
                    ids = dict(item.get("ids") or {})
                    body_ids = {
                        k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v
                    }
                    if body_ids:
                        unresolved.append(
                            {
                                "item": id_minimal(item),
                                "hint": f"add_failed:{resp.status_code}",
                            }
                        )
                        _freeze(
                            item,
                            action="add",
                            reasons=[f"write_failed:{resp.status_code}"],
                            ids_sent=body_ids,
                        )
        except Exception as exc:
            _warn("write_failed", op="add", error=str(exc))
            for item in part:
                ids = dict(item.get("ids") or {})
                body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
                if body_ids:
                    unresolved.append(
                        {"item": id_minimal(item), "hint": "add_exception"},
                    )
                    _freeze(
                        item,
                        action="add",
                        reasons=["exception"],
                        ids_sent=body_ids,
                    )
    _info("write_done", op="add", ok=bool(items_list) and len(unresolved) == 0 and ok == len(items_list), applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(
    adapter: Any,
    items: Iterable[Mapping[str, Any]],
) -> tuple[int, list[dict[str, Any]]]:
    session = adapter.client.session
    headers = _headers(adapter)
    items_list: list[Mapping[str, Any]] = list(items or [])
    if not items_list:
        _info("write_skipped", op="remove", reason="empty_payload", unresolved=0)
        return 0, []
    batch = max(1, int(getattr(adapter.cfg, "watchlist_batch_size", 100) or 100))
    ok = 0
    unresolved: list[dict[str, Any]] = []
    for part in _chunk_items(items_list, batch):
        payload, part_unresolved = _split_buckets(part)
        unresolved.extend(part_unresolved)
        if not payload:
            continue
        try:
            resp = session.post(
                URL_REMOVE,
                headers=headers,
                json=payload,
                timeout=adapter.cfg.timeout,
            )
            content_type = resp.headers.get("Content-Type", "").lower()
            resp_body: Any = (
                resp.json()
                if resp.text and "application/json" in content_type
                else {}
            )
            if 200 <= resp.status_code < 300:
                processed = _sum_processed_from_body(resp_body)
                empty_success = not str(resp.text or "").strip()
                if processed == 0 and empty_success:
                    ok += len(part)
                    _shadow_remove_keys([_mk_shadow_item(item)[0] for item in part])
                    _unfreeze_if_present([simkl_key_of(id_minimal(it)) for it in part])
                    _dbg("write_assumed_success", op="remove", reason="empty_body", applied=len(part))
                else:
                    if processed == 0:
                        for item in part:
                            ids = dict(item.get("ids") or {})
                            body_ids = {
                                k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v
                            }
                            if body_ids:
                                unresolved.append(
                                    {"item": id_minimal(item), "hint": "not_removed"},
                                )
                                _freeze(
                                    item,
                                    action="remove",
                                    reasons=["not_processed"],
                                    ids_sent=body_ids,
                                )
                        _warn("write_failed", op="remove", status=resp.status_code, reason="not_processed", body=(str(resp_body)[:180] if resp_body else "empty"))
                    ok += int(processed)
                    if processed > 0:
                        sigs = _sigs_from_write_resp(resp_body)
                        if sigs:
                            rm_keys: list[str] = []
                            for item in part:
                                sig = _id_sig(_ids_filter(item.get("ids") or {}))
                                if sig and sig in sigs:
                                    rm_keys.append(_mk_shadow_item(item)[0])
                            if rm_keys:
                                _shadow_remove_keys(rm_keys)
                        elif processed == len(part):
                            _shadow_remove_keys([_mk_shadow_item(item)[0] for item in part])
                        _unfreeze_if_present([simkl_key_of(id_minimal(it)) for it in part])
            else:
                _warn("write_failed", op="remove", status=resp.status_code, body=(resp.text or '')[:180])
                for item in part:
                    ids = dict(item.get("ids") or {})
                    body_ids = {
                        k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v
                    }
                    if body_ids:
                        unresolved.append(
                            {
                                "item": id_minimal(item),
                                "hint": f"remove_failed:{resp.status_code}",
                            }
                        )
                        _freeze(
                            item,
                            action="remove",
                            reasons=[f"write_failed:{resp.status_code}"],
                            ids_sent=body_ids,
                        )
        except Exception as exc:
            _warn("write_failed", op="remove", error=str(exc))
            for item in part:
                ids = dict(item.get("ids") or {})
                body_ids = {k: v for k, v in ids.items() if k in _ALLOWED_ID_KEYS and v}
                if body_ids:
                    unresolved.append(
                        {"item": id_minimal(item), "hint": "remove_exception"},
                    )
                    _freeze(
                        item,
                        action="remove",
                        reasons=["exception"],
                        ids_sent=body_ids,
                    )
    _info("write_done", op="remove", ok=bool(items_list) and len(unresolved) == 0 and ok == len(items_list), applied=ok, unresolved=len(unresolved))
    return ok, unresolved

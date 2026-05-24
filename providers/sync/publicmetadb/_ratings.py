# /providers/sync/publicmetadb/_ratings.py
# PUBLICMETADB Module for ratings functions
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import time
from typing import Any, Iterable, Mapping, Protocol, cast

from cw_platform.id_map import canonical_key, minimal as id_minimal

from .._log import log as cw_log
from ._common import as_int, cfg_section, media_type_for_item, read_json, state_file, tmdb_id_for_item, write_json


def _dbg(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "ratings", "debug", event, **fields)


def _info(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "ratings", "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log("PUBLICMETADB", "ratings", "warn", event, **fields)


class _ResponseLike(Protocol):
    status_code: int
    text: str


def _shadow_path():
    return state_file("publicmetadb_ratings.shadow.json")


def _quota_path():
    return state_file("publicmetadb_ratings.quota.json")


def _shadow_load() -> dict[str, Any]:
    doc = read_json(_shadow_path())
    if not isinstance(doc.get("items"), dict):
        doc["items"] = {}
    return doc


def _shadow_save(items: Mapping[str, Any]) -> None:
    write_json(_shadow_path(), {"items": dict(items)})


def _quota_limit(adapter: Any, bucket: str) -> int:
    key = "ratings_update_per_hour" if bucket == "update" else "ratings_submit_per_hour"
    default = 100 if bucket == "update" else 200
    try:
        val = int(getattr(adapter.cfg, key, default) or default)
    except Exception:
        val = default
    ceiling = 100 if bucket == "update" else 200
    return max(1, min(val, ceiling))


def _quota_take(adapter: Any, bucket: str) -> bool:
    now = int(time.time())
    doc = read_json(_quota_path())
    if not isinstance(doc.get("buckets"), dict):
        doc["buckets"] = {}
    buckets: dict[str, Any] = dict(doc.get("buckets") or {})
    cur_obj = buckets.get(bucket)
    cur: Mapping[str, Any] = cur_obj if isinstance(cur_obj, Mapping) else {}
    start = int(cur.get("window_start") or now)
    count = int(cur.get("count") or 0)
    if now - start >= 3600:
        start = now
        count = 0
    limit = _quota_limit(adapter, bucket)
    if count >= limit:
        _warn("quota_limited", bucket=bucket, count=count, limit=limit)
        buckets[bucket] = {"window_start": start, "count": count, "limit": limit}
        write_json(_quota_path(), {"buckets": buckets})
        return False
    buckets[bucket] = {"window_start": start, "count": count + 1, "limit": limit}
    write_json(_quota_path(), {"buckets": buckets})
    return True


def _label(adapter: Any, item: Mapping[str, Any] | None = None) -> str:
    raw = (item or {}).get("label") or (item or {}).get("rating_label")
    if not raw:
        raw = cfg_section(adapter).get("ratings_label") or "Overall"
    label = str(raw or "").strip()
    return label or "Overall"


def _item_key(item: Mapping[str, Any], *, label: str | None = None) -> str:
    base = canonical_key(id_minimal(item))
    lab = str(label or item.get("label") or item.get("rating_label") or "Overall").strip().lower()
    return f"{base}#rating:{lab or 'overall'}"


def _show_tmdb_for_item(item: Mapping[str, Any]) -> int | None:
    show_ids_obj = item.get("show_ids")
    show_ids: Mapping[str, Any] = show_ids_obj if isinstance(show_ids_obj, Mapping) else {}
    return as_int(show_ids.get("tmdb")) or tmdb_id_for_item(item)


def _valid_rating(value: Any) -> int | None:
    try:
        n = float(str(value).strip())
    except Exception:
        return None
    if n > 10 and n <= 100:
        n = n / 10.0
    i = int(round(n))
    return i if 1 <= i <= 10 else None


def _score_from_rating(value: Any) -> int | None:
    rating = _valid_rating(value)
    if rating is None:
        return None
    return max(0, min(100, int(rating) * 10))


def _rating_from_score(value: Any) -> int | None:
    try:
        score = float(str(value).strip())
    except Exception:
        return None
    if score < 0 or score > 100:
        return None
    return max(1, min(10, int(round(score / 10.0))))


def _rating_id(item: Mapping[str, Any]) -> str | None:
    rid = str(item.get("_publicmetadb_rating_id") or item.get("rating_id") or "").strip()
    return rid or None


def _to_minimal(row: Mapping[str, Any], *, episode_rating: bool) -> dict[str, Any] | None:
    tmdb = tmdb_id_for_item(row)
    rating = _rating_from_score(row.get("score") or row.get("rating"))
    if tmdb is None or rating is None:
        return None
    label = str(row.get("label") or "Overall").strip() or "Overall"
    rid = str(row.get("id") or row.get("rating_id") or "").strip()

    if episode_rating:
        season = as_int(row.get("season") or row.get("season_number"))
        episode = as_int(row.get("episode") or row.get("episode_number"))
        if season is None or episode is None:
            return None
        out: dict[str, Any] = {
            "type": "episode",
            "show_ids": {"tmdb": str(tmdb)},
            "season": season,
            "episode": episode,
            "rating": rating,
            "label": label,
        }
    else:
        media = str(row.get("media_type") or row.get("type") or "").strip().lower()
        typ = "show" if media == "tv" else "movie"
        out = {"type": typ, "ids": {"tmdb": str(tmdb)}, "rating": rating, "label": label}

    rated_at = row.get("rated_at") or row.get("created") or row.get("created_at")
    if rated_at:
        out["rated_at"] = str(rated_at)
    mini = id_minimal(out)
    if rid:
        mini["rating_id"] = rid
        mini["_publicmetadb_rating_id"] = rid
    mini["label"] = label
    return mini


def build_index(adapter: Any) -> dict[str, dict[str, Any]]:
    shadow = _shadow_load()
    raw: dict[str, Any] = dict(shadow.get("items") or {})
    out: dict[str, dict[str, Any]] = {}
    remote_ids: dict[str, Any] = {}
    for key, entry in raw.items():
        if isinstance(entry, Mapping):
            item_obj = entry.get("item")
            item: Mapping[str, Any] = item_obj if isinstance(item_obj, Mapping) else entry
            mini = id_minimal(item)
            if "rating" not in mini:
                continue
            label = str(entry.get("label") or item.get("label") or "Overall").strip() or "Overall"
            mini["label"] = label
            rid = str(entry.get("id") or item.get("_publicmetadb_rating_id") or item.get("rating_id") or "").strip()
            if rid:
                mini["rating_id"] = rid
                mini["_publicmetadb_rating_id"] = rid
            k = _item_key(mini, label=label)
            out[k] = mini
            remote_ids[k] = {"id": rid, "label": label, "item": mini}
        elif isinstance(entry, str) and entry:
            remote_ids[str(key)] = {"id": entry, "label": "Overall", "item": {}}
    if remote_ids != raw:
        _shadow_save(remote_ids)
    _info("index_done", count=len(out), source="shadow")
    return out


def _payload_for_item(adapter: Any, item: Mapping[str, Any]) -> tuple[str | None, dict[str, Any] | None, str | None, str | None]:
    mini = id_minimal(item)
    rating = _score_from_rating(mini.get("rating") if "rating" in mini else item.get("rating"))
    if rating is None:
        return None, None, None, "missing_rating"
    label = _label(adapter, item)
    typ = str(mini.get("type") or item.get("type") or "").strip().lower()

    if typ == "episode":
        tmdb = _show_tmdb_for_item(mini) or _show_tmdb_for_item(item)
        season = as_int(mini.get("season") or item.get("season"))
        episode = as_int(mini.get("episode") or item.get("episode"))
        if tmdb is None or season is None or episode is None:
            return None, None, label, "missing_show_tmdb_or_episode_numbers"
        return (
            "/api/external/episode-ratings",
            {
                "tmdb_id": int(tmdb),
                "media_type": "tv",
                "season": int(season),
                "episode": int(episode),
                "score": rating,
                "label": label,
            },
            label,
            None,
        )

    tmdb = tmdb_id_for_item(mini) or tmdb_id_for_item(item)
    if tmdb is None:
        return None, None, label, "missing_tmdb_id"
    media = media_type_for_item(mini)
    if media not in ("movie", "tv"):
        media = "movie"
    return (
        "/api/external/ratings",
        {"tmdb_id": int(tmdb), "media_type": media, "score": rating, "label": label},
        label,
        None,
    )


def add(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    items_list = list(items or [])
    shadow = _shadow_load()
    remote_ids: dict[str, Any] = dict(shadow.get("items") or {})
    unresolved: list[dict[str, Any]] = []
    ok = 0

    for it in items_list:
        mini = id_minimal(it)
        path, payload, label, hint = _payload_for_item(adapter, it)
        if payload is None or path is None:
            unresolved.append({"item": mini, "hint": hint or "unsupported_rating_item"})
            continue
        key = _item_key(mini, label=label)
        prior = remote_ids.get(key)
        prior_id = str(prior.get("id") or "").strip() if isinstance(prior, Mapping) else ""
        quota_bucket = "update" if prior_id else "submit"
        if not _quota_take(adapter, quota_bucket):
            unresolved.append({"item": mini, "hint": f"publicmetadb_hourly_rating_{quota_bucket}_limit"})
            continue
        post = getattr(adapter.client, "post_once", None)
        r = cast(_ResponseLike, post(path, json=payload) if callable(post) else adapter.client.post(path, json=payload))
        if 200 <= r.status_code < 300:
            data = adapter.client.safe_json(r)
            item = data.get("item") if isinstance(data, Mapping) else None
            rid = str(item.get("id") or "").strip() if isinstance(item, Mapping) else ""
            accepted = dict(mini)
            accepted["rating"] = _valid_rating(mini.get("rating") or it.get("rating"))
            accepted["label"] = label or "Overall"
            if rid:
                accepted["rating_id"] = rid
                accepted["_publicmetadb_rating_id"] = rid
            remote_ids[key] = {"id": rid, "label": label or "Overall", "item": accepted}
            if prior_id and prior_id != rid:
                old_path = "/api/external/episode-ratings" if str(mini.get("type") or "").lower() == "episode" else "/api/external/ratings"
                old = cast(_ResponseLike, adapter.client.delete(f"{old_path}/{prior_id}"))
                if not (200 <= old.status_code < 300):
                    _warn("write_failed", op="replace_delete_old", status=old.status_code, body=(old.text or "")[:200])
            ok += 1
        else:
            _warn("write_failed", op="add", status=r.status_code, body=(r.text or "")[:200])
            unresolved.append({"item": mini, "hint": f"http:{r.status_code}"})
    _shadow_save(remote_ids)
    _info("write_done", op="add", applied=ok, unresolved=len(unresolved))
    return ok, unresolved


def remove(adapter: Any, items: Iterable[Mapping[str, Any]]) -> tuple[int, list[dict[str, Any]]]:
    items_list = list(items or [])
    shadow = _shadow_load()
    remote_ids: dict[str, Any] = dict(shadow.get("items") or {})
    unresolved: list[dict[str, Any]] = []
    ok = 0

    for it in items_list:
        mini = id_minimal(it)
        label = _label(adapter, it)
        key = _item_key(mini, label=label)
        entry = remote_ids.get(key)
        rating_id = _rating_id(it) or _rating_id(mini)
        if not rating_id and isinstance(entry, Mapping):
            rating_id = str(entry.get("id") or "").strip() or None
        if not rating_id:
            unresolved.append({"item": mini, "hint": "missing_remote_rating_id"})
            continue
        path = "/api/external/episode-ratings" if str(mini.get("type") or "").lower() == "episode" else "/api/external/ratings"
        r = cast(_ResponseLike, adapter.client.delete(f"{path}/{rating_id}"))
        if 200 <= r.status_code < 300:
            remote_ids.pop(key, None)
            ok += 1
        else:
            _warn("write_failed", op="remove", status=r.status_code, body=(r.text or "")[:200])
            unresolved.append({"item": mini, "hint": f"http:{r.status_code}"})
    _shadow_save(remote_ids)
    _info("write_done", op="remove", applied=ok, unresolved=len(unresolved))
    return ok, unresolved

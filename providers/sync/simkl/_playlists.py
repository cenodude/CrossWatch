from __future__ import annotations

from typing import Any, Iterable, Mapping, Sequence

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PlaylistItem, PlaylistResource, PlaylistSnapshot

from .._log import log as cw_log
from ._common import (
    SIMKLFetchError,
    adapter_headers,
    extract_latest_ts,
    fetch_activities,
    simkl_api_params_from_headers,
    key_of as simkl_key_of,
)
from . import _watchlist as feat_watchlist

_PROVIDER = "SIMKL"
_FEATURE = "playlists"
BASE = "https://api.simkl.com"
URL_STATUS = f"{BASE}/sync/all-items/{{bucket}}/{{status}}"
URL_ADD = f"{BASE}/sync/add-to-list"
URL_REMOVE = f"{BASE}/sync/history/remove"
SIMKL_STATUS_WARNING = "SIMKL Custom Lists are not supported. These endpoints use SIMKL's built in status buckets, which are not true playlists. Changes may move or remove items from your SIMKL library. Use with caution."
SIMKL_REMOVE_WARNING = "Removing from a SIMKL status bucket removes the item from the full SIMKL library and clears its SIMKL rating."

_STATUS_ROWS: tuple[tuple[str, str, tuple[str, ...], tuple[str, ...]], ...] = (
    ("plantowatch", "Plan to Watch", ("movie", "show", "anime"), ("movies", "shows", "anime")),
    ("watching", "Watching", ("show", "anime"), ("shows", "anime")),
    ("hold", "On Hold", ("show", "anime"), ("shows", "anime")),
    ("dropped", "Dropped", ("movie", "show", "anime"), ("movies", "shows", "anime")),
    ("completed", "Completed", ("movie", "show", "anime"), ("movies", "shows", "anime")),
)
_STATUS_META = {status: {"label": label, "media_types": media, "buckets": buckets} for status, label, media, buckets in _STATUS_ROWS}
_MOVIE_BLOCKED_STATUSES = {"watching", "hold"}


class SIMKLPlaylistError(RuntimeError):
    pass


class SIMKLPlaylistNotFound(SIMKLPlaylistError):
    pass


def _info(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    cw_log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _instance_id(adapter: Any) -> str:
    inst = getattr(adapter, "instance_id", None)
    return str(inst).strip() if inst else "default"


def _status(value: Any) -> str:
    raw = value.id if isinstance(value, PlaylistResource) else value
    status = str(raw or "").strip().lower()
    if status not in _STATUS_META:
        raise SIMKLPlaylistNotFound("unknown simkl status bucket")
    return status


def _resource(adapter: Any, status: str) -> PlaylistResource:
    meta = _STATUS_META[_status(status)]
    return PlaylistResource(
        provider=_PROVIDER,
        id=status,
        name=str(meta["label"]),
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=True,
        can_remove=True,
        can_reorder=False,
        media_types=tuple(meta["media_types"]),
        extra={
            "endpoint_type": "status_bucket",
            "status": status,
            "fixed": True,
            "can_create": False,
            "can_rename": False,
            "can_delete": False,
            "custom_lists_supported": False,
            "destructive_remove": True,
            "remove_warning": SIMKL_REMOVE_WARNING,
            "warnings": [SIMKL_STATUS_WARNING],
        },
    )


def list_resources(adapter: Any) -> list[PlaylistResource]:
    out = [_resource(adapter, status) for status, _label, _media, _buckets in _STATUS_ROWS]
    _info("list_resources_done", count=len(out))
    return out


def _response_json(resp: Any) -> Any:
    try:
        return resp.json() if getattr(resp, "text", "") else {}
    except Exception:
        return {}


def _read_bucket(adapter: Any, bucket: str, status: str) -> list[PlaylistItem]:
    headers = adapter_headers(adapter)
    params = simkl_api_params_from_headers(headers, extended="full")
    if bucket == "anime":
        params["extended"] = "full_anime_seasons"
    if bucket in ("shows", "anime"):
        params["episode_watched_at"] = "yes"
    url = URL_STATUS.format(bucket=bucket, status=status)
    resp = adapter.client.session.get(url, headers=headers, params=params, timeout=getattr(adapter.cfg, "timeout", 15.0))
    if not (200 <= int(getattr(resp, "status_code", 0) or 0) < 300):
        raise SIMKLFetchError(f"simkl playlist read failed: {getattr(resp, 'status_code', 0)}")
    rows = feat_watchlist._rows_from_data(_response_json(resp), bucket)
    items: list[PlaylistItem] = []
    for row in rows:
        media = feat_watchlist._normalize_row(bucket, row)
        if not media:
            continue
        media["simkl_bucket"] = bucket
        media["simkl_status"] = status
        item = PlaylistItem.from_media(
            media,
            position=None,
            provider_media_id=str((media.get("ids") or {}).get("simkl") or ""),
            playlist_item_id=str((media.get("ids") or {}).get("simkl") or ""),
        )
        item.item["simkl_bucket"] = bucket
        item.item["simkl_status"] = status
        if item.key:
            items.append(item)
    return items


def _checkpoint(adapter: Any, status: str) -> str | None:
    try:
        headers = adapter_headers(adapter)
        acts, _rate = fetch_activities(adapter.client.session, headers, timeout=getattr(adapter.cfg, "timeout", 15.0))
        if not isinstance(acts, Mapping):
            return None
        paths = []
        for bucket in _STATUS_META[status]["buckets"]:
            paths.append((str(bucket), status))
        return extract_latest_ts(acts, paths)
    except Exception:
        return None


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    status = _status(playlist_id)
    items: list[PlaylistItem] = []
    seen: set[str] = set()
    for bucket in _STATUS_META[status]["buckets"]:
        for item in _read_bucket(adapter, bucket, status):
            if item.key in seen:
                continue
            seen.add(item.key)
            items.append(item)
    _info("snapshot_done", status=status, count=len(items))
    return PlaylistSnapshot(resource=_resource(adapter, status), items=items, checkpoint=_checkpoint(adapter, status))


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    raise SIMKLPlaylistError("SIMKL status bucket endpoints cannot be created")


def _item_bucket(item: Mapping[str, Any]) -> str:
    bucket = str(item.get("simkl_bucket") or "").strip().lower()
    if bucket in ("movies", "shows", "anime"):
        return bucket
    typ = str(item.get("type") or "").strip().lower()
    if typ == "movie":
        return "movies"
    if typ == "anime":
        return "anime"
    raw_ids = item.get("ids")
    ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
    if any(ids.get(k) for k in ("mal", "anilist", "kitsu", "anidb")):
        return "anime"
    return "shows"


def _ids(media: Mapping[str, Any]) -> dict[str, Any]:
    raw = media.get("ids") if isinstance(media.get("ids"), Mapping) else {}
    return feat_watchlist._ids_filter(raw or {})


def _accepted_items(items: Sequence[Mapping[str, Any]], status: str) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    body: dict[str, list[dict[str, Any]]] = {"movies": [], "shows": []}
    accepted: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        bucket = _item_bucket(raw)
        media = id_minimal(raw)
        if bucket in ("movies", "shows", "anime"):
            media["simkl_bucket"] = bucket
        if raw.get("simkl_status"):
            media["simkl_status"] = raw.get("simkl_status")
        if bucket == "movies" and status in _MOVIE_BLOCKED_STATUSES:
            unresolved.append({"item": media, "hint": "unsupported_media_type"})
            continue
        ids = _ids(media)
        if not ids:
            unresolved.append({"item": media, "hint": "missing_simkl_supported_id"})
            continue
        key = canonical_key(media)
        if not key or key in seen:
            continue
        seen.add(key)
        entry = {"ids": ids, "to": status}
        body["movies" if bucket == "movies" else "shows"].append(entry)
        accepted.append({"key": key, "item": media, "ids": ids, "bucket": bucket})
    return {k: v for k, v in body.items() if v}, accepted, unresolved


def _id_sig(ids: Mapping[str, Any]) -> str:
    return feat_watchlist._id_sig(ids)


def _walk_mappings(obj: Any) -> Iterable[Mapping[str, Any]]:
    if isinstance(obj, Mapping):
        yield obj
        for value in obj.values():
            yield from _walk_mappings(value)
    elif isinstance(obj, list):
        for value in obj:
            yield from _walk_mappings(value)


def _response_status_by_sig(body: Any) -> dict[str, str]:
    out: dict[str, str] = {}
    for row in _walk_mappings(body):
        ids = row.get("ids") if isinstance(row.get("ids"), Mapping) else None
        if not ids:
            continue
        status = str(row.get("status") or row.get("to") or row.get("watchlist_status") or row.get("list") or row.get("result_status") or "").strip().lower()
        if status:
            out[_id_sig(ids)] = status
    return out


def _confirmed_from_response(body: Any, accepted: Sequence[Mapping[str, Any]], status: str) -> tuple[list[str], list[dict[str, Any]]]:
    processed = feat_watchlist._sigs_from_write_resp(body)
    status_by_sig = _response_status_by_sig(body)
    processed_count = feat_watchlist._sum_processed_from_body(body)
    confirmed: list[str] = []
    unresolved: list[dict[str, Any]] = []
    for row in accepted:
        raw_ids = row.get("ids")
        ids: Mapping[str, Any] = raw_ids if isinstance(raw_ids, Mapping) else {}
        sig = _id_sig(ids)
        actual = status_by_sig.get(sig, "")
        if actual and actual != status:
            unresolved.append({"item": row.get("item") or {}, "hint": f"status_rewritten:{actual}", "actual_status": actual})
            continue
        if processed and sig not in processed:
            unresolved.append({"item": row.get("item") or {}, "hint": "not_confirmed"})
            continue
        if not processed and processed_count and len(accepted) != processed_count:
            unresolved.append({"item": row.get("item") or {}, "hint": "not_confirmed"})
            continue
        confirmed.append(str(row.get("key") or ""))
    return [k for k in confirmed if k], unresolved


def _write_status_shadow(status: str, accepted: Sequence[Mapping[str, Any]], confirmed: Sequence[str]) -> None:
    rows = [dict(x.get("item") or {}) for x in accepted if str(x.get("key") or "") in set(confirmed)]
    if not rows:
        return
    if status == "plantowatch":
        try:
            feat_watchlist._shadow_add_items(rows)
        except Exception:
            pass
    else:
        try:
            feat_watchlist._shadow_remove_keys(confirmed)
        except Exception:
            pass
    try:
        feat_watchlist._unfreeze_if_present(confirmed)
    except Exception:
        pass


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    status = _status(playlist_id)
    body, accepted, unresolved = _accepted_items(list(items or []), status)
    if not accepted:
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": []}
    headers = adapter_headers(adapter)
    resp = adapter.client.session.post(URL_ADD, headers=headers, params=simkl_api_params_from_headers(headers), json=body, timeout=getattr(adapter.cfg, "timeout", 15.0))
    data = _response_json(resp)
    if not (200 <= int(getattr(resp, "status_code", 0) or 0) < 300):
        unresolved.extend({"item": row.get("item") or {}, "hint": f"http:{getattr(resp, 'status_code', 0)}"} for row in accepted)
        return {"ok": False, "count": 0, "unresolved": unresolved, "confirmed_keys": []}
    confirmed, rewritten = _confirmed_from_response(data, accepted, status)
    unresolved.extend(rewritten)
    _write_status_shadow(status, accepted, confirmed)
    _info("write_done", op="add", status=status, applied=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "count": len(confirmed), "unresolved": unresolved, "confirmed_keys": confirmed}


def _remove_payload(items: Sequence[Mapping[str, Any]], status: str) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]], list[dict[str, Any]]]:
    body, accepted, unresolved = _accepted_items(items, status)
    for rows in body.values():
        for row in rows:
            row.pop("to", None)
    return body, accepted, unresolved


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    status = _status(playlist_id)
    body, accepted, unresolved = _remove_payload(list(items or []), status)
    keys = [str(x.get("key") or "") for x in accepted if x.get("key")]
    if not keys:
        return {"ok": True, "count": 0, "unresolved": unresolved, "confirmed_keys": [], "warnings": [SIMKL_REMOVE_WARNING]}
    headers = adapter_headers(adapter)
    resp = adapter.client.session.post(URL_REMOVE, headers=headers, params=simkl_api_params_from_headers(headers), json=body, timeout=getattr(adapter.cfg, "timeout", 15.0))
    data = _response_json(resp)
    if not (200 <= int(getattr(resp, "status_code", 0) or 0) < 300):
        unresolved.extend({"item": {"key": key}, "hint": f"http:{getattr(resp, 'status_code', 0)}"} for key in keys)
        return {"ok": False, "count": 0, "unresolved": unresolved, "confirmed_keys": [], "warnings": [SIMKL_REMOVE_WARNING]}
    processed = feat_watchlist._sigs_from_write_resp(data)
    processed_count = feat_watchlist._sum_processed_from_body(data)
    confirmed = keys if not processed and (processed_count == 0 or processed_count == len(keys)) else []
    if processed and accepted:
        by_sig = {_id_sig(x.get("ids") or {}): str(x.get("key") or "") for x in accepted if isinstance(x.get("ids"), Mapping)}
        confirmed = [key for sig, key in by_sig.items() if sig in processed and key]
    try:
        feat_watchlist._shadow_remove_keys(confirmed)
        feat_watchlist._unfreeze_if_present(confirmed)
    except Exception:
        pass
    _info("write_done", op="remove", status=status, applied=len(confirmed), unresolved=len(unresolved))
    return {"ok": True, "count": len(confirmed), "unresolved": unresolved, "confirmed_keys": confirmed, "warnings": [SIMKL_REMOVE_WARNING]}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}

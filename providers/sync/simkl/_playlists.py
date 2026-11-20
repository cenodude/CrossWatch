from __future__ import annotations

import copy
import time
from typing import Any, Iterable, Mapping, Sequence
from urllib.parse import unquote, urlparse

from .._pagination import PageGuard, page_numbers

from cw_platform.id_map import canonical_key, minimal as id_minimal, typed_keys_for_item
from cw_platform.playlists import PLAYLIST_KIND_REGULAR, PlaylistAuthError, PlaylistItem, PlaylistLinkError, PlaylistPlanError, PlaylistReadOnlyError, PlaylistResource, PlaylistSnapshot, PlaylistUnavailableError

from .._log import log as cw_log
from ._common import (
    SIMKLFetchError,
    adapter_headers,
    extract_latest_ts,
    fetch_activities,
    fetch_user_settings,
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
SIMKL_STATUS_WARNING = "SIMKL status buckets are not true playlists. Changes may move or remove items from your SIMKL library. Use with caution. SIMKL custom lists need SIMKL PRO or VIP."
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
URL_USER_LISTS = f"{BASE}/lists/user/{{user_id}}"
URL_LIST = f"{BASE}/lists/{{list_id}}"
URL_LISTS = f"{BASE}/lists"
URL_LIST_ITEMS = f"{BASE}/lists/{{list_id}}/items"
URL_LIST_ITEMS_REMOVE = f"{BASE}/lists/{{list_id}}/items/remove"
CUSTOM_PREFIX = "list:"
SIMKL_CUSTOM_WARNING = "This SIMKL list is read only. Use it as the source of a mapping."
_CUSTOM_TIERS = {"pro", "vip"}
_CUSTOM_MEDIA = {"movies": ("movies", "movie"), "tv": ("shows", "show"), "anime": ("anime", "anime")}
_CREATE_MEDIA = {"movie": "movies", "movies": "movies", "show": "tv", "shows": "tv", "tv": "tv", "anime": "anime"}
_CUSTOM_PAGE = 500
_WRITE_CHUNK = 1000
_WRITE_RETRIES = 3
_REAUTH_ERRORS = {"oauth2_token_required", "user_token_required", "insufficient_scope"}
_LIMIT_ERRORS = {"max_items", "max_lists", "max_items_total"}
_DENIED_ERRORS = {"no_access", "list_error", "private_list"}
_SIMKL_HOSTS = {"simkl.com", "www.simkl.com"}
_ROLE_TYPES = {"follower": "followed_list", "collaborator": "collaboration"}
_SNAPSHOT_MEMO_TTL = 6 * 3600.0
_SNAPSHOT_MEMO_MAX = 16
_SNAPSHOT_MEMO: dict[tuple[str, str], tuple[float, tuple[str, Any], list[PlaylistItem]]] = {}


class SIMKLPlaylistError(RuntimeError):
    pass


class SIMKLPlaylistNotFound(SIMKLPlaylistError):
    pass


class SIMKLListUnavailable(SIMKLPlaylistNotFound, PlaylistUnavailableError):
    pass


class SIMKLPremiumRequired(SIMKLPlaylistError, PlaylistPlanError):
    pass


class SIMKLListsNotEnabled(SIMKLPlaylistError):
    pass


class SIMKLListLimitReached(SIMKLPlaylistError, PlaylistPlanError):
    pass


class SIMKLListReconnectRequired(SIMKLPlaylistError, PlaylistAuthError):
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
            "custom_lists_supported": True,
            "can_edit_privacy": False,
            "destructive_remove": True,
            "remove_warning": SIMKL_REMOVE_WARNING,
            "warnings": [SIMKL_STATUS_WARNING],
        },
    )


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _custom_id(value: Any) -> str:
    raw = str(value.id if isinstance(value, PlaylistResource) else value or "").strip().lower()
    if not raw.startswith(CUSTOM_PREFIX):
        return ""
    list_id = raw[len(CUSTOM_PREFIX):].strip()
    return list_id if list_id.isdigit() else ""


def _account(adapter: Any) -> tuple[str, str]:
    settings = fetch_user_settings(adapter.client.session, adapter_headers(adapter), timeout=getattr(adapter.cfg, "timeout", 15.0))
    account = settings.get("account") if isinstance(settings, Mapping) else None
    if not isinstance(account, Mapping):
        return "", ""
    return str(account.get("id") or "").strip(), str(account.get("type") or "").strip().lower()


def _custom_get(adapter: Any, url: str, **query: Any) -> Mapping[str, Any]:
    headers = adapter_headers(adapter)
    params = simkl_api_params_from_headers(headers, **query)
    resp = adapter.client.session.get(url, headers=headers, params=params, timeout=getattr(adapter.cfg, "timeout", 15.0))
    code = int(getattr(resp, "status_code", 0) or 0)
    if code == 404:
        raise SIMKLListUnavailable("The SIMKL list no longer exists")
    error = str(_mapping(_response_json(resp)).get("error") or "") if code in (401, 403) else ""
    if code == 403 and error == "private_list":
        raise SIMKLListUnavailable("The SIMKL list is private")
    if error in ("oauth2_token_required", "user_token_required"):
        raise SIMKLListsNotEnabled(f"simkl custom lists need an AUTH V2 user token: {error}")
    if not (200 <= code < 300):
        raise SIMKLFetchError(f"simkl custom list read failed: {code}")
    data = _response_json(resp)
    if not isinstance(data, Mapping):
        raise SIMKLFetchError("simkl custom list read returned no data")
    if data.get("error"):
        if str(data.get("error")) == "premium_only":
            raise SIMKLPremiumRequired("SIMKL custom lists need SIMKL PRO or VIP")
        raise SIMKLPlaylistError(f"simkl custom list error: {data.get('error')}")
    return data


def _endpoint_type(row: Mapping[str, Any], me: str) -> str:
    role = str(row.get("role") or "").strip().lower()
    if role in _ROLE_TYPES:
        return _ROLE_TYPES[role]
    if role == "owner":
        return "custom_list"
    owner_id = str(_mapping(row.get("user")).get("id") or "").strip()
    return "public_list" if me and owner_id and owner_id != me else "custom_list"


def _custom_resource(adapter: Any, row: Mapping[str, Any], *, me: str = "") -> PlaylistResource | None:
    list_id = str(row.get("id") or "").strip()
    media = _CUSTOM_MEDIA.get(str(row.get("media_type") or "").strip().lower())
    if not list_id.isdigit() or not media:
        return None
    counts = _mapping(row.get("counts"))
    endpoint_type = _endpoint_type(row, me)
    owner = str(_mapping(row.get("user")).get("name") or "").strip()
    name = str(row.get("name") or list_id)
    shared: dict[str, Any] = {}
    owner_id = str(_mapping(row.get("user")).get("id") or "").strip()
    if not str(row.get("role") or "").strip() and not (me and owner_id == me):
        shared["unresolved"] = True
    if endpoint_type != "custom_list":
        shared["owner"] = owner
        if endpoint_type != "collaboration":
            shared["public_list"] = True
    list_type = str(row.get("type") or "").strip().lower()
    writable = endpoint_type == "custom_list" and list_type == "regular" and not shared.get("unresolved")
    return PlaylistResource(
        provider=_PROVIDER,
        id=f"{CUSTOM_PREFIX}{list_id}",
        name=f"{name} by {owner}" if endpoint_type != "custom_list" and owner else name,
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=True,
        can_add=writable,
        can_remove=writable,
        can_reorder=writable,
        media_types=(media[1],),
        extra={
            **shared,
            "endpoint_type": endpoint_type,
            "raw_id": list_id,
            "list_type": str(row.get("type") or ""),
            "privacy": str(row.get("privacy") or ""),
            "item_count": counts.get("items"),
            "updated_at": row.get("updated_at"),
            "fixed": not writable,
            "read_only": not writable,
            "can_create": False,
            "can_rename": writable,
            "can_delete": writable,
            "warnings": [] if writable else [SIMKL_CUSTOM_WARNING],
        },
    )


def _me(adapter: Any) -> tuple[str, str]:
    cached = getattr(adapter, "_simkl_custom_account", None)
    if isinstance(cached, tuple):
        return cached
    account = _account(adapter)
    try:
        adapter._simkl_custom_account = account
    except Exception:
        pass
    return account


def _user_lists(adapter: Any, user_id: str, me: str, **flags: Any) -> list[PlaylistResource]:
    out: list[PlaylistResource] = []
    guard = PageGuard(SIMKLFetchError, "simkl custom lists repeated page")
    for page in page_numbers():
        data = _custom_get(adapter, URL_USER_LISTS.format(user_id=user_id), limit=_CUSTOM_PAGE, page=page, **flags)
        rows = data.get("lists")
        pagination = _mapping(data.get("pagination"))
        if rows is None and page == 1 and int(pagination.get("total_pages") or 0) <= 1 and not int(pagination.get("total_items") or 0):
            rows = []
        if not isinstance(rows, list):
            raise SIMKLFetchError("simkl custom lists invalid page")
        guard.check(rows)
        if not rows and (page > 1 or page < int(pagination.get("total_pages") or 1) or int(pagination.get("total_items") or 0) > 0):
            raise SIMKLFetchError("simkl custom lists incomplete page")
        for row in rows:
            resource = _custom_resource(adapter, row, me=me) if isinstance(row, Mapping) else None
            if resource:
                out.append(resource)
        if page >= int(pagination.get("total_pages") or 1):
            break
    return out


def _custom_resources(adapter: Any) -> list[PlaylistResource]:
    user_id, tier = _me(adapter)
    if not user_id:
        _warn("custom_lists_failed", error="account_unavailable")
        raise SIMKLFetchError("simkl account settings unavailable")
    if tier not in _CUSTOM_TIERS:
        _info("custom_lists_skipped", reason="account_tier", tier=tier)
        return []
    try:
        out = _user_lists(adapter, user_id, user_id)
    except (SIMKLPremiumRequired, SIMKLListsNotEnabled) as e:
        _info("custom_lists_skipped", reason=type(e).__name__)
        return []
    except Exception as e:
        _warn("custom_lists_failed", error=str(e))
        raise
    known = {res.id for res in out}
    for flag in ("followed", "collaborants"):
        try:
            extra = _user_lists(adapter, user_id, user_id, **{flag: "true"})
        except Exception as e:
            _warn("custom_lists_failed", scope=flag, error=str(e))
            continue
        for res in extra:
            if res.id not in known:
                known.add(res.id)
                out.append(res)
    return out


def _snapshot_stamp(head: Mapping[str, Any]) -> tuple[str, Any] | None:
    updated = str(head.get("updated_at") or "").strip()
    if not updated or str(head.get("type") or "").strip().lower() != "regular":
        return None
    return updated, _mapping(head.get("counts")).get("items")


def _memo_get(adapter: Any, list_id: str, stamp: tuple[str, Any] | None) -> list[PlaylistItem] | None:
    if stamp is None:
        return None
    hit = _SNAPSHOT_MEMO.get((_instance_id(adapter), list_id))
    if not hit or hit[1] != stamp or (time.time() - hit[0]) > _SNAPSHOT_MEMO_TTL:
        return None
    return copy.deepcopy(hit[2])


def _memo_put(adapter: Any, list_id: str, stamp: tuple[str, Any] | None, items: list[PlaylistItem]) -> None:
    if stamp is None:
        return
    key = (_instance_id(adapter), list_id)
    _SNAPSHOT_MEMO.pop(key, None)
    _SNAPSHOT_MEMO[key] = (time.time(), stamp, copy.deepcopy(items))
    while len(_SNAPSHOT_MEMO) > _SNAPSHOT_MEMO_MAX:
        _SNAPSHOT_MEMO.pop(next(iter(_SNAPSHOT_MEMO)))


def _custom_snapshot(adapter: Any, list_id: str) -> PlaylistSnapshot:
    items: list[PlaylistItem] = []
    seen: set[str] = set()
    head: Mapping[str, Any] = {}
    stamp: tuple[str, Any] | None = None
    raw_rows = 0
    guard = PageGuard(SIMKLFetchError, "simkl custom list repeated page")
    for page in page_numbers():
        data = _custom_get(adapter, URL_LIST.format(list_id=list_id), limit=_CUSTOM_PAGE, page=page)
        if not head:
            head = data
            stamp = _snapshot_stamp(head)
            cached = _memo_get(adapter, list_id, stamp) if int(_mapping(head.get("pagination")).get("total_pages") or 1) > 1 else None
            if cached is not None:
                resource = _custom_resource(adapter, head, me=_me(adapter)[0])
                if resource is None:
                    raise SIMKLPlaylistNotFound("simkl custom list not found")
                _info("snapshot_unchanged", list_id=resource.id, count=len(cached))
                return PlaylistSnapshot(resource=resource, items=cached, checkpoint=str(head.get("updated_at") or "") or None)
        bucket = (_CUSTOM_MEDIA.get(str(data.get("media_type") or "").strip().lower()) or ("shows", "show"))[0]
        rows = data.get("items")
        pagination = _mapping(data.get("pagination"))
        total_pages = int(pagination.get("total_pages") or 1)
        if not isinstance(rows, list) or (not rows and page < total_pages):
            raise SIMKLFetchError("simkl custom list page incomplete")
        if any(not isinstance(row, Mapping) for row in rows):
            raise SIMKLFetchError("simkl custom list page has invalid rows")
        guard.check(rows)
        raw_rows += len(rows)
        for row in rows:
            ids = dict(row.get("ids") or {}) if isinstance(row.get("ids"), Mapping) else {}
            if ids.get("simkl_id") and not ids.get("simkl"):
                ids["simkl"] = ids.get("simkl_id")
            media = feat_watchlist._normalize_row(bucket, {**row, "ids": ids})
            simkl_id = str((media.get("ids") or {}).get("simkl") or "")
            item = PlaylistItem.from_media(media, position=len(items), provider_media_id=simkl_id, playlist_item_id=simkl_id)
            if item.key and item.key not in seen:
                seen.add(item.key)
                items.append(item)
        if page >= total_pages:
            break
    expected = _mapping(head.get("pagination")).get("total_items")
    if isinstance(expected, int) and not isinstance(expected, bool) and raw_rows != expected:
        raise SIMKLFetchError("simkl custom list read incomplete")
    resource = _custom_resource(adapter, head, me=_me(adapter)[0])
    if resource is None:
        raise SIMKLPlaylistNotFound("simkl custom list not found")
    _memo_put(adapter, list_id, stamp, items)
    _info("snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=str(head.get("updated_at") or "") or None)


def _link_id(ref: Any) -> str:
    raw = str(ref or "").strip()
    custom = _custom_id(raw)
    if custom:
        return custom
    if raw.isdigit():
        return raw
    if "://" not in raw and raw.split("/", 1)[0].lower() in _SIMKL_HOSTS:
        raw = f"https://{raw}"
    if "://" in raw:
        parsed = urlparse(raw)
        if (parsed.hostname or "").lower() not in _SIMKL_HOSTS:
            return ""
        path = parsed.path
    else:
        path = raw.split("?", 1)[0].split("#", 1)[0]
    parts = [unquote(p) for p in path.split("/") if p]
    for i, part in enumerate(parts[:-1]):
        if part.lower() in ("list", "lists") and parts[i + 1].isdigit():
            return parts[i + 1]
    return ""


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    list_id = _link_id(ref)
    if not list_id:
        raise PlaylistLinkError("unsupported SIMKL list link")
    user_id, tier = _me(adapter)
    if user_id and tier not in _CUSTOM_TIERS:
        raise SIMKLPremiumRequired("SIMKL custom lists need SIMKL PRO or VIP")
    wanted = f"{CUSTOM_PREFIX}{list_id}"
    for res in _custom_resources(adapter):
        if res.id == wanted:
            return res
    head = _custom_get(adapter, URL_LIST.format(list_id=list_id), limit=1, page=1)
    resource = _custom_resource(adapter, head, me=_me(adapter)[0])
    if resource is None:
        raise SIMKLListUnavailable("The SIMKL list cannot be used here")
    return resource


def _read_only_result(items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    unresolved = [{"item": dict(item), "hint": "read_only"} for item in items or [] if isinstance(item, Mapping)]
    return {"ok": False, "count": 0, "unresolved": unresolved, "confirmed_keys": [], "warnings": [SIMKL_CUSTOM_WARNING]}


def list_resources(adapter: Any) -> list[PlaylistResource]:
    out = [_resource(adapter, status) for status, _label, _media, _buckets in _STATUS_ROWS]
    try:
        out.extend(_custom_resources(adapter))
    except Exception as e:
        _warn("custom_lists_unavailable", error=str(e))
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
    custom = _custom_id(playlist_id)
    if custom:
        return _custom_snapshot(adapter, custom)
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


def _custom_write(adapter: Any, method: str, url: str, body: Any = None, **query: Any) -> tuple[int, Any]:
    headers = adapter_headers(adapter)
    params = simkl_api_params_from_headers(headers, **query)
    send = getattr(adapter.client.session, method.lower())
    code, data, error = 0, None, ""
    for attempt in range(_WRITE_RETRIES + 1):
        kwargs: dict[str, Any] = {"headers": headers, "params": params, "timeout": getattr(adapter.cfg, "timeout", 15.0)}
        if body is not None:
            kwargs["json"] = body
        resp = send(url, **kwargs)
        code = int(getattr(resp, "status_code", 0) or 0)
        data = _response_json(resp)
        error = str(_mapping(data).get("error") or "")
        if code == 400 and error == "rate_limit" and attempt < _WRITE_RETRIES:
            time.sleep(1.5 * (attempt + 1))
            continue
        break
    message = str(_mapping(data).get("message") or error or code)
    if code == 404:
        raise SIMKLListUnavailable("The SIMKL list no longer exists")
    if error == "premium_only":
        raise SIMKLPremiumRequired("SIMKL custom lists need SIMKL PRO or VIP")
    if error in _REAUTH_ERRORS:
        raise SIMKLListReconnectRequired(f"SIMKL custom list changes need a SIMKL reconnect: {error}")
    if error in _LIMIT_ERRORS:
        raise SIMKLListLimitReached(f"SIMKL list limit reached: {message}")
    if error in _DENIED_ERRORS:
        raise PlaylistReadOnlyError(f"The SIMKL list cannot be changed: {message}")
    if error and 200 <= code < 300:
        _warn("write_refused", url=url.replace(BASE, ""), status=code, error=error)
        raise SIMKLPlaylistError(f"simkl custom list write refused: {error}")
    if not (200 <= code < 300):
        _warn("write_failed", url=url.replace(BASE, ""), status=code, error=error)
    return code, data


def _forget_snapshot(adapter: Any, list_id: str) -> None:
    _SNAPSHOT_MEMO.pop((_instance_id(adapter), list_id), None)


def _writable_target(adapter: Any, list_id: str) -> tuple[Mapping[str, Any], PlaylistResource] | None:
    head = _custom_get(adapter, URL_LIST.format(list_id=list_id), limit=1, page=1)
    resource = _custom_resource(adapter, head, me=_me(adapter)[0])
    if resource is None or not resource.can_add:
        return None
    return head, resource


def _custom_resource_from_write(adapter: Any, data: Any, list_id: str) -> PlaylistResource:
    row = _mapping(data)
    resource = _custom_resource(adapter, row, me=_me(adapter)[0]) if row.get("id") and row.get("media_type") else None
    if resource is None:
        head = _custom_get(adapter, URL_LIST.format(list_id=list_id), limit=1, page=1)
        resource = _custom_resource(adapter, head, me=_me(adapter)[0])
    if resource is None:
        raise SIMKLPlaylistNotFound("simkl custom list not found")
    return resource


def _custom_entries(items: Sequence[Mapping[str, Any]], list_bucket: str) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    accepted: list[dict[str, Any]] = []
    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        media = id_minimal(raw)
        if (_item_bucket(raw) == "movies") != (list_bucket == "movies"):
            unresolved.append({"item": media, "hint": "wrong_media_type"})
            continue
        ids = _ids(media)
        if not ids:
            unresolved.append({"item": media, "hint": "missing_simkl_supported_id"})
            continue
        key = canonical_key(media)
        if not key or key in seen:
            continue
        seen.add(key)
        accepted.append({"key": key, "item": media, "ids": ids})
    return accepted, unresolved


def _id_pairs(ids: Any) -> set[tuple[str, str]]:
    return {(str(k), str(v)) for k, v in _mapping(ids).items() if v not in (None, "")}


def _rejected(rows: Any, accepted: Mapping[str, Any]) -> bool:
    mine = _id_pairs(accepted.get("ids"))
    return any(mine & _id_pairs(_mapping(row).get("ids")) for row in rows or [] if isinstance(row, Mapping))


def _custom_items_write(adapter: Any, list_id: str, items: Sequence[Mapping[str, Any]], *, remove: bool) -> dict[str, Any]:
    target = _writable_target(adapter, list_id)
    if target is None:
        return _read_only_result(items)
    head = target[0]
    bucket = (_CUSTOM_MEDIA.get(str(head.get("media_type") or "").strip().lower()) or ("shows", "show"))[0]
    accepted, unresolved = _custom_entries(items, bucket)
    confirmed: list[str] = []
    ok = True
    url = (URL_LIST_ITEMS_REMOVE if remove else URL_LIST_ITEMS).format(list_id=list_id)
    for start in range(0, len(accepted), _WRITE_CHUNK):
        chunk = accepted[start:start + _WRITE_CHUNK]
        code, data = _custom_write(adapter, "POST", url, {bucket: [{"ids": row["ids"]} for row in chunk]})
        if not (200 <= code < 300):
            ok = False
            unresolved.extend({"item": row["item"], "hint": f"http:{code}"} for row in chunk)
            continue
        body = _mapping(data)
        not_found = body.get("not_found") if isinstance(body.get("not_found"), list) else []
        wrong_type = body.get("wrong_type") if isinstance(body.get("wrong_type"), list) else []
        for row in chunk:
            if wrong_type and _rejected(wrong_type, row):
                unresolved.append({"item": row["item"], "hint": "wrong_media_type"})
            elif not remove and not_found and _rejected(not_found, row):
                unresolved.append({"item": row["item"], "hint": "not_found"})
            else:
                confirmed.append(row["key"])
    _forget_snapshot(adapter, list_id)
    _info("write_done", op="remove" if remove else "add", list_id=f"{CUSTOM_PREFIX}{list_id}", applied=len(confirmed), unresolved=len(unresolved))
    return {"ok": ok, "count": len(confirmed), "unresolved": unresolved, "confirmed_keys": confirmed}


def _custom_reorder(adapter: Any, list_id: str, ordered_keys: Sequence[str]) -> dict[str, Any]:
    snap = _custom_snapshot(adapter, list_id)
    if not snap.resource.can_reorder:
        return {"ok": False, "count": 0, "reordered": 0, "unsupported": True, "error": "read_only"}
    by_key: dict[str, str] = {}
    current: list[str] = []
    for it in snap.items:
        simkl_id = str(it.provider_media_id or "").strip()
        if not simkl_id:
            continue
        current.append(simkl_id)
        for key in {it.key, *typed_keys_for_item(it.item or {})}:
            if key:
                by_key.setdefault(str(key), simkl_id)
    wanted: list[str] = []
    for key in ordered_keys or []:
        simkl_id = by_key.get(str(key))
        if simkl_id and simkl_id not in wanted:
            wanted.append(simkl_id)
    wanted.extend(x for x in current if x not in set(wanted))
    if wanted == current:
        return {"ok": True, "count": 0, "reordered": 0}
    media = str(snap.resource.media_types[0] if snap.resource.media_types else "")
    bucket = {"movie": "movies", "show": "shows", "anime": "anime"}.get(media, "shows")
    url = URL_LIST_ITEMS.format(list_id=list_id)
    start = 0
    while start < len(wanted):
        chunk = wanted[max(0, start - 1):start + _WRITE_CHUNK - (1 if start else 0)]
        code, _data = _custom_write(adapter, "POST", url, {bucket: [{"ids": {"simkl": int(x) if x.isdigit() else x}} for x in chunk]}, allow_reorder="yes")
        if not (200 <= code < 300):
            _forget_snapshot(adapter, list_id)
            return {"ok": False, "count": 0, "reordered": 0, "error": f"http:{code}"}
        start += len(chunk) - (1 if start else 0)
    _forget_snapshot(adapter, list_id)
    after = [str(it.provider_media_id or "") for it in _custom_snapshot(adapter, list_id).items if it.provider_media_id]
    if after != wanted:
        _warn("reorder_unconfirmed", list_id=f"{CUSTOM_PREFIX}{list_id}", wanted=len(wanted), seen=len(after))
        return {"ok": False, "count": 0, "reordered": 0, "error": "playlist_order_unconfirmed"}
    _info("reorder_done", list_id=f"{CUSTOM_PREFIX}{list_id}", count=len(wanted))
    return {"ok": True, "count": len(wanted), "reordered": len(wanted)}


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    privacy: str | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    simkl_media = _CREATE_MEDIA.get(str(media_type or "").strip().lower())
    if not simkl_media:
        raise SIMKLPlaylistError("SIMKL custom lists hold movies, shows or anime")
    if dry_run:
        return PlaylistResource(
            provider=_PROVIDER,
            id="",
            name=nm,
            instance=_instance_id(adapter),
            kind=PLAYLIST_KIND_REGULAR,
            can_add=True,
            can_remove=True,
            can_reorder=True,
            media_types=(_CUSTOM_MEDIA[simkl_media][1],),
            extra={"endpoint_type": "custom_list"},
        )
    user_id, tier = _me(adapter)
    if user_id and tier not in _CUSTOM_TIERS:
        raise SIMKLPremiumRequired("SIMKL custom lists need SIMKL PRO or VIP")
    body: dict[str, Any] = {"name": nm, "media_type": simkl_media, "privacy": privacy or "public"}
    code, data = _custom_write(adapter, "POST", URL_LISTS, body)
    list_id = str(_mapping(data).get("id") or "").strip()
    if not (200 <= code < 300) or not list_id.isdigit():
        raise SIMKLPlaylistError(f"simkl create list failed: http {code}")
    resource = _custom_resource_from_write(adapter, data, list_id)
    _info("create_done", list_id=resource.id, media_type=simkl_media)
    if items:
        try:
            _custom_items_write(adapter, list_id, list(items), remove=False)
        except Exception as e:
            _warn("create_seed_failed", list_id=resource.id, error=type(e).__name__)
    return resource


def _edit_list(adapter: Any, playlist_id: Any, body: Mapping[str, Any], *, op: str, dry_run: bool) -> PlaylistResource:
    list_id = _custom_id(playlist_id)
    if not list_id:
        raise SIMKLPlaylistError("SIMKL status buckets cannot be changed")
    target = _writable_target(adapter, list_id)
    if target is None:
        raise PlaylistReadOnlyError("The SIMKL list cannot be changed")
    if dry_run:
        return target[1]
    code, data = _custom_write(adapter, "POST", URL_LIST.format(list_id=list_id), dict(body))
    if not (200 <= code < 300):
        raise SIMKLPlaylistError(f"simkl {op} list failed: http {code}")
    _forget_snapshot(adapter, list_id)
    resource = _custom_resource_from_write(adapter, data, list_id)
    _info(f"{op}_done", list_id=resource.id)
    return resource


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    return _edit_list(adapter, playlist_id, {"name": nm}, op="rename", dry_run=dry_run)


def set_privacy(adapter: Any, playlist_id: Any, privacy: str, *, dry_run: bool = False) -> PlaylistResource:
    return _edit_list(adapter, playlist_id, {"privacy": privacy}, op="privacy", dry_run=dry_run)


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    list_id = _custom_id(playlist_id)
    if not list_id:
        raise SIMKLPlaylistError("SIMKL status buckets cannot be deleted")
    if _writable_target(adapter, list_id) is None:
        raise PlaylistReadOnlyError("The SIMKL list cannot be deleted")
    if dry_run:
        return {"ok": True, "dry_run": True}
    code, _data = _custom_write(adapter, "DELETE", URL_LIST.format(list_id=list_id))
    if code not in (200, 204):
        raise SIMKLPlaylistError(f"simkl delete list failed: http {code}")
    _forget_snapshot(adapter, list_id)
    _info("delete_done", list_id=f"{CUSTOM_PREFIX}{list_id}")
    return {"ok": True, "count": 1}


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
    custom = _custom_id(playlist_id)
    if custom:
        return _custom_items_write(adapter, custom, list(items or []), remove=False)
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
    custom = _custom_id(playlist_id)
    if custom:
        return _custom_items_write(adapter, custom, list(items or []), remove=True)
    status = _status(playlist_id)
    _, accepted, unresolved = _remove_payload(list(items or []), status)
    present, absent, guard_unresolved = feat_watchlist.split_by_live_status(adapter, accepted, status, item_of=lambda row: row["item"])
    unresolved.extend(guard_unresolved)
    skipped = [str(row.get("key") or "") for row in absent if row.get("key")]
    if skipped:
        try:
            feat_watchlist._shadow_remove_keys(skipped)
            feat_watchlist._unfreeze_if_present(skipped)
        except Exception:
            pass
        _info("write_skipped", op="remove", status=status, reason="not_in_simkl_status", skipped=len(skipped))
    accepted = present
    body: dict[str, list[dict[str, Any]]] = {}
    for row in accepted:
        body.setdefault("movies" if row.get("bucket") == "movies" else "shows", []).append({"ids": row["ids"]})
    keys = [str(x.get("key") or "") for x in accepted if x.get("key")]
    if not keys:
        return {"ok": True, "count": len(skipped), "unresolved": unresolved, "confirmed_keys": skipped, "warnings": [SIMKL_REMOVE_WARNING]}
    headers = adapter_headers(adapter)
    resp = adapter.client.session.post(URL_REMOVE, headers=headers, params=simkl_api_params_from_headers(headers), json=body, timeout=getattr(adapter.cfg, "timeout", 15.0))
    data = _response_json(resp)
    if not (200 <= int(getattr(resp, "status_code", 0) or 0) < 300):
        unresolved.extend({"item": {"key": key}, "hint": f"http:{getattr(resp, 'status_code', 0)}"} for key in keys)
        return {"ok": False, "count": len(skipped), "unresolved": unresolved, "confirmed_keys": skipped, "warnings": [SIMKL_REMOVE_WARNING]}
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
    confirmed = list(dict.fromkeys([*confirmed, *skipped]))
    _info("write_done", op="remove", status=status, applied=len(confirmed), unresolved=len(unresolved), skipped=len(skipped))
    return {"ok": True, "count": len(confirmed), "unresolved": unresolved, "confirmed_keys": confirmed, "warnings": [SIMKL_REMOVE_WARNING]}


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    custom = _custom_id(playlist_id)
    if custom:
        return _custom_reorder(adapter, custom, ordered_keys)
    return {"ok": True, "count": 0, "reordered": 0, "unsupported": True}

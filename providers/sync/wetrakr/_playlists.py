# providers/sync/wetrakr/_playlists.py
# CrossWatch - WeTrakr lists as playlists: own, followed, liked, public and shared lists
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from collections.abc import Iterable, Mapping, Sequence
from typing import Any
from urllib.parse import unquote, urlparse

from cw_platform.id_map import canonical_key, minimal as id_minimal
from cw_platform.interactive_reads import remember_resource, retained_read
from cw_platform.playlists import (
    PLAYLIST_KIND_REGULAR,
    PlaylistItem,
    PlaylistLinkError,
    PlaylistReadOnlyError,
    PlaylistResource,
    PlaylistSnapshot,
    PlaylistUnavailableError,
)
from cw_platform.run_control import raise_if_cancelled
from providers.sync._log import log

from ._common import WeTrakrSyncError, body_of, identifier, identity_tokens, int_value, media_item, request, resolve_child

_PROVIDER = "WETRAKR"
_FEATURE = "playlists"
MEDIA_TYPES = ("movies", "shows", "seasons", "episodes")
_KINDS = ("movie", "show", "season", "episode")
PAGE_SIZE = 100
MAX_PAGES = 100
WRITE_BATCH = 100
REORDER_MAX = 5000
_HOSTS = {"wetrakr.com", "www.wetrakr.com"}


def _info(event: str, **fields: Any) -> None:
    log(_PROVIDER, _FEATURE, "info", event, **fields)


def _warn(event: str, **fields: Any) -> None:
    log(_PROVIDER, _FEATURE, "warn", event, **fields)


def _instance_id(adapter: Any) -> str:
    return str(getattr(adapter, "instance_id", None) or "default")


def _owner(row: Mapping[str, Any]) -> str:
    owner = row.get("owner")
    info = owner.get("info") if isinstance(owner, Mapping) else None
    info = info if isinstance(info, Mapping) else {}
    return str(info.get("display_name") or info.get("username") or "").strip()


def _stamp_extra(row: Mapping[str, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    count = int_value(row.get("items_count", row.get("total_items")))
    if count >= 0:
        out["item_count"] = count
    updated = str(row.get("list_updated_at") or "").strip()
    if updated:
        out["updated_at"] = updated
    privacy = str(row.get("privacy") or "").strip().lower()
    if privacy:
        out["privacy"] = privacy
    return out


def _resource(adapter: Any, row: Any, endpoint_type: str = "") -> PlaylistResource | None:
    if not isinstance(row, Mapping):
        return None
    lid = int_value(row.get("id"))
    if lid <= 0:
        return None
    owned = not endpoint_type
    collab = endpoint_type == "collaboration"
    locked = bool(row.get("locked"))
    name = str(row.get("name") or "").strip() or str(lid)
    owner = _owner(row)
    extra = _stamp_extra(row)
    if locked:
        extra.update(locked=True, locked_reason=str(row.get("locked_reason") or "plan"))
    if not owned:
        extra.update(endpoint_type=endpoint_type, owner=owner, can_rename=False, can_delete=False)
        if not collab:
            extra["public_list"] = True
    return PlaylistResource(
        provider=_PROVIDER,
        id=str(lid),
        name=name if owned or not owner else f"{name} by {owner}",
        instance=_instance_id(adapter),
        kind=PLAYLIST_KIND_REGULAR,
        can_read=not locked,
        can_add=(owned or collab) and not locked,
        can_remove=owned and not locked,
        can_reorder=owned and not locked,
        media_types=MEDIA_TYPES,
        extra=extra,
    )


def _rows(data: Any) -> list[Mapping[str, Any]]:
    if not isinstance(data, list) or any(not isinstance(row, Mapping) for row in data):
        raise WeTrakrSyncError("invalid_page")
    return data


def _paged(adapter: Any, path: str) -> list[Mapping[str, Any]]:
    out: list[Mapping[str, Any]] = []
    seen: set[str] = set()
    for page in range(1, MAX_PAGES + 1):
        raise_if_cancelled()
        response = request(adapter, "GET", path, params={"page": page, "limit": PAGE_SIZE})
        rows = _rows(body_of(response))
        if rows:
            signature = json.dumps(rows[0], sort_keys=True, default=str)
            if signature in seen:
                raise WeTrakrSyncError("repeated_page")
            seen.add(signature)
        out.extend(rows)
        headers = {str(k).lower(): v for k, v in response.headers.items()}
        page_count = int_value(headers.get("x-pagination-page-count"))
        if not rows or (page_count >= 0 and page >= page_count) or (page_count < 0 and len(rows) < PAGE_SIZE):
            return out
    raise WeTrakrSyncError("pagination_limit")


def _optional(adapter: Any, path: str, op: str, *, paged: bool = False) -> list[Mapping[str, Any]]:
    try:
        return _paged(adapter, path) if paged else _rows(body_of(request(adapter, "GET", path)))
    except WeTrakrSyncError as exc:
        if exc.status_code == 429:
            raise
        _warn("http_failed", op=op, reason=exc.reason, status=exc.status_code)
        return []


def _shared_type(row: Any, default: str) -> str:
    perms = row.get("permissions") if isinstance(row, Mapping) else None
    return "collaboration" if isinstance(perms, Mapping) and perms.get("is_collaborator") else default


@retained_read
def list_resources(adapter: Any) -> list[PlaylistResource]:
    try:
        own = _rows(body_of(request(adapter, "GET", "/sync/lists")))
    except WeTrakrSyncError as exc:
        _warn("http_failed", op="list_resources", reason=exc.reason, status=exc.status_code)
        raise
    out: list[PlaylistResource] = []
    known: set[str] = set()
    for row in own:
        res = _resource(adapter, row)
        if res and res.id not in known:
            known.add(res.id)
            out.append(res)
    locked = 0
    for row in _optional(adapter, "/sync/lists/followed", "followed_lists"):
        if row.get("locked") and str(row.get("locked_reason") or "followed").strip().lower() == "followed":
            locked += 1
            continue
        res = _resource(adapter, row, _shared_type(row, "followed_list"))
        if res and res.id not in known:
            known.add(res.id)
            out.append(res)
    for row in _optional(adapter, "/sync/likes/lists", "liked_lists", paged=True):
        res = _resource(adapter, row.get("list"), _shared_type(row.get("list"), "liked_list"))
        if res and res.id not in known:
            known.add(res.id)
            out.append(res)
    _info("list_resources_done", count=len(out), followed_locked=locked)
    return out


def _find_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    resources = list_resources(adapter)
    for res in resources:
        if res.id == lid:
            return res
    if lid.isdigit():
        return None
    for res in resources:
        if res.name == lid:
            return res
    return None


def _is_own(res: PlaylistResource) -> bool:
    return not res.extra.get("endpoint_type")


def _detail(adapter: Any, path: str) -> Mapping[str, Any] | None:
    try:
        data = body_of(request(adapter, "GET", path))
    except WeTrakrSyncError as exc:
        if exc.status_code in (400, 403, 404):
            _info("list_lookup_missed", status=exc.status_code)
            return None
        raise
    return data if isinstance(data, Mapping) and int_value(data.get("id")) > 0 else None


def _public_resource(adapter: Any, row: Mapping[str, Any]) -> PlaylistResource | None:
    perms = row.get("permissions")
    perms = perms if isinstance(perms, Mapping) else {}
    if perms.get("is_owner"):
        return _resource(adapter, row)
    if perms.get("is_collaborator"):
        return _resource(adapter, row, "collaboration")
    return _resource(adapter, row, "public_list")


def _public_list_resource(adapter: Any, lid: str) -> PlaylistResource:
    row = _detail(adapter, f"/lists/{lid}") if lid.isdigit() else None
    res = _public_resource(adapter, row) if row is not None else None
    if res is not None:
        return res
    return PlaylistResource(
        provider=_PROVIDER,
        id=lid,
        name=lid,
        instance=_instance_id(adapter),
        can_read=True,
        media_types=MEDIA_TYPES,
        extra={"unresolved": True, "public_list": True, "can_rename": False, "can_delete": False},
    )


def _unavailable(resource: PlaylistResource, status: int = 0) -> PlaylistUnavailableError:
    label = resource.name or resource.id
    if status == 420 or resource.extra.get("locked"):
        return PlaylistUnavailableError(f"The WeTrakr list {label} is locked by the WeTrakr plan limit")
    if resource.extra.get("unresolved"):
        return PlaylistUnavailableError(f"The WeTrakr list {label} could not be read. It may be private or deleted")
    if resource.extra.get("endpoint_type") == "collaboration":
        return PlaylistUnavailableError(f"You no longer have access to the WeTrakr list {label}")
    if resource.extra.get("public_list"):
        return PlaylistUnavailableError(f"The WeTrakr list {label} is private or no longer exists")
    return PlaylistUnavailableError(f"The WeTrakr list {label} no longer exists")


def _list_id(playlist_id: Any) -> str:
    return str(playlist_id or "").strip()


def get_snapshot(adapter: Any, playlist_id: Any) -> PlaylistSnapshot:
    lid = _list_id(playlist_id)
    resource = _find_resource(adapter, lid) or _public_list_resource(adapter, lid)
    if not resource.id.isdigit():
        raise _unavailable(resource)
    if resource.extra.get("locked"):
        raise _unavailable(resource)
    try:
        rows = _paged(adapter, f"/lists/{resource.id}/items/all")
    except WeTrakrSyncError as exc:
        if exc.status_code in (403, 404, 420):
            _warn("list_unavailable", list_id=resource.id, status=exc.status_code)
            raise _unavailable(resource, exc.status_code) from None
        raise
    items: list[PlaylistItem] = []
    for pos, row in enumerate(rows):
        kind = str(row.get("type") or "")
        if kind not in _KINDS:
            continue
        try:
            media = media_item(row, kind)
        except WeTrakrSyncError as exc:
            _warn("invalid_list_item", list_id=resource.id, reason=exc.reason, item_type=kind)
            raise WeTrakrSyncError("invalid_list_item") from None
        rank = int_value(row.get("list_item_rank"))
        items.append(
            PlaylistItem.from_media(
                media,
                playlist_item_id=row.get("list_item_id"),
                position=rank if rank > 0 else pos + 1,
                provider_media_id=media["ids"].get("wetrakr"),
            )
        )
    items.sort(key=lambda it: (it.position is None, it.position if it.position is not None else 0))
    _info("snapshot_done", list_id=resource.id, count=len(items))
    return PlaylistSnapshot(resource=resource, items=items, checkpoint=None)


def _link_id(ref: Any) -> str:
    raw = str(ref or "").strip()
    if raw.isdigit():
        return raw
    if "://" not in raw and raw.split("/", 1)[0].lower() in _HOSTS:
        raw = f"https://{raw}"
    if "://" in raw:
        parsed = urlparse(raw)
        if (parsed.hostname or "").lower() not in _HOSTS:
            return ""
        path = parsed.path
    else:
        path = raw.split("?", 1)[0].split("#", 1)[0]
    parts = [unquote(p) for p in path.split("/") if p]
    if len(parts) == 2 and parts[0].lower() in ("lists", "list") and parts[1].isdigit():
        return parts[1]
    return ""


def resolve(adapter: Any, ref: Any) -> PlaylistResource:
    lid = _link_id(ref)
    if not lid:
        raise PlaylistLinkError("unsupported WeTrakr list link")
    known = _find_resource(adapter, lid)
    if known is not None and not _needs_permission_check(known):
        return known
    row = _detail(adapter, f"/lists/{lid}")
    res = _public_resource(adapter, row) if row is not None else None
    if res is not None and (known is None or res.extra.get("endpoint_type") == "collaboration"):
        return res
    if known is not None:
        return known
    raise PlaylistUnavailableError(f"The WeTrakr list {lid} is private or no longer exists")


def _needs_permission_check(res: PlaylistResource) -> bool:
    return not _is_own(res) and res.extra.get("endpoint_type") != "collaboration"


def _editable_resource(adapter: Any, lid: str) -> PlaylistResource | None:
    row = _detail(adapter, f"/sync/lists/{lid}")
    perms = row.get("permissions") if row is not None else None
    perms = perms if isinstance(perms, Mapping) else {}
    if perms.get("is_owner"):
        return _resource(adapter, row)
    if perms.get("is_collaborator"):
        return _resource(adapter, row, "collaboration")
    return None


def _write_target(adapter: Any, playlist_id: Any, *, op: str) -> PlaylistResource:
    lid = _list_id(playlist_id)
    res = _find_resource(adapter, lid)
    if lid.isdigit() and (res is None or _needs_permission_check(res)):
        res = _editable_resource(adapter, lid) or res
    if res is None:
        raise PlaylistReadOnlyError(f"The WeTrakr list {lid} is read only")
    allowed = {"add": res.can_add, "remove": res.can_remove, "reorder": res.can_reorder}.get(op, False)
    if not allowed:
        raise PlaylistReadOnlyError(f"The WeTrakr list {res.name or res.id} is read only")
    return res


def _entry(adapter: Any, item: Mapping[str, Any]) -> tuple[str, dict[str, Any]]:
    kind = str(item.get("type") or "")
    if kind in ("season", "episode"):
        child = resolve_child(adapter, item)
        return f"{kind}s", {"id": int_value(child["ids"]["wetrakr"])}
    if kind not in ("movie", "show"):
        raise WeTrakrSyncError("unsupported_media_type")
    return f"{kind}s", identifier(item.get("ids"))


def _token(group: str, obj: Mapping[str, Any]) -> str:
    return group + json.dumps({k: obj[k] for k in ("id", "ids") if k in obj}, sort_keys=True)


def _rejected(data: Mapping[str, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for field, default in (("notFound", "not_found"), ("errored", "rejected")):
        block = data.get(field)
        for group, rows in (block.items() if isinstance(block, Mapping) else []):
            for row in rows if isinstance(rows, list) else []:
                if not isinstance(row, Mapping):
                    continue
                error = str(row.get("error") or "").strip().lower()
                out[_token(str(group), row)] = "already_added" if error.startswith("already added") else default
    return out


def _applied_count(data: Mapping[str, Any], op: str) -> int | None:
    block = data.get("added" if op == "add" else "removed")
    if not isinstance(block, Mapping):
        return None
    if "total" in block:
        total = int_value(block.get("total"))
        return total if total >= 0 else None
    counts = [int_value(block.get(group)) for group in ("movies", "shows", "seasons", "episodes") if group in block]
    if not counts or any(count < 0 for count in counts):
        return None
    return sum(counts)


def _write(adapter: Any, playlist_id: Any, items: Iterable[Mapping[str, Any]], *, op: str) -> dict[str, Any]:
    res = _write_target(adapter, playlist_id, op=op)
    unresolved: list[dict[str, Any]] = []
    unresolved_keys: list[str] = []
    entries: dict[str, tuple[str, dict[str, Any], list[str]]] = {}
    seen_keys: set[str] = set()
    by_identity: dict[str, str] = {}
    for raw in items or []:
        if not isinstance(raw, Mapping):
            continue
        key = canonical_key(raw)
        if not key or key in seen_keys:
            continue
        seen_keys.add(key)
        identity = identity_tokens(raw)
        same = next((by_identity[t] for t in identity if t in by_identity), None)
        if same is not None:
            entries[same][2].append(key)
            continue
        try:
            group, obj = _entry(adapter, raw)
        except WeTrakrSyncError as exc:
            if exc.status_code == 429:
                raise
            unresolved_keys.append(key)
            unresolved.append({"item": id_minimal(raw), "key": key, "hint": exc.reason})
            continue
        token = _token(group, obj)
        if token in entries:
            entries[token][2].append(key)
        else:
            entries[token] = (group, obj, [key])
        for t in identity:
            by_identity.setdefault(t, token)

    path = f"/sync/lists/{res.id}/items" + ("" if op == "add" else "/remove")
    confirmed: list[str] = []
    capacity = ""
    ok = True
    batches = list(entries.items())
    for start in range(0, len(batches), WRITE_BATCH):
        batch = batches[start:start + WRITE_BATCH]
        body: dict[str, list[dict[str, Any]]] = {}
        for _token_key, (group, obj, _keys) in batch:
            body.setdefault(group, []).append(obj)
        try:
            data = body_of(request(adapter, "POST", path, json=body))
        except WeTrakrSyncError as exc:
            ok = False
            hint = "list_full" if exc.status_code == 420 else exc.reason
            if exc.status_code == 420:
                capacity = "list_full"
            for _token_key, (_group, _obj, keys) in batch:
                for key in keys:
                    unresolved_keys.append(key)
                    unresolved.append({"item": {"key": key}, "key": key, "hint": hint})
            _warn("write_failed", op=op, list_id=res.id, status=exc.status_code, reason=exc.reason)
            continue
        if not isinstance(data, Mapping):
            ok = False
            for _token_key, (_group, _obj, keys) in batch:
                for key in keys:
                    unresolved_keys.append(key)
                    unresolved.append({"item": {"key": key}, "key": key, "hint": "write_not_verified"})
            continue
        rejected = _rejected(data)
        pending = sum(1 for token, _entry_row in batch if token not in rejected)
        applied = _applied_count(data, op)
        verified = pending == 0 or (applied is not None and applied >= pending)
        if not verified:
            ok = False
            _warn("write_not_verified", op=op, list_id=res.id, sent=pending, applied=applied)
        for token, (_group, _obj, keys) in batch:
            hint = rejected.get(token)
            if (hint is None and verified) or (op == "add" and hint == "already_added"):
                confirmed.extend(keys)
                continue
            hint = hint or "write_not_verified"
            for key in keys:
                unresolved_keys.append(key)
                unresolved.append({"item": {"key": key}, "key": key, "hint": hint})
    _info("write_done", op=op, list_id=res.id, confirmed=len(confirmed), unresolved=len(unresolved_keys))
    out: dict[str, Any] = {"ok": ok, "count": len(confirmed), "confirmed_keys": confirmed, "unresolved_keys": unresolved_keys, "unresolved": unresolved}
    if capacity:
        out["capacity"] = capacity
    return out


def add(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="add")


def remove(adapter: Any, playlist_id: Any, items: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return _write(adapter, playlist_id, list(items or []), op="remove")


def reorder(adapter: Any, playlist_id: Any, ordered_keys: Sequence[str]) -> dict[str, Any]:
    res = _write_target(adapter, playlist_id, op="reorder")
    snap = get_snapshot(adapter, res.id)
    by_key: dict[str, str] = {}
    for it in snap.items:
        if it.key and it.playlist_item_id:
            by_key.setdefault(it.key, str(it.playlist_item_id))
    order: list[str] = []
    used: set[str] = set()
    for key in ordered_keys or []:
        item_id = by_key.get(str(key or "").strip())
        if item_id and item_id not in used:
            used.add(item_id)
            order.append(item_id)
    for it in snap.items:
        item_id = str(it.playlist_item_id or "")
        if item_id and item_id not in used:
            used.add(item_id)
            order.append(item_id)
    current = [str(it.playlist_item_id) for it in snap.items if it.playlist_item_id]
    if not order or order == current:
        return {"ok": True, "count": 0, "reordered": 0}
    moved = 0
    chunks = [order[i:i + REORDER_MAX] for i in range(0, len(order), REORDER_MAX)]
    try:
        for chunk in reversed(chunks):
            data = body_of(request(adapter, "POST", f"/sync/lists/{res.id}/items/reorder", json={"items": chunk}))
            moved += max(0, int_value(data.get("moved"), 0)) if isinstance(data, Mapping) else 0
    except WeTrakrSyncError as exc:
        _warn("write_failed", op="reorder", list_id=res.id, status=exc.status_code, reason=exc.reason)
        return {"ok": False, "count": 0, "reordered": 0, "error": exc.reason}
    _info("reorder_done", list_id=res.id, moved=moved)
    return {"ok": True, "count": moved, "reordered": moved}


def _owned(adapter: Any, playlist_id: Any) -> PlaylistResource:
    res = _find_resource(adapter, _list_id(playlist_id))
    if res is None or not _is_own(res):
        raise PlaylistReadOnlyError("The WeTrakr list is not yours to change")
    return res


def create(
    adapter: Any,
    name: str,
    *,
    media_type: str | None = None,
    items: Sequence[Mapping[str, Any]] | None = None,
    dry_run: bool = False,
) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    if dry_run:
        return PlaylistResource(provider=_PROVIDER, id="", name=nm, instance=_instance_id(adapter), can_add=True, can_remove=True, can_reorder=True, media_types=MEDIA_TYPES)
    try:
        data = body_of(request(adapter, "POST", "/sync/lists", json={"name": nm[:100], "privacy": "private"}))
    except WeTrakrSyncError as exc:
        if exc.status_code == 420:
            raise RuntimeError("wetrakr list quota reached") from None
        raise
    res = _resource(adapter, data)
    if res is None:
        raise WeTrakrSyncError("create_returned_no_id")
    remember_resource(list_resources, adapter, res)
    if items:
        add(adapter, res.id, items)
    _info("create_done", list_id=res.id)
    return res


def rename(adapter: Any, playlist_id: Any, name: str, *, dry_run: bool = False) -> PlaylistResource:
    nm = str(name or "").strip()
    if not nm:
        raise ValueError("playlist name required")
    res = _owned(adapter, playlist_id)
    if dry_run:
        return PlaylistResource(provider=_PROVIDER, id=res.id, name=nm, instance=res.instance, can_add=res.can_add, can_remove=res.can_remove, can_reorder=res.can_reorder, media_types=res.media_types, extra=dict(res.extra))
    data = body_of(request(adapter, "PUT", f"/sync/lists/{res.id}", json={"name": nm[:100]}))
    renamed = _resource(adapter, data)
    _info("rename_done", list_id=res.id)
    return renamed or PlaylistResource(provider=_PROVIDER, id=res.id, name=nm, instance=res.instance, can_add=res.can_add, can_remove=res.can_remove, can_reorder=res.can_reorder, media_types=res.media_types, extra=dict(res.extra))


def delete(adapter: Any, playlist_id: Any, *, dry_run: bool = False) -> dict[str, Any]:
    res = _owned(adapter, playlist_id)
    if dry_run:
        return {"ok": True, "dry_run": True}
    request(adapter, "DELETE", f"/sync/lists/{res.id}")
    _info("delete_done", list_id=res.id)
    return {"ok": True, "count": 1}

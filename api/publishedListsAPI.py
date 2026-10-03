# /api/publishedListsAPI.py
# CrossWatch - Publish CrossWatch lists for Kometa, Radarr and Sonarr
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from collections.abc import Mapping
from typing import Any, cast

from fastapi import APIRouter, Body, Path as FPath, Query, Request
from fastapi.responses import JSONResponse

from cw_platform.access_policy import request_user, user_can_access_instance
from cw_platform.config_base import load_config
from services import published_lists as svc

router = APIRouter(prefix="/api/playlists/published", tags=["playlists"])
public_router = APIRouter(tags=["published"])


def _base_url(request: Request) -> str:
    base = str(request.base_url).rstrip("/")
    proto = str(request.headers.get("x-forwarded-proto") or "").split(",", 1)[0].strip().lower()
    if proto == "https" and base.startswith("http://"):
        base = "https://" + base[7:]
    return base


def _allowed(cfg: Mapping[str, Any], request: Request | None, instance: Any) -> bool:
    return user_can_access_instance(cfg, request_user(request), svc.PROVIDER, instance or "default")


def _denied() -> JSONResponse:
    return JSONResponse({"ok": False, "error": "profile_scope_denied"}, status_code=403)


def _feed_view(request: Request, feed: Mapping[str, Any]) -> dict[str, Any]:
    base = f"{_base_url(request)}/published/{feed.get('id')}"
    return {
        "id": feed.get("id"),
        "key": feed.get("key"),
        "created_at": feed.get("created_at") or 0,
        "last_fetch_at": feed.get("last_fetch_at") or 0,
        "last_fetch_format": feed.get("last_fetch_format") or "",
        "fetch_count": feed.get("fetch_count") or 0,
        "urls": {name: f"{base}/{name}?key={feed.get('key')}" for name in svc.FORMATS},
    }


@router.get("")
def api_published_lists(request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    by_list = {(feed.get("instance"), feed.get("list_id")): feed for feed in svc.feeds().values()}
    rows: list[dict[str, Any]] = []
    for row in svc.local_lists(cfg):
        if not _allowed(cfg, request, row["instance"]):
            continue
        feed = by_list.get((row["instance"], row["list_id"]))
        rows.append({**row, "feed": _feed_view(request, feed) if feed else None})
    formats = [{"name": name, **spec} for name, spec in svc.FORMATS.items()]
    return JSONResponse({"ok": True, "lists": rows, "formats": formats})


@router.post("")
def api_publish_list(payload: dict[str, Any] = Body(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    instance = payload.get("instance") or "default"
    list_id = str(payload.get("list_id") or "").strip()
    if not _allowed(cfg, request, instance):
        return _denied()
    if not any(row["instance"] == instance and row["list_id"] == list_id for row in svc.local_lists(cfg)):
        return JSONResponse({"ok": False, "error": "CrossWatch list not found"}, status_code=404)
    return JSONResponse({"ok": True, "feed": _feed_view(request, svc.publish(instance, list_id))})


@router.post("/{feed_id}/key")
def api_rotate_published_key(feed_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    feed = svc.feeds().get(feed_id)
    if not feed:
        return JSONResponse({"ok": False, "error": "Published list not found"}, status_code=404)
    if not _allowed(cfg, request, feed.get("instance")):
        return _denied()
    return JSONResponse({"ok": True, "feed": _feed_view(request, svc.rotate(feed_id) or feed)})


@router.delete("/{feed_id}")
def api_unpublish_list(feed_id: str = FPath(...), request: Request = cast(Request, None)) -> JSONResponse:
    cfg = load_config() or {}
    feed = svc.feeds().get(feed_id)
    if not feed:
        return JSONResponse({"ok": False, "error": "Published list not found"}, status_code=404)
    if not _allowed(cfg, request, feed.get("instance")):
        return _denied()
    svc.unpublish(feed_id)
    return JSONResponse({"ok": True})


@public_router.get("/published/{feed_id}/{name}")
def published_list(feed_id: str = FPath(...), name: str = FPath(...), key: str = Query("")) -> JSONResponse:
    headers = {"Cache-Control": "no-store"}
    feed = svc.authorized(feed_id, key)
    if not feed or name not in svc.FORMATS:
        return JSONResponse({"ok": False, "error": "Not found"}, status_code=404, headers=headers)
    items = svc.list_items(load_config() or {}, feed)
    if items is None:
        return JSONResponse({"ok": False, "error": "List unavailable"}, status_code=404, headers=headers)
    rows, skipped = svc.format_items(items, name)
    svc.touch(feed_id, name)
    return JSONResponse(rows, headers={**headers, "X-CrossWatch-Skipped": str(skipped)})

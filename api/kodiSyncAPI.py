# /api/kodiSyncAPI.py
# CrossWatch - Kodi add-on library sync endpoints
# Copyright (c) 2025-2026 CrossWatch / Cenodude
from __future__ import annotations

import json
import zlib
from typing import Any

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from starlette.concurrency import run_in_threadpool

from cw_platform.config_base import load_config

router = APIRouter(tags=["kodi-sync"])

BASE_PATH = "/webhook/kodiwatcher/sync/v1"
DEVICE_HEADER = "x-crosswatch-device"
MAX_BODY_BYTES = 16 * 1024 * 1024
MAX_INFLATED_BYTES = 64 * 1024 * 1024


def _error(status: int, code: str, message: str, retryable: bool = False) -> JSONResponse:
    return JSONResponse({"ok": False, "error": {"code": code, "message": message, "retryable": retryable}}, status_code=status)


def _too_large() -> JSONResponse:
    return _error(413, "payload_too_large", "Request body is too large. Split the upload into smaller parts.")


def _device(request: Request, cfg: dict[str, Any]) -> tuple[str | None, JSONResponse | None]:
    from providers.scrobble.kodi import addon as kodi_addon

    if "token" in request.query_params:
        return None, _error(400, "invalid_request", "Send the token in the X-CrossWatch-Token header.")
    token = str(request.headers.get(kodi_addon.TOKEN_HEADER) or "").strip()
    device = kodi_addon.instance_for_token(cfg, token)
    if device is None:
        return None, _error(401, "invalid_token", "Unknown or revoked token.")
    return device, None


def _device_id(request: Request) -> str:
    return str(request.headers.get(DEVICE_HEADER) or "").strip()


async def _json_body(request: Request) -> tuple[dict[str, Any] | None, JSONResponse | None]:
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            return None, _too_large()
        chunks.append(chunk)
    raw = b"".join(chunks)
    if str(request.headers.get("content-encoding") or "").strip().lower() == "gzip":
        try:
            inflater = zlib.decompressobj(16 + zlib.MAX_WBITS)
            raw = inflater.decompress(raw, MAX_INFLATED_BYTES)
            if inflater.unconsumed_tail:
                return None, _too_large()
        except zlib.error:
            return None, _error(400, "invalid_request", "Invalid gzip body.")
    try:
        body = json.loads(raw.decode("utf-8")) if raw else None
    except Exception:
        body = None
    if not isinstance(body, dict):
        return None, _error(400, "invalid_request", "Body must be a JSON object.")
    return body, None


async def _run(fn: Any, *args: Any, **kwargs: Any) -> JSONResponse:
    from providers.sync.kodi import _addon_sync as addon_sync

    try:
        result = await run_in_threadpool(fn, *args, **kwargs)
    except addon_sync.SyncRequestError as exc:
        return _error(exc.status, exc.code, exc.message, exc.retryable)
    except Exception:
        return _error(503, "temporarily_unavailable", "Library sync failed on the server.", True)
    return JSONResponse(result, status_code=200)


@router.post(f"{BASE_PATH}/upload")
async def kodi_sync_upload(request: Request) -> JSONResponse:
    from providers.sync.kodi import _addon_sync as addon_sync

    cfg = load_config() or {}
    device, failed = _device(request, cfg)
    if failed is not None:
        return failed
    body, failed = await _json_body(request)
    if failed is not None:
        return failed
    response = await _run(addon_sync.receive_upload, cfg, device, body, device_id=_device_id(request))
    if response.status_code == 200:
        try:
            result = json.loads(bytes(response.body).decode("utf-8"))
        except Exception:
            result = {}
        if result.get("mode") == "full" and result.get("complete") is True and result.get("instance"):
            addon_sync.trigger_pair_runs(cfg, result["instance"])
        elif result.get("mode") == "delta" and result.get("changed") is True and result.get("instance"):
            addon_sync.schedule_pair_runs(cfg, result["instance"])
    return response


@router.get(f"{BASE_PATH}/queue")
async def kodi_sync_queue(request: Request) -> JSONResponse:
    from providers.sync.kodi import _addon_sync as addon_sync

    cfg = load_config() or {}
    device, failed = _device(request, cfg)
    if failed is not None:
        return failed
    query = request.query_params
    return await _run(addon_sync.fetch_queue, cfg, device, query.get("viewer_id"), device_id=_device_id(request),
                      after=query.get("after"), limit=query.get("limit"))


@router.post(f"{BASE_PATH}/queue/ack")
async def kodi_sync_queue_ack(request: Request) -> JSONResponse:
    from providers.sync.kodi import _addon_sync as addon_sync

    cfg = load_config() or {}
    device, failed = _device(request, cfg)
    if failed is not None:
        return failed
    body, failed = await _json_body(request)
    if failed is not None:
        return failed
    return await _run(addon_sync.receive_ack, cfg, device, body, device_id=_device_id(request))

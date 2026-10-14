# api/assistantAPI.py
# CrossWatch - Built-in assistant settings and chat API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, Body, Request
from fastapi.responses import JSONResponse

from cw_platform.config_base import load_config, update_config
from services import assistant

__all__ = ["router"]

router = APIRouter(prefix="/api/assistant", tags=["assistant"])

NO_STORE = {"Cache-Control": "no-store"}
MAX_FIELD_CHARS = 400


def _reply(payload: dict[str, Any], status: int = 200) -> JSONResponse:
    return JSONResponse(payload, status_code=status, headers=NO_STORE)


def _blocked(request: Request) -> JSONResponse | None:
    user = getattr(request.state, "cw_user", None)
    if isinstance(user, dict) and (not user.get("is_admin") or user.get("monitor_view") or user.get("api_token_access")):
        return _reply({"ok": False, "error": "Administrator access required"}, 403)
    return None


def _public(conf: dict[str, Any]) -> dict[str, Any]:
    return {
        "ok": True,
        "enabled": conf["enabled"],
        "base_url": conf["base_url"],
        "model": conf["model"],
        "has_key": bool(conf["api_key"]),
        "ready": assistant.ready(conf),
    }


def _tools() -> dict[str, tuple[str, dict[str, Any]]]:
    from api.mcpAPI import TOOLS

    return {name: (description, schema) for name, (_title, description, schema, _handler) in TOOLS.items()}


def _runner(request: Request) -> assistant.ToolRunner:
    from api import mcpAPI

    def run(name: str, args: dict[str, Any]) -> str:
        try:
            result = mcpAPI._call_tool(request, {"name": name, "arguments": args})
        except mcpAPI.RpcError as exc:
            return exc.message
        return str(result["content"][0]["text"])

    return run


@router.get("/settings")
def assistant_settings(request: Request) -> JSONResponse:
    blocked = _blocked(request)
    if blocked is not None:
        return blocked
    return _reply(_public(assistant.settings(load_config() or {})))


@router.post("/settings")
def assistant_settings_save(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    blocked = _blocked(request)
    if blocked is not None:
        return blocked
    body = payload if isinstance(payload, dict) else {}
    base_url = str(body.get("base_url") or "").strip().rstrip("/")[:MAX_FIELD_CHARS] if "base_url" in body else None
    if base_url:
        parsed = urlparse(base_url)
        if parsed.scheme not in ("http", "https") or not parsed.hostname:
            return _reply({"ok": False, "error": "The address must start with http:// or https://"}, 400)

    def mutate(cfg: dict[str, Any]) -> dict[str, Any]:
        raw = cfg.get("assistant")
        block: dict[str, Any] = dict(raw) if isinstance(raw, dict) else {}
        key = str(body.get("api_key") or "").strip()
        moved = base_url is not None and base_url != assistant.settings(cfg)["base_url"]
        if "enabled" in body:
            block["enabled"] = bool(body.get("enabled"))
        if base_url is not None:
            block["base_url"] = base_url
        if "model" in body:
            block["model"] = str(body.get("model") or "").strip()[:MAX_FIELD_CHARS]
        if key:
            block["api_key"] = key[:MAX_FIELD_CHARS]
        elif moved or body.get("clear_key") is True:
            block["api_key"] = ""
        cfg["assistant"] = block
        return assistant.settings(cfg)

    _cfg, conf = update_config(mutate)
    return _reply(_public(conf))


@router.post("/models")
def assistant_models(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    blocked = _blocked(request)
    if blocked is not None:
        return blocked
    body = payload if isinstance(payload, dict) else {}
    conf = assistant.settings(load_config() or {})
    base_url = str(body.get("base_url") or "").strip().rstrip("/")[:MAX_FIELD_CHARS] or conf["base_url"]
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return _reply({"ok": False, "error": "The address must start with http:// or https://"}, 400)
    key = str(body.get("api_key") or "").strip()[:MAX_FIELD_CHARS]
    if not key and base_url == conf["base_url"]:
        key = conf["api_key"]
    lookup = {"base_url": base_url, "api_key": key}
    try:
        models = assistant.run_bounded(lambda: assistant.list_models(lookup), assistant.MODELS_TIMEOUT_S)
    except assistant.AssistantBusy as exc:
        return _reply({"ok": False, "error": str(exc)}, 429)
    except assistant.AssistantError as exc:
        return _reply({"ok": False, "error": str(exc)}, 502)
    return _reply({"ok": True, "models": models})


@router.post("/chat")
def assistant_chat(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    blocked = _blocked(request)
    if blocked is not None:
        return blocked
    conf = assistant.settings(load_config() or {})
    if not assistant.ready(conf):
        return _reply({"ok": False, "error": "The assistant is not set up yet"}, 400)
    body = payload if isinstance(payload, dict) else {}
    messages = assistant.clean_messages(body.get("messages"))
    scope = str(body.get("scope") or "all")
    if scope not in assistant.SCOPES:
        scope = "all"
    if not messages or messages[-1]["role"] != "user":
        return _reply({"ok": False, "error": "Ask a question first"}, 400)
    if not assistant.CHAT_SLOTS.acquire(blocking=False):
        return _reply({"ok": False, "error": assistant.BUSY}, 429)
    tools = _tools()
    runner = _runner(request)
    try:
        result = assistant.run_bounded(lambda: assistant.chat(conf, messages, tools, runner, scope), assistant.CHAT_DEADLINE_S)
    except assistant.AssistantBusy as exc:
        return _reply({"ok": False, "error": str(exc)}, 429)
    except assistant.AssistantError as exc:
        return _reply({"ok": False, "error": str(exc)}, 502)
    finally:
        assistant.CHAT_SLOTS.release()
    return _reply({"ok": True, "reply": result["reply"], "tools": result["tools"], "actions": result["actions"]})

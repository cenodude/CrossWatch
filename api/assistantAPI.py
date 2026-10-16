# api/assistantAPI.py
# CrossWatch - Built-in assistant settings and chat API
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from typing import Any
from urllib.parse import urlparse

from fastapi import APIRouter, Body, HTTPException, Request
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
        "wiki_help": conf["wiki_help"],
        "mode": assistant.mode(conf),
    }


def _tools() -> dict[str, tuple[str, dict[str, Any]]]:
    from api.mcpAPI import TOOLS

    return {name: (description, schema) for name, (_title, description, schema, _handler) in TOOLS.items()}


def _runner(request: Request, evidence: list | None = None) -> assistant.ToolRunner:
    from api import mcpAPI

    def run(name: str, args: dict[str, Any]) -> str:
        try:
            result = mcpAPI._call_tool(request, {"name": name, "arguments": args})
        except mcpAPI.RpcError as exc:
            return exc.message
        if evidence is not None and not result.get("isError"):
            from services.assistant_context import references
            text = str(result["content"][0]["text"])
            try:
                output = json.loads(text)
            except ValueError:
                output = None
            if not isinstance(output, str) and (output is not None or name in ("read_logs", "read_log_context")):
                try:
                    refs = references(request, name, args, output)
                except HTTPException:
                    refs = []
                for ref in refs:
                    if ref not in evidence and len(evidence) < 8:
                        evidence.append(ref)
        return str(result["content"][0]["text"])

    return run


def _proposal(action: str, args: dict[str, Any]) -> dict[str, Any] | str:
    from cw_platform.value_coercion import coerce_bool
    from services.snapshots import snapshot_manifest

    def text(value: Any) -> str:
        return str(value or "").strip()[:MAX_FIELD_CHARS]

    action = text(action)
    if action not in assistant.PROPOSAL_ACTIONS:
        return "Unknown action. Valid actions: " + ", ".join(assistant.PROPOSAL_ACTIONS)
    cfg = load_config() or {}
    params: dict[str, str] = {}
    if action == "run_pair":
        pairs = [p for p in cfg.get("pairs") or [] if isinstance(p, dict) and coerce_bool(p.get("enabled", True), True)]
        pair_id = text(args.get("pair_id"))
        pair = next((p for p in pairs if pair_id and str(p.get("id") or "") == pair_id), None)
        if pair is None:
            options = "; ".join(f"{text(p.get('id'))}: {text(p.get('source'))} → {text(p.get('target'))}" for p in pairs)
            return "Choose an enabled pair. Valid pairs: " + (options or "none")
        params = {"pair_id": pair_id}
        title = f"Run sync: {text(pair.get('source'))} → {text(pair.get('target'))}"
    elif action == "create_capture":
        provider = text(args.get("provider")).upper()
        instance = text(args.get("instance")) or "default"
        feature = text(args.get("feature")).lower()
        manifest = snapshot_manifest(cfg)
        valid = [
            (row, text(inst.get("id")), text(feat))
            for row in manifest
            for inst in row.get("instances") or [] if inst.get("configured")
            for feat, supported in (row.get("features") or {}).items() if supported
        ]
        match = next((row for row, inst, feat in valid if str(row.get("id") or "").upper() == provider and inst == instance and feat == feature), None)
        if match is None:
            options = "; ".join(f"{text(row.get('id')).upper()}/{inst}/{feat}" for row, inst, feat in valid)
            return "Choose a configured capture target. Valid provider/instance/feature combinations: " + (options or "none")
        params = {"provider": provider, "instance": instance, "feature": feature}
        title = f"Create capture: {text(match.get('label') or provider)} {feature}"
        if instance != "default":
            title += f" ({instance})"
    else:
        title = "Update anime mapping" if action == "refresh_anime_mapping" else "Test all connections"
    return {"action": action, "title": title[:MAX_FIELD_CHARS], "params": params}


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
        if "wiki_help" in body:
            block["wiki_help"] = body.get("wiki_help") is not False
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
        return _reply({"ok": False, "error": exc.public}, 429)
    except assistant.AssistantError as exc:
        return _reply({"ok": False, "error": exc.public}, 502)
    return _reply({"ok": True, "models": models})


@router.post("/test-model")
def assistant_test_model(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    blocked = _blocked(request)
    if blocked is not None:
        return blocked
    conf = assistant.settings(load_config() or {})
    base_url = str(payload.get("base_url") or "").strip().rstrip("/")[:MAX_FIELD_CHARS] or conf["base_url"]
    parsed = urlparse(base_url)
    if parsed.scheme not in ("http", "https") or not parsed.hostname:
        return _reply({"ok": False, "error": "The address must start with http:// or https://"}, 400)
    model = str(payload.get("model") or "").strip()[:MAX_FIELD_CHARS]
    if not model:
        return _reply({"ok": False, "error": "Choose a model first"}, 400)
    key = str(payload.get("api_key") or "").strip()[:MAX_FIELD_CHARS]
    if not key and base_url == conf["base_url"]:
        key = conf["api_key"]
    if not assistant.CHAT_SLOTS.acquire(blocking=False):
        return _reply({"ok": False, "error": assistant.BUSY}, 429)
    try:
        result = assistant.run_bounded(lambda: assistant.test_model({"base_url": base_url, "model": model, "api_key": key}), assistant.MODEL_TEST_TIMEOUT_S)
    except assistant.AssistantBusy as exc:
        return _reply({"ok": False, "error": exc.public}, 429)
    except assistant.AssistantError as exc:
        return _reply({"ok": False, "error": exc.public}, 502)
    finally:
        assistant.CHAT_SLOTS.release()
    return _reply({"ok": True, **result})


def _wiki_chat(messages: list[dict[str, str]]) -> JSONResponse:
    try:
        question = assistant.wiki_question(messages)
        found = assistant.run_bounded(lambda: assistant.ask_wiki(question), assistant.WIKI_ASK_TIMEOUT_S + assistant.GRACE_S)
    except assistant.AssistantBusy as exc:
        return _reply({"ok": False, "error": exc.public}, 429)
    except assistant.AssistantError as exc:
        return _reply({"ok": False, "error": exc.public}, 502)
    finally:
        assistant.CHAT_SLOTS.release()
    return _reply({
        "ok": True,
        "mode": "wiki",
        "reply": found["answer"],
        "tools": ["ask_wiki"],
        "actions": [],
        "sources": found["sources"],
        "proposals": [],
        "followups": found["followups"],
    })


@router.post("/chat")
def assistant_chat(request: Request, payload: dict[str, Any] = Body(default_factory=dict)) -> JSONResponse:
    blocked = _blocked(request)
    if blocked is not None:
        return blocked
    conf = assistant.settings(load_config() or {})
    mode = assistant.mode(conf)
    if mode == "off":
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
    if mode == "wiki":
        return _wiki_chat(messages)
    tools = _tools()
    evidence: list = []
    runner = _runner(request, evidence)
    try:
        from services.assistant_context import validate
        result = assistant.run_bounded(lambda: assistant.chat(conf, messages, tools, runner, scope, propose=_proposal, context=validate(request, body.get("context"))), assistant.CHAT_DEADLINE_S)
    except HTTPException as exc:
        return _reply({"ok": False, "error": str(exc.detail)}, exc.status_code)
    except assistant.AssistantBusy as exc:
        return _reply({"ok": False, "error": exc.public}, 429)
    except assistant.AssistantError as exc:
        return _reply({"ok": False, "error": exc.public}, 502)
    finally:
        assistant.CHAT_SLOTS.release()
    return _reply({"ok": True, "reply": result["reply"], "tools": result["tools"], "actions": result["actions"], "sources": result["sources"], "proposals": result["proposals"], **({"evidence": evidence} if evidence else {})})

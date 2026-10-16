# api/mcpAPI.py
# CrossWatch - Read-only MCP endpoint for AI clients
# Copyright (c) 2025-2026 CrossWatch / Cenodude (https://github.com/cenodude/CrossWatch)
from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any, Callable
from functools import partial

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse, Response
from starlette.concurrency import run_in_threadpool

__all__ = ["router"]

router = APIRouter(tags=["mcp"])

MCP_PATH = "/api/mcp"
PROTOCOL_VERSIONS = ("2025-11-25", "2025-06-18", "2025-03-26")
BATCH_VERSION = "2025-03-26"
VERSION_HEADER = "mcp-protocol-version"
MAX_BODY_BYTES = 64 * 1024
MAX_BATCH = 20
MAX_TEXT_CHARS = 60_000
MAX_LINE_CHARS = 2_000
UNRESOLVED_LIMIT = 100
HIDDEN_CONFIG_SECTIONS = frozenset({"app_auth"})
ARTWORK_KEYS = frozenset({"cover", "poster", "thumb", "grandparentThumb"})
NO_STORE = {"Cache-Control": "no-store"}

INSTRUCTIONS = (
    "CrossWatch is a self-hosted service that syncs watchlists, ratings, history and playback progress between media servers "
    "and trackers, and scrobbles live playback. Every tool here is read-only and secrets are redacted. "
    "To diagnose a problem start with get_health and get_last_sync, then use list_log_sessions and read_logs to find the cause, "
    "and get_config or list_pairs to check how things are set up. "
    "For one missing or wrong item use search_events, get_item_events and get_analyzer_problems. "
    "Use list_sync_context and search_sync_items for saved account state, explain_sync_item for item evidence, "
    "list_saved_mappings and list_anime_overrides for user rules, compare_captures for saved changes, "
    "get_scheduler_details for scheduling and list_sync_reviews for approvals. Saved state is not live provider state. "
    "Documentation: https://wiki.crosswatch.app/ (page index: https://wiki.crosswatch.app/llms.txt, "
    "docs MCP server: https://wiki.crosswatch.app/~gitbook/mcp)."
)

Handler = Callable[[Request, dict[str, Any]], Any]


class ToolError(Exception):
    pass


class RpcError(Exception):
    def __init__(self, code: int, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _payload(response: Any) -> Any:
    if isinstance(response, Response):
        return json.loads(bytes(response.body) or b"null")
    return response


def _text(args: dict[str, Any], key: str, limit: int = 200) -> str:
    return str(args.get(key) or "").strip()[:limit]


def _int(args: dict[str, Any], key: str, default: int, low: int, high: int) -> int:
    try:
        value = int(args.get(key, default))
    except (TypeError, ValueError):
        value = default
    return max(low, min(high, value))


def _choice(args: dict[str, Any], key: str, allowed: tuple[str, ...], default: str = "") -> str:
    value = _text(args, key)
    if not value:
        return default
    for option in allowed:
        if option.lower() == value.lower():
            return option
    raise ToolError(f"{key} must be one of: {', '.join(allowed)}")


def _iso(ts: Any) -> str:
    try:
        return datetime.fromtimestamp(float(ts), timezone.utc).isoformat(timespec="seconds")
    except (TypeError, ValueError, OverflowError, OSError):
        return ""


def _config() -> dict[str, Any]:
    from cw_platform.config_base import load_config

    return load_config() or {}


def _log_line(row: dict[str, Any]) -> str:
    from _logging import _redact_log_text

    text = _redact_log_text(row.get("text") or "")
    if len(text) > MAX_LINE_CHARS:
        text = text[:MAX_LINE_CHARS] + " [line cut]"
    return f"{row.get('id')} {_iso(row.get('ts'))} [{row.get('provider')}] {row.get('level')} {text}"


def _tool_health(request: Request, args: dict[str, Any]) -> Any:
    from api.healthAPI import api_health_details
    from api.versionAPI import get_version

    out = dict(_payload(api_health_details(request, strict=0)))
    try:
        out["version"] = get_version()
    except Exception:
        out["version"] = {}
    return out


def _tool_pairs(request: Request, args: dict[str, Any]) -> Any:
    from cw_platform.access_policy import filter_pairs_for_user, request_user

    cfg = _config()
    pairs = [pair for pair in cfg.get("pairs") or [] if isinstance(pair, dict)]
    return filter_pairs_for_user(cfg, request_user(request), pairs)


def _tool_last_sync(request: Request, args: dict[str, Any]) -> Any:
    from api.syncAPI import api_run_summary, api_run_unresolved

    unresolved = _payload(api_run_unresolved(request))
    if isinstance(unresolved, dict) and isinstance(unresolved.get("items"), list):
        unresolved["items"] = unresolved["items"][:UNRESOLVED_LIMIT]
    return {"summary": _payload(api_run_summary(request)), "unresolved": unresolved}


def _tool_config(request: Request, args: dict[str, Any]) -> Any:
    from services.support import scrub_config

    cfg = {key: value for key, value in scrub_config(_config()).items() if key not in HIDDEN_CONFIG_SECTIONS}
    section = _text(args, "section", 80)
    if section:
        if section not in cfg:
            raise ToolError(f"Unknown section. Available: {', '.join(sorted(cfg))}")
        return {section: cfg[section]}
    if len(json.dumps(cfg, default=str)) > MAX_TEXT_CHARS:
        return {"note": "Config is too large to return at once. Call again with one section.", "sections": sorted(cfg)}
    return cfg


def _tool_log_sessions(request: Request, args: dict[str, Any]) -> Any:
    from api import logsAPI

    channel = _choice(args, "channel", ("sync", "watcher", "debug"), "sync")
    limit = _int(args, "limit", 20, 1, 100)
    rows = [row for row in logsAPI.archive().sessions(channel) if logsAPI.allowed(request, row)][:limit]
    return [
        {
            "id": row["id"],
            "run_id": row["run_id"] or "",
            "label": row["label"],
            "status": row["status"],
            "started": _iso(row["started"]),
            "ended": _iso(row["ended"]) if row["ended"] is not None else "",
            "lines": row["lines"],
            "errors": row["errors"],
            "warnings": row["warnings"],
        }
        for row in rows
    ]


def _tool_read_logs(request: Request, args: dict[str, Any]) -> Any:
    from api import logsAPI

    sid = _text(args, "session_id", 64)
    session = logsAPI.get_session(request, sid)
    query = _text(args, "query")
    level = _choice(args, "level", ("ERROR", "WARN", "INFO", "DEBUG"))
    provider = _text(args, "provider", 80).upper()
    limit = _int(args, "limit", 200, 1, 500)
    store = logsAPI.archive()
    if args.get("offset") is None:
        total = store.page(sid, q=query, level=level, provider=provider, limit=1)["total"]
        offset = max(0, int(total) - limit)
    else:
        offset = _int(args, "offset", 0, 0, 10**9)
    page = store.page(sid, q=query, level=level, provider=provider, offset=offset, limit=limit)
    lines = [_log_line(row) for row in page["items"]]
    first = offset + 1 if lines else 0
    header = f"session {sid} ({session['label']}, {session['status']}): lines {first}-{offset + len(lines)} of {page['total']} matching"
    return "\n".join([header, *lines])


def _tool_log_context(request: Request, args: dict[str, Any]) -> Any:
    from api import logsAPI

    sid = _text(args, "session_id", 64)
    logsAPI.get_session(request, sid)
    rows = logsAPI.archive().context(sid, _int(args, "line_id", 0, 0, 2**62))
    return "\n".join(_log_line(row) for row in rows) or "No lines found."


def _tool_events(request: Request, args: dict[str, Any]) -> Any:
    from api.eventsAPI import events_search

    return _payload(
        events_search(
            q=_text(args, "query") or None,
            event_type=None,
            provider=_text(args, "provider", 80) or None,
            origin_provider=None,
            destination_provider=None,
            source_provider=None,
            feature=_text(args, "feature", 40).lower() or None,
            pair_key=None,
            item_key=None,
            run_id=_text(args, "run_id", 160) or None,
            reason_code=None,
            status=None,
            category=_choice(args, "category", ("problems", "successful", "informational")) or None,
            since=None,
            until=None,
            visibility="all",
            order="newest",
            view="groups",
            domain=_choice(args, "domain", ("sync", "scrobble"), "sync"),
            limit=_int(args, "limit", 25, 1, 100),
            offset=_int(args, "offset", 0, 0, 10**9),
            request=request,
        )
    )


def _tool_item_events(request: Request, args: dict[str, Any]) -> Any:
    from api.eventsAPI import events_item

    item_key = _text(args, "item_key", 400)
    if not item_key:
        raise ToolError("item_key is required")
    return _payload(
        events_item(
            item_key,
            limit=_int(args, "limit", 50, 1, 200),
            offset=_int(args, "offset", 0, 0, 10**9),
            visibility="all",
            request=request,
        )
    )


def _tool_watcher(request: Request, args: dict[str, Any]) -> Any:
    from api.scrobbleAPI import api_currently_watching, debug_watch_status

    out = dict(debug_watch_status(request))
    playing = _payload(api_currently_watching(request, user_profile=""))
    streams = playing.get("streams") or ([playing["currently_watching"]] if playing.get("currently_watching") else [])
    out["now_playing"] = [
        {key: value for key, value in stream.items() if key not in ARTWORK_KEYS}
        for stream in streams
        if isinstance(stream, dict)
    ]
    return out


def _tool_analyzer(request: Request, args: dict[str, Any]) -> Any:
    from services import analyzer

    pairs = analyzer._scoped_pairs_arg(request, _text(args, "pair_id", 160) or None)
    result = analyzer._analysis_with_saved_mappings(pairs, include_system=False, include_hints=False)
    rows = [row for row in result.get("problems") or [] if isinstance(row, dict)]
    feature = _text(args, "feature", 40).lower()
    if feature:
        rows = [row for row in rows if str(row.get("feature") or "").lower() == feature]
    limit = _int(args, "limit", 25, 1, 100)
    offset = _int(args, "offset", 0, 0, 10**9)
    raw_attention = result.get("attention")
    attention: dict[str, Any] = raw_attention if isinstance(raw_attention, dict) else {}
    return {
        "summary": result.get("summary"),
        "attention": attention.get("counts"),
        "total": len(rows),
        "offset": offset,
        "problems": rows[offset:offset + limit],
    }


def _tool_anime_mapping(request: Request, args: dict[str, Any]) -> Any:
    from api.animeMappingAPI import api_anime_mapping_status

    return _payload(api_anime_mapping_status())


def _tool_captures(request: Request, args: dict[str, Any]) -> Any:
    from api.snapshotsAPI import api_snapshots_list

    out = _payload(api_snapshots_list(request))
    rows = out.get("snapshots") if isinstance(out, dict) else None
    if not isinstance(rows, list):
        raise ToolError("Captures are not available")
    rows = sorted((row for row in rows if isinstance(row, dict)), key=lambda row: int(row.get("mtime") or 0), reverse=True)
    return {"total": len(rows), "captures": rows[: _int(args, "limit", 50, 1, 200)]}


def _schema(properties: dict[str, Any] | None = None, required: tuple[str, ...] = ()) -> dict[str, Any]:
    out: dict[str, Any] = {"type": "object", "properties": properties or {}, "additionalProperties": False}
    if required:
        out["required"] = list(required)
    return out


_SESSION_ID = {"type": "string", "description": "Log session id from list_log_sessions."}

TOOLS: dict[str, tuple[str, str, dict[str, Any], Handler]] = {
    "get_health": (
        "Get health",
        "Overall health: provider connections, watcher, scheduler, last sync run, open issues and the installed version. Start here.",
        _schema(),
        _tool_health,
    ),
    "get_last_sync": (
        "Get last sync",
        "Summary of the most recent sync run (per-feature counts, timeline, exit code) plus the items it could not resolve.",
        _schema(),
        _tool_last_sync,
    ),
    "list_pairs": (
        "List sync pairs",
        "Configured sync pairs with their id, source, target, mode and enabled features.",
        _schema(),
        _tool_pairs,
    ),
    "get_config": (
        "Get config",
        "The configuration with secrets, hosts and account names redacted. Pass section to get one top-level block such as pairs, scrobble or scheduling.",
        _schema({"section": {"type": "string", "description": "Top-level config key to return on its own."}}),
        _tool_config,
    ),
    "list_log_sessions": (
        "List log sessions",
        "Stored log sessions, newest first, with line, error and warning counts. A sync session is one sync run; watcher and debug sessions are one per day.",
        _schema(
            {
                "channel": {"type": "string", "enum": ["sync", "watcher", "debug"], "description": "Defaults to sync."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Defaults to 20."},
            }
        ),
        _tool_log_sessions,
    ),
    "read_logs": (
        "Read logs",
        "Lines from one log session as 'id time [provider] LEVEL text'. Without offset it returns the newest matching lines. Filter by level ERROR or WARN to find problems fast.",
        _schema(
            {
                "session_id": _SESSION_ID,
                "query": {"type": "string", "description": "Case-insensitive text to search for."},
                "level": {"type": "string", "enum": ["ERROR", "WARN", "INFO", "DEBUG"]},
                "provider": {"type": "string", "description": "Provider tag such as PLEX, TRAKT or SIMKL."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 500, "description": "Defaults to 200."},
                "offset": {"type": "integer", "minimum": 0, "description": "Position among matching lines, oldest first."},
            },
            ("session_id",),
        ),
        _tool_read_logs,
    ),
    "read_log_context": (
        "Read log context",
        "The lines right before and after one log line, to see what led to an error.",
        _schema({"session_id": _SESSION_ID, "line_id": {"type": "integer", "description": "Line id from read_logs."}}, ("session_id", "line_id")),
        _tool_log_context,
    ),
    "search_events": (
        "Search events",
        "Search the event archive: what each sync or scrobble did per item, newest first. Use category problems for failures and unresolved items.",
        _schema(
            {
                "query": {"type": "string", "description": "Text to search for, such as a title."},
                "domain": {"type": "string", "enum": ["sync", "scrobble"], "description": "Defaults to sync."},
                "category": {"type": "string", "enum": ["problems", "successful", "informational"]},
                "provider": {"type": "string"},
                "feature": {"type": "string", "description": "watchlist, ratings, history, progress, playlists or collection."},
                "run_id": {"type": "string"},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Defaults to 25."},
                "offset": {"type": "integer", "minimum": 0},
            }
        ),
        _tool_events,
    ),
    "get_item_events": (
        "Get item events",
        "Everything recorded for one movie, show or episode across runs, newest first. Use the item_key from search_events.",
        _schema(
            {
                "item_key": {"type": "string", "description": "Item key from search_events, such as tmdb:603#movie."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 200, "description": "Defaults to 50."},
                "offset": {"type": "integer", "minimum": 0},
            },
            ("item_key",),
        ),
        _tool_item_events,
    ),
    "get_watcher_status": (
        "Get watcher status",
        "Live state of the scrobble watcher: groups, routes, sinks, whether each route is running, and what is playing right now.",
        _schema(),
        _tool_watcher,
    ),
    "get_analyzer_problems": (
        "Get analyzer problems",
        "Items that differ between the two sides of a sync pair, with counts by severity and type. Answers why something is missing on one provider. The first call after a sync can take a while.",
        _schema(
            {
                "pair_id": {"type": "string", "description": "Pair id from list_pairs. Defaults to every enabled pair."},
                "feature": {"type": "string", "description": "watchlist, ratings, history, progress, playlists or collection."},
                "limit": {"type": "integer", "minimum": 1, "maximum": 100, "description": "Defaults to 25."},
                "offset": {"type": "integer", "minimum": 0},
            }
        ),
        _tool_analyzer,
    ),
    "get_anime_mapping_status": (
        "Get anime mapping status",
        "Whether anime ID mapping is enabled, which data it uses and when it was last updated. Check this when anime episodes land on the wrong season or number.",
        _schema(),
        _tool_anime_mapping,
    ),
    "list_captures": (
        "List captures",
        "Saved captures of provider data, newest first, with provider, feature, size and time.",
        _schema({"limit": {"type": "integer", "minimum": 1, "maximum": 200, "description": "Defaults to 50."}}),
        _tool_captures,
    ),
}


from services import assistant_tools as _shared_tools

TOOLS.update({name: (name.replace("_", " ").title(), description, schema, partial(_shared_tools.run, name))
              for name, (description, schema) in _shared_tools.TOOLS.items()})


def _tool_list() -> list[dict[str, Any]]:
    return [
        {
            "name": name,
            "title": title,
            "description": description,
            "inputSchema": schema,
            "annotations": {"title": title, "readOnlyHint": True, "openWorldHint": False},
        }
        for name, (title, description, schema, _handler) in TOOLS.items()
    ]


def _content(text: str, *, error: bool = False) -> dict[str, Any]:
    return {"content": [{"type": "text", "text": text}], "isError": error}


def _call_tool(request: Request, params: dict[str, Any]) -> dict[str, Any]:
    name = str(params.get("name") or "")
    tool = TOOLS.get(name)
    if tool is None:
        raise RpcError(-32602, f"Unknown tool: {name}")
    args = params.get("arguments")
    try:
        out = tool[3](request, args if isinstance(args, dict) else {})
    except ToolError as exc:
        return _content(str(exc), error=True)
    except HTTPException as exc:
        return _content(str(exc.detail), error=True)
    except Exception as exc:
        return _content(f"{name} failed: {type(exc).__name__}", error=True)
    text = out if isinstance(out, str) else json.dumps(out, ensure_ascii=False, separators=(",", ":"), default=str)
    if len(text) > MAX_TEXT_CHARS:
        text = text[:MAX_TEXT_CHARS] + "\n[output cut, narrow the request]"
    return _content(text)


def _initialize(params: dict[str, Any]) -> dict[str, Any]:
    from api.versionAPI import CURRENT_VERSION

    requested = str(params.get("protocolVersion") or "")
    return {
        "protocolVersion": requested if requested in PROTOCOL_VERSIONS else PROTOCOL_VERSIONS[0],
        "capabilities": {"tools": {"listChanged": False}},
        "serverInfo": {"name": "crosswatch", "title": "CrossWatch", "version": str(CURRENT_VERSION)},
        "instructions": INSTRUCTIONS,
    }


def _rpc_error(msg_id: Any, code: int, message: str) -> dict[str, Any]:
    return {"jsonrpc": "2.0", "id": msg_id, "error": {"code": code, "message": message}}


def _handle(request: Request, message: Any) -> dict[str, Any] | None:
    if not isinstance(message, dict) or message.get("jsonrpc") != "2.0":
        return _rpc_error(None, -32600, "Invalid request")
    method = message.get("method")
    if not isinstance(method, str) or "id" not in message:
        return None
    msg_id = message.get("id")
    raw_params = message.get("params")
    params: dict[str, Any] = raw_params if isinstance(raw_params, dict) else {}
    try:
        if method == "initialize":
            result: dict[str, Any] = _initialize(params)
        elif method == "ping":
            result = {}
        elif method == "tools/list":
            result = {"tools": _tool_list()}
        elif method == "tools/call":
            result = _call_tool(request, params)
        else:
            raise RpcError(-32601, f"Method not found: {method}")
    except RpcError as exc:
        return _rpc_error(msg_id, exc.code, exc.message)
    return {"jsonrpc": "2.0", "id": msg_id, "result": result}


def _token_caller(request: Request) -> bool:
    user = getattr(request.state, "cw_user", None)
    return isinstance(user, dict) and user.get("auth_kind") == "api_token" and bool(user.get("is_admin"))


def _origin_ok(request: Request) -> bool:
    if not str(request.headers.get("origin") or "").strip():
        return True
    from api.appAuthAPI import _origin_allowed

    return _origin_allowed(request)


async def _read_body(request: Request) -> bytes | None:
    try:
        declared = int(request.headers.get("content-length") or 0)
    except ValueError:
        declared = 0
    if declared > MAX_BODY_BYTES:
        return None
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > MAX_BODY_BYTES:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


@router.post(MCP_PATH)
async def mcp_post(request: Request) -> Response:
    if not _token_caller(request):
        return JSONResponse(
            {"ok": False, "error": "MCP needs an admin or monitor API token"},
            status_code=403,
            headers=NO_STORE,
        )
    if not _origin_ok(request):
        return JSONResponse({"ok": False, "error": "Origin mismatch"}, status_code=403, headers=NO_STORE)
    version = str(request.headers.get(VERSION_HEADER) or "").strip() or BATCH_VERSION
    if version not in PROTOCOL_VERSIONS:
        return JSONResponse(_rpc_error(None, -32600, f"Unsupported protocol version: {version}"), status_code=400, headers=NO_STORE)
    raw = await _read_body(request)
    if raw is None:
        return JSONResponse(_rpc_error(None, -32600, "Request too large"), status_code=413, headers=NO_STORE)
    try:
        message = json.loads(raw)
    except ValueError:
        return JSONResponse(_rpc_error(None, -32700, "Parse error"), status_code=400, headers=NO_STORE)
    batch = message if isinstance(message, list) else [message]
    if isinstance(message, list) and (version != BATCH_VERSION or not 0 < len(batch) <= MAX_BATCH):
        return JSONResponse(_rpc_error(None, -32600, "Invalid request"), status_code=400, headers=NO_STORE)
    replies = await run_in_threadpool(lambda: [reply for reply in (_handle(request, item) for item in batch) if reply is not None])
    if not replies:
        return Response(status_code=202, headers=NO_STORE)
    return JSONResponse(replies if isinstance(message, list) else replies[0], headers=NO_STORE)


@router.get(MCP_PATH)
def mcp_get() -> Response:
    return Response(status_code=405, headers={"Allow": "POST", **NO_STORE})
